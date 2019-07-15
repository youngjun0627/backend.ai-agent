import base64
import functools
import hashlib
from ipaddress import ip_network, _BaseAddress as BaseIPAddress
import json
import logging, logging.config
import os
from pathlib import Path
import pkg_resources
import platform
from pprint import pformat, pprint
import re
import secrets
import signal
import sys
import time
from typing import Collection, Mapping
import yaml

from aiodocker.docker import Docker, DockerContainer
from aiodocker.exceptions import DockerError
import aiohttp
import aioredis
import aiotools
import aiozmq
from aiozmq import rpc
import attr
import asyncio
import click
from kubernetes_asyncio import client as K8sClient, config as K8sConfig
from setproctitle import setproctitle
import snappy
import trafaret as t
import uvloop
import zmq

from ai.backend.common import config, utils, identity, msgpack
from ai.backend.common import validators as tx
from ai.backend.common.docker import ImageRef
from ai.backend.common.etcd import AsyncEtcd, ConfigScopes
from ai.backend.common.logging import Logger, BraceStyleAdapter
from ai.backend.common.monitor import DummyStatsMonitor, DummyErrorMonitor
from ai.backend.common.types import (
    HostPortPair,
    ResourceSlot,
    MountPermission,
)
from ai.backend.common.utils import StringSetFlag
from . import __version__ as VERSION
from .exception import InitializationError, InsufficientResource
from .fs import create_scratch_filesystem, destroy_scratch_filesystem
from .kernel import (
    KernelRunner
)
from .resources import (
    detect_resources,
    AbstractComputeDevice,
    AbstractComputePlugin,
    KernelResourceSpec,
    Mount,
    AbstractAllocMap,
)
from .utils import (
    current_loop, update_nested_dict
)
from .k8sconfs import (
    ConfigMapMountSpec,
    HostPathMountSpec,
    KernelDeployment,
    ConfigMap,
    Service
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.server'))

max_upload_size = 100 * 1024 * 1024 # 100 MB
ipc_base_path = Path('/tmp/backend.ai/ipc')

redis_config_iv = t.Dict({
    t.Key('addr', default=('127.0.0.1', 6379)): tx.HostPortPair,
    t.Key('password', default=None): t.Null | t.String,
})

@attr.s(auto_attribs=True, slots=True)
class ComputerContext:
    klass: AbstractComputePlugin
    devices: Collection[AbstractComputeDevice]
    alloc_map: AbstractAllocMap

@attr.s(auto_attribs=True, slots=True)
class VolumeInfo:
    name: str             # volume name
    container_path: str   # in-container path as str
    mode: str             # 'rw', 'ro', 'rwm'

class KernelFeatures(StringSetFlag):
    UID_MATCH = 'uid-match'
    USER_INPUT = 'user-input'
    BATCH_MODE = 'batch'
    QUERY_MODE = 'query'
    TTY_MODE = 'tty'

deeplearning_image_keys = {
    'tensorflow', 'caffe',
    'keras', 'torch',
    'mxnet', 'theano',
}

deeplearning_sample_volume = VolumeInfo(
    'deeplearning-samples', '/home/work/samples', 'ro',
)

def parse_service_port(s: str) -> dict:
    try:
        name, protocol, port = s.split(':')
    except (ValueError, IndexError):
        raise ValueError('Invalid service port definition format', s)
    assert protocol in ('tcp', 'pty', 'http'), \
           f'Unsupported service port protocol: {protocol}'
    try:
        port = int(port)
    except ValueError:
        raise ValueError('Invalid port number', port)
    if port <= 1024:
        raise ValueError('Service port number must be larger than 1024.')
    if port in (2000, 2001):
        raise ValueError('Service port 2000 and 2001 is reserved for internal use.')
    return {
        'name': name,
        'protocol': protocol,
        'container_port': port,
        'host_port': None,  # determined after container start
    }

def update_last_used(meth):
    @functools.wraps(meth)
    async def _inner(self, kernel_id: str, *args, **kwargs):
        try:
            kernel_info = self.container_registry[kernel_id]
            kernel_info['last_used'] = time.monotonic()
        except KeyError:
            pass
        return await meth(self, kernel_id, *args, **kwargs)
    return _inner

async def get_extra_volumes(docker, lang):
    avail_volumes = (await docker.volumes.list())['Volumes']
    if not avail_volumes:
        return []
    avail_volume_names = set(v['Name'] for v in avail_volumes)

    # deeplearning specialization
    # TODO: extract as config
    volume_list = []
    for k in deeplearning_image_keys:
        if k in lang:
            volume_list.append(deeplearning_sample_volume)
            break

    # Mount only actually existing volumes
    mount_list = []
    for vol in volume_list:
        if vol.name in avail_volume_names:
            mount_list.append(vol)
        else:
            log.info('skipped attaching extra volume {0} '
                     'to a kernel based on image {1}',
                     vol.name, lang)
    return mount_list

class AgentRPCServer(aiozmq.rpc.AttrHandler):
    def __init__(self, etcd, config, loop=None):
        self.loop = loop if loop else current_loop()
        self.etcd = etcd
        self.config = config
        self.agent_id = hashlib.md5(__file__.encode('utf-8')).hexdigest()[:12]

        self.event_sock = None
        self.k8sCoreApi = None
        self.k8sAppsApi = None
        self.computers: Mapping[str, ComputerContext] = {}
        self.container_registry = {}
        self.hb_timer = None
        self.clean_timer = None
        self.scan_images_timer = None
        self.images = []

        self.workers = {}
        self.master = None

        self.error_monitor = DummyErrorMonitor()

    async def init(self):
        await K8sConfig.load_kube_config()
        self.k8sCoreApi = K8sClient.CoreV1Api()
        self.k8sAppsApi = K8sClient.AppsV1Api()
        self.agent_sockpath = ipc_base_path / f'agent.{self.agent_id}.sock'

        self.slots = await detect_resources(self.k8sCoreApi)
        
        ipc_base_path.mkdir(parents=True, exist_ok=True)
        self.zmq_ctx = zmq.asyncio.Context()

        await self.fetch_workers()
        await self.read_agent_config()
        await self.update_status('starting')

        self.redis_stat_pool = await aioredis.create_redis_pool(
            self.config['redis']['addr'].as_sockaddr(), password=(self.config['redis']['password']
                      if self.config['redis']['password'] else None),
            timeout=3.0,
            encoding='utf8',
            db=0)  # REDIS_STAT_DB in backend.ai-manager

        self.event_sock = await aiozmq.create_zmq_stream(
            zmq.PUSH, connect=f"tcp://{self.config['event']['addr']}")
        self.event_sock.transport.setsockopt(zmq.LINGER, 50)

        await self.scan_images(None)
        # TODO: Create k8s compatible stat collector

        # Send the first heartbeat.
        self.hb_timer    = aiotools.create_timer(self.heartbeat, 3.0)
        self.clean_timer = aiotools.create_timer(self.clean_old_kernels, 10.0)
        self.scan_images_timer = aiotools.create_timer(self.scan_images, 60.0)

        # Start serving requests.
        rpc_addr = self.config['agent']['rpc-listen-addr']
        agent_addr = f"tcp://{rpc_addr}"
        self.rpc_server = await aiozmq.rpc.serve_rpc(self, bind=agent_addr)
        self.rpc_server.transport.setsockopt(zmq.LINGER, 200)
        log.info('started handling RPC requests at {}', rpc_addr)

        # Ready.
        await self.etcd.put('ip', rpc_addr.host, scope=ConfigScopes.NODE)
        await self.update_status('running')

        # Notify the gateway.
        await self.send_event('instance_started')

    async def fetch_workers(self):
        nodes = await self.k8sCoreApi.list_node()
        for node in nodes.items:
            if 'node-role.kubernetes.io/master' in node.metadata.labels.keys():
                continue
            self.workers[node.metadata.name] = node.status.capacity
            for addr in node.status.addresses:
                if addr.type == 'InternalIP':
                    self.workers[node.metadata.name]['InternalIP'] = addr.address

    async def update_status(self, status):
        await self.etcd.put('', status, scope=ConfigScopes.NODE)

    async def scan_images(self, interval):
        addr, port = self.config['registry']['addr'].as_sockaddr()
        registry_addr = f'https://{addr}:{port}'
        updated_images = []
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
            all_images = await session.get(f'{registry_addr}/v2/_catalog')
            all_images = await all_images.json()
            for image_name in all_images['repositories']:
                image_tags = await session.get(f'{registry_addr}/v2/{image_name}/tags/list')
                image_tags = await image_tags.json()
                updated_images += [f'{image_name}:{x}' for x in image_tags['tags']]
        
        for added_image in list(set(updated_images) - set(self.images)):
            log.debug('found kernel image: {0}', added_image)
        for removed_image in list(set(self.images) - set(updated_images)):
            log.debug('removed kernel image: {0}', removed_image)
        self.images = updated_images
        
    async def heartbeat(self, interval):
        '''
        Send my status information and available kernel images.
        '''
        res_slots = {
            'cpu': ('count', self.slots['cpu']),
            'mem': ('bytes', self.slots['mem'])
        }
        
        agent_info = {
            'ip': str(self.config['agent']['rpc-listen-addr'].host),
            'region': self.config['agent']['region'],
            'addr': f"tcp://{self.config['agent']['rpc-listen-addr']}",
            'resource_slots': res_slots,
            'version': VERSION,
            'compute_plugins': {
                key: {
                    'version': computer.klass.get_version(),
                    **(await computer.klass.extra_info())
                }
                for key, computer in self.computers.items()
            },
            'images': snappy.compress(msgpack.packb([
                (x, 'K8S_AGENT') for x in self.images
            ]))
        }

        try:
            await self.send_event('instance_heartbeat', agent_info)
        except asyncio.TimeoutError:
            log.warning('event dispatch timeout: instance_heartbeat')
        except Exception:
            log.exception('instance_heartbeat failure')
            self.error_monitor.capture_exception()

    async def clean_old_kernels(self, interval):
        now = time.monotonic()
        keys = tuple(self.container_registry.keys())
        tasks = []
        for kernel_id in keys:
            try:
                last_used = self.container_registry[kernel_id]['last_used']
                idle_timeout = \
                    self.container_registry[kernel_id]['resource_spec'] \
                    .idle_timeout
                if idle_timeout > 0 and now - last_used > idle_timeout:
                    log.info('destroying kernel {0} as clean-up', kernel_id)
                    task = asyncio.ensure_future(
                        self._destroy_kernel(kernel_id, 'idle-timeout'))
                    tasks.append(task)
            except KeyError:
                # The kernel may be destroyed by other means?
                pass
        await asyncio.gather(*tasks)

    async def detect_manager(self):
        log.info('detecting the manager...')
        manager_id = await self.etcd.get('nodes/manager')
        if manager_id is None:
            log.warning('watching etcd to wait for the manager being available')
            async for ev in self.etcd.watch('nodes/manager'):
                if ev.event == 'put':
                    manager_id = ev.value
                    break
        log.info('detecting the manager: OK ({0})', manager_id)
    
    async def read_agent_config(self):
        # Fill up runtime configurations from etcd.
        redis_config = await self.etcd.get_prefix('config/redis')
        self.config['redis'] = redis_config_iv.check(redis_config)
        self.config['event'] = {
            'addr': tx.HostPortPair().check(await self.etcd.get('nodes/manager/event_addr')),
        }
        log.info('configured redis_addr: {0}', self.config['redis']['addr'])
        log.info('configured event_addr: {0}', self.config['event']['addr'])

        vfolder_mount = await self.etcd.get('volumes/_mount')
        if vfolder_mount is None:
            vfolder_mount = '/mnt'
        vfolder_fsprefix = await self.etcd.get('volumes/_fsprefix')
        if vfolder_fsprefix is None:
            vfolder_fsprefix = ''
        self.config['vfolder'] = {
            'mount': Path(vfolder_mount),
            'fsprefix': Path(vfolder_fsprefix.lstrip('/')),
        }
        log.info('configured vfolder mount base: {0}', self.config['vfolder']['mount'])
        log.info('configured vfolder fs prefix: {0}', self.config['vfolder']['fsprefix'])

    async def shutdown(self, signal_code):
        # TODO: gracefully shudown AgentRPCServer
        pass
    
    @aiozmq.rpc.method
    @update_last_used
    async def create_kernel(self, kernel_id: str, config: dict) -> dict:
        log.debug('rpc::create_kernel({0}, {1})', kernel_id, config['image'])
        async with self.handle_rpc_exception():
            return await self._create_kernel(kernel_id, config)

    @aiozmq.rpc.method
    @update_last_used
    async def destroy_kernel(self, kernel_id: str):
        log.debug('rpc::destroy_kernel({0})', kernel_id)
        async with self.handle_rpc_exception():
            return await self._destroy_kernel(kernel_id, 'user-requested')

    @aiozmq.rpc.method
    async def execute(self, api_version: int,
                      kernel_id: str,
                      run_id: t.String | t.Null,
                      mode: str,
                      code: str,
                      opts: dict,
                      flush_timeout: t.Float | t.Null) -> dict:
        log.debug('rpc::execute({0})', kernel_id)
        async with self.handle_rpc_exception():
            result = await self._execute(api_version, kernel_id,
                                         run_id, mode, code, opts,
                                         flush_timeout)
            return result
    
    async def _create_kernel(self, kernel_id: str, kernel_config: dict, restarting: bool=False) -> dict:

        await self.send_event('kernel_preparing', kernel_id)
        log.debug('create_kernel: kernel_config -> {0}', json.dumps(kernel_config, ensure_ascii=False, indent=2))
        # Read image-specific labels and settings
        image_ref = ImageRef(
            kernel_config['image']['canonical'],
            [kernel_config['image']['registry']['name']])
        environ = kernel_config.get('environ', {})
        # extra_mount_list = await get_extra_volumes(self.docker, image_ref.short)

        image_labels = kernel_config['image']['labels']

        version        = int(image_labels.get('ai.backend.kernelspec', '1'))
        envs_corecount = image_labels.get('ai.backend.envs.corecount', '')
        envs_corecount = envs_corecount.split(',') if envs_corecount else []
        kernel_features = set(image_labels.get('ai.backend.features', '').split())

        scratch_dir = (self.config['container']['scratch-root'] / kernel_id).resolve()
        tmp_dir = (self.config['container']['scratch-root'] / f'{kernel_id}_tmp').resolve()
        config_dir = scratch_dir / 'config'
        work_dir = scratch_dir / 'work'

        slots = ResourceSlot.from_json(kernel_config['resource_slots'])
        resource_spec = KernelResourceSpec(
            container_id=None,
            allocations={},
            slots={**slots},  # copy
            mounts=[],
            scratch_disk_size=0,  # TODO: implement (#70)
            idle_timeout=kernel_config['idle_timeout'],
        )

        registry_addr, registry_port = self.config['registry']['addr']
        canonical = kernel_config['image']['canonical']
        
        _, repo, name_with_tag = canonical.split('/')
        deployment = KernelDeployment(f'{registry_addr}:{registry_port}/{repo}/{name_with_tag}')

        deployment.name = f"kernel-{image_ref.name.split('/')[-1]}-{kernel_id}".replace('.', '-')

        log.debug('Initial container config: {0}', deployment.to_dict())

        # Inject Backend.AI-intrinsic env-variables for gosu
        if KernelFeatures.UID_MATCH in kernel_features:
            uid = self.config['container']['kernel-uid']
            environ['LOCAL_USER_ID'] = str(uid)

        # Ensure that we have intrinsic slots.
        assert 'cpu' in slots
        assert 'mem' in slots


        # Realize ComputeDevice (including accelerators) allocations.
        dev_types = set()
        for slot_type in slots.keys():
            dev_type = slot_type.split('.', maxsplit=1)[0]
            dev_types.add(dev_type)

        resource_spec.allocations = {
            'cpu': { 'cpu': { '0': slots['cpu'] } },
            'mem': { 'mem': { 'root': slots['mem'] } }
        }
    

        # Inject Backend.AI-intrinsic env-variables for libbaihook and gosu
        cpu_core_count = len(resource_spec.allocations['cpu']['cpu'])
        environ.update({k: str(cpu_core_count) for k in envs_corecount})

        def _mount(kernel_id: str, hostPath: str, mountPath: str, mountType: str):
            name = (kernel_id + '-' + mountPath.split('/')[-1]).replace('.', '-')
            deployment.mount_hostpath(HostPathMountSpec(name, hostPath, mountPath, mountType))

        # Inject Backend.AI kernel runner dependencies.
        distro = image_labels.get('ai.backend.base-distro', 'ubuntu16.04')
        arch = platform.machine()
        _mount(kernel_id, '/opt/backend.ai/krunner/env.ubuntu16.04',
               '/opt/backend.ai', 'Directory')
        _mount(kernel_id, '/opt/backend.ai/runner/entrypoint.sh', 
               '/opt/backend.ai/bin/entrypoint.sh', 'File')
        _mount(kernel_id, f'/opt/backend.ai/runner/su-exec.{distro}.bin',
               '/opt/backend.ai/bin/su-exec', 'File')
        _mount(kernel_id, f'/opt/backend.ai/runner/jail.{distro}.bin',
               '/opt/backend.ai/bin/jail', 'File')
        _mount(kernel_id, f'/opt/backend.ai/hook/libbaihook.{distro}.{arch}.so',
               '/opt/backend.ai/hook/libbaihook.so', 'File')
        _mount(kernel_id, '/opt/backend.ai/kernel',
               '/opt/backend.ai/lib/python3.6/site-packages/ai/backend/kernel', 'Directory')
        _mount(kernel_id, '/opt/backend.ai/helpers',
               '/opt/backend.ai/lib/python3.6/site-packages/ai/backend/helpers', 'Directory')
        _mount(kernel_id, '/opt/backend.ai/runner/jupyter-custom.css',
               '/home/work/.jupyter/custom/custom.css', 'File')
        _mount(kernel_id, '/opt/backend.ai/runner/logo.svg',
               '/home/work/.jupyter/custom/logo.svg', 'File')
        _mount(kernel_id, '/opt/backend.ai/runner/roboto.ttf',
               '/home/work/.jupyter/custom/roboto.ttf', 'File')
        _mount(kernel_id, '/opt/backend.ai/runner/roboto-italic.ttf',
               '/home/work/.jupyter/custom/roboto-italic.ttf', 'File')
        environ['LD_PRELOAD'] = '/opt/backend.ai/hook/libbaihook.so'

        # TODO: Inject ComputeDevice-specific env-varibles and hooks

        # PHASE 3: Store the resource spec.

        resource_txt = resource_spec.write_to_text()
        environ_txt = ''

        for k, v in environ.items():
            environ_txt += f'{k}={v}\n'
                
        # accel_envs = computer_docker_args.get('Env', [])
        # for env in accel_envs:
        #    environ_txt += f'{env}\n'

        # TODO: Add additional args from config
        
        # PHASE 4: Run!
        log.info('kernel {0} starting with resource spec: \n',
                 pformat(attr.asdict(resource_spec)))

        exposed_ports = [2000, 2001]
        service_ports = {}
        
        for item in image_labels.get('ai.backend.service-ports', '').split(','):
            if not item:
                continue
            service_port = parse_service_port(item)
            container_port = service_port['container_port']
            service_ports[container_port] = service_port
            exposed_ports.append(container_port)
        if 'git' in image_ref.name:  # legacy (TODO: remove it!)
            exposed_ports.append(2002)
            exposed_ports.append(2003)
        log.debug('exposed ports: {!r}', exposed_ports)

        container_external_ip = '0.0.0.0'

        runtime_type = image_labels.get('ai.backend.runtime-type', 'python')
        runtime_path = image_labels.get('ai.backend.runtime-path', None)
        cmdargs = []
        if self.config['container']['sandbox-type'] == 'jail':
            cmdargs += [
                "/opt/backend.ai/bin/jail",
                "-policy", "/etc/backend.ai/jail/policy.yml",
            ]
            if self.config['container']['jail-args']:
                cmdargs += map(lambda s: s.strip(), self.config['container']['jail-args'])
        cmdargs += [
            "/opt/backend.ai/bin/python",
            "-m", "ai.backend.kernel", runtime_type,
        ]
        if runtime_path is not None:
            cmdargs.append(runtime_path)



        configmap = ConfigMap(deployment.name + '-configmap')
        configmap.put('environ', environ_txt)
        configmap.put('resource', resource_txt)

        deployment.cmd = cmdargs
        deployment.env = environ
        deployment.mount_configmap(ConfigMapMountSpec('environ', configmap.name, 'environ', '/home/config/environ.txt'))
        deployment.mount_configmap(ConfigMapMountSpec('resource', configmap.name, 'resource', '/home/config/resource.txt'))
        deployment.ports = exposed_ports

        service = Service(deployment.name + '-service', deployment.name, [ (port, f'{kernel_id}-svc-{index}') for port, index in zip(exposed_ports, range(1, len(exposed_ports) + 1)) ], service_type='ClusterIP')
        log.debug('container config: {!r}', deployment.to_dict())

        # We are all set! Create and start the container.
        try:
            clusterip_api_response = await self.k8sCoreApi.create_namespaced_service('backend-ai', body=service.to_dict())
        except:
            raise
        # pprint(nodeport_api_response)
        try:
            cm_api_response = await self.k8sCoreApi.create_namespaced_config_map('backend-ai', body=configmap.to_dict(), pretty='pretty_example')
        except:
            await self.k8sCoreApi.delete_namespaced_service(service.name, 'backend-ai')
            raise
        try:
            deployment_api_response = await self.k8sAppsApi.create_namespaced_deployment('backend-ai', body=deployment.to_dict(), pretty='pretty_example')
        except:
            await self.k8sCoreApi.delete_namespaced_service(service.name, 'backend-ai')
            await self.k8sCoreApi.delete_namespaced_config_map(configmap.name, 'backend-ai')
            raise

        stdin_port = 0
        stdout_port = 0
        repl_in_port = 2000
        repl_out_port = 2001

        exposed_ports = clusterip_api_response.spec.ports

        log.debug('ClusterIP: {0}', clusterip_api_response.spec.cluster_ip)

        self.container_registry[kernel_id] = {
            'deployment_name': deployment.name,
            'lang': image_ref,
            'version': version,
            'kernel_host': 'localhost',
            'cluster_ip': clusterip_api_response.spec.cluster_ip,
            'repl_in_port': repl_in_port,
            'repl_out_port': repl_out_port,
            'stdin_port': stdin_port,    # legacy
            'stdout_port': stdout_port,  # legacy
            'service_ports': list(service_ports.values()),
            'host_ports': exposed_ports,
            'last_used': time.monotonic(),
            'runner_tasks': set(),
            'resource_spec': resource_spec,
            'agent_tasks': [],
        }

        log.debug('kernel repl-in address: {0}:{1}', clusterip_api_response.spec.cluster_ip, repl_in_port)
        log.debug('kernel repl-out address: {0}:{1}', clusterip_api_response.spec.cluster_ip, repl_out_port)
        await self.send_event('kernel_started', kernel_id)
        return {
            'id': kernel_id,
            'kernel_host': 'localhost',
            'repl_in_port': repl_in_port,
            'repl_out_port': repl_out_port,
            'stdin_port': stdin_port,    # legacy
            'stdout_port': stdout_port,  # legacy
            'service_ports': [],
            'container_id': '#',
            'resource_spec': resource_spec.to_json(),
        }

    

    async def _destroy_kernel(self, kernel_id, reason):
        async def force_cleanup(reason='self-terminated'):
                await self.send_event('kernel_terminated',
                                      kernel_id, 'self-terminated',
                                      None)
        try:
            kernel = self.container_registry[kernel_id]
        except:
            log.warning('_destroy_kernel({0}) kernel missing (already dead?)',
                        kernel_id)

            await asyncio.shield(force_cleanup())
            return None
        deployment_name = kernel['deployment_name']
        try:
            await self.k8sCoreApi.delete_namespaced_service(f'{deployment_name}-service', 'backend-ai')
            await self.k8sCoreApi.delete_namespaced_config_map(f'{deployment_name}-configmap', 'backend-ai')
            await self.k8sAppsApi.delete_namespaced_deployment(f'{deployment_name}', 'backend-ai')
        except:
            log.warning('_destroy({0}) kernel missing (already dead?)', kernel_id)
        
        del self.container_registry[kernel_id]

        await force_cleanup(reason=reason)
        return None

    async def _ensure_runner(self, kernel_id, *, api_version=3):
        runner = self.container_registry[kernel_id].get('runner')
        
        if runner is not None:
            log.debug('_execute_code:v{0}({1}) use '
                        'existing runner', api_version, kernel_id)
        else:
            client_features = {'input', 'continuation'}
            runner = KernelRunner(
                self.container_registry[kernel_id]['deployment_name'], 
                self.container_registry[kernel_id]['repl_in_port'],
                self.container_registry[kernel_id]['repl_out_port'],
                self.container_registry[kernel_id]['cluster_ip'],
                0,
                self.k8sAppsApi,
                client_features)
            log.debug('_execute:v{0}({1}) start new runner',
                        api_version, kernel_id)
            self.container_registry[kernel_id]['runner'] = runner
            await runner.start()
        return runner

    async def _execute(self, api_version, kernel_id,
                       run_id, mode, text, opts,
                       flush_timeout):
        try:
            kernel_info = self.container_registry[kernel_id]
        except KeyError:
            await self.send_event('kernel_terminated', kernel_id, 'self-terminated', None)
            raise RuntimeError(f'The container for kernel {kernel_id} is not found! '
                               '(might be terminated--try it again)') from None
        
        kernel_info['last_used'] = time.monotonic()
        runner = await self._ensure_runner(kernel_id, api_version=api_version)

        try:
            myself = asyncio.Task.current_task()
            kernel_info['runner_tasks'].add(myself)

            await runner.attach_output_queue(run_id)

            # if mode == 'batch' or mode == 'query':
            #     kernel_info['initial_file_stats'] \
            #         = scandir(output_dir, max_upload_size)
            if mode == 'batch':
                await runner.feed_batch(opts)
            elif mode == 'query':
                await runner.feed_code(text)
            elif mode == 'input':
                await runner.feed_input(text)
            elif mode == 'continue':
                pass
            result = await runner.get_next_result(
                api_ver=api_version,
                flush_timeout=flush_timeout)

        except asyncio.CancelledError:
            await runner.close()
            kernel_info.pop('runner', None)
            return
        finally:
            runner_tasks = utils.nmget(self.container_registry,
                                       f'{kernel_id}/runner_tasks', None, '/')
            if runner_tasks is not None:
                runner_tasks.remove(myself)

        output_files = []

        if result['status'] in ('finished', 'exec-timeout'):

            log.debug('_execute({0}) {1}', kernel_id, result['status'])

            # final_file_stats = scandir(output_dir, max_upload_size)
            if utils.nmget(result, 'options.upload_output_files', True):
                # TODO: separate as a new task
                pass
            kernel_info.pop('initial_file_stats', None)

        if (result['status'] == 'exec-timeout' and
                kernel_id in self.container_registry):
            # clean up the kernel (maybe cleaned already)
            asyncio.ensure_future(
                self._destroy_kernel(kernel_id, 'exec-timeout'))

        return {
            **result,
            'files': output_files,
        }

    @aiotools.actxmgr
    async def handle_rpc_exception(self):
        try:
            yield
        except AssertionError:
            log.exception('assertion failure')
            raise
        except Exception:
            log.exception('unexpected error')
            self.error_monitor.capture_exception()
            raise
    
    async def send_event(self, event_name, *args):
        if self.event_sock is None:
            return
        log.debug('send_event({0})', event_name)
        self.event_sock.write((
            event_name.encode('ascii'),
            self.config['agent']['id'].encode('utf8'),
            msgpack.packb(args),
        ))


@aiotools.server
async def server_main(loop, pidx, _args):
    config = _args[0]

    etcd_credentials = None
    if config['etcd']['user']:
        etcd_credentials = {
            'user': config['etcd']['user'],
            'password': config['etcd']['password'],
        }
    scope_prefix_map = {
        ConfigScopes.GLOBAL: '',
        ConfigScopes.SGROUP: f"sgroup/{config['agent']['scaling-group']}",
        ConfigScopes.NODE: f"nodes/agents/{config['agent']['id']}",
    }
    etcd = AsyncEtcd(config['etcd']['addr'],
                     config['etcd']['namespace'],
                     scope_prefix_map,
                     credentials=etcd_credentials)
    subnet_hint = await etcd.get('config/network/subnet/agent')
    if subnet_hint is not None:
        subnet_hint = ip_network(subnet_hint)

    if not config['agent']['id']:
        config['agent']['id'] = await identity.get_instance_id()
    if not config['agent']['instance-type']:
        config['agent']['instance-type'] = await identity.get_instance_type()
    rpc_addr = config['agent']['rpc-listen-addr']
    if not rpc_addr.host:
        config['agent']['rpc-listen-addr'] = HostPortPair(
            await identity.get_instance_ip(subnet_hint),
            rpc_addr.port,
        )
    if not config['container']['kernel-host']:
        config['container']['kernel-host'] = config['container']['kernel-host']
    if not config['agent']['region']:
        config['agent']['region'] = await identity.get_instance_region()
    log.info('Node ID: {0} (machine-type: {1}, host: {2})',
             config['agent']['id'],
             config['agent']['instance-type'],
             rpc_addr.host)

    # Start RPC server.
    try:
        agent = AgentRPCServer(etcd, config, loop=loop)
        await agent.init()
    except InitializationError as e:
        log.error('Agent initialization failed: {}', e)
        os.kill(0, signal.SIGINT)
    except Exception:
        log.exception('unexpected error during AgentRPCServer.init()!')
        os.kill(0, signal.SIGINT)

    # Run!
    try:
        stop_signal = yield
    finally:
        # Shutdown.
        log.info('shutting down...')
        await agent.shutdown(stop_signal)

@click.group(invoke_without_command=True)
@click.option('-f', '--config-path', '--config', type=Path, default=None,
              help='The config file path. (default: ./agent.conf and /etc/backend.ai/agent.conf)')
@click.option('--debug', is_flag=True,
              help='Enable the debug mode and override the global log level to DEBUG.')
@click.pass_context
def main(cli_ctx, config_path, debug):

    agent_config_iv = t.Dict({
        t.Key('agent'): t.Dict({
            t.Key('rpc-listen-addr', default=('', 6001)): tx.HostPortPair(allow_blank_host=True),
            t.Key('id', default=None): t.Null | t.String,
            t.Key('region', default=None): t.Null | t.String,
            t.Key('instance-type', default=None): t.Null | t.String,
            t.Key('scaling-group', default='default'): t.String,
            t.Key('pid-file', default=os.devnull): tx.Path(type='file',
                                                           allow_nonexisting=True,
                                                           allow_devnull=True),
        }).allow_extra('*'),
        t.Key('container'): t.Dict({
            t.Key('kernel-uid', default=-1): tx.UserID,
            t.Key('kernel-host', default=''): t.String(allow_blank=True),
            t.Key('port-range', default=(30000, 31000)): tx.PortRange,
            t.Key('sandbox-type'): t.Enum('docker', 'jail'),
            t.Key('jail-args', default=[]): t.List(t.String),
            t.Key('scratch-type'): t.Enum('hostdir', 'memory'),
            t.Key('scratch-root', default='./scratches'): tx.Path(type='dir', auto_create=True),
            t.Key('scratch-size', default='0'): tx.BinarySize,
        }).allow_extra('*'),
        t.Key('registry'): t.Dict({
            t.Key('addr'): tx.HostPortPair(allow_blank_host=False)
        }),
        t.Key('logging'): t.Any,  # checked in ai.backend.common.logging
        t.Key('resource'): t.Dict({
            t.Key('reserved-cpu', default=1): t.Int,
            t.Key('reserved-mem', default="1G"): tx.BinarySize,
            t.Key('reserved-disk', default="8G"): tx.BinarySize,
        }).allow_extra('*'),
        t.Key('debug'): t.Dict({
            t.Key('enabled', default=False): t.Bool,
            t.Key('skip-container-deletion', default=False): t.Bool,
        }).allow_extra('*'),
    }).merge(config.etcd_config_iv).allow_extra('*')

    # Determine where to read configuration.
    raw_cfg, cfg_src_path = config.read_from_file(config_path, 'agent')

    # Override the read config with environment variables (for legacy).
    config.override_with_env(raw_cfg, ('etcd', 'namespace'), 'BACKEND_NAMESPACE')
    config.override_with_env(raw_cfg, ('etcd', 'addr'), 'BACKEND_ETCD_ADDR')
    config.override_with_env(raw_cfg, ('etcd', 'user'), 'BACKEND_ETCD_USER')
    config.override_with_env(raw_cfg, ('etcd', 'password'), 'BACKEND_ETCD_PASSWORD')
    config.override_with_env(raw_cfg, ('agent', 'rpc-listen-addr', 'host'),
                             'BACKEND_AGENT_HOST_OVERRIDE')
    config.override_with_env(raw_cfg, ('agent', 'rpc-listen-addr', 'port'),
                             'BACKEND_AGENT_PORT')
    config.override_with_env(raw_cfg, ('agent', 'pid-file'), 'BACKEND_PID_FILE')
    config.override_with_env(raw_cfg, ('container', 'port-range'), 'BACKEND_CONTAINER_PORT_RANGE')
    config.override_with_env(raw_cfg, ('container', 'kernel-host'), 'BACKEND_KERNEL_HOST_OVERRIDE')
    config.override_with_env(raw_cfg, ('container', 'sandbox-type'), 'BACKEND_SANDBOX_TYPE')
    config.override_with_env(raw_cfg, ('container', 'scratch-root'), 'BACKEND_SCRATCH_ROOT')
    if debug:
        config.override_key(raw_cfg, ('debug', 'enabled'), True)
        config.override_key(raw_cfg, ('logging', 'level'), 'DEBUG')
        config.override_key(raw_cfg, ('logging', 'pkg-ns', 'ai.backend'), 'DEBUG')

    # Validate and fill configurations
    # (allow_extra will make configs to be forward-copmatible)
    try:
        cfg = config.check(raw_cfg, agent_config_iv)
        if 'debug'in cfg and cfg['debug']['enabled']:
            print('== Agent configuration ==')
            pprint(cfg)
        cfg['_src'] = cfg_src_path
    except config.ConfigurationError as e:
        print('Validation of agent configuration has failed:', file=sys.stderr)
        print(pformat(e.invalid_data), file=sys.stderr)
        raise click.Abort()

    rpc_host = cfg['agent']['rpc-listen-addr'].host
    if (isinstance(rpc_host, BaseIPAddress) and
        (rpc_host.is_unspecified or rpc_host.is_link_local)):
        print('Cannot use link-local or unspecified IP address as the RPC listening host.',
              file=sys.stderr)
        raise click.Abort()

    if cli_ctx.invoked_subcommand is None:
        cfg['agent']['pid-file'].write_text(str(os.getpid()))
        try:
            logger = Logger(cfg['logging'])
            with logger:
                ns = cfg['etcd']['namespace']
                setproctitle(f"backend.ai: agent {ns}")
                log.info('Backend.AI Agent {0}', VERSION)
                log.info('runtime: {0}', utils.env_info())

                log_config = logging.getLogger('ai.backend.agent.config')
                if debug:
                    log_config.debug('debug mode enabled.')

                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
                aiotools.start_server(server_main, num_workers=1,
                                      use_threading=True, args=(cfg, ))
                log.info('exit.')
        finally:
            if cfg['agent']['pid-file'].is_file():
                # check is_file() to prevent deleting /dev/null!
                cfg['agent']['pid-file'].unlink()
    else:
        # Click is going to invoke a subcommand.
        pass
    return 0

if __name__ == "__main__":
    sys.exit(main())
