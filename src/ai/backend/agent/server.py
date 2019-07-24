import base64
import configparser
import datetime
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
import random
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
import boto3
import click
from kubernetes_asyncio import client as K8sClient, config as K8sConfig
import pytz
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
from .exception import (
    NoKernelError, 
    InitializationError, 
    InsufficientResource, 
    K8sError
)
from .fs import create_scratch_filesystem, destroy_scratch_filesystem
from .kernel import (
    KernelRunner,
    prepare_krunner_env,
    prepare_kernel_statics
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
    PVCMountSpec,
    ConfigMapMountSpec,
    HostPathMountSpec,
    KernelDeployment,
    ConfigMap,
    Service,
    NFSPersistentVolume,
    NFSPersistentVolumeClaim
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
        self.blocking_cleans = {}

        self.event_sock = None
        self.k8sCoreApi = None
        self.k8sAppsApi = None
        self.computers: Mapping[str, ComputerContext] = {}
        self.container_registry = {}
        self.hb_timer = None
        self.clean_timer = None
        self.scan_images_timer = None
        self.images = []
        self.vfolder_as_pvc = ''
        self.ecr_address = ''
        self.ecr_token = ''
        self.ecr_token_expireAt = ''

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

        if 'vfolder-pv' in self.config.keys():
            await self.create_vfolder_pv()

        if self.config['registry']['type'] == 'ecr':
            await self.get_aws_credentials()

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
        await self.check_krunner_pv_status()

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

    async def get_aws_credentials(self):
        region = configparser.ConfigParser()
        credentials = configparser.ConfigParser()

        aws_path = Path.home() / Path('.aws')
        region.read_string((aws_path / Path('config')).read_text())
        credentials.read_string((aws_path / Path('credentials')).read_text())

        profile = self.config['registry']['profile']
        self.config['registry']['region'] = region[profile]['region'] 
        self.config['registry']['access-key'] = credentials[profile]['aws_access_key_id']
        self.config['registry']['secret-key'] = credentials[profile]['aws_secret_access_key']

    async def check_krunner_pv_status(self):
        nfs_pv = await self.k8sCoreApi.list_persistent_volume(label_selector='backend.ai/bai-static-nfs-server')
        if len(nfs_pv.items) == 0:
            # PV does not exists; create one
            pv = NFSPersistentVolume(
                self.config['baistatic']['nfs-addr'], self.config['baistatic']['path'],
                'backend-ai-static-files-pv', str(self.config['baistatic']['capacity'])
            )
            pv.label('backend.ai/bai-static-nfs-server', self.config['baistatic']['nfs-addr'])
            pv.options = [x.strip() for x in self.config['baistatic']['options'].split(',')]
            
            try:
                pv_response = await self.k8sCoreApi.create_persistent_volume(body=pv.to_dict())
            except:
                raise
        
        nfs_pvc = await self.k8sCoreApi.list_namespaced_persistent_volume_claim('backend-ai', label_selector='backend.ai/bai-static-nfs-server')
        if len(nfs_pvc.items) == 0:
            # PV does not exists; create one
            pvc = NFSPersistentVolumeClaim(
                'backend-ai-static-files-pvc', 'backend-ai-static-files-pv', str(self.config['baistatic']['capacity'])
            )
            pvc.label('backend.ai/bai-static-nfs-server', self.config['baistatic']['nfs-addr'])
            try:
                pvc_response = await self.k8sCoreApi.create_namespaced_persistent_volume_claim('backend-ai', body=pvc.to_dict())
            except:
                raise
    
    async def create_vfolder_pv(self):
        log.debug('Trying to create vFolder PV/PVC...')
        
        addr = self.config['vfolder-pv']['nfs-addr']
        path = self.config['vfolder-pv']['path']
        capacity = str(self.config['vfolder-pv']['capacity'])
        options = [x.strip() for x in self.config['vfolder-pv']['options'].split(',')]

        nfs_pv = await self.k8sCoreApi.list_persistent_volume(label_selector='backend.ai/vfolder')
        if len(nfs_pv.items) == 0:
            pv = NFSPersistentVolume(addr, path, 'backend-ai-vfolder-pv', capacity)
            pv.label('backend.ai/vfolder', '')
            pv.options = options
            try:
                pv_response = await self.k8sCoreApi.create_persistent_volume(body=pv.to_dict())
            except:
                raise
        
        nfs_pvc = await self.k8sCoreApi.list_namespaced_persistent_volume_claim('backend-ai', label_selector='backend.ai/vfolder')
        if len(nfs_pvc.items) == 0:
            pvc = NFSPersistentVolumeClaim(
                'backend-ai-vfolder-pvc', 'backend-ai-vfolder-pv', str(capacity)
            )
            pvc.label('backend.ai/vfolder', '')
            try:
                pvc_response = await self.k8sCoreApi.create_namespaced_persistent_volume_claim('backend-ai', body=pvc.to_dict())
            except:
                raise
        self.vfolder_as_pvc = True
    
    async def fetch_workers(self):
        nodes = await self.k8sCoreApi.list_node()
        for node in nodes.items:
            if 'node-role.kubernetes.io/master' in node.metadata.labels.keys():
                continue
            self.workers[node.metadata.name] = node.status.capacity
            for addr in node.status.addresses:
                if addr.type == 'InternalIP':
                    self.workers[node.metadata.name]['InternalIP'] = addr.address
                if addr.type == 'ExternalIP':
                    self.workers[node.metadata.name]['ExternalIP'] = addr.address

    async def update_status(self, status):
        await self.etcd.put('', status, scope=ConfigScopes.NODE)

    async def get_docker_registry_info(self):
        # Determines Registry URL and its appropriate Authorization header
        if self.config['registry']['type'] == 'local':
            addr, port = self.config['registry']['addr'].as_sockaddr()
            registry_addr = f'https://{addr}:{port}'
            registry_pull_secrets = await self.k8sCoreApi.list_namespaced_secret('backend-ai')
            for secret in registry_pull_secrets.items:
                if secret.metadata.name == 'backend-ai-registry-secret':
                    pull_secret = secret
                    break
            else:
                raise K8sError('backend-ai-registry-secret does not exist in backend-ai namespace')
            
            addr, _ = self.config['registry']['addr'].as_sockaddr()

            auth_data = json.loads(base64.b64decode(pull_secret.data['.dockerconfigjson']))
            for server, authInfo in auth_data['auths'].items():
                if addr in server:
                    token = authInfo['auth']
                    return registry_addr, { 'Authorization': f'Basic {token}' }
            else:
                raise K8sError('No authentication information for registry server configured in agent.toml')

        elif self.config['registry']['type'] == 'ecr':
            # Check if given token is valid yet
            utc = pytz.UTC
            if self.ecr_address and self.ecr_token and self.ecr_token_expireAt and utc.localize(datetime.datetime.now()) < self.ecr_token_expireAt:
                registry_addr, headers = self.ecr_address, { 'Authorization': 'Basic ' + self.ecr_token } 
                return registry_addr, headers

            client = boto3.client(
                'ecr', 
                aws_access_key_id=self.config['registry']['access-key'],
                aws_secret_access_key=self.config['registry']['secret-key']
            )
            auth_data = client.get_authorization_token(registryIds=[ self.config['registry']['registry-id'] ])
            self.ecr_address = auth_data['authorizationData'][0]['proxyEndpoint']
            self.ecr_token = auth_data['authorizationData'][0]['authorizationToken']
            self.ecr_token_expireAt = auth_data['authorizationData'][0]['expiresAt']
            log.debug('ECR Access Token expires at: {0}', self.ecr_token_expireAt)

            return self.ecr_address, { 'Authorization': 'Basic ' + self.ecr_token, 'Content-Type': 'application/json' }

    async def scan_images(self, interval):

        updated_images = []
        registry_addr, headers = await self.get_docker_registry_info()

        async with aiohttp.ClientSession(headers=headers, connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
            all_images = await session.get(f'{registry_addr}/v2/_catalog')
            status_code = all_images.status
            log.debug('/v2/_catalog/: {0}', await all_images.text())
            all_images = json.loads(await all_images.text())
            if status_code == 401:
                raise K8sError('\n'.join([x['message'] for x in all_images['errors']]))
            
            for image_name in all_images['repositories']:
                image_tags = await session.get(f'{registry_addr}/v2/{image_name}/tags/list')
                image_tags = json.loads(await image_tags.text())
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

        if 'vfolder-pv' in self.config.keys():
            vfolder_mount = 'nfs:' + self.config['vfolder-pv']['nfs-addr'] + ':' + self.config['vfolder-pv']['path']
        else:
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

    async def shutdown(self, stop_signal):
        # TODO: gracefully shudown AgentRPCServer
        await self.deregister_myself()

        # Stop receiving further requests.
        if self.rpc_server is not None:
            self.rpc_server.close()
            await self.rpc_server.wait_closed()

        # Close all pending kernel runners.
        for kernel_id in self.container_registry.keys():
            await self.clean_runner(kernel_id)

        if stop_signal == signal.SIGTERM:
            await self.clean_all_kernels(blocking=True)

        # Stop timers.
        if self.scan_images_timer is not None:
            self.scan_images_timer.cancel()
            await self.scan_images_timer
        if self.hb_timer is not None:
            self.hb_timer.cancel()
            await self.hb_timer
        # TODO: stop live_stat_timer
        if self.clean_timer is not None:
            self.clean_timer.cancel()
            await self.clean_timer

        # TODO: Stop event monitoring.
        # TODO: Stop stat collector task.
        

        if self.redis_stat_pool is not None:
            self.redis_stat_pool.close()
            await self.redis_stat_pool.wait_closed()

        # Stop handlign agent sock.
        # (But we don't remove the socket file)
        # TODO: Find k8s-ful ways to handle agent socket

        # Notify the gateway.
        if self.event_sock is not None:
            await self.send_event('instance_terminated', 'shutdown')
            self.event_sock.close()

        self.zmq_ctx.term()

    async def get_image_manifest(self, image_name, tag):
        registry_addr, headers = await self.get_docker_registry_info()

        async with aiohttp.ClientSession(headers=headers, connector=aiohttp.TCPConnector(verify_ssl=False)) as session:
            image_manifest = await session.get(f'{registry_addr}/v2/{image_name}/manifests/{tag}')
            status_code = image_manifest.status
            image_manifest = json.loads(await image_manifest.text())
            if status_code == 401:
                raise K8sError('\n'.join([x['message'] for x in image_manifest['errors']]))
            
            image_manifest = await image_manifest.json()
            return image_manifest

    async def deregister_myself(self):
        # TODO: reimplement using Redis heartbeat stream
        pass
    
    @aiozmq.rpc.method
    def ping(self, msg: str) -> str:
        return msg

    @aiozmq.rpc.method
    @update_last_used
    async def ping_kernel(self, kernel_id: str):
        log.debug('rpc::ping_kernel({0})', kernel_id)

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
    @update_last_used
    async def interrupt_kernel(self, kernel_id: str):
        log.debug('rpc::interrupt_kernel({0})', kernel_id)
        async with self.handle_rpc_exception():
            await self._interrupt_kernel(kernel_id)

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

    @aiozmq.rpc.method
    @update_last_used
    async def get_completions(self, kernel_id: str,
                              text: str, opts: dict):
        log.debug('rpc::get_completions({0})', kernel_id)
        async with self.handle_rpc_exception():
            await self._get_completions(kernel_id, text, opts)

    @aiozmq.rpc.method
    @update_last_used
    async def upload_file(self, kernel_id: str, filename: str, filedata: bytes):
        log.debug('rpc::upload_file({0}, {1})', kernel_id, filename)
        async with self.handle_rpc_exception():
            await self._accept_file(kernel_id, filename, filedata)

    @aiozmq.rpc.method
    @update_last_used
    async def download_file(self, kernel_id: str, filepath: str):
        log.debug('rpc::download_file({0}, {1})', kernel_id, filepath)
        async with self.handle_rpc_exception():
            return await self._download_file(kernel_id, filepath)

    @aiozmq.rpc.method
    @update_last_used
    async def list_files(self, kernel_id: str, path: str):
        log.debug('rpc::list_files({0}, {1})', kernel_id, path)
        async with self.handle_rpc_exception():
            return await self._list_files(kernel_id, path)
    
    @aiozmq.rpc.method
    @update_last_used
    async def get_logs(self, kernel_id: str):
        log.debug('rpc::get_logs({0})', kernel_id)
        async with self.handle_rpc_exception():
            return await self._get_logs(kernel_id)

    @aiozmq.rpc.method
    @update_last_used
    async def refresh_idle(self, kernel_id: str):
        # update_last_used decorator already implements this. :)
        log.debug('rpc::refresh_idle()')
        pass

    @aiozmq.rpc.method
    @update_last_used
    async def start_service(self, kernel_id: str, service: str, opts: dict):
        log.debug('rpc::start_service({0}, {1})', kernel_id, service)
        async with self.handle_rpc_exception():
            return await self._start_service(kernel_id, service, opts)

    @aiozmq.rpc.method
    async def shutdown_agent(self, terminate_kernels: bool):
        # TODO: implement
        log.debug('rpc::shutdown_agent()')
        pass

    @aiozmq.rpc.method
    async def reset_agent(self):
        log.debug('rpc::reset()')
        async with self.handle_rpc_exception():
            kernel_ids = tuple(self.container_registry.keys())
            tasks = []
            for kernel_id in kernel_ids:
                try:
                    task = asyncio.ensure_future(
                        self._destroy_kernel(kernel_id, 'agent-reset'))
                    tasks.append(task)
                except Exception:
                    self.error_monitor.capture_exception()
                    log.exception('reset: destroying {0}', kernel_id)
            await asyncio.gather(*tasks)

    
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
        vfolders = kernel_config['mounts']
        resource_spec = KernelResourceSpec(
            container_id=None,
            allocations={},
            slots={**slots},  # copy
            mounts=[],
            scratch_disk_size=0,  # TODO: implement (#70)
            idle_timeout=kernel_config['idle_timeout'],
        )


        # Inject Backend.AI-intrinsic env-variables for gosu
        if KernelFeatures.UID_MATCH in kernel_features:
            uid = self.config['container']['kernel-uid']
            gid = self.config['container']['kernel-gid']
            environ['LOCAL_USER_ID'] = str(gid)
            environ['LOCAL_USER_ID'] = str(uid)
            if os.getuid() == 0:  # only possible when I am root.
                os.chown(work_dir, uid, uid)
                os.chown(work_dir / '.jupyter', uid, uid)
                os.chown(work_dir, uid, gid)
                os.chown(work_dir / '.jupyter', uid, gid)


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

        
        # Inject Backend.AI kernel runner dependencies.
        distro = image_labels.get('ai.backend.base-distro', 'ubuntu16.04')
        arch = platform.machine()

        if self.config['registry']['type'] == 'local':
            registry_addr, registry_port = self.config['registry']['addr']
            registry_name = f'{registry_addr}:{registry_port}'
        elif self.config['registry']['type'] == 'ecr':
            registry_name = self.ecr_address.replace('https://', '')
        
        canonical = kernel_config['image']['canonical']
        
        _, repo, name_with_tag = canonical.split('/')
        if self.config['registry']['type'] != 'local':
            deployment = KernelDeployment(kernel_id, f'{registry_name}/{repo}/{name_with_tag}', \
                distro, arch, ecr_url=registry_name, name=f"kernel-{image_ref.name.split('/')[-1]}-{kernel_id}".replace('.', '-'))
        else:
            deployment = KernelDeployment(kernel_id, f'{registry_name}/{repo}/{name_with_tag}', \
                distro, arch, name=f"kernel-{image_ref.name.split('/')[-1]}-{kernel_id}".replace('.', '-'))

        deployment.krunner_source = 'image'

        log.debug('Initial container config: {0}', deployment.to_dict())
        
        def _mount(kernel_id: str, hostPath: str, mountPath: str, mountType: str, perm='ro'):
            name = (kernel_id + '-' + mountPath.split('/')[-1]).replace('.', '-')
            deployment.mount_hostpath(HostPathMountSpec(name, hostPath, mountPath, mountType, perm))

        try:
            nfs_pvc = await self.k8sCoreApi.list_namespaced_persistent_volume_claim('backend-ai', label_selector='backend.ai/bai-static-nfs-server')
            if len(nfs_pvc.items) == 0:
                raise K8sError('No PVC for backend.ai static files')
            pvc = nfs_pvc.items[0]
            if pvc.status.phase != 'Bound':
                raise K8sError('PVC not Bound')
        except:
            raise
        deployment.baistatic_pvc = pvc.metadata.name
        
        deployment.mount_exec(PVCMountSpec('entrypoint.sh', '/krunner/entrypoint.sh', 'File'))
        deployment.mount_exec(PVCMountSpec(f'su-exec.{distro}.bin', '/krunner/su-exec', 'File'))
        deployment.mount_exec(PVCMountSpec(f'jail.{distro}.bin', '/krunner/jail', 'File'))
        deployment.mount_exec(PVCMountSpec(f'libbaihook.{distro}.{arch}.so', '/krunner/libbaihook.so', 'File'))
        deployment.mount_pvc(PVCMountSpec('kernel', '/opt/backend.ai/lib/python3.6/site-packages/ai/backend/kernel', 'Directory'))
        deployment.mount_pvc(PVCMountSpec('helpers', '/opt/backend.ai/lib/python3.6/site-packages/ai/backend/helpers', 'Directory'))
        deployment.mount_pvc(PVCMountSpec('jupyter-custom.css', '/home/work/.jupyter/custom/custom.css', 'File'))
        deployment.mount_pvc(PVCMountSpec('logo.svg', '/home/work/.jupyter/custom/logo.svg', 'File'))
        deployment.mount_pvc(PVCMountSpec('roboto.ttf', '/home/work/.jupyter/custom/roboto.ttf', 'File'))
        deployment.mount_pvc(PVCMountSpec('roboto-italic.ttf', '/home/work/.jupyter/custom/roboto-italic.ttf', 'File'))
    
        
        # Check if vFolder PVC is Bound; otherwise raise since we can't use vfolder
        if len(vfolders) > 0 and self.vfolder_as_pvc:
            try:
                nfs_pvc = await self.k8sCoreApi.list_namespaced_persistent_volume_claim('backend-ai', label_selector='backend.ai/vfolder')
                if len(nfs_pvc.items) == 0:
                    raise K8sError('No PVC for backend.ai static files')
                pvc = nfs_pvc.items[0]
                if pvc.status.phase != 'Bound':
                    raise K8sError('PVC not Bound')
            except:
                raise
            deployment.vfolder_pvc = pvc.metadata.name

        for vfolder in vfolders:
            if len(vfolder) == 4:
                folder_name, folder_host, folder_id, folder_perm = vfolder
            elif len(vfolder) == 3:  # legacy managers
                folder_name, folder_host, folder_id = vfolder
                folder_perm = 'rw'
            else:
                raise RuntimeError(
                    'Unexpected number of vfolder mount detail tuple size')
            
            folder_perm = MountPermission(folder_perm)
            perm_char = 'ro'
            if folder_perm == MountPermission.RW_DELETE or folder_perm == MountPermission.READ_WRITE:
                # TODO: enforce readable/writable but not deletable
                # (Currently docker's READ_WRITE includes DELETE)
                perm_char = 'rw'
            
            if self.vfolder_as_pvc:
                subPath = (folder_host / self.config['vfolder']['fsprefix'] / folder_id)
                log.debug('Trying to mount {0} to {1}', str(subPath), folder_id)
                deployment.mount_vfolder_pvc(PVCMountSpec(str(subPath), f'/home/work/{folder_name}', 'Directory', perm=perm_char))
            else:
                host_path = (self.config['vfolder']['mount'] / folder_host /
                    self.config['vfolder']['fsprefix'] / folder_id)
                _mount(kernel_id, host_path.absolute().as_posix(), f'/home/work/{folder_name}', 'Directory', perm=perm_char)

        # should no longer be used!
        del vfolders

        environ['LD_PRELOAD'] = '/opt/kernel/libbaihook.so'

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

        exposed_ports = []
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


        runtime_type = image_labels.get('ai.backend.runtime-type', 'python')
        runtime_path = image_labels.get('ai.backend.runtime-path', None)
        cmdargs = []
        if self.config['container']['sandbox-type'] == 'jail':
            cmdargs += [
                "/opt/kernel/jail",
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



        configmap = ConfigMap(kernel_id, deployment.name + '-configmap')
        configmap.put('environ', environ_txt)
        configmap.put('resource', resource_txt)

        deployment.cmd = cmdargs
        deployment.env = environ
        deployment.mount_configmap(ConfigMapMountSpec('environ', configmap.name, 'environ', '/home/config/environ.txt'))
        deployment.mount_configmap(ConfigMapMountSpec('resource', configmap.name, 'resource', '/home/config/resource.txt'))
        deployment.ports = exposed_ports

        repl_service = Service(kernel_id, deployment.name + '-service', deployment.name, [ (2000, 'repl-in'), (2001, 'repl-out') ], service_type='ClusterIP')
        expose_service = Service(kernel_id, deployment.name + '-nodeport', deployment.name, [ (port, f'{kernel_id}-svc-{index}') for port, index in zip(exposed_ports, range(1, len(exposed_ports) + 1)) ], service_type='NodePort')
        log.debug('container config: {!r}', deployment.to_dict())
        log.debug('nodeport config: {!r}', expose_service.to_dict())
        # We are all set! Create and start the container.

        node_ports = []
        try:
            clusterip_api_response = await self.k8sCoreApi.create_namespaced_service('backend-ai', body=repl_service.to_dict())
        except:
            raise
        
        if len(exposed_ports) > 0:
            try:    
                nodeport_api_response = await self.k8sCoreApi.create_namespaced_service('backend-ai', body=expose_service.to_dict())
                node_ports = nodeport_api_response.spec.ports
            except:
                await self.k8sCoreApi.delete_namespaced_service(repl_service.name, 'backend-ai')
                raise
        try:
            await self.k8sCoreApi.create_namespaced_config_map('backend-ai', body=configmap.to_dict(), pretty='pretty_example')
        except:
            await self.k8sCoreApi.delete_namespaced_service(repl_service.name, 'backend-ai')
            await self.k8sCoreApi.delete_namespaced_service(expose_service.name, 'backend-ai')
            raise
        try:
            await self.k8sAppsApi.create_namespaced_deployment('backend-ai', body=deployment.to_dict(), pretty='pretty_example')
        except:
            await self.k8sCoreApi.delete_namespaced_service(repl_service.name, 'backend-ai')
            await self.k8sCoreApi.delete_namespaced_service(expose_service.name, 'backend-ai')
            await self.k8sCoreApi.delete_namespaced_config_map(configmap.name, 'backend-ai')
            raise

        stdin_port = 0
        stdout_port = 0
        repl_in_port = 2000
        repl_out_port = 2001

        exposed_ports = {}

        node_external_ips = [x['ExternalIP'] for x in list(filter(lambda x: 'ExternalIP' in x, self.workers.values()))]
        # if len(node_external_ips) > 0:
        #     target_node_ip = random.choice(node_external_ips)
        # else:
        #     target_node_ip = random.choice([x['InternalIP'] for x in self.workers.values()])
        target_node_ip = random.choice([x['InternalIP'] for x in self.workers.values()])

        for port in node_ports:
            exposed_ports[port.port] = port.node_port
        
        for k in service_ports.keys():
            service_ports[k]['host_port'] = exposed_ports[k]

        self.container_registry[kernel_id] = {
            'deployment_name': deployment.name,
            'lang': image_ref,
            'version': version,
            'kernel_host': target_node_ip,
            'cluster_ip': clusterip_api_response.spec.cluster_ip,
            'repl_in_port': repl_in_port,
            'repl_out_port': repl_out_port,
            'stdin_port': stdin_port,    # legacy
            'stdout_port': stdout_port,  # legacy
            'service_ports': list(service_ports.values()),
            'host_ports': exposed_ports.values(),
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
            'kernel_host': target_node_ip,
            'repl_in_port': repl_in_port,
            'repl_out_port': repl_out_port,
            'stdin_port': stdin_port,    # legacy
            'stdout_port': stdout_port,  # legacy
            'service_ports': list(service_ports.values()),
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
            await self.k8sCoreApi.delete_namespaced_service(f'{deployment_name}-nodeport', 'backend-ai')
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

    async def _interrupt_kernel(self, kernel_id):
        runner = await self._ensure_runner(kernel_id)
        await runner.feed_interrupt()
        return {'status': 'finished'}

    async def _get_completions(self, kernel_id, text, opts):
        runner = await self._ensure_runner(kernel_id)
        result = await runner.feed_and_get_completion(text, opts)
        return {'status': 'finished', 'completions': result}

    async def _list_files(self, kernel_id: str, path: str):
        deployment_name = self.container_registry[kernel_id]['deployment_name']
        pods = await self.k8sCoreApi.list_namespaced_pod('backend-ai', label_selector=f'run={deployment_name}')
        pod_name = pods.items[0].metadata.name
        
        code = '''from pathlib import Path
abspath = Path('%(path)s').resolve(strict=True)
suffix = '/' if abspath.is_dir() else ''
print(str(abspath) + suffix)
''' % {'path': path}
        abspath = await self.k8sCoreApi.connect_get_namespaced_pod_exec(pod_name, 'backend-ai', command=f'python -c "{code}"')

        if not abspath.startswith('/home/work'):
            return { 'files': '', 'errors': 'No such file or directory' }
        
        code = '''import json
import os
import stat

files = []
for f in os.scandir('%(path)s'):
    fstat = f.stat()
    ctime = fstat.st_ctime  # TODO: way to get concrete create time?
    mtime = fstat.st_mtime
    atime = fstat.st_atime
    files.append({
        'mode': stat.filemode(fstat.st_mode),
        'size': fstat.st_size,
        'ctime': ctime,
        'mtime': mtime,
        'atime': atime,
        'filename': f.name,
    })
print(json.dumps(files))''' % {'path': path}
        outs = await self.k8sCoreApi.connect_get_namespaced_pod_exec(pod_name, 'backend-ai', command=f'python -c "{code}"')

        return { 'files': outs, 'errors': '', 'abspath': abspath }

    async def _accept_file(self, kernel_id, filename, filedata):
        pass
        # TODO: implement
    async def _download_file(self, kernel_id, filepath):
        pass
        # TODO: implement

    async def _get_logs(self, kernel_id):
        try:
            kernel_info = self.container_registry[kernel_id]
        except KeyError:
            raise RuntimeError(f'The container for kernel {kernel_id} is not found! ')
        deployment_name = kernel_info['deployment_name']
        pods = await self.k8sCoreApi.list_namespaced_pod('backend-ai', label_selector=f'run={deployment_name}')
        if len(pods.items) == 0:
            return { 'logs': '' }
        pod_name = pods.items[0].metadata.name
        logs = await self.k8sCoreApi.read_namespaced_pod_log(pod_name, 'backend-ai')
        return { 'logs': logs }
    
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
            self.container_registry[kernel_id]['runner_tasks'].add(myself)

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
            runner_tasks = utils.nmget(self.container_registry, f'{kernel_id}/runner_tasks', None, '/')
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


    async def _start_service(self, kernel_id, service, opts):
        runner = await self._ensure_runner(kernel_id)
        service_ports = self.container_registry[kernel_id]['service_ports']
        for sport in service_ports:
            if sport['name'] == service:
                break
        else:
            return {'status': 'failed', 'error': 'invalid service name'}
        result = await runner.feed_start_service({
            'name': service,
            'port': sport['container_port'],
            'protocol': sport['protocol'],
            'options': opts,
        })
        return result

    async def clean_runner(self, kernel_id):
        if kernel_id not in self.container_registry:
            return
        log.debug('interrupting & cleaning up runner for {0}', kernel_id)
        item = self.container_registry[kernel_id]
        tasks = item['runner_tasks'].copy()
        for t in tasks:  # noqa: F402
            if not t.done():
                t.cancel()
                await t
        runner = item.pop('runner', None)
        if runner is not None:
            await runner.close()

    async def clean_all_kernels(self, blocking=False):
        log.info('cleaning all kernels...')
        kernel_ids = tuple(self.container_registry.keys())
        tasks = []
        if blocking:
            for kernel_id in kernel_ids:
                self.blocking_cleans[kernel_id] = asyncio.Event()
        for kernel_id in kernel_ids:
            task = asyncio.ensure_future(
                self._destroy_kernel(kernel_id, 'agent-termination'))
            tasks.append(task)
        await asyncio.gather(*tasks)
        if blocking:
            waiters = [self.blocking_cleans[kernel_id].wait()
                       for kernel_id in kernel_ids]
            await asyncio.gather(*waiters)
            for kernel_id in kernel_ids:
                self.blocking_cleans.pop(kernel_id, None)

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

    nfs_mount_path = config['baistatic']['mounted-at']
    await prepare_krunner_env('alpine3.8', nfs_mount_path)
    await prepare_krunner_env('ubuntu16.04', nfs_mount_path)
    await prepare_krunner_env('centos7.6', nfs_mount_path)
    await prepare_kernel_statics(nfs_mount_path)

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
            t.Key('kernel-gid', default=-1): tx.UserID,
            t.Key('kernel-host', default=''): t.String(allow_blank=True),
            t.Key('port-range', default=(30000, 31000)): tx.PortRange,
            t.Key('sandbox-type'): t.Enum('docker', 'jail'),
            t.Key('jail-args', default=[]): t.List(t.String),
            t.Key('scratch-type'): t.Enum('hostdir', 'memory'),
            t.Key('scratch-root', default='./scratches'): tx.Path(type='dir', auto_create=True),
            t.Key('scratch-size', default='0'): tx.BinarySize,
        }).allow_extra('*'),
        t.Key('registry'): t.Dict({
            t.Key('type'): t.String
        }).allow_extra('*'),
        t.Key('baistatic'): t.Null | t.Dict({
            t.Key('nfs-addr'): t.String,
            t.Key('path'): t.String,
            t.Key('capacity'): t.Int,
            t.Key('options'): t.Null | t.String,
            t.Key('mounted-at'): t.String
        }),
        t.Key('vfolder-pv'): t.Null | t.Dict({
            t.Key('nfs-addr'): t.String,
            t.Key('path'): t.String,
            t.Key('capacity'): t.Int,
            t.Key('options'): t.Null | t.String
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
    registry_local_config_iv = t.Dict({
        t.Key('type'): t.String,
        t.Key('addr'): tx.HostPortPair()
    })

    registry_ecr_config_iv = t.Dict({
        t.Key('type'): t.String,
        t.Key('profile'): t.String,
        t.Key('registry-id'): t.String
    })

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

        registry_target_config_iv = None
        
        if cfg['registry']['type'] == 'local':
            registry_target_config_iv = registry_local_config_iv
        elif cfg['registry']['type'] == 'ecr':
            registry_target_config_iv = registry_ecr_config_iv
        else:
            print('Validation of agent configuration has failed: registry type {} not supported'.format(cfg['registry']['type']), file=sys.stderr)
            raise click.Abort()
        
        registry_cfg = config.check(cfg['registry'], registry_target_config_iv)
        cfg['registry'] = registry_cfg
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
