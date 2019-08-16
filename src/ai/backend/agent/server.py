import asyncio
import functools
from ipaddress import ip_network, _BaseAddress as BaseIPAddress
import logging, logging.config
import os, os.path
from pathlib import Path
import pkg_resources
from pprint import pformat, pprint
import signal
import sys
import time
from typing import Collection

from aiodocker.docker import Docker, DockerContainer
from aiodocker.exceptions import DockerError
import aiohttp
import aioredis
import aiotools
import aiozmq, aiozmq.rpc
from async_timeout import timeout
import attr
import click
from setproctitle import setproctitle
import snappy
import trafaret as t
import uvloop
import zmq
import zmq.asyncio

from ai.backend.common import config, utils, identity, msgpack
from ai.backend.common import validators as tx
from ai.backend.common.docker import ImageRef
from ai.backend.common.etcd import AsyncEtcd, ConfigScopes
from ai.backend.common.logging import Logger, BraceStyleAdapter
from ai.backend.common.monitor import DummyStatsMonitor, DummyErrorMonitor
from ai.backend.common.plugin import install_plugins
from ai.backend.common.types import (
    HostPortPair,
    ResourceSlot,
    MountPermission,
    MountTypes,
)
from . import __version__ as VERSION
from .exception import InitializationError, InsufficientResource
from .stats import (
    StatContext, StatModes,
    spawn_stat_synchronizer, StatSyncState,
)
from .resources import (
    AbstractComputeDevice,
    AbstractComputePlugin,
    Mount,
    AbstractAllocMap,
)
from .kernel import KernelFeatures
from .utils import (
    current_loop, update_nested_dict,
    get_kernel_id_from_container,
    host_pid_to_container_pid,
    container_pid_to_host_pid,
    get_subnet_ip,
)
from .fs import create_scratch_filesystem, destroy_scratch_filesystem

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.server'))

max_upload_size = 100 * 1024 * 1024  # 100 MB
ipc_base_path = Path('/tmp/backend.ai/ipc')

redis_config_iv = t.Dict({
    t.Key('addr', default=('127.0.0.1', 6379)): tx.HostPortPair,
    t.Key('password', default=None): t.Null | t.String,
})


@attr.s(auto_attribs=True, slots=True)
class VolumeInfo:
    name: str             # volume name
    container_path: str   # in-container path as str
    mode: str             # 'rw', 'ro', 'rwm'


@attr.s(auto_attribs=True, slots=True)
class RestartTracker:
    request_lock: asyncio.Lock
    destroy_event: asyncio.Event
    done_event: asyncio.Event


@attr.s(auto_attribs=True, slots=True)
class ComputerContext:
    klass: AbstractComputePlugin
    devices: Collection[AbstractComputeDevice]
    alloc_map: AbstractAllocMap


deeplearning_image_keys = {
    'tensorflow', 'caffe',
    'keras', 'torch',
    'mxnet', 'theano',
}

deeplearning_sample_volume = VolumeInfo(
    'deeplearning-samples', '/home/work/samples', 'ro',
)


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


async def get_env_cid(container: DockerContainer):
    if 'Name' not in container._container:
        await container.show()
    name = container['Name']
    if name.startswith('kernel-env.'):
        return name[11:], container._id
    return None, None


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
            kernel_info = self.agent.container_registry[kernel_id]
            kernel_info['last_used'] = time.monotonic()
        except KeyError:
            pass
        return await meth(self, kernel_id, *args, **kwargs)
    return _inner


class AbstractAgentServer:
    async def init(self):
        pass
    
    async def create_kernel(self, kernel_id: str, config: dict) -> dict:
        pass

    async def destroy_kernel(self, kernel_id: str):
        pass

    async def interrupt_kernel(self, kernel_id: str):
        pass

    async def get_completeions(self, kernel_id: str, text: dict, opts: dict):
        pass

    async def get_logs(self, kernel_id: str):
        pass
    
    async def execute(self, api_version: int, kernel_id: str,
                     run_id: t.String | t.Null, mode: str, code: str, 
                     opts: dict, flush_timeout: t.Float | t.Null) -> dict:
        pass

    async def start_service(slef, kernel_id: str, service: str, opts: dict):
        pass

    async def accept_file(self, kernel_id: str, filename: str, filedata: bytes):
        pass

    async def download_file(self, kernel_id: str, filepath: str):
        pass

    async def list_files(self, kernel_id: str, path: str):
        pass

    async def shutdown(self, stop_signal):
        pass

class AgentRPCServer(aiozmq.rpc.AttrHandler):

    __slots__ = (
        'loop',
        'docker', 'container_registry', 'container_cpu_map',
        'agent_sockpath', 'agent_sock_task',
        'redis_stat_pool',
        'etcd', 'config', 'slots', 'images',
        'rpc_server', 'event_sock',
        'monitor_fetch_task', 'monitor_handle_task',
        'stat_ctx', 'live_stat_timer',
        'stat_sync_states', 'stat_sync_task',
        'hb_timer', 'clean_timer',
        'stats_monitor', 'error_monitor',
        'restarting_kernels', 'blocking_cleans',
    )

    def __init__(self, etcd, config, loop=None):
        self.etcd = etcd 
        self.config = config

        self.agent = None
        self.rpc_server = None

    async def init(self, *, skip_detect_manager=False):
        # Start serving requests.
        await self.update_status('starting')

        if not skip_detect_manager:
            await self.detect_manager()

        await self.read_agent_config()
        
        if self.config['agent']['mode'] == 'docker':
            from .docker.server import AgentServer
            self.agent = AgentServer(self.etcd, self.config, loop=None)
        else:
            from .k8s.server import AgentServer
            self.agent = AgentServer(self.config, loop=None)

        rpc_addr = self.config['agent']['rpc-listen-addr']
        agent_addr = f"tcp://{rpc_addr}"
        self.rpc_server = await aiozmq.rpc.serve_rpc(self, bind=agent_addr)
        self.rpc_server.transport.setsockopt(zmq.LINGER, 200)
        log.info('started handling RPC requests at {}', rpc_addr)

        await self.etcd.put('ip', rpc_addr.host, scope=ConfigScopes.NODE)

        await self.agent.init()
        await self.update_status('running')

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
    

    async def shutdown(self, stop_signal):
        # Stop receiving further requests.
        if self.rpc_server is not None:
            self.rpc_server.close()
            await self.rpc_server.wait_closed()

        await self.agent.shutdown(stop_signal)
    
    async def update_status(self, status):
        await self.etcd.put('', status, scope=ConfigScopes.NODE)
        
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
            return await self.agent.create_kernel(kernel_id, config)

    @aiozmq.rpc.method
    @update_last_used
    async def destroy_kernel(self, kernel_id: str):
        log.debug('rpc::destroy_kernel({0})', kernel_id)
        async with self.handle_rpc_exception():
            return await self.agent.destroy_kernel(kernel_id, 'user-requested')

    @aiozmq.rpc.method
    @update_last_used
    async def interrupt_kernel(self, kernel_id: str):
        log.debug('rpc::interrupt_kernel({0})', kernel_id)
        async with self.handle_rpc_exception():
            await self.agent.interrupt_kernel(kernel_id)

    @aiozmq.rpc.method
    @update_last_used
    async def get_completions(self, kernel_id: str,
                              text: str, opts: dict):
        log.debug('rpc::get_completions({0})', kernel_id)
        async with self.handle_rpc_exception():
            await self.agent.get_completions(kernel_id, text, opts)

    @aiozmq.rpc.method
    @update_last_used
    async def get_logs(self, kernel_id: str):
        log.debug('rpc::get_logs({0})', kernel_id)
        async with self.handle_rpc_exception():
            return await self.agent.get_logs(kernel_id)

    @aiozmq.rpc.method
    @update_last_used
    async def restart_kernel(self, kernel_id: str, new_config: dict):
        log.debug('rpc::restart_kernel({0})', kernel_id)
        async with self.handle_rpc_exception():
            tracker = self.restarting_kernels.get(kernel_id)
            if tracker is None:
                tracker = RestartTracker(
                    request_lock=asyncio.Lock(),
                    destroy_event=asyncio.Event(),
                    done_event=asyncio.Event())
            async with tracker.request_lock:
                self.restarting_kernels[kernel_id] = tracker
                await self.agent.destroy_kernel(kernel_id, 'restarting')
                # clean_kernel() will set tracker.destroy_event
                try:
                    with timeout(30):
                        await tracker.destroy_event.wait()
                except asyncio.TimeoutError:
                    log.warning('timeout detected while restarting kernel {0}!',
                                kernel_id)
                    self.restarting_kernels.pop(kernel_id, None)
                    asyncio.ensure_future(self.clean_kernel(kernel_id))
                    raise
                else:
                    tracker.destroy_event.clear()
                    await self.agent.create_kernel(
                        kernel_id, new_config,
                        restarting=True)
                    self.restarting_kernels.pop(kernel_id, None)
            tracker.done_event.set()
            kernel_info = self.container_registry[kernel_id]
            return {
                'container_id': kernel_info['container_id'],
                'repl_in_port': kernel_info['repl_in_port'],
                'repl_out_port': kernel_info['repl_out_port'],
                'stdin_port': kernel_info['stdin_port'],
                'stdout_port': kernel_info['stdout_port'],
                'service_ports': kernel_info['service_ports'],
            }

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
            result = await self.agent.execute(api_version, kernel_id,
                                         run_id, mode, code, opts,
                                         flush_timeout)
            return result

    @aiozmq.rpc.method
    @update_last_used
    async def start_service(self, kernel_id: str, service: str, opts: dict):
        log.debug('rpc::start_service({0}, {1})', kernel_id, service)
        async with self.handle_rpc_exception():
            return await self.agent.start_service(kernel_id, service, opts)

    @aiozmq.rpc.method
    @update_last_used
    async def upload_file(self, kernel_id: str, filename: str, filedata: bytes):
        log.debug('rpc::upload_file({0}, {1})', kernel_id, filename)
        async with self.handle_rpc_exception():
            await self.agent.accept_file(kernel_id, filename, filedata)

    @aiozmq.rpc.method
    @update_last_used
    async def download_file(self, kernel_id: str, filepath: str):
        log.debug('rpc::download_file({0}, {1})', kernel_id, filepath)
        async with self.handle_rpc_exception():
            return await self.agent.download_file(kernel_id, filepath)

    @aiozmq.rpc.method
    @update_last_used
    async def list_files(self, kernel_id: str, path: str):
        log.debug('rpc::list_files({0}, {1})', kernel_id, path)
        async with self.handle_rpc_exception():
            return await self.agent.list_files(kernel_id, path)

    @aiozmq.rpc.method
    @update_last_used
    async def refresh_idle(self, kernel_id: str):
        # update_last_used decorator already implements this. :)
        log.debug('rpc::refresh_idle()')
        pass

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
                        self.agent.destroy_kernel(kernel_id, 'agent-reset'))
                    tasks.append(task)
                except Exception:
                    self.error_monitor.capture_exception()
                    log.exception('reset: destroying {0}', kernel_id)
            await asyncio.gather(*tasks)


@aiotools.server
async def server_main(loop, pidx, _args):
    config = _args[0]

    log.info('Preparing kernel runner environments...')
    if config['agent']['mode'] == 'docker':
        from .docker.kernel import prepare_krunner_env
        await asyncio.gather(
            prepare_krunner_env('alpine3.8'),
            prepare_krunner_env('ubuntu16.04'),
            prepare_krunner_env('centos7.6'),
            return_exceptions=True,
        )
    else:
        from .k8s.kernel import prepare_krunner_env
        nfs_mount_path = config['baistatic']['mounted-at']
        await asyncio.gather(
            prepare_krunner_env('alpine3.8', nfs_mount_path),
            prepare_krunner_env('ubuntu16.04', nfs_mount_path),
            prepare_krunner_env('centos7.6', nfs_mount_path),
            return_exceptions=True,
        )

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

    if not config['agent']['id']:
        config['agent']['id'] = await identity.get_instance_id()
    if not config['agent']['instance-type']:
        config['agent']['instance-type'] = await identity.get_instance_type()
    rpc_addr = config['agent']['rpc-listen-addr']
    if not rpc_addr.host:
        subnet_hint = await etcd.get('config/network/subnet/agent')
        if subnet_hint is not None:
            subnet_hint = ip_network(subnet_hint)
        log.debug('auto-detecting agent host')
        config['agent']['rpc-listen-addr'] = HostPortPair(
            await identity.get_instance_ip(subnet_hint),
            rpc_addr.port,
        )
    if not config['container']['kernel-host']:
        log.debug('auto-detecting kernel host')
        config['container']['kernel-host'] = await get_subnet_ip(
            etcd, 'container', config['agent']['rpc-listen-addr'].host
        )
    log.info('Agent external IP: {}', config['agent']['rpc-listen-addr'].host)
    log.info('Container external IP: {}', config['container']['kernel-host'])
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
    
    initial_config_iv = t.Dict({
        t.Key('agent'): t.Dict({
            t.Key('mode'): t.Enum('docker', 'k8s'),
            t.Key('rpc-listen-addr', default=('', 6001)): tx.HostPortPair(allow_blank_host=True),
            t.Key('id', default=None): t.Null | t.String,
            t.Key('region', default=None): t.Null | t.String,
            t.Key('instance-type', default=None): t.Null | t.String,
            t.Key('scaling-group', default='default'): t.String,
            t.Key('pid-file', default=os.devnull): tx.Path(type='file',
                                                           allow_nonexisting=True,
                                                           allow_devnull=True),
            t.Key('event-loop', default='asyncio'): t.Enum('asyncio', 'uvloop'),
        }).allow_extra('*'),
        t.Key('container'): t.Dict({
            t.Key('kernel-uid', default=-1): tx.UserID,
            t.Key('kernel-gid', default=-1): tx.GroupID,
            t.Key('kernel-host', default=''): t.String(allow_blank=True),
            t.Key('port-range', default=(30000, 31000)): tx.PortRange,
            t.Key('stats-type', default='docker'): t.Null | t.Enum(*[e.value for e in StatModes]),
            t.Key('sandbox-type', default='docker'): t.Enum('docker', 'jail'),
            t.Key('jail-args', default=[]): t.List(t.String),
            t.Key('scratch-type'): t.Enum('hostdir', 'memory'),
            t.Key('scratch-root', default='./scratches'): tx.Path(type='dir', auto_create=True),
            t.Key('scratch-size', default='0'): tx.BinarySize,
        }).allow_extra('*'),
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

    k8s_extra_config_iv = t.Dict({
        t.Key('registry'): t.Dict({
            t.Key('type'): t.String
        }).allow_extra('*'),
        t.Key('baistatic'): t.Null | t.Dict({
            t.Key('nfs-addr'): t.String,
            t.Key('path'): t.String,
            t.Key('capacity'): tx.BinarySize,
            t.Key('options'): t.Null | t.String,
            t.Key('mounted-at'): t.String
        }),
        t.Key('vfolder-pv'): t.Null | t.Dict({
            t.Key('nfs-addr'): t.String,
            t.Key('path'): t.String,
            t.Key('capacity'): tx.BinarySize,
            t.Key('options'): t.Null | t.String
        }),
    }).merge(initial_config_iv).allow_extra('*')

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
        cfg = config.check(raw_cfg, initial_config_iv)
        if cfg['agent']['mode'] == 'k8s':
            cfg = config.check(raw_cfg, k8s_extra_config_iv)
            if cfg['registry']['type'] == 'local':
                registry_target_config_iv = registry_local_config_iv
            elif cfg['registry']['type'] == 'ecr':
                registry_target_config_iv = registry_ecr_config_iv
            else:
                print('Validation of agent configuration has failed: registry type {} not supported'.format(cfg['registry']['type']), file=sys.stderr)
                raise click.Abort()
            
            registry_cfg = config.check(cfg['registry'], registry_target_config_iv)
            cfg['registry'] = registry_cfg
    
        if 'debug' in cfg and cfg['debug']['enabled']:
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

    if os.getuid() != 0 and cfg['container']['stats-type'] == 'cgroup':
        print('Cannot use cgroup statistics collection mode unless the agent runs as root.',
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

                if cfg['agent']['event-loop'] == 'uvloop':
                    uvloop.install()
                    log.info('Using uvloop as the event loop backend')
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


if __name__ == '__main__':
    sys.exit(main())
