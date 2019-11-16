import asyncio
import functools
from ipaddress import ip_network, _BaseAddress as BaseIPAddress
import logging, logging.config
import os, os.path
from pathlib import Path
from pprint import pformat, pprint
import signal
import sys
import time
from typing import cast, TYPE_CHECKING
from uuid import UUID

import aiotools
from aiotools import aclosing
import aiozmq, aiozmq.rpc
import click
from setproctitle import setproctitle
import trafaret as t
import zmq
import zmq.asyncio

from ai.backend.common import config, utils, identity
from ai.backend.common import validators as tx
from ai.backend.common.etcd import AsyncEtcd, ConfigScopes
from ai.backend.common.logging import Logger, BraceStyleAdapter
from ai.backend.common.monitor import DummyStatsMonitor, DummyErrorMonitor
from ai.backend.common.plugin import install_plugins
from ai.backend.common.types import (
    aobject, HostPortPair, KernelId,
    KernelCreationConfig,
)
from . import __version__ as VERSION
from .agent import AbstractAgent, VolumeInfo
from .config import (
    initial_config_iv,
    k8s_extra_config_iv,
    registry_local_config_iv,
    registry_ecr_config_iv,
)
from .utils import current_loop, get_subnet_ip

if TYPE_CHECKING:
    from typing import Any, Dict, Optional  # noqa

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.server'))

redis_config_iv = t.Dict({
    t.Key('addr', default=('127.0.0.1', 6379)): tx.HostPortPair,
    t.Key('password', default=None): t.Null | t.String,
})

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


def update_last_used(meth):
    @functools.wraps(meth)
    async def _inner(self, raw_kernel_id: str, *args, **kwargs):
        try:
            kernel_obj = self.agent.kernel_registry[KernelId(UUID(raw_kernel_id))]
            kernel_obj.last_used = time.monotonic()
        except KeyError:
            pass
        return await meth(self, raw_kernel_id, *args, **kwargs)
    return _inner


class AgentRPCServer(aiozmq.rpc.AttrHandler, aobject):

    loop: asyncio.AbstractEventLoop
    agent: AbstractAgent
    rpc_addr: str
    agent_addr: str

    def __init__(self, etcd, config, *, skip_detect_manager: bool = False) -> None:
        self.loop = current_loop()
        self.etcd = etcd
        self.config = config
        self.skip_detect_manager = skip_detect_manager
        self.stats_monitor = DummyStatsMonitor()
        self.error_monitor = DummyErrorMonitor()
        plugins = [
            'stats_monitor',
            'error_monitor'
        ]
        install_plugins(plugins, self, 'attr', self.config)

    async def __ainit__(self) -> None:
        # Start serving requests.
        await self.update_status('starting')

        if not self.skip_detect_manager:
            await self.detect_manager()

        await self.read_agent_config()

        if self.config['agent']['mode'] == 'docker':
            from .docker.agent import DockerAgent
            self.agent = await DockerAgent.new(self.config)
        else:
            from .k8s.agent import K8sAgent
            self.agent = await K8sAgent.new(self.config)

        rpc_addr = self.config['agent']['rpc-listen-addr']
        agent_addr = f"tcp://{rpc_addr}"
        self.rpc_server = await aiozmq.rpc.serve_rpc(self, bind=agent_addr)
        self.rpc_server.transport.setsockopt(zmq.LINGER, 200)
        log.info('started handling RPC requests at {}', rpc_addr)

        await self.etcd.put('ip', rpc_addr.host, scope=ConfigScopes.NODE)
        watcher_port = utils.nmget(self.config, 'watcher.service-addr.port', None)
        if watcher_port is not None:
            await self.etcd.put('watcher_port', watcher_port, scope=ConfigScopes.NODE)

        await self.update_status('running')

    async def detect_manager(self):
        log.info('detecting the manager...')
        manager_id = await self.etcd.get('nodes/manager')
        if manager_id is None:
            log.warning('watching etcd to wait for the manager being available')
            async with aclosing(self.etcd.watch('nodes/manager')) as agen:
                async for ev in agen:
                    if ev.event == 'put':
                        manager_id = ev.value
                        break
        log.info('detecting the manager: OK ({0})', manager_id)

    async def read_agent_config(self):
        # Fill up runtime configurations from etcd.
        redis_config = await self.etcd.get_prefix('config/redis')
        self.config['redis'] = redis_config_iv.check(redis_config)
        log.info('configured redis_addr: {0}', self.config['redis']['addr'])

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

    async def shutdown(self, stop_signal: signal.Signals):
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
        except (asyncio.CancelledError, asyncio.TimeoutError):
            raise
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
    async def create_kernel(self, kernel_id: str, config: dict):
        log.info('rpc::create_kernel(k:{0}, img:{1})',
                 kernel_id, config['image']['canonical'])
        async with self.handle_rpc_exception():
            return await self.agent.create_kernel(
                KernelId(UUID(kernel_id)), cast(KernelCreationConfig, config))

    @aiozmq.rpc.method
    @update_last_used
    async def destroy_kernel(self, kernel_id: str, reason: str = None):
        log.info('rpc::destroy_kernel(k:{0})', kernel_id)
        async with self.handle_rpc_exception():
            return await self.agent.destroy_kernel(
                KernelId(UUID(kernel_id)), reason or 'user-requested')

    @aiozmq.rpc.method
    @update_last_used
    async def interrupt_kernel(self, kernel_id: str):
        log.info('rpc::interrupt_kernel(k:{0})', kernel_id)
        async with self.handle_rpc_exception():
            await self.agent.interrupt_kernel(KernelId(UUID(kernel_id)))

    @aiozmq.rpc.method
    @update_last_used
    async def get_completions(self, kernel_id: str,
                              text: str, opts: dict):
        log.debug('rpc::get_completions(k:{0}, ...)', kernel_id)
        async with self.handle_rpc_exception():
            await self.agent.get_completions(KernelId(UUID(kernel_id)), text, opts)

    @aiozmq.rpc.method
    @update_last_used
    async def get_logs(self, kernel_id: str):
        log.info('rpc::get_logs(k:{0})', kernel_id)
        async with self.handle_rpc_exception():
            return await self.agent.get_logs(KernelId(UUID(kernel_id)))

    @aiozmq.rpc.method
    @update_last_used
    async def restart_kernel(self, kernel_id: str, new_config: dict):
        log.info('rpc::restart_kernel(k:{0})', kernel_id)
        async with self.handle_rpc_exception():
            return await self.agent.restart_kernel(
                KernelId(UUID(kernel_id)), cast(KernelCreationConfig, new_config))

    @aiozmq.rpc.method
    @update_last_used
    async def execute(self,
                      kernel_id,          # type: str
                      api_version,        # type: int
                      run_id,             # type: str
                      mode,               # type: str
                      code,               # type: str
                      opts,               # type: Dict[str, Any]
                      flush_timeout,      # type: float
                      ):
        # type: (...) -> Dict[str, Any]
        _log = log.debug if mode == 'continue' else log.info
        _log('rpc::execute(k:{0}, run-id:{1}, mode:{2}, code:{3!r})',
             kernel_id, run_id, mode,
             code[:20] + '...' if len(code) > 20 else code)
        async with self.handle_rpc_exception():
            result = await self.agent.execute(
                KernelId(UUID(kernel_id)),
                run_id, mode, code,
                opts=opts,
                api_version=api_version,
                flush_timeout=flush_timeout
            )
            return result

    @aiozmq.rpc.method
    @update_last_used
    async def start_service(self,
                            kernel_id,   # type: str
                            service,     # type: str
                            opts         # type: Dict[str, Any]
                            ):
        # type: (...) -> Dict[str, Any]
        log.info('rpc::start_service(k:{0}, app:{1})', kernel_id, service)
        async with self.handle_rpc_exception():
            return await self.agent.start_service(KernelId(UUID(kernel_id)), service, opts)

    @aiozmq.rpc.method
    @update_last_used
    async def upload_file(self, kernel_id: str, filename: str, filedata: bytes):
        log.info('rpc::upload_file(k:{0}, fn:{1})', kernel_id, filename)
        async with self.handle_rpc_exception():
            await self.agent.accept_file(KernelId(UUID(kernel_id)), filename, filedata)

    @aiozmq.rpc.method
    @update_last_used
    async def download_file(self, kernel_id: str, filepath: str):
        log.info('rpc::download_file(k:{0}, fn:{1})', kernel_id, filepath)
        async with self.handle_rpc_exception():
            return await self.agent.download_file(KernelId(UUID(kernel_id)), filepath)

    @aiozmq.rpc.method
    @update_last_used
    async def list_files(self, kernel_id: str, path: str):
        log.info('rpc::list_files(k:{0}, fn:{1})', kernel_id, path)
        async with self.handle_rpc_exception():
            return await self.agent.list_files(KernelId(UUID(kernel_id)), path)

    @aiozmq.rpc.method
    @update_last_used
    async def refresh_idle(self, kernel_id: str):
        # update_last_used decorator already implements this. :)
        log.debug('rpc::refresh_idle(k:{})', kernel_id)
        pass

    @aiozmq.rpc.method
    async def shutdown_agent(self, terminate_kernels: bool):
        # TODO: implement
        log.info('rpc::shutdown_agent()')
        pass

    @aiozmq.rpc.method
    async def reset_agent(self):
        log.debug('rpc::reset()')
        async with self.handle_rpc_exception():
            kernel_ids = tuple(self.agent.kernel_registry.keys())
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
async def server_main_logwrapper(loop, pidx, _args):
    setproctitle(f"backend.ai: agent worker-{pidx}")
    log_endpoint = _args[1]
    logger = Logger(_args[0]['logging'], is_master=False, log_endpoint=log_endpoint)
    with logger:
        async with server_main(loop, pidx, _args):
            yield


@aiotools.server
async def server_main(loop, pidx, _args):
    config = _args[0]

    log.info('Preparing kernel runner environments...')
    supported_distros = ['alpine3.8', 'ubuntu16.04', 'centos7.6', 'centos6.10']
    if config['agent']['mode'] == 'docker':
        from .docker.kernel import prepare_krunner_env
        krunner_volumes = {
            k: v for k, v in zip(supported_distros, await asyncio.gather(
                *[
                    prepare_krunner_env(distro)
                    for distro in supported_distros
                ],
                return_exceptions=True,
            ))
        }
    else:
        from .k8s.kernel import prepare_krunner_env
        nfs_mount_path = config['baistatic']['mounted-at']
        krunner_volumes = {
            k: v for k, v in zip(supported_distros, await asyncio.gather(
                *[
                    prepare_krunner_env(distro, nfs_mount_path)
                    for distro in supported_distros
                ],
                return_exceptions=True,
            ))
        }
    for distro, result in krunner_volumes.items():
        if isinstance(result, Exception):
            log.error('Loading krunner for {} failed: {}', distro, result)
            raise click.Abort()
    config['container']['krunner-volumes'] = krunner_volumes

    if not config['agent']['id']:
        config['agent']['id'] = await identity.get_instance_id()
    if not config['agent']['instance-type']:
        config['agent']['instance-type'] = await identity.get_instance_type()

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

    # Pre-load compute plugin configurations.
    config['plugins'] = await etcd.get_prefix_dict('config/plugins')

    # Start RPC server.
    agent = await AgentRPCServer.new(etcd, config)

    # Run!
    try:
        stop_signal = yield
    finally:
        # Shutdown.
        log.info('shutting down...')
        try:
            await agent.shutdown(stop_signal)
        except Exception:
            log.exception('unexpected error during agent shutdown')


@click.group(invoke_without_command=True)
@click.option('-f', '--config-path', '--config', type=Path, default=None,
              help='The config file path. '
                   '(default: ./agent.conf and /etc/backend.ai/agent.conf)')
@click.option('--debug', is_flag=True,
              help='Enable the debug mode and override the global log level to DEBUG.')
@click.pass_context
def main(cli_ctx: click.Context, config_path: Path, debug: bool) -> int:

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
    config.override_with_env(raw_cfg, ('container', 'port-range'),
                             'BACKEND_CONTAINER_PORT_RANGE')
    config.override_with_env(raw_cfg, ('container', 'kernel-host'),
                             'BACKEND_KERNEL_HOST_OVERRIDE')
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
                print('Validation of agent configuration has failed: registry type {} not supported'
                    .format(cfg['registry']['type']), file=sys.stderr)
                raise click.Abort()

            registry_cfg = config.check(cfg['registry'], registry_target_config_iv)
            cfg['registry'] = registry_cfg

        if 'debug' in cfg and cfg['debug']['enabled']:
            print('== Agent configuration ==')
            pprint(cfg)
        cfg['_src'] = cfg_src_path
    except config.ConfigurationError as e:
        print('ConfigurationError: Validation of agent configuration has failed:', file=sys.stderr)
        print(pformat(e.invalid_data), file=sys.stderr)
        raise click.Abort()

    rpc_host = cfg['agent']['rpc-listen-addr'].host
    if (isinstance(rpc_host, BaseIPAddress) and
        (rpc_host.is_unspecified or rpc_host.is_link_local)):
        print('ConfigurationError: '
              'Cannot use link-local or unspecified IP address as the RPC listening host.',
              file=sys.stderr)
        raise click.Abort()

    if os.getuid() != 0 and cfg['container']['stats-type'] == 'cgroup':
        print('Cannot use cgroup statistics collection mode unless the agent runs as root.',
              file=sys.stderr)
        raise click.Abort()

    if cli_ctx.invoked_subcommand is None:

        if cfg['debug']['coredump']['enabled']:
            if not sys.platform.startswith('linux'):
                print('ConfigurationError: '
                      'Storing container coredumps is only supported in Linux.',
                      file=sys.stderr)
                raise click.Abort()
            core_pattern = Path('/proc/sys/kernel/core_pattern').read_text().strip()
            if core_pattern.startswith('|') or not core_pattern.startswith('/'):
                print('ConfigurationError: '
                      '/proc/sys/kernel/core_pattern must be an absolute path '
                      'to enable container coredumps.',
                      file=sys.stderr)
                raise click.Abort()
            cfg['debug']['coredump']['core_path'] = Path(core_pattern).parent

        cfg['agent']['pid-file'].write_text(str(os.getpid()))
        log_endpoint = f'tcp://127.0.0.1:{utils.find_free_port()}'
        cfg['logging']['endpoint'] = log_endpoint
        try:
            logger = Logger(cfg['logging'], is_master=True, log_endpoint=log_endpoint)
            with logger:
                ns = cfg['etcd']['namespace']
                setproctitle(f"backend.ai: agent {ns}")
                log.info('Backend.AI Agent {0}', VERSION)
                log.info('runtime: {0}', utils.env_info())

                log_config = logging.getLogger('ai.backend.agent.config')
                if debug:
                    log_config.debug('debug mode enabled.')

                if cfg['agent']['event-loop'] == 'uvloop':
                    import uvloop
                    uvloop.install()
                    log.info('Using uvloop as the event loop backend')
                aiotools.start_server(server_main_logwrapper,
                                      num_workers=1,
                                      use_threading=True, args=(cfg, log_endpoint))
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
