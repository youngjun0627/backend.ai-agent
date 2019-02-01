import asyncio
import base64
import functools
from ipaddress import ip_address
import logging, logging.config
import os, os.path
from pathlib import Path
import platform
from pprint import pformat
import pkg_resources
import pwd
import secrets
import shlex
import signal
import shutil
import subprocess
import time
import sys
from typing import Collection, Mapping

from aiodocker.docker import Docker, DockerContainer
from aiodocker.exceptions import DockerError
import aiohttp
import aioredis
import aiotools
import aiozmq, aiozmq.rpc
from async_timeout import timeout
import attr
import configargparse
from setproctitle import setproctitle
import snappy
import trafaret as t
import uvloop
import zmq
import zmq.asyncio

from ai.backend.common import utils, identity, msgpack
from ai.backend.common.argparse import (
    port_no, port_range, HostPortPair,
    host_port_pair, non_negative_int)
from ai.backend.common.etcd import AsyncEtcd
from ai.backend.common.logging import Logger, BraceStyleAdapter
from ai.backend.common.monitor import DummyStatsMonitor, DummyErrorMonitor
from ai.backend.common.plugin import install_plugins, add_plugin_args
from ai.backend.common.types import (
    ImageRef, ResourceSlot,
    MountPermission,
)
from . import __version__ as VERSION
from .files import scandir, upload_output_files_to_s3
from .stats import (
    get_preferred_stat_type, collect_agent_live_stat,
    spawn_stat_collector, StatCollectorState,
    AgentLiveStat
)
from .resources import (
    known_slot_types,
    compute_device_types,
    AbstractComputeDevice,
    AbstractComputePlugin,
    KernelResourceSpec,
    Mount,
    bitmask2set,
    AbstractAllocMap,
    detect_slots,
)
from .kernel import KernelRunner, KernelFeatures
from .utils import update_nested_dict
from .fs import create_scratch_filesystem, destroy_scratch_filesystem

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.server'))

max_upload_size = 100 * 1024 * 1024  # 100 MB
stat_cache_lifespan = 30.0  # 30 secs


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
class ComputerSet:
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
            log.warning('could not attach volume {0} '
                        'to a kernel using language {1} ',
                        '(volume not found)',
                        vol.name, lang)
    return mount_list


async def get_env_cid(container: DockerContainer):
    if 'Name' not in container._container:
        await container.show()
    name = container['Name']
    if name.startswith('kernel-env.'):
        return name[11:], container._id
    return None, None


async def get_kernel_id_from_container(val):
    if isinstance(val, DockerContainer):
        if 'Name' not in val._container:
            await val.show()
        name = val['Name']
    elif isinstance(val, str):
        name = val
    name = name.lstrip('/')
    if not name.startswith('kernel.'):
        return None
    try:
        return name.rsplit('.', 2)[-1]
    except (IndexError, ValueError):
        return None


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


class AgentRPCServer(aiozmq.rpc.AttrHandler):

    __slots__ = (
        'loop',
        'docker', 'container_registry', 'container_cpu_map',
        'redis_stat_pool',
        'etcd', 'config', 'slots', 'images',
        'rpc_server', 'event_sock',
        'monitor_fetch_task', 'monitor_handle_task', 'stat_collector_task',
        'stat_type', 'live_stat', 'ls_timer',
        'hb_timer', 'clean_timer',
        'stats_monitor', 'error_monitor',
        'restarting_kernels', 'blocking_cleans',
    )

    def __init__(self, config, loop=None):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.config = config
        self.config.app_name = 'backend.ai-agent'
        self.etcd = None

        self.docker = Docker()
        self.container_registry = {}
        self.redis_stat_pool = None

        self.restarting_kernels = {}
        self.blocking_cleans = {}

        self.computers: Mapping[str, ComputerSet] = {}
        self.images: Mapping[str, str] = {}  # repoTag -> digest

        self.rpc_server = None
        self.event_sock = None
        self.scan_images_timer = None
        self.monitor_fetch_task = None
        self.monitor_handle_task = None
        self.hb_timer = None
        self.clean_timer = None
        self.stat_collector_task = None

        self.stat_type = get_preferred_stat_type()
        self.live_stat = None
        self.ls_timer = None

        self.port_pool = set(range(
            config.container_port_range[0],
            config.container_port_range[1] + 1,
        ))

        self.stats_monitor = DummyStatsMonitor()
        self.error_monitor = DummyErrorMonitor()

        self.runner_lock = asyncio.Lock()

        plugins = [
            'stats_monitor',
            'error_monitor'
        ]
        install_plugins(plugins, self, 'attr', self.config)


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

    async def read_etcd_configs(self):
        if not hasattr(self.config, 'redis_addr') or self.config.redis_addr is None:
            self.config.redis_addr = host_port_pair(
                await self.etcd.get('nodes/redis'))
        if not hasattr(self.config, 'event_addr') or self.config.event_addr is None:
            self.config.event_addr = host_port_pair(
                await self.etcd.get('nodes/manager/event_addr'))
        if not hasattr(self.config, 'idle_timeout') or \
                self.config.idle_timeout is None:
            idle_timeout = await self.etcd.get('nodes/idle_timeout')
            if idle_timeout is None:
                idle_timeout = 600  # default: 10 minutes
            self.config.idle_timeout = idle_timeout

        log.info('configured redis_addr: {0}', self.config.redis_addr)
        log.info('configured event_addr: {0}', self.config.event_addr)
        vfolder_mount = await self.etcd.get('volumes/_mount')
        if vfolder_mount is None:
            vfolder_mount = '/mnt'
        self.config.vfolder_mount = Path(vfolder_mount)
        vfolder_fsprefix = await self.etcd.get('volumes/_fsprefix')
        if vfolder_fsprefix is None:
            vfolder_fsprefix = ''
        self.config.vfolder_fsprefix = Path(vfolder_fsprefix.lstrip('/'))

    async def scan_running_containers(self):
        env_containers = {}

        for container in (await self.docker.containers.list()):
            kernel_id, env_container_id = await get_env_cid(container)
            if env_container_id is not None:
                env_containers[kernel_id] = env_container_id

        for container in (await self.docker.containers.list()):
            kernel_id = await get_kernel_id_from_container(container)
            if kernel_id is None:
                continue
            # NOTE: get_kernel_id_from_containers already performs .show() on
            #       the returned container objects.
            status = container['State']['Status']
            if status in {'running', 'restarting', 'paused'}:
                log.info('detected running kernel: {0}', kernel_id)
                await container.show()
                image = container['Config']['Image']
                labels = container['Config']['Labels']
                ports = container['NetworkSettings']['Ports']
                port_map = {}
                for private_port, host_ports in ports.items():
                    private_port = int(private_port.split('/')[0])
                    if host_ports is None:
                        public_port = 0
                    else:
                        public_port = int(host_ports[0]['HostPort'])
                        self.port_pool.discard(public_port)
                    port_map[private_port] = public_port
                for computer_set in self.computers.values():
                    await computer_set.klass.restore_from_container(
                        container, computer_set.alloc_map)
                if self.config.kernel_host_override:
                    kernel_host = self.config.kernel_host_override
                else:
                    kernel_host = '127.0.0.1'
                config_dir = (self.config.scratch_root /
                              kernel_id / 'config').resolve()
                with open(config_dir / 'resource.txt', 'r') as f:
                    resource_spec = KernelResourceSpec.read_from_file(f)
                service_ports = []
                for item in labels.get('ai.backend.service-ports', '').split(','):
                    if not item:
                        continue
                    service_port = parse_service_port(item)
                    service_port['host_port'] = \
                        port_map.get(service_port['container_port'], None)
                    service_ports.append(service_port)
                self.container_registry[kernel_id] = {
                    'lang': ImageRef(image),
                    'version': int(labels.get('ai.backend.kernelspec', '1')),
                    'container_id': container._id,
                    'env_container_id': env_containers.get(kernel_id),
                    'kernel_host': kernel_host,
                    'repl_in_port': port_map[2000],
                    'repl_out_port': port_map[2001],
                    'stdin_port': port_map.get(2002, 0),
                    'stdout_port': port_map.get(2003, 0),
                    'last_used': time.monotonic(),
                    'runner_tasks': set(),
                    'host_ports': [*port_map.values()],
                    'resource_spec': resource_spec,
                    'service_ports': service_ports,
                }
            elif status in {'exited', 'dead', 'removing'}:
                log.info('detected terminated kernel: {0}', kernel_id)
                await self.send_event('kernel_terminated', kernel_id,
                                      'self-terminated', None)

    async def scan_images(self, interval):
        all_images = await self.docker.images.list()
        updated_images = {}
        for image in all_images:
            if image['RepoTags'] is None:
                continue
            for repo_tag in image['RepoTags']:
                if repo_tag.endswith('<none>'):
                    continue
                img_detail = await self.docker.images.get(repo_tag)
                labels = img_detail['Config']['Labels']
                if labels and 'ai.backend.kernelspec' in labels:
                    updated_images[repo_tag] = img_detail['Id']
        for added_image in (updated_images.keys() - self.images.keys()):
            log.debug('found kernel image: {0}', added_image)
        for removed_image in (self.images.keys() - updated_images.keys()):
            log.debug('removed kernel image: {0}', removed_image)
        self.images = updated_images

    async def update_status(self, status):
        await self.etcd.put(f'nodes/agents/{self.config.instance_id}', status)

    async def deregister_myself(self):
        await self.etcd.delete_prefix(f'nodes/agents/{self.config.instance_id}')

    async def send_event(self, event_name, *args):
        if self.event_sock is None:
            return
        log.debug('send_event({0})', event_name)
        self.event_sock.write((
            event_name.encode('ascii'),
            self.config.instance_id.encode('utf8'),
            msgpack.packb(args),
        ))

    async def check_images(self):
        # Read desired image versions from etcd.
        for key, value in (await self.etcd.get_prefix('images')):
            # TODO: implement
            pass

        # If there are newer images, pull them.
        # TODO: implement

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

    async def collect_stats(self):
        context = zmq.asyncio.Context()
        stats_sock = context.socket(zmq.PULL)
        stats_sock.setsockopt(zmq.LINGER, 1000)
        stats_sock.bind(f'tcp://127.0.0.1:{self.config.stat_port}')
        log.info('collecting stats at port tcp://127.0.0.1:{0}',
                 self.config.stat_port)
        try:
            recv = functools.partial(stats_sock.recv_serialized,
                                     lambda vs: [msgpack.unpackb(v) for v in vs])
            async for msg in aiotools.aiter(lambda: recv(), None):
                cid = msg[0]['cid']
                status = msg[0]['status']
                if cid not in self.stats:
                    # If the agent has restarted, the events dict may be empty.
                    container = self.docker.containers.container(cid)
                    kernel_id = await get_kernel_id_from_container(container)
                    self.stats[cid] = StatCollectorState(kernel_id)
                self.stats[cid].last_stat = msg[0]['data']
                kernel_id = self.stats[cid].kernel_id
                pipe = self.redis_stat_pool.pipeline()
                pipe.hmset_dict(kernel_id, msg[0]['data'])
                pipe.expire(kernel_id, stat_cache_lifespan)
                await pipe.execute()
                if status == 'terminated':
                    self.stats[cid].terminated.set()
        except asyncio.CancelledError:
            pass
        finally:
            stats_sock.close()
            context.term()

    async def collect_live_stat(self, interval):
        await collect_agent_live_stat(self, self.stat_type)

    async def init(self, *, skip_detect_manager=False):
        # Show Docker version info.
        docker_version = await self.docker.version()
        log.info('running with Docker {0} with API {1}',
                 docker_version['Version'], docker_version['ApiVersion'])

        self.etcd = AsyncEtcd(self.config.etcd_addr,
                              self.config.namespace)

        # detect_slots() loads accelerator plugins and updates compute_device_types
        self.slots = await detect_slots(
            self.etcd,
            self.config.limit_cpus,
            self.config.limit_gpus)
        for name, klass in compute_device_types.items():
            devices = await klass.list_devices()
            alloc_map = await klass.create_alloc_map()
            self.computers[name] = ComputerSet(klass, devices, alloc_map)

        if not skip_detect_manager:
            await self.detect_manager()
        await self.read_etcd_configs()
        await self.update_status('starting')
        # scan_images task should be done before heartbeat,
        # so call it here although we spawn a scheduler
        # for this task below.
        await self.scan_images(None)
        await self.scan_running_containers()
        await self.check_images()

        self.redis_stat_pool = await aioredis.create_redis_pool(
            self.config.redis_addr.as_sockaddr(),
            timeout=3.0,
            encoding='utf8',
            db=0)  # REDIS_STAT_DB in backend.ai-manager

        self.event_sock = await aiozmq.create_zmq_stream(
            zmq.PUSH, connect=f'tcp://{self.config.event_addr}')
        self.event_sock.transport.setsockopt(zmq.LINGER, 50)

        # Spawn image scanner task.
        self.scan_images_timer = aiotools.create_timer(self.scan_images, 60.0)

        # Spawn stat collector task.
        self.stats = dict()
        self.stat_collector_task = self.loop.create_task(self.collect_stats())

        # Start container stats collector for existing containers.
        stat_addr = f'tcp://{self.config.agent_host}:{self.config.stat_port}'
        stat_type = get_preferred_stat_type()
        for kernel_id, info in self.container_registry.items():
            cid = info['container_id']
            self.stats[cid] = StatCollectorState(kernel_id)
            async with spawn_stat_collector(stat_addr, stat_type, cid):
                pass

        # Start collecting agent live stat.
        self.live_stat = AgentLiveStat()
        self.ls_timer = aiotools.create_timer(self.collect_live_stat, 2.0)

        # Spawn docker monitoring tasks.
        self.monitor_fetch_task  = self.loop.create_task(self.fetch_docker_events())
        self.monitor_handle_task = self.loop.create_task(self.monitor())

        # Send the first heartbeat.
        self.hb_timer    = aiotools.create_timer(self.heartbeat, 3.0)
        if self.config.idle_timeout != 0:
            # idle_timeout == 0 means there is no timeout.
            self.clean_timer = aiotools.create_timer(self.clean_old_kernels, 10.0)

        # Start serving requests.
        agent_addr = f'tcp://*:{self.config.agent_port}'
        self.rpc_server = await aiozmq.rpc.serve_rpc(self, bind=agent_addr)
        self.rpc_server.transport.setsockopt(zmq.LINGER, 200)
        log.info('serving at {0}', agent_addr)

        # Ready.
        await self.etcd.put(f'nodes/agents/{self.config.instance_id}/ip',
                            self.config.agent_host)
        await self.update_status('running')

        # Notify the gateway.
        await self.send_event('instance_started')

    async def shutdown(self, stop_signal):
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
        if self.ls_timer is not None:
            self.ls_timer.cancel()
            await self.ls_timer
        if self.clean_timer is not None:
            self.clean_timer.cancel()
            await self.clean_timer

        # Stop event monitoring.
        if self.monitor_fetch_task is not None:
            self.monitor_fetch_task.cancel()
            self.monitor_handle_task.cancel()
            await self.monitor_fetch_task
            await self.monitor_handle_task
        try:
            await self.docker.events.stop()
        except Exception:
            pass
        await self.docker.close()

        # Stop stat collector task.
        if self.stat_collector_task is not None:
            self.stat_collector_task.cancel()
            await self.stat_collector_task

        if self.redis_stat_pool is not None:
            self.redis_stat_pool.close()
            await self.redis_stat_pool.wait_closed()

        # Notify the gateway.
        if self.event_sock is not None:
            await self.send_event('instance_terminated', 'shutdown')
            self.event_sock.close()

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
    @update_last_used
    async def get_completions(self, kernel_id: str,
                              text: str, opts: dict):
        log.debug('rpc::get_completions({0})', kernel_id)
        async with self.handle_rpc_exception():
            await self._get_completions(kernel_id, text, opts)

    @aiozmq.rpc.method
    @update_last_used
    async def get_logs(self, kernel_id: str):
        log.debug('rpc::get_logs({0})', kernel_id)
        async with self.handle_rpc_exception():
            return await self._get_logs(kernel_id)

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
                await self._destroy_kernel(kernel_id, 'restarting')
                # clean_kernel() will set tracker.destroy_event
                try:
                    with timeout(30):
                        await tracker.destroy_event.wait()
                except asyncio.TimeoutError:
                    log.warning('timeout detected while restarting kernel {0}!',
                                kernel_id)
                    self.restarting_kernels.pop(kernel_id, None)
                    asyncio.ensure_future(self._clean_kernel(kernel_id))
                    raise
                else:
                    tracker.destroy_event.clear()
                    await self._create_kernel(
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
            result = await self._execute(api_version, kernel_id,
                                         run_id, mode, code, opts,
                                         flush_timeout)
            return result

    @aiozmq.rpc.method
    @update_last_used
    async def start_service(self, kernel_id: str, service: str, opts: dict):
        log.debug('rpc::start_service({0}, {1})', kernel_id, service)
        async with self.handle_rpc_exception():
            return await self._start_service(kernel_id, service, opts)

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
    async def reset(self):
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

    async def _create_kernel(self, kernel_id, kernel_config, restarting=False):

        await self.send_event('kernel_preparing', kernel_id)

        # Read image-specific labels and settings
        image_ref = ImageRef(
            kernel_config['image']['canonical'],
            kernel_config['image']['registry']['name'])
        environ: dict = kernel_config.get('environ', {})
        extra_mount_list = await get_extra_volumes(self.docker, image_ref.short)

        try:
            # Find the exact image using a digest reference
            digest_ref = f"{image_ref.name}@{kernel_config['image']['digest']}"
            await self.docker.images.get(digest_ref)
        except DockerError as e:
            if e.status == 404:
                await self.send_event('kernel_pulling',
                                      kernel_id, image_ref.canonical)
                auth_config = None
                dreg_user = kernel_config['image']['registry'].get('username')
                dreg_passwd = kernel_config['image']['registry'].get('password')
                if dreg_user and dreg_passwd:
                    encoded_creds = base64.b64encode(
                        f'{dreg_user}:{dreg_passwd}'.encode('utf-8')) \
                        .decode('ascii')
                    auth_config = {
                        'auth': encoded_creds,
                    }
                # TODO: digest refs are not working as expected...
                # digest_ref = f"{image_ref.registry}/{image_ref.name}@" \
                #              f"{kernel_config['image']['digest']}"
                digest_ref = image_ref.canonical
                log.info('pulling image {} from registry', digest_ref)
                await self.docker.images.pull(digest_ref, auth=auth_config)
            else:
                raise
        await self.send_event('kernel_creating', kernel_id)
        image_labels = kernel_config['image']['labels']

        version        = int(image_labels.get('ai.backend.kernelspec', '1'))
        envs_corecount = image_labels.get('ai.backend.envs.corecount', '')
        envs_corecount = envs_corecount.split(',') if envs_corecount else []
        kernel_features = set(image_labels.get('ai.backend.features', '').split())

        scratch_dir = (self.config.scratch_root / kernel_id).resolve()
        tmp_dir = (self.config.scratch_root / f'{kernel_id}_tmp').resolve()
        config_dir = scratch_dir / 'config'
        work_dir = scratch_dir / 'work'

        # PHASE 1: Read existing resource spec or devise a new resource spec.

        if restarting:
            with open(config_dir / 'resource.txt', 'r') as f:
                resource_spec = KernelResourceSpec.read_from_file(f)
        else:
            slots = ResourceSlot(kernel_config['resource_slots'])
            vfolders = kernel_config['mounts']
            slots = slots.as_numeric(known_slot_types)
            resource_spec = KernelResourceSpec(
                allocations={},
                slots={**slots},  # copy
                mounts=[],
                scratch_disk_size=0,  # TODO: implement (#70)
            )

        # PHASE 2: Apply the resource spec.

        # Inject Backend.AI-intrinsic env-variables for gosu
        if KernelFeatures.UID_MATCH in kernel_features:
            uid = self.config.kernel_uid
            environ['LOCAL_USER_ID'] = str(uid)

        # Inject Backend.AI-intrinsic mount points and extra mounts
        binds = [
            f'{config_dir}:/home/config:ro',
            f'{work_dir}:/home/work/:rw',
            f'{tmp_dir}:/tmp:rw',
        ]
        binds.extend(f'{v.name}:{v.container_path}:{v.mode}'
                     for v in extra_mount_list)

        if restarting:
            # Reuse previous CPU share.
            pass

            # Reuse previous memory share.
            pass

            # Reuse previous accelerator share.
            pass

            # Reuse previous mounts.
            for mount in resource_spec.mounts:
                binds.append(str(mount))
        else:
            # Ensure that we already converted all values to absolute real values.
            assert slots.numeric
            assert 'cpu' in slots
            assert 'mem' in slots

            # Realize ComputeDevice (including accelerators) allocations.
            dev_types = set()
            for slot_type in slots.keys():
                dev_type = slot_type.split('.', maxsplit=1)[0]
                dev_types.add(dev_type)

            for dev_type in dev_types:
                computer_set = self.computers[dev_type]
                device_specific_slots = {
                    slot_type: amount for slot_type, amount in slots.items()
                    if slot_type.startswith(dev_type)
                }
                resource_spec.allocations[dev_type] = \
                    computer_set.alloc_map.allocate(device_specific_slots)

            # Realize vfolder mounts.
            for folder_name, folder_host, folder_id in vfolders:
                host_path = (self.config.vfolder_mount / folder_host /
                             self.config.vfolder_fsprefix / folder_id)
                kernel_path = Path(f'/home/work/{folder_name}')
                # TODO: apply READ_ONLY for read-only shared vfolders
                mount = Mount(host_path, kernel_path, MountPermission.READ_WRITE)
                resource_spec.mounts.append(mount)
                binds.append(str(mount))

            # should no longer be used!
            del vfolders

        # Inject Backend.AI-intrinsic env-variables for libbaihook and gosu
        cpu_core_count = len(resource_spec.allocations['cpu']['cpu'])
        environ.update({k: str(cpu_core_count) for k in envs_corecount})

        def _mount(host_path, container_path, perm='ro'):
            nonlocal binds
            binds.append(f'{host_path}:{container_path}:{perm}')

        # Inject Backend.AI kernel runner dependencies.
        distro = image_labels.get('ai.backend.base-distro', 'ubuntu16.04')
        arch = platform.machine()
        entrypoint_sh_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../runner/entrypoint.sh'))
        suexec_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', f'../runner/su-exec.{distro}.bin'))
        jail_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', f'../runner/jail.{distro}.bin'))
        hook_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', f'../runner/libbaihook.{distro}.{arch}.so'))
        kernel_pkg_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../kernel'))
        helpers_pkg_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../helpers'))
        _mount(entrypoint_sh_path.resolve(),
               '/opt/backend.ai/bin/entrypoint.sh')
        _mount(suexec_path.resolve(),
               '/opt/backend.ai/bin/su-exec')
        _mount(jail_path.resolve(),
               '/opt/backend.ai/bin/jail')
        _mount(hook_path.resolve(),
               '/opt/backend.ai/hook/libbaihook.so')
        _mount(kernel_pkg_path.resolve(),
               '/opt/backend.ai/lib/python3.6/site-packages/ai/backend/kernel')
        _mount(helpers_pkg_path.resolve(),
               '/opt/backend.ai/lib/python3.6/site-packages/ai/backend/helpers')
        environ['LD_PRELOAD'] = '/opt/backend.ai/hook/libbaihook.so'

        # Inject ComputeDevice-specific env-varibles and hooks
        computer_docker_args = {}
        for dev_type, device_alloc in resource_spec.allocations.items():
            computer_set = self.computers[dev_type]
            update_nested_dict(computer_docker_args,
                               await computer_set.klass.generate_docker_args(
                                   self.docker, device_alloc))
            hook_paths = await computer_set.klass.get_hooks(distro, arch)
            if hook_paths:
                log.debug('accelerator {} provides hooks: {}',
                          computer_set.klass.__name__,
                          ', '.join(map(str, hook_paths)))
            for hook_path in hook_paths:
                container_hook_path = '/opt/backend.ai/hook/lib{}{}.so'.format(
                    computer_set.klass.key, secrets.token_hex(6),
                )
                _mount(hook_path, container_hook_path)
                environ['LD_PRELOAD'] += ':' + container_hook_path

        # PHASE 3: Store the resource spec.

        if restarting:
            pass
        else:
            os.makedirs(scratch_dir)
            os.makedirs(tmp_dir)
            if sys.platform == 'linux' and self.config.scratch_in_memory:
                await create_scratch_filesystem(scratch_dir, 64)
                await create_scratch_filesystem(tmp_dir, 64)
            os.makedirs(work_dir)
            if KernelFeatures.UID_MATCH in kernel_features:
                uid = int(environ['LOCAL_USER_ID'])
                if os.getuid() == 0:  # only possible when I am root.
                    os.chown(work_dir, uid, uid)
            os.makedirs(config_dir)
            # Store custom environment variables for kernel runner.
            with open(config_dir / 'environ.txt', 'w') as f:
                for k, v in environ.items():
                    f.write(f'{k}={v}\n')
                accel_envs = computer_docker_args.get('Env', [])
                for env in accel_envs:
                    f.write(f'{env}\n')
            with open(config_dir / 'resource.txt', 'w') as f:
                resource_spec.write_to_file(f)
                for dev_type, device_alloc in resource_spec.allocations.items():
                    computer_set = self.computers[dev_type]
                    kvpairs = \
                        await computer_set.klass.generate_resource_data(device_alloc)
                    for k, v in kvpairs.items():
                        f.write(f'{k}={v}\n')

        # PHASE 4: Run!
        log.info('kernel {0} starting with resource spec: \n',
                 pformat(attr.asdict(resource_spec)))

        # TODO: Refactor out as separate "Docker execution driver plugin" (#68)
        #   - Refactor volumes/binds lists to a plugin "mount" API
        #   - Refactor "/home/work" and "/opt/backend.ai" prefixes to be specified
        #     by the plugin implementation.

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

        if len(exposed_ports) > len(self.port_pool):
            raise RuntimeError('Container ports are not sufficiently available.')
        host_ports = []
        for eport in exposed_ports:
            hport = self.port_pool.pop()
            host_ports.append(hport)

        runtime_type = image_labels.get('ai.backend.runtime-type', 'python')
        runtime_path = image_labels.get('ai.backend.runtime-path', None)
        cmdargs = []
        if self.config.sandbox_type == 'jail':
            cmdargs += [
                "/opt/backend.ai/bin/jail",
                "-policy", "/etc/backend.ai/jail/policy.yml",
            ]
            if self.config.jail_arg:
                cmdargs += map(lambda s: s.strip(), self.config.jail_arg)
        cmdargs += [
            "/opt/backend.ai/bin/python",
            "-m", "ai.backend.kernel", runtime_type,
        ]
        if runtime_path is not None:
            cmdargs.append(runtime_path)
        container_config = {
            'Image': image_ref.canonical,
            'Tty': True,
            'OpenStdin': True,
            'Privileged': False,
            'StopSignal': 'SIGINT',
            'ExposedPorts': {
                f'{port}/tcp': {} for port in exposed_ports
            },
            'EntryPoint': ["/opt/backend.ai/bin/entrypoint.sh"],
            'Cmd': cmdargs,
            'Env': [f'{k}={v}' for k, v in environ.items()],
            'WorkingDir': '/home/work',
            'HostConfig': {
                'Init': True,
                'VolumesFrom': [f'kernel-env.{kernel_id}'],
                'Binds': binds,
                'PortBindings': {
                    f'{eport}/tcp': [{'HostPort': str(hport)}]
                    for eport, hport in zip(exposed_ports, host_ports)
                },
                'PublishAllPorts': False,  # we manage port mapping manually!
            },
        }
        if self.config.sandbox_type == 'jail':
            container_config['HostConfig']['SecurityOpt'] = [
                'seccomp=unconfined',
                'apparmor=unconfined',
            ]
        update_nested_dict(container_config, computer_docker_args)
        kernel_name = f"kernel.{image_ref.name.split('/')[-1]}.{kernel_id}"
        log.debug('container config: {!r}', container_config)

        # We are all set! Create and start the container.
        try:
            env_container = await self.docker.containers.create(config={
                'Image': f'lablup/backendai-krunner-env:{VERSION}-{distro}',
            }, name=f'kernel-env.{kernel_id}')
            container = await self.docker.containers.create(
                config=container_config, name=kernel_name)
            cid = container._id

            stat_addr = f'tcp://{self.config.agent_host}:{self.config.stat_port}'
            stat_type = get_preferred_stat_type()
            self.stats[cid] = StatCollectorState(kernel_id)
            async with spawn_stat_collector(stat_addr, stat_type, cid):
                await container.start()
        except Exception:
            # Oops, we have to restore the allocated resources!
            if sys.platform == 'linux' and self.config.scratch_in_memory:
                await destroy_scratch_filesystem(scratch_dir)
                await destroy_scratch_filesystem(tmp_dir)
            shutil.rmtree(scratch_dir)
            shutil.rmtree(tmp_dir)
            self.port_pool.update(host_ports)
            for dev_type, device_alloc in resource_spec.allocations.items():
                self.computers[dev_type].alloc_map.free(device_alloc)
            raise

        stdin_port = 0
        stdout_port = 0
        for idx, port in enumerate(exposed_ports):
            host_port = int((await container.port(port))[0]['HostPort'])
            assert host_port == host_ports[idx]
            if port in service_ports:
                service_ports[port]['host_port'] = host_port
            elif port == 2000:     # intrinsic
                repl_in_port = host_port
            elif port == 2001:     # intrinsic
                repl_out_port = host_port
            elif port == 2002:     # legacy
                stdin_port = host_port
            elif port == 2003:  # legacy
                stdout_port = host_port
        if self.config.kernel_host_override:
            kernel_host = self.config.kernel_host_override
        else:
            kernel_host = self.config.agent_host

        self.container_registry[kernel_id] = {
            'lang': image_ref,
            'version': version,
            'container_id': container._id,
            'env_container_id': env_container._id,
            'kernel_host': kernel_host,
            'repl_in_port': repl_in_port,
            'repl_out_port': repl_out_port,
            'stdin_port': stdin_port,    # legacy
            'stdout_port': stdout_port,  # legacy
            'service_ports': list(service_ports.values()),
            'host_ports': host_ports,
            'last_used': time.monotonic(),
            'runner_tasks': set(),
            'resource_spec': resource_spec,
        }
        log.debug('kernel repl-in address: {0}:{1}', kernel_host, repl_in_port)
        log.debug('kernel repl-out address: {0}:{1}', kernel_host, repl_out_port)
        for service_port in service_ports.values():
            log.debug('service port: {!r}', service_port)
        await self.send_event('kernel_started', kernel_id)
        return {
            'id': kernel_id,
            'kernel_host': kernel_host,
            'repl_in_port': repl_in_port,
            'repl_out_port': repl_out_port,
            'stdin_port': stdin_port,    # legacy
            'stdout_port': stdout_port,  # legacy
            'service_ports': list(service_ports.values()),
            'container_id': container._id,
            'resource_spec': resource_spec.to_json(),
        }

    async def _destroy_kernel(self, kernel_id, reason):
        try:
            cid = self.container_registry[kernel_id]['container_id']
        except KeyError:
            log.warning('_destroy_kernel({0}) kernel missing (already dead?)',
                        kernel_id)
            await self.clean_kernel(kernel_id)
            await self.send_event('kernel_terminated',
                                  kernel_id, 'self-terminated',
                                  None)
            return
        container = self.docker.containers.container(cid)
        await self.clean_runner(kernel_id)
        try:
            await container.kill()
            # Collect the last-moment statistics.
            last_stat = None
            if cid in self.stats:
                await self.stats[cid].terminated.wait()
                last_stat = self.stats[cid].last_stat
                del self.stats[cid]
            # The container will be deleted in the docker monitoring coroutine.
            return last_stat
        except DockerError as e:
            if e.status == 409 and 'is not running' in e.message:
                # already dead
                log.warning('_destroy_kernel({0}) already dead', kernel_id)
                pass
            elif e.status == 404:
                log.warning('_destroy_kernel({0}) kernel missing, '
                            'forgetting this kernel', kernel_id)
                resource_spec = self.container_registry[kernel_id]['resource_spec']
                for accel_key, accel_alloc in resource_spec.allocations.items():
                    self.computers[accel_key].alloc_map.free(accel_alloc)
                self.container_registry.pop(kernel_id, None)
                pass
            else:
                log.exception('_destroy_kernel({0}) kill error', kernel_id)
                self.error_monitor.capture_exception()
        except Exception:
            log.exception('_destroy_kernel({0}) unexpected error', kernel_id)
            self.error_monitor.capture_exception()

    async def _ensure_runner(self, kernel_id, *, api_version=3):
        # TODO: clean up
        async with self.runner_lock:
            runner = self.container_registry[kernel_id].get('runner')
            if runner is not None:
                log.debug('_execute_code:v{0}({1}) use '
                          'existing runner', api_version, kernel_id)
            else:
                client_features = {'input', 'continuation'}
                runner = KernelRunner(
                    kernel_id,
                    self.container_registry[kernel_id]['kernel_host'],
                    self.container_registry[kernel_id]['repl_in_port'],
                    self.container_registry[kernel_id]['repl_out_port'],
                    0,
                    client_features)
                log.debug('_execute:v{0}({1}) start new runner',
                          api_version, kernel_id)
                self.container_registry[kernel_id]['runner'] = runner
                # TODO: restoration of runners after agent restarts
                await runner.start()
            return runner

    async def _execute(self, api_version, kernel_id,
                       run_id, mode, text, opts,
                       flush_timeout):
        # Save kernel-generated output files in a separate sub-directory
        # (to distinguish from user-uploaded files)
        output_dir = self.config.scratch_root / kernel_id / 'work' / '.output'

        restart_tracker = self.restarting_kernels.get(kernel_id)
        if restart_tracker:
            await restart_tracker.done_event.wait()

        try:
            kernel_info = self.container_registry[kernel_id]
        except KeyError:
            await self.send_event('kernel_terminated',
                                  kernel_id, 'self-terminated',
                                  None)
            raise RuntimeError(f'The container for kernel {kernel_id} is not found! '
                               '(might be terminated--try it again)') from None

        kernel_info['last_used'] = time.monotonic()
        runner = await self._ensure_runner(kernel_id, api_version=api_version)

        try:
            myself = asyncio.Task.current_task()
            kernel_info['runner_tasks'].add(myself)

            await runner.attach_output_queue(run_id)

            if mode == 'batch' or mode == 'query':
                kernel_info['initial_file_stats'] \
                    = scandir(output_dir, max_upload_size)
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

            final_file_stats = scandir(output_dir, max_upload_size)
            if utils.nmget(result, 'options.upload_output_files', True):
                # TODO: separate as a new task
                initial_file_stats = \
                    kernel_info['initial_file_stats']
                output_files = await upload_output_files_to_s3(
                    initial_file_stats, final_file_stats, output_dir, kernel_id)

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

    async def _get_completions(self, kernel_id, text, opts):
        runner = await self._ensure_runner(kernel_id)
        result = await runner.feed_and_get_completion(text, opts)
        return {'status': 'finished', 'completions': result}

    async def _get_logs(self, kernel_id):
        container_id = self.container_registry[kernel_id]['container_id']
        container = await self.docker.containers.get(container_id)
        logs = await container.log(stdout=True, stderr=True)
        return {'logs': ''.join(logs)}

    async def _interrupt_kernel(self, kernel_id):
        runner = await self._ensure_runner(kernel_id)
        await runner.feed_interrupt()
        return {'status': 'finished'}

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

    async def _accept_file(self, kernel_id, filename, filedata):
        loop = asyncio.get_event_loop()
        work_dir = self.config.scratch_root / kernel_id / 'work'
        try:
            # create intermediate directories in the path
            dest_path = (work_dir / filename).resolve(strict=False)
            parent_path = dest_path.parent
        except ValueError:  # parent_path does not start with work_dir!
            raise AssertionError('malformed upload filename and path.')

        def _write_to_disk():
            parent_path.mkdir(parents=True, exist_ok=True)
            dest_path.write_bytes(filedata)

        try:
            await loop.run_in_executor(None, _write_to_disk)
        except FileNotFoundError:
            log.error('{0}: writing uploaded file failed: {1} -> {2}',
                      kernel_id, filename, dest_path)

    async def _download_file(self, kernel_id, filepath):
        container_id = self.container_registry[kernel_id]['container_id']
        container = self.docker.containers.container(container_id)
        # Limit file path to /home/work inside a container.
        # TODO: extend path search in virtual folders.
        abspath = os.path.abspath(os.path.join('/home/work', filepath))
        try:
            with await container.get_archive(abspath) as tarobj:
                tarobj.fileobj.seek(0, 2)
                fsize = tarobj.fileobj.tell()
                assert fsize < 1 * 1048576, 'too large file.'
                tarbytes = tarobj.fileobj.getvalue()
        except DockerError:
            log.warning('Could not found the file: {0}', abspath)
            raise FileNotFoundError(f'Could not found the file: {abspath}')
        return tarbytes

    async def _list_files(self, kernel_id: str, path: str):
        container_id = self.container_registry[kernel_id]['container_id']

        # Ensure target directory is under /home/work/ folder.
        # Append abspath with '/' if it is a directory since pathlib does not provide
        # a nicer way to add a trailing slash. If this is not appended, directories
        # like '/home/workfake' will pass the check.
        code = '''from pathlib import Path
abspath = Path('%(path)s').resolve(strict=True)
suffix = '/' if abspath.is_dir() else ''
print(str(abspath) + suffix)
''' % {'path': path}
        command = f'docker exec {container_id} python -c "{code}"'
        p = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        outs, errs = p.communicate()
        abspath = outs.decode('utf-8')
        errs = errs.decode('utf-8')
        if errs or (not abspath.startswith('/home/work/')):
            return {'files': '', 'errors': 'No such file or directory'}

        # Gather individual file information in the target path.
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
        command = f'docker exec {container_id} python -c "{code}"'
        p = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        outs, errs = p.communicate()
        outs = outs.decode('utf-8')
        errs = errs.decode('utf-8')

        return {'files': outs, 'errors': errs, 'abspath': abspath}

    async def heartbeat(self, interval):
        '''
        Send my status information and available kernel images.
        '''
        res_slots = {}
        for klass in compute_device_types.values():
            for slot_key, slot_type in klass.slot_types:
                res_slots[slot_key] = (
                    slot_type,
                    str(ResourceSlot.value_as_numeric(
                        self.slots.get(slot_key, 0),
                        slot_type)),
                )
        agent_info = {
            'ip': self.config.agent_host,
            'region': self.config.region,
            'addr': f'tcp://{self.config.agent_host}:{self.config.agent_port}',
            'resource_slots': res_slots,
            'images': snappy.compress(msgpack.packb([
                (repo_tag, digest) for repo_tag, digest in self.images.items()
            ])),
        }
        try:
            await self.send_event('instance_heartbeat', agent_info)
        except asyncio.TimeoutError:
            log.warning('event dispatch timeout: instance_heartbeat')
        except Exception:
            log.exception('instance_heartbeat failure')
            self.error_monitor.capture_exception()

    async def fetch_docker_events(self):
        while True:
            try:
                await self.docker.events.run()
            except asyncio.TimeoutError:
                # The API HTTP connection may terminate after some timeout
                # (e.g., 5 minutes)
                log.info('restarting docker.events.run()')
                continue
            except aiohttp.ClientError as e:
                log.warning('restarting docker.events.run() due to {0!r}', e)
                continue
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception('unexpected error')
                self.error_monitor.capture_exception()
                break

    async def monitor(self):
        subscriber = self.docker.events.subscribe()
        last_footprint = None
        while True:
            try:
                evdata = await subscriber.get()
            except asyncio.CancelledError:
                break
            if evdata is None:
                # fetch_docker_events() will automatically reconnect.
                continue

            # FIXME: Sometimes(?) duplicate event data is received.
            # Just ignore the duplicate ones.
            new_footprint = (
                evdata['Type'],
                evdata['Action'],
                evdata['Actor']['ID'],
            )
            if new_footprint == last_footprint:
                continue
            last_footprint = new_footprint

            if evdata['Action'] == 'die':
                # When containers die, we immediately clean up them.
                container_id = evdata['Actor']['ID']
                container_name = evdata['Actor']['Attributes']['name']
                kernel_id = await get_kernel_id_from_container(container_name)
                if kernel_id is None:
                    continue
                try:
                    exit_code = evdata['Actor']['Attributes']['exitCode']
                except KeyError:
                    exit_code = '(unknown)'
                log.debug('docker-event: container-terminated: '
                          '{0} with exit code {1} ({2})',
                          container_id[:7], exit_code, kernel_id)
                await self.send_event('kernel_terminated',
                                      kernel_id, 'self-terminated',
                                      None)
                asyncio.ensure_future(self.clean_kernel(kernel_id))

    async def clean_kernel(self, kernel_id):
        try:
            kernel_info = self.container_registry[kernel_id]

            container_id = kernel_info['container_id']
            env_container_id = kernel_info['env_container_id']
            container = self.docker.containers.container(container_id)
            env_container = self.docker.containers.container(env_container_id)
            try:
                await self.clean_runner(kernel_id)
            finally:
                # When the agent restarts with a different port range, existing
                # containers' host ports may not belong to the new port range.
                try:
                    if not self.config.debug_skip_container_deletion:
                        await container.delete()
                        await env_container.delete()
                except DockerError as e:
                    if e.status == 409 and 'already in progress' in e.message:
                        pass
                    elif e.status == 404:
                        pass
                    else:
                        log.warning('container deletion: {0!r}', e)
                finally:
                    port_range = self.config.container_port_range
                    restored_ports = [*filter(
                        lambda p: port_range[0] <= p <= port_range[1],
                        kernel_info['host_ports'])]
                    self.port_pool.update(restored_ports)
        except KeyError:
            pass
        if kernel_id in self.restarting_kernels:
            self.restarting_kernels[kernel_id].destroy_event.set()
        else:
            scratch_dir = self.config.scratch_root / kernel_id
            tmp_dir = self.config.scratch_root / f'{kernel_id}_tmp'
            try:
                if sys.platform == 'linux' and self.config.scratch_in_memory:
                    await destroy_scratch_filesystem(scratch_dir)
                    await destroy_scratch_filesystem(tmp_dir)
                shutil.rmtree(scratch_dir)
                shutil.rmtree(tmp_dir)
            except FileNotFoundError:
                pass
            try:
                resource_spec = self.container_registry[kernel_id]['resource_spec']
                for accel_key, accel_alloc in resource_spec.allocations.items():
                    self.computers[accel_key].alloc_map.free(accel_alloc)
                self.container_registry.pop(kernel_id, None)
            except KeyError:
                pass
            if kernel_id in self.blocking_cleans:
                self.blocking_cleans[kernel_id].set()

    async def clean_old_kernels(self, interval):
        now = time.monotonic()
        keys = tuple(self.container_registry.keys())
        tasks = []
        for kernel_id in keys:
            try:
                last_used = self.container_registry[kernel_id]['last_used']
                if now - last_used > self.config.idle_timeout:
                    log.info('destroying kernel {0} as clean-up', kernel_id)
                    task = asyncio.ensure_future(
                        self._destroy_kernel(kernel_id, 'idle-timeout'))
                    tasks.append(task)
            except KeyError:
                # The kernel may be destroyed by other means?
                pass
        await asyncio.gather(*tasks)

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


@aiotools.server
async def server_main(loop, pidx, _args):

    args = _args[0]
    args.instance_id = await identity.get_instance_id()
    args.inst_type = await identity.get_instance_type()
    if not args.agent_host:
        args.agent_host = await identity.get_instance_ip()
    args.region = await identity.get_instance_region()
    log.info('Node ID: {0} (machine-type: {1}, host: {2})',
             args.instance_id, args.inst_type, args.agent_host)

    # Start RPC server.
    try:
        agent = AgentRPCServer(args, loop=loop)
        await agent.init()
    except Exception:
        log.exception('unexpected error during AgentRPCServer.init()!')

    # Run!
    try:
        stop_signal = yield
    finally:
        # Shutdown.
        log.info('shutting down...')
        await agent.shutdown(stop_signal)


def main():
    parser = configargparse.ArgumentParser()
    parser.add('--namespace', type=str, default='local',
               env_var='BACKEND_NAMESPACE',
               help='The namespace of this Backend.AI cluster. (default: local)')
    parser.add('--agent-host-override', type=str, default=None,
               dest='agent_host',
               env_var='BACKEND_AGENT_HOST_OVERRIDE',
               help='Manually set the IP address of this agent to report to the '
                    'manager.')
    parser.add('--kernel-host-override', type=str, default=None,
               env_var='BACKEND_KERNEL_HOST_OVERRIDE',
               help='Manually set the IP address of kernels spawned by this agent '
                    'to report to the manager.')
    parser.add('--agent-port', type=port_no, default=6001,
               env_var='BACKEND_AGENT_PORT',
               help='The port number to listen on.')
    parser.add('--stat-port', type=port_no, default=6002,
               env_var='BACKEND_STAT_PORT',
               help='The port number to receive statistics reports from '
                    'local containers.')
    parser.add('--container-port-range', type=port_range, default=(30000, 31000),
               env_var='BACKEND_CONTAINER_PORT_RANGE',
               help='The range of host public ports to be used by containers '
                    '(inclusive)')
    parser.add('--etcd-addr', type=host_port_pair,
               env_var='BACKEND_ETCD_ADDR',
               default=HostPortPair(ip_address('127.0.0.1'), 2379),
               help='The host:port pair of the etcd cluster or its proxy.')
    parser.add('--idle-timeout', type=non_negative_int, default=None,
               help='The maximum period of time allowed for kernels to wait '
                    'further requests.')
    parser.add('--sandbox-type', type=str,
               choices=['docker', 'jail'], default='docker',
               env_var='BACKEND_SANDBOX_TYPE',
               help='Choose the additional security sandboxing layer for contaiers.')
    parser.add('--jail-arg', action='append', default=[],
               help='Additional arguments to the jail to change its behavior.')
    parser.add('--kernel-uid', type=str, default=None,
               help='The username/UID to run kernel containers. '
                    '(default: the same user where the agent runs)')
    parser.add('--scratch-in-memory', action='store_true', default=False,
               help='Keep the scratch and tmp directory in memory '
                    '(only available at Linux)')
    parser.add('--debug-kernel', type=Path, default=None,
               env_var='DEBUG_KERNEL',
               help='Deprecated.')
    parser.add('--debug-jail', type=Path, default=None,
               env_var='DEBUG_JAIL',
               help='Deprecated.')
    parser.add('--debug-hook', type=Path, default=None,
               env_var='DEBUG_HOOK',
               help='Deprecated.')
    parser.add('--debug-skip-container-deletion', action='store_true', default=False,
               help='If specified, skips container deletion when container is dead '
                    'or killed.  You may check the container logs for additional '
                    'in-container debugging, but also need to manaully remove them.')
    parser.add('--kernel-aliases', type=str, default=None,
               help='The filename for additional kernel aliases')
    parser.add('--limit-cpus', type=str, default=None,
               help='The hexademical mask to limit available CPUs '
                    'reported to the manager (default: not limited)')
    parser.add('--limit-gpus', type=str, default=None,
               help='The hexademical mask to limit available CUDA GPUs '
                    'reported to the manager (default: not limited)')
    parser.add('--scratch-root', type=Path,
               default=Path('/var/cache/scratches'),
               env_var='BACKEND_SCRATCH_ROOT',
               help='The scratch directory to store container working directories.')

    plugins = [
        'stats_monitor',
        'error_monitor',
    ]
    add_plugin_args(parser, plugins)

    Logger.update_log_args(parser)
    args = parser.parse_args()

    assert args.scratch_root.exists()
    assert args.scratch_root.is_dir()

    if args.debug_kernel is not None:
        args.debug_kernel = args.debug_kernel.resolve()
        assert args.debug_kernel.match('ai/backend'), \
               'debug-kernel path must end with "ai/backend".'
    if args.debug_jail is not None:
        args.debug_jail = args.debug_jail.resolve()
    if args.debug_hook is not None:
        args.debug_hook = args.debug_hook.resolve()

    if args.limit_cpus is not None:
        args.limit_cpus = int(args.limit_cpus, 16)
        args.limit_cpus = bitmask2set(args.limit_cpus)
    if args.limit_gpus is not None:
        args.limit_gpus = int(args.limit_gpus, 16)
        args.limit_gpus = bitmask2set(args.limit_gpus)

    try:
        if args.kernel_uid is None:
            args.kernel_uid = os.getuid()
        else:
            args.kernel_uid = int(args.kernel_uid)
        # Note that this uid may not exist in the host
        # as it might be only valid in containers
        # depending on the user setup.
    except ValueError:
        # But when the string is given, we must be able
        # to find it in our host's password db.
        args.kernel_uid = pwd.getpwnam(args.kernel_uid).pw_uid

    logger = Logger(args)
    logger.add_pkg('aiodocker')
    logger.add_pkg('aiotools')
    logger.add_pkg('ai.backend')
    setproctitle(f'backend.ai: agent {args.namespace} *:{args.agent_port}')

    with logger:
        log.info('Backend.AI Agent {0}', VERSION)
        log.info('runtime: {0}', utils.env_info())

        log_config = logging.getLogger('ai.backend.agent.config')
        if args.debug:
            log_config.debug('debug mode enabled.')

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        aiotools.start_server(server_main, num_workers=1,
                              use_threading=True, args=(args, ))
        log.info('exit.')


if __name__ == '__main__':
    main()
