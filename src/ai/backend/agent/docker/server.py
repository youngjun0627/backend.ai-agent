import asyncio
import base64
from decimal import Decimal
import functools
import logging, logging.config
import os
from pathlib import Path
import pkg_resources
import platform
from pprint import pformat
import secrets
import shutil
import signal
import struct
import sys
import time
from typing import Mapping, MutableMapping, Set

import aiohttp
import aioredis
import aiotools
import aiozmq, aiozmq.rpc
import attr
from async_timeout import timeout
import snappy
import zmq

from aiodocker.docker import Docker
from aiodocker.exceptions import DockerError

from ai.backend.common import msgpack
from ai.backend.common.docker import ImageRef
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    MountPermission,
    MountTypes,
    ResourceSlot
)
from .kernel import DockerKernel
from .resources import (
    detect_resources,
)
from .. import __version__ as VERSION
from ..exception import InsufficientResource
from ..fs import create_scratch_filesystem, destroy_scratch_filesystem
from ..kernel import match_krunner_volume, KernelFeatures
from ..resources import (
    Mount,
    KernelResourceSpec,
)
from ..server import (
    AbstractAgentServer,
    ComputerContext,
    ipc_base_path,
    get_extra_volumes,
    parse_service_port
)
from ..stats import (
    StatContext, StatModes,
    spawn_stat_synchronizer, StatSyncState
)
from ..utils import (
    update_nested_dict,
    get_kernel_id_from_container,
    host_pid_to_container_pid,
    container_pid_to_host_pid
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.server'))


class AgentServer(AbstractAgentServer):

    def __init__(self, config, loop=None):
        super().__init__(config, loop=loop)

        self.docker = Docker()
        self.container_registry: MutableMapping[str, DockerKernel] = {}
        self.redis_stat_pool = None

        self.restarting_kernels = {}
        self.blocking_cleans = {}

        self.computers: Mapping[str, ComputerContext] = {}
        self.images: Mapping[str, str] = {}  # repoTag -> digest

        self.event_sock = None
        self.scan_images_timer = None
        self.monitor_fetch_task = None
        self.monitor_handle_task = None
        self.hb_timer = None
        self.clean_timer = None
        self.stat_sync_task = None

        self.stat_ctx = StatContext(self, mode=StatModes(config['container']['stats-type']))
        self.live_stat_timer = None
        self.agent_sock_task = None

        self.port_pool = set(range(
            config['container']['port-range'][0],
            config['container']['port-range'][1] + 1,
        ))

        self._orphan_tasks: Set[asyncio.Task] = set()
        self.runner_lock: asyncio.Lock = asyncio.Lock()

    async def scan_running_containers(self):
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
                kernel_host = self.config['container']['kernel-host']
                config_dir = (self.config['container']['scratch-root'] /
                                kernel_id / 'config').resolve()
                with open(config_dir / 'resource.txt', 'r') as f:
                    resource_spec = KernelResourceSpec.read_from_file(f)
                    # legacy handling
                    if resource_spec.container_id is None:
                        resource_spec.container_id = container._id
                    else:
                        assert container._id == resource_spec.container_id, \
                                'Container ID from the container must match!'
                service_ports = []
                for item in labels.get('ai.backend.service-ports', '').split(','):
                    if not item:
                        continue
                    service_port = parse_service_port(item)
                    service_port['host_port'] = \
                        port_map.get(service_port['container_port'], None)
                    service_ports.append(service_port)
                self.container_registry[kernel_id] = DockerKernel(
                    kernel_id,
                    ImageRef(image),
                    int(labels.get('ai.backend.kernelspec', '1')),
                    resource_spec=resource_spec,
                    service_ports=service_ports,
                    data={
                        'container_id': container._id,
                        'kernel_host': kernel_host,
                        'repl_in_port': port_map[2000],
                        'repl_out_port': port_map[2001],
                        'stdin_port': port_map.get(2002, 0),
                        'stdout_port': port_map.get(2003, 0),
                        'host_ports': [*port_map.values()],
                    })
            elif status in {'exited', 'dead', 'removing'}:
                log.info('detected terminated kernel: {0}', kernel_id)
                await self.send_event('kernel_terminated', kernel_id,
                                        'self-terminated', None)

        log.info('starting with resource allocations')
        for computer_name, computer_ctx in self.computers.items():
            log.info('{}: {!r}', computer_name,
                        dict(computer_ctx.alloc_map.allocations))

    async def scan_images(self, interval):
        all_images = await self.docker.images.list()
        updated_images = {}
        for image in all_images:
            if image['RepoTags'] is None:
                continue
            for repo_tag in image['RepoTags']:
                if repo_tag.endswith('<none>'):
                    continue
                img_detail = await self.docker.images.inspect(repo_tag)
                labels = img_detail['Config']['Labels']
                if labels and 'ai.backend.kernelspec' in labels:
                    updated_images[repo_tag] = img_detail['Id']
        for added_image in (updated_images.keys() - self.images.keys()):
            log.debug('found kernel image: {0}', added_image)
        for removed_image in (self.images.keys() - updated_images.keys()):
            log.debug('removed kernel image: {0}', removed_image)
        self.images = updated_images

    async def deregister_myself(self):
        # TODO: reimplement using Redis heartbeat stream
        pass

    async def send_event(self, event_name, *args):
        if self.event_sock is None:
            return
        log.debug('send_event({0})', event_name)
        self.event_sock.write((
            event_name.encode('ascii'),
            self.config['agent']['id'].encode('utf8'),
            msgpack.packb(args),
        ))

    async def sync_container_stats(self):
        my_uid = os.getuid()
        my_gid = os.getgid()
        kernel_uid = self.config['container']['kernel-uid']
        kernel_gid = self.config['container']['kernel-gid']
        try:
            stat_sync_sock = self.zmq_ctx.socket(zmq.REP)
            stat_sync_sock.setsockopt(zmq.LINGER, 1000)
            stat_sync_sock.bind('ipc://' + str(self.stat_sync_sockpath))
            if my_uid == 0:
                os.chown(self.agent_sockpath, kernel_uid, kernel_gid)
            else:
                if my_uid != kernel_uid:
                    log.error('The UID of agent ({}) must be same to the container UID ({}).',
                                my_uid, kernel_uid)
                if my_gid != kernel_gid:
                    log.error('The GID of agent ({}) must be same to the container GID ({}).',
                                my_gid, kernel_gid)
            log.info('opened stat-sync socket at {}', self.stat_sync_sockpath)
            recv = functools.partial(stat_sync_sock.recv_serialized,
                                        lambda frames: [*map(msgpack.unpackb, frames)])
            send = functools.partial(stat_sync_sock.send_serialized,
                                        serialize=lambda msgs: [*map(msgpack.packb, msgs)])
            async for msg in aiotools.aiter(lambda: recv(), None):
                cid = msg[0]['cid']
                status = msg[0]['status']
                try:
                    if cid not in self.stat_sync_states:
                        # If the agent has restarted, the state-tracking dict may be empty.
                        container = self.docker.containers.container(cid)
                        kernel_id = await get_kernel_id_from_container(container)
                        self.stat_sync_states[cid] = StatSyncState(kernel_id)
                    if status == 'terminated':
                        self.stat_sync_states[cid].terminated.set()
                    elif status == 'collect-stat':
                        cstat = await asyncio.shield(self.stat_ctx.collect_container_stat(cid))
                        self.stat_sync_states[cid].last_stat = cstat
                    else:
                        log.warning('unrecognized stat sync status: {}', status)
                finally:
                    await send([{'ack': True}])
        except asyncio.CancelledError:
            pass
        except zmq.ZMQError:
            log.exception('zmq socket error with {}', self.stat_sync_sockpath)
        finally:
            stat_sync_sock.close()
            try:
                self.stat_sync_sockpath.unlink()
            except IOError:
                pass

    async def collect_node_stat(self, interval):
        await asyncio.shield(self.stat_ctx.collect_node_stat())

    async def init(self):
        ipc_base_path.mkdir(parents=True, exist_ok=True)
        self.zmq_ctx = zmq.asyncio.Context()

        # Show Docker version info.
        docker_version = await self.docker.version()
        log.info('running with Docker {0} with API {1}',
                 docker_version['Version'], docker_version['ApiVersion'])

        reserved_slots = {
            'cpu': self.config['resource']['reserved-cpu'],
            'mem': self.config['resource']['reserved-mem'],
            'disk': self.config['resource']['reserved-disk'],
        }
        computers, self.slots = await detect_resources(self.config['plugins'], reserved_slots)
        for name, klass in computers.items():
            devices = await klass.list_devices()
            alloc_map = await klass.create_alloc_map()
            self.computers[name] = ComputerContext(klass, devices, alloc_map)

        # scan_images task should be done before heartbeat,
        # so call it here although we spawn a scheduler
        # for this task below.
        await self.scan_images(None)
        await self.scan_running_containers()

        self.agent_sockpath = ipc_base_path / f'agent.{self.agent_id}.sock'
        self.agent_sock_task = self.loop.create_task(self.handle_agent_socket())

        self.redis_stat_pool = await aioredis.create_redis_pool(
            self.config['redis']['addr'].as_sockaddr(),
            password=(self.config['redis']['password']
                        if self.config['redis']['password'] else None),
            timeout=3.0,
            encoding='utf8',
            db=0)  # REDIS_STAT_DB in backend.ai-manager

        self.event_sock = await aiozmq.create_zmq_stream(
            zmq.PUSH, connect=f"tcp://{self.config['event']['addr']}")
        self.event_sock.transport.setsockopt(zmq.LINGER, 50)

        # Spawn image scanner task.
        self.scan_images_timer = aiotools.create_timer(self.scan_images, 60.0)

        # Spawn stat collector task.
        self.stat_sync_sockpath = ipc_base_path / f'stats.{os.getpid()}.sock'
        self.stat_sync_states = dict()
        self.stat_sync_task = self.loop.create_task(self.sync_container_stats())

        # Start container stats collector for existing containers.
        for kernel_id, kernel_obj in self.container_registry.items():
            cid = kernel_obj['container_id']
            self.stat_sync_states[cid] = StatSyncState(kernel_id)
            async with spawn_stat_synchronizer(self.config['_src'],
                                                self.stat_sync_sockpath,
                                                self.stat_ctx.mode, cid):
                pass

        # Start collecting agent live stat.
        self.live_stat_timer = aiotools.create_timer(self.collect_node_stat, 2.0)

        # Spawn docker monitoring tasks.
        self.monitor_fetch_task  = self.loop.create_task(self.fetch_docker_events())
        self.monitor_handle_task = self.loop.create_task(self.monitor())

        # Send the first heartbeat.
        self.hb_timer    = aiotools.create_timer(self.heartbeat, 3.0)
        self.clean_timer = aiotools.create_timer(self.clean_old_kernels, 10.0)

        # Notify the gateway.
        await self.send_event('instance_started')

    async def shutdown(self, stop_signal):
        await self.deregister_myself()

        # Close all pending kernel runners.
        for kernel_obj in self.container_registry.values():
            if kernel_obj.runner is not None:
                await kernel_obj.runner.close()

        if stop_signal == signal.SIGTERM:
            await self.clean_all_kernels(blocking=True)

        # Await for any orphan tasks such as cleaning/destroying kernels
        await asyncio.gather(*self._orphan_tasks, return_exceptions=True)

        # Stop timers.
        if self.scan_images_timer is not None:
            self.scan_images_timer.cancel()
            await self.scan_images_timer
        if self.hb_timer is not None:
            self.hb_timer.cancel()
            await self.hb_timer
        if self.live_stat_timer is not None:
            self.live_stat_timer.cancel()
            await self.live_stat_timer
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
        if self.stat_sync_task is not None:
            self.stat_sync_task.cancel()
            await self.stat_sync_task

        if self.redis_stat_pool is not None:
            self.redis_stat_pool.close()
            await self.redis_stat_pool.wait_closed()

        # Stop handlign agent sock.
        # (But we don't remove the socket file)
        if self.agent_sock_task is not None:
            self.agent_sock_task.cancel()
            await self.agent_sock_task

        # Notify the gateway.
        if self.event_sock is not None:
            await self.send_event('instance_terminated', 'shutdown')
            self.event_sock.close()

        self.zmq_ctx.term()

    async def handle_agent_socket(self):
        '''
        A simple request-reply socket handler for in-container processes.
        For ease of implementation in low-level languages such as C,
        it uses a simple C-friendly ZeroMQ-based multipart messaging protocol.

        Request message:
            The first part is the requested action as string,
            The second part and later are arguments.

        Reply message:
            The first part is a 32-bit integer (int in C)
                (0: success)
                (-1: generic unhandled error)
                (-2: invalid action)
            The second part and later are arguments.

        All strings are UTF-8 encoded.
        '''
        my_uid = os.getuid()
        my_gid = os.getgid()
        kernel_uid = self.config['container']['kernel-uid']
        kernel_gid = self.config['container']['kernel-gid']
        try:
            agent_sock = self.zmq_ctx.socket(zmq.REP)
            agent_sock.bind(f'ipc://{self.agent_sockpath}')
            if my_uid == 0:
                os.chown(self.agent_sockpath, kernel_uid, kernel_gid)
            else:
                if my_uid != kernel_uid:
                    log.error('The UID of agent ({}) must be same to the container UID ({}).',
                                my_uid, kernel_uid)
                if my_gid != kernel_gid:
                    log.error('The GID of agent ({}) must be same to the container GID ({}).',
                                my_gid, kernel_gid)
            while True:
                msg = await agent_sock.recv_multipart()
                if not msg:
                    break
                try:
                    if msg[0] == b'host-pid-to-container-pid':
                        container_id = msg[1].decode()
                        host_pid = struct.unpack('i', msg[2])[0]
                        container_pid = await host_pid_to_container_pid(
                            container_id, host_pid)
                        reply = [
                            struct.pack('i', 0),
                            struct.pack('i', container_pid),
                        ]
                    elif msg[0] == b'container-pid-to-host-pid':
                        container_id = msg[1].decode()
                        container_pid = struct.unpack('i', msg[2])[0]
                        host_pid = await container_pid_to_host_pid(
                            container_id, container_pid)
                        reply = [
                            struct.pack('i', 0),
                            struct.pack('i', host_pid),
                        ]
                    else:
                        reply = [struct.pack('i', -2), b'Invalid action']
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    reply = [struct.pack('i', -1), f'Error: {e}'.encode('utf-8')]
                await agent_sock.send_multipart(reply)
        except asyncio.CancelledError:
            pass
        except zmq.ZMQError:
            log.exception('zmq socket error with {}', self.agent_sockpath)
        finally:
            agent_sock.close()

    async def create_kernel(self, kernel_id, kernel_config, restarting=False):

        await self.send_event('kernel_preparing', kernel_id)

        # Read image-specific labels and settings
        image_ref = ImageRef(
            kernel_config['image']['canonical'],
            [kernel_config['image']['registry']['name']])
        environ: dict = kernel_config.get('environ', {})
        extra_mount_list = await get_extra_volumes(self.docker, image_ref.short)

        try:
            # Find the exact image using a digest reference
            digest_ref = f"{kernel_config['image']['digest']}"
            await self.docker.images.inspect(digest_ref)
            log.info('found the local up-to-date image for {}', image_ref.canonical)
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
                log.info('pulling image {} from registry', image_ref.canonical)
                repo_digest = kernel_config['image'].get('repo_digest')
                if repo_digest is not None:
                    await self.docker.images.pull(
                        f'{image_ref.short}@{repo_digest}',
                        auth=auth_config)
                else:
                    await self.docker.images.pull(
                        image_ref.canonical,
                        auth=auth_config)
            else:
                raise
        await self.send_event('kernel_creating', kernel_id)
        image_labels = kernel_config['image']['labels']

        version        = int(image_labels.get('ai.backend.kernelspec', '1'))
        envs_corecount = image_labels.get('ai.backend.envs.corecount', '')
        envs_corecount = envs_corecount.split(',') if envs_corecount else []
        kernel_features = set(image_labels.get('ai.backend.features', '').split())

        scratch_dir = (self.config['container']['scratch-root'] / kernel_id).resolve()
        tmp_dir = (self.config['container']['scratch-root'] / f'{kernel_id}_tmp').resolve()
        config_dir = scratch_dir / 'config'
        work_dir = scratch_dir / 'work'

        # PHASE 1: Read existing resource spec or devise a new resource spec.

        if restarting:
            with open(config_dir / 'resource.txt', 'r') as f:
                resource_spec = KernelResourceSpec.read_from_file(f)
        else:
            # resource_slots is already sanitized by the manager.
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

        # PHASE 2: Apply the resource spec.

        # Inject Backend.AI-intrinsic env-variables for gosu
        if KernelFeatures.UID_MATCH in kernel_features:
            uid = self.config['container']['kernel-uid']
            gid = self.config['container']['kernel-gid']
            environ['LOCAL_USER_ID'] = str(uid)
            environ['LOCAL_GROUP_ID'] = str(gid)

        # Inject Backend.AI-intrinsic mount points and extra mounts
        mounts = [
            Mount(MountTypes.BIND, config_dir, '/home/config',
                  MountPermission.READ_ONLY),
            Mount(MountTypes.BIND, work_dir, '/home/work/',
                  MountPermission.READ_WRITE),
        ]
        if (sys.platform.startswith('linux') and
            self.config['container']['scratch-type'] == 'memory'):
            mounts.append(Mount(MountTypes.BIND, tmp_dir, '/tmp',
                                MountPermission.READ_WRITE))
        mounts.extend(Mount(MountTypes.VOLUME, v.name, v.container_path, v.mode)
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
                mounts.append(mount)
        else:
            # Ensure that we have intrinsic slots.
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
                try:
                    resource_spec.allocations[dev_type] = \
                        computer_set.alloc_map.allocate(device_specific_slots,
                                                        context_tag=dev_type)
                except InsufficientResource:
                    log.info('insufficient resource: {} of {}\n'
                             '(alloc map: {})',
                             device_specific_slots, dev_type,
                             computer_set.alloc_map.allocations)
                    raise

            # Realize vfolder mounts.
            for vfolder in vfolders:
                if len(vfolder) == 4:
                    folder_name, folder_host, folder_id, folder_perm = vfolder
                elif len(vfolder) == 3:  # legacy managers
                    folder_name, folder_host, folder_id = vfolder
                    folder_perm = 'rw'
                else:
                    raise RuntimeError(
                        'Unexpected number of vfolder mount detail tuple size')
                host_path = (self.config['vfolder']['mount'] / folder_host /
                             self.config['vfolder']['fsprefix'] / folder_id)
                kernel_path = Path(f'/home/work/{folder_name}')
                folder_perm = MountPermission(folder_perm)
                if folder_perm == MountPermission.RW_DELETE:
                    # TODO: enforce readable/writable but not deletable
                    # (Currently docker's READ_WRITE includes DELETE)
                    folder_perm = MountPermission.READ_WRITE
                mount = Mount(MountTypes.BIND, host_path, kernel_path, folder_perm)
                resource_spec.mounts.append(mount)
                mounts.append(mount)

            # should no longer be used!
            del vfolders

        # Inject Backend.AI-intrinsic env-variables for libbaihook and gosu
        cpu_core_count = len(resource_spec.allocations['cpu']['cpu'])
        environ.update({k: str(cpu_core_count) for k in envs_corecount})

        def _mount(type, src, target, perm='ro', opts=None):
            nonlocal mounts
            mounts.append(Mount(type, src, target, MountPermission(perm), opts=opts))

        # Inject Backend.AI kernel runner dependencies.
        distro = image_labels.get('ai.backend.base-distro', 'ubuntu16.04')
        arch = platform.machine()
        entrypoint_sh_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../runner/entrypoint.sh'))
        suexec_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', f'../runner/su-exec.{distro}.bin'))
        if self.config['container']['sandbox-type'] == 'jail':
            jail_path = Path(pkg_resources.resource_filename(
                'ai.backend.agent', f'../runner/jail.{distro}.bin'))
        hook_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', f'../runner/libbaihook.{distro}.{arch}.so'))
        kernel_pkg_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../kernel'))
        helpers_pkg_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../helpers'))
        jupyter_custom_css_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../runner/jupyter-custom.css'))
        logo_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../runner/logo.svg'))
        font_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../runner/roboto.ttf'))
        font_italic_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../runner/roboto-italic.ttf'))

        _mount(MountTypes.BIND, self.agent_sockpath, '/opt/kernel/agent.sock', perm='rw')
        _mount(MountTypes.BIND, entrypoint_sh_path.resolve(), '/opt/kernel/entrypoint.sh')
        _mount(MountTypes.BIND, suexec_path.resolve(), '/opt/kernel/su-exec')
        if self.config['container']['sandbox-type'] == 'jail':
            _mount(MountTypes.BIND, jail_path.resolve(), '/opt/kernel/jail')
        _mount(MountTypes.BIND, hook_path.resolve(), '/opt/kernel/libbaihook.so')

        matched_distro, krunner_volume = match_krunner_volume(
            self.config['container']['krunner-volumes'], distro)
        _mount(MountTypes.VOLUME, krunner_volume, '/opt/backend.ai')
        _mount(MountTypes.BIND, kernel_pkg_path.resolve(),
                                '/opt/backend.ai/lib/python3.6/site-packages/ai/backend/kernel')
        _mount(MountTypes.BIND, helpers_pkg_path.resolve(),
                                '/opt/backend.ai/lib/python3.6/site-packages/ai/backend/helpers')

        # Since these files are bind-mounted inside a bind-mounted directory,
        # we need to touch them first to avoid their "ghost" files are created
        # as root in the host-side filesystem, which prevents deletion of scratch
        # directories when the agent is running as non-root.
        (work_dir / '.jupyter' / 'custom').mkdir(parents=True, exist_ok=True)
        (work_dir / '.jupyter' / 'custom' / 'custom.css').write_bytes(b'')
        (work_dir / '.jupyter' / 'custom' / 'logo.svg').write_bytes(b'')
        (work_dir / '.jupyter' / 'custom' / 'roboto.ttf').write_bytes(b'')
        (work_dir / '.jupyter' / 'custom' / 'roboto-italic.ttf').write_bytes(b'')
        _mount(MountTypes.BIND, jupyter_custom_css_path.resolve(),
                                '/home/work/.jupyter/custom/custom.css')
        _mount(MountTypes.BIND, logo_path.resolve(), '/home/work/.jupyter/custom/logo.svg')
        _mount(MountTypes.BIND, font_path.resolve(), '/home/work/.jupyter/custom/roboto.ttf')
        _mount(MountTypes.BIND, font_italic_path.resolve(),
                                '/home/work/.jupyter/custom/roboto-italic.ttf')
        environ['LD_PRELOAD'] = '/opt/kernel/libbaihook.so'
        if self.config['debug']['coredump']['enabled']:
            _mount(MountTypes.BIND, self.config['debug']['coredump']['path'],
                                    self.config['debug']['coredump']['core_path'],
                                    perm='rw')

        # Inject ComputeDevice-specific env-varibles and hooks
        computer_docker_args = {}
        for dev_type, device_alloc in resource_spec.allocations.items():
            computer_set = self.computers[dev_type]
            update_nested_dict(computer_docker_args,
                               await computer_set.klass.generate_docker_args(
                                   self.docker, device_alloc))
            alloc_sum = Decimal(0)
            for dev_id, per_dev_alloc in device_alloc.items():
                alloc_sum += sum(per_dev_alloc.values())
            if alloc_sum > 0:
                hook_paths = await computer_set.klass.get_hooks(distro, arch)
                if hook_paths:
                    log.debug('accelerator {} provides hooks: {}',
                              computer_set.klass.__name__,
                              ', '.join(map(str, hook_paths)))
                for hook_path in hook_paths:
                    container_hook_path = '/opt/kernel/lib{}{}.so'.format(
                        computer_set.klass.key, secrets.token_hex(6),
                    )
                    _mount(MountTypes.BIND, hook_path, container_hook_path)
                    environ['LD_PRELOAD'] += ':' + container_hook_path

        # PHASE 3: Store the resource spec.

        if restarting:
            pass
        else:
            os.makedirs(scratch_dir, exist_ok=True)
            if (sys.platform.startswith('linux') and
                self.config['container']['scratch-type'] == 'memory'):
                os.makedirs(tmp_dir, exist_ok=True)
                await create_scratch_filesystem(scratch_dir, 64)
                await create_scratch_filesystem(tmp_dir, 64)
            os.makedirs(work_dir, exist_ok=True)
            os.makedirs(work_dir / '.jupyter', exist_ok=True)
            if KernelFeatures.UID_MATCH in kernel_features:
                uid = self.config['container']['kernel-uid']
                gid = self.config['container']['kernel-gid']
                if os.getuid() == 0:  # only possible when I am root.
                    os.chown(work_dir, uid, gid)
                    os.chown(work_dir / '.jupyter', uid, gid)
            os.makedirs(config_dir, exist_ok=True)
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
                    computer_ctx = self.computers[dev_type]
                    kvpairs = \
                        await computer_ctx.klass.generate_resource_data(device_alloc)
                    for k, v in kvpairs.items():
                        f.write(f'{k}={v}\n')

        # PHASE 4: Run!
        log.info('kernel {0} starting with resource spec: \n',
                 pformat(attr.asdict(resource_spec)))

        # TODO: Refactor out as separate "Docker execution driver plugin" (#68)
        #   - Refactor volumes/mounts lists to a plugin "mount" API
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

        kernel_host = self.config['container']['kernel-host']
        if len(exposed_ports) > len(self.port_pool):
            raise RuntimeError('Container ports are not sufficiently available.')
        host_ports = []
        for eport in exposed_ports:
            hport = self.port_pool.pop()
            host_ports.append(hport)

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
        container_config = {
            'Image': image_ref.canonical,
            'Tty': True,
            'OpenStdin': True,
            'Privileged': False,
            'StopSignal': 'SIGINT',
            'ExposedPorts': {
                f'{port}/tcp': {} for port in exposed_ports
            },
            'EntryPoint': ["/opt/kernel/entrypoint.sh"],
            'Cmd': cmdargs,
            'Env': [f'{k}={v}' for k, v in environ.items()],
            'WorkingDir': '/home/work',
            'HostConfig': {
                'Init': True,
                'Mounts': [
                    {
                        'Target': str(mount.target),
                        'Source': str(mount.source),
                        'Type': mount.type.value,
                        'ReadOnly': mount.permission == MountPermission.READ_ONLY,
                        f'{mount.type.value.capitalize()}Options':
                            mount.opts if mount.opts else {},
                    }
                    for mount in mounts
                ],
                'PortBindings': {
                    f'{eport}/tcp': [{'HostPort': str(hport),
                                      'HostIp': kernel_host}]
                    for eport, hport in zip(exposed_ports, host_ports)
                },
                'PublishAllPorts': False,  # we manage port mapping manually!
            },
        }
        if self.config['container']['sandbox-type'] == 'jail':
            container_config['HostConfig']['SecurityOpt'] = [
                'seccomp=unconfined',
                'apparmor=unconfined',
            ]
        update_nested_dict(container_config, computer_docker_args)
        kernel_name = f"kernel.{image_ref.name.split('/')[-1]}.{kernel_id}"
        log.debug('container config: {!r}', container_config)

        # We are all set! Create and start the container.
        try:
            container = await self.docker.containers.create(
                config=container_config, name=kernel_name)
            cid = container._id

            resource_spec.container_id = cid
            # Write resource.txt again to update the contaienr id.
            with open(config_dir / 'resource.txt', 'w') as f:
                resource_spec.write_to_file(f)
                for dev_type, device_alloc in resource_spec.allocations.items():
                    computer_ctx = self.computers[dev_type]
                    kvpairs = \
                        await computer_ctx.klass.generate_resource_data(device_alloc)
                    for k, v in kvpairs.items():
                        f.write(f'{k}={v}\n')

            self.stat_sync_states[cid] = StatSyncState(kernel_id)
            async with spawn_stat_synchronizer(self.config['_src'],
                                               self.stat_sync_sockpath,
                                               self.stat_ctx.mode, cid):
                await container.start()

            # Get attached devices information (including model_name).
            attached_devices = {}
            for dev_type, device_alloc in resource_spec.allocations.items():
                computer_set = self.computers[dev_type]
                devices = await computer_set.klass.get_attached_devices(device_alloc)
                attached_devices[dev_type] = devices
        except asyncio.CancelledError:
            raise
        except Exception:
            # Oops, we have to restore the allocated resources!
            if (sys.platform.startswith('linux') and
                self.config['container']['scratch-type'] == 'memory'):
                await destroy_scratch_filesystem(scratch_dir)
                await destroy_scratch_filesystem(tmp_dir)
                shutil.rmtree(tmp_dir)
            shutil.rmtree(scratch_dir)
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

        self.container_registry[kernel_id] = DockerKernel(
            kernel_id,
            image_ref,
            version,
            service_ports=list(service_ports.values()),
            resource_spec=resource_spec,
            data={
                'container_id': container._id,
                'kernel_host': kernel_host,
                'repl_in_port': repl_in_port,
                'repl_out_port': repl_out_port,
                'stdin_port': stdin_port,    # legacy
                'stdout_port': stdout_port,  # legacy
                'host_ports': host_ports,
            })
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
            'attached_devices': attached_devices,
        }

    async def destroy_kernel(self, kernel_id, reason):
        try:
            kernel_obj = self.container_registry[kernel_id]
            cid = kernel_obj['container_id']
        except KeyError:
            kernel_obj = None
            log.warning('destroy_kernel({0}) kernel missing (already dead?)',
                        kernel_id)

            async def force_cleanup():
                await self.clean_kernel(kernel_id)
                await self.send_event('kernel_terminated',
                                      kernel_id, 'self-terminated',
                                      None)

            self._orphan_tasks.discard(asyncio.Task.current_task())
            await asyncio.shield(force_cleanup())
            return None
        container = self.docker.containers.container(cid)
        if kernel_obj.runner is not None:
            await kernel_obj.runner.close()
        try:
            await container.kill()
            # Collect the last-moment statistics.
            if cid in self.stat_sync_states:
                s = self.stat_sync_states[cid]
                await s.terminated.wait()
                last_stat = s.last_stat
                self.stat_sync_states.pop(cid, None)
                last_stat = {
                    key: metric.to_serializable_dict()
                    for key, metric in last_stat.items()
                }
                # make it client compatible with legacy
                last_stat['version'] = 2
                return last_stat
            # The container will be deleted in the docker monitoring coroutine.
            return None
        except DockerError as e:
            if e.status == 409 and 'is not running' in e.message:
                # already dead
                log.warning('destroy_kernel({0}) already dead', kernel_id)
                pass
            elif e.status == 404:
                log.warning('destroy_kernel({0}) kernel missing, '
                            'forgetting this kernel', kernel_id)
                resource_spec = kernel_obj.resource_spec
                for accel_key, accel_alloc in resource_spec.allocations.items():
                    self.computers[accel_key].alloc_map.free(accel_alloc)
                self.container_registry.pop(kernel_id, None)
            else:
                log.exception('destroy_kernel({0}) kill error', kernel_id)
                self.error_monitor.capture_exception()
        except asyncio.CancelledError:
            log.exception('destroy_kernel({0}) operation cancelled', kernel_id)
            raise
        except Exception:
            log.exception('destroy_kernel({0}) unexpected error', kernel_id)
            self.error_monitor.capture_exception()
        finally:
            self._orphan_tasks.discard(asyncio.Task.current_task())

    async def execute(self, api_version, kernel_id,
                      run_id, mode, text, opts,
                      flush_timeout):
        # Wait for the kernel restarting if it's ongoing...
        restart_tracker = self.restarting_kernels.get(kernel_id)
        if restart_tracker:
            await restart_tracker.done_event.wait()

        try:
            kernel_obj = self.container_registry[kernel_id]
            result = await kernel_obj.execute(
                run_id, mode, text,
                opts=opts,
                flush_timeout=flush_timeout,
                api_version=api_version)
        except KeyError:
            await self.send_event('kernel_terminated',
                                  kernel_id, 'self-terminated',
                                  None)
            raise RuntimeError(f'The container for kernel {kernel_id} is not found! '
                                '(might be terminated--try it again)') from None

        if result['status'] in ('finished', 'exec-timeout'):
            log.debug('_execute({0}) {1}', kernel_id, result['status'])
        if result['status'] == 'exec-timeout':
            self._orphan_tasks.add(
                self.loop.create_task(self.destroy_kernel(kernel_id, 'exec-timeout'))
            )
        return {
            **result,
            'files': [],  # kept for API backward-compatibility
        }

    async def get_completions(self, kernel_id, text, opts):
        return await self.container_registry[kernel_id].get_completions(text, opts)

    async def get_logs(self, kernel_id):
        return await self.container_registry[kernel_id].get_logs()

    async def interrupt_kernel(self, kernel_id):
        return await self.container_registry[kernel_id].interrupt_kernel()

    async def start_service(self, kernel_id, service, opts):
        return await self.container_registry[kernel_id].start_service(service, opts)

    async def accept_file(self, kernel_id, filename, filedata):
        return await self.container_registry[kernel_id].accept_file(filename, filedata)

    async def download_file(self, kernel_id, filepath):
        return await self.container_registry[kernel_id].download_file(filepath)

    async def list_files(self, kernel_id: str, path: str):
        return await self.container_registry[kernel_id].list_files(path)

    async def heartbeat(self, interval):
        '''
        Send my status information and available kernel images.
        '''
        res_slots = {}
        for cctx in self.computers.values():
            for slot_key, slot_type in cctx.klass.slot_types:
                res_slots[slot_key] = (
                    slot_type,
                    str(self.slots.get(slot_key, 0)),
                )
        agent_info = {
            'ip': str(self.config['agent']['rpc-listen-addr'].host),
            'region': self.config['agent']['region'],
            'scaling_group': self.config['agent']['scaling-group'],
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
                self._orphan_tasks.add(
                    self.loop.create_task(self.clean_kernel(kernel_id))
                )

    async def clean_kernel(self, kernel_id):
        try:
            kernel_obj = self.container_registry[kernel_id]
            found = True
        except KeyError:
            kernel_obj = None
            found = False
        try:
            if found:
                container_id = kernel_obj['container_id']
                container = self.docker.containers.container(container_id)
                if kernel_obj.runner is not None:
                    await kernel_obj.runner.close()

                # When the agent restarts with a different port range, existing
                # containers' host ports may not belong to the new port range.
                if not self.config['debug']['skip-container-deletion']:
                    try:
                        with timeout(20):
                            await container.delete()
                    except DockerError as e:
                        if e.status == 409 and 'already in progress' in e.message:
                            pass
                        elif e.status == 404:
                            pass
                        else:
                            log.exception(
                                'unexpected docker error while deleting container (k:{}, c:{})',
                                kernel_id, container_id)
                    except asyncio.TimeoutError:
                        log.warning('container deletion timeout (k:{}, c:{})',
                                    kernel_id, container_id)
                    finally:
                        port_range = self.config['container']['port-range']
                        restored_ports = [*filter(
                            lambda p: port_range[0] <= p <= port_range[1],
                            kernel_obj['host_ports'])]
                        self.port_pool.update(restored_ports)

            if kernel_id in self.restarting_kernels:
                self.restarting_kernels[kernel_id].destroy_event.set()

            if found:
                scratch_root = self.config['container']['scratch-root']
                scratch_dir = scratch_root / kernel_id
                tmp_dir = scratch_root / f'{kernel_id}_tmp'
                try:
                    if (sys.platform.startswith('linux') and
                        self.config['container']['scratch-type'] == 'memory'):
                        await destroy_scratch_filesystem(scratch_dir)
                        await destroy_scratch_filesystem(tmp_dir)
                        shutil.rmtree(tmp_dir)
                    shutil.rmtree(scratch_dir)
                except FileNotFoundError:
                    pass
                resource_spec = kernel_obj.resource_spec
                for accel_key, accel_alloc in resource_spec.allocations.items():
                    self.computers[accel_key].alloc_map.free(accel_alloc)
                await kernel_obj.close()
                self.container_registry.pop(kernel_id, None)
        except Exception:
            log.exception('unexpected error while cleaning up kernel (k:{})', kernel_id)
        finally:
            self._orphan_tasks.discard(asyncio.Task.current_task())
            if kernel_id in self.blocking_cleans:
                self.blocking_cleans[kernel_id].set()

    async def clean_old_kernels(self, interval):
        now = time.monotonic()
        keys = tuple(self.container_registry.keys())
        tasks = []
        for kernel_id in keys:
            try:
                last_used = self.container_registry[kernel_id]['last_used']
                idle_timeout = \
                    self.container_registry[kernel_id].resource_spec \
                    .idle_timeout
                if idle_timeout > 0 and now - last_used > idle_timeout:
                    log.info('destroying kernel {0} as clean-up', kernel_id)
                    task = asyncio.ensure_future(
                        self.destroy_kernel(kernel_id, 'idle-timeout'))
                    tasks.append(task)
            except KeyError:
                # The kernel may be destroyed by other means?
                pass
        await asyncio.gather(*tasks)

    async def clean_all_kernels(self, blocking=False):
        log.info('cleaning all kernels...')
        kernel_ids = [*self.container_registry.keys()]
        tasks = []
        if blocking:
            for kernel_id in kernel_ids:
                self.blocking_cleans[kernel_id] = asyncio.Event()
        for kernel_id in kernel_ids:
            task = asyncio.ensure_future(
                self.destroy_kernel(kernel_id, 'agent-termination'))
            tasks.append(task)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for kernel_id, result in zip(kernel_ids, results):
            log.info('force-terminated kernel: {} (result: {})', kernel_id, result)
        if blocking:
            waiters = [self.blocking_cleans[kernel_id].wait()
                        for kernel_id in kernel_ids]
            await asyncio.gather(*waiters)
            self.blocking_cleans.clear()
