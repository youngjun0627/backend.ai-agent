from __future__ import annotations

import asyncio
import base64
from decimal import Decimal
from functools import partial
import json
import logging
import os
from pathlib import Path
import pickle
import pkg_resources
import platform
import re
import secrets
import shutil
import signal
import struct
import sys
from typing import (
    Any,
    FrozenSet,
    Dict,
    List,
    Literal,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Sequence,
    Tuple,
    Union,
    cast,
    TYPE_CHECKING,
)

from aiodocker.docker import Docker, DockerContainer
from aiodocker.exceptions import DockerError, DockerContainerError
import aiotools
from async_timeout import timeout
import attr
import zmq

from ai.backend.common.docker import (
    ImageRef,
    MIN_KERNELSPEC,
    MAX_KERNELSPEC,
)
from ai.backend.common.exception import ImageNotAvailable
from ai.backend.common.logging import BraceStyleAdapter, pretty
from ai.backend.common.plugin.monitor import ErrorPluginContext, StatsPluginContext
from ai.backend.common.types import (
    AutoPullBehavior,
    BinarySize,
    ImageRegistry,
    KernelCreationConfig,
    KernelCreationResult,
    KernelId,
    ContainerId,
    DeviceName,
    SlotName,
    MountPermission,
    MountTypes,
    ResourceSlot,
    SessionTypes,
    ServicePortProtocols,
    Sentinel,
    MountTuple5,
    MountTuple4,
    MountTuple3,
    current_resource_slots,
)
from ai.backend.common.utils import AsyncFileWriter, current_loop
from .kernel import DockerKernel
from .resources import detect_resources
from .utils import PersistentServiceContainer
from ..exception import UnsupportedResource, InsufficientResource
from ..fs import create_scratch_filesystem, destroy_scratch_filesystem
from ..kernel import match_distro_data, KernelFeatures
from ..resources import (
    Mount,
    KernelResourceSpec,
)
from ..agent import (
    AbstractAgent,
    ACTIVE_STATUS_SET,
    ipc_base_path,
)
from ..proxy import proxy_connection, DomainSocketProxy
from ..resources import (
    AbstractComputePlugin,
    known_slot_types,
)
from ..server import (
    get_extra_volumes,
)
from ..types import (
    Container, Port, ContainerStatus,
    ContainerLifecycleEvent, LifecycleEvent,
)
from ..utils import (
    update_nested_dict,
    get_kernel_id_from_container,
    host_pid_to_container_pid,
    container_pid_to_host_pid,
    parse_service_ports,
)

if TYPE_CHECKING:
    from ai.backend.common.etcd import AsyncEtcd

log = BraceStyleAdapter(logging.getLogger(__name__))
eof_sentinel = Sentinel.TOKEN


def container_from_docker_container(src: DockerContainer) -> Container:
    ports = []
    for private_port, host_ports in src['NetworkSettings']['Ports'].items():
        private_port = int(private_port.split('/')[0])
        if host_ports is None:
            host_ip = '127.0.0.1'
            host_port = 0
        else:
            host_ip = host_ports[0]['HostIp']
            host_port = int(host_ports[0]['HostPort'])
        ports.append(Port(host_ip, private_port, host_port))
    return Container(
        id=src._id,
        status=src['State']['Status'],
        image=src['Config']['Image'],
        labels=src['Config']['Labels'],
        ports=ports,
        backend_obj=src,
    )


def _DockerError_reduce(self):
    return (
        type(self),
        (self.status, {'message': self.message}, *self.args),
    )


def _DockerContainerError_reduce(self):
    return (
        type(self),
        (self.status, {'message': self.message}, self.container_id, *self.args),
    )


class DockerAgent(AbstractAgent):

    docker: Docker
    monitor_docker_task: asyncio.Task
    agent_sockpath: Path
    agent_sock_task: asyncio.Task
    scan_images_timer: asyncio.Task

    def __init__(
        self,
        etcd: AsyncEtcd,
        local_config: Mapping[str, Any],
        *,
        stats_monitor: StatsPluginContext,
        error_monitor: ErrorPluginContext,
        skip_initial_scan: bool = False,
    ) -> None:
        super().__init__(
            etcd,
            local_config,
            stats_monitor=stats_monitor,
            error_monitor=error_monitor,
            skip_initial_scan=skip_initial_scan,
        )

        # Monkey-patch pickling support for aiodocker exceptions
        # FIXME: remove if https://github.com/aio-libs/aiodocker/issues/442 is merged
        DockerError.__reduce__ = _DockerError_reduce                     # type: ignore
        DockerContainerError.__reduce__ = _DockerContainerError_reduce   # type: ignore

    async def __ainit__(self) -> None:
        self.docker = Docker()
        if not self._skip_initial_scan:
            docker_version = await self.docker.version()
            log.info('running with Docker {0} with API {1}',
                     docker_version['Version'], docker_version['ApiVersion'])
        await super().__ainit__()
        (ipc_base_path / 'container').mkdir(parents=True, exist_ok=True)
        self.agent_sockpath = ipc_base_path / 'container' / f'agent.{self.agent_id}.sock'
        socket_relay_container = PersistentServiceContainer(
            self.docker,
            'backendai-socket-relay:latest',
            {
                'Cmd': [
                    f"UNIX-LISTEN:/ipc/{self.agent_sockpath.name},unlink-early,fork,mode=777",
                    f"TCP-CONNECT:127.0.0.1:{self.local_config['agent']['agent-sock-port']}",
                ],
                'HostConfig': {
                    'Mounts': [
                        {
                            'Type': 'bind',
                            'Source': '/tmp/backend.ai/ipc/container',
                            'Target': '/ipc',
                        },
                    ],
                    'NetworkMode': 'host',
                },
            },
        )
        await socket_relay_container.ensure_running_latest()
        self.agent_sock_task = asyncio.create_task(self.handle_agent_socket())
        self.monitor_docker_task = asyncio.create_task(self.monitor_docker_events())

    async def shutdown(self, stop_signal: signal.Signals):
        try:
            await super().shutdown(stop_signal)
        finally:
            # Stop docker event monitoring.
            if self.monitor_docker_task is not None:
                self.monitor_docker_task.cancel()
                await self.monitor_docker_task
            await self.docker.close()

        # Stop handlign agent sock.
        # (But we don't remove the socket file)
        if self.agent_sock_task is not None:
            self.agent_sock_task.cancel()
            await self.agent_sock_task

    async def detect_resources(self) -> Tuple[
        Mapping[DeviceName, AbstractComputePlugin],
        Mapping[SlotName, Decimal]
    ]:
        return await detect_resources(self.etcd, self.local_config)

    async def enumerate_containers(
        self,
        status_filter: FrozenSet[ContainerStatus] = ACTIVE_STATUS_SET,
    ) -> Sequence[Tuple[KernelId, Container]]:
        result = []
        fetch_tasks = []
        for container in (await self.docker.containers.list()):

            async def _fetch_container_info(container):
                try:
                    kernel_id = await get_kernel_id_from_container(container)
                    if kernel_id is None:
                        return
                    if container['State']['Status'] in status_filter:
                        await container.show()
                        result.append(
                            (
                                kernel_id,
                                container_from_docker_container(container),
                            )
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception(
                        'error while fetching container information (cid: {})',
                        container._id)

            fetch_tasks.append(_fetch_container_info(container))

        await asyncio.gather(*fetch_tasks, return_exceptions=True)
        return result

    async def scan_images(self) -> Mapping[str, str]:
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
                if labels is None or 'ai.backend.kernelspec' not in labels:
                    continue
                kernelspec = int(labels['ai.backend.kernelspec'])
                if MIN_KERNELSPEC <= kernelspec <= MAX_KERNELSPEC:
                    updated_images[repo_tag] = img_detail['Id']
        for added_image in (updated_images.keys() - self.images.keys()):
            log.debug('found kernel image: {0}', added_image)
        for removed_image in (self.images.keys() - updated_images.keys()):
            log.debug('removed kernel image: {0}', removed_image)
        return updated_images

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
        try:
            agent_sock = self.zmq_ctx.socket(zmq.REP)
            agent_sock.bind(f"tcp://127.0.0.1:{self.local_config['agent']['agent-sock-port']}")
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

    async def pull_image(self, image_ref: ImageRef, registry_conf: ImageRegistry) -> None:
        auth_config = None
        reg_user = registry_conf.get('username')
        reg_passwd = registry_conf.get('password')
        if reg_user and reg_passwd:
            encoded_creds = base64.b64encode(
                f'{reg_user}:{reg_passwd}'.encode('utf-8')) \
                .decode('ascii')
            auth_config = {
                'auth': encoded_creds,
            }
        log.info('pulling image {} from registry', image_ref.canonical)
        await self.docker.images.pull(
            image_ref.canonical,
            auth=auth_config)

    async def check_image(self, image_ref: ImageRef, image_id: str, auto_pull: AutoPullBehavior) -> bool:
        try:
            image_info = await self.docker.images.inspect(image_ref.canonical)
            if auto_pull == AutoPullBehavior.DIGEST:
                if image_info['Id'] != image_id:
                    return True
            log.info('found the local up-to-date image for {}', image_ref.canonical)
        except DockerError as e:
            if e.status == 404:
                if auto_pull == AutoPullBehavior.DIGEST:
                    return True
                elif auto_pull == AutoPullBehavior.TAG:
                    return True
                elif auto_pull == AutoPullBehavior.NONE:
                    raise ImageNotAvailable(image_ref)
            else:
                raise
        return False

    async def create_kernel(self, kernel_id: KernelId, kernel_config: KernelCreationConfig, *,
                            restarting: bool = False) -> KernelCreationResult:

        await self.produce_event('kernel_preparing', str(kernel_id))

        log.debug('Kernel Creation Config: {0}', json.dumps(kernel_config))
        # Read image-specific labels and settings
        image_ref = ImageRef(
            kernel_config['image']['canonical'],
            [kernel_config['image']['registry']['name']])
        environ: MutableMapping[str, str] = {**kernel_config['environ']}
        extra_mount_list = await get_extra_volumes(self.docker, image_ref.short)
        internal_data: Mapping[str, Any] = kernel_config.get('internal_data') or {}

        do_pull = await self.check_image(
            image_ref,
            kernel_config['image']['digest'],
            AutoPullBehavior(kernel_config.get('auto_pull', 'digest')),
        )
        if do_pull:
            await self.produce_event('kernel_pulling',
                                     str(kernel_id), image_ref.canonical)
            await self.pull_image(image_ref, kernel_config['image']['registry'])

        await self.produce_event('kernel_creating', str(kernel_id))
        image_labels = kernel_config['image']['labels']
        log.debug('image labels:\n{}', pretty(image_labels))
        version = int(image_labels.get('ai.backend.kernelspec', '1'))
        label_envs_corecount = image_labels.get('ai.backend.envs.corecount', '')
        envs_corecount = label_envs_corecount.split(',') if label_envs_corecount else []
        kernel_features = set(image_labels.get('ai.backend.features', '').split())

        scratch_dir = (self.local_config['container']['scratch-root'] / str(kernel_id)).resolve()
        tmp_dir = (self.local_config['container']['scratch-root'] / f'{kernel_id}_tmp').resolve()
        config_dir = scratch_dir / 'config'
        work_dir = scratch_dir / 'work'
        loop = current_loop()

        environ['BACKENDAI_KERNEL_ID'] = str(kernel_id)

        # PHASE 1: Read existing resource spec or devise a new resource spec.

        if restarting:
            resource_spec = await loop.run_in_executor(
                None,
                self._kernel_resource_spec_read,
                config_dir / 'resource.txt')
            resource_opts = None
        else:
            slots = ResourceSlot.from_json(kernel_config['resource_slots'])
            # accept unknown slot type with zero values
            # but reject if they have non-zero values.
            for st, sv in slots.items():
                if st not in known_slot_types and sv != Decimal(0):
                    raise UnsupportedResource(st)
            # sanitize the slots
            current_resource_slots.set(known_slot_types)
            slots = slots.normalize_slots(ignore_unknown=True)
            vfolders = kernel_config['mounts']
            vfolder_mount_map: Mapping[str, str] = {}
            if 'mount_map' in kernel_config.keys():
                vfolder_mount_map = kernel_config['mount_map']
            resource_spec = KernelResourceSpec(
                container_id='',
                allocations={},
                slots={**slots},  # copy
                mounts=[],
                scratch_disk_size=0,  # TODO: implement (#70)
                idle_timeout=kernel_config['idle_timeout'],
            )
            resource_opts = kernel_config.get('resource_opts', {})

        # PHASE 2: Apply the resource spec.

        # Inject Backend.AI-intrinsic env-variables for gosu
        if KernelFeatures.UID_MATCH in kernel_features:
            uid = self.local_config['container']['kernel-uid']
            gid = self.local_config['container']['kernel-gid']
            environ['LOCAL_USER_ID'] = str(uid)
            environ['LOCAL_GROUP_ID'] = str(gid)

        # Inject Backend.AI-intrinsic mount points and extra mounts
        mounts: List[Mount] = [
            Mount(MountTypes.BIND, config_dir, '/home/config',
                  MountPermission.READ_ONLY),
            Mount(MountTypes.BIND, work_dir, '/home/work',
                  MountPermission.READ_WRITE),
        ]
        if (sys.platform.startswith('linux') and
            self.local_config['container']['scratch-type'] == 'memory'):
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
            assert SlotName('cpu') in slots
            assert SlotName('mem') in slots

            # Realize ComputeDevice (including accelerators) allocations.
            dev_names: Set[SlotName] = set()
            for slot_name in slots.keys():
                dev_name = slot_name.split('.', maxsplit=1)[0]
                dev_names.add(dev_name)

            async with self.resource_lock:
                try:
                    for dev_name in dev_names:
                        computer_set = self.computers[dev_name]
                        device_specific_slots = {
                            slot_name: alloc
                            for slot_name, alloc in slots.items()
                            if slot_name.startswith(dev_name)
                        }
                        resource_spec.allocations[dev_name] = \
                            computer_set.alloc_map.allocate(
                                device_specific_slots,
                                context_tag=dev_name)
                except InsufficientResource:
                    log.info('insufficient resource: {} of {}\n'
                             '(alloc map: {})',
                             device_specific_slots, dev_name,
                             dict(computer_set.alloc_map.allocations))
                    raise

            # Realize vfolder mounts.
            for vfolder in vfolders:
                if len(vfolder) == 5:
                    folder_name, folder_host, folder_id, folder_perm_literal, host_path_raw = \
                        cast(MountTuple5, vfolder)
                    if host_path_raw:
                        host_path = Path(host_path_raw)
                    else:
                        host_path = (self.local_config['vfolder']['mount'] / folder_host /
                                     self.local_config['vfolder']['fsprefix'] / folder_id)
                elif len(vfolder) == 4:  # for backward compatibility
                    folder_name, folder_host, folder_id, folder_perm_literal = \
                        cast(MountTuple4, vfolder)
                    host_path = (self.local_config['vfolder']['mount'] / folder_host /
                                 self.local_config['vfolder']['fsprefix'] / folder_id)
                elif len(vfolder) == 3:  # legacy managers
                    folder_name, folder_host, folder_id = cast(MountTuple3, vfolder)
                    folder_perm_literal = 'rw'
                    host_path = (self.local_config['vfolder']['mount'] / folder_host /
                                 self.local_config['vfolder']['fsprefix'] / folder_id)
                else:
                    raise RuntimeError(
                        'Unexpected number of vfolder mount detail tuple size')
                if internal_data.get('prevent_vfolder_mounts', False):
                    # Only allow mount of ".logs" directory to prevent expose
                    # internal-only information, such as Docker credentials to user's ".docker" vfolder
                    # in image importer kernels.
                    if folder_name != '.logs':
                        continue
                if kernel_path_raw := vfolder_mount_map.get(folder_name):
                    if not kernel_path_raw.startswith('/home/work/'):  # type: ignore
                        raise ValueError(
                            f'Error while mounting {folder_name} to {kernel_path_raw}: '
                            'all vfolder mounts should be under /home/work')
                    kernel_path = Path(kernel_path_raw)  # type: ignore
                else:
                    kernel_path = Path(f'/home/work/{folder_name}')
                folder_perm = MountPermission(folder_perm_literal)
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
        cpu_core_count = len(resource_spec.allocations[DeviceName('cpu')][SlotName('cpu')])
        environ.update({k: str(cpu_core_count) for k in envs_corecount})

        def _mount(type: MountTypes,
                   src: Union[str, Path], target: Union[str, Path],
                   perm: Literal['ro', 'rw'] = 'ro',
                   opts: Mapping[str, Any] = None) -> None:
            nonlocal mounts
            mounts.append(Mount(type, src, target, MountPermission(perm), opts=opts))

        # Inject Backend.AI kernel runner dependencies.
        distro = image_labels.get('ai.backend.base-distro', 'ubuntu16.04')
        matched_distro, krunner_volume = match_distro_data(
            self.local_config['container']['krunner-volumes'], distro)
        matched_libc_style = 'glibc'
        if distro.startswith('alpine'):
            matched_libc_style = 'musl'
        krunner_pyver = '3.6'  # fallback
        if m := re.search(r'^([a-z-]+)(\d+(\.\d+)*)?$', matched_distro):
            matched_distro_pkgname = m.group(1).replace('-', '_')
            try:
                krunner_pyver = Path(pkg_resources.resource_filename(
                    f'ai.backend.krunner.{matched_distro_pkgname}',
                    f'krunner-python.{matched_distro}.txt',
                )).read_text().strip()
            except FileNotFoundError:
                pass
        log.debug('selected krunner: {}', matched_distro)
        log.debug('selected libc style: {}', matched_libc_style)
        log.debug('krunner volume: {}', krunner_volume)
        log.debug('krunner python: {}', krunner_pyver)
        arch = platform.machine()
        entrypoint_sh_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../runner/entrypoint.sh'))
        artifact_path = entrypoint_sh_path.parent

        def find_artifacts(pattern: str) -> Mapping[str, str]:
            artifacts = {}
            for p in artifact_path.glob(pattern):
                m = self._rx_distro.search(p.name)
                if m is not None:
                    artifacts[m.group(1)] = p.name
            return artifacts

        suexec_candidates = find_artifacts(f"su-exec.*.{arch}.bin")
        _, suexec_candidate = match_distro_data(suexec_candidates, distro)
        suexec_path = Path(pkg_resources.resource_filename(
            'ai.backend.runner', suexec_candidate))

        hook_candidates = find_artifacts(f"libbaihook.*.{arch}.so")
        _, hook_candidate = match_distro_data(hook_candidates, distro)
        hook_path = Path(pkg_resources.resource_filename(
            'ai.backend.runner', hook_candidate))

        sftp_server_candidates = find_artifacts(f"sftp-server.*.{arch}.bin")
        _, sftp_server_candidate = match_distro_data(sftp_server_candidates, distro)
        sftp_server_path = Path(pkg_resources.resource_filename(
            'ai.backend.runner', sftp_server_candidate))

        scp_candidates = find_artifacts(f"scp.*.{arch}.bin")
        _, scp_candidate = match_distro_data(scp_candidates, distro)
        scp_path = Path(pkg_resources.resource_filename(
            'ai.backend.runner', scp_candidate))

        jail_path: Optional[Path]
        if self.local_config['container']['sandbox-type'] == 'jail':
            jail_candidates = find_artifacts(f"jail.*.{arch}.bin")
            _, jail_candidate = match_distro_data(jail_candidates, distro)
            jail_path = Path(pkg_resources.resource_filename(
                'ai.backend.runner', jail_candidate))
        else:
            jail_path = None
        kernel_pkg_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '')).parent / 'kernel'
        helpers_pkg_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '')).parent / 'helpers'
        dropbear_path = Path(pkg_resources.resource_filename(
            'ai.backend.runner',
            f'dropbear.{matched_libc_style}.{arch}.bin'))
        dropbearconv_path = Path(pkg_resources.resource_filename(
            'ai.backend.runner',
            f'dropbearconvert.{matched_libc_style}.{arch}.bin'))
        dropbearkey_path = Path(pkg_resources.resource_filename(
            'ai.backend.runner',
            f'dropbearkey.{matched_libc_style}.{arch}.bin'))
        tmux_path = Path(pkg_resources.resource_filename(
            'ai.backend.runner', f'tmux.{matched_libc_style}.{arch}.bin'))
        dotfile_extractor_path = Path(pkg_resources.resource_filename(
            'ai.backend.runner', 'extract_dotfiles.py'
        ))

        if matched_libc_style == 'musl':
            terminfo_path = Path(pkg_resources.resource_filename(
                'ai.backend.runner', 'terminfo.alpine3.8'
            ))
            _mount(MountTypes.BIND, terminfo_path.resolve(), '/home/work/.terminfo')

        _mount(MountTypes.BIND, self.agent_sockpath, '/opt/kernel/agent.sock', perm='rw')
        _mount(MountTypes.BIND, dotfile_extractor_path.resolve(), '/opt/kernel/extract_dotfiles.py')
        _mount(MountTypes.BIND, entrypoint_sh_path.resolve(), '/opt/kernel/entrypoint.sh')
        _mount(MountTypes.BIND, suexec_path.resolve(), '/opt/kernel/su-exec')
        if jail_path is not None:
            _mount(MountTypes.BIND, jail_path.resolve(), '/opt/kernel/jail')
        _mount(MountTypes.BIND, hook_path.resolve(), '/opt/kernel/libbaihook.so')
        _mount(MountTypes.BIND, dropbear_path.resolve(), '/opt/kernel/dropbear')
        _mount(MountTypes.BIND, dropbearconv_path.resolve(), '/opt/kernel/dropbearconvert')
        _mount(MountTypes.BIND, dropbearkey_path.resolve(), '/opt/kernel/dropbearkey')
        _mount(MountTypes.BIND, tmux_path.resolve(), '/opt/kernel/tmux')
        _mount(MountTypes.BIND, sftp_server_path.resolve(), '/usr/libexec/sftp-server')
        _mount(MountTypes.BIND, scp_path.resolve(), '/usr/bin/scp')

        _mount(MountTypes.VOLUME, krunner_volume, '/opt/backend.ai')
        pylib_path = f'/opt/backend.ai/lib/python{krunner_pyver}/site-packages/'
        _mount(MountTypes.BIND, kernel_pkg_path.resolve(),
                                pylib_path + 'ai/backend/kernel')
        _mount(MountTypes.BIND, helpers_pkg_path.resolve(),
                                pylib_path + 'ai/backend/helpers')

        environ['LD_PRELOAD'] = '/opt/kernel/libbaihook.so'
        if self.local_config['debug']['coredump']['enabled']:
            _mount(MountTypes.BIND, self.local_config['debug']['coredump']['path'],
                                    self.local_config['debug']['coredump']['core_path'],
                                    perm='rw')

        domain_socket_proxies = []
        for host_sock_path in internal_data.get('domain_socket_proxies', []):
            await loop.run_in_executor(
                None,
                partial((ipc_base_path / 'proxy').mkdir, parents=True, exist_ok=True))
            host_proxy_path = ipc_base_path / 'proxy' / f'{secrets.token_hex(12)}.sock'
            proxy_server = await asyncio.start_unix_server(
                aiotools.apartial(proxy_connection, host_sock_path),
                str(host_proxy_path))
            await loop.run_in_executor(None, host_proxy_path.chmod, 0o666)
            domain_socket_proxies.append(DomainSocketProxy(
                Path(host_sock_path),
                host_proxy_path,
                proxy_server,
            ))
            _mount(MountTypes.BIND, host_proxy_path, host_sock_path, perm='rw')

        # Inject ComputeDevice-specific env-varibles and hooks
        computer_docker_args: Dict[str, Any] = {}
        for dev_type, device_alloc in resource_spec.allocations.items():
            computer_set = self.computers[dev_type]
            update_nested_dict(computer_docker_args,
                               await computer_set.instance.generate_docker_args(
                                   self.docker, device_alloc))
            alloc_sum = Decimal(0)
            for dev_id, per_dev_alloc in device_alloc.items():
                alloc_sum += sum(per_dev_alloc.values())
            if alloc_sum > 0:
                hook_paths = await computer_set.instance.get_hooks(distro, arch)
                if hook_paths:
                    log.debug('accelerator {} provides hooks: {}',
                              type(computer_set.instance).__name__,
                              ', '.join(map(str, hook_paths)))
                for hook_path in hook_paths:
                    container_hook_path = '/opt/kernel/lib{}{}.so'.format(
                        computer_set.instance.key, secrets.token_hex(6),
                    )
                    _mount(MountTypes.BIND, hook_path, container_hook_path)
                    environ['LD_PRELOAD'] += ':' + container_hook_path

        # PHASE 3: Store the resource spec.

        if restarting:
            pass
        else:
            # Create the scratch, config, and work directories.
            if (
                sys.platform.startswith('linux')
                and self.local_config['container']['scratch-type'] == 'memory'
            ):
                await loop.run_in_executor(None, partial(os.makedirs, tmp_dir, exist_ok=True))
                await create_scratch_filesystem(scratch_dir, 64)
                await create_scratch_filesystem(tmp_dir, 64)
            else:
                await loop.run_in_executor(None, partial(os.makedirs, scratch_dir, exist_ok=True))
            await loop.run_in_executor(None, partial(os.makedirs, config_dir, exist_ok=True))
            await loop.run_in_executor(None, partial(os.makedirs, work_dir, exist_ok=True))

            # Since these files are bind-mounted inside a bind-mounted directory,
            # we need to touch them first to avoid their "ghost" files are created
            # as root in the host-side filesystem, which prevents deletion of scratch
            # directories when the agent is running as non-root.
            def _clone_dotfiles():
                jupyter_custom_css_path = Path(pkg_resources.resource_filename(
                    'ai.backend.runner', 'jupyter-custom.css'))
                logo_path = Path(pkg_resources.resource_filename(
                    'ai.backend.runner', 'logo.svg'))
                font_path = Path(pkg_resources.resource_filename(
                    'ai.backend.runner', 'roboto.ttf'))
                font_italic_path = Path(pkg_resources.resource_filename(
                    'ai.backend.runner', 'roboto-italic.ttf'))
                bashrc_path = Path(pkg_resources.resource_filename(
                    'ai.backend.runner', '.bashrc'))
                bash_profile_path = Path(pkg_resources.resource_filename(
                    'ai.backend.runner', '.bash_profile'))
                vimrc_path = Path(pkg_resources.resource_filename(
                    'ai.backend.runner', '.vimrc'))
                tmux_conf_path = Path(pkg_resources.resource_filename(
                    'ai.backend.runner', '.tmux.conf'))
                jupyter_custom_dir = (work_dir / '.jupyter' / 'custom')
                jupyter_custom_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy(jupyter_custom_css_path.resolve(), jupyter_custom_dir / 'custom.css')
                shutil.copy(logo_path.resolve(), jupyter_custom_dir / 'logo.svg')
                shutil.copy(font_path.resolve(), jupyter_custom_dir / 'roboto.ttf')
                shutil.copy(font_italic_path.resolve(), jupyter_custom_dir / 'roboto-italic.ttf')
                shutil.copy(bashrc_path.resolve(), work_dir / '.bashrc')
                shutil.copy(bash_profile_path.resolve(), work_dir / '.bash_profile')
                shutil.copy(vimrc_path.resolve(), work_dir / '.vimrc')
                shutil.copy(tmux_conf_path.resolve(), work_dir / '.tmux.conf')
                if KernelFeatures.UID_MATCH in kernel_features:
                    uid = self.local_config['container']['kernel-uid']
                    gid = self.local_config['container']['kernel-gid']
                    if os.geteuid() == 0:  # only possible when I am root.
                        os.chown(work_dir, uid, gid)
                        os.chown(work_dir / '.jupyter', uid, gid)
                        os.chown(work_dir / '.jupyter' / 'custom', uid, gid)
                        os.chown(bashrc_path, uid, gid)
                        os.chown(bash_profile_path, uid, gid)
                        os.chown(vimrc_path, uid, gid)
                        os.chown(tmux_conf_path, uid, gid)

            await loop.run_in_executor(None, _clone_dotfiles)

            # Create bootstrap.sh into workdir if needed
            if bootstrap := kernel_config.get('bootstrap_script'):

                def _write_user_bootstrap_script():
                    (work_dir / 'bootstrap.sh').write_text(bootstrap)
                    if KernelFeatures.UID_MATCH in kernel_features:
                        uid = self.local_config['container']['kernel-uid']
                        gid = self.local_config['container']['kernel-gid']
                        if os.geteuid() == 0:
                            os.chown(work_dir / 'bootstrap.sh', uid, gid)

                await loop.run_in_executor(None, _write_user_bootstrap_script)

            # Store custom environment variables for kernel runner.
            with open(config_dir / 'kconfig.dat', 'wb') as fb:
                pickle.dump(kernel_config, fb)
            async with AsyncFileWriter(
                    loop=loop,
                    target_filename=config_dir / 'environ.txt',
                    access_mode='w') as writer:
                for k, v in environ.items():
                    await writer.write(f'{k}={v}\n')
                accel_envs = computer_docker_args.get('Env', [])
                for env in accel_envs:
                    await writer.write(f'{env}\n')
            with open(config_dir / 'resource.txt', 'w') as f:
                await loop.run_in_executor(None, resource_spec.write_to_file, f)
            async with AsyncFileWriter(
                    loop=loop,
                    target_filename=config_dir / 'resource.txt',
                    access_mode='a') as writer:
                for dev_type, device_alloc in resource_spec.allocations.items():
                    computer_ctx = self.computers[dev_type]
                    kvpairs = \
                        await computer_ctx.instance.generate_resource_data(device_alloc)
                    for k, v in kvpairs.items():
                        await writer.write(f'{k}={v}\n')
            with open(config_dir / 'kernel_id.txt', 'w') as f:
                await loop.run_in_executor(None, f.write, kernel_id.hex)
            docker_creds = internal_data.get('docker_credentials')
            if docker_creds:
                await loop.run_in_executor(
                    None,
                    (config_dir / 'docker-creds.json').write_text,
                    json.dumps(docker_creds))

        # Create SSH keypair only if ssh_keypair internal_data exists and
        # /home/work/.ssh folder is not mounted.
        if internal_data.get('ssh_keypair'):
            for mount in mounts:
                container_path = str(mount).split(':')[1]
                if container_path == '/home/work/.ssh':
                    break
            else:
                pubkey = internal_data['ssh_keypair']['public_key'].encode('ascii')
                privkey = internal_data['ssh_keypair']['private_key'].encode('ascii')
                ssh_dir = work_dir / '.ssh'

                def _populate_ssh_config():
                    ssh_dir.mkdir(parents=True, exist_ok=True)
                    ssh_dir.chmod(0o700)
                    (ssh_dir / 'authorized_keys').write_bytes(pubkey)
                    (ssh_dir / 'authorized_keys').chmod(0o600)
                    (work_dir / 'id_container').write_bytes(privkey)
                    (work_dir / 'id_container').chmod(0o600)
                    if KernelFeatures.UID_MATCH in kernel_features:
                        uid = self.local_config['container']['kernel-uid']
                        gid = self.local_config['container']['kernel-gid']
                        if os.geteuid() == 0:  # only possible when I am root.
                            os.chown(ssh_dir, uid, gid)
                            os.chown(ssh_dir / 'authorized_keys', uid, gid)
                            os.chown(work_dir / 'id_container', uid, gid)

                await loop.run_in_executor(None, _populate_ssh_config)

        for dotfile in internal_data.get('dotfiles', []):
            file_path: Path = work_dir / dotfile['path']
            file_path.parent.mkdir(parents=True, exist_ok=True)
            await loop.run_in_executor(
                None,
                file_path.write_text,
                dotfile['data'])

            tmp = Path(file_path)
            while tmp != work_dir:
                tmp.chmod(int(dotfile['perm'], 8))
                # only possible when I am root.
                if KernelFeatures.UID_MATCH in kernel_features and os.geteuid() == 0:
                    uid = self.local_config['container']['kernel-uid']
                    gid = self.local_config['container']['kernel-gid']
                    os.chown(tmp, uid, gid)
                tmp = tmp.parent

        # PHASE 4: Run!
        log.info('kernel starting with resource spec: \n{0}',
                 pretty(attr.asdict(resource_spec)))

        # TODO: Refactor out as separate "Docker execution driver plugin" (#68)
        #   - Refactor volumes/mounts lists to a plugin "mount" API
        #   - Refactor "/home/work" and "/opt/backend.ai" prefixes to be specified
        #     by the plugin implementation.

        exposed_ports = [2000, 2001]
        service_ports = []
        port_map = {}
        preopen_ports = kernel_config.get('preopen_ports', [])

        for sport in parse_service_ports(image_labels.get('ai.backend.service-ports', '')):
            port_map[sport['name']] = sport
        port_map['sshd'] = {
            'name': 'sshd',
            'protocol': ServicePortProtocols('tcp'),
            'container_ports': (2200,),
            'host_ports': (None,),
        }

        port_map['ttyd'] = {
            'name': 'ttyd',
            'protocol': ServicePortProtocols('http'),
            'container_ports': (7681,),
            'host_ports': (None,),
        }
        for port_no in preopen_ports:
            sport = {
                'name': str(port_no),
                'protocol': ServicePortProtocols('preopen'),
                'container_ports': (port_no,),
                'host_ports': (None,),
            }
            service_ports.append(sport)
            for cport in sport['container_ports']:
                exposed_ports.append(cport)
        for sport in port_map.values():
            service_ports.append(sport)
            for cport in sport['container_ports']:
                exposed_ports.append(cport)

        log.debug('exposed ports: {!r}', exposed_ports)

        kernel_host = self.local_config['container']['kernel-host']
        if len(exposed_ports) > len(self.port_pool):
            raise RuntimeError('Container ports are not sufficiently available.')
        host_ports = []
        for eport in exposed_ports:
            hport = self.port_pool.pop()
            host_ports.append(hport)

        runtime_type = image_labels.get('ai.backend.runtime-type', 'python')
        runtime_path = image_labels.get('ai.backend.runtime-path', None)
        cmdargs: List[str] = []
        if self.local_config['container']['sandbox-type'] == 'jail':
            cmdargs += [
                "/opt/kernel/jail",
                "-policy", "/etc/backend.ai/jail/policy.yml",
            ]
            if self.local_config['container']['jail-args']:
                cmdargs += map(lambda s: s.strip(), self.local_config['container']['jail-args'])
        cmdargs += [
            "/opt/backend.ai/bin/python",
            "-m", "ai.backend.kernel", runtime_type,
        ]
        if runtime_path is not None:
            cmdargs.append(runtime_path)

        container_log_size = self.local_config['agent']['container-logs']['max-length']
        container_log_file_count = 5
        container_log_file_size = BinarySize(container_log_size // container_log_file_count)
        container_config: MutableMapping[str, Any] = {
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
            'Labels': {
                'ai.backend.kernel-id': str(kernel_id),
                'ai.backend.internal.block-service-ports':
                    '1' if internal_data.get('block_service_ports', False) else '0'
            },
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
                                      'HostIp': str(kernel_host)}]
                    for eport, hport in zip(exposed_ports, host_ports)
                },
                'PublishAllPorts': False,  # we manage port mapping manually!
                'LogConfig': {
                    'Type': 'local',  # for efficient docker-specific storage
                    'Config': {
                        # these fields must be str
                        # (ref: https://docs.docker.com/config/containers/logging/local/)
                        'max-size': f"{container_log_file_size:s}",
                        'max-file': str(container_log_file_count),
                        'compress': 'false',
                    },
                },
            },
        }
        if resource_opts and resource_opts.get('shmem'):
            shmem = int(resource_opts.get('shmem', '0'))
            computer_docker_args['HostConfig']['ShmSize'] = shmem
            computer_docker_args['HostConfig']['MemorySwap'] -= shmem
            computer_docker_args['HostConfig']['Memory'] -= shmem
        if self.local_config['container']['sandbox-type'] == 'jail':
            container_config['HostConfig']['SecurityOpt'] = [
                'seccomp=unconfined',
                'apparmor=unconfined',
            ]
        encoded_preopen_ports = ','.join(f'{port_no}:preopen:{port_no}' for port_no in preopen_ports)
        container_config['Labels']['ai.backend.service-ports'] = \
                image_labels['ai.backend.service-ports'] + ',' + encoded_preopen_ports
        update_nested_dict(container_config, computer_docker_args)
        kernel_name = f"kernel.{image_ref.name.split('/')[-1]}.{kernel_id}"
        log.debug('full container config: {!r}', pretty(container_config))

        # We are all set! Create and start the container.
        try:
            container = await self.docker.containers.create(
                config=container_config, name=kernel_name)
            cid = container._id

            resource_spec.container_id = cid
            # Write resource.txt again to update the contaienr id.
            with open(config_dir / 'resource.txt', 'w') as f:
                await loop.run_in_executor(None, resource_spec.write_to_file, f)
            async with AsyncFileWriter(
                    loop=loop,
                    target_filename=config_dir / 'resource.txt',
                    access_mode='a') as writer:
                for dev_name, device_alloc in resource_spec.allocations.items():
                    computer_ctx = self.computers[dev_name]
                    kvpairs = \
                        await computer_ctx.instance.generate_resource_data(device_alloc)
                    for k, v in kvpairs.items():
                        await writer.write(f'{k}={v}\n')

            await container.start()

            # Get attached devices information (including model_name).
            attached_devices = {}
            for dev_name, device_alloc in resource_spec.allocations.items():
                computer_set = self.computers[dev_name]
                devices = await computer_set.instance.get_attached_devices(device_alloc)
                attached_devices[dev_name] = devices
        except asyncio.CancelledError:
            raise
        except Exception:
            # Oops, we have to restore the allocated resources!
            if (sys.platform.startswith('linux') and
                self.local_config['container']['scratch-type'] == 'memory'):
                await destroy_scratch_filesystem(scratch_dir)
                await destroy_scratch_filesystem(tmp_dir)
                await loop.run_in_executor(None, shutil.rmtree, tmp_dir)
            await loop.run_in_executor(None, shutil.rmtree, scratch_dir)
            self.port_pool.update(host_ports)
            async with self.resource_lock:
                for dev_name, device_alloc in resource_spec.allocations.items():
                    self.computers[dev_name].alloc_map.free(device_alloc)
            raise

        ctnr_host_port_map: MutableMapping[int, int] = {}
        stdin_port = 0
        stdout_port = 0
        for idx, port in enumerate(exposed_ports):
            host_port = int((await container.port(port))[0]['HostPort'])
            assert host_port == host_ports[idx]
            if port == 2000:     # intrinsic
                repl_in_port = host_port
            elif port == 2001:   # intrinsic
                repl_out_port = host_port
            elif port == 2002:   # legacy
                stdin_port = host_port
            elif port == 2003:   # legacy
                stdout_port = host_port
            else:
                ctnr_host_port_map[port] = host_port
        for sport in service_ports:
            sport['host_ports'] = tuple(
                ctnr_host_port_map[cport] for cport in sport['container_ports']
            )

        kernel_obj = await DockerKernel.new(
            kernel_id,
            image_ref,
            version,
            agent_config=self.local_config,
            service_ports=service_ports,
            resource_spec=resource_spec,
            data={
                'container_id': container._id,
                'kernel_host': kernel_host,
                'repl_in_port': repl_in_port,
                'repl_out_port': repl_out_port,
                'stdin_port': stdin_port,    # legacy
                'stdout_port': stdout_port,  # legacy
                'host_ports': host_ports,
                'domain_socket_proxies': domain_socket_proxies,
                'block_service_ports': internal_data.get('block_service_ports', False)
            })
        self.kernel_registry[kernel_id] = kernel_obj
        log.debug('kernel repl-in address: {0}:{1}', kernel_host, repl_in_port)
        log.debug('kernel repl-out address: {0}:{1}', kernel_host, repl_out_port)
        live_services = await kernel_obj.get_service_apps()

        # Update the service-ports metadata from the image labels
        # with the extended template metadata from the agent and krunner.
        if live_services['status'] != 'failed':
            for live_service in live_services['data']:
                for service_port in service_ports:
                    if live_service['name'] == service_port['name']:
                        service_port.update(live_service)
                        break
        log.debug('service ports:\n{!r}', pretty(service_ports))

        # Wait until bootstrap script is executed.
        # - Main kernel runner is executed after bootstrap script, and
        #   check_status is accessible only after kernel runner is loaded.
        await kernel_obj.check_status()

        # Finally we are done.
        await self.produce_event('kernel_started', str(kernel_id))

        # Execute the startup command if the session type is batch.
        if SessionTypes(kernel_config['session_type']) == SessionTypes.BATCH:
            log.debug('startup command: {!r}',
                      (kernel_config['startup_command'] or '')[:60])

            # TODO: make this working after agent restarts
            async def execute_batch() -> None:
                opts = {
                    'exec': kernel_config['startup_command'],
                }
                while True:
                    try:
                        result = await self.execute(
                            kernel_id,
                            'batch-job', 'batch', '',
                            opts=opts,
                            flush_timeout=1.0,
                            api_version=3)
                    except KeyError:
                        await self.produce_event(
                            'kernel_terminated',
                            str(kernel_id), 'self-terminated',
                            None)
                        break

                    if result['status'] == 'finished':
                        if result['exitCode'] == 0:
                            await self.produce_event(
                                'kernel_success',
                                str(kernel_id), 0)
                        else:
                            await self.produce_event(
                                'kernel_failure',
                                str(kernel_id), result['exitCode'])
                        break
                    if result['status'] == 'exec-timeout':
                        await self.produce_event(
                            'kernel_failure', str(kernel_id), -2)
                        break
                    opts = {
                        'exec': '',
                    }
                # TODO: store last_stat?
                destroyed = asyncio.Event()
                await self.container_lifecycle_queue.put(
                    ContainerLifecycleEvent(
                        kernel_id,
                        kernel_obj['container_id'],
                        LifecycleEvent.DESTROY,
                        'task-finished',
                        destroyed,
                    )
                )
                await destroyed.wait()

            self.loop.create_task(execute_batch())

        return {
            'id': KernelId(kernel_id),
            'kernel_host': str(kernel_host),
            'repl_in_port': repl_in_port,
            'repl_out_port': repl_out_port,
            'stdin_port': stdin_port,    # legacy
            'stdout_port': stdout_port,  # legacy
            'service_ports': service_ports,
            'container_id': container._id,
            'resource_spec': resource_spec.to_json_serializable_dict(),
            'attached_devices': attached_devices,
        }

    async def destroy_kernel(
        self,
        kernel_id: KernelId,
        container_id: Optional[ContainerId],
    ) -> None:
        if container_id is None:
            return
        try:
            container = self.docker.containers.container(container_id)
            # The default timeout of the docker stop API is 10 seconds
            # to kill if container does not self-terminate.
            await container.stop()
        except DockerError as e:
            if e.status == 409 and 'is not running' in e.message:
                # already dead
                log.warning('destroy_kernel(k:{0}) already dead', kernel_id)
                await self.rescan_resource_usage()
            elif e.status == 404:
                # missing
                log.warning('destroy_kernel(k:{0}) kernel missing, '
                            'forgetting this kernel', kernel_id)
                await self.rescan_resource_usage()
            else:
                log.exception('destroy_kernel(k:{0}) kill error', kernel_id)
                self.error_monitor.capture_exception()

    async def clean_kernel(
        self,
        kernel_id: KernelId,
        container_id: Optional[ContainerId],
        restarting: bool,
    ) -> None:
        loop = current_loop()
        if container_id is not None:
            container = self.docker.containers.container(container_id)

            async def log_iter():
                async for line in container.log(
                    stdout=True, stderr=True, follow=True,
                ):
                    yield line.encode('utf-8')

            try:
                with timeout(60):
                    await self.collect_logs(kernel_id, container_id, log_iter())
            except asyncio.TimeoutError:
                log.warning('timeout for collecting container logs (cid:{})', container_id)
            except Exception as e:
                log.warning('error while collecting container logs (cid:{})',
                            container_id, exc_info=e)

        kernel_obj = self.kernel_registry.get(kernel_id)
        if kernel_obj is not None:
            for domain_socket_proxy in kernel_obj.get('domain_socket_proxies', []):
                domain_socket_proxy.proxy_server.close()
                await domain_socket_proxy.proxy_server.wait_closed()
                try:
                    domain_socket_proxy.host_proxy_path.unlink()
                except IOError:
                    pass

        # When the agent restarts with a different port range, existing
        # containers' host ports may not belong to the new port range.
        if not self.local_config['debug']['skip-container-deletion'] and container_id is not None:
            container = self.docker.containers.container(container_id)
            try:
                with timeout(90):
                    await container.delete(force=True, v=True)
            except DockerError as e:
                if e.status == 409 and 'already in progress' in e.message:
                    return
                elif e.status == 404:
                    return
                else:
                    log.exception(
                        'unexpected docker error while deleting container (k:{}, c:{})',
                        kernel_id, container_id)
            except asyncio.TimeoutError:
                log.warning('container deletion timeout (k:{}, c:{})',
                            kernel_id, container_id)

        if not restarting:
            scratch_root = self.local_config['container']['scratch-root']
            scratch_dir = scratch_root / str(kernel_id)
            tmp_dir = scratch_root / f'{kernel_id}_tmp'
            try:
                if (sys.platform.startswith('linux') and
                    self.local_config['container']['scratch-type'] == 'memory'):
                    await destroy_scratch_filesystem(scratch_dir)
                    await destroy_scratch_filesystem(tmp_dir)
                    await loop.run_in_executor(None, shutil.rmtree, tmp_dir)
                await loop.run_in_executor(None, shutil.rmtree, scratch_dir)
            except FileNotFoundError:
                pass

    async def monitor_docker_events(self):
        subscriber = self.docker.events.subscribe(create_task=True)
        while True:
            try:
                evdata = await subscriber.get()
                if self.local_config['debug']['log-docker-events'] and evdata['Type'] == 'container':
                    log.debug('docker-event: action={}, actor={}',
                              evdata['Action'], evdata['Actor'])
                if evdata['Action'] == 'start':
                    container_name = evdata['Actor']['Attributes']['name']
                    kernel_id = await get_kernel_id_from_container(container_name)
                    if kernel_id is None:
                        continue
                    await self.inject_container_lifecycle_event(
                        kernel_id,
                        LifecycleEvent.START,
                        'new-container-started',
                        container_id=ContainerId(evdata['Actor']['ID']),
                    )
                elif evdata['Action'] == 'die':
                    # When containers die, we immediately clean up them.
                    container_name = evdata['Actor']['Attributes']['name']
                    kernel_id = await get_kernel_id_from_container(container_name)
                    if kernel_id is None:
                        continue
                    reason = None
                    kernel_obj = self.kernel_registry.get(kernel_id)
                    if kernel_obj is not None:
                        reason = kernel_obj.termination_reason
                    try:
                        exit_code = evdata['Actor']['Attributes']['exitCode']
                    except KeyError:
                        exit_code = 255
                    await self.inject_container_lifecycle_event(
                        kernel_id,
                        LifecycleEvent.CLEAN,
                        reason or 'self-terminated',
                        container_id=ContainerId(evdata['Actor']['ID']),
                        exit_code=exit_code,
                    )
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception('unexpected error while processing docker events')
        await self.docker.events.stop()

    def _kernel_resource_spec_read(self, filename):
        with open(filename, 'r') as f:
            resource_spec = KernelResourceSpec.read_from_file(f)
        return resource_spec
