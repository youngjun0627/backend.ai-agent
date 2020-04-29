import asyncio
import base64
from decimal import Decimal
import json
import logging
import os
from pathlib import Path
import pickle
import pkg_resources
import platform
from pprint import pformat
import secrets
import shutil
import signal
import struct
import sys
from typing import (
    Any, Optional, Union, Type,
    Dict, Mapping, MutableMapping,
    Set, FrozenSet,
    Sequence, List, Tuple,
)
from typing_extensions import Literal

import aiohttp
import attr
from async_timeout import timeout
import zmq

from aiodocker.docker import Docker, DockerContainer
from aiodocker.exceptions import DockerError
import aiotools

from ai.backend.common.docker import (
    ImageRef,
    MIN_KERNELSPEC,
    MAX_KERNELSPEC,
)
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    KernelCreationConfig,
    KernelCreationResult,
    KernelId,
    ContainerId,
    DeviceName,
    SlotName,
    MetricKey, MetricValue,
    MountPermission,
    MountTypes,
    ResourceSlot,
    ServicePort,
    SessionTypes,
)
from .kernel import DockerKernel
from .resources import detect_resources
from ..exception import InsufficientResource
from ..fs import create_scratch_filesystem, destroy_scratch_filesystem
from ..kernel import match_krunner_volume, KernelFeatures
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
)
from ..server import (
    get_extra_volumes,
)
from ..stats import (
    spawn_stat_synchronizer, StatSyncState
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

log = BraceStyleAdapter(logging.getLogger(__name__))


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


class DockerAgent(AbstractAgent):

    docker: Docker
    monitor_fetch_task: asyncio.Task
    monitor_handle_task: asyncio.Task
    agent_sockpath: Path
    agent_sock_task: asyncio.Task
    scan_images_timer: asyncio.Task

    def __init__(self, config) -> None:
        super().__init__(config)

    async def __ainit__(self) -> None:
        self.docker = Docker()
        docker_version = await self.docker.version()
        log.info('running with Docker {0} with API {1}',
                 docker_version['Version'], docker_version['ApiVersion'])
        await super().__ainit__()
        self.agent_sockpath = ipc_base_path / f'agent.{self.agent_id}.sock'
        self.agent_sock_task = self.loop.create_task(self.handle_agent_socket())
        self.monitor_fetch_task  = self.loop.create_task(self.fetch_docker_events())
        self.monitor_handle_task = self.loop.create_task(self.handle_docker_events())

    async def shutdown(self, stop_signal: signal.Signals):
        try:
            await super().shutdown(stop_signal)
        finally:
            # Stop docker event monitoring.
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

        # Stop handlign agent sock.
        # (But we don't remove the socket file)
        if self.agent_sock_task is not None:
            self.agent_sock_task.cancel()
            await self.agent_sock_task

    @staticmethod
    async def detect_resources(resource_configs: Mapping[str, Any],
                               plugin_configs: Mapping[str, Any]) \
                               -> Tuple[
                                   Mapping[DeviceName, Type[AbstractComputePlugin]],
                                   Mapping[SlotName, Decimal]
                               ]:
        return await detect_resources(resource_configs, plugin_configs)

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
        my_uid = os.geteuid()
        my_gid = os.getegid()
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

    async def create_kernel(self, kernel_id: KernelId, kernel_config: KernelCreationConfig, *,
                            restarting: bool = False) -> KernelCreationResult:

        await self.produce_event('kernel_preparing', str(kernel_id))

        # Read image-specific labels and settings
        image_ref = ImageRef(
            kernel_config['image']['canonical'],
            [kernel_config['image']['registry']['name']])
        environ: MutableMapping[str, str] = {**kernel_config['environ']}
        extra_mount_list = await get_extra_volumes(self.docker, image_ref.short)
        internal_data: Mapping[str, Any] = kernel_config.get('internal_data') or {}

        try:
            # Find the exact image using a digest reference
            digest_ref = f"{kernel_config['image']['digest']}"
            await self.docker.images.inspect(digest_ref)
            log.info('found the local up-to-date image for {}', image_ref.canonical)
        except DockerError as e:
            if e.status == 404:
                await self.produce_event('kernel_pulling',
                                         str(kernel_id), image_ref.canonical)
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
        await self.produce_event('kernel_creating', str(kernel_id))
        image_labels = kernel_config['image']['labels']

        version = int(image_labels.get('ai.backend.kernelspec', '1'))
        label_envs_corecount = image_labels.get('ai.backend.envs.corecount', '')
        envs_corecount = label_envs_corecount.split(',') if label_envs_corecount else []
        kernel_features = set(image_labels.get('ai.backend.features', '').split())

        scratch_dir = (self.config['container']['scratch-root'] / str(kernel_id)).resolve()
        tmp_dir = (self.config['container']['scratch-root'] / f'{kernel_id}_tmp').resolve()
        config_dir = scratch_dir / 'config'
        work_dir = scratch_dir / 'work'

        # PHASE 1: Read existing resource spec or devise a new resource spec.

        if restarting:
            with open(config_dir / 'resource.txt', 'r') as f:
                resource_spec = KernelResourceSpec.read_from_file(f)
            resource_opts = None
        else:
            # resource_slots is already sanitized by the manager.
            slots = ResourceSlot.from_json(kernel_config['resource_slots'])
            vfolders = kernel_config['mounts']
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
            uid = self.config['container']['kernel-uid']
            gid = self.config['container']['kernel-gid']
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
                if internal_data.get('prevent_vfolder_mounts', False):
                    # Only allow mount of ".logs" directory to prevent expose
                    # internal-only information, such as Docker credentials to user's ".docker" vfolder
                    # in image importer kernels.
                    if folder_name != '.logs':
                        continue
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
        cpu_core_count = len(resource_spec.allocations[DeviceName('cpu')][SlotName('cpu')])
        environ.update({k: str(cpu_core_count) for k in envs_corecount})

        def _mount(type: MountTypes,
                   src: Union[str, Path], target: Union[str, Path],
                   perm: Literal['ro', 'rw'] = 'ro',
                   opts: Mapping[str, Any] = None) -> None:
            nonlocal mounts
            mounts.append(Mount(type, src, target, MountPermission(perm), opts=opts))

        async def _copy(src: Union[str, Path], target: Union[str, Path]):
            def sync():
                shutil.copy(src, target)
            await self.loop.run_in_executor(None, sync)

        # Inject Backend.AI kernel runner dependencies.
        distro = image_labels.get('ai.backend.base-distro', 'ubuntu16.04')
        matched_distro, krunner_volume = match_krunner_volume(
            self.config['container']['krunner-volumes'], distro)
        matched_libc_style = 'glibc'
        if matched_distro.startswith('alpine'):
            matched_libc_style = 'musl'
        log.debug('selected krunner: {}', matched_distro)
        log.debug('selected libc style: {}', matched_libc_style)
        log.debug('krunner volume: {}', krunner_volume)
        arch = platform.machine()
        entrypoint_sh_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../runner/entrypoint.sh'))
        if matched_distro == 'centos6.10':
            # special case for image importer kernel (manylinux2010 is based on CentOS 6)
            suexec_path = Path(pkg_resources.resource_filename(
                'ai.backend.agent', f'../runner/su-exec.centos7.6.{arch}.bin'))
            hook_path = Path(pkg_resources.resource_filename(
                'ai.backend.agent', f'../runner/libbaihook.centos7.6.{arch}.so'))
            sftp_server_path = Path(pkg_resources.resource_filename(
                'ai.backend.agent',
                f'../runner/sftp-server.centos7.6.{arch}.bin'))
            scp_path = Path(pkg_resources.resource_filename(
                'ai.backend.agent',
                f'../runner/scp.centos7.6.{arch}.bin'))
        else:
            suexec_path = Path(pkg_resources.resource_filename(
                'ai.backend.agent', f'../runner/su-exec.{matched_distro}.{arch}.bin'))
            hook_path = Path(pkg_resources.resource_filename(
                'ai.backend.agent', f'../runner/libbaihook.{matched_distro}.{arch}.so'))
            sftp_server_path = Path(pkg_resources.resource_filename(
                'ai.backend.agent',
                f'../runner/sftp-server.{matched_distro}.{arch}.bin'))
            scp_path = Path(pkg_resources.resource_filename(
                'ai.backend.agent',
                f'../runner/scp.{matched_distro}.{arch}.bin'))
        if self.config['container']['sandbox-type'] == 'jail':
            jail_path = Path(pkg_resources.resource_filename(
                'ai.backend.agent', f'../runner/jail.{matched_distro}.bin'))
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

        dropbear_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent',
            f'../runner/dropbear.{matched_libc_style}.{arch}.bin'))
        dropbearconv_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent',
            f'../runner/dropbearconvert.{matched_libc_style}.{arch}.bin'))
        dropbearkey_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent',
            f'../runner/dropbearkey.{matched_libc_style}.{arch}.bin'))
        tmux_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', f'../runner/tmux.{matched_libc_style}.{arch}.bin'))

        bashrc_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../runner/.bashrc'))
        bash_profile_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../runner/.bash_profile'))
        vimrc_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../runner/.vimrc'))
        tmux_conf_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../runner/.tmux.conf'))

        if matched_libc_style == 'musl':
            terminfo_path = Path(pkg_resources.resource_filename(
                'ai.backend.agent', '../runner/terminfo.alpine3.8'
            ))
            _mount(MountTypes.BIND, terminfo_path.resolve(), '/home/work/.terminfo')
        _mount(MountTypes.BIND, self.agent_sockpath, '/opt/kernel/agent.sock', perm='rw')
        _mount(MountTypes.BIND, entrypoint_sh_path.resolve(), '/opt/kernel/entrypoint.sh')
        _mount(MountTypes.BIND, suexec_path.resolve(), '/opt/kernel/su-exec')
        if self.config['container']['sandbox-type'] == 'jail':
            _mount(MountTypes.BIND, jail_path.resolve(), '/opt/kernel/jail')
        _mount(MountTypes.BIND, hook_path.resolve(), '/opt/kernel/libbaihook.so')

        _mount(MountTypes.BIND, dropbear_path.resolve(), '/opt/kernel/dropbear')
        _mount(MountTypes.BIND, dropbearconv_path.resolve(), '/opt/kernel/dropbearconvert')
        _mount(MountTypes.BIND, dropbearkey_path.resolve(), '/opt/kernel/dropbearkey')
        _mount(MountTypes.BIND, tmux_path.resolve(), '/opt/kernel/tmux')
        _mount(MountTypes.BIND, sftp_server_path.resolve(), '/usr/libexec/sftp-server')
        _mount(MountTypes.BIND, scp_path.resolve(), '/usr/bin/scp')

        _mount(MountTypes.VOLUME, krunner_volume, '/opt/backend.ai')
        _mount(MountTypes.BIND, kernel_pkg_path.resolve(),
                                '/opt/backend.ai/lib/python3.6/site-packages/ai/backend/kernel')
        _mount(MountTypes.BIND, helpers_pkg_path.resolve(),
                                '/opt/backend.ai/lib/python3.6/site-packages/ai/backend/helpers')

        # Since these files are bind-mounted inside a bind-mounted directory,
        # we need to touch them first to avoid their "ghost" files are created
        # as root in the host-side filesystem, which prevents deletion of scratch
        # directories when the agent is running as non-root.
        jupyter_custom_dir = (work_dir / '.jupyter' / 'custom')
        jupyter_custom_dir.mkdir(parents=True, exist_ok=True)
        await _copy(jupyter_custom_css_path.resolve(), jupyter_custom_dir / 'custom.css')
        await _copy(logo_path.resolve(), jupyter_custom_dir / 'logo.svg')
        await _copy(font_path.resolve(), jupyter_custom_dir / 'roboto.ttf')
        await _copy(font_italic_path.resolve(), jupyter_custom_dir / 'roboto-italic.ttf')
        await _copy(bashrc_path.resolve(), work_dir / '.bashrc')
        await _copy(bash_profile_path.resolve(), work_dir / '.bash_profile')
        await _copy(vimrc_path.resolve(), work_dir / '.vimrc')
        await _copy(tmux_conf_path.resolve(), work_dir / '.tmux.conf')
        environ['LD_PRELOAD'] = '/opt/kernel/libbaihook.so'
        if self.config['debug']['coredump']['enabled']:
            _mount(MountTypes.BIND, self.config['debug']['coredump']['path'],
                                    self.config['debug']['coredump']['core_path'],
                                    perm='rw')

        domain_socket_proxies = []
        for host_sock_path in internal_data.get('domain_socket_proxies', []):
            (ipc_base_path / 'proxy').mkdir(parents=True, exist_ok=True)
            host_proxy_path = ipc_base_path / 'proxy' / f'{secrets.token_hex(12)}.sock'
            proxy_server = await asyncio.start_unix_server(
                aiotools.apartial(proxy_connection, host_sock_path),
                str(host_proxy_path))
            host_proxy_path.chmod(0o666)
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
                               await computer_set.klass.generate_docker_args(
                                   self.docker, device_alloc))
            alloc_sum = Decimal(0)
            for dev_id, per_dev_alloc in device_alloc.items():
                alloc_sum += sum(per_dev_alloc.values())
            if alloc_sum > 0:
                hook_paths = await computer_set.klass.get_hooks(matched_distro, arch)
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
                if os.geteuid() == 0:  # only possible when I am root.
                    os.chown(work_dir, uid, gid)
                    os.chown(work_dir / '.jupyter', uid, gid)
                    os.chown(work_dir / '.jupyter' / 'custom', uid, gid)
                    os.chown(work_dir / '.bashrc', uid, gid)
                    os.chown(work_dir / '.bash_profile', uid, gid)
                    os.chown(work_dir / '.vimrc', uid, gid)
                    os.chown(work_dir / '.tmux.conf', uid, gid)
            os.makedirs(config_dir, exist_ok=True)
            # Store custom environment variables for kernel runner.
            with open(config_dir / 'kconfig.dat', 'wb') as fb:
                pickle.dump(kernel_config, fb)
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
            with open(config_dir / 'kernel_id.txt', 'w') as f:
                f.write(kernel_id.hex)
            docker_creds = internal_data.get('docker_credentials')
            if docker_creds:
                (config_dir / 'docker-creds.json').write_text(json.dumps(docker_creds))
            # Create bootstrap.sh into workdir if needed
            bootstrap = internal_data.get('bootstrap_script')
            sys.stdout.flush()
            if bootstrap:
                (work_dir / 'bootstrap.sh').write_text(bootstrap)
                if KernelFeatures.UID_MATCH in kernel_features:
                    uid = self.config['container']['kernel-uid']
                    gid = self.config['container']['kernel-gid']
                    if os.geteuid() == 0:
                        os.chown(work_dir / 'bootstrap.sh', uid, gid)

        # Create SSH keypair only if ssh_keypair internal_data exists and
        # /home/work/.ssh folder is not mounted.
        if internal_data.get('ssh_keypair'):
            for m in mounts:
                container_path = str(m).split(':')[1]
                if container_path == '/home/work/.ssh':
                    break
            else:
                pubkey = internal_data['ssh_keypair']['public_key'].encode('ascii')
                privkey = internal_data['ssh_keypair']['private_key'].encode('ascii')
                os.makedirs(work_dir / '.ssh', exist_ok=True)
                (work_dir / '.ssh' / 'authorized_keys').write_bytes(pubkey)
                (work_dir / '.ssh' / 'authorized_keys').chmod(0o600)
                (work_dir / 'id_container').write_bytes(privkey)
                (work_dir / 'id_container').chmod(0o600)
                if KernelFeatures.UID_MATCH in kernel_features:
                    uid = self.config['container']['kernel-uid']
                    gid = self.config['container']['kernel-gid']
                    if os.geteuid() == 0:
                        os.chown(work_dir / '.ssh', uid, gid)
                        os.chown(work_dir / '.ssh' / 'authorized_keys', uid, gid)
                        os.chown(work_dir / 'id_container', uid, gid)

        for dotfile in internal_data.get('dotfiles', []):
            file_path: Path = work_dir / dotfile['path']
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(dotfile['data'])

            tmp = Path(file_path)
            while tmp != work_dir:
                tmp.chmod(int(dotfile['perm'], 8))
                # only possible when I am root.
                if KernelFeatures.UID_MATCH in kernel_features and os.geteuid() == 0:
                    uid = self.config['container']['kernel-uid']
                    gid = self.config['container']['kernel-gid']
                    os.chown(tmp, uid, gid)
                tmp = tmp.parent

        # PHASE 4: Run!
        log.info('kernel {0} starting with resource spec: \n',
                 pformat(attr.asdict(resource_spec)))

        # TODO: Refactor out as separate "Docker execution driver plugin" (#68)
        #   - Refactor volumes/mounts lists to a plugin "mount" API
        #   - Refactor "/home/work" and "/opt/backend.ai" prefixes to be specified
        #     by the plugin implementation.

        exposed_ports = [2000, 2001, 2200, 7681]
        service_ports: List[ServicePort] = [
            {
                'name': 'sshd',
                'protocol': 'tcp',
                'container_ports': (2200,),
                'host_ports': (None,),
            },
            {
                'name': 'ttyd',
                'protocol': 'http',
                'container_ports': (7681,),
                'host_ports': (None,),
            },
        ]

        for sport in parse_service_ports(image_labels.get('ai.backend.service-ports', '')):
            service_ports.append(sport)
            for cport in sport['container_ports']:
                exposed_ports.append(cport)
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
        cmdargs: List[str] = []
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
            },
        }
        if resource_opts and resource_opts.get('shmem'):
            shmem = int(resource_opts.get('shmem', '0'))
            computer_docker_args['HostConfig']['ShmSize'] = shmem
            computer_docker_args['HostConfig']['MemorySwap'] -= shmem
            computer_docker_args['HostConfig']['Memory'] -= shmem
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
                for dev_name, device_alloc in resource_spec.allocations.items():
                    computer_ctx = self.computers[dev_name]
                    kvpairs = \
                        await computer_ctx.klass.generate_resource_data(device_alloc)
                    for k, v in kvpairs.items():
                        f.write(f'{k}={v}\n')

            stat_sync_state = StatSyncState(kernel_id)
            self.stat_sync_states[cid] = stat_sync_state
            async with spawn_stat_synchronizer(self.config['_src'],
                                               self.stat_sync_sockpath,
                                               self.stat_ctx.mode, cid) as proc:
                stat_sync_state.sync_proc = proc
                await container.start()

            # Get attached devices information (including model_name).
            attached_devices = {}
            for dev_name, device_alloc in resource_spec.allocations.items():
                computer_set = self.computers[dev_name]
                devices = await computer_set.klass.get_attached_devices(device_alloc)
                attached_devices[dev_name] = devices
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
            agent_config=self.config,
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
        for service_port in service_ports:
            log.debug('service port: {!r}', service_port)
        await self.produce_event('kernel_started', str(kernel_id))

        # Execute the startup command if the session type is batch.
        if SessionTypes(kernel_config['session_type']) == SessionTypes.BATCH:
            log.debug('startup command: {!r}',
                      (kernel_config['startup_command'] or '')[:60])

            # TODO: make this working after agent restarts
            async def execute_batch():
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
                # TODO: store last_stat?
                destroyed = asyncio.Event()
                self.container_lifecycle_events.put(
                    ContainerLifecycleEvent(
                        kernel_id,
                        kernel_obj['container_id'],
                        LifecycleEvent.TERMINATED,
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
    ) -> Optional[Mapping[MetricKey, MetricValue]]:
        if container_id is None:
            return None
        try:
            container = self.docker.containers.container(container_id)
            # The default timeout of the docker stop API is 10 seconds
            # to kill if container does not self-terminate.
            await container.stop()
            # Collect the last-moment statistics.
            if container_id in self.stat_sync_states:
                s = self.stat_sync_states[container_id]
                try:
                    with timeout(5):
                        await s.terminated.wait()
                        if s.sync_proc is not None:
                            await s.sync_proc.wait()
                            s.sync_proc = None
                except ProcessLookupError:
                    pass
                except asyncio.TimeoutError:
                    log.warning('stat-collector shutdown sync timeout.')
                last_stat: MutableMapping[MetricKey, MetricValue] = {
                    key: metric.to_serializable_dict()
                    for key, metric in s.last_stat.items()
                }
                last_stat['version'] = 2  # type: ignore
                return last_stat
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
        return None

    async def clean_kernel(
        self,
        kernel_id: KernelId,
        container_id: Optional[ContainerId],
        restarting: bool,
    ) -> None:
        if container_id is not None:
            stat_sync_state = self.stat_sync_states.pop(container_id, None)
            if stat_sync_state:
                sync_proc = stat_sync_state.sync_proc
                try:
                    if sync_proc is not None:
                        sync_proc.terminate()
                        try:
                            with timeout(2.0):
                                await sync_proc.wait()
                        except asyncio.TimeoutError:
                            sync_proc.kill()
                            await sync_proc.wait()
                except ProcessLookupError:
                    pass

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
        if not self.config['debug']['skip-container-deletion'] and container_id is not None:
            container = self.docker.containers.container(container_id)
            try:
                with timeout(20):
                    await container.delete(force=True)
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

        if not restarting:
            scratch_root = self.config['container']['scratch-root']
            scratch_dir = scratch_root / str(kernel_id)
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

    async def handle_docker_events(self):
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
            try:
                new_footprint = (
                    evdata['Type'],
                    evdata['Action'],
                    evdata['Actor']['ID'],
                )
                if new_footprint == last_footprint:
                    continue
                last_footprint = new_footprint

                if self.config['debug']['log-docker-events']:
                    log.debug('docker-event: raw: {}', evdata)

                if evdata['Action'] == 'die':
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

        await asyncio.sleep(0.5)
