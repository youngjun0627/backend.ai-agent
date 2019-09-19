import asyncio
import base64
from decimal import Decimal
import logging
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
from typing import (
    Any, Optional, Union,
    Dict, Mapping, MutableMapping,
    Set,
    List, Tuple,
    Type,
)
from typing_extensions import Literal

import aiohttp
import attr
from async_timeout import timeout
import zmq

from aiodocker.docker import Docker
from aiodocker.exceptions import DockerError

from ai.backend.common.docker import ImageRef
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    KernelCreationConfig,
    KernelCreationResult,
    KernelId,
    DeviceName,
    SlotName,
    MetricKey, MetricValue,
    MountPermission,
    MountTypes,
    ResourceSlot,
    ServicePort,
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
    ipc_base_path,
)
from ..resources import (
    AbstractComputePlugin,
)
from ..server import (
    get_extra_volumes,
)
from ..stats import (
    spawn_stat_synchronizer, StatSyncState
)
from ..utils import (
    update_nested_dict,
    get_kernel_id_from_container,
    host_pid_to_container_pid,
    container_pid_to_host_pid,
    parse_service_port,
)

log = BraceStyleAdapter(logging.getLogger(__name__))


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

    async def scan_running_kernels(self) -> None:
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
                self.kernel_registry[kernel_id] = await DockerKernel.new(
                    kernel_id,
                    ImageRef(image),
                    int(labels.get('ai.backend.kernelspec', '1')),
                    agent_config=self.config,
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
                await self.produce_event('kernel_terminated', kernel_id,
                                         'self-terminated', None)

        log.info('starting with resource allocations')
        for computer_name, computer_ctx in self.computers.items():
            log.info('{}: {!r}', computer_name,
                        dict(computer_ctx.alloc_map.allocations))

    async def scan_images(self, interval: float = None) -> None:
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

        await self.produce_event('kernel_preparing', kernel_id)

        # Read image-specific labels and settings
        image_ref = ImageRef(
            kernel_config['image']['canonical'],
            [kernel_config['image']['registry']['name']])
        environ: MutableMapping[str, str] = {**kernel_config['environ']}
        extra_mount_list = await get_extra_volumes(self.docker, image_ref.short)

        try:
            # Find the exact image using a digest reference
            digest_ref = f"{kernel_config['image']['digest']}"
            await self.docker.images.inspect(digest_ref)
            log.info('found the local up-to-date image for {}', image_ref.canonical)
        except DockerError as e:
            if e.status == 404:
                await self.produce_event('kernel_pulling',
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
        await self.produce_event('kernel_creating', kernel_id)
        image_labels = kernel_config['image']['labels']

        version = int(image_labels.get('ai.backend.kernelspec', '1'))
        label_envs_corecount = image_labels.get('ai.backend.envs.corecount', '')
        envs_corecount = label_envs_corecount.split(',') if label_envs_corecount else []
        kernel_features = set(image_labels.get('ai.backend.features', '').split())

        scratch_dir = (self.config['container']['scratch-root'] / kernel_id).resolve()
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
            assert SlotName('cpu') in slots
            assert SlotName('mem') in slots

            # Realize ComputeDevice (including accelerators) allocations.
            dev_names: Set[SlotName] = set()
            for slot_name in slots.keys():
                dev_name = slot_name.split('.', maxsplit=1)[0]
                dev_names.add(dev_name)

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

        # Inject Backend.AI kernel runner dependencies.
        distro = image_labels.get('ai.backend.base-distro', 'ubuntu16.04')
        matched_distro, krunner_volume = match_krunner_volume(
            self.config['container']['krunner-volumes'], distro)
        log.debug('selected krunner: {}', matched_distro)
        log.debug('krunner volume: {}', krunner_volume)
        arch = platform.machine()
        entrypoint_sh_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', '../runner/entrypoint.sh'))
        suexec_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', f'../runner/su-exec.{matched_distro}.bin'))
        if self.config['container']['sandbox-type'] == 'jail':
            jail_path = Path(pkg_resources.resource_filename(
                'ai.backend.agent', f'../runner/jail.{matched_distro}.bin'))
        hook_path = Path(pkg_resources.resource_filename(
            'ai.backend.agent', f'../runner/libbaihook.{matched_distro}.{arch}.so'))
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
        service_ports: MutableMapping[int, ServicePort] = {}
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
        if resource_opts and resource_opts.get('shmem'):
            shmem = resource_opts.get('shmem')
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

            self.stat_sync_states[cid] = StatSyncState(kernel_id)
            async with spawn_stat_synchronizer(self.config['_src'],
                                               self.stat_sync_sockpath,
                                               self.stat_ctx.mode, cid):
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

        self.kernel_registry[kernel_id] = await DockerKernel.new(
            kernel_id,
            image_ref,
            version,
            agent_config=self.config,
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
        await self.produce_event('kernel_started', kernel_id)
        return {
            'id': kernel_id,
            'kernel_host': kernel_host,
            'repl_in_port': repl_in_port,
            'repl_out_port': repl_out_port,
            'stdin_port': stdin_port,    # legacy
            'stdout_port': stdout_port,  # legacy
            'service_ports': list(service_ports.values()),
            'container_id': container._id,
            'resource_spec': resource_spec.to_json_serializable_dict(),
            'attached_devices': attached_devices,
        }

    async def destroy_kernel(self, kernel_id: KernelId, reason: str) \
            -> Optional[Mapping[MetricKey, MetricValue]]:
        try:
            kernel_obj = self.kernel_registry[kernel_id]
            cid = kernel_obj['container_id']
            kernel_obj.termination_reason = reason
        except KeyError:
            log.warning('destroy_kernel(k:{0}) kernel missing (already dead?)',
                        kernel_id)

            async def force_cleanup():
                await self.clean_kernel(kernel_id)
                await self.produce_event('kernel_terminated',
                                         kernel_id, 'self-terminated',
                                         None)

            self.orphan_tasks.discard(asyncio.Task.current_task())
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
                try:
                    with timeout(5):
                        await s.terminated.wait()
                except asyncio.TimeoutError:
                    log.warning('stat-collector shutdown sync timeout.')
                self.stat_sync_states.pop(cid, None)
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
                kernel_obj.release_slots(self.computers)
                await kernel_obj.close()
                self.kernel_registry.pop(kernel_id, None)
            elif e.status == 404:
                # missing
                log.warning('destroy_kernel(k:{0}) kernel missing, '
                            'forgetting this kernel', kernel_id)
                kernel_obj.release_slots(self.computers)
                await kernel_obj.close()
                self.kernel_registry.pop(kernel_id, None)
            else:
                log.exception('destroy_kernel(k:{0}) kill error', kernel_id)
                self.error_monitor.capture_exception()
        except asyncio.CancelledError:
            log.exception('destroy_kernel(k:{0}) operation cancelled', kernel_id)
            raise
        except Exception:
            log.exception('destroy_kernel(k:{0}) unexpected error', kernel_id)
            self.error_monitor.capture_exception()
        finally:
            self.orphan_tasks.discard(asyncio.Task.current_task())
        # The container will be deleted in the docker monitoring coroutine.
        return None

    async def clean_kernel(self, kernel_id: KernelId, exit_code: int = 255):
        try:
            kernel_obj = self.kernel_registry[kernel_id]
            found = True
        except KeyError:
            found = False
        try:
            if found:
                await self.produce_event(
                    'kernel_terminated', kernel_id,
                    kernel_obj.termination_reason or 'self-terminated',
                    exit_code)
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
                kernel_obj.release_slots(self.computers)
                await kernel_obj.close()
                self.kernel_registry.pop(kernel_id, None)
        except Exception:
            log.exception('unexpected error while cleaning up kernel (k:{})', kernel_id)
        finally:
            self.orphan_tasks.discard(asyncio.Task.current_task())
            if kernel_id in self.blocking_cleans:
                self.blocking_cleans[kernel_id].set()

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
                try:
                    exit_code = evdata['Actor']['Attributes']['exitCode']
                except KeyError:
                    exit_code = 255
                self.orphan_tasks.add(
                    self.loop.create_task(self.clean_kernel(kernel_id, exit_code))
                )

        await asyncio.sleep(0.5)
