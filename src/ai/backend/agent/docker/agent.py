from __future__ import annotations

import asyncio
import base64
from decimal import Decimal
from functools import partial
from io import StringIO
import json
import logging
import os
from pathlib import Path
import pkg_resources
import secrets
import shutil
import signal
import struct
from subprocess import CalledProcessError
import sys
from typing import (
    Any,
    FrozenSet,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    TYPE_CHECKING,
)

from aiodocker.docker import Docker, DockerContainer
from aiodocker.exceptions import DockerError, DockerContainerError
import aiotools
from async_timeout import timeout
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
    ClusterInfo,
    ImageRegistry,
    KernelCreationConfig,
    KernelId,
    ContainerId,
    DeviceName,
    SlotName,
    MountPermission,
    MountTypes,
    ResourceSlot,
    Sentinel,
    current_resource_slots,
)
from ai.backend.common.utils import AsyncFileWriter, current_loop
from .kernel import DockerKernel
from .resources import detect_resources
from .utils import PersistentServiceContainer
from ..exception import UnsupportedResource, InitializationError
from ..fs import create_scratch_filesystem, destroy_scratch_filesystem
from ..kernel import KernelFeatures
from ..resources import (
    Mount,
    KernelResourceSpec,
)
from ..agent import (
    AbstractAgent,
    AbstractKernelCreationContext,
    ACTIVE_STATUS_SET,
    ComputerContext,
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
    Container,
    Port,
    ContainerStatus,
    LifecycleEvent,
)
from ..utils import (
    update_nested_dict,
    get_kernel_id_from_container,
    host_pid_to_container_pid,
    container_pid_to_host_pid,
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


class DockerKernelCreationContext(AbstractKernelCreationContext):

    scratch_dir: Path
    tmp_dir: Path
    config_dir: Path
    work_dir: Path
    docker: Docker
    container_configs: List[Mapping[str, Any]] = []
    domain_socket_proxies: List[DomainSocketProxy] = []
    computer_docker_args: Dict[str, Any] = {}
    port_pool: Set[int]
    agent_sockpath: Path
    resource_lock: asyncio.Lock

    def __init__(
        self,
        kernel_id: KernelId,
        kernel_config: KernelCreationConfig,
        local_config: Mapping[str, Any],
        computers: MutableMapping[str, ComputerContext],
        port_pool: Set[int],
        agent_sockpath: Path,
        resource_lock: asyncio.Lock,
        docker: Docker,
        restarting: bool = False
    ):
        super().__init__(kernel_id, kernel_config, local_config, computers, restarting=restarting)
        scratch_dir = (self.local_config['container']['scratch-root'] / str(kernel_id)).resolve()
        tmp_dir = (self.local_config['container']['scratch-root'] / f'{kernel_id}_tmp').resolve()

        self.scratch_dir = scratch_dir
        self.tmp_dir = tmp_dir
        self.config_dir = scratch_dir / 'config'
        self.work_dir = scratch_dir / 'work'

        self.docker = docker
        self.port_pool = port_pool
        self.agent_sockpath = agent_sockpath
        self.resource_lock = resource_lock

    def _kernel_resource_spec_read(self, filename):
        with open(filename, 'r') as f:
            resource_spec = KernelResourceSpec.read_from_file(f)
        return resource_spec

    async def get_extra_envs(self) -> Mapping[str, str]:
        return {}

    async def prepare_resource_spec(self) -> Tuple[KernelResourceSpec, Optional[Mapping[str, Any]]]:
        loop = current_loop()
        if self.restarting:
            resource_spec = await loop.run_in_executor(
                None,
                self._kernel_resource_spec_read,
                self.config_dir / 'resource.txt')
            resource_opts = None
        else:
            slots = ResourceSlot.from_json(self.kernel_config['resource_slots'])
            # Ensure that we have intrinsic slots.
            assert SlotName('cpu') in slots
            assert SlotName('mem') in slots
            # accept unknown slot type with zero values
            # but reject if they have non-zero values.
            for st, sv in slots.items():
                if st not in known_slot_types and sv != Decimal(0):
                    raise UnsupportedResource(st)
            # sanitize the slots
            current_resource_slots.set(known_slot_types)
            slots = slots.normalize_slots(ignore_unknown=True)
            resource_spec = KernelResourceSpec(
                container_id='',
                allocations={},
                slots={**slots},  # copy
                mounts=[],
                scratch_disk_size=0,  # TODO: implement (#70)
            )
            resource_opts = self.kernel_config.get('resource_opts', {})
        return resource_spec, resource_opts

    async def prepare_scratch(self) -> None:
        loop = current_loop()

        # Create the scratch, config, and work directories.
        if (
            sys.platform.startswith('linux')
            and self.local_config['container']['scratch-type'] == 'memory'
        ):
            await loop.run_in_executor(None, partial(self.tmp_dir.mkdir, exist_ok=True))
            await create_scratch_filesystem(self.scratch_dir, 64)
            await create_scratch_filesystem(self.tmp_dir, 64)
        else:
            await loop.run_in_executor(None, partial(self.scratch_dir.mkdir, exist_ok=True))

        def _create_scratch_dirs():
            self.config_dir.mkdir(parents=True, exist_ok=True)
            self.work_dir.mkdir(parents=True, exist_ok=True)

        await loop.run_in_executor(None, _create_scratch_dirs)

        if not self.restarting:
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
                jupyter_custom_dir = (self.work_dir / '.jupyter' / 'custom')
                jupyter_custom_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy(jupyter_custom_css_path.resolve(), jupyter_custom_dir / 'custom.css')
                shutil.copy(logo_path.resolve(), jupyter_custom_dir / 'logo.svg')
                shutil.copy(font_path.resolve(), jupyter_custom_dir / 'roboto.ttf')
                shutil.copy(font_italic_path.resolve(), jupyter_custom_dir / 'roboto-italic.ttf')
                shutil.copy(bashrc_path.resolve(), self.work_dir / '.bashrc')
                shutil.copy(bash_profile_path.resolve(), self.work_dir / '.bash_profile')
                shutil.copy(vimrc_path.resolve(), self.work_dir / '.vimrc')
                shutil.copy(tmux_conf_path.resolve(), self.work_dir / '.tmux.conf')
                if KernelFeatures.UID_MATCH in self.kernel_features:
                    uid = self.local_config['container']['kernel-uid']
                    gid = self.local_config['container']['kernel-gid']
                    if os.geteuid() == 0:  # only possible when I am root.
                        os.chown(self.work_dir, uid, gid)
                        os.chown(self.work_dir / '.jupyter', uid, gid)
                        os.chown(self.work_dir / '.jupyter' / 'custom', uid, gid)
                        os.chown(self.work_dir / '.bashrc', uid, gid)
                        os.chown(self.work_dir / '.bash_profile', uid, gid)
                        os.chown(self.work_dir / '.vimrc', uid, gid)
                        os.chown(self.work_dir / '.tmux.conf', uid, gid)

            await loop.run_in_executor(None, _clone_dotfiles)

    async def get_intrinsic_mounts(self) -> Sequence[Mount]:
        loop = current_loop()

        # scratch/config/tmp mounts
        mounts: List[Mount] = [
            Mount(MountTypes.BIND, self.config_dir, Path("/home/config"),
                  MountPermission.READ_ONLY),
            Mount(MountTypes.BIND, self.work_dir, Path("/home/work"),
                  MountPermission.READ_WRITE),
        ]
        if (sys.platform.startswith("linux") and
            self.local_config["container"]["scratch-type"] == "memory"):
            mounts.append(Mount(
                MountTypes.BIND,
                self.tmp_dir,
                Path("/tmp"),
                MountPermission.READ_WRITE,
            ))

        # lxcfs mounts
        lxcfs_root = Path("/var/lib/lxcfs")
        if lxcfs_root.is_dir():
            mounts.extend(
                Mount(
                    MountTypes.BIND,
                    lxcfs_proc_path,
                    "/" / lxcfs_proc_path.relative_to(lxcfs_root),
                    MountPermission.READ_WRITE,
                )
                for lxcfs_proc_path in (lxcfs_root / "proc").iterdir()
            )
            mounts.extend(
                Mount(
                    MountTypes.BIND,
                    lxcfs_root / path,
                    "/" / Path(path),
                    MountPermission.READ_WRITE,
                )
                for path in [
                    "sys/devices/system/cpu",
                    "sys/devices/system/cpu/online",
                ]
                if Path(lxcfs_root / path).exists()
            )

        # extra mounts
        extra_mount_list = await get_extra_volumes(self.docker, self.image_ref.short)
        mounts.extend(Mount(MountTypes.VOLUME, v.name, v.container_path, v.mode)
                      for v in extra_mount_list)

        # debug mounts
        if self.local_config['debug']['coredump']['enabled']:
            mounts.append(Mount(
                MountTypes.BIND,
                self.local_config['debug']['coredump']['path'],
                self.local_config['debug']['coredump']['core_path'],
                MountPermission.READ_WRITE,
            ))

        # agent-socket mount
        mounts.append(Mount(
            MountTypes.BIND,
            self.agent_sockpath,
            Path('/opt/kernel/agent.sock'),
            MountPermission.READ_WRITE,
        ))

        # domain-socket proxy mount
        # (used for special service containers such image importer)
        for host_sock_path in self.internal_data.get('domain_socket_proxies', []):
            await loop.run_in_executor(
                None,
                partial((ipc_base_path / 'proxy').mkdir, parents=True, exist_ok=True))
            host_proxy_path = ipc_base_path / 'proxy' / f'{secrets.token_hex(12)}.sock'
            proxy_server = await asyncio.start_unix_server(
                aiotools.apartial(proxy_connection, host_sock_path),
                str(host_proxy_path))
            await loop.run_in_executor(None, host_proxy_path.chmod, 0o666)
            self.domain_socket_proxies.append(DomainSocketProxy(
                Path(host_sock_path),
                host_proxy_path,
                proxy_server,
            ))
            mounts.append(Mount(
                MountTypes.BIND,
                host_proxy_path,
                host_sock_path,
                MountPermission.READ_WRITE,
            ))

        return mounts

    async def apply_network(self, cluster_info: ClusterInfo) -> None:
        if cluster_info['network_name'] is not None:
            self.container_configs.append({
                'HostConfig': {
                    'NetworkMode': cluster_info['network_name'],
                },
                'NetworkingConfig': {
                    'EndpointsConfig': {
                        cluster_info['network_name']: {
                            'Aliases': [self.kernel_config['cluster_hostname']],
                        },
                    },
                },
            })

    async def install_ssh_keypair(self, cluster_info: ClusterInfo) -> None:
        sshkey = cluster_info['ssh_keypair']
        if sshkey is None:
            return

        def _write_keypair():
            try:
                priv_key_path = (self.config_dir / 'ssh' / 'id_cluster')
                pub_key_path = (self.config_dir / 'ssh' / 'id_cluster.pub')
                priv_key_path.parent.mkdir(parents=True, exist_ok=True)
                priv_key_path.write_text(sshkey['private_key'])
                pub_key_path.write_text(sshkey['public_key'])
                if KernelFeatures.UID_MATCH in self.kernel_features:
                    uid = self.local_config['container']['kernel-uid']
                    gid = self.local_config['container']['kernel-gid']
                    if os.geteuid() == 0:  # only possible when I am root.
                        os.chown(str(priv_key_path), uid, gid)
                        os.chown(str(pub_key_path), uid, gid)
                priv_key_path.chmod(0o600)
            except Exception:
                log.exception('error while writing cluster keypair')

        current_loop().run_in_executor(None, _write_keypair)  # ???

    async def process_mounts(self, mounts: Sequence[Mount]):
        def fix_unsupported_perm(folder_perm: MountPermission) -> MountPermission:
            if folder_perm == MountPermission.RW_DELETE:
                # TODO: enforce readable/writable but not deletable
                # (Currently docker's READ_WRITE includes DELETE)
                return MountPermission.READ_WRITE
            return folder_perm

        container_config = {
            'HostConfig': {
                'Mounts': [
                    {
                        'Target': str(mount.target),
                        'Source': str(mount.source),
                        'Type': mount.type.value,
                        'ReadOnly': fix_unsupported_perm(mount.permission) == MountPermission.READ_ONLY,
                        f'{mount.type.value.capitalize()}Options':
                            mount.opts if mount.opts else {},
                    }
                    for mount in mounts
                ],
            }
        }
        self.container_configs.append(container_config)

    async def apply_accelerator_allocation(self, computer, device_alloc) -> None:
        update_nested_dict(
            self.computer_docker_args,
            await computer.generate_docker_args(self.docker, device_alloc))

    async def spawn(
        self,
        resource_spec: KernelResourceSpec,
        resource_opts,
        environ: Mapping[str, str],
        service_ports,
        preopen_ports,
        cmdargs: List[str]
    ) -> DockerKernel:
        loop = current_loop()
        image_labels = self.kernel_config['image']['labels']
        exposed_ports = [2000, 2001]
        for sport in service_ports:
            exposed_ports.extend(sport['container_ports'])

        if self.restarting:
            pass
        else:
            # Create bootstrap.sh into workdir if needed
            if bootstrap := self.kernel_config.get('bootstrap_script'):

                def _write_user_bootstrap_script():
                    (self.work_dir / 'bootstrap.sh').write_text(bootstrap)
                    if KernelFeatures.UID_MATCH in self.kernel_features:
                        uid = self.local_config['container']['kernel-uid']
                        gid = self.local_config['container']['kernel-gid']
                        if os.geteuid() == 0:
                            os.chown(self.work_dir / 'bootstrap.sh', uid, gid)

                await loop.run_in_executor(None, _write_user_bootstrap_script)

            with StringIO() as buf:
                for k, v in environ.items():
                    buf.write(f'{k}={v}\n')
                accel_envs = self.computer_docker_args.get('Env', [])
                for env in accel_envs:
                    buf.write(f'{env}\n')
                await loop.run_in_executor(
                    None,
                    (self.config_dir / 'environ.txt').write_bytes,
                    buf.getvalue().encode('utf8'),
                )

            with StringIO() as buf:
                resource_spec.write_to_file(buf)
                for dev_type, device_alloc in resource_spec.allocations.items():
                    computer_self = self.computers[dev_type]
                    kvpairs = \
                        await computer_self.instance.generate_resource_data(device_alloc)
                    for k, v in kvpairs.items():
                        buf.write(f'{k}={v}\n')
                await loop.run_in_executor(
                    None,
                    (self.config_dir / 'resource.txt').write_bytes,
                    buf.getvalue().encode('utf8'),
                )

            docker_creds = self.internal_data.get('docker_credentials')
            if docker_creds:
                await loop.run_in_executor(
                    None,
                    (self.config_dir / 'docker-creds.json').write_text,
                    json.dumps(docker_creds))

        # TODO: refactor out dotfiles/sshkey initialization to the base agent?

        # Create SSH keypair only if ssh_keypair internal_data exists and
        # /home/work/.ssh folder is not mounted.
        if self.internal_data.get('ssh_keypair'):
            for mount in resource_spec.mounts:
                container_path = str(mount).split(':')[1]
                if container_path == '/home/work/.ssh':
                    break
            else:
                pubkey = self.internal_data['ssh_keypair']['public_key'].encode('ascii')
                privkey = self.internal_data['ssh_keypair']['private_key'].encode('ascii')
                ssh_dir = self.work_dir / '.ssh'

                def _populate_ssh_config():
                    ssh_dir.mkdir(parents=True, exist_ok=True)
                    ssh_dir.chmod(0o700)
                    (ssh_dir / 'authorized_keys').write_bytes(pubkey)
                    (ssh_dir / 'authorized_keys').chmod(0o600)
                    (self.work_dir / 'id_container').write_bytes(privkey)
                    (self.work_dir / 'id_container').chmod(0o600)
                    if KernelFeatures.UID_MATCH in self.kernel_features:
                        uid = self.local_config['container']['kernel-uid']
                        gid = self.local_config['container']['kernel-gid']
                        if os.geteuid() == 0:  # only possible when I am root.
                            os.chown(ssh_dir, uid, gid)
                            os.chown(ssh_dir / 'authorized_keys', uid, gid)
                            os.chown(self.work_dir / 'id_container', uid, gid)

                await loop.run_in_executor(None, _populate_ssh_config)

        # higher priority dotfiles are stored last to support overwriting
        for dotfile in self.internal_data.get('dotfiles', []):
            if dotfile['path'].startswith('/'):
                if dotfile['path'].startswith('/home/'):
                    path_arr = dotfile['path'].split('/')
                    file_path: Path = self.scratch_dir / '/'.join(path_arr[2:])
                else:
                    file_path = Path(dotfile['path'])
            else:
                file_path = self.work_dir / dotfile['path']
            file_path.parent.mkdir(parents=True, exist_ok=True)
            await loop.run_in_executor(
                None,
                file_path.write_text,
                dotfile['data'])

            tmp = Path(file_path)
            while tmp != self.work_dir:
                tmp.chmod(int(dotfile['perm'], 8))
                # only possible when I am root.
                if KernelFeatures.UID_MATCH in self.kernel_features and os.geteuid() == 0:
                    uid = self.local_config['container']['kernel-uid']
                    gid = self.local_config['container']['kernel-gid']
                    os.chown(tmp, uid, gid)
                tmp = tmp.parent

        # PHASE 4: Run!
        kernel_host = self.local_config['container']['kernel-host']
        if len(exposed_ports) > len(self.port_pool):
            raise RuntimeError('Container ports are not sufficiently available.')
        host_ports = []
        for eport in exposed_ports:
            hport = self.port_pool.pop()
            host_ports.append(hport)

        container_log_size = self.local_config['agent']['container-logs']['max-length']
        container_log_file_count = 5
        container_log_file_size = BinarySize(container_log_size // container_log_file_count)
        container_config: MutableMapping[str, Any] = {
            'Image': self.image_ref.canonical,
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
            'WorkingDir': "/home/work",
            'Hostname': self.kernel_config['cluster_hostname'],
            'Labels': {
                'ai.backend.kernel-id': str(self.kernel_id),
                'ai.backend.internal.block-service-ports':
                    '1' if self.internal_data.get('block_service_ports', False) else '0'
            },
            'HostConfig': {
                'Init': True,
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
        # merge all container configs generated during prior preparation steps
        for c in self.container_configs:
            update_nested_dict(container_config, c)
        if self.local_config['container']['sandbox-type'] == 'jail':
            update_nested_dict(container_config, {
                'HostConfig': {
                    'SecurityOpt': ['seccomp=unconfined', 'apparmor=unconfined'],
                }
            })

        if resource_opts and resource_opts.get('shmem'):
            shmem = int(resource_opts.get('shmem', '0'))
            self.computer_docker_args['HostConfig']['ShmSize'] = shmem
            self.computer_docker_args['HostConfig']['MemorySwap'] -= shmem
            self.computer_docker_args['HostConfig']['Memory'] -= shmem

        encoded_preopen_ports = ','.join(f'{port_no}:preopen:{port_no}' for port_no in preopen_ports)
        container_config['Labels']['ai.backend.service-ports'] = \
                image_labels['ai.backend.service-ports'] + ',' + encoded_preopen_ports
        update_nested_dict(container_config, self.computer_docker_args)
        kernel_name = f"kernel.{self.image_ref.name.split('/')[-1]}.{self.kernel_id}"
        if self.local_config['debug']['log-kernel-config']:
            log.debug('full container config: {!r}', pretty(container_config))

        # We are all set! Create and start the container.
        try:
            container = await self.docker.containers.create(
                config=container_config, name=kernel_name)
            cid = container._id

            resource_spec.container_id = cid
            # Write resource.txt again to update the contaienr id.
            with open(self.config_dir / 'resource.txt', 'w') as f:
                await loop.run_in_executor(None, resource_spec.write_to_file, f)
            async with AsyncFileWriter(
                target_filename=self.config_dir / 'resource.txt',
                access_mode='a'
            ) as writer:
                for dev_name, device_alloc in resource_spec.allocations.items():
                    computer_ctx = self.computers[dev_name]
                    kvpairs = \
                        await computer_ctx.instance.generate_resource_data(device_alloc)
                    for k, v in kvpairs.items():
                        await writer.write(f'{k}={v}\n')

            await container.start()
        except asyncio.CancelledError:
            raise
        except Exception:
            # Oops, we have to restore the allocated resources!
            if (sys.platform.startswith('linux') and
                self.local_config['container']['scratch-type'] == 'memory'):
                await destroy_scratch_filesystem(self.scratch_dir)
                await destroy_scratch_filesystem(self.tmp_dir)
                await loop.run_in_executor(None, shutil.rmtree, self.tmp_dir)
            await loop.run_in_executor(None, shutil.rmtree, self.scratch_dir)
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
            self.kernel_id,
            self.image_ref,
            self.kspec_version,
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
                'domain_socket_proxies': self.domain_socket_proxies,
                'block_service_ports': self.internal_data.get('block_service_ports', False)
            })
        return kernel_obj


class DockerAgent(AbstractAgent[DockerKernel, DockerKernelCreationContext]):

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
        await self.check_swarm_status()
        if self.heartbeat_extra_info['swarm_enabled']:
            log.info('The Docker Swarm cluster is configured and enabled')
        (ipc_base_path / 'container').mkdir(parents=True, exist_ok=True)
        self.agent_sockpath = ipc_base_path / 'container' / f'agent.{self.local_instance_id}.sock'
        socket_relay_name = f"backendai-socket-relay.{self.local_instance_id}"
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
            name=socket_relay_name,
        )
        await socket_relay_container.ensure_running_latest()
        self.agent_sock_task = asyncio.create_task(self.handle_agent_socket())
        self.monitor_docker_task = asyncio.create_task(self.monitor_docker_events())
        self.monitor_swarm_task = asyncio.create_task(self.check_swarm_status(as_task=True))

    async def shutdown(self, stop_signal: signal.Signals):
        # Stop handling agent sock.
        if self.agent_sock_task is not None:
            self.agent_sock_task.cancel()
            await self.agent_sock_task

        try:
            await super().shutdown(stop_signal)
        finally:
            # Stop docker event monitoring.
            if self.monitor_docker_task is not None:
                self.monitor_docker_task.cancel()
                await self.monitor_docker_task
            await self.docker.close()

        if self.monitor_swarm_task is not None:
            self.monitor_swarm_task.cancel()
            await self.monitor_swarm_task

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
                kernel_id = "(unknown)"
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
                    pass
                except Exception:
                    log.exception(
                        "error while fetching container information (cid:{}, k:{})",
                        container._id, kernel_id,
                    )

            fetch_tasks.append(_fetch_container_info(container))

        await asyncio.gather(*fetch_tasks, return_exceptions=True)
        return result

    async def check_swarm_status(self, as_task=False):
        try:
            while True:
                if as_task:
                    await asyncio.sleep(30)
                try:
                    swarm_enabled = self.local_config['container'].get('swarm-enabled', False)
                    if not swarm_enabled:
                        continue
                    docker_info = await self.docker.system.info()
                    if docker_info['Swarm']['LocalNodeState'] == 'inactive':
                        raise InitializationError(
                            "The swarm mode is enabled but the node state of "
                            "the local Docker daemon is inactive."
                        )
                except InitializationError as e:
                    log.exception(str(e))
                    swarm_enabled = False
                finally:
                    self.heartbeat_extra_info = {
                        'swarm_enabled': swarm_enabled
                    }
                    if not as_task:
                        return
        except asyncio.CancelledError:
            pass

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
        """
        A simple request-reply socket handler for in-container processes.
        For ease of implementation in low-level languages such as C,
        it uses a simple C-friendly ZeroMQ-based multipart messaging protocol.

        The agent listens on a local TCP port and there is a socat relay
        that proxies this port via a UNIX domain socket mounted inside
        actual containers.  The reason for this is to avoid inode changes
        upon agent restarts by keeping the relay container running persistently,
        so that the mounted UNIX socket files don't get to refere a dangling pointer
        when the agent is restarted.

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
        """
        terminating = False
        while True:
            agent_sock = self.zmq_ctx.socket(zmq.REP)
            try:
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
                        terminating = True
                        raise
                    except Exception as e:
                        log.exception("handle_agent_socket(): internal error")
                        reply = [struct.pack('i', -1), f'Error: {e}'.encode('utf-8')]
                    await agent_sock.send_multipart(reply)
            except asyncio.CancelledError:
                terminating = True
                return
            except zmq.ZMQError:
                log.exception("handle_agent_socket(): zmq error")
            finally:
                agent_sock.close()
                if not terminating:
                    log.info("handle_agent_socket(): rebinding the socket")

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

    async def init_kernel_context(
        self,
        kernel_id: KernelId,
        kernel_config: KernelCreationConfig,
        *,
        restarting: bool = False,
    ) -> DockerKernelCreationContext:
        return DockerKernelCreationContext(
            kernel_id,
            kernel_config,
            self.local_config,
            self.computers,
            self.port_pool,
            self.agent_sockpath,
            self.resource_lock,
            self.docker,
            restarting=restarting
        )

    async def restart_kernel__load_config(
        self,
        kernel_id: KernelId,
        name: str,
    ) -> bytes:
        loop = current_loop()
        scratch_dir = (self.local_config['container']['scratch-root'] / str(kernel_id)).resolve()
        config_dir = scratch_dir / 'config'
        return await loop.run_in_executor(
            None,
            (config_dir / name).read_bytes,
        )

    async def restart_kernel__store_config(
        self,
        kernel_id: KernelId,
        name: str,
        data: bytes,
    ) -> None:
        loop = current_loop()
        scratch_dir = (self.local_config['container']['scratch-root'] / str(kernel_id)).resolve()
        config_dir = scratch_dir / 'config'
        return await loop.run_in_executor(
            None,
            (config_dir / name).write_bytes,
            data,
        )

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
                it = container.log(
                    stdout=True, stderr=True, follow=True,
                )
                async with aiotools.aclosing(it):
                    async for line in it:
                        yield line.encode('utf-8')

            try:
                with timeout(60):
                    await self.collect_logs(kernel_id, container_id, log_iter())
            except asyncio.TimeoutError:
                log.warning('timeout for collecting container logs (k:{}, cid:{})',
                            kernel_id, container_id)
            except Exception as e:
                log.warning('error while collecting container logs (k:{}, cid:{})',
                            kernel_id, container_id, exc_info=e)

        kernel_obj = self.kernel_registry.get(kernel_id)
        if kernel_obj is not None:
            for domain_socket_proxy in kernel_obj.get('domain_socket_proxies', []):
                if domain_socket_proxy.proxy_server.is_serving():
                    domain_socket_proxy.proxy_server.close()
                    await domain_socket_proxy.proxy_server.wait_closed()
                    try:
                        domain_socket_proxy.host_proxy_path.unlink()
                    except IOError:
                        pass

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
            except CalledProcessError:
                pass
            except FileNotFoundError:
                pass

    async def create_overlay_network(self, network_name: str) -> None:
        if not self.heartbeat_extra_info['swarm_enabled']:
            raise RuntimeError("This agent has not joined to a swarm cluster.")
        await self.docker.networks.create({
            'Name': network_name,
            'Driver': 'overlay',
            'Attachable': True,
            'Labels': {
                'ai.backend.cluster-network': '1'
            }
        })

    async def destroy_overlay_network(self, network_name: str) -> None:
        network = await self.docker.networks.get(network_name)
        await network.delete()

    async def create_local_network(self, network_name: str) -> None:
        await self.docker.networks.create({
            'Name': network_name,
            'Driver': 'bridge',
            'Labels': {
                'ai.backend.cluster-network': '1'
            }
        })

    async def destroy_local_network(self, network_name: str) -> None:
        network = await self.docker.networks.get(network_name)
        await network.delete()

    async def monitor_docker_events(self):

        async def handle_action_start(kernel_id: KernelId, evdata: Mapping[str, Any]) -> None:
            await self.inject_container_lifecycle_event(
                kernel_id,
                LifecycleEvent.START,
                'new-container-started',
                container_id=ContainerId(evdata['Actor']['ID']),
            )

        async def handle_action_die(kernel_id: KernelId, evdata: Mapping[str, Any]) -> None:
            # When containers die, we immediately clean up them.
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

        while True:
            subscriber = self.docker.events.subscribe(create_task=True)
            try:
                while True:
                    try:
                        # ref: https://docs.docker.com/engine/api/v1.40/#operation/SystemEvents
                        evdata = await subscriber.get()
                        if evdata is None:
                            # Break out to the outermost loop when the connection is closed
                            log.info("monitor_docker_events(): restarting aiodocker event subscriber")
                            break
                        if evdata['Type'] != 'container':
                            # Our interest is the container-related events
                            continue
                        container_name = evdata['Actor']['Attributes']['name']
                        kernel_id = await get_kernel_id_from_container(container_name)
                        if kernel_id is None:
                            continue
                        if (
                            self.local_config['debug']['log-docker-events']
                            and evdata['Action'] in ('start', 'die')
                        ):
                            log.debug('docker-event: action={}, actor={}',
                                      evdata['Action'], evdata['Actor'])
                        if evdata['Action'] == 'start':
                            await asyncio.shield(handle_action_start(kernel_id, evdata))
                        elif evdata['Action'] == 'die':
                            await asyncio.shield(handle_action_die(kernel_id, evdata))
                    except asyncio.CancelledError:
                        # We are shutting down...
                        return
                    except Exception:
                        log.exception("monitor_docker_events(): unexpected error")
            finally:
                await asyncio.shield(self.docker.events.stop())
