import asyncio
from contextvars import ContextVar
from decimal import Decimal
import json
import logging, logging.config
from pathlib import Path
import pkg_resources
from pprint import pformat
import random
import secrets
import time
from typing import (
    Any, Dict, List, Callable,
    Optional, Mapping, MutableMapping,
    Tuple, Type, Union, Sequence
)

import aiotools
from async_timeout import timeout as _timeout
import attr
from callosum.rpc import Peer, RPCUserError
from callosum.lower.zeromq import ZeroMQAddress, ZeroMQRPCTransport
from kubernetes_asyncio import client as K8sClient, config as K8sConfig
import zmq

from ai.backend.common import msgpack
from ai.backend.common.docker import ImageRef
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    AutoPullBehavior,
    ImageRegistry,
    DeviceName,
    BinarySize,
    KernelCreationConfig,
    KernelCreationResult,
    KernelId,
    MetricKey, MetricValue,
    MountPermission,
    ResourceSlot,
    SlotName,
    ServicePort,
    SessionTypes,
    ServicePortProtocols,
)
from .kernel import prepare_runner_files, K8sKernel
from .k8sapi import (
    PVCMountSpec,
    ConfigMapMountSpec,
    HostPathMountSpec,
    KernelDeployment,
    ConfigMap,
    Service,
    NFSPersistentVolume,
    NFSPersistentVolumeClaim
)
from .resources import (
    AbstractComputePlugin,
    Mount,
    detect_resources,
)
from ..agent import (
    AbstractAgent,
    KernelCreationContext,
)
from ..exception import (
    K8sError,
    AgentError
)
from ..resources import KernelResourceSpec
from ..utils import parse_service_ports

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.server'))
agent_peers: MutableMapping[str, zmq.asyncio.Socket] = {}  # agent-addr to socket


def format_binarysize(s: BinarySize) -> str:
    return f'{s:g}'[:-1] + 'Gi'


def parse_service_port(s: str) -> Dict[str, Any]:
    try:
        name, protocol, _port = s.split(':')
    except (ValueError, IndexError):
        raise ValueError('Invalid service port definition format', s)
    assert protocol in ('tcp', 'pty', 'http'), \
           f'Unsupported service port protocol: {protocol}'
    try:
        port = int(_port)
    except ValueError:
        raise ValueError('Invalid port number', port)
    if port <= 1024:
        raise ValueError('Service port number must be larger than 1024.')
    if port in (2000, 2001):
        raise ValueError('Service port 2000 and 2001 is reserved for internal use.')
    return {
        'name': name,
        'protocol': protocol,
        'container_ports': (port,),
        'host_ports': None,  # determined after container start
    }


class PeerInvoker(Peer):

    class _CallStub:

        _cached_funcs: Dict[str, Callable]
        order_key: ContextVar[Optional[str]]

        def __init__(self, peer: Peer):
            self._cached_funcs = {}
            self.peer = peer
            self.order_key = ContextVar('order_key', default=None)

        def __getattr__(self, name: str):
            if f := self._cached_funcs.get(name, None):  # noqa
                return f
            else:
                async def _wrapped(*args, **kwargs):
                    request_body = {
                        'args': args,
                        'kwargs': kwargs,
                    }
                    self.peer.last_used = time.monotonic()
                    ret = await self.peer.invoke(name, request_body,
                                                 order_key=self.order_key.get())
                    self.peer.last_used = time.monotonic()
                    return ret
                self._cached_funcs[name] = _wrapped
                return _wrapped

    call: _CallStub
    last_used: float

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.call = self._CallStub(self)
        self.last_used = time.monotonic()


@aiotools.actxmgr
async def RPCContext(addr, timeout=None, *, order_key: str = None):
    global agent_peers
    peer = agent_peers.get(addr, None)
    if peer is None:
        log.debug('Estabilshing connection to tcp://{}:16001', addr)
        peer = PeerInvoker(
            connect=ZeroMQAddress(f'tcp://{addr}:16001'),
            transport=ZeroMQRPCTransport,
            serializer=msgpack.packb,
            deserializer=msgpack.unpackb,
        )
        await peer.__aenter__()
        agent_peers[addr] = peer
    try:
        with _timeout(timeout):
            peer.call.order_key.set(order_key)
            yield peer
    except RPCUserError as orig_exc:
        raise AgentError(orig_exc.name, orig_exc.args)
    except Exception:
        raise


@attr.s(auto_attribs=True, slots=True)
class K8sKernelCreationContext(KernelCreationContext):
    k8sCoreApi: Any  # TODO: type annotation
    k8sAppsApi: Any  # TODO: type annotation
    deployment: Optional[KernelDeployment]


class K8sAgent(AbstractAgent[K8sKernel, K8sKernelCreationContext]):
    vfolder_as_pvc: bool
    k8s_images: List[str]
    workers: dict

    def __init__(self, config) -> None:
        super().__init__(config)

    async def __ainit__(self) -> None:
        self.vfolder_as_pvc = False
        self.k8s_images = []

        self.workers = {}

        await K8sConfig.load_kube_config()
        await self.fetch_workers()

        await super().__ainit__()

        if 'vfolder-pv' in self.config.keys():
            await self.ensure_vfolder_pv()

        await self.check_krunner_pv_status()
        await prepare_runner_files(self.config['baistatic']['mounted-at'])

        k8sVersionApi = K8sClient.VersionApi()
        k8s_version_response = await k8sVersionApi.get_code()
        k8s_version = k8s_version_response.git_version
        k8s_platform = k8s_version_response.platform

        log.info('running with Kubernetes {0} on Platform {1}',
                 k8s_version, k8s_platform)

    @staticmethod
    async def detect_resources(resource_configs: Mapping[str, Any],
                               plugin_configs: Mapping[str, Any]) \
                               -> Tuple[
                                   Mapping[DeviceName, Type[AbstractComputePlugin]],
                                   Mapping[SlotName, Decimal]
                               ]:
        return await detect_resources(resource_configs)

    async def check_krunner_pv_status(self) -> None:
        await K8sConfig.load_kube_config()
        k8sCoreApi = K8sClient.CoreV1Api()
        nfs_pv = await k8sCoreApi.list_persistent_volume(
            label_selector='backend.ai/bai-static-nfs-server'
        )
        if len(nfs_pv.items) == 0:
            # PV does not exists; create one
            pv = NFSPersistentVolume(
                self.config['baistatic']['nfs-addr'], self.config['baistatic']['path'],
                'backend-ai-static-files-pv', format_binarysize(self.config['baistatic']['capacity'])
            )
            pv.label('backend.ai/bai-static-nfs-server', self.config['baistatic']['nfs-addr'])
            pv.options = [x.strip() for x in self.config['baistatic']['options'].split(',')]

            try:
                await k8sCoreApi.create_persistent_volume(body=pv.as_dict())
            except:
                raise

        nfs_pvc = await k8sCoreApi.list_namespaced_persistent_volume_claim(
            'backend-ai', label_selector='backend.ai/bai-static-nfs-server'
        )
        if len(nfs_pvc.items) == 0:
            # PV does not exists; create one
            pvc = NFSPersistentVolumeClaim(
                'backend-ai-static-files-pvc', 'backend-ai-static-files-pv',
                format_binarysize(self.config['baistatic']['capacity'])
            )
            pvc.label('backend.ai/bai-static-nfs-server', self.config['baistatic']['nfs-addr'])
            try:
                await k8sCoreApi.create_namespaced_persistent_volume_claim(
                    'backend-ai', body=pvc.as_dict()
                )
            except:
                raise

    async def ensure_vfolder_pv(self) -> None:
        await K8sConfig.load_kube_config()
        k8sCoreApi = K8sClient.CoreV1Api()
        log.debug('Trying to create vFolder PV/PVC...')

        addr = self.config['vfolder-pv']['nfs-addr']
        path = self.config['vfolder-pv']['path']
        capacity = format_binarysize(self.config['vfolder-pv']['capacity'])
        options = [x.strip() for x in self.config['vfolder-pv']['options'].split(',')]

        nfs_pv = await k8sCoreApi.list_persistent_volume(label_selector='backend.ai/vfolder')
        if len(nfs_pv.items) == 0:
            pv = NFSPersistentVolume(addr, path, 'backend-ai-vfolder-pv', capacity)
            pv.label('backend.ai/vfolder', '')
            pv.options = options
            try:
                await k8sCoreApi.create_persistent_volume(body=pv.as_dict())
            except:
                raise

        nfs_pvc = await k8sCoreApi.list_namespaced_persistent_volume_claim(
            'backend-ai', label_selector='backend.ai/vfolder'
        )
        if len(nfs_pvc.items) == 0:
            pvc = NFSPersistentVolumeClaim(
                'backend-ai-vfolder-pvc', 'backend-ai-vfolder-pv', str(capacity)
            )
            pvc.label('backend.ai/vfolder', '')
            try:
                await k8sCoreApi.create_namespaced_persistent_volume_claim(
                    'backend-ai', body=pvc.as_dict()
                )
            except:
                raise
        log.debug('NFS vFolder mounted')
        self.vfolder_as_pvc = True

    async def fetch_workers(self) -> None:
        await K8sConfig.load_kube_config()
        k8sCoreApi = K8sClient.CoreV1Api()
        nodes = await k8sCoreApi.list_node()
        for node in nodes.items:
            is_master = False
            for taint in node.spec.taints if node.spec.taints is not None else []:
                if taint.key == 'node-role.kubernetes.io/master' and taint.effect == 'NoSchedule':
                    is_master = True
                    break

            if is_master:
                continue
            self.workers[node.metadata.name] = node.status.capacity
            for addr in node.status.addresses:
                if addr.type == 'ExternalIP':
                    self.workers[node.metadata.name]['ExternalIP'] = addr.address
                if addr.type == 'InternalIP':
                    self.workers[node.metadata.name]['InternalIP'] = addr.address

    async def scan_running_kernels(self) -> None:
        await K8sConfig.load_kube_config()
        k8sCoreApi = K8sClient.CoreV1Api()
        k8sAppsApi = K8sClient.AppsV1Api()

        deployments = await k8sAppsApi.list_namespaced_deployment(
            'backend-ai', label_selector='backend.ai/kernel'
        )

        for deployment in deployments.items:
            kernel_id = deployment.metadata.labels['backend.ai/kernel_id']
            _kernel_id = KernelId(kernel_id)
            try:
                registry_cm = await k8sCoreApi.read_namespaced_config_map(
                    f'{kernel_id}-registry', 'backend-ai'
                )
            except:
                log.error('ConfigMap for kernel {0} not found, skipping restoration', kernel_id)
                continue
            registry = json.loads(registry_cm.data['registry'])
            ports = deployment.spec.template.spec.containers[0].ports
            port_map = {
                p.container_port: p.host_port or 0 for p in ports
            }

            service_ports: List[ServicePort] = []
            service_ports.append({
                'name': 'sshd',
                'protocol': ServicePortProtocols('tcp'),
                'container_ports': (2200,),
                'host_ports': (port_map.get(2200, None),),
            })
            service_ports.append({
                'name': 'ttyd',
                'protocol': ServicePortProtocols('http'),
                'container_ports': (7681,),
                'host_ports': (port_map.get(7681, None),),
            })
            for service_port in parse_service_ports(registry_cm.data['registry']):
                service_port['host_ports'] = tuple(
                    port_map.get(cport, None) for cport in service_port['container_ports']
                )
                service_ports.append(service_port)
            self.kernel_registry[_kernel_id] = await K8sKernel.new(
                deployment.metadata.name,
                ImageRef(registry['lang']['canonical'], registry['lang']['registry']),
                registry['version'],
                config=self.config,
                resource_spec=KernelResourceSpec.read_from_string(registry['resource_spec']),
                service_ports=service_ports,
                data={
                    'kernel_host': random.choice([x['InternalIP'] for x in self.workers.values()]),
                    'repl_in_port': registry['repl_in_port'],
                    'repl_out_port': registry['repl_out_port'],
                    'stdin_port': registry['stdin_port'],
                    'stdout_port': registry['stdout_port'],
                    'host_ports': registry['host_ports'],
                    'sync_stat': False
                }
            )

            log.info('Restored kernel {0} from K8s', kernel_id)

    async def scan_images(self, interval: float = None) -> None:
        updated_images: Dict[str, str] = {}
        for name, node in self.workers.items():
            async with RPCContext(node['InternalIP'], 30) as rpc:
                _images: Dict[str, str] = await rpc.call.scan_images()
                for tag, image_id in _images.items():
                    if tag not in updated_images.keys():
                        updated_images[tag] = image_id

        self.images = updated_images

    async def check_image(self, image_id: str, auto_pull: AutoPullBehavior,
                          canonical: str,
                          known_registries: Union[Mapping[str, Any], Sequence[str]] = None) \
                              -> Dict[str, bool]:
        status: Dict[str, bool] = {}
        for name, node in self.workers.items():
            async with RPCContext(node['InternalIP'], 30) as rpc:
                status[name] = await rpc.call.check_image(canonical, known_registries, image_id,
                                                          auto_pull)
        return status

    async def pull_image(self, registry_conf: ImageRegistry, node_name: str,
                          canonical: str,
                          known_registries: Union[Mapping[str, Any], Sequence[str]] = None):
        node = self.workers[node_name]
        async with RPCContext(node['InternalIP']) as rpc:
            await rpc.call.pull_image(canonical, known_registries, registry_conf)

    async def get_service_ports_from_label(self, canonical: str) -> str:
        node: dict = random.choice(tuple(self.workers.values()))
        async with RPCContext(node['InternalIP']) as rpc:
            image_info = await rpc.call.inspect_image(canonical)
            return image_info['Config']['Labels']['ai.backend.service-ports']

    async def collect_node_stat(self, interval: float):
        pass

    async def create_kernel__init_context(
        self,
        kernel_id: KernelId,
        kernel_config: KernelCreationConfig,
        *,
        restarting: bool = False,
    ) -> K8sKernelCreationContext:
        base_ctx = await super().create_kernel__init_context(kernel_id, kernel_config)
        # Load K8s API object
        await K8sConfig.load_kube_config()
        return K8sKernelCreationContext(
            # k8s-specific fields
            k8sCoreApi=K8sClient.CoreV1Api(),
            k8sAppsApi=K8sClient.AppsV1Api(),
            deployment=None,
            # should come last because of python/mypy#9395
            **attr.asdict(base_ctx),
        )

    async def create_kernel__get_extra_envs(
        self,
        ctx: K8sKernelCreationContext,
    ) -> Mapping[str, str]:
        return {}

    async def create_kernel__prepare_resource_spec(
        self,
        ctx: K8sKernelCreationContext,
    ) -> Tuple[KernelResourceSpec, Optional[Mapping[str, Any]]]:
        assert not ctx.restarting, "restarting k8s session is not supported"
        slots = ResourceSlot.from_json(ctx.kernel_config['resource_slots'])
        # Ensure that we have intrinsic slots.
        assert SlotName('cpu') in slots
        assert SlotName('mem') in slots
        resource_spec = KernelResourceSpec(
            container_id='K8sDUMMYCID',
            allocations={},
            slots={**slots},  # copy
            mounts=[],
            scratch_disk_size=0,  # TODO: implement (#70)
            idle_timeout=ctx.kernel_config['idle_timeout'],
        )
        return resource_spec, None

    async def create_kernel__prepare_scratch(
        self,
        ctx: K8sKernelCreationContext,
    ) -> None:
        pass

    async def create_kernel__get_intrinsic_mounts(
        self,
        ctx: K8sKernelCreationContext,
    ) -> Sequence[Mount]:
        return []

    async def create_kernel__prepare_network(
        self,
        ctx: K8sKernelCreationContext,
    ) -> None:
        pass

    async def create_kernel__process_mounts(
        self,
        ctx: K8sKernelCreationContext,
        mounts: Sequence[Mount],
    ) -> None:

        arch, matched_distro, _, krunner_volume, _ = \
            self._create_kernel__get_krunner_info(ctx)

        # Create deployment object with given image name
        canonical = ctx.kernel_config['image']['canonical']
        registry_name, repo, name_with_tag = canonical.split('/')
        deployment = KernelDeployment(
            str(ctx.kernel_id), f'{registry_name}/{repo}/{name_with_tag}',
            krunner_volume, arch,
            name=f"kernel-{ctx.image_ref.name.split('/')[-1]}-{ctx.kernel_id}".replace('.', '-')
        )
        ctx.deployment = deployment

        # If agent is using vFolder by NFS PVC, check if vFolder PVC exists and Bound
        # # otherwise raise since we can't use vfolder
        if self.vfolder_as_pvc:
            try:
                nfs_pvc = await ctx.k8sCoreApi.list_namespaced_persistent_volume_claim(
                    'backend-ai', label_selector='backend.ai/vfolder'
                )
                if len(nfs_pvc.items) == 0:
                    raise K8sError('No PVC for backend.ai static files')
                pvc = nfs_pvc.items[0]
                if pvc.status.phase != 'Bound':
                    raise K8sError('PVC not Bound')
            except:
                raise
            deployment.vfolder_pvc = pvc.metadata.name

        # Check if NFS PVC for static files exists and bound
        nfs_pvc = await ctx.k8sCoreApi.list_namespaced_persistent_volume_claim(
            'backend-ai', label_selector='backend.ai/bai-static-nfs-server'
        )
        if len(nfs_pvc.items) == 0:
            raise K8sError('No PVC for backend.ai static files')
        pvc = nfs_pvc.items[0]
        if pvc.status.phase != 'Bound':
            raise K8sError('PVC not Bound')

        if krunner_volume is None:
            raise RuntimeError(f'Cannot run container based on {matched_distro}')

        def fix_unsupported_perm(folder_perm: MountPermission) -> MountPermission:
            if folder_perm == MountPermission.RW_DELETE:
                # TODO: enforce readable/writable but not deletable
                # (Currently docker's READ_WRITE includes DELETE)
                return MountPermission.READ_WRITE
            return folder_perm

        # def _mount(kernel_id: str, hostPath: str, mountPath: str, mountType: str, perm='ro'):
        #     name = (kernel_id + '-' + mountPath.split('/')[-1]).replace('.', '-')
        #     deployment.mount_hostpath(
        #         HostPathMountSpec(name, hostPath, mountPath, mountType, perm)
        #     )

        # Register to the deployment object
        deployment.baistatic_pvc = pvc.metadata.name
        krunner_root = Path(pkg_resources.resource_filename('ai.backend.runner', 'entrypoint.sh')).parent
        for mount in mounts:
            if krunner_root in mount.source.parents:
                mount_type = 'Directory' if mount.source.is_dir() else 'File'
                deployment.mount_krunner_pvc(
                    PVCMountSpec(
                        mount.source.relative_to(krunner_root),
                        mount.target,
                        mount_type,
                        fix_unsupported_perm(mount.permission).value,
                    )
                )
            else:
                if mount.is_unmanaged or not self.vfolder_as_pvc:
                    name = (
                        str(ctx.kernel_id) + '-' +
                        str(mount.source.absolute().as_posix()).split('/')[-1]
                    ).replace('.', '-')
                    deployment.mount_hostpath(
                        HostPathMountSpec(
                            name,
                            mount.source.absolute().as_posix(),
                            mount.target,
                            'Directory',
                            fix_unsupported_perm(mount.permission).value,
                        )
                    )
                else:
                    deployment.mount_vfolder_pvc(
                        PVCMountSpec(
                            mount.source.absolute().as_posix(),
                            mount.target,
                            'Directory',
                            fix_unsupported_perm(mount.permission).value,
                        )
                    )

    async def create_kernel__spawn(
        self,
        ctx: K8sKernelCreationContext,
        resource_spec: KernelResourceSpec,
        resource_opts,
        environ: Mapping[str, str],
        service_ports,
        preopen_ports,
        cmdargs: List[str],
    ) -> K8sKernel:
        # TODO
        assert ctx.deployment is not None

        # k8s-specific-marker: kubelet slot type
        # Request additional resources to Kubelet
        slots = resource_spec.slots
        for slot_type in slots.keys():
            if slot_type not in ['cpu', 'mem']:
                ctx.deployment.append_resource(f'backend.ai/{slot_type}', int(slots[slot_type]))

        # PHASE 3: Store the resource spec as plain text, and store it to ConfigMap later.

        resource_txt = resource_spec.write_to_string()
        for dev_type, device_alloc in resource_spec.allocations.items():
            computer_set = self.computers[dev_type]
            kvpairs = \
                await computer_set.klass.generate_resource_data(device_alloc)
            for k, v in kvpairs.items():
                resource_txt += f'{k}={v}\n'

        environ_txt = "\n".join(f"{k}={v}" for k, v in environ.items())

        # PHASE 4: Run!
        exposed_ports = [2000, 2001]
        for sport in service_ports:
            exposed_ports.extend(sport['container_ports'])
        cmdargs.append('--debug')

        # Prepare ConfigMap to store resource spec
        configmap = ConfigMap(str(ctx.kernel_id), 'configmap')
        configmap.put('environ', environ_txt)
        configmap.put('resource', resource_txt)

        # Set up appropriate arguments for kernel image
        ctx.deployment.cmd = cmdargs
        ctx.deployment.env = environ
        ctx.deployment.mount_configmap(
            ConfigMapMountSpec('environ', configmap.name, 'environ', '/home/config/environ_base.txt')
        )
        ctx.deployment.mount_configmap(
            ConfigMapMountSpec('resource', configmap.name, 'resource', '/home/config/resource_base.txt')
        )
        ctx.deployment.ports = exposed_ports

        # Create K8s service object to expose REPL port
        repl_service = Service(str(ctx.kernel_id), 'repl', ctx.deployment.name,
            [(2000, 'repl-in'), (2001, 'repl-out')], service_type='NodePort')
        # Create K8s service object to expose Service port
        expose_service = Service(
            str(ctx.kernel_id), 'expose', ctx.deployment.name,
            [(port['container_ports'][0], f'{ctx.kernel_id}-{port["name"]}')
                for port in service_ports],
            service_type='NodePort'
        )

        ctx.deployment.label('backend.ai/kernel', '')

        # We are all set! Create and start the container.

        # TODO: Find appropriate model import for V1ServicePort
        node_ports: List[Any] = []

        # Send request to K8s API
        if len(exposed_ports) > 0:
            try:
                exposed_service_api_response = await ctx.k8sCoreApi.create_namespaced_service(
                    'backend-ai', body=expose_service.as_dict()
                )
                node_ports = exposed_service_api_response.spec.ports
            except:
                raise
        try:
            repl_service_api_response = await ctx.k8sCoreApi.create_namespaced_service(
                'backend-ai', body=repl_service.as_dict()
            )
        except:
            await ctx.k8sCoreApi.delete_namespaced_service(expose_service.name, 'backend-ai')
            raise
        try:
            await ctx.k8sCoreApi.create_namespaced_config_map(
                'backend-ai', body=configmap.as_dict(), pretty='pretty_example'
            )
        except:
            await ctx.k8sCoreApi.delete_namespaced_service(repl_service.name, 'backend-ai')
            await ctx.k8sCoreApi.delete_namespaced_service(expose_service.name, 'backend-ai')
            raise
        try:
            await ctx.k8sAppsApi.create_namespaced_deployment(
                'backend-ai', body=ctx.deployment.as_dict(), pretty='pretty_example'
            )
        except:
            await ctx.k8sCoreApi.delete_namespaced_service(repl_service.name, 'backend-ai')
            await ctx.k8sCoreApi.delete_namespaced_service(expose_service.name, 'backend-ai')
            await ctx.k8sCoreApi.delete_namespaced_config_map(configmap.name, 'backend-ai')
            raise

        stdin_port = 0
        stdout_port = 0
        repl_in_port = 0
        repl_out_port = 0

        exposed_port_results: Dict[int, int] = {}

        # Randomly select IP from all available worker nodes
        # When service is exposed via NodePort, we do not have to make request only to the worker node
        # where pod is started
        target_node_ip = random.choice([x['InternalIP'] for x in self.workers.values()])

        for port in node_ports:
            exposed_port_results[port.port] = port.node_port

        for port in service_ports:
            host_ports = []
            for cport in port['container_ports']:
                host_ports.append(exposed_port_results[cport])
            port['host_ports'] = host_ports
        # Check NodePort assigend to REPL
        for nodeport in repl_service_api_response.spec.ports:
            if nodeport.target_port == 2000:
                log.debug('assigning {0} as REPL in port', nodeport.node_port)
                repl_in_port = nodeport.node_port
            elif nodeport.target_port == 2001:
                log.debug('assigning {0} as REPL out port', nodeport.node_port)
                repl_out_port = nodeport.node_port

        # Settings validation
        if repl_in_port == 0:
            await self.destroy_kernel(ctx.kernel_id, 'nodeport-assign-error')
            raise K8sError('REPL in port not assigned')
        if repl_out_port == 0:
            await self.destroy_kernel(ctx.kernel_id, 'elb-assign-error')
            raise K8sError('REPL out port not assigned')

        kernel_obj: K8sKernel = await K8sKernel.new(
            ctx.deployment.name,
            ctx.image_ref,
            ctx.kspec_version,
            config=self.config,
            resource_spec=resource_spec,
            service_ports=service_ports,
            data={
                'kernel_host': target_node_ip,  # IP or FQDN
                'repl_in_port': repl_in_port,  # Int
                'repl_out_port': repl_out_port,  # Int
                'stdin_port': stdin_port,    # legacy, Int
                'stdout_port': stdout_port,  # legacy, Int
                'host_ports': exposed_port_results.values(),  # List[Int]
                'sync_stat': False
            }
        )
        await kernel_obj.scale(1)
        self.kernel_registry[ctx.kernel_id] = kernel_obj

        registry_cm = ConfigMap(str(ctx.kernel_id), 'registry')
        registry_cm.put('registry', json.dumps({
            'lang': {
                'canonical': ctx.kernel_config['image']['canonical'],
                'registry': [ctx.kernel_config['image']['registry']['name']]
            },
            'version': ctx.kspec_version,  # Int
            'repl_in_port': repl_in_port,  # Int
            'repl_out_port': repl_out_port,  # Int
            'stdin_port': stdin_port,    # legacy, Int
            'stdout_port': stdout_port,  # legacy, Int
            'service_ports': service_ports,  # JSON
            'host_ports': list(exposed_port_results.values()),  # List[Int]
            'resource_spec': resource_spec.write_to_string(),  # JSON
        }))
        try:
            await ctx.k8sCoreApi.create_namespaced_config_map('backend-ai', body=registry_cm.as_dict())
        except:
            raise K8sError('Registry ConfigMap not saved')
        return kernel_obj

    async def destroy_kernel(self, _kernel_id: KernelId, reason: str) \
            -> Optional[Mapping[MetricKey, MetricValue]]:

        kernel_id = str(_kernel_id)
        # Load K8s API object
        await K8sConfig.load_kube_config()
        k8sCoreApi = K8sClient.CoreV1Api()
        k8sAppsApi = K8sClient.AppsV1Api()

        async def force_cleanup(reason='self-terminated'):
            await self.produce_event('kernel_terminated',
                                    kernel_id, 'self-terminated',
                                    None)
            try:
                del self.kernel_registry[_kernel_id]
            except KeyError:
                pass
        try:
            kernel = self.kernel_registry[_kernel_id]
        except:
            log.warning('_destroy_kernel({0}) kernel missing (already dead?)',
                        kernel_id)

            await asyncio.shield(force_cleanup())
            return None
        deployment_name = kernel.kernel_id

        try:
            log.debug('Trying to delete service repl-{0}', kernel_id)
            await k8sCoreApi.delete_namespaced_service(f'repl-{kernel_id}', 'backend-ai')
        except:
            log.warning('_destroy({0}) repl service missing (already dead?)', kernel_id)
        try:
            await k8sCoreApi.delete_namespaced_service(f'expose-{kernel_id}', 'backend-ai')
        except:
            log.warning('_destroy({0}) expose service missing (already dead?)', kernel_id)
        try:
            await k8sCoreApi.delete_namespaced_config_map(f'{kernel_id}-configmap', 'backend-ai')
        except:
            log.warning('_destroy({0}) configmap missing (already dead?)', kernel_id)
        try:
            await k8sCoreApi.delete_namespaced_config_map(f'{kernel_id}-registry', 'backend-ai')
        except:
            log.warning('_destroy({0}) configmap missing (already dead?)', kernel_id)
        try:
            await k8sAppsApi.delete_namespaced_deployment(f'{deployment_name}', 'backend-ai')
        except:
            log.warning('_destroy({0}) kernel missing (already dead?)', kernel_id)

        await self.kernel_registry[_kernel_id].runner.close()

        await force_cleanup(reason=reason)
        return None

    async def clean_kernel(self, kernel_id: KernelId) -> None:
        kernel_obj = self.kernel_registry[kernel_id]
        await kernel_obj.runner.close()
        await kernel_obj.close()
        del self.kernel_registry[kernel_id]
        self.blocking_cleans[kernel_id].set()

    async def clean_all_kernels(self, blocking=False):
        log.info('cleaning all kernels...')
        kernel_ids = tuple(self.kernel_registry.keys())
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
