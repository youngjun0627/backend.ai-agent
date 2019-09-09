import asyncio
import configparser
import base64
import datetime
from decimal import Decimal
import json
import logging, logging.config
import os
from pathlib import Path
import platform
from pprint import pformat
import random
import time
from typing import (
    Any, Dict, List, 
    Optional, Mapping,
    Tuple, Type,
)

import aiohttp
import attr
import boto3
from kubernetes_asyncio import client as K8sClient, config as K8sConfig
import pytz
import snappy

from ai.backend.common import msgpack
from ai.backend.common.docker import ImageRef
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    DeviceName,
    BinarySize,
    KernelCreationConfig,
    KernelCreationResult,
    KernelId,
    MetricKey, MetricValue,
    MountPermission,
    ResourceSlot,
    SlotName,
)
from .kernel import K8sKernel
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
    detect_resources,
)
from .. import __version__ as VERSION
from ..agent import AbstractAgent
from ..exception import InsufficientResource, K8sError
from ..resources import KernelResourceSpec
from ..kernel import KernelFeatures

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.server'))


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
        'container_port': port,
        'host_port': None,  # determined after container start
    }


class K8sAgent(AbstractAgent):
    vfolder_as_pvc: bool
    ecr_address: str
    ecr_token: str
    ecr_token_expireAt: float
    k8s_images: List[str]
    workers: dict

    def __init__(self, config) -> None:
        super().__init__(config)

    async def __ainit__(self) -> None:
        self.vfolder_as_pvc = False
        self.ecr_address = ''
        self.ecr_token = ''
        self.ecr_token_expireAt = -1
        self.k8s_images = []

        self.workers = {}
        await K8sConfig.load_kube_config()
        await self.fetch_workers()

        if 'vfolder-pv' in self.config.keys():
            await self.ensure_vfolder_pv()

        if self.config['registry']['type'] == 'ecr':
            await self.get_aws_credentials()

        await self.check_krunner_pv_status()

    @staticmethod
    async def detect_resources(resource_configs: Mapping[str, Any],
                               plugin_configs: Mapping[str, Any]) \
                               -> Tuple[
                                   Mapping[DeviceName, Type[AbstractComputePlugin]],
                                   Mapping[SlotName, Decimal]
                               ]:
        return await detect_resources(resource_configs)

    async def get_aws_credentials(self) -> None:
        region = configparser.ConfigParser()
        credentials = configparser.ConfigParser()

        aws_path = Path.home() / Path('.aws')
        region.read_string((aws_path / Path('config')).read_text())
        credentials.read_string((aws_path / Path('credentials')).read_text())

        profile = self.config['registry']['profile']
        self.config['registry']['region'] = region[profile]['region']
        self.config['registry']['access-key'] = credentials[profile]['aws_access_key_id']
        self.config['registry']['secret-key'] = credentials[profile]['aws_secret_access_key']

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
            if 'node-role.kubernetes.io/master' in node.metadata.labels.keys():
                continue
            self.workers[node.metadata.name] = node.status.capacity
            for addr in node.status.addresses:
                if addr.type == 'InternalIP':
                    self.workers[node.metadata.name]['InternalIP'] = addr.address
                if addr.type == 'ExternalIP':
                    self.workers[node.metadata.name]['ExternalIP'] = addr.address

    async def get_docker_registry_info(self) -> Tuple[str, dict]:
        await K8sConfig.load_kube_config()
        k8sCoreApi = K8sClient.CoreV1Api()
        # Determines Registry URL and its appropriate Authorization header
        if self.config['registry']['type'] == 'local':
            addr, port = self.config['registry']['addr'].as_sockaddr()
            registry_addr = f'https://{addr}:{port}'
            registry_pull_secrets = await k8sCoreApi.list_namespaced_secret('backend-ai')
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
                    return registry_addr, {'Authorization': f'Basic {token}'}
            raise K8sError(
                'No authentication information for registry server configured in agent.toml'
            )
        elif self.config['registry']['type'] == 'ecr':
            # Check if given token is valid yet
            utc = pytz.UTC
            if self.ecr_address and \
                    self.ecr_token and self.ecr_token_expireAt and \
                    utc.localize(datetime.datetime.now()) < \
                        utc.localize(datetime.datetime.utcfromtimestamp(self.ecr_token_expireAt)):
                registry_addr, headers = self.ecr_address, {'Authorization': 'Basic ' + self.ecr_token}
                return registry_addr, headers

            client = boto3.client(
                'ecr',
                aws_access_key_id=self.config['registry']['access-key'],
                aws_secret_access_key=self.config['registry']['secret-key']
            )
            auth_data = client.get_authorization_token(
                registryIds=[self.config['registry']['registry-id']]
            )
            self.ecr_address = auth_data['authorizationData'][0]['proxyEndpoint']
            self.ecr_token = auth_data['authorizationData'][0]['authorizationToken']
            self.ecr_token_expireAt = auth_data['authorizationData'][0]['expiresAt']
            log.debug('ECR Access Token expires at: {0}', self.ecr_token_expireAt)

            return self.ecr_address, {
                'Authorization': 'Basic ' + self.ecr_token,
                'Content-Type': 'application/json'
            }
        return '', {}

    async def scan_running_kernels(self) -> None:
        await K8sConfig.load_kube_config()
        k8sCoreApi = K8sClient.CoreV1Api()
        k8sAppsApi = K8sClient.AppsV1Api()

        deployments = await k8sAppsApi.list_namespaced_deployment(
            'backend-ai', label_selector='backend.ai/kernel'
        )

        for deployment in deployments.items:
            kernel_id = deployment.metadata.labels['backend.ai/kernel_id']

            registry_cm = await k8sCoreApi.read_namespaced_config_map(
                f'{kernel_id}-registry', 'backend-ai'
            )
            registry = json.loads(registry_cm.data['registry'])
            registry['deployment_name'] = deployment.metadata.name
            registry['lang'] = ImageRef(registry['lang']['canonical'], registry['lang']['registry'])
            registry['kernel_host'] = random.choice([x['InternalIP'] for x in self.workers.values()])
            registry['last_used'] = time.monotonic()
            registry['runner_tasks'] = set()
            registry['resource_spec'] = KernelResourceSpec.read_from_string(registry['resource_spec'])
            registry['agent_tasks'] = []

            self.kernel_registry[kernel_id] = registry
            log.info('Restored kernel {0} from K8s', kernel_id)

    async def scan_images(self, interval: float = None) -> None:
        updated_images: List[str] = []
        registry_addr, headers = await self.get_docker_registry_info()

        async with aiohttp.ClientSession(
            headers=headers, connector=aiohttp.TCPConnector(verify_ssl=False)
        ) as session:
            all_images_response = await session.get(f'{registry_addr}/v2/_catalog')
            status_code = all_images_response.status
            log.debug('/v2/_catalog/: {0}', await all_images_response.text())
            all_images = json.loads(await all_images_response.text())
            if status_code == 401:
                raise K8sError('\n'.join([x['message'] for x in all_images['errors']]))

            for image_name in all_images['repositories']:
                image_tags_response = await session.get(f'{registry_addr}/v2/{image_name}/tags/list')
                image_tags = json.loads(await image_tags_response.text())
                updated_images += [f'{image_name}:{x}' for x in image_tags['tags']]

        for added_image in list(set(updated_images) - set(self.k8s_images)):
            log.debug('found kernel image: {0}', added_image)
        for removed_image in list(set(self.k8s_images) - set(updated_images)):
            log.debug('removed kernel image: {0}', removed_image)
        self.k8s_images = updated_images

    async def heartbeat(self, interval: float) -> None:
        '''
        Send my status information and available kernel images.
        '''
        res_slots = {}

        for cctx in self.computers.values():
            for slot_key, slot_type in cctx.klass.slot_types:
                res_slots[slot_key] = (slot_type, str(self.slots.get(slot_key, 0)))

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
                (x, 'K8S_AGENT') for x in self.k8s_images
            ]))
        }

        try:
            await self.send_event('instance_heartbeat', agent_info)
        except asyncio.TimeoutError:
            log.warning('event dispatch timeout: instance_heartbeat')
        except Exception:
            log.exception('instance_heartbeat failure')
            self.error_monitor.capture_exception()

    async def create_kernel(self, kernel_id: KernelId, kernel_config: KernelCreationConfig, *,
                            restarting: bool = False) -> KernelCreationResult:

        # Load K8s API object
        await K8sConfig.load_kube_config()
        k8sCoreApi = K8sClient.CoreV1Api()
        k8sAppsApi = K8sClient.AppsV1Api()
        await self.send_event('kernel_preparing', kernel_id)
        log.debug(
            'create_kernel: kernel_config -> {0}',
            json.dumps(kernel_config, ensure_ascii=False, indent=2)
        )
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
        work_dir = scratch_dir / 'work'

        slots = ResourceSlot.from_json(kernel_config['resource_slots'])
        vfolders = kernel_config['mounts']
        resource_spec = KernelResourceSpec(
            container_id='K8sDUMMYCID',
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

        resource_spec.allocations = {}

        for dev_type in dev_types:
            computer_set = self.computers[dev_type]
            # {'cpu': 2 }
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

        # Inject Backend.AI-intrinsic env-variables for libbaihook and gosu
        cpu_core_count = len(resource_spec.allocations['cpu']['cpu'])
        environ.update({k: str(cpu_core_count) for k in envs_corecount})

        # Inject Backend.AI kernel runner dependencies.
        distro = image_labels.get('ai.backend.base-distro', 'ubuntu16.04')
        arch = platform.machine()

        # Add private registry information
        if self.config['registry']['type'] == 'local':
            registry_addr, registry_port = self.config['registry']['addr']
            registry_name = f'{registry_addr}:{registry_port}'
        elif self.config['registry']['type'] == 'ecr':
            registry_name = self.ecr_address.replace('https://', '')

        canonical = kernel_config['image']['canonical']

        # Create deployment object with given image name
        _, repo, name_with_tag = canonical.split('/')
        if self.config['registry']['type'] != 'local':
            deployment = KernelDeployment(
                kernel_id, f'{registry_name}/{repo}/{name_with_tag}',
                distro, arch, 'cuda' in self.computers.keys(),
                ecr_url=registry_name,
                name=f"kernel-{image_ref.name.split('/')[-1]}-{kernel_id}".replace('.', '-')
            )
        else:
            deployment = KernelDeployment(
                kernel_id, f'{registry_name}/{repo}/{name_with_tag}',
                distro, arch, 'cuda' in self.computers.keys(),
                name=f"kernel-{image_ref.name.split('/')[-1]}-{kernel_id}".replace('.', '-')
            )

        if 'cuda.device' in slots.keys():
            deployment.set_gpus(slots['cuda.device'])

        log.debug('Initial container config: {0}', deployment.as_dict())

        def _mount(kernel_id: str, hostPath: str, mountPath: str, mountType: str, perm='ro'):
            name = (kernel_id + '-' + mountPath.split('/')[-1]).replace('.', '-')
            deployment.mount_hostpath(HostPathMountSpec(name, hostPath, mountPath, mountType, perm))

        # Check if NFS PVC for static files exists and bound
        nfs_pvc = await k8sCoreApi.list_namespaced_persistent_volume_claim(
            'backend-ai', label_selector='backend.ai/bai-static-nfs-server'
        )
        if len(nfs_pvc.items) == 0:
            raise K8sError('No PVC for backend.ai static files')
        pvc = nfs_pvc.items[0]
        if pvc.status.phase != 'Bound':
            raise K8sError('PVC not Bound')

        krunner_volume = self.config['container']['krunner-volumes'].get(distro)
        if krunner_volume is None:
            raise RuntimeError(f'Cannot run container based on {distro}')

        deployment.baistatic_pvc = pvc.metadata.name

        # Mount essential files
        deployment.mount_pvc(
            PVCMountSpec('entrypoint.sh', '/opt/kernel/entrypoint.sh', 'File')
        )
        deployment.mount_pvc(
            PVCMountSpec(f'su-exec.{distro}.bin', '/opt/kernel/su-exec', 'File')
        )
        deployment.mount_pvc(
            PVCMountSpec(f'jail.{distro}.bin', '/opt/kernel/jail', 'File')
        )
        deployment.mount_pvc(
            PVCMountSpec(f'libbaihook.{distro}.{arch}.so', '/opt/kernel/libbaihook.so', 'File')
        )
        deployment.mount_pvc(
            PVCMountSpec(
                'kernel', '/opt/backend.ai/lib/python3.6/site-packages/ai/backend/kernel', 'Directory'
            )
        )
        deployment.mount_pvc(
            PVCMountSpec(
                'helpers', '/opt/backend.ai/lib/python3.6/site-packages/ai/backend/helpers', 'Directory'
            )
        )
        deployment.mount_pvc(
            PVCMountSpec('jupyter-custom.css', '/home/work/.jupyter/custom/custom.css', 'File')
        )
        deployment.mount_pvc(
            PVCMountSpec('logo.svg', '/home/work/.jupyter/custom/logo.svg', 'File')
        )
        deployment.mount_pvc(
            PVCMountSpec('roboto.ttf', '/home/work/.jupyter/custom/roboto.ttf', 'File')
        )
        deployment.mount_pvc(
            PVCMountSpec('roboto-italic.ttf', '/home/work/.jupyter/custom/roboto-italic.ttf', 'File')
        )

        # If agent is using vFolder by NFS PVC, check if vFolder PVC exists and Bound
        # # otherwise raise since we can't use vfolder
        if len(vfolders) > 0 and self.vfolder_as_pvc:
            try:
                nfs_pvc = await k8sCoreApi.list_namespaced_persistent_volume_claim(
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
                deployment.mount_vfolder_pvc(
                    PVCMountSpec(str(subPath), f'/home/work/{folder_name}', 'Directory', perm=perm_char)
                )
            else:
                host_path = (self.config['vfolder']['mount'] / folder_host /
                    self.config['vfolder']['fsprefix'] / folder_id)
                _mount(
                    kernel_id, host_path.absolute().as_posix(),
                    f'/home/work/{folder_name}', 'Directory', perm=perm_char
                )

        # should no longer be used!
        del vfolders

        environ['LD_PRELOAD'] = '/opt/kernel/libbaihook.so'

        # TODO: Inject ComputeDevice-specific env-varibles and hooks

        # PHASE 3: Store the resource spec as plain text, and store it to ConfigMap later.

        resource_txt = resource_spec.write_to_string()
        for dev_type, device_alloc in resource_spec.allocations.items():
            computer_set = self.computers[dev_type]
            kvpairs = \
                await computer_set.klass.generate_resource_data(device_alloc)
            for k, v in kvpairs.items():
                resource_txt += f'{k}={v}\n'

        environ_txt = ''

        for k, v in environ.items():
            environ_txt += f'{k}={v}\n'

        # PHASE 4: Run!
        log.info('kernel {0} starting with resource spec: \n',
                 pformat(attr.asdict(resource_spec)))

        exposed_ports = []
        service_ports = {}

        # Extract port expose information from kernel creation request
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

        # Prepare ConfigMap to store resource spec
        configmap = ConfigMap(kernel_id, 'configmap')
        configmap.put('environ', environ_txt)
        configmap.put('resource', resource_txt)

        # Set up appropriate arguments for kernel image
        deployment.cmd = cmdargs
        deployment.env = environ
        deployment.mount_configmap(
            ConfigMapMountSpec('environ', configmap.name, 'environ', '/home/config/environ.txt')
        )
        deployment.mount_configmap(
            ConfigMapMountSpec('resource', configmap.name, 'resource', '/home/config/resource.txt')
        )
        deployment.ports = exposed_ports

        # Create K8s service object to expose REPL port
        repl_service = Service(kernel_id, 'repl', deployment.name,
            [(2000, 'repl-in'), (2001, 'repl-out')], service_type='NodePort')
        # Create K8s service object to expose Service port
        expose_service = Service(
            kernel_id, 'expose', deployment.name,
            [(port['container_port'], kernel_id + '-' + port['name'])
                for port in service_ports.values()],
            service_type='NodePort'
        )

        deployment.label('backend.ai/kernel', '')

        log.debug('container config: {!r}', deployment.as_dict())
        log.debug('nodeport config: {!r}', expose_service.as_dict())
        # We are all set! Create and start the container.

        # TODO: Find appropriate model import for V1ServicePort
        node_ports: List[Any] = [] 

        # Send request to K8s API
        if len(exposed_ports) > 0:
            try:
                exposed_service_api_response = await k8sCoreApi.create_namespaced_service(
                    'backend-ai', body=expose_service.as_dict()
                )
                node_ports = exposed_service_api_response.spec.ports
            except:
                raise
        try:
            repl_service_api_response = await k8sCoreApi.create_namespaced_service(
                'backend-ai', body=repl_service.as_dict()
            )
        except:
            await k8sCoreApi.delete_namespaced_service(expose_service.name, 'backend-ai')
            raise
        try:
            await k8sCoreApi.create_namespaced_config_map(
                'backend-ai', body=configmap.as_dict(), pretty='pretty_example'
            )
        except:
            await k8sCoreApi.delete_namespaced_service(repl_service.name, 'backend-ai')
            await k8sCoreApi.delete_namespaced_service(expose_service.name, 'backend-ai')
            raise
        try:
            await k8sAppsApi.create_namespaced_deployment(
                'backend-ai', body=deployment.as_dict(), pretty='pretty_example'
            )
        except:
            await k8sCoreApi.delete_namespaced_service(repl_service.name, 'backend-ai')
            await k8sCoreApi.delete_namespaced_service(expose_service.name, 'backend-ai')
            await k8sCoreApi.delete_namespaced_config_map(configmap.name, 'backend-ai')
            raise

        stdin_port = 0
        stdout_port = 0
        repl_in_port = 0
        repl_out_port = 0

        exposed_port_results: Dict[str, int] = {}

        # Randomly select IP from all available worker nodes
        # When service is exposed via NodePort, we do not have to make request only to the worker node
        # where pod is started
        target_node_ip = random.choice([x['InternalIP'] for x in self.workers.values()])

        for port in node_ports:
            exposed_port_results[str(port.port)] = port.node_port

        for k in service_ports.keys():
            service_ports[k]['host_port'] = exposed_port_results[k]

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
            await self._destroy_kernel(kernel_id, 'nodeport-assign-error')
            raise K8sError('REPL in port not assigned')
        if repl_out_port == 0:
            await self._destroy_kernel(kernel_id, 'elb-assign-error')
            raise K8sError('REPL out port not assigned')

        log.debug('Exposed ports: {0}', exposed_port_results)
        log.debug('Service ports: {0}', service_ports)

        self.kernel_registry[kernel_id] = await K8sKernel.new(
            deployment.name,
            image_ref,
            version,
            self.config,
            resource_spec,
            list(service_ports.values()),
            data={
                'kernel_host': target_node_ip,  # IP or FQDN
                'repl_in_port': repl_in_port,  # Int
                'repl_out_port': repl_out_port,  # Int
                'stdin_port': stdin_port,    # legacy, Int
                'stdout_port': stdout_port,  # legacy, Int
                'host_ports': exposed_port_results.values(),  # List[Int]
            }
        )

        registry_cm = ConfigMap(kernel_id, 'registry')
        registry_cm.put('registry', json.dumps({
            'lang': {
                'canonical': kernel_config['image']['canonical'],
                'registry': [kernel_config['image']['registry']['name']]
            },
            'version': version,  # Int
            'repl_in_port': repl_in_port,  # Int
            'repl_out_port': repl_out_port,  # Int
            'stdin_port': stdin_port,    # legacy, Int
            'stdout_port': stdout_port,  # legacy, Int
            'service_ports': list(service_ports.values()),  # JSON
            'host_ports': list(exposed_port_results.values()),  # List[Int]
            'resource_spec': resource_spec.write_to_string(),  # JSON
        }))

        try:
            await k8sCoreApi.create_namespaced_config_map('backend-ai', body=registry_cm.as_dict())
        except:
            raise K8sError('Registry ConfigMap not saved')

        log.debug('kernel repl-in address: {0}:{1}', target_node_ip, repl_in_port)
        log.debug('kernel repl-out address: {0}:{1}', target_node_ip, repl_out_port)
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

    async def destroy_kernel(self, kernel_id: KernelId, reason: str) \
            -> Optional[Mapping[MetricKey, MetricValue]]:

        # Load K8s API object
        await K8sConfig.load_kube_config()
        k8sCoreApi = K8sClient.CoreV1Api()
        k8sAppsApi = K8sClient.AppsV1Api()

        async def force_cleanup(reason='self-terminated'):
            await self.send_event('kernel_terminated',
                                    kernel_id, 'self-terminated',
                                    None)
        try:
            kernel = self.kernel_registry[kernel_id]
        except:
            log.warning('_destroy_kernel({0}) kernel missing (already dead?)',
                        kernel_id)

            await asyncio.shield(force_cleanup())
            return None
        deployment_name = kernel['deployment_name']

        try:
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

        del self.kernel_registry[kernel_id]

        await force_cleanup(reason=reason)
        return None

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
