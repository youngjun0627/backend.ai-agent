import asyncio
import logging
import lzma
from pathlib import Path
import pkg_resources
import platform
import re
from typing import (
    Any,
    Dict, Mapping,
    FrozenSet,
)

from aiodocker.docker import Docker, DockerVolume
from aiodocker.exceptions import DockerError
from kubernetes_asyncio import client as K8sClient, config as K8sConfig

from ai.backend.common.docker import ImageRef
from ai.backend.common.logging import BraceStyleAdapter
from ..exception import K8sError
from ..resources import KernelResourceSpec
from ..kernel import AbstractKernel, AbstractCodeRunner

log = BraceStyleAdapter(logging.getLogger(__name__))


class K8sKernel(AbstractKernel):

    def __init__(self, deployment_name: str, image: ImageRef, version: int, *,
                 config: Mapping[str, Any],
                 resource_spec: KernelResourceSpec,
                 service_ports: Any,  # TODO: type-annotation
                 data: Dict[str, Any]) -> None:
        super().__init__(
            deployment_name, image, version,
            config=config,
            resource_spec=resource_spec,
            service_ports=service_ports,
            data=data)

    async def __ainit__(self) -> None:
        await super().__ainit__()

    async def close(self) -> None:
        pass

    async def create_code_runner(self, *,
                           client_features: FrozenSet[str],
                           api_version: int) -> AbstractCodeRunner:
        return await K8sCodeRunner.new(
            self.kernel_id,
            kernel_host=self.data['kernel_host'],
            repl_in_port=self.data['repl_in_port'],
            repl_out_port=self.data['repl_out_port'],
            exec_timeout=0,
            client_features=client_features)

    async def get_completions(self, text: str, opts: Mapping[str, Any]):
        result = await self.runner.feed_and_get_completion(text, opts)
        return {'status': 'finished', 'completions': result}

    async def check_status(self):
        # TODO: Implement
        result = await self.runner.feed_and_get_status()
        return result

    async def get_logs(self, kernel_id):
        await K8sConfig.load_kube_config()
        k8sCoreApi = K8sClient.CoreV1Api()

        if self.runner is None or not await self.runner.is_scaled():
            return {'logs': ''}

        pods = await k8sCoreApi.list_namespaced_pod(
            'backend-ai', label_selector=f'run={self.deployment_name}'
        )
        pod_name = pods.items[0].metadata.name
        logs = await k8sCoreApi.read_namespaced_pod_log(pod_name, 'backend-ai')
        return {'logs': logs}

    async def interrupt_kernel(self):
        await self.runner.feed_interrupt()
        return {'status': 'finished'}

    async def start_service(self, service: str, opts: Mapping[str, Any]):
        for sport in self.service_ports:
            if sport['name'] == service:
                break
        else:
            return {'status': 'failed', 'error': 'invalid service name'}
        result = await self.runner.feed_start_service({
            'name': service,
            'port': sport['container_port'],
            'protocol': sport['protocol'],
            'options': opts,
        })
        return result

    async def accept_file(self, filename: str, filedata: bytes):
        raise K8sError('Not implemented for Kubernetes')

    async def download_file(self, filepath: str):
        raise K8sError('Not implemented for Kubernetes')

    async def list_files(self, container_path: str):
        raise K8sError('Not implemented for Kubernetes')


class K8sCodeRunner(AbstractCodeRunner):
    kernel_host: str
    repl_in_port: int
    repl_out_port: int

    def __init__(self, deployment_name, *,
                 kernel_host, repl_in_port, repl_out_port,
                 exec_timeout=0, client_features=None) -> None:
        super().__init__(
            deployment_name,
            exec_timeout=exec_timeout,
            client_features=client_features)
        self.kernel_host = kernel_host
        self.repl_in_port = repl_in_port
        self.repl_out_port = repl_out_port

    async def get_repl_in_addr(self) -> str:
        return f'tcp://{self.kernel_host}:{self.repl_in_port}'

    async def get_repl_out_addr(self) -> str:
        return f'tcp://{self.kernel_host}:{self.repl_out_port}'

    async def is_scaled(self):
        await K8sConfig.load_kube_config()
        k8sAppsApi = K8sClient.AppsV1Api()
        scale = await k8sAppsApi.read_namespaced_deployment(self.kernel_id, 'backend-ai')
        if scale.to_dict()['status']['replicas'] == 0:
            return False
        for condition in scale.to_dict()['status']['conditions']:
            if not condition['status']:
                return False

        return True


async def prepare_krunner_env(distro: str, mount_path: str):
    '''
    Check if the volume "backendai-krunner.{distro}.{arch}" exists and is up-to-date.
    If not, automatically create it and update its content from the packaged pre-built krunner tar
    archives.
    '''
    m = re.search(r'^([a-z]+)\d+\.\d+$', distro)
    if m is None:
        raise ValueError('Unrecognized "distro[version]" format string.')
    distro_name = m.group(1)
    docker = Docker()
    arch = platform.machine()
    current_version = int(Path(
        pkg_resources.resource_filename(
            f'ai.backend.krunner.{distro_name}',
            f'./krunner-version.{distro}.txt'))
        .read_text().strip())
    volume_name = f'backendai-krunner.v{current_version}.{distro}'
    extractor_image = 'backendai-krunner-extractor:latest'

    try:
        for item in (await docker.images.list()):
            if item['RepoTags'] is None:
                continue
            if item['RepoTags'][0] == extractor_image:
                break
        else:
            log.info('preparing the Docker image for krunner extractor...')
            extractor_archive = pkg_resources.resource_filename(
                'ai.backend.agent', '../runner/krunner-extractor.img.tar.xz')
            with lzma.open(extractor_archive, 'rb') as reader:
                proc = await asyncio.create_subprocess_exec(
                    *['docker', 'load'], stdin=reader)
                if (await proc.wait() != 0):
                    raise RuntimeError('loading krunner extractor image has failed!')

        log.info('checking krunner-env for {}...', distro)
        do_create = False
        try:
            vol = DockerVolume(docker, volume_name)
            await vol.show()
        except DockerError as e:
            if e.status == 404:
                do_create = True
        if do_create:
            log.info('populating {} volume version {}',
                     volume_name, current_version)
            await docker.volumes.create({
                'Name': volume_name,
                'Driver': 'local',
            })
            archive_path = Path(pkg_resources.resource_filename(
                f'ai.backend.krunner.{distro_name}',
                f'./krunner-env.{distro}.{arch}.tar.xz')).resolve()
            extractor_path = Path(pkg_resources.resource_filename(
                'ai.backend.agent',
                f'../runner/krunner-extractor.sh')).resolve()
            proc = await asyncio.create_subprocess_exec(*[
                'docker', 'run', '--rm', '-i',
                '-v', f'{archive_path}:/root/archive.tar.xz',
                '-v', f'{extractor_path}:/root/krunner-extractor.sh',
                '-v', f'{mount_path}/{volume_name}:/root/volume',
                '-e', f'KRUNNER_VERSION={current_version}',
                extractor_image,
                '/root/krunner-extractor.sh',
            ])
            if (await proc.wait() != 0):
                raise RuntimeError('extracting krunner environment has failed!')
    except Exception:
        log.exception('unexpected error')
    finally:
        await docker.close()
    return volume_name
