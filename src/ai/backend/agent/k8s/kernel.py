import asyncio
import logging
import lzma
import os
from pathlib import Path
import pkg_resources
import platform
import re
import shutil
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
            agent_config=config,
            resource_spec=resource_spec,
            service_ports=service_ports,
            data=data)

    async def __ainit__(self) -> None:
        await super().__ainit__()

    async def close(self) -> None:
        pass

    async def scale(self, num: int):
        await K8sConfig.load_kube_config()
        k8sAppsApi = K8sClient.AppsV1Api()
        scale_result = await k8sAppsApi.replace_namespaced_deployment_scale(
            self.kernel_id, 'backend-ai',
            body={
                'apiVersion': 'autoscaling/v1',
                'kind': 'Scale',
                'metadata': {
                    'name': self.kernel_id,
                    'namespace': 'backend-ai',
                },
                'spec': {'replicas': num},
                'status': {'replicas': num, 'selector': f'run={self.kernel_id}'}
            }
        )

        if scale_result.to_dict()['spec']['replicas'] == 0:
            raise K8sError('Deployment scaling failed: ' + str(scale_result))

        while not await self.is_scaled():
            asyncio.sleep(0.5)

        log.debug('Session scaled to 1')


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

    async def get_service_apps(self):
        result = await self.runner.feed_service_apps()
        return result

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
            'backend-ai', label_selector=f'run={self.kernel_id}'
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
            'port': sport['container_ports'][0],
            'protocol': sport['protocol'],
            'options': opts,
        })
        return result

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


async def prepare_runner_files(mount_path_str: str):
    runner_path = Path(pkg_resources.resource_filename(
        'ai.backend.agent', f'../runner')
    )
    mount_path = Path(mount_path_str)
    
    runner_files = [
        'jupyter-custom.css', 'logo.svg',
        'roboto.ttf', 'roboto-italic.ttf',
        'entrypoint.sh', '.bashrc', '.vimrc'
    ]
    runner_files += [path.name for path in runner_path.glob('*.bin')]
    runner_files += [path.name for path in runner_path.glob('*.so')]

    kernel_pkg_path = Path(pkg_resources.resource_filename(
        'ai.backend.agent', '../kernel'))
    helpers_pkg_path = Path(pkg_resources.resource_filename(
        'ai.backend.agent', '../helpers'))

    for filename in runner_files:
        if (mount_path / filename).exists():
            os.remove(mount_path / filename)
        shutil.copyfile(runner_path / filename, mount_path / filename)
        (mount_path / filename).chmod(0o755)
    
    if (mount_path / 'kernel').exists():
        shutil.rmtree(mount_path / 'kernel')
    if (mount_path / 'helpers').exists():
        shutil.rmtree(mount_path / 'helpers')
    shutil.copytree(kernel_pkg_path, mount_path / 'kernel')
    shutil.copytree(helpers_pkg_path, mount_path / 'helpers')


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

        krunner_path = Path(mount_path) / volume_name
        do_create = not (krunner_path.exists() and os.path.isdir(krunner_path.absolute().as_posix()))
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
