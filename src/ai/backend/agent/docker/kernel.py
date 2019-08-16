import asyncio
import logging
import lzma
from pathlib import Path
import pkg_resources
import platform
import re
import time

from aiodocker.docker import Docker, DockerVolume
from aiodocker.exceptions import DockerError
import zmq

from ai.backend.common.logging import BraceStyleAdapter
from ..kernel import AbstractKernelRunner

log = BraceStyleAdapter(logging.getLogger(__name__))


class KernelRunner(AbstractKernelRunner):
    def __init__(self, kernel_id,
                    kernel_host, repl_in_port, repl_out_port,
                    exec_timeout, client_features=None):
        super().__init__(repl_in_port, repl_out_port, exec_timeout, client_features=client_features)
        self.kernel_id = kernel_id
        self.kernel_host = kernel_host

    async def start(self):
        self.started_at = time.monotonic()

        self.input_sock = self.zctx.socket(zmq.PUSH)
        self.input_sock.connect(f'tcp://{self.kernel_host}:{self.repl_in_port}')
        self.input_sock.setsockopt(zmq.LINGER, 50)

        self.output_sock = self.zctx.socket(zmq.PULL)
        self.output_sock.connect(f'tcp://{self.kernel_host}:{self.repl_out_port}')
        self.output_sock.setsockopt(zmq.LINGER, 50)

        self.read_task = asyncio.ensure_future(self.read_output())
        if self.exec_timeout > 0:
            self.watchdog_task = asyncio.ensure_future(self.watchdog())
        else:
            self.watchdog_task = None


async def prepare_krunner_env(distro: str):
    # TODO: make all subprocess calls asynchronous and run this function in parallel for all distro.
    '''
    Check if the volume "backendai-krunner.{distro}.{arch}" exists and is up-to-date.
    If not, automatically create it and update its content from the packaged pre-built krunner tar
    archives.
    '''
    distro_name = re.search(r'^([a-z]+)\d+\.\d+$', distro).group(1)
    docker = Docker()
    arch = platform.machine()
    name = f'backendai-krunner.{distro}'
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
            with lzma.open(extractor_archive, 'rb') as extractor_img:
                proc = await asyncio.create_subprocess_exec(*['docker', 'load'], stdin=extractor_img)
                if (await proc.wait() != 0):
                    raise RuntimeError('loading krunner extractor image has failed!')

        log.info('checking krunner-env for {}...', distro)
        try:
            vol = DockerVolume(docker, name)
            await vol.show()
        except DockerError as e:
            if e.status == 404:
                # create volume
                await docker.volumes.create({
                    'Name': name,
                    'Driver': 'local',
                })
        proc = await asyncio.create_subprocess_exec(*[
            'docker', 'run', '--rm', '-i',
            '-v', f'{name}:/root/volume',
            extractor_image,
            'sh', '-c', 'cat /root/volume/VERSION 2>/dev/null || echo 0',
        ], stdout=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        if (await proc.wait() != 0):
            raise RuntimeError('checking krunner environment version has failed!')
        existing_version = int(stdout.decode().strip())
        current_version = int(Path(
            pkg_resources.resource_filename(
                f'ai.backend.krunner.{distro_name}',
                f'./krunner-version.{distro}.txt'))
            .read_text().strip())
        if existing_version < current_version:
            log.info('updating {} volume from version {} to {}',
                     name, existing_version, current_version)
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
                '-v', f'{name}:/root/volume',
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
    return name
