import asyncio
import json
import logging
import lzma
from pathlib import Path
import pkg_resources
import platform
import re
import subprocess
import textwrap
import time
from typing import Any, Mapping, Set

from aiodocker.docker import Docker, DockerVolume
from aiodocker.exceptions import DockerError
import zmq

from ai.backend.common.docker import ImageRef
from ai.backend.common.logging import BraceStyleAdapter
from ..resources import KernelResourceSpec
from ..kernel import AbstractKernel, AbstractCodeRunner, ClientFeatures
from ..utils import current_loop

log = BraceStyleAdapter(logging.getLogger(__name__))


class DockerKernel(AbstractKernel):

    def __init__(self, kernel_id: str, image: ImageRef, version: str, *,
                 resource_spec: KernelResourceSpec,
                 service_ports: Any,  # TODO: type-annotation
                 data: Mapping[str, Any]):
        super().__init__(
            kernel_id, image, version,
            resource_spec=resource_spec,
            service_ports=service_ports,
            data=data)
        self._docker = Docker()

    async def close(self):
        await self._docker.close()

    def create_code_runner(self, *,
                           client_features: Set[ClientFeatures],
                           api_version: int):
        return DockerCodeRunner(
            self.kernel_id,
            self.data['kernel_host'],
            self.data['repl_in_port'],
            self.data['repl_out_port'],
            0,
            client_features)

    async def get_completions(self, text, opts):
        await self.ensure_runner()
        result = await self.runner.feed_and_get_completion(text, opts)
        return {'status': 'finished', 'completions': result}

    async def get_logs(self):
        container_id = self.data['container_id']
        container = await self._docker.containers.get(container_id)
        logs = await container.log(stdout=True, stderr=True)
        return {'logs': ''.join(logs)}

    async def interrupt_kernel(self):
        await self.ensure_runner()
        await self.runner.feed_interrupt()
        return {'status': 'finished'}

    async def start_service(self, service, opts):
        await self.ensure_runner()
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

    async def accept_file(self, filename, filedata):
        loop = current_loop()
        work_dir = self.config['container']['scratch-root'] / self.kernel_id / 'work'
        try:
            # create intermediate directories in the path
            dest_path = (work_dir / filename).resolve(strict=False)
            parent_path = dest_path.parent
        except ValueError:  # parent_path does not start with work_dir!
            raise AssertionError('malformed upload filename and path.')

        def _write_to_disk():
            parent_path.mkdir(parents=True, exist_ok=True)
            dest_path.write_bytes(filedata)

        try:
            await loop.run_in_executor(None, _write_to_disk)
        except FileNotFoundError:
            log.error('{0}: writing uploaded file failed: {1} -> {2}',
                      self.kernel_id, filename, dest_path)

    async def download_file(self, filepath):
        container_id = self.data['container_id']
        container = self._docker.containers.container(container_id)
        # Limit file path to /home/work inside a container.
        # TODO: extend path search in virtual folders.
        abspath = (Path('/home/work') / filepath).resolve()
        try:
            with await container.get_archive(abspath) as tarobj:
                tarobj.fileobj.seek(0, 2)
                fsize = tarobj.fileobj.tell()
                assert fsize < 1 * 1048576, 'too large file.'
                tarbytes = tarobj.fileobj.getvalue()
        except DockerError:
            log.warning('Could not found the file: {0}', abspath)
            raise FileNotFoundError(f'Could not found the file: {abspath}')
        return tarbytes

    async def list_files(self, container_path: str):
        container_id = self.data['container_id']

        # Confine the lookable paths in the home directory
        home_path = Path('/home/work')
        try:
            container_path = (home_path / container_path).resolve()
            container_path.relative_to(home_path)
        except ValueError:
            raise PermissionError('You cannot list files outside /home/work')

        # Gather individual file information in the target path.
        code = textwrap.dedent('''
        import json
        import os
        import stat
        import sys

        files = []
        for f in os.scandir(sys.argv[1]):
            fstat = f.stat()
            ctime = fstat.st_ctime  # TODO: way to get concrete create time?
            mtime = fstat.st_mtime
            atime = fstat.st_atime
            files.append({
                'mode': stat.filemode(fstat.st_mode),
                'size': fstat.st_size,
                'ctime': ctime,
                'mtime': mtime,
                'atime': atime,
                'filename': f.name,
            })
        print(json.dumps(files))
        ''')
        proc = await asyncio.create_subprocess_exec(
            *[
                'docker', 'exec', container_id,
                '/opt/backend.ai/bin/python', '-c', code,
                str(container_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        outs, errs = await proc.communicate()
        outs = outs.decode('utf-8')
        errs = errs.decode('utf-8')
        return {'files': outs, 'errors': errs, 'abspath': str(container_path)}


class DockerCodeRunner(AbstractCodeRunner):
    def __init__(self, kernel_id,
                    kernel_host, repl_in_port, repl_out_port,
                    exec_timeout, client_features=None):
        super().__init__(
            repl_in_port, repl_out_port, exec_timeout,
            client_features=client_features)
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
    '''
    Check if the volume "backendai-krunner.{distro}.{arch}" exists and is up-to-date.
    If not, automatically create it and update its content from the packaged pre-built krunner tar
    archives.
    '''
    distro_name = re.search(r'^([a-z]+)\d+\.\d+$', distro).group(1)
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
                '-v', f'{volume_name}:/root/volume',
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
