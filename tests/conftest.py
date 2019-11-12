import asyncio
import json
import secrets
import subprocess

from ai.backend.common.types import HostPortPair

import aiodocker
import pytest


@pytest.fixture(scope='session')
def backend_type():
    # TODO: change by environment/tox?
    return 'docker'


@pytest.fixture(scope='session')
def test_id():
    return f'testing-{secrets.token_urlsafe(8)}'


@pytest.fixture(scope='session')
def prepare_images(backend_type):

    async def pull():
        if backend_type == 'docker':
            docker = aiodocker.Docker()
            images_to_pull = [
                'alpine:3.8',
                'nginx:1.17-alpine',
                'redis:5.0.5-alpine',
            ]
            for img in images_to_pull:
                try:
                    await docker.images.inspect(img)
                except aiodocker.exceptions.DockerError as e:
                    assert e.status == 404
                    print(f'Pulling image "{img}" for testing...')
                    await docker.pull(img)
            await docker.close()
        elif backend_type == 'k8s':
            raise NotImplementedError

    asyncio.run(pull())


@pytest.fixture
async def docker():
    docker = aiodocker.Docker()
    try:
        yield docker
    finally:
        await docker.close()


@pytest.fixture(scope='session')
def redis_container(test_id, backend_type, prepare_images):
    if backend_type == 'docker':
        subprocess.run([
            'docker', 'run',
            '-d',
            '-P',
            '--name', f'{test_id}.redis',
            'redis:5.0.5-alpine',
        ], capture_output=True)
        proc = subprocess.run([
            'docker', 'inspect',
            f'{test_id}.redis',
        ], capture_output=True)
        container_info = json.loads(proc.stdout)
        host_port = int(container_info[0]['NetworkSettings']['Ports']['6379/tcp'][0]['HostPort'])
    elif backend_type == 'k8s':
        raise NotImplementedError

    try:
        yield {
            'addr': HostPortPair('localhost', host_port),
            'password': None,
        }
    finally:
        if backend_type == 'docker':
            subprocess.run([
                'docker', 'rm',
                '-f',
                f'{test_id}.redis',
            ], capture_output=True)
        elif backend_type == 'k8s':
            raise NotImplementedError


@pytest.fixture
async def create_container(test_id, backend_type, docker):
    container = None
    cont_id = secrets.token_urlsafe(4)

    if backend_type == 'docker':
        async def _create_container(config):
            nonlocal container
            container = await docker.containers.create_or_replace(
                config=config,
                name=f'kernel.{test_id}-{cont_id}',
            )
            return container
    elif backend_type == 'k8s':
        raise NotImplementedError

    try:
        yield _create_container
    finally:
        if container is not None:
            await container.delete(force=True)
