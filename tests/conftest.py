import aiodocker
import asyncio
import pytest


@pytest.fixture(scope='session')
def prepare_docker_images():
    event_loop = asyncio.get_event_loop()

    async def pull():
        docker = aiodocker.Docker()
        images_to_pull = [
            'alpine:latest',
            'lablup/kernel-lua:latest',
            'nginx:latest',
        ]
        for img in images_to_pull:
            try:
                await docker.images.get(img)
            except aiodocker.exceptions.DockerError as e:
                assert e.status == 404
                print(f'Pulling image "{img}" for testing...')
                await docker.pull(img)
        await docker.close()

    event_loop.run_until_complete(pull())


@pytest.fixture
def docker(event_loop, prepare_docker_images):
    docker = aiodocker.Docker()
    yield docker

    async def finalize():
        await docker.close()

    event_loop.run_until_complete(finalize())


@pytest.fixture
def container(event_loop, docker):
    container = None
    config = {
        'Cmd': ['-c', 'echo hello'],
        'Entrypoint': 'sh',
        'Image': 'alpine:latest',
    }

    async def spawn():
        nonlocal container
        container = await docker.containers.create_or_replace(
            config=config,
            name='kernel.test-container'
        )
        await container.start()

    event_loop.run_until_complete(spawn())

    yield container

    async def finalize():
        nonlocal container
        if container:
            await container.delete(force=True)

    event_loop.run_until_complete(finalize())


@pytest.fixture
def create_container(event_loop, docker):
    container = None

    async def _create_container(config):
        nonlocal container
        container = await docker.containers.create_or_replace(
            config=config,
            name='kernel.test-container'
        )
        return container

    yield _create_container

    async def finalize():
        nonlocal container
        if container:
            await container.delete(force=True)

    event_loop.run_until_complete(finalize())


@pytest.fixture
def daemon_container(event_loop, docker):
    container = None
    config = {
        'Image': 'nginx:latest',
        'ExposedPorts': {
            '80/tcp': {},
        },
        'HostConfig': {
            'PortBindings': {
                '80/tcp': [{'HostPort': '8080'}],
            }
        }
    }

    async def spawn():
        nonlocal container
        container = await docker.containers.create_or_replace(
            config=config,
            name='kernel.test-daemon-container'
        )
        await container.start()

    event_loop.run_until_complete(spawn())

    yield container

    async def finalize():
        nonlocal container
        if container:
            await container.delete(force=True)

    event_loop.run_until_complete(finalize())
