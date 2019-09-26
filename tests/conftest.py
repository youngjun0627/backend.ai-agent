import aiodocker
import asyncio
import pytest


@pytest.fixture
def loop(event_loop):
    # naming shortcut
    return event_loop


@pytest.fixture
def docker(loop):
    docker = None

    async def get_docker():
        nonlocal docker
        docker = aiodocker.Docker()

    async def cleanup():
        await docker.close()

    loop.run_until_complete(get_docker())
    yield docker
    loop.run_until_complete(cleanup())


@pytest.fixture(scope='session')
def prepare_docker_images():
    event_loop = asyncio.get_event_loop()

    async def pull():
        docker = aiodocker.Docker()
        images_to_pull = [
            'alpine:3.8',
            'nginx:1.17-alpine',
            'lablup/kernel-lua:5.3-alpine3.8',
        ]
        for img in images_to_pull:
            try:
                await docker.images.inspect(img)
            except aiodocker.exceptions.DockerError as e:
                assert e.status == 404
                print(f'Pulling image "{img}" for testing...')
                await docker.pull(img)
        await docker.close()

    event_loop.run_until_complete(pull())


@pytest.fixture
def container(event_loop, docker):
    container = None
    config = {
        'Cmd': ['-c', 'echo hello'],
        'Entrypoint': 'sh',
        'Image': 'alpine:3.8',
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
async def create_container(event_loop, docker):
    container = None

    async def _create_container(config):
        nonlocal container
        container = await docker.containers.create_or_replace(
            config=config,
            name='kernel.test-container'
        )
        return container

    yield _create_container

    if container:
        await container.delete(force=True)
