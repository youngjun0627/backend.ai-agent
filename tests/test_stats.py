import asyncio

import aiodocker
import pytest

from ai.backend.agent import stats


@pytest.fixture(scope='session')
def prepare_docker_images():
    event_loop = asyncio.get_event_loop()
    async def pull():
        docker = aiodocker.Docker()
        images_to_pull = [
            'alpine:latest',
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
            name='test-container'
        )
        await container.start()
    event_loop.run_until_complete(spawn())

    yield container

    async def finalize():
        nonlocal container
        if container:
            await container.delete(force=True)
    event_loop.run_until_complete(finalize())


@pytest.mark.asyncio
async def test_collect_stats(container):
    ret = await stats.collect_stats([container])

    assert 'cpu_used' in ret[0]
    assert 'mem_max_bytes' in ret[0]
    assert 'mem_cur_bytes' in ret[0]
    assert 'net_rx_bytes' in ret[0]
    assert 'net_tx_bytes' in ret[0]
    assert 'io_read_bytes' in ret[0]
    assert 'io_write_bytes' in ret[0]
    assert 'io_max_scratch_size' in ret[0]
    assert 'io_cur_scratch_size' in ret[0]


def test_numeric_list():
    s = '1 3 5 7'
    ret = stats.numeric_list(s)

    assert ret == [1, 3, 5, 7]


def test_read_sysfs(tmpdir):
    p = tmpdir.join('test.txt')
    p.write('1357')
    ret = stats.read_sysfs(p)

    assert isinstance(ret, int)
    assert ret == 1357
