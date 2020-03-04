import signal
from unittest.mock import AsyncMock

from aiodocker.exceptions import DockerError

from ai.backend.common.exception import ImageNotAvailable
from ai.backend.common.types import AutoPullBehavior
from ai.backend.common.docker import ImageRef

from ai.backend.agent.config import agent_local_config_iv
from ai.backend.agent.docker.agent import DockerAgent

import pytest


@pytest.fixture
async def agent(test_id, redis_container):
    agent = await DockerAgent.new(agent_local_config_iv.check({
        'agent': {
            'mode': 'docker',
            'id': f'i-{test_id}',
            'scaling-group': f'sg-{test_id}',
        },
        'container': {
            'scratch-type': 'hostdir',
            'stats-type': 'docker',
            'port-range': [19000, 19200],
        },
        'logging': {},
        'resource': {},
        'debug': {},
        'etcd': {
            'namespace': f'ns-{test_id}',
        },
        'redis': redis_container,
        'plugins': {},
    }), skip_initial_scan=True)  # for faster test iteration
    try:
        yield agent
    finally:
        await agent.shutdown(signal.SIGTERM)


@pytest.mark.asyncio
async def test_init(agent):
    print(agent)


imgref = ImageRef('index.docker.io/lablup/lua:5.3-alpine3.8', ['index.docker.io'])
query_digest = "sha256:b000000000000000000000000000000000000000000000000000000000000001"
digest_matching_image_info = {
    "Id": "sha256:b000000000000000000000000000000000000000000000000000000000000001",
    "RepoTags": [
        "lablup/lua:5.3-alpine3.8"
    ],
}
digest_mismatching_image_info = {
    "Id": "sha256:a000000000000000000000000000000000000000000000000000000000000002",
    "RepoTags": [
        "lablup/lua:5.3-alpine3.8"
    ],
}


@pytest.mark.asyncio
async def test_auto_pull_digest_when_digest_matching(agent, mocker):
    behavior = AutoPullBehavior.DIGEST
    inspect_mock = AsyncMock(return_value=digest_matching_image_info)
    mocker.patch.object(agent.docker.images, 'inspect', new=inspect_mock)
    pull = await agent.check_image(imgref, query_digest, behavior)
    assert not pull
    inspect_mock.assert_awaited_with(imgref.canonical)


@pytest.mark.asyncio
async def test_auto_pull_digest_when_digest_mismatching(agent, mocker):
    behavior = AutoPullBehavior.DIGEST
    inspect_mock = AsyncMock(return_value=digest_mismatching_image_info)
    mocker.patch.object(agent.docker.images, 'inspect', new=inspect_mock)
    pull = await agent.check_image(imgref, query_digest, behavior)
    assert pull
    inspect_mock.assert_awaited_with(imgref.canonical)


@pytest.mark.asyncio
async def test_auto_pull_digest_when_missing(agent, mocker):
    behavior = AutoPullBehavior.DIGEST
    inspect_mock = AsyncMock(
        side_effect=DockerError(status=404,
                                data={'message': 'Simulated missing image'}))
    mocker.patch.object(agent.docker.images, 'inspect', new=inspect_mock)
    pull = await agent.check_image(imgref, query_digest, behavior)
    assert pull
    inspect_mock.assert_called_with(imgref.canonical)


@pytest.mark.asyncio
async def test_auto_pull_tag_when_digest_matching(agent, mocker):
    behavior = AutoPullBehavior.TAG
    inspect_mock = AsyncMock(return_value=digest_matching_image_info)
    mocker.patch.object(agent.docker.images, 'inspect', new=inspect_mock)
    pull = await agent.check_image(imgref, query_digest, behavior)
    assert not pull
    inspect_mock.assert_awaited_with(imgref.canonical)


@pytest.mark.asyncio
async def test_auto_pull_tag_when_digest_mismatching(agent, mocker):
    behavior = AutoPullBehavior.TAG
    inspect_mock = AsyncMock(return_value=digest_mismatching_image_info)
    mocker.patch.object(agent.docker.images, 'inspect', new=inspect_mock)
    pull = await agent.check_image(imgref, query_digest, behavior)
    assert not pull
    inspect_mock.assert_awaited_with(imgref.canonical)


@pytest.mark.asyncio
async def test_auto_pull_tag_when_missing(agent, mocker):
    behavior = AutoPullBehavior.TAG
    inspect_mock = AsyncMock(
        side_effect=DockerError(status=404,
                                data={'message': 'Simulated missing image'}))
    mocker.patch.object(agent.docker.images, 'inspect', new=inspect_mock)
    pull = await agent.check_image(imgref, query_digest, behavior)
    assert pull
    inspect_mock.assert_called_with(imgref.canonical)


@pytest.mark.asyncio
async def test_auto_pull_none_when_digest_matching(agent, mocker):
    behavior = AutoPullBehavior.NONE
    inspect_mock = AsyncMock(return_value=digest_matching_image_info)
    mocker.patch.object(agent.docker.images, 'inspect', new=inspect_mock)
    pull = await agent.check_image(imgref, query_digest, behavior)
    assert not pull
    inspect_mock.assert_awaited_with(imgref.canonical)


@pytest.mark.asyncio
async def test_auto_pull_none_when_digest_mismatching(agent, mocker):
    behavior = AutoPullBehavior.NONE
    inspect_mock = AsyncMock(return_value=digest_mismatching_image_info)
    mocker.patch.object(agent.docker.images, 'inspect', new=inspect_mock)
    pull = await agent.check_image(imgref, query_digest, behavior)
    assert not pull
    inspect_mock.assert_awaited_with(imgref.canonical)


@pytest.mark.asyncio
async def test_auto_pull_none_when_missing(agent, mocker):
    behavior = AutoPullBehavior.NONE
    inspect_mock = AsyncMock(
        side_effect=DockerError(status=404,
                                data={'message': 'Simulated missing image'}))
    mocker.patch.object(agent.docker.images, 'inspect', new=inspect_mock)
    with pytest.raises(ImageNotAvailable) as e:
        await agent.check_image(imgref, query_digest, behavior)
    assert e.value.args[0] is imgref
    inspect_mock.assert_called_with(imgref.canonical)
