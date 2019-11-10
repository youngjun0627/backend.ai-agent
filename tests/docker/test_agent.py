import signal

# from ai.backend.common.types import AutoPullBehavior
from ai.backend.agent.config import initial_config_iv
from ai.backend.agent.docker.agent import DockerAgent

import pytest


@pytest.fixture
async def agent(test_id, redis_container):
    agent = await DockerAgent.new(initial_config_iv.check({
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
    }))
    try:
        yield agent
    finally:
        await agent.shutdown(signal.SIGTERM)


@pytest.mark.asyncio
async def test_init(agent):
    print(agent)


@pytest.mark.asyncio
async def test_auto_pull(agent):
    raise NotImplementedError
