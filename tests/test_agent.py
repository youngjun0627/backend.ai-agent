import asyncio
from pathlib import Path
from unittest import mock
import uuid

from aiodocker.docker import Docker
from aiodocker.exceptions import DockerError
import asynctest
import pytest
import uvloop

from sorna.agent.server import (
    heartbeat_timer, stats_timer, cleanup_timer, AgentRPCServer
)


# @pytest.fixture
# def loop():
#     """
#     Faster event loop than default one.

#     Using this loop fixture raises
#     `RuntimeError: Timeout context manager should be used inside a task`.
#     """
#     asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
#     loop = asyncio.new_event_loop()
#     yield loop
#     asyncio.set_event_loop_policy(None)  # restore default policy
#     loop.close()


@pytest.fixture
def loop(event_loop):
    return event_loop


@pytest.fixture
def docker(loop):
    docker = None

    async def get_docker():
        nonlocal docker
        docker = Docker(url='/var/run/docker.sock')

    async def cleanup():
        await docker.events.stop()
        docker.session.close()

    loop.run_until_complete(get_docker())
    yield docker
    loop.run_until_complete(cleanup())


@pytest.fixture
def config(tmpdir):
    class Config:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    config = Config(
        agent_ip=None,
        agent_port=6001,
        # redis_addr=HostPortPair(ip_address('127.0.0.1'), 6379),
        # event_addr=HostPortPair(ip_address('127.0.0.1'), 5002),
        exec_timeout=180,
        idle_timeout=600,
        max_kernels=1,
        # debug=False,
        # kernel_aliases=None,
        volume_root=Path(tmpdir),
    )
    return config


@pytest.fixture
def events():
    """
    Mocking manager's event server to not to actually run it for agent tests.
    """
    events = mock.Mock()
    events.call.dispatch = asynctest.CoroutineMock()
    return events


@pytest.fixture
def agent(loop, docker, config, events):
    async def cleanup():
        for kernel_id, info in agent.container_registry.items():
            # Kill and delete test containers
            container_id = info['container_id']
            try:
                await docker.containers.container(container_id).kill()
            except DockerError:
                pass
            finally:
                await docker.containers.container(container_id).delete()

    agent = AgentRPCServer(docker, config, events, loop=loop)
    yield agent
    loop.run_until_complete(cleanup())


@pytest.mark.integration
@pytest.mark.asyncio
class TestAgent:
    async def test_ping(self, agent):
        ret = agent.ping('hello')
        assert ret == 'hello'

    async def test_create_and_destroy_kernel(self, agent):
        kernel_id, stdin_port, stdout_port = await agent.create_kernel(
            'python3', {})
        assert kernel_id in agent.container_registry
        assert 'last_stat' not in agent.container_registry[kernel_id]

        await agent.destroy_kernel(kernel_id)
        assert 'last_stat' in agent.container_registry[kernel_id]
