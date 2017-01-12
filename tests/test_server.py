import asyncio

from unittest import mock

import asynctest
import pytest

from sorna.agent.resources import CPUAllocMap
from sorna.agent.server import (
    get_extra_volumes, heartbeat_timer, stats_timer, cleanup_timer,
    match_result, AgentRPCServer
)


@pytest.fixture
def mock_volumes_list():
    return {
        'Volumes': [
            {
                'Driver': 'local',
                'Labels': None,
                'Mountpoint': '/fake/mount/point/1',
                'Name': 'fakename1',
                'Options': {},
                'Scope': 'local'
            },
            {
                'Driver': 'local',
                'Labels': None,
                'Mountpoint': '/fake/mount/point/2',
                'Name': 'fakename2',
                'Options': {},
                'Scope': 'local'
            },
            {
                'Driver': 'local',
                'Labels': None,
                'Mountpoint': '/fake/mount/point/3',
                'Name': 'deeplearning-samples',
                'Options': {},
                'Scope': 'local'
            },
        ],
        'Warnings': None
    }


@pytest.mark.asyncio
async def test_get_extra_volumes(mock_volumes_list):
    mock_docker = mock.Mock()
    mock_docker.volumes.list = asynctest.CoroutineMock(
        return_value=mock_volumes_list)

    mnt_list = await get_extra_volumes(mock_docker, 'python3')
    assert mnt_list == []

    mnt_list = await get_extra_volumes(mock_docker, 'python3-tensorflow')
    assert len(mnt_list) == 1
    assert 'deeplearning-samples' in mnt_list[0]

    mnt_list = await get_extra_volumes(mock_docker, 'python3-tensorflow-gpu')
    assert len(mnt_list) == 1
    assert 'deeplearning-samples' in mnt_list[0]


@pytest.mark.skip('not implemented yet')
@pytest.mark.asyncio
async def test_heartbeat_timer():
    pass


@pytest.mark.skip('not implemented yet')
@pytest.mark.asyncio
async def test_stats_timer():
    pass


@pytest.mark.skip('not implemented yet')
@pytest.mark.asyncio
async def test_cleanup_timer():
    pass


def test_match_result():
    # TODO: parameterize test
    mock_result = {
        'stdout': 'stdout',
        'stderr': 'stderr',
        'exceptions': []
    }
    mock_match = {
        'op': 'contains',
        'target': 'stdout',
        'value': 'mock_value',
    }

    assert not match_result(mock_result, mock_match)
    mock_result['stdout'] = 'mock_value'
    assert match_result(mock_result, mock_match)


class TestAgentRPCServer:
    @pytest.fixture
    def mock_agent(self):
        mock_docker = mock.Mock()
        mock_args = mock.Mock()
        mock_events = mock.Mock()

        agent = AgentRPCServer(mock_docker, mock_args, mock_events)

        return agent

    def test_initialization(self):
        mock_docker = mock.Mock()
        mock_args = mock.Mock()
        mock_events = mock.Mock()

        agent = AgentRPCServer(mock_docker, mock_args, mock_events)

        assert isinstance(agent.loop, asyncio.BaseEventLoop)
        assert agent.docker == mock_docker
        assert agent.config == mock_args
        assert agent.events == mock_events
        assert agent.container_registry == {}
        assert isinstance(agent.container_cpu_map, CPUAllocMap)

    def test_ping(self, mock_agent):
        msg = mock_agent.ping('ping~')
        assert msg == 'ping~'

    @pytest.mark.asyncio
    async def test_create_kernel(self, mock_agent):
        agent = mock_agent
        agent._create_kernel = asynctest.CoroutineMock(return_value='fakeid')
        agent.container_registry['fakeid'] = {'stdin_port': 1,
                                              'stdout_port': 2}

        id, inport, outport = await agent.create_kernel('python3', {})

        agent._create_kernel.assert_called_once_with('python3')
        assert id == 'fakeid'
        assert inport == 1
        assert outport == 2

    @pytest.mark.asyncio
    async def test_destroy_kernel(self, mock_agent):
        agent = mock_agent
        agent._destroy_kernel = asynctest.CoroutineMock()
        await agent.destroy_kernel('fakeid')
        agent._destroy_kernel.assert_called_once_with('fakeid',
                                                      'user-requested')

    @pytest.mark.asyncio
    async def test_restart_kernel(self, mock_agent):
        agent = mock_agent
        agent.container_registry['fakeid'] = {'lang': 'python3',
                                              'stdin_port': 1,
                                              'stdout_port': 2}
        agent._create_kernel = asynctest.CoroutineMock(return_value='fakeid')
        agent._destroy_kernel = asynctest.CoroutineMock()

        inport, outport = await agent.restart_kernel('fakeid')

        agent._create_kernel.assert_called_once_with('python3',
                                                     kernel_id='fakeid')
        agent._destroy_kernel.assert_called_once_with('fakeid', 'restarting')
        assert inport == 1
        assert outport == 2

    @pytest.mark.asyncio
    async def test_execute_code(self, mocker, mock_agent):
        agent = mock_agent
        agent._execute_code = asynctest.CoroutineMock(return_value={'mock': 1})
        mock_match_result = mocker.patch('sorna.agent.server.match_result',
                                         return_value=True)

        result = await agent.execute_code(
            entry_id='fakeentry', kernel_id='fakekernel', code_id='codeid',
            code='print(1)', match=None)

        agent._execute_code.assert_called_once_with('fakeentry', 'fakekernel',
                                                    'codeid', 'print(1)')
        mock_match_result.assert_not_called()
        assert result == {'mock': 1}

    @pytest.mark.asyncio
    async def test_execute_code_match_result(self, mocker, mock_agent):
        agent = mock_agent
        agent._execute_code = asynctest.CoroutineMock(return_value={'mock': 1})
        mock_match_result = mocker.patch('sorna.agent.server.match_result',
                                         return_value=True)

        match = {
            'op': 'contains',
            'target': 'stdout',
            'value': 'mock_value',
        }
        await agent.execute_code(
            entry_id='fakeentry', kernel_id='fakekernel', code_id='codeid',
            code='print(1)', match=match)

        mock_match_result.assert_called()

    @pytest.mark.skip('not implemented yet')
    def test_reset(self):
        pass

    @pytest.mark.skip('not implemented yet')
    def test__create_kernel(self):
        pass

    @pytest.mark.skip('not implemented yet')
    def test__destroy_kernel(self):
        pass

    @pytest.mark.skip('not implemented yet')
    def test__execute_kernel(self):
        pass

