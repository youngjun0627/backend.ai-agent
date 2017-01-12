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

    @pytest.mark.asyncio
    async def test_reset(self, mock_agent):
        agent = mock_agent
        agent.container_registry = {'1': {}, '2': {}}
        agent._destroy_kernel = asynctest.CoroutineMock()

        await agent.reset()

        assert 2 == agent._destroy_kernel.call_count
        agent._destroy_kernel.assert_any_call('1', 'agent-reset')
        agent._destroy_kernel.assert_any_call('2', 'agent-reset')

    ################################
    # AgentRPCServer._create_kernel
    @pytest.mark.asyncio
    async def test__create_kernel_with_minimal_confs(self, mock_agent, tmpdir):
        agent = mock_agent
        mock_image_info = {
            'ContainerConfig': {
                'Labels': {}
            }
        }
        mock_container = mock.Mock()
        mock_container.port = asynctest.CoroutineMock()
        mock_container.port.side_effect = [
            [{'HostPort': 'repl'}],
            [{'HostPort': 'stdin'}],
            [{'HostPort': 'stdout'}],
        ]
        mock_container.start = asynctest.CoroutineMock()
        agent.docker.containers.create = asynctest.CoroutineMock(
            return_value=mock_container)
        agent.docker.images.get = asynctest.CoroutineMock(
            return_value=mock_image_info)
        agent.docker.volumes.list = asynctest.CoroutineMock(
            return_value={'Volumes': []})
        agent.config.exec_timeout = 10
        agent.config.volume_root = tmpdir
        agent.events.call.dispatch = asynctest.CoroutineMock()

        lang = 'python3'
        kernel_id = 'fake-kernel-id'
        ret = await agent._create_kernel(lang, kernel_id)

        assert 1 == agent.events.call.dispatch.call_count
        assert 1 == agent.docker.containers.create.call_count
        mock_container.start.assert_called_once_with()
        assert ret == kernel_id
        container_info = agent.container_registry[kernel_id]
        assert container_info
        assert container_info['lang'] == lang
        assert container_info['addr'] == 'tcp://127.0.0.1:repl'
        assert container_info['ip'] == '127.0.0.1'
        assert container_info['port'] == 2001
        assert container_info['host_port'] == 'repl'
        assert container_info['stdin_port'] == 'stdin'
        assert container_info['stdout_port'] == 'stdout'
        assert container_info['cpu_shares'] == 1024
        assert container_info['num_queries'] == 0

    @pytest.mark.asyncio
    async def test__create_kernel_with_existing_kernel_id(self, mock_agent,
                                                          tmpdir):
        agent = mock_agent
        mock_image_info = {
            'ContainerConfig': {
                'Labels': {}
            }
        }
        mock_container = mock.Mock()
        mock_container.port = asynctest.CoroutineMock()
        mock_container.port.side_effect = [
            [{'HostPort': 'repl'}],
            [{'HostPort': 'stdin'}],
            [{'HostPort': 'stdout'}],
        ]
        mock_container.start = asynctest.CoroutineMock()
        agent.docker.containers.create = asynctest.CoroutineMock(
            return_value=mock_container)
        agent.docker.images.get = asynctest.CoroutineMock(
            return_value=mock_image_info)
        agent.docker.volumes.list = asynctest.CoroutineMock(
            return_value={'Volumes': []})
        agent.config.exec_timeout = 10
        agent.config.volume_root = tmpdir
        agent.events.call.dispatch = asynctest.CoroutineMock()

        lang = 'python3'
        kernel_id = 'fake-kernel-id'

        from sorna.agent.server import restarting_kernels
        old_restarting_kernels = restarting_kernels
        restarting_kernels[kernel_id] = mock.Mock()
        restarting_kernels[kernel_id].wait = asynctest.CoroutineMock()
        agent.container_registry[kernel_id] = {'core_set': [0, 1, 2, 3]}

        await agent._create_kernel(lang, kernel_id)

        # Ensure wait is called to delete previous container.
        restarting_kernels[kernel_id].wait.assert_called_once_with()
        assert len(restarting_kernels) > 0

        # If timeout occurs while restarting kernel.
        restarting_kernels[kernel_id].wait.side_effect = asyncio.TimeoutError
        agent.clean_kernel = asynctest.CoroutineMock()

        with pytest.raises(asyncio.TimeoutError):
            await agent._create_kernel(lang, kernel_id)
        agent.clean_kernel.assert_called_once_with(kernel_id)
        assert len(restarting_kernels) == 0  # kernel_id is deleted

        restarting_kernels = old_restarting_kernels
    ################################

    @pytest.mark.asyncio
    async def test__destroy_kernel(self, mocker, mock_agent):
        mock_collect_stats = mocker.patch('sorna.agent.server.collect_stats',
                                          new_callable=asynctest.CoroutineMock)
        mock_collect_stats.return_value = [mock.Mock()]

        agent = mock_agent
        agent.container_registry = {
            'fake-kernel-id': {
                'container_id': 'fake-container-id',
            },
            'other-kernel-id': {
                'container_id': 'should-not-be-destroyed'
            }
        }
        mock_container = mock.Mock()
        mock_container.kill = asynctest.CoroutineMock()
        agent.docker.containers.container = mock.Mock(
            return_value=mock_container)

        kernel_id, reason = 'fake-kernel-id', 'fake-reason'
        await agent._destroy_kernel(kernel_id, reason)

        mock_collect_stats.assert_called_once_with([mock_container])
        mock_container.kill.assert_called_once_with()

    @pytest.mark.skip('not implemented yet')
    def test__execute_code(self):
        pass
