import argparse
import asyncio
import os

from unittest import mock

import asynctest
import pytest
import simplejson as json
from aiodocker.docker import Docker

import ai.backend.agent.server as server_mod
from ai.backend.agent.resources import CPUAllocMap
from ai.backend.agent.server import (
    get_extra_volumes,
    AgentRPCServer,
)
from ai.backend.common.argparse import host_port_pair


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


@pytest.fixture
async def mock_agent(monkeypatch):
    config = argparse.Namespace()
    config.etcd_addr = host_port_pair(os.environ.get('BACKEND_ETCD_ADDR', 'localhost:2379'))
    config.namespace = os.environ.get('BACKEND_NAMESPACE', 'local')
    config.instance_id = 'i-testing'
    config.instance_ip = '127.0.0.1'
    config.datadog_api_key = None
    config.datadog_app_key = None
    config.raven_uri = None

    monkeypatch.setattr(server_mod.AsyncEtcd, 'set',
                        asynctest.CoroutineMock(), raising=False)
    monkeypatch.setattr(server_mod.AsyncEtcd, 'get',
                        asynctest.CoroutineMock(), raising=False)
    # TODO
    #monkeypatch.setattr(server_mod.AgentEventPublisher, 'publish',
    #                    asynctest.CoroutineMock(), raising=False)

    agent = AgentRPCServer(config)
    print('mock_agent init1')
    await agent.init()
    print('mock_agent init2')

    yield agent

    print('mock_agent shutdown1')
    await agent.shutdown()
    print('mock_agent shutdown2')


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


@pytest.mark.asyncio
async def test_init(mock_agent):
    agent = mock_agent
    assert agent.loop is asyncio.get_event_loop()
    assert isinstance(agent.docker, Docker)
    assert agent.container_registry == {}
    assert isinstance(agent.container_cpu_map, CPUAllocMap)


def test_ping(mock_agent):
    msg = mock_agent.ping('ping~')
    assert msg == 'ping~'


@pytest.mark.asyncio
async def test_create_kernel(mock_agent):
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
async def test_destroy_kernel(mock_agent):
    agent = mock_agent
    agent._destroy_kernel = asynctest.CoroutineMock()
    await agent.destroy_kernel('fakeid')
    agent._destroy_kernel.assert_called_once_with('fakeid',
                                                  'user-requested')

@pytest.mark.asyncio
async def test_restart_kernel(mock_agent):
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
async def test_execute_code(mocker, mock_agent):
    agent = mock_agent
    #agent._execute_code = asynctest.CoroutineMock(return_value={'mock': 1})

    result = await agent.execute_code(
        api_version=2,
        kernel_id='fakekernel',
        mode='query',
        code='print(1)',
        opts={})

    assert result == {'mock': 1}

@pytest.mark.asyncio
async def test_reset(mock_agent):
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
async def test__create_kernel_with_minimal_confs(mock_agent, tmpdir):
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
async def test__create_kernel_with_existing_kernel_id(mock_agent,
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

    from ai.backend.agent.server import restarting_kernels
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
async def test__destroy_kernel(mocker, mock_agent):
    kernel_id, reason = 'fake-kernel-id', 'fake-reason'

    mock_collect_stats = mocker.patch('ai.backend.agent.server.collect_stats',
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

    assert 'last_stat' not in agent.container_registry[kernel_id]

    await agent._destroy_kernel(kernel_id, reason)

    mock_collect_stats.assert_called_once_with([mock_container])
    mock_container.kill.assert_called_once_with()
    assert 'last_stat' in agent.container_registry[kernel_id]
    assert 'last_stat' not in agent.container_registry['other-kernel-id']

###############################
# AgentRPCServer._execute_code
@pytest.mark.asyncio
@pytest.mark.parametrize('up_files', [True, False])
async def test__execute_code(mocker, mock_agent, tmpdir, up_files):
    entry_id = 'fake-entry-id'
    kernel_id = 'fake-kernel-id'
    code_id = 'fake-code-id'
    code = 'print(0)'

    tmpdir.mkdir(kernel_id)

    read_data = [
        json.dumps({
            'stdout': 'stdout',
            'stderr': 'stderr',
            'media': ['fake-media'],
            'options': {
                'upload_output_files': up_files,
            },
            'exceptions': ['fake-exceptions'],
        })
    ]
    mock_upload_to_s3 = mocker.patch(
        'ai.backend.agent.server.upload_output_files_to_s3',
        new_callable=asynctest.CoroutineMock
    )
    mock_upload_to_s3.return_value = ['fake-uploaded-files']
    mock_container_sock = mock.Mock()
    mock_container_sock.read = asynctest.CoroutineMock(
        return_value=read_data)
    mock_aiozmq = mocker.patch('ai.backend.agent.server.aiozmq')
    mock_aiozmq.create_zmq_stream = asynctest.CoroutineMock()
    mock_aiozmq.create_zmq_stream.return_value = mock_container_sock

    agent = mock_agent
    agent.config.volume_root = tmpdir
    agent.container_registry = {
        kernel_id: {
            'last_used': 0,
            'num_queries': 0,
            'addr': 'fake-container-addr',
            'exec_timeout': 10,
        }
    }

    result = await agent._execute_code(entry_id, kernel_id, code_id, code)

    assert agent.container_registry[kernel_id]['last_used'] != 0
    assert agent.container_registry[kernel_id]['num_queries'] == 1
    assert 1 == mock_aiozmq.create_zmq_stream.call_count
    mock_container_sock.write.assert_called_once_with([
        code_id.encode('ascii'), code.encode('utf8')
    ])
    assert result['stdout'] == 'stdout'
    assert result['stderr'] == 'stderr'
    assert result['media'] == ['fake-media']
    assert result['exceptions'] == ['fake-exceptions']
    if up_files:
        assert 1 == mock_upload_to_s3.call_count
        assert 'fake-uploaded-files' in str(result['files'])
    else:
        assert 0 == mock_upload_to_s3.call_count
        assert result['files'] == []  # not 'fake-uploaded-files'

@pytest.mark.asyncio
async def test__execute_code_timeout_error(mocker, mock_agent, tmpdir):
    entry_id = 'fake-entry-id'
    kernel_id = 'fake-kernel-id'
    code_id = 'fake-code-id'
    code = 'print(0)'

    tmpdir.mkdir(kernel_id)

    mock_container_sock = mock.Mock()
    mock_container_sock.read = asynctest.CoroutineMock(
        side_effect=asyncio.TimeoutError)
    mock_aiozmq = mocker.patch('ai.backend.agent.server.aiozmq')
    mock_aiozmq.create_zmq_stream = asynctest.CoroutineMock()
    mock_aiozmq.create_zmq_stream.return_value = mock_container_sock

    agent = mock_agent
    agent.config.volume_root = tmpdir
    agent.container_registry = {
        kernel_id: {
            'last_used': 0,
            'num_queries': 0,
            'addr': 'fake-container-addr',
            'exec_timeout': 10,
        }
    }
    agent._destroy_kernel = asynctest.CoroutineMock()

    with pytest.raises(asyncio.TimeoutError):
        await agent._execute_code(entry_id, kernel_id, code_id, code)

    agent._destroy_kernel.assert_called_once_with(kernel_id,
                                                  'exec-timeout')
###############################

@pytest.mark.asyncio
async def test_heartbeat(mock_agent):
    agent = mock_agent
    agent.events.call.dispatch = asynctest.CoroutineMock()
    fake_config_info = {
        'inst_id': 'fake-inst-id',
        'agent_ip': 'fake-agent-ip',
        'agent_port': 'fake-agent-port',
        'inst_type': 'fake-inst-type',
        'max_kernels': 'fake-max-kernels',
    }
    agent.config.configure_mock(**fake_config_info)
    agent.container_registry = {
        'kernel-id-1': {},
        'kernel-id-2': {},
    }

    await agent.heartbeat(5)

    mock_agent.events.call.dispatch.assert_called_once_with(
        'instance_heartbeat', fake_config_info['inst_id'], mock.ANY,
        ['kernel-id-1', 'kernel-id-2'], 5)

@pytest.mark.asyncio
async def test_update_stats(mocker, mock_agent):
    mock_collect_stats = mocker.patch('ai.backend.agent.server.collect_stats',
                                      new_callable=asynctest.CoroutineMock)
    mock_stats = mock_collect_stats.return_value = [
        {
            'cpu_stats': {
                'cpu_usage': {
                    'total_usage': 1e5,
                }
            }
        }
    ]

    agent = mock_agent
    agent.container_registry = {
        'kernel-id-1': {
            'container_id': 'fake-container-id-1',
            'exec_timeout': 10,
            'mem_limit': 1024,
            'num_queries': 2,
            'last_used': 000000,
        },
    }
    agent.config.idle_timeout = 30
    agent.docker = mock.Mock()
    agent.events.call.dispatch = asynctest.CoroutineMock()

    await agent.update_stats(5)

    # Assert stats updated
    assert mock_stats[0]['exec_timeout'] == 10
    assert mock_stats[0]['idle_timeout'] == 30
    assert mock_stats[0]['mem_limit'] == 1
    assert mock_stats[0]['num_queries'] == 2
    assert mock_stats[0]['idle'] > 000000

@pytest.mark.asyncio
async def test_fetch_docker_events(mock_agent):
    agent = mock_agent
    agent.docker.events.run = asynctest.CoroutineMock(side_effect=[0, 1])

    await agent.fetch_docker_events()

    assert 3 == agent.docker.events.run.call_count  # 0, 1, raise err

@pytest.mark.asyncio
async def test_monitor_clean_kernel_upon_action_die(mock_agent):
    agent = mock_agent
    mock_subscriber = agent.docker.events.subscribe.return_value = \
            mock.Mock()
    mock_subscriber.get = asynctest.CoroutineMock()
    mock_evdata = mock_subscriber.get.side_effect = [{
        'Action': 'die',
        'id': 'fake-container-id',
        'Actor': {
            'Attributes': {
                'name': 'kernel.fake-container-name.fake-kernel-id',
            }
        }
    }, asyncio.CancelledError]
    agent.clean_kernel = asynctest.CoroutineMock()

    await agent.monitor()

    agent.clean_kernel.assert_called_once_with('fake-kernel-id')

@pytest.mark.asyncio
async def test_clean_kernel(mock_agent, tmpdir):
    agent = mock_agent
    agent.container_registry = {
        'kernel-id-1': {
            'container_id': 'fake-container-id-1',
            'core_set': '',
        },
    }
    mock_container = mock.Mock()
    mock_container.delete = asynctest.CoroutineMock()
    agent.docker.containers.container.return_value = mock_container
    agent.config.volume_root = tmpdir
    agent.container_cpu_map.free = mock.Mock()
    agent.events.call.dispatch = asynctest.CoroutineMock()

    await agent.clean_kernel('kernel-id-1')

    mock_container.delete.assert_called_once_with()
    agent.container_cpu_map.free.assert_called_once_with(mock.ANY)
    assert 'kernel-id-1' not in agent.container_registry

@pytest.mark.asyncio
async def test_clean_old_kernels(mock_agent):
    agent = mock_agent
    agent.container_registry = {
        'kernel-id-1': {
            'last_used': 0,
        },
    }
    agent.config.idle_timeout = 1
    agent._destroy_kernel = asynctest.CoroutineMock()

    await agent.clean_old_kernels()

    agent._destroy_kernel.assert_called_once_with(
        'kernel-id-1', 'idle-timeout')

@pytest.mark.asyncio
async def test_do_not_clean_new_kernels(mock_agent):
    agent = mock_agent
    agent.container_registry = {
        'kernel-id-1': {
            'last_used': 0,
        },
    }
    agent.config.idle_timeout = 999999
    agent._destroy_kernel = asynctest.CoroutineMock()

    await agent.clean_old_kernels()

    agent._destroy_kernel.assert_not_called()

@pytest.mark.asyncio
async def test_clean_all_kernels(mock_agent):
    agent = mock_agent
    agent.container_registry = {
        'kernel-id-1': {
            'last_used': 0,
        },
    }
    agent._destroy_kernel = asynctest.CoroutineMock()

    await agent.clean_all_kernels()

    agent._destroy_kernel.assert_called_once_with(
        'kernel-id-1', 'agent-termination')
