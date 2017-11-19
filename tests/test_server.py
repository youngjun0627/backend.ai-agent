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
async def mock_agent(monkeypatch, mocker, tmpdir):
    config = argparse.Namespace()
    config.etcd_addr = host_port_pair(
        os.environ.get('BACKEND_ETCD_ADDR', 'localhost:2379'))
    config.namespace = os.environ.get('BACKEND_NAMESPACE', 'local')
    config.instance_id = 'i-testing'
    config.instance_ip = '127.0.0.1'
    config.datadog_api_key = None
    config.datadog_app_key = None
    config.raven_uri = None
    config.agent_ip = 'localhost'
    config.agent_port = '6001'
    config.region = 'local'
    config.scratch_root = tmpdir
    config.debug_kernel = False

    # monkeypatch.setattr(server_mod.AsyncEtcd, 'set',
    #                     asynctest.CoroutineMock(), raising=False)
    # monkeypatch.setattr(server_mod.AsyncEtcd, 'get',
    #                     asynctest.CoroutineMock(), raising=False)
    # from ai.backend.common.etcd import AsyncEtcd
    # mocker.patch.object(AsyncEtcd, 'get', asynctest.CoroutineMock())
    # TODO
    # monkeypatch.setattr(server_mod.AgentEventPublisher, 'publish',
    #                     asynctest.CoroutineMock(), raising=False)

    # Build fake etcd database
    # from ai.backend.common.etcd import AsyncEtcd
    # etcd = AsyncEtcd(config.etcd_addr, config.namespace)
    # await etcd.put('nodes/manager', 'fake-manager-id')
    # await etcd.put('nodes/manager/event_addr', 'localhost:5002')
    # await etcd.put('nodes/redis',
    #                os.environ.get('BACKEND_REDIS_ADDR', 'localhost:6379'))

    agent = AgentRPCServer(config)
    print('mock_agent init1')
    # await agent.init()
    print('mock_agent init2')

    yield agent

    print('mock_agent shutdown1')
    # await agent.shutdown()
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


# @pytest.mark.asyncio
# async def test_init(mock_agent):
#     print('1')
#     agent = mock_agent
#     print('2')
#     assert agent.loop is asyncio.get_event_loop()
#     print('3')
#     assert isinstance(agent.docker, Docker)
#     print('4')
#     assert agent.container_registry == {}
#     print('5')
#     assert isinstance(agent.container_cpu_map, CPUAllocMap)


def test_ping(mock_agent):
    msg = mock_agent.ping('ping~')
    assert msg == 'ping~'


@pytest.mark.asyncio
async def test_create_kernel(mock_agent):
    agent = mock_agent
    agent._create_kernel = asynctest.CoroutineMock(return_value='fake-return')
    r = await agent.create_kernel(
            'fake-kernel-id', {'lang': 'python3'})
    agent._create_kernel.assert_called_once_with('fake-kernel-id',
                                                 {'lang': 'python3'})
    assert r == 'fake-return'


@pytest.mark.asyncio
async def test_destroy_kernel(mock_agent):
    agent = mock_agent
    agent._destroy_kernel = asynctest.CoroutineMock()
    await agent.destroy_kernel('fakeid')
    agent._destroy_kernel.assert_called_once_with('fakeid',
                                                  'user-requested')


@pytest.mark.asyncio
async def test_interrupt_kernel(mock_agent):
    agent = mock_agent
    agent._interrupt_kernel = asynctest.CoroutineMock()
    await agent.interrupt_kernel('fakeid')
    agent._interrupt_kernel.assert_called_once_with('fakeid')


@pytest.mark.asyncio
async def test_get_completions(mock_agent):
    agent = mock_agent
    agent._get_completions = asynctest.CoroutineMock()
    await agent.get_completions('fakeid', 'fakemode', 'text', {})
    agent._get_completions.assert_called_once_with('fakeid', 'fakemode',
                                                   'text', {})


# @pytest.mark.asyncio
# async def test_restart_kernel(mock_agent):
#     agent = mock_agent
#     agent.container_registry['fakeid'] = {'lang': 'python3',
#                                           'stdin_port': 1,
#                                           'stdout_port': 2}
#     agent._create_kernel = asynctest.CoroutineMock(return_value='fakeid')
#     agent._destroy_kernel = asynctest.CoroutineMock()

#     inport, outport = await agent.restart_kernel('fakeid')

#     agent._create_kernel.assert_called_once_with('python3',
#                                                  kernel_id='fakeid')
#     agent._destroy_kernel.assert_called_once_with('fakeid', 'restarting')
#     assert inport == 1
#     assert outport == 2


@pytest.mark.asyncio
async def test_execute(mock_agent):
    agent = mock_agent
    agent._execute = asynctest.CoroutineMock(return_value='fake-result')

    result = await agent.execute(
        api_version=2,
        kernel_id='fakekernel',
        run_id='fakerunid',
        mode='query',
        code='print(1)',
        opts={}
    )

    assert agent._execute.called == 1
    assert result == 'fake-result'


@pytest.mark.asyncio
async def test_upload_file(mocker, mock_agent):
    agent = mock_agent
    agent._accept_file = asynctest.CoroutineMock()

    result = await agent.upload_file(
        kernel_id='fakeid',
        filename='fakefilename',
        filedata=b'print(1)',
    )

    agent._accept_file.assert_called_once_with('fakeid', 'fakefilename',
                                               b'print(1)')


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
        [{'HostPort': '2000'}],
        [{'HostPort': '2001'}],
        [{'HostPort': '2002'}],
        [{'HostPort': '2003'}],
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

    kernel_id = 'fake-kernel-id'
    config = {
        'lang': 'python3',
        'limits': {'cpu_slot': 1, 'gpu_slot': 0, 'mem_slot': 1},
        'mounts': [],
    }
    ret = await agent._create_kernel(kernel_id, config)

    assert 1 == agent.docker.containers.create.call_count
    mock_container.start.assert_called_once_with()
    assert ret['id'] == kernel_id
    assert ret['repl_in_port'] == 2000
    assert ret['repl_out_port'] == 2001
    assert ret['stdin_port'] == 2002
    assert ret['stdout_port'] == 2003
    container_info = agent.container_registry[kernel_id]
    assert container_info['lang'] == 'python3'
    assert container_info['repl_in_port'] == '2000'
    assert container_info['repl_out_port'] == '2001'
    assert container_info['stdin_port'] == '2002'
    assert container_info['stdout_port'] == '2003'
    assert container_info['container_ip'] == '127.0.0.1'


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
        [{'HostPort': '2000'}],
        [{'HostPort': '2001'}],
        [{'HostPort': '2002'}],
        [{'HostPort': '2003'}],
        [{'HostPort': '2000'}],
        [{'HostPort': '2001'}],
        [{'HostPort': '2002'}],
        [{'HostPort': '2003'}],
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

    kernel_id = 'fake-kernel-id'
    config = {
        'lang': 'python3',
        'limits': {'cpu_slot': 1, 'gpu_slot': 0, 'mem_slot': 1},
        'mounts': [],
    }

    old_restarting_kernels = agent.restarting_kernels
    agent.restarting_kernels[kernel_id] = mock.Mock()
    agent.restarting_kernels[kernel_id].wait = asynctest.CoroutineMock()
    agent.container_registry[kernel_id] = {'core_set': [0, 1, 2, 3]}

    await agent._create_kernel(kernel_id, config)
    assert len(agent.restarting_kernels) > 0

    agent.restarting_kernels[kernel_id].wait.side_effect = asyncio.TimeoutError
    await agent._create_kernel(kernel_id, config, restarting=True)
    # assert len(agent.restarting_kernels) == 0  # kernel_id is deleted

    assert agent.restarting_kernels == old_restarting_kernels


@pytest.mark.asyncio
async def test__create_kernel_debug_with_minimal_confs(mock_agent, tmpdir):
    agent = mock_agent
    agent.debug_kernel = True
    mock_image_info = {
        'ContainerConfig': {
            'Labels': {}
        }
    }
    agent.config.exec_timeout = 10
    agent.config.volume_root = tmpdir

    kernel_id = 'fake-kernel-id'
    config = {
        'lang': 'python3',
        'limits': {'cpu_slot': 1, 'gpu_slot': 0, 'mem_slot': 1},
        'mounts': [],
    }
    ret = await agent._create_kernel_debug(kernel_id, config)

    assert ret['id'] == kernel_id
    assert ret['repl_in_port'] == 2000
    assert ret['repl_out_port'] == 2001
    assert ret['stdin_port'] == 2002
    assert ret['stdout_port'] == 2003
    container_info = agent.container_registry[kernel_id]
    assert container_info['lang'] == 'python3'
    assert container_info['repl_in_port'] == 2000
    assert container_info['repl_out_port'] == 2001
    assert container_info['stdin_port'] == 2002
    assert container_info['stdout_port'] == 2003
    assert container_info['container_ip'] == '127.0.0.1'
################################


@pytest.mark.asyncio
async def test__destroy_kernel(mocker, mock_agent):
    kernel_id, reason = 'fake-kernel-id', 'fake-reason'

    mock_collect_stats = mocker.patch('ai.backend.agent.server.collect_stats',
                                      new_callable=asynctest.CoroutineMock)
    mock_collect_stats.return_value = [None]

    agent = mock_agent
    agent.container_registry = {
        'fake-kernel-id': {
            'container_id': 'fake-container-id',
            'runner_tasks': set(),
        },
        'other-kernel-id': {
            'container_id': 'should-not-be-destroyed',
            'runner_tasks': set(),
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
    # assert 'last_stat' in agent.container_registry[kernel_id]
    assert 'last_stat' not in agent.container_registry['other-kernel-id']


@pytest.mark.asyncio
async def test__destroy_kernel_debug(mocker, mock_agent):
    kernel_id, reason = 'fake-kernel-id', 'fake-reason'

    mock_collect_stats = mocker.patch('ai.backend.agent.server.collect_stats',
                                      new_callable=asynctest.CoroutineMock)
    mock_collect_stats.return_value = [None]

    agent = mock_agent
    agent.container_registry = {
        'fake-kernel-id': {
            'container_id': 'fake-container-id',
            'runner_tasks': set(),
        },
        'other-kernel-id': {
            'container_id': 'should-not-be-destroyed',
            'runner_tasks': set(),
        }
    }

    assert 'last_stat' not in agent.container_registry[kernel_id]

    await agent._destroy_kernel_debug(kernel_id, reason)

    # mock_collect_stats.assert_called_once_with([mock_container])
    # assert 'last_stat' in agent.container_registry[kernel_id]
    assert 'last_stat' not in agent.container_registry['other-kernel-id']


###############################
# AgentRPCServer._execute_code
@pytest.mark.asyncio
@pytest.mark.parametrize('up_files', [True, False])
async def test__execute_code(mocker, mock_agent, tmpdir, up_files):
    entry_id = 'fake-entry-id'
    kernel_id = 'fake-kernel-id'
    run_id = 'fake-run-id'
    code_id = 'fake-code-id'
    code = 'print(0)'

    tmpdir.mkdir(kernel_id)

    agent = mock_agent
    agent.config.volume_root = tmpdir
    agent.container_registry = {
        kernel_id: {
            'container_id': 'fake-container-id',
            'container_ip': '127.0.0.1',
            'last_used': 0,
            'addr': 'fake-container-addr',
            'repl_in_port': 2000,
            'repl_out_port': 2001,
            'exec_timeout': 10,
            'initial_file_stats': {},
            'runner_tasks': set(),
        }
    }

    mock_upload_to_s3 = mocker.patch(
        'ai.backend.agent.server.upload_output_files_to_s3',
        new_callable=asynctest.CoroutineMock
    )
    mock_upload_to_s3.return_value = ['fake-uploaded-files']

    result_data = {
        'status': 'finished',
        'console': {
            'stdout': 'stdout',
            'stderr': 'stderr'
        },
        'options': {
            'upload_output_files': up_files,
        },
    }
    mock_runner = asynctest.CoroutineMock()
    mock_runner.attach_output_queue = asynctest.CoroutineMock()
    mock_runner.get_next_result = asynctest.CoroutineMock(
            return_value=result_data)
    agent._ensure_runner = asynctest.CoroutineMock(return_value=mock_runner)

    result = await agent._execute(2, kernel_id, run_id, 'query', code, {})

    assert agent.container_registry[kernel_id]['last_used'] != 0
    assert result['console']['stdout'] == 'stdout'
    assert result['console']['stderr'] == 'stderr'
    if up_files:
        assert 1 == mock_upload_to_s3.call_count
        assert 'fake-uploaded-files' in str(result['files'])
    else:
        assert 0 == mock_upload_to_s3.call_count
        assert result['files'] == []  # not 'fake-uploaded-files'
###############################


@pytest.mark.asyncio
async def test_heartbeat(mock_agent):
    agent = mock_agent
    agent.send_event = asynctest.CoroutineMock()
    await agent.heartbeat(5)
    assert agent.send_event.called == 1


@pytest.mark.asyncio
async def test_fetch_docker_events(mock_agent):
    agent = mock_agent
    agent.docker.events.run = asynctest.CoroutineMock(side_effect=[0, 1])
    await agent.fetch_docker_events()
    assert 3 == agent.docker.events.run.call_count  # 0, 1, raise err


@pytest.mark.asyncio
async def test_monitor_clean_kernel_upon_action_die(mock_agent):
    agent = mock_agent
    agent.docker = asynctest.CoroutineMock()
    mock_subscriber = agent.docker.events.subscribe.return_value = \
            asynctest.CoroutineMock()
    mock_subscriber.get = asynctest.CoroutineMock()
    mock_subscriber.get.side_effect = [{
        'Type': 'fake-type',
        'Action': 'die',
        'Actor': {
            'ID': 'fake-id',
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
            'cpu_set': '',
        },
    }
    mock_container = mock.Mock()
    mock_container.delete = asynctest.CoroutineMock()
    agent.docker = asynctest.CoroutineMock()
    agent.docker.containers.container.return_value = mock_container
    agent.config.volume_root = tmpdir
    agent.container_cpu_map.free = mock.Mock()
    agent.clean_runner = asynctest.CoroutineMock()

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

    await agent.clean_old_kernels('kernel-id-1')

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

    await agent.clean_old_kernels('kernel-id-1')

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


def test_main(mocker, tmpdir):
    import sys
    import aiotools
    from ai.backend.agent.server import main

    cmd = ['backend.ai-agent', '--scratch-root', '.']
    mocker.patch.object(sys, 'argv', cmd)
    mocker.patch.object(aiotools, 'start_server')

    main()

    assert aiotools.start_server.called == 1
