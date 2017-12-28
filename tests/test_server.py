import argparse
from ipaddress import ip_address
import os
import uuid

from unittest import mock

import asynctest
import pytest
# import simplejson as json
# from aiodocker.docker import Docker

# import ai.backend.agent.server as server_mod
# from ai.backend.agent.resources import CPUAllocMap
from ai.backend.agent.server import (
    get_extra_volumes, get_kernel_id_from_container, AgentRPCServer
)
from ai.backend.common import identity
from ai.backend.common.argparse import HostPortPair, host_port_pair


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


@pytest.fixture
def agent(request, tmpdir, event_loop):
    config = argparse.Namespace()
    config.namespace = 'local'
    config.agent_ip = '127.0.0.1'
    config.agent_port = '6001'  # default 6001
    config.etcd_addr = HostPortPair(ip_address('127.0.0.1'), 2379)
    config.idle_timeout = 600
    config.debug = True
    config.debug_kernel = False
    config.kernel_aliases = None
    config.scratch_root = tmpdir

    agent = None

    async def serve():
        nonlocal agent
        config.instance_id = await identity.get_instance_id()
        config.inst_type = await identity.get_instance_type()
        config.region = await identity.get_instance_region()
        print(f'serving test agent: {config.instance_id} ({config.inst_type}),'
              f' ip: {config.agent_ip}')
        agent = AgentRPCServer(config, loop=event_loop)
        await agent.init()
    event_loop.run_until_complete(serve())

    yield agent

    async def terminate():
        nonlocal agent
        print('shutting down test agent...')
        if agent:
            await agent.shutdown()
    event_loop.run_until_complete(terminate())


@pytest.mark.asyncio
async def test_get_extra_volumes(docker):
    # No extra volumes
    mnt_list = await get_extra_volumes(docker, 'python:latest')
    assert len(mnt_list) == 0

    # Create fake deeplearning sample volume and check it will be returned
    vol = None
    try:
        config = {'Name': 'deeplearning-samples'}
        vol = await docker.volumes.create(config)
        mnt_list = await get_extra_volumes(docker, 'python-tensorflow:latest')
    finally:
        if vol:
            await vol.delete()

    assert len(mnt_list) == 1
    assert mnt_list[0].name == 'deeplearning-samples'


@pytest.mark.asyncio
async def test_get_kernel_id_from_container(docker, container):
    container_list = await docker.containers.list()
    kid = get_kernel_id_from_container(container_list[0])

    assert kid == container_list[0]['Names'][0].split('.')[-1]


@pytest.mark.integration
class TestAgentRPCServerMethods:
    @pytest.fixture
    def kernel_info(self, agent, docker, event_loop):
        kernel_id = str(uuid.uuid4())
        config = {
            'lang': 'lua:latest',
            'limits': {'cpu_slot': 1, 'gpu_slot': 0, 'mem_slot': 1},
            'mounts': [],
        }

        kernel_info = None

        async def spawn():
            nonlocal kernel_info
            kernel_info = await agent.create_kernel(kernel_id, config)
        event_loop.run_until_complete(spawn())

        yield kernel_info

        async def finalize():
            nonlocal kernel_info
            if kernel_info['id'] in agent.container_registry:
                # Container id may be changed (e.g. restarting kernel), so we
                # should not rely on the initial value of the container_id.
                container_info = agent.container_registry[kernel_info['id']]
                container_id = container_info['container_id']
            else:
                # If fallback to initial container_id if kernel is deleted.
                container_id = kernel_info['container_id']
            container = docker.containers.container(container_id)
            cinfo = await container.show() if container else None
            if cinfo and cinfo['State']['Status'] != 'removing':
                await container.delete(force=True)
        event_loop.run_until_complete(finalize())

    def test_ping(self, agent):
        ret = agent.ping('ping~')
        assert ret == 'ping~'

    @pytest.mark.asyncio
    async def test_create_kernel(self, agent, docker):
        kernel_id = str(uuid.uuid4())
        config = {
            'lang': 'lua:latest',
            'limits': { 'cpu_slot': 1, 'gpu_slot': 0, 'mem_slot': 1 },
            'mounts': [],
        }

        kernel_info = container_info = None
        try:
            kernel_info = await agent.create_kernel(kernel_id, config)
            container_info = agent.container_registry[kernel_id]
        finally:
            container = docker.containers.container(kernel_info['container_id'])
            await container.delete(force=True)

        assert kernel_info
        assert container_info
        assert kernel_info['id'] == kernel_id
        assert len(kernel_info['cpu_set']) == 1
        assert container_info['lang'] == config['lang']
        assert container_info['container_id'] == kernel_info['container_id']
        assert container_info['limits'] == config['limits']
        assert container_info['mounts'] == config['mounts']

    @pytest.mark.asyncio
    async def test_destroy_kernel(self, agent, kernel_info):
        stat = await agent.destroy_kernel(kernel_info['id'])

        assert stat
        assert 'cpu_used' in stat
        assert 'mem_max_bytes' in stat
        assert 'mem_cur_bytes' in stat
        assert 'net_rx_bytes' in stat
        assert 'net_tx_bytes' in stat
        assert 'io_read_bytes' in stat
        assert 'io_write_bytes' in stat
        assert 'io_max_scratch_size' in stat
        assert 'io_cur_scratch_size' in stat

    @pytest.mark.asyncio
    async def test_restart_kernel(self, agent, kernel_info):
        kernel_id = kernel_info['id']
        container_id = kernel_info['container_id']
        new_config = {
            'lang': 'lua:latest',
            'limits': { 'cpu_slot': 1, 'gpu_slot': 0, 'mem_slot': 1 },
            'mounts': [],
        }

        ret = await agent.restart_kernel(kernel_id, new_config)

        assert container_id != ret['container_id']


    @pytest.mark.asyncio
    async def test_execute(self, agent, kernel_info):
        # Test with lua:latest image only
        api_ver = 2
        kernel_id = kernel_info['id']
        run_id = 'test-run-id'
        mode = 'query'
        code = 'print(17)'
        ret = await agent.execute(api_ver, kernel_id, run_id, mode, code, {})

        assert ret['status'] == 'finished'
        assert ret['console'][0][0] == 'stdout'
        assert ret['console'][0][1] == '17\n'


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


@pytest.mark.asyncio
async def test_upload_file(mocker, mock_agent):
    agent = mock_agent
    agent._accept_file = asynctest.CoroutineMock()

    await agent.upload_file(
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
