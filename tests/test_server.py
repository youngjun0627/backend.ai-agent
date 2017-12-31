import argparse
import asyncio
from datetime import datetime
from ipaddress import ip_address
import os
from pathlib import Path
import time
import uuid
from unittest import mock

import aiodocker
import asynctest
import pytest

from ai.backend.agent.server import (
    get_extra_volumes, get_kernel_id_from_container, AgentRPCServer
)
from ai.backend.common import identity
from ai.backend.common.argparse import HostPortPair, host_port_pair


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
    config.scratch_root = Path(tmpdir)

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
            try:
                container = docker.containers.container(container_id)
                cinfo = await container.show() if container else None
            except aiodocker.exceptions.DockerError:
                cinfo = None
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
            'limits': {'cpu_slot': 1, 'gpu_slot': 0, 'mem_slot': 1},
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
            'limits': {'cpu_slot': 1, 'gpu_slot': 0, 'mem_slot': 1},
            'mounts': [],
        }

        ret = await agent.restart_kernel(kernel_id, new_config)

        assert container_id != ret['container_id']

    @pytest.mark.asyncio
    async def test_restart_kernel_cancel_code_execution(
            self, agent, kernel_info, event_loop):
        async def execute_code():
            nonlocal kernel_info
            api_ver = 2
            kid = kernel_info['id']
            runid = 'test-run-id'
            mode = 'query'
            code = ('local clock = os.clock\n'
                    'function sleep(n)\n'
                    '  local t0 = clock()\n'
                    '  while clock() - t0 <= n do end\n'
                    'end\n'
                    'sleep(10)\nprint("code executed")')
            while True:
                ret = await agent.execute(api_ver, kid, runid, mode, code, {})
                if ret is None:
                    break;
                elif ret['status'] == 'finished':
                    break
                elif ret['status'] == 'continued':
                    mode = 'continue',
                    code = ''
                else:
                    raise Exception('Invalid execution status')
            return ret

        async def restart_kernel():
            nonlocal kernel_info
            kernel_id = kernel_info['id']
            container_id = kernel_info['container_id']
            new_config = {
                'lang': 'lua:latest',
                'limits': {'cpu_slot': 1, 'gpu_slot': 0, 'mem_slot': 1},
                'mounts': [],
            }
            ret = await agent.restart_kernel(kernel_id, new_config)

        t1 = asyncio.ensure_future(execute_code(), loop=event_loop)
        start = datetime.now()
        await asyncio.sleep(1)
        t2 = asyncio.ensure_future(restart_kernel(), loop=event_loop)
        results = await asyncio.gather(t1, t2)
        end = datetime.now()

        assert results[0] is None  # no execution result
        assert (end - start).total_seconds() < 10

    @pytest.mark.asyncio
    async def test_execute(self, agent, kernel_info):
        # Test with lua:latest image only
        api_ver = 2
        kid = kernel_info['id']
        runid = 'test-run-id'
        mode = 'query'
        code = 'print(17)'

        while True:
            ret = await agent.execute(api_ver, kid, runid, mode, code, {})
            if ret['status'] == 'finished':
                break
            elif ret['status'] == 'continued':
                mode = 'continue',
                code = ''
            else:
                raise Exception('Invalid execution status')

        assert ret['console'][0][0] == 'stdout'
        assert ret['console'][0][1] == '17\n'

    @pytest.mark.asyncio
    async def test_upload_file(self, agent, kernel_info):
        fname = 'test.txt'
        await agent.upload_file(kernel_info['id'], fname, b'test content')
        uploaded_to = agent.config.scratch_root / kernel_info['id'] / fname
        assert uploaded_to.exists()

    @pytest.mark.asyncio
    async def test_reset(self, agent, docker):
        kernel_ids = []
        container_ids = []
        config = {
            'lang': 'lua:latest',
            'limits': {'cpu_slot': 1, 'gpu_slot': 0, 'mem_slot': 1},
            'mounts': [],
        }

        try:
            # Create two kernels
            for i in range(2):
                kid = str(uuid.uuid4())
                kernel_ids.append(kid)
                info = await agent.create_kernel(kid, config)
                container_ids.append(info['container_id'])

            # 2 containers are created
            assert docker.containers.container(container_ids[0])
            assert docker.containers.container(container_ids[1])

            await agent.reset()

            # Containers are destroyed
            with pytest.raises(aiodocker.exceptions.DockerError):
                c1 = docker.containers.container(container_ids[0])
                c1info = await c1.show()
                if c1info['State']['Status'] == 'removing':
                    raise aiodocker.exceptions.DockerError(
                            404, {'message': 'success'})
            with pytest.raises(aiodocker.exceptions.DockerError):
                c2 = docker.containers.container(container_ids[1])
                c2info = await c2.show()
                if c2info['State']['Status'] == 'removing':
                    raise aiodocker.exceptions.DockerError(
                            404, {'message': 'success'})
        finally:
            for cid in container_ids:
                try:
                    container = docker.containers.container(cid)
                    cinfo = await container.show() if container else None
                except aiodocker.exceptions.DockerError:
                    cinfo = None
                if cinfo and cinfo['State']['Status'] != 'removing':
                    await container.delete(force=True)


def test_main(mocker, tmpdir):
    import sys
    import aiotools
    from ai.backend.agent.server import main

    cmd = ['backend.ai-agent', '--scratch-root', '.']
    mocker.patch.object(sys, 'argv', cmd)
    mocker.patch.object(aiotools, 'start_server')

    main()

    assert aiotools.start_server.called == 1
