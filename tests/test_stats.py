import asyncio
import os
from pathlib import Path
from typing import Mapping, Union
import sys

import pytest
import zmq
import zmq.asyncio

from ai.backend.common import msgpack
from ai.backend.common.config import find_config_file
from ai.backend.agent import defs
from ai.backend.agent import stats


testing_ipc_base_path = Path('/tmp/backend.ai-testing')  # type: ignore


@pytest.fixture
def stats_server(mocker):
    mocker.patch('ai.backend.agent.stats.ipc_base_path', testing_ipc_base_path)
    mocker.patch.object(defs, 'ipc_base_path', testing_ipc_base_path)
    defs.ipc_base_path.mkdir(parents=True, exist_ok=True)
    zctx = zmq.asyncio.Context()
    stat_sync_sockpath = defs.ipc_base_path / f'test-stats.{os.getpid()}.sock'
    stat_sync_sock = zctx.socket(zmq.REP)
    stat_sync_sock.setsockopt(zmq.LINGER, 1000)
    stat_sync_sock.bind('ipc://' + str(stat_sync_sockpath))
    try:
        yield stat_sync_sock, stat_sync_sockpath
    finally:
        stat_sync_sock.close()
        stat_sync_sockpath.unlink()
        zctx.term()


active_stat_modes = [stats.StatModes.DOCKER]
if sys.platform.startswith('linux') and os.geteuid() == 0:
    active_stat_modes.append(stats.StatModes.CGROUP)

pipe_opts: Mapping[str, Union[None, int]] = {
    'stdout': None,
    'stderr': None,
    'stdin': asyncio.subprocess.DEVNULL,
}


async def recv_deserialized(sock):
    msg = await sock.recv_multipart()
    return [*map(msgpack.unpackb, msg)]


async def send_serialized(sock, msg):
    encoded_msg = [*map(msgpack.packb, msg)]
    await sock.send_multipart(encoded_msg)


def test_numeric_list():
    s = '1 3 5 7'
    ret = stats.numeric_list(s)
    assert ret == [1, 3, 5, 7]

    s = ''
    ret = stats.numeric_list(s)
    assert ret == []

    s = '123\n456'
    ret = stats.numeric_list(s)
    assert ret == [123, 456]


@pytest.mark.skipif('TRAVIS' in os.environ, reason='This test hangs in the Travis CI env.')
@pytest.mark.asyncio
@pytest.mark.parametrize('stat_mode', active_stat_modes)
async def test_synchronizer(event_loop,
                            create_container,
                            stats_server,
                            stat_mode):

    # Create the container but don't start it.
    container = await create_container({
        'Image': 'nginx:1.17-alpine',
        'ExposedPorts': {
            '80/tcp': {},
        },
        'HostConfig': {
            'PortBindings': {
                '80/tcp': [{'HostPort': '8080'}],
            }
        }
    })
    cid = container['id']

    # Initialize the agent-side.
    stat_sync_sock, stat_sync_sockpath = stats_server
    config_path = find_config_file('agent')
    log_endpoint = ''

    proc = None
    async with stats.spawn_stat_synchronizer(config_path, stat_sync_sockpath,
                                             stat_mode, cid,
                                             log_endpoint,
                                             exec_opts=pipe_opts) as p:
        proc = p
        await container.start()

    # Proceed to receive stats.
    async def kill_after_sleep():
        await asyncio.sleep(2.0)
        await container.kill()

    t = event_loop.create_task(kill_after_sleep())
    msg_list = []
    while True:
        msg = (await recv_deserialized(stat_sync_sock))[0]
        await send_serialized(stat_sync_sock, [{'ack': True}])
        msg_list.append(msg)
        if msg['status'] == 'terminated':
            break

    await proc.wait()
    await t  # for explicit clean up

    assert proc.returncode == 0
    assert len(msg_list) >= 1
    assert msg_list[0]['cid'] == cid
    assert msg_list[-1]['cid'] == cid
    assert msg_list[-1]['status'] == 'terminated'


@pytest.mark.skipif('TRAVIS' in os.environ, reason='This test hangs in the Travis CI env.')
@pytest.mark.asyncio
@pytest.mark.parametrize('stat_mode', active_stat_modes)
async def test_synchronizer_immediate_death(event_loop,
                                            create_container,
                                            stats_server,
                                            stat_mode):

    container = await create_container({
        'Cmd': ['-c', 'exit 0'],
        'Entrypoint': 'sh',
        'Image': 'alpine:3.8',
    })
    cid = container['id']

    # Initialize the agent-side.
    stat_sync_sock, stat_sync_sockpath = stats_server
    config_path = find_config_file('agent')
    log_endpoint = ''

    proc = None
    async with stats.spawn_stat_synchronizer(config_path, stat_sync_sockpath,
                                             stat_mode, cid,
                                             log_endpoint,
                                             exec_opts=pipe_opts) as p:
        proc = p
        await container.start()

    # Let it die first!
    await container.wait()

    # Proceed to receive stats.
    msg_list = []
    while True:
        msg = (await recv_deserialized(stat_sync_sock))[0]
        await send_serialized(stat_sync_sock, [{'ack': True}])
        if msg is None:
            break
        msg_list.append(msg)
        if msg['status'] == 'terminated':
            break

    await proc.wait()

    assert proc.returncode in (0, 1)
    assert len(msg_list) >= 1
    assert msg_list[0]['cid'] == cid
    assert msg_list[-1]['cid'] == cid
    assert msg_list[-1]['status'] == 'terminated'
