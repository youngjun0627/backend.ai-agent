import asyncio
import os
import sys

import pytest
import zmq
import zmq.asyncio

from ai.backend.common import msgpack
from ai.backend.agent import stats


@pytest.fixture
def stats_server():
    context = zmq.asyncio.Context()
    stats_sock = context.socket(zmq.PULL)
    stats_port = stats_sock.bind_to_random_port('tcp://127.0.0.1')
    try:
        yield stats_sock, stats_port
    finally:
        stats_sock.close()
        context.term()


active_collection_types = ['api']
if sys.platform.startswith('linux'):
    active_collection_types.append('cgroup')

pipe_opts = {
    'stdout': None,
    'stderr': None,
    'stdin': asyncio.subprocess.DEVNULL,
}
if 'TRAVIS' in os.environ:
    pipe_opts = {
        'stdout': asyncio.subprocess.DEVNULL,
        'stderr': asyncio.subprocess.DEVNULL,
        'stdin': asyncio.subprocess.DEVNULL,
    }


async def recv_deserialized(sock):
    msg = await sock.recv_multipart()
    return [msgpack.unpackb(v) for v in msg]


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


@pytest.mark.asyncio
@pytest.mark.parametrize('collection_type', active_collection_types)
async def test_collector(event_loop,
                         create_container,
                         stats_server,
                         collection_type):

    # Create the container but don't start it.
    container = await create_container({
        'Image': 'nginx:latest',
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
    stats_sock, stats_port = stats_server

    # Spawn the collector and wait for its initialization.
    stat_addr = f'tcp://127.0.0.1:{stats_port}'

    proc = None
    async with stats.spawn_stat_collector(stat_addr, collection_type, cid,
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
        msg = (await recv_deserialized(stats_sock))[0]
        print(msg)
        msg_list.append(msg)
        if msg['status'] == 'terminated':
            break
    await proc.wait()
    await t  # for explicit clean up

    assert proc.returncode == 0
    assert len(msg_list) >= 1
    assert msg_list[0]['cid'] == cid
    assert msg_list[0]['status'] in ('running', 'terminated')
    assert msg_list[0]['data'] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize('collection_type', active_collection_types)
async def test_collector_immediate_death(event_loop,
                                           create_container,
                                           stats_server,
                                           collection_type):
    container = await create_container({
        'Cmd': ['-c', 'exit 0'],
        'Entrypoint': 'sh',
        'Image': 'alpine:latest',
    })
    cid = container['id']

    # Initialize the agent-side.
    stats_sock, stats_port = stats_server

    # Spawn the collector and wait for its initialization.
    stat_addr = f'tcp://127.0.0.1:{stats_port}'

    proc = None
    async with stats.spawn_stat_collector(stat_addr, collection_type, cid,
                                          exec_opts=pipe_opts) as p:
        proc = p
        await container.start()

    # Let it die first!
    await container.wait()

    # Proceed to receive stats.
    msg_list = []
    while True:
        msg = (await recv_deserialized(stats_sock))[0]
        print(msg)
        msg_list.append(msg)
        if msg['status'] == 'terminated':
            break

    await proc.wait()

    assert proc.returncode == 0
    assert len(msg_list) >= 1
    assert msg_list[0]['cid'] == cid
    assert msg_list[0]['data'] is not None
