import asyncio
import functools
import os
import sys

import pytest
import zmq
from zmq.asyncio import Context as AsyncZmqContext

from ai.backend.common import msgpack
from ai.backend.agent import stats


@pytest.fixture
def stats_server():
    context = AsyncZmqContext.instance()
    stats_sock = context.socket(zmq.PULL)
    stats_port = stats_sock.bind_to_random_port('tcp://127.0.0.1')
    try:
        yield stats_sock, stats_port
    finally:
        stats_sock.close()


active_collection_types = ['api']
if sys.platform.startswith('linux'):
    active_collection_types.append('cgroup')

output_opts = {
    'stdout': None,
    'stderr': None,
}
if 'TRAVIS' in os.environ:
    output_opts = {
        'stdout': asyncio.subprocess.DEVNULL,
        'stderr': asyncio.subprocess.DEVNULL,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize('collection_type', active_collection_types)
async def test_collector(event_loop,
                         daemon_container,
                         stats_server,
                         collection_type):
    cid = daemon_container['id']
    stats_sock, stats_port = stats_server
    proc = await asyncio.create_subprocess_exec(*[
        'python', '-m', 'ai.backend.agent.stats',
        f'tcp://127.0.0.1:{stats_port}',
        cid,
        '--type', collection_type,
    ], **output_opts)

    async def kill_after_sleep():
        await asyncio.sleep(2.0)
        await daemon_container.kill()

    t = event_loop.create_task(kill_after_sleep())
    recv = functools.partial(
        stats_sock.recv_serialized,
        lambda vs: [msgpack.unpackb(v) for v in vs])
    msg_list = []
    while True:
        msg = (await recv())[0]
        msg_list.append(msg)
        if msg['status'] == 'terminated':
            break

    await proc.wait()
    await t  # for explicit clean up

    assert proc.returncode == 0
    assert len(msg_list) >= 1
    assert msg_list[0]['cid'] == cid
    assert msg_list[0]['data'] is not None


@pytest.mark.asyncio
@pytest.mark.parametrize('collection_type', active_collection_types)
async def test_collector_immediate_death_1(event_loop,
                                           create_container,
                                           stats_server,
                                           collection_type):
    container = await create_container({
        'Cmd': ['-c', 'exit 0'],
        'Entrypoint': 'sh',
        'Image': 'alpine:latest',
    })

    # Start before the collector is spawned.
    await container.start()
    await asyncio.sleep(0.5)

    cid = container['id']
    stats_sock, stats_port = stats_server
    proc = await asyncio.create_subprocess_exec(*[
        'python', '-m', 'ai.backend.agent.stats',
        f'tcp://127.0.0.1:{stats_port}',
        cid,
        '--type', collection_type,
    ], **output_opts)

    recv = functools.partial(
        stats_sock.recv_serialized,
        lambda vs: [msgpack.unpackb(v) for v in vs])
    msg_list = []
    while True:
        msg = (await recv())[0]
        msg_list.append(msg)
        if msg['status'] == 'terminated':
            break

    await proc.wait()

    assert proc.returncode == 0
    assert len(msg_list) >= 1
    assert msg_list[0]['cid'] == cid


@pytest.mark.asyncio
@pytest.mark.parametrize('collection_type', active_collection_types)
async def test_collector_immediate_death_2(event_loop,
                                           create_container,
                                           stats_server,
                                           collection_type):
    container = await create_container({
        'Cmd': ['-c', 'exit 0'],
        'Entrypoint': 'sh',
        'Image': 'alpine:latest',
    })

    cid = container['id']
    stats_sock, stats_port = stats_server
    proc = await asyncio.create_subprocess_exec(*[
        'python', '-m', 'ai.backend.agent.stats',
        f'tcp://127.0.0.1:{stats_port}',
        cid,
        '--type', collection_type,
    ], **output_opts)

    # Start after the collector is spawned.
    await asyncio.sleep(0.5)
    await container.start()

    recv = functools.partial(
        stats_sock.recv_serialized,
        lambda vs: [msgpack.unpackb(v) for v in vs])
    msg_list = []
    while True:
        msg = (await recv())[0]
        msg_list.append(msg)
        if msg['status'] == 'terminated':
            break

    await proc.wait()

    assert proc.returncode == 0
    assert len(msg_list) >= 1
    assert msg_list[0]['cid'] == cid


@pytest.mark.skip(reason='way to test exact numbers container used')
@pytest.mark.asyncio
async def test_collect_stats(container):
    ret = await stats.collect_stats([container])

    assert 'cpu_used' in ret[0]
    assert 'mem_max_bytes' in ret[0]
    assert 'mem_cur_bytes' in ret[0]
    assert 'net_rx_bytes' in ret[0]
    assert 'net_tx_bytes' in ret[0]
    assert 'io_read_bytes' in ret[0]
    assert 'io_write_bytes' in ret[0]
    assert 'io_max_scratch_size' in ret[0]
    assert 'io_cur_scratch_size' in ret[0]


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


def test_read_sysfs(tmpdir):
    p = tmpdir.join('test.txt')
    p.write('1357')
    ret = stats.read_sysfs(p)

    assert isinstance(ret, int)
    assert ret == 1357
