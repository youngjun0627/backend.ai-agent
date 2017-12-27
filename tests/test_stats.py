import asyncio

import pytest

from ai.backend.agent import stats


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


def test_read_sysfs(tmpdir):
    p = tmpdir.join('test.txt')
    p.write('1357')
    ret = stats.read_sysfs(p)

    assert isinstance(ret, int)
    assert ret == 1357
