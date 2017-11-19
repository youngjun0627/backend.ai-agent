from pathlib import Path
import sys
from unittest import mock

import asynctest
import pytest

from ai.backend.agent.stats import (
    _collect_stats_sysfs, _collect_stats_api, collect_stats, read_sysfs
)


@pytest.fixture
def mock_container():
    mock_container = mock.Mock()
    mock_container._id = 'fakeid'
    mock_container.stats = asynctest.CoroutineMock()
    mock_container.stats.return_value = {
        'cpu_stats': {
            'cpu_usage': {
                'total_usage': 1e5,
            }
        },
        'memory_stats': {
            'max_usage': 1000,
        },
        'blkio_stats': {
            'io_service_bytes_recursive': [
                {
                    'op': 'Read',
                    'value': 10,
                },
                {
                    'op': 'Write',
                    'value': 20,
                },
            ]
        },
        'networks': {
            '_': {
                'rx_bytes': 30,
                'tx_bytes': 40,
            },
            '+': {
                'rx_bytes': 50,
                'tx_bytes': 60,
            },
        }
    }

    return mock_container


@pytest.mark.skipif(sys.platform != 'linux',
                    reason="sysfs is only available in Linux")
def test_collect_stats_sysfs(mocker, mock_container):
    mock_read_sysfs = mocker.patch('ai.backend.agent.stats.read_sysfs')
    mock_read_sysfs.side_effect = [1e5, 1024, 16]

    stat = _collect_stats_sysfs(mock_container)

    assert stat['cpu_used'] == 1e5 / 1e6
    assert stat['mem_max_bytes'] == 1024


@pytest.mark.asyncio
async def test_collect_stats_api(mock_container):
    stat = await _collect_stats_api(mock_container)

    assert stat['cpu_used'] == 1e5 / 1e6
    assert stat['mem_max_bytes'] == 1000
    assert stat['net_rx_bytes'] == 80
    assert stat['net_tx_bytes'] == 100
    assert stat['io_read_bytes'] == 10
    assert stat['io_write_bytes'] == 20


@pytest.mark.asyncio
async def test_collect_stats(mocker):
    mock_sys = mocker.patch('ai.backend.agent.stats.sys')
    mock_sysfs = mocker.patch('ai.backend.agent.stats._collect_stats_sysfs')
    mock_api = mocker.patch('ai.backend.agent.stats._collect_stats_api')

    mock_sysfs.assert_not_called()
    mock_api.assert_not_called()

    # On linux platform, _collect_stats_sysfs should be called.
    mock_sys.platform = 'linux'
    await collect_stats([mock.Mock()])

    mock_sysfs.assert_called()
    sysfs_call_count = mock_sysfs.call_count
    mock_api.assert_not_called()

    # Else, _collect_stats_api should be called.
    mock_sys.platform = '!linux'
    with pytest.raises(TypeError):
        await collect_stats([mock.Mock()])

    assert mock_sysfs.call_count == sysfs_call_count


def test_read_sysfs(mocker):
    mocker.patch.object(Path, 'read_text', return_value='2')
    sysfs = read_sysfs('mock/path')

    assert isinstance(sysfs, int)
    assert sysfs == 2
