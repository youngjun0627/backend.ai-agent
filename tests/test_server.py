from unittest import mock

import asynctest
import pytest


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
