from pathlib import Path
from unittest import mock

import asynctest
import pytest
import requests

from sorna.agent.gpu import prepare_nvidia


class MockResponse:
    def __init__(self, json_data=None, status_code=200):
        if json_data is None:
            json_data = {}
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


@pytest.mark.asyncio
class TestPrepareNvidia:
    @pytest.mark.parametrize('numa_node', [1, 2])
    async def test_prepare_nvidia(self, mocker, numa_node):
        # Mocking parameters
        mock_nvidia_params = MockResponse({
            'Volumes': [
                'volname1:mtpoint1:perm1',
                'volname2:mtpoint2:perm2',
                'volname3:mtpoint3:perm3',
                'volname4:mtpoint4:perm4',
            ],
            'VolumeDriver': 'volumedriver',
            'Devices': [
                '/dev/other1',
                '/dev/other2',
                '/dev/nvidia1',
                '/dev/nvidia2',
            ]
        })
        mock_gpu_info = MockResponse({
            'Devices': [
                {
                    'Path': '/dev/nvidia1',
                    'PCI': {
                        'BusID': 'fakeid1'
                    }
                },
                {
                    'Path': '/dev/nvidia2',
                    'PCI': {
                        'BusID': 'fakeid2'
                    }
                }
            ]
        })
        mock_requests = mocker.patch.object(requests, 'get')
        mock_requests.side_effect = [mock_nvidia_params, mock_gpu_info]

        # Mocking docker object
        mock_docker = mock.Mock()
        mock_docker.volumes.list = asynctest.CoroutineMock()
        mock_docker.volumes.list.return_value = {
            'Volumes': [
                {
                    'Name': 'volname1'
                },
                {
                    'Name': 'volname2'
                },
            ]
        }
        mock_create = mock_docker.volumes.create = asynctest.CoroutineMock()

        # Mock numa node of all gpus as 1
        mock_readtext = mocker.patch.object(Path, 'read_text')
        mock_readtext.return_value = '1'

        binds, devices = await prepare_nvidia(mock_docker, numa_node)

        assert mock_create.call_count == 2  # two missing volumes
        assert set(binds) == set(mock_nvidia_params.json()['Volumes'])
        if numa_node == 1:  # in the same numa node, so all 4 devices
            assert len(devices) == 4
        else:  # other numa node, just 2 devices
            assert len(devices) == 2
