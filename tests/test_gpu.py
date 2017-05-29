import pathlib
import re

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


@pytest.fixture
def mock_nvidia_params():
    def _generate(num_gpus=1):
        data = {
            'Volumes': [
                'nvidia_driver_375.39:/usr/local/nvidia:ro',
                'nvidia_extra:/usr/local/nvidia-extra:ro',  # for testing
            ],
            'VolumeDriver': 'nvidia-docker',
            'Devices': [
                '/dev/nvidiactl',
                '/dev/nvidia-uvm',
                '/dev/nvidia-uvm-tools',
            ]
        }
        for i in range(num_gpus):
            data['Devices'].append(f'/dev/nvidia{i}')
        resp = MockResponse(data)
        return resp
    return _generate


@pytest.fixture
def mock_gpu_info():
    def _generate(num_gpus=1):
        # nvidia-docker returns UPPER-CASED Bus IDs!
        bus_ids = [
            '0000:00:1E.0',
            '0000:80:00.0',
            '0000:00:1F.0',
            '0000:81:00.0',
        ]
        data = {
            'Version': {
                'Driver': '375.39',
                'CUDA': '8.0',
            },
            'Devices': [],
        }
        for i in range(num_gpus):
            data['Devices'].append({
                'Model': 'Tesla K80',
                'Path': f'/dev/nvidia{i}',
                'PCI': {
                    'BusID': bus_ids[i],
                },
            })
        resp = MockResponse(data)
        return resp
    return _generate


@pytest.fixture
def mock_docker(mocker):
    def _generate(existing_volumes):
        m = mocker.MagicMock()
        m.volumes.list = asynctest.CoroutineMock()
        m.volumes.list.return_value = {
            'Volumes': [
                {'Name': v} for v in existing_volumes
            ]
        }
        return m
    return _generate

@pytest.mark.asyncio
async def test_prepare_nvidia_no_nvdocker(mocker, mock_docker):

    mocked_requests = mocker.patch.object(requests, 'get')
    mocked_requests.side_effect = requests.exceptions.ConnectionError
    mocked_docker = mock_docker(existing_volumes=[])

    with pytest.raises(RuntimeError):
        await prepare_nvidia(mocked_docker, 0)


@pytest.mark.asyncio
async def test_prepare_nvidia_no_gpu(
        mocker,
        mock_nvidia_params,
        mock_gpu_info,
        mock_docker):

    numa_node = 0
    num_gpus = 0

    mocked_requests = mocker.patch.object(requests, 'get')
    mocked_nvparams = mock_nvidia_params(num_gpus)
    mocked_gpuinfo = mock_gpu_info(num_gpus)
    mocked_requests.side_effect = [mocked_nvparams, mocked_gpuinfo]

    mocked_docker = mock_docker(existing_volumes=['x'])  # no existing volumes
    mocked_docker_volcreate = mocked_docker.volumes.create = asynctest.CoroutineMock()

    mocked_path = mocker.patch('sorna.agent.gpu.Path')
    mocked_path_obj = mocker.MagicMock()
    mocked_path_obj.read_text.return_value = '-1'
    mocked_path.return_value = mocked_path_obj

    binds, devices = await prepare_nvidia(mocked_docker, numa_node)

    # Should be mounted the common nvidia device files.
    assert len(devices) == 3 + num_gpus

    # Check if all nvidia volumes are returned.
    assert set(binds) == set(mocked_nvparams.json()['Volumes'])


@pytest.mark.asyncio
async def test_prepare_nvidia_no_numa(
        mocker,
        mock_nvidia_params,
        mock_gpu_info,
        mock_docker):

    numa_node = 0
    num_gpus = 2

    mocked_requests = mocker.patch.object(requests, 'get')
    mocked_nvparams = mock_nvidia_params(num_gpus)
    mocked_gpuinfo = mock_gpu_info(num_gpus)
    mocked_requests.side_effect = [mocked_nvparams, mocked_gpuinfo]

    mocked_docker = mock_docker(existing_volumes=['x'])  # no existing volumes
    mocked_docker_volcreate = mocked_docker.volumes.create = asynctest.CoroutineMock()

    mocked_path = mocker.patch('sorna.agent.gpu.Path')
    mocked_path_obj = mocker.MagicMock()
    mocked_path_obj.read_text.return_value = '-1'
    mocked_path.return_value = mocked_path_obj

    binds, devices = await prepare_nvidia(mocked_docker, numa_node)

    # Check if all devices are added.
    assert len(devices) == 3 + num_gpus

    # Check if all nvidia volumes are created.
    assert mocked_docker_volcreate.call_count == len(mocked_nvparams.json()['Volumes'])

    # Check if all nvidia volumes are returned.
    assert set(binds) == set(mocked_nvparams.json()['Volumes'])

    # Ensure if the PCI bus ID is properly included AND lower-cased.
    mocked_path.assert_any_call('/sys/bus/pci/devices/0000:00:1e.0/numa_node')
    mocked_path.assert_any_call('/sys/bus/pci/devices/0000:80:00.0/numa_node')


@pytest.mark.asyncio
async def test_prepare_nvidia_no_numa2(
        mocker,
        mock_nvidia_params,
        mock_gpu_info,
        mock_docker):

    numa_node = 0
    num_gpus = 2

    mocked_requests = mocker.patch.object(requests, 'get')
    mocked_nvparams = mock_nvidia_params(num_gpus)
    mocked_gpuinfo = mock_gpu_info(num_gpus)
    mocked_requests.side_effect = [mocked_nvparams, mocked_gpuinfo]

    mocked_docker = mock_docker(existing_volumes=['x'])  # no existing volumes
    mocked_docker_volcreate = mocked_docker.volumes.create = asynctest.CoroutineMock()

    # For old systems without NUMA, the sysfs may not have numa_node files!
    mocked_path = mocker.patch('sorna.agent.gpu.Path')
    mocked_path_obj = mocker.MagicMock()
    mocked_path_obj.read_text.side_effect = FileNotFoundError
    mocked_path.return_value = mocked_path_obj

    binds, devices = await prepare_nvidia(mocked_docker, numa_node)

    # Check if all devices are added.
    assert len(devices) == 3 + num_gpus

    # Check if all nvidia volumes are created.
    assert mocked_docker_volcreate.call_count == len(mocked_nvparams.json()['Volumes'])

    # Check if all nvidia volumes are returned.
    assert set(binds) == set(mocked_nvparams.json()['Volumes'])

    # Ensure if the PCI bus ID is properly included AND lower-cased.
    mocked_path.assert_any_call('/sys/bus/pci/devices/0000:00:1e.0/numa_node')
    mocked_path.assert_any_call('/sys/bus/pci/devices/0000:80:00.0/numa_node')


@pytest.mark.asyncio
async def test_prepare_nvidia_existing_vols(
        mocker,
        mock_nvidia_params,
        mock_gpu_info,
        mock_docker):

    numa_node = 0
    num_gpus = 1

    mocked_requests = mocker.patch.object(requests, 'get')
    mocked_nvparams = mock_nvidia_params(num_gpus)
    mocked_gpuinfo = mock_gpu_info(num_gpus)
    mocked_requests.side_effect = [mocked_nvparams, mocked_gpuinfo]

    # Simulate that "nvidia_extra" volume already exists.
    mocked_docker = mock_docker(existing_volumes=['x', 'nvidia_extra'])
    mocked_docker_volcreate = mocked_docker.volumes.create = asynctest.CoroutineMock()

    mocked_path = mocker.patch('sorna.agent.gpu.Path')
    mocked_path_obj = mocker.MagicMock()
    mocked_path_obj.read_text.return_value = '0'
    mocked_path.return_value = mocked_path_obj

    binds, devices = await prepare_nvidia(mocked_docker, numa_node)

    # Check if all devices are added.
    assert len(devices) == 3 + num_gpus

    # Check if only non-existing nvidia volumes were created.
    assert mocked_docker_volcreate.call_count == len(mocked_nvparams.json()['Volumes']) - 1
    assert mocked_docker_volcreate.call_args[0][0]['Name'] == 'nvidia_driver_375.39'

    # Check if all nvidia volumes are returned.
    assert set(binds) == set(mocked_nvparams.json()['Volumes'])

    # Ensure if the PCI bus ID is properly included AND lower-cased.
    mocked_path.assert_any_call('/sys/bus/pci/devices/0000:00:1e.0/numa_node')


@pytest.mark.asyncio
@pytest.mark.parametrize('numa_node', [0, 1])
async def test_prepare_nvidia_numa_multinode(
        mocker,
        mock_nvidia_params,
        mock_gpu_info,
        mock_docker,
        numa_node):

    num_gpus = 4

    mocked_requests = mocker.patch.object(requests, 'get')
    mocked_nvparams = mock_nvidia_params(num_gpus)
    mocked_gpuinfo = mock_gpu_info(num_gpus)
    mocked_requests.side_effect = [mocked_nvparams, mocked_gpuinfo]

    mocked_docker = mock_docker(existing_volumes=['x'])  # no existing volumes
    mocked_docker_volcreate = mocked_docker.volumes.create = asynctest.CoroutineMock()

    mocked_path = mocker.patch('sorna.agent.gpu.Path')
    mocked_path_obj = mocker.MagicMock()

    def get_node_of_gpu():
        if re.search(r'0000:00:1[ef]\.0', mocked_path.call_args[0][0]):
            return '0'
        elif re.search(r'0000:8[01]:00\.0', mocked_path.call_args[0][0]):
            return '1'
        else:
            assert False, 'unrecognized PCI bus address for testing'

    mocked_path_obj.read_text.side_effect = get_node_of_gpu
    mocked_path.return_value = mocked_path_obj

    binds, devices = await prepare_nvidia(mocked_docker, numa_node)

    # Check if only devices in the same NUMA node are added.
    assert len(devices) == 3 + (num_gpus / 2)

    # Check if all nvidia volumes are created.
    assert mocked_docker_volcreate.call_count == len(mocked_nvparams.json()['Volumes'])

    # Check if all nvidia volumes are returned.
    assert set(binds) == set(mocked_nvparams.json()['Volumes'])

    # Ensure if the PCI bus ID is properly included AND lower-cased.
    mocked_path.assert_any_call('/sys/bus/pci/devices/0000:00:1e.0/numa_node')
    mocked_path.assert_any_call('/sys/bus/pci/devices/0000:00:1f.0/numa_node')
    mocked_path.assert_any_call('/sys/bus/pci/devices/0000:80:00.0/numa_node')
    mocked_path.assert_any_call('/sys/bus/pci/devices/0000:81:00.0/numa_node')
