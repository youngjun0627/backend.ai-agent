import logging
import subprocess
from pathlib import Path
import re
from typing import Collection, Sequence

import requests

from .accelerator import AbstractAccelerator, ProcessorIdType
from .vendor.linux import libnuma
from .vendor.nvidia import libcudart

log = logging.getLogger('ai.backend.agent.gpu')


class CUDAAccelerator(AbstractAccelerator):

    slot_key = 'gpu'  # TODO: generalize as 'cuda-gpu'

    nvdocker_version = (0, 0, 0)
    rx_nvdocker_version = re.compile(r'^NVIDIA Docker: (\d+\.\d+\.\d+)')

    @classmethod
    def slots(cls, limit_gpus=None) -> float:
        try:
            ret = subprocess.run(['nvidia-docker', 'version'],
                                 stdout=subprocess.PIPE)
        except FileNotFoundError:
            log.info('nvidia-docker is not installed.')
            return 0
        rx = cls.rx_nvdocker_version
        for line in ret.stdout.decode().strip().splitlines():
            m = rx.search(line)
            if m is not None:
                cls.nvdocker_version = tuple(map(int, m.group(1).split('.')))
        if cls.nvdocker_version[0] == 1:
            try:
                r = requests.get('http://localhost:3476/gpu/info/json', timeout=0.5)
                gpu_info = r.json()
            except requests.exceptions.ConnectionError:
                return 0
            return min(len(limit_gpus), len(gpu_info['Devices']))
        elif cls.nvdocker_version[0] == 2:
            num_devices = libcudart.get_device_count()
            return min(len(limit_gpus), num_devices)
        elif cls.nvdocker_version[0] == 0:
            log.info('nvidia-docker is not available!')
        else:
            vstr = '{0[0]}.{0[1]}.{0[2]}'.format(cls.nvdocker_version)
            log.warning(f'Unsupported nvidia docker version: {vstr}')
        return 0

    @classmethod
    def list_devices(cls) -> Sequence[Collection[ProcessorIdType]]:
        devices_per_nodes = [[] for _ in range(libnuma.num_nodes())]
        num_devices = libcudart.get_device_count()
        for dev_idx in range(num_devices):
            dev_info = libcudart.get_device_props(dev_idx)
            sysfs_node_path = "/sys/bus/pci/devices/" \
                              f"{dev_info['pciBusID_str']}/numa_node"
            try:
                node = int(Path(sysfs_node_path).read_text().strip())
                if node == -1:
                    node = 0
            except OSError:
                node = 0
            devices_per_nodes[node] = dev_idx
        return devices_per_nodes

    @classmethod
    async def generate_docker_args(cls, docker, numa_node, limit_gpus=None):
        if cls.nvdocker_version[0] == 1:
            try:
                r = requests.get('http://localhost:3476/docker/cli/json')
                nvidia_params = r.json()
                r = requests.get('http://localhost:3476/gpu/info/json')
                gpu_info = r.json()
            except requests.exceptions.ConnectionError:
                raise RuntimeError('NVIDIA Docker plugin is not available.')

            volumes = await docker.volumes.list()
            existing_volumes = set(vol['Name'] for vol in volumes['Volumes'])
            required_volumes = set(vol.split(':')[0]
                                   for vol in nvidia_params['Volumes'])
            missing_volumes = required_volumes - existing_volumes
            binds = []
            for vol_name in missing_volumes:
                for vol_param in nvidia_params['Volumes']:
                    if vol_param.startswith(vol_name + ':'):
                        _, _, permission = vol_param.split(':')
                        driver = nvidia_params['VolumeDriver']
                        await docker.volumes.create({
                            'Name': vol_name,
                            'Driver': driver,
                        })
            for vol_name in required_volumes:
                for vol_param in nvidia_params['Volumes']:
                    if vol_param.startswith(vol_name + ':'):
                        _, mount_pt, permission = vol_param.split(':')
                        binds.append('{}:{}:{}'.format(
                            vol_name, mount_pt, permission))
            devices = []
            for dev in nvidia_params['Devices']:
                m = re.search(r'^/dev/nvidia(\d+)$', dev)
                if m is None:
                    devices.append(dev)
                    continue
                dev_idx = int(m.group(1))
                if limit_gpus is not None and dev_idx not in limit_gpus:
                    continue
                # Only expose GPUs in the same NUMA node.
                for gpu in gpu_info['Devices']:
                    if gpu['Path'] == dev:
                        try:
                            pci_id = gpu['PCI']['BusID'].lower()
                            pci_path = f"/sys/bus/pci/devices/{pci_id}/numa_node"
                            gpu_node = int(Path(pci_path).read_text().strip())
                        except FileNotFoundError:
                            gpu_node = -1
                        # Even when numa_node file exists, gpu_node may become -1
                        # (e.g., Amazon p2 instances)
                        if gpu_node == numa_node or gpu_node == -1:
                            devices.append(dev)
            devices = [{
                'PathOnHost': dev,
                'PathInContainer': dev,
                'CgroupPermissions': 'mrw',
            } for dev in devices]
            return {
                'HostConfig': {
                    'Binds': binds,
                    'Devices': devices,
                },
            }
        elif cls.nvdocker_version[0] == 2:
            gpus = []
            num_devices = libcudart.get_device_count()
            for dev_idx in range(num_devices):
                if limit_gpus is None or dev_idx in limit_gpus:
                    gpus.append(dev_idx)
            return {
                'HostConfig': {
                    'Runtime': 'nvidia',
                },
                'Env': [
                    f"NVIDIA_VISIBLE_DEVICES={','.join(map(str, gpus))}",
                ],
            }
        else:
            raise RuntimeError('BUG: should not be reached here!')
