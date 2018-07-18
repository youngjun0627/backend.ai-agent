import logging
from pathlib import Path
import re

import requests

log = logging.getLogger('ai.backend.agent.gpu')


async def prepare_nvidia(docker, numa_node, limit_gpus=None):
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
                binds.append('{}:{}:{}'.format(vol_name, mount_pt, permission))
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
    return binds, devices
