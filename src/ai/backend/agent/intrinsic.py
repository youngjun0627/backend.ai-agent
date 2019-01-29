import logging
from pathlib import Path
from typing import Any, Collection, Mapping, Sequence

from ai.backend.common.logging import BraceStyleAdapter
from .resources import (
    AbstractComputeDevice,
    AbstractComputePlugin,
    DiscretePropertyAllocMap,
    KernelResourceSpec,
)
from .vendor.linux import libnuma

import psutil

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.intrinsic'))


# Pseudo-plugins for intrinsic devices (CPU and the main memory)

class CPUDevice(AbstractComputeDevice):
    pass


class CPUPlugin(AbstractComputePlugin):
    key = 'cpu'
    slot_types = [
        ('cpu', 'count')
    ]

    @classmethod
    async def list_devices(cls) -> Collection[CPUDevice]:
        num_cores = libnuma.get_available_cores()
        return [
            CPUDevice(
                device_id=str(core_idx),
                hw_location='root',
                numa_node=libnuma.node_of_cpu(core_idx),
                memory_size=0,
                processing_units=2,  # TODO: hyper-threading? / over-commit factor?
            )
            for core_idx in sorted(num_cores)
        ]

    @classmethod
    async def available_slots(cls) -> Mapping[str, str]:
        devices = await cls.list_devices()
        return {
            'cpu': sum(dev.processing_units for dev in devices),
        }

    @classmethod
    async def create_alloc_map(cls) -> Mapping[str, str]:
        devices = await cls.list_devices()
        return DiscretePropertyAllocMap(
            devices=devices,
            prop_func=lambda dev: dev.processing_units)

    @classmethod
    async def get_hooks(cls, distro: str, arch: str) -> Sequence[Path]:
        # TODO: move the sysconf hook in libbaihook.so here
        return []

    @classmethod
    async def generate_docker_args(cls,
                                   docker: 'aiodocker.docker.Docker',  # noqa
                                   per_device_alloc,
                                  ) -> Mapping[str, Any]:
        cores = [*per_device_alloc.keys()]
        print('gen-docker-args', cores)
        return {
            'HostConfig': {
                'CpuPeriod': 100_000,  # docker default
                'CpuQuota': int(100_000 * len(cores)),
                'Cpus': ','.join(sorted(cores)),
                'CpusetCpus': ','.join(sorted(cores)),
                # 'CpusetMems': f'{resource_spec.numa_node}',
            }
        }

    @classmethod
    async def restore_from_container(cls, container, alloc_map):
        assert isinstance(alloc_map, DiscretePropertyAllocMap)
        # Docker does not return the original cpuset.... :(
        # We need to read our own records.
        for bind in container['HostConfig']['Binds']:
            host_path, cont_path, perm = bind.split(':', maxsplit=2)
            if cont_path == '/home/config':
                with open(Path(host_path) / 'resource.txt', 'r') as f:
                    resource_spec = KernelResourceSpec.read_from_file(f)
                break
        else:
            return
        alloc_map.allocations['cpu'] = resource_spec.allocations['cpu']['cpu']


class MemoryDevice(AbstractComputeDevice):
    pass


class MemoryPlugin(AbstractComputePlugin):
    key = 'mem'
    slot_types = [
        ('mem', 'bytes')
    ]

    @classmethod
    async def list_devices(cls) -> Collection[MemoryDevice]:
        # TODO: support NUMA?
        memory_size = psutil.virtual_memory().total
        return [MemoryDevice(
            device_id='root',
            hw_location='root',
            numa_node=0,
            memory_size=memory_size,
            processing_units=0,
        )]

    @classmethod
    async def available_slots(cls) -> Mapping[str, str]:
        devices = await cls.list_devices()
        return {
            'mem': sum(dev.memory_size for dev in devices),
        }

    @classmethod
    async def create_alloc_map(cls) -> Mapping[str, str]:
        devices = await cls.list_devices()
        return DiscretePropertyAllocMap(
            devices=devices,
            prop_func=lambda dev: dev.memory_size)

    @classmethod
    async def get_hooks(cls, distro: str, arch: str) -> Sequence[Path]:
        return []

    @classmethod
    async def generate_docker_args(cls,
                                   docker: 'aiodocker.docker.Docker',  # noqa
                                   per_device_alloc,
                                  ) -> Mapping[str, Any]:
        return {
            'HostConfig': {
                'MemorySwap': 0,
                'Memory': sum(per_device_alloc['mem'].values()),
            }
        }

    @classmethod
    async def restore_from_container(cls, container, alloc_map):
        assert isinstance(alloc_map, DiscretePropertyAllocMap)
        memory_limit = container['HostConfig']['Memory']
        alloc_map.allocations['mem']['root'] += memory_limit
