from abc import abstractmethod, ABCMeta
import decimal
from typing import (
    Any, Collection, Container, Hashable,
    Mapping, TypeVar, Optional,
)

import attr

ProcessorIdType = TypeVar('ProcessorIdType', int, str, Hashable)


accelerator_types = {}


@attr.s(auto_attribs=True)
class AbstractAcceleratorInfo(metaclass=ABCMeta):
    device_id: ProcessorIdType
    hw_location: str            # either PCI bus ID or arbitrary string
    numa_node: Optional[int]    # NUMA node ID (None if not applicable)
    memory_size: int            # bytes of available per-accelerator memory
    processing_units: int       # number of processing units (e.g., cores, SMP)

    @abstractmethod
    def max_share(self) -> decimal.Decimal:
        '''Calculated maximum share for scheduling.'''
        return decimal.Decimal('0')

    @abstractmethod
    def share_to_memory(self, share: decimal.Decimal) -> int:
        return 0

    def share_to_processing_units(self, share: decimal.Decimal) -> int:
        return 0


class AbstractAccelerator(metaclass=ABCMeta):

    slot_key = 'accelerator'

    @classmethod
    @abstractmethod
    def list_devices(cls) -> Collection[AbstractAcceleratorInfo]:
        '''
        Return a collection of processors.
        '''
        return []

    @abstractmethod
    async def generate_docker_args(
            cls,
            docker: 'aiodocker.docker.Docker',
            numa_node: int,
            limit_gpus: Container[ProcessorIdType]=None) \
            -> Mapping[str, Any]:
        return {}
