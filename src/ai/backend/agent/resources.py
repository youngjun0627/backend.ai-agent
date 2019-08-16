from abc import ABCMeta, abstractmethod
from collections import defaultdict
from decimal import Decimal
import json
import logging
from pathlib import Path
import pkg_resources
from typing import (
    Any, Container, Collection, Mapping, Sequence, Optional, Union,
)

import attr

from ai.backend.common.types import (
    BinarySize,
    ResourceSlot, MountPermission, MountTypes,
    DeviceId, SlotType, Allocation, ResourceAllocations,
)
from ai.backend.common.logging import BraceStyleAdapter
from .exception import InsufficientResource, InitializationError
from .stats import StatContext, NodeMeasurement, ContainerMeasurement


log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.resources'))

known_slot_types = {}


@attr.s(auto_attribs=True)
class AbstractComputeDevice():
    device_id: DeviceId
    hw_location: str            # either PCI bus ID or arbitrary string
    numa_node: Optional[int]    # NUMA node ID (None if not applicable)
    memory_size: int            # bytes of available per-accelerator memory
    processing_units: int       # number of processing units (e.g., cores, SMP)


class AbstractComputePlugin(metaclass=ABCMeta):

    key = 'accelerator'
    slot_types = []

    @classmethod
    @abstractmethod
    async def list_devices(cls) -> Collection[AbstractComputeDevice]:
        '''
        Return the list of accelerator devices, as read as physically
        on the host.
        '''
        return []

    @classmethod
    @abstractmethod
    async def available_slots(cls) -> Mapping[str, str]:
        '''
        Return available slot amounts for each slot key.
        '''
        return []

    @classmethod
    @abstractmethod
    def get_version(cls) -> str:
        '''
        Return the version string of the plugin.
        '''
        return ''

    @classmethod
    @abstractmethod
    async def extra_info(cls) -> Mapping[str, str]:
        '''
        Return extra information related to this plugin,
        such as the underlying driver version and feature flags.
        '''
        return {}

    @classmethod
    @abstractmethod
    async def gather_node_measures(cls, ctx: StatContext) -> Sequence[NodeMeasurement]:
        '''
        Return the system-level and device-level statistic metrics.

        It may return any number of metrics using different statistics key names in the
        returning map.
        Note that the key must not conflict with other accelerator plugins and must not
        contain dots.
        '''
        return {}

    @classmethod
    @abstractmethod
    async def gather_container_measures(cls, ctx: StatContext, container_ids: Sequence[str]) \
            -> Sequence[ContainerMeasurement]:
        '''
        Return the container-level statistic metrics.
        '''
        return {}

    @classmethod
    @abstractmethod
    def create_alloc_map(cls) -> 'AbstractAllocMap':
        '''
        Create and return an allocation map for this plugin.
        '''
        return None

    @classmethod
    @abstractmethod
    def get_hooks(cls, distro: str, arch: str) -> Sequence[Path]:
        '''
        Return the library hook paths used by the plugin (optional).

        :param str distro: The target Linux distribution such as "ubuntu16.04" or
                           "alpine3.8"
        :param str arch: The target CPU architecture such as "amd64"
        '''
        return []

    @classmethod
    @abstractmethod
    async def generate_docker_args(cls,
                                   docker: 'aiodocker.docker.Docker',  # noqa
                                   device_alloc,
                                  ) -> Mapping[str, Any]:
        '''
        When starting a new container, generate device-specific options for the
        docker container create API as a dictionary, referring the given allocation
        map.  The agent will merge it with its own options.
        '''
        return {}

    @classmethod
    async def generate_resource_data(cls, device_alloc) -> Mapping[str, str]:
        '''
        Generate extra resource.txt key-value pair sets to be used by the plugin's
        own hook libraries in containers.
        '''
        return {}

    @classmethod
    @abstractmethod
    async def restore_from_container(cls, container, alloc_map):
        '''
        When the agent restarts, retore the allocation map from the container
        metadata dictionary fetched from aiodocker.
        '''
        pass

    @classmethod
    @abstractmethod
    async def get_attached_devices(cls, device_alloc) -> Mapping[str, str]:
        '''
        Make up container-attached device information with allocated device id.
        '''
        pass


@attr.s(auto_attribs=True, slots=True)
class Mount:
    type: MountTypes
    source: Union[Path, str]
    target: Path
    permission: MountPermission = MountPermission.READ_ONLY
    opts: Optional[Mapping[str, Any]] = None

    def __str__(self):
        return f'{self.source}:{self.target}:{self.permission.value}'

    @classmethod
    def from_str(cls, s):
        source, target, perm = s.split(':')
        source = Path(source)
        type = MountTypes.BIND
        if not source.is_absolute():
            if len(source.parts) == 1:
                source = str(source)
                type = MountTypes.VOLUME
            else:
                raise ValueError('Mount source must be an absolute path '
                                 'if it is not a volume name.',
                                 source)
        target = Path(target)
        if not target.is_absolute():
            raise ValueError('Mount target must be an absolute path.', target)
        perm = MountPermission(perm)
        return cls(type, source, target, perm, None)


class AbstractAllocMap(metaclass=ABCMeta):

    allocations: Mapping[str, Decimal]

    def __init__(self, *, devices: Container[AbstractComputeDevice] = None,
                 device_mask: Container[DeviceId] = None):
        self.devices = {dev.device_id: dev for dev in devices}
        self.device_mask = device_mask

    @abstractmethod
    def allocate(self, slots: Mapping[SlotType, Allocation], *,
                 context_tag: str = None) -> ResourceAllocations:
        '''
        Allocate the given amount of resources.

        Returns a token that can be used to free the exact resources allocated in the
        current allocation.
        '''
        pass

    @abstractmethod
    def free(self, existing_alloc: ResourceAllocations):
        '''
        Free the allocated resources using the token returned when the allocation
        occurred.
        '''
        pass


def bitmask2set(mask):
    bpos = 0
    bset = []
    while mask > 0:
        if (mask & 1) == 1:
            bset.append(bpos)
        mask = (mask >> 1)
        bpos += 1
    return frozenset(bset)
    

class DiscretePropertyAllocMap(AbstractAllocMap):
    '''
    An allocation map using discrete property.
    The user must pass a "property function" which returns a desired resource
    property from the device object.

    e.g., 1.0 means 1 device, 2.0 means 2 devices, etc.
    (no fractions allowed)
    '''

    def __init__(self, *args, **kwargs):
        self.property_func = kwargs.pop('prop_func')
        super().__init__(*args, **kwargs)
        assert callable(self.property_func)
        self.allocations = defaultdict(lambda: {
            dev_id: 0 for dev_id in self.devices.keys()
        })

    def allocate(self, slots: Mapping[SlotType, Allocation], *,
                 context_tag: str = None) -> ResourceAllocations:
        allocation = {}
        for slot_type, alloc in slots.items():
            slot_allocation = {}
            remaining_alloc = int(alloc)

            # fill up starting from the most free devices
            sorted_dev_allocs = sorted(
                self.allocations[slot_type].items(),
                key=lambda pair: self.property_func(self.devices[pair[0]]) - pair[1],
                reverse=True)
            log.debug('DiscretePropertyAllocMap: allocating {} {}',
                      slot_type, alloc)
            log.debug('DiscretePropertyAllocMap: current-alloc: {!r}',
                      sorted_dev_allocs)

            total_allocatable = 0
            for dev_id, current_alloc in sorted_dev_allocs:
                current_alloc = self.allocations[slot_type][dev_id]
                total_allocatable += (self.property_func(self.devices[dev_id]) -
                                      current_alloc)
            if total_allocatable < alloc:
                raise InsufficientResource(
                    'DiscretePropertyAllocMap: insufficient allocatable amount!',
                    context_tag, slot_type, str(alloc), str(total_allocatable))

            slot_allocation = {}
            for dev_id, current_alloc in sorted_dev_allocs:
                current_alloc = self.allocations[slot_type][dev_id]
                allocatable = (self.property_func(self.devices[dev_id]) -
                               current_alloc)
                if allocatable > 0:
                    allocated = min(remaining_alloc, allocatable)
                    slot_allocation[dev_id] = allocated
                    self.allocations[slot_type][dev_id] += allocated
                    remaining_alloc -= allocated
                if remaining_alloc == 0:
                    break
            allocation[slot_type] = slot_allocation
        return allocation

    def free(self, existing_alloc: ResourceAllocations):
        for slot_type, per_device_alloc in existing_alloc.items():
            for dev_id, alloc in per_device_alloc.items():
                self.allocations[slot_type][dev_id] -= alloc


class FractionAllocMap(AbstractAllocMap):

    def __init__(self, *args, **kwargs):
        self.shares_per_device = kwargs.pop('shares_per_device')
        super().__init__(*args, **kwargs)
        self.allocations = defaultdict(lambda: {
            dev_id: Decimal(0) for dev_id in self.devices.keys()
        })

    def allocate(self, slots: Mapping[SlotType, Allocation], *,
                 context_tag: str = None) -> ResourceAllocations:
        allocation = {}
        for slot_type, alloc in slots.items():
            slot_allocation = {}
            remaining_alloc = Decimal(alloc).normalize()

            # fill up starting from the most free devices
            sorted_dev_allocs = sorted(
                self.allocations[slot_type].items(),
                key=lambda pair: self.shares_per_device[pair[0]] - pair[1],
                reverse=True)
            log.debug('FractionAllocMap: allocating {} {}', slot_type, alloc)
            log.debug('FractionAllocMap: current-alloc: {!r}', sorted_dev_allocs)

            total_allocatable = 0
            for dev_id, current_alloc in sorted_dev_allocs:
                current_alloc = self.allocations[slot_type][dev_id]
                total_allocatable += (self.shares_per_device[dev_id] -
                                      current_alloc)
            if total_allocatable < alloc:
                raise InsufficientResource(
                    'FractionAllocMap: insufficient allocatable amount!',
                    context_tag, slot_type, str(alloc), str(total_allocatable))

            slot_allocation = {}
            for dev_id, current_alloc in sorted_dev_allocs:
                current_alloc = self.allocations[slot_type][dev_id]
                allocatable = (self.shares_per_device[dev_id] -
                               current_alloc)
                if allocatable > 0:
                    allocated = min(remaining_alloc, allocatable)
                    slot_allocation[dev_id] = allocated
                    self.allocations[slot_type][dev_id] += allocated
                    remaining_alloc -= allocated
                if remaining_alloc <= 0:
                    break
            allocation[slot_type] = slot_allocation
        return allocation

    def free(self, existing_alloc: ResourceAllocations):
        for slot_type, per_device_alloc in existing_alloc.items():
            for dev_id, alloc in per_device_alloc.items():
                self.allocations[slot_type][dev_id] -= alloc

