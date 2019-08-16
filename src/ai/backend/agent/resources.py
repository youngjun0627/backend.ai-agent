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
    