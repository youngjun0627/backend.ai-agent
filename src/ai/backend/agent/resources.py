from abc import ABCMeta, abstractmethod
from collections import defaultdict
from decimal import Decimal
import logging
import json
from pathlib import Path
from typing import (
    cast,
    Any, Collection, Iterable,
    List, Sequence,
    MutableMapping, Mapping,
    FrozenSet,
    Tuple, Union,
    Optional,
    TextIO,
)

import attr
import aiodocker

from ai.backend.common.types import (
    ResourceSlot, SlotName, SlotTypes,
    DeviceId, DeviceName, DeviceModelInfo,
    MountPermission, MountTypes,
    BinarySize,
)
from ai.backend.common.logging import BraceStyleAdapter
from .exception import InsufficientResource
from .stats import StatContext, NodeMeasurement, ContainerMeasurement

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.resources'))


known_slot_types: Mapping[SlotName, SlotTypes] = {}


@attr.s(auto_attribs=True, slots=True)
class KernelResourceSpec(metaclass=ABCMeta):
    '''
    This struct-like object stores the kernel resource allocation information
    with serialization and deserialization.

    It allows seamless reconstruction of allocations even when the agent restarts
    while kernel containers are running.
    '''

    '''The container ID to refer inside containers.'''
    container_id: str

    '''Stores the original user-requested resource slots.'''
    slots: Mapping[SlotName, str]

    '''
    Represents the resource allocations for each slot (device) type and devices.
    '''
    allocations: MutableMapping[DeviceName, Mapping[SlotName, Mapping[DeviceId, Decimal]]]

    '''The size of scratch disk. (not implemented yet)'''
    scratch_disk_size: int

    '''The mounted vfolder list.'''
    mounts: List['Mount'] = attr.Factory(list)

    '''The idle timeout in seconds.'''
    idle_timeout: Optional[int] = None

    def write_to_string(self) -> str:
        mounts_str = ','.join(map(str, self.mounts))
        slots_str = json.dumps({
            k: str(v) for k, v in self.slots.items()
        })

        resource_str = f'CID={self.container_id}\n'
        resource_str += f'SCRATCH_SIZE={BinarySize(self.scratch_disk_size):m}\n'
        resource_str += f'MOUNTS={mounts_str}\n'
        resource_str += f'SLOTS={slots_str}\n'
        resource_str += f'IDLE_TIMEOUT={self.idle_timeout}\n'

        for device_name, slots in self.allocations.items():
            for slot_name, per_device_alloc in slots.items():
                if not (slot_name.startswith(f'{device_name}.') or slot_name == device_name):
                    raise ValueError(f'device_name ({device_name}) must be a prefix of '
                                     f'slot_name ({slot_name})')
                pieces = []
                for dev_id, alloc in per_device_alloc.items():
                    if known_slot_types[slot_name] == 'bytes':
                        pieces.append(f'{dev_id}:{BinarySize(alloc):s}')
                    else:
                        pieces.append(f'{dev_id}:{alloc}')
                alloc_str = ','.join(pieces)
                resource_str += f'{slot_name.upper()}_SHARES={alloc_str}\n'

        return resource_str

    def write_to_file(self, file: TextIO) -> None:
        file.write(self.write_to_string())

    @classmethod
    def read_from_string(cls, text: str) -> 'KernelResourceSpec':
        kvpairs = {}
        for line in text.split('\n'):
            if '=' not in line:
                continue
            key, val = line.strip().split('=', maxsplit=1)
            kvpairs[key] = val
        allocations = cast(MutableMapping[DeviceName,
                                          MutableMapping[SlotName,
                                                         Mapping[DeviceId, Decimal]]],
                           defaultdict(dict))
        for key, val in kvpairs.items():
            if key.endswith('_SHARES'):
                slot_name = SlotName(key[:-7].lower())
                device_name = DeviceName(slot_name.split('.')[0])
                per_device_alloc: MutableMapping[DeviceId, Decimal] = {}
                for entry in val.split(','):
                    raw_dev_id, _, raw_alloc = entry.partition(':')
                    if not raw_dev_id or not raw_alloc:
                        continue
                    dev_id = DeviceId(raw_dev_id)
                    try:
                        if known_slot_types[slot_name] == 'bytes':
                            alloc = Decimal(BinarySize.from_str(raw_alloc))
                        else:
                            alloc = Decimal(raw_alloc)
                    except KeyError as e:
                        log.warning('A previously launched container has '
                                    'unknown slot type: {}. Ignoring it.',
                                    e.args[0])
                        continue
                    per_device_alloc[dev_id] = alloc
                allocations[device_name][slot_name] = per_device_alloc
        mounts = [Mount.from_str(m) for m in kvpairs['MOUNTS'].split(',') if m]
        return cls(
            container_id=kvpairs.get('CID', 'unknown'),
            scratch_disk_size=BinarySize.from_str(kvpairs['SCRATCH_SIZE']),
            allocations=dict(allocations),
            slots=ResourceSlot(json.loads(kvpairs['SLOTS'])),
            mounts=mounts,
            idle_timeout=int(kvpairs.get('IDLE_TIMEOUT', '600')),
        )

    @classmethod
    def read_from_file(cls, file: TextIO) -> 'KernelResourceSpec':
        text = '\n'.join(file.readlines())
        return cls.read_from_string(text)

    def to_json_serializable_dict(self) -> Mapping[str, Any]:
        o = attr.asdict(self)
        for slot_name, alloc in o['slots'].items():
            if known_slot_types[slot_name] == 'bytes':
                o['slots'] = f'{BinarySize(alloc):s}'
            else:
                o['slots'] = str(alloc)
        for dev_name, dev_alloc in o['allocations'].items():
            for slot_name, per_device_alloc in dev_alloc.items():
                for dev_id, alloc in per_device_alloc.items():
                    if known_slot_types[slot_name] == 'bytes':
                        alloc = f'{BinarySize(alloc):s}'
                    else:
                        alloc = str(alloc)
                    o['allocations'][dev_name][slot_name][dev_id] = alloc
        o['mounts'] = list(map(str, self.mounts))
        return o

    def to_json(self) -> str:
        return json.dumps(self.to_json_serializable_dict())


@attr.s(auto_attribs=True)
class AbstractComputeDevice():
    device_id: DeviceId
    hw_location: str            # either PCI bus ID or arbitrary string
    numa_node: Optional[int]    # NUMA node ID (None if not applicable)
    memory_size: int            # bytes of available per-accelerator memory
    processing_units: int       # number of processing units (e.g., cores, SMP)


class AbstractComputePlugin(metaclass=ABCMeta):

    key: DeviceName = DeviceName('accelerator')
    slot_types: Sequence[Tuple[SlotName, SlotTypes]] = []

    @classmethod
    @abstractmethod
    async def list_devices(cls) -> Collection[AbstractComputeDevice]:
        '''
        Return the list of accelerator devices, as read as physically
        on the host.
        '''
        raise NotImplementedError

    @classmethod
    @abstractmethod
    async def available_slots(cls) -> Mapping[SlotName, Decimal]:
        '''
        Return available slot amounts for each slot key.
        '''
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_version(cls) -> str:
        '''
        Return the version string of the plugin.
        '''
        raise NotImplementedError

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
        raise NotImplementedError

    @classmethod
    @abstractmethod
    async def gather_container_measures(cls, ctx: StatContext, container_ids: Sequence[str]) \
            -> Sequence[ContainerMeasurement]:
        '''
        Return the container-level statistic metrics.
        '''
        raise NotImplementedError

    @classmethod
    @abstractmethod
    async def create_alloc_map(cls) -> 'AbstractAllocMap':
        '''
        Create and return an allocation map for this plugin.
        '''
        raise NotImplementedError

    @classmethod
    @abstractmethod
    async def get_hooks(cls, distro: str, arch: str) -> Sequence[Path]:
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
    async def restore_from_container(cls, container: Mapping[str, Any],
                                     alloc_map: 'AbstractAllocMap') -> None:
        '''
        When the agent restarts, retore the allocation map from the container
        metadata dictionary fetched from aiodocker.
        '''
        pass

    @classmethod
    @abstractmethod
    async def get_attached_devices(cls, device_alloc: Mapping[SlotName, Mapping[DeviceId, Decimal]]) \
                                   -> Sequence[DeviceModelInfo]:
        '''
        Make up container-attached device information with allocated device id.
        '''
        return []


@attr.s(auto_attribs=True, slots=True)
class Mount:

    type: MountTypes
    source: Union[Path, str]
    target: Union[Path, str]
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

    devices: Mapping[DeviceId, AbstractComputeDevice]
    device_mask: FrozenSet[DeviceId]
    allocations: MutableMapping[SlotName, MutableMapping[DeviceId, Decimal]]

    def __init__(self, *,
                 devices: Iterable[AbstractComputeDevice] = None,
                 device_mask: Iterable[DeviceId] = None):
        self.devices = {dev.device_id: dev for dev in devices} if devices is not None else {}
        self.device_mask = frozenset(device_mask) if device_mask is not None else frozenset()
        self.allocations = {}

    @abstractmethod
    def allocate(self, slots: Mapping[SlotName, Decimal], *,
                 context_tag: str = None) \
                 -> Mapping[SlotName, Mapping[DeviceId, Decimal]]:
        '''
        Allocate the given amount of resources.

        For a slot type, there may be multiple different devices which can allocate resources
        in the given slot type.  An implementation of alloc map finds suitable match from the
        remaining capacities of those devices.

        Returns a mapping from each requested slot to the allocations per device.
        '''
        pass

    @abstractmethod
    def free(self, existing_alloc: Mapping[SlotName, Mapping[DeviceId, Decimal]]):
        '''
        Free the allocated resources using the token returned when the allocation
        occurred.
        '''
        pass


def bitmask2set(mask: int) -> FrozenSet[int]:
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

    def allocate(self, slots: Mapping[SlotName, Decimal], *,
                 context_tag: str = None) \
                 -> Mapping[SlotName, Mapping[DeviceId, Decimal]]:
        allocation = {}
        for slot_name, alloc in slots.items():
            slot_allocation: MutableMapping[DeviceId, Decimal] = {}
            remaining_alloc = int(alloc)

            # fill up starting from the most free devices
            sorted_dev_allocs = sorted(
                self.allocations[slot_name].items(),
                key=lambda pair: self.property_func(self.devices[pair[0]]) - pair[1],
                reverse=True)
            log.debug('DiscretePropertyAllocMap: allocating {} {}',
                      slot_name, alloc)
            log.debug('DiscretePropertyAllocMap: current-alloc: {!r}',
                      sorted_dev_allocs)

            total_allocatable = int(0)
            for dev_id, current_alloc in sorted_dev_allocs:
                current_alloc = self.allocations[slot_name][dev_id]
                total_allocatable += (self.property_func(self.devices[dev_id]) -
                                      current_alloc)
            if total_allocatable < alloc:
                raise InsufficientResource(
                    'DiscretePropertyAllocMap: insufficient allocatable amount!',
                    context_tag, slot_name, str(alloc), str(total_allocatable))

            for dev_id, current_alloc in sorted_dev_allocs:
                current_alloc = self.allocations[slot_name][dev_id]
                allocatable = (self.property_func(self.devices[dev_id]) -
                               current_alloc)
                if allocatable > 0:
                    allocated = min(remaining_alloc, allocatable)
                    slot_allocation[dev_id] = allocated
                    self.allocations[slot_name][dev_id] += allocated
                    remaining_alloc -= allocated
                if remaining_alloc == 0:
                    break
            allocation[slot_name] = slot_allocation
        return allocation

    def free(self, existing_alloc: Mapping[SlotName, Mapping[DeviceId, Decimal]]):
        for slot_name, per_device_alloc in existing_alloc.items():
            for dev_id, alloc in per_device_alloc.items():
                self.allocations[slot_name][dev_id] -= alloc


class FractionAllocMap(AbstractAllocMap):

    def __init__(self, *args, **kwargs):
        self.shares_per_device = kwargs.pop('shares_per_device')
        super().__init__(*args, **kwargs)
        self.allocations = defaultdict(lambda: {
            dev_id: Decimal(0) for dev_id in self.devices.keys()
        })

    def allocate(self, slots: Mapping[SlotName, Decimal], *,
                 context_tag: str = None) \
                 -> Mapping[SlotName, Mapping[DeviceId, Decimal]]:
        allocation = {}
        for slot_name, alloc in slots.items():
            slot_allocation: MutableMapping[DeviceId, Decimal] = {}
            remaining_alloc = Decimal(alloc).normalize()

            # fill up starting from the most free devices
            sorted_dev_allocs = sorted(
                self.allocations[slot_name].items(),
                key=lambda pair: self.shares_per_device[pair[0]] - pair[1],
                reverse=True)
            log.debug('FractionAllocMap: allocating {} {}', slot_name, alloc)
            log.debug('FractionAllocMap: current-alloc: {!r}', sorted_dev_allocs)

            total_allocatable = Decimal(0)
            for dev_id, current_alloc in sorted_dev_allocs:
                current_alloc = self.allocations[slot_name][dev_id]
                total_allocatable += (self.shares_per_device[dev_id] -
                                      current_alloc)
            if total_allocatable < alloc:
                raise InsufficientResource(
                    'FractionAllocMap: insufficient allocatable amount!',
                    context_tag, slot_name, str(alloc), str(total_allocatable))

            slot_allocation = {}
            for dev_id, current_alloc in sorted_dev_allocs:
                current_alloc = self.allocations[slot_name][dev_id]
                allocatable = (self.shares_per_device[dev_id] -
                               current_alloc)
                if allocatable > 0:
                    allocated = min(remaining_alloc, allocatable)
                    slot_allocation[dev_id] = allocated
                    self.allocations[slot_name][dev_id] += allocated
                    remaining_alloc -= allocated
                if remaining_alloc <= 0:
                    break
            allocation[slot_name] = slot_allocation
        return allocation

    def free(self, existing_alloc: Mapping[SlotName, Mapping[DeviceId, Decimal]]):
        for slot_name, per_device_alloc in existing_alloc.items():
            for dev_id, alloc in per_device_alloc.items():
                self.allocations[slot_name][dev_id] -= alloc
