from abc import ABCMeta, abstractmethod
from collections import defaultdict
from decimal import Decimal
import io
import json
import logging
from pathlib import Path
import pkg_resources
from typing import (
    Any, Container, Collection, Mapping, Sequence, Optional,
)

import attr

from ai.backend.common.types import (
    BinarySize,
    ResourceSlot, MountPermission,
    DeviceId, SlotType, Allocation, ResourceAllocations,
)
from ai.backend.common.logging import BraceStyleAdapter


log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.resources'))

compute_device_types = {}

known_slot_types = {}


class InsufficientResource(Exception):
    pass


@attr.s(auto_attribs=True)
class AbstractComputeDevice(metaclass=ABCMeta):
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


@attr.s(auto_attribs=True, slots=True)
class Mount:
    host_path: Path
    kernel_path: Path
    permission: MountPermission

    def __str__(self):
        return f'{self.host_path}:{self.kernel_path}:{self.permission.value}'

    @classmethod
    def from_str(cls, s):
        hp, kp, perm = s.split(':')
        hp = Path(hp)
        kp = Path(kp)
        perm = MountPermission(perm)
        return cls(hp, kp, perm)


@attr.s(auto_attribs=True, slots=True)
class KernelResourceSpec:
    '''
    This struct-like object stores the kernel resoucre allocation information
    with serialization and deserialization.

    It allows seamless reconstruction of allocations even when the agent restarts
    while kernel containers are running.
    '''

    '''Stores the original user-requested resource slots.'''
    slots: ResourceSlot

    '''
    Represents the resource allocations for each slot (device) type and devices.
    '''
    allocations: ResourceAllocations

    '''The mounted vfolder list.'''
    mounts: Sequence[str] = attr.Factory(list)

    '''The size of scratch disk. (not implemented yet)'''
    scratch_disk_size: int = None

    # '''Intrinsic allocations'''
    # numa_node: int = None
    # cpu_set: Container[int] = None

    def write_to_file(self, file: io.TextIOBase):
        '''
        Write the current resource specification into a file-like object.
        '''
        file.write(f'SCRATCH_SIZE={BinarySize(self.scratch_disk_size):m}\n')
        mounts_str = ','.join(map(str, self.mounts))
        file.write(f'MOUNTS={mounts_str}\n')
        slots_str = json.dumps({
            k: str(v) for k, v in self.slots.items()
        })
        file.write(f'SLOTS={slots_str}\n')
        for device_type, slots in self.allocations.items():
            for slot_type, per_device_alloc in slots.items():
                pieces = []
                for dev_id, alloc in per_device_alloc.items():
                    if known_slot_types[slot_type] == 'bytes':
                        pieces.append(f'{dev_id}:{BinarySize(alloc):s}')
                    else:
                        pieces.append(f'{dev_id}:{alloc}')
                alloc_str = ','.join(pieces)
                file.write(f'{slot_type.upper()}_SHARES={alloc_str}\n')

    @classmethod
    def read_from_file(cls, file: io.TextIOBase):
        '''
        Read resource specification values from a file-like object.
        '''
        kvpairs = {}
        for line in file:
            key, val = line.strip().split('=', maxsplit=1)
            kvpairs[key] = val
        allocations = defaultdict(dict)
        for key, val in kvpairs.items():
            if key.endswith('_SHARES'):
                slot_type = key[:-7].lower()
                device_type = slot_type.split('.')[0]
                per_device_alloc = {}
                for entry in val.split(','):
                    dev_id, alloc = entry.split(':')
                    try:
                        if known_slot_types[slot_type] == 'bytes':
                            value = BinarySize.from_str(alloc)
                        else:
                            value = Decimal(alloc)
                    except KeyError as e:
                        log.warning('A previously launched container has '
                                    'unknown slot type: {}. Ignoring it.',
                                    e.args[0])
                        continue
                    per_device_alloc[dev_id] = value
                allocations[device_type][slot_type] = per_device_alloc
        mounts = [Mount.from_str(m) for m in kvpairs['MOUNTS'].split(',') if m]
        return cls(
            scratch_disk_size=BinarySize.from_str(kvpairs['SCRATCH_SIZE']),
            allocations=dict(allocations),
            slots=ResourceSlot(json.loads(kvpairs['SLOTS'])),
            mounts=mounts,
        )

    def to_json(self) -> str:
        o = attr.asdict(self)
        for slot_type, alloc in o['slots'].items():
            if known_slot_types[slot_type] == 'bytes':
                o['slots'] = f'{BinarySize(alloc):s}'
            else:
                o['slots'] = str(alloc)
        for dev_type, dev_alloc in o['allocations'].items():
            for slot_type, per_device_alloc in dev_alloc.items():
                for dev_id, alloc in per_device_alloc.items():
                    if known_slot_types[slot_type] == 'bytes':
                        alloc = f'{BinarySize(alloc):s}'
                    else:
                        alloc = str(alloc)
                    o['allocations'][dev_type][slot_type][dev_id] = alloc
        o['mounts'] = list(map(str, self.mounts))
        return json.dumps(o)


class AbstractAllocMap(metaclass=ABCMeta):

    def __init__(self, *, devices: Container[AbstractComputeDevice] = None,
                 device_mask: Container[DeviceId] = None):
        self.devices = {dev.device_id: dev for dev in devices}
        self.device_mask = device_mask

    @abstractmethod
    def allocate(self, slots: Mapping[SlotType, Allocation]) -> ResourceAllocations:
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

    def allocate(self, slots: Mapping[SlotType, Allocation]) -> ResourceAllocations:
        allocation = {}
        for slot_type, alloc in slots.items():
            slot_allocation = {}
            remaining_alloc = int(alloc)

            # fill up starting from the most free devices
            sorted_dev_allocs = sorted(
                self.allocations[slot_type].items(),
                key=lambda pair: self.property_func(self.devices[pair[0]]) - pair[1],
                reverse=True)

            total_allocatable = 0
            for dev_id, current_alloc in sorted_dev_allocs:
                current_alloc = self.allocations[slot_type][dev_id]
                total_allocatable += (self.property_func(self.devices[dev_id]) -
                                      current_alloc)
            if total_allocatable < alloc:
                raise InsufficientResource(
                    'DiscretePropertyAllocMap: insufficient allocatable amount!')

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

    def allocate(self, slots: Mapping[SlotType, Allocation]) -> ResourceAllocations:
        allocation = {}
        for slot_type, alloc in slots.items():
            slot_allocation = {}
            remaining_alloc = int(alloc)

            # fill up starting from the most free devices
            sorted_dev_allocs = sorted(
                self.allocations[slot_type].items(),
                key=lambda pair: self.shares_per_device[pair[0]] - pair[1],
                reverse=True)

            total_allocatable = 0
            for dev_id, current_alloc in sorted_dev_allocs:
                current_alloc = self.allocations[slot_type][dev_id]
                total_allocatable += (self.shares_per_device[dev_id] -
                                      current_alloc)
            if total_allocatable < alloc:
                raise InsufficientResource(
                    'FractionAllocMap: insufficient allocatable amount!')

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


def bitmask2set(mask):
    bpos = 0
    bset = []
    while mask > 0:
        if (mask & 1) == 1:
            bset.append(bpos)
        mask = (mask >> 1)
        bpos += 1
    return frozenset(bset)


async def detect_slots(etcd, limit_cpus=None, limit_gpus=None):
    '''
    Detect available resource slots of the system.

    limit_cpus, limit_gpus are deprecated.
    '''

    slots = {}

    from .intrinsic import CPUPlugin, MemoryPlugin

    compute_device_types[CPUPlugin.key] = CPUPlugin
    compute_device_types[MemoryPlugin.key] = MemoryPlugin

    entry_prefix = 'backendai_accelerator_v11'
    for entrypoint in pkg_resources.iter_entry_points(entry_prefix):
        log.info('loading accelerator plugin: {}', entrypoint.module_name)
        plugin = entrypoint.load()
        accel_klass = await plugin.init(etcd)
        if accel_klass is None:
            # plugin init failed. skip!
            continue
        assert all(skey.startswith(f'{accel_klass.key}.')
                   for skey, _ in accel_klass.slot_types), \
               "Slot types defined by an accelerator plugin must be prefixed " \
               "by the plugin's key."
        if accel_klass.key in compute_device_types:
            raise RuntimeError(
                "A plugin defining the same key already exists. "
                "You may need to uninstall it first.")
        compute_device_types[accel_klass.key] = accel_klass

    for key, klass in compute_device_types.items():
        known_slot_types.update(klass.slot_types)
        accel_slots = await klass.available_slots()
        for skey, sval in accel_slots.items():
            slots[skey] = sval

    log.info('Resource slots: {!r}', slots)
    log.info('Slot types: {!r}', known_slot_types)
    return slots
