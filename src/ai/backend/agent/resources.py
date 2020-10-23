from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections import defaultdict
from decimal import Decimal, ROUND_DOWN
import logging
import json
import operator
from pathlib import Path
from typing import (
    Any,
    Collection,
    Container,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    Optional,
    FrozenSet,
    Sequence,
    TextIO,
    Tuple,
    Type,
    cast,
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
from ai.backend.common.plugin import AbstractPlugin, BasePluginContext
from .exception import InsufficientResource
from .stats import StatContext, NodeMeasurement, ContainerMeasurement
from .types import Container as SessionContainer

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.resources'))


known_slot_types: Mapping[SlotName, SlotTypes] = {}


@attr.s(auto_attribs=True, slots=True)
class KernelResourceSpec:
    """
    This struct-like object stores the kernel resource allocation information
    with serialization and deserialization.

    It allows seamless reconstruction of allocations even when the agent restarts
    while kernel containers are running.
    """

    container_id: str
    """The container ID to refer inside containers."""

    slots: Mapping[SlotName, str]
    """Stores the original user-requested resource slots."""

    allocations: MutableMapping[DeviceName, Mapping[SlotName, Mapping[DeviceId, Decimal]]]
    """
    Represents the resource allocations for each slot (device) type and devices.
    """

    scratch_disk_size: int
    """The size of scratch disk. (not implemented yet)"""

    mounts: List['Mount'] = attr.Factory(list)
    """The mounted vfolder list."""

    def freeze(self) -> None:
        """Replace the attribute setter to make it immutable."""
        # TODO: implement
        pass

        # def _frozen_setattr(self, name, value):
        #     raise RuntimeError("tried to modify a frozen KernelResourceSpec object")

        # self.mounts = tuple(self.mounts)  # type: ignore
        # # TODO: wrap slots and allocations with frozendict?
        # setattr(self, '__setattr__', _frozen_setattr)  # <-- __setattr__ is read-only... :(

    def write_to_string(self) -> str:
        mounts_str = ','.join(map(str, self.mounts))
        slots_str = json.dumps({
            k: str(v) for k, v in self.slots.items()
        })

        resource_str = f'CID={self.container_id}\n'
        resource_str += f'SCRATCH_SIZE={BinarySize(self.scratch_disk_size):m}\n'
        resource_str += f'MOUNTS={mounts_str}\n'
        resource_str += f'SLOTS={slots_str}\n'

        for device_name, slots in self.allocations.items():
            for slot_name, per_device_alloc in slots.items():
                if not (slot_name.startswith(f'{device_name}.') or slot_name == device_name):
                    raise ValueError(f'device_name ({device_name}) must be a prefix of '
                                     f'slot_name ({slot_name})')
                pieces = []
                for dev_id, alloc in per_device_alloc.items():
                    if known_slot_types.get(slot_name, 'count') == 'bytes':
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
                        if known_slot_types.get(slot_name, 'count') == 'bytes':
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
            scratch_disk_size=BinarySize.finite_from_str(kvpairs['SCRATCH_SIZE']),
            allocations=dict(allocations),
            slots=ResourceSlot(json.loads(kvpairs['SLOTS'])),
            mounts=mounts,
        )

    @classmethod
    def read_from_file(cls, file: TextIO) -> 'KernelResourceSpec':
        text = '\n'.join(file.readlines())
        return cls.read_from_string(text)

    def to_json_serializable_dict(self) -> Mapping[str, Any]:
        o = attr.asdict(self)
        for slot_name, alloc in o['slots'].items():
            if known_slot_types.get(slot_name, 'count') == 'bytes':
                o['slots'] = f'{BinarySize(alloc):s}'
            else:
                o['slots'] = str(alloc)
        for dev_name, dev_alloc in o['allocations'].items():
            for slot_name, per_device_alloc in dev_alloc.items():
                for dev_id, alloc in per_device_alloc.items():
                    if known_slot_types.get(slot_name, 'count') == 'bytes':
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


class AbstractComputePlugin(AbstractPlugin, metaclass=ABCMeta):

    key: DeviceName = DeviceName('accelerator')
    slot_types: Sequence[Tuple[SlotName, SlotTypes]] = []

    @abstractmethod
    async def list_devices(self) -> Collection[AbstractComputeDevice]:
        """
        Return the list of accelerator devices, as read as physically
        on the host.
        """
        raise NotImplementedError

    @abstractmethod
    async def available_slots(self) -> Mapping[SlotName, Decimal]:
        """
        Return available slot amounts for each slot key.
        """
        raise NotImplementedError

    @abstractmethod
    def get_version(self) -> str:
        """
        Return the version string of the plugin.
        """
        raise NotImplementedError

    @abstractmethod
    async def extra_info(self) -> Mapping[str, str]:
        """
        Return extra information related to this plugin,
        such as the underlying driver version and feature flags.
        """
        return {}

    @abstractmethod
    async def gather_node_measures(self, ctx: StatContext) -> Sequence[NodeMeasurement]:
        """
        Return the system-level and device-level statistic metrics.

        It may return any number of metrics using different statistics key names in the
        returning map.
        Note that the key must not conflict with other accelerator plugins and must not
        contain dots.
        """
        raise NotImplementedError

    @abstractmethod
    async def gather_container_measures(
        self,
        ctx: StatContext,
        container_ids: Sequence[str],
    ) -> Sequence[ContainerMeasurement]:
        """
        Return the container-level statistic metrics.
        """
        raise NotImplementedError

    @abstractmethod
    async def create_alloc_map(self) -> 'AbstractAllocMap':
        """
        Create and return an allocation map for this plugin.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_hooks(self, distro: str, arch: str) -> Sequence[Path]:
        """
        Return the library hook paths used by the plugin (optional).

        :param str distro: The target Linux distribution such as "ubuntu16.04" or
                           "alpine3.8"
        :param str arch: The target CPU architecture such as "amd64"
        """
        return []

    @abstractmethod
    async def generate_docker_args(
        self,
        docker: aiodocker.docker.Docker,
        device_alloc,
    ) -> Mapping[str, Any]:
        """
        When starting a new container, generate device-specific options for the
        docker container create API as a dictionary, referring the given allocation
        map.  The agent will merge it with its own options.
        """
        return {}

    async def generate_resource_data(self, device_alloc) -> Mapping[str, str]:
        """
        Generate extra resource.txt key-value pair sets to be used by the plugin's
        own hook libraries in containers.
        """
        return {}

    @abstractmethod
    async def restore_from_container(
        self,
        container: SessionContainer,
        alloc_map: AbstractAllocMap,
    ) -> None:
        """
        When the agent restarts, retore the allocation map from the container
        metadata dictionary fetched from aiodocker.
        """
        pass

    @abstractmethod
    async def get_attached_devices(
        self,
        device_alloc: Mapping[SlotName, Mapping[DeviceId, Decimal]],
    ) -> Sequence[DeviceModelInfo]:
        """
        Make up container-attached device information with allocated device id.
        """
        return []


class ComputePluginContext(BasePluginContext[AbstractComputePlugin]):
    plugin_group = 'backendai_accelerator_v20'

    @classmethod
    def discover_plugins(
        cls,
        plugin_group: str,
        blocklist: Container[str] = None,
    ) -> Iterator[Tuple[str, Type[AbstractComputePlugin]]]:
        scanned_plugins = [*super().discover_plugins(plugin_group, blocklist)]

        def accel_lt_intrinsic(item):
            # push back "intrinsic" plugins (if exists)
            if item[0] in ('cpu', 'mem'):
                return 0
            return -1

        scanned_plugins.sort(key=accel_lt_intrinsic)
        yield from scanned_plugins

    def attach_intrinsic_device(self, plugin: AbstractComputePlugin) -> None:
        self.plugins[plugin.key] = plugin


@attr.s(auto_attribs=True, slots=True)
class Mount:
    type: MountTypes
    source: Path
    target: Path
    permission: MountPermission = MountPermission.READ_ONLY
    opts: Optional[Mapping[str, Any]] = None
    is_unmanaged: bool = False

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

    def __init__(
        self, *,
        devices: Iterable[AbstractComputeDevice] = None,
        device_mask: Iterable[DeviceId] = None,
    ) -> None:
        self.devices = {dev.device_id: dev for dev in devices} if devices is not None else {}
        self.device_mask = frozenset(device_mask) if device_mask is not None else frozenset()
        self.allocations = {}

    def clear(self) -> None:
        self.allocations.clear()

    @abstractmethod
    def allocate(
        self,
        slots: Mapping[SlotName, Decimal],
        *,
        context_tag: str = None,
    ) -> Mapping[SlotName, Mapping[DeviceId, Decimal]]:
        """
        Allocate the given amount of resources.

        For a slot type, there may be multiple different devices which can allocate resources
        in the given slot type.  An implementation of alloc map finds suitable match from the
        remaining capacities of those devices.

        Returns a mapping from each requested slot to the allocations per device.
        """
        pass

    @abstractmethod
    def apply_allocation(
        self,
        existing_alloc: Mapping[SlotName, Mapping[DeviceId, Decimal]],
    ) -> None:
        """
        Apply the given allocation restored from disk or other persistent storages.
        """
        pass

    @abstractmethod
    def free(
        self,
        existing_alloc: Mapping[SlotName, Mapping[DeviceId, Decimal]],
    ) -> None:
        """
        Free the allocated resources using the token returned when the allocation
        occurred.
        """
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
    """
    An allocation map using discrete property.
    The user must pass a "property function" which returns a desired resource
    property from the device object.

    e.g., 1.0 means 1 device, 2.0 means 2 devices, etc.
    (no fractions allowed)
    """

    def __init__(self, *args, **kwargs) -> None:
        self.property_func = kwargs.pop('prop_func')
        super().__init__(*args, **kwargs)
        assert callable(self.property_func)
        self.allocations = defaultdict(lambda: {
            dev_id: Decimal(0) for dev_id in self.devices.keys()
        })

    def allocate(
        self,
        slots: Mapping[SlotName, Decimal],
        *,
        context_tag: str = None,
    ) -> Mapping[SlotName, Mapping[DeviceId, Decimal]]:
        allocation = {}
        for slot_name, alloc in slots.items():
            slot_allocation: MutableMapping[DeviceId, Decimal] = {}
            remaining_alloc = Decimal(alloc).normalize()

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
                    allocated = Decimal(min(remaining_alloc, allocatable))
                    slot_allocation[dev_id] = allocated
                    self.allocations[slot_name][dev_id] += allocated
                    remaining_alloc -= allocated
                if remaining_alloc == 0:
                    break
            allocation[slot_name] = slot_allocation
        return allocation

    def apply_allocation(
        self,
        existing_alloc: Mapping[SlotName, Mapping[DeviceId, Decimal]],
    ) -> None:
        for slot_name, per_device_alloc in existing_alloc.items():
            for dev_id, alloc in per_device_alloc.items():
                self.allocations[slot_name][dev_id] += alloc

    def free(
        self,
        existing_alloc: Mapping[SlotName, Mapping[DeviceId, Decimal]],
    ) -> None:
        for slot_name, per_device_alloc in existing_alloc.items():
            for dev_id, alloc in per_device_alloc.items():
                self.allocations[slot_name][dev_id] -= alloc


class FractionAllocMap(AbstractAllocMap):

    def __init__(self, *args, **kwargs) -> None:
        self.shares_per_device = kwargs.pop('shares_per_device')
        super().__init__(*args, **kwargs)
        self.allocations = defaultdict(lambda: {
            dev_id: Decimal(0) for dev_id in self.devices.keys()
        })
        self.digits = Decimal(10) ** -2  # decimal points that is supported by agent
        self.powers = Decimal(100)  # reciprocal of self.digits

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

    def allocate_evenly(self, slots: Mapping[SlotName, Decimal], *,
                        context_tag: str = None,
                        min_memory: Decimal = Decimal(0.01)) \
                        -> Mapping[SlotName, Mapping[DeviceId, Decimal]]:

        # higher value means more even with 0 being the highest value
        def measure_evenness(alloc_map: Mapping[DeviceId, Decimal]) \
                             -> Decimal:
            alloc_arr = sorted([alloc_map[dev_id] for dev_id in alloc_map])
            evenness_score = Decimal(0).quantize(self.digits)
            for idx in range(len(alloc_arr) - 1):
                evenness_score += abs(alloc_arr[idx + 1] - alloc_arr[idx])
            return -evenness_score

        # higher value means more fragmented
        # i.e. the number of unusable resources is higher
        def measure_fragmentation(allocation: Mapping[DeviceId, Decimal],
                                  min_memory: Decimal):
            fragmentation_arr = [self.shares_per_device[dev_id] - allocation[dev_id]
                                 for dev_id in allocation]
            return sum(self.digits < v.quantize(self.digits) < min_memory.quantize(self.digits)
                       for v in fragmentation_arr)

        # evenly distributes remaining_alloc across dev_allocs
        def distribute_evenly(dev_allocs: List[Tuple[DeviceId, Decimal]],
                              remaining_alloc: Decimal,
                              allocation: MutableMapping[DeviceId, Decimal]):
            n_devices = len(dev_allocs)
            for dev_id, _ in dev_allocs:
                dev_allocation = remaining_alloc / n_devices
                dev_allocation = dev_allocation.quantize(self.digits, rounding=ROUND_DOWN)
                allocation[dev_id] = dev_allocation

            # need to take care of decimals
            remainder = round(remaining_alloc * self.powers -
                              dev_allocation * n_devices * self.powers)
            for idx in range(remainder):
                dev_id, _ = dev_allocs[idx]
                allocation[dev_id] += self.digits

        # allocates remaining_alloc across multiple devices i.e. dev_allocs
        # all devices in dev_allocs are being used
        def allocate_across_devices(dev_allocs: List[Tuple[DeviceId, Decimal]],
                                    remaining_alloc: Decimal, slot_name: str) \
                                    -> MutableMapping[DeviceId, Decimal]:
            slot_allocation: MutableMapping[DeviceId, Decimal] = {}
            n_devices = len(dev_allocs)
            idx = n_devices - 1  # check from the device with smallest allocatable resource
            while n_devices > 0:
                dev_id, current_alloc = dev_allocs[idx]
                allocatable = self.shares_per_device[dev_id] - current_alloc
                # if the remaining_alloc can be allocated to evenly among remaining devices
                if allocatable >= remaining_alloc / n_devices:
                    break
                slot_allocation[dev_id] = allocatable.quantize(self.digits)
                remaining_alloc -= allocatable
                idx -= 1
                n_devices -= 1

            if n_devices > 0:
                distribute_evenly(dev_allocs[:n_devices], remaining_alloc, slot_allocation)

            return slot_allocation

        min_memory = min_memory.quantize(self.digits)
        allocation = {}
        for slot_name, alloc in slots.items():
            slot_allocation: MutableMapping[DeviceId, Decimal] = {}
            remaining_alloc = Decimal(alloc).normalize()
            sorted_dev_allocs = sorted(
                self.allocations[slot_name].items(),
                key=lambda pair: self.shares_per_device[pair[0]] - pair[1],
                reverse=True)

            # do not consider devices whose remaining resource under min_memory
            sorted_dev_allocs = list(filter(
                lambda pair: self.shares_per_device[pair[0]] - pair[1] >= min_memory,
                sorted_dev_allocs))

            log.debug('FractionAllocMap: allocating {} {}', slot_name, alloc)
            log.debug('FractionAllocMap: current-alloc: {!r}', sorted_dev_allocs)

            # check if there is enough resource for allocation
            total_allocatable = Decimal(0)
            for dev_id, current_alloc in sorted_dev_allocs:
                current_alloc = self.allocations[slot_name][dev_id]
                total_allocatable += (self.shares_per_device[dev_id] -
                                      current_alloc)
            if total_allocatable.quantize(self.digits) < \
                    remaining_alloc.quantize(self.digits):
                raise InsufficientResource(
                    'FractionAllocMap: insufficient allocatable amount!',
                    context_tag, slot_name, str(alloc), str(total_allocatable))

            # allocate resources
            if remaining_alloc <= self.shares_per_device[sorted_dev_allocs[0][0]] \
                    - sorted_dev_allocs[0][1]:  # if remaining_alloc fits in one device
                slot_allocation = {}
                for dev_id, current_alloc in sorted_dev_allocs[::-1]:
                    allocatable = (self.shares_per_device[dev_id] -
                                current_alloc)
                    if remaining_alloc <= allocatable:
                        slot_allocation[dev_id] = remaining_alloc.quantize(self.digits)
                        break
            else:  # need to distribute across devices
                # calculate the minimum number of required devices
                n_devices, allocated = 0, Decimal(0)
                for dev_id, current_alloc in sorted_dev_allocs:
                    n_devices += 1
                    allocated += self.shares_per_device[dev_id] - current_alloc
                    if allocated.quantize(self.digits) >= \
                            remaining_alloc.quantize(self.digits):
                        break
                # need to check from using minimum number of devices to using all devices
                # evenness must be non-decreasing with the increase of window size
                best_alloc_candidate_arr = []
                for n_dev in range(n_devices, len(sorted_dev_allocs) + 1):
                    allocatable = sum(map(lambda x: self.shares_per_device[x[0]] - x[1],
                                      sorted_dev_allocs[:n_dev]))
                    # choose the best allocation from all possible allocation candidates
                    alloc_candidate = allocate_across_devices(sorted_dev_allocs[:n_dev],
                                                              remaining_alloc, slot_name)
                    max_evenness = measure_evenness(alloc_candidate)
                    # three criteria to decide allocation are
                    # eveness, number of resources used, and amount of fragmentatino
                    alloc_candidate_arr = [(alloc_candidate, max_evenness, -len(alloc_candidate),
                                            -measure_fragmentation(alloc_candidate, min_memory))]
                    for idx in range(1, len(sorted_dev_allocs) - n_dev + 1):
                        # update amount of allocatable space
                        allocatable -= self.shares_per_device[sorted_dev_allocs[idx - 1][0]] - \
                                       sorted_dev_allocs[idx - 1][1]
                        allocatable += self.shares_per_device[sorted_dev_allocs[idx + n_dev - 1][0]] - \
                                       sorted_dev_allocs[idx + n_dev - 1][1]
                        # break if not enough resource
                        if allocatable.quantize(self.digits) < \
                                remaining_alloc.quantize(self.digits):
                            break
                        alloc_candidate = allocate_across_devices(
                            sorted_dev_allocs[idx:idx + n_dev], remaining_alloc, slot_name)
                        # evenness gets worse (or same at best) as the allocatable gets smaller
                        evenness_score = measure_evenness(alloc_candidate)
                        if evenness_score < max_evenness:
                            break
                        alloc_candidate_arr.append((alloc_candidate, evenness_score,
                            -len(alloc_candidate), -measure_fragmentation(alloc_candidate, min_memory)))
                    # since evenness is the same, sort by fragmentation (low is good)
                    best_alloc_candidate_arr.append(
                        sorted(alloc_candidate_arr, key=lambda x: x[2])[-1])
                    # there is no need to look at more devices if the desired evenness is achieved
                    if max_evenness.quantize(self.digits) == self.digits:
                        best_alloc_candidate_arr = best_alloc_candidate_arr[-1:]
                        break
                # choose the best allocation with the three criteria
                slot_allocation = sorted(best_alloc_candidate_arr,
                                         key=operator.itemgetter(1, 2, 3))[-1][0]
            allocation[slot_name] = slot_allocation
            for dev_id in slot_allocation:
                self.allocations[slot_name][dev_id] += slot_allocation[dev_id]
        return allocation

    def apply_allocation(
        self,
        existing_alloc: Mapping[SlotName, Mapping[DeviceId, Decimal]],
    ) -> None:
        for slot_name, per_device_alloc in existing_alloc.items():
            for dev_id, alloc in per_device_alloc.items():
                self.allocations[slot_name][dev_id] += alloc

    def free(
        self,
        existing_alloc: Mapping[SlotName, Mapping[DeviceId, Decimal]],
    ) -> None:
        for slot_name, per_device_alloc in existing_alloc.items():
            for dev_id, alloc in per_device_alloc.items():
                self.allocations[slot_name][dev_id] -= alloc
