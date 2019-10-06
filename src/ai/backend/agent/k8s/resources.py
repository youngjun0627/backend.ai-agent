from decimal import Decimal
import logging
import pkg_resources
from typing import (
    Any, Dict, Mapping, MutableMapping,
    Type, Tuple
)

from kubernetes_asyncio import config as K8sConfig, client as K8sClient

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import SlotName, SlotTypes, DeviceId, DeviceName
from ..exception import InsufficientResource, InitializationError
from ..resources import AbstractComputePlugin, AbstractAllocMap, known_slot_types

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.resources'))


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

    def allocate(self, slots: Mapping[SlotName, Decimal], *,
                 context_tag: str = None) \
                 -> Mapping[SlotName, Mapping[DeviceId, Decimal]]:
        allocation: Mapping[SlotName, Mapping[DeviceId, Decimal]] = {}
        for slot_type, alloc in slots.items():
            remaining_alloc = int(alloc)

            log.debug('DiscretePropertyAllocMap: allocating {} {}',
                      slot_type, alloc)
            
            available_units = map(lambda x: self.property_func(x), self.devices.values())
            max_allocatable = max(available_units)
            if max_allocatable < alloc:
                raise InsufficientResource(
                    'DiscretePropertyAllocMap: insufficient allocatable amount!',
                    context_tag, slot_type, str(alloc), str(max_allocatable))

            allocation[slot_type] = {'k8s': remaining_alloc}
        return allocation

    def free(self, existing_alloc: Mapping[SlotName, Mapping[DeviceId, Decimal]]):
        pass


async def detect_resources(resource_configs: Mapping[str, Any]) \
                            -> Tuple[Mapping[DeviceName, Type[AbstractComputePlugin]],
                                                            Mapping[SlotName, Decimal]]:
    '''
    Detect available computing resource of the system.
    It also checks if target cluster supports CUDA by checking
    if nvidia-device-plugin-daemonset(https://github.com/NVIDIA/k8s-device-plugin) running on cluster.

    limit_cpus, limit_gpus are deprecated.
    '''

    reserved_slots = {
        'cpu':  resource_configs['reserved-cpu'],
        'mem':  resource_configs['reserved-mem'],
    }
    await K8sConfig.load_kube_config()
    k8sAppsApi = K8sClient.AppsV1Api()
    slots: MutableMapping[SlotName, Decimal] = {}

    from .intrinsic import CPUPlugin, MemoryPlugin

    compute_device_types: MutableMapping[DeviceName, Type[AbstractComputePlugin]] = {}
    compute_device_types[CPUPlugin.key] = CPUPlugin
    compute_device_types[MemoryPlugin.key] = MemoryPlugin

    entry_prefix = 'backendai_accelerator_v12'
    for entrypoint in pkg_resources.iter_entry_points(entry_prefix):
        log.info('loading accelerator plugin: {}', entrypoint.module_name)
        plugin = entrypoint.load()
        # TODO: scaling group-specific configs
        accel_klass = await plugin.init()
        if accel_klass is None:
            # plugin init failed. skip!
            continue
        if not all(sname.startswith(f'{accel_klass.key}.') for sname, _ in accel_klass.slot_types):
            raise InitializationError(
                "Slot types defined by an accelerator plugin must be prefixed "
                "by the plugin's key.")
        if accel_klass.key in compute_device_types:
            raise InitializationError(
                f"A plugin defining the same key '{accel_klass.key}' already exists. "
                "You may need to uninstall it first.")
        compute_device_types[accel_klass.key] = accel_klass

    for key, klass in compute_device_types.items():
        known_slot_types.update(klass.slot_types)
        resource_slots = await klass.available_slots()
        for skey, sval in resource_slots.items():
            slots[skey] = max(0, sval - reserved_slots.get(skey, 0))
            if slots[skey] <= 0 and skey in ('cpu', 'mem'):
                raise InitializationError(
                    f"The resource slot '{skey}' is not sufficient (zero or below zero). "
                    "Try to adjust the reserved resources or use a larger machine.")

    log.info('Resource slots: {!r}', slots)
    log.info('Slot types: {!r}', known_slot_types)
    return compute_device_types, slots
