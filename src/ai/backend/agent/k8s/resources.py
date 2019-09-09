from decimal import Decimal
import logging
from typing import (
    Any, Dict, Mapping, MutableMapping,
    Type, Tuple
)

from kubernetes_asyncio import config as K8sConfig, client as K8sClient

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import SlotName, SlotTypes, DeviceId, DeviceName
from ..exception import InsufficientResource, InitializationError
from ..resources import AbstractComputePlugin, AbstractAllocMap

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.resources'))

known_slot_types: Mapping[SlotName, SlotTypes] = {}


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

    from .intrinsic import CPUPlugin, MemoryPlugin, K8sCUDAPlugin

    compute_device_types: MutableMapping[DeviceName, Type[AbstractComputePlugin]] = {}
    compute_device_types[CPUPlugin.key] = CPUPlugin
    compute_device_types[MemoryPlugin.key] = MemoryPlugin

    try:
        daemonsets = await k8sAppsApi.read_namespaced_daemon_set(
            'nvidia-device-plugin-daemonset', 'kube-system'
        )
        if daemonsets.status.number_ready > 0:
            compute_device_types[K8sCUDAPlugin.key] = K8sCUDAPlugin
    except:
        pass

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
