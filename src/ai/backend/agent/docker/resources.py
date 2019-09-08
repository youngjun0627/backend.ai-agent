from decimal import Decimal
import logging
from pathlib import Path
import pkg_resources
from typing import (
    Any, Optional,
    Mapping, MutableMapping,
    Tuple,
    Type,
)

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    DeviceName, SlotName,
)
from ..exception import InitializationError
from ..resources import AbstractComputePlugin, KernelResourceSpec, known_slot_types

log = BraceStyleAdapter(logging.getLogger(__name__))


async def detect_resources(resource_configs: Mapping[str, Any],
                           plugin_configs: Mapping[str, Any]) \
                           -> Tuple[Mapping[DeviceName, Type[AbstractComputePlugin]],
                                    Mapping[SlotName, Decimal]]:
    '''
    Detect available computing resource of the system.
    It also loads the accelerator plugins.

    limit_cpus, limit_gpus are deprecated.
    '''
    reserved_slots = {
        'cpu':  resource_configs['reserved-cpu'],
        'mem':  resource_configs['reserved-mem'],
        'disk': resource_configs['reserved-disk'],
    }
    slots: MutableMapping[SlotName, Decimal] = {}

    from .intrinsic import CPUPlugin, MemoryPlugin

    compute_device_types: MutableMapping[DeviceName, Type[AbstractComputePlugin]] = {}
    compute_device_types[CPUPlugin.key] = CPUPlugin
    compute_device_types[MemoryPlugin.key] = MemoryPlugin

    entry_prefix = 'backendai_accelerator_v12'
    for entrypoint in pkg_resources.iter_entry_points(entry_prefix):
        log.info('loading accelerator plugin: {}', entrypoint.module_name)
        plugin = entrypoint.load()
        plugin_config = plugin_configs.get(plugin.PREFIX, {})
        # TODO: scaling group-specific configs
        accel_klass = await plugin.init(plugin_config)
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
        for sname, sval in resource_slots.items():
            slots[sname] = Decimal(max(0, sval - reserved_slots.get(sname, 0)))
            if slots[sname] <= 0 and sname in (SlotName('cpu'), SlotName('mem')):
                raise InitializationError(
                    f"The resource slot '{sname}' is not sufficient (zero or below zero). "
                    "Try to adjust the reserved resources or use a larger machine.")

    log.info('Resource slots: {!r}', slots)
    log.info('Slot types: {!r}', known_slot_types)
    return compute_device_types, slots


async def get_resource_spec_from_container(container_info) -> Optional[KernelResourceSpec]:
    for mount in container_info['HostConfig']['Mounts']:
        if mount['Target'] == '/home/config':
            with open(Path(mount['Source']) / 'resource.txt', 'r') as f:
                return KernelResourceSpec.read_from_file(f)
            break
    else:
        return None
