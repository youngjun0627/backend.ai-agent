from decimal import Decimal
import logging
from typing import (
    Any, Collection, List, Mapping, Sequence
)

from kubernetes_asyncio import config as K8sConfig, client as K8sClient

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    DeviceName, DeviceId,
    SlotName, SlotTypes
)
from .. import __version__
from ..resources import (
    AbstractAllocMap,
    DiscretePropertyAllocMap,
    AbstractComputeDevice,
    AbstractComputePlugin,
)
from ..stats import (
    StatContext, NodeMeasurement, ContainerMeasurement
)

log = BraceStyleAdapter(logging.getLogger(__name__))

# Pseudo-plugins for intrinsic devices (CPU and the main memory)


class CPUDevice(AbstractComputeDevice):
    pass


class CPUPlugin(AbstractComputePlugin):
    '''
    Represents the CPU.
    '''

    key = DeviceName('cpu')
    slot_types = [
        (SlotName('cpu'), SlotTypes.COUNT)
    ]

    @classmethod
    async def list_devices(cls) -> Collection[AbstractComputeDevice]:
        devices: List[AbstractComputeDevice] = []
        await K8sConfig.load_kube_config()
        k8sCoreApi = K8sClient.CoreV1Api()

        nodes = await k8sCoreApi.list_node()

        for node in nodes.items:
            cores = int(node.status.capacity['cpu'])
            devices.append(CPUDevice(
                device_id=DeviceId(str(len(devices))),
                hw_location='root',
                numa_node=0,
                memory_size=0,
                processing_units=cores,  # TODO: hyper-threading? / over-commit factor?
            ))
        return devices

    @classmethod
    async def available_slots(cls) -> Mapping[SlotName, Decimal]:
        devices = await cls.list_devices()
        return {
            SlotName('cpu'): Decimal(sum(dev.processing_units for dev in devices)),
        }

    @classmethod
    def get_version(cls) -> str:
        return __version__

    @classmethod
    async def extra_info(cls) -> Mapping[str, str]:
        return {
            'agent_version': __version__,
            'machine': 'Kubernetes',
            'os_type': 'Kubernetes',
        }

    @classmethod
    async def gather_node_measures(cls, ctx: StatContext) -> Sequence[NodeMeasurement]:
        # TODO: Implement
        pass

    @classmethod
    async def gather_container_measures(cls, ctx: StatContext, container_ids: Sequence[str]) \
            -> Sequence[ContainerMeasurement]:
        # TODO: Implement
        pass

    @classmethod
    async def create_alloc_map(cls) -> AbstractAllocMap:
        devices = await cls.list_devices()
        return DiscretePropertyAllocMap(
            devices=devices,
            prop_func=lambda dev: dev.processing_units)


class MemoryDevice(AbstractComputeDevice):
    pass


class MemoryPlugin(AbstractComputePlugin):
    '''
    Represents the main memory.

    When collecting statistics, it also measures network and I/O usage
    in addition to the memory usage.
    '''

    key = DeviceName('mem')
    slot_types = [
        (SlotName('mem'), SlotTypes.BYTES)
    ]

    @classmethod
    async def list_devices(cls) -> Collection[MemoryDevice]:
        devices = []

        await K8sConfig.load_kube_config()
        k8sCoreApi = K8sClient.CoreV1Api()

        nodes = await k8sCoreApi.list_node()

        for node in nodes.items:
            is_master = False
            for taint in node.spec.taints if node.spec.taints != None else []:
                if taint.key == 'node-role.kubernetes.io/master' and taint.effect == 'NoSchedule':
                    is_master = True
                    break
        
            if is_master:
                continue
            memory_size = int(node.status.capacity['memory'][:-2]) * 1024
            devices.append(MemoryDevice(
                device_id=DeviceId('root'),
                hw_location='root',
                numa_node=0,
                memory_size=memory_size,
                processing_units=0,
            ))

        return devices

    @classmethod
    async def available_slots(cls) -> Mapping[SlotName, Decimal]:
        devices = await cls.list_devices()
        return {
            SlotName('mem'): Decimal(sum(dev.memory_size for dev in devices)),
        }

    @classmethod
    def get_version(cls) -> str:
        return __version__

    @classmethod
    async def extra_info(cls) -> Mapping[str, str]:
        return {}

    @classmethod
    async def gather_node_measures(cls, ctx: StatContext) -> Sequence[NodeMeasurement]:
        # TODO: Implement
        pass

    @classmethod
    async def gather_container_measures(cls, ctx: StatContext, container_ids: Sequence[str]) \
            -> Sequence[ContainerMeasurement]:
        # TODO: Implement
        pass

    @classmethod
    async def create_alloc_map(cls) -> AbstractAllocMap:
        devices = await cls.list_devices()
        return DiscretePropertyAllocMap(
            devices=devices,
            prop_func=lambda dev: dev.memory_size)
