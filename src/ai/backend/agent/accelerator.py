from abc import abstractmethod, ABCMeta
from typing import Any, Collection, Container, Hashable, Mapping, Sequence, TypeVar

ProcessorIdType = TypeVar('ProcessorIdType', int, str, Hashable)


class AbstractAccelerator(metaclass=ABCMeta):

    slot_key = 'accelerator'

    @classmethod
    @abstractmethod
    def slots(cls) -> float:
        return 0.0

    @classmethod
    @abstractmethod
    def list_devices(cls) -> Sequence[Collection[ProcessorIdType]]:
        '''
        Return a list of processor sets per each NUMA node.
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
