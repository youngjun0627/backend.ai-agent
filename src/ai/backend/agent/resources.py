from decimal import Decimal
import logging
import operator
import sys
from typing import Container, Collection, Sequence

import psutil

from .accelerator import ProcessorIdType
from .vendor.linux import libnuma

log = logging.getLogger('ai.backend.agent.resources')


class CPUAllocMap:

    def __init__(self, limit_cpus=None):
        self.limit_cpus = limit_cpus
        self.core_topo = libnuma.get_core_topology(limit_cpus)
        self.num_cores = len(libnuma.get_available_cores())
        if limit_cpus is not None:
            self.num_cores = min(self.num_cores, len(limit_cpus))
        assert sum(len(node) for node in self.core_topo) == self.num_cores
        self.num_nodes = libnuma.num_nodes()
        self.core_shares = tuple({c: 0 for c in self.core_topo[node]
                                 if limit_cpus is None or c in limit_cpus}
                                 for node in range(self.num_nodes))
        self.alloc_per_node = {n: 0 for n in range(self.num_nodes)
                               if len(self.core_shares[n]) > 0}

    def alloc(self, num_cores):
        '''
        Find a most free set of CPU cores and return a tuple of the NUMA node
        index and the set of integers indicating the found cores.
        This method guarantees that all cores are alloacted within the same
        NUMA node.
        '''
        node, current_alloc = min(
            ((n, alloc) for n, alloc in self.alloc_per_node.items()),
            key=operator.itemgetter(1))
        self.alloc_per_node[node] = current_alloc + num_cores

        shares = self.core_shares[node].copy()
        allocated_cores = set()
        for _ in range(num_cores):
            core, share = min(((core, share) for core, share in shares.items()),
                              key=operator.itemgetter(1))
            allocated_cores.add(core)
            shares[core] = sys.maxsize   # prune allocated one
            self.core_shares[node][core] += 1  # update the original share
        return node, allocated_cores

    def update(self, core_set):
        '''
        Manually add a given core set as if it is allocated by us.
        '''
        any_core = next(iter(core_set))
        node = libnuma.node_of_cpu(any_core)
        self.alloc_per_node[node] += len(core_set)
        for c in core_set:
            self.core_shares[node][c] += 1

    def free(self, core_set):
        '''
        Remove the given set of CPU cores from the allocated shares.
        It assumes that all cores are in the same NUMA node.
        '''
        any_core = next(iter(core_set))
        node = libnuma.node_of_cpu(any_core)
        self.alloc_per_node[node] -= len(core_set)
        for c in core_set:
            self.core_shares[node][c] -= 1


class ProcessorAllocMap:

    def __init__(self,
                 processors_per_node: Sequence[Collection[ProcessorIdType]],
                 max_share_per_processor: Decimal=Decimal(0),
                 limit_mask: Container[ProcessorIdType]=None):
        self.limit_mask = limit_mask
        self.procs_per_node = processors_per_node
        self.max_share_per_proc = max_share_per_processor
        self.num_nodes = libnuma.num_nodes()
        self.num_processors = sum(len(p) for p in processors_per_node)
        if limit_mask is not None:
            self.num_processors = min(self.num_processors, len(limit_mask))

        self.node_membership = {}
        for node, procs in enumerate(processors_per_node):
            for p in procs:
                self.node_membership[p] = node
        self.proc_shares = tuple({p: Decimal(0) for p in self.procs_per_node[node]
                                 if limit_mask is None or p in limit_mask}
                                 for node in range(self.num_nodes))
        self.node_shares = {n: Decimal(0) for n in range(self.num_nodes)
                            if len(self.proc_shares[n]) > 0}

    def alloc_by_share(self, share: Decimal=Decimal('1.0')):
        node = 0
        allocated_procs = []
        if share > Decimal('1.0'):
            # TODO: allocate from multiple processors
            # TODO: evenly spread the share or fill from free ones?
            pass
        else:
            # TODO: allocate from single processor
            pass
        return node, allocated_procs

    def alloc_by_proc(self,
              num_processors: int,
              share_per_proc: Decimal=Decimal('1.0')):
        '''
        Find a most free set of processors and return a tuple of the NUMA node
        index and the set of allocated processor IDs.
        This method guarantees that all processors are alloacted within the same
        NUMA node.
        '''
        node, current_alloc = min(
            ((n, alloc) for n, alloc in self.node_shares.items()),
            key=operator.itemgetter(1))
        self.node_shares[node] = (
            current_alloc + (share_per_proc * num_processors))

        shares = self.proc_shares[node].copy()
        allocated_procs = set()
        for _ in range(num_processors):
            pshares = [(proc, share) for proc, share in shares.items()
                       if self.max_share_per_proc == Decimal(0) or
                          share + share_per_proc <= self.max_share_per_proc]
            assert len(pshares) > 0, 'Cannot allocate more shares'
            proc, share = min(pshares, key=operator.itemgetter(1))
            allocated_procs.add(proc)
            shares[proc] = self.max_share_per_proc + Decimal(1)  # prune allocated
            self.proc_shares[node][proc] += share_per_proc
        return node, allocated_procs

    def update(self,
               proc_set: Collection[ProcessorIdType],
               share_per_proc: Decimal):
        '''
        Manually add a given processor set as if it is allocated by us,
        regardless of their NUMA node membership.
        '''
        any_proc = next(iter(proc_set))
        node = self.node_membership[any_proc]
        self.node_shares[node] += len(proc_set)
        for p in proc_set:
            self.proc_shares[node][p] += share_per_proc

    def free(self,
             proc_set: Collection[ProcessorIdType],
             share_per_proc: Decimal):
        '''
        Remove the given set of processors from the allocated shares.
        It assumes that all processors are in the same NUMA node.
        '''
        any_proc = next(iter(proc_set))
        node = self.node_membership[any_proc]
        self.node_shares[node] -= len(proc_set)
        for p in proc_set:
            self.proc_shares[node][p] -= share_per_proc


def bitmask2set(mask):
    bpos = 0
    bset = []
    while mask > 0:
        if (mask & 1) == 1:
            bset.append(bpos)
        mask = (mask >> 1)
        bpos += 1
    return frozenset(bset)


def detect_slots(limit_cpus=None, limit_gpus=None):
    '''
    Detect available resource of the system and calculate mem/cpu/gpu slots.
    '''

    mem_bytes = psutil.virtual_memory().total
    num_cores = len(libnuma.get_available_cores())
    if limit_cpus is not None:
        num_cores = min(num_cores, len(limit_cpus))
    slots = {
        'mem': mem_bytes >> 20,  # MiB
        'cpu': num_cores,        # core count
    }
    # TODO: generalize as plugins of accelerators
    from .gpu import CUDAAccelerator
    accelerator_types = [
        CUDAAccelerator,
    ]
    for accel in accelerator_types:
        slots[accel.slot_key] = accel.slots(limit_gpus)
    log.info(f'Resource slots: {slots!r}')
    return slots
