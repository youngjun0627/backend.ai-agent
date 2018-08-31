from collections import defaultdict
from decimal import Decimal, Context as DecimalContext, ROUND_DOWN
import logging
import operator
import pkg_resources
import sys
from typing import Container, Collection, Mapping

import psutil

from .accelerator import AbstractAcceleratorInfo, ProcessorIdType, accelerator_types
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


class AcceleratorAllocMap:

    def __init__(self,
                 devices: Collection[AbstractAcceleratorInfo],
                 limit_mask: Container[ProcessorIdType]=None):
        self._ctx = DecimalContext(rounding=ROUND_DOWN)
        self.limit_mask = limit_mask
        self.devices = devices
        zero = self._ctx.create_decimal('0')
        self.device_shares = {
            p.device_id: zero for p in devices
            if limit_mask is None or p.device_id in limit_mask
        }

    def alloc(self, requested_share: Decimal, node: int=None):
        requested_share = self._ctx.create_decimal(requested_share)
        if node is None:
            # try finding a node, but this also may return "None"
            node = self._find_most_free_node()
        remaining_share = requested_share
        zero = Decimal('0')
        assert requested_share > zero, \
               'You cannot allocate zero share of devices.'
        device_shares = []
        try:
            # try filling up beginning from largest free shares
            # TODO: apply advanced scheduling and make it replacible
            while remaining_share > zero:
                p, s = self._find_largest_free_share(node)
                if s >= requested_share:
                    remaining_share = zero
                    self.device_shares[p] -= requested_share
                    device_shares.append((p, requested_share))
                elif s == 0:
                    raise RuntimeError('Cannot allocate requested shares')
                else:
                    remaining_share -= s
                    self.device_shares[p] -= s
                    device_shares.append((p, s))
        except RuntimeError:
            # revert back
            for p, s in device_shares:
                self.device_shares[p] += s
            raise
        return node, device_shares

    def free(self, device_shares: Mapping[ProcessorIdType, Decimal]):
        for proc, share in device_shares.items():
            self.device_shares[proc] -= share

    def _find_most_free_node(self):
        zero = self._ctx.create_decimal('0')
        per_node_allocs = defaultdict(lambda: zero)
        for p, s in self.device_shares:
            per_node_allocs[self.devices[p].numa_node] += s
        node, _ = min(
            ((n, alloc) for n, alloc in per_node_allocs.items()),
            key=operator.itemgetter(1))
        return node

    def _find_largest_free_share(self, node: int=None):
        shares = [
            (proc, share) for proc, share in self.device_shares
            if node is None or self.devices[proc].numa_node == node
        ]
        largest_proc_share = max(shares, key=operator.itemgetter(1))
        return largest_proc_share


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
    Detect available resource of the system and calculate mem/cpu/gpu slots.
    '''

    mem_bytes = psutil.virtual_memory().total
    num_cores = len(libnuma.get_available_cores())
    if limit_cpus is not None:
        num_cores = min(num_cores, len(limit_cpus))
    slots = {
        'mem': mem_bytes >> 20,  # MiB
        'cpu': num_cores,        # core count
        'gpu': '0.0',
    }
    entry_prefix = 'backendai_accelerator_v10'
    for entrypoint in pkg_resources.iter_entry_points(entry_prefix):
        log.info(f'loading accelerator plugin: {entrypoint.module_name}')
        plugin = entrypoint.load()
        await plugin.init(etcd)
    for accel_type, accel in accelerator_types.items():
        total_share = sum(
            dev.max_share() for dev in accel.list_devices()
            if limit_gpus is None or dev.device_id not in limit_gpus)
        slots[accel.slot_key] = str(total_share)
    log.info(f'Resource slots: {slots!r}')
    return slots
