import ctypes, ctypes.util
import functools
import logging
import operator
import os
import sys

import psutil
import requests
import requests_unixsocket as requnix

log = logging.getLogger('ai.backend.agent.resources')

_numa_supported = False

if sys.platform == 'linux':
    _libnuma_path = ctypes.util.find_library('numa')
    if _libnuma_path:
        _libnuma = ctypes.CDLL(_libnuma_path)
        _numa_supported = True


class libnuma:

    @staticmethod
    def node_of_cpu(core):
        if _numa_supported:
            return int(_libnuma.numa_node_of_cpu(core))
        else:
            return 0

    @staticmethod
    def num_nodes():
        if _numa_supported:
            return int(_libnuma.numa_num_configured_nodes())
        else:
            return 1

    @staticmethod
    @functools.lru_cache(maxsize=1)
    def get_available_cores():
        try:
            # Try to get the # cores allocated to Docker first.
            with requnix.Session() as sess:
                resp = sess.get('http+unix://%2fvar%2frun%2fdocker.sock/info')
                assert resp.status_code == 200
                return {idx for idx in range(resp.json()['NCPU'])}
        except:
            try:
                return os.sched_getaffinity(os.getpid())
            except AttributeError:
                return {idx for idx in range(os.cpu_count())}

    @staticmethod
    def get_core_topology(limit_cpus=None):
        topo = tuple([] for _ in range(libnuma.num_nodes()))
        for c in libnuma.get_available_cores():
            if limit_cpus is not None and c not in limit_cpus:
                continue
            n = libnuma.node_of_cpu(c)
            topo[n].append(c)
        return topo


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


def gpu_count():
    '''
    Get the number of GPU devices installed in this system
    available via the nvidia-docker plugin.
    '''

    try:
        r = requests.get('http://localhost:3476/gpu/info/json', timeout=0.5)
        gpu_info = r.json()
    except requests.exceptions.ConnectionError:
        return 0
    return len(gpu_info['Devices'])


def bitmask2set(mask):
    bpos = 0
    bset = []
    while mask > 0:
        if (mask & 1) == 1:
            bset.append(bpos)
        mask = (mask >> 1)
        bpos += 1
    return frozenset(bset)


def detect_slots(limit_cpus, limit_gpus):
    '''
    Detect available resource of the system and calculate mem/cpu/gpu slots.
    '''

    mem_bytes = psutil.virtual_memory().total
    num_cores = len(libnuma.get_available_cores())
    if limit_cpus is not None:
        num_cores = min(num_cores, len(limit_cpus))
    num_gpus = gpu_count()
    if limit_gpus is not None:
        num_gpus = min(num_gpus, len(limit_gpus))
    log.info('Resource slots: cpu=%d, gpu=%d, mem=%dMiB',
             num_cores, num_gpus, mem_bytes >> 20)
    return (
        mem_bytes >> 20,  # MiB
        num_cores,        # core count
        num_gpus,         # device count
    )
