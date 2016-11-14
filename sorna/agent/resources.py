import ctypes, ctypes.util
import os
import sys

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
    def get_available_cores():
        try:
            return os.sched_getaffinity(os.getpid())
        except AttributeError:
            return {idx for idx in range(os.cpu_count())}

    @staticmethod
    def get_core_topology():
        topo = tuple([] for _ in range(libnuma.num_nodes()))
        for c in libnuma.get_available_cores():
            n = libnuma.node_of_cpu(c)
            topo[n].append(c)
        return topo


class CPUAllocMap:

    def __init__(self):
        pass

