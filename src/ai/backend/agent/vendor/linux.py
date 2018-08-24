import ctypes, ctypes.util
import functools
import os
import sys

import requests_unixsocket as requnix

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
