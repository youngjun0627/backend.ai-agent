from unittest import mock

import ai.backend.agent.resources as resources
from ai.backend.agent.resources import libnuma, CPUAllocMap


class TestLibNuma:
    def test_node_of_cpu(self):
        numa = libnuma()

        # When NUMA is not supported.
        resources._numa_supported = False
        assert numa.node_of_cpu(5) == 0

        # When NUMA is supported.
        original_numa_supported = resources._numa_supported
        resources._numa_supported = True
        with mock.patch.object(resources, '_libnuma', create=True) \
                as mock_libnuma:
            numa.node_of_cpu(5)
            mock_libnuma.numa_node_of_cpu.assert_called_once_with(5)

        resources._numa_supported = original_numa_supported

    def test_num_nodes(self):
        numa = libnuma()

        # When NUMA is not supported.
        resources._numa_supported = False
        assert numa.num_nodes() == 1

        # When NUMA is supported.
        original_numa_supported = resources._numa_supported
        resources._numa_supported = True
        with mock.patch.object(resources, '_libnuma', create=True) \
                as mock_libnuma:
            numa.num_nodes()
            mock_libnuma.numa_num_configured_nodes.assert_called_once_with()

        resources._numa_supported = original_numa_supported

    def test_get_available_cores_without_docker(self, monkeypatch):

        def mock_sched_getaffinity(pid):
            raise AttributeError

        def mock_requnix_session():
            raise OSError

        numa = libnuma()
        monkeypatch.setattr(resources.requnix, 'Session', mock_requnix_session,
                            raising=False)
        monkeypatch.setattr(resources.os, 'sched_getaffinity',
                            mock_sched_getaffinity,
                            raising=False)
        monkeypatch.setattr(resources.os, 'cpu_count', lambda: 4)

        numa.get_available_cores.cache_clear()
        assert numa.get_available_cores() == {0, 1, 2, 3}

        def mock_sched_getaffinity2(pid):
            return {0, 1}

        monkeypatch.setattr(resources.os, 'sched_getaffinity',
                            mock_sched_getaffinity2,
                            raising=False)

        numa.get_available_cores.cache_clear()
        assert numa.get_available_cores() == {0, 1}

    def test_get_core_topology(self, mocker):
        mocker.patch.object(libnuma, 'num_nodes', return_value=2)
        mocker.patch.object(libnuma, 'get_available_cores',
                            return_value={1, 2, 5})
        mocker.patch.object(libnuma, 'node_of_cpu', return_value=1)

        numa = libnuma()
        assert numa.get_core_topology() == ([], [1, 2, 5])


class TestCPUAllocMap:
    def get_cpu_alloc_map(self, num_nodes=1, num_cores=1):
        core_topo = tuple([] for _ in range(num_nodes))
        for c in {_ for _ in range(num_cores)}:
            n = c % num_nodes
            core_topo[n].append(c)
        avail_cores = {n for n in range(num_cores)}

        with mock.patch.object(libnuma, 'get_core_topology',
                               return_value=core_topo):
            with mock.patch.object(libnuma, 'get_available_cores',
                                   return_value=avail_cores):
                with mock.patch.object(libnuma, 'num_nodes',
                                       return_value=num_nodes):
                    return CPUAllocMap()

    def test_cpu_alloc_map_initialization(self):
        cpu_alloc_map = self.get_cpu_alloc_map(num_nodes=3, num_cores=4)

        assert cpu_alloc_map.core_topo == ([0, 3], [1], [2])
        assert cpu_alloc_map.num_cores == 4
        assert cpu_alloc_map.num_nodes == 3
        assert cpu_alloc_map.alloc_per_node == [0, 0, 0]
        assert cpu_alloc_map.core_shares == ([0, 0], [0], [0])

    def test_alloc(self):
        cpu_alloc_map = self.get_cpu_alloc_map(num_nodes=3, num_cores=4)

        assert cpu_alloc_map.alloc_per_node == [0, 0, 0]
        assert cpu_alloc_map.core_shares == ([0, 0], [0], [0])

        assert cpu_alloc_map.alloc(3) == (0, {0, 3})
        assert cpu_alloc_map.alloc_per_node == [3, 0, 0]
        assert cpu_alloc_map.core_shares == ([2, 1], [0], [0])

        assert cpu_alloc_map.alloc(2) == (1, {1})
        assert cpu_alloc_map.alloc_per_node == [3, 2, 0]
        assert cpu_alloc_map.core_shares == ([2, 1], [2], [0])

        assert cpu_alloc_map.alloc(3) == (2, {2})
        assert cpu_alloc_map.alloc_per_node == [3, 2, 3]
        assert cpu_alloc_map.core_shares == ([2, 1], [2], [3])

        assert cpu_alloc_map.alloc(4) == (1, {1})  # 2nd node least populated
        assert cpu_alloc_map.alloc_per_node == [3, 6, 3]
        assert cpu_alloc_map.core_shares == ([2, 1], [6], [3])

    def test_free(self):
        cpu_alloc_map = self.get_cpu_alloc_map(num_nodes=3, num_cores=4)
        cpu_alloc_map.alloc_per_node = [3, 6, 3]
        cpu_alloc_map.core_shares = ([2, 1], [6], [3])

        with mock.patch.object(libnuma, 'node_of_cpu', return_value=0):
            cpu_alloc_map.free({0, 3})
            assert cpu_alloc_map.alloc_per_node == [1, 6, 3]
            assert cpu_alloc_map.core_shares == ([1, 0], [6], [3])

            cpu_alloc_map.free({0})
            assert cpu_alloc_map.alloc_per_node == [0, 6, 3]
            assert cpu_alloc_map.core_shares == ([0, 0], [6], [3])

        with mock.patch.object(libnuma, 'node_of_cpu', return_value=1):
            cpu_alloc_map.free({1})
            assert cpu_alloc_map.alloc_per_node == [0, 5, 3]
            assert cpu_alloc_map.core_shares == ([0, 0], [5], [3])

        with mock.patch.object(libnuma, 'node_of_cpu', return_value=2):
            cpu_alloc_map.free({2})
            assert cpu_alloc_map.alloc_per_node == [0, 5, 2]
            assert cpu_alloc_map.core_shares == ([0, 0], [5], [2])
