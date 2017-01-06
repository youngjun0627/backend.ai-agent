import os

import pytest
from unittest import mock

import sorna.agent.resources as resources
from sorna.agent.resources import libnuma, CPUAllocMap


class TestLibNuma:
    def test_node_of_cpu(self):
        numa = libnuma()

        # When NUMA is not supported.
        resources._numa_supported = False
        assert numa.node_of_cpu(5) == 0

        # When NUMA is supported.
        resources._numa_supported = True
        with mock.patch.object(resources, '_libnuma', create=True) \
                as mock_libnuma:
            numa.node_of_cpu(5)
            mock_libnuma.numa_node_of_cpu.assert_called_once_with(5)

    def test_num_nodes(self):
        numa = libnuma()

        # When NUMA is not supported.
        resources._numa_supported = False
        assert numa.num_nodes() == 1

        # When NUMA is supported.
        resources._numa_supported = True
        with mock.patch.object(resources, '_libnuma', create=True) \
                as mock_libnuma:
            numa.num_nodes()
            mock_libnuma.numa_num_configured_nodes.assert_called_once_with()

    def test_get_available_cores(self, monkeypatch):
        numa = libnuma()

        # When sched_getaffinity does not exist.
        def mock_sched_getaffinity(pid):
            raise AttributeError
        monkeypatch.setattr(os, 'sched_getaffinity', mock_sched_getaffinity,
                            raising=False)
        monkeypatch.setattr(os, 'cpu_count', lambda: 4)

        assert numa.get_available_cores() == {0, 1, 2, 3}

        # When sched_getaffinity does exist.
        def mock_sched_getaffinity2(_):
            return {0, 1}
        monkeypatch.setattr(os, 'sched_getaffinity', mock_sched_getaffinity2,
                            raising=False)
        assert numa.get_available_cores() == {0, 1}

    def test_get_core_topology(self, mocker):
        mocker.patch.object(libnuma, 'num_nodes', return_value=2)
        mocker.patch.object(libnuma, 'get_available_cores',
                            return_value={1, 2, 5})
        mocker.patch.object(libnuma, 'node_of_cpu', return_value=1)

        numa = libnuma()
        assert numa.get_core_topology() == ([], [1, 2, 5])
