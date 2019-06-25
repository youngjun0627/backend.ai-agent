from unittest import mock
from decimal import Decimal
import io
import json
from pathlib import Path

import pytest

from ai.backend.agent.vendor import linux
from ai.backend.agent.resources import (
    KernelResourceSpec,
    Mount, MountPermission,
)


# TODO: write tests for DiscretePropertyAllocMap, FractionAllocMap
# TODO: write tests for KernelResourceSpec


class TestLibNuma:
    def test_node_of_cpu(self):
        numa = linux.libnuma()

        # When NUMA is not supported.
        linux._numa_supported = False
        assert numa.node_of_cpu(5) == 0

        # When NUMA is supported.
        original_numa_supported = linux._numa_supported
        linux._numa_supported = True
        with mock.patch.object(linux, '_libnuma', create=True) \
                as mock_libnuma:
            numa.node_of_cpu(5)
            mock_libnuma.numa_node_of_cpu.assert_called_once_with(5)

        linux._numa_supported = original_numa_supported

    def test_num_nodes(self):
        numa = linux.libnuma()

        # When NUMA is not supported.
        linux._numa_supported = False
        assert numa.num_nodes() == 1

        # When NUMA is supported.
        original_numa_supported = linux._numa_supported
        linux._numa_supported = True
        with mock.patch.object(linux, '_libnuma', create=True) \
                as mock_libnuma:
            numa.num_nodes()
            mock_libnuma.numa_num_configured_nodes.assert_called_once_with()

        linux._numa_supported = original_numa_supported

    def test_get_available_cores_without_docker(self, monkeypatch):
        def mock_sched_getaffinity(pid):
            raise AttributeError

        def mock_requnix_session():
            raise OSError

        numa = linux.libnuma()
        monkeypatch.setattr(linux.requnix, 'Session', mock_requnix_session,
                            raising=False)
        monkeypatch.setattr(linux.os, 'sched_getaffinity',
                            mock_sched_getaffinity,
                            raising=False)
        monkeypatch.setattr(linux.os, 'cpu_count', lambda: 4)

        numa.get_available_cores.cache_clear()
        assert numa.get_available_cores() == {0, 1, 2, 3}

        def mock_sched_getaffinity2(pid):
            return {0, 1}

        monkeypatch.setattr(linux.os, 'sched_getaffinity',
                            mock_sched_getaffinity2,
                            raising=False)

        numa.get_available_cores.cache_clear()
        assert numa.get_available_cores() == {0, 1}

    def test_get_core_topology(self, mocker):
        mocker.patch.object(linux.libnuma, 'num_nodes', return_value=2)
        mocker.patch.object(linux.libnuma, 'get_available_cores',
                            return_value={1, 2, 5})
        mocker.patch.object(linux.libnuma, 'node_of_cpu', return_value=1)

        numa = linux.libnuma()
        assert numa.get_core_topology() == ([], [1, 2, 5])


'''
class TestKernelResourceSpec:

    @pytest.fixture
    def sample_resource_spec(self):
        return KernelResourceSpec(
            numa_node=99,
            cpu_set={1, 4, 9},
            memory_limit=128 * (2**20),
            scratch_disk_size=91124,
            shares={
                '_cpu': Decimal('0.47'),
                '_mem': Decimal('1.21'),
                '_gpu': Decimal('0.53'),
                'cuda': {
                    2: Decimal('0.33'),
                    5: Decimal('0.2'),
                },
            },
            mounts=[
                Mount(Path('/home/user/hello.txt'),
                      Path('/home/work/hello.txt'),
                      MountPermission.READ_ONLY),
                Mount(Path('/home/user/world.txt'),
                      Path('/home/work/world.txt'),
                      MountPermission.READ_WRITE),
            ],
        )

    def test_write_read_equality(self, sample_resource_spec):
        buffer = io.StringIO()
        sample_resource_spec.write_to_file(buffer)
        buffer.seek(0, io.SEEK_SET)
        read_spec = KernelResourceSpec.read_from_file(buffer)
        assert read_spec == sample_resource_spec

    def test_to_json(self, sample_resource_spec):
        o = json.loads(sample_resource_spec.to_json())
        assert o['cpu_set'] == [1, 4, 9]
        assert o['numa_node'] == 99
        assert o['shares']['_cpu'] == '0.47'
        assert o['shares']['_mem'] == '1.21'
        assert o['shares']['_gpu'] == '0.53'
        assert o['shares']['cuda']['2'] == '0.33'
        assert o['shares']['cuda']['5'] == '0.2'
        assert o['mounts'][0] == '/home/user/hello.txt:/home/work/hello.txt:ro'
        assert o['mounts'][1] == '/home/user/world.txt:/home/work/world.txt:rw'
'''
