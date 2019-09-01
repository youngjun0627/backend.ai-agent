import asyncio
from decimal import Decimal
import logging
import os
from pathlib import Path
import platform
from typing import Any, Collection, Mapping, Sequence

import aiohttp
from aiodocker.docker import DockerContainer
from aiodocker.exceptions import DockerError
import psutil

from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.utils import nmget
from .resources import get_resource_spec_from_container
from ..resources import (
    DiscretePropertyAllocMap,
    AbstractComputeDevice,
    AbstractComputePlugin,
)
from .. import __version__
from ..stats import (
    StatContext, NodeMeasurement, ContainerMeasurement,
    StatModes, MetricTypes, Measurement,
)
from ..utils import read_sysfs, current_loop
from ..vendor.linux import libnuma

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.intrinsic'))


# Pseudo-plugins for intrinsic devices (CPU and the main memory)

class CPUDevice(AbstractComputeDevice):
    pass


class CPUPlugin(AbstractComputePlugin):
    '''
    Represents the CPU.
    '''

    key = 'cpu'
    slot_types = [
        ('cpu', 'count')
    ]

    @classmethod
    async def list_devices(cls) -> Collection[CPUDevice]:
        num_cores = libnuma.get_available_cores()
        overcommit_factor = int(os.environ.get('BACKEND_CPU_OVERCOMMIT_FACTOR', '1'))
        assert 1 <= overcommit_factor <= 4
        return [
            CPUDevice(
                device_id=str(core_idx),
                hw_location='root',
                numa_node=libnuma.node_of_cpu(core_idx),
                memory_size=0,
                processing_units=1 * overcommit_factor,
            )
            for core_idx in sorted(num_cores)
        ]

    @classmethod
    async def available_slots(cls) -> Mapping[str, str]:
        devices = await cls.list_devices()
        return {
            'cpu': sum(dev.processing_units for dev in devices),
        }

    @classmethod
    def get_version(cls) -> str:
        return __version__

    @classmethod
    async def extra_info(cls) -> Mapping[str, str]:
        return {
            'agent_version': __version__,
            'machine': platform.machine(),
            'os_type': platform.system(),
        }

    @classmethod
    async def gather_node_measures(cls, ctx: StatContext) -> Sequence[NodeMeasurement]:
        _cstat = psutil.cpu_times(True)
        q = Decimal('0.000')
        total_cpu_used = sum((Decimal(c.user + c.system) * 1000).quantize(q) for c in _cstat)
        now, interval = ctx.update_timestamp('cpu-node')
        interval = Decimal(interval * 1000).quantize(q)

        return [
            NodeMeasurement(
                'cpu_util', MetricTypes.UTILIZATION,
                unit_hint='msec',
                current_hook=lambda metric: metric.stats.diff,
                per_node=Measurement(total_cpu_used, interval),
                per_device={str(idx): Measurement((Decimal(c.user + c.system) * 1000).quantize(q),
                                                  interval)
                            for idx, c in enumerate(_cstat)},
            ),
        ]

    @classmethod
    async def gather_container_measures(cls, ctx: StatContext, container_ids: Sequence[str]) \
            -> Sequence[ContainerMeasurement]:

        async def sysfs_impl(container_id):
            cpu_prefix = f'/sys/fs/cgroup/cpuacct/docker/{container_id}/'
            try:
                cpu_used = read_sysfs(cpu_prefix + 'cpuacct.usage') / 1e6
            except IOError as e:
                log.warning('cannot read stats: sysfs unreadable for container {0}\n{1!r}',
                            container_id[:7], e)
                return None
            return cpu_used

        async def api_impl(container_id):
            try:
                container = DockerContainer(ctx.agent.docker, id=container_id)
                ret = await container.stats(stream=False)  # TODO: cache
            except RuntimeError as e:
                msg = str(e.args[0]).lower()
                if 'event loop is closed' in msg or 'session is closed' in msg:
                    return None
                raise
            except (DockerError, aiohttp.ClientError):
                short_cid = container._id[:7]
                log.warning(f'cannot read stats: Docker stats API error for {short_cid}.')
                return None
            else:
                # API returned successfully but actually the result may be empty!
                if ret is None:
                    return None
                if ret['preread'].startswith('0001-01-01'):
                    return None
            cpu_used = nmget(ret, 'cpu_stats.cpu_usage.total_usage', 0) / 1e6
            return cpu_used

        if ctx.mode == StatModes.CGROUP:
            impl = sysfs_impl
        elif ctx.mode == StatModes.DOCKER:
            impl = api_impl

        q = Decimal('0.000')
        per_container_cpu_used = {}
        tasks = []
        for cid in container_ids:
            tasks.append(asyncio.ensure_future(impl(cid)))
        results = await asyncio.gather(*tasks)
        for cid, cpu_used in zip(container_ids, results):
            if cpu_used is None:
                continue
            per_container_cpu_used[cid] = Measurement(Decimal(cpu_used).quantize(q))
        return [
            ContainerMeasurement(
                'cpu_util', MetricTypes.UTILIZATION,
                unit_hint='percent',
                current_hook=lambda metric: metric.stats.diff,
                stats_filter={'avg', 'max'},
                per_container=per_container_cpu_used,
            ),
            ContainerMeasurement(
                'cpu_used', MetricTypes.USAGE,
                unit_hint='msec',
                per_container=per_container_cpu_used.copy(),
            ),
        ]

    @classmethod
    async def create_alloc_map(cls) -> Mapping[str, str]:
        devices = await cls.list_devices()
        return DiscretePropertyAllocMap(
            devices=devices,
            prop_func=lambda dev: dev.processing_units)

    @classmethod
    async def get_hooks(cls, distro: str, arch: str) -> Sequence[Path]:
        # TODO: move the sysconf hook in libbaihook.so here
        return []

    @classmethod
    async def generate_docker_args(cls,
                                   docker: 'aiodocker.docker.Docker',  # noqa
                                   device_alloc,
                                  ) -> Mapping[str, Any]:
        cores = [*device_alloc['cpu'].keys()]
        return {
            'HostConfig': {
                'CpuPeriod': 100_000,  # docker default
                'CpuQuota': int(100_000 * len(cores)),
                'Cpus': ','.join(sorted(cores)),
                'CpusetCpus': ','.join(sorted(cores)),
                # 'CpusetMems': f'{resource_spec.numa_node}',
            }
        }

    @classmethod
    async def restore_from_container(cls, container, alloc_map):
        assert isinstance(alloc_map, DiscretePropertyAllocMap)
        # Docker does not return the original cpuset.... :(
        # We need to read our own records.
        resource_spec = await get_resource_spec_from_container(container)
        if resource_spec is None:
            return
        alloc_map.allocations['cpu'].update(
            resource_spec.allocations['cpu']['cpu'])

    @classmethod
    async def get_attached_devices(cls, device_alloc) -> Sequence[Mapping[str, str]]:
        device_ids = [*device_alloc['cpu'].keys()]
        available_devices = await cls.list_devices()
        attached_devices = []
        for device in available_devices:
            if device.device_id in device_ids:
                attached_devices.append({
                    'device_id': device.device_id,
                    'model_name': '',
                })
        return attached_devices


class MemoryDevice(AbstractComputeDevice):
    pass


class MemoryPlugin(AbstractComputePlugin):
    '''
    Represents the main memory.

    When collecting statistics, it also measures network and I/O usage
    in addition to the memory usage.
    '''

    key = 'mem'
    slot_types = [
        ('mem', 'bytes')
    ]

    @classmethod
    async def list_devices(cls) -> Collection[MemoryDevice]:
        # TODO: support NUMA?
        memory_size = psutil.virtual_memory().total
        return [MemoryDevice(
            device_id='root',
            hw_location='root',
            numa_node=0,
            memory_size=memory_size,
            processing_units=0,
        )]

    @classmethod
    async def available_slots(cls) -> Mapping[str, str]:
        devices = await cls.list_devices()
        return {
            'mem': sum(dev.memory_size for dev in devices),
        }

    @classmethod
    def get_version(cls) -> str:
        return __version__

    @classmethod
    async def extra_info(cls) -> Mapping[str, str]:
        return {}

    @classmethod
    async def gather_node_measures(cls, ctx: StatContext) -> Sequence[NodeMeasurement]:
        _mstat = psutil.virtual_memory()
        total_mem_used_bytes = Decimal(_mstat.total - _mstat.available)
        total_mem_capacity_bytes = Decimal(_mstat.total)
        _nstat = psutil.net_io_counters()
        net_rx_bytes = _nstat.bytes_recv
        net_tx_bytes = _nstat.bytes_sent

        def get_disk_stat():
            pruned_disk_types = frozenset(['squashfs', 'vfat', 'tmpfs'])
            total_disk_usage = Decimal(0)
            total_disk_capacity = Decimal(0)
            per_disk_stat = {}
            for disk_info in psutil.disk_partitions():
                if disk_info.fstype not in pruned_disk_types:
                    dstat = os.statvfs(disk_info.mountpoint)
                    disk_usage = Decimal(dstat.f_frsize * (dstat.f_blocks - dstat.f_bavail))
                    disk_capacity = Decimal(dstat.f_frsize * dstat.f_blocks)
                    per_disk_stat[disk_info.device] = Measurement(disk_usage, disk_capacity)
                    total_disk_usage += disk_usage
                    total_disk_capacity += disk_capacity
            return total_disk_usage, total_disk_capacity, per_disk_stat

        loop = current_loop()
        total_disk_usage, total_disk_capacity, per_disk_stat = \
            await loop.run_in_executor(None, get_disk_stat)
        return [
            NodeMeasurement(
                'mem', MetricTypes.USAGE,
                unit_hint='bytes',
                stats_filter={'max'},
                per_node=Measurement(total_mem_used_bytes, total_mem_capacity_bytes),
                per_device={'root': Measurement(total_mem_used_bytes, total_mem_capacity_bytes)},
            ),
            NodeMeasurement(
                'disk', MetricTypes.USAGE,
                unit_hint='bytes',
                per_node=Measurement(total_disk_usage, total_disk_capacity),
                per_device=per_disk_stat,
            ),
            NodeMeasurement(
                'net_rx', MetricTypes.RATE,
                unit_hint='bps',
                current_hook=lambda metric: metric.stats.rate,
                per_node=Measurement(Decimal(net_rx_bytes)),
                per_device={'node': Measurement(Decimal(net_rx_bytes))},
            ),
            NodeMeasurement(
                'net_tx', MetricTypes.RATE,
                unit_hint='bps',
                current_hook=lambda metric: metric.stats.rate,
                per_node=Measurement(Decimal(net_tx_bytes)),
                per_device={'node': Measurement(Decimal(net_tx_bytes))},
            ),
        ]

    @classmethod
    async def gather_container_measures(cls, ctx: StatContext, container_ids: Sequence[str]) \
            -> Sequence[ContainerMeasurement]:

        def get_scratch_size(container_id: str) -> int:
            for kernel_id, info in ctx.agent.container_registry.items():
                if info['container_id'] == container_id:
                    break
            else:
                return 0
            work_dir = ctx.agent.config['container']['scratch-root'] / kernel_id / 'work'
            total_size = 0
            for path in work_dir.rglob('*'):
                if path.is_symlink():
                    total_size += path.lstat().st_size
                elif path.is_file():
                    total_size += path.stat().st_size
            return total_size

        async def sysfs_impl(container_id):
            mem_prefix = f'/sys/fs/cgroup/memory/docker/{container_id}/'
            io_prefix = f'/sys/fs/cgroup/blkio/docker/{container_id}/'
            try:
                mem_cur_bytes = read_sysfs(mem_prefix + 'memory.usage_in_bytes')
                active_anon = read_sysfs(mem_prefix + 'memory_stats.stats.active_anon')
                inactive_anon = read_sysfs(mem_prefix + 'memory_stats.stats.inactive_anon')
                active_file = read_sysfs(mem_prefix + 'memory_stats.stats.active_file')
                inactive_file = read_sysfs(mem_prefix + 'memory_stats.stats.inactive_file')
                cache = read_sysfs(mem_prefix + 'memory_stats.stats.cache')
                rss = read_sysfs(mem_prefix + 'memory_stats.stats.rss')
                shared_mem_cur_bytes = (
                    (active_anon + inactive_anon - active_file - inactive_file - rss + cache) / 2.0
                )
                io_stats = Path(io_prefix + 'blkio.throttle.io_service_bytes').read_text()
                # example data:
                #   8:0 Read 13918208
                #   8:0 Write 0
                #   8:0 Sync 0
                #   8:0 Async 13918208
                #   8:0 Total 13918208
                #   Total 13918208
                io_read_bytes = 0
                io_write_bytes = 0
                for line in io_stats.splitlines():
                    if line.startswith('Total '):
                        continue
                    dev, op, nbytes = line.strip().split()
                    if op == 'Read':
                        io_read_bytes += int(nbytes)
                    elif op == 'Write':
                        io_write_bytes += int(nbytes)
            except IOError as e:
                log.warning('cannot read stats: sysfs unreadable for container {0}\n{1!r}',
                            container_id[:7], e)
                return None
            loop = current_loop()
            scratch_sz = await loop.run_in_executor(
                None, get_scratch_size, container_id)
            return mem_cur_bytes, io_read_bytes, io_write_bytes, scratch_sz, shared_mem_cur_bytes

        async def api_impl(container_id):
            try:
                container = DockerContainer(ctx.agent.docker, id=container_id)
                ret = await container.stats(stream=False)  # TODO: cache
            except RuntimeError as e:
                msg = str(e.args[0]).lower()
                if 'event loop is closed' in msg or 'session is closed' in msg:
                    return None
                raise
            except (DockerError, aiohttp.ClientError):
                short_cid = container._id[:7]
                log.warning(f'cannot read stats: Docker stats API error for {short_cid}.')
                return None
            else:
                # API returned successfully but actually the result may be empty!
                if ret is None:
                    return None
                if ret['preread'].startswith('0001-01-01'):
                    return None
            mem_cur_bytes = nmget(ret, 'memory_stats.usage', 0)
            active_anon = nmget(ret, 'memory_stats.stats.active_anon', 0)
            inactive_anon = nmget(ret, 'memory_stats.stats.inactive_anon', 0)
            active_file = nmget(ret, 'memory_stats.stats.active_file', 0)
            inactive_file = nmget(ret, 'memory_stats.stats.inactive_file', 0)
            cache = nmget(ret, 'memory_stats.stats.cache', 0)
            rss = nmget(ret, 'memory_stats.stats.rss', 0)
            shared_mem_cur_bytes = (
                (active_anon + inactive_anon - active_file - inactive_file - rss + cache) / 2.0
            )
            io_read_bytes = 0
            io_write_bytes = 0
            for item in nmget(ret, 'blkio_stats.io_service_bytes_recursive', []):
                if item['op'] == 'Read':
                    io_read_bytes += item['value']
                elif item['op'] == 'Write':
                    io_write_bytes += item['value']
            loop = current_loop()
            scratch_sz = await loop.run_in_executor(
                None, get_scratch_size, container_id)
            return mem_cur_bytes, io_read_bytes, io_write_bytes, scratch_sz, shared_mem_cur_bytes

        if ctx.mode == StatModes.CGROUP:
            impl = sysfs_impl
        elif ctx.mode == StatModes.DOCKER:
            impl = api_impl

        per_container_mem_used_bytes = {}
        per_container_io_read_bytes = {}
        per_container_io_write_bytes = {}
        per_container_io_scratch_size = {}
        per_container_shared_mem_bytes = {}
        tasks = []
        for cid in container_ids:
            tasks.append(asyncio.ensure_future(impl(cid)))
        results = await asyncio.gather(*tasks)
        for cid, result in zip(container_ids, results):
            if result is None:
                continue
            per_container_mem_used_bytes[cid] = Measurement(
                Decimal(result[0]))
            per_container_io_read_bytes[cid] = Measurement(
                Decimal(result[1]))
            per_container_io_write_bytes[cid] = Measurement(
                Decimal(result[2]))
            per_container_io_scratch_size[cid] = Measurement(
                Decimal(result[3]))
            per_container_shared_mem_bytes[cid] = Measurement(
                Decimal(result[4]))
        return [
            ContainerMeasurement(
                'mem', MetricTypes.USAGE,
                unit_hint='bytes',
                stats_filter={'max'},
                per_container=per_container_mem_used_bytes,
            ),
            ContainerMeasurement(
                'io_read', MetricTypes.USAGE,
                unit_hint='bytes',
                stats_filter={'rate'},
                per_container=per_container_io_read_bytes,
            ),
            ContainerMeasurement(
                'io_write', MetricTypes.USAGE,
                unit_hint='bytes',
                stats_filter={'rate'},
                per_container=per_container_io_write_bytes,
            ),
            ContainerMeasurement(
                'io_scratch_size', MetricTypes.USAGE,
                unit_hint='bytes',
                stats_filter={'max'},
                per_container=per_container_io_scratch_size,
            ),
            ContainerMeasurement(
                'shared_mem', MetricTypes.USAGE,
                unit_hint='bytes',
                stats_filter={'max'},
                per_container=per_container_shared_mem_bytes,
            ),
        ]

    @classmethod
    async def create_alloc_map(cls) -> Mapping[str, str]:
        devices = await cls.list_devices()
        return DiscretePropertyAllocMap(
            devices=devices,
            prop_func=lambda dev: dev.memory_size)

    @classmethod
    async def get_hooks(cls, distro: str, arch: str) -> Sequence[Path]:
        return []

    @classmethod
    async def generate_docker_args(cls,
                                   docker: 'aiodocker.docker.Docker',  # noqa
                                   device_alloc,
                                  ) -> Mapping[str, Any]:
        memory = sum(device_alloc['mem'].values())
        return {
            'HostConfig': {
                'MemorySwap': memory,  # prevent using swap!
                'Memory': memory,
            }
        }

    @classmethod
    async def restore_from_container(cls, container, alloc_map):
        assert isinstance(alloc_map, DiscretePropertyAllocMap)
        memory_limit = container['HostConfig']['Memory']
        alloc_map.allocations['mem']['root'] += memory_limit

    @classmethod
    async def get_attached_devices(cls, device_alloc) -> Sequence[Mapping[str, str]]:
        device_ids = [*device_alloc['mem'].keys()]
        available_devices = await cls.list_devices()
        attached_devices = []
        for device in available_devices:
            if device.device_id in device_ids:
                attached_devices.append({
                    'device_id': device.device_id,
                    'model_name': '',
                })
        return attached_devices
