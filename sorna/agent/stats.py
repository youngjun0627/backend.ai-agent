import asyncio
import logging
import sys
from pathlib import Path

import aiohttp
from aiodocker.exceptions import DockerError

from sorna.common.utils import nmget

log = logging.getLogger('sorna.agent.stats')


def _collect_stats_sysfs(container):
    try:
        path = '/sys/fs/cgroup/cpuacct/docker/{}/cpuacct.usage' \
               .format(container._id)
        cpu_used = read_sysfs(path) / 1e6
        path = '/sys/fs/cgroup/memory/docker/{}/memory.max_usage_in_bytes' \
               .format(container._id)
        mem_max_bytes = read_sysfs(path)
        # TODO: implement
        io_read_bytes = 0
        io_write_bytes = 0
        net_rx_bytes = 0
        net_tx_bytes = 0
    except FileNotFoundError:
        return None
    return {
        'cpu_used': cpu_used,
        'mem_max_bytes': mem_max_bytes,
        'net_rx_bytes': net_rx_bytes,
        'net_tx_bytes': net_tx_bytes,
        'io_read_bytes': io_read_bytes,
        'io_write_bytes': io_write_bytes,
    }


async def _collect_stats_api(container):
    try:
        ret = await container.stats(stream=False)
    except (DockerError, aiohttp.ClientResponseError):
        log.warning('container {} missing on heartbeat'.format(container._id[:7]))
        return None
    else:
        cpu_used = nmget(ret, 'cpu_stats.cpu_usage.total_usage', 0) / 1e6
        mem_max_bytes = nmget(ret, 'memory_stats.max_usage', 0)
        io_read_bytes = 0
        io_write_bytes = 0
        for item in nmget(ret, 'blkio_stats.io_service_bytes_recursive', []):
            if item['op'] == 'Read':
                io_read_bytes += item['value']
            elif item['op'] == 'Write':
                io_write_bytes += item['value']
        net_rx_bytes = 0
        net_tx_bytes = 0
        for dev in nmget(ret, 'networks', {}).values():
            net_rx_bytes += dev['rx_bytes']
            net_tx_bytes += dev['tx_bytes']
    return {
        'cpu_used': cpu_used,
        'mem_max_bytes': mem_max_bytes,
        'net_rx_bytes': net_rx_bytes,
        'net_tx_bytes': net_tx_bytes,
        'io_read_bytes': io_read_bytes,
        'io_write_bytes': io_write_bytes,
    }


async def collect_stats(containers):
    if sys.platform == 'linux':
        results = tuple(_collect_stats_sysfs(c) for c in containers)
    else:
        tasks = []
        for c in containers:
            tasks.append(asyncio.ensure_future(_collect_stats_api(c)))
        results = await asyncio.gather(*tasks)
    return results


def read_sysfs(path, type_=int, default_val=0):
    return type_(Path(path).read_text().strip())
