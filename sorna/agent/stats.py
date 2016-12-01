import logging
import sys
from pathlib import Path

from aiodocker.docker import DockerContainer

from sorna.utils import nmget

log = logging.getLogger('sorna.agent.stats')


async def collect_stats(container: DockerContainer) -> dict:
    if sys.platform == 'linux':
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
    else:
        ret = await container.stats(stream=False)
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


def read_sysfs(path, type_=int, default_val=0):
    try:
        return type_(Path(path).read_text().strip())
    except FileNotFoundError:
        return default_val
