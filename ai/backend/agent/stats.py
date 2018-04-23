'''
A module to collect various performance metrics of Docker containers.

Reference: https://www.datadoghq.com/blog/how-to-collect-docker-metrics/
'''

import asyncio
import argparse
from contextlib import closing, contextmanager
from ctypes import CDLL, get_errno
import functools
import logging
import os
from pathlib import Path
import sys
import time

import aiohttp
from aiodocker.docker import Docker, DockerContainer
from aiodocker.exceptions import DockerError
import zmq

from ai.backend.common import msgpack
from ai.backend.common.utils import nmget
from ai.backend.common.identity import is_containerized

log = logging.getLogger('ai.backend.agent.stats')

CLONE_NEWNET = 0x40000000


def _errcheck(ret, func, args):
    if ret == -1:
        e = get_errno()
        raise OSError(e, os.strerror(e))


def _collect_stats_sysfs(container_id):
    cpu_prefix = f'/sys/fs/cgroup/cpuacct/docker/{container_id}/'
    mem_prefix = f'/sys/fs/cgroup/memory/docker/{container_id}/'
    io_prefix = f'/sys/fs/cgroup/blkio/docker/{container_id}/'

    try:
        cpu_used = read_sysfs(cpu_prefix + 'cpuacct.usage') / 1e6
        mem_max_bytes = read_sysfs(mem_prefix + 'memory.max_usage_in_bytes')
        mem_cur_bytes = read_sysfs(mem_prefix + 'memory.usage_in_bytes')

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
            dev, op, bytes = line.strip().split()
            if op == 'Read':
                io_read_bytes += int(bytes)
            elif op == 'Write':
                io_write_bytes += int(bytes)
        io_max_scratch_size = 0
        io_cur_scratch_size = 0

        net_dev_stats = Path('/proc/net/dev').read_text()
        # example data:
        #   Inter-|   Receive                                                |  Transmit                                                  # noqa: E501
        #    face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed    # noqa: E501
        #     eth0:     1296     16    0    0    0     0          0         0      816      10    0    0    0     0       0          0    # noqa: E501
        #       lo:        0      0    0    0    0     0          0         0        0       0    0    0    0     0       0          0    # noqa: E501
        net_rx_bytes = 0
        net_tx_bytes = 0
        for line in net_dev_stats.splitlines():
            if '|' in line:
                continue
            data = line.strip().split()
            if data[0].startswith('eth'):
                net_rx_bytes += int(data[1])
                net_tx_bytes += int(data[9])
    except IOError as e:
        short_cid = container_id[:7]
        log.warning('cannot read stats: '
                    f'sysfs unreadable for container {short_cid}!'
                    f'\n{e!r}')
        return None

    return {
        'cpu_used': cpu_used,
        'mem_max_bytes': mem_max_bytes,
        'mem_cur_bytes': mem_cur_bytes,
        'net_rx_bytes': net_rx_bytes,
        'net_tx_bytes': net_tx_bytes,
        'io_read_bytes': io_read_bytes,
        'io_write_bytes': io_write_bytes,
        'io_max_scratch_size': io_max_scratch_size,
        'io_cur_scratch_size': io_cur_scratch_size,
    }


async def _collect_stats_api(container):
    try:
        ret = await container.stats(stream=False)
    except (DockerError, aiohttp.ClientResponseError):
        short_cid = container._id[:7]
        log.warning(f'cannot read stats: Docker stats API error for {short_cid}.')
        return None
    else:
        # API returned successfully but actually the result may be empty!
        if ret['preread'].startswith('0001-01-01'):
            return None
        cpu_used = nmget(ret, 'cpu_stats.cpu_usage.total_usage', 0) / 1e6
        mem_max_bytes = nmget(ret, 'memory_stats.max_usage', 0)
        mem_cur_bytes = nmget(ret, 'memory_stats.usage', 0)

        io_read_bytes = 0
        io_write_bytes = 0
        for item in nmget(ret, 'blkio_stats.io_service_bytes_recursive', []):
            if item['op'] == 'Read':
                io_read_bytes += item['value']
            elif item['op'] == 'Write':
                io_write_bytes += item['value']
        io_max_scratch_size = 0
        io_cur_scratch_size = 0

        net_rx_bytes = 0
        net_tx_bytes = 0
        for dev in nmget(ret, 'networks', {}).values():
            net_rx_bytes += dev['rx_bytes']
            net_tx_bytes += dev['tx_bytes']
    return {
        'cpu_used': cpu_used,
        'mem_max_bytes': mem_max_bytes,
        'mem_cur_bytes': mem_cur_bytes,
        'net_rx_bytes': net_rx_bytes,
        'net_tx_bytes': net_tx_bytes,
        'io_read_bytes': io_read_bytes,
        'io_write_bytes': io_write_bytes,
        'io_max_scratch_size': io_max_scratch_size,
        'io_cur_scratch_size': io_cur_scratch_size,
    }


async def collect_stats(containers):
    if sys.platform == 'linux' and not is_containerized():
        results = tuple(_collect_stats_sysfs(c._id) for c in containers)
    else:
        tasks = []
        for c in containers:
            tasks.append(asyncio.ensure_future(_collect_stats_api(c)))
        results = await asyncio.gather(*tasks)
    return results


def numeric_list(s):
    return [int(p) for p in s.split()]


def read_sysfs(path, type_=int, default_val=0):
    return type_(Path(path).read_text().strip())


@contextmanager
def join_cgroup_and_namespace(cid, send):
    libc = CDLL('libc.so.6', use_errno=True)
    libc.setns.errcheck = _errcheck
    mypid = str(os.getpid())

    # The list of monitored cgroup resource types
    cgroups = ['memory', 'cpuacct', 'blkio', 'net_cls']

    try:
        procs_path = Path(f'/sys/fs/cgroup/net_cls/docker/{cid}/cgroup.procs')
        pids = numeric_list(procs_path.read_text())
    except PermissionError:
        print('Cannot read cgroup filesystem due to permission error!',
              file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print('Cannot read cgroup filesystem.\n'
              'The container did not start or may have already terminated.',
              file=sys.stderr)
        send({
            'cid': args.cid,
            'status': 'terminated',
            'data': None,
        })
        sys.exit(0)

    try:
        # Change the cgroup membership of myself.
        for cgroup in cgroups:
            Path(f'/sys/fs/cgroup/{cgroup}/docker/{cid}/tasks').write_text(mypid)

        # Enter the container's network namespace so that we can see the exact "eth0"
        # (which is actually "vethXXXX" in the host namespace)
        with open(f'/proc/{pids[0]}/ns/net', 'r') as f:
            libc.setns(f.fileno(), CLONE_NEWNET)
    except PermissionError:
        print('This process must be started with the root privilege or have explicit'
              'CAP_SYS_ADMIN, CAP_DAC_OVERRIDE, and CAP_SYS_PTRACE capabilities.',
              file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print('The container has already terminated.', file=sys.stderr)
        send({
            'cid': args.cid,
            'status': 'terminated',
            'data': None,
        })
        sys.exit(0)

    try:
        yield
    finally:
        # Move to the parent cgroup and self-remove the container cgroup
        for cgroup in cgroups:
            Path(f'/sys/fs/cgroup/{cgroup}/tasks').write_text(mypid)
            try:
                os.rmdir(f'/sys/fs/cgroup/{cgroup}/docker/{cid}')
            except OSError:
                pass


def is_cgroup_running(cid):
    pids = Path(f'/sys/fs/cgroup/net_cls/docker/{cid}/tasks').read_text()
    pids = numeric_list(pids)
    return (len(pids) > 1)


def main(args):
    context = zmq.Context.instance()
    stats_sock = context.socket(zmq.PUSH)
    stats_sock.setsockopt(zmq.LINGER, 1000)
    stats_sock.connect(args.sockaddr)
    send = functools.partial(stats_sock.send_serialized,
                             serialize=lambda v: [msgpack.packb(v)])

    if args.type == 'cgroup':
        with closing(stats_sock), join_cgroup_and_namespace(args.cid, send):
            last_data = None
            data = None
            while True:
                data = _collect_stats_sysfs(args.cid)
                msg = {
                    'cid': args.cid,
                    'data': data,
                }
                if is_cgroup_running(args.cid) and data is not None:
                    msg['status'] = 'running'
                    send(msg)
                else:
                    msg['status'] = 'terminated'
                    send(msg)
                    break
                time.sleep(1.0)
    elif args.type == 'api':
        loop = asyncio.get_event_loop()
        with closing(stats_sock), closing(loop):
            last_data = None
            data = None
            docker = Docker()
            container = DockerContainer(docker, id=args.cid)
            while True:
                data = loop.run_until_complete(_collect_stats_api(container))
                msg = {
                    'cid': args.cid,
                    'data': data,
                }
                if data is not None:
                    msg['status'] = 'running'
                    send(msg)
                    last_data = data
                else:
                    msg['status'] = 'terminated'
                    msg['data'] = last_data
                    send(msg)
                    break
                time.sleep(1.0)
            loop.run_until_complete(docker.close())

    sys.exit(0)


if __name__ == '__main__':
    # The entry point for stat collector daemon
    parser = argparse.ArgumentParser()
    parser.add_argument('sockaddr', type=str)
    parser.add_argument('cid', type=str)
    parser.add_argument('-t', '--type', choices=['cgroup', 'api'],
                        default='cgroup')
    args = parser.parse_args()
    main(args)
