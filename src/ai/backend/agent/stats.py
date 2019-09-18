'''
A module to collect various performance metrics of Docker containers.

Reference: https://www.datadoghq.com/blog/how-to-collect-docker-metrics/
'''

import asyncio
import argparse
from contextlib import closing, contextmanager
from ctypes import CDLL, get_errno
from decimal import Decimal
import enum
import logging
import os
from pathlib import Path
import signal
import sys
import time
from typing import (
    Optional,
    Callable,
    List, Tuple,
    Dict, Mapping, MutableMapping,
    FrozenSet,
    TYPE_CHECKING,
)
from typing_extensions import Final

import aiodocker
import aioredis
import aiotools
import attr
from setproctitle import setproctitle
import zmq
import zmq.asyncio

from ai.backend.common import config
from ai.backend.common.logging import Logger, BraceStyleAdapter
from ai.backend.common.identity import is_containerized
from ai.backend.common.types import (
    ContainerId, DeviceId, KernelId,
    MetricKey, MetricValue, MovingStatValue,
)
from ai.backend.common import msgpack
from .utils import (
    numeric_list, remove_exponent,
)
if TYPE_CHECKING:
    from .agent import AbstractAgent

__all__ = (
    'StatContext', 'StatModes',
    'MetricTypes', 'NodeMeasurement', 'ContainerMeasurement', 'Measurement',
    'StatSyncState',
    'check_cgroup_available',
    'spawn_stat_synchronizer',
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.stats'))

CLONE_NEWNET: Final = 0x40000000


def _errcheck(ret, func, args):
    if ret == -1:
        e = get_errno()
        raise OSError(e, os.strerror(e))


def check_cgroup_available():
    '''
    Check if the host OS provides cgroups.
    '''
    return (not is_containerized() and sys.platform.startswith('linux'))


class StatModes(enum.Enum):
    CGROUP = 'cgroup'
    DOCKER = 'docker'

    @staticmethod
    def get_preferred_mode():
        '''
        Returns the most preferred statistics collector type for the host OS.
        '''
        if check_cgroup_available():
            return StatModes.CGROUP
        return StatModes.DOCKER


class MetricTypes(enum.Enum):
    USAGE = 0        # for instant snapshot (e.g., used memory bytes, used cpu msec)
    RATE = 1         # for rate of increase (e.g., I/O bps)
    UTILIZATION = 2  # for ratio of resource occupation time per measurement interval (e.g., CPU util)
    ACCUMULATED = 3  # for accumulated value (e.g., total number of events)


@attr.s(auto_attribs=True, slots=True)
class Measurement:
    value: Decimal
    capacity: Optional[Decimal] = None


@attr.s(auto_attribs=True, slots=True)
class NodeMeasurement:
    '''
    Collection of per-node and per-agent statistics for a specific metric.
    '''
    # 2-tuple of Decimals mean raw values for (usage, available)
    # Percent values are calculated from them.
    key: str
    type: MetricTypes
    per_node: Measurement
    per_device: Mapping[DeviceId, Measurement] = attr.Factory(dict)
    unit_hint: Optional[str] = None
    stats_filter: FrozenSet[str] = attr.Factory(frozenset)
    current_hook: Optional[Callable[['Metric'], Decimal]] = None


@attr.s(auto_attribs=True, slots=True)
class ContainerMeasurement:
    '''
    Collection of per-container statistics for a specific metric.
    '''
    key: str
    type: MetricTypes
    per_container: Mapping[str, Measurement] = attr.Factory(dict)
    unit_hint: Optional[str] = None
    stats_filter: FrozenSet[str] = attr.Factory(frozenset)
    current_hook: Optional[Callable[['Metric'], Decimal]] = None


class MovingStatistics:
    __slots__ = (
        '_sum', '_count',
        '_min', '_max', '_last',
    )
    _sum: Decimal
    _count: int
    _min: Decimal
    _max: Decimal
    _last: List[Tuple[Decimal, float]]

    def __init__(self, initial_value: Decimal = None):
        self._last = []
        if initial_value is None:
            self._sum = Decimal(0)
            self._min = Decimal('inf')
            self._max = Decimal('-inf')
            self._count = 0
        else:
            self._sum = initial_value
            self._min = initial_value
            self._max = initial_value
            self._count = 1
            point = (initial_value, time.perf_counter())
            self._last.append(point)

    def update(self, value: Decimal):
        self._sum += value
        self._min = min(self._min, value)
        self._max = max(self._max, value)
        self._count += 1
        point = (value, time.perf_counter())
        self._last.append(point)
        # keep only the latest two data points
        if len(self._last) > 2:
            self._last.pop(0)

    @property
    def min(self) -> Decimal:
        return self._min

    @property
    def max(self) -> Decimal:
        return self._max

    @property
    def sum(self) -> Decimal:
        return self._sum

    @property
    def avg(self) -> Decimal:
        return self._sum / self._count

    @property
    def diff(self) -> Decimal:
        if len(self._last) == 2:
            return self._last[-1][0] - self._last[-2][0]
        return Decimal(0)

    @property
    def rate(self) -> Decimal:
        if len(self._last) == 2:
            return ((self._last[-1][0] - self._last[-2][0]) /
                    Decimal(self._last[-1][1] - self._last[-2][1]))
        return Decimal(0)

    def to_serializable_dict(self) -> MovingStatValue:
        q = Decimal('0.000')
        return {
            'min': str(remove_exponent(self.min.quantize(q))),
            'max': str(remove_exponent(self.max.quantize(q))),
            'sum': str(remove_exponent(self.sum.quantize(q))),
            'avg': str(remove_exponent(self.avg.quantize(q))),
            'diff': str(remove_exponent(self.diff.quantize(q))),
            'rate': str(remove_exponent(self.rate.quantize(q))),
            'version': 2,
        }


@attr.s(auto_attribs=True, slots=True)
class Metric:
    key: str
    type: MetricTypes
    stats: MovingStatistics
    stats_filter: FrozenSet[str]
    current: Decimal
    capacity: Optional[Decimal] = None
    unit_hint: Optional[str] = None
    current_hook: Optional[Callable[['Metric'], Decimal]] = None

    def update(self, value: Measurement):
        if value.capacity is not None:
            self.capacity = value.capacity
        self.stats.update(value.value)
        self.current = value.value
        if self.current_hook is not None:
            self.current = self.current_hook(self)

    def to_serializable_dict(self) -> MetricValue:
        q = Decimal('0.000')
        q_pct = Decimal('0.00')
        return {
            'current': str(remove_exponent(self.current.quantize(q))),
            'capacity': (str(remove_exponent(self.capacity.quantize(q)))
                         if self.capacity is not None else None),
            'pct': (
                str(remove_exponent(
                    (Decimal(self.current) / Decimal(self.capacity) * 100).quantize(q_pct)))
                if (self.capacity is not None and
                    self.capacity.is_normal() and
                    self.capacity > 0)
                else None),
            'unit_hint': self.unit_hint,
            **{f'stats.{k}': v  # type: ignore
               for k, v in self.stats.to_serializable_dict().items()
               if k in self.stats_filter},
        }


@attr.s(auto_attribs=True, slots=True)
class StatSyncState:
    kernel_id: str
    terminated: asyncio.Event = attr.Factory(asyncio.Event)
    last_stat: Mapping[MetricKey, Metric] = attr.Factory(dict)


class StatContext:

    agent: 'AbstractAgent'
    mode: StatModes
    node_metrics: Mapping[MetricKey, Metric]
    device_metrics: Mapping[MetricKey, MutableMapping[DeviceId, Metric]]
    kernel_metrics: MutableMapping[KernelId, MutableMapping[MetricKey, Metric]]

    def __init__(self, agent: 'AbstractAgent', mode: StatModes = None, *,
                 cache_lifespan: float = 30.0):
        self.agent = agent
        self.mode = mode if mode is not None else StatModes.get_preferred_mode()
        self.cache_lifespan = cache_lifespan

        self.node_metrics = {}
        self.device_metrics = {}
        self.kernel_metrics = {}

        self._lock = asyncio.Lock()
        self._timestamps: MutableMapping[str, float] = {}

    def update_timestamp(self, timestamp_key: str) -> Tuple[float, float]:
        '''
        Update the timestamp for the given key and return a pair of the current timestamp and
        the interval from the last update of the same key.

        If the last timestamp for the given key does not exist, the interval becomes "NaN".

        Intended to be used by compute plugins.
        '''
        now = time.perf_counter()
        last = self._timestamps.get(timestamp_key, None)
        self._timestamps[timestamp_key] = now
        if last is None:
            return now, float('NaN')
        return now, now - last

    async def collect_node_stat(self):
        '''
        Collect the per-node, per-device, and per-container statistics.

        Intended to be used by the agent.
        '''
        async with self._lock:
            # gather node/device metrics from compute plugins
            _tasks = []
            for computer in self.agent.computers.values():
                _tasks.append(computer.klass.gather_node_measures(self))
            for node_measures in (await asyncio.gather(*_tasks, return_exceptions=True)):
                if isinstance(node_measures, Exception):
                    log.error('gather_node_measures error',
                              exc_info=node_measures)
                    continue
                for node_measure in node_measures:
                    metric_key = node_measure.key
                    # update node metric
                    if metric_key not in self.node_metrics:
                        self.node_metrics[metric_key] = Metric(
                            metric_key, node_measure.type,
                            current=node_measure.per_node.value,
                            capacity=node_measure.per_node.capacity,
                            unit_hint=node_measure.unit_hint,
                            stats=MovingStatistics(node_measure.per_node.value),
                            stats_filter=frozenset(node_measure.stats_filter),
                            current_hook=node_measure.current_hook,
                        )
                    else:
                        self.node_metrics[metric_key].update(node_measure.per_node)
                    # update per-device metric
                    # NOTE: device IDs are defined by each metric keys.
                    for dev_id, measure in node_measure.per_device.items():
                        dev_id = str(dev_id)
                        if metric_key not in self.device_metrics:
                            self.device_metrics[metric_key] = {}
                        if dev_id not in self.device_metrics[metric_key]:
                            self.device_metrics[metric_key][dev_id] = Metric(
                                metric_key, node_measure.type,
                                current=measure.value,
                                capacity=measure.capacity,
                                unit_hint=node_measure.unit_hint,
                                stats=MovingStatistics(measure.value),
                                stats_filter=frozenset(node_measure.stats_filter),
                                current_hook=node_measure.current_hook,
                            )
                        else:
                            self.device_metrics[metric_key][dev_id].update(measure)

            # gather container metrics from compute plugins
            container_ids = []
            kernel_id_map = {}
            used_kernel_ids = set()
            for kid, kobj in self.agent.kernel_registry.items():
                cid = kobj['container_id']
                container_ids.append(cid)
                kernel_id_map[cid] = kid
                used_kernel_ids.add(kid)
            unused_kernel_ids = set(self.kernel_metrics.keys()) - used_kernel_ids
            for unused_kernel_id in unused_kernel_ids:
                log.debug('removing kernel_metric for {}', unused_kernel_id)
                self.kernel_metrics.pop(unused_kernel_id, None)
            _tasks = []
            for computer in self.agent.computers.values():
                _tasks.append(computer.klass.gather_container_measures(self, container_ids))
            for ctnr_measures in (await asyncio.gather(*_tasks, return_exceptions=True)):
                if isinstance(ctnr_measures, Exception):
                    log.error('gather_container_measures error',
                              exc_info=ctnr_measures)
                    continue
                for ctnr_measure in ctnr_measures:
                    metric_key = ctnr_measure.key
                    # update per-container metric
                    for cid, measure in ctnr_measure.per_container.items():
                        kernel_id = kernel_id_map[cid]
                        if kernel_id not in self.kernel_metrics:
                            self.kernel_metrics[kernel_id] = {}
                        if metric_key not in self.kernel_metrics[kernel_id]:
                            self.kernel_metrics[kernel_id][metric_key] = Metric(
                                metric_key, ctnr_measure.type,
                                current=measure.value,
                                capacity=measure.value,
                                unit_hint=ctnr_measure.unit_hint,
                                stats=MovingStatistics(measure.value),
                                stats_filter=frozenset(ctnr_measure.stats_filter),
                                current_hook=ctnr_measure.current_hook,
                            )
                        else:
                            self.kernel_metrics[kernel_id][metric_key].update(measure)

        # push to the Redis server
        redis_agent_updates = {
            'node': {
                key: obj.to_serializable_dict()
                for key, obj in self.node_metrics.items()
            },
            'devices': {
                metric_key: {dev_id: obj.to_serializable_dict()
                             for dev_id, obj in per_device.items()}
                for metric_key, per_device in self.device_metrics.items()
            },
        }
        if self.agent.config['debug']['log-stats']:
            log.debug('stats: node_updates: {0}: {1}',
                      self.agent.config['agent']['id'], redis_agent_updates['node'])
        while True:
            try:
                pipe = self.agent.redis_stat_pool.pipeline()
                pipe.set(self.agent.config['agent']['id'], msgpack.packb(redis_agent_updates))
                pipe.expire(self.agent.config['agent']['id'], self.cache_lifespan)
                for kernel_id, metrics in self.kernel_metrics.items():
                    serialized_metrics = {
                        key: obj.to_serializable_dict()
                        for key, obj in metrics.items()
                    }
                    pipe.set(kernel_id, msgpack.packb(serialized_metrics))
                    pipe.expire(kernel_id, self.cache_lifespan)
                await pipe.execute()
            except (ConnectionRefusedError, aioredis.errors.ConnectionClosedError,
                    aioredis.errors.PipelineError):
                await asyncio.sleep(0.5)
                continue
            else:
                break

    async def collect_container_stat(self, container_id: ContainerId) -> Mapping[MetricKey, Metric]:
        '''
        Collect the per-container statistics only,

        Intended to be used by the agent and triggered by container cgroup synchronization processes.
        '''
        async with self._lock:
            kernel_id_map: Dict[ContainerId, KernelId] = {}
            for kid, info in self.agent.kernel_registry.items():
                cid = info['container_id']
                kernel_id_map[ContainerId(cid)] = kid

            _tasks = []
            kernel_id = None
            for computer in self.agent.computers.values():
                _tasks.append(
                    computer.klass.gather_container_measures(self, [container_id]))
            for ctnr_measures in (await asyncio.gather(*_tasks, return_exceptions=True)):
                if isinstance(ctnr_measures, Exception):
                    log.error('gather_cotnainer_measures error',
                              exc_info=ctnr_measures)
                    continue
                for ctnr_measure in ctnr_measures:
                    metric_key = ctnr_measure.key
                    # update per-container metric
                    for cid, measure in ctnr_measure.per_container.items():
                        assert cid == container_id
                        try:
                            kernel_id = kernel_id_map[cid]
                        except KeyError:
                            continue
                        if kernel_id not in self.kernel_metrics:
                            self.kernel_metrics[kernel_id] = {}
                        if metric_key not in self.kernel_metrics[kernel_id]:
                            self.kernel_metrics[kernel_id][metric_key] = Metric(
                                metric_key, ctnr_measure.type,
                                current=measure.value,
                                capacity=measure.value,
                                unit_hint=ctnr_measure.unit_hint,
                                stats=MovingStatistics(measure.value),
                                stats_filter=frozenset(ctnr_measure.stats_filter),
                                current_hook=ctnr_measure.current_hook,
                            )
                        else:
                            self.kernel_metrics[kernel_id][metric_key].update(measure)

        if kernel_id is not None:
            metrics = self.kernel_metrics[kernel_id]
            serialized_metrics = {
                key: obj.to_serializable_dict()
                for key, obj in metrics.items()
            }
            if self.agent.config['debug']['log-stats']:
                log.debug('kernel_updates: {0}: {1}',
                          kernel_id, serialized_metrics)
            while True:
                try:
                    pipe = self.agent.redis_stat_pool.pipeline()
                    pipe.set(kernel_id, msgpack.packb(serialized_metrics))
                    pipe.expire(kernel_id, self.cache_lifespan)
                    await pipe.execute()
                except (ConnectionRefusedError, aioredis.errors.ConnectionClosedError,
                        aioredis.errors.PipelineError):
                    await asyncio.sleep(0.5)
                    continue
                else:
                    break
            return metrics
        return {}


@aiotools.actxmgr
async def spawn_stat_synchronizer(config_path: Path, sync_sockpath: Path,
                                  stat_type: MetricTypes, cid: str,
                                  *, exec_opts=None):
    # Spawn high-perf stats collector process for Linux native setups.
    # NOTE: We don't have to keep track of this process,
    #       as they will self-terminate when the container terminates.
    if exec_opts is None:
        exec_opts = {}

    context = zmq.asyncio.Context()
    ipc_base_path = Path('/tmp/backend.ai/ipc')
    ipc_base_path.mkdir(parents=True, exist_ok=True)

    proc = await asyncio.create_subprocess_exec(*[
        sys.executable, '-m', 'ai.backend.agent.stats',
        str(config_path), str(sync_sockpath), cid, '--type', stat_type.value,
    ], **exec_opts)

    signal_sockpath = ipc_base_path / f'stat-start-{proc.pid}.sock'
    signal_sock = context.socket(zmq.PAIR)
    signal_sock.connect('ipc://' + str(signal_sockpath))
    try:
        await signal_sock.recv_multipart()
        yield proc
    finally:
        await signal_sock.send_multipart([b''])
        signal_sock.close()
        context.term()


@contextmanager
def join_cgroup_and_namespace(cid, ping_agent, signal_sock):
    stop_signals = {signal.SIGINT, signal.SIGTERM}
    signal.pthread_sigmask(signal.SIG_BLOCK, stop_signals)
    libc = CDLL('libc.so.6', use_errno=True)
    libc.setns.errcheck = _errcheck
    mypid = os.getpid()

    # The list of monitored cgroup resource types
    cgroups = ['memory', 'cpuacct', 'blkio', 'net_cls']

    try:
        # Create the cgroups and change my membership.
        # The reason for creating cgroups first is to keep them alive
        # after the Docker has killed the container.
        for cgroup in cgroups:
            cg_path = Path(f'/sys/fs/cgroup/{cgroup}/docker/{cid}')
            cg_path.mkdir(parents=True, exist_ok=True)
            cgtasks_path = Path(f'/sys/fs/cgroup/{cgroup}/docker/{cid}/cgroup.procs')
            cgtasks_path.write_text(str(mypid))
    except PermissionError:
        print('Cannot write cgroup filesystem due to permission error!',
              file=sys.stderr)
        sys.exit(1)

    # Notify the agent to start the container.
    signal_sock.send_multipart([b''])

    # Wait for the container to be started.
    signal_sock.recv_multipart()

    try:
        procs_path = Path(f'/sys/fs/cgroup/net_cls/docker/{cid}/cgroup.procs')
        pids = procs_path.read_text()
        pids = set(int(p) for p in pids.split())
    except PermissionError:
        print('Cannot read cgroup filesystem due to permission error!',
              file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print([p.name for p in Path('/sys/fs/cgroup/memory/docker').iterdir()])
        print('Cannot read cgroup filesystem.\n'
              'The container did not start or may have already terminated.',
              file=sys.stderr)
        ping_agent({
            'cid': args.cid,
            'status': 'terminated',
        })
        sys.exit(0)

    try:
        # Get non-myself pid from the current running processes.
        pids.discard(mypid)
        first_pid = pids.pop()
        # Enter the container's network namespace so that we can see the exact "eth0"
        # (which is actually "vethXXXX" in the host namespace)
        with open(f'/proc/{first_pid}/ns/net', 'r') as f:
            libc.setns(f.fileno(), CLONE_NEWNET)
    except PermissionError:
        print('This process must be started with the root privilege or have explicit'
              'CAP_SYS_ADMIN, CAP_DAC_OVERRIDE, and CAP_SYS_PTRACE capabilities.',
              file=sys.stderr)
        sys.exit(1)
    except (FileNotFoundError, KeyError):
        print('The container has already terminated.', file=sys.stderr)
        ping_agent({
            'cid': args.cid,
            'status': 'terminated',
        })
        sys.exit(0)

    try:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, stop_signals)
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_BLOCK, stop_signals)
        # Move to the parent cgroup and self-remove the container cgroup
        for cgroup in cgroups:
            Path(f'/sys/fs/cgroup/{cgroup}/cgroup.procs').write_text(str(mypid))
            try:
                os.rmdir(f'/sys/fs/cgroup/{cgroup}/docker/{cid}')
            except OSError:
                pass
        signal.pthread_sigmask(signal.SIG_UNBLOCK, stop_signals)


def is_cgroup_running(cid):
    try:
        pids = Path(f'/sys/fs/cgroup/net_cls/docker/{cid}/cgroup.procs').read_text()
        pids = numeric_list(pids)
    except IOError:
        return False
    else:
        return (len(pids) > 1)


async def is_container_running(cid):
    docker = aiodocker.Docker()
    try:
        container = await docker.containers.get(cid)
        stats = await container.show(stream=False)
        return stats['State']['Running']
    except (aiodocker.exceptions.DockerError, Exception):
        return False
    finally:
        await docker.close()


def main(args):
    '''
    The stat collector daemon.
    '''
    context = zmq.Context.instance()
    mypid = os.getpid()

    ipc_base_path = Path('/tmp/backend.ai/ipc')
    signal_sockpath = ipc_base_path / f'stat-start-{mypid}.sock'
    log.debug('creating signal socket at {}', signal_sockpath)
    signal_sock = context.socket(zmq.PAIR)
    signal_sock.bind('ipc://' + str(signal_sockpath))
    try:
        sync_sock = context.socket(zmq.REQ)
        sync_sock.setsockopt(zmq.LINGER, 2000)
        sync_sock.connect('ipc://' + args.sockpath)

        def ping_agent(msg):
            sync_sock.send_serialized([msg], lambda msgs: [*map(msgpack.packb, msgs)])
            sync_sock.recv_serialized(lambda frames: [*map(msgpack.unpackb, frames)])

        log.debug('started statistics collection for {}', args.cid)

        if args.type == StatModes.CGROUP:
            with closing(sync_sock), join_cgroup_and_namespace(args.cid,
                                                               ping_agent,
                                                               signal_sock):
                # Agent notification is done inside join_cgroup_and_namespace
                while True:
                    if is_cgroup_running(args.cid):
                        ping_agent({'status': 'collect-stat', 'cid': args.cid})
                    else:
                        # Since we control cgroup destruction, we can collect
                        # the last-moment statistics!
                        ping_agent({'status': 'collect-stat', 'cid': args.cid})
                        ping_agent({'status': 'terminated', 'cid': args.cid})
                        break
                    # Agent periodically collects container stats.
                    time.sleep(0.3)
        elif args.type == StatModes.DOCKER:
            stop_signals = {signal.SIGINT, signal.SIGTERM}
            signal.pthread_sigmask(signal.SIG_BLOCK, stop_signals)
            loop = asyncio.get_event_loop()
            with closing(sync_sock):
                # Notify the agent to start the container.
                signal_sock.send_multipart([b''])
                # Wait for the container to be actually started.
                signal_sock.recv_multipart()
                signal.pthread_sigmask(signal.SIG_UNBLOCK, stop_signals)
                while True:
                    is_running = loop.run_until_complete(is_container_running(args.cid))
                    if is_running:
                        ping_agent({'status': 'collect-stat', 'cid': args.cid})
                    else:
                        ping_agent({'status': 'terminated', 'cid': args.cid})
                        break
                    # Agent periodically collects container stats.
                    time.sleep(1.0)
            loop.close()

    except (KeyboardInterrupt, SystemExit):
        exit_code = 1
    else:
        exit_code = 0
    finally:
        signal_sock.close()
        signal_sockpath.unlink()
        log.debug('terminated statistics collection for {}', args.cid)
        context.term()

    time.sleep(0.05)
    sys.exit(exit_code)


if __name__ == '__main__':
    # The entry point for stat collector daemon
    parser = argparse.ArgumentParser()
    parser.add_argument('config_path', type=str)
    parser.add_argument('sockpath', type=str)
    parser.add_argument('cid', type=str)
    parser.add_argument('-t', '--type', choices=list(StatModes), type=StatModes,
                        default=StatModes.DOCKER)
    args = parser.parse_args()
    setproctitle(f'backend.ai: stat-collector {args.cid[:7]}')

    raw_cfg, _ = config.read_from_file(args.config_path, 'agent')
    raw_logging_cfg = raw_cfg.get('logging', None)
    if raw_logging_cfg and 'file' in raw_logging_cfg['drivers']:
        # To prevent corruption of file logs which requires only a single writer.
        raw_logging_cfg['drivers'].remove('file')
    fallback_logging_cfg = {
        'level': 'INFO',
        'drivers': ['console'],
        'pkg-ns': {'ai.backend': 'INFO'},
        'console': {'colored': True, 'format': 'verbose'},
    }
    logger = Logger(raw_logging_cfg or fallback_logging_cfg)
    with logger:
        main(args)
