from abc import ABCMeta, abstractmethod
import asyncio
from decimal import Decimal
import hashlib
import logging
import os
from pathlib import Path
import signal
import time
from typing import (
    Any, Collection, Optional, Type,
    Mapping, MutableMapping,
    MutableSequence,
    Set,
    Tuple,
)
from typing_extensions import Final

import attr
import aioredis
import aiotools
from async_timeout import timeout
import snappy
import zmq, zmq.asyncio

from ai.backend.common import msgpack, redis
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.monitor import DummyStatsMonitor, DummyErrorMonitor
from ai.backend.common.plugin import install_plugins
from ai.backend.common.types import (
    aobject,
    # TODO: eliminate use of ContainerId
    ContainerId, KernelId,
    DeviceName, SlotName,
    KernelCreationConfig,
    KernelCreationResult,
    MetricKey, MetricValue,
)
from . import __version__ as VERSION
from .kernel import AbstractKernel
from .utils import current_loop
from .resources import (
    AbstractComputeDevice,
    AbstractComputePlugin,
    AbstractAllocMap,
)
from .stats import (
    StatContext, StatModes,
    spawn_stat_synchronizer, StatSyncState
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.agent'))
ipc_base_path: Final = Path('/tmp/backend.ai/ipc')


@attr.s(auto_attribs=True, slots=True)
class VolumeInfo:
    name: str             # volume name
    container_path: str   # in-container path as str
    mode: str             # 'rw', 'ro', 'rwm'


@attr.s(auto_attribs=True, slots=True)
class RestartTracker:
    request_lock: asyncio.Lock
    destroy_event: asyncio.Event
    done_event: asyncio.Event


@attr.s(auto_attribs=True, slots=True)
class ComputerContext:
    klass: Type[AbstractComputePlugin]
    devices: Collection[AbstractComputeDevice]
    alloc_map: AbstractAllocMap


class AbstractAgent(aobject, metaclass=ABCMeta):

    loop: asyncio.AbstractEventLoop
    config: Mapping[str, Any]
    agent_id: str
    kernel_registry: MutableMapping[KernelId, AbstractKernel]
    computers: MutableMapping[str, ComputerContext]
    images: Mapping[str, str]
    port_pool: Set[int]

    redis: aioredis.Redis
    zmq_ctx: zmq.asyncio.Context

    restarting_kernels: MutableMapping[KernelId, RestartTracker]
    blocking_cleans: MutableMapping[KernelId, asyncio.Event]
    timer_tasks: MutableSequence[asyncio.Task]
    orphan_tasks: Set[asyncio.Task]

    stat_ctx: StatContext
    stat_sync_sockpath: Path
    stat_sync_states: MutableMapping[ContainerId, StatSyncState]
    stat_sync_task: asyncio.Task

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.loop = current_loop()
        self.config = config
        self.agent_id = hashlib.md5(__file__.encode('utf-8')).hexdigest()[:12]
        self.kernel_registry = {}
        self.computers = {}
        self.images = {}  # repoTag -> digest
        self.restarting_kernels = {}
        self.blocking_cleans = {}
        self.stat_ctx = StatContext(self, mode=StatModes(config['container']['stats-type']))
        self.timer_tasks = []
        self.orphan_tasks = set()
        self.port_pool = set(range(
            config['container']['port-range'][0],
            config['container']['port-range'][1] + 1,
        ))
        self.stats_monitor = DummyStatsMonitor()
        self.error_monitor = DummyErrorMonitor()
        plugins = [
            'stats_monitor',
            'error_monitor'
        ]
        install_plugins(plugins, self, 'attr', self.config)

    async def __ainit__(self) -> None:
        '''
        An implementation of AbstractAgent would define its own ``__ainit__()`` method.
        It must call this super method in an appropriate order, only once.
        '''
        self.producer_lock = asyncio.Lock()
        self.redis_producer_pool = await redis.connect_with_retries(
            self.config['redis']['addr'].as_sockaddr(),
            db=4,  # REDIS_STREAM_DB in gateway.defs
            password=(self.config['redis']['password']
                      if self.config['redis']['password'] else None),
            encoding=None,
        )
        self.redis_stat_pool = await redis.connect_with_retries(
            self.config['redis']['addr'].as_sockaddr(),
            db=0,  # REDIS_STAT_DB in backend.ai-manager
            password=(self.config['redis']['password']
                      if self.config['redis']['password'] else None),
            encoding='utf8',
        )

        ipc_base_path.mkdir(parents=True, exist_ok=True)
        self.zmq_ctx = zmq.asyncio.Context()

        computers, self.slots = await self.detect_resources(
            self.config['resource'],
            self.config['plugins'])
        for name, klass in computers.items():
            devices = await klass.list_devices()
            alloc_map = await klass.create_alloc_map()
            self.computers[name] = ComputerContext(klass, devices, alloc_map)

        await self.scan_images(None)
        self.timer_tasks.append(aiotools.create_timer(self.scan_images, 60.0))
        await self.scan_running_kernels()

        self.timer_tasks.append(aiotools.create_timer(self.collect_node_stat, 2.0))

        # Spawn stat collector task.
        self.stat_sync_sockpath = ipc_base_path / f'stats.{os.getpid()}.sock'
        self.stat_sync_states = dict()
        self.stat_sync_task = self.loop.create_task(self.sync_container_stats())

        # Start container stats collector for existing containers.
        for kernel_id, kernel_obj in self.kernel_registry.items():
            cid = kernel_obj['container_id']
            stat_sync_state = StatSyncState(kernel_id)
            self.stat_sync_states[cid] = stat_sync_state
            async with spawn_stat_synchronizer(self.config['_src'],
                                                self.stat_sync_sockpath,
                                                self.stat_ctx.mode, cid) as proc:
                stat_sync_state.sync_proc = proc

        # Prepare heartbeats.
        self.timer_tasks.append(aiotools.create_timer(self.heartbeat, 3.0))

        # Prepare auto-cleaning of idle kernels.
        self.timer_tasks.append(aiotools.create_timer(self.clean_old_kernels, 1.0))

        # Notify the gateway.
        await self.produce_event('instance_started', 'self-started')

    async def shutdown(self, stop_signal: signal.Signals) -> None:
        '''
        An implementation of AbstractAgent would define its own ``shutdown()`` method.
        It must call this super method in an appropriate order, only once.
        '''
        # Close all pending kernel runners.
        for kernel_obj in self.kernel_registry.values():
            if kernel_obj.runner is not None:
                await kernel_obj.runner.close()
            await kernel_obj.close()

        # Await for any orphan tasks such as cleaning/destroying kernels
        await asyncio.gather(*self.orphan_tasks, return_exceptions=True)

        if stop_signal == signal.SIGTERM:
            await self.clean_all_kernels(blocking=True)

        # Stop timers.
        for task in self.timer_tasks:
            task.cancel()
        timer_cancel_results = await asyncio.gather(*self.timer_tasks, return_exceptions=True)
        for result in timer_cancel_results:
            if isinstance(result, Exception):
                log.error('timer cancellation error: {}', result)

        # Stop stat collector task.
        if self.stat_sync_task is not None:
            self.stat_sync_task.cancel()
            await self.stat_sync_task
        self.redis_stat_pool.close()
        await self.redis_stat_pool.wait_closed()

        # Notify the gateway.
        await self.produce_event('instance_terminated', 'shutdown')
        self.redis_producer_pool.close()
        await self.redis_producer_pool.wait_closed()

    async def produce_event(self, event_name: str, *args) -> None:
        '''
        Send an event to the manager(s).
        '''
        _log = log.debug if event_name == 'instance_heartbeat' else log.info
        _log('produce_event({0})', event_name)
        encoded_event = msgpack.packb({
            'event_name': event_name,
            'agent_id': self.config['agent']['id'],
            'args': args,
        })
        async with self.producer_lock:
            def _pipe_builder():
                pipe = self.redis_producer_pool.pipeline()
                pipe.rpush('events.prodcons', encoded_event)
                pipe.publish('events.pubsub', encoded_event)
                return pipe
            await redis.execute_with_retries(_pipe_builder)

    async def heartbeat(self, interval: float):
        '''
        Send my status information and available kernel images to the manager(s).
        '''
        res_slots = {}
        for cctx in self.computers.values():
            for slot_key, slot_type in cctx.klass.slot_types:
                res_slots[slot_key] = (
                    slot_type,
                    str(self.slots.get(slot_key, 0)),
                )
        agent_info = {
            'ip': str(self.config['agent']['rpc-listen-addr'].host),
            'region': self.config['agent']['region'],
            'scaling_group': self.config['agent']['scaling-group'],
            'addr': f"tcp://{self.config['agent']['rpc-listen-addr']}",
            'resource_slots': res_slots,
            'version': VERSION,
            'compute_plugins': {
                key: {
                    'version': computer.klass.get_version(),
                    **(await computer.klass.extra_info())
                }
                for key, computer in self.computers.items()
            },
            'images': snappy.compress(msgpack.packb([
                (repo_tag, digest) for repo_tag, digest in self.images.items()
            ])),
        }
        try:
            await self.produce_event('instance_heartbeat', agent_info)
        except asyncio.TimeoutError:
            log.warning('event dispatch timeout: instance_heartbeat')
        except Exception:
            log.exception('instance_heartbeat failure')
            self.error_monitor.capture_exception()

    async def collect_node_stat(self, interval: float):
        try:
            await self.stat_ctx.collect_node_stat()
        except asyncio.CancelledError:
            pass

    async def sync_container_stats(self):
        stat_sync_sock = self.zmq_ctx.socket(zmq.REP)
        stat_sync_sock.setsockopt(zmq.LINGER, 1000)
        stat_sync_sock.bind('ipc://' + str(self.stat_sync_sockpath))
        try:
            log.info('opened stat-sync socket at {}', self.stat_sync_sockpath)
            recv = aiotools.apartial(stat_sync_sock.recv_serialized,
                                     lambda frames: [*map(msgpack.unpackb, frames)])
            send = aiotools.apartial(stat_sync_sock.send_serialized,
                                     serialize=lambda msgs: [*map(msgpack.packb, msgs)])
            while True:
                try:
                    async for msg in aiotools.aiter(lambda: recv(), None):
                        cid = ContainerId(msg[0]['cid'])
                        status = msg[0]['status']
                        await send([{'ack': True}])
                        if status == 'terminated':
                            self.stat_sync_states[cid].terminated.set()
                        elif status == 'collect-stat':
                            cstat = await asyncio.shield(self.stat_ctx.collect_container_stat(cid))
                            if cid not in self.stat_sync_states:
                                continue
                            s = self.stat_sync_states[cid]
                            for kernel_id, kernel_obj in self.kernel_registry.items():
                                if kernel_obj['container_id'] == cid:
                                    break
                            else:
                                continue
                            now = time.monotonic()
                            if now - s.last_sync > 10.0:
                                await self.produce_event('kernel_stat_sync', str(kernel_id))
                            s.last_stat = cstat
                            s.last_sync = now
                        else:
                            log.warning('unrecognized stat sync status: {}', status)
                except asyncio.CancelledError:
                    break
                except zmq.ZMQError:
                    log.exception('zmq socket error with {}', self.stat_sync_sockpath)
                except Exception:
                    log.exception('unexpected-error')
        finally:
            stat_sync_sock.close()
            try:
                self.stat_sync_sockpath.unlink()
            except IOError:
                pass

    async def clean_old_kernels(self, interval: float):
        now = time.monotonic()
        tasks = []
        for kernel_id, kernel_obj in self.kernel_registry.items():
            idle_timeout = kernel_obj.resource_spec.idle_timeout
            if idle_timeout is None:
                continue
            if idle_timeout > 0 and now - kernel_obj.last_used > idle_timeout:
                tasks.append(self.destroy_kernel(kernel_id, 'idle-timeout'))
        await asyncio.gather(*tasks, return_exceptions=True)

    async def clean_all_kernels(self, blocking: bool = False):
        log.info('cleaning all kernels...')
        kernel_ids = [*self.kernel_registry.keys()]
        tasks = []
        if blocking:
            for kernel_id in kernel_ids:
                self.blocking_cleans[kernel_id] = asyncio.Event()
        for kernel_id in kernel_ids:
            task = asyncio.ensure_future(
                self.destroy_kernel(kernel_id, 'agent-termination'))
            tasks.append(task)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for kernel_id, result in zip(kernel_ids, results):
            log.info('force-terminated kernel: {} (result: {})', kernel_id, result)
        if blocking:
            waiters = [self.blocking_cleans[kernel_id].wait()
                       for kernel_id in kernel_ids]
            await asyncio.gather(*waiters)
            self.blocking_cleans.clear()

    @staticmethod
    @abstractmethod
    async def detect_resources(resource_configs: Mapping[str, Any],
                               plugin_configs: Mapping[str, Any]) \
                               -> Tuple[
                                   Mapping[DeviceName, Type[AbstractComputePlugin]],
                                   Mapping[SlotName, Decimal]
                               ]:
        '''
        Scan and define the amount of available resource slots in this node.
        '''

    @abstractmethod
    async def scan_images(self, interval: float = None) -> None:
        '''
        Scan the available kernel images/templates and update ``self.images``.
        This is called periodically to keep the image list up-to-date and allow
        manual image addition and deletions by admins.
        '''

    @abstractmethod
    async def scan_running_kernels(self) -> None:
        '''
        Scan currently running kernels and recreate the kernel objects in
        ``self.kernel_registry`` if any missing.
        '''
        pass

    @abstractmethod
    async def create_kernel(self, kernel_id: KernelId, config: KernelCreationConfig, *,
                            restarting: bool = False) -> KernelCreationResult:
        '''
        Create a new kernel.

        Things to do:
        * ``await self.produce_event('kernel_preparing', kernel_id)``
        * Check availability of kernel image.
        * ``await self.produce_event('kernel_pulling', kernel_id)``
        * Pull the kernel image if image is not ready in the agent. (optional; may return error)
        * ``await self.produce_event('kernel_creating', kernel_id)``
        * Create KernelResourceSpec object or read it from kernel's internal configuration cache.
          Use *restarting* argument to detect if the kernel is being created or restarted.
        * Determine the matched base-distro and kernel-runner volume name for the image
          using match_krunner_volume()
        * Set up the kernel-runner volume, other volumes, and bind-mounts.
        * Create and start the kernel (e.g., container, process, etc.) with statistics synchronizer.
        * Create the kernel object (derived from AbstractKernel) and add it to ``self.kernel_registry``.
        * ``await self.produce_event('kernel_started', kernel_id)``
        * Execute the startup command if the sessino type is batch.
        * Return the kernel creation result.
        '''

        # TODO: clarify when to ``self.produce_event('kernel_terminated', ...)``

    @abstractmethod
    async def destroy_kernel(self, kernel_id: KernelId, reason: str) \
            -> Optional[Mapping[MetricKey, MetricValue]]:
        '''
        Initiate destruction of the kernel.

        Things to do:
        * If kernel_obj is not present, schedule ``self.clean_kernel(kernel_id)``
          and ``await self.produce_event('kernel_terminated', kernel_id, 'self-terminated')``
        * ``await kernel_obj.runner.close()``
        * Send a forced-kill signal to the kernel
        * Collect last-moment statistics
        * ``await self.stat_sync_states[cid].terminated.wait()``
        * Return the last-moment statistics if availble.
        '''

    @abstractmethod
    async def clean_kernel(self, kernel_id: KernelId):
        '''
        Clean up kernel-related book-keepers when the underlying
        implementation detects an event that the kernel has terminated.

        Things to do:
        * ``await kernel_obj.runner.close()``
        * Delete the underlying kernel resource (e.g., container)
        * ``self.restarting_kernels[kernel_id].destroy_event.set()``
        * Release host-specific resources used for the kernel (e.g., scratch spaces)
        * ``await kernel_obj.close()``; this releases resource slots from allocation maps.
        * Delete the kernel object from ``self.kernel_registry``
        * ``self.blocking_cleans[kernel_id].set()``

        This method is intended to be called asynchronously by the implementation-specific
        event monitoring routine.
        '''

    async def restart_kernel(self, kernel_id: KernelId, new_config: KernelCreationConfig):
        # TODO: check/correct resource release/allocation timings during restarts
        # TODO: handle restart failure when resource allocation for new config fails
        tracker = self.restarting_kernels.get(kernel_id)
        if tracker is None:
            tracker = RestartTracker(
                request_lock=asyncio.Lock(),
                destroy_event=asyncio.Event(),
                done_event=asyncio.Event())
        async with tracker.request_lock:
            if not tracker.done_event.is_set():
                self.restarting_kernels[kernel_id] = tracker
                await self.destroy_kernel(kernel_id, 'restarting')
                # clean_kernel() will set tracker.destroy_event
                try:
                    with timeout(30):
                        await tracker.destroy_event.wait()
                except asyncio.TimeoutError:
                    log.warning('timeout detected while restarting kernel {0}!',
                                kernel_id)
                    self.restarting_kernels.pop(kernel_id, None)
                    asyncio.ensure_future(self.clean_kernel(kernel_id))
                    raise
                else:
                    tracker.destroy_event.clear()
                    await self.create_kernel(
                        kernel_id, new_config,
                        restarting=True)
                    self.restarting_kernels.pop(kernel_id, None)
            tracker.done_event.set()
        kernel_obj = self.kernel_registry[kernel_id]
        return {
            # TODO: generalize
            'container_id': kernel_obj['container_id'],
            'repl_in_port': kernel_obj['repl_in_port'],
            'repl_out_port': kernel_obj['repl_out_port'],
            'stdin_port': kernel_obj['stdin_port'],
            'stdout_port': kernel_obj['stdout_port'],
            'service_ports': kernel_obj.service_ports,
        }

    async def execute(self, kernel_id: KernelId,
                      run_id: Optional[str], mode: str, text: str, *,
                      opts: Mapping[str, Any],
                      api_version: int,
                      flush_timeout: float):
        # Wait for the kernel restarting if it's ongoing...
        restart_tracker = self.restarting_kernels.get(kernel_id)
        if restart_tracker is not None:
            await restart_tracker.done_event.wait()

        try:
            kernel_obj = self.kernel_registry[kernel_id]
            result = await kernel_obj.execute(
                run_id, mode, text,
                opts=opts,
                flush_timeout=flush_timeout,
                api_version=api_version)
        except KeyError:
            await self.produce_event('kernel_terminated',
                                     str(kernel_id), 'self-terminated',
                                     None)
            raise RuntimeError(f'The container for kernel {kernel_id} is not found! '
                                '(might be terminated--try it again)') from None

        if result['status'] in ('finished', 'exec-timeout'):
            log.debug('_execute({0}) {1}', kernel_id, result['status'])
        if result['status'] == 'exec-timeout':
            self.orphan_tasks.add(
                self.loop.create_task(self.destroy_kernel(kernel_id, 'exec-timeout'))
            )
        return {
            **result,
            'files': [],  # kept for API backward-compatibility
        }

    async def get_completions(self, kernel_id: KernelId, text: str, opts: dict):
        return await self.kernel_registry[kernel_id].get_completions(text, opts)

    async def get_logs(self, kernel_id: KernelId):
        return await self.kernel_registry[kernel_id].get_logs()

    async def interrupt_kernel(self, kernel_id: KernelId):
        return await self.kernel_registry[kernel_id].interrupt_kernel()

    async def start_service(self, kernel_id: KernelId, service: str, opts: dict):
        return await self.kernel_registry[kernel_id].start_service(service, opts)

    async def accept_file(self, kernel_id: KernelId, filename: str, filedata):
        return await self.kernel_registry[kernel_id].accept_file(filename, filedata)

    async def download_file(self, kernel_id: KernelId, filepath: str):
        return await self.kernel_registry[kernel_id].download_file(filepath)

    async def list_files(self, kernel_id: KernelId, path: str):
        return await self.kernel_registry[kernel_id].list_files(path)
