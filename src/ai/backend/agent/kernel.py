from abc import abstractmethod, ABCMeta
import asyncio
import codecs
from collections import OrderedDict, UserDict
from dataclasses import dataclass
import io
import json
import logging
import re
import secrets
import time
from typing import Any, Mapping, Set, Tuple

from async_timeout import timeout
import zmq

from ai.backend.common import msgpack
from ai.backend.common.docker import ImageRef
from ai.backend.common.utils import StringSetFlag
from ai.backend.common.logging import BraceStyleAdapter
from .resources import KernelResourceSpec
from .utils import current_loop

log = BraceStyleAdapter(logging.getLogger(__name__))

# msg types visible to the API client.
# (excluding control signals such as 'finished' and 'waiting-input'
# since they are passed as separate status field.)
outgoing_msg_types = {'stdout', 'stderr', 'media', 'html', 'log', 'completion'}


class KernelFeatures(StringSetFlag):
    UID_MATCH = 'uid-match'
    USER_INPUT = 'user-input'
    BATCH_MODE = 'batch'
    QUERY_MODE = 'query'
    TTY_MODE = 'tty'


class ClientFeatures(StringSetFlag):
    INPUT = 'input'
    CONTINUATION = 'continuation'


# TODO: use Python 3.7 contextvars for per-client feature selection
default_client_features = {ClientFeatures.INPUT, ClientFeatures.CONTINUATION}
default_api_version = 3


class RunEvent(Exception):

    def __init__(self, data=None):
        super().__init__()
        self.data = data


class InputRequestPending(RunEvent):
    pass


class CleanFinished(RunEvent):
    pass


class BuildFinished(RunEvent):
    pass


class RunFinished(RunEvent):
    pass


class ExecTimeout(RunEvent):
    pass


@dataclass
class ResultRecord:
    msg_type: str = None
    data: str = None


class AbstractKernel(UserDict, metaclass=ABCMeta):

    def __init__(self, kernel_id: str, image: ImageRef, version: str, *,
                 resource_spec: KernelResourceSpec,
                 service_ports: Any,  # TODO: type-annotation
                 data: Mapping[str, Any]):
        self.kernel_id = kernel_id
        self.image = image
        self.version = version
        self.last_used = time.monotonic()
        self.resource_spec = resource_spec
        self.service_ports = service_ports
        self.runner = None
        self.data = data
        self._runner_lock = asyncio.Lock()
        self._tasks: Set[asyncio.Task] = set()

    @abstractmethod
    async def close(self):
        '''
        Release internal resources used for interacting with the kernel.
        Note that this does NOT terminate the container.
        '''
        pass

    # We don't have "allocate_slots()" method here because:
    # - resource_spec is initialized by allocating slots at computer's alloc_map
    #   when creating new kernels.
    # - restoration from running containers is done by computer's classmethod
    #   "restore_from_container"

    def release_slots(self, computer_ctxs):
        '''
        Release the resource slots occupied by the kernel
        to the allocation maps.
        '''
        for accel_key, accel_alloc in self.resource_spec.allocations.items():
            computer_ctxs[accel_key].alloc_map.free(accel_alloc)

    @abstractmethod
    def create_code_runner(self, *,
                           client_features: Set[str],
                           api_version: int) \
                           -> 'AbstractCodeRunner':
        raise NotImplementedError

    @abstractmethod
    async def check_status(self):
        raise NotImplementedError

    @abstractmethod
    async def get_completions(self, text, opts):
        raise NotImplementedError

    @abstractmethod
    async def get_logs(self):
        raise NotImplementedError

    @abstractmethod
    async def interrupt_kernel(self):
        raise NotImplementedError

    @abstractmethod
    async def start_service(self, service, opts):
        raise NotImplementedError

    @abstractmethod
    async def accept_file(self, filename, filedata):
        raise NotImplementedError

    @abstractmethod
    async def download_file(self, filepath):
        raise NotImplementedError

    @abstractmethod
    async def list_files(self, path: str):
        raise NotImplementedError

    async def execute(self, run_id, mode, text, *,
                      opts, flush_timeout, api_version):
        await self.ensure_runner()
        self.last_used = time.monotonic()
        try:
            myself = asyncio.Task.current_task()
            self._tasks.add(myself)
            await self.runner.attach_output_queue(run_id)
            if mode == 'batch':
                await self.runner.feed_batch(opts)
            elif mode == 'query':
                await self.runner.feed_code(text)
            elif mode == 'input':
                await self.runner.feed_input(text)
            elif mode == 'continue':
                pass
            return await self.runner.get_next_result(
                api_ver=api_version,
                flush_timeout=flush_timeout)
        except asyncio.CancelledError:
            await self.runner.close()
            raise
        finally:
            self._tasks.remove(myself)

    async def ensure_runner(self):
        async with self._runner_lock:
            if self.runner is not None:
                log.debug('_execute_code:v{0}({1}) use '
                          'existing runner', default_api_version, self.kernel_id)
            else:
                runner = self.create_code_runner(
                    client_features=default_client_features,
                    api_version=default_api_version)
                log.debug('_execute:v{0}({1}) start new runner',
                          default_api_version, self.kernel_id)
                self.runner = runner
                await runner.start()
            return self.runner


class AbstractCodeRunner:

    def __init__(self, repl_in_port, repl_out_port, *,
                 exec_timeout=0, client_features=None):
        self.started_at = None
        self.finished_at = None
        self.repl_in_port = repl_in_port
        self.repl_out_port = repl_out_port
        self.zctx = zmq.asyncio.Context()
        self.input_sock = None
        self.output_sock = None
        assert exec_timeout >= 0
        self.exec_timeout = exec_timeout
        self.max_record_size = 10485760  # 10 MBytes
        self.completion_queue = asyncio.Queue(maxsize=128)
        self.service_queue = asyncio.Queue(maxsize=128)
        self.status_queue = asyncio.Queue(maxsize=128)
        self.read_task = None
        self.client_features = client_features or set()

        self.output_queue = None
        self.pending_queues = OrderedDict()
        self.current_run_id = None

        loop = current_loop()
        self.status_task = loop.create_task(self.ping_status())

    async def start(self):
        pass

    async def close(self):
        if self.watchdog_task and not self.watchdog_task.done():
            self.watchdog_task.cancel()
            await self.watchdog_task
        if self.status_task and not self.status_task.done():
            self.status_task.cancel()
            await self.status_task
        if self.input_sock:
            self.input_sock.close()
        if self.output_sock:
            self.output_sock.close()
        if self.read_task and not self.read_task.done():
            self.read_task.cancel()
            await self.read_task
        self.zctx.term()

    async def ping_status(self):
        '''
        This is to keep the REPL in/out port mapping in the Linux
        kernel's NAT table alive.
        '''
        try:
            while True:
                await self.feed_and_get_status()
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass

    async def feed_batch(self, opts):
        clean_cmd = opts.get('clean', '')
        if clean_cmd is None:
            clean_cmd = ''
        await self.input_sock.send_multipart([
            b'clean',
            clean_cmd.encode('utf8'),
        ])
        build_cmd = opts.get('build', '')
        if build_cmd is None:
            build_cmd = ''
        await self.input_sock.send_multipart([
            b'build',
            build_cmd.encode('utf8'),
        ])
        exec_cmd = opts.get('exec', '')
        if exec_cmd is None:
            exec_cmd = ''
        await self.input_sock.send_multipart([
            b'exec',
            exec_cmd.encode('utf8'),
        ])

    async def feed_code(self, text: str):
        await self.input_sock.send_multipart([b'code', text.encode('utf8')])

    async def feed_input(self, text: str):
        await self.input_sock.send_multipart([b'input', text.encode('utf8')])

    async def feed_interrupt(self):
        await self.input_sock.send_multipart([b'interrupt', b''])

    async def feed_and_get_status(self):
        await self.input_sock.send_multipart([b'status', b''])
        try:
            result = await self.status_queue.get()
            self.status_queue.task_done()
            return msgpack.unpackb(result)
        except asyncio.CancelledError:
            return None

    async def feed_and_get_completion(self, code_text, opts):
        payload = {
            'code': code_text,
        }
        payload.update(opts)
        await self.input_sock.send_multipart([
            b'complete',
            json.dumps(payload).encode('utf8'),
        ])
        try:
            result = await self.completion_queue.get()
            self.completion_queue.task_done()
            return json.loads(result)
        except asyncio.CancelledError:
            return []

    async def feed_start_service(self, service_info):
        await self.input_sock.send_multipart([
            b'start-service',
            json.dumps(service_info).encode('utf8'),
        ])
        try:
            with timeout(10):
                result = await self.service_queue.get()
            self.service_queue.task_done()
            return json.loads(result)
        except asyncio.CancelledError:
            return {'status': 'failed', 'error': 'cancelled'}
        except asyncio.TimeoutError:
            return {'status': 'failed', 'error': 'timeout'}

    async def watchdog(self):
        try:
            await asyncio.sleep(self.exec_timeout)
            if self.output_queue is not None:
                # TODO: what to do if None?
                self.output_queue.put(ResultRecord('exec-timeout', None))
        except asyncio.CancelledError:
            pass

    @staticmethod
    def aggregate_console(result, records, api_ver):

        if api_ver == 1:

            stdout_items = []
            stderr_items = []
            media_items = []
            html_items = []

            for rec in records:
                if rec.msg_type == 'stdout':
                    stdout_items.append(rec.data)
                elif rec.msg_type == 'stderr':
                    stderr_items.append(rec.data)
                elif rec.msg_type == 'media':
                    o = json.loads(rec.data)
                    media_items.append((o['type'], o['data']))
                elif rec.msg_type == 'html':
                    html_items.append(rec.data)

            result['stdout'] = ''.join(stdout_items)
            result['stderr'] = ''.join(stderr_items)
            result['media'] = media_items
            result['html'] = html_items

        elif api_ver in (2, 3):

            console_items = []
            last_stdout = io.StringIO()
            last_stderr = io.StringIO()

            for rec in records:

                if last_stdout.tell() and rec.msg_type != 'stdout':
                    console_items.append(('stdout', last_stdout.getvalue()))
                    last_stdout.seek(0)
                    last_stdout.truncate(0)
                if last_stderr.tell() and rec.msg_type != 'stderr':
                    console_items.append(('stderr', last_stderr.getvalue()))
                    last_stderr.seek(0)
                    last_stderr.truncate(0)

                if rec.msg_type == 'stdout':
                    last_stdout.write(rec.data)
                elif rec.msg_type == 'stderr':
                    last_stderr.write(rec.data)
                elif rec.msg_type == 'media':
                    o = json.loads(rec.data)
                    console_items.append((rec.msg_type, (o['type'], o['data'])))
                elif rec.msg_type in outgoing_msg_types:
                    console_items.append((rec.msg_type, rec.data))

            if last_stdout.tell():
                console_items.append(('stdout', last_stdout.getvalue()))
            if last_stderr.tell():
                console_items.append(('stderr', last_stderr.getvalue()))

            result['console'] = console_items
            last_stdout.close()
            last_stderr.close()

        else:
            raise AssertionError('Unrecognized API version')

    async def get_next_result(self, api_ver=2, flush_timeout=2.0):
        # Context: per API request
        has_continuation = ClientFeatures.CONTINUATION in self.client_features
        try:
            records = []
            with timeout(flush_timeout if has_continuation else None):
                while True:
                    rec = await self.output_queue.get()
                    if rec.msg_type in outgoing_msg_types:
                        records.append(rec)
                    self.output_queue.task_done()
                    if rec.msg_type == 'finished':
                        data = json.loads(rec.data) if rec.data else {}
                        raise RunFinished(data)
                    elif rec.msg_type == 'clean-finished':
                        data = json.loads(rec.data) if rec.data else {}
                        raise CleanFinished(data)
                    elif rec.msg_type == 'build-finished':
                        data = json.loads(rec.data) if rec.data else {}
                        raise BuildFinished(data)
                    elif rec.msg_type == 'waiting-input':
                        opts = json.loads(rec.data) if rec.data else {}
                        raise InputRequestPending(opts)
                    elif rec.msg_type == 'exec-timeout':
                        raise ExecTimeout
        except asyncio.CancelledError:
            self.resume_output_queue()
            raise
        except asyncio.TimeoutError:
            result = {
                'runId': self.current_run_id,
                'status': 'continued',
                'exitCode': None,
                'options': None,
            }
            type(self).aggregate_console(result, records, api_ver)
            self.resume_output_queue()
            return result
        except CleanFinished as e:
            result = {
                'runId': self.current_run_id,
                'status': 'clean-finished',
                'exitCode': e.data.get('exitCode'),
                'options': None,
            }
            type(self).aggregate_console(result, records, api_ver)
            self.resume_output_queue()
            return result
        except BuildFinished as e:
            result = {
                'runId': self.current_run_id,
                'status': 'build-finished',
                'exitCode': e.data.get('exitCode'),
                'options': None,
            }
            type(self).aggregate_console(result, records, api_ver)
            self.resume_output_queue()
            return result
        except RunFinished as e:
            result = {
                'runId': self.current_run_id,
                'status': 'finished',
                'exitCode': e.data.get('exitCode'),
                'options': None,
            }
            type(self).aggregate_console(result, records, api_ver)
            self.next_output_queue()
            return result
        except ExecTimeout:
            result = {
                'runId': self.current_run_id,
                'status': 'exec-timeout',
                'exitCode': None,
                'options': None,
            }
            log.warning('Execution timeout detected on kernel '
                        f'{self.kernel_id}')
            type(self).aggregate_console(result, records, api_ver)
            self.next_output_queue()
            return result
        except InputRequestPending as e:
            result = {
                'runId': self.current_run_id,
                'status': 'waiting-input',
                'exitCode': None,
                'options': e.data,
            }
            type(self).aggregate_console(result, records, api_ver)
            self.resume_output_queue()
            return result
        except Exception:
            log.exception('unexpected error')
            raise

    async def attach_output_queue(self, run_id):
        # Context: per API request
        if run_id is None:
            run_id = secrets.token_hex(16)
        assert run_id is not None
        if run_id not in self.pending_queues:
            q = asyncio.Queue(maxsize=4096)
            activated = asyncio.Event()
            self.pending_queues[run_id] = (activated, q)
        else:
            activated, q = self.pending_queues[run_id]
        if self.output_queue is None:
            self.output_queue = q
        else:
            if self.current_run_id == run_id:
                # No need to wait if we are continuing.
                pass
            else:
                # If there is an outstanding ongoning execution,
                # wait until it has "finished".
                await activated.wait()
                activated.clear()
        self.current_run_id = run_id
        assert self.output_queue is q

    def resume_output_queue(self):
        '''
        Use this to conclude get_next_result() when the execution should be
        continued from the client.

        At that time, we need to reuse the current run ID and its output queue.
        We don't change self.output_queue here so that we can continue to read
        outputs while the client sends the continuation request.
        '''
        self.pending_queues.move_to_end(self.current_run_id, last=False)

    def next_output_queue(self):
        '''
        Use this to conclude get_next_result() when we have finished a "run".
        '''
        assert self.current_run_id is not None
        self.pending_queues.pop(self.current_run_id, None)
        self.current_run_id = None
        if len(self.pending_queues) > 0:
            # Make the next waiting API request handler to proceed.
            _, (activated, q) = self.pending_queues.popitem(last=False)
            self.output_queue = q
            activated.set()
        else:
            # If there is no pending request, just ignore all outputs
            # from the kernel.
            self.output_queue = None

    async def read_output(self):
        # We should use incremental decoder because some kernels may
        # send us incomplete UTF-8 byte sequences (e.g., Julia).
        decoders = (
            codecs.getincrementaldecoder('utf8')(errors='replace'),
            codecs.getincrementaldecoder('utf8')(errors='replace'),
        )
        while True:
            try:
                msg_type, msg_data = await self.output_sock.recv_multipart()
                try:
                    if msg_type == b'status':
                        await self.status_queue.put(msg_data)
                    elif msg_type == b'completion':
                        await self.completion_queue.put(msg_data)
                    elif msg_type == b'service-result':
                        await self.service_queue.put(msg_data)
                    elif msg_type == b'stdout':
                        if self.output_queue is None:
                            continue
                        if len(msg_data) > self.max_record_size:
                            msg_data = msg_data[:self.max_record_size]
                        await self.output_queue.put(
                            ResultRecord(
                                'stdout',
                                decoders[0].decode(msg_data),
                            ))
                    elif msg_type == b'stderr':
                        if self.output_queue is None:
                            continue
                        if len(msg_data) > self.max_record_size:
                            msg_data = msg_data[:self.max_record_size]
                        await self.output_queue.put(
                            ResultRecord(
                                'stderr',
                                decoders[1].decode(msg_data),
                            ))
                    else:
                        # Normal outputs should go to the current
                        # output queue.
                        if self.output_queue is None:
                            continue
                        await self.output_queue.put(
                            ResultRecord(
                                msg_type.decode('ascii'),
                                msg_data.decode('utf8'),
                            ))
                except asyncio.QueueFull:
                    pass
                if msg_type == b'build-finished':
                    # finalize incremental decoder
                    decoders[0].decode(b'', True)
                    decoders[1].decode(b'', True)
                elif msg_type == b'finished':
                    # finalize incremental decoder
                    decoders[0].decode(b'', True)
                    decoders[1].decode(b'', True)
                    self.finished_at = time.monotonic()
            except (asyncio.CancelledError, GeneratorExit):
                break
            except Exception:
                log.exception('unexpected error')
                break


def match_krunner_volume(krunner_volumes: Mapping[str, Any], distro: str) -> Tuple[str, Any]:
    '''
    Find the latest or exactly matching entry from krunner_volumes mapping using the given distro
    string expression.

    It assumes that the keys of krunner_volumes mapping is a string concatenated with a distro
    prefix (e.g., "centos", "ubuntu") and a distro version composed of multiple integer components
    joined by single dots (e.g., "1.2.3", "18.04").
    '''
    rx_ver_suffix = re.compile(r'(\d+(\.\d+)*)$')
    m = rx_ver_suffix.search(distro)
    if m is None:
        # Assume latest
        distro_prefix = distro
        distro_ver = None
    else:
        distro_prefix = distro[:-len(m.group(1))]
        distro_ver = m.group(1)
    krunner_volumes = [
        (distro_key, volume)
        for distro_key, volume in krunner_volumes.items()
        if distro_key.startswith(distro_prefix)
    ]
    krunner_volumes = sorted(
        krunner_volumes,
        key=lambda item: tuple(map(int, rx_ver_suffix.search(item[0]).group(1).split('.'))),
        reverse=True)
    if krunner_volumes:
        if distro_ver is None:
            return krunner_volumes[0]
        for distro_key, volume in krunner_volumes:
            if distro_key == distro:
                return (distro_key, volume)
    raise RuntimeError('krunner volume not found', distro)
