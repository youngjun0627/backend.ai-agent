import asyncio
import codecs
import enum
import io
import logging
import time

from async_timeout import timeout
import aiozmq
from namedlist import namedlist
import simplejson as json
import zmq

from sorna.common.utils import StringSetFlag

log = logging.getLogger(__name__)

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


class KernelFeatures(StringSetFlag):
    UID_MATCH = 'uid-match'
    USER_INPUT = 'user-input'
    BATCH_MODE = 'batch'
    QUERY_MODE = 'query'
    TTY_MODE = 'tty'


class ClientFeatures(StringSetFlag):
    INPUT = 'input'
    CONTINUATION = 'continuation'


class ExecutionPhase(enum.Enum):
    BUILD = 1
    RUN = 2


class InputRequestPending(Exception):
    def __init__(self, opts):
        super().__init__()
        self.opts = opts


class BuildFinished(Exception):
    pass


class UserCodeFinished(Exception):
    def __init__(self, opts):
        super().__init__()
        self.opts = opts


class ExecTimeout(Exception):
    pass


ResultRecord = namedlist('ResultRecord', [
    ('msg_type', None),
    ('data', None),
])


class KernelRunner:

    def __init__(self, kernel_id, mode, opts,
                 kernel_ip, repl_in_port, repl_out_port,
                 exec_timeout, client_features=None):
        self.started_at = None
        self.finished_at = None
        self.kernel_id = kernel_id
        assert mode in ('query', 'batch', 'complete')
        self.mode = mode
        self.phase: ExecutionPhase
        self.accepts_input = False
        if mode == 'batch':
            self.phase = ExecutionPhase.BUILD
            assert 'build' in opts or 'exec' in opts
        else:
            self.phase = ExecutionPhase.RUN
        self.opts = opts
        self.kernel_ip = kernel_ip
        self.repl_in_port  = repl_in_port
        self.repl_out_port = repl_out_port
        self.input_stream = None
        self.output_stream = None
        assert exec_timeout > 0
        self.exec_timeout = exec_timeout
        self.max_record_size = 10485760  # 10 MBytes
        self.console_queue = asyncio.Queue(maxsize=1024)
        self.input_queue = asyncio.Queue()
        self.read_task = None
        self.client_features = client_features or set()

    async def start(self):
        self.started_at = time.monotonic()

        self.input_stream = await aiozmq.create_zmq_stream(
            zmq.PUSH, connect=f'tcp://{self.kernel_ip}:{self.repl_in_port}')
        self.input_stream.transport.setsockopt(zmq.LINGER, 50)

        self.output_stream = await aiozmq.create_zmq_stream(
            zmq.PULL, connect=f'tcp://{self.kernel_ip}:{self.repl_out_port}')
        self.output_stream.transport.setsockopt(zmq.LINGER, 50)

        if self.mode == 'query':
            self.accepts_input = True
        elif self.mode == 'batch':
            self.input_stream.write([
                b'build',
                self.opts.get('build', '').encode('utf8'),
            ])
        self.read_task = asyncio.ensure_future(self.read_output())
        has_continuation = ClientFeatures.CONTINUATION in self.client_features
        self.flush_timeout = 2.0 if has_continuation else None
        self.watchdog_task = asyncio.ensure_future(self.watchdog())

    async def close(self):
        if self.read_task and not self.read_task.done():
            self.read_task.cancel()
            await self.read_task
        if self.watchdog_task and not self.watchdog_task.done():
            self.watchdog_task.cancel()
            await self.watchdog_task
        if (self.input_stream and
            not self.input_stream.at_closing() and
            self.input_stream.transport):
            # only when really closable...
            self.input_stream.close()
        if (self.output_stream and
            not self.output_stream.at_closing() and
            # only when really closable...
            self.output_stream.transport):
            self.output_stream.close()

    async def feed_input(self, code_text):
        if self.accepts_input:
            log.warning(f'runnner.feed_input {code_text!r}')
            self.input_stream.write([
                b'input',
                code_text.encode('utf8'),
            ])
            self.accepts_input = False

    async def request_completions(self, code_text, opts):
        payload = {
            'code': code_text,
        }
        payload.update(opts)
        self.input_stream.write([
            b'complete',
            json.dumps(payload).encode('utf8'),
        ])

    async def watchdog(self):
        try:
            await asyncio.sleep(self.exec_timeout)
            self.console_queue.put(ResultRecord('exec-timeout', None))
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

        elif api_ver == 2:

            console_items = []
            completions = []
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

                if rec.msg_type == 'completion':
                    completions.extend(json.loads(rec.data))
                elif rec.msg_type == 'stdout':
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
            result['completions'] = completions
            last_stdout.close()
            last_stderr.close()

        else:
            raise AssertionError('Unrecognized API version')

    async def get_next_result(self, api_ver=2):
        try:
            records = []
            with timeout(self.flush_timeout):
                while True:
                    rec = await self.console_queue.get()
                    if rec.msg_type in outgoing_msg_types:
                        records.append(rec)
                    self.console_queue.task_done()
                    if rec.msg_type == 'finished':
                        o = json.loads(rec.data) if rec.data else {}
                        raise UserCodeFinished(o)
                    elif rec.msg_type == 'build-finished':
                        raise BuildFinished
                    elif rec.msg_type == 'waiting-input':
                        o = json.loads(rec.data) if rec.data else {}
                        raise InputRequestPending(o)
                    elif rec.msg_type == 'exec-timeout':
                        raise ExecTimeout
        except asyncio.TimeoutError:
            result = {
                'status': 'continued',
            }
            type(self).aggregate_console(result, records, api_ver)
            return result
        except BuildFinished as e:
            self.phase = ExecutionPhase.RUN
            result = {
                'status': 'build-finished',
            }
            type(self).aggregate_console(result, records, api_ver)
            return result
        except UserCodeFinished as e:
            self.accepts_input = True
            result = {
                'status': 'finished',
                'options': e.opts,
            }
            type(self).aggregate_console(result, records, api_ver)
            return result
        except ExecTimeout:
            self.accepts_input = True
            result = {
                'status': 'exec-timeout',
            }
            log.warning('Execution timeout detected on kernel '
                        f'{self.kernel_id}')
            type(self).aggregate_console(result, records, api_ver)
            return result
        except InputRequestPending as e:
            self.accepts_input = True
            result = {
                'status': 'waiting-input',
                'options': e.opts,
            }
            type(self).aggregate_console(result, records, api_ver)
            return result
        except asyncio.CancelledError:
            raise
        except:
            log.exception('unexpected error')
            raise

    async def read_output(self):
        # We should use incremental decoder because some kernels may
        # send us incomplete UTF-8 byte sequences (e.g., Julia).
        decoders = (
            codecs.getincrementaldecoder('utf8')(),
            codecs.getincrementaldecoder('utf8')(),
        )
        while True:
            try:
                msg_type, msg_data = await self.output_stream.read()
                # TODO: test if save-load runner states is working
                #       by printing received messages here
                if len(msg_data) > self.max_record_size:
                    msg_data = msg_data[:self.max_record_size]
                if not self.console_queue.full():
                    if msg_type == b'stdout':
                        await self.console_queue.put(
                            ResultRecord(
                                'stdout',
                                decoders[0].decode(msg_data),
                            ))
                    elif msg_type == b'stderr':
                        await self.console_queue.put(
                            ResultRecord(
                                'stderr',
                                decoders[1].decode(msg_data),
                            ))
                    else:
                        msg_data = msg_data.decode('utf8')
                        await self.console_queue.put(ResultRecord(
                            msg_type.decode('ascii'),
                            msg_data))
                if msg_type == b'build-finished':
                    # finalize incremental decoder
                    decoders[0].decode(b'', True)
                    decoders[1].decode(b'', True)
                    self.input_stream.write([
                        b'exec',
                        self.opts.get('exec', '').encode('utf8'),
                    ])
                elif msg_type == b'finished':
                    # finalize incremental decoder
                    decoders[0].decode(b'', True)
                    decoders[1].decode(b'', True)
                    self.finished_at = time.monotonic()
                    break
            except (asyncio.CancelledError, aiozmq.ZmqStreamClosed, GeneratorExit):
                break
            except:
                log.exception('unexpected error')
                break
