import asyncio
import codecs
import enum
import logging
import time

from async_timeout import timeout
import aiozmq
from namedlist import namedlist
import simplejson as json
import zmq

from sorna.utils import StringSetFlag

log = logging.getLogger(__name__)


class ClientFeatures(StringSetFlag):
    INPUT = 'input'
    CONTINUATION = 'continuation'


class InputRequestPending(Exception):
    def __init__(self, is_password=False):
        super().__init__()
        self.is_password = is_password


class UserCodeFinished(Exception):
    pass


class ExecTimeout(Exception):
    pass


ResultRecord = namedlist('ResultRecord', [
    ('msg_type', None),
    ('data', None),
])


class KernelRunner:

    def __init__(self, kernel_id, kernel_ip, repl_in_port, repl_out_port,
                 exec_timeout, features=None):
        self.started_at = None
        self.finished_at = None
        self.kernel_id = kernel_id
        self.kernel_ip = kernel_ip
        self.repl_in_port  = repl_in_port
        self.repl_out_port = repl_out_port
        assert exec_timeout > 0
        self.exec_timeout = exec_timeout
        self.max_record_size = 10485760  # 10 MBytes
        self.console_queue = asyncio.Queue(maxsize=1024)
        self.input_queue = asyncio.Queue()
        self.read_task = None
        self.features = features or set()

    async def start(self, code_id, code_text):
        self.started_at = time.monotonic()

        self.input_stream = await aiozmq.create_zmq_stream(
            zmq.PUSH, connect=f'tcp://{self.kernel_ip}:{self.repl_in_port}')
        self.input_stream.transport.setsockopt(zmq.LINGER, 50)

        self.output_stream = await aiozmq.create_zmq_stream(
            zmq.PULL, connect=f'tcp://{self.kernel_ip}:{self.repl_out_port}')
        self.output_stream.transport.setsockopt(zmq.LINGER, 50)

        self.input_stream.write([
            b'input',
            code_text.encode('utf8'),
        ])
        self.read_task = asyncio.ensure_future(self.read_output())
        self.flush_timeout = 2.0 if ClientFeatures.CONTINUATION in self.features else None
        self.watchdog_task = asyncio.ensure_future(self.watchdog())

    async def close(self):
        if not self.read_task.done():
            self.read_task.cancel()
            await self.read_task
        if not self.watchdog_task.done():
            self.watchdog_task.cancel()
            await self.watchdog_task
        self.input_stream.close()
        self.output_stream.close()

    async def feed_input(self, code_id, code_text):
        self.input_stream.write([
            b'input',
            code_text.encode('utf8'),
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
            output_types = {'stdout', 'stderr', 'media', 'table', 'html'}
            result['console'] = [(rec.msg_type, rec.data) for rec in records
                                 if rec.msg_type in output_types]
        else:
            raise AssertionError('Unrecognized API version')

    async def get_next_result(self, api_ver=1):
        try:
            records = []
            with timeout(self.flush_timeout):
                while True:
                    rec = await self.console_queue.get()
                    records.append(rec)
                    self.console_queue.task_done()
                    if rec.msg_type == 'finished':
                        raise UserCodeFinished
                    elif rec.msg_type == 'waiting-input':
                        o = json.loads(rec.data)
                        raise InputRequestPending(o['is_password'])
                    elif rec.msg_type == 'exec-timeout':
                        raise ExecTimeout
        except asyncio.TimeoutError:
            result = {
                'status': 'continued',
            }
            type(self).aggregate_console(result, records, api_ver)
            return result
        except UserCodeFinished:
            result = {
                'status': 'finished',
            }
            type(self).aggregate_console(result, records, api_ver)
            return result
        except ExecTimeout:
            result = {
                'status': 'exec-timeout',
            }
            log.warning(f'Execution timeout detected on kernel {self.kernel_id}')
            type(self).aggregate_console(result, records, api_ver)
            return result
        except InputRequestPending as e:
            result = {
                'status': 'waiting-input',
                'options': {'is_password': e.is_password},
            }
            type(self).aggregate_console(result, records, api_ver)
            return result
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
                if len(msg_data) > self.max_record_size:
                    msg_data = msg_data[:self.max_record_size]
                if not self.console_queue.full():
                    if msg_type == b'stdout':
                        msg_data = decoder[0].decode(msg_data)
                    elif msg_type == b'stderr':
                        msg_data = decoder[1].decode(msg_data)
                    else:
                        msg_data = msg_data.decode('utf8')
                    await self.console_queue.put(ResultRecord(
                        msg_type.decode('ascii'),
                        msg_data))
                if msg_type == b'finished':
                    # finalize incremental decoder
                    decoder[0].decode(b'', True)
                    decoder[1].decode(b'', True)
                    break
            except (asyncio.CancelledError, aiozmq.ZmqStreamClosed):
                break
            except:
                log.exception('unexpected error')
                break
        self.finished_at = time.monotonic()
