#! /usr/bin/env python3

'''
The Sorna Kernel Agent

It manages the namespace and hooks for the Python code requested and execute it.
'''

from sorna.proto import Message, odict
from sorna.proto.msgtypes import ManagerRequestTypes, ManagerResponseTypes
import asyncio, zmq, aiozmq
import builtins as builtin_mod
import code
import io
from namedlist import namedtuple
import types
import sys

ExceptionInfo = namedtuple('ExceptionInfo', [
    'exc',
    ('raised_before_exec', False),
    ('traceback', None),
])


class SockWriter(object):
    def __init__(self, sock, cell_id):
        self.cell_id_encoded = '{0}'.format(cell_id).encode('ascii')
        self.sock = sock
        self.buffer = io.StringIO()

    def write(self, s):
        if '\n' in s:  # flush on occurrence of a newline.
            s1, s2 = s.split('\n')
            s0 = self.buffer.getvalue()
            self.sock.send_multipart([self.cell_id_encoded, (s0 + s1 + '\n').encode('utf8')])
            self.buffer.seek(0)
            self.buffer.truncate(0)
            self.buffer.write(s2)
        else:
            self.buffer.write(s)
        if self.buffer.tell() > 1024:  # flush if the buffer is too large.
            s0 = self.buffer.getvalue()
            self.sock.send_multipart([self.cell_id_encoded, s0.encode('utf8')])
            self.buffer.seek(0)
            self.buffer.truncate(0)
        # TODO: timeout to flush?


class BufferWriter(object):
    def __init__(self):
        self.buffer = io.StringIO()

    def write(self, s):
        self.buffer.write(s)


class SockReader(object):
    def __init__(self, sock, cell_id):
        self.sock = sock

    def read(self, n):
        raise NotImplementedError()


class Kernel(object):
    '''
    The Kernel object.

    It creates a dummy module that user codes run and keeps the references to user-created objects
    (e.g., variables and functions).
    '''

    def __init__(self, ip, kernel_id, manager_addr):
        self.ip = ip
        self.kernel_id = kernel_id
        self.manager_addr = manager_addr

        # Initialize sockets.
        context = zmq.Context.instance()
        self.stdin_socket = None  #context.socket(zmq.ROUTER)
        self.stdin_port   = 0     #self.stdin_socket.bind_to_random_port('tcp://{0}'.format(self.ip))
        self.stdout_socket = context.socket(zmq.PUB)
        self.stdout_port   = self.stdout_socket.bind_to_random_port('tcp://{0}'.format(self.ip))
        self.stderr_socket = context.socket(zmq.PUB)
        self.stderr_port   = self.stderr_socket.bind_to_random_port('tcp://{0}'.format(self.ip))

        # Initialize user module and namespaces.
        user_module = types.ModuleType('__main__',
                                       doc='Automatically created module for the interactive shell.')
        user_module.__dict__.setdefault('__builtin__', builtin_mod)
        user_module.__dict__.setdefault('__builtins__', builtin_mod)
        self.user_module = user_module
        self.user_ns = user_module.__dict__

    @asyncio.coroutine
    def send_refresh(self):
        if self._testing: return
        stream = yield from aiozmq.create_zmq_stream(zmq.REQ, connect=self.manager_addr)
        req = Message(
            ('action', ManagerRequestTypes.REFRESH),
            ('kernel_id', self.kernel_id),
        )
        stream.write([req.encode()])
        resp_data = yield from stream.read()
        resp = Message.decode(resp_data[0])
        assert resp['reply'] == ManagerResponseTypes.SUCCESS
        stream.close()

    def execute_code(self, cell_id, src, redirect_output=False):

        # TODO: limit the scope of changed sys.std*
        #       (use a proxy object for sys module?)
        #self.stdin_reader = SockReader(self.stdin_socket)
        if redirect_output:
            self.stdout_writer = BufferWriter()
            self.stderr_writer = BufferWriter()
        else:
            self.stdout_writer = SockWriter(self.stdout_socket, cell_id)
            self.stderr_writer = SockWriter(self.stderr_socket, cell_id)
        #sys.stdin, orig_stdin   = self.stdin_reader, sys.stdin
        sys.stdout, orig_stdout = self.stdout_writer, sys.stdout
        sys.stderr, orig_stderr = self.stderr_writer, sys.stderr

        exec_result = None
        exceptions = []
        before_exec = True

        def my_excepthook(type_, value, tb):
            exceptions.append(ExceptionInfo(value, before_exec, tb))
        sys.excepthook = my_excepthook

        try:
            # TODO: cache the compiled code in the memory
            # TODO: attach traceback in a structured format
            code_obj = code.compile_command(src, symbol='exec')
        except IndentationError as e:
            exceptions.append(ExceptionInfo(e, before_exec, None))
        except (OverflowError, SyntaxError, ValueError, TypeError, MemoryError) as e:
            exceptions.append(ExceptionInfo(e, before_exec, None))
        else:
            before_exec = False
            try:
                # TODO: distinguish whethe we should do exec or eval...
                exec_result = exec(code_obj, self.user_ns)
            except Exception as e:
                exceptions.append(ExceptionInfo(e, before_exec, None))

        sys.excepthook = sys.__excepthook__

        if redirect_output:
            output = (self.stdout_writer.buffer.getvalue(), self.stderr_writer.buffer.getvalue())
            self.stdout_writer.buffer.close()
            self.stderr_writer.buffer.close()
        else:
            output = (None, None)
        #sys.stdin = orig_stdin
        sys.stdout = orig_stdout
        sys.stderr = orig_stderr

        return exec_result, exceptions, output

