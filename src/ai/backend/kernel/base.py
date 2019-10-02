from abc import ABCMeta, abstractmethod
import asyncio
import concurrent.futures
from functools import partial
import json
import logging
import os
from pathlib import Path
import signal
import sys
import time
from typing import ClassVar, MutableMapping, Optional

import janus
from jupyter_client import KernelManager
from jupyter_client.kernelspec import KernelSpecManager
import msgpack
import zmq

from .jupyter_client import aexecute_interactive
from .logging import BraceStyleAdapter, setup_logger
from .compat import asyncio_run_forever, current_loop
from .utils import wait_local_port_open
from .intrinsic import (
    init_sshd_service, prepare_sshd_service,
    prepare_ttyd_service,
)

log = BraceStyleAdapter(logging.getLogger())


async def pipe_output(stream, outsock, target, log_fd):
    assert target in ('stdout', 'stderr')
    target = target.encode('ascii')
    console_fd = sys.stdout.fileno() if target == 'stdout' else sys.stderr.fileno()
    loop = current_loop()
    try:
        while True:
            data = await stream.read(4096)
            if not data:
                break
            await asyncio.gather(
                loop.run_in_executor(None, os.write, console_fd, data),
                loop.run_in_executor(None, os.write, log_fd, data),
                outsock.send_multipart([target, data]),
                return_exceptions=True,
            )
    except asyncio.CancelledError:
        pass
    except Exception:
        log.exception('unexpected error')


async def terminate_and_kill(proc):
    proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=2.0)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()


class BaseRunner(metaclass=ABCMeta):

    log_prefix: ClassVar[str] = 'generic-kernel'
    default_runtime_path: ClassVar[Optional[str]] = None
    default_child_env: ClassVar[MutableMapping[str, str]] = {
        'LANG': 'C.UTF-8',
        'SHELL': '/bin/sh',
        'HOME': '/home/work',
        'LD_LIBRARY_PATH': os.environ.get('LD_LIBRARY_PATH', ''),
        'LD_PRELOAD': os.environ.get('LD_PRELOAD', ''),
    }
    jupyter_kspec_name: ClassVar[str] = ''
    kernel_mgr = None
    kernel_client = None

    child_env: MutableMapping[str, str]
    subproc: Optional[asyncio.subprocess.Process]
    runtime_path: Optional[Path]

    def __init__(self, loop=None):
        self.subproc = None
        self.runtime_path = None

        self.child_env = {**os.environ, **self.default_child_env}
        config_dir = Path('/home/config')
        try:
            evdata = (config_dir / 'environ.txt').read_text()
            for line in evdata.splitlines():
                k, v = line.split('=', 1)
                self.child_env[k] = v
                os.environ[k] = v
        except FileNotFoundError:
            pass
        except Exception:
            log.exception('Reading /home/config/environ.txt failed!')

        # initialized after loop creation
        self.loop = loop if loop is not None else current_loop()
        self.zctx = zmq.asyncio.Context()
        self.started_at: float = time.monotonic()
        self.insock = None
        self.outsock = None
        self.init_done = None
        self.task_queue = None
        self.log_queue = None

        self.services_running = set()
        self.service_processes = []

        # If the subclass implements interatcive user inputs, it should set a
        # asyncio.Queue-like object to self.user_input_queue in the
        # init_with_loop() method.
        self.user_input_queue = None

        # build status tracker to skip the execute step
        self._build_success = None

    async def _init_with_loop(self):
        if self.init_done is not None:
            self.init_done.clear()
        try:
            await self.init_with_loop()
        except Exception:
            log.exception('unexpected error')
            return
        await init_sshd_service()
        if self.init_done is not None:
            self.init_done.set()

    @abstractmethod
    async def init_with_loop(self):
        """Initialize after the event loop is created."""

    async def _init_jupyter_kernel(self):
        """Detect ipython kernel spec for backend.ai and start it if found.

        Called after `init_with_loop`. `jupyter_kspec_name` should be defined to
        initialize jupyter kernel.
        """
        # Make inline backend defaults in Matplotlib.
        kconfigdir = Path('/home/work/.ipython/profile_default/')
        kconfigdir.mkdir(parents=True, exist_ok=True)
        kconfig_file = kconfigdir / 'ipython_kernel_config.py'
        kconfig_file.write_text("c.InteractiveShellApp.matplotlib = 'inline'")

        kernelspec_mgr = KernelSpecManager()
        kernelspec_mgr.ensure_native_kernel = False
        kspecs = kernelspec_mgr.get_all_specs()
        for kname in kspecs:
            if self.jupyter_kspec_name in kname:
                log.debug('starting ' + kname + ' kernel...')
                self.kernel_mgr = KernelManager(kernel_name=kname)
                self.kernel_mgr.start_kernel()
                if not self.kernel_mgr.is_alive():
                    log.error('jupyter query mode is disabled: '
                              'failed to start jupyter kernel')
                else:
                    self.kernel_client = self.kernel_mgr.client()
                    self.kernel_client.start_channels(shell=True, iopub=True,
                                                      stdin=True, hb=True)
                    try:
                        self.kernel_client.wait_for_ready(timeout=10)
                        # self.init_jupyter_kernel()
                    except RuntimeError:
                        # Clean up for client and kernel will be done in `shutdown`.
                        log.error('jupyter channel is not active!')
                        self.kernel_mgr = None
                break
        else:
            log.debug('jupyter query mode is not available: '
                      'no jupyter kernelspec found')
            self.kernel_mgr = None

    async def _clean(self, clean_cmd):
        ret = 0
        try:
            if clean_cmd is None or clean_cmd == '':
                # skipped
                return
            elif clean_cmd == '*':
                ret = await self.clean_heuristic()
            else:
                ret = await self.run_subproc(clean_cmd)
        except Exception:
            log.exception('unexpected error')
            ret = -1
        finally:
            await asyncio.sleep(0.01)  # extra delay to flush logs
            payload = json.dumps({
                'exitCode': ret,
            }).encode('utf8')
            await self.outsock.send_multipart([b'clean-finished', payload])

    async def clean_heuristic(self) -> int:
        # it should not do anything by default.
        return 0

    async def _build(self, build_cmd):
        ret = 0
        try:
            if build_cmd is None or build_cmd == '':
                # skipped
                return
            elif build_cmd == '*':
                if Path('Makefile').is_file():
                    ret = await self.run_subproc('make')
                else:
                    ret = await self.build_heuristic()
            else:
                ret = await self.run_subproc(build_cmd)
        except Exception:
            log.exception('unexpected error')
            ret = -1
        finally:
            await asyncio.sleep(0.01)  # extra delay to flush logs
            self._build_success = (ret == 0)
            payload = json.dumps({
                'exitCode': ret,
            }).encode('utf8')
            await self.outsock.send_multipart([b'build-finished', payload])

    @abstractmethod
    async def build_heuristic(self) -> int:
        """Process build step."""

    async def _execute(self, exec_cmd: str):
        ret = 0
        try:
            if exec_cmd is None or exec_cmd == '':
                # skipped
                return
            elif exec_cmd == '*':
                ret = await self.execute_heuristic()
            else:
                ret = await self.run_subproc(exec_cmd)
        except Exception:
            log.exception('unexpected error')
            ret = -1
        finally:
            await asyncio.sleep(0.01)  # extra delay to flush logs
            payload = json.dumps({
                'exitCode': ret,
            }).encode('utf8')
            await self.outsock.send_multipart([b'finished', payload])

    @abstractmethod
    async def execute_heuristic(self) -> int:
        """Process execute step."""

    async def _query(self, code_text):
        ret = 0
        try:
            ret = await self.query(code_text)
        except Exception:
            log.exception('unexpected error')
            ret = -1
        finally:
            payload = json.dumps({
                'exitCode': ret,
            }).encode('utf8')
            await self.outsock.send_multipart([b'finished', payload])

    async def query(self, code_text) -> int:
        """Run user's code in query mode.

        The default interface is jupyter kernel. To use different interface,
        `Runner` subclass should override this method.
        """
        if not hasattr(self, 'kernel_mgr') or self.kernel_mgr is None:
            log.error('query mode is disabled: '
                      'failed to start jupyter kernel')
            return 127

        log.debug('executing in query mode...')

        async def output_hook(msg):
            content = msg.get('content', '')
            if msg['msg_type'] == 'stream':
                # content['name'] will be 'stdout' or 'stderr'.
                await self.outsock.send_multipart([content['name'].encode('ascii'),
                                                   content['text'].encode('utf-8')])
            elif msg['msg_type'] == 'error':
                tbs = '\n'.join(content['traceback'])
                await self.outsock.send_multipart([b'stderr', tbs.encode('utf-8')])
            elif msg['msg_type'] in ['execute_result', 'display_data']:
                data = content['data']
                if len(data) < 1:
                    return
                if len(data) > 1:
                    data.pop('text/plain', None)
                dtype, dval = list(data.items())[0]

                if dtype == 'text/plain':
                    await self.outsock.send_multipart([b'stdout',
                                                       dval.encode('utf-8')])
                elif dtype == 'text/html':
                    await self.outsock.send_multipart([b'media',
                                                       dval.encode('utf-8')])
                # elif dtype == 'text/markdown':
                #     NotImplementedError
                # elif dtype == 'text/latex':
                #     NotImplementedError
                # elif dtype in ['application/json', 'application/javascript']:
                #     NotImplementedError
                elif dtype in ['image/png', 'image/jpeg']:
                    await self.outsock.send_multipart([
                        b'media',
                        json.dumps({
                            'type': dtype,
                            'data': f'data:{dtype};base64,{dval}',
                        }).encode('utf-8')
                    ])
                elif dtype == 'image/svg+xml':
                    await self.outsock.send_multipart([
                        b'media',
                        json.dumps({'type': dtype, 'data': dval}).encode('utf8')
                    ])

        async def stdin_hook(msg):
            if msg['msg_type'] == 'input_request':
                prompt = msg['content']['prompt']
                password = msg['content']['password']
                if prompt:
                    await self.outsock.send_multipart([
                        b'stdout', prompt.encode('utf-8')])
                await self.outsock.send_multipart(
                    [b'waiting-input',
                     json.dumps({'is_password': password}).encode('utf-8')])
                user_input = await self._user_input_queue.async_q.get()
                self.kernel_client.input(user_input)

        # Run jupyter kernel's blocking execution method in an executor pool.
        allow_stdin = False if self._user_input_queue is None else True
        stdin_hook = None if self._user_input_queue is None else stdin_hook
        try:
            await aexecute_interactive(self.kernel_client, code_text, timeout=None,
                                       output_hook=output_hook,
                                       allow_stdin=allow_stdin,
                                       stdin_hook=stdin_hook)
        except Exception as e:
            log.error(str(e))
            return 127
        return 0

    async def _complete(self, completion_data):
        try:
            return await self.complete(completion_data)
        except Exception:
            log.exception('unexpected error')
        finally:
            pass  # no need to "finished" signal

    async def complete(self, completion_data):
        """Return the list of strings to be shown in the auto-complete list.

        The default interface is jupyter kernel. To use different interface,
        `Runner` subclass should override this method.
        """
        # TODO: implement with jupyter_client
        '''
        matches = []
        self.outsock.send_multipart([
            b'completion',
            json.dumps(matches).encode('utf8'),
        ])
        '''
        # if hasattr(self, 'kernel_mgr') and self.kernel_mgr is not None:
        #     self.kernel_mgr.complete(data, len(data))
        # else:
        #     return []
        return []

    async def _interrupt(self):
        try:
            if self.subproc:
                self.subproc.send_signal(signal.SIGINT)
                return
            return await self.interrupt()
        except Exception:
            log.exception('unexpected error')
        finally:
            # this is a unidirectional command -- no explicit finish!
            pass

    async def interrupt(self):
        """Interrupt the running user code (only called for query-mode).

        The default interface is jupyter kernel. To use different interface,
        `Runner` subclass should implement its own `complete` method.
        """
        if hasattr(self, 'kernel_mgr') and self.kernel_mgr is not None:
            self.kernel_mgr.interrupt_kernel()

    async def _send_status(self):
        data = {
            'started_at': self.started_at,
        }
        await self.outsock.send_multipart([
            b'status',
            msgpack.packb(data, use_bin_type=True),
        ])

    @abstractmethod
    async def start_service(self, service_info):
        """Start an application service daemon."""
        return None, {}

    async def _start_service(self, service_info):
        try:
            if service_info['name'] in self.services_running:
                result = {'status': 'running'}
                return
            print(f"starting service {service_info['name']}")
            if service_info['name'] == 'ttyd':
                cmdargs, env = await prepare_ttyd_service(service_info)
            elif service_info['name'] == 'sshd':
                cmdargs, env = await prepare_sshd_service(service_info)
            else:
                cmdargs, env = await self.start_service(service_info)
            if cmdargs is None:
                log.warning('The service {0} is not supported.',
                            service_info['name'])
                result = {'status': 'failed',
                          'error': 'unsupported service'}
                return
            if service_info['protocol'] == 'pty':
                # TODO: handle pseudo-tty
                raise NotImplementedError
            else:
                service_env = {**self.child_env, **env}
                # avoid conflicts with Python binary used by service apps.
                if 'LD_LIBRARY_PATH' in service_env:
                    service_env['LD_LIBRARY_PATH'] = \
                        service_env['LD_LIBRARY_PATH'].replace('/opt/backend.ai/lib:', '')
                proc = await asyncio.create_subprocess_exec(
                    *cmdargs,
                    env=service_env,
                )
                self.service_processes.append(proc)
            self.services_running.add(service_info['name'])
            await wait_local_port_open(service_info['port'])
            result = {'status': 'started'}
        except Exception as e:
            log.exception('unexpected error')
            result = {'status': 'failed', 'error': repr(e)}
        finally:
            await self.outsock.send_multipart([
                b'service-result',
                json.dumps(result).encode('utf8'),
            ])

    async def run_subproc(self, cmd: str):
        """A thin wrapper for an external command."""
        loop = current_loop()
        if Path('/home/work/.logs').is_dir():
            kernel_id = Path('/home/config/kernel_id.txt').read_text().strip()
            log_path = Path(
                '/home/work/.logs/task/'
                f'{kernel_id[:2]}/{kernel_id[2:4]}/{kernel_id[4:]}.log'
            )
            log_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            log_path = Path(os.path.devnull)
        try:
            # errors like "command not found" is handled by the spawned shell.
            # (the subproc will terminate immediately with return code 127)
            if isinstance(cmd, (list, tuple)):
                exec_func = partial(asyncio.create_subprocess_exec, *cmd)
            else:
                exec_func = partial(asyncio.create_subprocess_shell, cmd)
            pipe_opts = {}
            pipe_opts['stdout'] = asyncio.subprocess.PIPE
            pipe_opts['stderr'] = asyncio.subprocess.PIPE
            with open(log_path, 'ab') as log_out:
                proc = await exec_func(
                    env=self.child_env,
                    stdin=None,
                    **pipe_opts,
                )
                self.subproc = proc
                pipe_tasks = [
                    loop.create_task(
                        pipe_output(proc.stdout, self.outsock, 'stdout',
                                    log_out.fileno())),
                    loop.create_task(
                        pipe_output(proc.stderr, self.outsock, 'stderr',
                                    log_out.fileno())),
                ]
                retcode = await proc.wait()
                await asyncio.gather(*pipe_tasks)
            return retcode
        except Exception:
            log.exception('unexpected error')
            return -1
        finally:
            self.subproc = None

    async def shutdown(self):
        pass

    async def handle_user_input(self, reader, writer):
        try:
            if self.user_input_queue is None:
                writer.write(b'<user-input is unsupported>')
            else:
                await self.outsock.send_multipart([b'waiting-input', b''])
                text = await self.user_input_queue.get()
                writer.write(text.encode('utf8'))
            await writer.drain()
            writer.close()
        except Exception:
            log.exception('unexpected error (handle_user_input)')

    async def run_tasks(self):
        while True:
            try:
                coro = await self.task_queue.get()

                if (self._build_success is not None and
                        coro.func == self._execute and
                        not self._build_success):
                    self._build_success = None
                    # skip exec step with "command not found" exit code
                    payload = json.dumps({
                        'exitCode': 127,
                    }).encode('utf8')
                    await self.outsock.send_multipart([b'finished', payload])
                    self.task_queue.task_done()
                    continue

                await coro()
                self.task_queue.task_done()
            except asyncio.CancelledError:
                break

    async def _handle_logs(self):
        log_queue = self.log_queue.async_q
        try:
            while True:
                rec = await log_queue.get()
                await self.outsock.send_multipart(rec)
                log_queue.task_done()
        except asyncio.CancelledError:
            self.log_queue.close()
            await self.log_queue.wait_closed()

    async def main_loop(self, cmdargs):
        user_input_server = \
            await asyncio.start_server(self.handle_user_input,
                                       '127.0.0.1', 65000)
        await self._init_with_loop()
        await self._init_jupyter_kernel()
        log.debug('start serving...')
        while True:
            try:
                data = await self.insock.recv_multipart()
                if len(data) != 2:
                    # maybe some garbage data
                    continue
                op_type = data[0].decode('ascii')
                text = data[1].decode('utf8')
                if op_type == 'clean':
                    await self.task_queue.put(partial(self._clean, text))
                if op_type == 'build':    # batch-mode step 1
                    await self.task_queue.put(partial(self._build, text))
                elif op_type == 'exec':   # batch-mode step 2
                    await self.task_queue.put(partial(self._execute, text))
                elif op_type == 'code':   # query-mode
                    await self.task_queue.put(partial(self._query, text))
                elif op_type == 'input':  # interactive input
                    if self.user_input_queue is not None:
                        await self.user_input_queue.put(text)
                elif op_type == 'complete':  # auto-completion
                    data = json.loads(text)
                    await self._complete(data)
                elif op_type == 'interrupt':
                    await self._interrupt()
                elif op_type == 'status':
                    await self._send_status()
                elif op_type == 'start-service':  # activate a service port
                    data = json.loads(text)
                    await self._start_service(data)
            except asyncio.CancelledError:
                break
            except NotImplementedError:
                log.error('Unsupported operation for this kernel: {0}', op_type)
                await asyncio.sleep(0)
            except Exception:
                log.exception('unexpected error')
                # we need to continue anyway unless we are shutting down
                continue
        user_input_server.close()
        await user_input_server.wait_closed()
        await self.shutdown()

    async def _init(self, cmdargs):
        self.cmdargs = cmdargs
        self.loop = current_loop()
        # Initialize event loop.
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        self.loop.set_default_executor(executor)

        self.insock = self.zctx.socket(zmq.PULL, io_loop=self.loop)
        self.insock.bind('tcp://*:2000')
        self.outsock = self.zctx.socket(zmq.PUSH, io_loop=self.loop)
        self.outsock.bind('tcp://*:2001')

        self.log_queue = janus.Queue(loop=self.loop)
        self.task_queue = asyncio.Queue(loop=self.loop)
        self.init_done = asyncio.Event(loop=self.loop)

        setup_logger(self.log_queue.sync_q, self.log_prefix, cmdargs.debug)
        self._log_task = self.loop.create_task(self._handle_logs())
        self._main_task = self.loop.create_task(self.main_loop(cmdargs))
        self._run_task = self.loop.create_task(self.run_tasks())

    async def _shutdown(self):
        try:
            self.insock.close()
            log.debug('shutting down...')
            self._run_task.cancel()
            self._main_task.cancel()
            await self._run_task
            await self._main_task
            if self.service_processes:
                log.debug('terminating service processes...')
                await asyncio.gather(
                    *(terminate_and_kill(proc) for proc in self.service_processes),
                    return_exceptions=True,
                )
            log.debug('terminated.')
        finally:
            # allow remaining logs to be flushed.
            await asyncio.sleep(0.1)
            try:
                if self.outsock:
                    self.outsock.close()
                await self._shutdown_jupyter_kernel()
            finally:
                self._log_task.cancel()
                await self._log_task

    async def _shutdown_jupyter_kernel(self):
        if self.kernel_mgr and self.kernel_mgr.is_alive():
            log.info('shutting down ' + self.jupyter_kspec_name + ' kernel...')
            self.kernel_client.stop_channels()
            self.kernel_mgr.shutdown_kernel()
            assert not self.kernel_mgr.is_alive(), 'ipykernel failed to shutdown'

    def run(self, cmdargs):
        if cmdargs.runtime_path is None:
            self.runtime_path = self.default_runtime_path
        else:
            self.runtime_path = cmdargs.runtime_path

        # Replace stdin with a "null" file
        # (trying to read stdin will raise EOFError immediately afterwards.)
        sys.stdin = open(os.devnull, 'rb')

        # Terminal does not work with uvloop! :(
        # FIXME: asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        asyncio_run_forever(self._init(cmdargs), self._shutdown(),
                            stop_signals={signal.SIGINT, signal.SIGTERM})
