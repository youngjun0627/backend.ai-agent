import asyncio
import functools
import json
import logging
import os
from pathlib import Path
import shutil
import site
import tempfile

import janus
from jupyter_client import KernelManager
from jupyter_client.kernelspec import KernelSpecManager

from .. import BaseRunner

log = logging.getLogger()

DEFAULT_PYFLAGS = []


class Runner(BaseRunner):

    log_prefix = 'python-kernel'
    default_runtime_path = '/usr/bin/python'
    default_child_env = {
        'TERM': 'xterm',
        'LANG': 'C.UTF-8',
        'SHELL': '/bin/ash' if Path('/bin/ash').is_file() else '/bin/bash',
        'USER': 'work',
        'HOME': '/home/work',
        'PATH': ':'.join([
            '/usr/local/nvidia/bin',
            '/usr/local/cuda/bin',
            '/usr/local/sbin',
            '/usr/local/bin',
            '/usr/sbin',
            '/usr/bin',
            '/sbin',
            '/bin',
        ]),
        'LD_LIBRARY_PATH': os.environ.get('LD_LIBRARY_PATH', ''),
        'LD_PRELOAD': os.environ.get('LD_PRELOAD', ''),
        'PYTHONPATH': site.USER_SITE,
    }

    def __init__(self):
        super().__init__()
        self.sentinel = object()
        self.input_queue = None
        self.output_queue = None

        # Add sitecustomize.py to site-packages directory.
        # No permission to access global site packages, we use user local directory.
        input_src = Path(os.path.dirname(__file__)) / 'sitecustomize.py'
        # pkgdir = Path(site.getsitepackages()[0])
        pkgdir = Path(site.USER_SITE)
        pkgdir.mkdir(parents=True, exist_ok=True)
        shutil.copy(str(input_src), str(pkgdir / 'sitecustomize.py'))

        # Detect ipython kernel spec for backend.ai and start it.
        kernelspec_mgr = KernelSpecManager()
        kspecs = kernelspec_mgr.get_all_specs()
        for kname in kspecs:
            if kname.startswith('backendai-python'):
                log.info('starting ipykernel...')
                self.kernel_mgr = KernelManager(kernel_name=kname)
                self.kernel_mgr.start_kernel()
                if not self.kernel_mgr.is_alive():
                    log.error('query mode is disabled: '
                              'failed to start jupyter kernel')
                else:
                    self.kernel_client = self.kernel_mgr.client()
                    self.kernel_client.start_channels(shell=True, iopub=True,
                                                      stdin=True, hb=True)
                    try:
                        self.kernel_client.wait_for_ready(timeout=5)
                    except RuntimeError:
                        # Clean up for client and kernel will be done in `shutdown`.
                        log.error('jupyter channel is not active!')
                        self.kernel_mgr = None
                break
        else:
            log.info('query mode is disabled: '
                     'no jupyter kernelspec found')
            self.kernel_mgr = None

    async def init_with_loop(self):
        self.input_queue = janus.Queue(loop=self.loop)
        self.output_queue = janus.Queue(loop=self.loop)

        # We have interactive input functionality!
        self._user_input_queue = janus.Queue(loop=self.loop)
        self.user_input_queue = self._user_input_queue.async_q

    async def shutdown(self):
        if self.kernel_mgr and self.kernel_mgr.is_alive():
            log.info('shutting down ipykernel...')
            self.kernel_client.stop_channels()
            self.kernel_mgr.shutdown_kernel()
            assert not self.kernel_mgr.is_alive(), 'ipykernel failed to shutdown'

    async def build_heuristic(self) -> int:
        if Path('setup.py').is_file():
            cmd = [
                self.runtime_path, *DEFAULT_PYFLAGS,
                '-m', 'pip', 'install', '-e', '.',
            ]
            return await self.run_subproc(cmd)
        else:
            log.warning('skipping the build phase due to missing "setup.py" file')
            return 0

    async def execute_heuristic(self) -> int:
        if Path('main.py').is_file():
            cmd = [
                self.runtime_path, *DEFAULT_PYFLAGS,
                'main.py',
            ]
            return await self.run_subproc(cmd)
        else:
            log.error('cannot find the main script ("main.py").')
            return 127

    async def query(self, code_text) -> int:
        if self.kernel_mgr is None:
            log.error('query mode is disabled: '
                      'failed to start jupyter kernel')
            return 127

        log.debug('executing in query mode...')
        loop = asyncio.get_event_loop()

        def output_hook(msg):
            if msg['msg_type'] == 'stream':
                content = msg['content']
                loop.call_soon_threadsafe(self.outsock.send_multipart,
                                          [content['name'].encode('ascii'),
                                           content['text'].encode('utf-8')])

        def stdin_hook(msg):
            if msg['msg_type'] == 'input_request':
                prompt = msg['content']['prompt']
                password = msg['content']['password']
                if prompt:
                    loop.call_soon_threadsafe(self.outsock.send_multipart,
                                              [b'stdout', prompt.encode('utf-8')])
                loop.call_soon_threadsafe(
                    self.outsock.send_multipart,
                    [b'waiting-input',
                     json.dumps({'is_password': password}).encode('utf-8')])
                user_input = self._user_input_queue.sync_q.get()
                self.kernel_client.input(user_input)

        # Run jupyter kernel's blocking execution method in an executor pool.
        await loop.run_in_executor(
            None,
            functools.partial(self.kernel_client.execute_interactive,
                              code_text, allow_stdin=True, timeout=2,
                              output_hook=output_hook, stdin_hook=stdin_hook)
        )
        return 0

    async def complete(self, data):
        # TODO: implement with jupyter_client
        '''
        matches = []
        self.outsock.send_multipart([
            b'completion',
            json.dumps(matches).encode('utf8'),
        ])
        '''
        # self.kernel_mgr.complete(data, len(data))

    async def interrupt(self):
        # TODO: implement with jupyter_client
        self.kernel_mgr.interrupt_kernel()

    async def start_service(self, service_info):
        if service_info['name'] == 'jupyter':
            with tempfile.NamedTemporaryFile(
                    'w', encoding='utf-8', suffix='.py', delete=False) as config:
                print('c.NotebookApp.allow_root = True', file=config)
                print('c.NotebookApp.ip = "0.0.0.0"', file=config)
                print('c.NotebookApp.port = {}'.format(service_info['port']),
                      file=config)
                print('c.NotebookApp.token = ""', file=config)
            return [
                self.runtime_path, '-m', 'jupyter', 'notebook',
                '--no-browser',
                '--config', config.name,
            ], {}
        elif service_info['name'] == 'ipython':
            return [
                self.runtime_path, '-m', 'IPython',
            ], {}
