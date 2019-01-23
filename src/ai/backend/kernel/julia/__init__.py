import asyncio
import logging
import os
from pathlib import Path
import tempfile

import janus
from jupyter_client import KernelManager
from jupyter_client.kernelspec import KernelSpecManager

from .. import BaseRunner

log = logging.getLogger()


class Runner(BaseRunner):

    log_prefix = 'julia-kernel'
    default_runtime_path = '/usr/local/julia'
    default_child_env = {
        'TERM': 'xterm',
        'LANG': 'C.UTF-8',
        'SHELL': '/bin/ash' if Path('/bin/ash').is_file() else '/bin/bash',
        'USER': 'work',
        'HOME': '/home/work',
        'PATH': ('/usr/local/julia:/usr/local/julia/bin:/usr/local/sbin:'
             '/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'),
        'LD_LIBRARY_PATH': os.environ.get('LD_LIBRARY_PATH', ''),
        'JULIA_LOAD_PATH': ':/opt/julia',
    }

    def __init__(self):
        super().__init__()
        self.input_queue = None
        self.output_queue = None

        # Detect ijulia kernel spec for backend.ai and start it.
        kernelspec_mgr = KernelSpecManager()
        kspecs = kernelspec_mgr.get_all_specs()
        for kname in kspecs:
            if 'julia' in kname:
                log.info('starting ijulia kernel...')
                cmd = '/usr/local/bin/movecompiled.sh'
                asyncio.get_event_loop().run_until_complete(self.run_subproc(cmd))
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
            log.info('shutting down ijulia kernel...')
            self.kernel_client.stop_channels()
            self.kernel_mgr.shutdown_kernel()
            assert not self.kernel_mgr.is_alive(), 'ijulia kernel failed to shutdown'

    async def build_heuristic(self) -> int:
        log.info('no build process for julia language')
        return 0

    async def execute_heuristic(self) -> int:
        if Path('main.jl').is_file():
            cmd = 'julia main.jl'
            return await self.run_subproc(cmd)
        else:
            log.error('cannot find executable ("main.jl").')
            return 127

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
        log.error("start service")
        if service_info['name'] == 'jupyter':
            with tempfile.NamedTemporaryFile(
                    'w', encoding='utf-8', suffix='.py', delete=False) as config:
                print('c.NotebookApp.allow_root = True', file=config)
                print('c.NotebookApp.ip = "0.0.0.0"', file=config)
                print('c.NotebookApp.port = {}'.format(service_info['port']),
                      file=config)
                print('c.NotebookApp.token = ""', file=config)
            return [
                'jupyter', 'notebook',
                '--no-browser',
                '--config',
                config.name,
            ], {}
