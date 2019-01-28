import logging
import os
from pathlib import Path
import tempfile

import janus

from .. import BaseRunner

log = logging.getLogger()


class Runner(BaseRunner):

    log_prefix = 'r-kernel'
    default_runtime_path = '/usr/bin/R'
    default_child_env = {
        'TERM': 'xterm',
        'LANG': 'C.UTF-8',
        'SHELL': '/bin/ash' if Path('/bin/ash').is_file() else '/bin/bash',
        'USER': 'work',
        'HOME': '/home/work',
        'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
        'LD_LIBRARY_PATH': os.environ.get('LD_LIBRARY_PATH', ''),
    }
    jupyter_kspec_name = 'ir'

    def __init__(self):
        super().__init__()
        self.input_queue = None
        self.output_queue = None

    async def init_with_loop(self):
        self.input_queue = janus.Queue(loop=self.loop)
        self.output_queue = janus.Queue(loop=self.loop)

        # We have interactive input functionality!
        self._user_input_queue = janus.Queue(loop=self.loop)
        self.user_input_queue = self._user_input_queue.async_q

    async def build_heuristic(self):
        log.info('no build process for R language')
        return 0

    async def execute_heuristic(self):
        if Path('main.R').is_file():
            cmd = 'Rscript main.R'
            return await self.run_subproc(cmd)
        else:
            log.error('cannot find executable ("main.R").')
            return 127

    async def start_service(self, service_info):
        if service_info['name'] == 'jupyter' or service_info['name'] == 'jupyterlab':
            with tempfile.NamedTemporaryFile(
                    'w', encoding='utf-8', suffix='.py', delete=False) as config:
                print('c.NotebookApp.allow_root = True', file=config)
                print('c.NotebookApp.ip = "0.0.0.0"', file=config)
                print('c.NotebookApp.port = {}'.format(service_info['port']),
                      file=config)
                print('c.NotebookApp.token = ""', file=config)
            jupyter_service_type = 'lab' if service_info['name'] == 'jupyterlab' else 'notebook' 
            return [
                self.runtime_path, '-m', 'jupyter', jupyter_service_type,
                '--no-browser',
                '--config', config.name,
            ], {}
