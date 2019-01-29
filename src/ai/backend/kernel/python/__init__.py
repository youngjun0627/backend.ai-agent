import asyncio
import logging
import os
from pathlib import Path
import shutil
import site
import tempfile

import janus

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
    }
    jupyter_kspec_name = 'python'

    def __init__(self):
        super().__init__()
        self.input_queue = None
        self.output_queue = None

    async def init_with_loop(self):
        self.input_queue = janus.Queue(loop=self.loop)
        self.output_queue = janus.Queue(loop=self.loop)

        # We have interactive input functionality for query mode!
        self._user_input_queue = janus.Queue(loop=self.loop)
        self.user_input_queue = self._user_input_queue.async_q

        # Get USER_SITE for runtime python.
        cmd = [self.runtime_path, *DEFAULT_PYFLAGS,
               '-c', 'import site; print(site.USER_SITE)']
        proc = await asyncio.create_subprocess_exec(
            *cmd, env=self.child_env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        user_site = stdout.decode('utf8').strip()
        self.child_env['PYTHONPATH'] = user_site

        # Add support for interactive input in batch mode by copying
        # sitecustomize.py to USER_SITE of runtime python.
        sitecustomize_path = Path(os.path.dirname(__file__)) / 'sitecustomize.py'
        user_site = Path(user_site)
        user_site.mkdir(parents=True, exist_ok=True)
        shutil.copy(str(sitecustomize_path), str(user_site / 'sitecustomize.py'))

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

    async def start_service(self, service_info):
<<<<<<< HEAD
        if service_info['name'] == 'jupyter' or service_info['name'] == 'jupyterlab':
=======
        if service_info['name'] in ['jupyter', 'jupyterlab']:
>>>>>>> a525ebb39aa5da76d2d9c01b34f08c22d63a6b63
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
        elif service_info['name'] == 'ipython':
            return [
                self.runtime_path, '-m', 'IPython',
            ], {}
        elif service_info['name'] == 'digits':
            return [
                self.runtime_path, '-m', 'digits',
            ], {}
        elif service_info['name'] == 'tensorboard':
            return [
                self.runtime_path, '-m', 'tensorboard.main',
            ], {}
