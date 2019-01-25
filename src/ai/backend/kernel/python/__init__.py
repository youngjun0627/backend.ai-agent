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
        'PYTHONPATH': site.USER_SITE,
    }
    jupyter_kspec_name = 'backendai-python'

    def __init__(self):
        super().__init__()
        self.input_queue = None
        self.output_queue = None

        # Add sitecustomize.py to site-packages directory.
        # No permission to access global site packages, we use user local directory.
        input_src = Path(os.path.dirname(__file__)) / 'sitecustomize.py'
        # pkgdir = Path(site.getsitepackages()[0])
        pkgdir = Path(site.USER_SITE)
        pkgdir.mkdir(parents=True, exist_ok=True)
        shutil.copy(str(input_src), str(pkgdir / 'sitecustomize.py'))

    async def init_with_loop(self):
        self.input_queue = janus.Queue(loop=self.loop)
        self.output_queue = janus.Queue(loop=self.loop)

        # We have interactive input functionality!
        self._user_input_queue = janus.Queue(loop=self.loop)
        self.user_input_queue = self._user_input_queue.async_q

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
        elif service_info['name'] == 'digits':
            return [
                self.runtime_path, '-m', 'digits',
            ], {}
        elif service_info['name'] == 'tensorboard':
            return [
                self.runtime_path, '-m', 'tensorboard.main',
            ], {}
