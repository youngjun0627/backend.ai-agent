import asyncio
import logging
import os
from pathlib import Path
import shutil
import tempfile

import janus

from .. import BaseRunner

log = logging.getLogger()

DEFAULT_PYFLAGS = []


class Runner(BaseRunner):

    log_prefix = 'binary-kernel'
    default_runtime_path = '/usr/local/bin'
    default_child_env = {
        'TERM': 'xterm',
        'LANG': 'C.UTF-8',
        'SHELL': '/bin/ash' if Path('/bin/ash').is_file() else '/bin/bash',
        'USER': 'work',
        'HOME': '/home/work',
        'PATH': ':'.join([
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

    def __init__(self):
        super().__init__()
        self.input_queue = None
        self.output_queue = None

    async def init_with_loop(self):
        self.input_queue = janus.Queue(loop=self.loop)
        self.output_queue = janus.Queue(loop=self.loop)

    async def build_heuristic(self) -> int:
        raise NotImplementedError

    async def execute_heuristic(self) -> int:
        raise NotImplementedError

    async def start_service(self, service_info):
        print(service_info['name'])
        if service_info['name'] in ['ttyd']:
            return [
                self.runtime_path, '-p', '8090', 'bash'
            ], {}
