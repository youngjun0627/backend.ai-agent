import asyncio
import logging
import os
from pathlib import Path
import tempfile
from typing import List

from ... import BaseRunner

log = logging.getLogger()

DEFAULT_PYFLAGS: List[str] = []


class Runner(BaseRunner):

    log_prefix = 'matlab-kernel'
    default_runtime_path = '/usr/bin/python'
    default_child_env = {
        'TERM': 'xterm',
        'LANG': 'C.UTF-8',
        'SHELL': '/bin/bash',
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

    def __init__(self):
        super().__init__()

    async def init_with_loop(self):
        pass

    async def build_heuristic(self) -> int:
        log.warning('batch-mode execution is not supported')
        return 0

    async def execute_heuristic(self) -> int:
        log.warning('batch-mode execution is not supported')
        return 0

    async def start_service(self, service_info):
        if 'vnc' in service_info['name']:
            return [
                '/opt/noVNC/utils/launch.sh',
                '--listen', str(service_info['port']),
                '--vnc', 'localhost:5901',
            ], {}
