from asyncio import create_subprocess_exec, subprocess
import logging
import os
from pathlib import Path
import tempfile
from typing import Any, List, Mapping, MutableMapping, Optional

from .logging import BraceStyleAdapter

logger = BraceStyleAdapter(logging.getLogger())


async def write_file(
        variables: Mapping[str, Any], filename: str = '',
        body: List[str] = [], mode: str = '', append: bool = False):
    if len(filename) == 0 or len(body) == 0:
        return
    filename = filename.format_map(variables)
    open_mode = 'w' + ('+' if append else '')

    with open(filename, open_mode) as fw:
        for line in body:
            fw.write(line.format_map(variables) + '\n')
    if len(mode) > 0:
        os.chmod(filename, int(mode, 8))


async def write_tempfile(variables: Mapping[str, Any], body: List[str] = [], mode: str = '') -> \
                         Optional[str]:
    if len(body) == 0:
        return None
    with tempfile.NamedTemporaryFile(
            'w', encoding='utf-8', suffix='.py', delete=False) as config:
        for line in body:
            config.write(line.format_map(variables))
    if len(mode) > 0:
        os.chmod(config.name, int(mode, 8))
    return config.name


async def run_command(variables: Mapping[str, Any], command: List[str] = []) -> \
                      Optional[MutableMapping[str, str]]:
    if len(command) == 0:
        return None
    proc = await create_subprocess_exec(*command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = await proc.communicate()
    return {'out': out.decode(), 'err': err.decode()}


async def mkdir(variables: Mapping[str, Any], path: str = ''):
    if len(path) == 0:
        return
    Path(path).mkdir(parents=True, exist_ok=True)


async def log(variables: Mapping[str, Any], body: str = '', debug: bool = False):
    if len(body) == 0:
        return

    body_format = body.format_map(variables).replace('{', '{{').replace('}', '}}')
    if debug:
        logger.debug(body_format)
    else:
        logger.info(body_format)
