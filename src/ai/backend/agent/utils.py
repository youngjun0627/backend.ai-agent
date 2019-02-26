import asyncio
from typing import MutableMapping, Sequence

from packaging import version

from . import __version__ as VERSION


def update_nested_dict(dest, additions):
    for k, v in additions.items():
        if k not in dest:
            dest[k] = v
        else:
            if isinstance(dest[k], MutableMapping):
                assert isinstance(v, MutableMapping)
                update_nested_dict(dest[k], v)
            elif isinstance(dest[k], Sequence):
                assert isinstance(v, Sequence)
                dest[k].extend(v)
            else:
                dest[k] = v


if hasattr(asyncio, 'get_running_loop'):
    current_loop = asyncio.get_running_loop
else:
    current_loop = asyncio.get_event_loop


def get_krunner_image_ref(distro):
    v = version.parse(VERSION)
    if v.is_devrelease or v.is_prerelease:
        return f'lablup/backendai-krunner-env:dev-{distro}'
    return f'lablup/backendai-krunner-env:{VERSION}-{distro}'
