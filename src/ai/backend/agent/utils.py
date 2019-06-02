import asyncio
from decimal import Decimal
import ipaddress
from pathlib import Path
from typing import Iterable, MutableMapping, Sequence, Union

from aiodocker.docker import DockerContainer
import netifaces
from packaging import version

from . import __version__ as VERSION

IPNetwork = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]
IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]


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


def numeric_list(s):
    return [int(p) for p in s.split()]


def remove_exponent(num: Decimal):
    return num.quantize(Decimal(1)) if num == num.to_integral() else num.normalize()


def read_sysfs(path, type_=int, default_val=0):
    return type_(Path(path).read_text().strip())


def get_krunner_image_ref(distro):
    v = version.parse(VERSION)
    if v.is_devrelease or v.is_prerelease:
        return f'lablup/backendai-krunner-env:dev-{distro}'
    return f'lablup/backendai-krunner-env:{VERSION}-{distro}'


async def get_kernel_id_from_container(val):
    if isinstance(val, DockerContainer):
        if 'Name' not in val._container:
            await val.show()
        name = val['Name']
    elif isinstance(val, str):
        name = val
    name = name.lstrip('/')
    if not name.startswith('kernel.'):
        return None
    try:
        return name.rsplit('.', 2)[-1]
    except (IndexError, ValueError):
        return None


async def host_pid_to_container_pid(container_id, host_pid):
    try:
        for p in Path('/sys/fs/cgroup/pids/docker').iterdir():
            if not p.is_dir():
                continue
            tasks_path = p / 'tasks'
            cgtasks = [*map(int, tasks_path.read_text().splitlines())]
            if host_pid not in cgtasks:
                continue
            if p.name == container_id:
                proc_path = Path(f'/proc/{host_pid}/status')
                proc_status = {k: v for k, v
                               in map(lambda l: l.split(':\t'),
                                      proc_path.read_text().splitlines())}
                nspids = [*map(int, proc_status['NSpid'].split())]
                return nspids[1]  # in the given container
            return -2  # in other container
        return -1  # in host
    except (ValueError, KeyError, IOError):
        return -1  # in host


async def container_pid_to_host_pid(container_id, container_pid):
    # TODO: implement
    return -1


def fetch_local_ipaddrs(cidr: IPNetwork) -> Iterable[IPAddress]:
    ifnames = netifaces.interfaces()
    proto = netifaces.AF_INET if cidr.version == 4 else netifaces.AF_INET6
    for ifname in ifnames:
        addrs = netifaces.ifaddresses(ifname).get(proto, None)
        if addrs is None:
            continue
        for entry in addrs:
            addr = ipaddress.ip_address(entry['addr'])
            if addr in cidr:
                yield addr
