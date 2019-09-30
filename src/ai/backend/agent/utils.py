import asyncio
from decimal import Decimal
import io
import ipaddress
import logging
from pathlib import Path
from typing import (
    Any, Optional,
    Callable, Iterable,
    Mapping, MutableMapping,
    List, Sequence, Union,
    Type, overload,
)
from typing_extensions import Final
from uuid import UUID

from aiodocker.docker import DockerContainer
import netifaces
import trafaret as t

from ai.backend.common import identity
from ai.backend.common.etcd import AsyncEtcd
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    PID, HostPID, ContainerPID, KernelId,
    ServicePort,
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.utils'))

IPNetwork = Union[ipaddress.IPv4Network, ipaddress.IPv6Network]
IPAddress = Union[ipaddress.IPv4Address, ipaddress.IPv6Address]

InOtherContainerPID: Final = ContainerPID(PID(-2))
NotContainerPID: Final = ContainerPID(PID(-1))
NotHostPID: Final = HostPID(PID(-1))


def update_nested_dict(dest: MutableMapping, additions: Mapping) -> None:
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


current_loop: Callable[[], asyncio.AbstractEventLoop]
if hasattr(asyncio, 'get_running_loop'):
    current_loop = asyncio.get_running_loop  # type: ignore
else:
    current_loop = asyncio.get_event_loop    # type: ignore


def numeric_list(s: str) -> List[int]:
    return [int(p) for p in s.split()]


def parse_service_port(s: str) -> ServicePort:
    try:
        name, protocol, port = s.split(':')
    except (ValueError, IndexError):
        raise ValueError('Invalid service port definition format', s)
    assert protocol in ('tcp', 'pty', 'http'), \
           f'Unsupported service port protocol: {protocol}'
    try:
        port_no = int(port)
    except ValueError:
        raise ValueError('Invalid port number', port_no)
    if port_no <= 1024:
        raise ValueError('Service port number must be larger than 1024.')
    if port_no in (2000, 2001):
        raise ValueError('Service port 2000 and 2001 is reserved for internal use.')
    return {
        'name': name,
        'protocol': protocol,
        'container_port': port_no,
        'host_port': None,  # determined after container start
    }


def remove_exponent(num: Decimal) -> Decimal:
    return num.quantize(Decimal(1)) if num == num.to_integral() else num.normalize()


@overload
def read_sysfs(path: Union[str, Path], type_: Type[bool], default: bool) -> bool:
    ...


@overload
def read_sysfs(path: Union[str, Path], type_: Type[int], default: int) -> int:
    ...


@overload
def read_sysfs(path: Union[str, Path], type_: Type[float], default: float) -> float:
    ...


@overload
def read_sysfs(path: Union[str, Path], type_: Type[str], default: str) -> str:
    ...


def read_sysfs(path: Union[str, Path], type_: Type[Any], default: Any = None) -> Any:
    def_vals: Mapping[Any, Any] = {
        bool: False,
        int: 0,
        float: 0.0,
        str: '',
    }
    if type_ not in def_vals:
        raise TypeError('unsupported conversion type from sysfs content')
    if default is None:
        default = def_vals[type_]
    try:
        raw_str = Path(path).read_text().strip()
        if type_ is bool:
            return t.StrBool().check(raw_str)
        else:
            return type_(raw_str)
    except IOError:
        return default


async def read_tail(path: Path, nbytes: int) -> bytes:
    file_size = path.stat().st_size

    def _read_tail() -> bytes:
        with open(path, 'rb') as f:
            f.seek(max(file_size - nbytes, 0), io.SEEK_SET)
            return f.read(nbytes)

    loop = current_loop()
    return await loop.run_in_executor(None, _read_tail)


async def get_kernel_id_from_container(val: Union[str, DockerContainer]) -> Optional[KernelId]:
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
        return KernelId(UUID(name.rsplit('.', 2)[-1]))
    except (IndexError, ValueError):
        return None


async def get_subnet_ip(etcd: AsyncEtcd, network: str, fallback_addr: str = '0.0.0.0') -> str:
    subnet = await etcd.get(f'config/network/subnet/{network}')
    if subnet is None:
        addr = fallback_addr
    else:
        subnet = ipaddress.ip_network(subnet)
        if subnet.prefixlen == 0:
            addr = fallback_addr
        else:
            local_ipaddrs = [*identity.fetch_local_ipaddrs(subnet)]
            log.debug('get_subnet_ip(): subnet {} candidates: {}',
                      subnet, local_ipaddrs)
            if local_ipaddrs:
                addr = str(local_ipaddrs[0])
            else:
                addr = fallback_addr
    return addr


async def host_pid_to_container_pid(container_id: str, host_pid: HostPID) -> ContainerPID:
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
                nspids = [*map(lambda pid: ContainerPID(PID(int(pid))), proc_status['NSpid'].split())]
                return nspids[1]
            return InOtherContainerPID
        return NotContainerPID
    except (ValueError, KeyError, IOError):
        return NotContainerPID


async def container_pid_to_host_pid(container_id: str, container_pid: ContainerPID) -> HostPID:
    # TODO: implement
    return NotHostPID


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
