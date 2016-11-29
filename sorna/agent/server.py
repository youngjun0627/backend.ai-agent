import argparse
import asyncio
from distutils.version import LooseVersion
from itertools import chain
import logging, logging.config
import os, os.path
from pathlib import Path
import re
import signal
import shutil
import sys
import time
import urllib.parse

import zmq, aiozmq
import aioredis
from async_timeout import timeout
import botocore, aiobotocore
import docker
from namedlist import namedtuple
import requests
import simplejson as json
import uvloop

from sorna import utils, defs
from sorna.argparse import ipaddr, port_no, host_port_pair, positive_int
from sorna.proto import Message
from sorna.utils import odict, generate_uuid, nmget
from sorna.proto.msgtypes import AgentRequestTypes, SornaResponseTypes
from . import __version__
from .resources import CPUAllocMap
from .helper import call_docker_with_retries


log = logging.getLogger('sorna.agent.server')
log.setLevel(logging.DEBUG)

container_registry = dict()
container_cpu_map = None

apparmor_profile_path = '/etc/apparmor.d/docker-ptrace'
volume_root = None
supported_langs = {
    'python2',
    'python3',
    'python3-tensorflow',
    'python3-tensorflow-gpu',
    'python3-caffe',
    'r3',
    'php5',
    'php7',
    'nodejs4',
    'git',
    'julia',
    'lua5',
    'haskell'
}
lang_aliases = dict()
# the names of following AWS variables follow boto3 convention.
s3_access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'dummy-access-key')
s3_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'dummy-secret-key')
s3_region = os.environ.get('AWS_REGION', 'ap-northeast-1')
s3_bucket = os.environ.get('AWS_S3_BUCKET', 'codeonweb')
max_upload_size = 5 * 1024 * 1024  # 5 MB
max_kernels = 1
idle_timeout = 600.0
inst_id = None
agent_addr = None
agent_ip = None
agent_port = 0
inst_type = None
redis_addr = (None, 6379)
docker_ip = None
docker_version = None

# Shortcut for str.format
_f = lambda fmt, *args, **kwargs: fmt.format(*args, **kwargs)


def dict2kvlist(o):
    return chain.from_iterable((k, v) for k, v in o.items())


def docker_init():
    global docker_ip, docker_version
    docker_args = docker.utils.kwargs_from_env()
    if docker_ip is None:  # if not overriden
        if 'base_url' in docker_args:
            docker_ip = urllib.parse.urlparse(docker_args['base_url']).hostname
        else:
            docker_ip = '127.0.0.1'
    docker_cli = docker.Client(timeout=5, **docker_args)
    docker_version = LooseVersion(docker_cli.version()['Version'])
    log.info('detected docker version: {}'.format(docker_version))
    return docker_cli


def read_sysfs(path):
    return Path(path).read_text().strip()


def collect_stats(docker_cli, container_id, loop=None):
    if sys.platform == 'linux':
        path = '/sys/fs/cgroup/cpuacct/docker/{}/cpuacct.usage'.format(container_id)
        cpu_used = int(read_sysfs(path)) / 1e6
        path = '/sys/fs/cgroup/memory/docker/{}/memory.max_usage_in_bytes'.format(container_id)
        mem_max_bytes = int(read_sysfs(path))
        # TODO: implement
        io_read_bytes = 0
        io_write_bytes = 0
        net_rx_bytes = 0
        net_tx_bytes = 0
    else:
        ret = docker_cli.stats(container_id, stream=False)
        cpu_used = ret['cpu_stats']['cpu_usage']['total_usage'] / 1e6
        mem_max_bytes = ret['memory_stats']['max_usage']
        io_read_bytes = 0
        io_write_bytes = 0
        for item in ret['blkio_stats']['io_service_bytes_recursive']:
            if item['op'] == 'Read':
                io_read_bytes += item['value']
            elif item['op'] == 'Write':
                io_write_bytes += item['value']
        net_rx_bytes = 0
        net_tx_bytes = 0
        for dev in ret['networks'].values():
            net_rx_bytes += dev['rx_bytes']
            net_tx_bytes += dev['tx_bytes']
    return {
        'cpu_used': cpu_used,
        'mem_max_bytes': mem_max_bytes,
        'net_rx_bytes': net_rx_bytes,
        'net_tx_bytes': net_tx_bytes,
        'io_read_bytes': io_read_bytes,
        'io_write_bytes': io_write_bytes,
    }


async def _heartbeat(loop, docker_cli, interval):
    global container_registry
    global inst_id, inst_type, agent_ip, redis_addr
    running_kernels = [k for k in container_registry.keys()]
    stats = {}
    # stats collection may take a while.
    for kern_id in running_kernels:
        container_id = container_registry[kern_id]['container_id']
        stats[kern_id] = collect_stats(docker_cli,
                                       container_id,
                                       loop=loop)
        stats[kern_id]['exec_timeout'] = container_registry[kern_id]['exec_timeout']
        stats[kern_id]['idle_timeout'] = idle_timeout
        mem_limit = container_registry[kern_id]['mem_limit']
        mem_limit_in_kb = utils.readable_size_to_bytes(mem_limit) // 1024
        stats[kern_id]['mem_limit']   = mem_limit_in_kb
        stats[kern_id]['num_queries'] = container_registry[kern_id]['num_queries']
        stats[kern_id]['idle'] = (time.monotonic() - container_registry[kern_id]['last_used']) * 1000
    try:
        with timeout(interval / 2):
            ri = await aioredis.create_redis(
                redis_addr, encoding='utf8',
                db=defs.SORNA_INSTANCE_DB,
                loop=loop)
            rk = await aioredis.create_redis(
                redis_addr, encoding='utf8',
                db=defs.SORNA_KERNEL_DB,
                loop=loop)
            # should match with sorna.manager.structs.Instance
            my_kernels_key = '{}.kernels'.format(inst_id)
            ri_pipe, rk_pipe = ri.pipeline(), rk.pipeline()
            ri_pipe.hmset(inst_id, *dict2kvlist({
                'status': 'ok',
                'id': inst_id,
                'ip': agent_ip,
                'addr': 'tcp://{}:{}'.format(agent_ip, agent_port),
                'type': inst_type,
                'num_kernels': len(running_kernels),
                'max_kernels': max_kernels,
            }))
            ri_pipe.delete(my_kernels_key)
            for kern_id in running_kernels:
                ri_pipe.sadd(my_kernels_key, kern_id)
                rk_pipe.hmset(kern_id, *dict2kvlist(stats[kern_id]))
            # Create a "shadow" key that actually expires.
            # This allows access to agent information upon expiration events.
            ri_pipe.set('shadow:' + inst_id, '')
            ri_pipe.expire('shadow:' + inst_id, float(interval * 2))
            try:
                await ri_pipe.execute()
                await rk_pipe.execute()
            except asyncio.CancelledError:
                pass
            except:
                log.exception('Failed to finish heartbeat updates.')
            ri.close()
            rk.close()
    except asyncio.TimeoutError:
        log.warn('heartbeat failed.')


async def heartbeat_timer(loop, docker_cli, interval=6.0):
    '''
    Record my status information to the manager database (Redis).
    This information automatically expires after 2x interval, so that failure
    of executing this method automatically removes the instance from the
    manager database.
    '''
    global inst_id, inst_type, agent_ip, redis_addr
    if not inst_id:
        inst_id = await utils.get_instance_id()
    if not inst_type:
        inst_type = await utils.get_instance_type()
    if not agent_ip:
        agent_ip = await utils.get_instance_ip()
    log.info('myself: {} ({}), ip: {}'.format(inst_id, inst_type, agent_ip))
    log.info('using manager redis at tcp://{0}:{1}'.format(*redis_addr))
    try:
        while True:
            asyncio.ensure_future(_heartbeat(loop, docker_cli, interval))
            await asyncio.sleep(interval, loop=loop)
    except asyncio.CancelledError:
        pass


VolumeInfo = namedtuple('VolumeInfo', 'name container_path mode')
_extra_volumes = {
    'python3-tensorflow': [
        VolumeInfo('deeplearning-samples', '/home/work/samples', 'ro'),
    ],
    'python3-tensorflow-gpu': [
        VolumeInfo('deeplearning-samples', '/home/work/samples', 'ro'),
    ],
}


def get_extra_volumes(docker_cli, lang):
    avail_volumes = docker_cli.volumes()['Volumes']
    if not avail_volumes:
        return []
    volume_names = set(v['Name'] for v in avail_volumes)
    volume_list = _extra_volumes.get(lang, [])
    mount_list = []
    for vol in volume_list:
        if vol.name in volume_names:
            mount_list.append(vol)
        else:
            log.warning(_f('could not attach volume {} to '
                           'a kernel using language {} (volume not found)',
                           vol.name, lang))
    return mount_list


async def create_kernel(loop, docker_cli, lang, kernel_id=None):
    global container_registry, container_cpu_map
    if not kernel_id:
        kernel_id = generate_uuid()
        assert kernel_id not in container_registry
    work_dir = os.path.join(volume_root, kernel_id)
    os.makedirs(work_dir)
    if docker_version >= LooseVersion('1.10'):
        # We already have our own jail!
        security_opt = ['seccomp:unconfined']
    else:
        security_opt = ['apparmor:docker-ptrace'] if os.path.exists(apparmor_profile_path) else []
    mount_list = get_extra_volumes(docker_cli, lang)
    binds = {work_dir: {'bind': '/home/work', 'mode': 'rw'}}
    binds.update({v.name: {'bind': v.container_path, 'mode': v.mode} for v in mount_list})
    volumes = ['/home/work']
    volumes.extend(v.container_path for v in mount_list)
    devices = []
    image_name = 'lablup/kernel-{}'.format(lang)
    ret = await call_docker_with_retries(
        lambda: docker_cli.inspect_image(image_name),
        lambda: log.critical(_f('could not query image info for {} (timeout)', image_name)),
        lambda err: log.critical(_f('could not query image info for {} ({!r})', image_name, err))
    )
    if isinstance(ret, Exception):
        raise RuntimeError('docker.inspect_image failed') from ret
    mem_limit       = ret['ContainerConfig']['Labels'].get('io.sorna.maxmem', '128m')
    exec_timeout    = int(ret['ContainerConfig']['Labels'].get('io.sorna.timeout', '10'))
    requested_cores = int(ret['ContainerConfig']['Labels'].get('io.sorna.maxcores', '1'))
    num_cores       = min(container_cpu_map.num_cores, requested_cores)
    numa_node, core_set = container_cpu_map.alloc(num_cores)
    log.info('container config: mem_limit={}, exec_timeout={}, cores={!r}@{}'
             .format(mem_limit, exec_timeout, core_set, numa_node))

    if 'yes' == ret['ContainerConfig']['Labels'].get('io.sorna.nvidia.enabled', 'no'):
        extra_binds, extra_devices = prepare_nvidia(docker_cli, numa_node)
        binds.update(extra_binds)
        devices.extend(extra_devices)

    host_config = docker_cli.create_host_config(
        cpuset_cpus=','.join(map(str, sorted(core_set))),
        mem_limit=mem_limit,
        memswap_limit=0,
        security_opt=security_opt,
        port_bindings={
            2001: ('0.0.0.0', ),
            2002: ('0.0.0.0', ),
            2003: ('0.0.0.0', ),
        },
        devices=devices,
        binds=binds,
    )
    # docker-py is missing support this in the create_container API.
    # (only in update_contaienr API...)
    host_config['CpusetMems'] = '{}'.format(numa_node)

    ret = await call_docker_with_retries(
        lambda: docker_cli.create_container(
            image_name,
            name='kernel.{}.{}'.format(lang, kernel_id),
            cpu_shares=1024,  # full share
            ports=[
                (2001, 'tcp'),
                (2002, 'tcp'),
                (2003, 'tcp'),
            ],
            volumes=volumes,
            host_config=host_config,
            tty=True
        ),
        lambda: log.critical(_f('could not create container for kernel {} (timeout)', kernel_id)),
        lambda err: log.critical(_f('could not create container for kernel {} ({!r})', kernel_id, err))
    )
    if isinstance(ret, Exception):
        raise RuntimeError('docker.create_container failed') from ret
    container_id = ret['Id']
    ret = await call_docker_with_retries(
        lambda: docker_cli.start(container_id),
        lambda: log.critical(_f('could not start container {} for kernel {} (timeout)', container_id, kernel_id)),
        lambda err: log.critical(_f('could not start container {} for kernel {} ({!r})', container_id, kernel_id, err)),
    )
    if isinstance(ret, Exception):
        raise RuntimeError('docker.start failed') from ret
    container_info = docker_cli.inspect_container(container_id)
    # We can connect to the container either using
    # tcp://<NetworkSettings.IPAddress>:2001 (direct)
    # or tcp://127.0.0.1:<NetworkSettings.Ports.2011/tcp.0.HostPort> (NAT'ed)
    if docker_ip:
        kernel_ip = docker_ip
    else:
        # kernel_ip = container_info['NetworkSettings']['IPAddress']
        kernel_ip = '127.0.0.1'
    host_side_port = container_info['NetworkSettings']['Ports']['2001/tcp'][0]['HostPort']
    stdin_port  = container_info['NetworkSettings']['Ports']['2002/tcp'][0]['HostPort']
    stdout_port = container_info['NetworkSettings']['Ports']['2003/tcp'][0]['HostPort']
    container_registry[kernel_id] = {
        'lang': lang,
        'container_id': container_id,
        'addr': 'tcp://{0}:{1}'.format(kernel_ip, host_side_port),
        'ip': kernel_ip,
        'port': 2001,
        'host_port': host_side_port,
        'stdin_port': stdin_port,
        'stdout_port': stdout_port,
        'cpu_shares': 1024,
        'numa_node': numa_node,
        'core_set': core_set,
        'mem_limit': mem_limit,
        'exec_timeout': exec_timeout,
        'num_queries': 0,
        'last_used': time.monotonic(),
    }
    log.debug('kernel access address: {0}:{1}'.format(kernel_ip, host_side_port))
    log.debug('kernel stdin address: {0}:{1}'.format(kernel_ip, stdin_port))
    log.debug('kernel stdout address: {0}:{1}'.format(kernel_ip, stdout_port))
    return kernel_id


async def destroy_kernel(loop, docker_cli, kernel_id, keep_registry=False):
    global container_registry
    global inst_id
    if not inst_id:
        inst_id = await utils.get_instance_id()
    container_id = container_registry[kernel_id]['container_id']
    await call_docker_with_retries(
        lambda: docker_cli.kill(container_id),
        lambda: log.warning(_f('could not kill container {} used by kernel {} (timeout)', container_id, kernel_id)),
        lambda err: log.warning(_f('could not kill container {} used by kernel {} ({!r})', container_id, kernel_id, err)),
    )
    await call_docker_with_retries(
        lambda: docker_cli.remove_container(container_id),
        lambda: log.warning(_f('could not remove container {} used by kernel {} (timeout)', container_id, kernel_id)),
        lambda err: log.warning(_f('could not remove container {} used by kernel {} ({!r})', container_id, kernel_id, err)),
    )
    # We ignore returned exceptions above, because anyway we should proceed to clean up other things.
    work_dir = os.path.join(volume_root, kernel_id)
    shutil.rmtree(work_dir)
    container_cpu_map.free(container_registry[kernel_id]['core_set'])
    if not keep_registry:
        del container_registry[kernel_id]
        try:
            with timeout(1.5):
                redis = await aioredis.create_redis(redis_addr, encoding='utf8', loop=loop)
                redis.select(defs.SORNA_INSTANCE_DB)
                pipe = redis.pipeline()
                pipe.hincrby(inst_id, 'num_kernels', -1)
                pipe.srem(inst_id + '.kernels', kernel_id)
                await pipe.execute()
                redis.select(defs.SORNA_KERNEL_DB)
                redis.delete(kernel_id)
                redis.close()
        except asyncio.TimeoutError:
            log.warn('failed to update registry after kernel destruction.')


def scandir(root):
    file_stats = dict()
    for entry in os.scandir(root):
        # Skip hidden files.
        if entry.name.startswith('.'):
            continue
        if entry.is_file():
            stat = entry.stat()
            # Skip too large files!
            if stat.st_size > max_upload_size:
                continue
            file_stats[entry.path] = stat.st_mtime
        elif entry.is_dir():
            try:
                file_stats.update(scandir(entry.path))
            except PermissionError:
                pass
    return file_stats


def diff_file_stats(fs1, fs2):
    k2 = set(fs2.keys())
    k1 = set(fs1.keys())
    new_files = k2 - k1
    modified_files = set()
    for k in (k2 - new_files):
        if fs1[k] < fs2[k]:
            modified_files.add(k)
    return new_files | modified_files


def prepare_nvidia(docker_cli, numa_node):
    r = requests.get('http://localhost:3476/docker/cli/json')
    nvidia_params = r.json()
    r = requests.get('http://localhost:3476/gpu/info/json')
    gpu_info = r.json()
    existing_volumes = set(vol['Name'] for vol in docker_cli.volumes()['Volumes'])
    required_volumes = set(vol.split(':')[0] for vol in nvidia_params['Volumes'])
    missing_volumes = required_volumes - existing_volumes
    binds = {}
    for vol_name in missing_volumes:
        for vol_param in nvidia_params['Volumes']:
            if vol_param.startswith(vol_name + ':'):
                _, _, permission = vol_param.split(':')
                driver = nvidia_params['VolumeDriver']
                docker_cli.create_volume(name=vol_name, driver=driver)
    for vol_name in required_volumes:
        for vol_param in nvidia_params['Volumes']:
            if vol_param.startswith(vol_name + ':'):
                _, mount_pt, permission = vol_param.split(':')
                binds[vol_name] = {
                    'bind': mount_pt,
                    'mode': permission,
                }
    devices = []
    for dev in nvidia_params['Devices']:
        if re.search(r'^/dev/nvidia\d+$', dev) is None:
            devices.append(dev)
        else:
            # Only expose GPUs in the same NUMA node.
            for gpu in gpu_info['Devices']:
                if gpu['Path'] == dev:
                    pci_path = '/sys/bus/pci/devices/{}/numa_node'.format(gpu['PCI']['BusID'])
                    gpu_node = int(Path(pci_path).read_text().strip())
                    if gpu_node == numa_node:
                        devices.append(dev)
    devices = ['{0}:{0}:rwm'.format(dev) for dev in devices]
    return binds, devices


async def execute_code(loop, docker_cli, entry_id, kernel_id, cell_id, code):
    work_dir = os.path.join(volume_root, kernel_id)
    # TODO: import "connected" files from S3
    initial_file_stats = scandir(work_dir)

    container_addr = container_registry[kernel_id]['addr']
    container_sock = await aiozmq.create_zmq_stream(
        zmq.REQ, connect=container_addr, loop=loop)
    container_sock.transport.setsockopt(zmq.LINGER, 50)
    container_sock.write([cell_id.encode('ascii'), code.encode('utf8')])
    exec_timeout = container_registry[kernel_id]['exec_timeout']

    # Execute with a 4 second timeout.
    try:
        begin_time = time.monotonic()
        result_data = await asyncio.wait_for(container_sock.read(),
                                             timeout=exec_timeout,
                                             loop=loop)
        finish_time = time.monotonic()
        log.info(_f('execution time: {:.2f} / {} sec', finish_time - begin_time, exec_timeout))
        result = json.loads(result_data[0])
        diff_files = []
        if nmget(result, 'options.upload_output_files', True):
            final_file_stats = scandir(work_dir)
            diff_files = diff_file_stats(initial_file_stats, final_file_stats)
            diff_files = [os.path.relpath(fn, work_dir) for fn in diff_files]
            if diff_files:
                session = aiobotocore.get_session(loop=loop)
                client = session.create_client('s3', region_name=s3_region,
                                               aws_secret_access_key=s3_secret_key,
                                               aws_access_key_id=s3_access_key)
                for fname in diff_files:
                    key = 'bucket/{}/{}'.format(entry_id, fname)
                    # TODO: put the file chunk-by-chunk.
                    with open(os.path.join(work_dir, fname), 'rb') as f:
                        content = f.read()
                    try:
                        await client.put_object(Bucket=s3_bucket,
                                                Key=key,
                                                Body=content,
                                                ACL='public-read')
                    except botocore.exceptions.ClientError as exc:
                        log.exception('S3 upload error')
                client.close()
        return odict(
            ('stdout', result['stdout']),
            ('stderr', result['stderr']),
            ('media', nmget(result, 'media', [])),
            ('options', nmget(result, 'options', None)),
            ('exceptions', nmget(result, 'exceptions', [])),
            ('files', diff_files),
        )
    except asyncio.TimeoutError as exc:
        log.warning('Timeout detected on kernel {} (cell_id: {}).'
                    .format(kernel_id, cell_id))
        await destroy_kernel(loop, docker_cli, kernel_id)
        raise exc  # re-raise so that the loop return failure.
    finally:
        container_sock.close()


async def cleanup_timer(loop, docker_cli):
    try:
        while True:
            now = time.monotonic()
            # clone keys to avoid "dictionary size changed during iteration" error.
            keys = tuple(container_registry.keys())
            for kern_id in keys:
                try:
                    if now - container_registry[kern_id]['last_used'] > idle_timeout:
                        log.info('destroying kernel {} as clean-up'.format(kern_id))
                        await destroy_kernel(loop, docker_cli, kern_id)
                except KeyError:
                    # The kernel may be destroyed by other means?
                    # TODO: check this situation more thoroughly.
                    pass
            await asyncio.sleep(10, loop=loop)
    except asyncio.CancelledError:
        pass


async def clean_all_kernels(loop, docker_cli):
    log.info('cleaning all kernels...')
    kern_ids = tuple(container_registry.keys())
    for kern_id in kern_ids:
        await destroy_kernel(loop, docker_cli, kern_id)


def match_result(result, match):
    try:
        op = match['op']
        target = match['target']
        value = match['value']
    except KeyError:
        raise TypeError('Wrong match object format.')
    assert op in ('contains', 'equal', 'regex'), 'Invalid match operator.'
    assert target in ('stdout', 'stderr', 'exception'), 'Invalid match target.'
    assert isinstance(value, str), 'Match value must be a string.'
    if target in ('stdout', 'stderr'):
        content = result[target]
    elif target == 'exception':
        if len(result['exceptions']) > 0:
            content = result['exceptions'][-1][0]  # exception name
        else:
            # Expected exception, but there was none.
            return False
    if op == 'contains':
        matched = (value in content)
    elif op == 'equal':
        matched = (value == content)
    elif op == 'regex':
        matched = (re.search(value, content) is not None)
    return matched


def format_pyexc(e):
    if e.args:
        return '{0}: {1}'.format(type(e).__name__, ', '.join(map(str, e.args)))
    else:
        return type(e).__name__


async def run_agent(loop, docker_cli, server_sock):
    global container_registry
    global inst_id, agent_ip, inst_type, redis_addr

    if not inst_id:
        inst_id = await utils.get_instance_id()
    if not inst_type:
        inst_type = await utils.get_instance_type()
    if not agent_ip:
        agent_ip = await utils.get_instance_ip()

    # Initialize docker subsystem
    container_registry.clear()

    # Send the first heartbeat.
    hb_task    = asyncio.ensure_future(heartbeat_timer(loop, docker_cli), loop=loop)
    timer_task = asyncio.ensure_future(cleanup_timer(loop, docker_cli), loop=loop)
    await asyncio.sleep(0, loop=loop)

    # Then start running the agent loop.
    while True:
        try:
            request_data = await server_sock.read()
        except aiozmq.stream.ZmqStreamClosed:
            hb_task.cancel()
            timer_task.cancel()
            break
        request = Message.decode(request_data[0])
        resp = Message()

        if request['action'] == AgentRequestTypes.PING:

            resp['reply'] = SornaResponseTypes.PONG
            resp['body'] = request['body']

        elif request['action'] == AgentRequestTypes.CREATE_KERNEL:

            log.info('CREATE_KERNEL ({})'.format(request['lang']))
            if request['lang'] in lang_aliases:
                try:
                    lang = lang_aliases[request['lang']]
                    kernel_id = await create_kernel(loop, docker_cli, lang)
                    # TODO: (asynchronously) check if container is running okay.
                except Exception as exc:
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['cause'] = format_pyexc(exc)
                    log.exception('CREATE_KERNEL')
                else:
                    resp['reply'] = SornaResponseTypes.SUCCESS
                    resp['kernel_id'] = kernel_id
                    resp['stdin_port'] = container_registry[kernel_id]['stdin_port']
                    resp['stdout_port'] = container_registry[kernel_id]['stdout_port']
            else:
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['cause'] = 'Unsupported kernel language.'

        elif request['action'] == AgentRequestTypes.DESTROY_KERNEL:

            log.info('DESTROY_KERNEL ({})'.format(request['kernel_id']))
            if request['kernel_id'] in container_registry:
                try:
                    await destroy_kernel(loop, docker_cli, request['kernel_id'])
                except Exception as exc:
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['cause'] = format_pyexc(exc)
                    log.exception('DESTROY_KERNEL')
                else:
                    resp['reply'] = SornaResponseTypes.SUCCESS
            else:
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['cause'] = 'No such kernel.'

        elif request['action'] == AgentRequestTypes.RESTART_KERNEL:

            log.info('RESTART_KERNEL ({})'.format(request['kernel_id']))
            if request['kernel_id'] in container_registry:
                try:
                    kernel_id = request['kernel_id']
                    await destroy_kernel(loop, docker_cli, kernel_id, keep_registry=True)
                    lang = container_registry[kernel_id]['lang']
                    await create_kernel(loop, docker_cli, lang, kernel_id)
                except Exception as exc:
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['cause'] = format_pyexc(exc)
                    log.exception('RESTART_KERNEL')
                else:
                    resp['reply'] = SornaResponseTypes.SUCCESS
                    resp['stdin_port'] = container_registry[kernel_id]['stdin_port']
                    resp['stdout_port'] = container_registry[kernel_id]['stdout_port']
            else:
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['cause'] = 'No such kernel.'

        elif request['action'] == AgentRequestTypes.EXECUTE:

            log.info('EXECUTE (k:{}, c:{})'.format(request['kernel_id'],
                                                   request['cell_id']))
            if request['kernel_id'] in container_registry:
                try:
                    container_registry[request['kernel_id']]['last_used'] \
                        = time.monotonic()
                    container_registry[request['kernel_id']]['num_queries'] += 1
                    result = await execute_code(loop, docker_cli,
                                                request['entry_id'],
                                                request['kernel_id'],
                                                request['cell_id'],
                                                request['code'])
                    if 'match' in request:
                        resp['match_result'] = match_result(result, request['match'])
                except Exception as exc:
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['cause'] = format_pyexc(exc)
                    log.exception('EXECUTE')
                else:
                    resp['reply'] = SornaResponseTypes.SUCCESS
                    resp['result'] = result
            else:
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['cause'] = 'Could not find such kernel.'
        else:
            resp['reply'] = SornaResponseTypes.INVALID_INPUT
            resp['cause'] = 'Invalid request.'

        server_sock.write([resp.encode()])
        await server_sock.drain()


def handle_signal(loop, term_ev):
    if term_ev.is_set():
        log.warning('Forced shutdown!')
        sys.exit(1)
    else:
        term_ev.set()
        loop.stop()


def main():
    global max_kernels
    global agent_addr, agent_ip, agent_port
    global redis_addr
    global lang_aliases
    global volume_root
    global container_cpu_map

    argparser = argparse.ArgumentParser()
    argparser.add_argument('--agent-ip-override', type=ipaddr, default=None, dest='agent_ip',
                           help='Manually set the IP address of this agent to report to the manager.')
    argparser.add_argument('--agent-port', type=port_no, default=6001,
                           help='The port number to listen on.')
    argparser.add_argument('--redis-addr', type=host_port_pair, default=('localhost', 6379),
                           help='The host:port pair of the Redis (agent registry) server.')
    argparser.add_argument('--max-kernels', type=positive_int, default=1,
                           help='Set the maximum number of kernels running in parallel.')
    argparser.add_argument('--debug', action='store_true', default=False,
                           help='Enable more verbose logging.')
    argparser.add_argument('--kernel-aliases', type=str, default=None,
                           help='The filename for additional kernel aliases')
    argparser.add_argument('--volume-root', type=str, default='/var/lib/sorna-volumes',
                           help='The scratch directory to store container working directories.')
    args = argparser.parse_args()

    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'colored': {
                '()': 'coloredlogs.ColoredFormatter',
                'format': '%(asctime)s %(levelname)s %(name)s %(message)s',
                'field_styles': {'levelname': {'color': 'black', 'bold': True},
                                 'name': {'color': 'black', 'bold': True},
                                 'asctime': {'color': 'black'}},
                'level_styles': {'info': {'color': 'cyan'},
                                 'debug': {'color': 'green'},
                                 'warning': {'color': 'yellow'},
                                 'error': {'color': 'red'},
                                 'critical': {'color': 'red', 'bold': True}},
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'formatter': 'colored',
                'stream': 'ext://sys.stdout',
            },
            'null': {
                'class': 'logging.NullHandler',
            },
        },
        'loggers': {
            '': {
                'handlers': ['console'],
                'level': 'DEBUG' if args.debug else 'INFO',
            },
            'aioredis': {
                'handlers': ['console'],
                'level': 'INFO',
            },
        },
    })
    if args.debug:
        log.debug('debug mode enabled.')

    if args.agent_ip:
        agent_ip = str(args.agent_ip)
    agent_addr = 'tcp://*:{1}'.format('*', args.agent_port)
    agent_port = args.agent_port
    max_kernels = args.max_kernels
    redis_addr = args.redis_addr if args.redis_addr else ('sorna-manager.lablup', 6379)

    assert os.path.isdir(args.volume_root)
    volume_root = args.volume_root

    container_cpu_map = CPUAllocMap()

    # Load language aliases config.
    lang_aliases = {lang: lang for lang in supported_langs}
    lang_aliases.update({
        'python': 'python3',
        'python26': 'python2',
        'python27': 'python2',
        'python34': 'python3',
        'python35': 'python3',
        'python3-deeplearning':   'python3-tensorflow',      # temporary alias
        'tensorflow-python3':     'python3-tensorflow',      # package-oriented alias
        'tensorflow-gpu-python3': 'python3-tensorflow-gpu',  # package-oriented alias
        'caffe-python3':          'python3-caffe',           # package-oriented alias
        'r': 'r3',
        'R': 'r3',
        'Rscript': 'r3',
        'php': 'php7',
        'node': 'nodejs4',
        'nodejs': 'nodejs4',
        'javascript': 'nodejs4',
        'lua': 'lua5',
    })
    if args.kernel_aliases:  # for when we want to add extra
        with open(args.kernel_aliases, 'r') as f:
            for line in f:
                alias, target = line.strip().split()
                assert target in supported_langs
                lang_aliases[alias] = target

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    log.info('Sorna Agent {}'.format(__version__))

    server_sock = loop.run_until_complete(aiozmq.create_zmq_stream(zmq.REP, bind=agent_addr, loop=loop))
    server_sock.transport.setsockopt(zmq.LINGER, 50)

    term_ev = asyncio.Event()
    loop.add_signal_handler(signal.SIGTERM, handle_signal, loop, term_ev)
    loop.add_signal_handler(signal.SIGINT, handle_signal, loop, term_ev)
    docker_cli = docker_init()
    asyncio.ensure_future(run_agent(loop, docker_cli, server_sock), loop=loop)
    try:
        log.info('serving at {0}'.format(agent_addr))
        loop.run_forever()
        # interrupted
        loop.run_until_complete(clean_all_kernels(loop, docker_cli))
        server_sock.close()
        loop.run_until_complete(asyncio.sleep(0.1))
    finally:
        loop.close()
        log.info('exit.')


if __name__ == '__main__':
    main()
