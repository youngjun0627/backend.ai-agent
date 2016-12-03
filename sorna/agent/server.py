import argparse
import asyncio
import logging, logging.config
import os, os.path
from pathlib import Path
import re
import signal
import shutil
import sys
import time

import zmq, aiozmq, aiozmq.rpc
import aioredis
from aiodocker.docker import Docker
from async_timeout import timeout
from namedlist import namedtuple
import simplejson as json
import uvloop

from sorna import utils, defs
from sorna.argparse import ipaddr, port_no, host_port_pair, positive_int
from sorna.utils import odict, generate_uuid, nmget, readable_size_to_bytes
from . import __version__
from .files import scandir, upload_output_files_to_s3
from .gpu import prepare_nvidia
from .stats import collect_stats
from .resources import CPUAllocMap

log = logging.getLogger('sorna.agent.server')

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
max_upload_size = 5 * 1024 * 1024  # 5 MB

# Shortcut for str.format
_f = lambda fmt, *args, **kwargs: fmt.format(*args, **kwargs)

VolumeInfo = namedtuple('VolumeInfo', 'name container_path mode')
_extra_volumes = {
    'python3-tensorflow': [
        VolumeInfo('deeplearning-samples', '/home/work/samples', 'ro'),
    ],
    'python3-tensorflow-gpu': [
        VolumeInfo('deeplearning-samples', '/home/work/samples', 'ro'),
    ],
}


async def get_extra_volumes(docker, lang):
    avail_volumes = (await docker.volumes.list())['Volumes']
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


async def heartbeat_timer(agent, interval=6.0):
    '''
    Record my status information to the manager database (Redis).
    This information automatically expires after 2x interval, so that failure
    of executing this method automatically removes the instance from the
    manager database.
    '''
    try:
        while True:
            asyncio.ensure_future(agent.heartbeat(interval))
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass


async def cleanup_timer(agent):
    try:
        while True:
            asyncio.ensure_future(agent.clean_old_kernels())
            await asyncio.sleep(10)
    except asyncio.CancelledError:
        pass


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


class AgentRPCServer(aiozmq.rpc.AttrHandler):

    def __init__(self, docker, config, loop=None):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.docker = docker
        self.config = config
        self.lifecycle_lock = asyncio.Lock(loop=loop)
        self.container_registry = {}
        self.container_cpu_map = CPUAllocMap()
        self.redis_inst = None
        self.redis_kern = None
        self.first_heartbeat = True

    async def init(self):
        self.redis_inst = await aioredis.create_pool(self.config.redis_addr,
                                                     encoding='utf8',
                                                     db=defs.SORNA_INSTANCE_DB,
                                                     loop=self.loop)
        self.redis_kern = await aioredis.create_pool(self.config.redis_addr,
                                                     encoding='utf8',
                                                     db=defs.SORNA_KERNEL_DB,
                                                     loop=self.loop)

    async def shutdown(self):
        self.redis_kern.close()
        self.redis_inst.close()
        await self.redis_kern.wait_closed()
        await self.redis_inst.wait_closed()

    @aiozmq.rpc.method
    def ping(self, msg: str) -> str:
        return msg

    @aiozmq.rpc.method
    async def create_kernel(self, lang: str, opts: dict) -> tuple:
        if lang in lang_aliases:
            lang = lang_aliases[lang]
        log.debug('rpc::create_kernel({})'.format(lang))
        kernel_id = await self._create_kernel(lang)
        stdin_port = self.container_registry[kernel_id]['stdin_port']
        stdout_port = self.container_registry[kernel_id]['stdout_port']
        return kernel_id, stdin_port, stdout_port

    @aiozmq.rpc.method
    async def destroy_kernel(self, kernel_id: str):
        log.debug('rpc::destroy_kernel({})'.format(kernel_id))
        await self._destroy_kernel(kernel_id)

    @aiozmq.rpc.method
    async def restart_kernel(self, kernel_id: str):
        log.debug('rpc::restart_kernel({})'.format(kernel_id))
        await self._destroy_kernel(kernel_id, keep_registry=True)
        lang = self.container_registry[kernel_id]['lang']
        await self._create_kernel(lang, kernel_id=kernel_id)

    @aiozmq.rpc.method
    async def execute_code(self, entry_id: str, kernel_id: str,
                           code_id: str, code: str,
                           match: dict) -> dict:
        log.debug('rpc::execute_code({}, {}, ...)'.format(entry_id, kernel_id))
        result = await self._execute_code(entry_id, kernel_id, code_id, code)
        if match:
            result['match_result'] = match_result(result, match)
        return result

    async def _create_kernel(self, lang, kernel_id=None):
        if not kernel_id:
            kernel_id = generate_uuid()
            assert kernel_id not in self.container_registry

        async with self.lifecycle_lock, self.redis_inst.get() as ri:
            await ri.hincrby(self.config.inst_id, 'num_kernels', 1)

        work_dir = os.path.join(self.config.volume_root, kernel_id)
        os.makedirs(work_dir)

        image_name = 'lablup/kernel-{}'.format(lang)
        ret = await self.docker.images.get(image_name)
        mem_limit       = ret['ContainerConfig']['Labels'].get('io.sorna.maxmem', '128m')
        container_exec_timeout = int(ret['ContainerConfig']['Labels'].get('io.sorna.timeout', '10'))
        exec_timeout    = min(container_exec_timeout, self.config.exec_timeout)
        requested_cores = int(ret['ContainerConfig']['Labels'].get('io.sorna.maxcores', '1'))
        num_cores       = min(self.container_cpu_map.num_cores, requested_cores)
        numa_node, core_set = self.container_cpu_map.alloc(num_cores)
        log.debug('container config: mem_limit={}, exec_timeout={}, cores={!r}@{}'
                  .format(mem_limit, exec_timeout, core_set, numa_node))

        mount_list = await get_extra_volumes(self.docker, lang)
        binds = ['{}:/home/work:rw'.format(work_dir)]
        binds.extend('{}:{}:{}'.format(v.name, v.container_path, v.mode) for v in mount_list)
        volumes = ['/home/work']
        volumes.extend(v.container_path for v in mount_list)
        devices = []

        if 'yes' == ret['ContainerConfig']['Labels'].get('io.sorna.nvidia.enabled', 'no'):
            extra_binds, extra_devices = await prepare_nvidia(self.docker, numa_node)
            binds.extends(extra_binds)
            devices.extend(extra_devices)

        config = {
            'Image': image_name,
            'Tty': True,
            'Volumes': {v: {} for v in volumes},
            'StopSignal': 'SIGINT',
            'ExposedPorts': {
                '2001/tcp': {},
                '2002/tcp': {},
                '2003/tcp': {},
            },
            'HostConfig': {
                'MemorySwap': 0,
                'Memory': readable_size_to_bytes(mem_limit),
                'CpusetCpus': ','.join(map(str, sorted(core_set))),
                'CpusetMems': '{}'.format(numa_node),
                'SecurityOpt': ['seccomp:unconfined'],
                'Binds': binds,
                'Devices': devices,
                'PublishAllPorts': True,
            },
        }
        kernel_name = 'kernel.{}.{}'.format(lang, kernel_id)
        container = await self.docker.containers.create(config=config, name=kernel_name)
        await container.start()
        repl_port   = (await container.port(2001))[0]['HostPort']
        stdin_port  = (await container.port(2002))[0]['HostPort']
        stdout_port = (await container.port(2003))[0]['HostPort']
        kernel_ip = '127.0.0.1'

        self.container_registry[kernel_id] = {
            'lang': lang,
            'container_id': container._id,
            'addr': 'tcp://{0}:{1}'.format(kernel_ip, repl_port),
            'ip': kernel_ip,
            'port': 2001,
            'host_port': repl_port,
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
        log.debug('kernel access address: {0}:{1}'.format('0.0.0.0', repl_port))
        log.debug('kernel stdin address: {0}:{1}'.format('0.0.0.0', stdin_port))
        log.debug('kernel stdout address: {0}:{1}'.format('0.0.0.0', stdout_port))
        return kernel_id

    async def _destroy_kernel(self, kernel_id, keep_registry=False):
        if not keep_registry:
            try:
                async with self.lifecycle_lock, \
                           self.redis_inst.get() as ri, \
                           self.redis_kern.get() as rk:
                    with timeout(1.5):
                        ri.hincrby(self.config.inst_id, 'num_kernels', -1)
                        ri.srem(self.config.inst_id + '.kernels', kernel_id)
                        rk.delete(kernel_id)
            except asyncio.TimeoutError:
                log.warn('failed to update registry after kernel destruction.')

        cid = self.container_registry[kernel_id]['container_id']
        container = self.docker.containers.container(cid)
        try:
            await container.kill()
            await container.delete()
        finally:
            work_dir = os.path.join(self.config.volume_root, kernel_id)
            shutil.rmtree(work_dir)
            self.container_cpu_map.free(self.container_registry[kernel_id]['core_set'])
            del self.container_registry[kernel_id]

    async def _execute_code(self, entry_id, kernel_id, code_id, code):
        work_dir = os.path.join(self.config.volume_root, kernel_id)

        self.container_registry[kernel_id]['last_used'] = time.monotonic()
        self.container_registry[kernel_id]['num_queries'] += 1

        container_addr = self.container_registry[kernel_id]['addr']
        container_sock = await aiozmq.create_zmq_stream(
            zmq.REQ, connect=container_addr, loop=self.loop)
        container_sock.transport.setsockopt(zmq.LINGER, 50)
        container_sock.write([code_id.encode('ascii'), code.encode('utf8')])
        exec_timeout = self.container_registry[kernel_id]['exec_timeout']

        try:
            # TODO: import "connected" files from S3
            initial_file_stats = scandir(work_dir, max_upload_size)

            begin_time = time.monotonic()
            with timeout(exec_timeout):
                result_data = await container_sock.read()
            finish_time = time.monotonic()
            log.debug(_f('execution time: {:.2f} / {} sec',
                         finish_time - begin_time, exec_timeout))

            final_file_stats = scandir(work_dir, max_upload_size)
            result = json.loads(result_data[0])
            uploaded_files = []
            if nmget(result, 'options.upload_output_files', True):
                uploaded_files = await upload_output_files_to_s3(
                    initial_file_stats, final_file_stats, entry_id,
                    loop=self.loop)
                uploaded_files = [os.path.relpath(fn, work_dir) for fn in uploaded_files]

            return odict(
                ('stdout', result['stdout']),
                ('stderr', result['stderr']),
                ('media', nmget(result, 'media', [])),
                ('options', nmget(result, 'options', None)),
                ('exceptions', nmget(result, 'exceptions', [])),
                ('files', uploaded_files),
            )
        except asyncio.TimeoutError as exc:
            log.warning('Timeout detected on kernel {} (code_id: {}).'
                        .format(kernel_id, code_id))
            asyncio.ensure_future(self._destroy_kernel(kernel_id))
            raise
        finally:
            container_sock.close()

    async def heartbeat(self, interval):
        running_kernels = [k for k in self.container_registry.keys()]
        running_containers = [self.container_registry[k]['container_id']
                              for k in running_kernels]
        stats = await collect_stats(map(self.docker.containers.container, running_containers))
        try:
            async with self.lifecycle_lock, \
                       self.redis_inst.get() as ri, \
                       self.redis_kern.get() as rk:
                # Attach limits to collected stats.
                # Here, there may be destroyed kernels due to coroutine interleaving.
                for idx, stat in enumerate(stats):
                    kern_id = running_kernels[idx]
                    if kern_id not in self.container_registry:
                        stats[idx] = None
                        continue
                    stats[idx]['exec_timeout'] = self.container_registry[kern_id]['exec_timeout']
                    stats[idx]['idle_timeout'] = self.config.idle_timeout
                    mem_limit = self.container_registry[kern_id]['mem_limit']
                    mem_limit_in_kb = utils.readable_size_to_bytes(mem_limit) // 1024
                    stats[idx]['mem_limit']   = mem_limit_in_kb
                    stats[idx]['num_queries'] = self.container_registry[kern_id]['num_queries']
                    last_used = self.container_registry[kern_id]['last_used']
                    stats[idx]['idle'] = (time.monotonic() - last_used) * 1000
                with timeout(interval / 2):
                    # should match with sorna.manager.structs.Instance
                    my_kernels_key = '{}.kernels'.format(self.config.inst_id)
                    ri_pipe, rk_pipe = ri.pipeline(), rk.pipeline()
                    valid_count = 0
                    if self.first_heartbeat:
                        # if restarted, clean up the running kernels set.
                        ri.delete(my_kernels_key)
                        self.first_heartbeat = False
                    for idx, kern_id in enumerate(running_kernels):
                        if stats[idx]:
                            ri_pipe.sadd(my_kernels_key, kern_id)
                            rk_pipe.hmset(kern_id, *utils.dict2kvlist(stats[idx]))
                            valid_count += 1
                    ri_pipe.hmset(self.config.inst_id, *utils.dict2kvlist({
                        'status': 'ok',
                        'id': self.config.inst_id,
                        'ip': self.config.agent_ip,
                        'addr': 'tcp://{}:{}'.format(self.config.agent_ip, self.config.agent_port),
                        'type': self.config.inst_type,
                        'num_kernels': valid_count,
                        'max_kernels': self.config.max_kernels,
                    }))
                    # Create a "shadow" key that actually expires.
                    # This allows access to agent information upon expiration events.
                    ri_pipe.set('shadow:' + self.config.inst_id, '')
                    ri_pipe.expire('shadow:' + self.config.inst_id, float(interval * 2))
                    try:
                        await ri_pipe.execute()
                        await rk_pipe.execute()
                    except asyncio.CancelledError:
                        pass
                    except:
                        log.exception('heartbeat error')
                    ri.close()
                    rk.close()
        except asyncio.TimeoutError:
            log.warn('heartbeat timeout')

    async def clean_old_kernels(self):
        now = time.monotonic()
        keys = tuple(self.container_registry.keys())
        tasks = []
        for kern_id in keys:
            try:
                last_used = self.container_registry[kern_id]['last_used']
                if now - last_used > self.config.idle_timeout:
                    log.info('destroying kernel {} as clean-up'.format(kern_id))
                    task = asyncio.ensure_future(self._destroy_kernel(kern_id))
                    tasks.append(task)
            except KeyError:
                # The kernel may be destroyed by other means?
                pass
        await asyncio.gather(*tasks)

    async def clean_all_kernels(self):
        log.info('cleaning all kernels...')
        kern_ids = tuple(self.container_registry.keys())
        tasks = []
        for kern_id in kern_ids:
            try:
                task = asyncio.ensure_future(self._destroy_kernel(kern_id))
                tasks.append(task)
            except:
                log.exception('clean_all_kernels: destroying {}'.format(kern_id))
        await asyncio.gather(*tasks)


def main():
    global lang_aliases

    argparser = argparse.ArgumentParser()
    argparser.add_argument('--agent-ip-override', type=ipaddr, default=None, dest='agent_ip',
                           help='Manually set the IP address of this agent to report to the manager.')
    argparser.add_argument('--agent-port', type=port_no, default=6001,
                           help='The port number to listen on.')
    argparser.add_argument('--redis-addr', type=host_port_pair, default=('localhost', 6379),
                           help='The host:port pair of the Redis (agent registry) server.')
    argparser.add_argument('--exec-timeout', type=positive_int, default=180,
                           help='The maximum period of time allowed for kernels to run user codes.')
    argparser.add_argument('--idle-timeout', type=positive_int, default=600,
                           help='The maximum period of time allowed for kernels to wait further requests.')
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

    if args.agent_ip:
        args.agent_ip = str(args.agent_ip)
    args.redis_addr = args.redis_addr if args.redis_addr else ('sorna-manager.lablup', 6379)

    assert Path(args.volume_root).exists()
    assert Path(args.volume_root).is_dir()

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
        'git-shell': 'git',
        'shell': 'git',
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

    log_config = logging.getLogger('sorna.agent.config')
    if args.debug:
        log_config.debug('debug mode enabled.')

    def handle_signal(loop, term_ev):
        if term_ev.is_set():
            log.warning('Forced shutdown!')
            sys.exit(1)
        else:
            term_ev.set()
            loop.stop()

    term_ev = asyncio.Event()
    loop.add_signal_handler(signal.SIGTERM, handle_signal, loop, term_ev)
    loop.add_signal_handler(signal.SIGINT, handle_signal, loop, term_ev)

    docker = None
    agent = None
    server = None
    hb_task = None
    timer_task = None

    async def initialize():
        nonlocal docker, agent, server
        nonlocal hb_task, timer_task
        args.inst_id = await utils.get_instance_id()
        args.inst_type = await utils.get_instance_type()
        args.agent_ip = await utils.get_instance_ip()
        log.info('myself: {} ({}), ip: {}'.format(args.inst_id, args.inst_type, args.agent_ip))
        log.info('using manager redis at tcp://{0}:{1}'.format(*args.redis_addr))

        # Initialize Docker
        docker = Docker(url='/var/run/docker.sock')
        docker_version = await docker.version()
        log.info('running with Docker {0} with API {1}'
                 .format(docker_version['Version'], docker_version['ApiVersion']))

        # Start RPC server.
        agent_addr = 'tcp://*:{}'.format(args.agent_port)
        agent = AgentRPCServer(docker, args, loop=loop)
        await agent.init()
        server = await aiozmq.rpc.serve_rpc(agent, bind=agent_addr)
        # TODO: server_sock.transport.setsockopt(zmq.LINGER, 50)
        log.info('serving at {0}'.format(agent_addr))

        # Send the first heartbeat.
        hb_task    = asyncio.ensure_future(heartbeat_timer(agent), loop=loop)
        timer_task = asyncio.ensure_future(cleanup_timer(agent), loop=loop)
        await asyncio.sleep(0.01, loop=loop)

    async def shutdown():
        # Stop receiving further requests.
        server.close()
        await server.wait_closed()

        # Stop timers.
        hb_task.cancel()
        timer_task.cancel()
        await asyncio.sleep(0.01)

        # Clean all kernels.
        await agent.clean_all_kernels()
        docker.session.close()

        # Finalize.
        await agent.shutdown()
        await asyncio.sleep(0.01)

    try:
        loop.run_until_complete(initialize())
        loop.run_forever()
        # interrupted
        loop.run_until_complete(shutdown())
    finally:
        loop.close()
        log.info('exit.')


if __name__ == '__main__':
    main()
