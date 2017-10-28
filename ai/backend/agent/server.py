import asyncio
from ipaddress import ip_address
import logging, logging.config
import os, os.path
from pathlib import Path
import shutil
import time

from aiodocker.docker import Docker, DockerContainer
from aiodocker.exceptions import DockerError
import aioredis
import aiotools
import aiozmq, aiozmq.rpc
from async_timeout import timeout
import configargparse
from namedlist import namedtuple
import uvloop
import zmq
try:
    import datadog
    datadog_available = True
except ImportError:
    datadog_available = False
try:
    import raven
    raven_available = True
except ImportError:
    raven_available = False

from ai.backend.common import utils, identity, msgpack
from ai.backend.common.argparse import (
    ipaddr, port_no, HostPortPair,
    host_port_pair, positive_int)
from ai.backend.common.monitor import DummyStatsd, DummySentry
from .version import VERSION
from .files import scandir, upload_output_files_to_s3
from .gpu import prepare_nvidia
from .stats import collect_stats
from .resources import detect_slots, libnuma, CPUAllocMap
from .kernel import KernelRunner, KernelFeatures, ExpectedInput

log = logging.getLogger('ai.backend.agent.server')

supported_langs = {
    'python2',
    'python3',
    'python3-torch',
    'python3-torch-gpu',
    'python3-tensorflow',
    'python3-tensorflow-gpu',
    'python3-caffe',
    'python3-theano',
    'r3',
    'php5',
    'php7',
    'nodejs6',
    'git',
    'julia',
    'lua5',
    'haskell',
    'octave4',
    'cpp',
    'c',
    'java',
    'go',
    'rust',
}
lang_aliases = dict()
max_upload_size = 100 * 1024 * 1024  # 100 MB
stat_cache_lifespan = 30.0  # 30 secs

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
            log.warning(f'could not attach volume {vol.name} to '
                        f'a kernel using language {lang} (volume not found)')
    return mount_list


def get_kernel_id_from_container(val):
    name = val['Names'][0] if isinstance(val, DockerContainer) else val
    name = name.lstrip('/')
    if not name.startswith('kernel.'):
        return None
    try:
        return name.split('.', 2)[-1]
    except (IndexError, ValueError):
        return None


class AgentRPCServer(aiozmq.rpc.AttrHandler):

    def __init__(self, config, loop=None):
        self.config = config
        self.container_registry = {}
        self.container_cpu_map = CPUAllocMap()

        self.loop = loop if loop else asyncio.get_event_loop()
        self.docker = Docker()

        self.restarting_kernels = {}
        self.blocking_cleans = {}

        self.etcd = None
        self.config.redis_addr = None

        self.slots = detect_slots()

        self.server = None
        self.event_sock = None
        self.monitor_fetch_task = None
        self.monitor_handle_task = None
        self.hb_timer = None
        self.stats_timer = None
        self.clean_timer = None

        self.statsd = DummyStatsd()
        self.sentry = DummySentry()
        if datadog_available and self.config.datadog_api_key:
            self.statsd = datadog.statsd
        if raven_available and self.config.raven_uri:
            self.sentry = raven.Client(
                self.config.raven_uri,
                release=raven.fetch_package_version('backend.ai-agent'))

    async def detect_manager(self):
        log.info('detecting the manager...')
        from ai.backend.common.etcd import AsyncEtcd
        self.etcd = AsyncEtcd(self.config.etcd_addr, self.config.namespace)
        manager_id = await self.etcd.get('nodes/manager')
        if manager_id is None:
            log.warning('watching etcd to wait for the manager being availabile')
            async for ev in self.etcd.watch('nodes/manager'):
                if ev.event == 'put':
                    manager_id = ev.value
                    break
        log.info(f'detecting the manager: OK ({manager_id})')
        self.config.redis_addr = host_port_pair(await self.etcd.get('nodes/redis'))
        self.config.event_addr = host_port_pair(
            await self.etcd.get('nodes/manager/event_addr'))
        log.info(f'configured redis_addr: {self.config.redis_addr}')
        log.info(f'configured event_addr: {self.config.event_addr}')

    async def scan_running_containers(self):
        for container in (await self.docker.containers.list()):
            kernel_id = get_kernel_id_from_container(container)
            if kernel_id is None:
                continue
            if container['State'] in {'running', 'restarting', 'paused'}:
                log.info(f'detected running kernel: {kernel_id}')
                ports = {}
                for item in container['Ports']:
                    ports[item['PrivatePort']] = item['PublicPort']
                labels = container['Labels']
                # NOTE: this changes the content of container incompatibly.
                details = await container.show()
                cpu_set = set(
                    map(int, (details['HostConfig']['CpusetCpus']).split(',')))
                self.container_cpu_map.update(cpu_set)
                self.container_registry[kernel_id] = {
                    'lang': container['Image'],
                    'version': int(labels['io.sorna.version']),
                    'container_id': container._id,
                    'container_ip': '127.0.0.1',
                    'repl_in_port': ports[2000],
                    'repl_out_port': ports[2001],
                    'stdin_port': ports[2002],
                    'stdout_port': ports[2003],
                    'numa_node': libnuma.node_of_cpu(next(iter(cpu_set))),
                    'cpu_set': cpu_set,
                    'gpu_set': [],  # TODO: implement (using labels?)
                    'exec_timeout': int(labels['io.sorna.timeout']),
                    'last_used': time.monotonic(),
                    'limits': None,
                    # TODO: implement vfolders
                    'mounts': None,
                    'runner_tasks': set(),
                }
            elif container['State'] in {'exited', 'dead', 'removing'}:
                log.info(f'detected terminated kernel: {kernel_id}')
                await self.send_event('kernel_terminated', kernel_id,
                                      'self-terminated', None)

    async def update_status(self, status):
        await self.etcd.put(f'nodes/agents/{self.config.instance_id}', status)

    async def deregister_myself(self):
        await self.etcd.delete_prefix(f'nodes/agents/{self.config.instance_id}')

    async def send_event(self, event_name, *args):
        if self.event_sock is None:
            return
        log.debug(f'send_event({event_name})')
        self.event_sock.write((
            event_name.encode('ascii'),
            self.config.instance_id.encode('utf8'),
            msgpack.packb(args),
        ))

    async def check_images(self):
        # Read desired image versions from etcd.
        for key, value in (await self.etcd.get_prefix('images')):
            # TODO: implement
            pass

        # If there are newer images, pull them.
        # TODO: implement

    async def clean_runner(self, kernel_id):
        if kernel_id not in self.container_registry:
            return
        log.debug(f'interrupting & cleaning up runner for {kernel_id}')
        item = self.container_registry[kernel_id]
        for t in item['runner_tasks']:
            if not t.done():
                t.cancel()
                await t
        runner = item.get('runner')
        if runner is not None:
            await runner.close()

    async def init(self):
        # Show Docker version info.
        docker_version = await self.docker.version()
        log.info('running with Docker {0} with API {1}'
                 .format(docker_version['Version'], docker_version['ApiVersion']))

        await self.detect_manager()
        await self.update_status('starting')
        await self.scan_running_containers()
        await self.check_images()

        self.redis_stat_pool = await aioredis.create_pool(
            self.config.redis_addr.as_sockaddr(),
            create_connection_timeout=3.0,
            encoding='utf8',
            db=0)  # REDIS_STAT_DB in backend.ai-manager

        self.event_sock = await aiozmq.create_zmq_stream(
            zmq.PUSH, connect=f'tcp://{self.config.event_addr}')
        self.event_sock.transport.setsockopt(zmq.LINGER, 50)

        # Spawn docker monitoring tasks.
        self.monitor_fetch_task  = self.loop.create_task(self.fetch_docker_events())
        self.monitor_handle_task = self.loop.create_task(self.monitor())

        # Send the first heartbeat.
        self.hb_timer    = aiotools.create_timer(self.heartbeat, 3.0)
        self.stats_timer = aiotools.create_timer(self.update_stats, 5.0)
        self.clean_timer = aiotools.create_timer(self.clean_old_kernels, 10.0)

        # Start serving requests.
        agent_addr = 'tcp://*:{}'.format(self.config.agent_port)
        self.rpc_server = await aiozmq.rpc.serve_rpc(self, bind=agent_addr)
        self.rpc_server.transport.setsockopt(zmq.LINGER, 200)
        log.info('serving at {0}'.format(agent_addr))

        # Ready.
        await self.etcd.put(f'nodes/agents/{self.config.instance_id}/ip',
                            self.config.agent_ip)
        await self.update_status('running')

        # Notify the gateway.
        await self.send_event('instance_started')

    async def shutdown(self):
        await self.deregister_myself()

        # Stop receiving further requests.
        self.rpc_server.close()
        await self.rpc_server.wait_closed()

        # Close all pending kernel runners.
        for kernel_id in self.container_registry.keys():
            await self.clean_runner(kernel_id)

        # Stop timers.
        self.hb_timer.cancel()
        self.stats_timer.cancel()
        self.clean_timer.cancel()
        await self.hb_timer
        await self.stats_timer
        await self.clean_timer

        # Stop event monitoring.
        self.monitor_fetch_task.cancel()
        self.monitor_handle_task.cancel()
        await self.monitor_fetch_task
        await self.monitor_handle_task
        try:
            await self.docker.events.stop()
        except:
            pass
        await self.docker.close()

        self.redis_stat_pool.close()
        await self.redis_stat_pool.wait_closed()

        # Notify the gateway.
        await self.send_event('instance_terminated', 'shutdown')
        self.event_sock.close()

    @aiozmq.rpc.method
    def ping(self, msg: str) -> str:
        return msg

    @aiozmq.rpc.method
    async def create_kernel(self, kernel_id: str, config: dict) -> dict:
        config['lang'] = lang_aliases.get(config['lang'], config['lang'])
        log.debug(f"rpc::create_kernel({config['lang']})")
        try:
            return await self._create_kernel(kernel_id, config)
        except:
            log.exception('unexpected error')
            self.sentry.captureException()
            raise

    @aiozmq.rpc.method
    async def destroy_kernel(self, kernel_id: str):
        log.debug(f'rpc::destroy_kernel({kernel_id})')
        try:
            await self._destroy_kernel(kernel_id, 'user-requested')
        except:
            log.exception('unexpected error')
            self.sentry.captureException()
            raise

    @aiozmq.rpc.method
    async def interrupt_kernel(self, kernel_id: str):
        log.debug(f'rpc::interrupt_kernel({kernel_id})')
        try:
            await self._interrupt_kernel(kernel_id)
        except:
            log.exception('unexpected error')
            self.sentry.captureException()
            raise

    @aiozmq.rpc.method
    async def get_completions(self, kernel_id: str, mode: str,
                              text: str, opts: dict):
        log.debug(f'rpc::get_completions({kernel_id})')
        try:
            await self._get_completions(kernel_id, mode, text, opts)
        except:
            log.exception('unexpected error')
            self.sentry.captureException()
            raise

    @aiozmq.rpc.method
    async def restart_kernel(self, kernel_id: str, prev_config: dict):
        log.debug(f'rpc::restart_kernel({kernel_id})')
        try:
            self.restarting_kernels[kernel_id] = asyncio.Event()
            await self._destroy_kernel(kernel_id, 'restarting')
            await self._create_kernel(kernel_id, prev_config)
            if kernel_id in self.restarting_kernels:
                del self.restarting_kernels[kernel_id]
            return {
                'container_id': self.container_registry[kernel_id]['container_id'],
                'cpu_set': list(self.container_registry[kernel_id]['cpu_set']),
                'gpu_set': list(self.container_registry[kernel_id]['gpu_set']),
                'repl_in_port': self.container_registry[kernel_id]['repl_in_port'],
                'repl_out_port': self.container_registry[kernel_id]['repl_out_port'],
                'stdin_port': self.container_registry[kernel_id]['stdin_port'],
                'stdout_port': self.container_registry[kernel_id]['stdout_port'],
            }
        except:
            log.exception('unexpected error')
            self.sentry.captureException()
            raise

    @aiozmq.rpc.method
    async def execute(self, api_version: int, kernel_id: str,
                      run_id: str, mode: str, code: str, opts: dict) -> dict:
        log.debug(f'rpc::execute({kernel_id}, ...)')
        try:
            result = await self._execute(api_version, kernel_id,
                                         run_id, mode, code, opts)
            return result
        except:
            log.exception('unexpected error')
            self.sentry.captureException()
            raise

    @aiozmq.rpc.method
    async def upload_file(self, kernel_id: str, filename: str, filedata: bytes):
        log.debug(f'rpc::upload_file({kernel_id}, {filename})')
        try:
            await self._accept_file(kernel_id, filename, filedata)
        except:
            log.exception('unexpected error')
            self.sentry.captureException()
            raise

    @aiozmq.rpc.method
    async def reset(self):
        log.debug('rpc::reset()')
        kernel_ids = tuple(self.container_registry.keys())
        tasks = []
        try:
            for kernel_id in kernel_ids:
                try:
                    task = asyncio.ensure_future(
                        self._destroy_kernel(kernel_id, 'agent-reset'))
                    tasks.append(task)
                except:
                    self.sentry.captureException()
                    log.exception(f'reset: destroying {kernel_id}')
            await asyncio.gather(*tasks)
        except:
            log.exception('unexpected error')
            self.sentry.captureException()
            raise

    async def _create_kernel(self, kernel_id, config):

        await self.send_event('kernel_creating', kernel_id)

        lang = config['lang']
        mounts = config.get('mounts')
        limits = config.get('limits')

        image_name = f'lablup/kernel-{lang}'
        ret = await self.docker.images.get(image_name)
        image_labels = ret['ContainerConfig']['Labels']
        # TODO: apply limits
        version        = int(image_labels.get('io.sorna.version', '1'))
        mem_limit      = image_labels.get('io.sorna.maxmem', '128m')
        exec_timeout   = int(image_labels.get('io.sorna.timeout', '10'))
        envs_corecount = image_labels.get('io.sorna.envs.corecount', '')
        envs_corecount = envs_corecount.split(',') if envs_corecount else []
        kernel_features = set(image_labels.get('io.sorna.features', '').split())

        work_dir = self.config.scratch_root / kernel_id

        # TODO: implement
        vfolders = []  # noqa

        if kernel_id in self.restarting_kernels:
            # Wait until the previous container is actually deleted.
            try:
                with timeout(10):
                    await self.restarting_kernels[kernel_id].wait()
            except asyncio.TimeoutError:
                log.warning('timeout detected while restarting '
                            f'kernel {kernel_id}!')
                del self.restarting_kernels[kernel_id]
                asyncio.ensure_future(self.clean_kernel(kernel_id))
                raise
        else:
            os.makedirs(work_dir)

        cpu_set = config.get('cpu_set')
        if cpu_set is None:
            requested_cores = int(image_labels.get('io.sorna.maxcores', '1'))
            num_cores = min(self.container_cpu_map.num_cores, requested_cores)
            numa_node, cpu_set = self.container_cpu_map.alloc(num_cores)
        else:
            num_cores = len(cpu_set)
            numa_node = libnuma.node_of_cpu(next(iter(cpu_set)))

        cpu_set_str = ','.join(map(str, sorted(cpu_set)))
        envs = {k: str(num_cores) for k in envs_corecount}
        if KernelFeatures.UID_MATCH in kernel_features:
            envs['LOCAL_USER_ID'] = os.getuid()
        log.debug(f'container config: mem_limit={mem_limit}, '
                  f'exec_timeout={exec_timeout}, '
                  f'cores={cpu_set!r}@{numa_node}')

        mount_list = await get_extra_volumes(self.docker, lang)
        binds = [f'{work_dir}:/home/work:rw']
        binds.extend(f'{v.name}:{v.container_path}:{v.mode}'
                     for v in mount_list)
        volumes = ['/home/work']
        volumes.extend(v.container_path for v in mount_list)
        devices = []
        gpu_set = config.get('gpu_set', [])

        # TODO: implement vfolders and translate them into mounts

        nvidia_enabled = (ret['ContainerConfig']['Labels']
                          .get('io.sorna.nvidia.enabled', 'no'))
        if 'yes' == nvidia_enabled:
            extra_binds, extra_devices = \
                await prepare_nvidia(self.docker, numa_node)
            binds.extend(extra_binds)
            devices.extend(extra_devices)
            # TODO: update gpu_set

        config = {
            'Image': image_name,
            'Tty': True,
            'OpenStdin': True,
            'Privileged': False,
            'Volumes': {v: {} for v in volumes},
            'StopSignal': 'SIGINT',
            'ExposedPorts': {
                '2000/tcp': {},
                '2001/tcp': {},
                '2002/tcp': {},
                '2003/tcp': {},
            },
            'Env': [f'{k}={v}' for k, v in envs.items()],
            'HostConfig': {
                'MemorySwap': 0,
                'Memory': utils.readable_size_to_bytes(mem_limit),
                'CpusetCpus': cpu_set_str,
                'CpusetMems': f'{numa_node}',
                'SecurityOpt': ['seccomp=unconfined'],
                'Binds': binds,
                'Devices': devices,
                'PublishAllPorts': True,
            },
        }
        kernel_name = f'kernel.{lang}.{kernel_id}'
        container = await self.docker.containers.create(
            config=config, name=kernel_name)
        await container.start()
        repl_in_port  = (await container.port(2000))[0]['HostPort']
        repl_out_port = (await container.port(2001))[0]['HostPort']
        stdin_port  = (await container.port(2002))[0]['HostPort']
        stdout_port = (await container.port(2003))[0]['HostPort']
        kernel_ip = '127.0.0.1'

        self.container_registry[kernel_id] = {
            'lang': lang,
            'version': version,
            'container_id': container._id,
            'container_ip': kernel_ip,
            'repl_in_port': repl_in_port,
            'repl_out_port': repl_out_port,
            'stdin_port': stdin_port,
            'stdout_port': stdout_port,
            'numa_node': numa_node,
            'cpu_set': cpu_set,
            'gpu_set': gpu_set,
            'exec_timeout': exec_timeout,
            'last_used': time.monotonic(),
            'limits': limits,
            'mounts': mounts,
            'runner_tasks': set(),
        }
        log.debug(f'kernel repl-in address: {kernel_ip}:{repl_in_port}')
        log.debug(f'kernel repl-out address: {kernel_ip}:{repl_out_port}')
        log.debug(f'kernel stdin address:  {kernel_ip}:{stdin_port}')
        log.debug(f'kernel stdout address: {kernel_ip}:{stdout_port}')
        return {
            'id': kernel_id,
            'repl_in_port': int(repl_in_port),
            'repl_out_port': int(repl_out_port),
            'stdin_port': int(stdin_port),
            'stdout_port': int(stdout_port),
            'container_id': container._id,
            'cpu_set': list(cpu_set),
            'gpu_set': list(gpu_set),
        }

    async def _destroy_kernel(self, kernel_id, reason):
        try:
            cid = self.container_registry[kernel_id]['container_id']
        except KeyError:
            log.warning(f'_destroy_kernel({kernel_id}) kernel missing '
                        '(already dead?)')
            await self.clean_kernel(kernel_id)
            return
        container = self.docker.containers.container(cid)
        await self.clean_runner(kernel_id)
        try:
            # last moment stats must be collected before killing the container.
            last_stat = (await collect_stats([container]))[0]
            async with self.redis_stat_pool.get() as rs:
                pipe = rs.pipeline()
                pipe.hmset_dict(kernel_id, last_stat)
                pipe.expire(kernel_id, stat_cache_lifespan)
                await pipe.execute()
            await container.kill()
            # the container will be deleted in the docker monitoring coroutine.
        except DockerError as e:
            if e.status == 500 and 'is not running' in e.message:
                # already dead
                log.warning(f'_destroy_kernel({kernel_id}) kill 500')
                pass
            elif e.status == 404:
                log.warning(f'_destroy_kernel({kernel_id}) kill 404, '
                            'forgetting this kernel')
                self.container_cpu_map.free(
                    self.container_registry[kernel_id]['cpu_set'])
                del self.container_registry[kernel_id]
                pass
            else:
                log.exception(f'_destroy_kernel({kernel_id}) kill error')
        except:
            log.exception(f'_destroy_kernel({kernel_id}) unexpected error')

    async def _ensure_runner(self, kernel_id):
        # TODO: clean up
        api_version = 3
        runner = self.container_registry[kernel_id].get('runner')
        if runner is not None:
            log.debug(f'_execute_code:v{api_version}({kernel_id}) use '
                      'existing runner')
        else:
            client_features = {'input', 'continuation'}
            runner = KernelRunner(
                kernel_id,
                self.container_registry[kernel_id]['container_ip'],
                self.container_registry[kernel_id]['repl_in_port'],
                self.container_registry[kernel_id]['repl_out_port'],
                self.container_registry[kernel_id]['exec_timeout'],
                client_features)
            log.debug(f'_execute:v{api_version}({kernel_id}) start new runner')
            self.container_registry[kernel_id]['runner'] = runner
            # TODO: restoration of runners after agent restarts
            await runner.start()
        return runner

    async def _execute(self, api_version, kernel_id, run_id, mode, text, opts):
        work_dir = self.config.scratch_root / kernel_id
        # Save kernel-generated output files in a separate sub-directory
        # (to distinguish from user-uploaded files)
        output_dir = work_dir / '.output'

        try:
            self.container_registry[kernel_id]['last_used'] = time.monotonic()
        except KeyError:
            raise RuntimeError(f'The container for kernel {kernel_id} is not found! '
                               '(might be terminated)') from None

        runner = await self._ensure_runner(kernel_id)

        try:
            myself = asyncio.Task.current_task()
            self.container_registry[kernel_id]['runner_tasks'].add(myself)

            await runner.attach_output_queue(run_id)

            if runner.expects == ExpectedInput.USER_CODE:
                self.container_registry[kernel_id]['initial_file_stats'] \
                    = scandir(output_dir, max_upload_size)
                runner.mode = mode
                if mode == 'batch':
                    await runner.feed_batch_opts(opts)
                else:
                    await runner.feed_code(text)
            elif runner.expects == ExpectedInput.USER_INPUT:
                await runner.feed_input(text)
            elif runner.expects == ExpectedInput.CONTINUATION:
                # don't need to provide additional inputs
                pass
            result = await runner.get_next_result(api_ver=api_version)

        except asyncio.CancelledError:
            await runner.close()
            del self.container_registry[kernel_id]['runner']
            return
        except:
            log.exception('unexpected error')
            raise
        finally:
            runner_tasks = utils.nmget(self.container_registry,
                                       f'{kernel_id}/runner_tasks', None, '/')
            if runner_tasks is not None:
                runner_tasks.remove(myself)

        try:
            output_files = []

            if result['status'] in ('finished', 'exec-timeout'):

                log.debug(f"_execute({kernel_id}) {result['status']}")

                final_file_stats = scandir(output_dir, max_upload_size)
                if utils.nmget(result, 'options.upload_output_files', True):
                    # TODO: separate as a new task
                    # TODO: replace entry ID ('0') with some different ID
                    initial_file_stats = \
                        self.container_registry[kernel_id]['initial_file_stats']
                    output_files = await upload_output_files_to_s3(
                        initial_file_stats, final_file_stats, work_dir, kernel_id)
                    output_files = [os.path.relpath(fn, output_dir)
                                    for fn in output_files]

                self.container_registry[kernel_id].pop('initial_file_stats', None)

            if (result['status'] == 'exec-timeout' and
                    kernel_id in self.container_registry):
                # clean up the kernel (maybe cleaned already)
                asyncio.ensure_future(
                    self._destroy_kernel(kernel_id, 'exec-timeout'))

            return {
                'status': result['status'],
                'console': result['console'],
                'completions': utils.nmget(result, 'completions', None),
                'options': utils.nmget(result, 'options', None),
                'files': output_files,
            }
        except:
            log.exception('unexpected error')
            raise

    async def _get_completions(self, kernel_id, text, opts):
        runner = await self._ensure_runner(kernel_id)
        result = await runner.feed_and_get_completion(text, opts)
        return {'status': 'finished', 'completions': result}

    async def _interrupt_kernel(self, kernel_id):
        runner = await self._ensure_runner(kernel_id)
        await runner.feed_interrupt()
        return {'status': 'finished'}

    async def _accept_file(self, kernel_id, filename, filedata):
        work_dir = self.config.scratch_root / kernel_id
        try:
            # create intermediate directories in the path
            dest_path = (work_dir / filename).resolve(strict=False)
            rel_path = dest_path.parent.relative_to(work_dir)
        except ValueError:  # rel_path does not start with work_dir!
            raise AssertionError('malformed upload filename and path.')
        rel_path.mkdir(parents=True, exist_ok=True)
        # TODO: use aiofiles?
        with open(dest_path, 'wb') as f:
            f.write(filedata)

    async def heartbeat(self, interval):
        '''
        Record my status information to the manager database (Redis).
        This information automatically expires after 2x interval, so that failure
        of executing this method automatically removes the instance from the
        manager database.
        '''

        agent_info = {
            'ip': self.config.agent_ip,
            'region': self.config.region,
            'addr': f'tcp://{self.config.agent_ip}:{self.config.agent_port}',
            'mem_slots': self.slots[0],
            'cpu_slots': self.slots[1],
            'gpu_slots': self.slots[2],
        }
        try:
            await self.send_event('instance_heartbeat', agent_info)
        except asyncio.TimeoutError:
            log.warning('event dispatch timeout: instance_heartbeat')
        except:
            log.exception('instance_heartbeat failure')

    async def update_stats(self, interval):
        running_kernels = [k for k in self.container_registry.keys()]
        running_containers = [self.container_registry[k]['container_id']
                              for k in running_kernels]
        try:
            stats = await collect_stats(map(self.docker.containers.container,
                                            running_containers))
        except asyncio.CancelledError:
            return
        async with self.redis_stat_pool.get() as rs:
            pipe = rs.pipeline()
            for idx, stat in enumerate(stats):
                kernel_id = running_kernels[idx]
                pipe.hmset_dict(kernel_id, stat)
                pipe.expire(kernel_id, stat_cache_lifespan)
            await pipe.execute()

    async def fetch_docker_events(self):
        while True:
            try:
                await self.docker.events.run()
            except asyncio.TimeoutError:
                # The API HTTP connection may terminate after some timeout
                # (e.g., 5 minutes)
                log.info('restarting docker.events.run()')
                continue
            except asyncio.CancelledError:
                break
            except:
                log.exception('unexpected error')
                break

    async def monitor(self):
        subscriber = self.docker.events.subscribe()
        while True:
            try:
                evdata = await subscriber.get()
            except asyncio.CancelledError:
                break
            if evdata is None:
                # fetch_docker_events() will automatically reconnect.
                continue
            if evdata['Action'] == 'die':
                # When containers die, we immediately clean up them.
                container_id = evdata['id']
                container_name = evdata['Actor']['Attributes']['name']
                kernel_id = get_kernel_id_from_container(container_name)
                if kernel_id is None:
                    continue
                try:
                    exit_code = evdata['Actor']['Attributes']['exitCode']
                except KeyError:
                    exit_code = '(unknown)'
                log.debug('docker-event: container-terminated: '
                          f'{container_id[:7]} with exit code {exit_code} '
                          f'({kernel_id})')
                await self.send_event('kernel_terminated',
                                      kernel_id, 'self-terminated',
                                      None)
                asyncio.ensure_future(self.clean_kernel(kernel_id))

    async def clean_kernel(self, kernel_id):
        try:
            container_id = self.container_registry[kernel_id]['container_id']
            container = self.docker.containers.container(container_id)
            await self.clean_runner(kernel_id)
            try:
                await container.delete()
            except DockerError as e:
                if e.status == 400 and 'already in progress' in e.message:
                    pass
                elif e.status == 404:
                    pass
                else:
                    log.warning(f'container deletion: {e!r}')
        except KeyError:
            pass
        if kernel_id in self.restarting_kernels:
            self.restarting_kernels[kernel_id].set()
        else:
            work_dir = self.config.scratch_root / kernel_id
            try:
                shutil.rmtree(work_dir)
            except FileNotFoundError:
                pass
            try:
                self.container_cpu_map.free(
                    self.container_registry[kernel_id]['cpu_set'])
                del self.container_registry[kernel_id]
            except KeyError:
                pass
            if kernel_id in self.blocking_cleans:
                self.blocking_cleans[kernel_id].set()

    async def clean_old_kernels(self, interval):
        now = time.monotonic()
        keys = tuple(self.container_registry.keys())
        tasks = []
        for kern_id in keys:
            try:
                last_used = self.container_registry[kern_id]['last_used']
                if now - last_used > self.config.idle_timeout:
                    log.info(f'destroying kernel {kern_id} as clean-up')
                    task = asyncio.ensure_future(
                        self._destroy_kernel(kern_id, 'idle-timeout'))
                    tasks.append(task)
            except KeyError:
                # The kernel may be destroyed by other means?
                pass
        await asyncio.gather(*tasks)

    async def clean_all_kernels(self, blocking=False):
        log.info('cleaning all kernels...')
        kern_ids = tuple(self.container_registry.keys())
        tasks = []
        if blocking:
            for kern_id in kern_ids:
                self.blocking_cleans[kern_id] = asyncio.Event()
        for kern_id in kern_ids:
            task = asyncio.ensure_future(
                self._destroy_kernel(kern_id, 'agent-termination'))
            tasks.append(task)
        await asyncio.gather(*tasks)
        if blocking:
            waiters = [self.blocking_cleans[kern_id].wait() for kern_id in kern_ids]
            await asyncio.gather(*waiters)
            for kern_id in kern_ids:
                del self.blocking_cleans[kern_id]


@aiotools.actxmgr
async def server_main(loop, pidx, _args):

    args = _args[0]
    args.instance_id = await identity.get_instance_id()
    args.inst_type = await identity.get_instance_type()
    if not args.agent_ip:
        args.agent_ip = await identity.get_instance_ip()
    args.region = await identity.get_instance_region()
    log.info(f'myself: {args.instance_id} ({args.inst_type}), ip: {args.agent_ip}')

    # Start RPC server.
    agent = AgentRPCServer(args, loop=loop)
    await agent.init()

    # Run!
    yield

    # Shutdown.
    log.info('shutting down...')
    await agent.shutdown()


def main():
    global lang_aliases

    parser = configargparse.ArgumentParser()
    parser.add('--namespace', type=str, default='local',
               help='The namespace of this Backend.AI cluster. (default: local)')
    parser.add('--agent-ip-override', type=ipaddr, default=None, dest='agent_ip',
               env_var='BACKEND_AGENT_IP',
               help='Manually set the IP address of this agent to report to the '
                    'manager.')
    parser.add('--agent-port', type=port_no, default=6001,
               env_var='BACKEND_AGENT_PORT',
               help='The port number to listen on.')
    parser.add('--etcd-addr', type=host_port_pair,
               env_var='ETCD_ADDR',
               default=HostPortPair(ip_address('127.0.0.1'), 2379),
               help='The host:port pair of the etcd cluster or its proxy.')
    parser.add('--idle-timeout', type=positive_int, default=600,
               help='The maximum period of time allowed for kernels to wait '
                    'further requests.')
    parser.add('--debug', action='store_true', default=False,
               env_var='DEBUG',
               help='Enable more verbose logging.')
    parser.add('--kernel-aliases', type=str, default=None,
               help='The filename for additional kernel aliases')
    parser.add('--scratch-root', type=Path,
               default=Path('/var/cache/scratches'),
               env_var='BACKEND_SCRATCH_ROOT',
               help='The scratch directory to store container working directories.')
    if datadog_available:
        parser.add('--datadog-api-key', env_var='DATADOG_API_KEY',
                   type=str, default=None,
                   help='The API key for Datadog monitoring agent.')
        parser.add('--datadog-app-key', env_var='DATADOG_APP_KEY',
                   type=str, default=None,
                   help='The application key for Datadog monitoring agent.')
    if raven_available:
        parser.add('--raven-uri', env_var='RAVEN_URI', type=str, default=None,
                   help='The sentry.io event report URL with DSN.')
    args = parser.parse_args()

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
                'stream': 'ext://sys.stderr',
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
        },
    })

    if args.agent_ip:
        args.agent_ip = str(args.agent_ip)

    assert args.scratch_root.exists()
    assert args.scratch_root.is_dir()

    if datadog_available and args.datadog_api_key:
        datadog.initialize(
            api_key=args.datadog_api_key,
            app_key=args.datadog_app_key,
        )

    # Load language aliases config.
    lang_aliases = {lang: lang for lang in supported_langs}
    lang_aliases.update({
        'python': 'python3',
        'python26': 'python2',
        'python27': 'python2',
        'python34': 'python3',
        'python35': 'python3',
        'python36': 'python3',
        'python3-deeplearning':   'python3-tensorflow',
        'tensorflow-python3':     'python3-tensorflow',
        'tensorflow-python3-gpu': 'python3-tensorflow-gpu',
        'caffe-python3':          'python3-caffe',
        'theano-python3':         'python3-theano',
        'r': 'r3',
        'R': 'r3',
        'Rscript': 'r3',
        'php': 'php7',
        'node': 'nodejs6',
        'nodejs': 'nodejs6',
        'js': 'nodejs6',
        'javascript': 'nodejs6',
        'lua': 'lua5',
        'git-shell': 'git',
        'shell': 'git',
        'ocatve': 'octave4',
        'cpp': 'cpp',
        'c': 'c',
        'java': 'java',
        'go': 'go',
        'rust': 'rust',
    })
    if args.kernel_aliases:  # for when we want to add extra
        with open(args.kernel_aliases, 'r') as f:
            for line in f:
                alias, target = line.strip().split()
                assert target in supported_langs
                lang_aliases[alias] = target

    log.info(f'Backend.AI Agent {VERSION}')
    log.info(f'runtime: {utils.env_info()}')

    log_config = logging.getLogger('ai.backend.agent.config')
    if args.debug:
        log_config.debug('debug mode enabled.')

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    aiotools.start_server(server_main, num_workers=1,
                          use_threading=True, args=(args, ))
    log.info('exit.')


if __name__ == '__main__':
    main()
