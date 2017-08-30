import asyncio
from ipaddress import ip_address
import logging, logging.config
import os, os.path
from pathlib import Path
import secrets
import signal
import shutil
import sys
import time

from aiodocker.docker import Docker
from aiodocker.exceptions import DockerError
from async_timeout import timeout
import configargparse
from namedlist import namedtuple
import uvloop
import zmq, aiozmq, aiozmq.rpc
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

from sorna import utils
from sorna.argparse import (
    ipaddr, port_no, HostPortPair,
    host_port_pair, positive_int)
from sorna.monitor import DummyDatadog, DummySentry
from sorna.utils import nmget, readable_size_to_bytes
from .version import VERSION
from .files import scandir, upload_output_files_to_s3
from .gpu import prepare_nvidia
from .stats import collect_stats
from .resources import libnuma, CPUAllocMap
from .kernel import KernelRunner, KernelFeatures

log = logging.getLogger('sorna.agent.server')

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
max_upload_size = 5 * 1024 * 1024  # 5 MB

VolumeInfo = namedtuple('VolumeInfo', 'name container_path mode')
_extra_volumes = {
    'python3-tensorflow': [
        VolumeInfo('deeplearning-samples', '/home/work/samples', 'ro'),
    ],
    'python3-tensorflow-gpu': [
        VolumeInfo('deeplearning-samples', '/home/work/samples', 'ro'),
    ],
}

restarting_kernels = {}
blocking_cleans = {}


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


async def heartbeat_timer(agent, interval=3.0):
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


async def stats_timer(agent, interval=5.0):
    task = None
    try:
        while True:
            task = asyncio.ensure_future(agent.update_stats(interval))
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        if task and not task.done():
            task.cancel()
            await task


async def cleanup_timer(agent):
    task = None
    try:
        while True:
            task = asyncio.ensure_future(agent.clean_old_kernels())
            await asyncio.sleep(10)
    except asyncio.CancelledError:
        if task and not task.done():
            task.cancel()
            await task


class AgentRPCServer(aiozmq.rpc.AttrHandler):

    def __init__(self, docker, config, events, ddagent, sentry, loop=None):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.docker = docker
        self.config = config
        self.events = events
        self.container_registry = {}
        self.container_cpu_map = CPUAllocMap()
        self.datadog = ddagent
        self.sentry = sentry

    async def init(self):
        pass

    async def shutdown(self):
        pass

    @aiozmq.rpc.method
    def ping(self, msg: str) -> str:
        return msg

    @aiozmq.rpc.method
    async def create_kernel(self, lang: str,
                            limits: dict=None,
                            mounts: list=None) -> tuple:
        if lang in lang_aliases:
            lang = lang_aliases[lang]
        log.debug(f'rpc::create_kernel({lang}, {limits}, {mounts})')
        try:
            kernel_id = await self._create_kernel(lang, None, limits, mounts)
            stdin_port = self.container_registry[kernel_id]['stdin_port']
            stdout_port = self.container_registry[kernel_id]['stdout_port']
            return kernel_id, stdin_port, stdout_port
        except:
            self.sentry.captureException()
            raise

    @aiozmq.rpc.method
    async def destroy_kernel(self, kernel_id: str):
        log.debug(f'rpc::destroy_kernel({kernel_id})')
        try:
            await self._destroy_kernel(kernel_id, 'user-requested')
        except:
            self.sentry.captureException()
            raise

    @aiozmq.rpc.method
    async def restart_kernel(self, kernel_id: str):
        log.debug(f'rpc::restart_kernel({kernel_id})')
        try:
            restarting_kernels[kernel_id] = asyncio.Event()
            await self._destroy_kernel(kernel_id, 'restarting')
            lang = self.container_registry[kernel_id]['lang']
            limits = self.container_registry[kernel_id]['limits']
            mounts = self.container_registry[kernel_id]['mounts']
            await self._create_kernel(lang, kernel_id, limits, mounts)
            if kernel_id in restarting_kernels:
                del restarting_kernels[kernel_id]
            stdin_port = self.container_registry[kernel_id]['stdin_port']
            stdout_port = self.container_registry[kernel_id]['stdout_port']
            return stdin_port, stdout_port
        except:
            self.sentry.captureException()
            raise

    @aiozmq.rpc.method
    async def execute_code(self, api_version: int, kernel_id: str,
                           mode: str, code: str, opts: dict) -> dict:
        log.debug(f'rpc::execute_code({kernel_id}, ...)')
        try:
            result = await self._execute_code(api_version, kernel_id,
                                              mode, code, opts)
            return result
        except:
            self.sentry.captureException()
            raise

    @aiozmq.rpc.method
    async def upload_file(self, kernel_id: str, filename: str, filedata: bytes):
        log.debug(f'rpc::upload_file({kernel_id}, {filename})')
        try:
            await self._upload_file(kernel_id, filename, filedata)
        except:
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
            self.sentry.captureException()
            raise

    async def _create_kernel(self, lang, kernel_id=None,
                             limits=None, mounts=None):
        if not kernel_id:
            kernel_id = secrets.token_urlsafe(16)
            assert kernel_id not in self.container_registry
            await self.events.call.dispatch('kernel_creating', kernel_id)
        else:
            await self.events.call.dispatch('kernel_restarting', kernel_id)

        image_name = f'lablup/kernel-{lang}'
        ret = await self.docker.images.get(image_name)
        image_labels = ret['ContainerConfig']['Labels']
        # TODO: apply limits
        version        = int(image_labels.get('io.sorna.version', '1'))
        mem_limit      = image_labels.get('io.sorna.maxmem', '128m')
        exec_timeout   = int(image_labels.get('io.sorna.timeout', '10'))
        exec_timeout   = min(exec_timeout, self.config.exec_timeout)
        envs_corecount = image_labels.get('io.sorna.envs.corecount', '')
        envs_corecount = envs_corecount.split(',') if envs_corecount else []
        kernel_features = set(image_labels.get('io.sorna.features', '').split())

        work_dir = self.config.volume_root / kernel_id

        if kernel_id in restarting_kernels:
            core_set = self.container_registry[kernel_id]['core_set']
            any_core = next(iter(core_set))
            numa_node = libnuma.node_of_cpu(any_core)
            num_cores = len(core_set)
            # Wait until the previous container is actually deleted.
            try:
                with timeout(10):
                    await restarting_kernels[kernel_id].wait()
            except asyncio.TimeoutError:
                log.warning('timeout detected while restarting '
                            f'kernel {kernel_id}!')
                del restarting_kernels[kernel_id]
                asyncio.ensure_future(self.clean_kernel(kernel_id))
                raise
        else:
            os.makedirs(work_dir)
            requested_cores = int(ret['ContainerConfig']['Labels']
                                  .get('io.sorna.maxcores', '1'))
            num_cores = min(self.container_cpu_map.num_cores, requested_cores)
            numa_node, core_set = self.container_cpu_map.alloc(num_cores)

        envs = {k: str(num_cores) for k in envs_corecount}
        if KernelFeatures.UID_MATCH in kernel_features:
            envs['LOCAL_USER_ID'] = os.getuid()
        log.debug(f'container config: mem_limit={mem_limit}, '
                  f'exec_timeout={exec_timeout}, '
                  f'cores={core_set!r}@{numa_node}')

        mount_list = await get_extra_volumes(self.docker, lang)
        binds = [f'{work_dir}:/home/work:rw']
        binds.extend(f'{v.name}:{v.container_path}:{v.mode}'
                     for v in mount_list)
        volumes = ['/home/work']
        volumes.extend(v.container_path for v in mount_list)
        devices = []

        # TODO: apply mounts

        nvidia_enabled = (ret['ContainerConfig']['Labels']
                          .get('io.sorna.nvidia.enabled', 'no'))
        if 'yes' == nvidia_enabled:
            extra_binds, extra_devices = \
                await prepare_nvidia(self.docker, numa_node)
            binds.extend(extra_binds)
            devices.extend(extra_devices)

        config = {
            'Image': image_name,
            'Tty': True,
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
                'Memory': readable_size_to_bytes(mem_limit),
                'CpusetCpus': ','.join(map(str, sorted(core_set))),
                'CpusetMems': f'{numa_node}',
                'SecurityOpt': ['seccomp:unconfined'],
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
            'cpu_shares': 1024,
            'numa_node': numa_node,
            'core_set': core_set,
            'mem_limit': mem_limit,
            'exec_timeout': exec_timeout,
            'num_queries': 0,
            'last_used': time.monotonic(),
            'limits': limits,
            'mounts': mounts,
        }
        log.debug(f'kernel repl-in address: {kernel_ip}:{repl_in_port}')
        log.debug(f'kernel repl-out address: {kernel_ip}:{repl_out_port}')
        log.debug(f'kernel stdin address:  {kernel_ip}:{stdin_port}')
        log.debug(f'kernel stdout address: {kernel_ip}:{stdout_port}')
        return kernel_id

    async def _destroy_kernel(self, kernel_id, reason):
        try:
            cid = self.container_registry[kernel_id]['container_id']
        except KeyError:
            log.warning(f'_destroy_kernel({kernel_id}) kernel missing '
                        '(already dead?)')
            return
        container = self.docker.containers.container(cid)
        runner_task = self.container_registry[kernel_id] \
            .get('runner_task', None)
        if runner_task:
            log.warning(f'_destroy_kernel({kernel_id}) interrupting '
                        'running execution')
            runner_task.cancel()
            await runner_task
        try:
            # stats must be collected before killing it.
            last_stat = (await collect_stats([container]))[0]
            self.container_registry[kernel_id]['last_stat'] = last_stat
            await container.kill()
            # deleting containers will be done in docker monitor routine.
        except DockerError as e:
            if e.status == 500 and 'is not running' in e.message:
                # already dead
                log.warning(f'_destroy_kernel({kernel_id}) kill 500')
                pass
            elif e.status == 404:
                log.warning(f'_destroy_kernel({kernel_id}) kill 404, '
                            'forgetting this kernel')
                self.container_cpu_map.free(
                    self.container_registry[kernel_id]['core_set'])
                del self.container_registry[kernel_id]
                pass
            else:
                log.exception(f'_destroy_kernel({kernel_id}) kill error')
        except:
            log.exception(f'_destroy_kernel({kernel_id}) unexpected error')

    async def _execute_code(self, api_version, kernel_id,
                            mode, code_text, opts):
        work_dir = self.config.volume_root / kernel_id
        # Save kernel-generated output files in a separate sub-directory
        # (to distinguish from user-uploaded files)
        output_dir = work_dir / '.output'

        self.container_registry[kernel_id]['last_used'] = time.monotonic()
        self.container_registry[kernel_id]['num_queries'] += 1

        if 'runner' in self.container_registry[kernel_id]:
            log.debug(f'_execute_code:v{api_version}({kernel_id}) use '
                      'existing runner')
            runner = self.container_registry[kernel_id]['runner']
            await runner.feed_input(code_text)
        else:
            features = set()
            if api_version == 2:
                features |= {'input', 'continuation'}

            # TODO: import "connected" files from S3
            self.container_registry[kernel_id]['initial_file_stats'] \
                = scandir(output_dir, max_upload_size)

            runner = KernelRunner(
                kernel_id,
                mode, opts,
                self.container_registry[kernel_id]['container_ip'],
                self.container_registry[kernel_id]['repl_in_port'],
                self.container_registry[kernel_id]['repl_out_port'],
                self.container_registry[kernel_id]['exec_timeout'],
                features)
            log.debug(f'_execute_code:v{api_version}({kernel_id}) '
                      'start new runner')
            await runner.start(code_text)
            self.container_registry[kernel_id]['runner'] = runner

        try:
            self.container_registry[kernel_id]['runner_task'] = \
                asyncio.Task.current_task()
            result = await runner.get_next_result(api_ver=api_version)
        except asyncio.CancelledError:
            await runner.close()
            del self.container_registry[kernel_id]['runner']
            return
        except:
            log.exception('unexpected error')
            raise
        finally:
            if nmget(self.container_registry, f'{kernel_id}/runner_atsk', None, '/'):
                del self.container_registry[kernel_id]['runner_task']

        try:
            output_files = []

            if result['status'] in ('finished', 'exec-timeout'):

                log.debug(f'_execute_code({kernel_id}) finished/timeout')
                await runner.close()
                del self.container_registry[kernel_id]['runner']

                final_file_stats = scandir(output_dir, max_upload_size)
                if nmget(result, 'options.upload_output_files', True):
                    # TODO: separate as a new task
                    # TODO: replace entry ID ('0') with some different ID
                    initial_file_stats = \
                        self.container_registry[kernel_id]['initial_file_stats']
                    output_files = await upload_output_files_to_s3(
                        initial_file_stats, final_file_stats, '0')
                    output_files = [os.path.relpath(fn, output_dir)
                                    for fn in output_files]

                del self.container_registry[kernel_id]['initial_file_stats']

            if (result['status'] == 'exec-timeout' and
                    kernel_id in self.container_registry):
                # clean up the kernel (maybe cleaned already)
                asyncio.ensure_future(
                    self._destroy_kernel(kernel_id, 'exec-timeout'))

            return {
                'status': result['status'],
                'console': result['console'],
                'options': nmget(result, 'options', None),
                'files': output_files,
            }
        except:
            log.exception('unexpected error')
            raise

    async def _upload_file(self, kernel_id, filename, filedata):
        work_dir = self.config.volume_root / kernel_id
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
        running_kernels = [k for k in self.container_registry.keys()]
        # Below dict should match with sorna.manager.structs.Instance
        inst_info = {
            'id': self.config.inst_id,
            'ip': self.config.agent_ip,
            'addr': f'tcp://{self.config.agent_ip}:{self.config.agent_port}',
            'type': self.config.inst_type,
            'num_kernels': len(running_kernels),
            'max_kernels': self.config.max_kernels,
        }
        try:
            with timeout(1.0):
                log.debug('sending heartbeat')
                await self.events.call.dispatch('instance_heartbeat',
                                                self.config.inst_id,
                                                inst_info, running_kernels,
                                                interval)
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
        kern_stats = {}
        # Attach limits to collected stats.
        # Here, there may be destroyed kernels due to coroutine interleaving.
        for idx, stat in enumerate(stats):
            kern_id = running_kernels[idx]
            if kern_id not in self.container_registry:
                stats[idx] = None
                continue
            if stats[idx] is None:
                continue
            stats[idx]['exec_timeout'] = \
                self.container_registry[kern_id]['exec_timeout']
            stats[idx]['idle_timeout'] = self.config.idle_timeout
            mem_limit = self.container_registry[kern_id]['mem_limit']
            mem_limit_in_kb = utils.readable_size_to_bytes(mem_limit) // 1024
            stats[idx]['mem_limit']   = mem_limit_in_kb
            stats[idx]['num_queries'] = \
                self.container_registry[kern_id]['num_queries']
            last_used = self.container_registry[kern_id]['last_used']
            stats[idx]['idle'] = (time.monotonic() - last_used) * 1000
            kern_stats[kern_id] = stats[idx]
        try:
            with timeout(1.0):
                log.debug('sending stats')
                await self.events.call.dispatch('instance_stats',
                                                self.config.inst_id,
                                                kern_stats, interval)
        except asyncio.TimeoutError:
            log.warning('event dispatch timeout: instance_stats')
        except asyncio.CancelledError:
            pass
        except:
            log.exception('update_stats failure')

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
                if not container_name.startswith('kernel.'):
                    continue
                kernel_id = container_name.split('.', maxsplit=2)[2]
                try:
                    exit_code = evdata['Actor']['Attributes']['exitCode']
                except KeyError:
                    exit_code = '(unknown)'
                log.debug('docker-event: container-terminated: '
                          f'{container_id[:7]} with exit code {exit_code} '
                          f'({kernel_id})')
                asyncio.ensure_future(self.clean_kernel(kernel_id))

    async def clean_kernel(self, kernel_id):
        try:
            container_id = self.container_registry[kernel_id]['container_id']
            container = self.docker.containers.container(container_id)
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
        if kernel_id in restarting_kernels:
            restarting_kernels[kernel_id].set()
        else:
            work_dir = self.config.volume_root / kernel_id
            try:
                shutil.rmtree(work_dir)
            except FileNotFoundError:
                pass
            try:
                kernel_stat = \
                    self.container_registry[kernel_id].get('last_stat', None)
                self.container_cpu_map.free(
                    self.container_registry[kernel_id]['core_set'])
                del self.container_registry[kernel_id]
                with timeout(1.0):
                    await self.events.call.dispatch('kernel_terminated',
                                                    kernel_id, 'destroyed',
                                                    kernel_stat)
            except KeyError:
                pass
            except asyncio.TimeoutError:
                log.warning('event dispatch timeout: kernel_terminated')
            if kernel_id in blocking_cleans:
                blocking_cleans[kernel_id].set()

    async def clean_old_kernels(self):
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
                blocking_cleans[kern_id] = asyncio.Event()
        for kern_id in kern_ids:
            task = asyncio.ensure_future(
                self._destroy_kernel(kern_id, 'agent-termination'))
            tasks.append(task)
        await asyncio.gather(*tasks)
        if blocking:
            waiters = [blocking_cleans[kern_id].wait() for kern_id in kern_ids]
            await asyncio.gather(*waiters)
            for kern_id in kern_ids:
                del blocking_cleans[kern_id]


def main():
    global lang_aliases

    parser = configargparse.ArgumentParser()
    parser.add('--agent-ip-override', type=ipaddr,
               default=None, dest='agent_ip',
               help='Manually set the IP address of this agent to report to the '
                    'manager.')
    parser.add('--agent-port', type=port_no, default=6001,
               help='The port number to listen on.')
    parser.add('--redis-addr', type=host_port_pair,
               default=HostPortPair(ip_address('127.0.0.1'), 6379),
               help='The host:port pair of the Redis (agent registry) server.')
    parser.add('--event-addr', type=host_port_pair,
               default=HostPortPair(ip_address('127.0.0.1'), 5002),
               help='The host:port pair of the Gateway event server.')
    parser.add('--exec-timeout', type=positive_int, default=180,
               help='The maximum period of time allowed for kernels to run user '
                    'codes.')
    parser.add('--idle-timeout', type=positive_int, default=600,
               help='The maximum period of time allowed for kernels to wait '
                    'further requests.')
    parser.add('--max-kernels', type=positive_int, default=1,
               help='Set the maximum number of kernels running in parallel.')
    parser.add('--debug', action='store_true', default=False,
               help='Enable more verbose logging.')
    parser.add('--kernel-aliases', type=str, default=None,
               help='The filename for additional kernel aliases')
    parser.add('--volume-root', type=Path,
               default=Path('/var/lib/sorna-volumes'),
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
    if not args.redis_addr:
        args.redis_addr = ('sorna-manager.lablup', 6379)

    assert args.volume_root.exists()
    assert args.volume_root.is_dir()

    ddagent = DummyDatadog()
    sentry = DummySentry()
    if datadog_available and args.datadog_api_key:
        datadog.initialize(
            api_key=args.datadog_api_key,
            app_key=args.datadog_app_key)
        ddagent = datadog
    if raven_available and args.raven_uri:
        sentry = raven.Client(args.raven_uri)

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

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()
    log.info(f'Sorna Agent {VERSION}')
    log.info(f'runtime: {utils.env_info()}')

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
    stats_task = None
    timer_task = None
    monitor_handle_task = None
    monitor_fetch_task = None
    events = None

    async def initialize():
        nonlocal docker, agent, server, events
        nonlocal hb_task, stats_task, timer_task
        nonlocal monitor_handle_task, monitor_fetch_task
        args.inst_id = await utils.get_instance_id()
        args.inst_type = await utils.get_instance_type()
        if not args.agent_ip:
            args.agent_ip = await utils.get_instance_ip()
        log.info(f'myself: {args.inst_id} ({args.inst_type}), '
                 f'ip: {args.agent_ip}')
        log.info('using gateway event server at '
                 f'tcp://{args.event_addr}')

        # Connect to the events server.
        event_addr = f'tcp://{args.event_addr}'
        try:
            with timeout(5.0):
                events = await aiozmq.rpc.connect_rpc(connect=event_addr)
                events.transport.setsockopt(zmq.LINGER, 50)
                await events.call.dispatch('instance_started', args.inst_id)
        except asyncio.TimeoutError:
            events.close()
            await events.wait_closed()
            log.critical('cannot connect to the manager.')
            raise SystemExit(1)

        # Initialize Docker
        docker = Docker()
        docker_version = await docker.version()
        log.info('running with Docker {0} with API {1}'
                 .format(docker_version['Version'],
                         docker_version['ApiVersion']))

        # Start RPC server.
        agent_addr = 'tcp://*:{}'.format(args.agent_port)
        agent = AgentRPCServer(docker, args, events, ddagent, sentry, loop=loop)
        await agent.init()
        server = await aiozmq.rpc.serve_rpc(agent, bind=agent_addr)
        server.transport.setsockopt(zmq.LINGER, 200)
        log.info('serving at {0}'.format(agent_addr))

        # Send the first heartbeat.
        hb_task    = loop.create_task(heartbeat_timer(agent))
        stats_task = loop.create_task(stats_timer(agent))
        timer_task = loop.create_task(cleanup_timer(agent))
        monitor_fetch_task  = loop.create_task(agent.fetch_docker_events())
        monitor_handle_task = loop.create_task(agent.monitor())
        await asyncio.sleep(0.01)

    async def shutdown():
        log.info('shutting down...')

        # Stop receiving further requests.
        server.close()
        await server.wait_closed()

        # Clean all kernels.
        await agent.clean_all_kernels(blocking=True)
        log.debug('shutdown: kernel cleanup done.')

        # Stop timers.
        hb_task.cancel()
        stats_task.cancel()
        timer_task.cancel()
        await hb_task
        await stats_task
        await timer_task

        # Stop event monitoring.
        monitor_fetch_task.cancel()
        monitor_handle_task.cancel()
        await monitor_fetch_task
        await monitor_handle_task
        try:
            await docker.events.stop()
        except:
            pass
        docker.session.close()
        log.debug('shutdown: docker cleanup done.')

        try:
            with timeout(1.0):
                await events.call.dispatch('instance_terminated',
                                           args.inst_id, 'destroyed')
        except asyncio.TimeoutError:
            log.warning('event dispatch timeout: instance_terminated')
        await asyncio.sleep(0.01)
        events.close()
        await events.wait_closed()

        # Finalize.
        await agent.shutdown()
        await loop.shutdown_asyncgens()

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
