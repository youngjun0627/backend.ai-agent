#! /usr/bin/env python3

import asyncio, zmq, aiozmq, aioredis
import aiobotocore
import argparse
import botocore
from datetime import datetime, timedelta
from dateutil.tz import tzutc
import docker
from itertools import chain
import logging, logging.config
from namedlist import namedtuple
import os, os.path
import signal
import simplejson as json
import shutil
import sys
import urllib.parse
from sorna import utils, defs
from sorna.proto import Message, odict, generate_uuid
from sorna.proto.msgtypes import *

log = logging.getLogger('sorna.agent.server')
log.setLevel(logging.DEBUG)
container_registry = dict()
volume_root = '/var/lib/sorna-volumes'
supported_langs = frozenset(['python27', 'python34'])
# the names of following AWS variables follow boto3 convention.
s3_access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'dummy-access-key')
s3_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'dummy-secret-key')
s3_region = os.environ.get('AWS_REGION', 'ap-northeast-1')
s3_bucket = os.environ.get('AWS_S3_BUCKET', 'codeonweb')
max_execution_time = 4  # in seconds
max_upload_size = 5 * 1024 * 1024  # 5 MB
max_kernels = 1
inst_id = None
agent_addr = None
agent_ip = None
agent_port = 6001
inst_type = None
manager_ip = None

def docker_init():
    docker_tls_verify = int(os.environ.get('DOCKER_TLS_VERIFY', '0'))
    docker_addr = os.environ.get('DOCKER_HOST', 'unix://var/run/docker.sock')
    if docker_tls_verify == 1:
        docker_cert_path = os.environ['DOCKER_CERT_PATH']
        tls_config = docker.tls.TLSConfig(
                verify=docker_tls_verify,
                ca_cert=os.path.join(docker_cert_path, 'ca.pem'),
                client_cert=(os.path.join(docker_cert_path, 'cert.pem'),
                             os.path.join(docker_cert_path, 'key.pem')),
                assert_hostname=False,
        )
    else:
        tls_config = None
    if docker_addr.startswith('tcp://') and docker_tls_verify == 1:
        docker_addr = docker_addr.replace('tcp://', 'https://')
    return docker.Client(docker_addr, tls=tls_config, timeout=3)

async def heartbeat(loop, interval=3.0):
    '''
    Record my status information to the manager database (Redis).
    This information automatically expires after 2x interval, so that failure
    of executing this method automatically removes the instance from the
    manager database.
    '''
    global container_registry
    global inst_id, inst_type, agent_ip
    if not inst_id:
        inst_id = await utils.get_instance_id()
    if not inst_type:
        inst_type = await utils.get_instance_type()
    if not agent_ip:
        agent_ip = await utils.get_instance_ip()
    log.info('myself: {} ({}), ip: {}'.format(inst_id, inst_type, agent_ip))
    log.info('using manager at {}'.format(manager_ip))
    while True:
        try:
            redis = await asyncio.wait_for(
                    aioredis.create_redis((manager_ip, 6379),
                                          encoding='utf8',
                                          db=defs.SORNA_INSTANCE_DB,
                                          loop=loop), timeout=1)
        except asyncio.TimeoutError:
            log.warn('could not contact manager redis.')
        else:
            total_cpu_shares = sum(c['cpu_shares'] for c in container_registry.values())
            running_kernels = [k for k in container_registry.keys()]
            state = {
                'status': 'ok',
                'id': inst_id,
                'addr': 'tcp://{}:{}'.format(agent_ip, agent_port),
                'type': inst_type,
                'used_cpu': total_cpu_shares,
                'num_kernels': len(running_kernels),
                'max_kernels': max_kernels,
            }
            my_kernels_key = '{}.kernels'.format(inst_id)
            pipe = redis.multi_exec()
            pipe.hmset(inst_id, *chain.from_iterable((k, v) for k, v in state.items()))
            pipe.delete(my_kernels_key)
            if running_kernels:
                pipe.sadd(my_kernels_key, running_kernels[0], *running_kernels[1:])
            # Create a "shadow" key that actually expires.
            # This allows access to values upon expiration events.
            pipe.set('shadow:' + inst_id, '')
            pipe.expire('shadow:' + inst_id, float(interval * 2))
            await pipe.execute()
            await redis.quit()
        await asyncio.sleep(interval, loop=loop)

async def create_kernel(loop, docker_cli, lang):
    kernel_id = generate_uuid()
    assert kernel_id not in container_registry
    work_dir = os.path.join(volume_root, kernel_id)
    os.makedirs(work_dir)
    result = docker_cli.create_container('kernel-{}'.format(lang),
                                         name='kernel.{}.{}'.format(lang, kernel_id),
                                         cpu_shares=1024, # full share
                                         ports=[(2001, 'tcp')],
                                         volumes=['/home/work'],
                                         host_config=docker_cli.create_host_config(
                                            mem_limit='128m',
                                            memswap_limit=0,
                                            port_bindings={2001: ('127.0.0.1', )},
                                            binds={
                                                work_dir: {'bind': '/home/work', 'mode': 'rw'},
                                            }),
                                         tty=False)
    container_id = result['Id']
    docker_cli.start(container_id)
    container_info = docker_cli.inspect_container(container_id)
    # We can connect to the container either using
    # tcp://kernel_ip:2001 (direct) or tcp://127.0.0.1:kernel_host_port (NAT'ed)
    kernel_ip = container_info['NetworkSettings']['IPAddress']
    kernel_host_port = container_info['NetworkSettings']['Ports']['2001/tcp'][0]['HostPort']
    container_registry[kernel_id] = {
        'lang': lang,
        'container_id': container_id,
        'addr': 'tcp://{0}:{1}'.format(kernel_ip, 2001),
        'ip': kernel_ip,
        'port': 2001,
        'hostport': kernel_host_port,
        'cpu_shares': 1024,
        'last_used': datetime.now(tzutc()),
    }
    return kernel_id

async def destroy_kernel(loop, docker_cli, kernel_id):
    global container_registry
    global inst_id
    if not inst_id:
        inst_id = await utils.get_instance_id()
    container_id = container_registry[kernel_id]['container_id']
    docker_cli.kill(container_id)  # forcibly shut-down the container
    docker_cli.remove_container(container_id)
    work_dir = os.path.join(volume_root, kernel_id)
    shutil.rmtree(work_dir)
    del container_registry[kernel_id]
    try:
        redis = await asyncio.wait_for(
                aioredis.create_redis((manager_ip, 6379),
                                      encoding='utf8',
                                      db=defs.SORNA_INSTANCE_DB,
                                      loop=loop), loop=loop, timeout=1)
    except asyncio.TimeoutError:
        log.warn('could not contact manager redis.')
    else:
        pipe = redis.multi_exec()
        pipe.hincrby(inst_id, 'num_kernels', -1)
        pipe.srem(inst_id + '.kernels', kernel_id)
        await pipe.execute()
        await redis.quit()

def scandir(root):
    file_stats = dict()
    for entry in os.scandir(root):
        if entry.is_file():
            stat = entry.stat()
            # Skip too large files!
            if stat.st_size > max_upload_size:
                continue
            file_stats[entry.path] = stat.st_mtime
        elif entry.is_dir():
            file_stats.update(scandir(entry.path))
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

async def execute_code(loop, docker_cli, entry_id, kernel_id, cell_id, code):
    container_id = container_registry[kernel_id]['container_id']
    work_dir = os.path.join(volume_root, kernel_id)
    # TODO: import "connected" files from S3
    initial_file_stats = scandir(work_dir)

    container_addr = container_registry[kernel_id]['addr']
    container_sock = await aiozmq.create_zmq_stream(zmq.REQ,
            connect=container_addr, loop=loop)
    container_sock.transport.setsockopt(zmq.LINGER, 50)
    container_sock.write([cell_id.encode('ascii'), code.encode('utf8')])

    # Execute with a 4 second timeout.
    try:
        result_data = await asyncio.wait_for(container_sock.read(),
                                             timeout=max_execution_time,
                                             loop=loop)
        result = json.loads(result_data[0])

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
                    resp = await client.put_object(Bucket=s3_bucket,
                                                   Key=key,
                                                   Body=content,
                                                   ACL='public-read')
                except botocore.exceptions.ClientError as exc:
                    log.exception(exc)
            client.close()
        return odict(
            ('stdout', result['stdout']),
            ('stderr', result['stderr']),
            ('exceptions', result['exceptions']),
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
    while True:
        now = datetime.now(tzutc())
        # clone keys to avoid "dictionary size changed during iteration" error.
        keys = tuple(container_registry.keys())
        for kern_id in keys:
            if now - container_registry[kern_id]['last_used'] > timedelta(seconds=60):
                log.info('destroying kernel {} as clean-up'.format(kern_id))
                await destroy_kernel(loop, docker_cli, kern_id)
        await asyncio.sleep(10, loop=loop)

async def run_agent(loop, server_sock, manager_addr):
    global container_registry
    global inst_id, agent_ip, inst_type, manager_ip

    inst_id = await utils.get_instance_id()
    inst_type = await utils.get_instance_type()
    agent_ip = await utils.get_instance_ip()

    # Resolve the master address
    if manager_addr is None:
        manager_addr = 'tcp://sorna-manager.lablup:5001'
    manager_ip = urllib.parse.urlparse(manager_addr).hostname

    # Initialize docker subsystem
    docker_cli = docker_init()
    container_registry.clear()

    # Send the first heartbeat.
    asyncio.ensure_future(heartbeat(loop), loop=loop)
    asyncio.ensure_future(cleanup_timer(loop, docker_cli), loop=loop)
    await asyncio.sleep(0, loop=loop)

    # Then start running the agent loop.
    while True:
        try:
            request_data = await server_sock.read()
        except aiozmq.stream.ZmqStreamClosed:
            break
        request = Message.decode(request_data[0])
        resp = Message()

        if request['action'] == AgentRequestTypes.CREATE_KERNEL:

            log.info('CREATE_KERNEL ({})'.format(request['lang']))
            if request['lang'] in supported_langs:
                try:
                    kernel_id = await create_kernel(loop, docker_cli, request['lang'])
                    # TODO: (asynchronously) check if container is running okay.
                except Exception as exc:
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['cause'] = type(exc).__name__
                    log.exception(exc)
                else:
                    resp['reply'] = SornaResponseTypes.SUCCESS
                    resp['kernel_id'] = kernel_id
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
                    resp['cause'] = type(exc).__name__
                    log.exception(exc)
                else:
                    resp['reply'] = SornaResponseTypes.SUCCESS
            else:
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['cause'] = 'No such kernel.'

        elif request['action'] == AgentRequestTypes.EXECUTE:

            log.info('EXECUTE ({}, {})'.format(request['kernel_id'],
                                               request['cell_id']))
            if request['kernel_id'] in container_registry:
                try:
                    result = await execute_code(loop, docker_cli,
                                                request['entry_id'],
                                                request['kernel_id'],
                                                request['cell_id'],
                                                request['code'])
                except Exception as exc:
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['cause'] = type(exc).__name__
                    log.exception(exc)
                else:
                    reps['reply'] = SornaResponseTypes.SUCCESS
                    resp['result'] = result
                    container_registry[request['kernel_id']]['last_used'] \
                            = datetime.now(tzutc())
            else:
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['cause'] = 'Could not find such kernel.'
        else:
            assert False, 'Invalid kernel request type.'

        server_sock.write([resp.encode()])
        await server_sock.drain()


def main():
    global max_kernels
    global agent_addr, agent_port

    argparser = argparse.ArgumentParser()
    argparser.add_argument('--agent-port', type=int, default=agent_port)
    argparser.add_argument('--manager-addr', type=str, default=None)
    argparser.add_argument('--max-kernels', type=int, default=1)
    args = argparser.parse_args()

    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'DEBUG',
                'stream': 'ext://sys.stdout',
            },
            'null': {
                'class': 'logging.NullHandler',
            },
            'logstash': {
                'class': 'sorna.logging.LogstashHandler',
                'level': 'INFO',
                'endpoint': 'tcp://logger.lablup:2121',
            },
        },
        'loggers': {
            'sorna': {
                'handlers': ['console', 'logstash'],
                'level': 'DEBUG',
            },
        },
    })
    agent_addr = 'tcp://*:{0}'.format(args.agent_port)
    agent_port = args.agent_port
    max_kernels = args.max_kernels

    loop = asyncio.get_event_loop()
    server_sock = loop.run_until_complete(aiozmq.create_zmq_stream(zmq.REP, bind=agent_addr, loop=loop))
    server_sock.transport.setsockopt(zmq.LINGER, 50)
    log.info('serving at {0}'.format(agent_addr))
    try:
        asyncio.ensure_future(run_agent(loop, server_sock, args.manager_addr), loop=loop)
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        server_sock.close()
        for t in asyncio.Task.all_tasks():
            if not t.done():
                t.cancel()
        try:
            loop.run_until_complete(asyncio.sleep(0, loop=loop))
        except asyncio.CancelledError:
            pass
    finally:
        loop.close()
        log.info('exit.')


if __name__ == '__main__':
    main()
