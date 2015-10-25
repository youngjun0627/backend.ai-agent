#! /usr/bin/env python3

import asyncio, zmq, aiozmq
import argparse
import docker
import logging, logging.config
import aiobotocore
from namedlist import namedtuple
import os, os.path
import signal
import simplejson as json
import shutil
import sys
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

@asyncio.coroutine
def heartbeat(loop, agent_addr, manager_addr):
    '''
    Send a heartbeat mesasge to the master (sorna.manager).
    This message includes my socket information so that the manager can
    register or update its instance registry.
    '''
    global container_registry
    while True:
        # TODO: attach the list of currently running kernels
        # TODO: add extra info (e.g., capacity available)
        yield from asyncio.sleep(3, loop=loop)

@asyncio.coroutine
def create_kernel(loop, docker_cli, lang):
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
        #'addr': 'tcp://{0}:{1}'.format('127.0.0.1', kernel_host_port),
        'ip': kernel_ip,
        'port': 2001,
        'hostport': kernel_host_port,
    }
    return kernel_id

@asyncio.coroutine
def destroy_kernel(loop, docker_cli, kernel_id):
    global container_registry
    container_id = container_registry[kernel_id]['container_id']
    docker_cli.kill(container_id)  # forcibly shut-down the container
    docker_cli.remove_container(container_id)
    work_dir = os.path.join(volume_root, kernel_id)
    shutil.rmtree(work_dir)
    del container_registry[kernel_id]

def scandir(root):
    file_stats = dict()
    for entry in os.scandir(root):
        if entry.is_file():
            path = os.path.join(root, entry.name)
            file_stats[path] = entry.stat().st_mtime
        elif entry.is_dir():
            file_stats.update(scandir(os.path.join(root, entry.name)))
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

@asyncio.coroutine
def execute_code(loop, docker_cli, entry_id, kernel_id, cell_id, code):
    container_id = container_registry[kernel_id]['container_id']
    work_dir = os.path.join(volume_root, kernel_id)
    # TODO: import "connected" files from S3
    initial_file_stats = scandir(work_dir)

    container_addr = container_registry[kernel_id]['addr']
    container_sock = yield from aiozmq.create_zmq_stream(zmq.REQ,
            connect=container_addr, loop=loop)
    container_sock.write([cell_id.encode('ascii'), code.encode('utf8')])

    # Execute with a 4 second timeout.
    try:
        result_data = yield from asyncio.wait_for(container_sock.read(),
                                                  timeout=4, loop=loop)
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
                resp = yield from client.put_object(Bucket=s3_bucket,
                                                    Key=key,
                                                    Body=content,
                                                    ACL='public-read')
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
        yield from destroy_kernel(loop, docker_cli, kernel_id)
        raise exc  # re-raise so that the loop return failure.
    finally:
        container_sock.close()

@asyncio.coroutine
def run_agent(loop, server_sock, manager_addr, agent_addr):
    global container_registry, docker_addr

    # Resolve the master address
    if manager_addr is None:
        manager_addr = 'tcp://sorna-manager.lablup:5001'

    # Initialize docker subsystem
    docker_cli = docker_init()
    container_registry.clear()

    # Send the first heartbeat.
    asyncio.async(heartbeat(loop, agent_addr, manager_addr), loop=loop)
    yield from asyncio.sleep(0, loop=loop)

    # Then start running the agent loop.
    while True:
        try:
            request_data = yield from server_sock.read()
        except aiozmq.stream.ZmqStreamClosed:
            break
        request = Message.decode(request_data[0])
        resp = Message()

        if request['req_type'] == AgentRequestTypes.CREATE_KERNEL:

            log.info('CREATE_KERNEL ({})'.format(request['lang']))
            if request['lang'] in supported_langs:
                try:
                    kernel_id = yield from create_kernel(loop, docker_cli, request['lang'])
                    # TODO: (asynchronously) check if container is running okay.
                except Exception as exc:
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['body'] = type(exc).__name__
                    log.exception(exc)
                else:
                    resp['reply'] = SornaResponseTypes.SUCCESS
                    resp['kernel_id'] = kernel_id
            else:
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['body'] = 'Unsupported kernel language.'

        elif request['req_type'] == AgentRequestTypes.DESTROY_KERNEL:

            log.info('DESTROY_KERNEL ({})'.format(request['kernel_id']))
            if request['kernel_id'] in container_registry:
                try:
                    yield from destroy_kernel(loop, docker_cli, request['kernel_id'])
                except Exception as exc:
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['body'] = type(exc).__name__
                    log.exception(exc)
                else:
                    resp['reply'] = SornaResponseTypes.SUCCESS
            else:
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['body'] = 'No such kernel.'

        elif request['req_type'] == AgentRequestTypes.EXECUTE:

            log.info('EXECUTE ({}, {})'.format(request['kernel_id'],
                                               request['cell_id']))
            if request['kernel_id'] in container_registry:
                try:
                    result = yield from execute_code(loop, docker_cli,
                                                     request['entry_id'],
                                                     request['kernel_id'],
                                                     request['cell_id'],
                                                     request['code'])
                except Exception as exc:
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['body'] = type(exc).__name__
                    log.exception(exc)
                else:
                    reps['reply'] = SornaResponseTypes.SUCCESS
                    resp['body'] = result
            else:
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['body'] = 'Could not find such kernel.'
        else:
            assert False, 'Invalid kernel request type.'

        server_sock.write([resp.encode()])
        yield from server_sock.drain()


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--agent-port', type=int, default=6001)
    argparser.add_argument('--manager-addr', type=str, default=None)
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
            # TODO: refactor sorna.logging
            #'logstash': {
            #    'class': 'sorna.logging.LogstashHandler',
            #    'level': 'INFO',
            #    'endpoint': 'tcp://logger.lablup:2121',
            #},
        },
        'loggers': {
            'sorna': {
                #'handlers': ['console', 'logstash'],
                'handlers': ['console'],
                'level': 'DEBUG',
            },
        },
    })
    agent_addr = 'tcp://*:{0}'.format(args.agent_port)
    loop = asyncio.get_event_loop()
    server_sock = loop.run_until_complete(aiozmq.create_zmq_stream(zmq.REP, bind=agent_addr, loop=loop))
    log.info('serving at {0}'.format(agent_addr))
    try:
        asyncio.async(run_agent(loop, server_sock, args.manager_addr, agent_addr), loop=loop)
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
