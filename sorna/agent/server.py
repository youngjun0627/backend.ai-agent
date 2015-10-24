#! /usr/bin/env python3

import asyncio, zmq, aiozmq
import argparse
import docker
import logging, logging.config
import aiobotocore
from namedlist import namedtuple
import signal
import sys
from sorna.proto import Message, odict, generate_uuid
from sorna.proto.msgtypes import *

log = logging.getLogger('sorna.agent.server')
log.setLevel(logging.DEBUG)
container_registry = dict()
container_ports_available = set(p for p in range(2001, 2100))
volume_root = '/var/lib/sorna-volumes'
docker_addr = 'tcp://127.0.0.1:2375'
supported_langs = frozenset(['python27', 'python34'])

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
def run_agent(loop, server_sock, manager_addr, agent_addr):
    global container_registry

    # Resolve the master address
    if manager_addr is None:
        manager_addr = 'tcp://sorna-manager.lablup:5001'

    # Initialize docker subsystem
    cli = docker.Client(docker_addr, timeout=1)
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

                kernel_id = generate_uuid()
                kernel_port = container_ports_available.pop()
                work_dir = os.path.join(volume_root, kernel_id)
                os.makedirs(work_dir)
                container_id = cli.create_container('kernel-{}'.format(request['lang']),
                                                    mem_limit='128m',
                                                    memswap_limit=0,
                                                    cpu_shares=1024, # full share
                                                    ports=[2001],
                                                    host_config=docker.utils.create_host_config(
                                                       port_bindings={2001: ('127.0.0.1', kernel_port)},
                                                       binds={
                                                           '/home/work': {'bind': work_dir, 'mode': 'rw'},
                                                       }),
                                                    tty=False)
                container_info = cli.inspect_container(container_id)
                kernel_ip = container_info['NetworkSettings']['IPAddress']
                container_registry[kernel_id] = {
                    'lang': request['lang'],
                    'container_id': container_id,
                    'addr': 'tcp://{0}:{1}'.format(kernel_ip, kernel_port),
                    'ip': kernel_ip,
                    'port': kernel_port,
                }
                # TODO: (asynchronously) check if container is running okay.
                resp['reply'] = SornaResponseTypes.SUCCESS
                resp['kernel_id'] = kernel_id

            else:
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['body'] = 'Unsupported kernel language.'

        elif request['req_type'] == AgentRequestTypes.DESTROY_KERNEL:

            log.info('DESTROY_KERNEL ({})'.format(request['kernel_id']))
            if request['kernel_id'] in container_registry:

                kernel_id = request['kernel_id']
                kernel_port = container_registry[kernel_id]['port']
                container_id = container_registry[kernel_id]['container_id']
                # TODO: use graceful shutdown using aiodocker?
                cli.kill(container_id)
                cli.remove_container(container_id)
                work_dir = os.path.join(volume_root, kernel_id)
                shutil.rmtree(work_dir)
                container_ports_available.add(kernel_port)
                del container_registry[kernel_id]

            else:
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['body'] = 'No such kernel.'

        elif request['req_type'] == AgentRequestTypes.EXECUTE:

            log.info('EXECUTE')
            try:
                kernel_id = request['kernel_id']
                container_addr = container_registry[kernel_id]['addr']
                #redirect_output = request.get('redirect_output', False)
            except KeyError:
                resp['reply'] = SornaResponseTypes.INVALID_INPUT
                resp['body'] = 'Could not find such kernel.'
            else:
                # TODO: read the file list in container /home/work volume

                container_sock = yield from aiozmq.create_zmq_stream(zmq.REQ,
                        connect=container_addr, loop=loop)
                container_sock.write([request['cell_id'], request['code']])

                # Execute with a 4 second timeout.
                try:
                    read_task = asyncio.Task(container_sock.read())
                    result_data = yield from asyncio.wait_for(read_task,
                                                              timeout=4, loop=loop)

                    # TODO: check updated files in container /home/work volume.
                    # TODO: upload updated files to s3
                    result = json.loads(result_data[0])
                    resp['reply'] = SornaResponseTypes.SUCCESS
                    resp['body'] = odict(
                        ('eval_result', result['eval_result']),
                        ('stdout', result['stdout']),
                        ('stderr', result['stderr']),
                        ('exceptions', result['exceptions']),
                        ('files', []),
                    )
                    # TODO: handle connection error
                except asyncio.TimeoutError:
                    read_task.cancel()
                    resp['reply'] = SornaResponseTypes.FAILURE
                    resp['body'] = 'TimeoutError'
                finally:
                    container_sock.close()
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
