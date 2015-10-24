#! /usr/bin/env python3

from sorna.proto import Message, odict, generate_uuid
from sorna.proto.msgtypes import AgentRequestTypes
import asyncio, zmq, aiozmq
import argparse
import logging
from namedlist import namedtuple
import signal
from sorna.proto import Message, odict
from sorna.proto.msgtypes import ManagerRequestTypes, ManagerResponseTypes

ExceptionInfo = namedtuple('ExceptionInfo', [
    'exc',
    ('args', tuple()),
    ('raised_before_exec', False),
    ('traceback', None),
])

log = logging.getLogger(__name__)
container_registry = dict()


@asyncio.coroutine
def heartbeat(loop, agent_addr, manager_addr):
    '''
    Send a heartbeat mesasge to the master (sorna.manager).
    This message includes my socket information so that the manager can
    register or update its instance registry.
    '''
    global container_registry
    while True:
        msg = Message()
        msg['agent_addr'] = agent_addr
        # TODO: attach the list of currently running kernels
        # TODO: add extra info (e.g., capacity available)
        master_sock = yield from aiozmq.create_zmq_stream(zmq.REQ,
                connect=manager_addr, loop=loop)
        master_sock.write([msg.encode()])
        try:
            _ = yield from asyncio.wait_for(master_sock.read(), timeout=1, loop=loop)
        except asyncio.TimeoutError:
            log.error('Master does not respond to heartbeat.')
            # TODO: terminate myself if master does not respond for a long time.
        finally:
            master_sock.close()
        yield from asyncio.sleep(3)

@asyncio.coroutine
def run_agent(loop, server_sock, manager_addr):
    global container_registry

    # Resolve the master address
    if manager_addr is None:
        # TODO: implement
        manager_addr = 'tcp://{0}:{1}'

    # Initialize docker subsystem
    # TODO: implement
    # TODO: if docker is already running, init from existing container list?
    container_registry.clear()

    # Send the first heartbeat.
    asyncio.async(heartbeat(loop, agent_addr, manager_addr), loop=loop)
    yield from asyncio.sleep(0)

    # Then start running the agent loop.
    while True:
        try:
            request_data = yield from server_sock.read()
        except aiozmq.stream.ZmqStreamClosed:
            break
        request = Message.decode(request_data[0])
        resp = Message()

        # Reverse-direction heartbeat (not used yet)
        if request['req_type'] == AgentRequestTypes.HEARTBEAT:

            log.info('HEARTBEAT')
            resp['reply'] = AgentResponseTypes.SUCCESS
            resp['body'] = request['body']

        elif request['req_type'] == AgentRequestTypes.CREATE_KERNEL:

            log.info('CREATE_KERNEL ({})'.format(request['lang']))
            if request['lang'] == 'python34':
                resp['reply'] = AgentResponseTypes.SUCCESS
                # TODO: create a container with Python 3.4 image
                # TODO: add to container_registry
                # TODO: return the kernel ID
                container_registry[kernel_id] = {
                    'addr': '...',
                    'lang': request['lang'],
                }
            elif request['lang'] == 'python27':
                resp['reply'] = AgentResponseTypes.SUCCESS
                # TODO: create a container with Python 2.7 image
                # TODO: add to container_registry
                # TODO: return the kernel ID
                container_registry[kernel_id] = {
                    'addr': '...',
                    'lang': request['lang'],
                }
            else:
                resp['reply'] = AgentResponseTypes.INVALID_INPUT
                resp['body'] = 'Unsupported kernel language.'

        elif request['req_type'] == AgentRequestTypes.DESTROY_KERNEL:

            log.info('DESTROY_KERNEL ({})'.format(request['kernel_id']))
            # TODO: destroy the container
            # TODO: remove from container_registry

        elif request['req_type'] == AgentRequestTypes.EXECUTE:

            log.info('EXECUTE')
            try:
                container_addr = container_registry[request['kernel_id']]['addr']
                #redirect_output = request.get('redirect_output', False)
            except KeyError:
                resp['reply'] = AgentResponseTypes.INVALID_INPUT
                resp['body'] = 'Could not find such kernel.'
            else:
                # TODO: read the file list in container /home/work volume

                container_sock = yield from aiozmq.create_zmq_stream(zmq.REQ,
                        connect=container_addr, loop=loop)
                container_sock.write([request['cell_id'], request['code']])

                # Execute with a 4 second timeout.
                try:
                    result_data = yield from asyncio.wait_for(container_sock.read(),
                                                              timeout=4, loop=loop)

                    # TODO: check updated files in container /home/work volume.
                    # TODO: upload updated files to s3
                    result = json.loads(result_data[0])
                    resp['reply'] = AgentResponseTypes.SUCCESS
                    resp['body'] = odict(
                        ('eval_result': result['eval_result']),
                        ('stdout': result['stdout']),
                        ('stderr': result['stderr']),
                        ('exceptions': result['exceptions']),
                        ('files': []),
                    )
                except asyncio.TimeoutError:
                    resp['reply'] = AgentResponseTypes.FAILED
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

    def handle_exit():
        raise SystemExit()

    asyncio.set_event_loop_policy(aiozmq.ZmqEventLoopPolicy())
    loop = asyncio.get_event_loop()
    server_sock = loop.run_until_complete(aiozmq.create_zmq_stream(zmq.REP, bind=agent_addr, loop=loop))
    loop.add_signal_handler(signal.SIGTERM, handle_exit)

    log.info('serving at {1}'.format(agent_addr))
    try:
        asyncio.async(run_agent(loop, server_sock, args.manager_addr), loop=loop)
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        server.close()
        for t in asyncio.Task.all_tasks():
            t.cancel()
        try:
            loop._run_once()
        except asyncio.CancelledError:
            pass
    finally:
        loop.close()
        log.info('exit.')


if __name__ == '__main__':
    main()
