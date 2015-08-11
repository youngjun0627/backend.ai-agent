#! /usr/bin/env python3

from sorna.proto import Namespace, encode, decode
from sorna.proto.msgtypes import AgentRequestTypes
import asyncio, zmq, aiozmq
import argparse
import signal
import uuid
from . import Kernel

@asyncio.coroutine
def handle_request(loop, server, kernel):
    while True:
        try:
            req_data = yield from server.read()
        except aiozmq.stream.ZmqStreamClosed:
            break
        req = decode(req_data[0])
        resp = Namespace()

        if req.req_type == AgentRequestTypes.HEARTBEAT:
            print('[{0}] HEARTBEAT'.format(kernel.kernel_id))
            resp.body = req.body
        elif req.req_type == AgentRequestTypes.SOCKET_INFO:
            print('[{0}] SOCKET_INFO'.format(kernel.kernel_id))
            resp.body = {
                'stdin': 'tcp://{0}:{1}'.format(kernel.ip, kernel.stdin_port),
                'stdout': 'tcp://{0}:{1}'.format(kernel.ip, kernel.stdout_port),
                'stderr': 'tcp://{0}:{1}'.format(kernel.ip, kernel.stderr_port),
            }
        elif req.req_type == AgentRequestTypes.EXECUTE:
            print('[{0}] EXECUTE'.format(kernel.kernel_id))
            yield from kernel.send_refresh()
            request = req.body
            redirect_output = hasattr(request, 'redirect_output') and request.redirect_output
            exec_result, exceptions, output = kernel.execute_code(request.cell_id,
                                                                  request.code,
                                                                  redirect_output)
            if not (isinstance(exec_result, str) or exec_result is None):
                exec_result = str(exec_result)
            result = {
                'eval_result': exec_result,
                'exceptions': ['{0!r}'.format(e) for e in exceptions],
                'stdout': output[0].rstrip('\n') if redirect_output else None,
                'stderr': output[1].rstrip('\n') if redirect_output else None,
            }
            resp.body = result
        else:
            assert False, 'Invalid kernel request type.'

        server.write([encode(resp)])


def main():

    argparser = argparse.ArgumentParser()
    argparser.add_argument('--kernel-id', default=None)
    argparser.add_argument('--agent-port', type=int, default=6001)
    argparser.add_argument('--manager-addr', type=str, default='127.0.0.1')
    args = argparser.parse_args()

    kernel_id = args.kernel_id if args.kernel_id else uuid.uuid4().hex
    kernel = Kernel('127.0.0.1', kernel_id, args.manager_addr)  # for testing
    agent_addr = 'tcp://*:{0}'.format(args.agent_port)

    def handle_exit():
        raise SystemExit()

    asyncio.set_event_loop_policy(aiozmq.ZmqEventLoopPolicy())
    loop = asyncio.get_event_loop()
    print('[{0}] Serving at {1}'.format(kernel_id, agent_addr))
    server = loop.run_until_complete(aiozmq.create_zmq_stream(zmq.REP, bind=agent_addr, loop=loop))
    loop.add_signal_handler(signal.SIGTERM, handle_exit)
    try:
        asyncio.async(handle_request(loop, server, kernel), loop=loop)
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
        print('Exit.')


if __name__ == '__main__':
    main()
