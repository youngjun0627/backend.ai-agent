#! /usr/bin/env python3

import asyncio
import unittest, unittest.mock
import docker
import json
import os
import signal
import subprocess
import zmq
from sorna.proto import Message, odict, generate_uuid
from sorna.proto.msgtypes import *
from sorna.agent.server import container_registry, docker_init
from sorna.agent.server import create_kernel, destroy_kernel, execute_code

class AgentFunctionalTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.docker_cli = docker_init()

    def tearDown(self):
        self.docker_cli.close()
        self.loop.close()

    def test_create_and_destroy_kernel(self):
        kernel_id = self.loop.run_until_complete(
                create_kernel(self.loop, self.docker_cli, 'python34'))
        assert kernel_id in container_registry
        self.loop.run_until_complete(
                destroy_kernel(self.loop, self.docker_cli, kernel_id))
        assert kernel_id not in container_registry

    def test_execute_simple_python27(self):
        kernel_id = self.loop.run_until_complete(
                create_kernel(self.loop, self.docker_cli, 'python27'))
        assert kernel_id in container_registry

        result = self.loop.run_until_complete(
                execute_code(self.loop, self.docker_cli, kernel_id, '1', 'print "asdf"'))
        assert 'asdf' in result['stdout']

        self.loop.run_until_complete(
                destroy_kernel(self.loop, self.docker_cli, kernel_id))
        assert kernel_id not in container_registry

'''
    def test_execute_simple_python34(self):
        kernel_id = self.loop.run_until_complete(
                create_kernel(self.loop, self.docker_cli, 'python34'))
        assert kernel_id in container_registry

        result = self.loop.run_until_complete(
                execute_code(self.loop, self.docker_cli, kernel_id, '1', 'print("asdf")'))
        assert 'asdf' in result['stdout']

        self.loop.run_until_complete(
                destroy_kernel(self.loop, self.docker_cli, kernel_id))
        assert kernel_id not in container_registry
'''


'''
class AgentKernelResponseTest(unittest.TestCase):
    def setUp(self):
        self.agent_port = 6050
        self.agent_addr = 'tcp://{0}:{1}'.format('127.0.0.1', self.agent_port)
        self.dummy_manager_addr = 'tcp://{0}:{1}'.format('127.0.0.1', 5001)

        # Establish an agent server in a separate process
        cmd = ['python3', '-m', 'sorna.agent.server',
               '--agent-port', str(self.agent_port)]
        self.server = subprocess.Popen(cmd, start_new_session=True,
                                       stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL)

        # Connect to the agent server
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.agent_addr)
        self.dummy_manager_socket = self.context.socket(zmq.REP)
        self.dummy_manager_socket.bind(self.dummy_manager_addr)

    def tearDown(self):
        sid = os.getsid(self.server.pid)
        os.killpg(sid, signal.SIGTERM)
        exitcode = self.server.wait()

    def test_heartbeat_response_with_same_body_as_request(self):
        # Send test HEARTBEAT request
        request = Message(
            ('req_type', AgentRequestTypes.HEARTBEAT),
            ('body', 'test'),
        )
        self.socket.send(request.encode())

        # Receive response
        response_data = self.socket.recv()
        response = Message.decode(response_data)

        # Assert its body is equal to that of request
        self.assertEqual(request['body'], response['body'])

    def test_socket_info_response_with_correct_kernel_ip(self):
        # Send test SOCKET_INFO request
        request = Message(
            ('req_type', AgentRequestTypes.SOCKET_INFO),
            ('body', ''),
        )
        self.socket.send(request.encode())

        # Receive response
        response_data = self.socket.recv()
        response = Message.decode(response_data)
        sock_info = response['body']

        # Check kernel ip address matches
        self.assertEqual(sock_info['stdin'].rpartition(':')[0], 'tcp://' + self.kernel_ip)
        self.assertEqual(sock_info['stdout'].rpartition(':')[0], 'tcp://' + self.kernel_ip)
        self.assertEqual(sock_info['stderr'].rpartition(':')[0], 'tcp://' + self.kernel_ip)

    def test_execute_response_with_correct_exec_result(self):
        # Send test EXECUTE request
        request = Message(
            ('req_type', AgentRequestTypes.EXECUTE),
            ('body', odict(
                ('cell_id', 1),
                ('code', 'def sum(x,y):\n\treturn x+y\na=5\nb=2\nprint(sum(a,b))'),
                ('lang', 'python34'),
                ('redirect_output', True))),
        )
        self.socket.send(request.encode())

        # Receive response.
        response_data = self.socket.recv()
        response = Message.decode(response_data)
        exec_result = response['body']

        # Check the execution result is correct
        self.assertEqual(exec_result['stdout'], '7')
        self.assertEqual(exec_result['stderr'], '')

    def test_execution_raise_indentation_error(self):
        # Send test EXECUTE request
        request = Message(
            ('req_type', AgentRequestTypes.EXECUTE),
            ('body', odict(
                ('cell_id', 1),
                ('code', 'a=5\n\tb=2\nprint(a+b)'),  # code with an indentation error
                ('lang', 'python34'),
                ('redirect_output', True))),
        )
        self.socket.send(request.encode())

        # Receive response.
        response_data = self.socket.recv()
        response = Message.decode(response_data)
        exec_result = response['body']

        # Check the execution result is correct
        self.assertIn('IndentationError', str(exec_result['exceptions']))

    def test_execution_raise_name_error(self):
        # Send test EXECUTE request
        request = Message(
            ('req_type', AgentRequestTypes.EXECUTE),
            ('body', odict(
                ('cell_id', 1),
                ('code', 'print(this_is_nothing)'),  # code with use of undefined variable
                ('lang', 'python34'),
                ('redirect_output', True))),
        )
        self.socket.send(request.encode())

        # Receive response.
        response_data = self.socket.recv()
        response = Message.decode(response_data)
        exec_result = response['body']

        # Check the execution result is correct
        self.assertIn('NameError', str(exec_result['exceptions']))
'''

if __name__ == '__main__':
    unittest.main()
