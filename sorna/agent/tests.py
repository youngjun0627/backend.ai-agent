#! /usr/bin/env python3

import asyncio
import unittest, unittest.mock
import docker
import os
import signal
import subprocess
import zmq
from sorna.proto import Message, odict, generate_uuid
from sorna.proto.msgtypes import *
from sorna.agent.server import container_registry, volume_root, docker_init
from sorna.agent.server import max_execution_time, max_upload_size
from sorna.agent.server import create_kernel, destroy_kernel, execute_code

class AgentFunctionalTest(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.docker_cli = docker_init()

    def tearDown(self):
        self.docker_cli.close()
        self.loop.close()

    def test_create_and_destroy_kernel(self):
        kernel_id = self.loop.run_until_complete(create_kernel(self.loop, self.docker_cli, 'python34'))
        assert kernel_id in container_registry
        self.loop.run_until_complete(destroy_kernel(self.loop, self.docker_cli, kernel_id))
        assert kernel_id not in container_registry

    def test_execute_simple_python27(self):
        kernel_id = self.loop.run_until_complete(create_kernel(self.loop, self.docker_cli, 'python27'))
        result = self.loop.run_until_complete(
                execute_code(self.loop, self.docker_cli, 'test', kernel_id, '1', 'print "asdf"'))
        assert 'asdf' in result['stdout']
        self.loop.run_until_complete(destroy_kernel(self.loop, self.docker_cli, kernel_id))

    def test_execute_simple_python34(self):
        kernel_id = self.loop.run_until_complete(create_kernel(self.loop, self.docker_cli, 'python34'))
        result = self.loop.run_until_complete(
                execute_code(self.loop, self.docker_cli, 'test', kernel_id, '1', 'print("asdf")'))
        assert 'asdf' in result['stdout']
        self.loop.run_until_complete(destroy_kernel(self.loop, self.docker_cli, kernel_id))

    def test_execute_timeout(self):
        kernel_id = self.loop.run_until_complete(create_kernel(self.loop, self.docker_cli, 'python34'))
        with self.assertRaises(asyncio.TimeoutError):
            result = self.loop.run_until_complete(
                    execute_code(self.loop, self.docker_cli, 'test', kernel_id,
                                 '1', 'import time; time.sleep({0})'.format(max_execution_time + 2)))
        # the container should be automatically destroyed
        assert kernel_id not in container_registry

    def test_file_output(self):
        kernel_id = self.loop.run_until_complete(create_kernel(self.loop, self.docker_cli, 'python34'))
        work_dir = os.path.join(volume_root, kernel_id)
        assert os.path.exists(work_dir)
        untouched_path = os.path.join(work_dir, 'untouched')
        with open(untouched_path, 'w') as f:
            f.write('x')
        src = '''
import os
print(os.getcwd())
with open('test.txt', 'w', encoding='utf8') as f:
    print('hello world 한글 테스트', file=f)
'''
        # TODO: mock s3 upload
        result = self.loop.run_until_complete(
                execute_code(self.loop, self.docker_cli, 'test', kernel_id, '1', src))
        assert '/home/work' == result['stdout'].splitlines()[0].strip()
        test_path = os.path.join(work_dir, 'test.txt')
        assert os.path.exists(test_path)
        assert 'test.txt' in result['files']
        assert 'untouched' not in result['files']
        with open(test_path, 'r', encoding='utf8') as f:
            data = f.read()
            assert 'hello world' in data
            assert '한글 테스트' in data
        self.loop.run_until_complete(destroy_kernel(self.loop, self.docker_cli, kernel_id))
        assert not os.path.exists(test_path)
        assert not os.path.exists(work_dir)

    def test_file_output_too_large(self):
        kernel_id = self.loop.run_until_complete(create_kernel(self.loop, self.docker_cli, 'python34'))
        work_dir = os.path.join(volume_root, kernel_id)
        src = '''
with open('large.txt', 'wb') as f:
    f.write(b'x' * {0})
'''.format(max_upload_size + 1)
        # TODO: mock s3 upload
        result = self.loop.run_until_complete(
                execute_code(self.loop, self.docker_cli, 'test', kernel_id, '1', src))
        assert 'large.txt' not in result['files']
        self.loop.run_until_complete(destroy_kernel(self.loop, self.docker_cli, kernel_id))
        assert not os.path.exists(work_dir)

    def test_restricted_networking(self):
        kernel_id = self.loop.run_until_complete(create_kernel(self.loop, self.docker_cli, 'python34'))
        src = '''
import socket
socket.setdefaulttimeout(1.0)
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    s.connect(('google.com', 80))
    print('connected')
    s.close()
except OSError:
    print('failed')
'''
        result = self.loop.run_until_complete(
                execute_code(self.loop, self.docker_cli, 'test', kernel_id, '1', src))
        assert 'failed' in result['stdout']
        self.loop.run_until_complete(destroy_kernel(self.loop, self.docker_cli, kernel_id))

    def test_heavy_code(self):
        kernel_id = self.loop.run_until_complete(create_kernel(self.loop, self.docker_cli, 'python34'))
        src = '''
# alphametics.py
import re
import itertools

def solve(puzzle):
    words = re.findall('[A-Z]+', puzzle.upper())
    unique_characters = set(''.join(words))
    assert len(unique_characters) <= 10, 'Too many letters'
    first_letters = {word[0] for word in words}
    n = len(first_letters)
    sorted_characters = ''.join(first_letters) + \
        ''.join(unique_characters - first_letters)
    characters = tuple(ord(c) for c in sorted_characters)
    digits = tuple(ord(c) for c in '0123456789')
    zero = digits[0]
    for guess in itertools.permutations(digits, len(characters)):
        if zero not in guess[:n]:
            equation = puzzle.translate(dict(zip(characters, guess)))
            if eval(equation):
                return equation

if __name__ == '__main__':
    puzzles = ["HAWAII + IDAHO + IOWA + OHIO == STATES"]
    # puzzles = ["I + LOVE + YOU == DORA"]  # 주석을 제거해서 다른 식으로도 해보세요
    # puzzles = ["SEND + MORE == MONEY"]
    for puzzle in puzzles:
        print(puzzle)
        solution = solve(puzzle)
        if solution:
            print(solution)
'''
        with self.assertRaises(asyncio.TimeoutError):
            result = self.loop.run_until_complete(
                    execute_code(self.loop, self.docker_cli, 'test', kernel_id, '1', src))
        # it should have been automaitcally deleted
        assert kernel_id not in container_registry

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
