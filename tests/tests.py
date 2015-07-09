import unittest
from subprocess import call
from multiprocessing import Process
import json

import zmq
from sorna.proto.agent_pb2 import AgentRequest, AgentResponse
from sorna.proto.agent_pb2 import HEARTBEAT, SOCKET_INFO, EXECUTE

class AgentKernelResponseTest(unittest.TestCase):
    def setUp(self):
        self.kernel_ip = '127.0.0.1'
        self.kernel_id = 1
        self.agent_port = 5555
        self.agent_addr = 'tcp://{0}:{1}'.format(self.kernel_ip, self.agent_port)

        # Establish an agent server in a separate process
        cmd = ['python3', '-m', 'sorna.agent',
               '--kernel-id', str(self.kernel_id), '--agent-port', str(self.agent_port)]
        self.server = Process(target=call, args=(cmd,))
        self.server.start()

        # Connect to the agent server
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.agent_addr)

    def tearDown(self):
        self.server.terminate()
        call(['kill', str(self.server.pid+1)])  # To kill child process. Other way?

    def test_heartbeat_response_with_same_body_as_request(self):
        # Send test HEARTBEAT request
        request = AgentRequest()
        request.req_type = HEARTBEAT
        request.body = 'test'
        self.socket.send(request.SerializeToString())

        # Receive response
        response = AgentResponse()
        response_data = self.socket.recv()
        response.ParseFromString(response_data)

        # Assert its body is equal to that of request
        self.assertEqual(request.body, response.body)

    def test_socket_info_response_with_correct_kernel_ip(self):
        # Send test SOCKET_INFO request
        request = AgentRequest()
        request.req_type = SOCKET_INFO
        request.body = ''
        self.socket.send(request.SerializeToString())

        # Receive response
        response = AgentResponse()
        response_data = self.socket.recv()
        response.ParseFromString(response_data)
        sock_info = json.loads(response.body)

        # Check kernel ip address matches
        self.assertEqual(sock_info['stdin'].rpartition(':')[0], 'tcp://' + self.kernel_ip)
        self.assertEqual(sock_info['stdout'].rpartition(':')[0], 'tcp://' + self.kernel_ip)
        self.assertEqual(sock_info['stderr'].rpartition(':')[0], 'tcp://' + self.kernel_ip)

    def test_execute_response_with_correct_exec_result(self):
        # Send test EXECUTE request
        request = AgentRequest()
        request.req_type = EXECUTE
        request.body = json.dumps({
            'cell_id': 1,
            'code': 'def sum(x,y):\n\treturn x+y\na=5\nb=2\nprint(sum(a,b))',
            'redirect_output': True,
        })
        self.socket.send(request.SerializeToString())

        # Receive response.
        response = AgentResponse()
        response_data = self.socket.recv()
        response.ParseFromString(response_data)
        exec_result = json.loads(response.body)

        # Check the execution result is correct
        self.assertEqual(exec_result['stdout'], '7')
        self.assertEqual(exec_result['stderr'], '')

    def test_execution_raise_indentation_error(self):
        # Send test EXECUTE request
        request = AgentRequest()
        request.req_type = EXECUTE
        request.body = json.dumps({
            'cell_id': 1,
            'code': 'a=5\n\tb=2\nprint(a+b)',  # code with an indentation error
            'redirect_output': True,
        })
        self.socket.send(request.SerializeToString())

        # Receive response.
        response = AgentResponse()
        response_data = self.socket.recv()
        response.ParseFromString(response_data)
        exec_result = json.loads(response.body)

        # Check the execution result is correct
        self.assertIn('IndentationError', str(exec_result['exceptions']))

    def test_execution_raise_syntax_error(self):
        # Send test EXECUTE request
        request = AgentRequest()
        request.req_type = EXECUTE
        request.body = json.dumps({
            'cell_id': 1,
            'code': 'a=5\nc=2\nprint(a,b)',  # code with a syntax error
            'redirect_output': True,
        })
        self.socket.send(request.SerializeToString())

        # Receive response.
        response = AgentResponse()
        response_data = self.socket.recv()
        response.ParseFromString(response_data)
        exec_result = json.loads(response.body)

        # Check the execution result is correct
        self.assertIn('NameError', str(exec_result['exceptions']))

if __name__ == '__main__':
    unittest.main()