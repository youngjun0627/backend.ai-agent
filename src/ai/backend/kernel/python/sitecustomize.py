try:
    import builtins
except ImportError:
    builtins = __builtins__
import socket
import sys

input_host = '127.0.0.1'
input_port = 65000


def _input(prompt=''):
    sys.stdout.write(prompt)
    sys.stdout.flush()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.connect((input_host, input_port))
            userdata = sock.recv(1024)
        except ConnectionRefusedError:
            userdata = b'<user-input-unavailable>'
    return userdata.decode()


builtins._input = input
builtins.input = _input
