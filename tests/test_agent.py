import asyncio
from pathlib import Path
import time
from unittest import mock
import uuid

from aiodocker.docker import Docker
from aiodocker.exceptions import DockerError
import asynctest
import pytest

from ai.backend.agent.server import AgentRPCServer

pytestmark = pytest.mark.skip(reason='deprecated test cases')


@pytest.fixture
def loop(event_loop):
    return event_loop


@pytest.fixture
def docker(loop):
    docker = None

    async def get_docker():
        nonlocal docker
        docker = Docker(url='/var/run/docker.sock')

    async def cleanup():
        await docker.events.stop()
        docker.session.close()

    loop.run_until_complete(get_docker())
    yield docker
    loop.run_until_complete(cleanup())


@pytest.fixture
def config(tmpdir):
    class Config:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    config = Config(
        agent_ip=None,
        agent_port=6001,
        # redis_addr=HostPortPair(ip_address('127.0.0.1'), 6379),
        # event_addr=HostPortPair(ip_address('127.0.0.1'), 5002),
        exec_timeout=180,
        idle_timeout=600,
        max_kernels=1,
        # debug=False,
        # kernel_aliases=None,
        volume_root=Path(tmpdir),
    )
    return config


@pytest.fixture
def events():
    """
    Mocking manager's event server to not to actually run it for agent tests.
    """
    events = mock.Mock()
    events.call.dispatch = asynctest.CoroutineMock()
    return events


@pytest.fixture
def agent(loop, docker, config, events):
    async def cleanup():
        for kernel_id, info in agent.container_registry.items():
            # Kill and delete test containers
            container_id = info['container_id']
            try:
                await docker.containers.container(container_id).kill()
            except DockerError:
                pass
            try:
                await docker.containers.container(container_id).delete()
            except DockerError:
                pass

    agent = AgentRPCServer(docker, config, events, loop=loop)
    yield agent
    loop.run_until_complete(cleanup())


@pytest.mark.integration
@pytest.mark.asyncio
class TestAgent:
    async def test_reset(self, agent):
        kernel_id1, _, _ = await agent.create_kernel('python3', {})
        kernel_id2, _, _ = await agent.create_kernel('python3', {})

        assert 'last_stat' not in agent.container_registry[kernel_id1]
        assert 'last_stat' not in agent.container_registry[kernel_id2]

        await agent.reset()

        assert 'last_stat' in agent.container_registry[kernel_id1]
        assert 'last_stat' in agent.container_registry[kernel_id2]

    async def test_execution_raises_timeout(self, agent):
        agent.config.exec_timeout = 1
        kernel_id, _, _ = await agent.create_kernel('python3', {})

        exec_timeout = agent.container_registry[kernel_id]['exec_timeout']

        entry_id = str(uuid.uuid4())
        code_id = str(uuid.uuid4())
        code = f'import time; time.sleep({exec_timeout + 1})'
        with pytest.raises(asyncio.TimeoutError):
            await agent.execute_code(entry_id, kernel_id, code_id, code, {})

    async def test_file_output(self, agent, tmpdir, mocker):
        kernel_id, _, _ = await agent.create_kernel('python3', {})

        work_dir = tmpdir / kernel_id
        assert work_dir.exists()

        untouched_path = work_dir.join('untouched')
        untouched_path.write('x')

        code = """
from pathlib import Path
print(Path.cwd())
with open('test.txt', 'w', encoding='utf8') as f:
    print('hello world 한글 테스트', file=f)
"""
        entry_id = str(uuid.uuid4())
        code_id = str(uuid.uuid4())
        result = await agent.execute_code(entry_id, kernel_id, code_id, code,
                                          {})

        test_path = work_dir / 'test.txt'
        assert '/home/work' == result['stdout'].splitlines()[0].strip()
        assert test_path.exists()
        assert 'test.txt' in result['files']
        assert 'untouched' not in result['files']
        with open(test_path, 'r', encoding='utf8') as f:
            data = f.read()
            assert 'hello world' in data
            assert '한글 테스트' in data

    async def test_too_large_file_not_uploaded(self, agent, tmpdir, mocker):
        from ai.backend.agent import server
        original_max_upload_size = server.max_upload_size
        server.max_upload_size = 1

        kernel_id, _, _ = await agent.create_kernel('python3', {})

        work_dir = tmpdir / kernel_id
        assert work_dir.exists()

        code = """
with open('large.txt', 'wb') as f:
    f.write(b'x' * {0})
""".format(server.max_upload_size + 1)

        entry_id = str(uuid.uuid4())
        code_id = str(uuid.uuid4())
        result = await agent.execute_code(entry_id, kernel_id, code_id, code,
                                          {})

        assert 'large.txt' not in result['files']

        server.max_upload_size = original_max_upload_size

    async def test_restricted_networking(self, agent):
        """This test may be similar to `test_execution_raises_timeout`"""
        agent.config.exec_timeout = 0
        kernel_id, _, _ = await agent.create_kernel('python3', {})

        code = """
import socket
socket.setdefaulttimeout(1.0)
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    s.connect(('google.com', 80))
    print('connected')
    s.close()
except OSError:
    print('failed')
"""
        kernel_timeout = False
        try:
            entry_id = str(uuid.uuid4())
            code_id = str(uuid.uuid4())
            result = await agent.execute_code(entry_id, kernel_id, code_id,
                                              code, {})
        except asyncio.TimeoutError:
            kernel_timeout = True

        if not kernel_timeout:
            assert 'connected' in result['stdout']
        else:
            assert 'result' not in locals()

    async def test_heavy_code(self, agent):
        """This test may be similar to `test_execution_raises_timeout`"""
        agent.config.exec_timeout = 1
        kernel_id, _, _ = await agent.create_kernel('python3', {})

        code = """
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
"""
        with pytest.raises(asyncio.TimeoutError):
            entry_id = str(uuid.uuid4())
            code_id = str(uuid.uuid4())
            await agent.execute_code(entry_id, kernel_id, code_id, code, {})

    async def test_crash(self, agent):
        """This test may be similar to `test_execution_raises_timeout`"""
        agent.config.exec_timeout = 1
        kernel_id, _, _ = await agent.create_kernel('python3', {})

        code = '''
import ctypes
i = ctypes.c_char(b'a')
j = ctypes.pointer(i)
c = 0
while True:
    j[c] = b'a'
    c += 1
j'''
        with pytest.raises(asyncio.TimeoutError):
            entry_id = str(uuid.uuid4())
            code_id = str(uuid.uuid4())
            await agent.execute_code(entry_id, kernel_id, code_id, code, {})

    async def test_clean_kernel(self, agent, tmpdir):
        kernel_id1, _, _ = await agent.create_kernel('python3', {})
        kernel_id2, _, _ = await agent.create_kernel('python3', {})

        await agent.destroy_kernel(kernel_id1)
        await agent.destroy_kernel(kernel_id2)

        assert len(agent.container_registry) == 2
        assert kernel_id1 in str(tmpdir.listdir())
        assert kernel_id2 in str(tmpdir.listdir())

        await agent.clean_kernel(kernel_id2)

        assert len(agent.container_registry) == 1
        assert kernel_id1 in agent.container_registry
        assert kernel_id2 not in agent.container_registry
        assert kernel_id1 in str(tmpdir.listdir())
        assert kernel_id2 not in str(tmpdir.listdir())

    async def test_clean_old_kernels(self, agent):
        kernel_id1, _, _ = await agent.create_kernel('python3', {})
        kernel_id2, _, _ = await agent.create_kernel('python3', {})

        now = time.monotonic()
        timeout = agent.config.idle_timeout

        # kernel 2 is old
        agent.container_registry[kernel_id1]['last_used'] = now
        agent.container_registry[kernel_id2]['last_used'] = now - timeout - 10

        assert 'last_stat' not in agent.container_registry[kernel_id1]
        assert 'last_stat' not in agent.container_registry[kernel_id2]

        await agent.clean_old_kernels()

        assert 'last_stat' not in agent.container_registry[kernel_id1]
        assert 'last_stat' in agent.container_registry[kernel_id2]

    async def test_clean_all_kernels(self, agent):
        kernel_id1, _, _ = await agent.create_kernel('python3', {})
        kernel_id2, _, _ = await agent.create_kernel('python3', {})

        await agent.clean_all_kernels()

        assert 'last_stat' in agent.container_registry[kernel_id1]
        assert 'last_stat' in agent.container_registry[kernel_id2]
