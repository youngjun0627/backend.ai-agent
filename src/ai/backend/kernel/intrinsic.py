import asyncio
from pathlib import Path


async def init_sshd_service(child_env):
    Path('/tmp/dropbear').mkdir(parents=True, exist_ok=True)
    auth_path = Path('/home/work/.ssh/authorized_keys')
    if not auth_path.is_file():
        auth_path.parent.mkdir(parents=True, exist_ok=True)
        auth_path.parent.chmod(0o700)
        proc = await asyncio.create_subprocess_exec(
            *[
                '/opt/kernel/dropbearkey',
                '-t', 'rsa',
                '-s', '2048',
                '-f', '/tmp/dropbear/id_dropbear',
            ],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=child_env)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"sshd init error: {stderr.decode('utf8')}")
        pub_key = stdout.splitlines()[1]
        auth_path.write_bytes(pub_key)
        auth_path.chmod(0o600)

        # Make the generated private key downloadable by users.
        proc = await asyncio.create_subprocess_exec(
            *[
                '/opt/kernel/dropbearconvert',
                'dropbear', 'openssh',
                '/tmp/dropbear/id_dropbear', '/home/work/id_container',
            ],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=child_env)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(f"sshd init error: {stderr.decode('utf8')}")
    else:
        auth_path.parent.chmod(0o700)
        auth_path.chmod(0o600)
    proc = await asyncio.create_subprocess_exec(
        *[
            '/opt/kernel/dropbearkey',
            '-t', 'rsa',
            '-s', '2048',
            '-f', '/tmp/dropbear/dropbear_rsa_host_key',
        ],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=child_env)
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"sshd init error: {stderr.decode('utf8')}")


async def prepare_sshd_service(service_info):
    cmdargs = [
        '/opt/kernel/dropbear',
        '-r', '/tmp/dropbear/dropbear_rsa_host_key',
        '-E',  # show logs in stderr
        '-F',  # run in foreground
        '-s',  # disable password logins
        # '-W', str(256 * 1024),  # recv buffer size (256 KiB) -> built-in during compilation
        '-K', '15',               # keepalive interval
        '-I', '0',                # idle timeout
        '-p', f"0.0.0.0:{service_info['port']}",
    ]
    env = {}
    return cmdargs, env


async def prepare_ttyd_service(service_info):
    shell = 'sh'
    if Path('/bin/bash').exists():
        shell = 'bash'
    elif Path('/bin/ash').exists():
        shell = 'ash'

    cmdargs = ['/opt/backend.ai/bin/ttyd', f'/bin/{shell}']
    # FIXME: tmux is temporarily disabled for Agent v19.09.12 release
    # if shell != 'ash':
    #     cmdargs += ['-c',
    #                 '/opt/kernel/tmux -2 attach -t backendai '
    #                 '|| /opt/kernel/tmux -2 new -s backendai']
    return cmdargs, {}
