from subprocess import CalledProcessError
import asyncio


async def create_tmp_filesystem(tmp_dir, size):
    '''
    Create temporary folder size quota by using tmpfs filesystem.

    :param tmp_dir: The path of temporary directory.

    :param size: The quota size of temporary directory.
                 Size parameter is must be MiB(mebibyte).
    '''

    proc = await asyncio.create_subprocess_exec(*[
        'mount',
        '-t', 'tmpfs',
        '-o', f'size={size}M',
        'tmpfs', f'{tmp_dir}'
    ])
    exit_code = await proc.wait()

    if exit_code < 0:
        raise CalledProcessError(proc.returncode, proc.args,
                                 output=proc.stdout, stderr=proc.stderr)


async def destroy_tmp_filesystem(tmp_dir):
    '''
    Destroy temporary folder size quota by using tmpfs filesystem.

    :param tmp_dir: The path of temporary directory.
    '''
    proc = await asyncio.create_subprocess_exec(*[
        'umount',
        f'{tmp_dir}'
    ])
    exit_code = await proc.wait()

    if exit_code < 0:
        raise CalledProcessError(proc.returncode, proc.args,
                                 output=proc.stdout, stderr=proc.stderr)
