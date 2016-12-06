import asyncio
import logging
import os

import botocore, aiobotocore

log = logging.getLogger('sorna.agent.files')

# the names of following AWS variables follow boto3 convention.
s3_access_key = os.environ.get('AWS_ACCESS_KEY_ID', 'dummy-access-key')
s3_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', 'dummy-secret-key')
s3_region = os.environ.get('AWS_REGION', 'ap-northeast-1')
s3_bucket = os.environ.get('AWS_S3_BUCKET', 'codeonweb')


async def upload_output_files_to_s3(initial_file_stats, final_file_stats, entry_id, loop=None):
    loop = loop if loop else asyncio.get_event_loop()
    diff_files = diff_file_stats(initial_file_stats, final_file_stats)
    if s3_access_key == 'dummy-access-key':
        log.warning('skipping upload files due to misconfigured AWS access/secret keys.')
        return diff_files
    if diff_files:
        session = aiobotocore.get_session(loop=loop)
        client = session.create_client('s3', region_name=s3_region,
                                       aws_secret_access_key=s3_secret_key,
                                       aws_access_key_id=s3_access_key)
        for fname in diff_files:
            key = 'bucket/{}/{}'.format(entry_id, fname)
            # TODO: put the file chunk-by-chunk.
            with open(fname, 'rb') as f:
                content = f.read()
            try:
                await client.put_object(Bucket=s3_bucket,
                                        Key=key,
                                        Body=content,
                                        ACL='public-read')
            except botocore.exceptions.ClientError as exc:
                log.exception('S3 upload error')
        client.close()
    return diff_files


def scandir(root, allowed_max_size):
    '''
    Scans a directory recursively and returns a dictionary of all files and
    their last modified time.
    '''
    file_stats = dict()
    for entry in os.scandir(root):
        # Skip hidden files.
        if entry.name.startswith('.'):
            continue
        if entry.is_file():
            try:
                stat = entry.stat()
            except PermissionError:
                continue
            # Skip too large files!
            if stat.st_size > allowed_max_size:
                continue
            file_stats[entry.path] = stat.st_mtime
        elif entry.is_dir():
            try:
                file_stats.update(scandir(entry.path))
            except PermissionError:
                pass
    return file_stats


def diff_file_stats(fs1, fs2):
    k2 = set(fs2.keys())
    k1 = set(fs1.keys())
    new_files = k2 - k1
    modified_files = set()
    for k in (k2 - new_files):
        if fs1[k] < fs2[k]:
            modified_files.add(k)
    return new_files | modified_files
