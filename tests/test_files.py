import logging
import os
from pathlib import Path
import tempfile
from unittest import mock

import aiobotocore
import pytest

from ai.backend.agent.files import (
    upload_output_files_to_s3, scandir, diff_file_stats
)


def mock_awaitable(return_value):
    async def mock_coro(*args, **kwargs):
        return return_value
    return mock.Mock(wraps=mock_coro)


@pytest.fixture
def fake_s3_keys():
    import ai.backend.agent.files as files
    original_access_key = files.s3_access_key
    original_secret_key = files.s3_secret_key
    files.s3_access_key = 'fake-access-key'
    files.s3_secret_key = 'fake-secret-key'

    yield

    files.s3_access_key = original_access_key
    files.s3_secret_key = original_secret_key


@pytest.mark.asyncio
async def test_upload_output_files_to_s3(fake_s3_keys, mocker):
    fake_id = 'fake-entry-id'
    mock_get_session = mocker.patch.object(aiobotocore, 'get_session')
    mock_session = mock_get_session.return_value = mock.Mock()
    mock_client = mock_session.create_client.return_value = mock.Mock()
    mock_client.put_object = mock_awaitable(None)

    with tempfile.TemporaryDirectory() as tmpdir:
        fs1 = scandir(tmpdir, 1000)

        file1 = Path(tmpdir) / 'file.txt'
        file1.write_bytes(b'random-content')
        subdir = Path(tmpdir) / 'subdir'
        subdir.mkdir()
        file2 = subdir / 'file.txt'
        file2.write_bytes(b'awesome-content')

        fs2 = scandir(tmpdir, 1000)
        diff = await upload_output_files_to_s3(fs1, fs2, tmpdir, fake_id)

    assert len(diff) == 2
    mock_client.put_object.assert_any_call(
        Bucket=mock.ANY, Key=f'bucket/fake-entry-id/file.txt',
        Body=b'random-content', ACL='public-read'
    )
    mock_client.put_object.assert_any_call(
        Bucket=mock.ANY, Key=f'bucket/fake-entry-id/subdir/file.txt',
        Body=b'awesome-content', ACL='public-read'
    )


@pytest.mark.asyncio
async def test_s3_access_key_required(mocker):
    mock_warn = mocker.patch.object(logging.LoggerAdapter, 'warning')

    with tempfile.TemporaryDirectory() as tmpdir:
        file1 = Path(tmpdir) / 'test.txt'
        file1.write_bytes(b'something')
        diff = await upload_output_files_to_s3(
            {}, {file1: 1}, tmpdir, 'fake-id')

    mock_warn.assert_called_once_with(mock.ANY)
    assert 'test.txt' == diff[0]['name']
    assert '#dummy-upload' == diff[0]['url']


def test_scandir():
    # Create two files.
    with tempfile.TemporaryDirectory() as tmpdir:
        first = Path(tmpdir) / 'first.txt'
        first.write_text('first')
        second = Path(tmpdir) / 'second.txt'
        second.write_text('second')
        new_time = first.stat().st_mtime + 5
        os.utime(second, (new_time, new_time))

        file_stats = scandir(Path(tmpdir), 1000)

    assert len(file_stats) == 2
    assert int(file_stats[second]) == int(file_stats[first]) + 5


def test_scandir_skip_hidden_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        file = Path(tmpdir) / '.hidden_file'
        file.write_text('dark templar')
        file_stats = scandir(Path(tmpdir), 1000)

    assert len(file_stats) == 0


def test_scandir_skip_large_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        file = Path(tmpdir) / 'file.jpg'
        file.write_text('large file')
        file_stats = scandir(Path(tmpdir), 1)

    assert len(file_stats) == 0


def test_scandir_returns_files_in_sub_folder():
    with tempfile.TemporaryDirectory() as tmpdir:
        sub_folder = Path(tmpdir) / 'sub'
        sub_folder.mkdir()
        sub_file = sub_folder / 'sub-file.txt'
        sub_file.write_text('somedata')

        file_stats = scandir(Path(tmpdir), 1000)

    assert len(file_stats) == 1


def test_get_new_file_diff_stats():
    with tempfile.TemporaryDirectory() as tmpdir:
        first = Path(tmpdir) / 'first.txt'
        first.write_text('first')
        fs1 = scandir(tmpdir, 1000)

        second = Path(tmpdir) / 'second.txt'
        second.write_text('second')
        fs2 = scandir(tmpdir, 1000)

        diff_stats = diff_file_stats(fs1, fs2)

    assert first not in diff_stats
    assert second in diff_stats


def test_get_modified_file_diff_stats():
    with tempfile.TemporaryDirectory() as tmpdir:
        first = Path(tmpdir) / 'first.txt'
        first.write_text('first')
        second = Path(tmpdir) / 'second.txt'
        second.write_text('second')
        fs1 = scandir(tmpdir, 1000)

        new_time = first.stat().st_mtime + 5
        os.utime(second, (new_time, new_time))
        fs2 = scandir(tmpdir, 1000)

        diff_stats = diff_file_stats(fs1, fs2)

    assert first not in diff_stats
    assert second in diff_stats


def test_get_both_new_and_modified_files_stat():
    with tempfile.TemporaryDirectory() as tmpdir:
        first = Path(tmpdir) / 'first.txt'
        first.write_text('first')
        fs1 = scandir(tmpdir, 1000)

        new_time = first.stat().st_mtime + 5
        os.utime(first, (new_time, new_time))
        second = Path(tmpdir) / 'second.txt'
        second.write_text('second')
        fs2 = scandir(tmpdir, 1000)

        diff_stats = diff_file_stats(fs1, fs2)

    assert first in diff_stats
    assert second in diff_stats
