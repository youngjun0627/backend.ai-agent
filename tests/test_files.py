import logging
import os
from unittest import mock

import aiobotocore
import asynctest
import pytest

from sorna.agent.files import (
    upload_output_files_to_s3, scandir, diff_file_stats
)


async def mock_awaitable(**kwargs):
    """
    Mock awaitable.
    An awaitable can be a native coroutine object "returned from" a native
    coroutine function.
    """
    return asynctest.CoroutineMock(**kwargs)


@pytest.mark.asyncio
class TestUploadOutputFilesToS3:
    @pytest.fixture
    def fake_s3_keys(self):
        import sorna.agent.files as files
        original_access_key = files.s3_access_key
        original_secret_key = files.s3_secret_key
        files.s3_access_key = 'fake-access-key'
        files.s3_secret_key = 'fake-secret-key'

        yield

        files.s3_access_key = original_access_key
        files.s3_secret_key = original_secret_key

    async def test_upload_output_files_to_s3(self, fake_s3_keys, mocker,
                                             tmpdir):
        # Fake information
        fake_id = 'fake-entry-id'

        # Mocking
        mock_get_session = mocker.patch.object(aiobotocore, 'get_session')
        mock_session = mock_get_session.return_value = mock.Mock()
        mock_client = mock_session.create_client.return_value = mock.Mock()
        mock_client.put_object.return_value = mock_awaitable()

        # File to be updated
        fs1 = scandir(tmpdir, 1000)
        file = tmpdir.join('file.txt')
        file.write('file')
        fs2 = scandir(tmpdir, 1000)

        diff = await upload_output_files_to_s3(fs1, fs2, fake_id)

        assert len(diff) == 1
        mock_client.put_object.assert_called_once_with(
            Bucket=mock.ANY, Key='bucket/fake-entry-id/{}'.format(file.strpath),
            Body=b'file', ACL='public-read'
        )

    async def test_s3_access_key_required(self, mocker):
        mock_warn = mocker.patch.object(logging.Logger, 'warning')
        mock_diff = mocker.patch('sorna.agent.files.diff_file_stats')
        mock_diff.return_value = 'mock diff'

        diff = await upload_output_files_to_s3(
            mock.Mock(), mock.Mock(), 'fake-id')

        mock_warn.assert_called_once_with(mock.ANY)
        assert diff == mock_diff.return_value



class TestScandir:
    def test_scandir(self, tmpdir):
        # Create two files.
        first = tmpdir.join('first.txt')
        first.write('first')
        second = tmpdir.join('second.txt')
        second.write('second')
        new_time = first.stat().mtime + 5
        os.utime(second.strpath, (new_time, new_time))

        file_stats = scandir(tmpdir, 1000)

        assert len(file_stats) == 2
        assert file_stats[second.strpath] == file_stats[first.strpath] + 5

    def test_scandir_skip_hidden_files(self, tmpdir):
        # Create a hidden file.
        file = tmpdir.join('.hidden_file')
        file.write('dark templar')

        file_stats = scandir(tmpdir, 1000)

        assert len(file_stats) == 0

    def test_scandir_skip_large_files(self, tmpdir):
        file = tmpdir.join('file.jpg')
        file.write('large file')

        file_stats = scandir(tmpdir, 1)

        assert len(file_stats) == 0

    def test_scandir_returns_files_in_sub_folder(self, tmpdir):
        sub_folder = tmpdir.mkdir('sub')
        sub_file = sub_folder.join('sub-file.txt')
        sub_file.write('sub')

        file_stats = scandir(tmpdir, 1000)

        assert len(file_stats) == 1


class TestDiffFileStats:
    def test_get_new_file_diff_stats(self, tmpdir):
        first = tmpdir.join('first.txt')
        first.write('first')

        fs1 = scandir(tmpdir, 1000)

        second = tmpdir.join('second.txt')
        second.write('second')

        fs2 = scandir(tmpdir, 1000)

        diff_stats = diff_file_stats(fs1, fs2)

        assert first.strpath not in diff_stats
        assert second.strpath in diff_stats

    def test_get_modified_file_diff_stats(self, tmpdir):
        first = tmpdir.join('first.txt')
        first.write('first')
        second = tmpdir.join('second.txt')
        second.write('second')

        fs1 = scandir(tmpdir, 1000)

        new_time = first.stat().mtime + 5
        os.utime(second.strpath, (new_time, new_time))

        fs2 = scandir(tmpdir, 1000)

        diff_stats = diff_file_stats(fs1, fs2)

        assert first.strpath not in diff_stats
        assert second.strpath in diff_stats

    def test_get_both_new_and_modified_files_stat(self, tmpdir):
        first = tmpdir.join('first.txt')
        first.write('first')

        fs1 = scandir(tmpdir, 1000)

        new_time = first.stat().mtime + 5
        os.utime(first.strpath, (new_time, new_time))
        second = tmpdir.join('second.txt')
        second.write('second')

        fs2 = scandir(tmpdir, 1000)

        diff_stats = diff_file_stats(fs1, fs2)

        assert first.strpath in diff_stats
        assert second.strpath in diff_stats
