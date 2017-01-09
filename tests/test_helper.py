from unittest import mock

import docker
import pytest
import requests

from sorna.agent.helper import call_docker_with_retries


@pytest.mark.asyncio
class TestCallDockerWithRetries:
    async def test_return_function_result(self):
        mock_func = mock.Mock(return_value=200)
        assert await call_docker_with_retries(mock_func) == 200

    async def test_docker_api_error(self):
        mock_func = mock.Mock(side_effect=docker.errors.APIError('api error'))
        mock_handler = mock.Mock()

        err = await call_docker_with_retries(mock_func,
                                             apierr_handler=mock_handler)

        assert isinstance(err, docker.errors.APIError)
        assert 'api error' in str(err)
        mock_handler.assert_called_once_with(mock.ANY)

    async def test_request_timeout_error(self):
        mock_func = mock.Mock(
            side_effect=requests.exceptions.Timeout('timeout error'))
        mock_handler = mock.Mock()

        err = await call_docker_with_retries(mock_func,
                                             timeout_handler=mock_handler)

        assert isinstance(err, requests.exceptions.Timeout)
        assert 'timeout error' in str(err)
        mock_handler.assert_called_once_with()

