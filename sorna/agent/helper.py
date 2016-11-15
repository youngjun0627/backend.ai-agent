import asyncio

import docker
import requests


async def call_docker_with_retries(func, timeout_handler=None, apierr_handler=None):
    assert callable(func)
    retries = 0
    exc = None
    while True:
        try:
            return func()
        except docker.errors.APIError as e:
            if callable(apierr_handler):
                apierr_handler(e)
            exc = e
            break
        except requests.exceptions.Timeout as e:
            if retries == 3:
                if callable(timeout_handler):
                    timeout_handler()
                exc = e
                break
            retries += 1
            await asyncio.sleep(0.2)
    return exc
