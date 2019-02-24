import asyncio
from ipaddress import ip_address
import logging
import subprocess

import aiojobs.aiohttp
from aiohttp import web
import aiotools
import configargparse
from setproctitle import setproctitle

from ai.backend.common import utils
from ai.backend.common.argparse import (
    host_port_pair, HostPortPair,
    ipaddr, port_no,
)
from ai.backend.common.etcd import AsyncEtcd
from ai.backend.common.logging import Logger, BraceStyleAdapter
from . import __version__ as VERSION

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.watcher'))


@web.middleware
async def auth_middleware(request, handler):
    token = request.headers.get('X-BackendAI-Watcher-Token', None)
    if token == request.app['token']:
        return (await handler(request))
    return web.HTTPForbidden()


async def handle_status(request: web.Request) -> web.Response:
    proc = await asyncio.create_subprocess_exec(
        *['systemctl', 'is-active', 'backendai-agent.service'],
        stdout=subprocess.PIPE)
    status = (await proc.stdout.read()).strip().decode()
    await proc.wait()
    return web.json_response({
        'agent-status': status,  # maybe also "inactive", "activating"
        'watcher-status': 'active',
    })


async def handle_soft_reset(request: web.Request) -> web.Response:
    log.info('soft-reset')
    return web.Response(status=200)


async def handle_hard_reset(request: web.Request) -> web.Response:
    log.info('hard-reset')
    return web.Response(status=200)


async def handle_shutdown(request: web.Request) -> web.Response:
    log.info('shutdown')
    return web.Response(status=200)


async def init_app(app):
    r = app.router.add_route
    r('GET', '/', handle_status)
    r('POST', '/soft-reset', handle_soft_reset)
    r('POST', '/hard-reset', handle_hard_reset)
    r('POST', '/shutdown', handle_shutdown)


async def shutdown_app(app):
    pass


async def prepare_hook(request, response):
    response.headers['Server'] = 'BackendAI-AgentWatcher'


@aiotools.server
async def watcher_main(loop, pidx, args):
    app = web.Application()
    app['config'] = args[0]
    aiojobs.aiohttp.setup(app, close_timeout=10)

    etcd_credentials = None
    if app['config'].etcd_user is not None:
        etcd_credentials = {
            'user': app['config'].etcd_user,
            'password': app['config'].etcd_password,
        }
    etcd = AsyncEtcd(app['config'].etcd_addr,
                     app['config'].namespace,
                     credentials=etcd_credentials)

    token = await etcd.get('config/agent/watcher-token')
    if token is None:
        token = 'insecure'
    log.debug('watcher authentication token: {}', token)
    app['token'] = token

    app.middlewares.append(auth_middleware)
    app.on_shutdown.append(shutdown_app)
    app.on_startup.append(init_app)
    app.on_response_prepare.append(prepare_hook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(
        runner,
        str(app['config'].service_ip),
        app['config'].service_port,
        backlog=5,
        reuse_port=True,
    )
    await site.start()
    log.info('started at {}:{}',
             app['config'].service_ip, app['config'].service_port)
    try:
        yield
    finally:
        log.info('shutting down...')
        await runner.cleanup()


if __name__ == '__main__':
    parser = configargparse.ArgumentParser()
    parser.add('--namespace', type=str, default='local',
               env_var='BACKEND_NAMESPACE',
               help='The namespace of this Backend.AI cluster. (default: local)')
    parser.add('--etcd-addr', type=host_port_pair,
               env_var='BACKEND_ETCD_ADDR',
               default=HostPortPair(ip_address('127.0.0.1'), 2379),
               help='The host:port pair of the etcd cluster or its proxy.')
    parser.add('--etcd-user', type=str,
               env_var='BACKEND_ETCD_USER',
               default=None,
               help='The username for the etcd cluster.')
    parser.add('--etcd-password', type=str,
               env_var='BACKEND_ETCD_PASSWORD',
               default=None,
               help='The password the user for the etcd cluster.')
    parser.add('--service-ip', env_var='BACKEND_WATCHER_SERVICE_IP',
               type=ipaddr, default=ip_address('0.0.0.0'),
               help='The IP where the watcher server listens on.')
    parser.add('--service-port', env_var='BACKEND_WATCHER_SERVICE_PORT',
               type=port_no, default=6009,
               help='The TCP port number where the watcher server listens on.')
    Logger.update_log_args(parser)
    args = parser.parse_args()

    logger = Logger(args)
    logger.add_pkg('aiotools')
    logger.add_pkg('aiohttp')
    logger.add_pkg('ai.backend')
    setproctitle(f'backend.ai: watcher {args.namespace}')

    with logger:
        log.info('Backend.AI Agent Watcher {0}', VERSION)
        log.info('runtime: {0}', utils.env_info())

        log_config = logging.getLogger('ai.backend.agent.config')
        if args.debug:
            log_config.debug('debug mode enabled.')

        aiotools.start_server(watcher_main, num_workers=1,
                              use_threading=True, args=(args, ))
        log.info('exit.')
