import asyncio
import logging
from pathlib import Path
from pprint import pprint, pformat
import signal
import ssl
import subprocess
import sys

import aiojobs.aiohttp
from aiohttp import web
import aiotools
import click
from setproctitle import setproctitle
import trafaret as t

from ai.backend.common import config, utils, validators as tx
from ai.backend.common.etcd import AsyncEtcd, ConfigScopes
from ai.backend.common.logging import Logger, BraceStyleAdapter
from . import __version__ as VERSION

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.watcher'))

shutdown_enabled = False


@web.middleware
async def auth_middleware(request, handler):
    token = request.headers.get('X-BackendAI-Watcher-Token', None)
    if token == request.app['token']:
        return (await handler(request))
    return web.HTTPForbidden()


async def handle_status(request: web.Request) -> web.Response:
    svc = request.app['config']['watcher']['target-service']
    proc = await asyncio.create_subprocess_exec(
        *['systemctl', 'is-active', svc],
        stdout=subprocess.PIPE)
    status = (await proc.stdout.read()).strip().decode()
    await proc.wait()
    return web.json_response({
        'agent-status': status,  # maybe also "inactive", "activating"
        'watcher-status': 'active',
    })


async def handle_soft_reset(request: web.Request) -> web.Response:
    svc = request.app['config']['watcher']['target-service']
    proc = await asyncio.create_subprocess_exec(
        *['systemctl', 'reload', svc])
    await proc.wait()
    return web.json_response({
        'result': 'ok',
    })


async def handle_hard_reset(request: web.Request) -> web.Response:
    svc = request.app['config']['watcher']['target-service']
    proc = await asyncio.create_subprocess_exec(
        *['systemctl', 'stop', svc])
    await proc.wait()
    proc = await asyncio.create_subprocess_exec(
        *['systemctl', 'restart', 'docker.service'])
    await proc.wait()
    proc = await asyncio.create_subprocess_exec(
        *['systemctl', 'start', svc])
    await proc.wait()
    return web.json_response({
        'result': 'ok',
    })


async def handle_shutdown(request: web.Request) -> web.Response:
    global shutdown_enabled
    svc = request.app['config']['watcher']['target-service']
    proc = await asyncio.create_subprocess_exec(
        *['systemctl', 'stop', svc])
    await proc.wait()
    shutdown_enabled = True
    signal.alarm(1)
    return web.json_response({
        'result': 'ok',
    })


async def init_app(app):
    r = app.router.add_route
    r('GET', '/', handle_status)
    if app['config']['watcher']['soft-reset-available']:
        r('POST', '/soft-reset', handle_soft_reset)
    r('POST', '/hard-reset', handle_hard_reset)
    r('POST', '/shutdown', handle_shutdown)


async def shutdown_app(app):
    pass


async def prepare_hook(request, response):
    response.headers['Server'] = 'BackendAI-AgentWatcher'


@aiotools.server
async def watcher_server(loop, pidx, args):
    global shutdown_enabled

    app = web.Application()
    app['config'] = args[0]
    aiojobs.aiohttp.setup(app, close_timeout=10)

    etcd_credentials = None
    if app['config']['etcd']['user']:
        etcd_credentials = {
            'user': app['config']['etcd']['user'],
            'password': app['config']['etcd']['password'],
        }
    scope_prefix_map = {
        ConfigScopes.GLOBAL: '',
    }
    etcd = AsyncEtcd(app['config']['etcd']['addr'],
                     app['config']['etcd']['namespace'],
                     scope_prefix_map=scope_prefix_map,
                     credentials=etcd_credentials)

    token = await etcd.get('config/watcher/token')
    if token is None:
        token = 'insecure'
    log.debug('watcher authentication token: {}', token)
    app['token'] = token

    app.middlewares.append(auth_middleware)
    app.on_shutdown.append(shutdown_app)
    app.on_startup.append(init_app)
    app.on_response_prepare.append(prepare_hook)
    ssl_ctx = None
    if app['config']['watcher']['ssl-enabled']:
        ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_ctx.load_cert_chain(
            str(app['config']['watcher']['ssl-cert']),
            str(app['config']['watcher']['ssl-privkey']),
        )
    runner = web.AppRunner(app)
    await runner.setup()
    watcher_addr = app['config']['watcher']['service-addr']
    site = web.TCPSite(
        runner,
        str(watcher_addr.host),
        watcher_addr.port,
        backlog=5,
        reuse_port=True,
        ssl_context=ssl_ctx,
    )
    await site.start()
    log.info('started at {}', watcher_addr)
    try:
        stop_sig = yield
    finally:
        log.info('shutting down...')
        if stop_sig == signal.SIGALRM and shutdown_enabled:
            log.warning('shutting down the agent node!')
            subprocess.run(['shutdown', '-h', 'now'])
        await runner.cleanup()


@click.command()
@click.option('-f', '--config-path', '--config', type=Path, default=None,
              help='The config file path. (default: ./agent.conf and /etc/backend.ai/agent.conf)')
@click.option('--debug', is_flag=True,
              help='Enable the debug mode and override the global log level to DEBUG.')
@click.pass_context
def main(cli_ctx, config_path, debug):

    watcher_config_iv = t.Dict({
        t.Key('watcher'): t.Dict({
            t.Key('service-addr', default=('0.0.0.0', 6009)): tx.HostPortPair,
            t.Key('ssl-enabled', default=False): t.Bool,
            t.Key('ssl-cert', default=None): t.Null | tx.Path(type='file'),
            t.Key('ssl-key', default=None): t.Null | tx.Path(type='file'),
            t.Key('target-service', default='backendai-agent.service'): t.String,
            t.Key('soft-reset-available', default=False): t.Bool,
        }).allow_extra('*'),
        t.Key('logging'): t.Any,  # checked in ai.backend.common.logging
        t.Key('debug'): t.Dict({
            t.Key('enabled', default=False): t.Bool,
        }).allow_extra('*'),
    }).merge(config.etcd_config_iv).allow_extra('*')

    raw_cfg, cfg_src_path = config.read_from_file(config_path, 'agent')

    config.override_with_env(raw_cfg, ('etcd', 'namespace'), 'BACKEND_NAMESPACE')
    config.override_with_env(raw_cfg, ('etcd', 'addr'), 'BACKEND_ETCD_ADDR')
    config.override_with_env(raw_cfg, ('etcd', 'user'), 'BACKEND_ETCD_USER')
    config.override_with_env(raw_cfg, ('etcd', 'password'), 'BACKEND_ETCD_PASSWORD')
    config.override_with_env(raw_cfg, ('watcher', 'service-addr', 'host'),
                             'BACKEND_WATCHER_SERVICE_IP')
    config.override_with_env(raw_cfg, ('watcher', 'service-addr', 'port'),
                             'BACKEND_WATCHER_SERVICE_PORT')
    if debug:
        config.override_key(raw_cfg, ('debug', 'enabled'), True)

    try:
        cfg = config.check(raw_cfg, watcher_config_iv)
        if 'debug'in cfg and cfg['debug']['enabled']:
            print('== Watcher configuration ==')
            pprint(cfg)
        cfg['_src'] = cfg_src_path
    except config.ConfigurationError as e:
        print('Validation of watcher configuration has failed:', file=sys.stderr)
        print(pformat(e.invalid_data), file=sys.stderr)
        raise click.Abort()

    logger = Logger(cfg['logging'])
    setproctitle(f"backend.ai: watcher {cfg['etcd']['namespace']}")
    with logger:
        log.info('Backend.AI Agent Watcher {0}', VERSION)
        log.info('runtime: {0}', utils.env_info())

        log_config = logging.getLogger('ai.backend.agent.config')
        log_config.debug('debug mode enabled.')

        aiotools.start_server(
            watcher_server, num_workers=1,
            use_threading=True, args=(cfg, ),
            stop_signals={signal.SIGINT, signal.SIGTERM, signal.SIGALRM},
        )
        log.info('exit.')
    return 0


if __name__ == '__main__':
    main()
