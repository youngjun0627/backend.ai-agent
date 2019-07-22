import asyncio
from asyncio import create_subprocess_shell, subprocess
import contextlib
import itertools
import logging
import json
from setproctitle import setproctitle
from typing import Any, Mapping
from pathlib import Path

import aiohttp
import aiojobs
from aiojobs.aiohttp import atomic
import aioredis
import attr
import click
import trafaret as t
import yarl

from ai.backend.common import config, validators as tx
from ai.backend.common.etcd import (
    quote as etcd_quote,
    unquote as etcd_unquote,
    ConfigScopes
)
from ai.backend.common.docker import (
    login as registry_login,
    get_registry_info
)
from ai.backend.common.logging import Logger, BraceStyleAdapter
from .config import load as load_config

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.cli.images'))
REDIS_IMAGE_DB = 3

redis_config_iv = t.Dict({
    t.Key('addr', default=('127.0.0.1', 6379)): tx.HostPortPair,
    t.Key('password', default=None): t.Null | t.String,
}).allow_extra('*')

def chunked(iterable, n):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk

class ConfigServer:

    def __init__(self, app_ctx, etcd_addr, etcd_user, etcd_password, namespace, target_registry):
        from ai.backend.common.etcd import AsyncEtcd
        self.context = app_ctx
        credentials = None
        if etcd_user:
            credentials = {
                'user': etcd_user,
                'password': etcd_password,
            }
        scope_prefix_map = {
            ConfigScopes.GLOBAL: '',
            # TODO: provide a way to specify other scope prefixes
        }
        self.etcd = AsyncEtcd(etcd_addr, namespace, scope_prefix_map, credentials=credentials)
        self.target_registry = target_registry

    async def _rescan_images(self, registry_name: str,
                                registry_url: yarl.URL,
                                credentials: dict):
        all_updates = {}
        base_hdrs = {
            'Accept': 'application/vnd.docker.distribution.manifest.v2+json',
        }

        async def _scan_image(sess, image):
            rqst_args = await registry_login(
                sess, registry_url,
                credentials, f'repository:{image}:pull')
            tags = []
            rqst_args['headers'].update(**base_hdrs)
            async with sess.get(registry_url / f'v2/{image}/tags/list',
                                **rqst_args) as resp:
                data = json.loads(await resp.read())
                if 'tags' in data:
                    # sometimes there are dangling image names in the hub.
                    tags.extend(data['tags'])
            scheduler = await aiojobs.create_scheduler(limit=8)
            try:
                jobs = await asyncio.gather(*[
                    scheduler.spawn(_scan_tag(sess, rqst_args, image, tag))
                    for tag in tags])
                await asyncio.gather(*[job.wait() for job in jobs])
            finally:
                await scheduler.close()

        async def _scan_tag(sess, rqst_args, image, tag):
            config_digest = None
            labels = {}
            async with sess.get(registry_url / f'v2/{image}/manifests/{tag}',
                                **rqst_args) as resp:
                resp.raise_for_status()
                data = await resp.json()
                config_digest = data['config']['digest']
                size_bytes = (sum(layer['size'] for layer in data['layers']) +
                                data['config']['size'])
            async with sess.get(registry_url / f'v2/{image}/blobs/{config_digest}',
                                **rqst_args) as resp:
                # content-type may not be json...
                resp.raise_for_status()
                data = json.loads(await resp.read())
                raw_labels = data['container_config']['Labels']
                if raw_labels:
                    labels.update(raw_labels)

            log.debug('checking image repository {}:{}', image, tag)
            if not labels.get('ai.backend.kernelspec'):
                # Skip non-Backend.AI kernel images
                return

            log.info('Updating metadata for {0}:{1}', image, tag)
            url_str = str(registry_url).replace('https://', '')
            target_registry_url = self.target_registry['host'] + ':' + str(self.target_registry['port'])

            cmds = [
                f'docker pull {url_str}/{image}:{tag}',
                f'docker tag {url_str}/{image}:{tag} {target_registry_url}/{image}:{tag}',
                f'docker push {target_registry_url}/{image}:{tag}',
                f'docker image rm {url_str}/{image}:{tag}'
            ]

            for cmd in cmds:
                proc = await create_subprocess_shell(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
                log.debug('{0}', cmd)
                stdout, stderr = await proc.communicate()
                error = False

                outs = stdout.decode().strip().split('\n')
                errs = stderr.decode().strip().split('\n')
                
                for line in outs:
                    log.debug('[STDOUT] \'{0}\'', line)
                for line in errs:
                    if len(line.strip()) == 0:
                        continue
                    if not error:
                        error = True
                        log.error('Error while executing \'{0}\'', cmd)
                    log.error('[STDERR] \'{0}\'', line)
                
                if error:
                    exit(-1)
    
        ssl_ctx = False
        connector = aiohttp.TCPConnector(ssl=ssl_ctx)
        async with aiohttp.ClientSession(connector=connector) as sess:
            images = []
            if registry_url.host.endswith('.docker.io'):
                # We need some special treatment for the Docker Hub.
                params = {'page_size': '100'}
                username = await self.etcd.get(
                    f'config/docker/registry/{etcd_quote(registry_name)}/username')
                hub_url = yarl.URL('https://hub.docker.com')
                async with sess.get(hub_url / f'v2/repositories/{username}/',
                                    params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        images.extend(f"{username}/{item['name']}"
                                        for item in data['results']
                                        # a little optimization to ignore legacies
                                        if not item['name'].startswith('kernel-'))
                    else:
                        log.error('Failed to fetch repository list from {0} '
                                    '(status={1})',
                                    hub_url, resp.status)
            else:
                # In other cases, try the catalog search.
                rqst_args = await registry_login(
                    sess, registry_url,
                    credentials, 'registry:catalog:*')
                async with sess.get(registry_url / 'v2/_catalog',
                                    **rqst_args) as resp:
                    if resp.status == 200:
                        data = json.loads(await resp.read())
                        images.extend(data['repositories'])
                        log.debug('found {} repositories', len(images))
                    else:
                        log.warning('Docker registry {0} does not allow/support '
                                    'catalog search. (status={1})',
                                    registry_url, resp.status)

            scheduler = await aiojobs.create_scheduler(limit=8)
            try:
                jobs = await asyncio.gather(*[
                    scheduler.spawn(_scan_image(sess, image)) for image in images])
                await asyncio.gather(*[job.wait() for job in jobs])
            finally:
                await scheduler.close()

        if not all_updates:
            log.info('No images found in registry {0}', registry_url)
            return
        for kvlist in chunked(sorted(all_updates.items()), 16):
            await self.etcd.put_dict(dict(kvlist))

    async def rescan_images(self, registry: str = None):
        if registry is None:
            registries = []
            data = await self.etcd.get_prefix('config/docker/registry')
            for key, val in data.items():
                if key:
                    registries.append(etcd_unquote(key))
        else:
            registries = [registry]
        coros = []
        for registry in registries:
            log.info('Scanning kernel images from the registry "{0}"', registry)
            try:
                registry_url, creds = await get_registry_info(self.etcd, registry)
            except ValueError:
                log.error('Unknown registry: "{0}"', registry)
                continue
            coros.append(self._rescan_images(registry, registry_url, creds))
        await asyncio.gather(*coros)
        # TODO: delete images removed from registry?

        

@contextlib.contextmanager
def config_ctx(cli_ctx):
    config = cli_ctx.config
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    ctx = {}
    ctx['config'] = config
    # scope_prefix_map is created inside ConfigServer
    config_server = ConfigServer(
        ctx, config['etcd']['addr'],
        config['etcd']['user'], config['etcd']['password'],
        config['etcd']['namespace'], 
        config['registry']['addr'])
    raw_redis_config = loop.run_until_complete(config_server.etcd.get_prefix('config/redis'))
    config['redis'] = redis_config_iv.check(raw_redis_config)
    ctx['redis_image'] = loop.run_until_complete(aioredis.create_redis(
        config['redis']['addr'].as_sockaddr(),
        password=config['redis']['password'] if config['redis']['password'] else None,
        timeout=3.0,
        encoding='utf8',
        db=REDIS_IMAGE_DB))
    with contextlib.closing(loop):
        try:
            yield loop, config_server
        finally:
            ctx['redis_image'].close()
            loop.run_until_complete(ctx['redis_image'].wait_closed())
    asyncio.set_event_loop(None)


@attr.s(auto_attribs=True, frozen=True)
class CLIContext:
    logger: Logger
    config: Mapping[str, Any]


@click.group(invoke_without_command=True, context_settings={'help_option_names': ['-h', '--help']})
@click.option('-f', '--config-path', '--config', type=Path, default=None,
              help='The config file path. (default: ./agent.toml and /etc/backend.ai/agent.toml)')
@click.option('--debug', is_flag=True,
              help='Enable the debug mode and override the global log level to DEBUG.')
@click.pass_context
def main(ctx, config_path, debug):
    cfg = load_config(config_path, debug=debug)
    setproctitle(f"backend.ai: agent.cli {cfg['etcd']['namespace']}")
    if 'file' in cfg['logging']['drivers']:
        cfg['logging']['drivers'].remove('file')
    logger = Logger(cfg['logging'])
    ctx.obj = CLIContext(
        logger=logger,
        config=cfg,
    )


@main.command()
@click.argument('registry')
@click.pass_obj
def rescan_images(cli_ctx, registry):
    '''
    Update the kernel image metadata from all configured docker registries.

    Pass the name (usually hostname or "lablup") of the Docker registry configured as REGISTRY.
    '''
    with cli_ctx.logger, config_ctx(cli_ctx) as (loop, config_server):
        try:
            loop.run_until_complete(
                config_server.rescan_images(registry))
        except Exception:
            log.exception('An error occurred.')



if __name__ == '__main__':
    main()