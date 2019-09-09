import os
import sys
from pathlib import Path
from pprint import pprint, pformat

import click
import trafaret as t
from ai.backend.common import config, validators as tx

_max_cpu_count = os.cpu_count()
_file_perm = (Path(__file__).parent.parent / 'server.py').stat()

agent_config_iv = t.Dict({
    t.Key('agent'): t.Dict({
        t.Key('rpc-listen-addr', default=('', 6001)): tx.HostPortPair(allow_blank_host=True),
        t.Key('id', default=None): t.Null | t.String,
        t.Key('region', default=None): t.Null | t.String,
        t.Key('instance-type', default=None): t.Null | t.String,
        t.Key('scaling-group', default='default'): t.String,
        t.Key('pid-file', default=os.devnull): tx.Path(type='file',
                                                        allow_nonexisting=True,
                                                        allow_devnull=True),
    }).allow_extra('*'),
    t.Key('baistatic'): t.Null | t.Dict({
        t.Key('nfs-addr'): t.String,
        t.Key('path'): t.String,
        t.Key('capacity'): tx.BinarySize,
        t.Key('options'): t.Null | t.String,
        t.Key('mounted-at'): t.String
    }),
    t.Key('vfolder-pv'): t.Null | t.Dict({
        t.Key('nfs-addr'): t.String,
        t.Key('path'): t.String,
        t.Key('capacity'): tx.BinarySize,
        t.Key('options'): t.Null | t.String
    }),
    t.Key('container'): t.Dict({
        t.Key('kernel-uid', default=-1): tx.UserID,
        t.Key('kernel-host', default=''): t.String(allow_blank=True),
        t.Key('port-range', default=(30000, 31000)): tx.PortRange,
        t.Key('sandbox-type'): t.Enum('docker', 'jail'),
        t.Key('jail-args', default=[]): t.List(t.String),
        t.Key('scratch-type'): t.Enum('hostdir', 'memory'),
        t.Key('scratch-root', default='./scratches'): tx.Path(type='dir', auto_create=True),
        t.Key('scratch-size', default='0'): tx.BinarySize,
    }).allow_extra('*'),
    t.Key('logging'): t.Any,  # checked in ai.backend.common.logging
    t.Key('resource'): t.Dict({
        t.Key('reserved-cpu', default=1): t.Int,
        t.Key('reserved-mem', default="1G"): tx.BinarySize,
        t.Key('reserved-disk', default="8G"): tx.BinarySize,
    }).allow_extra('*'),
    t.Key('debug'): t.Dict({
        t.Key('enabled', default=False): t.Bool,
        t.Key('skip-container-deletion', default=False): t.Bool,
    }).allow_extra('*'),
}).merge(config.etcd_config_iv).allow_extra('*')


redis_config_iv = t.Dict({
    t.Key('addr', default=('127.0.0.1', 6379)): tx.HostPortPair,
    t.Key('password', default=None): t.Null | t.String,
}).allow_extra('*')


def load(config_path: Path = None, debug: bool = False):
    # Determine where to read configuration.
    raw_cfg, cfg_src_path = config.read_from_file(config_path, 'agent')

    # Override the read config with environment variables (for legacy).
    config.override_with_env(raw_cfg, ('etcd', 'namespace'), 'BACKEND_NAMESPACE')
    config.override_with_env(raw_cfg, ('etcd', 'addr'), 'BACKEND_ETCD_ADDR')
    config.override_with_env(raw_cfg, ('etcd', 'user'), 'BACKEND_ETCD_USER')
    config.override_with_env(raw_cfg, ('etcd', 'password'), 'BACKEND_ETCD_PASSWORD')
    config.override_with_env(raw_cfg, ('agent', 'rpc-listen-addr', 'host'),
                             'BACKEND_AGENT_HOST_OVERRIDE')
    config.override_with_env(raw_cfg, ('agent', 'rpc-listen-addr', 'port'),
                             'BACKEND_AGENT_PORT')
    config.override_with_env(raw_cfg, ('agent', 'pid-file'), 'BACKEND_PID_FILE')
    config.override_with_env(raw_cfg, ('container', 'port-range'), 'BACKEND_CONTAINER_PORT_RANGE')
    config.override_with_env(raw_cfg, ('container', 'kernel-host'), 'BACKEND_KERNEL_HOST_OVERRIDE')
    config.override_with_env(raw_cfg, ('container', 'sandbox-type'), 'BACKEND_SANDBOX_TYPE')
    config.override_with_env(raw_cfg, ('container', 'scratch-root'), 'BACKEND_SCRATCH_ROOT')
    if debug:
        config.override_key(raw_cfg, ('debug', 'enabled'), True)
        config.override_key(raw_cfg, ('logging', 'level'), 'DEBUG')
        config.override_key(raw_cfg, ('logging', 'pkg-ns', 'ai.backend'), 'DEBUG')

    # Validate and fill configurations
    # (allow_extra will make configs to be forward-copmatible)
    try:
        cfg = config.check(raw_cfg, agent_config_iv)
        if 'debug'in cfg and cfg['debug']['enabled']:
            print('== Agent configuration ==')
            pprint(cfg)
        cfg['_src'] = cfg_src_path
    except config.ConfigurationError as e:
        print('Validation of agent configuration has failed:', file=sys.stderr)
        print(pformat(e.invalid_data), file=sys.stderr)
        raise click.Abort()
    else:
        return cfg
