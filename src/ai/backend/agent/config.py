import os

import trafaret as t

from ai.backend.common import config
from ai.backend.common import validators as tx

from .stats import StatModes
from .types import AgentBackend


coredump_defaults = {
    'enabled': False,
    'path': './coredumps',
    'backup-count': 10,
    'size-limit': '64M',
}

agent_local_config_iv = t.Dict({
    t.Key('agent'): t.Dict({
        tx.AliasedKey(['backend', 'mode']): tx.Enum(AgentBackend),
        t.Key('rpc-listen-addr', default=('', 6001)):
            tx.HostPortPair(allow_blank_host=True),
        t.Key('agent-sock-port', default=6007): t.Int[1024:65535],
        t.Key('id', default=None): t.Null | t.String,
        t.Key('region', default=None): t.Null | t.String,
        t.Key('instance-type', default=None): t.Null | t.String,
        t.Key('scaling-group', default='default'): t.String,
        t.Key('pid-file', default=os.devnull): tx.Path(type='file',
                                                       allow_nonexisting=True,
                                                       allow_devnull=True),
        t.Key('event-loop', default='asyncio'): t.Enum('asyncio', 'uvloop'),
        t.Key('skip-manager-detection', default=False): t.ToBool,
        t.Key('aiomonitor-port', default=50002): t.Int[1:65535],
    }).allow_extra('*'),
    t.Key('container'): t.Dict({
        t.Key('kernel-uid', default=-1): tx.UserID,
        t.Key('kernel-gid', default=-1): tx.GroupID,
        t.Key('kernel-host', default=''): t.String(allow_blank=True),
        t.Key('port-range', default=(30000, 31000)): tx.PortRange,
        t.Key('stats-type', default='docker'):
            t.Null | t.Enum(*[e.value for e in StatModes]),
        t.Key('sandbox-type', default='docker'): t.Enum('docker', 'jail'),
        t.Key('jail-args', default=[]): t.List(t.String),
        t.Key('scratch-type'): t.Enum('hostdir', 'memory'),
        t.Key('scratch-root', default='./scratches'):
            tx.Path(type='dir', auto_create=True),
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
        t.Key('log-stats', default=False): t.Bool,
        t.Key('log-kernel-config', default=False): t.Bool,
        t.Key('log-alloc-map', default=False): t.Bool,
        t.Key('log-events', default=False): t.Bool,
        t.Key('log-heartbeats', default=False): t.Bool,
        t.Key('log-docker-events', default=False): t.Bool,
        t.Key('coredump', default=coredump_defaults): t.Dict({
            t.Key('enabled', default=coredump_defaults['enabled']): t.Bool,
            t.Key('path', default=coredump_defaults['path']):
                tx.Path(type='dir', auto_create=True),
            t.Key('backup-count', default=coredump_defaults['backup-count']):
                t.Int[1:],
            t.Key('size-limit', default=coredump_defaults['size-limit']):
                tx.BinarySize,
        }).allow_extra('*'),
    }).allow_extra('*'),
}).merge(config.etcd_config_iv).allow_extra('*')

docker_extra_config_iv = t.Dict({
    t.Key('container'): t.Dict({
        t.Key('swarm-enabled', default=False): t.Bool,
    }).allow_extra('*')
}).allow_extra('*')

default_container_logs_config = {
    'max-length': '10M',  # the maximum tail size
    'chunk-size': '64K',  # used when storing logs to Redis as a side-channel to the event bus
}

agent_etcd_config_iv = t.Dict({
    t.Key('container-logs', default=default_container_logs_config): t.Dict({
        t.Key('max-length', default=default_container_logs_config['max-length']): tx.BinarySize(),
        t.Key('chunk-size', default=default_container_logs_config['chunk-size']): tx.BinarySize(),
    }).allow_extra('*')
}).allow_extra('*')

k8s_extra_config_iv = t.Dict({
    t.Key('registry'): t.Dict({
        t.Key('type'): t.String
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
}).merge(agent_local_config_iv).allow_extra('*')

registry_local_config_iv = t.Dict({
    t.Key('type'): t.String,
    t.Key('addr'): tx.HostPortPair()
}).allow_extra('*')

registry_ecr_config_iv = t.Dict({
    t.Key('type'): t.String,
    t.Key('profile'): t.String,
    t.Key('registry-id'): t.String
}).allow_extra('*')

container_etcd_config_iv = t.Dict({
    t.Key('kernel-uid', optional=True): t.ToInt,
    t.Key('kernel-gid', optional=True): t.ToInt
}).allow_extra('*')
