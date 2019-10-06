import attr

from typing import (
    Any, Dict, List, Mapping
)

'''This file contains API templates for Python K8s Client.
Since I don't prefer using templates provided from vanila k8s client,
all API definitions for Backend.AI Agent will use needs to be defined here.
All API definitions defined here (especially for use with outside this file)
should implement as_dict() method, which returns complete definition in dictionary.
To pass API body from objects defined here, simply put return value of as_dict() method as a body:
e.g) await k8sCoreApi.create_persistent_volume(body=pv.as_dict())'''


class AbstractAPIObject:
    def as_dict(self) -> dict:
        return {}


class AbstractMountSpec:
    def as_volume_mount_dict(self) -> Dict[str, Any]:
        pass

    def as_volume_def_dict(self) -> Dict[str, Any]:
        pass


@attr.s(auto_attribs=True, slots=True)
class HostPathMountSpec(AbstractMountSpec):
    name: str
    hostPath: str
    mountPath: str
    mountType: str
    perm: str

    def as_volume_mount_dict(self) -> Dict[str, Any]:
        mount: Dict[str, Any] = {
            'name': self.name,
            'mountPath': self.mountPath
        }
        if self.perm == 'ro':
            mount['readOnly'] = True
        return mount

    def as_volume_def_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'hostPath': {
                'path': self.hostPath,
                'type': self.mountType
            }
        }


@attr.s(auto_attribs=True, slots=True)
class ConfigMapMountSpec(AbstractMountSpec):
    name: str
    configMapName: str
    configMapKey: str
    mountPath: str

    def as_volume_mount_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'mountPath': self.mountPath,
            'subPath': self.configMapKey
        }

    def as_volume_def_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'configMap': {
                'name': self.configMapName
            }
        }


@attr.s(auto_attribs=True, slots=True)
class PVCMountSpec(AbstractMountSpec):
    subPath: str
    mountPath: str
    type: str
    perm: str = 'ro'
    name: str = ''

    def as_volume_mount_dict(self) -> Dict[str, Any]:
        mount: Dict[str, Any] = {
            'name': self.name,
            'mountPath': self.mountPath,
            'subPath': self.subPath
        }
        if self.perm == 'ro':
            mount['readOnly'] = True
        return mount


class KernelDeployment:
    def __init__(self, kernel_id: str, image: str, krunner_volume: str, arch: str,
                ecr_url: str = '', name: str = 'deployment'):
        self.name = name
        self.image = image
        self.krunner_volume = krunner_volume
        self.arch = arch
        self.ecr_url = ecr_url

        self.hostPathMounts: List[AbstractMountSpec] = []
        self.pvcMounts: List[AbstractMountSpec] = []
        self.execMounts: List[AbstractMountSpec] = []
        self.configMapMounts: List[AbstractMountSpec] = []
        self.cmd: List[str] = []
        self.env: Dict[str, Any] = {}
        self.gpu_count = 0
        self.labels = {'run': name, 'backend.ai/kernel_id': kernel_id}
        self.ports: List[int] = []
        self.baistatic_pvc = ''
        self.vfolder_pvc = ''

        self.resource_def: Dict[str, Any] = {}

    def set_gpus(self, n: int):
        self.gpu_count = n

    def label(self, k: str, v: str):
        self.labels[k] = str(v)

    def append_resource(self, slot, amount):
        self.resource_def[slot] = amount

    def mount_pvc(self, mountSpec: PVCMountSpec):
        mountSpec.name = 'krunner'
        self.pvcMounts.append(mountSpec)

    def mount_exec(self, mountSpec: PVCMountSpec):
        mountSpec.name = 'krunner'
        mountSpec.perm = 'rw'
        self.execMounts.append(mountSpec)

    def mount_vfolder_pvc(self, mountSpec: PVCMountSpec):
        mountSpec.name = 'vfolder'
        self.pvcMounts.append(mountSpec)

    def mount_hostpath(self, mountSpec: HostPathMountSpec):
        self.hostPathMounts.append(mountSpec)

    def mount_configmap(self, mountSpec: ConfigMapMountSpec):
        self.configMapMounts.append(mountSpec)

    def bind_port(self, port: int):
        self.ports.append(port)

    def as_krunner_volumemount_dict(self) -> dict:
        return {
            'name': 'krunner',
            'persistentVolumeClaim': {
                'claimName': self.baistatic_pvc
            }
        }

    def as_vfolder_volumemount_dict(self) -> List[Dict[str, Any]]:
        if len(self.vfolder_pvc) > 0:
            return [{
                'name': 'vfolder',
                'persistentVolumeClaim': {
                    'claimName': self.vfolder_pvc
                }
            }]
        else:
            return []

    def as_dict(self) -> dict:
        base: Dict[str, Any] = {
            'apiVersion': 'apps/v1',
            'kind': 'Deployment',
            'metadata': {
                'name': self.name,
                'labels': self.labels
            },
            'spec': {
                'replicas': 0,
                'selector': {'matchLabels': {'run': self.name}},
                'template': {
                    'metadata': {'labels': {'run': self.name}},
                    'spec': {
                        'containers': [
                            {
                                'name': 'session',
                                'image': self.image,
                                'imagePullPolicy': 'IfNotPresent',
                                'command': ['sh', '/opt/kernel/entrypoint.sh'],
                                'args': self.cmd,
                                'env': [{'name': k, 'value': v} for k, v in self.env.items()],
                                'volumeMounts':    [{
                                    'name': 'workdir',
                                    'mountPath': '/home/work'
                                }, {
                                    'name': 'configdir',
                                    'mountPath': '/home/config'
                                }, {
                                    'name': 'jupyter',
                                    'mountPath': '/home/work/.jupyter'
                                }, {
                                    'name':    'krunner',
                                    'mountPath': '/opt/backend.ai',
                                    'subPath': self.krunner_volume,
                                    'readOnly': True
                                }] + [x.as_volume_mount_dict() for x in self.configMapMounts +
                                        self.hostPathMounts + self.pvcMounts],
                                'ports': [{'containerPort': x} for x in self.ports],
                                }
                        ],
                        'imagePullSecrets': [{
                            'name': 'backend-ai-registry-secret'
                        }] if len(self.ecr_url) == 0 else [{}],
                        'volumes': [{
                            'name': 'workdir',
                            'emptyDir': {}
                        }, {
                            'name': 'configdir',
                            'emptyDir': {}
                        }, {
                            'name': 'jupyter',
                            'emptyDir': {}
                        }, self.as_krunner_volumemount_dict()
                        ] + self.as_vfolder_volumemount_dict() +
                        [x.as_volume_def_dict() for x in self.configMapMounts + self.hostPathMounts]
                    }
                }
            }
        }

        if self.resource_def:
            base['spec']['template']['spec']['containers'][0]['resources'] = {
                'limits': self.resource_def
            }
        return base


class ConfigMap(AbstractAPIObject):

    def __init__(self, kernel_id, name: str):
        self.items: Dict[str, str] = {}
        self.name = f'{kernel_id}-{name}'
        self.labels = {'backend.ai/kernel_id': kernel_id}

    def put(self, key: str, value: str):
        self.items[key] = value

    def as_dict(self) -> dict:
        return {
            'apiVersion': 'v1',
            'kind': 'ConfigMap',
            'metadata': {
                'name': self.name,
                'labels': self.labels
            },
            'data': self.items
        }


class Service(AbstractAPIObject):
    def __init__(self, kernel_id: str, name: str, deployment_name: str, container_port: list,
                service_type: str = 'NodePort'):
        self.name = f'{name}-{kernel_id}'
        self.deployment_name = deployment_name
        self.container_port = container_port
        self.service_type = service_type
        self.labels = {'run': self.name, 'backend.ai/kernel_id': kernel_id}

    def label(self, k: str, v: str):
        self.labels[k] = str(v)

    def as_dict(self) -> dict:
        base: Dict[str, Any] = {
            'apiVersion': 'v1',
            'kind': 'Service',
            'metadata': {
                'name': self.name,
                'labels': self.labels
            },
            'spec': {
                'ports': [{'targetPort': x[0], 'port': x[0], 'name': x[1]} for x in self.container_port],
                'selector': {'run': self.deployment_name}
            }
        }
        if self.service_type == 'NodePort':
            base['spec']['type'] = 'NodePort'
        elif self.service_type == 'LoadBalancer':
            base['spec']['type'] = 'LoadBalancer'
        return base


class NFSPersistentVolume(AbstractAPIObject):

    def __init__(self, server, path, name, capacity):
        self.server = server
        self.path = path
        self.name = name
        self.capacity = capacity
        self.labels = {}
        self.options = []

    def label(self, k, v):
        self.labels[k] = v

    def as_dict(self) -> dict:
        return {
            'apiVersion': 'v1',
            'kind': 'PersistentVolume',
            'metadata': {
                'name': self.name,
                'labels': self.labels
            },
            'spec': {
                'capacity': {
                    'storage': self.capacity
                },
                'accessModes': ['ReadWriteMany'],
                'nfs': {
                    'server': self.server,
                    'path': self.path
                },
                'mountOptions': self.options
            }
        }


class NFSPersistentVolumeClaim(AbstractAPIObject):

    def __init__(self, name, pv_name, capacity):
        self.name = name
        self.pv_name = pv_name
        self.capacity = capacity
        self.labels = {}

    def label(self, k, v):
        self.labels[k] = v

    def as_dict(self) -> dict:
        base = {
            'apiVersion': 'v1',
            'kind': 'PersistentVolumeClaim',
            'metadata': {
                'name': self.name,
                'labels': self.labels
            },
            'spec': {
                'resources': {
                    'requests': {
                        'storage': self.capacity
                    }
                },
                'accessModes': ['ReadWriteMany'],
                'storageClassName': ''
            }
        }
        return base
