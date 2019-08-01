import yaml
from typing import Dict, List

'''This file contains API templates for Python K8s Client. 
Since I don't prefer using templates provided from vanila k8s client, 
all API definitions for Backend.AI Agent will use needs to be defined here.
All API definitions defined here (especially for use with outside this file) 
should implement to_dict() method, which returns complete definition in dictionary.
To pass API body from objects defined here, simply put return value of to_dict() method as a body:
e.g) await k8sCoreApi.create_persistent_volume(body=pv.to_dict())'''

class AbstractAPIObject:
  def to_dict(self) -> dict:
    return {}
  
class HostPathMountSpec:
  def __init__(self, name: str, hostPath: str, mountPath: str, mountType: str, perm: str):
    self.name = name
    self.hostPath = hostPath
    self.mountPath = mountPath
    self.type = mountType
    self.perm = perm
  
  def volumemount_dict(self) -> str:
    mount = {
      'name': self.name,
      'mountPath': self.mountPath
    }
    if self.perm == 'ro':
      mount['readOnly'] = True
    return mount

  def volumedef_dict(self) -> str:
    return {
      'name': self.name,
      'hostPath': {
        'path': self.hostPath,
        'type': self.type
      }
    }

class ConfigMapMountSpec:
  def __init__(self, name: str, configMapName: str, configMapKey: str, mountPath: str):
    self.name = name
    self.configMapName = configMapName
    self.configMapKey = configMapKey
    self.mountPath = mountPath

  def volumemount_dict(self) -> str:
    return {
      'name': self.name,
      'mountPath': self.mountPath,
      'subPath': self.configMapKey
    }
  
  def volumedef_dict(self) -> str:
    return {
      'name': self.name,
      'configMap': {
        'name': self.configMapName
      }
    }

class PVCMountSpec:
  def __init__(self, subPath: str, mountPath: str, mountType: str, perm: str='ro'):
    self.name = ''
    self.mountPath = mountPath
    self.subPath = subPath
    self.type = mountType
    self.perm = perm

  def volumemount_dict(self) -> str:
    mount = {
      'name': self.name,
      'mountPath': self.mountPath,
      'subPath': self.subPath
    }
    if self.perm == 'ro':
      mount['readOnly'] = True
    return mount

class KernelDeployment:
  hostPathMounts: List[HostPathMountSpec]
  configMapMounts: List[ConfigMapMountSpec]
  cmd: List[str]
  env: Dict[str, str]
  ports: List[int]

  def __init__(self, kernel_id:str, image:str, distro:str, arch:str, ecr_url: str='', name: str='deployment'):
    self.name = name
    self.image = image
    self.distro = distro
    self.arch = arch
    self.ecr_url = ecr_url

    self.hostPathMounts = []
    self.pvcMounts = []
    self.execMounts = []
    self.configMapMounts = []
    self.cmd = []
    self.env = {}
    self.labels = { 'run': name, 'backend.ai/kernel_id': kernel_id }
    self.ports = []
    self.krunnerPath = ''
    self.baistatic_pvc = ''
    self.vfolder_pvc = ''

  def label(self, k: str, v: str):
    self.labels[k] = str(v)
  
  def mount_pvc(self, mountSpec: PVCMountSpec):
    mountSpec.name = self.name + '-krunner'
    self.pvcMounts.append(mountSpec)

  def mount_exec(self, mountSpec: PVCMountSpec):
    mountSpec.name = self.name + '-krunner'
    mountSpec.perm = 'rw'
    self.execMounts.append(mountSpec)
  
  def mount_vfolder_pvc(self, mountSpec: PVCMountSpec):
    mountSpec.name = self.name + '-vfolder'
    self.pvcMounts.append(mountSpec)

  def mount_hostpath(self, mountSpec: HostPathMountSpec):
    self.hostPathMounts.append(mountSpec)
  
  def mount_configmap(self, mountSpec: ConfigMapMountSpec):
    self.configMapMounts.append(mountSpec)

  def bind_port(self, port: int):
    self.ports.append(port)

  def krunner_volumemount(self) -> dict:
    return {
      'name': self.name + '-krunner',
      'persistentVolumeClaim': {
        'claimName': self.baistatic_pvc
      }
    }

  def vfolder_volumemount(self) -> dict:
    if len(self.vfolder_pvc) > 0:
      return [{
        'name': self.name + '-vfolder',
        'persistentVolumeClaim': {
          'claimName': self.vfolder_pvc
        }
      }]
    else:
      return []


  def to_dict(self) -> dict:
    distro = self.distro
    return {
      'apiVersion': 'apps/v1',
      'kind': 'Deployment',
      'metadata': {
        'name': self.name,
        'labels': self.labels
      },
      'spec': {
        'replicas': 0,
        'selector': { 'matchLabels': { 'run': self.name } },
        'template': {
          'metadata': { 'labels': { 'run': self.name } },
          'spec': {
            'containers': [
              {
                'name': self.name + '-session',
                'image': self.image,
                'imagePullPolicy': 'IfNotPresent',
                'command': ['sh', '/opt/kernel/entrypoint.sh'],
                'args': self.cmd,
                'env': [{ 'name': k, 'value': v } for k, v in self.env.items()],
                'volumeMounts':  [{
                    'name': self.name + '-workdir',
                    'mountPath': '/home/work'
                }, {
                    'name': self.name + '-jupyter',
                    'mountPath': '/home/work/.jupyter'
                }, {
                    'name':  self.name + '-krunner',
                    'mountPath': '/opt/backend.ai',
                    'subPath': f'backendai-krunner.{distro}',
                    'readOnly': True
                }] + [x.volumemount_dict() for x in self.configMapMounts + self.hostPathMounts + self.pvcMounts],
                'ports': [{ 'containerPort': x } for x in self.ports],
                'imagePullSecrets': {
                  'name': 'backend-ai-registry-secret'
                } if len(self.ecr_url) == 0 else {}
              }
            ],
            'volumes': [{
                  'name': self.name + '-workdir',
                  'emptyDir': {}
              }, { 
                  'name': self.name + '-jupyter',
                  'emptyDir': {}
              }, self.krunner_volumemount()
            ] + self.vfolder_volumemount() + [x.volumedef_dict() for x in self.configMapMounts + self.hostPathMounts]
          }
        }
      }
    }

class ConfigMap(AbstractAPIObject):
  items: Dict[str, str] = {}

  def __init__(self, kernel_id, name: str): 
    self.name = name
    self.labels = { 'backend.ai/kernel_id': kernel_id }
  
  def put(self, key: str, value: str): 
    self.items[key] = value

  def to_dict(self) -> dict:
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
  def __init__(self, kernel_id: str, name: str, deployment_name: str, container_port: list, service_type='NodePort'):
    self.name = name
    self.deployment_name = deployment_name
    self.container_port = container_port
    self.service_type = service_type
    self.labels = { 'run': self.name, 'backend.ai/kernel_id': kernel_id }
  
  def to_dict(self) -> dict:
    base = {
      'apiVersion': 'v1',
      'kind': 'Service',
      'metadata': {
        'name': self.name,
        'labels': self.labels
      },
      'spec': {
        'ports': [ { 'targetPort': x[0], 'port': x[0], 'name': x[1] } for x in self.container_port ],
        'selector': { 'run': self.deployment_name }
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
  
  def to_dict(self) -> dict:
    return {
      'apiVersion': 'v1',
      'kind': 'PersistentVolume',
      'metadata': {
        'name': self.name,
        'labels': self.labels
      },
      'spec': {
        'capacity': {
          'storage': self.capacity + 'Gi'
        },
        'accessModes': [ 'ReadWriteMany' ],
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
  
  def to_dict(self) -> dict:
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
          'storage': self.capacity + 'Gi'
          }
        },
        'accessModes': [ 'ReadWriteMany' ],
        'storageClassName': ''
      }
    }
    return base


