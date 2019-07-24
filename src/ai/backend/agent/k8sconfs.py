import yaml
from typing import Dict, List
  
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
  def __init_(self, subPath: str, mountPath: str, mountType: str, perm: str='ro'):
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

  def __init__(self, kernel_id:str, image:str, arch:str, registry_type:str, name: str='deployment'):
    self.name = name
    self.image = image
    self.arch = arch
    self.registry_type = registry_type

    self.hostPathMounts = []
    self.pvcMounts = []
    self.configMapMounts = []
    self.cmd = []
    self.env = {}
    self.labels = { 'run': name, 'backend.ai/kernel_id': kernel_id }
    self.ports = []
    self.krunnerPath = ''
    self.baistatic_pvc = ''
    self.vfolder_pvc = ''
    self.krunner_source = 'local'

  def label(self, k: str, v: str):
    self.labels[k] = str(v)
  
  def mount_pvc(self, mountSpec: PVCMountSpec):
    mountSpec.name = self.name + '-krunner'
    self.pvcMounts.append(mountSpec)

  def mount_vfolder_pvc(self, mountSpec: PVCMountSpec):
    mountSpec.name = self.vfolder_pvc
    self.pvcMounts.append(mountSpec)

  def mount_hostpath(self, mountSpec: HostPathMountSpec):
    self.hostPathMounts.append(mountSpec)
  
  def mount_configmap(self, mountSpec: ConfigMapMountSpec):
    self.configMapMounts.append(mountSpec)

  def bind_port(self, port: int):
    self.ports.append(port)

  def krunner_volumemount(self):
    if self.baistatic_pvc:
      return {
        'name': self.name + '-krunner',
        'persistentVolumeClaim': {
          'claimName': self.baistatic_pvc
        }
      }
    else:
      return {
        'name': self.name + '-krunner',
        'hostPath': {
          'path': self.krunnerPath,
          'type': 'Directory'
        }
      }

  def initcontainer(self) -> dict:
    if self.krunner_source == 'image':
      distro = self.image.split(':')[-1]
      arch = self.arch

      return [{
        'name': self.name + '-krunner',
        'image': f'lablup/env:{distro}',
        'imagePullPolicy': 'IfNotPresent',
        'volumeMounts': [{
            'name': self.name + '-krunner',
            'mountPath': '/root/image.tar.xz',
            'subPath': f'krunner-env.{distro}.{arch}.tar.xz'
          }, {
            'name': 'krunner-provider',
            'mountPath': '/provider'
        }]
      }]
    else:
      return []
  
  def to_dict(self) -> dict:
    initcontainer = self.initcontainer()

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
            'initContainers': initcontainer,
            'containers': [
              {
                'name': self.name + '-session',
                'image': self.image,
                'imagePullPolicy': 'IfNotPresent',
                'command': ['/opt/kernel/entrypoint.sh'],
                'args': self.cmd,
                'env': [{ 'name': k, 'value': v } for k, v in self.env.items()],
                'volumeMounts':  [{
                    'name': self.name + '-workdir',
                    'mountPath': '/home/work'
                }, {
                    'name': self.name + '-jupyter',
                    'mountPath': '/home/work/.jupyter'
                }] + [{
                    'name':  'krunner-provider',
                    'mountPath': '/opt/kernel'
                }] if len(initcontainer) > 0 else [] + [x.volumemount_dict() for x in self.configMapMounts + self.hostPathMounts + self.pvcMounts],
                'ports': [{ 'containerPort': x } for x in self.ports],
                'imagePullSecrets': {
                  'name': 'backend-ai-registry-secret'
                } if self.registry_type == 'local' else {}
              }
            ],
            'volumes': [{
                'name': self.name + '-workdir',
                'emptyDir': {}
            }, { 
                'name': self.name + '-jupyter',
                'emptyDir': {}
            }, self.krunner_volumemount() , {
                'name': 'krunner-provider', 
                'emptyDir': {}
            }] + [x.volumedef_dict() for x in self.configMapMounts + self.hostPathMounts]
          }
        }
      }
    }

class ConfigMap:
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

class Service:
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

class NFSPersistentVolume:

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

class NFSPersistentVolumeClaim:

  def __init__(self, name, pv_name, capacity):
    self.name = name
    self.pv_name = pv_name
    self.capacity = capacity
    self.labels = {}

  def label(self, k, v):
    self.labels[k] = v
  
  def to_dict(self) -> dict:
    return {
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
        'accessModes': [ 'ReadWriteMany' ]
      }
    }