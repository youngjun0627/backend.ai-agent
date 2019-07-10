import yaml
from typing import Dict, List
  
class HostPathMountSpec:
  def __init__(self, name: str, hostPath: str, mountPath: str, mountType: str):
    self.name = name
    self.hostPath = hostPath
    self.mountPath = mountPath
    self.type = mountType
  
  def volumemount_dict(self) -> str:
    return {
      'name': self.name,
      'mountPath': self.mountPath
    }

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

class KernelDeployment:
  hostPathMounts: List[HostPathMountSpec]
  configMapMounts: List[ConfigMapMountSpec]
  cmd: List[str]
  env: Dict[str, str]
  ports: List[int]

  def __init__(self, name: str='deployment'):
    self.name = name

    self.hostPathMounts = []
    self.configMapMounts = []
    self.cmd = []
    self.env = {}
    self.ports = []
  
  def mount_hostpath(self, mountSpec: HostPathMountSpec):
    self.hostPathMounts.append(mountSpec)
  
  def mount_configmap(self, mountSpec: ConfigMapMountSpec):
    self.configMapMounts.append(mountSpec)

  def bind_port(self, port: int):
    self.ports.append(port)
  
  def to_dict(self) -> dict:
    return {
      'apiVersion': 'apps/v1',
      'kind': 'Deployment',
      'metadata': {
        'name': self.name,
        'labels': { 'run': self.name }
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
                'image': 'lablup/python:3.6-ubuntu18.04',
                'imagePullPolicy': 'IfNotPresent',
                'command': ['/opt/backend.ai/bin/entrypoint.sh'],
                'args': self.cmd,
                'env': [{ 'name': k, 'value': v } for k, v in self.env.items()],
                'volumeMounts':  [{
                    'name': self.name + '-workdir',
                    'mountPath': '/home/work'
                }] +[x.volumemount_dict() for x in self.configMapMounts + self.hostPathMounts],
                'ports': [{ 'containerPort': x } for x in self.ports],
                
              }
            ],
            'volumes': [{
                'name': self.name + '-workdir',
                'emptyDir': {}
            }] + [x.volumedef_dict() for x in self.configMapMounts + self.hostPathMounts]
          }
        }
      }
    }

class ConfigMap:
  items: Dict[str, str] = {}

  def __init__(self, name: str): 
    self.name = name
  
  def put(self, key: str, value: str): 
    self.items[key] = value

  def to_dict(self) -> dict:
    return {
      'apiVersion': 'v1',
      'kind': 'ConfigMap',
      'metadata': {
        'name': self.name
      },
      'data': self.items
    }

class Service:
  def __init__(self, name: str, deployment_name: str, container_port: list, service_type='NodePort'):
    self.name = name
    self.deployment_name = deployment_name
    self.container_port = container_port
    self.service_type = service_type
  
  def to_dict(self) -> dict:
    base = {
      'apiVersion': 'v1',
      'kind': 'Service',
      'metadata': {
        'name': self.name,
        'labels': { 'run': self.name }
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
