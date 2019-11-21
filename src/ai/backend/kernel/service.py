import json
from pathlib import Path
from typing import Any, List, Union, MutableMapping

from . import service_actions
from .exception import DisallowedArgument, DisallowedEnvironment, InvalidServiceDefinition


# Service Port Template
'''
{
    "prestart": [
      {
        "action": "write_tempfile",
        "args": {
          "body": "c.NotebookApp.allow_root = True\n"
        },
        "ref": "jupyter_cfg"
      },
      {
        "action": "writelines",
        "args": {
          "filename": "{jupyter_cfg}",
          "body": [
            "c.NotebookApp.ip = \"0.0.0.0\"",
            "c.NotebookApp.port = {ports[0]}",
            "c.NotebookApp.token = \"\"",
            "c.FileContentsManager.delete_to_trash = False"
          ],
          "mode": "w+"
        }
      }
    ],
    "command": "{runtime_path}",
    "args": "-m jupyterlab --no-browser --config {jupyter_cfg}"
}
'''


class ServiceDefinition:
    def __init__(
            self, prestart_actions: List[Any], noop: bool, command: List[str],
            env: MutableMapping[str, str], url_template: str, allowed_envs: List[str],
            allowed_arguments: List[str],
            default_arguments: MutableMapping[str, Union[None, str, List[str]]]):
        self.prestart_actions: List[Any] = prestart_actions
        self.raw_command: List[str] = command
        self.noop = noop
        self.raw_env: MutableMapping[str, str] = env
        self.url_template = url_template
        self.allowed_envs = allowed_envs
        self.allowed_arguments = allowed_arguments
        self.default_arguments = default_arguments


class ServiceParser:
    def __init__(self, variables: MutableMapping[str, str]):
        self.variables: MutableMapping[str, str] = variables
        self.services: MutableMapping[str, ServiceDefinition] = {}

    async def parse(self, path: Path):
        for service_def_file in path.glob('*.json'):
            print(f'found file {service_def_file}')
            with open(service_def_file.absolute(), 'r') as fr:
                service_def = json.loads(fr.read())

            for required in ['command']:
                if required not in service_def.keys():
                    print(f'{required} not fullfilled')
                    raise InvalidServiceDefinition(f'{required} not fullfilled')

            name = service_def_file.name
            raw_cmd = service_def['command']
            noop = False
            prestart_actions: List[Any] = []
            raw_env: MutableMapping[str, str] = {}
            raw_url_template: str = ''
            allowed_envs: List[str] = []
            allowed_arguments: List[str] = []
            default_arguments: MutableMapping[str, Union[None, str, List[str]]] = {}

            if 'prestart' in service_def.keys():
                prestart_actions = service_def['prestart']
            if 'noop' in service_def.keys():
                noop = service_def['noop']
            if 'env' in service_def.keys():
                raw_env = service_def['env']
            if 'url_template' in service_def.keys():
                raw_url_template = service_def['url_template']
            if 'allowed_envs' in service_def.keys():
                allowed_envs = service_def['allowed_envs']
            if 'allowed_arguments' in service_def.keys():
                allowed_arguments = service_def['allowed_arguments']
            if 'default_arguments' in service_def.keys():
                default_arguments = service_def['default_arguments']

            self.services[name.replace('.json', '')] = \
                ServiceDefinition(
                    prestart_actions, noop, raw_cmd,
                    raw_env, raw_url_template, allowed_envs,
                    allowed_arguments, default_arguments)

    async def start_service(
                self, service_name: str, default_envs: List[str],
                opts: MutableMapping[str, Any]):
        if service_name not in self.services.keys():
            return None, None
        service = self.services[service_name]
        if service.noop:
            return [], {}

        for action in service.prestart_actions:
            ret = await getattr(service_actions, action['action'])(self.variables, **action['args'])
            if action.get('ref') is not None:
                self.variables[action['ref']] = ret

        cmdargs, env = [], {}

        for arg in service.raw_command:
            cmdargs.append(arg.format_map(self.variables))

        additional_arguments = dict(service.default_arguments)
        if 'arguments' in opts.keys():
            for argname, argvalue in opts['arguments'].items():
                if argname not in service.allowed_arguments:
                    raise DisallowedArgument(
                        f'Argument {argname} not allowed for service {service_name}')
                additional_arguments[argname] = argvalue

        for env_name, env_value in service.raw_env.items():
            env[env_name.format_map(self.variables)] = env_value.format_map(self.variables)

        if 'envs' in opts.keys():
            for envname, envvalue in opts['envs'].items():
                if envname not in service.allowed_envs:
                    raise DisallowedEnvironment(
                        f'Environment variable {envname} not allowed for service {service_name}')
                elif envname in default_envs:
                    raise DisallowedEnvironment(f'Environment variable {envname} can\'t be overwritten')
                env[envname] = envvalue

        for arg_name, arg_value in additional_arguments.items():
            cmdargs.append(arg_name)
            if isinstance(arg_value, str):
                cmdargs.append(arg_value)
            elif isinstance(arg_value, list):
                cmdargs += arg_value

        return cmdargs, env

    async def get_apps(self, selected_service=''):
        def _format(service_name):
            service_info = {'name': service_name}
            service = self.services[service_name]
            if len(service.url_template) > 0:
                service_info['url_template'] = service.url_template
            if len(service.allowed_arguments) > 0:
                service_info['allowed_arguments'] = service.allowed_arguments
            if len(service.allowed_envs) > 0:
                service_info['allowed_envs'] = service.allowed_envs
            return service_info

        apps = []
        if selected_service:
            if selected_service in self.services.keys():
                apps.append(_format(selected_service))
        else:
            for service_name in self.services.keys():
                apps.append(_format(service_name))
        return apps
