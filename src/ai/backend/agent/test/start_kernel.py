from ai.backend.agent.k8sconfs import (
    ConfigMapMountSpec,
    HostPathMountSpec,
    KernelDeployment,
    ConfigMap,
    NodePort
)
import kubernetes_asyncio
import asyncio 
import json
from pprint import pprint

async def main():
    await kubernetes_asyncio.config.load_kube_config()

    v1 = kubernetes_asyncio.client.CoreV1Api()
    v1Apps = kubernetes_asyncio.client.AppsV1Api()

    distro = 'ubuntu16.04'
    arch = 'x86_64'

    deployment = KernelDeployment()
    environ = {
        'LOCAL_USER_ID': '501',
        'OPENBLAS_NUM_THREADS': '1',
        'OMP_NUM_THREADS': '1',
        'NPROC': '1'
    }

    def _mount(hostPath: str, mountPath: str, mountType: str):
        name = mountPath.split('/')[-1].replace('.', '-')
        deployment.mount_hostpath(HostPathMountSpec(name, hostPath, mountPath, mountType))

    _mount('/opt/backend.ai/krunner/env.ubuntu16.04',
            '/opt/backend.ai', 'Directory')
    _mount('/opt/backend.ai/runner/entrypoint.sh', 
            '/opt/backend.ai/bin/entrypoint.sh', 'File')
    _mount(f'/opt/backend.ai/runner/su-exec.{distro}.bin',
            '/opt/backend.ai/bin/su-exec', 'File')
    _mount(f'/opt/backend.ai/runner/jail.{distro}.bin',
            '/opt/backend.ai/bin/jail', 'File')
    _mount(f'/opt/backend.ai/hook/libbaihook.{distro}.{arch}.so',
            '/opt/backend.ai/hook/libbaihook.so', 'File')
    _mount('/opt/backend.ai/kernel',
            '/opt/backend.ai/lib/python3.6/site-packages/ai/backend/kernel', 'Directory')
    _mount('/opt/backend.ai/helpers',
            '/opt/backend.ai/lib/python3.6/site-packages/ai/backend/helpers', 'Directory')
    _mount('/opt/backend.ai/runner/jupyter-custom.css',
            '/home/work/.jupyter/custom/custom.css', 'File')
    _mount('/opt/backend.ai/runner/logo.svg',
            '/home/work/.jupyter/custom/logo.svg', 'File')
    _mount('/opt/backend.ai/runner/roboto.ttf',
            '/home/work/.jupyter/custom/roboto.ttf', 'File')
    _mount('/opt/backend.ai/runner/roboto-italic.ttf',
            '/home/work/.jupyter/custom/roboto-italic.ttf', 'File')
    environ['LD_PRELOAD'] = '/opt/backend.ai/hook/libbaihook.so'

    resource_txt = '''CID=8b918d742092fc9481408a03e108ea921a245c79450aad1536f2dbc840b8170a
SCRATCH_SIZE=0m
MOUNTS=
SLOTS={"cpu": "1", "mem": "268435456"}
IDLE_TIMEOUT=600
CPU_SHARES=0:1
MEM_SHARES=root:256m
'''
    environ_txt = ''
    for k, v in environ.items():
        environ_txt += f'{k}={v}\n'

    cmdargs = ["/opt/backend.ai/bin/python",
            "-m", "ai.backend.kernel", "python", "/usr/bin/python3"]

    deployment_name = 'test-deployment'

    configmap = ConfigMap(deployment.name + '-configmap')
    configmap.put('environ', environ_txt)
    configmap.put('resource', resource_txt)

    deployment.cmd = cmdargs
    deployment.env = environ
    deployment.mount_configmap(ConfigMapMountSpec('environ', configmap.name, 'environ', '/home/config/environ.txt'))
    deployment.mount_configmap(ConfigMapMountSpec('resource', configmap.name, 'resource', '/home/config/resource.txt'))

    service = NodePort(deployment.name + '-service', deployment.name, [ (2000, 'repl-in'), (2001, 'repl-out') ])

    try:
        service_api_response = await v1.create_namespaced_service('backend-ai', body=service.to_dict())
    except:
        raise
    pprint(service_api_response)

    try:
        cm_api_response = await v1.create_namespaced_config_map('backend-ai', body=configmap.to_dict(), pretty='pretty_example')
    except:
        await v1.delete_namespaced_service(service.name, 'backend-ai')
        raise
    pprint(cm_api_response)

    try:
        print(json.dumps(deployment.to_dict(), indent=2))
        deployment_api_response = await v1Apps.create_namespaced_deployment('backend-ai', body=deployment.to_dict(), pretty='pretty_example')
    except:
        await v1.delete_namespaced_service(service.name, 'backend-ai')
        await v1.delete_namespaced_config_map(configmap.name, 'backend-ai')
        raise
    pprint(deployment_api_response)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())