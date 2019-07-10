import kubernetes_asyncio
import asyncio 
import sys

async def main(name):
    await kubernetes_asyncio.config.load_kube_config()
    v1 = kubernetes_asyncio.client.CoreV1Api()
    v1apps = kubernetes_asyncio.client.AppsV1Api()

    await v1.delete_namespaced_service(f'{name}-service', 'backend-ai')
    await v1.delete_namespaced_config_map(f'{name}-configmap', 'backend-ai')
    await v1apps.delete_namespaced_deployment(f'{name}', 'backend-ai')

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(sys.argv[1]))