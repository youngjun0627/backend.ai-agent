import paramiko
import sys
import os

if len(sys.argv) == 1:
    print('Usage: REGISTRY_DOMAIN=<regstry domain> python push_certificate.py <IPs seperated by space>')
    exit(1)

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

domain = os.environ['REGISTRY_DOMAIN']
if 'REGISTRY_PORT' in os.environ.keys():
    port = os.environ['REGISTRY_PORT']
else:
    port = '5000'

certs = os.listdir('certs')
for cert in certs:
    if cert == f'{domain}.crt':
        break
else:
    print(f'SSL Certificate {domain}.crt not found!')
    exit(1)

for ip in sys.argv[1:]:
    ssh.connect(ip)

    stdin, stdout, stderr = ssh.exec_command('pwd')
    pwd = stdout.readlines()[0].strip().replace('\n', '')
    print(f'pwd: {pwd}')

    sftp = ssh.open_sftp()
    sftp.put(f'./certs/{domain}.crt', f'{pwd}/ca.crt')
    sftp.close()

    stdin, stdout, stderr = ssh.exec_command(f'sudo mkdir -p /etc/docker/certs.d/{domain}:{port}/ && sudo mv {pwd}/ca.crt /etc/docker/certs.d/{domain}:{port}')

    print('STDOUT:')
    print(''.join(stdout.readlines()))
    print('STDERR:')
    print(''.join(stderr.readlines()))