Sorna Agent
===========

Package Structure
-----------------

 * sorna
   * agent: The agent server implementation

Installation
------------

Sorna Agent requires Python 3.5 or higher.
We highly recommend to use [pyenv](https://github.com/yyuu/pyenv) to use an
isolated setup with custom Python versions that might not be supported by your
OS or Linux distributions by default.

```sh
pip install sorna-agent
```

### For development:

We recommend to use virtual environments in Python.
You may share a virtual environment with other Sorna projects.

```sh
git clone https://github.com/lablup/sorna-agent.git
python -m venv venv-sorna
source venv-sorna/bin/activate
python setup.py develop --user
```

Deployment
----------

### Running from a command line:

To handle uploads of generated files to AWS S3, you need to set several
environment variables.  If they are not set, the file upload feature is
disabled.  Currently we only support S3-based uploads.

```sh
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_REGION="..."     # e.g., ap-northeast-2
export AWS_S3_BUCKET="..."  # e.g., my-precious-sorna
python -m sorna.agent.server --manager-addr tcp://localhost:5001 --max-kernels 15
```

For details about arguments, run the server with `--help`.

### Example supervisord config:

```dosini
[program:sorna-agent]
stopsignal = TERM
stopasgroup = true
command = /home/sorna/run-agent.sh
environment = AWS_ACCESS_KEY_ID="...",AWS_SECRET_ACCESS_KEy="...",AWS_REGION="...",AWS_S3_BUCKET="..."
```

### TCP Port numbers to open:

 * 5001: ZeroMQ-based internal protocol
 * Docker REPL containers will use automatically-mapped host-side port numbers,
   and they do not need to be open to other hosts (localhost-only).
   Inside containers, the REPL daemons use the fixed port number: 2001.
