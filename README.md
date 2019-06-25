# Backend.AI Agent

The Backend.AI Agent is a small daemon that does:

* Reports the status and available resource slots of a worker to the manager
* Routes code execution requests to the designated kernel container
* Manages the lifecycle of kernel containers (create/monitor/destroy them)

## Package Structure

* `ai.backend`
  - `agent`: The agent daemon implementation


## Installation

Please visit [the installation guides](https://github.com/lablup/backend.ai/wiki).

### For development

#### Prerequisites

* `libnsappy-dev` or `snappy-devel` system package depending on your distro
* Python 3.6 or higher with [pyenv](https://github.com/pyenv/pyenv)
and [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) (optional but recommneded)
* Docker 18.03 or later with docker-compose (18.09 or later is recommended)

Clone [the meta repository](https://github.com/lablup/backend.ai) and install a "halfstack" configuration.
The halfstack configuration installs and runs dependency daemons such as etcd in the background.

```console
$ git clone https://github.com/lablup/backend.ai halfstack
$ cd halfstack
$ docker-compose -f docker-compose.halfstack.yml up -d
```

Then prepare the source clone of the agent as follows.
First install the current working copy.

```console
$ git clone https://github.com/lablup/backend.ai-agent agent
$ cd agent
$ pyenv virtualenv venv-agent
$ pyenv local venv-agent
$ pip install -U pip setuptools
$ pip install -U -r requirements-dev.txt
```

From now on, let's assume all shell commands are executed inside the virtualenv.

Before running, you first need to prepare "the kernel runner environment", which is
composed of a dedicated Docker image that is mounted into kernel containers at
runtime.
Since our kernel images have two different base Linux distros, Alpine and Ubuntu,
you need to build/download the krunner-env images twice as follows.

For development:
```console
$ python -m ai.backend.agent.kernel build-krunner-env alpine3.8
$ python -m ai.backend.agent.kernel build-krunner-env ubuntu16.04
```
or you pull the matching version from the Docker Hub (only supported for already
released versions):
```console
$ docker pull lablup/backendai-krunner-env:19.03-alpine3.8
$ docker pull lablup/backendai-krunner-env:19.03-ubuntu16.04
```

### Halfstack (single-node testing)

With the halfstack, you can run the agent simply.
Note that you need a working manager running with the halfstack already!

```console
$ cp config/halfstack.toml ./manager.toml
$ cp config/halfstack.alembic.ini alembic.ini
```

```console
$ mkdir -p "$HOME/scratches"
$ python -m ai.backend.agent.server --scratch-root=$HOME/scratches --debug
```

To run tests:

```console
$ python -m flake8 src tests
$ python -m pytest -m 'not integration' tests
```


## Deployment

### Running from a command line

The minimal command to execute:

```sh
python -m ai.backend.agent.server
```

The agent reads most configurations from the given etcd v3 server where
the cluster administrator or the Backend.AI manager stores all the necessary
settings.

The etcd address and namespace must match with the manager to make the agent
paired and activated.
By specifying distinguished namespaces, you may share a single etcd cluster with multiple
separate Backend.AI clusters.

By default the agent uses `/var/cache/scratches` directory for making temporary
home directories used by kernel containers (the `/home/work` volume mounted in
containers).  Note that the directory must exist in prior and the agent-running
user must have ownership of it.  You can change the location by
`--scratch-root` option.

For more arguments and options, run the command with ``--help`` option.

### Example config for agent server/instances

`/etc/supervisor/conf.d/agent.conf`:

```dosini
[program:backend.ai-agent]
user = user
stopsignal = TERM
stopasgroup = true
command = /home/user/run-agent.sh
```

`/home/user/run-agent.sh`:

```sh
#!/bin/sh
source /home/user/venv-agent/bin/activate
exec python -m ai.backend.agent.server
```

## Networking

Basically all TCP ports must be transparently open to the manager.
The manager and agent should run in the same local network or different
networks reachable via VPNs.

The operation of agent itself does not require both incoming/outgoing access to
the public Internet, but if the user's computation programs need, the docker
containers should be able to access the public Internet (maybe via some
corporate firewalls).

Several optional features such as automatic kernel image updates may require
outgoing public Internet access from the agent as well.
