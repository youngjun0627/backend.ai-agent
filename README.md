# Backend.AI Agent

The Backend.AI Agent is a small daemon that does:

* Reports the status and available resource slots of a worker to the manager
* Routes code execution requests to the designated kernel container
* Manages the lifecycle of kernel containers (create/monitor/destroy them)

## Package Structure

* `ai.backend`
  - `agent`: The agent package
    - `server`: The agent daemon which communicates with the manager and the Docker daemon
    - `watcher`: A side-by-side daemon which provides a separate HTTP endpoint for accessing the status
      information of the agent daemon and manipulation of the agent's systemd service


## Installation

Please visit [the installation guides](https://github.com/lablup/backend.ai/wiki).

### For development

#### Prerequisites

* `libnsappy-dev` or `snappy-devel` system package depending on your distro
* Python 3.6 or higher with [pyenv](https://github.com/pyenv/pyenv)
and [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) (optional but recommneded)
* Docker 18.03 or later with docker-compose (18.09 or later is recommended)

First, you need **a working manager installation**.
For the detailed instructions on installing the manager, please refer
[the manager's README](https://github.com/lablup/backend.ai-manager/blob/master/README.md)
and come back here again.

#### Common steps

Next, prepare the source clone of the agent and install from it as follows.

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

### Halfstack (single-node development & testing)

With the halfstack, you can run the agent simply.
Note that you need a working manager running with the halfstack already!

#### Recommended directory structure

* `backend.ai-dev`
  - `manager` (git clone from [the manager repo](https://github.com/lablup/backend.ai-manager))
  - `agent` (git clone from here)
  - `common` (git clone from [the common repo](https://github.com/lablup/backend.ai-common))

Install `backend.ai-common` as an editable package in the agent (and the manager) virtualenvs
to keep the codebase up-to-date.

```console
$ cd agent
$ pip install -U -e ../common
```

#### Steps

```console
$ mkdir -p "./scratches"
$ cp config/halfstack.toml ./agent.toml
```

Then, run it (for debugging, append a `--debug` flag):

```console
$ python -m ai.backend.agent.server
```

To run the agent-watcher:

```console
$ python -m ai.backend.agent.watcher
```

The watcher shares the same configuration TOML file with the agent.
Note that the watcher is only meaningful if the agent is installed as a systemd service
named `backendai-agent.service`.

To run tests:

```console
$ python -m flake8 src tests
$ python -m pytest -m 'not integration' tests
```


## Deployment

### Configuration

Put a TOML-formatted manager configuration (see the sample in `config/sample.toml`)
in one of the following locations:

 * `agent.toml` (current working directory)
 * `~/.config/backend.ai/agent.toml` (user-config directory)
 * `/etc/backend.ai/agent.toml` (system-config directory)

Only the first found one is used by the daemon.

The agent reads most other configurations from the etcd v3 server where the cluster
administrator or the Backend.AI manager stores all the necessary settings.

The etcd address and namespace must match with the manager to make the agent
paired and activated.
By specifying distinguished namespaces, you may share a single etcd cluster with multiple
separate Backend.AI clusters.

By default the agent uses `/var/cache/scratches` directory for making temporary
home directories used by kernel containers (the `/home/work` volume mounted in
containers).  Note that the directory must exist in prior and the agent-running
user must have ownership of it.  You can change the location by
`scratch-root` option in `agent.toml`.

### Running from a command line

The minimal command to execute:

```sh
python -m ai.backend.agent.server
python -m ai.backend.agent.watcher
```

For more arguments and options, run the command with `--help` option.

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
