Changes
=======

19.09.7 (2019-11-11)
--------------------

* ROLLBACK: SFTP throughput optimization. It has caused PyCharm's helper upload failures for its
  remote interpreter and debugging support, while all other tested SFTP clients (Cyberduck, FileZilla)
  have worked flawlessly.

  - While we are investigating both the SSHJ library and dropbear part to find the root cause,
    the optimization is hold back since working is better than fast.

19.09.6 (2019-11-04)
--------------------

* FIX/IMPROVE: entrypoint.sh for kernel containers startup

  - Handle UID overlap (not only GID) correctly by renaming the image's existing account

  - Allow execution as root if the agent is configured to do so.

  - FIX: Ensure library preloads not modifiable by the user accounts in kernels even when they unset
    "LD_PRELOAD" environment variable, by writing "/etc/ld.so.preload" file as root.

    NOTE: Alpine-based images does not support this because musl-libc do not use /etc/ld* configurations
    but only depend on environment variables with a few hard-coded defaults.

* FIX: Ensure dropbear (our intrinsic SSH daemon) to keep environment variables when users either open a
  new SSH session or execute a remote command.

* FIX: Regression of the batch-mode execution API.

* MAINTENANCE: Update dependencies and pin Trafaret to v1.x because Trafraet v2.0 release breaks the
  backward compatibility.

19.09.5 (2019-10-16)
--------------------

* FIX: SFTP/SCP should work consistently in all images, even without ``/usr/bin/scp`` and ``libcrypto``.
  Applied static builds of OpenSSH utilities with OpenSSL and zlib included.

19.09.4 (2019-10-15)
--------------------

* OPTIMIZE: SFTP file transfers are now 3x faster by increasing the network buffer sizes used by
  dropbear.

* FIX: Regression of entrypoint.sh that caused failure of user/group creation, which resulted in
  inability to use the SSH service port due to missing username.

19.09.3 (2019-10-14)
--------------------

* FIX: entrypoint.sh for kernel containers did not work properly when the container image has an user ID
  or group ID that overlaps with the given values or when the agent is configured to use root for
  containers.  This fixes kernel launches in macOS where the default user's group "staff" has the group
  ID 20 which overlaps with the group "dialout" in Ubuntu or "games" in CentOS.

19.09.2 (2019-10-11)
--------------------

* FIX: SSH and SFTP support now works as expected in all types of kernels, including Alpine-based ones.
  The auto-generated keypair name is changed to "id_container" and now it uses RSA instead of ECDSA for
  better compatibility.

* FIX: Handle rarely happened ProcessLookupError when cleaning up kernels and stat synchronizers
  which has caused infinitely repeated warning "cannot read stats: sysfs unreadable for container xxxx".

* FIX: Use the canonical, normalized version number for the backend.ai-common setup dependency to silence
  pip warnings during installation.

19.09.1 (2019-10-10)
--------------------

* FIX: Regression of code execution due to wrong-ordered arguments of code execution RPC call.

* FIX: Potential memory leak and PID exhaustion due to improper termination of stat synchronizer
  and its logger processes.

19.09.0 (2019-10-07)
--------------------

* FIX: In some kernels, git command has failed due to "undefined symbol: dlsym" error.
  It's fixed by adding ``-ldl`` option to the linker flag of libbaihook.

* FIX: Reconnection and cancellation of etcd watchers used for manager launch detection

19.09.0rc3 (2019-10-04)
-----------------------

This is the last preview, feature-freeze release for v19.09 series.
Stability updates will follow in the v19.09.0 and possibly a few more v19.09.x releases.

* NEW: Support batch tasks (#148, lablup/backend.ai#199)

* NEW: Support image import tasks, with internal-purpose security flag implementations (#149,
  lablup/backend.ai#171)

* NEW: Intrinsic SSH support to any session, as "sshd" service port.
  The host key and user keypair is randomly generated.  To pin your own SSH keypair, create a
  ".ssh" user vfolder which will be automatically mounted to all your compute sessions.

* NEW: Add support for a new service port: "sftp" for large-file transfers with vfolders using
  a special dedicated kernel.

* NEW: Add support for a new service port: "vscode" to access Visual Studio Code running as an
  web application in the interactive sessions.  Note that the sessions running VSCode are recommended to
  have more than 2 GiB of free main memory. (#147)

* IMPROVE: Enable the debugger port in TensorBoard.  Note that this port is for private-use only
  so that a TensorFlow process can send debug-logging data to it in the same container.

* IMPROVE: Add support for multiple TCP ports to be mapped for a single service.

19.09.0rc2 (2019-09-24)
-----------------------

* Minor bug fixes

* CHANGE: The default of "debug.coredump" config becomes false in the halfstack configuration.

19.09.0rc1 (2019-09-23)
-----------------------

* NEW: Add a new intrinsic service port "ttyd" for all kernels, which provides a clean and slick
  web-based shell access.

* NEW: Add support for sftp service if the kernel supports it (#146).

* FIX: Now "kernel_terminated" events carry the correct "reason" field, which is stored in the
  "status_info" in the manager's kernels table.

* FIX: Avoid binary-level conflicts of Python library (libpythonmX.Y.so) in containers due to
  "/opt/backend.ai/lib" mounts.  This had crashed some vendor-specific images which relies on
  Python 3.6.4 while our krunner daemon uses Python 3.6.8.

* CHANGE: The agent-to-manager notifications use Redis instead of ZeroMQ (#144,
  lablup/backend.ai-manager#192, lablup/backend.ai-manager#125), and make the agent to survive
  intermittent Redis connection disruptions.

19.09.0b12 (2019-09-09)
-----------------------

* NEW: Add support for specifying shared memory for containers (lablup/backend.ai#52, #140)

* Internally applied static type checks to avoid potential bugs due to human mistakes. (#138)
  Also refactored the codebase to split the manager-agent communication part and the kernel interaction
  part (which is now replacible!) for extensible development.

* Update dependencies including aiohttp 3.6, twine, setuptools, etc.

19.09.0b11 (2019-09-03)
-----------------------

* NEW: Add shared-memory stats

* CHANGE: watcher commands are now executed with "sudo".

19.09.0b10 (2019-08-31)
-----------------------

* FIX: regression of batch-mode execution (file uploads to kernels) due to refactoring

19.09.0b9 (2019-08-31)
----------------------

* FIX: Apply a keepalive messaging at the 10-sec interval for agent-container RPC connection to avoid
  kernel-enforced NAT connection tracker timeout (#126, lablup/backend.ai#46)

  This allow execution of very long computation (more than 5 days) without interruption as long as
  the idle timeout configuration allows.

* FIX: When reading plugin configurations, merge scaling-group and global configurations correctly.

* FIX: No longer change the fstab if mount operations fail. Also delete the unmounted folder
  if it is empty after unmount was successful.

19.09.0b8 (2019-08-30)
----------------------

* NEW: Add support for running CentOS-based kernel images by adding CentOS 7.6-based builds for
  libbaihook and su-exec binaries.

* NEW: watcher: Add support for fstab/mount/unmount management APIs for superadmins (#134)

* Improve stability of cancellation during shutdown via refactoring and let uvloop work more consistently
  with vanilla asyncio.  (#133)

  - Now the agent daemon handles SIGINT and SIGTERM much more gracefully.

  - Upgrade aiotools to v0.8.2+

  - Rewrite kernel's ``list_files`` RPC call to work safer and faster (#124).

19.09.0b7 (2019-08-27)
----------------------

* FIX: TensorBoard startup error due to favoring IPv6 address

* CHANGE: Internally restructured the codebase so that we can add different agent implementations
  easily in the future.  Kubernetes support is coming soon! (#125)

* Accept a wider range of ``ai.backend.base-distro`` image label values which do not
  include explicit version numbers.

19.09.0b6 (2019-08-21)
----------------------

* CHANGE: Reduce the default websocket ping interval of Jupyter notebooks to 10 seconds
  to prevent intermittent connection losts in specific browser environments. (#131)

19.09.0b5 (2019-08-19)
----------------------

* NEW: Add support for watcher information reports (#107)

* Improve versioning of krunner volumes not to interfere with running containers
  when upgraded (#120)

* Add support for getting core dumps inside container as configuration options (#114)

* Fix missing instance ID for configuration scope maps (#127)

* Pin the pyzmq version to 18.1.0 (lablup/backend.ai#47)

19.09.0b4 (2019-08-14)
----------------------

* FIX: Disable trash bins in the Jupyter browsers (lablup/backend.ai#45)

* FIX: Revert "net.netfilter.nf_conntrack_tcp_timeout_established" in the recommended kernel parameters
  to the Linux kernel's default (5 days = 432000 seconds). (lablup/backend.ai#46)

* CHANGE: The CPU overcommit factor (previously fixed to 2) is now adjustable by the environment variable
  "BACKEND_CPU_OVERCOMMIT_FACTOR" and the dfault is now 1.

* NEW: Add an option to change the underlying event loop implementation.

19.09.0b3 (2019-08-05)
----------------------

* Include attached_devices in the kernel creation response (lablup/backend.ai-manager#154)

  - Compute plugins now should implement ``get_attched_devices()`` method.

* Improved support for separation of agent host and kernel (container) hosts
  (lablup/backend.ai#37)

* Add support for scaling-groups as configured by including them in heartbeats
  (backend.ai-manager#167)

* Implement reserved resource slots for CPU and memory (#110, #112)

19.06.0b2 (2019-07-25)
----------------------

* CHANGE: Now krunner-env is served as local Docker volumes instead of dummy contaienrs (#117, #118)

  - This fixes infinite bloating of anonymous Docker volumes implicitly created from dummy containers
    which consumed the disk space indefinitely.

  - The agent auto-creates and auto-udpates the krunner-env volumes. Separate Docker image deployment
    and manual image tagging are no longer required!

  - The krunner-env image archives are distributed as separate "backend.ai-krunner-{distro}" wheel
    packages.

* IMPROVED: Now the agent can be run *without* root, given that:

  - The docker socket is accessible by the agent's user permission.
    (usually you have to add the user to the "docker" system group)

  - container.stats-type is set to "docker".

  - The permission/ownership of /tmp/backend.ai/ipc and agent/event sockets inside it is writable by the
    user/group of the agent.

  - container.kernel-uid, container.kernel-gid is set to -1 or the same values that
    ai/backend/agent/server.py file stored in the disk has (e.g., inside virtualenv's site-packages
    directory).

* Also improved the clean up of scratch directories due to permission issues caused by bind-mounting
  files inside bind-mounted directories.

19.06.0b1 (2019-07-13)
----------------------

- BREAKING CHANGE: The daemon configurations are read from TOML files and
  shared configurations are from the etcd. (#112)

- NEW: The agent now automatically determines the local agent IP address when:

  - etcd's "config/network/subnet/agent" is set to a non-zero network prefix

  - rpc-listen-addr is an empty string

- Update Jupyter custom styles and resources

- Update dependencies including uvloop

- Add explicit timeout for service-port startup

19.06.0a1 (2019-06-03)
----------------------

- Add support for live collection of for node-level, per-device, and per-kernel resource metrics.
  (#109)

- Include version and compute plugin information in heartbeats.

- Make it possible to use specific IP address ranges for public ports of kernel containers.
  (lablup/backend.ai#37)

19.03.4 (2019-08-14)
--------------------

- Fix inability to delete files in the Jupyter file browser running in containers.

19.03.3 (2019-07-12)
--------------------

- Add missing updates for Jupyter style resources to disable Jupyter cluster
  extension which is not compatible with us and to remove unused headers in the
  terminal window.

19.03.2 (2019-07-12)
--------------------

- Fix permission handling for container-agent intercommunication socket which
  has prevented unexpected crashes of containers in certain conditions.

- Mount hand-made tmp dirs only when custom tmpfs is enabled.

- Update Jupyter style resources.

19.03.1 (2019-04-21)
--------------------

- Fix handling of empty resource allocation when rescanning running containers.
  (The bug may happen when the CUDA plugin is installed in the nodes that do not have
  CUDA-capable GPUs.)

19.03.0 (2019-04-10)
--------------------

- Minor updates to match with the manager changes.

- Update dependency: aioredis

19.03.0rc2 (2019-03-26)
-----------------------

- NEW: Add (official) support for TensorBoard with the default logdir:
  /home/work/logs

- CHANGE: Use the same "dev" krunner-env image tags for all pre-release and
  development versions to prevent hassles of tag renaming during development.

- CHANGE: Now the idle timeout is applied per kernel to support
  lablup/backend.ai-manager#92 implementation.

- CHANGE: Rename "--redis-auth" option to "--redis-password" and its
  environment variable equivalent as well.

- Fix and update accelerator plugin support by adding an in-container socket
  which provides host-only-available information to in-container programs.

- Apply a customized look-and-feel to Jupyter notebooks in Python-based containers.

19.03.0rc1 (2019-02-25)
-----------------------

- NEW: A side-by-side watcher daemon (#107)

  - It provides a separate channel for watching and controlling the agent
    even when the agent become unavailable (e.g., deadlock or internal crash).

  - It works best with a SystemD integration.

  - WARNING: Currently "reload" (agent restart without terminating running
    containers) has problems with PID tracking.  Finding solutions for this...

- NEW: Support Redis/etcd authentication (lablup/backend.ai-manager#138)

  - NOTE: Currently etcd authentication is *not* usable in productions due to
    a missing implementation of automatic refreshing auth tokens in the upstream
    etcd3 library.

- NEW: Agent-level (system-wide) live statistics (#101)

- Fix detection of up-to-date local Docker image (#105)

- Fix ordering of prompt outputs and user input events in the query mode (#106)

19.03.0b7 (2019-02-15)
----------------------

- Make logs and error messages to have more details.

- Implement RW/RO permissions when mounting vfolders (lablup/backend.ai-manager#82)

- Change statistics collector to use UNIX domain socketes, for specific environments
  where locally bound sockets are not accessible via network-local IP addresses.

- Update Alpine-based kernel runners with a fix for uid-match functionality for them.

- Fix some bugs related to allocation maps and ImageRef class.

19.03.0b6 (2019-02-08)
----------------------

- NEW: Jupyter notebooks now have our Backend.AI logo and a slightly customized look.

- Fix the jupyter notebook service-port to work with conda-based images,
  where "python -m jupyter notebook" does not work but "python -m notebook"
  works.

- Let agent fail early and cleanly if there is an initialization error,
  for ease of debugging with supervisord.

- Fix restoration of resource allocation maps upon agent restarts.

19.03.0b5 (2019-02-01)
----------------------

- Handle failures of accelerator plugin initialization more gracefully.

19.03.0b4 (2019-01-31)
----------------------

- Fix duplicate resource allocation when a computedevice plugin defines
  multiple resource slots.

- Fix handling multiple sets of docker container configuration arguments
  generated by different compute device plugins.

19.03.0b3 (2019-01-30)
----------------------

- Restore support for fractionally scaled accelerators and a reusable
  FractionAllocMap class for them.

- Fix a bug after automatically pull-updating kernel images from registries.

- Fix heartbeat serialization error.

19.03.0b2 (2019-01-30)
----------------------

- Add missing implementation for authenticated image pulls from private docker
  registries.

19.03.0b1 (2019-01-30)
----------------------

- BIG: Support dynamic resource slots and full private Docker registries. (#98)

- Expand support for various kernel environments: Python 2, R, Julia, JupyterHub

19.03.0a3 (2019-01-21)
----------------------

- Replace "--skip-jail" option with "--sandbox-type", which now defaults to use
  Docker-provided sandboxing until we get our jail stabilized.

19.03.0a2 (2019-01-21)
----------------------

- Fix missing stderr outputs in the query mode.  Now standard Python exception logs
  may contain ANSI color codes as ``jupyter_client`` automatically highlights them.
  (#93)

19.03.0a1 (2019-01-18)
----------------------

- NEW: Rewrite the kernel image specification.  Now it is much easier to build
  your own kernel image by adding just a few more labels in Dockerfiles.
  (ref: https://github.com/lablup/backend.ai-kernels/#howto-adding-a-new-image)

  - We now support official NVIDIA GPU Cloud images in this way.

  - We are now able to support Python 2.x kernels again!

  - Now agent/kernel-runner/jail/hook are all managed together and the kernel
    images are completely separated from their changes.

- NEW: New command-line options

  - ``--skip-jail``: disables our jail and falls back to the Docker's default seccomp
    filter.  Useful for troubleshotting with our jail.

  - ``--jail-arg``: when using our jail, add extra command-line arguments to the jail
    by specifying this option multiple times.
    Note that options starting with dash must be prepended with an extra space to
    avoid parsing issues imposed by the Python's standard argparse module.

  - ``--kernel-uid``: when the agent is executed as root, use this to make the kernel
    containers to run as specific user/UID.

  - ``--scratch-in-memory``: moves the scratch and /tmp directories into a separate
    in-memory filesystem (tmpfs) to avoid inode/quota exahustion issues in
    multi-tenant setups.

    This option is only available at Linux and the agent must be run as root. When
    used, the size of each directory is limited to 64 MiB. (In the future this will
    become configurable.)

- CHANGE: The kernel runner now preserves container-defined environment variables.

18.12.1 (2019-01-06)
--------------------

- Technical release to fix a packaging mistake in 18.12.0.

18.12.0 (2019-01-06)
--------------------

- Version numbers now follow year.month releases like Docker.
  We plan to release stable versions on every 3 months (e.g., 18.12, 19.03, ...).

- NEW: Support TPU (Tensor Processing Units) on Google Clouds.

- Clean up log messages for on-premise devops & IT admins.

18.12.0a4 (2018-12-26)
----------------------

- NEW: Support specifying credentials for private Docker registries.

- CHANGE: Now it prefers etcd-based docker registry configs over CLI arguments.

18.12.0a3 (2018-12-21)
----------------------

- Technical release to fix the backend.ai-common dependency version.

18.12.0a2 (2018-12-21)
----------------------

- NEW: Support user-specified ranges for the service ports published by containers
  via the ``--container-port-range`` CLI argument for firewall-sensitive setups.
  (The default range is 30000-31000) (#90)

- CHANGE: The agent now automatically pulls the image if not available in the host.

- CHANGE: The process monitoring tools will now show prettified process names for
  Backend.AI's daemon processes which exhibit the role and key configurations (e.g.,
  namespace) at a glance.

- Improve support for using custom/private Docker registries.

18.12.0a1 (2018-12-14)
----------------------

- NEW: App service ports!  You can start a compute session and directly connect to a
  service running inside it, such as Jupyter Notebook! (#89)

- Internal refactoring to clean up and fix bugs related to image name references.

- Fix bugs in statistics collection.

- Monitoring tools are separated as plugins.

1.4.0 (2018-09-30)
------------------

- Generalizes accelerator supports

  - Accelerators such as CUDA GPUs can be installed as a separate plugin (#66)

  - Adds support for nvidia-docker v2 (#64)

  - Adds support for allocation of multiple accelerators for one kernel container as
    well as partial shares of each accelerator (#66)

- Revamp the agent restart and kernel initialization processes (#35, #73)

- The view of the agent can be limited to specific CPU cores and GPUs
  using extra CLI arguments: ``--limit-cpus``, ``--limit-gpus`` for
  debugging and performance benchmarks. (#65)

1.3.7 (2018-04-05)
------------------

- Hotfix for handling of dotted image names when they are terminated.

1.3.6 (2018-04-05)
------------------

- Hotfix for handling subdirectories in batch-mode file uploads.

1.3.5 (2018-03-20)
------------------

- Fix vfolder mounts to use the configuration specified in the etcd.
  (No more fixed to "/mnt"!)

1.3.4 (2018-03-19)
------------------

- Fix occasional KeyError when destroying kernels. (#56)

- Deploy a debug log for occasional FileNotFoundError when uploading files
  in the batch mode. (#57)

1.3.3 (2018-03-15)
------------------

- Fix wrong kernel_host sent back to the manager when not overridden.

1.3.2 (2018-03-15)
------------------

- Technical release to fix backend.ai-common depedency version.

1.3.1 (2018-03-14)
------------------

- Technical release to update CI configuration.

1.3.0 (2018-03-08)
------------------

- Fix repeating docker event polling even when there is connection/client-side
  aiohttp errors.

- Upgrade aiohttp to v3.0 release.

- Improve dockerization. (#55)

- Improve inner beauty.

1.2.0 (2018-01-30)
------------------

**NOTICE**

- From this release, the manager and agent versions will go together, which indicates
  the compatibility of them, even when either one has relatively little improvements.

**CHANGES**

- Include the exit code of the last executed in-kernel process when returning
  ``build-finished`` or ``finished`` results in the batch mode.

- Improve logging to support rotating file-based logs.

- Upgrade aiotools to v0.5.2 release.

- Remove the image name prefix when reporting available images. (#51)

- Improve debug-kernel mode to mount host-side kernel runner source into the kernel
  containers so that they use the latest, editable source clone of the kernel runner.

1.1.0 (2018-01-06)
------------------

- Automatically assign the run ID if set None when starting a run.

- Pass environment variables in the start-config to the kernels via
  ``/home/work/.config/environ.txt`` file mounted inside kernels.

- Include the list of kernel images available to the agent when sending
  heartbeats. (#51)

- Remove simplejson from dependencies in favor of the standard library.
  The stdlib has been updated to support all required features and use
  an internal C-based module for performance.

1.0.6 (2017-11-29)
------------------

- Update aioredis to v1.0.0 release.

- Remove "mode" argument from completion RPC calls.

- Fix a bug when terminating overlapped execute streams, which has caused
  indefinite hangs in the client side due to missing "finished" notification.

1.0.5 (2017-11-17)
------------------

- Implement virtual folder mounting (assuming /mnt is already configured)

1.0.4 (2017-11-14)
------------------

- Fix synchronization issues when restarting kernels

- Improve "debug-kernel" mode to use the given kernel name

1.0.3 (2017-11-11)
------------------

- Fix a bug in duplicate-check of our Docker event stream monitoring coroutine

1.0.2 (2017-11-10)
------------------

- Fix automatic mounting of deeplearning-samples Docker volume for ML kernels

- Stabilize statistics collection

- Fix typos

1.0.1 (2017-11-08)
------------------

- Prevent duplicate Docker event generation

- Various bug fixes and improvements (#44, #45, #46, #47)

1.0.0 (2017-10-17)
------------------

- This release is replaced with v1.0.1 due to many bugs.

**CHANGES**

- Rename the package to "Backend.AI" and the import path to ``ai.backend.agent``

- Rewrite interaction with the manager

- Read configuration from etcd shared with the manager

- Add FIFO-style scheduling of overlapped execution requests

- Implement I/O and network statistic collection using sysfs

0.9.14 (2017-08-29)
-------------------

**FIX**

- Fix and improve version reference mechanisms.

- Fix missing import error vanished during hostfix cherrypick

0.9.12 (2017-08-29)
-------------------

**IMPROVEMENTS**

- It now applies the same UID to the spawned containers if they have the "uid-match"
  feature label flag. (backported from develop)

0.9.11 (2017-07-19)
-------------------

**FIX**

- Add missing "sorna-common" dependency and update other requirements.

0.9.10 (2017-07-18)
-------------------

**FIX**

- Fix the wrong version range of an optional depedency package "datadog"

0.9.9 (2017-07-18)
------------------

**IMPROVEMENTS**

- Improve packaging so that setup.py has the source list of dependencies
  whereas requirements.txt has additional/local versions from exotic
  sources.

- Support exception/event logging with Sentry and runtime statistics with Datadog.

0.9.8 (2017-06-30)
------------------

**FIX**

- Fix interactive user inputs in the batch-mode execution.

0.9.7 (2017-06-29)
------------------

**NEW**

- Add support for the batch-mode API with compiled languages such as
  C/C++/Java/Rust.

- Add support for the file upload API for use with the batch-mode API.
  (up to 20 files per request and 1 MiB per each file)

**CHANGES**

- Only files stored in "/home/work.output" directories of kernel containers
  are auto-uploaded to S3 as downloadable files, as now we rely on our
  dedicated multi-media output interfaces to show plots and other graphics.
  Previously, all non-hidden files in "/home/work" were uploaded.

0.9.6 (2017-04-12)
------------------

- Fix a regression in console output streaming.

0.9.5 (2017-04-07)
------------------

- Add PyTorch support.

- Upgrade aiohttp to v2 and relevant dependencies as well.

0.9.4 (2017-03-19)
------------------

- Update missing long_description.

0.9.3 (2017-03-19)
------------------

- Improve packaging: auto-converted README.md as long description and unified
  requirements.txt and setup.py dependencies.

0.9.2 (2017-03-14)
------------------

- Fix sorna-common requirement version.

0.9.1 (2017-03-14)
------------------

**CHANGES**

- Separate console output formats for API v1 and v2.

- Deprecate unused matching option for execution API.

- Remove control messages in API responses.

0.9.0 (2017-02-27)
------------------

**NEW**

- PUSH/PULL-based kernel interaction protocol to support streaming outputs.
  This enables interactive input functions and streaming outputs for long-running codes,
  and also makes kernel execution more resilient to network failures.
  (ZeroMQ's REQ/REP sockets break the system if any messages get dropped)

0.8.2 (2017-01-16)
------------------

**FIXES**

- Fix a typo that generates errors during GPU kernel initialization.

- Fix regression of '--agent-ip-override' cli option.

0.8.1 (2017-01-10)
------------------

- Minor internal polishing release.

0.8.0 (2017-01-10)
------------------

**CHANGES**

- Bump version to 0.8 to match with sorna-manager and sorna-client.

**FIXES**

- Fix events lost by HTTP connection timeouts when using ``docker.events.run()`` from
  aiodocker.  (It is due to default 5-minute timeout set by aiohttp)

- Correct task cancellation

0.7.5 (2016-12-01)
------------------

**CHANGES**

- Add new aliases for "git" kernel: "git-shell" and "shell"

0.7.4 (2016-12-01)
------------------

**CHANGES**

- Now it uses `aiodocker`_ instead of `docker-py`_ to
  prevent timeouts with many concurrent requests.

  NOTE: You need to run ``pip install -r requirements.txt`` to install the
        non-pip (GitHub) version of aiodocker correctly, before running
        ``pip install sorna-agent``.

**FIXES**

- Fix corner-case exceptions in statistics/heartbeats.

.. _aiodocker: https://github.com/achimnol/aiodocker

.. _dockerpy: https://github.com/docker/docker-py

0.7.3 (2016-11-30)
------------------

**CHANGES**

- Increase docker API timeouts.

**FIXES**

- Fix heartbeats stop working after kernel/agent timeouts.

- Fix exception logging in the main server loop.

0.7.2 (2016-11-28)
------------------

**FIXES**

- Hotfix for missing dependency: coloredlogs

0.7.1 (2016-11-27)
------------------

**NEW**

- ``--agent-ip-override`` CLI option to override the IP address of agent
  reported to the manager.

0.7.0 (2016-11-25)
------------------

**NEW**

- Add support for kernel restarts.
  Restarting preserves kernel metadata and its ID, but removes and recreates
  the working volume and the container itself.

- Add ``--debug`` option to the CLI command.

0.6.0 (2016-11-14)
------------------

**NEW**

- Add support for GPU-enabled kernels (using `nvidia-docker plugin`_).
  The kernel images must be built upon nvidia-docker's base Ubuntu images and
  have the label "io.sorna.nvidia.enabled" set ``yes``.

**CHANGES**

- Change the agent to add "lablup/" prefix when creating containers from
  kernel image names, to ease setup and running using the public docker
  repository.  (e.g., "lablup/kernel-python3" instead of "kernel-python3")

- Change the prefix of kernel image labels from "com.lablup.sorna." to
  "io.sorna." for simplicity.

- Increase the default idle timeout to 30 minutes for offline tutorial/workshops.

- Limit the CPU cores available in kernel containers.
  It uses an optional "io.sorna.maxcores" label (default is 1 when not
  specified) to determine the requested number of CPU cores in kernels, with a
  hard limit of 4.

  NOTE: You will still see the full count of CPU cores of the underlying
  system when running ``os.cpu_count()``, ``multiprocessing.cpu_count()`` or
  ``os.sysconf("SC_NPROCESSORS_ONLN")`` because the limit is enforced by the CPU
  affinity mask.  To get the correct result, try
  ``len(os.sched_getaffinity(os.getpid()))``.

.. _nvidia-docker plugin: https://github.com/NVIDIA/nvidia-docker


0.5.0 (2016-11-01)
------------------

**NEW**

- First public release.


<!-- vim: set et: -->
