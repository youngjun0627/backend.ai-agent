Changes
=======

<!--
    You should *NOT* be adding new change log entries to this file, this
    file is managed by towncrier. You *may* edit previous change logs to
    fix problems like typo corrections or such.

    To add a new change log entry, please refer
    https://pip.pypa.io/en/latest/development/contributing/#news-entries

    We named the news folder "changes".

    WARNING: Don't drop the last line!
-->

.. towncrier release notes start

19.09.22 (2020-04-30)
---------------------

### Fixes
* Check closing of zmq sockets in code runner to avoid writing on invalid/closed sockets and indefinite waiting for the krunner execution results ([#205](https://github.com/lablup/backend.ai-agent/issues/205))
* Introduce "already-terminated" reason for `kernel_terminated` event ([#206](https://github.com/lablup/backend.ai-agent/issues/206))

19.09.21 (2020-04-30)
---------------------

### Fixes
* Ensure unhandled exceptions in aiozmq.rpc handlers to be always msgpack-serializable to keep the agent daemon logs clean ([#199](https://github.com/lablup/backend.ai-agent/issues/199))
* Add the force option when calling Docker's container deletion API to avoid rare "stop before removal" errors even when we try deletion after receiving the termination event ([#200](https://github.com/lablup/backend.ai-agent/issues/200))
* Keep the docker event processor running regardless of unexpected exceptions in the middle ([#202](https://github.com/lablup/backend.ai-agent/issues/202))
* Use destroy() of ZeroMQ context objects instead of term() to stabilize container removals ([#203](https://github.com/lablup/backend.ai-agent/issues/203))
* Delete auto-created/temporary volumes with when deleting containers ([#204](https://github.com/lablup/backend.ai-agent/issues/204))

### Miscellaneous
* Adopt towncrier for changelog management ([#201](https://github.com/lablup/backend.ai-agent/issues/201))

19.09.20 (2020-04-27)
---------------------

* FEATURE: Place bootstrap script upon session creation (#174)
* FIX: Update the restricted/reserved list of service port numbers, allowing privileged TCP ports
  to be used by the kernel image authors. (#195)

19.09.19 (2020-03-24)
---------------------

* HOTFIX: Regression in a majority of kernels due to a missing `self.loop` initialization.

19.09.18 (2020-03-21)
---------------------

* IMPROVE: Support code-server 2.x (the web version of VSCode) as an intrinsic service port.
  (#191, #194)
* BACKPORT/IMPROVE: Keep track of service process lifecycle expplicitly and allow service processes to
  be restarted if they terminate. (#183)
* FIX: Load sitecustomize.py that hooks user inputs only in the batch-mode execution.
  Previously it was loaded for all Python processes and prevented user prompts in pip and other tools.
  (#193)
* BACKPORT/FIX: Allow users to edit/override their dotfiles when spawning new containers. (#192)
* FIX: Image list update regression due to typo in the codes (#188)

19.09.17 (2020-03-08)
---------------------

* IMPROVE/FIX: Rewrite container lifecycle management using queues and persistent states (#187)
* MAINTENANCE: Update backend.ai-common dependency.

19.09.16 (2020-02-27)
---------------------

* MAINTENANCE: Update dependency packages.

19.09.15 (2020-02-21)
---------------------

* HOTFIX: Remove duplicate code block causing agent startup crashes, which is originated from
  a merge error.

19.09.14 (2020-02-17)
---------------------

* FIX: Corruption of resource allocation maps due to abnormal termination of containers and/or
  race conditions of resource cleanup handlers. (#180)
* FIX: .bashrc not loaded by the tmux session which is default-enabled in the ttyd intrinsic app.

19.09.13 (2020-02-10)
---------------------

* NEW: ttyd terminals now use a shared tmux session by default, making the container's shell session
  persistent across browser refreshes and intermittent connection failures, and also allowing
  pair-programming by sharing the ttyd session. (#168, #178)

19.09.12 (2020-02-10)
---------------------

* NEW: Add support for user-specific dotfiles population in session containers (#166)
* MAINTENANCE: Revamp the CI/CD pipelines (#173)

19.09.11 (2020-01-20)
---------------------

* FIX: Package version conflicts due to aiobotocore/botocore and python-dateutil, by removing no longer
  used codes and aiobotocore dependency.

19.09.10 (2020-01-09)
---------------------

* FIX: Support host-to-container PID mapping in older Linux kernels (lower than 4.1) which does not
  provide NSPid field in /proc task status.
* FIX: Invalid ownership of several runtime-generated files in the container working directory such as
  SSH keypair and basic dotfiles, which may prevent containers from working properly.
* MAINTENANCE: Update aiodocker to 0.17

19.09.9 (2019-12-18)
--------------------

* IMPROVE: Skip containers and images with a unsupported (future) kernelspec version.
  (lablup/backend.ai#80)

19.09.8 (2019-12-16)
--------------------

* NEW: Provide some minimal basic dotfiles in kernel containers by default (.bashrc and .vimrc) (#160)
  - Make the "ls" command always colorized using an alias.
* NEW: Add support for keypair-specific SSH private key setup (#158)

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

* FIX: SFTP/SCP should work consistently in all images, even without `/usr/bin/scp` and `libcrypto`.
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
  It's fixed by adding `-ldl` option to the linker flag of libbaihook.
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
  - Rewrite kernel's `list_files` RPC call to work safer and faster (#124).

19.09.0b7 (2019-08-27)
----------------------

* FIX: TensorBoard startup error due to favoring IPv6 address
* CHANGE: Internally restructured the codebase so that we can add different agent implementations
  easily in the future.  Kubernetes support is coming soon! (#125)
* Accept a wider range of `ai.backend.base-distro` image label values which do not
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
  - Compute plugins now should implement `get_attched_devices()` method.
* Improved support for separation of agent host and kernel (container) hosts
  (lablup/backend.ai#37)
* Add support for scaling-groups as configured by including them in heartbeats
  (backend.ai-manager#167)
* Implement reserved resource slots for CPU and memory (#110, #112)
