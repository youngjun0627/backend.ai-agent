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

20.03.7 (2020-11-23)
--------------------

### Fixes
* Fix a regression to spawning GPU sessions, by passing the original container distribution value (taken from image labels, e.g., `ubuntu16.04`) to compute plugins' `get_hooks()` method instead of the matched result (e.g., `static-gnu`) since the responsibility of choosing the optimal hook binary is on the plugin itself. ([#243](https://github.com/lablup/backend.ai-agent/issues/243))


20.03.6 (2020-11-23)
--------------------

### Features
* Improve compatibility with arbitrary Linux containers by using a statically built Python for kernel-runners compatible with manylinux2010, which eliminates, for instance, OpenSSL dependency in the containers ([#242](https://github.com/lablup/backend.ai-agent/issues/242))

### Misc
* Backport aiohttp, aiotools and other dependency version updates


20.03.5 (2020-10-24)
--------------------

### Fixes
* Update the krunner packages to run kernel runners (in-container daemons) on more recent Python version (3.8.6) and custom build of ttyd to fix periodic disconnection of terminals ([#237](https://github.com/lablup/backend.ai-agent/issues/237))


20.03.4 (2020-10-06)
--------------------

### Features
* Add a configuration option to allow skipping running manager detection upon agent startup ([#234](https://github.com/lablup/backend.ai-agent/issues/234))


20.03.3 (2020-09-09)
--------------------

### Fixes
* Add context parameter for init methods of intrinsic accelerators. ([#229](https://github.com/lablup/backend.ai-agent/issues/229))


20.03.2 (2020-08-10)
--------------------

### Fixes
* Fix a regression of container statistics collection with recent aiodocker versions (0.16 or later) ([#226](https://github.com/lablup/backend.ai-agent/issues/226))
* Fix missing `allow_extra` options in config input validators, which is a newly exposed regression after ff019ac9 ([#227](https://github.com/lablup/backend.ai-agent/issues/227))


20.03.1 (2020-07-28)
--------------------

* Maintenance release to fix the version reference format for `backend.ai-common` (from "~=20.03.0" to "~=20.3.0")


20.03.0 (2020-07-28)
--------------------

* No changes since RC1


20.03.0rc1 (2020-07-23)
-----------------------

### Features
* Add support for Ubuntu 20.04 as base-distro for session images
* Allow overriding of intrinsic compute devices (`cpu` and `mem`) using compute device plugins ([#224](https://github.com/lablup/backend.ai-agent/issues/224))

### Fixes
* Move invocation of user-defined bootstrap script from the container entrypoint to the krunner's main loop for better log visibility and ability to interrupt ([#225](https://github.com/lablup/backend.ai-agent/issues/225))


20.03.0b2 (2020-07-02)
----------------------

### Breaking Changes
* Apply the plugin API v2 -- all stat/error/accelerator plugins must be updated along with the agent ([#222](https://github.com/lablup/backend.ai-agent/issues/222))

### Features
* Allow kernel containers to know their identities via `BACKENDAI_KERNEL_ID` environment variable ([#218](https://github.com/lablup/backend.ai-agent/issues/218))
* Global configuration for agent/container
  - Global configuration on etcd overrides existsing local configuration for agent/container. ([#219](https://github.com/lablup/backend.ai-agent/issues/219))

### Fixes
* Fix instability caused by stat-synchronizer processes under heavy loads by collecting statistics periodically only ([#212](https://github.com/lablup/backend.ai-agent/issues/212))
* Apply batching when producing "kernel_stat_sync" events to reduce manager loads and increase timeout for caching stats in Redis from 30 seconds to 2 minutes ([#213](https://github.com/lablup/backend.ai-agent/issues/213))
* Improve stability under heavily loaded scenarios ([#214](https://github.com/lablup/backend.ai-agent/issues/214))
  - Skip lifecycle sync for already terminating kernels to reduce excessive Docker Engine overheads with a many number of being-terminated kernels
  - Increase timeout for container termination to 60 seconds during restarting kernels, by observing deletion latencies under heavy load tests
* Prevent executing startup command multiple times for batch session. ([#217](https://github.com/lablup/backend.ai-agent/issues/217))
* Stabilize container lifecycle management and RPC exception handling with updated Callosum ([#218](https://github.com/lablup/backend.ai-agent/issues/218))
* Make it possible to add more backend implementations by generalizing importing and initialization of backend modules ([#222](https://github.com/lablup/backend.ai-agent/issues/222))
* Fix hang-up of service-port functionality of a session when one of its service starts but fails to initialize ([#223](https://github.com/lablup/backend.ai-agent/issues/223))


20.03.0b1 (2020-05-12)
----------------------

### Breaking Changes
* - Now it runs on Python 3.8 or higher only.

### Features
* Now we support ROCM (Radeon Open Compute) accelerators via `backend.ai-accelerator-rocm` plugin.
* Now our manager-to-agent RPC uses [Callosum](https://github.com/lablup/callosum) instead of aiozmq, supporting Python 3.8 natively. ([#157](https://github.com/lablup/backend.ai-agent/issues/157))
* Support user-defined bootstrap script (e.g., this can be used to clone a git repo) ([#161](https://github.com/lablup/backend.ai-agent/issues/161))
* ResourceSlots are now more permissive. Agent still checks the validity of known slots but also allows zero-valued unknown slots as well. ([#162](https://github.com/lablup/backend.ai-agent/issues/162))
* All CLI commands are now accessible via `backend.ai ag` ([#165](https://github.com/lablup/backend.ai-agent/issues/165))
* Add support for pre-open service ports for user-written apps ([#167](https://github.com/lablup/backend.ai-agent/issues/167))
* Add a new "app" kernel-runner runtime type for GUI and application-only kernels ([#189](https://github.com/lablup/backend.ai-agent/issues/189))
* Mark kernel started after bootstrap script is executed ([#190](https://github.com/lablup/backend.ai-agent/issues/190))
* Generalize kernel-runner volume lists using plugin-like krunner package auto-detection ([#198](https://github.com/lablup/backend.ai-agent/issues/198))

### Deprecations
* Using CentOS 6.10 as the base distribution for importing images is deprecated. ([#189](https://github.com/lablup/backend.ai-agent/issues/189))

### Fixes
* Detection for manager now works for HA setup seamlessly. (It now determines if at least one manager is running.) [lablup/backend.ai#125](https://github.com/lablup/backend.ai/issues/125)
* Fix wrong ownership of .ssh and keypair files when the SSH keypair is set via the `internal_data` field of the kernel creation config.
* Make scratch directory accesses for setup/tear-down fully asynchronous ([#186](https://github.com/lablup/backend.ai-agent/issues/186))
* Fix service-port parsing routines to recognize "vnc-web" service ports for GUI containers ([#189](https://github.com/lablup/backend.ai-agent/issues/189))
* Make the kernel runner's service parser consistent with documentation ([#197](https://github.com/lablup/backend.ai-agent/issues/197))
* Update test fixtures to work with pytest-asyncio 0.11 and use a separate registry state data file for each test sessions ([#208](https://github.com/lablup/backend.ai-agent/issues/208))
* Update test fixtures to work with pytest-asyncio 0.12 ([#209](https://github.com/lablup/backend.ai-agent/issues/209))
* Fix lifecycle-related code errors when handling results of batch-mode tasks ([#210](https://github.com/lablup/backend.ai-agent/issues/210))

### Miscellaneous
* Now the kernel-runner runs on the prebuilt Python 3.8 mounted inside containers. ([#189](https://github.com/lablup/backend.ai-agent/issues/189))
* Adopt [towncrier](https://github.com/twisted/towncrier) to manage changelogs. ([#196](https://github.com/lablup/backend.ai-agent/issues/196))
* Update flake8 to a prerelease version supporting Python 3.8 syntaxes ([#211](https://github.com/lablup/backend.ai-agent/issues/211))
