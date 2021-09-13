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


## 21.09.0a1 (2021-08-25)

### Features
* Add support for `BACKEND_MEM_OVERCOMMIT_FACTOR` environment variable to allow overriding the capacity of main memory for the scheduler, while reporting statistics as-is. ([#280](https://github.com/lablup/backend.ai-agent/issues/280))
* Place a mardkwon file inside a container under /home/work/ which warns data saving on homedir. ([#281](https://github.com/lablup/backend.ai-agent/issues/281))
* An improved, colorized shell prompt with cluster hostname and session name. ([#283](https://github.com/lablup/backend.ai-agent/issues/283))
* Add support for lxcfs to provide a consistent view of `/proc/cpuinfo`, `/proc/meminfo`, `/proc/stat`, etc. inside containers ([#286](https://github.com/lablup/backend.ai-agent/issues/286))
* Implement an evenly-distributed allocation algorithm for `DiscretePropertyAllocMap`, which allows use of CPU overcommits by spreading out oversubscribed core assignments ([#288](https://github.com/lablup/backend.ai-agent/issues/288))
* Now individual container's hostname is set to the corresponding hostname in cluster sessions such as `main1`, `sub1`, etc. ([#291](https://github.com/lablup/backend.ai-agent/issues/291))
* Add aiomonitor module for manager ([#295](https://github.com/lablup/backend.ai-agent/issues/295))
* Add `~/.local/bin` to the default PATH. ([#296](https://github.com/lablup/backend.ai-agent/issues/296))
* Add arm64 (aarch64) support allowing users to run ARM-based container images natively ([#298](https://github.com/lablup/backend.ai-agent/issues/298))

### Fixes
* Stabilize container lifecycle management.
  - Let the container cleanup task to explicitly wait for the container destruction task
    because the clean event may be fired and handled before the docker's container destroy API returns.
  - Do not create/destroy libzmq contexts for individual containers and reuse a single
    global context, because doing so may hang up when there are many concurrent
    creation/destruction operations going on.
  - Explicitly clean up async generators related to log collection.
  - Limit the concurrency of container creation and destruction to 4 using async semaphore.
  - Add more category switches to control verbosity of debug logs. (`events`, `alloc-map`, `kernel-config`)
  - Add explicit timeouts when calling the docker stats API because it may hang under high loads,
    but this seems to be not a complete solution yet. ([#278](https://github.com/lablup/backend.ai-agent/issues/278))
* Refine the stability update by explicitly skipping over being-terminated containers when syncing container lifecycles periodically ([#279](https://github.com/lablup/backend.ai-agent/issues/279))
* Clarify the error message for quantum size check failure after dividing the request amount into even slices in the allocation map ([#284](https://github.com/lablup/backend.ai-agent/issues/284))
* "PosixPath object has no attribute chown" error in changing the ownership of cluster SSH-keypair. ([#285](https://github.com/lablup/backend.ai-agent/issues/285))
* Make the lxcfs support to be more future-proof ([#287](https://github.com/lablup/backend.ai-agent/issues/287))
* Add explicit dependencies of type annotation packages required by latest mypy (≥0.900) ([#289](https://github.com/lablup/backend.ai-agent/issues/289))
* Unify handling of unhandled exceptions to produce AgentError events and depreacte use of agent-side error monitor plugins ([#290](https://github.com/lablup/backend.ai-agent/issues/290))
* Fix the regression of batch-type sessions by moving `startup_command` invocation to agents ([#293](https://github.com/lablup/backend.ai-agent/issues/293))
* Remove the discouraged `loop` argument from the `AsyncFileWriter` constructor ([#294](https://github.com/lablup/backend.ai-agent/issues/294))
* If there is no exact distro version in the match_distro_data function, the latest distro of the same kind will be returned (e.g.: ubuntu20.04 -> ubuntu16.04). ([#299](https://github.com/lablup/backend.ai-agent/issues/299))

### Miscellaneous
* Increase the maximum allowed overcommit factor of CPU cores from 4 to 10. ([#282](https://github.com/lablup/backend.ai-agent/issues/282))
* Update package dependencies ([#297](https://github.com/lablup/backend.ai-agent/issues/297))


## Older changelogs

* [21.03](https://github.com/lablup/backend.ai-agent/blob/21.03/CHANGELOG.md)
* [20.09](https://github.com/lablup/backend.ai-agent/blob/20.09/CHANGELOG.md)
* [20.03](https://github.com/lablup/backend.ai-agent/blob/20.03/CHANGELOG.md)
