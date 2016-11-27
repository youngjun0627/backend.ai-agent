Changes
=======

0.7.1 (2016-11-27)
------------------

**NEW**

 - `--agent-ip-override` CLI option to override the IP address of agent
   reported to the manager.

0.7.0 (2016-11-25)
------------------

**NEW**

 - Add support for kernel restarts.
   Restarting preserves kernel metadata and its ID, but removes and recreates
   the working volume and the container itself.

 - Add `--debug` option to the CLI command.

0.6.0 (2016-11-14)
------------------

**NEW**

 - Add support for GPU-enabled kernels (using [nvidia-docker plugin][nvdocker]).
   The kernel images must be built upon nvidia-docker's base Ubuntu images and
   have the label "io.sorna.nvidia.enabled" set `yes`.

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
   system when running `os.cpu_count()`, `multiprocessing.cpu_count()` or
   `os.sysconf("SC_NPROCESSORS_ONLN")` because the limit is enforced by the CPU
   affinity mask.  To get the correct result, try
   `len(os.sched_getaffinity(os.getpid()))`.

[nvdocker]: https://github.com/NVIDIA/nvidia-docker


0.5.0 (2016-11-01)
------------------

**NEW**

 - First public release.

