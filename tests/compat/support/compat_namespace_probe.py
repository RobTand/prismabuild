"""Probe the exact private PID/proc namespace used by record compatibility."""
from __future__ import annotations

from itertools import islice
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess


BWRAP_NAMESPACE_ARGS = ("--unshare-pid", "--ro-bind", "/", "/", "--dev", "/dev", "--proc", "/proc")
PROC_MOUNT_ERROR = "bwrap: Can't mount proc on /newroot/proc: Operation not permitted"


def probe_compat_proc(bwrap):
    return subprocess.run([bwrap, *BWRAP_NAMESPACE_ARGS, "true"],
                          capture_output=True, text=True, timeout=10)


def compat_proc_diagnostics(probe):
    """Bounded, read-only observations of the refusing action's own namespace."""
    result = {"host": socket.gethostname(), "probe_argv": probe.args,
              "returncode": probe.returncode, "stderr": probe.stderr,
              "affinity": sorted(os.sched_getaffinity(0)),
              "action_key": os.environ.get("PRISMABUILD_ACTION_KEY")}
    paths = ("/proc/self/mountinfo", "/proc/self/status", "/proc/self/cgroup",
             "/proc/self/uid_map", "/proc/self/gid_map", "/proc/self/attr/current",
             "/proc/sys/kernel/apparmor_restrict_unprivileged_userns",
             "/proc/sys/user/max_user_namespaces", "/proc/sys/user/max_pid_namespaces",
             "/proc/sys/user/max_mnt_namespaces")
    for name in paths:
        try:
            text = Path(name).read_text()
            if name.endswith("/mountinfo"):
                text = "\n".join(line for line in text.splitlines()
                                 if line.split()[4].startswith("/proc"))
            elif name.endswith("/status"):
                text = "\n".join(line for line in text.splitlines()
                                 if line.startswith(("NoNewPrivs:", "Seccomp:", "Seccomp_filters:", "CapEff:")))
            result[name] = text[:16384]
        except OSError as exc:
            result[name] = f"{type(exc).__name__}: {exc}"
    for name in ("mnt", "pid", "user"):
        path = f"/proc/self/ns/{name}"
        try:
            result[path] = os.readlink(path)
        except OSError as exc:
            result[path] = f"{type(exc).__name__}: {exc}"
    try:
        with os.scandir("/sys/fs/cgroup/prismabuild.slice") as entries:
            siblings = [entry.name for entry in islice(entries, 129)]
        result["sibling_scopes"] = sorted(name for name in siblings[:128]
                                          if name.startswith("prismabuild-job") and name.endswith(".slice"))
        result["sibling_scopes_truncated"] = len(siblings) > 128
    except OSError as exc:
        result["sibling_scopes_error"] = f"{type(exc).__name__}: {exc}"
    try:
        version = subprocess.run([probe.args[0], "--version"],
                                 capture_output=True, text=True, timeout=10)
        result["bwrap_version"] = {"returncode": version.returncode,
                                   "stdout": version.stdout, "stderr": version.stderr}
    except (OSError, subprocess.TimeoutExpired) as exc:
        result["bwrap_version_error"] = f"{type(exc).__name__}: {exc}"
    return result


def compat_namespace_main():
    bwrap = shutil.which("bwrap")
    result = {"bwrap": bwrap, "affinity": sorted(os.sched_getaffinity(0))}
    if bwrap:
        child = probe_compat_proc(bwrap)
        result.update(bwrap_exit=child.returncode, bwrap_namespace=child.returncode == 0,
                      bwrap_error=child.stderr.strip())
        if child.returncode:
            result["diagnostics"] = compat_proc_diagnostics(child)
    print(json.dumps(result))


if __name__ == "__main__":
    compat_namespace_main()
