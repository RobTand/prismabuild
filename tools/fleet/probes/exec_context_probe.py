"""What does the memory-cap wrapper change about the execution besides bounding it?

``capped_launch_argv``'s contract is that the wrapper must not change *what* is
executed, only what bounds it.  A transient unit is forked by the **user
manager**, not by the caller, so nothing of the launcher's context reaches the
work except by being named -- and a member that is silently dropped reads as a
payload bug, not a wrapper bug.  This probe is how that list is kept honest: it
runs one identical child twice, once directly and once through the checkout's
own ``capped_launch_argv``, and diffs everything a wrapper could plausibly
change.

It is deliberately a *diff*, not an assertion list.  A new systemd version, a
different box, or a new property added to the wrapper can all move the answer,
and the useful output is "here is what differs now", which a reader can judge
against the two categories the docstring names: carried (execution) or
deliberately not carried (bound).

Submit it; it is cheap but it is still fleet work::

    tools/fleet/pbrun.py --cpus 1 --demand mem_gb=2 -- \
        python3 tools/fleet/probes/exec_context_probe.py

Run it from a checkout root, because it imports *that* checkout's ``pool``:
pointing it at two commits is how a change to the wrapper is measured rather
than asserted.  The launcher restricts itself first -- a two-CPU affinity and a
distinctive soft ``RLIMIT_NOFILE`` -- because a probe that inherits the box's
defaults would report "child matches launcher" for a wrapper that carries
nothing at all.
"""
from __future__ import annotations

import inspect
import json
import os
from pathlib import Path
import resource
import socket
import subprocess
import sys

sys.path.insert(0, str(Path.cwd() / "src"))

from prismabuild import pool  # noqa: E402

#: Distinctive on purpose: different from systemd's ``DefaultLimitNOFILE``
#: soft (1024) and from every box's hard limit, so neither "the child kept the
#: launcher's value" nor "the child got the default" can be read for the other.
PROBE_SOFT_NOFILE = 314159

#: What the child reports about itself.  Written to a file rather than passed
#: as ``-c`` so the two arms run byte-identical children.
CHILD = r'''
import json, os, resource
LIMITS = sorted(n for n in dir(resource) if n.startswith("RLIMIT_"))
mask = os.umask(0o022)
os.umask(mask)
print(json.dumps({
    "affinity": sorted(os.sched_getaffinity(0)),
    "rlimits": {n: list(resource.getrlimit(getattr(resource, n))) for n in LIMITS},
    "umask": oct(mask),
    "nice": os.nice(0),
    "cwd": os.getcwd(),
    "uid": os.getuid(), "gid": os.getgid(), "groups": sorted(os.getgroups()),
    "oom_score_adj": open("/proc/self/oom_score_adj").read().strip(),
    "cgroup": open("/proc/self/cgroup").read().strip(),
    "env_names": sorted(os.environ),
    "PATH": os.environ.get("PATH", ""),
}, sort_keys=True))
'''


def _run(argv: list[str]) -> dict:
    done = subprocess.run(argv, capture_output=True, text=True,
                          stdin=subprocess.DEVNULL)
    if done.returncode != 0:
        return {"_rc": done.returncode,
                "_err": (done.stderr or done.stdout or "")[-500:]}
    return json.loads(done.stdout)


def main() -> int:
    tmp = Path(os.environ.get("TMPDIR", "/home/rob/tmp"))
    tmp.mkdir(parents=True, exist_ok=True)
    child = tmp / f"pb_exec_child_{os.getpid()}.py"
    child.write_text(CHILD)
    try:
        allowed = sorted(os.sched_getaffinity(0))
        os.sched_setaffinity(0, set(allowed[:2]) or set(allowed))
        hard = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
        soft = (PROBE_SOFT_NOFILE
                if hard in (resource.RLIM_INFINITY,) or PROBE_SOFT_NOFILE <= hard
                else max(1, hard - 1))
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
        os.environ.setdefault("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")

        unwrapped = _run([sys.executable, str(child)])

        # Only what this checkout's wrapper actually accepts, so one probe
        # measures both sides of a change to its signature.
        params = inspect.signature(pool.capped_launch_argv).parameters
        carried = {}
        if "cpus" in params:
            carried["cpus"] = sorted(os.sched_getaffinity(0))
        if "nofile" in params:
            carried["nofile"] = resource.getrlimit(resource.RLIMIT_NOFILE)
        unit = "pbexecctx-" + os.urandom(4).hex()
        subprocess.run(["systemctl", "--user", "reset-failed", unit],
                       capture_output=True)
        wrapped = _run(pool.capped_launch_argv(
            [sys.executable, str(child)], cap_gb=4, unit=unit,
            cwd=os.getcwd(), env=os.environ, **carried))
    finally:
        child.unlink(missing_ok=True)

    report: dict[str, object] = {
        "host": socket.gethostname(),
        "commit": subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                                 capture_output=True, text=True).stdout.strip(),
        "wrapper_carries": sorted(carried),
        "launcher": {"affinity": sorted(os.sched_getaffinity(0)),
                     "nofile": list(resource.getrlimit(resource.RLIMIT_NOFILE))},
    }
    if "_rc" in unwrapped or "_rc" in wrapped:
        report["error"] = {"unwrapped": unwrapped, "wrapped": wrapped}
        print(json.dumps(report, indent=1, sort_keys=True))
        return 1

    diffs = {}
    for name in sorted(set(unwrapped) | set(wrapped)):
        if name in ("rlimits", "env_names"):
            continue
        if unwrapped.get(name) != wrapped.get(name):
            diffs[name] = {"unwrapped": unwrapped.get(name),
                           "wrapped": wrapped.get(name)}
    rlimits = {n: {"unwrapped": v, "wrapped": wrapped["rlimits"].get(n)}
               for n, v in unwrapped["rlimits"].items()
               if v != wrapped["rlimits"].get(n)}
    report["differs"] = diffs
    report["rlimits_differing"] = rlimits
    report["rlimits_identical"] = len(unwrapped["rlimits"]) - len(rlimits)
    report["env_only_in_unit"] = sorted(
        set(wrapped["env_names"]) - set(unwrapped["env_names"]))
    report["env_only_in_launcher"] = sorted(
        set(unwrapped["env_names"]) - set(wrapped["env_names"]))
    print(json.dumps(report, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
