"""What does the memory-cap wrapper change about the execution besides bounding it?

``capped_launch_argv``'s contract is that the wrapper must not change *what* is
executed, only what bounds it.  A transient unit is forked by the **user
manager**, not by the caller, so nothing of the launcher's context reaches the
work except by being named -- and a member that is silently dropped reads as a
payload bug, not a wrapper bug.  This probe is how that list is kept honest: it
runs one identical child twice, once directly and once through the checkout's
own ``capped_launch_argv``, and diffs everything a wrapper could plausibly
change.

**The launcher perturbs every axis it is about to compare, and that is the
method rather than a detail.**  An axis left at the box's default matches on
both sides whether the wrapper carries it or not, so a diff that leaves it
alone is green against a wrapper that carries nothing.  The first version of
this probe restricted only the affinity and ``RLIMIT_NOFILE``, and duly
reported "the other 14 rlimits: identical" -- true, and evidence of nothing.
Perturbed, sparky answers differently: ``CORE``, ``MSGQUEUE``, ``NPROC``,
``SIGPENDING`` and ``STACK`` move too, and so do the umask and the nice level.
Whatever this probe does not perturb, it does not get to call carried.

It is deliberately a *diff*, not an assertion list.  A new systemd version, a
different box, or a new property added to the wrapper can all move the answer,
and the useful output is "here is what differs now", which a reader can judge
against the three categories the docstring names: carried (execution),
deliberately not carried (the bound), or the mechanism itself.

Submit it; it is cheap but it is still fleet work::

    tools/fleet/pbrun.py --cpus 1 --demand mem_gb=2 -- \
        python3 tools/fleet/probes/exec_context_probe.py

Run it from a checkout root, because it imports *that* checkout's ``pool``:
pointing it at two commits is how a change to the wrapper is measured rather
than asserted.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import resource
import socket
import subprocess
import sys

sys.path.insert(0, str(Path.cwd() / "src"))

from prismabuild import pool  # noqa: E402

#: Soft limits the launcher moves before either arm runs.  Every value is
#: chosen to be neither the box's default nor systemd's, so "kept the
#: launcher's" and "got the manager's" cannot be read for each other.  A target
#: above the hard limit is clamped to the hard limit rather than skipped: any
#: value the launcher can actually hold distinguishes the two.
PROBE_LIMITS = {
    "RLIMIT_NOFILE": 314159,
    "RLIMIT_CORE": 1,
    "RLIMIT_STACK": 9 * 1024 * 1024,
    "RLIMIT_MSGQUEUE": 819100,
    "RLIMIT_NPROC": 100000,
    "RLIMIT_SIGPENDING": 100000,
    "RLIMIT_CPU": 86400,
    "RLIMIT_FSIZE": 10 ** 12,
    "RLIMIT_DATA": 10 ** 13,
    "RLIMIT_AS": 10 ** 13,
    "RLIMIT_MEMLOCK": 8 * 1024 * 1024,
    "RLIMIT_RTTIME": 10 ** 6,
}

PROBE_UMASK = 0o077
PROBE_NICE = 5

#: Differences that are the *bound* or the *mechanism* rather than the
#: execution, so a run that reports exactly these is a clean run.  Named here
#: so the probe's output can be read without the docstring: anything outside
#: this set is either a carry that regressed or a member nobody has classified.
EXPECTED_DIFFERENCES = ("cgroup", "oom_score_adj", "pgid", "sid", "ppid")

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
    # The process group and the session cannot be restated as unit properties.
    # They are reported anyway because their *consequence* is a bound: a signal
    # to the launcher's group does not reach a unit's work at all.
    "pgid": os.getpgid(0), "sid": os.getsid(0), "ppid": os.getppid(),
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


def _perturb() -> dict[str, object]:
    """Move every axis this probe is about to compare.  Returns what it moved."""

    moved: dict[str, object] = {}
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) > 1:
        os.sched_setaffinity(0, set(allowed[:2]))
        moved["affinity"] = sorted(os.sched_getaffinity(0))
    for name, target in PROBE_LIMITS.items():
        number = getattr(resource, name, None)
        if number is None:
            continue
        soft, hard = resource.getrlimit(number)
        want = target if hard == resource.RLIM_INFINITY else min(target, hard)
        if want == soft:
            continue
        try:
            resource.setrlimit(number, (want, hard))
        except (ValueError, OSError) as exc:
            moved[name] = f"unchanged: {exc}"
            continue
        moved[name] = [want, hard]
    moved["umask"] = oct(os.umask(PROBE_UMASK))
    # Only ever upward without privilege, which is why this process is the
    # launcher and not the caller's shell.
    moved["nice"] = os.nice(PROBE_NICE)
    return moved


def main() -> int:
    tmp = Path(os.environ.get("TMPDIR", "/home/rob/tmp"))
    tmp.mkdir(parents=True, exist_ok=True)
    child = tmp / f"pb_exec_child_{os.getpid()}.py"
    child.write_text(CHILD)
    unit = "pbexecctx-" + os.urandom(4).hex()
    try:
        os.environ.setdefault("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
        perturbed = _perturb()

        unwrapped = _run([sys.executable, str(child)])
        subprocess.run(["systemctl", "--user", "reset-failed", unit],
                       capture_output=True)
        wrapped = _run(pool.capped_launch_argv(
            [sys.executable, str(child)], cap_gb=4, unit=unit,
            cwd=os.getcwd(), env=os.environ, **pool.launcher_exec_context()))
    finally:
        child.unlink(missing_ok=True)
        subprocess.run(["systemctl", "--user", "reset-failed", unit],
                       capture_output=True)

    report: dict[str, object] = {
        "host": socket.gethostname(),
        "commit": subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                                 capture_output=True, text=True).stdout.strip(),
        "wrapper_carries": sorted(pool.launcher_exec_context()),
        "launcher_perturbed": perturbed,
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
    report["unclassified"] = sorted(
        set(diffs) - set(EXPECTED_DIFFERENCES)) + sorted(rlimits)
    report["clean"] = not report["unclassified"]
    report["env_only_in_unit"] = sorted(
        set(wrapped["env_names"]) - set(unwrapped["env_names"]))
    report["env_only_in_launcher"] = sorted(
        set(unwrapped["env_names"]) - set(wrapped["env_names"]))
    print(json.dumps(report, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
