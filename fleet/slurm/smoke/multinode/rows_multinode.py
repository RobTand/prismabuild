#!/usr/bin/python3
"""The three-node smoke's rows: what only a cluster can answer.

Each row is one named claim with one verdict, in the style of
``fleet/slurm/smoke/rows.py``, and the table at the end is the whole result.

Unlike ``rows.py`` this runs on the HOST rather than inside a node.  Three
rows have to reach into a container that is not the submitter's -- M5 submits
from ``sparky``, M6 kills ``sparky``'s slurmd, M7 restarts ``dl380g10``'s
slurmctld -- and the image's ``docker`` is the Epilog's fake one, which runs
nothing.  So a row drives a container with ``docker exec`` and reads the
records back off the shared volume at its host path.  The volume is the same
bytes either way; only the prefix differs, which is what ``inside`` and
``here`` name below.

Stdlib only, and no import from the tree under test: the rows are a reader of
what ``pbrun`` filed, not a second implementation of it.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import genconf  # noqa: E402  (same directory, for the fleet config reader)

STAMP = os.environ["PB_SMOKE3_STAMP"]
VOL = Path(os.environ["PB_SMOKE3_VOL"])          # the volume, at its host path
REPO = Path(os.environ["PB_SMOKE3_REPO"])
INSIDE = Path("/mnt/shared")                     # the same volume, in a node

NODES = ("dl380g10", "sparky", "gx10-6b77")
GPU_NODES = ("sparky", "gx10-6b77")
CONTAINER = {node: f"pb-smoke3-{node}-{STAMP}" for node in NODES}

SH = VOL / "prismabuild-fleet"
QUEUE = SH / "pb-queue"
LANE = SH / "slurm"
WORK = VOL / "work"
SRC = WORK / "src"
SRC_INSIDE = INSIDE / "work" / "src"

#: Job submission through this lane costs a git materialization and a poll
#: interval, so a row that expects an ending waits minutes, not seconds.
WAIT_S = 300.0
#: The container's own PATH.  `docker exec` gives a very short default one and
#: `pbrun` resolves argv[0] against what it is told.
PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

results: list[tuple[str, str, str]] = []
_submitted_re = re.compile(r"submitted ([0-9a-f]{12}) as slurm job (\d+)")


def record(name: str, ok: bool, detail: str = "") -> bool:
    results.append((name, "PASS" if ok else "FAIL", detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}  {detail}", flush=True)
    return ok


def sh(argv: list[str], *, timeout: float = 900.0) -> subprocess.CompletedProcess:
    return subprocess.run(argv, capture_output=True, text=True, timeout=timeout)


def exec_argv(node: str, argv: list[str], *, user: str = "rob",
              cwd: str | None = None) -> list[str]:
    head = ["docker", "exec", "-u", user, "-e", "HOME=/home/rob",
            "-e", f"PATH={PATH}"]
    if cwd:
        head += ["-w", cwd]
    return head + [CONTAINER[node], *argv]


def dexec(node: str, argv: list[str], *, user: str = "rob",
          cwd: str | None = None,
          timeout: float = 900.0) -> subprocess.CompletedProcess:
    return sh(exec_argv(node, argv, user=user, cwd=cwd), timeout=timeout)


def ddetach(node: str, argv: list[str], *, user: str = "root") -> None:
    """Start something in a container and leave it running.

    `docker exec -d`, not a backgrounded shell: a daemon restarted by a row has
    to outlive the exec session that started it, and the two rows that restart
    one are the rows about a daemon coming back.
    """

    sh(["docker", "exec", "-d", "-u", user, CONTAINER[node], *argv], timeout=60)


def ctl(argv: list[str], *, timeout: float = 120.0) -> subprocess.CompletedProcess:
    """A scheduler client command, run on the controller's box."""

    return dexec("dl380g10", argv, timeout=timeout)


def pbrun_argv(command: list[str], *, extra: list[str] = []) -> list[str]:
    return [
        "python3", "/repo/tools/fleet/pbrun.py",
        "--transport", "slurm",
        "--cwd", str(SRC_INSIDE),
        "--wait-s", str(WAIT_S),
        *extra,
        "--", *command,
    ]


def pbrun(node: str, command: list[str], *, extra: list[str] = [],
          timeout: float = 900.0) -> subprocess.CompletedProcess:
    return dexec(node, pbrun_argv(command, extra=extra),
                 cwd=str(SRC_INSIDE), timeout=timeout)


def submitted(completed: subprocess.CompletedProcess) -> tuple[str, str]:
    """The key prefix and job id ``pbrun`` announced, or two empty strings."""

    match = _submitted_re.search(completed.stderr or "")
    return (match.group(1), match.group(2)) if match else ("", "")


def outcome(state: str, prefix: str) -> tuple[Path | None, dict]:
    """The terminal record filed under ``done/`` or ``failed/`` for a prefix."""

    directory = QUEUE / state
    if not prefix or not directory.is_dir():
        return None, {}
    for path in sorted(directory.glob(f"{prefix}*.json")):
        try:
            return path, json.loads(path.read_text())
        except (OSError, ValueError):
            return path, {}
    return None, {}


def submission(prefix: str) -> dict:
    """The sealed record of what was sent to ``sbatch`` for this action."""

    if not prefix or not LANE.is_dir():
        return {}
    for directory in sorted(LANE.iterdir()):
        if directory.name.startswith(prefix) and (directory / "latest.json").is_file():
            try:
                return json.loads((directory / "latest.json").read_text())
            except (OSError, ValueError):
                return {}
    return {}


def flags(sealed: dict) -> list[str]:
    """The exact argv the lane sent to `sbatch`, out of the submission record."""

    return [str(item) for item in sealed.get("argv", [])]


def wait_for(predicate, *, timeout_s: float, poll_s: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(poll_s)
    return bool(predicate())


def job_state(job_id: str) -> str:
    if not job_id:
        return ""
    answer = ctl(["squeue", "-h", "-j", job_id, "-o", "%T"])
    text = (answer.stdout or "").strip()
    if text:
        return text.splitlines()[0].strip()
    answer = ctl(["scontrol", "show", "job", job_id])
    found = re.search(r"JobState=(\S+)", answer.stdout or "")
    return found.group(1) if found else ""


def job_node(job_id: str) -> str:
    """Which node a job holds, asked of `squeue` rather than `scontrol`.

    `scontrol show job` prints `ReqNodeList=` and `ExcNodeList=` before
    `NodeList=`, so the obvious search finds a fragment of the wrong field.
    """

    answer = ctl(["squeue", "-h", "-j", job_id, "-o", "%N"])
    node = (answer.stdout or "").strip().splitlines()
    node = node[0].strip() if node else ""
    return "" if node in ("", "(null)", "None assigned") else node


def node_states() -> dict[str, str]:
    answer = ctl(["sinfo", "-h", "-N", "-p", "all", "-o", "%N|%T"])
    states: dict[str, str] = {}
    for line in (answer.stdout or "").splitlines():
        name, _, state = line.strip().partition("|")
        if name:
            states[name] = state
    return states


def lines(path: Path) -> int:
    try:
        return path.read_text().count("\n")
    except OSError:
        return 0


# ---------------------------------------------------------------------------
# What the fleet's own configuration says, read rather than restated
# ---------------------------------------------------------------------------

def fleet_nodes() -> dict[str, dict[str, str]]:
    """``NodeName``'s Gres and Feature, straight out of fleet/slurm/slurm.conf."""

    text = (REPO / "fleet" / "slurm" / "slurm.conf").read_text(encoding="utf-8")
    found: dict[str, dict[str, str]] = {}
    for line in genconf.logical_lines(text):
        bare = line.strip()
        if not bare.startswith("NodeName="):
            continue
        fields = dict(
            part.split("=", 1) for part in bare.split() if "=" in part
        )
        found[fields["NodeName"]] = {
            "gres": fields.get("Gres", ""),
            "features": fields.get("Feature", ""),
        }
    return found


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def build_source_repo() -> None:
    """A git checkout for the actions to be snapshotted from, on the volume.

    Built inside the controller container so every path in it is a path a node
    will see, and executable so that ``./action.sh`` -- rather than ``bash
    action.sh`` -- is the command.  That distinction is load-bearing: ``pbrun``
    pins an action to the submitting box whenever argv[0] resolves to an
    executable that is neither in the checkout nor on shared storage, and
    ``/usr/bin/bash`` is one.  Every row below that expects the scheduler to
    choose a node needs the choice to be the scheduler's.
    """

    script = (
        "#!/bin/bash\n"
        "# The smoke's action.  Every row's command is a mode of this script,\n"
        "# so that what differs between rows is the cluster's behaviour.\n"
        "set -u\n"
        'mode="${1:-run}"\n'
        'nonce="${2:-}"\n'
        'echo "action: mode=$mode host=$(hostname -s) job=${SLURM_JOB_ID:-none}"\n'
        'if [ -n "$nonce" ]; then echo "ran on $(hostname -s)" >>"$nonce"; fi\n'
        'case "$mode" in\n'
        '    run)   echo "action: done" ;;\n'
        '    fail)  echo "action: failing on purpose" >&2; exit 7 ;;\n'
        '    sleep) sleep "${3:-300}" ;;\n'
        'esac\n'
        "exit 0\n"
    )
    dexec("dl380g10", ["rm", "-rf", str(SRC_INSIDE)])
    dexec("dl380g10", ["mkdir", "-p", str(SRC_INSIDE)])
    # `docker exec` here has no stdin, so the script travels as a single-quoted
    # argument the shell inside the container prints.
    payload = script.replace("'", "'\"'\"'")
    made = dexec("dl380g10", [
        "bash", "-c",
        f"printf '%s' '{payload}' >{SRC_INSIDE}/action.sh && "
        f"chmod 0755 {SRC_INSIDE}/action.sh",
    ])
    if made.returncode != 0:
        raise SystemExit(f"smoke3: could not write action.sh: {made.stderr}")
    for argv in (
        ["git", "-C", str(SRC_INSIDE), "init", "-q"],
        ["git", "-C", str(SRC_INSIDE), "add", "action.sh"],
        ["git", "-C", str(SRC_INSIDE), "commit", "-qm", "smoke action"],
    ):
        completed = dexec("dl380g10", argv)
        if completed.returncode != 0:
            raise SystemExit(f"smoke3: {argv} failed: {completed.stderr}")


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------

#: What `fleet/slurm/verify.sh` can and cannot answer in containers.
#:
#: Every row of that script is expected to PASS here except these, and each
#: exception is a fact about the containers rather than about the script.  A
#: row that changes verdict in either direction fails M0, which is the point:
#: the script has never run anywhere else, so this is the only thing standing
#: between it and the first time an operator types it on the real fleet.
VERIFY_EXPECTED_FAILURES = {
    "0": "the installed slurm.conf is genconf.py's, which deviates from the "
         "checkout's on purpose; on the fleet install.sh copies the checkout",
    "0b": "the same deviation, read off the other two boxes; what this row "
          "does establish here is that its ssh read reached them",
    "3": "no NVIDIA driver in a container, so nvidia-smi is not installed and "
         "there is no GPU for a shard job to see",
    "4": "genconf.py sets ConstrainDevices=no, there being no real device to "
         "constrain, so the job opens the mknod'd /dev/nvidia0 and the row "
         "correctly says so; this is the row that needs a box with a driver",
    "7": "the repository is mounted read-only, because a smoke must not be "
         "able to change the tree it is testing, and pbrun writes its "
         "closure stamp inside the checkout.  The claim itself -- pbrun "
         "through the lane, end to end -- is what M2 to M11 do eleven times "
         "from a writable checkout on the volume",
}

_VERIFY_ROW = re.compile(r"^\[(PASS|FAIL)\]\s+(\S+)\s")


def row_m0_verify_script() -> None:
    """Run the runbook's own post-install check, which has never run.

    `fleet/slurm/verify.sh` is step 8 of `docs/slurm_runbook_2026-09-04.md` as
    an executable, and every one of its rows needs a live controller, three
    registered nodes and an ssh route between the boxes.  This harness is the
    only place all three exist before the install.
    """

    completed = dexec(
        "dl380g10",
        ["bash", "/repo/fleet/slurm/verify.sh", "--keep-going"],
        cwd="/repo", timeout=1200.0,
    )
    text = (completed.stdout or "") + (completed.stderr or "")
    seen: dict[str, set[str]] = {}
    for line in text.splitlines():
        found = _VERIFY_ROW.match(line.strip())
        if found:
            seen.setdefault(found.group(2), set()).add(found.group(1))
    unexpected: list[str] = []
    for row, marks in sorted(seen.items()):
        wanted = {"FAIL"} if row in VERIFY_EXPECTED_FAILURES else {"PASS"}
        if marks != wanted:
            unexpected.append(f"{row}={'/'.join(sorted(marks))}")
    # And it must not have declared the fleet ready.  The marker is what
    # cutover.sh looks for, and a harness that wrote one would be handing a
    # container's verdict to the real cutover.
    marker = dexec("dl380g10", [
        "test", "-e", "/home/rob/.prismabuild/slurm-verify-passed.json",
    ])
    checks = {
        "verify.sh produced a table": bool(seen),
        "every row is PASS but the five a container cannot answer":
            not unexpected,
        "it refused to write the ready marker": marker.returncode != 0,
    }
    failed = [key for key, ok in checks.items() if not ok]
    print(text.rstrip(), flush=True)
    record(
        "M0 fleet/slurm/verify.sh runs, for the first time anywhere",
        not failed,
        f"{sum(1 for m in seen.values() if m == {'PASS'})} rows PASS, "
        f"{len(VERIFY_EXPECTED_FAILURES)} expected FAIL "
        f"({', '.join(sorted(VERIFY_EXPECTED_FAILURES))})"
        + ("" if not failed else f" MISSING {failed}; unexpected={unexpected}"),
    )


def row_m1_three_nodes() -> None:
    """All three nodes idle, offering exactly what the fleet's file declares."""

    declared = fleet_nodes()
    answer = ctl(["sinfo", "-h", "-N", "-p", "all", "-o", "%N|%T|%G|%f"])
    seen: dict[str, tuple[str, str, str]] = {}
    for line in (answer.stdout or "").splitlines():
        fields = line.strip().split("|")
        if len(fields) == 4:
            seen[fields[0]] = (fields[1], fields[2], fields[3])
    problems: list[str] = []
    for node, wanted in declared.items():
        if node not in seen:
            problems.append(f"{node}: absent from sinfo")
            continue
        state, gres, features = seen[node]
        if not state.startswith("idle"):
            problems.append(f"{node}: state {state!r}")
        # sinfo prints "(null)" where a node declares no Gres, and appends a
        # "(S:...)" socket qualifier when one is configured; neither is a
        # difference from the file.
        clean = re.sub(r"\(S:[^)]*\)", "", gres).strip()
        want_gres = wanted["gres"] or "(null)"
        if clean != want_gres:
            problems.append(f"{node}: gres {clean!r} != {want_gres!r}")
        if set(features.split(",")) != set(wanted["features"].split(",")):
            problems.append(f"{node}: features {features!r} != {wanted['features']!r}")
    record(
        "M1 sinfo -N: three nodes idle with the fleet's Gres and Features",
        not problems and len(seen) == 3,
        f"{'; '.join(f'{n}={v[0]} {v[1]} {v[2]}' for n, v in sorted(seen.items()))}"
        + (f" PROBLEMS {problems}" if problems else ""),
    )


def _placement_row(name: str, node: str, command: list[str], *,
                   extra: list[str] = [],
                   expect_host: tuple[str, ...],
                   expect_partition: str,
                   expect_constraint: list[str],
                   expect_gres: str = "",
                   extra_checks=None) -> None:
    completed = pbrun(node, command, extra=extra)
    prefix, job_id = submitted(completed)
    path, ending = outcome("done", prefix)
    sealed = submission(prefix)
    argv = flags(sealed)
    checks = {
        "pbrun rc==0": completed.returncode == 0,
        "done record": path is not None,
        "status=executed": ending.get("status") == "executed",
        f"claimed_host in {expect_host}": ending.get("claimed_host") in expect_host,
        f"record partition=={expect_partition!r}":
            sealed.get("partition", "<missing>") == expect_partition,
        f"constraint=={expect_constraint}": sealed.get("constraint") == expect_constraint,
        f"gres=={expect_gres!r}": (sealed.get("gres") or "") == expect_gres,
    }
    if expect_partition:
        checks[f"argv has --partition={expect_partition}"] = (
            f"--partition={expect_partition}" in argv)
    else:
        checks["argv has no --partition"] = not any(
            item.startswith("--partition") for item in argv)
    if expect_gres:
        checks[f"argv has --gres={expect_gres}"] = f"--gres={expect_gres}" in argv
    for key, value in (extra_checks or {}).items():
        checks[key] = value(completed, sealed, ending)
    failed = [key for key, ok in checks.items() if not ok]
    record(
        name, not failed,
        f"job={job_id} on {ending.get('claimed_host')!r} "
        f"partition={sealed.get('partition')!r} "
        f"constraint={sealed.get('constraint')} gres={sealed.get('gres')!r}"
        + ("" if not failed else
           f" MISSING {failed}; rc={completed.returncode}; "
           f"stderr={(completed.stderr or '')[-500:]}"),
    )


def row_m2_cpu_only(nonce: str) -> None:
    """Untagged CPU-only work goes to the CPU box, with no deadline."""

    _placement_row(
        "M2 an untagged CPU-only pbrun lands on dl380g10 via --partition=cpu",
        "dl380g10", ["./action.sh", "run", nonce],
        expect_host=("dl380g10",),
        expect_partition="cpu",
        expect_constraint=[],
        extra_checks={
            # No --timeout-s was given, so no deadline was asked for and none
            # should have been sent.  A job with no --time runs while it runs.
            'time_limit==""':
                lambda c, s, e: s.get("time_limit", "<missing>") == "",
            "argv has no --time":
                lambda c, s, e: not any(
                    item.startswith("--time") for item in flags(s)),
        },
    )


def row_m3_gpu(nonce: str) -> None:
    """A --gpu action goes to a Spark, in the GPU partition, holding a shard."""

    _placement_row(
        "M3 a --gpu pbrun lands on a Spark via --partition=gpu --gres=shard:1",
        "dl380g10", ["./action.sh", "run", nonce],
        extra=["--gpu"],
        expect_host=GPU_NODES,
        expect_partition="gpu",
        expect_constraint=[],
        expect_gres="shard:1",
    )


def row_m4_tagged(nonce: str) -> None:
    """A hostname tag becomes a --constraint, and the constraint decides."""

    _placement_row(
        "M4 a --tag gx10-6b77 CPU-only pbrun lands there with no --partition",
        "dl380g10", ["./action.sh", "run", nonce],
        extra=["--tag", "gx10-6b77"],
        expect_host=("gx10-6b77",),
        expect_partition="",
        expect_constraint=["gx10-6b77"],
    )


def row_m9_class_tag(nonce: str) -> None:
    """`cpu` is a Feature, not only a partition, so a class tag is schedulable.

    The pull queue announces `cpu` from a box with `--gpu-slots 0`, so
    `pbrun --tag cpu` is a thing an agent already writes.  Under this lane a
    tag becomes a `--constraint`, and a constraint naming a Feature no node
    carries is refused at submit -- which would turn a working submission into
    a refusal at the cutover.
    """

    _placement_row(
        "M9 a --tag cpu pbrun lands on the CPU box, the tag being a Feature",
        "dl380g10", ["./action.sh", "run", nonce],
        extra=["--tag", "cpu"],
        expect_host=("dl380g10",),
        expect_partition="",
        expect_constraint=["cpu"],
    )


def row_m10_anywhere_idle(nonce: str) -> None:
    """`--anywhere` opens the whole fleet, and weight still prefers the CPU box."""

    _placement_row(
        "M10 an --anywhere pbrun on an idle fleet lands on the CPU box",
        "dl380g10", ["./action.sh", "run", nonce],
        extra=["--anywhere"],
        expect_host=("dl380g10",),
        expect_partition="",
        expect_constraint=[],
    )


def row_m11_anywhere_overflow(nonce: str) -> None:
    """With the CPU box full, `--anywhere` work overflows onto a Spark.

    The point of the weight is that it is a preference and not a pin: work the
    submitter asserted portable must not queue behind a busy dl380g10 while
    two idle Sparks watch.  The box is filled with an `--exclusive`
    placeholder rather than by guessing a core count, so the row holds if the
    fleet's CPUs line changes.
    """

    blocker = ctl([
        "sbatch", "--parsable", "--exclusive", "--nodelist=dl380g10",
        "--mem=1024", "--time=00:05:00", "--chdir=/tmp",
        "--output=/dev/null", "--wrap=sleep 240",
    ])
    job = (blocker.stdout or "").strip().split(";")[0]
    held = wait_for(lambda: job_state(job) == "RUNNING",
                    timeout_s=120) if job.isdigit() else False
    try:
        _placement_row(
            "M11 an --anywhere pbrun overflows to a Spark when the CPU box is full",
            "dl380g10", ["./action.sh", "run", nonce],
            extra=["--anywhere"],
            expect_host=GPU_NODES,
            expect_partition="",
            expect_constraint=[],
            extra_checks={
                "dl380g10 was actually full": lambda c, s, e: held,
            },
        )
    finally:
        if job.isdigit():
            ctl(["scancel", job])


def row_m5_cross_box(nonce: str) -> None:
    """Submitted on sparky, executed on dl380g10, read back off the volume."""

    completed = pbrun("sparky", ["./action.sh", "run", nonce])
    prefix, job_id = submitted(completed)
    path, ending = outcome("done", prefix)
    sealed = submission(prefix)
    stdout_path = Path(str(sealed.get("stdout") or "").replace(
        str(INSIDE), str(VOL), 1))
    text = ""
    try:
        text = stdout_path.read_text(errors="replace")
    except OSError:
        pass
    host_nonce = VOL / Path(nonce).relative_to(INSIDE)
    checks = {
        "pbrun rc==0": completed.returncode == 0,
        "done record": path is not None,
        "submitted_host==sparky": sealed.get("submitted_host") == "sparky",
        "claimed_host==dl380g10": ending.get("claimed_host") == "dl380g10",
        "receipt_published": (ending.get("detail") or {}).get(
            "receipt_published") is True,
        "job stdout is on the shared volume": stdout_path.is_file(),
        "the action says it ran on dl380g10": "host=dl380g10" in text,
        "the action's own output file came back": "dl380g10" in (
            host_nonce.read_text() if host_nonce.exists() else ""),
    }
    failed = [key for key, ok in checks.items() if not ok]
    record(
        "M5 a pbrun submitted on sparky executes on dl380g10 and comes back",
        not failed,
        f"job={job_id} submitted on {sealed.get('submitted_host')!r} "
        f"claimed by {ending.get('claimed_host')!r} "
        f"stdout={stdout_path.name if stdout_path.name else '?'}"
        + ("" if not failed else
           f" MISSING {failed}; rc={completed.returncode}; "
           f"stderr={(completed.stderr or '')[-500:]}"),
    )


def row_m6_node_fail(nonce: str) -> None:
    """A node dies under a running job, and comes back on its own."""

    process = subprocess.Popen(
        exec_argv("dl380g10",
                  pbrun_argv(["./action.sh", "sleep", nonce, "300"],
                             extra=["--tag", "sparky"]),
                  cwd=str(SRC_INSIDE)),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    prefix = job_id = ""
    deadline = time.monotonic() + 240
    first = ""
    while time.monotonic() < deadline:
        first = process.stderr.readline()
        if not first:
            break
        match = _submitted_re.search(first)
        if match:
            prefix, job_id = match.group(1), match.group(2)
            break
    running = wait_for(lambda: job_state(job_id) == "RUNNING",
                       timeout_s=240) if job_id else False
    where = job_node(job_id) if job_id else ""

    # The node goes away the way a node goes away: its slurmd stops answering
    # and the processes it was supervising die with it.  Not `scontrol update
    # State=DOWN`, which is an operator telling the controller something it
    # would otherwise have to find out.
    killed = dexec("sparky", [
        "bash", "-c",
        "pkill -9 slurmd; pkill -9 slurmstepd; pkill -9 -u rob; exit 0",
    ], user="root")
    del killed

    try:
        rest = process.communicate(timeout=600)[1]
    except subprocess.TimeoutExpired:
        process.kill()
        rest = ""
    said = (first or "") + (rest or "")
    path, ending = outcome("failed", prefix)
    detail = ending.get("detail") or {}
    state = str((detail.get("slurm") or {}).get("state") or "")

    # And back, without an operator resuming it: ReturnToService=2 returns a
    # node when it registers again, which is the whole reason that line is in
    # the fleet's file.
    ddetach("sparky", ["/usr/local/bin/pb-start-slurmd"])
    returned = wait_for(lambda: node_states().get("sparky", "").startswith("idle"),
                        timeout_s=180, poll_s=2.0)

    checks = {
        "the job ran on sparky": running and where == "sparky",
        "pbrun failed": process.returncode not in (0, None),
        "failed/ record": path is not None,
        "a terminal state was recorded": bool(state),
        "pbrun named that state": f"failed ({state})" in said if state else False,
        "no receipt": detail.get("receipt_published") is not True,
        "sparky returned to idle with no operator action": returned,
    }
    failed = [key for key, ok in checks.items() if not ok]
    record(
        "M6 a node killed under a running job ends it, and returns on its own",
        not failed,
        f"job={job_id} on {where!r} state={state!r} pbrun rc={process.returncode} "
        f"returned={node_states().get('sparky')!r}"
        + ("" if not failed else
           f" MISSING {failed}; said={said[-600:]!r}"),
    )


def row_m7_controller_restart(nonce: str) -> None:
    """The controller is restarted under a running job; the job still lands."""

    process = subprocess.Popen(
        exec_argv("dl380g10",
                  pbrun_argv(["./action.sh", "sleep", nonce, "150"]),
                  cwd=str(SRC_INSIDE)),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    prefix = job_id = ""
    deadline = time.monotonic() + 240
    first = ""
    while time.monotonic() < deadline:
        first = process.stderr.readline()
        if not first:
            break
        match = _submitted_re.search(first)
        if match:
            prefix, job_id = match.group(1), match.group(2)
            break
    running = wait_for(lambda: job_state(job_id) == "RUNNING",
                       timeout_s=240) if job_id else False

    # TERM, not KILL: this row is about an operator restarting the controller,
    # and `systemctl restart slurmctld` sends TERM.  The difference is not
    # cosmetic and it was measured here -- see the note under M7 in
    # fleet/slurm/smoke/README.md for what a KILLed controller does to a job
    # that started since its last state save.
    dexec("dl380g10", ["bash", "-c", "pkill -TERM slurmctld; exit 0"], user="root")
    down = wait_for(
        lambda: ctl(["scontrol", "ping"]).returncode != 0, timeout_s=30)
    # Long enough to outlast SLURM's own client-side retry, which is what
    # makes this a row rather than a formality.  Measured here on 25.11.2: a
    # `scontrol` issued while the controller is down blocks about forty
    # seconds inside the client library and then answers, so an outage of
    # twenty seconds is invisible to the lane and proves nothing.  At sixty a
    # poll really does come back with nothing, which is the case the lane has
    # to tell apart from a job the controller has forgotten.
    time.sleep(60)
    ddetach("dl380g10", ["/usr/local/bin/pb-start-slurmctld"])
    back = wait_for(
        lambda: ctl(["scontrol", "ping"]).returncode == 0, timeout_s=120)

    try:
        rest = process.communicate(timeout=600)[1]
    except subprocess.TimeoutExpired:
        process.kill()
        rest = ""
    said = (first or "") + (rest or "")
    path, ending = outcome("done", prefix)
    detail = ending.get("detail") or {}
    checks = {
        "the job was running when the controller went": running,
        "the controller was really down": down,
        "the controller came back": back,
        "pbrun said it was waiting rather than reporting an ending":
            "still waiting, the job is not affected" in said,
        "and said when the scheduler answered again":
            "answers again after" in said,
        "pbrun rc==0": process.returncode == 0,
        "done record": path is not None,
        "receipt_published": detail.get("receipt_published") is True,
    }
    failed = [key for key, ok in checks.items() if not ok]
    record(
        "M7 the controller restarts under a running job and pbrun still gets it",
        not failed,
        f"job={job_id} down={down} back={back} pbrun rc={process.returncode} "
        f"status={ending.get('status')!r}"
        + ("" if not failed else
           f" MISSING {failed}; said={said[-800:]!r}"),
    )


def row_m8_cas_hit(cpu_nonce: str, gpu_nonce: str) -> None:
    """The same two actions again: routed the same way, executed neither time."""

    host_cpu = VOL / Path(cpu_nonce).relative_to(INSIDE)
    host_gpu = VOL / Path(gpu_nonce).relative_to(INSIDE)
    before = (lines(host_cpu), lines(host_gpu))
    again_cpu = pbrun("dl380g10", ["./action.sh", "run", cpu_nonce])
    again_gpu = pbrun("dl380g10", ["./action.sh", "run", gpu_nonce],
                      extra=["--gpu"])
    after = (lines(host_cpu), lines(host_gpu))
    cpu_prefix, cpu_job = submitted(again_cpu)
    gpu_prefix, gpu_job = submitted(again_gpu)
    said = "".join((completed.stdout or "") + (completed.stderr or "")
                   for completed in (again_cpu, again_gpu))
    checks = {
        "both exited 0": again_cpu.returncode == 0 and again_gpu.returncode == 0,
        "neither action ran again": after == before,
        "the worker reported a cache hit": said.count("cache_hit") >= 2,
        "the CPU one was still routed to the cpu partition":
            submission(cpu_prefix).get("partition") == "cpu",
        "the GPU one was still routed to the gpu partition":
            submission(gpu_prefix).get("partition") == "gpu",
    }
    failed = [key for key, ok in checks.items() if not ok]
    record(
        "M8 repeating M2 and M3 re-executes neither, and routes both the same",
        not failed,
        # A repeat still costs a job: the CAS lookup happens in the worker on
        # the node that won the allocation, not in `pbrun` before it submits,
        # so the second submission is a real job that finds a receipt and
        # publishes nothing.  That is the lane's design, and the row says so
        # rather than asserting a short circuit that does not exist.
        f"jobs {cpu_job},{gpu_job} (a repeat still costs a job id; the CAS "
        f"lookup is on the node) nonce lines {before}->{after}"
        + ("" if not failed else f" MISSING {failed}; said={said[-600:]!r}"),
    )


def main() -> int:
    for directory in (QUEUE, LANE, LANE / "jobs", WORK):
        directory.mkdir(parents=True, exist_ok=True)
    build_source_repo()

    nonces = {
        name: str(INSIDE / f"nonce-{name}.txt")
        for name in ("m2", "m3", "m4", "m5", "m6", "m7", "m9", "m10", "m11")
    }
    for value in nonces.values():
        (VOL / Path(value).name).write_text("")

    # First, on an otherwise idle fleet: verify.sh row 8 reads the lane's
    # jobs/ directory for orphans, and M6 deliberately leaves one behind by
    # killing a node before its Epilog can run.
    row_m0_verify_script()
    row_m1_three_nodes()
    row_m2_cpu_only(nonces["m2"])
    row_m3_gpu(nonces["m3"])
    row_m4_tagged(nonces["m4"])
    row_m9_class_tag(nonces["m9"])
    row_m10_anywhere_idle(nonces["m10"])
    row_m11_anywhere_overflow(nonces["m11"])
    row_m5_cross_box(nonces["m5"])
    row_m6_node_fail(nonces["m6"])
    row_m7_controller_restart(nonces["m7"])
    row_m8_cas_hit(nonces["m2"], nonces["m3"])

    width = max(len(name) for name, _, _ in results)
    print("\n" + "=" * (width + 60))
    print("PrismaBuild SLURM smoke, three nodes")
    print("=" * (width + 60))
    for name, verdict, detail in results:
        print(f"{verdict:4}  {name.ljust(width)}  {detail}")
    print("=" * (width + 60))
    failures = [name for name, verdict, _ in results if verdict != "PASS"]
    print(f"{len(results) - len(failures)}/{len(results)} rows passed")
    if failures:
        print(f"failing rows: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
