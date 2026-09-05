#!/usr/bin/python3
"""The smoke's rows: what a real controller has to do for this lane to work.

Each row is one named claim with one verdict, and the table at the end is the
whole result.  Runs as an unprivileged user inside the container, after
``inside.sh`` has a controller and a node up; stdlib only, because it runs on
whatever Python the image happens to have.

The rows are ordered so that each one's evidence is available to the next:
row 5 leaves the killed job whose Epilog row 8 reads, and row 2 leaves the
receipt row 3 must not re-earn.
"""
from __future__ import annotations

import getpass
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

REPO = Path(os.environ.get("PB_SMOKE_REPO", "/repo"))
VOL = Path(os.environ.get("PB_SMOKE_VOL", "/mnt/shared"))
NODE = os.environ.get("PB_SMOKE_NODE") or os.uname().nodename.split(".")[0]
# The unprivileged user the jobs run as, which is what SLURM_JOB_USER is
# inside the Epilog -- row 8 reads it back out of slurmd.log.
USER = getpass.getuser()

SH = VOL / "prismabuild-fleet"
QUEUE = SH / "pb-queue"
LANE = SH / "slurm"
WORK = VOL / "work"
SRC = WORK / "src"
PBRUN = REPO / "tools" / "fleet" / "pbrun.py"

#: Job submission through this lane costs a git materialization and a poll
#: interval, so a row that expects an ending waits minutes, not seconds.
WAIT_S = 300.0

results: list[tuple[str, str, str]] = []
_submitted_re = re.compile(r"submitted ([0-9a-f]{12}) as slurm job (\d+)")


def record(name: str, ok: bool, detail: str = "") -> bool:
    results.append((name, "PASS" if ok else "FAIL", detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}  {detail}", flush=True)
    return ok


def sh(argv: list[str], *, timeout: float = 600.0,
       env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    merged = dict(os.environ)
    merged.update(env or {})
    return subprocess.run(
        argv, capture_output=True, text=True, timeout=timeout, env=merged
    )


def pbrun(command: list[str], *, extra: list[str] = [],
          timeout: float = 600.0) -> subprocess.CompletedProcess:
    argv = [
        sys.executable, str(PBRUN),
        "--transport", "slurm",
        "--cwd", str(SRC),
        "--wait-s", str(WAIT_S),
        *extra,
        "--", *command,
    ]
    return sh(argv, timeout=timeout)


def submitted(completed: subprocess.CompletedProcess) -> tuple[str, str]:
    """The key prefix and job id ``pbrun`` announced, or two empty strings."""

    match = _submitted_re.search(completed.stderr or "")
    return (match.group(1), match.group(2)) if match else ("", "")


def outcome(state: str, prefix: str) -> tuple[Path | None, dict]:
    """The terminal record filed under ``done/`` or ``failed/`` for a prefix."""

    directory = QUEUE / state
    if not directory.is_dir():
        return None, {}
    for path in sorted(directory.glob(f"{prefix}*.json")):
        try:
            return path, json.loads(path.read_text())
        except (OSError, ValueError):
            return path, {}
    return None, {}


def wait_for(predicate, *, timeout_s: float, poll_s: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(poll_s)
    return bool(predicate())


def build_source_repo() -> None:
    """A git checkout for the actions to be snapshotted from.

    Small on purpose: the lane's contract is that the *sealed snapshot* is
    reconstructed on the node that won the allocation, and one file proves that
    as well as a hundred thousand do.
    """

    if SRC.exists():
        shutil.rmtree(SRC)
    SRC.mkdir(parents=True)
    (SRC / "action.sh").write_text(
        "#!/bin/bash\n"
        "# The smoke's action.  Every row's command is a mode of this script,\n"
        "# so that what differs between rows is the scheduler's behaviour.\n"
        "set -u\n"
        'mode="${1:-run}"\n'
        'nonce="${2:-}"\n'
        'echo \"action: mode=$mode host=$(hostname -s) job=${SLURM_JOB_ID:-none}\"\n'
        'if [ -n \"$nonce\" ]; then echo \"ran $(date +%s.%N)\" >>\"$nonce\"; fi\n'
        'case \"$mode\" in\n'
        '    run)   echo \"action: done\" ;;\n'
        '    fail)  echo \"action: failing on purpose\" >&2; exit 7 ;;\n'
        '    sleep) sleep \"${3:-300}\" ;;\n'
        'esac\n'
        "exit 0\n",
        encoding="utf-8",
    )
    os.chmod(SRC / "action.sh", 0o755)
    for argv in (
        ["git", "-C", str(SRC), "init", "-q"],
        ["git", "-C", str(SRC), "config", "user.name", "PrismaBuild smoke"],
        ["git", "-C", str(SRC), "config", "user.email", "smoke@example.invalid"],
        ["git", "-C", str(SRC), "add", "action.sh"],
        ["git", "-C", str(SRC), "commit", "-qm", "smoke action"],
    ):
        completed = sh(argv)
        if completed.returncode != 0:
            raise SystemExit(f"smoke: {argv} failed: {completed.stderr}")


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------

def row_1_node_and_wrap() -> None:
    # One line, not one per partition: the node is in three partitions here and
    # a row's verdict has to fit on a row.
    info = sh(["sinfo", "-h", "-n", NODE, "-p", "all", "-o", "%T|%G|%f"])
    text = (info.stdout or "").strip().splitlines()[0] if info.stdout.strip() else ""
    idle = text.startswith("idle") and "shard:2" in text
    record("1a sinfo: node idle with shard:2", idle, text or info.stderr.strip())

    # --chdir and --output explicitly: a batch job inherits the submitter's
    # working directory, and slurmd cannot open `slurm-%j.out` in a directory
    # the job's user may not write.  That is a property of this container, not
    # of the lane, which passes both flags itself.
    wrapped = sh([
        "sbatch", "--wait", f"--chdir={WORK}", f"--output={WORK}/wrap-%j.out",
        "--wrap=hostname",
    ], timeout=180)
    record(
        "1b sbatch --wait --wrap=hostname exits 0",
        wrapped.returncode == 0,
        f"rc={wrapped.returncode} {(wrapped.stderr or '').strip()[:120]}",
    )


def row_2_end_to_end(nonce: Path) -> tuple[str, str]:
    completed = pbrun(["bash", "action.sh", "run", str(nonce)])
    prefix, job_id = submitted(completed)
    path, rec = outcome("done", prefix) if prefix else (None, {})
    detail = rec.get("detail", {}) if isinstance(rec, dict) else {}
    slurm = detail.get("slurm", {}) if isinstance(detail, dict) else {}
    checks = {
        "pbrun rc==0": completed.returncode == 0,
        "done record": path is not None,
        "status=executed": rec.get("status") == "executed",
        "receipt_published": detail.get("receipt_published") is True,
        "slurm.job_id matches": str(slurm.get("job_id")) == job_id,
        f"claimed_host=={NODE}": rec.get("claimed_host") == NODE,
    }
    failed = [name for name, ok in checks.items() if not ok]
    record(
        "2 pbrun --transport slurm executes and files done/",
        not failed,
        f"job={job_id} " + (
            f"missing: {failed}; rc={completed.returncode}; "
            f"stderr={(completed.stderr or '')[-400:]}" if failed else "all fields"
        ),
    )
    return prefix, job_id


def row_3_cas_hit(nonce: Path, first_job: str) -> None:
    before = nonce.read_text().count("\n") if nonce.exists() else 0
    completed = pbrun(["bash", "action.sh", "run", str(nonce)])
    prefix, job_id = submitted(completed)
    after = nonce.read_text().count("\n") if nonce.exists() else 0
    _, rec = outcome("done", prefix) if prefix else (None, {})
    hit = "cache_hit" in (completed.stdout or "") + (completed.stderr or "")
    ok = (
        completed.returncode == 0
        and after == before
        and rec.get("status") == "executed"
    )
    record(
        "3 a repeat submission re-runs nothing (CAS hit)",
        ok,
        f"job={job_id} nonce lines {before}->{after} "
        f"worker said cache_hit={hit} status={rec.get('status')}"
        + ("" if ok else f" rc={completed.returncode} "
           f"stderr={(completed.stderr or '')[-800:]!r}"),
    )


def row_4_failure() -> None:
    completed = pbrun(["bash", "action.sh", "fail"])
    prefix, job_id = submitted(completed)
    path, rec = outcome("failed", prefix) if prefix else (None, {})
    detail = rec.get("detail", {}) if isinstance(rec, dict) else {}
    stderr_tail = str(detail.get("stderr") or "")
    ok = (
        path is not None
        and rec.get("status") == "failed"
        and isinstance(detail.get("returncode"), int)
        and detail.get("returncode") != 0
        and "failing on purpose" in stderr_tail + str(detail.get("stdout") or "")
    )
    record(
        "4 a failing command files failed/ with rc and stderr",
        ok,
        f"job={job_id} rc={detail.get('returncode')} "
        f"state={detail.get('slurm', {}).get('state')} "
        f"tail={'yes' if stderr_tail else 'no'}",
    )


def row_5_timeout() -> tuple[str, str]:
    started = time.monotonic()
    completed = pbrun(
        ["bash", "action.sh", "sleep", "", "300"],
        extra=["--timeout-s", "10"],
        timeout=600,
    )
    elapsed = time.monotonic() - started
    prefix, job_id = submitted(completed)
    path, rec = outcome("failed", prefix) if prefix else (None, {})
    detail = rec.get("detail", {}) if isinstance(rec, dict) else {}
    slurm = detail.get("slurm", {}) if isinstance(detail, dict) else {}
    limit = ""
    latest = LANE / (prefix and next(
        (d.name for d in LANE.iterdir() if d.name.startswith(prefix)), ""
    ) or "") / "latest.json"
    try:
        limit = json.loads(latest.read_text()).get("time_limit", "")
    except (OSError, ValueError):
        pass
    # "killed by SLURM, not by pbrun" is an assertion about what pbrun said,
    # not only about the record: SLURM reports a job it killed at the limit as
    # ExitCode=0:15, and reading the code alone made pbrun announce that the
    # job "exited 0 but published no receipt".
    said = "failed (TIMEOUT)" in (completed.stderr or "")
    ok = (
        path is not None
        and rec.get("status") == "failed"
        and slurm.get("state") == "TIMEOUT"
        and detail.get("signal") == 15
        and limit == "00:01:00"
        and said
        and elapsed < 300
    )
    record(
        "5 --timeout-s becomes --time and SLURM kills the job",
        ok,
        f"job={job_id} --time={limit} state={slurm.get('state')} "
        f"rc={detail.get('returncode')} signal={detail.get('signal')} "
        f"after {elapsed:.0f}s status={rec.get('status')} "
        f"pbrun said failed(TIMEOUT)={said}",
    )
    return prefix, job_id


def row_6_withdraw() -> None:
    # `600`, not row 5's `300`: an action key is a content hash of the whole
    # submission, so an identical command is the *same action*, and submitting
    # it here would have withdrawn row 5's key instead of a fresh one.
    process = subprocess.Popen(
        [
            sys.executable, str(PBRUN), "--transport", "slurm",
            "--cwd", str(SRC), "--wait-s", str(WAIT_S),
            "--", "bash", "action.sh", "sleep", "", "600",
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    prefix = job_id = ""
    deadline = time.monotonic() + 180
    line = ""
    while time.monotonic() < deadline:
        line = process.stderr.readline()
        if not line:
            break
        match = _submitted_re.search(line)
        if match:
            prefix, job_id = match.group(1), match.group(2)
            break
    running = wait_for(
        lambda: (sh(["squeue", "-h", "-j", job_id, "-o", "%T"]).stdout or
                 "").strip() == "RUNNING",
        timeout_s=180,
    ) if job_id else False
    withdrawn = sh([
        sys.executable, str(PBRUN), "--transport", "slurm",
        "--withdraw", prefix, "--reason", "smoke row 6",
    ]) if prefix else None
    try:
        rest = process.communicate(timeout=300)[1]
    except subprocess.TimeoutExpired:
        process.kill()
        rest = ""
    marker = QUEUE / "withdrawn" / f"{prefix}"
    marker_path = next(
        (p for p in (QUEUE / "withdrawn").glob(f"{prefix}*.json")), None
    ) if prefix else None
    path, rec = outcome("failed", prefix) if prefix else (None, {})
    ok = (
        running
        and withdrawn is not None
        and withdrawn.returncode == 0
        and marker_path is not None
        and path is not None
        and rec.get("status") == "withdrawn"
        and bool(rec.get("withdrawn_by"))
    )
    record(
        "6 --withdraw scancels, files withdrawn/ and a failed/ record",
        ok,
        f"job={job_id} running={running} marker={bool(marker_path)} "
        f"status={rec.get('status')} by={rec.get('withdrawn_by')!r}"
        + ("" if ok else
           f" pbrun rc={process.returncode} first={line.strip()!r} "
           f"rest={(rest or '')[-500:]!r} "
           f"withdraw={(withdrawn.stderr if withdrawn else '')[-300:]!r}"),
    )
    del marker


def row_7_gres_and_constraint() -> None:
    ids = []
    for _ in range(3):
        completed = sh([
            "sbatch", "--parsable", "--gres=shard:1", "--mem=1024",
            "--time=00:02:00", f"--chdir={WORK}",
            f"--output={WORK}/shard-%j.out", "--wrap=sleep 90",
        ])
        ids.append((completed.stdout or "").strip().split(";")[0])

    def snapshot() -> dict[str, str]:
        return {
            job: (sh(["squeue", "-h", "-j", job, "-o", "%T"]).stdout or
                  "").strip() or "GONE"
            for job in ids
        }

    # Waited for, not slept through.  Under 23.11.4 with accounting off, every
    # job sits in `InvalidAccount` until the association refresh fills it in
    # (~30 s) and backfill then starts it; a fixed sleep read three PENDING
    # jobs and called the shard budget broken.  What the row is about is how
    # many *can* run at once, so it waits for the answer to settle.
    states: dict[str, str] = {}

    def two_running() -> bool:
        states.update(snapshot())
        return sum(1 for value in states.values() if value == "RUNNING") == 2

    wait_for(two_running, timeout_s=120, poll_s=2.0)
    running = sum(1 for value in states.values() if value == "RUNNING")
    pending = sum(1 for value in states.values() if value == "PENDING")
    reason = (sh(["squeue", "-h", "-j", ids[2], "-o", "%r"]).stdout or "").strip()
    record(
        "7a shard:2 admits two jobs and holds the third",
        running == 2 and pending == 1,
        f"states={states} third pending for {reason!r}",
    )
    for job in ids:
        sh(["scancel", job])

    refused = pbrun(
        ["bash", "action.sh", "run", ""],
        extra=["--tag", "nonexistent-feature", "--wait-s", "30"],
        timeout=180,
    )
    said = (refused.stderr or "") + (refused.stdout or "")
    record(
        "7b --constraint for an unknown Feature is refused and reported",
        refused.returncode != 0 and "slurm refused this action" in said,
        f"rc={refused.returncode} {said.strip().splitlines()[-1][:120] if said.strip() else ''}",
    )


def row_8_epilog(job_id: str) -> None:
    """The Epilog ran for the job SLURM killed, and matched on the owner label.

    Read off row 5's job, not a fresh one: ``slurm_job.py`` removes its own
    state file when it gets to exit normally, so a job that ended cleanly
    leaves the Epilog nothing to do -- which is the design, and which is why
    the evidence has to come from a job that was killed.
    """

    log = VOL / "docker.log"
    text = log.read_text() if log.exists() else ""
    lines = [line for line in text.splitlines() if line.strip()]
    filtered = [line for line in lines if "label=prismabuild.action=" in line]
    removed = [line for line in lines if line.startswith("rm -f ")]
    state_files = sorted((LANE / "jobs").glob("*.job")) if (LANE / "jobs").is_dir() else []
    try:
        said = [
            line for line in (VOL / "logs" / "slurmd.log").read_text(
                errors="replace").splitlines()
            if "prismabuild-epilog" in line
        ]
    except OSError:
        said = []
    # And the state file went as the job's user, not as root.  Without this
    # the row passes either way: `lane_delete` falls back to root's unlink,
    # which works here because the lane root is a bind mount rather than an
    # NFS export, so the squash-safe path would be untested and look tested.
    as_user = [line for line in said
               if "removed state file" in line and f"as {USER}" in line]
    record(
        "8 the Epilog ran and matched containers by the action's owner label",
        bool(filtered) and bool(removed) and bool(as_user) and not state_files,
        f"docker calls={len(lines)} label-filtered={len(filtered)} "
        f"rm -f={len(removed)} leftover state files={[p.name for p in state_files]} "
        f"state file removed as {USER}={bool(as_user)} "
        f"epilog said {said[-2:]}",
    )
    del job_id


def row_9_no_slurmdbd(job_id: str) -> None:
    """What the lane reads when ``sacct`` has no accounting behind it."""

    started = time.monotonic()
    probe = sh([
        "sacct", "-j", job_id, "--parsable2", "--noheader",
        "-o", "JobID,State,ExitCode,Start,End,Elapsed,NodeList,Partition",
    ], timeout=120)
    sacct_s = time.monotonic() - started
    sacct_answered = probe.returncode == 0 and bool((probe.stdout or "").strip())
    control = sh(["scontrol", "show", "job", job_id])
    control_answers = control.returncode == 0 and "JobState=" in (control.stdout or "")

    path, rec = outcome("failed", job_id[:0] or "") if False else (None, {})
    # The record for row 5's job, found by its job id rather than its key.
    for state in ("done", "failed"):
        directory = QUEUE / state
        if not directory.is_dir():
            continue
        for candidate in directory.glob("*.json"):
            try:
                value = json.loads(candidate.read_text())
            except (OSError, ValueError):
                continue
            if str(value.get("detail", {}).get("slurm", {}).get("job_id")) == job_id:
                path, rec = candidate, value
    provenance_ok = bool(rec.get("claimed_host")) and rec.get("finished_unix")

    ok = control_answers and provenance_ok and sacct_s < 10
    record(
        "9 with no slurmdbd the lane falls back to scontrol for provenance",
        ok,
        f"sacct rc={probe.returncode} in {sacct_s:.1f}s answered={sacct_answered} "
        f"err={(probe.stderr or '').strip().splitlines()[:1]} "
        f"scontrol answers={control_answers} "
        f"record claimed_host={rec.get('claimed_host')!r} "
        f"elapsed_s={rec.get('detail', {}).get('elapsed_s')}",
    )
    del path


# ---------------------------------------------------------------------------
# Rows 10a-10d: what ConstrainCores and ConstrainRAMSpace actually do
#
# Under the pull queue, `pbrun --cpus` and `--demand mem_gb=` were admission
# declarations that nothing enforced.  Under `select/cons_tres` with
# `CR_Core_Memory` they become `--cpus-per-task` and `--mem`, and
# fleet/slurm/cgroup.conf turns those into a cpuset and a `memory.max`.  These
# rows measure the difference rather than argue it, and they are the evidence
# behind docs/resource_enforcement_2026-09-05.md.
#
# The block runs three times, one arm per setting the decision turns on:
#
#   PB_SMOKE_CONSTRAIN_CORES=yes PB_SMOKE_CONSTRAIN_SWAP=no   # the fleet today
#   PB_SMOKE_CONSTRAIN_CORES=no  PB_SMOKE_CONSTRAIN_SWAP=no   # cores unenforced
#   PB_SMOKE_CONSTRAIN_CORES=yes PB_SMOKE_CONSTRAIN_SWAP=yes  # memory that kills
#
# Each row names the arm it ran under, so a transcript says which one it is.
# ---------------------------------------------------------------------------

#: The container's cgroup settings, as ``inside.sh`` generated them.
CONSTRAIN_CORES = os.environ.get("PB_SMOKE_CONSTRAIN_CORES", "yes")
CONSTRAIN_SWAP = os.environ.get("PB_SMOKE_CONSTRAIN_SWAP", "no")
ARM = f"cores={CONSTRAIN_CORES} swap={CONSTRAIN_SWAP}"

#: How much memory row 10c writes past its declaration, and in what chunks.
#: ``bytearray(N)`` will not do: it is a calloc, so the pages are mapped and
#: never written, and a cgroup charges pages that are faulted in.  ``extend``
#: copies, which touches every page.
GROW_CHUNK_MIB = 64
OVER_DECLARED_GB = 1
OVER_GROW_MIB = 3072
UNDER_DECLARED_GB = 2
UNDER_GROW_MIB = 256

_AFFINITY_RE = re.compile(r"current affinity list:\s*(\S+)")
_NPROC_RE = re.compile(r"nproc=(\d+)")


def _affinity_width(text: str) -> int:
    """How many CPUs a ``taskset -cp`` list names, or ``-1`` when it said none.

    ``-cp`` rather than ``-p``: the list form is countable, and a hex mask is
    a second thing to get wrong in a row that is about a number.
    """

    match = _AFFINITY_RE.search(text or "")
    if not match:
        return -1
    total = 0
    for part in match.group(1).split(","):
        if "-" in part:
            first, last = part.split("-", 1)
            total += int(last) - int(first) + 1
        else:
            total += 1
    return total


def _grower(megabytes: int, *, report: bool = False) -> list[str]:
    """A command that writes ``megabytes`` MiB and, optionally, reports its cgroup.

    The report is what makes row 10c readable in every arm.  A job the kernel
    kills says nothing itself, so the state is the evidence; a job that
    survives has to say whether it survived unconstrained or by reclaiming,
    and only its own ``memory.events`` can answer that.
    """

    lines = [
        "b = bytearray()",
        f"for _ in range({megabytes // GROW_CHUNK_MIB}):",
        f"    b.extend(b'x' * ({GROW_CHUNK_MIB} << 20))",
        "print('grew', len(b) >> 20, 'MiB', flush=True)",
    ]
    if report:
        # The whole chain, not the leaf.  A cgroup v2 limit is hierarchical:
        # slurmstepd sets `memory.max` on the job's cgroup and the process
        # runs two levels below it, where `memory.max` reads `max` and
        # `memory.events` counts nothing.  Reading only the leaf reported the
        # constraint absent while it was being enforced one level up.
        lines += [
            "import pathlib",
            "rel = open('/proc/self/cgroup').read().strip().rsplit(':', 1)[-1]",
            "root = pathlib.Path('/sys/fs/cgroup')",
            "node = root / rel.lstrip('/')",
            "while True:",
            "    def read(name, node=node):",
            "        try:",
            "            return ' '.join((node / name).read_text().split())",
            "        except OSError:",
            "            return '-'",
            "    print('cgroup', node, 'memory.max=' + read('memory.max'),",
            "          'memory.swap.max=' + read('memory.swap.max'),",
            "          'memory.current=' + read('memory.current'),",
            "          'memory.swap.current=' + read('memory.swap.current'),",
            "          'memory.events=[' + read('memory.events') + ']',",
            "          flush=True)",
            "    if node == root:",
            "        break",
            "    node = node.parent",
        ]
    return ["python3", "-c", "\n".join(lines) + "\n"]


def _ending(prefix: str) -> tuple[str, dict]:
    """The terminal record filed for a key prefix, whichever directory it is in."""

    for state in ("done", "failed"):
        path, record = outcome(state, prefix)
        if path is not None:
            return state, record
    return "", {}


_CGROUP_RE = re.compile(
    r"^cgroup (?P<path>\S+) memory\.max=(?P<limit>\S+) "
    r"memory\.swap\.max=(?P<swap_limit>\S+) "
    r"memory\.current=(?P<current>\S+) "
    r"memory\.swap\.current=(?P<swap_current>\S+) "
    r"memory\.events=\[(?P<events>[^\]]*)\]$",
    re.M,
)


def _binding_cgroup(text: str) -> dict[str, str]:
    """The nearest ancestor cgroup that names a numeric ``memory.max``.

    A cgroup v2 limit binds the whole subtree, so the level that carries the
    number is the one that decides the job's fate -- not the leaf the process
    happens to be in, which reads ``max`` and counts no events.
    """

    for match in _CGROUP_RE.finditer(text or ""):
        if match.group("limit").isdigit():
            return match.groupdict()
    return {}


def row_10_cpu_containment() -> None:
    """What a job sees of the node's CPUs when it declares one, and when two.

    Read off ``taskset``, not ``nproc``.  ``nproc`` honours ``OMP_NUM_THREADS``
    before it looks at the affinity mask, and ``pbrun``'s sealed environment
    sets that to 4 (``tools/fleet/pbrun.py:1936``), so ``nproc`` answers 4
    under every declaration.  It is reported anyway, because an action that
    sizes its own parallelism from ``nproc`` is reading that 4.
    """

    node_cpus = int((sh(["nproc"]).stdout or "0").strip() or 0)
    for label, declared in (("10a", 1), ("10b", 2)):
        completed = pbrun(
            ["bash", "-c", "echo nproc=$(nproc); taskset -cp $$"],
            extra=["--cpus", str(declared)],
        )
        text = (completed.stdout or "") + (completed.stderr or "")
        match = _NPROC_RE.search(text)
        seen = int(match.group(1)) if match else -1
        width = _affinity_width(text)
        if CONSTRAIN_CORES == "yes":
            # The declaration is a cpuset: the job is confined to exactly what
            # it asked for, on a node that had more to give.
            ok = width == declared and node_cpus > declared
        else:
            # The declaration is an admission count only: the job is placed
            # against it and then sees the whole node.
            ok = width == node_cpus
        record(
            f"{label} --cpus {declared} confines the job [{ARM}]",
            ok,
            f"job affinity width={width} of the node's {node_cpus} CPUs "
            f"(declared {declared}); nproc said {seen} "
            f"(OMP_NUM_THREADS, not the cpuset)",
        )


def row_10c_over_declared_memory() -> None:
    """A job that writes past its declared memory does not run unconstrained.

    The claim is deliberately not ``state == OUT_OF_MEMORY``: what the
    decision turns on is whether the constraint reaches the job at all, and
    the state the controller picks is quoted rather than assumed.  With
    ``ConstrainSwapSpace=no`` -- the fleet's setting -- a job over
    ``memory.max`` reclaims into swap and lives; with it on, the kernel kills
    it.  Both are the constraint working, and the row passes on either, so the
    arms differ in what they report rather than in whether they pass.
    """

    completed = pbrun(
        _grower(OVER_GROW_MIB, report=True),
        extra=["--demand", f"mem_gb={OVER_DECLARED_GB}"],
    )
    prefix, job_id = submitted(completed)
    where, rec = _ending(prefix) if prefix else ("", {})
    detail = rec.get("detail", {}) if isinstance(rec, dict) else {}
    slurm = detail.get("slurm", {}) if isinstance(detail, dict) else {}
    state = str(slurm.get("state") or "")
    text = (completed.stdout or "") + str(detail.get("stdout") or "")
    binding = _binding_cgroup(text)
    limit = binding.get("limit", "")
    events = binding.get("events", "")
    hit = re.search(r"\bmax (\d+)", events)
    killed = state not in ("", "COMPLETED")
    throttled = bool(hit) and int(hit.group(1)) > 0
    declared_bytes = OVER_DECLARED_GB * 1024 ** 3
    said = f"pbrun: failed ({state})" in (completed.stderr or "")
    ok = (killed and said) or (
        throttled and limit.isdigit() and int(limit) == declared_bytes
    )
    record(
        f"10c a job over its mem_gb is constrained, not ignored [{ARM}]",
        ok,
        f"job={job_id} declared mem_gb={OVER_DECLARED_GB}, wrote "
        f"{OVER_GROW_MIB} MiB -> filed {where or 'nothing'}/ state={state!r} "
        f"rc={detail.get('returncode')} signal={detail.get('signal')} "
        f"pbrun said failed({state})={said}; binding cgroup "
        f"{binding.get('path', '(none reported)')} "
        f"memory.max={limit or '-'} "
        f"(declared {declared_bytes}) "
        f"memory.swap.max={binding.get('swap_limit', '-')} "
        f"memory.swap.current={binding.get('swap_current', '-')} "
        f"memory.events=[{events}]",
    )


def row_10d_within_declared_memory() -> None:
    """And a job that stays under its declaration is untouched by the limit."""

    completed = pbrun(
        _grower(UNDER_GROW_MIB),
        extra=["--demand", f"mem_gb={UNDER_DECLARED_GB}"],
    )
    prefix, job_id = submitted(completed)
    where, rec = _ending(prefix) if prefix else ("", {})
    detail = rec.get("detail", {}) if isinstance(rec, dict) else {}
    slurm = detail.get("slurm", {}) if isinstance(detail, dict) else {}
    ok = (
        completed.returncode == 0
        and where == "done"
        and rec.get("status") == "executed"
        and f"grew {UNDER_GROW_MIB} MiB" in (completed.stdout or "")
    )
    record(
        f"10d a job within its mem_gb completes [{ARM}]",
        ok,
        f"job={job_id} declared mem_gb={UNDER_DECLARED_GB}, wrote "
        f"{UNDER_GROW_MIB} MiB -> filed {where or 'nothing'}/ "
        f"status={rec.get('status')} state={slurm.get('state')} "
        f"rc={completed.returncode}",
    )


def main() -> int:
    for argv in (
        ["git", "config", "--global", "user.name", "PrismaBuild smoke"],
        ["git", "config", "--global", "user.email", "smoke@example.invalid"],
        ["git", "config", "--global", "--add", "safe.directory", "*"],
    ):
        sh(argv)
    QUEUE.mkdir(parents=True, exist_ok=True)
    LANE.mkdir(parents=True, exist_ok=True)
    WORK.mkdir(parents=True, exist_ok=True)
    build_source_repo()

    nonce = VOL / "nonce.txt"
    nonce.write_text("")

    row_1_node_and_wrap()
    prefix, first_job = row_2_end_to_end(nonce)
    row_3_cas_hit(nonce, first_job)
    row_4_failure()
    timeout_prefix, timeout_job = row_5_timeout()
    row_6_withdraw()
    row_7_gres_and_constraint()
    row_8_epilog(timeout_job)
    row_9_no_slurmdbd(timeout_job or first_job)
    row_10_cpu_containment()
    row_10c_over_declared_memory()
    row_10d_within_declared_memory()
    del prefix, timeout_prefix

    width = max(len(name) for name, _, _ in results)
    print("\n" + "=" * (width + 60))
    print("PrismaBuild SLURM smoke")
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
