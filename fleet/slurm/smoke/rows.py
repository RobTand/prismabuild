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
#: Where the jobs leave their Epilog state files.  A node-side path, resolved
#: by `slurm_job.py` and `epilog.sh` from `PRISMABUILD_SLURM_JOB_STATE_ROOT` or
#: this same default, and deliberately not derived from the lane root above --
#: a submitter may move that one, and moving it must not move this.
JOB_STATE_ROOT = Path(
    os.environ.get("PRISMABUILD_SLURM_JOB_STATE_ROOT")
    or "/mnt/shared/prismabuild-fleet/slurm/jobs")
WORK = VOL / "work"
SRC = WORK / "src"
PBRUN = REPO / "tools" / "fleet" / "pbrun.py"
PBCAMPAIGN = REPO / "tools" / "fleet" / "pbcampaign.py"

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


def campaign(manifest: Path, *, extra: list[str] = [],
             timeout: float = 900.0) -> subprocess.CompletedProcess:
    argv = [
        sys.executable, str(PBCAMPAIGN),
        "--transport", "slurm",
        "--wait-s", str(WAIT_S),
        *extra,
        str(manifest),
    ]
    return sh(argv, timeout=timeout)


def table_rows(completed: subprocess.CompletedProcess) -> list[dict[str, str]]:
    """The campaign's table, as one dict per row keyed by its column name."""

    lines = [line for line in (completed.stdout or "").splitlines() if line.strip()]
    if not lines:
        return []
    header = lines[0].split()
    if header[:2] != ["key", "status"]:
        return []
    return [dict(zip(header, line.split())) for line in lines[1:]]


def lane_latest(prefix: str) -> dict:
    """The newest submission recorded for a key, found by its printed prefix."""

    if not prefix or not LANE.is_dir():
        return {}
    for directory in sorted(LANE.iterdir()):
        if directory.is_dir() and directory.name.startswith(prefix):
            try:
                return json.loads((directory / "latest.json").read_text())
            except (OSError, ValueError):
                return {}
    return {}


def lane_submissions() -> int:
    """Every submission record the lane holds, across every action."""

    return len(list(LANE.glob("*/submissions/*.json"))) if LANE.is_dir() else 0


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


def row_3_cas_hit(nonce: Path, first_job: str, prefix: str) -> None:
    """The same action again costs no job: pbrun reads the receipt first.

    Before this row asserted a short circuit, a repeat was a real job that
    materialized the checkout and found the receipt on the node; the row then
    said "the worker reported a cache hit".  Now the CAS answers before
    ``sbatch``, and the run's own ``done/`` record is left as it was.
    """

    before = nonce.read_text().count("\n") if nonce.exists() else 0
    submissions_before = lane_submissions()
    _, rec_before = outcome("done", prefix)
    completed = pbrun(["bash", "action.sh", "run", str(nonce)])
    _, job_id = submitted(completed)
    after = nonce.read_text().count("\n") if nonce.exists() else 0
    _, rec_after = outcome("done", prefix)
    said = (completed.stdout or "") + (completed.stderr or "")
    checks = {
        "pbrun rc==0": completed.returncode == 0,
        "no job announced": job_id == "",
        "no new lane submission": lane_submissions() == submissions_before,
        "the action did not run again": after == before,
        "pbrun said the CAS answered":
            "already in the CAS" in said and "cache_hit" in said,
        "done/ still holds the run's own record":
            rec_after == rec_before and rec_after.get("status") == "executed",
    }
    failed = [name for name, ok in checks.items() if not ok]
    record(
        "3 a repeat submission submits nothing: the CAS answers before sbatch",
        not failed,
        f"first job={first_job}, now none; nonce lines {before}->{after}; "
        f"lane submissions {submissions_before}->{lane_submissions()}; "
        f"done/ status={rec_after.get('status')}"
        + ("" if not failed else f" MISSING {failed}; rc={completed.returncode} "
           f"stderr={(completed.stderr or '')[-800:]!r}"),
    )


def row_4_failure() -> None:
    completed = pbrun(["bash", "action.sh", "fail"])
    prefix, job_id = submitted(completed)
    path, rec = outcome("failed", prefix) if prefix else (None, {})
    detail = rec.get("detail", {}) if isinstance(rec, dict) else {}
    stderr_tail = str(detail.get("stderr") or "")
    # `action.sh fail` exits 7 and the launcher exits 1, so the two numbers
    # are different on purpose: `returncode` is the launcher's, which every
    # fleet reader means by it, and `action_returncode` is the action's, which
    # used to survive only as prose in the stderr tail.
    ok = (
        path is not None
        and rec.get("status") == "failed"
        and isinstance(detail.get("returncode"), int)
        and detail.get("returncode") != 0
        and detail.get("action_returncode") == 7
        and "failing on purpose" in stderr_tail + str(detail.get("stdout") or "")
    )
    record(
        "4 a failing command files failed/ with both rcs and stderr",
        ok,
        f"job={job_id} rc={detail.get('returncode')} "
        f"action_rc={detail.get('action_returncode')} "
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
    # The pool's rule: a withdrawal lands in withdrawn/ and never in failed/.
    # The marker pbrun --withdraw filed is enriched with the job's ending in
    # place, so one record carries the decision and the detail.
    path, rec = outcome("withdrawn", prefix) if prefix else (None, {})
    failed_path, _ = outcome("failed", prefix) if prefix else (None, {})
    ok = (
        running
        and withdrawn is not None
        and withdrawn.returncode == 0
        and marker_path is not None
        and path is not None
        and failed_path is None
        and rec.get("status") == "withdrawn"
        and bool(rec.get("withdrawn_by"))
        and isinstance(rec.get("detail"), dict)
        and (rec.get("detail") or {}).get("slurm", {}).get("job_id") == job_id
    )
    record(
        "6 --withdraw scancels and files one withdrawn/ record, nothing in failed/",
        ok,
        f"job={job_id} running={running} marker={bool(marker_path)} "
        f"failed_record={failed_path is not None} "
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


def _leftover_state_files() -> list[Path]:
    """State files for jobs the Epilog has not cleaned up after yet."""

    if not JOB_STATE_ROOT.is_dir():
        return []
    return sorted(JOB_STATE_ROOT.glob("*.job"))


def row_8_epilog(job_id: str) -> None:
    """The Epilog ran, matched on the owner label, and left nothing behind.

    Read off row 5's killed job, which is where the container evidence is: a
    job that reaches its own exit still has containers to remove -- they are
    reparented to containerd-shim and outlive it either way -- but a killed one
    is the case that has nothing else to fall back on.

    The state files are a settled reading, not an instant one.  The job runner
    no longer deletes its own: the Epilog owns node-side cleanup on every
    ending, and it runs after the job's processes are gone, which is after
    ``pbrun`` and ``scancel`` have already returned.  So a file for a job that
    ended a moment ago is not yet a leak.
    """

    log = VOL / "docker.log"
    text = log.read_text() if log.exists() else ""
    lines = [line for line in text.splitlines() if line.strip()]
    filtered = [line for line in lines if "label=prismabuild.action=" in line]
    removed = [line for line in lines if line.startswith("rm -f ")]
    def settled() -> bool:
        return not _leftover_state_files()

    wait_for(settled, timeout_s=60, poll_s=2.0)
    state_files = _leftover_state_files()
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


def row_13_liveness() -> None:
    """A job that sleeps is reported as stalled, and left to finish itself.

    Three claims in one row, because they only mean something together:
    ``sstat`` answers on this configuration (``jobacct_gather/cgroup``, no
    ``slurmdbd``) and its format names are the ones the lane asks for; a job
    that does nothing for the window is reported by ``pbrun`` on stderr; and
    that job completes on its own, with ``status=executed`` and the samples on
    the record -- nothing cancelled it.
    """

    helpformat = sh(["sstat", "--helpformat"])
    names = set((helpformat.stdout or "").split())
    wanted = [
        "JobID", "AveCPU", "MinCPU", "MaxRSS", "MaxDiskRead", "MaxDiskWrite",
        "NTasks", "TRESUsageInTot",
    ]
    missing = [name for name in wanted if name not in names]
    config = sh(["scontrol", "show", "config"])
    gather = next(
        (line.strip() for line in (config.stdout or "").splitlines()
         if line.strip().startswith("JobAcctGatherFrequency")), "",
    )

    # 240, not 300 or 600: an action key is a content hash, and rows 5 and 6
    # already own those two sleeps.  Long enough for the lane's 120 s window
    # to elapse after the materialization's own CPU stops counting as
    # progress, and for one report to be printed before the job ends.
    process = subprocess.Popen(
        [
            sys.executable, str(PBRUN), "--transport", "slurm",
            "--cwd", str(SRC), "--wait-s", "900",
            "--", "bash", "action.sh", "sleep", "", "240",
        ],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    prefix = job_id = ""
    first = ""
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        first = process.stderr.readline()
        if not first:
            break
        match = _submitted_re.search(first)
        if match:
            prefix, job_id = match.group(1), match.group(2)
            break
    running = wait_for(
        lambda: (sh(["squeue", "-h", "-j", job_id, "-o", "%T"]).stdout or
                 "").strip() == "RUNNING",
        timeout_s=180,
    ) if job_id else False
    # A raw sstat answer while the job runs, quoted so the README can say what
    # the lane parses on a real controller rather than on a fake.
    time.sleep(45)
    probe = sh([
        "sstat", "-j", job_id, "-a", "-P", "-n", "--noconvert",
        "--format=JobID,AveCPU,MinCPU,MaxRSS,MaxDiskRead,MaxDiskWrite,NTasks,"
        "TRESUsageInTot",
    ]) if job_id else None
    sstat_line = (probe.stdout or "").strip().replace("\n", " ; ") if probe else ""
    sstat_ok = probe is not None and probe.returncode == 0 and bool(sstat_line)
    try:
        rest = process.communicate(timeout=600)[1]
    except subprocess.TimeoutExpired:
        process.kill()
        rest = ""
    said = [line for line in (first + rest).splitlines()
            if "has shown no progress for" in line]
    path, rec = outcome("done", prefix) if prefix else (None, {})
    detail = rec.get("detail", {}) if isinstance(rec, dict) else {}
    liveness = detail.get("liveness", {}) if isinstance(detail, dict) else {}
    lane_dir = next(
        (d for d in LANE.iterdir() if prefix and d.name.startswith(prefix)), None
    )
    samples = []
    if lane_dir is not None and (lane_dir / "liveness.jsonl").exists():
        samples = [
            line for line in (lane_dir / "liveness.jsonl").read_text().splitlines()
            if line.strip()
        ]
    latest = liveness.get("latest") or {}
    ok = (
        not missing
        and sstat_ok
        and running
        and bool(said)
        and "still running" in (said[0] if said else "")
        and process.returncode == 0
        and rec.get("status") == "executed"
        and liveness.get("stalled_since") is not None
        and latest.get("evidence") == ["sstat", "output"]
        and len(samples) >= 2
    )
    record(
        "13 a sleeping job is reported as stalled, not killed, and completes",
        ok,
        f"job={job_id} pbrun rc={process.returncode} status={rec.get('status')} "
        f"reports={len(said)} first={said[0] if said else None!r} "
        f"samples={len(samples)} stalled_since={liveness.get('stalled_since')} "
        f"latest cpu_s={latest.get('cpu_s')} rss={latest.get('rss')} tres_cpu_s={latest.get('tres_cpu_s')} "
        f"sstat_error={latest.get('sstat_error')!r} "
        f"helpformat missing={missing} {gather!r} "
        f"sstat={sstat_line!r}"
        + ("" if ok else f" stderr={(rest or '')[-600:]!r}"),
    )
    del path
def row_10a_campaign(nonces: list[Path]) -> None:
    """One manifest, three rows, mixed demand, one table.

    This is the fan-out claim: an agent submits N actions with one command and
    waits for all of them with one command, and each one is a CAS-memoized
    action sealed by ``pbrun`` itself.  Two rows ask for a shard so the node's
    ``shard:2`` admits them together; the third asks for no GPU at all, which
    is the shape that goes to the CPU partition on the fleet.
    """

    manifest = WORK / "campaign.json"
    manifest.write_text(json.dumps([
        {"argv": ["bash", "action.sh", "run", str(nonces[0])],
         "cwd": str(SRC), "demand": {"gpu": 1, "mem_gb": 2},
         "timeout_s": 600},
        {"argv": ["bash", "action.sh", "run", str(nonces[1])],
         "cwd": str(SRC), "demand": {"gpu": 1, "mem_gb": 2},
         "timeout_s": 600},
        {"argv": ["bash", "action.sh", "run", str(nonces[2])],
         "cwd": str(SRC), "demand": {"cpu": 2, "mem_gb": 2},
         "timeout_s": 600},
    ], indent=1), encoding="utf-8")

    completed = campaign(manifest)
    rows = table_rows(completed)
    for line in (completed.stdout or "").splitlines():
        if line.strip():
            print(f"    | {line}", flush=True)
    jobs = [row.get("job", "") for row in rows]
    checks = {
        "campaign rc==0": completed.returncode == 0,
        "three rows": len(rows) == 3,
        "all executed": all(row.get("status") == "executed" for row in rows),
        "all via slurm": all(row.get("transport") == "slurm" for row in rows),
        "distinct job ids": len(set(jobs)) == 3 and all(j.isdigit() for j in jobs),
        f"all on {NODE}": all(row.get("host") == NODE for row in rows),
        "each ran once": all(
            nonce.exists() and nonce.read_text().count("\n") == 1
            for nonce in nonces
        ),
    }
    failed = [name for name, ok in checks.items() if not ok]
    record(
        "10a a three-row campaign runs on the fleet and reports one table",
        not failed,
        f"jobs={jobs} " + (
            f"missing: {failed}; rc={completed.returncode}; "
            f"stderr={(completed.stderr or '')[-600:]}" if failed else
            f"hosts={[row.get('host') for row in rows]}"
        ),
    )


def row_10b_campaign_rerun(nonces: list[Path]) -> None:
    """The same manifest again: every row a cache hit, and no new job ids.

    A detached submission of work already in the CAS would be a node occupied
    and a checkout materialized to discover what the submitter already knew,
    so the campaign does not submit one.  The lane's own record is the witness:
    the newest submission for each key is still the job that ran it.
    """

    manifest = WORK / "campaign.json"
    lines_before = [nonce.read_text().count("\n") for nonce in nonces]
    submissions_before = lane_submissions()
    completed = campaign(manifest)
    rows = table_rows(completed)
    for line in (completed.stdout or "").splitlines():
        if line.strip():
            print(f"    | {line}", flush=True)
    lines_after = [nonce.read_text().count("\n") for nonce in nonces]
    checks = {
        "campaign rc==0": completed.returncode == 0,
        "three rows": len(rows) == 3,
        "all cache_hit": all(row.get("status") == "cache_hit" for row in rows),
        "no job ids": all(row.get("job") == "-" for row in rows),
        "nothing re-ran": lines_before == lines_after,
        "no new submissions": lane_submissions() == submissions_before,
    }
    failed = [name for name, ok in checks.items() if not ok]
    record(
        "10b the same manifest re-run costs no job at all",
        not failed,
        f"nonce lines {lines_before}->{lines_after} "
        f"lane submissions {submissions_before}->{lane_submissions()} "
        + (f"missing: {failed}; rc={completed.returncode}; "
           f"stderr={(completed.stderr or '')[-600:]}" if failed else
           f"statuses={[row.get('status') for row in rows]}"),
    )


def row_10_scontrol_answers_the_job(nonce_dir: Path) -> None:
    """From inside a batch step, the controller shows the job's constraint and
    the node's Features to the job's owner -- the two facts the worker's
    host-class attestation reads.  Quoted, because the docs said what SLURM
    sets in a job's environment and were wrong once already."""

    out = WORK / "attest-%j.out"
    completed = sh([
        "sbatch", "--wait", "--constraint=gb10", f"--chdir={WORK}",
        f"--output={out}",
        "--wrap=scontrol show job $SLURM_JOB_ID; "
        "scontrol show node $SLURMD_NODENAME; "
        "echo env-constraints=${SLURM_JOB_CONSTRAINTS-unset}",
    ], timeout=180)
    text = ""
    for candidate in sorted(WORK.glob("attest-*.out")):
        text = candidate.read_text(errors="replace")
    features = [line.strip() for line in text.splitlines()
                if "Features=" in line or line.startswith("env-constraints=")]
    job_features = [line for line in features if line.startswith("Features=")
                    or " Features=" in line]
    node_features = [line for line in features if "ActiveFeatures=" in line]
    ok = (
        completed.returncode == 0
        and any(re.search(r"(^|\s)Features=gb10(\s|$)", line) for line in job_features)
        and any(re.search(r"ActiveFeatures=[^ ]*\bgb10\b", line) for line in node_features)
        and "env-constraints=unset" in text
    )
    record(
        "10 scontrol inside a job shows Features= and ActiveFeatures=",
        ok,
        f"rc={completed.returncode} " + " | ".join(features)[:400],
    )
    del nonce_dir


def row_11_host_class_measurement(nonce: Path) -> None:
    completed = pbrun(
        ["bash", "action.sh", "run", str(nonce)],
        extra=["--measurement", "--host-class", "gb10"],
    )
    prefix, job_id = submitted(completed)
    path, rec = outcome("done", prefix) if prefix else (None, {})
    producer: dict = {}
    if prefix:
        for candidate in sorted((SH / "cas" / "actions" / "v3" / prefix[:2]).glob(
                f"{prefix}*.json")):
            try:
                producer = json.loads(candidate.read_text()).get("producer", {})
            except (OSError, ValueError):
                producer = {}
    slurm = (producer.get("evidence") or {}).get("slurm") or {}
    controller = slurm.get("controller") or {}
    checks = {
        "pbrun rc==0": completed.returncode == 0,
        "done record": path is not None and rec.get("status") == "executed",
        "producer.host_class==gb10": producer.get("host_class") == "gb10",
        "controller.job_features has gb10": "gb10" in (controller.get("job_features") or []),
        "controller.node_active_features has gb10":
            "gb10" in (controller.get("node_active_features") or []),
        f"controller.batch_host=={NODE}": controller.get("batch_host") == NODE,
    }
    failed = [name for name, ok in checks.items() if not ok]
    record(
        "11 --measurement --host-class gb10 executes with an attested receipt",
        not failed,
        f"job={job_id} host_class={producer.get('host_class')!r} "
        f"partition={slurm.get('partition')!r} controller={controller}"
        + (f" missing: {failed}; rc={completed.returncode}; "
           f"stderr={(completed.stderr or '')[-600:]}" if failed else ""),
    )


def row_12_unknown_host_class_is_refused() -> None:
    refused = pbrun(
        ["bash", "action.sh", "run", ""],
        extra=["--measurement", "--host-class", "smoke-nonexistent", "--wait-s", "30"],
        timeout=180,
    )
    said = (refused.stderr or "") + (refused.stdout or "")
    record(
        "12 --host-class for a Feature no node has is refused at submit",
        refused.returncode != 0 and "slurm refused this action" in said
        and "submitted" not in (refused.stderr or ""),
        f"rc={refused.returncode} {said.strip().splitlines()[-1][:120] if said.strip() else ''}",
    )


# ---------------------------------------------------------------------------
# Rows 14a-14d: what ConstrainCores and ConstrainRAMSpace actually do
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

#: How much memory row 13c writes past its declaration, and in what chunks.
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

    The report is what makes row 13c readable in every arm.  A job the kernel
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


def row_14_cpu_containment() -> None:
    """What a job sees of the node's CPUs when it declares one, and when two.

    Read off ``taskset``, not ``nproc``.  ``nproc`` honours ``OMP_NUM_THREADS``
    before it looks at the affinity mask, and ``pbrun``'s sealed environment
    sets that to 4 (``tools/fleet/pbrun.py:2549``), so ``nproc`` answers 4
    under every declaration.  It is reported anyway, because an action that
    sizes its own parallelism from ``nproc`` is reading that 4.
    """

    node_cpus = int((sh(["nproc"]).stdout or "0").strip() or 0)
    for label, declared in (("14a", 1), ("14b", 2)):
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
            f"{label} what a --cpus {declared} job may run on [{ARM}]",
            ok,
            f"job affinity width={width} of the node's {node_cpus} CPUs "
            f"(declared {declared}); nproc said {seen} "
            f"(OMP_NUM_THREADS, not the cpuset)",
        )


def row_14c_over_declared_memory() -> None:
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
    swap_used = binding.get("swap_current", "")
    killed = state not in ("", "COMPLETED")
    declared_bytes = OVER_DECLARED_GB * 1024 ** 3
    said = f"pbrun: failed ({state})" in (completed.stderr or "")
    # And that `pbrun` named the declaration, not only the state.  A job the
    # kernel kills writes nothing to its own log, so the number that decided
    # the ending is in the submission and nowhere the operator was sent.
    named = (f"exceeded the {OVER_DECLARED_GB} GiB it declared"
             in (completed.stderr or ""))
    # Evidence that the limit did something, not merely that it exists.  A
    # `max` event is the direct form; pages in swap are the form it takes
    # under `ConstrainSwapSpace=no`, where reclaim succeeds and the counter
    # stays at zero -- measured on 2026-09-05: `memory.max` exactly the
    # declared 1073741824 bytes, `memory.events` all zero, and 2246184960
    # bytes of a 3072 MiB allocation resident in swap.
    acted = (bool(hit) and int(hit.group(1)) > 0) or (
        swap_used.isdigit() and int(swap_used) > 0
    )
    ok = (killed and said and named) or (
        acted and limit.isdigit() and int(limit) == declared_bytes
    )
    record(
        f"14c a job over its mem_gb is constrained, not ignored [{ARM}]",
        ok,
        f"job={job_id} declared mem_gb={OVER_DECLARED_GB}, wrote "
        f"{OVER_GROW_MIB} MiB -> filed {where or 'nothing'}/ state={state!r} "
        f"rc={detail.get('returncode')} signal={detail.get('signal')} "
        f"pbrun said failed({state})={said} and named the declaration"
        f"={named}; binding cgroup "
        f"{binding.get('path', '(none reported)')} "
        f"memory.max={limit or '-'} "
        f"(declared {declared_bytes}) "
        f"memory.swap.max={binding.get('swap_limit', '-')} "
        f"memory.swap.current={swap_used or '-'} "
        f"memory.events=[{events}]",
    )


def row_14d_within_declared_memory() -> None:
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
        f"14d a job within its mem_gb completes [{ARM}]",
        ok,
        f"job={job_id} declared mem_gb={UNDER_DECLARED_GB}, wrote "
        f"{UNDER_GROW_MIB} MiB -> filed {where or 'nothing'}/ "
        f"status={rec.get('status')} state={slurm.get('state')} "
        f"rc={completed.returncode}",
    )


# ---------------------------------------------------------------------------
# One job per action key at a time (issue #43)
# ---------------------------------------------------------------------------

def _job_field(job_id: str, fields: str) -> str:
    """One ``squeue`` line for a job, or an empty string if it is gone."""

    completed = sh(["squeue", "-h", "-j", str(job_id), "-o", fields])
    return (completed.stdout or "").strip().splitlines()[0].strip() \
        if (completed.stdout or "").strip() else ""


def _detached(completed: subprocess.CompletedProcess) -> dict:
    """The one JSON line ``pbrun --detach`` prints, or an empty mapping."""

    for line in (completed.stdout or "").splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                return json.loads(line)
            except ValueError:
                return {}
    return {}


def row_15a_singleton_holds_the_second_job(nonce: Path) -> None:
    """A second job of one action key waits for the first, then reads its work.

    The second submission is the lane's *own recorded argv*, replayed through
    ``sbatch`` while the first job runs.  A second ``pbrun`` would not submit
    at all -- it reads the CAS and attaches to the live submission, which is
    the submitter-side half of this and is row 15b -- so replaying the record
    is what puts a second job in front of the controller deterministically.
    What it proves is the scheduler's half: with ``--dependency=singleton``
    under the job name ``pb-<key12>``, the second job is held while the first
    runs, and when it starts it finds the receipt and materializes nothing.
    """

    started = pbrun(["bash", "action.sh", "sleep", str(nonce), "45"],
                    extra=["--detach"])
    announced = _detached(started)
    first_job = str(announced.get("job_id") or "")
    key = str(announced.get("action_key") or "")
    prefix = key[:12]
    if not first_job or not key:
        record("15a a second job of one key waits for the first", False,
               f"nothing detached: rc={started.returncode} "
               f"stderr={(started.stderr or '')[-400:]!r}")
        return

    running = wait_for(
        lambda: _job_field(first_job, "%T") == "RUNNING", timeout_s=180.0)
    recorded = lane_latest(prefix)
    argv = [str(value) for value in (recorded.get("argv") or [])]
    replay = sh(["sbatch", *argv[1:]]) if len(argv) > 1 else None
    second_job = (replay.stdout or "").strip().split(";")[0] if replay else ""

    # Issue #42: every invocation of sbatch names itself in the job's
    # Comment, and the adoption query is exactly this squeue.  Read while the
    # first job is still on the controller, because that is when a submitter
    # whose sbatch hung would be reading it.
    listed = sh([
        "squeue", "-h", "-u", USER, f"--name=pb-{prefix}",
        "--states=all", "-o", "%i|%k",
    ])
    commented = [
        line for line in (listed.stdout or "").splitlines()
        if line.startswith(f"{first_job}|pb:")
    ]
    shown = sh(["scontrol", "show", "job", str(first_job)]).stdout or ""

    held = ""
    if second_job:
        wait_for(lambda: _job_field(second_job, "%T|%r").startswith("PENDING"),
                 timeout_s=60.0)
        held = _job_field(second_job, "%T|%r")

    # Both jobs gone from the queue: the first finished its sleep, the second
    # was released by the dependency and ran.
    wait_for(lambda: not _job_field(first_job, "%T")
             and not _job_field(second_job, "%T"), timeout_s=240.0)
    lane = LANE / key if key else LANE
    second_out = lane / f"{second_job}.out"
    said = second_out.read_text() if second_out.exists() else ""
    ran = nonce.read_text().count("\n") if nonce.exists() else 0
    marker = lane / f"{second_job}.cache-hit.json"
    ending = _job_field(second_job, "%T")
    state = sh(["scontrol", "show", "job", str(second_job)]).stdout or ""

    checks = {
        "the first job ran": running,
        "sbatch accepted the replay": bool(second_job),
        "the second job was held on Dependency": held == "PENDING|Dependency",
        "the action ran exactly once": ran == 1,
        "the second job read the CAS instead": "already in the CAS" in said,
        "the second job filed a cache-hit marker": marker.exists(),
        "the second job exited 0": "ExitCode=0:0" in state or not ending,
        "squeue %k prints the submission's comment": bool(commented),
        "scontrol shows Comment=pb:": f"Comment=pb:{key}:" in shown,
    }
    failed = [name for name, ok in checks.items() if not ok]
    record(
        "15a a second job of one key is held on Dependency and then hits the CAS",
        not failed,
        f"job {first_job} RUNNING, job {second_job} squeue %T|%r={held!r}; "
        f"nonce lines={ran}; job {second_job} said "
        f"{((said.strip().splitlines() or ['(nothing)'])[-1])[:80]!r}; "
        f"squeue %i|%k={(commented or ['(none)'])[0][:64]!r}"
        + ("" if not failed else f"; MISSING {failed}"),
    )

    # Leave the key with the ending every reader of pb-queue expects: the
    # submitter detached, so nobody has filed one yet.
    sh([sys.executable, str(REPO / "tools" / "fleet" / "pbwait.py"),
        "--wait-s", "120", key], timeout=200)


def row_15b_two_pbruns_of_one_key(nonce: Path) -> None:
    """Two ``pbrun``s of one key started together: one execution, both exit 0.

    Which of the three paths the second takes is a race and is reported rather
    than asserted: it reads the receipt before submitting, it attaches to the
    first submission, or it submits and the controller holds it.  All three are
    correct and the invariants below hold for all three.  Asserting one of them
    would be asserting a scheduling coincidence.
    """

    command = ["bash", "action.sh", "run", str(nonce)]
    argv = [
        sys.executable, str(PBRUN), "--transport", "slurm",
        "--cwd", str(SRC), "--wait-s", str(WAIT_S), "--", *command,
    ]
    both = [
        subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         text=True, env=dict(os.environ))
        for _ in range(2)
    ]
    said = [process.communicate(timeout=900.0) for process in both]
    codes = [process.returncode for process in both]
    prefix = ""
    for _, err in said:
        match = (_submitted_re.search(err or "")
                 or re.search(r"pbrun: ([0-9a-f]{12}) is already in the CAS",
                              err or ""))
        if match:
            prefix = match.group(1)
            break
    ran = nonce.read_text().count("\n") if nonce.exists() else 0
    path, rec = outcome("done", prefix) if prefix else (None, {})
    detail = rec.get("detail", {}) if isinstance(rec, dict) else {}
    endings = len(list((QUEUE / "done").glob(f"{prefix}*.json"))) if prefix else 0
    took = []
    for out, err in said:
        printed = (out or "") + (err or "")
        if "cache_hit --" in printed:
            took.append("held by the scheduler, then read the CAS on the node")
        elif "already in the CAS; nothing submitted" in printed:
            took.append("read the CAS before submitting")
        elif "attaching to it" in printed:
            took.append("attached to the other submission")
        else:
            took.append("submitted and ran the work")
    checks = {
        "both exited 0": codes == [0, 0],
        "the action ran exactly once": ran == 1,
        "one done record": endings == 1 and path is not None,
        "the record is an execution": rec.get("status") == "executed",
        "receipt_published": detail.get("receipt_published") is True,
    }
    failed = [name for name, ok in checks.items() if not ok]
    record(
        "15b two concurrent pbruns of one key execute it once",
        not failed,
        f"rc={codes}; nonce lines={ran}; done records={endings}; "
        f"paths taken={took}"
        + ("" if not failed else f"; MISSING {failed}"),
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
    row_3_cas_hit(nonce, first_job, prefix)
    row_4_failure()
    timeout_prefix, timeout_job = row_5_timeout()
    row_6_withdraw()
    row_7_gres_and_constraint()
    row_8_epilog(timeout_job)
    row_9_no_slurmdbd(timeout_job or first_job)
    # Last, and after row 8: the Epilog row asserts that no job state file is
    # left in the lane, and a campaign in flight would legitimately hold three.
    campaign_nonces = [VOL / f"campaign-{name}.txt" for name in "abc"]
    for nonce in campaign_nonces:
        nonce.write_text("")
    row_10a_campaign(campaign_nonces)
    row_10b_campaign_rerun(campaign_nonces)
    row_10_scontrol_answers_the_job(WORK)
    row_11_host_class_measurement(VOL / "nonce-measurement.txt")
    row_12_unknown_host_class_is_refused()
    row_13_liveness()
    row_14_cpu_containment()
    row_14c_over_declared_memory()
    row_14d_within_declared_memory()
    row_15a_singleton_holds_the_second_job(VOL / "nonce-singleton.txt")
    row_15b_two_pbruns_of_one_key(VOL / "nonce-concurrent.txt")
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
