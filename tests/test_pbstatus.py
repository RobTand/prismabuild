"""The fleet status screen, driven against fake scheduler binaries.

SLURM is installed on no box in this fleet, so nothing here may import it or
assume it answered.  What the tests can pin is the pair of things a first
install would otherwise discover the hard way: the exact command line the tool
sends, and what it makes of the rows that come back.

The fake ``sinfo``, ``squeue`` and ``scontrol`` each record their argv, so the
explicit format strings are asserted rather than assumed -- a status screen
that silently loses a column reads exactly like a fleet that lost the value.
The rows themselves are the shapes a live 25.11.2 controller produced when this
was written: ``%e`` reads ``N/A`` on a node whose ``slurmd`` is not up, ``%G``
reads ``(null)`` where there is no GRES, ``%E`` reads ``none`` where there is
no reason, ``%l`` reads ``UNLIMITED`` for a job submitted without ``--time``,
and ``%r`` reads a sentence with spaces in it.

The endings half writes its records through ``slurm_lane.publish_outcome`` and
the pull queue's own schema rather than by hand, because the tool reads those
records by field name and a hand-built fixture is where a field name drifts.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool, slurm_lane as sl  # noqa: E402

import pbstatus  # noqa: E402


# --------------------------------------------------------------------------
# The fake controller: three scripts that echo canned rows and log their argv
# --------------------------------------------------------------------------

_RECORDER = '''\
import json, os, sys
from pathlib import Path

state = Path(os.environ["FAKE_SLURM_STATE"])
state.mkdir(parents=True, exist_ok=True)
with (state / "calls.jsonl").open("a") as handle:
    handle.write(json.dumps(
        {"program": Path(sys.argv[0]).name, "argv": sys.argv[1:]}) + "\\n")
'''

_SINFO = _RECORDER + '''
if os.environ.get("FAKE_SINFO_BROKEN") == "1":
    sys.stderr.write("sinfo: error: Unable to contact slurm controller\\n")
    raise SystemExit(1)
for row in os.environ.get("FAKE_SINFO_ROWS", "").split(";"):
    if row.strip():
        print(row)
'''

_SQUEUE = _RECORDER + '''
for row in os.environ.get("FAKE_SQUEUE_ROWS", "").split(";"):
    if row.strip():
        print(row)
'''

# `scontrol show node` prints one block per node, `Key=value` throughout.
_SCONTROL = _RECORDER + '''
detail = "-d" in sys.argv[1:]
for row in os.environ.get("FAKE_SCONTROL_NODES", "").split(";"):
    if not row.strip():
        continue
    name, alloc_mem, gres_used = row.split("|")
    print(f"NodeName={name} CoresPerSocket=10")
    print(f"   CPUAlloc=0 CPUTot=20 CPULoad=0.50")
    print(f"   RealMemory=73728 AllocMem={alloc_mem} FreeMem=N/A")
    # The controller packs GresUsed only under SHOW_DETAIL, and never for a
    # node that declares no GRES.  Measured on a live 25.11.2 controller.
    if detail and gres_used != "(null)":
        print(f"   GresUsed={gres_used}")
    print("")
'''

#: Two GB10 boxes in `all` and `gpu`, one x86 box in `all` and `cpu` that the
#: operator has drained, and one node the controller cannot reach.
SINFO_ROWS = ";".join([
    "sparky|all*|mixed|4/16/0/20|73728|N/A|gpu:1,shard:2|gb10,aarch64,sparky|1.20|none",
    "sparky|gpu|mixed|4/16/0/20|73728|N/A|gpu:1,shard:2|gb10,aarch64,sparky|1.20|none",
    # A drained node's CPUs count as `other`, not as idle: 0/0/80/80.
    "dl380g10|all*|drained|0/0/80/80|61440|N/A|(null)|x86,x86_64,dl380g10|0.10|"
    "operator took it out for a spin",
    "dl380g10|cpu|drained|0/0/80/80|61440|N/A|(null)|x86,x86_64,dl380g10|0.10|"
    "operator took it out for a spin",
    "gx10-6b77|all*|idle*|0/20/0/20|81920|N/A|gpu:1,shard:3|gb10,aarch64|0.00|"
    "Not responding",
])

SCONTROL_NODES = ";".join([
    "sparky|16384|gpu:0,shard:1",
    "dl380g10|0|(null)",
    "gx10-6b77|0|gpu:0,shard:0",
])


@pytest.fixture()
def fleet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
    """A fake controller on PATH, a private lane root, a private queue root."""

    binaries = tmp_path / "bin"
    binaries.mkdir()
    for name, body in (
        ("sinfo", _SINFO), ("squeue", _SQUEUE), ("scontrol", _SCONTROL),
    ):
        script = binaries / name
        script.write_text(
            f"#!{sys.executable}\n" + textwrap.dedent(body), encoding="utf-8")
        script.chmod(0o755)
    state = tmp_path / "slurm-state"
    state.mkdir()
    monkeypatch.setenv("FAKE_SLURM_STATE", str(state))
    monkeypatch.setenv("FAKE_SINFO_ROWS", SINFO_ROWS)
    monkeypatch.setenv("FAKE_SCONTROL_NODES", SCONTROL_NODES)
    monkeypatch.setenv("FAKE_SQUEUE_ROWS", "")
    lane = tmp_path / "lane"
    lane.mkdir()
    queue = tmp_path / "pb-queue"
    return {
        "bin": binaries, "state": state, "lane": lane, "queue": queue,
        "argv": [
            "--lane-root", str(lane), "--queue-root", str(queue),
            "--sinfo", str(binaries / "sinfo"),
            "--squeue", str(binaries / "squeue"),
            "--scontrol", str(binaries / "scontrol"),
        ],
    }


def _calls(fleet: dict) -> list[dict]:
    path = fleet["state"] / "calls.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def _run(fleet: dict, capsys, *extra: str) -> str:
    assert pbstatus.main(fleet["argv"] + list(extra)) == 0
    return capsys.readouterr().out


def _run_json(fleet: dict, capsys, *extra: str) -> dict:
    return json.loads(_run(fleet, capsys, "--json", *extra))


def _record_submission(fleet: dict, key: str, *, job_id: str,
                       resources: dict, constraint: list[str],
                       host: str) -> None:
    """Write one lane record with the fields ``slurm_lane.submit`` seals."""

    directory = sl.lane_directory(key, root=fleet["lane"])
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "latest.json").write_text(json.dumps({
        "schema": sl.SUBMISSION_SCHEMA_V1,
        "action_key": key,
        "attempt": 1,
        "job_id": job_id,
        "constraint": constraint,
        "partition": "",
        "time_limit": "",
        "resources": resources,
        "submitted_host": host,
        "published_unix": 1.0,
    }, sort_keys=True), encoding="utf-8")


KEY_RUNNING = "a1" * 32
KEY_PENDING = "b2" * 32


def _queue_rows(fleet: dict) -> None:
    """Two jobs the lane knows about, one it does not, and one PENDING each."""

    _record_submission(
        fleet, KEY_RUNNING, job_id="1001",
        resources={"cpu": 8, "gpu": 1, "mem_gb": 32},
        constraint=["sparky"], host="sparky")
    _record_submission(
        fleet, KEY_PENDING, job_id="1002",
        resources={"cpu": 2, "mem_gb": 4}, constraint=[], host="dl380g10")
    os.environ["FAKE_SQUEUE_ROWS"] = ";".join([
        f"1001|RUNNING|gpu|sparky|12:03|02:00:00|rob|pb-{KEY_RUNNING[:12]}|None",
        f"1002|PENDING|cpu||0:00|UNLIMITED|rob|pb-{KEY_PENDING[:12]}|Resources",
        "1003|PENDING|all||0:00|UNLIMITED|rob|pb-cccccccccccc|"
        "BadConstraints",
        "1004|RUNNING|all|dl380g10|1:00:07|UNLIMITED|someone|nightly-backup|None",
    ])


# --------------------------------------------------------------------------
# The command lines
# --------------------------------------------------------------------------

def test_asks_the_scheduler_with_explicit_formats(fleet, capsys):
    _run(fleet, capsys)
    calls = {call["program"]: call["argv"] for call in _calls(fleet)}
    assert calls["sinfo"] == ["-N", "-h", "-o", pbstatus.SINFO_FORMAT]
    assert calls["squeue"] == ["-h", "-o", pbstatus.SQUEUE_FORMAT]
    # One `scontrol` call for the whole node list, not one per node.
    # `-d` is required: the controller packs `GresUsed` only under SHOW_DETAIL.
    assert calls["scontrol"] == [
        "-d", "show", "node", "sparky,dl380g10,gx10-6b77"]
    assert len([c for c in _calls(fleet) if c["program"] == "scontrol"]) == 1


# --------------------------------------------------------------------------
# The nodes table
# --------------------------------------------------------------------------

def test_nodes_merge_their_partitions_and_carry_gres_in_use(fleet, capsys):
    nodes = {row["node"]: row for row in _run_json(fleet, capsys)["nodes"]}
    assert list(nodes) == ["sparky", "dl380g10", "gx10-6b77"]
    sparky = nodes["sparky"]
    # Two `sinfo -N` rows, one node.
    assert sparky["partitions"] == ["all", "gpu"]
    assert (sparky["cpus_alloc"], sparky["cpus_idle"]) == (4, 16)
    assert sparky["gres"] == "gpu:1,shard:2"
    assert sparky["gres_used"] == "gpu:0,shard:1"
    # AllocMem has no `sinfo` format code; it comes from `scontrol`.
    assert sparky["memory_alloc_mib"] == 16384
    assert sparky["memory_total_mib"] == 73728
    assert sparky["features"] == ["gb10", "aarch64", "sparky"]
    assert sparky["load"] == "1.20"
    assert sparky["healthy"] is True
    assert sparky["reason"] is None
    # `(null)` is SLURM's way of saying a node declares no GRES.
    assert nodes["dl380g10"]["gres"] is None


def test_a_drained_node_is_flagged_with_its_reason(fleet, capsys):
    out = _run(fleet, capsys)
    node = [row for row in _run_json(fleet, capsys)["nodes"]
            if row["node"] == "dl380g10"][0]
    assert node["state"] == "drained"
    assert node["healthy"] is False
    assert node["reason"] == "operator took it out for a spin"
    assert "operator took it out for a spin" in out


def test_an_unreachable_node_is_flagged_even_though_it_reads_idle(fleet, capsys):
    node = [row for row in _run_json(fleet, capsys)["nodes"]
            if row["node"] == "gx10-6b77"][0]
    # `idle*` is not idle: the star is the controller saying it cannot reach
    # the node, and the base state is the last thing it heard.
    assert node["state"] == "idle"
    assert node["flags"] == ["not responding"]
    assert node["healthy"] is False
    assert "not responding" in "\n".join(pbstatus.node_lines([node]))


def test_a_node_with_no_gres_reads_absent_not_unknown(fleet, capsys):
    node = [row for row in _run_json(fleet, capsys)["nodes"]
            if row["node"] == "dl380g10"][0]
    line = pbstatus.node_lines([node])[1]
    # The controller said this node declares no GRES.  That is its
    # configuration, not a column this table failed to read.
    assert " - " in line
    assert "? of ?" not in line


def test_the_flag_does_not_repeat_the_controllers_own_words(fleet, capsys):
    node = [row for row in _run_json(fleet, capsys)["nodes"]
            if row["node"] == "gx10-6b77"][0]
    line = pbstatus.node_lines([node])[1]
    assert line.lower().count("not responding") == 1


def test_split_node_state_reads_the_suffix_flags():
    assert pbstatus.split_node_state("mixed") == ("mixed", [])
    assert pbstatus.split_node_state("down*") == ("down", ["not responding"])
    assert pbstatus.split_node_state("IDLE~") == ("idle", ["powered down"])


# --------------------------------------------------------------------------
# The jobs table
# --------------------------------------------------------------------------

def test_jobs_resolve_their_action_key_to_the_lane_record(fleet, capsys):
    _queue_rows(fleet)
    jobs = {row["job_id"]: row for row in _run_json(fleet, capsys)["jobs"]}
    running = jobs["1001"]
    assert running["state"] == "RUNNING"
    assert running["partition"] == "gpu"
    assert running["node"] == "sparky"
    assert running["elapsed"] == "12:03"
    assert running["time_limit"] == "02:00:00"
    assert running["user"] == "rob"
    assert running["action_key_prefix"] == KEY_RUNNING[:12]
    assert running["resources"] == {"cpu": 8, "gpu": 1, "mem_gb": 32}
    assert running["constraint"] == ["sparky"]
    assert running["submitted_host"] == "sparky"
    # `squeue` prints `None` as the reason for a running job.
    assert running["reason"] is None
    assert running["note"] is None


def test_a_pending_job_shows_the_reason_it_is_waiting(fleet, capsys):
    _queue_rows(fleet)
    jobs = {row["job_id"]: row for row in _run_json(fleet, capsys)["jobs"]}
    assert jobs["1002"]["reason"] == "Resources"
    assert jobs["1003"]["reason"] == "BadConstraints"
    out = _run(fleet, capsys)
    assert "Resources" in out and "BadConstraints" in out


def test_an_unlimited_time_limit_is_reported_as_unlimited(fleet, capsys):
    _queue_rows(fleet)
    jobs = {row["job_id"]: row for row in _run_json(fleet, capsys)["jobs"]}
    # No `--time` was sent, the partitions are MaxTime=UNLIMITED, and the
    # record's own `time_limit` is empty.  That is a job with no limit, not a
    # limit nobody could read.
    assert jobs["1002"]["time_limit"] == "UNLIMITED"


def test_a_job_the_lane_never_submitted_still_lists(fleet, capsys):
    _queue_rows(fleet)
    jobs = {row["job_id"]: row for row in _run_json(fleet, capsys)["jobs"]}
    foreign = jobs["1004"]
    # Not a PrismaBuild job name, so there is no key to resolve and no note to
    # make about one.
    assert foreign["action_key_prefix"] is None
    assert foreign["resources"] is None
    assert foreign["note"] is None
    assert foreign["user"] == "someone"
    # A PrismaBuild-shaped name with no record says so rather than going blank.
    assert jobs["1003"]["action_key_prefix"] == "cccccccccccc"
    assert jobs["1003"]["note"] == "no submission recorded in the lane root"


def test_a_resubmitted_key_says_which_job_the_record_names(fleet, capsys):
    _queue_rows(fleet)
    _record_submission(
        fleet, KEY_RUNNING, job_id="2001",
        resources={"cpu": 8, "gpu": 1, "mem_gb": 32},
        constraint=["sparky"], host="sparky")
    jobs = {row["job_id"]: row for row in _run_json(fleet, capsys)["jobs"]}
    assert jobs["1001"]["note"] == "the lane's latest record is job 2001"


# --------------------------------------------------------------------------
# The endings table
# --------------------------------------------------------------------------

def _slurm_ending(fleet: dict, key: str, *, status: str, state: str) -> None:
    job = sl.SubmittedJob(
        action_key=key, job_id="900", attempt=1, argv=[],
        script=Path("/nonexistent/job.sh"),
        directory=Path("/nonexistent"),
        stdout_path=Path("/nonexistent/900.out"),
        stderr_path=Path("/nonexistent/900.err"),
        record_path=Path("/nonexistent/rec.json"))
    outcome = sl.Outcome(
        job_id="900", state=state, exit_code=0 if status == "executed" else 1,
        signal=None, stdout_path=None, stderr_path=None,
        provenance=sl.JobProvenance(
            state=state, elapsed_s=42.0, node="sparky",
            partition="gpu", end_unix=2000.0))
    sl.publish_outcome(
        queue_root=fleet["queue"], action_key=key, published_unix=1.0,
        published_by="sparky", status=status, attempts=1, max_attempts=1,
        retry_safe=False, job=job, outcome=outcome,
        receipt={"result_digest": "d"} if status == "executed" else None)


def _pool_ending(fleet: dict, key: str) -> None:
    directory = fleet["queue"] / pool.DONE
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{key}.json").write_text(json.dumps({
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "action_key": key,
        "status": "executed",
        "finished_unix": 1000.0,
        "finished_host": "dl380g10",
        "detail": {"returncode": 0, "elapsed_s": 7.5},
    }, sort_keys=True), encoding="utf-8")


def test_endings_name_their_transport_and_their_slurm_state(fleet, capsys):
    _slurm_ending(fleet, "c3" * 32, status="failed", state="TIMEOUT")
    _pool_ending(fleet, "d4" * 32)
    endings = _run_json(fleet, capsys)["endings"]
    # Newest first by the time the work finished: the SLURM record's end time
    # is 2000, the pull queue's is 1000.
    assert [row["transport"] for row in endings] == ["slurm", "pool"]
    slurm_row, pool_row = endings
    assert slurm_row["status"] == "failed"
    assert slurm_row["slurm_state"] == "TIMEOUT"
    assert slurm_row["host"] == "sparky"
    assert slurm_row["elapsed_s"] == 42.0
    # A TIMEOUT files returncode None, not the job's exit code.  SLURM reports
    # a job it killed at its time limit as ExitCode=0:15 -- exit code zero,
    # signal fifteen -- and slurm_lane.detail_status_and_returncode keeps the
    # pull queue's convention for that case on purpose: status is the
    # authority and the number is withheld, because a zero filed under
    # failed/ reads as a pass to any reader that takes zero as success, and
    # Tessera's merge_suite does.
    assert slurm_row["returncode"] is None
    assert slurm_row["receipt_published"] is False
    # The pull queue files no receipt field at all, which is not the same as a
    # receipt that was not published.
    assert pool_row["receipt_published"] is None
    assert pool_row["slurm_state"] is None
    assert pool_row["host"] == "dl380g10"
    out = _run(fleet, capsys)
    assert "TIMEOUT" in out and "dl380g10" in out


def test_a_withdrawal_is_an_ending_and_is_listed_from_withdrawn(fleet, capsys):
    """The pool files a withdrawal under ``withdrawn/`` and never under
    ``failed/``, and the lane follows that rule, so the endings table reads
    that directory too or a cancelled job vanishes from it."""

    _slurm_ending(fleet, "f6" * 32, status="withdrawn", state="CANCELLED")
    ending = _run_json(fleet, capsys)["endings"][0]
    assert ending["status"] == "withdrawn"
    assert ending["transport"] == "slurm"
    assert ending["slurm_state"] == "CANCELLED"
    assert ending["path"].endswith(f"/{pool.WITHDRAWN}/{'f6' * 32}.json")
    assert not (fleet["queue"] / pool.FAILED / f"{'f6' * 32}.json").exists()


def test_an_ending_names_the_actions_own_status_in_its_own_column(fleet, capsys):
    """The RC column is the launcher's status, which is 1 for every failure.

    The action's own goes in a column of its own rather than replacing it:
    every reader of these records, this table included, means the run's status
    by RC.
    """

    key = "e5" * 32
    sl.publish_outcome(
        queue_root=fleet["queue"], action_key=key, published_unix=1.0,
        published_by="sparky", status="failed", attempts=1, max_attempts=1,
        retry_safe=False, detail={"returncode": 1, "action_returncode": 7},
    )
    ending = _run_json(fleet, capsys)["endings"][0]
    assert ending["returncode"] == 1
    assert ending["action_returncode"] == 7

    out = _run(fleet, capsys)
    assert "ACTION RC" in out
    assert "7" in out.split("== endings")[1]


def test_a_receipt_puts_the_ending_under_done(fleet, capsys):
    _slurm_ending(fleet, "e5" * 32, status="executed", state="COMPLETED")
    ending = _run_json(fleet, capsys)["endings"][0]
    assert ending["status"] == "executed"
    assert ending["receipt_published"] is True
    assert ending["path"].endswith(f"/{pool.DONE}/{'e5' * 32}.json")


def test_recent_bounds_how_many_records_are_read(fleet, capsys):
    _slurm_ending(fleet, "c3" * 32, status="failed", state="TIMEOUT")
    _pool_ending(fleet, "d4" * 32)
    assert len(_run_json(fleet, capsys, "--recent", "1")["endings"]) == 1
    assert _run_json(fleet, capsys, "--recent", "0")["endings"] == []


def test_the_tool_never_creates_the_queue_directories(fleet, capsys):
    _run(fleet, capsys)
    # `slurm_lane._queue_dir` makes the directory it is asked for; a status
    # screen must not, or looking at an empty fleet invents its queue.
    assert not fleet["queue"].exists()


# --------------------------------------------------------------------------
# The output shapes
# --------------------------------------------------------------------------

def test_json_is_one_object_with_the_three_lists(fleet, capsys):
    _queue_rows(fleet)
    _pool_ending(fleet, "d4" * 32)
    payload = _run_json(fleet, capsys)
    assert set(payload) == {"schema", "nodes", "jobs", "endings", "scheduler"}
    assert payload["scheduler"] == []
    assert len(payload["nodes"]) == 3
    assert len(payload["jobs"]) == 4
    assert len(payload["endings"]) == 1


def test_the_three_tables_print_in_order(fleet, capsys):
    _queue_rows(fleet)
    _pool_ending(fleet, "d4" * 32)
    out = _run(fleet, capsys)
    assert out.index("== nodes") < out.index("== jobs") < out.index("== endings")
    assert "sparky" in out and "1001" in out


# --------------------------------------------------------------------------
# No scheduler
# --------------------------------------------------------------------------

def test_no_scheduler_says_so_once_and_still_prints_the_endings(
        fleet, capsys, tmp_path):
    _pool_ending(fleet, "d4" * 32)
    argv = [flag if flag != str(fleet["bin"] / "sinfo")
            else str(tmp_path / "nowhere" / "sinfo") for flag in fleet["argv"]]
    argv = [flag if flag != str(fleet["bin"] / "squeue")
            else str(tmp_path / "nowhere" / "squeue") for flag in argv]
    assert pbstatus.main(argv) == 0
    out = capsys.readouterr().out
    assert "not found; SLURM is not installed on this box" in out
    # The endings are files on the shared mount and do not need a scheduler.
    assert "dl380g10" in out


def test_a_refusing_controller_is_reported_not_raised(
        fleet, capsys, monkeypatch):
    monkeypatch.setenv("FAKE_SINFO_BROKEN", "1")
    out = _run(fleet, capsys)
    assert "Unable to contact slurm controller" in out
    payload = _run_json(fleet, capsys)
    assert payload["nodes"] == []
    assert any("sinfo:" in note for note in payload["scheduler"])


def test_a_missing_scontrol_says_why_the_two_columns_are_unknown(
        fleet, capsys, tmp_path):
    argv = [flag if flag != str(fleet["bin"] / "scontrol")
            else str(tmp_path / "nowhere" / "scontrol") for flag in fleet["argv"]]
    assert pbstatus.main(argv) == 0
    out = capsys.readouterr().out
    # AllocMem and GresUsed then read `?`, which must not look like a fleet
    # fact with nothing saying where it came from.
    assert "scontrol: not found" in out
    assert pbstatus.main(argv + ["--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert any("scontrol:" in note for note in payload["scheduler"])
    assert payload["nodes"][0]["gres_used"] is None
    assert payload["nodes"][0]["memory_alloc_mib"] is None


def test_a_bad_argument_is_the_only_non_zero_exit(fleet, capsys):
    with pytest.raises(SystemExit) as raised:
        pbstatus.main(fleet["argv"] + ["--recent", "-1"])
    assert raised.value.code == 2


def test_it_runs_as_a_command_with_no_arguments_beyond_the_roots(fleet):
    completed = subprocess.run(
        [sys.executable, str(REPOSITORY / "tools" / "fleet" / "pbstatus.py")]
        + fleet["argv"],
        capture_output=True, text=True, env={**os.environ},
    )
    assert completed.returncode == 0, completed.stderr
    assert "== nodes" in completed.stdout


def _purged_slurm_ending(fleet: dict, key: str) -> None:
    """A record for a job the controller no longer remembers.

    ``sacct`` is dead without ``slurmdbd`` and ``scontrol`` forgets a job after
    ``MinJobAge``, so a ``pbrun`` that files an ending late gets no provenance
    at all: no node, no elapsed time, no end time.  Every one of those is
    ``None`` on the record, which is the shape the pull queue could never
    produce and therefore the one a pool-record reader was never written for.
    """

    job = sl.SubmittedJob(
        action_key=key, job_id="4242", attempt=1, argv=["sbatch"],
        script=Path("/nonexistent/job.sh"), directory=Path("/nonexistent"),
        stdout_path=Path("/nonexistent/4242.out"),
        stderr_path=Path("/nonexistent/4242.err"),
        record_path=Path("/nonexistent/rec.json"))
    outcome = sl.Outcome(
        job_id="4242", state="FAILED", exit_code=None, signal=None,
        stdout_path=None, stderr_path=None, provenance=None)
    sl.publish_outcome(
        queue_root=fleet["queue"], action_key=key, published_unix=1.0,
        published_by="sparky", status="failed", attempts=1, max_attempts=1,
        retry_safe=False, job=job, outcome=outcome)


def test_a_purged_job_reads_as_unknown_rather_than_none(fleet, capsys) -> None:
    """The host is the field that matters, and ``None`` is not a hostname.

    Under the pull queue ``finish`` runs on the box doing the work, so
    ``finished_host`` is always a name.  Under SLURM the node comes from the
    scheduler, and a purged job has no answer -- so the row has to say it does
    not know, in the same word every other missing cell uses, rather than
    print the word ``None`` where an operator reads a box name.
    """

    _purged_slurm_ending(fleet, "f6" * 32)
    row = _run_json(fleet, capsys)["endings"][0]
    assert row["transport"] == "slurm"
    assert row["host"] is None
    assert row["elapsed_s"] is None
    # `claimed_by` is the job id under SLURM, not a worker name, and the
    # endings table does not offer it as a host.
    assert row["status"] == "failed"

    lines = pbstatus.ending_lines(_run_json(fleet, capsys)["endings"])
    assert "None" not in "\n".join(lines)
    assert pbstatus.UNKNOWN in lines[1]
