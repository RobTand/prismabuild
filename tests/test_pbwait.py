"""``pbwait`` reports endings for work it did not submit.

The interesting half is not the table.  It is that under SLURM there is nobody
else to file the terminal record -- ``slurm_lane.run`` writes it because it is
the process holding the submission open, and a detached submission has no such
process -- so the waiter has to resume the recorded job and file what ``run``
would have filed.  Every reader of ``pb-queue/done`` depends on that record
existing, so a detached submission nobody resumed would be invisible to all of
them.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbwait  # noqa: E402
from prismabuild import slurm_lane  # noqa: E402

from test_slurm_lane import fleet, _runnable_action  # noqa: E402,F401
from test_pbrun_detach import _checkout, _queue, _run_pbrun, _one_json_line  # noqa: E402

__all__ = ["fleet"]


def _count_single_poll(monkeypatch):
    real_look = pbwait._look_once
    calls = []

    def look(*args, **kwargs):
        calls.append(None)
        assert len(calls) == 1, "a terminal verdict must not be polled again"
        return real_look(*args, **kwargs)

    monkeypatch.setattr(pbwait, "_look_once", look)
    return calls


def _outcome(key: str, generation: float, *, status: str, returncode: int,
             host: str = "sparky") -> dict:
    return {
        "schema": pool.POOL_OUTCOME_SCHEMA_V1,
        "action_key": key,
        "status": status,
        "published_unix": generation,
        "finished_unix": 2.0,
        "finished_host": host,
        "attempts": 1,
        "detail": {"returncode": returncode, "elapsed_s": 12.25,
                   "stdout": "", "stderr": ""},
    }


def _file(queue, state: str, record: dict) -> Path:
    path = queue.item_path(state, str(record["action_key"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record), encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# The pull queue: the worker files the ending, this only watches
# --------------------------------------------------------------------------

def test_a_detached_pool_action_is_reported_once_a_worker_runs_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    work = _checkout(tmp_path)
    queue = _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach") == 0
    key = _one_json_line(capsys.readouterr())["action_key"]

    served = queue.serve_once(
        tags=["sparky"], python=sys.executable, timeout_s=60.0,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
    )
    assert served is not None and served["status"] == "executed", served

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    rows = pbwait.wait_for_keys(queue, [key], cas=cas, wait_s=5.0)
    assert [row["status"] for row in rows] == ["executed"]
    assert rows[0]["transport"] == "pool"
    assert rows[0]["host"] == socket.gethostname()
    assert rows[0]["returncode"] == 0
    assert pbwait.verdict(rows) == 0

    table = pbwait.render(rows)
    assert table.splitlines()[0].split() == [
        "key", "status", "transport", "job", "host", "elapsed", "rc",
        "receipt", "note"]
    # No job handle under the pull queue: the worker ran it in a process that
    # is gone, and there is nothing an operator could look up.
    assert rows[0]["job"] == "-"
    assert key[:12] in table.splitlines()[1]


def test_a_wait_on_a_key_with_no_record_yet_blocks_until_it_appears(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The whole point of detaching is that the ending is filed later.  A wait
    that answered "nothing is filed" would make the pair useless."""

    monkeypatch.setattr(pbrun, "POLL_S", 0.01)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    key = "a" * 64

    real_look = pbwait._look_once
    observations = []

    def look_then_land(*args, **kwargs):
        row = real_look(*args, **kwargs)
        observations.append(row)
        if len(observations) == 1:
            assert row is None, "the first poll must observe no ending"
            _file(queue, pool.DONE, _outcome(key, 5.0, status="executed",
                                           returncode=0))
        return row

    monkeypatch.setattr(pbwait, "_look_once", look_then_land)
    rows = pbwait.wait_for_keys(queue, [key], cas=cas, wait_s=30.0)
    assert len(observations) == 2
    assert rows[0]["status"] == "executed"
    assert pbwait.verdict(rows) == 0


def test_a_failed_action_makes_the_verdict_nonzero_beside_the_ones_that_worked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pbrun, "POLL_S", 0.01)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    good, bad = "b" * 64, "c" * 64
    _file(queue, pool.DONE, _outcome(good, 1.0, status="executed",
                                     returncode=0))
    _file(queue, pool.FAILED, _outcome(bad, 1.0, status="failed",
                                       returncode=7, host="dl380g10"))

    rows = pbwait.wait_for_keys(queue, [good, bad], cas=cas, wait_s=5.0)
    assert [row["status"] for row in rows] == ["executed", "failed"]
    assert rows[1]["returncode"] == 7
    assert rows[1]["host"] == "dl380g10"
    assert pbwait.verdict(rows) == 1
    # The one that worked is still reported, in the same table.
    assert good[:12] in pbwait.render(rows)
    assert bad[:12] in pbwait.render(rows)


def test_the_newest_ending_answers_when_no_generation_is_named(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One key holds the ending of every run of the same work, so a stale
    ``done`` can sit beside this run's ``failed``.  ``done`` is read first, so
    before the fix a bare-key wait reported ``executed`` for work that failed.

    A caller who knows the generation is not exposed to this at all: ``pbrun
    --detach`` prints it and ``pbcampaign`` passes it back.  The bare key is the
    case where nobody can, which is why the newest ending has to win.
    """

    monkeypatch.setattr(pbrun, "POLL_S", 0.01)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    key = "d" * 64
    _file(queue, pool.DONE, _outcome(key, 100.0, status="executed",
                                     returncode=0))
    _file(queue, pool.FAILED, _outcome(key, 200.0, status="failed",
                                       returncode=7))

    rows = pbwait.wait_for_keys(queue, [key], cas=cas, wait_s=1.0)
    assert rows[0]["status"] == "failed"
    assert rows[0]["returncode"] == 7
    assert pbwait.verdict(rows) == 1

    # Naming the older generation still answers with the older run: the
    # question was about that run, and its ending has not changed.
    older = pbwait.wait_for_keys(queue, [key], cas=cas, wait_s=1.0,
                                 generations={key: 100.0})
    assert older[0]["status"] == "executed"


def test_a_terminal_record_nobody_can_read_ends_the_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ending that was filed is an ending, readable or not.

    ``pbrun.terminal_record`` returns ``None`` for a record it cannot parse,
    which is right for "is this the generation I asked about" and wrong as the
    whole answer: the wait then spent ``--wait-s`` -- a day, by default -- on a
    verdict that was already on disk, and exited 75, which tells the operator
    to wait again. Terminal records are published by rename, so waiting again
    can only find the same broken file.
    """

    monkeypatch.setattr(pbrun, "POLL_S", 0.01)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    key = "ba" * 32
    path = queue.item_path(pool.FAILED, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"action_key": "' + key, encoding="utf-8")

    calls = _count_single_poll(monkeypatch)
    rows = pbwait.wait_for_keys(queue, [key], cas=cas, wait_s=30.0)
    assert len(calls) == 1
    assert rows[0]["status"] == "unreadable"
    assert rows[0]["note"] == f"not valid JSON: {path}"
    # 1, not 75. The guide gives 75 to "no verdict yet", whose remedy is to
    # run pbwait again; there is a record here and running again re-reads it.
    assert pbwait.verdict(rows) == 1
    assert "not valid JSON" in pbwait.render(rows)


def test_patience_running_out_is_reported_as_waiting_not_as_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A job that has not ended has not failed, and 75 is what ``pbrun`` says
    for the same thing: the work is still running."""

    monkeypatch.setattr(pbrun, "POLL_S", 0.01)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    rows = pbwait.wait_for_keys(queue, ["e" * 64], cas=cas, wait_s=0.05)
    assert rows[0]["status"] == "waiting"
    assert pbwait.verdict(rows) == pbwait.GAVE_UP_EXIT


# --------------------------------------------------------------------------
# SLURM: the waiter files the ending
# --------------------------------------------------------------------------

def test_a_detached_slurm_action_has_its_ending_filed_by_the_waiter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet: Path
) -> None:
    """The submitter detached, so nothing wrote ``done/<key>.json``.  Eleven
    fleet tools read that file; the wait is what makes it exist."""

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key = str(action["action_key"])

    assert pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=[],
        demand={"cpu": 1, "mem_gb": 1}, exclusive=False, timeout_s=600.0,
        wait_s=60.0, retry_safe=False, max_attempts=1, detach=True,
        queue_root=queue.root,
        worker_python=sys.executable, job_python=sys.executable,
        local_checkout_root=tmp_path / "checkouts",
    ) == 0
    assert not queue.item_path(pool.DONE, key).exists()

    rows = pbwait.wait_for_keys(
        queue, [key], cas=cas, wait_s=30.0, queue_root=queue.root, poll_s=0.0
    )
    assert rows[0]["status"] == "executed", rows
    assert rows[0]["transport"] == "slurm"
    assert rows[0]["receipt_published"] is True
    assert pbwait.verdict(rows) == 0

    record = json.loads(
        queue.item_path(pool.DONE, key).read_text(encoding="utf-8"))
    assert record["transport"] == "slurm"
    assert record["detail"]["receipt_published"] is True
    assert record["detail"]["slurm"]["state"] == "COMPLETED"
    # The job id is on the table, because it is what an operator types into
    # sacct and scontrol when they want more than the record holds.
    assert rows[0]["job"] == record["detail"]["slurm"]["job_id"]
    assert rows[0]["job"] in pbwait.render(rows)

    # And what the waiter filed is the record a waiting pbrun would have
    # filed.  Every reader of pb-queue reads one shape; a resumed ending that
    # was missing a field would be a second shape none of them expects.
    second = tmp_path / "attended"
    second.mkdir()
    other = _runnable_action(second, cas)
    attended_key = str(other["action_key"])
    assert pbrun.slurm_outcome(
        other, cas=cas, request_path=cas.publish_action_request(other), tags=[],
        demand={"cpu": 1, "mem_gb": 1}, exclusive=False, timeout_s=600.0,
        wait_s=60.0, retry_safe=False, max_attempts=1,
        queue_root=queue.root,
        worker_python=sys.executable, job_python=sys.executable,
        local_checkout_root=tmp_path / "checkouts",
    ) == 0
    attended = json.loads(
        queue.item_path(pool.DONE, attended_key).read_text(encoding="utf-8"))
    assert set(record) == set(attended)
    assert set(record["detail"]) == set(attended["detail"])
    assert record["published_by"] == attended["published_by"]
    assert record["status"] == attended["status"] == "executed"


def test_the_receipt_outranks_the_controller_when_the_ending_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet: Path
) -> None:
    """The CAS is asked before the scheduler, and it wins.

    A receipt says the work was done whatever the controller goes on to say --
    and what it says is unreliable in both directions here: after ``MinJobAge``
    it has forgotten the job entirely, and before its own bookkeeping catches
    up it can still call a finished job RUNNING.  Waiting on either for a
    verdict already in hand spends the whole of ``--wait-s`` to learn nothing,
    and the ending still has to be filed for the eleven tools that read it.
    """

    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key = str(action["action_key"])

    assert pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=[],
        demand={"cpu": 1, "mem_gb": 1}, exclusive=False, timeout_s=600.0,
        wait_s=60.0, retry_safe=False, max_attempts=1, detach=True,
        queue_root=queue.root,
        worker_python=sys.executable, job_python=sys.executable,
        local_checkout_root=tmp_path / "checkouts",
    ) == 0
    assert cas.lookup(action) is not None, "the job ran; the receipt is real"
    assert not queue.item_path(pool.DONE, key).exists()

    # The controller has not caught up: it still calls the finished job
    # RUNNING, which a waiter that trusted it would poll until its patience
    # ran out.
    job_id = str(slurm_lane.recorded_submission(key)["job_id"])
    (fleet / f"{job_id}.state").write_text("RUNNING|0:0\n", encoding="utf-8")

    calls = _count_single_poll(monkeypatch)
    rows = pbwait.wait_for_keys(
        queue, [key], cas=cas, wait_s=30.0, queue_root=queue.root, poll_s=0.0
    )
    assert rows[0]["succeeded"] is True, rows
    assert len(calls) == 1

    record = json.loads(
        queue.item_path(pool.DONE, key).read_text(encoding="utf-8"))
    assert record["status"] == "executed"
    assert record["detail"]["receipt_published"] is True


def test_a_detached_slurm_job_that_failed_is_filed_and_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet: Path
) -> None:
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "exit:7")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    action = _runnable_action(tmp_path, cas)
    request = cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key = str(action["action_key"])

    assert pbrun.slurm_outcome(
        action, cas=cas, request_path=request, tags=[],
        demand={"cpu": 1, "mem_gb": 1}, exclusive=False, timeout_s=600.0,
        wait_s=60.0, retry_safe=False, max_attempts=1, detach=True,
        queue_root=queue.root,
    ) == 0

    rows = pbwait.wait_for_keys(
        queue, [key], cas=cas, wait_s=30.0, queue_root=queue.root, poll_s=0.0
    )
    assert rows[0]["status"] == "failed"
    assert rows[0]["returncode"] == 7
    assert rows[0]["receipt_published"] is False
    assert pbwait.verdict(rows) == 1
    assert queue.item_path(pool.FAILED, key).exists()


# --------------------------------------------------------------------------
# Work that was already done
# --------------------------------------------------------------------------

def test_a_receipt_with_nothing_outstanding_is_reported_as_a_cache_hit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """A key whose work is in the CAS and whose queue item is long gone ended,
    however it was delivered.  ``cas`` is the honest answer to "which
    transport": none of them, this time."""

    work = _checkout(tmp_path)
    queue = _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach") == 0
    key = _one_json_line(capsys.readouterr())["action_key"]
    assert queue.serve_once(
        tags=["sparky"], python=sys.executable, timeout_s=60.0,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
    )["status"] == "executed"

    # The ending is filed; drop it, keeping the receipt, which is the state a
    # queue that has been reset leaves behind.
    queue.item_path(pool.DONE, key).unlink()
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    rows = pbwait.wait_for_keys(queue, [key], cas=cas, wait_s=0.5)
    assert rows[0]["status"] == "cache_hit"
    assert rows[0]["transport"] == "cas"
    assert rows[0]["receipt_published"] is True
    assert pbwait.verdict(rows) == 0


# --------------------------------------------------------------------------
# What an operator types
# --------------------------------------------------------------------------

def test_a_prefix_resolves_against_what_is_recorded_and_refuses_ambiguity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """Twelve characters is what every fleet log line prints, so twelve
    characters is what an operator has.  Two matches is refused rather than
    guessed: the wrong guess reports somebody else's work."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    first = "f0" + "0" * 62
    second = "f0" + "1" * 62
    _file(queue, pool.DONE, _outcome(first, 1.0, status="executed",
                                     returncode=0))
    _file(queue, pool.FAILED, _outcome(second, 1.0, status="failed",
                                       returncode=1))

    assert pbwait.resolve_key(queue, first[:12]) == first
    with pytest.raises(SystemExit) as raised:
        pbwait.resolve_key(queue, "f0")
    assert raised.value.code == pbwait.MISNAMED_EXIT
    assert "matches 2 actions" in capsys.readouterr().err

    # A whole key nothing has recorded is taken as given: waiting for work that
    # is not submitted yet is the case this tool exists for.
    assert pbwait.resolve_key(queue, "9" * 64) == "9" * 64


def test_a_key_nobody_can_resolve_exits_two_not_one(
    tmp_path: Path, capsys
) -> None:
    """Exit 2 is the code the operating guide gives a key that names nothing.

    ``pbrun --withdraw`` already exits 2 for the same two refusals, and the
    guide's table says ``pbrun`` and ``pbwait`` use the same codes.  Exit 1 is
    "the action failed", so a mistyped key read to a wrapper as a build that
    ran and lost, which is the one answer that provokes the wrong response.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    for key in ("ab" + "0" * 62, "ab" + "1" * 62):
        _file(queue, pool.DONE, _outcome(key, 1.0, status="executed",
                                         returncode=0))

    for name, expected in (
        ("", "empty key resolves to nothing"),
        ("   ", "empty key resolves to nothing"),
        ("cc", "nothing recorded matches"),
        ("ab", "matches 2 actions"),
    ):
        with pytest.raises(SystemExit) as raised:
            pbwait.resolve_key(queue, name)
        assert raised.value.code == 2, name
        assert expected in capsys.readouterr().err, name


def test_the_table_names_the_actions_status_beside_the_runs() -> None:
    """One column: an operator reads this table a campaign's worth of rows at
    a time, and the run's status is the one ``pbwait`` verdicts on."""

    row = pbwait._row("a" * 64, "failed", returncode=1, action_returncode=7)
    assert pbwait._cell(row, "returncode") == "1 (action 7)"

    signalled = pbwait._row(
        "a" * 64, "failed", returncode=1, action_returncode=-9, action_signal=9
    )
    assert pbwait._cell(signalled, "returncode") == "1 (action signal 9)"

    agreeing = pbwait._row("a" * 64, "failed", returncode=7, action_returncode=7)
    assert pbwait._cell(agreeing, "returncode") == "7"

    assert pbwait._cell(pbwait._row("a" * 64, "failed", returncode=1),
                        "returncode") == "1"


def test_a_wait_says_what_the_scheduler_says_while_it_waits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """``resume`` reports through ``on_notice``, and this is what prints it.

    Two things reach a waiter that way and neither is an ending: a controller
    that cannot be asked, and a job the scheduler is holding behind another
    job of the same action key.  ``pbrun`` prints both.  ``pbwait`` passed no
    ``on_notice`` at all, so a detached wait printed nothing for however long
    it waited, whatever the scheduler said.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key = "cd" * 32
    directory = slurm_lane.lane_directory(key, root=tmp_path / "lane")
    directory.mkdir(parents=True)
    (directory / "latest.json").write_text(json.dumps({
        "schema": slurm_lane.SUBMISSION_SCHEMA_V1,
        "action_key": key, "attempt": 1, "job_id": "1007",
        "directory": str(directory), "published_unix": 5.0,
    }), encoding="utf-8")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    request = Path(cas.root) / "requests" / key[:2] / f"{key}.json"
    request.parent.mkdir(parents=True)
    request.write_text(json.dumps({"action_key": key}), encoding="utf-8")

    def resume(submission, **kwargs):
        kwargs["on_notice"](
            "slurm job 1007: waiting for slurm job 1006 to finish the same "
            f"action {key[:12]}")
        return slurm_lane.RunResult(action_key=key)

    monkeypatch.setattr(pbwait.slurm_lane, "resume", resume)
    monkeypatch.setattr(cas, "lookup", lambda action: None)
    rows = pbwait.wait_for_keys(
        queue, [key], cas=cas, wait_s=0.0, lane_root=tmp_path / "lane",
        queue_root=queue.root,
    )

    assert rows[0]["status"] == "waiting"
    # The job id is what the operator reads the scheduler's own tools with,
    # and the submission record has it, so a waiting row prints it rather than
    # making them go and look it up.
    assert rows[0]["job"] == "1007"
    assert "waiting for slurm job 1006" in capsys.readouterr().err
