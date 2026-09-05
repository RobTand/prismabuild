"""``pool_reset`` after the cutover: which transport a re-submission rides.

The tool recovers work out of ``failed/`` and submits it again through
``pbrun``.  Once an ending in that directory can have been filed by the SLURM
lane, "submit it again" is ambiguous: the pull queue has no worker draining it
under SLURM, so a record the lane wrote must go back through the lane or the
reset is a re-submission into a queue nobody reads.

Two further things are asserted here because both are silent when wrong.  A
``TIMEOUT`` is a failure like any other -- the lane files it under ``failed/``
with ``detail.slurm.state`` saying so -- and it must be resettable, or the one
kind of failure a retry most often fixes is the one this tool skips.  And a
withdrawal is a decision: it stops a re-submission of *its own generation* and
of no other, because an action key is a content hash and asking for the same
work again is not an attempt to defeat somebody's cancellation.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import slurm_lane as sl  # noqa: E402

import pool_reset  # noqa: E402

from test_slurm_lane import (  # noqa: E402
    JOB_ENTRY,
    WORKER,
    _runnable_action,
    _submissions,
    fleet as slurm_fleet,
)

__all__ = ["slurm_fleet"]


KEY_SLURM = "a" * 64
KEY_POOL = "b" * 64


def _cas_request(cas_root: Path, key: str, argv: list[str]) -> None:
    path = cas_root / "requests" / key[:2] / f"{key}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "action_key": key,
        "params": {"command": argv},
        "task": {"working_directory": "."},
    }), encoding="utf-8")


def _failed(
    queue_root: Path, key: str, *, checkout: Path, transport: str | None,
    state: str = "FAILED", published_unix: float = 1000.0,
    resources: dict | None = None, gres: str | None = None,
) -> Path:
    record: dict[str, object] = {
        "action_key": key,
        "published_unix": published_unix,
        "status": "failed",
        "attempts": 1,
        "checkout_root": str(checkout),
        "resources": {"cpu": 2, "mem_gb": 4} if resources is None else resources,
        "tags": ["x86"],
        "detail": {"status": "failed", "returncode": 1},
    }
    if transport is not None:
        record["transport"] = transport
        record["schema"] = "prismaquant.prismabuild.slurm_outcome.v1"
        record["detail"]["slurm"] = {
            "job_id": "1001", "state": state, "gres": gres,
        }
    path = queue_root / pool.FAILED / f"{key}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record), encoding="utf-8")
    return path


@pytest.fixture()
def fleet(tmp_path: Path) -> dict:
    """A queue with one lane-filed failure and one pull-queue failure."""

    queue_root = tmp_path / "pb-queue"
    cas_root = tmp_path / "cas"
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    for key in (KEY_SLURM, KEY_POOL):
        _cas_request(cas_root, key, ["/usr/bin/python3", "-c", f"pass  # {key[:4]}"])
    _failed(queue_root, KEY_SLURM, checkout=checkout,
            transport="slurm", state="TIMEOUT")
    _failed(queue_root, KEY_POOL, checkout=checkout, transport=None)
    return {"queue": pool.PoolQueue(queue_root), "queue_root": queue_root,
            "cas_root": cas_root, "checkout": checkout}


def _plan_for(plans: list[dict], key: str) -> dict:
    found = [plan for plan in plans if plan["key"] == key]
    assert found, f"{key[:12]} was not planned"
    return found[0]


def test_a_lane_filed_timeout_is_resubmitted_through_the_lane(fleet) -> None:
    """The record says which dispatcher carried it, so the reset can too.

    A ``TIMEOUT`` is the case that matters most: it is a failure the pull queue
    would have retried, it lands in ``failed/`` like every other ending, and
    re-submitting it into a queue with no workers wastes the operator's whole
    reset.
    """

    plans, skipped = pool_reset.plan_resets(
        fleet["queue"], cas_root=fleet["cas_root"])
    assert not skipped, skipped
    plan = _plan_for(plans, KEY_SLURM)
    assert plan["transport"] == "slurm"
    command = pool_reset.submit_command(plan, transport=plan["transport"])
    assert "--transport" in command
    assert command[command.index("--transport") + 1] == "slurm"


def test_a_pull_queue_failure_stays_on_the_pull_queue(fleet, monkeypatch) -> None:
    """Even when the operator's shell has already been cut over.

    The transport is a property of the record, not of the environment: an
    ambient ``PRISMABUILD_TRANSPORT`` must not silently re-route work whose
    ending the pull queue filed, so the child's transport is always stated.
    """

    monkeypatch.setenv("PRISMABUILD_TRANSPORT", "")
    plans, _ = pool_reset.plan_resets(fleet["queue"], cas_root=fleet["cas_root"])
    plan = _plan_for(plans, KEY_POOL)
    assert plan["transport"] == "pool"
    command = pool_reset.submit_command(plan, transport=plan["transport"])
    assert command[command.index("--transport") + 1] == "pool"


def test_asking_for_slurm_carries_every_reset_onto_the_lane(fleet) -> None:
    """``--transport slurm`` is the cutover switch for a bulk reset."""

    plans, _ = pool_reset.plan_resets(
        fleet["queue"], cas_root=fleet["cas_root"], transport="slurm")
    assert {plan["transport"] for plan in plans} == {"slurm"}


def test_a_withdrawal_of_this_generation_is_never_resubmitted(fleet) -> None:
    """The marker, not just the stamp on the record.

    A cancellation reaches ``failed/`` two ways -- the lane files the ending
    and ``--withdraw`` files the marker -- and only the marker is guaranteed to
    be there.  Re-submitting past it undoes an operator's decision.
    """

    marker = fleet["queue_root"] / pool.WITHDRAWN / f"{KEY_SLURM}.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({
        "action_key": KEY_SLURM, "status": "withdrawn",
        "published_unix": 1000.0, "withdrawn_unix": 1001.0,
        "withdrawn_by": "rob@sparky", "reason": "wrong shard",
    }), encoding="utf-8")

    plans, skipped = pool_reset.plan_resets(
        fleet["queue"], cas_root=fleet["cas_root"])
    assert [plan["key"] for plan in plans] == [KEY_POOL]
    assert any("withdrawn by rob@sparky" in why for _, why in skipped)


def test_a_withdrawal_of_another_generation_does_not_blacklist_the_key(
    fleet,
) -> None:
    """An action key is a content hash; the same key is how work is re-asked for.

    ``PoolQueue.withdrawal_covers`` scopes the guard to ``published_unix`` for
    exactly this reason, and reading the directory by filename alone -- which
    is what this tool did -- turns one cancellation into a permanent ban.
    """

    marker = fleet["queue_root"] / pool.WITHDRAWN / f"{KEY_SLURM}.json"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({
        "action_key": KEY_SLURM, "status": "withdrawn",
        "published_unix": 12.0, "withdrawn_unix": 13.0,
        "withdrawn_by": "rob@sparky",
    }), encoding="utf-8")

    plans, _ = pool_reset.plan_resets(
        fleet["queue"], cas_root=fleet["cas_root"])
    assert KEY_SLURM in {plan["key"] for plan in plans}


KEY_EXCLUSIVE = "c" * 64


def _exclusive_failure(fleet: dict, *, gres: str) -> dict:
    """One lane-filed failure whose job asked for the device by that GRES."""

    _cas_request(fleet["cas_root"], KEY_EXCLUSIVE, ["/usr/bin/python3", "bench.py"])
    _failed(
        fleet["queue_root"], KEY_EXCLUSIVE, checkout=fleet["checkout"],
        transport="slurm", resources={"cpu": 8, "gpu": 1, "mem_gb": 16},
        gres=gres,
    )
    plans, _ = pool_reset.plan_resets(fleet["queue"], cas_root=fleet["cas_root"])
    return _plan_for(plans, KEY_EXCLUSIVE)


def test_an_exclusive_failure_is_resubmitted_exclusive(fleet) -> None:
    """The demand alone cannot say it, so the reset would quietly downgrade it.

    ``LaneResources.demand()`` records ``{"gpu": 1}`` for an action that had
    the whole device, which is byte for byte what a one-slot action records.
    Rebuilt as ``--demand gpu=1`` with no ``--exclusive``, that becomes
    ``shard:1`` -- a sharable slot -- so a timing run that failed while it
    owned the GPU is retried beside other work.
    """

    plan = _exclusive_failure(fleet, gres="gpu:1")
    assert plan["exclusive"] is True
    command = pool_reset.submit_command(plan, transport="slurm")
    assert "--exclusive" in command
    # The demand still travels: pbrun reads both, and the demand carries the
    # CPU and memory the exclusive flag says nothing about.
    assert command[command.index("--demand") + 1] == "cpu=8,gpu=1,mem_gb=16"


def test_a_shard_failure_is_not_promoted_to_the_whole_device(fleet) -> None:
    """The other half of the same distinction, and the more expensive mistake.

    Re-submitting a one-slot action as exclusive takes a GB10 away from
    everything else on it, for work that never asked for that.
    """

    plan = _exclusive_failure(fleet, gres="shard:1")
    assert plan["exclusive"] is False
    assert "--exclusive" not in pool_reset.submit_command(plan, transport="slurm")


def test_a_pull_queue_failure_names_no_gres_and_is_not_exclusive(fleet) -> None:
    """A record with no ``detail.slurm`` at all reads as not exclusive."""

    plans, _ = pool_reset.plan_resets(fleet["queue"], cas_root=fleet["cas_root"])
    plan = _plan_for(plans, KEY_POOL)
    assert plan["exclusive"] is False
    assert "--exclusive" not in pool_reset.submit_command(plan, transport="pool")


def test_a_reset_imposes_no_deadline_unless_the_operator_asks(fleet) -> None:
    """Elapsed time is not evidence that a worker is dead.

    The tool hardcoded ``--timeout-s 5400``, which the pull queue never
    enforced and which SLURM turns into ``--time`` and an enforced kill at
    ninety minutes.  A reset of a long export would therefore have killed
    every re-submission at the same wall-clock the original never had.
    """

    plans, _ = pool_reset.plan_resets(fleet["queue"], cas_root=fleet["cas_root"])
    plan = _plan_for(plans, KEY_SLURM)
    assert "--timeout-s" not in pool_reset.submit_command(plan, transport="slurm")
    asked = pool_reset.submit_command(plan, transport="slurm", timeout_s=600.0)
    assert asked[asked.index("--timeout-s") + 1] == "600.0"


def test_the_command_line_default_sends_no_deadline_either(fleet, capsys) -> None:
    """Read through ``main``, because the default an operator meets is the
    parser's, and a test that recomputed the expression would only agree with
    itself."""

    code = pool_reset.main([
        "--queue-root", str(fleet["queue_root"]),
        "--cas-root", str(fleet["cas_root"]),
    ])
    assert code == 0
    printed = capsys.readouterr().out
    assert "would submit" in printed
    assert "--timeout-s" not in printed


# --------------------------------------------------------------------------
# Resetting a sealed action: the same action, not a new one
# --------------------------------------------------------------------------

def _sealed_failure(
    tmp_path: Path, *, gres: str = "gpu:1", tags: tuple[str, ...] = ("gb10",),
    seed: str = "",
) -> dict:
    """A lane-filed failure whose action carries a real sealed snapshot.

    Built through the same helpers the lane's own tests use, so the action in
    the CAS is one ``slurm_job`` could actually materialize rather than a
    hand-written stand-in for one.
    """

    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    # ``seed`` gives a second failure its own source tree and its own
    # environment, so it is a distinct action key, while both records share
    # one queue and one store.
    source_root = tmp_path / seed if seed else tmp_path
    source_root.mkdir(parents=True, exist_ok=True)
    action = _runnable_action(source_root, cas, owner=seed)
    cas.publish_action_request(action)
    key = str(action["action_key"])
    queue_root = tmp_path / "pb-queue"
    record = {
        "schema": "prismaquant.prismabuild.slurm_outcome.v1",
        "transport": "slurm",
        "action_key": key,
        "published_unix": 1000.0,
        "status": "failed",
        "attempts": 1,
        "max_attempts": 1,
        "retry_safe": False,
        "resources": {"cpu": 8, "gpu": 1, "mem_gb": 16},
        "tags": list(tags),
        "claimed_by": "4242",
        "claimed_host": None,
        "finished_host": None,
        "checkout_snapshot": action["params"]["checkout_snapshot"],
        "detail": {"status": "failed", "returncode": 1, "elapsed_s": None,
                   "slurm": {"job_id": "4242", "state": "FAILED",
                             "gres": gres}},
    }
    path = queue_root / pool.FAILED / f"{key}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record), encoding="utf-8")
    return {"key": key, "action": action, "cas_root": tmp_path / "cas",
            "queue_root": queue_root, "queue": pool.PoolQueue(queue_root),
            "record_path": path}


def test_a_sealed_failure_is_planned_as_a_resubmission_of_itself(
    tmp_path: Path,
) -> None:
    """The defect: every lane failure from a sealing producer was unresettable.

    ``_recover`` looked for an absolute ``checkout_root``, which a sealed
    action never carries, and skipped. There is nothing to recover: the action
    names its own tree by commit, so the plan is the action.
    """

    sealed = _sealed_failure(tmp_path)
    plans, skipped = pool_reset.plan_resets(
        sealed["queue"], cas_root=sealed["cas_root"], transport="slurm")
    assert not skipped, skipped
    plan = _plan_for(plans, sealed["key"])
    assert plan["mode"] == "resubmit"
    assert plan["cwd"] is None
    assert plan["exclusive"] is True
    assert plan["transport"] == "slurm"
    assert plan["request_path"] == str(
        sealed["cas_root"] / "requests" / sealed["key"][:2]
        / f"{sealed['key']}.json")


def test_a_sealed_failure_whose_key_already_holds_a_receipt_is_not_resubmitted(
    tmp_path: Path, slurm_fleet: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One key is submitted again every time the same work is asked for, and
    a later generation can land a receipt while an older generation's
    ``failed/`` record is still on disk.  Pre-fix ``_recover`` never asked the
    CAS, so a bulk reset spent a job on every such key to be told
    ``cache_hit``; the path-addressed half already refuses on a receipt
    through ``repair_local_result``, and the sealed half now says the same."""

    sealed = _sealed_failure(tmp_path)
    cas = pb.PrismaBuildCAS(sealed["cas_root"])
    monkeypatch.setenv("FAKE_SBATCH_VERDICT", "run")
    landed = sl.run(
        sealed["action"], cas=cas,
        request_path=cas.publish_action_request(sealed["action"]),
        resources=sl.LaneResources(cpus=1, memory_mib=512), timeout_s=600.0,
        worker_script=WORKER, job_entry=JOB_ENTRY, poll_s=0.0,
    )
    assert landed.receipt is not None
    assert cas.lookup(sealed["action"]) is not None

    plans, skipped = pool_reset.plan_resets(
        sealed["queue"], cas_root=sealed["cas_root"], transport="slurm")
    assert not [plan for plan in plans if plan["key"] == sealed["key"]]
    assert skipped == [(sealed["key"][:12], "already has a CAS receipt; the work landed")]


def test_a_sealed_reset_submits_the_same_action_key_and_the_same_device(
    tmp_path: Path, slurm_fleet: Path,
) -> None:
    """The two things a reset must not change about sealed work.

    The key, because it is the memoization: re-sealing the work would mint a
    different hash for the same bytes and make an already-published receipt
    unfindable. And the device request, because ``{"gpu": 1}`` alone re-emits
    as ``shard:1`` and puts a job that owned the GPU beside other work.
    """

    sealed = _sealed_failure(tmp_path)
    plans, _ = pool_reset.plan_resets(
        sealed["queue"], cas_root=sealed["cas_root"], transport="slurm")
    job = pool_reset.resubmit_sealed(
        _plan_for(plans, sealed["key"]),
        cas_root=sealed["cas_root"], queue_root=sealed["queue_root"])

    assert job is not None
    assert job.action_key == sealed["key"]
    rows = _submissions(slurm_fleet)
    assert len(rows) == 1
    argv = rows[0]["argv"]
    assert f"--job-name=pb-{sealed['key'][:12]}" in argv
    assert "--gres=gpu:1" in argv
    assert "--constraint=gb10" in argv
    assert "--partition=gpu" in argv
    assert "--mem=16384M" in argv
    assert "--cpus-per-task=8" in argv
    # Behind interactive work, the way the pull-queue half is: the lane turns
    # the priority into the nice the controller subtracts.
    assert f"--nice={sl.nice_for(-10)}" in argv
    # No deadline unless the operator asked for one; the original had none.
    assert not [flag for flag in argv if flag.startswith("--time=")]

    # The action the job will read is the one that failed, snapshot and all:
    # the script names the request the lane was given, and that file still
    # carries the sealed checkout.
    script = Path(
        [flag.split("=", 1)[1] for flag in argv if flag.startswith("--chdir=")][0]
    ) / "job.sh"
    request = Path(
        script.read_text(encoding="utf-8").split("--action ", 1)[1].split()[0])
    assert request == (
        sealed["cas_root"] / "requests" / sealed["key"][:2]
        / f"{sealed['key']}.json")
    assert json.loads(request.read_text(encoding="utf-8"))["params"][
        "checkout_snapshot"] == sealed["action"]["params"]["checkout_snapshot"]


def test_a_shard_failure_is_not_resubmitted_as_the_whole_device(
    tmp_path: Path, slurm_fleet: Path,
) -> None:
    """The same distinction, read out of the record the lane filed."""

    sealed = _sealed_failure(tmp_path, gres="shard:1")
    plans, _ = pool_reset.plan_resets(
        sealed["queue"], cas_root=sealed["cas_root"], transport="slurm")
    pool_reset.resubmit_sealed(
        _plan_for(plans, sealed["key"]),
        cas_root=sealed["cas_root"], queue_root=sealed["queue_root"])
    assert "--gres=shard:1" in _submissions(slurm_fleet)[0]["argv"]


def test_a_sealed_reset_leaves_its_ending_to_pbwait(
    tmp_path: Path, slurm_fleet: Path, capsys: pytest.CaptureFixture,
) -> None:
    """It detaches, so it must not file a verdict it has not observed.

    What it leaves instead is the submission record ``pbwait`` reconstructs
    the ending from, and the key and job id an operator hands ``pbwait``.  The
    failed record is marked ``reset`` so the queue's failure count still means
    something.
    """

    sealed = _sealed_failure(tmp_path)
    code = pool_reset.main([
        "--apply",
        "--queue-root", str(sealed["queue_root"]),
        "--cas-root", str(sealed["cas_root"]),
    ])
    assert code == 0
    out = capsys.readouterr().out
    assert f"resubmitted {sealed['key'][:12]}" in out
    assert "as slurm job" in out
    assert f"pbwait.py {sealed['key'][:12]}" in out

    # No ending, and no second failure record: the ending is pbwait's.
    assert not (sealed["queue_root"] / pool.DONE).exists()
    assert json.loads(
        sealed["record_path"].read_text())["status"] == "reset"
    # And the submission pbwait resolves it from.
    found = sl.resolve_recorded(sealed["key"][:12])
    assert [record["action_key"] for record in found] == [sealed["key"]]


def test_a_sealed_reset_is_reported_before_it_is_applied(
    tmp_path: Path, slurm_fleet: Path, capsys: pytest.CaptureFixture,
) -> None:
    """The default still only reports, and says what it would do differently."""

    sealed = _sealed_failure(tmp_path)
    assert pool_reset.main([
        "--queue-root", str(sealed["queue_root"]),
        "--cas-root", str(sealed["cas_root"]),
    ]) == 0
    out = capsys.readouterr().out
    assert "would resubmit" in out
    assert "the sealed action itself, unchanged" in out
    assert _submissions(slurm_fleet) == []


def test_a_sealed_failure_is_never_carried_onto_the_pull_queue(
    tmp_path: Path,
) -> None:
    """Re-submitting a sealed action is a lane verb, and only a lane verb.

    The pull-queue path here is ``pbrun``, which re-seals against a live tree,
    and a sealed action names no live tree.  Rather than re-seal it into a
    different action key, the reset says so and leaves it.
    """

    sealed = _sealed_failure(tmp_path)
    record = json.loads(sealed["record_path"].read_text())
    del record["transport"]
    record["schema"] = pool.POOL_OUTCOME_SCHEMA_V1
    sealed["record_path"].write_text(json.dumps(record), encoding="utf-8")

    plans, skipped = pool_reset.plan_resets(
        sealed["queue"], cas_root=sealed["cas_root"])
    assert plans == []
    assert any("only the SLURM lane does" in why for _, why in skipped)


def test_a_refused_submission_does_not_abort_the_rest_of_the_reset(
    tmp_path: Path, slurm_fleet: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """``sbatch`` refusing is a capability answer, not a crash.

    An unknown Feature or an impossible GRES is rejected at submit time, and
    the lane raises that in this process -- unlike the ``pbrun`` half, whose
    detached child takes its stderr to ``DEVNULL``.  Unhandled, one bad record
    would end a 120-shard reset with a traceback and leave every later plan
    untouched.  Each refusal is reported against its own record, the record
    stays ``failed`` so the next run can try it again, and the exit status
    tells a wrapping script that something was refused.
    """

    monkeypatch.setenv("FAKE_SBATCH_REFUSE", "1")
    first = _sealed_failure(tmp_path, seed="one")
    second = _sealed_failure(tmp_path, seed="two")
    assert first["key"] != second["key"]

    code = pool_reset.main([
        "--apply",
        "--queue-root", str(first["queue_root"]),
        "--cas-root", str(first["cas_root"]),
    ])
    out = capsys.readouterr().out

    assert code == 1
    for failure in (first, second):
        assert f"refused {failure['key'][:12]}" in out
        assert json.loads(
            failure["record_path"].read_text())["status"] == "failed"
    assert "Requested node configuration is not available" in out
    assert _submissions(slurm_fleet) == []
