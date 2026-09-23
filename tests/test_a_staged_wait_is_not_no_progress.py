"""A consumer blocked on its own staged range is not stuck (#989).

A stage-fed reader waits for a range PrismaBuild has promised it: the mover
for the range is queued or copying.  Before #989 the reader refused at a
constant, and past the phase grace the worker's ``no_progress`` rung killed
it instead.  Either way hours of GPU work ended while the bytes were still
coming.  Now the reader waits as long as the mover is alive, and says so in
a staged-wait record beside its progress report.  The worker checks that
record against the consumer's own dependents (:meth:`PoolQueue.dependent_rows`)
and does not count that time as quiet.  It counts it, and says so, in the
progress observation.

What still ends: a record that names a mover the consumer does not depend
on, a mover that failed under a superseded plan, and a record with a foreign
token.  Those are the same ``no_progress`` ending as before.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import core as pb, pool, progress, residency_plan  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    STAGE_KIND, TIER, _hexkey, _row)
from test_progress_keeps_a_working_action_alive import _claimed, _policy  # noqa: E402

MANIFEST = "d" * 64

#: Declares a staged wait on the mover named in argv, stays quiet past the
#: grace, then commits one unit and exits.  Written without importing
#: PrismaBuild, the way a container writes it.
WAITER = '''
import json, os, sys, time
path = os.environ["PRISMABUILD_ACTION_PROGRESS_PATH"]
token = os.environ["PRISMABUILD_ACTION_PROGRESS_TOKEN"]
mover, seconds, forged = sys.argv[1], float(sys.argv[2]), sys.argv[3] == "forged"
record = {"schema": "prismabuild.staged_wait.v1",
          "token": "0" * 32 if forged else token,
          "since_unix": time.time(), "movers": [mover]}
tmp = path + ".staged-wait.tmp"
with open(tmp, "w") as handle:
    json.dump(record, handle)
os.replace(tmp, path + ".staged-wait")
time.sleep(seconds)
os.unlink(path + ".staged-wait")
report = {"schema": "prismabuild.action_progress.v1", "token": token,
          "phase": "run", "units_completed": 1, "reported_unix": time.time()}
with open(path + ".tmp", "w") as handle:
    json.dump(report, handle)
os.replace(path + ".tmp", path)
open("result", "w").write("ok")
'''


def _consumer_with_mover(tmp_path: Path, *, mover_seed: str, forged: bool = False,
                         seconds: float = 1.5, declared_seed: str | None = None):
    """A claimed consumer whose plan names one stage mover, and that mover.

    The staged-wait record names ``declared_seed``'s key, which is the
    plan's own mover unless a test says otherwise.
    """

    mover = _hexkey(mover_seed)
    declared = _hexkey(declared_seed or mover_seed)
    source = WAITER.replace("sys.argv[1]", repr(declared)).replace(
        "float(sys.argv[2])", repr(seconds)).replace(
        "sys.argv[3] == \"forged\"", repr(forged))
    queue, item = _claimed(tmp_path, mode="waiter", seconds=seconds,
                           policy=_policy(0.4, 0.4, 0.4), source=source)
    consumer = str(item["action_key"])
    size = 22 * 10 ** 9
    row = {**_row(queue, mover, {STAGE_KIND: 21, "cpu": 2, "mem_gb": 1}),
           "residency": {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                         "manifest_sha256": MANIFEST, "manifest_bytes": size,
                         "range_start_bytes": 0, "range_end_bytes": size}}
    plan = residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=size, phases=[{
            "name": "chain-043", "start_bytes": 0, "end_bytes": size,
            "stage_gib": 21, "mover_row": row,
            "egress_row": _row(queue, _hexkey(mover_seed + "egress"),
                               {"mem_gb": 1})}])
    residency_plan.freeze(queue, plan)
    return queue, item, plan, row


def _hand_to_claimed(queue: pool.PoolQueue, row) -> None:
    queue.publish(**dict(row))
    source = queue.item_path(pool.READY, str(row["action_key"]))
    record = json.loads(source.read_text())
    source.unlink()
    record.update({"claimed_unix": 1.0, "claimed_by": "copy-fixture",
                   "claimed_host": "dl380g10"})
    queue.item_path(pool.CLAIMED, str(row["action_key"])).write_text(
        json.dumps(record))


def _execute(queue, item):
    return queue.execute(item, timeout_s=None, heartbeat_s=0.05,
                         timeout_grace_s=0.2)


def test_a_consumer_waiting_on_its_claimed_mover_is_not_killed_no_progress(
        tmp_path: Path) -> None:
    """The copy is slower than every earlier receipt, and the mover is alive.

    Quiet for 1.5 s against a 0.4 s grace, all of it blocked on the claimed
    mover for its own range.  On main the rung kills it at 0.4 s.
    """

    queue, item, _plan, row = _consumer_with_mover(tmp_path, mover_seed="slow")
    _hand_to_claimed(queue, row)

    outcome = _execute(queue, item)

    assert outcome["status"] == "executed", repr(outcome.get("termination_reason"))
    observed = outcome["progress_observation"]
    assert observed["staged_wait_exempt_s"] > 0.4
    wait = observed["staged_wait"]
    assert wait["exempt"] is True
    assert wait["movers"] == [{"key": str(row["action_key"]), "state": "claimed"}]


def test_a_ready_mover_is_waited_on_as_well(tmp_path: Path) -> None:
    queue, item, _plan, row = _consumer_with_mover(tmp_path, mover_seed="queued")
    queue.publish(**dict(row))

    outcome = _execute(queue, item)

    assert outcome["status"] == "executed"
    assert outcome["progress_observation"]["staged_wait"]["movers"][0][
        "state"] == "ready"


def test_a_mover_that_failed_under_a_superseded_plan_is_not_waited_on(
        tmp_path: Path) -> None:
    """Nothing will republish it, so the wait is not a dependency wait."""

    queue, item, plan, row = _consumer_with_mover(tmp_path, mover_seed="dead")
    key = str(row["action_key"])
    queue.item_path(pool.FAILED, key).parent.mkdir(parents=True, exist_ok=True)
    queue.item_path(pool.FAILED, key).write_text(json.dumps(
        {**dict(row), "status": "failed"}))
    residency_plan.mark_superseded(
        queue, str(item["action_key"]), plan=plan,
        filing=residency_plan.read_filed(queue, str(item["action_key"]))[1],
        reason="mover-withdrawn", movers=[key], by="test")

    outcome = _execute(queue, item)

    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    wait = outcome["progress_observation"]["staged_wait"]
    assert wait["exempt"] is False
    assert wait["movers"] == [{"key": key, "state": "failed"}]


@pytest.mark.parametrize("seed,forged", [("stranger", False), ("forged", True)])
def test_a_wait_on_someone_elses_mover_or_a_forged_record_still_ends(
        tmp_path: Path, seed: str, forged: bool) -> None:
    """A claimed mover the plan never named, or a token this launch never minted."""

    queue, item, _plan, row = _consumer_with_mover(
        tmp_path, mover_seed=seed, forged=forged,
        declared_seed=None if forged else "not-" + seed)
    _hand_to_claimed(queue, row)
    if not forged:
        _hand_to_claimed(queue, {**dict(row), "action_key": _hexkey("not-" + seed)})

    outcome = _execute(queue, item)

    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    assert outcome["progress_observation"]["staged_wait_exempt_s"] == 0


def test_the_helper_writes_the_record_the_worker_reads(tmp_path: Path,
                                                       monkeypatch) -> None:
    path = tmp_path / "x.progress"
    monkeypatch.setenv(progress.ACTION_PROGRESS_PATH_ENV, str(path))
    monkeypatch.setenv(progress.ACTION_PROGRESS_TOKEN_ENV, "t" * 32)
    assert progress.declare_staged_wait(["a" * 64], since_unix=5.0) is True
    record = json.loads(Path(progress.staged_wait_path(str(path))).read_text())
    assert record == {"schema": progress.STAGED_WAIT_SCHEMA_V1,
                      "token": "t" * 32, "since_unix": 5.0, "movers": ["a" * 64]}
    assert pool.read_staged_wait(Path(progress.staged_wait_path(str(path))),
                                 token="t" * 32)["movers"] == ["a" * 64]
    assert progress.clear_staged_wait() is True
    assert not Path(progress.staged_wait_path(str(path))).exists()
