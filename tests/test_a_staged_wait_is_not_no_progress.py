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

import inspect
import json
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import (  # noqa: E402
    core as pb, movement_actions, pool, progress, residency_map, residency_plan)
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
    mover for its own range.  On main the rung kills it at 0.4 s.  The
    mover's landed-bytes report is fresh (within two heartbeats), which is
    the evidence the exemption now needs (#1022 review, item 1).
    """

    queue, item, _plan, row = _consumer_with_mover(tmp_path, mover_seed="slow")
    _hand_to_claimed(queue, row)
    _mover_reports(queue, row, units=1 << 30, reported_unix=time.time())

    outcome = _execute(queue, item)

    assert outcome["status"] == "executed", repr(outcome.get("termination_reason"))
    observed = outcome["progress_observation"]
    assert observed["staged_wait_exempt_s"] > 0.4
    wait = observed["staged_wait"]
    assert wait["exempt"] is True
    (mover,) = wait["movers"]
    assert (mover["key"], mover["state"], mover["evidence"]) == (
        str(row["action_key"]), "claimed", "progress")


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
                                 token="t" * 32)[0]["movers"] == ["a" * 64]
    assert progress.clear_staged_wait() is True
    assert not Path(progress.staged_wait_path(str(path))).exists()


# -- the verdict itself: finished movers and an over-committed tier ----------
#
# PR #1009 review.  A mover in ``done/`` or ``withdrawn`` is not coming, so a
# consumer still quiet after it is not waiting on a dependency.  And a range
# the window has not published is only coming while the tier can fit it: on
# an over-committed tier (#1011) the claimed consumers wait on each other,
# and exempting that wait would turn it into a deadlock.


TOKEN = "t" * 32


def _verdict_fixture(tmp_path: Path, *, over_committed_gib: int | None):
    """A claimed consumer's plan, a live tier loop, the tier's commitment
    record, and a staged-wait record naming the plan's mover."""

    queue, item, plan, row = _consumer_with_mover(tmp_path, mover_seed="verdict")
    queue.announce_tier({"tier_id": TIER, "tier": "stage"})
    if over_committed_gib is not None:
        queue.file_tier_commitment({
            "tier_id": TIER, "capacity_gib": 565,
            "committed_gib": 565 + over_committed_gib,
            "over_committed_gib": over_committed_gib})
    progress_path = tmp_path / "consumer.progress"
    Path(progress.staged_wait_path(str(progress_path))).write_text(json.dumps({
        "schema": progress.STAGED_WAIT_SCHEMA_V1, "token": TOKEN,
        "since_unix": 1.0, "movers": [str(row["action_key"])]}))
    return queue, item, row, progress_path


def _verdict(queue, item, progress_path):
    return queue.staged_wait_verdict(str(item["action_key"]), progress_path,
                                     token=TOKEN)


def _finish(queue: pool.PoolQueue, row, state: str) -> str:
    key = str(row["action_key"])
    queue.item_path(state, key).parent.mkdir(parents=True, exist_ok=True)
    queue.item_path(state, key).write_text(json.dumps({**dict(row), "status": state}))
    return key


def test_a_landed_mover_is_not_waited_on(tmp_path: Path) -> None:
    """In ``done/`` and still holding its tokens: the range is resident (or
    being adopted), so a consumer still quiet is not waiting on it -- a
    reader that hung with a stale record, say.  Plan live, loop alive, tier
    within commitment."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    key = _finish(queue, row, pool.DONE)
    queue.mint_tier_capacity(TIER, {STAGE_KIND: 21})
    assert queue.tier_ledger(TIER).acquire(key, {STAGE_KIND: 21})

    verdict = _verdict(queue, item, progress_path)

    assert verdict["exempt"] is False
    assert verdict["movers"] == [{"key": key, "state": "done"}]


@pytest.mark.parametrize("over,exempt", [(0, True), (161, False)])
def test_an_evicted_range_is_waited_on_like_an_unpublished_one(
        tmp_path: Path, over: int, exempt: bool) -> None:
    """In ``done/`` with no tokens: evicted, and the window publishes it
    again (``tier_loop.evict_beyond_horizon``) -- while the tier fits it."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=over)
    key = _finish(queue, row, pool.DONE)

    verdict = _verdict(queue, item, progress_path)

    assert verdict["movers"] == [{"key": key, "state": "evicted"}]
    assert verdict["exempt"] is exempt
    assert verdict["tier_over_committed_gib"] == over


def test_a_withdrawn_mover_is_not_waited_on(tmp_path: Path) -> None:
    """The window never republishes a withdrawn mover: its publish passes
    ``refuse_withdrawn`` and the refusal supersedes the plan (#708)."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    key = _finish(queue, row, pool.WITHDRAWN)

    verdict = _verdict(queue, item, progress_path)

    assert verdict["exempt"] is False
    assert verdict["movers"] == [{"key": key, "state": "withdrawn"}]


@pytest.mark.parametrize("over,exempt", [(0, True), (161, False), (None, False)])
def test_an_unpublished_range_is_exempt_only_on_a_tier_within_commitment(
        tmp_path: Path, over: int | None, exempt: bool) -> None:
    """The same unpublished range, the same live tier loop: only the tier's
    filed commitment differs.  No record filed reads as not exempt."""

    queue, item, _row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=over)

    verdict = _verdict(queue, item, progress_path)

    assert verdict["movers"][0]["state"] == "unpublished"
    assert verdict["exempt"] is exempt
    assert verdict["tier_over_committed_gib"] == over


def test_a_queued_mover_stays_exempt_on_an_over_committed_tier(
        tmp_path: Path) -> None:
    """A published mover already holds its room."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=161)
    queue.publish(**dict(row))

    verdict = _verdict(queue, item, progress_path)

    assert verdict["exempt"] is True
    assert verdict["movers"][0]["state"] == "ready"


# -- a queued mover is waited on only on evidence (#1011 review, item 1) -----
#
# A ``ready`` or ``claimed`` mover used to exempt its consumer with no clock:
# a row the claim pass can never place, or one it withholds, held a GPU
# consumer for as long as the row sat there.  The exemption now holds only
# while the mover shows progress: the bytes queued ahead of it in the
# consumer's landing record fell (``bytes_ahead``), or its own progress
# report grew within two heartbeats.  A refused or withheld row is not
# exempt at all.  The verdict records the evidence it went on.


def _judge(queue, item, progress_path, **evidence):
    """``_verdict`` plus the evidence arguments the review added, passed only
    where the method takes them, so a head without them fails on the
    assertion and not on the call."""

    accepted = inspect.signature(queue.staged_wait_verdict).parameters
    return queue.staged_wait_verdict(
        str(item["action_key"]), progress_path, token=TOKEN,
        **{name: value for name, value in evidence.items() if name in accepted})


def _landing_ahead(queue: pool.PoolQueue, item, row, *, bytes_ahead: int,
                   movers_ahead: list[str] | None = None) -> None:
    """The consumer's landing record, the tier loop's way: its one queued
    range ``bytes_ahead`` behind the movers ahead of it (``movers_ahead``,
    left out when ``None``).  Written as the tier loop's JSON, not through
    ``write_landing``, so a head whose schema lacks a field reads the record
    as unreadable -- no evidence -- rather than failing the fixture."""

    residency = row["residency"]
    queued: dict[str, object] = {
        "mover_action_key": str(row["action_key"]), "phase": "chain-043",
        "range_start_bytes": int(residency["range_start_bytes"]),
        "range_end_bytes": int(residency["range_end_bytes"]),
        "state": "ready", "queue_position": len(movers_ahead or ()),
        "bytes_ahead": int(bytes_ahead),
        "expected_landing_unix": time.time() + 600.0,
        "claimed_unix": None, "waiting_for": "", "basis": "queue"}
    if movers_ahead is not None:
        queued["movers_ahead"] = list(movers_ahead)
    path = residency_map.landing_path(queue.residency_fragment_root(),
                                      str(item["action_key"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "schema": residency_map.RESIDENCY_LANDING_SCHEMA_V1,
        "consumer_action_key": str(item["action_key"]), "tier_id": TIER,
        "manifest_sha256": MANIFEST, "written_unix": time.time(),
        "rates_measured_bytes_per_s": [], "ranges": [queued]}))


def _mover_reports(queue: pool.PoolQueue, row, *, units: int,
                   reported_unix: float) -> None:
    """The claimed mover's own landed-bytes report (#1010), as
    ``stage_move._ProgressReporter`` commits it: only when it grew."""

    path = queue.action_progress_path(str(row["action_key"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "schema": pb.PROGRESS_RECORD_SCHEMA_V1, "token": "m" * 32,
        "phase": movement_actions.MOVER_COPY_PHASE, "units_completed": units,
        "reported_unix": reported_unix}))


@pytest.mark.parametrize("reason,kind", [
    ("never_fits_tier_capacity", "refusal"),
    ("tier_unknown", "refusal"),
    # The #538 shape: a ``cpus=`` demand no box's census can place.
    ("never_fits_capacity", "refusal"),
])
def test_a_ready_mover_the_claim_pass_refuses_is_not_waited_on(
        tmp_path: Path, reason: str, kind: str) -> None:
    """Queued, and the claim pass's latest word on it is a refusal: nothing
    will copy it while that stands, so the consumer's wait on it is not a
    dependency wait.  Tier within commitment, loop alive.  A withhold is the
    opposite, and has its own tests below (#1022 review round 3, F1)."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    queue.publish(**dict(row))
    ready = json.loads(queue.item_path(pool.READY, str(row["action_key"])).read_text())
    queue._record_denial_transition(ready, host="dl380g10", reason=reason,
                                    decision_reason=None)

    verdict = _verdict(queue, item, progress_path)

    assert verdict["exempt"] is False, verdict
    mover = verdict["movers"][0]
    assert mover["state"] == "ready"
    assert mover.get(kind) == reason, mover


def test_a_ready_mover_is_waited_on_while_the_bytes_ahead_of_it_fall(
        tmp_path: Path) -> None:
    """The first look is the baseline; a fall in ``bytes_ahead`` renews it;
    a whole evidence window without one ends the exemption."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    queue.publish(**dict(row))
    window = 600.0
    gib = 1 << 30
    _landing_ahead(queue, item, row, bytes_ahead=100 * gib)
    first = _judge(queue, item, progress_path, prior=None, window_s=window)

    _landing_ahead(queue, item, row, bytes_ahead=40 * gib)
    fell = _judge(queue, item, progress_path, prior=first, window_s=window,
                  now=time.time() + 30.0)

    stale = _judge(queue, item, progress_path, prior=fell, window_s=window,
                   now=time.time() + 30.0 + window + 1.0)

    assert first["exempt"] is True, first
    assert first["movers"][0].get("evidence") == "baseline", first
    assert fell["exempt"] is True, fell
    assert fell["movers"][0].get("evidence") == "bytes-ahead-fell", fell
    assert fell["movers"][0].get("bytes_ahead") == 40 * gib, fell
    assert stale["exempt"] is False, stale
    assert stale["movers"][0].get("evidence") == "none", stale


def test_a_claimed_mover_is_waited_on_while_its_report_grows(
        tmp_path: Path) -> None:
    """Claimed, copying: exempt while its landed-bytes report is at most two
    heartbeats old, and not after."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    _hand_to_claimed(queue, row)
    now = time.time()
    _mover_reports(queue, row, units=5 << 30, reported_unix=now - 10.0)
    fresh = _judge(queue, item, progress_path, now=now)

    _mover_reports(queue, row, units=5 << 30,
                   reported_unix=now - 2 * pool.HEARTBEAT_S - 1.0)
    old = _judge(queue, item, progress_path, now=now)

    assert fresh["exempt"] is True, fresh
    assert fresh["movers"][0].get("evidence") == "progress", fresh
    assert fresh["movers"][0].get("progress_units") == 5 << 30, fresh
    assert old["exempt"] is False, old
    assert old["movers"][0].get("evidence") == "none", old


# -- a hold stops the report, not the lease (#1022 review round 2) -----------
#
# The disk pacer holds a copy while the pool is over its caps.  Nothing lands,
# so the mover's report (committed only when landed bytes grow) and the
# landing record's ``bytes_ahead`` (rewritten only when the queue changes)
# both freeze, for as long as the hold lasts.  The worker running the mover
# keeps heartbeating its lease, credits the hold on evidence it samples
# itself, and ends the copy if it stalls (#1010).  The verdict reads that
# lease, so a consumer waiting behind a held copy waits with it.  The
# campaign's phase grace is 900 s for a chunk; the tests hold for three.

GRACE_S = 900.0


def _other_claimed_copy(queue: pool.PoolQueue, seed: str) -> str:
    """Another consumer's mover on the tier, claimed and copying."""

    row = {**_row(queue, _hexkey(seed), {STAGE_KIND: 21, "cpu": 2, "mem_gb": 1}),
           "residency": {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                         "manifest_sha256": "e" * 64, "manifest_bytes": 22 * 10 ** 9,
                         "range_start_bytes": 0, "range_end_bytes": 22 * 10 ** 9}}
    _hand_to_claimed(queue, row)
    return str(row["action_key"])


def _mover_lease(queue: pool.PoolQueue, mover: str, *, heartbeat_unix: float,
                 held_s: float) -> None:
    """The lease the worker running ``mover`` writes every heartbeat, with
    the pacer hold it has credited so far (``ProgressWatch.as_record``).
    Written directly: ``write_lease`` is the claiming worker's, and checks it
    is that worker.  ``_hand_to_claimed`` claims at 1.0."""

    queue.lease_path(mover).write_text(json.dumps({
        "schema": pool.POOL_LEASE_SCHEMA_V1, "action_key": mover,
        "owner": "copy-fixture", "heartbeat_unix": heartbeat_unix,
        "claimed_unix": 1.0,
        "progress_observation": {
            "source": "action-progress", "accepted_count": 1,
            "quiet_s": held_s, "grace_s": GRACE_S / 3.0,
            "pool_contention_exempt_s": held_s, "start_gate_exempt_s": 0.0}}))


def _hold(queue, item, progress_path, mover: str, *, start: float,
          seconds: float) -> list[dict]:
    """One rung check a heartbeat through a pacer hold ``seconds`` long:
    ``mover``'s lease is refreshed and credits the hold, and nothing else
    moves.  Each check passes the one before as its prior, as the rung does
    (``ProgressWatch.staged_wait``)."""

    verdicts: list[dict] = []
    prior = None
    for step in range(1, int(seconds // pool.HEARTBEAT_S) + 1):
        now = start + step * pool.HEARTBEAT_S
        _mover_lease(queue, mover, heartbeat_unix=now - 1.0,
                     held_s=step * pool.HEARTBEAT_S)
        prior = _judge(queue, item, progress_path, prior=prior,
                       window_s=GRACE_S, now=now)
        verdicts.append(prior)
    return verdicts


def test_a_claimed_mover_the_pacer_holds_keeps_its_consumer_waiting(
        tmp_path: Path) -> None:
    """The consumer's own mover is claimed, landed its first entry, and is
    held for three graces.  Its report is stale after two heartbeats; its
    lease is not.  On the first head the verdict read only the report and
    ended the exemption two heartbeats into the hold."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    _hand_to_claimed(queue, row)
    mover = str(row["action_key"])
    start = time.time()
    _mover_reports(queue, row, units=1 << 30, reported_unix=start)

    verdicts = _hold(queue, item, progress_path, mover, start=start,
                     seconds=3 * GRACE_S)

    ended = [verdict for verdict in verdicts if not verdict["exempt"]]
    assert ended == [], ended[:1]
    last = verdicts[-1]["movers"][0]
    assert last["evidence"] == "lease-live", last
    assert last["hold_credited_s"] == pytest.approx(3 * GRACE_S), last
    assert last["progress_units"] == 1 << 30, last


def test_a_claimed_mover_whose_lease_went_quiet_is_not_waited_on(
        tmp_path: Path) -> None:
    """The report is stale and the lease's heartbeat is older than the bound
    the reaper takes a claim back at (``LEASE_TIMEOUT_S``): nothing says the
    copy's worker is alive, so nothing says it is coming."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    _hand_to_claimed(queue, row)
    mover = str(row["action_key"])
    now = time.time()
    _mover_reports(queue, row, units=1 << 30, reported_unix=now - 3600.0)
    _mover_lease(queue, mover, heartbeat_unix=now - pool.LEASE_TIMEOUT_S - 1.0,
                 held_s=0.0)

    verdict = _judge(queue, item, progress_path, now=now)

    assert verdict["exempt"] is False, verdict
    entry = verdict["movers"][0]
    assert entry["evidence"] == "none", entry
    assert entry.get("lease_live", False) is False, entry


def test_a_ready_mover_behind_a_copy_the_pacer_holds_keeps_its_consumer_waiting(
        tmp_path: Path) -> None:
    """The consumer's mover is ready behind another consumer's copy, which
    the pacer holds for three graces.  The landing record does not change:
    ``bytes_ahead`` is frozen.  On the first head the evidence ran baseline,
    carried, none, and the consumer was not exempt one window into the
    hold."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    queue.publish(**dict(row))
    copy = _other_claimed_copy(queue, "heldcopy")
    _landing_ahead(queue, item, row, bytes_ahead=22 * 10 ** 9, movers_ahead=[copy])

    verdicts = _hold(queue, item, progress_path, copy, start=time.time(),
                     seconds=3 * GRACE_S)

    ended = [verdict for verdict in verdicts if not verdict["exempt"]]
    assert ended == [], ended[:1]
    last = verdicts[-1]["movers"][0]
    assert last["state"] == "ready", last
    assert last["evidence"] == "copy-ahead-live", last
    assert last["copy_ahead"] == copy, last
    assert last["copy_ahead_hold_credited_s"] == pytest.approx(3 * GRACE_S), last
    assert last["waiting_behind"] == [copy], last


def test_a_copy_claimed_after_the_wait_began_does_not_renew_it(
        tmp_path: Path) -> None:
    """Nothing was queued ahead of the ready mover when its wait first
    looked.  Then another row is claimed instead of it -- the claim pass
    placed that one and not this one -- and is copying.  Its lease is live,
    but it was not ahead of this mover, so it says nothing about this one:
    the exemption ends one window after the first look, as before."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    queue.publish(**dict(row))
    start = time.time()
    _landing_ahead(queue, item, row, bytes_ahead=0, movers_ahead=[])
    first = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S,
                   now=start)
    passer = _other_claimed_copy(queue, "passer")
    _landing_ahead(queue, item, row, bytes_ahead=22 * 10 ** 9, movers_ahead=[passer])
    later = start + GRACE_S + 1.0
    _mover_lease(queue, passer, heartbeat_unix=later - 1.0, held_s=0.0)

    verdict = _judge(queue, item, progress_path, prior=first, window_s=GRACE_S,
                     now=later)

    assert first["exempt"] is True, first
    assert verdict["exempt"] is False, verdict
    entry = verdict["movers"][0]
    assert entry["evidence"] == "none", entry
    assert entry.get("waiting_behind", []) == [], entry


# -- a withhold is the pool holding the box for the row (#1022 review round 3)
#
# F1.  A withhold (``*_withholding``) is the claim pass keeping the stage host
# shut for this row while the holders in its way drain (#924): the row is
# next, and the pool bounds the veto by ``WITHHOLD_CEILING_S`` from the
# episode's start (``epoch_unix`` in the row's passes sidecar).  Round 2 read
# it as "not coming", so a withhold that ran toward its ceiling ended a
# healthy consumer whose chunk grace is the same 900 s.  The wait is now
# exempt on the withhold, until the ceiling of its epoch.
#
# ``deferred_behind_withholding`` is not tested here: the claim pass records
# it only for a producer's export (``cpu_admission.dependent_owner`` is
# ``None`` for anything but a ``generation`` action with
# ``params.produced_spool``), so no stage mover can carry it.


def _withheld(queue: pool.PoolQueue, row, reason: str, *,
              epoch_unix: float | None, first_unix: float | None,
              stamp_unix: float | None = None) -> None:
    """The claim pass's word on the ready ``row``: ``reason`` in its denial
    ring, and the passes sidecar ``record_pass`` keeps for it, with the
    withhold episode's start (``epoch_unix``) and the first denial
    (``first_unix``) where given, and the last counted denial
    (``updated_unix``) at ``stamp_unix``, or now."""

    key = str(row["action_key"])
    ready = json.loads(queue.item_path(pool.READY, key).read_text())
    queue._record_denial_transition(ready, host="dl380g10", reason=reason,
                                    decision_reason=None)
    passes: dict[str, object] = {
        "action_key": key, "passes": pool.STARVATION_FLOOR,
        "updated_unix": time.time() if stamp_unix is None else stamp_unix}
    if first_unix is not None:
        passes["first_unix"] = first_unix
    if epoch_unix is not None:
        passes["epoch_unix"] = epoch_unix
    path = queue.passes_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(passes))


@pytest.mark.parametrize("reason", [
    "reservation_unavailable_withholding",
    # The host-PSI shape of PB #985: an adaptive refusal the drain resolves.
    "adaptive_refused_withholding",
])
def test_a_ready_mover_the_claim_pass_withholds_for_is_waited_on(
        tmp_path: Path, reason: str) -> None:
    """F1: the withhold began 600 s ago, inside ``WITHHOLD_CEILING_S``, so
    the pool is still holding the box for this row.  The wait is exempt, the
    entry names the withhold and the host, and its ``evidence_unix`` is the
    withhold's epoch, not the check."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    queue.publish(**dict(row))
    now = time.time()
    epoch = now - 600.0
    _withheld(queue, row, reason, epoch_unix=epoch, first_unix=epoch - 120.0)

    verdict = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S,
                     now=now)

    assert verdict["exempt"] is True, verdict
    entry = verdict["movers"][0]
    assert (entry["state"], entry["evidence"]) == ("ready", "withheld"), entry
    assert (entry.get("withhold"), entry.get("denied_by")) == (reason, "dl380g10"), entry
    assert entry.get("evidence_unix") == epoch, entry
    assert entry.get("withhold_basis") == "episode", entry
    assert entry.get("withhold_ceiling_s") == pool.WITHHOLD_CEILING_S, entry


def test_a_withhold_with_no_episode_is_bounded_by_the_first_denial(
        tmp_path: Path) -> None:
    """F1: a withhold with no episode on file (``in_flight``: the tokens
    are between an acquisition and its rename) is bounded by the row's own
    first-denial clock, the one the pool bounds it by."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    queue.publish(**dict(row))
    now = time.time()
    first = now - 100.0
    _withheld(queue, row, "reservation_unavailable_withholding",
              epoch_unix=None, first_unix=first)

    verdict = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S,
                     now=now)

    assert verdict["exempt"] is True, verdict
    entry = verdict["movers"][0]
    assert entry["evidence"] == "withheld", entry
    assert (entry.get("evidence_unix"), entry.get("withhold_basis")) == (
        first, "first-denial"), entry


def test_a_withhold_on_one_host_outranks_a_refusal_on_another(
        tmp_path: Path) -> None:
    """F1: one box's census can never place the row (``never_fits_capacity``),
    and the stage host withholds its box for it.  The row is coming on the
    stage host, so the consumer waits on it, as it would on a host that says
    nothing but a transient reason."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    queue.publish(**dict(row))
    now = time.time()
    ready = json.loads(queue.item_path(pool.READY, str(row["action_key"])).read_text())
    queue._record_denial_transition(ready, host="sparky", reason="never_fits_capacity",
                                    decision_reason=None)
    _withheld(queue, row, "reservation_unavailable_withholding",
              epoch_unix=now - 60.0, first_unix=now - 120.0)

    verdict = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S,
                     now=now)

    assert verdict["exempt"] is True, verdict
    entry = verdict["movers"][0]
    assert (entry["evidence"], entry.get("denied_by")) == ("withheld", "dl380g10"), entry


@pytest.mark.parametrize("epoch_age_s,first_age_s", [
    # An episode past the ceiling whose last counted denial is older than a
    # claim pass's freshness: no pass has said since that the pool still
    # withholds, so a ring that still says so is not a veto coming to an end.
    # Before #1052 this case had a fresh stamp and asserted the same ruling;
    # a fresh stamp is now the pool's live answer (the next test).
    (pool.WITHHOLD_CEILING_S + 1.0, pool.WITHHOLD_CEILING_S + 60.0),
    # No sidecar at all: nothing bounds the withhold, so it is not waited on.
    (None, None),
], ids=["past-ceiling", "no-sidecar"])
def test_a_withhold_past_its_ceiling_is_not_waited_on(
        tmp_path: Path, epoch_age_s: float | None, first_age_s: float | None) -> None:
    """F1's bound: with no fresh pass on file, exempt only within
    ``WITHHOLD_CEILING_S`` of the epoch."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    queue.publish(**dict(row))
    now = time.time()
    _withheld(queue, row, "reservation_unavailable_withholding",
              epoch_unix=None if epoch_age_s is None else now - epoch_age_s,
              first_unix=None if first_age_s is None else now - first_age_s,
              stamp_unix=now - pool.WITHHOLD_STAMP_FRESH_S - 1.0)
    if epoch_age_s is None:
        queue.passes_path(str(row["action_key"])).unlink()

    verdict = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S,
                     now=now)

    assert verdict["exempt"] is False, verdict
    entry = verdict["movers"][0]
    assert entry["evidence"] == "withhold-lapsed", entry
    assert entry.get("withhold") == "reservation_unavailable_withholding", entry


def test_a_withhold_past_its_ceiling_with_a_fresh_pass_is_waited_on(
        tmp_path: Path) -> None:
    """#1052 flips the old ``past-ceiling`` ruling: the episode is past
    ``WITHHOLD_CEILING_S``, but the claim pass counted a withholding denial
    of the row just now, so the pool still holds the box for it."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    queue.publish(**dict(row))
    now = time.time()
    _withheld(queue, row, "reservation_unavailable_withholding",
              epoch_unix=now - pool.WITHHOLD_CEILING_S - 1.0,
              first_unix=now - pool.WITHHOLD_CEILING_S - 60.0, stamp_unix=now - 5.0)

    verdict = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S,
                     now=now)

    assert verdict["exempt"] is True, verdict
    entry = verdict["movers"][0]
    assert (entry["evidence"], entry.get("withhold_live_by")) == (
        "withheld", "claim-pass"), entry


# -- the pool's live answer is the withhold's bound (#1052)
#
# The pool expires a withhold episode at ``WITHHOLD_CEILING_S`` only when it
# has refills.  With none, a ``drains_soon`` withhold runs for as long as a
# holder stays ``transient``, which a bounded holder does up to its declared
# end.  The consumer read the ceiling from the epoch alone, so it read
# ``withhold-lapsed`` -- and its rung ended it for ``no_progress`` -- while
# the claim pass was still stamping ``*_withholding`` every pass.


class _OneHolderLedger:
    """The stage host's ledger, as ``_withhold_verdict`` reads it: every
    token held by one holder, and none free."""

    def __init__(self, holder: str) -> None:
        self.holder = holder

    def available(self) -> dict[str, int]:
        return {"cpu": 0, "mem_gb": 0}

    def held_keys(self) -> list[str]:
        return [self.holder]

    def holder_tokens(self, holder: str) -> dict[str, int]:
        return {"cpu": 4, "mem_gb": 8} if holder == self.holder else {}

    def holds_gpu(self, holder: str) -> bool:
        return False


def _claim_pass(queue: pool.PoolQueue, row, ledger: _OneHolderLedger) -> dict:
    """What one claim pass does for the mover on a token shortage: judge the
    withhold, count the denial and record its reason (``PoolQueue.claim``)."""

    key = str(row["action_key"])
    ready = json.loads(queue.item_path(pool.READY, key).read_text())
    verdict = queue._withhold_verdict(key, ledger=ledger, need={"cpu": 2, "mem_gb": 1},
                                      mode="tokens")
    queue.record_pass(key)
    reason = ("reservation_unavailable_withholding" if verdict["withhold"]
              else "reservation_unavailable" + pool._starved_suffix(verdict))
    queue.record_denial(ready, reason, {"withhold": verdict})
    return verdict


def _transient_holder_past_the_ceiling(tmp_path: Path, queue: pool.PoolQueue, row,
                                       monkeypatch, now: float):
    """The issue's scenario at t=1060: a measurement claimed at t=0 with a
    25 min timeout holds the stage host, and the mover's episode began at
    t=60, 1000 s ago, with no refill since.  Returns the fake ledger."""

    from test_a_ready_gpu_action_is_not_starved_by_cpu_shards import _sealed

    holder, cas_root, _checkout = _sealed(tmp_path, "measurement", timeout_s=1500)
    claimed = queue.item_path(pool.CLAIMED, holder)
    claimed.parent.mkdir(parents=True, exist_ok=True)
    claimed.write_text(json.dumps({"action_key": holder, "cas_root": cas_root,
                                   "claimed_unix": now - 1060.0}))
    key = str(row["action_key"])
    path = queue.passes_path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "action_key": key, "passes": pool.STARVATION_FLOOR,
        "first_unix": now - 1060.0, "updated_unix": now - 5.0,
        "epoch_unix": now - 1000.0}))
    monkeypatch.setattr(pool, "_now", lambda: now)
    assert queue.holder_bound(holder, now=now)["bound"] == "transient"
    return _OneHolderLedger(holder)


def test_a_withhold_the_pool_still_holds_past_the_ceiling_is_waited_on(
        tmp_path: Path, monkeypatch) -> None:
    """#1052: the episode is 1000 s old and has no refill, and the pool still
    withholds ``drains_soon`` for the mover on this pass.  So the consumer
    waits: its entry reads ``withheld`` on the claim pass's fresh stamp."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    queue.publish(**dict(row))
    now = time.time()
    ledger = _transient_holder_past_the_ceiling(tmp_path, queue, row, monkeypatch, now)

    pool_says = _claim_pass(queue, row, ledger)
    verdict = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S,
                     now=now)

    assert (pool_says["withhold"], pool_says["why"]) == (True, "drains_soon"), pool_says
    assert pool_says["episode_age_s"] > pool.WITHHOLD_CEILING_S, pool_says
    entry = verdict["movers"][0]
    assert (verdict["exempt"], entry["evidence"]) == (True, "withheld"), entry
    assert entry.get("withhold") == "reservation_unavailable_withholding", entry
    assert entry.get("evidence_unix") == now - 1000.0, entry
    assert entry.get("withhold_stamp_unix") == now, entry
    assert entry.get("withhold_live_by") == "claim-pass", entry


def test_a_withhold_whose_stamp_went_stale_past_the_ceiling_lapses(
        tmp_path: Path, monkeypatch) -> None:
    """#1052: the pool withheld for the mover, and then no claim pass
    re-judged it for longer than a claim pass's freshness bound
    (``WITHHOLD_STAMP_FRESH_S``).  Past the epoch's ceiling there is no
    evidence the pool still holds the box: ``withhold-lapsed``."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    queue.publish(**dict(row))
    now = time.time()
    ledger = _transient_holder_past_the_ceiling(tmp_path, queue, row, monkeypatch, now)
    assert _claim_pass(queue, row, ledger)["withhold"] is True

    later = now + pool.WITHHOLD_STAMP_FRESH_S + 1.0
    verdict = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S,
                     now=later)

    entry = verdict["movers"][0]
    assert (verdict["exempt"], entry["evidence"]) == (False, "withhold-lapsed"), entry
    assert entry.get("withhold_stamp_unix") == now, entry


def test_a_withhold_the_pool_stops_is_not_waited_on_as_one(
        tmp_path: Path, monkeypatch) -> None:
    """#1052: the holder's declared end passes, so the claim pass stops
    withholding and stamps ``_starved``.  The row's latest word is no longer
    a withhold, so its fresh pass is not read as one: the withhold's epoch is
    not carried, and the ready rule takes its own ``baseline``."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    queue.publish(**dict(row))
    _landing_ahead(queue, item, row, bytes_ahead=0, movers_ahead=[])
    now = time.time()
    ledger = _transient_holder_past_the_ceiling(tmp_path, queue, row, monkeypatch, now)
    assert _claim_pass(queue, row, ledger)["withhold"] is True
    later = now + 450.0          # the holder's end, t=1500, is behind us
    monkeypatch.setattr(pool, "_now", lambda: later)

    pool_says = _claim_pass(queue, row, ledger)
    verdict = _judge(queue, item, progress_path, prior=None, window_s=GRACE_S,
                     now=later)

    assert (pool_says["withhold"], pool_says["why"]) == (
        False, "holder_does_not_drain_soon"), pool_says
    entry = verdict["movers"][0]
    assert entry["evidence"] == "baseline", entry
    assert "withhold" not in entry, entry


def test_a_consumer_whose_mover_the_pool_stopped_withholding_gets_a_baseline(
        tmp_path: Path) -> None:
    """F1: the previous check, 100 s ago, saw a withhold whose epoch was
    850 s old then, inside the ceiling.  The pool has since stopped
    withholding and denies the row only transiently.  The withhold's epoch
    is not carried into the ready rule, where it would read ``none`` at once
    against a 900 s window: the ready rule takes its own baseline."""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    queue.publish(**dict(row))
    mover = str(row["action_key"])
    now = time.time()
    _landing_ahead(queue, item, row, bytes_ahead=0, movers_ahead=[])
    ready = json.loads(queue.item_path(pool.READY, mover).read_text())
    queue._record_denial_transition(ready, host="dl380g10",
                                    reason="reservation_unavailable",
                                    decision_reason=None)
    prior = {"since_unix": 1.0, "exempt": True, "movers": [{
        "key": mover, "state": "ready", "evidence": "withheld",
        "withhold": "reservation_unavailable_withholding",
        "evidence_unix": now - 950.0, "waiting_behind": []}]}

    verdict = _judge(queue, item, progress_path, prior=prior, window_s=GRACE_S,
                     now=now)

    entry = verdict["movers"][0]
    assert (verdict["exempt"], entry.get("evidence")) == (True, "baseline"), entry


def test_a_mover_requeued_after_its_worker_died_gets_a_fresh_baseline(
        tmp_path: Path) -> None:
    """F4: the previous check saw this mover ``claimed`` with a live lease
    400 s ago; its worker died, the reaper took the claim back at
    ``LEASE_TIMEOUT_S`` and the row is ``ready`` again with nothing ahead
    of it.  The claimed entry's evidence time is not the ready rule's: the
    ready evidence takes a baseline on the state change, instead of reading
    ``none`` at once against a 300 s window.  (The RV-1022 review's probe.)"""

    queue, item, row, progress_path = _verdict_fixture(tmp_path, over_committed_gib=0)
    queue.publish(**dict(row))
    mover = str(row["action_key"])
    _landing_ahead(queue, item, row, bytes_ahead=0, movers_ahead=[])
    now = time.time()
    first = _judge(queue, item, progress_path, prior=None, window_s=300.0, now=now)
    prior = dict(first)
    prior["movers"] = [{"key": mover, "state": "claimed", "evidence": "lease-live",
                        "evidence_unix": now - pool.LEASE_TIMEOUT_S - 100.0,
                        "lease_live": True}]

    verdict = _judge(queue, item, progress_path, prior=prior, window_s=300.0,
                     now=now)

    entry = verdict["movers"][0]
    assert entry["state"] == "ready", entry
    assert (verdict["exempt"], entry.get("evidence")) == (True, "baseline"), entry
