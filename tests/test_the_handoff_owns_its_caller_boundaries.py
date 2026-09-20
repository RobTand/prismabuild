"""The plan handoff is owned by its callers, not only by its locked helpers (#708).

The first #708 candidate made the helpers sound and left four seams between
them and their actual callers.  Aster's final caller-level review named each
one exactly:

* ``residency_plan.superseded`` read a malformed or unstattable incarnation as
  *a different filing* -- which authorizes reuse -- and could raise on a
  corrupt stamp.  Unknown identity is not a proved different filing: only a
  well-typed three-integer stamp compared against a successfully read current
  identity may answer ``None``;
* ``pbrun.residency_stage_rows`` decided against a plan it had read without an
  identity and then asked ``reap`` to archive "whatever is filed", so a
  replacement landing between the advisory check and the reap was archived by
  the stale caller;
* ``pbrun.main`` and the tier loop's two automatic publication sites published
  children outside the consumer's ownership boundary: a plan could be archived
  between its ``freeze`` and its consumer's publication (an old failed or
  withdrawn terminal is all a cleanup pass needs), and a stale window snapshot
  could publish a child after its parent had been withdrawn or replaced;
* ``residency_plan.handoff_safe`` read the consumer and then each child's
  CLAIMED-then-READY states with bare ``Path.exists()``, so an atomic
  READY->CLAIMED transition between the two reads looked absent, and an
  unreadable queue looked absent too.

Every rule here is driven through a real caller -- ``tier_loop``'s windows,
``pbrun.main`` or ``pbrun.residency_stage_rows`` -- rather than by calling the
helpers correctly from the test.  Nothing here touches the live queue or a
real device.
"""
from __future__ import annotations

import errno
import json
import os
from pathlib import Path
import sys
import threading
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_plan  # noqa: E402
import pbrun  # noqa: E402
import tier_loop  # noqa: E402

import test_pbrun_residency_stage_submission as submission  # noqa: E402
import test_the_ram_window_publishes_only_what_the_tmpfs_can_hold as ram_window  # noqa: E402
import test_withdrawal_retires_the_plan_it_minted as retirement  # noqa: E402


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    q.mint_tier_capacity(retirement.TIER, {"stage_gib": 8})
    return q


def _marker_body(plan: dict[str, object], **overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "schema": residency_plan.RESIDENCY_PLAN_SUPERSEDED_SCHEMA_V1,
        "consumer_action_key": plan["consumer_action_key"],
        "plan_sha256": residency_plan.plan_sha256(plan),
        "reason": "operator",
        "movers": [],
    }
    body.update(overrides)
    return body


def _file_marker(queue: pool.PoolQueue, plan: dict[str, object],
                 **overrides: object) -> Path:
    marker = residency_plan.superseded_path(queue, plan)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps(_marker_body(plan, **overrides)))
    return marker


def _stage_build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """One real ``--residency stage`` submission's inputs, without running main."""

    prepared = submission._prepare(tmp_path, monkeypatch)
    args = pbrun.parse_args()
    built = pbrun.prepare_submission(args)
    template = built["template"]
    return {
        "queue": prepared["queue"], "template": template,
        "cas": template["cas"], "args": args,
        "key": str(pbrun.seal_action_from_template(template)["action_key"]),
        "tier": pbrun.resolve_stage_tier(
            prepared["queue"], args.residency_tier),
    }


def _stage_rows(built: dict[str, object]) -> dict[str, object]:
    """Call the real planner and file its plan, exactly as ``main`` does."""

    staged = pbrun.residency_stage_rows(
        built["template"], consumer_action_key=built["key"],
        tier=built["tier"], args=built["args"], queue=built["queue"],
        cas=built["cas"])
    residency_plan.freeze(built["queue"], staged["plan"])
    return staged


# -- 1. unknown identity defers; it never proves a different filing ----------


@pytest.mark.parametrize("stamp", [
    "missing",          # no plan_incarnation at all
    [1, 2],             # the wrong length
    ["a", "b", "c"],    # not integers: the old reader raised here
    [1, 2, 3.5],        # a float is not a typed integer
    [True, 2, 3],       # a bool is not a typed integer
])
def test_a_malformed_incarnation_stamp_is_unknown_not_a_different_filing(
        queue: pool.PoolQueue, stamp: object) -> None:
    """A marker that cannot state its filing must defer, not authorize reuse."""

    plan = retirement._plan(queue, retirement.FIRST, label="first")
    retirement._publish_consumer(queue, retirement.FIRST, plan)
    overrides = {} if stamp == "missing" else {"plan_incarnation": stamp}
    _file_marker(queue, plan, **overrides)

    record = residency_plan.superseded(queue, plan)

    assert record is not None, (
        "a marker whose filing cannot be read answered 'no marker', which "
        "authorizes exactly the republication a withdrawal exists to stop")
    assert record.get("unreadable") is True, record


def test_an_unstattable_current_identity_defers(
        queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch) -> None:
    """A stat that fails is unknown, not proof the filing differs."""

    plan = retirement._plan(queue, retirement.FIRST, label="first")
    retirement._publish_consumer(queue, retirement.FIRST, plan)
    filing = residency_plan.incarnation(queue.residency_plan_path(retirement.FIRST))
    assert filing is not None
    _file_marker(queue, plan, plan_incarnation=list(filing))

    monkeypatch.setattr(residency_plan, "incarnation", lambda path: None)
    record = residency_plan.superseded(queue, plan)

    assert record is not None and record.get("unreadable") is True, record


def test_a_valid_stamp_for_an_older_filing_still_proves_a_different_one(
        queue: pool.PoolQueue) -> None:
    """The one case that may answer ``None``: both identities were readable."""

    plan = retirement._plan(queue, retirement.FIRST, label="first")
    retirement._publish_consumer(queue, retirement.FIRST, plan)
    older = residency_plan.incarnation(queue.residency_plan_path(retirement.FIRST))
    assert older is not None
    _file_marker(queue, plan, plan_incarnation=list(older), reason="withdrawn")

    # A deliberate resubmission replaces the filing with identical bytes: a
    # new filing, and the old marker covers only the one it named.
    queue.residency_plan_path(retirement.FIRST).unlink()
    residency_plan.freeze(queue, plan)

    assert residency_plan.superseded(queue, plan) is None


def test_the_window_defers_on_a_malformed_incarnation_stamp(
        queue: pool.PoolQueue, tmp_path: Path) -> None:
    """Through the real window: unknown retirement publishes nothing."""

    plan = retirement._plan(queue, retirement.FIRST, label="first")
    retirement._publish_consumer(queue, retirement.FIRST, plan)
    _file_marker(queue, plan, plan_incarnation=["x", "y", "z"])

    events = tier_loop.residency_window(
        queue, tiers={retirement.TIER: retirement._tier(tmp_path)})

    assert not queue.item_path(
        pool.READY, retirement._hexkey("firstmover0")).exists()
    assert not any(event.get("event") == "mover-published" for event in events)
    assert residency_plan.read(queue, retirement.FIRST) is not None, (
        "an unreadable marker never authorizes a replacement either")


def test_a_resubmission_refuses_a_malformed_incarnation_stamp(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Through the real planner: a corrupt stamp is a refusal, not a traceback."""

    submitted = submission._submit(tmp_path, monkeypatch)
    queue = submitted["queue"]
    consumer_key = submission._detach_key(capsys)
    stale = residency_plan.read(queue, consumer_key)
    assert stale is not None
    queue.withdraw(consumer_key, reason="test", by="test")
    marker = residency_plan.superseded_path(queue, stale)
    body = json.loads(marker.read_text())
    body["plan_incarnation"] = ["x", "y", "z"]
    marker.unlink()          # the immutable marker was filed read-only
    marker.write_text(json.dumps(body))

    with pytest.raises(SystemExit, match="unreadable"):
        pbrun.main()

    assert residency_plan.read(queue, consumer_key) == stale, (
        "a damaged marker never authorizes a replacement")


# -- 2. the planner owns the filing it decided against ------------------------


def test_residency_stage_rows_adopts_the_filing_that_replaced_its_own(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The locked reap must not archive a replacement the caller never saw."""

    built = _stage_build(tmp_path, monkeypatch)
    queue, key = built["queue"], built["key"]
    stale = _stage_rows(built)
    assert residency_plan.mark_superseded(
        queue, key, reason="operator") is not None

    replaced: dict[str, object] = {}
    real_handoff = residency_plan.handoff_safe

    def replace_after_the_advisory_check(q, consumer, plan):
        answer = real_handoff(q, consumer, plan)
        if "filing" not in replaced:
            # A deliberate resubmission replaces the filing between this
            # advisory answer and the locked reap that is supposed to archive
            # the filing this caller decided against.  It never saw the new
            # one, and must not archive it.
            q.residency_plan_path(consumer).unlink()
            residency_plan.freeze(q, stale["plan"])
            replaced["filing"] = residency_plan.incarnation(
                q.residency_plan_path(consumer))
        return answer

    monkeypatch.setattr(
        residency_plan, "handoff_safe", replace_after_the_advisory_check)

    result = _stage_rows(built)

    assert result.get("reused_frozen_plan") is True, result
    assert residency_plan.incarnation(
        queue.residency_plan_path(key)) == replaced["filing"], (
        "the replacement filing was archived and a third seal took its place")
    assert retirement._archives(queue, key) == []


def test_residency_stage_rows_refuses_when_its_locked_reap_finds_live_work(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A reap that removed nothing is never read as "the old filing is gone"."""

    built = _stage_build(tmp_path, monkeypatch)
    queue, key = built["queue"], built["key"]
    stale = _stage_rows(built)
    assert residency_plan.mark_superseded(
        queue, key, reason="operator") is not None
    lead = str(stale["plan"]["phases"][0]["mover_row"]["action_key"])

    real_handoff = residency_plan.handoff_safe
    calls = {"count": 0}

    def claim_after_the_advisory_check(q, consumer, plan):
        answer = real_handoff(q, consumer, plan)
        calls["count"] += 1
        if calls["count"] == 1:
            # The handoff went live between the advisory check and the reap:
            # the window published the lead and a worker claimed it.
            q.publish(**dict(stale["plan"]["phases"][0]["mover_row"]),
                      recompute=True)
            submission._claim(q, lead)
        return answer

    monkeypatch.setattr(
        residency_plan, "handoff_safe", claim_after_the_advisory_check)

    with pytest.raises(SystemExit, match="has not ended"):
        _stage_rows(built)

    assert residency_plan.read(queue, key) == stale["plan"], (
        "the old plan binding is preserved while its work is live")
    assert queue.item_path(pool.CLAIMED, lead).exists()


# -- 3. the ownership boundary covers freeze -> publication ------------------


def test_cleanup_cannot_archive_a_fresh_plan_before_its_consumer_is_published(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """An old terminal is not permission to reap a plan nobody owns yet.

    The dead-consumer pass reads the terminal record, then the plan, and reaps
    when nothing live names it.  The resubmission between that pass and the
    consumer's own publication has a filed plan and no consumer row -- unless
    the whole handoff/seal/publish transaction is inside the consumer's
    transition lock, which is the boundary this pins.
    """

    submitted = submission._prepare(tmp_path, monkeypatch)
    queue = submitted["queue"]
    assert pbrun.main() == 0
    key = submission._detach_key(capsys)
    plan = residency_plan.read(queue, key)
    assert plan is not None
    lead = str(plan["phases"][0]["mover_row"]["action_key"])

    # The generation ends the way a withdrawn consumer leaves it: a terminal
    # record for the cleanup pass, a live marker on the plan, and the old
    # lead concluded by the dead-consumer pass before the resubmission.
    withdrawal = queue.withdraw(key, reason="stale price", by="operator")
    assert withdrawal["residency_plan_superseded"] is True
    assert residency_plan.superseded(queue, plan) is not None
    # The submitter publishes no mover, so the lead row this stands in for
    # concluding may simply not exist yet.
    queue.item_path(pool.READY, lead).unlink(missing_ok=True)

    entered = threading.Event()
    attempted = threading.Event()
    cleanup_events: list[dict[str, object]] = []
    real_publication_row = pbrun.publication_row

    def paused_publication_row(action, *args, **kwargs):
        # The consumer's own publication is the one after freeze and before its
        # row exists: the exact window in which a cleanup pass sees a plan with
        # no owner.  The mover rows sealed before freeze never pause.
        if action.get("action_key") != key:
            return real_publication_row(action, *args, **kwargs)
        entered.set()
        assert attempted.wait(10)
        time.sleep(0.5)         # the window a racing cleanup would use
        return real_publication_row(action, *args, **kwargs)

    monkeypatch.setattr(pbrun, "publication_row", paused_publication_row)

    def cleanup() -> None:
        assert entered.wait(10)
        attempted.set()
        cleanup_events.extend(tier_loop.withdraw_dead_consumer_movers(queue))

    worker = threading.Thread(target=cleanup, daemon=True)
    worker.start()
    assert pbrun.main() == 0
    worker.join(10)
    assert not worker.is_alive(), "the cleanup pass never returned"

    assert not any(event.get("event") == "residency-plan-reaped"
                   for event in cleanup_events), cleanup_events
    assert residency_plan.read(queue, key) is not None, (
        "the freshly frozen plan was archived before its consumer row existed")
    assert queue.item_path(pool.READY, key).exists()
    assert not queue.item_path(pool.READY, lead).exists(), (
        "the submitter publishes no mover; the lead is the loop's to publish")


def test_the_window_does_not_publish_a_child_after_the_parent_was_withdrawn(
        queue: pool.PoolQueue, tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch) -> None:
    """A stale cycle snapshot cannot outrun the parent's own withdrawal."""

    plan = retirement._plan(queue, retirement.FIRST, label="first", gib=(2, 2))
    retirement._publish_consumer(queue, retirement.FIRST, plan)
    real_admits = tier_loop._tier_admits_movers
    withdrawn = {"done": False}

    def withdraw_then_admit(tier_record):
        if not withdrawn["done"]:
            withdrawn["done"] = True
            # The operator withdraws the consumer after the cycle computed its
            # decision from the live parent and before the child is published.
            queue.withdraw(retirement.FIRST, reason="operator", by="test")
        return real_admits(tier_record)

    monkeypatch.setattr(
        tier_loop, "_tier_admits_movers", withdraw_then_admit)

    events = tier_loop.residency_window(
        queue, tiers={retirement.TIER: retirement._tier(tmp_path)})

    assert not any(event.get("event") == "mover-published" for event in events)
    assert not queue.item_path(
        pool.READY, retirement._hexkey("firstmover0")).exists(), (
        "a child was published after its parent's window was withdrawn")
    assert any(event.get("event") == "mover-publish-deferred-stale-window"
               for event in events), events


def test_the_ram_window_does_not_promote_after_the_parent_was_withdrawn(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The same boundary on the promotion leg: the parent owns its children."""

    queue = ram_window._fixture(tmp_path, ram_capacity_gib=2, landed=1)
    real_admits = tier_loop._tier_admits_movers
    withdrawn = {"done": False}

    def withdraw_then_admit(tier_record):
        if not withdrawn["done"]:
            withdrawn["done"] = True
            queue.withdraw(ram_window.CONSUMER, reason="operator", by="test")
        return real_admits(tier_record)

    monkeypatch.setattr(
        tier_loop, "_tier_admits_movers", withdraw_then_admit)

    events = tier_loop.ram_residency_window(
        queue, tiers=ram_window._tiers(tmp_path))

    assert not any(event.get("event") == "ram-mover-published"
                   for event in events)
    assert not queue.item_path(
        pool.READY, ram_window._hexkey("rampromote0")).exists(), (
        "a promotion was published after its parent's window was withdrawn")
    assert any(event.get("event") == "ram-mover-publish-deferred-stale-window"
               for event in events), events


# -- 4. handoff_safe brackets each child's states ----------------------------


def test_handoff_safe_sees_a_transition_between_its_two_state_reads(
        queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch) -> None:
    """CLAIMED-then-READY is two reads: a claim between them is not absence."""

    plan = retirement._plan(queue, retirement.FIRST, label="first", gib=(2,))
    retirement._publish_consumer(queue, retirement.FIRST, plan)
    child = retirement._hexkey("firstmover0")
    queue.publish(**dict(plan["phases"][0]["mover_row"]), recompute=True)
    retirement._fail_consumer(queue, retirement.FIRST)

    real_stat = os.stat
    moved = {"done": False}

    def stat_and_move(path, *args, **kwargs):
        if not moved["done"] and str(path) == str(
                queue.item_path(pool.CLAIMED, child)):
            moved["done"] = True
            # The atomic READY->CLAIMED transition, landing between the
            # claimed read and the ready read.
            retirement._claim(queue, child)
            raise FileNotFoundError(errno.ENOENT, "gone", str(path))
        return real_stat(path, *args, **kwargs)

    # ``os.stat``, not ``Path.stat``: the target boxes run a pathlib whose
    # ``exists()`` reaches the syscall directly and suppresses its errors.
    monkeypatch.setattr(os, "stat", stat_and_move)

    safe, why = residency_plan.handoff_safe(queue, retirement.FIRST, plan)

    assert safe is False, why
    assert "claimed" in why, why


def test_handoff_safe_defers_on_an_unreadable_state(
        queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch) -> None:
    """An unreadable queue is uncertainty, never an absent child."""

    plan = retirement._plan(queue, retirement.FIRST, label="first", gib=(2,))
    retirement._publish_consumer(queue, retirement.FIRST, plan)
    child = retirement._hexkey("firstmover0")
    queue.publish(**dict(plan["phases"][0]["mover_row"]), recompute=True)
    retirement._fail_consumer(queue, retirement.FIRST)

    real_stat = os.stat

    def refuse_ready(path, *args, **kwargs):
        if str(path) == str(queue.item_path(pool.READY, child)):
            raise OSError(errno.EIO, "Input/output error")
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(os, "stat", refuse_ready)

    safe, why = residency_plan.handoff_safe(queue, retirement.FIRST, plan)

    assert safe is False, why
    assert "could not be read" in why, why
