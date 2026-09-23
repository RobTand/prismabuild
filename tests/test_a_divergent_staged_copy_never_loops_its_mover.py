"""A mover that cannot publish a divergent staged copy never loops (#966).

Production shape (Stage A stall at ``a7d31a4``, 2026-09-23): an earlier
consumer, ``2c164969``, had FAILED, and its DONE mover ``11c41436371f`` still
held a coherent copy under the shared staged name, with a material sidecar
dating the current inode.  The successor's mover ``950345d2b90a`` wanted
different bytes at that name.  The shared publisher refused ("different bytes
than manifest digest ... refusing to invalidate its owner"), and the refusal
was an entry error only: the receipt read incomplete with no refusal, the
mover exited rc 0, ``residency_pin_holds`` released its tokens, and the next
``residency_window`` cycle found the mover neither queued nor pinned and
republished it.  The copy reran forever while holding fill every run.

Neither sweep could end the old owner.  Its mention was coherent, so the
stale-mention prune (#853) kept it, and the orphan sweep only evicts under
pressure (#598), and there was none.

What this file pins, through the real ``stage_move.main``, ``PoolQueue.claim``
and ``finish``, and ``tier_loop.residency_window``:

* every owner ended (a FAILED, WITHDRAWN or DONE consumer, the DONE one
  still keeping its frozen plan as the tier loop leaves it; a DONE mover;
  nothing of either queued or leased): the successor invalidates the name
  and restages it, completes, keeps its tokens, and the window does not
  republish it;
* a live owner (its consumer still claimed): the mover refuses terminally
  with ``staged_destination_conflict``, names both owners, leaves the live
  copy untouched, exits rc 1, and retires its own window so the loop stops;
* an owner whose ending cannot be proven keeps today's retryable refusal:
  nothing is replaced.

Every fixture is a temp stage root registered to a temp queue (never a real
``/stage``); nothing is hashed beyond the small fixture payloads.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_dead_owner_fragment_blocks_then_retires as base  # noqa: E402
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402,F401
import test_stale_material_done_owner_retires as stale  # noqa: E402
from prismabuild import (  # noqa: E402
    adaptive_cpu, pool, reader_lease, residency_map, residency_plan)
import stage_move  # noqa: E402
import tier_loop  # noqa: E402

TIER = base.TIER
SIZE = base.SIZE
NAMES = base.NAMES
STAGE_KIND = f"stage_gib@{TIER}"
OLD = stale.OLD_PAYLOAD
NEW = stale.NEW_PAYLOAD

#: Which name the successor wants different bytes at.  ``last``: the first
#: entry adopts, so the run stages something and exits rc 0 on the base
#: source (the incident).  ``first``: the refusal stops dispatch before
#: anything lands, so the base source exits rc 1 ``residency_moved_nothing``,
#: the pool fails the row, and the window republishes it all the same.
ORDERS = {"last": {NAMES[0]: OLD, NAMES[1]: NEW},
          "first": {NAMES[0]: NEW, NAMES[1]: OLD}}


def _divergent(order: str) -> str:
    return NAMES[1] if order == "last" else NAMES[0]


def _staged(stage: Path, name: str) -> Path:
    return stage / stale.staged_name(name)


def _old_owner(fleet, consumer_state: str, *, pin: bool = False,
               ) -> tuple[str, str]:
    """An earlier consumer's DONE mover holding OLD bytes under both names.

    ``consumer_state`` is ``failed``, ``withdrawn`` or ``executed`` (ended),
    ``claimed`` (live), or ``absent`` (never queued, so no outcome record).
    The mover finished ``executed`` with a complete receipt, a fragment, a
    sidecar that dates the *current* inode of both names, and its tier
    charge: the incident's owner exactly.  ``pin`` takes a live reader pin on
    the owner's copy before the mover concludes, as a reader would.

    An ``executed`` consumer also keeps its frozen plan filed, because the
    tier loop's dead-consumer pass leaves a DONE consumer's plan in place so
    a retry republishes the same children; a filed plan must not read as an
    owner that has not ended.
    """

    queue, stage, _ = fleet
    if consumer_state == "failed":
        consumer, _generation = base._fail_consumer(queue)
    elif consumer_state == "withdrawn":
        # The helper withdraws and concludes any key; here it is a consumer.
        consumer = base._withdraw_mover(queue)
    elif consumer_state == "executed":
        consumer = base._key()
        base._publish(queue, consumer, max_attempts=1)
        queue.finish(consumer, status="executed", detail={"returncode": 0})
    elif consumer_state == "claimed":
        consumer = base._key()
        base._publish(queue, consumer, max_attempts=1)
    else:
        assert consumer_state == "absent", consumer_state
        consumer = base._key()
    mover = base._key()
    base._publish(queue, mover, max_attempts=1)
    for name in NAMES:
        stale._stage(stage, name, OLD)
    stale._write_sidecar(queue, stage, consumer, mover, stale._entries(stage))
    stale._fragment(queue, stage, consumer, mover)
    if pin:
        acquired = reader_lease.acquire(
            queue, consumer_action_key=consumer,
            attempt={"nonce": "n1", "scope_id": "s1"}, tier_id=TIER, epoch="",
            span={"start_bytes": 0, "end_bytes": SIZE},
            holder={"host": "fixture", "pid": os.getpid()},
            acquire_token="old-reader",
            covers=[{"mover_action_key": mover, "manifest_sha256": "a" * 64}])
        assert acquired.get("ok"), acquired
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": "a" * 64,
        "complete": True, "entries_declared": len(NAMES),
        "entries_staged": len(NAMES), "bytes_staged": len(NAMES) * SIZE,
        "range_bytes": len(NAMES) * SIZE, "range_start_bytes": 0,
        "range_end_bytes": len(NAMES) * SIZE, "errors": []})
    queue.finish(mover, status="executed", detail={"returncode": 0})
    stale._charge(queue, mover)
    if consumer_state == "executed":
        _keep_plan(queue, stage, consumer, mover)
    return consumer, mover


def _keep_plan(queue: pool.PoolQueue, stage: Path, consumer: str,
               mover: str) -> None:
    """File the one-phase plan that sealed the old owner's mover."""

    total = len(NAMES) * SIZE
    residency_plan.freeze(queue, residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root=str(stage),
        manifest_sha256="a" * 64, manifest_bytes=total,
        phases=[{
            "name": "phase-0", "start_bytes": 0, "end_bytes": total,
            "stage_gib": 1,
            "mover_row": {
                **_row(queue, mover, {STAGE_KIND: 1, "cpu": 1, "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": "a" * 64, "manifest_bytes": total,
                    "range_start_bytes": 0, "range_end_bytes": total}},
            "egress_row": _row(queue, base._key(), {"cpu": 1, "mem_gb": 1}),
        }]))
    assert queue.residency_plan_path(consumer).exists()


def _row(queue: pool.PoolQueue, key: str,
         resources: dict[str, int]) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


class _World:
    """The successor: its manifest, its frozen one-phase plan, its mover."""

    def __init__(self, fleet, tmp_path: Path, monkeypatch, order: str) -> None:
        queue, stage, cas = fleet
        # The claim reads an action's sealed identity out of the CAS, which a
        # fixture row does not have; the shape is all admission needs here.
        monkeypatch.setattr(adaptive_cpu, "action_identity",
                            lambda item: ("shape", False))
        monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", stale.GRACE)
        monkeypatch.setattr(stage_move, "_PUBLISH_POLL_S", 0.02)
        self.queue, self.stage, self.cas = queue, stage, cas
        self.payloads = ORDERS[order]
        (self.manifest, self.digest, self.entries,
         self.digests) = stale._successor_manifest(tmp_path, self.payloads)
        self.total = len(self.entries) * SIZE
        self.successor, self.copier = base._key(), base._key()
        egress = base._key()
        self.plan = residency_plan.build_plan(
            consumer_action_key=self.successor, tier_id=TIER,
            stage_root=str(stage), manifest_sha256=self.digest,
            manifest_bytes=self.total,
            phases=[{
                "name": "phase-0", "start_bytes": 0, "end_bytes": self.total,
                "stage_gib": 1,
                "mover_row": {
                    **_row(queue, self.copier,
                           {STAGE_KIND: 1, "cpu": 1, "mem_gb": 1}),
                    "max_attempts": 1,
                    "residency": {
                        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                        "manifest_sha256": self.digest,
                        "manifest_bytes": self.total,
                        "range_start_bytes": 0,
                        "range_end_bytes": self.total}},
                "egress_row": _row(queue, egress, {"cpu": 1, "mem_gb": 1}),
            }])
        residency_plan.freeze(queue, self.plan)
        queue.publish(
            action_key=self.successor, cas_root=queue.root / "cas",
            checkout_root=queue.root / "co",
            worker_script=queue.root / "worker.py",
            resources={"cpu": 1, "mem_gb": 1}, max_attempts=1, tags=["x86"],
            residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                       "manifest_sha256": self.digest,
                       "manifest_bytes": self.total,
                       "leads": residency_plan.leads_for(self.plan)})
        # Room for the old owner's charge and this mover's one token.
        queue.mint_tier_capacity(TIER, {"stage_gib": 4})
        self.tiers = {TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(stage)}}

    def window(self) -> list[str]:
        events = tier_loop.residency_window(self.queue, tiers=self.tiers)
        return [str(event.get("action_key")) for event in events
                if event.get("event") == "mover-published"]

    def claim(self) -> None:
        """Publish the mover through the window and claim it for real."""

        assert self.window() == [self.copier], (
            "control: the window must publish the successor's mover once")
        ready = [row for row in self.queue.ready_items()
                 if row.get("action_key") == self.copier]
        assert ready
        claimed = self.queue.claim(
            tags=["dl380g10"], owner=f"{socket.gethostname()}:1:stage",
            capacity={"cpu": 4, "mem_gb": 16}, ready=ready)
        assert claimed is not None and claimed["action_key"] == self.copier
        assert self.queue.tier_ledger(TIER).holder_tokens(self.copier), (
            "control: the claim must take the mover's tier tokens")
        self.claimed = claimed

    def run(self, *, claimed: bool = False) -> tuple[int, dict[str, object]]:
        """Claim (unless done), run the real mover, and finish it."""

        if not claimed:
            self.claim()
        rc = stage_move.main([
            "--pool-root", str(self.queue.root),
            "--cas-root", str(self.cas),
            "--action-key", self.copier,
            "--consumer-action-key", self.successor,
            "--tier-id", TIER, "--stage-root", str(self.stage),
            "--manifest", str(self.manifest),
            "--manifest-sha256", self.digest,
            "--range-start-bytes", "0",
            "--range-end-bytes", str(self.total),
            "--residency-root", str(self.queue.residency_fragment_root()),
            "--readers", "1", "--max-readers", "1", "--unpaced"])
        receipt = self.queue.move_record(self.copier)
        assert isinstance(receipt, dict)
        self.queue.finish(self.copier,
                          status="executed" if rc == 0 else "failed",
                          detail={"returncode": rc},
                          claim_snapshot=self.claimed)
        return rc, receipt


#: Where the copy meets the divergence.  ``adopt``: the adoption proof before
#: any copy, the ordinary case.  ``publish``: the name diverged only after
#: that proof, so the copy meets it at its own publication; ``try_adopt`` is
#: skipped to put the mover there.
STEPS = ["adopt", "publish"]


def _meet_at(monkeypatch, step: str) -> None:
    if step == "publish":
        monkeypatch.setattr(stage_move._StagedPublisher, "try_adopt",
                            lambda self, *args, **kwargs: None)


@pytest.mark.parametrize("step", STEPS)
@pytest.mark.parametrize("order", sorted(ORDERS))
@pytest.mark.parametrize("consumer_state", ["failed", "withdrawn", "executed"])
def test_an_ended_owner_is_invalidated_and_the_mover_is_not_republished(
        fleet, tmp_path, monkeypatch, consumer_state, order, step) -> None:
    """RED on the base source: the window republishes the mover every cycle."""

    queue, stage, _ = fleet
    consumer, mover = _old_owner(fleet, consumer_state)
    world = _World(fleet, tmp_path, monkeypatch, order)
    _meet_at(monkeypatch, step)
    name = _divergent(order)
    before = os.stat(_staged(stage, name))

    rc, receipt = world.run()
    again = world.window()

    assert world.copier not in again, (
        f"#966 loop: the window republished mover {world.copier[:12]} after "
        f"it exited rc {rc} with refusal={receipt.get('refusal')!r}, "
        f"complete={receipt.get('complete')!r}, errors={receipt.get('errors')}")
    assert rc == 0 and receipt["complete"] is True, receipt.get("errors")
    assert "refusal" not in receipt, receipt.get("refusal")
    assert queue.tier_ledger(TIER).holder_tokens(world.copier) == {
        "stage_gib": 1}
    for staged_name, payload in world.payloads.items():
        assert _staged(stage, staged_name).read_bytes() == payload
    # The ended owner's copy was invalidated: a fresh inode, so its sidecar's
    # mention now dates a superseded incarnation, which #853 prunes.
    assert os.stat(_staged(stage, name)).st_ino != before.st_ino
    assert receipt["entries_invalidated"] == 1
    [row] = receipt["invalidated"]
    assert row["stage_path"] == str(_staged(stage, name))
    assert row["owners"] == [{"consumer_action_key": consumer,
                              "mover_action_key": mover, "state": "ended"}]
    # The untouched name kept its bytes: adopted, or republished identically.
    other = NAMES[0] if name == NAMES[1] else NAMES[1]
    assert _staged(stage, other).read_bytes() == OLD
    assert residency_plan.superseded(queue, world.plan) is None
    # One judgment per owner, wherever the copy met it, and one act: the
    # copy's own rename over the old file, never an unlink before the copy.
    timings = receipt["phase_timings"]
    assert timings["thread_seconds"]["owner_judgement"]["calls"] == 1
    assert timings["outcomes"].get("replaced_ended_owner") == 1, (
        timings["outcomes"])
    # The owners' locks are held once per arbitration, and the hold is
    # recorded: at the adoption proof and at publication, or at publication.
    held = timings["thread_seconds"]["owner_locks_held"]["calls"]
    assert held == (2 if step == "adopt" else 1), held


@pytest.mark.parametrize("step", STEPS)
@pytest.mark.parametrize("order", sorted(ORDERS))
def test_a_live_owner_is_a_terminal_conflict_naming_both_owners(
        fleet, tmp_path, monkeypatch, order, step) -> None:
    """RED on the base source: rc 0, no refusal, and the loop continues."""

    queue, stage, _ = fleet
    consumer, mover = _old_owner(fleet, "claimed")
    world = _World(fleet, tmp_path, monkeypatch, order)
    _meet_at(monkeypatch, step)
    name = _divergent(order)
    path = _staged(stage, name)
    before = os.stat(path)

    rc, receipt = world.run()

    assert rc == 1, (rc, receipt.get("refusal"), receipt.get("errors"))
    assert receipt["refusal"] == "staged_destination_conflict"
    assert receipt["complete"] is False
    conflict = receipt["conflict"]
    assert conflict["stage_path"] == str(path)
    assert conflict["consumer_action_key"] == world.successor
    assert conflict["mover_action_key"] == world.copier
    assert conflict["declared_sha256"] == world.digests[name]
    assert conflict["owners"] == [{"consumer_action_key": consumer,
                                   "mover_action_key": mover,
                                   "state": "live"}]
    # The live copy is never destroyed: same inode, same bytes, and its
    # owner keeps its fragment, sidecar and charge.
    after = os.stat(path)
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino,
                                                 before.st_mtime_ns)
    assert path.read_bytes() == OLD
    assert residency_map.fragment_path(
        queue.residency_fragment_root(), consumer, mover).exists()
    assert reader_lease.material_path(
        queue.residency_fragment_root(), consumer, mover).exists()
    assert queue.tier_ledger(TIER).holder_tokens(mover) == {"stage_gib": 1}
    # The refused mover holds nothing, and its window is retired by name.
    assert queue.tier_ledger(TIER).holder_tokens(world.copier) == {}
    assert receipt["plan_superseded"] is True
    marker = residency_plan.superseded(queue, world.plan)
    assert marker is not None and marker["movers"] == [world.copier]
    assert "staged_destination_conflict" in marker["reason"]
    for key in (consumer, mover, world.successor, world.copier):
        assert key in marker["reason"], (key, marker["reason"])
    assert world.copier not in world.window(), "#966 loop"


GUARDS = ["no-outcome", "consumer-lease", "mover-queued", "mover-lease",
          "unreadable-fragment", "pinned"]


@pytest.mark.parametrize("guard", GUARDS)
def test_an_owner_whose_ending_is_unproven_is_never_replaced(
        fleet, tmp_path, monkeypatch, guard) -> None:
    """Uncertainty keeps the retryable refusal; nothing is invalidated.

    Each case is the ended owner of the test above with one fact missing or
    contradicted.  They pass on the base source, which never replaced, and
    they pin each condition the ending proof requires.
    """

    queue, stage, _ = fleet
    consumer, mover = _old_owner(
        fleet, "absent" if guard == "no-outcome" else "failed",
        pin=guard == "pinned")
    # An unreadable fragment fails every proof closed, the adoptable name's
    # too, so the divergent name goes first there: it is the one whose owners
    # are judged before dispatch stops.
    order = "first" if guard == "unreadable-fragment" else "last"
    world = _World(fleet, tmp_path, monkeypatch, order)
    # Damage after the claim, so the window and the claim see a clean queue
    # and only the mover's ending proof meets the missing fact.
    world.claim()
    if guard == "consumer-lease":
        queue.lease_path(consumer).write_text("{}")
    elif guard == "mover-lease":
        queue.lease_path(mover).write_text("{}")
    elif guard == "mover-queued":
        queue.publish(action_key=mover, cas_root="/cas", checkout_root="/co",
                      worker_script="/w.py", resources={"cpu": 1},
                      max_attempts=1, recompute=True)
        assert queue.item_path(pool.READY, mover).exists()
    elif guard == "unreadable-fragment":
        path = residency_map.fragment_path(
            queue.residency_fragment_root(), base._key(), base._key())
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{not json")
    name = _divergent(order)
    path = _staged(stage, name)
    before = os.stat(path)

    rc, receipt = world.run(claimed=True)

    after = os.stat(path)
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino,
                                                 before.st_mtime_ns)
    assert path.read_bytes() == OLD
    assert receipt["complete"] is False
    # Retryable, as before: never the terminal conflict.  The divergent-first
    # order stages nothing, which is the ordinary empty-move refusal.
    assert receipt.get("refusal") in (None, "residency_moved_nothing"), (
        receipt.get("refusal"), receipt.get("conflict"))
    assert "conflict" not in receipt
    assert rc == (0 if receipt.get("refusal") is None else 1)
    assert not receipt.get("invalidated")
    assert residency_plan.superseded(queue, world.plan) is None
    assert any(name in error for error in receipt["errors"]), (
        json.dumps(receipt["errors"]))


@pytest.mark.parametrize("step", STEPS)
def test_an_owner_that_appears_after_the_judgment_is_judged_before_any_act(
        fleet, tmp_path, monkeypatch, step) -> None:
    """A live owner that names the path between the judgment and the act.

    The ended owner is judged; then, before the stage lock is taken, a live
    consumer's mover files a fragment dating the same inode.  The decision
    under the stage lock sees an owner nobody judged, so it acts on nothing,
    and the publication judges the newcomer and finds it live.  The live
    copy is never destroyed on the strength of a judgment that did not
    include its owner.  ``publish`` is the case where the next act would be
    the replacement itself.
    """

    queue, stage, _ = fleet
    consumer, mover = _old_owner(fleet, "failed")
    world = _World(fleet, tmp_path, monkeypatch, "last")
    _meet_at(monkeypatch, step)
    world.claim()
    path = _staged(stage, NAMES[1])
    before = os.stat(path)
    late_consumer, late_mover = base._key(), base._key()
    real = stage_move._StagedPublisher._judge_owners
    arrived: list[bool] = []

    def judge_then_arrive(self, pairs):
        rows = real(self, pairs)
        if not arrived:
            arrived.append(True)
            # Queued, not claimed: ``claim`` could pick another row.
            queue.publish(action_key=late_consumer, cas_root="/cas",
                          checkout_root="/co", worker_script="/w.py",
                          resources={"cpu": 1})
            stale._write_sidecar(queue, stage, late_consumer, late_mover,
                                 stale._entries(stage))
            stale._fragment(queue, stage, late_consumer, late_mover)
        return rows

    monkeypatch.setattr(stage_move._StagedPublisher, "_judge_owners",
                        judge_then_arrive)

    rc, receipt = world.run(claimed=True)

    assert arrived
    after = os.stat(path)
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino,
                                                 before.st_mtime_ns)
    assert path.read_bytes() == OLD
    assert rc == 1 and receipt["refusal"] == "staged_destination_conflict", (
        receipt.get("refusal"), receipt.get("errors"))
    owners = {(row["consumer_action_key"], row["mover_action_key"]):
              row["state"] for row in receipt["conflict"]["owners"]}
    assert owners == {(consumer, mover): "ended",
                      (late_consumer, late_mover): "live"}
    assert not receipt.get("invalidated")


@pytest.mark.parametrize("requeued", ["consumer", "mover"])
def test_an_owner_resubmitted_after_its_ending_was_proven_is_judged_again(
        fleet, tmp_path, monkeypatch, requeued) -> None:
    """A remembered ending does not outlive the owner's resubmission.

    The owner is proven ended before the copy and remembered for the run.
    One of its keys is then resubmitted -- the dead-consumer pass and an
    operator both do this -- before the copy publishes.  The publication
    must see the queue record the resubmission wrote and judge the owner
    again, never replace its bytes on the strength of the remembered
    ending.  A resubmitted consumer is live, so the copy refuses it by
    name; a resubmitted mover alone leaves the ending unproven, so the copy
    defers, retryably.
    """

    queue, stage, _ = fleet
    consumer, mover = _old_owner(fleet, "failed")
    world = _World(fleet, tmp_path, monkeypatch, "last")
    path = _staged(stage, NAMES[1])
    before = os.stat(path)
    real = stage_move._StagedPublisher.publish
    resubmitted: list[bool] = []

    def resubmit_then_publish(self, *args, **kwargs):
        if not resubmitted:
            resubmitted.append(True)
            queue.publish(
                action_key=consumer if requeued == "consumer" else mover,
                cas_root="/cas", checkout_root="/co", worker_script="/w.py",
                resources={"cpu": 1})
        return real(self, *args, **kwargs)

    monkeypatch.setattr(stage_move._StagedPublisher, "publish",
                        resubmit_then_publish)

    rc, receipt = world.run()

    assert resubmitted
    after = os.stat(path)
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino,
                                                 before.st_mtime_ns)
    assert path.read_bytes() == OLD
    assert not receipt.get("invalidated")
    # Judged before the copy, and again once its return was seen.
    timings = receipt["phase_timings"]
    assert timings["thread_seconds"]["owner_judgement"]["calls"] == 2
    if requeued == "consumer":
        assert rc == 1, (rc, receipt.get("errors"))
        assert receipt["refusal"] == "staged_destination_conflict"
        assert receipt["conflict"]["owners"] == [
            {"consumer_action_key": consumer, "mover_action_key": mover,
             "state": "live"}]
    else:
        assert rc == 0 and receipt.get("refusal") is None, (
            rc, receipt.get("refusal"), receipt.get("errors"))
        assert receipt["complete"] is False
        assert receipt.get("conflict") is None
        assert residency_plan.superseded(queue, world.plan) is None
        assert any(NAMES[1] in error for error in receipt["errors"]), (
            json.dumps(receipt["errors"]))


def test_a_movers_own_record_is_not_an_owner_it_judges(fleet) -> None:
    """The divergence census never names the mover that is judging it.

    A same-key retry whose origin changed under a digest-less manifest meets
    its own earlier record dating other bytes.  Judged as an owner, its own
    consumer -- queued while its movers run -- would read as live, and the
    retry would refuse its own copy as a terminal conflict.  Any other mover
    meeting the same record collects it.
    """

    queue, stage, _ = fleet
    consumer, mover = _old_owner(fleet, "failed")
    path = os.path.normpath(str(_staged(stage, NAMES[0])))
    declared = hashlib.sha256(NEW).hexdigest()

    own = base._publisher(fleet, mover, consumer)
    collected = stage_move._Owners()
    _, standing, _ = own._proof_search(path, SIZE, declared, owners=collected)
    assert standing == "divergent"
    assert collected.pairs == set() and collected.complete

    other = base._publisher(fleet, base._key(), base._key())
    collected = stage_move._Owners()
    _, standing, _ = other._proof_search(path, SIZE, declared,
                                         owners=collected)
    assert standing == "divergent"
    assert collected.pairs == {(consumer, mover)} and collected.complete


def test_a_sibling_movers_copy_is_judged_by_its_mover_alone(fleet) -> None:
    """Another mover of this copy's own consumer is not a live other owner.

    A forward and a reverse pass, or two phases of one plan, stage one
    extent onto one name.  Under a digest-less manifest whose origin changed
    between them, the later mover meets the earlier one's record dating
    other bytes.  The owner's consumer is this copy's own, live by
    construction, so it must not read as a conflict that retires this
    copy's own window: the sibling is judged by its mover alone.  The same
    owner met by a mover of another consumer is live.
    """

    queue, _stage, _ = fleet
    consumer, sibling = base._key(), base._key()
    base._publish(queue, consumer, max_attempts=1)
    base._publish(queue, sibling, max_attempts=1)
    queue.finish(sibling, status="executed", detail={"returncode": 0})
    owner = {(consumer, sibling)}

    ours = base._publisher(fleet, base._key(), consumer)
    assert ours._judge_owners(owner) == [
        {"consumer_action_key": consumer, "mover_action_key": sibling,
         "state": "ended"}]
    theirs = base._publisher(fleet, base._key(), base._key())
    assert [row["state"] for row in theirs._judge_owners(owner)] == ["live"]
