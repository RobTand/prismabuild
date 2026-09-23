"""A produced-output mover whose producer attempt is dead gives its stage back (#929).

Live shape, 2026-09-21: the one-shot Stage A cycle's producer ``0dedb066f868``
published a prepaid produced-output mover, ``6fbc96301c6c``.  The mover's
first attempt failed in 0.76 s and was requeued.  The producer then failed
closed on ``BoundaryStagingTimeout`` (``max_attempts`` 1), and an operator
withdrew the stranded mover from ``ready``.  A prepaid mover holds its tokens
while it is ready, and a withdrawal releases nothing, so the mover kept
1 stage GiB with no receipt, no fragment and no plan.  The only release the
lane has is the producer's own ``retire_batch``, and a dead producer never
calls it.  The tier sweep left the holder alone on purpose, because a
produced mover's tokens belong to its batch's lifecycle.  And
``retire_terminal_output_funding`` kept the mover's funding record for as long
as the mover held the token, so each leak pinned the other.

The sweep now asks the produced-output lane about every held mover it has
funded, receipt or no receipt:

* the producer attempt is live, or the mover itself is still queued: kept;
* the producer attempt has ended (``dead``, or ``succeeded`` without retiring
  its batch) and the mover has ended too: the batch is retired through
  ``produced_output.retire_batch``, the same egress the producer would have
  run, whatever the tier's pressure.  A retried producer binds a new instance
  and a new batch namespace, so nothing can read this copy again;
* anything else: kept, and reported once per change of the reason.

A completed batch of a live producer used to be exposed the other way: its
move receipt names the batch namespace, not a queue action, so the orphan pass
took it for an orphan, looked for its fragment in the flat store, found none
and released its tokens while its bytes stayed on the stage.  The
joint-commitment census (#907) counted the same tokens as evictable room: 22
of R12's completed batches, 44 GiB, on 2026-09-23.

Everything runs on a synthetic stage under ``tmp_path`` registered to a queue
under ``tmp_path``; nothing reads or writes a real stage.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

from prismabuild import pool, residency_map  # noqa: E402
import prismabuild.produced_output as po  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

from test_prepaid_writer_integration import (  # noqa: E402
    KIND, REPO, TIER, _announce_tier, _broker_control, _claim_mover,
    _descriptors, _prewrite, _producer_request, _queue, _template,
    _tier_census,
)

#: The events an operator reads, spelled out: they are what one greps for.
RETIRED_EVENT = "stage-produced-orphan-retired"
UNRESOLVED_EVENT = "stage-holder-unresolved"
#: More room than the synthetic tier has: every retention costs something.
UNMET = {TIER: 64}
#: A tier nothing is waiting on: a pressure-gated orphan is never evicted.
IDLE = {TIER: 0}
PAYLOAD = b"b" * 1024


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    """No outer launch identity leaks in, and no report cache leaks across."""

    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT", "PRISMABUILD_ACTION_KEY"):
        monkeypatch.delenv(name, raising=False)
    reset = getattr(stage_release, "reset_holder_reports", lambda: None)
    reset()
    yield
    reset()


class _World:
    """One producer, bound once, with one prepaid batch published."""

    def __init__(self, tmp_path: Path) -> None:
        self.cas_root = tmp_path / "cas"
        self.template = _template(str(tmp_path / "outputs"))
        self.owner = _producer_request(tmp_path, self.cas_root, self.template)
        self.q = _queue(tmp_path)
        self.ledger = self.q.tier_ledger(TIER)
        # One attempt, as the live producer had: its failure is terminal.
        self.q.publish(action_key=self.owner, cas_root=str(self.cas_root),
                       worker_script=str(REPO / "tools" / "prismabuild_worker.py"),
                       checkout_root=str(tmp_path / "mover-checkout"),
                       resources={"cpu": 1, "mem_gb": 1,
                                  **po.owner_demand_terms(self.template)},
                       produced_output_template=self.template,
                       max_attempts=1, retry_safe=False)
        claimed = self.q.claim(owner="w-owner")
        assert claimed is not None and claimed["action_key"] == self.owner
        control = _broker_control(self.q, self.owner)
        env = {"PRISMABUILD_ACTION_KEY": self.owner,
               "PRISMABUILD_ACTION_NONCE": control["nonce"],
               "PRISMABUILD_ACTION_SCOPE": control["scope_id"]}
        po.declare_template(self.q.root, self.template)
        self.inst = po.bind_instance(self.q, self.template,
                                     owner_action_key=self.owner,
                                     claim_snapshot=claimed, env=env)
        po.declare_instance(self.q.root, self.inst)
        assert po.admit_instance(self.q, self.inst, self.template)["ok"] is True
        self.stage = tmp_path / "stage"
        _announce_tier(self.q, self.stage)
        self.descs = _descriptors(tmp_path, self.template, self.inst, "p1",
                                  PAYLOAD)
        _prewrite(self.q, self.inst, self.template, "b1", TIER, self.descs)
        res = po.publish_prepaid_batch(
            self.q, self.inst, self.template, self.descs, batch_id="b1",
            tier=TIER, cas_root=self.cas_root, producer_action_key=self.owner,
            command_extra=["--unpaced"])
        assert res.get("ok") is True, res
        self.mover = str(res["mover_key"])
        self.namespace = str(res["batch_namespace"])

    def strand_the_mover(self) -> None:
        """The live sequence: claim, fail, withdraw from wherever it went."""

        claimed = _claim_mover(self.q, "w-mover")
        assert claimed["action_key"] == self.mover
        funding = self.q.read_output_funding(self.mover, TIER)
        assert funding is not None and funding["state"] == "consumed"
        self.q.finish(self.mover, status="failed", detail={"returncode": 1})
        if self.q.item_path(pool.READY, self.mover).exists():
            self.q.withdraw(self.mover, by="test",
                            reason="stranded produced-output mover")
        ended = [state for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN)
                 if self.q.item_path(state, self.mover).exists()]
        assert len(ended) == 1, ended
        assert not self.q.item_path(pool.READY, self.mover).exists()
        assert self.q.move_record(self.mover) is None
        # Neither the failure nor the withdrawal released the prepaid token.
        assert self.ledger.holder_tokens(self.mover) == {KIND: 1}

    def fail_the_producer(self) -> None:
        self.q.finish(self.owner, status="failed", detail={"returncode": 1})
        assert self.q.item_path(pool.FAILED, self.owner).exists()
        assert not self.q.item_path(pool.READY, self.owner).exists()

    def sweep(self, pressure):
        return stage_release.sweep(
            self.q, stage_roots={TIER: str(self.stage)}, pressure=pressure)

    def batch_entry(self) -> dict:
        commitments = json.loads(
            (po.instance_dir(self.q.root, self.inst) / "commitments.json")
            .read_text())
        return commitments["batches"]["b1"]


def _about(receipts, key) -> list[dict]:
    return [entry for entry in receipts if entry.get("action_key") == key]


def test_a_withdrawn_mover_of_a_failed_producer_is_retired(tmp_path) -> None:
    """The 6fbc96301c6c shape: swept on an idle tier, both leaks closed."""

    world = _World(tmp_path)
    world.strand_the_mover()
    world.fail_the_producer()
    assert _tier_census(world.ledger)["holders"] == {world.mover: 1}

    receipts = world.sweep(IDLE)
    retired = _about(receipts, world.mover)
    assert [entry.get("event") for entry in retired] == [
        RETIRED_EVENT], receipts
    assert retired[0]["complete"] is True, retired
    assert retired[0]["producer_action_key"] == world.owner
    assert retired[0]["producer_state"] == "dead"
    assert retired[0]["batch_id"] == "b1"
    assert _tier_census(world.ledger) == {
        "capacity": 4, "free": 4, "holders": {}}
    assert world.batch_entry().get("retired") is True

    # The funding record no longer has a token pinning it.
    assert world.q.retire_terminal_output_funding()["retired"] == 1
    # A second pass has nothing left to do.
    assert not _about(world.sweep(IDLE), world.mover)


def test_a_succeeded_producer_that_left_its_batch_staged_is_retired(
        tmp_path) -> None:
    """A producer that ended without retiring its batch left garbage too."""

    world = _World(tmp_path)
    world.strand_the_mover()
    world.q.finish(world.owner, status="executed", detail={"returncode": 0})
    retired = _about(world.sweep(IDLE), world.mover)
    assert [entry.get("producer_state") for entry in retired] == ["succeeded"]
    assert retired[0]["complete"] is True, retired
    assert world.ledger.holder_tokens(world.mover) == {}


def test_a_withdrawn_mover_of_a_live_producer_is_kept(tmp_path) -> None:
    """A live claim names the holder: the producer may still restage it."""

    world = _World(tmp_path)
    world.strand_the_mover()
    receipts = world.sweep(UNMET)
    assert not _about(receipts, world.mover), receipts
    assert world.ledger.holder_tokens(world.mover) == {KIND: 1}
    assert world.batch_entry().get("retired") is not True
    # The producer's own window is named by its own live claim: it is not
    # reported as a holder nobody can place, under pressure or otherwise.
    assert world.ledger.holder_tokens(world.owner) == {KIND: 1}
    assert not _about(receipts, world.owner), receipts
    assert not _about(world.sweep(IDLE), world.owner)


def test_a_queued_mover_of_a_dead_producer_is_kept(tmp_path) -> None:
    """The mover's own row is live: releasing now would let it copy unpaid."""

    world = _World(tmp_path)
    assert world.q.item_path(pool.READY, world.mover).exists()
    world.fail_the_producer()
    receipts = world.sweep(UNMET)
    assert not _about(receipts, world.mover), receipts
    assert world.ledger.holder_tokens(world.mover) == {KIND: 1}


def test_a_completed_batch_of_a_live_producer_keeps_its_tokens(
        tmp_path) -> None:
    """Its receipt names the batch namespace, which is not a queue action."""

    world = _World(tmp_path)
    claimed = _claim_mover(world.q, "w-mover")
    assert claimed["action_key"] == world.mover
    staged = world.stage / "produced-output" / world.namespace / "p1.bin"
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(PAYLOAD)
    origin = str(world.descs[0]["path"])
    residency_map.write_fragment(
        po.output_fragment_root(world.q.root / pool.RESIDENCY), {
            "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
            "consumer_action_key": world.namespace,
            "mover_action_key": world.mover, "tier_id": TIER,
            "stage_root": str(world.stage), "manifest_sha256": "a" * 64,
            "entries": {residency_map.residency_map_key(origin, 0): {
                "stage_path": str(staged), "bytes": len(PAYLOAD),
                "sha256": "c" * 64, "offset": 0}}})
    world.q.record_move(world.mover, {
        "consumer_action_key": world.namespace, "tier_id": TIER,
        "stage_root": str(world.stage), "manifest_sha256": "a" * 64,
        "range_start_bytes": 0, "range_end_bytes": len(PAYLOAD),
        "bytes_staged": len(PAYLOAD), "complete": True})
    world.q.finish(world.mover, status="executed")
    assert world.ledger.holder_tokens(world.mover) == {KIND: 1}

    # The joint-commitment census does not offer its tokens as room either.
    tiers = {TIER: next(record for record in world.q.tiers()
                        if record.get("tier_id") == TIER)}
    census = tier_loop._commitment_census(world.q, tiers, consumers=[])[TIER]
    assert census.get("evictable_gib") == 0, census

    receipts = world.sweep(UNMET)
    assert not [entry for entry in _about(receipts, world.mover)
                if entry.get("tokens_released")], receipts
    assert world.ledger.holder_tokens(world.mover) == {KIND: 1}
    assert staged.read_bytes() == PAYLOAD


def test_an_unresolvable_produced_holder_is_reported_once_and_kept(
        tmp_path) -> None:
    """Neither live nor provably dead: the operator hears about it, once."""

    world = _World(tmp_path)
    world.strand_the_mover()
    world.fail_the_producer()
    template_file = (world.q.root / pool.RESIDENCY
                     / po.OUTPUT_TEMPLATES_SUBDIR
                     / f"{world.inst['template_id']}.json")
    template_file.chmod(0o644)
    template_file.unlink()

    first = _about(world.sweep(IDLE), world.mover)
    assert [entry.get("event") for entry in first] == [
        UNRESOLVED_EVENT], first
    assert "template" in " ".join(first[0]["errors"]), first
    assert first[0]["tier_id"] == TIER
    assert world.ledger.holder_tokens(world.mover) == {KIND: 1}
    # The same reason again is not news.
    assert not _about(world.sweep(IDLE), world.mover)
    assert world.ledger.holder_tokens(world.mover) == {KIND: 1}
