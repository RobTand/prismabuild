"""A failed mover's partial stays under the consumer reading it (#1151).

On 2026-09-25 Stage B row 013's consumer ``2eb09b1b5513`` was reading
spill-p0 when spill-p0's mover ``574747d717fc`` ended ``complete: false``
with 436 of 512 entries landed.  Under tier pressure the failed-mover reclaim
(#627) published that mover's egress, the egress retired its fragment, and the
consumer's next lease on a name ``covers_for_keys`` had just called covered
refused ``staged-tier-forbidden: unpublished``.  The row died.

The reclaim asked four questions -- no ledger tokens, not queued or running,
no complete receipt, tier pressure -- and never the fifth: can a live reader
still read these bytes?  A claimed consumer reads the phase its accepted
progress names and every phase after it (``residency_plan.remaining``).  Now
the reclaim declines those legs, and the window republishes the same mover
key, whose retry resumes its own landed coverage and copies only what is
missing.  A range every reader has read past is still reclaimed, and so is a
range whose consumer has not been claimed: #627 is unchanged there.

The fixtures drive the real ``tier_loop`` window and reclaim, the real
``PoolQueue.claim`` and ``finish``, the real ``stage_move.main`` and
``stage_release.evict``, and the real reader path
(``reader_lease.covers_for_keys`` then ``reader_lease.acquire``) over tiny
real files on a temp stage root registered to a temp queue.  Runs under
pbtest at priority -10; never executed locally.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import (  # noqa: E402
    adaptive_cpu, pool, reader_lease, residency_map, residency_plan)
import stage_move  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
SIZE = 16 * 1024
PER_PHASE = 2
PHASES = 2
TOTAL = SIZE * PER_PHASE * PHASES


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


CONSUMER = _hexkey("reader")
MOVERS = [_hexkey(f"mover{ordinal}") for ordinal in range(PHASES)]
EGRESSES = [_hexkey(f"egress{ordinal}") for ordinal in range(PHASES)]


def _payload(index: int) -> bytes:
    return bytes((position * 5 + index * 29) % 251 + 1
                 for position in range(SIZE))


def _row(queue: pool.PoolQueue, key: str,
         resources: dict[str, int]) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


class _Row:
    """One consumer of two phases, its manifest on disk, and its stage."""

    def __init__(self, tmp_path: Path, monkeypatch, *,
                 consumer: str = CONSUMER) -> None:
        # The claim reads an action's sealed identity out of the CAS, which a
        # fixture row does not have; the shape is all admission needs here.
        monkeypatch.setattr(adaptive_cpu, "action_identity",
                            lambda item: ("shape", False))
        self.monkeypatch = monkeypatch
        self.tmp_path = tmp_path
        self.consumer = consumer
        self.queue = pool.PoolQueue(tmp_path / "pb-queue")
        self.queue.ensure_layout()
        self.stage = tmp_path / "stage"
        self.stage.mkdir()
        assert stage_release.register_stage_root(
            self.queue, tier_id=TIER, stage_root=self.stage) == "registered"
        self.cas = tmp_path / "cas"
        self.cas.mkdir()
        origin = tmp_path / "origin"
        origin.mkdir()
        self.paths = []
        entries = []
        for index in range(PER_PHASE * PHASES):
            path = origin / f"boundary-{index}.pt"
            path.write_bytes(_payload(index))
            self.paths.append(path)
            entries.append({
                "path": str(path), "offset": 0, "bytes": SIZE,
                "sha256": hashlib.sha256(_payload(index)).hexdigest()})
        body = {"schema": "prismaquant.prismabuild.data_manifest.v1",
                "produced_by": {"tool": "failed-mover-reader-fixture"},
                "mount_prefix": str(origin), "entries": entries,
                "entry_count": len(entries), "total_bytes": TOTAL,
                "annotations": {}}
        self.manifest = tmp_path / "manifest.json"
        self.manifest.write_text(json.dumps(body))
        self.digest = hashlib.sha256(self.manifest.read_bytes()).hexdigest()
        self.keys = [residency_map.residency_map_key(str(path), 0)
                     for path in self.paths]
        phases = []
        for ordinal in range(PHASES):
            start = ordinal * PER_PHASE * SIZE
            end = start + PER_PHASE * SIZE
            phases.append({
                "name": f"phase-{ordinal}",
                "start_bytes": start, "end_bytes": end, "stage_gib": 1,
                "mover_row": {
                    **_row(self.queue, MOVERS[ordinal],
                           {STAGE_KIND: 1, "cpu": 1, "mem_gb": 1}),
                    "max_attempts": 1,
                    "residency": {
                        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                        "manifest_sha256": self.digest,
                        "manifest_bytes": TOTAL,
                        "range_start_bytes": start, "range_end_bytes": end}},
                "egress_row": _row(self.queue, EGRESSES[ordinal],
                                   {"cpu": 1, "mem_gb": 1}),
            })
        self.plan = residency_plan.build_plan(
            consumer_action_key=consumer, tier_id=TIER,
            stage_root=str(self.stage), manifest_sha256=self.digest,
            manifest_bytes=TOTAL, phases=phases)
        residency_plan.freeze(self.queue, self.plan)
        self.queue.publish(
            action_key=consumer, cas_root=self.queue.root / "cas",
            checkout_root=self.queue.root / "co",
            worker_script=self.queue.root / "worker.py",
            resources={"cpu": 1, "mem_gb": 1}, max_attempts=1, tags=["x86"],
            residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                       "manifest_sha256": self.digest,
                       "manifest_bytes": TOTAL,
                       "leads": residency_plan.leads_for(self.plan)})
        self.queue.mint_tier_capacity(TIER, {"stage_gib": 8})
        self.tiers = {TIER: {"tier_id": TIER, "tier": "stage",
                             "mountpoint": str(self.stage)}}
        self.refuse: set[str] = set()
        real = stage_move._Copier._copy_one

        def copy_one(copier, entry, destination, *args, **kwargs):
            if str(entry["path"]) in self.refuse:
                # What 574747d717fc's copy of boundary-425 met (#1151 defect
                # 2): a refusal that stops the range's dispatch.
                raise stage_move._PublicationRefused(
                    f"shared staged name still has a live publisher after "
                    f"the grace, deferring to retry: {destination}")
            return real(copier, entry, destination, *args, **kwargs)

        monkeypatch.setattr(stage_move._Copier, "_copy_one", copy_one)

    def window(self) -> list[dict[str, object]]:
        return tier_loop.residency_window(self.queue, tiers=self.tiers)

    def run_mover(self, ordinal: int) -> dict[str, object]:
        """Claim the phase's mover for real, run the real mover, finish it."""

        mover = MOVERS[ordinal]
        ready = [row for row in self.queue.ready_items()
                 if row.get("action_key") == mover]
        assert ready, f"control: phase-{ordinal}'s mover must be queued"
        claimed = self.queue.claim(
            tags=["dl380g10"], owner=f"{socket.gethostname()}:1:stage",
            capacity={"cpu": 4, "mem_gb": 16}, ready=ready)
        assert claimed is not None and claimed["action_key"] == mover
        start = ordinal * PER_PHASE * SIZE
        rc = stage_move.main([
            "--pool-root", str(self.queue.root),
            "--cas-root", str(self.cas),
            "--action-key", mover,
            "--consumer-action-key", self.consumer,
            "--tier-id", TIER, "--stage-root", str(self.stage),
            "--manifest", str(self.manifest),
            "--manifest-sha256", self.digest,
            "--range-start-bytes", str(start),
            "--range-end-bytes", str(start + PER_PHASE * SIZE),
            "--residency-root", str(self.queue.residency_fragment_root()),
            "--readers", "1", "--max-readers", "1", "--unpaced"])
        receipt = self.queue.move_record(mover)
        assert isinstance(receipt, dict)
        self.queue.finish(mover, status="executed" if rc == 0 else "failed",
                          detail={"returncode": rc}, claim_snapshot=claimed)
        return receipt

    def claim_reader(self, phase: str) -> None:
        """The consumer, claimed and reading ``phase``, as its lease says."""

        source = self.queue.item_path(pool.READY, self.consumer)
        item = json.loads(source.read_text())
        source.unlink()
        now = time.time()
        item.update({"action_key": self.consumer, "claimed_unix": now - 60.0,
                     "claimed_by": "reader-fixture",
                     "claimed_host": "sparklina"})
        self.queue.item_path(pool.CLAIMED, self.consumer).write_text(
            json.dumps(item))
        self.queue.write_lease(
            self.consumer, owner="reader-fixture", claim_snapshot=item,
            progress_observation={
                "source": "action-progress",
                "last_accepted": {"phase": phase, "units_completed": 1,
                                  "reported_unix": now - 9.0}})

    def read(self, indices: list[int], token: str) -> dict[str, object]:
        """The consumer's next lease, the way PQ's staged lease takes one."""

        residence = self.queue.residency_fragment_root()
        reader_lease.clear_cover_docs_cache()
        keys = [self.keys[index] for index in indices]
        found = reader_lease.covers_for_keys(
            residence, self.consumer, keys, tier_id=TIER,
            manifest_sha256=self.digest, epoch="", context={})
        if not found.get("ok"):
            return found
        # file_pin=False proves the window and files no pin, so nothing
        # outlives the read: the state between two chunk leases, which is
        # when the incident's reclaim ran.
        return reader_lease.acquire(
            self.queue, consumer_action_key=self.consumer,
            attempt={"nonce": "f" * 32, "scope_id": "reader-fixture"},
            tier_id=TIER, epoch="",
            span={"start_bytes": 0, "end_bytes": TOTAL},
            holder={"host": socket.gethostname(), "pid": os.getpid()},
            acquire_token=token, covers=found["covers"],
            expected=found["expected"], residency_root=residence,
            file_pin=False)

    def reclaim(self) -> list[dict[str, object]]:
        """One pressured reclaim pass, then run any egress it published."""

        consumers = tier_loop._planned_consumers(self.queue, self.tiers)
        events = tier_loop.reclaim_failed_mover_partials(
            self.queue, consumers, pressure={TIER: 1})
        for ordinal, egress in enumerate(EGRESSES):
            if self.queue.item_path(pool.READY, egress).exists():
                # What the egress node does when it runs.
                stage_release.evict(
                    self.queue, MOVERS[ordinal],
                    consumer_action_key=self.consumer,
                    stage_root=str(self.stage),
                    residency_root=self.queue.residency_fragment_root())
        return events

    def published(self, events: list[dict[str, object]]) -> list[str]:
        return [str(event.get("action_key")) for event in events
                if event.get("event") == "mover-published"]


def _fail_phase_zero(row: _Row) -> dict[str, object]:
    """Phase-0's mover lands its first entry and refuses its second."""

    assert MOVERS[0] in row.published(row.window())
    row.refuse = {str(row.paths[1])}
    receipt = row.run_mover(0)
    row.refuse = set()
    assert receipt["complete"] is False, receipt
    assert receipt["entries_staged"] == 1, receipt
    assert not row.queue.tier_ledger(TIER).holder_tokens(MOVERS[0]), (
        "control: an incomplete mover returns its tokens at finish")
    return receipt


def test_the_reader_keeps_reading_and_the_retry_resumes(
        tmp_path: Path, monkeypatch) -> None:
    """RED before #1151: the reclaim retired the fragment under the reader."""

    row = _Row(tmp_path, monkeypatch)
    _fail_phase_zero(row)
    row.claim_reader("phase-0")
    first = row.read([0], "reader:before")
    assert first.get("ok"), f"control: the landed entry reads: {first}"

    events = row.reclaim()

    after = row.read([0], "reader:after")
    published = [event for event in events
                 if event.get("event") == "failed-mover-egress-published"]
    assert after.get("ok"), (
        f"the consumer's next lease on a landed entry of the phase it is "
        f"reading refused: {after}; the reclaim published: {published}")
    assert published == [], (
        f"the reclaim published an egress for the phase the consumer is "
        f"reading: {published}")
    deferred = [event for event in events if event.get("event")
                == "failed-mover-reclaim-deferred-for-reader"]
    assert [(event["mover"], event["readers"]) for event in deferred] == [
        (MOVERS[0], [CONSUMER])], events

    # The window republishes the same key, and the retry resumes.
    window = row.window()
    assert MOVERS[0] in row.published(window), window
    assert not [event for event in window if event.get("event")
                == "mover-publish-deferred-for-egress"], window
    retry = row.run_mover(0)
    assert retry["complete"] is True, retry
    assert retry["entries_resumed"] == 1, retry
    assert retry["bytes_resumed"] == SIZE, retry
    timings = retry["phase_timings"]
    assert timings["thread_seconds"]["copy_read"]["calls"] == 1, (
        f"the retry copied more than the missing entry: {timings}")
    assert timings["outcomes"] == {"adopted": 1, "renamed": 1}, timings
    whole = row.read([0, 1], "reader:whole")
    assert whole.get("ok"), whole


def test_a_reader_protects_the_phases_it_has_not_reached(
        tmp_path: Path, monkeypatch) -> None:
    """RED before #1151: a later phase's partial went while the reader read an
    earlier one, so the reader would meet its retirement on arrival."""

    row = _Row(tmp_path, monkeypatch)
    _fail_phase_zero(row)
    # The reader has not reported yet: it is at the beginning, and every
    # phase is still ahead of it.
    row.claim_reader("")
    events = row.reclaim()
    assert not [event for event in events
                if event.get("event") == "failed-mover-egress-published"], (
        events)
    assert row.read([0], "reader:first").get("ok")


def test_a_passed_phase_is_still_reclaimed(tmp_path: Path, monkeypatch) -> None:
    """#627 holds where no reader can reach the bytes: read past, reclaimed."""

    row = _Row(tmp_path, monkeypatch)
    _fail_phase_zero(row)
    row.claim_reader("phase-1")
    events = row.reclaim()
    assert [event["mover"] for event in events
            if event.get("event") == "failed-mover-egress-published"] == [
        MOVERS[0]], events


def test_an_unclaimed_consumers_partial_is_still_reclaimed(
        tmp_path: Path, monkeypatch) -> None:
    """#627 holds for a consumer that is not reading yet: no claim admits it
    over a range that did not land, so the egress retires nothing it reads."""

    row = _Row(tmp_path, monkeypatch)
    _fail_phase_zero(row)
    events = row.reclaim()
    assert [event["mover"] for event in events
            if event.get("event") == "failed-mover-egress-published"] == [
        MOVERS[0]], events


def test_any_reading_sharer_protects_a_shared_mover() -> None:
    """A range several consumers name (#1026) is protected by any one of
    them that is claimed and can still read it."""

    plan = {"phases": [
        {"name": "phase-0", "mover_row": {"action_key": MOVERS[0]}},
        {"name": "phase-1", "mover_row": {"action_key": MOVERS[1]}}]}
    reading = _hexkey("sharer-reading")
    passed = _hexkey("sharer-passed")
    waiting = _hexkey("sharer-waiting")
    consumers = [
        (waiting, {"state": pool.READY, "accepted_phase": None}, plan, TIER),
        (passed, {"state": pool.CLAIMED, "accepted_phase": "phase-1"},
         plan, TIER),
        (reading, {"state": pool.CLAIMED, "accepted_phase": "phase-0"},
         plan, TIER),
    ]
    reach = tier_loop._legs_a_reader_can_reach(consumers)
    assert reach == {MOVERS[0]: [reading], MOVERS[1]: [passed, reading]}


@pytest.mark.parametrize("accepted", [None, "", "phase-9"])
def test_a_reader_that_has_not_said_where_it_is_protects_its_whole_plan(
        accepted) -> None:
    plan = {"phases": [
        {"name": "phase-0", "mover_row": {"action_key": MOVERS[0]}},
        {"name": "phase-1", "mover_row": {"action_key": MOVERS[1]}}]}
    reach = tier_loop._legs_a_reader_can_reach([
        (CONSUMER, {"state": pool.CLAIMED, "accepted_phase": accepted},
         plan, TIER)])
    assert reach == {MOVERS[0]: [CONSUMER], MOVERS[1]: [CONSUMER]}
