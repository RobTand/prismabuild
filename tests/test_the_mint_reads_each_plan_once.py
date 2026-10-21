"""The tier mint reads a filed plan once, not every cycle (pb-tierscan).

``stage_rounding.declared_range_index`` prices every in-flight holder from
the sealed range its plan declares, and it read every filed plan to do so:
740 plans, 374 MB, at every mint of every stage tier, inside the tier mint
lock.  On dl380g10 that was most of what the tier loop read: 374 of about
545 MB a 32-second cycle (2026-10-21).  A cycle now reads the plan directory
through the loop's kept records (``ReceiptCache.records``): a plan is read
when it is filed and not again while its version holds, and a plan filed or
withdrawn between cycles prices the next mint.

The coarse clock is stood in past the plans' own timestamps
(``kept_read_clock.after_paths``), so the #1045 fence keeps their versions
without a sleep.  Nothing here touches the live queue.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from kept_read_clock import after_paths

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import pool, residency_plan, stage_rounding, storage_tiers  # noqa: E402
from prismabuild import tier_cycle as tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
GIB = storage_tiers.GIB
KIND = storage_tiers.STAGE_CAPACITY_KIND
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "9" * 64
SPAN = int(2.4 * GIB)
PLANS = 6


def _key(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(queue, key: str, resources: dict[str, int]) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _file_plan(queue, consumer: str, mover: str) -> Path:
    gib = storage_tiers.stage_tokens_for_bytes(SPAN)
    phase = {
        "name": "phase-0", "start_bytes": 0, "end_bytes": SPAN,
        "stage_gib": gib,
        "mover_row": {
            **_row(queue, mover, {STAGE_KIND: gib, "cpu": 1, "mem_gb": 1}),
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                "manifest_sha256": MANIFEST, "manifest_bytes": SPAN,
                "range_start_bytes": 0, "range_end_bytes": SPAN},
        },
        "egress_row": _row(queue, _key(f"egress-{consumer}"), {"mem_gb": 1}),
    }
    residency_plan.freeze(queue, residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=SPAN, phases=[phase]))
    return queue.records.residency_plan_path(consumer)


class _PlanReads:
    """Plan documents read while a mint prices its in-flight holders."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, queue) -> None:
        self.read: list[str] = []
        self._pricing = False
        plans = queue.records.residency_registry_root()
        store = pool.record_store.RecordStore
        real_read = store.text_document_at
        real_index = stage_rounding.declared_range_index

        def text_document_at(path):  # type: ignore[no-untyped-def]
            if self._pricing and Path(path).parent == plans:
                self.read.append(Path(path).stem)
            return real_read(path)

        def declared_range_index(*args, **kwargs):  # type: ignore[no-untyped-def]
            self._pricing = True
            try:
                return real_index(*args, **kwargs)
            finally:
                self._pricing = False

        monkeypatch.setattr(store, "text_document_at",
                            staticmethod(text_document_at))
        monkeypatch.setattr(stage_rounding, "declared_range_index",
                            declared_range_index)

    def take(self) -> list[str]:
        taken, self.read = sorted(self.read), []
        return taken


def _queue(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.records.ensure_layout()
    return queue


def _cycle(queue, tmp_path: Path, receipts) -> dict[str, object]:
    def discover(**_kwargs):  # type: ignore[no-untyped-def]
        return {TIER: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                       "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                       "mountpoint": str(tmp_path / "stage"),
                       "capacity_bytes": 100 * GIB,
                       "capacity_source": storage_tiers.WRITABLE_CAPACITY_SOURCE}}
    announced = tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                                receipts=receipts, discover=discover)
    assert len(announced) == 1
    return announced[0]


def _filed(queue, tmp_path: Path, receipts, count: int):
    """``count`` filed plans, the first one's mover in flight."""

    _cycle(queue, tmp_path, receipts)
    consumers = [_key(f"tierscan-consumer-{index}") for index in range(count)]
    movers = [_key(f"tierscan-mover-{index}") for index in range(count)]
    paths = [_file_plan(queue, consumer, mover)
             for consumer, mover in zip(consumers, movers)]
    assert queue.tier(TIER).take(movers[0], {KIND: 3})
    return consumers, movers, paths


def test_a_steady_mint_reads_no_plan_again(tmp_path: Path, monkeypatch) -> None:
    queue = _queue(tmp_path)
    receipts = tier_loop.ReceiptCache()
    consumers, _movers, paths = _filed(queue, tmp_path, receipts, PLANS)
    after_paths(monkeypatch, queue.records.residency_registry_root(), *paths)
    reads = _PlanReads(monkeypatch, queue)

    first = _cycle(queue, tmp_path, receipts)
    assert reads.take() == sorted(consumers)
    assert first["in_flight_bytes"] == SPAN
    for _ in range(2):
        record = _cycle(queue, tmp_path, receipts)
        assert record["in_flight_gib"] == 3
        assert record["in_flight_bytes"] == SPAN
        assert record["in_flight_unknown_gib"] == 0
    assert reads.take() == []


def test_a_plan_filed_or_withdrawn_between_cycles_prices_the_next_mint(
        tmp_path: Path, monkeypatch) -> None:
    queue = _queue(tmp_path)
    receipts = tier_loop.ReceiptCache()
    _consumers, _movers, paths = _filed(queue, tmp_path, receipts, 2)
    after_paths(monkeypatch, queue.records.residency_registry_root(), *paths)
    reads = _PlanReads(monkeypatch, queue)
    assert _cycle(queue, tmp_path, receipts)["in_flight_bytes"] == SPAN
    reads.take()

    later = _key("tierscan-consumer-later")
    later_mover = _key("tierscan-mover-later")
    _file_plan(queue, later, later_mover)
    assert queue.tier(TIER).take(later_mover, {KIND: 3})
    record = _cycle(queue, tmp_path, receipts)
    assert record["in_flight_bytes"] == 2 * SPAN
    assert record["in_flight_unknown_gib"] == 0
    assert reads.take() == [later]

    paths[0].unlink()
    record = _cycle(queue, tmp_path, receipts)
    assert record["in_flight_gib"] == 6
    assert record["in_flight_bytes"] == SPAN
    assert record["in_flight_unknown_gib"] == 3
    assert reads.take() == [later]
