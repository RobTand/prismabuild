"""pbmetrics starvation gauges: tier fill, denials, mover depth.

Fixture-based, tmp_path only.  The new families follow the module's existing
conventions: restart-safe gauges, absent rather than zero where nothing was
measured, and no action-key labels.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
from prismabuild import pool, storage_tiers  # noqa: E402
import pbmetrics  # noqa: E402

NOW = 2_000_000.0
STAGE_TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{STAGE_TIER}"
MOVER = "a" * 64
MOVER2 = "b" * 64
CONSUMER = "c" * 64
HOST = "dl380g10"


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _row(key: str, resources: dict, queue: pool.PoolQueue,
         *, residency: dict | None = None) -> dict:
    record = {"action_key": key, "cas_root": str(queue.root / "cas"),
              "checkout_root": str(queue.root / "co"),
              "worker_script": str(queue.root / "worker.py"),
              "tags": [HOST], "resources": resources}
    if residency is not None:
        record["residency"] = residency
    return record


def _mover_residency(start: int, end: int) -> dict:
    return {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
            "manifest_sha256": "9" * 64, "manifest_bytes": end,
            "range_start_bytes": start, "range_end_bytes": end}


@pytest.fixture
def starved_queue(tmp_path, monkeypatch):
    monkeypatch.setattr(pool.socket, "gethostname", lambda: HOST)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
        "tier_id": STAGE_TIER, "tier": "stage", "host": HOST,
        "capacity_bytes": 600 * storage_tiers.GIB, "sampled_unix": NOW - 5,
        "fill_source": "measured-ceiling",
        "fill_supply": {"best_mb_s": 300.0, "ceiling_mb_s": 250.0,
                        "may_grow": False}})
    queue.mint_tier_capacity(STAGE_TIER, {"stage_gib": 64})
    assert queue.tier_ledger(STAGE_TIER).acquire(MOVER, {"stage_gib": 2})
    # Claim the mover by filing, not through claim(): these tests read
    # queue files, and admission is not what they exercise.
    queue.publish(**_row(MOVER2, {STAGE_KIND: 1}, queue,
                         residency=_mover_residency(0, storage_tiers.GIB)))
    ready_path = queue.item_path(pool.READY, MOVER2)
    record = json.loads(ready_path.read_text())
    record.update({"claimed_unix": NOW - 100, "claimed_host": HOST,
                   "claimed_by": f"worker-{HOST}"})
    queue.item_path(pool.CLAIMED, MOVER2).write_text(json.dumps(record))
    ready_path.unlink()
    queue.lease_path(MOVER2).write_text(json.dumps({
        "action_key": MOVER2, "owner": record["claimed_by"], "host": HOST,
        "claimed_unix": record["claimed_unix"],
        "published_unix": record["published_unix"],
        "heartbeat_unix": NOW - 1}))
    queue.publish(**_row(MOVER, {STAGE_KIND: 2}, queue,
                         residency=_mover_residency(0, 2 * storage_tiers.GIB)))
    queue.publish(**_row(CONSUMER, {"mem_gb": 1}, queue, residency={
        "schema": pool.RESIDENCY_SCHEMA_V1, "manifest_sha256": "9" * 64,
        "manifest_bytes": 4 * storage_tiers.GIB, "leads": [MOVER]}))
    published = {key: json.loads(queue.item_path(
        pool.CLAIMED if key == MOVER2 else pool.READY, key).read_text()
    )["published_unix"] for key in (MOVER, MOVER2)}
    _write(queue.ledger(HOST).base / "adaptive" / pool.CLAIM_DENIALS, {
        "schema": pool.CLAIM_DENIALS_SCHEMA_V1, "records": {
            "recent": {"action_key": MOVER, "published_unix": published[MOVER],
                       "host": HOST, "reason": "tier_reservation_unavailable",
                       "evidence": {}, "denied_unix": NOW - 10},
            "old": {"action_key": MOVER2, "published_unix": published[MOVER2],
                    "host": HOST, "reason": "tier_reservation_unavailable",
                    "evidence": {}, "denied_unix": NOW - 7200}}})
    return queue


def _samples(text: str, name: str) -> list[str]:
    return [line for line in text.splitlines() if line.startswith(name + "{")
            or line.startswith(name + " ")]


def test_tier_fill_and_token_occupancy(starved_queue):
    text = pbmetrics.collect_metrics(starved_queue.root, now=NOW)
    assert (f'prismabuild_tier_fill_supply_mb_s{{stat="best",tier="{STAGE_TIER}"}} 300'
            in text)
    assert (f'prismabuild_tier_fill_supply_mb_s{{stat="ceiling",tier="{STAGE_TIER}"}} 250'
            in text)
    assert (f'prismabuild_tier_tokens{{resource="stage_gib",state="capacity",tier="{STAGE_TIER}"}} 64'
            in text)
    assert (f'prismabuild_tier_tokens{{resource="stage_gib",state="available",tier="{STAGE_TIER}"}} 62'
            in text)
    assert (f'prismabuild_tier_tokens{{resource="stage_gib",state="held",tier="{STAGE_TIER}"}} 2'
            in text)


def test_denial_counts_are_windowed_by_reason(starved_queue):
    text = pbmetrics.collect_metrics(starved_queue.root, now=NOW,
                                     terminal_window_seconds=3600)
    assert (f'prismabuild_claim_denials{{host="{HOST}",reason="tier_reservation_unavailable"}} 1'
            in text)
    assert "prismabuild_claim_denial_window_seconds 3600" in text


def test_mover_depth_counts_only_movement_node_shape(starved_queue):
    text = pbmetrics.collect_metrics(starved_queue.root, now=NOW)
    assert 'prismabuild_mover_queue_depth{state="ready"} 1' in text
    assert 'prismabuild_mover_queue_depth{state="claimed"} 1' in text


def test_starvation_gauges_keep_metric_conventions(starved_queue):
    text = pbmetrics.collect_metrics(starved_queue.root, now=NOW)
    assert "action_key=" not in text
    assert " NaN" not in text and " Inf" not in text
    assert all(line.startswith("# TYPE ") and line.endswith(" gauge")
               for line in text.splitlines() if line.startswith("# TYPE "))
    assert "prismabuild_collection_success 1" in text


def test_corrupt_active_record_fails_collection(starved_queue):
    (starved_queue.root / "ready" / f"{MOVER}.json").write_text("{")
    text = pbmetrics.collect_metrics(starved_queue.root, now=NOW)
    assert "prismabuild_collection_success 0" in text
