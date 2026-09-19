"""pbstatus --starvation answers what is waiting on data, read-only.

The blob derives from records the fleet already files -- claims, leases,
plans, fragments, tier announcements, ledgers, denial snapshots -- and emits
``not_observable`` where no record carries the answer.  Nothing under test
here writes to the queue.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
from prismabuild import pool, residency_map, residency_plan, storage_tiers  # noqa: E402
import pbstatus  # noqa: E402

CONSUMER = "c" * 64
MANIFEST = "9" * 64
STAGE_TIER = "prismabuild-stage:dl380g10"
RAM_TIER = "ram:dl380g10"
STAGE_KIND = f"stage_gib@{STAGE_TIER}"
RAM_KIND = f"ram_gib@{RAM_TIER}"
GIB = storage_tiers.GIB
EPOCH = "1695052800-1a2b3c4d5e6f7a8b"
HOST = "dl380g10"
NOW = 2_000_000.0


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(key: str, resources: dict[str, int], queue: pool.PoolQueue) -> dict:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": [HOST], "resources": resources}


def _plan(queue: pool.PoolQueue, *, stage_root: str) -> dict:
    """Two phases; only the first carries a ram promotion leg."""
    phases = []
    start = 0
    for ordinal in range(2):
        end = start + 2 * GIB
        entry: dict = {
            "name": f"phase-{ordinal:04d}",
            "start_bytes": start, "end_bytes": end, "stage_gib": 2,
            "mover_row": {
                **_row(_hexkey(f"mover{ordinal}"), {STAGE_KIND: 2, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": end,
                    "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _row(_hexkey(f"egress{ordinal}"), {"mem_gb": 1}, queue),
        }
        if ordinal == 0:
            entry["ram_mover_row"] = {
                **_row(_hexkey("rampromote0"), {RAM_KIND: 2, "mem_gb": 1}, queue),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": RAM_TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": end,
                    "range_start_bytes": start, "range_end_bytes": end}}
            entry["ram_egress_row"] = _row(_hexkey("ramrelease0"), {"mem_gb": 1}, queue)
        phases.append(entry)
        start = end
    return residency_plan.build_plan(
        consumer_action_key=CONSUMER, tier_id=STAGE_TIER, stage_root=stage_root,
        manifest_sha256=MANIFEST, manifest_bytes=start, phases=phases,
        ram_tier_id=RAM_TIER)


@pytest.fixture
def starved_queue(tmp_path, monkeypatch):
    monkeypatch.setattr(pool.socket, "gethostname", lambda: HOST)
    monkeypatch.setattr(pbstatus.time, "time", lambda: NOW)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.announce(host=HOST, tags=[HOST], has_gpu=False,
                   capacity={"cpu": 8, "mem_gb": 32},
                   observed_capacity={"cpu": 8, "mem_gb": 32})
    plan = _plan(queue, stage_root=str(tmp_path / "stage"))
    residency_plan.freeze(queue, plan)
    # The consumer, claimed and quiet on its first phase.  Filed directly
    # rather than through claim(): the claim gate admits a consumer only once
    # its leads are resident, and this census reads the claim, not the gate.
    queue.publish(**_row(CONSUMER, {"mem_gb": 1}, queue), residency={
        "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
        "manifest_sha256": MANIFEST, "manifest_bytes": 4 * GIB,
        "leads": residency_plan.leads_for(plan)})
    ready_path = queue.item_path(pool.READY, CONSUMER)
    record = json.loads(ready_path.read_text())
    record.update({"claimed_unix": NOW - 100, "claimed_host": HOST,
                   "claimed_by": f"worker-{HOST}"})
    queue.item_path(pool.CLAIMED, CONSUMER).write_text(json.dumps(record))
    ready_path.unlink()
    queue.lease_path(CONSUMER).write_text(json.dumps({
        "action_key": CONSUMER, "owner": record["claimed_by"], "host": HOST,
        "claimed_unix": record["claimed_unix"],
        "published_unix": record["published_unix"],
        "heartbeat_unix": NOW - 1,
        "progress_observation": {
            "phase": "encode", "quiet_s": 800.0, "grace_s": 900.0,
            "last_accepted": {"phase": "phase-0000", "reported_unix": NOW - 800,
                              "units_completed": 3}},
        "execution_observation": {
            "child": {"alive": True, "pid_count": 3, "cpu_seconds": 12.0,
                      "silent_s": 700.0}}}))
    # Phase 0 staged on both tiers, with a ram fragment; phase 1 published
    # as a ready mover and staged nowhere.
    queue.mint_tier_capacity(STAGE_TIER, {"stage_gib": 64})
    queue.mint_tier_capacity(RAM_TIER, {"ram_gib": 8})
    assert queue.tier_ledger(STAGE_TIER).acquire(_hexkey("mover0"), {"stage_gib": 2})
    assert queue.tier_ledger(RAM_TIER).acquire(_hexkey("rampromote0"), {"ram_gib": 2})
    queue.publish(**_row(_hexkey("mover1"), {STAGE_KIND: 2, "mem_gb": 1}, queue),
                   residency={
                       "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": STAGE_TIER,
                       "manifest_sha256": MANIFEST, "manifest_bytes": 4 * GIB,
                       "range_start_bytes": 2 * GIB, "range_end_bytes": 4 * GIB})
    residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": _hexkey("rampromote0"),
        "tier_id": RAM_TIER, "stage_root": str(tmp_path / "stage"),
        "manifest_sha256": MANIFEST, "epoch": EPOCH,
        "entries": {residency_map.residency_map_key("/in/shard-0.bin", 0): {
            "stage_path": f"{tmp_path}/stage/model/shard-0.bin", "bytes": 4096,
            "offset": 0, "sha256": "b" * 64}}})
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
        "tier_id": STAGE_TIER, "tier": "stage", "host": HOST,
        "capacity_bytes": 600 * GIB, "sampled_unix": NOW - 5,
        "fill_source": "measured",
        storage_tiers.FILL_RECORD_FIELD: 310.0,
        "fill_supply": {"best_mb_s": 300.0, "ceiling_mb_s": None,
                        "may_grow": True}})
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
        "tier_id": RAM_TIER, "tier": "ram", "host": HOST,
        "capacity_bytes": 8 * GIB, "sampled_unix": NOW - 5,
        "epoch": EPOCH, "window_gib": 4})
    published = json.loads(queue.item_path(pool.READY, _hexkey("mover1")).read_text())
    denials = {
        "schema": pool.CLAIM_DENIALS_SCHEMA_V1, "records": {
            "mover": {"action_key": _hexkey("mover1"),
                      "published_unix": published["published_unix"],
                      "host": HOST, "reason": "tier_reservation_unavailable",
                      "evidence": {"decision": {"reason": "tier_busy"}},
                      "denied_unix": NOW - 10},
            "consumer": {"action_key": CONSUMER,
                         "published_unix": json.loads(
                             queue.item_path(pool.CLAIMED, CONSUMER).read_text()
                         )["published_unix"],
                         "host": HOST, "reason": "adaptive_cpu_refused",
                         "evidence": {}, "denied_unix": NOW - 20}}}
    adaptive = queue.ledger(HOST).base / "adaptive"
    adaptive.mkdir(parents=True, exist_ok=True)
    (adaptive / pool.CLAIM_DENIALS).write_text(json.dumps(denials))
    return queue


def test_quiet_consumer_with_silent_child_reads_as_waiting(starved_queue):
    blob = pbstatus.read_starvation(starved_queue.root, now=NOW)
    assert blob["schema"] == pbstatus.STARVATION_SCHEMA_V1
    assert blob["complete"] is True
    assert len(blob["waiting_claims"]) == 1
    claim = blob["waiting_claims"][0]
    assert claim["action_key_prefix"] == CONSUMER[:12]
    assert claim["accepted_phase"] == "phase-0000"
    assert claim["quiet_fraction"] == pytest.approx(800.0 / 900.0)
    assert claim["child"]["cpu_seconds"] == 12.0
    assert claim["cpu_growth"] == pbstatus.NOT_OBSERVABLE
    assert claim["waiting_on_data"] is True


def test_plan_promotion_state_names_ram_fragments_per_phase(starved_queue):
    blob = pbstatus.read_starvation(starved_queue.root, now=NOW)
    assert len(blob["residency_plans"]) == 1
    plan = blob["residency_plans"][0]
    assert plan["valid"] is True and plan["accepted_phase"] == "phase-0000"
    assert plan["accepted_index"] == 0
    first, second = plan["phases"]
    assert first["stage"]["staged"] is True
    assert first["ram"]["staged"] is True and first["ram"]["fragment"] is True
    assert second["stage"]["published"] is True and second["stage"]["staged"] is False
    assert second["ram"] is None
    gap = plan["cursor_gap"]
    assert gap["stage"]["unstaged_phases"] == ["phase-0001"]
    assert gap["stage"]["unstaged_bytes"] == 2 * GIB
    assert gap["live_byte_cursor"] == pbstatus.NOT_OBSERVABLE


def test_tiers_carry_announced_fill_and_ledger_occupancy(starved_queue):
    blob = pbstatus.read_starvation(starved_queue.root, now=NOW)
    tier = next(t for t in blob["tiers"] if t["tier_id"] == STAGE_TIER)
    assert tier["fill_supply"] == {"best_mb_s": 300.0, "ceiling_mb_s": None,
                                   "may_grow": True}
    assert tier["ledger_capacity"]["stage_gib"] == 64
    assert tier["ledger_available"]["stage_gib"] == 62
    assert tier["ledger_held"]["stage_gib"] == 2


def test_tiers_carry_kind_host_epoch_and_window(starved_queue):
    blob = pbstatus.read_starvation(starved_queue.root, now=NOW)
    assert blob["complete"] is True
    ram = next(t for t in blob["tiers"] if t["tier_id"] == RAM_TIER)
    assert ram["announced"] is True
    assert ram["tier_kind"] == "ram" and ram["host"] == HOST
    assert ram["epoch"] == EPOCH and ram["window_gib"] == 4
    stage = next(t for t in blob["tiers"] if t["tier_id"] == STAGE_TIER)
    assert stage["tier_kind"] == "stage" and stage["host"] == HOST
    assert stage["epoch"] is None and stage["window_gib"] is None


def test_denial_top_separates_mover_blocking_reasons(starved_queue):
    blob = pbstatus.read_starvation(starved_queue.root, now=NOW)
    assert len(blob["denial_top"]) == 1
    host = blob["denial_top"][0]
    assert host["host"] == HOST and host["denials"] == 2
    assert host["mover_blocked"] == 1
    assert host["mover_blocked_reasons"] == [
        {"reason": "tier_reservation_unavailable", "count": 1}]


def test_gap_list_names_what_would_have_to_be_recorded(starved_queue):
    blob = pbstatus.read_starvation(starved_queue.root, now=NOW)
    fields = {entry["field"] for entry in blob["not_observable"]}
    assert {"waiting_claims[].cpu_growth",
            "residency_plans[].cursor_gap.live_byte_cursor",
            "denial history for finished movers",
            "tiers[] live disk delivery"} <= fields
    assert all(entry["would_need"] for entry in blob["not_observable"])


def test_starvation_cli_prints_one_json_blob(starved_queue, capsys):
    assert pbstatus.main(["--starvation", "--queue-root",
                          str(starved_queue.root)]) == 0
    blob = json.loads(capsys.readouterr().out)
    assert blob["schema"] == pbstatus.STARVATION_SCHEMA_V1
    assert blob["waiting_claims"] and blob["residency_plans"]


def test_starvation_reads_without_writing(starved_queue):
    paths = [p for p in starved_queue.root.rglob("*") if p.is_file()]
    before = {str(p): (p.read_bytes(), p.stat().st_mtime_ns) for p in paths}
    pbstatus.read_starvation(starved_queue.root, now=NOW)
    after = {str(p): (p.read_bytes(), p.stat().st_mtime_ns)
             for p in starved_queue.root.rglob("*") if p.is_file()}
    assert after == before
