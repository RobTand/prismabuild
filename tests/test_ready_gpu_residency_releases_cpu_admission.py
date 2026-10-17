"""Recorded residency refusals release CPU work until ordinary GPU admission resumes."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

from prismabuild import (
    adaptive_gpu, candidate_policy, capability_eligibility, contracts,
    core as pb, pool, residency_map, residency_plan, storage_tiers,
)
from test_measurement_drains_gpu_backfill import (
    cpu_sample, fleet as fleet_fixture, gpu_sample,
)
from test_pbrun_detach import _checkout

import pbrun
import stage_move
import stage_release
import tier_loop

fleet = fleet_fixture
HOST = "sparklina"
TIER = f"prismabuild-stage:{HOST}"
LEAD = "e" * 64


def _recorded_release(residency, reason):
    item = {"action_key": "a" * 64, "published_unix": 1000.0, "attempts": 0}
    records = {f"{item['action_key']}:1000.0": {
        **item, "host": HOST, "reason": reason, "evidence": {"residency": residency},
    }}
    return pool.PoolQueue._ready_gpu_refused_here(item, records, host=HOST)


@pytest.mark.parametrize("state", candidate_policy.RESIDENCY_REFUSAL_STATES)
def test_every_owned_residency_refusal_releases_the_cpu_veto(state):
    verdict = {"state": state}
    if state in ("lead_not_resident", "lead_unpinned"):
        verdict["pending"] = [{"lead": LEAD, "status": (
            "unpinned" if state == "lead_unpinned" else "absent")}]
    reason = candidate_policy.residency_denial(verdict)
    assert reason is not None
    assert _recorded_release(verdict, reason)


@pytest.mark.parametrize("status", candidate_policy.RESIDENCY_LEAD_UNFINISHED)
def test_a_nonterminal_unpinned_lead_verdict_uses_the_owner_reason(status):
    verdict = {"state": "lead_unpinned", "pending": [{"lead": LEAD, "status": status}]}
    reason = candidate_policy.residency_denial(verdict)
    assert reason == "residency_lead_unpinned"
    assert _recorded_release(verdict, reason)


@pytest.mark.parametrize("statuses,reason", [
    (["absent"], "residency_lead_not_resident"),
    (["ready"], "residency_lead_not_resident"),
    (["claimed"], "residency_lead_not_resident"),
    (["failed", "ready"], "residency_lead_not_resident"),
    (["failed", "claimed"], "residency_lead_not_resident"),
    (["failed", "withdrawn", "dropped", "unpinned", "manifest_mismatch", "cache_hit"],
     "residency_lead_terminal"),
])
def test_pending_leads_use_the_owners_terminal_classification(statuses, reason):
    verdict = {"state": "lead_not_resident", "pending": [
        {"lead": f"{index:064x}", "status": status}
        for index, status in enumerate(statuses)]}
    assert candidate_policy.residency_denial(verdict) == reason
    assert _recorded_release(verdict, reason)
    other = ("residency_lead_terminal" if reason == "residency_lead_not_resident"
             else "residency_lead_not_resident")
    assert not _recorded_release(verdict, other)


@pytest.mark.parametrize("verdict", [
    None, [], "lead_not_resident", {}, {"state": None}, {"state": []},
    {"state": "unknown"}, {"state": "resident"}, {"state": "not_requested"},
    {"state": "lead_not_resident"},
    *({"state": "lead_not_resident", "pending": pending} for pending in (
        None, [], {}, [None], [{}], [{"lead": LEAD}],
        [{"status": "absent"}], [{"lead": None, "status": "absent"}],
        [{"lead": "", "status": "absent"}], [{"lead": 1, "status": "absent"}],
        [{"lead": "not-a-key", "status": "absent"}],
        *([{ "lead": LEAD, "status": status}] for status in (None, "", 1, False, [], {})),
        [{"lead": LEAD, "status": "failed"}, {"lead": LEAD}],
    )),
    {"state": "lead_unpinned", "pending": []},
])
@pytest.mark.parametrize("reason", ["residency_lead_not_resident", "residency_lead_terminal", None])
def test_partial_or_malformed_residency_evidence_keeps_gpu_protection(verdict, reason):
    assert not _recorded_release(verdict, reason)


@pytest.mark.parametrize("reason", [None, "unknown", "residency_map_stale",
                                   "residency_lead_not_resident"])
def test_a_residency_reason_must_match_the_recorded_state(reason):
    assert not _recorded_release({"state": "map_not_composed"}, reason)


def _stage_cycle(queue, stage):
    stage.mkdir(exist_ok=True)
    queue.tier(TIER).mint({'stage_gib': 8})
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    tier_loop.cycle(queue, host=HOST, source_pool="storage_pool",
        receipts=tier_loop.ReceiptCache(), discover=lambda **kwargs: {
            TIER: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                   "tier_id": TIER, "host": HOST, "tier": "stage",
                   "mountpoint": str(stage), "capacity_bytes": 8 * storage_tiers.GIB}})


@pytest.mark.parametrize("drain", [False, True], ids=["claim", "drain-then-claim"])
def test_a_production_lead_is_pinned_and_residency_resumption_restores_the_cpu_veto(
        fleet, tmp_path, monkeypatch, capsys, drain):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    work = _checkout(tmp_path)
    source = tmp_path / "shared" / "shard.bin"
    source.parent.mkdir()
    material = b"real staged bytes\n" * 256
    source.write_bytes(material)
    manifest = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1, "produced_by": {},
        "annotations": {"phases": [{"name": "read", "bytes": len(material),
                                    "cumulative_bytes": len(material)}]},
        "mount_prefix": str(source.parent), "entry_count": 1,
        "total_bytes": len(material), "entries": [{"path": str(source),
            "offset": 0, "bytes": len(material),
            "sha256": hashlib.sha256(material).hexdigest()}],
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    stage = tmp_path / "stage"
    worker_tags = [HOST, "gb10", contracts.INTERPRETER_TAG]
    queue.announce(host=HOST, tags=worker_tags, has_gpu=True,
        capacity={"cpu": 20, "mem_gb": 120, "gpu": 1},
        capabilities=capability_eligibility.OfferCapabilities(
            interpreters=capability_eligibility.InterpreterAnswers.from_observed(
                [sys.executable], None)))
    queue.announce_tier({"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
        "tier_id": TIER, "host": HOST, "tier": "stage", "mountpoint": str(stage),
        "mover_python": sys.executable, "mover_tools_root": str(Path(pbrun.__file__).parent)})
    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(sys, "argv", ["pbrun.py", "--cwd", str(work),
        "--detach", "--tag", HOST, "--gpu", "--cpus", "4", "--demand", "mem_gb=104",
        "--data-manifest", str(manifest_path), "--residency", "stage",
        "--residency-ram", "off", "--", "/bin/true"])
    assert pbrun.main() == 0
    gpu = json.loads(capsys.readouterr().out.strip())["action_key"]
    plan = residency_plan.read(queue, gpu)
    assert plan is not None
    [lead] = residency_plan.leads_for(plan)
    _stage_cycle(queue, stage)
    lead_row = pool.record_store.read_document(queue.records.item_path(pool.READY, lead))
    assert lead_row is not None and not lead_row["needs_gpu"]
    assert HOST in lead_row["tags"]
    lead_action = pb.PrismaBuildCAS(tmp_path / "cas").read_action_request(lead)
    assert HOST in lead_action["params"]["placement"]["required_tags"]
    gpu_row = pool.record_store.read_document(queue.records.item_path(pool.READY, gpu))
    assert gpu_row["needs_gpu"]
    pending = queue.residency_verdict(gpu_row)
    assert pending["state"] == "lead_not_resident"
    assert pending["pending"][0]["lead"] == lead

    cpu = publish("portable-cpu-before-real-lead", priority=1,
                  cpu=1, gpu=0, mem_gb=24 if drain else 3, tags=["gb10"])
    tick()
    claimed = queue.claim(capacity={"cpu": 20, "mem_gb": 120, "gpu": 1},
        cpu_tiers={"preferred": list(range(20)), "fallback": []}, adaptive_cpu=True,
        has_gpu=True, tags=worker_tags,
        cpu_sampler=lambda: cpu_sample(clock, readings),
        gpu_sampler=lambda: gpu_sample(sample, readings))
    assert claimed is not None and claimed["action_key"] == lead
    assert denial(cpu)["reason"] == "deferred_for_ready_gpu"
    assert queue.records.item_path(pool.READY, gpu).exists()

    assert stage_move.main([*lead_action["params"]["command"][2:],
        "--action-key", lead, "--unpaced", "--max-readers", "1",
        "--warm-after-copy", "never"]) == 0
    receipt = queue.move_record(lead)
    assert receipt["complete"] and receipt["bytes_staged"] == len(material)
    assert receipt["entries_staged"] == receipt["entries_declared"] == 1
    queue.finish(lead, status="executed", claim_snapshot=claimed)
    _stage_cycle(queue, stage)
    mapping = residency_map.read_map(queue.records.residency_map_path(gpu))
    [entry] = mapping["entries"].values()
    assert Path(entry["stage_path"]).read_bytes() == material
    assert queue.residency_verdict(gpu_row)["state"] == "resident"

    # The once-per-pass snapshot retains the previous refusal until reevaluation.
    assert claim() == cpu
    assert claim() == (None if drain else gpu)
    if drain:
        fresh = denial(gpu)
        assert not fresh["reason"].startswith("residency_"), fresh
        assert fresh["reason"].startswith("reservation_unavailable"), fresh
        later = publish("portable-cpu-after-residency", priority=1,
                        cpu=1, gpu=0, mem_gb=3, tags=["gb10"])
        assert claim() is None
        assert denial(later)["reason"] == "deferred_for_ready_gpu"
        queue.finish(cpu, status="executed")
        tick(adaptive_gpu.PSI_AVG10_S + 1)
        assert claim() == gpu, denial(gpu)
    else:
        queue.finish(cpu, status="executed")
    queue.finish(gpu, status="executed")
    if drain:
        assert claim() == later
        queue.finish(later, status="executed")
    assert not queue.ledger().held_keys()
    assert not list(queue.records.dir(pool.READY).glob("*.json"))
    assert not list(queue.records.dir(pool.CLAIMED).glob("*.json"))
