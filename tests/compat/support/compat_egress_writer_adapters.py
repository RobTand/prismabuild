"""Actual private egress, recovery and refusal report constructors."""
from __future__ import annotations

import base64
import hashlib
from pathlib import Path
import runpy
import time

C, M, OTHER, D, TIER = "c" * 64, "a" * 64, "e" * 64, "9" * 64, "prismabuild-stage:compat"


def operate(request, store, load):
    variant = request["case"].split("/", 1)[1]
    pool, release, maps = (load(name) for name in ("prismabuild.pool", "stage_release", "prismabuild.residency_map"))
    queue = pool.PoolQueue(store / "queue")
    queue.ensure_layout()
    if request["op"] == "read":
        path = queue.move_path(M)
        path.write_bytes(base64.b64decode(request["raw"]))
        request["_consumer_boundary"]()
        value = queue.move_record(M)
        assert value and value["action_key"] == M
        assert len(queue.move_records(schemas=(pool.POOL_EGRESS_SCHEMA_V1,))) == 1
        return value
    stage, root = store / "stage", queue.root / pool.RESIDENCY
    stage.mkdir()
    root.mkdir(parents=True, exist_ok=True)
    common = dict(tier_id=TIER, stage_root=str(stage))
    original_clock = time.perf_counter
    time.perf_counter = lambda: 100.0
    try:
        if variant == "root-refusal-writer":
            value = release._refused_receipt(**common, refusal="private unregistered root", mover_action_key=M, consumer_action_key=C)
        elif variant == "ledger-refusal-writer":
            value = release._ledger_unreadable_receipt(**common, skipped="private census", exc=OSError("private unreadable ledger"))
        elif variant == "receiptless-writer":
            value = release._receiptless_refusal(M, **common, why="private missing receipt")
        elif variant == "unresolved-writer":
            value = release._unresolved_reports(queue, TIER, str(stage), {M: "private unknown owner"}, reported=set())[0]
        elif variant == "uncharged-report-writer":
            value = release._uncharged_owner_report(queue, TIER, str(stage), [(M, C, {"entries": {"one": {"bytes": 8}}})],
                                                    evicted={}, needed=1)[0]
        elif variant == "reclaim-refusal-writer":
            value = load("stage_reclaim")._refusal(stage, "private refusal", tier_id=TIER)
        elif variant == "restore-writer":
            value = load("stage_reclaim").restore_run(queue, stage_root=str(stage), quarantine_root=str(store / "quarantine"), run_id="..")
        elif variant == "prune-writer":
            value = release.prune_stale_mentions(queue, M, consumer_action_key=C, stage=stage, tier_id=TIER, root=root,
                                                 observed={"entries": {}})
        elif variant == "recover-writer":
            value = release.recover_orphaned_range(queue, stage_root=str(stage), head_action_key=M, egress_action_key=OTHER)
        elif variant == "sweep-refusal-writer":
            corrupt = maps.fragment_path(root, C, M)
            corrupt.parent.mkdir(parents=True, exist_ok=True)
            corrupt.write_bytes(b"{private malformed fragment")
            values = release.sweep_dead_owner_fragments(queue, stage_roots={TIER: str(stage)})
            assert len(values) == 1, values
            value = values[0]
        elif variant == "uncharged-refusal-writer":
            (queue.root / pool.TIER_RESERVATIONS / TIER).write_bytes(b"private blocked ledger")
            value = release._evict_uncharged_owner(queue, M, C, **common, residency_root=root)
        elif variant == "produced-orphan-writer":
            queue.announce_tier({"tier_id": TIER, "host": "other-compat-host", "tier": "stage",
                                 "mountpoint": str(stage), "capacity_bytes": 0})
            value = release._retire_produced_orphan(queue, M, {"batch_id": "batch", "producer_action_key": C, "producer_state": "failed"},
                **common, residency_root=root)
        elif variant == "output-deferred-writer":
            queue.publish(action_key=M, cas_root=store / "cas", checkout_root="/compat/checkout", worker_script="/compat/worker.py",
                          resources={"cpu": 1, "mem_gb": 1})
            value = load("prismabuild.produced_output")._tier_host_egress(queue, record={}, producer=C, cas_root=store / "cas",
                batch_id="batch", generation=0, target_mover=M, consumer=C, stage_root=str(stage), residency_root=str(root))["answer"]["receipt"]
        else:
            assert release.register_stage_root(queue, tier_id=TIER, stage_root=stage) == "registered"
            if variant == "reconcile-writer":
                value = release.reconcile(queue, **common, wanted=set())
            elif variant == "reclaim-writer":
                value = load("stage_reclaim").reclaim_source_marks(queue, **common, mount_prefix=str(store / "origin"))
            elif variant == "shared-writer":
                rp = load("prismabuild.residency_plan")
                helpers = runpy.run_path(str(Path(__file__).with_name("compat_plan_adapters.py")))
                helpers["shared"](rp, pool, queue, "first")
                for key in (C, OTHER):
                    plan = {**helpers["plan"](rp, pool, "whole"), "consumer_action_key": key}
                    rp.freeze(queue, plan)
                    queue.publish(action_key=key, cas_root=store / "cas", checkout_root="/compat/checkout", worker_script="/compat/worker.py",
                                  resources={"cpu": 1, "mem_gb": 1})
                queue.publish(action_key=M, cas_root=store / "cas", checkout_root="/compat/checkout", worker_script="/compat/worker.py",
                              resources={"cpu": 1, "mem_gb": 1})
                value = release.evict(queue, M, consumer_action_key=C, stage_root=str(stage))
                assert value.get("interest_dropped"), value
            else:
                target = stage / "weights"
                target.write_bytes(b"contents")
                digest = hashlib.sha256(b"contents").hexdigest()
                maps.write_fragment(root, {"schema": maps.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1, "consumer_action_key": C,
                    "mover_action_key": M, "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": D,
                    "entries": {"0:/compat/input": {"stage_path": str(target), "bytes": 8, "sha256": digest, "offset": 0}}})
                lease = load("prismabuild.reader_lease")
                lease.write_material(root, consumer_action_key=C, mover_action_key=M, tier_id=TIER, stage_root=str(stage),
                    manifest_sha256=D, generation="7" * 32, entries={"0:/compat/input": {"stage_path": str(target), "bytes": 8,
                        "sha256": digest, "file_id": lease.stat_identity(str(target))}})
                queue.tier(TIER).mint({"stage_gib": 1})
                assert queue.tier(TIER).take(M, {"stage_gib": 1})
                value = release.evict(queue, M, consumer_action_key=C, stage_root=str(stage))
                assert value["entries_deleted"] == 1 and not target.exists(), value
        assert isinstance(value, dict), value
        path = queue.record_move(M, value)
    finally:
        time.perf_counter = original_clock
    return {"raw": base64.b64encode(path.read_bytes()).decode(),
            "fixture_facts": ["actual private egress or bounded refusal; supplied clock, no live root, broker, drain or physical abandonment proof"]}
