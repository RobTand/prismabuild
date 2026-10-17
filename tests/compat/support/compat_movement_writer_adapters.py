"""Actual eight-byte movers and tier-loop landing publication in private roots."""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import resource
import runpy
import time
from types import SimpleNamespace
import uuid
import secrets

C, M, SOURCE, D, G = "c" * 64, "a" * 64, "b" * 64, "9" * 64, "7" * 32
TIER, RAM = "prismabuild-stage:compat", "ram:compat"


def landing(request, store, load):
    maps, pool = load("prismabuild.residency_map"), load("prismabuild.pool")
    path = store / "landing.json"
    if request["op"] == "read":
        path.write_bytes(base64.b64decode(request["raw"]))
        request["_consumer_boundary"]()
        value = maps.read_landing(path)
        assert len(value["ranges"]) == 1 and value["ranges"][0]["range_end_bytes"] == 8
        return value
    rp, loop = load("prismabuild.residency_plan"), load("prismabuild.tier_cycle")
    queue = pool.PoolQueue(store / "queue")
    factory = runpy.run_path(str(Path(__file__).with_name("compat_plan_adapters.py")))["plan"]
    rp.freeze(queue, factory(rp, pool, "chunks"))
    original = time.monotonic
    time.monotonic = lambda: 100.0
    try:
        assert loop.publish_landing_expectations(queue, tiers={TIER: {"tier_id": TIER, "tier": "stage"}},
            consumers=[{"action_key": C, "state": pool.CLAIMED}], now=100.0) == []
    finally:
        time.monotonic = original
    return {"raw": base64.b64encode(maps.landing_path(queue.residency_fragment_root(), C).read_bytes()).decode()}


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    if family == "landing":
        return landing(request, store, load)
    pool, maps = load("prismabuild.pool"), load("prismabuild.residency_map")
    queue = pool.PoolQueue(store / "queue")
    queue.records.ensure_layout()
    if request["op"] == "read":
        raw = base64.b64decode(request["raw"])
        path = maps.fragment_path(queue.root / pool.RESIDENCY, C, M) if family == "fragment" else queue.move_path(M)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        request["_consumer_boundary"]()
        if family == "fragment":
            values = maps.read_fragments(queue.root / pool.RESIDENCY, C)
            assert len(values) == 1 and len(values[0]["entries"]) == 1
            return values[0]
        value = queue.move_record(M)
        assert value and value["action_key"] == M and value["complete"] == (variant != "ram-refusal-writer")
        assert len(queue.move_records(schemas=(pool.POOL_MOVE_SCHEMA_V1,))) == 1
        return value
    mover, lease, release, tiers = (load(name) for name in (
        "prismabuild.stage_move", "prismabuild.reader_lease", "prismabuild.staged_path", "prismabuild.storage_tiers"))
    stage, ram, origin = store / "stage", store / "ram", store / "origin"
    for path in (stage, ram, origin):
        path.mkdir()
    source = origin / "weights"
    source.write_bytes(b"contents")
    digest = hashlib.sha256(b"contents").hexdigest()
    core = load("prismabuild.core")
    codec = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["manifest_owner"](core, load)
    manifest = {"schema": codec.DATA_MANIFEST_SCHEMA_V1, "produced_by": {}, "annotations": {},
                "mount_prefix": str(origin), "entries": [{"path": str(source), "offset": 0, "bytes": 8, "sha256": digest}],
                "entry_count": 1, "total_bytes": 8}
    manifest_path = store / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    root = queue.root / pool.RESIDENCY
    is_ram = variant.startswith("ram")
    assert release.register_stage_root(queue, tier_id=RAM if is_ram else TIER, stage_root=ram if is_ram else stage) == "registered"
    uuid.uuid4 = lambda: uuid.UUID(G)
    secrets.token_hex = lambda size: "7" * (size * 2)
    common = ["--pool-root", str(queue.root), "--action-key", M, "--consumer-action-key", C,
              "--manifest-sha256", D, "--range-start-bytes", "0", "--range-end-bytes", "8",
              "--manifest", str(manifest_path), "--residency-root", str(root), "--readers", "1", "--max-readers", "1", "--block", "8"]
    if is_ram:
        writer = load("prismabuild.ram_promote")
        if variant != "ram-refusal-writer":
            assert tiers.ensure_ram_epoch(ram, host="compat-host")
            target = stage / mover.stage_relative(str(source), 0, 8, mount_prefix=str(origin))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"contents")
            entry = {"stage_path": str(target), "bytes": 8, "sha256": digest, "offset": 0}
            key = maps.residency_map_key(str(source), 0)
            maps.write_fragment(root, {"schema": maps.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1, "consumer_action_key": C,
                "mover_action_key": SOURCE, "tier_id": TIER, "stage_root": str(stage), "manifest_sha256": D, "entries": {key: entry}})
            lease.write_material(root, consumer_action_key=C, mover_action_key=SOURCE, tier_id=TIER, stage_root=str(stage),
                manifest_sha256=D, generation=G, entries={key: {**entry, "file_id": lease.stat_identity(str(target))}})
        args = writer.build_parser().parse_args(common + ["--tier-id", RAM, "--ram-root", str(ram), "--source-stage-root", str(stage)])
    else:
        writer = mover
        args = writer.build_parser().parse_args(common + ["--tier-id", TIER, "--stage-root", str(stage), "--unpaced",
            "--warm-after-copy", "never", "--no-incremental-fragment"])
    original_clock = time.perf_counter
    time.perf_counter = lambda: 100.0
    writer.proc_io, writer.cpu_seconds = lambda: {}, lambda: 0.0
    resource.getrusage = lambda who: SimpleNamespace(ru_maxrss=64, ru_utime=0.0, ru_stime=0.0)
    try:
        receipt = writer.promote(args) if is_ram else writer.move(args)
    finally:
        time.perf_counter = original_clock
    if family == "fragment":
        path = maps.fragment_path(root, C, M)
    else:
        path = queue.record_move(M, receipt)
    return {"raw": base64.b64encode(path.read_bytes()).decode(),
            "fixture_facts": ["real private eight-byte copy; supplied clock and process counters; ordinary directories, no hardware or throughput proof"]}
