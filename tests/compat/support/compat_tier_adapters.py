"""Private tier discovery and actual residency projection writers."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import runpy
from types import SimpleNamespace
import time
import io
from contextlib import redirect_stdout

C = "c" * 64


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    writing = request["op"] == "write"
    if family == "storage-tier":
        tiers = load("prismabuild.storage_tiers")
        if writing and variant == "retired-writer":
            pool, loop = load("prismabuild.pool"), load("tier_loop")
            queue = pool.PoolQueue(store / "queue")
            name = "prismabuild-stage:compat"
            queue.tier(name).mint({"stage_gib": 1})
            queue.announce_tier({"schema": tiers.TIER_RECORD_SCHEMA_V1, "tier_id": name, "tier": "stage", "host": "compat",
                                "mountpoint": str(store / "stage"), "capacity_bytes": tiers.GIB})
            original = time.monotonic
            time.monotonic = lambda: 100.0
            printed = io.StringIO()
            try:
                with redirect_stdout(printed):
                    loop.cycle(queue, host="compat", source_pool="compat", receipts=loop.ReceiptCache(), now=100.0,
                               discover=lambda **kwargs: {}, supply_reader_for=lambda *args: None)
            finally:
                time.monotonic = original
            return {"raw": base64.b64encode(queue.tier_record_path(name).read_bytes()).decode(),
                    "fixture_output": printed.getvalue(),
                    "fixture_facts": ["private tier retirement with supplied absent discovery/supply; no real hardware or root registration"]}
        if writing and variant.startswith("ram"):
            root = store / "ram"
            root.mkdir()
            (root / tiers.RAM_EPOCH_MARKER).write_text(json.dumps({"schema": tiers.RAM_EPOCH_MARKER_SCHEMA_V1,
                "epoch": "100-compat", "unix": 100, "host": "compat-host"}))
            mounts, meminfo, numa = store / "mounts", store / "meminfo", store / "numa"
            mounts.write_text(f"tmpfs {root} tmpfs rw,noswap 0 0\n")
            meminfo.write_text("MemTotal: 33554432 kB\n")
            (numa / "node0").mkdir(parents=True)
            (numa / "has_memory").write_text("0\n")
            (numa / "node0/meminfo").write_text("Node 0 MemTotal: 33554432 kB\n")
            policy = {"mountpoint": str(root), "window_gib_default": 4, "ceiling_gib_max": 8,
                      "arc_floor_gib": 0, "system_reserve_gib": 1}
            def capacity(path):
                if variant == "ram-refused":
                    raise OSError("private unavailable capacity fixture")
                return SimpleNamespace(f_blocks=8, f_bavail=8, f_frsize=tiers.GIB, f_files=100, f_favail=100)
            value = tiers.ram_tier(policy, host="compat-host", statvfs=capacity, proc_mounts=str(mounts),
                meminfo_path=str(meminfo), memory_numa_root=str(numa), stats={}, rows_held_gib=0, now=100.)
            assert value["ram_admission"]["admissible"] == (variant == "ram"), value
        elif writing:
            arc = store / "arcstats"
            arc.write_text("c_max 4 4294967296\narc_meta_used 4 0\nsize 4 1024\nc 4 4294967296\n")
            pressure = store / "pressure"
            pressure.write_text("some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")
            name = tiers.STAGE_POOL_PREFIX + "compat"
            def runner(argv):
                if "list" in argv and "name,size,allocated,free,health" in argv:
                    return f"{name}\t8589934592\t0\t8589934592\tONLINE\n" if variant == "stage" else ""
                if "name,available,mountpoint,primarycache" in argv:
                    return f"{name}/prewarm\t4294967296\t/compat/stage\tall\n"
                return ""
            value = tiers.discover_tiers(host="compat-host", runner=runner, arcstats_path=str(arc),
                sysfs=str(store / "sys"), by_id=str(store / "by-id"), proc_pressure=str(pressure), now=100.)
            value = next(row for row in value.values() if row["tier"] == variant)
        else:
            request["_consumer_boundary"]()
            value = json.loads(base64.b64decode(request["raw"]))
            tokens = tiers.tier_tokens(value)
            if variant == "retired-writer":
                assert not any(tokens.values()), tokens
            elif variant == "ram-refused":
                assert not tokens
            else:
                assert tokens.get("ram_gib" if variant == "ram" else "stage_gib" if variant == "stage" else "arc_gib") == 4
            return tokens
    else:
        rp, pool = load("prismabuild.residency_plan"), load("prismabuild.pool")
        queue = pool.PoolQueue(store / "queue")
        fixture = runpy.run_path(str(Path(__file__).with_name("compat_plan_adapters.py")))["plan"]
        plan = fixture(rp, pool, "ram-chunks" if variant == "ram" else "chunks")
        rp.freeze(queue, plan)
        if family == "residency-block":
            if writing:
                value = rp._filed_entry(queue, C)["residency"]
            else:
                request["_consumer_boundary"]()
                value = json.loads(base64.b64decode(request["raw"]))
                result = queue.validate_residency(value, {})
                assert result["leads"] and result["manifest_bytes"] == 8
                return result
        else:
            # Incarnation is a supplied stat observation, never physical custody evidence.
            observe = rp.incarnation
            rp.incarnation = lambda path: (2, 3, 4)
            try:
                path = rp._superseded_path(queue, C, rp.plan_sha256(plan))
                if writing:
                    value = rp.mark_superseded(queue, C, reason="compat", movers=["a" * 64], by="compat")
                    if variant == "replay":
                        assert rp.mark_superseded(queue, C, reason="changed", by="other") == value
                    return {"raw": base64.b64encode(path.read_bytes()).decode()}
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(base64.b64decode(request["raw"]))
                request["_consumer_boundary"]()
                result = rp.superseded(queue, plan)
                assert result and not result.get("unreadable") and result["reason"] == "compat"
                return result
            finally:
                rp.incarnation = observe
    return {"raw": base64.b64encode(json.dumps(value, sort_keys=True).encode()).decode()}
