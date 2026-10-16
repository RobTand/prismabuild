"""Deterministic private-store scenarios, shared unchanged by both generations."""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import runpy
import socket
import time

C, M, D, G = "c" * 64, "a" * 64, "9" * 64, "7" * 32
TOKEN = "compat-token"


def fixture(family, variant, module):
    if family in ("fragment", "map"):
        ram = variant == "ram"
        entry = {"stage_path": "/compat/stage/weights", "bytes": 8,
                 "offset": 0, "sha256": D}
        fragment = {"schema": module.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
                    "consumer_action_key": C, "mover_action_key": M,
                    "tier_id": "ram:compat" if ram else "stage:compat",
                    "stage_root": "/compat/ram" if ram else "/compat/stage",
                    "manifest_sha256": D, "entries": {"0:/compat/input": entry}}
        if ram:
            entry["stage_path"] = "/compat/ram/weights"
            fragment["epoch"] = "epoch-1"
        if family == "fragment":
            return fragment
        if ram:
            base = fixture("fragment", "ssd", module)
            return module.overlay_ram(module.compose([base]), [fragment],
                                      ram_tier_id="ram:compat", ram_root="/compat/ram",
                                      ram_epoch="epoch-1")
        return module.compose([fragment])
    if family == "landing":
        state = variant if variant in module.LANDING_STATES else "unpublished"
        row = {"mover_action_key": M, "phase": "copy", "chunk_index": 0,
               "range_start_bytes": 8, "range_end_bytes": 16, "state": state,
               "queue_position": 0, "bytes_ahead": 0,
               "expected_landing_unix": 101.0 if state in ("ready", "claimed") else None,
               "waiting_for": "copy"}
        value = {"schema": module.RESIDENCY_LANDING_SCHEMA_V1,
                 "consumer_action_key": C, "tier_id": "stage:compat",
                 "manifest_sha256": D, "written_unix": 100.0,
                 "landing_bytes_per_s": 8.0, "landing_basis": "compat",
                 "rates_measured_bytes_per_s": [8.0], "rate_min_bytes_per_s": 8.0,
                 "rate_max_bytes_per_s": 8.0, "report_latency_s": 1.0,
                 "tier_loop_liveness_s": 1.0, "publish_s": 1.0, "ranges": [row]}
        if variant in ("horizon", "null-horizon"):
            value["horizon"] = None if variant == "null-horizon" else {
                "end_bytes": 8, "accepted_phase": "copy", "reading_phase": "copy",
                "advance_mover_action_key": M, "consumption_bytes_per_s": 8.0,
                "consumption_basis": "measured", "readahead_bytes": 8,
                "reach_end_bytes": 16, "declared_wait_end_bytes": None}
            if variant == "horizon":
                row["deferred_by"] = "horizon"
        if variant in ("reported", "claim", "queue"):
            row.update(state="claimed", expected_landing_unix=101.0, basis=variant,
                       landed_bytes=4, copied_bytes=6, started_unix=99.0,
                       reported_unix=100.0, live_bytes_per_s=4.0)
            row["movers_ahead"] = ["b" * 64]
        if variant == "held":
            row.update(held_back_by="b" * 64, waiting_on="d" * 64,
                       waiting_gib=1, claim_rank=1, expected_landing_basis="claim",
                       expected_landing_unix=102.0)
        return value
    raise ValueError(f"no fixture: {family}/{variant}")


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    write = request["op"] == "write"
    time.time = lambda: 100.0
    socket.gethostname = lambda: "compat-host"
    os.environ["PRISMABUILD_BOX_STATE_ROOT"] = str(store / "box-state")
    os.environ.pop("PRISMABUILD_ACTION_PROGRESS_PHASES", None)
    if family.startswith("scratch-") or family == "reader-ownership":
        adapter = runpy.run_path(str(Path(__file__).with_name("compat_scratch_adapters.py")))["operate"]
        return adapter(request, store, load)
    if family.startswith("input-"):
        adapter = runpy.run_path(str(Path(__file__).with_name("compat_input_adapters.py")))["operate"]
        return adapter(request, store, load)
    if family.startswith("state-"):
        adapter = runpy.run_path(str(Path(__file__).with_name("compat_state_adapters.py")))["operate"]
        return adapter(request, store, load)
    if family.startswith("floor-"):
        adapter = runpy.run_path(str(Path(__file__).with_name("compat_floor_adapters.py")))["operate"]
        return adapter(request, store, load)
    if family.startswith("broker-"):
        adapter = runpy.run_path(str(Path(__file__).with_name("compat_broker_adapters.py")))["operate"]
        return adapter(request, store, load)
    if family.startswith("upgrade-"):
        adapter = runpy.run_path(str(Path(__file__).with_name("compat_upgrade_adapters.py")))["operate"]
        return adapter(request, store, load)
    if family.startswith("host-"):
        adapter = runpy.run_path(str(Path(__file__).with_name("compat_host_adapters.py")))["operate"]
        return adapter(request, store, load)
    if family.startswith("lease-"):
        adapter = runpy.run_path(str(Path(__file__).with_name("compat_lease_adapters.py")))["operate"]
        return adapter(request, store, load)
    if family.startswith("config-"):
        adapter = runpy.run_path(str(Path(__file__).with_name("compat_config_adapters.py")))["operate"]
        return adapter(request, store, load)
    if family.startswith("dc-"):
        adapter = runpy.run_path(str(Path(__file__).with_name("compat_domain_adapters.py")))["operate"]
        return adapter(request, store, load)
    if family.startswith("q-"):
        filename = ("compat_pool_wire_adapters.py" if family in ("q-item", "q-offer", "q-intent", "q-lease", "q-attempt",
                                                                "q-outcome", "q-tier-commitment", "q-stage-holder")
                    else "compat_queue_adapters.py")
        adapter = runpy.run_path(str(Path(__file__).with_name(filename)))["operate"]
        return adapter(request, store, load)
    if family.startswith("core-"):
        filename = ("compat_receipt_adapters.py" if family in ("core-receipt", "core-attestation", "core-runtime", "core-claim")
                    else "compat_core_adapters.py")
        adapter = runpy.run_path(str(Path(__file__).with_name(filename)))["operate"]
        return adapter(request, store, load)
    if family.startswith(("edge-", "out-")):
        filename = "compat_edge_adapters.py" if family.startswith("edge-") else "compat_output_adapters.py"
        adapter = runpy.run_path(str(Path(__file__).with_name(filename)))["operate"]
        return adapter(request, store, load)
    if family.startswith(("resident-", "tier-")):
        adapter = runpy.run_path(str(Path(__file__).with_name("compat_resident_adapters.py")))["operate"]
        return adapter(request, store, load)
    if family in ("plan", "shared-range", "shared-mover"):
        adapter = runpy.run_path(str(Path(__file__).with_name("compat_plan_adapters.py")))["operate"]
        return adapter(request, store, load)
    if family.startswith("gang-"):
        adapter = runpy.run_path(str(Path(__file__).with_name("compat_gang_adapters.py")))["operate"]
        return adapter(request, store, load)
    if family.startswith("pg-"):
        adapter = runpy.run_path(str(Path(__file__).with_name("compat_prelaunch_adapters.py")))["operate"]
        return adapter(request, store, load)
    path = store / "record.json"
    if not write:
        path.write_bytes(base64.b64decode(request["raw"]))

    if family in ("fragment", "map", "landing"):
        rm = load("prismabuild.residency_map")
        if write:
            value = fixture(family, variant.removeprefix("empty-"), rm)
            if family == "map" and variant.startswith("empty-"):
                seed = store / "seed.map.json"
                rm.write_map(seed, value)
                value = load("tier_loop")._emptied_map(seed)
                assert isinstance(value, dict) and not value["entries"]
            if family == "fragment":
                path = rm.write_fragment(store, value)
            elif family == "map":
                path = rm.write_map(path, value)
            else:
                path = rm.write_landing(path, value)
        else:
            if family == "fragment":
                destination = rm.fragment_path(store, C, M)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(path.read_bytes())
                request["_consumer_boundary"]()
                rows = rm.read_fragments(store, C)
                assert len(rows) == 1, "fragment consumer skipped the record"
                value = rows[0]
            elif family == "map":
                request["_consumer_boundary"]()
                value = rm.read_map(path)
                found = rm.lookup(value, "/compat/input")
                if variant.startswith("empty-"):
                    assert found is None and value["leads"] == [] and not value["entries"]
                else:
                    assert found and found["bytes"] == 8, "map did not resolve the declared input"
            else:
                request["_consumer_boundary"]()
                value = rm.read_landing(path)
                assert len(value["ranges"]) == 1 and value["ranges"][0]["range_end_bytes"] == 16
            return value
    elif family in ("material", "retiring"):
        lease = load("prismabuild.reader_lease")
        if write:
            if family == "material":
                args = dict(consumer_action_key=C, mover_action_key=M,
                            tier_id="ram:compat" if variant == "ram" else "stage:compat",
                            stage_root="/compat/stage", manifest_sha256=D, generation=G,
                            entries={"0:/compat/input": {"stage_path": "/compat/stage/weights",
                                     "bytes": 8, "sha256": D,
                                     "file_id": {"dev": 1, "ino": 2, "size": 8,
                                                 "mtime_ns": 3, "ctime_ns": 4}}})
                if variant == "ram":
                    args["epoch"] = "epoch-1"
                elif variant == "empty-epoch":
                    args["epoch"] = ""
                path = lease.write_material(store, **args)
            else:
                path = lease.write_retiring(store, consumer_action_key=C,
                                            mover_action_key=M, generation=G)
        else:
            if family == "material":
                destination = lease.material_path(store, C, M)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(path.read_bytes())
                request["_consumer_boundary"]()
                value = lease.read_material(store, C, M)
                assert isinstance(value, dict), f"material consumer refused: {value!r}"
            else:
                destination = lease.retiring_path(store, C, M)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(path.read_bytes())
                request["_consumer_boundary"]()
                rows, taint = lease.retiring_for(store, M)
                assert len(rows) == 1 and not taint, f"retiring consumer refused: {taint}"
                value = rows[0]
            assert value["generation"] == G
            return value
    else:
        pool = load("prismabuild.pool")
        queue = pool.PoolQueue(store / "queue")
        if family in ("move", "egress", "prewarm", "export"):
            if write:
                value = {"action_key": M, "tier_id": "stage:compat", "seconds": 2.0,
                         "bytes": 8, "unix": 100.0, "note": "caf\u00e9 \u03bb"}
                if family == "prewarm":
                    if variant == "stage":
                        value["stage"] = {"staged_through_bytes": 8, "swept": False}
                    path = queue.record_prewarm(M, value)
                elif family == "export":
                    value.update(schema=pool.POOL_EXPORT_SCHEMA_V1, owner=C)
                    path = queue.record_export(value)
                else:
                    if family == "egress":
                        value.update(schema=pool.POOL_EGRESS_SCHEMA_V1,
                                     entries_deleted=1, bytes_deleted=8, complete=True)
                    elif variant == "explicit":
                        value["schema"] = pool.POOL_MOVE_SCHEMA_V1
                    path = queue.record_move(M, value)
            else:
                if family == "prewarm":
                    destination = queue.prewarm_path(M)
                elif family == "export":
                    destination = queue.export_path(M)
                else:
                    destination = queue.move_path(M)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(path.read_bytes())
                request["_consumer_boundary"]()
                if family == "prewarm":
                    value = queue.prewarm(M)
                elif family == "export":
                    rows = queue.export_records(C, "stage:compat")
                    assert len(rows) == 1, "export reader skipped the record"
                    value = rows[0]
                else:
                    value = queue.move_record(M)
                    schemas = (pool.POOL_EGRESS_SCHEMA_V1,) if family == "egress" else (pool.POOL_MOVE_SCHEMA_V1,)
                    assert len(queue.move_records(schemas=schemas)) == 1
                assert value and value["action_key"] == M, "queue consumer skipped the record"
                return value
        elif family in ("staged-wait", "export-wait", "progress"):
            progress = load("prismabuild.progress")
            destination = queue.action_progress_path(M)
            os.environ[progress.ACTION_PROGRESS_PATH_ENV] = str(destination)
            os.environ[progress.ACTION_PROGRESS_TOKEN_ENV] = TOKEN
            if family == "staged-wait":
                destination = Path(progress.staged_wait_path(str(destination)))
            elif family == "export-wait":
                destination = Path(progress.export_wait_path(str(destination)))
            if write:
                if family == "staged-wait":
                    assert progress.declare_staged_wait([M], since_unix=99.0)
                elif family == "export-wait":
                    assert progress.declare_export_wait([M], since_unix=99.0)
                else:
                    movement = load("prismabuild.movement_actions")
                    phase = movement.MOVER_PROGRESS_PHASES[0]
                    if variant.startswith("core"):
                        writer = load("prismabuild.core").report_action_progress
                        assert writer(phase, 8, **({"unit": "bytes"} if variant.endswith("unit") else {}))
                    else:
                        assert progress.commit(8, phase, **({"unit": "bytes"} if variant.endswith("unit") else {}))
                path = destination
            else:
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(path.read_bytes())
                request["_consumer_boundary"]()
                if family == "staged-wait":
                    value, reason = pool.read_staged_wait(destination, token=TOKEN)
                    assert not reason, reason
                elif family == "export-wait":
                    value, reason = pool.read_export_wait(destination, token=TOKEN)
                    assert not reason, reason
                else:
                    value = queue.mover_report(M)
                assert value, "wait/progress consumer refused the record"
                return value
        else:
            raise ValueError(f"unknown adapter: {family}")
    return {"raw": base64.b64encode(path.read_bytes()).decode()}
