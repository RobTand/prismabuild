"""Real observation writers and consumers using bounded private sensor fixtures."""
from __future__ import annotations

import base64
import datetime
import itertools
import json
import os
from pathlib import Path
import time
from types import SimpleNamespace


def box_window(store, load):
    window = load("prismabuild.box_window")
    directory = store / "csv"
    directory.mkdir(exist_ok=True)
    (directory / "pqteld-compat-host-1970-01-01.s1.csv").write_text(
        "epoch_ms,power_draw_w,gpu_util\n95000,50,25\n")
    return window.read_window(90., 100., host="compat-host", csv_dir=directory, netdata_url=None,
        gpu_reference=lambda **kwargs: {"power_reference_w": 100., "power_reference_scope": "gpu-only", "power_reference_source": "fixture"})


def profile(store, scratch):
    root = store / "scratch"
    root.mkdir(exist_ok=True)
    observed = {"device": "1", "filesystem": "2", "root_inode": "3", "filesystem_type": "zfs"}
    before = scratch._descriptor_identity, scratch.root_identity, os.fstat, os.fstatvfs, time.perf_counter
    scratch._descriptor_identity = lambda fd: dict(observed)
    scratch.root_identity = lambda root: dict(observed)
    os.fstat = lambda fd: SimpleNamespace(st_dev=1, st_size=8)
    os.fstatvfs = lambda fd: SimpleNamespace(f_fsid=2)
    counter = itertools.count()
    time.perf_counter = lambda: float(next(counter))
    try:
        return scratch.record_profile({"root": str(root), "bytes": 8, "block_bytes": 8, "repetitions": 1})
    finally:
        scratch._descriptor_identity, scratch.root_identity, os.fstat, os.fstatvfs, time.perf_counter = before


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    writing = request["op"] == "write"
    core = load("prismabuild.core")
    if family in ("box-window", "resource-profile"):
        pool = load("prismabuild.pool")
        if writing:
            value = box_window(store, load)
            if family == "resource-profile":
                queue = pool.PoolQueue(store / "queue")
                queue._box_window = lambda *args: value
                telemetry = {"memory_peak_bytes": 32}
                outcome = {"resource_telemetry": telemetry}
                if variant == "io":
                    telemetry["process_io"] = {"read_bytes": 8, "write_bytes": 16, "members": ["private"]}
                if variant == "rusage":
                    outcome["child_rusage"] = {"user_seconds": 1., "system_seconds": 0.5}
                value = queue._resource_profile({"action_key": "c" * 64, "claimed_unix": 90.}, outcome)
        else:
            incoming = json.loads(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = pool.resource_profile_summary({"resource_profile": {"box_window": incoming} if family == "box-window" else incoming})
            assert result["gpu_power_peak_w"] == 50.
            if family == "resource-profile":
                assert result["memory_peak_bytes"] == 32
                if variant == "io":
                    assert result["io_read_bytes"] == 8 and result["io_write_bytes"] == 16
            return result
    elif family == "scratch-profile":
        scratch = load("prismabuild.local_scratch")
        if writing:
            value = profile(store, scratch)
        else:
            value = json.loads(base64.b64decode(request["raw"]))
            root = str(store / "scratch")
            offer = {"host": "compat-host", "observed_detail": {scratch.PROFILES: [{**value, "artifact_sha256": core.canonical_sha256(value)}],
                     scratch.DEVICES: {root: {"device": "1", "filesystem": "2", "root_inode": "3"}}}}
            intent = {"root": root, "declaration": {"profile_contract": scratch.PROFILE_CONTRACT, "max_profile_age_s": 10.,
                       "write_bytes": 8 if variant != "read" else 0, "read_bytes": 8 if variant != "write" else 0}}
            request["_consumer_boundary"]()
            result = scratch.profile_cost(offer, intent, now=100.)
            assert result["io_seconds"] > 0
            return result
    elif family == "scratch-profile-inputs":
        scratch = load("prismabuild.local_scratch")
        path = store / "profiles.json"
        value = {"schema": scratch.CONFIG_SCHEMA, "profiles": [{"root": str(store / "scratch")}]} if writing else json.loads(base64.b64decode(request["raw"]))
        path.write_bytes(core._canonical_file_bytes(value))
        reader = scratch.ProfileInputs(path, source_root=Path(scratch.__file__).parents[2], checkout_root=store,
                                       producer_python="/usr/bin/python3")
        original_load, original_root = reader._load, scratch.root_identity
        reader._load = lambda ref: {"host": "compat-host", "root": ref["root"], "device": "1", "filesystem": "2", "filesystem_type": "zfs", "root_inode": "3"}
        scratch.root_identity = lambda root: {"device": "1", "filesystem": "2", "filesystem_type": "zfs", "root_inode": "3"}
        if not writing:
            request["_consumer_boundary"]()
        try:
            result = reader.observe()
            assert len(result[scratch.PROFILES]) == 1
        finally:
            reader._load, scratch.root_identity = original_load, original_root
        if not writing:
            return result
    else:
        canary, gc = load("pbcanary"), load("pb_gc")
        namespace = store / "canary/compat-run"
        namespace.mkdir(parents=True)
        path = namespace / "run.json"
        if writing:
            original = datetime.datetime
            class Clock(original):
                @classmethod
                def now(cls, tz=None):
                    return original.fromtimestamp(100, tz=tz)
            datetime.datetime = Clock
            try:
                value = canary.build_run_record(run_id="compat-run", generation=None if variant == "legacy" else "compat-generation",
                    requested=["cpu"], priority=10, checkout=Path("/compat/checkout"), published_root=Path("/compat/published"))
                canary.write_json(path, value)
            finally:
                datetime.datetime = original
            return {"raw": base64.b64encode(path.read_bytes()).decode()}
        path.write_bytes(base64.b64decode(request["raw"]))
        request["_consumer_boundary"]()
        result = gc._survey_canary(namespace.parent, min_age_s=0., held_inodes=set())
        assert len(result["remove"]) == 1 and not result["keep"]
        return {"removable": 1, "deleted": False}
    return {"raw": base64.b64encode(core._canonical_file_bytes(value)).decode(),
            "fixture_facts": ["supplied sensor/identity observations; real private eight-byte recorder IO; config loader IO supplied; no throughput, root or admission proof"]}
