"""Actual whole-set and RAM epoch writers in deterministic private directories."""
from __future__ import annotations

import base64
import errno
import hashlib
import json
import os
from pathlib import Path
import secrets


def canonical_directory(store):
    # A process-owned directory FD gives the real fixture a stable wire path.
    # No shared directory or fabricated stat result is needed for byte goldens.
    descriptor = 197
    try:
        os.fstat(descriptor)
    except OSError as error:
        if error.errno != errno.EBADF:
            raise
    else:
        raise RuntimeError("reserved fixture directory FD is already occupied")
    opened = os.open(store, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.dup2(opened, descriptor, inheritable=False)
    finally:
        os.close(opened)
    root = Path(f"/proc/self/fd/{descriptor}/canonical")
    root.mkdir()
    (root / "weights").write_bytes(b"weights")
    return root


def resident_set(rs, core, pool, store, variant):
    directory = canonical_directory(store)
    manifest = {"schema": core.DATA_MANIFEST_SCHEMA_V1, "produced_by": {}, "annotations": {},
                "mount_prefix": str(directory), "entries": [{"path": str(directory / "weights"),
                "offset": 0, "bytes": 7, "sha256": hashlib.sha256(b"weights").hexdigest()}],
                "entry_count": 1, "total_bytes": 7}
    queue = pool.PoolQueue(store / "queue")
    queue.mint_tier_capacity("local:compat-host", {"local_gib": 1})
    lease = {"until": 150, "hard_max": 200}
    if variant == "campaign":
        lease = {"campaign": "compat", "hard_max": 200}
    resident = rs.ResidentSets(queue.root)
    value = resident.publish(manifest=manifest, canonical_root=str(directory), hosts=["compat-host"],
                             lease=lease, created_by="compat", now=100)
    return resident, value


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    write = request["op"] == "write"
    if family == "tier-epoch":
        tiers = load("prismabuild.storage_tiers")
        path = store / tiers.RAM_EPOCH_MARKER
        if write:
            secrets.token_hex = lambda count: "7" * (2 * count)
            assert tiers.ensure_ram_epoch(store, host="compat-host", now=100)
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            value = tiers.read_ram_epoch(store)
            assert value and value["epoch"] == "100-" + "7" * 16, "epoch reader lost mounted generation"
            return value
    else:
        rs, core, pool = (load("prismabuild." + name) for name in ("resident_sets", "core", "pool"))
        if write:
            resident, body = resident_set(rs, core, pool, store, variant)
            key = body["set_id"]
            if family == "resident-set":
                path = resident.set_path(key)
            elif family == "resident-copy":
                resident.write_copy(key, "compat-host", {"state": variant, "local_root": "/compat/local",
                                    "verification": [], "bytes": 7 if variant == "resident" else 0,
                                    "completed_unix": 100 if variant == "resident" else None})
                path = resident.copy_path(key, "compat-host")
            elif family == "resident-movements":
                resident.update_movements(key, "compat-host", {variant: {"action_key": "a" * 64, "status": "filed"}})
                path = resident.movement_path(key, "compat-host")
            else:
                raise ValueError(f"unknown resident adapter: {family}")
        else:
            raw = base64.b64decode(request["raw"])
            value = json.loads(raw)
            resident = rs.ResidentSets(store / "queue")
            key = value["set_id"]
            if family == "resident-set":
                path, reader, args = resident.set_path(key), resident.read, (key,)
            elif family == "resident-copy":
                path, reader, args = resident.copy_path(key, "compat-host"), resident.read_copy, (key, "compat-host")
            else:
                path, reader, args = resident.movement_path(key, "compat-host"), resident.read_movements, (key, "compat-host")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            result = reader(*args)
            assert result, "resident consumer lost its body or movement rows"
            if family == "resident-copy":
                assert result["state"] == variant
            return result
    return {"raw": base64.b64encode(path.read_bytes()).decode()}
