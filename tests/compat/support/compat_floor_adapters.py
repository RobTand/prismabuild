"""Private floor ledgers with controlled capacity and identity observations."""
from __future__ import annotations

import base64
import json
import os
import uuid
from types import SimpleNamespace

KEY, HOST = "uuid:compat", "compat-host"


def prepare(store, load):
    ff, pool = load("prismabuild.filesystem_floor"), load("prismabuild.pool")
    queue = pool.PoolQueue(store / "queue")
    queue.records.ensure_layout()
    root = store / "data"
    root.mkdir()
    # These are external sensor inputs, not evidence about the fleet's disk
    # or process identity. All locks, counters, tokens and file I/O are real.
    ff.identify = lambda path: {"key": KEY, "path": str(root), "fstype": "btrfs",
                               "size_bytes": 64 * ff.GIB, "free_bytes": 60 * ff.GIB}
    ff._sample = lambda found: {"size_bytes": 64 * ff.GIB, "free_bytes": 60 * ff.GIB,
                               "size_inodes": 1000, "free_inodes": 900, "floor_inodes": 50, "inode_refusal": None}
    ff._machine_id = lambda: "f" * 32
    ff._floor_boot_id = lambda: "7" * 32
    ff._host_addresses = lambda: ["192.0.2.1"]
    ff._process_start = lambda pid: "7"
    uuid.uuid4 = lambda: SimpleNamespace(hex="7" * 32)
    os.environ[ff.MODE_ENV] = "enforce"
    member = ("tier-reservations", "compat-stage", "stage_gib")
    binding = ff.register(queue.root, root, members=[member], filesystem_gib=4,
                          filesystem_ledger="compat-fs", now=100.0)["binding"]
    return ff, pool, queue, root, member, binding


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    ff, pool, queue, root, member, binding = prepare(store, load)
    directory = ff.fs_dir(queue.root, KEY)
    write = request["op"] == "write"
    if family == "floor-owner":
        ledger = pool.ResourceLedger(queue.root / ff.FILESYSTEM_RESERVATIONS, host="compat-fs")
        holder = f"operation-{HOST}-2-77777777"
        path = ledger.base / "operations" / (holder + ".json")
        amount = 2 if variant == "two" else 1
        if write:
            with ff.operation(queue.root, growth_gib={root: amount}, label="compat", lease_s=60):
                assert ledger.held().get("filesystem_gib") == amount
                raw = path.read_bytes()
            assert not path.exists() and ledger.held().get("filesystem_gib", 0) == 0
        else:
            assert ledger.acquire(holder, {"filesystem_gib": amount})
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = ff.reap_operations(queue.root, now=200.0)
            assert result == [holder] and not path.exists(), "real reaper retained the expired owner"
            assert ledger.held().get("filesystem_gib", 0) == 0
            return result
    elif family == "floor-granted":
        path = directory / "granted.json"
        amount = 2 if variant == "two" else 1
        if write:
            gate = ff.LedgerGate(queue.root, "enforce", {}, {KEY: {"binding": binding, "bytes": amount * ff.GIB}}, [])
            with gate.admitted() as admitted:
                assert admitted
                gate.granted()
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = ff._granted(directory)
            assert result == amount * ff.GIB, "grant consumer lost its byte charge"
            return result
        raw = path.read_bytes()
    else:
        path = (ff.member_path(queue.root, *member) if family == "floor-member" else
                directory / ("binding.json" if family == "floor-binding" else "sample.json"))
        if write:
            if variant == "grown":
                ff.register(queue.root, root, members=[("reservations", HOST, "spool_gb")], now=101.0)
            if variant == "charged":
                queue.tier("compat-stage").mint({"stage_gib": 1})
                assert queue.tier("compat-stage").take("compat-holder", {"stage_gib": 1})
                ff.refresh_binding(queue.root, binding, now=100.0)
            raw = path.read_bytes()
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            if family == "floor-member":
                result = ff.register(queue.root, root, members=[member], now=100.0)
                assert result["binding"]["key"] == KEY
                return result
            if family == "floor-binding":
                table = ff.member_bindings(queue.root, max_age_s=0)
                binding = table[ff.member_id(*member)]
            result = ff.published_verdict(queue.root, binding, now=100.0)
            assert result["allowed"] is True, result
            if family == "floor-sample":
                assert result["charge_bytes"] == (ff.GIB if variant == "charged" else 0)
            return result
    return {"raw": base64.b64encode(raw).decode()}
