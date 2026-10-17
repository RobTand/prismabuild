"""Real sealed spool producer, export, acknowledgement and durable intent records."""
from __future__ import annotations

import base64
import hashlib
import itertools
import json
import os
from pathlib import Path
import runpy
import time

G = "7" * 32
TIER = "prismabuild-stage:compat"


def context(store, load):
    bl = load("prismabuild.batch_ledger")
    oc = load("prismabuild.output_contract")
    spool, po, pool, core = (load("prismabuild." + name) for name in ("produced_spool", "produced_output", "pool", "core"))
    body = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["action_body"](core, store)
    template = runpy.run_path(str(Path(__file__).with_name("compat_output_adapters.py")))["template"](oc, "staged")
    template["output_prefix"] = str(store / "canonical")
    template = oc.validate_template(template)
    body["environment"]["variables"] = {spool.ROOT_ENV: str(store / "local"), spool.MAX_ENV: "8"}
    action = core.seal_action(body)
    cas = core.PrismaBuildCAS(store / "cas")
    cas.publish_action_request(action)
    owner = action["action_key"]
    queue = pool.PoolQueue(store / "queue")
    queue.records.ensure_layout()
    queue.tier(TIER).mint({"stage_gib": 2})
    published = queue.publish(action_key=owner, cas_root=cas.root, checkout_root="/compat/checkout",
        worker_script="/compat/worker.py", resources={"cpu": 1, "mem_gb": 1, **oc.owner_demand_terms(template)}, produced_output_template=template)
    claim = json.loads(published.read_bytes())
    scope = po._broker_scope_id(owner, G)
    claim.update(claimed_by="compat", claimed_host="compat-host", claimed_unix=100.,
                 resource_scope={"action_key": owner, "nonce": G, "scope_id": scope})
    published.unlink()
    queue.records.replace_record_at(queue.records.item_path(pool.CLAIMED, owner), claim)
    po.declare_template(queue.root, template)
    instance = po.bind_instance(queue, template, owner_action_key=owner, claim_snapshot=claim,
        env={core.ACTION_KEY_ENV: owner, core.ACTION_NONCE_ENV: G, core.ACTION_SCOPE_ENV: scope})
    bl.declare_instance(queue.root, instance)
    assert bl.BatchLedger.admit(queue, instance, template)["ok"]
    producer = spool.ProducedSpool(queue, instance, template, cas_root=cas.root, root=store / "local", max_bytes=8)
    destination = store / "canonical/weights"
    assert bl.BatchLedger.reserve(queue, instance, template, batch_id="batch", tier=TIER,
        class_bytes={"payload": 8, "checkpoint": 0, "temp": 0}, paths=[str(destination), str(destination) + ".tmp"])["ok"]
    payload = producer.reserve_group("batch", 8)
    source = payload / "weights"
    source.write_bytes(b"contents")
    entries = [{"source_path": str(source), "destination_path": str(destination), "bytes": 8,
                "sha256": hashlib.sha256(b"contents").hexdigest()}]
    return spool, po, pool, core, producer, entries


def operate(request, store, load):
    _, variant = request["case"].split("/", 1)
    writing = request["op"] == "write"
    spool, po, pool, core, producer, entries = context(store, load)
    group = producer._group("batch")
    queue = producer.queue
    lease = load("prismabuild.reader_lease")
    identity, fdstat, monotonic = lease.portable_identity, os.fstat, time.monotonic
    lease.portable_identity = lambda info: {"ino": 2, "size": info.st_size, "mtime_ns": 3, "ctime_ns": 4}
    temporary = entries[0]["destination_path"] + ".tmp"
    def observed_fstat(fd):
        value = fdstat(fd)
        if os.readlink(f"/proc/self/fd/{fd}") == temporary:
            return type("Stat", (), {"st_ino": 2})()
        return value
    os.fstat = observed_fstat
    try:
        if variant.startswith("reservation"):
            path = group / "reservation.json"
            if variant == "reservation-replay":
                assert producer.reserve_group("batch", 8) == group / "payload"
        else:
            producer.submit_group("batch", entries, paced=False)
            path = group / "manifest.json"
            if variant == "manifest-replay":
                assert producer.submit_group("batch", entries, paced=False)["ok"]
        record = spool._export_record(group, producer.owner) if (group / "export.json").exists() else None
        if variant.startswith("copy") or variant in ("receipt", "receipt-paced", "reservation-released"):
            if record is None:
                producer.submit_group("batch", entries, paced=False)
                record = spool._export_record(group, producer.owner)
            queue.records.replace_record_at(queue.records.item_path(pool.CLAIMED, record["export_key"]), {"claimed_host": "compat-host"})
            counter = itertools.count()
            time.monotonic = lambda: float(next(counter))
            persist, writes = spool._write, []
            class InterruptedFixture(Exception):
                pass
            def interrupted(path, value):
                persist(path, value)
                if path.name == "copy-0.json":
                    writes.append(True)
                    if len(writes) == (1 if variant == "copy-started" else 2):
                        raise InterruptedFixture("after the actual durable copy proof")
            if variant in ("copy-started", "copy-ready"):
                spool._write = interrupted
            try:
                spool.export_group(queue, group / "manifest.json", record["manifest_sha256"], record["export_key"],
                    pace_mb_s=1 if variant == "receipt-paced" else None, pace_tier=TIER if variant == "receipt-paced" else None)
            except InterruptedFixture:
                assert variant in ("copy-started", "copy-ready")
            finally:
                spool._write = persist
            if variant in ("copy-adopted", "copy-repinned"):
                manifest = spool._read(group / "manifest.json")
                entry = manifest["entries"][0]
                destination = Path(entry["destination_path"])
                with spool._parent(destination) as directory:
                    if variant == "copy-adopted":
                        others = spool._OtherClaims(queue, manifest, record["export_key"], group)
                        others._claims = {str(destination): [{"owner_action_key": "b" * 64, "owner_nonce": "8" * 32,
                            "template_id": manifest["template"]["template_id"], "owner_kind": "batch",
                            "owner_batch_id": "prior-batch", "owner_state": "succeeded"}]}
                        proof = others.take_over(directory, destination, entry, 0, record["manifest_sha256"],
                                                 spool._identity(destination, directory=directory))
                    else:
                        lease.portable_identity = lambda info: {"ino": 2, "size": info.st_size, "mtime_ns": 4, "ctime_ns": 5}
                        proof = spool._repinned(spool._read(group / "copy-0.json"),
                            spool._identity(destination, directory=directory), code="completed destination changed",
                            index=0, path=destination, name=destination.name, directory=directory, where="export")
                    spool._write(group / "copy-0.json", proof)
            time.monotonic = monotonic
            if variant == "reservation-released":
                assert producer._release_locked(group)["ok"]
                path = group / "reservation.json"
            else:
                path = group / ("copy-0.json" if variant.startswith("copy") else "receipt.json")
        if variant == "retirement-intent":
            others = spool._OtherClaims(queue, {}, record["export_key"], group)
            retire = spool._retire_foreign
            def unavailable(*args, **kwargs):
                raise OSError("fixture interrupts after durable intent, before retirement")
            spool._retire_foreign = unavailable
            try:
                with spool._parent(Path(entries[0]["destination_path"]), create=True) as directory:
                    try:
                        others._retire(directory, "weights", {"ino": 2, "size": 8, "mtime_ns": 3, "ctime_ns": 4}, Path(entries[0]["destination_path"]))
                    except OSError:
                        pass
            finally:
                spool._retire_foreign = retire
            path = group / spool.RETIRING_MARKER
        if writing:
            return {"raw": base64.b64encode(path.read_bytes()).decode(), "fixture_facts": ["supplied claim and portable stat observations; actual sealed CAS producer and private eight-byte export"]}
        raw = base64.b64decode(request["raw"])
        path.write_bytes(raw)
        if variant.startswith("manifest"):
            digest = hashlib.sha256(raw).hexdigest()
            action = dict(record["action"])
            action.pop("action_key")
            previous_digest = action["params"]["produced_spool"]["manifest_sha256"]
            action["params"]["produced_spool"]["manifest_sha256"] = digest
            for argv in (action["task"]["argv"], action["params"].get("command", [])):
                argv[:] = [digest if token == previous_digest else token for token in argv]
            for entry in action["inputs"]:
                if entry["id"] == "produced-spool-manifest":
                    entry.update(sha256=digest, bytes=len(raw))
            action = core.seal_action(action)
            record.update(action=action, export_key=action["action_key"], manifest_sha256=digest)
            spool._write(group / "export.json", record)
            queue.records.replace_record_at(queue.records.item_path(pool.CLAIMED, record["export_key"]), {"claimed_host": "compat-host"})
        request["_consumer_boundary"]()
        if variant.startswith("reservation"):
            result = producer._reservation(group)
            assert result and result["released"] == (variant == "reservation-released")
            return result
        if variant.startswith("manifest"):
            result = spool.export_group(queue, path, record["manifest_sha256"], record["export_key"])
            assert result["ok"]
            return result
        manifest = spool._read(group / "manifest.json")
        if variant.startswith("copy"):
            return spool._check_copy(json.loads(raw), manifest["entries"][0], 0, record["manifest_sha256"])
        if variant == "retirement-intent":
            result = spool._OtherClaims(queue, {}, record["export_key"], group)
            assert result._retiring
            return {"pending": len(result._retiring)}
        assert spool._check_receipt(json.loads(raw), manifest, record, destinations=False) == 0
        return {"acknowledged_entries": 1}
    finally:
        lease.portable_identity, os.fstat, time.monotonic = identity, fdstat, monotonic
