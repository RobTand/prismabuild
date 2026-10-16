"""Scratch naming and interrupted ownership writes in private supplied claims."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import runpy


def claim(store, load, *, versioned=False, persistent=False):
    scratch, pool, core, po = (load("prismabuild." + name) for name in
                               ("local_scratch", "pool", "core", "produced_output"))
    pairs = {scratch.PAIRS_ENV: "COMPAT_ROOT:COMPAT_MAX", "COMPAT_ROOT": str(store / "scratch"), "COMPAT_MAX": "16"}
    entries = [{"root_env": "COMPAT_ROOT", "name": "data"}]
    selection = scratch.build_scratch_lifetime_selection(
        [{**entry, "lifetime": "persistent" if persistent else "ephemeral"} for entry in entries]) if versioned else entries
    pairs[scratch.DECLARATIONS_ENV] = json.dumps(selection, sort_keys=True, separators=(",", ":"))
    body = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["action_body"](core, store)
    demand = {"cpu": 1, "mem_gb": 1, "spool_gb": 1}
    body["params"]["demand"] = demand
    body["environment"]["variables"] = pairs
    action = core.seal_action(body)
    key, nonce = action["action_key"], "7" * 32
    cas = core.PrismaBuildCAS(store / "cas")
    cas.publish_action_request(action)
    queue = pool.PoolQueue(store / "queue")
    path = queue.publish(action_key=key, cas_root=cas.root, checkout_root="/compat/checkout",
                         worker_script="/compat/worker.py", resources=demand, tags=["scratch-lifetime-v1"] if versioned else [])
    record = json.loads(path.read_bytes())
    scope = {"action_key": key, "nonce": nonce, "scope_id": po._broker_scope_id(key, nonce)}
    record.update(claimed_by="compat-owner", claimed_host="compat-host", claimed_unix=100.0, resource_scope=scope)
    path.unlink()
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, key), record)
    queue.write_lease(key, owner="compat-owner", claim_snapshot=record)
    env = {core.ACTION_KEY_ENV: key, core.ACTION_NONCE_ENV: nonce, core.ACTION_SCOPE_ENV: scope["scope_id"]}
    return scratch, pool, core, queue, record, env, selection


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    writing = request["op"] == "write"
    if family == "reader-ownership":
        reader, core = load("prismabuild._bounded_reader"), load("prismabuild.core")
        value = reader.ReaderOwnership(pid=2, starttime_ticks=3, host="compat-host", boot_id="7" * 8 + "-7777-4777-8777-" + "7" * 12,
            pool_identity="compat-pool", section=variant).to_record() if writing else json.loads(base64.b64decode(request["raw"]))
        if writing:
            return {"raw": base64.b64encode(core._sorted_json_bytes(value)).decode(),
                    "fixture_facts": ["supplied process identity; no liveness or drain authority"]}
        request["_consumer_boundary"]()
        return reader.ReaderOwnership.from_record(value).to_record()
    scratch, pool, core, queue, record, env, selection = claim(
        store, load, versioned=family in ("scratch-lifetime", "scratch-selection"), persistent=variant.startswith("persistent"))
    if family == "scratch-selection":
        if writing:
            raw = core._canonical_file_bytes(selection)
        else:
            variables = {scratch.PAIRS_ENV: "COMPAT_ROOT:COMPAT_MAX", "COMPAT_ROOT": str(store / "scratch"), "COMPAT_MAX": "16",
                         scratch.DECLARATIONS_ENV: base64.b64decode(request["raw"]).decode()}
            request["_consumer_boundary"]()
            result = scratch._scratch_lifetime_selections(variables)
            assert len(result) == 1 and result[0]["name"] == "data"
            return {"entries": result}
    elif family == "scratch-lifetime":
        if writing:
            prepare = scratch._prepare_scratch_directory
            if variant == "interrupted":
                def unavailable(declaration):
                    raise OSError("fixture storage unavailable before directory creation")
                scratch._prepare_scratch_directory = unavailable
            try:
                try:
                    queue._record_scratch_lifetimes(record["action_key"], claim_snapshot=record, env=env)
                except OSError:
                    assert variant == "interrupted"
                if variant == "persistent-replay":
                    queue._record_scratch_lifetimes(record["action_key"], claim_snapshot=record, env=env)
            finally:
                scratch._prepare_scratch_directory = prepare
            enclosing = queue.item_path(pool.CLAIMED, record["action_key"]).read_bytes()
            raw = runpy.run_path(str(Path(__file__).with_name("compat_batch_adapters.py")))["extract"](enclosing, scratch.SCRATCH_LIFETIME_FIELD)
        else:
            request["_consumer_boundary"]()
            return scratch._scratch_lifetime_record(json.loads(base64.b64decode(request["raw"])))
    elif writing:
        scratch.record_ephemeral_scratch_declarations(queue, claim_snapshot=record, env=env)
        if variant == "repeated":
            scratch.record_ephemeral_scratch_declarations(queue, claim_snapshot=record, env=env)
        enclosing = queue.item_path(pool.CLAIMED, record["action_key"]).read_bytes()
        extract = runpy.run_path(str(Path(__file__).with_name("compat_batch_adapters.py")))["extract"]
        raw = extract(enclosing, "scratch_declaration_record")
        if family == "scratch-ephemeral":
            raw = extract(raw, "declarations", array=True)
    else:
        request["_consumer_boundary"]()
        value = json.loads(base64.b64decode(request["raw"]))
        return (scratch._scratch_declaration(value) if family == "scratch-ephemeral" else
                scratch._scratch_declaration_record(value))
    return {"raw": base64.b64encode(raw).decode(),
            "fixture_facts": ["private supplied admitted claim; no broker, cleanup or physical identity proof"]}
