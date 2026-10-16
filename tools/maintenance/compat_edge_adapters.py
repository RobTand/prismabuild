"""Deferred submission, release and supersession production writers/consumers."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import runpy

C, M, D = "c" * 64, "a" * 64, "9" * 64


def deferred(ae, core, store, variant):
    body = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["action_body"](core, store)
    publication = {"priority": 0, "max_attempts": 1, "retry_safe": False,
                   "residency": False, "residency_tier": None, "residency_ram": "off",
                   "residency_mover_mem_gb": 1, "residency_mover_readers": 1,
                   "residency_mover_max_attempts": 1}
    if variant == "optional":
        publication.update(residency_share=True, residency_prefetch_depth_gib=1,
                           residency_read_mb_s=8, priority_reason="compat")
    return ae.deferred_body(edges=[{"producer": M, "kind": "key", "template_id": "compat"}],
                            template=body, cas_root="/compat/cas", static_manifest=None,
                            publication=publication)


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    ae, core, pool = (load("prismabuild." + name) for name in ("action_edges", "core", "pool"))
    queue = pool.PoolQueue(store / "queue")
    root = queue.root
    write = request["op"] == "write"
    if write:
        body = deferred(ae, core, store, variant)
        pending, _ = ae.file_deferred(root, body)
        if family == "edge-deferred":
            path = ae.deferred_path(root, pending)
        elif family == "edge-release":
            ae.file_release(root, pending, action_key=C,
                            producers=[{"action_key": M, "nonce": "7" * 32}], refs=[],
                            manifest_input={"id": "data_manifest", "sha256": D, "bytes": 8},
                            runtime={"root": "/compat/runtime", "generation": "compat-generation"})
            path = ae.release_path(root, pending)
        elif family == "edge-published":
            ae.file_published(root, pending, action_key=C, published_unix=100.0)
            path = ae.published_path(root, pending)
        elif family == "edge-supersession":
            ae.file_supersession(queue, pending, new=C, new_kind=variant)
            path = ae.supersession_path(root, pending)
        else:
            raise ValueError(f"unknown edge adapter: {family}")
        return {"raw": base64.b64encode(path.read_bytes()).decode()}
    raw = base64.b64decode(request["raw"])
    value = json.loads(raw)
    if family == "edge-deferred":
        key, path, reader = value["pending_id"], ae.deferred_path, ae.read_deferred
    elif family == "edge-release":
        key, path, reader = value["pending_id"], ae.release_path, ae.read_release
    elif family == "edge-published":
        key, path, reader = value["pending_id"], ae.published_path, ae.read_published
    else:
        key, path, reader = value["old"], ae.supersession_path, ae.read_supersession
    target = path(root, key)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(raw)
    result = reader(root, key)
    assert result and result.get("action_key", result.get("new", result.get("pending_id"))), "edge consumer lost binding"
    return result
