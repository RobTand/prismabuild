"""Exercise pin serialization and cleanup export filing without a live broker."""
from __future__ import annotations

import base64
import json
import uuid
import socket

C, M, G, N, S = "c" * 64, "a" * 64, "7" * 32, "9" * 32, "compat-scope"


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    lease, pool = load("prismabuild.reader_lease"), load("prismabuild.pool")
    queue = pool.PoolQueue(store / "queue")
    queue.records.ensure_layout()
    if family == "lease-pin":
        path = store / "pin.json"
        if request["op"] == "write":
            if variant == "acquired":
                maps, release = load("prismabuild.residency_map"), load("prismabuild.staged_path")
                root, stage = queue.root / pool.RESIDENCY, store / "stage"
                stage.mkdir()
                staged = stage / "weights"
                staged.write_bytes(b"contents")
                identity = {"ino": 2, "size": 8, "mtime_ns": 3, "ctime_ns": 4}
                lease.stat_identity = lambda name: dict(identity)
                uuid.uuid4 = lambda: uuid.UUID(G)
                assert release.register_stage_root(queue, tier_id="stage:compat", stage_root=stage) == "registered"
                entry = {"stage_path": str(staged), "bytes": 8, "sha256": "d" * 64, "offset": 0}
                maps.write_fragment(root, {"schema": maps.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
                    "consumer_action_key": C, "mover_action_key": M, "tier_id": "stage:compat", "stage_root": str(stage),
                    "manifest_sha256": "d" * 64, "entries": {"0:/compat/input": entry}})
                lease.write_material(root, consumer_action_key=C, mover_action_key=M, tier_id="stage:compat", stage_root=str(stage),
                    manifest_sha256="d" * 64, generation=G, entries={"0:/compat/input": {**entry, "file_id": identity}})
                result = lease.acquire(queue, consumer_action_key=C, attempt={"nonce": N, "scope_id": S},
                    tier_id="stage:compat", epoch="", span={"start_bytes": 0, "end_bytes": 8},
                    holder={"host": socket.gethostname(), "pid": 2}, acquire_token="compat-token",
                    covers=[{"mover_action_key": M, "manifest_sha256": "d" * 64}])
                assert result["ok"], result
                pins = list((root / lease.LEASES_SUBDIR).rglob("*.lease.json"))
                assert len(pins) == 1
                return {"raw": base64.b64encode(pins[0].read_bytes()).decode(),
                        "fixture_facts": ["private eight-byte material; supplied inode timestamps, no physical custody proof"]}
            ref = {"acquire_token": "compat-token", "attempt": {"nonce": N, "scope_id": S},
                   "holder": {"host": "sparklina", "pid": 2}, "unix": 100.0}
            value = {"schema": lease.LEASE_SCHEMA_V1, "pin_id": G, "consumer_action_key": C,
                     "owner_action_key": M if variant == "separate-owner" else C,
                     "tier_id": "ram:compat" if variant == "ram" else "stage:compat", "epoch": "epoch" if variant == "ram" else "",
                     "manifest_sha256": "d" * 64, "stage_root": "/compat/stage",
                     "range": {"coordinate_space": "rng01-source", "start_bytes": 0, "end_bytes": 8},
                     "covers": [{"mover_action_key": M, "generation": G}],
                     "entries": [{"key": "0:/compat/input", "stage_path": "/compat/stage/weights", "bytes": 8,
                                  "sha256": "d" * 64, "file_id": {"ino": 2, "size": 8, "mtime_ns": 3, "ctime_ns": 4},
                                  "mover_action_key": M, "generation": G}],
                     "ram": {"epoch": "epoch"} if variant == "ram" else None,
                     "refs": {"first": ref, **({"inherited": {**ref, "acquire_token": "child-token"}} if variant == "inherited" else {})}}
            lease._write_pin(path, value)
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            value = lease._read_pin(path)
            assert isinstance(value, dict), f"pin consumer refused: {value!r}"
            assert len(value["refs"]) == (2 if variant == "inherited" else 1)
            return value
    else:
        path = lease.attestation_path(queue, C, N)
        if request["op"] == "write":
            # Supplied export facts test wire semantics only. No RPC or physical
            # stopped-scope claim is made by this private fixture.
            record = {"action_key": C, "claimed_by": "sparklina:2:compat", "claimed_host": "sparklina"}
            verdict = {"scope_id": S, "released": True, "retired": False, "settled": False,
                       "empty": True, "tickets_pending": False, "stopped_unix": 99.0}
            if variant in ("retired", "unsettled"):
                verdict.update(released=False, retired=True, settled=variant == "retired")
            elif variant == "unknown-tickets":
                verdict.pop("tickets_pending")
            elif variant == "pending":
                verdict["tickets_pending"] = True
            assert queue._persist_reader_scope_proof(record, N, S, verdict)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            value = lease.read_scope_attestation(queue, C, N)
            assert isinstance(value, dict), f"scope-attestation consumer refused: {value!r}"
            proven, reason = lease.attestation_proves_empty(queue, C, N, S)
            assert proven == (variant in ("released", "retired")), reason
            return value
    return {"raw": base64.b64encode(path.read_bytes()).decode()}
