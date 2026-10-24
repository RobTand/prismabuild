"""Build a pre-contract fleet with the e9e9d907 (live) runtime's own code.

python3 -I review_r5_legacy_gen.py LEGACY_TREE WORLD_ROOT
Prints one JSON line describing what the legacy runtime wrote (derived fields only).
"""
import json
import os
import subprocess
import sys
import traceback
from pathlib import Path

legacy = Path(sys.argv[1]).resolve()
world_root = Path(sys.argv[2]).resolve()
sys.path[:0] = [str(legacy / "src"), str(legacy / "tests")]
os.chdir(world_root)

import prismabuild  # noqa: E402
assert Path(prismabuild.__file__).resolve().is_relative_to(legacy), prismabuild.__file__
from prismabuild import core as pb, pool, record_store  # noqa: E402
from fleet_world import Admission, FleetWorld  # noqa: E402

pool.LOCAL_CHECKOUT_ROOT = world_root / "materialized"
world = FleetWorld(world_root, admission=Admission(capacity={"cpu": 8, "mem_gb": 16},
                                                   default_demand={"cpu": 1, "mem_gb": 1}))
q = world.queue
raw = getattr(q, "queue", q)
report = {"legacy_module": str(Path(prismabuild.__file__).relative_to(legacy)), "errors": {},
          "queue_root": str(raw.root)}
K_DONE, K_LIVE, K_LSNAP = "d" * 64, "a" * 64, "b" * 64


def step(name, fn):
    try:
        report[name] = fn()
    except Exception as exc:                                 # noqa: BLE001
        report["errors"][name] = f"{type(exc).__name__}: {exc} | {traceback.format_exc()[-600:]}"


def snapshot():
    source = world_root / "materialization-source"
    source.mkdir()
    git = ["git", "-C", str(source)]
    subprocess.run(git + ["init", "-q"], check=True)
    subprocess.run(git + ["config", "user.name", "PrismaBuild test"], check=True)
    subprocess.run(git + ["config", "user.email", "test@example.invalid"], check=True)
    (source / "payload.txt").write_text("sealed lifecycle bytes\n")
    subprocess.run(git + ["add", "payload.txt"], check=True)
    subprocess.run(git + ["commit", "-qm", "sealed lifecycle source"], check=True)
    identity = pb.git_checkout_identity(source)
    stamp_name = f"{pb.PBRUN_STAMP_PREFIX}lifecycle.json"
    (source / stamp_name).write_text(json.dumps({"cwd": ".", **identity}, indent=1, sort_keys=True))
    snap = world_root / "snapshot-source"
    subprocess.run(["git", "clone", "-q", str(source), str(snap)], check=True)
    (snap / stamp_name).write_bytes((source / stamp_name).read_bytes())
    subprocess.run(["git", "-C", str(snap), "add", "-f", stamp_name], check=True)
    subprocess.run(["git", "-C", str(snap), "-c", "user.name=PrismaBuild test",
                    "-c", "user.email=test@example.invalid", "commit", "-qm", "sealed snapshot"], check=True)
    commit = subprocess.run(["git", "-C", str(snap), "rev-parse", "HEAD"], check=True,
                            capture_output=True, text=True).stdout.strip()
    bundle = world_root / "snapshot.bundle"
    subprocess.run(["git", "-C", str(snap), "bundle", "create", str(bundle), "HEAD"], check=True)
    cas_root = world_root / "materialization-cas"
    entry, _ = pb.PrismaBuildCAS(cas_root).ingest_input(bundle, input_id="pbrun.checkout-snapshot")
    return {"cas_root": str(cas_root), "checkout_snapshot": {
        "schema": "prismaquant.prismabuild.pbrun_checkout_snapshot.v1",
        "commit": commit, "subdirectory": ".", "input": entry}}


snap = snapshot()
stub = world_root / "stub_worker.py"
stub.write_text("import sys\nsys.exit(0)\n")


def done():
    world.actions.publish(K_DONE, worker_script=str(stub), cas_root=snap["cas_root"], checkout_root=None,
                          checkout_snapshot=snap["checkout_snapshot"], max_attempts=1)
    outcome = q.serve_once()
    rec = record_store.read_document(raw.records.item_path(record_store.DONE, K_DONE))
    return {"outcome_status": (outcome or {}).get("status"), "done": rec is not None,
            "top_returncode": None if rec is None else ("returncode" in rec),
            "detail_returncode": None if rec is None else (rec.get("detail") or {}).get("returncode"),
            "top_container_cleanup": None if rec is None else ("container_cleanup" in rec),
            "detail_container_cleanup": None if rec is None else ("container_cleanup" in (rec.get("detail") or {})),
            "scope_cleanup_complete": None if rec is None else (rec.get("resource_scope_cleanup") or {}).get("complete"),
            "status": None if rec is None else rec.get("status"),
            "snapshot": None if rec is None else rec.get("checkout_snapshot") is not None,
            "trees_left": sorted(p.name for p in pool.LOCAL_CHECKOUT_ROOT.iterdir()) if pool.LOCAL_CHECKOUT_ROOT.exists() else []}


def tier():
    raw.ledger("storage").ensure_capacity({"mem_gb": 8})
    holdings = raw.tier("ram:storage")
    holdings.grow_supply({"ram_gib": 4})
    return holdings


def leak_done():
    assert raw.ledger().acquire(K_DONE, {"cpu": 1})       # the live census: done keys holding host tokens
    assert tier().take(K_DONE, {"ram_gib": 1})            # and tier pins (with a RAM mirror)
    return {"host": raw.ledger().holder_tokens(K_DONE)}


def live(key, **kw):
    def run():
        world.actions.publish(key, worker_script=str(stub), max_attempts=1, **kw)
        item = q.claim()
        assert item is not None and item["action_key"] == key, item
        lease = record_store.read_document(raw.records.lease_path(key)) or {}
        claim = record_store.read_document(raw.records.item_path(record_store.CLAIMED, key)) or {}
        assert tier().take(key, {"ram_gib": 1})
        return {"claim_fields": sorted(k for k in ("attempts", "claimed_by", "claimed_host", "claimed_unix",
                                                   "published_unix", "checkout_snapshot") if k in claim),
                "lease_fields": sorted(k for k in ("attempts", "claimed_by", "claimed_host", "claimed_unix",
                                                   "published_unix", "owner", "host", "heartbeat_unix") if k in lease),
                "host": raw.ledger().holder_tokens(key)}
    return run


step("done", done)
step("leak_done", leak_done)
step("live", live(K_LIVE))
step("live_snapshot", live(K_LSNAP, cas_root=snap["cas_root"], checkout_root=None,
                           checkout_snapshot=snap["checkout_snapshot"]))
holders = {}
for kind, base in (("host", raw.root / pool.RESERVATIONS), ("tier", raw.root / "tier-reservations")):
    if base.is_dir():
        for ledger in sorted(os.listdir(base)):
            held = base / ledger / "held"
            if held.is_dir():
                for holder in sorted(os.listdir(held)):
                    holders[f"{kind}:{ledger}:{holder[:20]}"] = {
                        "tokens": len(os.listdir(held / holder)),
                        "release_owner": (base / ledger / "release-owners" / (holder + ".json")).exists()}
report["holders"] = holders
print(json.dumps(report, sort_keys=True))
