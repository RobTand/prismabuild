"""Actual private recovery plan capture/application with supplied process/stat observations."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import runpy
import stat
import subprocess
import tarfile


def operate(request, store, load):
    recovery, core, pool, pbrun = (load(name) for name in
        ("prismabuild.checkout_recovery", "prismabuild.core", "prismabuild.pool", "pbrun"))
    helpers = runpy.run_path(str(Path(__file__).with_name("compat_snapshot_adapters.py")))
    git = helpers["git"]
    work = store / "source"
    work.mkdir()
    git(work, "init", "--quiet", "--template=")
    git(work, "config", "user.name", "Compatibility fixture")
    git(work, "config", "user.email", "fixture@example.invalid")
    (work / "payload").write_bytes(b"contents")
    git(work, "add", "payload")
    git(work, "commit", "--quiet", "-m", "fixture")
    cas = core.PrismaBuildCAS(store / "cas")
    snapshot = pbrun.build_git_checkout_snapshot(work, cas=cas)
    body = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["action_body"](core, store)
    body["inputs"] = [snapshot["input"]]
    body["params"]["checkout_snapshot"] = snapshot
    action = core.seal_action(body)
    cas.publish_action_request(action)
    key = action["action_key"]
    queue = pool.PoolQueue(store / "queue")
    path = queue.publish(action_key=key, cas_root=cas.root, checkout_snapshot=snapshot,
        worker_script="/compat/worker.py", resources={"cpu": 1, "mem_gb": 1}, max_attempts=1)
    claim = json.loads(path.read_bytes())
    claim.update(claimed_by="compat", claimed_host="compat-host", claimed_unix=100.)
    path.unlink()
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, key), claim)
    queue.finish(key, status="failed", detail={"termination_reason": "private wire fixture"}, claim_snapshot=claim)
    local, bank, proc = (store / name for name in ("checkouts", "bank", "proc"))
    for directory in (local, bank, proc):
        directory.mkdir(mode=0o700)
    directory = local / (key[:12] + ".compat00")
    checkout = directory / "checkout"
    directory.mkdir(mode=0o700)
    checkout.mkdir(mode=0o700)
    bundle = cas.input_path(snapshot["input"])
    subprocess.run(["git", "clone", "--bare", "--quiet", "--template=", str(bundle), str(checkout / ".git")],
                   check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    git(checkout, "symbolic-ref", "HEAD", "refs/heads/compat")
    git(checkout, "update-ref", "refs/heads/compat", snapshot["commit"])
    assert git(checkout, "rev-parse", "HEAD") == snapshot["commit"]
    (checkout / "payload").write_bytes(b"retained private dirty bytes")
    archive = bank / "selected.tar"
    with tarfile.open(archive, "w", dereference=False) as target:
        def supplied_metadata(member):
            member.mtime = 0
            member.uid = member.gid = 1000
            member.uname = member.gname = ""
            member.pax_headers = {}
            return member
        target.add(directory, arcname=directory.name, filter=supplied_metadata)
    archive.chmod(0o444)
    gate = store / "maintenance.json"
    gate.write_bytes(core._canonical_file_bytes({"schema": "prismabuild.resource-maintenance.v1", "draining": True,
        "changed_unix": 99., "owner": "compat", "reason": "private codec fixture"}))
    gate.chmod(0o644)
    entries = [{"action_key": key, "path": str(directory), "archive_path": str(archive),
                "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest()}]
    inodes = {}
    for path in [store, *sorted(store.rglob("*"))]:
        info = path.lstat()
        inodes.setdefault((info.st_dev, info.st_ino), int(hashlib.sha256(str(path.relative_to(store)).encode()).hexdigest()[:12], 16))
    original_identity, original_gate, original_euid = recovery._checkout_recovery_identity, recovery._gate_uid, os.geteuid
    def observed_identity(info):
        value = original_identity(info)
        value.update(dev=1, ino=inodes[info.st_dev, info.st_ino], mtime_ns=3, ctime_ns=4,
                     nlink=2 if stat.S_ISDIR(info.st_mode) else 1)
        if stat.S_ISDIR(info.st_mode):
            value["size"] = 0
        return value
    recovery._checkout_recovery_identity = observed_identity
    recovery._gate_uid = lambda: os.getuid()
    os.geteuid = lambda: 0
    try:
        if request["op"] == "write":
            value, _ = recovery._checkout_recovery_capture(queue, entries, local, bank, gate, "compat", proc)
            return {"raw": base64.b64encode(core._canonical_file_bytes(value)).decode(),
                    "fixture_facts": ["supplied deterministic stat and empty private process census; actual Git/CAS/queue/archive and private cleanup only; no operator RootGO"]}
        value = json.loads(base64.b64decode(request["raw"]))
        request["_consumer_boundary"]()
        result = recovery.apply_checkout_recovery(queue, value, expected_plan_sha256=core.canonical_sha256(value),
            local_root=local, bank_root=bank, maintenance_gate=gate, maintenance_owner="compat", proc_root=proc)
        assert result["complete"] and result["removed"] == [str(directory)], result
        assert not directory.exists()
        return result
    finally:
        recovery._checkout_recovery_identity, recovery._gate_uid, os.geteuid = original_identity, original_gate, original_euid
