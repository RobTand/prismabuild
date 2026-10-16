"""A worker needs complete matching receipts before taking a ready unit."""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
from prismabuild import pool


COMMIT = "a" * 40
RECEIPTS = {
    "known": json.dumps({"commit": COMMIT, "generation": "first"}),
    "republished": json.dumps({"commit": COMMIT, "generation": "second"}),
    "missing": None,
    "malformed": "{",
    "partial": json.dumps({"commit": COMMIT}),
}
CASES = [
    pytest.param("known", "known", "match", id="complete-pair-match"),
    pytest.param("known", "republished", "drift", id="same-commit-generation-drift"),
    pytest.param("missing", "known", "unknown", id="loaded-missing"),
    pytest.param("malformed", "known", "unknown", id="loaded-malformed"),
    pytest.param("partial", "known", "unknown", id="loaded-partial"),
    pytest.param("known", "missing", "unknown", id="published-missing"),
    pytest.param("known", "malformed", "unknown", id="published-malformed"),
    pytest.param("known", "partial", "unknown", id="published-partial"),
]


def _json_lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


@pytest.mark.parametrize(("loaded", "published", "verdict"), CASES)
def test_once_requires_known_matching_receipts(tmp_path, loaded, published, verdict):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    keys = ["a" * 64, "b" * 64]
    for key in keys:
        queue.publish(action_key=key, cas_root=str(tmp_path / "cas"),
                      checkout_root=str(tmp_path), worker_script="worker.py",
                      resources={"cpu": 1, "mem_gb": 1}, max_attempts=1)
    ready_before = {key: queue.records.item_path(pool.record_store.READY, key).read_bytes() for key in keys}
    for name, kind in (("loaded.json", loaded), ("published.json", published)):
        if RECEIPTS[kind] is not None:
            (tmp_path / name).write_text(RECEIPTS[kind])
    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": False}))
    script = tmp_path / "exercise.py"
    script.write_text(f'''
from functools import partial
import json, os, socket, sys
from pathlib import Path
repository = Path({str(REPOSITORY)!r})
root = Path({str(tmp_path)!r})
sys.path.insert(0, str(repository / "src"))
sys.path.insert(0, str(repository / "tools" / "fleet"))
import worker_loop as wl
from prismabuild import generation_receipt, pool, role_session
wl.SH = root
wl.PUBLICATION_LOCK_ROOT = root / "publication-lock"
wl.container_images.InventoryCache = partial(
    wl.container_images.InventoryCache, root=root / "image-inventory")
role_session.GENERATION_VERSION = root / "loaded.json"
role_session.RUNTIME_VERSION = root / "published.json"
role_session.MAINTENANCE_GATE = root / "maintenance.json"
role_session.PARKED_ROOT = root / "parked"
role_session.ROLE_LOCK_ROOT = root / "role-locks"
affinity = sorted(os.sched_getaffinity(0))
(root / "process.json").write_text(json.dumps({{
    "pid": os.getpid(), "host": socket.gethostname(), "affinity": affinity,
    "runtime_root": str(wl.RUNTIME_ROOT),
    "sources": {{name: str(Path(module.__file__).resolve()) for name, module in (
        ("worker_loop", wl), ("pool", pool), ("role_session", role_session),
        ("generation_receipt", generation_receipt))}},
}}))
def record(name, value):
    with (root / name).open("a") as stream:
        stream.write(json.dumps(value) + "\\n")
def execute(self, item, **kwargs):
    key = item["action_key"]
    claimed = json.loads(self.records.item_path(pool.record_store.CLAIMED, key).read_text())
    assert self.root == root / "pb-queue"
    assert claimed["action_key"] == key
    assert self.records.lease_path(key).is_file()
    assert kwargs.get("containment") is True
    record("executions.jsonl", key)
    return {{"action_key": key, "status": "executed", "returncode": 0}}
pool.PoolQueue.execute = execute
code = wl.main(["--once", "--assume-idle", "--all-cores", "--cpu-slots", "1",
                "--mem-gb", "1", "--gpu-slots", "0", "--class", "x86",
                "--poll-s", "0", "--max-idle", "1"],
               on_outcome=lambda outcome: record("outcomes.jsonl", outcome))
assert sorted(os.sched_getaffinity(0)) == affinity
raise SystemExit(code)
''')
    environment = {
        **os.environ,
        "PYTHONDONTWRITEBYTECODE": "1",
        "PRISMABUILD_POOL_ROOT": str(queue.root),
        "PRISMABUILD_BOX_STATE_ROOT": str(tmp_path / "box-state"),
        "PRISMABUILD_LOCAL_CHECKOUT_ROOT": str(tmp_path / "checkouts"),
        "PRISMABUILD_PRODUCED_SPOOL_ROOT": str(tmp_path / "spool"),
        "PRISMABUILD_SPOOL_OFFER_ROOT": str(tmp_path / "spool-offer"),
        "PRISMABUILD_MAINTENANCE_GATE": str(gate),
        "PRISMABUILD_FILESYSTEM_FLOOR": "off",
        "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
    }
    child = subprocess.run([sys.executable, "-B", str(script)], env=environment,
                           capture_output=True, text=True, timeout=30)
    diagnostics = child.stdout + child.stderr
    process_path = tmp_path / "process.json"
    assert process_path.is_file(), diagnostics
    process = json.loads(process_path.read_text())
    assert process["runtime_root"] == str(REPOSITORY), diagnostics
    assert process["affinity"] == sorted(os.sched_getaffinity(0)), diagnostics
    assert process["sources"] == {
        "worker_loop": str(REPOSITORY / "tools" / "fleet" / "worker_loop.py"),
        **{name: str(REPOSITORY / "src" / "prismabuild" / f"{name}.py")
           for name in ("pool", "role_session", "generation_receipt")},
    }, diagnostics
    drifts = []
    for path in sorted((queue.root / "generation-drift").glob("*.json")):
        record = json.loads(path.read_text())
        drifts.append({field: record.get(field) for field in (
            "schema", "actor", "pid", "host", "loaded_commit", "loaded_generation",
            "published_commit", "published_generation")})
        drifts[-1]["mode"] = stat.S_IMODE(path.stat().st_mode)
    done = [json.loads(path.read_text()) for path in queue.records.dir(pool.record_store.DONE).glob("*.json")]
    observed = {
        "exit": child.returncode,
        "executions": _json_lines(tmp_path / "executions.jsonl"),
        "outcomes": _json_lines(tmp_path / "outcomes.jsonl"),
        "done": [(record["action_key"], record["status"]) for record in done],
        "failed": sorted(path.stem for path in queue.records.dir(pool.record_store.FAILED).glob("*.json")),
        "terminal_keys": sorted(queue.terminal_keys()),
        "claimed": sorted(path.name for path in queue.records.dir(pool.record_store.CLAIMED).iterdir()),
        "ready": {path.stem: path.read_bytes() for path in queue.records.dir(pool.record_store.READY).glob("*.json")},
        "drifts": drifts,
    }
    expected = {
        "exit": 75 if verdict == "unknown" else 0,
        "executions": [keys[0]] if verdict == "match" else [],
        "outcomes": [{"action_key": keys[0], "status": "executed", "returncode": 0}]
                    if verdict == "match" else [],
        "done": [(keys[0], "executed")] if verdict == "match" else [],
        "failed": [],
        "terminal_keys": [keys[0]] if verdict == "match" else [],
        "claimed": [],
        "ready": {key: ready_before[key] for key in keys[int(verdict == "match"):]},
        "drifts": [{
            "schema": "prismaquant.prismabuild.generation_drift.v1",
            "actor": "worker_loop", "pid": process["pid"], "host": process["host"],
            "loaded_commit": COMMIT, "loaded_generation": "first",
            "published_commit": COMMIT, "published_generation": "second", "mode": 0o444,
        }] if verdict == "drift" else [],
    }
    assert observed == expected, diagnostics
