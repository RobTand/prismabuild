"""The storage role must know both runtime identities before warming bytes."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from prewarm_fixture import Fleet


REPOSITORY = Path(__file__).resolve().parents[1]
KNOWN = {"commit": "a" * 40, "generation": "first"}
RECEIPTS = {
    "known": json.dumps(KNOWN),
    "republished": json.dumps({**KNOWN, "generation": "second"}),
    "missing": None,
    "malformed": "{",
    "partial": json.dumps({"commit": KNOWN["commit"]}),
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


@pytest.mark.parametrize(("loaded", "published", "verdict"), CASES)
def test_once_requires_known_matching_receipts(tmp_path, loaded, published, verdict):
    fleet = Fleet(tmp_path / "fleet")
    key = fleet.action("receipt-boundary", [fleet.file("row.bin", 4096)])
    ready = fleet.queue.root / "ready" / (key + ".json")
    original_ready = ready.read_bytes()
    for name, kind in (("loaded.json", loaded), ("published.json", published)):
        if RECEIPTS[kind] is not None:
            (tmp_path / name).write_text(RECEIPTS[kind])
    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": False}))
    disk = tmp_path / "sys-block" / "private0"
    disk.mkdir(parents=True)
    (disk / "stat").write_text("0 " * 11)
    (tmp_path / "nfsd-io").write_text("io 0 0\n")
    arcstats = fleet.arcstats(size=0, c=1 << 20, c_max=1 << 20)
    script = tmp_path / "exercise.py"
    script.write_text(f'''
import os, sys
from functools import partial
from pathlib import Path
repository = Path({str(REPOSITORY)!r})
root = Path({str(tmp_path)!r})
sys.path.insert(0, str(repository / "src"))
sys.path.insert(0, str(repository / "tools" / "fleet"))
import prewarm_loop as role
from prismabuild import generation_receipt, pool, role_session, storage_tiers
assert Path(role.__file__).resolve() == repository / "tools" / "fleet" / "prewarm_loop.py"
for name, module in (("pool", pool), ("role_session", role_session),
                     ("generation_receipt", generation_receipt), ("storage_tiers", storage_tiers)):
    assert Path(module.__file__).resolve() == repository / "src" / "prismabuild" / (name + ".py")
role_session.GENERATION_VERSION = root / "loaded.json"
role_session.RUNTIME_VERSION = root / "published.json"
role_session.MAINTENANCE_GATE = root / "maintenance.json"
role_session.PARKED_ROOT = root / "parked"
role_session.ROLE_LOCK_ROOT = root / "role-locks"
role.DiskPacer = partial(role.DiskPacer,
    stat_source=partial(storage_tiers.read_disk_stat, root=str(root / "sys-block")))
affinity = os.sched_getaffinity(0)
code = role.main([
    "--once", "--pool-root", {str(fleet.queue.root)!r}, "--cas-root", {str(fleet.cas_root)!r},
    "--mount-map", {str(fleet.mount) + "=" + str(fleet.mount)!r},
    "--arcstats", {arcstats!r}, "--arc-reserve-fraction", "1",
    "--min-manifest-bytes", "0", "--readers", "1", "--max-readers", "1",
    "--disks", "private0", "--pace-sample-s", "0", "--export-stats", "",
    "--nfsd-io", str(root / "nfsd-io"), "--log", str(root / "cycles.jsonl")])
assert os.sched_getaffinity(0) == affinity
raise SystemExit(code)
''')
    environment = {
        **os.environ, "PYTHONDONTWRITEBYTECODE": "1",
        "PRISMABUILD_POOL_ROOT": str(fleet.queue.root),
        "PRISMABUILD_BOX_STATE_ROOT": str(tmp_path / "box-state"),
        "PRISMABUILD_LOCAL_CHECKOUT_ROOT": str(tmp_path / "checkouts"),
        "PRISMABUILD_PRODUCED_SPOOL_ROOT": str(tmp_path / "spool"),
        "PRISMABUILD_SPOOL_OFFER_ROOT": str(tmp_path / "spool-offer"),
        "PRISMABUILD_MAINTENANCE_GATE": str(gate),
        "PRISMABUILD_FILESYSTEM_FLOOR": "off",
        "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
    }
    child = subprocess.run([sys.executable, "-B", str(script)], env=environment,
                           capture_output=True, text=True, timeout=10)
    log = tmp_path / "cycles.jsonl"
    record = fleet.queue.prewarm(key)
    observed = {
        "exit": child.returncode,
        "cycles": len(log.read_text().splitlines()) if log.exists() else 0,
        "warm_status": record.get("status") if record else None,
        "bytes_warmed": record.get("bytes_warmed", 0) if record else 0,
        "runtime_moved": "runtime moved" in child.stderr,
        "ready_unchanged": ready.read_bytes() == original_ready,
        "claims": list((fleet.queue.root / "claimed").glob("*.json")),
        "terminals": [path for state in ("done", "failed")
                      for path in (fleet.queue.root / state).glob("*.json")],
        "drifts": list((fleet.queue.root / "generation-drift").glob("*.json")),
        "parked": list((tmp_path / "parked").glob("*")),
    }
    expected = {
        "exit": 0 if verdict == "match" else 75,
        "cycles": int(verdict == "match"),
        "warm_status": "complete" if verdict == "match" else None,
        "bytes_warmed": 4096 if verdict == "match" else 0,
        "runtime_moved": verdict == "drift",
        "ready_unchanged": True, "claims": [], "terminals": [], "drifts": [], "parked": [],
    }
    assert observed == expected, child.stdout + child.stderr
    if record:
        assert record["disk_pacing"]["active"] is True
        assert record["disk_pacing"]["devices"] == ["private0"]
        assert record["disk_pacing"]["telemetry_state"] == "complete"
