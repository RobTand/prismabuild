"""A tier role needs complete matching receipts before layout, adoption or mint."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from prismabuild import pool
from test_prewarm_requires_known_receipts import CASES, RECEIPTS, REPOSITORY


@pytest.mark.parametrize(("loaded", "published", "verdict"), CASES)
def test_once_requires_known_matching_receipts(tmp_path, loaded, published, verdict):
    for name, kind in (("loaded.json", loaded), ("published.json", published)):
        if RECEIPTS[kind] is not None:
            (tmp_path / name).write_text(RECEIPTS[kind])
    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": False}))
    queue = pool.PoolQueue(tmp_path / "queue")
    receipt = queue.root / pool.PREWARM / "boundary-input.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({"receipt_probe": "actual tier-cycle input bytes"}))
    (tmp_path / "arcstats").write_text(f"size 4 0\nc 4 {1 << 30}\nc_max 4 {1 << 30}\n")
    (tmp_path / "ram-policy.json").write_text("null\n")
    (tmp_path / "pressure").write_text("some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")
    (tmp_path / "zpool.txt").write_text("")
    script = tmp_path / "exercise.py"
    script.write_text(f'''
import json, os, sys
from pathlib import Path
repository = Path({str(REPOSITORY)!r})
root = Path({str(tmp_path)!r})
sys.path.insert(0, str(repository / "src"))
sys.path.insert(0, str(repository / "tools" / "fleet"))
import tier_loop as role
from prismabuild import generation_receipt, pool, role_session, storage_tiers
assert Path(role.__file__).resolve() == repository / "tools" / "fleet" / "tier_loop.py"
for name, module in (("pool", pool), ("role_session", role_session),
                     ("generation_receipt", generation_receipt), ("storage_tiers", storage_tiers)):
    assert Path(module.__file__).resolve() == repository / "src" / "prismabuild" / (name + ".py")
role_session.GENERATION_VERSION = root / "loaded.json"
role_session.RUNTIME_VERSION = root / "published.json"
role_session.MAINTENANCE_GATE = root / "maintenance.json"
role_session.PARKED_ROOT = root / "parked"
role_session.ROLE_LOCK_ROOT = root / "role-locks"
role.RECEIPT_READERS = 1
read_arc = storage_tiers.read_arcstats
read_policy = storage_tiers.read_ram_policy
read_pressure = storage_tiers.read_io_pressure_some
storage_tiers.read_arcstats = lambda _path: read_arc(str(root / "arcstats"))
storage_tiers.read_ram_policy = lambda _path: read_policy(root / "ram-policy.json")
storage_tiers.read_io_pressure_some = lambda _path: read_pressure(str(root / "pressure"))
def kernel_command(argv):
    assert Path(argv[0]).name == "zpool", argv
    return (root / "zpool.txt").read_text()
storage_tiers._run = kernel_command
affinity = os.sched_getaffinity(0)
def observed_read(frame, event, value):
    if (event == "return" and frame.f_code == Path.read_bytes.__code__
            and frame.f_locals.get("self") == root / "queue" / "prewarm" / "boundary-input.json"):
        assert isinstance(value, bytes)
        assert json.loads(value) == {{"receipt_probe": "actual tier-cycle input bytes"}}
        (root / "receipt-read").write_bytes(value)
sys.setprofile(observed_read)
try:
    code = role.main(["--once", "--pool-root", str(root / "queue"), "--interval-s", "1"])
finally:
    sys.setprofile(None)
assert os.sched_getaffinity(0) == affinity
raise SystemExit(code)
''')
    environment = {
        **os.environ, "PYTHONDONTWRITEBYTECODE": "1",
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
                           capture_output=True, text=True, timeout=15)
    lines = []
    for line in child.stdout.splitlines():
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict):
            lines.append(value)
    cycles = [line for line in lines if line.get("event") == "tier-cycle"]
    tiers = queue.tiers()
    observed = {
        "exit": child.returncode,
        "cycles": len(cycles),
        "layout": (queue.root / pool.READY).exists(),
        "adoptions": sum(line.get("event") == "tier-records-adopted" for line in lines),
        "read_actual_receipt": (tmp_path / "receipt-read").exists(),
        "tiers": len(tiers),
        "runtime_moved": any(line.get("event") == "tier-runtime-moved" for line in lines),
        "drifts": list((queue.root / "generation-drift").glob("*.json")),
        "parked": list((tmp_path / "parked").glob("*")),
    }
    expected = {
        "exit": 0 if verdict == "match" else 75,
        "cycles": int(verdict == "match"),
        "layout": verdict == "match", "adoptions": int(verdict == "match"),
        "read_actual_receipt": verdict == "match", "tiers": int(verdict == "match"),
        "runtime_moved": verdict == "drift", "drifts": [], "parked": [],
    }
    assert observed == expected, child.stdout + child.stderr
    if cycles:
        assert cycles[0]["completed"] is True
        assert cycles[0]["reads"]["records_parsed"] >= 1
        assert (tmp_path / "receipt-read").read_bytes() == receipt.read_bytes()
        assert tiers[0]["tier"] == "arc"
        assert tiers[0]["tokens"] == {"arc_gib": 1}
        assert queue.tier_ledger(tiers[0]["tier_id"]).available() == {"arc_gib": 1}
