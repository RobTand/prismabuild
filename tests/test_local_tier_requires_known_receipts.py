"""A local-tier role needs complete matching receipts before a queue pass."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from prismabuild import resident_sets


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
    for name, kind in (("loaded.json", loaded), ("published.json", published)):
        if RECEIPTS[kind] is not None:
            (tmp_path / name).write_text(RECEIPTS[kind])
    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": False}))
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"schema": resident_sets.POLICY_SCHEMA, "hosts": {}}))
    script = tmp_path / "exercise.py"
    script.write_text(f'''
import json, os, sys
from pathlib import Path
repository = Path({str(REPOSITORY)!r})
root = Path({str(tmp_path)!r})
sys.path.insert(0, str(repository / "src"))
sys.path.insert(0, str(repository / "tools" / "fleet"))
import local_tier_loop as role
from prismabuild import generation_receipt, pool, role_session
assert Path(role.__file__).resolve() == repository / "tools" / "fleet" / "local_tier_loop.py"
for name, module in (("pool", pool), ("role_session", role_session),
                     ("generation_receipt", generation_receipt)):
    assert Path(module.__file__).resolve() == repository / "src" / "prismabuild" / (name + ".py")
role_session.GENERATION_VERSION = root / "loaded.json"
role_session.RUNTIME_VERSION = root / "published.json"
role_session.MAINTENANCE_GATE = root / "maintenance.json"
role_session.PARKED_ROOT = root / "parked"
role_session.ROLE_LOCK_ROOT = root / "role-locks"
affinity = os.sched_getaffinity(0)
real_cycle = role.local_tier_cycle
def cycle(queue, host, policy):
    assert queue.root == root / "queue"
    assert os.sched_getaffinity(0) == affinity
    result = real_cycle(queue, host, policy)
    with (root / "passes.jsonl").open("a") as stream:
        stream.write(json.dumps(result) + "\\n")
    return result
role.local_tier_cycle = cycle
code = role.main(["--once", "--pool-root", str(root / "queue"),
                  "--policy", str(root / "policy.json")])
assert os.sched_getaffinity(0) == affinity
raise SystemExit(code)
''')
    environment = {
        **os.environ, "PYTHONDONTWRITEBYTECODE": "1",
        "PRISMABUILD_POOL_ROOT": str(tmp_path / "queue"),
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
    passes = tmp_path / "passes.jsonl"
    observed = {
        "exit": child.returncode,
        "passes": len(passes.read_text().splitlines()) if passes.exists() else 0,
        "queue_created": (tmp_path / "queue").exists(),
        "drifts": list((tmp_path / "queue" / "generation-drift").glob("*.json")),
        "parked": list((tmp_path / "parked").glob("*")),
    }
    expected = {
        "exit": 0 if verdict == "match" else 75,
        "passes": int(verdict == "match"),
        "queue_created": verdict == "match",
        "drifts": [],
        "parked": [],
    }
    assert observed == expected, child.stdout + child.stderr
