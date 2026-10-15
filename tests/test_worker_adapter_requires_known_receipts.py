"""The public adapter distinguishes UNKNOWN from its two existing drift paths."""
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))
from prismabuild import pool
from worker_adapter_fixture import prepare


KNOWN = {"commit": "a" * 40, "generation": "first"}
RECEIPTS = {
    "known": json.dumps(KNOWN),
    "generation-moved": json.dumps({**KNOWN, "generation": "second"}),
    "commit-moved": json.dumps({**KNOWN, "commit": "b" * 40}),
    "missing": None, "malformed": "{",
    "commit-only": json.dumps({"commit": KNOWN["commit"]}),
    "generation-only": json.dumps({"generation": "second"}),
}
CASES = [
    pytest.param("known", "known", "match", id="real-contained-child-match"),
    pytest.param("known", "generation-moved", "generation-drift", id="adapter-generation-drift"),
    pytest.param("known", "commit-moved", "commit-drift", id="loop-commit-drift"),
    pytest.param("missing", "known", "unknown", id="loaded-missing-control"),
    pytest.param("malformed", "known", "unknown", id="loaded-malformed-control"),
    pytest.param("commit-only", "known", "unknown", id="loaded-commit-only-control"),
    pytest.param("known", "missing", "unknown", id="published-missing-control"),
    pytest.param("known", "malformed", "unknown", id="published-malformed-control"),
    pytest.param("known", "commit-only", "unknown", id="published-commit-only-control"),
    pytest.param("generation-only", "known", "unknown", id="loaded-generation-only-false-drift"),
    pytest.param("known", "generation-only", "unknown", id="published-generation-only-false-drift"),
]


@pytest.mark.parametrize(("loaded", "published", "verdict"), CASES)
def test_public_adapter_requires_a_known_pair(tmp_path, loaded, published, verdict):
    queue, cas, action = prepare(tmp_path, REPOSITORY)
    key = action["action_key"]
    ready = queue.item_path(pool.READY, key)
    original_ready = ready.read_bytes()
    for name, kind in (("loaded.json", loaded), ("published.json", published)):
        if RECEIPTS[kind] is not None:
            (tmp_path / name).write_text(RECEIPTS[kind])
    gate = tmp_path / "maintenance.json"
    script = tmp_path / "exercise.py"
    script.write_text(f'''
import sys
from pathlib import Path
repository = Path({str(REPOSITORY)!r})
sys.path.insert(0, str(repository / "src"))
sys.path.insert(0, str(repository / "tools" / "fleet"))
sys.path.insert(0, str(repository / "tests"))
from worker_adapter_fixture import run_adapter
raise SystemExit(run_adapter(Path({str(tmp_path)!r}), repository))
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
                           capture_output=True, text=True, timeout=30)
    diagnostics = child.stdout + child.stderr
    process = json.loads((tmp_path / "process.json").read_text())
    assert process["affinity"] == sorted(os.sched_getaffinity(0))
    assert process["sources"] == {
        "worker": str(REPOSITORY / "tools" / "fleet" / "worker.py"),
        "worker_loop": str(REPOSITORY / "tools" / "fleet" / "worker_loop.py"),
        **{name: str(REPOSITORY / "src" / "prismabuild" / (name + ".py"))
           for name in ("role_session", "generation_receipt", "pool", "core")},
    }
    assert child.stdout, diagnostics
    output = json.loads(child.stdout)
    drifts = list((queue.root / "generation-drift").glob("*.json"))
    receipt = cas.lookup(action)
    observed = {
        "exit": child.returncode, "payload_ran": (tmp_path / "payload.json").exists(),
        "outcome": output["outcome"]["status"] if output["outcome"] else None,
        "declined_as_drift": "runtime moved" in output.get("declined", ""),
        "drift_incidents": len(drifts), "cas_receipt": receipt is not None,
        "done": queue.item_path(pool.DONE, key).exists(),
        "failed": queue.item_path(pool.FAILED, key).exists(),
        "ready_unchanged": ready.exists() and ready.read_bytes() == original_ready,
        "claims": list(queue.dir(pool.CLAIMED).glob("*.json")),
        "held": queue.ledger().held(),
    }
    expected = {
        "exit": 75 if verdict == "unknown" else 0, "payload_ran": verdict == "match",
        "outcome": "executed" if verdict == "match" else None,
        "declined_as_drift": verdict == "generation-drift",
        "drift_incidents": int(verdict == "commit-drift"), "cas_receipt": verdict == "match",
        "done": verdict == "match", "failed": False, "ready_unchanged": verdict != "match",
        "claims": [], "held": {},
    }
    assert observed == expected, diagnostics
    if verdict == "unknown":
        assert "runtime moved" not in diagnostics
    if verdict == "commit-drift":
        drift = json.loads(drifts[0].read_text())
        assert drift["actor"] == "worker_loop"
        assert drift["pid"] == process["pid"]
        assert (drift["loaded_commit"], drift["published_commit"]) == ("a" * 40, "b" * 40)
        assert drift["loaded_generation"] == drift["published_generation"] == "first"
        assert stat.S_IMODE(drifts[0].stat().st_mode) == 0o444
    if verdict == "match":
        assert json.loads((tmp_path / "invocation.json").read_text()) == ["--once", "--gpu", "--timeout-s", "120"]
        assert cas._verified_receipt_result_path(receipt).read_bytes() == b"real child\n"
        payload = json.loads((tmp_path / "payload.json").read_text())
        assert payload["action_key"] == key
        assert payload["affinity"]
        assert set(payload["affinity"]) <= set(process["affinity"])
        assert len(payload["affinity"]) == action["params"]["demand"]["cpu"]
        assert not Path(f"/proc/{payload['pid']}").exists()
        records = [json.loads(path.read_text()) for path in (tmp_path / "broker-state").glob("*.json")]
        assert len(records) == 1
        control = records[0]
        assert control["action_key"] == key
        assert (control["nonce"], control["scope_id"]) == (payload["nonce"], payload["scope"])
        assert control["memory_max_bytes"] == 1 << 30
        assert control["launched_unix"] <= control["stopped_unix"] <= control["released_unix"]
        calls = json.loads((tmp_path / "broker-calls.json").read_text())
        runs = [call for call in calls if call["op"] == "run"]
        assert len(runs) == 1
        assert runs[0]["scope"] == control["scope_id"]
        assert payload["affinity"] == runs[0]["affinity"]
        operations = [call["op"] for call in calls]
        assert operations.index("create") < operations.index("run") < operations.index("stop") < operations.index("release")
