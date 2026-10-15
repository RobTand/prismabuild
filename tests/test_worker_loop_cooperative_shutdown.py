"""SIGTERM after an idle census must not strand a newly acquired claim."""
from __future__ import annotations

import json
from pathlib import Path
import stat
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
from prismabuild import pool




@pytest.mark.parametrize("fails", [False, True])
@pytest.mark.parametrize("stop_kind", ["signal", "generation"])
def test_stop_or_generation_change_finishes_current_claim_only(tmp_path, fails, stop_kind):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    keys = ["a" * 64, "b" * 64]
    for key in keys:
        queue.publish(action_key=key, cas_root=str(tmp_path / "cas"),
                      checkout_root=str(tmp_path), worker_script="worker.py", max_attempts=1,
                      resources={"cpu": 1, "mem_gb": 1})
    for name in ("loaded.json", "published.json"):
        (tmp_path / name).write_text(json.dumps({"commit": "test", "generation": "first"}))
    # Run in a child: the regression is real SIGTERM, which must not kill the
    # pytest worker when testing the original implementation.
    script = tmp_path / "exercise.py"
    script.write_text(f'''
import json, os, signal, sys
from pathlib import Path
sys.path.insert(0, {str(REPOSITORY / "src")!r})
sys.path.insert(0, {str(REPOSITORY / "tools" / "fleet")!r})
import worker_loop as wl
from prismabuild import role_session
wl.SH = Path({str(tmp_path)!r})
wl.PUBLICATION_LOCK_ROOT = Path({str(tmp_path / "publication-lock")!r})
role_session.GENERATION_VERSION = Path({str(tmp_path / "loaded.json")!r})
role_session.RUNTIME_VERSION = Path({str(tmp_path / "published.json")!r})
def execute(self, item, **kwargs):
    assert kwargs.get("containment") is True
    if {stop_kind!r} == "signal":
        os.kill(os.getpid(), signal.SIGTERM)
    else:
        role_session.RUNTIME_VERSION.write_text(json.dumps({{"commit": "successor", "generation": "second"}}))
    if {fails!r}:
        raise RuntimeError("action failed after shutdown request")
    return {{"action_key": item["action_key"], "status": "executed", "returncode": 0}}
wl.pool.PoolQueue.execute = execute
sys.argv = ["worker_loop.py", "--assume-idle", "--all-cores", "--cpu-slots", "1",
            "--gpu-slots", "0", "--poll-s", "0", "--max-idle", "1"]
previous = signal.getsignal(signal.SIGTERM)
code = wl.main()
assert signal.getsignal(signal.SIGTERM) == previous
raise SystemExit(code)
''')
    result = subprocess.run([sys.executable, str(script)], capture_output=True,
                            text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr
    state = pool.FAILED if fails else pool.DONE
    assert json.loads(queue.item_path(state, keys[0]).read_text())["status"] == (
        "failed" if fails else "executed")
    assert queue.item_path(pool.READY, keys[1]).exists()
    assert not list(queue.dir(pool.CLAIMED).glob("*.json"))
    assert not list(queue.dir(pool.CLAIMED).glob("*.lease"))
    records = list((queue.root / "generation-drift").glob("*.json"))
    assert len(records) == int(stop_kind == "generation")
    if records:
        record = json.loads(records[0].read_text())
        assert record["actor"] == "worker_loop"
        assert record["loaded_commit"] == "test"
        assert record["loaded_generation"] == "first"
        assert record["published_commit"] == "successor"
        assert record["published_generation"] == "second"
        assert stat.S_IMODE(records[0].stat().st_mode) == 0o444
