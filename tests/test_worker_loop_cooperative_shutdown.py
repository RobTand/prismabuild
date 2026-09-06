"""SIGTERM after an idle census must not strand a newly acquired claim."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
from prismabuild import pool


@pytest.mark.parametrize("fails", [False, True])
def test_sigterm_during_execution_files_the_claim_before_exiting(tmp_path, fails):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    keys = ["a" * 64, "b" * 64]
    for key in keys:
        queue.publish(action_key=key, cas_root=str(tmp_path / "cas"),
                      checkout_root=str(tmp_path), worker_script="worker.py", max_attempts=1)
    # Run in a child: the regression is real SIGTERM, which must not kill the
    # pytest worker when testing the original implementation.
    script = tmp_path / "exercise.py"
    script.write_text(f'''
import os, signal, sys
from pathlib import Path
sys.path.insert(0, {str(REPOSITORY / "src")!r})
sys.path.insert(0, {str(REPOSITORY / "tools" / "fleet")!r})
import worker_loop as wl
wl.SH = Path({str(tmp_path)!r})
wl.loaded_runtime_commit = lambda: "test"
wl.published_commit = lambda: "test"
def execute(self, item, **kwargs):
    assert kwargs.get("containment") is True
    os.kill(os.getpid(), signal.SIGTERM)
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
