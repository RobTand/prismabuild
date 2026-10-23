"""Round-3 review baseline (#1429): portable to main e9e9d90772 and the head.

An unfenced checkout-snapshot action whose checkout root refuses creation
(EACCES or ENOSPC at mkdtemp). The worker's own serve path fails it. Its
reservation must come back once the fault clears and recovery runs.
"""
import errno
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "tools" / "fleet"), str(ROOT / "tests")]
from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402
from prismabuild import core as pb, materialize, pool, record_store  # noqa: E402
from test_pool import _test_checkout_snapshot  # noqa: E402

WORKER = ROOT / "tools" / "prismabuild_worker.py"


def _unfenced_snapshot_queue(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    for args in (["init", "-q"], ["config", "user.name", "PrismaBuild test"],
                 ["config", "user.email", "test@example.invalid"]):
        subprocess.run(["git", "-C", str(source), *args], check=True)
    (source / "task.py").write_text("open('result', 'w').write('ok')\n")
    subprocess.run(["git", "-C", str(source), "add", "task.py"], check=True)
    subprocess.run(["git", "-C", str(source), "commit", "-qm", "sealed source"], check=True)
    identity = pb.git_checkout_identity(source)
    stamp_name = f"{pb.PBRUN_STAMP_PREFIX}test.json"
    (source / stamp_name).write_text(json.dumps({"cwd": ".", **identity}, indent=1, sort_keys=True))
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    snapshot = _test_checkout_snapshot(tmp_path, source, stamp_name, cas)
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                 "task_class": "generation", "determinism": "stochastic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"], "working_directory": ".",
                 "result_path": "result"},
        "inputs": [snapshot["input"]],
        "code_closure": pb.build_code_closure(source, [stamp_name]),
        "params": {"command": [sys.executable, "task.py"], "cwd": ".",
                   "demand": {"cpu": 1, "mem_gb": 1}, "checkout_snapshot": snapshot},
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas.publish_action_request(action)
    queue = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "queue"), capacity={"cpu": 8, "mem_gb": 16},
        default_demand={"cpu": 1, "mem_gb": 1})
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_snapshot=snapshot, worker_script=WORKER, max_attempts=1)
    return queue, action["action_key"]


@pytest.mark.parametrize("how", ["eacces", "enospc"])
def test_unfenced_checkout_creation_failure_returns_reservation(tmp_path, monkeypatch, how):
    queue, key = _unfenced_snapshot_queue(tmp_path)
    root = tmp_path / "materialized"
    root.mkdir()
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", root)
    real = materialize.os.mkdir
    if how == "eacces":
        if os.geteuid() == 0:
            pytest.skip("root ignores directory permission bits")
        root.chmod(0o555)
    else:
        def full(path, mode=0o777, *, dir_fd=None):
            if dir_fd is not None and Path(os.readlink(f"/proc/self/fd/{dir_fd}")) == root:
                raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))
            return real(path, mode, dir_fd=dir_fd)
        monkeypatch.setattr(materialize.os, "mkdir", full)
    with pytest.raises(OSError):
        queue.serve_once()
    claimed = queue.queue.records.item_path(pool.CLAIMED, key)
    print("serve-failure", how, "claim_retained", claimed.exists(),
          "held", queue.queue.ledger().held_keys())
    root.chmod(0o755)
    monkeypatch.setattr(materialize.os, "mkdir", real)
    offset = pool.LEASE_TIMEOUT_S + 400.0
    real_now = time.time
    monkeypatch.setattr(pool, "_now", lambda: real_now() + offset)
    monkeypatch.setattr(materialize, "_now", lambda: real_now() + offset, raising=False)
    for _ in range(3):
        queue.queue.reap_stale()
    durable = record_store.read_document(claimed)
    print("after-recovery", how, "claim_retained", durable is not None,
          "cleanup_error", ((durable or {}).get("checkout_cleanup") or {}).get("error"),
          "held", queue.queue.ledger().held_keys())
    assert queue.queue.ledger().held_keys() == []
    assert durable is None
