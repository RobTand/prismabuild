"""Terminal summary repair uses persisted exact-attempt proof, not inventory."""
import copy
import socket

import pytest

from prismabuild import lifecycle, pool, resource_scope
from prismabuild.produced_output import _broker_scope_id


@pytest.mark.parametrize("invalid", [None, "foreign", "nonce", "incomplete", "control", "docker", "scratch"])
def test_terminal_cleanup_recovery_requires_exact_scope_proof(tmp_path, monkeypatch, invalid):
    queue = pool.PoolQueue(tmp_path / "queue")
    key, nonce = "a" * 64, "b" * 32
    unit = _broker_scope_id(key, nonce)
    record = {
        "action_key": key, "claimed_host": socket.gethostname(),
        "status": "lease_lost_max_attempts", "attempts": 1,
        "published_unix": 1, "finished_unix": 2, "finished_host": "finisher",
        "attempt_history": [{"attempt": 1, "outcome": "saved"}],
        "detail": {"reason": "lease expired"},
        "resource_scope": {
            "action_key": key, "nonce": nonce, "scope_id": unit,
            "cgroup_path": "/sys/fs/cgroup/prismabuild.slice/" + unit,
            "socket_path": str(resource_scope.BROKER_SOCKET), "token": "c" * 64,
            "boot_id": "previous-boot",
        },
        "resource_scope_cleanup": {"complete": True, "nonce": nonce},
        "container_cleanup_pending": {
            "complete": False, "remaining": [],
            "error": "resource scope cleanup incomplete: broker unavailable",
        },
    }
    if invalid == "foreign":
        monkeypatch.setattr(pool.socket, "gethostname", lambda: "foreign-host")
    elif invalid == "nonce":
        record["resource_scope_cleanup"]["nonce"] = "d" * 32
    elif invalid == "incomplete":
        record["resource_scope_cleanup"]["complete"] = False
    elif invalid == "control":
        record["resource_scope"]["scope_id"] = "wrong-scope"
    elif invalid == "docker":
        record["container_cleanup_pending"]["error"] = "Docker unavailable"
    elif invalid == "scratch":
        record["container_cleanup_pending"]["error"] = "scratch lifetime cleanup incomplete: unavailable"
    path = queue.records.item_path(pool.record_store.FAILED, key)
    queue.records.replace_record_at(path, record)
    # An unrelated successor's ownership must not even be consulted by repair.
    claim = queue.records.item_path(pool.record_store.CLAIMED, key)
    lease = queue.records.lease_path(key)
    queue.records.replace_record_at(claim, {"successor": True})
    queue.records.replace_record_at(lease, {"successor": True})
    before = (claim.read_bytes(), lease.read_bytes())
    expected = copy.deepcopy(record)
    if invalid is None:
        expected.pop("container_cleanup_pending")
    for _ in range(2):
        with queue.action_transition_lock(key):
            lifecycle.conclude(queue, lifecycle.TerminalCleanupRecovery(key))
        assert pool.record_store.read_document(path) == expected
        assert (claim.read_bytes(), lease.read_bytes()) == before
