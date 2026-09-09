"""Two admitted actors qualify queue recovery across a shared filesystem.

Run `original` and `peer` through pbcampaign on different host classes, with
the same fresh --root beneath /mnt/shared/pb-qualification. This exercises
production queue methods on isolated records, not a production worker fault.
The inner claims never launch payloads or create broker scopes. The enclosing
PB actions provide resource admission and containment for both actors.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import threading
import time
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild import pool
import pbrun


def put(path, value):
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    os.replace(temporary, path)


def wait(path, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return json.loads(path.read_text())
        except FileNotFoundError:
            time.sleep(0.1)
    raise TimeoutError(f"peer did not publish {path.name} within {timeout}s")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def late_finish(q, key, first, status):
    try:
        path = q.finish(key, status=status, detail={
            "returncode": 0 if status == "executed" else 7,
            "stdout": "late original result"}, claim_snapshot=first)
    except pool.AmbiguousClaimHolder as exc:
        # A stale client view can disagree with the peer's committed ledger.
        # Refusal is safe only if the peer proves its state was preserved below.
        return {"disposition": "ownership_refused", "reason": str(exc)}
    assert path == q.attempt_path(first, 1)
    return {"disposition": "archived", "path": str(path)}


def refused_late_finish(q, key, first, status):
    """Model a stale claim read only in this caller, leaving its waiter live."""
    read = pool._read_json
    caller = threading.get_ident()
    claim_path = q.item_path(pool.CLAIMED, key)
    injected_reads = 0

    def stale_read(path):
        nonlocal injected_reads
        if threading.get_ident() == caller and path == claim_path:
            injected_reads += 1
            return dict(first)
        return read(path)

    # The successor ledger stays real. Production must detect that the stale
    # original claim and that ledger contradict each other and refuse to finish.
    # This does not change shared bytes or simulate a kernel/filesystem stall.
    with patch.object(pool, "_read_json", stale_read):
        result = late_finish(q, key, first, status)
    assert injected_reads > 0, "stale claim read was not exercised"
    assert result["disposition"] == "ownership_refused", result
    return {**result, "injected_claim_reads": injected_reads}


def original(root, late_status, stale_claim_read=False):
    root.mkdir(parents=True, exist_ok=False)
    q = pool.PoolQueue(root / "queue")
    key = hashlib.sha256(str(root).encode()).hexdigest()
    q.publish(action_key=key, cas_root=root / "cas", checkout_root=root / "checkout",
              worker_script=root / "unused.py", resources={"cpu": 1},
              max_attempts=2, retry_safe=True)
    first = q.claim(owner=f"original:{os.getpid()}", capacity={"cpu": 1})
    assert first is not None
    outcome = []
    errors = []

    def follow():
        try:
            outcome.append(pbrun.await_outcome(q, key, wait_s=120,
                                              generation=first["published_unix"]))
        except BaseException as exc:
            errors.append(repr(exc))

    waiter = threading.Thread(target=follow, daemon=True)
    waiter.start()
    put(root / "first.json", first)
    # Deliberately cease heartbeats. No inner process or broker scope exists.
    ready = wait(root / "ready.json")
    assert ready["host"] != socket.gethostname()
    # The peer verifies its exact pre/post bytes. A marker in another directory
    # cannot certify that this client's negative dentries have expired.
    ready_result = late_finish(q, key, first, late_status)
    assert waiter.is_alive() and not outcome and not errors
    put(root / "late-ready.json", {"waiter_pending": True, **ready_result})

    successor = wait(root / "successor.json")
    try:
        q.write_lease(key, owner=first["claimed_by"], claim_snapshot=first)
    except pool.PoolContractError:
        pass
    else:
        raise AssertionError("late heartbeat accepted")
    finish = refused_late_finish if stale_claim_read else late_finish
    claimed_result = finish(q, key, first, late_status)
    assert waiter.is_alive() and not outcome and not errors
    put(root / "late-claimed.json", {"heartbeat_refused": True,
                                    "waiter_pending": True, **claimed_result})
    terminal = wait(root / "terminal.json")
    waiter.join(timeout=10)
    assert not waiter.is_alive() and outcome == [0] and not errors, (outcome, errors)
    assert digest(q.item_path(pool.DONE, key)) == terminal["terminal_sha256"]
    assert q.ledger(first["claimed_host"]).held() == {}
    assert q.ledger(successor["host"]).held() == {}
    return {**terminal, "key": key, "host": socket.gethostname(), "peer": successor["host"],
            "late_status": late_status, "waiter_result": outcome[0],
            "first_attempt_sha256": ready["attempt_sha256"],
            "late_ready": ready_result, "late_claimed": claimed_result}


def peer(root, late_status, stale_claim_read=False):
    first = wait(root / "first.json")
    assert first["claimed_host"] != socket.gethostname(), "requires distinct hosts"
    q = pool.PoolQueue(root / "queue")
    key = first["action_key"]
    claim = q.item_path(pool.CLAIMED, key)
    before = claim.read_bytes()
    lease = q.lease_path(key).read_bytes()
    # An explicitly fabricated ownership contradiction must retain both ledgers.
    # This isolated ledger is data only; actual resources belong to outer PB.
    q.ledger().ensure_capacity({"cpu": 1})
    assert q.ledger().acquire(key, {"cpu": 1})
    deadline = time.monotonic() + 120
    while q.lease_age(key) <= 1:
        assert time.monotonic() < deadline
        time.sleep(0.1)
    assert q.reap_stale(timeout_s=1) == []
    assert claim.read_bytes() == before and q.lease_path(key).read_bytes() == lease
    assert q.ledger().held() == q.ledger(first["claimed_host"]).held() == {"cpu": 1}
    assert not q.item_path(pool.READY, key).exists()
    assert not q.item_path(pool.DONE, key).exists()
    assert not q.item_path(pool.FAILED, key).exists()
    # Remove exactly the evidence injected by this actor; preserve the original.
    q.ledger().release(key)
    assert q.reap_stale(timeout_s=1) == [key]
    assert q.ledger(first["claimed_host"]).held() == {}
    ready = q.item_path(pool.READY, key)
    assert json.loads(ready.read_text())["attempts"] == 1
    ready_hash = digest(ready)
    attempt = q.attempt_path(first, 1)
    attempt_hash = digest(attempt)
    put(root / "ready.json", {"host": socket.gethostname(),
        "ready_sha256": ready_hash, "attempt_sha256": attempt_hash})
    wait(root / "late-ready.json")
    assert digest(ready) == ready_hash and digest(attempt) == attempt_hash
    assert not q.item_path(pool.DONE, key).exists()
    assert not q.item_path(pool.FAILED, key).exists()
    successor = q.claim(owner=f"peer:{os.getpid()}", capacity={"cpu": 1})
    assert successor is not None and successor["attempts"] == 1
    assert successor["published_unix"] == first["published_unix"]
    claim_hash = digest(claim)
    lease_hash = digest(q.lease_path(key))
    put(root / "successor.json", {"host": socket.gethostname(),
        "claim_sha256": claim_hash, "lease_sha256": lease_hash})
    late = wait(root / "late-claimed.json")
    if stale_claim_read:
        assert late["disposition"] == "ownership_refused"
        assert late["injected_claim_reads"] > 0
    assert digest(claim) == claim_hash and digest(q.lease_path(key)) == lease_hash
    assert digest(attempt) == attempt_hash and q.ledger().held() == {"cpu": 1}
    assert not q.item_path(pool.DONE, key).exists()
    assert not q.item_path(pool.FAILED, key).exists()
    ending = q.finish(key, status="executed", detail={"returncode": 0,
        "stdout": "successor result"}, claim_snapshot=successor)
    record = json.loads(ending.read_text())
    history = q.attempt_outcomes(record)  # Revalidates immutable log hashes.
    assert [row["claimed_by"] for row in history] == [first["claimed_by"], successor["claimed_by"]]
    assert history[0]["status"] == "lease_lost"
    assert history[-1]["stdout"] == "successor result"
    assert q.ledger().held() == q.ledger(first["claimed_host"]).held() == {}
    result = {"terminal_sha256": digest(ending), "history_count": len(history),
              "ambiguity_retained": True, "host": socket.gethostname(),
              "original_host": first["claimed_host"], "late_status": late_status}
    put(root / "terminal.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=["original", "peer"])
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--late-status", required=True, choices=["executed", "failed"])
    parser.add_argument("--stale-claim-read", action="store_true",
                        help="inject the old claim into only the late caller's read; "
                             "require ownership refusal and waiter completion")
    args = parser.parse_args()
    if (not os.environ.get("PRISMABUILD_CONTAINER_OWNER")
            or "prismabuild-job" not in Path("/proc/self/cgroup").read_text()):
        parser.error("submit this qualification through published pbcampaign")
    if not __debug__:
        parser.error("assertions must be enabled")
    root = args.root.resolve()
    allowed = Path("/mnt/shared/pb-qualification")
    if not root.is_relative_to(allowed) or root == allowed:
        parser.error("--root must be a fresh directory beneath /mnt/shared/pb-qualification")
    actor = original if args.role == "original" else peer
    result = actor(root, args.late_status, args.stale_claim_read)
    print(json.dumps({"role": args.role, "host": socket.gethostname(),
                      "result": result}, sort_keys=True))


if __name__ == "__main__":
    main()
