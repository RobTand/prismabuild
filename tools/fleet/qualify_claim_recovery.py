"""Two admitted actors qualify queue recovery across a shared filesystem.

Run `original` and `peer` through pbcampaign on different host classes (or
with --same-host and matching hostname tags), with
the same fresh --root beneath /mnt/shared/pb-qualification. This exercises
production queue methods on isolated records, not a production worker fault.
By default inner claims are data only. --real-scope adds bounded direct
payloads in broker scopes; their demand is included in the enclosing PB actions.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import select
import subprocess
import sys
import threading
import time
import uuid
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild import pool
from prismabuild.resource_scope import ResourceScope, scope_pids
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


def scope_containers(scope):
    """Read exact-scope containers; a daemon error never proves absence."""
    prefix = ["/usr/bin/docker", "--host", "unix:///var/run/docker.sock"]
    found = subprocess.run([*prefix, "ps", "-aq", "--no-trunc", "--filter",
                            f"label=prismabuild.scope={scope.unit}"],
                           capture_output=True, text=True, check=True, timeout=20)
    ids = found.stdout.split()
    assert all(re.fullmatch(r"[a-f0-9]{64}", cid) for cid in ids), ids
    if not ids:
        return []
    inspected = subprocess.run([*prefix, "inspect", *ids],
                               capture_output=True, text=True, check=True, timeout=20)
    rows = json.loads(inspected.stdout)
    assert {row["Id"] for row in rows} == set(ids)
    for row in rows:
        assert row["Config"]["Labels"]["prismabuild.scope"] == scope.unit
        assert row["Config"]["Labels"]["prismabuild.action"] == scope.action_key
        assert row["HostConfig"]["CgroupParent"] == scope.unit
    return rows


def remove_stopped_scope_containers(scope):
    """Exceptional teardown removes only verified, already stopped objects."""
    rows = scope_containers(scope)
    assert all(row["State"]["Running"] is False for row in rows), rows
    if rows:
        subprocess.run(["/usr/bin/docker", "--host", "unix:///var/run/docker.sock",
                        "rm", *[row["Id"] for row in rows]],
                       capture_output=True, text=True, check=True, timeout=20)
    assert scope_containers(scope) == []


def check_container_alive(scope, container):
    rows = scope_containers(scope)
    assert len(rows) == 1 and rows[0]["Id"] == container["id"], rows
    row = rows[0]
    assert row["State"]["Running"] is True
    assert row["State"]["Pid"] in scope_pids(scope.cgroup_path)
    assert sorted(os.sched_getaffinity(row["State"]["Pid"])) == container["affinity"]


def inspect_started_container(scope, image, cid):
    assert re.fullmatch(r"[a-f0-9]{64}", cid), cid
    rows = scope_containers(scope)
    assert len(rows) == 1 and rows[0]["Id"] == cid, rows
    row = rows[0]
    assert row["HostConfig"]["Memory"] == scope.memory_max_bytes
    assert row["HostConfig"]["MemorySwap"] == scope.memory_max_bytes
    assert not row["HostConfig"]["Privileged"] and not row["HostConfig"]["DeviceRequests"]
    assert row["HostConfig"]["NetworkMode"] == "none"
    info = {"id": cid, "image_id": row["Image"], "image_requested": image,
            "affinity": sorted(os.sched_getaffinity(0)),
            "memory_max_bytes": row["HostConfig"]["Memory"],
            "scope_id": scope.unit}
    check_container_alive(scope, info)
    return info


def start_scope(q, claim, stack, docker_image=None):
    """Attach an exact disposable broker scope to isolated queue test data.

    This uses the established scope-qualification API, not the production
    launch/preflight path. The 128 MiB cap and inherited CPU affinity fit the
    outer actor's CPU1/mem2 GiB reservation, including an optional sleeping
    Docker payload in that same scope. No GPU payload is used.
    """
    key = claim["action_key"]
    scope = ResourceScope(key, uuid.uuid4().hex, 128 * 1024**2,
                          q.ledger().base / "telemetry" / f"{key}.json")
    scope.create()
    processes = []

    def cleanup():
        # Production recovery may already have retired this exact scope.
        # A redundant stop would overwrite its termination audit reason.
        if scope.cgroup_path.exists():
            scope.terminate_owned("disposable claim qualification cleanup")
        for process in processes:
            process.communicate(timeout=10)
        if docker_image:
            remove_stopped_scope_containers(scope)
        deadline = time.monotonic() + 5
        while True:
            try:
                scope.release()
                break
            except OSError as exc:
                if "scope still populated" not in str(exc) or time.monotonic() >= deadline:
                    raise
                time.sleep(.05)

    stack.callback(cleanup)
    control = scope.control_record()
    control.update(started_monotonic=scope.started,
                   boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip())
    claim["resource_scope"] = control
    put(q.item_path(pool.CLAIMED, key), claim)
    q.write_lease(key, owner=claim["claimed_by"], claim_snapshot=claim)
    # Both parent and descendant remain alive until exact-scope termination.
    # The child ignores SIGTERM to exercise whole-scope cleanup, not just its
    # launcher. It still has a bounded fallback lifetime if the actor fails.
    payload = """import json, os, pathlib, subprocess, sys, time
child = subprocess.Popen([sys.executable, '-c',
    'import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print("ready",flush=True); time.sleep(600)'],
    stdout=subprocess.PIPE, text=True)
assert child.stdout.readline().strip() == 'ready'
cid = None
if len(sys.argv) > 1:
    created = subprocess.run([sys.argv[1], 'run', '--detach', '--pull', 'never',
        '--network', 'none', '--entrypoint', '/bin/sh', sys.argv[2],
        '-c', 'exec sleep 600'], capture_output=True, text=True, check=True, timeout=30)
    cid = created.stdout.strip()
print(json.dumps({'pid':os.getpid(), 'child_pid':child.pid,
    'affinity':sorted(os.sched_getaffinity(0)),
    'cgroup':pathlib.Path('/proc/self/cgroup').read_text(), 'container_id':cid}),flush=True)
time.sleep(600)
"""
    argv = [sys.executable, "-c", payload]
    env = dict(os.environ)
    if docker_image:
        # The single inner launcher invokes the ordinary shim from within its
        # scope. Kernel ancestry selects that scope and inherited PB CPU mask.
        argv.extend([str(Path(__file__).resolve().parent / "docker"), docker_image])
        env.update(PRISMABUILD_CONTAINER_OWNER=key,
                   PRISMABUILD_CONTAINER_MARKER=str(q.container_marker(key)))
    process = subprocess.Popen(scope.wrap_argv(argv), env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    processes.append(process)
    assert select.select([process.stdout], [], [], 40 if docker_image else 20)[0], "scope payload startup stalled"
    line = process.stdout.readline()
    assert line.strip(), "scope payload exited before its startup record"
    started = json.loads(line)
    assert started["affinity"] == sorted(os.sched_getaffinity(0))
    assert scope.unit in started["cgroup"]
    members = scope_pids(scope.cgroup_path)
    assert {started["pid"], started["child_pid"]} <= set(members)
    info = {**started, "scope_id": scope.unit, "nonce": scope.nonce,
            "memory_max_bytes": scope.memory_max_bytes}
    if docker_image:
        info["container"] = inspect_started_container(scope, docker_image, started["container_id"])
    return scope, process, info


def retained_scope(q, first):
    key = first["action_key"]
    live = json.loads(q.item_path(pool.CLAIMED, key).read_text())
    assert pool._same_claim(live, first)
    assert live["resource_scope"] == first["resource_scope"]
    assert q.ledger(first["claimed_host"]).held() == {"cpu": 1}
    assert q.lease_path(key).is_file()
    assert not any(q.item_path(state, key).exists()
                   for state in (pool.READY, pool.DONE, pool.FAILED))
    assert not q.attempt_path(first, 1).exists()
    return live


def recover_real_scope(q, first, scope, process, root, container=None):
    wait(root / "peer-cleanup-ready.json")
    key = first["action_key"]
    retained_scope(q, first)
    caller = threading.get_ident()
    terminate = ResourceScope.terminate_owned
    calls = 0

    def unavailable(target, reason):
        nonlocal calls
        if (threading.get_ident() == caller and target.action_key == key
                and target.nonce == scope.nonce):
            calls += 1
            raise OSError("qualification injected broker unavailability")
        return terminate(target, reason)

    with patch.object(ResourceScope, "terminate_owned", unavailable):
        assert q.reap_stale(timeout_s=1) == []
    assert calls == 1
    live = retained_scope(q, first)
    assert "qualification injected broker unavailability" in live["container_cleanup_pending"]["error"]
    assert process.poll() is None and len(scope_pids(scope.cgroup_path)) >= 2
    if container:
        check_container_alive(scope, container)
    put(root / "local-cleanup-refused.json", {"calls": calls, "payload_alive": True})
    wait(root / "cleanup-refusal-checked.json")
    deadline = time.monotonic() + 10
    while q.reap_stale(timeout_s=1) != [key]:
        retained_scope(q, first)
        assert time.monotonic() < deadline, "scope cleanup did not complete"
        time.sleep(.1)
    process.communicate(timeout=10)
    assert process.returncode != 0 and scope_pids(scope.cgroup_path) == []
    assert not scope.cgroup_path.exists()
    if container:
        assert scope_containers(scope) == [], "production did not remove the original container"
    assert q.ledger().held() == {}
    attempt = json.loads(q.attempt_path(first, 1).read_text())
    telemetry = attempt["detail"]["resource_telemetry"]
    assert telemetry["nonce"] == scope.nonce
    released = scope._request("status")
    assert released["released"] and released["scope_id"] == scope.unit
    put(root / "local-reaped.json", {"scope_id": scope.unit,
        "nonce": scope.nonce, "proxy_returncode": process.returncode,
        "scope_absent": True, "broker_released": True,
        "container_removed": container["id"] if container else None})


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

    # The successor lease and ledger stay real. Production must detect that
    # the stale original claim contradicts them and refuse heartbeat and finish.
    # This does not change shared bytes or simulate a kernel/filesystem stall.
    with patch.object(pool, "_read_json", stale_read):
        try:
            q.write_lease(key, owner=first["claimed_by"], claim_snapshot=first)
        except pool.AmbiguousClaimHolder as exc:
            heartbeat_refusal = str(exc)
        else:
            raise AssertionError("stale-claim heartbeat overwrote the successor lease")
        result = late_finish(q, key, first, status)
    assert injected_reads > 0, "stale claim read was not exercised"
    assert result["disposition"] == "ownership_refused", result
    return {**result, "injected_claim_reads": injected_reads,
            "stale_heartbeat_refusal": heartbeat_refusal}


def original(root, late_status, stale_claim_read=False, *, stack, real_scope=False,
             docker_image=None, same_host=False):
    root.mkdir(parents=True, exist_ok=False)
    q = pool.PoolQueue(root / "queue")
    key = hashlib.sha256(str(root).encode()).hexdigest()
    q.publish(action_key=key, cas_root=root / "cas", checkout_root=root / "checkout",
              worker_script=root / "unused.py", resources={"cpu": 1},
              max_attempts=2, retry_safe=True,
              container_owner=key if docker_image else None)
    first = q.claim(owner=f"original:{os.getpid()}", capacity={"cpu": 1})
    assert first is not None
    scope_info = None
    if real_scope:
        scope, process, scope_info = start_scope(q, first, stack, docker_image)
    outcome = []
    errors = []

    def follow():
        try:
            outcome.append(pbrun.await_outcome(q, key, wait_s=480 if real_scope else 120,
                                              generation=first["published_unix"]))
        except BaseException as exc:
            errors.append(repr(exc))

    waiter = threading.Thread(target=follow, daemon=True)
    waiter.start()
    put(root / "first.json", first)
    # Deliberately cease heartbeats in this isolated queue only.
    if real_scope:
        recover_real_scope(q, first, scope, process, root, scope_info.get("container"))
    ready = wait(root / "ready.json")
    assert (ready["host"] == socket.gethostname()) == same_host, "wrong actor topology"
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
            "late_status": late_status, "waiter_result": outcome[0], "same_host": same_host,
            "real_scope": scope_info,
            "first_attempt_sha256": ready["attempt_sha256"],
            "late_ready": ready_result, "late_claimed": claimed_result}


def peer(root, late_status, stale_claim_read=False, *, stack, real_scope=False,
         docker_image=None, same_host=False):
    first = wait(root / "first.json")
    assert (first["claimed_host"] == socket.gethostname()) == same_host, "wrong actor topology"
    q = pool.PoolQueue(root / "queue")
    key = first["action_key"]
    claim = q.item_path(pool.CLAIMED, key)
    before = claim.read_bytes()
    lease = q.lease_path(key).read_bytes()
    # An explicitly fabricated ownership contradiction must retain both ledgers.
    # This isolated ledger is data only; actual resources belong to outer PB.
    injected = q.ledger("qualification-contradiction" if same_host else None)
    injected.ensure_capacity({"cpu": 1})
    assert injected.acquire(key, {"cpu": 1})
    deadline = time.monotonic() + 120
    while q.lease_age(key) <= 1:
        assert time.monotonic() < deadline
        time.sleep(0.1)
    assert q.reap_stale(timeout_s=1) == []
    assert claim.read_bytes() == before and q.lease_path(key).read_bytes() == lease
    assert injected.held() == q.ledger(first["claimed_host"]).held() == {"cpu": 1}
    assert not q.item_path(pool.READY, key).exists()
    assert not q.item_path(pool.DONE, key).exists()
    assert not q.item_path(pool.FAILED, key).exists()
    # Remove exactly the evidence injected by this actor; preserve the original.
    injected.release(key)
    if real_scope:
        if not same_host:
            assert q.reap_stale(timeout_s=1) == []
            live = retained_scope(q, first)
            error = live["container_cleanup_pending"]["error"]
            assert "resource scope cleanup must run on its claiming host" in error
            put(root / "foreign-cleanup-refused.json", {"error": error})
        put(root / "peer-cleanup-ready.json", {"same_host": same_host})
        wait(root / "local-cleanup-refused.json")
        retained_scope(q, first)
        put(root / "cleanup-refusal-checked.json", {"claim_and_reservation_retained": True})
        wait(root / "local-reaped.json")
    else:
        assert q.reap_stale(timeout_s=1) == [key]
    assert q.ledger(first["claimed_host"]).held() == {}
    ready = q.item_path(pool.READY, key)
    # In real-scope mode the other host published these entries. Its marker
    # in the root directory is not a negative-dentry barrier for this one.
    assert wait(ready)["attempts"] == 1
    ready_hash = digest(ready)
    attempt = q.attempt_path(first, 1)
    wait(attempt)
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
    scope_info = None
    if real_scope:
        scope, process, scope_info = start_scope(q, successor, stack, docker_image)
    claim_hash = digest(claim)
    lease_hash = digest(q.lease_path(key))
    put(root / "successor.json", {"host": socket.gethostname(),
        "claim_sha256": claim_hash, "lease_sha256": lease_hash})
    late = wait(root / "late-claimed.json")
    if stale_claim_read:
        assert late["disposition"] == "ownership_refused"
        assert late["injected_claim_reads"] > 0
        assert late["stale_heartbeat_refusal"]
    assert digest(claim) == claim_hash and digest(q.lease_path(key)) == lease_hash
    assert digest(attempt) == attempt_hash and q.ledger().held() == {"cpu": 1}
    assert not q.item_path(pool.DONE, key).exists()
    assert not q.item_path(pool.FAILED, key).exists()
    if real_scope:
        assert process.poll() is None and len(scope_pids(scope.cgroup_path)) >= 2
        if docker_image:
            check_container_alive(scope, scope_info["container"])
    ending = q.finish(key, status="executed", detail={"returncode": 0,
        "stdout": "successor result"}, claim_snapshot=successor)
    if real_scope:
        deadline = time.monotonic() + 10
        while ending != q.item_path(pool.DONE, key):
            assert q.ledger().held() == {"cpu": 1}
            assert time.monotonic() < deadline, "successor cleanup did not complete"
            time.sleep(.1)
            ending = q.finish(key, status="executed", detail={"returncode": 0,
                "stdout": "successor result"}, claim_snapshot=successor)
    record = json.loads(ending.read_text())
    history = q.attempt_outcomes(record)  # Revalidates immutable log hashes.
    assert [row["claimed_by"] for row in history] == [first["claimed_by"], successor["claimed_by"]]
    assert history[0]["status"] == "lease_lost"
    assert history[-1]["stdout"] == "successor result"
    assert q.ledger().held() == q.ledger(first["claimed_host"]).held() == {}
    if real_scope:
        process.communicate(timeout=10)
        assert process.returncode != 0 and not scope.cgroup_path.exists()
        cleanup = record["resource_scope_cleanup"]
        assert cleanup["complete"] and cleanup["released"]["ok"]
        assert cleanup["nonce"] == scope.nonce
        if docker_image:
            assert scope_containers(scope) == [], "production did not remove the successor container"
    result = {"terminal_sha256": digest(ending), "history_count": len(history),
              "ambiguity_retained": True, "host": socket.gethostname(),
              "real_scope": scope_info,
              "original_host": first["claimed_host"], "late_status": late_status,
              "same_host": same_host, "foreign_cleanup_checked": real_scope and not same_host}
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
    parser.add_argument("--same-host", action="store_true",
                        help="require both actors on one host; qualify late calls against a local successor")
    parser.add_argument("--real-scope", action="store_true",
                        help="qualify foreign/local cleanup refusal and exact broker "
                             "cleanup of bounded direct payloads")
    parser.add_argument("--docker-image", help="with --real-scope, also qualify a sleeping "
                        "container using this cached image's /bin/sh and sleep")
    args = parser.parse_args()
    if args.docker_image and not args.real_scope:
        parser.error("--docker-image requires --real-scope")
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
    with ExitStack() as stack:
        result = actor(root, args.late_status, args.stale_claim_read,
                       stack=stack, real_scope=args.real_scope, docker_image=args.docker_image,
                       same_host=args.same_host)
    print(json.dumps({"role": args.role, "host": socket.gethostname(),
                      "result": result}, sort_keys=True))


if __name__ == "__main__":
    main()
