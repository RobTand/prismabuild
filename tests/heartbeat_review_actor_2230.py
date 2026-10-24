"""Real-process actors for the independent L2 heartbeat review (not collected).

Each actor is its own interpreter process sharing one queue directory, so key
locks are contended through POSIX locks between processes, not threads.
Coordination is by stdout events and a stdin STOP word (event-driven).
Periodic beats and judge samples model worker timers for at most 900 seconds;
these waits are for scheduled sample times, not result polling.
"""
import json
import ctypes
import os
from pathlib import Path
import select
import struct
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tests"))

from prismabuild import census, pool, record_store  # noqa: E402

GRACE = {"accepted_count": 0, "staged_wait_exempt_s": 0.0, "quiet_s": 0.0, "grace_s": 720.0}


def emit(**fields):
    sys.stdout.write(json.dumps(fields, sort_keys=True, default=repr) + "\n")
    sys.stdout.flush()


def stop_event(wake_fd=None):
    event = threading.Event()

    def watch():
        for line in sys.stdin:
            if line.strip() == "STOP":
                break
        event.set()
        if wake_fd is not None:
            try:
                os.write(wake_fd, b"x")
            except OSError:
                pass  # actor shutdown already closed the wake pipe
    threading.Thread(target=watch, daemon=True).start()
    return event


def install_stalls(queue, key, stall):
    """Delay every heartbeat read and write point by ``stall`` seconds."""
    lease_path = queue.records.lease_path(key)
    claim_path = queue.records.item_path(record_store.CLAIMED, key)
    stamp_dir = queue.root / "lease-heartbeats" / key
    read, replace, item = record_store.read_document, queue.records.replace_record_at, queue.records.item

    def slow_read(path):
        if Path(path) in (lease_path, claim_path) or Path(path).parent == stamp_dir:
            threading.Event().wait(stall)  # deliberate I/O stall, not a condition poll
        return read(path)

    def slow_replace(path, payload):
        if Path(path) == lease_path or Path(path).parent == stamp_dir:
            threading.Event().wait(stall)  # deliberate I/O stall, not a condition poll
        return replace(path, payload)

    def slow_item(state, k):
        if state == record_store.CLAIMED and k == key:
            threading.Event().wait(stall)  # deliberate I/O stall, not a condition poll
        return item(state, k)
    record_store.read_document = slow_read
    queue.records.replace_record_at = slow_replace
    queue.records.item = slow_item


def beat_loop(queue, key, owner, item, interval, stop, limit=None):
    lease_path = queue.records.lease_path(key)
    beats = stamped = 0
    errors = []
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline and not stop.is_set() and (limit is None or beats < limit):
        before = lease_path.read_bytes()
        started = time.time()
        try:
            queue.write_lease(key, owner=owner, claim_snapshot=item, progress_observation=GRACE)
        except Exception as exc:  # noqa: BLE001 - in production this kills the payload
            errors.append(repr(exc))
            emit(event="beat_error", at=started, error=repr(exc))
        else:
            beats += 1
            contended = lease_path.read_bytes() == before
            stamped += contended
            emit(event="beat", at=started, done=time.time(), stamped=contended)
        stop.wait(interval)
    return beats, stamped, errors


def worker(args):
    if offset := args.get("clock_offset_s"):
        clock = pool._now
        pool._now = lambda: clock() + float(offset)
    queue = pool.PoolQueue(Path(args["root"]))
    key, owner = args["key"], args["owner"]
    item = queue.claim(owner=owner, capacity={"cpu": 2})
    assert item and item["action_key"] == key, item
    with open(args["runs"], "a") as runs:
        runs.write(f"{owner} {os.getpid()} {item['attempts']}\n")
    emit(event="claimed", pid=os.getpid(), attempts=item["attempts"], claimed_unix=item["claimed_unix"])
    if args.get("stall_s"):
        install_stalls(queue, key, float(args["stall_s"]))
    beats, stamped, errors = beat_loop(queue, key, owner, item, float(args["interval_s"]), stop_event())
    emit(event="worker_exit", beats=beats, stamped=stamped, errors=errors)


def claimer(args):
    """A second worker polling for the same key: any claim is a second run."""
    queue = pool.PoolQueue(Path(args["root"]))
    stop = stop_event()
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline and not stop.is_set():
        try:
            item = queue.claim(owner=args["owner"], capacity={"cpu": 2})
        except Exception as exc:  # noqa: BLE001
            emit(event="claim_error", error=repr(exc))
            item = None
        if item:
            with open(args["runs"], "a") as runs:
                runs.write(f"{args['owner']} {os.getpid()} {item['attempts']}\n")
            emit(event="claimed", at=time.time(), key=item["action_key"], attempts=item["attempts"])
            if args.get("finish"):
                own = threading.Event()
                beats, stamped, errors = beat_loop(queue, item["action_key"], args["owner"], item,
                                                   float(args.get("interval_s", 2)), own,
                                                   limit=int(args.get("beats", 4)))
                queue.finish(item["action_key"], status="executed", detail={"returncode": 0},
                             claim_snapshot=item)
                emit(event="finished", at=time.time(), beats=beats, stamped=stamped, errors=errors)
                return
        stop.wait(float(args["period_s"]))
    emit(event="claimer_exit")


def holder(args):
    queue = pool.PoolQueue(Path(args["root"]))
    key, hold = args["key"], float(args["hold_s"])
    reap_from = float(args.get("reap_from_s", hold + 1))
    pause = stop_event()
    with queue.action_transition_lock(key) as acquired:
        assert acquired
        start = time.time()
        emit(event="acquired", at=start, pid=os.getpid())
        while not pause.is_set() and (elapsed := time.time() - start) < hold:
            if elapsed < reap_from:
                pause.wait(min(reap_from, hold) - elapsed)
                continue
            # A reaper that stalled while holding this key judges it now.
            raw = record_store.read_document(queue.records.lease_path(key))
            sample = {"raw_lease_age": time.time() - raw["heartbeat_unix"]}
            try:
                sample["lease_age"] = queue.lease_age(key)
                sample["reaped"] = queue.reap_stale()
            except Exception as exc:  # noqa: BLE001
                sample["error"] = repr(exc)
            emit(event="held_reap", at=time.time(), **sample)
            pause.wait(2.0)
    emit(event="released", at=time.time())


def judge(args):
    queue = pool.PoolQueue(Path(args["root"]))
    key = args["key"]
    stop = stop_event()
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline and not stop.is_set():
        sample = {"at": time.time()}
        for name, call in (("reaped", queue.reap_stale), ("lease_age", lambda: queue.lease_age(key))):
            try:
                sample[name] = call()
            except Exception as exc:  # noqa: BLE001
                sample[name + "_error"] = repr(exc)
        claim = record_store.read_document(queue.records.item_path(record_store.CLAIMED, key))
        now = pool._now()  # sampled before the reads, as the production callers do
        if claim is not None:
            mover = queue._mover_lease(key, claim, now=now)
            sample["mover_live"] = None if mover is None else mover["lease_live"]
            live, evidence = queue._claim_order_ahead_live(
                key, now=now, window_s=60.0,
                prior={"evidence_unix": now - 3600, "accepted_count": 0, "staged_wait_exempt_s": 0.0})
            sample["order_live"], sample["order_evidence"] = live, evidence.get("evidence")
        try:
            rows = census.action_rows(queue.root).value["jobs"]
            row = next((row for row in rows if row.get("action_key") == key), None)
            sample["census"] = None if row is None else (row.get("state"), row.get("stale"))
        except Exception as exc:  # noqa: BLE001
            sample["census_error"] = repr(exc)
        raw = record_store.read_document(queue.records.lease_path(key))
        sample["raw_lease_age"] = None if raw is None else time.time() - raw["heartbeat_unix"]
        emit(event="judge", **sample)
        stop.wait(float(args["period_s"]))
    emit(event="judge_exit")


def skew_holder(args):
    """Judge immediately on each atomic stamp replacement under a held key.

    The directory watch is installed before taking the key. select blocks for
    a publication or STOP; its only timeout is the experiment's hold deadline.
    """
    queue = pool.PoolQueue(Path(args["root"]))
    key = args["key"]
    lease = record_store.read_document(queue.records.lease_path(key))
    stamp_path = queue._heartbeat_stamp_path(lease)
    stamp_path.parent.mkdir(parents=True, exist_ok=True)
    libc = ctypes.CDLL(None, use_errno=True)
    fd = libc.inotify_init1(os.O_CLOEXEC | os.O_NONBLOCK)
    if fd < 0:
        raise OSError(ctypes.get_errno(), "inotify_init1")
    stop_r, stop_w = os.pipe2(os.O_CLOEXEC | os.O_NONBLOCK)
    stopped = stop_event(stop_w)
    reaps = 0
    try:
        moved_to = 0x80
        if libc.inotify_add_watch(fd, os.fsencode(stamp_path.parent), moved_to) < 0:
            raise OSError(ctypes.get_errno(), "inotify_add_watch")
        with queue.action_transition_lock(key) as acquired:
            assert acquired
            start = time.monotonic()
            deadline = start + float(args["hold_s"])
            emit(event="acquired", at=time.time())
            while not stopped.is_set() and (remaining := deadline - time.monotonic()) > 0:
                readable, _, _ = select.select([fd, stop_r], [], [], remaining)
                if stop_r in readable:
                    break
                if fd not in readable:
                    continue  # hold deadline, checked by the loop condition
                events = os.read(fd, 65536)
                at = 0
                replacements = 0
                while at < len(events):
                    _, mask, _, size = struct.unpack_from("iIII", events, at)
                    name = events[at + 16:at + 16 + size].rstrip(b"\0")
                    replacements += bool(mask & moved_to and name == os.fsencode(stamp_path.name))
                    at += 16 + size
                for _ in range(replacements):
                    now = pool._now()
                    stamp = record_store.read_document(stamp_path)
                    lead = stamp["heartbeat_unix"] - now
                    current = record_store.read_document(queue.records.item_path(record_store.CLAIMED, key))
                    mover = queue._mover_lease(key, current, now=now)
                    order_live, _ = queue._claim_order_ahead_live(key, now=now, window_s=60.,
                        prior={"evidence_unix": now - 3600, "accepted_count": 0, "staged_wait_exempt_s": 0.})
                    row = next(row for row in census.action_rows(queue.root).value["jobs"]
                               if row["action_key"] == key)
                    age = queue.lease_age(key)
                    reaped = queue.reap_stale()
                    reaps += len(reaped)
                    emit(event="skew_sample", at=now, lead_s=lead,
                         late=now - lease["heartbeat_unix"] > pool.LEASE_TIMEOUT_S,
                         inside_skew=0 < lead <= pool.OFFER_FUTURE_TOLERANCE_S,
                         lease_live=age is not None and age <= pool.LEASE_TIMEOUT_S,
                         mover_live=mover is not None and mover["lease_live"],
                         order_live=order_live, census_live=not row["stale"], reaps=len(reaped))
        emit(event="released", at=time.time(), hold_s=time.monotonic() - start, reaps=reaps)
    finally:
        for descriptor in (fd, stop_r, stop_w):
            os.close(descriptor)


if __name__ == "__main__":
    {"worker": worker, "claimer": claimer, "holder": holder, "judge": judge,
     "skew-holder": skew_holder}[sys.argv[1]](
        json.loads(sys.argv[2]))
