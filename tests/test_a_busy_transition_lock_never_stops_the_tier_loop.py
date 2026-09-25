"""A busy transition lock never stops the tier loop (#1115).

Observed 2026-09-24 on dl380g10: the tier loop blocked in ``fcntl_setlk``
on one key's transition lock for 41 minutes.  The holder, the dl380g10
worker, was stuck in the kernel (``rename`` -> ``__break_lease`` on an NFS
directory delegation the server never recalled).  For those 41 minutes no
window was published, no mover was withdrawn, the dead-consumer pass did not
run, and the tier log was silent.

The loop is single-threaded, so one blocking acquire turns one key's stuck
holder into a fleet-wide stall.  These tests hold one key's lock from
another process -- the ``fcntl`` branch the loop hung in, not the in-process
thread mutex -- and require the pass to return while the holder still holds
it, to name the busy key in a ``transition-lock-busy`` event, and to finish
the work on the next pass once the holder lets go.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE_KIND = f"stage_gib@{TIER}"
MANIFEST = "9" * 64
GIB = storage_tiers.GIB
PHASE_GIB = 2
FIRST = "1" * 64

#: How long a pass may take with one key busy: one poll of the slowest
#: liveness reader (``pool.HEARTBEAT_S``).  A pass that skips a busy key
#: returns in milliseconds; one that blocks never returns while the holder
#: holds, so the bound only has to be finite and declared.
BOUND_S = float(pool.HEARTBEAT_S)

_HOLDER = """
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o600)
fcntl.lockf(fd, fcntl.LOCK_EX)
print("held", flush=True)
sys.stdin.read()
"""


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _row(queue: pool.PoolQueue, key: str,
         resources: dict[str, int]) -> dict[str, object]:
    return {"action_key": key, "cas_root": str(queue.root / "cas"),
            "checkout_root": str(queue.root / "co"),
            "worker_script": str(queue.root / "worker.py"),
            "tags": ["dl380g10"], "resources": resources}


def _plan(queue: pool.PoolQueue, consumer: str, *, label: str,
          phases: int = 2) -> dict[str, object]:
    built = []
    for ordinal in range(phases):
        start, end = ordinal * PHASE_GIB * GIB, (ordinal + 1) * PHASE_GIB * GIB
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end, "stage_gib": PHASE_GIB,
            "mover_row": {
                **_row(queue, _hexkey(f"{label}mover{ordinal}"),
                       {STAGE_KIND: PHASE_GIB, "cpu": 1, "mem_gb": 1}),
                "residency": {
                    "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                    "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                    "range_start_bytes": start, "range_end_bytes": end},
            },
            "egress_row": _row(queue, _hexkey(f"{label}egress{ordinal}"),
                               {"mem_gb": 1}),
        })
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=MANIFEST, manifest_bytes=1 << 30, phases=built)


def _publish_consumer(queue: pool.PoolQueue, consumer: str,
                      plan: dict[str, object]) -> None:
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=consumer, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                   "manifest_sha256": MANIFEST, "manifest_bytes": 1 << 30,
                   "leads": residency_plan.leads_for(plan)})


def _fail_consumer(queue: pool.PoolQueue, consumer: str) -> None:
    source = queue.item_path(pool.READY, consumer)
    record = json.loads(source.read_text())
    record["status"] = "failed"
    queue.item_path(pool.FAILED, consumer).write_text(json.dumps(record))
    source.unlink()


def _lock_path(queue: pool.PoolQueue, key: str) -> Path:
    """The file ``PoolQueue._transition_locked`` locks for ``key``."""

    name = hashlib.sha256(key.encode()).hexdigest()
    return queue.root / "transition-locks" / f"{name}.lock"


class _Holder:
    """Another process holding one key's transition lock until released."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.process = subprocess.Popen(
            [sys.executable, "-c", _HOLDER, str(path)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        assert self.process.stdout is not None
        assert self.process.stdout.readline().strip() == "held"

    @property
    def pid(self) -> int:
        return self.process.pid

    def release(self) -> None:
        if self.process.poll() is None:
            assert self.process.stdin is not None
            self.process.stdin.close()
            self.process.wait(timeout=BOUND_S)


class _Pass:
    """``withdraw_dead_consumer_movers`` on a thread, so a hang is observable."""

    def __init__(self, queue: pool.PoolQueue) -> None:
        self.events: list[dict[str, object]] | None = None
        self.error: BaseException | None = None

        def run() -> None:
            try:
                self.events = tier_loop.withdraw_dead_consumer_movers(queue)
            except BaseException as exc:  # noqa: BLE001 -- reported below
                self.error = exc

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def returned_within(self, seconds: float) -> bool:
        self.thread.join(timeout=seconds)
        return not self.thread.is_alive()


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    return queue


@pytest.fixture()
def holders():
    held: list[_Holder] = []
    passes: list[_Pass] = []
    yield held, passes
    # Release first, so a pass that blocked (the bug) can finish and the
    # process exits cleanly whatever the assertions said.
    for holder in held:
        holder.release()
    for stuck in passes:
        stuck.thread.join(timeout=BOUND_S)


def _busy(events, key: str) -> list[dict[str, object]]:
    return [event for event in events or []
            if event.get("event") == "transition-lock-busy"
            and event.get("lock_key") == key]


def test_a_busy_consumer_lock_skips_that_consumer_and_the_next_pass_converges(
        queue: pool.PoolQueue, holders) -> None:
    """RED before #1115: the pass waited in ``lockf`` for as long as the holder held."""

    held, passes = holders
    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    movers = [_hexkey("firstmover0"), _hexkey("firstmover1")]
    for phase in plan["phases"]:
        queue.publish(**phase["mover_row"], recompute=True)
    _fail_consumer(queue, FIRST)

    holder = _Holder(_lock_path(queue, FIRST))
    held.append(holder)
    run = _Pass(queue)
    passes.append(run)

    assert run.returned_within(BOUND_S), (
        "the dead-consumer pass blocked on the consumer's transition lock")
    assert run.error is None
    busy = _busy(run.events, FIRST)
    assert len(busy) == 1, run.events
    assert busy[0]["consumer"] == FIRST
    assert busy[0]["holder_pid"] == holder.pid
    # Skipped, not half-done: nothing was withdrawn and the plan stays filed.
    for mover in movers:
        assert queue.item_path(pool.READY, mover).exists()
    assert queue.residency_plan_path(FIRST).exists()

    holder.release()
    events = tier_loop.withdraw_dead_consumer_movers(queue)

    assert not _busy(events, FIRST)
    assert {event["mover"] for event in events if event.get("withdrawn")} == set(movers)
    assert not queue.residency_plan_path(FIRST).exists()


def test_a_busy_mover_lock_skips_that_mover_and_keeps_the_plan_until_it_goes(
        queue: pool.PoolQueue, holders) -> None:
    """RED before #1115: the per-mover lock blocked the pass the same way."""

    held, passes = holders
    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    busy_mover, free_mover = _hexkey("firstmover0"), _hexkey("firstmover1")
    for phase in plan["phases"]:
        queue.publish(**phase["mover_row"], recompute=True)
    _fail_consumer(queue, FIRST)

    holder = _Holder(_lock_path(queue, busy_mover))
    held.append(holder)
    run = _Pass(queue)
    passes.append(run)

    assert run.returned_within(BOUND_S), (
        "the dead-consumer pass blocked on a mover's transition lock")
    assert run.error is None
    busy = _busy(run.events, busy_mover)
    assert len(busy) == 1, run.events
    assert busy[0]["consumer"] == FIRST
    assert busy[0]["holder_pid"] == holder.pid
    # The other mover is withdrawn this pass; the busy one waits, and the
    # plan stays filed because the next pass needs it to find that mover.
    assert queue.item_path(pool.WITHDRAWN, free_mover).exists()
    assert queue.item_path(pool.READY, busy_mover).exists()
    assert queue.residency_plan_path(FIRST).exists()

    holder.release()
    events = tier_loop.withdraw_dead_consumer_movers(queue)

    assert not _busy(events, busy_mover)
    assert busy_mover in {event["mover"] for event in events if event.get("withdrawn")}
    assert not queue.residency_plan_path(FIRST).exists()


def test_a_busy_child_lock_defers_the_reap_to_the_next_pass(
        queue: pool.PoolQueue, holders) -> None:
    """RED before #1115: ``handoff_safe`` took each child's lock blocking under ``reap``."""

    held, passes = holders
    plan = _plan(queue, FIRST, label="first")
    _publish_consumer(queue, FIRST, plan)
    # No mover was ever published: the pass has nothing to withdraw and goes
    # straight to the reap, whose handoff proof locks every child in turn.
    _fail_consumer(queue, FIRST)
    child = _hexkey("firstegress1")

    holder = _Holder(_lock_path(queue, child))
    held.append(holder)
    run = _Pass(queue)
    passes.append(run)

    assert run.returned_within(BOUND_S), (
        "the reap blocked on a child's transition lock")
    assert run.error is None
    busy = _busy(run.events, child)
    assert len(busy) == 1, run.events
    assert busy[0]["consumer"] == FIRST
    assert busy[0]["holder_pid"] == holder.pid
    assert queue.residency_plan_path(FIRST).exists()

    holder.release()
    events = tier_loop.withdraw_dead_consumer_movers(queue)

    assert not _busy(events, child)
    assert any(event.get("event") == "residency-plan-reaped" for event in events)
    assert not queue.residency_plan_path(FIRST).exists()


def test_inside_the_cycle_a_blocking_acquire_of_a_busy_key_is_refused_not_waited(
        queue: pool.PoolQueue, holders) -> None:
    """The cycle's guard: anything a pass reaches that still asks to wait."""

    held, _passes = holders
    key, mine = _hexkey("busykey"), _hexkey("mykey")
    holder = _Holder(_lock_path(queue, key))
    held.append(holder)
    seen: list[dict[str, object]] = []
    outcome: dict[str, object] = {}

    def run() -> None:
        with pool.transition_locks_never_wait(seen.append):
            with queue._transition_locked(mine):
                # Nested re-entry of a key this thread holds is unaffected.
                with queue._transition_locked(mine):
                    outcome["nested"] = True
            try:
                with queue._transition_locked(key):
                    outcome["entered"] = True
            except pool.TransitionLockBusy as exc:
                outcome["refused"] = exc

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout=BOUND_S)
    assert not thread.is_alive(), "a blocking acquire waited inside the guard"
    assert outcome.get("nested") is True
    assert "entered" not in outcome
    refused = outcome["refused"]
    assert isinstance(refused, BlockingIOError)       # an OSError, EAGAIN
    assert refused.action_key == key
    assert seen == [{"lock_key": key, "holder": "process",
                     "holder_pid": holder.pid}]

    # Outside the guard a blocking acquire still waits, and the holder's
    # release lets it through.
    done = threading.Event()

    def wait_outside() -> None:
        with queue._transition_locked(key):
            done.set()

    waiter = threading.Thread(target=wait_outside, daemon=True)
    waiter.start()
    assert not done.wait(timeout=0.5)
    holder.release()
    assert done.wait(timeout=BOUND_S)


def test_a_cycle_that_lets_a_record_age_past_the_horizon_says_it_overran(
        monkeypatch, queue: pool.PoolQueue) -> None:
    """A completed long cycle is marked on its ``tier-cycle`` line."""

    clock = [1_000_000.0]
    monkeypatch.setattr(pool, "_now", lambda: clock[0])
    liveness = tier_loop.Liveness(interval_s=5.0)
    record = {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier_id": TIER,
              "tier": "stage", "host": "dl380g10", "sampled_unix": clock[0]}

    liveness.begin_cycle()
    liveness.announce(queue, record)
    clock[0] += 1.0
    liveness.checkpoint("quick")
    assert liveness.end_cycle(completed=True)["overran"] is False

    liveness.begin_cycle()
    liveness.announce(queue, record)
    clock[0] += liveness.horizon_s + 1.0      # one stretch past H
    liveness.checkpoint("stuck-step")
    assert liveness.end_cycle(completed=True)["overran"] is True


def test_a_stuck_loop_is_visible_from_outside_as_an_aged_announcement(
        queue: pool.PoolQueue) -> None:
    """pbmcp's tier rows and pbmetrics both expose ``announced_unix``'s age."""

    import pbmcp
    import pbmetrics

    now = 2_000_000.0
    stuck_for = pool.OFFER_TIMEOUT_S + 300.0
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1, "tier_id": TIER,
        "tier": "stage", "host": "dl380g10", "sampled_unix": now - stuck_for,
        "liveness_refresh": {"after": "sweep_orphans", "refreshes": 1,
                             "minted_unix": now - stuck_for - 60}},
        now=now - stuck_for)

    text = pbmetrics.collect_metrics(queue.root, now=now)
    assert (f'prismabuild_tier_announce_age_seconds{{tier="{TIER}"}} '
            f'{stuck_for:g}' in text
            or f'prismabuild_tier_announce_age_seconds{{tier="{TIER}"}} '
            f'{int(stuck_for)}' in text), [
        line for line in text.splitlines() if "announce_age" in line]

    row = pbmcp._tier_row(json.loads(queue.tier_record_path(TIER).read_text()))
    assert row["announced_age_s"] > pool.OFFER_TIMEOUT_S
    assert row["liveness_refresh"]["after"] == "sweep_orphans"
