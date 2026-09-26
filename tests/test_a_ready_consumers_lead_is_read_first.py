"""A ready consumer's lead is read first, and its denial says it is coming (#1186).

On 2026-09-26 a ``--measurement`` GPU row (``43bc7c59ca96``) sat ready for
28 min behind its 3 GiB lead (``263cfdfa1754``).  The lead copied at
1.8 MB/s: the disk pacer held it for 1,681.7 s of 1,695 s.  Two defects:

1. **The lead was never exempt.**  The tier's reader plan (``declared_wait``,
   #1091) listed only copies that *claimed* consumers declared a wait on.  A
   consumer the claim pass refuses until its lead lands can never be claimed,
   so it never declared one, and its lead was paced behind every other
   client reader on the pool.
2. **The denial read a stale ending.**  The lead's key is its range's content
   address, so the copy was the same key a previous consumer's lead had
   used.  Admission read that earlier ``done/`` record (``unpinned``) and
   reported ``residency_lead_terminal``, while the same key was claimed and
   copying under a later generation.

Each case below is red on ``origin/main`` ``041d030df345`` with its own
assertion.  Everything runs on ``tmp_path`` queues and stage roots; nothing
touches a live queue or a real stage mountpoint (#628).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import adaptive_cpu, pool, residency_plan  # noqa: E402
import prewarm_loop  # noqa: E402
from test_a_blocked_copy_goes_first_on_its_tier import (  # noqa: E402
    FILL_KIND, KNEE, SEALED_FILL_MB_S, WORKER, _clear_channel, _declare_wait,
    _file_curve, _hurting, _window)
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _claim, _fixture_queue, _plan, _publish_consumer)
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    STAGE_KIND, TIER, _cycle, _hexkey, _row)

#: The ready consumer, as the measurement row was: refused on its lead.
WAITING = _hexkey("ready-consumer")
WAITING_MANIFEST = "9" * 64
#: A claimed consumer reading earlier phases, with later-phase copies queued.
RUNNING = _hexkey("running-reader")
RUNNING_MANIFEST = "a" * 64


def _tier_plan(queue: pool.PoolQueue) -> dict[str, object]:
    return json.loads(queue.tier_record_path(TIER).read_text())["reader_plan"]


def _listed(queue: pool.PoolQueue) -> list[str]:
    return [str(row["mover_action_key"]) for row in _tier_plan(queue)["declared_wait"]]


def _claim_row(queue: pool.PoolQueue, key: str, *, host: str = "dl380g10") -> None:
    """Move ``key`` from ``ready/`` to ``claimed/`` as a worker's claim does."""

    ready = queue.item_path(pool.READY, key)
    queue.item_path(pool.CLAIMED, key).write_text(json.dumps({
        **json.loads(ready.read_text()), "claimed_unix": time.time(),
        "claimed_by": f"{host}:1:fixture", "claimed_host": host}))
    ready.unlink()


def _denial(queue: pool.PoolQueue, key: str) -> dict[str, object] | None:
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    matching = [entry for entry in records.values()
                if isinstance(entry, dict) and entry.get("action_key") == key]
    if not matching:
        return None
    return max(matching, key=lambda entry: float(entry.get("denied_unix", 0.0)))


def _end_earlier_generation(queue: pool.PoolQueue, row: dict[str, object]) -> float:
    """File ``row``'s key as an earlier consumer's lead that ran and ended.

    The incident's shape: ``executed`` in ``done/`` under one generation,
    holding no tier tokens (an egress took its range back), and the same key
    published again afterwards.  Returns the ended generation.
    """

    queue.publish(**row)
    key = str(row["action_key"])
    record = json.loads(queue.item_path(pool.READY, key).read_text())
    done = queue.item_path(pool.DONE, key)
    done.parent.mkdir(parents=True, exist_ok=True)
    done.write_text(json.dumps({
        **record, "status": "executed", "claimed_unix": time.time(),
        "claimed_host": "dl380g10", "finished_unix": time.time(),
        "finished_host": "dl380g10"}))
    queue.item_path(pool.READY, key).unlink()
    time.sleep(0.01)
    return float(record["published_unix"])


# ------------------------------------------------------- defect 1: the plan


def test_a_ready_consumers_lead_is_listed_and_claimed_before_other_copies(
        tmp_path: Path) -> None:
    """First staging: the lead is queued, the consumer is refused on it.

    A running reader's later-phase copy was queued first, so the queue's
    oldest-first order would claim it first.  The ready consumer can run
    only once its lead lands, and nothing but the lead stands in its way.
    """

    queue, stage = _fixture_queue(tmp_path, 40)
    running = _plan(queue, RUNNING, label="running", manifest=RUNNING_MANIFEST,
                    phases=3)
    _publish_consumer(queue, RUNNING, running, manifest=RUNNING_MANIFEST)
    now = time.time()
    _claim(queue, RUNNING, phase="phase-0", claimed_unix=now - 100.0,
           reported_unix=now - 10.0)
    later = dict(running["phases"][2]["mover_row"])        # type: ignore[index]
    queue.publish(**later)
    time.sleep(0.01)
    plan = _plan(queue, WAITING, label="waiting", manifest=WAITING_MANIFEST,
                 phases=2)
    _publish_consumer(queue, WAITING, plan, manifest=WAITING_MANIFEST)
    lead = str(residency_plan.leads_for(plan)[0])
    queue.publish(**dict(plan["phases"][0]["mover_row"]))  # type: ignore[index]

    _cycle(queue, stage, gib=40)

    rows = _tier_plan(queue)["declared_wait"]
    assert [row["mover_action_key"] for row in rows] == [lead], (
        f"the lead a ready consumer is refused on is not in the reader plan: {rows}")
    assert rows[0]["consumers"] == [WAITING]
    assert rows[0]["ready_consumers"] == [WAITING]
    assert rows[0]["state"] == pool.READY

    claimed = []
    for _ in range(6):
        item = queue.claim(capacity=dict(WORKER), tags=["dl380g10"])
        if item is None:
            break
        claimed.append(str(item["action_key"]))
    assert claimed and claimed[0] == lead, claimed
    assert str(later["action_key"]) not in claimed, (
        f"a copy nobody waits on was claimed beside the lead: {claimed}")
    denial = _denial(queue, WAITING)
    assert denial is not None
    assert denial["reason"] == "residency_lead_not_resident", denial


def _move_args(queue: pool.PoolQueue, stage: Path, manifest: Path, digest: str,
               *, key: str, consumer: str, start: int, end: int):
    import stage_move

    return stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root), "--action-key", key,
        "--consumer-action-key", consumer, "--tier-id", TIER,
        "--stage-root", str(stage), "--manifest-sha256", digest,
        "--manifest", str(manifest),
        "--residency-root", str(queue.residency_fragment_root()),
        "--range-start-bytes", str(start), "--range-end-bytes", str(end),
        "--readers", "1", "--max-readers", "1",
        "--warm-after-copy", "never", "--unpaced",
        "--progress-interval-s", "0.02"])


def test_the_pacer_never_holds_a_ready_consumers_lead_run_again(
        tmp_path: Path, monkeypatch) -> None:
    """The incident: a lead key that ended once, claimed again for a new consumer.

    The pool is hurting throughout (every pacer wait holds 50 ms), as the
    dl380g10 pool was while hash passes read it.  The lead is exempt only if
    the reader plan lists it.
    """

    import stage_move

    _clear_channel(monkeypatch)
    monkeypatch.setattr(prewarm_loop, "pacer_from_args", _hurting)
    queue, stage = _fixture_queue(tmp_path, 8)
    manifest, digest, total = _window(tmp_path, entries=6)
    half = total // 2
    phases = []
    for ordinal, (start, end) in enumerate(((0, half), (half, total))):
        phases.append({
            "name": f"layer-{ordinal}", "start_bytes": start, "end_bytes": end,
            "stage_gib": 1,
            "mover_row": {
                **_row(queue, _hexkey(f"leadmover{ordinal}"),
                       {STAGE_KIND: 1, FILL_KIND: SEALED_FILL_MB_S,
                        "cpu": 1, "mem_gb": 1}),
                "residency": {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                              "manifest_sha256": digest, "manifest_bytes": total,
                              "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _row(queue, _hexkey(f"leadegress{ordinal}"),
                               {"mem_gb": 1})})
    plan = residency_plan.build_plan(
        consumer_action_key=WAITING, tier_id=TIER, stage_root=str(stage),
        manifest_sha256=digest, manifest_bytes=total, phases=phases)
    lead_row = dict(phases[0]["mover_row"])
    lead = str(lead_row["action_key"])
    _end_earlier_generation(queue, lead_row)
    _publish_consumer(queue, WAITING, plan, manifest=digest)
    queue.publish(**lead_row, recompute=True)
    _claim_row(queue, lead)

    _cycle(queue, stage, gib=8)

    assert _listed(queue) == [lead], (
        "the reader plan does not list the lead a ready consumer is refused on: "
        f"{_tier_plan(queue)['declared_wait']}")
    receipt = stage_move.move(_move_args(queue, stage, manifest, digest, key=lead,
                                         consumer=WAITING, start=0, end=half))

    assert receipt["complete"], receipt
    assert receipt["reader_plan"]["exempt"] is True, receipt["reader_plan"]
    assert receipt["disk_pacing"]["held_seconds"] == 0, (
        "the pacer held the lead a ready consumer is refused on: "
        f"{receipt['disk_pacing']['held_seconds']} s")


def test_ready_leads_fill_only_the_room_under_the_measured_cap(
        tmp_path: Path) -> None:
    """Ready consumers are bounded by the pool's knee, not by the window.

    A copy in the plan is claimed past the cap, so the plan itself must stay
    under it.  A claimed consumer's wait is always listed; ready consumers'
    leads fill the rest of the knee, oldest first.
    """

    queue, stage = _fixture_queue(tmp_path, 64)
    _file_curve(queue, stage)
    running = _plan(queue, RUNNING, label="running", manifest=RUNNING_MANIFEST,
                    phases=3)
    _publish_consumer(queue, RUNNING, running, manifest=RUNNING_MANIFEST)
    now = time.time()
    _claim(queue, RUNNING, phase="phase-0", claimed_unix=now - 100.0,
           reported_unix=now - 10.0)
    blocked = str(running["phases"][1]["mover_row"]["action_key"])  # type: ignore[index]
    queue.publish(**dict(running["phases"][1]["mover_row"]))       # type: ignore[index]
    _declare_wait(queue, RUNNING, [blocked], since_unix=now - 5.0)
    leads = []
    for ordinal in range(5):
        consumer = _hexkey(f"ready{ordinal}")
        plan = _plan(queue, consumer, label=f"ready{ordinal}",
                     manifest=f"{ordinal + 1}" * 64, phases=1)
        _publish_consumer(queue, consumer, plan, manifest=f"{ordinal + 1}" * 64)
        queue.publish(**dict(plan["phases"][0]["mover_row"]))    # type: ignore[index]
        leads.append(str(residency_plan.leads_for(plan)[0]))
        time.sleep(0.01)

    _cycle(queue, stage, gib=64)

    listed = _listed(queue)
    assert len(listed) == KNEE, (
        f"{len(listed)} copies listed; the pool's knee is {KNEE}: {listed}")
    assert blocked in listed
    assert set(listed) - {blocked} == set(leads[:KNEE - 1]), listed


# ------------------------------------------------------ defect 2: the denial


def _terminal_queue(tmp_path: Path) -> tuple[pool.PoolQueue, str]:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    mover = _hexkey("mover")
    return queue, mover


def _publish_waiting(queue: pool.PoolQueue, lead: str) -> None:
    queue.publish(action_key=WAITING, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co",
                  worker_script=queue.root / "worker.py", resources={"cpu": 1},
                  residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                             "manifest_sha256": "b" * 64, "manifest_bytes": 4096,
                             "leads": [lead]})


def _mover_row(queue: pool.PoolQueue, key: str) -> dict[str, object]:
    return {"action_key": key, "cas_root": queue.root / "cas",
            "checkout_root": queue.root / "co",
            "worker_script": queue.root / "worker.py",
            "resources": {"cpu": 1}, "tags": ["dl380g10"]}


def test_a_lead_claimed_again_is_not_terminal_and_the_denial_names_the_claim(
        tmp_path: Path) -> None:
    queue, mover = _terminal_queue(tmp_path)
    ended = _end_earlier_generation(queue, _mover_row(queue, mover))
    queue.publish(**_mover_row(queue, mover), recompute=True)
    _claim_row(queue, mover)
    live = json.loads(queue.item_path(pool.CLAIMED, mover).read_text())
    _publish_waiting(queue, mover)

    assert queue.claim(owner="worker", capacity={"cpu": 4}, tags=["sparky"]) is None

    denial = _denial(queue, WAITING)
    assert denial is not None
    assert denial["reason"] == "residency_lead_not_resident", denial["reason"]
    [entry] = denial["evidence"]["residency"]["pending"]
    assert entry["lead"] == mover
    assert entry["status"] == pool.CLAIMED, entry
    assert entry["live"]["claimed_host"] == "dl380g10"
    assert entry["live"]["published_unix"] == live["published_unix"]
    assert entry["replaces"]["status"] == "unpinned"
    assert entry["replaces"]["published_unix"] == ended
    assert queue.item_path(pool.READY, WAITING).exists()


def test_a_lead_queued_again_reads_ready_not_terminal(tmp_path: Path) -> None:
    queue, mover = _terminal_queue(tmp_path)
    _end_earlier_generation(queue, _mover_row(queue, mover))
    queue.publish(**_mover_row(queue, mover), recompute=True)
    _publish_waiting(queue, mover)

    verdict = queue.residency_verdict(
        json.loads(queue.item_path(pool.READY, WAITING).read_text()))

    assert verdict["state"] == "lead_not_resident", verdict
    assert verdict["pending"][0]["status"] == pool.READY, verdict


def test_an_ending_of_the_live_records_own_generation_stays_terminal(
        tmp_path: Path) -> None:
    """A finish filed before its claim was gone is that claim's ending.

    Only a *later* generation replaces an ending: a claimed record beside a
    ``done/`` record of the same ``published_unix`` is one run, finishing.
    """

    queue, mover = _terminal_queue(tmp_path)
    queue.publish(**_mover_row(queue, mover))
    _claim_row(queue, mover)
    claimed = json.loads(queue.item_path(pool.CLAIMED, mover).read_text())
    done = queue.item_path(pool.DONE, mover)
    done.parent.mkdir(parents=True, exist_ok=True)
    done.write_text(json.dumps({**claimed, "status": "executed",
                                "finished_unix": time.time()}))
    _publish_waiting(queue, mover)

    assert queue.claim(owner="worker", capacity={"cpu": 4}, tags=["sparky"]) is None

    denial = _denial(queue, WAITING)
    assert denial is not None
    assert denial["reason"] == "residency_lead_terminal", denial["reason"]
    assert denial["evidence"]["residency"]["pending"] == [
        {"lead": mover, "status": "unpinned"}]


def test_a_requeue_bound_to_another_manifest_stays_terminal(tmp_path: Path) -> None:
    """A lead run again for the same wrong manifest will mismatch again."""

    queue, mover = _terminal_queue(tmp_path)
    row = {**_mover_row(queue, mover), "resources": {"cpu": 1, STAGE_KIND: 1},
           "residency": {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                         "manifest_sha256": "c" * 64, "manifest_bytes": 4096,
                         "range_start_bytes": 0, "range_end_bytes": 4096}}
    _end_earlier_generation(queue, row)
    queue.publish(**row, recompute=True)
    _claim_row(queue, mover)
    _publish_waiting(queue, mover)

    assert queue.claim(owner="worker", capacity={"cpu": 4}, tags=["sparky"]) is None

    denial = _denial(queue, WAITING)
    assert denial is not None
    assert denial["reason"] == "residency_lead_terminal", denial["reason"]
    [entry] = denial["evidence"]["residency"]["pending"]
    assert entry["status"] == "manifest_mismatch", entry
