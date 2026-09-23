"""A kill names what it waited on, and a denial keeps its history (#990, #991).

#982, #985 and #904's capture stall each took about an hour to diagnose,
because the evidence was spread over three hosts' stdout, a latest-only
denial cache, and timestamps (#983 pattern 4):

* A stall kill filed ``no_progress``, the phase and the last unit.  Nothing
  about the rows the action was waiting on: its produced-output exports, its
  movers and egresses, their states, how long they had been ready, or why
  they were refused.
* ``record_denial`` kept one record per action generation and host,
  overwritten every pass and dropped whenever its local lock was busy.  The
  sequence of reasons -- which is what diagnoses a starvation -- was gone.
* The tier loop printed ``window-stalled`` and its other verdicts to its own
  stdout, where no record of the consumer's could reach them.

The fixtures below are those three shapes at test size.
"""
from __future__ import annotations

import fcntl
import os
from pathlib import Path
import socket
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import adaptive_cpu, pool, residency_plan, storage_tiers  # noqa: E402
import test_progress_keeps_a_working_action_alive as progress_fx  # noqa: E402
import tier_loop  # noqa: E402

#: A four-CPU box, so the adaptive path has a CPU map to decide against.
CAPACITY = {"cpu": 4, "mem_gb": 8}
CPU_TIERS = {"preferred": [0, 1], "fallback": [2, 3]}
#: What a spool export demands (``adaptive_cpu.EXPORT_DEMAND``).
EXPORT_DEMAND = {"cpu": 1, "mem_gb": 1}


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _refuse_with(monkeypatch, reasons: list[str]) -> None:
    """A stub controller: every decision is a refusal, named in order.

    The real controller's ``decision`` sets ``last_decision`` and returns
    ``None`` when it refuses, and the claim path files that record as the
    denial's evidence.  This one does the same with a reason it is told, so
    what the test reads back is the claim path's own recording.
    """

    queue_of_reasons = list(reasons)

    def decision(self, item, demand, *, identity=None, owner=None, allowance=None):
        reason = queue_of_reasons.pop(0) if len(queue_of_reasons) > 1 else queue_of_reasons[0]
        self.last_decision = {"reason": reason, "stub": True}
        return None

    monkeypatch.setattr(adaptive_cpu.Controller, "decision", decision)


def _publish_export(queue: pool.PoolQueue, owner: str, seed: str) -> dict:
    """A produced-output export of ``owner``'s, pinned to this host."""

    key = _hexkey(seed)
    queue.publish(action_key=key, cas_root=queue.root / "cas",
                  checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
                  resources=dict(EXPORT_DEMAND), tags=[socket.gethostname()],
                  dependent_of=owner)
    record = pool._read_json(queue.item_path(pool.READY, key))
    assert record is not None
    return record


def _claim_pass(queue: pool.PoolQueue) -> dict | None:
    return queue.claim(capacity=CAPACITY, tags=[socket.gethostname()],
                       cpu_tiers=CPU_TIERS, adaptive_cpu=True)


# -- #990: the ending record names the rows the action waited on ----------

def test_a_stall_kill_names_its_refused_export_its_ready_age_and_the_reason(
        tmp_path, monkeypatch):
    """The #982 shape: a producer dies while its own export is refused."""

    queue, consumer = progress_fx._claimed(
        tmp_path, mode="silent", seconds=60,
        policy=progress_fx._policy(5.0, 5.0, 5.0))
    assert consumer is not None
    owner = str(consumer["action_key"])
    export = _publish_export(queue, owner, "export-0")
    _refuse_with(monkeypatch, ["measurement_holder"])
    assert _claim_pass(queue) is None

    outcome = queue.execute(consumer, timeout_s=60.0, heartbeat_s=0.05,
                            timeout_grace_s=0.2)

    assert outcome["status"] == "timeout"
    assert outcome["termination_reason"] == "no_progress"
    dependents = outcome["dependents"]
    named = {row["key"]: row for row in dependents}
    assert export["action_key"] in named, dependents
    row = named[export["action_key"]]
    assert row["role"] == "produced_export"
    assert row["state"] == "ready"
    assert row["ready_since_unix"] == export["published_unix"]
    # It waited through the whole grace, and the record says how long.
    assert row["ready_age_s"] >= 5.0
    denial = row["last_denial"]
    assert denial["reason"].startswith("adaptive_cpu_refused")
    assert denial["decision_reason"] == "measurement_holder"
    assert denial["host"] == socket.gethostname()
    assert outcome["dependents_truncated"] is False


def test_the_dependents_list_is_bounded_and_says_when_it_is_cut(tmp_path):
    """A campaign producer can have more rows than a record should carry."""

    queue = pool.PoolQueue(tmp_path / "queue")
    owner = _hexkey("owner")
    extra = 5
    for index in range(pool.MAX_ENDING_DEPENDENTS + extra):
        _publish_export(queue, owner, f"bounded-{index}")

    found = queue.dependent_rows(owner)

    assert len(found["dependents"]) == pool.MAX_ENDING_DEPENDENTS
    assert found["dependents_truncated"] is True
    assert found["dependents_total"] == pool.MAX_ENDING_DEPENDENTS + extra


def test_an_ordinary_ending_carries_no_dependents(tmp_path):
    """Only the kill rungs pay for the enumeration."""

    outcome = progress_fx._run(tmp_path, mode="report", seconds=0.5, ceiling=30.0,
                               policy=progress_fx._policy(5.0, 5.0, 5.0))
    assert outcome["status"] == "executed", outcome
    assert "dependents" not in outcome


# -- #991: every reason change survives, in order -------------------------

def test_three_reasons_across_three_passes_are_all_readable_in_order(
        tmp_path, monkeypatch):
    """busy -> measurement_holder -> host_pressure, with the middle one under
    contention for the latest-only file's lock.
    """

    queue = pool.PoolQueue(tmp_path / "queue")
    export = _publish_export(queue, _hexkey("owner"), "export-3")
    key = str(export["action_key"])
    reasons = ["reservation_busy", "measurement_holder", "host_pressure"]
    _refuse_with(monkeypatch, list(reasons))

    assert _claim_pass(queue) is None
    # The second pass runs while another loop on this box holds the
    # latest-only file's lock, which drops that file's write by design.
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    holder = os.open(base / "claim-denials.lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(holder, fcntl.LOCK_EX)
        assert _claim_pass(queue) is None
    finally:
        os.close(holder)
    assert _claim_pass(queue) is None
    # A fourth pass with an unchanged reason is not a transition.
    assert _claim_pass(queue) is None

    history = queue.denial_transitions(key)

    assert [entry["decision_reason"] for entry in history] == reasons
    assert all(entry["host"] == socket.gethostname() for entry in history)
    assert all(entry["reason"].startswith("adaptive_cpu_refused") for entry in history)
    stamps = [entry["unix"] for entry in history]
    assert stamps == sorted(stamps)


def test_an_unchanged_reason_writes_nothing(tmp_path, monkeypatch):
    """The steady state of a starved row is one reason for hours: no I/O."""

    queue = pool.PoolQueue(tmp_path / "queue")
    item = _publish_export(queue, _hexkey("owner"), "steady")
    queue.record_denial(item, "host_pressure")
    path = queue.denial_transitions_path(str(item["action_key"]))
    before = path.stat().st_mtime_ns
    writes = []
    real = pool._write_json_atomic
    monkeypatch.setattr(pool, "_write_json_atomic",
                        lambda target, value: (writes.append(Path(target)), real(target, value)))

    for _ in range(50):
        queue.record_denial(item, "host_pressure")

    assert path not in writes
    assert path.stat().st_mtime_ns == before
    assert len(queue.denial_transitions(str(item["action_key"]))) == 1


def test_the_ring_keeps_only_the_newest_transitions(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    item = _publish_export(queue, _hexkey("owner"), "flapping")
    total = pool.MAX_DENIAL_TRANSITIONS + 4
    for index in range(total):
        queue.record_denial(item, f"reason_{index}")

    history = queue.denial_transitions(str(item["action_key"]))

    assert len(history) == pool.MAX_DENIAL_TRANSITIONS
    assert history[-1]["reason"] == f"reason_{total - 1}"
    assert history[0]["reason"] == f"reason_{total - pool.MAX_DENIAL_TRANSITIONS}"


def test_a_busy_transition_lock_defers_the_entry_rather_than_racing_the_holder(tmp_path):
    """``transition_busy`` is recorded without the key's transition lock.

    Another loop holds that lock and may be writing the ring, so the busy
    verdict waits in this process and lands, in time order, with the next
    verdict this process records for the key under the lock.
    """

    queue = pool.PoolQueue(tmp_path / "queue")
    item = _publish_export(queue, _hexkey("owner"), "busy")
    key = str(item["action_key"])
    queue.record_denial(item, "host_pressure")
    queue.record_denial(item, "transition_busy", locked=False)
    assert [entry["reason"] for entry in queue.denial_transitions(key)] == [
        "host_pressure"]

    queue.record_denial(item, "measurement_holder")

    assert [entry["reason"] for entry in queue.denial_transitions(key)] == [
        "host_pressure", "transition_busy", "measurement_holder"]


def test_a_retired_keys_ring_is_swept_and_a_live_ones_is_kept(tmp_path):
    """The ring's retirement path (checklist 17): the prewarm loop's sweep."""

    queue = pool.PoolQueue(tmp_path / "queue")
    live = _publish_export(queue, _hexkey("owner"), "live")
    gone = _publish_export(queue, _hexkey("owner"), "gone")
    for item in (live, gone):
        queue.record_denial(item, "host_pressure")
    gone_key = str(gone["action_key"])
    Path(queue.item_path(pool.READY, gone_key)).rename(queue.item_path(pool.FAILED, gone_key))

    rows = queue.sweep_denial_transitions({str(live["action_key"])})

    assert rows == [{"action_key": gone_key, "pruned": True, "reason": "terminal"}]
    assert queue.denial_transitions(str(live["action_key"]))
    assert not queue.denial_transitions_path(gone_key).exists()


def test_pbstatus_starvation_reads_a_starved_producer_in_one_place(tmp_path, monkeypatch):
    """The runbook's one command: the producer, its export, why, and since when."""

    import pbstatus

    queue, consumer = progress_fx._claimed(tmp_path, mode="silent", seconds=1,
                                           policy=None)
    owner = str(consumer["action_key"])
    export = _publish_export(queue, owner, "export-status")
    _refuse_with(monkeypatch, ["measurement_holder", "host_pressure"])
    assert _claim_pass(queue) is None
    assert _claim_pass(queue) is None

    blob = pbstatus.read_starvation(queue.root)

    [entry] = [row for row in blob["claimed_dependents"] if row["action_key"] == owner]
    [row] = entry["dependents"]
    assert row["key"] == export["action_key"]
    assert row["role"] == "produced_export"
    assert row["last_denial"]["decision_reason"] == "host_pressure"
    assert [value["decision_reason"] for value in row["denial_transitions"]] == [
        "measurement_holder", "host_pressure"]


# -- #990: the tier loop's verdicts reach the consumer's own record -------

def test_a_window_stalled_verdict_is_readable_from_the_consumers_event_file(
        tmp_path, monkeypatch):
    """#904's capture stall printed ``window-stalled`` to stdout only."""

    import test_the_window_stops_when_the_consumer_releases_nothing as stall_fx

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(stall_fx.TIER, {"stage_gib": stall_fx.STAGE_CAPACITY_GIB})
    plan = stall_fx._plan(queue)
    residency_plan.freeze(queue, plan)
    queue.publish(
        action_key=stall_fx.CONSUMER, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1, "mem_gb": 1},
        residency={"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": stall_fx.TIER,
                   "manifest_sha256": stall_fx.MANIFEST, "manifest_bytes": 1 << 40,
                   "leads": residency_plan.leads_for(plan)})
    stage = tmp_path / "stage"
    stage.mkdir()

    def discover(**_kwargs):
        return {stall_fx.TIER: {
            "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
            "tier_id": stall_fx.TIER, "host": "dl380g10", "tier": "stage",
            "mountpoint": str(stage),
            "capacity_bytes": stall_fx.STAGE_CAPACITY_GIB * storage_tiers.GIB}}

    # A fresh loop: no verdict carried over from another test's cycles.
    monkeypatch.setattr(tier_loop, "_VERDICT_SINCE", {})
    for _ in range(2):
        tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                        receipts=tier_loop.ReceiptCache(), discover=discover)

    events = queue.consumer_events(stall_fx.CONSUMER)

    stalled = [event for event in events if event.get("event") == "window-stalled"]
    assert stalled, events
    assert stalled[-1]["consumer"] == stall_fx.CONSUMER
    assert stalled[-1]["reason"] == "no_accepted_progress"
    assert stalled[-1]["host"] == "dl380g10"
    # A wait is a duration: the second cycle says how long it has lasted.
    assert stalled[0]["waited_s"] == 0.0
    assert stalled[-1]["waited_s"] > 0.0


def test_the_event_file_is_bounded_and_keeps_the_newest(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    consumer = _hexkey("consumer")
    monkeypatch.setattr(tier_loop, "_EVENT_LINES", {})
    total = 2 * pool.MAX_CONSUMER_EVENT_LINES + 3
    for index in range(total):
        tier_loop._emit(queue, "sparky", {"event": "window-stalled",
                                          "consumer": consumer, "ordinal": index})

    events = queue.consumer_events(consumer)

    assert len(events) <= 2 * pool.MAX_CONSUMER_EVENT_LINES
    assert events[-1]["ordinal"] == total - 1
    assert [event["ordinal"] for event in events] == sorted(
        event["ordinal"] for event in events)


def test_a_tier_verdict_naming_no_consumer_is_filed_for_the_tiers_consumers(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    consumer = _hexkey("consumer")
    tier_loop._emit(queue, "dl380g10",
                    {"event": "beyond-horizon-eviction-futile", "tier_id": "t",
                     "needed_gib": 9, "free_gib": 1, "beyond_horizon_gib": 2},
                    tier_consumers={"t": [consumer]})

    [event] = queue.consumer_events(consumer)

    assert event["event"] == "beyond-horizon-eviction-futile"
    assert event["attributed_by"] == "tier_id"
    assert event["waited_s"] >= 0.0


def test_reaping_the_plan_retires_the_consumers_event_file(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    consumer = _hexkey("consumer")
    tier_loop._emit(queue, "dl380g10", {"event": "window-stalled", "consumer": consumer})
    assert queue.consumer_events(consumer)

    tier_loop._emit(queue, "dl380g10", {"event": "residency-plan-reaped",
                                        "consumer": consumer, "tier_id": "t"})

    assert not queue.consumer_events_dir(consumer).exists()
