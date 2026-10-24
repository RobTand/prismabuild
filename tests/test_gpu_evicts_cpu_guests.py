"""GPU denials reclaim restartable CPU guests through ordinary preemption."""
from __future__ import annotations

from admitted_queue_fixture import simulated_admission

from admitted_queue_fixture import simulated_claim_parameters

from admitted_queue_fixture import simulated_capacity

import time

import pytest

from prismabuild import pool
from prismabuild.action_wait import await_ending


class GuestFleet:
    capacity = {"cpu": 8, "mem_gb": 16, "gpu": 1}
    tiers = {"preferred": list(range(8)), "fallback": []}

    def __init__(self, tmp_path, monkeypatch):
        self.patch = monkeypatch
        self.queue = pool.PoolQueue(tmp_path / "queue")
        self.host = "sparky"
        self.use(self.host)
        monkeypatch.setattr(pool.cpu_admission, "action_identity", lambda row: ("shape", False))
        for host, gpu in ((self.host, True), ("cpuhost", False)):
            capacity = dict(self.capacity)
            if not gpu:
                capacity.pop("gpu")
            ledger = self.queue.ledger(host)
            ledger.configure_cpu_tiers(self.tiers)
            ledger.ensure_capacity(capacity)
            self.queue.announce(host=host, tags=[host, "gb10" if gpu else "x86"],
                has_gpu=gpu, capacity=capacity, observed_capacity=capacity,
                cpu_tiers=self.tiers, observed_detail={
                    "observed_unix": time.time(), "load1": 0.,
                    "gpu_power_fraction": .05, "gpu_power_sampled_unix": time.time(),
                    "gpu_capacity_trusted": True, "gpu_sampled_unix": time.time(),
                    "physical_gpus": 1, "foreign_gpu_processes": 0})
        # Exercise the pre-existing overflow path before Spark-first is added.
        assert self.queue.ledger("cpuhost").acquire("f" * 64, {"cpu": 8})

    def use(self, host):
        self.patch.setattr(pool.socket, "gethostname", lambda: host)

    def publish(self, digit, *, gpu=False, cpu=2, priority=0, **fields):
        key = digit * 64
        self.queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
            worker_script="/w.py", resources={"cpu": cpu, "mem_gb": cpu,
                                               **({"gpu": 1} if gpu else {})},
            retry_safe=fields.pop("retry_safe", True),
            max_attempts=fields.pop("max_attempts", 3), priority=priority, **fields)
        return key

    def claim(self, host="sparky", **fields):
        self.use(host)
        return self.queue.claim(tags=[host, "gb10" if host == self.host else "x86"],
            has_gpu=host == self.host, **simulated_admission(self.queue, simulated_capacity(self.capacity)), cpu_tiers=self.tiers, **simulated_claim_parameters(fields, queue=self.queue))

    def stopped(self, key):
        return [row for _, row in self.queue.records.withdrawal_decisions(key)]

    def finish(self, row, status="executed"):
        self.queue.finish(row["action_key"], status=status,
                          detail={"returncode": -15 if status == "withdrawn" else 0},
                          claim_snapshot=row)


@pytest.fixture
def guest_fleet(tmp_path, monkeypatch):
    return GuestFleet(tmp_path, monkeypatch)


@pytest.mark.parametrize("retry_status", ["executed", "failed"])
def test_gpu_eviction_crosses_bands_and_waiter_follows_retry(guest_fleet, retry_status):
    fleet = guest_fleet
    key = fleet.publish("a", cpu=8, priority=20, retry_scope="interruption")
    guest = fleet.claim()
    assert guest["cpu_guest"] is True
    fleet.queue.record_pass(key)
    gpu = fleet.publish("b", gpu=True, cpu=8, priority=-10)
    assert fleet.claim() is None
    decision = fleet.stopped(key)[0]
    assert decision["preempted_by"] == gpu
    retry = pool.record_store.value_or_raise(fleet.queue.records.item(pool.READY, key))
    assert retry["priority"] == 20 and fleet.queue.passes(key) == 1
    assert retry["attempts"] == 1 and retry["max_attempts"] == 3
    assert retry["supersedes_withdrawal"]["published_unix"] == guest["published_unix"]
    assert await_ending(fleet.queue, key, wait_s=0, generation=guest["published_unix"]) is None
    fleet.finish(guest, "withdrawn")
    taken = fleet.claim()
    assert taken["action_key"] == gpu
    gpu_claim = taken
    fleet.queue.ledger("cpuhost").release("f" * 64)
    taken = fleet.claim("cpuhost")
    assert taken["action_key"] == key and not taken.get("cpu_guest")
    fleet.queue.finish(key, status=retry_status,
        detail={"returncode": 0 if retry_status == "executed" else 1}, claim_snapshot=taken)
    ending = await_ending(fleet.queue, key, wait_s=0, generation=guest["published_unix"])
    assert ending[1]["status"] == retry_status and ending[1]["attempts"] == 2
    assert ending[1]["published_unix"] == taken["published_unix"]
    fleet.finish(gpu_claim)


def test_victims_are_lowest_priority_then_youngest_and_converge(guest_fleet):
    fleet = guest_fleet
    guests = []
    for digit, priority in (("a", 2), ("b", -1), ("c", -1)):
        fleet.publish(digit, priority=priority)
        row = fleet.claim()
        guests.append(row)
    gpu = fleet.publish("d", gpu=True, cpu=8, priority=-20)
    for victim in (guests[2], guests[1], guests[0]):
        assert fleet.claim() is None
        assert fleet.stopped(victim["action_key"])[0]["preempted_by"] == gpu
        stopped = sum(bool(fleet.stopped(row["action_key"])) for row in guests)
        assert fleet.claim() is None  # no second handoff before cleanup
        assert sum(bool(fleet.stopped(row["action_key"])) for row in guests) == stopped
        fleet.finish(victim, "withdrawn")
    assert fleet.claim()["action_key"] == gpu


@pytest.mark.parametrize("pin", ["sparky", "gb10"])
def test_pinned_cpu_holders_are_never_guest_victims(guest_fleet, pin):
    fleet = guest_fleet
    key = fleet.publish("a", cpu=8, priority=-30, tags=[pin])
    row = fleet.claim()
    assert row and not row.get("cpu_guest")
    fleet.publish("b", gpu=True, cpu=8, priority=-10)
    assert fleet.claim() is None
    assert not fleet.stopped(key)


def test_foreground_gpu_keeps_ordinary_pinned_background_preemption(guest_fleet):
    fleet = guest_fleet
    key = fleet.publish("a", cpu=8, priority=-10, tags=["sparky"])
    assert not fleet.claim().get("cpu_guest")
    gpu = fleet.publish("b", gpu=True, cpu=8)
    assert fleet.claim() is None
    assert fleet.stopped(key)[0]["preempted_by"] == gpu


@pytest.mark.parametrize("retry_safe,max_attempts", [(False, 1), (True, 1)])
def test_nonrestartable_overflow_is_not_an_evictable_guest(guest_fleet, retry_safe, max_attempts):
    fleet = guest_fleet
    key = fleet.publish("a", cpu=8, retry_safe=retry_safe, max_attempts=max_attempts)
    row = fleet.claim()
    assert row and not row.get("cpu_guest")
    fleet.publish("b", gpu=True, cpu=8)
    assert fleet.claim() is None
    assert not fleet.stopped(key)


@pytest.mark.parametrize("cpu_host_state", ["live", "absent", "passed", "stale"])
def test_two_evictions_end_guest_eligibility_even_with_larger_budget(guest_fleet, cpu_host_state):
    fleet = guest_fleet
    key = fleet.publish("a", cpu=8, max_attempts=10)
    original = fleet.claim()
    guest = original
    for digit in "bc":
        gpu = fleet.publish(digit, gpu=True, cpu=8)
        assert fleet.claim() is None
        assert any(row["preempted_by"] == gpu for row in fleet.stopped(key))
        fleet.finish(guest, "withdrawn")
        row = fleet.claim()
        assert row["action_key"] == gpu
        fleet.finish(row)
        guest = fleet.claim()
    assert guest is None
    assert len(fleet.stopped(key)) == 2
    denial = next(row for row in fleet.queue.latest_denials({key})[key]
                  if row["host"] == "sparky")
    assert denial["reason"] == "guest_retry_budget_exhausted"
    path = fleet.queue.root / pool.WORKERS / "cpuhost.json"
    if cpu_host_state == "live":
        fleet.queue.ledger("cpuhost").release("f" * 64)
        retry = fleet.claim("cpuhost")
    else:
        if cpu_host_state == "absent":
            path.unlink()
        elif cpu_host_state == "passed":
            assert fleet.claim("cpuhost") is None  # full CPU host passes on this generation
            # Mirror the loop's independently published exact-generation verdict.
            from test_pool_cpu_work_yields_gpu_host import _cpu_host_denies
            ready = pool.record_store.read_document(fleet.queue.records.item_path(pool.READY, key))
            _cpu_host_denies(fleet.queue, ready, "reservation_unavailable")
        else:
            offer = pool.record_store.read_document(path)
            offer["announced_unix"] -= pool.OFFER_TIMEOUT_S + 1
            pool.immutable_files.write_json_atomic(path, offer)
        retry = fleet.claim()
    assert retry["attempts"] == 2 and not retry.get("cpu_guest")
    fleet.finish(retry)
    assert await_ending(fleet.queue, key, wait_s=0,
                        generation=original["published_unix"])[1]["status"] == "executed"


def test_pbtest_cpu_shards_seal_restart_permission_and_three_launches(tmp_path, monkeypatch):
    from test_pbtest_seals_its_shard_deadline import _dispatch
    calls = _dispatch(tmp_path, monkeypatch, [])
    for command in calls:
        flags = command[:command.index("--")]
        assert "--retry-safe" in flags
        assert flags[flags.index("--retry-scope") + 1] == "interruption"
        assert flags[flags.index("--max-attempts") + 1] == "3"


@pytest.mark.parametrize("foreign", [False, True])
def test_adaptive_host_pressure_only_evicts_when_pool_drain_resolves_it(guest_fleet, monkeypatch, foreign):
    fleet = guest_fleet
    key = fleet.publish("a")
    guest = fleet.claim()
    assert guest["cpu_guest"]
    gpu = fleet.publish("b", gpu=True, cpu=2)
    # Control the CPU observation boundary, retain the actual drain classifier,
    # admission lock, victim proof and withdrawal. Free tokens already fit.
    def pressured(controller, item, demand, **kwargs):
        controller.last_decision = {"reason": "host_pressure",
            "held_cpus": guest["cpu_allocation"]["preferred"],
            "foreign_cpus": [7] if foreign else []}
        return None

    monkeypatch.setattr(pool.cpu_admission.Controller, "decision", pressured)
    assert fleet.claim(adaptive_cpu=True, gpu_sampler=lambda: {}) is None
    records = fleet.queue.latest_denials({gpu})[gpu]
    denial = next(row for row in records if row["host"] == "sparky")
    assert denial["evidence"]["decision"]["reason"] == "host_pressure"
    assert denial["evidence"]["drain_resolves"] is (not foreign)
    assert bool(fleet.stopped(key)) is (not foreign)


@pytest.mark.parametrize("ordinary_background", [False, True])
def test_a_token_verdict_reserved_elsewhere_does_not_evict_guests(guest_fleet, monkeypatch, ordinary_background):
    from types import SimpleNamespace

    fleet = guest_fleet
    key = fleet.publish("a", cpu=8,
                        **({"priority": -10, "tags": ["sparky"]} if ordinary_background else {}))
    assert bool(fleet.claim().get("cpu_guest")) is (not ordinary_background)
    fleet.publish("b", gpu=True, cpu=8)
    monkeypatch.setattr(fleet.queue, "_observe_drain", lambda *args, **kwargs:
        SimpleNamespace(notes=None, verdict={"eligible": False, "withhold": False,
            "drain_resolves": False, "why": "measurement_reserved_on_other_host"}))
    assert fleet.claim() is None
    assert bool(fleet.stopped(key)) is ordinary_background


def test_pbtest_shaped_worker_failure_is_terminal_on_first_attempt(guest_fleet):
    fleet = guest_fleet
    key = fleet.publish("a", cpu=8, priority=-10, tags=["sparky"],
                        retry_scope="interruption")
    first = fleet.claim()
    assert not first.get("cpu_guest")
    fleet.queue.finish(key, status="failed", detail={"returncode": 1}, claim_snapshot=first)
    ending = await_ending(fleet.queue, key, wait_s=0)
    assert ending is not None and ending[0].parent.name == pool.FAILED
    assert ending[1]["attempts"] == 1
    assert fleet.claim() is None


def test_guests_cannot_collectively_close_token_gap_held_by_non_guest(guest_fleet):
    fleet = guest_fleet
    key = fleet.publish("a", cpu=2)
    assert fleet.claim()["cpu_guest"]
    pinned = fleet.publish("c", cpu=6, tags=["sparky"])
    assert not fleet.claim().get("cpu_guest")
    fleet.publish("b", gpu=True, cpu=8, priority=-10)
    assert fleet.claim() is None
    assert not fleet.stopped(key) and not fleet.stopped(pinned)


def test_exclusive_pressure_requires_every_holder_to_be_an_eligible_guest(guest_fleet):
    fleet = guest_fleet
    key = fleet.publish("a")
    assert fleet.claim()["cpu_guest"]
    fleet.publish("c", tags=["sparky"])
    assert not fleet.claim().get("cpu_guest")
    gpu = fleet.publish("b", gpu=True, priority=-10)
    assert fleet.queue._preempt_background_holder(fleet.queue.ledger(), action_key=gpu,
        demand={"cpu": 2, "gpu": 1}, priority=-10, guest_drain="exclusive") is None
    assert not fleet.stopped(key)


def test_held_cpu_pressure_evicts_only_guest_on_named_cpus(guest_fleet):
    fleet = guest_fleet
    keys = [fleet.publish("a")]
    first = fleet.claim()
    keys.append(fleet.publish("c", priority=-1))
    second = fleet.claim()
    gpu = fleet.publish("b", gpu=True, priority=-10)
    victim = fleet.queue._preempt_background_holder(fleet.queue.ledger(), action_key=gpu,
        demand={"cpu": 2, "gpu": 1}, priority=-10, guest_drain=pool.DRAIN_HELD_CPUS,
        held_cpus=first["cpu_allocation"]["preferred"])
    assert victim == keys[0]
    assert not fleet.stopped(keys[1])
    assert not set(first["cpu_allocation"]["preferred"]).intersection(
        second["cpu_allocation"]["preferred"])


@pytest.mark.parametrize("pending", ["finish_pending", "container_cleanup_pending", "withdrawn"])
def test_pending_non_guest_release_blocks_guest_eviction(guest_fleet, pending):
    fleet = guest_fleet
    key = fleet.publish("a")
    assert fleet.claim()["cpu_guest"]
    pinned = fleet.publish("c", tags=["sparky"])
    row = fleet.claim()
    if pending == "withdrawn":
        fleet.queue.withdraw(pinned, reason="test pending release")
    else:
        row[pending] = {"reason": "test pending release"}
        fleet.queue.records.replace_record_at(fleet.queue.records.item_path(pool.CLAIMED, pinned), row)
    gpu = fleet.publish("b", gpu=True, priority=-10)
    assert fleet.queue._preempt_background_holder(fleet.queue.ledger(), action_key=gpu,
        demand={"cpu": 8, "gpu": 1}, priority=-10, guest_drain="tokens") is None
    assert not fleet.stopped(key)


def test_guest_finishing_between_selection_and_withdrawal_is_not_cancelled(guest_fleet, monkeypatch):
    fleet = guest_fleet
    key = fleet.publish("a", cpu=8)
    assert fleet.claim()["cpu_guest"]
    gpu = fleet.publish("b", gpu=True, cpu=8, priority=-10)
    original = fleet.queue._preempt_selected_holder
    def finishes(holder, record, **kwargs):
        fleet.finish(record)
        return original(holder, record, **kwargs)
    monkeypatch.setattr(fleet.queue, "_preempt_selected_holder", finishes)
    assert fleet.claim() is None
    assert not fleet.stopped(key)
    assert fleet.claim()["action_key"] == gpu
