"""A big GPU row refused for the GPU is not overtaken by later GPU rows (#1085).

The 2026-09-24 incident: the Stage A split round-1 prep ``dbd83885e746``
(``cpu 10, gpu 1, mem_gb 101``) waited 465 s over 249 passes on sparky while
smaller GPU test rows (``cpu 4, gpu 1, mem_gb 16``) at the same priority kept
taking the GPU.  The #924 withhold, the pool's liveness bound for large work,
never fired on the two paths that row took:

1.  An adaptive GPU refusal of a row that is not a measurement returned no
    withhold verdict, so the scan went on and admitted the next GPU row
    behind it.  That row shared the GPU with the holder already there, and
    the big row was refused again on the next pass.
2.  A ``transition_busy`` skip, where another loop held the row's per-key
    transition lock, left nothing withheld for the rest of the pass.  Every
    host took that lock for every ready row, including dl380g10, which can
    never place a GPU row, so the collision was routine.

These fixtures pin the fix: the GPU refusal withholds GPU rows only, so
CPU-only rows still fill the box; a live withhold on this host carries
across a busy lock for one pass; a box that cannot place a row does not take
its lock; and every withhold stays bounded.

Nothing here touches the live queue, a real pool or a real device.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import sys
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from prismabuild import adaptive_cpu, adaptive_gpu, pool  # noqa: E402

T0 = 2_000_000.0
GIB = adaptive_gpu.GIB


def _key(seed: str) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()


def _denial(q: pool.PoolQueue, key: str) -> dict:
    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return next(value for value in records.values() if value["action_key"] == key)


def _key_of(item):
    return None if item is None else item["action_key"]


class _Unheld:
    """What ``_transition_locked`` yields while another loop holds the key."""

    def __enter__(self):
        return False

    def __exit__(self, *args):
        return False


def _contest(monkeypatch, queue: pool.PoolQueue, key: str) -> None:
    """Hold ``key``'s transition lock elsewhere for every later pass."""

    original = queue._transition_locked

    def contested(action_key, **kwargs):
        if action_key == key:
            return _Unheld()
        return original(action_key, **kwargs)

    monkeypatch.setattr(queue, "_transition_locked", contested)


# -- a sparky-shaped box: one GB10, 20 CPUs, adaptive CPU and GPU admission --


@pytest.fixture()
def box(tmp_path: Path, monkeypatch):
    """Sparky's admission path with a broker sample the test controls.

    The GPU holder and the GPU test rows are ordinary shared generation rows
    (``gpu_exclusive: false``), so the GPU controller lets a later one share
    the device with a holder once a probe is authorized.  The big row's
    contract is set per test: exclusive, as a row that declares nothing is,
    or shared with no shape history, which is refused a probe.
    """

    clock = [T0]
    monkeypatch.setattr(time, "time", lambda: clock[0])
    monkeypatch.setattr(adaptive_cpu, "action_identity", lambda item: ("shape", False))
    contracts: dict[str, tuple[object, bool]] = {}

    def contract(item, demand):
        shape, exclusive = contracts.get(str(item["action_key"]), ("shape", False))
        return shape, False, exclusive, int(demand.get("mem_gb", 0)) * GIB

    monkeypatch.setattr(adaptive_gpu, "action_contract", contract)
    monkeypatch.setattr(adaptive_cpu.Controller, "sample", lambda self: {
        "sampled_unix": clock[0], "busy_cpus": 0., "psi_some": 0.,
        "cpu_count": 20, "interval_s": 1.})
    sample = {"schema": "prismabuild.gpu_capacity.v1", "sample_id": str(T0),
              "sampled_unix": T0, "complete": True, "attributed": True,
              "devices": [{"uuid": "GPU-1", "name": "NVIDIA GB10", "power_w": 15.,
                           "power_limit_w": None, "power_reference_w": 140.,
                           "power_reference_scope": "soc_tdp",
                           "memory_domain": "shared_system", "limited": False}],
              "host_total_bytes": 128 * GIB, "host_available_bytes": 120 * GIB,
              "memory_pressure_some": 0., "memory_pressure_full": 0.,
              "cpu_pressure_some": 0., "foreign_processes": [], "jobs": []}
    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda self: dict(sample))
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    capacity = {"cpu": 20, "gpu": 1, "mem_gb": 120}
    tiers = {"preferred": list(range(20)), "fallback": []}

    def publish(name: str, resources: dict[str, int], **kw) -> str:
        # A distinct publish time per row, so the ready order is the order
        # the test publishes in rather than a digest tie-break.
        clock[0] += 0.001
        key = _key(name)
        queue.publish(action_key=key, cas_root=str(tmp_path / "cas"),
                      checkout_root=str(tmp_path), worker_script="worker.py",
                      resources=resources, needs_gpu=bool(resources.get("gpu")), **kw)
        return key

    def tick(seconds: float = 2.0) -> None:
        """A new broker sample, with fresh telemetry for every holder."""

        clock[0] += seconds
        sample.update(sampled_unix=clock[0], sample_id=str(clock[0]))
        sample["jobs"] = []
        for key in queue.ledger().held_keys():
            record = {"action_key": key, "nonce": key + "-attempt",
                      "scope_unit": key + "-scope", "sampled_unix": clock[0],
                      "cpu_seconds": .01 * (clock[0] - T0),
                      "wall_seconds": clock[0] - T0, "complete": True}
            adaptive_cpu.write_json(
                adaptive_cpu.local_telemetry_path(queue.ledger().base, key), record)
            sample["jobs"].append({"action_key": key, "nonce": record["nonce"],
                                   "scope_id": record["scope_unit"], "complete": True})

    def claim():
        return _key_of(queue.claim(capacity=capacity, cpu_tiers=tiers,
                                   adaptive_cpu=True, has_gpu=True))

    return queue, clock, contracts, publish, tick, claim


#: The big row's contract, and the refusal the GPU controller gives it while a
#: GPU borrower holds the device.
BIG_ROW_CONTRACTS = pytest.mark.parametrize("contract,refusal", [
    (("shape", True), "exclusive_holder"),
    ((None, False), "sharing_probe_not_authorized"),
], ids=["exclusive", "shared-without-shape"])


def _big_row_behind_a_gpu_borrower(box, contract):
    """A GPU borrower holds the device; the big row is past the floor.

    Behind the big row wait a GPU test row, which the controller would let
    share the device with the borrower, and a CPU-only test row.
    """

    queue, clock, contracts, publish, tick, claim = box
    holder = publish("gpu-borrower", {"cpu": 2, "gpu": 1, "mem_gb": 16})
    assert claim() == holder
    holder_claimed = clock[0]
    big = publish("stage-a-quantum", {"cpu": 10, "gpu": 1, "mem_gb": 101})
    contracts[big] = contract
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(big)
    gpu_row = publish("gpu-test-shard", {"cpu": 4, "gpu": 1, "mem_gb": 16})
    cpu_row = publish("cpu-test-shard", {"cpu": 2, "mem_gb": 4})
    tick()
    return holder, holder_claimed, big, gpu_row, cpu_row


# -- path 1: an adaptive GPU refusal withholds GPU rows only -----------------


@BIG_ROW_CONTRACTS
def test_a_gpu_refused_big_row_withholds_later_gpu_rows_but_not_cpu_rows(
    box, contract, refusal: str,
) -> None:
    queue, clock, contracts, publish, tick, claim = box
    holder, _, big, gpu_row, cpu_row = _big_row_behind_a_gpu_borrower(box, contract)

    first = claim()
    assert first != gpu_row, (
        "a later GPU row took the GPU the refused big row waits for (#1085 path 1)")
    assert first == cpu_row, "CPU-only work stopped filling the box"
    denial = _denial(queue, big)
    assert denial["reason"] == "adaptive_gpu_refused_withholding"
    assert denial["evidence"]["decision"]["reason"] == refusal
    withhold = denial["evidence"]["withhold"]
    assert withhold["mode"] == "gpu" and withhold["why"] == "drains_soon"
    assert denial["evidence"]["withheld_kinds"] == ["gpu"]
    # The row that waited says why, in the same host-local record.
    waited = _denial(queue, gpu_row)
    assert waited["reason"] == "deferred_behind_withheld_row"
    assert waited["evidence"]["withheld_for"] == big
    assert waited["evidence"]["withheld_kinds"] == ["gpu"]
    assert queue.item_path(pool.READY, gpu_row).exists()
    assert queue.passes(gpu_row) == 0, "a deferred row earned a pass it was not refused"

    tick()
    assert claim() is None
    # The borrower leaves; the big row is next.
    queue.finish(holder, status="executed", detail={})
    tick()
    assert claim() == big


@BIG_ROW_CONTRACTS
def test_the_gpu_withhold_is_bounded_by_its_holders(box, contract, refusal: str) -> None:
    """Past ``WITHHOLD_CEILING_S`` the GPU row is admitted again.

    Only rows ahead of the big row may refill the GPU during its withhold, and
    a GPU row behind it never does.  What ends the veto here is the holder's
    own age: an unsealed holder past the ceiling no longer drains soon, the
    big row keeps its passes and its place, and it is reported starved.
    """

    queue, clock, contracts, publish, tick, claim = box
    holder, holder_claimed, big, gpu_row, cpu_row = _big_row_behind_a_gpu_borrower(
        box, contract)
    assert claim() == cpu_row
    assert claim() is None, "the GPU row overtook the big row inside the ceiling"

    clock[0] = holder_claimed + pool.WITHHOLD_CEILING_S + 1
    tick()
    claim()                  # a pass that re-reads the device after the gap
    tick()
    assert claim() == gpu_row, "the GPU withhold outlived its holder's transient bound"
    denial = _denial(queue, big)
    assert denial["reason"] == "adaptive_gpu_refused_starved"
    assert denial["evidence"]["starved"]["why"] == "holder_does_not_drain_soon"
    [named] = denial["evidence"]["starved"]["holders"]
    assert named["action_key"] == holder[:12] and named["bound"] == "long"
    assert queue.item_path(pool.READY, big).exists()


def test_a_gpu_withhold_carries_its_kind_across_a_busy_lock(box, monkeypatch) -> None:
    """Paths 1 and 2 together: the carried withhold is still GPU-only."""

    queue, clock, contracts, publish, tick, claim = box
    holder, _, big, gpu_row, cpu_row = _big_row_behind_a_gpu_borrower(
        box, ("shape", True))
    assert claim() == cpu_row
    _contest(monkeypatch, queue, big)
    second_cpu_row = publish("cpu-test-shard-2", {"cpu": 2, "mem_gb": 4})
    tick()
    assert claim() == second_cpu_row, (
        "the GPU row overtook the big row while another loop held its lock")
    busy = _denial(queue, big)
    assert busy["reason"] == "transition_busy"
    assert busy["evidence"]["withhold_carried"]["mode"] == "gpu"
    assert _denial(queue, gpu_row)["reason"] == "deferred_behind_withheld_row"
    assert queue.item_path(pool.READY, gpu_row).exists()


# -- path 2: a busy lock does not end a live withhold -------------------------


@pytest.fixture()
def ledger_clock(monkeypatch):
    now = [T0]
    monkeypatch.setattr(pool, "_now", lambda: now[0])
    return now


def _withholding_gpu_action(tmp_path: Path, clock):
    """#924's GPU-first withhold, which main already produces.

    A four-CPU box with a free GPU runs a two-CPU shard.  The GPU action needs
    all four CPUs, so it withholds the box on its first denial; the smaller
    GPU row behind it fits beside the shard.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    capacity = {"cpu": 4, "gpu": 1}

    def publish(name, resources):
        clock[0] += 0.001
        key = _key(name)
        queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
                      worker_script="/w.py", resources=resources)
        return key

    def claim():
        return _key_of(queue.claim(capacity=capacity))

    shard = publish("shard", {"cpu": 2})
    assert claim() == shard
    big = publish("gpu-action", {"cpu": 4, "gpu": 1})
    small = publish("small-gpu-row", {"cpu": 1, "gpu": 1})
    assert claim() is None
    denial = _denial(queue, big)
    assert denial["reason"] == "reservation_unavailable_withholding"
    return queue, claim, big, small


def test_a_busy_lock_keeps_this_hosts_live_withhold_for_the_pass(
    tmp_path: Path, ledger_clock, monkeypatch,
) -> None:
    queue, claim, big, small = _withholding_gpu_action(tmp_path, ledger_clock)
    _contest(monkeypatch, queue, big)
    ledger_clock[0] += 10
    assert claim() is None, (
        "a row overtook the withholding row while another loop held its lock "
        "(#1085 path 2)")
    busy = _denial(queue, big)
    assert busy["reason"] == "transition_busy"
    carried = busy["evidence"]["withhold_carried"]
    assert carried["reason"] == "reservation_unavailable_withholding"
    assert queue.item_path(pool.READY, small).exists()
    # A second busy pass in a row still reads the withhold it carried.
    ledger_clock[0] += 10
    assert claim() is None, "a second busy pass forgot the withhold"


def test_a_carried_withhold_lapses_with_its_episode(
    tmp_path: Path, ledger_clock, monkeypatch,
) -> None:
    """The carry is bounded by the withhold's own episode, not renewed by it."""

    queue, claim, big, small = _withholding_gpu_action(tmp_path, ledger_clock)
    epoch = ledger_clock[0]
    _contest(monkeypatch, queue, big)
    ledger_clock[0] += 10
    assert claim() is None, "a busy lock ended a live withhold"
    ledger_clock[0] = epoch + pool.WITHHOLD_CEILING_S + 1
    assert claim() == small, "a carried withhold outlived its episode's ceiling"


# -- placement before the lock -------------------------------------------------


@pytest.mark.parametrize("row,worker", [
    ({"tags": ["gb10"]}, {"tags": ["x86"], "has_gpu": False}),
    ({"needs_gpu": True}, {"tags": [], "has_gpu": False}),
], ids=["tags", "gpu"])
def test_a_box_that_cannot_place_a_row_does_not_take_its_lock(
    tmp_path: Path, monkeypatch, row, worker,
) -> None:
    """dl380g10 recorded ``placement_mismatch`` for the row on every pass.

    It took the row's transition lock to say so, and that is what the host
    that could place the row met as ``transition_busy``.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    key = _key("gpu-row")
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", resources={"cpu": 1, "gpu": 1}, **row)
    taken: list[str] = []
    original = queue._transition_locked

    def recording(action_key, **kwargs):
        taken.append(str(action_key))
        return original(action_key, **kwargs)

    monkeypatch.setattr(queue, "_transition_locked", recording)
    assert queue.claim(**worker) is None
    assert key not in taken, (
        "a box that can never place the row took its transition lock (#1085)")
    assert _denial(queue, key)["reason"] == "placement_mismatch"
