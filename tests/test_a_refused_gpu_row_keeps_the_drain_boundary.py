"""A GPU row refused on its holders keeps the boundary they release (#1240).

The 2026-09-27 incident on sparklina, unix times.  ``f0f45723`` (priority
+1, ``{cpu 2, gpu 1, mem_gb 40}``, exclusive, placeable only on sparklina)
waited behind ``4a48c9ce`` (priority -10, progress-governed with a 43200 s
timeout).  From 1790524440, 900 s after that holder's claim, ``holder_bound``
read it ``long``, so every GPU refusal of the +1 row (``exclusive_holder``)
was denied ``adaptive_gpu_refused_starved``: its holders did not drain soon,
so it withheld nothing and let the box run what fit (#924).  The holder
finished at 1790526968.138, mid-pass.  The same pass had already refused the
+1 row while the holder's tokens stood; it went on to the priority -10 row
``c026512d``, whose GPU decision found no holder and admitted it at
1790526968.228, 90 ms later.  The +1 row's next evaluation met ``c026512d``
on the device.

A refusal that let the box fill beside a holder must not also give the
holder's boundary away: the refused row keeps the room its claim would take,
and that room binds every row behind it once the free tokens fit it -- the
rows that demand a GPU with the rest.  While the holder holds, the room does
not fit, so it binds nothing and the box fills as #924 and #1085 allow.
CPU-only rows that fit beside the room still run (#1169).  The busy path has
the same boundary: a room read while the holder still held the GPU token must
not be dropped for not fitting yet (#1230 kept it only when it fit then).

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
HOST = "sparklina"


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


def _release_when(monkeypatch, queue: pool.PoolQueue, key: str, reason_prefix: str,
                  holder: str) -> list[str]:
    """Release ``holder``'s tokens the moment ``key`` is denied ``reason_prefix``.

    That is the incident's order inside one pass: the row is refused while
    the holder still holds, the holder finishes, and the pass goes on to the
    rows behind the refused one.  Returns the reasons seen for ``key``.
    """

    seen: list[str] = []
    original = queue.record_denial

    def record(item, reason, evidence=None):
        original(item, reason, evidence)
        if item.get("action_key") == key:
            seen.append(reason)
            if reason.startswith(reason_prefix) and holder in queue.ledger().held_keys():
                queue.ledger().release(holder)

    monkeypatch.setattr(queue, "record_denial", record)
    return seen


# -- a sparklina-shaped box: one GB10, 20 CPUs, adaptive CPU and GPU admission --


@pytest.fixture()
def box(tmp_path: Path, monkeypatch):
    """The adaptive admission path with a broker sample the test controls.

    Copied from ``test_a_gpu_refused_row_is_not_overtaken_by_gpu_rows``:
    every row is an ordinary shared generation row unless ``contracts``
    names it exclusive.
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
        clock[0] += 0.001
        key = _key(name)
        queue.publish(action_key=key, cas_root=str(tmp_path / "cas"),
                      checkout_root=str(tmp_path), worker_script="worker.py",
                      resources=resources, needs_gpu=bool(resources.get("gpu")), **kw)
        return key

    def tick(seconds: float = 2.0) -> None:
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
                                   adaptive_cpu=True, has_gpu=True,
                                   tags=[HOST, "gb10"]))

    return queue, clock, contracts, publish, tick, claim


#: The G2 holder and the G2 row behind the LDLQ row, as the pool charged them.
G2 = {"cpu": 6, "gpu": 1, "mem_gb": 69}
#: The LDLQ A/B/A/B timing row.
LDLQ = {"cpu": 2, "gpu": 1, "mem_gb": 40}
#: A CPU-only row that fits beside the LDLQ row's room.
SHARD = {"cpu": 2, "mem_gb": 4}


@pytest.mark.parametrize("holder_age,verdict", [
    (pool.WITHHOLD_CEILING_S + 1.0, "adaptive_gpu_refused_starved"),
    (10.0, "adaptive_gpu_refused_withholding"),
], ids=["long-holder-starved", "transient-holder-withholding"])
def test_the_refused_exclusive_row_keeps_the_boundary_its_holder_releases(
    box, monkeypatch, holder_age: float, verdict: str,
) -> None:
    """The incident: +1 exclusive row refused, holder finishes, -10 GPU row next."""

    queue, clock, contracts, publish, tick, claim = box
    holder = publish("4a48-g2-holder", G2, priority=-10, tags=["gb10"])
    assert claim() == holder
    holder_claimed = clock[0]
    ldlq = publish("f0f4-ldlq-abab", LDLQ, priority=1, tags=[HOST])
    contracts[ldlq] = ("shape", True)
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(ldlq)
    g2 = publish("c026-next-g2", G2, priority=-10, tags=["gb10"])
    shard = publish("cpu-shard", SHARD, priority=-10)

    clock[0] = holder_claimed + holder_age
    tick()
    seen = _release_when(monkeypatch, queue, ldlq, "adaptive_gpu_refused", holder)
    claimed = claim()
    assert seen and seen[0] == verdict, seen
    assert holder not in queue.ledger().held_keys(), "the fixture did not release"
    assert claimed != g2, (
        "a priority -10 GPU row took the boundary of the GPU the refused +1 "
        "row waits for (#1240)")
    assert claimed == shard, "CPU-only work that fits beside the room stopped filling"
    refused = _denial(queue, ldlq)
    assert refused["reason"] == verdict
    assert refused["evidence"]["decision"]["reason"] == "exclusive_holder"
    assert refused["evidence"]["gpu_room_kept"] == LDLQ
    assert refused["evidence"]["gpu_room_binds_all"] is True
    waited = _denial(queue, g2)
    if verdict.endswith("_withholding"):
        # The GPU-kind withhold already held it back unevaluated (#1085).
        assert waited["reason"] == "deferred_behind_withheld_row"
    else:
        assert waited["reason"] == "deferred_for_ready_gpu_row", waited
        assert waited["evidence"]["gpu_row"] == ldlq[:12]
        assert waited["evidence"]["room"] == LDLQ
        assert waited["evidence"]["kept_for"] == verdict
    assert queue.item_path(pool.READY, g2).exists()

    # The next pass decides the +1 row first, on the released GPU.
    tick()
    assert claim() == ldlq


def test_a_room_read_while_the_holder_held_binds_once_it_releases(
    tmp_path: Path, monkeypatch,
) -> None:
    """The busy path (#1217, #1230): the holder releases after the room is read.

    #1230 released the holder inside the busy row's lock call, before the
    room was read.  Released after it, the room was read against a held GPU
    token, did not fit, and was dropped; the lower GPU row took the boundary.
    """

    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    capacity = {"cpu": 20, "gpu": 1, "mem_gb": 104}
    ledger.ensure_capacity(capacity)
    holder, busy_row, lower = "0" * 64, "a" * 64, "b" * 64
    assert ledger.acquire(holder, dict(G2))
    queue.publish(action_key=busy_row, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", priority=1, resources=dict(LDLQ))
    queue.publish(action_key=lower, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", priority=-10, resources=dict(G2))
    original = queue._transition_locked

    def contested(key, **kwargs):
        return _Unheld() if key == busy_row else original(key, **kwargs)

    monkeypatch.setattr(queue, "_transition_locked", contested)
    _release_when(monkeypatch, queue, busy_row, "transition_busy", holder)
    claimed = queue.claim(capacity=capacity)
    assert claimed is None, (
        f"claimed {claimed and claimed['action_key'][:4]} past the busy +1 row "
        "whose holder released after its room was read")
    busy = _denial(queue, busy_row)
    assert busy["reason"] == "transition_busy"
    assert busy["evidence"]["gpu_room_kept"] == LDLQ
    assert busy["evidence"]["gpu_room_binds_all"] is True
    deferred = _denial(queue, lower)
    assert deferred["reason"] == "deferred_for_ready_gpu_row"
    assert deferred["evidence"]["gpu_row"] == busy_row[:12]
    assert deferred["evidence"]["kept_for"] == "transition_busy"
    monkeypatch.setattr(queue, "_transition_locked", original)
    assert queue.claim(capacity=capacity)["action_key"] == busy_row


def test_a_token_shortage_past_its_ceiling_keeps_the_boundary(
    tmp_path: Path, monkeypatch,
) -> None:
    """The ledger path: a GPU row short of tokens, overtaken, keeps its room.

    Without adaptive admission the +1 row is refused on the token shortage.
    Its holder has no readable claim and the row's own clock is past
    ``WITHHOLD_CEILING_S``, so it withholds nothing (``_past_ceiling``); the
    holder releases before the -10 GPU row behind it is decided.
    """

    now = [T0]
    monkeypatch.setattr(pool, "_now", lambda: now[0])
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    capacity = {"cpu": 20, "gpu": 1, "mem_gb": 104}
    ledger.ensure_capacity(capacity)
    holder, row, lower = "0" * 64, "a" * 64, "b" * 64
    assert ledger.acquire(holder, dict(G2))
    queue.publish(action_key=row, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", priority=1, resources=dict(LDLQ))
    queue.publish(action_key=lower, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", priority=-10, resources=dict(G2))
    for _ in range(pool.STARVATION_FLOOR):
        queue.record_pass(row)
    now[0] += pool.WITHHOLD_CEILING_S + 1.0
    _release_when(monkeypatch, queue, row, "reservation_unavailable", holder)
    claimed = queue.claim(capacity=capacity)
    assert claimed is None, (
        f"claimed {claimed and claimed['action_key'][:4]} into the boundary "
        "the +1 row was refused for")
    refused = _denial(queue, row)
    assert refused["reason"] == "reservation_unavailable_past_ceiling", refused
    assert refused["evidence"]["gpu_room_kept"] == LDLQ
    deferred = _denial(queue, lower)
    assert deferred["reason"] == "deferred_for_ready_gpu_row"
    assert deferred["evidence"]["kept_for"] == "reservation_unavailable_past_ceiling"
    assert queue.claim(capacity=capacity)["action_key"] == row


def test_a_refused_room_binds_nothing_while_its_holder_still_holds(box) -> None:
    """#924 and #1085 kept: behind a long holder the box still fills.

    The refused row's room does not fit the free tokens while the holder
    holds the GPU, so a CPU row claims beside the holder as before.
    """

    queue, clock, contracts, publish, tick, claim = box
    holder = publish("g2-holder", G2, priority=-10, tags=["gb10"])
    assert claim() == holder
    holder_claimed = clock[0]
    ldlq = publish("ldlq", LDLQ, priority=1, tags=[HOST])
    contracts[ldlq] = ("shape", True)
    for _ in range(pool.STARVATION_FLOOR - 1):
        queue.record_pass(ldlq)
    shard = publish("big-cpu-shard", {"cpu": 10, "mem_gb": 40}, priority=-10)
    clock[0] = holder_claimed + pool.WITHHOLD_CEILING_S + 1.0
    tick()
    assert claim() == shard, "a room that does not fit yet held CPU work back"
    assert _denial(queue, ldlq)["reason"] == "adaptive_gpu_refused_starved"
