"""A box at zero free CPU tokens can only be re-entered by borrowing, and a
busy box cannot produce the evidence borrowing requires.

This is the live half of #205, reproduced. Measured on sparky, 2026-09-06
16:30-16:40 UTC: ``reservations/sparky/free/`` held ``0`` cpu tokens, 7
``mem_gb`` and 1 ``gpu``; ``adaptive/cpu-sample.json`` carried
``"observation": {}`` and had not been rewritten for 153 s; eight items sat in
``ready``, one of them passed over 75 times across 1014 s. The offer said
``capacity {'cpu': 20}`` and ``observed_capacity {'cpu': 20}`` throughout.

The chain, all in ``adaptive_cpu``:

* ``available = self.ledger.available().get('cpu', 0)`` is ``0``, so
  ``borrowing = available < declared`` is true for *every* item, however small.
* ``if borrowing: if (not fresh or ...): return None`` -- borrowing is the only
  remaining door, and it is gated on ``fresh``.
* ``fresh`` needs an ``observation`` whose ``sampled_unix`` is within
  ``MAX_SAMPLE_AGE_S`` and whose ``busy_cpus``/``psi_some``/``interval_s`` are
  finite. ``sample()`` only produces one when consecutive readings are
  ``MIN_INTERVAL_S <= elapsed <= MAX_INTERVAL_S`` apart -- 1 to 60 seconds.
* ``sample()`` runs only inside ``decision()``, which runs only inside
  ``claim()``, which runs only when a worker loop polls. A box whose loops are
  all executing actions polls further apart than ``MAX_INTERVAL_S``, so every
  reading is a first reading, every observation is empty, and ``fresh`` is
  permanently false.

So the box refuses everything until a running action ends -- and the reason it
refuses is that it is busy, which is the same reason it cannot measure itself.
The instrument fails in exactly the state it exists to measure.

Two things this is NOT, both checked against the code and recorded because
they were the standing hypotheses:

* Not a wrong ``busy_cpus``. That figure comes from ``/proc/stat`` per-CPU
  jiffies as ``total - idle - iowait`` (``counters()``), so D-state NFS waits
  contribute nothing to it, and neither ``adaptive_cpu`` nor ``pool`` reads
  ``loadavg`` at all. ``load1`` is read only in ``box_capacity``, where its own
  comment says it is "Recorded, never clamped" -- confirmed live, an offer of
  ``observed_capacity {'cpu': 20}`` at ``load1 29.63``. In the observed failure
  there was no ``busy_cpus`` value to be wrong: the observation was ``{}``.
* Not the admission column gating placement. Nothing consults the sample when
  placing; see ``test_pool_admission_staleness_is_not_placement.py``.
"""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from prismabuild import adaptive_cpu, pool  # noqa: E402

TIERS = {"preferred": [0], "fallback": [1]}
CAPACITY = {"cpu": 2, "mem_gb": 8}


@pytest.fixture
def box(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A two-CPU box, a clock we advance, and ``/proc`` we hold still.

    ``counters()`` is replaced rather than ``sample()`` so the interval
    arithmetic under test is the real one: what fails live is the *cadence* of
    the readings, not the readings themselves.
    """

    clock = [1000.0]
    monkeypatch.setattr(adaptive_cpu.time, "time", lambda: clock[0])
    monkeypatch.setattr(adaptive_cpu, "action_identity",
                        lambda item: ("shape", False))

    def counters(cpus):
        # A genuinely idle box: busy jiffies never advance, total does.
        ticks = int(clock[0] * 100)
        return {"cpus": {str(c): [1000, ticks] for c in sorted(cpus)},
                "psi_total": 0, "sampled_unix": clock[0]}

    monkeypatch.setattr(adaptive_cpu, "counters", counters)

    queue = pool.PoolQueue(tmp_path / "queue")
    index = [0]

    def publish(cpu: int = 1, memory: int = 1) -> str:
        index[0] += 1
        key = f"{index[0]:064x}"
        queue.publish(action_key=key, cas_root=str(tmp_path / "cas"),
                      checkout_root=str(tmp_path), worker_script="worker.py",
                      resources={"cpu": cpu, "mem_gb": memory})
        return key

    def claim():
        return queue.claim(capacity=CAPACITY, cpu_tiers=TIERS,
                           adaptive_cpu=True)

    def telemetry(key: str, *, cpu: float = 0.1) -> None:
        adaptive_cpu.write_json(
            queue.ledger().base / "telemetry" / f"{key}.json",
            {"action_key": key, "sampled_unix": clock[0], "cpu_seconds": cpu,
             "wall_seconds": clock[0] - 1000.0, "memory_current_bytes": 100,
             "memory_peak_bytes": 100, "complete": True})

    return queue, clock, publish, claim, telemetry


def _fill_the_box(queue, clock, publish, claim, telemetry) -> str:
    """Admit one action that takes both CPU tokens, then prove it is idle."""

    key = publish(cpu=2)
    clock[0] += 2.0                      # a second reading: the box can sample
    assert claim(), "an idle box admits its first action"
    assert queue.ledger().available().get("cpu", 0) == 0
    for _ in range(2):                   # two readings make a rate
        clock[0] += 2.0
        telemetry(key)
    return key


def test_a_full_box_that_polls_slower_than_the_sampler_never_re_admits(box):
    """The starvation, deterministically.

    The holder is idle and lending would let the small item in. The box polls
    at ``MAX_INTERVAL_S + 1`` -- what a loop that is executing an action does --
    so every reading is a first reading, ``fresh`` is never true, and the item
    is passed over indefinitely.
    """

    queue, clock, publish, claim, telemetry = box
    holder = _fill_the_box(queue, clock, publish, claim, telemetry)
    small = publish(cpu=1)

    for _ in range(6):
        clock[0] += adaptive_cpu.MAX_INTERVAL_S + 1.0
        telemetry(holder)
        assert claim() is None

    assert queue.passes(small) == 6
    assert queue.item_path(pool.READY, small).exists()
    sample = adaptive_cpu.read_json(
        queue.ledger().base / "adaptive" / "cpu-sample.json")
    assert sample.get("observation") == {}, (
        "the observation the borrow gate needs is empty, and stays empty, "
        "because consecutive polls are further apart than MAX_INTERVAL_S")


def test_the_same_box_admits_the_same_item_the_moment_it_can_sample(box):
    """The control: cadence is the only variable.

    Identical box, identical holder, identical item -- polled inside the
    sampler's window instead of outside it. Nothing about the box's real
    idleness changed between the two tests, which is the point: what the gate
    is measuring is whether the loop got to poll, not whether the CPUs are
    free.
    """

    queue, clock, publish, claim, telemetry = box
    holder = _fill_the_box(queue, clock, publish, claim, telemetry)
    small = publish(cpu=1)

    clock[0] += 2.0
    telemetry(holder)
    admitted = claim()

    assert admitted and admitted["action_key"] == small
    assert queue.passes(small) == 0
    assert queue.ledger().held()["cpu"] == 2, "borrowed, never minted"
    assert queue.ledger().capacity()["cpu"] == 2


def test_the_borrow_gate_is_what_refuses_and_free_tokens_are_why(box):
    """Name the branch, so a later reader does not have to re-derive it.

    With a free CPU token the same stale-cadence poll is admitted: the box
    never reaches the borrow gate. Zero free tokens is what turns a missing
    observation from a lost optimisation into a closed door.
    """

    queue, clock, publish, claim, telemetry = box
    small = publish(cpu=1)

    # Nothing held: one CPU token is free, so borrowing is not required.
    assert queue.ledger().available().get("cpu", 0) == 2
    clock[0] += adaptive_cpu.MAX_INTERVAL_S + 1.0
    assert claim() is not None

    # Now the box is full, and an equally small item meets the closed door.
    other = publish(cpu=1)
    clock[0] += 2.0
    assert claim() is not None
    assert queue.ledger().available().get("cpu", 0) == 0
    starved = publish(cpu=1)
    clock[0] += adaptive_cpu.MAX_INTERVAL_S + 1.0
    assert claim() is None
    assert queue.passes(starved) == 1
    assert other and starved
