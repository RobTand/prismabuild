"""The offer is the declaration minus what else is on the box.

Every case here is a shape read off the live fleet on 2026-09-04, because the
defect was found by reading the fleet rather than the code: the ledger was
exactly right about the work the pool scheduled and exactly blind to the rest.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import box_capacity as bc  # noqa: E402

#: sparklina at 01:06 -- six hours into an out-of-pool encode campaign.
SPARKLINA_APPS = [(794915, 1145), (805673, 1543), (831002, 1477), (1242245, 1321)]
SPARKLINA_DECLARED = {"cpu": 10, "gpu": 1, "mem_gb": 40}


def test_the_box_that_went_down_looks_busy() -> None:
    """``offer {'cpu': 10, 'gpu': 1, 'mem_gb': 40}  ledger free 51  held 0``.

    Four GPU processes, load 4.72, and a pool that believed the card was idle.
    One GPU action placed there would have stacked on top of them.
    """

    seen = bc.observe(SPARKLINA_DECLARED, {},
                      gpu_apps=SPARKLINA_APPS, mem_gb=100, load1=4.72)

    assert seen.capacity["gpu"] == 0
    assert seen.foreign["gpu"] == 4
    # The evidence travels with the verdict, so a human reading the offer file
    # can see why it fell without re-running nvidia-smi an hour later.
    assert seen.detail["gpu_compute_apps"] == 4
    assert seen.detail["gpu_used_mib"] == 5486


def test_the_pools_own_containerised_action_is_not_foreign() -> None:
    """sparky at 01:33, running a GPU action the pool itself scheduled.

    Its ``nvidia-smi`` compute app 261917 is a child of
    ``containerd-shim-runc-v2`` in a different session, not a descendant of the
    worker loop that claimed the action.  Attribution by process tree would
    call it foreign and retire capacity against the pool's own work, for as
    long as that work ran.  Attribution by held token gets it right.
    """

    seen = bc.observe({"cpu": 10, "gpu": 2, "mem_gb": 48},
                      {"cpu": 9, "gpu": 1, "mem_gb": 20},
                      gpu_apps=[(261917, 34765)], mem_gb=72, load1=2.95)

    assert seen.foreign == {}
    assert seen.capacity == {"cpu": 10, "gpu": 2, "mem_gb": 48}


def test_a_second_gpu_process_the_pool_did_not_claim_costs_a_slot() -> None:
    seen = bc.observe({"gpu": 2}, {"gpu": 1}, gpu_apps=[(1, 100), (2, 200)])

    assert seen.capacity["gpu"] == 1
    assert seen.foreign["gpu"] == 1


def test_memory_is_bounded_by_what_is_physically_free() -> None:
    seen = bc.observe({"mem_gb": 40}, {}, mem_gb=12, load1=0.0)

    assert seen.capacity["mem_gb"] == 4          # 12 free, 8 kept as margin


def test_memory_the_pool_already_holds_is_not_charged_twice() -> None:
    """The bug in the ``--honest-memory`` flag this replaces.

    An action's bytes are missing from ``MemAvailable`` *and* already reserved
    as tokens.  Clamping to ``MemAvailable - margin`` alone charges the box for
    its own work, so a box doing exactly what the pool told it to do retires
    the capacity it is using.
    """

    seen = bc.observe({"mem_gb": 48}, {"mem_gb": 32}, mem_gb=20, load1=0.0)

    # 32 held + (20 free - 8 margin) = 44, capped by the 48 declared.
    assert seen.capacity["mem_gb"] == 44
    # The old arithmetic: min(48, 20 - 8) = 12, below what is already held.
    assert seen.capacity["mem_gb"] > 12


def test_the_offer_never_rises_above_the_declaration() -> None:
    seen = bc.observe({"mem_gb": 40, "gpu": 1}, {}, mem_gb=900, load1=0.0,
                      gpu_apps=[])

    assert seen.capacity == {"mem_gb": 40, "gpu": 1}
    assert seen.foreign == {}


def test_cores_the_pool_did_not_claim_come_off_the_offer() -> None:
    """The 80-core box at load 371 with four concurrent suites on it."""

    seen = bc.observe({"cpu": 80}, {"cpu": 24}, load1=64.0)

    assert seen.capacity["cpu"] == 40          # 64 runnable, 24 of them ours
    assert seen.foreign["cpu"] == 40


def test_half_a_runnable_task_takes_no_core() -> None:
    seen = bc.observe({"cpu": 10}, {}, load1=0.9)

    assert seen.foreign == {}


def test_an_unreadable_box_is_not_a_busy_box() -> None:
    """``None`` means the reading failed, and a failed reading is no evidence.

    A wedged ``nvidia-smi`` must not silently zero a box, and it must not
    silently declare one idle either -- it must leave the declaration alone.
    """

    seen = bc.observe(SPARKLINA_DECLARED, {},
                      gpu_apps=None, mem_gb=None, load1=None)

    assert seen.capacity == SPARKLINA_DECLARED
    assert seen.foreign == {}
    assert seen.detail == {}


def test_a_kind_nothing_can_read_passes_through() -> None:
    seen = bc.observe({"nvme_gb": 400}, {}, gpu_apps=[], mem_gb=100, load1=0.0)

    assert seen.capacity == {"nvme_gb": 400}


def test_a_box_with_no_gpu_is_not_asked_about_one(monkeypatch) -> None:
    """dl380g10 declares ``gpu: 0``; spawning nvidia-smi there is pure cost."""

    def refuse(**_kwargs):
        raise AssertionError("nvidia-smi must not be run on a gpu-less box")

    monkeypatch.setattr(bc, "gpu_compute_apps", refuse)
    seen = bc.observe({"gpu": 0, "cpu": 80}, {}, mem_gb=100, load1=0.0)

    assert seen.capacity["gpu"] == 0


# -- the window ---------------------------------------------------------

def test_one_bad_reading_does_not_retire_capacity() -> None:
    """The danger the mechanism has to be built around.

    A retire deletes free tokens, and the loop that made it re-mints them at
    its next poll -- but a worker spends the whole of an action inside
    ``serve_once``, up to two hours, and does not poll while it is there.  A
    single unlucky reading taken just before a long action would therefore
    starve the box for the length of that action.
    """

    observer = bc.CapacityObserver(samples=3)
    idle = dict(gpu_apps=[], mem_gb=100, load1=0.0)
    busy = dict(gpu_apps=SPARKLINA_APPS, mem_gb=100, load1=0.0)

    assert observer.offer(SPARKLINA_DECLARED, {}, **idle)["gpu"] == 1
    assert observer.offer(SPARKLINA_DECLARED, {}, **busy)["gpu"] == 1
    assert observer.offer(SPARKLINA_DECLARED, {}, **idle)["gpu"] == 1


def test_sustained_foreign_work_does_retire_capacity() -> None:
    observer = bc.CapacityObserver(samples=3)
    busy = dict(gpu_apps=SPARKLINA_APPS, mem_gb=100, load1=0.0)

    offers = [observer.offer(SPARKLINA_DECLARED, {}, **busy)["gpu"]
              for _ in range(4)]

    # Primed with the declaration, so the first two polls are not decisive.
    assert offers == [1, 1, 0, 0]


def test_the_offer_recovers_on_the_first_reading_that_says_so() -> None:
    """Falling is slow; recovering is immediate.  The asymmetry is the point.

    Work the pool did not schedule ends without telling anyone, and a box that
    waited another three polls to believe it would be idle for no reason.
    """

    observer = bc.CapacityObserver(samples=3)
    busy = dict(gpu_apps=SPARKLINA_APPS, mem_gb=100, load1=0.0)
    for _ in range(4):
        observer.offer(SPARKLINA_DECLARED, {}, **busy)
    assert observer.offer(SPARKLINA_DECLARED, {}, **busy)["gpu"] == 0

    recovered = observer.offer(SPARKLINA_DECLARED, {},
                               gpu_apps=[], mem_gb=100, load1=0.0)

    assert recovered["gpu"] == 1


def test_a_single_sample_window_lets_one_reading_decide() -> None:
    observer = bc.CapacityObserver(samples=1)

    offer = observer.offer(SPARKLINA_DECLARED, {},
                           gpu_apps=SPARKLINA_APPS, mem_gb=100, load1=0.0)

    assert offer["gpu"] == 0


def test_a_window_of_no_samples_is_refused() -> None:
    with pytest.raises(ValueError):
        bc.CapacityObserver(samples=0)


# -- the readers --------------------------------------------------------

def test_the_memory_reading_is_the_one_the_kernel_publishes() -> None:
    value = bc.mem_available_gb()

    assert value is None or 0 <= value < 1_000_000


def test_a_gpu_app_with_no_readable_memory_still_holds_the_device(
    monkeypatch,
) -> None:
    """Losing a pid over an unreadable byte count is the blindness, restored."""

    class _Done:
        returncode = 0
        stdout = "1234, [N/A]\n5678, 2048\n"

    monkeypatch.setattr(bc, "NVIDIA_SMI", Path("/bin/sh"))
    monkeypatch.setattr(bc.subprocess, "run", lambda *a, **k: _Done())

    assert bc.gpu_compute_apps() == [(1234, 0), (5678, 2048)]


def test_a_wedged_nvidia_smi_is_a_missing_reading_not_a_hang(monkeypatch) -> None:
    import subprocess as sp

    def timeout(*_args, **_kwargs):
        raise sp.TimeoutExpired(cmd="nvidia-smi", timeout=5.0)

    monkeypatch.setattr(bc, "NVIDIA_SMI", Path("/bin/sh"))
    monkeypatch.setattr(bc.subprocess, "run", timeout)

    assert bc.gpu_compute_apps() is None
