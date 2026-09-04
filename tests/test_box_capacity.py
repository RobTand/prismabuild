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


def test_the_run_queue_is_recorded_and_never_charged() -> None:
    """A ``cpu`` token is a slot; loadavg counts threads.  Not the same unit.

    Measured on sparky 2026-09-04: load1 22.47 against 5 held cpu tokens, with
    every runnable task a pool-scheduled action (two pytest suites, a refit, an
    export) -- each of them legitimately multithreaded under a one-slot
    reservation.  Subtracting held slots from a thread count charged the box 17
    foreign cpu and would have taken it from ten slots to zero for being busy
    with the pool's own work: the exact double-charge the ledger attribution
    exists to prevent, reappearing because the instrument does not measure the
    quantity the token names.

    So the load is carried for a human to read and nothing more.  gpu and
    mem_gb are clamped because their instruments ARE in the token's units.
    """

    seen = bc.observe({"cpu": 10}, {"cpu": 5}, load1=22.47)

    assert seen.capacity["cpu"] == 10
    assert seen.foreign == {}
    assert seen.detail["load1"] == 22.47


def test_a_genuinely_oversubscribed_box_is_also_not_charged() -> None:
    """The honest limit, stated as a test: this kind is not observed at all.

    The 80-core box at load 371 is really overloaded, and the offer still will
    not fall -- because nothing here can tell that load from four of our own
    suites.  Whoever finds an instrument that attributes a thread to a
    reservation should delete this test and clamp.
    """

    seen = bc.observe({"cpu": 80}, {"cpu": 24}, load1=371.0)

    assert seen.capacity["cpu"] == 80
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


# -- coming back from an action -----------------------------------------

SPARKY_DECLARED = {"cpu": 10, "gpu": 2, "mem_gb": 48}
TWO_FOREIGN = [(1, 1000), (2, 1000)]


def test_a_window_from_before_an_action_does_not_decide_after_it() -> None:
    """The break this file was reopened for.

    A loop polls an idle box, claims a GPU action, and takes no reading for as
    long as the action runs -- ``--timeout-s`` is 7200.  Foreign work arrives
    meanwhile and a sibling loop retires the box.  Because ``offer`` is an
    elementwise maximum, the returning loop's pre-action readings still decide
    its offer for ``samples`` - 1 polls, which are the polls in which it claims
    again, and ``ensure_capacity`` re-mints against them.
    """

    idle = dict(gpu_apps=[], mem_gb=100, load1=0.0)
    busy = dict(gpu_apps=TWO_FOREIGN, mem_gb=100, load1=0.0)

    stale = bc.CapacityObserver(samples=3)
    for _ in range(3):
        stale.offer(SPARKY_DECLARED, {}, **idle)
    # No rejoin: the window is what it was before the action.
    assert stale.offer(SPARKY_DECLARED, {}, **busy)["gpu"] == 2
    assert stale.last is not None and stale.last.foreign["gpu"] == 2

    rejoined = bc.CapacityObserver(samples=3)
    for _ in range(3):
        rejoined.offer(SPARKY_DECLARED, {}, **idle)
    rejoined.rejoin({"cpu": 10, "gpu": 1, "mem_gb": 48})

    assert rejoined.offer(SPARKY_DECLARED, {}, **busy)["gpu"] == 0


def test_the_ledger_caps_the_first_reading_back_rather_than_seeding_it() -> None:
    """A sibling's verdict outranks one lucky reading, for one poll.

    The foreign work on this fleet is a shell script launching one python
    process after another, so a single reading can land in the gap between two
    of them.  The ledger's total is the standing verdict of the loops that kept
    polling, so it caps the first reading back.  It caps and does not seed,
    because part of that total is the token the returning loop has just
    released -- seeding would raise the offer back to it.
    """

    observer = bc.CapacityObserver(samples=3)
    observer.rejoin({"cpu": 10, "gpu": 0, "mem_gb": 48})

    # The gap: nvidia-smi shows nothing, but the box was retired to 0.
    assert observer.offer(SPARKY_DECLARED, {}, gpu_apps=[], mem_gb=100,
                          load1=0.0)["gpu"] == 0
    # And the cap is spent, so a second reading of an idle box restores the
    # offer.  That one poll is the whole price of emptying the window, and it
    # is what makes the cap a cap rather than a deadlock.
    assert observer.offer(SPARKY_DECLARED, {}, gpu_apps=[], mem_gb=100,
                          load1=0.0)["gpu"] == 2


def test_a_loop_back_from_an_action_is_not_padded_again() -> None:
    """The seed pads a start, never a return.

    A start has never read the box, and the ledger total it pads with is the
    verdict of loops that have.  A return has readings and they have expired,
    so there is nothing to pad with that is not either stale or the loop's own
    released tokens.
    """

    observer = bc.CapacityObserver(samples=3, ledger_total=SPARKY_DECLARED)
    observer.offer(SPARKY_DECLARED, {}, gpu_apps=[], mem_gb=100, load1=0.0)
    observer.rejoin()

    offer = observer.offer(SPARKY_DECLARED, {}, gpu_apps=TWO_FOREIGN,
                           mem_gb=100, load1=0.0)

    assert offer["gpu"] == 0


def test_an_empty_ledger_total_caps_nothing() -> None:
    """"The ledger has no total for this host" is not "this host has nothing"."""

    observer = bc.CapacityObserver(samples=3)
    observer.rejoin({})

    assert observer.offer(SPARKY_DECLARED, {}, gpu_apps=[], mem_gb=100,
                          load1=0.0)["gpu"] == 2


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
