"""A worker offers the CPUs it may run on, not the ones the box has (issue #88).

``--all-cores`` turns off the topology pin, and the automatic CPU capacity
then fell back to ``os.cpu_count()``: the machine's logical CPU count, whether
or not an outer ``taskset`` or cpuset confined this loop and every action it
launches.  The checked-in dl380g10 configuration passes ``--all-cores``, so a
confined worker would admit 80 cpu tokens of demand against however few CPUs
it actually held, which is the promise-the-box-cannot-keep that the capacity
drift was.

The topology pinning path always preserved an outer restriction, since it pins
inside the affinity it inherited.  Turning the pin off must not throw that
away.  ``--cpu-slots`` can cap either path but cannot exceed its affinity.

Nothing here changes an affinity, a queue, or a process: the host readings are
synthetic and the queue is this test's own.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from unittest import mock

import pytest

WORKER_LOOP = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "worker_loop.py"


def _worker_loop():
    """The real entry point, loaded under a private name."""

    spec = importlib.util.spec_from_file_location("worker_loop_affinity",
                                                  WORKER_LOOP)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Ledger:
    def __init__(self, seen: dict[str, int]) -> None:
        self._seen = seen

    def capacity(self) -> dict[str, int]:
        return dict(self._seen)

    def held(self) -> dict[str, int]:
        return {}

    def retire_free_capacity(self, capacity: dict[str, int]) -> None:
        self._seen.clear()
        self._seen.update(capacity)

    def available(self) -> dict[str, int]:
        return dict(self._seen)


def _offer(module, argv: list[str], *, affinity, cpu_count: int = 80,
           pinned=None) -> dict[str, int]:
    """Run one poll of the real loop and return the capacity it retired to."""

    seen: dict[str, int] = {}

    class _Queue:
        def __init__(self, _root) -> None:
            pass

        def ledger(self) -> _Ledger:
            return _Ledger(seen)

        def announce(self, **_kwargs) -> None:
            pass

        def serve_once(self, **_kwargs):
            return None

        def placement_census(self) -> dict:
            return {}

    def sched_getaffinity(_pid):
        if isinstance(affinity, BaseException):
            raise affinity
        return set(affinity)

    with mock.patch.object(module.pool, "PoolQueue", _Queue), \
         mock.patch.object(module, "published_commit", return_value="audit"), \
         mock.patch.object(module, "loaded_runtime_commit", return_value="audit"), \
         mock.patch.object(module.cpu_topology, "pin_to_preferred",
                           return_value=pinned), \
         mock.patch.object(module.os, "cpu_count", return_value=cpu_count), \
         mock.patch.object(module.os, "sched_getaffinity", sched_getaffinity), \
         mock.patch.object(sys, "argv", ["worker_loop.py", *argv]):
        assert module.main() == 0
    return seen


BASE = ["--once", "--assume-idle", "--gpu-slots", "0", "--poll-s", "0"]


def test_all_cores_offers_the_inherited_affinity_not_the_machine(
    capsys: pytest.CaptureFixture,
) -> None:
    """The issue's reproduction: two CPUs inherited, 80 advertised."""

    module = _worker_loop()

    offer = _offer(module, [*BASE, "--all-cores"], affinity={2, 3},
                   cpu_count=80)

    assert offer["cpu"] == 2, (
        "a confined worker advertised CPUs it cannot run an action on")


def test_explicit_slots_cannot_advertise_more_cpus_than_the_affinity() -> None:
    """A slot now names one CPU, so overdeclaring the mask must refuse."""
    module = _worker_loop()
    with pytest.raises(SystemExit) as raised:
        _offer(module, [*BASE, "--all-cores", "--cpu-slots", "8"],
               affinity={2, 3}, cpu_count=80)
    assert raised.value.code == 2


def test_explicit_slots_can_cap_the_inherited_affinity() -> None:
    module = _worker_loop()
    offer = _offer(module, [*BASE, "--all-cores", "--cpu-slots", "1"],
                   affinity={2, 3}, cpu_count=80)
    assert offer["cpu"] == 1


def test_the_pinned_path_still_offers_what_it_pinned() -> None:
    """Without ``--all-cores`` the offer is the preferred set, unchanged."""

    module = _worker_loop()

    offer = _offer(module, BASE, affinity=set(range(80)), cpu_count=80,
                   pinned={5, 6, 7})

    assert offer["cpu"] == 3


def test_a_platform_with_no_affinity_call_falls_back_to_the_machine_count(
) -> None:
    """The last resort is the old answer, not a crash."""

    module = _worker_loop()

    offer = _offer(module, [*BASE, "--all-cores"],
                   affinity=OSError("no affinity"), cpu_count=12)

    assert offer["cpu"] == 12


def test_the_helper_reports_the_affinity_width_directly() -> None:
    module = _worker_loop()

    with mock.patch.object(module.os, "sched_getaffinity",
                           return_value={0, 1, 2, 40}):
        assert module.inherited_cpus() == 4
