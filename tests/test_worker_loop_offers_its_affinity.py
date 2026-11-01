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

import errno
import importlib.util
from functools import partial
import json
from pathlib import Path
import sys
import tempfile
from unittest import mock

import pytest

WORKER_LOOP = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "worker_loop.py"


from prismabuild import role_session


def _worker_loop():
    """The real entry point, loaded under a private name."""

    spec = importlib.util.spec_from_file_location("worker_loop_affinity",
                                                  WORKER_LOOP)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _CallingThread:
    """The kernel's affinity for the thread that runs the loop."""

    def __init__(self, affinity) -> None:
        self.affinity = affinity
        self.applied: list[set[int]] = []
        #: The mask the thread held each time the loop read its ledger.
        self.serving: list[set[int]] = []

    def get(self, _pid):
        if isinstance(self.affinity, BaseException):
            raise self.affinity
        return set(self.affinity)

    def set(self, _pid, mask) -> None:
        self.affinity = set(mask)
        self.applied.append(set(mask))


class _ReplacedCpuset(_CallingThread):
    """A cpuset replaced mid-run: no inherited CPU is permitted any more."""

    def set(self, pid, mask) -> None:
        if self.applied:
            raise OSError(errno.EINVAL, "Invalid argument")
        super().set(pid, mask)


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
           pinned=None, topology=None) -> dict[str, int]:
    """Run one poll of the real loop and return the capacity it retired to.

    ``affinity`` is the inherited mask, or a ``_CallingThread`` to inspect
    afterwards.  With ``topology`` (a synthetic sysfs CPU root) the loop pins
    itself through the real topology code instead of a stubbed pin.
    """

    seen: dict[str, int] = {}

    class _Queue:
        def __init__(self, _root) -> None:
            self.root = Path(_root)

        def ledger(self) -> _Ledger:
            # The loop reads its ledger in its own thread while it serves;
            # discovery (``ready_items``) runs in a bounded child instead.
            if not isinstance(thread.affinity, BaseException):
                thread.serving.append(set(thread.affinity))
            return _Ledger(seen)

        def claim_hold(self):
            return None

        def announce(self, **_kwargs) -> None:
            pass

        def ready_items(self) -> list:
            # The loop discovers the candidate list before it publishes or
            # admits; this double stands in for an empty queue.
            return []

        def serve_once(self, **_kwargs):
            return None

        def placement_census(self) -> dict:
            return {}

    thread = affinity if isinstance(affinity, _CallingThread) else _CallingThread(affinity)
    topology_code = module.cpu_topology
    pin = (mock.patch.object(topology_code, "pin_to_preferred", return_value=pinned)
           if topology is None else
           mock.patch.multiple(topology_code,
                               pin_to_preferred=partial(topology_code.pin_to_preferred, topology),
                               inherited_tiers=partial(topology_code.inherited_tiers, topology)))

    with tempfile.TemporaryDirectory(prefix="pb-affinity-") as scratch:
        private = Path(scratch)
        for name in ("loaded.json", "published.json"):
            (private / name).write_text(json.dumps({"commit": "audit", "generation": "test"}))
        gate = private / "maintenance.json"
        gate.write_text(json.dumps({"draining": False}))
        with mock.patch.object(module, "SH", private), \
             mock.patch.object(module.pool, "PoolQueue", _Queue), \
             mock.patch.object(role_session, "GENERATION_VERSION", private / "loaded.json"), \
             mock.patch.object(role_session, "RUNTIME_VERSION", private / "published.json"), \
             mock.patch.object(role_session, "MAINTENANCE_GATE", gate), \
             mock.patch.object(role_session, "ROLE_LOCK_ROOT", private / "role-locks"), \
             mock.patch.object(module, "PUBLICATION_LOCK_ROOT", private / "offer-locks"), \
             mock.patch.object(module.container_images, "InventoryCache", partial(
                 module.container_images.InventoryCache, root=private / "images")), \
             pin, \
             mock.patch.object(module.os, "cpu_count", return_value=cpu_count), \
             mock.patch.object(module.os, "sched_getaffinity", thread.get), \
             mock.patch.object(module.os, "sched_setaffinity", thread.set), \
             mock.patch.object(sys, "argv", ["worker_loop.py", *argv]):
            assert module.main() == 0
    return seen


BASE = ["--once", "--assume-idle", "--gpu-slots", "0", "--poll-s", "0"]


def test_all_cores_offers_the_inherited_affinity_not_the_machine(
    capsys: pytest.CaptureFixture,
) -> None:
    """The issue's reproduction: two CPUs inherited, 80 advertised."""

    module = _worker_loop()
    thread = _CallingThread({2, 3})

    offer = _offer(module, [*BASE, "--all-cores"], affinity=thread,
                   cpu_count=80)

    assert offer["cpu"] == 2, (
        "a confined worker advertised CPUs it cannot run an action on")
    assert thread.applied == [], "an unpinned loop set its caller's affinity"


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


def _two_thread_cores(tmp_path) -> Path:
    """Four CPUs as two cores with two threads each: 0 and 1 lead."""

    root = tmp_path / "devices" / "system" / "cpu"
    for cpu in range(4):
        siblings = root / f"cpu{cpu}" / "topology" / "thread_siblings_list"
        siblings.parent.mkdir(parents=True)
        siblings.write_text(f"{cpu % 2},{cpu % 2 + 2}\n")
    (root / "online").write_text("0-3\n")
    return root


def test_the_loop_returns_its_caller_the_affinity_it_inherited(tmp_path) -> None:
    """The pin lasts as long as the loop, not as long as its caller (#2446).

    A GitHub-hosted runner's four vCPUs are two cores with two threads each.
    While it serves, the loop pins itself to one thread per core, and every
    action it launches inherits that pin.  ``worker.main`` runs the loop in its
    caller's thread, so a pin still in place when the loop returned left the
    caller on half of the mask it was given.
    """

    thread = _CallingThread({0, 1, 2, 3})

    offer = _offer(_worker_loop(), BASE, affinity=thread, topology=_two_thread_cores(tmp_path))

    assert thread.serving and all(mask == {0, 1} for mask in thread.serving), (
        "the loop did not serve pinned")
    assert offer["cpu"] == 2
    assert thread.applied == [{0, 1}, {0, 1, 2, 3}], (
        "the loop's pin outlived the loop and narrowed its caller")
    assert thread.affinity == {0, 1, 2, 3}


def test_a_restore_the_kernel_refuses_does_not_replace_the_loops_outcome(tmp_path) -> None:
    """Giving the mask back is best effort, like taking the pin.

    A cpuset replaced while the loop ran can leave none of the inherited CPUs
    permitted, and the kernel then refuses the restore with EINVAL.  The loop
    already finished; its return code, and the one-shot outcome ``worker.main``
    prints after it, must stand.
    """

    thread = _ReplacedCpuset({0, 1, 2, 3})

    offer = _offer(_worker_loop(), BASE, affinity=thread, topology=_two_thread_cores(tmp_path))

    assert offer["cpu"] == 2
    assert thread.applied == [{0, 1}]
    assert thread.affinity == {0, 1}


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
