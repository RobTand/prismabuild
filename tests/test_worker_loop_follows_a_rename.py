"""A renamed box announces under its new name, not the one it booted with.

``host`` was read once, before the poll loop, and held for the life of the
process.  A box renamed under a running loop therefore kept announcing the
name it started with forever.

Measured on 2026-09-06: sparklina rebooted at 14:18, twenty loops started at
14:22, and the hostname fix landed after that.  Those twenty went on offering
``gx10-6b77`` while three later ones offered ``sparklina``, so ``pbstatus``
showed one physical box as two live nodes -- and the phantom's ADMISSION column
read ``cpu: unavailable, gpu: unavailable`` permanently, because nothing
published adaptive counters under a name no box answered to.  An action pinned
to that name would have waited for a worker that could never be admitted.  The
only cure was to retire every pre-rename loop by hand.

Nothing here touches a real queue, a real box or a real name: the hostname is
this test's own and the queue is a stub.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from unittest import mock

WORKER_LOOP = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "worker_loop.py"


def _worker_loop():
    """The real entry point, loaded under a private name."""

    spec = importlib.util.spec_from_file_location("worker_loop_rename",
                                                  WORKER_LOOP)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Ledger:
    def capacity(self) -> dict[str, int]:
        return {"cpu": 2}

    def held(self) -> dict[str, int]:
        return {}

    def retire_free_capacity(self, capacity: dict[str, int]) -> None:
        pass

    def available(self) -> dict[str, int]:
        return {"cpu": 2}


def _announced(module, names: list[str]) -> list[dict]:
    """One poll of the real loop, with ``gethostname`` answering ``names``.

    The first answer is the startup read and the second is the re-read at the
    top of the poll, so a two-element list is exactly the window a rename lands
    in.  The list's last value repeats, because a real box does not change its
    name once per call.
    """

    calls: list[dict] = []

    class _Queue:
        def __init__(self, _root) -> None:
            pass

        def ledger(self) -> _Ledger:
            return _Ledger()

        def announce(self, **kwargs) -> None:
            calls.append(kwargs)

        def serve_once(self, **_kwargs):
            return None

        def placement_census(self) -> dict:
            return {}

    answers = list(names)

    def gethostname() -> str:
        return answers.pop(0) if len(answers) > 1 else answers[0]

    argv = ["worker_loop.py", "--once", "--assume-idle", "--gpu-slots", "0",
            "--poll-s", "0", "--all-cores"]
    with mock.patch.object(module.pool, "PoolQueue", _Queue), \
         mock.patch.object(module, "published_commit", return_value="audit"), \
         mock.patch.object(module, "loaded_runtime_commit", return_value="audit"), \
         mock.patch.object(module.os, "sched_getaffinity",
                           lambda _pid: {2, 3}), \
         mock.patch.object(module.socket, "gethostname", gethostname), \
         mock.patch.object(sys, "argv", argv):
        assert module.main() == 0
    return calls


def test_a_loop_announces_the_name_the_box_has_now() -> None:
    """The rename lands between the startup read and the poll's re-read."""

    module = _worker_loop()

    calls = _announced(module, ["gx10-6b77", "sparklina"])

    assert calls, "the loop announced nothing"
    offer = calls[-1]
    assert offer["host"] == "sparklina", (
        "the loop announced the name it booted with, so this box is two nodes")
    assert "sparklina" in offer["tags"], offer["tags"]
    assert "gx10-6b77" not in offer["tags"], (
        "the old name is still offered, so work pinned to it still matches a "
        "worker that no longer answers to it")


def test_a_box_that_was_not_renamed_announces_exactly_what_it_did() -> None:
    """Re-reading the name must not disturb the ordinary case.

    The tags are a set the scheduler matches against, so an extra or reordered
    entry here is a placement change, not a cosmetic one.
    """

    module = _worker_loop()

    calls = _announced(module, ["sparky"])

    offer = calls[-1]
    assert offer["host"] == "sparky"
    assert offer["tags"] == ["gb10", "sparky", "cpu"], offer["tags"]
