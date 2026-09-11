"""A parked loop records that it parked, so a drain is evidence and not a clock.

``maintenance_requested`` stops the loop admitting, and then the loop sleeps
without leaving a trace.  From off the box, a drain that has taken effect and
a drain that was merely requested look the same, so anything waiting on a
drained fleet has to argue from elapsed time.  #458 needs the drain proven per
host before the runtime symlink moves, and the loop is where the proof starts.

The park marker is the whole of the loop's contribution: one file per process
per drain, named for the ``changed_unix`` the broker stamped on the gate.  The
loop learns no epoch vocabulary and reads nothing new.

Two properties are load bearing here and are asserted separately.  The first is
that splitting the gate read out of the verdict did not move the failure
polarity: an unreadable gate has to keep meaning "draining", because the other
reading admits work on a parse error.  The second is that the marker is best
effort, because the directory it lives in is created by a root actor the loop
does not control, and a loop that cannot write must still park.

Nothing here touches ``/run``, a real broker or a real queue.
"""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path

import pytest

WORKER_LOOP = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "worker_loop.py"


def _worker_loop(gate: Path):
    """The real entry point, loaded with its gate pointed somewhere writable.

    The path is read at import, so it is set before the module is executed
    rather than patched afterwards: that is the way a supervised loop child
    receives it too.
    """

    previous = os.environ.get("PRISMABUILD_MAINTENANCE_GATE")
    os.environ["PRISMABUILD_MAINTENANCE_GATE"] = str(gate)
    try:
        spec = importlib.util.spec_from_file_location("worker_loop_parked",
                                                      WORKER_LOOP)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        if previous is None:
            os.environ.pop("PRISMABUILD_MAINTENANCE_GATE", None)
        else:
            os.environ["PRISMABUILD_MAINTENANCE_GATE"] = previous
    return module


#: A boot has not initialized admission until an explicit open gate exists.
GATES = [
    ("absent", None, True, True),
    ("draining", {"draining": True, "changed_unix": 1788947072.5}, False, True),
    ("open", {"draining": False, "changed_unix": 1788947072.5}, False, False),
    ("draining is a string", {"draining": "yes"}, False, True),
    ("draining is missing", {"changed_unix": 1.0}, False, True),
    ("not a dict", [1, 2, 3], False, True),
    ("unparsable", None, False, True),
]


@pytest.mark.parametrize("name,value,absent,draining",
                         GATES, ids=[case[0] for case in GATES])
def test_gate_shapes_require_explicit_admission(tmp_path, name, value, absent, draining):
    """Missing and unreadable evidence cannot open admission."""

    gate = tmp_path / "maintenance.json"
    if not absent:
        gate.write_text("{not json" if value is None else json.dumps(value))
    loop = _worker_loop(gate)

    assert loop.maintenance_requested() is draining, name
    assert (loop.read_maintenance_gate() is not None) is draining, name


def test_a_lost_gate_does_not_reopen_a_restarted_worker(tmp_path):
    gate = tmp_path / 'maintenance.json'
    gate.write_text(json.dumps({'draining': True, 'changed_unix': 458.0}))
    assert _worker_loop(gate).maintenance_requested()
    gate.unlink()
    assert _worker_loop(gate).maintenance_requested()


def test_an_open_gate_reads_as_no_gate_at_all(tmp_path):
    """The caller must not have to inspect ``draining`` a second time.

    ``worker.refusal`` branches on truthiness, so an open gate returning a
    populated dict would refuse admission on a box that is free to serve.
    """

    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": False, "changed_unix": 12.0}))
    loop = _worker_loop(gate)

    assert loop.read_maintenance_gate() is None
    assert not loop.read_maintenance_gate()


def test_the_marker_names_the_drain_it_parked_on(tmp_path):
    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": True, "changed_unix": 1788947072.5}))
    loop = _worker_loop(gate)
    loop.PARKED_ROOT.mkdir(parents=True)

    marker = loop.post_park_marker(loop.read_maintenance_gate())

    assert marker is not None and marker.exists()
    assert marker.parent == loop.PARKED_ROOT
    pid, starttime, changed = marker.name.rsplit("-", 2)
    assert int(pid) == os.getpid()
    assert starttime == _starttime_of_self()
    assert changed == "1788947072.5"


def test_one_marker_per_drain_however_many_polls(tmp_path):
    """The loop reposts on every poll; the drain gets one marker regardless."""

    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": True, "changed_unix": 100.0}))
    loop = _worker_loop(gate)
    loop.PARKED_ROOT.mkdir(parents=True)

    for _ in range(4):
        loop.post_park_marker(loop.read_maintenance_gate())

    assert len(list(loop.PARKED_ROOT.iterdir())) == 1


def test_a_second_drain_gets_its_own_marker(tmp_path):
    """``maintenance_begin`` restamps ``changed_unix``, and the marker follows.

    The broker stamps a fresh value when beginning a new drain, preserving
    the current value on repeated begin calls, so a marker for the previous drain
    is not evidence about this one.  A reader matching on ``changed_unix``
    sees the stale marker as a different drain, which is what makes it safe to
    leave lying there.
    """

    gate = tmp_path / "maintenance.json"
    loop = None
    for stamp in (100.0, 200.0):
        gate.write_text(json.dumps({"draining": True, "changed_unix": stamp}))
        if loop is None:
            loop = _worker_loop(gate)
            loop.PARKED_ROOT.mkdir(parents=True)
        loop.post_park_marker(loop.read_maintenance_gate())

    assert sorted(p.name.rsplit("-", 1)[1]
                  for p in loop.PARKED_ROOT.iterdir()) == ["100.0", "200.0"]


def test_an_unwritable_root_parks_the_loop_anyway(tmp_path):
    """The directory belongs to a root actor that may not have run yet.

    ``/run/prismabuild`` is ``root:root 0755``, so the loop cannot create the
    parked directory itself.  Before the upgrade agent creates and hands it
    over, every write here fails.  Failing to record the park must not stop
    the loop parking: the host then reads as not drained, and a barrier that
    waits is the safe outcome.
    """

    gate = tmp_path / "maintenance.json"
    gate.write_text(json.dumps({"draining": True, "changed_unix": 1.0}))
    loop = _worker_loop(gate)

    assert not loop.PARKED_ROOT.exists()
    assert loop.post_park_marker(loop.read_maintenance_gate()) is None


def test_an_unreadable_gate_parks_under_a_key_no_reader_matches(tmp_path):
    """A drain with no identity must not pass for the drain being waited on."""

    gate = tmp_path / "maintenance.json"
    gate.write_text("{not json")
    loop = _worker_loop(gate)
    loop.PARKED_ROOT.mkdir(parents=True)

    marker = loop.post_park_marker(loop.read_maintenance_gate())

    assert marker is not None
    assert marker.name.rsplit("-", 1)[1] == "unknown"


def _starttime_of_self() -> str:
    """Field 22 of ``/proc/self/stat``, counted from the last ``)``.

    ``comm`` may contain spaces and parentheses, so the fields cannot be read
    by splitting the whole line.  This test computes it independently of the
    module so a bug in the module's own parse cannot certify itself.
    """

    _, _, rest = Path("/proc/self/stat").read_text().rpartition(")")
    return rest.split()[19]
