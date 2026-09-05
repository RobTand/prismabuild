"""A basename is not ownership (issue #87).

``supervise.py`` found its fleet's worker loops with ``pgrep -f
worker_loop.py`` and confirmed a candidate by asking whether an interpreter
was running a script whose name ended that way.  Both halves are true of an
unrelated ``/another-project/worker_loop.py`` under the same user, so one was
counted toward this box's fleet capacity, and on the first tick where its
arguments did not match ``fleet_boxes.json`` and it happened to have no child
process it was SIGTERMed.  A long-running Python workload is busy and
childless at the same time all the time.

Two facts now stand in for the name: the script resolves inside a runtime
generation this fleet published, and the process carries the ownership mark
the supervisor sets on what it launches.  Old generations count, because a
loop holds the bytes it imported for its whole life and publication never
deletes a generation.

Nothing here reads a real process or sends a real signal: the candidate list,
the ``/proc`` bytes and ``os.kill`` are all this test's own.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import supervise  # noqa: E402

HOST = "boxa"
#: Far outside any pid this box could hand out, so a mistake cannot land on a
#: real process even if a patch were to slip.
UNRELATED = 1000000007
LIVE = 1000000008
OLD = 1000000009
STAGING = 1000000010
UNMARKED = 1000000011
RELATIVE = 1000000012
NOT_PYTHON = 1000000013


def test_the_issues_reproduction_counts_and_signals_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The issue's own reproduction, patching exactly what it patched.

    Fake ``pgrep``, fake ``/proc`` bytes and fake signals, so no real process
    is inspected and none is signalled.  Before the fix this printed
    ``confirmed fleet workers: [1000000007]`` and ``fake signals:
    [(1000000007, 15)]`` for a program that belongs to another project.
    """

    argv = (b"/usr/bin/python3\0/another-project/worker_loop.py"
            b"\0--unrelated-work\0")
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    monkeypatch.setattr(
        supervise.subprocess, "run",
        lambda *_a, **_k: subprocess.CompletedProcess([], 0, str(UNRELATED), ""))
    monkeypatch.setattr(supervise.Path, "read_bytes", lambda _self: argv)

    live = supervise._live_loops()

    sent: list[tuple[int, int]] = []
    monkeypatch.setattr(supervise, "_is_idle", lambda _pid: True)
    monkeypatch.setattr(supervise.os, "kill",
                        lambda pid, sig: sent.append((pid, int(sig))))
    stopped = supervise._stop_idle_loops(live)

    assert live == [], "an unrelated worker_loop.py counted as fleet capacity"
    assert stopped == []
    assert sent == [], "an unrelated process was sent a signal"


@pytest.fixture
def fleet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A fake mount: two published generations, a staging survivor, the link."""

    mirror = tmp_path / "fleet"
    store = mirror / "runtime-generations"
    for name in ("gen-old", "gen-live", ".gen-next.staging"):
        generation = store / name
        (generation / "tools").mkdir(parents=True)
        (generation / "tools" / "worker_loop.py").write_text("# a loop\n")
        (generation / "RUNTIME_VERSION.json").write_text(
            json.dumps({"commit": name}))
    (mirror / "repo").symlink_to(store / "gen-live")

    proc = tmp_path / "proc"
    proc.mkdir()
    monkeypatch.setattr(supervise, "MIRROR", mirror)
    monkeypatch.setattr(supervise, "PROC", proc)
    monkeypatch.setattr(supervise, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(supervise.socket, "gethostname", lambda: HOST)
    return mirror, store, proc


def _process(proc: Path, pid: int, argv: list[str], *,
             mark: str | None = HOST, cwd: Path | None = None) -> int:
    """Write the ``/proc`` bytes one candidate process would have."""

    directory = proc / str(pid)
    directory.mkdir()
    (directory / "cmdline").write_bytes(
        b"".join(part.encode() + b"\0" for part in argv))
    entries = ["HOME=/home/rob", "PATH=/usr/bin:/bin"]
    if mark is not None:
        entries.append(f"{supervise.OWNERSHIP_ENV}={mark}")
    (directory / "environ").write_bytes(
        b"".join(entry.encode() + b"\0" for entry in entries))
    if cwd is not None:
        (directory / "cwd").symlink_to(cwd)
    return pid


@pytest.fixture
def candidates(monkeypatch: pytest.MonkeyPatch):
    """Stand in for ``pgrep``: the pids this test says are candidates."""

    offered: list[int] = []

    def fake_run(argv, *_args, **_kwargs):
        assert argv[0] == "pgrep", argv
        return subprocess.CompletedProcess(
            argv, 0, "\n".join(str(pid) for pid in offered), "")

    monkeypatch.setattr(supervise.subprocess, "run", fake_run)
    return offered


@pytest.fixture
def signals(monkeypatch: pytest.MonkeyPatch):
    """Every signal the supervisor would have sent, sent to nothing."""

    sent: list[tuple[int, int]] = []
    monkeypatch.setattr(supervise.os, "kill",
                        lambda pid, sig: sent.append((pid, int(sig))))
    monkeypatch.setattr(supervise, "_is_idle", lambda pid: True)
    return sent


def test_an_unrelated_worker_loop_is_neither_counted_nor_signalled(
    fleet, candidates, signals
) -> None:
    """The issue's reproduction: same user, same script name, another tree."""

    _mirror, _store, proc = fleet
    candidates.append(_process(
        proc, UNRELATED,
        ["/usr/bin/python3", "/another-project/worker_loop.py",
         "--unrelated-work"]))

    assert supervise._live_loops() == []
    assert supervise._stop_idle_loops([UNRELATED]) == []
    assert signals == [], "an unrelated process was signalled"


def test_a_loop_in_the_live_generation_is_ours(fleet, candidates) -> None:
    mirror, _store, proc = fleet
    pid = _process(proc, LIVE, [
        "/usr/bin/python3", str(mirror / "repo" / "tools" / "worker_loop.py"),
        "--gpu-slots", "2"])
    candidates.append(pid)

    assert supervise._live_loops() == [pid]


def test_a_loop_from_an_older_generation_is_still_ours(fleet, candidates) -> None:
    """A loop holds the bytes it imported, and publication deletes nothing.

    Refusing to recognize the previous generation's loops would make the
    supervisor spawn replacements beside actions still in flight.
    """

    _mirror, store, proc = fleet
    pid = _process(proc, OLD, [
        "/usr/bin/python3",
        str(store / "gen-old" / "tools" / "worker_loop.py"), "--gpu-slots", "2"])
    candidates.append(pid)

    assert supervise._live_loops() == [pid]


def test_a_staging_survivor_is_not_a_proven_root(fleet, candidates) -> None:
    """An interrupted publish leaves a dot-name that carries a receipt and was
    never sealed or probed; ``publish_runtime`` refuses it and so does this."""

    _mirror, store, proc = fleet
    candidates.append(_process(proc, STAGING, [
        "/usr/bin/python3",
        str(store / ".gen-next.staging" / "tools" / "worker_loop.py")]))

    assert supervise._live_loops() == []


def test_a_fleet_path_without_the_mark_is_not_ours(fleet, candidates) -> None:
    """The path says where the bytes came from; the mark says who launched it.

    A loop started by hand, or by a unit, is not this supervisor's to count or
    to stop, and neither is anything that merely sits in the tree.
    """

    mirror, _store, proc = fleet
    candidates.append(_process(
        proc, UNMARKED,
        ["/usr/bin/python3",
         str(mirror / "repo" / "tools" / "worker_loop.py")],
        mark=None))

    assert supervise._live_loops() == []


def test_a_relative_script_resolves_through_the_process_own_cwd(
    fleet, candidates
) -> None:
    """dl380g10's loops carry ``repo/tools/worker_loop.py`` and a cwd of the
    mount, so the path is resolved against the process rather than against
    whatever directory the supervisor happens to be in."""

    mirror, _store, proc = fleet
    pid = _process(proc, RELATIVE,
                   ["/usr/bin/python3", "repo/tools/worker_loop.py",
                    "--class", "x86"],
                   cwd=mirror)
    candidates.append(pid)

    assert supervise._live_loops() == [pid]


def test_something_that_is_not_an_interpreter_is_not_a_loop(
    fleet, candidates
) -> None:
    mirror, _store, proc = fleet
    candidates.append(_process(proc, NOT_PYTHON, [
        "/bin/bash", str(mirror / "repo" / "tools" / "worker_loop.py")]))

    assert supervise._live_loops() == []


def test_only_the_fleet_loops_are_counted_in_a_mixed_box(
    fleet, candidates, signals
) -> None:
    """The count is what decides a respawn, so a foreign process inflating it
    suppresses exactly the top-up the supervisor exists for."""

    mirror, _store, proc = fleet
    ours = _process(proc, LIVE, [
        "/usr/bin/python3", str(mirror / "repo" / "tools" / "worker_loop.py")])
    candidates.append(_process(proc, UNRELATED, [
        "/usr/bin/python3", "/another-project/worker_loop.py"]))
    candidates.insert(0, ours)

    assert supervise._live_loops() == [ours]
    assert supervise._stop_idle_loops() == [ours]
    assert signals == [(ours, 15)]


def test_the_supervisor_marks_every_loop_it_spawns(
    fleet, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The mark is set at the one place a fleet loop is created, so the read
    back out of ``/proc`` has something to find."""

    captured: dict[str, object] = {}

    class FakePopen:
        pid = 4242

        def __init__(self, argv, **kwargs):
            captured["argv"] = argv
            captured["env"] = kwargs.get("env")

    monkeypatch.setattr(supervise.subprocess, "Popen", FakePopen)

    assert supervise._spawn(["--gpu-slots", "2"], 0) == 4242
    assert captured["env"][supervise.OWNERSHIP_ENV] == HOST
    assert captured["env"]["PATH"], "the rest of the environment must survive"
