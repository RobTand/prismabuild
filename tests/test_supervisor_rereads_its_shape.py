"""The supervisor's declared shape is a file it re-reads, not a startup snapshot.

``supervise.py`` says the shape of the fleet is a versioned file rather than
three command lines nobody wrote down.  It read that file once, at startup, so
a supervisor older than a publish kept its old arguments for its whole life:
``fleet_boxes.json`` said three GPU slots, the box announced two, and the only
cure was remembering to restart the supervisor by hand.  These pin the reload
and the one refusal it must keep making.
"""

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import supervise  # noqa: E402


def _write(path, *, loops=3, slots="2"):
    path.write_text(json.dumps({"boxes": {"boxa": {
        "loops": loops,
        "args": ["--class", "gb10", "--gpu-slots", slots],
    }}}))


@pytest.fixture
def config(tmp_path, monkeypatch):
    path = tmp_path / "fleet_boxes.json"
    _write(path)
    monkeypatch.setattr(supervise, "CONFIG", path)
    # The fallback path must not resolve to the real published mirror, or a
    # test that corrupts its own file would quietly read the live fleet's.
    monkeypatch.setattr(supervise, "MIRROR", tmp_path / "absent")
    return path


def test_the_shape_is_read_from_the_file(config):
    assert supervise.declared_shape("boxa", 0) == (
        3, ["--class", "gb10", "--gpu-slots", "2"])


def test_a_rewritten_file_is_seen_without_a_restart(config):
    first = supervise.declared_shape("boxa", 0)
    _write(config, loops=4, slots="3")
    second = supervise.declared_shape("boxa", 0, first)
    assert second == (4, ["--class", "gb10", "--gpu-slots", "3"])
    assert second[1] != first[1], "the args must move, not only the count"


def test_an_explicit_loop_count_still_overrides_the_file(config):
    assert supervise.declared_shape("boxa", 7)[0] == 7


def test_a_truncated_file_keeps_the_last_good_shape(config):
    good = supervise.declared_shape("boxa", 0)
    config.write_text('{"boxes": {"boxa": {"loo')      # a publish, mid-write
    assert supervise.declared_shape("boxa", 0, good) == good


def test_the_first_read_still_refuses_to_guess(config):
    """A fallback is for keeping a known shape, never for inventing one."""
    config.write_text('{"boxes": {}}')
    with pytest.raises(SystemExit):
        supervise.declared_shape("boxa", 0)


def test_only_idle_loops_are_stopped(monkeypatch):
    """The rule both cycle reasons share: a loop holding an action is left.

    Ownership is stubbed rather than staged: these pids are not fleet loops
    and ``_stop_idle_loops`` proves ownership at the kill site since issue
    #87.  The rule under test here is the idleness one, and
    ``test_supervisor_owns_the_loops_it_signals.py`` pins the other.
    """
    monkeypatch.setattr(supervise, "_live_loops", lambda *_a, **_k: [11, 22, 33])
    monkeypatch.setattr(supervise, "_is_fleet_loop", lambda *_a, **_k: True)
    monkeypatch.setattr(supervise, "_is_idle", lambda pid: pid != 22)
    killed = []
    monkeypatch.setattr(supervise.os, "kill", lambda pid, sig: killed.append(pid))
    assert supervise._stop_idle_loops() == [11, 33]
    assert 22 not in killed


def test_a_loops_own_argv_is_what_gets_compared(tmp_path):
    """The authority is the file; the question is what the process carries.

    Comparing the file to the previous read of the file looks equivalent and
    is not: a supervisor restarted after a publish reads the new shape before
    its first tick, so no later tick ever sees a change, and loops spawned by
    the previous supervisor keep the old arguments for as long as they live.
    That is the live failure -- three loops announcing two GPU slots under a
    supervisor whose own banner said three.
    """
    assert supervise.loop_args_of(0) is None          # unreadable, not "empty"
    argv = ["/usr/bin/python3", "/mnt/x/worker_loop.py",
            "--tag", "boxa", "--gpu-slots", "2"]
    src = tmp_path / "cmdline"
    src.write_bytes(b"\0".join(a.encode() for a in argv) + b"\0")
    # loop_args_of reads /proc/<pid>/cmdline; the parse is what is pinned here.
    parsed = [p.decode() for p in src.read_bytes().split(b"\0") if p][2:]
    assert parsed == ["--tag", "boxa", "--gpu-slots", "2"]
    assert parsed != ["--tag", "boxa", "--gpu-slots", "3"], (
        "a loop carrying the old slot count must not compare equal to the new")


def test_only_the_mismatched_loops_are_candidates(monkeypatch):
    """A loop already on the declared shape is never stopped for it."""
    shape = ["--gpu-slots", "3"]
    carried = {11: ["--gpu-slots", "2"], 22: shape, 33: None}
    monkeypatch.setattr(supervise, "_live_loops", lambda *_a, **_k: [11, 22, 33])
    # As above: the rule under test is the shape mismatch, not the ownership
    # proof these fake pids cannot satisfy.
    monkeypatch.setattr(supervise, "_is_fleet_loop", lambda *_a, **_k: True)
    monkeypatch.setattr(supervise, "loop_args_of", lambda pid: carried[pid])
    monkeypatch.setattr(supervise, "_is_idle", lambda pid: True)
    killed = []
    monkeypatch.setattr(supervise.os, "kill", lambda pid, sig: killed.append(pid))

    wrong = [p for p in supervise._live_loops()
             if supervise.loop_args_of(p) not in (None, shape)]
    assert wrong == [11], "22 matches the shape; 33 is unreadable, not wrong"
    assert supervise._stop_idle_loops(wrong) == [11]
    assert killed == [11]
