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
    """The rule both cycle reasons share: a loop holding an action is left."""
    monkeypatch.setattr(supervise, "_live_loops", lambda: [11, 22, 33])
    monkeypatch.setattr(supervise, "_is_idle", lambda pid: pid != 22)
    killed = []
    monkeypatch.setattr(supervise.os, "kill", lambda pid, sig: killed.append(pid))
    assert supervise._stop_idle_loops() == [11, 33]
    assert 22 not in killed
