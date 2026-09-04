"""A loop that started unversioned must still notice the runtime moving.

``worker_loop`` imports its code once and holds those bytes for its whole
life, so it exits between actions when the published commit changes and lets
the supervisor respawn it on the new bytes.  The check was guarded on the
loop having a commit of its own -- and a loop with no commit is exactly the
one that cannot reload, since it can never match a published one.

Measured on the live fleet, 2026-09-04: 32 of 60 samples of sparky's offer
file announced ``runtime_commit: ""`` and an older capacity shape
(``{"gpu": 2, "mem_gb": 48}``, no ``cpu``), interleaved with 28 announcing
the current commit and ``{"cpu": 10, "gpu": 2, "mem_gb": 48}``.  One host,
one last-writer-wins offer, two generations of loops writing it -- and the
older generation had survived every publish since ``RUNTIME_VERSION.json``
was introduced.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
import socket
import sys
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

# Imported here, before ``worker_loop`` is exec'd: the loop puts the PUBLISHED
# mirror's ``src`` at ``sys.path[0]``, so whichever of the two is imported
# first wins for the whole process, and the checkout's bytes are the ones
# under test.
from prismabuild import pool  # noqa: E402

WORKER_LOOP = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "worker_loop.py"


def _worker_loop():
    spec = importlib.util.spec_from_file_location("wl_reload_under_test", WORKER_LOOP)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run(tmp_path: Path, commits: list[str], capsys) -> str:
    """One worker start whose view of the published commit changes under it."""

    wl = _worker_loop()
    with mock.patch.object(wl, "SH", tmp_path), \
         mock.patch.object(wl.cpu_topology, "pin_to_preferred", return_value=None), \
         mock.patch.object(wl, "loaded_runtime_commit", return_value=commits[0]), \
         mock.patch.object(wl, "published_commit", side_effect=commits[1:]), \
         mock.patch.object(sys, "argv",
                           ["worker_loop.py", "--once", "--gpu-slots", "0",
                            "--mem-gb", "8", "--class", "x86", "--all-cores"]):
        assert wl.main() == 0
    return capsys.readouterr().out


def test_an_unversioned_loop_reloads_when_a_commit_appears(tmp_path, capsys) -> None:
    """The one that could not reload is the one that most needed to."""

    out = _run(tmp_path, ["", "abc123def456abc"], capsys)

    assert "runtime moved (unversioned) -> abc123def456" in out
    assert "nothing admissible" not in out         # it left by the reload path


def test_a_versioned_loop_still_only_reloads_on_a_change(tmp_path, capsys) -> None:
    """The published commit standing still is not an event."""

    out = _run(tmp_path, ["abc123def456abc", "abc123def456abc"], capsys)

    assert "runtime moved" not in out
    assert "nothing admissible" in out             # it left by --once, as before


def test_an_unreadable_version_file_is_not_a_move(tmp_path, capsys) -> None:
    """"" means unknown.  A loop must not exit every poll on a bad read."""

    out = _run(tmp_path, ["abc123def456abc", ""], capsys)

    assert "runtime moved" not in out
    assert "nothing admissible" in out


def test_the_offer_carries_the_commit_the_loop_is_running(tmp_path, capsys) -> None:
    """Which is what makes a stale generation visible from another box."""

    _run(tmp_path, ["abc123def456abc", "abc123def456abc"], capsys)

    offers = pool.PoolQueue(tmp_path / "pb-queue").offers()
    mine = [o for o in offers if o.get("host") == socket.gethostname()]
    assert [o.get("runtime_commit") for o in mine] == ["abc123def456abc"]
