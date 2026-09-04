"""A worker's ledger must total what the box currently offers, not its peak.

``ensure_capacity`` is increase-only on purpose: two workers declaring the same
box converge, and neither takes back a token the other is executing under.  The
consequence is that the ledger remembers the LARGEST capacity anyone ever
declared for a host, while the offer file is last-writer-wins and falls.  The
two then disagree, and they disagree in the dangerous direction, because
placement reads the offer and admission reads the tokens.

Measured on the live fleet, 2026-09-04, before this landed:

    sparklina  offer gpu 1, mem_gb 40   ledger gpu 4, mem_gb 96
    dl380g10   offer gpu 0, mem_gb 60   ledger gpu 0, mem_gb 180

with three worker processes on each box.  Three concurrent ``gpu=1`` actions
were admissible on a box offering one slot -- the shape of the 2026-09-03
sparklina OOM, reached without any agent doing anything wrong.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

WORKER_LOOP = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "worker_loop.py"


def _worker_loop():
    spec = importlib.util.spec_from_file_location("wl_under_test", WORKER_LOOP)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run(tmp_path: Path, argv: list[str]):
    """One worker start against a private pool root, exiting on the first miss."""

    wl = _worker_loop()
    with mock.patch.object(wl, "SH", tmp_path), \
         mock.patch.object(wl.cpu_topology, "pin_to_preferred", return_value=None), \
         mock.patch.object(wl, "published_commit", return_value="deadbeef"), \
         mock.patch.object(sys, "argv", ["worker_loop.py", *argv]):
        assert wl.main() == 0
    return pool.PoolQueue(tmp_path / "pb-queue")


def _drifted(tmp_path: Path, host: str, capacity: dict[str, int]):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    queue.ledger(host).ensure_capacity(capacity)
    return queue


def test_a_shrunken_gpu_offer_shrinks_the_tokens(tmp_path: Path) -> None:
    """sparklina's live shape: four tokens minted, one slot offered."""

    import socket

    host = socket.gethostname()
    _drifted(tmp_path, host, {"gpu": 4, "mem_gb": 96})

    queue = _run(tmp_path, ["--once", "--gpu-slots", "1", "--mem-gb", "40",
                            "--class", "gb10", "--all-cores"])

    total = queue.ledger(host).capacity()
    assert (total["gpu"], total["mem_gb"]) == (1, 40)


def test_a_shrunken_memory_offer_shrinks_without_the_flag(tmp_path: Path) -> None:
    """dl380g10's live shape -- and it never passed ``--honest-memory``.

    The retire used to be reachable only under that flag, so the box that had
    drifted three-fold was exactly the box that could not correct itself.
    """

    import socket

    host = socket.gethostname()
    _drifted(tmp_path, host, {"gpu": 0, "mem_gb": 180})

    queue = _run(tmp_path, ["--once", "--gpu-slots", "0", "--mem-gb", "60",
                            "--class", "x86", "--all-cores"])

    assert queue.ledger(host).capacity()["mem_gb"] == 60


def test_a_running_action_keeps_the_tokens_it_is_executing_under(
    tmp_path: Path,
) -> None:
    """Retiring is blunt on purpose, so it must be safe under live work."""

    import socket

    host = socket.gethostname()
    queue = _drifted(tmp_path, host, {"gpu": 4, "mem_gb": 96})
    assert queue.ledger(host).acquire("a" * 64, {"gpu": 2, "mem_gb": 32}) is True

    queue = _run(tmp_path, ["--once", "--gpu-slots", "1", "--mem-gb", "40",
                            "--class", "gb10", "--all-cores"])

    ledger = queue.ledger(host)
    # The holder is untouched, so the total cannot fall below what it holds --
    # it falls the rest of the way when that action finishes and its tokens
    # are released rather than re-created.
    assert ledger.held_keys() == ["a" * 64]
    assert ledger.capacity()["gpu"] == 2
    assert ledger.available().get("gpu", 0) == 0
    assert ledger.capacity()["mem_gb"] == 40


def test_a_grown_offer_still_mints_the_new_tokens(tmp_path: Path) -> None:
    """Retiring must not turn the ledger into a ratchet in the other direction."""

    import socket

    host = socket.gethostname()
    _drifted(tmp_path, host, {"gpu": 1, "mem_gb": 8})

    queue = _run(tmp_path, ["--once", "--gpu-slots", "2", "--mem-gb", "48",
                            "--class", "gb10", "--all-cores"])

    total = queue.ledger(host).capacity()
    assert (total["gpu"], total["mem_gb"]) == (2, 48)


def test_the_retire_is_not_hidden_behind_the_honest_memory_flag() -> None:
    """The defect was the guard, not the primitive; keep the guard gone."""

    source = WORKER_LOOP.read_text()
    call = "queue.ledger().retire_free_capacity(capacity)"
    assert call in source
    before = source.split(call)[0]
    tail = before.rsplit("queue = pool.PoolQueue", 1)[-1]
    assert "honest_memory" not in tail, (
        "the retire is guarded again; a box that never passes the flag is "
        "exactly the box that cannot correct its own drift")
