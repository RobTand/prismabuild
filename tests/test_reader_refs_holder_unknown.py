"""refs_for_holder must signal unknown census, never report drained (PB #740).

Root integrated RED (pb728-reader-integrated.json): the membership RESIGN
path read the SDK's [] on an unreadable/corrupt census as drained and
reported RESIGNED. Only proven absence may return empty; unreadable,
incomplete, or corrupt census raises through the existing API (OSError /
ValueError), which the membership gate already catches into unknown-retain.

The membership gate integration imports this checkout's production module
and verifies its source path and SDK identity. Future membership changes
therefore run through the same regression. Run via published pbtest at -10.
"""
from __future__ import annotations

import importlib
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool, reader_lease  # noqa: E402

def _leases(queue, owner="c" * 64):
    root = queue.root / "residency" / "leases" / owner
    root.mkdir(parents=True)
    return root


def test_unreadable_owner_dir_signals_unknown(tmp_path) -> None:
    """An unreadable owner directory raises; it is never empty."""

    if os.geteuid() == 0:
        pytest.skip("permission-gated census needs a non-root reader")
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    owner_dir = _leases(queue)
    (owner_dir / ("p" * 32 + ".lease.json")).write_text("{}")
    os.chmod(owner_dir, 0o000)
    try:
        with pytest.raises(OSError):
            reader_lease.refs_for_holder(queue, "any-host")
    finally:
        os.chmod(owner_dir, 0o755)


def test_unreadable_leases_root_signals_unknown(tmp_path) -> None:
    """An unreadable leases root raises; it is never drained."""

    if os.geteuid() == 0:
        pytest.skip("permission-gated census needs a non-root reader")
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    root = queue.root / "residency" / "leases"
    _leases(queue)
    os.chmod(root, 0o000)
    try:
        with pytest.raises(OSError):
            reader_lease.refs_for_holder(queue, "any-host")
    finally:
        os.chmod(root, 0o755)


def test_corrupt_pin_signals_unknown(tmp_path) -> None:
    """An unparseable pin raises; never another host's business to clear."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    owner_dir = _leases(queue)
    (owner_dir / ("p" * 32 + ".lease.json")).write_text("{}")
    with pytest.raises(ValueError):
        reader_lease.refs_for_holder(queue, "any-host")


def test_absent_namespace_stays_drained(tmp_path) -> None:
    """Proven absence -- no leases namespace at all -- still returns empty."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    assert reader_lease.refs_for_holder(queue, "any-host") == []


def _membership_gate():
    """The production gate with prismabuild bound to this same checkout."""
    module = importlib.import_module("fleet_membership")
    expected = (Path(__file__).resolve().parents[1]
                / "tools" / "fleet" / "fleet_membership.py")
    assert Path(module.__file__).resolve() == expected.resolve()
    assert module.pool_module is pool, "gate bound a foreign pool module"
    assert module._reader_lease() is reader_lease, "gate bound a foreign SDK"
    return module


def test_membership_gate_retains_on_corrupt_pin(tmp_path) -> None:
    """Real gate + fixed SDK: corrupt census retains the fence."""

    fm = _membership_gate()
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    owner_dir = _leases(queue)
    (owner_dir / ("p" * 32 + ".lease.json")).write_text("{}")
    ok, state, _detail = fm.reader_refs_gate(queue, "any-host")
    assert ok is False
    assert state.startswith("unknown-refs-unreadable"), state


def test_membership_gate_drains_on_absent_namespace(tmp_path) -> None:
    """Real gate + fixed SDK: proven absence still drains.

    With the owning module present the census enumerates (absent reads
    as an empty enumeration, ``refs-drained``); ``drained-absent`` is
    the module-missing path.
    """

    fm = _membership_gate()
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    ok, state, detail = fm.reader_refs_gate(queue, "any-host")
    assert (ok, state, detail) == (True, "refs-drained", {"refs": []})


def test_membership_gate_drains_on_empty_enumerated_census(tmp_path) -> None:
    """Real gate + fixed SDK: an enumerable namespace with no pins drains."""

    fm = _membership_gate()
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    _leases(queue)
    ok, state, detail = fm.reader_refs_gate(queue, "any-host")
    assert (ok, state, detail) == (True, "refs-drained", {"refs": []})
