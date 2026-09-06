"""Retiring a worker record must not be able to retire a live name.

Issue #244.  There was no supported way to take a worker offer out of
``workers/``, so one was moved aside by hand.  The tool that makes that
repeatable is only worth having if it refuses the case the hand-move got right
by knowing the fleet: a name a loop is still announcing under.

Each refusal below is a different way for the name to be in use, and each has
its own test, because a tool that refuses everything for one reason is
indistinguishable from a tool that works until the reason it checks is not the
one that applies.
"""
from __future__ import annotations

import importlib.util
import json
import time
from pathlib import Path

import pytest
from prismabuild import pool

TOOL = Path(__file__).resolve().parents[1] / "tools" / "fleet" / "retire_worker.py"
HOST = "gx10-6b77"


def _tool():
    spec = importlib.util.spec_from_file_location("retire_worker_under_test", TOOL)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _queue(tmp_path: Path) -> pool.PoolQueue:
    return pool.PoolQueue(tmp_path / "queue")


def _announce(queue: pool.PoolQueue, *, age_s: float, host: str = HOST) -> Path:
    """A worker record as ``announce`` writes one, aged by hand."""

    directory = queue.root / pool.WORKERS
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{host}.json"
    path.write_text(json.dumps({
        "schema": pool.POOL_OFFER_SCHEMA_V1,
        "host": host,
        "tags": ["arm64"],
        "has_gpu": True,
        "capacity": {"cpu": 20, "mem_gb": 100, "gpu": 1},
        "announced_unix": time.time() - age_s,
    }))
    return path


def _retired(queue: pool.PoolQueue, host: str = HOST) -> Path:
    return queue.root / _tool().RETIRED / f"{host}.json"


# --- the refusals ----------------------------------------------------------

def test_a_record_a_loop_is_still_refreshing_is_not_retired(tmp_path, capsys):
    """One poll ago is a live loop, whatever the operator believes."""

    queue = _queue(tmp_path)
    path = _announce(queue, age_s=5.0)
    tool = _tool()

    assert tool.main([HOST, "--root", str(queue.root), "--apply"]) == 1

    assert path.exists(), "the record must stay where offers() reads it"
    assert not _retired(queue).exists()
    assert "lease timeout" in capsys.readouterr().out


def test_a_record_quiet_only_past_the_offer_timeout_is_not_retired(tmp_path):
    """A box between polls under load is quiet, not gone.

    The threshold is the lease timeout, not the offer timeout: the shorter one
    is how long a scheduler believes an offer, and three missed polls would
    retire a healthy box.
    """

    queue = _queue(tmp_path)
    _announce(queue, age_s=pool.OFFER_TIMEOUT_S + 30.0)
    assert pool.OFFER_TIMEOUT_S + 30.0 < pool.LEASE_TIMEOUT_S

    assert _tool().main([HOST, "--root", str(queue.root), "--apply"]) == 1
    assert (queue.root / pool.WORKERS / f"{HOST}.json").exists()


def test_a_name_with_work_out_on_it_is_not_retired(tmp_path, capsys):
    """A stale offer beside a live claim is a loop that stopped announcing.

    Retiring the name there takes away the only record of which box the
    reaper's lease belongs to.
    """

    queue = _queue(tmp_path)
    _announce(queue, age_s=pool.LEASE_TIMEOUT_S * 4)
    claimed = queue.dir(pool.CLAIMED)
    claimed.mkdir(parents=True, exist_ok=True)
    (claimed / f"{'b' * 64}.json").write_text(
        json.dumps({"claimed_host": HOST, "claimed_by": "worker-1"}))

    assert _tool().main([HOST, "--root", str(queue.root), "--apply"]) == 1

    assert (queue.root / pool.WORKERS / f"{HOST}.json").exists()
    assert "names this host" in capsys.readouterr().out


def test_a_name_holding_a_reservation_is_not_retired(tmp_path, capsys):
    """Capacity is reserved under the host name the ledger is keyed by."""

    queue = _queue(tmp_path)
    _announce(queue, age_s=pool.LEASE_TIMEOUT_S * 4)
    held = queue.ledger(HOST).held_dir / ("c" * 64)
    held.mkdir(parents=True, exist_ok=True)
    (held / "cpu-0").mkdir()

    assert _tool().main([HOST, "--root", str(queue.root), "--apply"]) == 1

    assert (queue.root / pool.WORKERS / f"{HOST}.json").exists()
    assert "reservation" in capsys.readouterr().out


def test_a_record_refreshed_between_the_check_and_the_move_is_put_back(
    tmp_path, monkeypatch,
):
    """The rename is the read: the moved file is what the checks are re-run on.

    A loop can announce in the window between reading the record and moving
    it, and on a shared filesystem it eventually will.  The refresh is driven
    from ``os.rename`` itself rather than from one of the checks, because the
    window this closes is the one *after* the last check: hooking a check
    would make this test bite when that check is removed, and then it would be
    measuring the wrong thing.
    """

    queue = _queue(tmp_path)
    path = _announce(queue, age_s=pool.LEASE_TIMEOUT_S * 4)
    tool = _tool()
    original = tool.os.rename
    seen = {}

    def refresh_then_rename(src, dst):
        if not seen:
            seen["done"] = True
            record = json.loads(path.read_text())
            record["announced_unix"] = time.time()
            path.write_text(json.dumps(record))
        return original(src, dst)

    monkeypatch.setattr(tool.os, "rename", refresh_then_rename)
    code, message = tool.retire(queue, HOST, now=time.time())

    assert code == 1
    assert "changed between the check and the move" in message
    assert path.exists(), "the record must be put back exactly where it was"
    assert not _retired(queue).exists()


# --- what it does when the name really is gone -----------------------------

def test_a_quiet_unclaimed_name_is_moved_where_offers_no_longer_sees_it(tmp_path):
    queue = _queue(tmp_path)
    _announce(queue, age_s=pool.LEASE_TIMEOUT_S * 4)

    assert _tool().main([HOST, "--root", str(queue.root), "--apply"]) == 0

    assert not (queue.root / pool.WORKERS / f"{HOST}.json").exists()
    assert _retired(queue).exists()
    assert [offer["host"] for offer in queue.offers(max_age_s=1e9)] == []


def test_without_apply_nothing_moves(tmp_path, capsys):
    queue = _queue(tmp_path)
    _announce(queue, age_s=pool.LEASE_TIMEOUT_S * 4)

    assert _tool().main([HOST, "--root", str(queue.root)]) == 0

    assert (queue.root / pool.WORKERS / f"{HOST}.json").exists()
    assert "re-run with --apply" in capsys.readouterr().out


def test_a_retirement_is_reversible(tmp_path):
    """Nothing is deleted, because the operator may find out it was wrong."""

    queue = _queue(tmp_path)
    _announce(queue, age_s=pool.LEASE_TIMEOUT_S * 4)
    tool = _tool()
    assert tool.main([HOST, "--root", str(queue.root), "--apply"]) == 0

    assert tool.main([HOST, "--root", str(queue.root), "--restore"]) == 0

    restored = queue.root / pool.WORKERS / f"{HOST}.json"
    assert restored.exists()
    assert json.loads(restored.read_text())["host"] == HOST
    assert not _retired(queue).exists()


def test_restoring_a_name_that_came_back_on_its_own_is_refused(tmp_path):
    """A live announce is the current truth; the archive must not overwrite it."""

    queue = _queue(tmp_path)
    _announce(queue, age_s=pool.LEASE_TIMEOUT_S * 4)
    tool = _tool()
    assert tool.main([HOST, "--root", str(queue.root), "--apply"]) == 0
    fresh = _announce(queue, age_s=1.0)

    assert tool.main([HOST, "--root", str(queue.root), "--restore"]) == 1

    assert json.loads(fresh.read_text())["announced_unix"] > time.time() - 60


def test_a_name_with_no_record_is_reported_not_invented(tmp_path, capsys):
    queue = _queue(tmp_path)
    (queue.root / pool.WORKERS).mkdir(parents=True, exist_ok=True)

    assert _tool().main(["gx10-nobody", "--root", str(queue.root), "--apply"]) == 1
    assert "no worker record" in capsys.readouterr().out
