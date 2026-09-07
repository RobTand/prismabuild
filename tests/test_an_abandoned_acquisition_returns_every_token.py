"""A ``begin_acquire`` that ends in an exception returns the tokens it took.

``begin_acquire`` takes the whole demand into a claimant-private directory
under ``held/`` and promises all-or-nothing: *"a multi-resource actor that keeps
what it managed to get while blocked on what it did not is holding resources it
cannot use."*  It keeps that promise with one handler::

    except _Insufficient:
        self._empty_into_free(destination)
        return None

``_Insufficient`` is raised by ``begin_acquire`` itself, so the handler covers
exactly the one ending the function authors.  Every other ending skips the
rollback -- and because the function never returns, the caller is handed no
``handle`` and therefore cannot call ``abandon_acquire`` either.  The tokens
stay under ``held/claiming.<micros>.<key>.<host>.<pid>.<uuid>/``, where
``capacity``, ``held``, ``retire_free_capacity`` and ``ensure_capacity``'s
holder scan all honestly count them as consumed.

The endings are not hypothetical and not all environmental:

* ``cpu_allocation`` raises ``PoolContractError("CPU token exceeds configured
  topology")`` whenever a held ``cpu-NNNN`` index is at or past the length of
  the configured tier list.  A box whose declared CPU tiers shrank below a
  previously minted token index reproduces it on **every** admission, not once.
* ``cpu_allocation`` also reads ``.adaptive.json`` through ``_read_json`` in its
  loud mode, so a torn or unreadable metadata file raises there.
* ``os.rename`` at the token move catches only ``FileNotFoundError`` and
  ``NotADirectoryError``; ESTALE, EACCES, EIO and ENOSPC on ``/mnt/shared``
  fall out of the loop.
* the two ``_write_json_atomic`` metadata writes are ordinary NFS writes.

Only ``sweep_stale_acquisitions`` recovers a stranded private holder, and its
grace is ``LEASE_TIMEOUT_S`` -- 300 s -- not ``HEARTBEAT_S``, because the
stamping pid is still alive whenever the caller survives the raise.  Against a
deterministic cause and a ten-second poll that is not a stall but a starvation:
each attempt strands the whole demand for five minutes, and the box's free pool
drains to nothing while every attempt looks, from outside, like a contract
error the operator is expected to read and ignore.

This is the shape issue #288 closed one layer up, where an unexpected exception
carried an action past its token-return gate.  The same reasoning applies to the
gate itself: a rollback that only fires for the exception the function meant to
raise is not a rollback, it is a happy path with a name.

Pre-fix failure line: ``src/prismabuild/pool.py:1254`` at ``67a44bb``,
``except _Insufficient:`` as the only handler around lines 1211-1253.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from prismabuild import pool  # noqa: E402

KEY = "e" * 64

# One CPU in the tier map, two CPU tokens in the ledger.  The second token's
# index is at the length of the tier list, which is what ``cpu_allocation``
# refuses.  Nothing here is injected: it is the ordinary reading of a box whose
# declared topology no longer covers a token it already minted.
NARROW_TIERS = {"preferred": [0], "fallback": []}


def _ledger(tmp_path: Path) -> pool.ResourceLedger:
    ledger = pool.ResourceLedger(tmp_path / "reservations", host="testbox")
    ledger.ensure_capacity({"cpu": 2})
    return ledger


def test_a_contract_error_inside_begin_acquire_still_returns_the_tokens(
    tmp_path: Path,
) -> None:
    """The demand is all-or-nothing however the attempt ends."""

    ledger = _ledger(tmp_path)
    assert ledger.available().get("cpu", 0) == 2

    with pytest.raises(pool.PoolContractError):
        ledger.begin_acquire(
            KEY, {"cpu": 2}, adaptive={}, cpu_tiers=NARROW_TIERS
        )

    assert ledger.available().get("cpu", 0) == 2, (
        "tokens taken by a begin_acquire that raised are still held"
    )
    assert ledger.held().get("cpu", 0) == 0


def test_no_private_holder_survives_a_failed_acquisition(
    tmp_path: Path,
) -> None:
    """The claimant-private directory is not left behind for the sweep.

    ``sweep_stale_acquisitions`` is the recovery of last resort, and it waits
    ``LEASE_TIMEOUT_S``.  A rollback that ran leaves it nothing to find.
    """

    ledger = _ledger(tmp_path)
    with pytest.raises(pool.PoolContractError):
        ledger.begin_acquire(
            KEY, {"cpu": 2}, adaptive={}, cpu_tiers=NARROW_TIERS
        )

    stranded = [
        holder.name
        for holder in ledger.held_dir.iterdir()
        if holder.name.startswith(pool.ACQUIRING_PREFIX)
    ]
    assert stranded == [], f"private acquisition holders left behind: {stranded}"


def test_an_insufficient_demand_still_rolls_back_and_answers_none(
    tmp_path: Path,
) -> None:
    """The regression guard for the ending the handler was written for."""

    ledger = _ledger(tmp_path)
    assert ledger.begin_acquire(KEY, {"cpu": 4}) is None
    assert ledger.available().get("cpu", 0) == 2
