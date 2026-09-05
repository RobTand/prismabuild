"""One token per configured index, whatever the scan sees mid-turnover.

``ensure_capacity`` used to snapshot ``free/``, then scan ``held/``, then
create anything neither listing showed. A token that moves between the two
listings appears in neither, and ``O_EXCL`` at a free pathname does not protect
a token of the same name under a holder. The ledger then declared more capacity
than the box was configured for and admitted work against it. ``claim`` calls
``ensure_capacity`` on every poll, so the interleaving overlaps ordinary action
turnover rather than only initialization.
"""

from __future__ import annotations

from pathlib import Path
import sys
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY_A = "a" * 64
KEY_B = "b" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _token_names(ledger: pool.ResourceLedger) -> tuple[list[str], list[str]]:
    """Every token name that is free, and every one that is held."""

    free = sorted(path.name for path in ledger.free_dir.glob("*-*"))
    held = sorted(
        path.name
        for holder in ledger.held_dir.iterdir()
        if holder.is_dir()
        for path in holder.glob("*-*")
    )
    return free, held


def test_turnover_during_a_capacity_scan_mints_no_duplicate(
    queue: pool.PoolQueue,
) -> None:
    """The issue #63 interleaving: release and re-acquire around both scans."""

    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 1})
    assert ledger.acquire(KEY_A, {"cpu": 1})

    original_scan = pool._scan
    original_open = pool.os.open

    def scan(path: Path):
        if path == ledger.held_dir:
            # The free listing is already taken; the held token becomes free
            # while held/ is being scanned, so neither listing shows it.
            ledger.release(KEY_A)
        return original_scan(path)

    def open_token(path, flags, *args, **kwargs):
        if path == ledger.free_dir / "cpu-0000":
            # And another action takes the existing token before the create.
            ledger.acquire(KEY_B, {"cpu": 1})
        return original_open(path, flags, *args, **kwargs)

    with mock.patch.object(pool, "_scan", scan), \
            mock.patch.object(pool.os, "open", open_token):
        ledger.ensure_capacity({"cpu": 1})

    assert ledger.capacity() == {"cpu": 1}
    free, held = _token_names(ledger)
    # The defect showed as one name filed twice, once free and once held.
    assert not set(free) & set(held)
    assert len(free) + len(held) == 1


def test_a_ledger_without_markers_is_adopted_and_not_reminted(
    queue: pool.PoolQueue,
) -> None:
    """Every ledger on the shared store has tokens and no markers yet."""

    ledger = queue.ledger()
    # Build the pre-marker shape by hand: two free tokens and two held.
    ledger.free_dir.mkdir(parents=True, exist_ok=True)
    holder = ledger.held_dir / KEY_A
    holder.mkdir(parents=True, exist_ok=True)
    for name in ("gpu-0000", "gpu-0003"):
        (ledger.free_dir / name).write_bytes(b"")
    for name in ("gpu-0001", "gpu-0002"):
        (holder / name).write_bytes(b"")
    assert not ledger.minted_dir.exists()
    assert ledger.capacity() == {"gpu": 4}

    ledger.ensure_capacity({"gpu": 4})

    assert ledger.capacity() == {"gpu": 4}
    assert ledger.held() == {"gpu": 2}
    assert ledger.available() == {"gpu": 2}
    assert sorted(path.name for path in ledger.minted_dir.iterdir()) == [
        "gpu-0000", "gpu-0001", "gpu-0002", "gpu-0003",
    ]
    free, held = _token_names(ledger)
    assert not set(free) & set(held)


def test_retiring_a_token_gives_back_its_mint_right(
    queue: pool.PoolQueue,
) -> None:
    """A retired index stays retired, and only a retired one is re-minted."""

    ledger = queue.ledger()
    ledger.ensure_capacity({"gpu": 3})
    assert ledger.capacity() == {"gpu": 3}
    assert ledger.retire_free_capacity({"gpu": 1}) == {"gpu": 2}
    assert sorted(path.name for path in ledger.minted_dir.iterdir()) == ["gpu-0000"]

    # A poll that still declares 3 re-mints the retired indices, which is the
    # documented behaviour of a declaration that goes back up.
    ledger.ensure_capacity({"gpu": 3})
    assert ledger.capacity() == {"gpu": 3}
    free, held = _token_names(ledger)
    assert free == ["gpu-0000", "gpu-0001", "gpu-0002"] and held == []


def test_a_missed_adoption_leaves_no_permanent_duplicate(
    queue: pool.PoolQueue,
) -> None:
    """The residual the marker scheme accepts is transient, not permanent.

    Adoption is a scan and can miss a token that is in flight, and the mint
    then creates a second file of that name. The fix accepts that because the
    duplicate is the free copy of a name whose real token is held, and
    ``release`` renames a held token onto ``free/<name>``, replacing it. The
    commit asserts that in prose; this executes it.

    The assertion is the end state, not the duplicate. A later change that
    closes the adoption gap outright should not fail this test, so the
    intermediate count is recorded rather than required.
    """

    ledger = queue.ledger()
    # A ledger from before the markers, with its only token held.
    holder = ledger.held_dir / KEY_A
    holder.mkdir(parents=True, exist_ok=True)
    ledger.free_dir.mkdir(parents=True, exist_ok=True)
    (holder / "cpu-0000").write_bytes(b"")
    assert not ledger.minted_dir.exists()
    assert ledger.capacity() == {"cpu": 1}

    original_scan = pool._scan

    def scan(path: Path):
        # The holder is invisible for the whole call, which is the worst this
        # race can do: adoption misses the token and so does the mint's own
        # held check.
        if path == ledger.held_dir:
            return []
        return original_scan(path)

    with mock.patch.object(pool, "_scan", scan):
        ledger.ensure_capacity({"cpu": 1})

    # Today this is 2, the duplicate the fix documents. Either value is a
    # correct starting point for the assertion that follows.
    assert ledger.capacity()["cpu"] in (1, 2)
    assert ledger.held() == {"cpu": 1}

    # The holder finishes. Nothing removed a token it was using, and the
    # duplicate, if there was one, is gone.
    assert ledger.release(KEY_A) == 1
    assert ledger.capacity() == {"cpu": 1}
    free, held = _token_names(ledger)
    assert free == ["cpu-0000"] and held == []
