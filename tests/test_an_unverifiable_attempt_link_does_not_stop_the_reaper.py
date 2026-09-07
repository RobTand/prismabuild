"""A finish tombstone whose attempt links are gone is filed, not raised over.

``sweep_finish_tombstones`` states the hazard it exists to avoid, in its own
docstring:

    A record whose attempt links no longer verify is filed rather than
    restored.  Restoring it would hand ``reap_stale`` a record that raises
    from ``archive_attempt``, and that exception stops reaping on every box
    for as long as the record exists.

It implements that with::

    if restorable and "attempt_history" in record:
        try:
            self.attempt_outcomes(record)
        except PoolContractError:
            restorable = False

``attempt_outcomes`` raises ``PoolContractError`` for everything it can decide
by *reading* the record -- a non-contiguous attempt number, a link that is not
its canonical path, an outcome that is not a JSON object.  But the link it
checks last is a file, read through ``pb._read_regular_file_nofollow``, and
``core._open_regular_nofollow`` raises three types that are not
``PoolContractError`` at all: a bare ``FileNotFoundError`` when the immutable
attempt outcome is absent, ``CASUnavailableError`` for every other ``OSError``
on the stat (ESTALE, EACCES, EIO on ``/mnt/shared``), and ``CASTamperError``
when the entry is not a regular readonly file.  All three derive from
``PrismaBuildError(RuntimeError)``; ``PoolContractError`` derives from
``PoolError`` and ``ValueError``.  They are disjoint.

So the guard misses the most literal reading of its own sentence.  "The attempt
link no longer verifies" *because the file it names is not there* is exactly
``FileNotFoundError``, and it escapes.

Where it escapes to is the whole cost.  ``sweep_finish_tombstones`` is called
unguarded from the tail of ``reap_stale``, and ``reap_stale`` is called from
``serve_once`` at ``pool.py:5747`` -- **before** ``claim`` and outside every
``try`` in that method.  One tombstone whose attempts directory this box cannot
resolve therefore stops the box from claiming any work at all, on every pass,
for as long as the file exists: precisely the fleet-wide reaping stall the
docstring says the guard prevents, arriving through the door the guard does not
cover.  It is the same shape as the ``ready/`` incident recorded at
``quarantine_orphans`` -- one unreadable file stopping every consumer on every
box until somebody deleted it by hand.

The fix keeps the module's distinction between absence and inability to look.
``FileNotFoundError`` and ``CASTamperError`` are positive evidence that the
link does not verify, so they file the record like a ``PoolContractError``.
``CASUnavailableError`` is *"could not look"*: it neither restores nor files,
because filing moves the record into ``withdrawn/superseded/``, which is
invisible to every reader, and doing that on a stale handle would turn a
transient mount blip into an action nobody can ever find.  It leaves the
tombstone for the next sweep, which is the retaining direction.

Pre-fix failure line: ``src/prismabuild/pool.py:3849`` at ``67a44bb``,
``except PoolContractError:`` as the only handler around
``self.attempt_outcomes(record)`` at ``3848``.
"""
from __future__ import annotations

import errno
import json
from pathlib import Path
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

KEY = "f" * 64


def _stranded_tombstone(
    tmp_path: Path, *, with_history: bool = True
) -> tuple[pool.PoolQueue, Path]:
    """A finisher that died mid-publish, with one archived attempt linked."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.publish(
        action_key=KEY, cas_root=queue.root / "cas",
        checkout_root=queue.root / "co", worker_script=queue.root / "worker.py",
        resources={"cpu": 1}, max_attempts=2, retry_safe=True,
    )
    item = queue.claim(capacity={"cpu": 1})
    assert item is not None
    claimed_path = queue.item_path(pool.CLAIMED, KEY)
    record = json.loads(claimed_path.read_text())
    if with_history:
        record["attempts"] = 1
        record["attempt_history"] = [
            {
                "attempt": 1,
                "outcome": str(
                    queue.attempt_path(record, 1).relative_to(queue.root)
                ),
            }
        ]
        # The generation directory exists -- this is a real archived attempt --
        # but the immutable outcome the record links is not readable here.
        queue.attempt_path(record, 1).parent.mkdir(parents=True, exist_ok=True)

    # The window sweep_finish_tombstones is the only reader of: the claim has
    # been moved aside and its next home was never published.
    tombstone = (
        queue.dir(pool.CLAIMED)
        / f"{KEY}.{int((pool._now() - pool.LEASE_TIMEOUT_S * 2) * 1_000_000)}"
        f"{pool.TOMBSTONE_SUFFIX}"
    )
    pool._write_json_atomic(tombstone, record)
    claimed_path.unlink(missing_ok=True)
    queue.lease_path(KEY).unlink(missing_ok=True)
    return queue, tombstone


def test_a_missing_attempt_outcome_does_not_escape_the_sweep(
    tmp_path: Path,
) -> None:
    """The sweep files the record instead of raising out of ``reap_stale``."""

    queue, tombstone = _stranded_tombstone(tmp_path)

    # ``swept`` names every tombstone this pass disposed of, restored or filed.
    assert queue.sweep_finish_tombstones() == [KEY]

    assert not tombstone.exists(), "the tombstone was neither filed nor removed"
    assert not queue.item_path(pool.CLAIMED, KEY).exists(), (
        "a record whose attempt links do not verify was restored anyway"
    )
    assert list(queue.superseded_dir().glob(f"{KEY}.*.finish-tombstone.json")), (
        "the record was deleted rather than filed as evidence"
    )


def test_a_tampered_attempt_outcome_does_not_escape_the_sweep(
    tmp_path: Path,
) -> None:
    """``CASTamperError`` is evidence too: the link does not verify."""

    queue, tombstone = _stranded_tombstone(tmp_path)
    record = json.loads(tombstone.read_text())
    # A directory where the immutable outcome should be: not a regular file.
    queue.attempt_path(record, 1).mkdir(parents=True, exist_ok=True)

    assert queue.sweep_finish_tombstones() == [KEY]
    assert not tombstone.exists()
    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert list(queue.superseded_dir().glob(f"{KEY}.*.finish-tombstone.json"))


def test_an_unreadable_attempt_outcome_retains_the_tombstone(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """"Could not look" is neither a restore nor a file.

    Filing moves the record into ``withdrawn/superseded/``, which the module
    documents as invisible to every reader.  Doing that because a directory
    handle went stale would lose the action permanently, so the sweep leaves
    the tombstone where the next pass -- or an operator -- can still see it.
    """

    queue, tombstone = _stranded_tombstone(tmp_path)

    def unavailable(*args, **kwargs):
        raise pb.CASUnavailableError("cannot inspect pool attempt outcome")

    monkeypatch.setattr(pb, "_read_regular_file_nofollow", unavailable)

    assert queue.sweep_finish_tombstones() == []
    assert tombstone.exists(), "a stale handle discarded the record"
    assert not queue.item_path(pool.CLAIMED, KEY).exists()


def test_a_sound_tombstone_is_still_restored(tmp_path: Path) -> None:
    """The regression guard: the restore path is untouched.

    A tombstone with no ``attempt_history`` never reaches the verification at
    all, so it isolates the disposition this change must not alter.
    """

    queue, tombstone = _stranded_tombstone(tmp_path, with_history=False)

    assert queue.sweep_finish_tombstones() == [KEY]
    assert queue.item_path(pool.CLAIMED, KEY).exists()
    assert not tombstone.exists()


def test_the_missing_outcome_really_raises_from_attempt_outcomes(
    tmp_path: Path,
) -> None:
    """The escape this test file is about, at its source.

    ``attempt_outcomes`` is documented as verifying the links; what it raises
    when a link does not resolve is an ``OSError``, not the queue's contract
    error, and that is the whole reason the sweep's handler was too narrow.
    """

    queue, tombstone = _stranded_tombstone(tmp_path)
    record = json.loads(tombstone.read_text())
    with pytest.raises(OSError) as raised:
        queue.attempt_outcomes(record)
    assert raised.value.errno == errno.ENOENT
    assert not isinstance(raised.value, pool.PoolContractError)
