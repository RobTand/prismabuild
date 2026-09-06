"""The pool's own sweeps must survive an entry that goes away under them.

``offers()`` learned this in #208: an entry that vanishes between the ``glob``
and the read is ordinary, and on NFS that event arrives as ``ESTALE`` through
a directory handle the client had already cached.  ``_read_json`` grew an
opt-in ``tolerate_stale`` for it, and #211 deliberately gave it to ``offers()``
alone.

#212 asks the same question of the three enumerations that walk pool
directories on the worker's side.  The answer is not uniform, and these tests
are where the difference is pinned:

* ``ready_items`` takes the tolerance.  ``None`` there already means "skip
  this entry", so an unreadable one is simply not offered this poll.
* ``quarantine_orphans`` takes it at the call site rather than through the
  flag, because it *discriminates* ``None``: a file that is gone is a race, a
  file that is there and empty is a torn write it files and unlinks.  The
  flag alone would lose the errno and let a live record be filed as empty.
* ``reap_stale`` stays loud.  Its ``None`` decides whether a claim concluded,
  and ``ESTALE`` -- unlike ``ENOENT`` -- can be an atomic *replace* seen
  through a cached handle, which is how ``finish()`` publishes
  ``finish_pending``.  Reading that as "absent" would walk past the guard that
  keeps a completed action from being reaped as ``lease_lost``.

Each function also keeps a test that a genuine I/O error still propagates, so
the tolerance cannot quietly become "the pool is empty".
"""

from __future__ import annotations

import errno
import json
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _raise_on(monkeypatch: pytest.MonkeyPatch, name: str, err: int) -> None:
    """Make ``read_bytes`` fail with ``err`` for one entry, honestly for the rest."""

    real = Path.read_bytes

    def fake(self: Path) -> bytes:
        if self.name == name:
            raise OSError(err, "injected", str(self))
        return real(self)

    monkeypatch.setattr(Path, "read_bytes", fake)


def _write_ready(queue: pool.PoolQueue, key: str) -> Path:
    """A ready record complete enough that ``quarantine_orphans`` leaves it be."""

    path = queue.item_path(pool.READY, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "action_key": key,
                "worker_script": "run.py",
                "cas_root": "/mnt/shared/prismabuild-fleet/cas",
                "checkout_root": "/home/rob/prismabuild",
                "published_unix": time.time(),
                "priority": 0,
            }
        ),
        encoding="utf-8",
    )
    return path


def _write_claim(queue: pool.PoolQueue, key: str, *, claimed_unix: float) -> Path:
    path = queue.item_path(pool.CLAIMED, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "action_key": key,
                "claimed_unix": claimed_unix,
                "claimed_host": "somebox",
                "published_unix": claimed_unix,
            }
        ),
        encoding="utf-8",
    )
    return path


# -- ready_items: tolerant ------------------------------------------------


def test_ready_items_skips_an_entry_that_went_stale(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One vanished entry must not cost the worker the rest of the queue."""

    _write_ready(queue, "alive")
    _write_ready(queue, "gone")
    _raise_on(monkeypatch, "gone.json", errno.ESTALE)

    keys = [str(r.get("action_key")) for r in queue.ready_items()]

    assert keys == ["alive"]


def test_ready_items_still_raises_on_a_genuine_read_error(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tolerating the race must not turn a broken mount into an empty queue."""

    _write_ready(queue, "alive")
    _write_ready(queue, "broken")
    _raise_on(monkeypatch, "broken.json", errno.EIO)

    with pytest.raises(OSError) as caught:
        queue.ready_items()
    assert caught.value.errno == errno.EIO


# -- quarantine_orphans: tolerant, and never destructively ----------------


def test_quarantine_orphans_leaves_a_stale_entry_alone(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The sweep survives the race, and files nothing it could not read.

    This is the assertion the plain ``tolerate_stale`` flag fails.  ``None``
    from the flag is indistinguishable from an empty file, and the sweep's
    next line asks ``path.exists()`` -- which answers ``True`` for a record
    that is still on disk.  A perfectly good ready item would be filed as
    ``unreadable`` and unlinked.  (``Path.exists`` also re-raises ``ESTALE``:
    ``pathlib._ignore_error`` covers ``ENOENT/ENOTDIR/EBADF/ELOOP`` only, so
    the flag would not even keep the sweep alive.)
    """

    _write_ready(queue, "orphan")          # no worker_script: genuinely unusable
    queue.item_path(pool.READY, "orphan").write_text(
        json.dumps({"action_key": "orphan"}), encoding="utf-8"
    )
    stale = _write_ready(queue, "stale")
    _raise_on(monkeypatch, "stale.json", errno.ESTALE)

    filed = queue.quarantine_orphans()

    assert filed == ["orphan"]
    assert stale.exists(), "a record the sweep could not read must survive it"
    assert not queue.item_path(pool.FAILED, "stale").exists()


def test_quarantine_orphans_still_files_a_truly_empty_record(
    queue: pool.PoolQueue,
) -> None:
    """The torn-write case the sweep exists for is unchanged."""

    torn = queue.item_path(pool.READY, "torn")
    torn.parent.mkdir(parents=True, exist_ok=True)
    torn.write_bytes(b"")

    assert queue.quarantine_orphans() == ["torn"]
    assert queue.item_path(pool.FAILED, "torn").exists()


def test_quarantine_orphans_still_raises_on_a_genuine_read_error(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only the race is tolerated; a broken mount is still an error."""

    _write_ready(queue, "broken")
    _raise_on(monkeypatch, "broken.json", errno.EIO)

    with pytest.raises(OSError) as caught:
        queue.quarantine_orphans()
    assert caught.value.errno == errno.EIO


# -- reap_stale: deliberately loud ---------------------------------------


def test_reap_stale_still_raises_on_a_stale_claim_read(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reaper's ``None`` is a verdict, so it is not allowed to be a guess.

    ``finish()`` publishes ``finish_pending`` by atomically *replacing*
    ``claimed/<key>.json`` (``pool.py`` ~L3876), and a replace seen through a
    cached handle is exactly what returns ``ESTALE`` -- a state ``ENOENT``
    cannot produce.  The ``finish_pending`` guard sits *before* the lease
    check (~L2921 vs ~L2939), because a payload awaiting container cleanup is
    no longer heartbeating: ``execute`` refreshes the lease only while the
    child runs.  So an expired lease is the designed steady state there, and a
    tolerated ``ESTALE`` on the first read would walk straight past the guard
    and reap a completed action as ``lease_lost``.

    ``reap_stale`` therefore keeps the loud default, and this test is the
    record of that decision rather than an omission.  See #212.
    """

    _write_claim(queue, "claimkey", claimed_unix=time.time() - 10_000)
    _raise_on(monkeypatch, "claimkey.json", errno.ESTALE)

    with pytest.raises(OSError) as caught:
        queue.reap_stale()
    assert caught.value.errno == errno.ESTALE
