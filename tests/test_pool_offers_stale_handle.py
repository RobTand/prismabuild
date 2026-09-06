"""One removed worker offer must not take a submission down with it.

``offers()`` enumerates ``workers/*.json`` on a shared filesystem whose
entries are expected to appear and disappear -- a worker registration is
exactly that.  An entry that vanishes between the ``glob`` and the read is
ordinary, and ``_read_json`` already answers ``None`` for it when the read
comes back ``ENOENT``.

On NFS the same event arrives as ``ESTALE`` through a directory handle the
client had already cached.  Untreated, it left ``_read_json``, ``offers()``
and ``placeable_hosts()`` and killed a ``pbrun`` submission before it queued
anything: a whole ``pbtest`` shard, rc=1, 74 tests never run, for two offer
files an operator had tidied away (issue #208).

The tolerance is opt-in and belongs only where the vanishing is normal.  In
``offers()`` the ``glob`` has already succeeded, so a per-file ``ESTALE``
after it can only be the race.  A caller addressing one record by key has no
such evidence -- there ``ESTALE`` may equally be a dead mount, and answering
"the record is absent" would be a confident wrong verdict -- so the default
still raises.  These tests pin all three edges: the race is tolerated, every
other errno is not, and the default is unchanged.
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


def _write_offer(root: Path, host: str) -> Path:
    directory = root / pool.WORKERS
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{host}.json"
    path.write_text(
        json.dumps(
            {
                "host": host,
                "tags": [],
                "has_gpu": False,
                "capacity": {"cpu": 4},
                "announced_unix": time.time(),
            }
        ),
        encoding="utf-8",
    )
    return path


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


def test_offers_skips_an_offer_that_went_stale(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The surviving box is still placeable when its neighbour's file vanishes."""

    _write_offer(queue.root, "alive")
    _write_offer(queue.root, "gone")
    _raise_on(monkeypatch, "gone.json", errno.ESTALE)

    live = queue.offers()

    assert [str(o.get("host")) for o in live] == ["alive"]


def test_offers_still_raises_on_a_genuine_read_error(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tolerating the race must not turn every I/O failure into an empty pool."""

    _write_offer(queue.root, "alive")
    _write_offer(queue.root, "broken")
    _raise_on(monkeypatch, "broken.json", errno.EIO)

    with pytest.raises(OSError) as caught:
        queue.offers()
    assert caught.value.errno == errno.EIO


def test_read_json_still_raises_on_stale_by_default(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A caller addressing one record by key is told, not quietly told nothing."""

    path = _write_offer(queue.root, "addressed")
    _raise_on(monkeypatch, "addressed.json", errno.ESTALE)

    with pytest.raises(OSError) as caught:
        pool._read_json(path)
    assert caught.value.errno == errno.ESTALE


def test_placeable_hosts_survives_a_stale_offer(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The path the submission actually took: ``pin_notice`` reaches this."""

    _write_offer(queue.root, "alive")
    _write_offer(queue.root, "gone")
    _raise_on(monkeypatch, "gone.json", errno.ESTALE)

    hosts = queue.placeable_hosts({"tags": [], "resources": {"cpu": 1}})

    assert hosts == ["alive"]
