"""A submission's pricing read opens only the receipts it has not logged (#1044).

``PoolQueue.move_records`` is what every consumer-row submission prices its
movers from (``pbrun.residency_stage_rows``, ``pbcampaign``'s frozen plans,
``produced_output``'s exporter).  It read every file in ``pb-queue/movers/``
on every call: 8,148 receipts on 2026-09-23, about 3,800 more a day, on an
HDD pool at 60-83% util.  py-spy caught a Stage B row submission 2 min 10 s
into that read, and the whole submission took 7 min 19 s.

The receipt set is still every receipt on disk, decided by one names-only
listing.  What a read must no longer pay for is opening the receipts it has
already logged.  These tests count the opens of files inside ``movers/``.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
#: More receipts than any bound a pricing read may open.
HISTORY = 5000


def _key(index: int) -> str:
    return f"{index:064x}"


def _receipt(index: int) -> dict[str, object]:
    key = _key(index)
    return {
        "schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": key,
        "tier_id": TIER, "consumer_action_key": f"{index // 8:064x}",
        "manifest_sha256": "d" * 64, "complete": True,
        "bytes_staged": 1 << 30, "seconds": 20.0 + index % 7,
        "cpu_seconds": 10.0, "peak_rss_bytes": (1 + index % 3) << 30,
        "mb_per_s_file_side": 400.0, "movers_claimed_on_tier": 2,
        "disk_pacing": {"mean_pool_read_mb_s": 180.0,
                        "pool_read_bytes": 1 << 30},
        "unix": 1_000_000.0 + index,
    }


def _file_directly(queue: pool.PoolQueue, count: int) -> None:
    """Receipts as a writer from before the pricing log filed them."""

    directory = queue.root / pool.MOVERS
    directory.mkdir(parents=True, exist_ok=True)
    for index in range(count):
        (directory / f"{_key(index)}.json").write_text(
            json.dumps(_receipt(index)))


def _count_receipt_opens(monkeypatch: pytest.MonkeyPatch,
                         queue: pool.PoolQueue) -> list[str]:
    """Every read of a file inside ``movers/``, by name."""

    directory = queue.root / pool.MOVERS
    opened: list[str] = []
    real = Path.read_bytes

    def counting(path: Path) -> bytes:
        if path.parent == directory:
            opened.append(path.name)
        return real(path)

    monkeypatch.setattr(Path, "read_bytes", counting)
    return opened


def test_a_second_pricing_read_opens_no_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    _file_directly(queue, HISTORY)
    first = queue.move_records()
    assert len(first) == HISTORY

    opened = _count_receipt_opens(monkeypatch, queue)
    second = queue.move_records()
    assert len(second) == HISTORY
    assert opened == [], (
        f"the second pricing read opened {len(opened)} of {HISTORY} receipts")


def test_a_receipt_filed_since_is_priced_without_opening_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    _file_directly(queue, HISTORY)
    queue.move_records()
    newest = _receipt(HISTORY)
    queue.record_move(str(newest["action_key"]), newest)

    opened = _count_receipt_opens(monkeypatch, queue)
    records = queue.move_records()
    assert len(records) == HISTORY + 1
    assert records[-1]["action_key"] == newest["action_key"]
    assert records[-1]["peak_rss_bytes"] == newest["peak_rss_bytes"]
    assert len(opened) <= 1, (
        f"pricing one new receipt opened {len(opened)} receipt files")
