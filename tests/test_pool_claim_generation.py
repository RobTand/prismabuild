"""A claim records the bytes the rename actually moved.

``publish`` writes ``ready/<key>.json`` unconditionally, so a re-submission can
replace a record a scan has already read.  The rename then moves the new
generation into ``claimed``, and every guard past the rename reads it back from
there for exactly that reason -- but the claim itself was rebuilt from the
scan's copy, which wrote the replaced generation back over it.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY_A = "a" * 64


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str, **kw: object) -> None:
    q.publish(
        action_key=key,
        cas_root=kw.pop("cas_root", "/cas"),
        checkout_root=kw.pop("checkout_root", "/co"),
        worker_script=kw.pop("worker_script", "/w.py"),
        **kw,
    )


def test_the_claim_records_the_bytes_the_rename_moved(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A submission landing between the scan and the rename is what is claimed.

    ``publish`` writes ``ready/<key>.json`` unconditionally, so a re-submission
    can replace the record a scan already read.  The rename then moves the new
    generation into ``claimed``, and every guard past the rename reads it back
    from there for exactly that reason.  Rebuilding the claim from the scan's
    copy wrote the old generation over it, so the claim, its lease and its
    eventual outcome all named a generation nobody had asked to run, and the
    waiter on the new one never saw an ending.
    """

    _publish(queue, KEY_A, priority=0)
    first = json.loads(
        queue.item_path(pool.READY, KEY_A).read_text(encoding="utf-8")
    )

    real_rename = os.rename

    def resubmit_then_rename(src: object, dst: object) -> None:
        if str(src).endswith(f"{pool.READY}/{KEY_A}.json"):
            _publish(queue, KEY_A, priority=7)
        return real_rename(src, dst)

    monkeypatch.setattr(pool.os, "rename", resubmit_then_rename)
    claimed = queue.claim()
    assert claimed is not None

    on_disk = json.loads(
        queue.item_path(pool.CLAIMED, KEY_A).read_text(encoding="utf-8")
    )
    assert on_disk["priority"] == 7
    assert on_disk["published_unix"] > first["published_unix"]
    # The caller executes what the queue recorded, so the two must agree.
    assert claimed["priority"] == 7
    assert claimed["published_unix"] == on_disk["published_unix"]
