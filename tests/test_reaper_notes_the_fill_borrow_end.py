"""The reaper's terminal filing also notes how a fill borrow ended (#999).

``finish`` annotates ``tier_fill_borrowed`` before the export's tokens go
back, so a filed ending names the lenders that released early.  A claim the
reaper concludes never passes through ``finish``: the reaper files the
terminal record itself, from the claim record the borrow was persisted into
at claim time.  Filing it without the note hides the overcommit interval the
note exists to name -- the ending is the last writer that can attribute it.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import uuid

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402
from admitted_queue_fixture import AdmittedQueueFixture  # noqa: E402

KEY = uuid.uuid4().hex + uuid.uuid4().hex
TIER = "prismabuild-stage:dl380g10"


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = AdmittedQueueFixture(
        pool.PoolQueue(tmp_path / "pb-queue"), capacity={"cpu": 2, "mem_gb": 4},
        default_demand={"cpu": 1, "mem_gb": 1})
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


def test_a_reaped_terminal_claim_notes_its_fill_borrow_end(
    queue: pool.PoolQueue,
) -> None:
    """A lease-lost claim filed as failed carries the borrow-end note.

    The claim record keeps the ``tier_fill_borrowed`` the claim persisted;
    the reaper that concludes the claim files that record, so it is the
    reaper that must ask the tier ledger which lenders are gone.  A tier no
    ledger answers is named unread; a lender holding nothing is released
    with the overcommit it left.  Either way the filed entry is annotated,
    which is the observable contract -- on main the reaper files the borrow
    silently.
    """

    _publish(queue, KEY, max_attempts=1)
    assert queue.claim() is not None
    path = queue.item_path(pool.CLAIMED, KEY)
    record = json.loads(path.read_bytes())
    record["tier_fill_borrowed"] = {
        TIER: {
            "lent": {"lender-a": 512},
            "borrowed": 1024,
            "taken_free": 2048,
        }
    }
    path.write_text(json.dumps(record))

    assert queue.reap_stale(timeout_s=-1.0) == [KEY]
    filed = json.loads(queue.item_path(pool.FAILED, KEY).read_bytes())
    assert filed["status"] == "lease_lost_max_attempts"
    entry = filed["tier_fill_borrowed"][TIER]
    assert "checked_unix" in entry, filed["tier_fill_borrowed"]
    named = (entry.get("lenders_released_before_end", [])
             + entry.get("lenders_unread", []))
    assert named == ["lender-a"], entry
    # The lender holds nothing in this private queue, so it counts as
    # released with the full lent amount as its overcommit.
    assert entry.get("lenders_released_before_end") == ["lender-a"]
    assert entry.get("overcommit_mb_s") == 512
