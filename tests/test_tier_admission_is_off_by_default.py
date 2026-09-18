"""Nothing on the fleet today asks a tier for anything, and it must stay that way (#583).

Tier reservation is opt-in.  An item that carries no ``<kind>@<tier>`` demand
and no ``residency`` block takes the claim path it took before this change: no
tier ledger is created, no tier token is taken or returned, no verdict is
written onto the record, and the record that reaches ``done/`` has the same
field set it had.  The release gate for flipping any of this on is a measured
campaign result, so the cheap thing to prove first is that merging the code
changes nothing.

The shape in :func:`test_the_running_campaigns_shape_still_admits` is copied
from the live claim record of the sealed GLM-5.3-Flash prepare
``8b53c37c09e9…`` on 2026-09-17 (``resources`` ``{"cpu": 6, "gpu": 1,
"mem_gb": 101}``, tags ``progress-helper-v1``/``progress-v1``/``sparky``,
``needs_gpu`` true).  It runs against a pool under ``tmp_path``; nothing here
reads, writes or touches the live queue.
"""

from __future__ import annotations

import errno
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

KEY = "c" * 64
#: The live row's demand, tags and GPU flag, 2026-09-17.
CAMPAIGN_RESOURCES = {"cpu": 6, "gpu": 1, "mem_gb": 101}
CAMPAIGN_TAGS = ["progress-helper-v1", "progress-v1", "sparky"]


@pytest.fixture()
def queue(tmp_path: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(tmp_path / "pb-queue")
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str, resources: dict[str, int], **kw: object) -> None:
    q.publish(
        action_key=key,
        cas_root=q.root / "cas",
        checkout_root=q.root / "co",
        worker_script=q.root / "worker.py",
        resources=resources,
        **kw,
    )


def test_an_item_without_tier_demand_never_touches_a_tier_ledger(
    queue: pool.PoolQueue,
) -> None:
    _publish(queue, KEY, {"cpu": 1, "mem_gb": 2})
    item = pool._read_json(queue.item_path(pool.READY, KEY))
    assert item is not None and "residency" not in item

    claimed = queue.claim(owner="worker", capacity={"cpu": 2, "mem_gb": 4})
    assert claimed is not None and claimed["action_key"] == KEY
    # The two fields this change can add are both absent, so a reader of the
    # record cannot tell the branch is in the code.
    assert "tier_reservations" not in claimed
    assert "residency_verdict" not in claimed
    assert claimed["reserved_on"] == pool.socket.gethostname()
    assert queue.ledger().held() == {"cpu": 1, "mem_gb": 2}
    # No tier ledger was created by admission: the root exists because
    # ``ensure_layout`` makes it, and it is empty.
    assert queue.tier_ids() == []
    assert queue.tier_holdings(KEY) == {}

    queue.finish(KEY, status="executed", claim_snapshot=claimed)
    assert queue.ledger().held() == {}
    assert queue.tier_ids() == []
    done = pool._read_json(queue.item_path(pool.DONE, KEY))
    assert done is not None and done["status"] == "executed"
    assert "tier_reservations" not in done and "residency_verdict" not in done


def test_the_running_campaigns_shape_still_admits(queue: pool.PoolQueue) -> None:
    """The sealed GLM-5.3-Flash prepare's exact demand claims unchanged.

    An admission change that refused or re-placed a running action would be a
    defect, not a feature.  The live row is reproduced field for field on a
    sparky-shaped worker; it claims, reserves on the host ledger only, and
    concludes.
    """

    _publish(queue, KEY, CAMPAIGN_RESOURCES, tags=CAMPAIGN_TAGS, needs_gpu=True,
             max_attempts=1, retry_safe=False)
    claimed = queue.claim(
        owner="sparky-worker-0",
        capacity={"cpu": 20, "gpu": 1, "mem_gb": 121},
        tags=CAMPAIGN_TAGS,
        has_gpu=True,
    )
    assert claimed is not None and claimed["action_key"] == KEY
    assert claimed["resources"] == CAMPAIGN_RESOURCES
    assert claimed["reserved_on"] == pool.socket.gethostname()
    assert "tier_reservations" not in claimed
    assert "residency_verdict" not in claimed
    assert queue.ledger().held() == CAMPAIGN_RESOURCES
    assert queue.tier_ids() == []
    queue.finish(KEY, status="executed", claim_snapshot=claimed)
    assert queue.ledger().held() == {}


def test_the_residency_gate_is_inert_without_a_residency_block(
    queue: pool.PoolQueue,
) -> None:
    """The gate reads one absent field and says so; it cannot refuse anything."""

    _publish(queue, KEY, {"cpu": 1})
    item = pool._read_json(queue.item_path(pool.READY, KEY))
    assert item is not None
    assert queue.residency_verdict(item) == {"state": "not_requested"}
    # ...and on a record that predates the field entirely.
    assert queue.residency_verdict({"action_key": KEY}) == {"state": "not_requested"}
    assert queue.residency_verdict({"action_key": KEY, "residency": None}) == {
        "state": "not_requested"}


def _tier_reads_fail(monkeypatch: pytest.MonkeyPatch, queue: pool.PoolQueue) -> None:
    """Make every readdir of the tier root fail the way a cold NFS handle does.

    Only that directory: a host ledger's scan must keep failing loudly, and a
    test that broke both would not show which one the conclusion depends on.
    """

    real = pool._scan
    target = (queue.root / pool.TIER_RESERVATIONS).resolve()

    def scan(directory: Path):
        if Path(directory).resolve() == target:
            raise OSError(errno.ESTALE, "stale NFS file handle")
        return real(directory)

    monkeypatch.setattr(pool, "_scan", scan)


def _uncontain(monkeypatch: pytest.MonkeyPatch) -> None:
    """Mutate the driver: put back the unguarded scan the fix contains."""

    monkeypatch.setattr(pool.PoolQueue, "tier_ids", lambda self: sorted(
        directory.name for directory in pool._scan(self.root / pool.TIER_RESERVATIONS)
        if directory.is_dir() and not directory.name.startswith(".")))


def test_an_unreadable_tier_root_does_not_cost_an_action_its_outcome(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`finish` entombs the claim and unlinks the lease before it writes the
    outcome, so anything that raises in between loses the ending.  The tier
    release sits in that window and reads a shared directory nothing else
    touches while the feature is off.
    """

    _publish(queue, KEY, {"cpu": 1, "mem_gb": 2})
    claimed = queue.claim(owner="worker", capacity={"cpu": 2, "mem_gb": 4})
    assert claimed is not None
    _tier_reads_fail(monkeypatch, queue)

    queue.finish(KEY, status="executed", claim_snapshot=claimed)

    done = pool._read_json(queue.item_path(pool.DONE, KEY))
    assert done is not None and done["status"] == "executed"
    assert not queue.item_path(pool.CLAIMED, KEY).exists()
    assert not queue.lease_path(KEY).exists()
    # The host tokens went back: the host ledger was never the broken one.
    assert queue.ledger().held() == {}
    # And the helpers answer emptily rather than raising.
    assert queue.tier_ids() == []
    assert queue.release_tier_reservations(KEY) == 0


def test_without_the_containment_that_finish_loses_the_outcome(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bite test: the unguarded scan is what costs the ending."""

    _publish(queue, KEY, {"cpu": 1})
    claimed = queue.claim(owner="worker", capacity={"cpu": 4})
    assert claimed is not None
    _tier_reads_fail(monkeypatch, queue)
    _uncontain(monkeypatch)

    with pytest.raises(OSError):
        queue.finish(KEY, status="executed", claim_snapshot=claimed)
    # Claim entombed, lease gone, and no outcome filed anywhere.
    assert pool._read_json(queue.item_path(pool.DONE, KEY)) is None
    assert pool._read_json(queue.item_path(pool.FAILED, KEY)) is None
    assert not queue.lease_path(KEY).exists()


def test_an_unreadable_tier_root_does_not_abort_a_reaper_cycle(
    queue: pool.PoolQueue, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`serve_once` reaps before every claim, so a persistent failure here
    would stop the poll on every box for as long as it persisted.
    """

    _publish(queue, KEY, {"cpu": 1})
    claimed = queue.claim(owner="worker", capacity={"cpu": 4})
    assert claimed is not None
    _tier_reads_fail(monkeypatch, queue)

    # Every walk that now reads the tier root returns instead of raising.
    assert isinstance(queue.reap_stale(timeout_s=0.0), list)
    assert queue.sweep_stale_acquisitions(grace_s=0.0) == []
    assert isinstance(queue.sweep_widowed_leases(timeout_s=0.0), list)

    # ...and each of them raises once the containment is mutated out, so the
    # assertions above are load-bearing rather than vacuous.
    _uncontain(monkeypatch)
    with pytest.raises(OSError):
        queue.reap_stale(timeout_s=0.0)
    with pytest.raises(OSError):
        queue.sweep_stale_acquisitions(grace_s=0.0)
