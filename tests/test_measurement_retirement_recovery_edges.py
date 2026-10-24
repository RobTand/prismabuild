"""Additional interrupted-claim and strict physical-census boundaries."""
import json

import pytest

from test_measurement_election_cleanup_2181 import _elected
from test_measurement_drains_gpu_backfill import fleet as fleet_fixture
from prismabuild import lifecycle, pool

fleet = fleet_fixture


def test_uncommitted_renamed_measurement_recovers_without_claim_metadata(fleet):
    queue = fleet[0]
    _, key, ready, sidecar = _elected(fleet)
    source = queue.records.item_path(pool.record_store.READY, key)
    source.rename(queue.records.item_path(pool.record_store.CLAIMED, key))
    record = json.loads(ready)
    assert "claimed_unix" not in record and "claimed_host" not in record

    lifecycle._expired_claim(queue, lifecycle.ExpiredClaim(
        key, record, record, "sparklina", None, set()))

    assert source.exists()
    assert not queue.records.item_path(pool.record_store.CLAIMED, key).exists()
    assert queue.records.passes_path(key).read_bytes() == sidecar


@pytest.mark.parametrize("namespace", ["root", "held"])
def test_malformed_physical_namespace_retains_concluded_election(fleet, tmp_path, namespace):
    queue = fleet[0]
    _, key, _, sidecar = _elected(fleet)
    queue.withdraw(key, reason="strict physical census")
    path = queue.records.passes_path(key)
    path.write_bytes(sidecar)
    if namespace == "root":
        malformed = queue.root / pool.record_store.TIER_RESERVATIONS
    else:
        malformed = queue.root / pool.RESERVATIONS / "unknown-owner" / "held"
        malformed.parent.mkdir(parents=True)
    backup = tmp_path / "namespace-backup"
    existed = malformed.exists()
    if existed:
        malformed.rename(backup)
    malformed.write_text("not a directory")

    assert key not in queue.sweep_orphan_passes(limit=16)
    assert path.read_bytes() == sidecar

    malformed.unlink()
    if existed:
        backup.rename(malformed)
    assert key in queue.sweep_orphan_passes(limit=16)
    assert not path.exists()
