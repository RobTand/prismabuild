"""Additional interrupted-claim and strict physical-census boundaries."""
import json
from concurrent.futures import ThreadPoolExecutor, wait

import pytest

from test_measurement_election_cleanup_2181 import _elected
from test_measurement_drains_gpu_backfill import fleet as fleet_fixture
from test_measurement_reservation_census_1419 import _legacy_claim
from prismabuild import _measurement_reservation as reservation
from prismabuild import lifecycle, pool, storage_tiers

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


@pytest.mark.parametrize("field,value", [("action_key", "f" * 64), ("schema", "unknown-lease")])
def test_contradictory_lease_cannot_recover_consumption(fleet, field, value):
    queue, _, _, _, _, _, claim, _ = fleet
    _, key, _, sidecar = _elected(fleet)
    assert claim("sparky") == key
    path = queue.records.passes_path(key)
    path.write_bytes(sidecar)
    leasepath = queue.records.lease_path(key)
    original = json.loads(leasepath.read_bytes())
    altered = {**original, field: value}
    queue.records.replace_record_at(leasepath, altered)

    queue.sweep_orphan_passes(limit=16)
    assert path.read_bytes() == sidecar
    queue.records.replace_record_at(leasepath, original)
    queue.sweep_orphan_passes(limit=16)
    assert reservation.FIELD not in json.loads(path.read_bytes())


@pytest.mark.parametrize("private", [False, True])
def test_ram_host_mirror_retains_concluded_sidecar(fleet, private):
    queue = fleet[0]
    _, key, _, sidecar = _elected(fleet)
    queue.withdraw(key, reason="RAM host mirror")
    path = queue.records.passes_path(key)
    path.write_bytes(sidecar)
    ledger = queue.ledger()
    tier = queue.tier("ram:" + str(ledger.host))
    assert tier._hold_host(key, 1) == ("taken", "")
    holder = storage_tiers.RAM_HOST_MEMORY_PREFIX + key
    assert ledger.holder_tokens(holder) == {"mem_gb": 1}
    if private:
        handle = "claiming.123." + holder + ".test-host.1.1"
        (ledger.held_dir / holder).rename(ledger.held_dir / handle)
        holder = handle

    assert key not in queue.sweep_orphan_passes(limit=16)
    assert path.read_bytes() == sidecar
    ledger.release(holder)
    assert key in queue.sweep_orphan_passes(limit=16)


@pytest.mark.parametrize("namespace", ["root", "owner", "held"])
def test_dangling_physical_namespace_retains_concluded_sidecar(fleet, tmp_path, namespace):
    queue = fleet[0]
    _, key, _, sidecar = _elected(fleet)
    queue.withdraw(key, reason="dangling physical namespace")
    path = queue.records.passes_path(key)
    path.write_bytes(sidecar)
    malformed = queue.root / pool.record_store.TIER_RESERVATIONS
    if namespace != "root":
        malformed = queue.root / pool.RESERVATIONS / "unknown-owner"
        if namespace == "held":
            malformed.mkdir()
            malformed /= "held"
    backup = tmp_path / "saved-namespace"
    existed = malformed.exists()
    if existed:
        malformed.rename(backup)
    malformed.symlink_to(tmp_path / "missing-target", target_is_directory=True)

    assert key not in queue.sweep_orphan_passes(limit=16)
    assert path.read_bytes() == sidecar
    malformed.unlink()
    if existed:
        backup.rename(malformed)
    assert key in queue.sweep_orphan_passes(limit=16)


def test_defer_serializes_with_final_host_census(fleet, monkeypatch):
    queue, _, _, _, publish, tick, claim, _ = fleet
    incumbent, key, _, _ = _elected(fleet)
    queue.finish(incumbent, status="executed")
    tick(11)
    assert claim() == key
    record = pool.record_store.value_or_raise(queue.records.item(pool.record_store.CLAIMED, key))
    lower = publish("lower-races-defer", priority=-10)
    candidate = pool.record_store.value_or_raise(queue.records.item(pool.record_store.READY, lower))
    original = reservation.CensusReader.capture
    calls = []
    futures = []
    with ThreadPoolExecutor(max_workers=1) as executor:
        def capture(reader):
            result = original(reader)
            calls.append(result)
            if len(calls) == 2:
                assert key not in result["selections"]
                future = executor.submit(queue._defer_unstarted_claim, record)
                futures.append(future)
                wait([future], timeout=2)
            return result
        monkeypatch.setattr(reservation.CensusReader, "capture", capture)
        admitted = _legacy_claim(pool.PoolQueue(queue.root), candidate)
        for future in futures:
            future.result(timeout=10)
    assert len(calls) == 2
    assert admitted is None, "lower work must not exploit release after a stale final census"
    # A contended handoff may defer processing rather than release authority.
    # Once host admission is free the canonical retry must restore READY and
    # keep actual lower admission fenced by the fresh election.
    monkeypatch.setattr(reservation.CensusReader, "capture", original)
    if queue.records.item_path(pool.record_store.CLAIMED, key).exists():
        queue._defer_unstarted_claim(record)
    assert queue.records.item_path(pool.record_store.READY, key).exists()
    assert reservation.FIELD in json.loads(queue.records.passes_path(key).read_bytes())
    assert _legacy_claim(pool.PoolQueue(queue.root), candidate) is None
