"""A corrupt gang refuses itself without removing other gangs' fences (#1521)."""
from __future__ import annotations

import json

import pytest

from test_gang_reservation_1517 import HOSTS, gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401

from prismabuild import _gang, _measurement_reservation as reservation, pool


def _damage(queue, group, fault):
    path = _gang.group_path(queue, group)
    reader = lambda: _gang.read_group(queue, group)
    if fault == "missing_publication":
        record = json.loads(path.read_text())
        del record["members"][0]["published_unix"]
        path.write_text(json.dumps(record))
    elif fault == "election_json":
        path = _gang.state_dir(queue, group) / "elect-0.json"
        path.write_bytes(b"{")
        reader = lambda: _gang.elections(queue, group, 2)
    elif fault == "teardown_json":
        path = _gang.state_dir(queue, group) / "teardown.json"
        path.write_bytes(b"{")
        reader = lambda: _gang.teardown(queue, group)
    elif fault == "duplicate_key":
        path.write_bytes(b'{"schema":"first","schema":"second"}')
    else:
        path.write_bytes(b"{")
    return reader


@pytest.mark.parametrize("fault", [
    "group_json", "missing_publication", "election_json", "teardown_json", "duplicate_key",
])
@pytest.mark.parametrize("scan", [reservation._scan_publications, reservation._scan_election_refresh])
def test_bad_gang_isolated_from_healthy_gang_census(gang_fleet, fault, scan):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    healthy, healthy_keys = members("healthy", priority=10)
    damaged, damaged_keys = members("damaged", priority=20)
    for group in (healthy, damaged):
        record = _gang.read_group(queue, group)
        for entry, host in zip(record["members"], HOSTS):
            _gang.elect_gang_member(queue, record, entry, host, clock.now)
    ordinary = publish("ordinary-census-row", cpu=1, gpu=0, mem_gb=1)
    read_damaged = _damage(queue, damaged, fault)

    census = scan(queue)

    assert set(census["gang_elections"]) == set(healthy_keys)
    assert {row["host"] for row in census["gang_elections"].values()} == set(HOSTS)
    assert all(row["member_state"] == "ready" for row in census["gang_elections"].values())
    assert queue.records.item_path(pool.record_store.READY, ordinary).exists()
    assert all(queue.records.item_path(pool.record_store.READY, key).exists()
               for key in damaged_keys)
    with pytest.raises(_gang.GangContractError):
        read_damaged()


@pytest.mark.parametrize("scan", [reservation._scan_publications, reservation._scan_election_refresh])
def test_unreadable_gang_directory_keeps_census_closed(gang_fleet, monkeypatch, scan):
    queue = gang_fleet[0]
    original = reservation.os.scandir

    def cannot_list(path):
        if path == _gang.root(queue):
            raise PermissionError("gang directory unavailable")
        return original(path)

    monkeypatch.setattr(reservation.os, "scandir", cannot_list)
    with pytest.raises(PermissionError, match="gang directory unavailable"):
        scan(queue)
