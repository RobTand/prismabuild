from pathlib import Path
import socket
import time
import uuid

import pytest

from fleet_world import Admission, Clock, FleetWorld
from prismabuild import pool, record_store, tier_holdings


def _key():
    return uuid.uuid4().hex + uuid.uuid4().hex


def test_plain_and_simulated_admission_keep_explicit_refusals(tmp_path):
    plain = FleetWorld(tmp_path / "plain")
    plain_key = _key()
    plain.actions.publish(plain_key)
    row = record_store.value_or_raise(plain.queue.records.item(record_store.READY, plain_key))
    assert row["resources"] == {}
    assert plain.queue.claim(capacity={"cpu": 2, "mem_gb": 4}) is None
    world = FleetWorld(tmp_path / "admitted", admission=Admission(
        capacity={"cpu": 2, "mem_gb": 4},
        default_demand={"cpu": 1, "mem_gb": 1}))
    explicit_key = _key()
    world.actions.publish(explicit_key, resources=None)
    row = record_store.value_or_raise(world.queue.records.item(record_store.READY, explicit_key))
    assert row["resources"] == {}
    assert world.queue.claim() is None
    key = _key()
    world.actions.publish(key)
    assert world.queue.claim(capacity=None) is None
    claim = world.queue.claim(owner="world-control")
    assert claim is not None and claim["action_key"] == key
    assert isinstance(world.queue.records.claimed(key), record_store.Present)
    assert world.queue.ledger().holder_tokens(key) == {"cpu": 1, "mem_gb": 1}


def test_one_clock_drives_offer_expiry_without_coupling_worlds(tmp_path, monkeypatch):
    world = FleetWorld(tmp_path / "first")
    other = FleetWorld(tmp_path / "second")
    world.clock.install(monkeypatch)
    world.use_host(monkeypatch, "world-box")
    world.host("world-box", capacity={"cpu": 2, "mem_gb": 4}, tags=["cpu"])
    offers = world.queue.offers()
    assert len(offers) == 1 and offers[0]["host"] == "world-box"
    assert offers[0]["capacity"] == {"cpu": 2, "mem_gb": 4}
    world.clock.advance(pool.OFFER_TIMEOUT_S + 1)
    assert world.queue.offers() == []
    assert other.clock.now == 1000.0
    assert other.queue.offers() == []


def test_tier_setup_uses_the_real_announcement_and_mint_writers(tmp_path):
    world = FleetWorld(tmp_path)
    stage = world.stage_root("ssd")
    tier_id = "prismabuild-stage:world-box"
    world.tier({"tier_id": tier_id, "host": "world-box",
                "mountpoint": str(stage)}, supply={"stage_gib": 2})
    records = world.queue.tiers()
    assert len(records) == 1 and records[0]["tier_id"] == tier_id
    assert records[0]["mountpoint"] == str(stage)
    assert records[0]["announced_unix"] == world.clock.now
    assert tier_holdings.ids(world.queue.records) == [tier_id]


def test_broker_directory_binds_under_a_long_private_world_root(tmp_path):
    root = tmp_path / ("a" * 90) / ("b" * 90)
    world = FleetWorld(root)
    with world.broker_directory() as directory:
        path = directory / "broker.sock"
        assert len(str(path).encode()) < 108
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(path))
            server.listen(1)
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.connect(str(path))
                accepted, _ = server.accept()
                accepted.close()
        assert path.exists()
    assert not directory.exists()


def test_corruption_is_explicit_and_preserves_unknown_state(tmp_path):
    world = FleetWorld(tmp_path)
    key = _key()
    path = world.corrupt(f"pb-queue/ready/{key}.json", b"not-json\n")
    assert path.read_bytes() == b"not-json\n"
    assert isinstance(world.queue.records.item(record_store.READY, key), record_store.Unknown)
    for outside in ("../outside", str(tmp_path.parent / "outside")):
        with pytest.raises(ValueError, match="private world path"):
            world.corrupt(outside, b"bad")


def test_pool_clock_install_preserves_the_reader_and_other_clocks(monkeypatch):
    wall = lambda: 9876.0
    monkeypatch.setattr(time, "time", wall)
    readings = iter([40.0, 41.0])
    events = []

    def reader():
        events.append("read")
        return next(readings)

    Clock(source=reader).install(monkeypatch, scope="pool")
    assert events == []
    assert pool._now is reader
    assert time.time is wall
    assert pool._now() == 40.0
    assert events == ["read"]
    assert time.time() == 9876.0
    assert pool._now() == 41.0
    assert events == ["read", "read"]


def test_pool_clock_keeps_the_callers_mutable_cell_and_patch_lifetime(monkeypatch):
    original = pool._now
    cell = [100.0]
    with monkeypatch.context() as patch:
        Clock(source=lambda: cell[0]).install(patch, scope="pool")
        assert pool._now() == 100.0
        cell[0] += 0.001
        assert pool._now() == 100.001
    assert pool._now is original
