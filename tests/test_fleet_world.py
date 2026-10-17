from pathlib import Path
import socket
import uuid

import pytest

from fleet_world import Admission, FleetWorld
from prismabuild import pool, record_store


def _key():
    return uuid.uuid4().hex + uuid.uuid4().hex


def test_plain_and_simulated_admission_keep_explicit_refusals(tmp_path):
    plain = FleetWorld(tmp_path / "plain")
    with pytest.raises(pool.PoolContractError):
        plain.actions.publish(_key())
    world = FleetWorld(tmp_path / "admitted", admission=Admission(
        capacity={"cpu": 2, "mem_gb": 4},
        default_demand={"cpu": 1, "mem_gb": 1}))
    with pytest.raises(pool.PoolContractError):
        world.actions.publish(_key(), resources=None)
    key = _key()
    world.actions.publish(key)
    with pytest.raises(pool.PoolContractError):
        world.queue.claim(capacity=None)
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
    assert world.queue.tier_ids() == [tier_id]


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
