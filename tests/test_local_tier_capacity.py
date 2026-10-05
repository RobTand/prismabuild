from types import SimpleNamespace

import pytest

from prismabuild import pool

GIB = 1024 ** 3


def policy(tmp_path):
    root = tmp_path / "local"
    root.mkdir()
    return {"root": str(root), "maximum_gib": 100, "floor_fraction": .05,
            "docker_allowance_gib": 10}


def sample(avail):
    return SimpleNamespace(f_frsize=GIB, f_blocks=1000, f_bavail=avail, f_bfree=avail + 50)


def test_capacity_uses_available_not_root_free_and_counts_occupied_bytes(tmp_path):
    from prismabuild import local_tier
    spec = policy(tmp_path)
    assert local_tier.capacity_gib(spec, sample(125), held_bytes=5 * GIB) == 70
    assert local_tier.capacity_gib(spec, sample(500), held_bytes=5 * GIB) == 100
    assert local_tier.capacity_gib(spec, sample(20), held_bytes=0) == 0


def test_remint_shrinks_only_free_tokens(tmp_path, monkeypatch):
    from prismabuild import local_tier
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    spec = policy(tmp_path)
    monkeypatch.setattr(local_tier.os, "statvfs", lambda _: sample(125))
    assert local_tier.mint(queue, "test-host", spec)["capacity"]["local_gib"] == 65
    ledger = queue.tier_ledger("local:test-host")
    assert ledger.acquire("a" * 64, {"local_gib": 4})
    monkeypatch.setattr(local_tier.os, "statvfs", lambda _: sample(10))
    local_tier.mint(queue, "test-host", spec)
    assert ledger.held()["local_gib"] == 4
    assert not list(ledger.free_dir.glob("local_gib-*"))


@pytest.mark.parametrize("avail,size,allowed", [(100, 40, True), (79, 40, False), (100, 60, False)])
def test_precopy_checks_both_d1_limits_and_docker_allowance(tmp_path, avail, size, allowed):
    from prismabuild import local_tier
    spec = policy(tmp_path)
    if allowed:
        local_tier.require_write_space(spec, sample(avail), size * GIB)
    else:
        with pytest.raises(ValueError, match="capacity"):
            local_tier.require_write_space(spec, sample(avail), size * GIB)


def test_reservation_is_all_hosts_or_none(tmp_path):
    from prismabuild import local_tier
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.mint_tier_capacity("local:one", {"local_gib": 1})
    queue.mint_tier_capacity("local:two", {"local_gib": 0})
    with pytest.raises(ValueError, match="two"):
        local_tier.reserve(queue, "b" * 64, ["one", "two"], GIB)
    assert queue.tier_ledger("local:one").held() == {}


def test_local_role_and_policy_are_published_but_not_activated():
    import supervise
    import publish_runtime
    assert supervise.ROLE_SCRIPTS["localtier"] == "local_tier_loop.py"
    assert "local_tier_loop.py" in publish_runtime.FLEET_SCRIPTS
