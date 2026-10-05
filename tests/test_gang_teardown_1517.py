"""PB #1517: a gang ends whole -- member failure, withdrawal, start barrier, pruning.

Same two-host fixture as ``test_gang_reservation_1517``: real queue, ledgers,
census and controllers; only the clock and sampler are controlled.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from test_gang_reservation_1517 import HOSTS, _busy_both, gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401

from prismabuild import _gang, pool

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402


def _both_claimed(queue, publish, finish, gclaim, members, **kwargs):
    incumbents = _busy_both(publish, gclaim)
    group, (first, second) = members("whole", **kwargs)
    for host in HOSTS:
        assert gclaim(host) is None
    for host in HOSTS:
        finish(incumbents[host], host)
    assert gclaim("sparklina") is None  # ready, waiting for sparky
    assert gclaim("sparky") == second
    assert gclaim("sparklina") == first
    return group, first, second


def test_a_member_failure_tears_the_gang_down_and_releases_its_fences(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, first, second = _both_claimed(queue, publish, finish, gclaim, members)
    pool.socket.gethostname = lambda: "sparky"  # noqa: E731 (gclaim resets it per call)
    queue.finish(second, status="failed", detail={"returncode": 1})
    torn = _gang.teardown(queue, group)
    assert torn is not None and second[:12] in torn["reason"], torn
    # The running sibling is stopped through the ordinary withdrawal path.
    claimed = pool._read_json(queue.item_path(pool.CLAIMED, first))
    assert claimed is not None and queue.withdrawal_covers(claimed) is not None
    # Its fences no longer hold lower-priority work off sparky.
    refill = publish("after-teardown", priority=-10, timeout_s=None, cpu=1, gpu=0, mem_gb=1)
    assert gclaim("sparky") == refill, denial(refill, "sparky")


def test_withdrawing_a_waiting_member_withdraws_the_gang_and_releases_both_hosts(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    group, (first, second) = members("withdrawn")
    for host in HOSTS:
        assert gclaim(host) is None
    assert set(_gang.elections(queue, group, 2)) == {0, 1}
    queue.withdraw(first, reason="operator cancelled the window")
    assert _gang.teardown(queue, group) is not None
    for key in (first, second):
        assert queue.item_path(pool.WITHDRAWN, key).exists(), key
        assert not queue.item_path(pool.READY, key).exists(), key
    for host in HOSTS:
        refill = publish(f"refill-{host}", priority=-10, timeout_s=None, cpu=1, gpu=0, mem_gb=1)
        assert gclaim(host) == refill, denial(refill, host)
        assert set(queue.ledger(host).held_keys()) == {incumbents[host], refill}
    # Every member ended: the sweep prunes the gang so no census reads it.
    assert queue.sweep_gangs() == [group]
    assert not _gang.group_path(queue, group).exists()
    assert not _gang.state_dir(queue, group).exists()


def test_a_claimed_member_never_launches_alone(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    group, (first, second) = members("barrier", skew_s=0.5)
    for host in HOSTS:
        assert gclaim(host) is None
    for host in HOSTS:
        finish(incumbents[host], host)
    assert gclaim("sparklina") is None  # first is ready
    assert gclaim("sparky") == second
    # Higher-priority work takes sparklina before first commits.
    urgent = publish("urgent", priority=20, timeout_s=None, cpu=2, gpu=1, mem_gb=100)
    assert gclaim("sparklina") == urgent
    record = pool._read_json(queue.item_path(pool.CLAIMED, second))
    pool.socket.gethostname = lambda: "sparky"  # noqa: E731
    outcome = queue._gang_start_barrier(record, owner="test", heartbeat_s=60.0)
    assert outcome is not None, "the member was released to launch without its sibling"
    assert outcome["termination_reason"] == "gang_start_skew_exceeded", outcome
    assert outcome["gang"]["siblings"] == {0: "ready"}
    assert _gang.teardown(queue, group) is not None
    assert queue.item_path(pool.WITHDRAWN, first).exists()


def test_the_barrier_releases_when_every_member_is_claimed(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, first, second = _both_claimed(queue, publish, finish, gclaim, members, skew_s=0.5)
    for key in (first, second):
        record = pool._read_json(queue.item_path(pool.CLAIMED, key))
        assert queue._gang_start_barrier(record, owner="test", heartbeat_s=60.0) is None
    assert _gang.teardown(queue, group) is None


def test_a_gang_member_gets_exactly_one_attempt(gang_fleet, tmp_path):
    queue = gang_fleet[0]
    gang = {"group": "a" * 32, "size": 2, "index": 0}
    for kwargs in ({"max_attempts": 3}, {"max_attempts": 1, "retry_safe": True}):
        with pytest.raises(pool.PoolContractError, match="one|max_attempts"):
            queue.publish(action_key="b" * 64, cas_root=str(tmp_path), checkout_root=str(tmp_path),
                          worker_script="w.py", resources={"cpu": 1, "mem_gb": 1},
                          gang=gang, **kwargs)


def test_pbrun_seals_a_gang_member_and_refuses_retries():
    base = ["--cwd", ".", "--transport", "pool", "--gang-group", "c" * 32, "--gang-size", "2", "--gang-index", "1",
            "--", "true"]
    args = pbrun.parse_args(base)
    assert pbrun.gang_declaration(args) == {"group": "c" * 32, "size": 2, "index": 1}
    for extra in (["--retry-safe", "--max-attempts", "2"],):
        with pytest.raises(SystemExit, match="one attempt"):
            pbrun.gang_declaration(pbrun.parse_args([*extra, *base]))
    with pytest.raises(SystemExit, match="go together"):
        pbrun.gang_declaration(pbrun.parse_args(["--cwd", ".", "--transport", "pool", "--gang-size", "2", "--", "true"]))
    assert pbrun.gang_declaration(pbrun.parse_args(["--cwd", ".", "--", "true"])) is None
