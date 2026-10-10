"""PB #1521 gang follow-ups: isolate, sweep, retry, off-switch, dead-partner, deadline.

Real private queues, ledgers and census; only the clock, sampler and worker
offers are controlled. Each test names the item it proves.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

from test_gang_reservation_1517 import HOSTS, gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401

from prismabuild import _gang, _measurement_reservation as reservation, pool

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))


def _offer(queue, clock, host, *, tags, fresh=True):
    directory = queue.root / pool.WORKERS
    directory.mkdir(parents=True, exist_ok=True)
    record = {
        "schema": pool.POOL_OFFER_SCHEMA_V1, "host": host, "tags": list(tags),
        "has_gpu": True, "capacity": {"cpu": 20, "gpu": 1, "mem_gb": 120},
        "announced_unix": clock[0] if fresh else clock[0] - 10_000.0,
    }
    (directory / f"{host}.json").write_text(json.dumps(record))


def _refresh(queue, clock, host, tags):
    _offer(queue, clock, host, tags=tags, fresh=True)
def _busy_both(publish, gclaim):
    incumbents = {}
    for host in HOSTS:
        incumbents[host] = publish(f"inc-{host}", priority=-10, timeout_s=None,
                                   cpu=2, gpu=1, mem_gb=48)
        assert gclaim(host) == incumbents[host]
    return incumbents


def test_malformed_group_isolates_only_its_gang(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    _busy_both(publish, gclaim)
    group1, keys1 = members("bad-one", priority=5)
    group2, keys2 = members("good-two", priority=5)
    for host in HOSTS:
        assert gclaim(host) is None
    assert set(_gang.elections(queue, group1, 2)) == {0, 1}
    assert _gang.elections(queue, group2, 2) == {}, "lower gang elected past a better one"
    path = _gang.group_path(queue, group1)
    record = json.loads(path.read_text())
    record["schema"] = "prismabuild.gang_group.vX"
    path.write_text(json.dumps(record))
    for host in HOSTS:
        assert gclaim(host) is None
    assert set(_gang.elections(queue, group2, 2)) == {0, 1}
    rows = {}
    for key in (*keys1, *keys2):
        row = pool._read_json(queue.item_path(pool.READY, key))
        rows[key] = [{"published_unix": row["published_unix"]}]
    found = reservation._gang_elections(queue, rows, 0)
    assert found, "the census went unavailable for every gang"
    assert all(v["group"] == group2 for v in found.values())
    assert not any(v["group"] == group1 for v in found.values())
    bad = publish("refill-bad", priority=-10, timeout_s=None, cpu=1, gpu=1, mem_gb=1,
                  tags=["sparklina"])
    assert gclaim("sparklina") is None
    assert denial(bad, "sparklina")["reason"] == "deferred_for_gang_reservation"
    member_row = pool._read_json(queue.item_path(pool.READY, keys1[0]))
    assert member_row is not None


def test_malformed_election_isolates_only_its_gang(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    _busy_both(publish, gclaim)
    group1, keys1 = members("bad-e", priority=5)
    group2, keys2 = members("good-e", priority=5)
    for host in HOSTS:
        assert gclaim(host) is None
    assert set(_gang.elections(queue, group1, 2)) == {0, 1}
    path = _gang.state_dir(queue, group1) / "elect-0.json"
    vote = json.loads(path.read_text())
    vote["host"] = 42
    path.write_text(json.dumps(vote))
    for host in HOSTS:
        assert gclaim(host) is None
    rows = {}
    for key in (*keys1, *keys2):
        row = pool._read_json(queue.item_path(pool.READY, key))
        rows[key] = [{"published_unix": row["published_unix"]}]
    found = reservation._gang_elections(queue, rows, 0)
    assert found
    assert all(v["group"] == group2 for v in found.values())


# -- item 2: orphan member rows ------------------------------------------------


def test_orphan_rows_wait_out_the_bound_then_withdraw(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, keys = members("orphan", file_group=False)
    assert _gang.read_group(queue, group) is None
    queue.sweep_gangs()
    for key in keys:
        assert queue.item_path(pool.READY, key).exists(), "fresh orphan withdrawn early"
    clock[0] += _gang.ORPHAN_ROW_AFTER_S + 1
    queue.sweep_gangs()
    for key in keys:
        assert queue.item_path(pool.WITHDRAWN, key).exists(), key
        assert not queue.item_path(pool.READY, key).exists(), key


def test_grouped_rows_survive_the_orphan_sweep(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, keys = members("grouped")
    clock[0] += _gang.ORPHAN_ROW_AFTER_S + 10_000
    queue.sweep_gangs()
    for key in keys:
        assert queue.item_path(pool.READY, key).exists(), key


# -- item 5: teardown retry ----------------------------------------------------


def test_sweep_retries_a_failed_ready_withdrawal(gang_fleet, monkeypatch):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    _busy_both(publish, gclaim)
    group, (first, second) = members("retry-ready")
    for host in HOSTS:
        assert gclaim(host) is None
    record = _gang.read_group(queue, group)
    assert record is not None
    record["wait_deadline_unix"] = clock[0] - 1.0
    pool._write_json_atomic(_gang.group_path(queue, group), record)
    calls = []
    real_withdraw = queue.withdraw

    def flaky(key, *args, **kwargs):
        if key == first and not calls:
            calls.append(key)
            raise pool.PoolContractError("injected teardown failure")
        return real_withdraw(key, *args, **kwargs)

    monkeypatch.setattr(queue, "withdraw", flaky)
    queue.sweep_gangs()
    assert calls, "the sweep never attempted the teardown withdrawal"
    assert queue.item_path(pool.READY, first).exists(), "first attempt must fail"
    monkeypatch.setattr(queue, "withdraw", real_withdraw)
    queue.sweep_gangs()
    assert queue.item_path(pool.WITHDRAWN, first).exists(), "retry never withdrew it"
    assert not queue.item_path(pool.READY, first).exists()


def test_sweep_retries_a_running_member_until_it_ends(gang_fleet, monkeypatch):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    incumbents = _busy_both(publish, gclaim)
    group, (first, second) = members("retry-running")
    for host in HOSTS:
        assert gclaim(host) is None
    for host in HOSTS:
        finish(incumbents[host], host)
    assert gclaim("sparklina") is None
    assert gclaim("sparky") == second
    assert gclaim("sparklina") == first
    assert queue.item_path(pool.CLAIMED, first).exists()
    assert queue.item_path(pool.CLAIMED, second).exists()
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparky")
    queue.finish(second, status="failed", detail={"returncode": 1})
    assert _gang.teardown(queue, group) is not None
    calls = []
    real_withdraw = queue.withdraw

    def flaky(key, *args, **kwargs):
        if key == first and not calls:
            calls.append(key)
            raise pool.PoolContractError("injected running teardown failure")
        return real_withdraw(key, *args, **kwargs)

    monkeypatch.setattr(queue, "withdraw", flaky)
    pruned = queue.sweep_gangs()
    assert calls, "the sweep never retried the running member"
    assert group not in pruned, "sweep pruned a gang with a running member"
    assert queue.item_path(pool.CLAIMED, first).exists()
    monkeypatch.setattr(queue, "withdraw", real_withdraw)
    pruned = queue.sweep_gangs()
    claimed = pool._read_json(queue.item_path(pool.CLAIMED, first))
    assert claimed is not None and queue.withdrawal_covers(claimed) is not None
    assert group not in pruned, "sweep pruned a gang with a running member"
    assert queue.item_path(pool.CLAIMED, first).exists()


# -- item 4: off-switch --------------------------------------------------------


def test_off_switch_offers_nothing_and_refuses_gang_submit(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    _offer(queue, clock, "sparklina", tags=["sparklina", "gb10"], fresh=True)
    _offer(queue, clock, "sparky", tags=["sparky", "gb10"], fresh=True)
    assert queue.offers()
    assert _gang.TAG not in queue.offered_tags(), "a box offers gang-v1 with the switch off"
    member_intent = {"tags": ["sparklina", _gang.TAG], "needs_gpu": True,
                     "resources": {"cpu": 2, "gpu": 1, "mem_gb": 100}}
    assert queue.placeable(member_intent) is False, "gang submit not refused with the switch off"


def test_non_gang_admission_carries_no_gang_marks(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    key = publish("plain", priority=-10, timeout_s=None, cpu=1, gpu=0, mem_gb=1)
    assert gclaim("sparklina") == key
    claimed = pool._read_json(queue.item_path(pool.CLAIMED, key))
    assert claimed is not None
    assert claimed.get("gang") is None
    assert not any(name.startswith("gang_") for name in claimed), claimed.keys()


# -- item 1: dead-partner loans -------------------------------------------------


def _fenced_idle(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group, (first, second) = members("dead-partner", priority=10, mem_gb=24)
    _offer(queue, clock, "sparklina",
           tags=["sparklina", "gb10", _gang.TAG], fresh=True)
    _offer(queue, clock, "sparky", tags=["sparky", "gb10", _gang.TAG], fresh=False)
    assert gclaim("sparklina") is None
    assert denial(first, "sparklina")["reason"] == "gang_waiting_for_peers"
    election = _gang.elections(queue, group, 2)[0]
    assert election["host"] == "sparklina"
    return queue, clock, publish, finish, gclaim, denial, group, first, second, election


def test_transient_blip_releases_nothing(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, group, first, second, election = _fenced_idle(gang_fleet)
    key = publish("bounded", priority=-10, timeout_s=600, cpu=1, gpu=0, mem_gb=1,
                  tags=["sparklina"])
    assert gclaim("sparklina") is None, "a single absent reading released the fence"
    _refresh(queue, clock, "sparky", tags=["sparky", "gb10", _gang.TAG])
    assert gclaim("sparklina") is None, "a present partner still denies but records presence"
    (queue.root / pool.WORKERS / "sparky.json").unlink()
    _offer(queue, clock, "sparky", tags=["sparky", "gb10", _gang.TAG], fresh=False)
    clock[0] += 100.0
    _refresh(queue, clock, "sparklina", tags=["sparklina", "gb10", _gang.TAG])
    assert gclaim("sparklina") is None, "a blip followed by brief absence released the fence"


def test_sustained_absence_admits_bounded_work_only(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, group, first, second, election = _fenced_idle(gang_fleet)
    bounded = publish("bounded", priority=-10, timeout_s=600, cpu=1, gpu=0, mem_gb=1,
                      tags=["sparklina"])
    unbounded = publish("unbounded", priority=-10, timeout_s=None, cpu=1, gpu=0, mem_gb=1,
                        tags=["sparklina"])
    assert gclaim("sparklina") is None
    clock[0] += _gang.DEAD_PARTNER_AFTER_S + 1
    _refresh(queue, clock, "sparklina", tags=["sparklina", "gb10", _gang.TAG])
    assert gclaim("sparklina") == bounded, denial(bounded, "sparklina")
    assert queue.item_path(pool.READY, unbounded).exists()
    assert gclaim("sparklina") is None
    assert denial(unbounded, "sparklina")["reason"] == "deferred_for_gang_reservation"
    over = publish("over", priority=-10, timeout_s=3600, cpu=1, gpu=0, mem_gb=1,
                   tags=["sparklina"])
    assert gclaim("sparklina") is None
    assert denial(over, "sparklina")["reason"] == "deferred_for_gang_reservation"


def test_partner_return_starts_gang_after_bounded_job(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, group, first, second, election = _fenced_idle(gang_fleet)
    bounded = publish("bounded", priority=-10, timeout_s=600, cpu=1, gpu=0, mem_gb=1,
                      tags=["sparklina"])
    assert gclaim("sparklina") is None, denial(bounded, "sparklina")
    clock[0] += _gang.DEAD_PARTNER_AFTER_S + 1
    _refresh(queue, clock, "sparklina", tags=["sparklina", "gb10", _gang.TAG])
    assert gclaim("sparklina") == bounded, denial(bounded, "sparklina")
    finish(bounded, "sparklina")
    _refresh(queue, clock, "sparky", tags=["sparky", "gb10", _gang.TAG])
    assert gclaim("sparky") == second, denial(second, "sparky")
    assert gclaim("sparklina") == first, denial(first, "sparklina")


# -- opt-in queue-wait deadline --------------------------------------------------


def test_wait_deadline_cancels_only_its_own_gang(gang_fleet):
    queue, clock, publish, finish, gclaim, denial, members = gang_fleet
    group_old, keys_old = members("deadline-old")
    record = _gang.read_group(queue, group_old)
    assert record is not None
    record["wait_deadline_unix"] = clock[0] - 1.0
    pool._write_json_atomic(_gang.group_path(queue, group_old), record)
    group_live, keys_live = members("deadline-live")
    pruned = queue.sweep_gangs()
    for key in keys_old:
        assert queue.item_path(pool.WITHDRAWN, key).exists(), key
    assert _gang.teardown(queue, group_old) is not None or group_old in pruned
    assert _gang.teardown(queue, group_live) is None
    for key in keys_live:
        assert queue.item_path(pool.READY, key).exists(), key
