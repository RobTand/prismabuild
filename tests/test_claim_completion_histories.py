"""Native reservation boundaries retained while the claim driver changes."""
import json

import pytest

from prismabuild import adaptive_gpu, pool
from test_local_scratch_measured_placement import (
    CAPACITY, FAST, SLOW, fleet,  # noqa: F401
)


KEY = "c" * 64


def _queue(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.publish(action_key=KEY, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker", resources={"cpu": 1, "mem_gb": 1})
    return queue


@pytest.mark.parametrize("phase", ["terminal", "claim-record", "lease"])
def test_unhandled_publication_errors_stay_outside_uncertain_cleanup(tmp_path, monkeypatch, phase):
    queue = _queue(tmp_path)
    ready = queue.item_path(pool.READY, KEY).read_bytes()
    cause = LookupError("publication boundary")
    exits = []
    abandon = pool._ClaimReservation.abandon

    def abandon_checked(self, exit, **kwargs):
        exits.append(exit)
        return abandon(self, exit, **kwargs)

    monkeypatch.setattr(pool._ClaimReservation, "abandon", abandon_checked)
    if phase == "terminal":
        terminal = queue.terminal_outcome_covers

        def fail_terminal(*args, **kwargs):
            if queue.item_path(pool.CLAIMED, KEY).exists():
                raise cause
            return terminal(*args, **kwargs)

        monkeypatch.setattr(queue, "terminal_outcome_covers", fail_terminal)
    elif phase == "claim-record":
        write = pool._write_json_atomic

        def fail_write(path, *args, **kwargs):
            if path == queue.item_path(pool.CLAIMED, KEY):
                raise cause
            return write(path, *args, **kwargs)

        monkeypatch.setattr(pool, "_write_json_atomic", fail_write)
    else:
        def fail_lease(*args, **kwargs):
            raise cause

        monkeypatch.setattr(queue, "write_lease", fail_lease)
    with pytest.raises(LookupError) as caught:
        queue.claim(owner="publication-owner", capacity={"cpu": 2, "mem_gb": 2})
    assert caught.value is cause
    assert exits == []
    assert queue.ledger().holder_tokens(KEY) == {"cpu": 1, "mem_gb": 1}
    assert not queue.item_path(pool.READY, KEY).exists()
    assert not queue.lease_path(KEY).exists()
    claimed = queue.item_path(pool.CLAIMED, KEY)
    if phase == "lease":
        assert json.loads(claimed.read_text())["claimed_by"] == "publication-owner"
    else:
        assert claimed.read_bytes() == ready
    assert pool._CLAIM_DIAGNOSTICS.get() is None
    assert queue.last_claim_pass is not None


def test_incomplete_commit_keeps_a_newer_ready_publication_byte_exact(tmp_path, monkeypatch):
    queue = _queue(tmp_path)
    old = queue.item_path(pool.READY, KEY).read_bytes()
    ledger = queue.ledger()
    commit = pool.ResourceLedger.commit_acquire
    newer = []

    def commit_then_publish(self, key, handle):
        filed = commit(self, key, handle)
        if key == KEY:
            assert filed == 2
            queue.publish(action_key=KEY, cas_root="/cas", checkout_root="/new-checkout",
                          worker_script="/worker", resources={"cpu": 2, "mem_gb": 1}, priority=17)
            newer.append(queue.item_path(pool.READY, KEY).read_bytes())
            return 0
        return filed

    monkeypatch.setattr(pool.ResourceLedger, "commit_acquire", commit_then_publish)
    assert queue.claim(owner="incomplete-owner", capacity={"cpu": 2, "mem_gb": 2}) is None
    assert len(newer) == 1 and newer[0] != old
    assert queue.item_path(pool.READY, KEY).read_bytes() == newer[0]
    assert queue.item_path(pool.CLAIMED, KEY).read_bytes() == old
    assert not queue.lease_path(KEY).exists()
    assert ledger.held_keys() == []
    assert ledger.available() == {"cpu": 2, "mem_gb": 2}


@pytest.mark.parametrize("changed", [False, True], ids=["late-expiry", "generation-before-expiry"])
def test_late_scratch_boundary_refuses_before_any_probe_or_borrow(fleet, monkeypatch, changed):
    fleet.publish(max_age=60.)
    real = adaptive_gpu.Controller.decision
    observed = []

    def decision_then_age(self, item, demand, **kwargs):
        value = real(self, item, demand, **kwargs)
        observed.append(value)
        fleet.clock[0] += 120.
        if changed:
            item["attempts"] = int(item.get("attempts", 0)) + 1
        return value

    monkeypatch.setattr(adaptive_gpu.Controller, "decision", decision_then_age)
    assert fleet.claim(FAST) is None
    assert observed and observed[-1] is not None
    assert fleet.denial(FAST)["reason"] == (
        "local_scratch_io_generation_changed" if changed else "local_scratch_io_profile_expired")
    for host in (SLOW, FAST):
        assert fleet.queue.ledger(host).held_keys() == []
        assert fleet.queue.ledger(host).available() == CAPACITY
    assert not fleet.queue.lease_path(fleet.key).exists()
    assert not fleet.queue.item_path(pool.CLAIMED, fleet.key).exists()
    assert fleet.queue.item_path(pool.READY, fleet.key).exists()
    assert not fleet.queue.passes_path(fleet.key).exists()


def test_changed_generation_refreshes_all_three_pass_views_before_the_next_candidate(tmp_path, monkeypatch):
    from prismabuild import _measurement_reservation as measurement
    from test_pool_tier_reservations import STAGE, TIER, _denial, _publish

    queue = pool.PoolQueue(tmp_path / "views-queue")
    queue.ensure_layout()
    queue.mint_tier_capacity(TIER, {"stage_gib": 2})
    first, second = "d" * 64, "e" * 64
    _publish(queue, first, {"cpu": 1, "mem_gb": 1, STAGE: 1}, priority=10)
    _publish(queue, second, {"cpu": 1, "mem_gb": 1, STAGE: 1}, priority=0)
    second_bytes = queue.item_path(pool.READY, second).read_bytes()
    listed, plans, scans, replaced = [], [], [], []
    listdir, rename = pool.os.listdir, pool.os.rename
    fresh_plan, census = queue._fresh_reader_plan, measurement.CensusReader._census

    def observe_list(path):
        value = listdir(path)
        if path == queue.dir(pool.CLAIMED):
            listed.append(list(value))
        return value

    def observe_plan(*args, **kwargs):
        value = fresh_plan(*args, **kwargs)
        plans.append(value)
        return value

    def observe_census(self, directory):
        scans.append(directory)
        return census(self, directory)

    def replace_then_rename(src, dst):
        if src == queue.item_path(pool.READY, first):
            _publish(queue, first, {"cpu": 2, "mem_gb": 1, STAGE: 1}, priority=10)
            replaced.append(queue.item_path(pool.READY, first).read_bytes())
            rename(src, dst)
            queue.announce_tier({"tier_id": TIER, pool.READER_PLAN_FIELD: {
                "declared_wait": [{"mover_action_key": "f" * 64}]}})
            return
        return rename(src, dst)

    monkeypatch.setattr(pool.os, "listdir", observe_list)
    monkeypatch.setattr(pool.os, "rename", replace_then_rename)
    monkeypatch.setattr(queue, "_fresh_reader_plan", observe_plan)
    monkeypatch.setattr(measurement.CensusReader, "_census", observe_census)
    assert queue.claim(owner="views-owner", capacity={"cpu": 8, "mem_gb": 8}) is None
    assert len(replaced) == 1
    assert queue.item_path(pool.READY, first).read_bytes() == replaced[0]
    assert queue.item_path(pool.READY, second).read_bytes() == second_bytes
    assert _denial(queue, first)["reason"] == "claimed_record_changed"
    assert _denial(queue, second)["reason"] == "deferred_for_declared_wait"
    assert len(listed) == 3
    assert plans[0] is None and plans[1]["declared_wait"] == [{"mover_action_key": "f" * 64}]
    assert len(scans) == 4
    assert queue.ledger().held_keys() == []
    assert queue.ledger().available() == {"cpu": 8, "mem_gb": 8}
    assert queue.tier_ledger(TIER).available() == {"stage_gib": 2}
    assert list(queue.dir(pool.CLAIMED).iterdir()) == []
    assert not queue.passes_path(first).exists() and not queue.passes_path(second).exists()
