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
