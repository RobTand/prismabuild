"""Issue 1521: private-queue lifecycle, isolated records and documented races."""
from __future__ import annotations

from pathlib import Path

import pytest

from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_gang_teardown_1517 import _both_claimed
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from prismabuild import _gang, pool


def test_unregistered_members_expire_after_the_registration_grace(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members("orphan-publication", file_group=False)
    ordinary = publish("unrelated", priority=0, gpu=0, mem_gb=1)
    latest = max(pool._read_json(queue.item_path(pool.READY, key))["published_unix"] for key in keys)
    clock[0] = latest + pool.LEASE_TIMEOUT_S - 1
    queue.sweep_gangs()
    assert all(queue.item_path(pool.READY, key).exists() for key in keys)
    clock[0] = latest + pool.LEASE_TIMEOUT_S + 1
    queue.sweep_gangs()
    assert all(queue.item_path(pool.WITHDRAWN, key).exists() for key in keys), (
        "expired gang members with no published group remained READY")
    assert not any(queue.item_path(pool.READY, key).exists() for key in keys)
    assert queue.item_path(pool.READY, ordinary).exists()
    assert not _gang.group_path(queue, group).exists()


def test_registered_slow_members_do_not_expire_as_orphans(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members("registered-slow")
    clock[0] += pool.LEASE_TIMEOUT_S * 10
    queue.sweep_gangs()
    assert all(queue.item_path(pool.READY, key).exists() for key in keys)
    assert _gang.read_group(queue, group) is not None


@pytest.mark.parametrize("bad_record", ["group", "election", "teardown"])
def test_a_bad_gang_record_does_not_poison_other_admission(gang_fleet, bad_record):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members("malformed", priority=10)
    assert claim("sparklina") is None
    paths = {"group": _gang.group_path(queue, group),
             "election": _gang.state_dir(queue, group) / "elect-0.json",
             "teardown": _gang.state_dir(queue, group) / "teardown.json"}
    paths[bad_record].write_text("{not valid JSON", encoding="utf-8")
    ordinary = publish("unrelated-after-bad-record", priority=0, gpu=0, mem_gb=1,
                       tags=["sparklina"])
    assert claim("sparklina") == ordinary, "one malformed gang blocked an unrelated row"
    assert denial(keys[0], "sparklina")["reason"] == "gang_contract_invalid"
    assert queue.item_path(pool.READY, keys[0]).exists()
    assert not queue.item_path(pool.CLAIMED, keys[0]).exists()


def test_the_sweep_retries_a_running_members_failed_withdrawal(gang_fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, first, second = _both_claimed(queue, publish, finish, claim, members)
    original = queue.withdraw
    failures = []
    def interrupted(key, **kwargs):
        if key == first and len(failures) < 2:
            failures.append(key)
            raise OSError("withdrawal interrupted")
        return original(key, **kwargs)
    monkeypatch.setattr(queue, "withdraw", interrupted)
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparky")
    queue.finish(second, status="failed", detail={"returncode": 1})
    assert _gang.teardown(queue, group) is not None
    assert queue.withdrawal_covers(pool._read_json(queue.item_path(pool.CLAIMED, first))) is None
    queue.sweep_gangs()
    assert len(failures) == 2, "the sweep did not retry the running member's withdrawal"
    assert _gang.group_path(queue, group).exists()
    queue.sweep_gangs()
    held = pool._read_json(queue.item_path(pool.CLAIMED, first))
    assert queue.withdrawal_covers(held) is not None
    assert queue.ledger("sparklina").held_keys() == [first]
    finish(first, "sparklina")
    assert queue.sweep_gangs() == [group]


def test_the_design_names_simultaneous_first_elections():
    design = (Path(__file__).resolve().parents[1] / "docs/design.md").read_text()
    ranking = design.split("**Ranking between gangs.**", 1)[1].split("**Lost workers.**", 1)[0]
    assert "simultaneous first elections" in ranking, "the residual race only names late publication"
    assert "two hosts" in ranking



def test_group_publication_cannot_race_a_member_transition(gang_fleet):
    import threading
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members("publication-lock", file_group=False)
    rows = [pool._read_json(queue.item_path(pool.READY, key)) for key in keys]
    entered, release = threading.Event(), threading.Event()
    def hold():
        with queue._transition_locked(keys[0]):
            entered.set()
            release.wait(10)
    thread = threading.Thread(target=hold)
    thread.start()
    try:
        assert entered.wait(10)
        with pytest.raises(_gang.GangContractError, match="publication busy"):
            _gang.publish_group(queue, group, rows)
        assert not _gang.group_path(queue, group).exists()
    finally:
        release.set()
        thread.join(10)
    assert not thread.is_alive()
    assert _gang.publish_group(queue, group, rows)["group"] == group


def test_a_late_publisher_cannot_resurrect_expired_unregistered_members(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys = members("late-publication", file_group=False)
    rows = [pool._read_json(queue.item_path(pool.READY, key)) for key in keys]
    clock[0] += pool.LEASE_TIMEOUT_S + 1
    queue.sweep_gangs()
    with pytest.raises(_gang.GangContractError, match="member ended"):
        _gang.publish_group(queue, group, rows)
    assert not _gang.group_path(queue, group).exists()

