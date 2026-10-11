"""Gang backfill uses real private queues; no payloads or live fleet mutations."""
from __future__ import annotations

import pytest

from test_gang_reservation_1517 import gang_fleet  # noqa: F401
from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from prismabuild import _gang, _measurement_reservation as reservation, pool


def waiting(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    incumbent = publish("partner-measurement", measurement=True, tags=["sparky"])
    assert claim("sparky") == incumbent
    group, keys = members("window", priority=10, mem_gb=104)
    assert claim("sparky") is None
    assert claim("sparklina") is None
    assert denial(keys[0], "sparklina")["reason"] == "gang_waiting_for_peers"
    return group, keys, incumbent


def backfill(publish, **kwargs):
    return publish("restartable-backfill", priority=-10, mem_gb=24,
                   tags=["sparklina"], retry_safe=True, **kwargs)


def test_idle_fenced_host_admits_restartable_minus_ten(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent = waiting(gang_fleet)
    key = backfill(publish)
    assert claim("sparklina") == key, "idle gang fence refused proven priority -10 backfill"
    held = pool._read_json(queue.item_path(pool.CLAIMED, key))
    assert held["gang_backfill"][0]["group"] == group
    assert queue.ledger("sparklina").held_keys() == [key]
    assert queue.item_path(pool.READY, keys[0]).exists()
    assert not queue.withdrawal_decisions(key)
    assert claim("sparklina") is None
    assert not queue.withdrawal_decisions(key), "backfill stopped while partner still blocked"


def test_an_equal_priority_reservation_shortfall_never_becomes_a_backfill_loan(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    incumbent = publish("unavailable-peer", tags=["sparky"], timeout_s=None)
    assert claim("sparky") == incumbent
    group, keys = members("aged-equal-priority", priority=-10)
    assert claim("sparklina") is None
    clock[0] += reservation.GANG_RESERVE_AFTER_S + 1
    assert claim("sparklina") is None
    election = _gang.elections(queue, group, 2)[0]
    assert _gang.backfill_allowed(queue, election, clock[0])
    key = backfill(publish)
    assert queue._preemption_eligible(pool._read_json(queue.item_path(pool.READY, key)))
    assert claim("sparklina") is None, "a loan bypassed the gang's demand reservation"
    blocked = denial(key, "sparklina")
    assert blocked["reason"] == "deferred_for_gang_reservation"
    assert blocked["evidence"]["gang_election"]["reservation"]["gpu"] == 1
    assert queue.ledger("sparklina").held_keys() == []
    assert queue.item_path(pool.READY, key).exists()
    assert queue.item_path(pool.READY, keys[0]).exists()


@pytest.mark.parametrize("options", [
    {"priority": 0}, {"priority": 1}, {"priority": -9},
    {"retry_safe": False, "max_attempts": 1}, {"max_attempts": 1},
    {"measurement": True},
])
def test_unqualified_rows_remain_fenced(gang_fleet, options):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    waiting(gang_fleet)
    args = dict(priority=-10, retry_safe=True, mem_gb=24, tags=["sparklina"])
    args.update(options)
    key = publish("unqualified", **args)
    assert claim("sparklina") is None
    assert denial(key, "sparklina")["reason"] == "deferred_for_gang_reservation"


def test_disabled_policy_restores_strict_fence(gang_fleet, monkeypatch):
    monkeypatch.setenv("PRISMABUILD_GANG_BACKFILL", "0")
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    waiting(gang_fleet)
    key = backfill(publish)
    assert claim("sparklina") is None
    assert denial(key, "sparklina")["reason"] == "deferred_for_gang_reservation"


def seeded_backfill(gang_fleet):
    """Isolate reclamation from admission, so unfixed code reaches the trap."""
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    incumbent = publish("partner-measurement", measurement=True, tags=["sparky"])
    assert claim("sparky") == incumbent
    key = backfill(publish)
    assert claim("sparklina") == key
    group, keys = members("window", priority=10, mem_gb=104)
    assert claim("sparky") is None
    assert claim("sparklina") is None
    election = _gang.elections(queue, group, 2)[0]
    row = pool._read_json(queue.item_path(pool.CLAIMED, key))
    row["gang_backfill"] = [election]
    pool._write_json_atomic(queue.item_path(pool.CLAIMED, key), row)
    return group, keys, incumbent, key


def test_peer_ready_preempts_before_token_gate_and_records_release(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent, key = seeded_backfill(gang_fleet)
    # The local member has never passed its memory/GPU token gates, and has
    # no ready mark. The remote member alone becomes fresh-ready.
    assert not (_gang.state_dir(queue, group) / "ready-0.json").exists()
    finish(incumbent, "sparky")
    assert claim("sparky") is None
    requested = clock[0]
    assert claim("sparklina") is None
    decisions = queue.withdrawal_decisions(key)
    assert len(decisions) == 1, "token-gate trap never requested gang backfill preemption"
    decision = decisions[0][1]
    assert decision["preempted_by"] == keys[0]
    timing = decision["gang_backfill_preemption"]
    assert timing["requested_unix"] >= requested
    assert timing["tokens_returned_unix"] is None
    assert queue.item_path(pool.READY, key).exists(), "preemption cancelled rather than requeued"
    assert queue.ledger("sparklina").held_keys() == [key], "preemption returned live tokens"
    blocked = denial(keys[0], "sparklina")
    assert blocked["reason"] == "gang_waiting_for_backfill_release"
    election = _gang.elections(queue, group, 2)[0]
    assert election["backfill_preemptions"][0]["tokens_returned_unix"] is None
    assert claim("sparklina") is None
    assert len(queue.withdrawal_decisions(key)) == 1
    # Explicitly settle the private holder. No assumed kill timeout returns
    # capacity. Once settled, each member commits in at most two claim passes.
    finish(key, "sparklina")
    released = clock[0]
    assert claim("sparklina") == keys[0]
    assert claim("sparky") == keys[1]
    assert clock[0] - released < _gang.READY_FRESH_S
    election = _gang.elections(queue, group, 2)[0]
    timing = election["backfill_preemptions"][0]
    assert timing["tokens_returned_unix"] is not None
    assert timing["tokens_returned_unix"] >= timing["requested_unix"]
    archived = pool._read_json(queue.superseded_dir() /
                              f"{key}.{queue.attempt_generation(decision)}.withdrawn-finish.json")
    assert archived["gang_backfill_release"]["tokens_returned_unix"] is not None


def test_failed_cleanup_is_visible_and_never_grants_capacity(gang_fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent, key = seeded_backfill(gang_fleet)
    finish(incumbent, "sparky")
    assert claim("sparky") is None
    assert claim("sparklina") is None
    assert queue.withdrawal_decisions(key), "no readiness-triggered preemption"
    monkeypatch.setattr(queue, "cleanup_action_containers", lambda *a, **k:
                        {"complete": False, "error": "GPU/container settlement unproved"})
    queue.finish(key, status="withdrawn")
    clock[0] += _gang.DEFAULT_SKEW_S + _gang.READY_FRESH_S
    assert claim("sparklina") is None
    assert denial(keys[0], "sparklina")["reason"] == "gang_waiting_for_backfill_release"
    assert queue.ledger("sparklina").held_keys() == [key]
    assert _gang.elections(queue, group, 2)[0]["backfill_preemptions"][0]["tokens_returned_unix"] is None


def test_teardown_releases_fence_without_requiring_preemption(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent = waiting(gang_fleet)
    queue.withdraw(keys[0], reason="window cancelled")
    key = publish("ordinary-after-teardown", priority=0, mem_gb=24,
                  tags=["sparklina"])
    assert claim("sparklina") == key
    assert not queue.withdrawal_decisions(key)



def test_backfill_on_both_hosts_reclaims_without_any_fresh_ready_mark(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    holders = [publish(f"backfill-{host}", priority=-10, retry_safe=True,
                       mem_gb=24, tags=[host]) for host in ("sparklina", "sparky")]
    for host, key in zip(("sparklina", "sparky"), holders):
        assert claim(host) == key
    group, keys = members("both-backfilled", priority=10, mem_gb=104)
    for index, host in enumerate(("sparklina", "sparky")):
        assert claim(host) is None
        holder = pool._read_json(queue.item_path(pool.CLAIMED, holders[index]))
        holder["gang_backfill"] = [_gang.elections(queue, group, 2)[index]]
        pool._write_json_atomic(queue.item_path(pool.CLAIMED, holders[index]), holder)
    clock[0] += _gang.READY_FRESH_S + 1
    for host, key in zip(("sparklina", "sparky"), holders):
        assert claim(host) is None
        assert queue.withdrawal_decisions(key), "two token-blocked members deadlocked each other"
    for host, key in zip(("sparklina", "sparky"), holders):
        finish(key, host)
    assert claim("sparklina") is None
    assert claim("sparky") == keys[1]
    assert claim("sparklina") == keys[0]



def test_release_telemetry_failure_does_not_gate_cleanup(gang_fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent, key = seeded_backfill(gang_fleet)
    finish(incumbent, "sparky")
    assert claim("sparky") is None
    assert claim("sparklina") is None
    original = pool._write_json_atomic
    def broken_observation(path, value):
        if str(path).endswith(".withdrawn-finish.json") and "gang_backfill_release" in value:
            raise OSError("telemetry write unavailable")
        return original(path, value)
    monkeypatch.setattr(pool, "_write_json_atomic", broken_observation)
    finish(key, "sparklina")
    assert queue.ledger("sparklina").held_keys() == []
    assert not queue.item_path(pool.CLAIMED, key).exists()
    assert claim("sparklina") == keys[0]
    assert claim("sparky") == keys[1]



def claim_backfill_snapshot(queue, key, tick, monkeypatch):
    """A concurrent worker's prefetched scan must not refresh the member."""
    from test_gang_reservation_1517 import CAPACITY, TIERS
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "sparklina")
    tick(0.01)
    row = pool._read_json(queue.item_path(pool.READY, key))
    return queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, adaptive_cpu=True,
                       has_gpu=True, tags=["gb10", "sparklina", _gang.TAG], ready=[row])


@pytest.mark.parametrize("peer_state", ["ready", "claimed"])
def test_a_startable_gang_never_lends_its_fresh_ready_host(
        gang_fleet, fleet, monkeypatch, peer_state):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent = waiting(gang_fleet)
    finish(incumbent, "sparky")
    record = _gang.read_group(queue, group)
    if peer_state == "ready":
        _gang.mark_ready(queue, record, record["members"][1], "sparky", clock[0])
    else:
        assert claim("sparky") == keys[1]
    ready = pool._read_json(_gang.state_dir(queue, group) / "ready-0.json")
    assert 0 <= clock[0] - ready["ready_unix"] <= _gang.READY_FRESH_S
    assert _gang.sibling_readiness(queue, record, record["members"][0],
                                  "sparklina", clock[0])["complete"]
    key = backfill(publish)
    assert claim_backfill_snapshot(queue, key, fleet[5], monkeypatch) is None, (
        "backfill borrowed a fresh-ready host after the whole gang could start")
    assert denial(key, "sparklina")["reason"] == "deferred_for_gang_reservation"
    assert queue.ledger("sparklina").held_keys() == []
    assert not queue.withdrawal_decisions(key)
    assert claim("sparklina") == keys[0]
    if peer_state == "ready":
        assert claim("sparky") == keys[1]
    assert queue.item_path(pool.CLAIMED, keys[0]).exists()
    assert queue.item_path(pool.CLAIMED, keys[1]).exists()
    assert queue.item_path(pool.READY, key).exists()


def test_a_stale_ready_member_never_lends_its_host(gang_fleet, fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent = waiting(gang_fleet)
    ready = pool._read_json(_gang.state_dir(queue, group) / "ready-0.json")
    clock[0] = ready["ready_unix"] + _gang.READY_FRESH_S + 1
    key = backfill(publish)
    assert claim_backfill_snapshot(queue, key, fleet[5], monkeypatch) is None, (
        "backfill borrowed the fenced host using an expired ready record")
    assert denial(key, "sparklina")["reason"] == "deferred_for_gang_reservation"
    assert queue.ledger("sparklina").held_keys() == []
    assert queue.item_path(pool.READY, keys[0]).exists()
    assert queue.item_path(pool.READY, key).exists()



def token_gang_claim(queue, host, tick, monkeypatch):
    """Exercise ordinary token denial, without adaptive CPU/GPU refusals."""
    from test_gang_reservation_1517 import CAPACITY, TIERS
    monkeypatch.setattr(pool.socket, "gethostname", lambda: host)
    tick(0.01)
    row = queue.claim(capacity=CAPACITY, cpu_tiers=TIERS, has_gpu=True,
                      tags=["gb10", host, _gang.TAG], adaptive_cpu=False)
    return None if row is None else row["action_key"]


def test_a_token_short_gang_member_preempts_an_unmarked_incumbent(
        gang_fleet, fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    holder = publish("before-election", priority=-10, retry_safe=True,
                     mem_gb=24, tags=["sparklina"])
    assert token_gang_claim(queue, "sparklina", fleet[5], monkeypatch) == holder
    assert "gang_backfill" not in pool._read_json(queue.item_path(pool.CLAIMED, holder))
    group, keys = members("ordinary-preemption", priority=10, mem_gb=104)
    assert token_gang_claim(queue, "sparklina", fleet[5], monkeypatch) is None
    decisions = queue.withdrawal_decisions(holder)
    assert len(decisions) == 1, "gang token denial lost ordinary preemption of unmarked work"
    assert decisions[0][1]["preempted_by"] == keys[0]
    assert queue.item_path(pool.READY, holder).exists()
    assert queue.ledger("sparklina").held_keys() == [holder]
    assert not queue.item_path(pool.CLAIMED, keys[0]).exists()


def test_ordinary_token_preemption_protects_the_members_own_lent_backfill(
        gang_fleet, fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent = waiting(gang_fleet)
    holder = backfill(publish)
    assert claim("sparklina") == holder
    assert token_gang_claim(queue, "sparklina", fleet[5], monkeypatch) is None
    assert not _gang.backfill_reclaiming(queue, _gang.read_group(queue, group))
    assert not queue.withdrawal_decisions(holder), "ordinary token denial stopped an untriggered loan"
    assert queue.ledger("sparklina").held_keys() == [holder]


def test_both_token_short_members_preempt_their_unmarked_incumbents(
        gang_fleet, fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    hosts = ("sparklina", "sparky")
    holders = [publish(f"before-election-{host}", priority=-10, retry_safe=True,
                       mem_gb=24, tags=[host]) for host in hosts]
    for host, holder in zip(hosts, holders):
        assert token_gang_claim(queue, host, fleet[5], monkeypatch) == holder
    group, keys = members("both-unmarked", priority=10, mem_gb=104)
    for host, holder, member in zip(hosts, holders, keys):
        assert token_gang_claim(queue, host, fleet[5], monkeypatch) is None
        decisions = queue.withdrawal_decisions(holder)
        assert len(decisions) == 1, "two unmarked token holders prevented ordinary gang preemption"
        assert decisions[0][1]["preempted_by"] == member
        assert queue.item_path(pool.READY, holder).exists()
        assert queue.ledger(host).held_keys() == [holder]



@pytest.mark.parametrize("writer", ["begin", "note", "observe"])
def test_election_updates_defer_to_the_member_transition(gang_fleet, writer):
    import threading
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent = waiting(gang_fleet)
    election = _gang.elections(queue, group, 2)[0]
    timing = {"election": election, "holder": "a" * 64, "generation": "b" * 64,
              "published_unix": clock[0], "requested_unix": clock[0],
              "tokens_returned_unix": None}
    if writer == "observe":
        _gang.note_backfill_preemption(queue, election, timing)
        pool._write_json_atomic(queue.superseded_dir() /
                               f"{timing['holder']}.{timing['generation']}.withdrawn-finish.json",
                               {"gang_backfill_release": {**timing, "tokens_returned_unix": clock[0] + 1}})
    path = _gang.state_dir(queue, group) / "elect-0.json"
    before = pool._read_json(path)
    entered, release = threading.Event(), threading.Event()
    def hold_member():
        with queue._transition_locked(keys[0]):
            entered.set()
            release.wait(10)
    def update():
        if writer == "begin":
            _gang.begin_backfill_reclaim(queue, election)
        elif writer == "note":
            _gang.note_backfill_preemption(queue, election, timing)
        else:
            _gang.observe_backfill_releases(queue, election)
    thread = threading.Thread(target=hold_member)
    thread.start()
    try:
        assert entered.wait(10)
        update()
        assert pool._read_json(path) == before, "election writer bypassed the held member transition"
    finally:
        release.set()
        thread.join(10)
    assert not thread.is_alive()
    update()
    after = pool._read_json(path)
    if writer == "begin":
        assert after["backfill_reclaiming"] is True
    elif writer == "note":
        assert after["backfill_preemptions"][0]["requested_unix"] == timing["requested_unix"]
    else:
        assert after["backfill_preemptions"][0]["tokens_returned_unix"] == clock[0] + 1


def test_a_late_request_observation_cannot_erase_the_token_return(gang_fleet):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    group, keys, incumbent = waiting(gang_fleet)
    election = _gang.elections(queue, group, 2)[0]
    timing = {"election": election, "holder": "a" * 64, "generation": "b" * 64,
              "published_unix": clock[0], "requested_unix": clock[0],
              "tokens_returned_unix": None}
    _gang.note_backfill_preemption(queue, election, timing)
    _gang.note_backfill_preemption(queue, election, {**timing, "tokens_returned_unix": clock[0] + 1})
    _gang.note_backfill_preemption(queue, election, {**timing, "requested_unix": clock[0] + 2})
    observed = _gang.elections(queue, group, 2)[0]["backfill_preemptions"][0]
    assert observed["tokens_returned_unix"] == clock[0] + 1, "late request erased a completed release"
    assert observed["requested_unix"] == clock[0]



def test_an_unfenced_minus_ten_row_needs_no_preemption_eligibility_read(
        gang_fleet, monkeypatch):
    queue, clock, publish, finish, claim, denial, members = gang_fleet
    key = backfill(publish)
    def forbidden_read(record):
        raise AssertionError("unfenced backfill unnecessarily read a restartability proof")
    monkeypatch.setattr(queue, "_preemption_eligible", forbidden_read)
    assert claim("sparklina") == key

