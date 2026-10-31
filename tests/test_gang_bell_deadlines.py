"""Publication-bound timers expose existing clocks without starting new ones."""
from copy import deepcopy
import math

import pytest

from prismabuild import _gang, pool
from prismabuild.gang_deadlines import member_deadlines
from prismabuild.host_wait import DeadlineReason


GROUP = "a" * 32
KEY = "b" * 64


@pytest.fixture
def evidence():
    entry = {"index": 0, "action_key": KEY, "published_unix": 80., "priority": 0}
    record = {"group": GROUP, "size": 2, "skew_s": 27., "members": [entry,
              {**entry, "index": 1, "action_key": "c" * 64, "published_unix": 81.}]}
    item = {**entry, "gang": {"group": GROUP, "size": 2, "index": 0}}
    ready = {"schema": _gang.READY_SCHEMA, "group": GROUP, "index": 0,
             "action_key": KEY, "ready_unix": 90., "host": "worker"}
    terminal = {"schema": _gang.TERMINAL_SCHEMA, "group": GROUP, "member": KEY,
                "signature": "original-proof", "first_seen_unix": 85.}
    return record, item, ready, terminal


def deadlines(evidence, *, wall_now=100., monotonic_now=100., **overrides):
    record, item, ready, terminal = evidence
    inputs = dict(ready=ready, terminal=terminal, terminal_signature="original-proof",
                  barrier_deadline=117., effective_heartbeat_unix=95.)
    inputs.update(overrides)
    return {result.source: result for result in member_deadlines(record, item,
        wall_now=wall_now, monotonic_now=monotonic_now, **inputs)}


def test_original_individual_clocks_and_publication_identity(evidence):
    result = deadlines(evidence)
    assert {source: value.deadline.monotonic_at for source, value in result.items()} == {
        "readiness": math.nextafter(120., math.inf), "terminal": 205.,
        "skew": 117., "lease": math.nextafter(395., math.inf)}
    for value in result.values():
        assert (value.member.group, value.member.index, value.member.action_key,
                value.member.published_unix) == (GROUP, 0, KEY, 80.)
    assert result["skew"].deadline.reason is DeadlineReason.POLICY_EXPIRY
    assert result["readiness"].deadline.reason is DeadlineReason.EVIDENCE_EXPIRY


def test_reconnect_and_lease_renewal_do_not_restart_other_clocks(evidence):
    original = deadlines(evidence)
    renewed = deadlines(evidence, wall_now=110., monotonic_now=110.,
                        effective_heartbeat_unix=109.)
    for source in ("readiness", "terminal", "skew"):
        assert renewed[source] == original[source]
    assert renewed["lease"].deadline.monotonic_at == math.nextafter(409., math.inf)
    expired = deadlines(evidence, wall_now=500., monotonic_now=500.)
    assert all(value.deadline.monotonic_at < 500. for value in expired.values())


@pytest.mark.parametrize("field,value", [("action_key", "d" * 64), ("published_unix", 81.),
    ("gang", {"group": "d" * 32, "size": 2, "index": 0}),
    ("gang", {"group": GROUP, "size": 3, "index": 0})])
def test_wrong_member_or_publication_cannot_inherit_clocks(evidence, field, value):
    evidence[1][field] = value
    with pytest.raises(_gang.GangContractError):
        deadlines(evidence)


@pytest.mark.parametrize("kind,field,value", [
    ("ready", "group", "d" * 32), ("ready", "index", 1),
    ("ready", "action_key", "d" * 64), ("ready", "schema", "old"),
    ("ready", "ready_unix", math.nan), ("terminal", "signature", "new-proof"),
    ("terminal", "member", "d" * 64), ("terminal", "group", "d" * 32),
    ("terminal", "first_seen_unix", 101.), ("terminal", "first_seen_unix", True),
])
def test_unrelated_or_unknown_evidence_never_arms_a_timer(evidence, kind, field, value):
    evidence[2 if kind == "ready" else 3][field] = value
    result = deadlines(evidence)
    assert ("readiness" if kind == "ready" else "terminal") not in result


def test_predicate_retirement_cancels_only_its_clocks(evidence):
    result = deadlines(evidence, ready=None, terminal=None, barrier_deadline=None,
                       effective_heartbeat_unix=None)
    assert result == {}
    assert set(deadlines(evidence, terminal_signature="")) == {"readiness", "skew", "lease"}


def test_individual_siblings_do_not_share_a_minimum_or_restart(evidence):
    sibling = deepcopy(evidence)
    sibling[1].update(sibling[0]["members"][1])
    sibling[1]["gang"]["index"] = 1
    sibling[2].update(index=1, action_key="c" * 64, ready_unix=96.)
    result = deadlines(sibling, terminal=None, barrier_deadline=127.)
    assert result["readiness"].deadline.monotonic_at == math.nextafter(126., math.inf)
    assert result["readiness"].member.published_unix == 81.
    assert result["skew"].deadline.monotonic_at == 127.


def test_future_ready_has_its_original_visibility_boundary(evidence):
    evidence[2]["ready_unix"] = 105.
    result = deadlines(evidence)
    assert result["readiness-visible"].deadline.monotonic_at == 105.
    assert result["readiness"].deadline.monotonic_at == math.nextafter(135., math.inf)


def test_inclusive_expiry_survives_conversion_to_a_larger_clock(evidence):
    result = deadlines(evidence, wall_now=100., monotonic_now=10000.)
    assert result["readiness"].deadline.monotonic_at == math.nextafter(10020., math.inf)
    assert result["lease"].deadline.monotonic_at == math.nextafter(10295., math.inf)
    assert result["terminal"].deadline.monotonic_at == 10105.


@pytest.mark.parametrize("wall_now,monotonic_now", [
    (1760000000., 1000.), (100., 10000.),
])
def test_readiness_timer_crosses_the_authoritative_expiry(
        evidence, tmp_path, wall_now, monotonic_now):
    record, item, _, _ = evidence
    queue = pool.PoolQueue(tmp_path)
    stamp = wall_now - 10.
    _gang.mark_ready(queue, record, record["members"][0], "worker", stamp)
    ready = pool.record_store.read_document(
        _gang.state_dir(queue, GROUP) / "ready-0.json")
    sibling = record["members"][1]
    assert _gang.sibling_readiness(queue, record, sibling, "sibling",
                                  stamp + _gang.READY_FRESH_S)["complete"]
    expiry = member_deadlines(record, item, wall_now=wall_now,
        monotonic_now=monotonic_now, ready=ready)[0].deadline.monotonic_at
    wall_at_expiry = wall_now + (expiry - monotonic_now)
    assert not _gang.sibling_readiness(queue, record, sibling, "sibling",
                                      wall_at_expiry)["complete"]
    reconnected = member_deadlines(record, item, wall_now=wall_now + 5.,
        monotonic_now=monotonic_now + 5., ready=ready)[0].deadline.monotonic_at
    assert reconnected == expiry


@pytest.mark.parametrize("wall_now,monotonic_now", [
    (1760000000., 1000.), (100., 10000.),
])
@pytest.mark.parametrize("timeout_s", [pool.LEASE_TIMEOUT_S, 17.25])
def test_lease_timer_crosses_the_authoritative_expiry(
        evidence, tmp_path, monkeypatch, wall_now, monotonic_now, timeout_s):
    record, item, _, _ = evidence
    queue = pool.PoolQueue(tmp_path)
    stamp = wall_now - 10.
    clock = {"wall": stamp + timeout_s}
    monkeypatch.setattr(pool, "_now", lambda: clock["wall"])
    lease = {"heartbeat_unix": stamp}
    assert not queue._heartbeat_cleanup_due(lease, timeout_s=timeout_s)
    expiry = member_deadlines(record, item, wall_now=wall_now,
        monotonic_now=monotonic_now, effective_heartbeat_unix=stamp,
        lease_timeout_s=timeout_s)[0].deadline.monotonic_at
    clock["wall"] = wall_now + (expiry - monotonic_now)
    assert queue._heartbeat_cleanup_due(lease, timeout_s=timeout_s)
    reconnected = member_deadlines(record, item, wall_now=wall_now + 5.,
        monotonic_now=monotonic_now + 5., effective_heartbeat_unix=stamp,
        lease_timeout_s=timeout_s)[0].deadline.monotonic_at
    assert reconnected == expiry


@pytest.mark.parametrize("wall_now,monotonic_now", [
    (1760000000., 1000.), (100., 10000.),
])
def test_strict_expiry_already_crossed_by_one_wall_instant_stays_due(
        evidence, wall_now, monotonic_now):
    record, item, ready, _ = evidence
    ready["ready_unix"] = wall_now - _gang.READY_FRESH_S
    result = member_deadlines(record, item,
        wall_now=math.nextafter(wall_now, math.inf), monotonic_now=monotonic_now,
        ready=ready, effective_heartbeat_unix=wall_now - pool.LEASE_TIMEOUT_S)
    assert {value.source for value in result} == {"readiness", "lease"}
    assert all(value.deadline.monotonic_at <= monotonic_now for value in result)


def test_actual_ready_renewal_moves_only_its_evidence_expiry(evidence):
    original = deadlines(evidence)
    evidence[2]["ready_unix"] = 99.
    renewed = deadlines(evidence)
    assert renewed["readiness"].deadline.monotonic_at == math.nextafter(129., math.inf)
    for source in ("terminal", "skew", "lease"):
        assert renewed[source] == original[source]


@pytest.mark.parametrize("options", [{"wall_now": math.inf}, {"monotonic_now": math.nan},
    {"effective_heartbeat_unix": True}, {"lease_timeout_s": -1.},
    {"lease_timeout_s": math.inf}, {"barrier_deadline": math.nan}])
def test_invalid_clock_evidence_does_not_produce_waits(evidence, options):
    with pytest.raises(ValueError):
        deadlines(evidence, **options)
