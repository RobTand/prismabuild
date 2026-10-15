"""Scan fairness preserves room boundaries without owning capacity or reads."""
from __future__ import annotations

import json

import pytest

from prismabuild import adaptive_cpu, pool
from prismabuild.candidate_policy import KeptRoom, ScanOrder, room_fits, room_taken


GPU = frozenset({"gpu"})
CPU = frozenset({"cpu"})


@pytest.mark.parametrize("holds,owner,kinds", [
    ([("a", GPU)], "a", GPU),
    ([("a", GPU), ("b", CPU)], "a", frozenset({"cpu", "gpu"})),
    ([("a", GPU), ("b", None)], "b", None),
    ([("a", None), ("b", GPU)], "a", None),
    ([("a", None), ("b", None)], "a", None),
])
def test_hold_join_preserves_first_owner_union_and_whole_box_promotion(holds, owner, kinds):
    initial = ScanOrder()
    order = initial
    for key, scope in holds:
        order = order.withhold(key, scope)
    assert (order.withheld_for, order.withheld_kinds) == (owner, kinds)
    assert order.whole_box_held is (kinds is None)
    assert initial.withheld_for is None
    assert initial.rooms == ()


def test_fresh_and_carried_measurement_membership_have_distinct_meaning():
    scoped = ScanOrder().carry("carried", {"mode": "gpu", "drain_until_unix": 30.0})
    whole = scoped.withhold("fresh", None, measurement=True)
    later = whole.withhold("later", GPU, measurement=True)
    assert later.withheld_for == "fresh"
    assert later.measurement_withholds == frozenset({"fresh", "later"})
    assert later.carried_measurement_withholds == frozenset({"carried"})
    assert scoped.measurement_withholds == frozenset()
    assert not scoped.whole_box_held


def test_carry_scope_and_deadline_membership_do_not_reinterpret_the_episode():
    ordinary = ScanOrder().carry("gpu", {"mode": "gpu"})
    assert ordinary.withheld_kinds == GPU
    assert ordinary.carried_measurement_withholds == frozenset()
    carried = ordinary.carry("unknown-mode", {"mode": "unknown", "drain_until_unix": None})
    assert carried.whole_box_held
    assert carried.withheld_for == "unknown-mode"
    assert carried.carried_measurement_withholds == frozenset({"unknown-mode"})
    assert ordinary.carry("absent", None) == ordinary


def test_kept_room_detaches_input_and_does_not_mutate_an_earlier_scan():
    demand = {"cpu": 2, "gpu": 1}
    room = KeptRoom("a" * 64, demand, "transition_busy", binds_all=True)
    original = ScanOrder()
    retained = original.keep(room)
    demand["cpu"] = 99
    assert retained.rooms[0].room == {"cpu": 2, "gpu": 1}
    assert original.rooms == ()
    with pytest.raises(TypeError):
        retained.rooms[0].room["cpu"] = 3


def test_first_ordered_room_that_fits_now_and_would_be_taken_names_the_denial():
    rooms = [KeptRoom("too-big", {"cpu": 10, "gpu": 1}, "ignored"),
             KeptRoom("a" * 64, {"cpu": 7, "gpu": 1}, "reservation_unavailable_starved", True),
             KeptRoom("b" * 64, {"cpu": 8, "gpu": 1}, "transition_busy", True)]
    available = {"cpu": 8, "gpu": 1}
    assert room_taken(available, rooms, {"cpu": 2}) == {
        "gpu_row": "a" * 12, "room": {"cpu": 7, "gpu": 1},
        "kept_for": "reservation_unavailable_starved", "available": available,
        "demand": {"cpu": 2}}


def test_room_binds_at_release_and_allows_work_that_fits_beside_it():
    rooms = [KeptRoom("waiting", {"cpu": 2, "gpu": 1}, "transition_busy", True)]
    assert room_taken({"cpu": 4, "gpu": 0}, rooms, {"cpu": 2, "gpu": 1}) is None
    assert room_taken({"cpu": 4, "gpu": 1}, rooms, {"cpu": 2, "gpu": 1})["gpu_row"] == "waiting"
    assert room_taken({"cpu": 4, "gpu": 1}, rooms, {"cpu": 2}) is None
    assert room_taken({"cpu": 4, "gpu": 2}, rooms, {"cpu": 2, "gpu": 1}) is None


@pytest.mark.parametrize("reservation,free,evidence,kept", [
    ({"cpu": 2, "gpu": 1}, None, {}, False),
    ({"cpu": 2}, {"cpu": 0}, {}, False),
    ({"gpu": 0}, {}, {}, False),
    ({"gpu": "bad"}, {}, {}, False),
    ({"gpu": None}, {}, {}, False),
    ({"cpu": 2, "gpu": 1}, {"cpu": 2, "gpu": 1}, {"gpu_room_fit_at_refusal": True}, False),
    ({"cpu": "2", "gpu": True}, {"cpu": 2, "gpu": 0},
     {"gpu_room_kept": {"cpu": 2, "gpu": 1}, "gpu_room_binds_all": True}, True),
])
def test_refused_room_selection_preserves_unknown_coercion_and_fit(reservation, free, evidence, kept):
    initial = ScanOrder()
    retained, actual = initial.keep_refused_room("row", reservation, "reservation_unavailable", free)
    assert actual == evidence
    assert bool(retained.rooms) is kept
    assert initial.rooms == ()
    if kept:
        assert retained.rooms[0].action_key == "row"
        assert retained.rooms[0].reason == "reservation_unavailable"
        assert retained.rooms[0].binds_all


def test_refused_room_does_not_read_unknown_counts_and_preserves_overflow_boundary():
    class Count:
        def __int__(self):
            raise AssertionError("unknown availability cannot inspect counts")

    order = ScanOrder()
    assert order.keep_refused_room("row", {"gpu": Count()}, "refused", None) == (order, {})
    with pytest.raises(OverflowError):
        order.keep_refused_room("row", {"gpu": float("inf")}, "refused", {})


def test_room_fit_keeps_foreign_count_coercion_and_missing_kind_semantics():
    assert room_fits({"cpu": 2}, {"cpu": "2"})
    assert not room_fits({"cpu": 2}, {"gpu": 1})
    assert room_fits({}, {"gpu": -1})
    with pytest.raises(ValueError):
        room_fits({}, {"gpu": "bad"})


@pytest.mark.parametrize("whole_box_already_held", [False, True], ids=["expired-carry", "suppressed-carry"])
def test_busy_row_distinguishes_expired_carry_from_a_whole_box_hold(
    tmp_path, monkeypatch, whole_box_already_held,
):
    from test_drain_boundary_order import _unheld, publish

    queue = pool.PoolQueue(tmp_path / "queue")
    capacity = {"cpu": 2, "gpu": 1}
    holder, head, busy, tail = (digit * 64 for digit in "0abc")
    monkeypatch.setattr(pool, "_now", lambda: 1000.0)
    ledger = queue.ledger()
    ledger.ensure_capacity(capacity)
    assert ledger.acquire(holder, capacity)
    if whole_box_already_held:
        publish(queue, head, resources={"cpu": 2}, priority=10)
        for _ in range(pool.STARVATION_FLOOR - 1):
            queue.record_pass(head)
    publish(queue, busy, resources={"cpu": 1, "gpu": 1}, dependent_of=holder, priority=0)
    publish(queue, tail, resources={"cpu": 1}, priority=-10)
    item = json.loads(queue.item_path(pool.READY, busy).read_text())
    queue.record_denial(item, "reservation_unavailable_withholding", {"withhold": {
        "withhold": True, "mode": "tokens", "withhold_age_s": pool.WITHHOLD_CEILING_S + 1.0}})
    transition = queue._transition_locked
    record = queue.record_denial
    carried_lookup = queue._carried_withhold
    room_lookup = queue._ready_gpu_row_room
    observations = []

    def contested(key, **kwargs):
        if key == busy:
            if not whole_box_already_held:
                ledger.release(holder)
            return _unheld()
        return transition(key, **kwargs)

    def deny(row, reason, evidence=None):
        record(row, reason, evidence)
        if row.get("action_key") == head:
            assert reason == "reservation_unavailable_withholding"
            ledger.release(holder)

    def carry(records, row, **kwargs):
        observations.append(("carry", row["action_key"]))
        return carried_lookup(records, row, **kwargs)

    def room(row, **kwargs):
        observations.append(("room", row["action_key"]))
        return room_lookup(row, **kwargs)

    monkeypatch.setattr(queue, "_transition_locked", contested)
    monkeypatch.setattr(queue, "record_denial", deny)
    monkeypatch.setattr(queue, "_carried_withhold", carry)
    monkeypatch.setattr(queue, "_ready_gpu_row_room", room)
    claimed = queue.claim(capacity=capacity)
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = json.loads(path.read_text())["records"].values()
    busy_denial = next(row for row in records if row["action_key"] == busy)
    assert busy_denial["reason"] == "transition_busy"
    assert "withhold_carried" not in busy_denial["evidence"]
    if whole_box_already_held:
        assert claimed is None
        assert observations == []
        assert "gpu_room_kept" not in busy_denial["evidence"]
        assert next(row for row in records if row["action_key"] == tail)["reason"] == "deferred_behind_withheld_row"
    else:
        assert claimed["action_key"] == tail
        assert observations == [("carry", busy), ("room", busy)]
        assert busy_denial["evidence"]["gpu_room_binds_all"] is True
