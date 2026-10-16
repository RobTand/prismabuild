"""Row precedence preserves conditional observations before transition ownership."""
import pytest

from prismabuild import candidate_policy as policy, pool
from test_measurement_drains_gpu_backfill import fleet


GPU = frozenset({"gpu"})
READY_GPU = {"action_key": "waiting-gpu", "published_unix": 1000.0}


@pytest.mark.parametrize("scope,takes,gang,priority,held", [
    (None, False, None, 0, True),
    (GPU, False, None, 0, False),
    (GPU, True, None, 0, True),
    (None, False, 0, 0, False),
    (None, False, 0, 1, True),
    (GPU, True, 1, 0, False),
])
def test_gang_reservation_yields_only_a_measurement_of_equal_or_lower_priority(
    scope, takes, gang, priority, held,
):
    order = policy.ScanOrder().withhold("measurement", scope, measurement=True)
    assert order.row_held_back(takes_kind=takes, reserving_priority=gang,
                               withheld_priority=priority) is held
    assert order.withheld_for == "measurement"
    assert not policy.ScanOrder().row_held_back(takes_kind=True)


@pytest.mark.parametrize("producer,reason", [
    (False, "deferred_behind_withheld_row"), (True, None),
])
def test_whole_box_hold_precedes_placement_but_allows_a_possible_producers_export(producer, reason):
    order = policy.ScanOrder().withhold("ahead", None)
    answer = policy.row_before_placement(order, held_back=True, producer=producer)
    if reason is None:
        assert answer == policy.RowOpening(True, True)
    else:
        assert answer.reason == reason
        assert answer.evidence == {"withheld_for": "ahead", "withheld_kinds": None}
        assert answer.withhold_after is None


@pytest.mark.parametrize("row,key,released,matches,answer", [
    ({"tags": []}, "cpu", set(), False, "placement_mismatch"),
    ({"tags": []}, "cpu", set(), True, "probe"),
    ({"tags": ["here"]}, "cpu", set(), True, "open"),
    ({"needs_gpu": True}, "gpu", set(), True, "open"),
    ({"tags": ["elsewhere"]}, "released", {"released"}, None, "open"),
    ({"tags": []}, "", set(), None, "probe"),
])
def test_ready_gpu_precedence_retains_pins_released_keys_and_empty_key_order(
    row, key, released, matches, answer,
):
    order = policy.ScanOrder()
    opening = policy.RowOpening(False, False)
    result = policy.row_after_placement(
        row, opening, order=order, key=key, released=released, matches=matches,
        worker_tags=frozenset({"here"}), has_gpu=True, ready_gpu=READY_GPU, host="here")
    if answer == "probe":
        assert result is policy.RowProbe.GPU_SCOPE
    elif answer == "open":
        assert result is opening
    else:
        assert result.reason == answer
        assert result.evidence == {"worker_tags": ["here"], "worker_has_gpu": True,
                                   "required_tags": [], "needs_gpu": None}


@pytest.mark.parametrize("takes_gpu,reason", [
    (False, "deferred_for_ready_gpu"), (True, "deferred_behind_withheld_row"),
])
def test_ready_gpu_veto_precedes_the_narrow_hold_after_the_actual_gpu_parse(takes_gpu, reason):
    order = policy.ScanOrder().withhold("ahead", GPU)
    opening = policy.row_before_placement(order, held_back=True, producer=False)
    assert isinstance(opening, policy.RowOpening)
    assert policy.row_after_placement(
        {"tags": []}, opening, order=order, key="later", released=set(), matches=True,
        worker_tags=frozenset(), has_gpu=True, ready_gpu=READY_GPU,
        host="here") is policy.RowProbe.GPU_SCOPE
    result = policy.row_after_gpu_scope(
        opening, order=order, takes_gpu=takes_gpu, ready_gpu=READY_GPU, total={"gpu": 1})
    assert result.reason == reason
    assert result.order_before is order
    if takes_gpu:
        assert result.evidence == {"withheld_for": "ahead", "withheld_kinds": ["gpu"]}
    else:
        assert result.evidence == {"gpu_row": "waiting-gpu", "gpu_published_unix": 1000.0,
                                   "capacity_total": {"gpu": 1}}


@pytest.mark.parametrize("carry,binds_all", [
    (None, True), ({"mode": "gpu"}, False), ({"mode": "tokens"}, False),
])
def test_busy_room_distinguishes_expired_carry_from_live_scoped_and_whole_holds(carry, binds_all):
    order = policy.ScanOrder().carry("busy", carry)
    result = policy.busy_denial(
        order, "busy", carried=carry, room={"action_key": "busy", "room": {"cpu": 2, "gpu": 1}})
    assert result.reason == "transition_busy"
    assert result.order_before.rooms[0].binds_all is binds_all
    assert ("gpu_room_binds_all" in result.evidence) is binds_all
    assert ("withhold_carried" in result.evidence) is (carry is not None)
    assert result.evidence["gpu_room_kept"] == {"cpu": 2, "gpu": 1}
    assert result.withhold_after is None
    assert order.rooms == ()


def test_busy_without_carry_or_room_omits_evidence():
    result = policy.busy_denial(policy.ScanOrder(), "busy", carried=None, room=None)
    assert result.reason == "transition_busy"
    assert result.evidence is None
    assert result.order_before.rooms == ()


@pytest.mark.parametrize("gpu_first,has_ledger,needed", [
    (frozenset(), True, False), (frozenset({"busy"}), True, True),
    (frozenset({"busy"}), False, False),
])
def test_gpu_first_room_probe_survives_an_existing_whole_box_hold(gpu_first, has_ledger, needed):
    order = policy.ScanOrder().withhold("ahead", None)
    assert order.busy_room_needed("busy", carried=None, gpu_first=gpu_first,
                                  has_ledger=has_ledger) is needed


@pytest.mark.parametrize("error_type", [TypeError, RuntimeError])
def test_ready_gpu_snapshot_is_observed_only_after_capabilities_match(tmp_path, monkeypatch, error_type):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    reads = pool._ClaimScanReads(queue, ledger=queue.ledger(), tags=frozenset(), has_gpu=True)
    row = {"action_key": "a" * 64, "resources": {"cpu": 1, "gpu": 1},
           "container_images": ["fixture:missing"]}
    error = error_type("prior verdict snapshot refused")

    def unreadable():
        raise error

    monkeypatch.setattr(queue, "_host_denial_records", unreadable)
    arguments = dict(tags=frozenset(), has_gpu=True, total={"cpu": 2, "gpu": 1},
                     gpu_controller=None, offers=lambda: (), observed_images=[], reads=reads)
    assert queue._ready_gpu_for_host([row], **arguments) is None
    row.pop("container_images")
    if error_type is TypeError:
        assert queue._ready_gpu_for_host([row], **arguments) is None
    else:
        with pytest.raises(RuntimeError) as caught:
            queue._ready_gpu_for_host([row], **arguments)
        assert caught.value is error


def test_real_whole_box_denial_does_not_parse_a_later_rows_gpu_demand(fleet, monkeypatch):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder = publish("incumbent-before-row-gates", timeout_s=1200)
    assert claim() == holder
    waiting = publish("measurement-before-row-gates", measurement=True, pinned=True)
    later = publish("cpu-after-measurement", gpu=0)
    demands = queue._demands_withheld_kind

    def no_early_gpu_parse(row, kinds):
        if row["action_key"] == later:
            raise KeyError("the whole-box denial must precede this GPU parse")
        return demands(row, kinds)

    monkeypatch.setattr(queue, "_demands_withheld_kind", no_early_gpu_parse)
    assert claim() is None
    assert denial(waiting)["reason"] == "adaptive_cpu_refused_withholding"
    assert denial(later)["reason"] == "deferred_behind_withheld_row"
    assert denial(later)["evidence"]["withheld_kinds"] is None
    assert queue.passes(later) == 0
    assert queue.ledger().held_keys() == [holder]
    assert queue.records.item_path(pool.record_store.READY, later).exists()
