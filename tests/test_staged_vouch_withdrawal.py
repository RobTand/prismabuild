"""A completed reissue retires every part of its donor's vouch."""
from pathlib import Path
import sys
from contextlib import contextmanager
import threading

import pytest

sys.path[:0] = [str(Path(__file__).resolve().parents[1] / "src"),
                str(Path(__file__).resolve().parents[1] / "tools/fleet")]

from prismabuild import pool, reader_lease, residency_map
import test_a_resident_range_is_adopted_rather_than_recopied as ranges
from prismabuild import staged_path


def test_adoption_clears_the_donors_retiring_generation(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    queue.tier(ranges.TIER).mint({"stage_gib": ranges.STAGE_GIB})
    stage = tmp_path / "stage"
    stage.mkdir()
    staged_path.register_stage_root(queue, tier_id=ranges.TIER, stage_root=stage)
    donor, successor = "d" * 64, "e" * 64
    paths = ranges._stage_range(queue, mover=donor, consumer=ranges.FIRST, stage=stage)
    identities = [reader_lease.stat_identity(str(path)) for path in paths]
    root = queue.records.residency_fragment_root()
    material = reader_lease.read_material(root, ranges.FIRST, donor)
    mark = reader_lease.write_retiring(
        reader_lease.leases_root(queue, root), consumer_action_key=ranges.FIRST,
        mover_action_key=donor, generation=material["generation"])

    result = staged_path.adopt(
        queue, old_key=donor, new_key=successor, consumer_action_key=ranges.SECOND,
        tier_id=ranges.TIER, phase="phase-0", range_start_bytes=0,
        range_end_bytes=ranges.PHASE_GIB * ranges.GIB, residency_root=root)

    assert result["adopted"] is True, result
    assert identities == [reader_lease.stat_identity(str(path)) for path in paths]
    assert queue.tier(ranges.TIER).holdings(donor) == {}
    assert queue.tier(ranges.TIER).holdings(successor) == {"stage_gib": ranges.PHASE_GIB}
    assert residency_map.fragment_path(root, ranges.SECOND, successor).is_file()
    assert not residency_map.fragment_path(root, ranges.FIRST, donor).exists()
    assert reader_lease.read_material(root, ranges.FIRST, donor) is None
    assert not mark.exists(), "the withdrawn donor still closes a material generation"


@pytest.mark.parametrize("changed_owner", ["donor", "successor"])
def test_an_owner_that_becomes_unknown_before_adoptions_lock_retains_authority(
        tmp_path, monkeypatch, changed_owner):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    queue.tier(ranges.TIER).mint({"stage_gib": ranges.STAGE_GIB})
    stage = tmp_path / "stage"
    stage.mkdir()
    staged_path.register_stage_root(queue, tier_id=ranges.TIER, stage_root=stage)
    donor, successor = "d" * 64, "e" * 64
    paths = ranges._stage_range(queue, mover=donor, consumer=ranges.FIRST, stage=stage)
    identities = [reader_lease.stat_identity(str(path)) for path in paths]
    root = queue.records.residency_fragment_root()
    unknown = (reader_lease.material_path(root, ranges.FIRST, donor)
               if changed_owner == "donor" else
               residency_map.fragment_path(root, ranges.SECOND, successor))
    before_lock, changed = threading.Event(), threading.Event()
    errors = []
    actual_lock = queue.stage_ownership_lock

    def writer():
        try:
            assert before_lock.wait(10)
            with actual_lock(stage):
                unknown.parent.mkdir(parents=True, exist_ok=True)
                unknown.write_bytes(b"{unknown-owner")
        except BaseException as exc:
            errors.append(exc)
        finally:
            changed.set()

    @contextmanager
    def after_hint(*args, **kwargs):
        before_lock.set()
        assert changed.wait(10)
        with actual_lock(*args, **kwargs) as acquired:
            yield acquired

    monkeypatch.setattr(queue, "stage_ownership_lock", after_hint)
    thread = threading.Thread(target=writer)
    thread.start()
    try:
        result = staged_path.adopt(
            queue, old_key=donor, new_key=successor, consumer_action_key=ranges.SECOND,
            tier_id=ranges.TIER, phase="phase-0", range_start_bytes=0,
            range_end_bytes=ranges.PHASE_GIB * ranges.GIB, residency_root=root)
    finally:
        thread.join(10)
    assert not thread.is_alive() and not errors, errors
    assert result["adopted"] is False, result
    assert identities == [reader_lease.stat_identity(str(path)) for path in paths]
    assert queue.tier(ranges.TIER).holdings(donor) == {"stage_gib": ranges.PHASE_GIB}
    assert queue.tier(ranges.TIER).holdings(successor) == {}
    assert unknown.read_bytes() == b"{unknown-owner"


def test_an_egress_withdraws_the_fragment_then_its_date_then_its_retiring_mark(
        tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    queue.tier(ranges.TIER).mint({"stage_gib": ranges.STAGE_GIB})
    stage = tmp_path / "stage"
    stage.mkdir()
    staged_path.register_stage_root(queue, tier_id=ranges.TIER, stage_root=stage)
    donor = "d" * 64
    ranges._stage_range(queue, mover=donor, consumer=ranges.FIRST, stage=stage)
    root = queue.records.residency_fragment_root()
    material = reader_lease.read_material(root, ranges.FIRST, donor)
    mark = reader_lease.write_retiring(
        reader_lease.leases_root(queue, root), consumer_action_key=ranges.FIRST,
        mover_action_key=donor, generation=material["generation"])
    names = {residency_map.fragment_path(root, ranges.FIRST, donor): "fragment",
             reader_lease.material_path(root, ranges.FIRST, donor): "material", mark: "retiring"}
    order = []
    actual_unlink = Path.unlink

    def unlink(path, *args, **kwargs):
        result = actual_unlink(path, *args, **kwargs)
        if path in names:
            order.append(names[path])
        return result

    monkeypatch.setattr(Path, "unlink", unlink)
    result = staged_path.evict(queue, donor, consumer_action_key=ranges.FIRST,
                               stage_root=str(stage), residency_root=root)
    assert result["complete"] is True, result
    assert order == ["fragment", "material", "retiring"]


@pytest.mark.parametrize("active_owner", ["donor", "successor"])
def test_adoption_keeps_a_range_with_a_live_mover_key(tmp_path, active_owner):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    queue.tier(ranges.TIER).mint({"stage_gib": ranges.STAGE_GIB})
    stage = tmp_path / "stage"
    stage.mkdir()
    staged_path.register_stage_root(queue, tier_id=ranges.TIER, stage_root=stage)
    donor, successor = "d" * 64, "e" * 64
    paths = ranges._stage_range(queue, mover=donor, consumer=ranges.FIRST, stage=stage)
    identities = [reader_lease.stat_identity(str(path)) for path in paths]
    active = donor if active_owner == "donor" else successor
    queue.publish(action_key=active, cas_root=tmp_path / "cas", checkout_root=tmp_path,
                  worker_script=tmp_path / "worker.py", resources={"cpu": 1, "mem_gb": 1})
    root = queue.records.residency_fragment_root()

    result = staged_path.adopt(
        queue, old_key=donor, new_key=successor, consumer_action_key=ranges.SECOND,
        tier_id=ranges.TIER, phase="phase-0", range_start_bytes=0,
        range_end_bytes=ranges.PHASE_GIB * ranges.GIB, residency_root=root)

    assert result["adopted"] is False, result
    assert queue.records.item(pool.record_store.READY, active).value["action_key"] == active
    assert identities == [reader_lease.stat_identity(str(path)) for path in paths]
    assert queue.tier(ranges.TIER).holdings(donor) == {"stage_gib": ranges.PHASE_GIB}
    assert queue.tier(ranges.TIER).holdings(successor) == {}
    assert residency_map.fragment_path(root, ranges.FIRST, donor).is_file()
    assert not residency_map.fragment_path(root, ranges.SECOND, successor).exists()


@pytest.mark.parametrize("corrupt_successor", [False, True])
def test_adoption_reconciles_a_crash_after_the_complete_handover(
        tmp_path, monkeypatch, corrupt_successor):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    queue.tier(ranges.TIER).mint({"stage_gib": ranges.STAGE_GIB})
    stage = tmp_path / "stage"
    stage.mkdir()
    staged_path.register_stage_root(queue, tier_id=ranges.TIER, stage_root=stage)
    donor, successor = "d" * 64, "e" * 64
    paths = ranges._stage_range(queue, mover=donor, consumer=ranges.FIRST, stage=stage)
    identities = [reader_lease.stat_identity(str(path)) for path in paths]
    root = queue.records.residency_fragment_root()
    material = reader_lease.read_material(root, ranges.FIRST, donor)
    mark = reader_lease.write_retiring(
        reader_lease.leases_root(queue, root), consumer_action_key=ranges.FIRST,
        mover_action_key=donor, generation=material["generation"])
    ledger_type = type(queue.tier(ranges.TIER))
    actual_handover = ledger_type.handover

    class PowerLost(BaseException):
        pass

    def handover(ledger, old_key, new_key):
        actual_handover(ledger, old_key, new_key)
        raise PowerLost("after token renames, before donor withdrawal and receipt")

    arguments = dict(old_key=donor, new_key=successor, consumer_action_key=ranges.SECOND,
                     tier_id=ranges.TIER, phase="phase-0", range_start_bytes=0,
                     range_end_bytes=ranges.PHASE_GIB * ranges.GIB, residency_root=root)
    monkeypatch.setattr(ledger_type, "handover", handover)
    with pytest.raises(PowerLost):
        staged_path.adopt(queue, **arguments)
    monkeypatch.undo()
    successor_path = residency_map.fragment_path(root, ranges.SECOND, successor)
    assert queue.tier(ranges.TIER).holdings(donor) == {}
    assert queue.tier(ranges.TIER).holdings(successor) == {"stage_gib": ranges.PHASE_GIB}
    assert queue.move_record(successor) is None
    if corrupt_successor:
        with queue.stage_ownership_lock(stage):
            successor_path.write_bytes(b"{unknown-successor")

    result = staged_path.adopt(queue, **arguments)

    assert identities == [reader_lease.stat_identity(str(path)) for path in paths]
    assert queue.tier(ranges.TIER).holdings(successor) == {"stage_gib": ranges.PHASE_GIB}
    if corrupt_successor:
        assert result["adopted"] is False, result
        assert successor_path.read_bytes() == b"{unknown-successor"
        assert residency_map.fragment_path(root, ranges.FIRST, donor).is_file()
        assert mark.is_file()
        assert queue.move_record(successor) is None
    else:
        assert result["adopted"] is True, result
        assert queue.move_record(successor)["complete"] is True
        assert not residency_map.fragment_path(root, ranges.FIRST, donor).exists()
        assert reader_lease.read_material(root, ranges.FIRST, donor) is None
        assert not mark.exists()
