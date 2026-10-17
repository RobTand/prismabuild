"""Private queues and a single growth reservation cover actual publication."""
from contextlib import contextmanager
from dataclasses import replace

import pytest

import pbresident
from prismabuild import action_edges, filesystem_floor, pool, submission
from submission_support import patch_store
from local_resident_space import roomy_disk
from test_prepaid_writer_integration import _isolated_synthetic_launch_context
from test_filesystem_floor_1483 import ample_space


def test_deferred_release_uses_its_private_queue_for_gates_and_publication(tmp_path, monkeypatch, capsys):
    import test_deferred_action_edges as deferred
    queue, work = deferred._env(tmp_path, monkeypatch)
    template = deferred._template(tmp_path / "canonical")
    producer = deferred._producer_key(tmp_path, template, "private-queue")
    deferred._publish_producer(queue, template, producer)
    record = deferred._submit(work, monkeypatch, capsys,
        "--after", f"{producer}:{template['template_id']}")
    instance = deferred._start(queue, template, producer)
    deferred._commit(queue, template, instance, "b1", b"private release")
    queue.finish(producer, status="executed")
    fleet = deferred.dr.store_for(deferred.dr.__file__)
    default_queue = tmp_path / "default-queue-trap"
    monkeypatch.setattr(deferred.dr, "store_for", lambda *args: replace(fleet, queue_root=default_queue))
    seen = []
    judge = submission.d38_gate.judge_publication

    def observed(options, action, **kwargs):
        seen.append(kwargs["queue_root"])
        assert kwargs["queue_root"] == queue.root
        return judge(options, action, **kwargs)

    monkeypatch.setattr(submission.d38_gate, "judge_publication", observed)
    events = deferred.dr.release_tick(queue)
    released, = deferred._released(events)
    assert seen == [queue.root]
    assert queue.records.item_path(pool.READY, released["action_key"]).exists()
    assert action_edges.read_published(queue.root, record["pending_id"])["action_key"] == released["action_key"]
    assert not default_queue.exists()


@pytest.mark.parametrize("operation", ["copy", "adopt"])
def test_resident_freeze_and_publication_share_one_bounded_growth_holder(tmp_path, monkeypatch, roomy_disk, operation):
    from test_local_resident_commands import _dispatch_fixture
    resident, record, spec, queue, tier, template = _dispatch_fixture(tmp_path, monkeypatch)
    patch_store(monkeypatch, root=tmp_path)
    active = []
    acquisitions = []
    effects = []

    @contextmanager
    def bounded(queue_root, **kwargs):
        assert queue_root == queue.root
        assert not active, "a resident operation took a second growth holder"
        acquisitions.append(kwargs)
        active.append(True)
        try:
            yield
        finally:
            active.pop()

    monkeypatch.setattr(filesystem_floor, "operation", bounded)
    freeze = pbresident.movement_template

    def frozen(*args, **kwargs):
        assert active
        effects.append("freeze")
        return freeze(*args, **kwargs)

    monkeypatch.setattr(pbresident, "movement_template", frozen)
    update = resident.update_movements

    def updated(*args, **kwargs):
        assert active and effects == ["freeze"]
        effects.append("descriptor")
        return update(*args, **kwargs)

    monkeypatch.setattr(resident, "update_movements", updated)
    if operation == "copy":
        rows = pbresident.submit_copies(resident, record["set_id"],
            policy_path="/policy.json", checkout=str(tmp_path))
        row = rows["test-host"]["copy"]
    else:
        row = pbresident.submit_adoption(resident, record["set_id"], host="test-host",
            source=str(tmp_path / "manual"), policy_path="/policy.json", checkout=str(tmp_path))
    assert len(acquisitions) == 1
    assert acquisitions[0]["growth_gib"] == {template["cas"].root: 1}
    assert effects == ["freeze", "descriptor"]
    assert queue.records.item_path(pool.READY, row["action_key"]).exists()
    assert not active


@pytest.mark.parametrize("operation", ["copy", "adopt"])
def test_resident_operation_fits_one_real_gib_of_growth_headroom(tmp_path, monkeypatch, roomy_disk, ample_space, operation):
    from test_local_resident_commands import _dispatch_fixture
    from test_filesystem_floor_1483 import _register, _stable
    resident, record, spec, queue, tier, template = _dispatch_fixture(tmp_path, monkeypatch)
    patch_store(monkeypatch, root=tmp_path)
    _stable(tmp_path)
    _register(queue, tmp_path, filesystem_gib=1, filesystem_ledger="review-growth")
    ledger = pool.ResourceLedger(queue.root / filesystem_floor.FILESYSTEM_RESERVATIONS,
                                 host="review-growth")
    monkeypatch.setenv(filesystem_floor.MODE_ENV, "enforce")
    if operation == "copy":
        rows = pbresident.submit_copies(resident, record["set_id"],
            policy_path="/policy.json", checkout=str(tmp_path))
        row = rows["test-host"]["copy"]
    else:
        row = pbresident.submit_adoption(resident, record["set_id"], host="test-host",
            source=str(tmp_path / "manual"), policy_path="/policy.json", checkout=str(tmp_path))
    assert queue.records.item_path(pool.READY, row["action_key"]).exists()
    assert ledger.held().get("filesystem_gib", 0) == 0
    assert not list((ledger.base / "operations").glob("*.json"))
