"""Used-path refusals stop real warming and movement before publication."""
from pathlib import Path
import json

import pytest

from prismabuild import filesystem_floor as floor
from prismabuild import local_resident, pool, reader_lease, resident_sets, stage_move
from prewarm_fixture import Fleet
from test_a_mover_stages_the_range_it_declared_and_no_more import _args, _manifest, _write
from test_local_resident_mover import world
from local_resident_space import roomy_disk  # noqa: F401
from test_prewarm_rollout import role  # noqa: F401
from filesystem_floor_fixture import closed_coverage

pytestmark = pytest.mark.usefixtures("roomy_disk")
_enforce_paths = floor.enforce_paths


def guard(monkeypatch, denied=None, *, on_check=None):
    checked = []
    initialized = set()
    space = dict(size_bytes=100 * floor.GIB, free_bytes=10 * floor.GIB,
                 size_inodes=1000, free_inodes=900, floor_inodes=50, inode_refusal=None)

    def identify(path):
        blocked = denied is not None and Path(path).is_relative_to(denied)
        return {**space, "path": str(path),
                "key": "uuid:caller-refused" if blocked else "uuid:caller-test",
                "source": "/dev/test-refused" if blocked else "/dev/test-only",
                "device": 2 if blocked else 1, "mount_id": 2 if blocked else 1, "fstype": "ext4",
                **({"free_bytes": 0} if blocked else {})}

    monkeypatch.setattr(floor, "identify", identify)
    monkeypatch.setattr(floor, "_sample", lambda found:
        {**space, "free_bytes": found["free_bytes"]})
    monkeypatch.setattr(floor, "_host_addresses", lambda: [])
    monkeypatch.setenv(floor.MODE_ENV, "enforce")

    def enforce(queue_root, paths, *, label):
        paths = [Path(path) for path in paths]
        if str(queue_root) not in initialized:
            for root in [queue_root] + ([] if denied is None else [denied]):
                binding = floor.register(queue_root, root)["binding"]
                floor.register(queue_root, root,
                    coverage=closed_coverage(binding, additional_bytes=4 * floor.GIB))
            initialized.add(str(queue_root))
        checked.extend(paths)
        if on_check is not None:
            on_check(paths)
        return _enforce_paths(queue_root, paths, label=label)

    monkeypatch.setattr(floor, "enforce_paths", enforce)
    return checked


@pytest.mark.parametrize("allowed", [False, True])
def test_canonical_read_only_prewarm_checks_source_before_receipt(tmp_path, monkeypatch, allowed):
    fleet = Fleet(tmp_path)
    source, size = fleet.file("payload", 32)
    key = fleet.action("floor-source", [(source, size)])
    mounted = tmp_path / "mounted"
    mounted.mkdir()
    actual_source = mounted / "payload"
    actual_source.write_bytes(Path(source).read_bytes())
    checked = guard(monkeypatch, None if allowed else actual_source)

    event = fleet.cycle(fleet.args(mount_map=[f"{fleet.mount}={mounted}"]))

    if allowed:
        assert fleet.queue.prewarm(key)["status"] == "complete"
    else:
        assert fleet.queue.prewarm(key) is None
        assert not event["warmed"]
        assert any(row["action_key"] == key and row["reason"] == "filesystem_floor"
                   for row in event["skipped"])
    assert actual_source in checked


@pytest.mark.parametrize("blocked", ["source", "created_destination", "reused_destination"])
def test_stage_refusal_stops_copy_and_fragment_even_on_reuse(tmp_path, monkeypatch, blocked):
    source = tmp_path / "origin" / "payload"
    entry = _write(source, b"payload")
    args = _args(tmp_path, _manifest(source.parent, [entry]), start=0, end=7,
                 max_readers=1, readers=1)
    destination = Path(args.stage_root)
    guard(monkeypatch)
    if blocked == "reused_destination":
        assert stage_move.move(args)["complete"]
    snapshots = {p: p.read_bytes() for p in destination.rglob("*") if p.is_file()}
    refused = source if blocked == "source" else destination

    def created(paths):
        if destination in paths:
            assert destination.is_dir(), "check the destination after creation"

    checked = guard(monkeypatch, refused, on_check=created)
    with pytest.raises(floor.FloorRefused):
        stage_move.move(args)

    assert refused in checked
    assert {p: p.read_bytes() for p in destination.rglob("*") if p.is_file()} == snapshots
    if blocked != "reused_destination":
        assert not pool.PoolQueue(args.pool_root).move_record(args.action_key)
        from prismabuild import residency_map
        assert not residency_map.read_fragments(args.residency_root, args.consumer_action_key)


@pytest.mark.parametrize("state", ["new", "partial", "final"])
def test_local_resident_refusal_preserves_existing_tree_and_state(tmp_path, monkeypatch, state):
    store, record, spec = world(tmp_path)
    root = Path(spec["root"])
    final = root / record["set_id"]
    partial = root / (record["set_id"] + ".partial")
    guard(monkeypatch)
    if state == "final":
        local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
    elif state == "partial":
        partial.mkdir()
        (partial / "weights").write_bytes(b"weights")
    previous = store.read_copy(record["set_id"], "test-host")
    denied = final if state == "final" else partial
    checked = guard(monkeypatch, denied)

    with pytest.raises(floor.FloorRefused):
        local_resident.copy(store, record["set_id"], "test-host", spec, now=120)

    assert denied in checked
    assert store.read_copy(record["set_id"], "test-host") == previous
    if state == "final":
        assert (final / "weights").read_bytes() == b"weights"
    else:
        assert not final.exists()


def test_local_resident_checks_actual_stage_source_while_pin_is_held(tmp_path, monkeypatch):
    import test_a_promotion_handoff_defers_the_source_retirement as fixture
    queue, stage, staged, manifest_sha, manifest_path = fixture._world(tmp_path)
    manifest = json.loads(manifest_path.read_text())
    queue.tier("local:test-host").mint({"local_gib": 1})
    store = resident_sets.ResidentSets(queue.root)
    record = store.publish(manifest=manifest, canonical_root=str(tmp_path / "origin"),
        hosts=["test-host"], lease={"until": 150, "hard_max": 200}, created_by="test", now=100)
    root = tmp_path / "local"
    spec = {"root": str(root), "maximum_gib": 100, "floor_fraction": .05,
            "docker_allowance_gib": 0}
    ctx = {"queue_root": str(queue.root), "action_key": "e" * 64, "nonce": "f" * 32,
           "scope_id": "copy-test", "host": "test-host", "worker": "test"}
    held = []

    def pinned(paths):
        if staged in paths:
            pins, errors = reader_lease.live_for(queue, {str(staged)})
            assert not errors and pins[str(staged)]
            held.append(True)

    guard(monkeypatch, staged, on_check=pinned)
    with pytest.raises(floor.FloorRefused):
        local_resident.copy(store, record["set_id"], "test-host", spec,
                            reader_context=ctx, now=120)
    assert held
    pins, errors = reader_lease.live_for(queue, {str(staged)})
    assert not errors and not pins
    assert store.read_copy(record["set_id"], "test-host")["state"] != "resident"



def test_storage_control_refusal_defers_before_cycle_or_log(role, monkeypatch, tmp_path):
    loop, gate, parked, current, calls, argv, stop = role
    log = tmp_path / "role.log"
    guard(monkeypatch, log)

    assert loop.main(argv + ["--once", "--log", str(log)]) == 75
    assert calls == []
    assert not log.exists()


@pytest.mark.parametrize("state", ["new", "partial", "final"])
def test_resident_copy_checks_nested_destinations_before_publication(tmp_path, monkeypatch, state):
    store, record, spec = nested_world(tmp_path)
    final = Path(spec["root"]) / record["set_id"]
    partial = final.with_name(final.name + ".partial")
    guard(monkeypatch)
    if state == "final":
        local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
    elif state == "partial":
        (partial / "nested").mkdir(parents=True)
        (partial / "nested" / "weights").write_bytes(b"weights")
    refused = (final if state == "final" else partial) / "nested"
    checked = guard(monkeypatch, refused)
    previous = store.read_copy(record["set_id"], "test-host")

    with pytest.raises(floor.FloorRefused):
        local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
    assert refused in checked
    result = store.read_copy(record["set_id"], "test-host")
    if state == "final":
        assert result == previous
    else:
        assert result["state"] != "resident"


def nested_world(tmp_path):
    store, original, spec = world(tmp_path)
    root = Path(original["canonical_root"])
    nested = root / "nested" / "weights"
    nested.parent.mkdir()
    (root / "weights").rename(nested)
    manifest = {**original["manifest"], "entries": [
        {**original["manifest"]["entries"][0], "path": str(nested)}]}
    pool.PoolQueue(store.queue_root).tier("local:test-host").mint({"local_gib": 2})
    record = store.publish(manifest=manifest, canonical_root=str(root),
        hosts=["test-host"], lease={"until": 150, "hard_max": 200},
        created_by="test", now=100)
    return store, record, spec


@pytest.mark.parametrize("reuse", [False, True])
def test_resident_adoption_checks_nested_files_before_publication(tmp_path, monkeypatch, reuse):
    store, record, spec = nested_world(tmp_path)
    source = tmp_path / "manual"
    (source / "nested").mkdir(parents=True)
    (source / "nested" / "weights").write_bytes(b"weights")
    guard(monkeypatch)
    if reuse:
        local_resident.adopt_resident_copy(store, record["set_id"], "test-host", spec, source, now=120)
    target = Path(spec["root"]) / record["set_id"] if reuse else source
    checked = guard(monkeypatch, target / "nested")

    with pytest.raises(floor.FloorRefused):
        local_resident.adopt_resident_copy(store, record["set_id"], "test-host", spec, source, now=120)
    assert target / "nested" in checked


def test_stage_adoption_checks_existing_destination_leaf(tmp_path, monkeypatch):
    source = tmp_path / "origin" / "payload"
    entry = _write(source, b"payload")
    args = _args(tmp_path, _manifest(source.parent, [entry]), start=0, end=7,
                 max_readers=1, readers=1)
    guard(monkeypatch)
    assert stage_move.move(args)["complete"]
    destination = next(p for p in Path(args.stage_root).rglob("*")
                       if p.is_file() and p.read_bytes() == b"payload")
    checked = guard(monkeypatch, destination)
    with pytest.raises(floor.FloorRefused):
        stage_move.move(args)
    assert destination in checked
    assert destination.read_bytes() == b"payload"


@pytest.mark.parametrize("route", ["final", "partial", "adopt"])
def test_resident_reuse_checks_actual_leaf(tmp_path, monkeypatch, route):
    store, record, spec = nested_world(tmp_path)
    final = Path(spec["root"]) / record["set_id"]
    guard(monkeypatch)
    if route == "final":
        local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
        target = final
    else:
        target = tmp_path / "manual" if route == "adopt" else final.with_name(final.name + ".partial")
        (target / "nested").mkdir(parents=True)
        (target / "nested" / "weights").write_bytes(b"weights")
    leaf = target / "nested" / "weights"
    previous = store.read_copy(record["set_id"], "test-host")
    checked = guard(monkeypatch, leaf)
    with pytest.raises(floor.FloorRefused):
        if route == "adopt":
            local_resident.adopt_resident_copy(store, record["set_id"], "test-host", spec, target, now=120)
        else:
            local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
    assert leaf in checked
    assert leaf.read_bytes() == b"weights"
    current = store.read_copy(record["set_id"], "test-host")
    if route == "partial":
        assert current["state"] == "copying"
        assert not final.exists()
    else:
        assert current == previous
