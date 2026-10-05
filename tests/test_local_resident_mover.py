import hashlib
from pathlib import Path

import pytest

from prismabuild import pool, resident_sets
from test_resident_sets_records import publish


def world(tmp_path):
    store, record = publish(tmp_path)
    root = tmp_path / "local"
    root.mkdir()
    spec = {"root": str(root), "maximum_gib": 100, "floor_fraction": .05,
            "docker_allowance_gib": 0}
    return store, record, spec


def test_copy_hashes_fsyncs_then_lands_whole_tree(tmp_path, monkeypatch):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    fsyncs = []
    real = local_resident.os.fsync
    def fsync(fd):
        fsyncs.append(fd)
        return real(fd)
    monkeypatch.setattr(local_resident.os, "fsync", fsync)
    result = local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
    assert result["state"] == "resident"
    assert Path(result["local_root"]).joinpath("weights").read_bytes() == b"weights"
    assert result["verification"][0]["sha256"] == record["manifest"]["entries"][0]["sha256"]
    assert fsyncs
    assert not Path(spec["root"]).joinpath(record["set_id"] + ".partial").exists()
    assert pool.PoolQueue(store.queue_root).tier_ledger("local:test-host").held()["local_gib"] == 1


def test_corrupt_source_never_becomes_resident_and_retry_resumes(tmp_path):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    source = Path(record["canonical_root"]) / "weights"
    source.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="sha256"):
        local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
    assert store.read_copy(record["set_id"], "test-host")["state"] == "copying"
    assert not Path(spec["root"]).joinpath(record["set_id"]).exists()
    source.write_bytes(b"weights")
    assert local_resident.copy(store, record["set_id"], "test-host", spec, now=120)["state"] == "resident"


def test_verified_partial_file_is_reused_but_rehashed(tmp_path, monkeypatch):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    partial = Path(spec["root"]) / (record["set_id"] + ".partial")
    partial.mkdir()
    (partial / "weights").write_bytes(b"weights")
    def no_copy(*_args, **_kwargs):
        raise AssertionError("already verified file must not be recopied")
    monkeypatch.setattr(local_resident, "copy_file", no_copy)
    assert local_resident.copy(store, record["set_id"], "test-host", spec, now=120)["state"] == "resident"


def test_source_metadata_is_diagnostic_not_a_gate(tmp_path):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    source = Path(record["canonical_root"]) / "weights"
    source.touch()
    result = local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
    assert result["source_stamp"][str(source)]["mtime_ns"] == source.stat().st_mtime_ns


def test_copy_checks_capacity_and_never_serves_partial(tmp_path, monkeypatch):
    from prismabuild import local_resident
    from test_local_tier_capacity import sample
    store, record, spec = world(tmp_path)
    monkeypatch.setattr(local_resident.os, "statvfs", lambda _: sample(10))
    with pytest.raises(ValueError, match="capacity"):
        local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
    assert store.read_copy(record["set_id"], "test-host")["state"] != "resident"


def test_movement_actions_reuse_shared_builder_and_pin_host(tmp_path, monkeypatch):
    from prismabuild import local_resident
    store, record, spec = world(tmp_path)
    captured = {}
    def seal(template, **kwargs):
        captured.update(kwargs)
        return {"action_key": "a" * 64}
    monkeypatch.setattr(local_resident.movement_actions, "seal_movement_action", seal)
    tier = {"host": "test-host", "mountpoint": spec["root"],
            "mover_python": "/usr/bin/python3", "mover_tools_root": "/generation/tools"}
    local_resident.movement_action({}, store, record["set_id"], tier, policy_path="/policy.json", operation="copy")
    assert captured["tags"] == ["test-host"]
    assert captured["demand"] == {"cpu": 1, "mem_gb": 1}
    assert "/generation/tools/local_resident.py" in captured["command"]


def test_stage_selection_holds_real_reader_pin_until_copy_finishes(tmp_path, monkeypatch):
    from prismabuild import local_resident
    from prismabuild import reader_lease
    import test_a_promotion_handoff_defers_the_source_retirement as fixture
    queue, stage, staged, manifest_sha, manifest_path = fixture._world(tmp_path)
    import json
    manifest = json.loads(manifest_path.read_text())
    queue.mint_tier_capacity("local:test-host", {"local_gib": 1})
    store = resident_sets.ResidentSets(queue.root)
    record = store.publish(manifest=manifest, canonical_root=str(tmp_path / "origin"),
        hosts=["test-host"], lease={"until": 150, "hard_max": 200}, created_by="test", now=100)
    root = tmp_path / "local"
    root.mkdir()
    spec = {"root": str(root), "maximum_gib": 100, "floor_fraction": .05, "docker_allowance_gib": 0}
    owner = "e" * 64
    ctx = {"queue_root": str(queue.root), "action_key": owner, "nonce": "f" * 32,
           "scope_id": "copy-test", "host": "test-host", "worker": "test"}
    observed = []
    original = local_resident.copy_file
    def copying(source, *args, **kwargs):
        pins, errors = reader_lease.live_for(queue, {str(staged)})
        assert not errors and pins[str(staged)]
        assert Path(source) == staged
        observed.append(str(source))
        return original(source, *args, **kwargs)
    monkeypatch.setattr(local_resident, "copy_file", copying)
    assert local_resident.copy(store, record["set_id"], "test-host", spec, reader_context=ctx, now=120)["state"] == "resident"
    assert observed == [str(staged)]
    pins, errors = reader_lease.live_for(queue, {str(staged)})
    assert not errors and not pins
