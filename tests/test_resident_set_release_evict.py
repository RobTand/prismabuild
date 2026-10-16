import json
from pathlib import Path

import pytest

from prismabuild import core, local_resident, local_tier, pool, resident_sets
from local_resident_space import roomy_disk
from test_resident_sets_records import source


pytestmark = pytest.mark.usefixtures("roomy_disk")


def publish(store, directory, lease):
    directory.mkdir()
    root, manifest = source(directory)
    return store.publish(manifest=manifest, canonical_root=str(root), hosts=["test-host"],
                         lease=lease, created_by="test", now=100)["set_id"]


def world(tmp_path, *, capacity=20):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.mint_tier_capacity("local:test-host", {"local_gib": capacity})
    return resident_sets.ResidentSets(queue.root)


def snapshot(root, *, except_paths=()):
    return {path.relative_to(root): path.read_bytes() for path in root.rglob("*")
            if path.is_file() and path not in except_paths}


def journal(store, set_id):
    return store.set_path(set_id).with_name("lease.jsonl")


def persisted_set(store, directory, lease):
    directory.mkdir()
    canonical, manifest = source(directory)
    manifest = resident_sets.validate_set_manifest(manifest, str(canonical))
    set_id = core.raw_sha256(core.compact_ascii_json_bytes(manifest))
    local_tier.reserve(pool.PoolQueue(store.queue_root), set_id, ["test-host"], manifest["total_bytes"])
    with store.lock(set_id):
        store.set_path(set_id).write_bytes(core.compact_ascii_json_bytes({
            "schema": resident_sets.SET_SCHEMA, "set_id": set_id, "immutable": True,
            "manifest": manifest, "canonical_root": str(canonical), "hosts": ["test-host"],
            "lease": lease, "created_by": "test", "created_unix": 100}) + b"\n")
        store._append_lease(set_id, {"event": "published", "lease": lease, "unix": 100, "by": "test"})
        store.write_copy(set_id, "test-host", {"state": "absent", "bytes": 0,
            "verification": [], "completed_unix": None, "local_root": None})
    return set_id


def test_effective_lease_folds_real_journal_in_append_order(tmp_path):
    store = resident_sets.ResidentSets(tmp_path / "queue")
    set_id = "a" * 64
    a = {"campaign": "A", "hard_max": 200}
    b = {"campaign": "B", "hard_max": 300}
    events = [
        ({"event": "published", "lease": a, "unix": 100}, (a, False)),
        ({"event": "released", "unix": 120}, (a, True)),
        ({"event": "renewed", "lease": b, "unix": 110, "renewal_ceiling_s": 200}, (b, False)),
        ({"event": "renewed", "lease": a, "unix": 115, "renewal_ceiling_s": 200}, (a, False)),
        ({"event": "released", "unix": 114}, (a, True)),
        ({"event": "published", "lease": b, "unix": 101}, (b, False)),
    ]
    with store.lock(set_id):
        for event, expected in events:
            store._append_lease(set_id, {**event, "by": "test"})
            assert store.effective_lease(set_id) == expected


@pytest.mark.parametrize("change", [
    {"event": "unknown"}, {"event": None}, {"unix": None}, {"unix": True},
    {"unix": float("nan")}, {"by": ""}, {"by": None}, {"lease": None},
    {"lease": {"campaign": "A", "hard_max": 100}}, {"manifest_sha256": "b" * 64},
    {"event": "renewed", "renewal_ceiling_s": 200},
])
def test_effective_lease_rejects_corrupt_event_data(tmp_path, change):
    store = resident_sets.ResidentSets(tmp_path / "queue")
    set_id = "a" * 64
    row = {"event": "published", "manifest_sha256": set_id, "by": "test", "unix": 100,
           "lease": {"campaign": "A", "hard_max": 200}, **change}
    path = journal(store, set_id)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(row) + "\n")
    with pytest.raises(ValueError):
        store.effective_lease(set_id)


@pytest.mark.parametrize("make_set", [publish, persisted_set], ids=["publication", "persisted"])
def test_close_matches_effective_campaign_and_only_appends_matching_journals(tmp_path, make_set):
    store = world(tmp_path)
    leases = {
        "a": {"campaign": "A", "hard_max": 200},
        "a_to_b": {"campaign": "A", "hard_max": 200},
        "b_to_a": {"campaign": "B", "hard_max": 200},
        "until": {"until": 150, "hard_max": 200},
        "a_to_until": {"campaign": "A", "hard_max": 200},
    }
    ids = {name: make_set(store, tmp_path / name, lease) for name, lease in leases.items()}
    store.renew(ids["a_to_b"], {"campaign": "B", "hard_max": 300}, by="test", now=110)
    store.renew(ids["b_to_a"], {"campaign": "A", "hard_max": 300}, by="test", now=110)
    store.renew(ids["a_to_until"], {"until": 150, "hard_max": 200}, by="test", now=110)
    for state in (pool.record_store.READY, pool.record_store.CLAIMED):
        for set_id in ids.values():
            resident_sets.write_record(store.queue_root / state / (set_id + ".json"),
                                       {"resident_set": set_id})
    matching = sorted([ids["a"], ids["b_to_a"]])
    journals = {set_id: journal(store, set_id).read_bytes() for set_id in matching}
    before = snapshot(tmp_path, except_paths=[journal(store, set_id) for set_id in matching])
    assert store.close_campaign("A", by="controller", now=201) == {"released": matching, "errors": {}}
    assert snapshot(tmp_path, except_paths=[journal(store, set_id) for set_id in matching]) == before
    for set_id in matching:
        assert journal(store, set_id).read_bytes().startswith(journals[set_id])
        assert store.read_lease_log(set_id)[-1] == {
            "event": "released", "unix": 201, "by": "controller", "manifest_sha256": set_id}
        assert not local_resident.lease_active(store, set_id, now=201)
    assert local_resident.lease_active(store, ids["a_to_b"], now=201)
    assert local_resident.lease_active(store, ids["until"], now=151)
    assert not local_resident.lease_active(store, ids["until"], now=201)
    after = snapshot(tmp_path)
    assert store.close_campaign("A", by="controller", now=202) == {"released": [], "errors": {}}
    assert snapshot(tmp_path) == after
    store.renew(ids["a"], {"campaign": "A", "hard_max": 400}, by="test", now=203)
    assert local_resident.lease_active(store, ids["a"], now=204)
    assert store.close_campaign("A", by="controller", now=205) == {"released": [ids["a"]], "errors": {}}
    assert not local_resident.lease_active(store, ids["a"], now=206)


@pytest.mark.parametrize("name", ["", "../A", "A/B", "A B", None])
def test_invalid_campaign_is_refused_before_mutation(tmp_path, name):
    store = resident_sets.ResidentSets(tmp_path / "queue")
    with pytest.raises(ValueError, match="safe nonempty name"):
        store.close_campaign(name, by="test", now=110)
    assert not store.queue_root.exists()


def test_missing_store_is_an_empty_close(tmp_path):
    store = resident_sets.ResidentSets(tmp_path / "queue")
    assert store.close_campaign("A", by="test", now=110) == {"released": [], "errors": {}}
    assert not store.root.exists()


def test_refused_publication_is_skipped_but_missing_published_journal_is_reported(tmp_path):
    store = world(tmp_path, capacity=1)
    good = persisted_set(store, tmp_path / "good", {"campaign": "A", "hard_max": 200})
    refused = tmp_path / "refused"
    refused.mkdir()
    root, manifest = source(refused)
    with pytest.raises(ValueError, match="capacity unavailable"):
        store.publish(manifest=manifest, canonical_root=str(root), hosts=["test-host"],
                      lease={"campaign": "A", "hard_max": 200}, created_by="test", now=100)
    unpublished = core.raw_sha256(core.compact_ascii_json_bytes(manifest))
    assert store.set_path(unpublished).with_name(".record.lock").is_file()
    assert not store.set_path(unpublished).exists()
    assert store.close_campaign("A", by="test", now=110) == {"released": [good], "errors": {}}
    journal(store, good).unlink()
    result = store.close_campaign("A", by="test", now=111)
    assert result["released"] == []
    assert set(result["errors"]) == {good}
    assert "FileNotFoundError" in result["errors"][good]


@pytest.mark.parametrize("defect", [
    "json", "empty", "binding", "event", "lease", "unix", "by", "shape",
    "maximum", "until", "campaign", "renewal_without_publication", "renewal_ceiling",
])
def test_corrupt_published_journal_is_reported_and_other_sets_continue(tmp_path, defect):
    store = world(tmp_path)
    bad = persisted_set(store, tmp_path / "bad", {"campaign": "A", "hard_max": 200})
    good = persisted_set(store, tmp_path / "good", {"campaign": "A", "hard_max": 200})
    rows = store.read_lease_log(bad)
    if defect == "json":
        journal(store, bad).write_text("{broken\n")
    elif defect == "empty":
        journal(store, bad).write_text("")
    else:
        if defect == "binding":
            rows[0]["manifest_sha256"] = good
        elif defect == "event":
            rows[0]["event"] = "unknown"
        elif defect in ("lease", "unix", "by"):
            del rows[0][defect]
        elif defect == "shape":
            rows[0] = []
        elif defect == "maximum":
            rows[0]["lease"]["hard_max"] = float("inf")
        elif defect == "until":
            rows[0]["lease"] = {"until": 201, "hard_max": 200}
        elif defect == "campaign":
            rows[0]["lease"]["campaign"] = "../A"
        elif defect == "renewal_without_publication":
            rows[0].update(event="renewed", renewal_ceiling_s=100)
        else:
            rows.append({**rows[0], "event": "renewed", "renewal_ceiling_s": 1})
        journal(store, bad).write_text("".join(json.dumps(row) + "\n" for row in rows))
    before = journal(store, bad).read_bytes()
    result = store.close_campaign("A", by="test", now=201)
    assert result["released"] == [good]
    assert set(result["errors"]) == {bad}
    assert journal(store, bad).read_bytes() == before
    with pytest.raises(ValueError):
        local_resident.lease_active(store, bad, now=201)


@pytest.mark.parametrize("defect", ["body_json", "body_digest", "body_read", "journal_read"])
def test_published_body_and_read_failures_are_not_hidden(tmp_path, monkeypatch, defect):
    store = world(tmp_path)
    bad = persisted_set(store, tmp_path / "bad", {"campaign": "A", "hard_max": 200})
    good = persisted_set(store, tmp_path / "good", {"campaign": "A", "hard_max": 200})
    if defect == "body_json":
        store.set_path(bad).write_text("{broken")
    elif defect == "body_digest":
        body = store.read(bad)
        body["manifest"]["annotations"]["changed"] = True
        resident_sets.write_record(store.set_path(bad), body)
    else:
        target = store.set_path(bad) if defect == "body_read" else journal(store, bad)
        original = Path.read_text
        def read_text(path, *args, **kwargs):
            if path == target:
                raise PermissionError("unreadable published record")
            return original(path, *args, **kwargs)
        monkeypatch.setattr(Path, "read_text", read_text)
    result = store.close_campaign("A", by="test", now=110)
    assert result["released"] == [good]
    assert set(result["errors"]) == {bad}


def test_eviction_recovers_persisted_state_with_all_three_trees(tmp_path, monkeypatch):
    store = world(tmp_path)
    set_id = persisted_set(store, tmp_path / "source", {"campaign": "A", "hard_max": 200})
    store.write_copy(set_id, "test-host", {"state": "evicting", "bytes": 7,
        "verification": [], "completed_unix": 120, "local_root": str(tmp_path / "local" / set_id)})
    queue = pool.PoolQueue(store.queue_root)
    spec = {"root": str(tmp_path / "local")}
    trees = [Path(spec["root"]) / (set_id + suffix) for suffix in ("", ".partial", ".evicting")]
    for tree in trees:
        tree.mkdir(parents=True)
        (tree / "weights").write_bytes(b"weights")
    ledger = queue.tier_ledger("local:test-host")
    original_release = type(ledger).release
    def release(instance, owner, *args, **kwargs):
        assert not any(tree.exists() for tree in trees)
        return original_release(instance, owner, *args, **kwargs)
    monkeypatch.setattr(type(ledger), "release", release)
    result = local_resident.evict_resident_copy(store, set_id, "test-host", spec, now=201)
    assert result["state"] == "absent" and result["bytes"] == 0
    assert not any(tree.exists() for tree in trees)
    assert pool.held_names_visible(ledger, set_id) == set()
    assert Path(store.read(set_id)["canonical_root"]).joinpath("weights").read_bytes() == b"weights"


def test_close_preserves_pinned_bytes_and_eviction_deletes_every_tree_before_tokens(tmp_path, monkeypatch):
    store = world(tmp_path)
    set_id = persisted_set(store, tmp_path / "source", {"campaign": "A", "hard_max": 200})
    spec = {"root": str(tmp_path / "local"), "maximum_gib": 100,
            "floor_fraction": .05, "docker_allowance_gib": 0}
    local_resident.copy(store, set_id, "test-host", spec, now=120)
    token = local_resident.pin(store, set_id, "test-host", spec,
        {"action_key": "a" * 64, "nonce": "b" * 32, "scope_id": "test-scope"}, now=120)
    trees = [Path(spec["root"]) / (set_id + suffix) for suffix in ("", ".partial", ".evicting")]
    for tree in trees[1:]:
        tree.mkdir()
        (tree / "weights").write_bytes(b"unfinished bytes")
    before = snapshot(tmp_path, except_paths=[journal(store, set_id)])
    assert store.close_campaign("A", by="test", now=121) == {"released": [set_id], "errors": {}}
    assert snapshot(tmp_path, except_paths=[journal(store, set_id)]) == before
    assert local_resident.evict_resident_copy(store, set_id, "test-host", spec, now=122)["reason"] == "pinned"
    assert snapshot(tmp_path, except_paths=[journal(store, set_id)]) == before
    with pytest.raises(ValueError, match="lease expired"):
        local_resident.pin(store, set_id, "test-host", spec,
            {"action_key": "a" * 64, "nonce": "b" * 32, "scope_id": "test-scope"}, now=122)
    local_resident.release_pin(store, set_id, "test-host", spec, token)
    ledger = pool.PoolQueue(store.queue_root).tier_ledger("local:test-host")
    tokens = pool.held_names_visible(ledger, set_id)
    original_delete = local_resident.shutil.rmtree
    original_release = type(ledger).release
    def delete(tree):
        assert pool.held_names_visible(ledger, set_id) == tokens
        assert store.read_copy(set_id, "test-host")["state"] == "evicting"
        if tree == trees[1]:
            raise OSError("delete interrupted")
        return original_delete(tree)
    def release(instance, owner, *args, **kwargs):
        assert not any(tree.exists() for tree in trees)
        return original_release(instance, owner, *args, **kwargs)
    monkeypatch.setattr(local_resident.shutil, "rmtree", delete)
    monkeypatch.setattr(type(ledger), "release", release)
    with pytest.raises(OSError, match="delete interrupted"):
        local_resident.evict_resident_copy(store, set_id, "test-host", spec, now=123)
    assert store.read_copy(set_id, "test-host")["state"] == "evicting"
    assert store.read_copy(set_id, "test-host")["bytes"] == 7
    assert pool.held_names_visible(ledger, set_id) == tokens
    assert trees[1].exists()
    monkeypatch.setattr(local_resident.shutil, "rmtree", original_delete)
    result = local_resident.evict_resident_copy(store, set_id, "test-host", spec, now=124)
    assert result["state"] == "absent" and result["bytes"] == 0
    assert not any(tree.exists() for tree in trees)
    assert pool.held_names_visible(ledger, set_id) == set()
