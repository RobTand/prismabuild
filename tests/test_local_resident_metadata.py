"""Operator descriptors cannot resurrect or overwrite the mover's copy state."""
import json
from pathlib import Path

import pytest

from prismabuild import local_resident, pool, resident_sets
from test_local_resident_mover import world

from local_resident_space import roomy_disk  # noqa: F401

pytestmark = pytest.mark.usefixtures("roomy_disk")


def _publication(tmp_path, monkeypatch, store, set_id, spec, operation):
    import pbresident
    import test_a_stage_mover_declares_the_cpu_and_retries_it_owns as fixture

    class Cas:
        root = tmp_path / "cas"

        def publish_action_request(self, action):
            pass

    template = fixture._template("c" * 64, 7)
    template["cas"] = Cas()
    tier = {"tier_id": "local:test-host", "host": "test-host", "mountpoint": spec["root"],
            "mover_python": "/usr/bin/python3", "mover_tools_root": "/generation/tools"}
    if operation == "copy":
        return local_resident.publish_actions(template, store, set_id, [tier], policy_path="/policy.json")["test-host"]["copy"]
    pool.PoolQueue(store.queue_root).announce_tier(tier)
    monkeypatch.setattr(pbresident, "movement_template", lambda *args: template)
    return pbresident.submit_adoption(store, set_id, host="test-host", source=str(tmp_path / "manual"),
                                     policy_path="/policy.json", checkout=str(tmp_path))


def _referenced_action_keys(store, set_id):
    """Inspect stored references without depending on the old or new container."""
    keys = set()

    def visit(value):
        if isinstance(value, dict):
            if "action_key" in value:
                keys.add(value["action_key"])
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    for path in store.set_path(set_id).parent.rglob("test-host.json"):
        visit(json.loads(path.read_text()))
    return keys


@pytest.mark.parametrize("operation", ["copy", "adopt"])
def test_descriptor_write_cannot_resurrect_an_evicted_copy(tmp_path, monkeypatch, operation):
    store, record, spec = world(tmp_path)
    set_id = record["set_id"]
    local_resident.copy(store, set_id, "test-host", spec, now=120)
    original_write = resident_sets.write_record
    interleaved = []

    def evict_before_descriptor_write(path, value):
        path = Path(path)
        is_descriptor = (path.parent.name == "movements"
                         or "movement_rows" in value or "adoption_row" in value)
        if is_descriptor and not interleaved:
            interleaved.append(True)
            assert local_resident.evict_resident_copy(store, set_id, "test-host", spec, now=201)["state"] == "absent"
        return original_write(path, value)

    monkeypatch.setattr(resident_sets, "write_record", evict_before_descriptor_write)
    row = _publication(tmp_path, monkeypatch, store, set_id, spec, operation)
    assert interleaved, "the eviction must finish before the pending descriptor write"
    assert not Path(spec["root"]).joinpath(set_id).exists()
    assert pool.PoolQueue(store.queue_root).tier_ledger("local:test-host").holder_tokens(set_id) == {}
    assert store.read_copy(set_id, "test-host")["state"] == "absent", "descriptor publication must not resurrect copy state"
    assert row["action_key"] in _referenced_action_keys(store, set_id)
    assert (store.set_path(set_id).parent / "movements" / "test-host.json").exists()


@pytest.mark.parametrize("operation", ["copy", "adopt"])
def test_eviction_state_write_cannot_drop_new_descriptors(tmp_path, monkeypatch, operation):
    store, record, spec = world(tmp_path)
    set_id = record["set_id"]
    local_resident.copy(store, set_id, "test-host", spec, now=120)
    original_write = resident_sets.write_record
    publication = []

    def publish_before_absent_write(path, value):
        if Path(path) == store.copy_path(set_id, "test-host") and value.get("state") == "absent" and not publication:
            publication.append(None)
            publication[0] = _publication(tmp_path, monkeypatch, store, set_id, spec, operation)
        return original_write(path, value)

    monkeypatch.setattr(resident_sets, "write_record", publish_before_absent_write)
    assert local_resident.evict_resident_copy(store, set_id, "test-host", spec, now=201)["state"] == "absent"
    assert publication
    assert publication[0]["action_key"] in _referenced_action_keys(store, set_id), "final state write must not drop concurrently published descriptors"
    assert store.read_copy(set_id, "test-host")["state"] == "absent"
    assert (store.set_path(set_id).parent / "movements" / "test-host.json").exists()


def test_copy_and_adoption_descriptors_merge_without_changing_state(tmp_path, monkeypatch):
    store, record, spec = world(tmp_path)
    set_id = record["set_id"]
    local_resident.copy(store, set_id, "test-host", spec, now=120)
    before = store.copy_path(set_id, "test-host").read_bytes()
    adopt = _publication(tmp_path, monkeypatch, store, set_id, spec, "adopt")
    copy = _publication(tmp_path, monkeypatch, store, set_id, spec, "copy")
    assert store.copy_path(set_id, "test-host").read_bytes() == before, "operator publication must write no copy-state bytes"
    refs = _referenced_action_keys(store, set_id)
    assert {adopt["action_key"], copy["action_key"]} <= refs
    metadata = json.loads((store.set_path(set_id).parent / "movements" / "test-host.json").read_text())
    assert set(metadata["rows"]) == {"copy", "evict", "adopt"}
