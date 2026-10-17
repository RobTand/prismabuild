"""C24 extraction controls, executed only inside an admitted PB action."""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest

from prismabuild import contracts, local_scratch, pool, scratch_lifetime
from prismabuild.record_store import RecordStore
from test_local_scratch_measured_placement import (
    PROFILES, REPO, ScratchFleet, _profile_reader, _recorder_action,
    _successful_profile_execution, pb, worker_loop,
)
from test_scratch_declaration_record import runtime  # noqa: F401
from test_scratch_lifetime_pool import launch, lifetime  # noqa: F401


def test_lifetime_edits_preserve_real_profile_acceptance_but_producer_edits_refuse(
        tmp_path, monkeypatch):
    fleet = ScratchFleet(tmp_path, monkeypatch)
    action = _recorder_action(fleet)
    receipt, raw = _successful_profile_execution(fleet, monkeypatch, action)
    assert receipt["action_key"] == action["action_key"]
    body = json.loads(raw)
    assert body["completed"] is True and body["write_bytes"] == 1024
    config = _profile_reader(fleet, action, pb.canonical_sha256(body)).config_path
    source = tmp_path / "installed-source"
    for name in (*local_scratch.PRODUCER_FILES, "src/prismabuild/scratch_lifetime.py"):
        target = source / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO / name, target)

    def reader():
        return local_scratch.ProfileInputs(
            config, source_root=source, checkout_root=tmp_path / "verify",
            producer_python=sys.executable)

    before = reader()
    assert worker_loop.scratch_observed_detail(None, before)[PROFILES]
    owner = source / "src/prismabuild/scratch_lifetime.py"
    owner.write_bytes(owner.read_bytes() + b"\nraise AssertionError('unattested lifetime loaded')\n")
    after = reader()
    assert after.expected_sources == before.expected_sources
    assert worker_loop.scratch_observed_detail(None, after)[PROFILES]
    producer = source / "src/prismabuild/local_scratch.py"
    producer.write_bytes(producer.read_bytes() + b"\n# changed producer bytes\n")
    changed = reader()
    assert changed.expected_sources != before.expected_sources
    assert worker_loop.scratch_observed_detail(None, changed)[PROFILES] == []


def test_registration_keeps_intent_parent_leaf_and_mirror_order(lifetime, monkeypatch):
    queue, item, variables, calls = lifetime
    queue._start_resource_scope(item)
    events = []

    def phase(record):
        evidence = record[scratch_lifetime.SCRATCH_LIFETIME_FIELD]
        entry = evidence["entries"][0]
        return ("ready" if evidence["registration_complete"] else "leaf" if entry["identity"]
                else "parent" if entry["parent_identity"] else "intent")

    for name, label in (("write_scratch_owner", "owner"), ("write_scratch_lease", "lease")):
        original = getattr(RecordStore, name)
        def write(self, path, record, original=original, label=label):
            events.append((label, phase(record)))
            return original(self, path, record)
        monkeypatch.setattr(RecordStore, name, write)
    for name, label in (("_prepare_scratch_directory", "prepare"),
                        ("_create_scratch_directory", "create"),
                        ("_check_registered_scratch_directory", "check")):
        original = getattr(scratch_lifetime, name)
        def step(*args, original=original, label=label):
            events.append(label)
            return original(*args)
        monkeypatch.setattr(scratch_lifetime, name, step)
    lease_before = pool._read_json(queue.lease_path(item["action_key"]))
    scope = item["resource_scope"]
    scratch_lifetime.register(RecordStore(queue.root), queue, item["action_key"],
        claim_snapshot=item, env={pb.ACTION_KEY_ENV: item["action_key"],
                                 pb.ACTION_NONCE_ENV: scope["nonce"],
                                 pb.ACTION_SCOPE_ENV: scope["scope_id"]})
    assert events == [("owner", "intent"), ("lease", "intent"), "prepare",
                      ("owner", "parent"), ("lease", "parent"), "create",
                      ("owner", "leaf"), ("lease", "leaf"), "check",
                      ("owner", "ready"), ("lease", "ready")]
    lease_after = pool._read_json(queue.lease_path(item["action_key"]))
    assert {k: v for k, v in lease_after.items() if k != scratch_lifetime.SCRATCH_LIFETIME_FIELD} == lease_before


def test_persistent_only_owner_does_not_read_containment_or_directories(runtime, monkeypatch):
    create, calls = runtime
    claim = pool.PoolQueue.claim
    monkeypatch.setattr(pool.PoolQueue, "claim", lambda self, **kw: claim(
        self, **{"tags": [contracts.SCRATCH_LIFETIME_TAG], **kw}))
    selection = scratch_lifetime.build_scratch_lifetime_selection([
        {"root_env": "CACHE_ROOT", "name": "compile", "lifetime": "persistent"}])
    queue, item, variables = create(json.dumps(selection))
    queue._start_resource_scope(item)
    monkeypatch.setattr(queue, "_scope_from_record", lambda *a: pytest.fail("needless containment read"))
    monkeypatch.setattr(scratch_lifetime, "_prepare_scratch_directory", lambda *a: pytest.fail("persistent directory read"))
    scratch_lifetime.register(RecordStore(queue.root), queue, item["action_key"], claim_snapshot=item, env={})
    assert scratch_lifetime.finalize(RecordStore(queue.root), queue, item["action_key"], item,
                                     export=None) == {"complete": True, "used": True}
    assert not Path(variables["CACHE_ROOT"]).exists()


def test_durably_cleaned_owner_does_not_requery_scope(lifetime, monkeypatch):
    queue, item, variables, outcome = launch(lifetime, monkeypatch)
    assert scratch_lifetime.finalize(RecordStore(queue.root), queue, item["action_key"], item, export=None)["complete"]
    monkeypatch.setattr(queue, "_scope_from_record", lambda *a: pytest.fail("needless containment read"))
    monkeypatch.setattr(scratch_lifetime, "_clean_scratch_directory", lambda *a: pytest.fail("consumed directory revisited"))
    assert scratch_lifetime.finalize(RecordStore(queue.root), queue, item["action_key"], item, export=None)["complete"]
