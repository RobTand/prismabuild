"""The scratch and lifecycle owners share facts without importing each other."""
import ast
from pathlib import Path

import pytest

from prismabuild import claim_identity, core, pool, record_store, scratch_lifetime


def test_claim_identity_ignores_only_mutable_conclusion_fields():
    original = dict(claimed_by="worker", claimed_unix=2.0, published_unix=1.0, attempts=0)
    changed = {**original, "finish_pending": {"status": "executed"}, "max_attempts": 9,
               "container_cleanup_pending": True, "resource_scope_cleanup": {"complete": True}}
    assert claim_identity.same_claim(original, changed)
    for field in claim_identity.CLAIM_IDENTITY_FIELDS:
        assert not claim_identity.same_claim(original, {**changed, field: "different"})


def test_lease_consistency_raises_the_original_public_error_object():
    assert pool.AmbiguousClaimHolder is record_store.AmbiguousClaimHolder
    assert pool.AmbiguousClaimHolder.__bases__ == (pool.PoolContractError,)
    claim = dict(claimed_by="worker", claimed_host="host", claimed_unix=2.0, published_unix=1.0)
    lease = dict(owner="worker", host="host", claimed_unix=2.0, published_unix=1.0)
    claim_identity.check_lease_identity("a" * 64, claim, lease, invalid=pool.AmbiguousClaimHolder)
    claim_identity.check_lease_identity("a" * 64, claim, None, invalid=pool.AmbiguousClaimHolder)
    claim_identity.check_lease_identity("a" * 64, claim, {}, invalid=pool.AmbiguousClaimHolder)
    for field in lease:
        with pytest.raises(pool.AmbiguousClaimHolder, match="claim and reservations retained"):
            claim_identity.check_lease_identity(
                "a" * 64, claim, {**lease, field: "different"}, invalid=pool.AmbiguousClaimHolder)


def test_late_finish_name_keeps_the_exact_historical_identity_recipe(tmp_path):
    record = dict(action_key="a" * 64, claimed_by="worker", claimed_unix=2.0,
                  published_unix=1.0, attempts=0, unrelated="ignored")
    identity = core.canonical_sha256({field: record[field] for field in (
        "claimed_by", "claimed_unix", "published_unix", "attempts")})
    store = record_store.RecordStore(tmp_path)
    assert store.late_finish_path(record) == tmp_path / "claimed" / (
        "a" * 64 + "." + identity + ".late-finish")


def test_scratch_facts_and_store_dependencies_are_acyclic():
    for module in (claim_identity, record_store, scratch_lifetime):
        tree = ast.parse(Path(module.__file__).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert node.module not in {"pool", "lifecycle"}
                assert not any(alias.name in {"pool", "lifecycle"} for alias in node.names)


def test_named_action_boundaries_replace_the_retired_queue_apis():
    assert callable(pool.PoolQueue.action_transition_lock)
    assert callable(pool.PoolQueue.scope_for_record)
    for name in ("_transition_locked", "_scope_from_record"):
        assert not hasattr(pool.PoolQueue, name)
    for name in ("_same_claim", "_check_claim_lease_identity", "_CLAIM_IDENTITY"):
        assert not hasattr(pool, name)
