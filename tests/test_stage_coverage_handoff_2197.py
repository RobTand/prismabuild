"""Consumer-side stage coverage handoff, not a new coverage producer (#2197).

Contract for the existing CensusIndex owner: stage_coverage(root) returns one
root-scoped handoff with complete, begin_recovery(vector),
finish_recovery(token, vector), and invalidate(source, epoch=..., cursor=...,
paths=..., gap=...). A vector maps materialization/ownership/receipts to
(epoch, consumed cursor, positive coverage). These are internal normalized
positions, not a new on-disk record or a declaration of bell topic coverage.

The owner supplies a reconstructed inventory and replay boundary outside this
interface. Finishing attests that that work reached the supplied vector; the
consumer rejects missing coverage, obsolete epochs, gaps and unreplayed heads.
No real producer is supplied here. In particular a retained directory listing,
a successful reconcile, and a material generation never imply coverage.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from prismabuild import kept_reads, pool, staged_path
from record_store_trust import ObservedTrust


SOURCES = ("materialization", "ownership", "receipts")


def _vector():
    return {source: (f"{source}-epoch-1", 0, True) for source in SOURCES}


def _handoff(index, root):
    factory = getattr(index, "stage_coverage", None)
    assert callable(factory), (
        "#2197: CensusIndex has no root-scoped stage coverage/epoch handoff; "
        "stage_walk membership proofs are not materialization coverage"
    )
    return factory(root)


def _recover(handoff, vector):
    token = handoff.begin_recovery(vector)
    assert token is not None, "positive complete source coverage admits recovery"
    assert handoff.complete is False, "an unfinished recovery is not reusable"
    assert handoff.finish_recovery(token, vector) is True
    assert handoff.complete is True
    return token


def _seed(index, directory):
    """A real DirectoryCaptures entry with deterministic filesystem trust."""
    with index.stage_walk.acquire(str(directory)) as read:
        read.commit(((), {}), candidates=((), {}))


def _is_kept(index, directory):
    with index.stage_walk.acquire(str(directory)) as read:
        hit = read.hit
        read.commit(((), {}), candidates=((), {}))
    return hit


def test_membership_reuse_is_not_coverage_and_roots_and_restarts_are_separate(tmp_path):
    root = tmp_path / "stage"
    root.mkdir()
    index = kept_reads.CensusIndex(trust=ObservedTrust())
    _seed(index, root)
    assert _is_kept(index, root)
    handoff = _handoff(index, root)
    assert handoff.complete is False, "directory capture complete != stage coverage"
    assert _handoff(index, str(root / ".")) is handoff
    _recover(handoff, _vector())
    other = tmp_path / "other-stage"
    other.mkdir()
    assert _handoff(index, other).complete is False
    assert _handoff(kept_reads.CensusIndex(trust=ObservedTrust()), root).complete is False


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("missing", [True, False], ids=["absent", "uncovered"])
def test_every_dependency_needs_explicit_positive_coverage(tmp_path, source, missing):
    handoff = _handoff(kept_reads.CensusIndex(trust=ObservedTrust()), tmp_path)
    vector = _vector()
    if missing:
        vector.pop(source)
    else:
        epoch, cursor, _ = vector[source]
        vector[source] = (epoch, cursor, False)
    token = handoff.begin_recovery(vector)
    if token is not None:
        assert handoff.finish_recovery(token, vector) is False
    assert handoff.complete is False
    _recover(handoff, _vector())


@pytest.mark.parametrize("source", SOURCES)
def test_invalidations_revoke_classifications_without_a_directory_stamp_change(
        tmp_path, source):
    root = tmp_path / "stage"
    changed = root / "changed"
    sibling = root / "sibling"
    changed.mkdir(parents=True)
    sibling.mkdir()
    index = kept_reads.CensusIndex(trust=ObservedTrust())
    handoff = _handoff(index, root)
    vector = _vector()
    _recover(handoff, vector)
    _seed(index, changed)
    _seed(index, sibling)
    proof = index.trust.directory_current(changed)
    epoch, cursor, _ = vector[source]
    handoff.invalidate(source, epoch=epoch, cursor=cursor + 1,
                       paths=(changed / "object.bin",))
    assert index.trust.directory_current(changed) == proof
    assert handoff.complete is False
    assert not _is_kept(index, changed), "same directory stamp cannot retain stale marks"
    # An implementation may conservatively forget more than the known path.
    # This package does not require candidate-specific dependency narrowing.
    vector[source] = (epoch, cursor + 1, True)
    _recover(handoff, vector)
    _seed(index, changed)
    handoff.invalidate(source, epoch=epoch, cursor=cursor + 2,
                       paths=(changed / "object.bin",))
    assert not _is_kept(index, changed), "a second same-key mutation must not coalesce away"


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("gap", [False, True], ids=["unknown-paths", "coverage-gap"])
def test_unknown_scope_or_gap_revokes_subtree_and_inflight_capture(tmp_path, source, gap):
    root = tmp_path / "stage"
    child = root / "nested"
    child.mkdir(parents=True)
    index = kept_reads.CensusIndex(trust=ObservedTrust())
    handoff = _handoff(index, root)
    vector = _vector()
    _recover(handoff, vector)
    _seed(index, root)
    _seed(index, child)
    token = handoff.begin_recovery(vector)
    with index.stage_walk.acquire(str(child)) as active:
        epoch, cursor, _ = vector[source]
        handoff.invalidate(source, epoch=epoch, cursor=cursor + 1,
                           paths=None, gap=gap)
        active.commit(((), {}), candidates=((), {}))
    assert handoff.complete is False
    assert not _is_kept(index, root)
    assert not _is_kept(index, child), "an in-flight scan cannot resurrect an invalidated memo"
    vector[source] = (epoch, cursor + 1, True)
    if gap:
        assert handoff.finish_recovery(token, vector) is False, "gap revokes the old recovery token"
        _recover(handoff, vector)
    else:
        assert handoff.finish_recovery(token, vector) is True, "covered replay may finish recovery"


def test_recovery_requires_replay_through_current_heads_and_rejects_epoch_aba(tmp_path):
    index = kept_reads.CensusIndex(trust=ObservedTrust())
    handoff = _handoff(index, tmp_path)
    vector = _vector()
    token = handoff.begin_recovery(vector)
    assert token is not None
    handoff.invalidate("materialization", epoch="materialization-epoch-1", cursor=1,
                       paths=(tmp_path / "new-object",))
    assert handoff.finish_recovery(token, vector) is False, "scan raced an unreplayed change"
    assert handoff.complete is False
    replayed = dict(vector, materialization=("materialization-epoch-1", 1, True))
    assert handoff.finish_recovery(token, replayed) is True
    assert handoff.complete is True

    old_token = handoff.begin_recovery(replayed)
    handoff.invalidate("materialization", epoch="materialization-epoch-2", cursor=0,
                       paths=None, gap=False)
    new_epoch = dict(replayed, materialization=("materialization-epoch-2", 0, True))
    assert handoff.finish_recovery(old_token, new_epoch) is False
    _recover(handoff, new_epoch)
    # A late completion from before the restart must neither certify the old
    # epoch nor overwrite the newer successful recovery.
    assert handoff.finish_recovery(old_token, replayed) is False
    assert handoff.complete is True


def test_forget_stage_revokes_coverage_and_outstanding_recovery(tmp_path):
    root = tmp_path / "stage"
    root.mkdir()
    index = kept_reads.CensusIndex(trust=ObservedTrust())
    handoff = _handoff(index, root)
    vector = _vector()
    _recover(handoff, vector)
    token = handoff.begin_recovery(vector)
    index.forget_stage(root)
    assert _handoff(index, root).complete is False
    assert handoff.finish_recovery(token, vector) is False
    _recover(_handoff(index, root), vector)


def test_handoff_does_not_replace_fresh_locked_destructive_proofs(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    root = tmp_path / "stage"
    root.mkdir()
    tier = "prismabuild-stage:test-host"
    staged_path.register_stage_root(queue, tier_id=tier, stage_root=root)
    changed = root / ".changed.bin.partial"
    steady = root / ".steady.bin.partial"
    changed.write_bytes(b"old")
    steady.write_bytes(b"steady")
    index = kept_reads.CensusIndex(trust=ObservedTrust())
    _recover(_handoff(index, root), _vector())
    original = staged_path._unattributed_candidates

    def after_walk(*args, **kwargs):
        found = original(*args, **kwargs)
        changed.write_bytes(b"different length after discovery")
        return found

    monkeypatch.setattr(staged_path, "_unattributed_candidates", after_walk)
    receipt = staged_path.reconcile(queue, tier_id=tier, stage_root=str(root),
                                    wanted=set(), index=index)
    assert changed.exists(), "coverage is not current locked deletion authority"
    assert not steady.exists(), "ordinary safe reclamation must still work"
    assert receipt["entries_deleted"] == 1, receipt
    assert receipt["left_since_walk"] == 1, receipt
