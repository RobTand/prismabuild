"""A failed produced mover cannot reuse the funding its claim consumed.

Private CPU queues, real sealed publisher, ledger, claim and copy process.
The fixture uses synthetic producer scope authority and explicit unpaced copy,
as disclosed by test_produced_output_restage; no fleet state is touched.
"""
from pathlib import Path

import pytest

from test_produced_output_restage import (
    _World, _descriptors, _claim_mover, _prewrite, TIER, KIND,
    _isolated_synthetic_launch_context,
)
from prismabuild import pool


def _funded_claim(tmp_path):
    w = _World(tmp_path, window_gib=2)
    descs = _descriptors(w.template, w.inst, "p-good", b"good", digest_mode="sha")
    descs += _descriptors(w.template, w.inst, "p-bad", b"bad", digest_mode="sha")
    _prewrite(w.q, w.inst, w.template, "b1", descs)
    out = w.first_publish("b1", descs)
    mover = str(out["mover_key"])
    claimed = _claim_mover(w.q, "copy-worker")
    assert claimed["action_key"] == mover
    funding = w.q.read_output_funding(mover, TIER)
    assert funding["state"] == "consumed"
    return w, mover, claimed, funding, descs


def _failed_copy(tmp_path, *, partial=False):
    w, mover, claimed, funding, descs = _funded_claim(tmp_path)
    # Real origin reachability refusal, with or without an already copied prefix.
    Path(descs[1]["path"]).unlink()
    if not partial:
        Path(descs[0]["path"]).unlink()
    result = w.q.execute(claimed, timeout_s=120)
    # stage_move returns zero for an incomplete nonempty prefix. Model a
    # subsequent failed wrapper/lease without calling that partial copy a
    # successful batch; the zero-copy regression really exits nonzero.
    if not partial:
        assert result["returncode"] != 0
    receipt = w.q.move_record(mover)
    assert receipt["complete"] is False
    assert receipt["entries_staged"] == int(partial)
    return w, mover, claimed, funding, result


@pytest.mark.parametrize("producer_terminal", [False, True])
def test_failed_copy_with_spent_credit_is_terminal(tmp_path, producer_terminal):
    w, mover, claimed, funding, result = _failed_copy(tmp_path)
    if producer_terminal:
        # Exhaust the producer's unchanged ordinary retry contract.
        for attempt in range(3):
            owner_end = w.q.finish(w.owner, status="failed")
            if attempt < 2:
                assert w.q.claim(owner="producer-retry")["action_key"] == w.owner
        assert owner_end.parent.name == pool.FAILED
    ended = w.q.finish(mover, status="failed", detail=result, claim_snapshot=claimed)
    # On the regression, it is READY but admission can never claim it again.
    if ended.parent.name == pool.READY:
        assert w.q.output_funded_cover(TIER, pool._read_json(ended), KIND, 1) == (0, None)
        assert w.q.claim(owner="retry-worker") is None
    assert ended.parent.name == pool.FAILED
    assert not w.q.item_path(pool.READY, mover).exists()
    record = pool._read_json(ended)
    assert record["attempts"] == 1
    assert record["max_attempts"] == claimed["max_attempts"] == 3
    attempt = w.q.attempt_outcomes(record)[0]
    assert attempt["disposition"] == pool.FAILED
    assert attempt["status"] == "failed"
    assert attempt["detail"]["returncode"] == result["returncode"]
    assert w.q.read_output_funding(mover, TIER) == funding


@pytest.mark.parametrize("reap", [False, True])
def test_partial_copy_is_terminal_but_keeps_occupancy(tmp_path, reap):
    w, mover, claimed, funding, result = _failed_copy(tmp_path, partial=True)
    held = w.ledger.holder_tokens(mover)
    assert held[KIND] == 1
    material = {p: p.read_bytes() for p in w.stage_root.rglob("*") if p.is_file()}
    assert material
    if reap:
        # Only the mover lease expires; the producer stays live.
        lease = pool._read_json(w.q.lease_path(mover))
        lease["heartbeat_unix"] = 0.0
        pool._write_json_atomic(w.q.lease_path(mover), lease)
        w.q.reap_stale(timeout_s=60)
        ended = w.q.item_path(pool.FAILED, mover)
    else:
        ended = w.q.finish(mover, status="failed", detail=result, claim_snapshot=claimed)
    assert ended.parent.name == pool.FAILED and ended.exists()
    assert not w.q.item_path(pool.READY, mover).exists()
    assert w.ledger.holder_tokens(mover) == held
    assert {p: p.read_bytes() for p in material} == material
    assert w.q.read_output_funding(mover, TIER) == funding
    archived = w.q.attempt_outcomes(pool._read_json(ended))[0]
    assert archived["output_retry_stop"]["reason"] == "output_funding_consumed"


@pytest.mark.parametrize("fault", ["absent", "corrupt", "generation", "publication", "binding", "tokens", "transferring"])
def test_unproved_or_unspent_funding_never_claims_a_terminal_override(tmp_path, fault):
    w, mover, claimed, funding, _ = _funded_claim(tmp_path)
    path = w.q.funding_output_path(mover, TIER)
    changed = dict(funding)
    if fault == "absent":
        path.unlink()
    elif fault == "corrupt":
        path.write_text("not-json")
    else:
        field, value = {
            "generation": ("generation", "f" * 32),
            "publication": ("published_unix", funding["published_unix"] + 1),
            "binding": ("manifest_digest", "f" * 64),
            "tokens": ("tokens", [KIND + "-unknown"]),
            "transferring": ("state", "transferring"),
        }[fault]
        changed[field] = value
        pool._write_json_atomic(path, changed)
    before = path.read_bytes() if path.exists() else None
    ended = w.q.finish(mover, status="failed", claim_snapshot=claimed)
    assert ended.parent.name == pool.READY
    record = pool._read_json(ended)
    archived = w.q.attempt_outcomes(record)[0]
    assert "output_retry_stop" not in archived
    assert record["attempts"] == 1 and record["max_attempts"] == 3
    assert (path.read_bytes() if path.exists() else None) == before


def test_ordinary_failure_still_retries_and_can_succeed(tmp_path):
    q = pool.PoolQueue(tmp_path / "queue")
    q.ensure_layout()
    key = "a" * 64
    q.publish(action_key=key, cas_root=tmp_path / "cas", checkout_root=tmp_path,
              worker_script="worker.py", resources={"cpu": 1},
              max_attempts=3, retry_safe=True)
    first = q.claim(owner="first")
    assert first is not None
    assert q.finish(key, status="failed", claim_snapshot=first).parent.name == pool.READY
    second = q.claim(owner="second")
    assert second is not None and second["attempts"] == 1
    ended = q.finish(key, status="executed", claim_snapshot=second)
    assert ended.parent.name == pool.DONE
    assert [a["disposition"] for a in q.attempt_outcomes(pool._read_json(ended))] == ["requeued", pool.DONE]


@pytest.mark.parametrize("state", [pool.READY, pool.CLAIMED])
def test_late_failure_never_concludes_a_live_successor(tmp_path, state):
    w, mover, claimed, funding, _ = _funded_claim(tmp_path)
    successor = {**claimed, "published_unix": claimed["published_unix"] + 1,
                 "claimed_by": "new-worker", "claimed_unix": claimed["claimed_unix"] + 1}
    w.q.item_path(pool.CLAIMED, mover).unlink()
    path = w.q.item_path(state, mover)
    pool._write_json_atomic(path, successor)
    before = path.read_bytes()
    lease = w.q.lease_path(mover).read_bytes()
    held = w.ledger.holder_tokens(mover)
    w.q.finish(mover, status="failed", claim_snapshot=claimed)
    assert path.read_bytes() == before
    assert w.q.lease_path(mover).read_bytes() == lease
    assert w.ledger.holder_tokens(mover) == held
    assert not w.q.item_path(pool.FAILED, mover).exists()
    assert w.q.read_output_funding(mover, TIER) == funding


def test_archived_stop_remains_valid_after_funding_changes(tmp_path):
    w, mover, claimed, funding, _ = _funded_claim(tmp_path)
    ended = w.q.finish(mover, status="failed", claim_snapshot=claimed)
    record = pool._read_json(ended)
    assert w.q.adopted_attempt_summary(record)["disposition"] == pool.FAILED
    w.q.funding_output_path(mover, TIER).unlink()
    assert w.q.adopted_attempt_summary(record)["disposition"] == pool.FAILED
    assert record["max_attempts"] == 3


@pytest.mark.parametrize("field", ["generation", "published_unix", "mover_action_key"])
def test_corrupt_archived_stop_cannot_authorize_early_failure(tmp_path, field):
    w, mover, claimed, funding, _ = _funded_claim(tmp_path)
    ended = w.q.finish(mover, status="failed", claim_snapshot=claimed)
    record = pool._read_json(ended)
    path = w.q.attempt_path(record, 1)
    archived = pool._read_json(path)
    replacement = {"generation": "f" * 32, "published_unix": funding["published_unix"] + 1,
                   "mover_action_key": "f" * 64}[field]
    archived["output_retry_stop"]["funding"][field] = replacement
    # Deliberate immutable-evidence corruption, confined to this private queue.
    path.chmod(0o644)
    pool._write_json_atomic(path, archived)
    path.chmod(0o444)
    with pytest.raises(pool.PoolContractError, match="binding differs"):
        w.q.adopted_attempt_summary(record)


def test_consumed_claim_with_missing_lease_is_not_an_unstarted_retry(tmp_path, monkeypatch):
    w, mover, claimed, funding, _ = _funded_claim(tmp_path)
    w.q.lease_path(mover).unlink()
    now = pool._now()
    monkeypatch.setattr(pool, "_now", lambda: now + pool.HEARTBEAT_S + 1)
    held = w.ledger.holder_tokens(mover)
    w.q.reap_stale(timeout_s=3600)
    ended = w.q.item_path(pool.FAILED, mover)
    assert ended.exists()
    assert not w.q.item_path(pool.READY, mover).exists()
    record = pool._read_json(ended)
    assert record["attempts"] == 1 and record["max_attempts"] == 3
    assert "unstarted_releases" not in record
    assert w.q.adopted_attempt_summary(record)["disposition"] == pool.FAILED
    # No copy evidence is unknown occupancy, never proof that storage is free.
    assert w.ledger.holder_tokens(mover) == held
    assert w.q.read_output_funding(mover, TIER) == funding
