"""R4 repro: unknown-as-absent reads and short split re-homes (must fail pre-fix).

Two exact recovery seams with REAL filesystem boundaries (real ``chmod``
permission loss, a really-empty terminal proof, a really-failed ``rename``
that ``ResourceLedger.transfer`` then suppresses) -- never a monkeypatched
high-level function raising where its production dependency swallows the
error:

1. Authoritative reads must distinguish *missing* from *unreadable,
   corrupt, or empty*.  ``_settle_terminal_fence`` reading a holder
   directory it cannot list, a present-but-empty DONE/FAILED proof, and
   ``reserve_fence`` facing a present-but-unreadable funding record must
   all RETAIN the fence (record open, tokens held, publication deferred
   with a named unknown reason) -- a present record/proof is unproved,
   never absent, and unknown authority is never destroyed to make
   recovery proceed.

2. The split stale-rehome retry in ``_reserve_fence_locked`` must verify
   exact post-transfer ownership across mover+grant before closing
   ``transferring -> released`` or taking a fresh deficit.  A really
   failed rename on retry leaves the record open, the partial split
   intact, and no fresh token beside the leftover; once the fault
   clears, the same reserve converges with no free interval and no
   duplicate credit.

Eventual progress is asserted in each test: with the fault cleared (or
the record made truly absent), the same production call makes the
correct decision on the next attempt.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, storage_tiers, window_credit  # noqa: E402
import tier_loop  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
GIB = storage_tiers.GIB
SPAN = 1 << 20


def _hexkey(seed: str) -> str:
    return (seed.encode().hex() * 64)[:64]


def _queue(tmp_path: Path, gib: int) -> pool.PoolQueue:
    root = tmp_path / "pb-queue"
    q = pool.PoolQueue(root)
    (root / "cas").mkdir(parents=True, exist_ok=True)
    (root / "co").mkdir(parents=True, exist_ok=True)
    (root / "worker.py").write_text("x")
    q.mint_tier_capacity(TIER, {"stage_gib": gib})
    return q


def _funding(q: pool.PoolQueue, mover: str, *, tokens: list[str],
             published: float = 1.0) -> dict[str, object]:
    return {"schema": pool.TIER_FUNDING_SCHEMA_V1, "tier_id": TIER,
            "consumer_action_key": _hexkey("r4-consumer"),
            "plan_sha256": "8" * 64, "mover_action_key": mover,
            "range_start_bytes": 0, "range_end_bytes": SPAN,
            "kind": "stage_gib", "tokens": tokens,
            "generation": "b" * 32, "state": "reserved",
            "unix": 1.0, "published_unix": published}


def _terminal_transferring(q: pool.PoolQueue, gib: int = 3
                           ) -> tuple[pool.PoolQueue, object, str, str]:
    """A terminal mover holding exactly its bound ``transferring`` fence."""
    ledger = q.tier_ledger(TIER)
    mover = _hexkey("r4-mover")
    assert ledger.acquire(mover, {"stage_gib": 1}) is True
    name = sorted(p.name
                  for p in (ledger.held_dir / mover).glob("*-*"))[0]
    q._write_funding_locked(
        _funding(q, mover, tokens=[name]), expect_generation=None)
    assert q.advance_funding_state(
        mover, TIER, expect="reserved", advance_to="transferring",
        generation="b" * 32) is True
    done = q.item_path(pool.DONE, mover)
    done.parent.mkdir(parents=True, exist_ok=True)
    return ledger, mover, name, done


def _names(ledger, key: str) -> list[str]:
    return sorted(p.name for p in (ledger.held_dir / key).glob("*-*"))


def test_repro_unreadable_holder_dir_retains_terminal_fence(
        tmp_path: Path) -> None:
    q = _queue(tmp_path, 3)
    ledger, mover, name, done = _terminal_transferring(q)
    # Readable terminal file with positively no tier_funding proof: only
    # the holder census is unknown here.
    done.write_text('{"status": "executed"}')
    held = ledger.held_dir / mover
    held.chmod(0o000)  # real EACCES for a non-root worker
    try:
        events = tier_loop._settle_terminal_fence(
            q, ledger, tier_id=TIER, mover_role="mover_row",
            consumer=None, mover=mover)
    finally:
        held.chmod(0o755)
    # Unknown holder census retains: token still held, record still open,
    # a named unknown reason is emitted.
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1, \
        "unreadable holder census must not release or strand the fence"
    record = q.read_funding(mover, TIER)
    assert record is not None and record["state"] == "transferring"
    assert [e for e in events
            if e.get("event") == "advance-deferred-unknown-evidence"
            and "holder census" in str(e.get("error", ""))], events
    # Same deferral through the real cycle caller (funding scan).
    held.chmod(0o000)
    try:
        window_events = tier_loop.residency_window(
            q, tiers={TIER: {"tier_id": TIER}})
    finally:
        held.chmod(0o755)
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1
    record = q.read_funding(mover, TIER)
    assert record is not None and record["state"] == "transferring"
    assert [e for e in window_events
            if e.get("event") == "advance-deferred-unknown-evidence"], \
        window_events
    # Eventual progress: fault cleared, the same settle now reads the
    # census, releases by exact name match, and closes the record.
    settled = tier_loop._settle_terminal_fence(
        q, ledger, tier_id=TIER, mover_role="mover_row",
        consumer=None, mover=mover)
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 0
    record = q.read_funding(mover, TIER)
    assert record is not None and record["state"] == "released"
    assert [e for e in settled if e.get("event") == "advance-released"]


def test_repro_empty_terminal_proof_retains_terminal_fence(
        tmp_path: Path) -> None:
    q = _queue(tmp_path, 3)
    ledger, mover, name, done = _terminal_transferring(q)
    # A present but zero-byte terminal proof: unproved, not absent.
    done.write_bytes(b"")
    events = tier_loop._settle_terminal_fence(
        q, ledger, tier_id=TIER, mover_role="mover_row",
        consumer=None, mover=mover)
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1, \
        "a present empty proof must not become unused authority"
    record = q.read_funding(mover, TIER)
    assert record is not None and record["state"] == "transferring"
    assert [e for e in events
            if e.get("event") == "advance-deferred-unknown-evidence"
            and "proof" in str(e.get("error", ""))], events
    # The real cycle caller defers identically (no free, no close).
    window_events = tier_loop.residency_window(
        q, tiers={TIER: {"tier_id": TIER}})
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1
    record = q.read_funding(mover, TIER)
    assert record is not None and record["state"] == "transferring"
    assert [e for e in window_events
            if e.get("event") == "advance-deferred-unknown-evidence"], \
        window_events
    # Eventual progress: the proof becomes readable and positively binds
    # this attempt's fence -- retained as fence-or-bytes, still not free.
    done.write_text(json.dumps({
        "status": "executed",
        "tier_funding": {TIER: {"generation": "b" * 32,
                                "tokens": [name]}}}))
    tier_loop._settle_terminal_fence(
        q, ledger, tier_id=TIER, mover_role="mover_row",
        consumer=None, mover=mover)
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1
    record = q.read_funding(mover, TIER)
    assert record is not None and record["state"] == "transferring"


def _reserve_fields(published: float) -> dict[str, object]:
    return {"consumer_action_key": _hexkey("r4-corrupt-consumer"),
            "plan_sha256": "9" * 64,
            "mover_action_key": _hexkey("r4-corrupt-mover"),
            "range_start_bytes": 0, "range_end_bytes": SPAN,
            "kind": "stage_gib", "published_unix": published}


def test_repro_reserve_defers_on_unknown_funding_record(
        tmp_path: Path) -> None:
    from test_tier_funding import _queue as _tf_queue
    q = _tf_queue(tmp_path, stage_gib=3)
    ledger = q.tier_ledger(TIER)
    mover = _hexkey("r4-corrupt-mover")
    assert ledger.acquire(mover, {"stage_gib": 1}) is True
    path = q.funding_path(mover, TIER)
    path.parent.mkdir(parents=True, exist_ok=True)
    grant = window_credit.grant_key(
        _hexkey("r4-corrupt-consumer"), TIER, "mover_row", "phase-r4")
    fields = _reserve_fields(1.0)

    # Real permission boundary: the present record cannot be read at all.
    path.write_text('{"schema": "placeholder"}')
    path.chmod(0o000)
    try:
        assert q.reserve_fence(TIER, grant, fields, 2) is False, \
            "an unreadable funding record must defer, never re-fence"
    finally:
        path.chmod(0o644)
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1
    assert int(ledger.holder_tokens(grant).get("stage_gib", 0)) == 0
    assert int(ledger.available().get("stage_gib", 0)) == 2

    # Present but empty: still unproved authority, never true absence.
    path.write_bytes(b"")
    assert q.reserve_fence(TIER, grant, fields, 2) is False, \
        "a present empty funding record must defer, never be replaced"
    assert path.read_bytes() == b""
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1
    assert int(ledger.holder_tokens(grant).get("stage_gib", 0)) == 0
    assert int(ledger.available().get("stage_gib", 0)) == 2

    # Eventual progress via true absence (operator removes the torn
    # record): a fresh fence binds normally -- no double-hold remains.
    path.unlink()
    assert q.reserve_fence(TIER, grant, fields, 2) is True
    record = q.read_funding(mover, TIER)
    assert record is not None and record["state"] == "reserved"
    assert int(ledger.holder_tokens(grant).get("stage_gib", 0)) == 2
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 1
    assert int(ledger.available().get("stage_gib", 0)) == 0


def test_repro_split_rehome_retry_short_transfer(tmp_path: Path) -> None:
    from test_tier_funding import (_fields, _hexkey as _tf_hexkey,
                                   _plan, _publish_mover,
                                   _queue as _tf_queue)
    q = _tf_queue(tmp_path, stage_gib=3)
    ledger = q.tier_ledger(TIER)
    mover = _tf_hexkey("r4-split-mover")
    consumer = _tf_hexkey("r4-split-consumer")
    plan = _plan(q, consumer, mover, tag="r4split", gib=2)
    row = _publish_mover(q, plan, mover)
    grant = window_credit.grant_key(
        consumer, TIER, "mover_row", "phase-r4split")
    fields = _fields(q, plan, mover, row, kind="stage_gib")
    # Real handoff: fence reserved under the grant, moved onto the mover.
    assert q.reserve_fence(TIER, grant, fields, 2) is True
    generation = str(q.read_funding(mover, TIER)["generation"])
    assert q.transfer_fence(TIER, grant, mover) == 2
    assert q.advance_funding_state(
        mover, TIER, expect="reserved", advance_to="transferring",
        generation=generation) is True
    record = q.read_funding(mover, TIER)
    bound = sorted(str(name) for name in record["tokens"])
    assert len(bound) == 2
    first, second = bound
    # Crash part-way through a stale re-home (the exact split state a
    # retry inherits): the first bound token already sits grant-ward.  The
    # grant holder directory must be recreated by hand because the
    # completed handoff above removed it once it emptied.
    os.makedirs(ledger.held_dir / grant, exist_ok=True)
    os.rename(ledger.held_dir / mover / first,
              ledger.held_dir / grant / first)
    assert _names(ledger, mover) == [second]
    assert _names(ledger, grant) == [first]
    # A republished mover makes the old transferring binding stale.
    fields2 = dict(fields, published_unix=float(
        row["published_unix"]) + 5.0)  # type: ignore[arg-type]
    # Actual failed rename on retry: the mover's holder directory is
    # read-only for this non-root worker, so the per-token rename inside
    # ResourceLedger.transfer fails with EACCES and transfer returns a
    # short count.
    (ledger.held_dir / mover).chmod(0o500)
    try:
        got = q.reserve_fence(TIER, grant, fields2, 2)
    finally:
        (ledger.held_dir / mover).chmod(0o755)
    assert got is False, "a short retry transfer must retain, never close"
    record = q.read_funding(mover, TIER)
    assert record is not None
    assert record["state"] == "transferring"
    assert str(record["generation"]) == generation
    assert sorted(str(name) for name in record["tokens"]) == bound
    # Exact ownership at the prefixes: the partial split is intact, no
    # fresh token was taken beside the leftover bound name.
    assert _names(ledger, mover) == [second]
    assert _names(ledger, grant) == [first]
    assert int(ledger.available().get("stage_gib", 0)) == 1
    # Fault cleared: the same retry converges.  Remainder moved grant-ward
    # with no free interval, the record closed through the legal step and
    # rotated to a fresh reserved generation bound to the exact grant
    # names -- no duplicate credit anywhere.
    assert q.reserve_fence(TIER, grant, fields2, 2) is True
    record2 = q.read_funding(mover, TIER)
    assert record2 is not None and record2["state"] == "reserved"
    assert str(record2["generation"]) != generation
    assert sorted(str(name) for name in record2["tokens"]) == bound
    assert _names(ledger, mover) == []
    assert _names(ledger, grant) == bound
    assert int(ledger.holder_tokens(grant).get("stage_gib", 0)) == 2
    assert int(ledger.holder_tokens(mover).get("stage_gib", 0)) == 0
    assert int(ledger.available().get("stage_gib", 0)) == 1
