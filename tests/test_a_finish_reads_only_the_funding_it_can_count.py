"""The prepaid-output census a finish pays scales with what it can count (#747).

Every ``PoolQueue.finish`` concludes through ``_release_reservation``, which
asked the output census once per tier, and the census read every output
funding record in the pool. ``consumed`` is terminal and nothing advanced it,
so the records only accumulated: 973 on 2026-09-22, 0.8 s of NFS reads per
finish for an action that held nothing. These tests pin the three fixes: no
census without a tier holding, one census per conclusion, and terminal
records retired out of the census directory while every per-mover read of
them answers as before.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import pool  # noqa: E402

STAGE = "prismabuild-stage:dl380g10"
RAM = "ram:dl380g10"
OWNER = "a" * 64
OTHER = "b" * 64
MOVER = "c" * 64


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "q")
    queue.mint_tier_capacity(STAGE, {"stage_gib": 4})
    queue.mint_tier_capacity(RAM, {"ram_gib": 4})
    return queue


def _record(*, owner=OWNER, mover=MOVER, tier=STAGE, state="consumed",
            kind="stage_gib", tokens=None) -> dict:
    return {"schema": pool.TIER_FUNDING_OUTPUT_SCHEMA_V1, "tier_id": tier,
            "kind": kind, "mover_action_key": mover,
            "tokens": list(tokens or [f"{kind}-0"]), "generation": "e" * 32,
            "state": state, "unix": 1.0, "published_unix": 1.0,
            "owner_action_key": owner, "owner_nonce": "f" * 32,
            "owner_scope_id": "scope", "owner_published_unix": 1.0,
            "template_id": "template", "template_sha256": "1" * 64,
            "batch_id": "b1", "manifest_digest": "2" * 64,
            "range_start_bytes": 0, "range_end_bytes": 1}


def _file(queue: pool.PoolQueue, record: dict) -> Path:
    path = queue.funding_output_path(record["mover_action_key"], record["tier_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record))
    return path


def _row(queue: pool.PoolQueue, state: str, key: str) -> None:
    path = queue.item_path(state, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"action_key": key}))


def _held(queue: pool.PoolQueue, tier: str, key: str) -> list[str]:
    directory = queue.tier_ledger(tier).held_dir / key
    return sorted(p.name for p in directory.iterdir()) if directory.is_dir() else []


def _count_funding_reads(monkeypatch, queue: pool.PoolQueue) -> list[str]:
    reads: list[str] = []
    original = pool._read_json
    funding = str(queue.root / pool.TIER_FUNDING)

    def counted(path, *args, **kwargs):
        if str(path).startswith(funding):
            reads.append(str(path))
        return original(path, *args, **kwargs)
    monkeypatch.setattr(pool, "_read_json", counted)
    return reads


def test_a_finish_holding_no_tier_token_reads_no_funding_record(tmp_path, monkeypatch):
    queue = _queue(tmp_path)
    for index in range(40):
        _file(queue, _record(owner=OTHER, mover=f"{index:064x}"))
    reads = _count_funding_reads(monkeypatch, queue)
    assert queue._release_reservation(OWNER, host=None) == 0
    assert reads == []


def test_a_holder_reads_one_census_and_keeps_only_its_promised_names(tmp_path, monkeypatch):
    queue = _queue(tmp_path)
    assert queue.tier_ledger(STAGE).acquire(OWNER, {"stage_gib": 2})
    assert queue.tier_ledger(RAM).acquire(OWNER, {"ram_gib": 1})
    promised = _held(queue, STAGE, OWNER)[0]
    _file(queue, _record(state="reserved", tokens=[promised]))
    for index in range(10):
        _file(queue, _record(owner=OTHER, mover=f"{index:064x}"))
    calls: list[str] = []
    original = queue.output_census_for_owner

    def counted(owner_key):
        calls.append(owner_key)
        return original(owner_key)
    monkeypatch.setattr(queue, "output_census_for_owner", counted)
    queue._release_reservation(OWNER, host=None)
    assert calls == [OWNER]
    assert _held(queue, STAGE, OWNER) == [promised]
    assert _held(queue, RAM, OWNER) == []


def test_an_unknown_census_still_retains_a_holders_tokens(tmp_path):
    queue = _queue(tmp_path)
    assert queue.tier_ledger(STAGE).acquire(OWNER, {"stage_gib": 1})
    held = _held(queue, STAGE, OWNER)
    corrupt = queue.funding_output_path(MOVER, STAGE)
    corrupt.parent.mkdir(parents=True, exist_ok=True)
    corrupt.write_text("{not json")
    queue._release_reservation(OWNER, host=None)
    assert _held(queue, STAGE, OWNER) == held


def _terminal_world(tmp_path):
    queue = _queue(tmp_path)
    record = _record()
    path = _file(queue, record)
    _row(queue, pool.DONE, MOVER)
    return queue, record, path


def test_a_terminal_record_leaves_the_census_and_reads_as_before(tmp_path, monkeypatch):
    queue, record, path = _terminal_world(tmp_path)
    assert queue._output_funding_unretired(MOVER)
    counts = queue.retire_terminal_output_funding()
    assert counts["retired"] == 1 and counts["scanned"] == 1
    assert not path.exists()
    assert queue.funding_output_retired_path(MOVER, STAGE).exists()
    assert queue.output_funding_file_state(MOVER, STAGE) == (record, "ok")
    assert queue.read_output_funding(MOVER, STAGE) == record
    assert queue._output_funding_unretired(MOVER)
    reads = _count_funding_reads(monkeypatch, queue)
    assert queue.output_census_for_owner(OWNER) == ([], False)
    assert reads == []
    with pytest.raises(pool.PoolContractError, match="retired"):
        queue.write_output_funding(_record(state="reserved", tokens=["stage_gib-1"]))
    assert queue.retire_terminal_output_funding() == {
        "scanned": 0, "retired": 0, "kept": 0, "busy": 0, "unreadable": 0}


@pytest.mark.parametrize("fact", [
    "transferring", "reserved", "claimed", "ready", "not-terminal", "lease",
    "holds-a-token", "corrupt"])
def test_a_record_that_could_still_count_stays_in_the_census(tmp_path, fact):
    queue, record, path = _terminal_world(tmp_path)
    if fact in ("transferring", "reserved"):
        _file(queue, _record(state=fact))
    elif fact == "claimed":
        _row(queue, pool.CLAIMED, MOVER)
    elif fact == "ready":
        _row(queue, pool.READY, MOVER)
    elif fact == "not-terminal":
        os.unlink(queue.item_path(pool.DONE, MOVER))
    elif fact == "lease":
        queue.lease_path(MOVER).parent.mkdir(parents=True, exist_ok=True)
        queue.lease_path(MOVER).write_text("{}")
    elif fact == "holds-a-token":
        assert queue.tier_ledger(STAGE).acquire(MOVER, {"stage_gib": 1})
    elif fact == "corrupt":
        path.write_text("{not json")
    counts = queue.retire_terminal_output_funding()
    assert counts["retired"] == 0 and counts["kept"] == 1
    assert path.exists()
    assert not queue.funding_output_retired_path(MOVER, STAGE).exists()
