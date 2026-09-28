"""The client SDK's two fail-closed edges refuse, never crash or misreport (#1267).

``cas_receipt_self_check`` answers a non-dict ``producer`` with the
attestation refusal instead of an ``AttributeError``, and
``read_claimed_record`` reads an EMPTY claimed file as unreadable rather
than "not claimed" -- a torn write or a broken mount is never a verdict
that the action is unclaimed.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from prismabuild import client, core as pb, pool


def _queue_at(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    return queue


def _valid_receipt(producer) -> dict:
    body = {
        "action_key": "a" * 64,
        "action_manifest_sha256": "0" * 64,
        "producer": producer,
        "result": {"bytes": 1, "sha256": "1" * 64},
        "schema": client.CAS_RECEIPT_SCHEMA_V3,
    }
    return {**body, "receipt_sha256": pb.canonical_sha256(body)}


GOOD_PRODUCER = {
    "schema": client.WORKER_ATTESTATION_SCHEMA_V2,
    "action_key": "a" * 64,
    "attestation_sha256": "2" * 64,
    "evidence": {"host": "sparky"},
}


def test_a_good_receipt_self_checks_clean():
    producer = dict(GOOD_PRODUCER)
    producer["attestation_sha256"] = pb.canonical_sha256(
        {k: v for k, v in producer.items() if k != "attestation_sha256"})
    assert client.cas_receipt_self_check(_valid_receipt(producer)) is None


@pytest.mark.parametrize("producer", [None, ["not", "a", "dict"], "a-string"])
def test_a_non_dict_producer_is_the_attestation_refusal(producer):
    """#1267: the refusal, never an AttributeError from producer.get."""

    refusal = client.cas_receipt_self_check(_valid_receipt(producer))

    assert refusal == client.RECEIPT_REFUSALS[2]


def test_an_absent_claim_row_is_none(tmp_path):
    queue = _queue_at(tmp_path)
    assert client.read_claimed_record(queue, "b" * 64) is None


def test_an_empty_claim_row_is_unreadable_not_unclaimed(tmp_path):
    """#1267: a torn write or broken mount is never read as 'not claimed'."""

    queue = _queue_at(tmp_path)
    claimed = queue.item_path(pool.CLAIMED, "c" * 64)
    claimed.parent.mkdir(parents=True, exist_ok=True)
    claimed.write_text("")

    with pytest.raises(Exception, match="unreadable|empty|not an object|not valid JSON"):
        client.read_claimed_record(queue, "c" * 64)


def test_a_non_object_claim_row_still_raises(tmp_path):
    queue = _queue_at(tmp_path)
    claimed = queue.item_path(pool.CLAIMED, "d" * 64)
    claimed.parent.mkdir(parents=True, exist_ok=True)
    claimed.write_text(json.dumps(["a", "list"]))

    with pytest.raises(pool.PoolContractError):
        client.read_claimed_record(queue, "d" * 64)


def test_a_real_claim_row_reads(tmp_path):
    queue = _queue_at(tmp_path)
    claimed = queue.item_path(pool.CLAIMED, "e" * 64)
    claimed.parent.mkdir(parents=True, exist_ok=True)
    claimed.write_text(json.dumps({"action_key": "e" * 64}))

    assert client.read_claimed_record(
        queue, "e" * 64) == {"action_key": "e" * 64}
