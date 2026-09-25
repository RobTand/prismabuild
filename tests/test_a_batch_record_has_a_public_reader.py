"""A produced-output batch record has a public reader (#955).

PQ's Stage A retirement tool imported PB's private ``_load_batch_record`` and
``_read_commitments`` because nothing public returned a committed batch's
paths, identities and state.  ``produced_output.batch_record`` (one batch)
and ``batch_records`` (every batch an instance committed) are that reader:
entries with ``path``, ``bytes`` and ``sha256``; the lifetime; the
commitments entry; and the state -- ``committed``, ``retiring`` or
``reclaimed``.  An unreadable record raises and is never read as empty.

Fixture concessions: owners, batches and the reclaim are the real
``PoolQueue`` and ``produced_output`` calls.  ``retiring`` is filed onto the
commitments entry the way ``origin_retirement_tick`` files its decision
before it deletes anything, because a real tick that could not finish its
deletes leaves exactly that entry.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import produced_output as po  # noqa: E402
from test_consumed_origin_retirement import (  # noqa: E402
    _commit, _queue, _template,
)

PAYLOAD = b"band handoff bytes"


def _committed(tmp_path: Path, **kwargs):
    template = _template(tmp_path / "canonical")
    queue = _queue(tmp_path)
    instance, path, committed = _commit(queue, template, "reader",
                                        payload=PAYLOAD, **kwargs)
    return queue, template, instance, path, committed


def test_a_committed_batch_reads_back_whole(tmp_path: Path) -> None:
    queue, template, instance, path, committed = _committed(tmp_path)

    record = po.batch_record(queue, instance, template, batch_id="b1")

    assert record["state"] == po.BATCH_STATE_COMMITTED == "committed"
    assert record["lifetime"] == po.ORIGIN_LIFETIME_CONSUMED
    assert record["entries"] == [{
        "path": str(path), "bytes": len(PAYLOAD),
        "sha256": hashlib.sha256(PAYLOAD).hexdigest(),
        "artifact_class": record["entries"][0]["artifact_class"]}]
    assert record["total_bytes"] == len(PAYLOAD)
    assert record["manifest_digest"] == committed["ref"]["manifest_digest"]
    assert record["commitment"]["manifest_digest"] == record["manifest_digest"]
    assert sum(record["commitment"]["class_bytes"].values()) == len(PAYLOAD)
    assert str(path) in record["record"]["origin_identity"]
    assert po.batch_records(queue, instance, template) == [record]


def test_a_retained_batch_reports_its_lifetime(tmp_path: Path) -> None:
    queue, template, instance, _path, _committed_ = _committed(
        tmp_path, lifetime=po.ORIGIN_LIFETIME_RETAIN)

    record = po.batch_record(queue, instance, template, batch_id="b1")

    assert record["lifetime"] == po.ORIGIN_LIFETIME_RETAIN


def test_a_retiring_batch_says_so(tmp_path: Path) -> None:
    queue, template, instance, _path, _committed_ = _committed(tmp_path)
    commitments_path = po._commitments_path(queue.root, instance)
    commitments = po._read_commitments(commitments_path)
    commitments["batches"]["b1"]["retiring"] = {"reason": "orphan",
                                               "consumers": []}
    po._write_commitments(commitments_path, commitments)

    record = po.batch_record(queue, instance, template, batch_id="b1")

    assert record["state"] == po.BATCH_STATE_RETIRING == "retiring"
    assert record["commitment"]["retiring"]["reason"] == "orphan"
    assert record["entries"], "a retiring batch still names what it deletes"


def test_a_reclaimed_batch_still_reads_its_record(tmp_path: Path) -> None:
    queue, template, instance, path, _committed_ = _committed(tmp_path)
    queue.finish(instance["owner_action_key"], status="executed")
    path.unlink()
    assert po.reclaim_origin(queue, instance, template,
                             batch_id="b1")["reclaimed"] is True

    record = po.batch_record(queue, instance, template, batch_id="b1")

    assert record["state"] == po.BATCH_STATE_RECLAIMED == "reclaimed"
    assert record["commitment"]["origin_reclaimed"] is True
    assert [entry["path"] for entry in record["entries"]] == [str(path)]
    assert [r["state"] for r in po.batch_records(queue, instance, template)] == [
        "reclaimed"]


def test_an_unreadable_record_raises_and_is_never_empty(tmp_path: Path) -> None:
    queue, template, instance, _path, _committed_ = _committed(tmp_path)
    batch_file = (queue.root / "residency" / po.OUTPUT_BATCHES_SUBDIR
                  / po.instance_namespace(instance) / "b1.json")
    # The record is published immutable (0444, `_publish_immutable`); only
    # root writes through that, so a pbtest shard's user makes it writable
    # first.
    batch_file.chmod(0o644)
    batch_file.write_text("{ not json")

    with pytest.raises(po.ProducedOutputError, match="unknown-retain"):
        po.batch_record(queue, instance, template, batch_id="b1")
    with pytest.raises(po.ProducedOutputError, match="unknown-retain"):
        po.batch_records(queue, instance, template)

    batch_file.unlink()
    with pytest.raises(po.ProducedOutputError, match="batch-record-missing"):
        po.batch_record(queue, instance, template, batch_id="b1")


def test_unreadable_commitments_raise_and_are_never_empty(tmp_path: Path) -> None:
    queue, template, instance, _path, _committed_ = _committed(tmp_path)
    po._commitments_path(queue.root, instance).write_text("garbage")

    with pytest.raises(po.ProducedOutputError, match="unreadable"):
        po.batch_records(queue, instance, template)
    with pytest.raises(po.ProducedOutputError, match="unreadable"):
        po.batch_record(queue, instance, template, batch_id="b1")


def test_an_unknown_batch_and_a_foreign_template_are_refusals(
        tmp_path: Path) -> None:
    queue, template, instance, _path, _committed_ = _committed(tmp_path)

    with pytest.raises(po.ProducedOutputError, match="unknown-batch"):
        po.batch_record(queue, instance, template, batch_id="b2")
    other = dict(template, template_id="another-template")
    with pytest.raises(po.ProducedOutputError, match="template-mismatch"):
        po.batch_record(queue, instance, other, batch_id="b1")


def test_the_reader_is_public() -> None:
    for name in ("batch_record", "batch_records", "BATCH_STATE_COMMITTED",
                 "BATCH_STATE_RETIRING", "BATCH_STATE_RECLAIMED"):
        assert name in po.__all__, name
