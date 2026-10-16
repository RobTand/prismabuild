"""Wire records and observation order survive denial encoding extraction."""
from __future__ import annotations

import json
import math

import pytest

from prismabuild import adaptive_cpu, pool


KEY = "a" * 64


@pytest.fixture
def queue(tmp_path, monkeypatch):
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "box")
    monkeypatch.setattr(pool, "_now", lambda: 100.0)
    monkeypatch.setattr(adaptive_cpu.adaptive_snapshot, "publish", lambda *args: None)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    return queue


def _latest_path(queue):
    return adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS


def _latest(queue):
    return json.loads(_latest_path(queue).read_text())


def test_latest_and_transition_files_keep_exact_wire_records(queue, monkeypatch):
    moments = iter([40.0, 41.0])
    monkeypatch.setattr(pool, "_now", lambda: next(moments))
    queue.record_denial(
        {"action_key": KEY, "published_unix": 2, "attempts": 7, "dependent_of": "producer"},
        "adaptive_cpu_refused", {"decision": {"reason": "host_pressure", "cpus": [2]}, "note": "λ"})
    expected = {
        "schema": "prismabuild.claim_denials.v1", "records": {f"{KEY}:2.0": {
            "action_key": KEY, "published_unix": 2.0, "host": "box",
            "reason": "adaptive_cpu_refused", "attempts": 7,
            "evidence": {"decision": {"reason": "host_pressure", "cpus": [2]},
                         "dependent_of": "producer", "note": "λ"}, "denied_unix": 41.0}}}
    ring = {"schema": "prismabuild.denial_transitions.v1", "action_key": KEY,
            "transitions": [{"unix": 40.0, "host": "box", "reason": "adaptive_cpu_refused",
                             "decision_reason": "host_pressure",
                             "decision": {"reason": "host_pressure", "cpus": [2]},
                             "published_unix": 2.0, "count": 1, "last_unix": 40.0}]}
    assert _latest_path(queue).read_bytes() == json.dumps(
        expected, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    assert queue.records.denial_transitions_path(KEY).read_bytes() == json.dumps(
        ring, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


@pytest.mark.parametrize("item_fields,evidence,expected", [
    ({}, {}, {}),
    ({"dependent_of": None}, {}, {}),
    ({"dependent_of": "producer"}, {}, {"dependent_of": "producer"}),
    ({"dependent_of": "producer"}, {"dependent_of": None}, {"dependent_of": None}),
    ({"dependent_of": "producer"}, {"dependent_of": "sealed"}, {"dependent_of": "sealed"}),
])
def test_dependency_evidence_preserves_omission_and_explicit_values(queue, item_fields, evidence, expected):
    queue.record_denial({"action_key": KEY, "published_unix": 1.0, **item_fields},
                        "placement_mismatch", evidence)
    record = _latest(queue)["records"][f"{KEY}:1.0"]
    assert record["evidence"] == expected
    assert record["attempts"] == 0
    assert not queue.records.denial_transitions_path(KEY).exists()


def test_evidence_bounds_preserve_collisions_depth_and_sequence_order(queue):
    class Label:
        def __repr__(self):
            return "opaque-label"

    prefix = "k" * 256
    evidence = {"collision": {prefix + "1": "first", prefix + "2": "last"},
                "deep": {"a": {"b": {"c": {"d": {"e": "leaf"}}}}},
                "many": {f"{n:02}": n for n in range(40)}, "text": "x" * 300,
                "sequence": tuple(range(40)), "one_set": {"only"}, "opaque": Label()}
    queue.record_denial({"action_key": KEY, "published_unix": 1.0}, "placement_mismatch", evidence)
    actual = _latest(queue)["records"][f"{KEY}:1.0"]["evidence"]
    assert actual == {
        "collision": {prefix: "last"}, "deep": {"a": {"b": {"c": {"d": {"e": "<truncated>"}}}}},
        "many": {f"{n:02}": n for n in range(32)}, "text": "x" * 256,
        "sequence": list(range(32)), "one_set": ["only"], "opaque": "opaque-label"}


@pytest.mark.parametrize("generation,identity", [(True, "1.0"), (-0.0, "-0.0"), (1, "1.0")])
def test_generation_spelling_keeps_numeric_identity(queue, generation, identity):
    queue.record_denial({"action_key": KEY, "published_unix": generation}, "placement_mismatch")
    records = _latest(queue)["records"]
    assert list(records) == [f"{KEY}:{identity}"]


def test_prepared_evidence_is_detached_before_stamping():
    from prismabuild import denial_records

    evidence = {"decision": {"cpus": [1]}, "note": ["first"]}
    prepared = denial_records.PreparedDenial.from_item(
        {"action_key": KEY, "published_unix": 1.0}, "fixture", evidence, host="box")
    evidence["decision"]["cpus"].append(2)
    evidence["note"][0] = "changed"
    record = prepared.stamped(50.0)
    assert record["evidence"] == {"decision": {"cpus": [1]}, "note": ["first"]}
    assert record["denied_unix"] == 50.0
    assert prepared.identity == f"{KEY}:1.0"


def test_bounding_precedes_the_latest_timestamp_after_the_ring(queue, monkeypatch):
    events = []

    class Observation:
        def __repr__(self):
            assert queue.records.denial_transitions_path(KEY).exists()
            events.append("bounded")
            return "observed"

    def clock():
        events.append("clock")
        return float(len(events))

    monkeypatch.setattr(pool, "_now", clock)
    queue.record_denial({"action_key": KEY, "published_unix": 1.0}, "fixture", {"note": Observation()})
    assert events == ["clock", "bounded", "clock"]
    assert _latest(queue)["records"][f"{KEY}:1.0"]["denied_unix"] == 3.0
    assert queue.denial_transitions(KEY)[0]["unix"] == 1.0


def test_latest_encoding_failure_keeps_the_already_written_transition(queue):
    class Unencodable:
        def __repr__(self):
            raise ValueError("unavailable evidence")

    queue.record_denial({"action_key": KEY, "published_unix": 1.0}, "fixture", {"note": Unencodable()})
    assert not _latest_path(queue).exists()
    assert queue.denial_transitions(KEY) == [{
        "unix": 100.0, "host": "box", "reason": "fixture", "decision_reason": None,
        "decision": None, "published_unix": 1.0, "count": 1, "last_unix": 100.0}]


def test_failed_transition_write_retries_then_memo_skips_reads(queue, monkeypatch):
    path = queue.records.denial_transitions_path(KEY)
    original_read, original_write = pool.record_store._read_json, pool._write_json_atomic
    reads, writes = [], []

    def read(candidate):
        if candidate == path:
            reads.append(candidate)
        return original_read(candidate)

    def write(candidate, document):
        if candidate == path:
            writes.append(document)
            if len(writes) == 1:
                raise OSError("first write failed")
        return original_write(candidate, document)

    monkeypatch.setattr(pool.record_store, '_read_json', read)
    monkeypatch.setattr(pool, "_write_json_atomic", write)
    item = {"action_key": KEY, "published_unix": 1.0}
    queue._record_denial_transition(item, host="box", reason="fixture", decision_reason=None)
    assert not path.exists()
    queue._record_denial_transition(item, host="box", reason="fixture", decision_reason=None)
    assert path.is_file()
    queue._record_denial_transition(item, host="box", reason="fixture", decision_reason=None)
    assert len(reads) == len(writes) == 2
    assert json.loads(path.read_text())["transitions"][0]["count"] == 1


def test_newest_on_file_uses_last_timestamp_and_primes_the_memo(queue, monkeypatch):
    path = queue.records.denial_transitions_path(KEY)
    path.parent.mkdir(parents=True, exist_ok=True)
    original = {"transitions": [
        {"host": "box", "published_unix": 1.0, "reason": "returning", "decision_reason": None,
         "unix": 1.0, "last_unix": 5.0, "count": 2, "decision": {"cpus": [1]}},
        {"host": "box", "published_unix": 1.0, "reason": "other", "decision_reason": None,
         "unix": 2.0, "last_unix": 2.0, "count": 1}]}
    path.write_text(json.dumps(original))
    events = []
    old_read = pool.record_store._read_json

    def read(candidate):
        if candidate == path:
            events.append("read")
        return old_read(candidate)

    def clock():
        events.append("clock")
        return 10.0

    monkeypatch.setattr(pool.record_store, '_read_json', read)
    monkeypatch.setattr(pool, "_now", clock)
    for _ in range(2):
        queue._record_denial_transition(
            {"action_key": KEY, "published_unix": 1.0}, host="box", reason="returning",
            decision_reason=None, decision={"cpus": [9]})
    assert events == ["clock", "read"]
    assert json.loads(path.read_text()) == original


def test_corrupt_ring_is_replaced_without_losing_the_latest_record(queue):
    path = queue.records.denial_transitions_path(KEY)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{broken")
    queue.record_denial({"action_key": KEY, "published_unix": 1.0}, "fixture")
    assert queue.denial_transitions(KEY)[0]["decision"] is None
    assert _latest(queue)["records"][f"{KEY}:1.0"]["reason"] == "fixture"


@pytest.mark.parametrize("reason,raises", [("placement_mismatch", False), ("fixture", True)])
def test_overflow_failure_boundary_depends_on_ring_exemption(queue, reason, raises):
    item = {"action_key": KEY, "published_unix": 10 ** 400}
    if raises:
        with pytest.raises(OverflowError):
            queue.record_denial(item, reason)
    else:
        queue.record_denial(item, reason)
    assert not queue.records.denial_transitions_path(KEY).exists()
    assert not _latest_path(queue).exists()


def test_nonfinite_evidence_is_not_repaired_by_the_encoder(queue):
    from prismabuild import denial_records

    item = {"action_key": KEY, "published_unix": 1.0}
    prepared = denial_records.PreparedDenial.from_item(
        item, "fixture", {"unknown": float("nan"), "limit": float("inf")}, host="box")
    record = prepared.stamped(10.0)
    assert math.isnan(record["evidence"]["unknown"])
    assert record["evidence"]["limit"] == float("inf")
    queue.record_denial(item, "placement_mismatch", record["evidence"])
    assert not _latest_path(queue).exists()


def test_document_merge_keeps_newer_evidence_and_stable_ties_at_both_caps():
    from prismabuild import denial_records

    existing = {
        "records": {f"r{n}": {"denied_unix": 5.0, "value": "prior"} for n in range(256)},
        "claim_passes": {f"p{n}": {"passed_unix": 5.0} for n in range(64)}}
    updates = {"r0": {"denied_unix": 5.0, "value": "tie"},
               "r1": {"denied_unix": 4.0, "value": "old"},
               "late": {"denied_unix": 5.0}, "new": {"denied_unix": 6.0}}
    document = denial_records.diagnostic_document(
        existing, updates, {"late": {"passed_unix": 5.0}, "new": {"passed_unix": 6.0}})
    assert document["schema"] == "prismabuild.claim_denials.v1"
    assert list(document["records"]) == ["new", *[f"r{n}" for n in range(255)]]
    assert list(document["claim_passes"]) == ["new", *[f"p{n}" for n in range(63)]]
    assert document["records"]["r0"]["value"] == "tie"
    assert document["records"]["r1"]["value"] == "prior"
    assert existing["records"]["r0"]["value"] == "prior"
    assert len(existing["records"]) == 256


@pytest.mark.parametrize("prior,has_summaries", [({}, True), (None, False), ([], False)])
def test_empty_summary_presence_follows_the_prior_document(prior, has_summaries):
    from prismabuild import denial_records

    document = denial_records.diagnostic_document({"claim_passes": prior}, {}, {})
    assert ("claim_passes" in document) is has_summaries
    assert document["records"] == {}
