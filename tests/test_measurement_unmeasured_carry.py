"""Incomplete legacy CPU proof cannot masquerade as measured protection."""
from __future__ import annotations

import copy
import pytest

from test_measurement_drains_gpu_backfill import fleet  # noqa: F401
from test_measurement_unmeasured_timeout import _unmeasured, FAIRNESS_BOUND_S
from prismabuild import pool


@pytest.mark.parametrize("baseline", [None, {"basis": "unknown", "exceeds": True}],
                         ids=["missing", "unknown-basis"])
def test_incomplete_legacy_baseline_proof_is_not_measured(fleet, baseline):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, first, started = _unmeasured(fleet)
    item = pool._read_json(queue.item_path(pool.READY, measurement))
    assert isinstance(item, dict)
    records = copy.deepcopy(queue._host_denial_records())
    stamp = item["published_unix"]
    assert isinstance(stamp, (int, float))
    generation = f"{measurement}:{float(stamp)!r}"
    record = records[generation]
    assert isinstance(record, dict)
    evidence = record["evidence"]
    assert isinstance(evidence, dict)
    decision = evidence["decision"]
    assert isinstance(decision, dict)
    decision["baseline"] = baseline
    verdict = evidence["withhold"]
    assert isinstance(verdict, dict)
    # Legacy holder protection had only the incumbent-derived deadline. A
    # missing baseline classification must not bypass the uncertainty bound.
    verdict["drain_until_unix"] = first["measurement_drain_until_unix"]
    verdict.pop("unmeasured_until_unix", None)
    verdict.pop("measurement_unmeasured", None)
    assert queue._carried_withhold(records, item, host="sparklina", now=clock[0]) is None
    assert queue.item_path(pool.CLAIMED, holder).exists()


def test_expired_unknown_does_not_reserve_a_physically_free_gpu(fleet):
    queue, clock, readings, sample, publish, tick, claim, denial = fleet
    holder, measurement, first, started = _unmeasured(fleet, holder_gpu=0)
    backfill = publish("free-gpu-backfill")
    assert claim() is None
    assert denial(backfill)["evidence"]["withheld_for"] == measurement
    tick(FAIRNESS_BOUND_S + 1)
    result = claim()
    if result is None:
        assert denial(measurement)["evidence"]["withhold"]["withhold"] is False
    assert result == backfill
    assert queue.item_path(pool.READY, measurement).exists()
    assert queue.item_path(pool.CLAIMED, holder).exists()


def _tail_decision():
    return {"reason": "measurement_host_not_idle", "fresh": True,
            "baseline": {"state": "holder_tail", "exceeds": True,
                         "holders_seen_unix": 9.5, "interval_start_unix": 9.0},
            "sample": {"sampled_unix": 10.0, "interval_s": 1.0}}


@pytest.mark.parametrize("seen", [9.0, 9.5, 10.0])
def test_known_holder_tail_is_not_an_invented_measured_baseline(seen):
    decision = _tail_decision()
    decision["baseline"]["holders_seen_unix"] = seen
    assert "basis" not in decision["baseline"]
    assert pool._measurement_holder_tail_refusal(decision)
    assert not pool._measurement_unmeasured_refusal("adaptive_cpu_refused", decision)


@pytest.mark.parametrize("section,field,value", [
    ("baseline", "state", "idle"), ("baseline", "exceeds", False),
    ("baseline", "holders_seen_unix", None), ("baseline", "holders_seen_unix", 11.0),
    ("baseline", "holders_seen_unix", 8.0), ("baseline", "interval_start_unix", 8.0),
    ("baseline", "interval_start_unix", float("nan")),
    ("sample", "sampled_unix", float("inf")), ("sample", "sampled_unix", 1 << 8192),
    ("sample", "sampled_unix", True), ("sample", "interval_s", 0.0),
    ("sample", "interval_s", 31.0), ("sample", "interval_s", None),
], ids=["not-tail", "not-exceeded", "missing-overlap", "future-overlap", "before-window",
        "wrong-start", "nan-start", "infinite-sample", "huge-sample", "bool-sample",
        "zero-interval", "large-interval", "missing-interval"])
def test_malformed_holder_tail_does_not_authorize_protection(section, field, value):
    decision = _tail_decision()
    decision[section][field] = value
    assert not pool._measurement_holder_tail_refusal(decision)


def test_stale_or_missing_holder_tail_proof_remains_unknown():
    decision = _tail_decision()
    decision["fresh"] = False
    assert not pool._measurement_holder_tail_refusal(decision)
    assert not pool._measurement_holder_tail_refusal(None)
    assert not pool._measurement_holder_tail_refusal({"fresh": True})
