"""The independent denial reader preserves record order and host authority."""
import json

import pytest

from claim_denial_reader import latest_claim_denial
from prismabuild import adaptive_cpu, pool


@pytest.fixture
def denial_log(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    queue.root.mkdir(parents=True, exist_ok=True)
    (queue.root / pool.CLAIM_DENIALS).write_text(json.dumps({
        "records": {"decoy": {"action_key": "ours", "denied_unix": 999}}}))
    return queue, path


@pytest.mark.parametrize("records, expected", [
    ({}, None),
    ({"bad": [], "unkeyed": {}, "foreign": {"action_key": "other", "denied_unix": "invalid"},
      "match": {"action_key": "ours", "denied_unix": 1}}, "match"),
    ({"nine": {"action_key": "ours", "denied_unix": "9"},
      "ten": {"action_key": "ours", "denied_unix": "10"}}, "ten"),
    ({"negative": {"action_key": "ours", "denied_unix": -1},
      "missing": {"action_key": "ours"}}, "missing"),
    ({"first": {"action_key": "ours", "denied_unix": 3},
      "second": {"action_key": "ours", "denied_unix": "3"}}, "first"),
], ids=["empty", "filter", "numeric", "default", "tie"])
def test_selects_the_latest_matching_host_local_record(denial_log, records, expected):
    queue, path = denial_log
    for name, record in records.items():
        if isinstance(record, dict):
            record["tag"] = name
    raw = json.dumps({"records": records})
    path.write_text(raw)

    result = latest_claim_denial(queue, "ours")

    assert (None if result is None else result["tag"]) == expected
    assert path.read_text() == raw


@pytest.mark.parametrize("raw", [None, "{broken", "{}"], ids=["absent", "invalid-json", "no-records"])
def test_absent_denials_do_not_use_shared_queue_copies(denial_log, raw):
    queue, path = denial_log
    if raw is not None:
        path.write_text(raw)

    assert latest_claim_denial(queue, "ours") is None


@pytest.mark.parametrize("timestamp, error", [("invalid", ValueError), (None, TypeError)])
def test_invalid_matching_timestamps_remain_errors(denial_log, timestamp, error):
    queue, path = denial_log
    path.write_text(json.dumps({"records": {
        "bad": {"action_key": "ours", "denied_unix": timestamp}}}))

    with pytest.raises(error):
        latest_claim_denial(queue, "ours")
