"""Offer and live interpreter evidence preserve placement and claim authority."""
from __future__ import annotations

import json
import sys

import pytest

from prismabuild import capability_eligibility as eligibility, contracts, pool


TAG = contracts.INTERPRETER_TAG
CAPACITY = {"cpu": 1, "mem_gb": 1}


def _publish(queue, key, interpreter):
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", resources=CAPACITY,
                  interpreter=interpreter)


def _denials(queue):
    from prismabuild import adaptive_cpu
    base = adaptive_cpu.local_state_base(queue.ledger().base)
    return list(adaptive_cpu.read_json(base / pool.CLAIM_DENIALS).get("records", {}).values())


def _executable(path):
    path.write_text("#!/bin/sh\n")
    path.chmod(0o700)
    return str(path)


@pytest.mark.parametrize("kind", ["executable", "file", "directory", "dangling", "missing"])
def test_worker_offer_and_live_claim_agree_on_observed_paths(tmp_path, kind):
    path = tmp_path / "python"
    if kind == "executable":
        _executable(path)
    elif kind == "file":
        path.write_text("not executable")
        path.chmod(0o600)
    elif kind == "directory":
        path.mkdir()
    elif kind == "dangling":
        path.symlink_to(tmp_path / "missing-target")
    named = str(path)
    answers = eligibility.observe_interpreters([
        {"interpreter": named}, {"interpreter": named}, {}, None])
    fields = answers.offer_fields()
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.announce(host="observed", tags=[TAG], has_gpu=False, capacity=CAPACITY,
                   interpreters=fields["interpreters"],
                   interpreters_absent=fields["interpreters_absent"])
    _publish(queue, "a" * 64, named)
    expected = kind == "executable"

    assert queue.placeable({"tags": [TAG], "interpreter": named,
                            "resources": CAPACITY}) is expected
    claimed = queue.claim(tags=[TAG], capacity=CAPACITY)

    assert (claimed is not None) is expected
    if not expected:
        assert queue.item_path(pool.READY, "a" * 64).is_file()
        assert queue.passes("a" * 64) == 0
        denial = _denials(queue)[-1]
        assert denial["reason"] == "interpreter_not_present"
        assert denial["evidence"]["interpreter"] == named


@pytest.mark.parametrize("offered_present", [True, False])
def test_live_claim_uses_current_path_instead_of_offer_answer(tmp_path, offered_present):
    path = tmp_path / "python"
    named = str(path) if offered_present else _executable(path)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.announce(host="previous-poll", tags=[TAG], has_gpu=False, capacity=CAPACITY,
                   interpreters=[named] if offered_present else [],
                   interpreters_absent=[] if offered_present else [named])
    _publish(queue, "a" * 64, named)

    assert queue.placeable({"tags": [TAG], "interpreter": named,
                            "resources": CAPACITY}) is offered_present
    claimed = queue.claim(tags=[TAG], capacity=CAPACITY)

    assert (claimed is not None) == (not offered_present)
    if offered_present:
        assert _denials(queue)[-1]["reason"] == "interpreter_not_present"


@pytest.mark.parametrize("value,reason", [
    ("", "malformed_interpreter"),
    ("relative/python", "malformed_interpreter"),
    (7, "malformed_interpreter"),
    ("/", "interpreter_not_present"),
    ("/invalid\x00python", "interpreter_not_present"),
])
def test_publication_and_foreign_claim_keep_their_existing_validation(tmp_path, value, reason):
    queue = pool.PoolQueue(tmp_path / "queue")
    with pytest.raises(pool.PoolContractError, match="absolute path to an executable"):
        _publish(queue, "a" * 64, value)
    assert not queue.item_path(pool.READY, "a" * 64).exists()
    _publish(queue, "b" * 64, sys.executable)
    path = queue.item_path(pool.READY, "b" * 64)
    record = json.loads(path.read_text())
    record["interpreter"] = value
    path.write_text(json.dumps(record))

    assert queue.claim(tags=[TAG], capacity=CAPACITY) is None

    denial = _denials(queue)[-1]
    assert denial["reason"] == reason
    assert denial["evidence"]["interpreter"] == value
    assert queue.passes("b" * 64) == 0
    assert path.is_file()


@pytest.mark.parametrize("answers,expected", [
    ([], "unknown"),
    ([{}], "unknown"),
    ([{"interpreters_absent": ["/python"]}], "absent"),
    ([{"interpreters_absent": ["/python"]}, {}], "unknown"),
    ([{"interpreters_absent": ["/python"]}, {"interpreters": ["/python"]}], "present"),
    ([{"interpreters": ["/python"], "interpreters_absent": ["/python"]}], "present"),
    ([{"interpreters": "/python", "interpreters_absent": ["/python"]}], "absent"),
])
def test_submission_requires_unanimous_negative_answers(tmp_path, answers, expected):
    queue = pool.PoolQueue(tmp_path / "queue")
    for index, fields in enumerate(answers):
        queue.announce(host=f"host-{index}", tags=[TAG], has_gpu=False,
                       capacity=CAPACITY)
        path = queue.root / pool.WORKERS / f"host-{index}.json"
        record = json.loads(path.read_text())
        record.update(fields)
        path.write_text(json.dumps(record))

    assert queue.interpreter_placement_verdict(
        {"tags": [TAG], "resources": CAPACITY}, "/python") == expected


def test_missing_path_stays_missing_for_the_pass_and_refreshes_next_pass(tmp_path, monkeypatch):
    path = tmp_path / "python"
    named = str(path)
    queue = pool.PoolQueue(tmp_path / "queue")
    for digit in "ab":
        _publish(queue, digit * 64, named)
    isfile = eligibility.os.path.isfile

    def installed_after_lookup(candidate):
        if candidate == named and not path.exists():
            _executable(path)
            return False
        return isfile(candidate)

    monkeypatch.setattr(eligibility.os.path, "isfile", installed_after_lookup)

    assert queue.claim(tags=[TAG], capacity=CAPACITY) is None
    assert {denial["reason"] for denial in _denials(queue)} == {"interpreter_not_present"}
    assert len(_denials(queue)) == 2
    assert queue.claim(tags=[TAG], capacity=CAPACITY) is not None
