"""Offer and live interpreter evidence preserve placement and claim authority."""
from __future__ import annotations

import hashlib
import json
import sys

import pytest

from prismabuild import capability_eligibility as eligibility, contracts, dependency_digest, pool


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


def _publish_dependencies(queue, key, *, files=None, local=None):
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", resources=CAPACITY,
                  requires_files=files, local_dependencies=local)


def _required(path, payload=b"required bytes"):
    return {"path": str(path), "sha256": hashlib.sha256(payload).hexdigest()}


@pytest.mark.parametrize("kind,reason", [
    ("matching", None),
    ("changed", "dependency_digest_mismatch"),
    ("missing", "dependency_not_present"),
    ("directory", "dependency_not_present"),
    ("unreadable", "dependency_unreadable"),
])
def test_offer_path_presence_does_not_prove_required_bytes(tmp_path, monkeypatch, kind, reason):
    path = tmp_path / "dependency"
    entry = _required(path)
    if kind == "directory":
        path.mkdir()
    elif kind != "missing":
        path.write_bytes(b"different bytes" if kind == "changed" else b"required bytes")

    def cannot_hash_offer(_path):
        raise AssertionError("an advisory offer must not read dependency bytes")

    with monkeypatch.context() as observation:
        observation.setattr(dependency_digest, "claim_digest", cannot_hash_offer)
        answers = eligibility.observe_dependencies(
            [{"requires_files": [entry]}], python=sys.executable)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.announce(host="observed", tags=[contracts.DEPENDENCY_DIGEST_TAG], has_gpu=False,
                   capacity=CAPACITY, dependency_files=sorted(answers.files_present),
                   dependency_files_absent=sorted(answers.files_absent))
    _publish_dependencies(queue, "a" * 64, files=[entry])
    if kind == "unreadable":
        claim_digest = dependency_digest.claim_digest

        def vanished_before_read(candidate):
            path.unlink()
            return claim_digest(candidate)

        monkeypatch.setattr(dependency_digest, "claim_digest", vanished_before_read)

    assert queue.placeable({"tags": [contracts.DEPENDENCY_DIGEST_TAG],
                            "requires_files": [entry], "resources": CAPACITY}) is (
                                kind not in ("directory", "missing"))
    claimed = queue.claim(tags=[contracts.DEPENDENCY_DIGEST_TAG], capacity=CAPACITY)

    assert (claimed is not None) == (reason is None)
    if reason is not None:
        denial = _denials(queue)[-1]
        assert denial["reason"] == reason
        if kind == "changed":
            assert denial["evidence"]["expected"] == entry["sha256"]
            assert denial["evidence"]["observed"] == hashlib.sha256(b"different bytes").hexdigest()
        assert queue.passes("a" * 64) == 0
        assert queue.item_path(pool.READY, "a" * 64).is_file()


@pytest.mark.parametrize("required_kind", ["path", "executable"])
@pytest.mark.parametrize("actual", ["file", "executable", "directory", "dangling", "missing"])
def test_local_path_offer_and_claim_share_requirement_semantics(tmp_path, required_kind, actual):
    path = tmp_path / "dependency"
    if actual == "executable":
        _executable(path)
    elif actual == "file":
        path.write_text("data")
        path.chmod(0o600)
    elif actual == "directory":
        path.mkdir()
    elif actual == "dangling":
        path.symlink_to(tmp_path / "absent")
    local = {str(path): required_kind}
    answers = eligibility.observe_dependencies(
        [{"local_dependencies": local}], python=sys.executable)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.announce(host="observed", tags=[contracts.LOCAL_DEPENDENCY_TAG], has_gpu=False,
                   capacity=CAPACITY, local_dependency_answers=answers.local_paths)
    _publish_dependencies(queue, "a" * 64, local=local)
    expected = actual == "executable" or (
        required_kind == "path" and actual in ("file", "directory"))

    assert queue.placeable({"tags": [contracts.LOCAL_DEPENDENCY_TAG],
                            "local_dependencies": local, "resources": CAPACITY}) is expected
    assert (queue.claim(tags=[contracts.LOCAL_DEPENDENCY_TAG], capacity=CAPACITY) is not None) is expected
    if not expected:
        denial = _denials(queue)[-1]
        assert denial["reason"] == "local_dependency_not_present"
        assert denial["evidence"]["paths"] == [str(path)]


@pytest.mark.parametrize("malformed_local,reason", [
    (False, "local_dependency_not_present"),
    (True, "local_dependencies_unavailable"),
])
def test_local_refusal_precedes_a_foreign_digest_error(tmp_path, monkeypatch, malformed_local, reason):
    path = tmp_path / "dependency"
    path.write_bytes(b"required bytes")
    queue = pool.PoolQueue(tmp_path / "queue")
    _publish_dependencies(queue, "a" * 64, files=[_required(path)],
                          local={str(tmp_path / "missing"): "path"})
    ready = queue.item_path(pool.READY, "a" * 64)
    record = json.loads(ready.read_text())
    record["requires_files"] = False
    if malformed_local:
        record["local_dependencies"] = []
    ready.write_text(json.dumps(record))

    def cannot_hash(_path):
        raise AssertionError("a local refusal must precede the digest probe")

    monkeypatch.setattr(dependency_digest, "claim_digest", cannot_hash)
    assert queue.claim(tags=[contracts.LOCAL_DEPENDENCY_TAG, contracts.DEPENDENCY_DIGEST_TAG],
                       capacity=CAPACITY) is None
    assert _denials(queue)[-1]["reason"] == reason
    assert queue.passes("a" * 64) == 0


def test_first_digest_failure_follows_the_foreign_rows_declared_order(tmp_path):
    paths = [tmp_path / "z", tmp_path / "a"]
    for path in paths:
        path.write_bytes(b"actual")
    files = [_required(path) for path in paths]
    queue = pool.PoolQueue(tmp_path / "queue")
    _publish_dependencies(queue, "a" * 64, files=files)
    ready = queue.item_path(pool.READY, "a" * 64)
    record = json.loads(ready.read_text())
    record["requires_files"] = files
    ready.write_text(json.dumps(record))

    assert queue.claim(tags=[contracts.DEPENDENCY_DIGEST_TAG], capacity=CAPACITY) is None

    denial = _denials(queue)[-1]
    assert denial["reason"] == "dependency_digest_mismatch"
    assert denial["evidence"]["path"] == str(paths[0])


@pytest.mark.parametrize("answers,expected", [
    ([], "unknown_capability"),
    ([{}], "unknown_paths"),
    ([{"present": ["/a"]}, {"present": ["/b"]}], "unknown_paths"),
    ([{"present": ["/a", "/b"]}, {}], "present"),
    ([{"absent": ["/a", "/b"]}, {"absent": ["/a", "/b"]}], "absent"),
    ([{"absent": ["/a", "/b"]}, {}], "unknown_paths"),
    ([{"present": ["/a", "/b"], "absent": ["/a", "/b"]}], "present"),
])
def test_submission_needs_one_host_with_all_required_paths(tmp_path, answers, expected):
    queue = pool.PoolQueue(tmp_path / "queue")
    for index, fields in enumerate(answers):
        queue.announce(host=f"host-{index}", tags=[contracts.DEPENDENCY_DIGEST_TAG],
                       has_gpu=False, capacity=CAPACITY, dependency_files=fields.get("present"),
                       dependency_files_absent=fields.get("absent"))
    assert queue.dependency_placement_verdict({
        "tags": [contracts.DEPENDENCY_DIGEST_TAG], "resources": CAPACITY,
        "requires_files": [_required("/a"), _required("/b")],
    }) == expected


def test_dependency_evidence_is_detached_from_its_source_and_advertisement():
    offer = {"local_dependencies": {"/a": "path"}, "dependency_files": ["/b"]}
    answers = eligibility.DependencyAnswers.from_offer(offer)
    offer["local_dependencies"]["/a"] = "absent"
    offer["dependency_files"].clear()
    advertisement = answers.offer_fields()
    advertisement["local_dependencies"].clear()
    advertisement["dependency_files"].clear()

    assert answers.matches({"/a": "path"}, [_required("/b")])


def test_live_digest_keeps_its_first_answer_but_reobserves_presence_each_candidate(tmp_path, monkeypatch):
    path = tmp_path / "dependency"
    path.write_bytes(b"old")
    entry = _required(path, b"new")
    queue = pool.PoolQueue(tmp_path / "queue")
    for digit in "ab":
        _publish_dependencies(queue, digit * 64, files=[entry])
    claim_digest = dependency_digest.claim_digest
    isfile = eligibility.os.path.isfile
    removed = False

    def removed_after_hash(candidate):
        nonlocal removed
        digest = claim_digest(candidate)
        if not removed:
            path.unlink()
            removed = True
        return digest

    def installed_after_missing_observation(candidate):
        present = isfile(candidate)
        if candidate == str(path) and removed and not present:
            path.write_bytes(b"new")
        return present

    monkeypatch.setattr(dependency_digest, "claim_digest", removed_after_hash)
    monkeypatch.setattr(eligibility.os.path, "isfile", installed_after_missing_observation)

    assert queue.claim(tags=[contracts.DEPENDENCY_DIGEST_TAG], capacity=CAPACITY) is None
    denials = _denials(queue)
    assert len(denials) == 2
    assert {denial["reason"] for denial in denials} == {"dependency_digest_mismatch"}
    assert {denial["evidence"]["observed"] for denial in denials} == {hashlib.sha256(b"old").hexdigest()}
    assert path.read_bytes() == b"new"
    assert queue.claim(tags=[contracts.DEPENDENCY_DIGEST_TAG], capacity=CAPACITY) is not None
