"""PB #1429: a person's recorded acceptance of the lifetime contract's assumption.

No component bounds how long settlement takes, so a deadline is a release
bound only for someone who accepts the assumption. Admission reads that
decision from the queue root. These tests pin what counts as a decision and
what does not: the default is no decision, and every record that is not an
exact acceptance of the current statement is no decision either.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src")]
from prismabuild import lifetime_acceptance as acceptance, lifetime_fence  # noqa: E402


def _record(**changes):
    record = {
        "schema": acceptance.ACCEPTANCE_SCHEMA_V1,
        "contract": lifetime_fence.LIFETIME_SCHEMA_V1,
        "assumption": lifetime_fence.ASSUMPTION,
        "accepted_by": "a person",
        "authority": "an explicit instruction",
        "accepted_unix": 1000.0,
    }
    record.update(changes)
    return record


def _file(root: Path, payload: object) -> Path:
    path = acceptance.acceptance_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload if isinstance(payload, bytes) else json.dumps(payload).encode())
    return path


def test_nothing_is_accepted_by_default(tmp_path):
    assert acceptance.assumption_accepted(tmp_path) is False
    assert acceptance.current_acceptance(tmp_path) == (None, "no acceptance record")


def test_a_recorded_acceptance_names_the_person_the_authority_and_the_statement(tmp_path):
    record = acceptance.record_acceptance(
        tmp_path, accepted_by=" a person ", authority=" an explicit instruction ",
        now_unix=1234.5)
    assert record == _record(accepted_unix=1234.5)
    assert acceptance.current_acceptance(tmp_path) == (record, None)
    assert acceptance.assumption_accepted(tmp_path) is True
    path = acceptance.acceptance_path(tmp_path)
    assert path == tmp_path / "lifetime-fence" / "acceptance.json"
    assert path.stat().st_mode & 0o777 == 0o644
    assert lifetime_fence.ASSUMPTION in path.read_text()
    # Another queue is another decision.
    other = tmp_path / "other"
    other.mkdir()
    assert acceptance.assumption_accepted(other) is False


@pytest.mark.parametrize("accepted_by,authority", [
    ("", "x"), ("  ", "x"), (None, "x"), ("x", ""), ("x", "\n"), ("x", None), (3, "x")])
def test_an_acceptance_without_a_person_or_an_authority_is_refused_and_files_nothing(
        tmp_path, accepted_by, authority):
    with pytest.raises(ValueError):
        acceptance.record_acceptance(tmp_path, accepted_by=accepted_by, authority=authority)
    assert not acceptance.acceptance_path(tmp_path).exists()


@pytest.mark.parametrize("changes", [
    {"schema": "other.v1"},
    {"contract": "other.v1"},
    {"assumption": "Another statement."},
    {"accepted_by": ""},
    {"accepted_by": "  "},
    {"accepted_by": None},
    {"authority": ""},
    {"authority": 3},
    {"accepted_unix": "now"},
    {"accepted_unix": None},
    {"accepted_unix": True},
    {"accepted_unix": float("nan")},
])
def test_a_record_that_is_not_an_acceptance_of_the_current_assumption_is_no_decision(
        tmp_path, changes):
    _file(tmp_path, _record(**changes))
    record, why = acceptance.current_acceptance(tmp_path)
    assert record is None and why
    assert acceptance.assumption_accepted(tmp_path) is False


def test_a_missing_or_extra_field_is_no_decision(tmp_path):
    for field in _record():
        _file(tmp_path, {key: value for key, value in _record().items() if key != field})
        assert acceptance.assumption_accepted(tmp_path) is False, field
    _file(tmp_path, {**_record(), "extra": 1})
    assert acceptance.assumption_accepted(tmp_path) is False
    _file(tmp_path, _record())
    assert acceptance.assumption_accepted(tmp_path) is True


@pytest.mark.parametrize("payload", [
    b"", b"not json", b"[]", b"null", b"3", b"\xff\xfe",
    b'{"schema": "a", "schema": "b"}',
])
def test_a_record_that_is_not_strict_json_is_no_decision(tmp_path, payload):
    _file(tmp_path, payload)
    assert acceptance.assumption_accepted(tmp_path) is False


def test_a_changed_statement_ends_an_older_acceptance(tmp_path, monkeypatch):
    acceptance.record_acceptance(tmp_path, accepted_by="a person", authority="a message")
    assert acceptance.assumption_accepted(tmp_path) is True
    # Another reserve or other words is another decision.
    monkeypatch.setattr(lifetime_fence, "ASSUMPTION", lifetime_fence.ASSUMPTION + " More.")
    assert acceptance.assumption_accepted(tmp_path) is False
    record, why = acceptance.current_acceptance(tmp_path)
    assert record is None and "another statement" in why


def test_a_link_an_oversized_file_and_a_directory_are_no_decision(tmp_path):
    outside = tmp_path / "outside.json"
    outside.write_text(json.dumps(_record()))
    path = acceptance.acceptance_path(tmp_path)
    path.parent.mkdir(parents=True)
    path.symlink_to(outside)
    assert acceptance.assumption_accepted(tmp_path) is False
    path.unlink()
    path.write_text(" " * (acceptance.MAX_RECORD_BYTES + 1) + json.dumps(_record()))
    assert acceptance.assumption_accepted(tmp_path) is False
    path.unlink()
    path.mkdir()
    assert acceptance.assumption_accepted(tmp_path) is False


def test_a_record_reached_through_a_linked_directory_is_no_decision(tmp_path):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / acceptance.ACCEPTANCE_FILE).write_text(json.dumps(_record()))
    root = tmp_path / "queue"
    root.mkdir()
    (root / acceptance.ACCEPTANCE_DIR).symlink_to(elsewhere)
    assert acceptance.assumption_accepted(root) is False


def test_a_second_acceptance_replaces_the_first(tmp_path):
    first = acceptance.record_acceptance(
        tmp_path, accepted_by="a", authority="b", now_unix=1.0)
    second = acceptance.record_acceptance(
        tmp_path, accepted_by="c", authority="d", now_unix=2.0)
    assert acceptance.current_acceptance(tmp_path) == (second, None)
    assert second != first
    assert [path.name for path in acceptance.acceptance_path(tmp_path).parent.iterdir()] == [
        acceptance.ACCEPTANCE_FILE]


def test_a_withdrawn_acceptance_is_gone_and_withdrawing_twice_is_harmless(tmp_path):
    acceptance.record_acceptance(tmp_path, accepted_by="a", authority="b")
    assert acceptance.withdraw_acceptance(tmp_path) is True
    assert acceptance.assumption_accepted(tmp_path) is False
    assert acceptance.withdraw_acceptance(tmp_path) is False


def test_the_command_records_shows_and_withdraws_the_decision(tmp_path, capsys):
    def run(*argv):
        assert acceptance.main(["--queue", str(tmp_path), *argv]) == 0
        return json.loads(capsys.readouterr().out)

    status = run("status")
    assert status["accepted"] is False and status["why_not"] == "no acceptance record"
    assert status["record"] is None and status["assumption"] == lifetime_fence.ASSUMPTION
    done = run("accept", "--by", "a person", "--authority", "a message")
    assert done["accepted"] is True and done["record"]["accepted_by"] == "a person"
    assert done["record"]["authority"] == "a message"
    status = run("status")
    assert status["accepted"] is True and status["why_not"] is None
    assert status["record"] == done["record"]
    assert run("revoke") == {"revoked": True}
    assert run("revoke") == {"revoked": False}
    assert run("status")["accepted"] is False


@pytest.mark.parametrize("argv", [
    ["accept"],
    ["accept", "--by", "a person"],
    ["accept", "--authority", "a message"],
    ["accept", "--by", " ", "--authority", "a message"],
    ["accept", "--by", "a person", "--authority", ""],
])
def test_the_command_refuses_an_acceptance_without_a_person_and_an_authority(
        tmp_path, argv):
    with pytest.raises(SystemExit) as refused:
        acceptance.main(["--queue", str(tmp_path), *argv])
    assert refused.value.code != 0
    assert not acceptance.acceptance_path(tmp_path).exists()
