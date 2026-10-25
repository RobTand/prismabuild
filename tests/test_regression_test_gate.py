"""A fix needs changed test behavior; metadata and cosmetic changes cannot supply it."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("regression_gate", ROOT / "tools/ci/check_regression_test.py")
gate = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = gate
SPEC.loader.exec_module(gate)


def pull(title="fix: repair claim ownership", *, labels=(), count=1, body="Change type: feature"):
    return {"title": title, "labels": [{"name": name} for name in labels],
            "body": body, "changed_files": count}


def test_change(before="", after="def test_claim():\n    assert claim() == 'owned'\n", **fields):
    return {"filename": "tests/test_claim.py", "status": "modified" if before else "added",
            "additions": 2, "before": before, "after": after, **fields}


test_change.__test__ = False


@pytest.mark.parametrize("title", ["fix: wrong root", "fix(pool): wrong root", "Fix wrong root",
                                  "[P1] Fix wrong root", "[P1] [PB] Repair wrong root", "bugfix: wrong root",
                                  "Regression: wrong root"])
def test_fix_title_without_tests_is_rejected(title):
    verdict = gate.evaluate(pull(title, count=0), [])
    assert not verdict.allowed
    assert "requires" in verdict.reason


@pytest.mark.parametrize("label", ["bug", "fix", "bugfix", "regression", "type:bug", "type:fix"])
def test_bug_label_cannot_be_overridden_by_feature_title(label):
    assert not gate.evaluate(pull("feat: new control", labels=[label], count=0), []).allowed


def test_explicit_fix_type_is_classified_but_issue_closing_keyword_is_not():
    assert not gate.evaluate(pull("Guard owner", body="Change type: fix", count=0), []).allowed
    assert gate.evaluate(pull("feat: new control", body="Change type: feature\nFixes #4", count=0), []).allowed
    assert gate.evaluate(pull("Add regression-test gate", count=0), []).allowed


def test_new_asserted_test_passes_and_identifies_the_test():
    verdict = gate.evaluate(pull(), [test_change()])
    assert verdict.allowed
    assert verdict.tests == ("tests/test_claim.py::test_claim",)


@pytest.mark.parametrize("source", [
    "def test_claim():\n    with pytest.raises(ValueError):\n        claim()\n",
    "class TestClaim:\n    def test_owner(self):\n        assert claim() == 'owned'\n",
    "class ClaimTests(unittest.TestCase):\n    def test_owner(self):\n        self.assertEqual(claim(), 'owned')\n",
    "async def test_owner():\n    assert await claim() == 'owned'\n",
])
def test_supported_test_shapes(source):
    assert gate.evaluate(pull(), [test_change(after=source)]).allowed


def test_modified_expectation_and_parametrization_count():
    before = "@pytest.mark.parametrize('owner', ['a'])\ndef test_claim(owner):\n    assert claim(owner) == 'owned'\n"
    expanded = before.replace("['a']", "['a', 'b']")
    assert gate.evaluate(pull(), [test_change(before, expanded)]).allowed
    assert gate.evaluate(pull(), [test_change(before, before.replace("'owned'", "'released'"))]).allowed


@pytest.mark.parametrize("after", [
    "# test fixed\ndef test_claim():\n    assert claim() == 'owned'\n",
    "def test_claim():\n    \"\"\"Explains claim behavior.\"\"\"\n    assert claim() == 'owned'\n",
    "def test_claim():\n\n    assert claim() == 'owned'  # regression\n",
])
def test_comments_docstrings_and_whitespace_do_not_count(after):
    before = "def test_claim():\n    assert claim() == 'owned'\n"
    assert not gate.evaluate(pull(), [test_change(before, after)]).allowed


def test_fixtures_helpers_and_assertions_outside_tests_do_not_count():
    source = "import pytest\n@pytest.fixture\ndef owner():\n    assert claim() == 'owned'\n    return 'owned'\ndef helper():\n    assert claim() == 'owned'\nassert True\n"
    assert not gate.evaluate(pull(), [test_change(after=source)]).allowed
    fixture_named_test = "@pytest.fixture\ndef test_owner():\n    assert claim() == 'owned'\n"
    assert not gate.evaluate(pull(), [test_change(after=fixture_named_test)]).allowed


def test_deletions_and_unchanged_renames_do_not_count():
    source = "def test_claim():\n    assert claim() == 'owned'\n"
    assert not gate.evaluate(pull(), [test_change(source, "", status="removed", additions=0)]).allowed
    assert not gate.evaluate(pull(), [test_change(source, source, status="renamed")]).allowed
    assert not gate.evaluate(pull(), [test_change(source, source.replace("test_claim", "test_owner"))]).allowed
    assert not gate.evaluate(pull(), [test_change(source + "\n\ndef test_old():\n    assert False\n", source, additions=0)]).allowed


def test_file_must_be_a_python_test_under_tests():
    for filename in ["tests/conftest.py", "src/test_claim.py", "tests/test_claim.txt", "tools/test_claim.py"]:
        assert not gate.evaluate(pull(), [test_change(filename=filename)]).allowed
    assert gate.evaluate(pull(), [test_change(filename="tests/nested/claim_test.py")]).allowed


def test_complete_sources_qualify_without_an_api_patch():
    change = test_change()
    assert "patch" not in change
    assert gate.evaluate(pull(), [change]).allowed


@pytest.mark.parametrize("missing", ["before", "after"])
def test_missing_test_text_is_not_a_pass(missing):
    change = test_change()
    del change[missing]
    assert not gate.evaluate(pull(), [change]).allowed


def test_syntax_failure_is_not_a_pass():
    assert not gate.evaluate(pull(), [test_change(after="def test_broken(:")]).allowed


def test_incomplete_or_api_limited_inventory_is_not_a_pass():
    change = test_change()
    assert not gate.evaluate(pull(count=101), [change]).allowed
    assert gate.evaluate(pull(count=101), [dict(filename=f"src/file{i}.py", status="modified", additions=1)
                                             for i in range(100)] + [change]).allowed
    assert not gate.evaluate(pull(count=3001), [change] * 3001).allowed


def test_cli_returns_json_and_rejects_invalid_input(tmp_path):
    payload = tmp_path / "input.json"
    payload.write_text(json.dumps({"pull": pull(), "files": [test_change()]}))
    command = [sys.executable, str(ROOT / "tools/ci/check_regression_test.py"), str(payload)]
    done = subprocess.run(command, capture_output=True, text=True, timeout=10)
    assert done.returncode == 0, done.stderr
    assert json.loads(done.stdout)["tests"] == ["tests/test_claim.py::test_claim"]
    payload.write_text(json.dumps({"pull": pull(), "files": []}))
    denied = subprocess.run(command, capture_output=True, text=True, timeout=10)
    assert denied.returncode == 1
    assert not json.loads(denied.stdout)["allowed"]
    payload.write_text('{"pull":null}')
    invalid = subprocess.run(command, capture_output=True, text=True, timeout=10)
    assert invalid.returncode == 2
    assert "invalid" in json.loads(invalid.stdout)["reason"]


@pytest.mark.parametrize("body", ["", "Change type: typo", "Change type: fix\nChange type: docs"])
def test_missing_invalid_or_ambiguous_change_type_is_rejected(body):
    assert not gate.evaluate(pull("DL380 checkout sealing times out", body=body), [test_change()]).allowed


def test_same_file_assertion_helper_qualifies():
    source = "def _assert_refused(value):\n    assert value == 'refused'\n\ndef test_refusal():\n    _assert_refused(claim())\n"
    assert gate.evaluate(pull(), [test_change(after=source)]).allowed


@pytest.mark.parametrize("kind", ["feature", "docs", "refactor", "test", "chore", "perf", "build", "ci", "revert"])
def test_declared_nonfix_does_not_require_a_test(kind):
    assert gate.evaluate(pull("Add a control", count=0, body=f"Change type: {kind}"), []).allowed


def test_helper_without_proof_or_a_second_hop_does_not_qualify():
    source = "def _assert_refused(value):\n    assert value == 'refused'\n\ndef wrapper(value):\n    _assert_refused(value)\n\ndef test_refusal():\n    wrapper(claim())\n"
    assert not gate.evaluate(pull(), [test_change(after=source)]).allowed


def test_existing_helper_does_not_make_an_unchanged_test_new():
    source = "def _assert_refused(value):\n    assert value == 'refused'\n\ndef test_refusal():\n    _assert_refused(claim())\n"
    assert not gate.evaluate(pull(), [test_change(source, source)]).allowed
