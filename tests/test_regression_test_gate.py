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
            "body": body, "changed_files": count,
            "head": {"sha": "head"}, "base": {"sha": "base"}}


def test_change(before="", after="def test_claim():\n    assert claim() == 'owned'\n", **fields):
    return {"filename": "tests/test_claim.py", "status": "modified" if before else "added",
            "additions": 2, "before": before, "after": after, **fields}


test_change.__test__ = False


@pytest.mark.parametrize("title", ["fix: wrong root", "fix(pool): wrong root", "Fix wrong root",
                                  "[P1] Fix wrong root", "[P1] [PB] Repair wrong root", "bugfix: wrong root",
                                  "Regression: wrong root"])
def test_fix_title_without_tests_is_rejected(title):
    verdict = gate.check_pull_request(pull(title, count=0), [])
    assert not verdict.allowed
    assert "requires" in verdict.reason


@pytest.mark.parametrize("label", ["bug", "fix", "bugfix", "regression", "type:bug", "type:fix"])
def test_bug_label_cannot_be_overridden_by_feature_title(label):
    assert not gate.check_pull_request(pull("feat: new control", labels=[label], count=0), []).allowed


def test_explicit_fix_type_is_classified_but_issue_closing_keyword_is_not():
    assert not gate.check_pull_request(pull("Guard owner", body="Change type: fix", count=0), []).allowed
    assert gate.check_pull_request(pull("feat: new control", body="Change type: feature\nFixes #4", count=0), []).allowed
    assert gate.check_pull_request(pull("Add regression-test gate", count=0), []).allowed


def test_new_asserted_test_passes_and_identifies_the_test():
    verdict = gate.check_pull_request(pull(), [test_change()])
    assert verdict.allowed
    assert verdict.tests == ("tests/test_claim.py::test_claim",)


@pytest.mark.parametrize("source", [
    "def test_claim():\n    with pytest.raises(ValueError):\n        claim()\n",
    "class TestClaim:\n    def test_owner(self):\n        assert claim() == 'owned'\n",
    "class ClaimTests(unittest.TestCase):\n    def test_owner(self):\n        self.assertEqual(claim(), 'owned')\n",
    "async def test_owner():\n    assert await claim() == 'owned'\n",
])
def test_supported_test_shapes(source):
    assert gate.check_pull_request(pull(), [test_change(after=source)]).allowed


def test_modified_expectation_and_parametrization_count():
    before = "@pytest.mark.parametrize('owner', ['a'])\ndef test_claim(owner):\n    assert claim(owner) == 'owned'\n"
    expanded = before.replace("['a']", "['a', 'b']")
    assert gate.check_pull_request(pull(), [test_change(before, expanded)]).allowed
    assert gate.check_pull_request(pull(), [test_change(before, before.replace("'owned'", "'released'"))]).allowed


@pytest.mark.parametrize("after", [
    "# test fixed\ndef test_claim():\n    assert claim() == 'owned'\n",
    "def test_claim():\n    \"\"\"Explains claim behavior.\"\"\"\n    assert claim() == 'owned'\n",
    "def test_claim():\n\n    assert claim() == 'owned'  # regression\n",
])
def test_comments_docstrings_and_whitespace_do_not_count(after):
    before = "def test_claim():\n    assert claim() == 'owned'\n"
    assert not gate.check_pull_request(pull(), [test_change(before, after)]).allowed


def test_fixtures_helpers_and_assertions_outside_tests_do_not_count():
    source = "import pytest\n@pytest.fixture\ndef owner():\n    assert claim() == 'owned'\n    return 'owned'\ndef helper():\n    assert claim() == 'owned'\nassert True\n"
    assert not gate.check_pull_request(pull(), [test_change(after=source)]).allowed
    fixture_named_test = "@pytest.fixture\ndef test_owner():\n    assert claim() == 'owned'\n"
    assert not gate.check_pull_request(pull(), [test_change(after=fixture_named_test)]).allowed


def test_deletions_and_unchanged_renames_do_not_count():
    source = "def test_claim():\n    assert claim() == 'owned'\n"
    assert not gate.check_pull_request(pull(), [test_change(source, "", status="removed", additions=0)]).allowed
    assert not gate.check_pull_request(pull(), [test_change(source, source, status="renamed")]).allowed
    assert not gate.check_pull_request(pull(), [test_change(source, source.replace("test_claim", "test_owner"))]).allowed
    assert not gate.check_pull_request(pull(), [test_change(source + "\n\ndef test_old():\n    assert False\n", source, additions=0)]).allowed


def test_file_must_be_a_python_test_under_tests():
    for filename in ["tests/conftest.py", "src/test_claim.py", "tests/test_claim.txt", "tools/test_claim.py"]:
        assert not gate.check_pull_request(pull(), [test_change(filename=filename)]).allowed
    assert gate.check_pull_request(pull(), [test_change(filename="tests/nested/claim_test.py")]).allowed


def test_complete_sources_qualify_without_an_api_patch():
    change = test_change()
    assert "patch" not in change
    assert gate.check_pull_request(pull(), [change]).allowed


@pytest.mark.parametrize("missing", ["before", "after"])
def test_missing_test_text_is_not_a_pass(missing):
    change = test_change()
    del change[missing]
    assert not gate.check_pull_request(pull(), [change]).allowed


def test_syntax_failure_is_not_a_pass():
    assert not gate.check_pull_request(pull(), [test_change(after="def test_broken(:")]).allowed


def test_incomplete_or_api_limited_inventory_is_not_a_pass():
    change = test_change()
    assert not gate.check_pull_request(pull(count=101), [change]).allowed
    assert gate.check_pull_request(pull(count=101), [dict(filename=f"src/file{i}.py", status="modified", additions=1)
                                             for i in range(100)] + [change]).allowed
    assert not gate.check_pull_request(pull(count=3001), [change] * 3001).allowed


def test_cli_returns_json_and_rejects_invalid_input(tmp_path):
    payload = tmp_path / "input.json"
    payload.write_text(json.dumps({"pull": pull(), "current": pull(), "files": [test_change()]}))
    command = [sys.executable, str(ROOT / "tools/ci/check_regression_test.py"), str(payload)]
    done = subprocess.run(command, capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    assert json.loads(done.stdout)["tests"] == ["tests/test_claim.py::test_claim"]
    payload.write_text(json.dumps({"pull": pull(), "current": pull(), "files": []}))
    denied = subprocess.run(command, capture_output=True, text=True)
    assert denied.returncode == 1
    assert not json.loads(denied.stdout)["allowed"]
    payload.write_text('{"pull":null}')
    invalid = subprocess.run(command, capture_output=True, text=True)
    assert invalid.returncode == 2
    assert "invalid" in json.loads(invalid.stdout)["reason"]


@pytest.mark.parametrize("body", ["", "Change type: typo", "Change type: fix\nChange type: docs"])
def test_missing_invalid_or_ambiguous_change_type_is_rejected(body):
    assert not gate.check_pull_request(pull("DL380 checkout sealing times out", body=body), [test_change()]).allowed


def test_same_file_assertion_helper_qualifies():
    source = "def _assert_refused(value):\n    assert value == 'refused'\n\ndef test_refusal():\n    _assert_refused(claim())\n"
    assert gate.check_pull_request(pull(), [test_change(after=source)]).allowed


@pytest.mark.parametrize("kind", ["feature", "docs", "refactor", "test", "chore", "perf", "build", "ci", "revert"])
def test_declared_nonfix_does_not_require_a_test(kind):
    assert gate.check_pull_request(pull("Add a control", count=0, body=f"Change type: {kind}"), []).allowed


def test_helper_without_proof_or_a_second_hop_does_not_qualify():
    source = "def _assert_refused(value):\n    assert value == 'refused'\n\ndef wrapper(value):\n    _assert_refused(value)\n\ndef test_refusal():\n    wrapper(claim())\n"
    assert not gate.check_pull_request(pull(), [test_change(after=source)]).allowed


def test_existing_helper_does_not_make_an_unchanged_test_new():
    source = "def _assert_refused(value):\n    assert value == 'refused'\n\ndef test_refusal():\n    _assert_refused(claim())\n"
    assert not gate.check_pull_request(pull(), [test_change(source, source)]).allowed


def test_moving_a_test_between_files_does_not_qualify():
    source = "def test_claim():\n    assert claim() == 'owned'\n"
    moved = [test_change(source, "", status="removed", additions=0),
             test_change(after=source, filename="tests/test_other.py")]
    assert not gate.check_pull_request(pull(count=2), moved).allowed


def test_release_train_pr_needs_no_change_type_but_lookalike_branches_do():
    release = {**pull("Release 83df53ab", body="- member PR", count=0),
               "head": {"ref": "tl/release-83df53ab"}}
    assert gate.check_pull_request(release, []).allowed
    for ref in ("tlx/release-83df53ab", "fix/tl/release-83df53ab", "prismabuild-2281"):
        assert not gate.check_pull_request({**release, "head": {"ref": ref}}, []).allowed



def merge_queue_batch():
    repository = {"full_name": "RobTand/prismabuild"}
    return {
        **pull("Mergify merge queue batch", body="- member PR", count=0),
        "draft": True,
        "user": {"login": "mergify[bot]", "id": 37929162, "type": "Bot"},
        "head": {"ref": "mergify/merge-queue/batch-id", "repo": dict(repository)},
        "base": {"repo": dict(repository)},
    }


def test_authenticated_merge_queue_draft_is_not_applicable_without_change_type():
    verdict = gate.check_pull_request(merge_queue_batch(), [])
    assert verdict.allowed
    assert "not applicable" in verdict.reason
    assert verdict.tests == ()


@pytest.mark.parametrize("location,field,value", [
    (None, "draft", False),
    ("user", "login", "ordinary-author"),
    ("user", "id", 37929163),
    ("user", "type", "User"),
    ("head", "ref", "mergify/other/batch-id"),
    ("head", "ref", "prismabuild-2428-regression-test-drafts"),
    ("head", "repo", {"full_name": "external/prismabuild"}),
    ("head", "repo", None),
    ("base", "repo", None),
])
def test_merge_queue_lookalikes_still_require_change_type(location, field, value):
    subject = merge_queue_batch()
    (subject if location is None else subject[location])[field] = value
    verdict = gate.check_pull_request(subject, [])
    assert not verdict.allowed
    assert "Declare exactly one Change type:" in verdict.reason


def _node_outcome(tmp_path, monkeypatch, github_actions):
    """Run the workflow adapter helper with no `node` on PATH and report how it ended."""
    monkeypatch.setenv('PATH', str(tmp_path))
    if github_actions is None:
        monkeypatch.delenv('GITHUB_ACTIONS', raising=False)
    else:
        monkeypatch.setenv('GITHUB_ACTIONS', github_actions)
    try:
        run_regression_workflow_snapshot(tmp_path, [], {})
    except pytest.skip.Exception:
        return 'skipped'
    except pytest.fail.Exception:
        return 'failed'
    return 'ran'


def test_missing_node_fails_on_github_actions(tmp_path, monkeypatch):
    assert _node_outcome(tmp_path, monkeypatch, 'true') == 'failed'


@pytest.mark.parametrize('github_actions', [None, '', 'false'])
def test_missing_node_skips_off_github_actions(tmp_path, monkeypatch, github_actions):
    assert _node_outcome(tmp_path, monkeypatch, github_actions) == 'skipped'


def run_regression_workflow_snapshot(tmp_path, labels, current_changes):
    """Drive the shipped github-script against two deterministic REST snapshots."""
    import os
    import shutil
    import yaml
    if not shutil.which('node'):
        # Hosted CI must run these cases: the merge gate does not fail on skips.
        if os.environ.get('GITHUB_ACTIONS') == 'true':
            pytest.fail('Node.js is required on GitHub Actions to exercise the workflow adapter')
        pytest.skip('Node.js unavailable; hosted CI exercises the workflow adapter')
    workflow = yaml.safe_load((ROOT / '.github/workflows/regression-test.yml').read_text())
    script = workflow['jobs']['regression-test']['steps'][0]['with']['script']
    before = {**pull('Add control', labels=labels, count=0),
              'head': {'sha': 'head'}, 'base': {'sha': 'base'}}
    current = {**before, **current_changes}
    payload = tmp_path / 'snapshots.json'
    payload.write_text(json.dumps({'script': script, 'before': before, 'current': current,
                                  'helper': (ROOT / 'tools/ci/check_regression_test.py').read_text()}))
    driver = r'''
const fs = require('fs');
const cp = require('child_process');
const input = JSON.parse(fs.readFileSync(process.argv[1], 'utf8'));
const failures = [], messages = [];
let reads = 0;
const github = {
  rest: {
    pulls: {get: async () => ({data: reads++ ? input.current : input.before}), listFiles: 'files'},
    repos: {
      getContent: async () => ({data: {type: 'file', encoding: 'base64', size: input.helper.length,
                                      content: Buffer.from(input.helper).toString('base64')}}),
      compareCommitsWithBasehead: async () => ({data: {merge_base_commit: {sha: 'merge'}}}),
    },
  },
  paginate: async () => [],
};
const core = {setFailed: text => failures.push(text), info: text => messages.push(text)};
const exec = {getExecOutput: async (command, args) => {
  const result = cp.spawnSync(command, args, {encoding: 'utf8'});
  if (result.error) throw result.error;
  return {exitCode: result.status, stdout: result.stdout, stderr: result.stderr};
}};
const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
new AsyncFunction('require', 'context', 'github', 'core', 'exec', input.script)(
  require, {repo: {owner: 'example', repo: 'pb'}, payload: {pull_request: {number: 1}}},
  github, core, exec).then(() => console.log(JSON.stringify({failures, messages}))).catch(error => {
    console.error(error); process.exitCode = 1;
  });
'''
    result = subprocess.run(['node', '-e', driver, str(payload)], capture_output=True, text=True,
                            env={**os.environ, 'RUNNER_TEMP': str(tmp_path)}, timeout=60)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


@pytest.mark.parametrize('label', ['queue', 'queued', 'dequeued', 'tl3-queue'])
def test_queue_only_label_change_does_not_invalidate_gathered_evidence(tmp_path, label):
    result = run_regression_workflow_snapshot(tmp_path, [], {'labels': [{'name': label}]})
    assert result['failures'] == [], 'queue-only change must not invalidate regression evidence'


@pytest.mark.parametrize('labels,current', [
    (['queue', 'bug'], [{'name': 'bug', 'color': 'new'}]),
    (['bug', 'fix'], [{'name': ' FIX '}, {'name': 'BUG'}]),
])
def test_label_order_cosmetics_and_normalisation_do_not_invalidate_evidence(tmp_path, labels, current):
    # No test diff is present, so a fix verdict must still be rejected for coverage,
    # rather than incorrectly rejected as a metadata race.
    result = run_regression_workflow_snapshot(tmp_path, labels, {'labels': current})
    assert len(result['failures']) == 1
    assert 'requires an added or modified Python test' in ''.join(result['messages'])


@pytest.mark.parametrize('label', sorted(gate._FIX_LABELS))
@pytest.mark.parametrize('remove', [False, True])
def test_verdict_label_addition_or_removal_invalidates_gathered_evidence(tmp_path, label, remove):
    result = run_regression_workflow_snapshot(tmp_path, [label] if remove else [],
        {'labels': [] if remove else [{'name': label}]})
    assert result['failures']
    assert 'PR changed while gathering evidence' in ''.join(result['messages'] + result['failures'])


@pytest.mark.parametrize('field,value', [
    ('head', {'sha': 'new-head'}), ('base', {'sha': 'new-base'}),
    ('title', 'New title'), ('body', 'Change type: ci'),
])
def test_nonlabel_verdict_metadata_changes_still_invalidate_evidence(tmp_path, field, value):
    result = run_regression_workflow_snapshot(tmp_path, [], {field: value})
    assert result['failures']
    assert 'PR changed while gathering evidence' in ''.join(result['messages'] + result['failures'])


@pytest.mark.parametrize('field,value', [
    ('head', {'sha': 'new-head'}), ('base', {'sha': 'new-base'}),
    ('title', 'New title'), ('body', 'Change type: ci'),
])
def test_python_evidence_rejects_nonlabel_verdict_metadata_changes(field, value):
    before = pull('Add a control')
    assert gate.regression_evidence_matches(before, {**before, field: value}) is False


def write_replay_input(tmp_path, **fields):
    payload = tmp_path / 'input.json'
    payload.write_text(json.dumps({'pull': pull('Add a control'), 'current': pull('Add a control'),
                                  'files': [], **fields}))
    return [sys.executable, str(ROOT / 'tools/ci/check_regression_test.py'), str(payload)]


def test_cli_rejects_replay_when_current_differs_from_pull(tmp_path):
    command = write_replay_input(tmp_path, current={**pull('Add a control'), 'title': 'Renamed'})
    done = subprocess.run(command, capture_output=True, text=True)
    assert done.returncode == 1, done.stderr
    verdict = json.loads(done.stdout)
    assert not verdict['allowed']
    assert 'PR changed while gathering evidence' in verdict['reason']


def test_cli_rejects_replay_input_without_current(tmp_path):
    command = write_replay_input(tmp_path)
    payload = Path(command[-1])
    data = json.loads(payload.read_text())
    del data['current']
    payload.write_text(json.dumps(data))
    done = subprocess.run(command, capture_output=True, text=True)
    assert done.returncode == 2, done.stderr
    assert not json.loads(done.stdout)['allowed']
    assert 'invalid' in json.loads(done.stdout)['reason']


@pytest.mark.parametrize('labels,current_labels,expected', [
    ([], ['queue'], True), (['queue', 'bug'], [' BUG '], True),
    ([], ['fix'], False), (['fix'], [], False),
])
def test_python_evidence_uses_the_same_label_semantics_as_fix_classification(labels, current_labels, expected):
    before = pull('Add a control', labels=labels)
    current = pull('Add a control', labels=current_labels)
    assert gate.regression_evidence_matches(before, current) is expected
    assert gate.is_fix(before) is ('bug' in labels or 'fix' in labels)
