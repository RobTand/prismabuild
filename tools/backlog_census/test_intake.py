import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from census import REPOS
from intake import has_evidence
from duplicates import likely_duplicate


FAKE_GH = r'''
import json, os, sys
from pathlib import Path
path = Path(os.environ['INTAKE_FAKE_STATE'])
state = json.loads(path.read_text())
if '--slurp' in sys.argv:
    print('unknown flag: --slurp', file=sys.stderr)
    sys.exit(1)
route = sys.argv[2]
state['calls'].append(route)
path.write_text(json.dumps(state))
if state.get('fail') == route:
    print('GitHub unavailable', file=sys.stderr)
    sys.exit(1)
if route == 'graphql':
    payload = json.load(sys.stdin)
    if state.get('graphql_errors'):
        print(json.dumps({'data': None, 'errors': [{'message': 'forbidden'}]}))
        sys.exit(0)
    fields = payload['variables']['input']
    assert 'createIssue(input:$input)' in payload['query']
    assert fields['parentIssueId'] == state['parent']['node_id']
    state['created'].append(fields)
    path.write_text(json.dumps(state))
    print(json.dumps({'data': {'createIssue': {'issue': {
        'number': 41, 'url': 'https://github.com/RobTand/prismabuild/issues/41',
        'parent': {'id': fields['parentIssueId']}}}}}))
elif route.endswith('/issues?state=open&per_page=100'):
    repo = route.split('/')[1:3]
    for page in state['issues']['/'.join(repo)]:
        print(json.dumps(page))
elif '/issues/' in route:
    print(json.dumps(state['parent']))
elif '/labels?' in route:
    print(json.dumps(state['labels']))
else:
    print(json.dumps({'node_id': 'repository-7'}))
'''


@pytest.fixture
def cli(tmp_path):
    program = tmp_path / "gh"
    program.write_text(f"#!{sys.executable}\n" + FAKE_GH)
    program.chmod(0o755)
    state_path = tmp_path / "state.json"
    body_path = tmp_path / "body.md"
    state = {"issues": {repo: [[]] for repo in REPOS}, "calls": [], "created": [],
             "parent": {"node_id": "parent-40", "state": "open"},
             "labels": [{"name": "P2", "node_id": "label-p2"},
                        {"name": "ready-for-agent", "node_id": "label-ready"}]}

    def run(*extra, body="Build the ticket intake command."):
        state_path.write_text(json.dumps(state))
        body_path.write_text(body)
        command = [sys.executable, str(Path(__file__).with_name("intake.py")),
                   "--repo", "RobTand/prismabuild", "--parent", "40", "--priority", "P2",
                   "--title", "Build the ticket intake command", "--body-file", str(body_path), *extra]
        env = {**os.environ, "PATH": str(tmp_path) + os.pathsep + os.environ.get("PATH", ""),
               "INTAKE_FAKE_STATE": str(state_path)}
        result = subprocess.run(command, text=True, capture_output=True, env=env, timeout=20)
        after = json.loads(state_path.read_text())
        return result.returncode, json.loads(result.stdout), after

    return state, run


def test_dry_run_checks_every_repo_and_never_creates(cli):
    _, run = cli
    code, report, state = run()
    assert code == 0
    assert report["status"] == "dry_run"
    assert state["created"] == []
    assert len([call for call in state["calls"] if "state=open" in call]) == 3


def test_apply_creates_a_native_child_with_canonical_priority_and_existing_labels(cli):
    _, run = cli
    code, report, state = run("--apply", "--label", "ready-for-agent")
    assert code == 0
    assert report == {"status": "created", "number": 41,
                      "url": "https://github.com/RobTand/prismabuild/issues/41"}
    assert len(state["created"]) == 1
    assert state["created"][0] == {
        "repositoryId": "repository-7", "parentIssueId": "parent-40",
        "title": "[P2] Build the ticket intake command", "body": "Build the ticket intake command.",
        "labelIds": ["label-p2", "label-ready"]}


def test_duplicate_on_second_page_of_another_repo_refuses_without_writes(cli):
    state, run = cli
    state["issues"]["RobTand/tessera"] = [[], [{
        "state": "open", "title": "[P3] BUILD the ticket intake command!",
        "html_url": "https://github.com/RobTand/tessera/issues/99"}]]
    code, report, after = run("--apply")
    assert code == 2
    assert report["reason"] == "likely_duplicate"
    assert report["duplicates"] == ["https://github.com/RobTand/tessera/issues/99"]
    assert after["created"] == []


def test_pull_requests_are_not_duplicate_issues(cli):
    state, run = cli
    state["issues"]["RobTand/prismabuild"] = [[{
        "pull_request": {}, "state": "open", "title": "Build the ticket intake command",
        "html_url": "https://github.com/RobTand/prismabuild/pull/99"}]]
    assert run()[1]["status"] == "dry_run"


@pytest.mark.parametrize("priority", ["P0", "P1"])
def test_high_priority_without_evidence_refuses_before_network(cli, priority):
    _, run = cli
    code, report, after = run("--priority", priority, "--apply", body="## Done when\nThe bug is fixed.")
    assert code == 2
    assert report["reason"] == "high_priority_requires_reproduction_or_measurement"
    assert after["calls"] == []


@pytest.mark.parametrize("body", [
    "## Reproduction\n`pbstatus --json` exits 1 for an empty queue.",
    "## Measurement\nThe retained receipt reports 8 GiB for a declared 4 GiB action.",
    "**Reproduction:** Run `pbstatus`; observe exit code 1.",
    "**Measurement**: Receipt records 3 seconds.",
])
def test_supplied_reproduction_or_measurement_allows_high_priority(cli, body):
    _, run = cli
    assert run("--priority", "P1", body=body)[1]["status"] == "dry_run"


@pytest.mark.parametrize("body", [
    "Needs a reproduction someday.", "Reproductionless report", "Reproduction is missing.", "## Measurement\nTODO",
    "## Reproduction\n\n## Acceptance criteria\nWrite a test.",
    "## Reproduction\n<!-- paste steps here -->", "## Measurement\nN/A",
    "Reproduction: TBD", "## Reproduction\n```\n```", "Reproduction: <steps here>",
    "## Reproduction\n```bash\n```", "## Measurement\n- [ ] TODO", "## Reproduction\n1. TBD",
])
def test_empty_or_placeholder_sections_are_not_evidence(body):
    assert not has_evidence(body)


def test_fuzzy_matching_preserves_census_threshold_and_short_title_rule():
    assert likely_duplicate("[P1] Stop ordinary CPU admission waiting on the measurement census",
                            "[P3] Stop ordinary CPU admission waiting on a measurement census")
    assert not likely_duplicate("short report A", "short report B")
    assert not likely_duplicate("", "!!!")


def test_late_pagination_failure_refuses_creation(cli):
    state, run = cli
    state["fail"] = "repos/RobTand/tessera/issues?state=open&per_page=100"
    code, report, after = run("--apply")
    assert code == 1
    assert report["status"] == "error"
    assert after["created"] == []


@pytest.mark.parametrize("parent", [{"state": "closed", "node_id": "parent-40"},
                                   {"state": "open", "node_id": "parent-40", "pull_request": {}}])
def test_parent_must_be_an_open_issue(cli, parent):
    state, run = cli
    state["parent"] = parent
    code, report, after = run("--apply")
    assert code == 2
    assert report["reason"] == "parent_must_be_an_open_issue"
    assert after["created"] == []


@pytest.mark.parametrize("extra", [("--title", "[P1] conflicting"),
                                  ("--title", "[P2] [P2] doubled"),
                                  ("--parent", "0"), ("--parent", "somewhere/else#3"),
                                  ("--title", " !!! ")])
def test_invalid_input_never_contacts_github(cli, extra):
    _, run = cli
    code, report, after = run(*extra, "--apply")
    assert code == 1
    assert report["status"] == "error"
    assert after["calls"] == []


def test_matching_title_prefix_is_not_doubled(cli):
    _, run = cli
    report = run("--title", "[P2] Create work")[1]
    assert report["input"]["title"] == "[P2] Create work"


def test_cross_repo_parent_is_resolved_in_its_repository(cli):
    _, run = cli
    code, report, after = run("--parent", "https://github.com/RobTand/tessera/issues/40")
    assert code == 0
    assert "repos/RobTand/tessera/issues/40" in after["calls"]
    assert report["input"]["parentIssueId"] == "parent-40"


@pytest.mark.parametrize("label,reason", [("P1", "conflicting_priority_labels"),
                                         ("unknown-label", "unknown_labels")])
def test_invalid_labels_refuse_before_creation(cli, label, reason):
    _, run = cli
    code, report, after = run("--apply", "--label", label)
    assert code == 2
    assert report["reason"] == reason
    assert after["created"] == []


def test_absent_priority_label_does_not_invent_a_label(cli):
    state, run = cli
    state["labels"] = []
    code, report, after = run("--apply")
    assert code == 0
    assert report["status"] == "created"
    assert after["created"][0]["labelIds"] == []
    assert after["created"][0]["title"].startswith("[P2] ")


def test_graphql_error_is_not_reported_as_a_creation(cli):
    state, run = cli
    state["graphql_errors"] = True
    code, report, after = run("--apply")
    assert code == 1
    assert report["status"] == "error"
    assert after["created"] == []


def test_requested_dry_run_examples(cli):
    state, run = cli
    state["issues"]["RobTand/prismabuild"] = [[{
        "state": "open", "title": "[P2] Build the ticket intake command",
        "html_url": "https://github.com/RobTand/prismabuild/issues/99"}]]
    for extra, body, reason in [
        ((), "Build the command.", "likely_duplicate"),
        (("--priority", "P1"), "Urgent fix with no reproduction.",
         "high_priority_requires_reproduction_or_measurement"),
    ]:
        code, report, after = run(*extra, body=body)
        print("CLI dry run:", json.dumps(report, sort_keys=True))
        assert code == 2
        assert report["reason"] == reason
        assert after["created"] == []


@pytest.mark.parametrize("priority", ["P0", "P1"])
@pytest.mark.parametrize("body", [
    "Reproduction: TBD\nExpected behavior: create a native child",
    "Measurement: N/A\nImpact: urgent backlog cleanup",
    "## Reproduction\nTODO\nExpected behavior: create a native child",
    "```markdown\nReproduction: run false and observe exit 1\n```",
])
def test_unrelated_fields_and_fenced_labels_refuse_before_github(cli, priority, body):
    _, run = cli
    code, report, after = run("--priority", priority, "--apply", body=body)
    assert (code, report.get("reason"), after["calls"], after["created"]) == (
        2, "high_priority_requires_reproduction_or_measurement", [], [])


@pytest.mark.parametrize("body", [
    "## Reproduction\n```bash\n# Reproduce on a fresh checkout\nfalse\n```",
    "## Measurement\n~~~sh\n# Measure the failure\nfalse\n~~~",
    "Reproduction:\n````bash\n```\n# A shorter fence is code\nfalse\n````",
    "## Reproduction\n```bash\n~~~\n# The other delimiter is code\nfalse\n```",
])
def test_fenced_shell_comments_preserve_evidence_through_apply(cli, body):
    _, run = cli
    code, report, after = run("--priority", "P1", "--apply", body=body)
    assert (code, report.get("status"), len(after["created"])) == (0, "created", 1)


@pytest.mark.parametrize("priority", ["P0", "P1"])
@pytest.mark.parametrize("prefix", ["- ", "* ", "+ ", "1. ", "2) ", "- [ ] ", "* [x] "])
def test_listed_unrelated_fields_refuse_before_github(cli, priority, prefix):
    _, run = cli
    code, report, after = run("--priority", priority, "--apply",
                              body="Reproduction: TBD\n" + prefix + "Expected behavior: create a native child")
    assert (code, report.get("reason"), after["calls"], after["created"]) == (
        2, "high_priority_requires_reproduction_or_measurement", [], [])


@pytest.mark.parametrize("prefix", ["- ", "* ", "+ ", "1. ", "2) ", "- [ ] ", "* [x] "])
def test_listed_evidence_labels_allow_apply(cli, prefix):
    _, run = cli
    code, report, after = run("--priority", "P1", "--apply",
                              body=prefix + "Reproduction: run false and observe exit 1")
    assert (code, report.get("status"), len(after["created"])) == (0, "created", 1)


@pytest.mark.parametrize("fence", ["```", "~~~"])
def test_listed_fields_inside_fences_remain_literal_evidence(cli, fence):
    _, run = cli
    code, report, after = run("--priority", "P1", "--apply",
                              body="Reproduction:\n" + fence + "text\n- Expected behavior: literal test input\n" + fence)
    assert (code, report.get("status"), len(after["created"])) == (0, "created", 1)
