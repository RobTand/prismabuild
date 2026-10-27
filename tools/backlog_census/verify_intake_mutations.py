"""Run inside one PB action. Every mutant gets a private source copy."""
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

SOURCE = Path.cwd() / 'tools/backlog_census'
CASES = [
    ('dry-run-publishes', 'intake.py', 'if apply:', 'if True:', 'test_dry_run_checks_every_repo_and_never_creates'),
    ('skip-two-repos', 'intake.py', 'for repo in REPOS:', 'for repo in REPOS[:1]:', 'test_dry_run_checks_every_repo_and_never_creates'),
    ('corrupt-create-payload', 'intake.py', '"title": ticket.title', '"title": "corrupted title"', 'test_apply_creates_a_native_child_with_canonical_priority_and_existing_labels'),
    ('duplicate-gate-disabled', 'intake.py', 'if duplicates:', 'if False:', 'test_duplicate_on_second_page_of_another_repo_refuses_without_writes'),
    ('pull-requests-are-issues', 'intake.py', 'if "pull_request" not in row:', 'if True:', 'test_pull_requests_are_not_duplicate_issues'),
    ('priority-gate-disabled', 'intake.py', 'if ticket.priority in {"P0", "P1"} and not has_evidence(ticket.body):', 'if False:', 'test_high_priority_without_evidence_refuses_before_network'),
    ('reject-valid-evidence', 'intake.py', 'and not has_evidence(ticket.body):', 'and True:', 'test_supplied_reproduction_or_measurement_allows_high_priority'),
    ('accept-empty-evidence', 'intake.py', '    return False\n\n\nclass GitHub:', '    return True\n\n\nclass GitHub:', 'test_empty_or_placeholder_sections_are_not_evidence'),
    ('disable-fuzzy-match', 'duplicates.py', 'SequenceMatcher(None, left, right).ratio() >= .92', 'False', 'test_fuzzy_matching_preserves_census_threshold_and_short_title_rule'),
    ('match-short-titles', 'duplicates.py', 'len(left) >= 30 and len(right) >= 30 and\n                          SequenceMatcher(None, left, right).ratio() >= .92', 'SequenceMatcher(None, left, right).ratio() >= .5', 'test_fuzzy_matching_preserves_census_threshold_and_short_title_rule'),
    ('pagination-fails-open', 'intake.py', '    issues = github.open_issues()', '    try:\n        issues = github.open_issues()\n    except GitHubError:\n        issues = []', 'test_late_pagination_failure_refuses_creation'),
    ('allow-closed-parent-and-pr', 'intake.py', 'if parent["state"] != "open" or "pull_request" in parent:', 'if False:', 'test_parent_must_be_an_open_issue'),
    ('allow-conflicting-prefix', 'intake.py', 'if prefix and prefix[1] != args.priority:', 'if False:', 'test_invalid_input_never_contacts_github'),
    ('allow-double-prefix', 'intake.py', 'if not normalize_title(title) or PREFIX.match(title):', 'if not normalize_title(title):', 'test_invalid_input_never_contacts_github'),
    ('allow-parent-zero', 'intake.py', '([1-9]\\d*)', '(\\d+)', 'test_invalid_input_never_contacts_github'),
    ('allow-foreign-parent', 'intake.py', 'if parent_repo not in REPOS:', 'if False:', 'test_invalid_input_never_contacts_github'),
    ('allow-empty-title', 'intake.py', 'if not normalize_title(title) or PREFIX.match(title):', 'if PREFIX.match(title):', 'test_invalid_input_never_contacts_github'),
    ('double-matching-prefix', 'intake.py', 'f"[{args.priority}] {title}", body,', 'f"[{args.priority}] [{args.priority}] {title}", body,', 'test_matching_title_prefix_is_not_doubled'),
    ('resolve-parent-in-wrong-repo', 'intake.py', '{ticket.parent.repo}/issues/{ticket.parent.number}', '{ticket.repo}/issues/{ticket.parent.number}', 'test_cross_repo_parent_is_resolved_in_its_repository'),
    ('allow-conflicting-label', 'intake.py', 'if conflicting:', 'if False:', 'test_invalid_labels_refuse_before_creation'),
    ('allow-unknown-label', 'intake.py', 'raise Refused("unknown_labels", labels=sorted(missing))', 'return {}', 'test_invalid_labels_refuse_before_creation'),
    ('invent-missing-priority-label', 'intake.py', 'if ticket.priority in labels:', 'if True:', 'test_absent_priority_label_does_not_invent_a_label'),
    ('graphql-error-becomes-success', 'intake.py', 'raise GitHubError(f"GitHub GraphQL errors: {response[\'errors\']}")', 'return {"data": {"createIssue": {"issue": {"parent": {"id": "parent-40"}, "number": 41, "url": "fake"}}}}', 'test_graphql_error_is_not_reported_as_a_creation'),
    ('dry-run-examples-bypass-refusals', 'intake.py', 'def submit(ticket: Ticket, github: GitHub, *, apply=False) -> dict:', 'def submit(ticket: Ticket, github: GitHub, *, apply=False) -> dict:\n    return {"status": "dry_run"}', 'test_requested_dry_run_examples'),
]
results = []
for name, filename, before, after, test in CASES:
    with tempfile.TemporaryDirectory(prefix='pri5-mutant-') as directory:
        copy = Path(directory) / 'backlog_census'
        shutil.copytree(SOURCE, copy, ignore=shutil.ignore_patterns('__pycache__', '.pytest_cache'))
        path = copy / filename
        text = path.read_text()
        if text.count(before) != 1:
            raise RuntimeError(f'{name}: mutation target count {text.count(before)}')
        path.write_text(text.replace(before, after))
        result = subprocess.run([sys.executable, '-m', 'pytest', '-q', '--tb=short', str(copy / 'test_intake.py') + '::' + test],
                                env={**os.environ, 'PYTHONPATH': str(copy)}, text=True, capture_output=True, timeout=45)
        killed = result.returncode == 1 and re.search(r'^E\s+(?:assert |AssertionError:)', result.stdout, re.M) is not None and 'ERROR ' not in result.stdout
        row = {'mutant': name, 'test': test, 'exit_code': result.returncode, 'killed_by_assertion': killed}
        results.append(row)
        print(json.dumps(row, sort_keys=True), flush=True)
        if not killed:
            print(result.stdout, result.stderr, flush=True)
            raise SystemExit(1)
print(json.dumps({'mutants': len(results), 'killed': len(results), 'test_functions': len({r['test'] for r in results})}, sort_keys=True))
