"""Run inside one PB action. Every mutant gets a private source copy."""
import ast
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
CASES += [
    ('empty-normalized-titles-match', 'duplicates.py', 'return bool(left and right and (', 'return bool((', 'test_fuzzy_matching_preserves_census_threshold_and_short_title_rule'),
    ('drop-canonical-priority', 'intake.py', 'f"[{args.priority}] {title}", body,', 'title, body,', 'test_absent_priority_label_does_not_invent_a_label'),
    ('wrong-refusal-reason', 'intake.py', '"reason": reason, **details', '"reason": "wrong_reason", **details', ''),
    ('wrong-duplicate-reference', 'intake.py', 'duplicates=duplicates', 'duplicates=["https://example.invalid/duplicate"]', 'test_duplicate_on_second_page_of_another_repo_refuses_without_writes'),
    ('wrong-report-status', 'intake.py', '    print(json.dumps(report, indent=2, sort_keys=True))', '    report["status"] = "wrong_status"\n    print(json.dumps(report, indent=2, sort_keys=True))', ''),
    ('wrong-process-exit', 'intake.py', '    return code', '    return 79', ''),
    ('hidden-dry-run-write', 'intake.py', '    return {"status": "dry_run",', '    github.create(payload)\n    return {"status": "dry_run",', 'test_dry_run_checks_every_repo_and_never_creates'),
    ('double-create', 'intake.py', '        return github.create(payload)', '        github.create(payload)\n        return github.create(payload)', 'test_apply_creates_a_native_child_with_canonical_priority_and_existing_labels'),
    ('read-before-validation', 'intake.py', '        parent = IssueRef.parse(args.parent, args.repo)', '        GitHub().open_issues()\n        parent = IssueRef.parse(args.parent, args.repo)', ''),
    ('create-before-listing', 'intake.py', '    issues = github.open_issues()', '    if apply:\n        github.create(github.prepare(ticket))\n    issues = github.open_issues()', ''),
    ('create-before-parent-and-label-validation', 'intake.py', '        if parent["state"] != "open" or "pull_request" in parent:', '        self.create({"repositoryId": repo["node_id"], "parentIssueId": parent["node_id"], "title": ticket.title, "body": ticket.body, "labelIds": []})\n        if parent["state"] != "open" or "pull_request" in parent:', ''),
    ('list-scope-normalization-disabled', 'intake.py', '        line = LIST_PREFIX.sub("", line)', '        line = line', 'test_listed_unrelated_fields_refuse_before_github'),
    ('reject-new-valid-evidence', 'intake.py', 'and not has_evidence(ticket.body):', 'and True:', ''),
]


def instrument_assertions(text):
    """Evaluate every original assertion, then fail if any failed."""
    module = ast.parse(text)
    sites = {}
    for function in module.body:
        if not isinstance(function, ast.FunctionDef) or not function.name.startswith('test_'):
            continue
        class Assertions(ast.NodeTransformer):
            def visit_Assert(self, node):
                key = f'{function.name}:{node.lineno}'
                sites[key] = ast.get_source_segment(text, node)
                failure = ast.parse(f'print("INTAKE_ASSERTION_FAILED:" + {key!r}, flush=True)\n_negative_control_failures.append({key!r})').body
                unavailable = ast.parse(f'print("INTAKE_ASSERTION_UNAVAILABLE:" + {key!r}, flush=True)\n_negative_control_unavailable.append({key!r})').body
                handlers = [ast.ExceptHandler(type=ast.Name(id='AssertionError', ctx=ast.Load()), name=None, body=failure),
                            ast.ExceptHandler(type=ast.Name(id='Exception', ctx=ast.Load()), name=None, body=unavailable)]
                return ast.copy_location(ast.Try(body=[node], handlers=handlers, orelse=[], finalbody=[]), node)
        function = Assertions().visit(function)
        function.body.insert(0, ast.parse('_negative_control_failures, _negative_control_unavailable = [], []').body[0])
        function.body.append(ast.parse('assert not _negative_control_failures and not _negative_control_unavailable, (_negative_control_failures, _negative_control_unavailable)').body[0])
    return ast.unparse(ast.fix_missing_locations(module)) + '\n', sites


instrumented, assertion_sites = instrument_assertions((SOURCE / 'test_intake.py').read_text())
results = []
coverage = {}
for name, filename, before, after, test in [('unmutated-control', None, None, None, ''), *CASES]:
    with tempfile.TemporaryDirectory(prefix='pri5-mutant-') as directory:
        copy = Path(directory) / 'backlog_census'
        shutil.copytree(SOURCE, copy, ignore=shutil.ignore_patterns('__pycache__', '.pytest_cache'))
        (copy / 'test_intake.py').write_text(instrumented)
        if filename is not None:
            path = copy / filename
            text = path.read_text()
            if text.count(before) != 1:
                raise RuntimeError(f'{name}: mutation target count {text.count(before)}')
            path.write_text(text.replace(before, after))
        target = str(copy / 'test_intake.py') + ('::' + test if test else '')
        result = subprocess.run([sys.executable, '-m', 'pytest', '-q', '-s', '--tb=short', target],
                                env={**os.environ, 'PYTHONPATH': str(copy)}, text=True, capture_output=True, timeout=90)
        failed_sites = sorted(set(re.findall(r'INTAKE_ASSERTION_FAILED:(test_\w+:\d+)', result.stdout)))
        unexpected = re.findall(r'^E\s+([\w.]+(?:Error|Exception))(?::|$)', result.stdout, re.M)
        only_assertions = all(error == 'AssertionError' for error in unexpected)
        killed = result.returncode == 1 and bool(failed_sites) and only_assertions and 'ERROR ' not in result.stdout
        passed = result.returncode == 0 and not failed_sites if filename is None else killed
        row = {'mutant': name, 'test': test or '*', 'exit_code': result.returncode,
               'killed_by_assertion': killed, 'failed_assertions': failed_sites,
               'unavailable_assertions': sorted(set(re.findall(r'INTAKE_ASSERTION_UNAVAILABLE:(test_\w+:\d+)', result.stdout)))}
        results.append(row)
        print(json.dumps(row, sort_keys=True), flush=True)
        print(result.stdout, result.stderr, flush=True)
        if not passed:
            raise SystemExit(1)
        for site in failed_sites:
            coverage.setdefault(site, []).append(name)
missing = sorted(assertion_sites.keys() - coverage.keys())
print(json.dumps({'assertion_coverage': {site: {'assertion': text, 'mutants': coverage.get(site, [])}
                                         for site, text in assertion_sites.items()},
                  'missing_assertions': missing}, sort_keys=True), flush=True)
if missing:
    raise SystemExit(1)
print(json.dumps({'mutants': len(CASES), 'killed': len(CASES),
                  'assertion_sites': len(assertion_sites), 'covered_assertion_sites': len(coverage)}, sort_keys=True))
