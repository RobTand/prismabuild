# Regression-test check

The optional `regression-test` check rejects fix PRs without changed Python test
behavior. It runs after this workflow reaches the default branch. Its own
introducing PR is bootstrap evidence, not one of the five rollout observations.
The check does not change branch protection.

Every PR must have exactly one `Change type:` line in its body. Supported values
are `fix`, `bugfix`, `feature`, `docs`, `refactor`, `test`, `chore`, `perf`,
`build`, `ci` and `revert`. Missing, invalid or repeated declarations reject the
PR, including PRs whose titles do not look like fixes. This avoids relying on
rarely used labels or title conventions. Use `Change type: fix` for a bug repair.
Release-train PRs from `tl/release-*` branches pass without a declaration. They
only merge member PRs that this check already judged. `issue-link` exempts the
same branches from its branch-name rule.

A fix also has a title starting with `fix`, `fix(scope):`, `repair`, `bugfix` or
`regression`, after optional `[priority]` prefixes. The labels `bug`, `fix`,
`bugfix`, `regression`, `type:bug` and `type:fix` also classify it. An explicit
`Change type: fix` or `Change type: bugfix` line does too. These signals override
a non-fix declaration. Closing an issue with
`Fixes #123` alone does not classify a feature PR as a fix.

A qualifying change adds or changes a `test_*` function in a Python test file
under `tests/`. Test classes start with `Test` or inherit a `TestCase` base.
The function must contain `assert`, a unittest assertion, `pytest.raises`,
`pytest.warns` or `pytest.fail`, or directly call a same-file module function
containing one of those checks. Helper lookup follows one call only.
AST comparison ignores comments, whitespace and
docstrings. Changes only to fixtures, helpers, deleted tests or unchanged
renamed files do not qualify. Parametrization changes on a qualifying test do.

This is a presence check. The Principal Engineer still checks that the test
reproduces the issue and passes with the fix, using linked PrismaBuild evidence.
Imported helpers, longer helper chains and tests in other languages need a small
supported detector before they can satisfy this check. Do not add a meaningless
assertion or bypass label. Record any false rejection during the optional rollout.

The workflow uses `pull_request_target` with a read-only token and a pinned
`github-script` action. It fetches the evaluator from the base SHA, reads PR
blobs as data, and never checks out or executes PR code. It paginates the PR file
list and rejects incomplete lists or PRs beyond GitHub's 3000-file limit. Whole
before/after sources come from immutable commit refs, so an absent or truncated
API patch cannot hide a test change. Unreadable sources, files over 1 MiB and
Python syntax errors fail the check. See GitHub's
[pull-request file API](https://docs.github.com/en/rest/pulls/pulls#list-pull-requests-files)
and [target-workflow security guide](https://docs.github.com/en/actions/reference/security/securely-using-pull_request_target).

To replay a saved API input inside an admitted PrismaBuild action, run
`python3 tools/ci/check_regression_test.py /path/to/input.json`. The input has
`pull` metadata and the complete `files` list. Candidate test files include
`before` and `after` text. Exit 0 accepts, exit 1 rejects missing coverage, and
exit 2 rejects invalid input. The JSON verdict names qualifying tests.

Keep an observation table on PRI-8 with PR URL, head SHA, workflow run URL,
classification, result, reviewer verdict and false-rejection status. After at
least five real PRs classified as fixes run without a false rejection, the authorized owner can add
`regression-test` to branch protection while preserving `issue-link` and other
existing requirements. Record the actual protection response and time on PRI-8.
A dry run, synthetic fixture or planned protection change does not count.

Before/after comparison includes removed test files and compares test bodies
across the changed files. Moving an unchanged test between those files does not
qualify as regression evidence.
