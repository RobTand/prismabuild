# Tautological test guard

[Issue #1803](https://github.com/RobTand/prismabuild/issues/1803) enforces
Rob's D-32 rule against mechanical tautologies in every test under `tests/`.
The gate is `tests/test_no_tautological_tests.py`. It reports every finding
as `path:line: test: rule`, without suppressions or an allowlist.

`src/prismabuild/tautology.py` is an unchanged copy of
[PrismaGraph's scanner at b905a80d](https://github.com/RobTand/prismagraph/blob/b905a80d838bb3d743be29889f54e8567bbc12cf/prismagraph/tautology.py).
Its upstream Git blob is `3f2c21d0ad75fe7f8c67bf375a857d5b2a552f76`.
The source contract is
[PG-UPLIFT-001-SB156](https://github.com/RobTand/prismagraph/blob/0f47031b1a112f65383cbce10797e47d1b7078e6/docs/prismagraph/bootstrap/contracts/SB156-static-tautology-guard.md).

The scanner detects self-comparisons, constant assertions, and assertions
swallowed by inert exception handlers. It parses source without executing it.
Semantic tautologies outside those rules still need review.

`tests/test_tautology_guard.py` plants all three rules in a scratch test tree
and calls the real suite guard. It checks the complete failure message,
including each file, line, test name, and rule.

Run both test files through the published PrismaBuild client as required by
[the execution policy](agent_execution_policy.md).
