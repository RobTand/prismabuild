# PrismaBuild agent instructions

All tests and GPU work by agents and subagents must run through PrismaBuild,
using the published `pbrun.py`, `pbtest.py` or `pbcampaign.py` entrypoints.
Read [the execution policy](docs/agent_execution_policy.md) and
[the operating guide](docs/operating_prismabuild.md) before running work.
Declare aggregate CPU, memory and GPU demand, bound native threads per worker,
and verify terminal records, logs and CAS receipts. Already-admitted action
children execute directly; do not submit recursively. Read-only inspection,
source edits and Git operations may run on the coordinator. If the fleet is
unavailable, repair it; do not silently fall back to local tests or GPU work.

Read `docs/design.md` before changing execution, identity or queue contracts;
update it with contract/default changes. Preserve existing worktrees and dirty
work. Reproduce behavioral bugs before fixing them and validate the integrated
result. Delegate substantial independent work when the user authorizes it;
match effort to difficulty and verify returned artifacts independently.

Changes to `main` require a GitHub issue and pull request (Rob, 2026-09-06).
Use an isolated branch/worktree, link the issue in the PR, validate and review
before merging. Never push directly to `main`. See [contributing](docs/contributing.md).
