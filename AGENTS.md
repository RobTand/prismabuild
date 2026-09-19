# PrismaBuild agent instructions

All tests and batch GPU work by agents and subagents must run through PrismaBuild,
using the published `pbrun.py`, `pbtest.py` or `pbcampaign.py` entrypoints.
Read [the execution policy](docs/agent_execution_policy.md) and
[the operating guide](docs/operating_prismabuild.md) before running work.
Declare aggregate CPU, memory and GPU demand, bound native threads per worker,
and verify terminal records, logs and CAS receipts. Already-admitted action
children execute directly; do not submit recursively. Read-only inspection,
source edits and Git operations may run on the coordinator. If the fleet is
unavailable, repair it; do not silently fall back to local tests or GPU work.

Exception — Rob, 2026-09-07: vLLM is completely and totally exempt from
PrismaBuild and GPU admission constraints. This includes serving, tests,
censuses, routing runs, benchmarks, and GPU containers that run vLLM. Run them
directly without PrismaBuild submission or bootstrap approval. Non-vLLM tests,
benchmarks, exports, probes and other batch GPU work still use PrismaBuild.
vLLM resource use remains external load when admitting other batch work.
This instruction supersedes the earlier serving-only exception.

Read `docs/design.md` before changing execution, identity or queue contracts;
update it with contract/default changes. Preserve existing worktrees and dirty
work. Reproduce behavioral bugs before fixing them and validate the integrated
result. Delegate substantial independent work when the user authorizes it;
match effort to difficulty and verify returned artifacts independently.
