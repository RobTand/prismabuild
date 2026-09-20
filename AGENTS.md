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
PrismaBuild owns placement, including its bounded best-effort preferences ---
`deferred_for_preferred_cpu` and `deferred_for_cross_resource_placement`, which
prefers not to spend a busy box's GPU power on work that can go elsewhere. A
deferral expires by itself; do not read one as a refusal, and do not retag or
resubmit to defeat it.

Everything vLLM is exempt, universally (Rob, 2026-09-07): a serve, a census, a
routing run, a benchmark against a live endpoint, its GPU containers. Run it
directly, without submission. It is a ruling, not a claim about what the fleet
could technically admit, so do not argue it from a wrapper's capability. A
running vLLM is still external load for admission, so account for it when
declaring demand. Work that does not run vLLM still submits.

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

Changes to `main` require a GitHub issue and pull request (Rob, 2026-09-06).
Use an isolated branch/worktree, link the issue in the PR, validate and review
before merging. Never push directly to `main`. See [contributing](docs/contributing.md).

## Staged-read contract — Rob, 2026-09-20

Before changing lifecycle, residency, or dev-identity behavior, or claiming
campaign completion, read [the staged-read contract](docs/staged_read_contract_2026-09-20.md)
and its [requirement ledger](docs/staged_read_requirements_2026-09-20.json).
State target versus implemented/deployed per requirement — a `proposed`
target is owed work, never a claim. Merging, launching, deploying, or
declaring completion each needs its scoped acceptance record from the
contract's checklist (§10) with actual evidence (action keys, receipts,
refusals); reasoned exceptions cite explicit user authority, never agent
waiver. Spec completion is not implementation satisfaction.


## Progress and termination — Rob, 2026-09-10 (#480)

For long campaign work with supported semantic reporting, declare ordered
progress phases and workload-based stall allowances instead of an unexplained
whole-run timeout. Report cumulative committed units only after durable work;
logs, heartbeats and CPU/GPU activity do not establish advancement. Preserve
explicitly requested hard deadlines, withdrawal, containment and cleanup.
Emit committed units from any loop that runs longer than a few minutes: the
channel is in the action's environment, so a shell loop, a foreign interpreter
and a container all report through the same helper rather than a copy of the
record (#488). Use the published execution policy and skill: progress
submission requires a runtime advertising both `progress-v1` and
`progress-helper-v1` on the pool transport. A source merge alone
does not establish deployed support. Never change an existing sealed request
to extend it; use supported recovery and its identity-bound checkpoints.

## Work decomposition — Rob, 2026-09-11

Decompose declared logical work into the smallest useful independent execution
units before those units are published. A logical parent may first be queued
for a PB decomposition stage. PB owns decomposition, placement and balancing;
producers declare tasks, real dependencies, residency needs and measured cost
inputs instead of assigning hosts or choosing fleet shard counts.

Validate and freeze the complete child plan before publishing its first unit.
Once published, an execution unit's scope and task membership stay immutable,
whether ready or running. Availability changes which worker takes a unit, not
what the unit contains. Retry the same unit and adopt its verified durable
results; do not split or resize queued/running units. Amortize startup and
resident input preparation within useful units without hiding a long independent
worklist in one opaque action. Report an unsupported decomposition explicitly.

The agreed implementation design is
[PB #517](docs/design_work_decomposition_2026-09-11.md); PR #518 implements its synchronous MVP. Durable queued parents and parent
withdrawal remain proposed; source support does not establish deployed support.
