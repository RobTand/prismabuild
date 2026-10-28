# PrismaBuild agent instructions

PrismaBuild admission is for computational work by agents and subagents:
GPU jobs, benchmarks, and large or long-running compute. Use the published
`pbrun.py`, `pbtest.py` or `pbcampaign.py` entrypoints for that work.
PrismaBuild is not the CI system. CI and integration tests run as GitHub checks
in the Mergify merge queue on the integrated SHA.
While developing, run the fast relevant tests in your worktree;
a local run is evidence, not the merge gate. Merge only through the Mergify
queue; never bypass required checks, reviews or branch protections.
Read [the execution policy](docs/agent_execution_policy.md) and
[the operating guide](docs/operating_prismabuild.md) before running work.
Declare aggregate CPU, memory and GPU demand, bound native threads per worker,
and verify terminal records, logs and CAS receipts. Already-admitted action
children execute directly; do not submit recursively. Read-only inspection,
source edits and Git operations may run on the coordinator. If the fleet is
unavailable, repair it; do not silently fall back to local tests or GPU work.
PrismaBuild owns placement. Its bounded best-effort preferences
`deferred_for_preferred_cpu`, `deferred_for_cross_resource_placement`, and
`deferred_for_idle_gpu_host` expire after at most 20 seconds. Do not retag or
resubmit to defeat them. Idle GPUs give restartable portable CPU work first
claim on the Sparks' fast CPUs (#1929). Idle requires no PB GPU reservation,
fresh trusted GPU evidence with no foreign process, including exempt vLLM,
and no eligible READY GPU row (#1526). A denied GPU row whose pool drain would
resolve the refusal evicts CPU guests through ordinary preemption, lowest
priority then most recently started, with normal cleanup and retry lineage.
Pinned CPU rows are never guests; ordinary #364 preemption still applies. CPU
pbtest shards seal interruption-only retry safety and three launches: worker
failures end immediately; interruptions and lease loss may requeue. After two
interruptions, a portable retry waits only while a live matching CPU-only host
has not passed on it; otherwise it may run as a non-guest. Nonrestartable rows
retain #1262.
While a GPU is in use or GPU work is pending, `deferred_for_cpu_only_host`
remains an untimed rule leaving a CPU row to a matching CPU-only host that
fits it now, until that host claims it or passes on it. `deferred_for_ready_gpu_row` also preserves the room a READY GPU
row needs during a pass that cannot evaluate it (#1169). Leave portable work
untagged; tags declare real architecture or host requirements.

Everything vLLM is exempt, universally (Rob, 2026-09-07): a serve, a census, a
routing run, a benchmark against a live endpoint, its GPU containers. Run it
directly, without submission. It is a ruling, not a claim about what the fleet
could technically admit, so do not argue it from a wrapper's capability. A
running vLLM is still external load for admission, so account for it when
declaring demand. Non-vLLM computational work in the admission scope above
still submits; fast local development tests follow the CI routing above.

Exception — Rob, 2026-09-07: vLLM is completely and totally exempt from
PrismaBuild and GPU admission constraints. This includes serving, tests,
censuses, routing runs, benchmarks, and GPU containers that run vLLM. Run them
directly without PrismaBuild submission or bootstrap approval. Non-vLLM GPU
jobs, benchmarks, and large or long-running compute still use PrismaBuild.
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
declaring completion each needs its scoped acceptance record — see the
[acceptance-record template](docs/staged_read_acceptance_record_template_2026-09-20.md)
for the distinct prelaunch/merge/deploy/complete fields — with actual
evidence (action keys, receipts, refusals); reasoned exceptions cite
explicit user authority, never agent waiver. No waiver of the current
staged-only policy without user authority. Spec completion is not
implementation satisfaction.


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

## Test quality — Rob, 2026-10-24 (#2249)

"Please fix the flaky tests and create guidance that no longer creates them.
Flaky tests derail this entire platform." Every test you add or change must
be deterministic and able to fail.

1. Deterministic.
   - No wall-clock or elapsed-time thresholds. Inject or fake clocks instead.
   - No sleeping to wait for a condition. Synchronize on explicit events;
     timeouts are only generous failure bounds.
   - No dependence on host load, CPU count, scheduling order, inode or mtime
     reuse, listing order or test order.
   - Seed randomness, make no network calls and use `tmp_path`.
   - Fit the declared CPU and memory under xdist, and pass identically when
     rerun or run in parallel.
2. Not tautological. A test must be able to fail when the behavior is wrong.
   Don't write self-comparisons, constant asserts, asserts on a mock's own
   configured return, expectations recomputed with the code under test, or
   "called with what I passed". Remove tautological tests you find.

Fix a flaky test at its root. Retries, quarantine, xfail, skips and widened
timeouts are not fixes. The method is in [the contributing guide](docs/contributing.md).

## Code design principles — Rob, 2026-09-30

When two principles collide, pick the one that cuts future cost in THIS codebase.

HARD RULE: refactor to the principle FIRST, then change behavior.

1. Separation of Concerns — one kind of work per part (UI / domain / persistence / infra). Root principle.
2. Encapsulation / Information Hiding — small stable contract; hide internals.
3. High Cohesion + Loose Coupling — change-together lives together; independents talk narrow.
4. DRY — one authoritative representation of each piece of *knowledge* (not every similar line). Avoid over-DRY.
5. KISS — simplest design that works; complexity is the long-term tax.
6. Single Responsibility — one reason to change.
7. Depend on Abstractions — policy doesn't depend on details; both depend on contracts.
8. YAGNI — no speculative features, frameworks, or "later" hooks.
9. Composition over Inheritance — assemble pieces; don't grow fragile hierarchies.
10. Open/Closed (with discipline) — extend at stable boundaries; only where change showed up twice.

Honorable: Law of Demeter · fail fast / illegal states unrepresentable · optimize for deletion · Unix do-one-thing + compose.

Treat as constraints. Violate slogans when judgment says so.
