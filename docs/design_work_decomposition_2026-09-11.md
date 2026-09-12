# PB #517: decompose logical requests before execution

Design agreed by Rob on 2026-09-11; implementation assigned to Claude.
This document specifies proposed behavior. No decomposer or new worker
capability is implemented by this documentation change.

Issue: https://github.com/RobTand/prismabuild/issues/517.
Source basis: `origin/main` at `508f4e3fef` (merged PR #516).

## Recommendation

Introduce an immutable **logical parent request** and a PB-owned decomposition
stage. Submission queues the parent. Before any execution child is published, the
decomposer validates its complete logical task roster, derives the smallest
useful batches from declared cost estimates, freezes one immutable child plan, and then publishes
ordinary sealed `pbrun` child actions from that plan. Existing queue placement,
claiming, resource admission, retry, withdrawal, leases, broker scopes, CAS and
cleanup carry the children unchanged.

The boundary is strict:

```text
submit immutable parent
  -> PB decomposition claim
  -> freeze immutable exact-cover child plan
  -> publish all planned ordinary child actions idempotently
  -> existing PB admission/execution
  -> exact-cover group receipt
```

No execution child changes task membership after publication, including while it
is merely ready. No running unit splits, spills, steals or re-enqueues a subset.
Retries rerun the same sealed child or adopt its verified durable result. A GPU
that becomes free later takes another already-published useful child through the
existing scheduler.

This does not reinterpret the opaque action already running in issue #517. Only a
new parent request that declares the roster and consumer protocol is decomposable.

## Why this fits the current architecture

Current `pbcampaign` validates a list and submits independent whole argv rows
through `pbrun` (`tools/fleet/pbcampaign.py:399-557`). Its all-rows validation,
row conversion, detached submissions and wait table are the natural client-side
entry point, but PB must own the decomposition record and algorithm.

`pbrun` already seals complete ordinary action identity: checkout/source, code
closure, command, environment/toolchain, placement, resources, retry/progress/
profile policy and data inputs (`tools/fleet/pbrun.py:4120-4381`). Extract that
construction into a callable used by the decomposer. Do not implement a second
action-key function in `pbcampaign`.

`PoolQueue.publish` already publishes one sealed action and resource/placement
contract (`src/prismabuild/pool.py:2751-2897`). The ready-to-claimed rename is
the unique execution claim (`src/prismabuild/pool.py:4311` onward). Exact claim
identity, attempt generation, lease, resource scope, transition lock, reaper,
finish and withdrawal already provide child ownership and recovery.

`--data-manifest` is reusable for common resident input preparation. It is a
content-addressed action input sealed in params (`tools/fleet/pbrun.py:3863-3870,
4303-4368`). Every child carries the required common data manifest and summary, plus its exact batch input, so the
existing storage-role prewarm path stays authoritative. GPU setup is amortized
within each useful multi-task child; this must not become one cold process per
rate when setup dominates.

## Minimal API

A new `pbcampaign` parent manifest can look like:

```json
{
  "schema": "prismabuild.logical_request.v1",
  "common": {
    "argv": ["python", "collect.py", "--pb-task-batch",
             "{pb.task_batch}"],
    "cwd": "/checkout",
    "demand": {"gpu": 1, "cpu": 4, "mem_gb": 72},
    "gpu_memory_gb": 72,
    "data_manifest": "/inputs/data-manifest.json",
    "env": {"OMP_NUM_THREADS": "1"}
  },
  "roster": {
    "schema": "prismabuild.logical_task_roster.v1",
    "tasks": [
      {"id": "family=e4m3/rate=833",
       "payload": {"family": "e4m3", "rate": 833},
       "residency_key": "glm-l20-e4m3-calibration-v1",
       "estimated_seconds": 8.2,
       "estimate_evidence": "cas:sha256:...",
       "output_id": "family=e4m3/rate=833"}
    ]
  },
  "batch_policy": {
    "schema": "prismabuild.roster_batch_policy.v1",
    "residencies": [{
      "key": "glm-l20-e4m3-calibration-v1",
      "setup_seconds": 45.0,
      "setup_evidence": "cas:sha256:..."
    }],
    "max_setup_fraction": 0.20,
    "max_estimated_wall_seconds": 300.0
  }
}
```

The sample shows one task from a longer roster; it is not a complete executable
request. The numbers illustrate shape and are not defaults. The producer declares
logical tasks, stable output IDs and measured cost inputs.
It does not declare hosts, worker count, shard count or a task-to-worker mapping.
PB owns a versioned deterministic batcher. A simpler v1 may accept a directly
measured `min_batch_work_s`; either form must record estimate provenance and seal
every value that affects partitioning.

For each contiguous run of one `residency_key`, a batch is feasible only when:

```text
setup_seconds / (setup_seconds + sum(task.estimated_seconds))
    <= max_setup_fraction

setup_seconds + sum(task.estimated_seconds)
    <= max_estimated_wall_seconds
```

PB computes a deterministic contiguous exact-cover partition and chooses the
largest feasible number of batches (the smallest sensible units), breaking ties
by minimum maximum estimated wall time and then lexicographic cut positions. It
never mixes residency keys or incompatible common execution/resource contracts.
This can be a small dynamic program rather than a greedy packer, because a greedy
last remainder can be infeasible even when a rebalanced exact partition exists.

No numeric default should be invented in v1. Both limits and their evidence are
required. If one task alone exceeds the wall bound, or the setup-fraction lower
bound and wall upper bound admit no exact-cover partition, decomposition refuses
before publishing the plan or any child. The refusal names the residency run,
task range, minimum useful-work requirement and wall ceiling. It never falls back
to one opaque oversized child or silently changes a limit.

The algorithm version and exact output plan are sealed. Live offer count does not
alter the plan: queue enough smallest sensible units and let ordinary admission
distribute them as capacity changes. Cost estimates are hash-bound planning hints,
not measured speed claims. Qualification still measures the resulting execution.

The producer adapter must explicitly consume a canonical batch file. The
illustrative `{pb.task_batch}` is a reserved whole-argument placeholder: PB
replaces it with the immutable input's path before ordinary action sealing.
It is not shell expansion, string interpolation over arbitrary argv, or a
variable the user can override. A worker-side materialization mechanism is
also possible, provided its capability is versioned and fails closed. Each
batch file holds parent and plan keys, child ordinal and exact ordered tasks.
Opaque commands without this declared input protocol remain ordinary actions;
PB does not infer a decomposition from their command text.

## Identity and frozen-plan semantics

```text
parent_key = H(common execution semantics,
               immutable canonical roster,
               versioned batch policy)

plan_key = H(parent_key, algorithm version,
             ordered exact-cover lists of task IDs)

child action key = ordinary pbrun key with:
  inputs += [roster input, exact child-batch input]
  params.logical_batch = {
    schema, parent_key, plan_key, roster_sha256,
    batch_policy_sha256, child_ordinal, ordered_task_ids
  }
```

Avoid cyclic hashes: first hash the plan blueprint containing task partitions;
then derive batch-input envelopes that reference that plan key; then seal the
ordinary child actions. An immutable publication index binds the blueprint,
batch-input digests and exact child action keys. Freeze that entire index before
publishing any child. The blueprint never includes a child digest that itself
contains the blueprint's hash.

Freeze the common source/action template once. Later decomposition recovery or
child publication must not snapshot a mutable checkout again. Extract and reuse
`pbrun`'s ordinary sealer rather than cloning its hashing rules.

For the PrismaQuant consumer, the common domain identity remains the original
full-band campaign identity; PB's parent request additionally binds scheduling
policy and generic execution semantics. Each child action key binds one
exact subset. Source, calibration/data bytes, recipe, environment/toolchain,
resources, placement, roster task/payload, policy or membership changes the
appropriate key. Host choice, queue timing and claiming worker do not.

The decomposer must freeze the whole plan before publishing the first child. Its
transaction is:

1. Atomically claim the parent decomposition request. Bounded queue metadata
   handling is control-plane work. A substantial planning computation runs as
   an admitted CPU action and returns a plan; PB's coordinator publishes its
   children afterward. An admitted planner must not recursively submit work.
2. Validate/canonicalize the full roster and common spec, derive every batch, and
   prove nonempty disjoint exact cover.
3. First-writer-publish immutable plan bytes under `parent_key`. If a plan already
   exists, verify it and reuse it; never derive and replace it.
4. Idempotently seal and publish exactly the child keys named by the plan. A crash
   after any subset of ready links resumes from the same plan and fills missing
   links. It cannot repartition on retry.
5. Mark parent decomposition complete only after every planned child is either
   already receipted, ready, claimed or terminal under the same publication
   generation. Partial publication remains visible and recoverable.

A queued logical parent holds no execution resources merely by existing.
An admitted CPU planner has its own normal resource reservation. Parent or
planning-stage ownership cannot release child resources or substitute for their
attempt leases. For an MVP, this stage may execute synchronously inside initial
`pbcampaign` submission as long as the immutable parent/plan records and crash
recovery semantics are identical. The durable queued parent is preferable because
it makes decomposition observable and recoverable without keeping the submitter
alive.

## Outputs and exact merge

Children never append to one shared full-band file or journal. Each gets a
namespace derived from `parent_key` and child action key and writes an
attempt-private result/journal manifest. PrismaQuant can keep its current
one-per-unit journal model and merge same-qname fragments deterministically.

The child PB result must attest the exact batch, not only a launcher log. The
producer adapter emits one canonical child result manifest containing exact task
IDs/output IDs, values or artifact digests, and common source identity. PB ingests
that manifest as the action result; logs remain supplemental evidence. Missing,
duplicate or foreign task results fail the child closed.

After all child receipts exist, a generic exact-cover verifier checks each receipt
and payload against its child action, the immutable plan, one result per roster
task/output ID, and common parent identity. Only then does PB publish a group
receipt binding `parent_key`, `plan_key`, ordered child action/receipt digests and
merged-result digest. An incomplete/failed/withdrawn set is never group success.
Producer-specific qname merging may remain in PrismaQuant; PB need only enforce
generic manifest identity and exact cover.

## Resources, retry, withdrawal and cleanup

Every child inherits the common full per-process peak demand, GPU-memory budget,
exclusivity, placement, native-thread environment, checkout/data inputs,
timeout/progress/profile and retry policy. Aggregate demand is enforced by normal
concurrent child admission. The decomposer adds no resource scheduler.

Children are portable unless ordinary dependencies earn a real constraint. The
plan never includes host pins. All children were published by PB before their
execution; code inside an admitted child runs directly and never calls pbrun or
pbcampaign.

Retry and preemption preserve the same immutable child membership and existing
bounded attempt budget. A verified receipt is adopted/cached normally. There is
no inner task lease, live membership edit or extra capacity-release path.

Parent withdrawal first seals an immutable parent-generation decision, blocks or
stops decomposition, then routes every planned ready/claimed child through the
existing withdrawal ladder. If withdrawal races partial publication, recovery
uses the frozen plan and the decision to ensure no missing child is newly
published. Child capacity returns only after existing exact scope/container
cleanup. Parent/plan/group sidecars never release resources.

## Minimal source changes

* `tools/fleet/pbcampaign.py`: accept logical request v1; validate everything
  before parent publication; submit/wait/report parent and planned children.
* `tools/fleet/pbrun.py`: extract ordinary action build/seal/publish from `main()`;
  accept internal extra CAS inputs/sealed params and a child result manifest;
  resolve the batch-input placeholder without shell expansion. Preserve ordinary CLI behavior.
* A small `src/prismabuild/decomposition.py` module: parent/roster/policy/plan/
  batch/group-receipt schemas, canonical validators, deterministic partitioner
  and exact-cover verification. Reuse core canonical action/CAS primitives;
  extend `core.py` only where the ordinary action contract requires it.
* `src/prismabuild/pool.py`: parent decomposition directories/claim/lease,
  immutable plan publication, idempotent child-plan reconciliation, parent
  withdrawal and group status. Reuse ordinary `publish` for every child; do not
  change execution-child claim/admission.
* a small published `tools/fleet/decompose_loop.py` (or a supervisor role): claim
  queued parents and drive freeze-then-publish recovery. MVP may call the same
  library inline before deploying the role.
* `pbwait.py`, `pbstatus.py` and structured MCP readers: parent decomposition
  status, plan digest, child/task totals and terminal completeness.
* `docs/design.md`, operating policy and published skill: make pre-execution
  immutable planning the normative subdivision paradigm.
* PrismaQuant adapter: consume batch input after resident setup, write private
  child fragments, and exact-merge them under the unchanged full-band identity.

If batch-input materialization requires worker support, add a versioned
`logical-batch-v1` offer tag so old workers cannot claim these actions during
rollout. Prefer materializing it through existing CAS input machinery so no
worker-loop semantic change is needed.

## Meaningful failure/recovery tests

All eventual tests run through PB.

1. Fixed roster/policy derives deterministic smallest-useful batches and exact
   cover. Empty/duplicate IDs, bad estimates, unknown fields and illegal final
   remainder refuse before any child publish.
2. Identity matrix: same semantics gives same parent/plan/child keys; source,
   calibration/data, roster/payload, policy, membership, resources, environment
   or placement changes the proper key; host and timing do not.
3. Fault injection after frozen plan and after each child ready link: recovery
   reuses the byte-identical plan and fills only missing children. It never derives
   a fresh partition after partial publication.
4. Two compatible workers claim disjoint ordinary children. A worker becoming
   available after execution begins takes an already-ready child, with no plan or
   action mutation.
5. Failure/retry/preemption never changes child membership. Replay cache-hits
   verified children and runs only missing keys.
6. Missing/duplicate/foreign/conflicting child results and incomplete plans fail
   exact-cover merge closed. Group receipt appears only for a verified full roster.
7. Every child inherits exact resources, thread bounds, placement, checkout/data
   inputs and policies; batching adds no host pins or worker counts.
8. Withdrawal racing decomposition and each partial-publish boundary emits no
   post-decision child, withdraws visible planned children through existing code,
   and releases capacity only through exact child cleanup.
9. Old/opaque commands refuse the protocol; ordinary pbrun/pbcampaign, progress,
   prewarm, cache hits, worker claims and terminal readers remain compatible.

Live qualification should compare the same frozen grid unsplit and pre-chunked,
verify identical values/full-band identity, and collect before/after in-process
profiles plus Netdata/power on both GB10s and useful work per joule. Batch size is
adjusted only from measured setup/useful-work cost. No speedup claim precedes it.

## PrismaQuant integration handoff

Use `/home/rob/tmp/pq-sparse-rate-20260911` at `20ae48a222` as the inspected
source basis. Do not edit the live frozen study worktrees or requests.

- `prismaquant/tessera_campaign.py:1901`: reuse the full campaign identity.
  An explicit batch task-list filters work, not the original rate band, source,
  calibration, recipe, activation policy or full menu. Reject unknown tasks.
- `prismaquant/tessera_campaign.py:675`: retain `_anchor_batches` and existing
  selected-source preparation, production cache and calibration prefetch.
  Sequential tasks in one child amortize setup; add no parallel cache system.
- `prismaquant/tessera_campaign.py:2633`: reuse receipt-before-journal publication.
  Children write private fragments, never concurrently update one unit journal.
- `tools/dispatch_tessera_campaign.py:1332`: its existing merge rejects repeated
  qnames. Add an explicit frozen-rate fragment merge that permits only disjoint
  anchor keys with equal full-band identities and verifies all wire receipts.
- Materialize the normal complete campaign journal before invoking
  `experiments/collect_complete_rate_curve.py`; retain the collector's strict
  full-band and measurement-plan checks.

Adaptive next-anchor choices remain dependency barriers. Only an already-frozen
rate roster or completed-round decision is eligible for this decomposition.
No serving format, rate menu, artifact bytes, serving gate or production default
changes are part of this task.

Detailed producer-side source inventory and test suggestions are retained at
`/home/rob/tmp/pb517-pq-adapter-design.md`; this document's user-approved
pre-execution boundary takes precedence over any earlier draft.

## Existing overlap

No PR references #517. Closed #387 is GPU pytest file fanout; #480 is progress/
liveness; open #487 supplies the reusable data-manifest prewarm path. Historical
elastic-worker changes scale claimant loops, not work supply. PrismaQuant #282's
group fanout and current per-unit journals are producer-side reuse; this change
makes PB's immutable pre-execution child plan the remaining fanout layer.

## Explicitly rejected designs

Resident actor consumers with inner PB task leases are rejected for this change.
They add a second ownership/recovery protocol and make an actor's consumed subset
late-bound. Live pause/transfer/resume and exit-to-split continuations are also
rejected. Both change ownership after execution starts and require new fencing at
the most failure-prone boundary. Static application-selected shard counts are
rejected because the application would become the fleet dispatcher. The chosen
parent-decomposer-plan boundary gives PB control of granularity while every
published execution unit stays immutable for its whole lifetime.
