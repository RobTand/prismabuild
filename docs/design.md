# PrismaBuild — distributed campaign execution

**Status: DETERMINISTIC CORE + SHARED CAS + PULL QUEUE LIVE; SLURM LANE BUILT
AND VERIFIED AGAINST A REAL CONTROLLER IN A CONTAINER, NOT YET INSTALLED ON
THE FLEET; DAGSTER AND OBSERVABILITY LAYERS NOT DEPLOYED.** The
dependency-free action-key, immutable-CAS, and local-worker core lives in
`src/prismabuild/core.py`. On 2026-09-04 Rob ratified replacing the pull queue
with SLURM (`docs/scheduler_decision_2026-09-04.md`). The thin SLURM lane that
implements it lives in `src/prismabuild/slurm_lane.py` (`pbrun --transport
slurm`: seal, `sbatch`, wait, CAS lookup, and the terminal records the pull
queue's readers already look for), with the job entry in
`tools/fleet/slurm_job.py`, the fleet's configuration under `fleet/slurm/`, and
the install runbook in `docs/slurm_runbook_2026-09-04.md`. Only SLURM's own
variables reach a job (`--export=NIL`); the action's environment is the sealed
one the worker builds. A SLURM `COMPLETED` state without a CAS receipt is a
failed action, and a receipt is success whatever the exit code said. The lane
routes work by what the action already declares: a GPU demand goes to the
`gpu` partition (the two GB10 boxes, as `shard` GRES), untagged CPU-only work
goes to the `cpu` partition (dl380g10), tagged work goes to the default
partition, where its sealed constraint picks the node, and CPU-only work the
submitter asserted portable with `pbrun --anywhere` goes to the default
partition too, where node weight prefers dl380g10 and a GB10 box takes it
only when dl380g10 is full. The lane has run
against a real `slurmctld` and `slurmd` in a privileged container on sparky
(`fleet/slurm/smoke/`, 23 rows on the fleet's 25.11.2 rebuild; the first
eleven also on Ubuntu 24.04's 23.11.4), and across three container nodes built
from the fleet's own `slurm.conf` (`fleet/slurm/smoke/multinode/`, 12 rows on
both versions: placement per partition, tag and weight, a node killed under a
job, a controller restart under a job, and the runbook's `verify.sh`). A
re-run of receipted work submits nothing on either path: `pbrun` reads the
receipt before `sbatch`. It is installed on no box: the install needs root,
which is Rob's. `fleet/slurm/install.sh`, `verify.sh`,
`cutover.sh` and `rollback.sh` are the operator's four steps, in that order.
`tools/fleet/pbcampaign.py` fans a manifest out over the lane and
`pbwait.py` waits for the keys, whichever transport filed their endings;
`pbrun --transport slurm --measurement --host-class` seals a class-keyed action
the worker attests through the controller. `src/prismabuild/slurm.py` is the earlier durable-state SLURM
adapter, superseded by the lane and retained until the decision record's
Phase 3. `tools/prismabuild_worker.py` is the direct batch-script entry point.
`docs/operating_prismabuild.md` is the usage guide for operators and agents.
The optional asset/DAG adapter lives in `src/prismabuild/dagster.py`; it
constructs deterministic assets from sealed action keys, binds each edge to an
expected CAS output digest, and materializes only after re-reading that
receipt and payload from the CAS. The shared CAS, NFS pull queue, and worker
loops on Sparky, Sparklina, and dl380g10 are deployed and remain the live
execution plane until the cutover. The fleet has dispatched Tessera and
PrismaQuant test, quantization, and measurement campaigns. Dagster and the
proposed observability stack remain uninstalled.

Worker-offer freshness is evaluated after the complete directory and record
scan. An offer that expires during any read is excluded, including an offer
read before a later file stalls. Offers require a finite numeric announcement
timestamp no later than the scan's completion time, matching the pool status
reader's refusal of invalid or future evidence. Shared filesystem I/O itself
remains synchronous and has no caller deadline; this expiry rule does not bound a
queue read or repair an NFS client stall (issue #16).
The pool status census likewise collects active records and admission, lease,
and denial sidecars before deriving worker/sample freshness and placement. Its
`sampled_unix` is the time that collection finished; it remains a non-atomic
diagnostic snapshot, not a process-liveness or storage-recovery guarantee.

Adaptive claimants discover ready candidates and their aging sidecars outside
the host admission lock. A brief nonblocking lock check preserves early busy
refusal before discovery; admission is reacquired before decisions and queue
mutations. The candidate list is advisory: an intervening claim wins, and the
record actually moved must still satisfy placement and resource checks. An
empty scan returns without entering the shared capacity prelude or repeating
discovery under admission, and clears absent-generation fallback pacing hints.
Capacity reconciliation still precedes every nonempty candidate pass; active
holders are unchanged by the empty-poll return. The worker's independent offer
refresh and capacity clamp continue on their normal cadence. This removes scan
stalls from the critical section but does not bound discovery or shared transition, lease and
token I/O, which still need ownership-safe recovery qualification (#266).

When the claimant supplies CPU tiers, validation of an existing `cpu-map.json`
also runs before host admission. That map is immutable while workers run;
changing it requires stopped workers and drained reservations. A missing map
is initialized only after acquiring admission, with the existing legacy-holder
guard and atomic publication. Legacy callers with no supplied tiers still
resolve the map in the capacity prelude. Validation grants no capacity: minting,
retirement, holder accounting and reservations remain under the same exclusion.
An intervening busy gate leaves the candidate queued. Map reads remain
synchronous and can delay their own caller.

CPU/GPU policy refusals and unfunded reservations record denial aging outside
host admission, while retaining the candidate's per-key transition lock.
Withholding-age reads also run outside admission. A denied reservation owns
no tokens or probe/borrow credit. Background preemption selection still requires
host exclusion to serialize pending releases; it reacquires admission
nonblockingly after aging and re-reads capacity and holders. A busy gate leaves
the candidate queued with its recorded denial. Funded reservation, GPU probe
consumption and CPU borrow consumption remain one admission critical section.
Selection and its withdrawal/requeue handoff also share a separate nonblocking
host-local preemption lock. Admission is released after selecting a victim;
withdrawal, broker stop requests and retry publication hold only the preemption
lock and the holder's per-key transition lock. A stalled handoff prevents
another preemption selection, but fitting ordinary claims can proceed. The
withdrawal still revalidates the exact selected claim, and no tokens are
returned before the holder's normal cleanup. The preemption lock is a permanent
`<ledger-and-host-digest>.preemption` inode under the same box-state root
as admission; it is never removed or released on a timeout. Changing the root
or mixing generations that do and do not use this lock requires drained work
and completed worker rotation before submissions resume.
Shared capacity/holder scans and token moves remain under host admission;
this separation does not make admission NFS-free or bound a shared syscall.

CPU action identity and GPU exclusivity/memory-contract reads from sealed CAS
requests run before candidate admission, retaining the per-key transition lock.
The controllers receive those candidate-specific facts, including unknown
results, without re-reading requests inside host exclusion. No capacity or
sample credit is prefetched: current host samples, holder accounting, token
acquisition and probe/borrow consumption still run under admission. A gate
that becomes busy during request reading leaves the candidate queued without
a reservation. Standalone controller calls may still resolve their own action
facts. Request I/O remains synchronous; a stalled read no longer holds the
host gate but can still delay its own key.

The pull queue orders ready items by descending priority, then descending
admission-denial count, then oldest publication time. Aging changes order only
within a priority band. A denied item past `STARVATION_FLOOR` may withhold its
host until `WITHHOLD_CEILING_S`, but higher-priority items have already been
considered before that veto is reached. The existing withholding rule still
protects large items within each band. Priority defaults to 0 and is queue
metadata outside action identity; `pbtest --priority` forwards it to every
shard. Agent self-validation uses -10 so queued campaign work at 0 is considered
first. A denied foreground item may also preempt one admitted background holder
on the same host: the lowest-priority holder whose released tokens would admit
the denied demand is withdrawn through the existing withdrawal ladder and
re-published at its original priority and aging count, as a new generation the
cancellation does not cover. Release is asynchronous, so the denied item is
admitted on a later pass. Preemption never crosses into the foreground band,
never stops a holder whose release would not close the gap, and never cancels a
second holder while a withdrawn one's tokens are still owed. It stops nothing when
the selected claim concluded or changed before withdrawal, and refuses rather than raises when the holder's
reservations contradict each other, because neither is the denied item's
business. Eligibility requires a verified generation action, explicit
`retry_safe: true`, and an unused attempt after the interrupted launch.
Measurements, unknown actions and holders with recorded failures remain running;
the latter keeps its attempt history in the original generation. Priority alone
never grants retry permission. An interruption consumes one of `max_attempts`
through the existing `attempts` counter, so repeated preemptions and subsequent
failures cannot refresh the launch budget. New generations account for earlier
interruptions through `attempt_history_missing_before`; their immutable
withdrawal decisions remain linked by `supersedes_withdrawal.published_unix`.
No failure history is discarded or rewritten. A preempted attempt records
`preempted_by`; a waiter follows the exact `supersedes_withdrawal` lineage
through any repeated interruptions and reports that retry's ending. It does
not adopt an unrelated later generation's verdict merely because the key matches.
The existing immutable attempt outcome also retains the preemption handoff
context and interrupted-attempt prefix. If a later same-status generation
replaces the mutable terminal summary, the waiter reconstructs the original
retry's ending from that attempt, revalidating canonical history, log digests
and withdrawal lineage. Recovery writes no queue pointer and names the immutable
attempt as its source. Attempts predating this context provide no inferred link.
The withdrawal and replacement publication share the holder's transition lock;
waiters acquire it before resolving the replacement, so a partially completed
handoff cannot report cancellation while the replacement is being published.

The pull queue admits the generation actually moved from `ready/`, including
its placement and resource demand. A replacement whose admission requirements
changed returns to `ready/` for a fresh decision. Requeued records use the item
schema and retain attempt history, but discard claim ownership, reservation and
cleanup stamps, and the sidecar aging count. A record the reaper concludes
names two boxes and never conflates them: `claimed_host` is the box the action
was on, recovered from the unique committed reservation when the claimant was
lost before it rewrote the record. Only when no ledger names a holder does the
same-generation claim-intent marker supply that fallback. Multiple committed
holder ledgers found during that recovery are a contradiction, distinct from
absent evidence: the reaper
reports the action key and conflicting hosts, retains the claim and every
reservation, and continues with other claims. Withdrawal refuses that ambiguous
claim before recording a cancellation or releasing anything. Recovery retries
the evidence on later sweeps; it does not guess a host. A lease whose claim is
gone and whose host is missing or invalid uses the same holder resolution
before cleanup. Conflicting ledgers retain the lease and every reservation;
absent ownership never becomes the sweeping host's ledger.
Every concluding path reconciles a nonempty mutable claim or lease host with the
unique committed ledger before cleanup or a state transition. A mismatch is
contradictory evidence and retains the claim, lease, and reservation; the
recorded host remains the legacy fallback only when no committed ledger exists.
A claim-to-tombstone
rename that fails does not establish cleanup ownership: the reaper retains
the claim, lease and reservation, reports the refusal, and retries on a later
sweep without publishing a runnable replacement. `finished_host` is the box
that filed the ending. Readers report the first as where the work was; the
second reaps most of the fleet's work and would otherwise absorb its failures. An attempt counts an execution, so
a claim reaped with no lease ever written and no immutable attempt published
under the number it would take is *released* rather than concluded: it returns
to `ready/` with its attempt count unchanged, the release filed under
`withdrawn/superseded/` as an `unstarted-claim` and counted on the item as
`unstarted_releases`. Both halves of that test are load-bearing, because a
restored finish tombstone also has no lease and must keep the charged path. A
claim covered by a withdrawal decision is concluded without a retry. Legacy
withdrawal stamps remain readable, but new withdrawals never rewrite a claimed
record's retry limit or ownership fields.
Releases are counted, not bounded: the measured stall between the rename and
the lease has no upper bound on this filesystem, so a bound would be a guess
about a delegation recall. Both readers surface the count, because a release
nobody can see is indistinguishable from a quiet queue: `pbstatus` carries it
as a `RELEASES` column and repeats it in a ready job's note, and `pbmetrics`
exports the queue-wide sum per active state plus a per-box count over its
bounded recent window. The box comes from the `withdrawn/superseded/` filing
rather than from the requeued item, which no longer names one. Unparseable ready records are
isolated under `withdrawn/superseded/` with their original bytes and a bounded
diagnostic; healthy records continue through the queue. A quarantine restores
a concurrently repaired record without replacing another submission and never
overwrites an existing failed outcome. These contracts have local filesystem
regression coverage; they are not a cross-host NFS qualification claim.

Ownership mutations hold a permanent per-key POSIX record lock under
`transition-locks/<sha256(action_key)>.lock`. Publication, scope startup and
recovery, heartbeat writes, finish, withdrawal and recovery sweeps use the same
lock. Claim and sweeps skip busy keys; independent keys continue. A queued
successor cannot replace an active claim or pending finish tombstone. The lock
inode is never deleted, thread nesting retains its original descriptor, and
process death releases kernel ownership. Cross-host POSIX lock visibility is a
queue mount requirement; NFS client mounts with local-only locks are unsupported.
The helper is shared with SLURM terminal-summary publication. Bidirectional
exclusion and reacquisition were qualified through admitted PB jobs between each
GB10 NFSv4.2 client (`local_lock=none`) and the dl380g10 server-local ZFS path.

Heartbeats verify the owner and, for worker execution, the exact claim snapshot
while holding that lock. A late heartbeat refuses to overwrite a successor's
lease. Legacy owner-only callers cannot distinguish attempts sharing an owner;
internal claim, scope and execution writers always supply the snapshot.

Before heartbeat publication or ordinary finish cleanup, available claim and
lease identities must agree on owner, host, claim time and publication. A
contradiction refuses the heartbeat before it overwrites the existing lease, preserving the evidence
the finish guard needs even when a stale claim matches the caller. It refuses
finish before broker or action-wide Docker cleanup, telemetry writes or token
release, even when the ledger names the same host. Missing legacy lease fields
supply no additional proof. This closes a stale-claim/fresh-lease case; it does
not make two shared reads an atomic snapshot or qualify jointly stale evidence.

Scope startup applies the same claim/lease consistency check before writing
the creation intent and again after the broker reply, before writing the
created scope. The subsequent heartbeat check is too late to protect those
claim writes. A contradiction before intent persistence creates no broker
scope; one found after creation stops and releases only the caller's newly
created, unlaunched scope, archiving its stop marker under that attempt's nonce.
Successor claim and lease bytes, telemetry and
reservations remain unchanged. These reads hold per-key transition exclusion,
outside host admission, and do not qualify jointly stale observations or
bound shared-filesystem latency.

Recovery of a lost scope-create reply also checks claim/lease consistency
before writing recovered authority into a matching live claim. A contradiction
retains the durable creation intent and all successor state, and reports
incomplete cleanup for a later retry. The exact-nonce broker recovery may have
already succeeded; refusal neither creates another scope nor releases tokens.
When a fresh read identifies a different successor, the predecessor's recovered
authority remains confined to its own cleanup record and diagnostic archive.

The stale-claim reaper checks claim/lease identity before payload cleanup and
ownership mutations, even when the lease has expired. A conflicting key retains
its claim, lease and reservation while independent keys continue recovering.
The pending-finish branch likewise retains identity contradictions reported by
ordinary finish and continues the sweep. Consistent observations on a later
sweep may resume recovery; missing legacy lease fields add no proof. These
checks do not qualify jointly stale reads or bound shared-filesystem latency.

A released unstarted claim has no numbered execution outcome. If its original
caller reports a result while the same publication is ready or claimed with
that attempt number still uncharged, the late report is retained under
`withdrawn/superseded/` as `uncharged-late-finish`, after any exact-scope cleanup.
It cannot occupy the successor's immutable attempt slot, create a terminal,
or release the successor's reservation. A cleanup refusal retains the existing
late-finish recovery authority. Charged retries and different publications keep
their existing numbered, first-writer-wins history. A late report uses that
same archive and exact-scope cleanup path while any successor is still READY,
including a charged retry or a replacement publication. It cannot publish a
terminal beside that queued successor or stop its waiter early. This covers
deterministic lease-loss/late-caller faults; it is not cross-host NFS-stall
qualification.

The [cross-host recovery qualification](claim_recovery_qualification.md)
records admitted queue-method actors in both directions between DL380 and each
of Sparky and Sparklina, including late success/failure, immutable history and
waiter continuity. Default-mode inner claims launch no payload or broker scope.
The optional real-scope campaigns use bounded direct payloads and descendants:
foreign-host cleanup refusal and a caller-local injected broker failure retain
the claim and reservation, then cleanup on the owning host retires its exact
scope before retry. Successor scopes survive late original-owner calls, and
the original waiters subsequently return the successor results. Successful
harness teardown preserves production's termination audit.
The optional Docker mode also places one sleeping CPU-only container in each
scope, using the ordinary shim. The recorded DL380/Sparky and DL380/Sparklina runs check inherited
CPU affinity, container removal before retry, successor container survival
after late calls, and final removal through production cleanup.

The same-host mode retains the old caller while a successor scope and container
run locally. It requires identity refusal under an injected stale claim read,
and checks ordinary late calls, cleanup retention and original waiter continuity.
The original payload scope is retired before the successor launches.

These campaigns depend on the owning host remaining available and supply the
successor's queue result through the harness. They do not qualify simultaneous
payload attempts or jointly stale claim/lease evidence,
late Docker creation RPCs, permanent host loss/reboot,
induced kernel NFS stalls, execution-budget
accounting during stalls, normal scope creation/preflight, or normal execution
result collection. The runbook retains the failed campaigns and receipt tables;
the remaining #234 requirements cannot be inferred from those passing cases.

Execution heartbeats carry an optional `execution_observation`: the direct
launcher's polled liveness, cumulative stdout/stderr bytes captured at the
checkpoint, and the time output was last observed to grow. Observation time is
sampled locally before lease publication and is never refreshed merely because
a delayed shared write completes. Lease publication also records the claim's
publication identity. Status accepts observations only from that exact key,
owner, host, claim time and publication, with finite ordered timestamps,
nonnegative integer counters and an actual boolean liveness value. Missing,
invalid or expired observations report unknown liveness, even with a recent
heartbeat. The existing lease-expiry interval bounds observation freshness;
the observation age remains independently visible.

The pool's local execution deadline excludes synchronous checkpoint intervals:
initial observation/lease publication and the observation, scope sample,
withdrawal read and heartbeat work between subprocess waits. Each completed
checkpoint shifts the monotonic deadline by only its own elapsed duration;
previously charged spawn/wait time remains spent. The shorter sealed budget
and worker ceiling still governs, including budgets shorter than a heartbeat.
This prevents delayed worker bookkeeping from exhausting a payload's remaining
budget. It does not infer progress from observation fields, exclude payload
kernel stalls or stalled subprocess waits, or bound a blocked checkpoint.
Resource failures and withdrawals retain precedence and exact-scope cleanup.

The pipes include inherited application output and launcher messages. Silence,
buffered output and a live launcher do not establish application progress; an
exited launcher does not establish that descendants stopped. These fields are
diagnostics and grant no retry, termination, resource release or execution-budget
change. Endings retain the last execution observation with its original sample
time, which can precede the final output and process exit. Ownership-safe
stalled-claim recovery remains a separate qualification under #234.

Withdrawal records an immutable decision under
`withdrawn/decisions/<action_key>/<attempt_generation>.json`, using the same
publication identity as attempt history. The first decision for that generation
wins. The current `withdrawn/<action_key>.json` remains the operator-facing
ending, and publication may retire it without erasing an original attempt's
stop request. All cancellation gates consult the generation decision even when
the live marker has been retired. A malformed decision is a refusal, never an
inferred cancellation of another generation.

The withdrawal caller owns no claimed record, lease or reservation and never
rewrites, removes or releases them. It returns `released: 0` and a pending stop
until the claiming worker or reaper completes cleanup. On the claiming host,
a saved broker scope authority may accelerate stopping only that exact
attempt; uncontained work stops through its worker's marker checkpoint. A
process search by action key cannot distinguish successor attempts and is not
used for withdrawal. Ready cancellation moves and reads the record before
checking its generation, and restores replacements without overwriting them.
This contract retires the old retry-limit poison write and synchronous local
process scan. Deployment requires draining and upgrading workers to readers of
immutable decisions before relying on asynchronous cancellation across a
re-submission; there is no unsafe legacy fallback.

READY-record examinations use `ready-transitions/` as their recoverable
intermediate namespace. Withdrawal and orphan cleanup move the original bytes
there under the key's POSIX transition lock, then either restore them with a
no-clobber link or retain them as superseded evidence after a durable disposition.
The reaper revisits expired captures under the same nonblocking key lock. Live
successors are preserved, usable records require a matching-generation ending
before retirement, and unavailable evidence or a failed restore keeps the
capture for another sweep. An orphan capture is acknowledged only after its
ending is published or a successor is observed. These transitions never release
reservations and do not overwrite another terminal record.

Widowed-lease recovery also holds the nonblocking key transition lock from its
claim census through capacity return and lease removal. A busy key is deferred
while independent keys remain recoverable.

A finish tombstone remains cleanup authority even when its generation already
has a durable cancellation or terminal ending. Before retiring such a tombstone
with no live claim, recovery verifies its holder and payload cleanup, returns
any remaining reservation, and removes only a matching lease. Unknown cleanup,
conflicting holders, or an incomplete token return retains the tombstone. A
queued successor remains untouched; a claimed successor's resources and lease
are never released using an older tombstone.

A late finisher with an exact broker scope persists its original claim authority
and completed payload result in `claimed/<key>.<claim-identity>.late-finish`
before cleanup. The existing finish-recovery sweep retries this record only on
its claiming host, under the same key lock, even while a successor is live. It
stops and releases only the saved action/nonce/token scope; a nonce also named by
the live claim is a refusal. It never uses action-wide Docker ownership, returns
the successor's tokens, or rewrites its claim, lease or live telemetry. Cleanup
telemetry instead lives under the existing ledger's
`telemetry/attempts/<nonce>/<key>.json`, and a delayed cleanup trains no admission
profile. Broker stop/release still proves aggregate containment, including
containers; an empty frozen retired scope remains protected against late Docker
RPCs by the existing broker contract.

Unproven cleanup retains the late-finish record with its original result, exact
scope authority, failure count and first/last failure times. A restart can retry
it without the original worker, and it is never converted to a lost lease or
inferred success. After cleanup, the original immutable attempt keeps its
first-writer-wins outcome; separate superseded evidence retains the cleanup proof
even when that attempt already existed. Only then is the recovery record removed.
New claimants defer while this pending finish exists. Older runtimes ignore the
new suffix rather than discard its authority as an ordinary superseded tombstone;
all claimants must be upgraded before relying on the new admission deferral.

Local task output is now crash-recoverable without accepting unowned bytes.
Before argv, the worker publishes an immutable claim for the exact action,
resolved checkout, working directory, and declared result. Under the same
output lock, a retry may discard and recompute only a regular contained result
with that exact claim; it never adopts the old bytes under a new producer
attestation. `repair-local-result` performs only that checked cleanup. One
subprocess SIGKILL fault test covers death after result/blob and receipt-temp
staging but before canonical receipt publication. This is process-fault
coverage, not power-loss or deployed cross-host-lock evidence.

`run-local` also has one qualification-only, opt-in causal hook:
`--initial-miss-rendezvous /absolute/manifest.json`. It proves that two exact
worker processes both observed an initial miss against the same configured CAS
root before either can
reach the output lock and publish. It is inert when unset, is not entered on an
initial cache hit, and is incompatible with `--recompute`. This proves worker/
miss contention; it makes no task-argv timing claim. The output lock serializes
workers by the canonical physical path of the declared output, whatever
checkout root and working directory the caller spelled it with, so nested
roots naming one file share one lock. Workers whose declared outputs are
distinct files may execute task argv concurrently and converge through
ordinary CAS publication.

## Sealed execution environment

`pbrun` executes its wrapper with `bash --noprofile --norc -c`. Host login
profiles cannot rewrite the sealed PATH or select a different executable.
CUDA tooling outside the default PATH must be named explicitly or supplied
with `--env PATH=...` and appropriate placement. Native OMP, MKL and OpenBLAS
thread defaults equal the sealed CPU demand; explicit environment overrides
remain caller-owned. Parallel test processes must reserve their combined CPU
and memory demand. These defaults change new action identities; old immutable
requests and receipts retain their original meaning.

## Test fanout submission

`pbtest` validates every requested path before fanout. A path that is neither
a file nor a directory, or a discovery error, refuses with exit code 2 before
any shard is submitted. Valid paths never hide a missing member of the request;
directory discovery and deduplication retain their existing semantics.

`pbtest` file fanout is a public submission contract. CPU-only remains the
default; `--gpu` adds GPU demand to every shard, with an optional pool-only
`--gpu-memory-gb` budget validated by the same helpers as `pbrun`. The published
default placement class is `x86` for CPU and `gb10` for GPU, overridable by
explicit tags. CPU demand remains pytest workers times their native thread
ceiling, or a larger explicit reservation; host memory covers the entire shard.

Structured `--pytest-args` forwarding uses a closed population/report vocabulary
and replaces environment/project `addopts` when supplied. Worker count, config
indirection, extra file paths, and xdist's population-duplicating `each` mode
are refused rather than overriding PB's reservations or file partitioning.
Surface report names expand `{shard}` or receive `.shard-N` before the final
suffix. Expanded arguments and GPU budgets enter the ordinary sealed action
identity through `pbrun`; no second dispatcher or placement policy is added.

## Problem

Campaign work (screens, per-point KL fan-outs, per-tensor encodes, A/Bs)
serializes behind one coordinator's attention while GPUs and CPUs idle.
Utilization is bursty; dispatch is manual (ssh + systemd-run). We want
independent work to run the moment its inputs exist, across a heterogeneous
fleet, without hand dispatch — and with strong observability.

## Live and proposed fleet inventory (2026-09-04)

The pull queue discovers and enforces the live offers from Sparky, Sparklina,
and dl380g10. Other rows remain proposed expansion. PrismaBuild has not
installed a SLURM controller or node daemon, created the named
partitions/reservations, or attested any machine through a SLURM allocation.

| host class | machines | role |
|---|---|---|
| `gb10` | sparky, sparklina (GB10, 128 GB unified, sm_121) | live pull-queue workers for probes, validated KL, ship gates, and big renders; no SLURM reservation is installed |
| `rocm-16g` | Rob's + son's 9800X3D/9070 XT desktops | 0.6B screen tier; brute-force search/encode (trellis Viterbi, permutation/gauge searches, CB training) |
| `strix-32g` | son's AI Max laptop (32 GB unified, opportunistic) | 4B screen tier (the size 16 GB cards can't hold) |
| `cpu-x86-large` | dl380g10 (40 physical cores, 80 SMT threads, 300 GB, NFS server) | live pull-queue CPU worker and shared CAS/NFS host; page-cache, hashing, repacking, shard merges, references, bootstraps, and CPU encode work |
| — | M5 Mac mini | below the value line; not a tier |

The live data plane is `/mnt/shared` (NFS from dl380), including the deployed
PrismaBuild CAS and pull queue under `/mnt/shared/prismabuild-fleet`. Workers
load immutable published runtime generations and use per-architecture venvs
(envs cannot be shared across aarch64-CUDA / x86). A future
munge-authenticated SLURM installation remains the proposed trust plane for a
larger cluster.

## Deployed execution plane and optional target services

The repository implements and tests the PrismaBuild core, the live pull-queue
transport, the SLURM lane, and optional Dagster definitions. The shared
CAS/pull queue and three worker hosts are live. SLURM daemons, `slurmdbd`,
Dagster, and the listed telemetry services are not installed.

1. **SLURM** — resource layer, ratified 2026-09-04. As configured in
   `fleet/slurm/slurm.conf`: one cluster with the controller on dl380g10;
   partitions `gpu` (sparky, gx10-6b77), `cpu` (dl380g10) and the default
   `all`; `shard` GRES for fractional GPU slots (2 on sparky, 3 on gx10-6b77)
   and `gpu:1` for exclusive use; cores and memory both consumable
   (`select/cons_tres`, `CR_Core_Memory`); cgroup containment of cores,
   memory and devices; the fleet's `RealMemory` budgets carried over from
   `fleet_boxes.json`; and a node-side Epilog that removes a killed job's
   containers by ownership label and its materialized checkout. Scheduling
   is FIFO plus backfill. `slurmdbd`, age priority, QOS and standing
   reservations are deferred until a measurement asks for them. Machines
   joining and leaving (rented or contributed) are a later concern; SLURM's
   cloud-node mechanism is the sanctioned route when it comes, and the
   transport-agnostic core is what keeps that door open.
2. **Dagster** — DAG + memoization layer. Selected over Snakemake because two
   hard requirements point at it: (a) native asset memoization keyed by
   `code_version` + upstream input versions — exactly the cache model below;
   (b) best-in-class live observability (run timelines, per-step logs, asset
   lineage/staleness UI). Known seam we own: Dagster→sbatch run-launcher
   glue is community-grade (~100 LoC).
3. **CAS + pull queue on /mnt/shared** — deployed content-addressed store and
   NFS-safe dispatch plane; payload paths derive from content hashes and worker
   claims are rename-owned leases.
4. **Prometheus + Grafana + Loki + Alertmanager** on dl380 — the proposed
   stack would use node_exporter, dcgm-exporter (GB10), AMD SMI exporter, and
   slurm-exporter, with job logs via promtail. Receipts would be pushed as
   metrics so campaign progress (KL per point, stage durations, gate outcomes)
   is graphable, not just machine health. It would remain orchestrator-
   independent.

The current fleet dashboard implementation is versioned in
`fleet/observability/`, targeting the existing Grafana instance on beelink.
Its read-only Prometheus collector observes pool records and saved admission
evidence; existing Netdata agents supply whole-host activity. Neither path
participates in admission or changes reservations. Missing or stale evidence
is distinguished from idle capacity. Retained terminal-window gauges describe
recorded outcomes, not independent CAS verification or a permanent event ledger.
See [the deployment guide](../fleet/observability/README.md) for datasource,
worker-target and qualification requirements. This supersedes the proposed
dashboard host above; it does not claim the other proposed telemetry services
are installed.

## Cache/action-key semantics (the Bazel steal)

Process-I/O sampling rechecks a PID's starttime after reading its counters.
If the process disappeared, its identity cannot be read, or the PID now names
a different incarnation, that observation contributes no new counters. This
prevents replacement bytes being attached to the earlier sampled identity;
it is not an atomic process-tree snapshot or complete short-lived-child
accounting. This changes diagnostic evidence, not action identity.
An accepted reading retains the parent PID from that identity recheck. A child
orphaned during counter collection can thereby become a scope root, preserving
its observed counters on departure instead of assuming its former parent
absorbed them. Reparenting outside the read interval and unseen final I/O remain
sampling limitations.
If the initial identity read is unavailable or malformed, the sampler retains
that PID's previous reading without new counters or a new process identity.
It records an unreadable process and does not retire the prior root merely
because identity could not be inspected. A missing procfs record still denotes
departure; later confirmed PID reuse is accounted as a separate incarnation.
When a scope census omits a previously sampled PID, the sampler checks that
PID's cgroup membership before retiring it. A still-contained process is
resampled; unreadable or malformed membership retains the prior reading with
an unreadable diagnostic and contributes no new bytes. Confirmed departure
retires last observed counters when no ancestor from the previous sample
survives with the same PID/starttime. If a child and parent both depart between
samples, retire both last readings: the stale parent reading predates the reap.
A surviving ancestor, including a grandparent, continues to carry inherited
counters without separate retirement. Historical PID membership alone cannot
establish inheritance. This preserves observed departed subtrees, not their
unobserved final I/O or an atomic process-tree census. This protects known
members against partial scans, not discovery of processes never observed or atomic membership during collection.

The procfs discovery fallback reports enumeration failures and counts unreadable
or malformed cgroup membership records in process-I/O `errors`. Confirmed
departures (ENOENT/ESRCH) are ordinary. Valid discovered members and recovered
known-member counters remain available, but they do not prove a complete census.
An unknown record can belong to another scope; the diagnostic establishes
incomplete discovery, not missing I/O attributable to this action. Successful
fallback after a protected cgroup directory read is not itself an error.
Unreadable entry metadata and malformed or nonpositive `cgroup.procs` entries
also trigger that fallback, preserving valid members found by the hierarchy
walk. Metadata errors must not become a false non-directory result. This
changes diagnostic evidence only; it does not recover unseen processes or alter
admission, containment, or action identity.

Always-on box-window evidence is outside action identity. Its collectors share
a cooperative two-second finish budget: the GPU power-reference query receives
only the remaining budget, capped at one second, and is skipped after expiry.
Already-recorded power remains available without a reference fraction. This
same remaining-budget rule applies to every Netdata chart request, with no
minimum timeout grant; CPU data already collected survives skipped pressure
reads. This does not impose a hard deadline on filesystem reads, HTTP response
processing or process cleanup.

The pqteld collector checks expiry before discovery, each open, the header,
each seek/read block and between parsed rows. It visits files in reverse
discovery order and rows from each captured EOF backwards in 64 KiB blocks,
so old day rows do not consume the budget before a recent action's samples.
A read already in progress may overrun the budget; its first complete row is
processed before checking expiry again. Memory holds a block plus a spanning
row. Aggregates retain the last measured cell in original file/append order.
Timestamp comparisons only filter rows: clock corrections prohibit early
stopping, so a complete window may still require scanning whole day files.
Appends after EOF capture belong to a later read; short block reads report a
file error. This is cooperative accounting, not a hard filesystem deadline,
an atomic recorder snapshot or a timestamp index. Action identity is unchanged.

Recorder filename discovery uses the executing hostname and its explicit
`fleet_boxes.json` `_alias` equivalence from the collector's own generation.
Both names can contribute rows after a hostname change; the evidence retains
the executing hostname. Ambiguous declarations refuse CSV evidence rather than
merging machines. Missing configuration permits only the exact hostname, and
directory contents never establish identity. This affects telemetry discovery,
not placement or action keys.

Netdata chart reads request 4096 points with average grouping, while
retaining the 4 MiB response read cap. Long windows therefore use averaged
buckets instead of asking for every stored row and truncating valid JSON.
Netdata rounds the point target to whole time buckets, so the returned count
can exceed 4096. JSON wrapping supplies `view_update_every`, the bucket interval,
which is distinct from the database's raw `update_every` collection interval.
The CPU window records `time_group: average` and each available chart's
`update_every_s` (prefixed `psi_some_avg10_` / `psi_full_avg10_` for pressure).
Means and maxima describe returned buckets; maxima are not raw-sample peaks.
Missing intervals stay absent. This changes run evidence, not action identity.

Result address = hash(input artifacts, **code closure**, params, env-that-
matters). Rules:
- **Code closure, not repo SHA** — per-task declared file lists (stage-7's
  contract-pinned dependency list is the house precedent). Bias to
  over-declare: over-invalidation wastes compute; under-invalidation serves
  stale results.
- **Generation vs measurement tasks**: ordinary generation (encodes,
  permutation/gauge searches — discrete outputs re-scored later) may exclude
  host from the key → any box's result is valid ("surrogates generate, real KL
  selects" applied to hardware). Measurement (KL, PPL, probe) includes verified
  platform and toolchain identity because numerics do not transfer across
  architectures. The pool seals a `platform_keyed` action and an implicit
  submitting-host placement pin by default. An explicit pool `--host-class`
  instead seals class placement plus matching platform/ABI/device models.
  SLURM seals an explicit `host_class_keyed`
  action; the gold path remains pinned to `gb10`. Codebook generation is also
  nonportable because D29 records cross-architecture row-scale byte drift.
- **Artifact family is explicit** — action schema
  `prismaquant.prismabuild.action.v2` requires the closed
  `task.artifact_family` value `generic` or `codebook`. `artifact_kind` remains
  a descriptive identifier and never drives portability by substring. V1 is
  not reinterpreted: callers must redeclare the family and reseal the action.
- **Deterministic vs stochastic** task classes: deterministic entries may be
  verified by recompute; stochastic (probe backward is recorded
  non-bit-reproducible) get run-once / first-result-wins.
- **Pool retry safety is a separate contract** — numerical determinism says the
  declared result bytes repeat; it does not make external effects idempotent.
  An arbitrary `pbrun` command gets one attempt. Only `--retry-safe` plus a
  larger `--max-attempts` opts into bounded retry; the exact policy is sealed
  in action params and carried in the queue record. Each attempt is immutable
  first-writer evidence: its adopted status and disposition determine the
  mutable queue destination, summary, and `pbrun` exit status even when a
  finisher and stale reaper race; any disagreement fails closed.
- **An opt-in profile is a parameter, and a queue hint is not** — `--profile
  MODE` is sealed in `params.profile`, so a profiled run has its own key. A
  profiler is inside the measurement: answering a profile request from an
  unprofiled receipt would return a receipt with no profile, and comparing a
  profiled arm with an unprofiled one would compare two different executions.
  `--priority` is the contrast and stays out of the key, being a hint about
  *when* the same work runs. Omitting `--profile` leaves the key what it was
  before the flag existed. The blob itself is content-addressed like any
  payload and referenced from the pool's ending, never from the CAS receipt,
  whose v3 key set is an immutable interpretation domain.
- **An in-process profiler is a contract, not a monkeypatch** — `torch.profiler`
  cannot be started from outside the process it profiles, so `--profile torch`
  names a path in an environment variable and validates what the action wrote
  there, rather than injecting code into an action's interpreter. The action
  keeps its own executed contract; PrismaBuild keeps the whole ingest, budget
  and refusal path it applies to a profiler it ran itself. A mode never takes
  over a variable the action already seals, and an action that ignores the
  contract fails: a cache hit carries no `profile` key, so publishing a receipt
  for a run that produced no profile would answer every later submission of
  that key with an unprofiled hit and no reason attached.
- **A profile has a size budget, because evidence is not free** — a trace that
  fills the disk is a cost the next action pays. The budget is measured against
  the observed growth rate of the format, a mode that can bound its own capture
  offers a window sealed into the key, and a profile over the budget fails the
  run with the remedy in the message rather than filing a receipt without it.
  Torch's 2 GiB limit applies to both the stored file and decoded JSON bytes.
  Gzip decoding reads at most one byte beyond that limit and refuses before
  JSON parsing; concatenated gzip members share the same decoded budget.
  Accepted profiles report `decoded_bytes`. JSON object memory is additional;
  this validation limit does not cap trace growth on disk during capture or
  preserve a trace interrupted before ingestion. Action identity is unchanged.
- **A diagnostic must not change the shape of what it observes** — a profiled
  action's failure record is the unprofiled one: the same `returncode` and the
  same `signal`, carried out of the profiler by a relay that can distinguish a
  signalled child from one that exited 128+n. The profiler's own status is
  recorded and never silently becomes the action's; it is judged by what it
  cost, so a profiler that ends badly having produced a usable profile and a
  recorded ending is marked, not fatal, while one that produced neither fails
  through the paths that already refuse those. An action stopped by its deadline files the profile it
  had reached, marked partial and bounded by the time the pool allows a
  signalled launcher, because the run somebody profiled for being slow is the
  run whose profile matters. A validated primary profile is checkpointed in
  the attempt's status sidecar after CAS ingestion and before optional summary
  extraction or supplemental blob ingestion. Normal completion returns the
  richer record; an interrupted supplement cannot hide the saved primary.
  After successful result publication, the final profile replaces the partial
  sidecar checkpoint as well, so fallback from an unparseable launcher stdout
  retains complete evidence and supplemental references. An action that ran and
  exited nonzero never reaches publication, so it carries the same complete
  report on its `LocalActionError` instead, and the status recorder writes it
  beside the action's returncode. Without that the only profile left beside a
  failing job is the checkpoint, marked partial for a report that is complete,
  and a failing run is the run somebody most wants a profile of. Status writes
  remain best effort; a failed refresh can leave the earlier checkpoint.
  Optional `nsys stats` extraction waits at most five seconds, then terminates
  its own process group with 0.5-second TERM and KILL waits. A timeout omits
  the summary with a diagnostic and preserves the primary profile and action
  verdict. Signal unwinding also reaps that group. The wait bound does not
  bound filesystem reads, CAS ingestion, or uninterruptible kernel cleanup;
  the enclosing broker scope remains the containment authority.
  A windowed profiler also checkpoints the enriched report before waiting
  for the action. A contained deadline kills the broker scope without Python
  cleanup, so only already-checkpointed evidence is guaranteed to survive
  that path; an unfinished or not-yet-ingested trace can still be lost.
  Checkpointing a profile never publishes a success receipt for the action.
- **Effective pool placement is a parameter** — `pbrun` seals the sorted,
  deduplicated conjunction of tags that its placement rule actually returned,
  including a derived hostname pin. The normalized constraint moves the action
  key, result/stamp fingerprint, and container owner. CLI spelling, order, and
  duplicate tags do not; changing the admissible worker population does.
- **Container ownership is complete pre-owner action identity** — the Docker
  owner is a versioned digest of the normalized command, logical checkout and
  checkout identity, demand, environment (including the deployed wrapper),
  placement, task determinism, retry policy, and marker namespace. The owner
  and marker variables themselves are the only recursive exclusions. Exact
  repeats therefore share an owner, while every supported semantic distinction
  that moves the `pbrun` action key moves the cleanup namespace too.
- Re-enqueue of an existing verified key is a tested cache-hit no-op. A future
  speculative policy could build on that property, but no such enqueueing or
  superseded-key scheduler exists yet.

### Worker preflight and execution attestation

Scheduler placement is intent, not producer identity. `run-local` accepts no
`--worker-id`, `--platform-key`, or `--host-class` arguments. Before a cache
miss executes, `prismaquant.prismabuild.preflight_action` emits and validates a
`prismaquant.prismabuild.worker_attestation.v2` record bound to the action key:

- `platform_key` is derived from the live lower-case OS and machine plus the
  single visible NVIDIA compute capability, when present (for example,
  `linux-aarch64-sm121`). Heterogeneous visible capabilities are ambiguous and
  refuse.
- A pool `pbrun --measurement` derives that platform key and its executable/ABI
  toolchain from the submitter's live evidence, seals both, and implicitly adds
  the submitter's hostname to effective placement. The claiming worker derives
  its own evidence and must match. `--anywhere` is refused because it contradicts
  that host pin.
- Pool `--measurement --host-class CLASS` opts into any worker offering that
  class whose live platform, ABI, shell executable, driver and accelerator
  models match the sealed facts. It retains `platform_keyed` scope; the class
  is sealed placement intent, never an invented SLURM `host_class` attestation.
  `accelerator_models.sha256` binds the sorted device models/counts and compute
  capabilities. The live NVIDIA model and physical UUID are recorded in the
  receipt; UUID is provenance, not a requirement to use the same physical GPU.
  Missing model/UUID evidence or a failed identity probe refuses this opt-in.
  Legacy receipts and ordinary measurement keys retain their existing shape.
  Explicit `--here` still pins the host. `--anywhere` remains invalid.
  Declaring a class asserts that external command, container, Python and data
  dependencies are identical across its workers; pbrun seals its shell and
  snapshot, not the internals of arbitrary shell commands or container tags.
  Paired experiments must be complete, interleaved actions on one admitted
  worker. This option neither splits their arms nor relaxes CPU/GPU isolation.
- `worker_id` is the live hostname locally or SLURM's node name inside an
  allocation. Inside an allocation the job id is derived from the `job_<id>`
  cgroup the kernel placed the process in; `SLURM_JOB_ID`, `SLURMD_NODENAME`
  and `SLURM_JOB_PARTITION` are recorded evidence that must agree with it and
  decide nothing, because a batch script can export any variable regardless
  of `--export=NIL`. `SLURM_JOB_CONSTRAINTS` is set only for the Prolog and
  Epilog, never in a job's environment.
- A `host_class_keyed` action is SLURM-only and is attested through the
  controller: the worker runs `scontrol show job <id>` for `Partition`,
  `BatchHost` and the job's own constraint (`Features=`), then
  `scontrol show node <BatchHost>` for `ActiveFeatures`. The class is
  attested when the node carries the Feature **and** the job's constraint is
  a plain conjunction that requires it, so the scheduler enforced the
  placement rather than a worker observing it. Partitions are the resource
  axis (`all`, `gpu`, `cpu`) and never a class. The controller is retried on
  the bounded `SCONTROL_RETRY_DELAYS_S` schedule; an unreachable controller
  refuses by name and is never read as attested. Portable work inside a job
  never asks the controller. The controller's answer is recorded as
  `evidence.slurm.controller`, optional in the persisted shape so earlier
  receipts keep validating, and a receipt re-derives the class from that
  record alone.
- `pbrun --transport slurm --measurement --host-class CLASS` seals such an
  action: the class
  joins the effective placement, so the SLURM lane sends `--constraint=CLASS`
  and the action key moves with it. The submission binds the submitting
  box's argv[0] and ABI facts, as every nonportable action must, so it has to
  originate on a box of that class; a worker of another class refuses it at
  preflight, naming the field that differs.
- The resolved regular file behind `argv[0]` is hashed before execution and
  checked again before publication. Nonportable actions must bind that digest
  and byte count as `environment.toolchain.{argv0.sha256,argv0.bytes}`, plus
  the exact system, machine, and libc ABI fields. Their
  toolchain may contain only preflight-backed fields (`python`, `torch`,
  `transformers`, `vllm`, `gridbook`, OS/machine/libc, CUDA capability, NVIDIA
  driver, and the executable identity); every declared field must verify.
  NVIDIA workers additionally require the CUDA capability and driver fields.
- The worker implementation is a separate closed
  `prismaquant.prismabuild.worker_runtime.v1` object. It binds the exact
  `prismaquant/prismabuild.py` source snapshot taken once while that module
  initializes. Canonical JSON and SHA-256 are implemented in that same file,
  so the receipt-digest implementation does not escape into an unrecorded
  repository import. The live core file must still match the load-time
  snapshot at preflight, after task execution, and at publication. For the
  SLURM path, `tools/prismabuild_worker.py` snapshots its own source at the
  earliest executed wrapper code, before importing the core, and passes that
  identity into preflight. The launcher is checked there and at the same two
  later boundaries. Direct Python API calls record the explicit `in_process`
  mode and a null launcher rather than inventing a script identity.
- Every nonportable `action.inputs` digest must already exist and verify in the
  PrismaBuild CAS before argv starts. Portable actions preserve the existing
  external-input contract: CAS-resident inputs and recognized toolchain fields
  are verified when possible, while unresolved inputs and descriptive
  toolchain fields remain permitted and are visibly absent from the
  attestation's verified subsets.
- A `fleet/pbrun` cache miss additionally parses its closure stamp and
  recomputes the live checkout's Git identity immediately before argv. The
  canonical computation is one core function shared by submitter and worker:
  `HEAD`, the tracked delta, and the content digest of every untracked regular
  file or the literal link text of every untracked symlink (including members
  below a newly-added directory). The tracked delta uses `diff-index --binary`
  with external diff and text conversion disabled, preserving default keys.
  It reads the source index and object store through a temporary Git directory
  with canonical configuration, excluding personal diff drivers, attributes,
  and diff environment settings without modifying the source index. Personal global
  excludes are disabled for both the untracked roster and special-inode screen;
  repository `.gitignore` and `info/exclude` remain effective.
  Git's NUL-delimited, repository-root-relative
  untracked roster owns pathname decoding, so quotes, backslashes, and newlines
  remain literal path bytes and a requested subdirectory cannot hide a
  repository sibling. Only basenames matching pbrun's exact generated
  16-hex-fingerprint stamp/result grammar are excluded; submission migrates
  the former broad local Git globs before taking identity and refuses if that
  migration cannot be published. Once Git identifies a repository, every
  subsequent Git roster/diff error also refuses rather than collapsing a
  missing read to an empty delta. A filesystem `.git` marker at or above the
  requested cwd establishes that state before the first Git subprocess, so a
  transient initial `rev-parse` failure cannot downgrade a checkout to the
  legacy no-Git identity; a true plain directory remains supported there.
  Symlinks are never dereferenced into bytes
  outside the checkout; an untracked FIFO, socket, or other special inode
  anywhere in that repository refuses rather than being opened as an unstable
  payload, and an untracked payload that cannot be read refuses rather than
  collapsing to a reusable `unreadable` sentinel. A stamp whose bytes are
  intact but whose claim no longer matches therefore refuses before execution.
  Git checkouts are made immutable across the remaining interval by default:
  the submitter synthesizes a deterministic commit from the exact tracked
  and untracked working tree, including the closure stamp, parented on the
  source's own `HEAD`. The stamp is injected into the submitter's private Git
  index directly from its UTF-8 payload; no stamp or temporary stamp pathname
  is published into the source checkout. Its historical relative name, bytes,
  and regular-file mode are retained, preserving closure and bundle identities
  while concurrent submissions need no shared stamp lock or cleanup. Existing
  source-side stamps from older versions are left untouched. The submitter publishes its
  bundle as a verified CAS input, and puts the commit rather than the
  submitter path in the queue. Those bundle bytes are a function of the sealed
  objects alone: the pack is written with every setting that influences it
  pinned on the command line and with delta reuse off, so an unchanged tree
  seals to one action key across repeated submissions, across a `git gc` of
  the source, and across boxes. `--snapshot-ref NAME` adds a source branch to
  that bundle by name. The claimant fetches that bundle into a fresh
  worker-local checkout, runs from the original relative subdirectory, and
  removes the private tree afterward. A failed removal is warned and recorded
  under the worker's local materialization root; it never changes completed
  task work into a retry. The worker preflight requires the private tree to be
  clean at the sealed commit, to carry the recorded parent, and to resolve
  every recorded branch to its recorded id. This snapshot proof applies to
  every definition carrying `params.checkout_snapshot`, including Tessera
  producers; only the closure-stamp proof is specific to `fleet/pbrun`.
  Thus `HEAD~1` and `BASE...HEAD`
  are facts a diff-derived gate can rely on rather than a
  `fatal: ambiguous argument`. Absolute submitter-repository paths in argv or
  environment are refused because they would escape the snapshot. The lexical
  screen requires a boundary after the repository directory name, so sibling
  names such as `repo-results` remain external paths. It also checks embedded
  `--out=<path>`, quoted command strings, and colon-separated path lists. New
  submissions from non-Git directories refuse: there is no mutable-path
  override. The command executable is resolved exactly from argv[0] and the
  declared `PATH`. An executable outside the repository and shared storage
  retains the submitting host's tag; an absent executable refuses unless an
  explicit tag names the worker class that owns it. Other direct argv and
  caller-environment paths receive a conservative lexical screen, not a claim
  that PrismaBuild can parse shell/application indirection. `--tag` explicitly
  assigns those dependencies to a worker class; `--anywhere` explicitly
  asserts that they are portable. The normalized effective tags are sealed in
  action params, so a receipt produced for one placement conjunction cannot
  answer an otherwise identical submission constrained to another. Workers
  continue to understand
  already-published `checkout_root` queue records only so that the
  pre-migration queue can drain. Relative argv paths may reach repository
  siblings from a requested subdirectory because the whole repository is
  snapshotted. Active Git content transforms, gitlinks, and symlinks whose
  lexical target escapes the sealed tree (including `.git`) refuse: none
  guarantees that a parent bundle recreates the submitter's exact working
  bytes. A checkout that leaves a tracked path out of the working tree refuses
  for the same reason from the other side: `git add -A` honours the
  skip-worktree bit `git sparse-checkout` sets, so those paths would be sealed
  from `HEAD` rather than from bytes the submitter has. A shallow or partial
  clone refuses because the bundle cannot walk ancestry the source does not
  hold. The submitter's own `core.excludesFile` is pinned away from the seal:
  the repository's `.gitignore` and `$GIT_DIR/info/exclude` decide what the
  sealed tree contains, never a personal setting on the box that submits.
  The hard 512 MiB fleet ceiling applies independently to logical
  materialized bytes (summed per path) and compressed bundle bytes; a caller
  may lower but never raise it.

The supported preparation boundary is `PrismaBuildCAS.ingest_input()` or the
dependency-free `ingest-input` CLI. It takes a stable regular-file snapshot,
derives the canonical SHA-256 and byte count, optionally checks both against
caller-supplied expectations, publishes through a read-only first-writer-wins
hard link, and fsyncs the blob shard. Each ingest holds an exclusive filesystem
lock on `.staging/ingest.<random>/.owner.lock` for its complete staging lifetime.
The directory is initialized under a hidden name and renamed into that namespace
only after locking. A death during initialization can leave a hidden directory
with at most its empty marker; no payload is written before publication. Success
and ordinary refusal remove it. Cleanup enumerates from a fresh directory
file description anchored to the held inode, so prior directory stream offsets
cannot hide its ownership marker. The unlinked marker is closed before removing
the directory so NFS removes any temporary open-file placeholder first. A
source rejected before file-copy ownership transfers likewise closes its staged
payload descriptor before unlinking it.
Process death leaves
an attributable directory whose lock is released by the kernel. A reaper must
acquire the owner lock before removal; local PID absence cannot establish that
a writer on another host is dead.
The `pb_gc` operator command also collects legacy root staging copies, claims,
worker locks, and empty result staging namespaces. Applying any sweep requires
an explicitly acknowledged maintenance window with every CAS producer paused
on every host and candidate checkout roots verified absent on all hosts.
Neither file age nor local process inspection proves remote abandonment.
This is an operator prerequisite, not an automatically acquired fleet lock.
Exclusive lock probes open existing markers read-write without modifying their
bytes, because NFS requires a writable descriptor for its byte-range lock.
When deleting a private ingest, GC closes the unlinked ownership marker before
removing the directory so NFS can clear its temporary `.nfs*` name.
GC retains records, unknown entries, occupied result namespaces, and private
ingest directories whose owner lock cannot be acquired. Rechecks compare inode
identity and removal traverses directory descriptors without following symlinks.
A winning publisher reopens the canonical
name and proves that it is the exact private, read-only staging inode whose
bytes it just hashed and fsynced; it does not hash that same inode again. A
loser never trusts the other writer's inode and hashes the canonical blob in
full. `input_path()`, `verify-input`, and every public cache lookup retain the
schema, size, mode, and full-content check. A conflicting, malformed,
symlinked, truncated, writable, or changed object refuses.
This closes the code-level input-ingress gap. One narrow cross-host pilot was
run on 2026-08-30 from repository commit `5bd2d2c`: Sparky and Sparklina used
their direct stdlib launchers concurrently to ingest the same 2,601-byte
`pyproject.toml` into the fresh NFS4 CAS
`/mnt/shared/prismaquant-prismabuild-validation/5bd2d2c/input-cas-race4-direct`
on the same export with `local_lock=none`. The source SHA-256 was
`2a872eb7dfbe734920ec90e997a91460a33b725a8ab19372340e68d11f39a495`;
Sparky returned `published` in about 3.2 seconds, Sparklina returned
`already_present` in about 3.3 seconds, and both exited zero. That historical
pilot validated only the small-file concurrent input hard-link/readback case;
the larger evidence and its remaining limits are recorded below.

#### Scheduler-free live-NFS qualification (2026-08-31)

A larger wall-clock-overlap run at exact commit `568eeb4` found a real CAS
false refusal before qualification. In simultaneous identical 128 MiB input
and 8 MiB deterministic-result publication, one losing reader raised
`CASTamperError` even though the canonical bytes were correct. The retained
trace held device, inode, size, mode, uid, gid, mtime, and `nlink == 2`
constant while NFS reported ctime moving backward from
`1788148069334890999` to `1788148069099740663`. The before-run, timed reports,
and trace are retained under the `timed-overlap` and `ctime-trace` directories
of `/mnt/shared/prismaquant-prismabuild-validation/568eeb4/run-20260831T034200Z-codex-live-nfs-v2`.

Commit `7acf3ad` closes that defect without a ctime exception. A CAS read has at
most `_STABLE_FILE_READ_ATTEMPTS = 3` attempts. Each attempt resolves the path
again through held no-follow directory descriptors, opens a fresh leaf FD, and
replays the entire read or SHA-256 from byte zero. PrismaBuild accepts only an
attempt whose complete identity (device, inode, size, mode, uid, gid, mtime,
nlink, and ctime) is identical before and after the read and whose expected
byte count and content address match. A ctime/nlink-only within-read mismatch
discards that attempt and retries; a substantive identity change, wrong
content address or byte count, non-regular/writable object, changed path, or
symlink hop refuses immediately. The final canonical-file identity check reports
a missing or replaced entry as tamper, but other stat failures as unavailable.
Recovery retains work on unavailable evidence and retries the whole read; a
temporary NFS or permission fault cannot retire a finish tombstone as corrupt.
Three unstable reads refuse. Deterministic
regressions cover transient `2 -> 1` publication-link cleanup, transient
`1 -> 1` link/unlink ctime churn followed by a stable pass, perpetual `1 -> 1`
churn, and immediate content/mode/mtime/owner refusal. The focused core,
Slurm-adapter, and Dagster-adapter suite passed `174 passed, 1 skipped`.
The Slurm durable-state adapter now calls that same core primitive with
read-only enforcement and a 16 MiB bound instead of maintaining a divergent
single-pass copy. Adapter regressions separately pin transient ctime/nlink
replay from a fresh FD and immediate substantive mode/mtime refusal.

The exact-fix rerun is retained without overwrite at
`/mnt/shared/prismaquant-prismabuild-validation/7acf3ad/run-20260831T035517Z-codex-live-nfs-fix`.
Both clean checkouts reported exact
commit `7acf3adec56a44cb909297938fd6a860e0c1a78b` and identical core SHA-256
`733b1515af957e08bcb9ff2f51dba5c4e338e3cbda7330d5936e238f55acbe69`.
Sparky and Sparklina mounted the same NFSv4.2 export with `local_lock=none` and
reported NTP synchronized. Sequential clock samples placed the remote sample
0.30--0.56 seconds after the local one (including SSH latency), so races used
absolute wall-clock starts rather than NFS marker visibility.

The retained verifier (`facts/verification.json`, SHA-256
`f1e3d40eb6072244aa8817f3bdcaf31611710b1424c3d89fdf448eb9fb0324d7`)
establishes the following scheduler-free cases:

- Both 128 MiB identical-input operations overlapped, returned successfully,
  and observed exactly one winner at content address
  `254bcc3fc4f27172636df4bf32de9f107f620d559b20d760197e452b97453917`.
  Both 8 MiB identical-result operations also overlapped and returned one
  winner plus one verified hit with receipt
  `60051d82481570da096e91c643a19dd170a0c3548e4ef6385599fed360d4a302`.
- Conflicting deterministic writers overlapped and produced one result plus
  one `CASConflictError`; independent reads on both hosts agreed on the
  canonical receipt, payload SHA-256, inode, and read-only mode.
- Both continuous readers saw misses (`592` and `559`) before publication,
  then each completed 100 fully verified hits with no hit-to-miss or identity
  regression. Both parent rename-to-symlink traps raised `CASTamperError`, and
  the outside directory remained empty.
- With retained fake executables only, concurrent Slurm adapters produced one
  `submitted` and one `adopted` result for the same job, unique poll ordinals 1
  and 2, and one successful cancel claim plus one fail-closed concurrent
  refusal. The command log contains exactly one `sbatch`, one `sacct`, two
  `squeue`, and one `scancel`; no real scheduler was contacted. Both durable
  readbacks were identical except for the client-local NFS device number.

The final 173-file manifests read independently on both hosts are byte-identical
at SHA-256
`ba8f70879b8f198ed989335172399a51cfbc4c0189be4444d9b1df2425a29f06`.
This qualifies the exercised scheduler-free CAS and durable-state races on the
current NFS mount. It does not qualify host/power loss at a durability
boundary, ACL/WORM retention, a production-scale result, real Slurm services
or allocation identity, a Dagster daemon, GPU execution, or deployment.

The dependency-free launcher for a bare host is direct script invocation, for
example `/usr/bin/python3 /path/to/prismaquant/prismabuild.py ingest-input ...`.
That form ran on both pilot hosts. `python -m prismaquant.prismabuild` first
executes `prismaquant/__init__.py` and therefore requires the installed
PrismaQuant environment; on bare Sparklina system Python it failed on the
package's `compressed_tensors` dependency before reaching the stdlib-only
core. The module form works in the `pq-cu130` environment, but must not be
advertised as the dependency-free launcher.

Two limits are explicit. For portable actions the observed executable hash is
receipt provenance, not a newly required action-key field; callers that need
the executable to participate in cache identity must use a nonportable scope
and the `argv0.*` toolchain fields. Input preflight proves that the declared
CAS bytes exist and match before execution, but the sealed argv/code remains
responsible for resolving and consuming those bytes; process provenance is not
an OS-level proof of every file read. Worker core/launcher identity is likewise
receipt provenance, not action-key identity: a cache hit retains the producer
revision that created its canonical result. Separately, Slurm submission intent
v2 seals a self-hashed runtime object containing the exact loaded adapter-module
bytes and configured worker-launcher bytes. Both are rehashed after the durable
intent readback and immediately before `sbatch`; path or byte drift refuses
without invoking the scheduler. This does not put transport code into the
reusable action key or action request. The scheduler cannot retain the submit-
host FD while a job is queued: the started wrapper therefore still records and
rechecks its own earliest live snapshot, and that receipt provenance may
visibly differ from the submission-time planned launcher if deployment changed
after `sbatch`. Python does not expose the already-started script's parser input
buffer, so even that early snapshot is not a cryptographic proof of the exact
bytes the interpreter parsed.

The attestation becomes `producer` in the self-hashed
`prismaquant.prismabuild.cas_receipt.v3` receipt. CAS lookup replays its action,
scope, platform derivation, host-class evidence, worker core/launcher identity,
task-executable identity, verified toolchain, verified-input subset, and self-
digest before accepting the result. V3 receipts use
`actions/v3/<prefix>/<action-key>.json`. Legacy v2 receipts retain their
immutable unversioned `actions/<prefix>/<action-key>.json` addresses: v3 lookup
does not parse them as v3, overwrite them, delete them, or silently migrate
them. A v2-only key is a v3 cache miss and must be recomputed under the new
producer contract.

Receipt publication fsyncs the candidate, runs the potentially longer
action-closure and executable callback first, then rehashes the core and
launcher as the final userspace check before the first-writer-wins hard link.
There remains an unavoidable sequential interval between that final check
returning and the `os.link` syscall; this design minimizes that interval rather
than claiming a zero-gap filesystem snapshot.
Result-blob publication uses the same consumed-inode rule as input ingestion.
The successful publisher already computed SHA-256 while copying into a
private read-only staging inode. After the canonical hard link is durable, it
reopens that name and requires exact device/inode and substantive metadata
agreement. A mismatch refusal records both already-observed identities (device,
inode, size, mode, UID, GID and nanosecond mtime) in the exception so the failing
field can be diagnosed without a later filesystem read. This evidence does not
relax the comparison or turn the refusal into a retry.
Receipt readback can then validate the canonical receipt without a
second or third payload hash. If another blob or stochastic receipt won, every
unconsumed winning blob is hashed normally. Returning a path immediately from
that successful publication reuses this proof; later `lookup()` and
`result_path()` calls always consume and hash the canonical payload anew.
The before/after 2 GiB and 256 MiB NFS measurements and their limits are in
`docs/results/prismabuild_publish_io_2026-08-31.md`.
CAS staging, blob, request, and receipt directories are walked or created only
through held `dir_fd` values with `O_NOFOLLOW`; new components use `mkdirat`
semantics and are fsynced after their final mode is applied. Hard links,
readback, hashing, and cleanup are relative to those held descriptors. Before
accepting a read or completed publication, PrismaBuild reopens the configured
parent path and canonical leaf and verifies their device/inode identities.
Thus an ancestor rename-to-symlink race fails closed and never redirects a CAS
read, write, or unlink outside the configured root. The Slurm worker likewise
accepts only the canonical `requests/<prefix>/<action-key>.json` address and
reads it through this anchored path after its restart guard. These guarantees
depend on Linux `openat`/`O_NOFOLLOW` and `/proc/self/fd`; a returned payload
`Path` is only evidence of the just-verified name, not a file descriptor held
open for an arbitrary later consumer.

The live-checkout output path has a separate recovery contract. A canonical
`prismaquant.prismabuild.local_result_claim.v1` record below
`local-results/v1/` binds the action key, full action-manifest digest, resolved
checkout, normalized working directory, and normalized result path. It is
durable before argv starts. A matching retry first holds the existing
checkout/output lock, validates the claim byte-for-byte, rejects symlinks and
non-regular paths, unlinks only the claimed leaf, removes at most 64 permitted
same-UID files from the claim-private result-staging directory, and reruns argv
to produce a new attestation. An unclaimed dirty result remains a hard error.
A valid CAS receipt also blocks explicit repair; repair repeats that lookup
after acquiring the output lock so a receipt published while it waited wins.
Claims are retained as immutable recovery authority; they are not success
records and cannot satisfy `lookup()`.

The checkout/output `flock` is also an action-lifetime lease. The worker passes
the exact locked open-file description into task argv, so abrupt worker death
does not release exclusion while that direct task process can still write its
declared result. A retry waits; after the orphan exits it removes only the
exact claimed result and recomputes. If a handled worker exception such as
`SIGINT` unwinds Python, the worker terminates and reaps the task's complete
new-session process group before its context manager closes the worker's lock
descriptor. It never explicitly unlocks the shared open-file description: if
the task cannot be reaped, its inherited descriptor retains exclusion.
Regressions kill the worker both during argv and after result staging. This is
not a kernel-enforced sandbox: task code that deliberately closes inherited
descriptors or escapes its process group violates the local-action contract,
and an indefinitely uninterruptible task can retain the lock indefinitely.
The proposed Slurm deployment's cgroup and sealed time limit remain required
external containment; PrismaBuild has no durable local holder lease or
lock-acquisition timeout today.

#### Opt-in two-phase initial-miss rendezvous

The qualification hook is called immediately after the ordinary first
`PrismaBuildCAS.lookup(action)` returns `None` and before checkout/output path
resolution or `_local_output_lock`. Its immutable, canonical, self-hashed
`prismaquant.prismabuild.initial_miss_rendezvous_manifest.v1` file binds one
normalized non-root absolute rendezvous namespace, the exact normalized
non-root absolute CAS root used by both workers, a 128-bit lowercase-hex run
nonce, the exact action key, exactly two sorted unique lowercase hostnames, and
a positive finite local-monotonic timeout no greater than one day. The manifest
must be a read-only regular inode with exactly one link and is read through the
existing bounded no-follow stable-file primitive. The namespace has exactly
the `arrivals/` and `ready/` directories; each phase admits only the two exact
`<hostname>.json` leaves (plus a bounded transient private publication name).

For participant `i`, let `M_i` be its initial verified miss. It derives its
hostname from `socket.gethostname().lower()`, and a self-hashed process identity
from hostname, PID, Linux `/proc/<pid>/stat` start tick, a fresh invocation
nonce, and the existing exact loaded-core plus optional launcher runtime
identity. It no-clobber publishes an immutable
`initial_miss_rendezvous_arrival.v1` record only after `M_i`. Both workers wait
for and validate the exact complete arrival set while refusing a CAS receipt.
Each then makes a fresh CAS-absence observation and publishes an immutable
`initial_miss_rendezvous_ready.v1` record that binds its process/arrival and the
canonical digest of that complete arrival set. The same absence/runtime check
is the last userspace callback before the ready hard link. Only the exact
complete ready set releases either worker to the output lock. Thus, for both
participants, `M_i < arrival_i < ready_i < release < any in-protocol result
publication`; no cross-host wall clock or realtime timestamp participates in
the proof. A worker stopped after release may resume later and return the
ordinary post-lock cache hit, carrying the same proof.

Arrival, ready, manifest, process, and returned
`initial_miss_rendezvous_receipt.v1` objects have closed schemas and canonical
self-digests. Wrong action/run/manifest/host/runtime bindings, corrupt digests,
duplicate/replayed participants, missing or extra entries, writable files,
symlinks, special files, persistent hard links, source drift, and a CAS receipt
visible during either worker's pre-release checks fail closed. The protocol
does not claim atomic exclusion against an out-of-protocol receipt linked in
the interval between the last ready pre-link callback and the ready link;
workers released by that link may legitimately return proof-bearing hits.
Polling and directory cardinality are bounded and use
`time.monotonic()`; an unavailable peer times out without task argv. The second
ready link is the logical release event. If the CAS receipt becomes visible
while NFS still returns an incomplete ready-directory view, the worker keeps
performing bounded exact scans through the original monotonic deadline; this
does not misclassify a legitimate peer publication after release.

This is an integrity protocol inside PrismaBuild's existing cooperative
filesystem trust boundary, not authentication or a Byzantine quorum. SHA-256
self-digests detect corruption and cross-contract mismatch but are unkeyed. A
principal able to write arbitrary correctly formed files as another hostname,
or to remove namespace entries, can fabricate or suppress the evidence.
Qualification therefore requires the same isolated worker principal and
ACL/WORM/retention controls already required for CAS and Slurm state. The hook
has CPU-only hostile tests. Its exact V4 two-host run on `gx10-6b77` and Sparky
also passed the configured shared-NFS causal path for source commit `452c6f6`;
the frozen command, authority hashes and post-run verification are recorded in
`docs/results/prismabuild_two_host_qualification_2026-08-31.md`. That does not
qualify other mounts, host/power loss, live Slurm, daemon deployment, or a
hostile namespace principal.

The `preflight` CLI prints the same machine-readable record without executing
the action. This is process/platform provenance, not a cryptographic quote. In
the target deployment, the trust boundary would be the munge-authenticated,
cgroup-enforced cluster and its shared CAS; that boundary is not live today.

**Intended restart economics + provenance (Rob, 2026-08-26).** Unit-tested
local/CAS semantics are designed so a rerun can become a replay: after a
failure or code fix, re-enqueueing a campaign should return cached results for
unchanged keys and recompute only what the edit invalidated. No end-to-end
SLURM/Dagster campaign replay has run, so this is not a measured deployment
claim. The stage-7 trellis chain is a motivating counterexample from the
pre-PrismaBuild workflow: its contract bound one closure over the whole chain,
so each of the eight 2026-08 re-arms re-ran plan + preflight + calibration
(~10 min each) even when the edit touched only the spotcheck gate. Four
calibrations were byte-identical to v2; that supports the value of finer task
closures but does not prove the timing or reliability of a deployed
PrismaBuild replay. The key is also intended as provenance: hash(inputs, code
closure, params, env) is machine-checkable identity, and deterministic-class
entries can be audited by recompute-and-compare.
Honest caveats: stochastic tasks (probe backward is recorded
non-bit-reproducible) get run-once/first-result-wins — their entry is the
*canonical* result, pinned but not re-derivable; and a cached measurement is
valid only under its exact nonportable scope. A pool measurement retains its
platform, toolchain and host placement by default. An explicitly class-scoped
pool measurement retains class placement, platform/ABI and device-model facts;
a result's selected host and GPU UUID remain recorded producer provenance.
A changed class, architecture, device model or declared toolchain changes the
action key. A cache hit is the same recorded experiment, not a fresh measurement
of the querying host. A SLURM measurement retains its host
class (a gb10 KL never answers an x86 query).

### Durable SLURM submission, polling, and cancellation (superseded, never live-validated)

This section describes `src/prismabuild/slurm.py`, the adapter written before
any scheduler existed on the fleet. The lane in `src/prismabuild/slurm_lane.py`
replaced it on 2026-09-04 with a smaller contract (submit, wait, cancel, and
the pull queue's terminal records) that has run against a real controller.
The adapter stays in the tree until the decision record's Phase 3 removes it.

Scheduler identity is shared CAS state, separate from result truth. For each
action the adapter owns one immutable lineage:

```text
submissions/v2/<action-prefix>/<action-key>/
  intent.json
  job.json
  transitions/polls/00000000-<ordinal>.json
  mutations/<ordinal>.json
```

`intent.json` uses `prismaquant.prismabuild.slurm_submission_intent.v2` and
contains a `prismaquant.prismabuild.slurm_submit_spec.v2`. It
seals the action key; one cluster; CAS, log, checkout, worker, and SLURM
executable paths; the closed submit environment; resources and placement; and
the exact `max_polls`, `poll_interval_seconds`, and zero-`max_requeues` policy.
It also seals the complete canonical worker argv.
The submit spec also carries a self-hashed
`prismaquant.prismabuild.slurm_runtime.v1` record for the load-time adapter
source and configured worker-launcher source. The runtime launcher's declared
path must exactly equal the sealed worker path, and both source identities and
the runtime digest are strictly validated. Existing `submissions/v1` records
remain immutable history: the v2 adapter neither parses nor migrates them.
SLURM recompute is also sealed false. Its canonical submit-spec digest
derives both the full `pqb-<digest>` job name and
`prismabuild:<digest>` comment. The read-only first-writer object is published
and re-read before `sbatch`; a changed resource, path, environment, or retry
limit on replay conflicts rather than creating another lineage.

The worker is deliberately not passed as `sbatch`'s positional batch script:
Slurm copies such a script into its spool, so the executed launcher's
`__file__` becomes a spool path and its checkout-relative core import fails.
Instead the adapter supplies one `--wrap=exec <command>` option. The command is
derived only from the sealed worker argv with `shlex.join`; tests round-trip
spaces, quotes, dollar signs, and semicolons through `shlex.split` and assert
there is no positional script. This is a controlled POSIX-shell encoding at
the Slurm boundary, while the submitting process still uses `shell=False` and
the task's own argv remains inside the immutable JSON request rather than shell
text. A real Slurm launch of this path remains part of deployment validation.

Submission uses `--no-requeue`, and positive same-job retry is deliberately
unavailable. This is not a temporary command-line combination: current Slurm
uses the job's single Requeue eligibility flag for both explicit
`scontrol requeue` and automatic/site/admin restart. `--no-requeue` therefore
also makes explicit requeue ineligible; changing it to `--requeue` would admit
restarts that have no durable PrismaBuild authorization. The adapter and
Dagster `ActionSpec` reject `max_requeues > 0`, and `SlurmAdapter.requeue()`
never sends `scontrol`. The batch argv adds `--require-slurm-initial-start`;
before `run-local` can reach task argv, the worker requires a real numeric
`SLURM_JOB_ID` and absent-or-zero `SLURM_RESTART_COUNT`. A malformed or nonzero
count refuses even if a site administrator overrides the submission policy.
Positive retry can return only with a new protocol that binds Slurm's actual
restart counter/`Restarts` state to an authorized durable mutation claim.
The v2 submit spec still seals the configured absolute `scontrol` path even
though this zero-requeue protocol never executes it. That field is inert and
over-broad provenance, not a hidden retry path. Removing it would change the
canonical submit-spec digest and therefore requires an honest later schema/
namespace boundary; D9 deliberately does not reinterpret v2.

It also uses `--export=NIL`, not `NONE`: current Slurm defines `NONE` to invoke
the implicit `--get-user-env` path, whereas `NIL` passes only scheduler/SPANK
variables to the already-required absolute worker path.

`sbatch --clusters=<sealed-cluster>` restricts submission to one cluster rather
than creating federation siblings. After `sbatch --parsable` returns,
`job.json` binds that intent to its exact cluster-qualified job id; clusterless
output is normalized only because the submission already selected exactly that
one cluster, and a different returned cluster refuses. A restart first reuses a
valid binding. If the process died after scheduler acceptance but before
binding, recovery queries `sacct --clusters=<sealed-cluster>` from the Unix
epoch by the sealed name, requests widths large enough not to truncate the
name/comment, and binds only one allocation row whose name, comment, and
cluster all match. Both adoption and bound accounting queries request
`--duplicates`: Slurm documents duplicate records after requeue, federation,
resize, or JobID rollover, so hiding all but the newest row could hide an
ambiguous allocation lineage. Zero rows are ambiguous between a pre-`sbatch`
death, accounting lag, or retention loss; multiple rows, malformed rows,
identity drift, and unknown states also refuse without another `sbatch`.
Bound state queries are cluster-qualified; the helper's only clusterless query
form forces `--local`, preventing federation display defaults from silently
widening it.

The poll budget and cadence survive process loss. Each canonical self-digested
poll record includes its append wall-clock nanoseconds. Ordinals must be a
contiguous prefix, the count may not exceed sealed `max_polls`, and a restarted
adapter waits out the remaining sealed interval before winning the next
first-writer claim. The interval is positive, finite, and capped at one day;
the poll and filename counters are bounded to their eight-digit durable
representation. Clock rollback refuses. A crash after a poll claim
conservatively consumes it. This wall-clock protocol still requires deployed
hosts to have bounded synchronized time; that is a live gate below.

Poll replay is accelerated only by a process-local, non-authoritative snapshot.
The first access in an adapter process (and therefore every adapter restart)
replays and validates the complete append-only retry journal. An uncontended
subsequent claim revalidates the exact canonical durable tail, preserves its
timestamp for pacing, and attempts the next first-writer publication in O(1)
journal work. A read-only progress query also probes the one expected successor;
if another writer advanced it, the adapter invalidates the snapshot and derives
a new one by full replay. A lost publication race follows the same invalidation
and full-replay path before it refuses the loser. Striped process-local `RLock`s
serialize threads sharing one adapter for the same action without globally
serializing distinct actions, but are not—and are not presented as—cross-host
filesystem locks. Cross-host serialization remains the no-clobber append
itself. No durable record is compacted, rewritten, or deleted.

That optimization is deliberately unavailable as terminal authority. Poll
budget exhaustion, scheduler terminal resolution, cancellation, and success
of an action with a durable Slurm intent all discard the cached view and audit
the complete prefix before returning. Thus deletion/corruption hidden behind a
still-valid tail can allow another nonterminal observation, but cannot license
success, failure, cancellation, or budget exhaustion. Latest-step deletion or
replacement refuses immediately in the hot loop. Complete replay remains the
crash/restart recovery mechanism; the cache contains no state that must survive
a process loss.

The Slurm module contains no bare Python `assert` invariants. Durable-schema,
runtime-identity, retry-policy, attempt-bound, cache-reconstruction, placement,
worker-argv, and anchored-path assumptions all raise explicit contract,
tamper, protocol, or local-action exceptions. An AST regression pins that
property, so `python -O` cannot erase a refusal check.

`SlurmAdapter.resolve()` itself remains an unbudgeted scheduler-observation API:
it does **not** consume a poll claim. The native `DagsterActionRunner` enforces
the intended pairing by calling `claim_poll()` immediately before each
`resolve()`. A caller that invokes `resolve()` directly can issue scheduler RPCs
without consuming the sealed `max_polls`; PrismaBuild does not yet implement a
claim token or exact claim-to-observation consumption contract for that API.
Accordingly the current bound applies to the native orchestrated loop, not to
arbitrary direct `resolve()` calls. This is an explicit remaining contract gap,
not a throughput claim.

The exact merged-source CPU profile at 4,000 retained poll records is immutable
under `/home/rob/dq-runs/prismabuild-d9-poll-cache-merged-20260831` (manifest
SHA-256
`189e7ddc8d9c56ff546954c5ab7a09312c5b492137ce5a10c1706cdeee416037`,
comparison SHA-256
`265db8e2e3871b5d274ba009ed39c447fdd31c93d1ce1b27b54770e47766a65d`).
Against exact pre-change commit `a71680c`, eight claims after one warm replay
on merged commit `825120c` fell from 13.003521138 s to 0.053521276 s
(242.96x), `/proc/self/io` read syscalls from 128,226 to 162 (791.52x fewer),
and `rchar` from 27,068,601 to 67,465 bytes (401.22x less). First complete
replay remained 1.597760866 s versus 1.603223845 s, as required for
restart/audit semantics. The before/after hot cProfile artifacts have SHA-256
`5650df4b43aa8bf0698dd35b5ce6deb4ffb4dacc37a78818a3cbe9d569ff3d76`
and `1ce2aadc0fdbc8715d5b5141b8cda93fc784124444832f37b3dc2255bb09de6e`.
Raw `system.cpu`, `system.io`, `system.net`, and `nfs.rpc` Netdata windows from
both active `gx10-6b77` and Sparky are included and individually hashed by the
manifest. This is a local-filesystem CPU microprofile with host-context
telemetry; it used no GPU or live Slurm service and does not qualify shared-NFS
latency, a daemon, an allocation, or deployment.
The final core/Slurm/Dagster/docs-focused suite passed `215 passed, 1 skipped`;
the skip is the existing optional-dependency boundary.

Scheduler mutations use one append-only ordinal journal. The active protocol
emits only `cancel`: after proving the exact bound allocation is active, the
adapter first-writer-claims the next mutation ordinal, rechecks receipt and
scheduler identity/state, and issues at most one cluster-qualified `scancel`.
Cancel must be the unique final mutation. Concurrent contenders have one
winner. A crash, timeout, or command error after the claim is deliberately
ambiguous; a restarted caller sees the final claim and never replays the RPC.
The schema admits a future `requeue` kind so cancel and retry cannot race in
separate journals, but the current zero-requeue policy rejects any such record.
SLURM state directories are created component-by-component relative to held
directory descriptors (`mkdirat` semantics), and reads, listings, and
first-writer publication use `O_NOFOLLOW`-anchored directory descriptors.
New directory entries and final read-only file modes are fsynced before use.
Symlinked directory hops, noncanonical/writable files, gaps, wrong ordinals,
excess counters, wrong identities, and self-digest mismatches refuse. This is
a Linux/procfs implementation contract: atomic temporary publication uses the
held parent through `/proc/self/fd` rather than reopening its pathname.

This remains one allocation lineage per action. `recompute=True` is de-menued
in the SLURM adapter: launching a second allocation after the canonical receipt
exists would make resolve/cancel short-circuit on that old receipt and strand
the new allocation. The adapter never silently creates a fresh job or retries
a terminal one. Repair after an irrecoverably ambiguous or exhausted lineage
is an explicit operator/schema action. The verified CAS receipt remains the
sole result authority throughout.

### Optional Dagster orchestration (implemented, not deployed)

`prismaquant.prismabuild_dagster` is an optional-import adapter over the core
and SLURM resource layer; importing PrismaQuant still does not import or require
Dagster. `ActionSpec` binds the sealed action, checkout, exact SLURM resources
and placement, zero-requeue/durable-poll policy, and content-addressed upstream
dependencies. An edge is the tuple `(upstream action key, downstream input id,
result sha256, result bytes)`. Graph construction refuses an edge unless that
tuple is also present exactly in the downstream action's sealed `inputs`, and
uses a key-sorted topological order.

Native definitions use one asset key per action key and set `code_version` to
that full key. The resource requires the single cluster name in addition to
CAS/log/worker paths. Dagster-level retries and same-job requeues are disabled.
`DagsterActionRunner` passes the action's poll maximum and interval into the
sealed pre-submit identity, accepts either a new submission or adoption, loads
durable progress, and lets the adapter pace and atomically claim every poll. A
fresh runner therefore continues the remaining limit instead of resetting a
Python counter. On poll exhaustion it cancels only the bound allocation and
then re-reads the CAS, so a receipt published concurrently with a no-op or
completed cancellation wins the race. A cache hit, upstream
dependency, or successful SLURM resolution is accepted only after an independent
`PrismaBuildCAS.lookup()` verifies the exact receipt, producer scope, and blob
bytes. Dagster run and materialization state are views of CAS truth, never
certification themselves. The optional package extra is
`prismaquant[prismabuild]` (supported `>=1.13,<2`, checked against 1.13.20); no
daemon, webserver, workspace, or scheduler installation is performed by the
repository. A native-import package-level run against Dagster 1.13.20 passed
all 18 adapter tests on 2026-08-31 (14 Torch deprecation warnings):

```bash
PYTHONPATH=/home/rob/venvs/pq-cu130/lib/python3.12/site-packages /tmp/pq-prismabuild-dagster-1.13.20/bin/python -m pytest -q tests/test_prismabuild_dagster.py
```

This is adapter compatibility evidence, not a daemon, workspace,
materialization, or restart pilot.

### Remaining live-deployment gates

The state machine above is covered by mocked crash/restart/corruption tests and
the scheduler-free two-host NFS race above; it has not submitted, adopted,
polled, or cancelled a live SLURM job.
Deployment still requires all of the following evidence:

- The fake-command race above covers shared-CAS `intent.json`, `job.json`, poll
  claims, and the unified mutation journal. Host/power-loss injection around
  every file, link, directory, and scheduler-RPC boundary remains required.
- Live `slurmctld`/`slurmd`/`slurmdbd` behavior with accounting configured to
  retain job comments (`AccountingStoreFlags=job_comment`) for longer than the
  adoption horizon. Exact `JobName`, `Comment`, `Cluster`, allocation-only
  filtering, state widths, single-cluster selection, and permissions must be
  verified on the deployed version. Purged accounting leaves an
  intent-without-binding deliberately ambiguous; mutable/absent comments fail
  adoption. Slurm permits an authorized user to mutate stored job comments,
  including after completion, so the deployment must isolate the submission
  principal or independently audit `Comment`/`JobName` mutations; these
  scheduler fields are discovery evidence, not a cryptographic identity.
- Crash injection immediately before/after `sbatch`, binding publication,
  cancel-journal publication, and `scancel`, including accounting lag, stale
  terminal jobs, numeric job-id reuse, command timeout, and concurrent
  orchestrators. No live SLURM validation is claimed.
- A live forced/admin restart must demonstrate that `SLURM_RESTART_COUNT` is
  present and nonzero before the worker reaches task argv and that
  `--no-requeue` blocks ordinary restart. Positive same-job retry remains
  unavailable; a future implementation additionally needs trustworthy
  scheduler `Restarts` reconciliation and exact claim-to-worker binding.
- NTP/chrony monitoring must bound wall-clock offset between orchestrator hosts
  because durable poll pacing uses append timestamps and refuses rollback.
- A real Dagster daemon/webserver restart and concurrent-run pilot proving that
  Dagster-level retry settings cannot escape through a second submission and
  that the durable poll budget and pacing interval are preserved.
- Storage retention and access control for the active submission namespace.
  Read-only hard-linked files plus strict semantic/canonical validation reject
  malformed, conflicting, writable, symlinked, and gapped state, but this is
  not a keyed or WORM log. Individual Slurm state records are capped at 16 MiB,
  and history/temp entry counts are bounded before loading their contents. An
  authorized directory owner can still unlink the final member of an
  append-only prefix; preventing or auditing that tail deletion requires the
  deployed filesystem/ACL/backup policy.
- The orchestrator hosts must expose Linux `openat`/`O_NOFOLLOW` semantics and
  `/proc/self/fd`, and the worker/Slurm executable path ancestors must be
  immutable to the submitting principal. The adapter seals and rehashes the
  worker leaf immediately before submit, but a later scheduler launch cannot
  retain that submit-host file descriptor across the allocation boundary.
- Production-scale large-result NFS publication, host-loss directory
  durability, munge/cgroup worker attestation, launcher deployment, and the
  production-run Netdata/Prometheus evidence on both boxes remain separate
  gates. The D9 CPU microprofile's two-host context windows are not that live
  scheduler/production telemetry qualification.
- The local SIGKILL test proves deterministic cleanup while `flock` exclusion
  is available in one filesystem/process environment. Deployment must still
  prove that the shared checkout mount enforces that lock across hosts and
  inject host/power loss at the claim, unlink, staging-reap, and recompute
  fsync boundaries.

## Proposed speculative tier (not implemented)

There is no `speculative` field, idle-hardware router, or disk-budget policy in
the current action schema or adapters. The following is target behavior for a
future scheduler policy, not a capability of the tested implementation.

The probe is the true DAG barrier — everything decision-relevant consumes
its outputs. Input-complete before it, enqueue-able the moment a model's
tensor inventory exists:
- RTN-tier renders + weight-space error tables (importance-independent;
  real pipeline inputs: legality/fallback/statistics).
- Candidate GENERATION under weight-only scores (doctrine-legal proposals).
- Staging, hashing, FP8 source-map verification, census metadata,
  page-cache pre-warm.
Such actions would be marked explicitly, routed only to idle non-gold hardware,
and governed by a disk budget (≥10 % free is non-negotiable). The spelling
`speculative: true` is illustrative, not a currently accepted schema field.

## Memory-pressure hypothesis (not live-validated; Rob, 2026-08-26)

The adapter emits SLURM `--mem`, but this repository has not validated a live
controller/cgroup configuration or GB10 unified-memory accounting. With
correctly requested limits and a correctly configured cluster, cgroups should
isolate an over-budget job instead of letting the kernel OOM-kill an unrelated
victim. The current code and mocked tests do **not** establish that work which
does not fit is never placed, that requested limits are correctly sized, or
that GPU allocations in GB10's unified physical pool are isolated. Those
claims require a live allocation plus cgroup and Netdata evidence. Lowering
worker counts/capacity per node is the intended allocation-time knob, not a
reactive userspace monitor like the recorded Ray landmine.

Even a validated scheduler limit would not retire the **intra-job** LRU: layer
streaming exists because one task's working set (a 328 GB model through a
128 GB box) exceeds physical memory, and no scheduler shrinks a model. What
sharding may buy: per-layer/per-tensor tasks have few-GB working sets, so as
heavy stages shard, the OS page cache plus dl380's 300 GB NFS backing may
absorb re-reads and shrink the LRU's role. The floor that remains:
order-dependent monolithic forwards (the sequential probe on a 314B teacher)
keep streaming regardless.

## Target boundaries that do not move

- **Certification stays PrismaQuant's.** Shipcards, fail-closed gates, receipts,
  and provenance stamps run inside dispatched jobs. The
  orchestrator would schedule and remember; it would never certify.
- `run-pipeline.sh` remains the intended per-run executor (v0: one task = one
  pipeline run; later versions may shard heavy stages: per-point KL,
  per-tensor encodes, per-expert measurements, parallel coord-descent). Live
  stages execute through the pull queue; no live run currently uses a
  PrismaBuild SLURM or Dagster job.

## Rejected alternatives (with reasons)

- **Airflow / k8s**: ops weight, time-oriented, poor measured-KL branching.
- **Ray**: recorded unified-memory landmine (OOM monitor kills ranks on
  GB10); runtime-env sync across three architectures.
- **Bazel directly**: right cache semantics, wrong job model — no honest
  representation of long exclusive-GPU jobs; hermeticity dies on 328 GB NFS
  inputs; BUILD-file loop taxes research-pace code churn; cache presumes
  determinism our probe lacks. We take its action-key discipline, not the
  tool. ("Bazel's cache discipline on SLURM's job model.")
- **Snakemake** (as the DAG layer): file-native and simple, but weak live
  observability and mtime/param triggers rather than content keys; loses to
  Dagster on the two requirements Rob weighted hardest. Remains the
  fallback if the Dagster–SLURM seam proves painful.
- **Roll-your-own queue dir**: explicitly declined by Rob 2026-08-26.

## Sequencing

> **2026-09-04.** Step 1 below was never executed: no box ever had `sbatch`,
> and `pool.py` filled the gap as the sole execution plane. The review of that
> day found the pull queue to be the "roll-your-own queue dir" declined above
> and recommends carrying out step 1 now, with a thin `pbrun --transport slurm`
> lane instead of the Dagster seam; see
> `docs/scheduler_decision_2026-09-04.md` for the evidence, the alternatives
> (HTCondor is the runner-up), the per-issue dispositions and the migration
> plan. It proposes; Rob ratifies.

1. (May precede GLM v1, CPU-side only) Minimal SLURM: controller on dl380,
   slurmd on both Sparks, `interactive` reservation on sparky; drive
   existing scripts via sbatch unchanged.
2. After GLM v1: observability stack; Dagster pilot on the speculative tier
   (GLM RTN render sweep = shakedown asset); family nodes join as
   `rocm-16g`/`strix-32g`.
3. Then: shard heavy stages; GLM/Qwen validation fan-outs as the first
   production campaign on the full stack.

### Sealed execution budgets

An explicit `pbrun --timeout-s` is sealed as `params.execution_timeout_s`, a
positive finite number of seconds. Its value participates in the action key.
The pool reads and validates this value from the CAS request, not the mutable
queue record, and applies the shorter of it and the worker's timeout ceiling.
Without the field, existing actions retain the worker ceiling. The pool starts
its monotonic budget immediately before launcher spawn, after checkout
materialization, withdrawal checks, scope preparation and status-file cleanup.
Those prelaunch operations and queue waiting do not consume it. Once launch
begins, blocked heartbeat or telemetry operations still consume the budget;
stalled-execution accounting remains unqualified under issue #234. Communication
waits are capped by the remaining budget independently of lease-heartbeat cadence. Expiry uses the
existing bounded process-group termination and timeout receipt path. SLURM
continues enforcing the submitter budget through its scheduler time limit.

The versioned fleet configuration sets both GB10 worker ceilings to 86400
seconds for dependent full-model calibration capture (issue #385). The CPU
host retains its 3600-second ceiling. A GB10 action without an explicit budget
inherits the one-day ceiling; capture requests that budget explicitly. This
changes only the permitted duration: reservations, physical memory guards,
containment, priority and admission are unchanged. Supervisors adopt the
published configuration through the existing idle-worker transition; a live
attempt retains the ceiling under which it started.

## Fleet durability and terminal publication (2026-09-05)

The two NFS client exports on dl380g10 now use `sync`, with ZFS
`sync=standard`. This removes the known asynchronous-export acknowledgement
exception; it is verified configuration, not a power-loss test or a hardware
durability claim. The server's `/mnt/shared` is a persistent bind mount of
`/storage_pool/shared`, ordered after ZFS mounting.

SLURM summary writers serialize each key's generation comparison and atomic
publication with a permanent POSIX lock file under `.summary-locks/`, plus
in-process thread exclusion. Newer sibling terminal states also prevent an
older ending from landing. POSIX lock exclusion was verified in both directions
between sparky's NFSv4.2 mount (`local_lock=none`) and the server-local ZFS path.
Do not unlink lock files while publishers can run. Mounts with local-only
locking are not supported for this contract.

`pbsweep --apply` reconciles unwatched SLURM endings. Keep campaign manifest
re-run recovery: it also resubmits unfinished work and supports the pool,
whereas a sweep only files an authoritative ending. Sweep before re-running a
SLURM campaign to preserve its execution record. An unknown job without a CAS
receipt remains unresolved; neither recovery path invents success.

## Automatic client convergence

The published immutable runtime is the desired client version. Worker loops
reload at an idle boundary for every generation, including a republish of the
same commit. Locally installed privileged clients converge through a root timer
that verifies manifest members, closes new admission under the broker lock,
waits for active scopes, and validates the replacement before reopening work.
An interrupted or unhealthy replacement restores verified previous bytes.
The maintenance gate records its holder; a named drain can be released only by
that holder or an explicit forced release that records both identities. Repeated
begin requests preserve the original reason and timestamp. The updater names
itself `client-upgrade` and leaves another named holder's drain in place, including
when its installed files are already current. For rolling client compatibility,
unnamed legacy drains remain releasable by any root caller, and the updater sends
ownership fields only after a broker status reply advertises support. The gate
retains its v1 schema so older brokers can read it; ownership protection requires
adoption of the newer broker and updater.
A loop that parks on a drain records that it parked, one file per process per
drain under `/run/prismabuild/rollout/parked/`, named for the gate's
`changed_unix` so a marker left by an earlier drain reads as the earlier drain.
The marker also carries the PID and procfs start time. The write is best effort,
so a loop that cannot record its park still parks and missing evidence cannot
certify a drained host. The updater creates the directory for the unprivileged
worker uid and reports whether every serving process has a marker for the
current drain and the broker reports no active scopes. Processes count by argv
basename, including a one-shot invoked through the symlink or a local checkout.
Unreadable or malformed process evidence prevents a positive result and is
reported explicitly; only a process directory proved gone may be omitted after
a read failure. Process start identity is checked around the argv read. Missing
gate identity or a gate change during the census also prevents certification.
The drain identity comes from the gate file. This is an observation of the
current processes, not an admission barrier or a guarantee against future
process launches; it neither opens nor closes a drain.
The updater also states, fleet-wide, which version of itself is running. It is
installed by a copy step rather than by the runtime symlink, so no shared
record answers for it: the loops' `runtime_commit` answers for the loops, a
generation receipt says what a host is supposed to install, and what it
actually installed is root-owned host-local state. On the first tick after a
version of it is installed, it posts `rollout/agents/<host>.<sha256>.json`
beside the generation store through an unprivileged child, because NFS
root_squash denies root there. The name carries the hash, so a tick that finds
its own claim already posted costs one `stat` and no write, and that `stat`
runs as root because the fleet root is world readable. Markers in that tree are
write-once: content lands in a `.tmp-` sibling and is linked onto its final
name, so a name that exists refuses the write instead of replacing what
somebody else recorded. Posting is best effort and failure is recorded rather
than raised; whether a rollout may proceed is a separate question, asked by
whoever reads these files.
Maintenance refusal before payload launch returns a claim to ready without
burning an execution attempt. The published store is explicitly authorized to
supply these privileged bytes; manifest hashes provide copy consistency, not
an independent signature. See [client upgrades](client_upgrade.md).

## Physical and adaptive GPU admission

Both current GB10 workers have one physical GPU. Their fleet shape uses the
same `--gpu` policy and contains no hand-tuned GPU concurrency count. At each
idle claim boundary the worker reads the broker's root-owned
`/run/prismabuild/gpu-capacity.json` through the trusted reader. A complete,
attributed snapshot supplies physical device identities, memory domains and
bounds, device power evidence, host memory and CPU pressure, exact active job
scopes, and processes the broker could not attribute. The worker never starts
one `nvidia-smi` process per loop.

Missing, malformed, stale, incomplete or unattributed GPU evidence offers zero
GPU capacity and must refuse a GPU claim. A fresh snapshot with one known
device and no foreign work may admit the first action even when GB10 exposes no
programmable GPU-only power limit: its actual draw and explicitly scoped 140 W
SoC reference remain evidence, while utilization percentage is not treated as
a saturation measure. Any foreign GPU process closes admission on these
single-device hosts. Processes attributed to one broker attempt do not consume
extra capacity when that attempt opens multiple CUDA contexts or uses a daemon
container.

The pool ledger represents one physical GPU token per current GB10. A legacy
action whose sealed demand says `gpu>1` is conservatively normalized by the GPU
controller to that one token plus exclusive intent; its original declaration
remains part of action identity. Memory is not normalized across domains.
`shared_system` residency is already part of GB10 host memory, while `discrete`
VRAM is monitored and budgeted separately from the action's host `mem_gb`
cgroup limit. Unknown domains refuse admission.

Worker cadence follows pressure. When `ready` is nonempty, an idle loop retries
at most once per second so a fresh capacity decision can admit work promptly.
When the queue is empty it uses the configured 10--20 second backoff, avoiding
an NFS scan and telemetry read per loop per second. GPU telemetry itself is
collected once by the broker and shared by all loops.

## Preferred, overflow and adaptive CPU admission

Host admission uses a nonblocking local FLOCK around the box's headroom
decision, not around the claim that follows it. A claiming loop takes it for
the capacity prelude that mints and retires this box's own tokens, and then
once per candidate for the adaptive CPU decision, the adaptive GPU decision,
the reservation through `begin_acquire`, and the borrow record that decision
consumes. `begin_acquire` moves the tokens out of `free/` and into a directory
every sibling's `decision` and `available` already counts, so the same headroom
cannot be spent twice once that block returns. A successful claim then runs
outside the lock without reacquiring it: the record rename that decides ownership,
the lease write and the token renames are arbitrated fleet-wide by that rename
and by the per-key transition lock, to which a host-local FLOCK adds nothing.
Holding it across them emptied whole boxes out of the claiming population while
one loop was slow on the shared mount (issue #351).
A losing loop reports `host admission lock busy` to its worker log,
with the holder PID observed at refusal (or `unknown`), before returning to
its normal poll cadence. Output is limited to one line per 60 monotonic seconds
per queue instance, including across holder changes and successful acquisitions.
This is an observed refusal at the enclosing gate, not process ownership for
recovery or evidence about any candidate's placement, CPU or GPU decision.
The diagnostic adds no shared-filesystem reads or writes. A holder can still
block on shared I/O inside the narrowed critical section, which reads holder
token metadata and renames under `begin_acquire`; the diagnostic does not bound
that operation or release its locks and reservations (issues #266 and #351).

CPU and GPU controllers resolve their host-local state paths before taking
admission, since deriving the ledger identity may stat the shared mount. This
keeps that lookup outside the host-wide lock without bounding the lookup itself.

Adaptive CPU bookkeeping is authoritative only on the host, under
`PRISMABUILD_BOX_STATE_ROOT/<ledger-and-host-digest>.adaptive-cpu-v1/`.
`cpu-sample.json`, `jobs.json`, `profiles.json` and `last-borrow.json` share
the existing admission lock across worker loops, and since the second half of
#266 so do the GPU probe state `gpu-state.json` and each running scope's live
telemetry under `telemetry/<action-key>.json` in the same directory
(`docs/host_local_reservations.md`). Cold local state starts with
no interval or learned credit; it never imports an old shared diagnostic copy.
Deploy or roll back this authority change with a drained queue, and verify all
worker loops have adopted the generation before resuming work. Do not mix
workers using shared authority with workers using local authority, or clear
the local state while workers are alive. Before rolling back to shared CPU
authority, also prove every snapshot publisher has exited: a delayed copy must
not overwrite state that a legacy worker is again treating as authoritative.

After releasing admission, a worker may start one independent snapshot
publisher per host/ledger. A separate permanent local `publish.lock` is acquired
nonblockingly and inherited only by that child across exec. No admission
descriptor is inherited. A blocked publisher retains the publication slot
until it actually exits, including after its originating worker exits; new
loops cannot create more blocked copies. Publisher PID, start ticks and nonce
remain in `publisher-owner.json`; `publisher-result.json` names that nonce on
completion or error. No timeout, signal or assumed reaping releases the slot.
Starts are limited to one per second, and completed children are reaped without
waiting at subsequent publication attempts.
Every acquired admission pass checks whether the local files differ from the
last successful publisher's source signature, even when that pass wrote no
new state. Rate-limited, busy or failed copies therefore retry across controller
and worker replacement; unchanged successful copies do not rewrite the mount.
The child captures the local file signature before copying and records it only
as successful after completion, so a concurrent state update stays pending.
These file identities are diagnostic retry hints, never admission authority.

The files under `reservations/<host>/adaptive/` are independent diagnostic
copies. The CPU and GPU copies add `_snapshot.source=host-local` and a copy
timestamp, but retain the original `sampled_unix`. `pbstatus` and `pbmetrics` classify
freshness from that original timestamp; a late copy remains stale and missing
evidence remains unknown. A snapshot can lag current admission and never grants
admission credit. Publication failure cannot change a claim result. This removes
CPU bookkeeping writes from the critical section; holder telemetry and GPU
probe state followed (`docs/host_local_reservations.md`, with the before/after
syscall counts). The shared `reservations/<host>/telemetry/<key>.json` is now
a copy the executing box's sampler writes after the host-local record, read by
`pbmetrics` and never by admission. A reconstructed resource scope restores
cumulative process I/O from its configured local authority, gated by the
attempt nonce. Missing or unusable local state never imports the shared copy.
Scopes without local authority, including late cleanup into a separate attempt
archive, retain their telemetry-path accounting. Action requests, holder token metadata,
transitions, leases and token operations still use the shared filesystem: the
token ledger is cross-host ownership evidence (see holder resolution above) and
does not move. It is not a bound on the entire claim operation or a claim
that the recurring NFS fault is repaired.

The fleet retains `--all-cores` so all usable CPU capacity remains available.
Within each worker's inherited affinity, physical performance cores form the
preferred tier. SMT siblings and efficiency cores form the lower tier and are
allocated last. Kernel online state, sibling topology and ARM `cpu_capacity`
determine the split; Intel hybrid `cpu_atom` PMU metadata identifies efficiency
cores where available. Missing class metadata cannot prove a heterogeneous
split and is treated as uniform capacity. No core numbering is hardcoded into
the scheduler.

Each host's immutable `reservations/<host>/cpu-map.json` maps CPU token ordinals
to preferred CPU IDs followed by fallback IDs. Admission acquires those ordered
tokens, and the canonical worker launches through `taskset` with exactly its
held CPU set. The launcher checks the reservation, CPU count and inherited
mask before execution. Physical-token baseline reservations therefore select
disjoint CPU IDs. The adaptive lending contract below may deliberately share
an attributed, lightly used CPU; unknown or busy reservations remain disjoint.
Children inherit the assigned affinity. The action's Docker shim carries
that kernel mask into local `run`/`create` containers with `--cpuset-cpus`,
intersects an explicit requested mask, and refuses an empty intersection.
It resolves and pins the selected Unix daemon endpoint; remote or unresolved
contexts refuse because CPU identities belong to the admitted host. Agents
must retain this shim and must not widen their assigned affinity. These are
cooperative execution controls, not hostile-process containment.
Offers advertise `cpu_tiers`, and
claims and endings retain `cpu_allocation`. Already-running overflow actions
are not migrated when preferred cores become free; subsequent actions reuse
the released preferred capacity.

Before accepting a local allocation containing fallback CPUs, a worker gives
another fresh compatible offer up to 20 seconds to claim the action if that
host can fit the entire CPU, memory and GPU demand using free preferred CPU
tokens. This is bounded advisory deferral over distributed observations, not
an atomic global scheduling order. An incompatible host, an undersized host,
or a stale offer does not strand host-specific or wide work. Local ordered
allocation remains effective after the deferral expires.

Physical CPU tokens are the conservative baseline, not a fixed concurrency
gate. The adaptive controller samples busy time for every CPU in the worker's
inherited mask and host CPU pressure. That host-level view includes processes
outside PrismaBuild, so unrelated load can close admission even when the pool
ledger appears free. Samples are short-lived; pressure or near-saturation stops
new CPU claims. A local lock serializes each host's adaptive decisions, while
the shared queue's rename still decides ownership.

Every held action begins at its full declared CPU cost. A complete, fresh
aggregate telemetry interval may lower the estimated cost of a generation
action, with a safety margin. Repeated completions of the same exact workload
shape build a bounded, expiring profile so short cheap jobs can benefit too.
Consumption increases take effect immediately; decreases decay slowly. Shape
identity retains command, code, environment, inputs, parameters and resources,
while excluding result bookkeeping. Custom or unverifiable launch shapes never
borrow. Declared CPU remains the peak contract and is not rewritten by learning.

When preferred tokens are exhausted, freshly attributed low use may make a
running generation action's preferred CPU IDs lendable. Admission shares those
IDs before consuming free fallback CPUs. If total free tokens are insufficient,
the same evidence may lend reserved IDs, but only while the fresh host sample
shows enough aggregate headroom. Unknown startup intervals are charged in full,
protected and excluded from the lending set. CPUs assigned to any busy, unknown
or measurement action remain protected. One sample cannot authorize an
unbounded burst: a successful borrowing decision consumes its freshness for the
next borrower. That consumption is recorded under the host admission lock, in
the same block as the decision and the reservation it belongs to, before the
claim rename. A claim that does not happen returns it: every branch that
abandons the reservation restores the record, since a claimant that lost the
rename occupied no borrowed CPU and is owed its retry. The restore is a
compare-and-set under the same lock and never overwrites a newer borrow, and a
lock busy at that moment leaves the borrow spent, which can only refuse the next
borrow and never authorize a second one against one sample.
Each consumption has a fresh `borrow_id` stored beside its sample timestamp.
Return compares both fields, since separate claimants can borrow the same
sample after a return. The caller retires its return authority before state
I/O, so repeated cleanup or a write-then-error cannot return a peer's borrow.
Missing ownership grants no return; an uncertain rollback may conservatively
leave the sample spent until fresh telemetry arrives. This host-local field
does not change action identity or CPU/memory reservation sizes.

Memory resources retain ordinary all-or-nothing token admission. CPU telemetry
cannot discount memory or GPU demand. The separate adaptive GPU controller below
may share the single physical GPU using its own trusted device evidence. Measurements require a fresh nearly idle
host, never lend or borrow CPU IDs, and do not overlap another held CPU action.
Measurement placement and identity remain transport-specific: the pool uses an
implicit submitting-host pin with platform/toolchain identity, or an explicit
class with matching platform/ABI/device models; SLURM uses an explicit host
class. Any required exclusive GPU reservation remains a
separate contract. In particular, GB10 GPU utilization
percentage is not accepted as saturation evidence; device power, CPU activity,
residency and useful work per unit time are the relevant host view.

Configured host memory is an aggregate fleet budget, not physical RAM or an
individual action's limit. The dl380g10 budget is 192 GiB against 294.5 GiB of
physical RAM, with measured allowance for unrelated services and host margin;
see [the capacity evidence](dl380_memory_capacity_2026-09-05.md). Live host
observation may lower advertised capacity. Increasing the configured ceiling
does not resize existing reservations or their cgroup limits.

Safe lending requires complete attribution of the entire attempt, including
direct descendants and Docker containers created through a daemon. The resource
scope architecture assigns each attempt one broker-owned cgroup, launches the
payload inside it, attaches owned containers, enforces the declared memory limit
over the aggregate, records CPU time and memory peak, and proves the scope empty
before releasing its reservation. Parent-local `memory.events.local` `oom`
identifies exhaustion of this aggregate limit and authorizes exact-attempt
termination even before a victim is counted. Hierarchical OOM victim counters
remain diagnostic: an independently capped descendant can OOM without exhausting
the enclosing attempt's budget or causing its termination. A missing broker, failed attachment,
ambiguous container operation or incomplete/stale telemetry grants no lending
credit. This is the activation contract, not evidence that broker deployment or
cross-host qualification is complete; live status is recorded separately.

The pool persists the creation key, nonce, memory budget and broker endpoint in
both claim and lease before requesting a scope. A lost reply is reconciled by
`recover_create` using that exact identity, without creating a kernel group.
When the group and authority are absent, recovery persists a cancelled attempt
tombstone before reporting absence, so a delayed original create cannot revive
the attempt. Known pending setup can be released only when unlaunched, without
Docker intents, and provably empty. Creation-recovery telemetry is incomplete
and grants no CPU lending credit. Creates carry a recovery protocol marker;
older brokers refuse it before mutation, and workers defer without consuming an
attempt until the installed authority has upgraded.

Worker-loop count supplies enough claimants to exercise this admission policy
without becoming a second scheduler. `fleet_boxes.json` declares an automatic
floor. Above that floor the supervisor sizes on the claims the box is holding:
the target is the loops with a lease or a running child, plus the loops whose
local process state is unreadable, plus a fixed idle reserve, bounded by a
housekeeping ceiling derived from visible CPU and memory. Ready work does not
enter the sizing law. A ready record does not say why work is waiting, so it
cannot distinguish a box with no free poller from a box whose pollers cannot
convert, and sizing on it made growth a fraction of the ceiling per busy tick
while requiring an empty backlog to shrink -- a ratchet on any queue that does
not fully drain, in which each poller added load to the shared metadata path
the queue itself depends on. Sizing on held claims makes growth self-limiting
without a batch, since a new claim is what earns the spare that lets the next
one be taken without a process start, and makes shrink independent of the
queue, since an idle poller is idle whether or not work is waiting. An
unreadable claim census freezes the count in both directions. Only excess loops
proven idle by one batched claim census plus local process state receive
`SIGTERM`; active work is never selected. Busy or backlogged cycles use a short
bounded tick, spawning is amortized, and monotonically allocated log slots
preserve append evidence across shrink and growth. `--loops` explicitly selects
fixed mode, while `--once` tops up only to the configured floor.

The supervisor owns reaping its exited direct children across runtime re-exec.
Before each cycle's re-exec check and census, it makes at most 256 nonblocking
`waitpid(-1, WNOHANG)` calls, stopping when no exited child is available. The
kernel retains child ownership across exec even though Python's subprocess
registry is lost. An inherited backlog larger than the per-cycle budget drains
over subsequent cycles without restarting the supervisor or signalling live
workers. The supervisor is single-threaded; synchronous subprocess status reads
finish between these boundaries, and worker-loop exit statuses have no other
consumer. `SIGCHLD` remains unchanged so descendants retain real failure statuses.

Every claim, offer, receipt and CAS read crosses one shared filesystem, and
the fleet measures it per box. `tools/fleet/mount_latency.py` samples three
things that answer different questions: NFS per-operation queue time and
round-trip time differenced from `/proc/self/mountstats`, which costs the mount
no operation and therefore keeps reporting when the mount does not, and a
bounded set of timed syscalls including the create/rename/unlink the claim path
performs. The queue/rtt split is the attribution -- time at the server against
time this client could not send -- and it separates a healthy mount from a
client in a state-recovery storm by two orders of magnitude rather than by a
chosen margin. The syscall leg runs in a forked child abandoned at a deadline,
because a hard mount blocks uninterruptibly and no signal reaches it, and at
most one such child is ever outstanding: a wedged mount suppresses the next
probe instead of accumulating one blocked process per scrape. The third
reading is not about the mount: admission is gated by a local `flock` whose
critical section still contains shared operations, so a slow mount still
makes the *holder* slow. What it no longer does is convert into a local queue.
Until #267 the acquisition blocked, and one process waiting on one remote peer
starved every other loop on the box; `locked()` now takes `LOCK_NB` and raises
`AdmissionBusy`, and the caller returns to the top of its poll and announces.
The reading is kept for two reasons: the holder's dwell time is still the thing
the mount is doing to this box, and a *waiter* now means the blocking
acquisition has come back.
`/proc/locks` is filtered to the admission lock files and split into holders and
waiters, each with its state and `wchan`, and the hold age is accumulated across
samples as a lower bound. The count of waiters not in a running or
uninterruptible state is reported alongside `load1`. Device aliases in the
passive census require bounded procfs evidence tying a holder's lock descriptor
to the watched path in the same mount namespace; inode equality alone is not
proof. Waiters follow that holder's kernel lock group. Missing, ambiguous or
changing alias evidence sets `locks.identity_complete: false`, and the collector
withholds gate/hold chart samples rather than presenting an incomplete zero as
health. The measurement remains lock-free and does not open or follow holder
descriptor targets. A blocking `flock`
sleeps interruptibly and load average counts neither: fifteen fully blocked
processes moved `load1` from 0.24 to 0.30 on sparky, which is why a box with
every loop starved reported load 1.13 and why no load-based check can see this.
Readings are a
per-box property and the three boxes are not symmetric, since the host that
exports the filesystem reaches it as local storage and has no RPC statistics;
it is still measured for lock contention, being the host that stalled.
Nothing in it decides anything; deprioritising admission on a box whose
latency is out of line with the fleet needs the fleet-relative view the
recorded series exists to provide, and is not built.

Initial activation requires drained legacy reservations. Changing an existing
host's topology map requires draining reservations, stopping that host's worker
loops and supervisor, and then removing only its `cpu-map.json` before restart;
never reinterpret held tokens under a changed map. New hosts receive their own
maps. Logical CPU counts are capacity units, not equal-throughput claims across
cores or hosts. Performance measurements still require declared architecture,
resource demand and isolation, with measured evidence for any speedup claim.


## Adaptive GPU admission and independent memory domains

Each GPU worker advertises physical devices, not manually tuned concurrent job
slots. The two identical GB10 hosts each advertise one device and use the same
policy. The current adaptive controller supports one physical device per worker;
multi-device placement requires a future UUID-specific allocation contract.

A root-owned broker snapshot at `/run/prismabuild/gpu-capacity.json` describes
all active scopes, attributed GPU processes, foreign processes, host memory and
pressure, device power and memory domain. The controller reads only a regular
root-owned file below directories that other users cannot modify. Missing,
incomplete, stale or unknown-device observations refuse new GPU admissions,
including cold start. A fresh complete snapshot permits one cold-start action.
Two consecutive low-load samples permit one additional generation action per
new sample, after every running GPU action has complete current attempt
attribution and at least two seconds to start. A candidate first acquires its
provisional aggregate reservation, then persists sample consumption under the
same host admission lock, before publishing a runnable claim. A hard-resource
refusal consumes no sample, allowing a smaller candidate to use it. Failure to
persist consumption returns the provisional reservation through the existing
exception cleanup; it never launches work. Crashes after consumption may lose
a probe opportunity but cannot reuse it. Released or retried launched actions
cannot reset that sample's spent credit.

Four ordinary pre-launch abandonment paths may return their sample credit:
fallback deferral, a lost claim rename, changed placement/demand in the moved
record, and a reservation rejected after commit. After returning the reservation,
the claimant reacquires host admission nonblockingly and compares a unique
consumption nonce and both sample identity fields. A newer probe, including reuse
of the same sample, cannot be refunded by an older claimant. The return restores
only the prior consumed sample fields and removes only its unchanged pending
power feedback; intervening observations remain intact. The prior nonce is not
restored, so an older return authority cannot be revived. The in-memory return
ticket is retired before I/O. Lock contention, missing ownership, crashes and
uncertain errors may lose credit until fresh telemetry, never launch without a
reservation or grant a second probe. Exception rollback and ordinary completion
do not return GPU credit. These host-local fields do not change action identity,
memory budgets or physical GPU reservations.

After each concurrency probe, at least three fresh samples after startup must
show how device power responds before another probe is allowed. The controller
compares the mean change with observed sample noise and a relative deadband,
not a benchmark-specific wattage limit. No measurable increase latches an
activity plateau and closes further admission. That state survives restarts and
individual holder exits, allowing concurrency to fall while remaining work
still sustains the plateau. A sustained power change in either direction, or the
end of the GPU busy period, invalidates the old phase and permits new exploration
only through the current headroom, attribution and reservation gates. An idle
startup plateau cannot establish saturation for a later active phase.
This is a conservative admission heuristic;
useful throughput and energy measurements must qualify its practical effect.

GPU concurrency uses the same host admission lock as CPU lending. Additional
GPU reservations live in each claimant's `.gpu.json`; they never mint physical
GPU or host memory tokens. Failure, abandonment and release remove the metadata
with the reservation. Physical tokens return before metadata is removed, so
an unlink failure cannot strand the entire physical reservation. A partial
token return retains adaptive metadata until a later release completes;
metadata cleanup failures remain visible and retryable.
Existing CPU affinity, GPU visibility and hard cgroup
limits remain the execution boundaries. Rising load closes new admission and
does not stop running work. Thermal/power limiting, foreign GPU processes, host
memory pressure and CPU pressure close admission. Low power permits a probe;
it does not certify hardware saturation or useful throughput. GB10's 140 W SoC
design envelope is explicitly a reference, not an NVML GPU power limit, and
GPU utilization percentage does not drive admission. Performance claims require
useful work, elapsed time, energy and the relevant host observations.

New GPU submissions seal `params.gpu_exclusive` as an explicit boolean.
Measurements and exclusive work never overlap another GPU holder. Legacy
requests without that marker are conservatively exclusive because an old
`gpu=1` request could mean the whole device. Historical `gpu>1` sharing-slot
requests also remain exclusive; their sealed demand and receipts retain the
original count, while physical reservation and fit checks use one device.

Host `mem_gb` always retains its complete token reservation and cgroup cap.
The versioned GB10 fleet ceiling is 104 GiB per box, shared by every action
on that box. A 104 GiB action waits until other memory reservations release;
the ceiling does not grant overlap. The live host-memory clamp retains its
8 GiB margin and may lower the offer. See
[the revised capacity decision](gb10_memory_104_capacity_2026-09-07.md).
On a `shared_system` device such as GB10, CPU and GPU allocations share physical
DRAM and remain inside that existing aggregate budget. On a `discrete` device,
VRAM is an independent pool: each action reserves its full GPU budget, the sum
cannot exceed device VRAM, and currently free VRAM must cover a new reservation.
Missing VRAM counters are unknown, never free. The pool-only `--gpu-memory-gb`
option seals `params.gpu_memory_gb`; its GiB value must convert to between 1
and 2**63 - 1 integer bytes. Submission, admission, and execution use the same
bounded conversion. Without it the GPU budget conservatively
defaults to `mem_gb`. RAM-heavy, GPU-light jobs should declare their separate
VRAM budget. On shared-memory devices this explicit GPU cap is an additional
subset cap, not a second reservation of the same physical DRAM. The exact
GPU budget follows scope creation, durable recovery and release. SLURM refuses
this option until its execution contract supports separate VRAM budgets.
Campaign rows expose the same budget as `gpu_memory_gb` and forward it through
`pbrun`'s seal path, preserving action identity with an equivalent direct
submission. Manifest preflight validates the bounded numeric conversion and
refuses a budget without GPU demand (explicit or implied by `exclusive`) or
under SLURM before any row is submitted.

## Model-level Tessera dispatch

The [full-model dispatcher](tessera_model_dispatch.md) owns decomposition into
Tessera's whole-layer serving-part domain. It delegates admission/distribution
to the existing campaign interface, seals the producer/source/plan/scale/image
identity, and admits assembly only behind an exact complete CAS-receipt barrier.
The assembler uses the producer's checked merge and revalidates part bytes.
Per-worker source-hash reuse requires unchanged filesystem identity and matching
expected digests, with before/after export checks. It is cooperative cache
validation, not a claim of hostile-writer immutability or cross-action residency.

### Status census completeness

`pbstatus` exits 3 when required queue reads time out or fail, active pool
records are unreadable, or selected terminal records cannot be parsed. Its
top-level `complete` flag covers all these cases. Terminal directory read
failures are unavailable sections; an unreadable terminal record retains its
diagnostic row and names its path in `unavailable_sections`. Missing terminal
directories remain valid for transports that have not filed outcomes.

A bounded status reader belongs to its calling process. SIGINT/SIGTERM unwind
through bounded pipe closure and exact-child termination/reaping; failed
termination retains PID/starttime evidence, including on cancellation. Reader
EOF does not by itself prove process exit. SIGKILL cannot execute cleanup.

The status read budget starts before default transport metadata is opened. A
failed lookup reports unknown transport and incomplete status rather than
guessing a scheduler. Script imports, output delivery, cleanup grace, and the
explicit SLURM scheduler/lane lookups are outside this pool census budget.

The empty-endings root diagnostic propagates filesystem errors to the bounded
census, so an error rendered as a note still makes the top-level read incomplete.

These completeness checks use explicit stat calls, preserving ENOENT as missing
and permission/I/O errors as unavailable. Boolean pathlib predicates are not
evidence of absence because Python 3.14 suppresses OSError in them.

### Structured status for agent consumers

`pbmcp` serves the same census to a program instead of a person, over MCP's
stdio JSON-RPC subset, stdlib-only so it runs from a published generation on
any box with `/usr/bin/python3`. It is a reader: no rename, no lock, no
`passes` write, no `record_pass`, and no submission -- submission stays behind
`pbrun`, which is where the permission hooks that gate it live. It reuses the
bounded reader rather than repeating it, so a section that does not answer is
named in `timed_out` and leaves `complete` false, exactly as the census does;
partial is reported, never awaited.

The stdio subset negotiates `2024-11-05` or `2025-06-18`. It does not offer
`2025-03-26`, whose base protocol requires JSON-RPC batch reception; clients
requesting that revision receive the supported `2024-11-05` fallback. Tool
arguments are checked against the advertised types, required fields, property
names, array items, minima and enums before any tool read. Invalid arguments
return JSON-RPC `-32602`; operational tool refusals retain `isError` results.

`pb_actions` can match `snapshot_parent` and `snapshot_commit` exactly against
the sealed `checkout_snapshot` Git fields, as well as live `checkout_root`.
Filters intersect and operate inside the existing newest-record window; exact
action keys bypass that window. Git identity is not submitter identity: several
agents can submit from the same parent, and a snapshot commit includes sealed
changes. A missing or malformed snapshot never matches a requested Git field.

Session construction also bounds the initial runtime-link read using the
configured deadline. A failed startup read is retained as `startup-repo-link`
in every tool response's timeout/error diagnostics, with `complete: false`.
Later successful reads may identify the current generation but cannot recover
the startup observation: `generation_stale` and `started_from_generation`
remain null until a new session starts successfully. This bounds session
initialization after Python has loaded the server; it cannot bound interpreter
startup or imports from an unavailable shared filesystem. The existing
zero-deadline opt-out also applies to session construction.

A session retains ownership of an unreaped bounded reader across startup and
all subsequent calls. Before each shared read it polls only its retained child
with nonblocking `waitpid`. While that child remains alive or its exit cannot
be established, no further shared read is started: sections are null and named
in `unavailable` with type `ReaderStillRunning`, `complete` is false, and
`abandoned_readers` retains the PID/starttime evidence. Once reaped, reads resume.
This caps retained readers at one per session, including when a reader returned
a payload but did not exit. It does not limit independent client sessions, and
cannot force an uninterruptible kernel wait to finish. Startup observation
diagnostics remain historical even after that reader is reaped.

Two derivations belong to the reader rather than to the queue. A preemption's
successor generation is re-derived without the transition lock `pbrun` takes,
because a reporter must not participate in the protocol it describes, and the
answer is stamped `unlocked_read` so an in-flight handoff reads as not yet
visible rather than as abandoned. The local-result claim digest is not on any
queue record; it is recomputed from the action manifest, the record's resolved
checkout root and the manifest's declared paths, with the producer's own
canonical hash, and a `checkout_snapshot` submission is refused with its
reason rather than answered with a guess.

The public read-only `core.local_result_claim_body` is shared by claim
producers and status consumers. It resolves the checkout strictly before
hashing, including symlinked checkout roots; MCP invokes it inside a bounded
read. An inaccessible checkout is unavailable, rather than a digest based on
an unresolved spelling. The existing claim format and producer identity are
unchanged.

MCP's record and terminal-entry readers use the public read-only
`pool.read_queue_record` and `pbstatus.recent_ending_paths` interfaces. Receipt
self-consistency uses core's immutable `CAS_RECEIPT_BODY_KEYS`,
`CAS_RECEIPT_KEYS` and `LOCAL_RESULT_CLAIM_BODY_KEYS`, shared with the producer.
These interfaces do not acquire locks, verify full worker attestations or add
their own read deadlines; MCP provides the deadline around each filesystem read.

Attempt logs are read by seeking to their end for a capped tail. The verifying
reader in `pool.attempt_outcomes` reads every stream whole to check a digest,
which is the right contract for a verifier and would make a status call cost
whatever the action printed.
