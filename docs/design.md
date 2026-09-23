# PrismaBuild — distributed campaign execution

**Status (corrected 2026-09-23): DETERMINISTIC CORE + SHARED CAS + PULL QUEUE
LIVE; SLURM LANE BUILT BUT PAUSED AND INSTALLED ON NO BOX; DAGSTER AND
OBSERVABILITY LAYERS NOT DEPLOYED.** The dependency-free action-key,
immutable-CAS, and local-worker core lives in `src/prismabuild/core.py`. The
shared CAS, the NFS pull queue, and the worker loops on Sparky, Sparklina, and
dl380g10 are the live execution plane. At 2026-09-23 13:30Z they run runtime
generation `81d95cba8d91-1790160372-5d100532e024` (canary verified).
`docs/staged_read_contract_2026-09-20.md` and its ledger
`docs/staged_read_requirements_2026-09-20.json` are the acceptance measure for
the staged-read work: the contract's top summary says what is workload-proven,
deployed, and only validated, and a PR that changes a requirement's status
updates its ledger row in the same PR. The fleet has dispatched Tessera and
PrismaQuant test, quantization, and measurement campaigns.
`docs/operating_prismabuild.md` is the usage guide for operators and agents;
`tools/prismabuild_worker.py` is the direct batch-script entry point.

SLURM: on 2026-09-04 Rob ratified replacing the pull queue with SLURM
(`docs/scheduler_decision_2026-09-04.md`). On 2026-09-19 he paused that
migration (PB#657): no SLURM work on the hot path until the fleet grows past
about four hosts or orphaned terminals are observed in receipts. The lane stays
as built. It lives in `src/prismabuild/slurm_lane.py` (`pbrun --transport
slurm`: seal, `sbatch`, wait, CAS lookup, and the terminal records the pull
queue's readers already look for), with the job entry in
`tools/fleet/slurm_job.py`, the fleet's configuration under `fleet/slurm/`, and
the install runbook in `docs/slurm_runbook_2026-09-04.md`. Only SLURM's own
variables reach a job (`--export=NIL`); the action's environment is the sealed
one the worker builds. A SLURM `COMPLETED` state without a CAS receipt is a
failed action, and a receipt is success whatever the exit code said. The lane
routes a GPU demand to the `gpu` partition (the two GB10 boxes, as `shard`
GRES), untagged CPU-only work to the `cpu` partition (dl380g10), and tagged or
`pbrun --anywhere` work to the default partition, where the sealed constraint
or node weight picks the node. It has run against a real `slurmctld` and
`slurmd` in a privileged container on sparky (`fleet/slurm/smoke/`, 23 rows on
the fleet's 25.11.2 rebuild; the first eleven also on Ubuntu 24.04's 23.11.4)
and across three container nodes built from the fleet's own `slurm.conf`
(`fleet/slurm/smoke/multinode/`, 12 rows on both versions). It has never run
on the fleet: the install needs root, which is Rob's, and
`fleet/slurm/install.sh`, `verify.sh`, `cutover.sh` and `rollback.sh` are
the operator's four steps if the migration resumes. `tools/fleet/pbcampaign.py`
and `pbwait.py` work over either transport. `src/prismabuild/slurm.py` is the
earlier durable-state SLURM adapter, superseded by the lane (see "Durable
SLURM submission" below). The optional asset/DAG adapter lives in
`src/prismabuild/dagster.py`; it constructs deterministic assets from sealed
action keys, binds each edge to an expected CAS output digest, and materializes
only after re-reading that receipt and payload from the CAS. Dagster and the
proposed observability stack remain uninstalled.

Worker-offer freshness is evaluated after the complete directory and record
scan. An offer that expires during any read is excluded, including an offer
read before a later file stalls. Offers require a finite numeric announcement
timestamp at most 60 seconds ahead of the scan's completion time. Bounded
future skew counts as age zero, including for a caller's shorter offer TTL;
greater future skew is excluded even when retained capability uses an infinite
TTL. The pool status reader shares this rule and reports `offer_clock_skew_s`;
both status and submission diagnostics name future discrepancies and whether
they were tolerated or ignored. The discrepancy compares the record's writer
clock with the reader, not with an independently trusted time source. Offers
retain their existing wire format. CPU/GPU telemetry freshness, capacity
reservation, containment and lease recovery receive no additional tolerance.
The pool's shared filesystem calls remain synchronous except for the bounded
`pbrun` offer, detached attachment and pull-queue outcome reads and `pbwait` observations
described below. `pbrun` runs its one pre-submission **worker-offer** record scan in
an abandonable reader with a fixed five-second budget. It refuses loudly if
that reader times out or fails, names any retained reader identity, and never
publishes a runnable `ready/` item from an unavailable snapshot. A timeout whose
reader was reaped raises `OfferDiscoveryTimedOut`, a `SystemExit` subclass. Plain
`pbrun` still exits 1 on it. A windowed campaign may retry it, because nothing
was published and no reader survives. Within the scan, an offer read that
returns nothing (`ENOENT`, `ESTALE` or empty) is read once more. Workers publish
offers with `os.replace`, which never removes the name. A reader that opened the
replaced file can get `ESTALE`, and the second read opens the name again
(#560). An offer that fails both reads stays excluded, as #208 requires. The parent
re-evaluates freshness and future skew only after the complete child scan, for
each verdict; retained capability therefore still has its infinite-age rule.
The five-second read budget includes child FD isolation and IPC waits. Parent
process creation, decoding the completed reply, cleanup grace and runtime
imports are not themselves interruptible at that deadline.
The offer boundary covers no CAS request/staging, publication, terminal wait,
claim, lease, token, or other queue read/write, which remain synchronous and
can still stall (issue #16), except for the worker publication below.

The worker's own **offer publication** is the one queue *write* with a bound,
because it is the one write that is purely advisory: it reserves nothing,
claims nothing and starts nothing. A worker loop publishes its offer through
the same isolated, abandonable child as the status reader
(`pbstatus.bounded`), with a fixed five-second budget. A publication that does
not finish in the budget skips admission for that poll -- the loop returns to
its generation and maintenance checks after the normal poll delay and never
calls `serve_once` from an unavailable advertisement. Process creation, helper setup, completed-reply decoding and cleanup grace
can add time; this bounds waiting, not a kernel syscall or the whole poll.

Inside the helper's child, acquired **after** the helper's file-descriptor
isolation and before any shared operation, the publisher takes a host-local,
per-uid, nonblocking `flock` under a private local directory
(`/tmp/prismabuild-offer-publish-<uid>`; never the shared mount). The directory
is verified as this uid's private 0700 non-symlink and the lock is opened
through that directory's descriptor. The lock is held across the shared write
and until the child exits, so a sibling loop, or a supervisor replacement,
cannot pile another writer onto a box whose publisher the parent could not
reap. Contention (`EAGAIN`/`EWOULDBLOCK`) is retried inside the same budget;
any other lock failure fails the publication instead of serving.

A timed-out publication is reported as an **unknown** outcome, not as "nothing
was published": the write may already have landed, and the previous offer is
left to expire on its own rather than unlinked. A publisher the parent could
not reap is retained by `(pid, starttime)`, re-checked with nonblocking
`waitpid` on later polls, and fences new launches while its identity survives;
an unreadable `/proc` keeps it retained (fail closed). The budget, ordering
and expiry are deliberately not configurable. This boundary does not order
writers outside the mechanism: an older generation's loop publishes without
the lock, so two generations can still interleave one offer file, a late write
keeps its original `announced_unix`, and the claim, lease and token mutations
that follow a claim decision remain synchronous and unbounded (#266).
Offer timestamps, wire format and freshness rules are unchanged. Forked
publishers retain their process identity and remain visible to worker/drain
censuses; a surviving writer can conservatively prevent rotation proof. No
process-title manipulation hides it from those checks.
Detached attachment discovery has its own five-second isolated-reader budget.
It reads recorded submissions, covering outcomes and leases using the existing
liveness rules, and returns the display path with the selected generation so
printing an attachment performs no further queue read. Reader timeout, setup
failure or retained child refuses with exit 74 before runnable publication;
the error retains any unreaped PID/start-time identity. SLURM controller queries
stay in the parent under their existing command timeouts. Record parsing keeps
its historical tolerant behavior; this boundary does not make a swallowed
record error distinguishable from absence. CAS lookup/staging, publication,
post-publication generation discovery, runtime imports and other shared
operations remain outside this bound. Process creation, reply decoding and
cleanup retain the same isolated-reader limitations as the offer boundary.
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
refresh and capacity clamp continue on their normal cadence, with the refresh
itself bounded as described above, and a refresh that does not complete skips
that poll's admission rather than being served from. Discovery -- the ready
records and their aging sidecars -- additionally runs in an abandonable child
before either: the loop hands the completed snapshot to ``serve_once`` as its
``ready`` candidate list, and any other outcome skips publication and admission
for that poll, so a box that cannot read the queue lets its offer expire
instead of refreshing it on no evidence (#16). The snapshot is advisory, as the
in-process scan was: an intervening claim wins at the rename. This removes
scan stalls from the worker's wait, but shared transition, lease and token I/O
remain synchronous and still need ownership-safe recovery qualification (#266).

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
Each host also keeps a bounded, best-effort latest claim-denial record per
action generation in its local admission state. It names the exact branch and
the controller decision/sample already consumed, never a diagnostic resample.
The independent snapshot publisher coalesces copies to the shared adaptive
directory at its existing one-second cadence, after admission is released.
This is observability, not an audit log or admission authority: a contended
local diagnostic lock or delayed/failed copy may leave status without a current
reason, and readers reject a record whose `published_unix` does not match the
ready generation. Claim aging remains in `passes/` and is unchanged by these
records. The snapshot retains at most 256 latest action generations per host;
claiming an item does not erase its diagnostic history. Status exposes each
host's record on ready and claimed rows only when it matches that submission,
with the original observation age. A malformed diagnostic makes the census
partial without making its valid job unreadable or erasing queue counts.
An unfunded token acquisition also retains `token_shortage`: the first failing
resource, the physical tokens requested after any CPU borrowing adjustment,
and the tokens actually obtainable before rollback. This is evidence from the
acquisition itself, with no diagnostic rescan or additional shared write. It is
not a current free-capacity snapshot or a claim that all other resources fit.
Only controllers evaluated for this candidate contribute decision evidence;
a CPU-only denial cannot inherit a previous candidate's GPU decision. Old
denial snapshots without shortage details remain readable. Admission, rollback,
aging and the bounded/coalesced diagnostic publication remain unchanged.
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
Restartability proof (the sealed action class and immutable prior-withdrawal
chain) is read before admission while that handoff lock is held. The live
selection then re-reads the holder set, capacity, tokens, claim and current
withdrawal coverage under admission; a proof names only its exact claim
identity and the live fields it consumed (`retry_safe`, attempt/history/budget
and lineage, plus the sealed action address/resources), and missing or changed
evidence defers preemption. This comparison preserves JSON types and field
presence, so `true` is not `1`, `3` is not `3.0`, and absent is not null.
Nonfinite proof inputs defer that holder without aborting the candidate pass.
Protected foreground, finishing and cleanup-pending holders do not incur that
sealed proof read. The final
per-key withdrawal repeats that exact-claim check, so a concurrent finish,
withdrawal or successor cannot turn a stale proof into a second victim.
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
within a priority band.

A ready record that states any of those three fields in a way the queue cannot
read is **skipped from the listing and filed by the sweep**. `publish` refuses a
non-integer `priority` and writes `published_unix` itself, so such a record was
written by something that is not this queue; the ordering used to be computed in
the sort, after the per-record parse guard, so one of them raised out of
`ready_items` and took the whole listing — the claim scan, the prewarm loop's
claim-order read and the supervisor view — for every caller at once, ahead of any
per-item denial. An enumerator has no `record_denial` channel, which is why this
site was left out of the per-item raise fixes. Skipping alone would be a second
silence, so `_ready_record_usable` reads the same three fields from one
definition: a record with no place in the queue's order is never listed, never
claimed and never runs, which is exactly the unaddressable-resident defect
`quarantine_orphans` files into `failed/` as `orphaned_stub`, where `pbstatus`
counts it. The filed record names the offending field and the value it stated,
and the original bytes stay beside it in `superseded/`. Values `int`/`float`
accept keep the place they have always had: the guard turns a raise into an
answer and changes no reading the sort already made. A denied item past `STARVATION_FLOOR` may withhold its
host, but higher-priority items have already been considered before that veto
is reached. The withholding rule protects large items within each band, and
since #924 it withholds only while the box will drain soon. That is judged per
holder from what the holder declared (`PoolQueue.holder_bound`), not from a new
constant: a holder younger than `WITHHOLD_CEILING_S`, or whose sealed
`execution_timeout_s` ends inside it, is transient; a bounded holder past that
line is long; a holder past its own sealed end is overdue, which outranks age,
since a holder that outlived what it declared is no evidence of a drain (#939);
a progress-governed holder with no total timeout is unbounded;
a raw ledger holder with no readable claim is judged by the waiting item's own
first-denial clock, as every holder was before #924. The age half of
"transient" is a presumption, not a declaration: a young deadline-governed
holder with no requested timeout may yet run for hours, and it becomes long by
itself once it passes the line, so a veto resting on it ends on its own. A
token shortage withholds when transient holders cover every short kind. An
adaptive refusal that draining resolves withholds too: an exclusive need (a
measurement's `measurement_host_not_idle`/`measurement_holder`, unbounded or
full-width CPU demand on a pressured host, and the GPU refusals for a
measurement) when every holder is transient, and the adaptive CPU refusals
that stand for a CPU token shortage (`borrow_evidence_unavailable`,
`pressure_override_no_borrow`, `projected_cpu_cost` with the tokens short)
by the token rule. Every other adaptive refusal is overtaken as before. An item
whose holders do not drain soon keeps its passes and its place, is denied
`..._starved` (or `..._past_ceiling` when its own clock ran out, or when the
veto expired under refills), and is listed under `starved` by
`pbstatus --starvation` and `pb_starvation`. Three bounds keep a veto finite:
holders age, and none stays transient past its declared end; a veto refilled by work ahead of it in the ready order expires
`WITHHOLD_CEILING_S` into the episode until those refills have gone; and an
exclusive need with no holder in the way withholds only for one CPU sample
window (`adaptive_cpu.MAX_INTERVAL_S + MAX_SAMPLE_AGE_S`) of the last holder's
tail, after which the load is treated as foreign and the item does not withhold
for `WITHHOLD_CEILING_S`, as it does not whenever a GPU refusal names processes
the pool does not own. An item that needs a GPU on a box whose GPU is free is
eligible on its first denial rather than at the floor, because every admission
behind it takes CPU or memory it needs; "free" is the token and, where the box
samples its GPU, a fresh sample naming no foreign process. Priority defaults to 0 and is queue
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
and withdrawal lineage. The same exact-generation reader serves an ordinary
generation whose row a later run replaced, with no handoff context to follow.
Recovery writes no queue pointer and names the immutable attempt as its source.
Attempts predating this context provide no inferred link.
The withdrawal and replacement publication share the holder's transition lock;
waiters acquire it before resolving the replacement, so a partially completed
handoff cannot report cancellation while the replacement is being published.

**A publisher that must not duplicate says so, like `refuse_withdrawn`.**
`publish` overwrites `ready/<key>` whatever state the key is in, and a fresh
generation over a live key stays the default: that is how an operator asks for
the same work again, and `_claim` treats the new generation as uncovered by the
old cancellation for exactly that reason. `refuse_if_live` is the declaration a
publisher makes when a second row would be a duplicate rather than a new
generation. Under it, a key readable in `ready/` or `claimed/` refuses with
`ActionAlreadyLiveError` naming that state and the live row's `published_unix`.
The check runs inside the key's transition lock, which `claim`, `finish`,
`withdraw` and the reapers all take, so it is exact rather than advisory. A
live cancellation skips it — the marker is what makes the submission a
replacement — and so does a publication carrying `preempted_claim`, which the
preemption and resign handoff rules above already judge. A name that is present
but unreadable (a truncated write, a tombstone or late-finish sidecar) is not a
generation anybody waits on, and republishing repairs such a key, so it stays
publishable.

Two kinds of publisher opt in. `pbrun` and `pbcampaign` do, and turn the
refusal into an attachment: the waiting pool path waits on the generation the
refusal names, and the detached path prints `attached` rather than `submitted`.
Identical submissions seal one content-addressed key, so three `pbtest` shards
started at the same moment each stamped their own generation over one `ready`
row; the worker ran the survivor once and filed one terminal, and every client
pinned to a superseded generation polled an empty queue until its wait budget
expired (#812). `live_submission`/`bounded_attachment` still answer the
`--detach` question from outside the queue, because that path has to decide
what to print without publishing anything; the publication's own refusal is
what closes the window between that read and the write. The live row's
priority, tags and demand are what the attached submitter gets — a republication
under `refuse_if_live` changes none of them. `pbrun --residency stage` does not
opt in: it has already frozen a window plan, which is first-writer and has its
own answer for a second seal of the same body.

The automatic republishers in `tier_loop` and `produced_output` opt in too.
Each already looked at `ready/` and `claimed/` before publishing; outside the
lock that look could not be atomic, and on NFS its answer could be stale. In
the 2026-09-21 Stage A cycle it was: each egress was republished three times
and ran four, separated by timing and refused by nothing, and because a
movement node carries `recompute` the duplicates were not answered from the
receipt (#810). The refusal returns each caller to the answer its own
pre-check gives — the egress defers as in-flight, the output mover reports
`published: false` — so the flag makes the existing decision exact rather than
adding a new one.

A waiter pinned to a generation is not limited to the mutable terminal rows,
which a later generation of the same key can replace. The immutable attempt
outcome that `finish` publishes before the row moves is read back by
`PoolQueue.archived_generation_outcomes`: exact to the generation, every
attempt in its directory verified against the `(action_key, published_unix)`
name and its canonical attempt path, the contiguous numbered run ending at its
terminal attempt rebuilt as history, and the adopted first-writer summary,
log digests and byte counts checked. A numbered run must begin at attempt 1
unless the attempt's immutable handoff context proves the interrupted prefix;
a missing prefix with no context refuses rather than being reported as
authorized history, and the `<number>.receipt-reconciliation.json` sidecars
filed beside the attempts are not attempts. Tampered, malformed or incomplete
archive evidence refuses with `PoolContractError` instead of being read
around; an archive with no terminal attempt answers nothing rather than
inventing a verdict. The selection step `pbrun.outcome_poll` calls it beside
the mutable rows for the same reason it follows a preemption successor: the
waiter's generation is the one it submitted, and a newer generation's ending
is never reported as this run's.

The synchronous pull-queue path in `pbrun` reads one terminal snapshot at a
time in an isolated child with a five-second read budget. That snapshot covers
the three mutable terminal rows, immutable withdrawal decisions, the archived
endings of the exact generation (preemption successors included), and the
exact successor selection above; the selected
generation returns to the parent and is carried into the next snapshot.
After an ending lands, its immutable attempt history and logs are verified in a
second, separately bounded child before `pbrun` prints a result. The parent
keeps the original `--wait-s` deadline across polls and sleeps itself, so each
new child cannot renew caller patience. A `--wait-s 0` caller still receives
one immediate bounded snapshot. An unavailable reader (timeout, child failure,
or reader that cannot be reaped) returns filesystem exit 74 with its retained
PID/start-time identity; it does not cancel work, publish a record, or
manufacture a verdict. Two exceptions keep a finished action reportable. A
complete payload from a reader that could not be reaped is used, not refused:
EOF is the proof the payload is whole and the child holds nothing but its
pipe, so the 0.25 s reap grace is a scheduling artifact under load, not a
verdict on the data (#630). And before any unavailable observation becomes
exit 74, the wait spends one last bounded snapshot -- the terminal re-read --
asking whether the ending has landed since; a pass that will not report a
finished shard is the mirror image of the submission-acknowledgement trap.
The re-read is bounded by the same five-second budget, runs no mutation, and
is the wait's last observation either way, so no polling loop ever races a
retained reader. The one exception is patience: with `--wait-s` above 0,
a snapshot or verification that timed out, and whose reader was killed and
reaped, is taken again at the next poll under the same deadline. Because a
retry follows only a reaped reader, one wait never has two readers alive;
the terminal re-read above is the single terminal exception, and it polls
nothing after itself. A deadline that
passes on an unavailable read exits 74 with its own message, not 75, because
no record was read to show the work unfinished. A published unreadable terminal retains its existing
exit-1 report, and immutable contract validation retains its existing error.
The budget covers the child read and IPC wait; process creation, completed JSON
decoding, cleanup grace, runtime imports, and output can add time. This is only
the synchronous pool `pbrun` path: it does not bound the whole submission.

`pbwait` also resolves a non-full key prefix by scanning recorded SLURM rows,
the five pull-queue state directories, and withdrawal decisions in one
isolated child with the same five-second read budget. It returns candidate key
strings only; the parent retains the exit-2 not-found/ambiguity decision. A
timed-out, failed, or retained prefix reader exits 74 and names any retained
PID/start-time identity, without retrying any scanner in the parent. Full
64-hex keys retain their no-read fast path.

Each later `pbwait` pass runs its read-only submission lookup, pool outcome and
preemption selection, and any needed sealed-request/CAS receipt lookup in one
five-second bounded child. A landed outcome's immutable attempt/log verification
runs in a second separately bounded child. A terminal
or unreadable outcome outranks CAS lookup; a SLURM parent may then resume and
repair its own terminal record, but no resume or record mutation runs in a
disposable reader. The parent retains the selected exact generation across
passes and follows a just-observed preemption successor immediately without
renewing the original deadline; it defers request/CAS lookup until that
successor is observed. A failed or retained observation becomes that key's
`record_error` row and exit 74; it does not start another parent diagnostic
read, CAS lookup, or record mutation. A timed-out observation whose reader was
reaped is marked on its row (`observation_timed_out`); `wait_one` repeats the
pass while its deadline lasts, and a `pbcampaign` window keeps the key pending
instead of stopping. At the deadline it stays `record_error` and exit 74. The five-second budget
does not bound SLURM controller work, process creation, child cleanup, JSON
decoding, a kernel syscall, or an actual cross-host hard-NFS stall.

`wait_for_keys` retains its existing thread-per-key concurrency. Its bounded
readers therefore fork from a multithreaded `pbwait` process; the reader's FD
isolation prevents an abandoned child retaining parent descriptors, but cannot
remove POSIX inherited-lock/startup risk. A fork/setup/reader failure still
becomes the per-key exit-74 result rather than a retry or a claim about a
terminal verdict.

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
checkpoint, and the time output was last observed to grow. The launcher is not
the payload. Under the resource transport it is a proxy that hands the broker
its stdio and waits on the socket, and the broker forks the work, so the
launcher's liveness says nothing about whether the work is progressing. The
observation therefore also carries a `child` record read from the attempt's
cgroup: the pids in it, a per-pid kernel liveness check, the CPU the group has
been charged split user and system, and how long it has been since output was
seen. It fails closed. Absent scope, absent cgroup, an unreadable group, or a
refused read beside an empty result all report `source: unobserved` and no
liveness field at all; a group that was read and found empty reports no pids
and a false liveness, which is a different and stronger statement. Readers
treat a missing `child`, as an observation written before this field, as
unobserved and never as an absent payload. Observation time is
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

Retired scopes with a recorded matching kernel identity can reclaim memory
while they remain empty and frozen. Reclamation does not release containment.
For the action's own scope, the holder can submit token-authenticated container
settlement after cleanup proves its ownership marker absent and both reserved
Docker label queries empty. Inventory removes only settled scopes after a
reclaim request followed by no remaining anon/file charge and fresh
empty/frozen/identity checks before and after reasserting stop.
Failed reclaim and residual or unknown page charge retain the group for retry
without failing maintenance health. Legacy unsettled groups remain retained;
their removal needs the offline evidence described in
[resource authority](resource_authority.md#recovery-evidence). Late-finish
cleanup has no authority to settle the action-wide container transaction.

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

The immutable runtime-generation Docker wrapper path participates in both
sealed PATH and wrapper argv, and therefore in action, snapshot and container
owner identity. An ordinary re-seal after publication can produce a new key
even when the command and runtime code are unchanged. Request-bound resealing
(`pbrun --as-sealed-by ACTION_KEY`, campaign row `as_sealed_by`) recovers only
that wrapper path from the original canonical, validated CAS request. The
path must name a retained generation in this fleet; its read-only generation
receipt and Docker shim digest must agree. It never invokes the old submitter
or imports command/options from the request. Current checkout bytes, inputs,
environment, placement and all other parameters are sealed normally, then the
complete action key must equal the requested key before publishing an action
request or queue item. Snapshot inputs may already have been ingested when a
key mismatch refuses. Missing/corrupt reference evidence refuses without
falling back to a fresh key. The option itself adds no identity field.

This is explicit recovery across a publication, not a migration of old keys or
generation-independent default memoization. A retained old wrapper remains
required; a change in the sealing algorithm can still prevent reproduction
and is refused. The ordinary transport, attachment, retry and verified CAS
lookup paths remain responsible for the exact matched action. A cache miss
can submit that same key; this is not a receipts-only switch. Runtime
execution/attestation provenance remains the existing worker/receipt contract.

### Admitted reader launch identity

Beyond the sealed environment, a claimed action executed under a
broker-owned scope carries its attempt identity: `PRISMABUILD_ACTION_NONCE`
and `PRISMABUILD_ACTION_SCOPE` (derived by the `resource_exec` proxy from
the exact launch key and nonce, never the broker token) plus
`PRISMABUILD_READER_HELPER_ROOT` (the immutable generation root the proxy
resolves from its own sealed path). The pool carries its already-known
sealed `worker_script` explicitly into `scope.wrap_argv`, so the proxy
comes from the runtime being launched -- retained or current -- however
the argv is prefixed (the CPU-affinity `taskset` wrapper stays inside
the wrap untouched; no root is ever inferred from command text). A
retained root is trusted only as a direct non-staging child of the
fleet's generation store whose receipt names it (40-hex commit) and
whose manifest covers the sealed worker, the proxy, its broker/layout
imports, and the package code imported before containment (`__init__`,
`resource_scope`, `core`, `progress`, `residency_map`, `storage_tiers`)
byte-for-byte over sealed non-symlink files
(the `supervise._proven_roots` / `_published_generation` and
`publish_runtime._barrier_generation` rule); anything else refuses
rather than executing an untrusted proxy outside the contained slice.
Dev stubs and missing shapes keep the current-runtime proxy.
`run_local_action` forwards exactly
these three from the launcher environment through the residency
environment contract, so strict readers bind pins to the live claim and
import sealed helpers. Sealed conflicts on any identity name refuse, as
does a partial bundle (no producer ever emits half of one). A complete
but unbound tuple -- malformed nonce, scope that is not this action's
broker slice, helper that is not this executing generation -- refuses
rather than feeding an outer nonce into an unrelated synthetic action:
post-deploy harnesses and nested local runs isolate their synthetic
unit contexts through their own fixture, never through weakened
production checks. Absence forwards nothing (legacy behavior for
uncontained and pre-reader actions). The broker token and socket never cross. These
values are runtime authority bound to the attempt, not sealed request
input, so forwarding them cannot change an action key.

### Fleet command demand vocabulary

`pbrun` and manifests consumed by `pbcampaign` use the closed demand vocabulary
`cpu`, `gpu`, and `mem_gb`. Validation occurs before a request is sealed; a
manifest is validated as a whole before its first row is published. The live
pool offers and SLURM translation both define only these resource kinds, so an
unknown name would otherwise create an action no worker could admit. This does
not narrow the generic `PoolQueue` resource ledger, whose direct producers may
define resources outside the fleet-command client contract.

Storage-tier kinds are exactly such a producer-defined resource. `PoolQueue`
understands a demand key spelled `<kind>@<tier_id>` and reserves it on a
cluster-scoped tier ledger rather than on the executing box; `pbrun --demand`
and campaign rows still refuse every name outside `cpu`, `gpu` and `mem_gb`, so
no fleet-command submission can carry one. See
[Cluster-scoped storage tiers](#cluster-scoped-storage-tiers-583).

## Work decomposition boundary

Rob's 2026-09-11 design decision is to partition logical requests into small,
useful execution units before those units are published. The logical parent
may be queued first and pass through a PB-owned decomposer. That stage freezes
and validates the complete child plan before publishing ordinary child actions.
After publication, child scope and task membership are immutable, both while
ready and while running. Existing admission chooses where a child executes;
retry preserves that child's identity and adopts verified durable results.

The detailed [decomposer design](design_work_decomposition_2026-09-11.md) is a
contract for #517. PR #518 implements synchronous immutable decomposition and
exact-cover closure; durable queued parents and parent withdrawal remain
proposed. This is source support, not deployed qualification. The ordinary
campaign path still accepts complete independent action rows. Producers must expose their
logical tasks and necessary calibration/residency boundaries; PB must not guess
how to split an opaque command or modify a published execution unit.
The ordinary row controller retains `--max-inflight`; logical requests refuse
that option until bounded child publication is supported.

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

A checkout containing `tools/resolve_<module>_dev_pin.py` opts into reviewed
Python dependency verification for every `pbtest` shard. The resolver runs
under the target interpreter inside the admitted, sealed checkout and must
print one full lowercase Git commit. The module must have one owning installed
distribution, non-editable PEP 610 Git provenance at that commit, and intact
hashed RECORD files. Python's selected module must be recorded by that
distribution; unrecorded package files refuse. Missing/ambiguous provenance,
local-directory installs without Git metadata, resolver errors, drift and
import shadows refuse before pytest. Nothing installs into a shared venv.

The standard-library guard is embedded in the shard command and therefore
enters action identity; old unguarded receipts cannot satisfy guarded requests.
Verified module, distribution, expected/installed commits and import origin
travel in the action's stdout payload. Existing requests remain immutable. This trusts managed
installation metadata; it is not a package signature or a sandbox against
tests/resolvers changing imports. Environments must remain immutable while
actions use them, including between verification and pytest execution. Generic
`pbrun` commands do not opt into this `pbtest` convention automatically.

Every `pbtest` shard runs pytest under the outcome recorder in
`tools/fleet/pbtest_outcomes.py` (#942). The shard command carries the recorder
as source, beside the dependency guard when the checkout pins one, and never as
a path. The recorder needs only the standard library and the target's pytest.
It records each report the terminal summary counts, classified as the terminal
classifies it: the category `pytest_report_teststatus` gives a test report, and
`error` or `skipped` for a collection report that failed or skipped. It prints
the record as one `pbtest-outcomes: {json}` line in the shard's stdout, which
the pool stores with the action. The recorder is an object plugin, so under
pytest-xdist it stays in the controller, which receives every worker's reports.
`pbtest` reads the record into each shard's receipt entry as `skipped`: every
skip's node ID, phase, reason and location. It prints the same list, and names
a shard whose summary counts skips it has no record for. A shard with no record
has `skipped: null`, which means its skip reasons are unknown, not that nothing
was skipped. The recorder changes every shard's command, so receipts from
before it are not cache hits for shards after it.

`pbtest` reconciles every shard by node ID (#941). Each shard's receipt entry
carries `reconciliation`: its collected tests matched against the outcomes its
recorder saw, and its record's counts matched against its summary line. A
shard is not green, whatever its exit code, when a collected test has no
outcome, an outcome belongs to no collected test, a node ID is collected twice,
another shard also collected one of its node IDs, its counts disagree with its
summary, or it reported a summary and printed no record. A `--collect-only`
shard is matched on its collected count instead. Two differences are not
failures, and the report names each: an outcome at collection (a module that
skipped or failed at import, which the summary counts and a `--collect-only`
pass does not) and a test the summary counts in more than one phase (a pass
whose teardown errors or skips). The report's totals line states the sum:
outcomes equal tests, plus outcomes at collection, plus extra phases.

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
- **Native Nsys does not trace Docker daemon children** — `nsys` and its
  windowed form add `PRISMABUILD_PROFILE_NSYS=1` to the launched environment.
  A sealed value for that variable refuses rather than being overwritten.
  The action's Docker shim refuses run/create/exec and start/compose-start
  forms before contacting the daemon, including calls nested in launch scripts
  and calls using Docker global options. Metadata reads remain available.
  This is an early refusal of an unsupported profiling route, not transparent
  container instrumentation. A native CUDA child or explicit instrumentation
  inside the admitted container is required. Callers retain the shim and its
  resource scope/CPU affinity contract; clearing the guard or bypassing the
  shim is unsupported. Existing reports retain `kernel_summary_absent` when
  no completed kernel evidence was collected. Existing sealed requests and
  running old generations are not rewritten by publication.
- **This descendant sampler does not see a Docker daemon's child** — `sample`
  adds `PRISMABUILD_PROFILE_SAMPLE=1` and `PRISMABUILD_PROFILE_SAMPLE_MARKER` to
  the launched environment; a sealed value for either refuses rather than being
  overwritten. The worker arms the marker before the action starts and the
  action's Docker shim replaces the armed record with the attempted route
  before it refuses run/create/exec and start/compose-start, exactly as the
  Nsys guard refuses them. Settlement treats a marker that is missing,
  unreadable, malformed, nonregular, symlinked, oversized or in an unknown
  state as unknown coverage -- absence after preinit and an unfinished write
  are negative evidence, never a clean profile. Any such record carries
  `produced: false`, `workload_coverage` (`unsupported` for a named route,
  `unknown` otherwise), `container_route` and a reason, on the final record and
  on every partial/status checkpoint, because a launcher can swallow the
  shim's `125` and exit zero. An uncovered record refuses receipt publication
  even on a zero exit: the host-side blob and its negative metadata are
  retained on the error and the action's own result is ingested for reading,
  while a receipt filed for the key would answer every later submission with a
  cache hit that carries no profile. A nonzero action keeps its own
  `returncode`/`signal` and the negative profile beside them. This is an early
  refusal of an unsupported route, not transparent container instrumentation:
  py-spy's `--subprocesses` follows the action's descendants, and a
  daemon-started process is not one. Attaching through the container's PID
  namespace, or injecting the sampler inside the container, is a separate
  route this change does not implement, and PB does not widen ptrace or
  container privilege/seccomp settings for a diagnostic. The marker is
  bookkeeping in the action's own scratch directory, not a security boundary:
  an action that clears the guard or tampers with that scratch is unsupported.
  A native child or explicit in-container instrumentation is required (for
  PyTorch, `--profile torch` with `PRISMABUILD_PROFILE_TORCH_OUT` forwarded
  and mounted). Callers retain the shim and its resource scope/CPU affinity
  contract (#562).
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
  A windowed profiler ending is not an action deadline: once its trace is
  checkpointed, the relayed workload remains governed by the sealed execution
  deadline or progress supervisor. The relay records its own pid and Linux
  start ticks with the launched child; if that exact live relay is missing or
  dead before it records an ending, the worker terminates the owned action
  group and refuses rather than waiting without an execution authority (#514).
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
  source-side stamps from older versions are left untouched. The snapshot
  builder accepts either a stamp name and UTF-8 payload together or neither;
  it no longer reads a stamp file from the submitting worktree. Producers
  that seal their own action bodies omit both. The submitter publishes its
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
worker locks, empty result staging namespaces, private ingest directories, and
— under an explicit `--canary-root` — the canary run namespaces, each gated by
the retention rule its own `run.json` stamps: a valid canary record naming the
namespace, recognizable members only, and a seal or an expired age backstop. Applying any sweep requires
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
Those prelaunch operations and queue waiting do not consume it. After launch,
time spent in synchronous observation, lease, withdrawal and scope-telemetry
checkpoints is excluded without resetting previously spent execution time. Communication
waits are capped by the remaining budget independently of lease-heartbeat cadence. Expiry uses the
existing bounded process-group termination and timeout receipt path. SLURM
continues enforcing the submitter budget through its scheduler time limit.

### Declared container images (#714)

An action may declare exact local container image references it needs on its
claiming box: `pbrun --container-image REF`, sealed as
`params.container_images` and copied to the queue item as the scheduling
projection. The declaration exists because an action tagged for a class whose
image existed on only one member of it was claimed by the other and failed
inside its wrapper, spending its only attempt (2026-09-20, `gb10`).

Contract:

- **Reference forms.** `sha256:<64 hex>` names a local image ID;
  `repository@sha256:<64 hex>` names a repository manifest digest, matched
  only as that exact `repository@sha256:...` string; `content:sha256:<64
  hex>` names the image's store-independent content (#805, below). A bare
  RepoDigest is never announced, so a hex collision cannot satisfy another
  form. A mutable tag is refused at declaration: it is not an identity and
  cannot be sealed into an action key.
- **Store-independent content identity (#805).** An image ID is what the
  box's own image store calls the image, and the two Sparks do not agree.
  Measured 2026-09-21, both on Docker Engine 29.6.2: sparky runs the
  containerd image store and reports the image's top-level descriptor digest
  (an OCI index for 16 of its 27 images), sparklina runs the classic store
  and reports the config digest. The GLM campaign image is
  `sha256:c0e532d2…` on sparky and `sha256:9195c23f…` on sparklina with the
  same 36 `RootFS.Layers`, so an action sealed with either ID was claimable
  by one Spark only, and the denial read as "the image is missing". The
  `repository@` form does not rescue it: 18 of sparklina's 32 images carry
  an empty `RepoDigests`, the campaign image among them, because a locally
  built or loaded image has no repository digest.

  `container_images.content_ref` computes one string from what the image
  *is*: the ordered `RootFS.Layers` diff ids, `Architecture`, `Os` and the
  covered image-config keys (`Cmd`, `Entrypoint`, `Env`, `ExposedPorts`,
  `Healthcheck`, `Labels`, `OnBuild`, `Shell`, `StopSignal`, `StopTimeout`,
  `User`, `Volumes`, `ArgsEscaped`), canonically serialized under a schema
  string that is part of the digest. Zero-valued and absent config keys are
  one statement, because Docker writes the config with Go's `omitempty`;
  `Labels` keeps empty values, which 10 of sparky's and 12 of sparklina's
  images carry. A config key outside the covered set is **refused when it
  carries a value**, never ignored: a daemon saying something this cannot
  price must not have it priced wrong. Excluded: `Id`, `RepoTags`,
  `RepoDigests`, `Size` (measured to disagree on every shared image),
  `Metadata`, `Parent`, `DockerVersion`, and the store-exclusive
  `GraphDriver`, `Descriptor` and `Identity`; also `Created`, `Author`,
  `Comment` and `Variant`, which are client-rendered strings that do not
  reach the container, so covering them could only refuse a box that holds
  the image. Measured over 72 images on three boxes and two stores: every
  one hashed, and all 17 images the two Sparks share produced one reference
  on both.

  What it proves and does not. Equal ordered diff ids are an equal
  filesystem, and every build step that adds no layer lands in the covered
  config, so two images with one reference run identically. It does not
  prove that the *name* the action's own `docker run` uses resolves to that
  content on the claiming box; a content-sealed action still names an image
  itself, exactly as an ID-sealed one does. Changing the covered set changes
  every digest, which fails closed: an already-sealed reference stops
  matching and its item stays ready.
- **Discovering a reference.** `python3 -m prismabuild.container_images
  <local ref>` prints the content reference for an image the local daemon
  resolves, through the same bounded, endpoint-pinned, environment-scrubbed
  read the inventory uses.
- **Identity.** Present, the normalized references participate in the action
  params (hence the key) and in `container_owner`'s pre-owner identity, so
  two actions differing only in the image never share a Docker ownership
  label or `<owner>.used` marker. Absent, the action, its owner and its queue
  item are byte-for-byte what they were before the field existed.
- **Capability.** A declaration requires the `container-image-v1` placement
  tag, offered by loops whose code performs the claim check. A loop from
  before the check cannot match image-pinned work; a loop that has the check
  but no readable Docker still offers the tag and fails closed at claim.
  `PoolQueue.publish` adds the tag with the references and refuses an item
  that carries the tag without references, and every declaration check runs
  before the publication's first side effect (including retiring a live
  withdrawal), so a refused publication changes nothing.
- **Producer responsibility.** Deriving the queue projection from the sealed
  params is the producer's job: `pbrun`, `fleet_submit` and `pbcampaign` do
  it. The direct `PoolQueue.publish` API validates the references and the tag
  pairing it is handed and never re-reads the CAS request to prove the
  comparison -- the row is trusted, not cryptographically verified against
  the sealed body.
- **Placement evidence.** A worker announces the references its local Docker
  positively holds (`container_images` on the offer). An offer with no field
  is unknown, not empty, and matches no image-pinned item. `_matching_offers`
  requires every declared reference, so `placeable`, `placeable_hosts`,
  `placement_census` and the submit-time refusal all read one matcher. With
  no offers on record at all, an image-declared submission is refused rather
  than submitted unchecked.
- **Claim evidence and staleness.** The worker's poll probes its local
  Docker once per box per TTL through a shared, private, no-follow local
  record; that record answers the offer. A claim reads it with a short
  freshness bound (`CLAIM_FRESHNESS_S`), re-probing under the same lock while
  image-pinned work waits, and the probe runs outside every pool lock. A
  missing reference denies (`container_image_absent`, digest named) and an
  unreadable inventory denies (`container_image_presence_unknown`); neither
  records a pass, spends an attempt or takes a token, so the item stays
  `ready` for a box that has the image. An image removed between the
  observation and the container start is the residual race; the action's own
  failure reports it.
- **The probe is two bounded reads.** One `docker image ls` answers the ID
  and `repository@` forms; one `docker image inspect` of exactly the IDs
  that listing named answers the content form. Both spend a single
  `INVENTORY_TIMEOUT_S` budget, so the pair cannot hold a worker's poll for
  twice the ceiling. Anything unreadable in either read makes the whole
  inventory unknown, never the listing alone: an inventory holding IDs but
  no content references would answer a content-form requirement with
  `container_image_absent` on a box that holds the image, which is the
  misleading refusal #805 is about. `docker image inspect` exits nonzero
  when a named ID is gone, so an image removed between the two reads makes
  one refresh unknown and the next one heals it.

  Cost and ceilings, measured 2026-09-21: the listing takes 0.25 s for 27
  images on the containerd store and 0.03 s for 32 on the classic store; the
  inspect takes 1.0 s and 0.06 s for the same sets, returning 206 KB and
  347 KB. At roughly 10 KB per image, `MAX_INVENTORY_BYTES` (8 MB) is the
  binding limit at about 800 images, and the record's
  `MAX_INVENTORY_ENTRIES` (4096) now holds up to three entries per image
  rather than two. A box past either ceiling reports unknown by design.
- **Rollout.** The inventory record schema is
  `prismabuild.container_image_inventory.v2`. A loop of the earlier
  generation reads a v2 record as foreign and answers unknown, so it refuses
  image-pinned work and leaves ordinary work untouched; the new form reaches
  the fleet only when a runtime generation carrying it is published.
- **No transfer.** PB never pulls, loads or copies an image. Archive-backed
  specs (`container.archive`) establish presence inside the action and must
  not declare it: the claim check would refuse before the loader ran.
- **Transport.** The check is a pull-queue claim decision over worker offers;
  `--container-image` is refused on the SLURM lane, and so is a sealed
  action that declares one when another producer submits it there, because
  that lane has no inventory to verify.

### Progress-bounded execution

An action may declare, in `params.progress`
(`prismabuild.action_progress_policy.v1`), an ordered closed list of phases
with a positive `grace_s` each. The value participates in the action key, as
`execution_timeout_s` does, so an action admitted under the contract is a
distinct action from its unbounded twin. Declaring it changes what bounds the
action: the worker's ceiling is applied to each phase's `grace_s` rather than
to total duration, and total duration is bounded only by an explicitly sealed
`execution_timeout_s`.

`pbrun --progress-phase NAME=SECONDS` and its explicit alias
`--progress NAME=SECONDS` append the same ordered phase declarations. Both
require the caller to choose the allowance; neither adds an implicit phase
or total-duration deadline.

`--progress-cycle` (campaign row `progress_cycle: true`) adds optional
`cycle: true` to the v1 policy. Omitted or false retains the existing canonical
linear policy and its action key. Cyclic policy bytes produce a distinct key;
older readers reject the extra field. The progress record format is unchanged.

In cyclic mode the current phase may return to an earlier declaration. Every
phase may grant its allowance once between strictly increasing cumulative
committed counts. Launch consumes the initial phase's grant at count zero.
An accepted larger count resets the available grants and consumes the named
phase's grant; at the same count only a phase whose grant is still available
can advance the watch. Regressing counts are rejected even on a phase change.
For example, after publishing unit 2 under a 5-second allowance, reporting
`encode, 2` can grant encode's 200 seconds. Repeating publish/encode at count 2
cannot re-arm either phase again. Observations may skip intermediate phases;
each accepted phase still consumes its grant. The sum of effective allowances
bounds quiet after the last count increase as well as an action that never
commits. The worker's per-phase clamp, hard deadlines, withdrawal and
containment retain their precedence. Endings record `progress_cycle` and the
current accepted phase and grace in `progress_observation`.

The channel is `claimed/<key>.progress`, named to the action through
`PRISMABUILD_ACTION_PROGRESS_PATH` with a per-launch token in
`PRISMABUILD_ACTION_PROGRESS_TOKEN`. Both are forwarded into the action's own
environment by `run_local_action` -- almost the only variables that are, and
only when the sealed params declare the contract; an action that seals either
name itself is refused rather than overwritten. The token is minted per launch,
not per key, so an action that outlived SIGKILL on a previous attempt and still
holds the path cannot report for its successor.

Two more variables travel with them, and they are conveniences rather than
channel (#488): `PRISMABUILD_ACTION_PROGRESS_PHASES` is the sealed phase list
as a JSON array, and `PRISMABUILD_ACTION_PROGRESS_HELPER` is the absolute path
of `prismabuild/progress.py` inside the runtime generation that launched the
action. `progress.py` is a leaf module -- standard library only, no
intra-package imports. `core` mirrors the contract constants and retains the
exported `prismabuild.core.report_action_progress(phase, units_completed)`
compatibility writer without a repository import, preserving its standalone
attestation boundary. That entry point keeps its original explicit-phase,
path-and-token-only contract. Tests compare its wire records, including exact
integer counts, with `prismabuild.progress.commit`. The package-level
`prismabuild.report_action_progress` and module run by path or as a program use
the new helper; the skill's container snippet emits the same record format.
A missing path or token is still refused as no channel at all; a
missing phase list or helper path identifies an older worker generation, which
can still run previously sealed watchdog actions. New progress submissions
require the helper capability described below. An action that seals any of the
four names is refused.
`commit` defaults the phase to the first declared one and raises on a name the
submission did not declare, which is the difference between a typo that reports
nothing acceptable and dies at its stall allowance and one that fails on its
first commit.

For the default linear policy, the worker accepts a record as advancement only when its schema is
`prismabuild.action_progress.v1`, its token is this launch's, its phase is one
the policy declared, `units_completed` is finite and non-negative, and either
that count exceeds the highest accepted so far or the phase index exceeds the
highest entered so far. Each phase re-arms its allowance at most once, so
`sum(grace_s)` bounds an action that never advances at all, and that sum is
reported (`progress_no_progress_bound_s`). The count is cumulative across all
phases, starts at zero, and preserves integer precision. A first zero report
in the initial phase does not re-arm startup. Everything else -- replay,
regression, an undeclared phase, a foreign token, unparsable bytes, an absent
file -- is not accepted; rejected records appear on the receipt as
`progress_observation.rejected_count` / `last_rejection`. `_observe_execution`
is untouched and remains a separate, differently-sourced sample: launcher
liveness and pipe bytes are still not evidence of application progress.

Timing uses `time.monotonic()`, so a wall-clock jump in either direction
decides nothing; the record's own `reported_unix` is carried but never
consumed. Time spent in the loop's own synchronous checkpoints is refunded to
the stall clock exactly once, including the initial lease write. The file is read on the
lease-heartbeat cadence, in the directory the lease already writes to, and
once more immediately before a stall would end the action so a record
published between polls still counts. Normal completion also samples the final
report before removing it, without changing the completed action's verdict.

Reports use the existing stable no-follow regular-file reader with a 64 KiB
accepted-byte limit. Symlinks, FIFOs, oversized or changing files are rejected;
strict UTF-8 JSON rejects duplicate keys, malformed data and non-finite values.
Parser depth errors and unrepresentable timestamps cannot escape into action
termination. Missing reports retain the current grace; invalid reports count
as rejections and do not extend it. These are byte and type bounds, not a hard
deadline on NFS syscalls: like the existing lease and withdrawal checkpoints,
a regular-file operation can block in the kernel. Shared-filesystem recovery
remains tracked by #16; this contract introduces no new queue or recovery owner.

Termination precedence is unchanged with one rung added at the bottom:
resource containment, withdrawal, the sealed deadline, then the stall
allowance. A stall files `status: timeout` with `termination_reason:
no_progress`; the sealed deadline files the same status with
`execution_deadline`. Every ending, including the action's own exit, carries
`progress_observation` and clears the file through one funnel.

Workers announce `progress_contracts` on their offer, and `pbrun` refuses a
progress-declaring submission when no eligible box announces support -- a box
that does not understand the policy would apply its whole-run ceiling to an
action submitted without one, which is the defect (#480) rather than a
degraded form of the fix.

Workers also announce `addresses` on their offer: the global-scope IPv4
addresses the box's kernel holds, read from `ip -4 -o addr show scope global`
(`box_capacity.ipv4_addresses`), absent when the reading could not be taken.
The storage host's prewarm pacer follows a claim's `claimed_host` to this
field and joins it to the per-client counter in `/proc/fs/nfsd/export_stats`,
which is how it tells the action it is warming for from a client it must
protect (#580; see [data_manifest_prewarm.md](data_manifest_prewarm.md)).
A box whose offer carries no addresses is protected as every client was
before the field existed.

The announcement reports; the placement tag enforces. A worker that can run
the watchdog offers `progress-v1` (`core.PROGRESS_TAG`) alongside its class
and hostname. A worker that also exports the helper path and phase list offers
`progress-helper-v1` (`core.PROGRESS_HELPER_TAG`). New `pbrun` submissions that
declare a policy require both tags. Previously sealed `progress-v1` actions
remain eligible on either generation; their requests are unchanged.

Cyclic policies additionally require `progress-cycle-v1`
(`core.PROGRESS_CYCLE_TAG`). Updated worker loops offer all three tags.
Submission refuses cyclic mode if no eligible offer proves that capability,
including an empty offer census. In a mixed fleet only cyclic-capable hosts
contribute phase ceilings or receive the action. Existing linear requests keep
their previous placement. Publication and worker adoption must precede cyclic
submission; no existing sealed policy is rewritten.

Item tags must already be a subset of the worker's, so no matcher change is
needed. During rolling adoption, an old watchdog cannot claim a new action
that depends on a helper it does not export. The record tag remains versioned
with its schema; helper capability versions the additional action environment.
On a mixed fleet the submission is narrowed to helper-capable workers. The
notice names withheld boxes and excludes their phase ceilings; it refuses a
known fleet that only supports the older watchdog.

The contract is offered only on the pull queue. `pbrun --transport slurm` and a
`progress_phases` row submitted to SLURM are refused
(`pbrun.require_progress_scope`): the watchdog is the pull-queue worker's, and
the SLURM lane can enforce only a total duration (`--time`, sent only when
`--timeout-s` was given). Sealing the policy there would admit the action on
the promise that its advancement bounds it and then run it under no watchdog
and, absent `--timeout-s`, no deadline at all. `run_local_action` refuses the
same launch from the other end: a declared policy with neither environment
variable set is an `ActionContractError`, not a silent unbounded run.

The versioned fleet configuration sets both GB10 worker ceilings to 86400
seconds for dependent full-model calibration capture (issue #385). The CPU
host retains its 3600-second ceiling. A GB10 action without an explicit budget
inherits the one-day ceiling; capture requests that budget explicitly. This
changes only the permitted duration: reservations, physical memory guards,
containment, priority and admission are unchanged. Supervisors adopt the
published configuration through the existing idle-worker transition; a live
attempt retains the ceiling under which it started.

## Campaign submission windows

`pbcampaign --max-inflight N` is optional waiting-pool controller policy, outside
sealed action identity and the resource ledger. One invocation retains at most
N distinct unfinished action keys and publishes a replacement only after a
successful `pbwait` observation and absence of that key's READY/CLAIMED leaves,
including a finish tombstone or `.late-finish` record under `claimed/`.
`PoolQueue.finish` entombs the claim before it releases capacity and files the
ending, and a generation-pinned observation can answer from the attempt archive
inside that window (#886), so the entombed claim still holds the slot.
A receipt or withdrawal outcome alone cannot free a slot while queue work or
claim cleanup remains. Leaf read errors stop publication; they grant no capacity.
An outcome read that timed out with its reader reaped keeps the key pending and
its slot held until the shared deadline, then reports exit 74.
Rows keep ordinary sealing, placement, admission, containment and receipts.
The initial window is published before the shared monotonic wait budget starts;
expiry leaves published work intact and reports the unsubmitted suffix.

The controller stops refilling at any refusal, failed action or unreadable
observation, preserving an ordered resumable prefix. Continuing beyond a failure
would let a restart republish failed early rows before encountering later live
work. Restart requires the previous controller to stop and the ordered manifest,
source identity, options and limit to stay the same; existing pbrun cache/attach
semantics recover that prefix. The one refusal that does not stop refilling is
`pbrun.OfferDiscoveryTimedOut`. It is raised before any runnable publication,
and only after its reader was reaped, so the controller holds no slot and
resubmits the same row each poll until the shared wait budget expires. The row
is then `not_submitted` (#560). No durable parent or background dispatcher is
introduced. Concurrent controllers and other producers do not share this count;
there is no per-host or bandwidth guarantee. The option refuses detached and
SLURM modes. Omitted policy retains the existing submit-all campaign behavior.

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

Worker membership uses this same broker maintenance gate and the existing
roster, offers, queue withdrawal and reaper machinery. The published
`fleet_membership.py join` command qualifies a registered local worker before
opening its gate. `resign` closes admission first, requests handoff only for
retry-safe owned work, and waits for exact-attempt containment and reader
settlement before declaring departure. Unknown ownership or cleanup evidence
retains the hold. It neither terminates unrelated work nor treats absence from
one census as proof of departure.

Retry handoffs preserve the sealed work unit, finite attempt budget, residency
declaration and frozen window plan. A durable carrier binds the withdrawal to
the live claim's action, nonce, broker scope and publication lineage; an
operator label alone grants no retry or plan-preservation authority. Repeated
handoffs follow that lineage without resetting the budget. The implementation
and heterogeneous-worker qualification boundaries are specified in
[the fleet expansion contract](../worker-expansion-design.md), with
[per-requirement evidence](fleet_expansion_requirements_2026-09-20.json) and
[component merge acceptance](fleet_expansion_acceptance_2026-09-20.json).
Component qualification does not establish deployed JOIN/RESIGN conformance.

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
ownership fields only after a broker status reply advertises support. The
worker-facing `/run` gate retains its v1 schema so older brokers can read it;
the broker keeps its canonical maintenance record in root-only host-local
`/var/lib/prismabuild-resource-broker/maintenance.json`. An adjacent initialized
marker distinguishes first migration of a valid volatile gate from erased durable
evidence, which fails closed. A durable close is written before its volatile
mirror; durable release is written before the open mirror, and a post-commit
mirror failure keeps the running broker closed. At startup an absent volatile
gate turns even a durable open record into a persisted `client-upgrade` boot
hold, released only after the updater's current-client and health checks.
The upgraded updater refuses candidates lacking either durable broker or
durable updater support, and refuses missing running capability. That guard
takes effect after the coupled updater/broker generation converges; the older
updater executing the first transition may still restore its previous files.
This host-local authority retains the fleet rollout hold across reboot; shared
epoch decisions, described below, determine when that hold may be released.
A loop that parks on a drain records that it parked, one file per process per
drain under `/run/prismabuild/rollout/parked/`, named for the gate's
`changed_unix` so a marker left by an earlier drain reads as the earlier drain.
The marker also carries the PID and procfs start time. The write is best effort,
so a loop that cannot record its park still parks and missing evidence cannot
certify a drained host. The updater creates the directory for the unprivileged
worker uid and reports whether every serving process has a marker for the
current drain and the broker reports no active scopes. Processes count by argv
basename: `worker_loop.py`, `worker.py` and `prewarm_loop.py`, including one-shot
or storage invocations through the symlink, either published layout or a local
checkout. A storage reader missing an exact current-drain PID/start-time marker
prevents a positive observation even with zero broker scopes. Legacy storage
readers without parking support remain unparked until they exit.
Unreadable or malformed process evidence prevents a positive result and is
reported explicitly; only a process directory proved gone may be omitted after
a read failure. Process start identity is checked around the argv read. The
stat start tick is nonnegative, and 0 is valid -- PID 1 reports it on this
kernel -- so only a malformed or negative tick refuses. Missing gate identity
or a gate change during the census also prevents certification.
The drain identity comes from the gate file. This is an observation of the
current processes, not an admission barrier or a guarantee against future
process launches; it neither opens nor closes a drain. Ordinary privileged-client
convergence still gates on broker active scopes, not this reported observation.
The updater also records, fleet-wide, which version of itself has run. It is
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
`publish_runtime.py --rollout barrier --dry-run` reads that historical tree as
a bootstrap preflight. Every roster box must have posted an attestation naming
the sha256 recorded for the target updater; refusals name missing boxes and
their previously posted versions. A box answers under its roster key or its
declared alias (`gx10-6b77` / `sparklina`). A box the roster declares absent
(`status` `retired` or `offline` in `fleet_boxes.json`, #606) is skipped by
the preflight and by the epoch roster instead of vetoing them: an offline box
must not block a publish for the boxes that are live. The declaration needs
its provenance -- nonblank `status_reason`, `status_by` and a finite
`status_unix` -- and an unknown status or a missing provenance refuses
wherever the roster is read. The skip is said out loud, so a stale retirement
cannot pass silently. The epoch roster excludes the same boxes from its
quorum, and refuses if an absent box is still announcing (stop its loops or
un-declare the absence) or if a box group mixes absent and active names. The
supervisor side converges an absent box's loops to zero -- no spawns, no idle
reserve, mid-action loops finish first -- so its offers expire and placement
stops seeing it; a fresh supervisor refuses to start there at all. Roles
already running are left to the operator's stop. New-publication preflight uses the
source manifest, and `--activate-generation` preflight uses the existing
generation's receipt. The publisher loads the updater's marker-name function
and member key from its checkout. A marker counts only when its schema and
body reproduce its filename through that function.
These historical markers grant no activation authority. Public barrier mutation
remains disabled by `FINAL_BARRIER_QUALIFICATION_GUARD` pending real host lifecycle
qualification. The following describes the guarded protocol, exercised by private
qualification actors. With the guard removed, the publisher defaults
to `barrier`: it seals and verifies a candidate, then writes a fresh immutable
intent under `rollout/epochs/<epoch>/intent.json`. The intent fixes the source
and target generation, sorted host roster, participating updater SHA-256 and
`wait` drain policy. The roster includes both generations' declared hosts;
fresh offers resolve aliases and reject undeclared or ambiguous live identities.
Offers do not count as participation. Both sealed generations must contain the
same rollout-aware updater. A new updater therefore needs a reviewed rolling
bridge and fleet-wide installed-hash verification before a barrier can use it.

Each root updater persists the epoch and intent hash on local durable storage
before participating. It reads bounded shared evidence through its root-owned
export program running as the configured reader uid, independently validates
that evidence, and keeps its owned durable maintenance hold closed. An unreadable
or corrupt rollout tree, missing persisted epoch, another drain holder, incomplete
process census or unhealthy broker cannot authorize release. Each host posts a
fresh `drained` record only with zero active scopes and all worker, one-shot and
prewarm processes parked on the current drain. Its executing and installed updater
hashes must match the intent. Existing actions finish naturally; this protocol
does not interrupt or requeue them.

The epoch records and `repo` pointer publish atomically but are read separately.
If a valid pointer move falls between an updater's desired-receipt read and its
next epoch refresh, the updater keeps its durable drain and retries with a fresh
view. It does not write `failed`: that marker is reserved for a verified local
transaction, recovery, or service failure and would otherwise trigger an
unwarranted coordinated rollback.

Only the complete drain quorum permits the coordinator to atomically move `repo`
and record `activated`. Each participant then verifies its desired and installed
privileged bytes and healthy loaded broker, and observes exactly one supervisor
and at least one worker loop running from the selected immutable generation.
Workers, one-shot workers and the continuous prewarm loop must be parked on that
host's current drain. The prewarm loop finishes its current cycle before parking
and follows generation changes even while parked. Only the complete `rotated`
quorum permits a shared `resume` decision. Every host rechecks local rotation and
health before releasing its own gate and posting `resumed`; the complete resumed
quorum permits a terminal `completed` record.

Intent and phase records are write-once, published by fsync plus exclusive link.
Every marker carries its epoch and canonical intent hash. Coordinator decisions
bind the exact canonical SHA-256 of every required participant record; readers
validate the complete dependency chain. Diagnostic wall-clock timestamps never
establish ordering or replace quorum. A permanent POSIX publication lock with
in-process exclusion serializes publishers across the NFS mount and its local
server path. Never unlink this lock. An active or unreadable epoch blocks ordinary
publication and explicit rolling activation too.

Before a resume decision, a participant failure or explicit `--rollback-barrier`
records rollback under the same drain quorum, restores the exact source pointer,
and waits for every host's `rolled-back` proof before authorizing resume. A local
transaction failure restores verified previous bytes while retaining the fleet
hold. An interrupted coordinator can replay a pointer move that preceded its
activation marker. `--resume-barrier` continues the same immutable intent;
`--barrier-wait-s` bounds the coordinator's polling between completed reads,
returns 75 on expiry and
never releases admission. Rollback after resume is refused and requires a new
epoch. No missing-host exclusion, quarantine or interrupt/requeue policy is
implemented: an unavailable participant leaves the epoch pending until repaired.
Direct coordinator filesystem reads can block past that polling deadline; they
retain the publication lock and admission holds until the read or process ends.

`--stage-only` seals and import-probes through PB without arming an epoch or moving
`repo`. The subsequent activation/recovery coordinator performs control-plane
work outside the live fleet's own admitted scope, which it must drain; it refuses
to wait on its own scope. Historical `--dry-run` attestation checks remain bootstrap
diagnostics, not a simulation of the epoch or permission to activate. Independent
host convergence requires explicit `--rollout rolling --rollout-reason TEXT` with
a nonblank compatibility explanation, recorded in a new generation receipt.
Existing-generation rolling activation requires its own reason. A reason neither
proves compatibility nor waives the publication window.

Fresh publication runs the fleet canary (issue #688) after activation by
default. `CANARY_DEFAULT_ENABLED` is a versioned source constant, `True` since
2026-09-19 (phase 2) after the first verified live 4-leg run
(`pb-canary/20260919T173543Z`, exit 0, leg-4 envelopes bitwise-equal across
sparky+sparklina); phase 1 landed default-OFF with `--canary` as the opt-in.
A leg-3 verdict additionally requires staged reader-lease routing and the
worker's accepted cumulative progress (issue #784), not digest equality on
originally opened paths; see the rollout runbook's leg-3 gate section.
`--no-canary` is the skip, and the outcome lands in the generation's sibling
rollout record (`verified`/`failed`/`not_run`). A failed canary marks `failed`
and exits nonzero; it never rolls back the activation or touches admission. The
running fleet adopts this default only when a generation carrying it is
published.

The source delivery keeps `FINAL_BARRIER_QUALIFICATION_GUARD` enabled. Every
public barrier mutation, including publication, activation, resume and rollback,
therefore refuses before staging or stepping the epoch state machine.
`--stage-only` remains available because it only seals and import-probes a
candidate. The private qualifier disables the guard only after asserting that
its root is below `/mnt/shared/pb-qualification`; its simulated broker, service
and process census remain source evidence, not live-host qualification.
An epoch also binds the exact SHA-256 of `publish_runtime.py`. Source and target
generations must carry the same updater and coordinator bytes, and recovery
rechecks its captured coordinator bytes plus the imported updater-marker
semantics before every pointer move or decision publication. Import retains
only the loaded module code object; the first operational identity read hashes
source bytes after confirming they compile to that code, before reading an
epoch. Subsequent reads require the captured byte hash, so source changes cannot
silently replace an already loaded coordinator. A coordinator
change therefore needs a reviewed rolling bridge before it can drive a barrier.

Workers, including the one-shot entrypoint, refuse admission when the local
`/run` gate is missing. After boot the updater initializes the gate through the
broker only after desired and installed clients match, loaded hashes and health
agree, and no active scopes remain. An active epoch additionally requires its
quorum-backed resume decision. An existing owned drain waits for zero scopes;
another holder's drain stays held. Unavailable reads are not absence, and
`current` requires an explicit open gate readback. The volatile worker-facing
mirror and root-owned durable authority serve these same rules after reboot.
Maintenance refusal before payload launch returns a claim to ready without
burning an execution attempt. The published store is explicitly authorized to
supply these privileged bytes; manifest hashes provide copy consistency, not
an independent signature. See [client upgrades](client_upgrade.md).

The storage prewarm role also checks the existing maintenance gate before each
queue cycle. A closed, missing or unreadable gate parks the continuous process
and records its PID/start-time marker through the worker's existing helper.
`--once` returns 75 without starting a cycle under the same conditions, including
with `--dry-run`. An in-progress cycle finishes before the process records a
park; disk holds and filesystem waits can therefore delay parking. At the next
boundary a changed runtime commit or generation makes the process exit for
supervisor replacement, even while parked. One-cycle invocations return 75
on that transition so an unperformed cycle is not reported as complete.
The gate and runtime are rechecked after disk setup, immediately before the
cycle, so topology-discovery delays do not preserve an earlier open decision.

The `tiers` role reads the same runtime gate at the top of every cycle, and
`--once` returns 75 on a moved generation for the same reason. It did not, and
nothing else reached it either: the installed unit runs `supervise.py --ensure
--systemd`, so `cycle_stale` is an operator verb rather than part of a publish,
and it walked worker loops only — `_stop_idle_loops` re-proved ownership
against the worker script, which drops a role pid a second time. A tier loop
therefore served bytes two publishes old while every worker on the box had
moved. The cycle boundary is the safe place to exit: every mutation the loop
makes is one atomic rename, and the map, its only composite, is recomposed from
the fragments on disk each cycle rather than accumulated in the process.

`cycle_stale` now covers roles too, on a narrower rule than a worker: a role is
cycled only when the generation it is running differs from the published one. A
worker respawns in a poll interval and the box has others; a role is a
singleton, so stopping a live-generation one costs a cycle of a service nothing
else provides. The idle rule is unchanged — only `SIGTERM`, only a loop holding
no action.

### The generation-drift handshake (2026-09-19)

On 2026-09-19 the fleet's active generation sat on a four-day-old tree while
`origin/main` advanced through ~8 republications, a supervisor re-exec'd itself
into the successor without touching its role children, and a `prewarm_loop`
kept the pre-#703 `--readers 1` shape for hours. Nothing refused, and nothing
wrote a record a reader could find afterwards; the drift was found by forensic
inspection. The handshake closes both halves, fail-closed and without any new
daemon:

* **A claim is taken only under the active generation.** `worker_loop`'s
  poll-top fence guards the poll, but offer publication and queue discovery
  sit between that fence and the claim, and a publisher can activate a
  successor inside exactly that window. So immediately before `serve_once`
  the loop re-reads the one tiny `RUNTIME_VERSION.json` through the live
  `repo` name — the same existing reader, one file per claim, no second
  path — and compares it with the generation its own bytes were loaded
  from, derived from `__file__`'s immutable root. On mismatch it refuses
  the claim, stamps one record, and exits so the supervisor respawns it.
  The same check runs at startup before the first claim, in both
  directions: a loop resurrected from a generation newer than the fleet
  retreated to refuses just as an old one does. An unreadable receipt is
  "unknown", not "moved", and never licenses a refusal — the same rule the
  reload fence has always kept.
* **`--ensure` ensures the declared role, not a process's existence.** A
  running role whose argv differs from the current `fleet_boxes.json`
  declaration, or whose executable resolves outside the active generation,
  is stopped when idle and respawned from the active generation once the
  census reports it gone (since #709; a SIGTERM request is not an exit, and
  an old generation's role holds no singleton lock that could keep the
  replacement from serving beside it). The idle rule is every restart path's:
  SIGTERM only, never a role mid-cycle, so a stale role finishes the service
  cycle it is inside and cycles on a later tick.
* **Every refusal is stamped.** One `generation-drift/` record namespace
  under the queue root, written by the one shared helper in
  `worker_loop.py` (the module the roles already import as `runtime_gate`
  and the supervisor now imports too), following the queue's immutable
  record conventions: canonical JSON, atomic first-writer link, mode 0444,
  naming both generations, the actor's pid and host, a timestamp, and — for
  a supervisor restart — the role, the argv it was running, the argv the
  declaration names, and which rule fired. One record per incident, not
  per poll: every writer stamps on the way out the door. A record that
  cannot be written is reported as `UNWRITABLE` and the refusal still
  happens — the refusal is the safety, the stamp is the evidence.
* **Rolling rollout convergence is untouched.** `publish_runtime`'s rolling
  mode relies on loops cycling at their own boundaries; the handshake sits
  at claim boundaries only, an executing action is atomic to the loop and
  finishes under the generation that claimed it, and a worker that refuses
  leaves the ready record for a successor to claim. Nothing interrupts
  work in flight, so the handshake cannot fight a converge.

### The role singleton and stopped-role health (2026-09-19, #709)

The handshake above makes a running role *fresh*; it does not make it
*single*.  On the same night a duplicate supervisor ran beside the primary on
the storage box, and each was free to start its own ``prewarm_loop`` and
``tier_loop`` against one queue: two readers of one ready list, double-published
movers and contradictory fill measurements compounding the fill-capacity wedge.
The supervisor's own host-wide claim already refuses a second supervisor, but
the claim is the launcher's, not the role's: role entrypoints previously had
no per-role lock, so a direct invocation, a legacy loop or a stale
generation's supervisor could serve a second role beside the current one.
The guard exists because a role cannot depend on its launcher being single.

* **A service role owns a host-local singleton lock.**  ``worker_loop``'s
  ``take_role_singleton`` takes a nonblocking ``flock`` on
  ``/tmp/prismabuild-roles-<uid>/<script-stem>.lock``: a private per-uid
  directory under the admission lock's discipline, the script name rather
  than a generation so a republished role and a stale one contend on the
  same inode, and the file is never unlinked.  The exclusion lives on the
  open file description and is released by its final close on every exit
  from the serving block -- a service rotation, a one-shot return, an
  exception -- never by an unlock or an unlink, so a lock taken by a
  one-shot invocation cannot leak into a hosting interpreter.  A loser exits
  ``3``; the holder pid is a diagnostic from the admission lock's own
  ``/proc/locks`` rule, read only when a refusal needs decorating, and an
  unreadable holder is "unknown", never "nobody", and never changes the
  refusal.  The one-cycle operator form is not exempt: a ``--once``
  invocation can mint, announce, warm and publish against the real queue, so
  it takes the same lock and the same refusal.  An operator cycle beside a
  running role needs the role stopped first (or another box); the per-tier
  mint lock still serializes the one-shot runs that do start.
* **The supervisor probes the lock before spawning.**  A live role the
  ownership census cannot prove -- a duplicate supervisor's child, a
  hand-started loop, a stale generation's -- can still hold the lock.  The
  probe reports the holder in the supervisor's own log and starts nothing
  that would only refuse; the child proves the same lock again at startup.
  Anything unproven remains unsignalled, exactly as before.
* **A SIGTERM request is not an exit.**  A signalled stale role stays
  counted as live until the ownership census reports it gone -- whether it is
  stopped with the TERM queued behind ``T``/``t`` or still finishing a cycle,
  and whether or not it is new enough to hold the singleton lock at all (an
  old generation's role holds none).  The replacement starts on the next
  tick, after the predecessor exits: at most one interval later, and never
  as a second reader beside it.  The health line names a stopped one and an
  operator's stop is not raced.
* **A stopped role is named.**  A SIGSTOPped loop answers ``pgrep`` and
  appears present in every census, exactly as a quiet one does, so the
  supervisor reports ``/proc/<pid>/stat``'s scheduler state on each
  transition: ``role storage pid N state T (stopped)`` in its log, and the
  shut down pending line names the state holding a stop open.  A state that
  cannot be read is "unreadable", never "stopped"; ``Z`` is a zombie, dead
  and awaiting reap, not an alarm.  The guard never sends ``SIGCONT`` and
  never starts a replacement before the predecessor actually exits; the
  existing stale-role rotation may still queue its ``SIGTERM``, which the
  kernel holds while the role is stopped.  Resuming is the operator's to
  undo (``kill -CONT``), and the stopped holder keeps the singleton lock
  while it is diagnosed.

The updater includes this storage reader in its drain observation using that
same marker. These checks establish no cross-host quorum and do not enable
barrier activation. The #458 protocol still needs fresh epoch participation
and a generation-uniform rotation proof that includes this reader.

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
programmable GPU-only power limit. Its actual draw is GPU-only, so the number
admission divides it by is GPU-only too: `adaptive_gpu.admission_power_reference`
returns the driver's programmable limit when there is one, and otherwise the
highest draw this host has sampled from that device, floored by the declared
per-device capacity fact `DECLARED_GPU_POWER_REFERENCE_W`. The published 140 W
SoC TDP stays on the sample as `power_reference_w` with scope `soc_tdp` for
display and provenance, and is never a denominator: it covers CPU power, and
the measured GPU peak on these boxes is 106 W to 114 W (#806). The ratchet is
capped by that published envelope, so one implausible `power.draw` cannot raise
the reference. A device with neither a driver limit nor a declared entry has no
reference, which leaves its sample invalid and refuses.

Everything that *reports* a power ratio divides by that same reference.
`adaptive_gpu.reporting_power_reference` defers to the admission derivation and
differs from it in one direction only: admission refuses a device it has no
GPU-only reference for, while a reader has nothing to refuse, so a device that
publishes only its vendor SoC envelope is still described --- under the
`soc_tdp` scope, which says the denominator covers CPU power the numerator does
not. The scope travels beside every published ratio, in the receipt
(`box_window.gpu.power_reference_scope`), in the ending summary
(`gpu_power_reference_scope`), in the `pbstatus` and `pbrun` resource line, in
the `pbmetrics` `scope` label and in the placement offer, so a fraction is never
read against a reference nobody named. The measured half of the reference lives
only in admission's host-local `gpu-state.json`; a reader reaches it through
`adaptive_gpu.host_local_power_state`, and failing to reach it falls back to the
declared GPU-only floor, never to the SoC envelope (#806). Utilization percentage
is not treated as a saturation measure. Any foreign GPU process closes admission on these
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

### AMD devices, and a device with no saturation instrument (2026-09-12)

`gpu_capacity.devices()` reads NVML first and, only when NVML found nothing and
`rocminfo` is installed, an AMD reader that publishes one device from two
runtime sources that agree on every quantity both can see: the HSA agent report
for identity, architecture, CU count, wavefront, peak clock and the VRAM pool,
and a short-lived HIP subprocess for device count, free/total VRAM and the
integration attribute that states the memory domain. Disagreement publishes
nothing. More than one AMD GPU agent publishes nothing, because one HIP ordinal
is all the probe reads. The VRAM total is keyed on `Device Type: GPU`, never on
pool order: the first `GLOBAL` pool in a `rocminfo` report is the CPU agent's
host RAM. Both readers are refused unless they are root-owned and writable by
nobody else, because the broker runs `rocminfo` and loads `libamdhip64.so` as
root.

Each device record declares its `telemetry_class`. `power_and_clocks` is the
NVML contract. `memory_only` is a device whose runtime publishes identity and
memory and no power, clock or throttle counter at all, which is the AMD/WSL2
case. The adaptive controller admits a `memory_only` device on the evidence it
carries — identity, memory domain, free VRAM against the declared budget,
foreign holders, host memory and CPU pressure — and withholds the two
permissions power exists to authorize: `low` is never true, so there is no
concurrency probe and no `measurement` action, and the device runs one
attributed job at a time. Absent power *without* the declaration remains
invalid, so this narrows one declared class of device rather than weakening the
contract for every sample.

Attribution on such a host is a census of the GPU device node's open handles in
`/proc`, routed through the same PID start-time and cgroup-identity checks as
the NVML rows. It resolves ownership and reports no per-process bytes, which
the sample declares as `gpu_process_bytes: false` and each scope as
`gpu_budget_enforceable: false`: the Guard does not confirm a GPU-allowance
violation it has no counter for. Ownership unknown still refuses; bytes unknown
no longer does. A `shared_system` device without per-process bytes has no
system-memory lower bound to state and stays incomplete.
`foreign_inventory_scope` records how far the census could see —
`gpu_compute_apps` for NVML, `host_gpu_handles` for the node census, which
covers the processes this `/proc` lists and nothing outside it. A handle is
identified by the character device's device number, not by the node's inode: a
container runtime creates its own node for a passed-through device, so an inode
comparison would report a containerized GPU user as holding nothing. An
unreadable descriptor table refuses rather than reporting an empty foreign
list. Measured
evidence and the residual risks are in
[amd_gpu_capacity_2026-09-12.md](amd_gpu_capacity_2026-09-12.md).

### Memory-only GPU action windows

For a contained GPU action on a `memory_only`, `discrete` device, the worker
samples the existing root-owned GPU capacity snapshot during its running-scope
telemetry ticks. It retains distinct fresh observations for that exact attempt
and device in constant space. No extra HIP or NVML probe is launched per action.
The resulting `resource_profile.box_window.gpu` group declares
`source: broker_gpu_capacity`, `telemetry_class: memory_only`,
`memory_domain: discrete`, device identity, sample count and first/last sample
timestamps. `framebuffer_total_bytes`, `framebuffer_used_bytes_peak` and
`framebuffer_free_bytes_min` describe the whole device during observed instants.
They include other users' occupancy and are not per-process allocation or an
enforceable per-attempt GPU budget. No power, utilization or unified-memory
field is invented. This is diagnostic run metadata, with no admission or
action-identity change.

Repeated, stale, malformed or mismatched observations add no samples. A window
with no accepted running-scope observation has no framebuffer group; a single
finish-time driver reading cannot reconstruct a prior peak. Sampling can miss
short-lived allocations and does not certify complete interval coverage. CPU
windows remain sourced from Netdata and GB10 windows from their existing
recorders. Status and metrics expose discrete VRAM peak/total separately from
host cgroup memory and GPU power.

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

### Cross-resource placement preference

A box that is already working one resource prefers not to take work for the
other one, when another box can have it. Before reserving, a worker compares
its own load on the resource the action does *not* want against every
compatible offer's: GPU work arriving at a CPU-busy box, or CPU-only work
arriving at a box drawing GPU power. If a compatible box reads materially
freer on that axis and can fit the whole demand, this worker gives it up to 20
seconds to claim, records `deferred_for_cross_resource_placement`, and then
claims the action itself.

This is a preference, not a requirement, and it is best effort in both
directions. The work is never refused, never starved and never placed where it
could not run; the only effect is a bounded wait that a better placement may
or may not win. With no alternative, a stale reading on either side, or no
GPU-power evidence, there is no preference at all.

It is not a thermal control and nothing here measures temperature or
throughput. The GPU side reads drawn power against a reference, because
`gpu_utilization` reports a resident kernel rather than working SMs. The
reading is GPU-only, so the reference is: `box_capacity.observe` asks
`adaptive_gpu.reporting_power_reference` for it and divides by the same watts
admission does, rather than writing a second rule down (#806). It is given
admission's host-local state by the worker loop, so a ratcheted measured peak
is the denominator here too; without that state the declared GPU-only floor
applies, which is coarser and still GPU-only. A device nothing better is known
about keeps its published SoC envelope, and then the scope below says `soc_tdp`
so a reader knows the denominator covers CPU power the numerator does not.
Two fractions leave `box_capacity.observe`: `gpu_power_measured_fraction` is
the raw sampled draw over that reference, while the legacy `gpu_power_fraction`
is the congestion proxy the fleet already published (`max(raw, 1.0 if limited)`,
the same reading `adaptive_gpu` calls congested). Placement prefers the
measured fraction and falls back to the legacy proxy for old offers, so an
idle SW-capped GB10 (~0.03 measured, 1.0 proxy, Sep-20 Sparklina flap) does
not defer CPU work. `gpu_power_reference_w` and `gpu_power_reference_scope`
name the denominator behind the measured fraction, so a ratio and what it is a
ratio of are never read apart. The limiter itself travels as `gpu_limited` with
`gpu_throttle_mask` / `gpu_throttle_reasons` for diagnosis. The CPU side reads
`load1` per preferred core. All come from the offer's `observed_detail`
(`gpu_power_measured_fraction`, `gpu_power_fraction`, `gpu_power_reference_w`,
`gpu_power_reference_scope`, `gpu_limited`,
`gpu_power_sampled_unix`, `observed_unix`, `load1`) and
both must be fresher than `GPU_SAMPLE_MAX_AGE_S`. The two thresholds --- when a
box counts as busy, and how much better an alternative must look --- are
`PoolQueue.CROSS_RESOURCE_BUSY` and `PoolQueue.CROSS_RESOURCE_MARGIN`. They are
heuristic, which is why they bound a wait and never a decision. The margin is
what keeps two similarly loaded boxes from deferring to each other.

Worker loops poll on a longer cadence than the freshness window, so on many
scans neither reading qualifies and the preference does not apply. That is
consistent with its being best effort; it is not a defect to tune away.

Physical CPU tokens are the conservative baseline, not a fixed concurrency
gate. The adaptive controller samples busy time for every CPU in the worker's
inherited mask and host CPU pressure. That host-level view includes processes
outside PrismaBuild, so unrelated load can close admission even when the pool
ledger appears free. Samples are short-lived. A fresh sample at or above 95%
occupancy refuses new CPU claims outright, and a fresh PSI `some` at or above
.10 ordinarily refuses as well. One exception exists for ordinary bounded
generation claims narrower than the host: the reading is treated as a pinned
neighbour's local contention only when the CPUs this claim's own free tokens
would map to are all idle (`per_cpu_busy <= .05`) and none is held by another
action, in which case the claim proceeds on those disjoint free tokens and
borrowing is disabled for that decision. Measurements, unbounded demand that
declares no CPU count, and full-width reservations keep the pressure refusal
whatever their per-CPU reading says; a learned cheap cost and an all-zero
reading do not reopen it. Missing, malformed or out-of-range per-CPU evidence
under fresh high pressure is unknown, and unknown refuses. CPU tokens are
ordinals mapped through the preferred and fallback tiers, so "the CPUs this
claim would be given" is exactly that mapping; processes outside PrismaBuild
are not part of the token ledger, and this is admission accounting, not
per-core OS isolation. An absent or stale overall sample keeps its existing
meaning: the current gates do not run, and an ordinary bounded claim on free
tokens can still be placed, while borrowing and measurement need fresh
evidence. A local lock serializes each host's adaptive decisions, while the
shared queue's rename still decides ownership.

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
individual action's limit. The dl380g10 budget is 96 GiB against 294.523 GiB of
physical RAM, preserving the storage host's 176 GiB ZFS ARC minimum and
16 GiB system-free target with another 6.523 GiB outside the PB ceiling
(issue #488). The prior 192 GiB budget remains historical
[capacity evidence](dl380_memory_capacity_2026-09-05.md). Live host
observation may lower advertised capacity. Increasing the configured ceiling
does not resize existing reservations or their cgroup limits.

Safe lending requires complete attribution of the entire attempt, including
direct descendants and Docker containers created through a daemon. The resource
scope architecture assigns each attempt one broker-owned cgroup, launches the
payload inside it, attaches owned containers, enforces the declared memory limit
over the aggregate, records CPU time and memory peak, and proves the scope empty
before releasing its reservation. The payload runs in the scope's `payload`
leaf, which the broker sets to `drwxr-xr-x root` and verifies before any
process enters (#916). A bare-host payload can therefore read its own limits,
such as `memory.max`, as a container reads its docker scope's; it cannot write
them, create cgroups or move processes. The broker's `UMask=0077` alone would
leave the leaf `drwx------`. Parent-local `memory.events.local` `oom`
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

Pool receipt reconciliation is an explicit evidence operation, exposed by
`pbwait --reconcile-pool --generation <digest> --attempt <number> <key>`.
It requires the current failed submission generation and its final immutable
attempt, the exact broker-completion EOF diagnostic with launcher code 125,
verified immutable logs, and complete cleanup for the same action, nonce,
scope and host. Timeout, withdrawal, OOM, other resource termination,
outstanding work or capacity, and conflicting records refuse reconciliation.
The completed claimant's retained intent is accepted only when its identity
and timestamp match that attempt. The operation shares the queue's per-key
transition lock; CAS verification holds no host admission lock.

The canonical action, receipt, producer attestation and result blob are
verified before publishing a first-writer-wins immutable supplement at
`attempts/<key>/<generation>/<number>.receipt-reconciliation.json`. It binds
the original terminal and attempt hashes, log addresses, cleanup evidence and
verified action result. It records `payload_verified` for the action alongside
the unchanged failed transport and return code 125. Receipt identity does not
bind a pool attempt, so this never claims that attempt exited zero. A repeated
explicit call revalidates all inputs and refuses conflicting supplement bytes.
Normal wait, retry, admission and terminal readers retain their existing
semantics; they do not interpret the supplement as another terminal or as
permission to restart work. Shared filesystem reads are synchronous and can
delay this explicit operation; it grants no deadline or missing-data bypass.

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

On SIGTERM the supervisor stops replenishment and retains its box-local claim
while cooperatively draining its attributed workers and stopping auxiliary
roles. It uses pidfds for ownership-rechecked signals and exit waits; it never
signals payload process groups or adds an action deadline. Worker loops honor
their existing SIGTERM flag after completing the current claim and cleanup.
A blocked or externally stopped process keeps shutdown pending, as does an
unavailable pidfd acquisition or signal; the supervisor retains its claim
and retries after reporting the error. The systemd
unit therefore uses `KillMode=process`, `TimeoutStopSec=infinity` and
`SendSIGKILL=no`. Runtime re-exec remains independent of this shutdown path.

An installed unit's explicit `--ensure --systemd` ExecStart gives systemd
startup ownership even while inactive. A cron/manual `--ensure` then returns
without acquiring the claim or spawning work; an already running cron owner
rechecks each cycle and hands over without stopping workers. The installer
enables and starts the service. Legacy units retain legacy
behavior until reinstalled; the installer travels in the runtime inventory.
The stop covers supervised processes only and is not a filesystem-quiescence
or fleet-barrier certificate. Manually launched processes and stale published
offers require separate operator readback.

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
design envelope is published for display and provenance, not as the admission
denominator: admission divides by the measured GPU peak or the declared
per-device capacity floor. GPU utilization percentage does not drive
admission. Performance claims require
useful work, elapsed time, energy and the relevant host observations.

Idle SW-cap first-job exception (narrow, Sep-20): a GB10 (`NVIDIA GB10`,
`shared_system`, `soc_tdp`) at idle power (≤0.65×reference) and idle clocks
(≤10% of valid max SM clock, e.g. 208/3003) whose `limited` is explained only
by `sw_power_cap` (all other limiters incl. `sync_boost` false, mask only
idle/SW-cap bits, mask consistent with reasons) may admit one first
generation job when holders, broker jobs and foreign processes are all zero,
evidence is fresh/complete/attributed, and existing memory/CPU-pressure gates
pass. `mask 0x4` is the SW cap, never idle (`gpu_idle` is `0x1`); the two are
not interchanged. The exception grants no `low` credit (so sharing probes
still need genuinely free samples), never applies to `measurement=True`, and
never applies with holders present. Missing clocks, missing limiter
breakdown, or unknown mask bits deny the exception and keep the existing
`host_or_device_congested` refusal. Thermal, power-brake, HW/SW-thermal,
foreign, pressure, attribution and budget gates are unchanged. The admitted
metadata records `sw_cap_idle_exception` with the threshold and observations;
refusals record the exception diagnosis alongside the congested reason.

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

## Storage prewarm pacing

The data-manifest prewarmer is described in
[`data_manifest_prewarm.md`](data_manifest_prewarm.md).  A storage-role warm
requires complete fresh disk telemetry for every discovered data-vdev member:
missing or partial rows hold reads, recovery establishes a new baseline before
it can resume, and a later topology-discovery failure exits the role for
supervisor retry.  No-disk pacing remains an explicit direct-fixture or
non-storage mode, never a storage fallback.  Sequential rows retain the
shared pacing verdict and stat baseline, while their receipts reset only the
row's accounting counters.

Data-manifest inputs retain the v1 JSON contract and may be carried as one gzip
member. Stored bytes remain capped at 64 MiB; gzip decoding is bounded at
512 MiB before parsing, with at most 1,000,000 entries. The CAS input binds the
wire bytes and compressed summaries declare `content_encoding: gzip`; plain
summaries are unchanged. Header-based decoding works on extensionless CAS
paths and refuses incomplete, corrupt, concatenated or trailing gzip data.
Parsed objects require memory beyond the decoded-byte ceiling. See the input
guide for producer and deployed-storage-role adoption requirements.

Large manifests may declare entry-aligned cumulative phase boundaries.  The
role may end a warm window at any entry boundary that fits its budget, including
inside an oversized phase, but holds only the resident window ahead of an
action's accepted read frontier.  Only the matching claim lease's
`ProgressWatch` phase observation advances that frontier and releases reserve;
the role never infers consumed bytes from arbitrary progress units. It keeps a
declared action reserved when that observation is absent; claim grace is only
the fallback for actions with no progress policy.
Opt-in data-manifest v2 separates the unique tensor-range registry
(`entry_count`, `total_bytes`) from an ordered `read_plan` of phase-local entry
references. A later phase may reference the same range again, and the role
uses `read_bytes` including these revisits for its linear window/frontier and
ARC reservation. Each v2 read phase must match a sealed linear progress phase
in order. Empty read phases can mark compute frontiers. The source-side v2
validator closes malformed references and totals, while an older storage
generation refuses the schema; deployment of the compatible running storage
role is a prerequisite to v2 submission. V1 summaries and consumption order
are unchanged. See the input guide for the exact wire contract.
The storage role never trusts the action-writable progress file directly,
because only the worker has the per-launch token that authenticates it.  Cyclic
progress does not establish an irreversible manifest frontier.  A disk hold
also requires active NFS client reads and a read-await or backlog breach;
unreadable disk telemetry remains a fail-closed hold and unreadable client
telemetry is treated as active.
Every cycle event stamps `client_attribution` whether it held or not (#575,
#585): one entry per window warmed that cycle, in warm order, each naming its
own `served_host`, `served_attribution`, self/other read rates,
`telemetry_state` and `missing_devices`, plus the cycle's own hold counters
diffed against the role-lifetime ledger.  The claimed-window advances carry
their `disk_pacing` on the event beside the ready rows.  Rows are never
merged -- a cycle serving a claimed window beside a ready row reports two
verdicts, and a cycle that warmed nothing reports no rows, which beside
`pacing_active` reads as "nothing to read" rather than "pacing was off".
Only rows warmed that cycle appear: the pacer is shared, and stamping its
current verdict for an earlier cycle's row would certify a read that never
happened.
`pbcampaign` warns when a row's declared `argv` or `env` names a path under the
shared mount and the row carries no `data_manifest`.  That scan is best-effort
over the declaration only, so the warning says the row may read cold and a
write-only path is a false positive.  The campaign flag
`--require-data-manifest` is the strict form and independent of the scan: it
refuses the whole manifest before its first row is sealed unless every row and
a logical request's common half carries a nonblank `data_manifest`, so a
producer whose reads are not visible in the declaration can still require them
to be declared.

## Cluster-scoped storage tiers (#583)

Off by default. Nothing the fleet publishes today carries tier demand or a
`residency` block, no tier is minted unless a box runs the `tiers` role, and
that role is not in `fleet_boxes.json`. With no tier ledger and no residency
block, admission takes the path it took before: no ledger is scanned, no token
moves, and a claim record gains no field.

### The resource PB could not see

`cpu`, `gpu` and `mem_gb` are reserved on the box that executes the action.
Storage-tier residency is not: the pool lives on dl380g10 while its consumers
run on the Sparks, so a reservation against it is a reservation against a box
*other* than the executing one.

On 2026-09-14 three concurrent export arms pinned dl380g10's HDDs at 92%
utilisation and 522 MB/s pool-side. Withdrawing the third arm raised the other
two from about 2.0+2.1 to 3.0+4.5 units/s: the fleet did more total work with
fewer readers. PB admitted all three because none of the resources it counts
were scarce. The scarce resource was cache residency and the bandwidth to fill
it, and PB had no representation of either.

### Two reservable quantities, both discovered

| Tier | Token | Capacity, read every cycle | Residency it guarantees |
|---|---|---|---|
| `arc` | `arc_gib` | `c_max` less `arc_meta_used` from `arcstats` | budgetary; ZFS exposes no pin |
| `prismabuild-stage*` | `stage_gib` | the stage pool's own `size` from `zpool list -Hp` | pinned while a key holds the tokens |
| `ram` | `ram_gib` | the tmpfs mount's own `statvfs` `f_bavail`, capped by the policy window (#640) | pinned while a key holds the tokens |
| source pool | `fill_mb_s_pool_side` | the best `disk_pacing.mean_self_read_mb_s` any move off it recorded | none; it is the source |

Every quantity is discovered by `src/prismabuild/storage_tiers.py` on every
cycle, so adding an SSD or more RAM changes behaviour with no config edit: a
stage tier is any imported ZFS pool whose name starts with `prismabuild-stage`
(the pool name is the device's own declaration, the way a `storage_pool` label
declares a data member), its members are bound through `/dev/disk/by-id`, and
its capacity is the pool's own arithmetic. No tier quantity is a constant.

Minting is serialized per tier (#593): `PoolQueue.mint_tier_capacity` holds
the tier's mint lock across `ensure_capacity` and `retire_free_capacity`, so
two one-shot minters that do start (for example during an operator's
maintenance window with no role running) wait rather than interleave a
second mint with the first. One tier's lock never blocks another's. Since
#709 a `tier_loop --once` beside the supervised role does not even get that
far: the role's own host-local singleton refuses the second minter at
startup (exit 3), so stop the role first or run the cycle on another box.

Since #733 R6 the same per-tier mint lock is the cache-tier mutation
exclusion, attached by the explicit `PoolQueue.tier_ledger` factory (host
ledgers carry none): every token rename through a tier ledger -- claim
begin (non-blocking, declining as `tier_reservation_unavailable`),
commit/abandon/transfer/release (blocking, completing under the lock),
mint grow/shrink, egress decharge, stale-handle sweep, and the grant
`acquire` in fence reservation -- serializes against the dead-name
reclaim headroom scan, so a held/private token renamed to free between
the free listing and the holder listing cannot be missed by both and
reissued as unbacked credit. Lock order is always parent (key
transition, then stage ownership) into the mint leaf; the mint holder
never acquires a parent lock, and nothing is held over payload I/O.
The grow/reclaim census on a tier ledger is error-visible: an
unreadable directory aborts the mint/reissue with everything retained
for the next cycle, so capacity is never minted from a partial view;
host ledgers keep their legacy scans.
Mixed-version operation is NOT qualified -- a worker or storage role
without the guard admits outside the exclusion -- so deploying the
guarded generation requires a quiescent queue and reader state with no
new tier-admitted workloads until worker AND storage roles converge
(root reviews the actual publication). No bounded-overshoot exception:
above-wanted credit from an unguarded interleaving is unbacked at every
prefix even when a later retire would trim it.

**Bandwidth figures name their side.** The token, the demand key and the tier
record all read `fill_mb_s_pool_side`, because a file-side rate and a pool-side
rate differ by whatever the ARC answered. One live receipt on dl380g10 records
1141 MB/s file-side for 206 GB off a four-spindle raidz1, which those disks
never produced; the same action's pool-side attribution at depth 16 is
242.5 MB/s. A tier with no pool-side-attributed receipt mints no fill tokens,
which is the probe rule.

**A standing ceiling probes above itself (#706).** The supply fold
(`storage_tiers.fill_supply_from_records`) seals a ceiling off the most recent
receipt that fell short of its reservation, and clears it only on a later
delivery that exceeds it. But movers are admitted against the tier's fill
tokens, and a tier at a ceiling used to mint exactly the ceiling: a mover priced
above it read `never_fits_tier_capacity` and never ran, so the pool was never
asked for more. Two separate live observations have that shape — the archived
receipt `aa34e2a6` measured 111.2 MB/s while the box churned, and after #707
deployed the stage announced a 65.7 MB/s ceiling whose fold offer of 171 MB/s
still stood under six already-ready movers reserving 259 MB/s each, every one
denied. Neither is a hard rate cap: `fill_mb_s_pool_side` is recorded on the
receipt as the copy's own reservation, and the disk and client pacer is
unchanged by this; the freeze is admission.

Two offers can lift the supply over a standing ceiling, and the loop selects
the larger:

* the **historical offer** the fold has had since #707: `ceiling_mb_s` plus the
  median of the single-reader shares its own receipts price — `min` of a copy's
  file-side rate and its window's delivery over its sharers, the same bound
  that prices a next mover — announced on the supply record as
  `probe_offer_mb_s`, marked `probing` with its basis; and
* the **queued floor**: `int(ceiling_mb_s)` plus the oldest ready mover's own
  sealed fill demand, the one-ready-reader probe rule every other branch
  already uses, which the fold cannot compute because it reads receipts and not
  the queue.

`tier_loop.cycle` mints the selected offer (`fill_source: "measured-probing"`,
`fill_probe_mb_s` the selected increment over the ceiling) and announces the
selected offer and basis under `fill_supply`, so a reader — and the
`probe_offer` stat on `prismabuild_tier_fill_supply_mb_s` — never mistakes a
smaller historical fold for the live selected offer. With no ready demand the
historical offer stands, and with neither the offer is exactly
`int(ceiling_mb_s)`. The next admitted mover decides: a delivery refutes the
ceiling and growth resumes off the new best; a shortfall re-sets it at the
pool's own delivery with a fresh probe above it — bounded oscillation, never a
one-way ratchet down. The queued floor is a demand, never a measurement, and
repeated cycles over identical evidence and ready work mint identical capacity.

An already-ready row is never rewritten: its key, sealed resources and copy
argv stay what they were sealed as, and the queued floor is what makes it claim
on the next cycle. A mover republished by a window publishes with the resources
its dispatch sealed for the same reason — rewriting the resources without
rewriting the sealed request and the copy's own account of what it reserved
would admit a reservation nothing sealed.  Once such a row is the oldest ready
demand, the next cycle raises the fill offer to accommodate that unchanged
demand; other admission gates still apply, so this is not a claim of immediate
execution.

### Demand is derived from the data manifest

A movement node declares the half-open byte range of its consumer's read order
it makes resident. The range comes out of the manifest the action already
sealed — `storage_tiers.manifest_phase_ranges` reads the same running byte sum
for v1 `annotations.phases` and v2 `read_plan.phases` that the prewarm role
reads, through one shared refusal rule for both tables (#594): a missing,
empty, repeated or non-string name, a non-integer cumulative, a step back, an
overrun, a boundary off an entry, or a table that ends anywhere but the total
yields no ranges. A v2 table is checked against the plan's own consumption
order, which exists to differ from entry-list order, so a reordered plan the
core validator accepted is not refused here. `storage_tiers.residency_demand` turns it into whole GiB, rounded
up. `publish` refuses an item whose declared `stage_gib` on that tier is below
the ceiling of its own range, so the number in a claim record traces back to a
declared read set rather than to a habit.

### The second cache layer: the stage in the file server's ARC

Two layers, not one. Layer 1 is the HDD pool copied onto the SSD stage, which
a mover does and a consumer reads through the residency map. Layer 2 is those
staged blocks living in dl380g10's own 240 GiB ARC, so a Spark's read is
answered out of RAM over the 100 Gbps RDMA link instead of off the SSD.

Measured sparky to dl380g10 on 2026-09-18 -- one 5.37 GB file, `dd
iflag=direct` so the client page cache cannot answer, 16 streams of 256 MiB at
matched concurrency:

| arm | throughput | share of the link |
|---|---|---|
| ARC miss, served from NVMe | 2,402 MB/s | 19% |
| ARC hit, served from RAM | **10,045 MB/s** | **80%** |

**4.18x at matched concurrency.** Three things make it happen, and each one is
discovered or measured rather than configured here:

* **`primarycache=all` on the stage dataset.** `metadata` -- which is what the
  dataset carried until 2026-09-18, for a rationale that named a consumer that
  did not exist yet -- caches no file data, so every consumer read of a staged
  file reaches the SSD. `storage_tiers.stage_dataset` reads the setting with
  the dataset's `available`, `discover_tiers` announces it on the tier record,
  and `tier_loop` logs `stage-primarycache-refused` and stamps
  `arc_warm.eligible: false` when a rebuilt pool has inherited `metadata`
  again. The tier is still announced: layer 1 works without layer 2.
* **A warm step in the mover.** The copy is a write, and writing a block is not
  reading it, so stage blocks reached the ARC only incidentally -- a repeat
  read fell from 9580 to 7423 MiB/s as other shards evicted them. After the
  range is copied and verified, `stage_move.warm_staged` reads it back on the
  box that owns the stage, which is the one place a read fills that ARC. It
  runs after every measurement of the copy is taken, in its own `arc_warm`
  receipt block, so a warm never prices a copy. Its bound is the mover's own
  range: it reads back exactly the files it staged and nothing else on the
  stage, and it is refused by the dataset's `primarycache` rather than by any
  token it holds.
* **No mover reserves `arc_gib` for the warm.** The ARC tier announced 233 GiB
  every cycle and nothing spends it -- that is deliberate now, not the
  accident #638 opened on. A mover co-demanding the ARC's GiB would bound the
  published SSD window by min(stage, ARC) -- 233 GiB against 721 on dl380g10
  -- and once the ARC shrinks to make room for an explicit RAM tier, that
  bound would throttle layer 1's read-ahead with it. So the warm set is
  bounded per mover by its range and unbounded across movers: successive
  phases may evict each other's warm, which costs the *pre*-warm and never
  the stage residency a verdict gates on. The RAM occupancy budget returned
  with the RAM tier's own movement nodes, holding `ram_gib` the way movers
  hold `stage_gib` today — the section above this one.

**ARC residency is a performance tier, never a correctness gate.** ZFS exposes
no pin and the ARC target `c` is volatile on a shared box -- it fell 99 GB
inside one five-minute window on 2026-09-11 with no tenant asking for the
memory. The warm fills it and promises nothing about what is still there
later. `PoolQueue.residency_verdict` keeps gating on stage residency alone,
which is durable and checkable -- a file exists and the composed map names its
mover -- and no ARC leg was added to it.

**What the claim-time prewarm stopped doing.** The prewarm role warms the
*pool* path, which is the path a consumer opened before PrismaBuild published a
residency map. A consumer admitted on a resident window opens the staged path
instead, and the two are different datasets with different ARC entries, so
warming the pool for it both misses the target and evicts it. The role now
skips that warm for a claimed row whose `residency_verdict` reads `resident`
and records the skip. Only `resident`: a consumer whose map is not composed yet
was never admitted, and one whose later phases were never staged reads the pool
for them exactly as it always did.

### The RAM tier: explicit placement, mount-epoch identity (#640)

The interim layer above warms the ARC by reading the stage back, which is
caused rather than hoped for — but the ARC is still a cache PB cannot pin,
cannot evict on purpose, and cannot refuse on identity. The RAM tier is the
directed replacement: **an explicit tmpfs on the storage box, filled and
evicted by PrismaBuild's DAG**, memory PB owns outright. Measured sparky →
dl380g10 over the same 100 Gbps RDMA link, tmpfs over NFS served
**11,866 MB/s — 93% of the link** — against the ARC's 80% and the NVMe
stage's 19%, and `noswap` is live on the box's kernel
(`7.0.0-31-generic`), so a `mount -t tmpfs -o size=<N>,noswap` cannot page
out and overfill is **ENOSPC — fail-closed**. L2ARC is out of the design
entirely (0 hits in 20,780 lookups): it only holds the ARC's past.

**The mount is the tier; the policy is the sizing.** A `ram:<host>` tier is
discovered from the tmpfs mounted at the policy's mountpoint: capacity is
the mount's own `statvfs` (`f_bavail × f_frsize` — never `MemAvailable`,
which moves with other tenants' habits and is not placed RAM), the ceiling
is its own `size=`, and the record announces `mountpoint`, `mount_options`,
`size_bytes`, `ceiling_bytes`, `window_gib`, the effective
`promotion_chunk_gib` the submitter cuts phases into, and `epoch`. The numbers PB is allowed to
decide live in one versioned file, `tools/fleet/ram_tier_policy.json`,
published with the runtime the way `fleet_boxes.json` is and read fresh by
the tier loop every cycle: `ceiling_gib_max` (256), `window_gib_default`
(160 — sized 2026-09-19 to hold one whole phase plus margin: promotion is
phase-granular, the largest phase is 134.2 GiB, and a 112 GiB window made
`capacity − step` negative, minting a zero run-ahead budget so nothing
could ever promote — the GPU starved between layers by arithmetic. 160
fits a phase and stays inside the worker-demand guard's 160.5 GiB bound), `arc_floor_gib`
(20), `system_reserve_gib` (16), `prefill_depth` (`null` — the #633
run-ahead semantics; a positive GiB caps them), and `promotion_chunk_gib`
(`null` — the submitter cuts each phase into window quarters at seal time;
a positive GiB pins the chunk instead, #673). **A change to it is a
publish, not an ssh:** the next cycle mints from the mount's own `statvfs`
again, so a declared policy change or a rare operator remount is picked up
between cycles automatically. The ceiling is a roof, not a target; the
policy-minted window below it is what PB actually fills, and the minted
supply is `writable + landed`, capped at the window — the #621/#623
arithmetic, one tier over.

**Five refusals, each fail-closed and each naming its numbers.** The mount
absent: announce nothing — free RAM is not placed RAM. `statvfs` unreadable:
announce the tier with capacity zero and the refusal on the record, minting
nothing, so an operator sees a tmpfs that is not answering rather than a
tier that quietly vanished. And the floor guard, in two halves: the tier
refuses while `ceiling + max(arc_c_max, arc_floor, arc_meta_used) +
system_reserve > MemTotal`, read live from `/proc/meminfo` and `arcstats`. `size=` is a limit
on file bytes, not an allocation, so a tmpfs whose roof plus the ARC's own
permission plus the reserve exceeds `MemTotal` never reaches its ENOSPC —
the OOM killer arrives first, which is fail-random rather than fail-closed;
the runbook's `zfs_arc_max` shrink is an operational precondition, and the
guard refuses until it is done. The ARC floor itself is the larger of the
policy's declared floor and the metadata the ARC cannot drop. The roof is
only the mount's ENOSPC backstop, though: what PB actually fills is the
policy window below it, capped by the ledger — so the window must fit beside
the box's own offered job capacity too, read live every cycle from the tier
host's worker record (`workers/<host>.json`, `capacity.mem_gb`): the tier
refuses while `window + worker_demand + max(arc_c_max, arc_floor) +
system_reserve > MemTotal` (#645), and it refuses when no offer names a
number at all, because a loop can appear between cycles. Tonight's box is
the proof both halves hold together: the 240 GiB roof admits
(240 ≤ 294.5 − 22 − 16), the 112 GiB window admits beside the 96 GiB the
loops offer (worst case 112 + 96 + 22 + 16 = 246 ≤ 294.5), and a window
publish toward the sanctioned 256 with jobs admitted would refuse. **The tmpfs
must be mounted `noswap`:** the options are announced, and a mount without
it refuses the warm-path admission outright — a swappable tmpfs can page
"resident" bytes out, and a consumer whose gate says resident would then
pay a swap read behind a claim of RAM, which is the correctness lie this
tier exists to end.

**Mount-epoch identity — the rule the whole safety argument rests on.**
tmpfs empties on reboot; the ledger and the residency-map fragments on the
shared mount survive. Without an epoch, a reboot would leave a map naming
ram paths whose bytes are gone and a ledger counting tokens for ranges that
no longer exist — a RAM gate *less* safe than the ARC budget it replaces.
So the tier loop stamps an epoch (mount time plus a random nonce) into a
marker file at the tmpfs root at bootstrap; the marker dies with the mount,
and every promotion's fragment and receipt carries the epoch it landed
under — a ram fragment without one does not validate at all. On the first
cycle after a change the loop logs `ram-epoch-changed`, **drops every
prior-epoch fragment**, and returns the ghost tokens of held keys whose
receipts date them to a prior epoch — their bytes were deleted by the
reboot, not by an egress, and holding them would starve the new window,
which is the one failure the direction names ("starvation is the failure to
avoid"). A mount that is gone entirely is the same rule one step further:
there is no current epoch, so every ram fragment is a prior one. Until
fresh ranges land, nothing reads as ram-resident, and `residency_verdict`
denies `ram_epoch_stale` whenever a composed map's ram epoch is not the one
the tier announces now — the same one-cycle wait as `map_not_composed`.

Epoch reclamation distinguishes material from unconsumed advance credit
(#879). An exact current blind grant is a future reservation, not a RAM copy:
its candidate name must resolve to a live consumer's fresh frozen RAM plan,
then equal that plan's current `advance_needs` fence and its whole demand.
A name prefix alone grants nothing. A queued mover's unconsumed credit must
also bind the frozen leg/range, current publication generation and actual
funding tokens. The tier mint guard holds the ledger stable through the decision; consumer
and mover transition locks are acquired nonblocking beneath it, so a
contending normal mover→mint path makes cleanup defer rather than deadlock.
Only the swept tier is released under that guard, reentrantly on the mint it
already holds; any other tier is swept after the guard drops, the same leaf
ordering `stage_release._evict_owned` uses after its own mint section. A tier
ledger's mutation guard is that tier's mint lock, so releasing every tier
inside the guard would wait on a second tier's mint while holding the first,
which is the one order the "mint is a leaf" analysis does not cover.
Consumer and mover transition locks protect the ownership decision;
unknown evidence, funding rotation or contention retain the credits with a
`ram-credit-cleanup-deferred` diagnostic. Known dead/missing or superseded
owners and unrelated/forged grants remain reclaimable. The existing window
still retires grants its frontier no longer needs.

A qualified future reservation survives a mount-epoch change because it
claims no already-resident bytes; old-epoch material still drops exactly as
before. A disappeared RAM tier retains no future grant. Reservations remain
charged to the ordinary ledger, including after capacity shrink, and cannot
admit a copy beyond current capacity. This adds no funding schema or migration.


**The promotion node, and the occupancy it holds.** Stage→ram promotion is
a movement node like the stage's own: `ram_promote.py` copies a *landed*
stage range into the tmpfs under the same content-addressed names, with the
same digests, drawing no pool bandwidth and pacing nothing — its source is
the stage on the same box, which is why the ram window publishes a promotion
only for a phase whose stage range has landed. Pool→ram directly is
refused (`ram_source_stage_absent`): the SSD stage stays the durable tier a
verdict gates on, and ram is a performance tier in front of it. The plan
grows two optional rows per phase — `ram_mover_row`, `ram_egress_row` —
sealed by the submitter beside the stage's own (`--residency-ram auto`,
the default, seals the leg when a ram tier is live on the stage's host;
`off` is the A/B's other arm; a plan already frozen keeps the leg it was
frozen with). A phase bigger than the tier's effective chunk seals one
promotion node plus one egress node *per chunk* instead (`ram_chunks`, in
read order, each carrying its phase, its chunk index and its chunk range —
#673): at window 160 the chunk is 40 GiB, so a 123 GiB phase of small
entries seals 4 chunks and the movement node shape is otherwise today's. The stage leg
slides the same way (#675): a phase bigger than the stage record's
effective chunk seals one movement node plus one egress node *per chunk*
instead (`stage_chunks`, in read order, under the stage's own role names),
because there is one chunk family across tiers — the stage record announces
the same `promotion_chunk_gib` the ram tier on its host announces, and the
submitter cuts both legs at that size. Cuts fall only on the manifest's entry
boundaries in read order (#965): a mover stages every entry its range
touches, so a cut inside an entry made both neighbouring movers overrun their
reservations and refuse `residency_overran_reservation` on every attempt. A
chunk is therefore whole entries, packed up to the chunk size and reserved at
their bytes, so a phase's chunks tile its entry bytes exactly (their tokens
round up per chunk, like every reservation). An entry larger than the chunk
is a chunk of its own. An entry larger than the window a tier announces could
never be admitted, so the submission refuses at seal
(`residency_entry_exceeds_window`) before anything is sealed or published;
the stage record announces no window, so only the ram leg checks one. A
phase that fits
in one chunk seals the whole-phase pair, and a plan sealed before chunks
keeps the leg it was frozen with — a node whose range is its phase's whole
range follows the whole-phase rules, byte-identically. A promotion holds
`ram_gib` the way a mover holds
`stage_gib`: from claim, past finish — the pin, read off its receipt — and
back only when an egress deletes its files, because held-by-nobody bytes on
a roof-limited tmpfs are ENOSPC waiting to happen.

**A reservation is not residency.** Those tokens are taken at *claim*, before
a byte moves: they bound what the tier may hold and they are what an egress
gives back, and they say the room is booked, never that the bytes arrived.
Until #759 the tier loop read them as both — `_mover_state` and
`_ram_mover_state` set `staged` from `holder_tokens` alone, and
`_stage_source_staged` let a RAM promotion publish on that — so on 2026-09-20
the live Stage A head promotion published while its 10.9 GB stage copy was
still running, found no per-consumer fragment, and refused
`source-coverage-gap` through 80 retained attempts across 27 publication
generations. The `pb_cursors` census read the same equivalence and called the
head phase staged while the identity proof saw nothing: two predicates, one
premature trigger.

Readiness is now one shared predicate,
`residency_plan.resident_movers(queue, plan, tier_id)`, which the tier
window gates on and the status census reports, so a gate and a cursor cannot
disagree. It asks for the reservation *and* the publication: the key still
holds its tier tokens; a current fragment for this consumer, tier and
manifest vouches for the bytes, naming the plan's stage root on the stage leg
and the **announced** ram root under the **announced** epoch on the ram leg;
the key is not `CLAIMED`, because `stage_move` republishes its fragment as
entries land and a running copy's receipt belongs to a previous run; a
`complete` receipt covers the span the plan sealed for that key; and that
receipt's `entries_declared == entries_staged` equals the current fragment's
entry count, which is what stops a historical complete receipt from speaking
for a new partial copy after a crash or a requeue. Adoption passes unchanged
— `tier_loop.adopt` re-issues the donor's fragment under the successor and
files a receipt carrying that fragment's own counts, with the tokens
transferred rather than released. Cost is one fragment-directory listing per
plan plus one small receipt read per fragment-backed key; no payload is read
and no model is stat-ed. Evidence that cannot be *read* is unknown, not absent, and unknown is never
reported as not-staged: `staged: false` asserts a fact a caller acts on, and
a corrupt or unopenable fragment supports no such assertion — the same
unproven-reported-as-known error in another costume. A missing fragment
directory is known-empty; a directory that cannot be listed, or a fragment
named for one of this plan's movers that cannot be opened or does not
validate, is unknown, so the window gates closed and emits
`ram-window-unknown` and the census reports `staged: null`. The
map-composition reader keeps skipping such a file — a consumer that can still
find three of its four movers' copies must read those three — and readiness
deliberately does not reuse that tolerance, because skipping is exactly what
flattens unknown into false (`residency_plan._plan_fragments`). A file no leg
of the plan names is still skipped: it cannot change this plan's answer, so
refusing on it would be a stall with no reason. Token holdings
keep their own jobs untouched — the window still evicts a passed phase on
what the ledger holds, the advance fence still counts it, and the census
reports it as `reserved` beside `staged`, so capacity in use is never
hidden behind the stricter readiness answer. Its action demand carries a second
number, and the two answer different questions: `ram_gib` is the tier
occupancy the range retains, while `mem_gb` is the action's own containment
cap — the copier's runtime working set that the mover receipts price, plus
the destination range in whole GiB (`storage_tiers.ram_promotion_mem_gb`),
because shmem pages stay charged to the writing cgroup and writeback never
reclaims them. A row sealed with only the runtime term runs out of its own
cap partway through every range bigger than that term: on 2026-09-19 four
4-11 GiB promotions died at exactly 1 GiB with `memory_limit_oom`, and no
promotion above 1 GiB had ever completed. The withdrawn #639 part-3 plumbing — a
second tier leg on `storage_tiers.residency_demand`, occupancy
classification, release at egress — transfers intact, aimed at the right
actor: one occupancy leg per movement node.

**Shared staged paths and who may delete them.** A staged name is a pure function
of the manifest entry (`stage_relative`: every staged input lands at
`<rel>.pbrange/<offset>-<size>`, whole files included), with no mover namespace --
so forward and reverse passes, or two read phases of one v2 plan, stage the same
source extent onto one file, and a promotion reads it back from that same staged
name at offset zero (never the manifest's pool path at the manifest offset). An
egress deletes only what is exclusively its mover's: under the stage root's
ownership lock (held across scan-to-release, inside the mover transition lock;
adoption takes them in the same order, declining on contention), it intersects one
fragment walk against its own paths as validated strings plus the sealed ranges of
claimed movers -- claims first, then fragments, because a claim exists before its
copy starts (each mover passes a start gate before its first rename, holding
nothing during the copy) and a fragment exists before its claim is gone. Shared
paths are kept with `entries_shared`/`shared_with` on the receipt while the
mover's own tokens come back and its own fragment is dropped; the last owner to
leave deletes the file. Anything unreadable fails the pass closed with the reason
on the receipt. Limits, stated: the ownership scan parses every consumer's
fragments once per egress (measured 2.9 s over 50 consumers / 140 fragments on the
live corpus -- JSON parsing, linear in corpus size, no global map by design); a
worker killed between rename and fragment publication leaves a recovery interval.

Stage and RAM movers also use this ownership lock when publishing a copied
file (#751/#752). Copying stays outside the lock. Publication adopts an
existing incarnation when its material record and file identity still match,
replaces an absent name, and retains a divergent or ambiguously owned name.
A slow live publisher remains an owner after the bounded wait expires;
elapsed time alone never permits replacement. Residue can be replaced only
after the fragment, pin, live-claim and partial-copy censuses show no owner.
For dev manifests without content digests, unchanged origin metadata in the
existing `user.pbstage.source` attribute permits reuse without another payload
read. Otherwise a necessary private copy can be compared with the stored
copy-time digest; the existing staged file is never rehashed for adoption.
The same rule applies to retries by the original consumer. A changed origin
cannot silently reuse its earlier bytes, and existing readers keep their file.

Range adoption also checks the donor's dated material against the current file
identity under the ownership lock before publishing a successor or transferring
credit (#755/#756). A superseded donor is skipped in favor of another current
donor for the same range. Path-level publication likewise searches past records
for an older inode; those records do not describe the current file's bytes.
An in-place modification of a dated inode remains a conflict. These checks use
metadata and preserve valid zero-copy reuse; they do not rehash staged payloads.

**The proof-lookup index is a bounded, exact projection.** A mover runs
`_proof_search` once per destination and again on every publish poll, so the
publisher keeps one invocation-local index of parsed publication metadata
(#761/#778): each fragment and sidecar is read and validated once per version
and retained as a packed projection of exactly what a decision reads —
normalized paths in one bytes blob with 8-byte end offsets and 64-bit hash
keys, and one fixed 72-byte record per validated ordered mention (size, raw
digest, four-field identity). Membership is exact byte comparison: a hash
collision costs a comparison, never an answer. Tables naming the same paths
are interned once per `(blob, ends)` identity and shared by every document
that references them; a table's allocated storage is measured with
`sys.getsizeof` (so array growth capacity is charged, not just used slots)
plus its intern structures, lives in the interned charge alone, and is never
also priced into a record. A record's own charge is its key, mover, mention
values and per-record overhead. `_reclaim` rebuilds the interned set from the
live records and recomputes that charge from those same measurements. The
ceiling stays 192 MiB, priced in retained bytes, and a record that does not
fit is decided uncached from the fresh parse — the same verdict, never a
truncated or held one — so a document whose packable content does not fit the
budget degrades to the pre-#761 cost, not to a wrong answer, and a validated
mention the fixed record cannot carry exactly (an arbitrary-size integer
identity) takes the same uncached path. Every lookup still stats each
metadata file first and compares its full
dev/inode/size/mtime/ctime version, so an added, removed, rewritten,
permission-changed or same-size-restored-mtime file is seen before the next
decision; whole-document validation, taint and foreign-owner semantics, the
live destination stat, the pin and handoff gates, and the ordered
duplicate-mention reads are unchanged. This is a work bound, not a fairness
or throughput claim: it removes repeated whole-document reads and decodes
when the compact working set fits the fixed budget. The #778 reproduction
demonstrates the amplification mechanism at fixture scale; it does not
measure the live forest's size or its exact decode multiplier.

**The window, the sweep, and the egress order.** Promotion scheduling is
the stage window's own semantics, pointed at the ram ledger: admission
needs free `ram_gib` — Rob's instinct, "empty space in tmpfs", made exact
through the ledger — bounded by the #633 run-ahead budget on the consumer's
accepted progress (`prefill_depth` may cap it) and, since #906, by the
consumer's refill horizon on the tmpfs (see "The ram window stages only to
its refill horizon too" below), in the plan's read order, and reported as
`ram-window-stalled` when it declines. Chunked (#673), the
window publishes the next *chunk* when free `ram_gib` covers it and the
budget admits it: chunks of the phase being read are the reader's near-term
food and promote as soon as their turn comes, while later chunks spend the
budget — which now buys several chunks instead of zero phases — so the
tmpfs refills as it frees instead of sawtoothing a whole phase at a time.
Chunked (#675), the stage window plays the same game one tier down: it
publishes the next *chunk* when free `stage_gib` covers it and the budget
admits it, and evicts each chunk of a passed phase through its own node, so
the SSD refills as it frees while the reader is still inside the phase.
That is the two-tier streaming relay, HDD→SSD→RAM: the disks fill the SSD
while the reader reads it, the SSD promotes to the tmpfs while the reader
reads that, and every tier refills as it frees instead of sawtoothing a
whole phase at a time. When the consumer's
progress passes a phase, that phase's ram egress rows are published — one
per chunk, each through its own node — *before* the stage
egress in the same cycle: a ram range that outlives its stage range is a
promotion whose source is gone. Orphaned ram bytes — a failed promotion's
landed partials, a dead consumer's unclaimed promotions — are eviction
candidates when the tier needs its tokens, on the same ownership discipline
as the stage sweep: `window_pressure` asks the ram leg the same
"what would it publish given room" question, and the sweep, egress and
reconciliation treat the ram root like any owned root, skipping the epoch
marker the way they skip the ownership marker.

**Serving.** The consumer reads the ram tier through an NFS export of the
tmpfs — an explicit `fsid=` in `/etc/exports`, which tmpfs supplies none of,
and read-only like `/stage/prewarm` (the operator's runbook, in #640, carries
the exact lines; PB does not touch the box). The composed map is still one
document: `compose` refuses fragments that disagree about the tier, so the
ram fragments are laid *over* the stage map (`residency_map.overlay_ram`),
and an entry keeps the stage path it already had while gaining `ram_path`
— same `(path, offset)` key, same digest, the bytes on the tmpfs under the
same name. A consumer prefers the ram copy and falls back to the staged
copy the map already vouched for, which is what makes a stale ram entry a
cache miss rather than an ENOENT. The map's header names the ram tier, root
and epoch, which is what the verdict compares.

### What a stage tier's capacity counts

A stage tier's `capacity_bytes` is the dataset's ZFS `available`: what a
writer may still write, net of parity, slop and the bytes already on the
dataset. A mover takes its `stage_gib` tokens at claim and keeps them past
`finish` only once its receipt says the whole range landed, so the tokens a
ledger holds are two things: **landed** (a holder with a complete, unrefused
receipt; its bytes are already subtracted from `available`) and **in flight**
(a holder still copying, or one whose copy fell short and is about to
release; its bytes are not). The ledger's supply is minted as **writable +
landed** (`tier_loop.landed_and_in_flight`), and the record announces
`writable_gib`, `landed_gib`, `in_flight_gib`, `held_gib` and
`capacity_basis: "zfs available + landed"`.

Both simpler formulas failed on `prismabuild-stage:dl380g10` on 2026-09-18.
`available` alone counted every landed GiB twice -- free fell as
`available - held` and the window starved at half the pool, an 11 GiB head
phase unrepublished behind 433 GiB held and 275 GiB writable (#621).
`available + held` counted a claimed mover's unlanded bytes as free and
published one more window every cycle while the first was still copying: ten
82 GiB movers admitted against 275 GiB writable, all ten ENOSPC (#623). A
record that does not name the writable source (a fake, a legacy tier) is
minted as it was.

Two owners of one staged file counted its bytes twice on both sides of the
ledger: each complete holder's tokens read as landed, so the mint carried
the duplicate, and the first owner's shared egress handed its tokens back
as writable free while the bytes stayed -- a newcomer claimed the phantom
before the next mint (#733). A shared egress now decharges instead of
freeing: tokens for bytes staying under a co-owner are destroyed
(`ResourceLedger.retire_held`, one atomic rename per token into the
ledger's dead namespace -- held or dead, never half-moved) while only
whole GiB actually leaving the stage return to free, so a fractional
split can never free more room than was made. A destroyed name keeps
its mint marker, which `ensure_capacity` skips forever, and its token
file waits in the dead namespace until honest headroom reissues it with
a second atomic rename: no name reappears except inside a backed wanted
bound. A decharge that fails partway keeps its tokens and fails loudly
instead of freeing the duplicate. The mint re-samples writable in-lock
beside the landed snapshot and apply, and completions file under the
same tier mint lock, so mixed-time pairs cannot overmint; the single
authoritative per-tier mint covers the full token dict so rate kinds
are never zeroed. The last owner to leave still deletes the file and
frees its tokens.

**The mover being evicted's own live claim is not a distinct co-owner
(#793).** A movement node files its final `record_move` receipt before the
worker retires its `claimed/` row, so an egress can run in that gap and find
the mover it is retiring still claimed. Reading that claim as another pending
publisher skipped every entry as `in-flight-copy`, decharged the mover's own
duplicate token, dropped its only fragment and reported `complete`: bytes
nobody vouches for and a token destroyed, with the tier's free capacity still
zero. The census now attributes the evicted mover's own claim separately
(`_claimed_paths_attributed` with `own_key`) and a live own claim **defers**:
the file, this mover's fragment, its material sidecar and its full occupancy
charge stay, and the reason rides the additive egress-receipt
`deferred_own` field (`["own-copy-in-flight"]`) with `complete: false`. It is
never a shared skip: sharing would destroy this mover's own duplicate and
drop its only same-path proof while the bytes remained behind nothing. The
deferral is settled by the ordinary retry: once the worker's terminal
transition has retired the claim row, the census no longer sees that key at
all, and the next sweep deletes the bytes and returns the token exactly once.
That is bookkeeping, not data readiness -- `residency_verdict` and the
composed map still read the filed receipt and fragment the moment they land
(PO-02), so a producer's own read of just-produced bytes is untouched; only
retirement waits for the child mover's claim to conclude. A move receipt
carries no immutable attempt identity (no nonce or scope on its wire), so a
complete-looking one cannot be told from a previous attempt's while the same
key is claimed again, and a wall-clock stamp is no substitute (2026-09-21
root QA); the deferral needs no new field and no wire change. Foreign claims,
co-owners, reader pins and pending promotion handoffs keep exactly the
protection they had, and the census is still taken inside the mover's
transition lock and the stage root's ownership lock, so the lock order is
unchanged.

### A copy has no result to replay

A mover's action key is a content hash and its receipt is filed in the CAS
like any computation's, so republishing the same key -- the window asking for
a range again after an egress, or after a copy that landed short -- would be
answered by `run-local` with the old receipt as a `cache_hit` that moves no
byte. `residency_pin_holds` rightly pins nothing for it, and the next cycle
republishes it; nothing ever re-executes the copy (#624: the GLM run's 11 GiB
head mover, replayed 25 times at 0.39 s each while the consumer sat `ready`
on `lead_unpinned`). The pool cannot tell a copy from a computation by its
key; the publisher can. The tier loop publishes every mover row and every
egress row with `recompute=True`; `PoolQueue.publish` stamps `recompute` on
the item, where it is not claim-scoped (a requeued movement node is still a
movement node), and the claim launch passes it to `worker_argv`, which appends
`--recompute`. `run-local` has carried that flag since before generation
`2113f37bc68e`, so the frozen rows of a running campaign execute it. SLURM's
launch is unchanged: it refuses recompute, and everything that is not a
movement node still launches byte-identically to it.

### Where a tier ledger lives, and why it is a second root

A tier ledger is an ordinary `ResourceLedger` under `tier-reservations/<tier_id>/`,
keyed by tier id (`prismabuild-stage:dl380g10`) instead of hostname. It is a
second root on purpose: every directory under `reservations/` is read as a
*box* by `claim_reservation_hosts`, and a tier that held the same key would make
the claim's holder ambiguous and refuse every finish and reap of that action.

`_claim` splits an item's demand by ledger. Host kinds are acquired as before,
under the host admission lock. Tier kinds are acquired **after** host admission,
so a box that cannot seat the work never touches the shared ledger, **before**
the ready-to-claimed rename, so a claim is never won on capacity it does not
hold, and **outside** the host admission lock, because holding box admission
across a mount stall is the #351 shape. Either shortage abandons both, records a
denial naming the tier and the shortage — `tier_reservation_unavailable`,
`never_fits_tier_capacity`, `tier_unknown` — and records no pass: the shortage is
cluster-wide, so withholding this box for it would idle a box that has other
work. Every path that concludes a claim goes through one `_release_reservation`
helper, so a claim that reserved on a tier cannot be concluded on one ledger and
forgotten on the other; a dead claimant's stage tokens come back with whichever
reaper finds the stale lease.

### The residency gate

An item may carry a `residency` block naming its lead movement nodes. It is
admitted only when every lead has a `done/` record whose status is `executed`;
otherwise the claim is denied before any token
moves, and the box goes and does other work.  A lead that may still arrive
reads `residency_lead_not_resident`; a lead that ended somewhere no later
poll repairs -- failed, withdrawn, dropped, unpinned, or bound to another
manifest -- reads `residency_lead_terminal`, so the fleet-wide denial
snapshot tells the two apart.  Admission is the same either way: the item
stays ready.  A `cache_hit` lead moved no bytes
and does not satisfy the gate — the residency descriptor is deterministic on
purpose, so that a consumer can bind it as a CAS dependency before the mover
runs, which is exactly what makes a cached mover look finished.

Target contract: [staged-read contract](staged_read_contract_2026-09-20.md)
(requirement ledger `staged_read_requirements_2026-09-20.json`) names the
allowed-tier, lease, and readiness rules this gate participates in, with
honest per-requirement status. The PQ endgame is an application acceptance
boundary referencing that contract, not a PB scheduler responsibility.

### Produced-output admission (working window, not the corpus)

A producer that stages bytes it writes itself declares a tiny validated
immutable template (`pbrun --produced-output-template PATH`, at most 64 KiB,
`produced_output.validate_template`). The template is captured as an ordinary
CAS declared input plus sealed action params
(`produced_output_template` declaration, validated by `core.
validate_produced_output_declaration` against `action.inputs`), so changing
the template changes the action key and editing the file after seal changes
nothing the worker reads. The qualified tier demand is derived from the
bounded working window (`produced_output.owner_demand_terms`: window GiB,
never the durable corpus maxima or host decode memory) and added to the
explicit user CPU/memory/GPU reservation, which is otherwise untouched. Pool
transport only. A write-only template declares no window, so its producer
carries no tier demand at all; see "Write-only templates: origin-only
batches (#912)".

`PoolQueue.publish(..., produced_output_template=...)` validates the template
(closed fields, stage/ram kinds, minimum-within-window), requires the
carried tier demand to exactly cover the derived window (plus the input range
floor when an input residency range lands on the same tier; input leads carry
none), files the template immutably, and projects
`item["produced_output"] = {template_id, template_sha256}`. The #595 gate is
extended narrowly for this declared window only: tier demand with neither an
input residency block nor a correct produced-output declaration still
refuses, and underdeclared, mismatched, foreign, tampered, or extra tier
demand refuses with no refused-publication side effects. Input and output
demand coexist and are admitted once through the existing host + tier ledger
channel: the claim holds every required token before the producer starts, so
insufficient tier capacity denies even with ample CPU/GPU.

The runtime binds from the sealed item, never from caller arguments
(`produced_output.declared_template` / `bind_declared_instance` over the
protected live claim + launch halves). The template carries no action key or
nonce; the instance binds the real protected attempt later. `pbcampaign`
list rows forward the option (`produced_output_template` row field to
`--produced-output-template`); decomposed logical children are out of scope.
The frozen template's top-level `produced_output_template` entry is a
submitter handle (`pbrun._TEMPLATE_SUBMITTER_KEYS`), not part of the shared
half a decomposition parent is keyed on: the binding declaration is the sealed
`params` copy, and the top-level one is what the submitter projects into its
own queue row. The template carries the entry whether or not the flag was
given, so naming it in neither key set refused every Stage A freeze, not only
a producer's.

Release (`produced_output.safe_release_instance`) requires the coherent
accepted `prismabuild.reader_lease` package (same file the fleet imports;
a missing or foreign SDK retains), an untainted SDK pin census over the
owner and batch namespaces, and exact owner containment: any live claim
retains, an unreadable claim retains as unknown, and with no live claim
only `containment_certificate_ok` over the exact owner nonce/scope
(broker attestation + matching terminal telemetry) authorizes -- a bare
DONE/FAILED/WITHDRAWN record by key alone never suffices. Funding-intent
movers always retain with `funding-intent-reconcile-retain` for the
funding/reconciliation lane: metadata absence never proves physical
absence. Every mutation re-checks the single admitted-template boundary
(instance digest equals the passed template; a substituted larger maxima
is refused). `require_prewrite` SUCCESS is the authorization to start
writing bytes, so new reservations require the live owner claim to name
the exact attempt (stale/superseded/absent owners refuse before the
first payload write; replays grant nothing). `commit_batch`
additionally requires the live owner for quota consumption and token
movement. Prewrite aborts prove exact planned-path absence before
freeing headroom; duplicate commits never re-mint quota. Batches load
through one bounded validator (schema/binding/strict counts/
re-validated entries/canonical manifest); damaged records retain.
Retirement has one public path (`retire_batch` validates provenance
and the commitments/immutable-record agreement on mover, tier, and
canonical namespace BEFORE egress, drives egress, and files under
lock) and frees the tier window only; durable-origin classes stay
charged until `reclaim_origin` proves every loader-validated entry
path absent, exactly once. Holder release derives from validated
records, never mutable fields alone. Census paths attributable to
recorded staged paths retain by name; unknown census retains;
unrelated pins never block. The egress claimed-copy attribution skips
only verified producer holds (item ref equals the sealed request's
validated declaration, no movement range); substituted or
declaration-less rows taint. Commit-batch funding, movement/tick
handoff, and the general funded-window primitive remain with their
owning lanes.

### The movement node

`tools/fleet/stage_move.py` is the mover: an ordinary PB action, placed by tag
on the box that serves the pool, that copies one declared byte range of a data
manifest's read order onto a stage tier. It reads through the prewarm loop's
own mount map, pacer and admission gate — imported, never copied — because a
mover that paced differently would not be measuring the same pool. What it adds
is a sink: the ARC is keyed by on-pool block pointer, so reading a block warms
the path a consumer will open, while a copy onto another device does not, and
nothing about reading a file makes a copy of it.

Each entry is written beside its final name and renamed into place, so a partial
file is never visible under the name a consumer reads, and its digest is
computed on the way through. A staged range that the manifest gave a digest for
and does not match is deleted and left out of the map: publishing it would make
the map a lie a consumer trusts in preference to the pool. Every staged input is
named by the exact range it holds, `<rel>.pbrange/<offset>-<size>`, because two movers
holding two ranges of one shard cannot both rename-publish into one file, and
the staged object's length has to be the range's length. There is no bare-name
case for "whole" files: a manifest entry carries no file size, so a pure
function of the entry cannot tell a whole-file read from a prefix read. The
former rule gave the bare name to any path a manifest named once from offset
zero, and on 2026-09-22 a routing capture's 45 KB safetensors-header reads and
GLM Stage A's 5.37 GB whole-shard reads derived one name for 74 shards; each
side's movers refused the other's publication forever. Two entries now share a
staged name exactly when they name the same bytes of the same source, so after
a runtime publication a new mover recopies a range an older mover staged under
its bare name, and the bare copy is left behind as orphan cache. The
retention censuses (`_claimed_paths_attributed`, `_claimed_source_paths`) also
count the former bare spelling, because a plan row keeps the tools of the
generation that sealed it and may still write it; nothing publishes or deletes
by that spelling, and orphan recovery retains a bare copy it finds instead of
counting its entry as gone. A produced output keeps the name its producer
declared, `produced-output/<namespace>/<rel>` for a path its manifest names once
from offset zero: the namespace is the producing action's own digest, so no
other publisher's read can derive a name inside it.

A range whose entries total more bytes than the range reserved is
`residency_overran_reservation`, refused before the copy rather than after it:
the tokens bound what the tier can hold, so staging past them breaks the
accounting that keeps the stage from overfilling.

The receipt is filed with `record_move` into `movers/`, a sidecar beside
`prewarm/` for the same reason that one is, and the `tiers` role reads both for
the fill measurement. It carries the pacer's pool-side attribution, the
file-side rate under a name that says which side it is
(`mb_per_s_file_side`), the `/proc/PID/io` delta, and the range it was asked
for beside the bytes it staged.

Each entry's temporary beside its final name is keyed by the mover writing it
(`.<name>.<owner>.partial`, #620): stage paths are content-addressed per
manifest entry and shared between consumers, so a dead consumer's unstarted
mover and its successor's copy one entry to one destination, and a shared
temporary is truncated by both and renamed away by the winner. Keyed
temporaries verify the same digest and land the same bytes independently, and
the sweep still recognises both spellings.

### Mover receipts are keyed on pool identity (#611)

`mover_demand_from_receipts`, `mover_fill_demand_from_receipts` and
`fill_supply_from_records` fold over every usable receipt in `movers/` for a
tier id — and nothing on the receipt said which pool it measured. After a
resilver, a member swap, an added vdev or a pool rebuild, the old receipts
still price cpu, mem_gb, the fill share and the ceiling for the new pool.

`storage_tiers.pool_identity` names the pool as `zpool` describes it: the guid
(which a destroy/recreate mints anew), the state, the coarse scan (running vs
finished — never the progress line, which changes every cycle), and the
data-vdev members. Discovery stamps it onto every stage tier record
(`pool_identity: {stage, source}`); the mover copies the announced record's
into its receipt; the three folds read only receipts carrying the tier's
current one. A receipt with no identity predates the stamping and is dropped
by a gated fold — failing closed re-measures through the probe rule rather
than guessing — while an ungated fold (a tier announced by an older
generation) reads everything, exactly as before. Prewarm records carry no tier
and no identity and are the pool's other measurement; the supply fold keeps
reading them, and keying them is a separate change.

### The residency map

`prismabuild.residency_map` is what a consumer reads to find its staged bytes;
its path arrives as `PRISMABUILD_RESIDENCY_MAP`. Entries are keyed by
`(path, offset)` spelled `"<offset>:<path>"`, because a data manifest refuses a
repeated `(path, offset)` and therefore permits one path at several offsets — a
map keyed by path alone would be ambiguous exactly where a partial copy is most
dangerous. `sha256` is required on a map entry although a manifest may carry
null on its own: the map's whole claim is that these are those bytes on another
device, and a copy nobody hashed cannot make it. A path the map does not name
falls back to the pool.

Movers write fragments, one file per mover under the consumer's directory, and
the map is composed from them. One file that every mover read-modify-wrote would
lose entries the moment two of a consumer's movers finished together, and rename
is the only concurrency primitive this fleet trusts on NFS — a rename cannot
merge. Fragments that disagree about the consumer, tier, stage root or manifest
refuse rather than merge, and so do two movers that staged one range
differently.

**Two fragment layouts, one strict census (#798).** The store holds records
and fragments together: `leases/` (pins and retiring marks), `material/`
(publish-time sidecars) and the produced-output template, scope and batch
directories are bookkeeping, and are never parsed as namespaces, while the
produced fragments are real ownership evidence one level deeper at
`produced-output-fragments/<batch namespace>/<mover>.json` (reached through
`produced_output.output_fragment_root`). `stage_release._fragment_census`
walks both layouts — legacy flat `<consumer>/<mover>.json` and the nested
produced namespaces — and is strict on purpose: a directory that is neither
reserved bookkeeping nor a 64-character namespace, a `.json` document
that cannot be read or validate, a `.json` entry that is not a regular file
(a displaced fragment directory, fifo, socket or device), and a
64-character namespace name or `produced-output-fragments` container that is
not a directory, come back as taint rather than a skip. That
distinction is load-bearing because the census' consumers delete:
`residency_map.read_fragments` skips a bad file so a consumer still finds
its other copies, and reusing that tolerance for attribution is how
corruption reads as "unowned" and staged bytes are lost. A composed
`<consumer>.map.json` is the one non-directory beside a namespace that is
legitimate (`residency_map.map_path`); its name is nine characters longer
than a namespace, so it names no layout and taints nothing, and the pass
still cleans true orphans beside it. The reconciliation
carries the taint into an incomplete receipt (`skipped:
attribution_unreadable`, bounded reasons), deletes nothing, and the tier
cycle continues; the next sweep retries. The walk is bounded to those two
layouts: the produced container is descended once from the base store, and a
second container name or any symlink in the store is unknown ownership —
taint, never a walk — so a link back into the store cannot recurse the
daemon to death. Self-exclusion is scoped to the
root being walked — only a fragment filed directly under that root can be
the caller's own document — so a fragment carrying the same key in a foreign
namespace stays protected. The tier-loop crash this closes (runtime
`8990d78df216`, 2026-09-21) was a name, not a byte: the census called
`read_fragments` with the produced bookkeeping directories as consumer keys,
and `cycle -> sweep_orphans -> sweep -> reconcile -> attributed_stage_paths`
raised `ResidencyMapError` every cycle before the service could publish a
lead.

### Where stage capacity comes from

A stage tier's capacity is `available` on its dataset (`<pool>/prewarm`, or the
pool's root dataset when that does not exist), never `zpool list` `size`. They
are different numbers and only one is a promise: `size` is the raw geometry,
while `available` is what the dataset may actually write after parity, the slop
reservation, quotas and whatever its siblings hold. Minting from `size` puts the
slop reserve inside the accounting as an overfill margin — tokens for bytes the
pool refuses at ENOSPC, discovered by a mover that has already read them off the
disks. The record carries `capacity_source` so the fallback announces itself.

### The pin, and what may take it back

**Held tier tokens cover the bytes on the stage, at every instant.** Every
resident byte is behind a held token, and a holder is settled exactly once.
Equality is the ordinary case; a conservative reservation is the one
documented exception, and it errs the safe way -- a partial stale-mention
prune (#853) deletes some of an owner's files and keeps the owner's whole
charge, so the tier reserves for bytes that have already gone until the last
fragment leaves through the ordinary whole-owner egress. Tokens for bytes
that are gone cost capacity; bytes with no token behind them are the overfill
the reservation exists to prevent, and nothing here creates those. A mover
keeps its tokens from `finish` until an egress deletes its files, because releasing at
`finish` bounds concurrent copies rather than resident bytes: twenty-one movers
of 34.4 GB run one after another leave 722 GB on a 721 GB stage while the ledger
reads its full supply free at every step. `PoolQueue.residency_pin_holds` decides
it from the mover's own receipt — a record that exists, carries no refusal, names
the same tier, says `complete`, and whose `bytes_staged` equals its declared
range — and `finish` releases the host reservation while keeping the tier's.

There is no retained-but-unpinned state, so the egress node is one operation:
delete this mover's files, release its key, drop its fragment, in that order. A
crash after the deletes costs capacity until a sweep returns it; a crash after a
release would leave bytes on a stage the ledger believes is empty, which is the
failure the whole accounting exists to prevent. An unreadable fragment keeps the
tokens and deletes nothing: its bytes may be there and cannot be named.

Because it is the only node that returns tokens, an egress must be admissible
exactly when the tier is fullest: on the stage's own file server, beside the
resident loops that make that box hold something at all times. Its row
therefore declares `{"cpu": 1, "mem_gb": 1}` — no tier demand, it *returns*
that — with the CPU a declared bound of the single-process unlink-and-record
it is, never a measurement: an egress files no receipts, and pricing it off
the movers' copy receipts would measure the wrong node (#655's lesson, and
#607's unknown-CPU discipline on the node that fix skipped; 2026-09-19,
dl380g10, campaign `397b8f851004`'s three `stage-release` rows refused
`unbounded_cpu_not_exclusive` at `psi 0.043`, `busy 3.61 of 80`, eight
holders).

A consumer is admitted only when every lead has moved its bytes **and still
holds them**; the second half is `residency_lead_unpinned`, and a missing mover
receipt fails it, so "no receipt" can never read as "staged". Tokens alone are
not the pin: they are filed under the key at *claim*, before a byte is written,
and only kept past `finish` when the receipt says the range landed, so a lead
that is claimed right now holds tokens exactly like one that finished and
pinned. `_lead_is_pinned` therefore also refuses a lead that is claimed and one
whose receipt is not a complete, unrefused copy (`staged_range_of`). On
2026-09-18 a consumer's claim scan landed inside one replay's claim window,
read a `done: executed` record from the previous generation beside claim-time
tokens, judged the head resident, and was admitted onto a stage missing the
range's 7 GB anchors file (#625).

There is one other way a mover's tokens may change hands, and it is a hand-over
rather than a return: `ResourceLedger.transfer` renames each token between two
holder directories under `held/`, so a range that is already on the stage can
change owner without any instant in which the ledger reads capacity it does not
have. Release-then-reacquire has exactly that instant, and whatever is admitted
inside it lands on a stage that is full. A transfer interrupted part-way splits
the reservation across two holders: the sum is unchanged, nothing is lost and
nothing is over-admitted, and calling it again finishes the move. That is the
whole ledger half of adoption, below.

### Funded mover claims (#738)

A forthcoming window policy can reserve one next movement on the existing
tier ledger through `PoolQueue.reserve_fence`. Its funding record binds the
consumer, sealed plan, mover publication, tier, range, token kind and count
to one generation. These bindings are immutable within that generation.
The only state steps are `reserved -> transferring -> consumed`, or
`reserved/transferring -> released` before consumption. Generation rotation
belongs to the checked reserve path; public writes cannot turn a consumed
reservation back into credit. Mover transition locks serialize these writes.

Claim transfers the reserved tokens rather than releasing and reacquiring
them, and acquires only the unfunded remainder. Execution requires the
durable claim, lease and consumed funding proof. If their publication fails,
the row returns to READY and only newly acquired remainder tokens return;
funded holdings stay recoverable. A consumed record never discounts another
claim, even when the mover still holds the full token count: those tokens may
already represent physical bytes. Normal pin/egress rules govern those bytes.
The present single-residency schema permits at most one funded tier per mover.

Recovery reads at the authoritative mutation points distinguish *missing*
from *unreadable, corrupt, or empty* (`PoolQueue.read_funding_evidence`):
`read_funding` still collapses both to `None` for reads that decide nothing,
but reserve, settle and the protect pass defer on unknown — a present record,
terminal proof, or holder census that cannot be read is unproved authority,
never absence, so nothing is unlinked, closed, cancelled or fenced beside it
(each deferral names its reason). Holder censuses that gate a mutation read
error-visible (`_glob_visible`, #742 semantics); `Path.glob` hides `EACCES`
as an empty listing, which would silently strand a held fence while its
record closes. The split stale-rehome retry verifies exact post-transfer
ownership across mover+grant — the mover empty and every bound name under the
grant — before closing `transferring -> released` or taking any deficit,
because `ResourceLedger.transfer` suppresses individual rename failures and
its returned count cannot witness previously moved names; a short or failed
rename retains the record and the partial split for the next cycle, and the
same retry converges once the fault clears. These are the recovery rules for
the existing seams, proven by targeted component tests only; the claim
path's `funded_cover` still answers `(0, None)` on unreadable records
(fail-closed for cover), and no whole-fleet conformance or deployed-support
claim is made.

This is the claim primitive, not the complete progress policy. Joint window
funding, fairness and two-consumer liveness remain unqualified until the tier
loop uses it. Funding generated outputs from a producer's already admitted
window also needs an exact-owner transfer extension; a second acquisition is
not evidence that the same bytes have been accounted once. Component tests
establish neither deployed support nor whole-fleet conformance.

**Advance fencing and its return contract (#832).**
`residency_plan.advance_needs` names the fence on *every* answer:
`fence_target` is the leg after the frontier (the earliest unstaged ahead
leg), `fence_prior` the legs before it, and a final leg is
`fence_target: None` said explicitly -- a missing field is never "final".
`tier_loop._protect_tier_advances` fences exactly that one advance per
consumer/tier/leg. It binds the fence to the mover's queued row when the row
is already published (the record carries that row's `published_unix`, so a
republished key never inherits older credit) and takes it blind under the
grant before its own publication when it is not, so a current is never
exposed without the room its next step was promised. A blind grant whose
target row is not published yet is itself the retained proof **only when it
already covers the whole demand**: the pass permits the window (blind-held)
and defers the bind to the pass that sees the row, because gating the row's
publication behind a bind that waits for that row is circular and wedges the
window behind its own fence. A partial grant is never that proof -- it falls
through to the bind path and fails closed while the row is unpublished,
retaining what it holds; replenishment for an unpublished advance has no
supported path (the bind needs the row's own `published_unix`), so a partial
grant is an unsupported state that holds rather than publishes. A bind that
fails beside a published row, and any unreadable record, row census, ledger
or capacity evidence, still denies; nothing is bound beside unknown
authority. A target whose own key already holds the demand is landed (or a
live fence) and is never fenced twice; a grant the current frontier no
longer needs is released by the dangling pass. The dangling release runs
after the want pass on a complete census, so one cycle can still gate
transiently while a superseded target's grant holds the fresh target's room;
the next pass takes from the returned room. Scoped validation and its
remaining limits are in `832_advance_fence_acceptance_2026-09-21.json`.

#### Prepaid-output funding from the admitted window (candidate, R6)

Funds one precommitted produced-output batch from the producer's existing
window with an exact token-subset transfer (`ResourceLedger.transfer_tokens`),
never a second reservation from free. One pool-owned authoritative intent per
output mover per tier (`*.output-funding.json`, schema
`prismabuild.tier_funding.output.v1`, same `TIER_FUNDING` dir, same
`_FUNDING_TRANSITIONS`, same mover lock as the window binding above; V1
validation unchanged). Claim-safe writer order: stage intent (reserved, 0.0
unpublished sentinel) -> publish mover READY -> drive (rotate to real
publication, transfer, `transferring`) -> commit batch (filed via the R4
`_load_batch_record`) -> claim. Prewrite is budget, filed batch is commit,
pool intent is the sole funding authority. Creation needs the exact live
owner; cover requires live-OR-terminal owner plus the filed commit (never
prewrite-only); drive accepts precommit-OR-commit. Owner-outer/mover-inner
lock order serializes fund against owner finish (`_release_reservation` holds
the owner lock with fail-retain census: UNKNOWN retains all). Pending output
intents refuse fresh-acquisition fallback in `_begin_tier_acquire`
(`output_funding_pending`). Release proves mover nonexecution (no
CLAIMED/DONE/FAILED/receipt/lease) before retiring credit. See
`prepaid-output-pool-api-design.md` (R6) and
`tests/test_prepaid_output_funding.py` (candidate-component scope; parent
stack root-unaccepted). The sealed `produced_output_batch` reference in mover
params (derived from the CAS-filed action request at publication, kwarg only
for direct API) is the positive required signal; claim never scans output
history. At claim, `_begin_tier_acquire` reads the CAS-filed request ONCE per
action (cas_root from the listing row): requiredness = immutable ref OR sealed
projection key OR any funding-file state; the READY projection must agree
with the immutable ref, and output cover must bind back to it; unknown READY
or request evidence defers, never fresh (key ABSENCE alone is legacy). A
successful cover rests on that same immutable agreement rather than replacing
it: unknown request evidence, a filed request that declares no reference, a
contradictory projection, or a record that fails the immutable binding all
drop the cover (defer, never fresh); the funding-record binding is the
shared identity fields, since `batch_namespace` is projection-only (the
record's closed schema carries no such field). The renamed claim re-checks
the same agreement.
Transfer uses the accepted tier mutation guard (`_guarded_mutation`,
blocking; host ledgers no-op). Writer (744) MUST include the reference for
every output mover via `build_produced_output_batch_ref` (omission yields
legacy treatment).
Publication derives the projection from the CAS-filed request (contradictory
kwarg and corrupt requests refuse) and requires staged-or-committed intent
before READY exposure; claim derives requiredness from the filed request once
per action (combined mutable-authority loss defers, never fresh).

**What a finish reads (#747).** `consumed` is terminal and nothing advances
it, so every produced batch leaves one record behind. The finish census reads
only what it can count: `output_keep_names_for_owner` skips the census when the
owner holds nothing on the tier (the keep set is a subset of the holdings, and
UNKNOWN retains only holdings), and `_release_reservation` reads one census per
conclusion across all tiers. The tier loop runs
`retire_terminal_output_funding` once per cycle. It moves a record to
`tier-funding/retired/` only when the record is `consumed` or `released`, its
mover has a filed `done`, `failed` or `withdrawn` record with no `ready` or
`claimed` row, no lease, and no token on the record's tier, re-read under the
mover's transition lock. Per-mover reads (`read_output_funding`,
`output_funding_file_state`, `_output_funding_unretired`) fall back to the
retired copy, so every decision about one mover reads what it read before, and
writers refuse to file beside a retired record. Only the directory scans stop
reading it. Measured on the 2026-09-22 queue (973 records, 972 terminal):
concluding an action that holds no tier token went from 2,919 record reads and
0.80 s to none and 0.0007 s; a holder's conclusion went from 2,919 reads to one
census over the live records only.

#### Operational writer path (R7 integration, candidate)

`produced_output.publish_prepaid_batch` is the one production call per
finished batch: sealed reference -> real CAS request for the stage mover
(params carry the reference) -> `stage_output_intent` -> `publish` ->
`fund_output_batch` (exact transfer of the owner's existing window) ->
`commit_batch`. `commit_batch` reconciles the pool record (drives a
`reserved` remainder through `drive_output_funding`, requires
`transferring` with the mover holding the full token set) and files the
batch with no second acquisition; the legacy per-batch
`{batch_id}.funding.json` path survives only as in-flight recovery for
batches that already filed it. Liveness, closed with the integration:
`release_output_funding` refuses once `_output_batch_authority` holds
(committed batches are recovery, not cancellation); `abort_prewrite`
refuses while an owner intent cites the prewrite (`prepaid-intent-exists-
retain`; retire the intent first); `stage_output_intent`/`fund_output_batch`
never select a token name already promised to another outstanding intent
of the same owner (`tier-reservation-unavailable` instead of a wedged
transfer-short). `admit_funded_window` reports the delivered binding
(`mode: prepaid-per-batch` + `owner_demand_terms`) once the funded-claim
primitives are present.

Every seal path (`publish_prepaid_batch`, `ensure_batch_materialized`) builds
the mover through `_seal_output_mover`, and that child inherits the producer's
own checkout addressing: the producer request's validated
`params.checkout_snapshot` and the declared input it names ride the child's
sealed params, exactly as `movement_actions.seal_movement_action` already
carries them for pbrun's own movement nodes, while the row's addressing keeps
coming from `_producer_launch_context`. The worker's `preflight_action` then
proves the snapshot the row materializes -- the sealed commit, clean, with its
sealed ancestry -- instead of falling through to the `fleet/pbrun` closure
stamp, which describes the producer's pre-snapshot tree and that no
materialized snapshot can satisfy (the 2026-09-21 Stage A launch blocker: the
first produced mover was refused in 0.76 s before `stage_move` ran). A
producer without a snapshot keeps its absence -- no snapshot param is
invented, and the legacy stamp proof applies unchanged -- and a malformed
record, or one whose declared input the child does not inherit, refuses at
seal time (`producer-checkout-snapshot-invalid` /
`producer-checkout-snapshot-input-missing`) rather than on a worker after a
claim.

#### Produced-output physical namespaces (#849)

New produced-output movers explicitly seal `--produced-output-namespace` with
PB's existing immutable `batch_namespace`. Their files live below
`<tier-root>/produced-output/<batch_namespace>/<manifest-relative-path>`.
The registered tier root, ownership lock, fragment and material schemas,
original descriptors and prepaid funding stay unchanged. Different batches
or owner attempts can therefore retain different bytes with the same basename
without replacing one another's material. The namespace is stable across the
same batch's later materializations; normal pin-aware retirement still must
finish before a successor can reuse that batch's paths.

Before staging, the opt-in requires the CAS request's validated produced-output
reference, the existing template/namespace/demand validator, and exact agreement
with the invocation's consumer, tier, manifest and range. It cannot accept a
foreign namespace, arbitrary path component, missing or corrupt reference, or
an unsealed `--manifest` override. Destination collision checks, copying and
same-key partial-coverage recovery all use the same namespace derivation.
Ordinary inputs and historical mover requests without the explicit flag retain
their original path layout; nothing migrates or overwrites existing material.
A source merge does not change sealed deployed runtimes or revive a mover whose
funding was already consumed (#848).

#### Write-only templates: origin-only batches (#912)

A producer whose outputs only a later action reads declares
`"write_only": true` in its template. Every tier in its `working_demands`
must then carry `minimum_gib` 0 and `window_gib` 0: the tiers stay named,
because the prewrite names the tier and the spool paces its export to that
tier's pool-side fill, but the template reserves no window.
`owner_demand_terms` skips a zero window, so the producer's sealed demand
carries no tier term and its claim takes no stage token. A template without
the field, or with `false`, is canonicalized without it and keeps its
`template_sha256`.

Such a producer writes under `require_prewrite` as before, and commits each
batch with `produced_output.commit_origin_batch`, the write-only sibling of
`commit_batch`. It makes the same checks under the output-prefix lock (live
owner, matching prewrite, omitted planned paths absent, durable maxima, one
lstat per origin), with two of its own: every descriptor carries its sha256
(`origin-batch-needs-sha256`), and when the caller passes the identities its
writer recorded as each file landed (`landed`), each origin must still be
that file (`origin-is-not-the-landed-copy`). The spool's
`ProducedSpool.commit_origin_group` passes them from the export receipt and
refuses before the receipt is durable: a retried export can replace a copy
that landed earlier. The commit files an immutable batch record with
`origin_only: true` and `mover_key: null`, files the commitments entry, and
consumes the prewrite. It seals no mover, moves no token and writes no stage
copy. A replay over the same manifest answers `duplicate`; a crash between
the record and the entry resumes from the filed record. `commit_batch`,
`publish_prepaid_batch`, `refill_window`, `admit_funded_window` and
`ensure_batch_materialized` refuse a write-only template
(`template-is-write-only`), and `commit_origin_batch` refuses a read-back one
(`template-reads-back`).

Every reader that asks whether a stage copy is live hears no:
`_active_materialization` answers `origin-only`, retired, with no mover, so
`retire_batch` returns `staged: false`, `safe_release_instance` finds no
batch or mover to hold it, and `recover_batches` emits
`output-batch-origin-only`. The commitments entry keeps `retired: false`,
because `_committed_restage_authority` reads that flag and it is what keeps
the batch from being funded as a restage. Path ownership does not follow the
"no copy" answer: the batch owns its origin paths, and keeps its durable
class bytes charged, until `reclaim_origin` proves every origin absent.

A consumer declares batches by reference. `origin_batch_ref` names a batch
by where PB filed it (owner action key, attempt nonce, template id, batch
id) and pins it by manifest digest. `origin_batch_manifest(queue_root, refs)`
builds the v1 data manifest the consumer submits: each batch's entries in
its own order, one read phase per batch, the common directory of the output
prefixes as `mount_prefix`, and the references under
`annotations.produced_output_batches`. Each batch resolves through
`load_origin_batch`, which reads the filed instance, template, commitments
entry and immutable record, and refuses a batch that is not write-only,
uncommitted, committed over another digest, reclaimed, or whose origin no
longer has the identity its commit recorded. A path named by two batches
refuses. The consumer submits the manifest with
`pbrun --data-manifest --residency stage`. At freeze, `pbrun` derives the
manifest again from the queue (`require_declared_origin_batches`) and refuses
one whose `mount_prefix`, entries or references differ, and any transport
other than the pull queue. A manifest without the annotation is not checked.
A data_manifest.v2 instead declares its batches where its read plan reads
them, and is checked by placing them again ("Produced batches in a v2 read
plan (#946)"). From there the batch is an ordinary input: the tier loop publishes the
consumer's lead mover, and the mover verifies each entry's sha256 as it
copies.

Limits:

- Path ownership is per owner attempt, as it is for staged batches: a
  retried attempt can commit a second origin-only batch over a path an
  earlier attempt's unreclaimed batch still names, and both stay charged.
  A consumer cannot stage the wrong bytes: the earlier batch refuses its
  identity recheck at submission, the manifest refuses a path named twice,
  and the mover verifies digests. For a `consumed` batch, #914's orphan
  sweep releases the earlier attempt's charge (next section); a `retain`
  batch keeps it until its files are gone and `reclaim_origin` runs.
- A `retain` batch records no consumers, so a producer that deletes its
  origins and reclaims can strand a consumer frozen before the deletion: its
  mover finds the origin gone. A `consumed` batch is held until its declared
  consumers succeed (#914).
- `pbrun` rechecks origin identities from the submitting host, and the tier
  host must mount the output prefix for its mover to copy it.

#### Consumed origin batches: retired after their consumers (#914)

A write-only producer gives each batch a lifetime when it commits it, with the
`lifetime` argument of `commit_origin_batch` or
`ProducedSpool.commit_origin_group`:

- `retain`, the default, is the #912 batch. PB never deletes it, even after
  its producer fails. Its record and its commitments entry carry no
  `lifetime` field, so it is filed byte for byte as #912 filed it.
- `consumed` marks a handoff that PB retires. The record and the entry carry
  `lifetime: "consumed"`, and a replay that names another lifetime refuses
  (`batch-lifetime-mismatch`).

**Consumer declarations.** A consumer declares the batches it reads in its
data manifest, as in #912. After `pbrun` seals the consumer's key and before
it publishes the row, it calls `declare_origin_consumer` for each declared
batch (`pbrun.declare_origin_consumers`, ahead of both the staged and the
plain publication).
For a consumed batch this files `consumers/<batch_id>/<consumer_key>.json`
under the batch's instance directory. The call takes the batch's
output-prefix lock, one batch at a time, and refuses a batch that is retiring
or reclaimed. So every queued consumer of a consumed batch is on file before
the retirement can see its row. The same key declaring again finds its own
file, unless an operator released that declaration: a released key is
refused (`origin-consumer-released`, #945). A `retain` batch files nothing.
From its first declaration through its row, `pbrun` holds the consumer key's
transition lock (`pbrun.submission_window`, #945), in `main` and in the
deferred release alike. The declarations take their output-prefix locks
inside it, in the usual transition-then-ownership order, and the row's own
publication takes the same transition lock again, which nests. A consumer
that declares no consumed batch takes no lock.

**The decision.** `tier_loop.cycle` calls `origin_retirement_tick` once per
cycle, after it retires terminal output funding. The tick scans the
produced-output scopes, as `unheld_window_gib` and `output_scope_tick` do.
For each consumed batch that is not reclaimed, it takes the batch's
output-prefix lock and decides:

- If consumers are declared, every one must have succeeded. A consumer's
  state is its action key's latest generation. A `claimed` or `ready` row,
  or a claim a finisher or reaper is moving (a tombstone or late-finish file
  in `claimed/`), holds the batch without a log line. A widowed lease is not
  read. A `done` record with status `executed`
  or `cache_hit` is success. A failed, withdrawn, unpublished (declared but
  no row) or unreadable consumer holds the batch, and the tick logs it as a
  stall, because a retry of that consumer needs the batch. A resubmission
  publishes a new generation of the same key and leaves the earlier failed
  record in `failed/`, so the terminal record with the latest
  `published_unix` answers for the key. The claim-intent marker outlives its
  claim and is not read. Each key's state is read twice, and two reads that
  disagree read as unknown. A failed or withdrawn consumer stops holding the
  batch in two cases (#926), and an unpublished one in one (#945), listed
  below under "Replacing a failed consumer".
- If no consumer is declared, the batch is an orphan once its producer
  attempt is dead: the owner's claim names another attempt, or its latest
  generation ended `failed` or `withdrawn`, or it is `done` by another
  attempt. An owner that is `done` by this attempt, still claimed by it,
  queued, being moved, or unreadable keeps the batch without a log line,
  because a consumer may still come.

**The delete.** The tick first stats the instance's output prefix. If the
prefix is not a directory on this host, it refuses
(`output-prefix-unreachable`) and keeps the batch, because an absent file
proves nothing on a file system that is not mounted. It then compares each
origin with the identity the commit recorded:

- The same file is deleted.
- An absent file is already gone.
- A file with another inode is no longer this batch's: a retried producer
  attempt wrote the path again (per-attempt ownership, above). The tick
  leaves it, and the batch stops charging for it.
- The same inode changed in place, or a stat that fails, refuses
  (`origin-changed`, `origin-unstatable`) and keeps the batch.

Before the first unlink the tick sets `retiring: {reason, consumers}` on the
entry. From then on `declare_origin_consumer` and `load_origin_batch` refuse
the batch (`origin-batch-retiring`), and a crash resumes the delete instead
of deciding again. Each file is compared again just before its unlink. The
tick then fsyncs the parent directories and sets `origin_reclaimed: true`,
which frees the durable class bytes and the paths, as `reclaim_origin` does.

**Logging.** Each retirement prints one JSON line in the tier log:
`output-origin-retired` with `ref`, `bytes`, `reason` (`consumed` or
`orphan`), `consumers`, `origin_identity` (the recorded identity each file
was checked against) and the `unlinked`, `superseded` and `absent` paths. A
stall (`output-origin-retirement-stalled`, with the consumers and their
states) or a refusal (`output-origin-retirement-refused`, with a reason)
prints once per change: the entry keeps a digest of its last report in
`retirement_report`, and drops it when the hold clears. A report the tick
cannot file on the entry, because the entry or its scope is unreadable or
the step raised, is remembered by the tier-loop process instead and prints
again once after a restart. A cycle with nothing to retire and nothing new to
report prints nothing.

**Replacing a failed consumer (#926).** Every publish moves every key, so a
failed consumer is usually resubmitted under a new key, and its first
declaration can never succeed. A failed or withdrawn declaration is resolved
in either of two ways:

- **Superseded.** `pbrun --supersedes OLD` files `supersessions/<OLD>.json`
  (#913), which is allowed only once OLD is failed or withdrawn. The tick
  follows that chain, through further supersessions and through a deferred
  submission's release to the key it was released as
  (`action_edges.successor_of`). When the chain ends at a key that is also
  declared against this batch, the old consumer's state is `superseded`, with
  `superseded_by` naming that key, and the successor holds the batch in its
  place: it must succeed. An ordinary `pbrun --supersedes` files its
  declaration before its supersession, so the successor is never missing from
  the batch in between. A chain that ends at a submission not yet released,
  or at a key that did not declare this batch, resolves nothing; the stall
  names it as `superseded_by`. A deferred retry is also held by #913's rule
  until it is released.
- **Released.** `pbrun --release-origin-consumer BATCH_REF CONSUMER_KEY`
  calls `release_origin_consumer`, which files
  `released-consumers/<batch_id>/<consumer_key>.json` beside the
  declarations, with the consumer's state, who released it, and `--reason`.
  It first takes the consumer key's transition lock without waiting, and
  refuses while a submitter holds it (`origin-consumer-submitting`, #945).
  Then, under the output-prefix lock, it refuses a batch that is `retain`,
  uncommitted, retiring or reclaimed; a key that did not declare the batch;
  a consumer that is queued, claimed or being moved
  (`origin-consumer-live`); and one whose state is anything but failed,
  withdrawn or unpublished. The consumer's state then reads `released`. The
  declaration file stays, and the key can no longer declare the batch. Before
  its record, the release files an entry in the queue-wide index (#954,
  below), so a claim refuses any row of the key.

**Releasing an unpublished consumer (#945).** A declaration with no queue
record is left by a submitter that died between its declaration and its
row, or belongs to one still between them. A release accepts it only when
nothing can publish that key any more, which it shows from these facts:

- **No submitter is in the window.** Every submitter holds the key's
  transition lock from its first declaration through its row, and the
  release holds it too, so no submitter is between the two while the
  release decides.
- **The live generation did not seal it.** The key's sealed request (the
  one `pbrun` publishes before it declares) leads its `PATH` with the
  wrapper of the generation that sealed it (`action_edges.request_wrapper`,
  which `--as-sealed-by` also reads). If that is the live generation
  (`SH/repo`), the release refuses (`origin-consumer-unpublished-live`):
  submitting the same key again clears the hold and runs the work. Without
  a readable request or a live generation it refuses too
  (`origin-consumer-unpublished-unknown`).
- **No pinned release names it.** A deferred release (#913) pins its key
  before it publishes anything. One that stopped before its row resumes on
  a later tick and publishes exactly that key, sealed into the retained
  generation its template froze. While such a pin is unpublished the
  release refuses (`origin-consumer-release-pending`,
  `action_edges.pending_release_of`). A pin it cannot read also refuses.
- **The key cannot come back.** After the release, `declare_origin_consumer`
  refuses the key (`origin-consumer-released`), under the same
  output-prefix lock. A later submission of it, whether an identical
  resubmission or `--as-sealed-by` from a retained generation, dies at its
  declaration, before any row. So the release does not have to prove that
  nobody will ever submit the key again: it makes that submission unable to
  read the batch. A row that a `pbrun` older than #945 still publishes, after
  declaring before the release, is failed at claim (#954, below).

The release records the wrapper that sealed the key as `sealed_wrapper`.
The retirement tick reads an unpublished consumer with a release as
`released`; nothing supersedes an unpublished consumer, because
`--supersedes` accepts only a failed or withdrawn key.

**Refusing a released key at claim (#954).** The declaration refusal runs
only in live `pbrun`. A `pbrun` older than #945 takes no lock and makes no
check between its declaration and its row, so its row can land after the
release, reading a batch the retirement tick may already be deleting. The
release is therefore enforced where live code always runs:

- **The index.** `release_origin_consumer` files
  `released-origin-consumers/<consumer_key>.<ref sha256>.json` at the queue
  root, naming the key and the ref, before the release record. Only the
  record makes the release real: an entry without one is a release that
  stopped before its record, and `origin_consumer_release` reads it as no
  release. The order is the point. A record with no entry would let the
  retirement tick delete the batch while the claim, which lists only the
  index, ran a row of the key. Running a release again for a record that has
  no entry, such as one filed before #954, files the entry.
- **The claim.** `PoolQueue._claim` lists the index once per scan, as it
  lists `withdrawn/`, and for a listed key confirms the release against its
  record under the key's transition lock, which a release also holds while
  it writes the entry and the record. A confirmed key's ready row is failed
  before placement, so any box's scan files it: status
  `origin_consumer_released`, with `refusal: origin-consumer-released`, the
  ref, and the release's state, author and time in its `detail`. The row
  keeps its `published_unix`, so the submitter's wait loop reads it as its
  generation's ending, and its ready bytes are kept under
  `withdrawn/superseded/`. The release binds the key, not one generation: a
  key seals its data manifest, so every row of it reads the batch. A
  release that cannot be read is a denial,
  `origin_consumer_release_unreadable`, and the row stays ready. The row is
  captured in `ready-transitions/` (kind `origin-released`) before its ending
  is written, so `sweep_ready_transitions` restores it after a crash and the
  next claim files it.
- **The tier loop.** `tier_loop.live_consumers` leaves out a ready consumer
  whose key has a confirmed release, or one it cannot read, so the window
  publishes no phase of its frozen plan, not even its lead. Once the claim
  has failed the row, the dead-consumer sweep (#620) archives the plan. A
  claimed consumer is never left out: it was claimed before any release.

The old consumer's own state answers first while it is queued, running or
has succeeded. Since #945 a released key also cannot declare the batch
again, so it never holds it again. A batch retires once every declared
consumer is `succeeded`, `superseded` or `released`, with the same delete as
above. The other declared consumers still hold it.

A batch is *blocked* when every consumer still holding it (every declared
consumer that is not `succeeded`, `superseded` or `released`) is `failed` or
`withdrawn`: nothing queued or running will free it. The stall line fires
earlier, as soon as any holding consumer has failed, so it also covers a
batch that a live consumer still holds; that batch is waiting, not blocked.
The stall line names each consumer's terminal state once per change. It is
byte-identical to #914's unless a supersession exists that did not apply;
then that consumer also carries `superseded_by`.

`pbstatus --blocked-origins` lists the blocked batches at any time, so the
leak does not scroll away with the log. It reads the same records the tick
does through `produced_output.blocked_origin_batches`, skips the scopes the
tick skips, takes no lock and writes nothing. It skips a batch that is
retiring, reclaimed or without a
declared consumer, and one held for an unreleased deferred consumer (#913),
which is waiting. Each entry carries the batch's `ref` and `bytes`, every
declared consumer's resolved state, the holding keys, `reported` (whether
the entry carries the tick's report memo) and, per holding consumer,
`remedies`: the exact `pbrun --release-origin-consumer` command, and the
`--supersedes` resubmission with the consumer's own options and command left
as placeholders. The remedies are computed when the listing is printed and
never stored in the event or the entry. A record it cannot read goes to
`unreadable`, `complete` turns false and the command exits 3; if the deferred
holds cannot be read, it lists nothing, because no batch can then be told
apart from one waiting for a release. The MCP tool `pb_blocked_origins`
serves the same reader.

**Where it runs.** Only dl380g10 runs the tiers role. There `/mnt/shared` is
the local ZFS dataset, so the tick stats origin paths as they are written;
the tier loop takes no `--mount-map`.

Limits:

- A consumer that `pbrun` declared and that never reached the queue (the
  submitter died in between) holds the batch, and the stall names it
  `unpublished`. On the live generation, submitting the same key again
  clears it. After a publish it can be released (#945, above). A publish
  alone does not stop a key being submitted again: `--as-sealed-by` reseals
  it from a retained generation, and an old-generation `pbrun` that is still
  running can still publish its row. The release therefore makes the key
  unable to declare the batch again, not unsubmittable.
- A `pbrun` older than #945 does not hold the key's transition lock between
  its declaration and its row. A row it publishes after the release is
  failed at claim (#954). One it publishes after the release has read the
  consumer's state but before the release's record lands can be claimed
  first. It then runs, and the retirement tick waits for it, because a
  claimed consumer reads `live` whatever its release says; only a batch the
  tick had already started to retire can go from under it.
- A worker or tier loop still running a generation older than #954 does not
  read the index, so the claim refusal holds only on boxes that run #954.
  A release filed by a generation between #945 and #954 has no index entry
  until its release command is run again.
- `pbstatus --blocked-origins` does not list a batch that an unpublished
  consumer holds, because a submission may still be in its window. The stall
  line names the consumer `unpublished`; `--release-origin-consumer` decides
  whether it can be released.
- A key that an operator released cannot be the key of a later deferred
  release that reads the same batch: its declaration is refused, and the
  release is refused each tick with the producer's batches held for it.
  That needs two submissions to seal to one key.
- Every declared key must succeed, be superseded by a key declared against
  the same batch, or be released. A failed consumer resubmitted under a
  different key without `--supersedes` keeps holding the batch until an
  operator releases it (#926).
- A supersession is followed only from a failed or withdrawn consumer, and
  only to a key that declared the same batch. A resubmission that reads a
  different batch of the same producer, for example after the producer was
  retried, leaves the first batch's declaration to an operator release.
- The stall line is logged once per change. A batch that stays blocked is
  not logged again until something about it changes; `pbstatus
  --blocked-origins` lists it until it is freed.
- A consumer submitted by a `pbrun` older than this change files no
  declaration, and the orphan sweep can delete a consumed batch under it once
  the producer is dead. Only a producer running this change can commit a
  `consumed` batch, so the gap is a consumer submitted from an older
  checkout while a newer producer runs.
- The comparison and the unlink of one file are two calls. A writer that
  replaces the path between them loses its file. Only a retried attempt of
  the same producer writes those paths, and only under its own prewrite.
- The latest generation is ordered by `published_unix`, which each
  submitter stamps with its own clock. Two generations of one key published
  within the clock skew between two submitting hosts can be misordered.
- The reachability check assumes the output prefix lies below a mount
  point. A prefix that is itself the mount point of an unmounted file system
  is still an empty directory, and its origins would read as absent.
- The tick unlinks files only, never directories. An instance whose
  commitments cannot be read is skipped, as the other scope scans skip it:
  its batches stay on disk and charged.

#### Deferred consumers: action edges (#913)

A consumer can be submitted before its producer runs. `pbrun --after
PRODUCER:TEMPLATE_ID` names a write-only template (#912) that the producer
declares. PB holds the submission until the producer succeeds. It then builds
the consumer's data manifest from the origin-only batches the producer
committed, and seals and publishes the consumer.

**Why the consumer is sealed at release, not at submission.** A key covers
the action's data manifest, and a manifest entry names a path and a positive
byte count (`core.validate_data_manifest`). A handoff's paths and sizes exist
only after the producer commits. A consumer keyed at submission would have a
key that does not cover the bytes it reads, and it could have no residency
plan, because its mover ranges index into that manifest. `dagster.py`'s
`CASDependency` has the same shape: an edge carries the upstream key and the
digest it produced, so the downstream action is keyed after the upstream
finishes. PB therefore keeps the frozen submission and seals it later,
instead of adding a second kind of key.

**Submission.** `--after PRODUCER:TEMPLATE_ID` is repeatable and needs the
pull queue. PRODUCER is an action key, or the pending id of another deferred
submission, which is how a chain names a consumer that is not sealed yet.
`pbrun` refuses the edge when:

- the producer is unknown: a key needs a readable row or terminal record,
  and a pending id needs a readable deferred record;
- the producer does not declare the template, or the template is not
  write-only.

`pbrun` then prepares the submission as it would any other: the checkout
gates, the environment, the demand and the placement, which it announces and
refuses as usual. It freezes the template without a data manifest, and
refuses it when no release could seal it: the template's generation must be
the published one or pass the retained-generation check below. A `pbrun` in a
development checkout freezes its own wrapper, which is neither. The
optional `--data-manifest` is the consumer's static part (for example, a
model head); it is ingested and kept beside the template. It is plain JSON:
a data_manifest.v1, or a data_manifest.v2 whose read plan names the phase
each edge fills (#946). The command may
carry `{pb.data_manifest}` and `{pb.data_manifest_sha256}`, each at most once
and as a whole argument. At release the first becomes the CAS path of the
resolved manifest, as `decomposition.resolve_task_batch` does for a batch
path, and the second the SHA-256 the sealed request binds for it
(`params.data_manifest.input.sha256`), which is the digest of the file at
that path (#933). A consumer that hashes the file and compares detects a
manifest that changed after its release; the key covers both values.

The frozen template, the static manifest, the edges and the publication
options (priority, attempts, retry safety and the residency options) form a
deferred record, filed immutably at `pb-queue/deferred/<pending_id>.json`.
The pending id is the SHA-256 of the record's canonical JSON, so an identical
submission finds its own record. `pbrun` prints the pending id. Without
`--detach` it waits for the release and then for the consumer's terminal
record, within `--wait-s` in total.

**Release.** `tier_loop.cycle` calls `deferred_release.release_tick` once per
cycle, directly after the origin retirement (#914). Only dl380g10 runs the
tiers role, so one process releases. For each deferred record without a
publication record, the tick:

1. **Resumes a pinned release.** A record with a release record (step 4) is
   finished as pinned. The tick does not resolve its producers again; it seals
   the key the release record names, into the same generation, and refuses
   with `release-key-mismatch` if the key comes out different.
2. **Skips a superseded record.** A pending id with a supersession record is
   never released; edges that name it follow its successor.
3. **Resolves every edge.** Supersession records are followed first,
   unconditionally. A pending producer is followed through its publication
   record to its key; one not yet released holds the consumer quietly. Then
   the key's latest generation decides:
   - a `ready` or `claimed` row, or a claim being moved, holds quietly;
   - `done` with status `executed` resolves to the nonce of that attempt;
   - any other `done` (a `cache_hit`, or a record without an attempt nonce)
     resolves to the one attempt of that key that committed an origin-only
     batch under the template and not reclaimed (`committed_attempt`). A
     cache hit ran nothing: its attempt found the receipt an earlier attempt
     published, and that earlier attempt may have died before it could file
     `done`. None, or more than one, holds;
   - `failed`, `withdrawn`, absent or unreadable holds.

   A hold other than a quiet one is reported once per change
   (`deferred-held`). A failed producer therefore never releases its
   consumer.
4. **Pins the release.** It reads the resolved attempt's instance
   `<producer>/<template_id>.<nonce>` and takes every origin-only batch that
   attempt committed, in batch-id order, through `load_origin_batch`, which
   rechecks each origin's recorded identity by `lstat`. It reads and hashes
   no payload bytes. With a v1 static manifest, or none, the consumer's
   manifest is the static entries with their phases, then each batch as its
   own phase, with the batch refs under `produced_output_batches`. A v2
   static manifest has each edge's batches placed at the phase it names
   (#946). The tick seals the consumer and files a
   first-writer release record at `pb-queue/deferred-releases/<pending_id>.json`
   with the producers, the refs, the manifest input, the key and the runtime
   generation it sealed into. A crash after this point resumes from the
   record, so a producer that later succeeds again, or fails, cannot give one
   pending id two keys.
5. **Publishes as `pbrun` does after it seals.** If the queue has no row or
   record for the key yet, the tick publishes the CAS request, files the
   origin-consumer declarations (#914) and publishes the row with the
   submission's publication options. A submission that asked for
   `--residency stage` has its movers sealed and its plan frozen under the
   consumer's transition lock, as `pbrun` does. On a resume it first checks the generation again and
   each pinned batch with `load_origin_batch`. If the key already has a row
   or a terminal record, it was published before a crash kept step 6 from
   running, and its generation is taken as it is.
6. **Records the generation** at `deferred-releases/<pending_id>.published.json`.
   That is what `pbrun` and chains wait on.

`pbrun`'s notices during a release travel inside the release's log event,
because the tier log is JSON lines.

**Generation.** Two generations meet in a release, and the rule for each
matches an ordinary submission's:

- *The generation that seals.* The loop's own code seals. The tick releases
  nothing, and reports `loop-runtime-is-not-published` once, unless the
  loop's runtime root is the generation the `repo` link names. The loop
  restarts on the published generation at its next cycle.
- *The generation sealed into.* An action runs under the generation that
  froze it, and a deferred consumer is frozen at `pbrun --after` time. Its
  template's `PATH` starts with that generation's `tools` directory, and the
  container owner, the stamp name and the checkout snapshot are fingerprinted
  over it, so it cannot be re-frozen without the submitter's checkout. The
  consumer is therefore sealed into its template's generation, as an ordinary
  submission sealed just before a publish runs the older generation. When
  that is not the loop's own generation, the wrapper must pass
  `pbrun.verify_retained_wrapper`, the check `--as-sealed-by` makes: it sits
  in the generation store, beside a receipt naming that generation, and
  hashes to what the receipt recorded. If the generation is missing or does
  not verify, the consumer is held and reported once as
  `runtime-generation-unavailable`, naming the remedy: resubmit with
  `--supersedes <pending_id>`. It is never sealed into another generation.

A retained generation stays retained. Publication never deletes a generation
(`tools/fleet/publish_runtime.py` `_activate` and `_activate_existing`); the only tree
it removes is its own failed `.staging` directory, and `_remove_staging_tree`
refuses anything else. `pb_gc` surveys the CAS, never the generation store.
So nothing needs to count deferred records as references.
`pbstatus --deferred` lists every unreleased consumer with the generation it
is pinned to and `off_published` when that is not the published one, so an
operator can supersede the ones a publish fixed something for.

**Supersession.** Every publish moves every key, so a producer that failed
is often resubmitted under a new key. `pbrun --supersedes OLD` files
`pb-queue/supersessions/<OLD>.json`, first writer wins, naming the new
submission. A key can be superseded only once its latest generation is
`failed` or `withdrawn`; a pending id only while it has no release record. A
link that would close a loop of supersessions refuses. Once filed, a
supersession is followed unconditionally: if OLD later runs again and
succeeds, edges still read the successor, so every consumer of "the
producer" reads the same bytes, whenever it is released.

**Consumed batches (#914).** A consumed batch has no declared consumer until
its deferred consumer is released, and the retirement tick would delete it as
an orphan once its producer attempt is dead, or once the consumers already
declared have succeeded. `action_edges.held_producer_batches` names what must
stay, as `(producer key, template id)`: the producers a pinned, unpublished
release names, and the key each unreleased, unsuperseded edge resolves to,
unless that key failed, was withdrawn or is absent. Every attempt's batches
under that template stay, because which attempt a release reads is settled
only when it is pinned. A record that fails validation can never be released
and holds nothing; a record that exists and cannot be read keeps every
consumed batch for that tick. Separately, a batch whose producer's `done` is
a cache hit by another attempt is no longer an orphan: the cache hit ran
nothing, and that batch may be the producer's only output.

**Bounds.** The tick runs after its cycle's window publication, so a burst of
releases cannot delay that cycle's windows. It starts no release after the
cycle's deadline (`cycle_started + CYCLE_INTERVAL_S`, the cadence the #903
horizon and the #907 commitment assume) or after
`MAX_RELEASES_PER_CYCLE` (8) releases, which bounds how much new window work
one burst hands the next cycle. The rest waits for the next cycle. While
anything is unreleased, the tick logs one `deferred-release-tick` line per
cycle with its counts and its wall time. A malformed or unreadable deferred
record is reported once (`deferred-release-refused`) and kept for an
operator; it never ends the loop. With nothing filed the tick lists one
missing directory and prints nothing.

Limits:

- Two placeholders, the manifest's path and its digest. A consumer that
  needs values derived from the batches, such as a handoff path or its
  digest, reads them from the manifest; PB substitutes nothing else.
- The once-per-change memory lives in the tier-loop process, so a standing
  hold prints once more after a restart.
- A record the tick cannot release stays filed until an operator supersedes
  it; there is no withdrawal of a pending id.
- `pbwait` takes keys. A detached deferred submission's key is in its
  `.published.json` record once released.
- A supersession filed while the tick pins the old pending id can race it;
  then both the old consumer and the successor run.
- The deadline is checked before every release, including the first. A tier
  loop whose earlier work routinely takes the whole cycle interval releases
  nothing; the `carried` count in `deferred-release-tick` is the only sign.
- A released consumer that fails and is resubmitted with `--supersedes`
  gives up its declaration once the successor is released and declared
  against the same batch (#926, "Replacing a failed consumer").

#### Produced batches in a v2 read plan (#946)

A consumer that reads a producer's handoff in the middle of its readset, and
reads its own entries again after it, fits neither v1 shape. #912's manifest
holds only batches, #913's release appends each batch after every static
phase, and a v1 manifest cannot read an entry twice. PQ's Stage B band-serial
consumer reads its head, then the handoff, then replay windows over the
head's entries.

A data_manifest.v2 therefore names where it reads its batches. Its static
part is the consumer's own entries and read plan. Each phase that reads
batches reads nothing yet (`entry_indices: []`) and is named under
`annotations.produced_output_slots`, one slot per phase:

- **Ordinary submission:** `{"phase", "refs"}`, the committed batches'
  references. The submitter builds the manifest with
  `produced_output.place_origin_batches(queue_root, static, slots)`.
- **Deferred submission (`--after`):** `{"phase", "after":
  "PRODUCER:TEMPLATE_ID"}`, spelled as the `--after` edge is. The release
  builds the manifest with `action_edges.place_after_slots`, which resolves
  each edge to the batches its producer's attempt committed
  (`committed_batch_refs`) and calls the same function.

`place_origin_batches` resolves each slot through `origin_batch_manifest`,
so it refuses what #912 refuses. It appends each slot's batch entries after
the static entries, in plan order, and points the slot phase at exactly those
entries, in the batch's own order. Every other phase keeps its
`entry_indices`, re-reads included. Phase sizes, running sums, `read_bytes`
and the totals are recomputed, and `mount_prefix` is the common directory of
the static prefix and the batches'. The result lists every ref under
`produced_output_batches` and the placed `{"phase", "refs"}` slots under
`produced_output_slots`. Two slots cannot name one batch. For one static
plan and one set of refs, the two paths yield the same manifest.

**Checks at submission.** For a v2 manifest that declares batches,
`require_declared_origin_batches` calls
`produced_output.verify_placed_origin_batches`. It takes the trailing batch
entries off, empties the slot phases, places the declared slots into that
static part again from the queue's records, and refuses unless the result is
the manifest. So a non-slot phase that reads a batch entry refuses, and so do
a slot phase that also reads a static entry, a batch placed at another phase,
and a changed entry. A v2 manifest with `produced_output_batches` and no
slots, or with an `after` slot, refuses too. A v1 manifest may not carry
slots, and is otherwise checked as #912 checks it.

`pbrun.require_deferred_read_plan` checks a deferred v2 submission.
`action_edges.after_slots` requires exactly one slot per `--after` edge, each
on an empty phase, and no `produced_output_batches` in the static part. The
v2 gates of an ordinary submission also apply: the published storage
generation must read v2 (`require_deployed_read_plan_storage`), and every
read phase must be a linear progress phase, in order. With `--residency
stage`, the plan's first phase must read something or be a slot. Otherwise
the placed plan's first boundary is at byte 0, which ends no read, and the
plan has no range to stage. The release seals `schema` and `read_bytes` into
the manifest summary, as a v2 submission does.

**Residency.** The placed manifest is an ordinary v2 plan, so the window
stages its phases in plan order: the head, then the batch, then each replay
window, whose own mover stages the head's entries again
(`prewarm_loop.manifest_read_entries`, `entries_between`).

Limits:

- A deferred v2 plan places every batch through an `after` slot. It cannot
  also read batches committed before it was submitted (`refs` slots), which
  a v1 static manifest can declare.
- A v2 manifest needs static entries of its own. A consumer that reads only
  batches uses the v1 manifest `origin_batch_manifest` builds.
- A slot is filled by one edge. When the producer's attempt commits several
  batches, they are all read at that slot, in batch-id order.

#### Repeat materialization: one batch, one charge, many windows

A committed batch is an immutable logical unit with ONE durable origin charge.
The stage copy under it is not: the bounded-window path stages a batch, lets a
reader consume it, retires the copy to give the window credit back, and stages
the SAME batch again when a later read needs those bytes.
`produced_output.ensure_batch_materialized` is that one transition, and the
only one this adds. `require_prewrite` already admits every initial write
without a physical token, so first PUBLICATION is what the bounded window
delays until an actual read -- a read-back batch is never committed at its
origin (only a write-only template's is, above), and there is no v2 record.

The governing rows of the staged-read contract
(`docs/staged_read_requirements_2026-09-20.json`, 69 requirements as of
2026-09-21) are **PO-01** (origin retention and physical materialization are
separate lifetimes), **PO-02** (readiness anchors on the immutable published
batch, not an action terminal), **PO-03** (repeat materialization, one origin
charge, one live-or-pending generation, PB-derived successor identity),
**PO-04** (change detection on reuse), **PO-05** (interruption, idempotence
and retirement responsibility), **PO-06** (origin charge releases only on
proven origin deletion) and **PO-07** (lock ordering for retirement and
egress); acceptance for a strict produced read is **ACC-07**. Those three
files are maintenance's to publish -- this section is the implementation
design they describe, and the bullets below name the row each satisfies.

Everything about the batch stays bound: same owner action and attempt, same
logical batch, manifest, descriptors, digest-derived namespace and output
prefix, same durable charge, same prepaid funding, same strict SDK, same
egress, same recovery. The instance directory is metadata. The caller supplies
neither origins (the descriptors come from the immutable batch record), nor
tokens (ordinary exact transfer from the producer's window; `refill_window`
remains the producer's own lifecycle call and `ensure` never acquires from
free), nor the successor id.

`refill_window` replenishes that bounded window opportunistically (#854).
When free capacity or the nonblocking tier mutation guard refuses a top-up,
positive owner holdings at least as large as the declared `minimum_gib`
allow `ok: true`, `acquired: 0`, and
`refill_deferred: tier-reservation-unavailable`. This reports retained usable
credit, not a full window or authorization for any particular batch. The
subsequent exact prepaid transfer still checks the actual batch size and
refuses its real shortfall. Empty holdings (including a zero declared minimum),
holdings below the minimum, and unknown ownership or funding census retain
their refusal. Refill adds no retry loop and changes no retirement authority.

* **Readiness is read fresh (#808).** An owner learns that a copy landed from
  the mover's filed receipt (`materialization_state`,
  `mover_receipt_complete`), and it polls for that receipt from another box
  than the one that files it. The queue is on NFS with default attribute
  caching, where a lookup made before the name existed is cached as absent
  until the parent directory is revalidated. On sparky that held an owner for
  26.3 s to 26.6 s after a 4 s copy, on every staged group; with the parent
  opened first the same read saw the receipt in 0.04 s to 0.25 s
  (`tools/fleet/qualify_record_visibility.py`, both arms run on the real
  mount). `PoolQueue.move_record` and every leg of `_mover_live_state`
  therefore go through `pool._read_json_fresh`: a miss opens and closes the
  parent, then reads once more. The live legs matter as much as the terminal
  ones, because `_publish_output_mover_row` republishes on "absent" and
  `_tier_host_egress` republishes an egress whose row it does not read as
  live and whose receipt it does not see: in the 2026-09-21 live cycle stale
  misses republished each egress three times with `recompute`, so each ran
  four times (three no-op re-runs) and each retirement took more than 20 s.
  Opening
  is used rather than the listing `slurm_lane._read_json_object` and
  `pbrun.terminal_record` use, because these reads are polled several times a
  second and a listing costs what the directory holds (`done` held 17,117
  names on 2026-09-21). A hit costs nothing extra, and the revalidation is
  best effort: a parent that cannot be opened leaves the first answer.
* **Origin reachability is typed at the mover (#804).** A produced-output
  prefix is validated as an absolute normalized path and nothing more, and
  the mover runs on the tier host, which is usually not the box that wrote
  it. The classification is a bounded POST-COPY diagnosis, not a census over
  healthy work: only when the copy has staged nothing and did not overrun
  does `stage_move.origin_reachability_diagnosis` stat the distinct origin
  directories of the declared window -- one stat each, through the same
  mount map the copy reads through -- and file the result on that receipt as
  `origin_reachability`: `unreachable` only for positive proof (a missing
  component, a path that is not a directory, EACCES/EPERM), `unknown` for
  every other stat failure, `partial` for a mixed window, `reachable` for a
  reachable root whose declared file is gone. A mover that staged nothing
  beside an `unreachable` window refuses `origin_unreachable` instead of the
  ordinary `residency_moved_nothing`. It never vetoes the copy, so adoption
  of an already-published incarnation still completes a mover whose origin
  root is gone, and a mover that stages any byte pays no directory stat at
  all.
  `materialization_state` reads the active mover's receipt ONCE and derives
  `mover_receipt_complete` and `mover_refusal` from that exact observation,
  so the two can never describe two generations of a rewritten record; a
  typed `origin_unreachable` is returned through the failure contract every
  caller already raises on, as `ok: False` with `refusal:
  origin_unreachable` beside the retained mover/materialization identity
  (`mover_key`, `generation`, `tier`, `mover_queue_state`). The owner
  therefore stops on the FIRST failed attempt, with no consumer patch.
  Unknown I/O is never typed as an origin refusal, and an absent or
  unreadable receipt leaves both answers None -- silence is not a named
  failure and not readiness. Only the ACTIVE materialization's own receipt
  can answer this: a retired predecessor's refusal is history and never
  poisons its successor.
* **Successor identity (PO-03).** The successor's mover key IS its funding key, and it
  is the content-addressed key of a request PB seals over the filed
  materialization GENERATION (`_seal_output_mover`, `log_name` plus
  `params.produced_output_materialization`). It is deterministic on replay and
  cannot be supplied as a nonce. The old terminal key and its fence stay
  terminal and spent. At most ONE live or pending materialization exists per
  logical batch (`_live_materialization`), and a malformed materialization
  list is unknown state that raises rather than reading as empty.
* **Legal history, checked whole (PO-05).** Per-row shape is not enough, because the
  dangerous histories are the internally inconsistent ones: `[gen1 live, gen2
  retired]` passes every row check, yet reading the latest row alone would
  answer "retired" and authorize a reclaim over a live earlier mover. So
  `_materializations` is THE validator -- one place every caller reads
  through, including `_active_materialization`, `_batch_stage_retired`, the
  censuses and the retain paths. A legal history has generations 1..N in
  order, every generation before the last retired, the batch's own first copy
  retired whenever any successor exists, and no repeated mover key (the
  batch's own spent key included). None of those are reachable transitions;
  each is corruption, and corruption fails RETAIN everywhere rather than
  reading as ordinary staged state.
* **Origin proof (PO-04).** `commit_batch` captures each origin's identity tuple in
  the immutable batch record (`origin_identity`, one `os.lstat` feeding both
  the size check and the record, through `reader_lease.portable_identity`),
  and every re-materialization rechecks exactly that with
  `reader_lease.file_id_matches` BEFORE any funding. An lstat size is not the
  proof: a DEV null-digest descriptor carries no payload digest, so a
  rewritten file of identical length would pass a size check and is refused
  here as `restage-origin-changed`. A batch filed before the field existed has
  no proof and refuses `restage-origin-proof-missing`; its current bytes are
  never retroactively blessed. Nothing is rehashed -- the writer digest, where
  one exists, still rides the manifest and the mover verifies it on copy.
* **Crash safety (PO-05).** The materialization intent is filed under the
  output-prefix ownership lock BEFORE any funding or movement side effect
  reaches the pool, so a crash at any prefix leaves a durable resumption point
  naming the sealed key. A restart re-calls `ensure_batch_materialized`, which
  re-drives that exact row -- never a fresh generation, never a second funding
  record, never a second credit. It deliberately does NOT re-seal on resume: a
  tier record that drifted between crash and restart would otherwise derive a
  different key for work already funded under the first.
* **Locks (PO-07).** `ensure_batch_materialized` holds the output-prefix ownership
  lock for the intent and for the final flag only, never across
  `stage_output_intent`/`fund_output_batch` (owner -> mover transition locks)
  and never across the reconcile's drive. `retire_batch` was changed the same
  way: it selects and captures under the lock, releases it, runs
  `stage_release.evict` (mover transition lock, then the STAGE ROOT's
  ownership lock, then containment reclamation below that) with no lock of
  this lane held, then reacquires and revalidates the exact
  manifest/mover/generation before filing `retired`. Holding an output-prefix
  ownership lock across the egress nests two locks of one family with a
  blocking transition wait between them; nothing needs it, because the
  materialization stays unretired for the whole window and therefore keeps
  refusing both a second writer over its origins and any successor.
* **Which copy is current (PO-05, PO-07).** `retire_batch`, `recover_batches`,
  `due_mover_rows`, `output_scope_tick`, `safe_release_instance`,
  `_live_output_paths` and `_live_path_owner` all read the ACTIVE
  materialization (`_active_materialization`), never the entry's own
  `retired` flag, which describes only the first copy. An intent whose mover
  row was never published reports `output-materialization-intent-pending`
  with the route back (`ensure-batch-materialized`) instead of being lost.
  `PoolQueue._output_batch_authority` authorizes the successor's claim cover
  through the same single live row, and only with the batch's own tier.
* **Charge (PO-01, PO-06).** Unchanged across forward and reverse staging: the class sums a
  batch contributes are fixed at commit, each materialization spends exactly
  `ceil(batch bytes / GiB)` of window credit and returns it at its own
  retirement, an ACTIVE pin blocks retirement and therefore the successor, and
  `reclaim_origin` stays actual-absence-only -- a reclaimed batch can never be
  restaged (`origin-reclaimed-no-restage`).

#### Terminal occupancy: a batch stays charged until its bytes are gone

The tier invariant is that held tokens cover the bytes on the stage at every
instant, so a produced-output mover's terminal releases its tier tokens only
when the stage is proven empty of its material. `PoolQueue.residency_pin_holds`
answers the complete case (whole declared range, unrefused receipt);
`PoolQueue.output_partial_pin_holds` answers the rest for this lane, and
`PoolQueue.pin_holds_tier_tokens` is the union that `finish`, `reap_stale`,
the tombstone/widowed-lease sweeps and `reclaim_terminal_reservation` all ask,
so no two concluding paths can disagree.

The contract is three-valued, never two:

* **Occupied — retain.** A receipt naming the tier with a positive
  `bytes_staged`, or a published residency fragment naming this mover. A
  partial batch is occupancy: half a batch on the stage is half a stage spent.
  An overrun is occupancy too -- it refused for staging MORE than it declared.
* **Proven empty — release.** A mover that filed a refusal receipt
  (`residency_moved_nothing`, or `origin_unreachable` for a window whose
  origin directories are not on the tier host) naming this tier, reporting
  `bytes_staged` as EXACT non-boolean integer zero, and published no
  fragment. All three, as a conjunction. A zero-output failure frees its
  reservation because nothing is occupying anything.
* **Unknown — retain.** Anything else, including a MISSING move receipt and a
  malformed count. `stage_move` publishes a fragment per entry as the bytes
  land and calls `record_move` once, last, so a kill in that window leaves
  real bytes and no receipt at all. Absence of a receipt is silence, not a
  report of zero; a negative count or a `bool` (`isinstance(False, int)` is
  True) is broken metadata, which is not a measurement of an empty stage.

The tier-token half of a terminal may also be asked when NO terminal record
exists at all -- a finish tombstone whose finisher died, a lease widowed by a
record that is gone. `_filed_pin_holds` answers that case, and no ending at
all is the strongest form of "this cleanup cannot see the ending", never proof
that nothing is pinned: it retains when the key still holds a prepaid-output
funding record that is not `released`. A mover outside this lane has no such
record and is swept exactly as before.

Retention is safe here only because the leftovers have a named owner:
`produced_output.retire_batch` runs `stage_release.evict` for that mover key,
which deletes the files and returns the holder's tokens in the same call --
exactly once, with a re-drive answering `duplicate` and returning nothing
further. The consumer window's twin (#627: a failed mover's partials, held by
nobody) has no such owner and keeps the tier loop's eviction-candidate sweep
instead; nothing in this contract changes it.

Where that egress runs follows the tier record (#801). Only the tier host
mounts the stage read-write -- the GPU hosts mount it read-only -- so
`retire_batch` runs `stage_release.evict` in its own process only when the
announced tier `host` is the box it is on. Anywhere else it seals the egress
node `pbrun` seals for a consumer's staged range (`stage_release.py` off the
tier record, one CPU and one GiB, no tier demand, no residency block, no data
manifest), publishes it placed on the tier host with `recompute`, and answers
`egress-incomplete` with `deferred_own: ["own-egress-in-flight"]`. The owner
re-drives the call exactly as it does for `own-copy-in-flight`; the call that
finds the action ended reads the receipt the action filed under its own key and
files the retirement. The action is sealed with the roots the mover was sealed
with -- the tier's announced `mountpoint` and the produced-output fragment root
under the pool -- and never with the calling process's spelling of them: on the
tier host a wrong stage root reads as an unregistered stage, and a wrong
fragment root reads as "nothing staged", which is a complete receipt that
deleted nothing. A retirement whose own arguments name other roots is refused
`unknown-retain` before anything is published. The egress key is
content-addressed over the whole sealed action: the command naming the
materialization's mover and the batch namespace, the tier's announced
interpreter and tools, the placement tag and the producer's request. Every call
re-derives it and nothing records it; it moves only when one of those facts
does, and an egress under the earlier key is then an idempotent no-op beside
the new one. A process inside a container reads the container's name as its
hostname unless it was given the host's, and then takes this route, which is
the safe direction: the route works from any box, the tier host included.

`stage_release` is a fleet tool, not part of the `prismabuild` package, and a
production owner imports the package from `<generation>/src` with the
generation's `tools/` off its import path. `retire_batch` therefore imports
the tool only on the in-process route, and an owner that cannot import it takes
the tier-host route even on the tier host. The package imports no other fleet
tool by bare name; a test fixture that puts `tools/fleet` on `sys.path` hides
such an import, so the tier-host tests make the owner's calls with
`stage_release` unimportable.

Four behaviours differ from the in-process route, all deliberate. The staged
paths are filed on the still-live copy before the action is published, because
the fragment is gone by the call that files the retirement. The cause of a
refused egress reaches the owner late: a retry-safe failure returns the row to
`READY`, so a re-drive between attempts is answered `own-egress-in-flight`, and
only when the row's three attempts have ended is the call answered with the
receipt the last attempt filed -- a live pin as `live_pins`, never as a
deferral -- while the same key is published again for the next re-drive. That
republication is automatic, so it passes `refuse_withdrawn`: an operator's
withdrawal of the egress stands, and the call answers `egress-withdrawn` with
no deferral for a poller to wait on. A complete receipt is read whatever state
the row is in, because the tool files its receipt before it exits and `finish`
removes the claim before it writes the terminal record. A mover still `READY`
or `CLAIMED` is answered `own-copy-in-flight` before any egress is published. A
retirement the owner abandons leaves its egress action queued: if that action
later completes, the next `retire_batch` files it, and a consumer's read before
that finds no fragment and refuses. Publishing the
action needs the producer's claimed row for its launch context, as publishing a
mover does, so a call made after the owner's claim has ended can file an egress
that already completed and cannot start one.

`produced_output.recover_batches` is the read-only census over the same
evidence, and it may not turn unproven into a verdict. Fragments prove bytes
landed, not that the batch landed, so `output-batch-staged` requires the
mover's receipt to say `complete`; fragments with an unreadable receipt are
`output-recovery-unknown`. A READY row is judged only by its filed funding
record through `PoolQueue.output_funding_file_state` (a census path may not
use `read_output_funding`, which conflates absent with corrupt): `consumed` is
spent and yields `output-mover-unfundable-retire` with the terminal route
(retire -> reclaim -> re-plan), `corrupt` and a `released` record beside an
unretired batch are unknown, and `absent` means no prepaid intent was filed so
the ordinary claim path applies. A CLAIMED row is never told to retire; its
recovery is the lease reaper's. Ledger holdings are not an input in either
direction.

A spent fence is spent in BOTH queue states. A terminal FAILED mover whose
funding reads `consumed` can no more be retried than a requeued READY one can
be claimed -- `output_funded_cover` covers only `transferring`, and
`publish_prepaid_batch` answers a committed batch with `duplicate`, funding
nothing -- so that branch reports the same terminal route rather than
`output-mover-failed-retry`, and `due_mover_rows` emits no row for it. One
shared question (`_output_funding_verdict`) decides both, because a retry
event pointing at permanently unclaimable work is the same wait-forever
defect as a live-wait one.

### Failed output claims retain their real terminal disposition (#848)

An executed produced-output mover has consumed a one-claim funding fence.
If that attempt fails, remaining `max_attempts` cannot make the same fence
claimable again. Before publishing its immutable attempt, `archive_attempt`
compares the validated consumed funding record with the durable claim's
`tier_funding` output generation and exact token set, its publication, residency,
and produced-output reference (including the CAS request when filed). A positive
match files `FAILED` with `output_retry_stop` evidence even before the attempt
budget is exhausted. The sealed request, budget, failure status, logs, and actual
attempt count are unchanged. The immutable evidence is validated by attempt
adoption without rereading mutable funding, so later retirement cannot change
history. Both normal finish and stale-lease recovery use this archive contract.
A consumed claim whose lease later disappears is not an unstarted release:
its durable funding proof establishes that claim and lease publication already
completed. It follows the same failed-attempt path after the existing grace.
Late finish still cannot alter a live successor's queue row or reservations.

Absent, corrupt, changed-generation, or otherwise mismatched funding supplies
no such proof and keeps the existing retry and fail-closed admission behavior.
Ordinary work and unspent funding keep their retry contracts. Successful output
claims remain successful. This transition releases no material or funding;
partial or unknown material retains the existing occupancy pins, including when
a reaper files the terminal failure. Recovery of the committed output still uses
the existing retire, reclaim, and re-plan lifecycle, never a second spend of the
failed claim's funding. Already archived READY retries are not rewritten by this
change and still require an explicit supported operational disposition.


### The window

A campaign stage reads several times the size of the stage, so "admitted when
every lead is executed" cannot hold for the whole read set. The consumer depends
on its **first phase only**; the `tiers` loop publishes every phase -- the first
included -- as the consumer's accepted progress advances, and egress rows for the
phases it has read past.

The submitter publishes the consumer row and no mover. It used to publish the
first phase itself, and that one row was enough to hide a range the fleet already
had: the loop adopts before it publishes, but its adoption pass must skip any leg
whose row already exists, because a `ready` or `claimed` key may be a copy in
flight. The first phase was therefore the only one that could never be taken
over. With a single publisher there is also no interleaving between two of them
to arbitrate. The cost is that a cold first phase waits for the next cycle rather
than being queued at submission. Staging for phase k+N overlaps compute on phase k, and the stage never
overfills.

Membership is frozen before anything is published and publication is what is
deferred. An action key is `canonical_sha256` of an action body, so a mover's key
is not something a loop may derive: `pbrun --residency stage` seals every
movement and egress node up front and writes their whole queue rows into a
first-writer plan under `residency-plans/`. A restart republishes those same
children rather than cutting a new partition of the read order — the decomposer's
transaction — and the coordinator that publishes them is the `tiers` role, which
already holds the queue and the tier ledger every cycle. How many are in flight
is bounded by the tier's free tokens and by the consumer's own progress, never
by a number.

### Mover run-ahead is bounded by the consumer, not only by the tier (#632)

Free capacity is the only brake that acts on the **admit** side. The release
side is driven by the consumer's accepted progress, so a consumer that
publishes none never advances the window, never gets an egress row published,
and stages its plan up to the last phase that fits. On 2026-09-18 the
GLM-5.3-Flash run `ad8803aa` did exactly that: a well-formed plan of 46 phases
and 3.60 TB against a 744 GB stage, **19 movers done and 1 egress done**, each
phase 81–134 GB, `prismabuild-stage/prewarm` at 0 B available. The #628
ownership marker is rewritten under that root and needs one block; it could not
get one, so from then on every sweep and every egress refused with
`stage_root_unregistered` (#631). A stall is recoverable and legible; a full
dataset that cannot write its own marker is not.

So `residency_plan.window` bounds **run-ahead**: the tokens the window holds
for phases *strictly after* the one the consumer is reading. The phase it is
inside is the work, not run-ahead. The bound is in tier tokens — the quantity
that overfills — and is read off the plan and the ledger rather than picked:

* `step` is the largest `stage_gib` still ahead of the consumer. It is the
  same quantity `window_pressure` calls "what the tier must be able to offer",
  and the largest rather than the next so the answer does not depend on which
  phase happens to come first in a plan whose phases differ by 65%.
* A consumer that has accepted **nothing** gets a run-ahead budget of `step` —
  one phase. `N = 1` is the smallest N for which the window's own overlap
  claim ("staging for phase k+N overlaps compute on phase k") is satisfiable,
  and a consumer that has published nothing has given no evidence it consumes
  at all, so a deeper prefetch is speculation on a rate nobody has measured.
  The incident's shape then stalls at two phases of 744 GB rather than
  nineteen.
* A consumer that **has** accepted a phase is rolling, and its bound is the
  tier rather than the plan: `capacity − step`. Free capacity still binds
  first whenever it is smaller, exactly as before, and `capacity` is the
  ledger's minted total rather than its free remainder — a bound read off what
  is free would shrink as the window it bounds fills it. Since #903 a rolling
  window is also bounded by its consumer's **refill horizon** (see
  "A window stages only to its consumer's refill horizon" below), which is
  what keeps `capacity − step` from being spent on speculation.

The second bound covers run-ahead only. Total occupancy is the phases awaiting
egress, plus the phase being read, plus run-ahead, and the first two are
bounded by free capacity as they always were — so a rolling window can still
reach the tier's last token between a mover landing and its egress running.
What the bound removes is the *unrelieved* growth: a window whose release side
has published nothing can no longer spend the tier, which is the state that
reached 0 B and stayed there.

A consumer that reported some phases and then went quiet gets the second bound
and stalls there, not the first. Telling "quiet" from "slow" needs a clock, and
a clock is what #598 took out of this subsystem: an orphan is evicted when the
tier needs its tokens, never because a clock said so. The only progress-free
fact available without one is whether the consumer has ever accepted anything,
so that is the fact the two regimes turn on.

**The stall is reported.** `window` returns a `stall` descriptor — the consumer,
the phase it is reading, the phase declined and its size, the run-ahead held
against the budget, and what it is waiting for — and `residency_window` files it
as a `window-stalled` event beside `mover-published` and `egress-published`. Not
a claim denial: the consumer is not denied, it is running and reporting nothing.
A silent stall would reproduce the incident in the other direction.

**It does not fight #598's deferred eviction.** `window_pressure` now asks the
window what it *would publish given room*, rather than reading the first phase
the consumer has not staged. A phase the run-ahead bound has declined is not
the tier needing tokens, and reporting it as pressure would evict a resident
range to make room nobody is going to use — a stall no eviction can relieve.

**A feasible newcomer's admission is also pressure (#orphan-pressure).** The
next-phase term alone answers "one phase", while the joint-fit gate admits a
newcomer on *current plus protected next* against held and queued bytes. A
window that fits only after orphan reclamation therefore deadlocked beside
reclaimable bytes: the sweep relieved one phase, the gate kept refusing on
cur+next, and nothing re-pressured (2026-09-20, attributable pristine-main
failure `44b15d345804`). `window_pressure` probes each newcomer through
`gate_newcomer` itself. A newcomer has an unpublished lead: adopted later
ranges do not admit its missing head (#829). A newcomer is also not running:
a claimed consumer passed the claim's residency gate on its leads, so its
window is admitted for the rest of its run, even after its first range is
egressed and so reads as unpublished again (#908). Its next range is an
admitted window's advance, fenced and counted in `existing_min_next`, and
never a newcomer's current asking the gate for held + queued + current +
next. An admitted window's own unpublished current is not a next: the gate
counts it for newcomers (as `running_extra`, like a same-pass newcomer's
current) only when it permits the window this pass, and a window gated on
its own fence reserves nothing, so it cannot hold every newcomer out (#881).
Both the pressure probe and
the publication gate use that identity and the next still-unpublished legs,
with conservative obligations (full queued demand and a minimum next-step term
from progressing windows). The relief is stated as the free the sweep must reach
(`free + shortfall`) and is bounded to what the tier can give back: its
orphans, and since #903 the landed ranges past their readers' refill
horizons. A window that cannot fit even after all of that returns —
permanently oversize, or blocked by live readers — adds no admission-relief
term. The ordinary next-phase
pressure remains independent, and the sweep still takes only eligible orphans.
The real gate re-checks
everything before publishing; the probe only decides whether the room is
worth reclaiming.

**A ready consumer's claim is also pressure (#901).** Once a ready consumer's
leads hold their tokens, the claim's residency gate passes, and the next thing
that refuses the claim is its own claim-time tier demand, such as a
produced-output window. The claim takes that demand from free and checks
nothing else. Without this term, a withdrawn consumer's landed movers kept the
room while nothing asked the sweep for it. On 2026-09-22, GLM Stage A R12
(`683cb3caa5ea`) waited about 25 minutes in `ready/` with
`tier_reservation_unavailable` (available 35, requested 48). Meanwhile, 22
landed movers of the withdrawn R11 held 484 GiB, and an operator had to egress
them by hand. `window_pressure` now probes each such claim through the same
`gate_newcomer` path as a newcomer. The claim is a final window of one step,
with only the obligation the claim gate checks: what is held. The relief is
`free + shortfall`, bounded to the tier's orphans (and, since #903, the
landed ranges past their readers' refill horizons), and the sweep evicts
oldest first until the claim fits. A claim that cannot fit even after every orphan
returns, or that exceeds the tier, asks for nothing (#632). A withdrawn ready
key asks for nothing either (#708). No new eviction rule was needed. The
withdrawn consumer's movers were already orphans under the existing
definition: no ready or claimed item names them. The missing piece was the
pressure that lets the sweep take them.

New window publication processes already-admitted windows before newcomers,
then newcomers by descending consumer priority (#874). If a feasible
higher-priority newcomer fails the existing joint-fit gate transiently, the
same tier pass defers further lower-priority new windows with
`higher-priority-window-waiting`. Already-queued promises drain normally;
the pass neither revokes them nor relaxes credit or advancement fences.
Equal-priority order and backfill remain unchanged. Permanently oversized
windows do not establish this wait, nor do current-plus-next needs exceeding
the tier's entire capacity. The barrier is recomputed each pass, not stored
as a reservation or a change to any frozen plan. This prevents smaller new
windows from indefinitely replenishing ahead of a larger priority lead;
it does not promise progress while existing readers retain capacity.

This priority policy applies only to a positively `READY` consumer in its
current publication (#881). A `CLAIMED` consumer whose original lead has
retired can still satisfy the older unpublished-lead credit-gate predicate;
that does not make it an unstarted admission. Such running windows neither
establish nor receive the priority barrier and remain ahead of new admissions.
The original credit gate, minimum next-step reservation and funding checks
are unchanged. A READY retry remains eligible regardless of historical attempts.

Admission relief applies to both stage and RAM. RAM asks only when its normal
bounded window has a promotion whose stage source is resident, preserving
the existing rule against eviction for an unavailable source or a declined
run-ahead phase. Its full queued-demand estimate can defer
additional relief when some ready rows are already funded. It is not a
complete liveness proof. Earlier acceptance is recorded in
`orphan_pressure_acceptance_2026-09-20.json`; #829's repair has its own scoped
acceptance in `r3_admission_pressure_acceptance_2026-09-21.json`.

The other half of the 2026-09-18 deadlock is the consumer's: the joint run
carries no progress-v1 transport at all, so `accepted_phase` was `None` on
every cycle. That is PrismaQuant's, tracked at `RobTand/prismaquant#741`; this
bound is what keeps it from costing a tier.

Two consequences worth stating, because both were bugs first. A live consumer
protects its **whole plan** from the orphan sweep, not just its leads: a pinned
mover three phases ahead is named by nothing in the queue. And a terminal mover
that holds no tokens counts as unpublished, because the same manifest seals the
same key on a second campaign and a leftover `done` record would otherwise read
as "already staged".

**A withdrawal supersedes the window, and only then can it be repriced (#708).**
An operator's `--withdraw` of a mover is a decision about the plan that sealed
it, not about one row: the row cannot be edited or repriced in place -- its
action key hashes the sealed resources and argv -- and a window that silently
republished the cancelled copy would undo the decision at the price it was
cancelled for (2026-09-19: movers sealed at fill 259 republished against a
measured offer of 65.7, re-wedging the tier the withdrawal was meant to break).
So the coordinator marks the plan superseded, by the plan body's digest **and
the filing's incarnation** (inode, mtime, size), and publishes nothing further
from it: no mover and no promotion, at any price. Egress rows still run --
freeing bytes the consumer has read past is cleanup, not staging.

Three identities keep the marker honest. The digest keys it to the body it
retired; the incarnation keys it to the *filing*, so a deliberate same-body
resubmission after a reap is not covered by the marker of the body it
replaced; and `preempted_by` on the marker separates an operator's decision
from admission's own preemption, which republishes its holder with
`supersedes_withdrawal` in the same breath and must keep its plan. A marker
that cannot be read or parsed is **not** "no marker":
`residency_plan.superseded` answers with an `unreadable` record, and the
window, adoption, pressure probe and planner all refuse or defer on it.

`None` from `superseded` -- "not retired" -- is an assertion about identity,
so it is only allowed on a proof: the stamp must be three whole integers
(JSON integers, not floats or booleans) **and** the current filing's
incarnation must have been successfully read. A missing, wrongly shaped or
non-integer stamp, or a stat of the plan that fails, is *unknown* retirement,
not a different filing, and answers `unreadable` with the reason. The same
distinction reaches `read_filed`: a stat that failed is reported through
`on_unreadable` and never answered as "no plan filed", because a caller about
to seal or reap over unknown state would be guessing.

**The handoff belongs to its callers, not only to its helpers.** The locked
helpers were correct before the callers were: a caller that read a plan by
key and then asked `reap` to archive "whatever is filed" could archive a
replacement it never saw, and a publication outside the consumer's boundary
could land after the plan that authorized it was reaped. Three call sites fix
that by sharing one rule -- capture the filing, then act only on that filing
under the consumer's transition lock:

* `pbrun.residency_stage_rows` captures `(plan, filing)` through
  `read_filed`, passes both to `reap`, and decides again from whatever is
  actually filed when the locked reap removes nothing. A reap that refused is
  never read as "the old filing is gone": it either refuses by name while
  live work remains, or adopts the replacement filing that now stands.
* `pbrun.main` holds the consumer's transition lock across the whole
  ownership transaction -- handoff, seal, `freeze` and the consumer's own
  publication. A dead consumer's cleanup pass rereads an
  old failed or withdrawn terminal every cycle; between a bare `freeze` and
  the consumer's row it would see a filed plan nobody owns and reap it.
* Both automatic window publications (`residency_window` and
  `ram_residency_window`) call `residency_plan.window_owned` under the
  consumer's lock immediately before `publish(..., refuse_withdrawn=True)`:
  the captured filing must still be the filed one and the consumer must still
  be the live, current generation. A cycle snapshot that went stale in
  between publishes nothing and files a
  `mover-publish-deferred-stale-window` (or its ram spelling) event. Egress
  publication stays outside this boundary: freeing bytes the consumer has
  read past is cleanup, not a new child.
* `tier_loop.withdraw_dead_consumer_movers` sweeps one terminal per
  consumer-lock transaction. Its scan observes the terminal and checks for a
  live consumer outside the lock, so both are re-read inside it -- through
  `residency_plan.live_state`, whose uncertain answer defers -- and the plan
  attribution, every child withdrawal and the reap happen there too. Without
  the lock, a pass that read the old terminal could reach the lead the
  window had just published for a fresh resubmission and cancel it; `reap`'s locked
  recheck runs far too late to undo that.

`handoff_safe` reads under the same discipline. It holds the consumer's lock
across the whole scan and each child's transition lock across that child's
two state reads, parent before child -- the one order every writer here keeps
and the same-thread nesting `posix_lock.held` already supports. Two bare
`Path.exists` reads let an atomic READY->CLAIMED claim land between them and
look like a child none of whose states is live; a read that fails is
uncertainty too, and a state that cannot be read answers "defer", never
"absent". Only a complete, current scan proves a handoff safe.

The body stays filed while anything still names it. A withdrawn consumer's
queued or claimed children are still attributable, so
`withdraw_dead_consumer_movers` keeps cancelling them; a running consumer's
other resident ranges stay named by the plan for the orphan sweep and for
adoption, because a handoff must not expose them. The body is archived -- the
reason in its name, the retired marker beside it as evidence -- only once
`residency_plan.handoff_safe` says no consumer and no queued or claimed child
still refers to it, under the consumer's transition lock. `freeze`,
`mark_superseded` and `reap` all take that one lock and recheck the exact
filing inside it, so a stale reaper cannot archive the plan a concurrent
resubmission just sealed, even when the new body is byte-identical. Until the
handoff is safe a resubmission refuses by name rather than replacing the old
plan. Nothing about retirement releases a token: resident ranges keep their
holders, and their bytes are adopted or evicted by the ordinary paths.

**Automatic republication cannot retire a cancellation.** The cycle's
withdrawn-key snapshot is a scheduling input, not an exclusion: a cancellation
filed after the snapshot would otherwise be superseded by the very
`publish` that hands the mover out again. So the tier loop's publications pass
`refuse_withdrawn`, and `publish` checks for a live marker *inside its own
transition lock* before it retires anything: the cancellation either wins
outright (the publication refuses, the plan is marked) or loses outright (the
withdrawal runs after and cancels the fresh row). Explicit submissions keep
their own semantics -- re-submitting a key is how a person asks for the work
again, and the marker is retired as evidence. A live marker also skips the
`refuse_if_live` duplicate check described above, for the same reason: the
submission is the replacement the cancellation asked for. A deliberately requested fresh
plan seals its price through the same `pbrun --residency stage` path: the
tier's **current announced offer** caps one copy's measured rate (#909), so
a window sealed after the offer sank is admissible without any sealed row
being rewritten. `residency_plan.freeze` stays first-writer; a superseded
filing must be reaped before its successor can be sealed, and the planner
reaps it itself once the handoff is safe.

**A deliberate seal renews the generation it replaces.** A submission
publishes its consumer and nothing else -- every phase is the window's to
publish, the first included -- so the visible
cancellations a reaped predecessor left on its later children outlive both the
plan and the ownership they were made against: the child keys are content
hashes, and a same-body resubmission -- same consumer, price, tool and ranges
-- seals the same ones. Read as live, they would supersede the fresh plan
before its second phase ever published, which is the same-body half of #708
the filing-identity marker does not cover. So a fresh seal (`pbrun
--residency stage`, never a reused frozen plan) retires those *visible*
markers as evidence: under the consumer's transition lock, after
`residency_plan.handoff_safe` proves no live consumer and no queued or claimed
child still names the old window, and under each child's own lock in the
parent-before-child order every writer here keeps. The immutable decision
under `withdrawn/decisions/` stays, and the visible marker itself is filed
under `withdrawn/superseded/`. For a later child the boundary is *that child's
own locked retirement* in this transaction, not the submission and not the
freeze that follows it: a cancellation filed for the child after its marker is
moved survives, and the window's next cycle reads it as live -- it refuses to
publish the child and marks the fresh plan superseded. The first lead is a
child like any other here: `child_keys` names every phase, so its marker is
retired under its own transition lock in this same pass, on the same boundary
and with no special case. The renewal never teaches the automatic publisher to ignore a
marker: the window's `refuse_withdrawn` publications and its supersession pass
are unchanged, and only the deliberate submission retires one.

An operator's `--withdraw` of a historical child whose new queue row does not
exist yet addresses that child's *old* generation: the verb re-affirms the
durable decision but writes no new visible marker, so it does not stop future
staging. To cancel future staging, withdraw the **live consumer** -- whose
withdrawal marks the plan superseded -- or withdraw the child once its new row
is queued.

**The pin lives on the row, not only in the sealed body.** `residency_pin_holds`
reads the *queue record* of a concluding mover to decide whether its tier tokens
stay held, so a mover row that reaches the queue without a residency block --
`publish` refuses tier demand with no block, so only a record the pool never
wrote can still arrive shaped that way -- stages its range
and hands the tokens straight back: the mover ends `executed`, the files are on
the stage, the ledger reads its full supply free, and the consumer's gate waits
for a lead that can never read as pinned. Nothing but the ledger can see it.
`pbrun` stamps each `mover_row` with the block naming that phase's range, and
`validate_plan` refuses a frozen plan whose mover row carries no pin or pins a
different range — the last point at which it is cheap, because after it the row
is in the queue.

**Keeping the pin is not only `finish`'s job.** `_release_reservation` takes
`keep_tier`, and three cleanup paths run *after* an ending that already kept its
tokens: `reap_stale`'s terminal-claim branch (a stale `claimed/` view of a
concluded mover — this mount's documented reality), `sweep_finish_tombstones`
(an interrupted finisher), and `sweep_widowed_leases` (a lease whose record is
gone). Each judges `keep_tier` on the *filed* terminal record, not on the copy
it is cleaning up. Releasing there does not lose a token, it loses the
attribution: every path that reclaims stage capacity — an egress node, the
orphan sweep — walks the tier ledger's held keys, so a key released while its
files remain is occupancy nothing can charge to anyone. The ledger reads it
free, the next mover is admitted against capacity already spent, and the stage
ENOSPCs. The window does republish that mover eventually, because
`_mover_state` reads an unpinned, unqueued mover as unpublished, so the
consumer is not stuck for ever — it pays a second full copy of the range, and
the over-admission happens first.

**The claim races conclude selectively too.** `_claim` drops a committed claim
at two post-acquisition points — a terminal for the same generation already
filed when the row reached `ready` again, and a withdrawal that landed between
the ready scan and the rename — and both used to release the tier half with the
blanket `release_tier_reservations`. That freed names an outstanding output
intent still owned under the same key: an owner that concluded with a staged,
unfunded batch keeps exactly those names (`output_keep_names_for_owner`), and
the blanket release cannot tell the claim's own acquisition from the intent's
retained occupancy, so the intent went on citing names the ledger read as free.
Both sites now take the same `_release_reservation(key, host=None)` every other
concluding path takes — the host tokens went back on the line above — which
releases the claim's acquisition and keeps the intent's names. An occupied
mover never reaches either site: occupancy requires the mover to have run,
running consumes its funding, and a consumed record covers nothing, so the R6
output claim gate reads the row as REQUIRED and refuses
`output_funding_terminal` before `ledger.begin_acquire` is ever called; cutting
that gate is measured to open the claim again (fault-injection RED, 2026-09-20).
`release_tier_reservations` remains only behind `pin_holds_tier_tokens` in
`reclaim_terminal_reservation`, where an explicit `unpin=True` already names
the release.

### A window stages only to its consumer's refill horizon (#903)

The #632 bound keeps the stage from reaching 0 B; it does not ask how far
ahead a consumer needs its bytes. On 2026-09-22 GLM Stage A R12
(`683cb3caa5ea`) published 22 movers in one cycle, 69 s after its claim,
under a budget of `565 − 22 = 543` GiB. At 22:30Z it was reading `chain-043`
while 24 landed ranges held 528 GiB of the 565 GiB stage, the farthest
(`chain-019`) about 24 phases ahead. Two of its later movers and the native
capture's 3 GiB lead waited on `tier_reservation_unavailable`. At 23:03Z the
capture (`a92f62783e8f`) was admitted, read `head` to `layer-2`, reported
`layer-3`, and waited 300 s for a 14 GiB range the window never published
before PB stopped it. None of R12's ranges could be evicted for it: every one
belonged to a live plan, so none was an orphan.

Rob: *"The time a spark spends processing should be used to refill the
ramdisk and ssds."* A window now publishes only to its consumer's refill
horizon, and a range already landed past a horizon is room another window can
take.

**The horizon.** `residency_plan.refill_horizon` measures it in the plan's
read order, which is its byte order (`validate_plan` requires each phase to
start where the previous one ended). It has three spans:

* The phase the consumer's accepted progress names, which it is reading.
* The consumer's read-ahead: `mem_gb` plus its admission's
  `gpu_memory_budget_bytes`, the most it can hold ahead of what it reads.
  The two are summed even where they share one physical pool (GB10 unified
  memory), which over-states the reach, so the horizon errs long.
* The refill: ranges past that reach until they cover what the consumer
  reads while a copy published now lands, and never less than one range.

The refill time is one heartbeat (`pool.HEARTBEAT_S`, 30 s, how stale an
accepted phase can be) plus one tier-loop cycle (`--interval-s`) plus the
landing time: the largest range still ahead at the slowest rate a copy of
this plan has landed at (`bytes_staged / seconds` from its receipts). It is
priced by throughput rather than by a receipt's duration, so a plan whose
landed copies were small ranges does not under-price its large ones. A
range copied again replaces its receipt, and the tier loop reads the new one,
so a range is always priced at its latest copy. A remembered first rate would
price a slower second copy too fast and make the horizon short. The
consumption rate is the bytes through the end of the accepted phase over the
time from the claim to that phase's report. Counting the whole accepted phase
as read over-states the rate, which errs toward a longer horizon.

Before anything is measured, the plan's own numbers stand in (#909). Until a
copy of the plan lands, the landing rate is the smallest fill any of its
movers was sealed with; on 2026-09-22 R12's 24 copies, all sealed at
144 MB/s, landed at 134 to 626 MB/s (median 154). The consumption rate is
the larger of the measured rate and the read rate the plan declares
(`reader.read_mb_s`); before the consumer reports, the declared rate alone.
With neither, or with no accepted progress, there is no horizon, and the
window keeps its #633 run-ahead bound and the #632 regime (one step). The
tier's announced fill supply stood in for both rates until #909, and it
moves as the loop probes the pool; see "An unmeasured reader or mover is
priced from a declared or measured rate" below.

For R12 at 22:30Z: 81.2 GB read in 3919 s (20.7 MB/s), 180 GiB of read-ahead
reaching to 274.5 GB (`chain-042` to `chain-034`), a 174 s landing for a
23.4 GB range at 134 MB/s, so 4.3 GB of refill with the live 5 s cycle
(5.5 GB at the parser's 60 s default, which the tests use), and one range,
`chain-033`, either way. The horizon ends where `chain-032` starts. The refill term is one range for
any consumer whose rate is small beside its ranges; the read-ahead term is
what sets R12's horizon.

**What the horizon bounds.** Every decision that asks "what will this window
publish" asks it with the horizon:

* `window` publishes no range of a later phase that starts at or past the
  horizon. That is the normal state of a rolling window, not a stall, and
  files no `window-stalled`.
* `advance_needs` lists no such range in `waiting`, so a window's advance
  asks the gate for no range it will not publish, and its fence protects the
  range after the frontier only when that range is inside the horizon.
* `window_pressure` counts no such range as pressure, whether as a probe or
  as a row queued before the horizon existed.

**Ranges past a horizon are room.** Staging ahead is choice (b) of the two
the issue named: ranges land ahead of need and stay resident as a cache
until another window needs the room, rather than being published only as
the consumer advances (a). A landed range costs nothing while the tier has
room, and Rob's rule is to use the hardware: *"As long as we are maximally
using the hardware, I am OK with latency incurred by waiting for IO."* New
publication stops at the horizon, so (b) only ever applies to ranges landed
before the horizon moved or existed.

`evict_beyond_horizon` runs after the orphan sweep, on the same pressure.
When a stage tier is still short of the free a live window needs — a running
consumer's in-horizon range, a newcomer's lead, a ready consumer's claim —
it gives back landed ranges past their readers' horizons:

* Farthest-needed first (Belady's order), in seconds at each reader's own
  consumption rate, so two readers' ranges compare in one unit.
* One at a time, re-reading the ledger after each, stopping at the room.
* Never a range inside any reader's horizon, nor the advance (the first
  range past it), nor a range whose mover or egress is queued or running,
  nor a superseded or withdrawn window's (the orphan sweep and adoption own
  those).
* Not at all when every candidate together could not make the room (#632:
  no futile eviction). The event is `beyond-horizon-eviction-futile`.

Each eviction is all or nothing: `stage_release.evict(..., whole=True)`
judges every entry under the ownership lock before the first unlink. A range
a reader has pinned, that a promotion is reading, or whose ownership the
pass cannot prove is declined whole — nothing unlinked, no retiring mark,
tokens and fragment held — and the next candidate goes instead
(`beyond-horizon-eviction-declined`). An egress may delete part of a passed
range and defer the rest; a range a consumer will still read may not be
left looking staged with holes in it.

An evicted range's mover holds no tokens, so it reads as unpublished (the
rule above), and its window publishes it again, whole, on the cycle the
reader's progress brings it back inside the horizon.

The newcomer and claim relief terms (#orphan-pressure, #901) are bounded by
orphans plus these ranges. That is what the 23:03Z capture lacked twice: on
the cycle that saw its `layer-3` report, the would-publish term asked for
14 GiB and the sweep had no orphan to give; after its `head` egress retired
its original lead, the gate re-read it as a newcomer, and the relief was
bounded to orphans, which were zero. Since #908 the second case does not
arise: a claimed consumer is never re-read as a newcomer, and its `layer-3`
is an admitted window's advance, which the would-publish term covers.

**Assumptions and limits.**

* The horizon trusts the consumer to read in plan order and to hold no more
  than its reservations ahead. A consumer that prefetches past its
  reservations can find a range evicted; the PrismaQuant layer reader then
  waits `STAGED_RANGE_WAIT_S` (300 s) for the window to publish it again.
* Since #906 the ram window is bounded by its own horizon as well (next
  section).
* Since #907 admission charges the horizons jointly: a newcomer is admitted
  only when its read footprint fits beside every admitted window's (see
  "Admission charges refill horizons jointly" below).
* A consumer claimed before #903 keeps its landed ranges until another
  window needs the room. Its movers already in `ready/` past its horizon stay
  queued: withdrawing one would retire the whole plan (#708). Such a row
  claims room like any other, so while R12's `chain-021` and `chain-018`
  rows wait, room freed for someone else can be taken by them first; they
  then land past R12's horizon and are candidates again. The joint gate's
  relief counts queued rows, so a newcomer's relief covers them in one cycle.

The replay in
`tests/test_r12_and_the_capture_replay_under_the_refill_horizon.py` runs
the tier loop's cycle on R12's and the capture's live byte ranges, copy rates
and claim times (`tests/fixtures/r12_stage_20260922.json`). One cycle gives
back `chain-019` for the capture's 3 GiB lead (R12 keeps 506 of 528 GiB);
on the cycle that sees the capture's `layer-3` report, `chain-019` goes and
`layer-3` publishes; after the capture's lead retires, `chain-019` alone goes
and `layer-3` publishes. Before #908 that last case re-read the capture as a
newcomer, whose relief counted R12's two queued rows, and gave back
`chain-022`, `chain-020` and `chain-019` (R12 kept 462 GiB).

### The ram window stages only to its refill horizon too (#906)

#903 bounded the stage window by the refill horizon. The ram window kept its
own bounds only: #633's run-ahead (capacity minus one step) and the ram
policy's optional `prefill_depth`. A consumer with a small reservation could
promote as far ahead as the tmpfs had room, and no other consumer's promotion
could take any of it back, because every promotion it held belonged to a live
plan.

**The ram horizon.** `tier_loop._ram_horizon` is the same
`residency_plan.refill_horizon`, asked of the plan's ram legs: the same
reading phase, read-ahead and consumption rate, and the refill priced at the
slowest complete *promotion* receipt of the plan (`bytes_staged / seconds`
from `ram_promote`'s receipt). Every decision that asks what the ram window
will publish asks it with this horizon: `_ram_window_state`'s `window`, both
ram passes of `_protect_tier_advances`, and the ram probe and `advance_needs`
in `window_pressure`. `prefill_depth` still applies as a declared ceiling.

A ram horizon has no stand-in before it is measured. A promotion copies the
stage into the tmpfs, which neither a sealed fill nor the tier's fill supply
measures, so until one of the plan's promotions lands the ram horizon is
undefined and the ram window keeps its #633 bound. Pricing the refill at the
promotion alone, rather than at a stage copy plus a promotion, is safe for a
different reason than on the stage. A short stage horizon makes the consumer
wait. A short ram horizon means the consumer reads a range from the stage
while its ram copy is not there yet: slower, but never a stall.

For R12 on 2026-09-23: 22 complete promotions landed at 231 to 544 MB/s
(median 470), on legs of 9 to 22 GiB. Its read-ahead (180 GiB) is larger than
the 160 GiB tmpfs, so the ram horizon ends past anything the #633 bound
(160 − 22 = 138 GiB of run-ahead) would publish, and R12's ram window is
unchanged. The horizon binds a consumer whose reservation is small beside the
tmpfs, which is the case where another consumer's promotion needs the room.

**Ram ranges past the horizon are room.** `evict_beyond_horizon` now runs on
ram tiers as well as stage tiers, on the same pressure (`window_pressure`'s
ram leg) and the same rules: farthest-needed first, whole or not at all,
never inside a horizon or the advance, and not when the room cannot be made.
The eviction is `stage_release.evict` against the announced ram root. The
newcomer and claim relief terms count these ranges on the ram tier as they
do on the stage.

**A stage range takes its ram copy with it, ram first (#640).** A ram range
whose stage source is gone is one the consumer's map can no longer read:
`overlay_ram` reads a ram entry only beside its stage entry. So a stage range
past its horizon is a candidate only when each promotion over its bytes that
still holds ram tokens is itself a ram candidate (past the ram horizon, not
the ram advance, nothing queued or running on it). Its eviction gives back
those ram copies first. A copy whose eviction is declined (a reader's pin,
say) keeps its stage range too, and the next candidate goes. Ram tiers are
processed before stage tiers, so the tokens of the smaller tier come back
before the bytes that feed it leave, the order the ram egress already keeps.

**The ram window reads its own fence back.** The stage window has always read
the consumer's own advance fence back into free, because that fence is the
room its advance publishes into (#745). `_ram_window_state` took the same
figure as `own_fence_gib` and did not add it. On a tmpfs with room for
exactly the current promotion and its advance, the advance could then never
publish. It now adds it.

### Admission charges refill horizons jointly (#907)

#903 bounded each window by its refill horizon, and admission did not follow.
The joint-fit gate (`window_credit.gate_newcomer`) admits a newcomer when its
*minimum* fits: held + queued + its current + its next. The window then grows
into whatever room is free, up to its horizon. Nothing bounded the *sum* of
the horizons, so two admitted windows could between them want more of the
stage than it has. Ranges inside a horizon are never evicted, so one reader
then waits on the other's reading: a range miss, a 300 s range wait, a stall.
No incident has shown this yet. It is the soundness gap the #903 review named,
and the fixtures in `tests/test_admission_charges_refill_horizons_jointly.py`
reproduce it.

**The read footprint.** `residency_plan.read_footprint` asks a window's own
rule at every phase the consumer still has to read: the stage GiB `window`
publishes from an empty tier with the refill horizon recomputed at that phase,
and the largest of them. It is the most the window will ever hold at once,
not the plan. The #633 run-ahead bound keeps it at or under the tier's
capacity. The horizon at each phase is priced at the consumer's rates now:

* Consumption: for a claimed consumer with accepted progress, the horizon's
  own measurement (bytes through the accepted phase over the time from the
  claim to its report), which is what bounds its window now, or the fastest
  rate its claim has *attained*, whichever is higher. A reader that slowed
  can speed up again, and its window grows back into the room it gave up,
  so the footprint does not follow the rate down past what the reader has
  shown it can do. Attained counts only the phases before the accepted one,
  which the reader has certainly read by its report. The horizon's rate
  counts the whole accepted phase as read, and a first report a few seconds
  after the claim makes that a whole phase over a few seconds: kept for the
  claim's lifetime, that peak would refuse every newcomer beside it. The
  tier loop keeps the attained rate in memory; a restart forgets it and
  prices each claim at its current rate. A declared read rate above the
  measured one raises it (#909). Anything else — a newcomer, a ready
  consumer, a claim with no report — has measured nothing, and its plan's
  declared read rate stands in; with none, the footprint is the #633
  run-ahead bound, named `undeclared`.
* Landing: the slowest complete copy of the plan, else the smallest fill its
  movers were sealed with, as for the horizon.
* Read-ahead: the prefetch depth the plan declares
  (`reader.prefetch_depth_bytes`), else the consumer's reservations
  (`mem_gb` plus its admission's GPU budget), as for the horizon. The census
  names each basis on the window (`consumption_basis`, `readahead_basis`,
  `landing_basis`).

With no consumption or landing rate the horizon is undefined, and the
footprint is what the #633 bound alone lets the window publish.

**The commitment.** `tier_loop._commitment_census` is, per stage tier, what
admission has already promised:

* every held token nothing can evict: held, less the tier's orphans, less each
  window's passed legs and legs past its horizon;
* queued new money: ready rows' tier demand no funding record covers;
* the unheld produced-output windows (`produced_output.unheld_window_gib`);
* each admitted window's **growth**: its read footprint less what it already
  holds toward it (its in-horizon legs held or queued, and its fence grants).

Every token is in exactly one term. A leg is passed, past the horizon or ahead
of it; an orphan is in no live plan; a queued row is new money once, whether
or not a window's holding names it. So an admitted window is committed at the
larger of what it holds and its footprint, and a static holder (a receipt-less
token, an output owner's held window) at what it holds.

A live consumer the census cannot read is not free room. A plan read fails on
a torn write or the mount's quarter-hourly ESTALE (#575); the consumer then
drops out of the pass, and its ranges would count as orphans and its growth
as nothing. So a range whose receipt names a live queue item is never counted
evictable, and a tier that an admitted window may be on uncensused (its plan
or its mover state did not read) refuses its newcomers with
`advance-deferred-unknown-evidence` until a pass reads it. An unreadable plan
blinds the tier its queue item declares, or every tier if the item does not
read either. A consumer that is certainly a newcomer (ready, none of the leads
its item declares published) blinds nothing: it is admitted nowhere, so it
commits nothing, and it waits for its own plan anyway.

`window_credit.gate_commitment` admits a newcomer when the commitment plus its
own growth fits the tier. Otherwise it is refused with `joint-commitment-stall`,
a transient wait for admitted windows to finish, and the `window-gated` event
carries every term under `commitment`. One exception: a newcomer with no other
admitted window, owed output or queued demand on the tier is admitted under
the joint-fit gate alone. What is committed then is holders that never grow,
so the newcomer contends only with itself: its window runs short of its
footprint, as every window did before #907, and a refusal would be one no later
cycle could lift.

**Where it is asked.** A newcomer's formation event is the one admission
point, and it has three spellings:

* The joint-fit gate in `_protect_tier_advances`. The commitment is asked
  unless the joint-fit gate refused for good (`joint-fit-oversize`), and when
  both refuse the reason is `joint-commitment-stall`, because no eviction can
  admit what the commitment refuses. Newcomers are asked in the gate's own
  order, and each admitted one is committed before the next. A commitment
  wait sets the priority barrier as a joint-fit wait does.
* Adoption. Taking a newcomer's lead over from a withdrawn donor admits it,
  and adoption moves tokens rather than acquiring them, so no joint-fit gate
  saw it: a successor over its predecessor's ranges (R13 over R12's) would
  have been admitted without any gate. `adopt_resident_ranges` now asks the
  commitment before a ready consumer's first adoption, and files
  `adoption-deferred` when it refuses. The donor's range stays an orphan.
* The eviction pressure. A newcomer the commitment refuses publishes nothing
  this cycle, so `window_pressure` counts neither its lead nor its admission
  shortfall (#632: no eviction for room nobody will use).

**Owed outputs (#905).** The unheld produced-output windows are charged in the
commitment whether or not `--output-windows` is set: the producer takes that
room back from free, and a newcomer admitted into it would be squeezed by it.
An output census that does not read refuses the tier's newcomers
(`advance-deferred-unknown-evidence`, the error under `commitment`) rather than
counting zero; admitted windows keep publishing. `--output-windows` now decides
only whether the joint-fit gate, the fence check and the relief count the owed
window as well.

**On R12's recorded state** (`tests/fixtures/r12_stage_20260922.json`)
against the 585 GiB stage of 2026-09-23, with the live 5 s cycle:

* R12, claimed at `chain-043`, has a footprint of 242 GiB (measured
  20.7 MB/s, 180 GiB of read-ahead, one 22 GiB refill leg) and a 48 GiB
  output window.
* An R12-shaped newcomer (R13) that declares nothing is priced at its
  run-ahead bound and waits beside R12 (#909). Declaring what R12 does, one
  22 GiB layer of read-ahead at 21 MB/s, its footprint is 88 GiB and it is
  admitted. Until #909 its refill was priced at the announced fill supply:
  220 GiB at 413 MB/s, admitted at 558 GiB.
* A native capture's footprint is its whole 20 GiB plan, and a Stage-B
  quantum's (a 3 GiB head and three 22 GiB layers) its whole 69 GiB.
* R13 alone, at R12's rate, commits 290 GiB and leaves 295: four Stage-B
  quanta or fourteen captures beside it, where the joint-fit gate alone
  admitted any number whose current and next fit.

The last section of the test file replays the same state on the 530 GiB the
stage had for windows on 2026-09-22 (R12's claim reservation and the
receipt-less holder folded out), with the 60 s default cycle. R12 commits
308 GiB: 264 inside its horizon and at its advance, and its two queued rows
past the horizon (44). Its 264 GiB past the horizon is evictable.

* The capture passes the commitment (308 + 20 = 328). The joint-fit gate
  refuses it until the relief gives back ranges past R12's horizon, as
  before #907, and then its lead publishes.
* An undeclared R13 newcomer (its 528 GiB run-ahead bound) is refused.
  Before #907 the relief gave back R12's farthest ranges for its lead; now
  nothing is evicted for it.
* After the relief took R12's ranges past its horizon, four Stage-B quanta
  all fit the joint-fit gate (264 + 44 + 4 × 25 = 408). The commitment
  admits three (515) and the fourth waits (584).

Until #909 a newcomer's footprint moved with the announced fill supply, which
priced its consumption until it reported: R13 was 242 GiB and refused at
413 MB/s, and 176 GiB and admitted at 144 (PB `ff200b3dacf1` on main
`81d95cba`). It no longer reads the supply.

**Assumptions and limits.**

* Sound only while what it counts as evictable comes back without another
  reader's progress: reader pins release independently of progress, egress
  rows run, and no mover key is in two live plans.
* A newcomer's read-ahead is its host reservation only. The GPU budget is set
  at claim and enters its footprint from then on, so a newcomer admitted
  beside it before then was not charged for it. R13 above is charged 220 GiB
  at admission and needs 242 at R12's measured rate: 22 GiB admitted
  uncharged. A declared prefetch depth has no such gap: it is the same
  before and after the claim.
* An undeclared read-ahead is the reservation. R12's 180 GiB over-states a
  one-phase (22 GiB) lookahead by about 150 GiB. In the horizon that error
  only cached more; here it refuses work that would fit. A consumer that
  declares its depth is priced at it (#909).
* A consumer whose state does not read publishes nothing while it stays
  unknown and is not counted.
* A leg left past the horizon that no eviction took becomes the window's
  advance, which is not charged until it is inside: at most one leg.
* A window's rows queued past its horizon (R12's `chain-021` and
  `chain-018`, published before #903) are committed as queued new money
  until they are withdrawn or land, although once landed they are evictable.
  This errs toward refusing.
* Footprints are recomputed each pass. If admitted windows later want more
  than the tier (a claim measured faster than its stand-in), they keep
  running and contend as before; newcomers are refused until the sum fits.
  Since #930 the tier loop says `tier-over-committed` on every cycle while
  it lasts, with the terms.
* An admitted window that the joint-fit gate has physically stalled still
  holds its footprint in the commitment. Beside a holder nothing can evict, a
  tier can be over-committed by that window alone, and then a newcomer that
  would fit the free room waits behind a window that cannot progress either,
  until the holder goes. Before #930 nothing reported the overcommit, so
  this looked like a stall to an operator: the newcomer's `window-gated`
  event carried only the totals (`committed_gib` above `capacity_gib`). Now
  the tier says `tier-over-committed`, and the refusal names the holder.
* The horizon's own in-phase over-estimate is real window behavior: a claim
  whose first report lands seconds after it has a window that runs to the
  #633 bound until its next report. The footprint follows it while it lasts,
  so newcomers wait out that interval.
* The eviction pressure asks the commitment in the gate's priority order but
  not behind the gate's priority barrier. With mixed priorities, a
  lower-priority newcomer that the barrier holds back while the commitment
  admits it can still be given relief it does not use that cycle. Uniform
  priorities never raise the barrier.
* The census reads the ready and claimed items, every live window's movers on
  the tier and the output census, on each pass that asks it (at most three a
  cycle, and only when a newcomer is present). Its cost is measured since #909
  (see below): about 20 ms a call from sparky over NFS.
* Stage tiers only. A ram miss is a read from the stage, slower but never a
  stall (#906), and the ram leg keeps the joint-fit gate alone.

### An unmeasured reader or mover is priced from a declared or measured rate (#909)

Three numbers stood in for measurements nothing had taken yet, and two of them
were the tier's whole fill offer:

* a newcomer's consumption, and its landing when its rows were sealed with no
  fill, was the tier's announced fill supply;
* a mover was sealed at the largest single-reader share any receipt on the
  tier had priced (440 MB/s live on 2026-09-23), capped by the offer
  (428 MB/s), so every seal was the whole offer and each copy ran alone;
* a consumer's read-ahead was its memory reservation (R12: 180 GiB, where it
  holds about one 22 GiB layer).

The supply moves as the loop probes the pool, so the same queue admitted a
newcomer on one cycle and refused it on the next: on main at `81d95cba` an
R12-shaped R13 beside R12 was refused at 413 MB/s (242 GiB) and admitted at
144 (176 GiB), PB `ff200b3dacf1`.

**A consumer declares its reading.** `pbrun --residency-prefetch-depth-gib`
and `--residency-read-mb-s` seal a `reader` block on the residency plan
(`prefetch_depth_bytes`, `read_mb_s`), separate from the memory reservation.
A deferred submission (#913) carries both as optional publication options, so
a record filed before #909 still releases, undeclared. A logical campaign
declares them once for every child, as the optional `prefetch_depth_gib` and
`read_mb_s` fields of its `task_data_manifest` policy; a policy without them
freezes the same parent identity as before. The tier loop prices:

| Term | First of | Basis names |
|---|---|---|
| Consumption | the larger of the measured and the declared rate; else the declared rate | `measured`, `declared`, `undeclared` |
| Read-ahead | the declared depth; else the memory reservation | `declared`, `memory-reservation` |
| Landing | the plan's slowest complete copy; else its smallest sealed fill | `measured`, `sealed`, `none` |

Both consumption terms are lower bounds on how fast the consumer reads, and a
faster rate only lengthens the horizon and the footprint, so the larger wins.
A consumer that declares nothing and has measured nothing has no horizon: its
footprint is the #633 run-ahead bound, which refuses more than any rate would
(an undeclared R13 beside R12: 528 GiB, refused). The submitter lifts it by
declaring. An over-reaching declaration costs speed, not correctness: a range
the window did not stage is read from the pool, and a range under a live
reader lease is never evicted. `residency_plan.refill_horizon` no longer
takes a fill supply at all.

**A mover is sealed at one copy's measured rate.** A seal is three things at
once: the fill tokens the copy holds, so the offer over the seal is how many
copies run together; the rate `_fell_short` holds the copy's own delivery
against, which reads as saturation only when the seal is one copy's worth; and
the landing rate the horizon assumes until the plan's first copy lands.
`storage_tiers.mover_fill_price` seals at the first of:

* `landing`: the slowest landing (`bytes_staged / seconds`) among the copies
  of the same manifest onto the tier in its latest window -- the consumer
  whose last receipt is newest -- each copy at its latest complete receipt
  that read the pool. The latest window, never every window: receipts are
  append-only, and the slowest copy ever measured would ratchet each
  generation's seal below the last (R11's slowest copy of R12's manifest was
  77 MB/s, R12's 116);
* `single-reader-share`: the median over the tier's pool-reading receipts of
  one reader's share, the same "one reader's worth" the fold's probe grows
  by. The median, not the maximum the seal used before;
* `none`: the tier's offer is then the stated bound (`tier-offer`), and one
  copy runs at a time.

`current_fill_offer` still caps the seal at the tier's current offer (#708).
pbrun and the produced-output restage seal through the same rule, and the plan's
`demand_source.fill_measured` names the statistic, its basis and the window
it read. On the live receipts of 2026-09-23 an R13 on R12's manifest is
sealed at 116 MB/s where it was sealed at 428, and a first submission of an
unseen manifest at 59 (the median over 965 pool-reading receipts; the
latest 200 have a median of 138). The fold bounds the concurrency a smaller
seal buys: a copy that falls short of its seal sets a ceiling, and the tier
mints no more than the ceiling plus one reader's worth.

**Cost.** Each cycle that builds the commitment census reports what it cost:
`{"event": "commitment-census", "calls", "elapsed_s", "max_s"}` on the tier
loop's output. Since #930 every cycle builds at least one. Measured on 2026-09-23 from sparky over NFS (the loop itself
reads the queue locally on dl380g10), 25 warm calls per tree, as PB actions:

| Queue | Before (`81d95cba`) median / p90 | After median / p90 |
|---|---|---|
| Live, no live window | 23.1 / 28.9 ms | 23.5 / 26.8 ms |
| #907 replay: R12, R13, the capture, three quanta | 20.2 / 20.4 ms | 18.3 / 18.6 ms |

The profile splits a loaded call about evenly between `read_footprint` (six
calls) and the ledger's token-directory scans (`holder_tokens`,
`_mover_state`). An undeclared newcomer's footprint is the run-ahead bound,
which recomputes no horizon, so the after-tree is slightly cheaper.

### A joint-commitment wait and an over-committed tier are reported (#930)

The #907 commitment refused a newcomer with one `window-gated` event of
totals, and a stage tier whose admitted windows were promised more than it
has said nothing at all. Both waits were silent: an operator saw a newcomer
waiting and could not tell which holder or window it waited on, or whether
any of them could be evicted.

**The census names what it sums.** `_commitment_census` classifies every held
token once, beside the sums admission reads, which do not change. Each holder
carries a `basis` and, where a window owns it, that window (`consumer`):

| Basis | Evictable | What it is |
|---|---|---|
| `in-horizon-leg` | no | a window's leg inside its refill horizon |
| `fence-grant` | no | a window's advance fence |
| `passed-leg` | yes | a leg of a phase the window has read past |
| `beyond-horizon` | yes | a leg past the window's refill horizon |
| `superseded-plan` | no | a leg of a plan a withdrawal superseded |
| `live-plan` | no | a mover a live item's plan or leads name, on no window of this tier |
| `live-item` | no | a live queue item's own tokens |
| `receipt-names-live-item` | no | a holder whose receipt names a live item |
| `funded-output` | no | a produced-output mover with a funding record (#929) |
| `funding-unreadable` | no | the same, with a funding record that does not read |
| `orphan` | yes | a holder whose receipt names no live item |
| `receipt-less` | no | a holder with no receipt (the `6fbc96301c6c` shape) |

The evictable holders sum to `evictable_gib`, and all of them to `held_gib`.
The census also names the queued new money by the window its row stages for,
the owed output windows by owner, and `committed_gib`: held less evictable,
plus queued, owed output and every admitted window's growth.
`over_committed_gib` is the part of it past the capacity.

**A refusal names its gap.** A `joint-commitment-stall` refusal's
`commitment` carries `shortfall_gib` (the committed total plus the newcomer's
own growth, less the capacity) and `terms`: the holders grouped by basis and
window (a holder no window owns is its own term, by key), each admitted
window's growth, the queued rows and the owed output, each marked
`evictable`. The non-evictable terms sum to the committed total the gate
compared. The `window-gated` event carries both.

**The record.** Every cycle, `residency_window` files one record per stage
tier this loop announces, `tier-commitments/<tier_id>.json`
(`prismabuild.tier_commitment.v1`), from the census the protection pass
admitted on. It holds the totals, the terms, every holder, every window's
footprint, and every newcomer the pass refused on the tier (`waiting`), with
its reason and, for a commitment refusal, the gate's terms. A newcomer gated
behind another's wait (`higher-priority-window-waiting`) names that one and
its reason under `waiting_on`. The record is a report: admission never reads
it.

A pass that asked no newcomer took no census. The report then takes one with
`remember=False`, which prices each window as admission would at that moment
but does not raise the consumption memo (`_FASTEST_CONSUMPTION`), so admission
prices every window exactly as it did before #930.

**Over-commitment.** While `over_committed_gib` is positive, the loop appends
`tier-over-committed` to the cycle's events, with the totals and the terms,
on every cycle.

**pbstatus.** `pbstatus --starvation` reads the record, and `pb_starvation`
serves the same blob. Each tier carries `commitment` (the record, or null
where no loop files one: not a stage tier, or a loop from before #930) and
`commitment_age_s`. `joint_commitment_waits` names every newcomer waiting on
the commitment: refused by it, refused because its census did not read, or
waiting behind one of those. Each entry has the newcomer's footprint, the
committed total, the capacity, the shortfall and the terms.

**Cost.** A cycle with a newcomer reuses the admission census, so the report
adds the record write and the terms. A cycle without one adds a census. Median
and p90 of 25 steady-state `residency_window` calls per tree, measured on
2026-09-23 as PB actions on sparky over NFS, on the #907 replay:

| Queue | Before (`ee2420db`) | After | PB keys (before / after) |
|---|---|---|---|
| R12 alone, no newcomer | 21.5 / 21.8 ms | 37.5 / 37.7 ms | `4c27c6b2ac88` / `663142e0138f` |
| R12, R13, the capture, three quanta | 75.4 / 75.7 ms | 80.1 / 81.1 ms | `e856991f8e48` / `15ae898822f4` |

The census itself is unchanged: 11.9 ms beside R12 alone and 18.4 ms against
18.6 ms loaded. The record is 5.9 KB for R12 alone (24 holders) and 9.9 KB
loaded. The profile of an R12-alone call puts the added time in
`_report_commitments`: the census's ledger token-directory scans
(`holder_tokens`, `_mover_state`), as in #909, and one atomic write. At the
loop's 60 s cadence the worst case is 16 ms a cycle.

### Adopting a resident range, and when an orphan is evicted

The campaign is one probe and many artifacts of one model, so every artifact
reads the same shards. Measured on 2026-09-18: a run stage filled 731.5 GB
across 11 movers, its consumer failed on an application defect at 13:50Z, the
orphan sweep had deleted all of it by 14:17Z, and the resubmission of the same
manifest had to copy every byte again. Rob: *"We should not be rerunning
anything in bulk if avoidable."*

**The invariant does not move.** Held tier tokens cover the bytes on the stage
at every instant, before this and after it. #598 refused retained-but-unpinned
bytes for that reason and nothing here reintroduces them: every resident byte
is held by some key throughout, and the two changes below are about *which* key
and *when* the bytes go, never about whether they are counted. The one holder
that is deliberately larger than its bytes is a partially pruned stale-mention
owner (#853): it keeps its whole charge, slack included, until its last
fragment leaves through the ordinary whole-owner egress, so the reservation
errs toward reserving for bytes that have gone and never toward bytes no token
covers.

**Adoption.** A mover's action key hashes an argv carrying
`--consumer-action-key`, so two consumers of one manifest seal two different
keys for the same bytes; the residency descriptor —
`(manifest_sha256, tier, range_start_bytes, range_end_bytes)` — is the identity
they share, and it is deterministic. When a live consumer's plan names a phase
whose descriptor is already resident under a key **no live item still names**,
the `tiers` loop hands the range over instead of staging it again: the
successor's fragment is written first, the tokens are transferred, and only then
does the old fragment stop accounting for the bytes. Dropping the old fragment
first would leave an egress able to release tokens for bytes that are still
there; dropping it last means the worst an interrupted adoption leaves is a
range named twice, which every reader already tolerates.

Adoption follows the contiguous qualified prefix of the remaining stage legs
(#864). A missing leg, an unfinished copy, a withdrawn leg, or an unqualified
donor stops that consumer's adoption pass. A reservation alone does not bridge
the gap: the existing `resident_movers` predicate establishes that an earlier
leg has landed. This also applies between chunks inside a phase. Farther donor
ranges stay charged to their historical owners and remain cached while there
is no pressure; the existing orphan sweep may reclaim them when a missing
frontier and its funded advance need the room. Transferring them early would
put them under whole-live-plan protection and could starve that frontier.

This changes cross-consumer adoption, not live-plan ownership. Already-owned
future ranges, including same-key retries and tails adopted before this change,
keep their existing protection. It does not retroactively evict a live tail or
provide a live producer-window shrink operation. Reader pins, co-owner checks,
the immutable plan, and the existing admission/funding algorithms are unchanged.

An adopted mover never runs, so it files no terminal record. What it files is a
move receipt carrying `adopted_from` and `bytes_copied: 0`, and what it holds is
the range's tokens — the same two facts the gate's `executed` branch is really
checking, so `residency_verdict` reads them directly rather than looking for a
`done/` record that will never exist.

**The egress is the other party, and they exclude each other rather than being
ordered.** An egress that reads the fragment before an adoption and unlinks
after it deletes bytes a live consumer now holds tokens for; one that reads it
after would release tokens for bytes that are still on the device. Neither
ordering is safe, so both take the *mover's* transition lock — the egress row's
own key is a different action, so it is not the lock `_claim` already takes.
The egress waits; an adoption that cannot take the lock declines and the range
is copied, which costs time and never correctness.

**When an orphan is evicted.** An orphan is a range whose consumer finished,
failed or was withdrawn, still pinned because its bytes are still there. It is
now evicted when the tier needs its tokens: `window_pressure` reads the first
phase a live consumer has not made resident, and the sweep takes orphans back —
oldest receipt first, a deterministic order rather than a ranking — only until
that much is free. A tier no window is waiting on keeps its orphans, held and
counted, for the next artifact that names them.

The grace is therefore not a timer and there is no constant anywhere in it. The
deferral is safe for the same reason adoption is: the tokens are held the whole
time, so the ledger still counts every resident byte and nothing can be admitted
onto capacity that is not there. What is *not* deferred is the reconciliation
inside the sweep — bytes no key holds are not a cache, they are the accounting
hole #608 closed, and they are taken back every cycle.

**A held mover whose receipt is gone is named by its fragment (#892).** The
sweep reads an orphan's consumer from its move receipt, and a mover can outlive
its receipt: the canary leg-3 mover `aa34e2a6e22f` held 1 stage GiB from
2026-09-19 with no receipt in `movers/`. When the receipt names no consumer,
the sweep takes one fragment census and resolves the holder only if exactly
one direct fragment names it and that consumer has provably ended: it is
neither ready nor claimed, and exactly one outcome record (`done`, `failed`,
or `withdrawn` with its one immutable decision) says how. That holder is then
an orphan like any other: pressure-gated, oldest receipt first (it has none,
so it sorts first), then `evict`, which rechecks co-owners, claims, pins and
handoffs under its own locks. No fragment, several, a produced-output
fragment, a tainted census, or a consumer with no ending retains the holder.
A consumer the queue has no outcome for is not an ending: #798's legacy
fixture is that shape, and the first version of this change deleted its
bytes. A retained holder is reported as a `stage-receiptless-holder-retained`
receipt naming the reason only when the tier's window still lacks room after
the pass; otherwise it waits quietly, so an unresolvable holder does not add
a line to every tier cycle.

**A funded produced-output mover is its lane's, receipt or no receipt (#929).**
A prepaid produced-output mover holds its tokens from publication, and neither
its failure nor a withdrawal releases them: the only release is its producer's
own `retire_batch`. On 2026-09-21 the one-shot Stage A producer `0dedb066f868`
failed closed on `BoundaryStagingTimeout`, an operator withdrew its stranded
mover `6fbc96301c6c` from `ready`, and the mover kept 1 stage GiB with no
receipt, fragment or plan. The rules above could not see whose it was, and
`retire_terminal_output_funding` kept the mover's funding record because the
mover still held the token, so each leak pinned the other. The sweep now asks
the output funding record first. For a key it funds on this tier:

- **Kept** when the producer attempt still holds its claim, or the mover itself
  is ready, claimed or in a transition.
- **Retired at once** when the producer attempt has ended (`dead`, or
  `succeeded` without retiring the batch), the mover has one outcome record,
  its funding is `consumed`, and its batch's active copy is this mover's and
  is not retired. The sweep runs the producer's own `retire_batch` for it, on
  the tier host only, whatever the tier's pressure: a retried producer binds a
  new instance and batch namespace, so no one can read this copy again and
  keeping it is not a cache (#598).
- **Unknown** otherwise, and kept. An absent producer is unknown, not dead: no
  outcome record is not an ending (#798).

The same question fixes the opposite exposure. A completed produced mover's
receipt names its batch namespace, not a queue action, and its fragment is in
the produced store, so the orphan pass used to take a live producer's
completed batch for an orphan, look for its fragment in the flat store, find
none, and release its tokens while its bytes stayed on the stage.

Every held key the pass can prove neither live nor an orphan is reported as
`stage-holder-unresolved`, naming the reason, once per change of that reason
and whatever the tier's pressure. A holder nothing can classify is therefore
seen once, not never and not on every cycle. A live item's own holding is not
such a holder, and neither report names it: a producer's output window, whose
row carries no residency, and a window's `advance-` fence grant for a live
consumer are both named by a live claim. The joint-commitment census
still counts a receipt-less holder as held. The one shape the sweep frees
from that group, it frees before the cycle's windows admit. The pressure and
adoption passes that run before the sweep still count it, so for one cycle a
tier's pressure can read higher by that holder's size. That errs toward
evicting one more orphan, never toward admitting onto occupied room.

Two limits, stated rather than hidden. A direct call to the sweep with no
pressure named still takes every orphan, which is what an operator means. And
`reclaim_terminal_reservation` refuses an adopted mover, because it demands
exactly one terminal record and an adopted mover has none; the supported way to
return that range is its egress, which is the path the sweep already uses.

**Uncharged dead-owner fragments are reclaimed without pressure (#839, #866).**
The standard sweep now makes one complete fragment discovery across its tiers
before its held-key pass. A failed consumer's withdrawn mover can leave a
fragment after its reservation has gone, without filing a move receipt or
material sidecar. The fragment still blocks the shared publisher indefinitely,
so this exact shape is retired even when no tier needs capacity. This changes
the automatic cleanup default for that shape; charged resident ranges retain
the pressure-based adoption policy above.

Discovery does not authorize deletion. The sweep holds the consumer transition
lock, then the mover transition lock, through the existing egress, which takes
the stage ownership lock and retains its pin, co-owner and source-handoff
checks. Fresh live-state checks exclude a republication that won discovery.
The consumer must have exactly one terminal record, and it must be proven. A
failed consumer needs the queue's verified immutable failed-attempt summary.
A withdrawn consumer needs its visible withdrawal to agree with its immutable
decision for that exact action and generation (#892): until 2026-09-22 only
failure counted, and three withdrawn consumers' owners were cleared by hand
that day. A consumer that is live again, done, or both failed and withdrawn
retains. For the withdrawn mover form, the mover's visible withdrawal must agree with
its immutable decision for that exact action and generation, and no move
receipt may remain. There must be no live row, lease, filed consumer plan,
material sidecar, or reservation directory. Missing proof, unreadable or
nonregular records, incompatible identity, and terminal shapes outside these
two named forms retain. Produced-output namespaces retain their separate
lifecycle, and the stage-root marker remains mandatory. One ledger discovery
per tier is reused to filter candidates; exact absence is checked again while
the ownership transitions are excluded. Existing egress performs its fresh
co-owner census for each actual retirement; no discovery cache replaces that
atomic safety check.

The same pass also handles an executed `DONE` mover whose move receipt
explicitly describes an incomplete copy and whose material sidecar is absent
(#866). Its consumer must still have the immutable failed-attempt proof above;
the mover needs its own immutable executed-attempt proof and no competing
terminal state. The receipt must bind this exact consumer, mover, tier, stage
root and manifest, with positive staged entries fewer than declared entries,
the exact fragment entry count and byte sum, and coherent bounded range
arithmetic. The same live-row, lease, plan, material and reservation absence
checks apply under both ownership locks before ordinary `evict` runs.
Complete, material-bearing, corrupt or otherwise unproved DONE copies remain
outside this recovery. The held-range pressure policy, produced-output
lifecycle, source files, reader pins and co-owner/source-handoff guards do not
change. This closes the zero-credit partial-copy publication obstruction;
it does not treat an obsolete material incarnation as current proof.

**A material-bearing DONE owner is pruned path by path, under its charge
(#853).** A failed consumer's executed `DONE` mover can leave a material
sidecar that dates an incarnation the live destination no longer carries: a
later publication replaced the name under a fresh inode. The shared publisher
is right to refuse both adoption and replacement of such a name -- the record
cannot prove the bytes that are there, and a live or unknown publication must
not be overwritten -- yet whole-owner `evict` is also wrong for a *mixed*
document, because the coherent entries beside the stale ones are valid
reusable cache whose proof, bytes and charge the successor would otherwise
adopt for free. So the same dead-owner pass prunes the positively stale paths
in a bounded transaction of its own.

Selection, classification and action all run inside one stage-ownership hold
(the lock a publisher's decide-and-rename uses), under the mover transition
lock the pass already holds. The stage root must positively belong to this
queue (`stage_root_refusal`), the fragment must be the snapshot the terminal
checks authorized and bind to this tier and stage, and the material is read
with the cleanup authority's regular/no-follow rules. Every fragment entry
must bind **exactly** to its own material key -- key present, matching stage
path, matching bytes, and matching digest where the fragment declares one --
before any path state is classified, so a by-path, first-mention or
partially-known sidecar can never authorize a deletion; extra material keys
are the crash superset and are never ownership. Positive staleness for an
existing regular file is **inode difference**, exactly as
`_StagedPublisher._proof_candidate` reads it (#755); a same-inode size/time
change is divergence, not permission. Absent paths prune as absent.
Everything else retains the whole owner: an unreadable, malformed or foreign
fragment/material, any binding mismatch, an epoch that is not the shape the
strict reader requires (absent or empty off the ram tier, the same non-empty
string on it -- two documents agreeing on a staged epoch are two documents
the reader refuses), a nonregular path, a
symlinked intermediate directory or any component that leaves the stage, or
any taint in the claim, pin, co-owner or promotion-handoff censuses. A live
claim, a live reader pin, a promotion source handoff or a same-key claim
overlapping the candidate also retains the whole owner before any deletion:
the bounded #877 mover ends first, and a later ordinary sweep recovers the
still-owned stale paths rather than dropping a mention while a foreign claim
merely promises a future proof -- that path recreates the unowned marked
files and the per-entry orphan grace the incident paid. A co-owner fragment
protects its physical file per path; that entry and its date stay untouched.

A fully stale, unprotected owner goes through the ordinary whole-owner
`evict` inside the same transaction (containment reclamation deliberately
does not run under this lock), so its holder is settled exactly once. A mixed
owner is partially pruned: each positively stale destination is unlinked
after a fresh identity comparison immediately before the act, then the
fragment is rewritten to its survivors and the material to the same
survivors' mentions, under the unchanged generation and epoch. A nonregular
path never authorizes cleanup: it is unknown ownership and retains the whole
candidate, documents included. Only a missing leaf may be pruned, and only
after its key bound exactly -- the mention goes, nothing is unlinked. The
fragment is authoritative and written first, so a crash between the two
document writes leaves a material superset that dates removed fragment
entries but cannot itself assert ownership, and a replay settles nothing
twice.

A partial prune deliberately moves **no charge**. The entire old holder is
retained as a conservative reservation, including the slack for the deleted
entries, and it is released exactly once when the final old fragment
disappears through the ordinary whole-owner egress; ordinary pressure
eviction may retire the smaller fragment and return the remainder before
that. Partial capacity reclamation is explicitly not claimed. The receipt
(`stage-stale-mention-pruned`) reports committed metadata prunes only after
the fragment write lands, already-absent entries, files actually unlinked and
the bytes those files actually held, measured by the same `lstat` that
identified each one immediately before its `unlink` -- never a declared or
planned length -- whether the fragment/material pair completed, and
`charge_retained`; it never reports full-owner recovery (`complete`) while
survivors remain, and an interrupted pair reports committed and physical
work separately.

Unchanged, fully coherent owners are skipped by a bounded, process-local,
**skip-only** checkpoint: the fragment and material file versions -- sampled
before their reads and again after the scan, and installed only when the two
samples are equal -- plus device/inode/mtime/ctime stamps of every unique
immediate parent directory of the fragment's paths, sampled before and after
the classification scan and installed only when all are present and equal.
Nothing is re-sampled at installation, so a rename landing after the scan can
never be blessed as clean: the next pass reads the recorded (older) stamp,
sees the difference and re-scans. A symlink or non-directory parent is never
cached. A hit only ever skips the cleanup scan -- terminal, live, lease and
plan checks still run, and no deletion or adoption is authorized by it. The
cache is bounded by entry count, by total retained parent paths and by total
retained path bytes: overflow forgets the oldest entry and, when one
candidate cannot fit, caches nothing and takes the uncached scan. It never
caches an unknown or an actionable stale candidate, and it holds no
ownership authority. The cache key includes the queue, residency and stage
roots, so two roots can never share a checkpoint.

A partially pruned owner is a **per-path cache, never a whole-range donor**.
Its historical move receipt stays factual -- nothing rewrites it to hide the
prune -- so whole-range adoption notices the disagreement: `tier_loop.adopt`
refuses unless the current donor fragment still agrees with the complete
receipt's `entries_declared`/`entries_staged`, **covers its staged bytes
exactly**, and matches the requested leg's range. Coverage is equality, not a
bound: `stage_move` adds each landed entry's own length to both that entry's
`bytes` and the receipt's `bytes_staged` (an adopted incarnation returns the
same length without copying), and a run that did not land every entry files an
incomplete receipt -- so for `complete: true` the two are equal, and equal
entry counts prove nothing about bytes. Two surviving 4 KiB entries under a
receipt that staged 16 KiB is a certificate for bytes the stage does not
carry. All of it is checked under the donor's transition and stage ownership
transaction, and before the per-file identity walk -- metadata is cheap and
only a donor that can still stand for the whole range is worth qualifying file
by file -- before any successor document is written or any token moves (a
failed adoption leaves donor, successor and tokens untouched).  A
successor therefore takes the surviving entry through the shared publisher's
per-path proof and copies the rest; whole-range adoption never certifies a
shortened fragment as the original complete range.  When a crash between the
two document writes leaves the material a superset, the next sweep trims it
to the surviving fragment's exact validated key set under the same
generation, so the strict reader's every-material-entry walk accepts the
recovered pair; safe extra material never grants ownership or deletion
authority.

### Reader pins: a live reader blocks eviction until it releases

Fragments say bytes are staged; they do not say who is reading them. A reader
pin (`src/prismabuild/reader_lease.py`, schema `reader_pin.v1`) says it: one
pin file per staged window beside the fragments, holding one ref per logical
acquisition. `acquire` proves the window is covered by published material,
checks the tier epoch and every file's portable identity under the stage
root's ownership lock, and appends the ref; `open_pinned` checks the
descriptor it will actually read and records the serving tier at open;
`release` drops exactly its own ref, independent of compute progress. Two
acquires need two releases; retrying one acquire token reuses its ref; a
forked child registers its own inherited ref, so a parent release cannot
unpin it.

The egress defers to any live ref: it keeps the file, the fragment and the
charge, files a retiring mark bound to the material generation (closed to
new acquires for that generation only), and deletes after the last release.
The reconciliation holds the same ownership lock across query and delete,
and treats a live pin as attribution. Lock order is transition, then
ownership, then rename/unlink on every path; acquire takes ownership only.

One stage root at a time (#780). That ladder orders the three lock families
and says nothing about two locks from the ownership family, which is where a
cycle fits: `release_refs` settles each ref under the ownership lock of the
root that ref's own pin names, so an egress holding root A that reclaims an
unscoped census asks for root B, while an egress holding B asks for A.
`posix_lock.held` nests on the same path only, so same-root reentrancy does
not prevent it. The rule is that no caller holds one root's ownership lock
while requesting another's: the egress runs containment reclamation between
its transition lock and its ownership lock, and the pin census the delete
decision reads is taken inside the ownership lock, after it. That census,
not the reclaim, is what makes check-and-act atomic, so moving the reclaim
out changes no delete decision; a ref whose containment evidence lands
between the two defers to the next sweep, the direction the egress already
fails in.

A pending copy handoff defers the same way (#768). While a live ram
promotion's sealed claim names a stage source leg, the egress keeps that
leg's file, the stage mover's fragment and material sidecar, and the
mover's full occupancy charge; the reason rides the additive egress-receipt
`deferred_handoffs` field (`["promotion-handoff"]`). The handoff outranks
the generic co-owner and in-flight-destination shared skip: another
consumer's same-path fragment proves the bytes for a general stage
publisher, but a promotion resolves its source cover in its own
consumer/manifest namespace, so a co-owner cannot stand in for this owner's
pending proof acquisition. The source owner's proof and full charge are
therefore retained while the handoff lives, even when another owner exists;
it retries after the claim ends, and only then does the ordinary shared
decharge settle against a genuine same-path accounted co-owner, or the
last owner's source delete. No retiring mark is filed while any
handoff in this mover's document is deferred: a mark closes one material
generation to new acquires, and the promotion takes its proof-only cover
through `reader_lease.acquire` after its claim row exists, so the mark
would refuse the very handoff it is protecting. Retirement marking waits
until no handoff remains across the whole document; a pinned entry on the
same mover then files the ordinary mark, and a mark already on disk is
preserved, never cleared by a deferral. Without a material sidecar no
generation mark can be filed (`retiring` stays false), but the deferral
still retains the file, the fragment and the full charge until the claim
ends. The deferral is whole-document, as every deferred retirement is: an
entry another owner has already deleted is not recreated, but one deferred
entry retains this mover's fragment, material and full holder charge until
a retry settles the document exactly once. The promotion's ram fragment
names another tier and path and cannot prove the SSD incarnation it read,
so dropping the source's same-path proof while the copy is pending leaves
a surviving file no publisher can prove and frees capacity its bytes still
occupy.

Object identity is portable across clients: tier namespace, epoch, path,
length, per-publish materialization generation (uuid4; a resumed run
carries its prior own generation and mints a fresh one only when it first
replaces bytes, so unchanged coverage keeps its date), content digest, and
backend `(ino, size, mtime_ns, ctime_ns)` — never `st_dev`, which disagrees
across NFS clients. Publish-time identity rides a mover-written sidecar
(`residency/material/`, schema `reader_material.v1`); the map fragment
schema v1 is unchanged, and the fragment-owner scan skips the `leases/` and
`material/` namespaces. A RAM promotion proves its source window across
however many stage movers cover it (`source-coverage-gap` refuses
boundedly), verifies its copy against the sidecar digest, and its live
claim — parsed from the sealed request — protects the source leg through
the copy. Containment of another attempt's refs needs the broker's scope
attestation plus the attempt's terminal evidence, read authoritatively;
anything unanswerable retains the charge. Capability tag `reader-lease-v1`
is advertised only once qualified and deployed. Capacity liveness (who may
hold how much against whom) stays a named next policy step.

Automatic ref recovery binds the owning action's exact nonce, scope, worker
incarnation and terminal record, separately from the material's namespace.
Cleanup obtains the broker's token-gated stopped-scope export; a release
reply alone is not containment proof. If that export is incomplete and exact
attempt refs remain, `finish_pending` retains the claim and reservations.
The existing worker reaper retries normal cleanup after settlement; egress
can then reclaim orphan refs. A lost attestation file can be reconstructed
from a validated complete export stored in the terminal. Unknown scope or
pin state retains ownership, and an old attempt cannot release a successor.

The cover lookup (`covers_for_keys`) only selects movers; `acquire`
revalidates under the ownership lock before anything pins. The lookup keeps
each mover's validated sidecar and fragment per process, keyed by root,
consumer and mover, and bounded at `COVER_DOCS_CACHE_PAIRS` (#893). Every
call still opens both files, so NFS close-to-open revalidates them, and a
file is read and validated again only when the `(st_dev, st_ino, st_size,
st_mtime_ns, st_ctime_ns)` of the descriptor just opened changes. The
identity never leaves the process, so `st_dev` is consistent here, unlike in
the portable identity above. Both writers rename a new inode into place, so
every republish, #823's same-generation one included, is seen; a same-size
rewrite of the same inode within one timestamp tick would not be, and no
writer makes one. Absence and malformation are never cached.

### A same-key retry resumes its own qualified coverage

A mover's action key is a content hash, so a retried mover — a timeout, a
requeue — runs under the **same** consumer and mover identity, and
everything it files is keyed by that identity: one fragment, one material
sidecar, each *replaced* wholesale by every publication. A copier that
starts every dictionary empty therefore publishes, on its first
incremental snapshot, a fragment holding only the entries it has
re-encountered so far — and the qualified suffix its previous attempt
staged loses proof, is recopied, and pays the publication grace per entry.
Head `5c46f93b…` did exactly this after its 3600 s deadline: 2,022 entries
of proof fell to 1 and grew back at seconds per file, which is why the
same-key retry is a relaunch blocker even after the bounded orphan
recovery retires the unowned copies.

`stage_move._resume_own_coverage` is the narrow resume, and it changes no
lifecycle the loop owns. Inside the same stage ownership lock the
publication gate holds — document reads, header checks and the per-entry
file stats together, so the qualification cannot act on a snapshot that a
retirement or rewrite has already superseded — the mover reads back its
**own** prior fragment and sidecar, and only when the fragment is this
invocation's own does it preserve anything. Unknown or contradictory
ownership refuses the whole invocation before any copy or publication:
a fragment that exists but cannot be read or validated, one whose headers
disagree with the invocation (consumer, mover, tier, stage root, manifest,
or an epoch on the SSD tier), an entry it names outside the window this
command derives, or a record at a different extent than the manifest
derives, each raise a typed refusal and leave the record exactly as it
was. The causal case is the one that must not silently converge: a
tainted prior fragment, old destinations still holding bytes, and one
declared destination that never landed — without the refusal the absent
destination replaces cleanly and its publication overwrites the tainted
fragment, converting unknown ownership into apparent absence; with it,
the retry (and the next) keeps meeting the same refusal until an owner
resolves the record. Confirmed absence of the own fragment is the one
non-conflict: nothing is preserved and nothing that says anything is
overwritten, so a first publication initializes empty through the
ordinary machinery. Two tiers, deliberately different:

* The **vouch** (the fragment record) is preserved for every prior entry
  this window derives whose record agrees with the window's own
  derivation. The name stays published, so a rerun that reaches a
  *changed* entry still meets its own vouch at the publication gate and
  is refused, exactly as any vouched name is refused — dropping the
  vouch instead would turn that refusal into a grace-then-heal
  replacement of bytes another publication once named.
* The **date** (the material sidecar mention) is carried only where the
  sidecar's headers qualify against this invocation and the fragment
  (consumer, mover, tier, stage root, manifest, and the SSD no-epoch
  convention) *and* the existing proof standard still holds per entry:
  the manifest's declared digest agrees, and the sidecar's `file_id`
  matches the live file. A readable sidecar with contradictory headers
  is conflicting state and refuses — it must never be carried and
  republished under corrected headers — while an absent or unparseable
  sidecar is the documented crash window (a vouch without a date):
  vouches are kept, no date is invented, and the rerun overwrites both
  as it always has. No payload is hashed.

A retry's publications are therefore never smaller than the coverage it
inherited, in flight and at the final publish, and repeated interruptions
only ever grow the published set. Re-encountered entries still go through
`try_adopt`, so an unchanged suffix adopts with no repeat payload read,
copy or hash. The receipt's `entries_resumed`/`bytes_resumed` report
**preserved coverage** — a kept vouch is not necessarily a requalified
date and not committed progress — beside `entries_staged`/`bytes_staged`;
resumed coverage adds no staged bytes and cannot complete a receipt on
its own, and no second progress counter exists. A controlled
two-worker regression (per-landing publication, zero rate limit) holds
every published snapshot at or above the preserved prefix.

The generation follows the `adopted_generation` rule already used across
key changes: a resumed run *carries* its prior own generation (same bytes
without replacement keep their date, so a live pin's covers still match),
and the publisher mints a fresh one exactly once — the first time the run
replaces actual bytes, because from that moment the carried date no longer
describes the whole document. A resumed run that replaces nothing rewrites
its fragment and sidecar under the unchanged generation, so a reader
re-acquiring over the same window gets its own pin back rather than a
`generation-changed` refusal. Existing live pins are never invalidated,
rewritten or treated as stale by a restart: an unchanged resumed entry
keeps its inode, so `open_pinned` still verifies its descriptor while
other entries land.
`tests/test_a_same_key_retry_preserves_its_own_qualified_coverage.py`
holds each rule as a focused case on tiny real files through the real
tool: the never-smaller prefix, the repeated partial retry, the changed
entry that refuses instead of being replaced, the corrupt sidecar that
dates nothing, the live pin that stays valid as new material lands, the
all-adopted resume that keeps its generation, the corrupt or conflicting
prior fragment that refuses before any copy leaves the record untouched
(including the new-destination causal shape, and the same refusal again
on the next retry), the contradictory sidecar headers that refuse rather
than republish corrected, and the two-worker per-landing publication
regression.

### A refused publication ends dispatch for the range's remaining entries (#853)

The publication gate can refuse one shared staged name and will not replace
what another publication owns, what a live pin protects, or what an
unreadable proof might name. That refusal is made per name and proves nothing
about the others -- they may be perfectly publishable. It does make the range
incomplete, though: this mover cannot publish what it declared, so every
further copy and, for an undatable vouch, every further grace is spent on a
run that must be retried anyway, and the first obstruction is buried by the
receipt's capped error list. On the 2026-09-22 full512 head one
stale-material vouch left sixteen workers sleeping through grace after grace
while the consumer stayed READY; the accepted red fixture measures 8,192
payload bytes copied for two blocked entries on one worker. The policy is
therefore to bound the wasted work and surface the obstruction, not to
conclude anything about the remaining names.

The gate signals that refusal with `stage_move._PublicationRefused`, an
`OSError` subclass, so every caller that already catches `OSError` keeps
catching it and no existing behavior breaks on the type; the copier
deliberately changes its range handling for it. On receiving it a copier
thread records the refusal under the same small dispatch lock that hands out
entries, before logging the entry error, so once the refusal is recorded no
worker is handed a new entry. The entries already dispatched to the active
group still finish or refuse, and everything they committed keeps its
fragment, sidecar and byte count in the usual incomplete receipt, which names
the refused path and is retained for the retry. The flag is internal and
separate from the caller's `stop` event: a refusal never sets cancellation or
withdrawal, which keep their own decision and terminal. An ordinary source
read, digest mismatch or filesystem failure stays a per-entry error and does
not stop the range -- one unreadable source is not evidence about the other
names. Nothing here adds a terminal-owner lookup, a global scan, or any new
permission to adopt or overwrite: the proof, pin, claim, grace and ownership
decisions are exactly the ones the gate already made.

`tests/test_publication_refusal_stops_the_range.py` holds the bound with
deterministic work counters and barriers rather than wall-clock: one worker
consumes exactly the refusing entry; a four-worker barrier puts four entries
really in flight, only that prefix is dispatched with no duplicates, and the
one valid in-flight entry keeps its committed bytes and proof while its peers
refuse; an adopted entry keeps its proof and bytes while the next entry's
refusal stops the range; a pinned publication is refused unchanged and never
replaced; a missing source only records its own error and the range
continues; and a pre-set external stop is never confused with the internal
flag. The accepted red evidence (`853-red-results.json`, shards
`c36202318cfa` and `d992f975c10a`) is retained separately.

### A failed consumer's movers are withdrawn; a failed mover's partials are evicted (#620, #627)

A consumer that fails with movers published leaves them running for nobody.
The egress evicts the completed ones, but the not-yet-run ones are not
withdrawn: on 2026-09-18 six movers staged ~400 GB for a consumer already in
`failed/`, and the successor's head mover over the same content paths finished
`complete: false` — its `.partial` vanished before the rename, taken by
another mover working the same path for the dead consumer.

So in the same egress cycle that evicts the dead consumer's resident ranges,
`tier_loop.withdraw_dead_consumer_movers` withdraws its movers still in
`ready/` or `claimed/`: the queued ones never start, the claimed ones are
stopped through the withdrawal decision (their `claimed/` record stays until
the claiming worker concludes — withdrawing never concludes another worker's
claim), and both leave their partials to the sweep and the reconciliation.
Never touched: a mover with a complete receipt (a resident range, which the
successor adopts), an egress row (cleanup, not staging), a consumer key also
present in `ready/`/`claimed/` (resubmitted — the withdrawal names a
generation, not a key), and a consumer whose plan this reader refuses
(unattributable movers are hands off; a refused withdrawal is reported, never
forced).

Discovery first intersects terminal names with one observation of filed-plan
names (#870). Unrelated historical outcomes cannot own staging work and do not
trigger terminal-body reads or repeated live-queue absence scans. This is only
a candidate filter: each selected consumer still undergoes the unchanged
locked terminal, live-state, current-plan and incarnation checks before any
withdrawal or reap. An unreadable registry defers the pass; a plan not visible
in this observation is considered on a later pass. No mutable live-state cache
or cross-cycle authorization is introduced.

The other half is a mover that ends without a complete receipt: its tokens go
back at `finish`, but every entry it renamed into place before it failed stays
on the dataset, named by its fragment and counted by no token — and nothing
publishes an egress for it, because the window evicts only phases the consumer
has read past. `reclaim_failed_mover_partials` treats that mover as an
eviction candidate whenever the window has no room for the next phase and
publishes its own egress row, which already handles "an earlier egress removed
it" and returns no tokens when none are held. No pressure, no reclaim; never
from under a queued recopy, a complete receipt, or a concluded egress (which
refused rather than raced — republishing would only repeat it). While the
egress is queued the window holds the recopy (`mover-publish-deferred-for-egress`):
the egress frees device bytes, not ledger tokens, so republishing into a stage
that is still full would ENOSPC into the very room being made.

### A stage root belongs to one queue (#628)

Every rule above decides *what* the sweep deletes: a held key no live plan
names, a `.partial` no live copy is producing, an unmarked file no wanted
fragment names. None of them asked *whose* stage was being walked. On
2026-09-18 at 16:05Z a test on the storage box announced `/stage/prewarm` to a
queue under `tmp_path` and ran one tier cycle; that queue's fragments attributed
nothing, the prewarm xattr marked nothing, and `stage_release.reconcile` deleted
671 GB of staged shards in one walk while the run they were staged for was
reading them. The bytes came back by the #624 recompute movers over the next
two hours. The mechanism was working exactly as specified; the specification
had no owner.

**The tier loop marks its own stage before it announces it.** For every stage
tier discovered on the loop's own host, `cycle` calls
`stage_release.register_stage_root`, which writes
`<mountpoint>/.prismabuild-stage.json` naming the queue by the real path of its
root, the tier id, the host and the time (schema
`prismabuild.stage-root.v1`), by temporary and `os.replace`. A marker that
already names this queue is left alone. A marker naming **another queue is never
overwritten**: two owners is the state this refuses, and an operator moves a
marker by hand when the queue really has moved. A root the loop cannot write
(the Sparks mount the stage read-only; a mountpoint that is not there) is
reported, not raised: the tier is still announced, with the outcome on the
record as `stage_root_owner` (`registered`, or the refusal string).

**The three deleters refuse anything but their own.** `sweep`, `reconcile` and
`evict` each ask `stage_root_refusal` before the first `unlink`. A marker that
is missing (`stage_root_unregistered`), unreadable
(`stage_root_marker_unreadable`), not a marker (`stage_root_marker_invalid`) or
another queue's (`stage_root_belongs_to_another_queue: <root>`) refuses:
nothing is deleted, no token is released, the fragment is kept for the owner,
and the receipt carries the reason in `skipped` and `errors` with
`complete: false` under the event `stage-root-refused`. `sweep` refuses once
per tier per cycle and runs neither the held-key evictions nor the
reconciliation; `evict` and `reconcile` refuse on the same fact for the callers
that reach them directly, the egress action row among them. The marker itself
is the one unmarked, unattributed file at the stage root the reconciliation
skips: without that line the sweep would delete the fact that lets it sweep.

**Tests use temporary roots, registered.** The three tests that named the real
mountpoint now name `tmp_path`, and every fixture that drives a sweep or an
egress registers its temporary root first, the way the loop registers the real
one. `tests/test_a_stage_root_belongs_to_one_queue.py` holds the incident's
exact shape — a throwaway queue, the fleet's marker already on the root, one
cycle — and asserts nothing is deleted and the announced record says why.

**A present-but-unregistered root refuses movers and keeps its tokens (#631).**
Registration is a ~300-byte marker write, and the cycle marks before it
mints. When the mountpoint exists but `stage_root_owner` is anything but
`registered`, the tier still mints its whole supply -- refuse-and-keep, not
refuse-and-remove. Popping the occupancy kind retired it to zero and turned
every `[]` read of the ledger into a `KeyError`, while the held reservations
the refusal exists to protect kept working. The announced record carries the
refusal as `stage_root_owner` with `stage_root_admits: False`, said once on
the log; the window publishes no movers against such a tier
(`mover-publish-deferred-unregistered-root`, ram:
`ram-mover-publish-deferred-unregistered-root`); egress still publishes, and
the sweep refuses on the same fact, as it always has. A fresh root therefore
always marks before its first mover is admitted, and a full unregistered
root stops admitting instead of filling to exactly 0 B available and then
refusing the sweep and the egress rows that are the only way room is made.

**A mountpoint that is not there at all is pre-registration, not refusal.**
Registration never got a chance to mark -- a discovered dataset's mountpoint
always exists -- so the cycle mints as before and stamps no verdict; the
record carries only the owner the registration reported. Telling the two
apart is what keeps fixture paths and discover anomalies on the supply
arithmetic they pin instead of answering the refusal.

**Bootstrapping an already-full root is an operator path, not a bypass.**
The loop cannot delete under a root it does not own, so no code path clears
the deadlock from inside; what the operator does is make room for the loop's
own next marker write, and the loop registers itself:

* Prefer the slop window: `spa_slop_shift` 5→6 for seconds on the storage box
  frees ~11 GB of `available` out of the slop with no deletion and no data
  touched, the loop's next cycle writes its marker, and the shift is restored
  on exit (trap it). Needs sudo for the two kernel-parameter writes.
* Without sudo: delete a bounded set of staged fragments whose pool originals
  match by sha256 — enough bytes for the marker, recopyable from the pool —
  and only fragments the live run's residency plan does not name.

Either way the marker is written by the loop, never by hand: a hand-written
marker is a second copy of the queue identity the ownership check exists to
refuse. Once registered, the loop's own egress reclaims the rest through the
ledger, which is where that decision belongs. Whether the ledger should ever
fill a dataset to 0 B available at all -- a measured headroom off the
dataset's own `used`/`logicalused` ratio rather than a constant -- is open;
it is not this gate.

### Bounded recovery of a retired head's orphaned copies

Routine `reconcile` structurally cannot free one specific history: a head
whose fragment and material a later egress retired while shared files
survived. Every surviving copy carries the same `user.pbstage.source` mark
`stage_move` stamps, so `reconcile` reads each as prewarm-owned and leaves it
in `unowned_left` for the life of the fleet -- and every later head pays the
publisher's 30 s grace once per orphaned entry (measured on the incident
that motivated this: 36439 entries, 10895318814 bytes, ~0.53 files/s).
`stage_release.recover_orphaned_range` is the operator-scoped repair for
exactly that history, and nothing else: it changes no lifecycle the loop
owns, takes no scope from its caller, and defaults to a dry run.

Scope is bound to history, not to argument. The only identities taken are
the head's filed move receipt and the egress receipt that retired it; the
consumer, tier, stage root, manifest and exact byte range are read off those
receipts. Each receipt must be filed, well-formed, `complete`, and record
its **own** action key -- a record read by pathname that names another
action is that action's receipt, misfiled, and is refused. Each historical
CAS request is then read as an action and validated under its own key
(`validate_action` plus the key comparison, the same sequence the pool
applies to a claimed action): a bare JSON load would let any bytes at that
path wear the key, and the flags below are the authority, so the body has to
be the bytes that hash to the identity they are used under. The surviving
flag checks bind the two requests to each other and to the receipts -- the
egress names the head as its mover, both name the same consumer and stage
root, the head alone carries tier and range, a missing or duplicated flag is
refused, and a filed receipt may not widen the window its request authorized.

Permission is that scope plus the positive absence of every other ownership,
under the same stage ownership lock the egress holds, with the same
stage-root ownership refusal first (#628) and no pass while any mover on the
tier is ready or claimed. Every fragment retains, wanted or not -- a
withdrawn mover's fragment included -- and the fragment census is
`_fragment_owners` over the exact staged-path set the scope derived, never
the map-composition reader's tolerance for bad fragments: a fragment that
cannot be read or validated, or a residency root that cannot be listed,
refuses the whole pass, because skipped is how unknown collapses into
unowned and unowned is what deletes. Live pins, claims in flight and
promotion handoffs retain; the old source mark is a necessary condition,
never permission.

Originals are proven before anything is destroyed: each window entry's
source must exist as a **regular file** holding at least the `offset+bytes`
extent the manifest names, resolving outside the stage root. `exists` alone
would pass a directory or a source shrunken below its extent off as a
surviving input. Size, never a digest -- proving recapturability is a stat,
not a model-sized hash. Over-retaining is a pass and over-removing is a
failure; every refusal lands before the first unlink.
`tests/test_a_retired_heads_orphaned_cache_is_recovered_by_identity.py`
holds each rule as a focused case, with the staged copies and originals
asserted intact after every refusal.

### How the map reaches the consumer

`tier_loop` is the map's **single writer**: movers write one fragment each into a
file only they name, and the loop composes them and recomposes after every
eviction, because a rename cannot merge and a map naming an evicted range points
at deleted files. The launcher puts the composed map's path in
`PRISMABUILD_RESIDENCY_MAP`, and only when the file exists.

**The map lives as long as its consumer runs (#908).** When a consumer has
nothing staged, the loop's answer depends on whether it is running:

* A consumer in `ready` has no map. The claim's residency gate reads that as
  `map_not_composed` and waits a cycle.
* A claimed consumer keeps its map, emptied: the tier, stage root, manifest
  and generation it adopted, with no entries and no leads. This is the gap
  between an egress of the last range it read and the landing of the next.

A reader answers a declared span that is not staged the same way from an
empty map as from a missing one: it waits. A missing map, though, is refused
whole and logged as `residency map is unreadable`. On 2026-09-22 capture
`a92f62783e8f` logged that line after its `layer-2` egress, and it read as the
cause of the stall when the cause was a `layer-3` range nobody published
(#903). The next fragment to land recomposes the map as usual. A claimed
consumer requeued into `ready` loses the emptied map on the next cycle.

Neither that variable nor `PRISMABUILD_ACTION_KEY` is sealed. Every map entry
carries the manifest's own digest, so an action that reads a staged copy computes
what an action that reads the pool computes; sealing the map would make one
question two actions and cost every staged run its CAS hit. `PRISMABUILD_ACTION_KEY`
is how a movement node learns the key it files receipts and holds tokens under —
a key sealed into the argv it is computed from has no fixed point.

Because the variable is set only when the file exists, the residency gate
requires the composed map as well as the pin: a consumer admitted after its
mover pinned 34 GB but before the loop's next cycle would launch with no map,
read the pool at full cost and file a clean receipt. That is the one failure
this whole change exists to remove and the one nothing downstream can see, so
the verdict denies `map_not_composed`, which leaves the item ready, ages
nothing and takes no token; the next cycle composes and it is admitted then,
unchanged.

A tier reservation prices two different things and only one of them outlives
the copy (#636). `stage_gib` prices *occupancy*: the bytes are on the device,
so a mover keeps that token from `finish` until an egress deletes them, which
is what the paragraph above about released-but-resident capacity is defending.
`fill_mb_s_pool_side` prices the pool-side bandwidth a copy *draws*, and
nothing draws it once the copy stops. `keep_tier` kept both, so every finished
mover held its full rate for the life of the fleet and `tier_loop.adopt`
carried one range's rate on to each successive consumer although adoption
copies no bytes. Measured on `prismabuild-stage:dl380g10` on 2026-09-18: 506 of
635 fill units held by seven terminal or never-published keys, 129 free against
a fresh mover's demand of 188, so no mover was placeable and every stage-fed
consumer waited on a lead that could not land. `keep_tier` now returns the rate
kinds and keeps the occupancy kinds — enumerating the kinds to release, so a
tier resource added later is kept by default — and `tier_loop.reclaim_idle_rates`
returns the rate of any holder that is not currently claimed, which heals the
ledger as it stands and closes the same hole for an adoption or a mover killed
between its last byte and its outcome. It touches no file on the stage and no
occupancy token.

Existing is not the same as current (#634). The loop composes from the
fragments on disk once a cycle, so a consumer whose lead pins between two
cycles finds a map that is real, readable and missing exactly the range the
gate just waited for — and the faster the mover, the more certain that is,
because admission waits for the pin and the pin is what the composed document
does not know about yet. Measured on action `26dfde9dd764`: the claim record
named one lead, the map named four others, and 90.4% of the payload came off
the pool under a receipt saying `resident`. `compose` already records the
movers whose fragments it merged, so the verdict reads the document and denies
`map_stale`, naming the missing leads, whenever a lead is not among them. It is
the same one-cycle wait as `map_not_composed` and must read differently from it,
because a queue that has stopped moving is diagnosed from which of the two it
is sitting on. An adopted range files a fragment under its own mover key, so a
window nobody had to copy satisfies this without a special case.

### A plan the coordinator cannot read

`residency_plan.read` answers `None` both when no plan was filed and when the
filed one does not validate, and only the second is a denial. `residency_window`
used to `continue` on either, so a plan written by a generation that knows one
more key than the reader — #609 added `demand_source` — silently removed its
consumer from every cycle: the GLM run stage sat ready for 25 minutes behind a
staged head window, over an idle GPU, with a log that said only `tier-cycle`.

`read` now takes an `on_unreadable` callback, still answering `None` so no
caller grows a branch to stay safe. The loop reports a refusal twice, because
two different people read them: a `plan-unreadable` event naming the consumer
and the refusal, and a `residency_plan_unreadable` claim denial, which
`pbstatus` aggregates from any box. The consumer's own verdict distinguishes
them too: `map_not_composed` means "the next cycle composes it", while
`plan_unreadable` means no cycle ever will, so the item names the refusal
instead of waiting out a cycle that is not coming.

Which reader refuses is what decides where the answer comes from. A plan that
is corrupt for everybody is caught on the claiming box as well; the incident's
plan was refused only by the *coordinator*, two generations behind a claimant
that read it fine, and the event and the denial are what make that visible.
The other half of that case is the generation gate below.

### What runs a mover

A mover's argv names an interpreter and a script path, and both belong to the
box that owns the stage rather than to the box that seals the action. The
submitter is very often neither: PrismaQuant's dispatcher submits from an
aarch64 Spark and the stage is dl380g10's, so the submitter's `sys.executable`
names a venv that is not there and the action would die at exec — after the
tier had already reserved its capacity. So `tier_loop` announces `mover_python`
and `mover_tools_root` on every tier record, discovered on the box that will
run them, beside `mountpoint`, which is the same kind of fact; `pbrun` reads
both off the tier or refuses. `publish_runtime` writes every fleet script to
both `tools/<name>` and `tools/fleet/<name>`, and a checkout keeps only the
latter, so the loop's own directory holds `stage_move.py` in either layout.

A mover is also sealed as a mover, not as its consumer (#944). A pool
`--measurement` consumer seals `task_class: measurement`, a `platform_keyed`
scope and the submitter's toolchain (the Spark's shell digest, `machine:
aarch64`). Its movers used to copy all three. Admission then held each mover
to a measurement's idle-host rule on dl380g10, which always runs the tier
loop, the broker and pbmetrics, so every pass refused it
`measurement_host_not_idle`, and the consumer never got a staged byte (PQ
d54952c1fcac, movers 68fdb8728f38 and 750a4f8c65eb, 1,596 refusals in
134 s). Had admission let one through, preflight would have refused it next:
an x86_64 worker matches neither the scope's platform key nor the toolchain.
`movement_actions.seal_movement_action` now seals every mover and egress, for
pbrun, the produced-output lane and the spool alike, as `generation`,
`generic`, `portable` work with an empty toolchain
(`MOVEMENT_TASK`, `MOVEMENT_EXECUTION_SCOPE`). A measurement's isolation is
its own host's: the consumer is still refused anywhere but an idle host, and
mover traffic on the stage host while it runs is part of the design.

A mover keeps only these of its consumer's fields:

| Field | Why the mover keeps it |
|---|---|
| `task.definition_id`, `definition_version` | They name the sealing tool; `adaptive_cpu.action_identity` keys pbrun's shape on `fleet/pbrun`. |
| `task.working_directory` | Where the wrapper starts, relative to `cwd`. |
| `task.determinism` | Keeps every generation consumer's mover key. It matters only when one key publishes a second result: a deterministic mover whose log differs is then refused as a conflict. |
| `inputs`, `code_closure`, `params.cwd`, `params.checkout_snapshot` | Preflight materializes and proves the consumer's snapshot before the mover runs. |
| `params.data_manifest` | The mover copies the manifest's ranges and verifies each entry's digest against it. |
| `environment.variables` | The runtime generation's shim `PATH`; the two container-owner variables are re-derived for the mover. |
| Row `priority` | The consumer's urgency: a mover that ranked below its consumer would starve it. |
| Row `checkout_snapshot` | The materialization the sealed snapshot names. |

Its own, never the consumer's: the task class, artifact family and kind,
execution scope, toolchain, `argv` and result, command, demand, placement
(`required_tags` is the tier's host, never the consumer's tags or
`--host-class`), a mover's `retry_policy`, `max_attempts` and `retry_safe`
(#603), and its container owner. It carries none of the consumer's
`execution_timeout_s`, progress, profile, GPU or container-image
parameters. An egress, which passes no retry policy, still inherits the
consumer's retry policy and attempt limit.

### Not built here

No end-to-end campaign speedup is claimed, and none is measurable until a
consumer reads the stage.

The RAM tier's 93%-of-the-link measurement (11,866 MB/s, tmpfs over NFS/RDMA)
is a *medium* measurement, not a served claim: no consumer has yet read a
promoted range through a composed map, so no end-to-end arm of the A/B the
issue names exists. The claim this build makes is placement, identity and
occupancy; the 93% is the evidence bar the A/B has to clear, not a number it
has produced.

Reuse of a resident prefix *across artifacts* is built (#598) and described
above; what is still not claimed is a measured saving. No end-to-end campaign
number is available until a second artifact of one model runs against a stage
the first one filled, and the 731.5 GB figure is the cost of the copy that was
repeated, not evidence that the adoption avoided it.

Ram ranges are not adopted (#598's hand-over, one tier over): a finished
consumer's ram fragments become eviction candidates under pressure like any
orphan, and a successor re-promotes from the stage — a cheap copy at the
tier's own speed — rather than inheriting tokens. Nothing measured asked for
the hand-over yet.

Still not built: nothing decides *which* orphan is worth keeping when several
could be evicted. The order is the oldest receipt first, which is deterministic
and is not a ranking — there is no read-ahead model saying a later artifact is
more likely to want one range than another, and inventing one would be a
heuristic where no measurement exists.

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
records are unreadable, or selected terminal records cannot be parsed within
the 8 MiB per-record bound. Its
top-level `complete` flag covers all these cases. Terminal directory read
failures are unavailable sections; an unreadable terminal record retains its
diagnostic row and names its path in `unavailable_sections`. An oversized
record is reported as unreadable rather than decoded, so status memory
cannot scale with an action's copied output. Missing terminal
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

`pbstatus` also reports the local NFS readahead window in `host_storage`.
This optional observation never changes admission, census completeness or exit
status. Its generation helper is on NFS, so loading and observing run in the
existing bounded child after required census reads, capped at 0.25 seconds of
the remaining deadline plus cleanup grace. Failure retains unknown values and
`read_status` under `host_storage`; unreaped child identity remains in
`abandoned_children`. Explicit `--timeout-s 0` disables the bound. The helper
loader creates no bytecode, and runtime publication installs no host unit.

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

`pb_starvation` serves the starvation census through `pbstatus`'s own reader
rather than a second one, so the tool, the `--starvation` command and the
pbmetrics gauges cannot disagree about who is waiting; `pb_cursors` serves
the per-plan cursor join from the same census, adding the full consumer key
and the progress record's own timestamp for the accepted phase. Both keep
the census's own completeness under its own name beside the envelope's,
because "the mount answered" and "every record answered" are different
facts. `pb_blocked_origins` serves `pbstatus --blocked-origins`'s reader the
same way, with its completeness as `census_complete` (#926).

`pb_actions` can match `snapshot_parent` and `snapshot_commit` exactly against
the sealed `checkout_snapshot` Git fields, as well as live `checkout_root`.
Filters intersect and operate inside the existing newest-record window; exact
action keys bypass that window. Git identity is not submitter identity: several
agents can submit from the same parent, and a snapshot commit includes sealed
changes. A missing or malformed snapshot never matches a requested Git field.

`pb_action` reports a withdrawn action as withdrawn and reads the attempts its
withdrawal preserved under `attempt_history_before_withdrawal` exactly as it
reads a record's own links: `attempts_history` names the source,
`adopted_attempt` stays null, and `outcome_before_withdrawal` reports the
preserved ending's returncode. Preserved execution is evidence, never the
record's ending. The record's attempt count is untrusted input:
`unretained_attempts` is capped at `UNRETAINED_ATTEMPT_LIST_CAP`, with
`unretained_attempt_count` and `unretained_attempts_truncated` beside it, so a
forged count cannot expand the reader's work. Every retained link is checked
before its body is served -- canonical pathname, then the outcome's own schema,
action key, generation, attempt number, attempt budget and retry contract --
and a log is read only at the canonical address `attempt_log_path` derives for
that attempt, stream and digest. A record that counts attempts but can link
none is reported as missing or corrupt retained history, never as an action
that never ran.

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

### Producer-local precommit spool and asynchronous canonical export (#857)

`produced_spool.ProducedSpool` is an opt-in precommit writer lane. The producer's
sealed environment declares `PRISMABUILD_PRODUCED_SPOOL_ROOT` and
`PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES`; the source host comes from its live claim.
The root must be local disk, not network storage or tmpfs. Existing canonical
`require_prewrite` ownership and durable quota precede every local reservation.
Reservations bound the sum of live group ceilings within one owner instance.
They are not a global host disk ledger: physical space is reserved by the
consumer serializer's `posix_fallocate` on the SAME inode it writes, with a
bounded writer and truncation to actual bytes before close. A separate reserve
file followed by an unbounded ordinary write does not satisfy this contract.

After a complete local group closes, PB seals a source-host CPU1/mem1GiB action
through `movement_actions` and publishes it on the existing pool queue. Each
group is independently retryable and scheduler-owned; no application copy
thread or secondary dispatcher moves bytes. The exporter reads only the local
sources, verifies their recorded identity and the writer's digest, writes each
canonical temporary, fsyncs it, publishes the canonical name and fsyncs its
parent directory. First publication never overwrites an unexpected destination.
Local per-file proofs bind the sealed manifest, entry, writer digest and completed
canonical incarnation; a completed retry adopts those proofs with stat checks
and no shared payload read. Actual bytes must fit their canonical artifact
class's prewrite budget, never another class's credit. The ordinary temporary
becomes that same payload inode, so it does not require duplicate temp credit.
A crash after name publication but before its post-publication proof first pins
the unchanged local source, then removes only the recorded UNACKNOWLEDGED
canonical incarnation under the export lock before recopying. It never overlaps
that old copy with a new temporary, and never deletes an acknowledged canonical
file to recover. This preserves the payload/temp bound even when temp is zero.
Every source/destination ancestor is opened through the accepted no-follow
directory walk and payload I/O uses pinned directory descriptors; post-seal
symlink substitutions refuse. Unknown paths, changed bytes, incomplete groups
and corrupt records retain.

The group acknowledgement is bound to its sealed manifest and export action.
Only that verified durable acknowledgement permits existing canonical
produced-output descriptors and committed progress; local writes and queued
exports are not committed units. Existing `publish_prepaid_batch`, canonical
origin identity, strict RAM/SSD leases, retirement and restaging stay unchanged.
The first slice therefore moves synchronous origin writes off the compute
thread but does not yet provide local read leases or eliminate subsequent
strict staging/readback. Each export consumes its own ordinary CPU/memory
reservation; producer memory and local disk ceilings must leave honest capacity
for it. There is no GPU demand on an exporter.

Completed local files can be removed only against the bound acknowledgement,
reverified after taking the export lock, with a source-identity census and pinned
directory-relative unlink. Pending/failed/unknown exports and
partial writer groups retain their local bytes and reservation for recovery.
A repeated successful release is idempotent, including interruption after
unlink but before updating its reservation. Metadata remains as bounded proof;
this lane does not add an automatic orphan sweeper or change owner cancellation.

### Declared outputs enter the pool under a fill reservation and a pace (#747)

A declared output is the write-side twin of a declared input, and it gets the
same treatment: a reservation on the tier ledger it disturbs, and a movement
node in the DAG that is paced to what it reserved. The produced-spool export
is that node for producer outputs.

**What the pool pays for a write.** Measured read-only on dl380g10 on
2026-09-22 (Netdata and ZFS kstat, R10 windows):

* Memory was never the constraint. Memory PSI stayed at 0, available memory
  stayed at or above 164 GiB, and the ARC memory throttle count stayed at 0.
  Dirty data already sits inside the ARC term that RAM admission charges.
  PB therefore does not budget dirty data as a separate tier kind.
* Spindle time was the constraint. Stage-mover reads from the four-disk
  raidz1 changed with pool member writes:

  | Member writes | Mover reads |
  |---|---|
  | 0-5 MB/s | 175 MB/s |
  | 150-300 MB/s | 92 MB/s |
  | Above 300 MB/s | 19 MB/s |

  Read await doubled from 28 ms to 54 ms, and HDD busy ran at 84-88%.

A pool write is therefore paid for in displaced mover reads. The ledger that
already prices mover reads, `fill_mb_s_pool_side@<tier>`, is the one an
export reserves from.

**Off by default.** The price below is not measured, so the reservation and
the pace are both opt-in. A producer turns them on by setting
`PRISMABUILD_PRODUCED_SPOOL_PACED_EXPORT=1` in its sealed environment. When
the variable is absent, empty, or `0`, every export is sealed exactly as it
was before #747, so publishing a runtime that carries the pacer changes no
live export. Any other value is refused when the spool is built.
`submit_group(..., paced=True|False)` overrides the producer's setting for
one group, so an A/B can interleave paced and unpaced exports from one
producer. A replay keeps whatever the first submission sealed.

**Reservation.** When the export is opted in and the batch's prewrite tier
announces a fill offer, `ProducedSpool.submit_group` seals `fill_mb_s_pool_side@<tier>` into the
export's demand. It prices that demand with `storage_tiers.current_fill_offer`,
the rule pbrun's movers use. It also seals `--pace-mb-s N --pace-tier <tier>`
into the command. The publish row carries exactly the sealed demand, and a
replay keeps the price it was sealed at. A tier that announces no fill offer
leaves the export unreserved and unpaced, as before.

**Publish attribution.** The #595 gate refuses tier demand that carries no
residency block or produced-output template, because such demand names bytes
that no manifest maps. A rate kind names no bytes: `TIER_RATE_KINDS` tokens
price bandwidth while the action runs and are returned when it stops (#636).
The gate now applies only to occupancy kinds, and occupancy demand without a
range or a working window is still refused.

**Pace.** `ExportPacer` is a token bucket over the export's canonical writes,
denominated in decimal MB per second like the ledger. When the copy gets
ahead of schedule, it flushes and `fdatasync`s before it waits. Without the
flush, the NFS client would send its page-cache backlog at line rate, and the
pace would bind only on the client side. The receipt's `pacing` block
(`prismabuild.produced_spool.pacing.v1`) records the rate, the bytes, the
seconds, the seconds held, the flushes, and the file-side MB/s. On
dl380g10's `sync=disabled` dataset, `fdatasync` is acknowledged from RAM. It
bounds the arrival rate at the server, not durability.

**The price is an over-charge, deliberately.** An export reserves the tier's
whole current offer (167 on 2026-09-22), one read MB per written MB. A mover
reserves its receipts share (about 57). One export therefore takes about
three movers' worth of fill while it runs. Two things are known about the
price:

* A fit of the live member series gives k ≈ 0.38 read MB displaced per
  physical written MB. Physical write bytes are the logical bytes, divided by
  compression, times 4/3 for raidz1 parity, plus metadata. This is an
  estimate from one day's windows, not a measurement of an export.
* No export receipt prices a pool write yet.

1:1 errs toward the movers until the A/B below sizes the real price. The
pace, not the reservation, bounds what an export does to the spindles.
`probe_fill_demand` treats any oldest ready fill demand as the tier's probe
floor (#706). While an export is the oldest ready fill row, the tier mints
the ceiling plus the export's demand. The export can then run beside the
movers instead of after them. Excluding exports from the floor would bring
back the `never_fits_tier_capacity` deadlock that #706 closed, so exports
keep it.

**A producer's unheld window is an obligation the tier gate can count.**
A producer reserves its template's `window_gib` on the tier at claim, and its
batches spend the window by exact transfer. Retirement returns the spent
credits to free, and the producer takes them back with `refill_window`.
Between the retirement and the refill the window is owed but held by nobody.
The joint-fit gate (`window_credit.gate_newcomer`), the fence check
(`fence_fits`) and the relief in `window_pressure` counted it as zero
(`output_gib=0`, note `output-scope-unenforced`), so a consumer's window could
take the room the refill needs.

`produced_output.unheld_window_gib(queue, tier_id)` measures that gap. For
every owner whose live claimed row names its instance's own attempt, it
returns `window - held - outstanding`, floored at zero. `window` comes from
the filed template the instance is bound to, and `held` and `outstanding`
are the terms `refill_window` bounds itself by. Both are held tokens that the
gate already counts, so no token is counted twice. A queued owner's window is
still in its ready demand, and a finished or superseded owner owes nothing.
The census reads each owner's claimed row before any of its records, so a
dead owner's torn record costs nothing. For a live owner, an unreadable
instance, claim, template or holding makes the result unknown, and the tier
loop defers the tier as it does for an unreadable ledger
(`advance-deferred-unknown-evidence`). An unreadable batch census counts no
outstanding tokens, which can only raise the result.

When counted, the obligation enters all three. The gate and the fence check
add it to what they hold against capacity. The relief adds it to each tier's
next-phase term, because a fence needs the owed window free as well. Without
that, an admitted window's advance would wait beside reclaimable orphans. The
newcomer probe's shortfall includes it too. A tier whose obligation is unknown
asks for no relief, because its publication defers.

The tier loop counts the obligation only when it runs with `--output-windows`
(or `PRISMABUILD_TIER_OUTPUT_WINDOWS=1`). Unset, every decision is the one it
made before, with the same note. Since #907 that is true of the joint-fit
gate, the fence check and the relief only: the admission commitment charges
the owed window whatever the switch says (#905). Any other value of the variable stops the
loop at start. The supervisor restarts a role whose argv differs from its
declaration, and a role inherits the supervisor's environment, so the switch
is the `tiers` role's arguments in `tools/fleet/fleet_boxes.json`, published
like any other change, not a hand relaunch. The window, not the template's `minimum_gib`, is the charge:
`refill_window` treats a top-up as optional once holdings reach the minimum,
so the window over-counts the room a producer strictly needs, and it errs
toward the producer. An A/B that turns it on compares, against a run
without it, the `window-gated` events that name `joint-fit-stall` with an
empty `output_note` and the producer's `refill_deferred` count.

**A restaged batch's mover can reserve fill like an input mover.** A restage
(`ensure_batch_materialized`) copies a retired batch back from its origin on
the pool. The read is cold, like a consumer's input mover, but the restage
mover reserved no fill, so it read the spindles outside the ledger that
rations them. It can now reserve `fill_mb_s_pool_side@<tier>` at the price
pbrun gives a mover: `storage_tiers.current_fill_offer` over one copy's
measured rate (`mover_fill_demand_from_receipts` with the batch's manifest,
#909, filtered by the tier's `pool_identity`), capped by the tier's current
offer. The
sealed command carries the matching `--fill-mb-s-pool-side`, so the receipt
records what the claim reserved. The stage window still comes from the
producer by exact transfer. The fill is not prepaid: the claim takes it from
the tier's free fill and the stop returns it. The produced-output batch gate
(`validate_produced_output_batch`, R4) therefore reads past rate kinds on
the batch's own tier, as the #595 gate does. The occupancy term must still be
exactly the range floor, and a demand that names a second tier is still
refused.

The reservation is off by default. A producer opts in by setting
`PRISMABUILD_PRODUCED_OUTPUT_RESTAGE_FILL=1` in its sealed environment, and
`ensure_batch_materialized(..., restage_fill=True|False)` overrides that for
one call. When the variable is absent, empty, or `0`, a restage is sealed
exactly as before. Any other value refuses the restage at seal time, before
an intent is filed. A first publication never reserves fill, because it
reads what the producer has just written. A tier with no offer and no usable
receipt prices nothing, and the mover stays unreserved. The price is kept on
the materialization row (`fill_mb_s_pool_side`), so a resumed restage
republishes the demand it was sealed at: the launch refuses a row whose
resources differ from its sealed demand. Pricing reads every movement
receipt once per restage seal, the same census pbrun takes for each
submission. Only producers that opt in pay it.

**Host spool window.** A producer's local spool window can be a host
reservation, so two producers on one box cannot together overrun its disk.
Two switches turn it on, and both default off. A box declares a spool budget
with `worker_loop.py --spool-gb N`, which adds `spool_gb: N` to the host
kinds it offers. Without the flag, the box declares only `mem_gb` and `cpu`,
as before. A producer opts in with `PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW=1`
in its sealed environment. `pbrun` then derives `spool_gb` =
ceil(`PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES` / 2^30) into the sealed demand,
the same way the produced-output template derives tier demand. `pbrun`
refuses a typed `--demand spool_gb`, an opt-in with no positive byte bound,
and an opt-in on `--transport slurm`, which cannot hold a host spool. Claim
charges `spool_gb` through the ordinary host ledger like `mem_gb`, and
`ProducedSpool` refuses to start when its claimed row reserves less than the
derived amount. The per-owner byte bound and the `statvfs` check still apply.
If no box declares `spool_gb`, an opted-in producer is unplaceable, and
`pbrun` does not say so: its placement check reads a kind that an offer does
not name as unknown, not zero, so it queues the action. Every claim then
records `never_fits_capacity`, and the action stays in `ready`. A waiting
`pbrun` prints `gave up waiting` when `--wait-s` expires and cancels nothing.
Withdraw the action, or declare `--spool-gb` for a box in the roster. `pbrun`
does refuse the action at submission when every box that matches its tags
declares a smaller `spool_gb` than the demand. On 2026-09-22, sparky had
125 GB free (93% used) with a 628 MB spool, and sparklina had 411 GB free
with 30 MB.

**The roster declares each box's disk budget, and the supervisor measures it
(#910, #911).** A box's `--spool-gb` sits in its `args` in
`tools/fleet/fleet_boxes.json`, beside `--mem-gb`, so the supervisor passes it
to every worker loop it starts. Its roster value is `auto` or a whole number of
GiB, and the supervisor always hands the worker an integer: roster `args` are
the supervisor's input, not a command line to copy. Two more roster fields
define the measurement. `local_disk`, on the box, names a directory on the
filesystem the budget comes from; the roots themselves are the actions' sealed
variables, which no box knows in advance. `local_disk_free_floor_percent`,
beside `boxes`, is the fleet's disk-headroom floor, 5% of a filesystem's size.

When `supervise.py` starts, it measures `f_bavail` on `local_disk` minus the
floor, in whole GiB, and passes that as `--spool-gb`. A number in the roster
caps the measured value; `auto` does not. No measured value is written in the
roster, so freeing space on a disk raises the box's offer at its next start.
The supervisor refuses the declaration, and drops the flag, when `local_disk`
is missing, relative or not on a local disk (the same mount check
`ProducedSpool` makes), when the floor is missing, when the filesystem cannot
be read, and when the measurement is 0 GiB. It logs one line with the reason
and, for a measurement, the free space and the floor. It does not exit, because
the supervisor runs under `Restart=always` and an exit would take every loop on
the box with it; a refused budget costs the box only the disk kind. A box whose
`args` carry no `--spool-gb` reads nothing new, and its arguments are the
file's, byte for byte.

The measurement is taken only while the box's host ledger shows no `spool_gb`
held, read both before and after `statvfs`. With nothing held, every used byte
on the disk belongs to no reservation, so the measured room is exactly what the
ledger may promise. With a holder, the room is short by what the holder has
already written, and those bytes cannot be told from other use of the disk.
Counting them as used would charge the holder twice. Adding the holder's whole
reservation back, as the `mem_gb` offer does with `MemAvailable`, would credit
the part it has not yet written, which the next holder could then reserve as
well: a 200 GiB box holding one 166 GiB holder that has written 10 GiB would
offer 356 GiB and admit a second 166 GiB holder. So while any `spool_gb` is
held, the loops keep the ledger's current total, which is the last measured
value (capped by a numeric declaration), and the supervisor measures again on
each tick until nothing is held. The ledger's census is error-visible: a
holder directory it cannot list makes the supervisor wait, never measure.

A settled measurement is kept once per distinct declaration in a supervisor
process: at start, and again after a publish, which re-execs the supervisor. It
is not repeated every tick, because other writers move free space all the time,
and each new value would change the loops' arguments and cycle every idle loop.
Two limits follow. The value does not fall as other writers fill the disk
until the next start; the runtime `statvfs` check in `reserve_group` still
fails a spool producer closed when the disk is short. A refusal, or a later
roster without the flag, does not shrink the host ledger: `ensure_capacity`
only grows it, and the worker loop retires free tokens only for the kinds its
offer names, so `spool_gb` tokens minted by an earlier declaration stay until a
smaller positive value retires them.

Both GB10s declare `auto` on `/home/rob`. At 2026-09-23T03:50Z, `statvfs`
there gave sparky 138,829,066,240 B free of 1,968,362,958,848 B, which is
37.6 GiB above the floor, and sparklina 436,092,063,744 B free of
982,819,848,192 B, which is 360.4 GiB.

**Bounded local scratch draws from the same budget (#911).** An action can
write scratch to a box's local disk: a replay spill, a cotangent sink, or a
cache. PrismaQuant bounds each one with a pair of sealed variables, a root and
a byte ceiling, and its launcher refuses a root that is not an identity bind
of a local path. PrismaBuild could not see those pairs, so it could place two
large scratch holders on one box, or one on a box whose disk cannot hold it.
The action then failed closed only after it had taken its claim. An action now
lists its pairs in one more sealed variable,
`PRISMABUILD_LOCAL_SCRATCH_PAIRS=ROOT_ENV:MAX_ENV[,ROOT_ENV:MAX_ENV...]`.
`pbrun` derives ceil(`MAX` / 2^30) for each pair (`local_scratch.scratch_terms`)
and adds the sum to the same `spool_gb` kind the spool window uses, because
both use the same disk. A producer that also opts in to the spool window
reserves the window plus its scratch. The kind keeps the name #747 gave it,
so boxes, the roster and the ledger need nothing new. Claim charges it
through the host ledger like `mem_gb`, so two holders on one box can never
reserve more than its declaration together. An action whose scratch exceeds a
box's declaration records `never_fits_capacity` there and is claimed on a box
where it fits.

`pbrun` refuses at submission:
- a list item that is not `ROOT_ENV:MAX_ENV`, or a name that is not a variable
  name;
- a name listed twice, or the list naming itself;
- the spool window's own pair (`PRISMABUILD_PRODUCED_SPOOL_ROOT` and
  `PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES`), which
  `PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW=1` already charges; listing it here
  would charge the same bytes twice;
- a named variable the sealed environment does not set;
- a root that is not a canonical absolute path, or is `/`, or appears twice;
- a ceiling that is not a positive decimal integer;
- a typed `--demand spool_gb`;
- any declaration on `--transport slurm`, which has no host ledger.

Freeze derives the demand again from the final environment and refuses a
sealed `spool_gb` that differs. A pair is never inferred from a variable's
name: without the list, an environment that carries scratch-shaped variables
seals exactly the demand it sealed before, and `pbrun` does not import the
module. PrismaBuild charges the bound; it does not check at run time that a
root sits on the declared `local_disk` filesystem, or that the action stays
under its ceiling. PrismaQuant's launcher and the action's own byte bound do
both.

**Still open.**

* The export's pool-side cost is unmeasured. The receipt records file-side
  MB/s. Server-side `pool_write` stamping, which mirrors a mover's
  `disk_pacing`, is not implemented.
* The A/B that sizes the price has not run. Its primary endpoint is displaced
  mover read bytes per exported GiB. Pair paced and unpaced exports under
  the same mover load, about 20 pairs, and read member sectors from
  `/proc/diskstats` on dl380g10.
* `stage_move.movers_claimed_on_tier` does not count exports among the
  readers that share a fill measurement.

### Logical child read-manifest projection (#862)

The logical decomposer's optional `task_data_manifest` policy projects declared
reads from each PB-assigned batch into its own ordinary v1 data manifest and
single-phase staged read window. It uses the existing stage planner and tier-role
publisher, freezes all movement graphs before the first child row, and reuses
those graphs on replay. Each new logical freeze observes movement pricing
receipts once for all siblings (#867). The explicit observation is local to that
freeze: independent submissions observe again, while replay and all-empty-read
requests need no receipt census. Per-tier pricing and admission remain unchanged;
there is no global receipt cache. Empty-read cached tasks declare no bulk input. The closed
policy, identity binding, scope and client-only compatibility contract are in
[the decomposition design](design_work_decomposition_2026-09-11.md#per-child-read-manifests-through-the-existing-staging-lane-862).
