Committed with #1881 (slice 0); slice issues link here.
# Event-driven waits after #1894

Design for the PB lead on dl380g10. Read-only investigation, 2026-10-22.

## Decision and scope

Replace recurring claim, prewarm, and tier discovery with subscriptions plus explicit deadlines. Extend the queue-host bell to carry missing invalidations. Combine those hints with host-local resource notifications in one wait hub per supervised host. Keep admission, records, token ownership, and deletion checks authoritative.

The bell is a change transport, not a scheduler or a database. Its current feed does not, by itself, satisfy #1879 or #1880. Retained discovery indexes must consume that feed. Stage reconciliation also needs a separate index of actual staged files and ownership dependencies. Waking the existing full cycle for every hint would retain the expensive scans and can create a feedback loop.

Rob's rule, 2026-10-21, governs every proposed slice:

> "make sure everyone/everything respects my guidances about busy waits. I want everything to be event-driven where possible and use waits only circumstances that require them."

A timeout is justified by a particular deadline or by a named source that cannot notify. Merely using `Event.wait` instead of `sleep` does not make recurring observation event-driven.

This task changes no source, Git state, runtime, supervision, or fleet configuration. The only written artifact is this document. No PB action, test, benchmark, GPU probe, live syscall trace, or measurement was run. Action keys and test counts therefore do not exist for this investigation. All validation below is proposed work for builders and TL.

## Evidence and current behavior

The inspected checkout is `/home/rob/.t3/worktrees/prismabuild/pb-1894-gate`, HEAD `e5a657758f8eae6b7606393f22f9ccb049dc5ea4`. Its working tree was clean when inspected. All source references below are relative to that checkout unless stated otherwise.

Read the requested reviewer rules, `/home/rob/prismabuild/docs/agent_execution_policy.md`, the operating guide, and `/mnt/shared/prismabuild-fleet/repo/skills/prismabuild/SKILL.md`. Other skills read were under `/home/rob/.agents/skills`. No file under `~/.codex` or `~/.claude` was opened. Inspection used bounded source reads, source-text searches, source-only AST inspection, read-only Git commands, and GitHub issue/API reads. No credentials, account state, filesystem-wide scan, or source execution was needed.

Primary issue evidence:

- [#1894](https://github.com/RobTand/prismabuild/issues/1894) defines slice 1 and reports that queue-host inotify sees Spark NFS writes. That live observation was not repeated here.
- [#1876](https://github.com/RobTand/prismabuild/issues/1876) describes recurring tier discovery and stage reconciliation overhead. Its correlation with `transition_busy` does not establish causation.
- [#1879](https://github.com/RobTand/prismabuild/issues/1879) requires incremental queue/residency discovery, recovery, legacy-writer coverage, and decision parity.
- [#1880](https://github.com/RobTand/prismabuild/issues/1880) requires incremental stage reconciliation with fresh deletion proofs. Filesystem free-space policy must continue using actual capacity observations.
- [#1881](https://github.com/RobTand/prismabuild/issues/1881) specifies matched-load before/after evidence. Baseline collection must precede runtime changes. Publication and rollout belong to TL.
- [#1929](https://github.com/RobTand/prismabuild/issues/1929) proposes idle-GPU CPU guests, bounded CPU-host deferral, eviction, and retry lineage. Remote branch `prismabuild-1929` was read through GitHub at `a57e3813c3d5056b45d29e5e3323b1a040c93adc`; it is separate from the inspected checkout and must not be assumed deployed.

The relevant flows are:

1. `worker_loop._run_loop`, at `tools/fleet/worker_loop.py:1001`, prepares generation/gate evidence, resource observations, images and interpreters, a bounded ready snapshot, and an offer before `PoolQueue.serve_once`. `serve_once`, at `pool.py:22100`, reaps when due, claims, executes synchronously, finishes, and returns `None` for both empty and refused work. That return value is insufficient to choose an event wait.
2. `src/prismabuild/prewarm_loop.py:2133` reads ready rows, ARC headroom, claimed progress/grace reservations, existing warm receipts, and the RAM-tier announcement. It warms windows, writes receipts, and sleeps after the cycle. `SelectionWatch` separately rereads liveness during long reads.
3. `tools/fleet/tier_loop.py` is a wrapper. `tier_cycle._serve_cycles`, at line 10460, runs `cycle`, refreshes filesystem-floor work, and waits out the remainder of five seconds. `ReceiptCache` already retains parsed records, listings, and fragment classifications. Discovery validation and `sweep_orphans -> stage_release.sweep -> staged_path.reconcile -> _unattributed_candidates` still visit unchanged names. `staged_path.py:3920` explicitly says every retained entry is still `lstat`-ed.
4. `supervise.py` computes child/claim censuses, adjusts the idle reserve, refreshes spool offers, maintains roles/logs, and sleeps 5 or 30 seconds. The broker samples independently. These duties must survive when workers stop revisiting them on claim polls.

### What slice 1 actually supplies

`queue_bell.py`, `queue_bell_events.py`, `queue_notifier.py`, `queue_bell_http.py`, and `tools/fleet/pbbell.py` implement the documented v1 contract:

- Outcome namespaces are `ready`, `claimed`, `done`, `failed`, `withdrawn`, `deferred`, `intent`, `attempts`, and `publication-endings`. Nested supported outcome paths map to a 64-hex action key. Progress, leases, and logs deliberately do not wake outcome waiters.
- The bell reads names, not record bodies. Watches precede scans. It follows no symlinks and confines traversal to the supported namespace shapes. Overflow or lost history resets cursors; lost required coverage stops the service.
- A random epoch and increasing sequence identify a bounded 4,096-event ring. `/v1/now` establishes a cursor. `/v1/wait` filters directories and keys, caps a request at 300 seconds, and returns events, an empty timeout, or reset. There is a shared 128-connection cap.
- Clients identify the queue path. They discover the numeric endpoint from the longest covering NFS mount source or loopback, and bound connection/response work by one absolute budget. Scratch queues cannot silently borrow production notifications.
- `action_wait.py:524-615` captures a cursor before observing records, then waits. It preserves the original patience, handles resets, and uses named 0.25-to-5-second unavailable-callback backoff. Its isolated readers and generation/ending logic remain authoritative.
- The role has singleton, signal, generation-retirement, and request-thread shutdown handling. The four existing listeners remain unchanged.

Worker offers, reservation ledgers, adaptive snapshots, residency, accepted progress, tiers, mover/prewarm receipts, and stage files are absent from v1. A worker cannot safely subscribe only to `ready` and then sleep indefinitely.

## Inventory of recurring waits

Classification: **E** means replaceable by an event, **D** means a specific moment-in-time deadline, and **S** means observations from a source that cannot supply the required callback. Mixed rows distinguish the components. Sampling periods below describe source behavior, not new permission to rescan the queue.

| Role/path | Current cadence or bound | What can change | Class | Proposed source or treatment |
|---|---|---|---|---|
| Worker empty/refused claim, `worker_loop.py:1699-1720` | Roster: dl380 10 s, sparky 15 s, sparklina 20 s. Nonempty census reduces this to 1 s | Ready work and every admission input below | E | Bell v2 plus host wait hub; remove `PRESSURE_POLL_S` claim rescans |
| Worker drain, unknown generation, failed discovery/publication, floor refusal, exception branches | `--poll-s` in several branches | Gate, runtime, source availability, resource floor, retained child | E + S | Local watches, broker state, pidfd; bounded source-failure backoff only |
| Worker offer/loop/address census | Every idle claim pass | Host capability, offer liveness, peer placement evidence | E + D + S | Host facts change notifications; separate offer-renewal deadline before 120 s expiry; refresh only on usable evidence |
| Worker capacity observer and ARC-dependent RAM roof | Every idle claim pass, retained sample-count window | MemAvailable, host CPU load, ARC policy/roof | S + E | One host sampler, local policy watch, integer-capacity or proof-state notifications; preserve observer window semantics |
| Adaptive CPU admission | Demand-driven, at most once per 1 s; valid deltas 1-60 s; freshness 5 s | CPU counters, PSI, attributed work, baseline maturity | S + E + D | One host observation source; sample while there are admission interests, plus one exact next-sample deadline when a baseline is needed |
| Adaptive GPU admission | Broker snapshot, freshness 5 s; evaluated during claims | Attribution, GPU memory/power, foreign load, learned state | E + S + D | Broker snapshot notification and freshness expiry; vendor numeric readings still sampled |
| Adaptive diagnostic mirror, `adaptive_snapshot.py` | Opportunistic publication, minimum spacing 1 s | Local CPU/GPU/denial evidence copied for peers | E + D | Watch local changes, publish at rate-limit expiry if dirty; completion event. No separate polling copier |
| Host sweep in `serve_once`, `pool.py:18920` | Once per host per 30 s | Lease expiry, orphan passes, gang state, membership settlement, `finish_pending` | E + D | Indexed dirty claims and exact expiry heap; scope-empty and terminal events. Independent of idle workers |
| Producer spool retirement | Once per host per 300 s when a worker reaches it | Dead producer and retained/exported namespaces | E + D | Producer terminal/release/export events plus retention/lease deadlines; retained namespace index |
| Filesystem-floor owner refresh, `filesystem_floor.py:104` | Throttled at 30 s; evidence ages out at 300 s | `statvfs` room, owner bindings, growth holders | S + E + D | Bounded filesystem sampler, binding/growth mutations, pidfds, evidence-expiry deadlines |
| Worker offer publication lock | Retry every 50 ms within 5 s | Sibling publisher completes | E; legacy S | Shared host publisher completion/pidfd. For an unknown older holder only, named 50 ms-to-1 s backoff within the original 5 s budget |
| Filesystem-floor lock and image-refresh lock | Floor 50 ms within 5 s; images 20 ms within refresh budget | Local critical section ends | E; legacy S | Owner completion channel or blocking local-lock helper cancellable at deadline; unknown legacy holder gets bounded backoff |
| Container image inventory | Offer TTL 30 s; claim freshness 5 s, demand-driven | Docker image availability/class evidence | E + D + S | Docker event stream invalidates; reconnect reconciles inventory; explicit evidence-expiry deadline, bounded API fallback |
| Required interpreters, dependency metadata, class/scratch policy | Checked per claim pass | Local executable, metadata or configuration replacement | E; unsupported S | Watch required files and nearest existing parents; bounded capability probe on invalidation, explicit backoff for unwatchable dependency |
| Supervisor main tick, `supervise.py:2317` | 30 s idle, at most 5 s active | Children, claims, runtime, roster, required idle reserve | E | pidfd/SIGCHLD, hub claim-state notification, local inotify, broker notification; deadline only for explicit stabilization/renewal work |
| Supervisor startup spread | Once after a spawn batch, up to 0.5 s | Deliberate launch staggering | D | One scheduled launch-deferral expiry, cancellable by stop |
| Supervisor role refusal | 5 s doubling to 300 s | Unknown singleton/port holder leaves | S + E | pidfd/lock owner where known; otherwise keep named capped refusal backoff |
| Supervisor log maintenance | Each supervision pass; size trigger 64 MiB, keep 8 MiB; worker retention 7 days | Role log size, worker log retirement | E + D | Log close/write invalidation, event-triggered size check, oldest-retention deadline; preserve hysteresis |
| Supervisor shutdown | pidfd poll timeout 1 s; census-error retry 1 s | Owned workers/roles exit; ownership evidence becomes readable | E; failure S | Block on pidfds plus signal/control fd without 1 s timeout; named 1-to-5 s census-failure backoff |
| Prewarm main loop, `prewarm_loop.py:2785` | Roster 20 s after work; parser default 10 s | Ready/claimed rows, progress, receipts, reserve/headroom | E + S + D | Bell v2 dirty rows; active/headroom-blocked work attaches to ARC sampler; grace expiry is one deadline |
| Prewarm maintenance/unknown-runtime branches | `--poll-s` | Gate or generation changes | E; unreadable S | Local gate/runtime watches; bounded read-failure backoff |
| Prewarm `SelectionWatch` | Cached recheck every 5 s, requested by reader threads | Selected generation ends, withdraws, or is superseded | E | Per-warm key/generation bell subscription, update cancellation condition; unknown evidence keeps the existing conservative verdict |
| Prewarm disk/client pacer | CLI sample 0.25 s, hold 0.25 s while blocked. Constructor sample default 0.5 s | Diskstat and NFS-client read counters | S | One sampler per active device set, bounded sampling delay/backoff at the configured bound; publish pacer verdict and depth changes |
| Prewarm reader `Admission.acquire` | Timed condition recheck using caller wait bound | Slot released, pacer depth changes, cancellation | E | Condition notify on all three inputs; remove timed condition polling |
| Tier main cycle, `tier_cycle.py:10566` | Minimum start interval 5 s; parser default 60 s. Long cycles can leave zero wait | Queue, residency, receipts, stage ownership, capacity | E + S + D | Dirty indexes, capacity sampler, explicit liveness/expiry deadlines; no periodic full cycle |
| Tier discovery of ARC/ZFS/pool pressure, topology | Each tier cycle | `statvfs`, ZFS available, ARC limits, disk pressure, mounts/devices | S + E | Lightweight capacity sampling at the existing 5 s bound when relevant; mount/device/ZFS notifications invalidate topology. Never walk stage to infer free space |
| Tier announcement liveness, `Liveness` | Checkpoints within cycles, reader freshness floor 120 s and measured horizons | Observer liveness and sample age | D | Separate renewal deadline before existing reader horizon, bounded content age; renewal does not rerun discovery or renew stale capacity evidence indefinitely |
| `localtier` supported role, not declared in this roster | Default 5 s | Resident sets, lease expiry, eviction, capacity | E + D + S | Set/lease invalidations, expiry heap, local capacity sampler; include before claiming global TL compliance |
| `pbmetrics` snapshot refresh | On `/metrics` request after 10 s cache expiry; no background refresh loop | Requested metrics snapshot and elapsed-time gauges | E + D | Incoming scrape already triggers work. Keep cache-expiry semantics; invalidate queue-derived parts from feed/index if useful; compute ages on request |
| `pbmetrics` listener loop | `handle_request`, timeout 0.1 s | HTTP request or stop | E | Selector on listener and stop fd, as the bell already does |
| Broker `ResourceMonitor._loop` | 1 s wait after each sample, so duration adds to cadence | OOM, cgroup population, GPU counters, host memory and scope telemetry | E + S | Pollable `memory.events`/`memory.events.local`, `cgroup.events`, pidfd for known exits; retain one bounded vendor/counter sampler |
| Broker health and enforcement confirmation | Health requires successful sample age below 15 s; GPU guard evaluates consecutive observations | Sampler silence, OOM/limit evidence, stable GPU over-budget evidence | D + E + S | Exact health-expiry deadline, observable OOM events and required vendor samples. A process exit alone cannot replace whole-scope enforcement |
| Broker listener and run handler | `serve_forever` shutdown tick 0.5 s; run handler selects socket/pidfd with 1 s timeout | Request, client disconnect, child exit, stop | E | Block on existing descriptors plus stop fd. Existing 10/15 s startup/termination budgets remain deadlines |
| Active worker execution checkpoint, `pool.py:21817` | Communicate timeout up to 2 s with scope, otherwise heartbeat 30 s | Scope failure, remote withdrawal, child output/exit, progress; lease renewal | E + S + D | Bell withdrawal/progress, broker scope notifications, pidfd/pipe readiness; retain telemetry sampling where needed; independent 30 s lease-renewal deadline |
| Gang start barrier, `pool.py:20223` | 0.25 s, bounded by group skew, default 120 s | Siblings claimed/ended, election/teardown, own withdrawal | E + D | Bell group/member hints; deadline only for skew and lease renewal. Gang readiness freshness 30 s and terminal confirmation 120 s are exact expiries |
| Placement and withhold checks | Revisited by claim polls | Preferred-CPU and cross-resource 20 s preferences, resident-copy 30 s preference, withhold/measurement horizons | D + E | Return next actual deadline from policy; otherwise await dependency changes. Current #1262 itself has no timeout |
| Termination/profile/contained settling and image probe exits | Rechecks 10/50 ms in `core`, `pool`, `container_images`; `_wait_for_pidfd_exit` unnecessarily reselects every 50 ms | Process/group exit, profile/status publication | E + D; unsupported S | pidfd/pipe, cgroup-empty and status-file notifications until existing grace deadline; bounded backoff only for an unobservable legacy process group |
| Profile sampling and byte-rate export pacing | Requested profiler rate, default 100 Hz; spool pacing sleeps until computed next-send time | Sampling estimate or byte-rate allowance | S or D | Profiler sampling is requested measurement. Export pacing waits for its next byte-budget deadline; both remain admitted action work |
| pbwait/synchronous pbrun after slice 1 | Bell long-poll up to 300 s; unavailable 0.25 to 5 s | Ending and handoff inputs | E + D; failure S | Keep #1894 contract. Retained NFS reader settlement needs pidfd and a deadline; remote SLURM discovery retains named bounded backoff |

Cadences are source/configuration facts, not observed live process arguments. In particular, the storage role's actual arguments say `--readers 4 --lookahead 2 --max-readers 8`; its historical `_roles_why` text describes older 1/1 tuning. This design does not change those parameters. WSL is explicitly offline. `localtier` exists in supervisor support but is not configured on these three hosts.

## Worker claim waits

### Alternatives considered

**A. Expanded hints, host-local wait aggregation, and retained discovery.** The queue host observes shared mutations, each host combines them with local resources, and existing claim policy returns its dependencies and next deadline. Durable records remain the only authority. This preserves current writers, including older ones observed by queue-host inotify. Resets require explicit bounded reconstruction.

**B. A central durable eligibility service and scheduler.** Every writer appends a mutation transaction, a shared database tracks eligibility, and the service assigns wakeups or workers. This can offer precise targeted dispatch, but introduces dual-write/crash contracts, requires migrating all writers, and risks making a derived index admission authority. It is too large for these slices.

Choose A. A retained per-role index is still required, but a new authoritative fleet-wide mutation journal is not. Use existing retained readers and mutation boundaries rather than creating parallel record representations.

### Complete wakeup inputs

Each row must become an executable acceptance case. An idle/refused claim result carries dependencies for every policy it consulted, even when that dependency is absent or unreadable. Subscribing only to the last printed denial loses dependencies and is unsafe.

| Input that can permit a later claim | Event source | Deadline or bounded fallback |
|---|---|---|
| New ready publication, retry/requeue, changed priority/publication, deferred-to-ready move | Bell action/deferred namespace hints, from queue host | Bell outage recovery, 0.25 s doubling to at most 5 s; authoritative ready read before every claim |
| Another claim finishes, releases, withdraws, is reaped, rolls back an incomplete reservation, or completes cleanup | Bell claim/terminal/intent hints **and** host/tier ledger mutations | Watch free/held/acquisition state, not just terminal records. Release can precede/follow a terminal write or happen without one |
| Peer takes/releases capacity or changes measured capacity/preferred CPUs | Bell `reservations/<host>` and adaptive-mirror hints, host-local mutation notifications | Observation expiry deadlines. A future peer offer is not a substitute for watching its current ledger |
| Peer actually refuses a portable row, changing #1262's answer | Bell peer `adaptive/claim-denials.json`, per-key `denial-transitions`, offer/capability changes | Current #1262 has no deferral expiry. It ends on peer refusal, fullness, stale evidence, or disappearance |
| Preferred-CPU/cross-resource preference expires | Policy returns the existing monotonic start plus 20 s | One deadline, never reset on hints, refusals, or reconnection |
| #1929 idle-GPU preference expires or idle state changes | Same peer/local GPU, offer, reservation, ready-GPU, and denial sources | Branch adds `TimedPreference.IDLE_GPU_HOST`, 20 s. Register before sleeping. Do not implement a fictitious 20 s timeout for #1262 |
| GPU row appears/disappears or its refusal changes whether draining can help | Bell ready and peer/local denial inputs; residency/tier events | Reevaluate #1526 and guest eviction dependencies even when the CPU row itself did not change |
| Guest withdrawal, cleanup, requeue, restartability/eviction-budget lineage | Bell withdrawal, attempts, publication-endings, claims and ready; ledger release; broker scope-empty | Existing cleanup/escalation deadlines; preserve `preempted_by`/`supersedes_withdrawal`. #1929 merge is a separate integration dependency |
| Admission CPU sample becomes fresh/complete or establishes a usable counter delta/baseline | Host observation notification and adaptive local state; peer mirror event | One next-sample deadline, at least the existing 1 s delta interval; do not wait for new ready work to seed the second sample |
| GPU sample becomes fresh, attributed, clean, or changes capacity/power proof | Broker public snapshot replacement or subscription; learned GPU-state event | Snapshot freshness 5 s. One host sampler retries unavailable vendor telemetry with a named cap of 1 s |
| Sample/offer becomes stale, making a peer preference disappear | Recorded expiry armed when reading evidence | CPU/GPU 5 s freshness, offers 120 s, relevant peer-sample validity. Staleness itself emits no filesystem event |
| Memory frees, CPU/PSI load falls, ARC memory roof changes | Host sampler, pollable PSI threshold where applicable, policy watch | `/proc` values do not provide complete threshold callbacks. Named 1 s maximum resample delay for claim-blocked resource interests; preserve capacity-window behavior |
| Foreign GPU process or vLLM load ends or changes while process stays alive | pidfd for observed exit, broker notification; vendor/host counter sampling | PID exit is only a fast path. Memory/power/load changes in a live process still require the bounded 1 s sample source |
| Tier capacity/epoch, residency plan/map/fragment, pin, funding, commitment, mover receipt, prewarm receipt, origin release changes | Bell typed tier/residency/funding/receipt/ownership topics; local pin/mover notifications | Existing record horizons and resident-copy 30 s preference deadline. Invalidate planner-derived consumers as well as explicit row residency |
| Local filesystem floor/spool headroom improves, growth reservation releases | Host capacity notification, filesystem binding/holder mutations | `statvfs` sampling remains bounded. Claim-blocked disk interests request at most 5 s delay, not a 30 s worker poll |
| Image installed, removed, retagged, class policy changes | Docker event stream and policy watch; request-time inventory validation | Reconnect/reconcile; unavailable Docker service backoff 0.25-to-5 s. Claim evidence still meets existing 5 s freshness |
| Required interpreter/dependency/profile/config becomes available or changes | Local watch on file and parent; image/runtime policy source events | Missing parents are watched at the nearest existing ancestor. Unsupported watch/API uses named capped 5 s retry, not permanent refusal |
| Maintenance gate opens, generation publishes/rolls back, roster or hostname/affinity changes | Local inotify for gate/runtime/config, broker maintenance event, mount/network/kernel notifications | No claim while identity unknown. NFS-hosted control inputs need queue-host relay coverage or named 5 s unobservable-source fallback |
| `transition_busy`, admission lock, publisher/ownership lock releases without writing records | New local owner-completion notification, pidfd where meaningful | File unlock is **not** an inotify event. An older/remote lock owner without a callback uses 50 ms doubling to 1 s within the operation's existing absolute budget; pending claim contention uses a cancellable retry deadline |
| Per-key ordering/pass sidecar or withhold changes, measurement/gang reservation clears | Bell passes/denial/claim-order/reservation/group hints, local admission state | Withhold ceiling 900 s, unmeasured measurement ceiling 180 s, or recorded horizon. Count real policy evaluations, never manufacture passes from a timer |
| Gang member/election readiness or terminal-confirmation window changes | Bell `gangs` and all member keys, including teardown/readiness records | Existing 30 s readiness, 120 s confirmation, group skew, reservation horizons are individually armed deadlines |
| Delayed cleanup/lease loss frees a claim, retained NFS child exits, source recovers | Broker cgroup-empty/termination event; pidfd; lease/claim events | Exact lease expiry 300 s and no-lease grace 30 s; named source-unavailable backoff. A retained D-state reader must not spawn replacements repeatedly |
| Previously unreadable/corrupt/absent evidence becomes readable | Watch both object and parent, coverage reset, service recovery notification | Named bounded retry for read failure while coverage is uncertain. Treat unknown as unknown, never cached empty/free |

This list includes time-based changes in inputs, not just writes. Builders must audit every `CandidateDenial`, early return, withheld row, and resource probe against it. New policy refusal variants must declare their wake dependencies. An unmapped variant uses explicit degraded backoff and emits a coverage diagnostic; it must never enter indefinite event sleep.

### Contracts and module sketch

Caller shape, illustrative rather than implementation:

```python
with host_wait_session(queue, stop=stop) as waits:
    while True:
        before = waits.arm()  # Capture all source generations before reads.
        result = serve_or_observe_once(...)  # Existing authoritative claim path.
        if result.completed:
            continue
        waits.wait_since(before, result.blocked_on)
```

Proposed types and ownership:

```python
@dataclass(frozen=True)
class RetryDeadline:
    reason: DeadlineReason
    monotonic_at: float

@dataclass(frozen=True)
class ClaimWait:
    inputs: frozenset[WakeInput]
    deadlines: tuple[RetryDeadline, ...]
    publication: PublicationIdentity | None
    coverage: CompleteCoverage | DegradedCoverage

class HostWaitHub:
    def arm(self) -> SourceGeneration: ...
    def wait_since(self, before: SourceGeneration, need: ClaimWait) -> WakeReason: ...
    def close(self) -> None: ...
```

`WakeInput` is a tagged union for action/publication, peer, tier, group, local capability, resource observation, and control state. Deadline reasons distinguish policy expiry, evidence expiry, renewal, and unavailable-source retry. Absence/unreadability is an explicit variant. Use the existing publication/attempt identity rather than introducing another notion of a run.

- Add structured blocked-claim reporting at the existing `claim`/`serve_once` boundary, including refusals before claim scanning. Keep old one-shot/result consumers working through one adapter; do not parse log text or persist an eligibility verdict into ready records.
- Have `_peer_placement` expose the actual next preference deadline. Do not reconstruct it from `yielding_s` diagnostics. Preserve starts across wakes and local loop handoffs. Mixed old/new workers keep their current policy state; converting a timer into a different fleet-wide placement rule is out of scope.
- Host wait aggregation belongs with supervisor-owned host services, in a new small module, not in privileged admission logic. One remote subscription per host feeds local Unix-socket/socketpair listeners. A standalone worker uses the same module privately. The hub never claims or reserves work.
- Hub source generations are latched. Register interest and compare against the pre-observation generation atomically, so changes during a ready scan, failed publication, or wait registration cause immediate reevaluation.
- Keep isolated NFS discovery/publication children, their 30 s/5 s budgets, retained identity caps, generation fences, and final locked admission checks. An event does not justify doing queue reads under the host admission or transition lock.
- Offer renewal must independently establish usable current host evidence and bounded authoritative queue-read health. A connected bell does not prove that the Spark's NFS reads work. Reuse the isolated-reader mechanism for a small record/namespace-health observation at that renewal deadline, without a recursive ready scan. Incomplete discovery coverage, an unreadable queue, or a retained stuck reader prevents renewal as today. This contract change needs explicit #16 fault-case coverage.
- A blocking remote bell request needs cancellation by socket shutdown when local events or stop win. Its reader owns and joins its thread. Do not create abandoned 300-second wait threads for successive local wakeups.
- Replace `--max-idle` poll-count retirement with a documented elapsed idle deadline or disable recycling under supervision. Preserve the CLI spelling during rollout, but explicitly document any conversion. A count of unrelated events is not idle duration.

There is one policy migration that cannot be hidden in the wait adapter. `drain_policy.py:371` makes ordinary draining eligible at `passes + 1 >= STARVATION_FLOOR`, currently three. Repeated unchanged polls therefore advance a policy input. Keep `passes` as actual evaluated refusals for ordering and diagnostics, but add a one-shot drain-maturity deadline as an alternative to the threshold. Proposed default: two seconds after the first qualifying refusal, matching the old nonempty-queue one-second cadence's third evaluation. At that deadline reevaluate the drain policy once with fresh evidence; do not synthesize pass increments. Preserve existing immediate measurement/GPU-first eligibility and 180/900-second ceilings. Store maturity against the existing publication/refusal episode so worker handoff does not restart it. Older `record_pass` writers reconstruct their sidecar, so a new optional field there alone is unsafe during rollout. Use separate generation-scoped deadline metadata that old writers cannot erase, or gate this policy on completion of their rollout. This is an explicit placement-policy change requiring Rob/TL acceptance and its own PR before worker polling is removed. It cannot be claimed to preserve poll-count-dependent live timing exactly. #1879/#1880 parity remains on identical recorded policy inputs.

### Bell extension and coverage

Keep `/v1/*` and its filters unchanged for old waiters. Add a versioned `/v2/now` and `/v2/wait` for typed hints. A v2 event contains `{seq, topic, id}`. IDs are canonical bounded identifiers appropriate to the topic, not assumed to be action hashes. Tier IDs, hostnames, group IDs and fragment addresses must not be squeezed into v1's 64-hex parser. Do not expose arbitrary paths or record bodies.

`/v2/now` returns epoch/sequence plus a coverage version and the covered topic set. A reset or coverage change invalidates derived completeness. A worker enables complete event waiting only after all its needed topics are positively supported. A 404, partial coverage, ring gap, overflow, lost watch, or wrong queue is degraded availability, never permission to sleep forever.

Cover these mutation families through explicitly bounded namespace adapters:

- Existing outcomes and deferred/intent records.
- `workers`, host `reservations` including adaptive mirrors and free/held/token acquisition state, passes, and denial transitions.
- `tiers`, tier ledgers, tier reservations, funding, commitments, and claim-order records.
- `residency`, `residency-plans`, accepted-progress observations, released-origin-consumers, stage-ownership holders, and pins.
- `prewarm`, mover receipts and retirement/pricing inputs, exports, and gang group/readiness/election/teardown records.

Resolve concrete paths from existing stores/configuration. Do not extend the outcome watcher's fixed depth blindly to all queue trees. Add adapters in separate small changes, with bounded traversal and an explicit coverage declaration. A new adapter captures before scanning and maintains watches for lazy/moved-in populated subtrees, deletion, in-place writes, chmod/xattrs where relevant, rename, and replacement. Watch-limit failure marks that family incomplete; it must not report full coverage.

Lease and progress traffic should use separate topics. Existing terminal waiters must not receive every heartbeat. V2 hints may coalesce duplicate IDs without changing their invalidation meaning. The bell remains names-only; semantic comparison belongs in the consumer/index.

### NFS, recovery, and herd control

The Sparks subscribe to the queue host through their numeric mount-source addresses, with the existing queue-path header. They do not put inotify watches on the NFS mount and assume remote invalidation. Local watches cover their own broker output, Docker, interpreter/config paths, and host state only.

A bell hint can arrive before an NFS client sees the new record. Preserve existing confirmed-record reads. For a changed key whose expected version is not visible, retain a dirty marker and use a named NFS-visibility backoff from 50 ms to 1 s, bounded by the existing read/discovery budget. If visibility is still unknown, enter degraded source recovery. Never consume that invalidation as proof of absence and then sleep indefinitely. New directory membership also needs explicit cache revalidation; rereading a cached `readdir` is insufficient.

Capture the cursor before the initial observation. Buffer changes during reads. On restart, overflow, epoch mismatch or ring loss, invalidate only the covered affected indexes when identifiable, otherwise all dependent indexes. Reconstruct in bounded chunks with watches already installed; replay buffered invalidations before declaring complete. Preserve resource refusals and deletion conservatism during reconstruction. Repeated immediate resets/empty replies use the same anti-spin backoff as slice 1.

On an ordinary empty 300-second renewal, renew the subscription. Do **not** run a full ready scan just because the long-poll ended. Only a due deadline, changed input, reset, or declared source failure triggers observation.

When the bell is unavailable, one host-level fallback controller rechecks with 0.25, 0.5, 1, 2, then at most 5 seconds between attempts. Network/request budgets remain bounded and stop-interruptible. Small positive jitter may spread reconnects without raising the five-second cap. This is explicitly commented as an unavailable callback service. A recovered complete stream cancels fallback. Renewed healthy connections alone do not prove dirty NFS records are visible.

Every retained repeated timed wait needs a source-specific code comment naming why that source cannot notify, its minimum/maximum backoff, its original operation deadline if any, and what cancels it. Resource-counter sampling must name its measurement interval and maximum next-observation delay. Keep the existing one-second CPU delta minimum and GPU enforcement confirmation semantics. A generic `sleep(1)` annotated as resource recovery is insufficient, and a sampler notification must not automatically rescan unrelated refused work.

Avoid two forms of herd:

1. Coalesce remote hints and wake one idle claim scanner per host. Rotate the idle scanner fairly. A successful claim allows the next idle contender immediately; a refused scan records the new dependency set. If the selected scanner is blocked by its own retained reader or incomplete local evidence, pass the scan opportunity to another eligible idle loop. Busy workers returning from execution remain able to continue.
2. Deduplicate local invalidations by source generation and meaningful dependency changes. An offer heartbeat, identical denial timestamp, pass-counter write caused by this very scan, or tier liveness rewrite must not create an endless scan/publish/wake cycle. Completeness, freshness recovery, proof maturity, and learned-policy changes still count even if a rounded capacity number stayed equal.

All three hosts may contend for one ready record. That bounded cross-host race is acceptable; rename plus existing locks still decides the claim. Do not invent a bell-owned claim token, host election, or placement decision. Avoid timer-based random wake spreading on every healthy event. First measure whether one scanner per host is sufficient.

Old workers keep polling during rollout and can claim first. Queue-host watches must observe their mutations. Keep records and v1 compatible, tolerate lost races, and count old poll traffic separately in performance evidence. Complete TL compliance cannot be claimed while old pollers remain.

## Prewarm design

Replace the 20-second full selection loop with a retained set of ready/claimed candidates, manifest references, warm windows, and protected-byte contributions. Use the queue's existing claim-order function. Each invalidation recomputes only the affected row and its ordering/budget dependencies. A row being already warm must still consume the same protected bytes and preserve lookahead behavior.

Wake on ready/requeue/withdrawal/terminal changes, accepted progress, claim grace expiry, prewarm replacement/removal, planner-derived residency, origin release, RAM-tier liveness/epoch, relevant worker-address offers, and ARC budget changes. The claim grace is `claimed_unix + --claim-grace-min`, currently 20 minutes in the roster. It is a deadline only for the paths that actually use grace, not a substitute for declared progress.

The current bell intentionally ignores progress. Extend coverage for the validated accepted-progress observation as well as the raw source that causes its validation. Raw unvalidated bytes do not advance a window. Subscribe to both action key and publication identity and reread through the existing validation functions.

Long-running reads need their own subscription while the outer selection cycle is busy. `SelectionWatch` gets a latched cancellation condition from authoritative terminal/supersession observations. Wake reader admission and pacer waits when it changes. Unknown evidence must not cancel another action's selected work. Runtime retirement and maintenance still acknowledge only a completed accounting/thread boundary; stop issuing new reads promptly, then join and finalize existing work according to that contract.

Keep one disk/client counter sampler per active device set. It samples while readers are active or blocked, at the existing configured bound, and notifies the pacer's condition when its verdict/depth changes. Slot release, depth changes, terminal cancellation and role stop all notify `Admission`. Threads no longer repeatedly ask a condition whether the pacer changed.

ARC/statvfs/NFS counters cannot fully call back. Sample relevant headroom while a candidate is blocked on it, with a proposed five-second maximum delay, and use mutation/pressure notifications for earlier checks. No eligible candidate means no ARC-triggered queue scan. Source failures use named bounded backoff. A completed warm updates the retained state directly and does not wake itself indefinitely from its own receipt.

This wait conversion does not complete #1880. ARC warming receipts and the stage tree's ownership/deletion reconciliation are different state.

## Tier discovery and stage reconciliation

### #1879: queue and residency discovery

Use the extended bell as invalidation transport and extend `ReceiptCache`, `DirectoryRecords`, `CensusIndex`, and `EpochRecords` to retain membership directly between events. Repeated directory-stamp checks across all residency namespaces are still work proportional to the tree and do not meet the issue.

Keep maps from changed address to affected consumer, mover, tier, receipt aggregate, and ordering dependencies. Maintain active ready/claimed membership and per-tier receipt aggregates. Preserve raw invalid fragment headers needed for epoch cleanup, planner-filed consumers, unknown evidence, and the current ordering rules. A cycle may reconsider multiple consumers sharing a changed capacity budget; discovery must not enumerate unrelated historical trees to find them.

The bell's finite ring is sufficient for live invalidation only while complete coverage and a valid cursor hold. Each role needs a rebuildable discovery index. Persisting a checkpoint can reduce restart work, but a checkpoint from an old bell epoch is a hint and cannot certify unchanged disk state. No new durable authoritative journal is required for these PRs. If later work demands restart with zero reconstruction, that is a separate durable change-log contract.

Bootstrap/recovery is explicit work with a bounded iterator and progress. Proposed budgets are 256 entries or 50 ms per chunk, whichever comes first, adjustable through existing configuration style and verified in PB. Install watches before enumerating each directory; include events from concurrent old writers and populated moved-in subtrees. Buffer overflow during reconstruction invalidates completeness and restarts only the necessary scope. Unreadable namespaces remain unknown. A continuously churning tree must not repeatedly restart the whole bootstrap and starve useful work.

Finish an event batch to a consistent dirty-set boundary, run affected tier work, then block on notifications or the earliest actual deadline. Own writes update the index directly. Coalesced identical announcements do not trigger another full cycle. Liveness renewal is separate from capacity rediscovery and must retain existing `sampled_unix`/content-age protections.

### #1880: actual stage contents and ownership

The queue bell alone cannot see `/stage/prewarm` or establish its complete file membership. Add a stage index owned by the tier/reconciliation process, with these inputs:

- Successful materialization and replacement at `stage_move._Copier._copy_one` and `staged_path.StagedPublisher.publish/_note_replacement`.
- Eviction/removal results, fragment cleanup, ownership/residency changes, live pins, and mover state.
- Local watch-before-scan coverage of configured stage roots, including old writers, in-place file changes, rename, unlink, metadata/mark changes, and files that no queue fragment names.

Index canonical relative path, current identity/version, classification, attributed owners, pin/mover dependencies, and uncertainty. Use adjacency maps so a changed owner or pin invalidates its files rather than every file. At startup, or after watcher gaps, reconstruct with bounded traversal and progress, then replay concurrent invalidations. The index may be retained on disk outside the stage, but stale persisted ownership is never deletion authority. It is distinct from the queue discovery index even if both use shared invalidation machinery.

Atomic publisher callbacks are a fast path, not the only evidence. A writer can crash after renaming and before notifying; local watch coverage catches it. Old writers remain observable without adopting callbacks. If a filesystem cannot provide complete local watch coverage, mark that scope incomplete and suspend index-based destructive reconciliation there until explicit bounded recovery establishes current evidence. A documented degraded callback fallback may perform bounded recovery; it must not quietly reinstate a full recursive sweep every five seconds.

On a delete candidate, reread attribution, live pins, mover ownership, unreadable marks, containment and exact identity under existing ownership exclusion. Revalidate the candidate itself, not the full tree. Replacement or failed unlink leaves it dirty and correctly accounted. Symlinks are never followed out of the configured root. Preserve fresh locked identity checks and per-key nonwaiting transition exclusion. No discovery scan runs inside a queue transition lock.

Capacity remains measured from ZFS dataset availability/statvfs with current held/staged accounting and filesystem floors. Summing indexed file sizes cannot replace that policy.

### Remaining tier timers

Keep a lightweight capacity source for ARC, ZFS available, `statvfs`, and disk-counter estimates. Device/mount/ZFS events invalidate topology immediately, but these events cannot report every numeric headroom change. The proposed sample delay remains capped at five seconds for relevant tiers, separate from queue discovery and stage reconciliation. A capacity change dirties affected tier decisions only.

Arm explicit deadlines for announcement liveness/content expiry, withhold and measurement horizons, relief expiry, pin/lease validity, resident-copy preference, and recovery. A deadline must name the policy input it revisits. Preserve current verdicts on identical recorded inputs and times. Faster observation will change live timestamps and when decisions occur, which is expected; silently changing placement, budgets, relief rules, or pass-based aging is not.

## Ordered builder slices and validation

Every row is a separate PR with its own focused PB evidence. Split an adapter row further if its producer coverage cannot fit one builder round. No builder should combine the entire worker or tier migration into one PR.

All tests, traces, counters, benchmarks, probes and GPU canaries run through the published `/mnt/shared/prismabuild-fleet/repo/tools/pbrun.py` or `tools/fleet/pbtest.py`. Tests inside an admitted action execute directly, without nested submission. Use honest aggregate CPU/memory/GPU reservations, one native thread per shard unless the workload requires more, and preserve assigned affinity. Portable work may use any eligible host. Architecture, NFS-server access, privileged tracing, or a live-GPU dependency justify a narrower test arm. Do not pin otherwise portable tests to dl380g10.

For each PB run retain the submission command, source/published revisions, action keys, exit status, logs, terminal records and CAS receipts, and report counts/skips/missing tools. An acknowledgement is not success. Never silently run a failed-to-admit validation locally. Test fixtures that start local scratch bells do so inside their admitted action and use a distinct queue root/port.

| Order | PR-sized change | Dependency on #1894 | Acceptance and PB validation |
|---|---|---|---|
| 0 | #1881 reusable baseline collector/analyzer, without runtime changes | None; collect baseline **before** changed runtime | PB collector/parity tests; TL schedules admitted matched-load dl380 baseline with short tracing separately. Record syscall/min, physical read MB/s, CPU s/hour, same-epoch publish-to-claim p50/p99, censored rows and clock assumptions |
| 1 | Versioned bell topics and positive coverage negotiation, initially covering existing outcomes | Source merge needed; publish before consumers enable v2 | PB `tests/test_queue_bell.py` plus focused protocol cases: v1 unchanged, caps, filters, reset, wrong queue, shutdown, partial coverage, overflow and moved-in trees |
| 2a | Bell worker-offer and host-ledger adapters | PR 1, published before worker rollout | PB host IDs, legacy token writer, release without terminal record, partial acquisition/rollback, moved-in holder and coverage loss. No scan under admission |
| 2b | Bell adaptive-mirror, passes and denial adapters | PR 2a, published before worker rollout | PB peer refusal changing #1262 without a ready event, mirror completion/replacement, in-place old-writer updates, same-value timestamp traffic and denial feedback suppression |
| 3a | Bell tier-ledger, funding, commitment and claim-order adapters | PR 1; publish before dependent consumers | PB each mutation boundary, non-action tier IDs, lazy/populated subtrees, rollback, retirement and lost coverage |
| 3b | Bell residency, ownership, origin-release and progress adapters | PR 3a; publish before dependent consumers | PB nested fragments and raw invalid headers, planner-derived consumer, pin/ownership mutation, replacement/removal, accepted-progress validation and heartbeat isolation |
| 3c | Bell prewarm/mover/export receipt and gang adapters | PR 3b; publish before dependent consumers | PB receipt replacement/retirement, pricing changes, group/election/readiness/teardown mutations, watch exhaustion. Measure ring/connection demand under heartbeat traffic through PB |
| 4 | Shared cancellable multi-source wait and deadline module | Source can build before publication; integration uses published PRs 1 through 3c | PB arm/read/register race cases for every source, event during blocking bell wait, deadline ties, stop, empty renewals, repeated resets, bounded network failure and no abandoned reader/thread accumulation |
| 5 | Host observation notifications in broker and local state adapters | No bell dependency for local component | PB CPU/GPU sample fixtures and real broker integration: OOM notification, cgroup-empty, pidfd reuse, lost socket, source restart, freshness recovery with equal rounded capacity. Retain vendor sampler and scope safety. Any real GPU check uses admitted GPU work |
| 6 | Structured worker refusal dependencies and actual policy deadlines, no wait default change yet | No publication dependency; combine against current source | PB audit case for every refusal/early-return family, #1262 has no timer, exact 20/30 s deadlines, sample-baseline next deadline, withhold expiry and unknown fallback. Existing admission/transition tests remain green |
| 6a | Replace dependence on unchanged polling for drain maturity with an explicit episode deadline | No bell dependency; Rob/TL accepts the timing contract before PR 8b | PB three-refusal path still works; a silent unchanged queue reaches maturity once; early notifications do not accelerate elapsed maturity; new publication resets it; handoff and old sidecar writers preserve it; measurement/GPU-first shortcuts and drain ceilings remain unchanged |
| 7 | Supervisor-owned host wait hub and idle-scanner handoff | Needs PRs 2a, 2b, 4, 5 available; production enable requires complete published coverage | PB multiworker tests: one ready record, many loops, fair scanner rotation, retained-reader handoff, stop/restart, no offer/denial self-wake storm. Verify remote connection count scales with hosts |
| 8a | Move offer renewal, claim reaping, membership and spool/floor maintenance onto hub events/deadlines | PR 7; live enable requires complete published coverage | PB silent ready queue still renews a valid offer and reaps at expiry; NFS read failure prevents renewal; finish-pending scope-empty returns tokens; membership/spool retirement survives all workers being asleep. Preserve existing ownership and budgets |
| 8b | Convert all worker idle/drain/error waits through one wait adapter | **Merged and published #1894 plus required adapters, PRs 6a, 7, 8a** | PB table-driven every-wakeup case, healthy idle with zero recurring ready discovery, backoff cap, NFS delayed visibility, reset/restart, mixed old/new claim races. Run actual dl380-local and Spark-NFS admitted integration arms. Preserve 30/5 s reader budgets, fences, ledgers and CAS receipts |
| 9 | Prewarm main/selection/admission event waits; shared active pacer notification | **Published #1894 and PRs 3a-3c, 4, 5** | PB existing prewarm order/window/progress/terminal tests plus event tests: terminal during long read, phase advancement without ready event, unknown evidence, 20-minute grace expiry, ARC release, depth increase wakes blocked reader, no sample thread when idle, own-receipt no-spin |
| 10 | #1879 incremental queue/residency/receipt discovery indexes, behind existing cycle entry | Source after PR 3c; live enable requires its published feed | PB existing steady-tier, same-tick version, transition contention and record-store mutation suites; count `scandir/getdents` and per-entry metadata, not only JSON reads. Changed/deleted/requeued/unreadable/restart/concurrent/old-writer parity against authoritative discovery |
| 11 | #1880 stage membership/classification index and publisher/removal inputs, retain fresh deletion path | Independent of #1894 for local stage component; shares PR 10 dependency maps | PB reconcile/capacity/floor suites, unchanged-tree metadata bound, replacement, in-place/mark change, pin change, crash after rename, concurrent publisher, unlink failure, symlink containment, overflow and uncertain coverage. No deletion from stale index |
| 12 | Tier event scheduler, separate capacity sampler and announcement renewals | **Published #1894 + PRs 3a-3c, 4, 10, 11** | PB identical-input `tier-cycle` decision parity, no timer-driven tree work, time-only expiry transitions, long bootstrap preserves bounded liveness, stop/drain natural boundary, no stale-capacity renewal. Count residual work and classify every retained timer |
| 13a | Supervisor main/stop event conversion | Queue-driven census needs published host hub | PB pidfd child adoption/reaping, stable idle reserve, roster/runtime changes, refusal backoff, log thresholds/retention and shutdown ownership uncertainty. Replace two-tick stabilization with a documented elapsed deadline |
| 13b | Metrics and broker listener housekeeping | No bell dependency | PB metrics requests, stop while idle, run-client disconnect and pidfd exit, joined request threads and no 100 ms/500 ms/1 s listener ticks |
| 14a | Active-action withdrawal/progress checkpoints and gang barrier waits | **Published #1894 with required topics** for cross-host inputs | PB withdrawal from remote NFS client, barrier succeeds without a tick, skew/lease deadlines, broker failure notification, progress/heartbeat separation and original hard budgets |
| 14b | Process/profile settling waits | No bell dependency for local helpers | PB scope-empty and profile relay completion, grace budgets, PID reuse, no periodic pidfd reselection and conservative unknown-group fallback |
| 14c | Supported but undeclared `localtier` event waits | Local source changes independent of bell; shared set mutations need complete queue coverage | PB resident-set mutation, lease expiry, held-token release, missing/unreadable evidence, capacity changes and restart; no idle tree rescan |
| 15 | #1929 event-wait integration and placement/guest canary | PR 8b plus #1929 **merged and published**; do not merge its placement policy here | PB exact 20 s expiry with otherwise silent queue, peer staleness/denial changes, pending GPU reclassifies CPU dependency, guest drain/requeue/eviction cap, waiter lineage. TL runs admitted GPU canary plus portable guest shards and verifies all receipts |
| 16 | #1881 after measurement, demand-3 timer audit and rollout documentation | All enabled slices published by TL; baseline already retained | PB matched-load dl380 evidence and deterministic replay parity, with old-worker traffic removed or reported separately. TL confirms all three hosts use the intended generations and callback coverage before retiring old polling defaults |

Existing issue-listed focused suites include `test_a_steady_tier_cycle_rereads_only_what_changed.py`, `test_a_same_tick_record_version_is_not_kept.py`, `test_a_busy_transition_lock_never_stops_the_tier_loop.py`, `test_pool_transition_lock.py`, `test_an_egress_census_before_the_lock_decides_nothing.py`, `test_record_store_mutation_controls.py`, `test_the_reconcile_walk_reuses_only_what_moved.py`, `test_the_reconcile_resolves_only_what_it_may_delete.py`, `test_stage_capacity_counts_the_bytes_already_staged.py`, and `test_filesystem_floor_1483.py`. Run through PB with an admitted temporary test directory such as `--basetemp="$TMPDIR/pytest"`; do not copy the issues' direct local pytest examples onto the coordinator.

The mandatory integration proof is a wakeup matrix, not a collection of sleeping happy paths. For each row: establish a refused or idle state, arm the subscription, change only that input, and prove claim/selection/reconciliation progresses without a new ready publication or periodic discovery. Include mutation before observation, during observation, and between observation and blocking. A healthy unchanged system must show no recurring queue/tree enumeration, apart from explicitly recorded recovery. Inject every source failure separately and verify bounded fallback and recovery.

Before/after evidence must retain runtime revisions, duration, queue/tree sizes, equivalent load, sample count, cache warmth and tracing cost. Distinguish `read_bytes` from `rchar`, CPU from elapsed time, cold reconstruction from steady state, and publication epochs from reused action keys. Report decisions as well as latency. Do not attribute a remaining `transition_busy` rate to tier IO without tracing that connection.

## Documentation changes when code lands

Update these existing `docs/design.md` sections with each contract-changing PR:

- Pool-ending wait text near line 174 and "Queue change hints on the queue host (#1894)" near line 9602: v1/v2 compatibility, coverage, typed identifiers, reset, cancellation and NFS visibility.
- "Preferred, overflow and adaptive CPU admission", "Physical and adaptive GPU admission", "Cross-resource placement preference", and #1262/#1526 placement text: dependency reporting, deadline ownership, sample notifications, and the separate #1929 integration.
- Worker supervision/role lifecycle, offer lifetime, maintenance/generation fencing, claim discovery, pass/withhold semantics, and lease/reaper contracts: source ownership while idle, elapsed idle/stabilization behavior, and no lost-wakeup algorithm.
- "Storage prewarm pacing": accepted-progress events, selection cancellation, grace deadlines, shared pacing sampler, and condition notifications.
- "What the tier loop reads per cycle (#992, #1004, #1027)", "A long cycle keeps its tier records alive (#1072)", and "No tier-loop pass waits on a transition lock (#1115)": dirty discovery, bounded reconstruction, independent liveness, and unchanged lock rules.
- Stage capacity, ownership/reconciliation, reader pins, and gang reservation sections: stage index scope, fresh deletion proofs, time-only changes, and event-driven barriers.

Update `docs/operating_prismabuild.md` alongside the design: published capability checks, fallback diagnostics, watcher/ring limits, bell connectivity and NFS visibility recovery, broker notification compatibility, rollout/rollback, timer inventory, collector use, and the changed meanings/defaults of `--poll-s`, `--interval-s`, and `--max-idle`. Existing flags must not silently become event-count controls. Keep `fleet_boxes.json` explanatory text consistent with the final arguments.

Emit useful counters for wakes by source, healthy empty renewals, fallback reason/duration, coverage gaps, discovery rebuild progress, coalesced hints, actual claim scans, sample age, and index metadata work. These explain a sleeping worker without logging every long-poll renewal.

## Risks and decisions reserved for Rob or TL

1. **Rollout ownership and timing.** TL must name the baseline window, publication sequence, canary windows, and rollback generations. This designer has not published or activated anything. A source merge alone does not establish callback coverage on the fleet.
2. **Operational budgets.** Rob/TL must set the live acceptance SLO for event-to-claim latency, outage recovery, bootstrap duration, bell connection/watch limits, and allowed tracing overhead. The proposed five-second degraded retry/capacity bounds and chunk budgets are design defaults to validate, not measured fleet guarantees.
3. **Scope of demand-3 completion.** The supported but undeclared `localtier` role, optional SLURM discovery, active-action/profile helpers, and legacy workers must appear in TL's final timer ledger. Rob/TL decide which supported-off components must be converted before retiring the demand. No active recurring timer should disappear from the accounting by renaming it a sampler.
4. **#1929 sequencing.** TL decides when that separately reviewed placement/retry policy becomes the integration baseline. Until then, retain current #1262 semantics and validate the future dependency adapter without pretending guests are deployed.
5. **Polling-dependent drain maturity.** Rob/TL must accept an explicit elapsed-time alternative to the existing three-refusal threshold. The proposed two-second deadline avoids reintroducing unchanged polling just to accumulate passes. Exact historical timing varied with loop count, contention and old poll cadence, so the intended fairness contract needs an owner decision.

The main engineering risks are lost inputs, incomplete watch coverage, NFS visibility lag, self-generated event storms, sample freshness incorrectly treated as a write-only property, and stale indexes used for deletion/admission. The contracts and matrix above address them directly. Any acceptance criterion without PB terminal evidence remains unverified.
