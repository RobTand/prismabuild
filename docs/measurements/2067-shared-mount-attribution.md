# #2067: shared-mount attribution before repair (O3/O4)

## Implementation and verification addendum (O1/O2/O5)

The attribution research below is preserved as prior evidence, not a claim that
this implementation was documentation-only. This change hardens only
`tools/maintenance/event_wait_baseline.py`: analysis anchors rates to the initial
PID/start identity (legacy captures use the first verified sample); counter reads
verify start identity on both sides; missing initial cmdline/stat/io remains explicit
in a completed capture, with unknown rates rather than invented zeros. Verified
cumulative endpoints can still span an unreadable intermediate sample.

Optional tracing drains nonblocking pipes in bounded chunks with a combined
65,536-byte retained budget. Overflow records `trace_output_bytes` and suppresses
the syscall rate. SIGINT, kill escalation and waits target only the diagnostic
child, with finite teardown deadlines and pipe closure even if a descendant holds
a pipe open. This remains a single-process summary, not pathname attribution.
No production candidate loop, deployment, compatibility inventory or golden changed.

Verification command on x86 Python 3.14 and ARM Python 3.12:

```sh
python -m pytest tests/test_event_wait_baseline.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

Both completed with **152 passed**, including all O1/O2 acceptance cases and four
mandatory guards. The executable trace fixture is synthetic, not a ptrace
attachment. These tests authenticate no shared-mount observation, process census,
tracing authorization or polling reduction. No live tracing, active mount probes
or fleet mutations were performed in this implementation.

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"171a559200323581a452577c4ef499e7f07997348f25a1c44f5d3372e70cf98c","size":2047}},"operation":"b7813e2a6a6ac0ac7252ccc7c6e1d84f55aee0f722dd6fd678608e7d1c141814","receipt":"3718c05ffd57314a6e89c707a3a43154acd0f60026e0ca9c90b00973bf370d98"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"6278a2a8e734aeaf19457a05bccfeb5ac901d76d2b55ed37e7e38e88f788984c","size":2048}},"operation":"4b0e000b264838be21275582d5674392d2029b53a628d02fabed5ea308f9d56c","receipt":"9833692c0377527281f6edff40cd247e3420259dfe6197ae472b0a6018778659"}]

## Prior research: disposition and evidence boundary

**Attribution is unresolved; no polling removal or measured reduction is claimed.**
The approximately **1,779 metadata operations/s per Spark** is a reported observation
in [#2067](https://github.com/RobTand/prismabuild/issues/2067), not an authenticated
measurement in this checkout. Neither its numerator, denominator, exact operation
selection, client/mount identity nor process attribution can be reconstructed from
the supplied material. It is not a baseline against which this report can claim a
percentage improvement. GETATTR, OPEN/CLOSE and LOOKUP reportedly dominate, but
writes, removals and attribute changes also occur. That mix does not establish that
all traffic is polling, or even that all traffic belongs to PrismaBuild (PB).

This is a documentation-only, attribution-first research result for O3/O4. No live
host attachment, tracing, mount probe, benchmark, test, runtime/configuration change,
remount, restart, kernel-debug change or application-code edit was performed. No
issue was filed, commented on or transitioned, and nothing was published. Existing
source is evidence of possible behavior, not evidence of deployed arguments or
traffic. The supplied prior negative directory search is accepted only for its
stated bounded scope: it did not supply the needed capture. It was not repeated or
expanded into a broad filesystem search; it does not prove that no capture exists
elsewhere.

### Inspected evidence

Source snapshot: `ebe1aa5d500a776767aa03a1c94e56134d76ad1f` (confirmed at
finalization). At finalization, initial `git status --short` showed only this
existing untracked report; the tracked diff was empty. The draft's earlier
clean-worktree statement is not the status of this finalization checkout.
References below describe source, not installed fleet revisions. The draft's
research and owner routing are preserved; provenance distinguishes those prior
reads from checks repeated during finalization.

- `AGENTS.md` and `docs/agent_execution_policy.md`: admission, evidence, safety and
  publication boundaries. Documentation review is not authorization for a live probe.
- `docs/mount_measurement.md`: per-client/per-mount RPC semantics, composition-sensitive
  aggregates, active-probe overhead and local-server asymmetry. Its historical
  September incidents are not the missing 1,779 ops/s capture.
- `docs/event_driven_waits_design_2026-10-22.md`: inventory, worker wakeup matrix,
  prewarm/tier ownership, ordered slices and validation; and the event-wait portion
  of `docs/operating_prismabuild.md`: baseline, coverage, rollout and timer accounting.
- `tools/fleet/mount_latency.py`: `_Mount`, `read_mountstats`, `_op_delta`, `_rpc_view`,
  `CLAIM_PATH_OPS`, collector sampling and `_attribution` (roughly lines 135–359 and
  808–909). `timed_probe` documents the mutating syscall leg; it was not run.
- `tools/maintenance/event_wait_baseline.py`: role/process sampling, bounded capture
  loop and optional `ewb_trace` (roughly lines 165–229, 296–342 and 389–465).
- Narrow candidate reads: `tools/fleet/worker_loop.py` `_run_loop`, including resource
  observation/`serve_once` and final idle handling (1331–1440, 1661–1730);
  `src/prismabuild/prewarm_loop.py` `_serve` (2701–2800);
  `src/prismabuild/tier_cycle.py` `_serve_cycles` (10460–10572).
- Read-only GitHub issue bodies: #2067, #1942–1959, #1894, #2062–2064, #2066,
  #2102–2107 and #2123/#2124. These establish requested owner boundaries, not
  completion, deployment or live attribution. Links use
  `https://github.com/RobTand/prismabuild/issues/<number>`.

The issue reads and narrow worker/prewarm/tier candidate reads above are retained
from the existing draft, not independently repeated during finalization. Finalization
re-read the policy, event design, operating event-wait/baseline sections,
`mount_measurement.md`, mount-latency parser/delta/rate/sampling support and baseline
capture/trace support. It performed only bounded repository reads and read-only Git
inspection; no GitHub/API retrieval or live evidence collection was attempted.

The draft records that issue retrieval used the public API after `gh` was unavailable
through the normal wrapper and the system CLI lacked authentication. The initial requested issue-body
batch succeeded; a later redundant retrieval was rate-limited (HTTP 403). Conclusions
use the successful reads, not the failed refresh. No issue-state freshness beyond
those reads is implied.

A subsequent bounded reconciliation in the same source snapshot preserved this
prior report and its owner handoffs. It rechecked repository status, the policy and
measurement/event-wait contracts, and the mount-latency and baseline support cited
above. It corrected only the optional trace's duration interpretation: helper elapsed
time is not verified attachment time. No issue refresh, live tracing, test or other
file edit was performed; the missing empirical inputs and O3/O4 disposition remain
unchanged.

### O3/O4 disposition at finalization

| Objective | Finalized here | Still blocked / not claimed |
|---|---|---|
| O3 attribution before repair | Instrument audit, explicit unavailable operation breakdown, exact capture requirements and classification rules | No authenticated #2067 capture, deployed role/child census or authorized pathname-aware traces were supplied. No measured per-process attribution or avoidable-polling share is available. |
| O4 route attributed work to its owner | Prepared owner-specific handoff and acceptance requirements below | No source has been positively attributed, no owner acceptance or external-fix determination exists here, and no repair/reduction or operational completion is claimed. |

Finalizing this report is not completion of the missing empirical work. The exact
inputs in the capture ledger are the handoff blockers, not permission to acquire
them, launch a probe, change the fleet or publish an issue.

## What the existing instruments actually count

| Instrument | Supported interpretation | Not supported |
|---|---|---|
| Selected `/proc/self/mountstats` record | Client-side, selected-mount cumulative NFS operation counters and timing sums, differenced over an identified window | PID, pathname, project or polling attribution; server physical IOPS; one combined number for all clients |
| Pathname-aware syscall sample, if separately authorized | Calls by an identified task/covered children to resolved path classes in a bounded interval | One syscall equals one NFS RPC; complete mount-wide attribution without coverage evidence |
| Baseline optional `strace -c -p PID` | Single-process syscall summary and elapsed helper duration (including startup/shutdown) | Verified attachment duration, pathnames, automatic child coverage, or per-process NFS RPC counts |
| `/proc/PID/io`, stat, cgroup observations | Process syscall-read/write counters, byte counters, CPU, start identity and scope context | NFS metadata counts; `rchar` as physical disk throughput; cgroup counters as exclusive PID counters |
| rpcdebug or other kernel diagnostics | Diagnostic events within the enabled scope, with overhead and loss limitations | Per-process RPC counts merely because a PID or client appears nearby |
| Active mount-latency probe | Timed stat/open/listdir/create/rename/unlink behavior, plus a client-wide upper bound during the probe | Passive observation, exact probe cost under concurrent activity, or proof application polling caused the traffic |

Caches, revalidation, NFS compound operations, state recovery, retransmissions and
asynchronous work break a one-to-one syscall/RPC mapping. `ops` and `trans` are
separate counters; retransmission/transport counts must not be added to operation
counts as if they were independent useful operations. Local ZFS access on the
exporting host has no NFS RPC leg: absent RPC data is not zero network latency or
zero local metadata work. Observe each Spark independently and retain the local
server arm separately.

### Parser and rate arithmetic audit

`read_mountstats(target)` selects the longest containing mount point; among equal
mount points it prefers a record carrying per-op statistics (important for autofs).
It parses operation rows only after `per-op statistics`, avoiding header rows such
as `events:`. The nine parsed fields are `ops`, `trans`, `timeouts`, `bytes_sent`,
`bytes_recv`, `queue_ms`, `rtt_ms`, `exec_ms`, `errors`. Retain the raw record, not
just the formatted headline: absent/unreadable data must remain unavailable.

`_op_delta(before, after)` iterates operations in the after snapshot, subtracts all
nine fields, drops the entire operation if any field regresses, and keeps positive
operation-count rows. A missing before row defaults to zero; it is not proof of a
new operation starting within the window. Dropped or missing rows must be reported
as incomplete/reset evidence, not silently credited as a traffic reduction.

For a valid comparable pair and actual positive duration `T`:

```
delta_op = after[op].ops - before[op].ops
rate_op = delta_op / T
rate_all = sum(delta_op for every valid operation) / T
mean_queue_op = delta_queue_ms / delta_op
mean_rtt_op = delta_rtt_ms / delta_op
mean_exec_op = delta_exec_ms / delta_op
queue_share_all = sum(delta_queue_ms) / (sum(delta_queue_ms) + sum(delta_rtt_ms))
```

The existing view emits zero for an empty aggregate mean or zero queue-share
denominator; that arithmetic convention is not evidence that missing measurements
were zero. Per-op errors and major timeouts are window counts, not rates; aggregate
counts sum the valid rows. `_rpc_view` rounds rates and timing means to three
decimals and aggregate queue share to four decimals.

`_rpc_view` totals **all** valid delta operations,
but its `by_op` output is restricted to `CLAIM_PATH_OPS`: GETATTR, LOOKUP, ACCESS,
RENAME, CREATE, REMOVE, OPEN, READ, WRITE and TEST_STATEID. **CLOSE and SETATTR are
not in that display whitelist** even though they can contribute to the total.
Consequently, summing the displayed rows is not necessarily the total, and the
standard formatted output alone cannot reconstruct the reported full mix. Preserve
all raw per-operation rows, including CLOSE/SETATTR and other non-whitelisted state,
commit, directory or recovery operations actually present. Do not relabel total RPC
operations as a metadata-only sum without documenting its exact selected set.

Collector sampling timestamps `now = time.time()` before the probe, reads mountstats
before and after it, and differences successive **after** snapshots using successive
`now` values. First samples, nonpositive windows and an age decrease produce no rate;
the age-decrease case notes a remount. This is existing wall-clock arithmetic, not
a monotonic interval exactly bracketing both counter endpoints. Variable probe time
or clock adjustment matters when authenticating a short-window rate. A future
capture must retain wall and monotonic timestamps at counter-read boundaries and
actual elapsed duration, rather than substitute nominal scrape interval, mount age
or process uptime. Existing rounded `window_s` remains useful provenance, not a
replacement for raw endpoints.

The collector's explicit remount check is age regression, not a complete
cross-sample device/mount/namespace identity check. Preserve mount source/device,
selected mount point, filesystem/version/transport/options, mount namespace, boot
identity and age at both ends. Segment on changed identity, age reset, counter
regression, disappearance or uncertain continuity, even if some counters grew.
Do not bridge a remount/restart into a synthetic low rate. Reuse the parser; this
report proposes no replacement parser or speculative code change.

`_attribution` differences counters bracketing the active probe. Its
`probe_rpcs_upper_bound` and RPC-time bound include concurrent processes on the
observed client/mount during the interval; they are not an exact deduction to
subtract from application traffic and do not include RPCs from another Spark.
Probe stat/GETATTR observations do not
establish cache effectiveness for arbitrary application paths.

### O3 operation breakdown: unavailable measurements, explicit categories

This is the operation ledger for the reported observation, **not a reconstructed
sample**. For every row, raw before/after counters, valid delta, duration, ops/s,
queue/RTT/execution means, errors/timeouts and process/path owner are unavailable.
“Reported” below refers only to the draft's #2067 account, not a verified ranking.
No row is assigned zero and no row is assigned to PB or another project.

| Operation rows | Operation-level classification only | Existing support / evidence status |
|---|---|---|
| GETATTR | Attribute observation/revalidation; may be useful work or discovery | In `by_op`; reportedly dominant; measured rate unavailable |
| LOOKUP | Name resolution, including possible negative lookup/revalidation | In `by_op`; reportedly dominant; measured rate unavailable |
| OPEN | Open/state establishment, not proof of recurring discovery | In `by_op`; reportedly dominant with CLOSE; measured rate unavailable |
| CLOSE | Close/state release; cannot infer OPEN-state retention from rate alone | Raw parsed row and all-op total only, not `by_op`; measured rate unavailable |
| ACCESS | Permission/access checks | In `by_op`; measured rate unavailable |
| SETATTR | Attribute mutation; do not drop from a read-heavy account | Raw parsed row and all-op total only, not `by_op`; attribute changes reported without authenticated row/count |
| CREATE, RENAME, REMOVE | Namespace mutation: publication, claim, probe or cleanup are possibilities, not assignments | In `by_op`; removals reported; measured per-row rates unavailable |
| READ, WRITE | Data operations; keep separate from any explicitly defined metadata-only subtotal | In `by_op`; writes reported; measured per-row rates unavailable |
| TEST_STATEID | State validation/recovery; historical storm is not this capture | In `by_op`; measured rate unavailable |
| Every other operation present in raw endpoints (e.g. directory, commit or other state operations) | Preserve its native operation name; classify only with evidence | Parsed into all-op total if valid; omitted from `by_op`; presence and rate unavailable |

Use only the existing `read_mountstats` → `_op_delta` → `_rpc_view` logic described
above to interpret an authorized retained pair; no new parser, collector or probe
is introduced. For raw operations omitted from `by_op`, the same delta/positive
window division applies when presenting the ledger, without changing the display
whitelist. Keep a separately labeled all-valid-op sum and, only if the original
finding supplies an exact operation set, its metadata subtotal. Do not allocate
1,779 across these rows or infer CLOSE/SETATTR counts from the displayed subset.

Operation class is not workload attribution. A later evidence-backed classification
must distinguish **useful application work**, **explicit expiry/renewal**,
**necessary non-notifying-source sampling**, **avoidable repeated discovery**,
**diagnostic overhead**, and **unattributed work**. The first five are candidate
categories only here: no traffic amount is established for any. At process/RPC
level the reported traffic remains unassigned, not an external-project residual.

## Exact missing captures and reconciliation ledger

All of the following are **missing for the reported 1,779 ops/s observation**. They
are requirements for a later authorized capture, not commands executed here.

1. **Original evidence:** original finding/log/table or time series with timestamp,
   host, instrument/version, operation list, raw counts, units, interval and aggregation
   method. Explain whether “per Spark” is two separate observations, an average or an
   extrapolation; whether “metadata” excludes READ/WRITE; and whether recovery and
   retransmissions are counted. No raw numerator/denominator authenticates 1,779 here.
2. **Mount endpoints:** two raw selected-mount snapshots per client with actual
   counter-read wall/monotonic boundaries, mount identity/options/namespace, kernel
   and boot identity, age and reset markers. Preserve all operation fields/rows,
   capture errors and dropped rows. Record probe/collector schedules and overlap.
3. **Deployed process inventory:** identified roles and relevant children, PID plus
   start ticks and boot/PID namespace, argv, executable/source/installed identity,
   runtime generation, cgroup and uptime at both ends. Record spawn/exit/reuse and
   unreadable/vanished processes. Source defaults are not deployed argv; a single
   parent PID is not a complete worker or subprocess tree.
4. **Bounded process counters:** matched start/end `/proc` and scope observations
   with units, elapsed duration, limits/errors and workload phase. Reuse
   `event_wait_baseline.py` role sampling rather than invent a tracing service.
   Its role records retain argv, initial/start ticks, uptime and runtime paths; each
   sample retains start ticks. Verify stable identity before deriving deltas; do not
   merge across PID reuse. Runtime paths inferred from argv may be absent, so obtain
   positive deployed-generation evidence separately. The collector expects queue-local
   watches and performs bounded bootstrap/record reads: it is not a passive remote
   NFS watcher and its own metadata cost belongs in the overhead ledger.
5. **Path correlation:** only through an existing authorized route, a short
   pathname-aware sample of identified roles and relevant children, aligned to the
   mount endpoints. Include path/dirfd or descriptor resolution, mount association,
   PID/start identity, operation/result, timestamps, count/byte caps, coverage/loss,
   attachment errors and actual duration. A `strace -c` summary cannot supply this.
   Bound tracer output, duration and lifetime; observe detach/exit rather than assume
   timeout means cleanup. Do not install tools, relax ptrace or enable debug settings.
6. **Workload and comparison:** queue/tree sizes, publications/attempt epochs,
   useful work/throughput, idle versus active state, other projects, cache warmth,
   source and published revisions, sample counts, diagnostic CPU/read/write cost and
   equivalent before/after windows. Separate cold bootstrap, steady state, fallback,
   outages and censored intervals. No matched-workload repair comparison exists here.

The baseline's optional trace is explicitly separate from latency capture; it sends
SIGINT on timeout and escalates to kill after a grace interval. Its `duration_s`
starts before launching `strace` and ends after communication/shutdown; it is not a
verified attached interval. Retain return code and stderr, and do not interpret the
helper's `syscalls_per_min` as an authenticated attachment-window rate without
coverage evidence. Neither this summary nor `/proc` data establishes pathname
attribution. Preserve unavailable/partial results honestly rather than converting
them to zero. Live access and privileges
have not been established by this research; the missing captures do not themselves
constitute a physical-action request.

A future ledger must retain, for **each full operation row**, raw before/after,
validity/reset reason, duration and rate, then separately list syscall evidence by
PID/start/path class, useful work, explicit renewal/expiry work, unavoidable sampling,
avoidable discovery, diagnostic overhead and unattributed work. Include WRITE,
REMOVE, SETATTR, CLOSE, rename/create and recovery traffic, not just the dominant
read-side rows. Cleanup, record publication and attribute updates may be legitimate;
classify them by observed call path rather than delete them to improve a headline.

Do not subtract syscall counts from RPC totals to manufacture an RPC residual.
Report unattributed mount operations as such; if no process-to-RPC correspondence is
established, the whole mount total remains unassigned at that level. Avoid double
counting overlapping traces, cgroup/child totals and probes. An external project or
kernel cause requires positive evidence, not “whatever PB did not explain.”

## Narrow source candidates: hypotheses, not attribution

| Candidate inspected | What source supports | Distinction and existing owner |
|---|---|---|
| `worker_loop._run_loop` | Resource observation/serve passes and timed idle handling revisit inputs; error/gate/drain paths also matter | #1951 owns all worker idle/error waits, #1947 dependencies, #1949 hub, #1950 maintenance. A new event wrapper or longer sleep here would duplicate conversion and may strand renewal/reaping. |
| `prewarm_loop._serve` | Gate/runtime checks and recurring cycle wait; cycles invoke selection and prewarming | #1952 owns selection/admission/pacing notifications. Data reads/writes can be useful prewarming; active disk/counter pacing is not automatically discovery polling. |
| `tier_cycle._serve_cycles` | Gate/runtime waits, cycle invocation, floor maintenance and recurring cycle timer | #1953 owns scheduling/renewal/capacity separation; #1879/#1880 own retained queue/stage discovery. Wake-on-every-hint full cycles would retain amplification. |

No inspected candidate is proven to generate any particular fraction of 1,779 ops/s.
Do not alter these loops based solely on source recurrence. A retained timer must
name a real expiry/renewal, a non-notifying numeric source or bounded failure/visibility
recovery, with bounds and cancellation; merely replacing sleep with `Event.wait`
is not event-driven behavior. Preserve original starts and absolute deadlines across
hints/reconnects, authoritative rereads, admission fences, liveness and cleanup.

## O4: owner-specific handoff and acceptance

These are prepared handoffs, **not postings or new issues**. Every recipient needs
the identity/window/path bundle above; source hypotheses alone do not meet acceptance.

| Owner / boundary | Evidence to hand off and acceptance to request |
|---|---|
| #1894 outcome waiter | Attribute claim/outcome-file recurring reads by deployed waiter identity and path; retain authoritative reads, generation/publication checks, original deadlines and outage fallback. Prove healthy bell-enabled waiting removes repeated discovery for that waiter under matched load. Do not implement another waiter here. |
| #1942 (3a) | Tier funding/commitment/reservation/claim-order readers and writes: correlate exact configured namespaces; validate mutation/replacement/removal, legacy writers and lost coverage. Complete producer coverage is required before consumer sleep. |
| #1943 (3b), refined by #2104/#2105 | Route residency/ownership/pin/released-origin paths to #2104 and progress paths to #2105. Accept configured roots, nested/moved-in/replaced/removed/in-place mutations, watch loss and conservative unknown ownership. Progress must pass existing publication validation; raw writes and heartbeats must not advance warm windows or restart leases. |
| #1944 (3c), refined by #2106/#2107 | Route prewarm/mover/export/retirement/pricing receipt paths to #2106; gang member/election/readiness/teardown to #2107. Require watch-before-scan/replay, bounded reconstruction, explicit incomplete coverage, heartbeat isolation and measured wake/metadata demand. Preserve individual readiness/confirmation/skew/lease deadlines and authoritative receipt/gang checks. |
| #2102/#2103 (slice 2b) | Mirror/pass/denial amplification goes to these existing owners, not a new cache or transport. #2102 retains dirty changes through rate limits/copy completion without a periodic copier. #2103 adds semantic bell invalidation: suppress unchanged own/timestamp feedback but retain disappearance, freshness and proof-maturity changes. Prove bounded copies/writes/wakes and no invented #1262 timeout. |
| #1945, #1946 | Send cancellation/registration races to shared waits (#1945), worker-facing resource-change notification to #1946. Require before/during/between-observation wake tests, bounded readers/threads and prompt cancellation; distinguish event-capable OOM/process exit from necessary numeric telemetry sampling. Neither item owns the broker's privileged internal observation refactor. |
| #1947/#1948 | Supply actual refusal input/deadline episodes and measured old cadence. Require all refusal families represented and original 20/30-second horizons preserved. #1948/Rob owns drain-maturity timing: preserve the measured effective old window, not the design's superseded two-second suggestion; hints must not synthesize refusals or accelerate fairness. |
| #1949/#1950/#1951 | Supply per-worker scan multiplicity, host/generation and maintenance traffic separately. Require one host hub/fair scanner, no healthy recurring ready discovery, all idle/error/drain paths covered, renewal/reaping/membership/spool/floor duties alive with workers asleep, and no offer renewal when authoritative queue health is unknown. Do not duplicate their hub or maintenance implementation. |
| #1952 | Supply prewarm selection versus useful IO versus active-pacer counts. Require phase/terminal/progress/receipt invalidation, long-read cancellation, slot/depth notifications, preserved order/protected bytes and no idle sampler or own-receipt storm. |
| #1953 with #1879/#1880 | Supply queue/residency/stage enumeration and per-entry metadata, not just JSON/physical reads. Require identical-input decision parity, incremental dirty work, separate actual-capacity sampling and liveness renewal, time-only expiry, bounded bootstrap and fresh locked deletion proofs. An incomplete index cannot authorize destruction. |
| #1954, split into #2123/#2124 | #2123 owns independently buildable identity-safe pidfd/stop/adoption/reaping work: cover PID reuse, uncertain ownership, prompt cooperative stop and whole-child custody. It does **not** complete #1954. #2124 owns hub-driven census/scheduling, measured elapsed reserve stabilization and log deadlines; require published hub plus separately owned maintenance, preserve refusal backoff, 64 MiB/8 MiB log hysteresis and seven-day last-write retention. No substitute hub and no event-count stabilization. |
| #1955/#1956/#1957 | Demonstrated TL-blocking withdrawal/progress/gang waits go to #1955 with original hard/stall/grace and gang deadlines. Generic process/profile helpers (#1956), off-by-default localtier (#1957) and optional SLURM remain visible exclusions unless a demonstrated in-scope path changes that classification. Preserve PID/start/scope identity; leader exit is not scope-empty evidence. |
| #1958/#1959 and #1881 | #1958 integrates separately merged #1929 placement with waits; it does not reimplement placement. #1959 coordinates slices, #1881 baseline/after and timer ledger. Require same-publication event-to-claim p50 ≤1 s, p95 no worse than baseline, fallback claim resumption ≤60 s, no lost/double claims, and old-generation traffic separately accounted. These are acceptance requirements, not measured results here. |
| #2062/#2063/#2064 | Keep broker syscall-read amplification separate from NFS metadata. #2062 owns file-set/bounded baseline; #2063 owns candidate-scaled GPU process identity reads; #2064 owns cgroup event observations with necessary numeric sampling. Require deterministic count evidence plus separately labeled deployed evidence, PID-reuse/cgroup-move safety, fresh authoritative stop checks and no slower safety sampling merely to lower reads. Proc read bytes are not physical disk MB/s or per-process RPCs. |
| #2066 | Retained NFSv4 OPEN states require correlated client/server state, fd/maps, kernel/mount and OPEN/CLOSE/error evidence. Probe `claim-<pid>` files and application `done/<key>.json` are distinct populations. Missing descriptors do not prove no mappings or a kernel cause. Keep state retention separate from operation rate; reuse #1894 mitigation and distinguish diagnostic-probe traffic. No remount experiment is authorized here. |

For a **proven, otherwise-unowned PB source**, the #2067 handoff must first identify
PID/start/argv/generation, exact recurring call path and paths, measured full mix,
workload and lack of overlap with the owners above. Only then reproduce it and
propose the smallest existing-cache or event-driven repair within #1917, preserving
records/admission/liveness/renewal/cleanup. Reader/writer changes require applicable
record-compatibility capture/adoption evidence; do not hand-edit goldens. Intake
retains duplicate-filing decisions. No such source is established here.

For a **proven unserved process/project or Linux/NFS defect**, prepare an external-fix
note for #1917/#2067 containing the executable/project or kernel/mount identity,
observed paths/operations/window, reproducer, exact requested external repair and
acceptance measurement, plus what PB did and did not cause. That is an exit for the
attributed portion, not permission to invent an external cause for the residual.
No external-fix conclusion is currently supported.

## Acceptance still outstanding

A repair claim needs authenticated capture, positive process/path attribution,
owner agreement and matched-workload before/after evidence showing which operation
classes changed and which legitimate work remained. Report diagnostic overhead,
residuals, resets, losses, sample counts, errors and unavailable checks. Unit fixture
success cannot substitute for shared-mount evidence. The prior documentation-only
research ran no tests; the implementation checks are recorded in the addendum above.

The existing baseline and deployment process remain with their owners: baseline
before runtime change, positive published topic coverage before consumer activation,
one consumer per generation with TL approval, canary/watch and previous-generation
rollback. Source merge is not deployed coverage. This report authorizes none of those
actions and claims neither an authenticated 1,779 ops/s baseline nor polling removed.
