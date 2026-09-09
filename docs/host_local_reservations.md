# Host-local admission evidence: telemetry and GPU probe state (#266, second half)

Status: implemented on branch `triage/266-host-local-reservations`; measured on
dl380g10 through PrismaBuild. Refs #266, #351.

## Summary

PR #355 moved the adaptive CPU sample, learned profiles and borrowing state
from `reservations/<host>/adaptive/` on the shared mount to host-local authority
under `PRISMABUILD_BOX_STATE_ROOT`, published back to the shared path by an
independent child after admission is released. Two records with the same
meaning were still touched on the mount inside the admission critical section:

- **Holder telemetry.** `adaptive_cpu.Controller.decision` and
  `adaptive_gpu.Controller.decision` read `telemetry/<action-key>.json` for
  every reservation under `held/`. The record is written by this box's own
  sampler for this box's own action; no other box ever reads it for a decision.
- **GPU probe state.** `adaptive_gpu.Controller` read `adaptive/gpu-state.json`
  and rewrote it by rename inside the lock on every GPU decision and on every
  `reserve_probe`. The only remote reader is `pbstatus`.

Both now live under the host-local state directory and reach the old shared
paths only as diagnostic copies. Remote readers (`pbstatus`, `pbmetrics`) keep
their paths and schema, so no reader migration is needed.

This change also corrects the premise of the issue as filed. The token ledger
(`minted/`, `free/`, `held/`) and the per-holder metadata inside `held/<key>/`
are **not** host-private in meaning and do not move; the next section says why.

## What moves

| Record | Before | After | Shared copy |
|---|---|---|---|
| `telemetry/<key>.json` (live sample of a running scope) | written by the sampler on the mount; read per holder inside admission | authority at `<box-state>/<digest>.adaptive-cpu-v1/telemetry/<key>.json` (`adaptive_cpu.local_telemetry_path`); admission reads only this | still written by the sampler, after the local record, at the old path for `pbmetrics` |
| `adaptive/gpu-state.json` (probe window, plateau feedback, consumed sample) | read and rewritten on the mount inside the lock | authority at `<box-state>/<digest>.adaptive-cpu-v1/gpu-state.json`, written through the CPU controller's `write_state`, which marks the state dirty | copied by the #355 publisher child after the lock is released; stamped `_snapshot.source=host-local` |
| per-holder `.adaptive.json` second read | read once in `decision`, again inside `cpu_allocation` | read once; `cpu_allocation` accepts the metadata already read | unchanged (stays on the mount, see below) |

`ResourceScope` gains an optional `authority_path`. `write_telemetry` writes it
first and the shared `telemetry_path` second, so a reader of the shared copy
never sees a sample the host has not already credited. The three places the
pool constructs a scope (`_start_resource_scope`, `_scope_from_record`,
`_recover_resource_scope_creation`) derive the same authority path from the
action key, so a restarted worker loop that recovers a scope samples into the
file admission is already reading.

A reconstructed scope also restores cumulative process I/O from this local
file, retaining counters saved before a failed shared copy. Only a matching
attempt nonce is accepted. Missing, unreadable or malformed local state starts
without prior counters; it never falls back to the diagnostic copy. Scopes
without an `authority_path` continue restoring from `telemetry_path`, including
the separate late-cleanup archive below.

A `scope_only` cleanup (a late finisher for a key whose successor attempt is
live) sets `authority_path = None`: its final sample goes only to the shared
`telemetry/attempts/<nonce>/<key>.json` archive, never into the local record
the live successor owns. After a scope is released, the local authority file is
removed; the shared copy remains as the attempt's last observation.

**Non-goal: the telemetry copy is not routed through the publisher child.**
The sampler writes the shared copy directly, as it did before. The loop that
samples is the heartbeat loop, and it renews the lease on the mount every
iteration, so a stalled mount stalls that loop whether or not the telemetry
copy goes through it; a publisher child per second per box would be new cost
with no gain inside the critical section. `gpu-state.json` is different: it was
written *inside* the lock, so it takes the #355 path.

## What stays shared, and why

The claim is the queue. The `ready/` scan now runs outside host admission after
a brief busy check; the lock is reacquired before candidate decisions. A scan
that finishes late cannot displace an intervening claimant, and the moved
record is revalidated before its reservation is committed. The scan still uses
the shared mount and has no I/O deadline. An adaptive loop with an empty READY
snapshot returns before shared capacity reconciliation, so an idle poll cannot
hold admission in that prelude ahead of a sibling's newly arrived work. Every
nonempty candidate pass still reconciles capacity before making a decision;
the worker's separate offer refresh and capacity clamp are unchanged. This does
not remove capacity I/O from nonempty passes or bound an NFS call.

The per-key transition lock, the claim
intent, the `ready/` to `claimed/` rename and the lease are how every other
box learns what this box took; they cannot be anywhere but the mount.

The token ledger stays shared because it is cross-host evidence, not a private
cache:

- `resolve_claim_holder` walks every host's `held/<key>` and treats the unique
  committed reservation as the exact owner of a claim whose record was lost;
  two committed holders are a contradiction it reports rather than resolves
  (`docs/design.md`, "Multiple committed holder ledgers found during that
  recovery are a contradiction").
- The reaper, `sweep_widowed_leases`, `sweep_finish_tombstones` and
  `_defer_unstarted_claim` release a dead holder's tokens on the holder's
  ledger from whatever box runs the sweep. `sweep_stale_acquisitions` says so
  in its docstring: a foreign write there is the established shape.
- `_defer_fallback` reads other boxes' `free/` and `cpu-map.json` to decide
  whether to wait for a better-placed host. Preemption reads the ledger the
  same way.
- `execute` re-validates its CPU allocation against the held ledger before
  launching.

Moving tokens to host-local authority would change every one of those
contracts, which this issue forbids. Per-holder metadata (`.adaptive.json`,
`.gpu.json`) lives inside the token directory and is unlinked by the same
foreign release, so it stays with the tokens. What admission does about it is
read it once per holder instead of twice.

CPU action identity and the GPU action contract (`adaptive_gpu.action_contract`)
still read the sealed request from the CAS on the mount. The pool now resolves
these immutable facts before candidate admission, retaining per-key exclusion,
and passes them into the controllers without a locked request re-read. Host
samples, holder accounting and resource acquisition remain inside admission;
a delayed request read cannot grant capacity. Standalone controller calls can
still resolve their own request facts. Request reads have no I/O deadline, so
this removes another host-lock stall path without bounding the whole claim.

## Recovery and ownership

**Worker-loop restart.** The box-state directory survives a loop restart. New
controller instances read the same local `gpu-state.json` and the same local
telemetry directory; a recovered scope (`_scope_from_record`) writes to the
same authority path. Nothing is imported from the shared copies: the #355 rule
that cold local state trusts no diagnostic copy applies to both records.

**Cold host after reboot.** `gpu-state.json` is absent, so
`low_samples` restarts at one, there is no `power_feedback`, and the consumed
sample markers are gone. Probing a non-empty GPU needs two continuous fresh
samples again, which costs one extra admission pass. The consumed-sample
markers guard against re-spending one broker sample; a reboot also restarts the
broker, whose sample ids are new, so the guard's absence cannot be exploited by
the old sample. Cold telemetry is a missing record, which the existing validity
predicate treats as "attribute the full reservation" -- the conservative
answer.

**Local state lost without reboot.** With `PRISMABUILD_BOX_STATE_ROOT` unset,
the default is `/tmp/prismabuild-admission-<uid>`. Losing `gpu-state.json`
forgets the pending or plateau `power_feedback`, its power window and consumed
sample markers even if the GPU holders and broker are still running. The shared
snapshot cannot restore them. The first valid low-power sample rebuilds
`low_samples` as one; rereading that same `sample_id` cannot advance it. A
second continuous fresh sample can reopen probing once holder attribution,
settling, memory and pressure checks pass. Thus lost state can discard a
previous plateau decision; conservative attribution does not preserve learned
feedback. Missing local holder telemetry blocks GPU sharing and grants no CPU
lending credit until usable local samples return.

This describes loss of adaptive records, not safe deletion of the root while
workers run. The root also contains permanent admission lock inodes: clearing
or changing it can let a new loop lock a replacement while an older loop holds
the original. Preserve the root while any loop or publisher can use it. A
configured persistent host-local root must be shared by all loops on that box;
changing it requires quiescing its users first. The current default remains
under `/tmp`; a persistent default and cleanup of abandoned local telemetry
remain #266 follow-ups.

**Disagreement.** Admission never reads the shared copies, so there is nothing
to reconcile and nothing wins: an edited or stale shared record changes no
decision, and the next publication overwrites it. Stale *local* telemetry
cannot grant credit either: the predicate requires `complete`, freshness within
`MAX_SAMPLE_AGE_S`, `sampled_unix >= admitted_unix`, an action key equal to the
holder, and (GPU) a nonce and scope unit matching the broker's live job list.

**Tokens.** This change mints and releases nothing new. `ensure_capacity`
mints by marker only; release happens through `finish` and the sweeps, all of
which resolve the holder through the shared ledger as before. A host that
cannot prove a token is its own still refuses admission rather than admitting
(#267's non-blocking refusal is unchanged).

## Crash consistency of the local files

- `gpu-state.json` is written through `adaptive_cpu.write_json`, which is
  `materialize._write_json_atomic`: `O_EXCL` temporary, `fsync`, `os.replace`.
  A crash leaves either the previous or the next complete record.
- Telemetry is written through `resource_scope._atomic_json`: temporary plus
  `os.replace`, no `fsync`. A record lost to a crash is a missing or stale
  sample, which admission already refuses to credit; durability buys nothing
  there and the sampler runs every two seconds per running scope.
- The directory is `local_state_base`: mode `0700`, owner-checked, never
  unlinked while workers run (same operating rule as #355).

## Telling a stale snapshot from a dead host

A remote reader has two independent timestamps and must use both:

1. The box's offer under `workers/<host>.json`, aged against
   `pool.OFFER_TIMEOUT_S`. `pbstatus.read_pool` reports the node `live` or
   `stale` from this alone. A live offer means the loop is announcing; it says
   nothing about the publisher.
2. The record's own `sampled_unix` inside the snapshot. `_admission_sample`
   classifies `fresh`/`stale` against the controller's `MAX_SAMPLE_AGE_S`,
   never from the copy time.

| offer | snapshot | reading |
|---|---|---|
| live | fresh | admission evidence is current |
| live | stale or absent | the box is up; its publisher is behind or blocked -- inspect `publisher-owner.json` / `publisher-result.json` under the box-state directory (#355 procedure) |
| stale | any | the box is not announcing; the snapshot is history, however recent its copy time |

The `_snapshot.copied_unix` stamp on `cpu-sample.json` and now
`gpu-state.json` tells the reader when the copy was made; it is not used for
freshness.

## Deployment

Same rule as #355: drain the queue and confirm every worker loop on a box runs
this generation before resuming work. A box running one loop that reads
`gpu-state.json` from the mount and one that reads it locally has two probe
authorities and can probe twice on one sample. Rollback needs every publisher
to have exited, or a late copy overwrites state a legacy loop is again
treating as authority.

## Measurement

Tool: `tools/fleet/admission_shared_io.py`, run through `pbrun.py`, pinned to
dl380g10 (`--tag dl380g10`, priority -10, `cpu=1, mem_gb=2`). It builds a
private queue root under `/mnt/shared/pb266b-harness/runs/` (the live fleet
mount, not the live queue), points `PRISMABUILD_BOX_STATE_ROOT` at a private
local directory, admits 8 holders, and runs 20 marked `claim` passes under
`strace -T -y`. Every traced syscall between the markers around
`PoolQueue._claim` is classified by the path it names. Workload: 8 holders with
fresh telemetry, exactly one candidate in `ready/` per pass, one admission and
`finish` per pass (CPU) or one probe decision per pass (GPU). The broker sample
and the action contract are patched as the unit tests patch them, so the count
is the claim code's own file I/O.

| workload | generation | PB action | shared syscalls per pass (steady state) | shared syscall time per pass (mean) | `_claim` wall per pass (mean) |
|---|---|---|---|---|---|
| CPU: 8 holders, 1 candidate, admission + finish per pass | before | `66308deb1b58` | 561 | 7.4 ms | 35.3 ms |
| GPU: 8 holders, 1 candidate, probe decision per pass | before | `b6abd958bc3e` | 740 | 10.8 ms | 45.3 ms |
| CPU: 8 holders, 1 candidate, admission + finish per pass | after | `f639a284dad3` | 473 | 6.5 ms | 31.6 ms |
| GPU: 8 holders, 1 candidate, probe decision per pass | after | `895f5dc3971f` | 570 | 9.8 ms | 43.0 ms |

Steady state is passes 4 to 20; the first passes carry one-time directory
creation and, on the GPU workload, the cold `low_samples` refusal. The CPU
path lost 88 shared syscalls per pass, 11 per holder: one telemetry read and
one metadata re-read, each an `openat`/`fstat`/`read`/`lseek`/`close` chain.
The GPU path lost 170 per pass: about a dozen for the `gpu-state.json` read and
`mkdir`/`openat`/`write`/`fsync`/`rename` write chain, which is per pass, and
the rest, about 20 per holder, for both controllers' telemetry reads and the
metadata re-read. Local syscalls per pass rose from 34 to 82 (CPU) and 34 to
164 (GPU); they are on the box's own disk and are not what another machine can
stall. The syscall *time* deltas were measured on a healthy mount with
microsecond round trips; the count is the claim, because the failure mode this
addresses is a stall per operation (#234, #351), not a slow mount.

Evidence: `/home/rob/tmp/pb-266b-evidence/{before,after2}-{cpu,gpu}.json` (full
per-pass classification and the strace syscall histogram) and the matching
`.log` files with the PB receipts. The publisher child's own copy to the mount
runs after the lock is released and is outside the markers by construction.

The syscalls that remain are the queue: `ready/` scan and record read, the
transition lock, `claimed/` listing, token renames, intent, rename, lease,
`commit_acquire`, and one metadata read per holder. This is not a bound on the
whole claim operation and does not repair the NFS fault (#234); it removes
the host-private reads and the one write that had no reason to be there.

## Line references

These quotations make `tests/test_design_doc_line_references.py` fail when the
named code moves, rather than when a line number changes.

| where | the line it names |
|---|---|
| `adaptive_cpu.local_telemetry_path` | `    return local_state_base(base) / 'telemetry' / f'{action_key}.json'` |
| `adaptive_gpu.Controller._write_state` | `            self._publisher.write_state('gpu-state.json', state)` |
| `adaptive_snapshot` | `STAMPED = ('cpu-sample.json', 'gpu-state.json')` |
| `resource_scope.ResourceScope.write_telemetry` | `        """Refresh the host-local authority first, then the shared copy."""` |
| `pool.PoolQueue.cleanup_action_containers` | `                scope.authority_path = None` |
| `pool.PoolQueue.cleanup_action_containers` | `                scope.authority_path.unlink(missing_ok=True)` |
| `pool.PoolQueue.sweep_stale_acquisitions` | `        ``reap_stale`` already releases a dead claimant's tokens from whatever` |
| `pool.ResourceLedger.cpu_allocation` | `                       metadata: Mapping[str, object] \| None = None) -> dict:` |
