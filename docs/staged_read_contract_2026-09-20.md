# Staged-read contract: allowed tiers, leases, and readiness (target + status ledger)

Status: TARGET CONTRACT with honest per-requirement status in
`staged_read_requirements_2026-09-20.json`. This document is normative for
what it names and silent otherwise. It claims no deployment by prose: every
requirement carries `status` (proposed / implemented / validated / deployed /
workload-proven) in the ledger, and prose `IMPLEMENTED`/`DEPLOYED` notes cite
the enforcing code and test. PB owns all admission, placement, stage
movement, retries, and cleanup. PQ declares work, read sets, and progress,
and consumes. No parallel dispatcher, cache, preload, residency, or
scheduler is created here; no application steering of placement. The PQ
endgame (Stage A → quanta → equality gate → join → allocation → export →
served) is an application acceptance boundary that consumes this contract;
it is referenced, not re-specified, and it imposes no new PB scheduler
responsibility.

## 1. Scope and authority

- SC-01: PB decides which action runs where and when (admission, placement,
  claim, retry, withdrawal, cleanup). PQ submits rows with declared inputs
  and reads the verdicts.
- SC-02: PB moves bytes (movers), publishes tier residency, and retires it.
  PQ names byte ranges and reports durable progress. Neither side duplicates
  the other's job: a second mover path, cache, or placement knob is a
  violation of this contract, not an implementation of it.
- SC-03: This document is a target plus a status ledger. A MUST whose ledger
  status is `proposed` is owed work, not a claim. Root merges validated
  changes; no merged contract change completes a campaign — only the
  application gates in §10 do.

## 2. Typed identities

- ID-01 action: `action_key` (content hash of the sealed action). Identity of
  *intent*, never of outcome.
- ID-02 attempt: `(action_key, nonce, scope_id)`. Retries mint new attempts;
  results are keyed per attempt and adopted per action only through the
  rules in INV-08.
- ID-03 manifest wire identity: SHA-256 over the sealed file bytes
  (gzip member included where the manifest ships gzipped). What pbrun seals
  into the action key and what the dispatcher binds on the wire.
- ID-04 manifest canonical identity: SHA-256 over `canonical_json` of the
  decoded document (sorted keys, fixed separators, UTF-8, no trailing
  whitespace beyond the single writer newline excluded from the digest
  input). Decoded-compare goes canonical-vs-canonical via the one producer
  seal function; raw-bytes-vs-seal comparisons are forbidden (they refuse
  valid writer output).
- ID-05 payload identity and trust mode: every materialized payload names
  `{producer_digest, manifest_wire_sha256, manifest_canonical_sha256,
  trust: staged-verified | pool-declared}`. `staged-verified` means each
  byte was read through a held lease on a pinned range (INV-05, INV-06);
  `pool-declared` means bytes came from the declared path and the payload
  is ineligible for staged-only acceptance. The mode travels in the
  payload; a consumer that requires staged input refuses
  `pool-declared` without opening it.
- ID-06 tier epoch: `(tier_id, epoch)` from the pool's tier record. All
  residency statements are epoch-qualified; an epoch change invalidates
  outstanding readiness (SM-02 `retiring`).
- ID-07 range: half-open `[offset, end)` in manifest byte space, mapped to
  exactly one staged object. Ranges tile without overlap; a cut through an
  entry is refused at seal.
- ID-08 lease: `(lease_id, holder_action_key, tier_id, epoch, range,
  acquired_unix)`. The unit of "these bytes stay here while I read".
- ID-09 worker/runtime generation: `(runtime_commit, generation_id)`.
  Behavior is attributed per generation; a source merge alone establishes
  no deployed support.
- ID-10 completion vs correctness: a terminal record (`done/`, status
  `executed`) attests the action ran to exit 0 under the named generation.
  It attests nothing about payload bytes. Artifact correctness comes only
  from identity checks (ID-04/ID-05) and the application gates (§10).

## 3. State machines

States map to existing PB fields only; no competing runtime control is
introduced. Durable record per transition is named; any transition without
its record is refused.

### SM-01 action lifecycle

| From → to | Actor | Precondition | Effect | Durable record | Failure |
|---|---|---|---|---|---|
| submitted → waiting | submitter | sealed action, manifests bound (ID-03/ID-04) | row visible in `ready/` | sealed request in CAS | malformed seal → never published |
| waiting → admitted | claiming worker | tokens fit (INV-01); declared initial working set ready (INV-02); runtime supported (ID-09) | `ready/` → `claimed/` rename under queue discipline | claim record + lease intent | shortage → denial naming tier/shortage; no pass recorded |
| admitted → running | worker loop | leases acquired (SM-03) | payload executes | attempt `(nonce, scope_id)` | lease refused → back to waiting with reason |
| running → terminal-success | worker loop | exit 0 AND payload identities verify (ID-04/ID-05) | record in `done/`, status `executed` | terminal record + CAS receipt | identity mismatch → failed, bytes unpublished |
| running → terminal-failed | worker loop / reaper | nonzero exit, timeout, or identity refusal | record in `done/` failed or `failed/` | terminal record + log tail | — |
| any → withdrawn | coordinator / supersede | plan revision or duplicate | row leaves contention without verdict | `withdrawn/superseded/` drop | a drop is never read as a verdict |

### SM-02 materialization (per staged range)

| From → to | Actor | Precondition | Effect | Durable record | Failure |
|---|---|---|---|---|---|
| absent → copying | mover | tokens reserved for range ceiling; source readable | bytes copy to temp beside final name | mover row receipt (started) | overrun vs reservation → `residency_overran_reservation`, refused before copy |
| copying → published | mover | digest matches manifest entry; length == range length | atomic rename into place; map names it under epoch | `movers/` receipt via `record_move` + map fragment | mismatch → delete temp, range unpublished |
| published → retiring | tier loop / egress | no live lease covers the range (INV-06: last reader incl. pending copy handoff) | fragment dropped, tokens released | fragment removal + token release | live lease → eviction refused, range stays |
| retiring → absent | tier loop | fragment gone, tokens released | range unstaged | ledger state | — |
| any → absent (epoch) | tier loop | epoch change | all prior readiness invalid | new epoch announcement | readers re-verify, never assume |

### SM-03 reader lease

| From → to | Actor | Precondition | Effect | Durable record | Failure |
|---|---|---|---|---|---|
| — → acquisition | consumer | range published under current epoch; request names ID-07 + ID-06 | lease ID-08 issued, pin held | lease intent beside holder record | unpublished/stale-epoch → wait with reason (bounded, §5) or clear fail; never pool fallback for bulk input |
| acquisition → use | consumer | lease held | actual reads instrumented per tier (INV-04) | per-tier byte counters | forbidden-tier open → fail before payload bytes |
| use → release | consumer | reads complete | pin released; range becomes evictable | progress/lease release record | crash → reaper releases via stale-lease path; bytes never trusted without re-verify |

## 4. Invariants (MUST, stable IDs)

- INV-01 admitted resources fit aggregate real lifetimes: a claim's CPU,
  memory, GPU, and tier tokens fit the box and tier ledgers simultaneously;
  a claim that reserved on a tier is concluded on both ledtger sides via
  one release helper. (IMPLEMENTED: tier reservation paths; ledger cited
  in design §"Cluster-scoped storage tiers".)
- INV-02 claim requires the declared initial working set ready: every lead
  movement node has a `done/` `executed` record for the same manifest the
  consumer names, still token-pinned (adoption counts; `cache_hit` movers
  that moved nothing do not). IMPLEMENTED + DEPLOYED: `PoolQueue.
  residency_verdict` gates admission; denials `residency_lead_not_resident`
  (may arrive) vs `residency_lead_terminal` (never will).
- INV-03 actual reads only allowed tiers: GPU-consumed bulk input opens
  only RAM or explicitly-allowed SSD staged objects. HDD (declared pool
  path) opens for bulk input are forbidden. STATUS: proposed (missing —
  current readers fall back to pool; see §9 G1/G2).
- INV-04 tier of every actual read is instrumented: the open path records
  which tier served each byte before payload bytes are trusted; counters
  checked after a read do not retroactively authorize it. STATUS: proposed
  (current `bytes_from_pool` accounting observes but does not gate —
  rejected as enforcement).
- INV-05 prefer valid RAM, then explicitly allowed SSD: lease acquisition
  tries the RAM leg first within the announced epoch; SSD serves only
  ranges the plan explicitly permits; anything else waits or fails.
  STATUS: proposed (RAM leg exists `--residency-ram auto`; preference rule
  missing).
- INV-06 stale epoch, corrupt, or missing data never falls back to HDD:
  stale-epoch reads refuse; digest mismatch deletes temp and unpublishes;
  missing ranges wait boundedly with reason or fail clearly. STATUS:
  proposed (map refuses whole on mismatch — IMPLEMENTED; pool-fallback on
  miss is the gap).
- INV-07 copy/publish/lease/release serialize safely: rename-before-
  fragment (temp beside final name, atomic rename); claim handoff ordered;
  simultaneous egress serialized with shared-path ownership; last reader
  including pending copy handoff blocks eviction. STATUS: implemented for
  rename/egress paths under review (shared-path worker lane; this document
  changes none of it).
- INV-08 retries never double-count: new attempts mint new nonces; results
  adopted once per action; checkpoint adoption verifies identity/trust
  without recompute. STATUS: implemented (journal re-verify grammar).
- INV-09 durable units monotonic and once: progress counters cumulative;
  published units never retracted; replays are idempotent. STATUS:
  implemented (progress channel + checkpoint journals).
- INV-10 failed/gapped outputs never publish campaign success: gapped
  joins exit advisories; consumers refuse gapped payloads. STATUS:
  implemented in joiner contract (application side; referenced).
- INV-11 eligibility is any-gb10-class, never AND-of-both-hosts:
  class-tag conjunction semantics stay as implemented; host-pair tags that
  admit neither box are a plan error refused at dispatch. STATUS:
  implemented (dispatch conjunction rule).
- INV-12 checkpoint adoption checks identity/trust without recompute:
  adopted ranges verify manifest binding + digest + epoch; adopted trust
  mode caps at the source's mode (adopted `pool-declared` never becomes
  `staged-verified`). STATUS: proposed.

## 5. Progress vs read frontier; fit and bounded waits

- PRG-01: durable compute progress (units committed, checkpoints journaled)
  is NOT proof that async readers released prior bytes. Eviction needs the
  lease state (SM-03), never the progress counter alone.
- PRG-02: the exact read plan names forward and reverse repeats and the
  source/render/activation legs. Lookahead (prefetch depth × chunk) fits in
  RAM/SSD budget plus compute memory *before* admission of the window; the
  plan refuses an infeasible window at seal with `unsupported-workset`
  naming the overshoot.
- PRG-03: a later phase whose window is not yet resident waits boundedly
  with a named reason (`residency_lead_not_resident` + owning mover);
  the wait is bounded by mover retry/timeout policy, never circular:
  a consumer holds only its current window while waiting (no
  hold-and-wait on the next window's tokens).
- PRG-04: EITHER the reservation policy proves at least the next feasible
  window fits (admit), OR the plan is terminally refused as
  `unsupported-workset`. There is no third state that silently streams
  the missing window from HDD.
- PRG-05: no requirement that a whole working set (or any named GiB
  total) fit RAM. Windows, not totals, are the unit of fit.

## 6. Dev mode

- DEV-01: giant input/output resealing and per-unit source walks are
  disabled in dev mode. Reuse existing identity/change-detection evidence
  where valid (CAS digests, sealed manifests, tier receipts).
- DEV-02: missing or changed evidence MUST NOT trigger hidden whole-model
  hashing. A wall that cannot be checked cheaply is reported as
  `dev_uncertified` with the missing evidence named.
- DEV-03: small manifest-binding checks (ID-03/ID-04 comparisons,
  argv-digest checks) and integrity-on-copy (digest on the way through)
  are always on and are distinct from expensive sealing.
- DEV-04: explicit `dev_uncertified: true` + executing-tree digest on
  every dev result. Certified release gate unchanged; the final artifact
  carries actual end-artifact evidence, never dev stamps.
- DEV-05 (open gap, stated): cross-host source-identity (a source that
  validates on one box reading as identical on another) has no cheap
  check yet; current posture is same-host re-verification. STATUS:
  proposed.

## 7. Safety and conditional liveness

Safety (never violated, no liveness owed without the conditions):

- SAFE-01 no eviction of a live lease (INV-06/INV-07).
- SAFE-02 no bulk-input read from a forbidden tier (INV-03/INV-04).
- SAFE-03 no false complete: terminal success requires exit 0 plus
  identity verification (ID-10); wrapper exit/shard counts alone complete
  nothing.

Conditional liveness: fair eligible workers + finite I/O + a fitting
workset (PRG-04 admit arm) lead to advancement or a named bounded failure.
Unknown or unreadable telemetry is never read as zero (capacity, fill,
residency, power).

Failure matrix (each names the refusal/status, the record, and the repair):

| Failure | Refusal / status | Durable record | Repair |
|---|---|---|---|
| rename-before-fragment race | second publisher waits; exactly-once visible | fragment + owner-keyed temp | ownership-serialized egress (existing lane) |
| claim handoff contention | loser abandons both ledgers, records tier denial | `_release_reservation` both sides | re-claim when fit |
| simultaneous egress | serialized; last-owner deletes | ordered egress records | — (mechanism) |
| restart / epoch change | readiness invalid; SM-02 `retiring` | epoch announcement | re-verify, re-stage |
| expired worker / stale lease | reaper releases; bytes untrusted until re-verified | stale-lease record | re-acquire |
| malformed metadata/index | map refused whole with reason | `[residency] refused` print + state | fix producer, republish map |
| read miss (unstaged range) | bounded wait with reason, else clear fail (never pool) | pending-lead record + reader refusal | stage the range / shrink window |
| oversized next window | `unsupported-workset` at seal | plan refusal | re-chunk the plan |
| retry / join gaps | per-action single adoption; gapped join advisory | attempt nonces; gap list | resubmit sealed key; re-join |

## 8. Acceptance ladder (executable; every test maps an ID with an assertion)

- ACC-01 schema + real-producer parser fixtures incl. gzip member, path
  shapes, and writer serialization variants: parse valid, refuse tampered
  bytes/content, refuse HERE-rooted records. (Precedent: PQ #842 contract
  tests. STATUS: pattern proven; staged-read fixtures missing.)
- ACC-02 PB lifecycle state-machine/property/race tests: rename atomicity,
  simultaneous egress exactly-once, claim-handoff abandonment releases
  both ledgers, epoch invalidation. (STATUS: largely implemented in PB
  suites; staged-lease races missing.)
- ACC-03 thin actual-PB-storage + real-reader chain: stage bytes through a
  real tier, open through the real PQ reader, assert payload bytes equal
  AND serving tier == staged AND zero forbidden opens (open-path
  instrumentation, INV-04). A direct-RAM-read fixture is NOT a substitute
  for the actual PQ reader. STATUS: missing — the ladder's load-bearing
  rung.
- ACC-04 restart/retry: kill during copy/lease/use; assert single adoption,
  no double-count, bytes re-verified. STATUS: partially implemented
  (journal grammar); lease-crash cases missing.
- ACC-05 both-Spark real placement + results: eligible rows drain on
  either box with per-box byte/energy counters. STATUS: missing live proof
  in this lane (PB720 proved CPU placement/admission only).
- ACC-06 real campaign output: end-to-end staged-only run with
  `bytes_from_pool == 0` on bulk legs and the §10 application gates green.
  STATUS: missing (blocked on reader strictness + Stage A run).

No test merely restates prose: each asserts a state transition, a refusal,
or a byte/tier equality.

## 9. Delivery evidence levels (distinct axes, never one YES)

`proposed` (this ledger) → `implemented` (commit) → `validated` (exact
action keys, terminal status, logs, CAS receipts + recorded exceptions) →
`deployed` (generation id + role convergence on the fleet) →
`workload-proven` (real outputs, counters, profiles on campaign bytes).
Root may merge `validated` changes; nothing is called campaign-complete
before the §10 gates. Reports cite actual paths/keys and preserve
failures (e.g. PQ action `38c8b4fd…` returned 1 with 195 passed / 3
failed is a failure with evidence, not a green; root's later generator
gzip/path/reproduction defects stay open repairs). No self-graded green
from wrapper exit or shard counts.

VER-01, bounded search completeness: before declaring evidence missing or
unrecoverable, state the index queried, the filters, the time window, and
the truncation bound — and query the authoritative indexes first
(`pb_actions` with `snapshot_parent`/`checkout_root`, the CAS/request
index). Counts and timings alone are not verification. (Precedent: the
PB720 final-shard keys were declared unrecoverable from `done/`-scan
truncation while the endings had not rotated; `pb_actions
snapshot_parent=31abdf251b38f259892acbabf222f74cdd26c935 limit 1000`
recovered all three. The ledger records the corrected keys.)

## 10. Root operating checklist (before each launch / merge / claim)

- [ ] Ledger statuses re-read (no prose claim treated as deployed).
- [ ] Action/attempt/manifest/payload/epoch identities bound (ID-01–ID-09).
- [ ] Initial working set ready per `residency_verdict` (INV-02).
- [ ] Strict tier opens enforced on bulk legs (INV-03/INV-04) or the run
      is explicitly accepted as non-staged with `pool-declared` payloads.
- [ ] Window fit proven or `unsupported-workset` refused (PRG-02/PRG-04).
- [ ] Progress channel committed per durable unit; leases released (SM-03).
- [ ] Terminal success = exit 0 + identity verification (SAFE-03).
- [ ] Endgame application gates, each stating what its output proves:
  Stage A receipt (adjoints bound) → bounded quanta admitted (per-layer
  payloads) → equality gate (bitwise match) → join coverage (exact roster)
  → full-menu allocation (per-Linear assignment) → export on pinned
  native cells (shippable bytes) → served quality + prefill/decode +
  memory/perf vs same baseline (production claim). Open unknowns: numeric
  tradeoff thresholds are an open calibration item (engineering continues;
  Rob prices the tradeoff when evidence exists — not a premature ask).
