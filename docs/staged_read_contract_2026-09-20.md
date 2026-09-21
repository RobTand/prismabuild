# Staged-read contract: allowed tiers, leases, and readiness (target + status ledger)

Status: TARGET CONTRACT with honest per-requirement evidence in
`staged_read_requirements_2026-09-20.json` (schema
`pb.staged_read_requirements.v3`). This document is normative for what it
names and silent otherwise. It claims no deployment by prose: each
requirement records five orthogonal axes — owner, implementation, validation,
deployment, workload proof — plus independent target facts, and any axis may
read `unknown`. A requirement's
target stands irrespective of current partial support. PB owns all admission,
placement, stage movement, retries, and cleanup. PQ declares work, read sets,
and progress, and consumes. No parallel dispatcher, cache, preload,
residency, or scheduler is created here; no application steering of
placement. The PQ endgame (Stage A → quanta → equality gate → join →
allocation → export → served) is an application acceptance boundary that
consumes this contract; it is referenced, not re-specified, and it imposes
no new PB scheduler responsibility. Existing worker join/resign and endgame
support stays in scope and is unchanged by this update.

Updated 2026-09-21: §7 separates immutable origin publication from physical
staged-copy lifetimes and adds the produced-output, budget, and durability
requirements PO-01–PO-07, BUD-01–BUD-03, and DUR-01; ACC-07 defines the
produced-output strict-read acceptance gate. The dated addendum
`staged_read_produced_output_addendum_2026-09-21.md` qualifies the evidence
this update does and does not provide. A specification update is not deployed
conformance.

## 1. Scope and authority

- SC-01: PB decides which action runs where and when (admission, placement,
  claim, retry, withdrawal, cleanup). PQ submits rows with declared inputs
  and reads the verdicts.
- SC-02: PB moves bytes (movers), publishes tier residency, and retires it.
  PQ names byte ranges and reports durable progress. Neither side duplicates
  the other's job: a second mover path, cache, or placement knob violates
  this contract rather than implementing it.
- SC-03: this document is a target plus an evidence ledger. An axis reading
  `unknown` is owed work or an unrun check — never a claim, never a blank
  to be filled by assertion. "Suites" or "the live loop" named without an
  exact test path, action key, or receipt are not evidence and are not
  cited as such below.

## 2. Typed identities

The identities below are independent facts; no single field asserts more
than one of them. "Independent" means separately recorded and separately
checked — including the four facts ID-05 splits into.

- ID-01 action: `action_key` (content hash of the sealed action). Identity
  of *intent*, never of outcome.
- ID-02 attempt: `(action_key, nonce, scope_id)`. Retries mint new attempts
  with the request immutable; results are keyed per attempt and adopted per
  action only through INV-08. Timeout and retry transitions preserve attempt
  identity and record a terminal reason; a retry never rewrites the request.
- ID-03 manifest wire identity: SHA-256 over the sealed file bytes (gzip
  member included where the manifest ships gzipped). What pbrun seals into
  the action key and what the dispatcher binds on the wire.
- ID-04 manifest canonical identity: canonicalization is schema-specific,
  never a universal PB rule. The PQ receipt path uses exactly
  `cost_stage_checkpoint.canonical_json_sha256` with producer compat
  (`ensure_ascii=False`, `allow_nan=False`: UTF-8 bytes, non-finite
  floats refused — they have no canonical form and refuse at seal; the
  single writer newline is excluded from the digest input). PB schemas
  retain their own canonical function; PQ's function is never imported
  into PB across the repo boundary — each side names its function and
  version, and cross-side checks compare digest strings, never code.
  Decoded-compare goes canonical-vs-canonical through the named function.
  Raw-bytes-vs-seal comparisons are forbidden. Not every manifest needs
  both identities: a manifest consumed only as sealed wire carries ID-03;
  a manifest compared after decode carries ID-04; a manifest doing both
  carries both, each checked at its own boundary.
- ID-05 provenance, certification, integrity, and location — four
  independent facts, never one field. `certification: dev_uncertified |
  certified` asserts only which release gate ran with its required
  evidence; promotion MUST NEVER turn `dev_uncertified` into `certified`
  or reissue certification — promotion changes residency and integrity
  evidence only, and artifact certification comes solely from the
  application release gates with their required evidence. `provenance:
  {source identity}` names what the bytes claim to be.
  `integrity: {manifest binding, digest, epoch}` names the checks that
  actually ran. `location: {tier_id?, epoch?}` names where the bytes sit
  now. A pool-produced payload CAN move into RAM; the move updates
  integrity and location evidence, never certification. Digest integrity
  does not certify quality; RAM residency does not prove correctness. No
  new every-payload wire schema is required: these are logical
  requirements carried by existing metadata (tier receipts, map
  fragments, journal envelopes, release-gate records); any new carrier
  is explicitly target, not implemented.
- ID-06 content and change-detection evidence: `{manifest_wire_sha256,
  manifest_canonical_sha256?, range_digest?, epoch}`. Logical requirements
  are carried by existing metadata (tier receipts, map fragments, journal
  envelopes); any new every-payload wire field is explicitly target, not
  implemented, and is proposed only where an existing carrier cannot hold
  the fact.
- ID-07 serving tier, epoch, and lease: `{tier_id, epoch, lease_id,
  range_ref}`. The fact of *where bytes were actually served from, under
  which epoch and lease* — recorded at open time (INV-04), not inferred
  from counters afterwards.
- ID-08 worker/runtime generation: `(runtime_commit, generation_id)`.
  Behavior is attributed per generation; a source merge alone establishes
  no deployed support.
- ID-09 completion vs correctness, four distinct facts: (a) submit
  acknowledgement (row accepted, no promise); (b) terminal exit (process
  ended, reason recorded); (c) CAS publication (bytes content-addressed);
  (d) application validity (identity checks plus application gates).
  PB attests (a)–(c) for its own records and never inspects arbitrary PQ
  payload semantics. No claim of terminal success guarantees ID-04/ID-06
  of arbitrary application output.

## 3. Coordinates and ranges

Three coordinates, defined separately; conflating them is refused.

- RNG-01 source-file offset: `[o, o+n)` in the declared (pool-path) file's
  bytes. Source ranges MAY overlap and MAY be co-owned across phases and
  consumers; there is no universal tile-without-overlap across physical
  files.
- RNG-02 staged-object identity: `(tier_id, epoch, object_name,
  object_length, materialization_generation, content_identity)` where
  split ranges live at offset 0 of their own object. Generation plus
  content identity are part of the tuple: the same epoch, path, and
  length MUST NOT rebind new bytes (ABA) — a republished object is a
  different lease target. The lease pins this physical byte object.
  Acquisition and open validate the bound identity (generation +
  content) and pin the lifetime including async prefetch and mmap
  mappings: no stat-then-open race — the descriptor validated is the
  descriptor read, and a mapping outliving its pin is refused.
- RNG-03 logical read-plan cursor: `(phase, position)` in consumption
  order, which MAY repeat the same source ranges (forward reference,
  reverse replay, chain rebuild). Coverage is proven once per logical
  phase; repeats are explicit in the plan, never implied.
- RNG-04 mapping capacity: no assumption that a full tensor fits one
  mapping. Where production supports gathering ranges, the plan declares
  the gather set; absence of supported coverage for a span is refused
  (`unsupported-workset`), never silently narrowed to whatever fits.

## 4. State machines

States map to existing PB fields or are named as missing features. Lease
states below map onto the existing `reader_lease` pins/fragments: the lease
path is implemented in part (`reader_lease.acquire`, `open_pinned`,
`release`; ledger SM-03), and SM-03 names each remaining gap. No lease-intent
files or stale-lease records are pretended to exist. Any transition without
its named durable record is refused. Here and below, `durable` carries the
DUR-01 meaning: recorded in the named carrier and re-readable, not a claim of
power-loss durability.

### SM-01 action lifecycle

| From → to | Actor | Precondition | Effect | Durable record | Failure |
|---|---|---|---|---|---|
| submitted → waiting | submitter | sealed action, manifests bound (ID-03/ID-04 as applicable) | row visible in `ready/` | sealed request in CAS | malformed seal → never published; ack (ID-09a) is acceptance, not promise |
| waiting → admitted | claiming worker | tokens fit (INV-01); declared initial working set ready (INV-02); runtime supported (ID-08) | `ready/` → `claimed/` rename under queue discipline | claim record | shortage → denial naming tier/shortage; no pass recorded |
| admitted → running | worker loop | leases acquired (SM-03) | payload executes | attempt `(nonce, scope_id)`; request immutable | lease refused → back to waiting with reason |
| running → terminal-success | worker loop | exit 0 AND PB publication succeeds (terminal record filed plus CAS receipt for PB's own records) | record in `done/`, status `executed` | terminal record + CAS receipt | nonzero exit, timeout, or worker-observed refusal → failed. Exit 0 with ingestion failure → `result-ingestion-failed`, a failure distinct from execution failure; bytes unpublished. Payload-identity verification (ID-04/ID-06) is the application consumer's check on its own inputs, not something the worker performs on arbitrary payloads and not something terminal success attests |
| running → terminal-failed | worker loop / reaper | nonzero exit, timeout, or identity refusal; attempt identity + terminal reason preserved | record in `done/` failed or `failed/` | terminal record + log tail | — |
| terminal-failed → waiting (retry) | authorized PB lifecycle only | remaining attempt budget authorizes it; identical request, new attempt, old attempt contained | row re-enters contention as a new attempt | new attempt record linked to the terminal one | no automatic retry of `unsupported-workset` or policy refusals; retry-before-terminal forbidden |
| waiting/admitted/running → withdrawn | authorized PB lifecycle only (plan revision, duplicate, supersede policy) | running attempts reach containment, then a durable terminal record for the attempt, before any resource release or retry | row leaves contention without verdict | `withdrawn/superseded/` drop plus the attempt's terminal record | a drop is never read as a verdict; release-or-retry-before-terminal is forbidden |

### SM-02 materialization (per staged range)

| From → to | Actor | Precondition | Effect | Durable record | Failure |
|---|---|---|---|---|---|
| absent → copying | mover | tokens reserved for range ceiling; source readable | bytes copy to temp beside final name | mover row receipt (started) | overrun vs reservation → `residency_overran_reservation`, refused before copy |
| copying → published | mover | tokens reserved for range ceiling; source readable; length == range length; integrity computed during the necessary copy | atomic rename into place; map names it under epoch with the recorded actual digest plus source change-detection evidence | `movers/` receipt via `record_move` + map fragment | expected digest null → the actual digest is recorded, never claimed as "matches known expected". Mismatch against a present expectation → delete temp, range unpublished. Dev certification unchanged (DEV-04) |
| published → retiring | tier loop / egress, under ownership guard | marked retiring: no NEW readers admitted; live leases and pending copy handoffs recorded | range closed to new leases; charge and pin RETAINED | retiring mark + retained charge | new lease during retiring → refused |
| retiring → absent | tier loop | last live lease absent AND physical bytes actually deleted | object gone; ownership released exactly once; tokens freed | safe-deletion record + single release | last live lease still present → deletion forbidden; deletion failure → charge retained with retryable cleanup reason; release-before-reclaim forbidden |
| copying → absent (failed copy) | mover / reaper | temp and any published orphans safely reclaimed; charge retained until then | range unstaged, tokens released exactly once | reclaim record + single release | charge released before reclaim forbidden; orphan bytes left addressable forbidden |
| published → readiness-invalid | tier loop | epoch change | readiness statements void; bytes NOT proven gone, resources NOT proven free | new epoch announcement | readers re-verify; no any→absent shortcut |
| readiness-invalid → published | tier loop / reader | revalidation under the new epoch plus new lease binding | readiness restored under the new epoch only | revalidation record + new lease | old-epoch lease reused → refused |
| readiness-invalid → retiring | tier loop / egress, under ownership guard | range superseded under the new epoch | closed to new leases with charges held per SM-02 retiring | retiring mark + retained charge | — |

Ownership transfer is not a transition to absent: a transfer keeps the
physical object published for the remaining owners under the same epoch
and moves only the logical owner release (which owner's charge covers
the bytes). Logical owner release and physical object state are distinct
facts with distinct records; conflating them double-frees or leaks.
`absent` is entered only by actual delete after the last live lease is
gone.

Crash reaper: proves owned child processes and readers stopped (not merely
a stale timestamp) before releasing tokens or pins.

Reason labels in these tables (`residency_lead_not_resident`,
`unsupported-workset`, `result-ingestion-failed`, …) are logical target
labels. A label governs only where it is mapped to an existing wire enum
or denial string; unmapped labels define the spec, they do not fabricate
current status strings. No new runtime implementation is implied by any
row above.

### SM-03 reader lease (abstract; maps to pins/fragments where present)

| From → to | Actor | Precondition | Effect | Durable record | Failure |
|---|---|---|---|---|---|
| — → acquisition | consumer | range published under current epoch; request names RNG-02 + epoch | lease issued, pin held through the existing `reader_lease.acquire`/`open_pinned` path (implementation partial per ledger SM-03; no deployment or workload proof) | holder/pin records exist; a distinct lease-intent file remains MISSING — gap | unpublished/stale-epoch → bounded wait with reason or clear fail (the bounded wait lives in the claim path, not in `acquire`); never pool fallback for bulk input |
| acquisition → use | consumer | lease held | actual reads instrumented per tier (ID-07 recorded at open) | per-tier byte counters + serving-tier record | forbidden-tier open → fail before payload bytes |
| use → release | consumer | reads complete | pin released (`reader_lease.release` drops exactly its ref); range becomes evictable | a distinct durable lease-release record remains MISSING — gap; the progress channel exists | crash → reaper path above; bytes never trusted without re-verify |

## 5. Invariants (MUST, stable IDs)

- INV-01 admitted resources fit aggregate real lifetimes: a claim's CPU,
  memory, GPU, and tier tokens fit the box and tier ledgers simultaneously;
  a claim that reserved on a tier is concluded on both ledger sides via one
  release helper.
- INV-02 claim requires the declared initial working set ready: every lead
  movement node has a `done/` `executed` record for the same manifest the
  consumer names, still token-pinned (adoption counts; no-byte `cache_hit`
  does not). Live denial reasons `residency_lead_not_resident` (may arrive)
  vs `residency_lead_terminal` (never will).
- INV-03 actual bulk-input reads open only RAM or explicitly-allowed SSD
  staged objects. HDD (declared pool path) bulk opens are forbidden.
  (Target: current readers fall back to pool — the gap ACC-03 closes.)
- INV-04 the serving tier of every actual read (ID-07) is recorded at open
  time, before payload bytes are trusted. After-read counters observe;
  they do not authorize.
- INV-05 lease acquisition prefers valid RAM, then explicitly-allowed SSD
  for ranges the plan permits; anything else waits or fails. (RAM leg
  `--residency-ram auto` exists; the preference rule is target.)
- INV-06 stale epoch, corrupt, or missing data never falls back to HDD:
  stale-epoch reads refuse; digest mismatch deletes temp and unpublishes;
  missing ranges wait boundedly with reason or fail clearly.
- INV-07 copy/publish/lease/release serialize safely: rename-before-
  fragment (temp beside final name, atomic rename); claim handoff ordered;
  simultaneous egress serialized with shared-path ownership; last reader
  including pending copy handoff blocks eviction; retiring retains charge
  until actual delete with the last live lease already absent, released
  exactly once (§4 SM-02 ordering; a transfer moves only the logical
  owner release while the object stays published).
- INV-08 retries never double-count: new attempts mint new nonces with the
  request immutable; results adopted once per action; checkpoint adoption
  verifies identity/trust without recompute.
- INV-09 durable units monotonic and once: progress counters cumulative;
  published units never retracted; replays idempotent.
- INV-10 failed/gapped outputs never publish campaign success: gapped
  joins exit advisories; consumers refuse gapped payloads (application
  side; referenced, PB attests only ID-09a–c for its own records).
- INV-11 eligibility is class-tag conjunction as implemented; host-pair
  tags admitting neither box are plan errors refused at dispatch.
- INV-12 checkpoint adoption checks identity/trust without recompute, caps
  integrity/location evidence at what re-verification of the bytes in
  place actually established, and never upgrades either without that
  re-verification; promotion updates integrity and location only and never
  re-issues certification (ID-05).

## 6. Prefetch, tiers, and the read set

- TIER-01: RAM preferred; explicitly-permitted SSD allowed; no forbidden
  HDD bulk opens (INV-03). Residency readiness (is the window staged?) and
  source-tier enforcement (where did these bytes actually come from?) are
  audited separately — `require_prefetched` proves local prefetch timing
  only, never the tier.
- TIER-02 metadata allowance is bounded header/index/control bytes only
  (header from declared file, map/tier records, journal manifests). Any
  call that can materialize payload bytes — including `get_slice` — is a
  data reader under INV-03/INV-04, never an exemption by naming.
- TIER-03 boundary and activation bulk inputs are in the read set and
  dependency graph, or the plan fails `unsupported-workset`. Existing
  unstaged boundary reads are an explicit implementation gap, not a
  standing exception. Dynamically produced artifacts are planned only
  after their durable receipt exists: no phantom input hashes, no
  rerun-the-whole-producer to conjure inputs.
- TIER-04 no waiver checkbox: no flag, role, or agent acceptance lets a
  run waive the user's staged-only policy. Legacy diagnostics that must
  read pool bytes require explicit scoped user authorization naming the
  ranges and the reason; the authorization rides the sealed request, and
  resulting payloads carry uncertified integrity/location records with no
  staged tier claim. Our own acceptance never
  substitutes.
- TIER-05 a reservation is not residency, and one predicate answers both
  the gate and the report (#759). Tier tokens are taken at CLAIM, before a
  byte moves: they are the booking that bounds occupancy and that an egress
  gives back, and they never establish that bytes arrived. A movement node
  counts as RESIDENT only when, together: it still holds its tier tokens; a
  current map fragment for this consumer, tier and manifest vouches for the
  bytes, naming the plan's stage root on the stage leg and the ANNOUNCED ram
  root under the ANNOUNCED epoch on the ram leg; the node is not `claimed/`,
  because a running copy republishes its fragment as a prefix and whatever
  receipt is on disk belongs to a previous run; a `complete` `record_move`
  receipt covers the span the frozen plan sealed for that key; and that
  receipt's `entries_declared == entries_staged` equals the current
  fragment's entry count, so a historical complete receipt cannot speak for
  a later partial copy after a crash or requeue. Adoption satisfies this by
  re-issuing the donor's fragment under the successor with a receipt
  carrying that fragment's counts and the tokens transferred. Deciding
  residency by reading payload bytes, hashing a model, or stat-ing a whole
  manifest per cycle is forbidden; the evidence is the records above.
  `residency_plan.resident_movers` is the single implementation, and
  `tier_loop` (RAM publication precondition) and `pbstatus`/`pb_cursors`
  (reported state) both read it, so a gate and a cursor cannot disagree.
  Evidence that cannot be READ is UNKNOWN, and UNKNOWN is never reported as
  NOT STAGED. `staged: false` asserts a fact a caller acts on; unreadable or
  malformed evidence supports no such assertion, and collapsing the two is
  the same unproven-reported-as-known error as reserved-equals-resident.
  Concretely: a fragment directory that is absent is known-empty; a directory
  that cannot be listed, and a fragment file named for one of this plan's
  movers that cannot be opened or does not validate, are UNKNOWN. The
  map-composition reader (`residency_map.read_fragments`) deliberately skips
  such a file so a consumer still finds its other movers' copies, and that
  stays; readiness must NOT reuse that tolerance, because skipping is what
  flattens unknown into false. A file no leg of the plan names is still
  skipped -- it cannot change this plan's answer. On UNKNOWN the window
  gates closed and says so (`ram-window-unknown`) and the census reports
  `staged: null`; neither may report `false`. The census reports the booking as `reserved` beside
  `staged`, so capacity in use is never hidden behind the stricter
  readiness answer, and token holdings keep their own jobs -- window
  eviction of a passed phase and the advance fence still count what the
  ledger holds.
  UNKNOWN survives AGGREGATION, not only the leg. `cursor_gap` counts a
  phase as `unstaged_phases`/`unstaged_bytes` only when its leg is `false`,
  carries `unknown_phases`/`unknown_bytes` beside them, and keeps the three
  additive against `remaining_phases`; a chunked leg is `true` only when
  every chunk is, `false` as soon as one chunk is known missing, and
  UNKNOWN otherwise. A two-way test (`is not True`) over a three-state
  answer is banned here for the same reason `staged: false` is: it reports
  an unproven state as a known one, one level above the leg that was fixed.
  `prismabuild_residency_unstaged_*` therefore counts KNOWN backlog only,
  and unknown bytes are not yet exported as a family of their own.

## 7. Produced outputs: origins, materialization, budget, and durability

A processed output is an immutable logical origin batch that stays on the
shared pool. Producing output creates no producer read exemption, no mandatory
SSD writeback, and no second cache: reading a produced artifact, including one
that the same action just wrote, uses the same staged RAM/SSD path and the
same reader lease as any other bulk input. The produced-output materialization
API that this section names is unmerged target against PB main `f5b6bba0358`
(PR781 lane). The rows below are normative requirements, not deployed support;
axis-qualified evidence is in
`staged_read_produced_output_addendum_2026-09-21.md`.

- PO-01 origin publication and physical materialization are separate
  lifetimes. PB publishes and charges the immutable logical origin batch once,
  and the origin MAY stay on shared ZFS with no SSD copy at any moment.
  SSD/RAM materializations are bounded, leased, and retired separately. Every
  required bulk read of produced bytes, own or foreign, opens only RAM or
  explicitly-allowed SSD staged objects under an actual reader lease
  (INV-03/INV-04, TIER-01). No producer exception, HDD waiver (TIER-04 stays
  the only authorization shape), or parallel cache/preload path (SC-02).
  Evidence scoped to one producer: the PQ Stage A writer writes boundary
  artifacts to shared ZFS, and its strict reader rejects outputs missing from
  the input residency map (`zfs-output-path-root-review-20260921.json`). That
  is one campaign's source fact, not a PB requirement to write ZFS.
- PO-02 produced-byte readiness anchors on immutable batch publication, not on
  an ACTION terminal record. For a same-action self-read, the dependency chain
  is: immutable batch publication → funded PB staging (mover admitted with the
  exact prepaid transfer) → reader acquisition/use → reader release →
  cached-copy retirement. Depending on the producer action's terminal success
  self-deadlocks the producer, because that terminal cannot exist until the
  action finishes; this contract refuses that edge. A declared dependency with
  no publication record (phantom output hash) is refused, and rerunning a
  producer to conjure inputs is refused (TIER-03). PB stages the successor
  through the ordinary pool; no new application dispatcher is created
  (SC-02). Existing batch-publication and movement records carry the edge; no
  mandatory v2 wire schema is introduced.
- PO-03 repeat materialization. After a staged copy of an unchanged committed
  logical batch fully retires, the batch MAY be materialized again on demand
  (reverse reads, replay, restaged windows). One origin charge covers every
  generation; at most one pending or live materialization exists per logical
  batch; each successor gets a PB-derived mover identity and sequence filed
  under the existing lifecycle state, never a caller- or random-nonce
  identity; spent movers and funding fences stay spent and are never replayed
  or refunded. PB publishes no successor until the predecessor's retirement
  commits, and origin bytes are neither rewritten nor charged twice.
- PO-04 change detection on reuse. First publication captures the existing
  change-detection identity as an immutable producer record: the origin file
  identity (the existing stat-identity tuple on the normalized bound path)
  plus the writer-provided digest/identity evidence already sealed with the
  batch. Every materialization revalidates that exact identity before funding
  a copy and refuses a changed original, including a same-size mutation
  (`restage-origin-changed`, target label). A batch that lacks the
  first-publication proof refuses with `restage-origin-proof-missing` (target
  label) instead of blessing the bytes it finds. DEV reuses existing producer
  digest/identity evidence and adds no separate full-payload hash pass
  (DEV-01/DEV-02); the copy that must happen anyway may verify content in
  passing.
- PO-05 interruption, idempotence, and retirement responsibility.
  Materialization state and mutation intent are filed under the existing
  ownership lock before any funding or movement side effect, and they survive
  interruption. Duplicate or replayed `ensure` and retry are idempotent and
  resume the exact intent with no fresh epoch, quota, or successor. A failed
  or partial retirement retains the old materialization's responsibility and
  credits until safe reclaim, and occupied/unknown fences keep retention
  (SM-02; no refund from a missing record).
- PO-06 origin charge release follows proven origin deletion only. Retiring a
  cached copy releases that materialization's physical lifetime; it never
  releases the origin charge. The origin charge is released only after
  authorized origin deletion is proven absent, never by cached-copy eviction
  or a stale receipt.
- PO-07 lock ordering for retirement and egress. No prefix/tier ownership lock
  is held across the acquisition of another mover transition lock. The order
  is: validate and select the exact batch and materialization under ownership,
  release ownership, run the existing egress, then re-acquire ownership and
  revalidate the exact batch/materialization/intent before committing the
  retirement record. Late-reader pin censuses stay authoritative (a reader
  that arrives after the egress decision is still found by the locked census),
  and unknown, corrupt, or mixed ownership fails closed: retain, never free.
  Scope of the fixed edge: PB783 (merge `f5b6bba0358`) moves reclamation
  outside the stage ownership lock, and each certificate-bound release takes
  only its own root. The unmerged produced-output `retire_batch` still wraps
  egress in the outer output-prefix ownership lock, so this contract claims
  the lower egress edge, not the whole stack.

Budget and durability requirements for produced workloads:

- BUD-01 keep four quantities separate. The logical retained-artifact byte
  budget (what the plan must keep addressable), the physical tier
  reservations/current windows (SSD/RAM tokens charged at claim), the
  process/decoder resident RAM, and any per-entry serialization hold are
  distinct. Evidence for one is not evidence for another, and a resident-tier
  booking is neither free bytes nor residency (TIER-05).
- BUD-02 derive any unavoidable lower bound from the actual geometry: the
  actual last and remainder batch (not a full-batch ceiling), retained input
  boundary groups, live probe planes, and retained checkpoint copies, with
  headers and serialization as stated allowances. A planning allowance is an
  assumption, not a mathematical upper bound; the runtime byte guard stays
  authoritative and refuses before forward work when the floor cannot be met.
  Dimensions are producer-specific; no universal PB constant is set here.
- BUD-03 override only as an explicit, stamped seam. An invocation-level
  budget override records the original sealed limit, the effective budget, and
  the override identity together in the run's effective configuration and
  receipt, and preserves the plan, prepared, and calibration identities. The
  original sealed plan is never rewritten, and silent budget inflation is
  refused.
- DUR-01 `durable` means recorded and re-readable, not power-loss durable.
  Atomic rename, fsync, digest, and CAS receipt attest that a record was
  written and can be re-read under the observed storage policy; they do not by
  themselves establish power-loss durability when the backing dataset's sync
  policy is unchecked or asynchronous. State the persistence assumption
  wherever a claim depends on it. Observed for this campaign: the shared
  output dataset `storage_pool/shared` has `sync=disabled`, so receipts,
  digests, and recovery on volatile writes confer no physical durability; an
  earlier reading of the parent `storage_pool` dataset as `sync=standard` was
  wrong and retracted. This contract authorizes no durability sealing work and
  no automatic dataset policy change.

## 8. Progress, frontier, and liveness

- PRG-01 durable compute progress is NOT proof of reader release. A
  consumer's releasable safe frontier (leases it can drop without
  re-read) is established separately from its durable compute progress;
  eviction needs lease state, never the progress counter alone.
- PRG-02 the exact read plan names forward and reverse repeats and the
  source/render/activation legs. Lookahead fits in budget plus compute
  memory before the window is admitted; infeasible windows refuse at seal
  as `unsupported-workset`.
- PRG-03 waits are typed: fit-lack waits (`residency_lead_not_resident`
  + owning mover) vs permanent-oversize refusals (`unsupported-workset`).
  A missing initial phase may wait before GPU admission; mid-phase waits
  carry explicit grace and progress semantics (no forward movement within
  grace → terminal reason, not silent stall).
- PRG-04 no circular hold-and-wait by assertion: PRG-03's "holds current
  window" alone proves nothing with multiple consumers. The contract
  requires the formal inequality over all competing consumers —
  active leases + copy buffers + next minimum feasible advance + reserve
  ≤ announced capacity — OR a deterministic PB policy guaranteeing at
  least one admissible next step. No new agent scheduler is created to
  discharge this; the inequality or the policy is proved, named, and
  tested, else PRG-04 stays `proposed`.
- PRG-05 no whole-working-set RAM requirement; windows are the unit of
  fit.
- LIVE-01 conditional liveness: fair eligible workers + finite I/O + a
  fitting workset lead to advancement or a named bounded failure. Queue
  wait timeout implies neither withdrawal nor process containment.
  Fairness premises, finite-I/O premises, and the uninterruptible-NFS
  limitation are stated honestly wherever liveness is claimed; unknown
  or unreadable telemetry is never read as zero.

## 9. Dev mode and reuse without recompute

- DEV-01 giant input/output resealing and per-unit source walks are
  disabled in dev mode. Existing identity/change-detection evidence is
  reused where valid (CAS digests, sealed manifests, tier receipts).
- DEV-02 missing or changed evidence MUST NOT trigger hidden whole-model
  hashing and MUST NOT stamp-and-trust an old checksum: refuse reuse and
  open an explicit new lineage, or fail. "No recompute" means reuse of
  VALID banked units — it is never an absolute prohibition when inputs
  changed, which would be mathematically unsatisfiable.
- DEV-03 small manifest-binding checks and integrity-on-copy stay always
  on, distinct from expensive sealing.
- DEV-04 every dev result carries `dev_uncertified` plus executing-tree
  digest; the certified release gate stays distinct and demands actual
  end-artifact evidence.
- DEV-05 cross-host source identity: PR844 is implemented and under
  review — not proven. Until proven, posture is same-host
  re-verification, stated as incomplete rather than implied as sufficient.

## 10. Acceptance ladder (executable; every test maps an ID with an assertion)

- ACC-01 schema plus real-producer parser fixtures including gzip
  members, path shapes, and writer serialization variants: parse valid,
  refuse tampered bytes/content, refuse mis-rooted records.
- ACC-02 PB lifecycle state-machine, property, and race tests: rename
  atomicity, simultaneous egress exactly-once, claim-handoff abandonment
  releasing both ledgers, epoch invalidation, withdrawal-only-by-policy
  with containment-before-release.
- ACC-03 thin actual-PB-storage plus real-reader chain for source AND
  render AND activation readers: stage bytes through a real tier, open
  through the real reader with default flags, assert payload bytes equal
  AND serving tier staged AND zero forbidden opens — including negative
  assertions that a forbidden open fails before payload bytes, gzip and
  actual path resolution covered, end-to-end parser defaults. No fixture
  that manually opens RAM substitutes for the actual reader.
- ACC-04 restart and retry including lease-crash cases: single adoption,
  no double-count, bytes re-verified; attempt identity and terminal
  reason preserved across timeout/retry.
- ACC-05 both-Spark acceptance with actually independent results on each
  box concurrently wherever enough runnable work exists; PB owns
  placement. "Either box" placement that lets one GPU idle does not pass.
- ACC-06 real campaign output staged-only with `bytes_from_pool == 0` on
  bulk legs plus the application gates green.
- ACC-07 produced-output strict read and accepted progress. A produced-output
  read path passes only with: the real reader SDK (pinned immutable module)
  opening the actual validated staged descriptor; a negative control showing
  the declared origin path is refused before payload bytes; an accepted
  terminal semantic progress counter observed from the admitted action, not
  merely written; and attributable source, runtime, action, and CAS proof.
  These are insufficient alone: wrapper exit 0, reservations or occupancy,
  cache-hit counters, and digest equality of direct original-file reads.
  Historical canary CPU, CUDA, and cross-Spark payload evidence survives; the
  strict staged-only and accepted-progress qualification is unqualified
  (#784, branch `fix/784-canary-staged-reader-20260921`). The correction of
  record and the remaining gaps are in the dated addendum.
- No test merely restates prose: each asserts a state transition, a
  refusal, or a byte/tier equality. Lease and read lifetimes are covered
  across async prefetch, two consumers, eviction, and epoch restart.

Safety invariants, aligned with the ledger:

- SAFE-01 no eviction of a live lease (INV-06/INV-07; SM-02 retiring
  retains charge until actual delete with the last live lease absent).
- SAFE-02 no bulk-input read from a forbidden tier (INV-03/INV-04).
- SAFE-03 no false complete: the terminal record plus CAS receipt attest
  exit 0 and PB's own records only. Payload-identity verification is the
  application consumer's gate, never attested by terminal success, and
  wrapper exit or shard counts alone complete nothing.

## 11. Delivery evidence levels; root operating checklists

Levels are distinct axes, never one YES: `proposed` → `implemented`
(commit) → `validated` (exact action keys, terminal status, logs, CAS
receipts plus recorded exceptions) → `deployed` (generation id plus role
convergence on the fleet) → `workload-proven` (real outputs, counters,
profiles on campaign bytes). Root may merge validated changes; nothing is
called campaign-complete before the application gates. Reports cite actual
paths and keys and preserve failures. No self-graded green from wrapper
exit or shard counts. Bounded-search rule VER-01: state index, filters,
window, truncation before declaring evidence missing; query authoritative
indexes first; counts/timings alone are not verification.

Staged workflow checklists (each machines-readable: every box names its
ledger requirement id; reasoned exceptions cite explicit user authority;
no agent waiver; no performance/quality numbers set here):

- Before launch: submission is declarative (rows name inputs, ranges,
  progress, runtime); PB waits and gates readiness — initial working-set
  readiness is a pre-claim responsibility of the claiming worker reading
  the verdict, never pre-submission proof and never agent polling of
  readiness as a runtime. Identities bound (ID-01–ID-09 as applicable);
  strict tier opens enforced on bulk legs or the run carries scoped user
  authorization with uncertified payload records and no staged tier claim
  (INV-03/INV-04 or TIER-04 authorization); window fit proven or
  `unsupported-workset` refused (PRG-02/PRG-04). A future terminal receipt
  is never demanded before launch.
- Before merge: scoped validated repair with documented remaining gaps
  may merge; full-policy claims may not ride it.
- After deploy: generation plus role convergence observed; strict-reader
  and prerequisite gates re-checked on the fleet before any staged-only
  claim or certification.
- At completion: endgame application gates in order — Stage A receipt,
  bounded quanta, equality gate, join coverage, full-menu allocation,
  export on pinned native cells, served quality plus prefill/decode plus
  memory/perf against the same baseline — each stating what its output
  proves and its open unknowns. Numeric tradeoff thresholds stay an open
  calibration item: engineering continues on evidence; Rob prices the
  tradeoff when evidence exists.

Update of 2026-09-21. §7 and the dated addendum
`staged_read_produced_output_addendum_2026-09-21.md` update this contract
without proving runtime conformance. At PB main `f5b6bba0358`, the
produced-output API is unmerged, the produced-output strict-read/progress gate
is unqualified, and no PO, BUD, or DUR requirement is deployed. Deployment
facts stay on their own axes: PB782 deployed runtime generation
`054d7f0b66c8-1789960683-23b36e18a4f6`; PB783 is merged, but its staged
generation `43b790cce88c-1789962578-9e60f8c7ea49` was not activated at
observation, with activation under Astra review; PR781, PQ881, and PQ882
remain unaccepted, in-flight work. None of those facts satisfies a PO, BUD,
DUR, or ACC-07 requirement: a source merge is not deployment (ID-08), and a
staged generation is not an activated one.
