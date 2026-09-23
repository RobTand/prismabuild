# Staged-read contract: allowed tiers, leases, and readiness (target + status ledger)

**Current state, 2026-09-23 13:50Z** (evidence in §12 and the ledger):

- **Workload-proven:** nothing end to end. No staged-only campaign output exists
  (ACC-06 `no`). Bounded partial evidence only: R12 was admitted on a resident
  lead (INV-02), published produced batches and promoted them to RAM (PO-01),
  and was withdrawn from claimed (SM-01); PQ action `2c164969c33c` refused a
  cold source read instead of falling back (INV-06).
- **Live violations:** R12 released 44 GiB of produced-batch charge before
  reclaim (PB#929; SM-02, INV-07, PO-06). On `81d95cba8d91`, PB#944, PB#965
  and PB#966 stalled the R13 launch attempts (LIVE-01, PRG-03, PRG-04, INV-11,
  RNG-02), and PQ#1080 ended one (PRG-02).
- **Deployed:** generation `a0fdcd2f7482-1790170897-121d887b4732` (PB main
  through #972, including #948 and #970), canary verified at 13:44:38Z, three
  workers and the tier role converged. PB#966 has a fix in review (branch
  `fix/966-mover-dead-owner`), not merged and not deployed.
- **Validated, not deployed:** none of the PB merges the ledger cites; PQ#1079 is
  merged, but whether a campaign has run it is unknown.
- **Next acceptance step:** run (a), then R13, on `a0fdcd2f7482` until at
  least one chain completes with `bytes_from_pool == 0` on its bulk legs; merge
  and deploy the PB#966 fix.

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

Status corrected later on 2026-09-21 (docs-only reconciliation; no obligation
weakened): PR781 is merged and its produced-output module and
`pbrun --produced-output-template` are present in the deployed runtime
generation `d794839c589d-1789966072-c66c7f0568d0`; the produced-output
lifecycle, deployment of the PR792 repair, and every PO/BUD/DUR workload proof
remain open. §7 and §11 state the corrected current status; the dated
addendum carries the appended evidence and its snapshot time.

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
its named durable record is refused. DUR-01 states what the named records and
receipts do and do not prove about persistence; it does not weaken the commit
or recovery requirements.

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

A processed output is an immutable logical origin batch retained on its
declared origin storage; the current shared ZFS output path is the observed
example, not a requirement that origin storage be ZFS. Producing output
creates no producer read exemption, no mandatory SSD writeback, and no second
cache: reading a produced artifact, including one that the same action just
wrote, uses the same staged RAM/SSD path and the same reader lease as any
other bulk input. Write preauthorization precedes the write; committing the
origin and creating its first staged materialization are separate steps, so
the first copy may wait until a read needs it without reserving a full-corpus
SSD window. Current status, corrected later on 2026-09-21: the produced-output
materialization API this section names is merged into PB main by PR781
(`dd5523dd3ff66025170f1e2248d8b32d180d7a78`, head
`42f2cfb874077da66c27401e718d3636903a8a40`), root-accepted for source merge
only, and is present in the deployed runtime generation
`d794839c589d-1789966072-c66c7f0568d0` (source
`d794839c589dee8cc9427e10127f6381800eaa05`). Deployed code presence is not
behavioral conformance: the PQ production constructor/SDK/template/lifetime
wiring is incomplete, the PR792 snapshot-forwarding repair is merged
(`aa03c0bde1b52138b02be2b13083bf3e8274d9ac`) but not in the active runtime,
the own-copy egress repair had no accepted commit at this snapshot (PR795
merged later at 2026-09-21T06:33:58Z; source-merge accepted, deployment and
global behavior proof pending),
and no successful global produced-output lifecycle has completed: the one
global attempt failed at the mover preflight before any read, while library and
component cycles did run. The rows below are normative requirements,
not deployed support; axis-qualified evidence is in
`staged_read_produced_output_addendum_2026-09-21.md`.

- PO-01 origin publication and physical materialization are separate
  lifetimes. PB publishes and charges the immutable logical origin batch once,
  and the origin MAY stay on its declared origin storage with no SSD copy at
  any moment. SSD/RAM materializations are bounded, leased, and retired
  separately. Every required bulk read of produced bytes, own or foreign,
  opens only RAM or explicitly-allowed SSD staged objects under an actual
  reader lease (INV-03/INV-04, TIER-01). No producer exception, HDD waiver
  (TIER-04 stays the only authorization shape), or parallel cache/preload
  path (SC-02). Evidence scoped to one producer: the PQ Stage A writer writes
  boundary artifacts to the current shared ZFS path, and its strict reader
  rejects outputs missing from the input residency map
  (`zfs-output-path-root-review-20260921.json`). That is one campaign's
  source fact about its origin storage, not a PB requirement to write ZFS.
- PO-02 produced-byte readiness anchors on immutable data-ready proof, not on
  an ACTION terminal record. Existing first publication seals the immutable
  manifest, stages, publishes, and funds the mover, and records the batch
  accounting entry (`publish_prepaid_batch` then `commit_batch` in the PR781
  lane). A same-action self-read depends on the immutable published batch and
  its ready staged data, not on the whole action's terminal record, which
  cannot exist before the action finishes; depending on that terminal record
  self-deadlocks the producer, and this contract refuses that edge. The
  semantic dependency is: immutable batch publication → funded PB staging →
  reader acquisition/use → reader release → cached-copy retirement. A
  declared dependency with no publication record (phantom output hash) is
  refused, and rerunning a producer to conjure inputs is refused (TIER-03).
  PB stages successors through the ordinary pool; no new application
  dispatcher is created (SC-02). Existing records carry the edge, and no
  mandatory new wire schema or ready-record API is required. Recorded
  implementation gap (historical, 8ca attempt): the strict reader rejected a
  newly produced batch absent from the static input map
  (`stage-a-8ca-attempt-review.json`); the current PQ881 branch carries bridge
  component tests, and the global cycle is not qualified. (Correction,
  2026-09-23: PQ#881 merged on 2026-09-21T16:38Z as `f20cccb0e953`; see §12.) PR781 is merged at
  source level (`dd5523dd3ff66025170f1e2248d8b32d180d7a78`) and its acceptance
  is not a live-cycle proof. The one actual global produced-output live
  attempt failed before any read: the child mover request lost the producer's
  sealed checkout
  snapshot, the mover (`6fbc96301c6cc2ad245003a4866271288532dcde5ff37a324046e7004abd0846`)
  was claimed and then refused in 0.76 s by core preflight, and its owner
  (`0dedb066f8684fba56f388496089790b53a0b0658a4abe8d5945e75592178669`) failed
  closed on its 600 s boundary-staging budget with no full Stage A launched
  (`produced-mover-snapshot-root-diagnosis.json`). The child
  snapshot-forwarding repair is merged as PR792
  (`aa03c0bde1b52138b02be2b13083bf3e8274d9ac`) and is not in the active
  runtime; retirement waits are a different edge and do not change this
  anchor.
- PO-03 repeat materialization. After a staged copy of an unchanged committed
  logical batch fully retires, the batch MAY be materialized again on demand
  (reverse reads, replay, restaged windows). One origin charge covers every
  generation; at most one pending or live materialization exists per logical
  batch; each successor gets a PB-derived mover identity and sequence filed
  under the existing lifecycle state, never a caller- or random-nonce
  identity. A spent mover or funding fence is never reopened or reused to
  authorize a fresh materialization, and never refunded; an identical replay
  may answer from the recorded outcome without new tokens or a new copy. PB
  publishes no successor until the predecessor's retirement commits, and
  origin bytes are neither rewritten nor charged twice.
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
  (SM-02; no refund from a missing record). The shared validator refuses a
  shape-valid but inconsistent materialization history — an older unretired
  materialization beside a newer retired one, multiple live entries, or
  duplicate keys — and retains instead of hiding the older live copy. This is
  a source finding, not yet a reproduced RED.
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
  Evidence of record: PB783 (merge `f5b6bba0358`) moves reclamation outside
  the stage ownership lock, and each certificate-bound release takes only its
  own root; it is activated at generation
  `43b790cce88c-1789962578-9e60f8c7ea49` (publication verified
  2026-09-21T04:07:13Z; three live nodes converged at 2026-09-21T04:09:13Z
  with the rollout idle). The outer-caller produced-output `retire_batch`
  source carries the same order and is merged by PR781
  (`dd5523dd3ff66025170f1e2248d8b32d180d7a78`, head
  `42f2cfb874077da66c27401e718d3636903a8a40`): it validates and selects under
  the output-prefix ownership lock, releases that lock before
  `stage_release.evict`, and revalidates the exact selection before filing the
  retirement record, and that source is present in deployed generation
  `d794839c589d-1789966072-c66c7f0568d0`. That is source and code presence,
  not behavioral proof: no global produced-output retirement has been accepted
  (library and component cycles did run; a separate admitted private-queue
  diagnostic, action `83264d103a2a` iteration 1, exposed the own-copy defect
  recorded in the addendum, issue 793, whose
  repair PR795 merged later at 2026-09-21T06:33:58Z and is source-merge
  accepted (`pb795-root-acceptance.json`); deployment and global behavior
  proof pending; PO-05/PO-07/ACC-07 obligations cover it), the PR792 repair
  is not deployed, and the deployed lower edge alone does not discharge this
  row.

Budget and durability requirements for produced workloads:

- BUD-01 keep four quantities separate. The logical retained-artifact byte
  budget (what the plan must keep addressable), the physical tier
  reservations/current windows (SSD/RAM tokens charged at claim), the
  process/decoder resident RAM, and any per-entry serialization hold are
  distinct. Evidence for one is not evidence for another, and a resident-tier
  booking is neither free bytes nor residency (TIER-05).
- BUD-02 derive any mandatory floor from the actual geometry: the actual last
  and remainder batch (not a full-batch ceiling), retained input boundary
  groups, live probe planes, and retained checkpoint copies. The floor counts
  only bytes that are independently unavoidable or minimum; headers and
  serialization that depend on implementation choices belong to named
  planning allowances, not to the floor. Keep the gates distinct: the
  preflight floor refusal, which runs before any forward work; reserve, which
  runs before writing; and commit, which validates the actual serialized
  bytes. The runtime guards stay authoritative. A planning estimate includes
  named allowances and is an assumption, not a mathematical upper bound.
  Dimensions are producer-specific; no universal PB constant is set here.
- BUD-03 override only as an explicit, stamped seam. An invocation-level
  budget override records the original sealed limit, the effective budget, and
  the override identity together in the run's effective configuration and
  receipt, and preserves the plan, prepared, and calibration identities. The
  original sealed plan is never rewritten, and silent budget inflation is
  refused.
- DUR-01 producer commit boundaries and recovery requirements are unchanged;
  this row adds a persistence-assumption requirement, it does not weaken or
  redefine them. Atomic rename places a record, and verified publication
  evidence (the published record is read back and validated) establishes that
  the record is present and re-readable. A rename, fsync, digest, or CAS
  receipt does not prove stronger persistence, and none of them proves
  power-loss durability when the backing dataset's sync policy is
  asynchronous or disabled. When stable-storage durability is required,
  `sync=disabled` does not satisfy it. State the persistence assumption
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
  Historical canary CPU, CUDA, and cross-Spark payload evidence survives. The
  #784 staged-reader stack is now merged (PR788, merge
  `c9af82333eeb8900b139e6c593fa6258b0421d63`) and carried by deployed
  generation `d794839c589d-1789966072-c66c7f0568d0`; the verified baseline
  canary ran leg 3 — the strict staged read with pin/tier records and
  independently observed accepted progress — on Sparky only, action
  `e44c5a11db6a87dfa95c29425ef040e90f1c790370230e2a58cb5a2b3f74a299`, on
  static inputs. The both-Spark part of the canary is the separate leg-4
  payload comparison (action `859436101045acedd1a88173a96b0b5b6fd55d1873f9f37112044d916ece8f0e`
  on Sparky and `ac2f43ffdb749dd4176fa0e4326b893e5e5a72c29e02f3706f8f19dc0c095bd9`
  on Sparklina), not a strict-read claim.
  That qualifies the reader gate on the baseline only: the produced-output
  application of this row — a produced output read in the global PQ cycle
  with the origin-refusal negative control — remains unqualified, as does the
  PQ live lifecycle. The correction of record and the remaining gaps are in
  the dated addendum.
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

Update of 2026-09-21, revised later that day (current-status reconciliation;
no obligation weakened). §7 and the dated addendum
`staged_read_produced_output_addendum_2026-09-21.md` update this contract
without proving runtime conformance. At PB main `aa03c0bde1b5` (this docs
snapshot's base), PR781 is merged
(`dd5523dd3ff66025170f1e2248d8b32d180d7a78`, head
`42f2cfb874077da66c27401e718d3636903a8a40`) and its produced-output module and
`pbrun --produced-output-template` are present in the deployed runtime
generation `d794839c589d-1789966072-c66c7f0568d0` (737 files; three live nodes
converged; rollout idle). The baseline fleet canary passed on both Sparks as
static-input evidence (the leg-4 payload comparison on Sparky and Sparklina),
with the strict staged-read leg 3 on Sparky including independently observed
accepted progress; it is not a produced-output or global-cycle acceptance. PR792
(`aa03c0bde1b52138b02be2b13083bf3e8274d9ac`) is merged but not deployed; the
own-copy egress repair had no accepted commit at this snapshot (PR795 merged
later at 2026-09-21T06:33:58Z; source-merge acceptance recorded, deployment
and global behavior proof pending); the
produced-output strict-read/progress gate is unqualified; PQ881 is unmerged and
no upgraded Stage A has completed. (Correction, 2026-09-23: PQ#881 merged later
that day, at 16:38Z; §12 carries the current status.) Consequently no PO, BUD, or DUR
requirement is deployed-with-behavior or workload-proven beyond the PB783
lower cross-root egress edge of PO-07 and the scoped PR781/PR792
source-and-component acceptance records. Deployment facts stay on their own axes: PB782 deployed
runtime generation `054d7f0b66c8-1789960683-23b36e18a4f6`; PB783 is merged and
activated at generation `43b790cce88c-1789962578-9e60f8c7ea49` (publication
verified 2026-09-21T04:07:13Z; three live nodes converged and the rollout idle
at 2026-09-21T04:09:13Z). A source merge is not deployment (ID-08); a deployed
generation is not workload proof.

## 12. Status on 2026-09-23

This section records the status at 2026-09-23 13:50Z, against PB main
`a0fdcd2f7482` and PQ main `f12313f9903d`. It weakens no obligation. The ledger
carries the per-row evidence: every row changed today has a dated
`2026-09-23 update` sentence on each axis it changes, and a `live_defects`
list where a live run violated it. Earlier dated paragraphs above stay as
written, with in-place corrections where they are now wrong.

### Live runtime generation

The current generation is `a0fdcd2f7482-1790170897-121d887b4732`:

- Commit `a0fdcd2f748213f3ca78f914099ffbd403382461` (the #972 merge), rollout
  `rolling`, published at 1790170897 (13:41:37Z). It adds #943, #947, #948,
  #951, #952, #953, #956, #957, #962, #967, #968, #969, #970 and #972 to
  `81d95cba8d91`; each merge SHA was checked with
  `git merge-base --is-ancestor <sha> a0fdcd2f7482`.
- Pre-publish suite on `a0fdcd2`: 8 of 8 shards green, 7,658 passed (shard
  keys `b702ae46dd65`, `443e59d7de70`, `cb79312716a5`, `4a65329f8fc5`,
  `b80dda640506`, `b4530f4b7bc8`, `b32975f893cc`, `4d6205023723`); record in
  `/home/rob/tmp/pb-publish-a0fdcd2-evidence/suite.json`.
- Canary run `pb-canary/20260923T134140Z`: verdict `verified`, sealed, exit 0,
  "legs 1-4 executed, receipts ok, leg-4 envelopes equal", recorded at
  1790171078 (13:44:38Z). Leg receipts: `3d902bc49590`, `65001325ae4a`,
  `7bc04fa2d7d3` (leg 3: 3 chunks, 25,165,824 bytes verified) and
  `b068b58ddf6d` (leg 4: sparky and sparklina envelopes bitwise equal). A
  canary is not workload proof.
- Role convergence: the offers from sparky, sparklina and dl380g10 announce
  `runtime_commit` `a0fdcd2f7482` from 1790171012 to 1790171023 (13:43Z). The
  dl380g10 tier role runs `tools/tier_loop.py` from this generation's tree,
  still without `--output-windows`. At 13:45Z sparky offers `spool_gb` 278 and
  sparklina 359.

The previous generation, live from about 10:46Z to 13:41Z, which the R13
launch attempts and the defects below ran on:

- Generation `81d95cba8d91-1790160372-5d100532e024`: commit
  `81d95cba8d911907ca85db5618d34c0c0855e7bf` (the #935 merge), not dirty,
  published from sparky at 1790160374 (2026-09-23 about 10:46Z).
- Canary: `runtime-generations/81d95cba8d91-1790160372-5d100532e024.canary.json`
  reads `canary_status: verified` ("every leg executed and every receipt
  verified"), recorded at 1790160559.
- Role convergence: the worker offers from sparky, sparklina and dl380g10
  announce `runtime_commit` `81d95cba8d91` at 1790169703. The dl380g10 tier role
  runs `tools/tier_loop.py` from this generation's tree, without
  `--output-windows`, so the #895 output-window obligation is deployed and off.
  DESKTOP-P5UOGNJ is offline by decision (PB#900) and still announces
  `a80eea97`.
- Spool budget (#917, #921) was live then too.
- A source merge is not deployment (ID-08): the fixes merged after this
  generation became deployed only with `a0fdcd2f7482`.

### Campaign runs

- R12 (action `683cb3caa5ea`), submitted on generation
  `c2bda68758a3-1790110205-9e9735511db2` and claimed on sparky at 1790112229,
  with residency verdict `resident` on one lead at tier
  `prismabuild-stage:dl380g10` and produced-output template
  `pq-stagea-boundary-entries-512-20260922-w24-r11`. Rob withdrew it from
  claimed at chain-016 (2026-09-23T10:43:47Z) to relaunch as R13; its sealed
  checkpoints remain as bounded evidence. It ran without #902 and #904: it
  waited 25 minutes beside 484 GiB of orphan cache (PB#901) and held 528 of
  565 GiB of stage (PB#903). Whether it read only from staged tiers
  (`bytes_from_pool == 0`) is not recorded.
- R13 launch attempts on the live generation stalled or failed on the four
  defects below. No campaign has yet exercised the fixes deployed in
  `81d95cba8d91`.

### Live-run defects and the requirements they violate

| Defect | Observed | Violates | Status |
|---|---|---|---|
| PB#929 | R12: the orphan pass released the tokens of 22 completed produced batches (44 GiB) while the bytes stayed staged | SM-02, INV-07, PO-06 | Fixed by #935; deployed; not yet exercised |
| PB#944 | Movers `68fdb8728f38` and `750a4f8c65eb` of consumer `d54952c1fcac` inherited `task_class: measurement`; refused `measurement_host_not_idle` on 1,596 passes over 134 s | INV-11, PRG-03, LIVE-01 | Fixed by #948 (`abb13ff58bf7`); deployed in `a0fdcd2f7482`; not yet exercised |
| PB#965 | Chunk mover `8faf2233c63a` (consumer `a7d31a4da9c1`) cut a manifest entry and refused `residency_overran_reservation` on every attempt | SM-02, PRG-02, PRG-03, LIVE-01 | Fixed by #970 (`0411c83330da`); deployed in `a0fdcd2f7482`; not yet exercised; consumers sealed on an older generation need a resubmit |
| PB#966 | Mover `950345d2b90a` could not invalidate failed consumer `2c164969c33c`'s copy, exited rc 0 with `complete: false`, and reran without end holding 365 of 730 fill tokens; consumer `a7d31a4da9c1` was never admitted | PRG-03, RNG-02, SM-02, PRG-04, LIVE-01 | Fix in review on branch `fix/966-mover-dead-owner` (Fixes #966): a divergent name is settled by its owners' states; not merged, not deployed |
| PQ#1080 | Stage A seed `2c164969c33c` did not prefetch its first layer and refused a cold source read, rc 1 after 282 s | PRG-02 (INV-06 held: the reader refused and did not fall back) | Fixed by PQ#1079 (`f12313f9903d`); use in a campaign unknown |

### Corrections to earlier status

- PQ#881 merged on 2026-09-21T16:38Z (`f20cccb0e953`), and PQ#883 merged on
  2026-09-21T04:22Z (`8c204ea24a05`). Both are in R12's PQ checkout (snapshot
  `f008a76ee425`, parent `34a07e0f2ebc`). The ledger called both unmerged.
- TIER-05's fix merged as PB#763 (`bb54907e53be`, 2026-09-20T18:46Z) and is in
  both `c2bda68758a3` and `81d95cba8d91`. The ledger called it source only.
- PR792 (`aa03c0bde1b5`) and PR795 (`3641e29332bf`), which §11 and the PO rows
  call not deployed or pending, are in `81d95cba8d91`.

### Other open items that bear on acceptance

- PB#978: the pbcanary GitHub workflow has never run. No runner is
  registered, and 136 of 138 runs were cancelled. The canary verdicts above
  come from `pbcanary` runs on the fleet, not from that workflow.
- PB#975: pbtest's per-test bound for a GPU shard follows the Sparks'
  86,400 s campaign ceiling.

### Keeping the ledger current

From this update on, a PB PR that changes a requirement's status updates that
requirement's ledger row in the same PR (PB#971). Status detail belongs in the
ledger, not only in `docs/design.md`.

### Next acceptance step

Run (a), then R13, on `a0fdcd2f7482` until at least one chain completes with
`bytes_from_pool == 0` on its bulk legs, and merge and deploy the PB#966 fix. Workload
proof on every row stays as recorded until such a run exercises it. That run is the first candidate
workload proof for LIVE-01, PRG-03, INV-11 and ACC-06.
