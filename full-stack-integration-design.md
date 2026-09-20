# Full-stack integration harness — architecture and test plan

Owner: integration-harness lane (this branch). Production code owned by
reader-lifetime, strict-reader, cost_streaming/844, and tier workers —
this lane edits none of it without root coordination. New files only:
this design doc, harness helpers, integration test files,
the PQ bindings manifest mechanism, and the ACC01..06 acceptance mapping (§6). PR + issue required; root
final review. Rob asked for an integration test spanning the entire
functionality that protects future changes.

## 1. What the harness proves (thin vertical slice)

One bounded fixture corpus flows through real interfaces end to end:

real producer (manifest bytes + `residency_stage_rows` + `residency_plan`
freeze/validate) → published PB submission rows → HDD-source fixture →
movers copy to SSD stage (whole file AND nonzero-offset `.pbrange`
splits) → RAM promotion → map compose/lookup/`overlay_ram` → staged
reads through the real open path → durable progress advancing the
residency window (`residency_plan.remaining`) → retry/resume from
journals → receipt/CAS publication → ownership-safe cleanup (exactly-once
release) → deterministic join with coverage proof and gap refusal.

Every arrow is a production function or a real OS/file boundary. No
assertion merely restates harness code; no mock bypasses a cache or
reader path; no second dispatcher, cache, or placement knob is built —
PB owns placement throughout.

## 2. Two lanes, one suite

- **Local-CI lane (runs now):** inside one admitted PB CPU job, pytest
  runs the suite against an isolated local queue (`PoolQueue(tmp_path)`)
  with temporary tier dirs announced on it, plus small generated fixtures
  (MiB scale, generated in-test, never committed binaries). All
  production functions execute for real; the filesystem (permissions,
  missing files, corrupted bytes, epoch bumps) provides the adversarial
  half. Bounded: single-digit CPUs, a few GiB, minutes.
- **Live-fleet lane (designed, gated):** the same suite via `pbtest`
  with two independent consumer rows on the `gb10` class tag so PB
  places across both Sparks — never manual host sharding. Blocked until
  real PQ readers bind via the bindings manifest (§4): the live lane must read
  through actual readers, so direct-RAM opens cannot fake a pass. Until
  then the live lane is an unavailable target, stated not implied.

## 3. PQ dependency pin (existing mechanism, pending commit)

`pbtest` already enforces reviewed dependencies inside the admitted
worker: when the checkout carries `tools/resolve_*_dev_pin.py`, the
`pbtest_pins.py` guard checks the target interpreter's installed module
against its full Git commit before pytest starts. The harness will carry
`tools/resolve_pq_fixture_pin.py` following that exact pattern — but the
file lands only with root's candidate-commit API bindings. Until then the
harness is hermetic: fixtures generated in-test, zero PQ imports. No
parallel reader implementation is vendored to fill the gap (§4).

## 4. PQ wiring (no stubs; cross-repo tests requested)

The staged-only chain cannot be proven without the real PQ source,
render, and activation readers plus the join — and no stub, fake parser,
unavailable-assertion, or parallel reader is vendored to pretend
otherwise. Attempted here and rejected with evidence: wiring PQ imports
through box-local checkouts fails fleet-wide — sparky carries
`22149e1a` but dl380g10 carries `3541205a`, which predates the joint API
entirely, so no single checkout pin can hold across workers (proven by
PB actions `b5256f8ea000` and the pin-mismatch setup error, not by
reasoning). Mutable per-box checkouts are therefore not a dependency
mechanism.

Resolution (bounded ownership requested via root): PQ-contract tests —
`roster_digest` / `quantum_id` / `qname_layer` / `phase_ranges` /
canonical-JSON compat against real PQ code, then reader and lease
integration once the published candidate lands — belong in the PQ repo
against declared published PB, where the source is the checkout under
test. This lane keeps no fixture-conformance tests: invented payload/roster
helpers and their sha tests are deleted. Real PQ-side join coverage uses
actual roster/receipts when root's bindings land. Root reviews the API before
any interface-rigid test is written. The approved follow-on is
designing the source-snapshot/artifact dependency the bound readers run
against.

## 5. GPU path (deferred, bounded)

No giant-model reruns, no synthetic burn. When the CPU chain is green,
one tiny known-container fixture (MiB-scale tensor through the real
reader inside the campaign container image) covers the CUDA open path.
Design only until the CPU harness is accepted.

## 6. Coverage matrix (contract ACC01..06 → harness files)

| ID | Contract demand | Harness file | Status |
|----|-----------------|--------------|--------|
| ACC-01 | schema/parser fixtures: gzip, paths, serialization, tamper refusal | `test_fullstack_producer_rows.py` (phase tiling, freeze first-writer, wire/canonical split, empty-range refusal, class tags) | PB-side now |
| ACC-02 | lifecycle/race incl. staged-lease races | `test_fullstack_reader_boundaries.py` (charged double-egress balances, cleanup orphans) + `test_fullstack_progress_retry.py` (freeze refusal) | implemented PB legs; staged-lease races await lease API |
| ACC-03 | real-tier + real-reader chain with forbidden-open negatives | `test_fullstack_stage_ram_chain.py` (whole+split stage, whole+split RAM promotion, composed lookup byte equality) + `test_fullstack_reader_boundaries.py` (overlay mismatch/happy, epoch revalidation refusal) | PB legs now; PQ-reader legs requested PQ-side via root; reader/lease integration blocked on published candidate |
| ACC-04 | restart/retry incl. lease-crash, single adoption | `test_fullstack_progress_retry.py` + `test_fullstack_claim_retry_primitives.py` (freeze/refusal/remaining/egress/attempt ledgers) | PB legs now; lease-crash blocked on lease API (single assertion, never blanket) |
| ACC-05 | both-Spark concurrent independent results, PB-placed | live lane only (2× gb10 rows) | pending (needs runnable PQ work) |
| ACC-06 | staged-only campaign output, bytes_from_pool==0 bulk legs | pending strict-reader enforcement + live lane | pending |

## 7. Negative matrix (each a real boundary, not a mock)

- (removed: chmod tripwire claimed a refusal policy its manual staged read
  cannot prove; pool-touching reads belong to the PQ-reader lane.)
- live hold beats egress: blocked on the lease API (single assertion when it lands);
  today double-egress balances exactly once and cleanup leaves no orphans.
- invalidated RAM serves only permitted SSD: reboot voids the epoch and the
  old fragment refuses the new epoch in production overlay.
- (removed: stdlib FileNotFoundError plus lookup-None proves no policy.)
- epoch/restart: bumped epoch demands revalidation, never assumption.
- ram copy disagreeing with stage-vouched bytes/digest: overlay refuses;
  tampered map shapes refuse whole via validate.
- resources reclaimed exactly once: double release balances the ledger.
- unaccepted phases stay in the residency window (PB accounting only —
  not a join verdict); full join refusal waits on the PQ join (§4).

Failures, gzip/raw-canonical mismatches, required argv, placement tags,
defaults, and container bindings are caught by real boundary tests
(producer-row file), not by prose.

## 8. Claim/retry primitives (no membership behavior asserted)

Worker JOIN/RESIGN commands, supervision, and qualification belong to the
membership worker and have not landed — this lane edits none of that
production and asserts no membership behavior. `test_fullstack_claim_retry_primitives.py`
pins real queue/ledger mechanics only, on an isolated local queue:
tag/image conjunction gating before claim (both directions, unknown
inventory never capable), handoff with containment before token return,
retry-safe attempt-history preservation, explicit unsafe-retry
interruption (publish-time contradiction; terminal, never requeued),
incarnation-collision fail-closed, and charge retention. The
mixed-capability matrix covers classes, not hosts — separate from
both-Spark placement qualification, which stays in the live lane (§2).

## 9. RED/GREEN and delivery

- RED: first PB run of the new files (real failures from misused production
  APIs); fix harness-only issues. The qualification gate stays RED until
  root provides the bindings manifest.
- GREEN: full suite green via `pbtest --priority -10 --json` (keys
  filed outside the checkout); negatives that await strict-reader or
  PQ-API land as explicit skips naming the blocker — visible, never
  silent.
- Deliver: test commands, fixture description (generated, bounded),
  this matrix with per-ID status, issue + PR, RED and GREEN action keys
  with logs/CAS receipts, manifest/generation/source identity of the
  validation runs, limitations, and the unavailable live lane.
- Launch is not completion. Subsequent live gates need root approval of
  the concrete PQ-API implementation, not a permission loop.
