# Full-stack integration harness — architecture and test plan

Owner: integration-harness lane (this branch). Production code owned by
reader-lifetime, strict-reader, cost_streaming/844, and tier workers —
this lane edits none of it without root coordination. New files only:
this design doc, harness helper, integration test files, PQ seam module,
and the ACC01..06 acceptance mapping (§6). PR + issue required; root
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
  the PQ-reader seam (§4) binds real PQ readers: the live lane must read
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

## 4. PQ-reader seam (fail-closed, no fake green)

`tests/fullstack_pq_seam.py` is the single seam between the PB chain and
PQ's source/render/activation readers. Until root's API bindings land,
every seam entry raises `FullstackPQUnavailable` naming the missing
binding; seam tests assert that refusal (fail-closed wiring proof, not
capability proof). Stable now: fixture payload shapes (sorted keys,
sha256-named), receipt shapes, and the coverage-input roster format the
future join consumes. Root reviews the API before any interface-rigid
test is written against it.

## 5. GPU path (deferred, bounded)

No giant-model reruns, no synthetic burn. When the CPU chain is green,
one tiny known-container fixture (MiB-scale tensor through the real
reader inside the campaign container image) covers the CUDA open path.
Design only until the CPU harness is accepted.

## 6. Coverage matrix (contract ACC01..06 → harness files)

| ID | Contract demand | Harness file | Status |
|----|-----------------|--------------|--------|
| ACC-01 | schema/parser fixtures: gzip, paths, serialization, tamper refusal | `test_fullstack_producer_rows.py` | implement now (PB-side manifest/row shapes) |
| ACC-02 | lifecycle/race incl. staged-lease races | existing PB suites + `test_fullstack_reader_boundaries.py` (egress-vs-hold, double release) | implement now |
| ACC-03 | real-tier + real-reader chain with forbidden-open negatives | `test_fullstack_reader_boundaries.py` (OS-enforced: chmod-000 pool, missing stage+ram, corrupt map, epoch bump) + PQ seam (pending) | PB boundaries now; PQ legs pending API |
| ACC-04 | restart/retry incl. lease-crash, single adoption | `test_fullstack_progress_retry.py` (journal/resume via real checkpoint grammar where PB-owned; lease-crash pending) | partial now |
| ACC-05 | both-Spark concurrent independent results, PB-placed | live lane only (2× gb10 rows) | pending (§2) |
| ACC-06 | staged-only campaign output, bytes_from_pool==0 bulk legs | pending strict-reader enforcement + live lane | pending |

## 7. Negative matrix (each a real boundary, not a mock)

- forbidden pool open reads zero pool bytes: pool fixture chmod-000 after
  staging; staged reads succeed; any pool fallback surfaces as EACCES.
- live hold beats egress: pinned/held range survives release; file intact.
- invalidated RAM serves only permitted SSD: epoch bump → lookup resolves
  the stage copy, pool untouched (pool chmod-000 as tripwire).
- both tiers gone → clear fail: lookup unresolvable AND open raises;
  never silent pool bytes.
- epoch/restart: bumped epoch demands revalidation, never assumption.
- corrupted map/digest: whole-map refusal with reason.
- resources reclaimed exactly once: double release balances the ledger.
- gapped join refused: `residency_plan.remaining` reports the gap (PB
  side now); full join refusal waits on the PQ join API (§4).

Failures, gzip/raw-canonical mismatches, required argv, placement tags,
defaults, and container bindings are caught by real boundary tests
(producer-row file), not by prose.

## 8. RED/GREEN and delivery

- RED: first PB run of the new files (real failures expected at seam
  edges and any misused production API); fix harness-only issues.
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
