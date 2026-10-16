# Supported-window compatibility

This is the C31/B11 implementation record for the isolated branch
`sol/refactor-compatibility-corpus-20261016`, based on
`416a1eccebaea4ef224e0ddef8f6a1fe424756b4`. The initial T3 checkout at
`2d9ee5191ef6e7ed79c59786b59f1ed59bc22a6f` was clean. C16, B14, B15,
resident-test cleanup and C25 remain in the base. Root owns integration and
qualification against its later C01/C05/C06/C28 changes. This record grants no
publication, deployment or live-store mutation authority.

## Catalogue and inventory

`src/prismabuild/record_kinds.py` records 152 kinds with wire identity, location,
field policy, accepted legacy shapes, actual reader and writer references,
and 377 cases. These comprise 363 production-writer scenarios, 11 caller inputs
and three immutable configuration sources. Caller inputs and configurations
earn no runtime-writer credit. The policies are 62 closed, 89 additive and one
presence-only safety reader. That last reader deliberately treats malformed
resident-pin bodies as protection; inventing a JSON decoder would weaken its
actual contract.

The catalogue drives the real adapters and the eight protocol test populations.
It does not import all production modules or replace validators with
`json.loads`. Five residency-map/lease IDs now use the catalogue's semantic
owner. Other IDs remain at their existing production owner with reference
metadata, avoiding a broad ownership migration through concurrent refactors.
No wire ID or compatibility alias was introduced.

`tests/compat/support/wire_inventory.py` scans Python, imports, embedded Python,
shell-expanded JSON templates, source data and retained source archives. Its
`--require-complete` gate accounts for every discovered ID and required corpus
case. `tests/compat/dispositions.jsonl` binds each retired, identity, derived or
operator-only disposition to its actual source witnesses. A new reader or
constructor invalidates that disposition. Qualification fixtures and old
artifacts do not become supported generations by being retained.

The current inventory has 279 versioned IDs plus one unversioned record across
source, controls and history. All 152 catalogue entries have current and
historical captures or explicit source-bound participant absences. Other
dispositions are 57 operator controls, 13 identity domains, four derived
projections, 16 retired APIs, one rejected retired schema and 37 test/history
references. The original card's 225 IDs, 15 newer versions and roughly 30 closed
modules were historical estimates, not acceptance targets.

`tests/compat/field_sets.json` pins 292 resolved field sets and 221 symbolic
field expressions across 51 production modules. Symbolic expressions stay
visible. The scanner also reports all serialization sites and six dynamic
schema constructors; discovery alone does not qualify their runtime paths.
`reconcile_record_writers.py` joins source constructors to actual executed-line
proofs from verified native logs and reports uncovered constructors explicitly.

## Supported window and rollback transition

Support means live worker generations plus the explicitly designated rollback
target. Retained directories confer no support. Offline WSL is excluded.
The persistent local designation is
`docs/compatibility_rollback_designation.json`:

- Directory `a17477b2b206-1791745175-65dffa700dfb`.
- Commit `a17477b2b206adf79d89e65c7b9d09584f0bfa34`.
- Raw receipt SHA-256
  `5cef73692997fbd9f3d8d187c9f7aad15ee6b699912ceb1cf88fe9444082c9dd`.
- Complete file-map digest
  `dd694fdb23b193f46ab82349c81067b6a9fd68eeaaba3ec24e24bf4176767ff3`.

This designation applies only to candidates without the optional B09 proof
member. Once the installed-or-target union contains `park_proof.py`, both
updaters must declare optional-member protocol 2 and proof-member protocol 2.
a174 lacks the proof declaration and is refused as a B09 rollback target.
The model checks actual member presence as well as declarations.

The unpublished bridge is source `6fa9b780304df7b24bad5ea3bb0fde540ff95153`,
tree `13979e7ffee4557fec993c39fdcc68055c878e49`. It retains the deployed
broker/behavior, replaces only the updater and has no proof source. Its corpus
is a prepared source control, not a live generation or designated rollback.
Root's four fresh installed-reader transaction fences passed, with simulated
broker/service I/O. Publication, fleet convergence, an immutable bridge
receipt/directory designation and actual physical G-to-bridge rollback remain
separate root gates. No bridge publication is authorized here.

`compat_predeploy.py qualify` captures an attested read-only published status
and validates it immediately inside one admitted action. It requires complete,
fresh evidence, unambiguous live-generation identity and the exact rollback
directory, receipt and all 1,299 file hashes. A changed live generation requires
recapture and corpus qualification; it never relabels the rollback target.
A future rollout must rerun this check. A saved green capture is not permanent
support or deployment approval. `pb_gc`'s explicit movement-receipt retirement
precondition includes every live reader and the designated rollback target.

## Historical bytes and process isolation

The original a174 Git object is unavailable. All 1,299 deployed receipt members
match the actual qualified source
`2a248e6364eef635f4bb417b6eff41d2aebbab6f` and flat-export mapping exactly.
This proves byte equivalence, not invented a174 Git ancestry. The raw deployed
receipt, independent comparison and original CAS source receipt are retained.

Read-only archives retain the complete receipt-bound import/data closures:
200 deployed files and 197 prepared bridge files. Every extracted member and
raw provenance receipt is verified. Each operation starts fresh `python -I -S`
with only its selected closure and stdlib paths. The driver checks module
paths, specs and hashes, every executed retained source file, compiled code
objects and the interpreter. Candidate packages and installed site packages
cannot satisfy old imports. Stable-path fixtures preserve PB CPU affinity.

Some coordinator writers never shipped with the deployed reader. Their complete
qualified 2a source is captured separately in `tests/compat/writer-sources`.
Canary and qualification-receipt scenarios record that writer provenance while
the actual old consumer still runs in its own deployed closure. They do not
invent a deployed writer, fill missing siblings from the candidate, or combine
writer source with the old reader's search path.

There are 377 current golden records and 740 historical records. Fourteen
immutable participant-absence observations have no codec credit: D38 caller
inputs, the actual a174 shape-gate absence and the introduced publication-ending
namespace. A real old queue listing and validator control also observes the new
disjoint namespace without opening or decoding it. Six pre-window proof/upgrader
artifacts remain history controls. Pre-#1018 is not implicitly supported.

## Qualification and retained failures

The baseline action
`35f302be6c7db2d691f3036c02f069636bb56d2b506afc33980c9afc29ef9ba3`
passed 128 relevant cases before the implementation. The recovered original
seven-slot cohort at `b891548bc0d5082bbf721d2750814870fa670109` completed
554 native passes, including all eight architecture cases. All original slots
were published and terminal; none was duplicated after app cancellation.

The current complete-unit gate includes all eight protocol files, standalone
negative controls, window and inventory checks, the whole architecture file,
Core/result/source-pin consumers and the four corrected fixture files. Its
source pin, actual population, primary chains and final status are recorded in
`refactor_c31_status_2026-10-16.json` when verification completes. Earlier green
cohorts do not qualify later source. Native writer branch proofs are compressed
into the actual primary stdout log, so they need no newer optional pytest flag.

An actual Core change emitted `immutable_files` inside closed
`worker_runtime.v1`. Twelve reverse reads failed in action
`570a71c5749d381c8b7454587166208b002525031bb7d6dce374eb13a8160dd6`.
The fix binds the helper digest in attested Core source and checks it before
publication; the old wire shape stays unchanged. The same twelve old consumers
then accepted the new writer, and all twelve historical goldens stayed exact.
`audit_runtime_transition.py` reruns this comparison. An independent isolated
production-writer mutation is also rejected by the unchanged old landing reader.
No historical bytes were repaired to make a test pass.

Other controls reject closure tampering, candidate-path poisoning, counterfeit
code with an old filename, setup-only reader calls, unknown closed fields and
invalid bound identities. Additive and presence-only behavior is checked at
actual consumers. The first duplication failure, fixture defects, empty
selection, usage errors and every mixed RED capture remain in evidence.
The optional `--surface-json` submission was rejected by frozen pytest before
collection and earns zero native credit. No sealed request was edited.

## Rerun and integration limits

Source/data inventory can run locally without importing product code:

```sh
python3 tests/compat/support/wire_inventory.py --root . --out NEW_DIRECTORY --require-complete
```

Run product-backed capture through published `pbrun.py`, with a frozen checkout,
`--anywhere --cpus 1 --demand mem_gb=1 --priority -10`, native threads one and
worker `TMPDIR=/home/rob/pbt17`. Invoke `record_compat.py --generate NEW_DIRECTORY
--kind KIND` inside the admitted action. Preserve every output directory.
`adopt_compat_capture.py` adopts only cases with complete directions or explicit
absence evidence and retains excluded failures in provenance. Never replace
captured historical bytes. Use `compat_sources.py` to recapture published or
prepared source closures; its capture refuses an existing changed output.

The complete native gate uses published `pbtest.py`, no deselection, no
`--basetemp`, and all eight architecture cases. Existing original pbt16 plans
retain pbt16. Every new controller uses pbt17. `audit_compat_actions.py` verifies
terminal/request/log/source/runtime/input/result/receipt and native
collection/call/teardown chains. `map_record_test_nodes.py` maps the original
514 corpus nodes to unique passing current owners.

Private sensor, process, clock and custody observations are explicit fixture
inputs. The corpus proves wire/consumer behavior, not measured throughput,
physical abandonment, broker RootGO or deployed drain safety. Profile cost and
configuration scenarios qualify their selected consumer boundaries separately
from full CAS profile-source authentication.

Metadata references overlap 46 active modules, especially PoolQueue (34 kinds),
Core (17), decomposition (11), produced output (10) and prelaunch groups (nine).
The only production changes beyond the catalogue/window are five ID references,
the Core/helper/result source pin and the GC precondition text. PoolQueue and
RecordStore production code are untouched. Root's future C06 integration must
change current attempt ownership to `endings.resolve_attempt` /
`PoolQueue.attempt_ending` once. Historical closures keep their actual old owner.
If C05 changes `immutable_files.py`, regenerate the Core pin and current runtime
goldens; never rewrite historical runtime goldens.

Bulk evidence is under
`/mnt/shared/prismabuild-agent-evidence-rob/refactor-20261016/active/compatibility-corpus/`.
Frozen original HTML and completion audits remain historical. Whole-refactor
completion, root integration and rollout remain outside this local acceptance.
