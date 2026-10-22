# Supported-window compatibility

This is the C31/B11 implementation record for the isolated branch
`sol/refactor-compatibility-corpus-20261016`, based on
`416a1eccebaea4ef224e0ddef8f6a1fe424756b4`. The initial T3 checkout at
`2d9ee5191ef6e7ed79c59786b59f1ed59bc22a6f` was clean. C16, B14, B15,
resident-test cleanup and C25 remain in the base. Root owns integration and
qualification against its later C01/C05/C06/C28 changes. This record grants no
publication, deployment or live-store mutation authority.

## Catalogue and inventory

`src/prismabuild/record_kinds.py` records 168 kinds with wire identity, location,
field policy, accepted legacy shapes, actual reader and writer references,
and 475 cases. These comprise 458 production-writer scenarios, 14 caller inputs
and three immutable configuration sources. Caller inputs and configurations
earn no runtime-writer credit. The policies are 64 closed, 103 additive and one
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

The current inventory has 279 versioned IDs plus 16 unversioned record kinds across
source, controls and history. All 168 catalogue entries have current and
historical captures or explicit source-bound participant absences. Other
dispositions are 57 operator controls, 13 identity domains, three derived
projections, 16 retired APIs, one rejected retired schema and 37 test/history
references. The original card's 225 IDs, 15 newer versions and roughly 30 closed
modules were historical estimates, not acceptance targets.

`tests/compat/field_sets.json` pins 312 resolved field sets and 257 symbolic
field expressions across 114 source files. The bell query review in
`tests/compat/field-set-review-bell-1894.json` accounts for two symbolic HTTP
query filters, not durable record fields. Regenerate this inventory with
`tools/maintenance/refresh_compat_field_sets.py` after reviewing every delta.
Symbolic expressions stay visible. The scanner also reports 2,086 serialization sites across all scopes and six
dynamic schema constructors. All six dynamic schema sites have exact source-hash,
owner, line and field reviews. Unreviewed or stale dynamic sites fail the gate.
Discovery alone does not qualify their runtime paths.
`reconcile_record_writers.py` joins source constructors to actual executed-line
proofs from verified native logs and reports uncovered constructors explicitly.
Fifty-four reviews pin each constructor to its source hash, owner, line and
fields. They distinguish internal normalization and private qualification
inputs from active writers. Filename
and validator-prefix exemptions are forbidden. The active canary
`pbcanary_legs.leg3.manifest_for_run` has its own reproduced writer scenario;
its incomplete `build()` skeleton earns no standalone wire credit.

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
The latest check at the final source pin captured complete status at
`2026-10-17T02:40:36.856874+00:00`: dl380g10, sparky and sparklina were healthy
and live, all reporting the designated a174 commit. The deployed and prepared
closures remain byte-verified. A future rollout must rerun this check. A saved green capture is not permanent
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

There are 475 current golden records and 936 historical records: 467 from the
live/designated a174 closure and 469 from the prepared bridge control. Fourteen
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
verified final population is 1,419 native nodes: 1,401 passes and 18 existing
recovery capability skips. All eight architecture cases passed, with no
deselection. Twenty successful primary chains bind the exact frozen source,
logs, runtime, inputs, CAS receipts and completed native teardown. Producer
input lists and actual local executable bytes also match. The skip requires
CAP_CHECKPOINT_RESTORE or CAP_SYS_ADMIN for `map_files` identity; it grants no
physical abandonment proof. Earlier green cohorts do not qualify later source.

The final frozen source is `1925ce605158951f58837fb36765e1962dfab77e`;
`complete-unit-04` repeats the relevant 22 whole files after adding the active
canary writer and correcting source-reference resolution. The previous
`complete-unit-03` at `6990d6ac9c592cbdf087504c2af95e4e8285d4d7` remains
unchanged and is audited separately. The final reconciler joins all 475 actual
case proofs to the golden wire hashes and all 358 production constructor sites:
227 executed constructors and 131 source-reviewed identity, normalization,
projection or control sites, with zero uncovered sites or cases. All original
514 actual-consumer nodes map to unique passing current owners. Native writer
branch proofs are compressed into the actual primary stdout log, so they need no newer optional pytest flag.

An actual Core change emitted `immutable_files` inside closed
`worker_runtime.v1`. Twelve reverse reads failed in action
`570a71c5749d381c8b7454587166208b002525031bb7d6dce374eb13a8160dd6`.
The fix binds the helper digest in attested Core source and checks it before
publication; the old wire shape stays unchanged. The same twelve old consumers
then accepted the new writer, and all twelve historical goldens stayed exact.
`audit_runtime_transition.py` reruns this comparison. An independent isolated
production-writer mutation is also rejected by the unchanged old landing reader.
No historical bytes were repaired to make a test pass. The six superseded
current-runtime goldens remain byte-for-byte in
`tests/compat/history-controls/superseded-current-runtime/`; their actual Core
source digests changed when commit `96daf6252` relocated the generator path in
a Core comment. Even that comment change alters the attested source hash.
They are provenance controls, not the final current writer bytes.

Other controls reject closure tampering, candidate-path poisoning, counterfeit
code with an old filename, setup-only reader calls, unknown closed fields and
invalid bound identities. Additive and presence-only behavior is checked at
actual consumers. The first duplication failure, fixture defects, empty
selection, usage errors and every mixed RED capture remain in evidence.
The optional `--surface-json` submission was rejected by frozen pytest before
collection and earns zero native credit. No sealed request was edited.

The actual generator exposed nondeterministic landing/mover bytes when fixture
clock and hostname seeding ran after the new builder dispatch. The original
current and historical capture files remain unchanged in
`tests/compat/history-controls/rejected-nondeterministic/`. Seeding now precedes
all dispatch, and the new captures reproduce independently. A separate guard-only
run against the unchanged faulty adapter fails before generating a golden.
This is a generator control; the unchanged old-reader production mutant above
provides reverse-consumer rejection credit.

## Rerun and integration limits

Source/data inventory can run locally without importing product code:

```sh
python3 tests/compat/support/wire_inventory.py --root . --out NEW_DIRECTORY --require-complete
```

Run product-backed capture through published `pbrun.py`, with a frozen checkout,
`--anywhere --cpus 1 --demand mem_gb=1 --priority -10`, native threads one and
worker `TMPDIR=/home/rob/pbt17`. Invoke `record_compat.py --generate NEW_DIRECTORY
--kind KIND` inside the admitted action. Preserve every output directory.
The generator invokes each selected current and historical writer twice in
independent isolated processes and requires byte equality before writing a
golden. `adopt_compat_capture.py` requires `--primary-audit` from the successful
source-bound PB chain, exact request output path, complete directions or explicit
absence evidence, and the independent-reproduction flag. Partial producers,
ENOSPC/no-CAS results and nonreproduced captures cannot become corpus inputs.
Excluded failures remain in provenance. Never replace captured historical bytes. Use `compat_sources.py` to recapture published or
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

Current and legacy metadata references overlap 52 modules, especially PoolQueue
(42 kinds), Core (17), produced output (15), decomposition (12) and prelaunch
groups (10). This overlap is an integration review obligation, not a request to
move those owners or create aliases.
The only production changes beyond the catalogue/window are five ID references,
the Core/helper/result source pin and the GC precondition text. PoolQueue and
RecordStore production code are untouched. Root's future C06 integration must
change current attempt ownership to `endings.resolve_attempt` /
`PoolQueue.attempt_ending` once. Historical closures keep their actual old owner. Root also reports a qualified
C06 private FD schema 1 in `client_channel.py`: two strict atomic JSON records
between parent and pbrun/recovery from the same `RUNTIME_ROOT`. It is an
invocation control with no durable queue record or worker consumer. Root must
add a source-bound control disposition at integration, rather than invent a
historical participant. That later source is absent from this frozen gate.
Root evidence is `active/c06-root-design/frontend-callers-r2/qualification.json`
and `client-exit-site-audit.json`.
If C05 changes `immutable_files.py`, regenerate the Core pin and current runtime
goldens; never rewrite historical runtime goldens.

`compatibility_acceptance_2026-10-17.json` records this local unit only. The
current staged-read policy and unrelated ledger axes remain unchanged. No
power-loss durability or staged application workload is qualified here.
An older optional ARM capture remains uncredited; the final native population
ran on dl380g10.

Bulk evidence is under
`/mnt/shared/prismabuild-agent-evidence-rob/refactor-20261016/active/compatibility-corpus/`.
Frozen original HTML and completion audits remain historical. Whole-refactor
completion, root integration and rollout remain outside this local acceptance.
