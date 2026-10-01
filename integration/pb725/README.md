# Refs #725 — opt-in reader component integration

Integration-only files. Default PB discovery remains `tests/`; this directory
is opt-in and is not a production reader dependency. No scheduler, cache,
reader, pin, API, runtime pairing or production default changes.

## Identities and sealed inputs

- Harness/verifier baseline: `be674a35f30da209075e237a9d380afb857c05c2`.
  The existing `tools/fleet/pbtest_pins.py` must have SHA-256
  `7ec1e3b1a3fcb268f07bc0d92e557567f1de48391161c2c24f3ec7093e1d57e9`.
- PQ production baseline: `a3f8d01a4fdb26cfa81c5f6879ae5b17af367c89`,
  tree `814dbbf3b8cdb591609d1749e614391fe306f1cb`, package subtree
  `3d55c59ef80457fbde8e43ffdb31067220ac0e09`.
- Behavioral PB install **and mover tools**:
  `059953bc3793f539600d333cd3311773e592b0e6`, SDK1,
  tree `35106b5deeb6945bb3929296f12fba2ff7450a40`, tool subtree
  `97c93bf9c091b8e07cb612b01b028688966839d2`.
- Interpreter: `/home/rob/venvs/pq-pb059953bc-tessera-b40c93cb/bin/python`.
  Reviewed PQ's Tessera pin remains
  `b40c93cb73745097e57a1ba4cf5b9eee166c759a` (unchanged; not a serving claim).
- `activation_binding.json` seals operands, selected-input digest, verifier
  digest and prerequisite action/receipt/result/summary digests. The parent
  verified dependency-only action
  `e8215f06c6cfb672641e4930ca96b86010c8f45e37b3400aadde2ffde4268f4c`
  on dl380g10: SDK1, 45 RECORD files, Python 3.14, Torch 2.11.0+cpu.
  This is not behavioral RED. This launcher repeats `verify_install` before
  any behavioral import; that successful prerequisite cannot replace it.

The parent's existing local Git branches travel via supported `--snapshot-ref`.
The worker checks exact commit IDs and extracts only `prismaquant/` and
`tools/fleet/` (plus their container directories), rejecting traversal, links
and nonregular members, into a fresh
0700 directory under its TMPDIR. It records Git trees, tar SHA-256, extracted
file SHA-256 inventory, integration source hashes and actual import origins.
No PB `src/` is extracted or imported. Pinned tools' automatic `src` insertion
points at an absent directory; all `prismabuild.*` must resolve to the verified
installed package. No live source path, source installer, global PQ resolver,
automatic pin selection or activation-payload copy is involved.

## Parent-only submission

Do not run locally. Parent owns GO/admission, priority, serial execution,
receipt verification and review. Ordinary x86 CPU submission under the current
05:00 coordinator GO ignores WINDOW; GB10/GPU remains WINDOW/SSH gated.
The default command preserves the accepted activation regression:

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --cwd /home/rob/tmp/claude-campaign-20260926/wt-sol-prismabuild-sched-15 \
  --snapshot-ref pb-sched-725-pq-source \
  --snapshot-ref pb-sched-725-sdk1-tools \
  --priority -10 --timeout-s 180 --tag x86 --cpus 2 --demand mem_gb=4 \
  --env TMPDIR=/home/rob/tmp/claude-campaign-20260926/tmp \
  --env OMP_NUM_THREADS=1 --env MKL_NUM_THREADS=1 --env OPENBLAS_NUM_THREADS=1 \
  --env NUMEXPR_NUM_THREADS=1 --env VECLIB_MAXIMUM_THREADS=1 \
  --env PYTHONDONTWRITEBYTECODE=1 -- \
  /home/rob/venvs/pq-pb059953bc-tessera-b40c93cb/bin/python -I \
  integration/pb725/run_activation.py
```

One action, three sequential tests, no pytest fanout/shards (one execution
unit, <=2); two CPUs cover caller plus the real mover's single copy worker;
4 GiB aggregate memory; zero GPU demand. No host pin, Docker or hidden shell
runner. A missing ref, unsupported published snapshot-ref facility, missing
installed package or closure/import/setup failure is a named failure, **not
RED**. Do not substitute a different dispatcher or runtime.

## Verified inactive-policy RED witnesses

1. `PB725_INSTALL` proves installed059953 before behavior. `PB725_SOURCE`
   attributes exact source closures and integration hashes separately.
2. All three `PB725_SETUP_COMPLETE` records name actual mover completion,
   fragment composition and material generation; private launch nonce/scope
   and claim match; the CAS manifest is actually bound by PQ's SDK reader.
3. `test_exact_activation_serves_ssd_without_origin_payload` fails with
   **OriginPayloadOpened**, following the actual exact prefetch reader's
   `source.open("rb")` under inactive policy. Its case evidence has
   `allowed_tiers_at_prefetch: null` and the covered origin-open attempt.
4. `test_valid_omitted_reference_refuses_before_origin_payload` also fails
   with **OriginPayloadOpened** (not the expected TierPolicyRefused): a valid,
   separately produced reference omitted from the sealed map/readset is
   attempted at the origin under the same inactive policy.
5. `test_old_inactive_boundary_is_independently_caught` passes **only** when
   that same independent audit tripwire catches OriginPayloadOpened, records
   exactly the covered origin and emits `PB725_OLD_BOUNDARY_CAUGHT`.

PB action `13a57aa6a171512353b72e57c6fb836dcdda1ffc0387707e709bab04e9282926`
completed on dl380g10 with **2 failed, 1 passed, 0 skipped**, exit 1, three
completed setups and attributable origin-open failures (46.62 seconds).
Terminal/immutable identity, full log hashes and released containment were
verified. Earlier action `0a4e3f2a7dd998fc6417bdbde9e53dabac093d810350a491cea88a31ad6ba7e5`
had three setup errors; `48f913cc73838520231f04f193b10560edfeebbebff89f1053c7e3053334b443`
timed out before setup completed. Neither is behavioral RED. The latter's
stall cause is unresolved; diagnostic-only phase prints and a 30-second
faulthandler timeout preceded the completed RED, not a demonstrated speedup.
Do not label a package, missing harness, staging setup or import failure RED.
The audit hook permits stat fences, forbids origin `open` before payload,
and is armed only after actual production and actual PB movement. It changes
no SDK verdict or storage behavior. A blocked origin read can leave the
reader's pool counter at zero; that counter alone is not a RED/strict witness.

JSON evidence is retained in the action's private directory and also printed
as `PB725_CASE_EVIDENCE` (including every imported behavioral PB/PQ origin,
references, manifest, claim, real receipt/fragment/material/map, opens, pins and
post-window lease census). `source-evidence.json` records source inventories.
Parent verifies authoritative terminal status, logs and CAS receipt/result
hashes, not wrapper acknowledgement. Copying these metadata records for
acceptance is distinct from copying activation payloads, which is forbidden.

## Strict-policy GREEN (parent barrier required)

After verified RED, `reader_policy` changed only from `nullcontext()` to
`staged_tier_policy_test_context(DEFAULT_ALLOWED_TIERS)` (`ram,ssd`).
Production and the old-boundary control are unchanged. PB action
`e4725f0b9f90646316d7fcbb08e923ef79a9f80737643c4e02a90430d54f6894`
completed on dl380g10 with **3 passed, 0 failed, 0 skipped** (14 warnings,
52.62 seconds), exit 0 and three completed setups. Parent verified immutable
identity/log metadata and hashes, canonical CAS receipt and result blob, source
file/import origins, and released containment. Its positive read served
2089 stage bytes and zero pool bytes, with zero origin opens, a matching
live pin/lease observed at the actual staged open and no remaining leases.
The omitted reference refused before payload opens; the unchanged inactive
control independently caught one actual origin attempt. The positive requires
exact tensor equality, actual stage descriptor opens with actual SDK lease
records observed independently at open, matching serving `pin_id`/`range_ref`,
SSD-tier identity, `bytes_from_stage == file_bytes`, `bytes_from_pool == 0`,
zero origin opens and no leases after reader exit. The negative must refuse
before any staged or origin payload open. Helper/injection/resolver/policy
state and live environment are restored; all mutations are private fixtures.
Audit hooks cannot be removed by CPython, so they are disarmed on exit and die
with this dedicated action process.

## Source/render extension — verified CPU component RED/GREEN

The serial writer first extracted `pb725_scaffold.py`: private SDK1 injection,
queue/claim, manifest metadata, pinned real mover, fragments/material/map,
origin/stage observation, import attribution and restoration. Activation
producers remain in `test_activation_reader.py`; its three accepted assertions
and strict `reader_policy` are unchanged. Parent-admitted refactor regression
`0cdaa0a658e97b3e9c0f0546cbebdaa2a1d8c3f7608e4c8d14f05ce5c7a9c8ec`
passed all three cases with no failures/skips, 8.28s. Full immutable/log/CAS/result
and executed file hashes were verified; historical evidence is not substituted.

Only `--test-path integration/pb725/test_source_render_reader.py` selects the
new `source_render_binding.json`. The launcher explicitly allowlists those two
paths, defaults to activation, reuses the same `verify_install`/RECORD and
immutable extraction, and hashes the selected binding/test plus shared scaffold.
No whole test-tree extraction or join-helper closure expansion is involved.

Parent submits the same command above with this final launcher argument:

```text
integration/pb725/run_activation.py --test-path integration/pb725/test_source_render_reader.py
```

Three behaviors are independently parameterized over source/render (six
sequential test invocations, no fanout). The RED checkpoint deliberately used
`nullcontext()`; after attributable RED the only behavior change was activating
`staged_tier_policy_test_context(DEFAULT_ALLOWED_TIERS)` for positives/negatives.
Independent old-boundary controls remain inactive. This fixture activation does
not activate production or establish a current SDK3 pairing.

- Source: real `safetensors.torch.save_file`, real `selection_spans`, a sealed
  nonzero-offset covered payload span and a valid omitted span in the same
  shard. `_source_safe_open(..., framework="pt")` is the actual reader.
- Render: tiny CPU BF16 `render_production_weight` then
  `_store_rendered_weight_entry`; one covered shard and one valid omitted shard.
  Actual `ProductionWeightCache` uses SHA-256 binding, 65664-byte serialized
  bound, 256-byte LRU budget, `prefetch(max_workers=1)` and resident `get`.
  Producer expectations are cloned and retained before movement for both kinds.
- The real mover stages both covered entries in one private sealed manifest;
  no payload copying occurs in fixture staging. The strict assertions require
  stage bytes, zero pool bytes, live actual SDK pin/range/digest identity at
  descriptor open, no pool source handle, and empty lease census after exit.
  Valid omissions must refuse `readset-not-staged` (source) or
  `staged-not-serving` (render) before payload.
- Source header `os.pread` is transparently observed and bounded by the real
  producer's header extent. A native opener wrapper calls the **real**
  safetensors constructor, then independently compares exact origin device/inode
  in `/proc/self/maps` and `/proc/self/fd` before/after acquisition. It closes
  the real handle and proves those new kernel handles gone **before** raising
  `OriginPayloadOpened`. Invocation alone or CPython audit is never source
  proof. `NativeAcquisitionUnproven`/`source_observer_limit` is a qualification
  limit, **not RED**; no fabricated reader or fallback witness exists.
- Render uses the independent actual CPython payload-open tripwire. Its inactive
  control uses the **valid omitted shard**: covered inactive PWC still uses stage.
  Source observations and audit events are recorded separately, never conflated.

Recorded RED is **4 failed, 2 passed, zero skipped**, with all six
`PB725_SETUP_COMPLETE` records and genuine native-source acquisition witnesses
in `PB725_SOURCE_NATIVE_ACQUIRED` (new handles plus empty `handles_after_close`).
Both source positive/omitted strict assertions should fail from the released
actual native origin acquisition; both render assertions should fail from the
valid **omitted** origin payload open for the negative, while the covered
positive should fail its strict live-pin assertion (inactive covered PWC serves
stage without a pin). `PB725_RENDER_INACTIVE_STAGE_PIN_WITNESS` must show actual
staged descriptor opens, producer tensor equality, exact staged-byte accounting
and no live refs at those opens; a policy label alone is not this witness.
Both inactive controls should independently catch origin and pass. This
covered-render positive is a stage-without-live-pin witness, not an origin-open
witness. The parent explicitly approved this distinction after inspecting the
pinned inactive stage-selection branch; coverage/resolver remain unchanged. Setup/import/mover failures, scanner limits or wrong
witnesses do not establish RED. Parent owns logs, terminal/CAS receipts and the
review barrier; **no new tests/import probes/compilation ran in the writer**.

Observer patches are confined to each call context and restored even on refusal;
helper/injection/resolver/policy and live environment restore at fixture exit.
CPython audit hooks cannot unregister, so they are disarmed and die with the
worker process. No runtime dependency, package pin, API, wire, score, numerical,
production policy or residency contract changes; #1399's PQ-pin wait is unrelated.

Target ACC-03 remains source + render + activation defaults end to end; this is
only prepared component evidence. INV-03/SAFE-02 and SM-03 assertions are present
and source/render CPU component execution is now verified below. Deployment
and full workload acceptance remain **pending**. No ledger status is promoted.

### Verified source/render action evidence

- RED `3f6b552803d5bb86ba35f0775ad80b57e0a77992f2bd5edc3d68f37e3e6e86b8`:
  4 failed, 2 passed, zero skipped, 10.94s. All six setups complete. Three real
  native origin mappings were independently observed then released; covered
  render consumed 1810 staged bytes/equal producer tensor without a live pin.
  Omitted render reached actual origin. No setup/scanner error is counted RED.
- GREEN `1c1fa755bd6a273bef2899f8865e4e38affad74b1a251ef15c21e33ba92ec02b`:
  6 passed, zero failures/skips, 14.67s. Strict source consumed 128 staged bytes;
  strict render consumed 1810. Both had matching actual live pin/lease at the
  staged descriptor open, zero origin/pool payload, and released leases. Both
  valid omissions refused before payload; independent inactive controls caught
  actual origin acquisition/open. Exact tensors are asserted against producer
  expectations retained before staging.
- Both are ordinary CPU-only admitted actions on dl380g10, priority -10,
  timeout180, CPU2/memory4GiB/native1. Parent checked terminal/immutable identity,
  full log size/SHA and released containment; GREEN canonical CASv3 receipt,
  result blob and executed source/binding/verifier/scaffold hashes also match.
- Initial refactor `803689748c36ca5b1b0f0285c805ddf0f16869f47fe6dd9e0094223e70192a78`
  timed out in actual DiskPacer.wait after two setups; not acceptance or RED.
  Pinned `--unpaced` skips topology validation but still constructs a pacer.
  This non-storage private fixture now uses the existing supported empty
  pool/disks/NFS configuration instead of discovering unrelated host ZFS.
  No production/admission/deadline change or performance claim follows.
- Full-source and configuration-correction independent reviews approved the
  inspected source; final exact-delta and compile receipts remain shipment
  checks, separate from the executed behavioral evidence. #725 stays open; no join, complete campaign,
ACC-03, both-Spark, SDK3, native/container, GPU or performance claim follows.

## Target versus evidence

ACC-03 targets source **and** render **and** activation with defaults; this
directory retains accepted exact-activation and source/render CPU component
RED/GREEN evidence. It does not satisfy full ACC-03. INV-03/SAFE-02 strict origin refusal and SM-03 live pins/release are
validated component assertions in the parent-verified RED/GREEN above, with
independent full-delta source review. The static binding's
`prepared_checkpoint: behavioral-red-inactive-policy` records fixture history;
actual per-case `allowed_tiers_at_prefetch` establishes the executed policy.
No deployed policy, SDK3 runtime pairing,
both-Spark (ACC-05), campaign output (ACC-06), accepted semantic progress,
produced-output (ACC-07), native-container, numerical, performance or GPU
qualification is claimed. The private stage is a real mover-created SSD/stage
logical tier, not a measurement of production NVMe topology or throughput.
No architecture/default/ledger status is changed by this opt-in harness.
