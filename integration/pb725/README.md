# Refs #725 — opt-in exact-activation reader integration

New integration files only. Default PB discovery remains `tests/`; this directory
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

Do not run locally. Parent owns WINDOW guards, priority, serial admission,
receipt verification and review. The supported command refuses an active
window immediately before starting the client:

```bash
test ! -e /home/rob/tmp/claude-campaign-20260926/tmp/u4-release/WINDOW_ACTIVE || exit 75
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py \
  --cwd /home/rob/tmp/claude-campaign-20260926/wt-sol-prismabuild-sched-11 \
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

## Target versus evidence

ACC-03 targets source **and** render **and** activation with defaults; this
file prepares only one exact-activation component. It does not satisfy full
ACC-03. INV-03/SAFE-02 strict origin refusal and SM-03 live pins/release are
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
