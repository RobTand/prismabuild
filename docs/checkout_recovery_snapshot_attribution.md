# Checkout-recovery snapshot attribution (#2043 / #2044)

## Follow-up on 2026-10-25

The [current-fleet report](measurements/1711-current-fleet-2026-10-25.md) recovers
and authenticates the original `full.json` and all seven tracebacks. It supersedes
the missing-report and missing-traceback statements below. Original pytest output
still truncates the changed queue leaves, so historical writer attribution remains
unproved. A fresh x86 run passes 279 tests and skips 18 native-positive proofs.

## Disposition and evidence boundary

W1 remains unresolved: none of the seven historical failures can be attributed to a specific changed leaf or writer from the available assertion evidence. This report preserves the bounded unresolved inventory; it does **not** claim historical attribution acceptance. W2 changes only preservation reporting in the contract helpers. Fresh passing tests qualify the current tree, not the historical cause.

Historical custody below is inherited from the supplied read-only recovery brief, not a claim that this implementation session independently recovered the unavailable full log. No new reference to original `full.json` became available; absent reads were not repeated. There is no evidence that the report is lost everywhere and no basis for requesting credentials.

### Original identities and access outcomes

All queue/CAS paths here are relative to stores resolved by the earlier recovery through FleetStore/configured_store; these are evidence identities, not deployment configuration.

* Action: `7bd57dbb47e9ce95ab288cb9c10d7c6ab5f449ff10bd37a0eeb969f659ed8f3f`.
* Readable `failed/<action>.json`: 82,558 bytes, SHA-256 `3c7214266d57921689ced2e0962a93cbab19485cd430888708431653a70d0909`.
* Readable `attempts/<action>/4f50a44ed2776f23619fa01202716e22bda51286b5cda60e9832276f1b8f9db2/00000001.json`: SHA-256 `b5d1226ec796e57faf271b3e12d97c4eeb2ebfc76782bd7b18099a4404898658`.
* Terminal: exit 1, **24 failed, 1990 passed, 18 skipped** in the broader run, not only these seven.
* Recorded full stdout: 1,389,592 bytes, SHA-256 `43b8a4d3f2518324f7a43da9cacaad9dc9741fd32f3a34ed69cc028ddc1d01a9`. Issue #2043 reports earlier successful verification. Later recovery got FileNotFoundError at `attempts/<action>/4f50a44ed2776f23619fa01202716e22bda51286b5cda60e9832276f1b8f9db2/00000001.stdout.43b8a4d3f2518324f7a43da9cacaad9dc9741fd32f3a34ed69cc028ddc1d01a9.log`; the attempt directory exposed only the JSON record and the exact CAS blob candidate was absent. That recovery did not independently repeat full-log verification.
* Terminal excerpt: 65,536 bytes, `stdout_truncated=true`, decoded UTF-8 SHA-256 `fbfc4dd48f57a709254e236afc5ca24e1fdf5e70692f93396769134fe6b67020`. It begins mid-outcomes data and retains all seven literal FAILED summary lines, **no corresponding tracebacks or assertion actuals**. The remaining 1,324,056 bytes were unavailable through those reads. This digest is not the full-log digest.
* Original `full.json`: unlocated. Neither failed record nor sealed request linked it. The sealed task names `pbrun_result.99ed075d080529f1.txt`; its readable local-result claim binds action and checkout but has neither payload digest nor output bytes. The exact v3 action-receipt candidate and claim-specific staging directory were absent. Bounded listings of exposed evidence/results/investigation locations supplied no original report linkage. These are unresolved access/linkage dependencies, not global nonexistence proofs.
* Sealed bundle `blobs/24/245904b606241d424b9d461e1723af18adb63c14423b978e42b9bd3ef9a9578d`: independently hashed by earlier recovery, 13,971,750 bytes, SHA-256 equal to filename. Snapshot `1794a5d7561a81f1ea5f69642cd028d715c46bad`, parent `9c85d1156bbc08432460c14cbe7c7192cc690677`.
* Sealed test blob `1e745d1af74be59716f0febbca1868355b17fba2`: SHA-256 `a68bb44297fb1125ada9aa1484d07907d1780f7431880269658f6c59de4d9e86`. Recovery implementation blob `db66394e60b45b26f9fdf2fbf46b6fa2d54deb03`: SHA-256 `73e92b86b46c73411e901373cde5a5527b0a3e68408009491f811d6cb7b9c60d`. No acceptance test requires these Git objects.

## Seven individual attribution rows

Every selector is in `tests/test_checkout_recovery_1465.py`. For **each row**, retained evidence is only a FAILED summary: the exact failed assertion/phase, actual changed snapshot root and leaf, old/new type/device/inode/UID/mode/size/hash or symlink target (and any underlying bytes), and historical writer/process/timing are missing. Intended mutations below are not observed preservation deltas. Nested/shared snapshot views cannot establish separate writes or an equivalence class without actual paths and identities. No mutation count is established.

| Exact selector suffix | Sealed boundary and individual missing evidence | Conditional settlement coverage and focused W2 result |
| --- | --- | --- |
| `test_archive_is_a_complete_stable_nofollow_copy_not_merely_a_digest[symlink]` | Starts at sealed line 553. Archive is replaced by a symlink before preservation. Missing assertion output cannot distinguish refusal, schema, cleanup, or preservation failure in `_refused`; no actual archive/queue path or old/new tuple retained. | Settlement plausibly covers fixture-started diagnostic drift only, not a demonstrated archive difference. Exact selector passed; refusal reporting is now deterministically exercised. |
| `test_authentic_banked_ending_is_planned_then_removed_only_by_cleanup_owner[root-lease_lost]` | Starts at 357. Planning, identity/archive assertions, apply, and protected-state checks are possible boundaries. Missing traceback cannot show whether apply was reached; no changed leaf, metadata or writer retained. | Plausible for planning-time diagnostic drift, not proof about later cleanup/protection assertions. Exact selector passed with real admission and existing settlement. |
| `test_apply_revalidates_every_plan_identity_and_lifetime_before_deletion[directory-replaced]` | Starts at 750. `_planned` precedes deliberate directory replacement. Unknown whether failure preceded replacement or occurred in apply refusal; no before/after inode values or actual changed root retained. | Could cover diagnostic drift at either helper; does not establish a historical revalidation defect or repair. Exact selector passed; both helpers' reporting covered separately. |
| `test_apply_revalidates_every_plan_identity_and_lifetime_before_deletion[ready]` | Same sealed test at 750. READY publication follows initial planning. Missing assertion/phase, paths, metadata and writer prevent distinguishing planning drift from refusal drift. | Same conditional coverage, no historical READY-lifetime conclusion. Exact selector passed including current refusal behavior. |
| `test_real_kernel_mount_census_cannot_hide_external_banks_or_processes[unreadable]` | Starts at 1284. Injects PermissionError at real mountinfo reader. Unknown which `_refused` assertion failed; no changed root/leaf, tuple or writer retained. | Plausible only for unrelated fixture diagnostic drift, not proof of historical mount-census behavior. Exact selector passed; deterministic refusal diagnostics passed. |
| `test_anonymous_inode_label_rejects_every_ascii_control[ascii-14]` | Starts at 1920. `ascii-14` is hexadecimal U+0014. Explicit regex assertion, `_refused`, and error-text assertion are separate possible failures. Missing assertion plus path/metadata/writer evidence precludes choosing one. | Plausible only if snapshot drift was the failure, not regex/error-message failure. Exact selector passed, separately from synthetic drift reporting. |
| `test_real_operator_open_fd_is_live_even_when_it_belongs_to_this_pid` | Starts at 878. Opens selected library, supplies same-PID FD evidence, checks refusal and PID/inode error text. Exact failed boundary and changed paths/tuples/writer are unknown. | Plausible only for diagnostic drift, not historical FD-census attribution. Exact selector passed including current same-PID protection. |

W1 would need original assertion output correlated to these artifact identities for every row before attributing a difference. Passing W2 cannot supply that historical evidence.

## Sealed-source writer trace and existing settlement comparison

The supplied sealed-source inspection found no publisher settlement and no `_snapshot_delta` in the historical fixture. Historical `_snapshot` (lines 50–74) records type, device, inode, UID and mode, plus file size/hash or symlink target; it does not record timestamps or GID. `preserved` (138–150) covers selected checkout, local checkout root, bank, CAS, queue, maintenance gate, process fixture, source and protected trees, including alternate entry spellings. Only `queue/transition-locks` is excluded.

Candidate chain in that source:

1. `pool.py:21833–21845` flushes claim diagnostics in `finally`, even with `adaptive_cpu=False`.
2. `_write_claim_diagnostics` at `pool.py:9713–9764` writes local `claim-denials.json` and invokes `adaptive_snapshot.publish(base, ledger.base / 'adaptive')`. ResourceLedger places this under `queue/reservations/<host>/adaptive`.
3. `adaptive_snapshot.py:68–107` starts a child and returns without waiting; `copy_snapshot` at 122–137 copies six available diagnostic JSON records. `Controller.locked` is another publication entry point.
4. `materialize.py:130–152` writes a fresh temporary inode then `os.replace`, with no same-content no-op. Thus unchanged JSON bytes can still change protected identity.

This demonstrates a possible asynchronous writer, **not** a writer attribution to any historical row or leaf. No recovery-owned mutation or new settlement gap was demonstrated.

Commit `47753b959b6933bc95734a7cee1eb081331b1d57` is already integrated (ancestor of the inspected base `b675ee50e52856c0c783fb29639e2cb035fe4e68`). The current fixture retains its wrapper of the real publisher, tracking only returned non-None child handles, waiting in fixture claim `finally` with timeout=10 and zero-exit assertion. This session inspected that ownership and left it unchanged. It is bounded failure-reporting synchronization, not a sleep, disabled diagnostic, module-global drain or broader queue exclusion.

## Reporting-only change and preserved contract

Both `_planned` and `_refused` now take one before and one post-call snapshot on the preservation-check path and report `_snapshot_delta(before, after)` from exactly those objects. Equality remains over the full snapshots; root coverage and type/identity/mode/size/hash comparisons are unchanged. The delta is converted to a string so pytest's object repr shortening cannot elide tuple fields in the assertion message. Synthetic content and identical-content inode changes test diagnostics, not the historical writer.

No production code, acceptance tests, schemas, SDK signatures, compatibility inventories or goldens changed. Real admission, read-only planning, no deletion on refusal, validation of all selected entries before deletion, maintenance/process/archive checks and the distinction between pristine refusal and incomplete destructive cleanup remain intact. Production record/Core behavior is not implicated; if later evidence implicates it, release-gate namespace record-compatibility capture and strict runtime adoption would be required, never hand-edited goldens.

## Current-tree verification

Commands ran through `run_tests` as plain commands inside admitted actions, not nested submissions. Implementation-session starting HEAD was `3aa40754ecb45bfbd841d6cb8929181a6941e547`. Final tested contract SHA-256: `5405ef32f511abf4fd5a72b21726085be61ab2eafca4958b1f859f7d54de1b1f`; unchanged acceptance file SHA-256: `11307f17fe96ed5e587373e4693daba3231d33f8f6874c0fa41939d0ed4de9ee`.

* `python -m pytest tests/test_checkout_recovery_preservation_reporting.py -q --basetemp="$TMPDIR/pytest"`: initially 4 failed (planning took three snapshots; refusal never called delta). First two-snapshot implementation still failed all four because pytest shortened the dict assertion message; full actual output retained below. String formatting fixed this: **4 passed** on x86/Python 3.14.4.
* Seven individually quoted node IDs from the table, passed to `python -m pytest ... -q --basetemp="$TMPDIR/pytest"`: **7 passed** on x86/Python 3.14.4.
* `python -m pytest tests/test_checkout_recovery_1465.py tests/test_checkout_recovery_preservation_reporting.py -q --basetemp="$TMPDIR/pytest" && python -m pytest tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"`: **279 passed, 18 skipped**, then **109 passed** on x86/Python 3.14.4, and **279 passed, 18 skipped**, then **109 passed** on ARM/Python 3.12.3. Skips are not passes; these quiet runs retain skip counts but not individual skip reasons. No submitted check was unavailable in this session.

These results qualify already-integrated settlement plus the new reporting change; they do not reproduce or explain historical differences. Full output (including assertion failures) is retained by the following receipts rather than copied into the repository.

### Test evidence receipts

Initial red reporting regression:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"d3c0651e4b5fb5b0f4507372aa5b5536836c5eb295a37465a99be8cfc2e1ca66","size":12136}},"operation":"2fd462f3895016b5316d169559afaea4dd956e5a89c60d52691c6e896e84260b","receipt":"af80f0e8c0547787afe6be7133b0c2524edf6ec81664ce096d1a8b226515d822"}]

Intermediate failure (shortened object representation):

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"0d6794a41b9351efd47207fdb570df364d71395d1be709f5faf017c723078997","size":20794}},"operation":"df49b195642818c2baa466c7269ce1c30683f2d4556bfe1a4bf1c4eaf4f09814","receipt":"a1d0cb415ecf1066cadc6c2c15474a1a07d5746b78e572061c4d5c475974dc8e"}]

Final reporting:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"199acfd6f417aa0c016294f5fb7803f2ef80cc6a07eecc3728682487258a1dd7","size":1188}},"operation":"0847cf2aee297fa6c23a1672744fcb92386ebd1eb4116a6dad9b3653d53c2cc1","receipt":"101e1afebbab08bf2738bf68e9395d8986fc556c2458219137d479a73b529362"}]

Seven selectors:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"0601674e01e75bcefec4eef4a40cb84fc128545fe6af13863baf37606799f775","size":1938}},"operation":"289195cfb71029ccee73d111394aaf18d4f363a2027fb04ca33dadea8ebac345","receipt":"8f18ac39b11b95f7494ef7683712302be253667a8e31d955d60606a5a2e5b5c2"}]

Full recovery and four guards, x86:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"bc6aeb91026b0ee165477b13bae946a9725054d34d8a7efb8e69b45f1dbfa5e6","size":3511}},"operation":"c1e46f0e9f7bf5d8a80dfe1d4aef8d2b563ad05c3b2ac6ba981814508931eeef","receipt":"52b9b1cbfad947026a872f74f5e76e8acdcfac1f1fc8f269557a0771e110e327"}]

Full recovery and four guards, ARM:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"d18746bec58833c1ce68d4b8c998c9a3403f1c41f45608712e861aaf35cf11b9","size":3528}},"operation":"ae4f7b4e32eb83c595f7711add0ac04ea704ce4cc19ad0f1b7f486c48a1d65c3","receipt":"00e98e6de01255aa36724e371eb5680813e4ba3533a0f6f21960ad3ddf6eb18b"}]
