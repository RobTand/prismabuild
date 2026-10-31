# #1543 terminal plan confirmation validation

Implemented the remaining terminal-plan claim-pass path using the existing durable
confirmation and teardown owners. The existing locked terminal-lead proof remains
a separate helper. No public verdict, SDK signature, field inventory, or golden
was changed. The acceptance overlay was not edited.

## Review repair: resident supersession and one-shot uncertainty

The initial 162-case results below did not cover two reproduced gaps. Added
`tests/test_gang_plan_confirmation.py`, using the existing private-pool fixtures
without modifying the acceptance overlay. Before the repair, its three cases
failed on x86: memory-blocked resident supersession kept fencing, unblocked
resident supersession launched, and a one-shot verdict read failure reused the
aged mark. The final regressions also exercise both the first and second plan
lookup with OSError and temporary absence. They require a fresh confirmation
window, normal withdrawal, LOW admission, and preservation of the executed mover
and unrelated incumbent holdings.

The claim pass now probes terminal plan evidence even when residency is ready,
and defers a superseded member's admission during confirmation. The plan owner
privately reports read uncertainty within a scoped context, so a later successful
read in the same verdict cannot erase the reset. Ordinary absence of a filed
plan still permits the existing independent terminal-lead proof. Public verdict
and SDK shapes and locked lead-generation proof remain unchanged.

Final command on both arms:

```sh
python -m pytest tests/test_gang_plan_confirmation.py tests/test_gang_plan_terminal_1543.py tests/test_gang_residency_members.py tests/test_gang_teardown_with_prelaunch_1543.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py tests/test_client_sdk_surface.py -q --basetemp="$TMPDIR/pytest"
```

Observed: **339 passed on x86/Python 3.14; 339 passed on ARM/Python 3.12**.
Final diff and `git diff --check` inspected. No golden, field inventory or
acceptance-file changes. Record compatibility was not rerun during this repair:
the independently confirmed PB-owner namespace/runtime prerequisite below
remains unresolved. These passes are not release qualification.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"eb356b97239e1841764f1ab851c2fdac08f87f2cc0fe2e0ae42d9c23eaf00078","size":2990}},"operation":"34408385784b760f59015b9b2879c4b55d27f8e95c8b5b2870a5ae47322686d7","receipt":"6e38864bd791118f86028e43e1a04c87d0f1a84015a341ce0821eb9f8a3ee80a"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"d374cb48ef50f9d119ffbdf7e37d3086d74bd9c6cee2bc65683b4505a1bf17f5","size":2991}},"operation":"2d0d950b3c1e51f08de869259caa035823ed3b509cbb6e25dec8b865dbb63996","receipt":"2241587f6f52dbab56118429f0fddc1cbe369b17d687a71f4f9a0be1bb1e6790"}]

Pre-repair reproduction command: `python -m pytest tests/test_gang_plan_confirmation.py -q --tb=short --basetemp="$TMPDIR/pytest"` (3 failed).

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"22aa6af555d9655a78e181acf19826439198538646f7ae9f942d2657cd81aac9","size":3845}},"operation":"5d3ab11f2352fe347c263a921cebe51f5ba3d2b151f47cf48b8aba7deb03d667","receipt":"4e9ad9e28797b8401b0d1a8919d71eda0d0baceee39c2dfb58d5c4d32b731fc8"}]

Development checks also exposed and resolved a test-double staticmethod wiring
error and an overbroad reset on ordinary absent plans (35 existing lead cases
failed before absence was distinguished from actual read failure). The final
combined results above supersede those intermediate failures.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"befdfc7ad7bab088aa7719b4baa87d1dc5514f5439eaf1121c4857ca49b8331b","size":4597}},"operation":"daac97aa708ffd0de0f2f83b09275b67a436a5034d47e3aaece215147cc7872d","receipt":"eeadee66b2ba87c8448031903c21c5a54c82670970fe6133c27a301c42759482"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"276cac445c075ca751bf0f0186d5c28b2c0f2c88a154b8d34afc865bb1b28868","size":50158}},"operation":"7a3541d94713a0d1bddd2e0a64d4d53fa25523ef9b1df4394885d448ed93114a","receipt":"18860863ff153a2d2839d912f78c06198503e46721ea0705dfbcb7c596027d6b"}]

## Initial acceptance and repository guards (before review repair)

On each arm:

```sh
python -m pytest tests/test_gang_plan_terminal_1543.py tests/test_gang_residency_members.py tests/test_gang_teardown_with_prelaunch_1543.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

Observed: x86/Python 3.14: 162 passed; ARM/Python 3.12: 162 passed.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"ceac55630e730d314566300a99aaeedb725b423ee322754159e5468e3daba608","size":2467}},"operation":"0044c56d2db437d9882b5373b761497b177c649021ad37da81d97d0dbf5e03f1","receipt":"ece1cb800b24f4ce6d106c5670458d75c222071cda40903c6a165536e13c5ccd"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"f5b6a3041f58a71507f732094fe1daf433a0659ebb5d3b73d4ac1b6aadf12a6d","size":2470}},"operation":"3ebab9968618baa4230db96724441663179d8e0232d4af761f72d0cd88b8d75c","receipt":"2a9b2562237c84068f88055645650b6957df73c9f52891a9629bcb80b79dd088"}]

## Record compatibility: not qualified

The full x86 command was:

```sh
python -m pytest tests/test_record_compatibility.py tests/test_record_compatibility_*.py -q --basetemp="$TMPDIR/pytest"
```

Observed: 472 failed, 391 passed. The retained output is truncated. This is not
compatibility qualification. To obtain readable failure attribution, a bounded
first-failure command was subsequently run on both arms:

```sh
python -m pytest tests/test_record_compatibility_queue.py -q -x --tb=short --basetemp="$TMPDIR/pytest"
```

Each stopped at `q-census-report/first` before the old writer could run:

- x86: `RuntimeError: archival replay runtime does not match its explicit qualification`.
  Selected qualification pins TMPDIR `/home/rob/pbt17`; the observed admitted
  run has its own TMPDIR. This matches the existing #2178/#2162 scope, not
  evidence of a new gang record incompatibility.
- ARM: `bwrap: execvp /home/rob/venvs/pb-cpu/bin/python3.14: No such file or directory`;
  the selected archival runtime is the x86/Python 3.14 record, not the native
  ARM/Python 3.12 runtime. No historical writer result was obtained.

PB owner prerequisite: supply the release namespace lane with an available,
explicitly qualified archival runtime and admitted TMPDIR on each arm, then rerun
compatibility. No runtime qualification, capture, strict adoption, golden edit,
or field-inventory edit was attempted. Whether a new capture/adoption is needed
cannot be established while archival replay fails before the historical writer.
Work stops at this release-validation prerequisite; acceptance passes are not a
release or deployment claim.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"2efd0d178f25ebb819a67980d300ef355d187b367c51f58519cd49e219247714","size":1048576}},"operation":"1ebefc00e40d09dce2faafb25a61e1f9a7a26c0bd444905af9232c50585c9c8b","receipt":"33d109c32f697880189c341acf1f8fad79744b365c3b87495dfd18a2105043fd"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"0f323f3a5712f615c4d4179fecf23d0c211816a3c87d26b50852742e7e250887","size":120747}},"operation":"d6132ad3a14b8911c389091f8d48eb886f3e8b53d8d7cc5d08c2854f9199d9d8","receipt":"db72d492b8270cdc43a788dbce5c50eeb6a94879c946826a88ba38b7ad6585e6"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"b12b512c1aae481aa081c3c2907165a41573b908b6dc5bd2ac72c19e83e82fa2","size":121109}},"operation":"11c7ee28efce4cfb7bdd78ad53adb5c2b31c88eea358344cf41d52a29ec12e98","receipt":"9aace63e2285d0e7a9955a186fc6a930555f0887ad0014294b3a6bbde1a26fff"}]

## Stat uncertainty repair, #2323, 2026-10-26

Independent review reproduced a retired gang member claiming after an actual
plan-stat fault. Action `0cdf3a5c872326cf91d0a9be7760bef4b5addc09602924f5eef37c4b1ce98470`
failed one of two tests. A second deterministic control, action
`19c8fb5e9724a14b7f7fd1665d7879efe4a06b248c4c73918432a6a453905412`,
failed one of 18 tests when fresh verdict uncertainty reused aged confirmation.
Neither failed action has a successful receipt.

`incarnation` now returns `None` only for genuine absence and propagates other
stat errors. Its four existing best-effort callers retain their own deferral
contracts. Admission, terminal-plan reproof and the final locked lead-verdict
refresh defer on unavailable or changed filing observations and clear the
confirmation window. A genuine no-plan lead-only consumer retains its separate
locked proof. The original acceptance overlay is unchanged.

Source `ef29f71e2e1352be45b5c5d78e9ca97d7b83809c` passed 443 tests in 17 files
on x86 with Python 3.14 and ARM with Python 3.12. Four further rounds of the two
new regression files passed 44 tests each on each architecture. Both files
therefore have five executed runs per architecture, all with two xdist workers.
There were zero failures or skips in these 50 actions.

[Qualification](../evidence/issue2323/qualification.json) records every action,
receipt and log digest. The retained verifier read terminal records, native
call and teardown coverage, cleanup, both immutable logs, CAS receipts/results
and the actual source bundles. The tested `src`, `tests` and `tools` trees match
the qualified commit. Each shard reserved two CPUs, 4 GiB memory and zero GPUs,
with one native thread per worker, priority -10 and an 1800-second deadline.
Exact published pbtest commands and reports are retained under
`/mnt/shared/paperclip-runs/PRI-150` and in the Paperclip evidence attachment.

The [merge acceptance record](../evidence/issue2323/merge-acceptance.json) and
scoped ledger entries cover this component only. The historical compatibility
release prerequisite above remains separate. This task publishes no runtime,
changes no generation, restarts no service and proves no campaign completion.
