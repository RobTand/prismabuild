# #1543 terminal plan confirmation validation

Implemented the remaining terminal-plan claim-pass path using the existing durable
confirmation and teardown owners. The existing locked terminal-lead proof remains
a separate helper. No public verdict, SDK signature, field inventory, or golden
was changed. The acceptance overlay was not edited.

## Acceptance and repository guards

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
