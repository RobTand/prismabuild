# #2199 shared-census review repair

Scope: repair the two safety findings against PR #2221 at `b8ea84e6`.
All checks below used the provided admitted test runner, fixtures only. No live
lock-state probing, queue mutation, publication or deployment was performed.

## Behavior

- A sibling's cache omission does not retire this pass's remembered selections.
  Their local M keys are taken before H, live fences remain, and busy keys and
  retained holders can still restore a fence.
- Optional source-generation preparation and verification use an independently
  guarded, durably owned bounded child. It uses the existing refresh budget and
  descriptor-isolation/ownership handshake, but a distinct section and marker.
  A busy or unsettled source owner disables cache work, not a complete census.
  The ordinary proof still does not acquire the census reader fence.
- The new fixture parks a source callback at preparation, post-read verification,
  or shared-cache verification. It deliberately retains the timed-out child,
  verifies sibling M/H acquisition while that child remains alive, verifies that
  a fresh reader cannot replace it, and verifies that the main reader still works.
  Cleanup releases/signals and reaps only the fixture's own child.

## Reproduction

With production repairs temporarily reversed, the remembered-selection fixture
failed with a missing selection after sibling publication. The source-read
fixture exceeded its five-second watchdog while the inline callback retained H,
then completed after the fixture released it. The source reproduction run also
exposed an initial fixture signature error in the other selected case; the
remembered-selection case was corrected and rerun independently.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"e320e59198ff117367cf6e0c7ea2e920b155ff15884fd5bb5a394136676f92db","size":4579}},"operation":"560c0e98b66149d61b433250adcfdf54a3b53653682ee6d6ba6568caed779c3e","receipt":"5c8f7417d1d380f39964ff3b56317187e3b1293fc4d883ecd5a4b28fbfaf00af"}]
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"ba78e06e4fe4d61c841b85ea837c25a4bd8bc6167e5a74c32310c0c544739653","size":16351}},"operation":"3096b74fdbad8fc9aa834cc1ea1898d0299213536527b024a8bef3f9bf590bfb","receipt":"903958466126617d9fd1f06ded02b9a81e2714c71c898e4c43c70b2686694ded"}]

An earlier development run timed out because the initial fixture parked a second
callback during cleanup. The fixture now parks only one invocation and releases
it in `finally`; that incomplete run is not evidence of production behavior.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"add8e39db5bb7efe4d3a3528c80c7aaaf28f71511f0093e3d884d14aeac43273","size":4}},"operation":"d597fd77e1164e6e6f6cc2fde8ea4fb2988559edaea2977d2427085c009d0de3","receipt":"bacbb146431c927a3cbc69b78d16bbc8819b1584c33cc858143dda88f544f0da"}]

## Final focused checks

The same command completed on x86/Python 3.14 and ARM/Python 3.12:
**385 passed per arm**, including eight new safety cases and all four mandatory
guards. The 37 warnings per arm concern the existing owned-reader fork from
threaded fixture contexts. The independent owner's distinct section keeps the
refresh-timeout fixture observing election refreshes, not optional metadata work.

```sh
python -m pytest tests/test_shared_census_safety_2199.py tests/test_ordinary_census_admission_2199.py tests/test_measurement_reservation_census_1419.py tests/test_census_admission_nfs_slow_1498.py tests/test_census_refusal_is_once_per_pass_1571.py tests/test_census_reuse_transitions_1571.py tests/test_census_reused_once_per_pass_1571.py tests/test_census_reuse_reader_custody_1571.py tests/test_measurement_census_reader_fence_1498.py tests/test_census_tmpfs_state_1451.py tests/test_bounded_reader_ownership.py tests/test_claim_arbitration_histories.py tests/test_claim_completion_histories.py tests/test_kept_read_clock.py tests/test_pool.py tests/test_pool_unreadable_ready_record.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"bb1af532551265e18bb90a4de04ef98152c210ba2d15f89c2eb24b6c7c05040d","size":5945}},"operation":"29d903568c93f7b324f689c5bb76ad947e1cfbb6257b54551488b5bcac3fc22c","receipt":"e8858e3575258da0579204f376849c41b3f4616497fc3629c968ce189af330b3"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"7b2d6bb2e8ec67a8aceecfd2e97b877d1e829daae043bbe5b67795541be7f8a9","size":5959}},"operation":"c6617ecc8946d83ef73d98d4c9034a33ee252008a63cbc6e0ece9c2595f1643c","receipt":"8ad2bc1c3756f6ee946bead7989dc29ce485986409c89d448be83d5e082ac1d0"}]

## Record compatibility

```sh
python -m pytest tests/test_record_compatibility_ownership.py -k host-admission-census -q --basetemp="$TMPDIR/pytest"
```

**3 passed, 45 deselected on each arm.** The census record belongs to the ownership
protocol group despite its host-prefixed name. An initial selection against the
host file selected no cases; it was not a pass. No golden or field-set changes
were needed for this repair.

Initial empty selection (65 deselected, exit 5):
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"52c1459b31d1df90b06f3ebf21b588d7865ff458f1f63c5c46612c9c925abe79","size":919}},"operation":"8e25594da70f1420fabf072b675b87602939be4be323e3309ca9714ea024f091","receipt":"2af7bbad60bcf923da151a8a0634fee400572e1eb89ea0e5d354a4a962da8f05"}]

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"8d1cdb107fcad0a81980931a3beca0a84d77a43b84921ed678c643097ea91be9","size":103028}},"operation":"9e2ea68fdfc927956ad7655197c7547f2e2f5ec295f50238f29e5e21944be21a","receipt":"1794e1a6c5e06000d80fb3f161e96c4300bf40a8b65496332190e052af382375"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"5b52025ce094a073fd4e72bb43326c4264743370a89b7660406ce9ec3901f1c6","size":101967}},"operation":"87f40bfd65885054efc914c34eaf10b9b98403828eaf05a90ecf55daa2282cc8","receipt":"1d786280de8ffc5f9afa6a71f41fecd81091fdddd475dcc1f4b58685748d4b15"}]

The broader gate is **not green**. This bounded recheck stopped at its first
failure on each arm, before the remaining cases executed:

```sh
python -m pytest tests/test_record_compatibility_host.py tests/test_record_compatibility_ownership.py tests/test_record_compatibility_queue.py -x -q --tb=short --basetemp="$TMPDIR/pytest"
```

Both failed at `scope-telemetry/complete` during archival writer startup, not at
the changed census boundary. x86 reported `archival replay runtime does not match
its explicit qualification`: the selected environment pins the captured
`TMPDIR=/home/rob/pbt17`, unlike the runner's per-action TMPDIR (existing #2178).
ARM reported `bwrap: execvp .../python3.14: No such file or directory` for the
selected archival interpreter. Neither outcome is compatibility acceptance or
proof of a census regression. No environment, capture, golden, historical source,
or runtime qualification was changed to bypass these failures.

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"aa305e9df072d8434e0e429a698a079ecee3be9452c5f8115738991d09b07b44","size":134433}},"operation":"751a7d48b844fc22fc5a62b327f2d62283c85b01372de54e7a2f59d93ab128f6","receipt":"33a8c710700e7db0f8b203efac571d1f822a799edc717fbb04cc86431fe87277"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"12982a12d4424a38d5bf3f22ce40898bce7c0acee212a52580c27975e21522d7","size":134417}},"operation":"9a19792b7887aca52ffbbd72253689effcfa8669e5bcda2dad345f0fcde64b92","receipt":"0d8f993697e966d3ac4e9717fcb4a56a80e9f7b256426b50cff5df585888bc34"}]
