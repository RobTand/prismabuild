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

## Follow-up: remember the first shared observation

The next review found that the shared-result reconciliation above only protected
selections already stored by an owned census. A pass learning its first election
through shared reuse did not store it. The repair now stores the accepted,
post-merge shared census before yielding for candidate evaluation. This is a
memory-only operation under the existing M/H locks; it adds no I/O or lock
acquisition. Candidate-relative ordinary negative proofs still do not store.
The source-generation reader, retirement rules, and M-before-H ordering are
unchanged.

The added end-to-end regression starts an unprimed real claim pass over two
lower-priority ordinary candidates. Between candidates it removes the election
sidecar without retiring the still-READY measurement. It covers both stale-cache
fallback and a sibling publishing a new census without that selection. Both
candidates must remain deferred, with no holdings. It also verifies that the
first candidate actually used shared reuse, rather than an owned read. A third
case inserts a higher-priority ordinary negative proof between lower-priority
checks and verifies that this candidate-relative empty result does not erase the
remembered election. Cache-age clocks are injected; no latency bound is asserted.

Before the source fix, the two-candidate cases failed by admitting the second
candidate (x86, 2 failed). Command:

```sh
python -m pytest tests/test_shared_census_safety_2199.py -k first_seen -q --basetemp="$TMPDIR/pytest"
```

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"bdc514b6ac47ca53e02de415aeb099cbe822a9fdeffe2ee9ac70cc0af2d8f2f5","size":8677}},"operation":"736ad917365458b29fde7a914636325b92e0ec9fd627b2f5fa862d2a72a484ba","receipt":"7017615601b5f3740cbc80d42dffcdc6e054d41f3aee78738a581e990bd058f1"}]

An initial broad run after the repair returned 390 passed / 1 failed: the
completed-gang shared-reuse test expected no refresh. The denied probe writes a
pass counter, invalidating the shared source generation. Remembering the shared
census now correctly selects the narrow owned refresh for its follower, instead
of a fresh full capture. The fixture now injects cache age and observes both
capture and refresh: shared-first performs no complete capture; owned-first
performs two; both narrowly refresh once, observe no live gang fence, and admit
the follower. Its retirement and acquisition assertions remain intact. No
tautological tests were found or removed.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"b6eff9b446c4a7b24efee62ed1a1f3443afc9a1b639b53564ad2511533bf29a8","size":10248}},"operation":"460bbdc4e21c6adcc40b06e5150ee3c51c4158b3329d3b2a17e1244de423bcb2","receipt":"6f2d4b963e009370898ce523b34bde670502b050b2a2a685a3a56baaa8a1fd5e"}]

### Final verification of this follow-up

All checks used plain commands inside the supplied admitted actions. The final
focused suite, including all four required guards, passed **391 tests on each
arm** (x86 Python 3.14 and ARM Python 3.12):

```sh
python -m pytest tests/test_shared_census_safety_2199.py tests/test_ordinary_census_admission_2199.py tests/test_bounded_reader_ownership.py tests/test_measurement_reservation_census_1419.py tests/test_census_lock_set_1498.py tests/test_census_reuse_reader_custody_1571.py tests/test_census_tmpfs_state_1451.py tests/test_census_admission_nfs_slow_1498.py tests/test_census_refusal_is_once_per_pass_1571.py tests/test_census_reuse_transitions_1571.py tests/test_census_reused_once_per_pass_1571.py tests/test_measurement_census_reader_fence_1498.py tests/test_claim_arbitration_histories.py tests/test_claim_completion_histories.py tests/test_pool.py tests/test_pool_unreadable_ready_record.py tests/test_kept_read_clock.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"17a8ec9d56ddf3656f72a5d6357179e9ec8581e7d10fb0fc013218260171dd7a","size":6176}},"operation":"7eee80a705ad373fa384a2db89db1d19bb35f09bb53aa502114a04f84ff3713b","receipt":"f89b85b5e0af87b54ae79d02cc0988edf2a8717230c9ae69c66639868151718d"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"f0d40c7f10033f032daf00839c60d40e24c394a05a7b1efdf0143f7960113be3","size":6188}},"operation":"0f2737db52530503ad0557af5da60b0aeab3627df2f3f20e38833dff91d05c6a","receipt":"304c5d805abc43027583bd019be78e3faa989cee65648cda60515bb7d4cb716a"}]

The two changed behavioral test files also passed with two parallel workers,
**16 tests on each arm**:

```sh
python -m pytest tests/test_shared_census_safety_2199.py tests/test_census_reuse_transitions_1571.py -n 2 -q --basetemp="$TMPDIR/pytest"
```

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"ce34069be8c1c395e82e4207542e1095be6c794fcddb46d09590ccabe16c6018","size":1965}},"operation":"d276741474c34d519d83e45e4e3bf69bb988462ef08e3aa9445471972c1addae","receipt":"34d8675acd18c4951313e9c1998f9184f3244f1e30a0fc3830273cbe529f4e95"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"72115c2dd09b910f06f2757e0ba5ba23e000be00285e20a73caac86a6139cd90","size":1975}},"operation":"81329096342e51c021e102bc6ea3cae0134d692fa40a30fafe0a2fd0a4bb0e10","receipt":"be0775194974722be097d47cd34dc92674ac77715024b56c344ef24a5cb1e42c"}]

The shared-census record compatibility family belongs to the **ownership** lane
because its reader is `_measurement_reservation`. An initial selector against
the host lane returned exit 5 (65 deselected), not a pass:

```sh
python -m pytest tests/test_record_compatibility_host.py -k host-admission-census -q --basetemp="$TMPDIR/pytest"
```

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"ea6e281a0ba060cb347fdd2429869fd1ac6e60be01fb2dbe0a9b3f494fd4b857","size":921}},"operation":"e424737a90d55c90066cb6c1b33eb40944db2be8f4fae1dc9d5a7fdf24ec6a67","receipt":"a32defd6c0401d63940aef5a00ee5637d453dec8ca6606b5b3236598f836555d"}]

The corrected selector passed **3 tests on each arm** (45 deselected):

```sh
python -m pytest tests/test_record_compatibility_ownership.py -k host-admission-census -q --basetemp="$TMPDIR/pytest"
```

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"1a1595d174f24deaab94273aa0d8ba9b013bbc9e43247c33144456923a97a4e5","size":103043}},"operation":"ff2060ed33bfb763bd54e2df3ddcd9da3d26c41f3def98faea4a5e1606d07894","receipt":"7c0b4d54313e498d4aed69af2c0d93692887cbc4baebd436190ecc7ade2a778b"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"8272eccd9a015897e73ed7c1e1eede7466be1980a107ded8ee759308d4a5da68","size":101962}},"operation":"9f4e50d94a95c414bae1e40a5decca3e09778a9791b652ba2f60687fd574a90a","receipt":"e791d40ce6cf0fe439b6fb16117050b6487d1b036093c0bf1038cbf97b32b2bb"}]

The existing bounded-reader fork deprecation warnings remain. Full archival
host/ownership/queue qualification was not repeated in this follow-up; the
previously recorded runtime/TMPDIR failures remain unresolved, not passed or
attributed to this repair. No record format, field-set inventory, golden, or
qualification was changed. No live-host probing, deployment, or portable 44 ms
latency claim is made.
