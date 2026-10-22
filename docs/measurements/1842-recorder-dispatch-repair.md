# #1842 recorder dispatch repair

## Scope

The registered recorder's lifecycle generators no longer establish phase
completion. A private monitor installed through pluggy's public API observes
real dispatch entry/return, the invocation's command, session identity, and
phase order. It owns population snapshots and final emission. Test-report
recording is also monitor-owned: consuming or abandoning a directly called
adapter cannot forge, duplicate, or suppress a real report. Synchronous
population/report adapters require the actual dispatched argument objects.
No caller-frame inspection, descriptor changes, new wire schemas, or binding
mode changes were introduced by this repair.

Legacy compatibility checks now load the unmodified historical reader from
`tests/fixtures/pbtest-reader-before-1842/`, not from Git at test runtime. Its
README records the original commit/blob and byte digest. Compatibility
assertions remain enabled. This does not create an in-process security boundary.

## Reproduction

- Before the repair, the new sessionstart probes produced **2 failed, 4 passed**
  in `tests/test_pbtest_record_phases_1842.py`. In particular, advancing the
  lifecycle generators and fabricating passing reports before real collection
  could produce a green verdict for a never-executed failing test.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"976011a1be36b15efce66c61bfbd0a10811cc584dd21509f1680b0fc5b2566b5","size":3345}},"operation":"acf34a025fe74233b35b8b6cfe801d29ea81229c3062de214f0cdfe9d983df18","receipt":"6fcf7c8bacf7e4891e06bd668fb104d8a9ac7885e54da2deb291667ed5679c55"}]

- An intermediate repair still allowed a tryfirst wrapper to claim a real
  failing report through a direct adapter call and abandon the generator.
  With a genuine teardown skip and forged final summary/exit, this also went
  green. The added abandonment regression failed before moving report
  recording wholly into the monitor; it passes in the final checks below.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"15c64fcb3209a695e07b55d2ab55033f913a16fbe1be61865d667f44857de52f","size":3038}},"operation":"3738f6dc23cf5ac39e5f33e8dbfbb650da53d0347ce3dd86652c740b14361f2f","receipt":"808672afb3e5eb181aad521ad1ba6330e19509cd02b0d84ee165cfebff75f831"}]

## Final verification

All test execution used `run_tests` inside the supplied admitted x86 Python
3.14 environment, two CPUs, with pytest temporary files under `$TMPDIR`.

The focused check covered all #1842 recorder cases, interrupted and blocked-exit
completion, native directory/ignored-file collection, reconciliation, skip
reasons, xdist, sealed-program size, and bootstrap binding:

```sh
python -m pytest -q --basetemp="$TMPDIR/pytest" tests/test_pbtest_record_phases_1842.py tests/test_pbtest_record_privacy_1842.py tests/test_pbtest_record_binding_1842.py tests/test_pbtest_authoritative_records_1842.py tests/test_pbtest_exit_completion_1530.py tests/test_pbtest_reconciles_outcomes.py tests/test_pbtest_reports_whether_pytest_ran.py tests/test_pbtest_records_skip_reasons.py tests/test_pbtest_native_directory_collection_1458.py tests/test_pbtest_honours_collect_ignore_1304.py tests/test_pbtest_xdist_shared_skip_1220.py tests/test_pbtest_program_fits_one_argument.py tests/test_bootstrap_record_binding_1842.py
```

**144 passed in 59.89 s**, no skips.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"d007ee9f841c85fb9af0758acc04d8ca6a900e7ee8d1b230ba2347fee0600477","size":3773}},"operation":"b1167e650c77c4bf099bbea3918601356481fe9480ae6163fac9783d59322fe8","receipt":"dfcac2b6131af592a7ee878d99a7a31de446484f192b964f6abb5c31b8cd477b"}]

Broader final check:

```sh
python -m pytest -q --basetemp="$TMPDIR/pytest" tests/test_pbtest*.py tests/test_bootstrap_record_binding_1842.py tests/test_bootstrap_qualification.py tests/test_pbmergeq_runtime.py tests/test_wire_inventory.py
```

**673 passed in 329.34 s**, no skips. There were 34 existing Python
multithreaded-fork deprecation warnings from `_bounded_reader.py`.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"716cc479cff610cb5727b0d29544596a98b97a4ac07bb844af2c0444680e9759","size":17351}},"operation":"442d9b46dcd978fedef0b5060333a316dceda4652a13ac58ec66293d69735b06","receipt":"00891b928bdce4ddb35e91d3ce63713e22c3697e10383f6ebf72ca2e6b8d126b"}]

`git diff --check` also passed.

## Other observed failure / limits

An earlier broader invocation included the unchanged
`tests/test_digest_primitives_1547.py` as well as the final broader selection
above. It returned **679 passed, 10 failed**. All ten failures were historical
`git show` exit 128 errors against commit
`4815485c5060f3cecbf42655a37c7585e14c1791`, before their compatibility assertions
could run (including historical core, dependency, pins, and trace source).
Those older digest fixtures are outside this repair; they were not skipped,
weakened, or changed. This invocation predates the final report-abandonment
repair and is not final passing evidence.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"2502cd777ce2b3774a991ce50bb4d1a50389bff9536cd63e97b066ee0f2438af","size":73102}},"operation":"f4bb6fc5b55348c1556f047833fe110c6b4abb0f19b7e0e45283d7e55caba080","receipt":"45f46e01a6299c7d1bb88795cc07ab4d7a87e61c9d03539d8e9e36a04a913f60"}]

No full-repository suite, ARM lane, live gate, runtime publication, rolling
worker deployment, or deliberate closure/pluggy/descriptor tampering was
exercised in this repair.
