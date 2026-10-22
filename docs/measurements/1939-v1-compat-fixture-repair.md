# #1939: self-contained v1 bell compatibility baseline

## Scope and provenance

The repair changes only the compatibility test's baseline acquisition. All seven
wire cases, four client/service pairings (28 cases), status checks, client checks
and exact raw-body comparisons remain unchanged. Production code is untouched.

`tests/fixtures/queue-bell-v1-8b2d47ff.tar.gz` contains the four unmodified
`src/prismabuild/` modules `queue_bell.py`, `queue_bell_events.py`,
`queue_bell_http.py` and `queue_notifier.py` from
`8b2d47ff824e89aa60f83126d397bbd4207fe3d2`. These exact Git blobs were available
in the implementation checkout and captured without rewriting their bytes.
The adjacent `.tar.json` records the commit, tree, Git blob identities, byte
lengths, per-module SHA-256 hashes and archive SHA-256. The archive follows the
existing archived-source fixture pattern, keeping historical Python out of the
wire inventory without changing its policy or baseline.

The fixture loader verifies the revision, exact four-file closure, archive hash,
regular-file membership, byte lengths and module hashes before importing. It
writes only explicitly named modules into pytest's temporary package. It needs
neither Git nor historical objects at test runtime and has no current-source
fallback or skip path. The archive is a test oracle, not a published generation.

## Verification

All pytest commands below ran through `run_tests` in the supplied admitted x86
Python 3.14 environment, with two CPUs and native thread limits of one.

### Reproduced before repair

```sh
python -m pytest tests/test_queue_bell_v1_compat.py -q --basetemp="$TMPDIR/pytest"
```

Exit 1: **28 setup errors**. Retained stdout confirms the historical `git show`
exited 128: the requested baseline path was absent from the runner's checkout
history. No compatibility case reached its assertions.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"c8c2c38a9b8068b682c3b19fe62848089a9e919e973c62b877790e0e3742bded","size":152482}},"operation":"7fe4039dfc6800879cedf1696ca2cd0398e64a88fa644508381688f61e7995ec","receipt":"ef327680f22ab7aa50e74b2a5506778086e640845a80a0b9a5f940b44034d4ff"}]

### After repair: unchanged compatibility matrix

The same command exited 0: **28 passed**, no skips, in 1.43 seconds.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"a7de73d9bdf45c7312d7a20d0db575c96880e554d4bd5c07c1e9a065fec0c176","size":1129}},"operation":"19cad036fbd55d1a89d175c916d44a8f5e08954cba45a7010410f0fce4209174","receipt":"c8b8b9693f28bd456e61a82011c47963fb9f0cd25710563dc6d53ee41f87f4ef"}]

### Bell regressions and inventory guards

```sh
python -m pytest tests/test_queue_bell.py tests/test_queue_bell_v2.py tests/test_queue_bell_installed_coverage.py tests/test_queue_bell_lazy_coverage.py tests/test_queue_bell_startup_coverage.py tests/test_queue_bell_v1_compat.py tests/test_action_wait_bell_deadlines.py tests/test_wire_inventory.py tests/test_duplication_baseline.py -q --basetemp="$TMPDIR/pytest"
```

Exit 0: **209 passed**, no skips, in 81.93 seconds. Two multithreaded-fork
`DeprecationWarning`s came from `_bounded_reader.py` in
`test_real_requeue_supersession_wakes_exact_publication_waiter`.
Retained stdout was read back and checked.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"e4a6cc2afcb0751ce208c3dd118eee6b63e987cbfbc1c45d86d83ca7a4911733","size":3353}},"operation":"adc664c8df56f7cd608ed77dd5e47e1383423dd669f89d9318d1a4a77dc3c8f9","receipt":"e2987501dad18086f2f3176df6ca98c54eb8b8dc7ceeef4c58587ca9be39614e"}]

## Limits

The tool rejected an initial explicit `arm="x86"` selection without running it;
the completed commands used the default arm, whose runtime evidence confirms
x86/Python 3.14. No full repository gate, ARM check, remote-NFS v2 integration or
consumer latency/outage acceptance was run. No runtime was published or consumer
enabled; this repair supplies portable compatibility evidence only.
