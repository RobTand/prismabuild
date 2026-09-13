# Reviewed test dependency pins — 2026-09-13 (#551)

The old `pbtest` dispatcher reached pytest and reported green when an installed
dependency named a different Git commit from the checkout resolver. The new
guard refuses before pytest, and a matching install proceeds. Verification is
inside each admitted worker action; no shared project environment was modified.

## Reproduction and live refusal

- PB action `39dac4c7a375816548212c3acbd5b4814a2a3f117fd670e1ab17ad355b016a62`
  read the live dl380g10 `pq-cpu312` environment: Tessera contract v22, 93,048
  bytes, owned by distribution `tessera-quant`. Its PEP 610 metadata names a
  local install and carries no Git commit. Receipt
  `3b009749a8c4e2886a7ad185580cee05226a4989c1ff921f3d904e4e5f95237d`
  binds payload `42e51c8bbf70d2e8b2464a2fd93ac8e7ad9195f2f5d9629aa2e3ca41e5406b42`.
- Guarded action `6896dd1d4e530a3c99cb2c9a0251a6a60e76299a8503d3e97aa240f111cf7733`
  used PrismaQuant main `381f8c76be` and the same interpreter. It failed before
  pytest, naming required commit `7dbbacbd0900f6b6f468690e2525cc018564382d`
  and installed commit `<unknown>`. This is expected refusal, not a passed
  PrismaQuant suite. The guard source was sealed in the action's `-c` argv.

A separate admitted action installed the actual reviewed Tessera Git commit in
an action-local temporary venv, without dependencies or changes to `pq-cpu312`.
The guard accepted the pip-produced Git provenance and verified all 81 hashed
installed files. This exercises the real installation format, not only a
fixture: action `345915cfc9cfcb1d1966c6967e40f9a825546b4aef9166a80120de825adc9605`,
receipt `9715a396d3a4756854779fb1093655ede1322ac806cbce6be73b7665bdf80fdf`,
payload `d2f2ca0706ca6b308ef624799091ee6af93c8537d83d7c61c0a40f15b0ab157a`.
This is a dependency-installation check, not a full PrismaQuant test run.

## Regression tests

All commands used the published `pbtest.py`, CPU-only, priority -10. The red
regression reserved 1 CPU / 2 GiB. Validation used eight file shards, each
reserving 2 CPUs / 3 GiB and running two pytest workers with one native thread.
PB placed them on dl380g10 and DESKTOP-P5UOGNJ. Total: 108 passed, no skips or
missing collection. The final 14-test pin shard was repeated after adding
assertions on the retained provenance JSON; this is not 14 additional tests.

The tests exercise actual shard commands inside an admitted fixture action,
including mismatched/missing provenance, editable/local installs, altered and
unrecorded files, import shadows, resolver failures, multiple pins and the
matching install. Existing pbtest argument/resource/summary tests and runtime
publication tests passed. The coordinator never runs a checkout's resolver.

| Run | Result | Action key | Payload SHA-256 |
|---|---|---|---|
| red.json / 0 | 1 failed in 4.27s | `53ad2a5385ad3ff75637a77b51fe23acf1a1b60fc2083ed5b15718ddc6adddf2` | `no success payload` |
| green.json / 0 | 14 passed in 13.67s | `c7508473b3cded12f4da269af30d32fa98f05b345f1c0923e295961e73f8bb95` | `cedb4fdd48b01d3b4300b0f1e6094dd227f57c1e580f2f73e546a1177e892d67` |
| green.json / 1 | 7 passed in 49.17s | `e8bd3b68e0d53f76ad4878686cfb637402d2c621e0dc1864dc9842c030bb560c` | `d40ec38d83b5a0883dfc6b70ac545385e99c5f913ba434c7a68b60f5cfb3bc14` |
| green.json / 2 | 32 passed in 13.14s | `f5a7428a16c243c063c72bfab3e562810d741c32fdfc5061879e14822ea4ffd4` | `0d62be00c243b44ed8b0413141ada8924fb66797084493b1fba51873b8df4188` |
| green.json / 3 | 9 passed in 11.11s | `3d4859260d282952d72d9f2f80bb42a8a90ea966dbc11a5d49165bf7d6976d9f` | `1800798190285731d2ab31c64fb0f42c4062836bce05c0c0241d2054ba564b39` |
| green.json / 4 | 7 passed in 11.23s | `9aa0f84f386989043cb7652a4a0e04c99a2bb80ce8a203236fd727e4f8ede105` | `e14e33569932bde9e3a29812684744e7df8ffebecdfb9e5ffc63d636c2c6714e` |
| green.json / 5 | 3 passed in 11.09s | `bf86d7a3785ae759d551ae885890b7a07cb117c6f296b25058839f3195db3d92` | `85f6fef202d7230552f15f6f39e52d0df41384e2781e0c9b46450a581737ed2d` |
| green.json / 6 | 19 passed in 11.24s | `9335c8930cc19d9e4d2c0c49d4fcad2a28e151d2bcfa811470a3e18312e9ce72` | `b65371fce189bb9c920e12405817e4ec780c96606c0a4b35ed2e9c6fec3c1956` |
| green.json / 7 | 17 passed in 14.46s | `2b7a6e3871139334f4530570e2106c87116dd12857b75efa950bacea548bdca7` | `2329c1293a9db59a006959922d537553c899968bb761bf7307d399ff93db9a83` |
| pins-final.json / 0 | 14 passed in 10.46s | `bc51801be1c6c6592a54a754f2573a8995745e2b074c82d8b0c40a49b7701b12` | `9b050535086e326087964b871a1a7873dc2229df1ae9278e8a0169ff3733436f` |

Terminal records and CAS payload hashes/lengths were independently read back;
pytest summaries were checked in the payload bytes. The red test's recorded
failure is the expected `0 == 1`: the old dispatcher called a mismatched
fixture green. Completed profiles report approximately 0.34–0.74 GiB peak
scope memory per green shard; the 3 GiB reservation was not exhausted. The
fleet had unrelated GPU work and no GPU work was created for this check.

## Scope and deployment

This verifies the `pbtest` convention and does not make arbitrary `pbrun`
commands infer Python dependencies. Managed Git installation metadata and
stable environments remain prerequisites. A local-directory install is
refused even if its directory name contains the required commit. Old requests
and cached results retain their original meanings; guarded commands use new
identities. Runtime activation and host adoption must be recorded separately
from these source validations; #551 stays open until deployed adoption.
