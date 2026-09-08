# GPU test fanout and pytest population options — issue #387

The published `dfd1f1850092` client lacked GPU admission and pytest-option
forwarding. An admitted regression on dl380g10 failed in argparse before any
shard submission: `--gpu`, `--gpu-memory-gb`, and `--pytest-args` were unknown.
Action `fd70ff8b214f2feb0d1b203b56b37b938d24eb472e0cc575f9688b3ecc26f948`
has a failed ending (exit 1), attributable pytest failure log, and no CAS receipt.

The implementation adds per-shard GPU demand and a pool GPU budget, a closed
JSON argv vocabulary, and distinct surface report paths. It leaves file
partitioning, admission, and CPU affinity with PB. Supplying the explicit
pytest argv replaces config/environment addopts and refuses resource/config
overrides and population duplication. Existing calls without forwarding retain
their pytest configuration behavior.

Validation used the published `pbrun.py`, CPU-only on dl380g10, reserving four
CPUs and four GiB with OMP/MKL/OpenBLAS native threads bounded to one:

```text
/home/rob/venvs/pb-cpu/bin/python -m pytest -q
  tests/test_pbtest_gpu_options.py tests/test_pbtest.py
  tests/test_pbtest_workers.py tests/test_pbtest_reserves_its_threads.py
  tests/test_pbtest_reports_whether_pytest_ran.py
```

All **74 tests passed**, zero skips. Two actual pytest subprocesses ran
concurrently, each with two xdist workers, proving file populations and surface
reports stay separate and project `-n 99` plus environment `-n 88` cannot
override the two-worker reservation. The remaining tests cover submission
flags, defaults, rejection before submission, and existing verdict behavior.

- Action: `9b3ce8d59152eec0724fd4a4a0ffa4dc7e73ba084bf76005c37cd9544b7b589a`
- Receipt: `43aaa75df8f5655580ec1288647df13a152ec664ddd61bcb8fa7597de0c44fc2`
- Payload: `42c6c4e3c53b1d45228ab68ec605c5f5aec56e863c7a65f89c54a8f8137956f4`
- CAS receipt path: `/mnt/shared/prismabuild-fleet/cas/actions/v3/9b/9b3ce8d59152eec0724fd4a4a0ffa4dc7e73ba084bf76005c37cd9544b7b589a.json`

The ending, complete structured status, pytest log, and actual CAS payload
digest/size were checked. Peak scope memory was 253.7 MiB. This is CPU client
and subprocess validation, not CUDA kernel coverage or a throughput claim.
Publication and live GPU acceptance evidence will be recorded on issue #387.
