# CPU oversubscription and cross-resource placement validation — 2026-09-06

> **Historical record. It does not describe `main`.** This measures a source
> state that was never committed: the implementation it validates lives
> uncommitted in the `/home/rob/prismabuild` working tree on branch
> `fix/cpu-placement-policy`, which is why no commit, `git log -S` or pull
> request carries it (RobTand/prismabuild#573). Two specific divergences from
> `main` as of 2026-09-17:
>
> - **The ordering below is not `main`'s.** This document says the new checks
>   precede reservation. On `main` the fallback deferral runs *after*
>   allocation, with reservation and probe rollback
>   (`src/prismabuild/pool.py:4755-4767`).
> - **The thermal-placement test named in the receipt table is not on `main`.**
>   Against `main` its two `heavy_opposite_resource` cases fail (2 failed, 10
>   passed); against the uncommitted tree they pass 12 of 12.
>
> The 226-test total and its eight receipts are preserved as measured. They
> qualify that snapshot and nothing else, and no part of this document is
> current-`main` qualification.

The integrated change passed 226 tests in eight admitted CPU actions on dl380g10,
with no skips or missing tools. GPU admission tests used simulated device evidence;
no GPU kernels or thermal/performance measurements were run.

Each action reserved four CPUs and 4 GiB, with two pytest workers and bounded native
threads, using the published pbtest fanout. Terminal status was executed and wrapper
exit status was zero for every shard. Stored CAS receipts matched reported receipts;
each actual output blob matched its receipt SHA-256.

Regression evidence before implementation:

- Oversubscription: two cases failed because local borrowing ignored free preferred
  cores on a compatible remote worker (action prefix `6a254049b6e1`).
- Cross-resource placement: both CPU-on-GPU-heavy and GPU-on-CPU-heavy cases failed
  because a loaded host immediately claimed work despite a cooler eligible peer
  (action prefix `37a510807651`; eight control cases passed).

The regression lesson is that local admission alone does not express fleet placement
preferences. The new checks precede reservation and GPU probe consumption.

Final command:

```bash
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py \
  --checkout /home/rob/prismabuild \
  --python /home/rob/venvs/pb-cpu/bin/python \
  --workers-per-shard 2 --mem-gb 4 \
  --json /tmp/pb-placement-final-tests.json \
  tests/test_pool_thermal_placement.py tests/test_box_capacity.py \
  tests/test_adaptive_cpu.py tests/test_adaptive_gpu.py \
  tests/test_pool_cpu_tiers.py tests/test_pool.py \
  tests/test_worker_loop_offers_what_is_free.py \
  tests/test_worker_loop_retires_every_kind.py
```

| Suite | Result | CAS receipt |
|---|---|---|
| tests/test_pool_thermal_placement.py | 12 passed in 1.45s | `/mnt/shared/prismabuild-fleet/cas/actions/v3/3b/3b6a852d93f7b862382b659ecbddbe58ff6da8838539a5d0fd0e6698c6919c56.json` |
| tests/test_box_capacity.py | 33 passed in 1.40s | `/mnt/shared/prismabuild-fleet/cas/actions/v3/15/1580cb1afdf9e3625c635d248b1e8d9a484ce55813b64170f2356de4ef03cdd0.json` |
| tests/test_adaptive_cpu.py | 34 passed in 2.05s | `/mnt/shared/prismabuild-fleet/cas/actions/v3/60/605225186601ffed91eecda5255b1582eddfd852be28524368c0a9e5e0c9faae.json` |
| tests/test_adaptive_gpu.py | 51 passed in 2.17s | `/mnt/shared/prismabuild-fleet/cas/actions/v3/18/18eee1f5d7d56fec68d303f92369f1cc68be9969ba29277798cab27ff69895a2.json` |
| tests/test_pool_cpu_tiers.py | 9 passed in 1.53s | `/mnt/shared/prismabuild-fleet/cas/actions/v3/1f/1f72d6f2a2993dbe36bad92ea6ec6282468fdf8c3a7cd300482348eb082bdc52.json` |
| tests/test_pool.py | 74 passed in 6.91s | `/mnt/shared/prismabuild-fleet/cas/actions/v3/ce/ceed755a24e081b0d4a423f27edb0080293c3e8aca5b38423473e63e851f1b54.json` |
| tests/test_worker_loop_offers_what_is_free.py | 8 passed in 1.47s | `/mnt/shared/prismabuild-fleet/cas/actions/v3/74/7409444a508dde09969db6ddae804a9eef12e86af4d6efd93c42840375166aaa.json` |
| tests/test_worker_loop_retires_every_kind.py | 5 passed in 1.34s | `/mnt/shared/prismabuild-fleet/cas/actions/v3/8f/8f2e090b866abcc49f38a219a1aac828677605793ffe5330f90b0d00bc9bf276.json` |

These results validate scheduling behavior, not reduced temperature or faster
throughput. Placement uses bounded advisory observations; whole-action resource
fit and placement constraints limit which remote capacity is eligible. Publication
has not occurred: the operating guide requires explicit approval and an idle queue.
The final inspection found unrelated admitted work still running.
