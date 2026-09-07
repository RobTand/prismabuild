# Offer freshness after a stalled shared read

Issue: [#16](https://github.com/RobTand/prismabuild/issues/16). This is a
partial correctness fix, not closure of the shared-storage liveness issue.

## Finding and change

At base `c0d99fe5ff37d68febdf59f59adb5efd9f373586`,
`src/prismabuild/pool.py:1760` sampled time before worker-directory enumeration
and offer reads. An offer fresh at that instant remained accepted even when
the scan completed after its expiration. A slow later file also extended the
apparent lifetime of offers read earlier. `PoolQueue.offers` now collects the
records, then evaluates all their timestamps using the scan's completion time.

`tools/fleet/pbstatus.py:545` had the same starting-time problem, spanning not
only worker/ready/claimed directories but admission, lease and denial sidecars.
Its census now reads those inputs first, retains each sidecar's read error for
its owning row, and then derives freshness, ages and placement. Unavailable
sidecars keep their evidence unknown. The screen remains a non-atomic census.

## Regression evidence

Every run used the published PrismaBuild client, portable `--anywhere` CPU
placement and the sealed checkout. No live queue fixture or induced NFS outage
was used. Tests advanced a logical clock during reads of private local fixtures;
the ordinary JSON/directory readers still supplied the fixture data.

Common submission flags:

```text
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbrun.py
  --cwd /home/rob/tmp/pb16-offer-freshness --anywhere
  --cpus N --demand mem_gb=M --env OMP_NUM_THREADS=1
  --env MKL_NUM_THREADS=1 --env OPENBLAS_NUM_THREADS=1
  --timeout-s T --detach -- /home/rob/venvs/pb-cpu/bin/python -m pytest ...
```

| Run | N / M / T | Pytest arguments | Outcome / worker |
|---|---|---|---|
| Pool red `dcafff1f30ecc93acc2cc774c11b5cdd9e8b86b3f5ed456085fcf762a5b816cf` | 1 / 2 / 120 | `-q tests/test_pool_offer_scan_freshness.py` | 3 assertion failures / Sparklina |
| Pool green `2cdb8e101bc03ebc353633ac3f25478d085779e3c69b0dfef03ed55b947f8c88` | 4 / 4 / 300 | `-q -n 4 tests/test_pool_offer_scan_freshness.py tests/test_pool_offers_stale_handle.py tests/test_pool_enumeration_stale_handle.py tests/test_pool.py` | 87 passed / Sparklina |
| Status red `6e2da2f60a0ea506d8fec507e39412427e162a601cb5882bfe902965ed750410` | 4 / 4 / 120 | `-q -n 4 tests/test_pbstatus_pool.py -k completed_census` | 6 assertion failures / Sparklina |
| Status final `36177c8d4754cc07158315853da771fea35912e23c06ccb639e9605c90498fb4` | 4 / 4 / 300 | `-q -n 4 tests/test_pbstatus_pool.py tests/test_pbstatus.py` | 51 passed / Sparklina |

The first pool fixture attempt (`4c68d391d0c9679ed6ab50cac8609c6d0c1eddaeee97dd8d5ea12cb6b9aa8df8`)
failed because its wrapper omitted the reader's `tolerate_stale` keyword; it is
superseded and is not regression evidence. The corrected red above failed on
the expired offer assertion in all three cases: enumeration, first read and
last read. The status red failed at each of its six read positions. Before
adding three sidecar-error checks, status also passed 48 tests on dl380g10
(`d07736b59d1c7d2c57625fa17f8f2dedd99c9a3d1fa805e8fee50b3ca0406444`).

All final tests ran on CPU with GPU visibility disabled and PB-assigned
affinity; no tests skipped. Imports exercised the changed modules. Terminal
records report `status=executed` and return code 0 for both final green runs.
Their receipts and result bytes were read back from the server-local CAS path,
and result lengths and SHA-256 values matched:

| Run | Result bytes | Result SHA-256 |
|---|---:|---|
| Pool green | 467 | `0b4c891bfeee0c22e6f114974f1e6075e841bc51e4e9a9c0b30fc95e41e2b0a3` |
| Status final | 387 | `ba9820dcedfb7d282d2778fe4a89c09dd15b7e070ae5603233f2434d11dffb54` |

Terminal logs: `/mnt/shared/prismabuild-fleet/pb-queue/{failed,done}/<key>.json`.
Receipts: `/mnt/shared/prismabuild-fleet/cas/actions/v3/<key[:2]>/<key>.json`.
Results: `/mnt/shared/prismabuild-fleet/cas/blobs/<digest[:2]>/<digest>`.
Local readbacks: `/home/rob/tmp/astra-review-20260906/pb16-*.json` and
`pb16-status-final-result.txt`.

## Remaining boundary

Queue filesystem calls remain synchronous. In particular, `offers`,
`ready_items`, and `pbstatus._pool_records` have no caller I/O deadline. The
existing subprocess drain/termination helpers govern action execution, not
these queue reads. A thread wait timeout does not establish that a blocked
filesystem syscall exited or that its owning process can be reaped.

No worker was killed, reservation released, mount changed, service restarted,
or runtime published for this qualification. It establishes expiry semantics
under simulated elapsed read time, not cancellation of a real stalled NFS
operation. A bounded reader/supervision design and non-destructive cross-host
stall qualification remain required for #16. This work makes no throughput,
GPU saturation, or NFS root-cause claim.
