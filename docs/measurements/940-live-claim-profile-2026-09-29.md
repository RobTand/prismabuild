# Live claim-path observations: 2026-09-29

Refs #940. These are before-profiles of a deployed worker. They do not justify
an immutable-request cache or establish its benefit. No worker was signalled,
stopped or restarted; no runtime code changed.

## Method

The coordinator authorized read-only py-spy sampling of live PB worker PID
1367870 on dl380g10. Capture used `--idle --threads --subprocesses --rate 50`
with each interval bounded to 600 s. The worker ran generation
`3aff9642ab39-1790654284-1eed70850170`. Netdata series were captured for the
same intervals, and capture hashes were checked. Sampling includes idle stacks;
inclusive sampled time is not exclusive CPU time or an end-to-end benchmark.

## Observations

| Interval (Unix seconds) | Duration | Samples | Claim-path coverage |
|---|---:|---:|---|
| 1790714923.7635758–1790715104.7501554 | 180.99 s | 13,514 total; 9,019 root | 54 `_claim_pass`, 41 `held`, no sampled `holder_bound` |
| 1790717463.2099535–1790717762.7493875 | 299.54 s | 29,998 total | No `_claim_pass` or `holder_bound`; not claim-path evidence |
| Third completed claim capture | 599.296 s | 29,999 root | 238 `_claim_pass` samples, 4.76 s inclusive; 0.94 s held census inside claim pass |

In the third capture, membership reconciliation occupied 67.34 s inclusive
across the root process. No `holder_bound` or `_withhold_verdict` frame was
sampled. Matched Netdata includes CPU, disk and NFS-server series. The first
and second captures recorded no Netdata fetch errors.

## Decision and remaining acceptance

Do not add speculative caching based on these profiles. The absence of a
sampled frame does not prove zero request-read cost. These intervals exercise
claim passes, but do not establish the specific several-holder bound-lookup
cost in #940.

A follow-up must observe the target path under real eligible contention and
retain its matched Netdata interval. A hermetic replay can complement it, not
replace the live before-evidence. Cache immutable CAS requests per key only if
that evidence identifies their reads as material; mutable claim records must
remain fresh. Any implementation needs a matching after-profile and reported
delta. None is claimed here, and #940 remains open.

Capture artifact directories under the campaign's `tmp/p2p3/prismabuild/`:
`940-before-20260929T2048`, `940-coverage-ts-20260929T2130`, and
`940-claim-ts-20260929T2214`. They contain the original profiles and matched
Netdata, not manufactured timing receipts.

## Follow-up: 2026-09-30

A fourth authorized observation sampled the same live worker and generation
at 1790737282.2061265–1790737585.0962844 (302.890 s). Sampling returned 0;
all recorded capture hashes match and matched Netdata has no fetch errors.
The root process has 14,999 samples, including 116 `_claim_pass` samples
(2.32 s inclusive). No `holder_bound`, `_withhold_verdict` or
`_declared_run_bound` frame was sampled. This is another claim-pass observation,
not the missing eligible-holder read-cost measurement or an after-profile.

A preceding fleet census at 1790737214.4822521 reported seven dl380g10 ledger
entries, including metadata records. That count is not proof that seven real
holders blocked this worker's eligible ready item during the profile. Do not
turn a ledger count into evidence that the unobserved function executed.

Artifact: `940-holder-before-ts-20260930T030120`, beneath the same campaign
artifact directory. Speedscope SHA-256:
`20afa5fb8740f96bb27dd8d691a3ec00506d8b450f8f3b994ccd38c0ce3b5356`.

`tools/maintenance/diag940_profile_coverage.py` now provides one reproducible
coverage calculation. It selects only the requested PID's threads and the
exact published generation's pool module, counts duplicate/recursive frames
once per sample, and refuses invalid indices, weights, units and missing
worker samples. A claim-pass sample alone does not establish holder-read cost;
same-named functions in another tree or a child process do not count. Its
report always distinguishes sampling absence from zero cost and leaves the
performance delta unknown. It neither attaches to a worker nor changes caches.

Run analysis in an admitted action with the existing profile, explicit PID and
published generation root:

```sh
python tools/maintenance/diag940_profile_coverage.py \
  --profile CAPTURE/worker.speedscope.json --pid 1367870 \
  --generation-root /mnt/shared/prismabuild-fleet/runtime-generations/3aff9642ab39-1790654284-1eed70850170
```

The decision above is unchanged: #940 remains open, and no speculative
optimization or speedup is claimed.

## Numeric refusal repair: 2026-09-30

Reviewing the coverage tool uncovered two malformed-input paths. A JSON integer
outside Python float's representable range escaped the weight check as
`OverflowError: int too large to convert to float`. Individually finite weights
could also accumulate to an infinite `inclusive_seconds`, within one thread or
across threads. The latter could produce a nonfinite report rather than a
named refusal.

The tool now translates conversion overflow into its existing weight refusal
and checks each accumulated total before storing it. Weights and inclusive
seconds must be representable as finite Python floats; there is no heuristic
weight limit. Normal coverage semantics, worker selection and the report's
unknown performance delta are unchanged. This is a diagnostic repair, not a
worker, cache, lifecycle or deployment change.

### CPU evidence

Both arms used the published `pbtest.py`, one shard, two worksteal workers,
native threads limited to one, 2 CPUs and 2 GiB memory, priority -10 and a 600 s
action timeout. They ran on sparklina with Python 3.12.3, GPU demand absent and
`CUDA_VISIBLE_DEVICES=''`. No GPU-surface or live-contention proof is claimed.

- RED action `9bacbe6eab2b681f0c49c3f3eaf7ad48764e37d04837acc3be7c67f8d77e1d01`:
  terminal failed, exit 1, one attempt; **4 failed / 7 passed / 0 skipped**.
  Both integer-sign cases failed with the `OverflowError` above. Both
  same/cross-thread sum cases failed with `Failed: DID NOT RAISE ValueError`.
  No successful CAS receipt exists for this failed action.
- GREEN action `154babb155f3233fe38593107f0d1b8481f8e42bf71df09bbc0160f5e1bfced2`:
  terminal executed, client exit 0, one attempt; **11 passed / 0 failed/skipped**.
  Both arms collected and ran all 11 cases; outcome reconciliation was clean,
  with no missing or uncollected files. The successful CAS receipt's canonical
  digest and its result blob's SHA-256 and byte count were checked. This is
  integrity verification, not an independent full producer-attestation audit.

The file selected in each arm was `tests/test_diag940_profile_coverage.py`:

```sh
export TMPDIR=/home/rob/tmp/claude-campaign-20260926/tmp
test ! -e "$TMPDIR/u4-release/WINDOW_ACTIVE" || exit 3
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
python3 /mnt/shared/prismabuild-fleet/repo/tools/pbtest.py \
  --checkout WORKTREE --python /home/rob/venvs/pb-cpu/bin/python \
  --tag gb10 --shards 1 --workers-per-shard 2 --threads-per-shard 1 \
  --mem-gb 2 --priority -10 --timeout-s 600 --wait-s 600 \
  --pytest-args '["--dist","worksteal","--durations","20"]' \
  --json RECORD tests/test_diag940_profile_coverage.py
```

Logs and reconciled JSON are retained under the campaign's
`tmp/p2p3/prismabuild/940-numeric-{red,green}-ts.*`. The GREEN receipt is
`/mnt/shared/prismabuild-fleet/cas/actions/v3/15/154babb155f3233fe38593107f0d1b8481f8e42bf71df09bbc0160f5e1bfced2.json`;
its canonical SHA-256 is
`29316dc6c4064291fa319de24e9e345b7aee4f1222a755088d0b5d871fa31658`.
The result blob is 2976 bytes, SHA-256
`97f65d6abb0673d3b0657a6130f3d2299629faf0efcf2bbcadcd79591349e7ab`.

#940 still needs the eligible several-holder path and, if optimization is
supported by it, a matched after-profile. This repair supplies neither.
