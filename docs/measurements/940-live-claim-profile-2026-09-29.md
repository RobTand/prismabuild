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
