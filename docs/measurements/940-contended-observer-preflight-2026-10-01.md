# Contended observer preflight — 2026-10-01

This record covers the observer's CPU unit controls, not a live worker profile.
The coordinator authorized ordinary PB-admitted observation of an existing
contended worker for #940. Near-idle measurement isolation would remove the
contention under investigation.

## Implementation boundary

`tools/maintenance/diag940_contended_observer.py` binds a published worker-loop
PID to its process start ticks, generation path, command digest, executable and
pool source digest. Before starting py-spy, it requires the named action to have
a claim on the observer's host. A retired, foreign-host or host-unbound claim is
refused. Post-capture claim reads are observations: a claim retiring during
capture does not discard the profile or imply an atomic admission census.

The capture, Netdata and CAS main path has not executed in this record. The
unit result qualifies the preflight and coverage controls, not that main path.

The ordinary observer reserves its own CPU and memory through PB. It preserves
assigned affinity and TMPDIR, does not manufacture holders, and does not mutate
workers, reservations or drains. Its output includes sampled-stack coverage,
matched-interval Netdata charts, target identity and artifact digests. Sampling
occupancy is not call latency. Missing eligible-holder samples are not zero-cost
evidence. No cache change or before/after speedup is claimed.

## RED

Action `cd8982115ffcdfac1ec44b1c5207e484166d9c085d0d0f6496dcd1b9a6a582f7`
finished failed/exit 1 on dl380g10 in one attempt. Six cases were collected and
ran: **3 failed, 3 passed, 0 skipped**, with no missing or uncollected cases.

The three failures were the retired, foreign-host and host-unbound preflight
cases in `tests/test_diag940_contended_observer.py`:

```text
Failed: DID NOT RAISE ValueError
```

The original preflight returned an observation without requiring an active,
same-host claim. Positive controls covered a matching claim, key mismatch
refusal and a nonqualifying post-capture read. The behavior fix follows this RED.

Immutable worker stdout: 5605 bytes,
`dc143183ae152534a399e7029d7fc035889eb37559d468cf2a796a5047aa5bf0`.
A failed action has no successful CAS receipt.

## GREEN

Action `7c58f6907e12524c3ec1aa0163e0cc05ef15b987abc9767444dc3801611619fc`
finished done/executed/exit 0 on dl380g10 in one attempt. The observer controls
and existing profile-coverage controls collected and ran **17 passed, 0 failed,
0 skipped**, with no extra phases, missing or uncollected cases. Each action
reserved CPU 2 and memory 2 GiB, with one shard, two worksteal workers, native
threads 1, priority -10 and an explicit 600-second execution deadline. These
were CPU-only rows with CUDA visibility disabled, not GPU validation.

- Canonical receipt digest:
  `ce25d102bc7f1f0e84b8d50aff0397f3f32aa1c9c6142f19a9d7c057348fa333`.
- Result payload: 4497 bytes,
  `293fe33fe7a19f844dcd4d11e58e94ca3e574297d1cf94484f864c3cd955aaea`.
- Immutable worker stdout: 6979 bytes,
  `5859eea1423a41bd939d7e18c1068f12e18ad7b387a59cdf66625ce64d030c1c`.
- Local result claim:
  `63d0a391ac0eb992af738f35fff276089d2d4ef0cc6a1e2f3b82706bfa386209`.

Canonical terminal records, final pytest outcomes and direct payload/stdout
hashes agree. `pb_verify_claim` passed all nine integrity checks. Full worker
attestation was not independently audited. The dated documentation was added
after the code controls; this is not a whole-tree or deployed-runtime claim.

## Live observation remains pending

The original live observer action
`2dd76a3a22c764157ae741c901a597a74d4f6c491f192c3f82950d53b5d1de3f`
was withdrawn before admission when its named holder retired. No profile or
performance result was produced. At the later complete fleet snapshot
(Unix 1790839733.449889), both Sparks were deliberately draining for
`TP2 window u4-TSTAR-20261001T0536Z (staged: boundary-aligned)` while dl380g10
remained live. This does not authorize clearing another owner's drain or
reusing a stale holder. #940 remains open for a newly admitted observation with
a fresh, same-host holder and target identity.
