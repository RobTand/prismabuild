# Checkout-cache experiment: 2026-09-29

Refs #811. This is a measured experimental slice, not a shipped cache or a
production claim-pass speedup. Runtime behavior and admission remain unchanged.

## Scope and provenance

PB action `c39f72e0ae94fbce2b858c4cecc2637dc548aee82812efac22ad9cef217785be`
completed once on dl380g10 with exit 0. Submission used `--measurement`, the
dl380g10 tag, `--profile sample`, CPU 2, memory 2 GiB, priority -10 and timeout
1200 s. Its terminal admission evidence records an idle measured baseline:
`state=idle`, `exceeds=false`, 256 samples over 3050.474 s. Current busy CPUs
were 7.255051/80 against the measured baseline's upper band of 8.607741; this
was not a fixed 5% threshold.

The experiment ran generation `3aff9642ab39-1790654284-1eed70850170` and source
harness SHA-256 `3bbd009bc025cfdad138991d5d09ae0292f8043cad9164306be5f8255a6c5271`.
It read immutable request
`81db7b3b6321db7d7284ef952e83465f4b6a73774972941db39241ef17727caa`,
whose body SHA-256 was
`1e5d2e3db9dc15d2a42941b5c10281c37addaa65a17d1875008f56a64beae13f`.
The checkout's HEAD was `329a1c82ad596a656d92fb49e12e80e066b321a6`.

## Paired results

Four interleaved AB/BA pairs compared ordinary checkout materialization with
an invocation-owned, indexed-pack reuse experiment:

| Boundary | Median seconds |
|---|---:|
| Ordinary materialization | 5.7489 |
| Experimental warm reuse | 0.6742 |
| Cold cache publication | 4.8094 |
| Cold entry verification | 4.6109 |

The bundle was 32,899,482 bytes, including a 32,899,392-byte pack. Both arms
matched Git identity, HEAD, refs, index, status and all 2,763 tracked paths.
Checkouts used independent reflinks; parity fetch transferred no objects.
Pack-hash tampering, pack-verification corruption and index-verification
corruption all refused.

## What this does not establish

A warm experimental boundary is not end-to-end launch latency. Cold costs
remain, and no production cache policy, reader lifecycle, retention rule or
worker integration has landed. The harness reports phase costs and raw paired
timings, not an operational speedup. A production promotion still needs the
in-process before/after profile and matched Netdata interval, lifecycle and
corruption controls, integration tests, and measured real claim latency.

The original report uses schema `prismabuild.diag811.e1_checkout_cache.v1` and
markers `E1-REPORT-BEGIN` / `E1-REPORT-END` in the action log. Its
`timing_claim_eligible=true` is a harness screen, not an independent promotion
or admission gate. No completion of #811 is claimed by this document.

## Correction, 2026-09-30: equal timing boundaries

The original v1 harness included verification in arm A's headline but omitted
it from arm B's. The 5.7489 s / 0.6742 s table above is therefore not an
apples-to-apples materialize-and-verify comparison. Preserve those original
records; do not use their unequal headline boundaries to price a change.

Summing the original per-repetition phases, excluding cleanup in both arms,
gives these medians:

| Boundary | Median seconds |
|---|---:|
| A materialization plus verification | 5.7489 |
| B materialization plus verification | 1.3039 |

This is a derivation from the same action's recorded phases, not a new run.
The four B totals are 1.3362, 1.3484, 1.2716 and 1.1382 s. It establishes no
production speedup; the cold costs and promotion requirements above still
apply.

The retained reproducer is `tools/maintenance/diag_811_e1_checkout_cache.py`.
Its v2 report uses one timing function for both arms, includes verification,
and names the excluded cold, control, parity and cleanup phases. The original
harness hash and v1 records above continue to identify what actually ran.
