# Tier-ledger directory reuse: current ZFS qualification

This is a hermetic CPU tier-cycle experiment for #1027, not a quantization or
live-queue result. The recovered cache revision removed steady ledger listings
but made this workload slower. It is not accepted as a performance improvement.

## Workload and source

One admitted dl380g10 measurement compared main
`2f72b9e1aba9bd5efe36975053dabe4e43bfb171` with cache revision
`9ef26305f00c62bf07805f29dfa92baeb9ff948f`. Both used the same existing benchmark
bytes and fixed fixture: 30,000 done records, 7,142 failed, 1,877 withdrawn,
6,000 receipts, 370 empty namespaces, 30 small namespaces, 145 fragments and
240,705 fragment entries. Each arm ran one cold and twenty steady cycles.
The frozen order was before, after, after, before in one action.

The private fixture's descriptor mount evidence identified direct local ZFS
(device `0:92`), not the unresolved Btrfs device. No live queue, mount, service,
calibration, or worker configuration changed. Resources were one preferred
physical CPU, 8 GiB RAM, no GPU, native threads one, priority zero, a 600-second
hard deadline, and no inner benchmark timeout. Python was 3.14.4/GCC 15.2.0.

The executing payload and exact source were sealed by PrismaBuild. Command and
script identity are retained in
`/home/rob/tmp/astra-resume-20261002/pb_tiers/1027-sealed-argv.json`; the historical
request is authoritative because the working protocol file subsequently moved
to the prepared changed-code experiment.

## Observed result

| Arm | Steady median, seconds |
| --- | ---: |
| Before 1 | 0.4272 |
| After 1 | 0.4892 |
| After 2 | 0.4814 |
| Before 2 | 0.4228 |

The median of the two arm medians was 0.4250 seconds before and 0.4853 after:
**14.2% slower**. Both after arms recorded `ledger_listed=0` and positive
`ledger_kept`. Eliminating the directory reads alone did not improve throughput.

The raw py-spy profiles sampled the existing benchmark at 250 Hz. Across twenty
steady cycles, inclusive `Path.__lt__` samples rose from approximately
1.77–2.06 seconds to 3.68–4.01 seconds; the dominant added caller was
`ResourceLedger._scan_names` under `_glob_names`. The adapter constructed and
sorted every path, filtered the result, then sorted the matching paths again.
Inclusive frame totals overlap and must not be added together.

All four arms, 84 cycles, 68 per-arm artifact files, source hashes, immutable
logs, the canonical CAS payload, and the payload's binding to the external result
digest were checked. The terminal reports executed/0 and complete released
scope cleanup, including stopped and empty scope evidence. Aggregate scope peak
memory was 440,848,384 bytes; aggregate CPU time was 95.431 seconds, including
setup, profiling and evidence collection rather than only the cycles.

Action: `01b50feff9ff813a669c77ddddba33e8d50683f91aa8caa1760527f3a411f4b4`.
Artifacts:
`/mnt/shared/prismabuild-fleet/measurements/astra-pb-tiers-20261002/1027-zfs-pair-fqdn/`.
Independent checks and profile attribution:
`/home/rob/tmp/astra-resume-20261002/pb_tiers/1027-measurement-fqdn-verified.json`,
`1027-measurement-artifacts-verified.json`, and `1027-profile-attribution.json`.

## Host telemetry and retained negative evidence

All three Netdata agents were qualified by their expected local hostname and
GUID through `.lan` endpoints before setup. CPU, RAM, I/O and load responses for
each arm were retained for dl380g10 and both Sparks. Original responses used
Netdata's default aligned windows. A verifier caught their extra leading buckets
and incomplete trailing extent; the original responses were preserved, and the
same historical windows were subsequently read with `jsonwrap,unaligned` to
retain explicit API bounds. These supplemental reads are not new benchmark runs.

The query alignment is documented in the
[Netdata query contract](https://learn.netdata.cloud/docs/developer-and-contributor-corner/rest-api/queries/).
Native CPU/RAM/I/O intervals were one second and load was five seconds. API
average buckets and virtual points do not establish raw sensor peaks or clock
agreement. Supplemental raw responses and their hashes are under
`/home/rob/tmp/astra-resume-20261002/pb_tiers/1027-netdata-unaligned/`.
No GPU utilization, energy, work-per-joule, or whole-campaign claim is made.

The preceding action `fe3ecaf090221dae5e21585deb30bc311b94e927f680261222283daf82c90f37`
failed during Netdata collection because the short Spark hostnames did not resolve
on dl380g10. Its completed first baseline arm remains failed-action exploratory
evidence and was excluded from the successful comparison. Its private scratch
and output are retained; no successful receipt was assigned to the failed action.

## Prepared correction, not yet measured

Refactor `bb0433c3961b` centralizes the existing names-reader selection.
Revision `b30c1ded419d67d2745c2e104f6daf9bb08c717e` then filters flat names before
constructing paths and sorts leaf names once with native case normalization.
It keeps the same clock/filesystem trust, error visibility, optional-parent
absence proof, token mutation authority, and scoped reader.

The meaningful allocation RED `204abe73092e` failed one assertion and passed
28 controls: the old adapter constructed 64 unrelated paths. Earlier run
`f73583019f7b` had a recursive instrumentation fixture and is not causal RED.
Current revised-source checks `97869b51a4c5` and `101df02d4532` passed 32 and 5
cases respectively, with no skips; canonical CAS, source parent, closure-only
snapshot deltas, full log hashes and released scope cleanup were verified.
Their source head is exactly `b30c1ded419d67d2745c2e104f6daf9bb08c717e`.

A new paired profile against the same main baseline is prepared but has not run.
The source change is not a speedup claim. Full #1027 acceptance, merge and
deployment remain with the coordinator.

## Continuation: corrected adapter, measured 2026-10-02

The prepared correction above subsequently ran under a new explicit root GO.
This continuation preserves the earlier negative result and pending-state history.
One new admitted action used the same main baseline, fixture, source-bound
benchmark bytes, ABBA order, CPU/memory/native-thread limits and 600-second bound.
The changed source was exactly
`b30c1ded419d67d2745c2e104f6daf9bb08c717e`; it did not adopt any completed arm
from a previous action. The qualified fleet runtime was the restored `5b6`.

| Arm | Steady median, seconds |
| --- | ---: |
| Before 1 | 0.3642 |
| After 1 | 0.2495 |
| After 2 | 0.3183 |
| Before 2 | 0.4246 |

The median of arm medians was 0.3944 seconds before and 0.2839 after:
**28.0% less steady-cycle wall time** on this fixture. The corresponding
in-process CPU medians were 0.3710 and 0.26665 seconds. Both after arms recorded
zero ledger listings and positive reuse. Each before/after pairing has the same
direction, but the changing host load means this is a scoped observation rather
than a constant-load or whole-application performance claim.

The profiling change matches the source correction: inclusive `Path.__lt__`
samples across twenty steady cycles were 1.86/2.06 seconds before and
0.032/0.040 after. Inclusive `_parts_normcase` samples fell from 0.880/0.928
seconds to 0.024/0.036. These overlapping sampled totals cannot be added.
The dominant path-sorting work has disappeared; directory freshness checks remain.

The original complete raw Netdata responses now use `jsonwrap,unaligned`, retain
the agents' native intervals, and cover all four requested wall windows on
dl380g10 and both Sparks. A bounded read-only wait let the last collector bucket
arrive; it never restarted a benchmark arm. dl380g10's mean sum of non-idle
dimensions (including I/O wait) over each arm's in-window API points was
5.47%, 4.59%, 9.71% and 12.12%. This records the load drift; it is not a raw peak
or an energy measurement.

Action:
`ad2b1a4e1b7ea7b3ea99df5cf2db66d4db83755bc69ffc27d79334ba6369df7d`.
It executed/0 on dl380g10 in one attempt. Canonical CAS lookup, immutable full
log hashes, source bundle/parent/closure, result-to-artifact digest, all 68 arm
files, all 84 cycles, 48 raw chart windows, and complete released/empty/stopped
scope cleanup were independently checked. No live queue or runtime was changed
by this experiment, and no GPU, work-per-joule or served-throughput claim follows.

Artifacts:
`/mnt/shared/prismabuild-fleet/measurements/astra-pb-tiers-20261002/1027-filter-pair/`.
Checks and attribution:
`/home/rob/tmp/astra-resume-20261002/pb_tiers/1027-filter-measurement-verified.json`,
`1027-filter-artifacts-verified.json`, and `1027-filter-profile-attribution.json`.
The root coordinator owns acceptance, merge and any later deployment.

### Estimator clarification from independent review

The existing benchmark's `steady_median_wall_s` uses the upper middle observation
for an even number of steady samples. The tables above preserve that historical
estimator; the 28.0172% calculation uses it rather than the ordinary median.
Ordinary medians of the twenty steady observations are 0.36205/0.42445 seconds
for the two before arms and 0.24910/0.31790 for after. Their aggregates are
0.39325 and 0.28350 seconds, a 27.9085% reduction. **Approximately 28% less
fixture wall time** is the appropriately precise claim. No new run or arm
selection was used. Independent calculation:
`/home/rob/tmp/astra-resume-20261002/pb_tiers/1027-estimator-check.json`.
