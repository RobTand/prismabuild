# #2085 CPU ambient disposition (not fleet qualification)

## Separate claims

- **Configuration population:** `Roster.at(repository_root)`, active presence from
  `box_status(name, entry)[0] == ACTIVE`, and positive CPU worker `loops` select
  sparky (5), gx10-6b77 (3, configured alias sparklina), and dl380g10 (16).
  wsl-gpu has three loops but is explicitly offline and is excluded. This is
  captured configuration, not deployed-runtime attestation or observed identity.
- **Source correctness:** W1 owns reference validation, statistics, explicit CPU
  collection/import/calculation, configuration transport and admission. This
  piece does not establish that implementation's correctness or deployment.
- **Measured qualification:** all three required hosts are **unqualified**.
  No new host measurement, qualified reference, independent review approval,
  held-out empirical replay or statistical configuration proposal was produced.
- **Declared transport:** each active CPU box selects `cpu-ambient-policy.json`
  relative to the roster's parent. That versioned policy has `hosts={}`. It
  selects no reference, k, evidence minima or bounds. It is intentionally not a
  proposed qualified statistical configuration. Per-host `proposed_configuration`
  remains null. The W1 agreement carries this selection into actual worker and
  controller configuration as `PRISMABUILD_CPU_AMBIENT_POLICY`; that execution
  path has not been verified here. An incidental operator shell is not authority.
- **Deployed support:** not claimed; `deployed=false`.

`acceptance.json` binds the final raw roster bytes and each portable inspected
source by SHA-256. `source-inventory.json` records extraction, origin, size and
limitations. These are source bindings, not evidence of authentic calibration.
The proposed formula is an **empirical envelope, not a Gaussian confidence
bound**. Remaining empirical acceptance under #1317 and #1322 stays open. No
provisional minimum or fairness constant is derived or changed here.

## Bounded read-only investigation

Inspected the top-level directories of `/mnt/shared/tessera-measurements` and
`/mnt/shared/prismabuild-fleet/measurements`, then the latter's
`astra-pb-tiers-20261002/1027-filter-pair/0-before` directory. Read that run's
`summary.json` and the three `netdata-*-system.cpu.json` charts named in the
inventory. The summary describes tier-cycle benchmark context (Python 3.14.4,
queue-shaped input); a small selected-field excerpt and its original raw digest
are retained. Three complete raw charts total only 6,765 bytes and are retained
verbatim for portable inspection. Chart labels associate sparklina with the
configured alias; they do not independently authenticate observed host identity.

These charts are aggregate CPU percentages, not assigned foreign-per-CPU
observations. Their timestamps and update interval do not supply missing CPU/tier
identity, attribution version, or affirmative holder-free and holder-tail-free
whole-interval provenance. They are **not** usable calibration or held-out
acceptance windows. No threshold was calculated from them. Historical sparklina
summaries likewise do not qualify the configured fleet. The bounded search was
selective, not exhaustive: other retained windows may exist but were not verified.
No remote host probe, controlled load, queue mutation, GPU/Docker execution,
deployment or publication was performed. Ordinary admitted two-CPU pytest actions
are report checks, not host-side idle-window observations.

## Supported follow-up procedure (pending, not executed)

1. Re-derive active CPU population with `Roster.at()` and presence plus worker
   loops; update the raw roster hash only after final roster bytes are fixed.
   Do not change status/population to obtain acceptance. Observe actual host and
   effective CPU/tier identity separately; never infer topology from loop count.
2. Once W1's explicit bounded CPU-only collection/import facility is available,
   use its documented supported interface on declared admitted resources, or
   import only fully verifiable retained windows. No concrete collection command
   or successful replay is asserted here. Preserve original interval/source
   timestamps and cadence and affirmative authoritative whole-interval holder-free
   **and holder-tail-free** records. Missing evidence leaves the host unqualified;
   collection alone neither approves nor selects a candidate. Do not use mutable
   admission history, a refused spike, sustained load or aggregate busy/PSI as
   calibration authority. Do not invent a credential/privilege requirement.
3. Retain complete portable raw sources and digests, effective CPU identities,
   attribution implementation identity, sample count/span/spacing, and an
   independently reviewed dependence/decorrelation analysis. Justify each host's
   k and minimum evidence/bounds empirically, not from synthetic test inputs or
   one historical host. Max+k*population-stdev is not a confidence guarantee.
4. Retain separate timestamp-disjoint ambient, foreign-spike, sustained-load and
   complete-proof clear/recovery evaluation windows, including all provenance.
   Calculate and replay through W1 facilities rather than a duplicate report
   implementation. Preserve full-precision strict comparisons and report every
   per-CPU max/stdev/threshold, false refusal and transition result. Reject
   detector-disabling envelopes instead of clipping them. Retain commands,
   results, source digests and canonical-content calibration SHA-256.
5. Only explicit independent qualification approval can authorize an immutable
   host-specific reference and proposed configuration. Bind repository-relative
   reference paths relative to the policy file. Keep proposed configuration,
   measured status and any separately attested deployment distinct. No automatic
   adaptation or deployment is required by this deliverable. Keep #1317/#1322
   acceptance explicit until all required populations have genuine evidence.

## Rechecking this disposition

From the repository root, the population can be inspected without measuring hosts:

```python
from pathlib import Path
from prismabuild.fleet_roster import ACTIVE, Roster, box_status
roster = Roster.at(Path.cwd())
population = {
    name: entry for name, entry in roster.boxes.items()
    if box_status(name, entry)[0] == ACTIVE and int(entry.get("loops", 0)) > 0
}
```

Check portable source bindings with Python's `hashlib.sha256(Path(path).read_bytes())`
against the raw hashes recorded in the JSON. The summary excerpt's `sha256`
identifies its original external source, not the excerpt file. There are no
calibration/evaluation inputs to replay: claiming a replay from these aggregate
charts would be incorrect. See `verification.md` for actual checks and limitations.
