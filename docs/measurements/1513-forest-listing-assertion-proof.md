# #1513: proof that main corrected the forest-listing assertion

Status: the controlled assertion proof is verified. Proposed disposition: close
#1513 as an assertion defect already corrected on main. Independent review and
merge remain required. The original event's exact trigger remains unknown.

## Scope

The close-with-proof target is the historical test's unconditional rejection of
residency-forest listings under the stage ownership lock. Main already changed
that assertion in `a56bd4de95cb3a9032b3bf359136f681d5d13c42` and
`c5646ec2cd42a2fa3ed563af9901d65fcb7c4b9a`, both for #1506.
This report proposes no production change. The original `7520710b811d...`
event has no caller stack or trust-refusal record. Its exact trigger remains
unknown.

The inspected and tested base is `6185a6ee60f32cea85e4c45144493767a7477998`.
Both assertion corrections are ancestors of that base. The task branch was
rebased once on origin/main before review preparation and was already current.

## Why a protected re-list is permitted

Source references at the tested base are
[`DirectoryCaptures._acquire`](../../src/prismabuild/kept_reads.py#L636),
[`_arbitration`](../../src/prismabuild/staged_path.py#L6230), and
[`_forest_census`](../../src/prismabuild/staged_path.py#L7286).

`StagedPublisher._forest_census` retains directory names through
`kept_reads.DirectoryCaptures`. An outside-lock census can retain a namespace,
then `LinuxFilesystemTrust.directory_current` can refuse its trust proof during
arbitration's protected redo. `DirectoryCaptures._acquire` discards the retained
slot and takes a new stamp. `_forest_census` then lists that namespace while the
ownership lock is held. Restored trust retains the new listing. Other unchanged,
trusted directories retain their names throughout.

An initial `directory_before` refusal has the same observable result. The first
outside-lock listing supplies names without retaining them, so the next census
must list that directory again. Refusing to re-list would trust names whose
freshness the cache cannot prove.

The historical test required `listed_locked == []` unconditionally. The current
`_assert_forest_listings` permits one re-list per earlier trust refusal for that
same directory, consumes each refusal once, and preserves per-directory and total
operation bounds. Stable trust still permits no listing under the lock.

The existing deterministic regression
`test_only_the_directory_that_refuses_trust_is_relisted` covers `stable`, `root`,
`namespace`, `initial`, and `both`. It uses the complete object-filesystem trust
resolver to inject a refusal, then restores trust. It checks actual census
listings, refusal counts, operation counts, and retained cache entries. The
fixture uses `after_paths` logical clock fencing, introduced by
`339e9df28383a7805b7fc68d43841cf393986335`, instead of a sleep.

The [earlier recovery disposition](1513-forest-listing-recovery-disposition.md)
retains later historical failures on both control and merged #1509 sources.
Their stacks associate protected listings with preceding same-directory
refusals. That report's execution-custody limitations remain. This proof uses
fresh controlled executions of the current regression rather than adopting the
old aggregate run claims or attributing the original incident.

## Deterministic negative control

The isolated worktree `PRI-184-strict` starts from the same base. Its only change
restores the historical zero-under-lock-listing condition at the beginning of
`_assert_forest_listings`:

```python
assert not any(under for _kind, _name, under in events if _kind == "list"), "historical zero-listing assertion"
```

This is a deliberately restored assertion, not an execution of the entire old
revision. The production implementation, controlled trust refusals, cache
behavior, and current operation bounds are identical. The negative control runs
only the existing five-case regression through published `pbtest.py`.

Observed on dl380g10: **4 failed, 1 passed, 2 deselected**, exit 1. Stable
trust passes. The `root`, `namespace`, `initial`, and `both` cases fail exactly
at the restored historical assertion after a protected re-list. The two other
tests in the file are deliberately deselected for this control only. The ten
green executions run all seven cases.

The failed action has descriptor-bound terminal and log evidence, and no success
receipt or local-result claim. It is a successful negative control, never a green
test run. Source SHA-256 is
`a8e31ad4622836eb5b369393dec4bf4047411534aac8a569e3304e659b41cb33`.

## Fleet qualification

Evidence directory: `/mnt/shared/paperclip-runs/PRI-184`.
`commands-v2.json` retains exact transient-unit commands. Units are
`pb-pri184-strict-x86-6185a6-v2` and
`pb-pri184-{x86,arm}-r{1..5}-6185a6-v2`.
Published generation: `189c563202ae-1793002494-cb98cac97353`.

Each file execution reserves 1 CPU, or 2 CPUs for xdist repetitions 4 and 5,
2 GiB memory, no GPU, and one native thread per worker. The hard deadline is
600 seconds. Green runs declare aggregate demand of 14 CPUs and 20 GiB.
The separate negative control requests 1 CPU and 2 GiB. Tags `x86` and `gb10`
select the requested architecture qualification; PB chooses the hosts.
`--durations=1` through `--durations=5` produce distinct action identities.

| Architecture | Repetitions complete | xdist complete | Test outcomes | Verification |
| --- | --- | --- | --- | --- |
| ARM | 5/5 | 2/2 | 35 passed, zero skips | Terminal, logs, source bundle, CAS verified |
| x86 | 5/5 | 2/2 | 35 passed, zero skips | Terminal, logs, source bundle, CAS verified |

The first unit commands refused unsupported `-q`/`-s` options before publication.
The v2 commands remove those options. Those refusals count as zero executions.
No runtime publication, new runtime generation, service restart, or production
source change is part of this work.

## Evidence verification

[`commands.json`](1513-forest-listing-proof/commands.json) retains every published
invocation. [`actions.json`](1513-forest-listing-proof/actions.json) pins all
11 action keys. [`evidence.json`](1513-forest-listing-proof/evidence.json) records
each terminal path and hash, snapshot input, source hash, full log descriptor,
receipt identity, result descriptor, and local-result claim.

The retained reports and detailed verifier output are under the evidence
directory. Recheck them against the fleet with this read-only command:

```sh
python3 docs/measurements/1513-forest-listing-proof/verify_evidence.py /mnt/shared/paperclip-runs/PRI-184
```

The verifier returned `verified 11 of 11 actions`. It checks terminal state and
return code, one actual attempt per action, complete collection and teardown,
each call outcome, architecture, native thread bounds, log lengths and hashes,
snapshot bundle lengths and hashes, and source-tree differences against the base.
Each green snapshot differs only by the PB closure stamp. The control adds only
the assertion shown above. Every green selected source has SHA-256
`2778d1750e5ef913c12f2241f9cf2e5135dc0b1e3dac6d12b1d559dfdfb7c777`.

For all ten successes, `pb_verify_claim(hash_payload=True)` returns
`checks_passed: true`, including receipt self-consistency and claim-to-manifest
binding. The verifier also hashes each receipt's result payload. Worker
attestation verification is outside that MCP check and remains `null`.
The serial runtime records bind the selected test source hash. The xdist
coordinator records no selected source hash. For xdist, source evidence is the
immutable checkout bundle and sealed test argv, with node startup and all seven
call outcomes in the execution log. This report does not claim a separate source
hash observation from each xdist worker.

| Run | Host | Outcome | Action key |
| --- | --- | --- | --- |
| strict-x86 | dl380g10 | 4 failed, 1 passed, 2 deselected | `2d53c99526e9205630c40923e9c7000c1bdd2b277c73a9ae9229c6ca39c8ad63` |
| x86-r1 | dl380g10 | 7 passed | `76e6223cbfa48e2b49a58e76d3018ff1d66e942fe9ffc24df941d783aed9aae2` |
| x86-r2 | dl380g10 | 7 passed | `d80748bd7cf3270ca4f3d12bf0eb955badc894775fa06683769003b7648bbb48` |
| x86-r3 | dl380g10 | 7 passed | `03a1c870a091389bb2e8af0ec61a35cf9a77bcd6240d74ef00e5567e172bf0f3` |
| x86-r4 | dl380g10 | 7 passed | `76e327194f26039dc510caf7a6e63d49c57d454a5bf44d95187466fd8d2b3f04` |
| x86-r5 | dl380g10 | 7 passed | `ae4c3d4fe0074c0fcfd1561487fd296c07595e533342521b7ba925b26af0757f` |
| arm-r1 | sparky | 7 passed | `ad0fa8267bca43b33a7b229e12587b6ace3bba346f117a8fd62645ce789f3230` |
| arm-r2 | sparklina | 7 passed | `364942ffcaeb65911f2183602a3d12e7b9fbb3dc41050996fc5c8106e44cb954` |
| arm-r3 | sparky | 7 passed | `8395f729306ad2544659663c9a8cb9a53b3e1c65670840b43cb525077f52048b` |
| arm-r4 | sparklina | 7 passed | `41a27ecd2dd628c7d216a940a6c6bed94be944179471806a81fcb3d4db424467` |
| arm-r5 | sparklina | 7 passed | `2186646b2254fa7b404ffef1c6a16c9c77e3e3e3226b68d4f45139500b440bb1` |

Terminal files live under `pb-queue/done/<key>.json`, or
`pb-queue/failed/<key>.json` for the control. Successful receipts live under
`cas/actions/v3/<first-two-key-characters>/<key>.json`. Full stdout and stderr
paths and their SHA-256 digests are in the evidence manifest. The downloadable
PRI-184 evidence archive retains the reports, terminal records, logs, successful
receipts, local-result claims, and result payloads. Checkout Git bundles remain
in the fleet CAS at their recorded input addresses. The archive is Paperclip
attachment `7c243c5f-0ef6-4ac7-a2a1-2241951f085f`, 215,598 bytes, SHA-256
`09cdeba65fedca6b03d29b8db21c9c2f0c47cadde9600b467125554007544ea0`.

## Review disposition

The evidence establishes that an unconditional zero-listing assertion rejects
necessary freshness recovery. Main already replaced that assertion with ordered,
single-use, same-directory refusal accounting. The current deterministic cases
cover the recovered behavior on both architectures, including stable-trust bounds
and repeated refusals. No further lock-scope change is justified by this evidence.

The proposed closure covers the invalid-assertion scope under the task's
close-with-proof option.
It does not recover the original event's missing stack or complete the historical
20-per-source comparison. The earlier recovery report's limitations remain.
The proof PR uses `Fixes #1513`; independent Engineer 01 review and the merge
queue decide whether to accept that disposition. No deployment is requested.
