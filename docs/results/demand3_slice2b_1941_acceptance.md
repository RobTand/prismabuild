# Demand 3 slice 2b source acceptance

PR #2299 implements #1941 and existing children #2102 and #2103. This record covers source validation for independent review. Existing worker loops retain one-shot publication. Later slices install the owned publisher session and consume semantic input changes. This change is not deployed.

## Freshness validation

Tested source `b7b33b021c3b265cd61894ed8636d2e46b2d50f0` is rebased onto `4c18ee61e1943b039027e08a2c5e8b20ffb1a291`. All 11 reviewed source and test SHA-256 values are unchanged. The sealed checkout `162fcbe21d421b0d8d268b887c66a8130380e096` differs from that source only by PB's generated closure record.

The complete 27-file population passed in one PB run: 654 unique tests collected, ran, and passed, zero skips, two existing warnings, and 20 successful shards. Tests ran on dl380g10, Sparky, and Sparklina. Each action declared one CPU, two GiB, no GPU, and native thread ceilings of one. The declared sum was 20 CPUs and 40 GiB. The default eight clients bound outstanding client concurrency to eight CPUs and 16 GiB. PB chose partitioning and placement without tags.

Transient unit `pb-pri64-refresh-b7b33b021c.service` completed with exit status zero. Its report is `/mnt/shared/paperclip-runs/PRI-64/refresh/integrated.json`. The adjacent `submission.json` records the exact argv, source, base, file population, resources, 180-second action deadlines, 600-second wait, and priority -10.

The acceptance JSON records every terminal, both immutable log digests, canonical CAS receipt, snapshot binding, and result payload digest. The audit read those artifacts and reconciled collected, outcome, and teardown identities. It also read the sealed checkout bundle and compared its source and test blobs. All current actions finished after the fleet incident. Suspect pre-incident action `45b0b6c073669f4f92edb0319591cb381d944727b880565cf4d3f7b51768575d` is excluded from merge evidence.

Rerun the read-only audit on a coordinator with access to the retained PB artifacts:

```sh
python3 docs/results/demand3_slice2b_1941_verify.py \
  --run-dir /mnt/shared/paperclip-runs/PRI-64/refresh \
  --checkout /home/rob/paperclip-worktrees/prismabuild/PRI-64-refresh
```

The audit creates an isolated bare repository under the run directory to inspect the sealed checkout. It executes no tests. Reuse the exact `submission.json` argv through a transient user unit when a new validation run is required.

## Review and publication state

[Independent re-review](https://github.com/RobTand/prismabuild/pull/2299#issuecomment-6405556388) passed Standards and Spec at `fb6c674a0e90c45086c2d5027d3a381f78613009` with zero findings. The freshness hand-back is administrative, not a second failed review.

The rebased source is local. The published PR remains at that reviewed head. Connection discovery reports the installed GitHub connection ready, but a PR read returns `github_identity_unavailable`. A normal Git push dry-run also fails because managed credentials are unavailable. Main subsequently advanced to `e994ac98f3843a1a2a6936cee3a736241b52abee`. Restore access, rebase onto then-current main, validate the affected population through PB, refresh this record, and publish before returning to Code Reviewer 01. No current merge-readiness claim is made.

The JSON separates target, implementation, and deployment for scoped requirements ID-08, ID-09, and VER-01. Source review, source validation, publication, and deployment remain distinct facts. The Principal Engineer owns release. The next slice is #1942 after the merge is verified. The Chief of Staff relayed Rob's #1948 decision; verify its exact source before changing that later slice's scope.

## Reproductions and repaired review findings

The original PB regressions exposed missing adaptive-input coverage in `13e4ff51cc181ba8e8cb45390d25f106520afed9e17d5efa7cd79e4ec9e258a9` and stale mirror removal in `6f717b8190d5a83366c3b66b2570517c4f3b81e9e9c11e7d10ad870f605e7e79`. Internal review exposed overflow watch loss and unbounded shared comparison reads. Four deterministic regressions failed in `52cc55de45d9eade78752f9cd905bb50613d1c5161862e7e913ba3601e6e46d0`; the repaired publisher passed 86 cases in `4301d8b746e01bb2f2cded052d3ff799a2de2efaf969a4c0eb5bc866ffe37894`.

External review round 1 identified failed empty-source retries and retained copy backoff after recovery. PB actions `60466f2b847456d7cc749972b43b532cd7822cf19b4bf00ba5a114791f1c8f0a` and `fa19b9abecdd80cb2188cec29683a01556cef3129ee29ef2901aca2fbdb4a02c` reproduced the fake-clock failures. Partial-denial regressions failed in `5f52c461c4a909fea2f0e130e24f5c3a3a60866cf8ae29085f62eeb3cade6353`.

The fix keeps failed empty-source publication dirty, resets recovered copy backoff, handles partial denial records, and reuses shared inotify constants. State decisions were separated before behavior changed. The revised source previously passed 650 integrated tests and four supplemental recovery tests in 21 actions, all after the incident. Those historical results and the passing independent review are retained in the JSON. The current 654-test integrated run supersedes them for this exact local source.
