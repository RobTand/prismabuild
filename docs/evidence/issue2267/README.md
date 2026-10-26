# Measurement drain qualification for #2267

The deterministic regression fails before the fix for one and two elected measurements. Fresh affirmative foreign CPU load now suspends the exact measurement fence within one claim pass. The persisted election and original opportunity remain unchanged.

The delivery source was rebased on `6a96867b3c4a`. Local `d95f1c1d7a1c` and remote `7dbd10791526` have identical tree `ff77bdff584d1db0d6a517bf5850d1d7f6aa6f45`. Git push credentials were unavailable; the connected GitHub API recreated the test-first commits, and the fetched remote tree was compared with the local tree.

| Architecture | Repeat | xdist workers | Result | Action |
|---|---:|---:|---|---|
| x86_64 | 1 | 0 | 103 passed, 8 warnings in 66.78s (0:01:06) | `cbee1e0e3f48696d08a071a7d8baf929fde005191b8f6f56f927d4dbbc5ff191` |
| x86_64 | 2 | 0 | 29 passed, 8 warnings in 11.53s | `7c11870daede25bbfd822df8742be91957b9bb40b1f369dc862d059f9c63d2d6` |
| x86_64 | 3 | 0 | 29 passed, 8 warnings in 14.32s | `7c8631722e90e94d334efad3f34cca1512c6113fccfdfd8b4abd2d2bc6360a8e` |
| x86_64 | 4 | 2 | 29 passed, 241 warnings in 8.23s | `14aa227dadfa1864b337ed24c9fc5f8ed5bf6e0099dafa8b62d29561dbabc96f` |
| x86_64 | 5 | 2 | 29 passed, 241 warnings in 12.82s | `ac554a69b23faeb24ac0f9bcbef251bb07f35bbe40ebc15e1e3ebd9a3abe7a4f` |
| aarch64 | 1 | 0 | 103 passed, 8 warnings in 34.30s | `1a295e617ce064f6540add7d9502f7d36fe28cb01edfcc744c09a95b61d6527f` |
| aarch64 | 2 | 0 | 29 passed, 8 warnings in 7.30s | `75328df329c2cc3c5ee8d3e65b1df9be9df0d118236e87609cd16f1091ff706a` |
| aarch64 | 3 | 0 | 29 passed, 8 warnings in 7.52s | `3984f75db5bbcd3e4c53668a4283b742bc20d1a43c6f746bf81448c07d63346b` |
| aarch64 | 4 | 2 | 29 passed, 241 warnings in 4.20s | `34d6b55126b91ac8ef0ebc06ce3d17a28709baf2ac329c6a3d7c036ed3bd908a` |
| aarch64 | 5 | 2 | 29 passed, 241 warnings in 4.25s | `60a459349f9abdca79d0e283013d0b59cf45f0f52c376405bc9931b4a757111f` |

No tests skipped. Existing fork DeprecationWarnings remain on both architectures. Every action finished outside the suspect dl380g10 window of 2026-10-25 21:45–21:52 UTC. Terminal records, runtime provenance, executed source trees, log digests, execution receipts, CAS payloads and scope cleanup were read back. The complete runtime records accompany the Paperclip artifact.

Each action reserves one CPU, or two for xdist, and four GiB with zero GPUs. Native OMP, MKL and OpenBLAS threads are one. The two-action window reserves at most four CPUs and eight GiB. Action timeout is 300 seconds; controller wait budget is 1800 seconds.

Recheck the retained evidence from a checkout with the same src, tests and tools trees:

```bash
python3 docs/evidence/issue2267/audit_validation.py \
  /mnt/shared/paperclip-runs/PRI-26/rebased-matrix.json \
  /mnt/shared/paperclip-runs/PRI-26/rebased-matrix.log \
  /tmp/issue2267-validation.json
```

SC-01 requires PB-owned admission. This repair keeps the policy in `PassCensus`. LIVE-01 remains a whole-policy target with incomplete implementation and workload proof. [The scoped merge record](merge-acceptance.json) validates only this component. No runtime was published, no service restarted, and no live measurement submitted. Principal review and merge remain required.

The original recovery manifest refused a sealing-key mismatch before any publication. Previously completed x86 actions were verified outside the crash window. The integrated ten-row matrix uses a new source snapshot after rebase and does not mutate or replace those actions.
