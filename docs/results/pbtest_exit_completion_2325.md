# Blocker fixture output backpressure (#2325)

`blocked_shard` waited for readiness while leaving merged child output in an
unread stdout pipe. The blocker dumps all thread stacks before it signals.
If output fills the pipe, the child cannot reach the signal. xdist adds worker
and receiver-thread output before the controller reaches its exit hook.

The repair captures merged output in a temporary file and waits on the child
separately. It preserves the readiness handshake, the 30-second failure bound,
and every existing exit, summary, completion, and non-green verdict assertion.

## Deterministic reproduction

`test_blocker_readiness_survives_output_larger_than_a_pipe` writes 1,245,184 bytes
of diagnostic output inside the real blocker before it signals readiness.
It exercises both single-process and xdist children and requires the complete
diagnostic plus the final `2 passed` summary after the fixture kills the child.

Pre-fix commit `8ebda7b8ef0` produced:

```text
2 failed, 22 deselected in 60.37s (0:01:00)
AssertionError: The shard did not reach the controlled blocker.
```

The assertion occurred in both regression parameters. Pre-fix action
`0e617329f1ec2283a6b162d75dcdaddabd5086ab0e4d46cdef0aab29eabf2320`
ended failed with complete cleanup and no successful CAS receipt.

Fresh selected runs on unchanged main `6185a6ee60f32cea85e4c45144493767a7477998`
passed on x86_64 and aarch64. They establish that the natural trigger is
intermittent. The explicit output load makes the fixture defect deterministic.

A separate diagnostic run reduced the original xdist fixture's output pipe to
4 KiB and passed, action `b36a10902d1e5cc14b24b2cc84f9670792a24e78653329e7fd179bd0e28d7d92`.
Capacity alone does not reproduce every run. The historical failures discarded
child output, so their exact natural fill event cannot be reconstructed. The
forced-output regression proves the unread pipe's causal failure mode.

## Repeated fleet validation

All ten full-file runs on fixed code produced `24 passed`. Runs 4 and 5 on each
architecture used two outer pytest-xdist workers. Each run varied `--durations=N`
to change its sealed identity. PB placed the actions within the requested x86
or GB10 architecture class. No runtime generation was published.

Tested code commit: `65cce925be7b4dce3487c461f9da73de52a5a1f8`.
Tested file SHA-256: `4957c1b0ce744c25d54e3a453adbced8d29e5cf6ce668a3cfdfd08d16f502ec0`.
The six single-worker reports emit this hash. The four outer-xdist controllers
emit no selected-source hash; all ten sealed snapshots name the tested commit.
The PR adds this evidence document after testing and keeps the test bytes intact.

| Architecture | Repeat | Outer workers | Action key | Verified receipt SHA-256 |
| --- | ---: | ---: | --- | --- |
| x86_64 | 1 | 1 | `f94a1ffddce7740fe86fc826889d5ea7856117112b6cdcdac46a7ad2c1e4e163` | `e80a27cc54eea69e8b32d18ce011212c1765375019df806fe22c65b57afadf7a` |
| x86_64 | 2 | 1 | `a65dca7dc77b61c8d77694d16391fd4d8a88268bdd895db82c27b5a2c36378b6` | `2285996611ec0beb8cb9fbf0c549f385bc7860d2c143c24f922fe76bfaf907a9` |
| x86_64 | 3 | 1 | `213984c40b56d4505bbb138518c21c2a4f7373c80f2c103eaaed6c1306d41cd5` | `37c83e9bb29dd0647d2307ba4ba5174e174f2174aa621fee1907625b3c74c2e6` |
| x86_64 | 4 | 2 | `2dbf19ff454c35a1367f16c0f2c4371ce2975c3cea9adbd750076182f1ef94da` | `db1fef8d1d23cd00131acaf269526a6a0c7e8b970d386c80cdc2d3b2435abbfe` |
| x86_64 | 5 | 2 | `9983d1ffed261cbd50bdd83aa3d741b96f9293ec3cc27c4303f1c3ab170c3531` | `0b8cbddbf09693ac177387c7d8f57c54f49c720a53e4efa0adc149d0783cc6cf` |
| aarch64 | 1 | 1 | `7e7963e034fcda0a8d7b58c8683b7fc21d03e30c96e0fbfb18de5bad14dde5c0` | `885c27d5d481eb204dc5472392ac8f93bdbb87573ca8d8b8444034f8e081065e` |
| aarch64 | 2 | 1 | `55fc2c9ef4cdde9531af59cecb0ac39c2a514a23d64fa0c3f112b3c2165e3c48` | `333e9607d0657ede7eca95e75a3dc3c577f6d1edec4812d9930d40d5266b7749` |
| aarch64 | 3 | 1 | `96454614a711fe7786c83fabc81cbc41054b5c56ba8cfd15db37cc4f6fd006cd` | `4520a3c3b4ecdaffe8d8159bc229f1426abdc247aa931f5082b93cf92ce92394` |
| aarch64 | 4 | 2 | `138ce5211f9dabca87b0c4b92877425c8746e15a2f12a206e567614f4b5356fe` | `aed82a7c4d9559622027877ae4b543fd57482a3408d3e2e0261161b3c979745a` |
| aarch64 | 5 | 2 | `5c4e131507a480603fca69d33d467148aa83f0ad7bee7da3bb9268bde688a5ee` | `2668275ad7945bd66ce99bec82fa43e26942d9f9d95d5b292663efd94469838b` |

Each shard reserved 4 CPUs and 4 GiB for one outer worker, or 8 CPUs and
4 GiB for two outer workers. Native thread limits were 1, GPU demand was zero,
and the explicit shard deadline was 240 seconds. Scratch was `/tmp`; this file
does not test durable filesystem semantics.

## Retained evidence

Run directory: `/mnt/shared/paperclip-runs/PRI-180`.

- `validation-units.json` retains every exact command, transient unit, and report path.
- `validation-actions.json` proves all ten snapshots have the tested commit as parent.
- `verified-evidence.json` maps each action to full stdout/stderr paths, byte counts,
  SHA-256 hashes, host architecture, emitted source hashes, cleanup, and receipt hash.
- `endings.json` retains terminal records. `receipt-<key>.json` retains each successful
  receipt after `prismabuild_worker.py verify --action <sealed-request>`.
- `audit_evidence.py` reruns the read-only audit against the reports, logs, terminal
  records, and CAS data. It verified 14 actions and 13 successful receipts.

The evidence archive is attached to Paperclip PRI-180 as attachment
`c2ad3d96-a2ec-4918-9856-e5a6e72f2cfd`. It contains reports, complete logs, receipts,
the command ledger, snapshot evidence, and the auditor. Archive SHA-256:
`47106b73fb8605e2de4608f32d7300f3a7b12a30a2d6c0cdc62890a492830de6`.

Validation covers the fixture and its existing pbtest verdict checks. It makes
no deployment or whole-suite qualification claim.
