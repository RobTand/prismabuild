# Submission wait inventory, issue #1888

This inventory covers preparation before runnable publication by `pbrun`,
`pbtest`, and `pbcampaign`. A process exit is an event. A caller's deadline
is a moment in time. Slow disk I/O does not establish a Git failure.

| Owner and operation | Former cap | What ends the wait | Contract after #1888 |
| --- | --- | --- | --- |
| `core.git_checkout_identity` through `_identity_git`: top-level and HEAD probes, untracked/ignored rosters, special-inode ignore check, object/index path probes, binary tracked diff | 30 s per call | Git exits, an event | No per-call cap; optional caller deadline |
| `core._git_run` and `_git` defaults | 30 s | Git exits, an event | Default `None`; explicit non-submission timeouts remain supported |
| `submission.git_repository_root`: `rev-parse --show-toplevel` | 30 s | Git exits, an event | No per-call cap; optional caller deadline |
| `submission.tested_repository_name`: top-level and common-dir probes | 30 s per call | Git exits, an event | Same |
| `submission.keep_droppings_out_of_git`: common-dir probe before exclude migration | 30 s | Git exits, an event | Same |
| `submission._snapshot_git`: roster, attributes/config, sparse and complete-history checks, branch validation/resolution, object probes, alternate-index `read-tree`, forced pathspec `add`, `add -A`, stamp `hash-object`/`update-index`, `write-tree`, tree/blob inspection, `commit-tree`, bare `init`, `update-ref`, pack object-format probe | 120 s per call | Git exits, an event | Same; all calls share the same absolute deadline |
| `submission.write_deterministic_bundle`: binary-streaming `pack-objects` | 1,800 s | Git exits, an event | Same; no `BUNDLE_PACK_TIMEOUT_S` constant |

`submission_frontend`, `fleet_submit`, and `tools/fleet/pbrun.py` have no
additional local-Git subprocess cap. They delegate preparation to `submission`.
`tools/fleet/pbtest.py` prepares each pool plan there once. Its final identity
recheck, after publication, also inherits the uncapped default.

`pbtest` starts each shard's `--wait-s` deadline before submission. It now passes
that absolute monotonic deadline through `_Templates`, `submission.preparation`,
the snapshot helpers and checkout identity. Every subprocess receives only the
remaining budget. A windowed campaign starts its deadline after the initial
window and passes it through `submit_row` and `submit_request` for replacement
rows. The initial window still publishes with `--wait-s 0`. Ordinary `pbrun`,
non-windowed campaigns and logical decomposition have no preparation deadline;
their wait begins after publication. No new duration constant is introduced.

Nonzero exit handling, accepted return codes, transport refusal causes,
identity bytes, snapshot bytes and size limits remain unchanged. An expired
caller deadline refuses instead of accepting a partial identity or snapshot.
Shared filesystem operations outside Git remain synchronous; this change does
not make a kernel-blocked filesystem operation interruptible.

## Retained waits outside local Git

| Boundary | Cap or budget | What it waits for | Ending |
| --- | --- | --- | --- |
| `submission.bounded_offer_snapshot`, including `pbtest` preflight placement | 5 s | Isolated worker-offer directory/record scan | Scan reply is an event; budget expiry is a moment in time |
| `submission.bounded_attachment`, before a detached re-publication | 5 s | Existing submission, terminal and lease observations | Reader reply is an event; budget expiry is a moment in time |
| `slurm_lane._run` | 60 s per scheduler command | Remote controller reply through `sbatch`, `squeue`, `sacct`, etc. | Process exit is an event; remote-command cap is unchanged |
| `filesystem_floor._command` | 10 s | Local `zfs`/`zpool` space probes before submission writes | Process exit is an event; non-Git cap unchanged |
| `filesystem_floor` lock acquisition | 5 s default | Filesystem-floor transaction lock | Lock acquisition is an event; existing timed retry unchanged |
| `core` NVIDIA, SLURM and Python toolchain probes, where requested | 10 s, 30 s, 30 s respectively | Device/controller/toolchain observations | Process exit is an event; non-Git caps unchanged |
| Client outcome observation and offer retry | Existing `--wait-s`, reader budgets and backoff | Published action verdict or a service that cannot call back | Verdict is an event; caller deadline is a moment in time |

Offer freshness, cleanup grace, resource admission deferrals, payload execution
ceilings and per-test alarms are separate contracts and remain unchanged.
`seal_and_publish._git` explicitly retains its 30-second timeout outside these
submission entrypoints. No remote wait has been changed.

The regression tests simulate Git calls taking 1,801 seconds at the subprocess
seam without sleeping. They exercise real identity reads and real snapshot
creation, with both no deadline and one shared caller deadline, plus refusal
on caller-deadline expiry. Tests that required the old identity/snapshot caps
now check caller-deadline refusal or uncapped defaults. Existing injected
nonzero-exit and transport-failure tests retain their refusal assertions.

This is a source change. It claims no deployed runtime, staged-read ledger
promotion, or completion of #1888's two consecutive TrustLadder gate verdicts.
