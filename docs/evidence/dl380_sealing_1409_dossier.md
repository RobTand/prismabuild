# #1409 DL380 pre-admission source sealing: unresolved evidence dossier

## Disposition and evidence standard

**Cause unresolved; affected-client recovery unverified. No production fix or fresh
submission is proposed.** This dossier addresses #1409 only. It adopts the
[checked-snapshot family census](source_sealing_family_1409_1410_2026-10-01.md)
and [PB #1888 Git deadline policy](../submission_git_waits_1888.md), not another
timeout mechanism. PB #1888 / PR #1901 is distinct from consumer PQ #1888.

Repository inspected: `7bf539a90402fbab88f58bd834e2abf7ee5e1e34` (initially clean).
Source and currently readable publication bytes were inspected directly. Historical
incident outcomes below are **attributed reports**, not newly authenticated client
logs: the exact original records were not readable in this context. A reference
is preserved as a recovery lead, not represented as recovered evidence.

## Historical incident ledger (O1)

Primary reference: [#1409 body](https://github.com/RobTand/prismabuild/issues/1409),
created 2026-10-01 08:52:19 UTC. Full committed PQ source was reported as
`7d15d3c8e061fb516e434bf8b88d0c5d8f356da6`, 3,199 tracked paths,
79,612,424 logical bytes, clean; worktree
`/home/rob/tmp/claude-campaign-20260926/wt-sol-pb-sdk3-1888`.
These are **consumer source identities**, not PB runtime revisions.

| Client / record | Reported operation and outcome | Admission and execution |
| --- | --- | --- |
| Build `b42b3c2ed`, issue body | `cannot identify checkout: cannot compute pbrun checkout identity`; isolated `diff-index -p --binary -M --no-ext-diff --no-textconv HEAD` exceeded 30s | No action; no build/test result |
| Retry `bcd37bac8`, issue body | `cannot snapshot checkout`; `git add -f --pathspec-from-file=- --pathspec-file-nul` exceeded 120s | No action |
| pbtest `bd7baa138`, issue body | Same snapshot timeout; `NO PYTEST SUMMARY` | Assigned file never ran; no action key; not behavioral RED |
| Startup integration, [comment 5932648740](https://github.com/RobTand/prismabuild/issues/1409#issuecomment-5932648740), 13:40:08 UTC | Same force-add 120s refusal on clean `2ce208ee402aadda7d30f25508ba106416b39baa`; 3,227 files / 79,873,140 logical bytes, no missing files/external filters; remained clean | Zero actions/tests/receipts from failing client |
| Sampler RED `b8825086a`, [comment 5934384729](https://github.com/RobTand/prismabuild/issues/1409#issuecomment-5934384729), 15:15:09 UTC | Same force-add 120s refusal on full detached `2ce208ee402aadda7d30f25508ba106416b39baa`, worktree `wt-sol-pq-observer-1933-sampler-RED` under the same campaign directory | No queued/attached action, pytest summary or receipt; three selected files never ran. Worker deadline 300s did not govern this pre-admission step |

The startup owner's one supported same-protocol retry reportedly sealed/published
`f7bd17a26b7c6817f39ce3d36eaa9b4f0465726bcb653bcace40c0d7c72d9dad`;
its 18-suite result was still pending in that comment. Intermittent success does
not qualify a repair.

[Comment 5928374243](https://github.com/RobTand/prismabuild/issues/1409#issuecomment-5928374243),
09:12:08 UTC, records Sparky sealing the exact `7d15d3c8e...` parent. Action
`3b525047cc740c5ee5e58f6b0b20fc3ef2a8ca2fe635f4e5a3f9810b394e9a57`
then ran on DL380 at 08:54 UTC, rc1, **2 failed / 4 passed / 0 skipped**, six
collected/run; real-resolver test deliberately deselected. Rob independently
checked immutable attempt/log hashes and parent at that time. This is attributed
consumer migration RED (old reader pin/SDK1 acceptance), not DL380-client sealing
recovery, passing SDK3 migration, or an expected RED for the unadmitted clients.
No successful CAS receipt is expected for that failed action.

## Runtime reconciliation

The issue names the historical entrypoint
`/mnt/shared/prismabuild-fleet/repo/tools/fleet/pbrun.py` and its fixed 120s
snapshot implementation, but supplies **no historical PB commit, generation,
receipt digest, entrypoint hash or loaded module hashes for the failed clients**.
Those cannot be inferred from PQ HEAD, line numbers, timeout wording, today's
symlink or a later successful action.

Git inspection verifies integrated diagnostic commit
`b3ecd33adbb195114023dda2598d4cfb5b1d23b9` (#1432), and deadline commit
`84cfb2fe054e89a667e015d932c8f0f62f10607c`, through merge
`bdd65823270ef70f84a9bed7b9b36271002c5176`, is an ancestor of inspected HEAD.
The census's 30/120/1800-second descriptions describe the historical diagnostic
change, **not current submission policy**. Current ownership is
`src/prismabuild/submission.py` (`_snapshot_git`, `_snapshot_fail`,
`write_deterministic_bundle`, preparation) and `src/prismabuild/core.py`
(`_git`, `_git_run`, `git_deadline_timeout`, `git_checkout_identity`).
Operation-specific refusals retain argv, failure cause and fail-closed semantics.
Without an absolute caller preparation deadline submission Git waits for exit;
with one, each operation consumes the remaining shared monotonic budget. Pack
streaming uses that same budget. Worker payload deadlines are separate.
No obsolete timeout constant was restored or increased.

Publication lookup followed the existing defaults in `submission_frontend.store_for`,
`submission_types.FleetStore`, `runtime_paths.generation_root` and
`tools/fleet/publish_runtime.py`, as documented in the operating guide's
"Publish the runtime the fleet executes" section. Read-only inspection found:

* Live receipt resolved to `/mnt/shared/prismabuild-fleet/runtime-generations/e9e9d9077212-1792715962-50f991688c86/RUNTIME_VERSION.json`.
* Receipt SHA-256: `3b844488032d0a1afa23d02b231a4d131f1a7b1443d75af1a4c59cb98da1d880`.
* Receipt declares commit `e9e9d907721282f517e4e8feca40eda1aec79e5d`, dirty=false,
  published_unix=1792715967.2358475. Git ancestry confirms deadline commit
  `84cfb2fe...` precedes this declared commit.
* Relevant generation bytes matched receipt `files`: `src/prismabuild/submission.py`
  SHA-256 `a0071c3188aa5d9e5951c4d322a01f691719e97b0003bc3a7fb429c928aa8d8f`;
  `src/prismabuild/core.py` `d398c3541cdc26e808191dd98d1674bd9c9c3a33c320967ca4b9a3323ad5e461`;
  both `tools/pbrun.py` and `tools/fleet/pbrun.py`
  `c1856fd267cd310ec68c88bd8858298d58ef903f853add5a0e974cc0e42b39c8`.

This establishes readable publication receipt/member consistency **now**, not
which generation an October 1 client loaded, DL380-local runtime visibility,
worker attestation, complete generation verification, or successful sealing on
the affected path. No publication was performed.

## Causal assessment (O2)

| Hypothesis | Attributable evidence | Limit / conclusion |
| --- | --- | --- |
| Storage contention | Rob's 09:06–09:07 readback reports PSI some avg10=91.06%, full=82.12%, ~60% CPU iowait, 256 NFS-server threads in D state, server/Git `zio_wait -> dmu_read_uio -> zfs_read` stacks | Later host observation supports investigation, not causality at earlier client timeouts. Git was another agent's `git diff --name-only` in `wt-sol-prismaquant-tessera-15`, **not this client**. PQ worktree was local ZFS `storage_pool/rob/campaign`, not an NFS client mount |
| Healthy storage disproves contention | 80/80 mount-collector `ok` small operations under `storage_pool/shared`; healthy `zpool status -x`; single cumulative `zpool iostat` | Different dataset/operation; pool health is not latency; cumulative sample is not current throughput. None excludes bulk campaign-read stalls |
| Owned Git lifecycle failure | Issue body reports an interrupted own add left an own index.lock, no active own Git verified before historical lock removal | Not an attributable leaked child census. Current `_git_run` uses ordinary `subprocess.run`; no process-group/descendant or hard-return guarantee for uninterruptible I/O follows. Later probe-child ownership improvements concern another population and prove no #1409 leak |
| Caller-budget exhaustion | Historical refusals name 30s identity / 120s snapshot caps; sampler worker budget 300s | Shows historical local cap expiry, not why Git stalled, nor exhaustion of today's caller preparation budget. No original absolute deadline, monotonic timing chain or remaining-budget trace recovered |
| Unresolved | Repeated pre-admission failures plus a later supported retry success | Availability symptom established by attributed incident reports; no remaining reproducible code defect or physical causal repair established |

No contemporaneous owned-client PID/start identity, Git lifecycle trace,
per-operation timings or storage deltas were recovered. #1398 retains mount-probe /
storage investigation; #1410 retains campaign-dataset reads. The existing census
is a shared diagnostic reference only. #2119 snapshot generations and #2121
checkout retirement do not authorize redesign here. #1635's named test-only
exception does not authorize #1409 production changes or publication.

## Exact retained-record access and validation disposition (O3)

Bounded read-only recovery results in this execution context:

| Original reference | Observed access |
| --- | --- |
| `/home/rob/tmp/pb-maintenance-due-20261001-0925/sdk3-dl380-client-failure.json` | `read_file`: `file_not_found` |
| `/home/rob/tmp/claude-campaign-20260926/tmp/row-startup/snapshot-2ce208ee-prepublication-inspected.json` | `read_file`: `file_not_found` |
| `/home/rob/prismaquant/.pi/tasks/01a0eea7-71c9-7535-aba7-e27672794f17-3766960/b8825086a.output` | `read_file`: `permission_denied`; not retried |

The maintenance comment also names `action-sdk3-red.json`, `sdk3-red-audit.json`,
immutable log copies, `dl380-current-evidence.json`, `dl380-storage.txt`,
`dl380-git-blocked.txt`, `dl380-io-processes.txt` in that maintenance directory.
Their bytes/hashes were not recovered here. These names remain exact leads, not
independently verified evidence. The platform-served evidence roots were listed
once: `incident-evidence` exposed only three `nfs16-*` directories;
`investigations` only `1513-35e2bcfd`; `evidence` only
`kernels-931-saved-compile-load-refusal-20261006`. No #1409 mapping was exposed
there. This is not a claim that the records are lost or that no authorized reader
exists. `pbwait` is not used as a passive fallback: it may resume/file records.

**Precise missing prerequisites:** a reviewer-readable route to the original
client/inspection records and their PB generation linkage; verified availability
of the exact full historical committed PQ tree on the affected submission path;
and an existing authorized original-client execution context. The served PQ
latest-main checkout alone supplies neither that tree/path proof nor that context.
The supplied two-arm actions authorize unit/regression execution, not original
DL380 client authentication or recursive submissions. No reproduction was
launched, no toy checkout substituted, no retries requested, and no deployment,
permissions, service, process, locks, admission or source scope was changed.
Access errors alone do not establish a necessary credential or host privilege.

Recovery remains **unverified**, not failed acceptance of a newly proposed fix.
A future authorized validation must first resolve these prerequisites, bind exact
full source and runtime bytes to the client, declare bounded aggregate resources
and native threads through the supported protocol, and retain client timing,
owned-process identity and prepublication/terminal outcomes. No fresh run can
retroactively authenticate the original incident. Leave issue acceptance and
publication to TrustLadder and the existing incident owners.

## Verification

Documentation-only change; no acceptance test was fabricated. The existing Git
regressions and mandatory repository guards are regression evidence only, never
physical-storage diagnosis or full-source DL380 recovery. Both retained stdout
artifacts were independently read: x86/Python 3.14 **219 passed** (113.99s),
ARM/Python 3.12 **219 passed** (38.23s); both exit 0, empty stderr.

Command on each arm:

```sh
python -m pytest tests/test_submission_git_deadline_1888.py tests/test_git_runners_1307.py tests/test_snapshot_git_refusal_1430.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

x86:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"3d27070cc20766e741ed2b7f9e90a97d9246956845e3d0a514682332e0f5666b","size":2521}},"operation":"517962022304a801b2963b3d2b2ddf393c32755ecda20769110172d29c9a472b","receipt":"49b4b76b96c836db0640a28d5d1b9c7792db2447f2d61b313d97026c06b8386a"}]

ARM:
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"9c2607b380e4fe10195f21dcd6ebc55663d8156e2acb270c7deba8ef84b7ba1a","size":2524}},"operation":"aa86023679c019e3a748b11ea31b4d7c7de02b98162068d3b73c4524a9e697fd","receipt":"aef8b5f7bfb8aef8d22a6e7413c035125629819b03c388d05a8767152387bb80"}]
