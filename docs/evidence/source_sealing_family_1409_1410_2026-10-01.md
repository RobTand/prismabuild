# Source-sealing refusal family census — PB1430; Refs PB1409 / PB1410

Owner: native Sol row-startup writer. **Bounded checked-snapshot family census complete; parent incident acceptance remains open.** This is a source/diagnostic inventory, not physical-storage recovery, deployment, campaign completion or performance proof.

## Population and method

At the implementation tree, inspect `git ls-files -z -- '*.py'`, parse every tracked Python file with `ast.parse`, record failures rather than silently dropping them, then select calls named `_git`, `_git_run`, `_snapshot_git`, `git_checkout_identity`, `write_deterministic_bundle`, and subprocess calls containing a literal `git`. The full fresh scan parsed **972/972 files, zero parse/read/decode failures**, selecting **55 production calls and 349 test calls**. These are syntactic leads, not 55 equivalent defects. The previous 971-file/55-production/265-test handover count did not account for parser failures and is not reused as a complete census.

A second literal-Git pass plus owner-body inspection covers commands passed through variables (notably packing, materialization, merge-queue mirrors and maintenance/smoke wrappers). Machine inventory: `/home/rob/tmp/claude-campaign-20260926/tmp/row-startup/native-1430-census-final.json`. Scope is tracked Python and the named source-seal owners/callers; arbitrary dynamically constructed shell/external commands are not proven exhaustively enumerated. No broad shared-storage scan was performed.

## Equivalent checked snapshot family — implemented

All **23** production `_snapshot_git` call sites in `tools/fleet/pbrun.py` now delegate to `core._git` with one caller `_snapshot_fail` factory. Every operation below retains its actual argv/options; the diagnostic includes the entire supplied argv, including `-c` and `--git-dir` selectors.

| Owner body | Concrete operations / call count | Preserved contract |
| --- | --- | --- |
| `snapshot_path_roster` | `ls-files -co --exclude-standard -z` (1) | Personal-exclude pin; raw NUL output, deduplicated paths |
| `_seed_index_roster` | `ls-files -z`, force `add --pathspec-from-file=- --pathspec-file-nul` (2) | Literal pathspecs, private index/env, exact NUL stdin, missing paths omitted |
| `require_untransformed_checkout` | `config --get core.autocrlf`, `check-attr -z -a --stdin` (2) | Codes 0/1 for config; raw attribute bytes decoded with surrogateescape |
| `require_supported_snapshot_tree` | `ls-tree -r -l -z`, `cat-file blob` (2) | Raw tree/link output; type, size, gitlink and composed-link containment refusals unchanged |
| `require_materialized_checkout` | `ls-files -t -z` (1) | Sparse/skip-worktree refusal before bytes are sealed |
| `require_complete_history` | `rev-parse --is-shallow-repository`, `config --get extensions.partialclone` (2) | Existing shallow/partial refusal; config codes 0/1 |
| `resolve_snapshot_refs` | `check-ref-format --branch`, fully qualified `rev-parse --verify --quiet` (2) | Codes 0/1/128 and 0/1 respectively; no rewrite/missing-branch acceptance |
| `write_deterministic_bundle` format probe | `--git-dir=... rev-parse --show-object-format` (1) | Existing v2/v3 header selection |
| `build_git_checkout_snapshot` | object path `rev-parse`, `read-tree`, pinned `add -A`, `hash-object`, `update-index`, `write-tree`, `commit-tree`, bare `init`, snapshot `update-ref`, optional branch `update-ref` (10) | Overlay stamp, private index/object store, fixed commit metadata, source parent/ref ancestry, source identity before/after, size ceiling and CAS input bytes |

The complete owner bodies were read, including roster/index, transforms, symlink resolution, history/refs, deterministic pack writer and source/CAS sealing. Text operations preserve `SystemExit`, `pbrun: cannot snapshot checkout:`, **120 seconds**, all accepted return codes, `surrogateescape`, stdin/environment and raw versus stripped stdout. `core._git` owns nonzero/transport mapping; no new execution framework or timeout policy is introduced.

## Distinct binary-streaming population — implemented separately

`write_deterministic_bundle` has one `subprocess.run` pack writer fed by `deterministic_bundle_argv`. It is **not** a text-runner candidate: `stdout` is the open bundle file, stdin is encoded tip IDs, stderr is bytes decoded UTF-8 with replacement, and the separate bound is **1800 seconds** (`BUNDLE_PACK_TIMEOUT_S`). Header flushing, ref advertisement order, configuration pins, pack options and pack bytes remain unchanged. Nonzero/OSError/timeout refusals now use the same caller fail factory and name the complete pinned pack command. No text capture of pack output.

Focused causal controls cover nonzero stderr including invalid UTF-8, empty-stderr code fallback, OSError, timeout, SHA-1/SHA-256 headers and binary success output. The unchanged determinism suite also verifies actual repeated seals, loose versus packed storage and parent/ref bundle transport.

## Remaining sites — read and deliberately retained

| Owner / caller | Classification and reason not migrated by PB1430 |
| --- | --- |
| `core.git_checkout_identity::_identity_git` | Already calls `_git` with its own ActionContractError fail factory, **30s**, exclude/config isolation and surrogateescape. Plain-directory no-git is deliberately distinct from repository read failure. No behavior change. |
| `core._materialized_git`; `_verify_pbrun_checkout_snapshot` / `_verify_pbrun_checkout_identity` | Already checked with worker-specific failure factory; proof includes clean snapshot commit, parent/ref ancestry and closure stamp. No identity change. |
| `pbrun._git_identity`, `fleet_submit.seal_checkout_snapshot` | Identity callers map the canonical owner refusal; fleet_submit imports the same snapshot builder and translates SystemExit to SubmitRefused. Its existing per-store/identity cache stays unchanged. |
| `pbrun.git_repository_root` | Raw CompletedProcess probe, **30s**, intentionally returns None for unavailable/non-repository root; checked snapshot operations begin only after root selection. |
| `pbrun.keep_droppings_out_of_git` | Raw **30s** probe with recognized-checkout versus plain-directory handling and its own excludes publication refusal. Not an operationless checked snapshot peer. |
| `materialize._run_materializer_git` (five direct materialization calls) | **120s**, raw-return primitive mapped with caller-provided `where` and MaterializationError; no surrogate/raw-output parity with snapshot text wrapper. Not changed. |
| `seal_and_publish._git` / `ensure_snapshottable_checkout` | CompletedProcess, **30s**, fixed commit environment; probe/init/add/commit return codes are interpreted by caller. Existing timeout already names operation. |
| `publish_runtime._git_result` | Raw CompletedProcess, deliberately unbounded timeout; publication reads its own return codes/identity/dirty state. No broader timeout repair. |
| `shape_gate._git` | Checked shape-receipt reader, **300s**, ShapeGateFailure category and own first-two-argument context. Different contract. |
| `pbsnapshot._git` | `check_output` bytes + DEVNULL + CalledProcessError, **10s**; verifies subjects/stamp bytes, never text-runner consolidation. |
| `admission_shared_io.git_head`, `pbcanary.default_checkout` | Raw metadata/probe callers; guarded import fallback and per-caller handling. Not new production snapshot peers. |
| `pbmergeq.Mirror.git` | Queue-owned bare mirror/throwaway-worktree commands through generic `call_tool`; distinct checked/raw result modes, not checkout snapshot writer. No queue activation. |
| `diag_811_e1_checkout_cache.Git.run`, `repo_state` | Diagnostic wrapper delegates to materializer, records spans; parity state probe has **60s** and E1Error. Does not diagnose PB1410's physical cause here. |
| `integration/pb725/run_activation.git` | Admitted opt-in activation source/archive bytes reader via check_output. Not changed or activated. |
| `fleet/slurm/smoke/{rows,multinode/rows_multinode}.build_source_repo` | Disposable smoke-fixture Git setup via generic shell/container wrappers, not production seal runner. No smoke execution. |
| Literal `git` in `pbtest_pins`, `require_pool` | VCS/provenance comparison and command whitelist data, not execution sites. |

There is no remaining equivalent checked text snapshot call outside `_snapshot_git` in the inspected population. The binary writer is intentionally kept separate. Retained distinct contracts above are not silently marked fixed.

## RED provenance and final gate

- Baseline historical head `1bbc502d11066227dc359c27a1f1abcd9a74d920`, action `2a8ed6e9dcfcd2ae0af3ab6f69a3772d3bbcf2e8e0056df2cda815ffc99210f8`: 19 passed/no skips; historical receipt/source proof remains valid only for that population/head.
- Historical context RED at `f30c49ff0c63a822dba63e9dd2ac601f964fd019`, action `135a313fabfd35f00b202acdb9e9eb80a62003d3ec8be4d3fc9a9563af546ccb`: **67 ran, 44 intended failures, 23 passed, no skips**. Independent authentication now completed: bundle SHA-256 `0a4cf73a8dfba6eaf084c0c6de1b2707f8fbacbff3aebc6c3e607b9dbd964e9a`, 11869095 bytes; synthetic `c444cab0e3901e7c9bc0db8cbec457d3bbfaf323` has exact f30 parent, only generated closure addition, no source/test drift. No successful receipt for a failed action. **Not rerun.**
- Additional missing-verb/streaming RED at `8686ffcc791698a61108057a04d41c4c9c5b4153`, action `10f0d9459e36a552f0987d7043a9aa031d268f1d8d83ed3b1de3d82805efc233`: **24 ran, 22 intended failures, 2 passed, no skips**. Bundle `559f0244ba4815f885e884cb6a040795f9dcf9c785fb2f51dadf1e20fb3e333c`, 11871568 bytes; synthetic `4a52c11de40409df207981fc0aa12af3ca9f0ac4`, exact 8686 parent, only closure addition. Focused module tests config/ls-tree/hash-object/update-index/init/check-ref-format and git-dir update-ref plus streaming writer; original 67-case RED not duplicated.
- Focused RED scope deviation: published pbtest defaulted worker TMPDIR to `/home/rob/tmp`, not campaign TMPDIR despite coordinator variable. No `/tmp` use. Supervisor explicitly authorized preserving/reusing that real RED and routing the final combined pytest/compile proof through published pbrun with explicit action-owned campaign scratch. It is not retroactively labelled compliant.
- Final exact-head combined Git-runner/identity/snapshot/determinism/ratchet/compile result and command/source/receipt mapping belong to the external native report `/home/rob/tmp/claude-campaign-20260926/pi/native-row-startup/PB1430-REPORT.md`, avoiding a new source head merely to insert its own result. Independent exact-head review and integration belong to Astra. No worker-attestation verification claim.

## Explicit parent and campaign holds

**Refs #1409 / #1410 only:** operation context improves a refusal; it does not repair historical force-add120s stalls or tracked-file ZFS reads. Physical storage ownership/diagnosis, causal recovery evidence and original real profiled startup A/per-lever/PB1350 deployed workload remain held. No stat shortcut, retry/deadline increase, partial snapshot, source fallback, immutable pin, serving/numerical or resource-policy change. No GPU/profile/timing window, merger/helper activation, reference/worktree deletion, main/live configuration edit or campaign completion claim.

The staged-read contract and ledger were read. This diagnostic-only source delta changes none of their lifecycle/residency/dev-identity requirement axes; targets remain targets and existing implementation/deployment/workload gaps remain unchanged. In particular ID-01/ID-03/ID-08/ID-09 and SAFE-03 evidence is scoped to exact source/action/receipt inspection, not whole-requirement acceptance; LIVE-01, ACC-06 and ACC-07 are not satisfied by these tests. No ledger promotion or staged-only waiver is requested.
