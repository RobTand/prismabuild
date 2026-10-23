# #2066 O4 — retained open states: evidence and limits

Issue: [#2066](https://github.com/RobTand/prismabuild/issues/2066). This is a
read-only research report, not a remediation, benchmark, deployment, or claim
that O4 has been experimentally resolved.

## Adoption provenance and current documentary review

This delivery adopts the diagnostic and report from retained candidate
`3a3fda496055ce8ebba3d9ecd1b935cdb2fb520d` onto inspected checkout
`feb2614c2237e648dd8a13308e7e4225eae79096`. The sampler and usage guide
had no intervening differences from that candidate's base; no waiter change
or application workaround is included. The supplied research-owned acceptance
file now constructs snapshots before monkeypatching; it is not edited here.
#2135's earlier acceptance-file dependency does not require another feature.

**Historical boundary:** all runtime responses, identity checks, access attempts
and synthetic results below, except the explicitly labelled “Initial delivery
verification” and “O3 repair verification” sections, are attributed history
retained from the candidate, not checks newly performed by this delivery.
In those sections, “fresh”, “current”,
“this session” and “follow-up” refer only to the earlier pass being described.
The earlier recursion finding is superseded by the supplied fixture repair;
it is not a current impossibility claim. Retaining a transcript or evidence
reference is not independent verification of its raw bytes.

Current O4 review is documentary. The inventory below still lacks exact
retained-file identities/directories, matching client/server/mount/session
populations and executing collector/waiter/bell identities. No new authorized
production capture interface or matched-load raw input is supplied. The exact
historical directory denial is preserved, not retried without a changed
basis. There are no new production counts, fd/maps observations, RPC windows,
state-growth measurements or bell comparisons in this delivery. Diagnosis and
mitigation remain **unverified**; the inventory and prerequisites, not a
negative census or synthetic qualification, are the O4 deliverable.

## Historical identities: inspection versus attributed populations

This follow-up's coordinator identity check at `2026-10-23T12:15:11Z`
returned hostname `dl380g10`, UID/GID `1000(rob)`, with no identity change.
The worktree HEAD remains `592a5482e2f52cb21be3871bea57e10edb3fa317`.
These identify the reader and source, **not** either NFS client's state owners.
The attributed populations remain sparky (6,358) and sparklina (6,128), with
Rob's **no corresponding sparky file descriptor** qualification. Their count
window, mount/export, NFS client/session, open-owner/filehandle/stateid and
contemporaneous descriptor-visibility coverage remain unavailable.

A fresh bounded published `pb_runtime` response at
`generated_unix=1792757722.619694` identified generation
`e9e9d9077212-1792715962-50f991688c86` and commit
`e9e9d907721282f517e4e8feca40eda1aec79e5d`. Runtime identity is a separate
population from retained NFS state; details and discovery limits follow below.

## Result

The published read-only `pbmcp.pb_runtime` interface was accessible and resolved
the runtime generation and reported loop commits. Retained mount records were
not accessible at the documented Netdata record directory. Consequently **live
retained-open-state counts, live RPC rates, and a matched before/after comparison
are unavailable** in this investigation. Runtime identity is not NFS open-state
identity, and a successful runtime response does not fill that gap.

The figures **6,358 on sparky and 6,128 on sparklina** are attributed background
from the task, not measurements made here. Rob's qualification — **no
corresponding sparky file descriptor** — must travel with those figures. They
do not establish 6,358 live application descriptors, leaked Python handles,
current state, or a cause of latency. No timestamped raw count source or
identity mapping for those figures was supplied or recovered here.

## Scope and inspected evidence

Source inspection used worktree revision
`592a5482e2f52cb21be3871bea57e10edb3fa317`. On the completion pass, Git status
already contained a modified `tools/fleet/mount_latency.py` and this untracked
report. Both were preserved; the collector and tests were not edited. The
runtime-response transcript below was inherited from the dirty report; the
prior completion pass did not re-execute or independently verify it. Its
historical observations must not be read as the fresh follow-up response,
which is separately identified under bounded interface discovery.
Inspected guidance and interfaces:

- [`../../AGENTS.md`](../../AGENTS.md),
  [`../agent_execution_policy.md`](../agent_execution_policy.md), and
  [`../operating_prismabuild.md`](../operating_prismabuild.md), including the
  published-runtime/read-only MCP guidance.
- [`../mount_measurement.md`](../mount_measurement.md): per-client attribution,
  per-operation queue/RTT interpretation, retained JSONL location and access
  limitations, and the explicit prohibition on global lock-table diagnostics.
- [`../../tools/fleet/pbmcp.py`](../../tools/fleet/pbmcp.py): `pb_runtime`,
  deadline/completeness reporting, and configured default root selection.
- [`../../tools/maintenance/event_wait_baseline.py`](../../tools/maintenance/event_wait_baseline.py):
  `collect`, `analyze`, `compare`, and the separate optional `trace` interface.
  These were inspected as source, not run.
- The existing published manifest at
  `/mnt/shared/prismabuild-fleet/repo/RUNTIME_VERSION.json`: the prior report
  recorded a first-45-line read; this completion pass read the first 32 lines
  and confirmed the same commit/generation fields, not current loop liveness.
- The dirty collector diff and CLI: passive sampling skips the active probe
  and lock inspection, but its CLI still appends a local record. See the
  [passive usage boundary](../mount_measurement.md#passive-observation-and-o4-evidence).

The completion pass listed the published repository directory once and retried
the documented Netdata directory once, reproducing the access failure below.
These repeat the prior report's recorded inspections. Reads of guidance/source
were paged and bounded. No queue tree census, descriptor census, remote host access, or
alternative permission workaround was attempted. Paths below describe inspected
installation evidence, not new application defaults or portable deployment
requirements.

### Runtime identity: response retained from the prior report

The inherited report records the following completed invocation and selected
response fields. This completion pass did not recover a separate raw response
artifact; the transcript is preserved evidence with that provenance limitation.
The bounded manifest read corroborates its published identity only.

A single read-only MCP session invoked `pb_runtime` through the existing
published entrypoint, with a five-second read deadline and a 15-second shell
bound. Bytecode writes were disabled. No action was submitted. Invocation:

```sh
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"2066-read-only-research","version":"1"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"pb_runtime","arguments":{}}}' \
  | PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 \
    /mnt/shared/prismabuild-fleet/repo/tools/fleet/pbmcp.py --deadline-s 5
```

The command completed with exit code 0. Selected response fields, retained here
as observations rather than a new measurement series:

| Field | Observed value |
| --- | --- |
| `generated_unix` | `1792753680.436287` |
| `complete` | `true` |
| `timed_out`, `unavailable`, `abandoned_readers` | all `[]` |
| `generation` / `server_generation` | `e9e9d9077212-1792715962-50f991688c86` |
| `generation_stale` | `false` |
| `started_from_generation` | `true` |
| manifest commit | `e9e9d907721282f517e4e8feca40eda1aec79e5d` |
| manifest `dirty` | `false` |
| manifest publisher / publication time | `sparky` / `1792715967.2358475` |
| `server_entrypoint` | `/mnt/shared/prismabuild-fleet/runtime-generations/e9e9d9077212-1792715962-50f991688c86/tools/fleet/pbmcp.py` |
| rollout | `idle`; no failed or outstanding hosts |

Loop summary at that response time:

| Node | Reported state | Loops | Age (seconds) | Reported runtime commit |
| --- | --- | ---: | ---: | --- |
| sparky | live | 9 | 0.7856616973876953 | `e9e9d907721282f517e4e8feca40eda1aec79e5d` |
| sparklina | live | 8 | 0.7926020622253418 | `e9e9d907721282f517e4e8feca40eda1aec79e5d` |
| dl380g10 | live | 21 | 1.3013062477111816 | `e9e9d907721282f517e4e8feca40eda1aec79e5d` |
| DESKTOP-P5UOGNJ | retired | 3 | 3299919.2675368786 | `a80eea97fe8f9ced1f3f2776188ee1f51a029c11` |

These are heartbeat/runtime summaries, not PID/start-time, filehandle/stateid,
NFS client/session, server open-owner, or mount identity resolutions. In
particular, loop counts cannot be compared numerically to the attributed open
state counts as if they measured the same population.

### Retained measurement access: exact failure

The file-tool request `ls(path="/var/lib/netdata/prismabuild")` returned:

```text
Error: Path '/var/lib/netdata/prismabuild': permission_denied
```

No contents or host-specific JSONL filenames were obtained. This is an access
failure, **not** evidence that recording is absent, the records are empty, or
the mount is healthy. The guide documents that directory as the Netdata-mode
default, allows an existing `--record-dir` override, and warns that successful
recording does not imply another UID can read it. This investigation did not
verify an override or collector configuration, so it does not assert this is
the effective record destination on both clients.

Prerequisite: an operator-provided, already authorized read-only view of the
actual retained per-client records and collector configuration (or an approved
export of those records), with sample timestamps and recording status. Any
access adoption belongs to an independently authorized operator; this task
made no ACL/group/mode change and does not authorize one. No SSH or alternate
UID was used to retry the failure. A current chart label, even if separately
available, would not substitute for a retained matched time series.

### Fresh bounded interface discovery (follow-up)

The prior transcript above remains historical evidence. This follow-up made a
new published MCP stdio session: `initialize`, `tools/list`, then `pb_runtime`,
using `/usr/bin/python3`, `PYTHONDONTWRITEBYTECODE=1`, the published
`tools/fleet/pbmcp.py`, `--deadline-s 5` and an outer `timeout 15s`. All three
requests returned JSON-RPC results and the shell exited 0. No action was
submitted and no collector was launched.

| Read-only interface/read | Fresh observed result | Limit |
| --- | --- | --- |
| `tools/list` | Advertised `pb_status`, `pb_denials`, `pb_tier`, `pb_movers`, `pb_action`, `pb_actions`, `pb_receipts`, `pb_verify_claim`, `pb_log`, `pb_runtime`, `pb_starvation`, `pb_cursors`, `pb_blocked_origins`. None advertises an O4 retained-state, descriptor-census or mount-rate-history interface. | This inventories this server's tools, not all possible evidence services. Queue tools were not called as a substitute for an O4 census. |
| `pb_runtime`, `generated_unix=1792757722.619694` | `complete: true`, `timed_out: []`, `unavailable: []`, `generation_stale: false`, `started_from_generation: true`; manifest `dirty: false`; rollout `idle`. Generation/commit match the identity section above. Live announcements: dl380g10 (25 loops), sparklina (5), sparky (7), all on that generation; DESKTOP-P5UOGNJ retired. | Runtime announcements, not verification of each Netdata collector, its effective options or NFS state. These are not historical rollout evidence for the attributed count window. |
| Published `tools/fleet/mount_latency.py`, literal search for `passive` | No matches found. | Source through the published repo link only, not each installed plugin or running command. The dirty source capability cannot be assumed deployed. |
| `/etc/netdata` and `/etc/netdata/custom-plugins.d` listings; `/etc/netdata/netdata.conf` read | Custom plugin directory listed no files. The 12-line config contains only comments, including example localhost retrieval commands. | Does not enumerate effective configuration, other plugin locations, daemon defaults or remote clients. Examples are not a configured historical endpoint. No HTTP request was made. |
| `/var/lib/netdata/prismabuild` listing | Again returned `Error: Path '/var/lib/netdata/prismabuild': permission_denied`. | Not an absent-directory or absent-log result; no records or child filenames obtained. |

At follow-up entry both documentation files and `tools/fleet/mount_latency.py`
were already dirty (the report untracked). The collector was not edited or
executed; its observed SHA-256 was
`ad07fce9988e4fa2c07630c8a8f2b8dbae771f10adce67866daee13a34f48e41`.
Source inspection confirms that `main()` still calls `append_record` even with
`--passive --once --json`; passive is not a write-free discovery command.
No tests were run for this documentation-only follow-up.

**Unavailable inventory, not a negative census:** no accessible O4 records,
per-client effective collector configuration, approved alternate record
location, retained-state/FD export, authorized historical metric endpoint or
matched time window was obtained. No SSH, lock reads, active NFS probe,
remount, permission change or collection/deployment change was attempted.
The supplied interface's successful runtime response does not fill these gaps.
An operator must provide an explicit authorized read interface/record export,
host/window/schema provenance and existing read permission before a comparison
or state attribution is possible. No zero rates or improvement follow.

### Exact interface gaps and prerequisites

| Evidence/interface | Availability and prerequisite |
| --- | --- |
| Published `pbmcp.pb_runtime` | Documented read-only runtime/heartbeat interface; inherited response plus the fresh bounded response above. Advertised tools were also enumerated. Runtime metadata exposes no retained-open-state census and does not repair the missing O4 evidence. |
| Per-client retained mount JSONL | Documented default directory read denied on this coordinator; actual client destinations/overrides unverified. Need an already authorized export/view of each client's effective collector configuration and timestamped records, including recording failures and coverage gaps. |
| Retained NFS open-state count and no-FD attribution | No configured authorized interface or raw timestamped export was supplied for either count or the contemporaneous no-FD observation. Need definition, source, identity/visibility coverage and same-window client/session/export provenance. This is not a claim that the OS has no diagnostic facility. No descriptor census, lock table or remote inspection is authorized here. |
| Passive collector (`--passive`) | Present in dirty source only; deployed support was not verified. Reads mountstats, not retained-open-state inventory. Its CLI still writes local JSONL, so it was **not run** under this read-only assignment. Need separately authorized collection and retained output, not a production probe. |
| Netdata chart API | Documented current diagnostics only. No configured authorized endpoint was supplied for these clients; no HTTP request was made or endpoint guessed. Even an available current label would not recover historical counts or a matched comparison. |
| `event_wait_baseline collect/analyze/compare` | Source interfaces inspected, not executed. Missing local-server capture authority, current role identities and matched raw inputs; neither this tool nor runtime metadata measures NFS retained states. |
| Issue bodies/upstream correction | No issue-service interface is available in this session. Issue references are ownership boundaries, not verified acceptance status or proof of an applicable external fix. |

These are distinct gaps: a denied documented path, an unverified deployment,
and an unsupplied interface/input must not all be described as an empty result.
No SSH, alternate identity, ACL change, service restart, lock access or
production `done/` probe is a permitted fallback.

## O4 interpretation: facts versus hypotheses

| Proposition | Classification / limitation |
| --- | --- |
| A deployed generation and three live loop summaries were resolved | Observed through the completed `pb_runtime` response above. |
| 6,358 sparky / 6,128 sparklina retained states | Attributed counts only; population, source, epoch and sampling interval unverified here. |
| No corresponding sparky FD | Rob's attributed observation; not independently re-enumerated here. Preserve it rather than replacing it with an FD-leak narrative. |
| Application code still holds all those opens | Unproven; contradicted as a simple one-state/one-visible-FD explanation by the attributed qualification. |
| NFS client/server retained state outlives a userspace descriptor | Candidate explanation to distinguish from active application handles; not measured here. |
| Such retention drives recovery traffic or queue delay | Hypothesis requiring same-client, time-aligned state and RPC evidence. |
| A particular kernel bug caused this incident | Not established. Neither runtime identity nor historical incident documentation identifies the cause of these counts. |

The historical `TEST_STATEID` storm in `mount_measurement.md` is useful evidence
about how to read per-operation RPC rates and queue/RTT splits, not a live rate
for this investigation. Do not reuse its 4,784 calls/s or 97.7% share as current
values. `dl380g10` is the exporter/local-ZFS case in that guide; its absent NFS
client RPC leg must be represented as unavailable/not applicable, not zero.

## Candidate diagnostics versus production names

A future diagnostic claim filename must be explicitly diagnostic and unique,
for example `o4-diagnostic-claim-<capture-id>.json` in a separately approved
scratch namespace. This is a proposed naming convention only: no such file
was created, discovered, opened on the shared mount, or used as a probe here.
A diagnostic result would describe that diagnostic path and workload only.

Production completion lookup is `done/<action-key>.json` (often written in
issue discussion as `done/action-key.json`), tied to the real action key and
publication/attempt lineage. It is not a diagnostic claim filename. A renamed
scratch file cannot stand in for the production namespace's cache behavior,
open/close lifetime, completion visibility, or retained-state attribution.
Conversely, production `done` records must not be renamed, touched, repeatedly
opened, or otherwise exercised to manufacture evidence under this assignment.

The inspected baseline interface reinforces this identity distinction: its
analysis groups by `(action_key, published_unix)` and attempt, excludes backlog
from fresh-publication latency, and reports censorship/conflicting claims. A
filename count alone supplies none of that accounting.

## Comparison interface and prerequisites (not executed)

`event_wait_baseline collect` requires an explicit queue on its filesystem
server, current `NAME=PID` role identities, a cold-start/steady label, load
note, source revision and an output outside the queue. It supports bounded
entries, keys, records and bytes; tree sizes are externally supplied rather
than scanned. It collects proc accounting and queue/tier evidence, not a
retained-NFS-open-state census. No current role PID/start-time set, authorized
local-server capture, or matching pair of retained raw baseline inputs was
supplied here, so neither `collect`, `analyze` nor `compare` was executed.

`compare` requires identical recorded/replayed experimental input, non-summary
tier events, tier-cycle accounting and no capture errors/limits. It normalizes
only top-level `unix`; unrelated live windows cannot establish equivalence.
The separate `trace` subcommand attaches `strace` and is expressly outside this
task. No trace was run. No speedup, exact event parity, publication-to-claim
percentile, CPU/I/O delta, or rate reduction is claimed.

A decision-quality follow-up needs existing authorized evidence that binds:

1. Each count to a timestamp/window, host, mount/export, client/session and the
   precise definition of retained state; resolve any available filehandle/
   stateid/open-owner provenance without unsafe lock-table enumeration.
2. The sparky no-corresponding-FD observation to its contemporaneous identity
   and coverage limits, without treating missing visibility as an empty census.
3. Same-client retained mount samples to per-operation RPC deltas, queue time,
   RTT, recording/probe status and workload; keep clients separate.
4. Production action/publication/attempt and deployed generation to the same
   window, without substituting diagnostic filenames for `done/<action-key>.json`.
5. Any comparison to matched workload/cache state and complete retained inputs.

These are prerequisites, not instructions to launch additional measurement in
this work package. Until supplied, live rates and comparative conclusions
remain unavailable, not zero and not a passing result.

## Ownership and conditional external handoff

This report is limited to O4 research for #2066. It neither absorbs nor closes
[#1894](https://github.com/RobTand/prismabuild/issues/1894) or
[#2103](https://github.com/RobTand/prismabuild/issues/2103),
[#2104](https://github.com/RobTand/prismabuild/issues/2104),
[#2105](https://github.com/RobTand/prismabuild/issues/2105),
[#2106](https://github.com/RobTand/prismabuild/issues/2106), and
[#2107](https://github.com/RobTand/prismabuild/issues/2107). Existing ownership
and acceptance boundaries remain with those issues. Their full issue bodies
were not available through an issue interface in this session; no detailed
allocation or completion status is inferred from their numbers.

If subsequently authorized evidence isolates a problem outside PrismaBuild
and identifies an applicable external correction, hand the evidence and exact
prerequisites to [#1917](https://github.com/RobTand/prismabuild/issues/1917):
client/server implementation and version, mount/session identity, timestamped
state/RPC evidence, workload and deployed runtime, missing visibility, proposed
upstream/vendor correction and an operator-owned validation/rollback plan.
That is a **conditional external-fix handoff**, not a finding of kernel
causation and not authority to upgrade, remount, restart, or patch production.

## Earlier documentary follow-up boundary

No tests were run during that earlier documentation-only follow-up. No probes, SSH, `/proc/locks`, fdinfo lock
reads, remounts, permission changes, traces, runtime publication or production
mutation were performed. The completion pass updated this pre-existing dirty
report and `docs/mount_measurement.md` only; it preserved the dirty collector
without running it. Review was documentary: source/interface inspection,
bounded published-manifest read, inherited runtime-response transcript,
fresh bounded `tools/list`/`pb_runtime` discovery, limited local Netdata
configuration inspection, reproduced access failure, and documentation diff
inspection. No test result is claimed here; no tests are needed for this
documentation-only follow-up. The report does not satisfy any live-measurement or
remediation acceptance gate.

## Diagnostic implementation completion

The implementation completion inherited the dirty collector and both documents
above; it inspected and adopted the collector diff rather than discarding it.
The earlier sessions' observations retain their attribution above. This session
independently called the same published `pb_runtime` entrypoint with
`--deadline-s 5`, bytecode disabled and an outer `timeout 15s`, using the
initialize/initialized/tools-call sequence above (client name
`2066-diagnostic-completion`). It exited 0 and returned
`generated_unix=1792761681.4798255`, `complete=true`, no unavailable/timed-out
readers, and the same clean manifest commit/generation. Announcements reported
live dl380g10 (25 loops, age 0.265 s), sparklina (8, 0.294 s), sparky (5,
0.895 s). These remain announcements, **not executing-process byte checks**.
The documented record-directory listing independently returned exactly
`Error: Path '/var/lib/netdata/prismabuild': permission_denied`. No alternate
identity, endpoint guess or SSH fallback was attempted.

### File populations and unresolved measurements

`timed_probe` in the inspected source context-manages and closes
`claim-<pid>.tmp`, renames it to `claim-<pid>.done`, then unlinks it. This is a
candidate source of the attributed filename population, not demonstrated
causation. Production terminal reads instead address `done/<action-key>.json`
and failed/withdrawn records. Exact directories, filehandles/inodes, client
sessions and open owners for Rob's population remain unresolved. Failed/skipped
CLOSE, client caching and mmap retention are hypotheses; no mmap census was
obtained. A visible descriptor absence alone does not exclude mapped files.

| Required comparison | Newly measured result / exact missing prerequisite |
| --- | --- |
| Matching server `nfsd type:open` counts and state-count growth | Unavailable; need authorized timestamped server export with client/session/export and exact file identity mapping, at both ends of each window. Attributed counts have no measured duration, so no growth rate can be computed. |
| Client fd and maps observations | Unavailable; need an authorized bounded per-process observation/export including PID/start-time, namespaces, permission/coverage limits, maps and descriptor identities at matching timestamps. Never read global locks or fdinfo lock lines. |
| Client/server kernels and mount source/options | Unavailable for the retained-state population; need contemporaneous client/server kernel versions and selected mount/export/options tied to that population. Test-runner kernels below do not resolve incident kernels. |
| Collector, waiter and bell executing identities | Runtime announcements above only; need effective collector command/plugin/configuration and process PID/start-time/loaded-source identity, waiter generation and bell service identity/enablement at the count windows. Source support is not deployment proof. |
| Before/after OPEN/CLOSE/errors/timeouts and activity | Unavailable; need at least two same-population passive samples per window and same-window action/publication/attempt and role/activity evidence. No live mountstats capture was made. |
| Matched-load bell-enabled comparison | Unavailable; need both raw windows with matched workload/cache state, deployed identities and retained-state plus RPC/activity coverage. Neither source inspection nor synthetic tests establishes mitigation success. |

The existing authoritative reads, race-covering re-observation, bell waits,
deadlines, freshness/generation matching and bounded outage fallback in
`action_wait.py` (consumed by `action_client.py` and `pbwait.py`) are unchanged.
#1894 owns polling removal. No tuning or reimplementation of that feature, or
#2103–#2107 coverage, is included.

**Remount-reset experiment: unperformed**, with no separately authorized safe
window. No active production probe, passive production collector, kernel-debug
change or trace was run. Hence there is no trace authorization/duration/cleanup
to report. Any future trace needs an existing authorized interface, finite
stop time and verified cleanup; no such facility is inferred from source.
Evidence does not yet locate the repair in Linux/NFS, so the #1917 external-fix
note above remains conditional: affected client/server versions and a specific
upstream correction are unknown. Do not publish a kernel diagnosis or upgrade
recommendation without the missing discriminating evidence.

## O3 repair verification (synthetic only)

Independent review of `fbba5884f56e016bb4aec7466182b8dbbdb956b6`
found that the initial passing suite below missed an explicit O3 requirement:
malformed NFS operation rows were silently discarded, and two empty operation
maps could yield healthy zero RPCs. Retained stdout from reviewer operations
`a05389d62a0cf84dba28988653f5642e5a4296db1f835cb53a96cd53de905350`
(x86) and
`46b5bd731089db86e772b80ae38a4ead12fbb72a046f4a467a5e8e0684a022a0`
(ARM) was read in this repair: both showed `window_s=10.0`, zero RPC counts
and an empty `by_op` after malformed operation inputs. Those are synthetic
failure observations, not production idle measurements.

The existing parser now rejects a selected NFS snapshot with missing/empty
per-operation data or any malformed row, even if other rows parse correctly.
Selection happens before rejection, so unusable NFS data cannot fall back to
a parent or autofs trigger; invalid data on an unrelated mount does not poison
the selected mount. Empty maps also cannot pass `_same_population`. Rejection
uses the existing unavailable path: unknown transport, `rpc: null`,
`window_s: 0`, explanatory note, no attribution and a cleared baseline. The
next usable snapshot rebaselines; subsequent same-population rates resume.
Valid unchanged counters still report genuine zero activity. No fields,
acceptance files, compatibility goldens or waiter behavior changed.

Before the parser repair, the focused regression command
`python -m pytest tests/test_mount_latency.py -q -k unusable_nfs --basetemp="$TMPDIR/pytest"`
reported **16 failed, 1 passed, 31 deselected** on x86. Its retained failures
cover unavailable RPC evidence/notes and unwanted mount fallback:

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"ed09ffe3218fb32775d7ba0d9211bfeb970977e3b40579c17aae00b6b2aac882","size":48348}},"operation":"e590643fc06b9cf899c06c287da1d2a972159a7fc09f9950ed99432a03a88d8f","receipt":"817bab177f39320af04b4c0b646c274132cc780c0a7862fee3808f3cb8fb5eb3"}]

Final combined validation, with 17 added regression cases in the existing
mount-latency test file, completed on both arms:

```sh
python -m pytest tests/test_mount_latency_retained_states.py tests/test_mount_latency.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

- x86 / Python 3.14.4: **168 passed in 138.11 s**, exit 0.
- ARM / Python 3.12.3: **168 passed in 40.47 s**, exit 0.

Both retained stdout artifacts were read. Regressions exercise active and
passive modes through the binary procfs reader, initial repeated invalid
snapshots, interrupted windows and recovery, missing/empty/short/nonnumeric/
negative or partially malformed rows, plus explicit-text mount selection.
The immutable acceptance file is unchanged. This qualifies the diagnostic
repair, not production state retention or mitigation; all O4 evidence gaps,
safety limits and conditional external handoff above remain unchanged.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"0d998b44b5289db4de374fa26efffbf47ac7a482bc802a07776a35b96896a417","size":2237}},"operation":"80af1c2607b53cb8132ff5f7617522927b511c8b6e5d445e7ec67e50dc538c46","receipt":"dfeb4feafe143569871148aaddc03dd87cacea889df7144e4d2133940a143d0b"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"328210225dd0f8cc1956c7182b509aa716c86dc3fca220be62d47692affc6a65","size":2238}},"operation":"86c6b091da79818cbc4bcfa9f1156a56123e2426aad6b6a14e9d5b0d997649d9","receipt":"eda30dfda828a6fcd7ba833e63643461995f9cce6f63b213e23249c7637b8f8f"}]

## Initial delivery verification (synthetic only)

The combined command below was initially completed on both arms after adopting
the candidate, using the supplied corrected acceptance file unchanged. These
passes did not cover the unusable-operation defect repaired above:

```sh
python -m pytest tests/test_mount_latency_retained_states.py tests/test_mount_latency.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

- x86 / Python 3.14.4: **151 passed in 110.42 s**, exit 0.
- ARM / Python 3.12.3: **151 passed in 42.98 s**, exit 0.

Retained stdout was independently read for both operations. No recursion,
setup or discovery failure occurred. The existing compatibility inventory and
other required guards passed without a golden refresh. These fixture checks
qualify diagnostic behavior only, not production retention or bell efficacy.
No production capture, remount or mitigation comparison was run; the O4 gaps
above remain unresolved.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"939dd05603e8a7b02193605d35fc429e77b720de641ae797a481d8e94a28cacf","size":2236}},"operation":"a03024bee2a46eb1d7f35680d70910eb9c17d9f327fc140c84b9dc486b21bbb7","receipt":"4318773268dcda6fb267e55ca731d488ece9fb352da0cdf000f552c73307b6c9"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"1c22aaf7ff5ab2149be678b1f9c7aad296b3f5060fda2d5754cdb77174ce940c","size":2241}},"operation":"9fffae35236aa3101a32c83c565c461dffc7e90e87f63a082333f22eeff87f1c","receipt":"4c63a562e5afb45ce9ff938477b725282345ce78adfaf80cf2b0f2420e61e720"}]

### Historical synthetic verification (not current qualification)

Both arms ran:

```sh
python -m pytest tests/test_mount_latency_retained_states.py tests/test_mount_latency.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q --basetemp="$TMPDIR/pytest"
```

x86/Python 3.14: **149 passed, 2 failed** (115.89 s).
ARM/Python 3.12: **149 passed, 2 failed** (36.23 s).
Both failures are acceptance-fixture recursion, not failed diagnostic assertions:
`passive_cli_never_runs_probe_or_lock_scan` and
`default_sample_is_explicitly_active` monkeypatch `read_mountstats` with a
function that calls `retained_state_snapshot()`. That helper dynamically calls
`mount_latency.read_mountstats` again, recursing into the patched function
until `RecursionError: maximum recursion depth exceeded`. The stack repeats
fixture lines 25/60 (passive) and 25/92 (active), reached from sampler line 839.
The passive test never reaches its probe/lock assertions, and the active test
never reaches its probe assertion. This makes O1's specified check impossible
as written. Minimal fixture repair: build the synthetic snapshot **before**
patching, then have the fake return it (or retain the original parser for the
helper). The immutable acceptance file was not modified and no application
workaround for the fixture was added. All other acceptance cases, existing
mount-latency cases and required repo guards passed on both arms.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"01657a4fc51ab73daa252bf6c845fdddd98988025823f721caf72d37b2de99fa","size":6736}},"operation":"65deeb3607f7a760976d4e2adcc227f78689cb4b1a5c9a1027e2683d37eef4d0","receipt":"872d8d2fb9c853beb09ecbcbe99813fb04ef11999099fc85d9cdc5c82f224b80"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"6958478937c2d1962b7e75f27268cb91ce9d28d28178bc6c83f4205f018ecac8","size":6751}},"operation":"65e4733a90bcc8d1b17d10e7fd28807cdc39c7803f5050beb2ff12f333152b50","receipt":"7b9f1aee220fb6faa43e1836cd609be351358bc43b8617b425cc2a6526aa5c84"}]
