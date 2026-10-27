# #2062 / W1: bounded source-level broker read baseline

## Reproduce and interpret

From the checkout, in the supplied admitted test environment:

```sh
python -m pytest tests/test_broker_read_baseline.py -q -s --basetemp="$TMPDIR/pytest"
```

The first test prints the JSON report. Its checkout-only callable is
`tools/maintenance/broker_read_baseline.py:run_synthetic(root, populations=(0, 8, 64), cycles=2)`;
`root` must not exist. Direct Python imports outside pytest need `PYTHONPATH=src`.
The second test compares count projections across fresh roots. No CLI or public
application API is added. Full verification (both acceptance tests and four guards):

```sh
python -m pytest tests/test_broker_read_baseline.py tests/test_resource_broker.py tests/test_gpu_memory.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q -s --basetemp="$TMPDIR/pytest"
```

Measurement kind is **synthetic_python_file_reads**: attempts at
`Path.read_text/read_bytes`, successful returns, and length of returned bytes
(text encoded as UTF-8; all fixtures are ASCII). Attempts are recorded before
the read, successes/bytes only after return. These are not read syscalls: buffering,
multiple syscalls, EOF, descriptor operations, stat and directory enumeration are
not counted. Syscall-returned bytes are themselves not physical disk throughput
(proc/cgroup pseudo-files, page cache and device I/O differ). No conversion to
reads/s or MB/s is justified. The reported approximately 17,000 reads/s and
153 MB/s remain unattributed claims; neither original findings nor a live trace
was inspected. This baseline neither verifies nor refutes those deployed rates.

## W3 event-observation comparison

The #2064 regression uses this same callable with three monitor ticks and zero
unrelated PIDs. Before the source change, `memory.current`, `memory.events` and
`memory.events.local` each have three successful Python reads. W3 requires all
three numeric memory reads while each unchanged OOM input has one initial read.
The fixture now also supplies `cgroup.events`, classified as `scope_population`,
and closes each monitor so notification descriptors do not escape the workload.
The baseline still uses the real collector and monitor with fixed time and
stubbed vendor results. Retained W1 counts below describe the historical source.

Controlled acceptance separately exercises dirty scopes, enrollment races,
attempt and inode replacement, lost coverage, overflow recovery, pending stop
retries, cancellation and numeric-deadline fairness. Real inotify tests use
ordinary fixture files. They prove the Linux notification adapter's behavior,
not privileged cgroup-controller behavior. This comparison does not qualify
live cgroups, attribute the original reported rate or establish fleet rollout.

## Fixture and source identity

Recovered the reproducer and this note from implementation commit
`fa3fbf5e47ccefe123aba38f0794f3886db7954d`, rather than recreating them.
The three measured production files have no diff against current main
`7bf539a90402fbab88f58bd834e2abf7ee5e1e34` or that implementation commit.
Fresh verification uses checkout base `1ba19531393bbfab527e8f0ad93c075578caf4e5`
with this measurement-only overlay; the restored acceptance file is unchanged.
Historical test claims are not adopted as evidence. Fresh runs recompute these
source SHA-256 identities outside fixture collection totals:

| Source | SHA-256 |
| --- | --- |
| `tools/fleet/resource_broker.py` | `0f8dfa4bca6633aa8d86aecf81c2d6fe06bba756cf376acb91f7fdeaff54be1c` |
| `src/prismabuild/gpu_memory.py` | `42c2f07bc7520a8d0c7a42e3f4c6e96728e05f13563292a781a1f36a5a06442a` |
| `src/prismabuild/gpu_capacity.py` | `6c61d4543e60e5e42c0a0879cbff6566442ea07969e764afbf563374860c7f28` |

Each scenario has 0, 8 or 64 unrelated PIDs (1000 onward). `empty` has
zero active scopes and no GPU; only its population-zero tree has no PIDs.
`no_gpu`, `gpu_error` and `gpu_present` each have one active scope with PID 42.
There are two direct collector calls, two real `ResourceMonitor.poll_once` calls,
and two real `Authority.admin(maintenance_status)` requests per tree; these are
separate workloads, not summed to model one production cycle. Setup runs once.
Monitor scope identity uses the actual fixture inode. Scope memory is 1 MiB,
budget 1 GiB, host memory 8 GiB with 7 GiB available; PSI and OOM counters are zero.
GPU-present stdout identifies a fixture A100 and one 1 MiB process allocation.

Per-file sizes: unrelated stat 57 B, membership 14 B; PID 42 stat 55 B,
membership 76 B; meminfo 46 B; each PSI file 94 B; memory.current 8 B;
each memory.events variant 17 B. Installed fixtures are tiny ASCII files:
broker 29 B, gpu_memory 33 B, gpu_capacity 35 B. They are not deployed binaries.
Their initial hash reads are explicitly **setup**, not collector reads.
The request workload clears active records to isolate installed health rereads;
it uses a healthy empty backend inventory and bypasses only privileged ownership
validation for the exact user-owned installed fixture allowlist. It does not
qualify installation trust, authority reconciliation or production health.

Collectors, parsers, monitor and admin dispatch are real implementations, not
copied loops. Adapters supply synthetic proc/cgroup roots through existing seams.
Vendor subprocess stdout/outcomes and time are injected. No real commands run;
ROCm discovery fallback is disabled. Backend operations are fixture adapters,
not systemd operations. Path/subprocess/time patches are scoped and restored even
on failure. Run in an isolated test process, not concurrently with other threads:
the temporary Python monkeypatches are process-global.

## Observed counts and scaling

All observed attempts succeeded; the full report contains relative paths and
per-file attempts/successes/bytes. Below, `reads / returned bytes` includes two
iterations except setup. These formulas summarize observed class totals, with
`N` the unrelated-PID population, not a performance requirement:

| File class | Collector (and monitor) empty | Collector (and monitor) no_gpu/error | Collector (and monitor) GPU-present |
| --- | ---: | ---: | ---: |
| proc_identity | 2N / 114N | (2N+2) / (114N+110) | (2N+4) / (114N+220) |
| proc_membership | 2N / 28N | (2N+2) / (28N+152) | (2N+4) / (28N+304) |
| host_memory | 2 / 92 | 2 / 92 | 2 / 92 |
| memory_pressure | 2 / 188 | 2 / 188 | 2 / 188 |
| scope_memory | 0 / 0 | 2 / 16 | 2 / 16 |

Monitor additionally reads cpu_pressure **2 / 188** in every case, and each
of oom_hierarchical and oom_local **2 / 34** when a scope is active.
Setup installed_privileged is **3 / 97** in every tree; request installed_privileged
is **6 / 194**, independent of N, with no child commands. There were no fixture
state-file reads in these clean-state cases. Imports/source hashing are harness
overhead, outside fixture rows; fixture creation is writes, not measured reads.

| Scenario | Collector totals at N=0, 8, 64 (reads / bytes) | Monitor totals at N=0, 8, 64 (reads / bytes) |
| --- | --- | --- |
| empty | 4/280; 36/1416; 260/9368 | 6/468; 38/1604; 262/9556 |
| no_gpu or gpu_error | 10/558; 42/1694; 266/9646 | 16/814; 48/1950; 272/9902 |
| gpu_present | 14/820; 46/1956; 270/9908 | 20/1076; 52/2212; 276/10164 |

Observed slope per unrelated PID is **4 reads and 142 B over two calls**
(2 reads and 71 B per call), for collector and monitor alike. Whole-proc
identity/membership census occurs even with empty GPU output or query failure;
GPU-present adds a second identity/membership check for PID 42 per call.
Monitor's extra OOM/CPU-PSI reads are population-independent here. These are
candidate mechanisms demonstrated at source level, not attribution of deployed
traffic, an optimization target, or a requirement to preserve amplification.

### Stubbed subprocesses, separately accounted

Collector makes two compute-app query calls per tree. Monitor makes two of those
and two device queries. For empty/no_gpu, all exit 0 with 0 stdout bytes;
for gpu_error, all exit 1 with 0 bytes. GPU-present returns 38 total bytes for
the two compute-app calls and 182 for the two device queries. Setup/request have
none. The JSON retains argv, call counts, outcomes and stdout byte totals.
Those bytes are **not included** in file-read totals. GPU utility internals,
process startup, driver I/O and utility file reads are unmeasured, not zero.

## Bounded live disposition (not performed)

Inspected existing configured routes only: `event_wait_baseline.py`'s `trace`
subcommand and the operating guide's event-wait diagnostics, plus
`tools/fleet/admission_shared_io.py`. The first is `strace -c` attachment to a
bare supplied PID with a requested attachment window capped at 10 seconds
(plus teardown grace periods). It has no filename trace and no `-f`;
it does not bind PID/start identity, installed broker identity, or prove full
thread/child coverage. The admission tool is a filename-tracing precedent for
its own synthetic child/private queue, not an authorized live broker route.
Ordinary admitted execution does not establish ptrace authorization to the
root-run broker. No current demonstrably authorized and usable live broker
file-tracing route was established. Tool availability/attachment capability
was not probed; it must not be inferred from source documentation.

Thus no live attachment, syscall summary, file-set trace, deployed PID/start
before/after identity, installed broker identity, duration/workload window or
thread/child coverage evidence was collected. The missing prerequisite is an
existing authorized, usable target-bound file-tracing route; this is not a
request for Rob to perform a physical action. No install, privilege/ptrace change,
service restart, assumed root attachment, publication or workflow transition
was attempted. The live disposition and this note require human review:
deterministic offline tests cannot authenticate deployed observations or access.

Other unmeasured candidates include many-scope scaling, disappearing PIDs,
nonzero OOM/enforcement paths, foreign GPU processes/ROCm/unified memory,
active-scope request reconciliation, real installed-file sizes and request rate,
state persistence, locks, directory/stat traffic and subprocess internals.
No cadence, cache, attribution, fencing or authority behavior changes here.
W2/#2063 owns collector repair; W3/#2064 owns event-capable broker observations;
#1946, #1895/#2039 and #1967 retain notification, GPU-fencing and authority-lock
outcomes. #2067 owns shared-mount attribution. The shared interface here is the
existing root/runner seams and this report, not shared production edits or
new duplicated issues. #1635's exception does not authorize publication here.

## Retained verification evidence

Fresh combined acceptance, broker/collector and four mandatory guards, using
exactly the full `-q -s` command above: **239 passed** on x86 Python 3.14
(122.09 s) and **239 passed** on ARM Python 3.12 (32.85 s), both exit 0.
This includes fresh-root repeatability and independent Path observers/real
collector-call checks; it does not constitute a live broker observation.
Both retained stdout files contain fresh full numeric JSON reports. Inspected
report excerpts through `read_test_artifact`, including fixture rows, subprocess
accounting, source hashes and the ARM terminal result. Counts and byte sizes
agree with the recovered baseline tables above. An additional x86 direct
`run_synthetic` invocation with `PYTHONPATH=src` printed all 48 phase totals
(attempts, successes, bytes) and independently confirmed every table total.
Amplification is observational, not pinned as required behavior. O2/O3 still
require reviewer inspection: tests cannot establish the truth of a narrative or
authenticate live access evidence.

Compact projection evidence (additional x86 run):

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"7c4f1f8e7dda7b7694bc0d1111d631cd48397125cd7179684e4a1255e5a63335","size":1673}},"operation":"a722d43f843f7d4d53663aa7d230b9508da25ccc637666efe46d0b9952fa8821","receipt":"d375047f744e36c6012a765724dde7d6b9d56fd8cc37acd86c3d468693f30a73"}]

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"560e78c94797b94fb16af54b2d537d64c77b327c141d9e27b9785b24b356c64c","size":178373}},"operation":"fbb83d8e08f74075ff13be7c8d7b8c164b79b2e58deec28c60cc2a0d3790256e","receipt":"a895239bce359e4d1cb9eaa0e3a77e213a6b7722deb043de55fcd349212acf00"}]
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"6be7145a5480852b3cf42d34693ff0b93a5322a153f38803e284c2e43c5727f3","size":178379}},"operation":"86e2120f4762272de8974bb03a742146976c61674c20b42c4bc15f2da00edd7b","receipt":"ae47d868345065bdc21f59cf6fdbdf6bae51f82889975723a3524f0da51e1a92"}]


## W2 comparison — candidate-first NVIDIA attribution (#2063)

These are new synthetic observations, not a relabelling of the historical W1
measurements above. The existing W1 observer and real ResourceMonitor fixture
were reused, along with the W2 compact projection in
`test_w2_nvidia_read_counts_remove_w1_unrelated_pid_slope`. No acceptance tests,
compatibility inventories or goldens were edited during implementation.

Measured source SHA-256 identities (both arms):

| Source | SHA-256 |
| --- | --- |
| `src/prismabuild/gpu_memory.py` | `dfbef5d3d1771303ff6da249aecd0b9f3676b98a6256fefb3150925ed1e018b9` |
| `src/prismabuild/gpu_capacity.py` (unchanged) | `6c61d4543e60e5e42c0a0879cbff6566442ea07969e764afbf563374860c7f28` |
| `tools/fleet/resource_broker.py` (unchanged) | `0f8dfa4bca6633aa8d86aecf81c2d6fe06bba756cf376acb91f7fdeaff54be1c` |

The supplied acceptance fixture sources were retained:
`tests/test_broker_read_baseline.py` SHA-256
`a8b3571ba7c34150a53908a677a0e1fcc4af7343a3f4eea0cee96026dac0c2df`,
and `tests/test_gpu_memory.py` SHA-256
`ddc994d083f1dff35500a2f4fa3647ac9361256d1193b74e7661835513580634`.
Base: `9dce28460e76d4115c925bac8efc4f397160f9cf`.

All totals below are **two calls**, with N = unrelated fixture PIDs.
W2 counts/bytes are identical at each of N=0, 8 and 64 on both CPU arms.

| Scenario | Phase | W1 reads N=0/8/64 | W2 reads N=0/8/64 | W2 returned file bytes (each N) | W2 stat / cgroup attempts (each N) |
| --- | --- | --- | --- | --- | --- |
| empty | collector | 4/36/260 | 4/4/4 | 280 | 0 / 0 |
| empty | monitor | 6/38/262 | 6/6/6 | 468 | 0 / 0 |
| no_gpu | collector | 10/42/266 | 6/6/6 | 296 | 0 / 0 |
| no_gpu | monitor | 16/48/272 | 12/12/12 | 552 | 0 / 0 |
| gpu_error | collector | 10/42/266 | 6/6/6 | 296 | 0 / 0 |
| gpu_error | monitor | 16/48/272 | 12/12/12 | 552 | 0 / 0 |
| gpu_present | collector | 14/46/270 | 14/14/14 | 820 | 4 / 4 |
| gpu_present | monitor | 20/52/276 | 20/20/20 | 1076 | 4 / 4 |

Thus W1's unrelated-PID slope of 4 file reads / 142 returned bytes per PID
per two calls is zero in W2, for collector and monitor alike. For byte
comparison against W1, empty/gpu_present remove exactly `142*N` bytes;
no_gpu/gpu_error also remove the single non-GPU PID's 262 bytes of identity
reads over two calls (`2*(55+76)`), for a total reduction of `142*N+262` bytes.
Empty or failed discovery performs no candidate identity
reads. GPU-present identities are read before and after measurement, not just
after a query. Discovery counters are discarded; newly measured PIDs without
matching pre-measurement identity proof stay incomplete. Scope inode fencing,
unknown-not-zero semantics, memory domains and Guard logic remain in force.

### Subprocess cost (separate from file bytes)

At every N, successful discovery adds one compute-app query per collection:
W1's 2 become W2's 4 compute-app calls over two collections. Empty and no_gpu
return 0 stdout bytes; gpu_present returns 76 bytes total instead of W1's 38.
Failed discovery (gpu_error) ends after the original 2 failed compute-app
calls, with 0 stdout bytes; it does not attempt measurement. The monitor also
retains its 2 capacity queries, so total vendor calls are 6 for successful
scenarios and 4 for gpu_error. GPU-present capacity stdout remains 182 bytes,
separate from the 76 compute-app bytes. These are stubbed subprocess calls and
returned stdout, **not** traced utility-internal reads, driver work or latency.
Both NVIDIA queries and identity work consume the same collection deadline;
the second query is not started after exhaustion. AMD/WSL still uses fresh
host identities bracketing its measured descriptor census, not an event-driven
or cached holder inventory.

### Verification and retained evidence

Command (plain pytest through run_tests, separately on x86/Python 3.14 and
ARM/Python 3.12):

```sh
python -m pytest tests/test_gpu_memory.py tests/test_gpu_capacity.py tests/test_resource_broker.py tests/test_broker_read_baseline.py tests/test_wire_inventory.py tests/test_fleet_tool_flags_have_help.py tests/test_duplication_baseline.py tests/test_design_doc_line_references.py -q -s --basetemp="$TMPDIR/pytest"
```

Observed: **300 passed**, exit 0 on each arm (77.88s x86, 32.89s ARM).
This includes O1–O3 selectors, AMD/WSL, foreign inventory, memory-domain,
Guard/sample-gap and capacity/broker regressions and all four repo-wide guards.
Retained stdout was inspected for the compact projection, source identities,
subprocess stdout and terminal outcomes. O5's narrative/refactor/public-shape
review remains a reviewer responsibility, not an executable-test claim.

[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"18b29671ed73eebfee515cd3f960b984ff833f11bcd6454b8653b70eaa458ee5","size":49983}},"operation":"85d392c2a2275be66814ca440e365c0491387fb2dd9dbd10a6d8eadcba71b44e","receipt":"5f1da693db428c54b4f15fd246c5c1a14545f1eaaa98947e94fa2fb843d013cf"}]
[Test evidence {"artifacts":{"stderr":{"digest":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":0},"stdout":{"digest":"0400bccfaf60e96b14fc4d3aa162751e248e5bcf071268e31ae664198ffebd7f","size":49986}},"operation":"74a3ea191076ff047d25c1c5ecf9b27e73e6d07219ae8c38e195511e0e1545f5","receipt":"ff2f12f78418f9d99a4546485f4a09ba4d585fbcc8efea6f547f5dacdd4a2951"}]

No privileged broker was deployed or restarted, and no live GPU qualification
was run. Synthetic fixture results cannot authenticate the reported production
17,000 reads/s or 153 MB/s, or establish any live reduction. W1's original
limitations and historical identities above remain applicable.

W1 is already on main: `1ba19531393b` supplies the baseline tests, and
`2082d829e3ec` supplies the reproducer and historical measurement note.
PR #2234 reuses those artifacts for W2 and integrates current main into the
existing branch. Review and publication now follow Paperclip's Principal
Engineer review and batched merge path. No #2040 UMA accounting redesign is
included.
