# What a declared `mem_gb` now means, and exactly how far it reaches

**Status:** measured on the fleet, 2026-09-04. Closes the enforcement half of
prismabuild issue #1. Supersedes the "not live-validated" paragraph in
`docs/design.md` for the cgroup half; see *Scope* for the half it does not.

`mem_gb` was a reservation and nothing else. `ResourceLedger` admitted work
against a declared figure, and an action that exceeded its declaration was
admitted, ran, and finished. The declaration was an honour system, and the
kernel's answer to a breach picked its victim from the whole box rather than
from the offender: on sparky at 2026-09-01 09:58:41 the OOM killer took
`pqwork.service`, 20.6 MB peak, which was consuming nothing.

An action now runs inside a transient user unit whose `MemoryMax` **is** its
own declaration, so the reservation and the limit are one object held in one
place: the tokens the action took out of the ledger in `claim` are the number
its cgroup refuses to let it exceed.

The ordering here was set by the issue itself: *a cap that silently does not
bind is worse than no cap, because the ledger would then read as enforced.*
Everything below is a measurement, taken before the mechanism was trusted.

---

## 1. The box can cap, and that is a property of the box

Capping needs the `memory` controller delegated to the *user* manager. That is
a boot-time property of each host, not a property of this code, so it is probed
rather than assumed — and a box that cannot cap says so in its offer
(`mem_cap_scope`) instead of failing every capped action forever and looking
like a flaky queue.

Measured 2026-09-04 on all three fleet boxes:

| host | `user-1000.slice/cgroup.controllers` | `Linger` | kernel | systemd |
|---|---|---|---|---|
| sparky (GB10) | `cpu memory pids` | yes | 6.17.0-1031-nvidia | 255 |
| gx10-6b77 / sparklina (GB10) | `cpu memory pids` | yes | 6.17.0-1032-nvidia | 255 |
| dl380g10 | `cpu memory pids` | yes | 7.0.0-30-generic | 259 |

All three enforce today. The probe stays anyway: this table is a fact about
three machines on one date, not a fact about the fleet.

## 2. The cap binds a host allocation, and the whole chain reports it

One capped unit, 256 MiB, against a Python process faulting in 4 GiB of
`bytearray` 64 MiB at a time. Run on sparky, 2026-09-04:

```
took 64 MiB
took 128 MiB
took 192 MiB
systemd_run_rc=1
Result=oom-kill ExecMainCode=2 ExecMainStatus=9 MemoryPeak=268435456
unit_outcome -> {'result': 'oom-kill', 'exec_main_code': 2,
                 'exec_main_status': 9, 'memory_peak': 268435456,
                 'returncode': -9, 'oom_killed': True}
```

`MemoryPeak` is 268435456 — exactly the cap, to the byte. The offender died;
nothing else on the box was a candidate; and the child's stdout up to the kill
survived the wrapper, which is what the outcome record and the submitter's
error tail are made of.

`tests/test_pool_memory_cap_binds.py` is this measurement as four tests against
the real kernel, skipped on a box that cannot cap.

## 3. What `systemd-run --pipe --wait` actually reports

`--pipe` is load-bearing: without it the child's stdout and stderr go to the
journal, and the outcome record becomes empty. What it costs is that
`systemd-run`'s own exit status is not the child's in every case, so the truth
is read back from the unit. Measured, same box, same day:

| child did | `systemd-run` rc | `Result` | `ExecMainCode` | `ExecMainStatus` | what the pool reports |
|---|---|---|---|---|---|
| `exit 0` | 0 | success | 0 | 0 | 0 |
| `exit 3` | 3 | exit-code | 1 | 3 | 3 |
| `kill -9 $$` | 255 | signal | 2 | 9 | −9 |
| exceeded `MemoryMax` | 1 | **oom-kill** | 2 | 9 | −9, `oom_killed` |

Two of those four rows cannot be recovered from the launcher's status alone,
and the one that matters most — an OOM kill — is the one that looks like a
plain `exit 1`. Hence `unit_outcome`: read `Result`/`ExecMainCode`/
`ExecMainStatus`/`MemoryPeak` back, translate `CLD_KILLED`/`CLD_DUMPED` to
Python's negative-return convention, and let a `None` fall back to the
launcher's own status. `pbrun` renders a negative status as `128 + signal`, so
a killed action exits 137 at a shell like every other tool on the box.

### Three launcher preconditions, each measured rather than reasoned about

* **A timeout must stop the unit, not the launcher.** Killing `systemd-run`
  does not stop the service it started, and under `--pipe` the service holds
  the pipe the caller is about to read. Measured with the stop suppressed: a
  one-second timeout was still running 100 s later with its unit `active`.
  `execute` issues `systemctl --user stop` first; the launcher then exits.
* **A failed transient unit lingers.** The unit name carries a per-attempt
  nonce, so a leak is never cleaned up by the next launch. `reset-failed` runs
  in a `finally`, because the paths that skip it are exactly the ones that leak.
* **`--pipe` forwards stdin, and a closed fd 0 breaks it before the work
  starts** — `Failed to create bus message: Bad file descriptor`, rc 1. A
  worker reads no stdin, so every launch gets `/dev/null` and the mode is gone.

And one that is not about systemd: `systemd-run --user` finds the bus through
`XDG_RUNTIME_DIR`. A loop spawned outside a login session can have the bus
without the name; the probe would then answer "this box cannot cap" — true of
the loop, false of the hardware — and quietly stand every declaration back down
to an honour system. The probe repairs the name when `/run/user/<uid>/bus`
actually exists. (All three live `worker_loop` processes carry it today; the
repair is for the spawn that does not.)

---

## 4. The wrapper bounds the execution and must not move it

A transient unit is forked by the **user manager**, not by the caller, so
nothing of the launcher's context reaches the work except by being named. Two
members were not named, and the first pass shipped them silently. Neither could
have been caught by an argv assertion, because argv was not where they went
missing.

Measured with `tools/fleet/probes/exec_context_probe.py` — one identical child
run twice under one launcher, only the wrapper differing — at the two commits
on **sparky**, the box the regression was attested on, and again on
**gx10-6b77**, both GB10:

| what the child sees | launcher | unit, before | unit, after |
|---|---|---|---|
| CPU affinity | `0-1` | `0-19` (all) | `0-1` |
| soft `RLIMIT_NOFILE` | 314159 | 1024 | 314159 |
| the other 14 rlimits | — | identical | identical |
| cgroup path | `session-15.scope` | `pbexecctx-….service` | `pbexecctx-….service` |
| `oom_score_adj` | −1000 | 200 | 200 |

(sparky, commits `79e58bd` → `8074298`: `rlimits_identical` 14 of 16 → 16 of
16, `differs` down to `cgroup` and `oom_score_adj`. gx10-6b77 returns the same
two arms with the launcher pinned to `5-6`. The dl380g10 row in section 1 is a
delegation fact only; the *before* arm was also seen there — affinity `0-1` →
`0-79`, soft `RLIMIT_NOFILE` 314159 → 1024 — with an earlier draft of this
probe, and no after arm was run on it.)

**Affinity.** `cpu_topology.pin_to_preferred`'s stated mechanism is inheritance
by fork — "Pin this process *and so every child it forks*" — which a unit is
not. Every capped action escaped the loop's pin: on a GB10 that puts compute on
the 2.8 GHz A725 half of an interleaved machine, and it makes the loop's
cpu-token offer describe ten cores while its actions use twenty. The fix is
`CPUAffinity=`, which is an exec-context setting and so needs no `cpuset`
delegation — these boxes delegate `cpu memory pids` and not `cpuset`.

**The fd ceiling.** Soft `RLIMIT_NOFILE` fell to systemd's
`DefaultLimitNOFILE` soft of 1024, hard untouched. It is the only one of
sixteen rlimits that moved. Since `pbrun` defaults `mem_gb` to 4, essentially
every action is capped, so essentially every action would have run at a 500×
lower fd ceiling; an NFS shard reader, a torch `DataLoader` or `pytest -n N`
crossing 1024 raises `EMFILE`, which the queue retries `max_attempts` times and
files against the payload.

**Two differences are left alone on purpose, because they are the bound rather
than the execution.** The cgroup path *is* the mechanism. And `oom_score_adj`
rises to 200, which points the right way: the incident this cap exists for is a
*bystander* being chosen by the kernel, and an action that has outgrown its own
declaration should be a likelier victim than the loop supervising it — carrying
the loop's own −1000 across would make the offender the last thing the kernel
would pick.

**A third difference is recorded rather than fixed.** The unit's environment is
a strict *superset* of the launcher's — `env_only_in_launcher` is empty on both
boxes, so nothing is lost — and names are added (`INVOCATION_ID`, `MANAGERPID`,
`MEMORY_PRESSURE_WATCH`/`_WRITE`, `SSH_AUTH_SOCK`, `SYSTEMD_EXEC_PID` and the
desktop-session names; 9 on sparky, 13 on gx10-6b77, the difference being only
what each launcher's own environment already carried), because the user manager
passes its own environment to every unit it starts. They reach the pool *worker*, not the
action: `run_local_action` builds the payload's environment from the variables
the action declared and nothing else (`core.py`, `env={...variables...}`), so
the closed environment sealed into the action key is unchanged. `systemd-run`
can add names but cannot clear the manager's, and the set is box-dependent, so
naming it in `UnsetEnvironment=` would be a roster where a rule is wanted. It
is measured, bounded to the worker, and left.

The test that holds this is `tests/test_pool_cap_keeps_the_exec_context.py`,
and it compares the *child's own view* against the launcher's rather than the
argv — which is what catches a property systemd ignores, or `setrlimit_closest`
clamping one the user manager will not grant. It restricts the launcher first
(a two-CPU mask, a soft `RLIMIT_NOFILE` of 314159): a suite already running
inside a capped unit has soft 1024 and the full mask, so without that the test
would pass against a wrapper that carries nothing.

---

## 5. Scope: what the cap does **not** reach

A cap is only worth what it charges. Four boundaries, each read off the live
box rather than reasoned about, because the ledger reads as enforced either
way and a silent gap is the failure mode the issue named:

* **A nested `systemd-run --user` escapes.** Measured 2026-09-04: a capped unit
  at `.../app.slice/pbcap-nest-370423.service` started an inner unit at
  `.../app.slice/run-u51.service` — a *sibling*, not a descendant, so the
  inner work is charged to nothing. Ordinary `fork`/`exec` children are
  descendants and are charged; it is specifically the act of asking the user
  manager for a new unit that leaves the cgroup.
* **A container escapes.** A live container on sparky sits at
  `/sys/fs/cgroup/system.slice/docker-2254231f…scope` — under the *system*
  manager, not under `user.slice`. A pbrun payload that runs `docker run` is
  therefore uncapped, which matters because that is a shape the fleet actually
  submits (`require_pool.py` was written for a `docker run --gpus all` holding
  a GPU lock for 73 minutes).
* **Page cache is charged, and then reclaimed rather than killed.** A 1 GiB cap
  reading a cold 3 GiB file charged `file` 1068630016 and hit
  `memory.events max` 8211 times — the cap engaged, over and over — but
  `oom_kill 0`, and the read finished. So a streaming action under a small
  declaration is not stopped; it re-reads. On an NFS input that is "a loud kill
  becomes a slow box" arriving through the other door. A `mem_gb` declaration
  bounds an action's *anonymous* footprint and only throttles its I/O.
* **The device half of a GPU action escapes entirely** — the largest of the
  four, and the subject of §6.

## 6. The measurement the issue asked for first: CUDA on GB10 unified memory

Three arms, one 4 GiB cap, each asking for 8192 MiB in 512 MiB steps, each
touching every page it takes. Only the allocator differs. Run on sparky
through the pool as action `1244c3e5db31`, 2026-09-04, 9.8 s:

| arm | allocator | `memory.current` at the end | `MemAvailable` fell | outcome |
|---|---|---|---|---|
| host | `bytearray`, page-faulted | 3771064320, rising step for step | 2873 MiB (partial) | **killed**, `Result=oom-kill`, `MemoryPeak` 4294967296 |
| pinned | `torch.empty(pin_memory=True)` | 4127821824, rising step for step | 2743 MiB (partial) | **killed**, `Result=oom-kill`, `MemoryPeak` 4294967296 |
| cuda | `torch.empty(device="cuda")`, `fill_`, `synchronize` | **381095936, flat through all 8 GiB** | **8725 MiB** | **survived**, `memory.events: max 0 oom 0 oom_kill 0` |

Read the cuda row twice. The action took 8.5 GB out of a 128 GB pool the host
and the GPU share; the box saw the loss in `MemAvailable`; the cgroup that was
supposed to be holding it to 4 GiB **charged none of it** and never fired. The
381 MB it did charge is the CUDA context's host-side footprint, and that number
does not move again.

So the answer to the issue's first question is **no**: on a GB10, `MemoryMax`
does not account memory taken through the CUDA allocator, even though that
memory is physically the same pool the cap is denominated in.

### What that means for what shipped

**The name of this deliverable is host-footprint enforcement**, and every
field says so rather than saying "enforced":

* a worker publishes `mem_cap_scope: "host"` (or `"none"`, or `null` for a loop
  that predates the field) — never a bare boolean;
* every capped outcome record carries `cap_scope: "host"` next to
  `declared_mem_gb` and `capped`;
* the loop's start line says the same in words, with a pointer to this file.

That is not a consolation prize. The declaration being enforced was calibrated
on *resident host* memory in the first place (`worker_loop.py`'s capacity notes:
one exporter holds ~8 GB resident; four took a GB10 from 116 GB free to 55 GB),
and the bystander kill this issue was opened over — `pqwork.service`, 20.6 MB
peak, killed on sparky at 2026-09-01 09:58:41 — is a host-side runaway, which
this stops. But a GPU action's device half is unbounded, and on this hardware
that is the half that fills the box.

**The tension the issue named is now a measured fact, not a hypothesis.**
PrismaBuild's `mem_gb` tokens are a discrete resource contract; PR #112's survey
argued a discrete contract is the wrong shape for a UMA pool whose real ceiling
is system `MemAvailable`. The cuda row above is that argument in numbers. The
next question — not this issue's — is what bounds the device half: a
`MemAvailable`-denominated admission ceiling, `CUDA_MPS`/MIG-style device
limits, or a per-action device budget the payload itself honours. None of those
is a cgroup.

## 7. What is still not measured, and what cannot yet be measured

* **`memory_peak_bytes` is `null` for every action that succeeds.** A transient
  unit is freed the moment it goes inactive, so `MemoryPeak` is readable only
  for the units that *failed* — measured, not inferred. The consequence is that
  the fleet can see its kills and cannot see its margins: no declaration can yet
  be calibrated from data. The mechanism for fixing it exists (an
  `ExecStopPost=` that copies the unit's own `memory.peak` out while the cgroup
  is still there); it is deliberately not in this change.
* **This makes an uncalibrated default dangerous.** `pbrun` defaults a non-GPU
  action to `mem_gb=4`. Of 44 items live in the queue while this was written,
  **7 declared 4 and 11 declared 8** — figures nobody has ever had to be right
  about, because until now nothing checked. On the first publish they become
  hard limits, and an under-declared action will exit 137 where it used to
  finish. That is the mechanism working; it is also a fleet-behaviour change
  that belongs to whoever publishes, not to the branch. **The memory ceiling is
  the whole of that change**, which is worth stating because for one revision of
  this branch it was not: the fd ceiling and the core placement changed too
  (§4), and an EMFILE or an action on the slow cores would have been read as a
  payload problem. They are carried now, and
  `tests/test_pool_cap_keeps_the_exec_context.py` is what keeps them carried.
* **Three boxes, one date.** Section 1 is a fact about sparky, gx10-6b77 and
  dl380g10 on 2026-09-04. The probe stays because the next box is not covered
  by it.
* **The CUDA arm is one box, one allocator, one driver.** sparky, GB10,
  sm_121, driver 595.84, torch 2.11.0+cu130. It is not a claim about discrete
  NVIDIA hardware, where device memory is not the host's pool at all.

## 8. Reproducing this

```
# the three arms (needs a GPU token; submit, do not run out of pool)
tools/fleet/pbrun.py --gpu --demand mem_gb=16 -- \
    bash tools/fleet/probes/run_cgroup_cuda_probe.sh

# what the wrapper changes besides bounding (§4); run it at two commits
tools/fleet/pbrun.py --cpus 1 --demand mem_gb=2 -- \
    python3 tools/fleet/probes/exec_context_probe.py

# everything else, on any box that can cap
python3 -m pytest tests/test_pool_memory_cap.py \
                  tests/test_pool_memory_cap_binds.py \
                  tests/test_pool_cap_keeps_the_exec_context.py \
                  tests/test_worker_loop_caps_a_declared_action.py
```

The raw three-arm output is the `detail.stdout` of pool action
`1244c3e5db319c4d2daa0f6da7bb9e71adc8578cc764b5ee6b961ba4216f3b37`.

---

## Appendix: the three-arm trace, verbatim

Kept here because the CAS is not an archive. `sparky`, 2026-09-04,
`MemAvailable` 106856000 kB at start, GPU drawing 29.89 W of a ~140 W envelope
(so the box was not otherwise loaded; on a GB10 `gpu_utilization` would not
have told you that).

```
=================== arm=host cap=4G target=8192MiB ===================
{"arm": "host", "memory_max": 4294967296, "memory_swap_max": 0, "mem_available_mb_start": 104691}
{"held_mb": 512,  "memory_current": 543023104,  "mem_available_mb": 104690}
{"held_mb": 1024, "memory_current": 1080942592, "mem_available_mb": 104321}
{"held_mb": 1536, "memory_current": 1619124224, "mem_available_mb": 103791}
{"held_mb": 2048, "memory_current": 2157043712, "mem_available_mb": 103290}
{"held_mb": 2560, "memory_current": 2695225344, "mem_available_mb": 102778}
{"held_mb": 3072, "memory_current": 3233144832, "mem_available_mb": 102270}
{"held_mb": 3584, "memory_current": 3771064320, "mem_available_mb": 101818}
-- systemd-run rc=1
-- unit: Result=oom-kill ExecMainCode=2 ExecMainStatus=9 MemoryPeak=4294967296

=================== arm=cuda cap=4G target=8192MiB ===================
{"arm": "cuda", "memory_max": 4294967296, "mem_available_mb_start": 104737,
 "torch": "2.11.0+cu130", "cuda_available": true,
 "memory_current_after_import": 291414016, "memory_current_after_context": 381095936}
{"held_mb": 512,  "memory_current": 381095936, "mem_available_mb": 103690}
{"held_mb": 1024, "memory_current": 381095936, "mem_available_mb": 103170}
{"held_mb": 2048, "memory_current": 380837888, "mem_available_mb": 102186}
{"held_mb": 4096, "memory_current": 380837888, "mem_available_mb": 100147}
{"held_mb": 6144, "memory_current": 380342272, "mem_available_mb": 98080}
{"held_mb": 8192, "memory_current": 380342272, "mem_available_mb": 96012}
{"arm": "cuda", "verdict": "SURVIVED", "held_mb": 8192,
 "memory_current": 380342272, "memory_peak": 381095936, "mem_available_mb_end": 96012,
 "memory_events": ["low","0","high","0","max","0","oom","0","oom_kill","0","oom_group_kill","0"]}
-- systemd-run rc=0
-- unit: Result=success ExecMainCode=0 ExecMainStatus=0 MemoryPeak=[not set]

=================== arm=pinned cap=4G target=8192MiB ===================
{"arm": "pinned", "memory_max": 4294967296, "mem_available_mb_start": 104326,
 "torch": "2.11.0+cu130", "memory_current_after_import": 291704832}
{"held_mb": 512,  "memory_current": 894943232,  "mem_available_mb": 104173}
{"held_mb": 1024, "memory_current": 1432162304, "mem_available_mb": 103688}
{"held_mb": 2048, "memory_current": 2510671872, "mem_available_mb": 102720}
{"held_mb": 3072, "memory_current": 3589062656, "mem_available_mb": 101717}
{"held_mb": 3584, "memory_current": 4127821824, "mem_available_mb": 101583}
-- systemd-run rc=1
-- unit: Result=oom-kill ExecMainCode=2 ExecMainStatus=9 MemoryPeak=4294967296
```

Steps elided from the cuda and pinned arms are monotone between the rows shown;
the cuda arm's `memory.current` never leaves the 380–381 MB band at any step.
