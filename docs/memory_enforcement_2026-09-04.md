# What a declared `mem_gb` now means, and exactly how far it reaches

**Status:** measured on the fleet, 2026-09-04. Closes the **host-page** half of
prismabuild issue #1. Supersedes the "not live-validated" paragraph in
`docs/design.md` for the cgroup half; see *Scope* for the half it does not.

**Read the name before the result.** What ships here is *host-footprint*
enforcement. §6 is the measurement the issue asked for first, and its answer is
that a GB10's cgroup charges nothing at all for memory taken through the CUDA
allocator — which on this hardware is the half that fills the box. That half is
**issue #8**, it is open, and nothing below closes it. Wherever this file says
"enforced", it means "enforced against host pages".

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
nothing of the launcher's context reaches the work except by being named.
Members were not named, and the first pass shipped them silently. None could
have been caught by an argv assertion, because argv was not where they went
missing.

### The control, corrected

The first version of this section is **retracted on method**. It measured with
`tools/fleet/probes/exec_context_probe.py`, one identical child run twice under
one launcher — which is right — but the launcher restricted only two axes,
its CPU affinity and its soft `RLIMIT_NOFILE`. Every other axis therefore sat
at the box's default on *both* sides of the wrapper, where it matches whether
the wrapper carries it or not. The row it produced —

> | the other 14 rlimits | — | identical | identical |

— was true, and evidence of nothing. So was the `rlimits_identical` 16 of 16
that followed it. **Two treatments are not a control**, and a dimension nobody
perturbed is a dimension nobody measured.

The probe now perturbs every axis it compares — the affinity, the umask, the
nice level, and twelve soft rlimits, each to a value that is neither the box's
default nor systemd's — before either arm runs. Same box, same day, same
probe, at the two commits:

| what the child sees | launcher | unit, uncarried | unit, carried |
|---|---|---|---|
| CPU affinity | `0-1` | `0-19` (all) | `0-1` |
| umask | `0o077` | `0o002` | `0o077` |
| nice | 5 | 0 | 5 |
| soft `RLIMIT_NOFILE` | 314159 | 1024 | 314159 |
| soft `RLIMIT_STACK` | 9437184 | 8388608 | 9437184 |
| soft `RLIMIT_CORE` | 1 | 0 | 1 |
| soft `RLIMIT_NPROC` | 100000 | 511827 (the hard) | 100000 |
| soft `RLIMIT_SIGPENDING` | 100000 | 511827 (the hard) | 100000 |
| soft `RLIMIT_MSGQUEUE` | 819100 | 819200 (the hard) | 819100 |
| soft `RLIMIT_AS` / `DATA` / `FSIZE` / `CPU` / `MEMLOCK` / `RTTIME` | finite | `infinity` | finite |
| `rlimits_identical` | — | **5 of 16** | **16 of 16** |
| cgroup path | `session-15.scope` | `pbexecctx-….service` | `pbexecctx-….service` |
| `oom_score_adj` | −1000 | 200 | 200 |
| pgid / sid / ppid | the launcher's | the manager's | the manager's |

(sparky, `735a315` → `3fc0d9d`. The probe's own verdict field moves with it:
`clean: false` with thirteen `unclassified` entries, against `clean: true` with
none. The earlier *uncarried* arm was also seen on gx10-6b77 and dl380g10 with
the two-axis probe — affinity `0-1` → `0-79` and soft `RLIMIT_NOFILE`
314159 → 1024 on the Xeon — and those two rows still stand; the eleven rows
this table adds are sparky, systemd 255, and are a fact about the mechanism
rather than about a roster, which is why the fix is a rule.)

Both sparky arms were run **direct, not through the pool**: sparky's ledger had
no `mem_gb` to offer at the time — its whole 48 GB and both GPU tokens are held
by an exclusive campaign — and the probe is a sub-second `systemd-run`, not
compute. The launcher in the sparky column is therefore the probe's own shell.
Its `oom_score_adj` of −1000 is what a *loop* carries too, read separately:
every `worker_loop.py` on sparky reports −1000 in `/proc`. What a *pooled*
launcher reads is not the same number: the dl380g10 arm, submitted through the
pool and so forked by that box's loop, saw its launcher at 0. Whether that
box's loop is also at −1000 was not read, so the two figures are recorded
rather than resolved into a rule — which is why §7 says publish day moves a
capped action from 0 or −1000 rather than picking one.

### What is carried, and on what rule

**The affinity.** `cpu_topology.pin_to_preferred`'s stated mechanism is
inheritance by fork — "Pin this process *and so every child it forks*" — which
a unit is not. Every capped action escaped the loop's pin: on a GB10 that puts
compute on the 2.8 GHz A725 half of an interleaved machine, and it makes the
loop's cpu-token offer describe ten cores while its actions use twenty. The fix
is `CPUAffinity=`, which is an exec-context setting and so needs no `cpuset`
delegation — these boxes delegate `cpu memory pids` and not `cpuset`.

**Every resource limit systemd can express, by rule and not by roster.** Soft
`RLIMIT_NOFILE` is the one that bites on today's fleet: the live loops sit at
500000/500000 and a unit gets systemd's `DefaultLimitNOFILE` soft of 1024, hard
untouched. Since `pbrun` defaults `mem_gb` to 4, essentially every action is
capped, so essentially every action would have run at a 500× lower fd ceiling;
an NFS shard reader, a torch `DataLoader` or `pytest -n N` crossing 1024 raises
`EMFILE`, which the queue retries `max_attempts` times and files against the
payload.

It is *not* the only one that can move, and the reason it is the only one that
does today is worth being exact about. Read off the live box: sparky's three
`gb10` loops and the user manager itself hold the **same** values for `STACK`,
`CORE`, `NPROC`, `SIGPENDING` and `MSGQUEUE`, so those five match across the
wrapper by coincidence of this week's roster. Carrying only the limit the
regression happened to expose would be green on this fleet and wrong about the
mechanism. `SYSTEMD_RLIMIT_STEMS` is systemd's own list of sixteen
`Limit<X>=` properties; `launcher_exec_context` reads whichever of them this
process has and `capped_launch_argv` names all of them, so a limit is carried
because a unit file can express it — not because somebody remembered it. A
stem systemd has no property for is refused rather than accepted and ignored.

**The umask and the nice level.** Both are exec context the manager resets: a
launcher at `0o077` produced a child at the manager's `0o002`, and nice 5
produced nice 0. The mask decides the mode of every byte an action writes into
the shared CAS — `0o002` is group-writable where the launcher asked for
owner-only — and the nice level is the CPU half of the same escape
`CPUAffinity` closes. (On this fleet today both already match: the loops run at
umask `0002` and nice 0, the same as the manager. Same reasoning as the
rlimits: a roster, not a rule.) A **negative** nice is not carried, and that
too is measured rather than assumed — `Nice=-5` and `Nice=-1` both start and
both land the child at nice 0, because raising priority needs a privilege the
user manager does not have. Naming it would be the wrapper claiming a carry it
does not perform.

### What publish day actually moves, read off the live loops

The table above is a *perturbed* launcher, which is what measures the
mechanism. What the fleet will see is a different question, and it is answered
by reading a real loop rather than by inference. Sparky, 2026-09-04, a live
`worker_loop.py` (`/proc/<pid>/limits`, `/proc/<pid>/status`) against what a
unit gets by default:

| | live loop | unit default | moves on publish |
|---|---|---|---|
| soft `RLIMIT_NOFILE` | 500000 | 1024 | **yes** |
| `STACK`, `CORE`, `NPROC`, `SIGPENDING`, `MSGQUEUE` | 8388608 / 0 / 511827 / 511827 / 819200 | identical | no |
| `CPU`, `FSIZE`, `DATA`, `RSS`, `MEMLOCK`, `LOCKS`, `AS`, `RTTIME` | unlimited | unlimited | no |
| `NICE`, `RTPRIO` | 0 | 0 | no |
| umask | `0002` | `0002` (the manager's) | no |
| nice | 0 | 0 | no |

So on this fleet, on this date, carrying everything changes exactly one thing
more than carrying `NOFILE` alone would: nothing. That is the point rather than
an argument against it — the five limits that match, match because the loops
inherit them from the same user manager that starts the units, which is a fact
about this week's loops. The hard limits of the manager (pid 2067) equal the
loops', so nothing is clamped on the way in either.

### What is deliberately not carried

**The cgroup path** is the mechanism itself.

**`oom_score_adj`** rises to 200, which points the right way: the incident this
cap exists for is a *bystander* being chosen by the kernel, and an action that
has outgrown its own declaration should be a likelier victim than the loop
supervising it — carrying the loop's own −1000 across would make the offender
the last thing the kernel would pick.

**The process group and the session** cannot be restated as a unit property at
all: the work is forked by the manager, so it is in neither the launcher's
group nor its session. What that costs is not a value but a *bound*, and the
bound is the one every timeout on this box is built from. Measured on sparky,
2026-09-04, against a unit running `sleep 120`:

```
launcher_rc_after_TERM_to_its_process_group : -15
unit_before_signal    : ActiveState=active   MainPID=2932625
unit_after_TERM       : ActiveState=active   MainPID=2932625
unit_6s_after_TERM    : ActiveState=active   MainPID=2932625
```

The launcher died; the work did not notice. So `pool.execute` stops the *unit*
— on the timeout path, which it already did, and now on the abort path too,
which it did not. Without it a Ctrl-C or any exception unwinding out of
`execute` leaves the action running against a claim `serve_once` is about to
file as failed and a ledger token it is about to release, which is the one
thing a ledger must never say. Both paths run in the same order (stop the unit,
kill the launcher, drain the pipes with a bound) and both are bounded twice:
`TimeoutStopSec` on the unit bounds systemd's TERM-then-KILL escalation, and
`CAP_STOP_GRACE_S` bounds the launcher's wait. The default `TimeoutStopSec` for
a user unit is 90 s — six times the window the caller waits — so without naming
it the bound would only decide which of the two gave up first. The timeout
record carries `unit_stopped` and `action_survived_kill` rather than assuming:
a stop that failed silently would release a ledger token for a box somebody
still holds.

**A difference that is recorded rather than fixed.** The unit's environment is
a strict *superset* of the launcher's — `env_only_in_launcher` is empty on both
boxes, so nothing is lost — and names are added (`INVOCATION_ID`, `MANAGERPID`,
`MEMORY_PRESSURE_WATCH`/`_WRITE`, `SSH_AUTH_SOCK`, `SYSTEMD_EXEC_PID` and the
desktop-session names; 9 on sparky, 13 on gx10-6b77, the difference being only
what each launcher's own environment already carried), because the user manager
passes its own environment to every unit it starts. They reach the pool
*worker*, not the action: `run_local_action` builds the payload's environment
from the variables the action declared and nothing else (`core.py`,
`env={...variables...}`), so the closed environment sealed into the action key
is unchanged. `systemd-run` can add names but cannot clear the manager's, and
the set is box-dependent, so naming it in `UnsetEnvironment=` would be a roster
where a rule is wanted. It is measured, bounded to the worker, and left.

The tests that hold this are `tests/test_pool_cap_keeps_the_exec_context.py`
and the two abort/stop cases in `tests/test_pool_memory_cap_binds.py`. The
first compares the *child's own view* against the launcher's rather than the
argv — which is what catches a property systemd ignores, or `setrlimit_closest`
clamping one the user manager will not grant — and it perturbs every axis it
compares, for the reason this section was rewritten. The second interrupts a
real capped launch and asserts the unit is inactive afterwards.

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
  that belongs to whoever publishes, not to the branch. It is worth being exact
  about *what else* moves, because for two revisions of this branch the answer
  was wrong: the fd ceiling and the core placement changed too (§4), and an
  EMFILE or an action on the slow cores would have been read as a payload
  problem. The second revision carried those two and reported the rest
  identical against a control that could not have found otherwise. Everything
  a unit file can express is carried now, and
  `tests/test_pool_cap_keeps_the_exec_context.py` — perturbing every axis it
  compares — is what keeps them carried.
* **Two things do still change besides the ceiling, by design.** A capped
  action's `oom_score_adj` becomes 200 where it was 0 or the loop's −1000, so
  under *box-wide* pressure — the case §5's device half puts back on the table —
  a capped action is now a preferred victim where it was not. That is the
  direction this cap wants (§4), and it is still a live change to who dies
  first. And the unit adds 9–13 environment names, which reach the pool worker
  and not the payload (§4). Neither is a bound; both are named here rather than
  discovered on publish day.
* **Three boxes, one date.** Section 1 is a fact about sparky, gx10-6b77 and
  dl380g10 on 2026-09-04. The probe stays because the next box is not covered
  by it.
* **The CUDA arm is one box, one allocator, one driver.** sparky, GB10,
  sm_121, driver 595.84, torch 2.11.0+cu130. It is not a claim about discrete
  NVIDIA hardware, where device memory is not the host's pool at all.
* **The device half is open, and it is issue #8.** Nothing in this file bounds
  it, and the fields exist so that no reader has to remember that: an offer
  says `mem_cap_scope: "host"` and every capped outcome record says
  `cap_scope: "host"`. A summary of this work that says "`mem_gb` is enforced"
  without the word *host* is over-claiming by exactly the half §6 measured.

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

The probe's own verdict is the short form of §4: `clean: true` with an empty
`unclassified` means every axis it perturbed survived the wrapper and the only
differences left are the five it classifies as bound or mechanism -- the cgroup
path, `oom_score_adj`, and the process group / session / parent triple.

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
