# NFS recurrence while Sparky admission is held

Follow-up to [issue #16](https://github.com/RobTand/prismabuild/issues/16),
after the offer/status expiry correction in PR #347. This record attributes
the admission blockage; it does not identify the underlying NFS protocol or
kernel defect, or claim recovery.

## Exact owner and queue state

The first recovery record,
`/home/rob/tmp/astra-review-20260906/nfs-idle-worker-recovery.json`, identifies
PID 3315675 by start ticks 6550940, command digest, empty children list,
lock identities, and its still-ready action
`697ce2008033007a8c2d21e4af83dbdab5e39232b4de86fb57bf01344e601895`
with zero attempts. The coordinator verified that this idle worker exited
after SIGKILL when SIGTERM had not stopped its `rpc_wait_bit_killable` wait.
It did not edit queue records or reservations.

The next owner, PID **3473413**, was still in `D` / `nfs_lookup_revalidate`
at the follow-up inspection, more than ten minutes after its process start.
Its file descriptors and `/proc/<pid>/fdinfo` establish:

| Descriptor | Ownership |
|---|---|
| 3 | Local admission FLOCK, inode 1441858, `/tmp/prismabuild-admission-1000/a9de3e92bf5dec7d7850631612a6ba95e709d3899f15149b3da14e18431dfc1d.lock` |
| 4 | NFS POSIX transition lock, inode 747230, `transition-locks/4b1c463d46721a7af6b1c36fa0189ca94115fcf9b1415f283d0b7a9a87bf341b.lock` |

The transition name equals SHA-256 of action key
`c6de6cb4d34e7a41937c732fcb9839756cd681dd16f253f9d8bc42f4262defff`.
At 14:23:09 UTC, server-local inspection found that action in `ready`,
attempts 0, tags `gb10`, demand CPU=4/GPU=1/mem_gb=96, with no Sparky held
reservations. Its sealed command is
`python3 experiments/run_full_model_original_wire_checkpoint.py`.
PID 3473413 had no children and ran published generation `cc4de5c726be`.
No signal was sent to this replacement worker.

The replacement holding the same admission lock while blocked on a different
key establishes that retiring the first worker did not repair the underlying
condition. `PoolQueue.claim` holds `Controller.locked()` across `_claim`,
including shared reads and mutations (`src/prismabuild/pool.py`, `claim` and
`_claim`, at base `36524a3b0`). `AdmissionBusy` keeps sibling loops polling;
it does not allow them to enter the occupied admission section. That is the
confirmed connection between this filesystem wait and lost host admission.

## Storage evidence and limits

Sparky runs `6.17.0-1032-nvidia` against dl380g10's `7.0.0-31-generic`, using
NFSv4.2 over RDMA, a hard mount, and `local_lock=none`. The server-local ZFS
queue reads above completed. At 14:23 UTC, the server's 64 `nfsd` threads and
its `nfsd4_callbacks` worker were idle in the process snapshot. The accessible
server kernel journal contained no NFS diagnostic for this window. ZFS data
vdevs were ONLINE with no data errors; the unavailable cache-only device was
still present. None of those observations establishes the cache device,
callbacks, RDMA, or a particular kernel bug as the cause.

Existing Netdata telemetry is more specific about a diagnostic limitation:
at each of the four 30-second points ending 14:26 UTC, the generic mount probe
reported `ok=1` on **both Sparky and dl380g10**, while the admission worker
remained blocked. Its private probe path therefore does not qualify the
queue path that is stuck. Sparky's aggregate completed-RPC means in those
samples were queue 5.2–11.0 ms and RTT 0.24–0.45 ms. These averages do not
locate the pending syscall; the server-local ZFS mount has no NFS RPC chart.

`py-spy dump --pid 3473413` and `/proc/3473413/stack` were denied by current
ptrace permissions. The server's `/proc/fs/nfsd/clients`, NFS administration
files, and tracefs are also restricted. Without a stack/RPC trace, the exact
blocked Python call and NFS operation outcome are unconfirmed. No remount,
service restart, privilege change, further worker termination, or queue/token
mutation was performed in this follow-up.

Artifacts under `/home/rob/tmp/astra-review-20260906/`:

- `nfs-admission-recurrence-3473413.json`: process identity, descriptors,
  locks, empty children, and the mapped ready key.
- `nfs-netdata-sparky-incident.json` and `nfs-netdata-server-incident.json`:
  the existing telemetry readbacks, including unavailable server RPC charts.
- `nfs-mountstats-1427.txt`: client cumulative mount counters; the filename
  is an artifact label, not a precise sample-time assertion.

## Feasible caller-boundary work

PrismaBuild already has a useful mechanism in
`tools/fleet/mount_latency.py:MountSampler._run_probe`: isolate a diagnostic
in one child, bound the parent's pipe wait, poll reaping without blocking,
and refuse to add another child while the previous one remains unreaped.
This is not a scheduler and should be factored for reuse rather than replaced
with another dispatcher. The existing sampler does not protect queue callers.

Adopting it for queue reads needs these additional acceptance criteria:

1. A local supervisor and local runtime/cwd must perform no shared-path
   lookup before or during the parent deadline. Use a monotonic deadline,
   bounded IPC payloads, explicit unavailable results, and retained child
   identity. Expired or partial observations grant no admission credit.
2. A child must close unrelated inherited descriptors before entering the
   shared filesystem. In particular, local FLOCK ownership follows the open
   file description across `fork`; closing only the parent's descriptor can
   leave that lock held by a blocked child. Traditional POSIX record locks
   have different inheritance semantics. See the primary
   [flock](https://man7.org/linux/man-pages/man2/flock.2.html) and
   [fcntl locking](https://man7.org/linux/man-pages/man2/fcntl_locking.2.html)
   documentation.
3. Begin with a pure read-only census boundary. A timed-out mutation may
   still finish later, so moving all of `_claim` to a disposable helper is
   insufficient. Claim/rename/lease/reservation phases require durable
   ownership, a retained attempt identity, and deterministic reconciliation
   before another attempt can use their capacity.
4. Qualification must cover an unreaped helper, inherited-lock exclusion,
   partial/oversized output, restart ownership, failed reads and later
   recovery through admitted PB fixtures. A generic mount-probe success is
   insufficient; the actual read boundary must be tested. No live outage
   needs to be induced to validate those ownership rules.

This is a proposed implementation boundary, not delivered runtime behavior.
It would make callers fail closed and remain observable during a stall;
it would not itself restore NFS or make an unsafe claim runnable. Administrative
NFS recovery still needs a separately scoped operation based on better trace
evidence. The encountered prose asserting that SIGKILL never affects a hard
NFS wait is corrected in the sampler documentation; observed process exit,
rather than a signal or a `D` state alone, decides whether cleanup occurred.

Validation for this follow-up was read-only process, code, queue, journal and
telemetry inspection plus review of the prose diff. No tests were run for
these documentation/comment-only edits; no runtime behavior changed.

## Later diagnosis and scoped recovery, 14:40–14:51 UTC

This later evidence supersedes the earlier diagnostic and recovery limits.
The operator authorized temporary, isolated tracefs instances and read-only
kernel-memory inspection. Docker's default AppArmor policy caused the earlier
inspection denials; an authorized diagnostic container with explicit capabilities
and read-only bindings could read the existing kernel state. No kernel memory,
mount configuration, server service, or client-expiry control was changed.

The installed Sparky kernel's `__nfs_lookup_revalidate+0xd4` waits on a dentry's
`d_fsdata == NFS_FSDATA_BLOCKED`. This is a local uninterruptible wait for a
pending rename/unlink to finish. The installed unblock code already contains
the ordering barrier and `wake_up_var`; this incident does not establish the
historical missing-barrier bug as its cause.

Read-only BTF-offset and `/proc/kcore` inspection identified the sole remaining
RPC task, debug ID **62544**, as an asynchronous rename owned by original worker
PID 3315675:

```text
reservations/sparky/adaptive/.cpu-sample.json.3315675.4c19366e55fc418c8dc32b3925052a16.tmp
  -> reservations/sparky/adaptive/cpu-sample.json
```

Its target dentry had `d_fsdata=1`, and `nfs_renamedata.cancelled=1`. The original
rename was already stalled before the worker was killed: cancellation did not
establish the initial cause. It left an asynchronous operation alive whose
completion still controlled the replacement worker's lookup. The NFS client
had `cl_state=0`, no occupied session slots, and advancing lease renewals.
The RPC retried from `rpc_prepare_task` on `delayq`.

A separate 35-second trace instance, filtered to that exact old/new filename
pair, captured two completions **15.359 seconds apart** with error **-10008**:
NFSv4 `NFS4ERR_DELAY` (the generic NFS trace renders its legacy name `JUKEBOX`).
This is a retry response, not evidence of an absent transport or session slot.
Server state then established the conflict:

| Server inode | State | Delegation owner |
|---|---|---|
| `00:36:874593`, `adaptive/cpu-sample.json` | `DELEG BREAKING UNLCK`, read delegation | client 9, Sparklina `10.100.99.2`, stateid `0000000121c79d6a9fdaa64bf7874200` |
| `00:36:854449`, `workers/dl380g10.json` | `DELEG BREAKING UNLCK`, read delegation | client 9, stateid `0000000121c79d6a9fdaa64bc9bc4500` |

Sparklina retained both matching delegations with only the `REFERENCED` flag,
without `RETURN` or `RETURNING`. Its NFS client state was zero and it had no
outstanding RPC tasks. The server reported the callback channel UP and current
renewals but no outstanding callback RPC tasks. Why these breaking delegations
had not been returned remains unresolved; the snapshots do not establish where
the recall was lost. Client expiry would have revoked 1,015 read delegations,
59 write delegations, and 490 read opens in this snapshot, so it was not used.

The narrower recovery uses the normal Linux NFS open path. In
[`nfs4proc.c` at v6.17](https://github.com/torvalds/linux/blob/v6.17/fs/nfs/nfs4proc.c),
`_nfs4_do_open` calls `nfs4_return_incompatible_delegation` before OPEN. An
existing read delegation cannot satisfy `FMODE_WRITE`, so this path performs
DELEGRETURN. On Sparklina, the operator verified regular-file device/inode
identity, then opened each of the two authorized paths with
`O_WRONLY | O_CLOEXEC | O_NOFOLLOW` and immediately closed the descriptor.
There was no create, truncate, data write, rename, unlink, or chmod in the
recovery script.

At **14:49:12.882 UTC**, the CPU-sample open completed in **0.561 ms**. Its
immediate before/after inode, size, mtime and SHA-256 were unchanged:
`e1664e7e1cee7102d6897fb83ebce0da4e0976721d0e346e3cbfb168c0ca294f`.
The owner-side trace captured DELEGRETURN **error=0** for inode 874593. The
Sparky trace then captured the original rename **error=0**. Replacement worker
3473413, which had a pending SIGKILL by this stage, exited; both its local
admission FLOCK and NFS transition lock disappeared. No further signal was
needed for this recovery.

At **14:49:46.108 UTC**, the worker-offer path was still breaking and received
the same scoped open/close. Resumed server-local PrismaBuild writers replaced
inode 854449 during that call; the opened inode was already 855626. Its
immediate post-read returned `ESTALE` amid those replacements, which is retained
as a failed verification attempt, not represented as a pass. A subsequent
normal read succeeded with a current `announced_unix` value. No owner-side
DELEGRETURN trace was retained for this second call because its trace window
had ended.

Final direct Sparky reads of the exact CPU-sample and DL380 worker-offer paths
completed in **0.216 ms** and **1.399 ms** respectively. The CPU sample had a
new current writer timestamp and different inode/bytes after resumed PB
updates; these were not bytes written by the recovery open. `/proc/3473413`
was absent, both recorded lock inodes were absent from `/proc/locks`, and the
original RPC task was gone. The server no longer reported any BREAKING
delegation in the final snapshot. Both original NFS clients 7 and 9 remained
confirmed. The isolated trace instances were removed and existing global and
rasdaemon tracing was preserved. The coordinator separately owns supervisor
restoration and subsequent admitted-work validation.

Evidence under `/home/rob/tmp/astra-review-20260906/` is indexed, sized and
SHA-256 hashed by `nfs-recovery-artifact-manifest.json`. Principal artifacts:

- `nfs-kcore-rename.json`, `nfs-sparklina-kcore-delegations.json`, and
  `nfs-server-client-states.txt`: exact RPC/dentry/delegation attribution.
- `nfs-rename-trace-20260907.txt`: repeated pre-recovery DELAY responses.
- `nfs-cpu-delegation-recovery.jsonl` and
  `nfs-worker-delegation-recovery.jsonl`: operation identities, timings,
  contents/SHA snapshots, including the second call's failed post-read.
- `nfs-delegreturn-recovery-trace.txt` and `nfs-rename-recovery-trace.txt`:
  successful protocol completion and the original rename's terminal result.
- `nfs-server-recovery-verified.txt`: no remaining BREAKING delegation and
  preservation of the two client identities.

These are live incident measurements and a scoped manual repair, not a new
PrismaBuild runtime recovery policy or a tested general NFS fix. The proposed
caller boundary above remains unimplemented. No tests were run for this
documentation-only follow-up.
