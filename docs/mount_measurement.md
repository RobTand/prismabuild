# Measuring the shared mount

`tools/fleet/mount_latency.py` samples the filesystem every claim, offer,
receipt and CAS read crosses. It runs per box, because the number it produces
is a property of one client's path to the storage and not of the storage.

## Why this exists

On 2026-09-06 one Spark's NFS client entered an NFSv4.1 state-recovery retry
loop. `TEST_STATEID` reached 4,784 calls/s and 97.7% of the box's NFS traffic,
saturating the session slot table, so every real operation waited 100–180 ms
*to be transmitted* while the server answered in 0.3 ms. `RENAME` — the claim
operation — cost 165 ms. PrismaBuild admitted, offered and claimed on that box
exactly as it did on a healthy one, because nothing sampled the medium and a
box that is slow is indistinguishable from a box that is busy when nobody
measures it. Diagnosing it took a from-scratch investigation, because no
series existed to look up.

## What it measures

Three instruments, because they answer different questions, only one of them
survives a sick mount, and the third catches a failure the other two call
healthy.

**Where the time went** — `/proc/self/mountstats`, per mount and per NFS
operation, differenced over a stated window. Each operation carries queue time
and round-trip time separately, and that split is the diagnosis:

| `GETATTR` | queue | rtt | per-op share | reading |
|---|---|---|---|---|
| storm, 2026-09-06 16:37 | 142.47 ms | 0.32 ms | 0.998 | this client cannot send |
| healthy, 2026-09-06 21:45 | 0.002 ms | 0.031 ms | 0.06 | the medium is not the constraint |

`queue_share` is that ratio, and the two states separate by two orders of
magnitude rather than by a margin somebody has to pick.

Read it per operation. The top-level `queue_share` is an aggregate over the
whole mount, and on the storm it reads **0.912, not 0.998** — because
`TEST_STATEID` was 97.7% of the calls and its own queue time was *zero*. The
storm operation is what fills the session slot table; it does not wait in it.
So the sickest mount we have on record dilutes its own headline number with
the very traffic that made it sick, and only `by_op` shows the 165 ms `RENAME`
that actually stalled the claim path. Threshold on operations you care about.

This leg reads one procfs file and costs the mount no operation at all, so it
keeps reporting when the mount does not.

**What it actually cost** — a `stat`, an `open`, a bounded `listdir` and a
create/rename/unlink, timed against the mount in a box-private directory. The
rename is there because `RENAME` is the claim path and a read-only probe would
time everything except the operation the queue depends on.

**Who is queued behind the admission gate** — `/proc/locks`, filtered to the
`flock` files under `/tmp/prismabuild-admission-<uid>/`, split into holders and
waiters, with each process's state and `wchan`.

This leg is not about the mount at all, and that is why it is here. PrismaBuild's
admission gate is a *local* `flock` (`adaptive_cpu.py` `locked()`) around the
whole of `pool.py` `_claim` — the `ready/` scan, the record rename, the lease
write, the token renames, every one of them on NFS (#266). It *was* the
conversion point: a mount that was merely slow became a local queue, and one
process waiting on one remote peer starved every other loop on the box.

#267 closed that conversion. The acquisition is now `LOCK_NB`: a loop that
finds admission busy raises `AdmissionBusy`, returns to the top of its poll and
announces, and tries again on the next tick. The critical section is unchanged
and still on the mount, so this leg still matters — but it has swapped which
number carries the signal. A long-held lock is the mount doing something to
this box; a *waiter* is a regression, because nothing should be queueing here
any more.

That is what happened to dl380g10 on 2026-09-06, and it is the incident #267
fixed: 15 of 16 worker loops in `locks_lock_inode_wait`, one holder in
`__break_lease` waiting for a remote client to return an NFS delegation,
nothing served at all. The box announced nothing while it waited, so a box that
was merely blocked looked exactly like a box that had died. **Its load average
read 1.13.**

Not bad luck — structural. A blocking `flock` sleeps *interruptibly*, and load
average counts only running and uninterruptible tasks, so a total admission
stall moves load by approximately nothing. Measured on sparky through
PrismaBuild, action key `463aacd60ceb`: 15 fully blocked processes took `load1`
from 0.24 to 0.30, with **zero** waiters in D state. Every load-based health
check on this fleet is blind to this failure by construction, which is why
`waiters_invisible_to_load` is reported as a first-class number and why `load1`
is recorded beside it — the pair is the finding, and either number alone is
misleading.

Read together the three answer "was the mount the constraint?" from data: a slow
probe with fast RPCs locates the fault on this client, a slow probe with slow
RPCs locates it at the server, a fast probe says the box was busy rather than
blocked whatever its load average claimed — and a fast probe *with a queue at
the gate* says the box was neither, it was starved, and the mount is innocent.

## Reading the output

Prefer `by_op` to the aggregate means. The aggregate is composition-sensitive:
driving 65,000 negative `LOOKUP`/s at the mount moved the all-ops mean
round-trip time *down*, from 0.52 ms to 0.03 ms, because the added operations
are cheaper than the baseline mix. The mount got busier and the headline number
improved. `by_op` reported the same window honestly.

`probe_rpcs_upper_bound` is a bound, not a cost. `mountstats` counts the whole
client, so anything else touching the mount while the probe ran is counted in.

The three boxes are not symmetric and must never be given one number.
dl380g10 exports this filesystem and reaches it as local ZFS, so it has a
probe reading and no RPC statistics at all; a zero there would read as a
perfectly healthy network mount. It still gets the gate reading, deliberately:
it has no RPC leg and it is the box that actually stalled.

One implementation note with teeth: `/proc/locks` prints the device as
`%02x:%02x`, zero-padded. `/tmp` is tmpfs on dl380g10 — **major 0, written
`00`** — so an unpadded key would have matched nothing and reported a gate with
no holders and no waiters, which is indistinguishable from a healthy one. On
sparky, where `/tmp` is major 259 (`103`), the bug is invisible. It would have
blinded this leg on exactly the box it was built for, and on no other.

The collector now compares the device numbers numerically. On btrfs,
`stat` can name a subvolume's device while `/proc/locks` names the superblock's,
so even correctly padded device numbers can differ. An inode number alone
cannot resolve this in a passive census: the watched file may be unlocked
while another filesystem holds a lock on that number.

For an alias, the collector reads the holder's procfs `fdinfo`, mount namespace,
mount table and descriptor **link text**. The descriptor must carry that lock
and inode, name the watched absolute path in the collector's mount namespace,
and belong to the visible mount at that path. The path check distinguishes
nested subvolumes even when their mount table entry and inode numbers coincide.
Descriptor and namespace evidence are rechecked for changes. No descriptor
target is opened or followed, and no lock is acquired. Waiters attach to the
proved holder's kernel lock-record group, rather than every matching inode.

Missing permissions, ambiguous evidence, changed descriptors, hidden/deleted
paths, and scan limits leave an alias unverified. Hardlink or symlink spellings
that do not match the holder's descriptor path are also unverified. The JSON
gate record then sets `identity_complete: false`; its counts describe only the
identified rows, the human output says `gate=(identity unverified)`, and no
gate/hold Netdata sample is emitted for that reading. Mount/probe/RPC samples
continue. Do not read an incomplete census as a healthy zero.

The extra work applies only to device aliases and is bounded to 32 holder
processes, 256 descriptors per holder and 1 MiB per procfs record. It is not
included in the historical collector-cost measurements below. The ordinary
exact-device path retains its single `/proc/locks` read.

`max_hold_s` is a **lower** bound. `/proc/locks` carries no timestamp, so the
age is accumulated across samples and quantised by the interval; a hold that
begins and ends between two samples is not seen. That is enough for the failure
it is for — the gate is meant to be held for the length of a few renames, so
any reading in seconds is already a defect, and the incident this catches
lasted hours.

## Running it

One reading, for a human:

```
python3 tools/fleet/mount_latency.py --once --json
```

As a netdata external plugin, which is where the series and its retention come
from. Netdata already runs on every box, so the collector itself is one symlink
rather than a store somebody has to build and back up — but netdata runs its
plugins as the unprivileged `netdata` user, and that user owns neither the
probe directory nor `/home/rob/tmp`. Provision both before installing the
plugin. The probe must stay on the shared filesystem: moving it to local disk
would stop measuring the claim path.

### Provision the shared probe on the NFS server

On a root-squashed NFS client, even `sudo install -d -o netdata` cannot
change ownership on the export. Run ownership changes on **dl380g10, against
its local ZFS path**, using the **client's numeric Netdata UID and GID**.
Do not resolve `netdata` on the server for a client's directory: service UIDs
can differ across hosts (#314).

First collect these values on each client and retain them with the adoption
record. The collector uses Python's full hostname, not `hostname -s`:

```sh
hostname
id netdata
```

Then, in an administrator session on dl380g10, provision one client's directory
at a time. This example uses Sparky's observed identity; recheck the values
before applying it:

```sh
probe_host=sparky
probe_uid=983
probe_gid=983
probe_dir=/storage_pool/shared/prismabuild-fleet/mount-probe/$probe_host
findmnt -T /storage_pool/shared -o SOURCE,FSTYPE,TARGET
# Confirm local ZFS, inspect every parent and any existing directory/anchor.
# Coordinate existing probe users before changing ownership; reject symlinks.
ls -ld /storage_pool/shared/prismabuild-fleet/mount-probe "$probe_dir"
sudo install -d -m 0750 -o "$probe_uid" -g "$probe_gid" -- "$probe_dir"
# If an existing anchor is a reviewed regular probe file, transfer it too:
# sudo chown "$probe_uid:$probe_gid" -- "$probe_dir/anchor"
# sudo chmod 0640 -- "$probe_dir/anchor"
```

An absent directory in the inspection is expected for a first installation.
Provision a missing `mount-probe` parent separately with traversable permissions;
do not recursively chown the fleet, CAS, queue, or other hosts' probes. The
host directory and an existing anchor must be usable by that client's Netdata
identity. This ownership model limits the grant to the probe directory. It also
means an ordinary worker-user one-shot probe needs its own authorized writable
probe location; coordinate any existing worker-user collector before transfer.

Adding `netdata` to group `rob` is a different access policy: it grants access
to **all** group-rob-accessible files, not just this probe. Do not use it as an
implicit install prerequisite. A dedicated shared probe group requires a common
numeric GID and deliberate membership provisioning on the participating hosts.
World-writable probe directories are not required. Account group changes do not
update a running daemon's supplementary groups; inspect `/proc/<pid>/status`
and restart through the service manager when adopting an authorized change.

### Install the local plugin

On each client, in an administrator session, inspect existing paths before
installing; retain an existing plugin rather than overwriting it blindly:

```sh
sudo install -d -m 0750 -o netdata -g netdata /var/lib/netdata/prismabuild
sudo ln -s /mnt/shared/prismabuild-fleet/repo/tools/fleet/mount_latency.py \
      /usr/libexec/netdata/plugins.d/mount_latency.plugin
sudo systemctl restart netdata
```

Verify a sample from the actual Netdata process on **each** client: its
`probe.status` must be `ok`, its claim timing present, and its box-local JSONL
record growing. A standalone validation must run through published PrismaBuild
under the target UID and supplementary groups with declared CPU/memory demand;
an exit-zero `--once` alone is insufficient because failed probes are recorded
as data. Record the terminal result, logs, CAS receipt and payload as well as
the probe fields. Charts existing or a successful probe as `rob` do not prove
Netdata adoption. Keep #314 open until both GB10 clients have this evidence.

The published script is executable *and readable by Netdata's own UID*: a
published generation is world-readable, `0555` for the files Git records as
executable and `0444` for the rest, so the symlink resolves to bytes the
collector can run without being added to any group. See [what a published
generation permits](operating_prismabuild.md#what-a-published-generation-permits).
A generation published before that fix carries mode `0500` and the collector
cannot read it; the repair is the next publication, not a `chmod` on a sealed
generation.

Netdata's invocation
`mount_latency.plugin <update_every>` automatically selects protocol output.
In that mode the box-local record directory defaults to
`/var/lib/netdata/prismabuild`, so the symlink needs no extra arguments or
wrapper. An explicit `--netdata` selects the same mode when invoking Python
directly; `--record-dir` can override the directory. Human and JSON `--once`
reads retain their existing output and `/home/rob/tmp` record default.

The admission-lock leg must target the **worker's** directory. Its default
uses the collector's UID, so an unconfigured Netdata process would instead look
for its own locks. Set the existing `--lock-dir` option through Netdata's
[external-plugin command options](https://learn.netdata.cloud/docs/developer-and-contributor-corner/external-plugins)
in `/etc/netdata/netdata.conf`, using the actual worker UID (`id -u rob`)
or its configured `PRISMABUILD_BOX_STATE_ROOT`. For worker UID 1000:

```ini
[plugin:mount_latency]
    update every = 15
    command options = --lock-dir /tmp/prismabuild-admission-1000
```

That selects the correct path but does not grant access. PrismaBuild keeps
this directory worker-owned at mode `0700` and repairs broader permissions.
Do not loosen it for the collector. Under an ordinary separate Netdata UID,
the admission-gate measurement is therefore unavailable: the JSON record says
`locks.present: false`, and no gate samples are emitted. Mount/probe/RPC output
and the local JSONL record still work. Retained gate samples need a separately
approved collector identity or reader arrangement; this installation does not
provide one. Run `--once --lock-dir <worker-directory>` as the worker user for
an accessible one-shot gate reading, and verify `locks.present` before claiming
that the gate charts have been adopted.

Even with an accessible lock directory, `/proc/<pid>/wchan` is gated by
`ptrace_may_access`, so running as `netdata` yields `"0"` for every process it
does not own and `waiter_wchans` reads `{"0": N}`. The **counts and states are
unaffected by that wchan restriction** — `/proc/<pid>/stat` is world-readable —
so the headline
(`waiters`, `waiters_invisible_to_load`, `max_hold_s`) is intact and only the
"waiting on *what*" attribution is lost. Run `--once` as the loops' own user
when you need to tell `__break_lease` from slow work.
Check the first sample rather than assuming: a plugin that cannot reach the
mount still emits charts, and its `probe_status` will read `error` on every
one of them. That is the tool working correctly and the install being wrong,
and it is exactly the failure this document exists to make legible.

The plugin takes its interval as netdata's first positional argument and
publishes seven charts: five in the `latency`/`rpc` families — metadata latency
per operation, RPC rate, time per RPC split into queue and rtt, queue share, and
the probe's own outcome — and two in a separate `gate` family for the admission
lock's holders/waiters and hold age. The families are separate because the gate
is not the mount, and the whole point is that one can be sick while the other is
fine. The probe-outcome chart matters — a wedged mount must appear in
the series as an incident, not as a gap where the incident was.

A box-local JSONL copy is written alongside, capped at two generations of
16 MiB — a backstop for the delay between an incident and somebody coming to
look, not the archive. It is deliberately never written to the mount being
measured.

## Cost

Measured on sparky at a 2 s interval, which is eight times more often than the
intended scrape:

| | per sample |
|---|---|
| wall clock | 25 ms median, 521 ms worst |
| NFS RPCs | ≤ 31 median |
| RPC time | ≤ 22 ms median |
| files left on the mount | one `anchor`, reused |
| gate leg | one `/proc/locks` read + ≤ 32 `wchan` reads, no mount I/O |

At a 15 s scrape that is under 0.2% of one core and about 2 RPC/s, against a
baseline of 20–300 RPC/s idle and 65,000 RPC/s under load. Cheap enough to
leave on, which is the only kind of measurement worth having: one that runs
only when somebody already suspects a problem cannot tell you when the problem
started.

## Boundedness on a wedged mount

A hard NFS mount does not fail, it waits, and a `stat` on a wedged one blocks
uninterruptibly — no signal, no thread timeout, no `SIGKILL` reaches it. So the
syscall leg runs in a forked child that the parent abandons at a deadline and
records as `timed_out`.

At most one child is ever outstanding. While one is unreaped the next sample
skips the syscall leg entirely and reports `wedged`, which is the strongest
statement this tool makes, not a degraded one. Probing anyway would add a
blocked process every scrape — 240 an hour, all of them waiting on the thing
being measured. The procfs leg keeps reporting throughout.

Nothing here takes a lock. A probe that serialises against a wedged peer is a
probe that wedges.

## What this does not do

It does not watch the gate on any box but this one, so "the fleet is starved"
needs the recorded series from every box compared, which is the same missing
piece as above.

The mount instruments are client-side, on the box being measured. They are
independent — `mountstats` and `/proc/net/rpc/nfs` are separate kernel
counters, and cross-checking them is what validated the RPC rate to 0.3% — but
they are two views from one end of the wire. Nothing here reads dl380g10's
disk or network series, so "the server was slow" is a conclusion this tool
supports by elimination (probe slow, queue time low, so the wait was not on
this client) and never by direct evidence. Pairing it with the exporter's own
series is the obvious next leg and is not built.

It does not decide anything. Refusing or deprioritising admission on a box
whose latency is out of line with the fleet is a policy decision that belongs
to the queue, and it needs the fleet-relative comparison that only the recorded
series can give it. That half is not built.
