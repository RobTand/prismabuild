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

Two instruments, because they answer different questions and only one of them
survives a sick mount.

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

Read together they answer "was the mount the constraint?" from data: a slow
probe with fast RPCs locates the fault on this client, a slow probe with slow
RPCs locates it at the server, and a fast probe says the box was busy rather
than blocked, whatever its load average claimed.

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
perfectly healthy network mount.

## Running it

One reading, for a human:

```
python3 tools/fleet/mount_latency.py --once --json
```

As a netdata external plugin, which is where the series and its retention come
from. Netdata already runs on every box, so the collector itself is one symlink
rather than a store somebody has to build and back up — but netdata runs its
plugins as the unprivileged `netdata` user, and that user owns neither the
probe directory nor `/home/rob/tmp`. Both have to be granted, once per box,
before the symlink means anything:

```
# the probe directory: the plugin can create its own anchor but not its own
# parent, and a failed mkdir makes every sample report status=error
install -d -o netdata -g netdata \
        /mnt/shared/prismabuild-fleet/mount-probe/$(hostname -s)

# the box-local record: append_record returns None rather than raising when it
# cannot write, so an unwritable directory is a silent loss of the backstop
install -d -o netdata -g netdata /var/lib/netdata/prismabuild

ln -s /mnt/shared/prismabuild-fleet/repo/tools/fleet/mount_latency.py \
      /usr/libexec/netdata/plugins.d/mount_latency.plugin
```

with `--record-dir /var/lib/netdata/prismabuild` in the plugin's argument list.
Check the first sample rather than assuming: a plugin that cannot reach the
mount still emits charts, and its `probe_status` will read `error` on every
one of them. That is the tool working correctly and the install being wrong,
and it is exactly the failure this document exists to make legible.

The plugin takes its interval as netdata's first positional argument and
publishes five charts under the `prismabuild` family: metadata latency per
operation, RPC rate, time per RPC split into queue and rtt, queue share, and
the probe's own outcome. The last one matters — a wedged mount must appear in
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

Both instruments are client-side, on the box being measured. They are
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
