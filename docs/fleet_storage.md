# Fleet storage host configuration

Host configuration is installed separately from a PrismaBuild runtime generation.
This record accompanies [issue #523](https://github.com/RobTand/prismabuild/issues/523)
and the [paced storage prewarmer](data_manifest_prewarm.md).

## Observed state, 2026-09-12

Maintenance independently read these settings at 18:32 UTC:

| Host | `/mnt/shared` source | NFS BDI | `read_ahead_kb` |
|---|---|---|---|
| sparky | `10.100.98.3:/storage_pool/shared` | `0:64` | 1024 |
| sparklina | `10.100.99.3:/storage_pool/shared` | `0:88` | 1024 |

Both clients use NFSv4.2 over RDMA, `rsize=wsize=1048576`, `nconnect=16`,
`hard,timeo=600,retrans=2,local_lock=none`. BDI numbers are mount-lifetime
identifiers; never persist either number in a command or rule.

The issue reports a serial reader limited to about one READ RPC in flight,
259 MB/s cold, and 9,348 MB/s with 32 readers. Those are the reporter's
measurements, not a maintenance before/after result. Independent readback
reproduced the 1 MiB window; it did not establish an RPC concurrency limit
for every reader or measure a speedup from changing it.

### DL380 write durability and cache layout

`zfs get` reports `storage_pool/shared sync=disabled` (source `local`),
`logbias=latency` (default), and mountpoint `/storage_pool/shared`.
`/etc/exports` still specifies `rw,sync` for the fleet clients.
`zpool status -P storage_pool` reports a four-member raidz1 (`sdb1` through
`sde1`), a cache vdev at
`/dev/disk/by-id/nvme-PCIe_SSD_19060410243242-part5`, and no log vdev.
The pool is ONLINE with no known data errors in that readback.

#523 records Rob's decision to retain `sync=disabled` on this UPS-backed host
and retain the consumer NVMe as L2ARC instead of converting it to a SLOG
without power-loss protection. The dataset setting is already persistent;
this maintenance change does not set it, change exports, or alter vdevs.
The UPS and drive power-loss protection rationale is attributed to the issue;
maintenance verified the property and topology, not that hardware protection.

`sync=disabled` ignores synchronous-write requests. An NFS `sync` export
therefore does not restore stable-storage acknowledgement on this dataset.
Acknowledged writes can be lost after a server crash or power loss before the
transaction group reaches stable storage. A five-second transaction-group
interval is not a guaranteed maximum loss window, and filesystem consistency
is not application durability. This is a host-specific tradeoff, not a default
for new storage servers. See the [OpenZFS sync property](https://openzfs.github.io/openzfs-docs/man/master/7/zfsprops.7.html#sync)
and [transaction-group implementation](https://github.com/openzfs/zfs/blob/master/module/zfs/txg.c).

## Pending client readahead change

The proposed window is 16384 KiB (16 MiB) on each Spark's `/mnt/shared` NFS
BDI. The [kernel BDI interface](https://www.kernel.org/doc/Documentation/ABI/testing/sysfs-class-bdi)
defines `read_ahead_kb` as the window, not an RPC-slot reservation.
A larger window may help serial buffered reads and file mappings but must be
measured with the actual consumer and its cache residency.

As of the readback above, **neither client is changed**: interactive root
authentication is required. Two 102 GiB GPU claims and 16 ready actions were
present, so isolated throughput and remount/reboot acceptance are also pending.
Issue #523 stays open through those checks.

### Install after reviewing the measurement window

Use a clean checkout of the merged change, staged locally on each Spark.
The helper is stdlib-only and defaults to read-only inspection. It resolves
the whole NFS export from `/proc/self/mountinfo`, ignores the autofs layer,
rejects wrong or ambiguous mounts, checks mount identity around the operation,
and reads the resulting value back. It changes only `read_ahead_kb`.

```bash
# From that local checkout, before installation (read-only):
/usr/bin/python3 -I fleet/storage/nfs_readahead.py

# In an authenticated root session on each Spark:
sudo install -d -m 0755 /usr/local/libexec
sudo install -o root -g root -m 0644 fleet/storage/nfs_readahead.py \
  /usr/local/libexec/prismabuild-nfs-readahead.py
sudo install -o root -g root -m 0644 fleet/storage/prismabuild-nfs-readahead.service \
  /etc/systemd/system/prismabuild-nfs-readahead.service
sudo systemctl daemon-reload
sudo systemctl enable --now prismabuild-nfs-readahead.service

# Readback, including the dynamically discovered BDI and applied value:
/usr/bin/python3 -I /usr/local/libexec/prismabuild-nfs-readahead.py
systemctl status --no-pager prismabuild-nfs-readahead.service
journalctl -u prismabuild-nfs-readahead.service --no-pager -n 20
```

The unit is wanted by and ordered after `mnt-shared.mount`, so it follows the
actual NFS mount instead of tuning the autofs pseudo-device. `BindsTo` plus
`After` stops it when the mount disappears; enabling the unit makes a later
mount start it again. This follows [systemd's unit dependency semantics](https://github.com/systemd/systemd/blob/main/man/systemd.unit.xml).
Do not unmount or reboot a host with active work to exercise persistence.
After the next planned mount cycle or reboot, retain a fresh helper readback,
service log and boot ID before recording persistence as verified.

To undo the setting on a mounted client:

```bash
sudo systemctl disable --now prismabuild-nfs-readahead.service
sudo /usr/bin/python3 -I /usr/local/libexec/prismabuild-nfs-readahead.py --apply --kib 1024
/usr/bin/python3 -I /usr/local/libexec/prismabuild-nfs-readahead.py --kib 1024
```

### Acceptance evidence still required

Use published `pbrun.py --measurement` for each complete paired experiment,
with its genuine host dependency, aggregate CPU/memory demand and native
threads bounded. Root installation remains a host operation; PB admission
does not grant a payload root access. Do not grant worker sudo privileges to
make the comparison possible. Coordinate the root setting transitions with
the admitted experiment and keep 1024/16384 arms interleaved. Preserve PB's
affinity, receipts and measurement isolation.

Use the same identified useful campaign inputs and serial consumer. Record
bytes, cache residency and readahead readback for each arm; a warm-cache arm
does not establish cold-read improvement. Collect a workload profile before
and after, client mountstats deltas (READ bytes/ops, RTT/op, execution/op,
retransmits and RDMA reconnects) and wall throughput. During the same windows,
retain DL380 iostat/Netdata disk utilization, await/backlog and NFS service
rate, plus the storage role's pacing records. Keep its 25% utilization,
10 ms read-await and 2000 ms backlog caps and its one-reader/one-lookahead
shape. Those caps govern the prewarmer's reads; they do **not** throttle the
clients' new readahead. Assess total server pressure and roll back a harmful
client change rather than weakening pacing.

For campaign acceptance, retain cumulative committed units, GPU power against
the device reference, CPU activity, memory residency and useful work per joule.
GPU utilization percentage alone is insufficient on GB10. Re-read terminal
states, immutable logs and CAS receipt payloads. An installed value, a passing
fixture, or a submission acknowledgement is not a measured fleet improvement.

## Provisioning checklist

- On the Sparks, record the actual NFS source/options, dynamically resolved
  BDI and readahead window. Install the unit from the reviewed local checkout
  when adopting #523, then verify both immediate and planned mount-cycle
  readbacks. Runtime publication does not install this host unit.
- On DL380, record `zfs get sync,logbias storage_pool/shared`, the export's
  `sync` option, `zpool status -P storage_pool`, and UPS state. Retain the
  existing `sync=disabled`/L2ARC/no-SLOG decision with its durability tradeoff;
  do not silently carry it onto a new server or treat L2ARC as a log device.
- Keep the [prewarm pacing and ARC budget](data_manifest_prewarm.md) and
  [worker admission policy](agent_execution_policy.md#adding-workers).
  Host tuning does not expand any CPU, RAM or GPU reservation.
