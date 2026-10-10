# Runbook: throwaway same-path NVMe test (root steps for the CEO)

Author pb-integrator, 2026-10-05. Everything marked ROOT needs root; everything else I (rob) already did or will do.
Nothing here touches any real artifact path. The only thing deleted anywhere is the throwaway test data, and only at
the very end if you choose.

## What the test proves

A copy of a directory on the NVMe stage pool, exported with its own `fsid` and mounted by the Sparks over the SAME
client path as the HDD directory, is what NEW opens see, while files already open keep working and the HDD copy is
untouched underneath. It follows the live `pb-queue` precedent exactly (read from the live systems on 2026-10-05).

## Paths (all throwaway)

| what | path |
|---|---|
| HDD copy, server side | `/storage_pool/shared/fleet-ceo/nvme-same-path-test` |
| same path on both Sparks | `/mnt/shared/fleet-ceo/nvme-same-path-test` |
| NVMe dataset | `prismabuild-stage/nvme-same-path-test` (quota 4 GiB; the stage pool has only 9.98 GiB free) |
| scripts, no root needed | `/mnt/shared/fleet-ceo/nvme-same-path-test-tools/` (`holder.py`, `readtest.sh`) |
| temporary server mount for filling | `/mnt/nvme-test-fill` |
| export id | `fsid=13` (the live exports use only 11 and 12) |

I already created the HDD-side data as rob: `big.bin` 1.5 GiB (sha256 `da0ab9a79dee9a39d09bafbea1feb6bee7f4321b9850f92e3e602ed2f880c2e0`),
`small1..3.txt`, `MANIFEST.sha256`, and `WHICH_COPY` containing `HDD copy (storage_pool, raidz1)`.

## Precedent facts I read (so you can check my copy of them)

- Server fstab: `prismabuild-stage/pbqueue /storage_pool/shared/prismabuild-fleet/pb-queue zfs defaults,nofail,x-systemd.requires=zfs-mount.service,x-systemd.after=zfs-mount.service,x-systemd.before=nfs-server.service 0 0` (dataset `mountpoint=legacy`).
- Server exports: `/storage_pool/shared/prismabuild-fleet/pb-queue 192.168.1.180(rw,sync,no_subtree_check,fsid=12,mp) 192.168.1.110(...) 10.100.98.1(...) 10.100.99.2(...)` and one `192.168.1.68(... insecure,fsid=12,mp,nohide)`.
- The Sparks' pb-queue client mount is a plain `mount` (no unit file, no fstab line), options exactly:
  `rw,noatime,vers=4.2,rsize=1048576,wsize=1048576,namlen=255,hard,proto=rdma,nconnect=16,port=20049,timeo=600,retrans=2,sec=sys,local_lock=none`.
  sparky mounts from `10.100.98.3`, sparklina from `10.100.99.3` (clients `10.100.98.1` and `10.100.99.2`).
- Both Sparks ALREADY mount `/stage/prewarm` and `/ram/prewarm` read-only over NFS-RDMA from the same server, so the stage NVMe is already reachable from the GPU boxes.
- On dl380g10, `/mnt/shared` is an `rbind` of `/storage_pool/shared`. A dataset mounted later under `/storage_pool/shared` is NOT automatically visible under `/mnt/shared` on dl380g10 itself; the test does not need it.

## Part 1: dl380g10 (ROOT)

1. Create the dataset (properties match `/stage/prewarm`, not the queue's 16K and lz4; `primarycache=metadata` keeps the test file out of ARC so a read of the NVMe copy is a real device read):
   ```
   zfs create -o mountpoint=legacy -o recordsize=1M -o compression=off -o atime=off -o xattr=sa -o acltype=off -o quota=4G -o primarycache=metadata prismabuild-stage/nvme-same-path-test
   ```
   Expect no output. Check: `zfs get -H -o value quota,recordsize,compression,primarycache prismabuild-stage/nvme-same-path-test` prints `4G`, `1M`, `off`, `metadata`.
2. Fill it from the HDD copy through a temporary mount, so the HDD directory is never covered while it is read:
   ```
   mkdir -p /mnt/nvme-test-fill
   mount -t zfs prismabuild-stage/nvme-same-path-test /mnt/nvme-test-fill
   rsync -a --exclude WHICH_COPY /storage_pool/shared/fleet-ceo/nvme-same-path-test/ /mnt/nvme-test-fill/
   echo "NVMe copy (prismabuild-stage)" > /mnt/nvme-test-fill/WHICH_COPY
   chown rob:rob /mnt/nvme-test-fill/WHICH_COPY
   (cd /mnt/nvme-test-fill && sha256sum -c MANIFEST.sha256)
   ```
   Expect `big.bin: OK` and the three small files `OK`. STOP if any is not OK.
3. Move the dataset onto the test path (legacy mount, like the queue):
   ```
   umount /mnt/nvme-test-fill
   mount -t zfs prismabuild-stage/nvme-same-path-test /storage_pool/shared/fleet-ceo/nvme-same-path-test
   findmnt /storage_pool/shared/fleet-ceo/nvme-same-path-test
   cat /storage_pool/shared/fleet-ceo/nvme-same-path-test/WHICH_COPY
   ```
   Expect the findmnt row to show source `prismabuild-stage/nvme-same-path-test`, and `WHICH_COPY` to say `NVMe copy`. The HDD files are now only hidden, not changed.
4. Add the export (read-only, Sparks only, `mp` needs the path to be a mountpoint, which step 3 made true). Append ONE line to `/etc/exports`:
   ```
   /storage_pool/shared/fleet-ceo/nvme-same-path-test 10.100.98.1(ro,sync,no_subtree_check,root_squash,fsid=13,mp) 10.100.99.2(ro,sync,no_subtree_check,root_squash,fsid=13,mp)
   ```
   Then `exportfs -ra` and `exportfs -v | grep nvme-same-path`. Expect the line with `fsid=13`. STOP if `exportfs` prints an error.

## Part 2: each Spark, run first on sparky, then repeat on sparklina. Run everything as root (the holder and the timed read need it; there is no sudo in these commands).

Use `SRV=10.100.98.3` on sparky and `SRV=10.100.99.3` on sparklina. `D=/mnt/shared/fleet-ceo/nvme-same-path-test`, `T=/mnt/shared/fleet-ceo/nvme-same-path-test-tools`.

A. BEFORE the overlay (as rob except the timed read):
   ```
   cat $D/WHICH_COPY                 # expect: HDD copy (storage_pool, raidz1)
   stat -c 'dev,ino=%d,%i' $D/big.bin    # write this down: the HDD view's identity
   nohup python3 $T/holder.py $D 900 > /tmp/holder-$(hostname).log 2>&1 &    # holds big.bin open for 15 minutes
   sleep 3; head -3 /tmp/holder-$(hostname).log        # expect: opened ... and recorded 64 sample block hashes
   $T/readtest.sh $D/big.bin hdd-warm                       # ROOT: bytes through the mount, sha256, dev,ino; the timing is WARM-CACHE (see the caveat)
   ```
   The sha256 must equal `da0ab9a79dee9a39d09bafbea1feb6bee7f4321b9850f92e3e602ed2f880c2e0`. Keep the holder running.
B. THE OVERLAY (ROOT; nothing may be reading `$D`; the holder holds a file on the PARENT mount, not on this one):
   ```
   fuser -vm $D 2>&1 | head        # expect no process listed other than the mount point itself
   mount -t nfs4 -o ro,noatime,vers=4.2,rsize=1048576,namlen=255,hard,proto=rdma,nconnect=16,port=20049,timeo=600,retrans=2,sec=sys,local_lock=none $SRV:/storage_pool/shared/fleet-ceo/nvme-same-path-test $D
   findmnt $D
   ```
   Expect a second nfs4 row at `$D` with `proto=rdma,nconnect=16`. STOP and `umount $D` if the mount errors or hangs for more than 30 seconds.
C. AFTER the overlay (rob, then ROOT for the timed read):
   ```
   cat $D/WHICH_COPY                 # expect: NVMe copy (prismabuild-stage)   <- new opens see the NVMe copy
   stat -c 'dev,ino=%d,%i' $D/big.bin    # expect a DIFFERENT dev,ino than step A (the identity question)
   (cd $D && sha256sum -c MANIFEST.sha256)    # expect all OK
   $T/readtest.sh $D/big.bin nvme-device                 # ROOT: a real NVMe device read over NFS-RDMA (primarycache=metadata)
   grep -v ' OK$' /tmp/holder-$(hostname).log | tail          # expect only the opened/recorded lines: NO ERROR and NO HASH-MISMATCH
   tail -2 /tmp/holder-$(hostname).log                         # expect fresh OK lines: the OLD handle still works through the overlay
   ```
D. REMOVE THE OVERLAY (ROOT):
   ```
   umount $D
   cat $D/WHICH_COPY                 # expect: HDD copy again
   tail -2 /tmp/holder-$(hostname).log   # expect OK lines still
   ```
   Let the holder finish (or `pkill -f holder.py`) and send me `/tmp/holder-$(hostname).log` and the output of every `readtest.sh`.

## Pass and fail

PASS needs ALL of: (1) after B, new opens read `NVMe copy` and the manifest verifies; (2) the holder logs no ERROR and no HASH-MISMATCH across B, C and D; (3) after D the HDD copy is visible again and unchanged (manifest OK); (4) both timed reads deliver the whole file: `serverreadbytes delta` is about 1536 MiB each time and the sha256 equals the manifest (the TIMINGS are recorded, not judged: see the caveat); (5) the dev,ino pair differs between the two views.
FAIL if: a mount hangs or errors, the holder shows ESTALE or EIO, a hash differs, or a Spark reports a stale handle on `ls $D` after the unmount. In every failure case run D on that Spark and stop.

## Timing caveat (read this before quoting a number)

I wrote the HDD copy at 12:10Z, so it is in the server's ARC (63.9 GiB in use of 64 at 12:15Z, partly because of the A8S pre-warm), and `hdd-warm` is a cache read, not a disk read. It must NOT be used to claim a speed-up. The valid NVMe number is `nvme-device` (the test dataset has `primarycache=metadata`, so the data comes from the device over NFS-RDMA). The valid HDD baseline is the earlier measurement: 111 MB/s pool-side under load, 27 MB/s per disk. This test proves MECHANICS (new opens, old handles, identity), not the gain; the gain is derived in the proposal.

## Rollback of Part 1 (ROOT, only when the test is over)

```
# remove the exports line you added (the one with fsid=13), then:
exportfs -ra
umount /storage_pool/shared/fleet-ceo/nvme-same-path-test
# keep the dataset for a rerun, OR delete the throwaway data (your decision):
#   zfs destroy prismabuild-stage/nvme-same-path-test
rmdir /mnt/nvme-test-fill
```

## What I will check once you send the logs

Holder logs across the mount transitions on both Sparks; the two timing runs against the HDD and NVMe copies; the dev,ino difference;
`zpool iostat` shows I can request from you during the nvme-cold read (stage pool busy, `storage_pool` HDDs idle) if you want it.

## Differences when this is used for a REAL artifact (not part of this test)

- The export would be `ro` and the mount persistent (an fstab line or a unit with `x-systemd.requires=mnt-shared.mount`), unlike the transient `mount` here.
- dl380g10's own jobs read through its `rbind` of `/mnt/shared`; they would still see the HDD copy unless the dataset is ALSO mounted at the `/mnt/shared/...` path on dl380g10.
- Never mount over a path while a job reads it; take the Spark workers idle first. A job's open files keep the HDD inode, new opens get the NVMe copy: a job that opens its shards at different times could see both.
- Inodes and `st_dev` change. I am asking campaign and kernels whether any identity binding includes them.
- The stage pool has 9.98 GiB free to datasets; a 163.47 GiB artifact needs room made through PrismaBuild's own eviction first (632 GiB are evictable per its ledger).

### Added 12:25Z from kernels' identity answer
- Treat the whole job or window as the no-mount interval. Held FDs and mmaps count, and the A8S arms reopen the same shards between arms, so the "mount only between windows" rule is essential for a real artifact.
- The copy changes inode, ctime and, unless preserved, mtime_ns. Tessera's `source_digest_cache.py` (key: leaf, inode, size, mtime_ns, ctime_ns) and PrismaQuant's `cost_streaming.py` source-checkpoint cache will miss and rehash. Plan their repopulation off the GPU. Content hashes are unaffected.
- Preserve nanosecond mtimes in the real copy (some banks pin them) and verify them, as well as sha256.

