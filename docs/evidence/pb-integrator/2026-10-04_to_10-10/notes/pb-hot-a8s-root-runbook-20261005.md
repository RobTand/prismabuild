# Runbook: put the A8S release on the stage NVMe at its original path (root steps for the CEO)

Author pb-integrator, 2026-10-05 13:00Z. NOTHING HERE HAS BEEN RUN. It follows the CEO's go and the throwaway test
(`/mnt/shared/fleet-ceo/nvme-same-path-test-tools/results-ceo-20261005/`). Part 0 (making room) uses the mechanism the CEO approved (dec-1005-125354-6e19, option B); its code is being built and
reviewed, so Part 0's exact commands are filled in when that lands. Parts 1-5 are ready.

## What it does, in one paragraph

A read-only copy of the 128-file A8S release (163.47 GiB) goes on a new dataset on the stage NVMe, is verified byte for byte,
and is mounted over the original directory on the server and, by an explicit NFS overlay mount, on both Sparks. Every host
then reads the same paths from NVMe. The HDD originals stay underneath, untouched, and come back on unmount. The ship window
reloads A8S on every leg, so it gains most (derived: about 26 min from HDD against about 4 min from NVMe per load; the
measured single-stream NVMe read over NFS-RDMA was 852 MB/s).

## Facts this plan rests on (all read by me, none assumed)

| fact | value | where I read it |
|---|---|---|
| directory | server `/storage_pool/shared/tessera-runs/moe/glm53-a8-bf16menu-20260930/release/exported`, same path under `/mnt/shared` on every host | `ls`, `findmnt` |
| files | 128 regular files, 175,527,382,717 bytes (163.47 GiB), owner rob:rob, modes 644 (120) and 664 (8) | `stat` |
| mtimes | all 128 have non-zero nanoseconds | `stat` |
| rsync | 3.4.1; I tested that `rsync -a` keeps a 9-digit nanosecond mtime exactly | test on a temp file |
| manifest | `/mnt/shared/tessera-measurements/window4-946/a8s-content-manifest.json`: 128 entries of name, bytes, sha256; the bytes sum to the directory's exact size | read. **You named it `t8-v1-a8s-content-manifest`; I found no file by that name, so please confirm this is the one.** |
| stage pool | `prismabuild-stage` 744 GiB: `prewarm` uses 647 GiB and has only 9.98 GiB available (the `pbqueue` reservation and ZFS slop take the rest) | `zfs list` |
| server mounts | `/mnt/shared` and `/storage_pool/shared` are the same ZFS dataset in the SAME mount peer group (`shared:191`), and `pb-queue` already appears at both paths with one peer group (`shared:300`) | `/proc/self/mountinfo` |
| precedent | `pbqueue` fstab line and the Sparks' RDMA mount options, as in my first runbook | server `/etc/fstab`, Sparks' `findmnt` |

## Corrections and deviations from your plan (please read)

1. **No separate rbind on dl380g10.** My first runbook said a dataset mounted under `/storage_pool/shared` would NOT show under
   `/mnt/shared` on dl380g10. That was wrong: I inferred it from the word `rbind` in fstab. The two paths share a peer group, so the
   mount should propagate, exactly as `pb-queue` did. Step 1.8 checks it, and has the fallback if it does not.
2. **`primarycache=metadata` instead of `all` (my recommendation, your call).** A 163 GiB sequential read cannot stay in a 64 GiB
   ARC, so data caching would give almost no hits and would push out the ARC contents other jobs use. NVMe is fast enough
   without it. `zfs set primarycache=all prismabuild-stage/hot-a8s` is an online change if you disagree.
3. **The dataset is made read-only after the copy and exported read-only to the Sparks only.** Other NFS clients keep seeing
   the HDD originals; step 2.6 checks that from celestia.
4. **Reservation.** The dataset gets a reservation equal to its quota. PrismaBuild reads its stage capacity from the `prewarm`
   dataset's `available`, which already withholds sibling reservations, so the tier's ledger shrinks by the same amount without
   anyone editing it.

## Rules from your test and from kernels' audit (hold every one)

- **Never rely on the server mount plus export alone.** Your test: with the dentry cached and busy, both Sparks kept the HDD copy.
  Only the explicit client overlay is deterministic.
- **Mount and unmount ONLY between windows.** The no-mount interval is the whole job or window, including held files and mmaps:
  the A8S arms reopen the same shards. Held handles survived mount and unmount in your test, but the per-job identity checks and
  the two inode-keyed digest caches make a change during a job a hazard. Part 1.6 is the gate.
- Do not run any step while a gang, measurement or ship-window row is pending, admitted or running.
- The copy changes inode and ctime. Tessera's `source_digest_cache.py` keys on inode and ctime and will miss; Part 3 seeds it.

## Variables (use on every host)

```
P=tessera-runs/moe/glm53-a8-bf16menu-20260930/release/exported
SRC=/storage_pool/shared/$P          # the server's path (HDD original)
D=/mnt/shared/$P                     # the same path as every host sees it
T=/mnt/shared/fleet-ceo/nvme-same-path-test-tools
MAN=/mnt/shared/tessera-measurements/window4-946/a8s-content-manifest.json
WORK=/mnt/shared/fleet-ceo/hot-a8s-20261005     # logs and tables; I create it as rob
```

## Part 0: room (PrismaBuild's own eviction): `tier_loop.py --once --evict-gib N`. APPROVED OPTION B; code not yet delivered

Why this and not `rm`: held tier tokens must equal bytes on the stage at every instant, and PrismaBuild evicts only under pressure from a live consumer's window, which does not exist now.
So a one-shot of the tier loop is given the pressure by the operator and runs the SAME orphan sweep the live loop runs. The CEO's conditions, each of which I hold the change to:

1. It evicts only orphaned ranges that no live consumer names, exactly as the normal sweep does (the beyond-horizon pass, which touches live consumers' landed ranges, is not allowed to evict in this mode).
2. An independent pool review happens before anyone runs it.
3. A dry-run listing comes first and the CEO sees it.
4. The real run is done with the tier role stopped for the shortest possible time, then the role is restarted and the ledger is checked: held tokens equal bytes on the stage.
5. Not during Window 4.

Status: Flash worker `TierLoopEvictGib` is building it on branch `sdk4/evict-gib-20261005` from the live commit `04f6f00e0410` (not main). Then my review, then an independent pool review, then I will
put the exact commands here: the dry-run, the role stop (record and recheck PID, start time and command line; `kill -TERM` that exact PID; start the one-shot at once so it takes the role's singleton lock
before the supervisor respawns the role, which on dl380g10 runs as rob under `supervise.py`; a refusal is harmless and is retried), the run, the confirmation that the role is back, and the ledger check.

Gate to enter Part 1: on dl380g10 `zfs get -Hp -o value available prismabuild-stage/prewarm` is at least 180 GiB (193,273,528,320 bytes). Today it is 9.98 GiB. Do not create the dataset first and hope; the reservation in step 1.2 fails if the room is not there. Create it
immediately after the eviction, before any consumer can ask the tier for the room again.

## Part 1: dl380g10

Steps marked (rob) need no root. Steps marked ROOT do.

**1.1 (rob) Capture the original's stat table, before anything is mounted over it.**
```
mkdir -p $WORK && python3 $T/hot_a8s_verify.py snapshot $D $WORK/source-stat.json
```
Expect: `snapshot: 128 files, 175527382717 bytes`.

**1.2 ROOT Create the dataset** (properties match `/stage/prewarm`, plus the read-only and caching choices above; 172 GiB covers 163.47 GiB
of data plus metadata):
```
zfs create -o mountpoint=legacy -o recordsize=1M -o compression=off -o atime=off -o xattr=sa -o acltype=off \
  -o primarycache=metadata -o secondarycache=none -o quota=172G -o reservation=172G prismabuild-stage/hot-a8s
zfs get -H -o property,value quota,reservation,recordsize,compression,primarycache,readonly prismabuild-stage/hot-a8s
```
Expect `172G`, `172G`, `1M`, `off`, `metadata`, `off`. STOP if `zfs create` fails for lack of space: Part 0 is not done.

**1.3 ROOT Copy through a temporary mount, so the original directory is never covered while it is read.**
```
mkdir -p /mnt/hot-a8s-fill && mount -t zfs prismabuild-stage/hot-a8s /mnt/hot-a8s-fill
nice ionice -c2 -n7 rsync -a --numeric-ids --info=progress2 $SRC/ /mnt/hot-a8s-fill/ 2>&1 | tee $WORK/rsync.log | tail -3
```
Expect rsync to end with no error and no "vanished" or "some files" line, and 128 files. The HDD side reads at about 111 MB/s cold
(about 26 min); the CEO's pre-warm may make it faster. STOP on any rsync error and send me the log.

**1.4 (rob) Verify the copy at the temporary mount** (size, mtime to the nanosecond, mode and owner against step 1.1, sha256 against the manifest, exact file set):
```
python3 $T/hot_a8s_verify.py verify /mnt/hot-a8s-fill --manifest $MAN --stat-table $WORK/source-stat.json \
  --workers 4 --report $WORK/verify-server-fill.json
```
Expect the last line `RESULT: PASS: 128 files, 175527382717 bytes hashed`. STOP on FAIL and send me the output. (Tested on a small copy: a flipped byte, a
second-truncated mtime and an extra file are each reported and exit 1.)

**1.5 ROOT Make it read-only and release the temporary mount.**
```
zfs set readonly=on prismabuild-stage/hot-a8s && umount /mnt/hot-a8s-fill && rmdir /mnt/hot-a8s-fill
```

**1.6 BETWEEN-WINDOWS GATE (ROOT, on dl380g10, sparky and sparklina; all three must be clean).**
```
lsof +D $D 2>/dev/null | head        # expect NO output: nothing holds a file under the directory
pbstatus.py shows no gang, measurement or A8S-reading row pending, admitted or running   # I check this with you; say "clear" to me
```
Do not continue unless all three hosts print nothing and the fleet shows no such row.

**1.7 ROOT Persistent server mount, then mount over the original** (same line shape as `pbqueue`):
```
cp -p /etc/fstab $WORK/fstab.before
echo "prismabuild-stage/hot-a8s /storage_pool/shared/$P zfs defaults,nofail,x-systemd.requires=zfs-mount.service,x-systemd.after=zfs-mount.service,x-systemd.before=nfs-server.service 0 0" >> /etc/fstab
systemctl daemon-reload
mount $SRC
findmnt $SRC ; ls $SRC | wc -l ; stat -c 'dev,ino=%d,%i' $SRC/config.json
```
Expect a `zfs prismabuild-stage/hot-a8s` row, `128`, and an inode that differs from the HDD's. Write the inode down.

**1.8 ROOT Does it show at the `/mnt/shared` path on dl380g10 (propagation)?**
```
findmnt $D ; stat -c 'dev,ino=%d,%i' $D/config.json
```
Expect the same `zfs prismabuild-stage/hot-a8s` source and the SAME inode as step 1.7. If `$D` still shows the HDD copy (a different inode),
propagation did not happen: run `mount --bind $SRC $D` as the fallback, add it to fstab, and tell me. STOP if neither works.

**1.9 ROOT Export it, read-only, to the Sparks only.** Append one line to `/etc/exports`, then export:
```
cp -p /etc/exports $WORK/exports.before
echo "$SRC 10.100.98.1(ro,sync,no_subtree_check,root_squash,fsid=13,mp) 10.100.99.2(ro,sync,no_subtree_check,root_squash,fsid=13,mp)" >> /etc/exports
exportfs -ra ; echo rc=$?
exportfs -v | grep -A1 glm53-a8-bf16menu
```
Expect the line with `fsid=13` and `ro`. You saw `exportfs` return rc 1 silently on the transient form; for this one, the check that matters is the listing (and
`grep glm53 /proc/fs/nfsd/exports`). STOP if the line is not listed. `fsid=13` is free again because the test export is gone; 11 and 12 are in use.

## Part 2: each Spark, sparky first, then sparklina (ROOT unless marked). `SRV=10.100.98.3` on sparky, `10.100.99.3` on sparklina.

**2.1 Gate again on this host (step 1.6 command).** Nothing may hold `$D`.

**2.2 Record the HDD view's identity (rob):** `stat -c 'dev,ino=%d,%i' $D/config.json`. Write it down.

**2.3 Persistent line, then the overlay mount.**
```
cp -p /etc/fstab $WORK/fstab.$(hostname).before
echo "$SRV:/storage_pool/shared/$P $D nfs4 ro,noatime,vers=4.2,rsize=1048576,namlen=255,hard,proto=rdma,nconnect=16,port=20049,timeo=600,retrans=2,sec=sys,local_lock=none,nofail,_netdev 0 0" >> /etc/fstab
systemctl daemon-reload
mount $D ; findmnt $D
```
Expect a second `nfs4` row at `$D` with `proto=rdma,nconnect=16`. If it hangs more than 30 seconds, interrupt and `umount $D`, then remove the fstab line.
(Why fstab and not a plain mount: it survives a reboot. `nofail` means a Spark that cannot mount it boots anyway and silently reads the HDD copy: correct
data, a digest-cache miss. Part 5 makes that visible.)

**2.4 (rob) The new view.**
```
stat -c 'dev,ino=%d,%i' $D/config.json      # expect: a DIFFERENT dev, and the inode equal to dl380g10's (step 1.7); not the 2.2 inode
```

**2.5 (rob) Full verification through this host's mount** (about 3 to 7 min each; the page cache is dropped behind the read):
```
python3 $T/hot_a8s_verify.py verify $D --manifest $MAN --stat-table $WORK/source-stat.json --workers 4 \
  --report $WORK/verify-$(hostname).json --fingerprints $WORK/fingerprints-$(hostname).json
```
Expect `RESULT: PASS: 128 files`. This also writes the exact stat identity Tessera's digest cache keys on, for Part 3.

**2.6 (rob, from celestia, once) Confirm a non-Spark client still sees the HDD originals.** `stat -c 'dev,ino=%d,%i' $D/config.json` there should show the HDD inode
(the Sparks-only export means celestia is not offered the child filesystem). If it shows the NVMe inode, tell me; it is harmless but I want to know.

## Part 3: the inode-keyed digest caches (no GPU; owners kernels and campaign, I hand over the exact input)

- **Tessera `source_digest_cache.py`** keys on leaf, inode, size, mtime_ns and ctime_ns. The copy's ctime is the copy time, so every key is new.
  `SourceDigestCache.adopt(path, digest, fingerprint=..., writer={"kind": ...})` seeds an entry without rereading, and requires the shard to be quiet for 300 s
  (its newest of mtime and ctime must be 300 s old) and the fingerprint to be re-takeable through the adopting host's own mount. The inode and times are equal on
  all three hosts, and `st_dev` is not part of the key, so adopting on ONE host serves all. Input: `$WORK/fingerprints-<host>.json` (sha256 plus the fingerprint, per file). Wait at least 5 minutes after step 1.5 before adopting.
  If nobody adopts, the first Window 4 or ship-window job rehashes 163 GiB once through the normal path: correct, just slower.
- **PrismaQuant `cost_streaming.py`** keys its source-checkpoint cache on path, device, inode, size, mtime and ctime. It is a different artifact (BF16 teacher source) and A8S is a Tessera export; kernels said Window 4 does not read the BF16 source. Campaign to confirm nothing in PrismaQuant reads this directory.
- Do not reuse an old stat fingerprint as if it still matched.

## Part 4: record, then tell me

Send me (or leave in `$WORK`): `source-stat.json`, `rsync.log`, `verify-server-fill.json`, `verify-sparky.json`, `verify-sparklina.json`, the dev,ino lines from steps 1.7, 1.8, 2.2, 2.4 on each host and 2.6, and `exportfs -v`. I will record them and mark the proposal's step done.

## Part 5: keep it honest afterwards

Add one assertion to the window preflight (kernels' side): on each host `findmnt -n -o SOURCE,FSTYPE $D` shows the hot dataset (server and dl380g10) or the `nfs4` overlay (Sparks). A silent fallback to the HDD is correct but slow, and
should be a visible warning, not a surprise. Also: before any future change to the A8S directory (it is meant to be immutable), unmount first and re-run Part 1.

## Rollback (ROOT; only between windows, with the step 1.6 gate clean on every host)

```
# each Spark
umount $D        # fails busy if anything holds a file: that is the safety; then cp -p $WORK/fstab.$(hostname).before /etc/fstab && systemctl daemon-reload
# server
cp -p $WORK/exports.before /etc/exports && exportfs -ra
umount $SRC      # (and umount $D on dl380g10 if you used the bind fallback)
cp -p $WORK/fstab.before /etc/fstab && systemctl daemon-reload
cat $D/config.json | head -c 40 ; stat -c 'dev,ino=%d,%i' $D/config.json      # expect the HDD inode from step 2.2 again
# keep the dataset for another try, or free the 172 GiB:   zfs destroy prismabuild-stage/hot-a8s
```
Your test showed the HDD view returns cleanly after unmount. The HDD files are never modified at any step.

Option A (the running loop reads an operator pressure request) is filed as a follow-up issue for a later release; it does not block this.
