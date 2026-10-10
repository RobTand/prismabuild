#!/usr/bin/env python3
"""Gate probe for prismabuild#1730: speed, space, bind mount.

Runs as a PB-admitted action child on one Spark (host pinned at submit:
disk and NFS paths are host-local facts). It measures nothing on the
coordinator. Standard library only; the sealed checkout carries it.

Three legs:
  space: statvfs of / and the Docker root, D1 verdict for the A8S set.
  speed: 1/4/16-stream client-cold and warm reads of a fixed byte total
    (16 files x 512 MiB prefix = 8 GiB) from the stage tier
    (/stage/prewarm, NFS ro), the HDD pool (/mnt/shared), and a matching
    host-local NVMe file set. Every arm reads the same bytes with one
    file per thread at 16 streams, so no thread idles.
  bind:  Docker nested bind over a subdirectory of -v /mnt/shared:/mnt/shared.
"""
from __future__ import annotations

import argparse
import concurrent.futures as futures
import json
import os
import socket
import subprocess
import sys
import tempfile
import time

GIB = 1024 ** 3

#: Exact canonical A8S bytes (du -sb, 2026-10-10): 163.47 GiB.
A8S_BYTES = 175527382717

#: D1 (fleet-diskcheck): 5% free floor; a write of N needs 1.5N + 20 GiB.
D1_COPY_FACTOR = 1.5
D1_COPY_SPARE_GIB = 20.0
D1_FLOOR_FRACTION = 0.05

CANONICAL_DIR = "/mnt/shared/tessera-runs/moe/glm53-a8-bf16menu-20260930/release/exported"
STAGE_DIR = "/stage/prewarm/tessera-runs/moe/glm53-a8-bf16menu-20260930/release/exported"

CHUNK = 1024 * 1024

#: Fixed arm shape: 16 distinct files, 512 MiB prefix each, 8 GiB total.
#: 16 streams then map one file per thread; no thread idles.
ARM_FILES = 16
PREFIX_MIB = 512
PREFIX_BYTES = PREFIX_MIB * 1024 * 1024

#: Arm order: the 16-stream cold arm touches the bytes first, so it is
#: the closest to a true cold read. Warm arms follow their cold pair.
ARM_ORDER = [(16, True), (16, False), (4, True), (4, False), (1, True), (1, False)]


def d1_verdict(avail_bytes: int, total_bytes: int, need_bytes: int) -> dict:
    """D1 fit of a set of need_bytes on one filesystem."""
    floor_bytes = int(total_bytes * D1_FLOOR_FRACTION)
    room_after_copy = avail_bytes - int(need_bytes * D1_COPY_FACTOR) - int(D1_COPY_SPARE_GIB * GIB)
    room_after_floor = avail_bytes - need_bytes - floor_bytes
    fits = room_after_copy >= 0 and room_after_floor >= 0
    if room_after_copy < 0:
        reason = "1.5N+20GiB rule fails"
    elif room_after_floor < 0:
        reason = "5% floor fails"
    else:
        reason = "fits"
    return {
        "need_gib": round(need_bytes / GIB, 2),
        "avail_gib": round(avail_bytes / GIB, 2),
        "floor_gib": round(floor_bytes / GIB, 2),
        "tmp_need_gib": round(need_bytes * D1_COPY_FACTOR / GIB + D1_COPY_SPARE_GIB, 2),
        "fits": fits,
        "reason": reason,
    }


def statvfs_gib(path: str) -> dict | None:
    """Free/total GiB visible to this uid at path, or None."""
    try:
        st = os.statvfs(path)
    except OSError as exc:
        return {"path": path, "error": str(exc)}
    return {
        "path": path,
        "avail_gib": round(st.f_bavail * st.f_bsize / GIB, 2),
        "free_gib": round(st.f_bfree * st.f_bsize / GIB, 2),
        "total_gib": round(st.f_blocks * st.f_bsize / GIB, 2),
        "use_pct": round(100.0 * (1.0 - st.f_bavail / st.f_blocks), 1) if st.f_blocks else None,
    }


def list_files(root: str) -> list[tuple[str, int]]:
    """Regular files under root with sizes; empty when root is absent."""
    found: list[tuple[str, int]] = []
    if not os.path.isdir(root):
        return found
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            full = os.path.join(dirpath, name)
            try:
                if os.path.islink(full):
                    continue
                found.append((full, os.path.getsize(full)))
            except OSError:
                continue
    found.sort()
    return found


def select_arm_files(files: list[tuple[str, int]], count: int = ARM_FILES,
                     prefix_cap: int = PREFIX_BYTES) -> dict:
    """Pick count large files of similar size; one prefix fits them all.

    Takes the count largest files so each thread reads a similar share.
    The prefix is the cap clipped to the smallest pick, so every arm
    reads exactly len(picked) * prefix bytes.
    """
    usable = [(path, size) for path, size in files if size > 0]
    usable.sort(key=lambda entry: (-entry[1], entry[0]))
    picked = usable[:count]
    if not picked:
        return {"paths": [], "prefix": 0, "total": 0, "count": 0, "full": False}
    prefix = min(prefix_cap, min(size for _, size in picked))
    return {
        "paths": [path for path, _ in picked],
        "prefix": prefix,
        "total": len(picked) * prefix,
        "count": len(picked),
        "full": len(picked) == count,
    }


def drop_cache(path: str) -> bool:
    """Advise the kernel to drop cached pages of path. Client-side cold."""
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return False
    try:
        try:
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)  # type: ignore[attr-defined]
        except OSError:
            return False
        return True
    finally:
        os.close(fd)


def read_prefix_bytes(path: str, limit: int) -> int:
    """Sequential buffered read of at most limit bytes; returns bytes read."""
    total = 0
    with open(path, "rb") as stream:
        while total < limit:
            buf = stream.read(min(CHUNK, limit - total))
            if not buf:
                return total
            total += len(buf)
    return total


def allocated_bytes(path: str) -> int:
    """Device bytes backing path (st_blocks), or -1 when unreadable."""
    try:
        return os.stat(path).st_blocks * 512
    except OSError:
        return -1


def is_sparse(size: int, allocated: int) -> bool | None:
    """True when allocated device bytes fall short of the file size."""
    if allocated < 0:
        return None
    return allocated < size


def run_arm(paths: list[str], streams: int, cold: bool, prefix: int) -> dict:
    """One stream-count pass over disjoint shards. Aggregate rate.

    Every shard holds at least one file when len(paths) >= streams.
    Cold drops the client page cache first; the file server cache is
    outside client reach (see read_arcstats), so cold means client-cold.
    """
    if cold:
        dropped = sum(1 for p in paths if drop_cache(p))
    else:
        dropped = 0
    shards: list[list[str]] = [[] for _ in range(max(streams, 1))]
    for i, path in enumerate(paths):
        shards[i % len(shards)].append(path)
    idle = sum(1 for shard in shards if not shard)
    per_thread: list[float] = []
    started = time.monotonic()

    def work(shard: list[str]) -> tuple[int, float]:
        begin = time.monotonic()
        count = 0
        for path in shard:
            count += read_prefix_bytes(path, prefix)
        return count, time.monotonic() - begin

    total = 0
    with futures.ThreadPoolExecutor(max_workers=streams) as pool:
        for count, elapsed in pool.map(work, shards):
            total += count
            per_thread.append(round(elapsed, 3))
    wall = time.monotonic() - started
    mib_s = round(total / GIB * 1024 / wall, 1) if wall > 0 else 0.0
    return {
        "streams": streams,
        "cold": cold,
        "cache_dropped": dropped,
        "bytes_gib": round(total / GIB, 3),
        "wall_s": round(wall, 3),
        "mib_s": mib_s,
        "per_thread_s": per_thread,
        "idle_threads": idle,
        "load": list(os.getloadavg()),
    }


def read_arcstats() -> dict | None:
    """ZFS ARC counters when this host serves ZFS; else None.

    The Sparks are NFS clients, so this is None there. It records the
    attempt: a client cannot prove a server-cold read.
    """
    try:
        with open("/proc/spl/kstat/zfs/arcstats") as stream:
            rows = stream.read().splitlines()
    except OSError:
        return None
    want = {"hits", "misses", "l2_hits", "l2_misses", "size", "c"}
    out: dict = {}
    for line in rows[2:]:
        parts = line.split()
        if len(parts) >= 3 and parts[0] in want:
            try:
                out[parts[0]] = int(parts[2])
            except ValueError:
                continue
    return out or None


def measure_source(label: str, root: str) -> dict:
    """Cold/warm 1/4/16-stream reads of a fixed 8 GiB arm at root."""
    out: dict = {"label": label, "root": root, "present": os.path.isdir(root)}
    out["arcstats_before"] = read_arcstats()
    if not out["present"]:
        return out
    files = [entry for entry in list_files(root) if entry[1] > 0]
    out["file_count"] = len(files)
    out["total_gib"] = round(sum(size for _, size in files) / GIB, 3)
    sel = select_arm_files(files)
    out["arm_files"] = sel["count"]
    out["arm_full"] = sel["full"]
    out["arm_prefix_mib"] = round(sel["prefix"] / 1024 / 1024, 3)
    out["arm_total_gib"] = round(sel["total"] / GIB, 3)
    out["arm_names"] = [os.path.basename(p) for p in sel["paths"][:20]]
    paths = sel["paths"]
    out["arms"] = [run_arm(paths, n, cold, sel["prefix"]) for n, cold in ARM_ORDER]
    out["arcstats_after"] = read_arcstats()
    return out


def host_local_root() -> dict:
    """A writable host-local directory (device differs from /mnt/shared)."""
    try:
        shared_dev = os.stat("/mnt/shared").st_dev
    except OSError:
        shared_dev = None
    best: dict = {"path": None, "reason": "no writable host-local dir"}
    for cand in ("/var/tmp", "/tmp", os.path.expanduser("~"), os.getcwd()):
        try:
            st = os.stat(cand)
        except OSError:
            continue
        if shared_dev is not None and st.st_dev == shared_dev:
            continue
        if not os.access(cand, os.W_OK):
            continue
        vfs = statvfs_gib(cand)
        if vfs is None or "avail_gib" not in vfs:
            continue
        if best["path"] is None or vfs["avail_gib"] > best.get("avail_gib", -1):
            best = {"path": cand, "avail_gib": vfs["avail_gib"], "device": st.st_dev}
    return best


def write_test_file(path: str, size: int) -> None:
    """Write size non-sparse bytes (varied pattern, no holes), then fsync."""
    blk = bytes(((i * 31 + 17) & 0xFF) for i in range(CHUNK))
    with open(path, "wb") as stream:
        left = size
        while left > 0:
            step = min(CHUNK, left)
            stream.write(blk[:step])
            left -= step
        stream.flush()
        os.fsync(stream.fileno())
    drop_cache(path)


def measure_local_nvme(target_total: int) -> dict:
    """Same fixed arm shape on host-local disk: 16 files, 1/4/16 streams.

    File bytes match the NFS arm total, so rates compare directly. Each
    file is checked non-sparse (du backing vs size) before the arms run.
    """
    out: dict = host_local_root()
    if out["path"] is None:
        out["skipped"] = True
        return out
    per_file = (target_total + ARM_FILES - 1) // ARM_FILES if target_total > 0 else PREFIX_BYTES
    need_gib = target_total / GIB + 1.0
    if out.get("avail_gib", 0) < need_gib:
        out["skipped"] = True
        out["reason"] = "free space below arm total plus 1 GiB margin"
        return out
    tmpdir = tempfile.mkdtemp(prefix="gate-1730-", dir=out["path"])
    paths = [os.path.join(tmpdir, "nvme-%02d.bin" % i) for i in range(ARM_FILES)]
    out["arm_files"] = ARM_FILES
    out["arm_prefix_mib"] = round(per_file / 1024 / 1024, 3)
    out["arm_total_gib"] = round(ARM_FILES * per_file / GIB, 3)
    try:
        started = time.monotonic()
        for path in paths:
            write_test_file(path, per_file)
        write_s = time.monotonic() - started
        out["write_mib_s"] = round(ARM_FILES * per_file / GIB * 1024 / write_s, 1)
        backing = [allocated_bytes(p) for p in paths]
        out["sparse_flags"] = [is_sparse(per_file, b) for b in backing]
        if any(flag is not False for flag in out["sparse_flags"]):
            out["sparse_alarm"] = True
        out["arms"] = [run_arm(paths, n, cold, per_file) for n, cold in ARM_ORDER]
    finally:
        for path in paths:
            try:
                os.unlink(path)
            except OSError:
                pass
        try:
            os.rmdir(tmpdir)
        except OSError:
            pass
    return out


def run_docker(args: list[str], timeout: int = 180) -> dict:
    """One docker call with bounded wait; output truncated."""
    try:
        proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        return {"argv": args, "error": "docker not found"}
    except subprocess.TimeoutExpired:
        return {"argv": args, "error": "timeout"}
    return {
        "argv": args,
        "returncode": proc.returncode,
        "stdout": (proc.stdout or "")[-2000:],
        "stderr": (proc.stderr or "")[-2000:],
    }


def measure_bind() -> dict:
    """Nested bind over a subdirectory of -v /mnt/shared:/mnt/shared:ro."""
    out: dict = {"docker": run_docker(["docker", "version", "--format", "{{.Server.Version}}"])}
    images = run_docker(["docker", "images", "--format", "{{.Repository}}:{{.Tag}} {{.Size}}"])
    out["images"] = images
    image = None
    if images.get("returncode") == 0:
        for line in images["stdout"].splitlines():
            name = line.split()[0] if line.split() else ""
            if name and "<none>" not in name:
                image = name
                break
    if image is None:
        out["skipped"] = "no local image; will not pull on a full disk"
        return out
    out["image"] = image
    host = socket.gethostname()
    local = host_local_root()
    if local["path"] is None:
        out["skipped"] = "no host-local dir for the bind source"
        return out
    src = tempfile.mkdtemp(prefix="gate-1730-src-", dir=local["path"])
    shadow = "/mnt/shared/gate-1730-%s" % host
    calls: list[dict] = []
    try:
        with open(os.path.join(src, "marker.txt"), "w") as stream:
            stream.write("local-wins\n")
        with open(os.path.join(src, "second.txt"), "w") as stream:
            stream.write("second\n")
        try:
            os.makedirs(shadow, exist_ok=True)
            with open(os.path.join(shadow, "canonical.txt"), "w") as stream:
                stream.write("canonical\n")
            made_shadow = True
        except OSError as exc:
            made_shadow = False
            calls.append({"step": "make shadow dir", "error": str(exc)})
        base = ["docker", "run", "--rm", "-v", "/mnt/shared:/mnt/shared:ro"]
        calls.append({"step": "baseline ls", **run_docker([*base, image, "ls", shadow])})
        if made_shadow:
            nested = [*base, "--mount",
                      "type=bind,src=%s,dst=%s,readonly" % (src, shadow),
                      image, "sh", "-c", "ls %s; cat %s/marker.txt" % (shadow, shadow)]
            calls.append({"step": "nested bind ls+cat", **run_docker(nested)})
            missing = [*base, "--mount",
                       "type=bind,src=%s/missing,dst=%s,readonly" % (src, shadow),
                       image, "true"]
            calls.append({"step": "mount with missing src", **run_docker(missing)})
    finally:
        for name in ("marker.txt", "second.txt"):
            try:
                os.unlink(os.path.join(src, name))
            except OSError:
                pass
        try:
            os.rmdir(src)
        except OSError:
            pass
        try:
            os.unlink(os.path.join(shadow, "canonical.txt"))
            os.rmdir(shadow)
        except OSError:
            pass
    out["calls"] = calls
    nested = next((c for c in calls if c.get("step") == "nested bind ls+cat"), {})
    stdout = nested.get("stdout", "")
    out["nested_shows_local_only"] = ("marker.txt" in stdout and "second.txt" in stdout
                                      and "canonical.txt" not in stdout
                                      and nested.get("returncode") == 0)
    return out


def mountinfo_line(mountpoint: str) -> str | None:
    """The /proc/self/mountinfo row for mountpoint, if any."""
    try:
        with open("/proc/self/mountinfo") as stream:
            for line in stream:
                parts = line.split()
                if len(parts) > 4 and parts[4] == mountpoint:
                    return line.strip()[-400:]
    except OSError:
        return None
    return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Gate probe for prismabuild#1730.")
    ap.add_argument("--out", default="", help="Write the JSON report here too.")
    args = ap.parse_args(argv)
    report: dict = {
        "schema": "prismabuild.gate_1730_probe.v2",
        "host": socket.gethostname(),
        "started_unix": time.time(),
    }
    report["mounts"] = {
        "/mnt/shared": mountinfo_line("/mnt/shared"),
        "/stage/prewarm": mountinfo_line("/stage/prewarm"),
    }
    root_vfs = statvfs_gib("/")
    report["root_vfs"] = root_vfs
    if root_vfs is not None and "avail_gib" in root_vfs:
        st = os.statvfs("/")
        report["d1_a8s"] = d1_verdict(st.f_bavail * st.f_bsize,
                                      st.f_blocks * st.f_bsize, A8S_BYTES)
    docker_root = run_docker(["docker", "info", "--format", "{{.DockerRootDir}}"])
    report["docker_root"] = docker_root.get("stdout", "").strip()
    if report["docker_root"]:
        report["docker_root_vfs"] = statvfs_gib(report["docker_root"])
    report["stage_source"] = measure_source("stage", STAGE_DIR)
    report["hdd_source"] = measure_source("hdd-pool", CANONICAL_DIR)
    nfs_totals = [report[k].get("arm_total_gib", 0) for k in ("stage_source", "hdd_source")]
    target = int(max(nfs_totals) * GIB) if any(nfs_totals) else ARM_FILES * PREFIX_BYTES
    report["local_nvme"] = measure_local_nvme(target)
    report["bind"] = measure_bind()
    report["finished_unix"] = time.time()
    text = json.dumps(report, indent=1)
    if args.out:
        with open(args.out, "w") as stream:
            stream.write(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
