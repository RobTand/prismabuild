#!/usr/bin/env python3
"""Gate probe for prismabuild#1730: speed, space, bind mount.

Runs as a PB-admitted action child on one Spark (host pinned at submit:
disk and NFS paths are host-local facts). It measures nothing on the
coordinator. Standard library only; the sealed checkout carries it.

Three legs:
  space: statvfs of / and the Docker root, D1 verdict for the A8S set.
  speed: 1/4/16-stream cold and warm reads of staged A8S bytes from the
    stage tier (/stage/prewarm, NFS ro) and the HDD pool (/mnt/shared),
    plus a bounded host-local NVMe write/read when space allows.
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
SUBSET_GIB = 8.0
LOCAL_TEST_GIB = 2.0


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


def pick_subset(files: list[tuple[str, int]], budget_bytes: int) -> list[tuple[str, int]]:
    """First files in sorted order until the budget is met."""
    picked: list[tuple[str, int]] = []
    total = 0
    for entry in files:
        picked.append(entry)
        total += entry[1]
        if total >= budget_bytes:
            break
    return picked


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


def read_file_bytes(path: str) -> int:
    """Sequential buffered read; returns bytes read."""
    total = 0
    with open(path, "rb") as stream:
        while True:
            buf = stream.read(CHUNK)
            if not buf:
                return total
            total += len(buf)


def run_arm(paths: list[str], streams: int, cold: bool) -> dict:
    """One stream-count pass over disjoint shards. Aggregate rate."""
    if cold:
        dropped = sum(1 for p in paths if drop_cache(p))
    else:
        dropped = 0
    shards: list[list[str]] = [[] for _ in range(max(streams, 1))]
    for i, path in enumerate(paths):
        shards[i % len(shards)].append(path)
    per_thread: list[float] = []
    started = time.monotonic()

    def work(shard: list[str]) -> tuple[int, float]:
        begin = time.monotonic()
        count = 0
        for path in shard:
            count += read_file_bytes(path)
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
        "load": list(os.getloadavg()),
    }


def measure_source(label: str, root: str) -> dict:
    """Cold/warm 1/4/16-stream reads of an ~8 GiB subset at root."""
    out: dict = {"label": label, "root": root, "present": os.path.isdir(root)}
    if not out["present"]:
        return out
    files = [entry for entry in list_files(root) if entry[1] > 0]
    out["file_count"] = len(files)
    out["total_gib"] = round(sum(size for _, size in files) / GIB, 3)
    subset = pick_subset(files, int(SUBSET_GIB * GIB))
    out["subset_files"] = len(subset)
    out["subset_gib"] = round(sum(size for _, size in subset) / GIB, 3)
    out["subset_names"] = [os.path.basename(p) for p, _ in subset[:8]]
    paths = [p for p, _ in subset]
    out["arms"] = [run_arm(paths, n, cold) for n in (1, 4, 16) for cold in (True, False)]
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


def measure_local_nvme() -> dict:
    """Bounded write/fsync + cold/warm read on host-local disk."""
    out: dict = host_local_root()
    if out["path"] is None:
        out["skipped"] = True
        return out
    avail = out["avail_gib"]
    if avail < 6.0:
        out["skipped"] = True
        out["reason"] = "less than 6 GiB free; refuse to press a full disk"
        return out
    size = int(LOCAL_TEST_GIB * GIB)
    tmpdir = tempfile.mkdtemp(prefix="gate-1730-", dir=out["path"])
    target = os.path.join(tmpdir, "nvme-test.bin")
    out["test_gib"] = LOCAL_TEST_GIB
    try:
        started = time.monotonic()
        with open(target, "wb") as stream:
            left = size
            blk = b"\x00" * CHUNK
            while left > 0:
                stream.write(blk[: min(CHUNK, left)])
                left -= CHUNK
            stream.flush()
            os.fsync(stream.fileno())
        write_s = time.monotonic() - started
        out["write_mib_s"] = round(size / GIB * 1024 / write_s, 1)
        drop_cache(target)
        t0 = time.monotonic()
        n1 = read_file_bytes(target)
        cold_s = time.monotonic() - t0
        t0 = time.monotonic()
        n2 = read_file_bytes(target)
        warm_s = time.monotonic() - t0
        out["cold_read_mib_s"] = round(n1 / GIB * 1024 / cold_s, 1)
        out["warm_read_mib_s"] = round(n2 / GIB * 1024 / warm_s, 1)
    finally:
        try:
            os.unlink(target)
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
        "schema": "prismabuild.gate_1730_probe.v1",
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
    report["local_nvme"] = measure_local_nvme()
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
