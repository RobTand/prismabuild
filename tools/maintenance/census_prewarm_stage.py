#!/usr/bin/env python3
"""Read-only prewarm --stage drain census. Run through published pbrun."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from prismabuild import digest_primitives


TEMPORARY = re.compile(r"\.pbstage@\d+\+\d+\.(\d+)\.\d+\.tmp$")
PREFIX = "prismabuild-stage"


def writer_dataset(name: str, prefixes: set[str]) -> bool:
    """discover_stage selects only a pool root or its prewarm dataset."""
    pool, separator, child = name.partition("/")
    return any(pool.startswith(prefix) for prefix in prefixes) and (
        not separator or child == "prewarm")


def stamp() -> dict:
    now = time.time()
    return {"unix": now, "utc": datetime.fromtimestamp(now, timezone.utc).isoformat()}


def file_identity(path: Path) -> dict:
    data = path.read_bytes()
    stat = path.stat()
    return {"path": str(path), "resolved": str(path.resolve()),
            "sha256": digest_primitives.raw_sha256(data), "bytes": len(data),
            "device": stat.st_dev, "inode": stat.st_ino,
            "mtime_ns": stat.st_mtime_ns}


def process(pid: str) -> dict:
    root = Path("/proc") / pid
    before = (root / "stat").read_text()
    fields = before[before.rfind(")") + 2:].split()
    argv = [os.fsdecode(v) for v in (root / "cmdline").read_bytes().split(b"\0") if v]
    after = (root / "stat").read_text()
    if before[before.rfind(")") + 2:].split()[19] != after[after.rfind(")") + 2:].split()[19]:
        raise RuntimeError(f"PID {pid} changed identity during census")
    return {"pid": int(pid), "ppid": int(fields[1]), "start_ticks": int(fields[19]),
            "uid": root.stat().st_uid, "state": fields[0], "argv": argv}


def prewarm_processes(errors: list[str]) -> list[dict]:
    found = []
    for entry in sorted(Path("/proc").iterdir()):
        if not entry.name.isdecimal():
            continue
        try:
            item = process(entry.name)
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, ValueError, IndexError, RuntimeError) as exc:
            errors.append(f"process {entry.name}: {exc}")
            continue
        scripts = [v for v in item["argv"] if Path(v).name == "prewarm_loop.py"]
        module = any(v.endswith(".prewarm_loop") or v == "prewarm_loop"
                     for v in item["argv"])
        if not scripts and not module:
            continue
        # Unknown abbreviations are treated as possible writers, not absence.
        item["stage_requested"] = any(v == "--stage" or
                                      (v.startswith("--sta") and
                                       not v.startswith("--stage-"))
                                      for v in item["argv"])
        try:
            cwd = Path(os.readlink(entry / "cwd"))
            item["cwd"] = str(cwd)
            item["cgroup"] = (entry / "cgroup").read_text()
            item["executable"] = os.readlink(entry / "exe")
            if scripts:
                script = Path(scripts[0])
                script = (script if script.is_absolute() else cwd / script).resolve()
                item["source"] = file_identity(script)
                for parent in script.parents:
                    version = parent / "RUNTIME_VERSION.json"
                    if version.is_file():
                        item["runtime"] = file_identity(version)
                        document = json.loads(version.read_bytes())
                        item["runtime"]["commit"] = document.get("commit")
                        break
        except (OSError, ValueError) as exc:
            errors.append(f"prewarm identity {entry.name}: {exc}")
        found.append(item)
    return found


def mount_rows() -> list[dict]:
    def decode(value: str) -> str:
        return re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), value)
    rows = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        left, right = line.split(" - ", 1)
        fields, tail = left.split(), right.split()
        rows.append({"mount_id": fields[0], "device": fields[2],
                     "root": decode(fields[3]), "mountpoint": decode(fields[4]),
                     "fstype": tail[0], "source": decode(tail[1]), "raw": line})
    return rows


def collect_prewarm_stage_census(args) -> dict:
    errors: list[str] = []
    result = {"schema": "prismabuild.prewarm_stage_drain.v1", "started": stamp(),
              "host": socket.gethostname(), "expected_host": args.host,
              "source_base": args.source_base, "probe": file_identity(Path(__file__)),
              "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
              "affinity": sorted(os.sched_getaffinity(0)), "errors": errors}
    runtime = Path("/mnt/shared/prismabuild-fleet/repo").resolve()
    result["published_runtime"] = file_identity(runtime / "RUNTIME_VERSION.json")
    result["published_runtime"]["commit"] = json.loads(
        (runtime / "RUNTIME_VERSION.json").read_bytes()).get("commit")
    roster = runtime / "tools/fleet/fleet_boxes.json"
    result["roster"] = file_identity(roster)
    boxes = json.loads(roster.read_bytes())["boxes"]
    result["roster_hosts"] = [
        {"key": key, "alias": box.get("_alias"), "status": box.get("status"),
         "storage_argv": box.get("roles", {}).get("storage")}
        for key, box in boxes.items()]
    aliases = {args.host}
    for key, box in boxes.items():
        names = {key, box.get("_alias", key)}
        if args.host in names:
            aliases.update(names)
    result["host_aliases"] = sorted(aliases)
    if result["host"] not in aliases:
        errors.append("actual host is not the requested host or its declared alias")
    loops = prewarm_processes(errors)
    result["prewarm_before"] = loops
    roots: dict[str, list[str]] = {}

    def add_root(path: str, source: str):
        if path.startswith("/"):
            roots.setdefault(path, []).append(source)

    for path in args.root:
        add_root(path, "explicit census root")
    mounts = mount_rows()
    result["tier_records"] = []
    for path in sorted((Path(args.queue_root) / "tiers").glob("*.json")):
        try:
            record = json.loads(path.read_bytes())
            if str(record.get("pool", "")).startswith(PREFIX) or str(
                    record.get("tier_id", "")).startswith(PREFIX):
                result["tier_records"].append({"source": file_identity(path),
                                                "record": record})
                add_root(str(record.get("mountpoint", "")), "tier:" + path.name)
        except (OSError, ValueError) as exc:
            errors.append(f"tier {path}: {exc}")
    prefixes = {PREFIX}
    for loop in loops:
        argv = loop["argv"]
        for n, word in enumerate(argv):
            if word == "--stage-pool-prefix" and n + 1 < len(argv):
                prefixes.add(argv[n + 1])
            elif word.startswith("--stage-pool-prefix="):
                prefixes.add(word.split("=", 1)[1])
    result["stage_mounts"] = []
    result["other_stage_pool_mounts"] = []
    for row in mounts:
        if (row["mountpoint"] == "/stage" or row["mountpoint"].startswith("/stage/")
                or writer_dataset(row["source"], prefixes)):
            result["stage_mounts"].append(row)
            add_root(row["mountpoint"], "mountinfo:" + row["source"])
        elif any(row["source"].startswith(prefix) for prefix in prefixes):
            result["other_stage_pool_mounts"].append(row)
    zfs = shutil.which("zfs") or next(
        (p for p in ("/sbin/zfs", "/usr/sbin/zfs") if Path(p).is_file()), None)
    result["zfs"] = {"executable": zfs, "missing_tool": zfs is None}
    if zfs:
        run = subprocess.run([zfs, "list", "-H", "-o", "name,mountpoint"],
                             capture_output=True, text=True, timeout=30)
        result["zfs"].update(returncode=run.returncode, stdout=run.stdout, stderr=run.stderr)
        if run.returncode:
            errors.append("zfs list failed; imported stage-root coverage unknown")
        else:
            for line in run.stdout.splitlines():
                name, mountpoint = line.split("\t", 1)
                if writer_dataset(name, prefixes):
                    add_root(mountpoint, "zfs:" + name)
    elif "zfs" in Path("/proc/filesystems").read_text():
        errors.append("ZFS is loaded but zfs root discovery tool is missing")
    result["roots"] = []
    temporaries = {}
    for spelling, sources in sorted(roots.items()):
        root = Path(spelling)
        row = {"path": spelling, "sources": sources, "started": stamp(),
               "directories": 0, "files": 0, "symlink_directories": []}
        result["roots"].append(row)
        try:
            stat = root.stat()
        except FileNotFoundError:
            row.update(state="absent", finished=stamp())
            continue
        except OSError as exc:
            errors.append(f"root {root}: {exc}")
            row.update(state="unreadable", finished=stamp())
            continue
        row.update(state="present", resolved=str(root.resolve()), device=stat.st_dev,
                   inode=stat.st_ino)

        def walk_error(exc):
            errors.append(f"walk {root}: {exc}")

        for directory, dirs, files in os.walk(root, onerror=walk_error, followlinks=False):
            row["directories"] += 1
            for name in dirs:
                path = Path(directory) / name
                if path.is_symlink():
                    row["symlink_directories"].append(str(path))
                    errors.append(f"unwalked symlink directory {path}")
            for name in files:
                row["files"] += 1
                match = TEMPORARY.search(name)
                if not match:
                    continue
                path = Path(directory) / name
                try:
                    stat = path.lstat()
                    item = {"path": str(path), "device": stat.st_dev,
                            "inode": stat.st_ino, "bytes": stat.st_size,
                            "uid": stat.st_uid, "gid": stat.st_gid,
                            "mode": stat.st_mode, "mtime_ns": stat.st_mtime_ns,
                            "ctime_ns": stat.st_ctime_ns, "owner_pid_text": match[1]}
                    try:
                        item["current_pid_identity"] = process(match[1])
                    except (FileNotFoundError, ProcessLookupError):
                        item["current_pid_identity"] = None
                    except (OSError, ValueError, IndexError, RuntimeError) as exc:
                        item["current_pid_error"] = str(exc)
                    item["xattrs"] = {}
                    for attribute in ("user.pbstage.source", "user.pbstage.mover"):
                        try:
                            item["xattrs"][attribute] = os.getxattr(path, attribute,
                                                                  follow_symlinks=False).hex()
                        except OSError as exc:
                            item["xattrs"][attribute] = {"errno": exc.errno, "error": str(exc)}
                    temporaries[str(path)] = item
                except OSError as exc:
                    errors.append(f"temporary {path}: {exc}")
        row["finished"] = stamp()
    result["temporaries"] = list(temporaries.values())
    result["prewarm_after"] = prewarm_processes(errors)
    writers = {(p["pid"], p["start_ticks"]): p for p in
               loops + result["prewarm_after"] if p["stage_requested"]}
    result["stage_writers"] = list(writers.values())
    result["finished"] = stamp()
    result["empty_drain_observed"] = not errors and not writers and not temporaries
    result["limits"] = ["Point-in-time observation; no launch or write gate is installed.",
                        "PID text in a filename does not prove ownership after PID reuse.",
                        "Offline roster hosts require an explicit relevance decision."]
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--source-base", required=True)
    parser.add_argument("--queue-root", default="/mnt/shared/prismabuild-fleet/pb-queue")
    parser.add_argument("--root", action="append", default=["/stage", "/stage/prewarm"])
    args = parser.parse_args()
    result = collect_prewarm_stage_census(args)
    print(json.dumps(result))
    return 0 if result["empty_drain_observed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
