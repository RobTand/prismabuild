#!/usr/bin/env python3
"""PB-admitted, read-only sampling of an existing contended worker.

Coordinator 2026-10-01 ~05:00Z authorized this ordinary observer instead of
near-idle measurement isolation, which would remove the contention. This is
sampling coverage, not a wall-time benchmark, cache authorization or speedup.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import socket
import subprocess
import sys
import tarfile
import time
import urllib.parse
import urllib.request


def identity(pid: int) -> dict:
    proc = Path(f"/proc/{pid}")
    command = (proc / "cmdline").read_bytes()
    scripts = [Path(arg.decode()) for arg in command.split(b"\0")
               if arg.startswith(b"/") and arg.endswith(b"/tools/worker_loop.py")
               and b"/runtime-generations/" in arg]
    if len(scripts) != 1:
        raise ValueError("observer target is not one published worker loop")
    script = scripts[0].resolve(strict=True)
    root = script.parent.parent
    pool = root / "src/prismabuild/pool.py"
    return {"pid": pid, "start_ticks": int((proc / "stat").read_text().rsplit(")", 1)[1].split()[19]),
            "command_sha256": hashlib.sha256(command).hexdigest(),
            "worker_script": str(script), "generation_root": str(root),
            "pool_sha256": hashlib.sha256(pool.read_bytes()).hexdigest(),
            "exe": str((proc / "exe").resolve(strict=True))}


def _holder_observation(queue: Path, key: str) -> dict:
    """Read a claim without asserting that it qualifies a live observation."""
    try:
        data = json.loads((queue / "claimed" / f"{key}.json").read_text())
    except FileNotFoundError:
        return {"action_key": key, "present": False}
    if data.get("action_key") != key:
        raise ValueError("observer holder claim does not bind its key")
    return {"action_key": key, "present": True,
            "claimed_host": data.get("claimed_host"),
            "claimed_by": data.get("claimed_by"), "claimed_unix": data.get("claimed_unix")}


def holder(queue: Path, key: str) -> dict:
    """Refuse a retired or foreign holder before starting the profiler."""
    observed = _holder_observation(queue, key)
    if not observed["present"]:
        raise ValueError("observer holder is not claimed")
    if observed["claimed_host"] != socket.gethostname():
        raise ValueError("observer holder claimed_host does not match this host")
    return observed


def netdata(base: str, started: float, ended: float, out: Path) -> dict:
    errors = {}
    for chart in ("system.cpu", "system.load", "system.io", "nfsd.io", "nfsd.rpc", "nfsd.proc4ops"):
        query = urllib.parse.urlencode({"chart": chart, "after": int(started),
                                       "before": int(ended), "points": max(1, int(ended - started)),
                                       "format": "json", "group": "average"})
        try:
            with urllib.request.urlopen(base.rstrip("/") + "/api/v1/data?" + query, timeout=15) as response:
                raw = response.read(4 * 1024 * 1024 + 1)
            if len(raw) > 4 * 1024 * 1024:
                raise ValueError("observer Netdata response exceeds 4 MiB")
            data = json.loads(raw)
            if not data.get("data"):
                raise ValueError("observer Netdata chart has no matched rows")
            (out / f"{chart}.json").write_bytes(raw)
        except (OSError, ValueError) as error:
            errors[chart] = str(error)
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--start-ticks", required=True, type=int)
    parser.add_argument("--generation-root", required=True, type=Path)
    parser.add_argument("--holder-key", required=True)
    parser.add_argument("--duration", required=True, type=int)
    parser.add_argument("--netdata-url", required=True)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    if args.pid <= 0 or not 0 < args.duration <= 600:
        raise ValueError("observer pid/duration must be positive, duration at most 600s")
    if len(args.holder_key) != 64 or any(c not in "0123456789abcdef" for c in args.holder_key):
        raise ValueError("observer holder key must be a full SHA256")
    if args.out.is_absolute() or ".." in args.out.parts:
        raise ValueError("observer output must remain inside the materialized checkout")
    before = identity(args.pid)
    if before["start_ticks"] != args.start_ticks or before["generation_root"] != str(args.generation_root):
        raise ValueError("observer target identity changed before capture")
    args.out.mkdir(exist_ok=False)
    queue = Path("/mnt/shared/prismabuild-fleet/pb-queue")
    holder_before = holder(queue, args.holder_key)
    pyspy = Path("/usr/local/bin/py-spy")
    version = subprocess.check_output([str(pyspy), "--version"], text=True).strip()
    profile = args.out / "worker.speedscope.json"
    command = ["sudo", "-n", "--preserve-env=TMPDIR", str(pyspy), "record", "--pid", str(args.pid), "--idle", "--threads",
               "--subprocesses", "--rate", "100", "--duration", str(args.duration),
               "--format", "speedscope", "--output", str(profile)]
    started = time.time()
    with (args.out / "py-spy.stdout").open("w") as stdout, (args.out / "py-spy.stderr").open("w") as stderr:
        result = subprocess.run(command, stdout=stdout, stderr=stderr, check=False)
    ended = time.time()
    after = identity(args.pid)
    if after != before:
        raise ValueError("observer target identity changed during capture")
    # A holder retiring during capture is evidence to retain, not permission
    # to discard the profile or assert an atomic admission census.
    holder_after = _holder_observation(queue, args.holder_key)
    errors = netdata(args.netdata_url, started, ended, args.out)
    summary = runpy.run_path(str(Path(__file__).with_name("diag940_profile_coverage.py")))["summarize_profile"]
    coverage_error = None
    try:
        coverage = summary(json.loads(profile.read_text()), pid=args.pid, generation_root=str(args.generation_root))
    except (OSError, ValueError) as error:
        coverage_error = str(error)
        coverage = {"status": "profile_unreadable", "error": coverage_error}
    report = {"schema": "prismabuild.diag940.contended_observation.v1", "target": before,
              "observer_host": socket.gethostname(), "observer_affinity": sorted(os.sched_getaffinity(0)),
              "started_unix": started, "ended_unix": ended, "pyspy_returncode": result.returncode,
              "pyspy_version": version, "pyspy_sha256": hashlib.sha256(pyspy.read_bytes()).hexdigest(),
              "holder_before": holder_before, "holder_after": holder_after,
              "netdata_source": args.netdata_url, "netdata_errors": errors, "coverage": coverage,
              "performance_delta": None,
              "limits": ["no synthetic holders or worker mutations", "observer itself reserves CPU and memory",
                         "holder snapshots are not an atomic admission census", "NFS server charts are fleet-wide",
                         "sampled occupancy is not call latency; absence is not zero cost",
                         "no cache change or before/after speedup is claimed"]}
    (args.out / "capture.json").write_text(json.dumps(report, indent=2) + "\n")
    report["files"] = {p.name: {"bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
                       for p in args.out.iterdir() if p.is_file()}
    archive = args.out.with_suffix(".tar.gz")
    with tarfile.open(archive, "w:gz") as bundle:
        for path in sorted(args.out.iterdir()):
            bundle.add(path, arcname=path.name)
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
    from prismabuild.core import PrismaBuildCAS
    artifact, _ = PrismaBuildCAS("/mnt/shared/prismabuild-fleet/cas").ingest_input(
        archive, input_id="diag940.observer-artifacts")
    report["artifact_blob"] = artifact
    print(json.dumps(report, sort_keys=True))
    return result.returncode or int(bool(errors) or coverage_error is not None)


if __name__ == "__main__":
    raise SystemExit(main())
