#!/usr/bin/env python3
"""Prepare and profile small movement payloads inside admitted PB actions.

The private fixture has no live tier reservations. Claim-to-terminal timing
comes from the outer PB attempt, not from this child profile. cProfile adds
overhead; these runs cannot establish an unprofiled performance delta.
"""
from __future__ import annotations

import argparse
import cProfile
import hashlib
import json
from pathlib import Path
import pstats
import runpy
import subprocess
import sys

TOTAL_BYTES = 8356
CONSUMER = hashlib.sha256(b"PRI-198 consumer").hexdigest()
MOVER = hashlib.sha256(b"PRI-198 mover").hexdigest()
EGRESS = hashlib.sha256(b"PRI-198 egress").hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def prepare(root: Path, runtime: Path) -> None:
    root.mkdir(parents=True, exist_ok=False)
    source = root / "source"
    source.mkdir()
    (root / "stage").mkdir()
    entries = []
    for index in range(4):
        payload = bytes([index + 1]) * (TOTAL_BYTES // 4)
        path = source / f"part-{index}.bin"
        path.write_bytes(payload)
        entries.append({"path": str(path), "offset": 0, "bytes": len(payload),
                        "sha256": hashlib.sha256(payload).hexdigest()})
    manifest = {
        "schema": "prismaquant.prismabuild.data_manifest.v2",
        "produced_by": {"tool": "diag_811_small_actions"},
        "annotations": {},
        "mount_prefix": str(source), "entries": entries,
        "entry_count": len(entries), "total_bytes": TOTAL_BYTES,
        "read_plan": {"read_bytes": TOTAL_BYTES, "phases": [{
            "name": "small", "bytes": TOTAL_BYTES,
            "cumulative_bytes": TOTAL_BYTES, "entry_indices": list(range(4))}]},
    }
    write_json(root / "manifest.json", manifest)
    write_json(root / "fixture.json", {
        "schema": "prismabuild.diag811.small_fixture.v1",
        "runtime_root": str(runtime), "bytes": TOTAL_BYTES,
        "manifest_sha256": hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest(),
        "private_queue": True, "tier_reservations": False,
    })


def command(mode: str, root: Path, runtime: Path) -> list[str]:
    common = ["--pool-root", str(root / "queue"),
              "--stage-root", str(root / "stage"),
              "--consumer-action-key", CONSUMER,
              "--receipt", str(root / f"{mode}-receipt.json")]
    if mode == "move":
        manifest = root / "manifest.json"
        return [str(runtime / "tools/stage_move.py"), *common,
                "--action-key", MOVER, "--cas-root", str(root / "cas"),
                "--tier-id", "diag811:private", "--manifest", str(manifest),
                "--manifest-sha256", hashlib.sha256(manifest.read_bytes()).hexdigest(),
                "--range-start-bytes", "0", "--range-end-bytes", str(TOTAL_BYTES),
                "--readers", "1", "--max-readers", "1",
                "--unpaced", "--warm-after-copy", "never"]
    return [str(runtime / "tools/stage_release.py"), *common,
            "--action-key", EGRESS, "--mover-action-key", MOVER]


def run_payload(mode: str, root: Path, profile: Path | None) -> None:
    fixture = json.loads((root / "fixture.json").read_text())
    runtime = Path(fixture["runtime_root"])
    argv = command(mode, root, runtime)
    expected = fixture["manifest_sha256"]
    if hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest() != expected:
        raise SystemExit("fixture manifest changed")
    if mode == "egress":
        landed = list((root / "stage").rglob("part-*.bin"))
        if len(landed) != 4 or sum(path.stat().st_size for path in landed) != TOTAL_BYTES:
            raise SystemExit("egress needs all four landed fixture entries")
    sys.argv = argv
    profiler = cProfile.Profile() if profile else None
    if profiler:
        profiler.enable()
    try:
        runpy.run_path(argv[0], run_name="__main__")
    except SystemExit as exc:
        if exc.code not in (None, 0):
            raise
    finally:
        if profiler:
            profiler.disable()
            profiler.dump_stats(str(profile))
            stats = pstats.Stats(profiler)
            rows = [{"file": key[0], "line": key[1], "function": key[2],
                     "primitive_calls": values[0], "calls": values[1],
                     "self_s": values[2], "cumulative_s": values[3]}
                    for key, values in stats.stats.items()]
            write_json(profile.with_suffix(".json"), {
                "scope": "payload runpy, including runtime imports; excludes outer PB launcher",
                "runtime_root": str(runtime), "argv": argv,
                "rows": sorted(rows, key=lambda row: row["cumulative_s"], reverse=True),
            })
    receipt = json.loads((root / f"{mode}-receipt.json").read_text())
    if not receipt.get("complete"):
        raise SystemExit(f"{mode} did not complete: {receipt}")
    if mode == "move" and receipt.get("bytes_staged") != TOTAL_BYTES:
        raise SystemExit("mover did not stage the full fixture")
    if mode == "egress" and any((root / "stage").rglob("part-*.bin")):
        raise SystemExit("egress left fixture bytes behind")
    if mode == "egress" and receipt.get("bytes_deleted") != TOTAL_BYTES:
        raise SystemExit("egress did not delete the full fixture")
    print(json.dumps({"mode": mode, "verified": True, "bytes": TOTAL_BYTES,
                      "runtime_root": str(runtime), "profile": str(profile) if profile else None}))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "move", "egress"))
    parser.add_argument("--fixture-root", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path)
    parser.add_argument("--profile", type=Path)
    args = parser.parse_args()
    root = args.fixture_root
    owner = root.parent if root.name == "validation" else root
    if not root.is_absolute() or owner.parent != Path("/home/rob/tmp") or not owner.name.startswith("pri198-811-"):
        parser.error("fixture must be a pri198-811-* child of /home/rob/tmp")
    if args.mode == "prepare":
        if args.runtime_root is None or not (args.runtime_root / "RUNTIME_VERSION.json").is_file():
            parser.error("prepare needs an immutable published --runtime-root")
        prepare(root, args.runtime_root.resolve(strict=True))
        validation = root / "validation"
        prepare(validation, args.runtime_root.resolve(strict=True))
        for mode in ("move", "egress"):
            subprocess.run([sys.executable, __file__, mode,
                            "--fixture-root", str(validation)], check=True)
        print(json.dumps({"prepared": str(root), "validated": True, "bytes": TOTAL_BYTES}))
    else:
        run_payload(args.mode, root, args.profile)


if __name__ == "__main__":
    main()
