"""One private fleet -- queue, CAS, shared mount -- for the prewarm tests.

Shared because five behaviour claims about one loop need the same rig and
copying it five times is how the fifth copy stops matching the code.  Nothing
here asserts; every file that imports it states exactly one property.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402


def data_manifest(paths_and_sizes, *, prefix: str) -> dict:
    entries = [
        {"path": path, "offset": 0, "bytes": size, "sha256": None}
        for path, size in paths_and_sizes
    ]
    return {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "tests"},
        "annotations": {},
        "mount_prefix": prefix,
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": sum(e["bytes"] for e in entries),
    }


class Fleet:
    """A queue, a CAS and a shared mount with real files in it."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.mount = root / "shared"
        self.mount.mkdir(parents=True, exist_ok=True)
        self.queue = pool.PoolQueue(root / "pb-queue")
        self.queue.ensure_layout()
        self.cas_root = root / "cas"
        self.cas = pb.PrismaBuildCAS(self.cas_root)
        self.warmed: list[str] = []

    def file(self, name: str, size: int) -> tuple[str, int]:
        path = self.mount / name
        path.write_bytes(b"\0" * size)
        return str(path), size

    def action(self, key_seed: str, files, *, priority: int = 0,
               with_manifest: bool = True) -> str:
        """Seal a request carrying a manifest input and publish it ready."""

        action_key = hashlib.sha256(key_seed.encode()).hexdigest()
        inputs: list[dict] = []
        params: dict = {"command": ["true"]}
        if with_manifest:
            manifest = data_manifest(files, prefix=str(self.mount))
            blob = self.root / f"{key_seed}.manifest.json"
            blob.write_text(json.dumps(manifest))
            entry, _ = self.cas.ingest_input(
                blob, input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
            inputs.append(entry)
            params["data_manifest"] = {
                "input": entry, "mount_prefix": manifest["mount_prefix"],
                "entry_count": manifest["entry_count"],
                "total_bytes": manifest["total_bytes"],
            }
        request = self.cas_root / "requests" / action_key[:2] / f"{action_key}.json"
        request.parent.mkdir(parents=True, exist_ok=True)
        request.write_text(json.dumps(
            {"action_key": action_key, "inputs": inputs, "params": params}))
        self.queue.publish(
            action_key=action_key, cas_root=self.cas_root,
            worker_script=str(self.root / "worker.py"),
            checkout_root=str(self.root), priority=priority)
        return action_key

    def arcstats(self, *, size: int, c: int, c_max: int) -> str:
        # One file per set of counters, never one file rewritten: a fixture
        # that clobbers the counters a test already installed reads as the
        # loop ignoring them.
        path = self.root / f"arcstats-{size}-{c}-{c_max}"
        path.write_text(
            "name type data\n"
            f"size 4 {size}\nc 4 {c}\nc_max 4 {c_max}\n")
        return str(path)

    def args(self, **overrides) -> argparse.Namespace:
        base = dict(
            pool_root=str(self.queue.root), cas_root=str(self.cas_root),
            mount_map=[f"{self.mount}={self.mount}"], readers=2, lookahead=2,
            poll_s=0.0, arc_reserve_fraction=1.0,
            arcstats=self.arcstats(size=0, c=1 << 40, c_max=1 << 40),
            claim_grace_min=20.0, once=True, dry_run=False, log=None,
            min_manifest_bytes=0)
        base.update(overrides)
        return argparse.Namespace(**base)

    def cycle(self, args) -> dict:
        import threading
        mounts = prewarm_loop.MountMap(list(args.mount_map))
        return prewarm_loop.cycle(args, self.queue, mounts, threading.Event())
