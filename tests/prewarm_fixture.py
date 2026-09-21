"""One private fleet -- queue, CAS, shared mount -- for the prewarm tests.

Shared because five behaviour claims about one loop need the same rig and
copying it five times is how the fifth copy stops matching the code.  Nothing
here asserts; every file that imports it states exactly one property.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from prismabuild import progress as progress_v1  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prewarm_loop  # noqa: E402


def data_manifest(paths_and_sizes, *, prefix: str,
                  annotations: dict | None = None) -> dict:
    entries = [
        {"path": path, "offset": 0, "bytes": size, "sha256": None}
        for path, size in paths_and_sizes
    ]
    return {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "tests"},
        "annotations": dict(annotations or {}),
        "mount_prefix": prefix,
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": sum(e["bytes"] for e in entries),
    }


def phase_table(named_sizes) -> list[dict]:
    """``annotations.phases``: a running byte sum in the consumer's read order.

    The same shape PrismaQuant's producer writes (PQ #524), built from the
    sizes the test already declares, so a fixture phase boundary can never
    drift from the entry it is supposed to fall between.
    """

    table: list[dict] = []
    total = 0
    for name, size in named_sizes:
        total += size
        table.append({"name": name, "bytes": size, "cumulative_bytes": total})
    return table


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
        # A real closure over a real file, because the request this fixture
        # files is read back through ``core.validate_action`` on the publish
        # path and a closure is validated against its own digest.
        worker = root / "worker.py"
        worker.write_text("raise SystemExit(0)\n", encoding="utf-8")
        self.code_closure = pb.build_code_closure(root, ["worker.py"])

    def file(self, name: str, size: int) -> tuple[str, int]:
        path = self.mount / name
        path.write_bytes(b"\0" * size)
        return str(path), size

    def action(self, key_seed: str, files, *, priority: int = 0,
               with_manifest: bool = True,
               annotations: dict | None = None,
               progress_phases: list[str] | None = None,
               read_plan: dict | None = None) -> str:
        """Seal a request carrying a manifest input and publish it ready.

        Sealed rather than hand-written: since R4 the pool binds admission to
        the CAS-filed request and reads it with the full
        ``core.validate_action``, so a request carrying only the three fields
        a prewarm test looks at refuses before the row is ever READY.  The key
        is therefore the canonical hash of this body, and ``key_seed`` reaches
        it through the command -- which is what makes two actions in one test
        two actions.
        """

        inputs: list[dict] = []
        params: dict = {
            "command": ["true", key_seed],
            "cwd": str(self.root),
            "demand": {"cpu": 1},
            "placement": {"required_tags": []},
            "retry_policy": {"max_attempts": 1},
        }
        if progress_phases is not None:
            # The policy the action seals, in the shape ``core`` validates.
            # The loop reads records only where one of these exists, so a test
            # about progress has to declare it exactly as a submitter does.
            params[pb.PROGRESS_PARAM] = {
                "schema": pb.PROGRESS_POLICY_SCHEMA_V1,
                "phases": [{"name": name, "grace_s": 600.0}
                           for name in progress_phases],
            }
        if with_manifest:
            manifest = data_manifest(files, prefix=str(self.mount),
                                     annotations=annotations)
            if read_plan is not None:
                manifest["schema"] = pb.DATA_MANIFEST_SCHEMA_V2
                manifest["read_plan"] = read_plan
            manifest = pb.validate_data_manifest(manifest)
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
            if read_plan is not None:
                params["data_manifest"].update({
                    "schema": pb.DATA_MANIFEST_SCHEMA_V2,
                    "read_bytes": manifest["read_plan"]["read_bytes"],
                })
        action = pb.seal_action({
            "schema": pb.ACTION_SCHEMA_V2,
            "task": {"definition_id": "fleet/tests", "definition_version": "v1",
                     "task_class": "generation", "determinism": "stochastic",
                     "artifact_family": "generic", "artifact_kind": "generic",
                     "argv": ["true", key_seed], "working_directory": ".",
                     "result_path": "result"},
            "inputs": inputs,
            "code_closure": self.code_closure,
            "params": params,
            "environment": {"variables": {"PATH": "/usr/bin"},
                            "toolchain": {}},
            "execution_scope": {"portability": "portable",
                                "platform_key": None, "host_class": None},
        })
        action_key = str(action["action_key"])
        request = self.cas_root / "requests" / action_key[:2] / f"{action_key}.json"
        request.parent.mkdir(parents=True, exist_ok=True)
        request.write_text(json.dumps(action))
        self.queue.publish(
            action_key=action_key, cas_root=self.cas_root,
            worker_script=str(self.root / "worker.py"),
            checkout_root=str(self.root), priority=priority)
        return action_key

    def claim(self, action_key: str, *, age_s: float = 0.0,
              host: str | None = None) -> Path:
        """Move a ready item into ``claimed/`` the way a claim moves it.

        ``host`` is written as ``claimed_host``, the field the pool's own
        claim writes: it is the first link of the identity chain the pacer
        follows to the served action's reads (#580).
        """

        source = self.queue.root / "ready" / f"{action_key}.json"
        item = json.loads(source.read_text())
        source.unlink()
        item.update({"action_key": action_key,
                     "claimed_unix": time.time() - age_s,
                     "claimed_by": "prewarm-fixture"})
        if host is not None:
            item["claimed_host"] = host
        target = self.queue.root / "claimed" / f"{action_key}.json"
        target.write_text(json.dumps(item))
        return target

    def report_progress(self, action_key: str, phase: str, *,
                        units: int = 1, at: float | None = None) -> Path:
        """Write the matching worker's accepted heartbeat observation."""

        claimed = json.loads((self.queue.root / "claimed" /
                              f"{action_key}.json").read_text())
        observation = {
                "source": "action-progress",
                "last_accepted": {
                    "phase": phase, "units_completed": units,
                    "reported_unix": time.time() if at is None else at,
                },
            }
        self.queue.write_lease(
            action_key, owner="prewarm-fixture", claim_snapshot=claimed,
            progress_observation=observation)
        return self.queue.lease_path(action_key)

    def offer(self, host: str, *, addresses: list[str] | None = None,
              announced_unix: float | None = None) -> Path:
        """A worker offer for ``host``, written the way a box's loops write it.

        Written as a file rather than through ``PoolQueue.announce`` on
        purpose: a test about what the storage role does with the record must
        be able to state the record exactly, including the shape a runtime
        that announced no ``addresses`` leaves behind.
        """

        directory = self.queue.root / pool.WORKERS
        directory.mkdir(parents=True, exist_ok=True)
        record: dict = {
            "schema": pool.POOL_OFFER_SCHEMA_V1, "host": host, "tags": [host],
            "has_gpu": True, "capacity": {"cpu": 1, "gpu": 1, "mem_gb": 1},
            "announced_unix": time.time() if announced_unix is None else announced_unix,
        }
        if addresses is not None:
            record["addresses"] = list(addresses)
        path = directory / f"{host}.json"
        path.write_text(json.dumps(record))
        return path

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
            # One depth tier: ``max_readers`` at 0 makes the pacer answer
            # ``readers`` whoever is reading, which is every pre-#580 test's
            # shape.  A test about depth sets both.
            max_readers=0,
            poll_s=0.0, arc_reserve_fraction=1.0,
            arcstats=self.arcstats(size=0, c=1 << 40, c_max=1 << 40),
            claim_grace_min=20.0, once=True, dry_run=False, log=None,
            min_manifest_bytes=0,
            # No pool, no disks: the pacer these tests build is inactive and
            # reads at full speed.  A test about pacing builds its own pacer
            # on a fake stat source and hands it to ``cycle``.
            pace_pool="", disks="", max_util_pct=40.0,
            max_read_await_ms=15.0, max_backlog_ms=4000.0,
            pace_sample_s=0.5, pace_hold_s=0.25,
            # No client counter either: these fixtures build no pacer that
            # reads one, and a pacer that cannot see clients treats them as
            # reading, which is the shape every non-pacing test wants.
            nfsd_io="", export_stats="",
            client_active_mb_s=prewarm_loop.CLIENT_ACTIVE_MB_S)
        base.update(overrides)
        return argparse.Namespace(**base)

    def cycle(self, args, pacer=None) -> dict:
        import threading
        mounts = prewarm_loop.MountMap(list(args.mount_map))
        return prewarm_loop.cycle(
            args, self.queue, mounts, threading.Event(), pacer=pacer)


class StagePool:
    """A stage tier the tests can state exactly: real files, fake ``zpool``.

    The loop discovers its stage by asking ``zpool`` and ``zfs``, so a test
    about that discovery has to answer as those tools answer -- byte counts
    from ``zpool list -Hp``, a mountpoint from ``zfs get``, leaf paths from
    ``zpool status -P`` -- and let the real parse do the rest.  The mountpoint
    is a real directory and the members are real device nodes under a private
    ``by-id`` tree, so a record's member names come from resolving symlinks
    the way they do on the box.
    """

    def __init__(self, root: Path, *, name: str = "prismabuild-stage",
                 size: int = 1 << 40, free: int = 1 << 40,
                 health: str = "ONLINE",
                 by_id_name: str = "nvme-LT0800KEXVA_CVMD54710026800BGN",
                 device: str = "nvme9n1", mounted: bool = True,
                 has_dataset: bool = True,
                 available: "int | None" = None,
                 answers_available: bool = True) -> None:
        self.name = name
        self.size = size
        self.free = free
        #: What ``zfs get -Hp available`` on the dataset answers.  Its own
        #: number, below the pool's ``free``, because ZFS has already
        #: withheld the pool's slop from it.  ``None`` is a dataset that
        #: cannot answer, which is the loop's fallback path.
        self.available = free if available is None else available
        #: A dataset that cannot answer ``zfs get available`` at all, which is
        #: the loop's fallback-to-pool-``free`` path.
        self.answers_available = answers_available
        self.health = health
        self.by_id_name = by_id_name
        self.device = device
        self.has_dataset = has_dataset
        self.dataset = f"{name}/prewarm" if has_dataset else name
        self.mount = root / "stage-mount"
        if mounted:
            self.mount.mkdir(parents=True, exist_ok=True)
        self.mountpoint = str(self.mount)
        devices = root / "devices"
        devices.mkdir(parents=True, exist_ok=True)
        self.device_path = devices / device
        self.device_path.write_bytes(b"")
        self.by_id = root / "by-id"
        self.by_id.mkdir(parents=True, exist_ok=True)
        link = self.by_id / by_id_name
        if not link.exists():
            link.symlink_to(self.device_path)
        self.calls: list[list[str]] = []

    def runner(self, argv: list[str]) -> str:
        self.calls.append(list(argv))
        if "list" in argv:
            return (f"{self.name}\t{self.size}\t{self.size - self.free}\t"
                    f"{self.free}\t{self.health}\n")
        if "get" in argv:
            # ``zfs get`` on a dataset that does not exist exits nonzero, the
            # way the loop's fallback to the pool root expects it to.
            if argv[-1] != self.dataset:
                raise subprocess.CalledProcessError(1, argv)
            if "available" in argv:
                if not self.answers_available:
                    raise subprocess.CalledProcessError(1, argv)
                return f"{self.available}\n"
            return self.mountpoint + "\n"
        if "status" in argv:
            return (f"  pool: {self.name}\nconfig:\n\n"
                    "\tNAME                 STATE\n"
                    f"\t{self.name}          ONLINE\n"
                    f"\t  {self.device_path}  ONLINE\n\nerrors: No known data errors\n")
        raise AssertionError(f"unexpected command: {argv}")

    def install(self, monkeypatch) -> None:
        """Answer the loop's own subprocess boundary, and nothing else."""

        monkeypatch.setattr(prewarm_loop, "run_tool", self.runner)
        monkeypatch.setattr(prewarm_loop, "BY_ID", str(self.by_id))

    def objects(self) -> list[str]:
        """Every staged object, as a path relative to the stage mountpoint."""

        return sorted(
            str(path.relative_to(self.mount))
            for path in self.mount.rglob("*") if path.is_file())
