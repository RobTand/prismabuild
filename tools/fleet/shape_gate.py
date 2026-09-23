#!/usr/bin/env python3
"""Stage a real campaign's manifest shape through both tiers, then read it back.

This is the pre-publish shape gate.  Every unit and fixture test of the
staging path passed while a sealing defect refused every mover of a real
campaign (#965): the chunk splitter cut a phase at byte offsets, a real phase
has thousands of entries of uneven size, and a byte cut inside one entry made
both neighbouring movers overrun their reservations.  The fixtures had built
each phase from one entry, so no cut could land inside one.  What caught it
was the campaign.  This gate runs the campaign's shape before a generation is
published instead.

A **table** is a real data manifest with its paths replaced by file ordinals:
every entry's file, offset and size in manifest order, whether it declares a
digest, and the v2 read plan.  Tables are committed under
``shape_gate_tables/``, so the gate reads nothing from the CAS and a
collection cannot remove its input.  ``extract`` writes one from a manifest.

The harness scales the table by :data:`SCALE`: 1 GiB of the campaign is 1 MiB
here.  The seal, the ledger and the windows count in
``storage_tiers.GIB``, so a caller that sets ``storage_tiers.GIB`` to
:data:`UNIT_BYTES` keeps every ratio -- entries to chunks, chunks to the RAM
window, the window to the reference manifest -- while the movers copy real
bytes.  About 176 MiB moves per tier for the reference table.

Then, on a queue, CAS, pool, stage and RAM root of the caller's own:

1. Each source file is written once, and each entry's digest is taken from the
   bytes as they are written.
2. The consumer is sealed by ``pbrun.seal_action_from_template`` and
   published by ``pbrun.publish_consumer_row``, which seals every mover and
   egress through ``residency_stage_rows``, as a submission does.
3. ``tier_loop.cycle`` announces the tiers and publishes the windows.  Its
   discovery is the one part the harness supplies: the stage record, and the
   RAM record built by ``storage_tiers.ram_tier`` from the checkout's own RAM
   policy against the storage host's recorded memory facts.
4. A worker loop claims each ready movement node and runs its sealed command
   in this process, through the tool's own ``main``.
5. A reader claims the consumer, reports each phase as it starts it, and reads
   every entry through the consumer's residency map and a reader-lease pin on
   the RAM tier, hashing the bytes it reads.  An entry that is not in RAM is a
   failure; there is no pool fallback.

The result says what the gate exercised and what each tier moved.  A shape that
exercises no phase larger than a chunk, or no byte cut inside an entry, fails
as ``did_not_test`` rather than passing.

Nothing here touches the live queue, a real pool, ``/stage/prewarm`` or
``/ram/prewarm``: every root is the caller's.
"""
from __future__ import annotations

import argparse
import contextlib
import gzip
import hashlib
import io
import itertools
import json
import os
from pathlib import Path
import random
import socket
import sys
import time
from collections.abc import Callable, Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import generation_root  # noqa: E402

sys.path.insert(0, str(generation_root(__file__) / "src"))

from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, reader_lease, residency_map  # noqa: E402
from prismabuild import residency_plan, storage_tiers  # noqa: E402

TABLE_SCHEMA_V1 = "prismabuild.shape_gate.table.v1"
RESULT_SCHEMA_V1 = "prismabuild.shape_gate.result.v1"
#: Where the committed tables live, one per reference manifest.
TABLE_ROOT = Path(__file__).resolve().parent / "shape_gate_tables"
#: 1 GiB of the campaign is 1 MiB here.
SCALE = 1024
UNIT_BYTES = (1 << 30) // SCALE

#: The storage host as the live queue announced it at 2026-09-23T13:57Z
#: (``pb-queue/tiers/*:dl380g10.json`` and ``workers/dl380g10.json``).  Byte
#: figures are scaled by :data:`SCALE` when used; GiB counts are counts of
#: ``storage_tiers.GIB`` and are used as they are.
HOST_PROFILE: dict[str, object] = {
    "host": "dl380g10",
    "source_pool": "storage_pool",
    "stage_pool": "prismabuild-stage",
    # The stage's supply with nothing landed: ``capacity.stage_gib``.
    "stage_empty_gib": 601,
    # The tmpfs roof: ``size=251658240k``.
    "tmpfs_ceiling_bytes": 257698037760,
    "mem_total_bytes": 316241039360,
    "arc_c_max_bytes": 23622320128,
    "arc_size_bytes": 23557060320,
    "arc_meta_used_bytes": 7421280480,
    "worker": {"cpu": 80, "mem_gb": 96},
}

#: Cycles in a row that may pass with nothing claimed, nothing read and
#: nothing announced before the harness calls the window stalled.  A mover's
#: funding takes a cycle to reserve and one to transfer; this is several of
#: those, not a timeout.
MAX_IDLE_CYCLES = 12


class ShapeGateFailure(Exception):
    """The gate's verdict when it is not a pass, with a reason a caller can match."""

    def __init__(self, reason: str, detail: str,
                 evidence: Mapping[str, object] | None = None) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail
        self.evidence = dict(evidence or {})


# ------------------------------------------------------------------ tables


def table_from_manifest(manifest: Mapping[str, object], *,
                        manifest_sha256: str) -> dict[str, object]:
    """A v2 manifest's shape, with every path replaced by a file ordinal.

    Kept: each entry's file, offset and size in manifest order, whether it
    declares a digest, and the read plan's phases.  Dropped: the paths, the
    digests and the annotations, none of which the staging path reads for
    anything but identity.
    """

    normalized = pb.validate_data_manifest(manifest)
    if normalized["schema"] != pb.DATA_MANIFEST_SCHEMA_V2:
        raise ValueError("a shape table is taken from a v2 manifest's read plan")
    files: dict[str, int] = {}
    rows: list[list[int]] = []
    for entry in normalized["entries"]:                      # type: ignore[union-attr]
        ordinal = files.setdefault(str(entry["path"]), len(files))
        rows.append([ordinal, int(entry["offset"]), int(entry["bytes"]),
                     0 if entry["sha256"] is None else 1])
    plan = normalized["read_plan"]
    phases = [{"name": str(phase["name"]),
               "entry_indices": [int(index) for index in phase["entry_indices"]]}
              for phase in plan["phases"]]                   # type: ignore[index]
    return {
        "schema": TABLE_SCHEMA_V1,
        "source": {
            "manifest_sha256": str(manifest_sha256),
            "manifest_schema": str(normalized["schema"]),
            "entry_count": int(normalized["entry_count"]),   # type: ignore[arg-type]
            "total_bytes": int(normalized["total_bytes"]),   # type: ignore[arg-type]
            "file_count": len(files),
            "phase_count": len(phases),
        },
        "entries": rows,
        "phases": phases,
    }


def table_bytes(table: Mapping[str, object]) -> bytes:
    """The committed encoding: canonical JSON in one gzip member, mtime 0."""

    raw = json.dumps(table, sort_keys=True, separators=(",", ":")).encode()
    return gzip.compress(raw, compresslevel=9, mtime=0)


def read_table(path: str | Path) -> dict[str, object]:
    """Load and check one committed table."""

    with open(path, "rb") as stream:
        return parse_table(stream.read(), where=str(path))


def parse_table(raw: bytes, *, where: str = "table") -> dict[str, object]:
    """Check one table's committed bytes, for a caller that already holds them."""

    path = where
    table = json.loads(gzip.decompress(raw))
    if not isinstance(table, dict) or table.get("schema") != TABLE_SCHEMA_V1:
        raise ValueError(f"{path}: not a {TABLE_SCHEMA_V1} table")
    rows = table.get("entries")
    phases = table.get("phases")
    source = table.get("source")
    if not isinstance(rows, list) or not isinstance(phases, list) or not isinstance(source, dict):
        raise ValueError(f"{path}: a table needs entries, phases and a source")
    if len(rows) != int(source.get("entry_count", -1)):
        raise ValueError(f"{path}: entry_count disagrees with the entries")
    if sum(int(row[2]) for row in rows) != int(source.get("total_bytes", -1)):
        raise ValueError(f"{path}: total_bytes disagrees with the entries")
    return table


# ------------------------------------------------------------------ shape


def _overlap(a_offset: int, a_bytes: int, b_offset: int, b_bytes: int) -> bool:
    return a_offset < b_offset + b_bytes and b_offset < a_offset + a_bytes


def scaled_shape(table: Mapping[str, object], *, scale: int = SCALE) -> dict[str, object]:
    """The table at ``1/scale`` size, refusing a scale that changes its shape.

    Offsets and sizes are divided by ``scale``, and a size never falls below
    one byte.  Within a file that the manifest reads more than once, two
    ranges must overlap after scaling exactly when they overlapped before,
    and no two may land on one offset: those are the shapes a shared staged
    name and the manifest's ``(path, offset)`` rule are about.
    """

    rows = table["entries"]                                  # type: ignore[index]
    entries = [{"file": int(row[0]), "offset": int(row[1]) // scale,
                "bytes": max(1, int(row[2]) // scale), "declared": bool(row[3])}
               for row in rows]                              # type: ignore[union-attr]
    members: dict[int, list[int]] = {}
    for index, entry in enumerate(entries):
        members.setdefault(int(entry["file"]), []).append(index)
    multi = overlapping = 0
    for file_index, indices in members.items():
        if len(indices) < 2:
            continue
        multi += 1
        any_overlap = False
        for a, b in itertools.combinations(indices, 2):
            before = _overlap(int(rows[a][1]), int(rows[a][2]),   # type: ignore[index]
                              int(rows[b][1]), int(rows[b][2]))   # type: ignore[index]
            after = _overlap(int(entries[a]["offset"]), int(entries[a]["bytes"]),
                             int(entries[b]["offset"]), int(entries[b]["bytes"]))
            if before != after or entries[a]["offset"] == entries[b]["offset"]:
                raise ShapeGateFailure(
                    "shape_not_preserved",
                    f"scaling by {scale} changes how entries {a} and {b} of "
                    f"file {file_index} relate; choose a smaller scale")
            any_overlap = any_overlap or before
        overlapping += int(any_overlap)
    file_bytes = [0] * len(members)
    for entry in entries:
        end = int(entry["offset"]) + int(entry["bytes"])
        file_bytes[int(entry["file"])] = max(file_bytes[int(entry["file"])], end)
    return {"source": dict(table["source"]),                 # type: ignore[arg-type]
            "scale": scale, "entries": entries, "file_bytes": file_bytes,
            "phases": [dict(phase) for phase in table["phases"]],  # type: ignore[union-attr]
            "multi_range_files": multi, "overlapping_files": overlapping}


def shape_coverage(shape: Mapping[str, object], *, chunk_bytes: int) -> dict[str, object]:
    """What this shape exercises against a chunk of ``chunk_bytes``.

    The count that matters is byte cuts that land strictly inside an entry:
    the cuts a splitter that ignores entries would make, and each one is a
    pair of movers that overrun.  The shape is checked here, not assumed.
    """

    entries = shape["entries"]                               # type: ignore[index]
    boundaries = {0}
    position = 0
    spans: list[tuple[str, int, int]] = []
    for phase in shape["phases"]:                            # type: ignore[union-attr]
        start = position
        for index in phase["entry_indices"]:
            position += int(entries[index]["bytes"])         # type: ignore[index]
            boundaries.add(position)
        spans.append((str(phase["name"]), start, position))
    over: list[str] = []
    inside = 0
    for name, start, end in spans:
        if end - start <= chunk_bytes:
            continue
        over.append(name)
        cut = start + chunk_bytes
        while cut < end:
            inside += int(cut not in boundaries)
            cut += chunk_bytes
    return {"phases_over_chunk": over, "byte_cuts_inside_entries": inside,
            "chunk_bytes": chunk_bytes, "read_bytes": position}


# ------------------------------------------------------------------ sources


def write_sources(shape: Mapping[str, object], pool_dir: Path, *,
                  seed: int) -> tuple[list[str], list[str]]:
    """Write every source file once; return the paths and each entry's digest.

    The bytes are a seeded pseudo-random stream, so no two files share
    content.  Each entry's digest is taken from the buffer that was just
    written, never by reading a file back.
    """

    pool_dir.mkdir(parents=True, exist_ok=True)
    entries = shape["entries"]                               # type: ignore[index]
    members: dict[int, list[int]] = {}
    for index, entry in enumerate(entries):                  # type: ignore[arg-type]
        members.setdefault(int(entry["file"]), []).append(index)
    rng = random.Random(seed)
    paths: list[str] = []
    digests: list[str] = [""] * len(entries)                 # type: ignore[arg-type]
    for ordinal, size in enumerate(shape["file_bytes"]):     # type: ignore[arg-type]
        payload = rng.randbytes(int(size))
        path = pool_dir / f"f{ordinal:05d}.bin"
        with open(path, "wb") as stream:
            stream.write(payload)
        paths.append(str(path))
        view = memoryview(payload)
        for index in members.get(ordinal, ()):
            entry = entries[index]                           # type: ignore[index]
            offset, size_ = int(entry["offset"]), int(entry["bytes"])
            digests[index] = hashlib.sha256(view[offset:offset + size_]).hexdigest()
    return paths, digests


def build_manifest(shape: Mapping[str, object], paths: Sequence[str],
                   digests: Sequence[str], pool_dir: Path) -> dict[str, object]:
    """The scaled v2 manifest, digest-less exactly where the table's is."""

    entries = [{"path": paths[int(entry["file"])], "offset": int(entry["offset"]),
                "bytes": int(entry["bytes"]),
                "sha256": digests[index] if entry["declared"] else None}
               for index, entry in enumerate(shape["entries"])]  # type: ignore[arg-type]
    phases = []
    cumulative = 0
    for phase in shape["phases"]:                            # type: ignore[union-attr]
        size = sum(int(entries[index]["bytes"]) for index in phase["entry_indices"])
        cumulative += size
        phases.append({"name": str(phase["name"]),
                       "entry_indices": list(phase["entry_indices"]),
                       "bytes": size, "cumulative_bytes": cumulative})
    manifest = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V2,
        "produced_by": {"tool": "shape_gate",
                        "table_manifest_sha256":
                            str(shape["source"]["manifest_sha256"])},  # type: ignore[index]
        "annotations": {},
        "mount_prefix": str(pool_dir),
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": sum(int(entry["bytes"]) for entry in entries),
        "read_plan": {"phases": phases, "read_bytes": cumulative},
    }
    pb.validate_data_manifest(manifest)
    return manifest


# ------------------------------------------------------------------ audit


class _PoolOpens:
    """Counts opens under the pool directory while a named step is armed.

    One audit hook per process, installed on first use: a hook cannot be
    removed, so it reads module state and does nothing while disarmed.
    """

    installed = False
    prefix: str | None = None
    step: str | None = None
    hits: list[tuple[str, str]] = []

    @classmethod
    def install(cls) -> None:
        if not cls.installed:
            sys.addaudithook(cls._hook)
            cls.installed = True

    @classmethod
    def _hook(cls, event: str, args: tuple) -> None:
        if event != "open" or cls.step is None or cls.prefix is None or not args:
            return
        target = args[0]
        if isinstance(target, int):
            return
        try:
            text = os.fsdecode(target)
        except (TypeError, ValueError):
            return
        if text == cls.prefix or text.startswith(cls.prefix + os.sep):
            cls.hits.append((cls.step, text))

    @classmethod
    @contextlib.contextmanager
    def armed(cls, step: str):
        previous = cls.step
        cls.step = step
        try:
            yield
        finally:
            cls.step = previous


# ------------------------------------------------------------------ harness


def _tree_bytes(root: Path) -> int:
    """What a filesystem would report as used under ``root``: file bytes."""

    total = 0
    for directory, _dirs, files in os.walk(root):
        for name in files:
            try:
                total += os.lstat(os.path.join(directory, name)).st_size
            except OSError:
                continue
    return total


def _staged_bytes(root: Path) -> int:
    """The entry bytes a tier holds: files named for a manifest range.

    Every staged entry is ``<rel>.pbrange/<offset>-<size>`` under its root
    (``stage_move.stage_relative``); the epoch stamp and a copy's
    ``.partial`` are not entries.
    """

    total = 0
    for directory, _dirs, files in os.walk(root):
        if not directory.endswith(".pbrange"):
            continue
        for name in files:
            if name.startswith("."):
                continue
            try:
                total += os.lstat(os.path.join(directory, name)).st_size
            except OSError:
                continue
    return total


class _Statvfs:
    """A tmpfs of a given roof, answering with what its directory holds."""

    def __init__(self, root: Path, ceiling: int, frsize: int = 4096) -> None:
        self.root = root
        self.ceiling = ceiling
        self.frsize = frsize

    def __call__(self, path: str):
        used = _tree_bytes(self.root)
        free = max(0, self.ceiling - used)
        return os.statvfs_result((self.frsize, self.frsize,
                                  self.ceiling // self.frsize, free // self.frsize,
                                  free // self.frsize, 1 << 20, 1 << 20, 1 << 20,
                                  0, 255))


def _silenced(call: Callable[[], object]) -> tuple[object, str]:
    """Run ``call`` with stdout captured, as a worker's log would hold it."""

    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        value = call()
    return value, buffer.getvalue()


class ShapeGate:
    """One run of the gate over one scaled shape, on the caller's roots."""

    def __init__(self, shape: Mapping[str, object], *, root: Path,
                 shared_root: Path,
                 host_profile: Mapping[str, object] = HOST_PROFILE,
                 seed: int = 965) -> None:
        import pbrun
        import tier_loop

        if storage_tiers.GIB * int(shape["scale"]) != 1 << 30:     # type: ignore[arg-type]
            raise ShapeGateFailure(
                "unit_mismatch",
                f"storage_tiers.GIB is {storage_tiers.GIB}, but a shape scaled by "
                f"{shape['scale']} needs {(1 << 30) // int(shape['scale'])}")  # type: ignore[arg-type]
        if Path(pbrun.SH) != Path(shared_root):
            raise ShapeGateFailure(
                "shared_root_mismatch",
                f"pbrun seals against {pbrun.SH}, not {shared_root}; a mover "
                f"sealed there would run against another queue")
        self.pbrun = pbrun
        self.tier_loop = tier_loop
        self.shape = shape
        self.scale = int(shape["scale"])                     # type: ignore[arg-type]
        self.root = Path(root)
        self.shared = Path(shared_root)
        self.profile = dict(host_profile)
        self.host = str(self.profile["host"])
        self.seed = seed
        self.pool_dir = self.root / "pool"
        self.stage_dir = self.root / "stage"
        self.ram_dir = self.root / "ram"
        self.host_dir = self.root / "host-facts"
        self.queue = pool.PoolQueue(self.shared / "pb-queue")
        self.cas = pb.PrismaBuildCAS(self.shared / "cas")
        # The policy ``cycle`` reads on every pass, from this checkout: the
        # window and chunk the gate reasons about are the ones it mints.
        policy = tier_loop.load_ram_policy()
        if policy is None:
            raise ShapeGateFailure(
                "no_ram_policy",
                f"{storage_tiers.RAM_POLICY_FILE} is missing or refused beside "
                f"tier_loop.py; a gate with no RAM tier tests half the path")
        self.ram_policy = {**dict(policy), "mountpoint": str(self.ram_dir)}
        self.stage_tier_id = storage_tiers.tier_id(
            "stage", self.host, str(self.profile["stage_pool"]))
        self.ram_tier_id = storage_tiers.tier_id("ram", self.host)
        self.window_gib = int(self.ram_policy["window_gib_default"])
        self.chunk_gib = storage_tiers.promotion_chunk_gib_for_window(
            self.window_gib, self.ram_policy.get("promotion_chunk_gib"))  # type: ignore[arg-type]
        worker = self.profile["worker"]
        self.capacity = {"cpu": int(worker["cpu"]),                 # type: ignore[index]
                         "mem_gb": int(worker["mem_gb"])}           # type: ignore[index]
        # A fleet worker's identity is ``<host>:<pid>:<tag>``.
        self.worker_owner = f"{self.host}:{os.getpid()}:shape-gate-worker"
        self.reader_owner = f"{self.host}:{os.getpid()}:shape-gate-reader"
        self.timings: dict[str, float] = {}
        self.events: list[dict[str, object]] = []
        self.ran: dict[str, list[dict[str, object]]] = {
            "stage_move.py": [], "ram_promote.py": [], "stage_release.py": []}
        self.read: list[dict[str, object]] = []
        self.ram_peak_bytes = 0
        self.cycles = 0

    # -- the storage host's discovery ------------------------------------

    def _host_facts(self) -> None:
        """The files ``storage_tiers.ram_tier`` reads, as the host has them."""

        self.host_dir.mkdir(parents=True, exist_ok=True)
        ceiling = int(self.profile["tmpfs_ceiling_bytes"]) // self.scale  # type: ignore[arg-type]
        (self.host_dir / "mounts").write_text(
            f"tmpfs {self.ram_dir} tmpfs rw,relatime,size={ceiling // 1024}k,"
            f"mode=755,inode64,noswap 0 0\n")
        mem_total = int(self.profile["mem_total_bytes"]) // self.scale  # type: ignore[arg-type]
        (self.host_dir / "meminfo").write_text(f"MemTotal: {mem_total // 1024} kB\n")
        self.ram_ceiling = ceiling
        self.arc = {"c_max": int(self.profile["arc_c_max_bytes"]) // self.scale,  # type: ignore[arg-type]
                    "size": int(self.profile["arc_size_bytes"]) // self.scale,  # type: ignore[arg-type]
                    "arc_meta_used": int(self.profile["arc_meta_used_bytes"]) // self.scale}  # type: ignore[arg-type]

    def discover(self, *, host, source_pool, fill_records, now, ram_policy,
                 worker_mem_gb):
        """The storage host's two tiers, as ``discover_tiers`` would find them.

        The stage is the pool's writable bytes, which shrink as bytes land,
        the way ``zfs available`` does.  The RAM tier is the production
        record builder over the policy ``cycle`` read from this checkout,
        with the host's tmpfs, memory and ARC facts, and the worker demand
        the loop read from the host's own announcement.  The stage names no
        pool identity and no fill of its own; ``cycle`` prices its fill from
        the movers' receipts, as it does for a tier with no fill history.
        """

        del source_pool, fill_records
        stage_total = int(self.profile["stage_empty_gib"]) * storage_tiers.GIB  # type: ignore[arg-type]
        stage = {
            "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
            "tier": "stage", "tier_id": self.stage_tier_id, "host": host,
            "pool": str(self.profile["stage_pool"]),
            "dataset": f"{self.profile['stage_pool']}/prewarm",
            "mountpoint": str(self.stage_dir),
            "capacity_bytes": max(0, stage_total - _tree_bytes(self.stage_dir)),
            "capacity_source": storage_tiers.WRITABLE_CAPACITY_SOURCE,
            "source_pool": str(self.profile["source_pool"]),
            "primarycache": "all",
            "sampled_unix": time.time() if now is None else now,
        }
        policy = {**dict(ram_policy), "mountpoint": str(self.ram_dir)} if ram_policy else None
        tiers = {self.stage_tier_id: stage}
        if policy is not None:
            ram = storage_tiers.ram_tier(
                policy, host=host, statvfs=_Statvfs(self.ram_dir, self.ram_ceiling),
                proc_mounts=str(self.host_dir / "mounts"),
                meminfo_path=str(self.host_dir / "meminfo"), stats=self.arc,
                now=now, worker_mem_gb=worker_mem_gb)
            if ram is not None:
                tiers[self.ram_tier_id] = ram
        return tiers

    def cycle(self) -> list[dict[str, object]]:
        announced, log = _silenced(lambda: self.tier_loop.cycle(
            self.queue, host=self.host, source_pool=str(self.profile["source_pool"]),
            receipts=self.receipts, discover=self.discover))
        self.cycles += 1
        for line in log.splitlines():
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if isinstance(event, dict):
                self.events.append(event)
        self.ram_peak_bytes = max(self.ram_peak_bytes, _staged_bytes(self.ram_dir))
        return list(announced)                               # type: ignore[arg-type]

    # -- the submission ---------------------------------------------------

    def _template(self) -> dict[str, object]:
        """The frozen template a submission seals its consumer from.

        The shape ``pbrun`` freezes before it adds the data manifest: the
        manifest input and its summary are added by the seal, as
        ``seal_action_from_template`` adds them for a released consumer.
        """

        pbrun = self.pbrun
        snapshot = {"id": pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID,
                    "sha256": hashlib.sha256(b"shape-gate-checkout").hexdigest(),
                    "bytes": 4096}
        return {
            "cas": None, "marker_root": self.root / "markers",
            "checkout_identity": {"commit": "0" * 40},
            "log_name": "shape-gate.log", "stamp_name": "pbrun.stamp",
            "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                     "task_class": "generation", "determinism": "stochastic",
                     "artifact_family": "generic", "artifact_kind": "generic",
                     "working_directory": "."},
            "inputs": [snapshot],
            "code_closure": pbrun.build_stamp_closure("pbrun.stamp", "{}"),
            "params": {"command": ["true"], "cwd": str(self.root),
                       "demand": {"cpu": 1, "mem_gb": 1},
                       "placement": {"required_tags": []},
                       "checkout_snapshot": {
                           "schema": pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
                           "commit": "0" * 40, "subdirectory": ".",
                           "input": snapshot},
                       "retry_policy": {"max_attempts": 1, "retry_safe": False}},
            "environment": {"variables": {"PATH": "/usr/bin:/bin"}, "toolchain": {}},
            "execution_scope": {"portability": "portable", "platform_key": None,
                                "host_class": None},
        }

    def submit(self, manifest: Mapping[str, object]) -> str:
        """Seal and publish the consumer the way ``pbrun`` does.

        The manifest goes into the CAS as one gzip member, the encoding a
        campaign's manifest has, and the summary is the one ``pbrun`` builds
        for a v2 plan.  ``publish_consumer_row`` then seals the whole window
        and publishes the consumer's row.
        """

        raw = gzip.compress(pb._canonical_file_bytes(manifest), mtime=0)
        manifest_input, _ = self.cas.ingest_bytes(
            raw, input_id=pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
        self.manifest_sha256 = str(manifest_input["sha256"])
        summary = {"input": dict(manifest_input),
                   "mount_prefix": manifest["mount_prefix"],
                   "entry_count": manifest["entry_count"],
                   "total_bytes": manifest["total_bytes"],
                   "schema": pb.DATA_MANIFEST_SCHEMA_V2,
                   "read_bytes": manifest["read_plan"]["read_bytes"],  # type: ignore[index]
                   "content_encoding": "gzip"}
        template = self._template()
        action = self.pbrun.seal_action_from_template(
            template, command=["true"], extra_inputs=[manifest_input],
            extra_params={"data_manifest": summary})
        sealed = {**template,
                  "params": {**template["params"], "data_manifest": summary},  # type: ignore[dict-item]
                  "inputs": [*template["inputs"], manifest_input]}  # type: ignore[misc]
        self.cas.publish_action_request(action)
        key = str(action["action_key"])
        args = self.pbrun.parse_args(
            ["--residency", "stage", "--priority", "-10", "--", "true"])
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            self.pbrun.publish_consumer_row(
                self.queue, action, sealed, key=key, args=args, cas=self.cas)
        self.submit_log = stderr.getvalue()
        return key

    # -- the worker -------------------------------------------------------

    def _movement_module(self, tool: str):
        import ram_promote
        import stage_move
        import stage_release

        return {"stage_move.py": stage_move, "ram_promote.py": ram_promote,
                "stage_release.py": stage_release}[tool]

    def _execute(self, item: Mapping[str, object]) -> None:
        """Run one claimed movement node's sealed command in this process.

        The command is read from the node's own sealed request, as a worker
        reads it.  Two things differ from a worker's launch, both recorded
        on the result: the key arrives through ``ACTION_KEY_ENV`` as the
        launcher sets it, and a stage mover runs ``--unpaced``, because the
        gate's pool is a directory with no disks to pace.
        """

        key = str(item["action_key"])
        request_path = (Path(str(item["cas_root"])) / "requests" / key[:2]
                        / f"{key}.json")
        request = json.loads(request_path.read_text())
        command = [str(part) for part in request["params"]["command"]]
        tool = Path(command[1]).name
        if tool not in self.ran:
            raise ShapeGateFailure(
                "unexpected_action", f"{key[:12]} runs {tool!r}, not a movement node")
        argv = command[2:] + (["--unpaced"] if tool == "stage_move.py" else [])
        module = self._movement_module(tool)
        step = {"ram_promote.py": "promote", "stage_release.py": "egress"}.get(tool)
        previous = os.environ.get(pb.ACTION_KEY_ENV)
        os.environ[pb.ACTION_KEY_ENV] = key
        started = time.monotonic()
        buffer = io.StringIO()
        try:
            with contextlib.ExitStack() as stack:
                if step is not None:
                    stack.enter_context(_PoolOpens.armed(step))
                stack.enter_context(contextlib.redirect_stdout(buffer))
                try:
                    code = module.main(argv)
                except SystemExit as exc:
                    code = exc.code
                    print(f"SystemExit: {exc}")
        finally:
            if previous is None:
                os.environ.pop(pb.ACTION_KEY_ENV, None)
            else:
                os.environ[pb.ACTION_KEY_ENV] = previous
        seconds = time.monotonic() - started
        receipt = self.queue.move_record(key)
        root = argv[argv.index("--stage-root") + 1] if "--stage-root" in argv else None
        record = {"action_key": key, "code": code, "seconds": round(seconds, 3),
                  "tier": ("ram" if root is not None and Path(root) == self.ram_dir
                           else "stage"),
                  "receipt": receipt if isinstance(receipt, dict) else None}
        self.ran[tool].append(record)
        if code not in (0, None):
            refusal = (receipt.get("refusal") if isinstance(receipt, dict) else None)
            raise ShapeGateFailure(
                "movement_refused",
                f"{tool} {key[:12]} exited {code}: "
                f"{refusal or buffer.getvalue().strip()[-2000:]}",
                {"tool": tool, "action_key": key, "receipt": receipt,
                 "log_tail": buffer.getvalue()[-4000:]})
        self.queue.finish(key, status="executed", claim_snapshot=dict(item))

    def work(self) -> int:
        """Claim and run every movement node the queue will admit now."""

        ran = 0
        while True:
            ready = [item for item in self.queue.ready_items()
                     if str(item.get("action_key")) != self.consumer]
            if not ready:
                return ran
            got = self.queue.claim(tags=[self.host], owner=self.worker_owner,
                                   capacity=self.capacity, ready=ready)
            if got is None:
                return ran
            self._execute(got)
            ran += 1

    # -- the reader -------------------------------------------------------

    def _claim_consumer(self) -> dict[str, object] | None:
        ready = [item for item in self.queue.ready_items()
                 if str(item.get("action_key")) == self.consumer]
        if not ready:
            return None
        got = self.queue.claim(tags=[self.host], owner=self.reader_owner, ready=ready)
        if got is None:
            return None
        if str(got.get("action_key")) != self.consumer:
            raise ShapeGateFailure("claim_mismatch", "the reader claimed another item")
        return dict(got)

    def _report(self, phase: str, units: int) -> None:
        claimed = pool._read_json(self.queue.item_path(pool.CLAIMED, self.consumer))
        self.queue.write_lease(
            self.consumer, owner=self.reader_owner,
            claim_snapshot=claimed if isinstance(claimed, dict) else None,
            progress_observation={
                "source": "action-progress",
                "last_accepted": {"phase": phase, "units_completed": units,
                                  "reported_unix": time.time()}})

    def _ram_map(self, phase_index: int) -> dict[str, object] | None:
        """The composed map, when it lays every entry of this phase on RAM.

        The one read of the map per poll: the reader that finds the phase
        ready reads through this same document.
        """

        try:
            mapping = residency_map.read_map(
                self.queue.residency_map_path(self.consumer))
        except (OSError, ValueError):
            return None
        if mapping.get("ram_tier_id") != self.ram_tier_id:
            return None
        for index in self.phases[phase_index]["entry_indices"]:
            entry = self.manifest["entries"][index]           # type: ignore[index]
            found = residency_map.lookup(mapping, str(entry["path"]), int(entry["offset"]))
            if found is None or "ram_path" not in found:
                return None
        return mapping

    def read_phase(self, phase_index: int, mapping: Mapping[str, object]) -> None:
        """Read one phase strictly: map, cover, pin, open, hash, release."""

        epoch = str(mapping.get("ram_epoch") or "")
        root = self.queue.root / pool.RESIDENCY
        indices = list(self.phases[phase_index]["entry_indices"])
        entries = [self.manifest["entries"][index] for index in indices]  # type: ignore[index]
        keys = [residency_map.residency_map_key(str(entry["path"]), int(entry["offset"]))
                for entry in entries]
        with _PoolOpens.armed("read"):
            covers = reader_lease.covers_for_keys(
                root, self.consumer, keys, tier_id=self.ram_tier_id,
                manifest_sha256=self.manifest_sha256, epoch=epoch)
            if not covers.get("ok"):
                raise ShapeGateFailure(
                    "not_resident_in_ram",
                    f"phase {self.phases[phase_index]['name']!r}: "
                    f"{covers.get('refusal')}")
            expected = {key: {"bytes": int(entry["bytes"]),
                              "sha256": self.digests[index]}
                        for key, entry, index in zip(keys, entries, indices)}
            start = sum(int(self.phase_bytes[i]) for i in range(phase_index))
            acquired = reader_lease.acquire(
                self.queue, consumer_action_key=self.consumer,
                attempt={"nonce": "5" * 32, "scope_id": "shape-gate"},
                tier_id=self.ram_tier_id, epoch=epoch,
                span={"start_bytes": start,
                      "end_bytes": start + int(self.phase_bytes[phase_index])},
                holder={"host": socket.gethostname(), "pid": os.getpid()},
                acquire_token=f"shape-gate:{phase_index}",
                covers=covers["covers"], expected=expected)  # type: ignore[arg-type]
            if not acquired.get("ok"):
                raise ShapeGateFailure(
                    "pin_refused",
                    f"phase {self.phases[phase_index]['name']!r}: "
                    f"{acquired.get('refusal')}")
            pinned = {str(item["key"]): str(item["stage_path"])
                      for item in acquired["pin"]["entries"]}   # type: ignore[index]
            mapped = mapping["entries"]                          # type: ignore[index]
            strays = [key for key in keys
                      if pinned.get(key) != mapped[key].get("ram_path")]  # type: ignore[index]
            if strays:
                raise ShapeGateFailure(
                    "pin_off_the_map",
                    f"{len(strays)} entries of phase "
                    f"{self.phases[phase_index]['name']!r} are pinned at a path "
                    f"the map does not name for RAM, e.g. {strays[0]}")
            try:
                for key, entry, index in zip(keys, entries, indices):
                    fd, serving = reader_lease.open_pinned(
                        self.queue, acquired["pin"], str(acquired["ref_id"]), key)
                    try:
                        digest = hashlib.sha256()
                        remaining = int(entry["bytes"])
                        while remaining:
                            piece = os.read(fd, min(remaining, 1 << 20))
                            if not piece:
                                break
                            digest.update(piece)
                            remaining -= len(piece)
                    finally:
                        os.close(fd)
                    if remaining or digest.hexdigest() != self.digests[index]:
                        raise ShapeGateFailure(
                            "digest_mismatch",
                            f"entry {index} ({entry['path']} at {entry['offset']}) "
                            f"read back different bytes from {serving.get('tier_id')}")
                    if str(serving.get("tier_id")) != self.ram_tier_id:
                        raise ShapeGateFailure(
                            "served_off_ram",
                            f"entry {index} was served by {serving.get('tier_id')}")
                    self.read.append({"entry": index, "bytes": int(entry["bytes"])})
            finally:
                released = reader_lease.release(
                    self.queue, str(acquired["pin_id"]), str(acquired["ref_id"]),
                    consumer_action_key=self.consumer)
            if not released:
                raise ShapeGateFailure("release_refused",
                                       f"phase {self.phases[phase_index]['name']!r}")

    # -- the run ----------------------------------------------------------

    def run(self) -> dict[str, object]:
        _PoolOpens.install()
        _PoolOpens.prefix = str(self.pool_dir)
        _PoolOpens.hits = []
        began = time.monotonic()
        chunk_bytes = self.chunk_gib * storage_tiers.GIB
        coverage = shape_coverage(self.shape, chunk_bytes=chunk_bytes)
        if not coverage["phases_over_chunk"] or not coverage["byte_cuts_inside_entries"]:
            raise ShapeGateFailure(
                "did_not_test",
                f"no phase is larger than a {self.chunk_gib}-unit chunk, or no "
                f"byte cut lands inside an entry: {coverage}")

        mark = time.monotonic()
        paths, self.digests = write_sources(self.shape, self.pool_dir, seed=self.seed)
        self.manifest = build_manifest(self.shape, paths, self.digests, self.pool_dir)
        self.phases = self.manifest["read_plan"]["phases"]   # type: ignore[index]
        self.phase_bytes = [int(phase["bytes"]) for phase in self.phases]
        self.timings["sources_s"] = time.monotonic() - mark

        for directory in (self.stage_dir, self.ram_dir):
            directory.mkdir(parents=True, exist_ok=True)
        self._host_facts()
        self.queue.ensure_layout()
        # What the storage host's worker loops offer.  The tier loop reads
        # this record for the RAM admission's worker demand, and the claim
        # reads it for placement.
        self.queue.announce(host=self.host, tags=[self.host], has_gpu=False,
                            capacity=self.capacity)
        self.receipts = self.tier_loop.ReceiptCache()

        mark = time.monotonic()
        announced = self.cycle()
        ram = [record for record in announced if record.get("tier") == "ram"]
        if not ram or not storage_tiers.tier_tokens(ram[0]).get(storage_tiers.RAM_CAPACITY_KIND):
            raise ShapeGateFailure(
                "ram_tier_not_admitted",
                f"the RAM tier minted nothing: {ram[0].get('ram_admission') if ram else 'absent'}")
        self.consumer = self.submit(self.manifest)
        plan = residency_plan.read(self.queue, self.consumer)
        self.timings["seal_s"] = time.monotonic() - mark

        mark = time.monotonic()
        claimed = None
        phase_index = 0
        idle = 0
        while phase_index < len(self.phases):
            before = len(self.events)
            self.cycle()
            progressed = self.work() > 0
            if claimed is None:
                claimed = self._claim_consumer()
                if claimed is not None:
                    self._report(str(self.phases[0]["name"]), 1)
                    progressed = True
            else:
                mapping = self._ram_map(phase_index)
                if mapping is not None:
                    self.read_phase(phase_index, mapping)
                    phase_index += 1
                    if phase_index < len(self.phases):
                        self._report(str(self.phases[phase_index]["name"]),
                                     phase_index + 1)
                    progressed = True
            idle = 0 if progressed else idle + 1
            if idle > MAX_IDLE_CYCLES:
                raise ShapeGateFailure(
                    "stalled",
                    f"{idle} cycles with nothing claimed or read, reading "
                    f"phase {phase_index} ({self.phases[phase_index]['name']!r}); "
                    f"consumer claimed: {claimed is not None}",
                    {"recent_events": self.events[before:][-40:]})
        self.queue.finish(self.consumer, status="executed", claim_snapshot=claimed)
        self.timings["window_s"] = time.monotonic() - mark

        result = self._result(plan, coverage)
        result["timings_s"] = {name: round(value, 3) for name, value in self.timings.items()}
        result["timings_s"]["total_s"] = round(time.monotonic() - began, 3)
        return result

    def _result(self, plan: object, coverage: Mapping[str, object]) -> dict[str, object]:
        total = int(self.manifest["total_bytes"])            # type: ignore[arg-type]

        def moved(tool: str) -> int:
            return sum(int((record["receipt"] or {}).get("bytes_staged") or 0)
                       for record in self.ran[tool])

        phases = plan.get("phases", []) if isinstance(plan, dict) else []
        chunks = {"stage": sum(len(phase.get("stage_chunks") or [phase]) for phase in phases),
                  "ram": sum(len(phase.get("ram_chunks") or [phase]) for phase in phases
                             if "ram_mover_row" in phase or "ram_chunks" in phase)}
        opens = {"promote": 0, "read": 0, "egress": 0}
        for step, _path in _PoolOpens.hits:
            opens[step] = opens.get(step, 0) + 1
        return {
            "schema": RESULT_SCHEMA_V1,
            "table": dict(self.shape["source"]),              # type: ignore[arg-type]
            "scale": self.scale,
            "unit_bytes": storage_tiers.GIB,
            "entries": len(self.manifest["entries"]),         # type: ignore[arg-type]
            "phases": len(self.phases),
            "multi_range_files": self.shape["multi_range_files"],
            "overlapping_files": self.shape["overlapping_files"],
            "digest_less_entries": sum(1 for entry in self.manifest["entries"]  # type: ignore[union-attr]
                                       if entry["sha256"] is None),
            "coverage": dict(coverage),
            "chunk_units": self.chunk_gib,
            "ram_window_units": self.window_gib,
            "chunks": chunks,
            "manifest_bytes": total,
            "stage_bytes_moved": moved("stage_move.py"),
            "ram_bytes_moved": moved("ram_promote.py"),
            "bytes_read_strictly": sum(int(item["bytes"]) for item in self.read),
            "entries_read_strictly": len(self.read),
            "stage_movers": len(self.ran["stage_move.py"]),
            "ram_promotions": len(self.ran["ram_promote.py"]),
            "stage_egresses": sum(1 for record in self.ran["stage_release.py"]
                                  if record["tier"] == "stage"),
            "ram_egresses": sum(1 for record in self.ran["stage_release.py"]
                                if record["tier"] == "ram"),
            "ram_peak_bytes": self.ram_peak_bytes,
            "ram_window_bytes": self.window_gib * storage_tiers.GIB,
            "pool_opens": opens,
            "cycles": self.cycles,
            "adaptations": [
                "each node's key arrives through ACTION_KEY_ENV, as the launcher sets it",
                "stage movers run --unpaced: the gate's pool is a directory",
                "the stage's fill is priced from the gate's own receipts, "
                "as a tier with no fill history prices it",
            ],
        }


def run_gate(table: Mapping[str, object], *, root: Path, shared_root: Path,
             scale: int = SCALE,
             host_profile: Mapping[str, object] = HOST_PROFILE) -> dict[str, object]:
    """Scale ``table``, stage it through both tiers, read it back, and judge.

    Returns the result on a pass.  Raises :class:`ShapeGateFailure` otherwise:
    a refused mover, a stalled window, a read the RAM tier could not serve,
    bytes that differ, a pool open during a promotion or a read, bytes moved
    twice, or a shape that tested nothing.
    """

    shape = scaled_shape(table, scale=scale)
    gate = ShapeGate(shape, root=root, shared_root=shared_root,
                     host_profile=host_profile)
    result = gate.run()
    total = int(result["manifest_bytes"])
    problems = []
    for field in ("stage_bytes_moved", "ram_bytes_moved", "bytes_read_strictly"):
        if int(result[field]) != total:
            problems.append(f"{field} is {result[field]}, not the manifest's {total}")
    if int(result["entries_read_strictly"]) != int(result["entries"]):
        problems.append("not every entry was read back")
    if any(result["pool_opens"].values()):                   # type: ignore[union-attr]
        problems.append(f"the pool was opened outside a stage mover: {result['pool_opens']}")
    if int(result["ram_peak_bytes"]) > int(result["ram_window_bytes"]):
        problems.append("the RAM tier held more than its window")
    if total > int(result["ram_window_bytes"]) and not int(result["ram_egresses"]):
        problems.append("the shape is larger than the RAM window and nothing "
                        "egressed from RAM")
    if problems:
        raise ShapeGateFailure("result_refused", "; ".join(problems), result)
    return result


# ------------------------------------------------------------------ CLI


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    extract = sub.add_parser("extract", help="write a shape table from a manifest")
    extract.add_argument("manifest", help="a data manifest file or CAS blob")
    extract.add_argument("--out", help="the table path; default "
                         "shape_gate_tables/<sha12>.json.gz")
    args = parser.parse_args(argv)
    if args.command == "extract":
        # One read: the digest is the CAS name of these bytes, and the same
        # bytes are decoded.  ``table_from_manifest`` validates the result.
        with open(args.manifest, "rb") as stream:
            raw = stream.read()
        digest = hashlib.sha256(raw).hexdigest()
        decoded = gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw
        table = table_from_manifest(json.loads(decoded), manifest_sha256=digest)
        out = Path(args.out) if args.out else TABLE_ROOT / f"{digest[:12]}.json.gz"
        out.parent.mkdir(parents=True, exist_ok=True)
        encoded = table_bytes(table)
        out.write_bytes(encoded)
        print(json.dumps({"table": str(out), "sha256": hashlib.sha256(encoded).hexdigest(),
                          **table["source"]}, sort_keys=True))  # type: ignore[dict-item]
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
