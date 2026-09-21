"""pbcanary leg 3 — chunked residency (PB #688, staged routing #784).

24 MiB of deterministic bytes as a 3-chunk v2 data-manifest, submitted with
``--residency stage``; the worker action serves every chunk from the
residency map's staged copy through the existing reader-lease pin
(``PRISMABUILD_RESIDENCY_MAP`` + ``injected_context``/``acquire_for``/
``open_pinned``/``release``), SHA-256-verifies each range against the
manifest, and updates the combined digest in the same read.  It never opens
the declared origin path: a missing map, unpublished coverage, or absent
launch identity refuses with no passing envelope, even when the originals
are readable.  Exercises movers, tier staging, chunk progress phases, and
egress.

Driver flow (the driver owns waiting, placement records, and CAS paths;
this module owns the fixed spec and the mechanical verdict):

  1. ``spec = build()`` — fixed spec; refuses unless recomputed digests
     match the pinned constants below.
  2. Driver writes the 3 chunk files into the run namespace
     (``/mnt/shared/prismabuild-fleet/pb-canary/<run-id>/``) with
     ``write_chunk_files(run_dir)`` (or equivalently ``chunk_bytes(i)``).
  3. Driver renders the manifest with ``manifest_for_run(chunk_paths,
     mount_prefix)`` and submits::

         pbrun --cwd <checkout> --data-manifest <manifest> --residency stage \\
             --progress-phase leg3-c0=600 --progress-phase leg3-c1=600 \\
             --progress-phase leg3-c2=600 \\
             --timeout-s 600 --wait-s 900 --deterministic -- \\
             python3 tools/fleet/pbcanary_legs/leg3.py --run-action

      (the driver injects its own ``--priority`` once for every leg;
      the spec no longer pins one — issue #690)

     with ``PBCANARY_LEG3_MANIFEST=<absolute manifest path>`` in the
     action environment. The manifest's ``read_plan`` phases and the
     ``--progress-phase`` names are identical strings: the tiers loop
     publishes the rest of the staging window as the action's accepted
     progress advances.
  4. The action prints one canonical-JSON envelope line (see
     ``LEG3_SCHEMA``) on stdout and exits 0 iff every staged range
     matches its manifest digest, nonzero otherwise.
  5. Driver calls ``verify(receipt, spec["expected"])``; exit 1 names
     the refusing check, exit 2 is the driver's precondition refusal.

Interface contract (same as legs 1-2): ``build() -> dict``,
``verify(receipt, expected) -> (ok, reason)``. No runtime intelligence:
``build`` computes nothing from the live fleet, ``verify`` decides
nothing — it checks fixed expectations in a fixed order.

Stdlib only: no PrismaBuild imports, so the module runs unmodified both
in the driver and inside the sealed worker action.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import posixpath
import sys
import time

sys.path.insert(0, str(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")))
try:
    from pbcanary_legs.common import (
        canonical_json,
        deterministic_bytes,
        deterministic_stream,
        sha256_hex,
        sha256_stream_hex,
    )
except ImportError:  # Worker runs the file directly: sibling import.
    from common import (  # type: ignore[no-redef]
        canonical_json,
        deterministic_bytes,
        deterministic_stream,
        sha256_hex,
        sha256_stream_hex,
    )

LEG3_NAME = "leg-3"
LEG3_SCHEMA = "prismabuild.pbcanary.leg3.v1"

#: v2 data-manifest schema id. Literal (no prismabuild import; see module
#: docstring): must equal ``core.DATA_MANIFEST_SCHEMA_V2``.
LEG3_MANIFEST_SCHEMA = "prismaquant.prismabuild.data_manifest.v2"

#: One durable per-chunk verification checkpoint, written by the action
#: beside its manifest before that chunk's progress unit is reported.
LEG3_CHECKPOINT_SCHEMA = "prismabuild.pbcanary.leg3_checkpoint.v1"

LEG3_WAIT_S = 900  # Issue #688 residency wait budget.
LEG3_TIMEOUT_S = 600  # Execution deadline: read 24 MiB + hash takes seconds.
LEG3_PRIORITY = -10  # Documented default; the driver's --priority governs
# the submission (issue #690 dedupe: specs do not pin --priority).

LEG3_PHASES = ("leg3-c0", "leg3-c1", "leg3-c2")
LEG3_CHUNK_NAMES = ("leg3-chunk-0.bin", "leg3-chunk-1.bin", "leg3-chunk-2.bin")
LEG3_CHUNK_SIZES = (6 * 1048576, 8 * 1048576, 10 * 1048576)  # 24 MiB total.
LEG3_PROGRESS_ALLOWANCE_S = 600

#: Fixed expectations (PB #688: "a fixed expected outcome"). ``build()``
#: recomputes every digest from the seeds and refuses unless all match.
LEG3_EXPECTED_CHUNK_SHA256 = (
    "f14d66167aed483351f1d4c6169b373fa119fd74db6c4792e17299786d38f880",
    "4fe7368e989c50d55f5e618127cd6a50e7202914c97e5e6210502d186b11e299",
    "b2da8102fb57dbc032ac96eab8487e7934ccf04bb2d0f6ada7f2811c5e5c0f40",
)
LEG3_EXPECTED_COMBINED = (
    "529bb1dc00a6cf45310deeafd38a1a3eab025cd78bf1dbf9009243bad85ac5af"
)

_ACTION_ENV_MANIFEST = "PBCANARY_LEG3_MANIFEST"

#: One chunk's staged coverage may still be publishing while this reader is
#: at an earlier phase of the moving window (the plan stages later phases as
#: accepted progress advances; the lease carries that progress at heartbeat
#: cadence).  Wait boundedly inside the declared 600 s per-phase allowance
#: instead of demanding the whole working set up front; the override exists
#: for bounded tests and operator diagnosis.
LEG3_CHUNK_WAIT_S = 300.0
LEG3_STAGE_WAIT_ENV = "PBCANARY_LEG3_STAGE_WAIT_S"
LEG3_WAIT_POLL_S = 0.5

#: Cover-lookup refusals a later phase can cure: the mover has not published
#: this range yet.  Everything else (malformed material, ownership
#: uncertain, identity changed) is terminal and refuses immediately.
LEG3_RETRYABLE_REFUSALS = frozenset({"unpublished", "source-coverage-gap"})

#: The staged storage tiers a served read may name.  Literals (no
#: prismabuild import on the driver side), equal to
#: ``storage_tiers.STAGE_POOL_PREFIX``/``storage_tiers.RAM_TIER_PREFIX``.
LEG3_STAGED_TIER_PREFIXES = ("prismabuild-stage:", "ram:")

_ACTION_ENV_PROGRESS_PATH = "PRISMABUILD_ACTION_PROGRESS_PATH"
_ACTION_ENV_PROGRESS_TOKEN = "PRISMABUILD_ACTION_PROGRESS_TOKEN"
_ACTION_ENV_PROGRESS_HELPER = "PRISMABUILD_ACTION_PROGRESS_HELPER"
_READER_HELPER_ROOT_ENV = "PRISMABUILD_READER_HELPER_ROOT"

_HEX = frozenset("0123456789abcdef")


class _StagedReadRefusal(RuntimeError):
    """The staged-read gate refused; there is no origin fallback."""


def _seed(index: int) -> bytes:
    return b"/leg3/chunk%d" % index


def chunk_bytes(index: int) -> bytes:
    """The deterministic content of chunk ``index`` (0-based)."""
    return deterministic_bytes(_seed(index), LEG3_CHUNK_SIZES[index])


def _check_pinned() -> list[dict]:
    """Recompute chunk digests and refuse unless they match the constants."""
    chunks = []
    for index, (name, size) in enumerate(zip(LEG3_CHUNK_NAMES, LEG3_CHUNK_SIZES)):
        digest = sha256_hex(chunk_bytes(index))
        if digest != LEG3_EXPECTED_CHUNK_SHA256[index]:
            raise RuntimeError(
                f"leg3: pinned digest mismatch for {name}: "
                "regenerate the LEG3_EXPECTED_* constants, do not ship this"
            )
        chunks.append(
            {
                "name": name,
                "size": size,
                "seed_hex": _seed(index).hex(),
                "sha256": digest,
            }
        )
    combined = sha256_stream_hex(
        piece
        for index, size in enumerate(LEG3_CHUNK_SIZES)
        for piece in deterministic_stream(_seed(index), size)
    )
    if combined != LEG3_EXPECTED_COMBINED:
        raise RuntimeError(
            "leg3: pinned combined-digest mismatch: "
            "regenerate LEG3_EXPECTED_COMBINED, do not ship this"
        )
    return chunks


def build() -> dict:
    """Return the fixed leg-3 submission/verification spec.

    Pure data plus pinned digests; reads nothing from the fleet and takes
    no arguments, so re-runs create an identical spec (the run namespace
    enters only via ``manifest_for_run`` at submit time).
    """
    chunks = _check_pinned()
    total = sum(LEG3_CHUNK_SIZES)
    phases = []
    cumulative = 0
    for phase_index, (phase, chunk) in enumerate(zip(LEG3_PHASES, chunks)):
        cumulative += chunk["size"]
        phases.append(
            {
                "name": phase,
                "entry_indices": [phase_index],
                "bytes": chunk["size"],
                "cumulative_bytes": cumulative,
            }
        )
    return {
        "name": LEG3_NAME,
        "schema": LEG3_SCHEMA,
        "wait_s": LEG3_WAIT_S,
        "timeout_s": LEG3_TIMEOUT_S,
        "priority": LEG3_PRIORITY,
        "total_bytes": total,
        "chunks": chunks,
        "expected": {
            "chunks": [
                {"name": c["name"], "size": c["size"], "sha256": c["sha256"]}
                for c in chunks
            ],
            "combined": LEG3_EXPECTED_COMBINED,
        },
        # Manifest skeleton: the driver fills ``mount_prefix`` and the
        # entry ``path`` values with ``manifest_for_run``. Everything else
        # is fixed here so the staging identity cannot drift per run.
        "manifest": {
            "schema": LEG3_MANIFEST_SCHEMA,
            "produced_by": {"tool": "pbcanary", "leg": 3},
            "annotations": {"producer": "pbcanary", "leg": 3},
            "read_plan": {"phases": phases, "read_bytes": total},
        },
        "progress_phases": [
            f"{phase}={LEG3_PROGRESS_ALLOWANCE_S}" for phase in LEG3_PHASES
        ],
        "action": {
            "argv": ["python3", "tools/fleet/pbcanary_legs/leg3.py", "--run-action"],
            "env": {_ACTION_ENV_MANIFEST: "<run-namespace>/leg3.manifest.json"},
        },
        "pbrun_flags": [
            "--residency",
            "stage",
            "--timeout-s",
            str(LEG3_TIMEOUT_S),
            "--wait-s",
            str(LEG3_WAIT_S),
            "--deterministic",
            "--retry-safe",
            "--max-attempts",
            "1",
        ],
    }


def write_chunk_files(run_dir: str) -> list[str]:
    """Write the 3 deterministic chunk files into ``run_dir``.

    Returns the absolute paths in chunk order. Deterministic: rewriting
    yields byte-identical files.
    """
    paths = []
    for index, name in enumerate(LEG3_CHUNK_NAMES):
        path = os.path.join(run_dir, name)
        with open(path, "wb") as handle:
            for piece in deterministic_stream(
                _seed(index), LEG3_CHUNK_SIZES[index]
            ):
                handle.write(piece)
        paths.append(path)
    return paths


def manifest_for_run(chunk_paths: list[str], mount_prefix: str) -> dict:
    """Render the submittable v2 data-manifest for this run's chunk paths.

    Mechanical substitution only: entry order, offsets (0), sizes, digests,
    and the read plan come from ``build()``; only the absolute ``path``
    values and ``mount_prefix`` vary per run. Refuses paths outside the
    prefix or a chunk list that does not match the spec.
    """
    spec = build()
    if len(chunk_paths) != len(LEG3_CHUNK_NAMES):
        raise ValueError(
            f"leg3: need {len(LEG3_CHUNK_NAMES)} chunk paths, "
            f"got {len(chunk_paths)}"
        )
    if not mount_prefix.startswith("/") or mount_prefix == "/":
        raise ValueError("leg3: mount_prefix must be a non-root absolute path")
    prefix = posixpath.normpath(mount_prefix)
    entries = []
    for path, chunk in zip(chunk_paths, spec["chunks"]):
        if not posixpath.isabs(path) or posixpath.normpath(path) != path:
            raise ValueError(f"leg3: chunk path must be normalized absolute: {path!r}")
        if not (path == prefix or path.startswith(prefix + "/")):
            raise ValueError(f"leg3: chunk path outside mount prefix: {path!r}")
        entries.append(
            {
                "path": path,
                "offset": 0,
                "bytes": chunk["size"],
                "sha256": chunk["sha256"],
            }
        )
    total = sum(LEG3_CHUNK_SIZES)
    return {
        "schema": LEG3_MANIFEST_SCHEMA,
        "produced_by": {"tool": "pbcanary", "leg": 3},
        "annotations": {"producer": "pbcanary", "leg": 3},
        "mount_prefix": prefix,
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": total,
        "read_plan": spec["manifest"]["read_plan"],
    }


def _validated_entries(manifest: object) -> list[dict]:
    """The manifest's declared ranges, checked before any reader binds them.

    These fields feed the residency-map key and the pin's expected digest,
    so a malformed one refuses here rather than steering a lookup.
    """
    if not isinstance(manifest, dict):
        raise ValueError("manifest is not an object")
    if manifest.get("schema") != LEG3_MANIFEST_SCHEMA:
        raise ValueError("manifest schema is not the leg-3 v2 schema")
    entries = manifest.get("entries")
    if not isinstance(entries, list) or len(entries) != len(LEG3_CHUNK_NAMES):
        raise ValueError(
            f"manifest has {len(entries) if isinstance(entries, list) else 'no'} "
            f"entries, leg 3 needs {len(LEG3_CHUNK_NAMES)}")
    checked: list[dict] = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("manifest entry is not an object")
        path = entry.get("path")
        offset = entry.get("offset", 0)
        size = entry.get("bytes")
        digest = entry.get("sha256")
        if (not isinstance(path, str) or not path.startswith("/")
                or posixpath.normpath(path) != path or "\x00" in path):
            raise ValueError(
                f"manifest entry path must be normalized absolute: {path!r}")
        if isinstance(offset, bool) or type(offset) is not int or offset < 0:
            raise ValueError(f"manifest entry offset must be non-negative: {offset!r}")
        if isinstance(size, bool) or type(size) is not int or size <= 0:
            raise ValueError(f"manifest entry bytes must be positive: {size!r}")
        if (not isinstance(digest, str) or len(digest) != 64
                or any(character not in _HEX for character in digest)):
            raise ValueError(f"manifest entry sha256 must be 64 lowercase hex: {digest!r}")
        checked.append({"path": path, "offset": offset, "bytes": size,
                        "sha256": digest})
    return checked


def _progress_channel() -> str:
    """The launcher's canonical helper path, or a refusal.

    Leg 3 declares progress phases, so an admitted run always carries the
    launcher's helper plus its token.  Missing any of the three is a canary
    refusal, never a quiet legacy mode: qualification requires accepted
    cumulative progress, and there is no second record writer here.
    """
    present = {
        name: os.environ.get(name) or ""
        for name in (_ACTION_ENV_PROGRESS_PATH, _ACTION_ENV_PROGRESS_TOKEN,
                     _ACTION_ENV_PROGRESS_HELPER)}
    missing = [name for name, value in present.items() if not value]
    if missing:
        raise _StagedReadRefusal(
            "leg3 staged-read refusal: no progress channel (missing "
            + ", ".join(sorted(missing)) + ")")
    return present[_ACTION_ENV_PROGRESS_HELPER]


def _pb_commit(units: int, phase: str, helper: str) -> bool:
    """Report one durably read chunk through the launcher's canonical helper.

    The helper is ``prismabuild.progress.commit`` (the launcher exports its
    own generation's file), which parses the declared phases, echoes the
    per-launch token, and refuses a phase the submission did not declare.
    Returns False when nothing was written; the caller must not qualify.
    """
    import runpy

    try:
        commit = runpy.run_path(helper)["commit"]
    except Exception:  # Unreadable helper: fail closed, never guess.
        return False
    try:
        return bool(commit(units, phase))
    except Exception:  # ValueError for an undeclared phase, OSError, ...
        return False


def _hash_fd(fd: int, combined) -> str:
    """SHA-256 one opened pinned range, updating ``combined`` in the same pass."""
    digest = hashlib.sha256()
    while True:
        piece = os.read(fd, 1 << 20)
        if not piece:
            break
        digest.update(piece)
        combined.update(piece)
    return digest.hexdigest()


def _load_reader_modules():
    """Import the strict reader from the admitted launch helper tree only.

    ``PRISMABUILD_READER_HELPER_ROOT`` is the immutable generation root the
    launcher's proxy stamped; consumers append ``/src`` themselves.  A
    missing or broken helper tree refuses -- it never falls back to an
    unrelated installed or checkout copy.
    """
    root = os.environ.get(_READER_HELPER_ROOT_ENV) or ""
    if (not root.startswith("/") or posixpath.normpath(root) != root
            or "\x00" in root):
        raise _StagedReadRefusal(
            "leg3 staged-read refusal: no admitted reader helper root")
    source = os.path.join(root, "src")
    if not os.path.isdir(source):
        raise _StagedReadRefusal(
            f"leg3 staged-read refusal: admitted helper tree has no src: {source}")
    if source not in sys.path:
        sys.path.insert(0, source)
    from prismabuild import pool as pool_mod
    from prismabuild import reader_lease, residency_map

    base = os.path.join(source, "prismabuild") + os.sep
    for module in (pool_mod, reader_lease, residency_map):
        loaded = os.path.abspath(getattr(module, "__file__", "") or "")
        if not loaded.startswith(base):
            raise _StagedReadRefusal(
                "leg3 staged-read refusal: reader modules did not load from "
                "the admitted helper tree")
    return pool_mod, reader_lease, residency_map


class _StagedWindow:
    """One chunk at a time: map-keyed cover lookup, pin, read, release."""

    def __init__(self, pool_mod, reader_lease, residency_map, ctx, root,
                 tier_id, epoch, manifest_sha256, entries, wait_s,
                 manifest_path, manifest_file_sha256) -> None:
        self.pool_mod = pool_mod
        self.reader_lease = reader_lease
        self.residency_map = residency_map
        self.ctx = ctx
        self.queue = pool_mod.PoolQueue(str(ctx["queue_root"]))
        self.root = root
        self.tier_id = tier_id
        self.epoch = epoch
        self.manifest_sha256 = manifest_sha256
        self.entries = entries
        self.wait_s = wait_s
        self.manifest_path = manifest_path
        self.manifest_file_sha256 = manifest_file_sha256
        self.consumer = str(ctx["action_key"])

    @classmethod
    def open(cls, entries: list[dict], *, manifest_path: str,
             manifest_file_sha256: str) -> "_StagedWindow":
        pool_mod, reader_lease, residency_map = _load_reader_modules()
        context = reader_lease.injected_context(env=os.environ)
        if not isinstance(context, dict) or not context.get("ok"):
            refusal = (context.get("refusal")
                       if isinstance(context, dict) else "unreadable")
            raise _StagedReadRefusal(
                f"leg3 staged-read refusal: reader context refused: {refusal}")
        ctx = context["ctx"]
        map_path = str(ctx.get("map_path") or "")
        try:
            mapping = residency_map.read_map(map_path)
        except (OSError, ValueError) as exc:
            raise _StagedReadRefusal(
                f"leg3 staged-read refusal: staged map unavailable: {exc}") from exc
        tier_id = str(mapping.get("tier_id") or "")
        manifest_sha256 = str(mapping.get("manifest_sha256") or "")
        if not tier_id or not manifest_sha256:
            raise _StagedReadRefusal(
                "leg3 staged-read refusal: staged map names no tier or manifest")
        # The epoch belongs to the tier actually selected.  This leg is
        # stage-explicit (``--residency stage``): an SSD stage fragment
        # carries no epoch, and a RAM overlay header must never lend its
        # epoch to an SSD pin.
        if tier_id.startswith("ram:"):
            epoch = str(mapping.get("ram_epoch") or "")
            if not epoch:
                raise _StagedReadRefusal(
                    "leg3 staged-read refusal: ram tier names no epoch")
        else:
            epoch = ""
        return cls(pool_mod, reader_lease, residency_map, ctx,
                   os.path.dirname(map_path), tier_id, epoch,
                   manifest_sha256, entries, _stage_wait_s(),
                   manifest_path, manifest_file_sha256)

    def write_checkpoint(self, index: int, payload: dict) -> str:
        """Persist one verified chunk's result before its progress unit.

        The manifest's own directory is this action's namespace, so the
        checkpoint rides the canary's retention and never lands in the live
        queue.  The repo's existing canonical atomic writer (fsync plus
        rename) publishes it; a checkpoint that cannot be made durable
        refuses the qualification instead of reporting progress without it.
        """
        directory = os.path.join(os.path.dirname(self.manifest_path),
                                 "checkpoints")
        path = os.path.join(directory, f"chunk-{index}.json")
        try:
            self.pool_mod._write_json_atomic(Path(path), payload)
        except OSError as exc:
            raise _StagedReadRefusal(
                f"leg3 staged-read refusal: checkpoint not durable: {exc}") from exc
        return path

    def _prepare(self, key: str, entry: dict) -> dict:
        covers = None
        try:
            covers = self.reader_lease.covers_for_keys(
                self.root, self.consumer, [key], tier_id=self.tier_id,
                manifest_sha256=self.manifest_sha256, epoch=self.epoch)
        except Exception as exc:  # Unreadable records are terminal, not a wait.
            return {"ok": False, "refusal": f"unreadable: {exc}"}
        if not isinstance(covers, dict) or not covers.get("ok"):
            refusal = (str(covers.get("refusal"))
                       if isinstance(covers, dict) and covers.get("refusal")
                       else "unreadable")
            return {"ok": False, "refusal": refusal}
        expected = {key: {"bytes": entry["bytes"], "sha256": entry["sha256"]}}
        acquired = self.reader_lease.acquire_for(
            self.ctx, tier_id=self.tier_id, epoch=self.epoch,
            covers=covers["covers"], expected=expected,
            span={"start_bytes": 0, "end_bytes": entry["bytes"]},
            acquire_token=f"pbcanary-leg3:{self.ctx['nonce']}:{key}",
            residency_root=self.root)
        if not isinstance(acquired, dict) or not acquired.get("ok"):
            refusal = (str(acquired.get("refusal"))
                       if isinstance(acquired, dict) and acquired.get("refusal")
                       else "unreadable")
            return {"ok": False, "refusal": refusal}
        return {"ok": True, "acquired": acquired}

    def read_chunk(self, index: int, combined) -> tuple[str, str, dict]:
        """Serve chunk ``index`` under its pin; combined digest same pass."""
        entry = self.entries[index]
        key = self.residency_map.residency_map_key(entry["path"], entry["offset"])
        deadline = time.monotonic() + self.wait_s
        while True:
            prepared = self._prepare(key, entry)
            if prepared.get("ok"):
                acquired = prepared["acquired"]
                break
            refusal = str(prepared.get("refusal") or "unreadable")
            if (refusal not in LEG3_RETRYABLE_REFUSALS
                    or time.monotonic() >= deadline):
                raise _StagedReadRefusal(
                    "leg3 staged-read refusal: no staged coverage for "
                    f"{entry['path']!r} at offset {entry['offset']}: {refusal}")
            time.sleep(LEG3_WAIT_POLL_S)
        pin_id = str(acquired["pin_id"])
        ref_id = str(acquired["ref_id"])
        try:
            fd, serving = self.reader_lease.open_pinned(
                self.queue, acquired["pin"], ref_id, key,
                residency_root=self.root)
            try:
                digest = _hash_fd(fd, combined)
            finally:
                os.close(fd)
        except BaseException:
            self.reader_lease.release(
                self.queue, pin_id, ref_id,
                consumer_action_key=self.consumer, residency_root=self.root)
            raise
        if not self.reader_lease.release(
                self.queue, pin_id, ref_id,
                consumer_action_key=self.consumer, residency_root=self.root):
            raise _StagedReadRefusal(
                "leg3 staged-read refusal: reader lease release refused")
        stage_path = ""
        for pin_entry in acquired["pin"]["entries"]:
            if pin_entry.get("key") == key:
                stage_path = str(pin_entry.get("stage_path") or "")
                break
        if not stage_path:
            raise _StagedReadRefusal(
                "leg3 staged-read refusal: pin names no staged path for its entry")
        return digest, stage_path, dict(serving)


def _stage_wait_s() -> float:
    raw = os.environ.get(LEG3_STAGE_WAIT_ENV)
    if not raw:
        return LEG3_CHUNK_WAIT_S
    try:
        value = float(raw)
    except ValueError as exc:
        raise _StagedReadRefusal(
            f"leg3 staged-read refusal: malformed {LEG3_STAGE_WAIT_ENV}: {raw!r}") from exc
    if (not math.isfinite(value) or value < 0
            or value > LEG3_PROGRESS_ALLOWANCE_S):
        raise _StagedReadRefusal(
            f"leg3 staged-read refusal: {LEG3_STAGE_WAIT_ENV} must be a finite "
            f"value in [0, {LEG3_PROGRESS_ALLOWANCE_S}], got {raw!r}")
    return value


def run_action() -> int:
    """Worker entrypoint: serve and verify every chunk through its lease pin.

    Reads ``PBCANARY_LEG3_MANIFEST``, resolves each range's staged copy
    through the residency map plus the reader-lease pin, hashes chunk and
    combined digest in one pass, commits one accepted progress unit per
    durably read chunk through the launcher's helper, prints the envelope,
    and exits 0 iff every range matches and every progress commit landed.
    Prints ``ok:false`` (exit 1) rather than dying silent so ``verify`` can
    name the refusing check.
    """
    manifest_path = os.environ.get(_ACTION_ENV_MANIFEST, "")
    envelope: dict = {"schema": LEG3_SCHEMA, "leg": 3, "chunks": [], "ok": False}
    try:
        with open(manifest_path, "rb") as handle:
            manifest_raw = handle.read()
        manifest = json.loads(manifest_raw)
        entries = _validated_entries(manifest)
        manifest_file_sha256 = hashlib.sha256(manifest_raw).hexdigest()
        helper = _progress_channel()
        window = _StagedWindow.open(
            entries, manifest_path=manifest_path,
            manifest_file_sha256=manifest_file_sha256)
        combined = hashlib.sha256()
        observed = []
        progress = []
        for index, entry in enumerate(entries):
            digest, stage_path, serving = window.read_chunk(index, combined)
            if digest != entry["sha256"]:
                # Corrupt staged bytes are never durable work: no checkpoint,
                # no progress unit, no continuation.
                raise _StagedReadRefusal(
                    "leg3 staged-read refusal: chunk "
                    f"{index} verified digest does not match the manifest")
            checkpoint = window.write_checkpoint(index, {
                "schema": LEG3_CHECKPOINT_SCHEMA,
                "action_key": str(window.ctx.get("action_key") or ""),
                "nonce": str(window.ctx.get("nonce") or ""),
                "scope_id": str(window.ctx.get("scope_id") or ""),
                "manifest_sha256": manifest_file_sha256,
                "staged_manifest_sha256": window.manifest_sha256,
                "chunk_index": index,
                "chunk_path": entry["path"],
                "chunk_offset": entry["offset"],
                "bytes": entry["bytes"],
                "sha256": digest,
                "serving": {
                    "tier_id": str(serving.get("tier_id") or ""),
                    "epoch": str(serving.get("epoch") or ""),
                    "pin_id": str(serving.get("pin_id") or ""),
                    "range_ref": str(serving.get("range_ref") or ""),
                    "path": stage_path,
                },
                "units_completed": index + 1,
                "written_unix": time.time(),
            })
            observed.append({
                "path": entry["path"],
                "offset": entry["offset"],
                "bytes": entry["bytes"],
                "sha256": digest,
                "checkpoint": checkpoint,
                "serving": {
                    "tier_id": str(serving.get("tier_id") or ""),
                    "epoch": str(serving.get("epoch") or ""),
                    "pin_id": str(serving.get("pin_id") or ""),
                    "range_ref": str(serving.get("range_ref") or ""),
                    "path": stage_path,
                },
            })
            committed = _pb_commit(index + 1, LEG3_PHASES[index], helper)
            progress.append({"phase": LEG3_PHASES[index],
                             "units_completed": index + 1,
                             "checkpoint": checkpoint,
                             "committed": bool(committed)})
            if not committed:
                raise _StagedReadRefusal(
                    "leg3 staged-read refusal: progress commit refused for "
                    f"phase {LEG3_PHASES[index]}")
        envelope["chunks"] = observed
        envelope["combined"] = combined.hexdigest()
        envelope["progress"] = progress
        envelope["checkpoints"] = [chunk["checkpoint"] for chunk in observed]
        envelope["strict"] = {
            "reader": "reader-lease-v1",
            "tier_id": window.tier_id,
            "epoch": window.epoch,
            "pin_ids": [chunk["serving"]["pin_id"] for chunk in observed],
        }
        envelope["ok"] = True
    except Exception as exc:  # Fail-closed: report, never traceback-only.
        envelope["error"] = f"{type(exc).__name__}: {exc}"
    sys.stdout.write(canonical_json(envelope) + "\n")
    sys.stdout.flush()
    return 0 if envelope.get("ok") else 1


def _extract_envelope(receipt: object) -> tuple[str | None, dict | None, str]:
    """Find the leg-3 envelope in ``receipt``.

    Returns ``(raw, parsed, where)``; ``raw`` is None when the receipt
    carries only a parsed envelope. Accepted locations, in order:
    ``receipt["envelope"]``, ``receipt["stdout"]``,
    ``receipt["detail"]["stdout"]``. Stdout is scanned for the last line
    that parses as a JSON object with this leg's schema marker.
    """
    if not isinstance(receipt, dict):
        return None, None, ""
    candidate = receipt.get("envelope")
    if isinstance(candidate, str):
        try:
            parsed = json.loads(candidate)
        except ValueError:
            return None, None, "receipt[envelope]"
        if isinstance(parsed, dict) and parsed.get("schema") == LEG3_SCHEMA:
            return candidate.strip(), parsed, "receipt[envelope]"
        return None, None, "receipt[envelope]"
    if isinstance(candidate, dict) and candidate.get("schema") == LEG3_SCHEMA:
        return None, candidate, "receipt[envelope]"
    for where in ("stdout", "detail.stdout"):
        node: object = receipt
        for key in where.split("."):
            node = node.get(key) if isinstance(node, dict) else None
        if not isinstance(node, str):
            continue
        found = None
        for line in node.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                parsed = json.loads(line)
            except ValueError:
                continue
            if isinstance(parsed, dict) and parsed.get("schema") == LEG3_SCHEMA:
                found = (line, parsed)
        if found is not None:
            return found[0], found[1], f"receipt[{where}]"
    return None, None, ""


def _returncode(receipt: dict) -> int | None:
    for where in ("returncode", "detail.returncode", "action_returncode"):
        node: object = receipt
        for key in where.split("."):
            node = node.get(key) if isinstance(node, dict) else None
        if isinstance(node, bool):
            continue
        if isinstance(node, int):
            return node
    return None


def _serving_refusal(chunk: dict) -> str | None:
    """Why this chunk's serving evidence does not prove a staged read."""
    offset = chunk.get("offset")
    if isinstance(offset, bool) or type(offset) is not int or offset < 0:
        return "lacks its manifest offset"
    path = chunk.get("path")
    if not isinstance(path, str) or not path:
        return "lacks its manifest path"
    serving = chunk.get("serving")
    if not isinstance(serving, dict):
        return "lacks reader-lease serving evidence"
    tier = serving.get("tier_id")
    if (not isinstance(tier, str)
            or not any(tier.startswith(prefix)
                       for prefix in LEG3_STAGED_TIER_PREFIXES)):
        return "names no staged serving tier"
    opened = serving.get("path")
    if (not isinstance(opened, str) or not opened.startswith("/")
            or posixpath.normpath(opened) != opened or "\x00" in opened):
        return "opened path is not normalized absolute"
    if opened == path:
        return "opened path is the origin path"
    if serving.get("range_ref") != f"{offset}:{path}":
        return "serving range_ref does not bind the manifest entry"
    pin = serving.get("pin_id")
    if not isinstance(pin, str) or not pin:
        return "lacks a reader-lease pin id"
    return None


def _progress_refusal(receipt: dict, units: int) -> str | None:
    """Why the launcher's accepted-progress observation does not qualify.

    Only the worker's authenticated ``ProgressWatch`` observation counts:
    the action's own self-report (``envelope["progress"]``) is audit text,
    never proof.  The observation must carry a real cumulative count in the
    final declared phase; a boolean masquerading as a counter or a malformed
    rejection counter is refused as malformed evidence.
    """
    observation = receipt.get("progress_observation")
    if not isinstance(observation, dict):
        return "no accepted-progress observation"
    if observation.get("source") != "action-progress":
        return "accepted-progress observation is not the action-progress source"
    accepted = observation.get("accepted_count")
    if type(accepted) is not int or accepted < 1:
        return "no accepted progress report"
    rejected = observation.get("rejected_count")
    if type(rejected) is not int or rejected < 0:
        return "progress rejection counter is malformed"
    entered = observation.get("phases_entered")
    if type(entered) is not int or entered != len(LEG3_PHASES):
        return (f"accepted phases entered {entered!r} != {len(LEG3_PHASES)}")
    last = observation.get("last_accepted")
    if not isinstance(last, dict):
        return "no accepted progress record"
    completed = last.get("units_completed")
    if type(completed) is not int or completed != units:
        return f"accepted cumulative units {completed!r} != {units}"
    if str(last.get("phase") or "") != LEG3_PHASES[-1]:
        return (f"last accepted phase {last.get('phase')!r} != "
                f"{LEG3_PHASES[-1]!r}")
    return None


def verify(receipt: object, expected: object) -> tuple[bool, str]:
    """Verify the leg-3 action receipt against ``build()["expected"]``.

    Fail-closed, in fixed order: receipt shape, returncode, envelope
    presence, per-chunk digest, byte count, and staged-serving evidence (in
    order), combined digest, ``ok`` flag, accepted cumulative progress from
    the launcher's observation.  The first refusal names the leg and the
    check.
    """
    if not isinstance(receipt, dict):
        return False, "leg3: receipt is not an object"
    if not isinstance(expected, dict):
        return False, "leg3: expected spec is not an object"
    rc = _returncode(receipt)
    if rc is not None and rc != 0:
        return False, f"leg3: action returncode {rc} != 0"
    _, parsed, where = _extract_envelope(receipt)
    if parsed is None:
        return False, "leg3: no leg3 envelope in receipt"
    want_chunks = expected.get("chunks")
    want_combined = expected.get("combined")
    if not isinstance(want_chunks, list) or not isinstance(want_combined, str):
        return False, "leg3: expected spec lacks chunks/combined"
    got_chunks = parsed.get("chunks")
    if not isinstance(got_chunks, list) or len(got_chunks) != len(want_chunks):
        return False, (
            f"leg3: envelope carries {len(got_chunks) if isinstance(got_chunks, list) else 'no'} "
            f"chunks, expected {len(want_chunks)} ({where})"
        )
    for index, (got, want) in enumerate(zip(got_chunks, want_chunks)):
        if not isinstance(got, dict) or got.get("sha256") != want.get("sha256"):
            return False, (
                f"leg3: chunk {index} ({want.get('name')}) digest mismatch "
                f"({where}): staged bytes differ from manifest"
            )
        if got.get("bytes") != want.get("size"):
            return False, (
                f"leg3: chunk {index} ({want.get('name')}) byte-count mismatch "
                f"({where})"
            )
        serving_refusal = _serving_refusal(got)
        if serving_refusal is not None:
            return False, (
                f"leg3: chunk {index} ({want.get('name')}) "
                f"{serving_refusal} ({where})"
            )
    if parsed.get("combined") != want_combined:
        return False, f"leg3: combined digest mismatch ({where})"
    if parsed.get("ok") is not True:
        return False, f"leg3: envelope ok != true ({where})"
    progress_refusal = _progress_refusal(receipt, len(want_chunks))
    if progress_refusal is not None:
        return False, f"leg3: {progress_refusal} ({where})"
    total = sum(c["size"] for c in want_chunks)
    return True, f"leg3: {len(want_chunks)} chunks, {total} bytes verified ({where})"


def main(argv: list[str]) -> int:
    if argv == ["--run-action"]:
        return run_action()
    sys.stderr.write("usage: leg3.py --run-action\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
