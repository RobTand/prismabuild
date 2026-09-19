"""pbcanary leg 3 — chunked residency (PB #688).

24 MiB of deterministic bytes as a 3-chunk v2 data-manifest, submitted with
``--residency stage``; the worker action reads the staged bytes back and
SHA-256-verifies each range against the manifest. Exercises movers, tier
staging, chunk progress phases, and egress.

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
import os
import posixpath
import sys
import time

sys.path.insert(0, str(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")))
try:
    from pbcanary_legs.common import (
        canonical_json,
        deterministic_bytes,
        deterministic_stream,
        sha256_file_hex,
        sha256_hex,
        sha256_stream_hex,
    )
except ImportError:  # Worker runs the file directly: sibling import.
    from common import (  # type: ignore[no-redef]
        canonical_json,
        deterministic_bytes,
        deterministic_stream,
        sha256_file_hex,
        sha256_hex,
        sha256_stream_hex,
    )

LEG3_NAME = "leg-3"
LEG3_SCHEMA = "prismabuild.pbcanary.leg3.v1"

#: v2 data-manifest schema id. Literal (no prismabuild import; see module
#: docstring): must equal ``core.DATA_MANIFEST_SCHEMA_V2``.
LEG3_MANIFEST_SCHEMA = "prismaquant.prismabuild.data_manifest.v2"

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


def _pb_commit(units: int, phase: str) -> bool:
    """Report durable progress; unconditional no-op when not admitted."""
    path = os.environ.get("PRISMABUILD_ACTION_PROGRESS_PATH")
    token = os.environ.get("PRISMABUILD_ACTION_PROGRESS_TOKEN")
    allowed = (os.environ.get("PRISMABUILD_ACTION_PROGRESS_PHASES") or "").split(",")
    if not path or not token or phase not in allowed:
        # Helper-file fallback mirrors the documented snippet contract.
        helper = os.environ.get("PRISMABUILD_ACTION_PROGRESS_HELPER")
        if not helper:
            return False
        import runpy

        try:
            commit = runpy.run_path(helper)["commit"]
        except Exception:
            return False
        try:
            commit(units, phase)
        except Exception:
            return False
        return True
    record = canonical_json(
        {
            "phase": phase,
            "reported_unix": time.time(),
            "schema": "prismabuild.action_progress.v1",
            "token": token,
            "unit": None,
            "units_completed": units,
        }
    )
    temporary = f"{path}.{os.getpid()}.tmp"
    try:
        with open(temporary, "w") as handle:
            handle.write(record + "\n")
        os.replace(temporary, path)
    except OSError:
        return False
    return True


def run_action() -> int:
    """Worker entrypoint: verify staged bytes against the manifest.

    Reads ``PBCANARY_LEG3_MANIFEST``, hashes each declared range,
    commits one progress unit per phase, prints the envelope line, and
    exits 0 iff every range matches. Prints ``ok:false`` (exit 1) rather
    than dying silent so ``verify`` can name the refusing check.
    """
    manifest_path = os.environ.get(_ACTION_ENV_MANIFEST, "")
    envelope: dict = {"schema": LEG3_SCHEMA, "leg": 3, "chunks": [], "ok": False}
    try:
        with open(manifest_path, encoding="utf-8") as handle:
            manifest = json.load(handle)
        entries = manifest["entries"]
        if manifest.get("schema") != LEG3_MANIFEST_SCHEMA:
            raise ValueError("manifest schema is not the leg-3 v2 schema")
        if len(entries) != len(LEG3_CHUNK_NAMES):
            raise ValueError(f"manifest has {len(entries)} entries, leg 3 needs 3")
        observed = []
        ok = True
        for index, entry in enumerate(entries):
            digest = sha256_file_hex(
                entry["path"], offset=entry["offset"], size=entry["bytes"]
            )
            observed.append(
                {"path": entry["path"], "bytes": entry["bytes"], "sha256": digest}
            )
            _pb_commit(index + 1, LEG3_PHASES[index])
            if digest != entry["sha256"]:
                ok = False
        envelope["chunks"] = observed
        envelope["combined"] = _combined_of_paths([c["path"] for c in observed])
        envelope["ok"] = bool(ok)
    except Exception as exc:  # Fail-closed: report, never traceback-only.
        envelope["error"] = f"{type(exc).__name__}: {exc}"
    sys.stdout.write(canonical_json(envelope) + "\n")
    sys.stdout.flush()
    return 0 if envelope.get("ok") else 1


def _combined_of_paths(paths: list[str]) -> str:
    """SHA-256 over the concatenation of the files' bytes, in order."""
    digest = hashlib.sha256()
    for path in paths:
        with open(path, "rb") as handle:
            while True:
                piece = handle.read(1 << 20)
                if not piece:
                    break
                digest.update(piece)
    return digest.hexdigest()


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


def verify(receipt: object, expected: object) -> tuple[bool, str]:
    """Verify the leg-3 action receipt against ``build()["expected"]``.

    Fail-closed, in fixed order: receipt shape, returncode, envelope
    presence, per-chunk digests (in order), combined digest, ``ok`` flag.
    The first refusal names the leg and the check.
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
    if parsed.get("combined") != want_combined:
        return False, f"leg3: combined digest mismatch ({where})"
    if parsed.get("ok") is not True:
        return False, f"leg3: envelope ok != true ({where})"
    total = sum(c["size"] for c in want_chunks)
    return True, f"leg3: {len(want_chunks)} chunks, {total} bytes verified ({where})"


def main(argv: list[str]) -> int:
    if argv == ["--run-action"]:
        return run_action()
    sys.stderr.write("usage: leg3.py --run-action\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
