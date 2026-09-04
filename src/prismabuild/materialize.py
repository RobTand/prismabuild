"""Materialize a sealed checkout snapshot, whichever transport delivered it.

Split out of ``pool.py`` unchanged.  The pull queue was the first transport to
need this, so it was written where it was first used; it is not *about* the
queue.  A SLURM batch job has to perform exactly the same materialization --
initialize a repository under a box-local root, fetch the sealed commit out of
the CAS-carried bundle, check it out detached, hand the subdirectory to the
worker, and remove the tree afterwards without ever turning completed work into
a retry -- and a second copy of that sequence is how two transports end up
executing two different trees for one action key.

So the code lives here and both transports import it.  The functions keep the
names and bodies they had in ``pool.py``; only the module they live in changed.
The one addition is ``local_checkout_root``: the root is a deployment fact, and
each transport keeps its own spelling of it (``pool.LOCAL_CHECKOUT_ROOT`` stays
the pull queue's), so the caller passes the root it means rather than this
module deciding for it.

Stdlib only, like everything a worker node has to import.
"""
from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid

from . import core as pb

# The snapshot bundle travels through the shared CAS, but execution trees stay
# on each worker's local disk. Same spelling on every box, different storage.
LOCAL_CHECKOUT_ROOT = Path(
    os.environ.get(
        "PRISMABUILD_LOCAL_CHECKOUT_ROOT",
        "/home/rob/tmp/prismabuild-checkouts",
    )
)


class MaterializationError(pb.PrismaBuildError):
    """A checkout could not be materialized from its sealed snapshot."""


class MaterializationContractError(MaterializationError, ValueError):
    """A record handed to the materializer does not satisfy its schema."""


def _now() -> float:
    return time.time()


def _write_json_atomic(path: Path, payload: Mapping[str, object]) -> None:
    """Publish a record by rename, so no reader ever sees a partial file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    data = pb._canonical_bytes(dict(payload))
    descriptor = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    os.replace(tmp, path)

def _run_materializer_git(
    argv: Sequence[str],
    *,
    where: str,
    environment: Mapping[str, str] | None = None,
) -> str:
    try:
        completed = subprocess.run(
            list(argv),
            capture_output=True,
            text=True,
            timeout=120,
            env=None if environment is None else dict(environment),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise MaterializationError(f"{where} failed: {exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise MaterializationError(f"{where} failed: {detail or completed.returncode}")
    return completed.stdout

def _cleanup_execution_checkout(
    base: Path, temporary: Path, item: Mapping[str, object]
) -> None:
    """Remove one private tree, recording a leak without changing task status."""

    error = ""
    try:
        shutil.rmtree(temporary)
    except Exception as exc:  # cleanup must not turn completed work into retry
        error = str(exc)
    try:
        remains = temporary.exists() or temporary.is_symlink()
    except OSError as exc:
        remains = True
        error = error or f"cannot verify removal: {exc}"
    if remains and not error:
        error = "materialized checkout still exists after recursive cleanup"
    if not error:
        return

    action_key = str(item.get("action_key") or "unknown-action")
    record = {
        "schema": "prismaquant.prismabuild.checkout_cleanup_failure.v1",
        "action_key": action_key,
        "path": str(temporary),
        "error": error,
        "recorded_unix": _now(),
        "host": socket.gethostname(),
    }
    record_path = (
        base / "cleanup-failures" /
        f"{action_key}.{temporary.name}.json"
    )
    record_error = ""
    try:
        _write_json_atomic(record_path, record)
    except Exception as exc:  # stderr remains the observable fallback
        record_error = f"; could not publish {record_path}: {exc}"
    print(
        "prismabuild: checkout cleanup failed after action "
        f"{action_key}: {temporary}: {error}{record_error}",
        file=sys.stderr,
        flush=True,
    )


@contextmanager
def _execution_checkout(
    item: Mapping[str, object],
    *,
    local_checkout_root: str | Path | None = None,
) -> Iterator[Path]:
    """Yield the live path or a private checkout of the sealed snapshot."""

    raw_snapshot = item.get("checkout_snapshot")
    if raw_snapshot is None:
        raw_root = item.get("checkout_root")
        if not raw_root:
            raise MaterializationContractError(
                "pool item has neither checkout_root nor checkout_snapshot"
            )
        yield Path(str(raw_root))
        return

    snapshot = pb.validate_pbrun_checkout_snapshot(raw_snapshot)
    cas = pb.PrismaBuildCAS(str(item["cas_root"]))
    bundle = cas.input_path(snapshot["input"])
    base = Path(
        LOCAL_CHECKOUT_ROOT if local_checkout_root is None
        else local_checkout_root
    )
    if not base.is_absolute() or base == Path("/") or ".." in base.parts:
        raise MaterializationContractError(
            "PRISMABUILD_LOCAL_CHECKOUT_ROOT must be an absolute non-root path"
        )
    base.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(
            prefix=f"{str(item['action_key'])[:12]}.", dir=str(base)
        )
    )
    repository = temporary / "checkout"
    try:
        _run_materializer_git(
            ["git", "init", "-q", str(repository)],
            where="initialize materialized checkout",
        )
        heads = _run_materializer_git(
            ["git", "bundle", "list-heads", str(bundle)],
            where="read checkout snapshot bundle",
        )
        commit = str(snapshot["commit"])
        refs = [
            fields[1]
            for line in heads.splitlines()
            if len(fields := line.split(maxsplit=1)) == 2 and fields[0] == commit
        ]
        if not refs:
            raise MaterializationContractError(
                "checkout snapshot bundle does not advertise its sealed commit"
            )
        _run_materializer_git(
            [
                "git", "-C", str(repository), "fetch", "-q", "--no-tags",
                str(bundle), refs[0],
            ],
            where="fetch checkout snapshot bundle",
        )
        _run_materializer_git(
            [
                "git",
                "-c", "core.autocrlf=false",
                "-c", "core.attributesFile=/dev/null",
                "-C", str(repository), "checkout", "-q", "--detach", commit,
            ],
            where="check out sealed commit",
            environment={**os.environ, "GIT_ATTR_NOSYSTEM": "1"},
        )
        subdirectory = repository / str(snapshot["subdirectory"])
        if not subdirectory.is_dir():
            raise MaterializationContractError(
                "checkout snapshot subdirectory is absent after materialization"
            )
        yield subdirectory
    finally:
        # The path was created by mkdtemp below a validated local-only root;
        # never widen this cleanup to the root itself. A root-owned container
        # dropping can leave residue inside this bounded root, but cleanup
        # failure must not change an already-published action into a failure.
        _cleanup_execution_checkout(base, temporary, item)


