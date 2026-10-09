"""Root-controlled runtime copies: the authority behind PrismaBuild's movement roles.

A queue row can name one of PrismaBuild's own movement tools (a stage mover, an
egress, an export).  Whether those bytes are PrismaBuild's is a fact only a
root-controlled copy can state: an ordinary runtime store belongs to the user
that submits work, so a receipt written there proves integrity, not authority.
This module holds both halves of that fact and nothing else.

* The root half copies a published generation, byte for byte, into
  :data:`PROTECTED_GENERATION_STORE` (:func:`publish_generation`).  A root
  timer, enrolled once per host, keeps the host converged on the live
  generation without a person (:func:`converge`, :func:`main`).
* The fleet half asks whether a path, or the generation this process runs, has
  such a copy (:func:`published_member`, :func:`live_authority`) and names the
  copy a box can announce to the submitters that seal its movers
  (:func:`protected_tools_dir`).

Unknown authority grants no role and refuses no launch.  The caller falls back
to the behaviour it had before roles existed.

Standard library only: the installed copy runs standalone, as
``python3 -I runtime_publication.py``, from a root-owned directory.
"""
from __future__ import annotations

import argparse
import fcntl
import functools
import hashlib
import json
import os
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import sys
import tempfile
import time

PROTECTED_GENERATION_STORE = Path("/opt/prismabuild/movement-generations")
PUBLICATION_RECORD = "MOVEMENT_PUBLICATION.json"
PUBLICATION_SCHEMA = "prismabuild.movement-publication.v1"
RUNTIME_SCHEMA = "prismaquant.prismabuild.runtime_version.v1"
STATUS_SCHEMA = "prismabuild.movement-publication-status.v1"
#: The enrolled host's root-owned settings (written once by the installer).
DEFAULT_CONFIG = Path("/etc/prismabuild/movement-publish.json")
#: The movement tool every tool root carries; its copy stands for the root.
PROBE_TOOL = "stage_release.py"
_DIGEST = re.compile(r"[0-9a-f]{64}")
_GENERATION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
#: Verified generation copies and members, keyed by what their files look like
#: now (device, inode, ctime, size), so an administrator's change is seen.
_ROOTS: dict[tuple, tuple[dict, str]] = {}
_MEMBERS: set[tuple] = set()
_RECEIPT_SHA: dict[tuple, str] = {}


def _protected_path(path: Path) -> bool:
    """Require root custody from the filesystem root through this path."""
    for entry in (path, *path.parents):
        info = entry.lstat()
        if (info.st_uid != 0 or info.st_mode & 0o022
                or not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode))):
            return False
    return True


def _stamp(path: Path) -> tuple:
    info = path.stat()
    return (info.st_dev, info.st_ino, info.st_ctime_ns, info.st_size)


def _create_directories(path: Path) -> None:
    """Create each missing component without group or other write access."""
    missing = []
    while not path.exists():
        missing.append(path)
        path = path.parent
    for directory in reversed(missing):
        directory.mkdir(mode=0o755)
        directory.chmod(0o755)


def _regular_bytes(path: Path) -> bytes:
    """Read a regular file without following a final symlink."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError(f"not a regular publication file: {path}")
        return stream.read()


def _receipt(raw: bytes, generation: str) -> dict:
    value = json.loads(raw)
    if (not isinstance(value, dict) or value.get("schema") != RUNTIME_SCHEMA
            or value.get("generation") != generation
            or re.fullmatch(r"[0-9a-f]{40}", str(value.get("commit", ""))) is None
            or not isinstance(value.get("files"), dict) or not value["files"]):
        raise ValueError("invalid runtime publication receipt")
    for name, digest in value["files"].items():
        parts = PurePosixPath(name).parts
        if (not parts or name != "/".join(parts) or name.startswith("/")
                or any(part in (".", "..") for part in parts)
                or name in ("RUNTIME_VERSION.json", PUBLICATION_RECORD)
                or not isinstance(digest, str) or _DIGEST.fullmatch(digest) is None):
            raise ValueError("invalid runtime publication member")
    return value


def _receipt_sha256(path: Path) -> str:
    """Digest of one receipt file, read again only when the file changed."""
    key = (str(path), _stamp(path))
    if key not in _RECEIPT_SHA:
        _RECEIPT_SHA[key] = hashlib.sha256(_regular_bytes(path)).hexdigest()
    return _RECEIPT_SHA[key]


# --- the root half --------------------------------------------------------------

class _StoreLock:
    """One publisher at a time in the protected store (root only).

    The lock file lives in the store, so it needs the store's root custody: an
    unprivileged user cannot hold it, and a rename of one publication cannot
    meet another publisher's.
    """

    def __init__(self, store: Path):
        self.path = store / ".lock"

    def __enter__(self):
        self.descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        fcntl.flock(self.descriptor, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        os.close(self.descriptor)


def _sweep_staging(store: Path) -> None:
    """Remove what a publisher that died left behind; the caller holds the lock."""
    for stale in store.glob(".publication-*"):
        for entry in (stale, *stale.rglob("*")):
            if entry.is_dir() and not entry.is_symlink():
                entry.chmod(0o755)
        shutil.rmtree(stale)


def publish_generation(source: Path, *, receipt_sha256: str) -> Path:
    """Copy one generation and its receipt, byte for byte, without executing it (root only).

    ``receipt_sha256`` is the digest of the receipt the caller selected.  The
    copy is refused when the receipt on disk differs, when any member differs
    from the digest the receipt names, or when the store has no root custody.
    A publication is never replaced.
    """
    if os.geteuid() != 0:
        raise PermissionError("movement publication requires root authority")
    if _DIGEST.fullmatch(receipt_sha256) is None:
        raise ValueError("movement publication requires a receipt sha256")
    source = Path(source).resolve(strict=True)
    if _GENERATION.fullmatch(source.name) is None:
        raise ValueError("invalid movement generation name")
    raw = _regular_bytes(source / "RUNTIME_VERSION.json")
    if hashlib.sha256(raw).hexdigest() != receipt_sha256:
        raise ValueError("runtime receipt differs from the selected digest")
    receipt = _receipt(raw, source.name)
    store = PROTECTED_GENERATION_STORE
    # Reject unsafe existing ancestors before creating anything under them.
    ancestor = store
    while not ancestor.exists():
        ancestor = ancestor.parent
    if not _protected_path(ancestor):
        raise PermissionError("movement publication store has no root custody")
    _create_directories(store)
    if not _protected_path(store):
        raise PermissionError("movement publication store has no root custody")
    with _StoreLock(store):
        target = store / source.name
        # Under the lock no other publisher can create the target, and nothing
        # else creates entries here, so this rename cannot replace a publication.
        if target.exists():
            raise FileExistsError("movement generations are append-only")
        _sweep_staging(store)
        stage = Path(tempfile.mkdtemp(prefix=".publication-", dir=store))
        try:
            for name, expected in receipt["files"].items():
                member = source / name
                # Every source component must be literal, not a symlink or alias.
                if member.resolve(strict=True) != member:
                    raise ValueError(f"runtime member is a symlink: {name}")
                data = _regular_bytes(member)
                if hashlib.sha256(data).hexdigest() != expected:
                    raise ValueError(f"runtime member digest differs: {name}")
                copied = stage / name
                _create_directories(copied.parent)
                copied.write_bytes(data)
                copied.chmod(0o555 if member.stat().st_mode & 0o111 else 0o444)
            (stage / "RUNTIME_VERSION.json").write_bytes(raw)
            (stage / PUBLICATION_RECORD).write_text(json.dumps({
                "schema": PUBLICATION_SCHEMA, "generation": source.name,
                "receipt_sha256": receipt_sha256,
            }, sort_keys=True) + "\n", encoding="utf-8")
            for entry in stage.rglob("*"):
                if entry.is_file():
                    with entry.open("rb") as stream:
                        os.fsync(stream.fileno())
                    if entry.name in ("RUNTIME_VERSION.json", PUBLICATION_RECORD):
                        entry.chmod(0o444)
            directories = [entry for entry in stage.rglob("*") if entry.is_dir()]
            for directory in (*reversed(directories), stage):
                directory.chmod(0o555)
                descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            os.rename(stage, target)
            descriptor = os.open(store, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            return target
        finally:
            if stage.exists():
                _sweep_staging(store)


def _write_status(path: Path, record: Mapping[str, object]) -> None:
    """Replace the status record atomically; a failure to write it changes nothing else."""
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_text(json.dumps(record, sort_keys=True) + "\n", encoding="utf-8")
        temporary.chmod(0o644)
        os.replace(temporary, path)
    except OSError:
        temporary.unlink(missing_ok=True)


def converge(config: Mapping[str, object], *, now: float | None = None) -> dict[str, object]:
    """Publish the enrolled store's live generation here, once (root only).

    ``config`` names ``runtime`` (the live pointer), ``generation_store`` (the
    store the pointer must lead into) and ``status`` (where the result is
    recorded).  The live generation is the only one published: a host that
    still runs an older one has that copy from when it was live.  The result
    is ``current`` (the copy exists and matches), ``published`` or ``error``;
    an error leaves the host without a copy, and a host without a copy keeps
    the behaviour it had before roles existed.
    """
    result: dict[str, object] = {
        "schema": STATUS_SCHEMA, "state": "error", "generation": None,
        "checked_unix": time.time() if now is None else now}
    try:
        store = Path(str(config["generation_store"])).resolve(strict=True)
        live = Path(str(config["runtime"])).resolve(strict=True)
        if live.parent != store or _GENERATION.fullmatch(live.name) is None:
            raise ValueError("the live runtime is not a generation of the enrolled store")
        result["generation"] = live.name
        digest = hashlib.sha256(_regular_bytes(live / "RUNTIME_VERSION.json")).hexdigest()
        try:
            publish_generation(live, receipt_sha256=digest)
            result["state"] = "published"
        except FileExistsError:
            published = _published_root(PROTECTED_GENERATION_STORE / live.name)
            if published is None or published[1] != digest:
                raise ValueError(
                    "a protected copy of this generation exists and differs; "
                    "protected copies are append-only") from None
            result["state"] = "current"
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    status = config.get("status")
    if isinstance(status, str) and status:
        _write_status(Path(status), result)
    return result


def _read_config(path: Path) -> dict:
    if not _protected_path(path):
        raise PermissionError(f"{path} has no root custody")
    value = json.loads(_regular_bytes(path))
    if (not isinstance(value, dict)
            or not all(isinstance(value.get(name), str)
                       for name in ("runtime", "generation_store", "status"))):
        raise ValueError(f"{path} names no runtime, generation_store and status")
    return value


def main(argv: list[str] | None = None) -> int:
    """The root unit's entry: converge this host once and print the result."""
    parser = argparse.ArgumentParser(
        description="Publish the live runtime generation's protected movement copy (root).")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    args = parser.parse_args(argv)
    if os.geteuid() != 0:
        parser.exit(2, "movement publication requires root authority\n")
    try:
        # Absolute, not resolved: a link in the path is a custody failure.
        result = converge(_read_config(Path(os.path.abspath(args.config))))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        result = {"schema": STATUS_SCHEMA, "state": "error",
                  "error": f"{type(exc).__name__}: {exc}"}
    print(json.dumps(result, sort_keys=True))
    return 0 if result["state"] in ("current", "published") else 1


# --- the fleet half -------------------------------------------------------------

def _tool_shape(relative: tuple[str, ...]) -> bool:
    """``<generation>/tools/<name>`` or ``<generation>/tools/fleet/<name>``, nothing deeper."""
    return (len(relative) in (3, 4) and relative[1] == "tools"
            and (len(relative) == 3 or relative[2] == "fleet")
            and _GENERATION.fullmatch(relative[0]) is not None)


def spelled_member(path: object) -> bool:
    """Whether ``path`` is spelled as a tool of a protected generation.

    Lexical only: it reads nothing, so it holds on every host.  It is the
    shape a publishing host can check; whether the tool exists and is
    authentic is :func:`published_member`'s answer, given where it runs.
    """
    if not isinstance(path, str) or not path.startswith("/") or os.path.normpath(path) != path:
        return False
    try:
        relative = PurePosixPath(path).relative_to(
            PurePosixPath(PROTECTED_GENERATION_STORE)).parts
    except ValueError:
        return False
    return _tool_shape(relative)


def _published_root(root: Path) -> tuple[dict, str] | None:
    """The receipt and its digest for a protected copy whose authority record holds."""
    record_path = root / PUBLICATION_RECORD
    receipt_path = root / "RUNTIME_VERSION.json"
    if (not _protected_path(record_path) or not _protected_path(receipt_path)
            or root.stat().st_mode & 0o222):
        return None
    # Root custody makes the entire copied namespace immutable to submitters.
    # Inode and ctime also invalidate an entry after an administrator changes it.
    key = (str(root), _stamp(record_path), _stamp(receipt_path))
    if key not in _ROOTS:
        authority = json.loads(_regular_bytes(record_path))
        raw = _regular_bytes(receipt_path)
        digest = hashlib.sha256(raw).hexdigest()
        if (not isinstance(authority, dict)
                or authority.get("schema") != PUBLICATION_SCHEMA
                or authority.get("generation") != root.name
                or authority.get("receipt_sha256") != digest):
            return None
        _ROOTS[key] = (_receipt(raw, root.name), digest)
    return _ROOTS[key]


def published_member(path: Path) -> Path | None:
    """Return the protected, receipt-bound tool at exactly ``path``, else ``None``.

    Unreadable, unpublished or altered means unknown authority, which stays
    ordinary.  The caller compares the answer with its own spelling: a symlink
    or alias resolves to a different path and is no tool of the copy.
    """
    try:
        resolved = Path(path).resolve(strict=True)
        relative = resolved.relative_to(PROTECTED_GENERATION_STORE).parts
        if not _tool_shape(relative) or not _protected_path(resolved):
            return None
        published = _published_root(PROTECTED_GENERATION_STORE / relative[0])
        if published is None or resolved.stat().st_mode & 0o222:
            return None
        expected = published[0]["files"].get("/".join(relative[1:]))
        key = (str(resolved), _stamp(resolved), expected)
        if key not in _MEMBERS:
            if expected != hashlib.sha256(_regular_bytes(resolved)).hexdigest():
                return None
            _MEMBERS.add(key)
        return resolved
    except (OSError, ValueError, TypeError):
        return None


@functools.lru_cache(maxsize=1)
def _own_generation() -> Path | None:
    try:
        root = Path(__file__).resolve().parents[2]
    except IndexError:
        return None
    return root if (root / "RUNTIME_VERSION.json").is_file() else None


def executing_generation() -> Path | None:
    """The published runtime generation this process runs from, else ``None``.

    A checkout, a test and the installed root program have none.
    """
    return _own_generation()


def live_authority() -> bool:
    """Whether this host holds the protected copy of the generation this process runs.

    The reservation of a waiting gang (#1579) holds work by its demand and
    spares only the movement nodes PrismaBuild itself seals.  Those nodes can
    be told from other work only where a protected copy of their tools exists.
    Where it does not (the host is not enrolled, the copy of a new generation
    has not arrived, the process is not a published generation) nothing a gang
    waits on can be told apart, so the reservation does not apply.
    """
    root = executing_generation()
    if root is None:
        return False
    try:
        copy = PROTECTED_GENERATION_STORE / root.name
        published = _published_root(copy)
        # The copy was made from these receipt bytes, not from a generation
        # that merely has this name.
        return (published is not None
                and published[1] == _receipt_sha256(root / "RUNTIME_VERSION.json"))
    except (OSError, ValueError, TypeError):
        return False


def protected_counterpart(path: Path, *, retained_store: Path) -> Path | None:
    """The protected copy of a retained-generation member, else ``None``.

    ``None`` means this host holds no copy of that generation, or one made
    from other bytes.  A path that already is a protected member returns itself.
    """
    already = published_member(path)
    if already is not None:
        return already
    try:
        store = Path(retained_store).resolve(strict=True)
        relative = Path(path).resolve(strict=True).relative_to(store)
        protected = published_member(PROTECTED_GENERATION_STORE / relative)
        if protected is None:
            return None
        generation = relative.parts[0]
        if (_receipt_sha256(store / generation / "RUNTIME_VERSION.json")
                != _receipt_sha256(PROTECTED_GENERATION_STORE / generation
                                   / "RUNTIME_VERSION.json")):
            return None
        return protected
    except (OSError, ValueError):
        return None


def protected_tools_dir(tools_dir: Path, *, retained_store: Path) -> Path | None:
    """The protected twin of a retained generation's tool directory, else ``None``.

    What a box announces as its tool root once it holds the copy: submitters
    on other boxes then seal paths this box has.  A box without the copy
    announces its own directory, as before.
    """
    probe = protected_counterpart(Path(tools_dir) / PROBE_TOOL, retained_store=retained_store)
    return None if probe is None else probe.parent


if __name__ == "__main__":
    sys.exit(main())
