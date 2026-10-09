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
import errno
import fcntl
import functools
import json
import math
import os
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import sys
import tempfile
import time

if __package__:
    from .digest_primitives import new_sha256, raw_sha256, sorted_json
else:
    # The root installer ships the same digest owner beside this program.
    # Isolated Python does not add the script directory to its import path.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from digest_primitives import new_sha256, raw_sha256, sorted_json

PROTECTED_GENERATION_STORE = Path("/opt/prismabuild/movement-generations")
PUBLICATION_RECORD = "MOVEMENT_PUBLICATION.json"
PUBLICATION_SCHEMA = "prismabuild.movement-publication.v1"
RUNTIME_SCHEMA = "prismaquant.prismabuild.runtime_version.v1"
STATUS_SCHEMA = "prismabuild.movement-publication-status.v1"
#: The enrolled host's root-owned settings (written once by the installer).
DEFAULT_CONFIG = Path("/etc/prismabuild/movement-publish.json")
#: The publisher approval that binds a generation's bytes to its publisher
#: (#1659): a sibling ``<generation>.approval`` file in the enrolled store
#: holds the HMAC of the receipt digest.  Without a valid approval the timer
#: publishes nothing (fallback to main, never deadlock).
APPROVAL_SUFFIX = ".approval"
#: Where the enrolled host holds the verification secret (root-only, 0400).
APPROVAL_KEY_PATH = Path("/etc/prismabuild/movement-approval.key")
#: Copy age supplies a minimum transition delay, not evidence of completion.
#: Claim admission also requires a complete census with no unqualified
#: movement row on this host. READY time has no execution deadline.
PROTECTED_COPY_MATURITY_S = 600.0
#: The movement tool every tool root carries; its copy stands for the root.
PROBE_TOOL = "stage_release.py"
#: The root unit never fills its filesystem: it publishes nothing below this.
#: A copy is about 27 MB and the store only grows, so the host falls back
#: (no copy, main's behaviour) instead of starving the rest of the box.
MIN_FREE_BYTES = 1 << 30
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
        _RECEIPT_SHA[key] = raw_sha256(_regular_bytes(path))
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

    ``receipt_sha256`` is the digest of the receipt the caller read.  The copy
    is refused when the receipt on disk differs from it (the generation changed
    under the copy), when any member differs from the digest the receipt names,
    or when the store has no root custody.  The digest binds the copy to the
    bytes that were read; it does not authenticate who published them.  The
    root unit (:func:`converge`) takes its authority from the store's live
    pointer.  A publication is never replaced.
    """
    if os.geteuid() != 0:
        raise PermissionError("movement publication requires root authority")
    if _DIGEST.fullmatch(receipt_sha256) is None:
        raise ValueError("movement publication requires a receipt sha256")
    source = Path(source).resolve(strict=True)
    if _GENERATION.fullmatch(source.name) is None:
        raise ValueError("invalid movement generation name")
    raw = _regular_bytes(source / "RUNTIME_VERSION.json")
    if raw_sha256(raw) != receipt_sha256:
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
        room = os.statvfs(store)
        if room.f_bavail * room.f_frsize < MIN_FREE_BYTES:
            raise OSError(errno.ENOSPC, f"less than {MIN_FREE_BYTES} bytes free in {store}")
        _sweep_staging(store)
        stage = Path(tempfile.mkdtemp(prefix=".publication-", dir=store))
        try:
            for name, expected in receipt["files"].items():
                member = source / name
                # Every source component must be literal, not a symlink or alias.
                if member.resolve(strict=True) != member:
                    raise ValueError(f"runtime member is a symlink: {name}")
                data = _regular_bytes(member)
                if raw_sha256(data) != expected:
                    raise ValueError(f"runtime member digest differs: {name}")
                copied = stage / name
                _create_directories(copied.parent)
                copied.write_bytes(data)
                copied.chmod(0o555 if member.stat().st_mode & 0o111 else 0o444)
            (stage / "RUNTIME_VERSION.json").write_bytes(raw)
            (stage / PUBLICATION_RECORD).write_text(sorted_json({
                "schema": PUBLICATION_SCHEMA, "generation": source.name,
                "receipt_sha256": receipt_sha256,
                "published_unix": time.time(),
            }) + "\n", encoding="utf-8")
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


def _read_approval_key() -> bytes:
    """The verification secret from its root-only file (#1659)."""
    import hmac as _hmac_mod
    path = APPROVAL_KEY_PATH
    if not _protected_path(path):
        raise PermissionError(f"{path} has no root custody")
    info = path.stat()
    if info.st_uid != 0 or info.st_mode & 0o077:
        raise PermissionError(f"{path} must be root-owned 0400")
    raw = _regular_bytes(path).decode("utf-8").strip()
    if _DIGEST.fullmatch(raw) is None:
        raise ValueError("movement approval key is not 64 hex")
    return bytes.fromhex(raw)


def approval_hmac(receipt_sha256: str, key: bytes) -> str:
    """The approval tag for ``receipt_sha256`` under ``key`` (#1659)."""
    import hmac as _hmac_mod
    return _hmac_mod.new(key, receipt_sha256.encode("utf-8"), new_sha256).hexdigest()


def _valid_approval(store: Path, generation: str, digest: str) -> bool:
    """Whether the enrolled store holds a valid publisher approval (#1659)."""
    import hmac as _hmac_mod
    try:
        key = _read_approval_key()
        sibling = store / f"{generation}{APPROVAL_SUFFIX}"
        tag = sibling.read_text(encoding="utf-8").strip()
        if _DIGEST.fullmatch(tag) is None:
            return False
        expected = approval_hmac(digest, key)
        return _hmac_mod.compare_digest(tag, expected)
    except (OSError, ValueError, TypeError, PermissionError):
        return False


def sign_approval(generation_dir: Path, *, receipt_sha256: str,
                  key_path: Path | None = None) -> Path:
    """Write the publisher approval sibling for ``generation_dir`` (#1659).

    The publisher principal runs this after publishing a generation, with its
    dedicated signing secret (0600, owned by the publisher account, not shared).
    The secret must live under a publisher-only account that ordinary
    submitters and store owners cannot read: a key in the same account that
    owns the runtime store does not separate authority and is refused here, so
    a same-user forgery gets no approval and enrolled hosts keep the behaviour
    of main. Fleet agents without the publisher secret cannot forge an approval
    for tampered bytes. The deployment contract names the publisher account;
    a person (Rob or the CEO) approves that principal once, not per generation.
    """
    source = Path(generation_dir).absolute()
    if _DIGEST.fullmatch(receipt_sha256) is None:
        raise ValueError("trusted receipt digest is not 64 hex")
    key_file = key_path if key_path is not None else Path.home() / ".config" / "prismabuild" / "movement-approval.key"
    info = key_file.stat()
    if info.st_mode & 0o077:
        raise PermissionError("movement signing key must be 0600 or stricter")
    if info.st_uid != os.geteuid():
        raise PermissionError("movement signing key must belong to the publisher account")
    try:
        store = source.parent.resolve(strict=True)
        store_uid = store.stat().st_uid
    except OSError as exc:
        raise PermissionError("movement signing key store is unreadable") from exc
    if info.st_uid == store_uid:
        raise PermissionError(
            "movement signing key shares its account with the runtime store; "
            "use the dedicated publisher account")
    secret = _regular_bytes(key_file).decode("utf-8").strip()
    if _DIGEST.fullmatch(secret) is None:
        raise ValueError("movement signing key is not 64 hex")
    tag = approval_hmac(receipt_sha256, bytes.fromhex(secret))
    sibling = source.parent / f"{source.name}{APPROVAL_SUFFIX}"
    sibling.write_text(tag + "\n", encoding="utf-8")
    return sibling


def _write_status(path: Path, record: Mapping[str, object]) -> None:
    """Replace the status record atomically; a failure to write it changes nothing else."""
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary.write_text(sorted_json(record) + "\n", encoding="utf-8")
        temporary.chmod(0o644)
        os.replace(temporary, path)
    except OSError:
        temporary.unlink(missing_ok=True)


def converge(config: Mapping[str, object], *, now: float | None = None) -> dict[str, object]:
    """Publish the enrolled store's live generation here, once (root only).

    ``config`` names ``runtime`` (the live pointer), ``generation_store`` (the
    store the pointer must lead into) and ``status`` (where the result is
    recorded). A dedicated publisher approves the trusted receipt digest.
    The timer compares that approval with the exposed receipt and verifies
    every copied member. The live generation is the only one published.
    A host that runs an older generation retains its existing protected copy.
    The result is ``current``, ``published`` or ``error``.
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
        digest = raw_sha256(_regular_bytes(live / "RUNTIME_VERSION.json"))
        if not _valid_approval(store, live.name, digest):
            raise PermissionError(
                "the live generation has no valid publisher approval; "
                "a store writer cannot approve its own bytes")
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
    print(sorted_json(result))
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
        digest = raw_sha256(raw)
        if (not isinstance(authority, dict)
                or authority.get("schema") != PUBLICATION_SCHEMA
                or authority.get("generation") != root.name
                or authority.get("receipt_sha256") != digest):
            return None
        _ROOTS[key] = (_receipt(raw, root.name), digest)
    return _ROOTS[key]

def protected_published_unix(generation: str) -> float | None:
    """When the protected copy of ``generation`` was published, else ``None``."""
    try:
        record_path = PROTECTED_GENERATION_STORE / generation / PUBLICATION_RECORD
        if not _protected_path(record_path):
            return None
        authority = json.loads(_regular_bytes(record_path))
        published = authority.get("published_unix")
        if (isinstance(published, (int, float)) and not isinstance(published, bool)
                and math.isfinite(published) and published > 0):
            return float(published)
        return None
    except (OSError, ValueError, TypeError):
        return None


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
            if expected != raw_sha256(_regular_bytes(resolved)):
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


def live_authority(*, now: float | None = None) -> bool:
    """Whether this host holds a mature protected copy of the generation this process runs.

    The reservation of a waiting gang (#1579) holds work by its demand and
    spares only the movement nodes PrismaBuild itself seals.  Those nodes can
    be told from other work only where a protected copy of their tools exists.
    Where it does not (the host is not enrolled, the copy of a new generation
    has not arrived, the process is not a published generation) nothing a gang
    waits on can be told apart, so the reservation does not apply.

    Copy age supplies only a minimum transition delay. The claim path also
    requires a complete census that proves all unqualified movement rows
    have ended on this host. Legacy rows never gain a role from mutable bytes.
    """
    root = executing_generation()
    if root is None:
        return False
    try:
        copy = PROTECTED_GENERATION_STORE / root.name
        published = _published_root(copy)
        # The copy was made from these receipt bytes, not from a generation
        # that merely has this name.
        if (published is None
                or published[1] != _receipt_sha256(root / "RUNTIME_VERSION.json")):
            return False
        birth = protected_published_unix(root.name)
        if birth is None:
            return True
        moment = time.time() if now is None else float(now)
        return bool(math.isfinite(moment) and moment - float(birth) > PROTECTED_COPY_MATURITY_S)
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
