#!/usr/bin/env python3
"""Render or persist the fixed RAM mount unit; never activate a mount (#1032).

Invoke with a trusted interpreter's -I -B flags from an independently qualified,
pinned generation. Pinning paths is not source attestation. Owner IDs and size
are operator inputs, not discovered defaults or runtime admission decisions.
"""
from __future__ import annotations

# Disable bytecode before loading the pinned validator (bootstrap ordering).
# ruff: noqa: E402, I001
import sys

sys.dont_write_bytecode = True
if __name__ == "__main__" and not sys.flags.isolated:
    raise SystemExit("invoke with python3 -I -B; inherited Python paths are unsupported")

import argparse
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import uuid
from typing import cast

_ENTRYPOINT = Path(__file__).resolve(strict=True)
sys.path.insert(0, str(_ENTRYPOINT.parent))
from runtime_paths import generation_root  # noqa: E402

_GENERATION = generation_root(_ENTRYPOINT)
sys.path.insert(0, str(_GENERATION / "src"))
from prismabuild.storage_tiers import GIB, read_ram_policy  # noqa: E402

UNIT_NAME = "ram-prewarm.mount"
_MOUNTPOINT = "/ram/prewarm"
_INSTALL_ROOT = Path("/")  # Private test boundary only; never a CLI option.


class _Once(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if getattr(namespace, self.dest, None) is not None:
            parser.error(f"duplicate {option_string}")
        setattr(namespace, self.dest, values if self.nargs != 0 else True)


def _positive(text: str) -> int:
    if re.fullmatch(r"[1-9][0-9]*", text) is None:
        raise argparse.ArgumentTypeError("expected a canonical positive decimal integer")
    try:
        return int(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("integer is too large") from exc


def _owner(text: str) -> int:
    number = _positive(text)
    if number >= 4294967295:
        raise argparse.ArgumentTypeError("owner must be non-root usable uint32 (1..4294967294)")
    return number


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate policy field: {key}")
        result[key] = value
    return result


def plan_unit(*, policy: str, size_bytes: int, owner_uid: int, owner_gid: int) -> bytes:
    """Validate explicit inputs and render the sole authoritative unit format."""
    path = Path(policy).resolve(strict=True)
    if not path.is_relative_to(_GENERATION):
        raise ValueError("policy must belong to the pinned entrypoint generation")
    # The shared validator owns the schema; also refuse ambiguous JSON keys.
    json.loads(path.read_text(), object_pairs_hook=_unique_object)
    declared = read_ram_policy(path)
    if declared is None:
        raise ValueError("invalid RAM policy")
    if declared["mountpoint"] != _MOUNTPOINT:
        raise ValueError("only /ram/prewarm is supported")
    if type(size_bytes) is not int or not 0 < size_bytes <= cast(int, declared["ceiling_gib_max"]) * GIB:
        raise ValueError("size exceeds policy ceiling or is not positive integer bytes")
    if any(type(number) is not int or not 0 < number < 4294967295
           for number in (owner_uid, owner_gid)):
        raise ValueError("owner IDs must be non-root usable uint32")
    return (
        "[Unit]\nDescription=PrismaBuild RAM tier tmpfs\n\n"
        "[Mount]\nWhat=tmpfs\nWhere=/ram/prewarm\nType=tmpfs\n"
        f"Options=size={size_bytes},noswap,mpol=interleave,uid={owner_uid},"
        f"gid={owner_gid},mode=0755\n\n"
        "[Install]\nWantedBy=local-fs.target\n"
    ).encode("ascii")


def _trusted_directory(path: Path) -> os.stat_result:
    metadata = path.lstat()
    if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != 0
            or metadata.st_gid != 0 or metadata.st_mode & 0o022):
        raise ValueError(f"untrusted system directory: {path}")
    return metadata


def _same_inode(left, right) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _conflict_path_is_present(path: Path) -> bool:
    """Only a missing name proves absence; unknown metadata refuses install."""
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _conflicts(root: Path) -> None:
    fstab = root / "etc/fstab"
    if _conflict_path_is_present(fstab):
        for line in fstab.read_text().splitlines():
            fields = line.split("#", 1)[0].split()
            if len(fields) >= 2:
                mountpoint = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), fields[1])
                # Linux treats repeated leading slashes as one. No filesystem
                # resolution: effective namespace/symlink aliases are operator-owned.
                if (mountpoint.startswith("/")
                        and os.path.normpath("/" + mountpoint.lstrip("/")) == _MOUNTPOINT):
                    raise ValueError("fstab mountpoint conflict; operator resolution required")
    directories = ("etc/systemd/system", "run/systemd/system",
                   "usr/local/lib/systemd/system", "usr/lib/systemd/system",
                   "lib/systemd/system", "run/systemd/generator",
                   "run/systemd/generator.early", "run/systemd/generator.late")
    for directory in directories:
        base = root / directory
        names = (f"{UNIT_NAME}.d", "ram-.mount.d", "mount.d")
        if directory != "etc/systemd/system":
            names += (UNIT_NAME,)
        for name in names:
            candidate = base / name
            if _conflict_path_is_present(candidate):
                raise ValueError(f"unit conflict; operator resolution required: {candidate}")


def _check_unit_metadata(metadata: os.stat_result) -> None:
    if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != 0
            or metadata.st_gid != 0 or stat.S_IMODE(metadata.st_mode) != 0o644
            or metadata.st_nlink != 1):
        raise ValueError("existing unit must be nonsymlink root:root0644 regular file")


def _existing_unit(directory_fd: int, unit: bytes) -> bool:
    try:
        metadata = os.stat(UNIT_NAME, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    _check_unit_metadata(metadata)
    descriptor = os.open(UNIT_NAME, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                         dir_fd=directory_fd)
    with os.fdopen(descriptor, "rb") as stream:
        opened = os.fstat(stream.fileno())
        _check_unit_metadata(opened)
        if not _same_inode(metadata, opened):
            raise ValueError("existing unit identity changed")
        if stream.read(len(unit) + 1) != unit:
            raise ValueError("differing existing unit; operator resolution required")
    return True


def _systemctl(root: Path, arguments: list[str]) -> None:
    # A private installer fixture cannot accidentally contact the live manager.
    if root != Path("/"):
        raise ValueError("systemctl is forbidden for a private installation root")
    subprocess.run(["/usr/bin/systemctl", *arguments], check=True,
                   env={"PATH": "/usr/bin:/bin", "LANG": "C"})


def persist_unit(unit: bytes) -> None:
    """Persist root:root0644 without overwrite, then reload/enable without now.

    Root administrators and independently qualified source are trusted. These
    checks refuse unsafe observed metadata, not malicious concurrent root
    mutation. A committed unit is retained on reload/enable failure.
    """
    if os.geteuid() != 0:
        raise PermissionError("--install requires effective UID 0")
    root = _INSTALL_ROOT
    directory = root / "etc/systemd/system"
    ancestors = (root, root / "etc", root / "etc/systemd", directory)
    identities = [_trusted_directory(path) for path in ancestors]
    _conflicts(root)
    directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    temporary = None
    try:
        if not _same_inode(identities[-1], os.fstat(directory_fd)):
            raise ValueError("system directory identity changed")
        if not _existing_unit(directory_fd, unit):
            name = f".{UNIT_NAME}.{uuid.uuid4().hex}.tmp"
            descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                 0o600, dir_fd=directory_fd)
            temporary = name
            try:
                os.fchown(descriptor, 0, 0)
                os.fchmod(descriptor, 0o644)  # Only this newly created inode.
                remaining = memoryview(unit)
                while remaining:
                    written = os.write(descriptor, remaining)
                    if written <= 0:
                        raise OSError("unit write made no progress")
                    remaining = remaining[written:]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            for path, identity in zip(ancestors, identities, strict=True):
                if not _same_inode(identity, _trusted_directory(path)):
                    raise ValueError("system directory identity changed")
            # link is an atomic no-overwrite commit, unlike replace/rename.
            os.link(name, UNIT_NAME, src_dir_fd=directory_fd, dst_dir_fd=directory_fd,
                    follow_symlinks=False)
            os.unlink(name, dir_fd=directory_fd)
            temporary = None
            os.fsync(directory_fd)
    finally:
        try:
            if temporary is not None:
                os.unlink(temporary, dir_fd=directory_fd)
        finally:
            os.close(directory_fd)
    _systemctl(root, ["daemon-reload"])
    _systemctl(root, ["enable", UNIT_NAME])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--policy", required=True, action=_Once,
                        help="RAM policy JSON inside the pinned entrypoint generation")
    parser.add_argument("--size-bytes", required=True, type=_positive, action=_Once,
                        help="positive decimal mount size in bytes, bounded by the policy ceiling")
    parser.add_argument("--owner-uid", required=True, type=_owner, action=_Once,
                        help="mount owner UID: canonical decimal non-root uint32 (1..4294967294)")
    parser.add_argument("--owner-gid", required=True, type=_owner, action=_Once,
                        help="mount owner GID: canonical decimal non-root uint32 (1..4294967294)")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--render", nargs=0, action=_Once,
                      help="render the fixed unit to stdout without installing or activating it")
    mode.add_argument("--install", nargs=0, action=_Once,
                      help="persist as root, reload and enable the unit without starting the mount")
    args = parser.parse_args(argv)
    try:
        unit = plan_unit(policy=args.policy, size_bytes=args.size_bytes,
                         owner_uid=args.owner_uid, owner_gid=args.owner_gid)
        if args.render:
            sys.stdout.buffer.write(unit)
        else:
            persist_unit(unit)
    except (OSError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"RAM mount unit refused: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
