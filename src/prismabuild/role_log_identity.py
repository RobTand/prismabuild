"""Read-only diagnostic namespace and append-description observations.

Policy (retention versus exact legacy metadata migration) belongs to callers.
These observations do not serialize writers or the directory namespace.
"""
from __future__ import annotations

import os
import stat
from pathlib import Path


def check_directory(fd: int, path: Path, uid: int) -> None:
    held = os.fstat(fd)
    named = path.stat(follow_symlinks=False)
    if (not stat.S_ISDIR(held.st_mode) or not stat.S_ISDIR(named.st_mode)
            or held.st_uid != uid or named.st_uid != uid
            or (held.st_dev, held.st_ino) != (named.st_dev, named.st_ino)):
        raise OSError("unsafe or renamed diagnostic log directory")


def check_append_descriptors(pid: int, held: os.stat_result,
                             proc_root: Path) -> list[dict]:
    observations = []
    for descriptor in (1, 2):
        writer = os.stat(proc_root / str(pid) / "fd" / str(descriptor))
        if (writer.st_dev, writer.st_ino) != (held.st_dev, held.st_ino):
            raise OSError("role writer FD does not name the current log")
        with (proc_root / str(pid) / "fdinfo" / str(descriptor)).open("rb") as stream:
            raw = stream.read(4097)
        flags = [line.split(b":", 1)[1].strip() for line in raw.splitlines()
                 if line.startswith(b"flags:")]
        try:
            if len(raw) > 4096 or len(flags) != 1:
                raise ValueError("missing or oversized fdinfo flags")
            mode = int(flags[0], 8)
        except ValueError as exc:
            raise OSError("role writer append flags unreadable") from exc
        if (not mode & os.O_APPEND
                or mode & os.O_ACCMODE not in (os.O_WRONLY, os.O_RDWR)):
            raise OSError("role writer FD is not writable append")
        observations.append({"fd": descriptor, "dev": writer.st_dev,
                             "ino": writer.st_ino, "flags": mode,
                             "flags_text": flags[0].decode("ascii")})
    return observations
