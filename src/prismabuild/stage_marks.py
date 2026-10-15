"""Marks and filenames shared by stage writers and reclamation roles."""
from __future__ import annotations

import os
import re

STAGE_OBJECT_MARK = ".pbstage@"
STAGE_SOURCE_XATTR = "user.pbstage.source"
STAGE_SOURCE_FORMAT = "st_size:st_mtime_ns:st_ino at copy time"
STAGE_MOVER_XATTR = "user.pbstage.mover"

_STAGE_TEMPORARY = re.compile(r"\.pbstage@\d+\+\d+\.(\d+)\.\d+\.tmp$")


def source_identity(source: os.stat_result) -> str:
    """The source version recorded in the source xattr at copy time."""

    return f"{source.st_size}:{source.st_mtime_ns}:{source.st_ino}"


def prewarm_temporary_name(name: str, pid: int, tid: int) -> str:
    """The in-flight name for one prewarm stage object."""

    return f"{name}.{pid}.{tid}.tmp"


def prewarm_temporary_pid(name: str) -> str | None:
    """The textual owner PID of a prewarm temporary, if the name matches."""

    found = _STAGE_TEMPORARY.search(name)
    return found.group(1) if found else None
