"""Source identity marks shared by movers and reclamation roles."""
from __future__ import annotations

import os

STAGE_SOURCE_XATTR = "user.pbstage.source"
STAGE_MOVER_XATTR = "user.pbstage.mover"


def source_identity(source: os.stat_result) -> str:
    """The source version recorded in the source xattr at copy time."""

    return f"{source.st_size}:{source.st_mtime_ns}:{source.st_ino}"
