"""Read-shape coverage checks, separate from names-only topic mapping."""
from __future__ import annotations

from pathlib import Path
import stat

from .holder_census import CPU_METADATA, GPU_METADATA
from .queue_bell_topics import (HostReservations, WorkerOffers, HOST_RESERVATION_TOPICS,
                                AdaptiveInputs)


class HostInputShapes:
    """The filesystem read set, independent of the watch installation code.

    Names-only token reads need no descent below their containing directory.
    Unexpected shapes are incomplete, rather than extending the traversal.
    """

    @staticmethod
    def kind(relative: Path) -> str | None:
        parts = relative.parts
        if parts and parts[0] in AdaptiveInputs.action_directories:
            if len(parts) == 1:
                return "directory"
            if len(parts) == 2 and relative.suffix == ".json":
                return "file" if AdaptiveInputs.hint(relative) else "unknown"
            return None
        if not parts or parts[0] not in ("workers", "reservations"):
            return None
        if len(parts) == 1:
            return "directory"
        if parts[0] == "workers":
            if len(parts) == 2 and parts[1].endswith(".json"):
                return "file" if WorkerOffers.hint(relative) else "unknown"
            return None
        if not HOST_RESERVATION_TOPICS[0].accepts_id(parts[1]):
            return "unknown"
        if len(parts) == 2 or (len(parts) == 3 and parts[2] in HostReservations.token_directories):
            return "directory"
        if len(parts) == 3 and parts[2] == "adaptive":
            return "directory"
        if len(parts) == 4 and parts[2] == "adaptive":
            return "file" if parts[3] in AdaptiveInputs.mirror_files else None
        if len(parts) == 3 and parts[2] in ("cpu-map.json", "capacity.json"):
            return "file"
        if len(parts) == 4 and parts[2] == "held":
            return "holder"
        if len(parts) == 5 and parts[2] == "held":
            return "file" if parts[4] in (CPU_METADATA, GPU_METADATA) else "entry"
        if len(parts) == 4 and parts[2] in ("free", "minted"):
            return "entry"
        return None

    @staticmethod
    def watchable(kind: str, info) -> bool:
        if kind == "unknown" or stat.S_ISLNK(info.st_mode):
            return False
        if kind == "directory":
            return stat.S_ISDIR(info.st_mode)
        if kind == "holder":
            # Non-directories are excluded by the authoritative predicate;
            # their later replacement is visible through the held watch.
            return True
        if kind == "entry":
            return stat.S_ISREG(info.st_mode)
        return (stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                and bool(info.st_mode & 0o444))

