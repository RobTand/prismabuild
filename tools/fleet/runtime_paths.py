"""Bind a fleet entry point to the complete runtime generation that contains it."""

from __future__ import annotations

from pathlib import Path


def generation_root(entrypoint: str | Path) -> Path:
    """Return the immutable runtime root containing ``entrypoint``.

    The publisher retains both historical layouts: ``tools/name.py`` for the
    stable fleet commands and ``tools/fleet/name.py`` for checkout-compatible
    imports.  Resolving the entry point first crosses the live ``repo`` symlink
    once; every source import and child launcher can then use that same absolute
    generation instead of resolving the mutable live name independently.
    """

    resolved = Path(entrypoint).resolve(strict=True)
    directory = resolved.parent
    if directory.name == "fleet" and directory.parent.name == "tools":
        return directory.parent.parent
    if directory.name == "tools":
        return directory.parent
    raise RuntimeError(
        f"fleet entry point is outside tools/ or tools/fleet/: {resolved}"
    )
