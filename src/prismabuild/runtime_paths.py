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


#: Where a fleet entry point can sit under a root, in the order searched.
#: The checkout keeps every tool at ``tools/fleet/name.py`` and only a
#: published generation carries the flat ``tools/name.py`` copy, so the
#: checkout layout is looked at first: a published root holds both, and a
#: checkout holds one.
TOOL_LAYOUTS = (("tools", "fleet"), ("tools",))


def tool_candidates(name: str, *, root: str | Path) -> tuple[Path, ...]:
    """Every path ``name`` could have under ``root``, in the order searched.

    Kept beside ``generation_root`` because it is the same fact about the
    publisher's two layouts, and a refusal that names both candidates needs
    the list rather than the answer.
    """

    return tuple(
        Path(root).joinpath(*parts, name) for parts in TOOL_LAYOUTS
    )


def fleet_tool(name: str, *, root: str | Path) -> Path | None:
    """The path of fleet entry point ``name`` under ``root``, or ``None``.

    A launcher that hardcodes one of the two layouts works under one of them
    and not the other. ``pool_reset`` hardcoded the published one, so every
    path-addressed reset run from a checkout, which is the invocation the
    operating guide shows, started a child on a file that does not exist and
    exited 2.

    ``None`` rather than an exception, because the caller is the one that can
    say what a missing launcher means: a tool imported for a report has
    nothing to refuse yet, and the refusal belongs where the child would have
    been started.
    """

    for candidate in tool_candidates(name, root=root):
        if candidate.is_file():
            return candidate
    return None
