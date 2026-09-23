"""Bounded local scratch is a host reservation, charged at claim (#911).

An action that writes scratch to a box's local disk -- a replay spill, a
cotangent sink, a cache root -- already bounds it with a pair of sealed
environment variables: a root and a byte ceiling.  PrismaBuild could not see
those pairs, so it could place two large scratch holders on one box, or one
on a box whose disk cannot hold it, and the action failed closed only after
it had taken its claim.

An action now names its pairs in one more sealed variable,
:data:`PAIRS_ENV`, as ``ROOT_ENV:MAX_ENV`` items separated by commas.  pbrun
derives the reservation from the named ceilings, ``ceil(MAX / 2**30)`` per
pair, and charges it to :data:`KIND` -- the one local-disk host kind a box
declares with ``worker_loop.py --spool-gb`` in the roster (#910).  The
produced-output spool window (#747) draws from the same kind, because both
use the same disk.  No new ledger and no new dispatcher: the kind passes
through the ordinary host ledger like ``mem_gb``.

Off -- :data:`PAIRS_ENV` absent or empty -- nothing is derived and nothing
is read, whatever other variables the environment carries.  A pair is never
inferred from a variable's name.
"""
from __future__ import annotations

from collections.abc import Mapping
from pathlib import PurePosixPath
import re

#: The sealed variable that names an action's bounded-local pairs.
PAIRS_ENV = "PRISMABUILD_LOCAL_SCRATCH_PAIRS"
#: The one local-disk host kind, in GiB like ``mem_gb``.  It keeps the name
#: #747 gave it for the spool window; scratch and spool share it.
KIND = "spool_gb"
GIB = 1 << 30
#: The spool window's own pair.  It is charged through
#: ``PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW`` (#747), and listing it here too
#: would charge the same bytes twice.
SPOOL_PAIR = ("PRISMABUILD_PRODUCED_SPOOL_ROOT",
              "PRISMABUILD_PRODUCED_SPOOL_MAX_BYTES")
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class LocalScratchError(ValueError):
    """A declared bounded-local pair that cannot be charged."""


def _canonical_root(value: object) -> str | None:
    """``value`` when it is a canonical absolute POSIX path other than ``/``."""

    if (not isinstance(value, str) or not value.startswith("/")
            or "\x00" in value or ".." in PurePosixPath(value).parts
            or str(PurePosixPath(value)) != value or value == "/"):
        return None
    return value


def scratch_pairs(variables: Mapping[str, str]) -> list[dict[str, object]]:
    """The pairs ``variables`` declares, validated, in declaration order.

    Each entry is ``{"root_env", "max_env", "root", "max_bytes"}``.  Raises
    :class:`LocalScratchError` for a malformed list, a name that is not a
    variable name, a name used twice, the spool window's pair, a named
    variable the environment does not carry, a root that is not a canonical
    absolute path, a root declared twice, or a ceiling that is not a
    positive decimal integer.
    """

    raw = variables.get(PAIRS_ENV)
    if raw is None or raw == "":
        return []
    if not isinstance(raw, str):
        raise LocalScratchError(f"{PAIRS_ENV} must be a string")
    pairs: list[dict[str, object]] = []
    names: set[str] = set()
    roots: set[str] = set()
    for item in raw.split(","):
        root_env, sep, max_env = item.partition(":")
        if not sep or ":" in max_env:
            raise LocalScratchError(
                f"{PAIRS_ENV} item {item!r} is not ROOT_ENV:MAX_ENV")
        for name in (root_env, max_env):
            if not _NAME.match(name):
                raise LocalScratchError(
                    f"{PAIRS_ENV} names {name!r}, which is not a variable name")
            if name == PAIRS_ENV:
                raise LocalScratchError(f"{PAIRS_ENV} cannot name itself")
            if name in names:
                raise LocalScratchError(f"{PAIRS_ENV} names {name} twice")
            names.add(name)
        if {root_env, max_env} & set(SPOOL_PAIR):
            raise LocalScratchError(
                f"{PAIRS_ENV} must not list the produced spool's pair: its window "
                "is charged by PRISMABUILD_PRODUCED_SPOOL_HOST_WINDOW=1")
        if root_env not in variables or max_env not in variables:
            missing = [n for n in (root_env, max_env) if n not in variables]
            raise LocalScratchError(
                f"{PAIRS_ENV} declares {root_env}:{max_env}, but the sealed "
                f"environment does not set {', '.join(missing)}")
        root = _canonical_root(variables[root_env])
        if root is None:
            raise LocalScratchError(
                f"{root_env} must be a canonical absolute path other than /, "
                f"not {variables[root_env]!r}")
        if root in roots:
            raise LocalScratchError(f"{PAIRS_ENV} declares the root {root} twice")
        roots.add(root)
        maximum = variables[max_env]
        if (not isinstance(maximum, str) or not maximum.isascii()
                or not maximum.isdigit() or int(maximum) <= 0):
            raise LocalScratchError(
                f"{max_env} must be a positive integer byte ceiling, not {maximum!r}")
        pairs.append({"root_env": root_env, "max_env": max_env,
                      "root": root, "max_bytes": int(maximum)})
    return pairs


def scratch_terms(variables: Mapping[str, str]) -> dict[str, int]:
    """The host demand an environment's declared pairs derive, or ``{}``.

    Each pair's ceiling is rounded up to whole GiB on its own, so each
    reservation covers its own bound; the terms are their sum.
    """

    total = sum(-(-int(pair["max_bytes"]) // GIB) for pair in scratch_pairs(variables))
    return {KIND: total} if total else {}
