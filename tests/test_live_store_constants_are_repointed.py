"""Every module constant naming the live store is repointed away from it.

A test must not read or write ``/mnt/shared``. The conftest fixture repoints
the constants that name it, but the list was hand-maintained, so a constant
added later was covered only if somebody remembered. One was not:
``supervise.MIRROR``. ``supervise._proven_roots`` lists
``MIRROR / "runtime-generations"``, so ``test_only_idle_loops_are_stopped``
read the live store on every run of the suite on every box. It passed, which
is why it went unnoticed for as long as it did. The costs surfaced elsewhere:
a test whose result depended on what the running fleet happened to have
published, and a suite that stopped dead for as long as the NFS server was
unreachable, because the mount is ``hard`` and a blocked read never returns.

So the list stops being remembered and starts being checked. This reads the
sources with ``ast`` rather than importing them, for the reason
``test_tools_do_not_run_on_import`` gives: two of these files did their work
at import, and one of them wrote to the live store.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from conftest import LIVE_DEFAULTS

REPO = Path(__file__).resolve().parents[1]
SOURCES = sorted(
    [*(REPO / "tools" / "fleet").glob("*.py"), *(REPO / "src" / "prismabuild").glob("*.py")]
)
LIVE_MOUNT = "/mnt/shared"

#: Constants that name the mount and must NOT be repointed, with the reason.
#: The distinction is destination against classifier. A destination is a root
#: the code reads or writes, and repointing it moves the I/O somewhere safe.
#: A classifier is only ever compared against some other path, to answer
#: "is this on shared storage?", and repointing it does no I/O either way
#: while silently changing the answer the test is checking.
EXEMPT: dict[tuple[str, str], str] = {
    ("pbrun", "SHARED_ROOT"): (
        "classifier: reached only by relative_to() in the placement rules and "
        "by the text of two operator messages, never opened"
    ),
    ("prismabuild.pool", "SHARED_ROOT"): (
        "classifier: reached only by is_relative_to() to decide whether a "
        "checkout is box-local, never opened"
    ),
    ("prismabuild.slurm_lane", "DEFAULT_LANE_ROOT"): (
        "fallback behind PRISMABUILD_SLURM_LANE_ROOT, which LIVE_ENV already "
        "repoints; its value is also asserted equal to the Epilog script's "
        "own spelling, so moving it would break that agreement"
    ),
    ("prismabuild.slurm_lane", "DEFAULT_JOB_STATE_ROOT"): (
        "fallback behind PRISMABUILD_SLURM_JOB_STATE_ROOT, repointed by "
        "LIVE_ENV; see DEFAULT_LANE_ROOT"
    ),
}


def _module_name(path: Path) -> str:
    if path.parent.name == "prismabuild":
        return f"prismabuild.{path.stem}"
    return path.stem


def _live_constants(path: Path) -> list[tuple[str, str, int]]:
    """``(module, attribute, line)`` for each module-level constant naming the mount.

    Derivation is followed. Most of these constants do not spell the mount
    themselves: a module writes ``SH = Path("/mnt/shared/prismabuild-fleet")``
    once and then ``CHECKOUT = SH / "checkout"``, and a reader that only
    matched the literal would call ``CHECKOUT`` clean while it resolves into
    the live store exactly as ``SH`` does. So a name whose value mentions a
    name already known to be live is itself live, applied until it settles.
    """

    tree = ast.parse(path.read_text(), filename=str(path))
    module = _module_name(path)
    live: dict[str, int] = {}
    assignments = []
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        try:
            value = ast.unparse(node.value)
        except Exception:  # pragma: no cover - unparseable node
            continue
        names = [t.id for t in node.targets if isinstance(t, ast.Name)]
        referenced = {n.id for n in ast.walk(node.value) if isinstance(n, ast.Name)}
        assignments.append((names, value, referenced, node.lineno))

    changed = True
    while changed:
        changed = False
        for names, value, referenced, lineno in assignments:
            if LIVE_MOUNT in value or referenced & live.keys():
                for name in names:
                    if name not in live:
                        live[name] = lineno
                        changed = True
    return [(module, name, lineno) for name, lineno in live.items()]


def _declared() -> set[tuple[str, str]]:
    return {(module, attr) for module, attr, _sub in LIVE_DEFAULTS}


@pytest.mark.parametrize("path", SOURCES, ids=lambda p: p.name)
def test_every_live_store_constant_is_in_the_repoint_list(path: Path) -> None:
    declared = _declared()
    missing = [
        f"{module}.{attr} ({path.name}:{line})"
        for module, attr, line in _live_constants(path)
        if (module, attr) not in declared and (module, attr) not in EXEMPT
    ]
    assert missing == [], (
        "these constants name the live store and nothing repoints them, so a "
        "test that reaches one reads or writes the real fleet: "
        + ", ".join(missing)
        + ". Add each to LIVE_DEFAULTS in tests/conftest.py, or to EXEMPT here "
        "with the reason it cannot be reached."
    )


def test_the_repoint_list_names_only_constants_that_exist() -> None:
    """A stale entry is a silent no-op, so it must not survive a rename."""

    live = {(module, attr) for path in SOURCES for module, attr, _l in _live_constants(path)}
    # ``LOCAL_CHECKOUT_ROOT`` on ``materialize`` and its import-time copy on
    # ``pool`` read an environment variable whose default is a local path, so
    # they never name the mount and the scan is right not to report them. The
    # fixture still repoints them, because a test must not write to the real
    # local checkout root either.
    env_backed = {
        ("prismabuild.materialize", "LOCAL_CHECKOUT_ROOT"),
        ("prismabuild.pool", "LOCAL_CHECKOUT_ROOT"),
    }
    # The box-state directory is under ``/tmp``, so it is not the live store
    # and the scan is right not to report it either -- but it is live *state*,
    # belonging to the loops running on this box, and the suite was minting a
    # permanent lock file into it per temp queue root (#265). Repointing it is
    # the fix, so these have to survive this test the same way the two above
    # do: named here with the reason, rather than left out of LIVE_DEFAULTS.
    host_local = {
        ("prismabuild.adaptive_cpu", "BOX_STATE_ROOT"),
        ("mount_latency", "ADMISSION_LOCK_DIR"),
    }
    stale = sorted(
        f"{module}.{attr}"
        for module, attr in _declared() - live - env_backed - host_local
    )
    assert stale == [], (
        "LIVE_DEFAULTS names constants that no longer exist or no longer name "
        "the live store; a stale entry repoints nothing: " + ", ".join(stale)
    )


def test_the_reader_finds_a_constant_it_should_find() -> None:
    """The scan is not vacuously empty."""

    found = _live_constants(REPO / "tools" / "fleet" / "supervise.py")
    assert ("supervise", "MIRROR") in {(m, a) for m, a, _l in found}


def test_the_reader_follows_a_constant_derived_from_another() -> None:
    """A path built from a live constant is live, which is most of them."""

    found = {(m, a) for m, a, _l in _live_constants(REPO / "tools" / "fleet" / "tessera_status.py")}
    # ``SH`` spells the mount; ``CAS``, ``Q`` and ``RES`` are built from it.
    assert ("tessera_status", "SH") in found
    for derived in ("CAS", "Q", "RES"):
        assert ("tessera_status", derived) in found, derived


def test_every_exemption_names_a_constant_that_still_exists() -> None:
    """An exemption outliving its constant would hide the next one silently."""

    live = {(module, attr) for path in SOURCES for module, attr, _l in _live_constants(path)}
    stale = sorted(f"{module}.{attr}" for module, attr in EXEMPT if (module, attr) not in live)
    assert stale == [], (
        "these exemptions name constants that no longer name the live store: "
        + ", ".join(stale)
    )


def test_no_exemption_is_also_repointed() -> None:
    """A constant is a destination or a classifier, and cannot be both."""

    both = sorted(f"{module}.{attr}" for module, attr in _declared() & set(EXEMPT))
    assert both == [], (
        "these are exempted as classifiers and repointed as destinations: "
        + ", ".join(both)
    )
