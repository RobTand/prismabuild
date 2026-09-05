"""Keep every test off the fleet's live store, and say so if one gets there.

Several modules default a root to the shared mount: ``pbrun.SH``,
``pbstatus.SHARED_ROOT``, ``pool_reset.SH``, ``fleet_submit.SH``,
``pool.DEFAULT_POOL_ROOT``, and the lane's ``PRISMABUILD_SLURM_LANE_ROOT`` and
``PRISMABUILD_SLURM_JOB_STATE_ROOT``. A test that forgets to pass a root then
reads or writes the live queue, CAS, or lane root. Between 2026-09-04 and
2026-09-05 the lane tests filed 336 terminal records, 145 CAS requests, and 6
receipts into the live store that way, because their fixture never overrode
``pbrun.SH``. Those files were moved to ``quarantine/pytest-leak-2026-09-05``
on the mount.

Two guards, because neither is complete on its own:

*   ``_off_the_live_store`` repoints every such default at the test's own
    ``tmp_path`` before each test. It cannot reach a default bound at function
    definition, such as ``PoolQueue(root=DEFAULT_POOL_ROOT)``, so a test that
    calls one of those without a root still gets the live path.
*   ``pytest_sessionfinish`` lists the live store's queue directories and lane
    root before the session and after it, and fails the session when a new
    entry names this session's ``basetemp``. A new entry that does not is
    reported but not counted: the fleet may file real work while the suite
    runs.
"""
from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest

#: The mount the fleet executes against. Absent on a box without the mount,
#: in which case the session guard does nothing. The environment override
#: exists so the guard itself can be exercised against a scratch store.
LIVE_ROOT = Path(
    os.environ.get("PRISMABUILD_TEST_LIVE_ROOT") or "/mnt/shared/prismabuild-fleet"
)

#: Directories a test could file into by mistake, relative to ``LIVE_ROOT``.
WATCHED = (
    "pb-queue/ready",
    "pb-queue/claimed",
    "pb-queue/done",
    "pb-queue/failed",
    "pb-queue/withdrawn",
    "slurm",
)

#: Module attributes that default to the live store, and the subpath under the
#: test's guard root each is repointed at. Applied only to modules already
#: imported; the tests import these through their own ``sys.path`` inserts.
LIVE_DEFAULTS = (
    ("pbrun", "SH", "fleet"),
    ("pbstatus", "SHARED_ROOT", "fleet"),
    ("pbstatus", "DEFAULT_QUEUE_ROOT", "fleet/pb-queue"),
    ("pool_reset", "SH", "fleet"),
    ("fleet_submit", "SH", "fleet"),
    ("worker_loop", "SH", "fleet"),
    ("prismabuild.pool", "DEFAULT_POOL_ROOT", "pb-queue"),
)

#: Environment variables the lane and the pool read on use.
LIVE_ENV = (
    ("PRISMABUILD_SLURM_LANE_ROOT", "slurm"),
    ("PRISMABUILD_SLURM_JOB_STATE_ROOT", "slurm/jobs"),
    ("PRISMABUILD_POOL_ROOT", "pb-queue"),
)


@pytest.fixture(autouse=True)
def _off_the_live_store(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    root = tmp_path / "live-guard"
    for name, sub in LIVE_ENV:
        monkeypatch.setenv(name, str(root / sub))
    for module_name, attr, sub in LIVE_DEFAULTS:
        module = sys.modules.get(module_name)
        if module is not None and hasattr(module, attr):
            monkeypatch.setattr(module, attr, root / sub)
    yield


def listing(live_root: Path = LIVE_ROOT) -> dict[str, set[str]]:
    """The names under each watched directory, empty for one that is absent."""

    out: dict[str, set[str]] = {}
    for rel in WATCHED:
        try:
            out[rel] = set(os.listdir(live_root / rel))
        except OSError:
            out[rel] = set()
    return out


def _names(path: Path, needle: str) -> bool:
    """Whether the file, or any file in the directory, contains ``needle``."""

    candidates = [path]
    if path.is_dir():
        candidates = [p for p in path.rglob("*") if p.is_file()]
    for candidate in candidates:
        try:
            if needle in candidate.read_text(encoding="utf-8", errors="replace"):
                return True
        except OSError:
            continue
    return False


def leaked_entries(
    before: dict[str, set[str]],
    after: dict[str, set[str]],
    *,
    live_root: Path,
    basetemp: str,
) -> tuple[list[str], list[str]]:
    """New entries that name this session's basetemp, and new entries that do not.

    Args:
        before: ``listing()`` taken before the session.
        after: ``listing()`` taken after it.
        live_root: The store the listings were taken from.
        basetemp: This session's pytest base temporary directory, as a string.

    Returns:
        ``(leaked, unattributed)``, each a list of paths relative to the store.
    """

    leaked: list[str] = []
    unattributed: list[str] = []
    for rel, names in after.items():
        for name in sorted(names - before.get(rel, set())):
            entry = f"{rel}/{name}"
            if _names(live_root / rel / name, basetemp):
                leaked.append(entry)
            else:
                unattributed.append(entry)
    return leaked, unattributed


def pytest_sessionstart(session: pytest.Session) -> None:
    session.config._pb_live_before = (  # type: ignore[attr-defined]
        listing() if LIVE_ROOT.is_dir() else None
    )


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    before = getattr(session.config, "_pb_live_before", None)
    if before is None:
        return
    factory = getattr(session.config, "_tmp_path_factory", None)
    if factory is None:
        return
    basetemp = str(factory.getbasetemp())
    leaked, unattributed = leaked_entries(
        before, listing(), live_root=LIVE_ROOT, basetemp=basetemp
    )
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    write = reporter.write_line if reporter is not None else print
    if unattributed:
        write(
            f"note: {len(unattributed)} new entries under {LIVE_ROOT} during "
            "this session do not name its basetemp; the fleet may have filed "
            "them: " + ", ".join(unattributed[:5])
        )
    if leaked:
        write(
            f"FAILED: {len(leaked)} entries under {LIVE_ROOT} were written by "
            f"this test session (they name {basetemp}). A test reached the "
            "live store; pass it a root under tmp_path. Entries: "
            + ", ".join(leaked[:10])
        )
        session.exitstatus = 1
