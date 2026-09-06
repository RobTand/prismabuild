"""Keep every test off the fleet's live store, and say so if one gets there.

Several modules default a root to the shared mount: ``pbrun.SH``,
``pbstatus.SHARED_ROOT``, ``pool_reset.SH``, ``fleet_submit.SH``,
``pool.DEFAULT_POOL_ROOT``, and the lane's ``PRISMABUILD_SLURM_LANE_ROOT`` and
``PRISMABUILD_SLURM_JOB_STATE_ROOT``. A test that forgets to pass a root then
reads or writes the live queue, CAS, or lane root. One more root is box-local
rather than shared, and leaks the same way: ``materialize.LOCAL_CHECKOUT_ROOT``
puts a materialized checkout under ``/home/rob/tmp/prismabuild-checkouts``,
which is a real tree the fleet's own workers use. Between 2026-09-04 and
2026-09-05 the lane tests filed 336 terminal records, 145 CAS requests, and 6
receipts into the live store that way, because their fixture never overrode
``pbrun.SH``. Those files were moved to ``quarantine/pytest-leak-2026-09-05``
on the mount.

Two guards, because neither is complete on its own:

*   ``_off_the_live_store`` repoints every such default at the test's own
    ``tmp_path`` before each test. It cannot reach a default bound at function
    definition, such as ``PoolQueue(root=DEFAULT_POOL_ROOT)``, so a test that
    calls one of those without a root still gets the live path.
*   ``pytest_sessionfinish`` walks the live store before the session and after
    it, and fails the session when a new entry names this session's
    ``basetemp``. A new entry that does not is reported but not counted: the
    fleet may file real work while the suite runs.
"""
from __future__ import annotations

import os
import subprocess
from collections.abc import Callable
from pathlib import Path, PurePosixPath
import sys

import pytest

#: The mount the fleet executes against. The environment override exists so
#: the guard itself can be exercised against a scratch store.
LIVE_ROOT = Path(
    os.environ.get("PRISMABUILD_TEST_LIVE_ROOT") or "/mnt/shared/prismabuild-fleet"
)

#: How long the session guard will wait to find out whether ``LIVE_ROOT`` is
#: there. The mount is ``hard`` with ``timeo=600``, so when the NFS server is
#: down a plain ``is_dir()`` on it does not return, ever: measured on
#: 2026-09-05, ``Path("/mnt/shared/prismabuild-fleet").is_dir()`` had not
#: answered after 15 s and the xdist workers sat in ``rpc_wait_bit_killable``
#: for over 330 s. The guard is a convenience and the suite is not, so an
#: unreachable store costs this many seconds and then the guard stands down.
LIVE_PROBE_TIMEOUT_S = float(
    os.environ.get("PRISMABUILD_TEST_LIVE_PROBE_TIMEOUT_S") or "10"
)

#: Top-level entries of ``LIVE_ROOT`` the guard leaves alone. The quarantine
#: holds records already moved out of the fleet's way, so a new entry there is
#: housekeeping and not a leak.
UNWATCHED = frozenset({"quarantine"})

#: The store's top-level entries, so the guard's own tests have a store to
#: build and a reader can see the coverage without the mount. ``listing`` does
#: not depend on this list being complete: it walks every top-level entry the
#: store has, so a directory added to the store later is watched on sight.
WATCHED = ("cas", "checkout", "pb-queue", "repo", "runtime-generations", "slurm")

#: Module attributes that default to the live store, and the subpath under the
#: test's guard root each is repointed at. Applied only to modules already
#: imported; the tests import these through their own ``sys.path`` inserts.
LIVE_DEFAULTS = (
    ("pbrun", "SH", "fleet"),
    ("pbstatus", "SHARED_ROOT", "fleet"),
    ("pbsweep", "SH", "fleet"),
    ("pbstatus", "DEFAULT_QUEUE_ROOT", "fleet/pb-queue"),
    ("pool_reset", "SH", "fleet"),
    ("fleet_submit", "SH", "fleet"),
    ("worker_loop", "SH", "fleet"),
    ("worker_loop", "RUNTIME_VERSION", "fleet/repo/RUNTIME_VERSION.json"),
    ("worker", "SH", "fleet"),
    # ``supervise.MIRROR`` was the gap this list was completed to close.
    # ``_proven_roots`` lists ``MIRROR / "runtime-generations"``, so
    # ``test_only_idle_loops_are_stopped`` read the live store on every run of
    # the suite, on every box. It passed, which is why nothing noticed: the
    # cost was a test that depended on the fleet's state and a suite that hung
    # for as long as the mount was unreachable.
    ("supervise", "MIRROR", "fleet"),
    # The mount probe times real syscalls against whatever this names, and
    # creates a directory under it.  Left pointed at the live store, the suite
    # would write to the fleet's mount on every run and block on it whenever
    # it was the thing being diagnosed.
    ("mount_latency", "DEFAULT_MOUNT", "fleet"),
    ("publish_runtime", "MIRROR", "fleet/repo"),
    ("seal_and_publish", "SH", "fleet"),
    ("pbtest", "SHARED", "mount"),
    ("tessera_status", "SH", "fleet"),
    ("tessera_status", "CAS", "fleet/cas"),
    ("tessera_status", "Q", "fleet/pb-queue"),
    ("tessera_status", "RES", "fleet/checkout/results/glm53-tessera"),
    ("tessera_status", "PARTS", "mount/models/parts"),
    ("dispatch_tessera_ladder", "SH", "fleet"),
    ("dispatch_tessera_model", "PUBLISHED_TOOLS", "fleet/repo/tools"),
    ("dispatch_tessera_ladder", "CHECKOUT", "fleet/checkout"),
    ("dispatch_tessera_ladder", "SOURCE", "mount/models/source"),
    ("dispatch_tessera_shards", "SH", "fleet"),
    ("dispatch_tessera_shards", "CHECKOUT", "fleet/checkout"),
    ("dispatch_tessera_shards", "SOURCE", "mount/models/source"),
    ("dispatch_tessera_shards", "PLAN", "mount/plan.json"),
    ("dispatch_tessera_shards", "PARTS", "mount/models/parts"),
    ("render_identity", "MODEL", "mount/models/render"),
    ("prismabuild.pool", "DEFAULT_POOL_ROOT", "pb-queue"),
    # Two spellings of one root, and each transport reads its own: the SLURM
    # job entry reads ``materialize.LOCAL_CHECKOUT_ROOT`` and the pull queue
    # reads ``pool.LOCAL_CHECKOUT_ROOT``, which is a copy taken at import.
    ("prismabuild.materialize", "LOCAL_CHECKOUT_ROOT", "checkouts"),
    ("prismabuild.pool", "LOCAL_CHECKOUT_ROOT", "checkouts"),
    # Box-local, like the checkout root above, and it leaked the same way: the
    # admission identity is keyed on the queue root, so a suite whose every
    # test builds a queue under a fresh ``tmp_path`` minted a permanent lock
    # file per test into the directory the fleet's own loops use.  Both
    # spellings of the one directory move together -- ``mount_latency`` reads
    # what ``adaptive_cpu`` writes, and a guard that moved only one would have
    # the probe measuring a directory nothing writes to.
    ("prismabuild.adaptive_cpu", "BOX_STATE_ROOT", "box-state"),
    ("mount_latency", "ADMISSION_LOCK_DIR", "box-state"),
)

#: Environment variables the lane and the pool read on use.
LIVE_ENV = (
    ("PRISMABUILD_SLURM_LANE_ROOT", "slurm"),
    ("PRISMABUILD_SLURM_JOB_STATE_ROOT", "slurm/jobs"),
    ("PRISMABUILD_POOL_ROOT", "pb-queue"),
    # Read at import, so this reaches a module imported after the fixture ran
    # and a child process such as ``slurm_job``; the attributes above reach
    # the modules already imported.
    ("PRISMABUILD_LOCAL_CHECKOUT_ROOT", "checkouts"),
)


@pytest.fixture(autouse=True)
def _off_the_live_store(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    root = tmp_path / "live-guard"
    for name, sub in LIVE_ENV:
        monkeypatch.setenv(name, str(root / sub))
    for module_name, attr, sub in LIVE_DEFAULTS:
        module = sys.modules.get(module_name)
        if module is None or not hasattr(module, attr):
            continue
        # Keep the declared type. Several of these are plain strings, and
        # handing a module a ``Path`` where it declared a ``str`` changes
        # behaviour the test was not asking about.
        replacement = root / sub
        if isinstance(getattr(module, attr), str):
            replacement = str(replacement)
        monkeypatch.setattr(module, attr, replacement)
    yield


def _top_level(live_root: Path) -> set[str]:
    """The store's own entries, empty for a store that is not mounted."""

    try:
        return {name for name in os.listdir(live_root) if name not in UNWATCHED}
    except OSError:
        return set()


def _walk(root: Path) -> set[str]:
    """Every path under ``root``, relative to it, empty for a missing one.

    ``os.walk`` rather than ``Path.rglob``: a stale NFS handle or a directory
    this user cannot read has to leave the rest of the listing intact, and a
    guard that raises out of ``pytest_sessionfinish`` fails a suite over a
    permission it never needed.

    ``os.walk`` does not descend into a symlink it meets during the walk, but
    it does follow ``root`` itself. The live ``repo`` link is a root here, so
    the generation it points at is listed twice, once under ``repo`` and once
    under ``runtime-generations``. That costs a second walk and reports a leak
    into the live runtime under both names, which is the direction to err in.
    """

    found: set[str] = set()
    for directory, subdirectories, files in os.walk(root, onerror=lambda _e: None):
        base = Path(directory).relative_to(root)
        for name in (*subdirectories, *files):
            found.add((base / name).as_posix())
    return found


def listing(live_root: Path = LIVE_ROOT) -> dict[str, set[str]]:
    """Every path in the store, keyed by the top-level entry that holds it.

    Recursive, and over the whole store rather than a list of queue
    directories, because both of the September leak's halves were invisible
    to a shallower guard. A CAS request is
    ``cas/requests/<shard>/<digest>.json``, so listing ``cas`` alone reports
    the four directory names that were there before; the 145 requests and 6
    receipts filed that day named this session's ``basetemp`` inside the JSON,
    which ``leaked_entries`` reads, but nothing offered it the paths.

    The ``""`` key holds the top-level names, so an entry written into the
    store itself is a new entry too.
    """

    root = Path(live_root)
    out: dict[str, set[str]] = {"": _top_level(root)}
    for name in sorted(out[""] | set(WATCHED)):
        out[name] = _walk(root / name)
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
        fresh = names - before.get(rel, set())
        # A new directory and everything inside it are one leak. Reporting the
        # directory and dropping its children keeps a leaked checkout to one
        # line instead of several thousand; ``_names`` reads the whole subtree
        # either way.
        outermost = sorted(
            name for name in fresh
            if not any(
                parent.as_posix() in fresh
                for parent in PurePosixPath(name).parents
            )
        )
        for name in outermost:
            entry = f"{rel}/{name}" if rel else name
            if _names(live_root / rel / name, basetemp):
                leaked.append(entry)
            else:
                unattributed.append(entry)
    return leaked, unattributed


def _probe_argv(root: Path) -> list[str]:
    return [
        sys.executable, "-c",
        "import os,sys; sys.exit(0 if os.path.isdir(sys.argv[1]) else 1)",
        str(root),
    ]


def reachable(
    root: Path,
    timeout_s: float = LIVE_PROBE_TIMEOUT_S,
    argv: Callable[[Path], list[str]] = _probe_argv,
) -> bool:
    """Whether ``root`` is a readable directory, answered within ``timeout_s``.

    The question is asked in a child process rather than in this one, because
    the answer can never arrive. A read of an unreachable ``hard`` NFS mount
    is uninterruptible: no timeout, no signal and no thread cancellation ends
    it, and whichever process asked is stuck until the server returns. Asking
    in a child means the stuck process is one this session can walk away from,
    and pytest still runs and still reports.

    The child is killed on timeout and deliberately not waited for. A process
    in uninterruptible sleep does not die on SIGKILL either; it is reaped when
    the mount comes back. Waiting for it here would move the hang back into
    the session, which is the whole thing being avoided.
    """

    try:
        probe = subprocess.Popen(
            argv(root), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except OSError:
        return False
    try:
        return probe.wait(timeout=timeout_s) == 0
    except subprocess.TimeoutExpired:
        probe.kill()
        return False


def pytest_sessionstart(session: pytest.Session) -> None:
    available = reachable(LIVE_ROOT)
    session.config._pb_live_reachable = available  # type: ignore[attr-defined]
    session.config._pb_live_before = listing() if available else None  # type: ignore[attr-defined]


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    before = getattr(session.config, "_pb_live_before", None)
    if before is None:
        # Absent and unreachable are different, and the guard used to report
        # neither. A box without the mount is expected and silent; a box whose
        # mount did not answer means the suite ran unguarded, and the operator
        # has to be told which of the two happened.
        if getattr(session.config, "_pb_live_reachable", True) is False and LIVE_ROOT.parent.exists():
            reporter = session.config.pluginmanager.get_plugin("terminalreporter")
            write = reporter.write_line if reporter is not None else print
            write(
                f"note: {LIVE_ROOT} did not answer within "
                f"{LIVE_PROBE_TIMEOUT_S:g}s, so the live-store leak guard did "
                "not run this session. Re-run it when the mount is back."
            )
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
