"""Keep every test off the fleet's live store, and refuse it at the call.

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

Two guards run on every test, and a third is opt-in:

*   ``_off_the_live_store`` repoints every such default at the test's own
    ``tmp_path`` before each test. It cannot reach a default bound at function
    definition, such as ``PoolQueue(root=DEFAULT_POOL_ROOT)``, so a test that
    calls one of those without a root still gets the live path.
*   An audit hook (``sys.addaudithook``, installed in ``pytest_configure``)
    refuses the call itself. ``open``, ``os.listdir``, ``os.scandir``, the
    ``os`` calls that create, rename, remove, link or change an entry, and
    ``shutil.rmtree`` raise ``RuntimeError`` naming the event and the path when
    that path lies under ``LIVE_ROOT``. The comparison is lexical and costs no
    filesystem lookup, so the guard adds no NFS traffic of its own. The failing
    test is the one that made the call, and its traceback names the path and
    the line. A test that must read the store says so with
    ``@pytest.mark.live_store(reason=...)``. What the hook cannot see:
    ``os.stat`` and ``os.access`` (CPython raises no audit event for them), and
    child processes, which start without the hook.
*   The census. ``pytest_sessionstart`` and ``pytest_sessionfinish`` walk the
    whole store in an abandonable reader and fail the session when complete
    before/after observations find a new entry naming this session's
    ``basetemp``. It is off unless ``PRISMABUILD_TEST_LIVE_CENSUS=1`` asks for
    it, or ``PRISMABUILD_TEST_LIVE_ROOT`` points the guard at a scratch store.
    Every pbtest shard used to run it twice, and a recursive walk of the store
    over NFS is a lookup storm: on 2026-09-23 five concurrent shards made
    about 16,000 lookups each in 10 s, 29,345 lookups/s in all, against the
    nfsd that also carries the campaign's data (#1019). The #643 fix had raised
    the census budget to 180 s rather than shrink the walk. The hook refuses
    what the census could only report afterwards, so the census is kept as an
    audit, not as the per-session gate. A new entry that does not name the
    basetemp is reported but not counted, because the fleet may file real work
    while the suite runs. An unavailable or partial observation says so; it is
    never treated as a clean leak check.
"""
from __future__ import annotations

import os
import math
import stat
import subprocess
from collections.abc import Callable
from pathlib import Path, PurePosixPath
import sys

import pytest

# The status reader already owns the fleet's bounded fork-and-abandon contract.
# Reuse it here rather than adding a second timeout shape for a hard-mounted
# store. The root conftest adds ``src``; this test-only hook also needs the
# script directory because ``pbstatus`` is intentionally not a package module.
_FLEET_TOOLS = Path(__file__).resolve().parents[1] / "tools" / "fleet"
if str(_FLEET_TOOLS) not in sys.path:
    sys.path.insert(0, str(_FLEET_TOOLS))
import pbstatus  # noqa: E402
import prismabuild.core as pb_core  # noqa: E402

#: The mount the fleet executes against. The environment override exists so
#: the guard itself can be exercised against a scratch store.
LIVE_ROOT = Path(
    os.environ.get("PRISMABUILD_TEST_LIVE_ROOT") or "/mnt/shared/prismabuild-fleet"
)

#: Whether this session walks the store before and after (see the module
#: docstring). A scratch ``PRISMABUILD_TEST_LIVE_ROOT`` turns it on because
#: that is how the guard's own tests exercise it.
LIVE_CENSUS = (os.environ.get("PRISMABUILD_TEST_LIVE_CENSUS") == "1"
               or bool(os.environ.get("PRISMABUILD_TEST_LIVE_ROOT")))

#: The fleet's own store, guarded even when ``PRISMABUILD_TEST_LIVE_ROOT``
#: points ``LIVE_ROOT`` at a scratch store.
DEFAULT_LIVE_ROOT = "/mnt/shared/prismabuild-fleet"


def _guarded_roots(*roots: str | Path) -> tuple[tuple[str, str], ...]:
    """Each root as ``(path, path + "/")``, absolute and normalized, not resolved."""

    out: list[tuple[str, str]] = []
    for root in roots:
        text = os.path.normpath(os.path.abspath(os.fspath(root)))
        if (text, text + "/") not in out:
            out.append((text, text + "/"))
    return tuple(out)


#: What the audit hook refuses. Tests of the hook repoint it at a scratch store.
GUARDED = _guarded_roots(DEFAULT_LIVE_ROOT, LIVE_ROOT)

#: Audited events whose arguments name a path, as ``(path index, dir_fd
#: index)`` pairs; ``None`` where the event carries no ``dir_fd``. CPython
#: audits ``os.replace`` as ``os.rename`` and ``os.unlink`` as ``os.remove``;
#: both spellings are listed so neither depends on that. ``os.symlink`` is
#: checked at the link it creates, not at the target it names, because making
#: a link reads nothing through it.
AUDITED_PATHS: dict[str, tuple[tuple[int, int | None], ...]] = {
    "open": ((0, None),),
    "os.listdir": ((0, None),),
    "os.scandir": ((0, None),),
    "os.mkdir": ((0, 2),),
    "os.rename": ((0, 2), (1, 3)),
    "os.replace": ((0, 2), (1, 3)),
    "os.link": ((0, 2), (1, 3)),
    "os.remove": ((0, 1),),
    "os.unlink": ((0, 1),),
    "os.rmdir": ((0, 1),),
    "os.symlink": ((1, 2),),
    "os.chmod": ((0, 2),),
    "os.truncate": ((0, None),),
    "os.utime": ((0, 3),),
    "shutil.rmtree": ((0, 1),),
    # ``PrismaBuildCAS`` and the SLURM lane open directories by descending
    # from ``/`` one dir_fd-relative component at a time, so the ``open``
    # events of that walk name only ``mnt``, ``shared`` and so on. They
    # announce the absolute directory in this event first.
    pb_core.NOFOLLOW_DIRECTORY_AUDIT_EVENT: ((0, None),),
}

#: ``dir_fd`` values that mean "relative to the working directory".
_CWD_DIR_FDS = (None, -1, getattr(os, "AT_FDCWD", -100))

#: How many callers currently let the hook through: the census, and a test
#: marked ``live_store``.
_live_access_depth = 0

#: Every refusal this process made, so a test that catches the ``RuntimeError``
#: still fails.
REFUSALS: list[str] = []


class _LiveAccess:
    """Let the hook through for the duration of a ``with`` block."""

    def __enter__(self) -> None:
        global _live_access_depth
        _live_access_depth += 1

    def __exit__(self, *_exc: object) -> None:
        global _live_access_depth
        _live_access_depth -= 1


def _live_store_audit(event: str, args: tuple) -> None:
    """Refuse a call whose path lies under a guarded root.

    One dict lookup for every event outside ``AUDITED_PATHS``, and string
    work only for the rest: no ``stat``, no ``realpath``, nothing that would
    itself look a name up on the mount. A relative path is absolutized
    against ``os.getcwd()`` (the kernel answers that from its own dentry,
    without an NFS lookup). A path relative to a ``dir_fd`` cannot be placed
    lexically and is let through here: it is caught where its directory FD
    was opened, which was an audited ``open`` of an absolute path or a
    no-follow descent that raised ``NOFOLLOW_DIRECTORY_AUDIT_EVENT``. The
    ``open`` event carries no ``dir_fd``, so an ``os.open(name, dir_fd=fd)``
    is placed against the working directory; that can only err towards a
    refusal, and only when the working directory is itself in the store.
    """

    spec = AUDITED_PATHS.get(event)
    if spec is None or _live_access_depth:
        return
    for path_index, dir_fd_index in spec:
        if path_index >= len(args):
            continue
        path = args[path_index]
        if path is None or isinstance(path, int):
            # A file descriptor: it was opened by a call this hook saw.
            continue
        try:
            text = os.fsdecode(path)
        except TypeError:
            continue
        if not text.startswith("/"):
            if (dir_fd_index is not None and dir_fd_index < len(args)
                    and args[dir_fd_index] not in _CWD_DIR_FDS):
                continue
            try:
                text = os.getcwd() + "/" + text
            except OSError:
                continue
        text = os.path.normpath("/" + text.lstrip("/"))
        for root, prefix in GUARDED:
            if text == root or text.startswith(prefix):
                message = (
                    f"live-store guard: {event} on {text} refused: it lies under "
                    f"{root}, the fleet's live store. Give the test a root under "
                    "tmp_path, or mark it @pytest.mark.live_store(reason=...) if "
                    "it must read the store (#1019)."
                )
                REFUSALS.append(message)
                raise RuntimeError(message)


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "live_store(reason): the test reads the fleet's live store on purpose; "
        "the call-time live-store guard lets it through (#1019)",
    )
    # An audit hook cannot be removed, so install it once per process even if
    # this module is configured again (a nested in-process pytest run).
    if not getattr(sys, "_prismabuild_live_store_hook", False):
        sys.addaudithook(_live_store_audit)
        sys._prismabuild_live_store_hook = True  # type: ignore[attr-defined]


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item: pytest.Item, nextitem: pytest.Item | None):
    if item.get_closest_marker("live_store") is None:
        yield
        return
    with _LiveAccess():
        yield


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    outcome = yield
    if outcome.get_result().failed:
        item._pb_live_guard_failed = True  # type: ignore[attr-defined]


@pytest.fixture(autouse=True)
def _live_store_refusals(request: pytest.FixtureRequest):
    """Fail a test whose refused call was swallowed by its own ``except``.

    The refusal is an exception, so code under test that catches broadly
    (``except Exception``) would otherwise turn it into a pass.
    """

    del REFUSALS[:]
    yield
    refused = list(REFUSALS)
    del REFUSALS[:]
    if refused and not getattr(request.node, "_pb_live_guard_failed", False):
        pytest.fail(
            f"{len(refused)} call(s) into the live store were refused and the "
            "refusal was caught: " + "; ".join(refused[:3]),
            pytrace=False,
        )


#: How long the session guard will wait to find out whether ``LIVE_ROOT`` is
#: there. The mount is ``hard`` with ``timeo=600``, so when the NFS server is
#: down a plain ``is_dir()`` on it does not return, ever: measured on
#: 2026-09-05, ``Path("/mnt/shared/prismabuild-fleet").is_dir()`` had not
#: answered after 15 s and the xdist workers sat in ``rpc_wait_bit_killable``
#: for over 330 s. The guard is a convenience and the suite is not, so an
#: unreachable store costs this many seconds and then the guard stands down.
def _positive_timeout(value: object, *, name: str) -> float:
    """Reject a timeout spelling that would select pbstatus's unbounded path."""

    timeout_s = float(value)
    if not math.isfinite(timeout_s) or timeout_s <= 0:
        raise ValueError(f"{name} must be a finite positive number of seconds")
    return timeout_s


LIVE_PROBE_TIMEOUT_S = _positive_timeout(
    os.environ.get("PRISMABUILD_TEST_LIVE_PROBE_TIMEOUT_S") or "10",
    name="PRISMABUILD_TEST_LIVE_PROBE_TIMEOUT_S",
)

#: How long the session guard waits for a full live-store census.  A separate
#: budget from the probe above, because they answer different questions: the
#: probe asks "is the mount there" and the census walks every entry of every
#: top-level directory, which never fitted in the probe's 10 s -- on dl380g10,
#: where the fleet's shared mount lives, the start census timed out on nearly
#: every pbtest shard (#643).  Eight shards of this branch's validation ran on
#: dl380g10 with this budget and none emitted the timeout note, so the walk
#: fits in it there.  The census runs in an abandonable child, so an
#: unreachable store still costs only the probe above; this budget sizes the
#: walk to the store.
LIVE_CENSUS_TIMEOUT_S = _positive_timeout(
    os.environ.get("PRISMABUILD_TEST_LIVE_CENSUS_TIMEOUT_S") or "180",
    name="PRISMABUILD_TEST_LIVE_CENSUS_TIMEOUT_S",
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
    ("fleet_membership", "DEFAULT_QUEUE_ROOT", "fleet/pb-queue"),
    ("pool_reset", "SH", "fleet"),
    ("fleet_submit", "SH", "fleet"),
    ("worker_loop", "SH", "fleet"),
    ("worker_loop", "PUBLICATION_LOCK_ROOT", "offer-publication"),
    # Role singleton locks are host-local and per-uid (#709): left pointed at
    # the live namespace, a routine ``ensure_roles`` or role-entrypoint test
    # would contend with the box's own running role or file stray lock
    # directories into /tmp.
    ("worker_loop", "ROLE_LOCK_ROOT", "role-locks"),
    ("prewarm_loop", "SH", "fleet"),
    ("worker_loop", "RUNTIME_VERSION", "fleet/repo/RUNTIME_VERSION.json"),
    ("worker", "SH", "fleet"),
    # ``supervise.MIRROR`` was the gap this list was completed to close.
    # ``_proven_roots`` lists ``MIRROR / "runtime-generations"``, so
    # ``test_only_idle_loops_are_stopped`` read the live store on every run of
    # the suite, on every box. It passed, which is why nothing noticed: the
    # cost was a test that depended on the fleet's state and a suite that hung
    # for as long as the mount was unreachable.
    ("supervise", "MIRROR", "fleet"),
    ("supervise", "SYSTEMD_UNIT", "systemd/prismabuild-supervisor.service"),
    # The mount probe times real syscalls against whatever this names, and
    # creates a directory under it.  Left pointed at the live store, the suite
    # would write to the fleet's mount on every run and block on it whenever
    # it was the thing being diagnosed.
    ("mount_latency", "DEFAULT_MOUNT", "fleet"),
    ("publish_runtime", "MIRROR", "fleet/repo"),
    # Crew-A canary driver (issue #688): both defaults name the live fleet
    # store, so a test importing pbcanary without repointing would submit
    # through or read the fleet's own roots.
    ("pbcanary", "DEFAULT_PUBLISHED_ROOT", "fleet/repo"),
    ("pbcanary", "DEFAULT_FLEET_ROOT", "fleet"),
    ("qualify_rollout", "QUALIFICATION_ROOT", "qualification"),
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
    ("prismabuild.resource_scope", "RETAINED_GENERATION_STORE",
     "fleet/runtime-generations"),
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
    # Host-local and persistent beside the checkouts; a suite that wrote here
    # would leave a file per test in the directory the fleet's loops read.
    ("prismabuild.local_scratch", "OFFER_ROOT", "spool-offer"),
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
    # Same reason, and the same directory as the two BOX_STATE_ROOT
    # attributes below: a child worker imports ``adaptive_cpu`` fresh and
    # would otherwise sweep-mark the fleet's own directory.
    ("PRISMABUILD_BOX_STATE_ROOT", "box-state"),
    ("PRISMABUILD_SPOOL_OFFER_ROOT", "spool-offer"),
)


@pytest.fixture(autouse=True)
def _off_the_live_store(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    root = tmp_path / "live-guard"
    root.mkdir(exist_ok=True)
    # Tests also load fleet tools under private module names after this
    # fixture starts (``spec_from_file_location("wl_census", WORKER_LOOP)``).
    # Repoint such a module's live defaults as it is loaded, matched by file
    # name. Before #1019 only worker_loop's two lock roots were covered, and
    # 31 tests read the fleet's live ``repo/RUNTIME_VERSION.json`` through a
    # privately loaded worker_loop; the call-time guard found them.
    import importlib.machinery
    load = importlib.machinery.SourceFileLoader.exec_module
    def isolated_import(loader, module):
        load(loader, module)
        stem = Path(getattr(module, "__file__", "") or "").stem
        for module_name, attr, sub in LIVE_DEFAULTS:
            if module_name.rsplit(".", 1)[-1] != stem or not hasattr(module, attr):
                continue
            replacement = root / sub
            if isinstance(getattr(module, attr), str):
                replacement = str(replacement)
            setattr(module, attr, replacement)
    monkeypatch.setattr(importlib.machinery.SourceFileLoader, "exec_module",
                        isolated_import)
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


#: The launch context the pool publishes to an admitted attempt: the action
#: key, the residency map and the queue root (#961), plus the broker-owned
#: reader tuple ``resource_exec`` injects. Taken from the module that defines
#: it so the two spellings cannot drift.
_LAUNCH_CONTEXT_ENV = pb_core.ACTION_RESIDENCY_ENV


@pytest.fixture(autouse=True)
def _standalone_action_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    """A synthetic action runs as itself, not as the action that admitted it.

    The suite itself runs as an admitted PrismaBuild action, so the outer
    attempt's launch context is in the environment of every test.
    ``PoolQueue.execute`` launches its nested worker with this process's own
    environment, and ``core._reader_identity_environment`` refuses to forward
    an outer identity to an unrelated synthetic action -- correctly, because
    that check is what stops a nested action borrowing the attempt it was
    launched under (#786). The queue root is the same kind of name: since #961
    the launcher publishes ``PRISMABUILD_QUEUE_ROOT``, which under PB is the
    fleet's live queue, and ``reader_lease.launch_queue_root()`` reads
    ``os.environ`` by default. None of that is what these tests are about, so
    they run the way a standalone box runs them: no ambient launch context at
    all, with the production checks left strict.

    Autouse and conftest-wide because the ambient context reaches every test,
    not only the ones that know about it. A test that does mean to state
    something about launch identity sets the names itself, after this
    fixture, and ``tests/test_reader_launch_identity.py`` scrubs them first in
    any case.
    """

    for name in _LAUNCH_CONTEXT_ENV:
        monkeypatch.delenv(name, raising=False)


def _top_level(live_root: Path) -> tuple[set[str], bool, list[str]]:
    """The store's own entries, empty for a store that is not mounted."""

    try:
        return ({name for name in os.listdir(live_root) if name not in UNWATCHED},
                True, [])
    except OSError as exc:
        return set(), False, [f"{live_root}: {type(exc).__name__}: {exc}"]


def _walk(root: Path) -> tuple[set[str], bool, list[str]]:
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

    try:
        mode = os.stat(root).st_mode
    except FileNotFoundError:
        # A known store component may legitimately not exist yet. It was not
        # listed at the root, so this is an empty complete branch, not an I/O
        # failure hidden as a clean census.
        return set(), True, []
    except OSError as exc:
        return set(), False, [f"{root}: {type(exc).__name__}: {exc}"]
    if not stat.S_ISDIR(mode):
        # ``listing`` walks newly discovered root entries too. Ordinary files
        # are entries to compare, not directories whose lack of children makes
        # the whole census partial.
        return set(), True, []

    found: set[str] = set()
    errors: list[str] = []

    def record_error(exc: OSError) -> None:
        if isinstance(exc, FileNotFoundError):
            # Renamed under us between the parent listing and the descent.
            # The fleet renames reservation files constantly -- a ``claiming``
            # record that disappears mid-walk is ordinary churn, and a path
            # that no longer exists cannot be a persistent leak, so it is
            # dropped rather than reported as a partial census (#643).
            return
        errors.append(f"{root}: {type(exc).__name__}: {exc}")

    for directory, subdirectories, files in os.walk(root, onerror=record_error):
        base = Path(directory).relative_to(root)
        for name in (*subdirectories, *files):
            found.add((base / name).as_posix())
    return found, not errors, errors


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

    return _census(live_root)["listing"]


def _census(live_root: Path) -> dict:
    """Read the full inventory and retain whether every directory answered."""

    root = Path(live_root)
    with _LiveAccess():
        top_level, complete, errors = _top_level(root)
        out: dict[str, set[str]] = {"": top_level}
        for name in sorted(top_level | set(WATCHED)):
            found, walked, walk_errors = _walk(root / name)
            out[name] = found
            complete = complete and walked
            errors.extend(walk_errors)
    return {"listing": out, "complete": complete, "errors": errors}


def _census_payload(live_root: Path) -> dict:
    """Make the set-based inventory safe to carry over ``pbstatus.bounded``."""

    census = _census(live_root)
    return {
        "complete": census["complete"],
        "errors": census["errors"],
        "listing": {name: sorted(entries)
                    for name, entries in census["listing"].items()},
    }


def bounded_listing(live_root: Path = LIVE_ROOT,
                    timeout_s: float = LIVE_CENSUS_TIMEOUT_S) -> dict:
    """Return a complete live-store census, or explicit incomplete evidence.

    The entire recursive traversal runs in ``pbstatus.bounded``.  A hard NFS
    read can stall after the root probe succeeds, so placing only ``reachable``
    in an abandonable child does not protect this test session.

    This bounds the pytest parent rather than promising that every cleanup is
    bounded: a reader stuck in uninterruptible I/O can survive SIGKILL, remain
    in the admitted PrismaBuild action's scope, and delay its final cleanup or
    receipt until the mount recovers. Its PID/start-time identity is retained
    as evidence instead of being mistaken for a completed read.
    """

    timeout_s = _positive_timeout(timeout_s, name="live-store census timeout")
    abandoned: list[dict] = []
    observed = pbstatus.bounded(
        "pytest live-store census", lambda: _census_payload(Path(live_root)),
        deadline=pbstatus.Deadline(timeout_s), abandoned=abandoned,
    )
    if observed["status"] != "ok":
        return {
            "status": "unavailable", "reason": observed["status"],
            "detail": observed.get("error"), "abandoned": abandoned,
        }
    payload = observed["value"]
    if not isinstance(payload, dict) or not isinstance(payload.get("listing"), dict):
        return {
            "status": "unavailable", "reason": "invalid_payload",
            "detail": "the census reader returned no inventory", "abandoned": abandoned,
        }
    try:
        inventory = {str(name): set(entries)
                     for name, entries in payload["listing"].items()}
    except (TypeError, ValueError):
        return {
            "status": "unavailable", "reason": "invalid_payload",
            "detail": "the census reader returned an invalid inventory",
            "abandoned": abandoned,
        }
    if payload.get("complete") is not True:
        return {
            "status": "partial", "reason": "traversal_error",
            "detail": "; ".join(str(error) for error in payload.get("errors", [])),
            "abandoned": abandoned,
        }
    return {"status": "complete", "listing": inventory, "abandoned": abandoned}


def _names(path: Path, needle: str) -> tuple[bool | None, bool, list[str]]:
    """Read a new entry for ``needle`` without hiding an incomplete read.

    The first element is ``None`` when the entry vanished before it could be
    read -- renamed under us the way the walk above tolerates.  Nothing that
    no longer exists can be a persistent leak, so the caller drops it from
    both the leaked and the unattributed lists rather than reporting either
    the fleet's churn or a partial census for it (#643).
    """

    errors: list[str] = []
    try:
        mode = os.stat(path).st_mode
    except FileNotFoundError:
        return None, True, []
    except OSError as exc:
        return False, False, [f"{path}: {type(exc).__name__}: {exc}"]
    candidates = [path]
    if stat.S_ISDIR(mode):
        candidates = []

        def record_error(exc: OSError) -> None:
            if isinstance(exc, FileNotFoundError):
                return
            errors.append(f"{path}: {type(exc).__name__}: {exc}")

        for directory, _subdirectories, files in os.walk(path, onerror=record_error):
            candidates.extend(Path(directory) / name for name in files)
    for candidate in candidates:
        try:
            if needle in candidate.read_text(encoding="utf-8", errors="replace"):
                return True, not errors, errors
        except FileNotFoundError:
            # A directory entry renamed between the walk above and this read
            # is the same churn as a vanished entry: nothing persists.
            continue
        except OSError as exc:
            errors.append(f"{candidate}: {type(exc).__name__}: {exc}")
    return False, not errors, errors


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

    leaked, unattributed, _complete, _errors = _leaked_entries(
        before, after, live_root=live_root, basetemp=basetemp,
    )
    return leaked, unattributed


def _leaked_entries(
    before: dict[str, set[str]],
    after: dict[str, set[str]],
    *,
    live_root: Path,
    basetemp: str,
) -> tuple[list[str], list[str], bool, list[str]]:
    """Like ``leaked_entries``, but retain incomplete attribution evidence."""

    with _LiveAccess():
        return _attribute_entries(before, after, live_root=live_root,
                                  basetemp=basetemp)


def _attribute_entries(
    before: dict[str, set[str]],
    after: dict[str, set[str]],
    *,
    live_root: Path,
    basetemp: str,
) -> tuple[list[str], list[str], bool, list[str]]:
    leaked: list[str] = []
    unattributed: list[str] = []
    complete = True
    errors: list[str] = []
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
            names_basetemp, read_complete, read_errors = _names(
                live_root / rel / name, basetemp
            )
            complete = complete and read_complete
            errors.extend(read_errors)
            if names_basetemp is None:
                # Vanished between the census and the attribution read: the
                # fleet renamed it under us, and nothing that no longer
                # exists is a leak or an unattributed entry (#643).
                continue
            if names_basetemp:
                leaked.append(entry)
            else:
                unattributed.append(entry)
    return leaked, unattributed, complete, errors


def bounded_leaked_entries(
    before: dict[str, set[str]], *, live_root: Path = LIVE_ROOT,
    basetemp: str, timeout_s: float = LIVE_CENSUS_TIMEOUT_S,
) -> dict:
    """Census and attribute new entries in one abandonable reader."""

    timeout_s = _positive_timeout(timeout_s, name="live-store leak-check timeout")
    abandoned: list[dict] = []

    def read() -> dict:
        census = _census(Path(live_root))
        if not census["complete"]:
            return {"complete": False, "errors": census["errors"]}
        leaked, unattributed, complete, errors = _leaked_entries(
            before, census["listing"], live_root=Path(live_root), basetemp=basetemp,
        )
        return {"complete": complete, "errors": errors, "leaked": leaked,
                "unattributed": unattributed}

    observed = pbstatus.bounded(
        "pytest live-store leak check", read,
        deadline=pbstatus.Deadline(timeout_s), abandoned=abandoned,
    )
    if observed["status"] != "ok":
        return {"status": "unavailable", "reason": observed["status"],
                "detail": observed.get("error"), "abandoned": abandoned}
    payload = observed["value"]
    if not isinstance(payload, dict):
        return {"status": "unavailable", "reason": "invalid_payload",
                "detail": "the leak-check reader returned no result", "abandoned": abandoned}
    if payload.get("complete") is not True:
        return {"status": "partial", "reason": "traversal_error",
                "detail": "; ".join(str(error) for error in payload.get("errors", [])),
                "leaked": list(payload.get("leaked", [])),
                "unattributed": list(payload.get("unattributed", [])),
                "abandoned": abandoned}
    return {"status": "complete", "leaked": list(payload.get("leaked", [])),
            "unattributed": list(payload.get("unattributed", [])),
            "abandoned": abandoned}


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
    # xdist workers still receive the per-test root-repointing fixture above.
    # The controller's basetemp is their common parent, so one controller
    # before/after census attributes every worker while avoiding a full live
    # history traversal per worker.
    if hasattr(session.config, "workerinput"):
        session.config._pb_live_guard_worker = True  # type: ignore[attr-defined]
        return
    if not LIVE_CENSUS:
        # Not even the reachability probe: it is a lookup on the mount too.
        session.config._pb_live_census_off = True  # type: ignore[attr-defined]
        return
    available = reachable(LIVE_ROOT)
    session.config._pb_live_reachable = available  # type: ignore[attr-defined]
    observation = (
        bounded_listing(LIVE_ROOT, timeout_s=LIVE_CENSUS_TIMEOUT_S)
        if available else {"status": "unavailable", "reason": "probe_failed"}
    )
    session.config._pb_live_guard_start = observation  # type: ignore[attr-defined]
    session.config._pb_live_before = (  # type: ignore[attr-defined]
        observation.get("listing") if observation["status"] == "complete" else None
    )


def _guard_write(session: pytest.Session, message: str) -> None:
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    write = reporter.write_line if reporter is not None else print
    write(message)


def _guard_evidence(observation: dict) -> str:
    detail = observation.get("detail")
    retained = observation.get("abandoned") or []
    suffix = f"; {detail}" if detail else ""
    if retained:
        suffix += "; retained reader " + ", ".join(
            f"pid={child.get('pid')} starttime={child.get('starttime_ticks')}"
            for child in retained
        )
    return suffix


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if getattr(session.config, "_pb_live_guard_worker", False):
        return
    if getattr(session.config, "_pb_live_census_off", False):
        # Silent by design: a note on every shard is the wallpaper #643 was
        # about, and the call-time hook is the per-session guard.
        return
    before = getattr(session.config, "_pb_live_before", None)
    if before is None:
        observation = getattr(session.config, "_pb_live_guard_start", {
            "status": "unavailable", "reason": "no_start_observation",
        })
        _guard_write(
            session,
            f"note: live-store leak guard start census is {observation['status']} "
            f"({observation.get('reason', 'unknown')}); its census budget was "
            f"{LIVE_CENSUS_TIMEOUT_S:g}s, so it did not certify this session."
            + _guard_evidence(observation),
        )
        return
    factory = getattr(session.config, "_tmp_path_factory", None)
    if factory is None:
        return
    basetemp = str(factory.getbasetemp())
    observation = bounded_leaked_entries(
        before, live_root=LIVE_ROOT, basetemp=basetemp,
        timeout_s=LIVE_CENSUS_TIMEOUT_S,
    )
    if observation["status"] != "complete":
        _guard_write(
            session,
            f"note: live-store leak guard finish census is {observation['status']} "
            f"({observation.get('reason', 'unknown')}); its census budget was "
            f"{LIVE_CENSUS_TIMEOUT_S:g}s, so it did not certify this session."
            + _guard_evidence(observation),
        )
        leaked = observation.get("leaked", [])
        if leaked:
            _guard_write(
                session,
                f"FAILED: {len(leaked)} entries under {LIVE_ROOT} were written by "
                f"this test session despite the partial census (they name {basetemp}). "
                "A test reached the live store; pass it a root under tmp_path. "
                "Entries: " + ", ".join(leaked[:10]),
            )
            session.exitstatus = 1
        return
    leaked = observation["leaked"]
    unattributed = observation["unattributed"]
    if unattributed:
        _guard_write(
            session,
            f"note: {len(unattributed)} new entries under {LIVE_ROOT} during "
            "this session do not name its basetemp; the fleet may have filed "
            "them: " + ", ".join(unattributed[:5])
        )
    if leaked:
        _guard_write(
            session,
            f"FAILED: {len(leaked)} entries under {LIVE_ROOT} were written by "
            f"this test session (they name {basetemp}). A test reached the "
            "live store; pass it a root under tmp_path. Entries: "
            + ", ".join(leaked[:10])
        )
        session.exitstatus = 1


# --------------------------------------------------------------------------
# Decomposition: a stand-in for what pbrun's Stage A freezes
#
# A decomposition parent is keyed on the half of a sealed action that its
# children all share, which ``pbrun.template_action_common`` produces off a
# real checkout.  The batcher, the refusal surface and the merge do not need a
# real tree to be tested against, but they do need that half to be *shaped*
# like the real one, because that is what ``freeze_common`` validates.  So this
# is the miniature: the same six sections, plausible values, and nothing a
# caller cannot vary when the point of the test is that varying it moves the
# parent.
# --------------------------------------------------------------------------

#: Two digests that stand for a source tree and a data manifest.  Named rather
#: than inlined so a test that means "another tree" says so.
DECOMPOSITION_SNAPSHOT = "a" * 64
DECOMPOSITION_MANIFEST = "b" * 64


def action_common(
    *,
    snapshot: str = DECOMPOSITION_SNAPSHOT,
    cwd: str = ".",
    manifest: str | None = None,
    variables: dict[str, str] | None = None,
    **params: object,
) -> dict[str, object]:
    """The sealed half of an action, as a template would hand it over."""

    sealed: dict[str, object] = {
        "cwd": cwd,
        "demand": {"cpu": 1, "mem_gb": 4},
        "placement": {"required_tags": []},
        "retry_policy": {"max_attempts": 1, "retry_safe": False},
        "checkout_snapshot": {
            "input": {"id": "pbrun.checkout-snapshot", "sha256": snapshot,
                      "bytes": 4096},
        },
        **params,
    }
    if manifest is not None:
        sealed["data_manifest"] = {
            "input": {"id": "pbcampaign.data-manifest", "sha256": manifest,
                      "bytes": 512},
            "mount_prefix": "/mnt/shared",
            "entry_count": 1,
            "total_bytes": 512,
        }
    return {
        "task": {
            "definition_id": "fleet/pbrun",
            "definition_version": "v1",
            "task_class": "generation",
            "determinism": "stochastic",
            "artifact_family": "generic",
            "artifact_kind": "generic",
            "working_directory": ".",
        },
        "params": sealed,
        "inputs": [{"id": "pbrun.checkout-snapshot", "sha256": snapshot,
                    "bytes": 4096}],
        "code_closure": {"schema": "prismaquant.prismabuild.code_closure.v1",
                         "files": [], "closure_sha256": "c" * 64},
        "environment": {
            "variables": {"PATH": "/usr/local/bin:/usr/bin:/bin",
                          **(variables or {})},
            "toolchain": {},
        },
        "execution_scope": {"kind": "platform_keyed",
                            "platform_key": "linux-aarch64-sm121"},
    }
