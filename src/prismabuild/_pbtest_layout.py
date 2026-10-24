"""Private pool/worker handoff for the explicit read-only pbtest recipe.

Paths in the launch environment select writable destinations, never a source
verification exemption. The worker borrows its own live source lease and checks
that it names exactly the source selected by the pool. An interpreter adopting
a generation still has to obtain its own core proof; no proof is serialized.

Core integration contract (public run_local_action stays unchanged): enter
``_worker_layout`` for the explicit layout, then pass its ``result_root``,
``environment``, ``preflight`` and ``verify_source`` to the private execution
implementation. Merge ``action_pass_fds`` with the output-lock descriptor in
Popen's pass_fds, so action children independently pin source and scratch.
Execution cwd/closure remain rooted at checkout_root; output
path resolution, containment, locking and recovery claims use result_root.
Both post-exec verification and CAS precommit must call verify_source. Cache
hits must also validate the live source/action, not bypass the lease.
"""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import tempfile
from typing import Any


_LAYOUT_DIRECTORY = "pbtest-layout"


def _constants():
    # movement_actions imports pool; keep this out of core's import bootstrap.
    from . import movement_actions

    return movement_actions


def _layout_directory(path: Path, *, source: Path) -> Path:
    from . import core

    if not path.is_absolute() or ".." in path.parts:
        raise core.ActionContractError("pbtest worker directory must be absolute")
    try:
        resolved = path.resolve(strict=True)
        observed = path.lstat()
    except OSError as exc:
        raise core.ActionContractError("pbtest worker directory is unavailable") from exc
    if (path != resolved or not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.geteuid() or observed.st_mode & 0o022
            or resolved == source or source in resolved.parents):
        raise core.ActionContractError(
            "pbtest worker directory must be private, nonsymlinked and outside source")
    return resolved


def _result_name(action: Mapping[str, Any]) -> Path:
    from . import core

    name = action["task"]["result_path"]
    relative = PurePosixPath(name)
    if (not name or relative.is_absolute() or ".." in relative.parts
            or str(relative) != name or name == "."):
        raise core.ActionContractError("pbtest logical result path must be canonical and relative")
    return Path(*relative.parts)


def _expected_paths(action: Mapping[str, Any], attempt: Path) -> dict[str, Path]:
    movement = _constants()
    base = attempt / _LAYOUT_DIRECTORY
    return {
        movement._RESULT_PATH_ENV: base / "results" / _result_name(action),
        movement._PBTEST_HELPER_DIR_ENV: base / "helpers",
        movement._PBTEST_CACHE_DIR_ENV: base / "pytest-cache",
        movement._PBTEST_SURFACE_DIR_ENV: base / "surfaces",
    }


def _launcher_environment(action: Mapping[str, Any], *, attempt_tmpdir: Path,
                          checkout_root: Path, local_checkout_root: Path) -> dict[str, str]:
    """Create writable paths in a caller-owned attempt directory, before exec.

    Called only after the pool selected and pinned shared source. No checkout
    is copied here and the directory lifetime remains with _attempt_environment.
    """
    from . import core, materialize

    movement = _constants()
    if action["params"].get("execution_layout") != movement._PBTEST_SOURCE_READONLY_LAYOUT:
        raise core.ActionContractError("unsupported pool execution layout")
    source = checkout_root.resolve(strict=True)
    attempt = _layout_directory(attempt_tmpdir.resolve(strict=True), source=source)
    base = attempt / _LAYOUT_DIRECTORY
    base.mkdir(mode=0o700)  # Fresh attempt ownership, not an adoptable marker.
    for name in ("results", "helpers", "pytest-cache", "surfaces", "bytecode"):
        (base / name).mkdir(mode=0o700)
    paths = _expected_paths(action, attempt)
    result = paths[movement._RESULT_PATH_ENV]
    result.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    for path in (base, *[path for key, path in paths.items()
                          if key != movement._RESULT_PATH_ENV], result.parent):
        _layout_directory(path, source=source)
    if result.exists() or result.is_symlink():
        raise core.ActionContractError("pbtest worker result must be absent before launch")
    # The configured host-local root must reach the worker interpreter too:
    # pool tests and deployments may configure it without changing defaults.
    return {
        **{key: str(value) for key, value in paths.items()},
        materialize.LOCAL_CHECKOUT_ROOT_ENV: str(local_checkout_root),
        "TMPDIR": str(attempt),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPYCACHEPREFIX": str(base / "bytecode"),
    }


_ATTEMPT_READERS = ".readers"


@contextmanager
def _attempt_lock(attempt: Path, operation: int, *,
                  expected_reader: int | None = None) -> Iterator[int]:
    """A close-only flock: inherited copies must outlive a killed worker."""
    from . import immutable_files

    directory_fd = immutable_files.open_directory_nofollow(
        attempt, where="pbtest attempt directory")
    try:
        descriptor = os.open(
            _ATTEMPT_READERS, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
            dir_fd=directory_fd)
        try:
            observed = os.fstat(descriptor)
            if (not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1
                    or observed.st_uid != os.geteuid() or observed.st_mode & 0o077):
                raise OSError("pbtest attempt reader lock must be a private regular file")
            if expected_reader is not None:
                expected = os.fstat(expected_reader)
                if (observed.st_dev, observed.st_ino) != (expected.st_dev, expected.st_ino):
                    raise OSError("pbtest attempt reader lock was replaced")
            immutable_files.assert_directory_identity(
                directory_fd, attempt, where="pbtest attempt directory")
            fcntl.flock(descriptor, operation)
            yield descriptor
        finally:
            # Never LOCK_UN: that would also unlock a live child's duplicate.
            os.close(descriptor)
    finally:
        os.close(directory_fd)


class _AttemptEnvironment(dict[str, str]):
    """Launch variables plus the pool's explicit final-outcome acknowledgement."""

    def __init__(self, environment: dict[str, str], *, attempt: Path,
                 directory_descriptor: int, reader_descriptor: int):
        super().__init__(environment)
        self.attempt = attempt
        self._directory_descriptor = directory_descriptor
        self._reader_descriptor = reader_descriptor
        self._outcome: dict[str, object] | None = None

    def record_outcome(self, outcome: dict[str, object]) -> None:
        # Called only after _execute_in_checkout's finally/drain, source
        # revalidation and resource-scope verdict. No acknowledgement means
        # interruption/uncertain completion, even if no Python exception lands.
        self._outcome = outcome

    def _finish(self, *, interrupted: bool) -> None:
        from . import immutable_files

        outcome = self._outcome
        retained, reason = True, "attempt_unsuccessful"
        if (not interrupted and outcome is not None
                and outcome.get("status") == "executed"
                and outcome.get("returncode") == 0
                and outcome.get("action_returncode") in (None, 0)
                and not outcome.get("termination_reason")
                and not outcome.get("action_survived_kill")):
            try:
                # A zero-exit launcher is not proof that inherited readers
                # drained. An exclusive try must succeed before deleting any
                # writable state. Surviving readers leave the attempt intact;
                # there is deliberately no new background reaper here.
                immutable_files.assert_directory_identity(
                    self._directory_descriptor, self.attempt, where="pbtest attempt directory")
                with _attempt_lock(self.attempt, fcntl.LOCK_EX | fcntl.LOCK_NB,
                                   expected_reader=self._reader_descriptor):
                    from . import owned_tree
                    # Pytest may leave read-only directories. Use the same
                    # fd-relative remover as private owned scratch, but only
                    # after this outer attempt's reader/verdict gate.
                    parent = immutable_files.open_directory_nofollow(
                        self.attempt.parent, where="pbtest attempt parent")
                    try:
                        info = os.fstat(self._directory_descriptor)
                        owned_tree.remove_tree(
                            parent, self.attempt.name, expect=(info.st_dev, info.st_ino))
                    finally:
                        os.close(parent)
                retained, reason = False, "success_drained"
            except BlockingIOError:
                reason = "readers_present"
            except (OSError, immutable_files.CASTamperError,
                    immutable_files.CASUnavailableError):
                # Cleanup failure must not reinterpret a published result.
                reason = "cleanup_unavailable"
        if outcome is not None:
            outcome["attempt_scratch"] = {
                "path": str(self.attempt), "retained": retained, "reason": reason}


@contextmanager
def _attempt_environment(action: Mapping[str, Any], *, checkout_root: Path,
                         local_checkout_root: Path) -> Iterator[_AttemptEnvironment]:
    """Retain unsuccessful/interrupted scratch; clean only acknowledged success.

    The pool keeps this context across _execute_in_checkout's termination
    cleanup and publication, then records its final outcome. mkdtemp is
    intentional: no TemporaryDirectory finalizer may erase failure evidence.
    """
    from . import immutable_files

    source = checkout_root.resolve(strict=True)
    parent = local_checkout_root / ".execution"
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    parent = _layout_directory(parent.resolve(strict=True), source=source)
    attempt = Path(tempfile.mkdtemp(prefix="pbtest-", dir=parent))
    directory_fd = immutable_files.open_directory_nofollow(
        attempt, where="pbtest attempt directory")
    try:
        descriptor = os.open(
            _ATTEMPT_READERS,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600, dir_fd=directory_fd)
        try:
            # Keep the original directory and unlocked lock inode alive. An
            # action replacing a pathname must not fool the cleanup try-lock.
            environment = _AttemptEnvironment(_launcher_environment(
                action, attempt_tmpdir=attempt, checkout_root=source,
                local_checkout_root=local_checkout_root.resolve(strict=True)),
                attempt=attempt, directory_descriptor=directory_fd,
                reader_descriptor=descriptor)
            interrupted = True
            try:
                yield environment
                interrupted = False
            finally:
                environment._finish(interrupted=interrupted)
        finally:
            os.close(descriptor)
    finally:
        os.close(directory_fd)


class _WorkerLayout:
    """Live, action-bound inputs to core's private execution implementation."""

    def __init__(self, *, action: Mapping[str, Any], lease: Any,
                 result_root: Path, environment: dict[str, str],
                 worker_launcher_identity: object | None,
                 attempt_reader_descriptor: int):
        self.result_root = result_root
        self.environment = environment
        self._action = action
        self._lease = lease
        self._worker_launcher_identity = worker_launcher_identity
        self._attempt_reader_descriptor = attempt_reader_descriptor
        self._source_reader_descriptor: int | None = None
        self._active = True

    @property
    def action_pass_fds(self) -> tuple[int, int]:
        """Descriptors core must pass to the action alongside its output lock.

        shared_sources' private lease API is duplicate_reader_fd() -> int:
        return an owned, CLOEXEC os.dup of this live reader's locked open-file
        description. The lease and this layout close only their own copies;
        neither may issue LOCK_UN. No source path or environment token is a
        substitute for the inherited kernel hold.
        """
        from . import core

        if not self._active:
            raise core.ActionContractError("pbtest worker layout is no longer live")
        if self._source_reader_descriptor is None:
            duplicate = getattr(self._lease, "duplicate_reader_fd", None)
            if not callable(duplicate):
                raise core.ActionContractError(
                    "shared source lease lacks descendant reader descriptor handoff")
            self._source_reader_descriptor = duplicate()
        return (self._source_reader_descriptor, self._attempt_reader_descriptor)

    def _close(self) -> None:
        self._active = False
        if self._source_reader_descriptor is not None:
            os.close(self._source_reader_descriptor)
            self._source_reader_descriptor = None

    def preflight(self) -> dict[str, object]:
        # shared_sources must forward this identity to core's generation-aware
        # preflight, so the attestation describes the actual launched worker.
        return self._lease.preflight(
            self._action, worker_launcher_identity=self._worker_launcher_identity)

    def verify_source(self) -> None:
        # A live lease's census detects ignored caches and same-size rewrites,
        # not only code-closure changes. Re-attestation does not redo Git proof.
        self.preflight()


@contextmanager
def _worker_layout(action: object, *, cas_root: str | Path,
                   checkout_root: str | Path,
                   worker_launcher_identity: object | None = None
                   ) -> Iterator[_WorkerLayout | None]:
    """Core's worker-side entry point; ordinary execution yields None.

    Merely supplying environment variables cannot opt an action into this
    layout. The exact sealed recipe and the live materializer lease are both
    required. A failed adoption/check is never permission for private replay.
    """
    from . import action_result, core, materialize

    normalized = core.validate_action(action)
    params = normalized["params"]
    movement = _constants()
    if "execution_layout" not in params:
        yield None
        return
    if params["execution_layout"] != movement._PBTEST_SOURCE_READONLY_LAYOUT:
        raise core.ActionContractError("unsupported worker execution layout")
    action_result.bind_standard_capture_command(normalized)
    variables = normalized["environment"]["variables"]
    conflicts = movement._WORKER_PATH_ENV_KEYS.intersection(variables)
    if conflicts:
        raise core.ActionContractError(
            "reserved worker path variables: " + ", ".join(sorted(conflicts)))
    if (params.get("profile") or params.get("cwd") != "."
            or normalized["task"]["working_directory"] != "."
            or normalized["task"]["definition_id"] != "fleet/pbrun"):
        raise core.ActionContractError("pbtest source-readonly requires an unprofiled root action")
    source = Path(checkout_root).resolve(strict=True)
    tmp = os.environ.get("TMPDIR", "")
    if not tmp:
        raise core.ActionContractError("pbtest worker requires its private attempt TMPDIR")
    attempt = _layout_directory(Path(tmp), source=source)
    paths = _expected_paths(normalized, attempt)
    for key, expected in paths.items():
        if os.environ.get(key) != str(expected):
            raise core.ActionContractError("missing or non-private worker path: " + key)
    result_root = _layout_directory(attempt / _LAYOUT_DIRECTORY / "results", source=source)
    for key, path in paths.items():
        _layout_directory(path.parent if key == movement._RESULT_PATH_ENV else path, source=source)
    result = paths[movement._RESULT_PATH_ENV]
    core._refuse_existing_result_symlink_prefix(result, result_root)
    bytecode = _layout_directory(attempt / _LAYOUT_DIRECTORY / "bytecode", source=source)
    environment = {
        **{key: str(value) for key, value in paths.items()},
        "TMPDIR": str(attempt),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPYCACHEPREFIX": str(bytecode),
    }
    item = {"action_key": normalized["action_key"], "cas_root": str(cas_root),
            "checkout_snapshot": params.get("checkout_snapshot")}
    with materialize._shared_source_checkout(
            item, local_checkout_root=materialize.LOCAL_CHECKOUT_ROOT) as lease:
        if lease.checkout_root.resolve(strict=True) != source:
            raise core.ActionContractError("worker source does not match its pinned generation")
        with _attempt_lock(attempt, fcntl.LOCK_SH) as attempt_reader:
            layout = _WorkerLayout(
                action=normalized, lease=lease, result_root=result_root,
                environment=environment, worker_launcher_identity=worker_launcher_identity,
                attempt_reader_descriptor=attempt_reader)
            try:
                # Validate before even a cache hit; never trust a JSON proof.
                layout.preflight()
                yield layout
                layout.verify_source()
            finally:
                # Close only our copy. An action/descendant inherited the same
                # open-file descriptions and must keep its independent hold.
                layout._close()
