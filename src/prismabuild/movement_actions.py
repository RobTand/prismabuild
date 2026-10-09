"""Movement-node construction shared by the submitter and the writer lanes.

Narrow extraction of pbrun's ordinary movement path (``movement_tools`` and
``seal_movement_action``): both the submitter's residency sealing and the
produced-output writer lane must construct movers the same way -- off the
tier record the storage role announced, behind the movement bash wrapper,
with the submission template's identity. No second sealing scheme lives
here; this module IS the one construction, moved verbatim.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import shlex
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import NamedTuple

from . import core as pb
from . import pool

#: The sealed shell that wraps a fleet tool: the tool writes no result file,
#: so the log the wrapper tees IS the declared result.
SEALED_ARGV0 = "/bin/bash"


def captured_command(command: Sequence[str], log_name: str) -> str:
    """Capture bytes with scoped GNU preference and retain both pipeline codes.

    Selection uses the action's existing PATH. The resolved executable and
    capture failures are retained in the ordinary action stderr evidence.
    """
    return (
        'if _pb_capture=$(command -v gnutee); then :; '
        'elif _pb_capture=$(command -v tee); then :; '
        'else printf "pbrun: log capture executable unavailable\\n" >&2; exit 127; fi; '
        'printf "pbrun: log capture executable=%s\\n" "$_pb_capture" >&2; '
        f'{shlex.join(command)} 2>&1 | "$_pb_capture" {shlex.quote(log_name)}; '
        '_pb_status=("${PIPESTATUS[@]}"); '
        'if (( _pb_status[1] != 0 )); then '
        'printf "pbrun: capture status=%s; producer status=%s\\n" '
        '"${_pb_status[1]}" "${_pb_status[0]}" >&2; fi; '
        'if (( _pb_status[0] != 0 )); then exit "${_pb_status[0]}"; fi; '
        'exit "${_pb_status[1]}"'
    )


def standard_capture_argv(
    command: Sequence[str], log_name: str, *, path_prefix: str
) -> list[str]:
    """The sealed ``task.argv`` for a standard captured-log result, byte for byte.

    One wrapper, one owner.  ``pbrun`` seals exactly this list for an ordinary
    submission whose declared result is the log the wrapper tees
    (``task.result_path == log_name``): the first ``PATH`` component is
    exported ahead of the inherited ``PATH``, then :func:`captured_command`
    runs the command through the scoped capture.  A client that must prove the
    sealed ``params.command`` is the one the worker actually executed
    reconstructs this list from the validated request and compares it with
    ``task.argv``; nothing else in the tree may spell the wrapper (#1446).
    """

    return [
        SEALED_ARGV0, "--noprofile", "--norc", "-c",
        f"export PATH={shlex.quote(path_prefix)}:$PATH; "
        + captured_command(command, log_name),
    ]

#: Parameters a movement action may restate off its submission template.
#: ``retry_policy`` is not one of them (#950): a mover's is its own.
_MOVEMENT_PARAM_KEYS = ("cwd", "checkout_snapshot", "data_manifest")

#: Task fields a movement action keeps off its submission template (#944).
#: ``definition_id`` and ``definition_version`` name the sealing tool, and
#: ``adaptive_cpu.action_identity`` reads its pbrun shape off them;
#: ``working_directory`` is where the wrapper starts, relative to ``cwd``.
#: Everything else in the task is the mover's own, ``determinism`` included
#: (#950, `MOVEMENT_TASK`).
_MOVEMENT_TASK_KEYS = ("definition_id", "definition_version",
                       "working_directory")

#: What a movement action is, whatever its consumer is (#944).  A mover copies
#: bytes on the box that owns the stage, so it is ordinary portable generation
#: work that logs its copy.  A consumer's ``measurement`` class, its
#: platform-keyed or host-class-keyed scope and its toolchain describe the box
#: that will compute, and on the stage host they only refuse the copy:
#: admission demands an idle host for a measurement
#: (``measurement_host_not_idle``), and preflight refuses a platform or
#: toolchain the worker does not have.  For a ``generation`` consumer these are
#: exactly the values it already had.
#:
#: ``determinism`` is ``stochastic`` whatever the consumer's (#950).  A mover's
#: result is the log of one copy, which differs from the next copy's, and the
#: fleet re-runs a movement key on purpose: a re-stage after an eviction
#: republishes the same key with ``recompute`` so that the bytes are copied
#: again.  Sealed deterministic, that second log is refused as a CAS conflict
#: after the copy has already landed (``CASConflictError``), so every staging
#: must be a stochastic one to be a real copy.
MOVEMENT_TASK = {"task_class": "generation", "determinism": "stochastic",
                 "artifact_family": "generic", "artifact_kind": "generic"}
MOVEMENT_EXECUTION_SCOPE = {"portability": "portable", "platform_key": None,
                            "host_class": None}

#: Roles PrismaBuild itself assigns to a queue row (#1579): ``returns_capacity``
#: for a node whose whole job is to give capacity back, ``serves_residency`` for
#: a stage mover or RAM promotion a residency consumer waits on.  A gang's
#: reservation never holds either, because the running action, and through it
#: the gang, waits on them.  Nothing here is a flag a submitter can declare:
#: ``PoolQueue.publish`` refuses both names in a sealed action and derives the
#: role from the node's executed identity (:func:`capacity_role`).  The mark is
#: honoured only on a host that holds a protected copy of the tool the row
#: names (:func:`authorized_role`), so a host judges what it enforces.
#: The row field that names the tool a role mark was derived for.
ROLE_SCRIPT_FIELD = "movement_script"
PRODUCED_EXPORT_SCRIPT = "produced_export.py"
LOCAL_RESIDENT_SCRIPT = "local_resident.py"
CAPACITY_ROLE_FIELDS = ("returns_capacity", "serves_residency")
#: The row field that names a retained tool a pre-copy mover was sealed from
#: (#1659): no role mark, only a hint the enforcing host times against its copy.
RETAINED_SCRIPT_FIELD = "retained_movement_script"


class CapacityRole(NamedTuple):
    """A derived role and the protected-spelled tool path it was derived for."""
    role: str
    script: str


#: The interpreter a role-bearing movement node runs under (#1579, review 3).
#: The live fleet announces ``/usr/bin/python3`` on every tier record and uses
#: it in the produced-spool sealer, and the path is a root-owned system file a
#: submitter cannot rewrite.  Anything else runs the script under an
#: interpreter the submitter chose, so it is an ordinary row.
MOVEMENT_PYTHON = "/usr/bin/python3"
MOVEMENT_PYTHON_ARGS = ("-I",)

#: Isolated launch: ``-I`` runs Python without user-site startup code, so a
#: submitter-controlled ``~/.local`` cannot run before the protected tool.

#: Extra sealed environment names a movement node carries past
#: :func:`movement_environment`'s own (``PATH``, the locale): the Docker
#: ownership the sealer injects (:func:`seal_movement_action`).  Nothing else
#: is a movement launch: an extra ``BASH_ENV``, ``PYTHONPATH`` or startup hook
#: would run submitter code around the published tool.
MOVEMENT_EXTRA_ENVIRONMENT = ("PRISMABUILD_CONTAINER_OWNER", "PRISMABUILD_CONTAINER_MARKER")

#: How late a row may be published after its protected copy and still count as
#: sealed before the copy arrived (#1659): clock skew plus one tier cycle.
#: A row published later names the protected copy directly, so only this
#: bounded window can exempt a retained path.
PRE_COPY_SKEW_S = 120.0


def retained_pre_copy_candidate(action: Mapping[str, object], demand: Mapping[str, object], *,
                                residency: Mapping[str, object] | None) -> str | None:
    """The retained tool a pre-copy mover names, else ``None`` (#1659).

    The same executed identity :func:`capacity_role` requires, except the
    script names the retained store rather than a protected copy: the wrapper,
    task fields, scope, environment, isolated interpreter, small demand and
    residency/evict shape all hold.  A post-copy sealer names the protected
    twin, so a retained path names a mover sealed before the copy arrived
    (or a forgery in the bounded window, which the caller times).
    """
    from . import runtime_publication
    params = action.get("params")
    task = action.get("task")
    if not isinstance(params, Mapping) or not isinstance(task, Mapping):
        return None
    command = params.get("command")
    if (not isinstance(command, list) or len(command) < 3
            or not all(isinstance(part, str) for part in command)
            or not isinstance(demand, Mapping) or demand.get("gpu")):
        return None
    if (command[0] != MOVEMENT_PYTHON
            or command[1:1 + len(MOVEMENT_PYTHON_ARGS)] != list(MOVEMENT_PYTHON_ARGS)):
        return None
    script_index = 1 + len(MOVEMENT_PYTHON_ARGS)
    script_path = command[script_index]
    if runtime_publication.spelled_member(script_path):
        return None
    result_path = task.get("result_path")
    if (not isinstance(result_path, str)
            or task.get("argv") != [SEALED_ARGV0, "--noprofile", "--norc", "-c",
                                    captured_command(command, result_path)]
            or any(task.get(name) != value for name, value in MOVEMENT_TASK.items())
            or action.get("execution_scope") != MOVEMENT_EXECUTION_SCOPE
            or not _movement_environment_ok(action, command)):
        return None
    try:
        from . import resource_scope
        retained_store = Path(resource_scope.RETAINED_GENERATION_STORE)
        resolved = Path(script_path).resolve(strict=True)
        store = retained_store.resolve(strict=True)
        relative = resolved.relative_to(store)
    except (OSError, ValueError, TypeError):
        return None
    if len(relative.parts) < 3 or relative.parts[1] != "tools":
        return None
    script = Path(script_path).name
    if script in (STAGE_MOVER_SCRIPT, RAM_PROMOTE_SCRIPT):
        if isinstance(residency, Mapping) and "range_start_bytes" in residency:
            return script_path
        return None
    evict = script == LOCAL_RESIDENT_SCRIPT and _local_resident_evict(command)
    if script not in (STAGE_RELEASE_SCRIPT, PRODUCED_EXPORT_SCRIPT) and not evict:
        return None
    for kind, count in demand.items():
        if type(count) is not int or count < 0:
            return None
        if kind == "cpu" and count > 1 or kind == "mem_gb" and count > 1:
            return None
        if kind not in ("cpu", "mem_gb") and "@" not in str(kind):
            return None
    return script_path

def retained_pre_copy_exempt(item: Mapping[str, object], action: Mapping[str, object],
                             demand: Mapping[str, object],
                             residency: Mapping[str, object] | None) -> bool:
    """Whether a retained-path mover queued before the copy may run (#1659).

    The mover was sealed from the retained store before its host held the
    protected twin, so it carries no role mark.  Once the copy arrives the
    reservation would otherwise hold it by demand beside a whole-CPU member,
    and the gang it serves could never start.  The exemption needs all of:
    the retained candidate shape, a protected counterpart this host holds,
    and a row published no later than the copy plus skew.  Later retained
    rows name the protected twin directly, so the window is bounded.
    """
    from pathlib import Path as _Path
    try:
        from . import resource_scope, runtime_publication
        retained = retained_pre_copy_candidate(action, demand, residency=residency)
        if retained is None:
            return False
        counterpart = runtime_publication.protected_counterpart(
            _Path(retained), retained_store=_Path(resource_scope.RETAINED_GENERATION_STORE))
        if counterpart is None:
            return False
        try:
            generation = counterpart.resolve(strict=True).relative_to(
                runtime_publication.PROTECTED_GENERATION_STORE).parts[0]
        except (OSError, ValueError, TypeError):
            return False
        birth = runtime_publication.protected_published_unix(generation)
        if birth is None:
            return True
        published = item.get("published_unix")
        if (not isinstance(published, (int, float)) or isinstance(published, bool)
                or not math.isfinite(published)):
            return False
        return float(published) <= float(birth) + PRE_COPY_SKEW_S
    except (OSError, ValueError, TypeError, KeyError):
        return False

def retained_hint_exempt(item: Mapping[str, object]) -> bool:
    """Whether the row's retained hint names a pre-copy mover that may run (#1659)."""
    from pathlib import Path as _Path
    try:
        from . import resource_scope, runtime_publication
        retained = item.get(RETAINED_SCRIPT_FIELD)
        if not isinstance(retained, str) or not retained.startswith("/"):
            return False
        counterpart = runtime_publication.protected_counterpart(
            _Path(retained), retained_store=_Path(resource_scope.RETAINED_GENERATION_STORE))
        if counterpart is None:
            return False
        try:
            generation = counterpart.resolve(strict=True).relative_to(
                runtime_publication.PROTECTED_GENERATION_STORE).parts[0]
        except (OSError, ValueError, TypeError):
            return False
        birth = runtime_publication.protected_published_unix(generation)
        if birth is None:
            return True
        published = item.get("published_unix")
        if (not isinstance(published, (int, float)) or isinstance(published, bool)
                or not math.isfinite(published)):
            return False
        return float(published) <= float(birth) + PRE_COPY_SKEW_S
    except (OSError, ValueError, TypeError, KeyError):
        return False


def _movement_environment_ok(action: Mapping[str, object], command: list) -> bool:
    """Whether the sealed environment is exactly a movement launch (#1579)."""
    from . import pool
    environment = action.get("environment")
    if not isinstance(environment, Mapping):
        return False
    variables = environment.get("variables")
    if not isinstance(variables, Mapping):
        return False
    if (set(variables) - {"PATH", *MOVEMENT_LOCALE, *MOVEMENT_EXTRA_ENVIRONMENT}
            or not {"PATH", *MOVEMENT_LOCALE} <= set(variables)):
        return False
    if any(not isinstance(value, str) for value in variables.values()):
        return False
    if {name: variables[name] for name in ("PATH", *MOVEMENT_LOCALE)} != movement_environment(command):
        return False
    owner = variables.get(pool.CONTAINER_OWNER_ENV)
    marker = variables.get(pool.CONTAINER_MARKER_ENV)
    if (owner is None) != (marker is None):
        return False
    if owner is None:
        return True
    return (re.fullmatch(r"[0-9a-f]{64}", owner) is not None
            and str(marker).endswith(f"/{owner}.used"))


def effective_local_resident_operation(argv: object) -> str | None:
    """The operation a sealed ``local_resident`` command runs, shared with the tool.

    Parsed exactly as ``tools/fleet/local_resident.py`` parses it (#1579,
    review 3): the LAST ``--operation`` wins, ``--operation=value`` and
    unambiguous prefixes count, and anything argparse refuses is ``None``.
    This duplicates the tool's option shape rather than importing the tool, so
    the pool never imports a fleet script; the shape is asserted equal by
    ``test_local_resident_operation_parsing_matches_the_tool``.
    """
    import argparse
    import contextlib
    import io
    if not isinstance(argv, list) or not all(isinstance(part, str) for part in argv):
        return None
    parser = argparse.ArgumentParser()
    parser.add_argument("--pool-root", required=True)
    parser.add_argument("--set-id", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--operation", choices=("copy", "evict", "adopt"), required=True)
    parser.add_argument("--source")
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            args, _ = parser.parse_known_args(list(argv))
    except SystemExit:
        return None
    operation = getattr(args, "operation", None)
    return operation if operation in ("copy", "evict", "adopt") else None


def _tool_arguments(command: list) -> list:
    """The tool and its arguments: the interpreter and ``-I`` come first (#1659)."""
    return command[1 + len(MOVEMENT_PYTHON_ARGS):]


def _local_resident_evict(command: list) -> bool:
    """Whether ``command`` runs the evict operation, spelled once, literally (#1579).

    The effective operation comes from the tool's own parsing contract
    (:func:`effective_local_resident_operation`, last ``--operation`` wins),
    and the spelling must be exactly one literal ``--operation evict``: a
    duplicate, an ``--operation=value`` form or a prefix abbreviation may run
    ``evict`` today but is not the shape the sealer emits, so it is ordinary.
    """
    if command.count("--operation") != 1:
        return False
    if any(part != "--operation" and (part.startswith("--operation=")
                                      or part.startswith("--oper"))
            for part in command):
        return False
    tool_args = _tool_arguments(command)
    return effective_local_resident_operation(tool_args[1:]) == "evict"


def capacity_role(action: Mapping[str, object], demand: Mapping[str, object], *,
                  residency: Mapping[str, object] | None) -> CapacityRole | None:
    """The role PrismaBuild's own movement nodes have, from their executed identity.

    ``None`` for everything else, which a reservation then holds by its demand:
    unknown is consuming.  The role is read from what the node EXECUTES, never
    from a sidecar field a submitter sets, and from the sealed definition
    alone: it reads nothing from this host's filesystem, so a submitter's box
    and the box that enforces the role need not agree on anything but the
    definition.

    * the interpreter runs isolated (``-I`` after :data:`MOVEMENT_PYTHON`), so
      no user-site startup code runs before the protected tool (#1659);
    * the interpreter is :data:`MOVEMENT_PYTHON`, the root-owned system python
      the fleet seals; a submitter-owned python-named executable is ordinary;
    * the script is spelled as a tool of a protected runtime copy
      (``runtime_publication.spelled_member``), never a retained-store path or
      an alias.  Whether this host holds that copy is the claiming host's
      question (:func:`authorized_role`);

    ``returns_capacity``: ``stage_release.py``, ``produced_export.py`` or a
    ``local_resident.py`` whose effective operation (the tool's own
    ``--operation`` parsing, last wins) is a single literal ``--operation
    evict``; demanding at most one CPU and one GiB, no GPU, and no kind but a
    tier's (``kind@tier``).  ``serves_residency``: ``stage_move.py`` or
    ``ram_promote.py`` carrying a residency range, no GPU. A caller can choose
    arguments for a genuine published tool, within that tool's demand limits.
    ``recompute`` is not a condition.
    """
    from . import runtime_publication
    params = action.get("params")
    task = action.get("task")
    if not isinstance(params, Mapping) or not isinstance(task, Mapping):
        return None
    command = params.get("command")
    if (not isinstance(command, list) or len(command) < 3
            or not all(isinstance(part, str) for part in command)
            or not isinstance(demand, Mapping) or demand.get("gpu")):
        return None
    if (command[0] != MOVEMENT_PYTHON
            or command[1:1 + len(MOVEMENT_PYTHON_ARGS)] != list(MOVEMENT_PYTHON_ARGS)
            or not runtime_publication.spelled_member(command[1 + len(MOVEMENT_PYTHON_ARGS)])):
        return None
    script_index = 1 + len(MOVEMENT_PYTHON_ARGS)
    result_path = task.get("result_path")
    if (not isinstance(result_path, str)
            or task.get("argv") != [SEALED_ARGV0, "--noprofile", "--norc", "-c",
                                    captured_command(command, result_path)]
            or any(task.get(name) != value for name, value in MOVEMENT_TASK.items())
            or action.get("execution_scope") != MOVEMENT_EXECUTION_SCOPE
            or not _movement_environment_ok(action, command)):
        return None
    script = Path(command[script_index]).name
    if script in (STAGE_MOVER_SCRIPT, RAM_PROMOTE_SCRIPT):
        if isinstance(residency, Mapping) and "range_start_bytes" in residency:
            return CapacityRole("serves_residency", command[script_index])
        return None
    evict = script == LOCAL_RESIDENT_SCRIPT and _local_resident_evict(command)
    if script not in (STAGE_RELEASE_SCRIPT, PRODUCED_EXPORT_SCRIPT) and not evict:
        return None
    for kind, count in demand.items():
        if type(count) is not int or count < 0:
            return None
        if kind == "cpu" and count > 1 or kind == "mem_gb" and count > 1:
            return None
        if kind not in ("cpu", "mem_gb") and "@" not in str(kind):
            return None
    return CapacityRole("returns_capacity", command[script_index])


def authorized_role(item: Mapping[str, object]) -> bool:
    """Whether a READY row's role mark stands on THIS host (#1579).

    The mark was derived from the sealed definition when the row was
    published, on whatever box published it.  It exempts the row from a gang's
    reservation only here, on the box that enforces the reservation, and only
    if the tool the row names is a member of a protected copy this box holds:
    root custody through every path component, a receipt-bound publication
    record and the member's digest.  A mark with no such tool (a hand-written
    row, a copy this box lacks, an altered file, a path that is not spelled
    exactly) exempts nothing.  A row with no mark costs nothing to ask about.
    """
    from . import runtime_publication
    marks = [role for role in CAPACITY_ROLE_FIELDS if item.get(role) is True]
    script = item.get(ROLE_SCRIPT_FIELD)
    if len(marks) != 1 or not runtime_publication.spelled_member(script):
        return False
    path = Path(str(script))
    permitted = (STAGE_MOVER_SCRIPT, RAM_PROMOTE_SCRIPT) if marks[0] == "serves_residency" else (
        STAGE_RELEASE_SCRIPT, PRODUCED_EXPORT_SCRIPT, LOCAL_RESIDENT_SCRIPT)
    return path.name in permitted and runtime_publication.published_member(path) == path


#: The retry policy a movement node gets when its caller names none (#950).
#: Its own, never its consumer's: a mover copies into a temporary, verifies
#: and renames, and an egress unlinks what its fragment names and counts a
#: file already gone as released, so a second attempt of either finds the
#: work done or redoes what failed.  The bound matches pbrun's
#: ``--residency-mover-max-attempts`` default and the produced-output lane's.
MOVEMENT_RETRY_POLICY = {"max_attempts": 3, "retry_safe": True}

#: The system directories a movement node's ``PATH`` searches after its own
#: interpreter's (#996): pbrun's default ``PATH``, the one every fleet box has.
MOVEMENT_SYSTEM_PATH = ("/usr/local/bin", "/usr/bin", "/bin")

#: The locale a movement node runs in: pbrun's default, so a tool's output
#: encoding never depends on the box.
MOVEMENT_LOCALE = {"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}


def movement_environment(command: Sequence[str]) -> dict[str, str]:
    """The environment a movement node is sealed with, before its owner (#996).

    A mover's own, never its consumer's: the consumer's environment describes
    the box that will compute (a GB10 venv at the head of its ``PATH``, its
    thread caps, its spool root and pacing opt-ins, its client's own
    reader settings), and the mover runs on the box that owns the stage.  So
    it is the tier interpreter's directory -- ``command[0]``, which every
    caller takes off the tier record (`movement_tools`) or names absolutely --
    ahead of :data:`MOVEMENT_SYSTEM_PATH`, and :data:`MOVEMENT_LOCALE`.  The
    pool and CAS roots a mover works on are sealed on its command.  Nothing a
    movement tool reads comes from its environment except what the worker
    injects at launch (the action key, the progress and residency channels).

    It is also why a consumer's environment is not in a mover's key.
    """

    interpreter = str(command[0]) if command else ""
    head = [str(Path(interpreter).parent)] if interpreter.startswith("/") else []
    path: list[str] = []
    for directory in (*head, *MOVEMENT_SYSTEM_PATH):
        if directory not in path:
            path.append(directory)
    return {"PATH": ":".join(path), **MOVEMENT_LOCALE}


#: The progress phases a stage mover reports in (#1010), in the order it
#: enters them: its start (launch to the first read: the manifest, the
#: resume census and ``PoolQueue.ownership_start_gate``), the copy, then the
#: optional read-back that warms the file server's cache
#: (``stage_move.warm_staged``).  The sealer and the reporter import these
#: names, because ``progress.commit`` refuses a phase the submission did not
#: declare.
MOVER_START_PHASE = "start"
MOVER_COPY_PHASE = "copy"
MOVER_WARM_PHASE = "warm"
MOVER_PROGRESS_PHASES = (MOVER_START_PHASE, MOVER_COPY_PHASE, MOVER_WARM_PHASE)

#: The disk pacer's defaults for a stage mover: the storage pool it reads and
#: the caps it holds a copy at (``stage_move.py --pace-pool``,
#: ``--max-read-await-ms``, ``--max-backlog-ms``; pbrun passes none of them).
#: The worker judges pool contention against the same caps
#: (``pool.PoolContentionProbe``), so a mover's pacer and its stall check
#: never disagree about what "the pool is the bottleneck" means.
MOVER_PACE_POOL = "storage_pool"
MOVER_MAX_READ_AWAIT_MS = 10.0
MOVER_MAX_BACKLOG_MS = 2000.0

#: How many entries a stage mover copies at once while nobody else reads the
#: pool: ``stage_move.py --max-readers``' default, which pbrun does not
#: override.  Each copy worker holds one entry in flight, so this is also how
#: many entries can be part-copied with none of them landed yet.
MOVER_MAX_READERS = 16


def mover_report_latency_s() -> float:
    """How late a mover's progress can reach the worker's stall check (#1010).

    Two heartbeats: the mover reports at most one ``pool.HEARTBEAT_S`` after
    an entry lands (``stage_move --progress-interval-s``), and the worker
    samples the report at most one ``pool.HEARTBEAT_S`` after that
    (``PoolQueue.execute``'s progress poll).  Read at call time so a test
    that shortens the heartbeat shortens the grace with it.
    """

    return 2.0 * float(pool.HEARTBEAT_S)


def mover_progress_policy(
    entry_bytes: Sequence[int], *,
    landing_bytes_per_s: float | None,
    copy_depth: int = MOVER_MAX_READERS,
    report_latency_s: float | None = None,
) -> tuple[dict[str, object] | None, dict[str, object]]:
    """A stage mover's progress policy and every term of it (#1010).

    A mover reports the bytes it has landed: an entry counts once it is
    copied, verified and renamed into place, which is the durable unit
    ``progress.commit`` asks for.  So the longest a healthy copy can go
    without a new report is the time until its next entry lands.  Up to
    ``copy_depth`` entries are in flight at once and share the copy's rate,
    so none of them may land until the bytes of all of them have been read:
    the unit is the sum of the ``copy_depth`` largest entries of the range.
    A copy running at ``landing_bytes_per_s`` or faster lands one within
    ``unit / rate``, and its report reaches the stall check within
    ``report_latency_s`` more (:func:`mover_report_latency_s`).  The grace is
    their sum, rounded up to a whole second so float noise never moves a
    key.

    Every phase gets that grace.  ``start`` covers launch to the first read,
    so the copy's own clock starts at its first read.  ``warm`` reads back
    the stage the copy just wrote, no slower than the pool read the rate
    measured.

    ``landing_bytes_per_s`` is the slowest measured landing of the plan's
    manifest on this tier (``storage_tiers.mover_fill_price`` with basis
    ``landing``), capped by the fill the mover reserves.  ``None`` means
    nothing measured one.  The policy is then ``None`` and the mover is
    sealed with no stall grace at all, which is what every mover had before
    #1010; the derivation says ``basis: "unmeasured"``.

    The grace prices the copy, not the pool.  Time the pool is measurably
    the bottleneck -- a pacer hold, an unadmitted reader, a live egress at
    the start gate -- is credited by the worker from evidence it samples
    itself (:func:`pool_contention_spec`, ``pool.PoolContentionProbe``), so
    it never has to be priced here.

    Returns ``(policy, derivation)``.  ``policy`` is in the normalized form
    ``core.seal_action`` requires; ``derivation`` is for the plan's
    ``demand_source``.
    """

    depth = max(1, int(copy_depth))
    unit = sum(sorted((int(size) for size in entry_bytes), reverse=True)[:depth])
    latency = (mover_report_latency_s() if report_latency_s is None
               else float(report_latency_s))
    derivation: dict[str, object] = {
        "basis": "unmeasured", "landing_bytes_per_s": None,
        "copy_depth": depth, "unit_bytes": unit,
        "report_latency_s": latency, "grace_s": None}
    if (landing_bytes_per_s is None or isinstance(landing_bytes_per_s, bool)
            or not math.isfinite(float(landing_bytes_per_s))
            or float(landing_bytes_per_s) <= 0 or unit <= 0):
        return None, derivation
    rate = float(landing_bytes_per_s)
    grace = int(math.ceil(unit / rate + latency))
    derivation.update({"basis": "landing", "landing_bytes_per_s": rate,
                       "grace_s": grace})
    policy = {"schema": pb.PROGRESS_POLICY_SCHEMA_V1,
              "phases": [{"name": name, "grace_s": grace}
                         for name in MOVER_PROGRESS_PHASES]}
    return pb.validate_progress_policy(policy), derivation


def pool_contention_spec(
    *, members: Sequence[str], stage_root: str,
    priced_bytes_per_s: float,
    max_read_await_ms: float = MOVER_MAX_READ_AWAIT_MS,
    max_backlog_ms: float = MOVER_MAX_BACKLOG_MS,
) -> dict[str, object]:
    """What a mover's worker checks before it charges quiet to the copy (#1010).

    Sealed beside the progress policy as ``core.POOL_CONTENTION_PARAM``: the
    pool's member devices as the storage role announced them, the pacer's
    caps, the stage root whose ownership lock the mover's start gate takes,
    and the rate the grace was priced at, so the kill record can put the
    delivered rate beside it.  Normalized by ``core.validate_pool_contention``.
    """

    return pb.validate_pool_contention({
        "schema": pb.POOL_CONTENTION_SCHEMA_V1,
        "members": sorted({str(member) for member in members}),
        "max_read_await_ms": float(max_read_await_ms),
        "max_backlog_ms": float(max_backlog_ms),
        "priced_bytes_per_s": float(priced_bytes_per_s),
        "stage_root": str(stage_root)})


#: The progress phases a stage egress reports in (#1021), in the order it
#: enters them.  ``snapshot`` runs from launch until every entry is judged
#: under the stage's ownership lock: the fragment read, containment
#: reclamation, the census taken as a hint, the wait for the lock, and the
#: census and verdicts taken under it (``stage_release._evict_locked``,
#: #988).  ``drain`` is the unlinks.  ``release`` returns the tokens, drops
#: the fragment, lets the lock go, prunes and files the receipt.
EGRESS_SNAPSHOT_PHASE = "snapshot"
EGRESS_DRAIN_PHASE = "drain"
EGRESS_RELEASE_PHASE = "release"
EGRESS_PROGRESS_PHASES = (EGRESS_SNAPSHOT_PHASE, EGRESS_DRAIN_PHASE,
                          EGRESS_RELEASE_PHASE)

#: The receipt fields an egress price reads (``stage_release._evict_owned``
#: and ``_hold_record``, #988): the census before the lock and under it,
#: the hold, the unlinks and the prune after the lock.
_EGRESS_TIMINGS = ("census_s", "census_validate_s", "lock_held_s", "unlink_s")


def _seconds(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) and number >= 0 else None


def egress_price(records: Sequence[Mapping[str, object]], *,
                 stage_root: str) -> dict[str, object]:
    """The slowest measured terms of an egress on ``stage_root`` (#1021).

    Read off the egress receipts the stage has filed (``pool.
    POOL_EGRESS_SCHEMA_V1``, ``PoolQueue.move_records`` with that schema),
    each one a complete run of ``stage_release``, split three ways:

    * ``census_s``: the census before the lock plus the census under it
      (``census_s + census_validate_s``).  It reads the whole stage's
      claims, fragments and pins, so it is priced as the stage's cost, not
      per entry of one range.
    * ``unlink_s_per_entry``: the unlinks per entry judged
      (``unlink_s / entries_judged``).
    * ``settle_s``: the rest of the hold -- the verdicts, the token release
      and the fragment drop -- plus the prune after it
      (``lock_held_s - census_validate_s - unlink_s + prune_s``).

    Each term is the slowest any receipt measured, so the whole price is no
    faster than the slowest egress the stage has receipted.  A receipt that
    judged no entry measured no unlink rate and is skipped, as is one
    missing a timing (an interest drop, a refusal).  ``basis`` is ``egress``
    when at least one receipt priced it and ``none`` otherwise.
    """

    census = unlink = settle = None
    read = 0
    for record in records:
        if not isinstance(record, Mapping):
            continue
        if record.get("schema") != pool.POOL_EGRESS_SCHEMA_V1:
            continue
        if str(record.get("stage_root") or "") != str(stage_root):
            continue
        judged = record.get("entries_judged")
        if isinstance(judged, bool) or not isinstance(judged, int) or judged <= 0:
            continue
        timings = {name: _seconds(record.get(name)) for name in _EGRESS_TIMINGS}
        prune = _seconds(record.get("prune_s", 0.0))
        if prune is None or any(value is None for value in timings.values()):
            continue
        read += 1
        this_census = timings["census_s"] + timings["census_validate_s"]  # type: ignore[operator]
        this_unlink = timings["unlink_s"] / judged                        # type: ignore[operator]
        this_settle = max(0.0, timings["lock_held_s"]                     # type: ignore[operator]
                          - timings["census_validate_s"]
                          - timings["unlink_s"]) + prune
        census = this_census if census is None else max(census, this_census)
        unlink = this_unlink if unlink is None else max(unlink, this_unlink)
        settle = this_settle if settle is None else max(settle, this_settle)
    return {"basis": "egress" if read else "none", "receipts": read,
            "census_s": census, "unlink_s_per_entry": unlink,
            "settle_s": settle}


def egress_progress_policy(
    entry_bytes: Sequence[int], *, price: Mapping[str, object],
    report_latency_s: float | None = None,
) -> tuple[dict[str, object] | None, dict[str, object]]:
    """A stage egress's progress policy and every term of it (#1021).

    An egress reports the bytes it has released: an entry counts once its
    unlink has returned, or found the file already gone, which is the
    durable unit ``progress.commit`` asks for.  Its grace is the whole
    egress of this range at the stage's slowest measured terms
    (:func:`egress_price`) plus the time a report takes to reach the stall
    check (:func:`mover_report_latency_s`), rounded up to a whole second::

        priced_s = census_s + entries * unlink_s_per_entry + settle_s
        grace_s  = ceil(priced_s + report_latency_s)

    Every phase gets it.  Every quiet stretch of a healthy egress is part of
    its whole run, so at the measured terms each one fits in one grace; the
    drain reports as it goes, so a range larger than any measured one is
    not charged for its size, only for its quiet.  The time the egress
    waits for another holder of the stage's lock is not priced: the worker
    credits it (``pool.PoolContentionProbe``), and the holder's own contract
    bounds the hold.

    ``price.basis`` other than ``egress``, or an empty range, means nothing
    measured an egress here.  The policy is then ``None`` and the egress is
    sealed with no stall grace, which is what every egress had before
    #1021; the derivation says ``basis: "unmeasured"``.

    Returns ``(policy, derivation)``.  The derivation's
    ``priced_bytes_per_s`` is the range's bytes over ``priced_s``: the rate
    the grace was priced at, for the kill record.
    """

    sizes = [int(size) for size in entry_bytes]
    entries = len(sizes)
    range_bytes = sum(sizes)
    latency = (mover_report_latency_s() if report_latency_s is None
               else float(report_latency_s))
    derivation: dict[str, object] = {
        "basis": "unmeasured", "entries": entries, "range_bytes": range_bytes,
        "egress_receipts": int(price.get("receipts") or 0),  # type: ignore[arg-type]
        "census_s": None, "unlink_s_per_entry": None, "settle_s": None,
        "priced_s": None, "priced_bytes_per_s": None,
        "report_latency_s": latency, "grace_s": None}
    terms = [_seconds(price.get(name))
             for name in ("census_s", "unlink_s_per_entry", "settle_s")]
    if (price.get("basis") != "egress" or entries <= 0 or range_bytes <= 0
            or any(term is None for term in terms)):
        return None, derivation
    census, unlink, settle = (float(term) for term in terms)  # type: ignore[arg-type]
    priced = census + entries * unlink + settle
    if not math.isfinite(priced) or priced <= 0:
        return None, derivation
    grace = int(math.ceil(priced + latency))
    derivation.update({"basis": "egress", "census_s": census,
                       "unlink_s_per_entry": unlink, "settle_s": settle,
                       "priced_s": priced,
                       "priced_bytes_per_s": range_bytes / priced,
                       "grace_s": grace})
    policy = {"schema": pb.PROGRESS_POLICY_SCHEMA_V1,
              "phases": [{"name": name, "grace_s": grace}
                         for name in EGRESS_PROGRESS_PHASES]}
    return pb.validate_progress_policy(policy), derivation


#: The fleet's movement-node scripts, named once (REVIEW-1252-r4): the
#: sealing path's defaults and any reader that must recognize a movement row
#: by its sealed command share this one home -- a second copy of the same
#: fact drifts the day a script is added.
STAGE_MOVER_SCRIPT = "stage_move.py"
RAM_PROMOTE_SCRIPT = "ram_promote.py"
STAGE_RELEASE_SCRIPT = "stage_release.py"
MOVEMENT_SCRIPTS = (STAGE_MOVER_SCRIPT, RAM_PROMOTE_SCRIPT,
                    STAGE_RELEASE_SCRIPT)


def movement_tools(tier: Mapping[str, object], *,
                   mover: str = STAGE_MOVER_SCRIPT) -> tuple[str, str, str]:
    """The interpreter and the two movement scripts, as the tier announces them.

    ``mover`` names the movement node's script -- ``stage_move.py`` for a
    stage tier's pool-to-stage copy, ``ram_promote.py`` for the ram tier's
    stage-to-tmpfs promotion (#640); the egress node is ``stage_release.py``
    for both, pointed at whichever root the row names.

    Off the tier record, never off this process.  A mover runs on the box that
    owns the stage, and the box that seals it is very often a different one of
    a different architecture: a client's dispatcher submits from an aarch64
    Spark while the stage is dl380g10's.  ``sys.executable`` here names a venv
    that does not exist there, and ``RUNTIME_ROOT`` is this process's view of
    the generation; sealing either produces an action whose argv cannot start
    on the only box it can be placed on -- and it would fail at exec time,
    after the tier has already reserved its capacity.

    ``tier_loop.py`` discovers both on that box and announces them beside
    ``mountpoint``, which is the same kind of fact.  A tier that carries
    neither is a tier announced by a generation older than this, and the
    refusal says so rather than guessing.
    """

    tier_id = str(tier.get("tier_id") or "?")
    python = str(tier.get("mover_python") or "")
    root = str(tier.get("mover_tools_root") or "")
    if not python.startswith("/") or not root.startswith("/"):
        raise SystemExit(
            f"stage tier {tier_id} announces no interpreter or tool "
            f"root for its movement nodes (mover_python={python!r}, "
            f"mover_tools_root={root!r}). tier_loop.py discovers both on the "
            f"box that runs the movers; a tier last announced by a generation "
            f"older than this one is the usual cause, and publishing the "
            f"runtime again fixes it.  Filling them in from this process "
            f"would seal an argv naming a python that is not on that box")
    return (python, str(Path(root) / mover), str(Path(root) / STAGE_RELEASE_SCRIPT))


def container_owner(
    command,
    cwd,
    demand,
    variables,
    *,
    determinism,
    retry_policy,
    marker_root,
    identity=None,
    logical_cwd=None,
    placement=None,
    container_images=(),
    identity_fn: Callable[[Path], dict] | None = None,
) -> str:
    """Stable ownership id sealed before the action key exists.

    The action key includes the environment, and the environment needs this id,
    so using the final key would be recursive.  Hash the complete pre-lifecycle
    submission identity, including task and retry policy, normalized effective
    placement, normalized declared images, the pre-owner environment, and the
    marker namespace, instead.  Adding the owner and marker variables
    afterwards is deterministic and leaves no caller-chosen ownership
    namespace.

    ``container_images`` belongs to that identity even though it is not part
    of the command: two actions differing only in which image they require
    must not share a Docker ownership label and ``<owner>.used`` marker, or
    one action's cleanup can remove the other's live container.  It is
    included only when nonempty, so a submission without a declaration is
    byte-for-byte what it was before the field existed.
    """

    if identity is None:
        if identity_fn is None:
            raise ValueError(
                "container_owner needs an explicit identity or an "
                "identity_fn; ownership is never guessed")
        identity = identity_fn(Path(cwd))
    cwd_identity = str(cwd) if logical_cwd is None else str(logical_cwd)
    params = {
        "command": command,
        "cwd": cwd_identity,
        "demand": demand,
        "placement": placement or {"required_tags": []},
        "retry_policy": retry_policy,
    }
    if container_images:
        params["container_images"] = list(container_images)
    pre_owner_identity = {
        "schema": "prismaquant.prismabuild.container_owner_identity.v1",
        "task": {"determinism": determinism},
        "checkout": identity,
        "params": params,
        "environment": {"variables": variables},
        "container_lifecycle": {"marker_root": str(marker_root)},
    }
    return hashlib.sha256(
        json.dumps(pre_owner_identity, sort_keys=True).encode()
    ).hexdigest()


def seal_movement_action(
    template: Mapping[str, object],
    *,
    command: Sequence[str],
    demand: Mapping[str, int],
    tags: Sequence[str],
    log_name: str,
    retry_policy: Mapping[str, object] | None = None,
    container_owner_fn: Callable[..., str] | None = None,
    extra_params: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Seal one movement or egress node off the submission that needs it.

    The child keeps everything an action's identity is made of and a mover
    does not vary: the same ``inputs`` (checkout snapshot, data manifest)
    and the same code closure. Its environment variables are a mover's
    (`movement_environment`, #996), never the consumer's. Its command is a
    fleet tool rather than the submitter's, its demand is tier tokens rather
    than CPU and GPU, and it is placed on the box that owns the stage rather
    than on the box that will compute. So its task class, artifact family,
    execution scope and toolchain are a mover's (`MOVEMENT_TASK`,
    `MOVEMENT_EXECUTION_SCOPE`, no toolchain), never the consumer's (#944):
    a measurement consumer's isolation is its own host's, not the stage
    host's.

    ``determinism`` and the retry policy are a mover's too (#950): every
    movement node is ``stochastic``, so a re-stage under one key publishes
    its own log instead of being refused as a conflicting recomputation, and
    its ``retry_policy`` is ``retry_policy`` or, when the caller names none,
    `MOVEMENT_RETRY_POLICY` -- never the template's. The caller's queue row
    must carry the same ``max_attempts`` and ``retry_safe``.

    ``container_owner_fn`` defaults to this module's ``container_owner``
    with the template's ``checkout_identity``; pbrun passes its wrapper so
    a template without an explicit identity keeps its exact historical
    git-derived digest. ``extra_params`` carries lane-specific parameters
    beside the movement keys -- the produced-output lane seals its
    ``produced_output_batch`` reference and its own batch data manifest
    here.
    """

    if container_owner_fn is None:
        container_owner_fn = container_owner
    params: dict[str, object] = {
        name: template["params"][name]                    # type: ignore[index]
        for name in _MOVEMENT_PARAM_KEYS
        if name in template["params"]                     # type: ignore[operator]
    }
    isolated = list(command)
    if (len(isolated) >= 1 and isolated[0] == MOVEMENT_PYTHON
            and isolated[1:1 + len(MOVEMENT_PYTHON_ARGS)] != list(MOVEMENT_PYTHON_ARGS)):
        isolated[1:1] = list(MOVEMENT_PYTHON_ARGS)
    params["command"] = isolated
    params["demand"] = {str(key): int(value) for key, value in dict(demand).items()}
    params["placement"] = {"required_tags": list(tags)}
    # A mover's retry policy is its own, not the consumer's (#603, #950).  The
    # template's belongs to work that may not be safe to run twice; a mover
    # copies into a temporary, verifies the digest against the manifest entry
    # and then ``os.replace``s, so a second attempt either finds the bytes
    # already right or redoes the copy that failed, and an egress's second
    # attempt finds released what the first released.  Inheriting a
    # single-attempt policy makes one transient read error cost the whole
    # staged range, and one transient unlink error leaves the range's bytes
    # holding their tokens until pressure eviction.
    params["retry_policy"] = dict(MOVEMENT_RETRY_POLICY if retry_policy is None
                                  else retry_policy)
    if extra_params:
        params.update(dict(extra_params))
    variables = movement_environment(params["command"])  # type: ignore[arg-type]
    marker_root = template["marker_root"]
    owner = container_owner_fn(
        params["command"], params["cwd"], params["demand"], variables,
        determinism=MOVEMENT_TASK["determinism"],
        retry_policy=params["retry_policy"],
        marker_root=marker_root,
        identity=template["checkout_identity"],           # type: ignore[index]
        logical_cwd=params["cwd"],
        placement=params["placement"],
    )
    variables[pool.CONTAINER_OWNER_ENV] = owner
    variables[pool.CONTAINER_MARKER_ENV] = str(marker_root / f"{owner}.used")
    task = template["task"]
    body = {
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {
            **{name: task[name] for name in _MOVEMENT_TASK_KEYS},  # type: ignore[index]
            **MOVEMENT_TASK,
            # The sealed ``PATH`` is the launch environment's whole ``PATH``
            # (``core.run_local_action`` builds the child's environment from
            # the sealed variables alone), so the wrapper exports nothing.
            "argv": [SEALED_ARGV0, "--noprofile", "--norc", "-c",
                     captured_command(params["command"], log_name)],
            "result_path": log_name,
        },
        "inputs": template["inputs"],                     # type: ignore[index]
        "code_closure": template["code_closure"],         # type: ignore[index]
        "params": params,
        "environment": {**template["environment"],       # type: ignore[index]
                        "variables": variables, "toolchain": {}},
        "execution_scope": dict(MOVEMENT_EXECUTION_SCOPE),
    }
    try:
        return pb.seal_action(body)
    except pb.ActionContractError as exc:
        raise SystemExit(f"refusing to seal a movement action: {exc}") from None
