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
import shlex
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from . import core as pb
from . import pool

#: The sealed shell that wraps a fleet tool: the tool writes no result file,
#: so the log the wrapper tees IS the declared result.
SEALED_ARGV0 = "/bin/bash"

#: Parameters a movement action may restate off its submission template.
_MOVEMENT_PARAM_KEYS = ("cwd", "checkout_snapshot", "retry_policy",
                        "data_manifest")

#: Task fields a movement action keeps off its submission template (#944).
#: ``definition_id`` and ``definition_version`` name the sealing tool, and
#: ``adaptive_cpu.action_identity`` reads its pbrun shape off them;
#: ``working_directory`` is where the wrapper starts, relative to ``cwd``;
#: ``determinism`` keeps every existing mover key (see the note in
#: `seal_movement_action`).  Everything else in the task is the mover's own.
_MOVEMENT_TASK_KEYS = ("definition_id", "definition_version", "determinism",
                       "working_directory")

#: What a movement action is, whatever its consumer is (#944).  A mover copies
#: bytes on the box that owns the stage, so it is ordinary portable generation
#: work that logs its copy.  A consumer's ``measurement`` class, its
#: platform-keyed or host-class-keyed scope and its toolchain describe the box
#: that will compute, and on the stage host they only refuse the copy:
#: admission demands an idle host for a measurement
#: (``measurement_host_not_idle``), and preflight refuses a platform or
#: toolchain the worker does not have.  For a ``generation`` consumer these are
#: exactly the values it already had, so its movers keep their keys.
MOVEMENT_TASK = {"task_class": "generation", "artifact_family": "generic",
                 "artifact_kind": "generic"}
MOVEMENT_EXECUTION_SCOPE = {"portability": "portable", "platform_key": None,
                            "host_class": None}

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
    thread caps, its spool root and pacing opt-ins, its ``PRISMAQUANT_*``
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


def movement_tools(tier: Mapping[str, object], *,
                   mover: str = "stage_move.py") -> tuple[str, str, str]:
    """The interpreter and the two movement scripts, as the tier announces them.

    ``mover`` names the movement node's script -- ``stage_move.py`` for a
    stage tier's pool-to-stage copy, ``ram_promote.py`` for the ram tier's
    stage-to-tmpfs promotion (#640); the egress node is ``stage_release.py``
    for both, pointed at whichever root the row names.

    Off the tier record, never off this process.  A mover runs on the box that
    owns the stage, and the box that seals it is very often a different one of
    a different architecture: PrismaQuant's dispatcher submits from an aarch64
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
    return (python, str(Path(root) / mover), str(Path(root) / "stage_release.py"))


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

    ``determinism`` is still the consumer's, so a generation consumer's
    movers keep their keys. It only matters when a second result is
    published under one key: a deterministic mover whose log differs would
    then be refused as a conflict.

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
    params["command"] = list(command)
    params["demand"] = {str(key): int(value) for key, value in dict(demand).items()}
    params["placement"] = {"required_tags": list(tags)}
    if retry_policy is not None:
        # A mover's retry policy is its own, not the consumer's (#603).  The
        # template's belongs to work that may not be safe to run twice; a mover
        # copies into a temporary, verifies the digest against the manifest
        # entry and then ``os.replace``s, so a second attempt either finds the
        # bytes already right or redoes the copy that failed.  Inheriting a
        # single-attempt policy makes one transient read error cost the whole
        # staged range, and the window behind it.
        params["retry_policy"] = dict(retry_policy)
    if extra_params:
        params.update(dict(extra_params))
    variables = movement_environment(params["command"])  # type: ignore[arg-type]
    marker_root = template["marker_root"]
    owner = container_owner_fn(
        params["command"], params["cwd"], params["demand"], variables,
        determinism=template["task"]["determinism"],      # type: ignore[index]
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
                     f"{shlex.join(params['command'])} 2>&1 | tee {shlex.quote(log_name)}; "
                     f"exit ${{PIPESTATUS[0]}}"],
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
