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

#: The progress phases a stage mover reports in (#1010), in the order it
#: enters them: the copy, then the optional read-back that warms the file
#: server's cache (``stage_move.warm_staged``).  The sealer and the reporter
#: import these names, because ``progress.commit`` refuses a phase the
#: submission did not declare.
MOVER_COPY_PHASE = "copy"
MOVER_WARM_PHASE = "warm"
MOVER_PROGRESS_PHASES = (MOVER_COPY_PHASE, MOVER_WARM_PHASE)

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
    """The progress policy a stage mover is sealed with, and its derivation.

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

    ``landing_bytes_per_s`` is the slowest measured landing of the plan's
    manifest on this tier (``storage_tiers.mover_fill_price`` with basis
    ``landing``).  ``None`` means nothing measured one.  The policy is then
    ``None`` and the mover is sealed with no stall grace at all, which is
    what every mover had before #1010; the derivation says
    ``basis: "unmeasured"``.

    Both declared phases get the same grace.  The read-back in ``warm`` reads
    the stage the copy just wrote, in the same number of workers, which is no
    slower than the pool read the rate measured.

    Returns ``(policy, derivation)``.  ``policy`` is in the normalized form
    ``core.seal_action`` requires; ``derivation`` is for the plan's
    ``demand_source`` and names every term.
    """

    latency = (mover_report_latency_s() if report_latency_s is None
               else float(report_latency_s))
    sizes = sorted((int(size) for size in entry_bytes if int(size) > 0),
                   reverse=True)
    depth = max(1, int(copy_depth))
    unit = sum(sizes[:depth])
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
    does not vary: the same ``inputs`` (checkout snapshot, data manifest),
    the same code closure, the same environment variables. Its command is a
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
    variables = dict(template["environment"]["variables"])  # type: ignore[index]
    variables.pop(pool.CONTAINER_OWNER_ENV, None)
    variables.pop(pool.CONTAINER_MARKER_ENV, None)
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
            "argv": [SEALED_ARGV0, "--noprofile", "--norc", "-c",
                     f"export PATH={shlex.quote(variables['PATH'].split(':', 1)[0])}:$PATH; "
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
