"""Pre-execution decomposition: one logical request, many ordinary children.

A producer that wants ten thousand small measurements has two bad options in
an ordinary fleet.  It can submit ten thousand actions, and pay one cold
process -- model load, calibration residency, kernel warmup -- per measurement.
Or it can submit one action per box and shard the work itself, which makes the
producer a second scheduler: it picks hosts, it decides worker counts, it
rebalances, and every one of those decisions is invisible to PrismaBuild's
admission, retry and receipt machinery.

This module is the third option.  The producer declares *logical tasks* and the
measured cost inputs that say what it costs to set a residency up and what each
task costs once it is up.  PrismaBuild owns the partition: a deterministic,
versioned batcher turns the roster into the smallest batches that still amortize
setup, and every batch becomes an ordinary sealed action that claims, retries,
caches and receipts exactly like any other.  The producer never names a host, a
worker count or a shard.

Three properties make that safe, and this module exists to hold them:

* **The plan is frozen before the first child is published.**  A partition that
  can be re-derived is a partition that can change under a retry, and a retry
  that changes membership makes two children that both claim to have measured
  the same task.  So the plan's bytes are published first, under the parent's
  own key, and recovery reuses them rather than re-deriving them.
* **The partition is a pure function of sealed bytes.**  Not of the live offer
  count, not of what is idle right now.  Queue the smallest sensible units and
  let ordinary admission distribute them as capacity changes.
* **Refusal, never a fallback.**  If the roster and the policy admit no exact
  cover, decomposition says so and names the run, the task range and the two
  limits.  It does not emit one oversized opaque child, and it does not quietly
  relax a limit the producer declared.

Nothing here reaches the network, the queue or a GPU.  The partitioner is
arithmetic over the roster, which is what makes its determinism testable.

Estimates are planning hints bound into the plan's identity, not measured speed
claims.  Qualification measures what the resulting execution actually costs.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
import hashlib
from pathlib import Path
from typing import Any

from . import core as pb
from .core import ActionContractError, canonical_sha256

__all__ = [
    "ActionContractError",
    "BATCH_ENVELOPE_SCHEMA_V1",
    "CHILD_RESULT_MANIFEST_SCHEMA_V1",
    "FROZEN_COMMON_SCHEMA_V1",
    "GROUP_RECEIPT_SCHEMA_V1",
    "LOGICAL_BATCH_PARAM",
    "LOGICAL_BATCH_SCHEMA_V1",
    "LOGICAL_REQUEST_SCHEMA_V1",
    "LOGICAL_TASK_ROSTER_SCHEMA_V1",
    "PARENT_IDENTITY_SCHEMA_V1",
    "PARTITION_ALGORITHM_VERSION",
    "PLAN_SCHEMA_V1",
    "PUBLICATION_INDEX_SCHEMA_V1",
    "RESULT_MANIFEST_INPUT_ID",
    "TASK_BATCH_INPUT_ID",
    "ROSTER_BATCH_POLICY_SCHEMA_V1",
    "TASK_BATCH_PLACEHOLDER",
    "batch_envelope",
    "child_result_manifest_path",
    "document_bytes",
    "document_sha256",
    "write_document",
    "build_plan",
    "freeze_common",
    "logical_batch_param",
    "parent_key",
    "partition_roster",
    "publication_index",
    "resolve_task_batch",
    "validate_batch_policy",
    "validate_child_result_manifest",
    "validate_common_spec",
    "validate_frozen_common",
    "validate_logical_request",
    "validate_plan",
    "validate_roster",
    "verify_exact_cover",
]

LOGICAL_REQUEST_SCHEMA_V1 = "prismabuild.logical_request.v1"
LOGICAL_TASK_ROSTER_SCHEMA_V1 = "prismabuild.logical_task_roster.v1"
ROSTER_BATCH_POLICY_SCHEMA_V1 = "prismabuild.roster_batch_policy.v1"
PARENT_IDENTITY_SCHEMA_V1 = "prismabuild.logical_parent_identity.v1"
FROZEN_COMMON_SCHEMA_V1 = "prismabuild.frozen_common.v1"
PLAN_SCHEMA_V1 = "prismabuild.decomposition_plan.v1"
BATCH_ENVELOPE_SCHEMA_V1 = "prismabuild.task_batch.v1"
LOGICAL_BATCH_SCHEMA_V1 = "prismabuild.logical_batch.v1"
PUBLICATION_INDEX_SCHEMA_V1 = "prismabuild.decomposition_publication.v1"
CHILD_RESULT_MANIFEST_SCHEMA_V1 = "prismabuild.child_result_manifest.v1"
GROUP_RECEIPT_SCHEMA_V1 = "prismabuild.group_receipt.v1"

#: The sealed ``params`` key under which a child carries its membership.  A
#: child is an ordinary action in every other respect, so what makes it a
#: member of a plan has to be in the bytes its key hashes.
LOGICAL_BATCH_PARAM = "logical_batch"

#: The input ids a child carries beyond an ordinary action's: the frozen
#: roster every child of a parent shares, and the one batch envelope that
#: is only this child's.  Separate inputs rather than one, because the
#: roster's digest is the same across a campaign and the CAS stores it once.
TASK_ROSTER_INPUT_ID = "prismabuild.logical-task-roster"
TASK_BATCH_INPUT_ID = "prismabuild.task-batch"
RESULT_MANIFEST_INPUT_ID = "prismabuild.child-result-manifest"

#: Bumping this changes every ``plan_key`` while leaving parent identity alone,
#: which is the point: a better batcher must not look like a different
#: campaign, and a re-planned campaign must not silently reuse the old plan's
#: children.
PARTITION_ALGORITHM_VERSION = "contiguous-residency-exact-cover.v1"

#: The reserved whole-argument placeholder.  It is replaced, before sealing,
#: with the CAS path of the child's own batch input -- an exact argv element
#: match and nothing else.  It is deliberately not shell expansion, not
#: interpolation inside a larger string, and not a variable a producer can
#: redefine: the batch a child measures has to be a fact about its action key,
#: not about what its command line happened to evaluate to on some worker.
TASK_BATCH_PLACEHOLDER = "{pb.task_batch}"

_TASK_KEYS = frozenset(
    {"id", "payload", "residency_key", "estimated_seconds", "estimate_evidence",
     "output_id"}
)
_ROSTER_KEYS = frozenset({"schema", "tasks"})
_RESIDENCY_KEYS = frozenset({"key", "setup_seconds", "setup_evidence"})
_POLICY_KEYS = frozenset(
    {"schema", "residencies", "max_setup_fraction", "max_estimated_wall_seconds"}
)
_COMMON_KEYS = frozenset(
    {"argv", "cwd", "demand", "gpu_memory_gb", "data_manifest", "env"}
)
_REQUEST_KEYS = frozenset({"schema", "common", "roster", "batch_policy"})
_FROZEN_COMMON_KEYS = frozenset(
    {"schema", "argv", "cwd", "demand", "env", "gpu_memory_gb",
     "checkout_snapshot_sha256", "data_manifest_sha256"}
)
_PLAN_KEYS = frozenset(
    {"schema", "parent_key", "algorithm_version", "partitions", "plan_key"}
)
_MANIFEST_KEYS = frozenset(
    {"schema", "parent_key", "plan_key", "child_ordinal", "results"}
)
_RESULT_KEYS = frozenset({"task_id", "output_id", "value_sha256"})


# --------------------------------------------------------------------------
# Validators
#
# Every one of these refuses unknown fields.  A decomposition's identity is the
# hash of exactly these bytes, so a field PB does not understand is a field a
# producer believes is binding and PB would silently drop.
# --------------------------------------------------------------------------


def _finite_positive(value: object, *, where: str) -> float:
    """A duration the batcher can add up: finite, real and strictly positive."""

    if type(value) is bool or not isinstance(value, (int, float)):
        pb._fail(f"{where} must be a number")
    number = float(value)
    if number != number or number in (float("inf"), float("-inf")):
        pb._fail(f"{where} must be finite")
    if not number > 0.0:
        pb._fail(f"{where} must be greater than zero")
    return number


def _finite_nonnegative(value: object, *, where: str) -> float:
    if type(value) is bool or not isinstance(value, (int, float)):
        pb._fail(f"{where} must be a number")
    number = float(value)
    if number != number or number in (float("inf"), float("-inf")):
        pb._fail(f"{where} must be finite")
    if number < 0.0:
        pb._fail(f"{where} must not be negative")
    return number


def validate_roster(value: object) -> dict[str, Any]:
    """Canonicalize the producer's declared task list.

    Order is meaningful and preserved: the batcher only ever cuts a contiguous
    run, so the roster order is the producer's statement about which tasks
    share a residency and may therefore share a process.
    """

    roster = pb._exact_mapping(value, keys=_ROSTER_KEYS, where="task roster")
    if roster["schema"] != LOGICAL_TASK_ROSTER_SCHEMA_V1:
        pb._fail(f"task roster schema must be {LOGICAL_TASK_ROSTER_SCHEMA_V1!r}")
    raw_tasks = roster["tasks"]
    if not isinstance(raw_tasks, Sequence) or isinstance(raw_tasks, (str, bytes)):
        pb._fail("task roster tasks must be an array")
    if not raw_tasks:
        pb._fail("task roster must declare at least one task")
    tasks: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_outputs: set[str] = set()
    for index, raw in enumerate(raw_tasks):
        where = f"task roster tasks[{index}]"
        task = pb._exact_mapping(raw, keys=_TASK_KEYS, where=where)
        identity = pb._text(task["id"], where=f"{where}.id", pattern=pb._ID_RE)
        output_id = pb._text(
            task["output_id"], where=f"{where}.output_id", pattern=pb._ID_RE
        )
        if identity in seen_ids:
            pb._fail(f"{where}.id repeats an earlier task id: {identity!r}")
        if output_id in seen_outputs:
            pb._fail(f"{where}.output_id repeats an earlier output id: {output_id!r}")
        seen_ids.add(identity)
        seen_outputs.add(output_id)
        tasks.append({
            "id": identity,
            "payload": pb._normalize_json_value(
                task["payload"], where=f"{where}.payload"
            ),
            "residency_key": pb._text(
                task["residency_key"],
                where=f"{where}.residency_key",
                pattern=pb._ID_RE,
            ),
            "estimated_seconds": _finite_positive(
                task["estimated_seconds"], where=f"{where}.estimated_seconds"
            ),
            "estimate_evidence": pb._text(
                task["estimate_evidence"], where=f"{where}.estimate_evidence"
            ),
            "output_id": output_id,
        })
    return {"schema": LOGICAL_TASK_ROSTER_SCHEMA_V1, "tasks": tasks}


def validate_batch_policy(value: object) -> dict[str, Any]:
    """Canonicalize the limits that decide where a batch may be cut.

    Both limits and both kinds of evidence are required.  A default invented
    here would be PrismaBuild deciding how much of a producer's GPU hour may go
    to setup, which is exactly the judgment the producer measured and declared.
    """

    policy = pb._exact_mapping(value, keys=_POLICY_KEYS, where="batch policy")
    if policy["schema"] != ROSTER_BATCH_POLICY_SCHEMA_V1:
        pb._fail(f"batch policy schema must be {ROSTER_BATCH_POLICY_SCHEMA_V1!r}")
    raw_residencies = policy["residencies"]
    if (not isinstance(raw_residencies, Sequence)
            or isinstance(raw_residencies, (str, bytes))):
        pb._fail("batch policy residencies must be an array")
    if not raw_residencies:
        pb._fail("batch policy must declare at least one residency")
    residencies: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(raw_residencies):
        where = f"batch policy residencies[{index}]"
        entry = pb._exact_mapping(raw, keys=_RESIDENCY_KEYS, where=where)
        key = pb._text(entry["key"], where=f"{where}.key", pattern=pb._ID_RE)
        if key in seen:
            pb._fail(f"{where}.key repeats an earlier residency: {key!r}")
        seen.add(key)
        residencies.append({
            "key": key,
            "setup_seconds": _finite_nonnegative(
                entry["setup_seconds"], where=f"{where}.setup_seconds"
            ),
            "setup_evidence": pb._text(
                entry["setup_evidence"], where=f"{where}.setup_evidence"
            ),
        })
    fraction = policy["max_setup_fraction"]
    if type(fraction) is bool or not isinstance(fraction, (int, float)):
        pb._fail("batch policy max_setup_fraction must be a number")
    fraction = float(fraction)
    if not 0.0 < fraction <= 1.0:
        # Zero is refused rather than accepted-and-empty: it admits no batch
        # with any setup at all, so it is a refusal wearing a policy's clothes,
        # and a producer that means "never amortize" should say so by putting
        # one task in each residency run.
        pb._fail("batch policy max_setup_fraction must be in (0, 1]")
    return {
        "schema": ROSTER_BATCH_POLICY_SCHEMA_V1,
        "residencies": residencies,
        "max_setup_fraction": policy["max_setup_fraction"],
        "max_estimated_wall_seconds": _finite_positive(
            policy["max_estimated_wall_seconds"],
            where="batch policy max_estimated_wall_seconds",
        ),
    }


def validate_common_spec(value: object) -> dict[str, Any]:
    """Canonicalize what every child of this parent executes identically.

    This is the half of a child's action that decomposition does not vary, so
    it is also the half that has to be frozen once: a later recovery seals its
    missing children from these bytes, never from a checkout that has moved on.
    """

    common = pb._exact_mapping(value, keys=_COMMON_KEYS, where="common spec")
    raw_argv = common["argv"]
    if not isinstance(raw_argv, Sequence) or isinstance(raw_argv, (str, bytes)):
        pb._fail("common spec argv must be an array")
    if not raw_argv:
        pb._fail("common spec argv must not be empty")
    argv = [
        pb._text(part, where=f"common spec argv[{index}]", allow_control=True)
        for index, part in enumerate(raw_argv)
    ]
    # Embedding is checked first.  An embedded placeholder does not count as
    # one, so the arity check would otherwise answer "you have none of these"
    # to a producer who wrote one and chose the wrong shape -- the diagnosis
    # they need last rather than first.
    for index, part in enumerate(argv):
        if TASK_BATCH_PLACEHOLDER in part and part != TASK_BATCH_PLACEHOLDER:
            pb._fail(
                f"common spec argv[{index}] embeds {TASK_BATCH_PLACEHOLDER} in a "
                "larger argument; it is a whole-argument placeholder, not "
                "string interpolation"
            )
    if argv.count(TASK_BATCH_PLACEHOLDER) != 1:
        # Exactly one, because a child that cannot say which batch it measured
        # is an opaque action wearing a plan's identity, and two placeholders
        # would give one child two answers to that question.
        pb._fail(
            f"common spec argv must carry {TASK_BATCH_PLACEHOLDER} exactly once "
            "as a whole argument; a command without the declared batch input "
            "protocol stays an ordinary action"
        )
    demand = pb._normalize_json_value(common["demand"], where="common spec demand")
    if not isinstance(demand, Mapping) or not demand:
        pb._fail("common spec demand must be a non-empty object")
    for name, amount in demand.items():
        pb._nonnegative_integer(amount, where=f"common spec demand[{name!r}]")
    env = pb._normalize_json_value(common["env"], where="common spec env")
    if not isinstance(env, Mapping):
        pb._fail("common spec env must be an object")
    for name, text in env.items():
        pb._text(name, where="common spec env key", pattern=pb._ENV_RE)
        pb._text(text, where=f"common spec env[{name!r}]",
                 allow_empty=True, allow_control=True)
    gpu_memory_gb = common["gpu_memory_gb"]
    if gpu_memory_gb is not None:
        gpu_memory_gb = _finite_positive(
            gpu_memory_gb, where="common spec gpu_memory_gb"
        )
    data_manifest = common["data_manifest"]
    if data_manifest is not None:
        data_manifest = pb._text(data_manifest, where="common spec data_manifest")
    return {
        "argv": argv,
        "cwd": pb._text(common["cwd"], where="common spec cwd"),
        "demand": dict(demand),
        "env": dict(env),
        "gpu_memory_gb": gpu_memory_gb,
        "data_manifest": data_manifest,
    }


def validate_logical_request(value: object) -> dict[str, Any]:
    """Canonicalize a whole request and check the two halves agree.

    The cross-check that matters is residency membership: a task naming a
    residency the policy does not price has no setup cost, and a batcher with
    no setup cost has no reason to put anything in the same process.  Refusing
    here means the producer hears about it before a parent record exists.
    """

    request = pb._exact_mapping(value, keys=_REQUEST_KEYS, where="logical request")
    if request["schema"] != LOGICAL_REQUEST_SCHEMA_V1:
        pb._fail(f"logical request schema must be {LOGICAL_REQUEST_SCHEMA_V1!r}")
    common = validate_common_spec(request["common"])
    roster = validate_roster(request["roster"])
    policy = validate_batch_policy(request["batch_policy"])
    priced = {entry["key"] for entry in policy["residencies"]}
    for index, task in enumerate(roster["tasks"]):
        if task["residency_key"] not in priced:
            pb._fail(
                f"task roster tasks[{index}].residency_key "
                f"{task['residency_key']!r} is not priced by the batch policy; "
                f"it prices {sorted(priced)}"
            )
    return {
        "schema": LOGICAL_REQUEST_SCHEMA_V1,
        "common": common,
        "roster": roster,
        "batch_policy": policy,
    }


# --------------------------------------------------------------------------
# Identity
# --------------------------------------------------------------------------


def freeze_common(
    common: Mapping[str, Any],
    *,
    logical_cwd: str,
    checkout_snapshot_sha256: str,
    data_manifest_sha256: str | None = None,
) -> dict[str, Any]:
    """Replace the submitter's paths with the bytes they resolved to.

    A logical request names its source and its data by *path*, because that is
    what a producer can type.  A path is not an identity: the same
    ``/home/rob/prismaquant`` is a different tree after every commit, and two
    worktrees of one commit are the same tree under two names.  Hashing the
    declaration would therefore make a parent that recovery resumes against a
    tree that has moved on, and make two parents out of one campaign run from
    two checkouts.

    So the parent is keyed on this record instead, which pbrun's own Stage A
    produces: the snapshot digest for the source, the ingested manifest digest
    for the data, and the logical cwd the children will actually run in.  The
    rest of the declaration is already identity -- argv with the placeholder
    still in it, the demand, the environment, the GPU budget -- and is carried
    through unchanged.
    """

    common = validate_common_spec(common)
    declared = common["data_manifest"]
    if (declared is None) != (data_manifest_sha256 is None):
        # Freezing is the only place the two halves can be compared, and a
        # mismatch here means the parent would be keyed on data nobody
        # ingested, or ingested data no child was told to read.
        pb._fail(
            "frozen common declares a data manifest at "
            f"{declared!r} and was frozen with digest "
            f"{data_manifest_sha256!r}; both or neither"
        )
    return {
        "schema": FROZEN_COMMON_SCHEMA_V1,
        "argv": list(common["argv"]),
        "cwd": pb._text(logical_cwd, where="frozen common cwd"),
        "demand": dict(common["demand"]),
        "env": dict(common["env"]),
        "gpu_memory_gb": common["gpu_memory_gb"],
        "checkout_snapshot_sha256": pb._sha256(
            checkout_snapshot_sha256, where="frozen common checkout_snapshot_sha256"
        ),
        "data_manifest_sha256": None if data_manifest_sha256 is None else pb._sha256(
            data_manifest_sha256, where="frozen common data_manifest_sha256"
        ),
    }


def validate_frozen_common(value: object) -> dict[str, Any]:
    """Re-check a frozen common read back from storage."""

    frozen = pb._exact_mapping(
        value, keys=_FROZEN_COMMON_KEYS, where="frozen common"
    )
    if frozen["schema"] != FROZEN_COMMON_SCHEMA_V1:
        pb._fail(f"frozen common schema must be {FROZEN_COMMON_SCHEMA_V1!r}")
    return freeze_common(
        {
            "argv": frozen["argv"],
            "cwd": frozen["cwd"],
            "demand": frozen["demand"],
            "env": frozen["env"],
            "gpu_memory_gb": frozen["gpu_memory_gb"],
            # Already a digest; the declaration it came from is gone by now,
            # so present/absent is all that has to agree.
            "data_manifest": (
                None if frozen["data_manifest_sha256"] is None else "<frozen>"
            ),
        },
        logical_cwd=frozen["cwd"],
        checkout_snapshot_sha256=frozen["checkout_snapshot_sha256"],
        data_manifest_sha256=frozen["data_manifest_sha256"],
    )


def parent_key(
    frozen_common: Mapping[str, Any],
    roster: Mapping[str, Any],
    batch_policy: Mapping[str, Any],
) -> str:
    """The identity of the work, before anybody decides how to cut it.

    Deliberately free of the algorithm version: a better batcher reorganizes a
    campaign, it does not make it a different campaign.
    """

    return canonical_sha256({
        "schema": PARENT_IDENTITY_SCHEMA_V1,
        "common": validate_frozen_common(frozen_common),
        "roster": validate_roster(roster),
        "batch_policy": validate_batch_policy(batch_policy),
    })


def build_plan(
    request: Mapping[str, Any], frozen_common: Mapping[str, Any]
) -> dict[str, Any]:
    """Freeze one partition of one request, and give it a key.

    Takes the frozen common rather than deriving identity from the request's
    paths, so the plan cannot exist before the tree it will run against does.

    The blueprint holds task *ids* rather than batch digests, and the batch
    envelopes reference the plan key rather than the other way round.  That
    ordering is what keeps the hashes acyclic: blueprint, then envelopes, then
    the ordinary child actions that carry both.
    """

    validated = validate_logical_request(request)
    frozen = validate_frozen_common(frozen_common)
    if frozen["argv"] != validated["common"]["argv"]:
        pb._fail(
            "frozen common argv differs from the request's; the plan would be "
            "keyed on a command no child runs"
        )
    partitions = partition_roster(
        validated["roster"], validated["batch_policy"]
    )
    blueprint = {
        "schema": PLAN_SCHEMA_V1,
        "parent_key": parent_key(
            frozen, validated["roster"], validated["batch_policy"]
        ),
        "algorithm_version": PARTITION_ALGORITHM_VERSION,
        "partitions": partitions,
    }
    return {**blueprint, "plan_key": canonical_sha256(blueprint)}


def validate_plan(value: object) -> dict[str, Any]:
    """Re-check a plan read back from storage, key included.

    Recovery reuses published plan bytes instead of re-deriving a partition, so
    the one thing that must not be taken on trust is that those bytes are the
    plan their key names.
    """

    plan = pb._exact_mapping(value, keys=_PLAN_KEYS, where="decomposition plan")
    if plan["schema"] != PLAN_SCHEMA_V1:
        pb._fail(f"decomposition plan schema must be {PLAN_SCHEMA_V1!r}")
    raw_partitions = plan["partitions"]
    if (not isinstance(raw_partitions, Sequence)
            or isinstance(raw_partitions, (str, bytes))):
        pb._fail("decomposition plan partitions must be an array")
    if not raw_partitions:
        pb._fail("decomposition plan must contain at least one batch")
    partitions: list[list[str]] = []
    for ordinal, raw in enumerate(raw_partitions):
        where = f"decomposition plan partitions[{ordinal}]"
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
            pb._fail(f"{where} must be an array of task ids")
        if not raw:
            pb._fail(f"{where} is empty; every batch covers at least one task")
        partitions.append([
            pb._text(entry, where=f"{where}[{index}]", pattern=pb._ID_RE)
            for index, entry in enumerate(raw)
        ])
    blueprint = {
        "schema": PLAN_SCHEMA_V1,
        "parent_key": pb._sha256(
            plan["parent_key"], where="decomposition plan parent_key"
        ),
        "algorithm_version": pb._text(
            plan["algorithm_version"],
            where="decomposition plan algorithm_version",
        ),
        "partitions": partitions,
    }
    recorded = pb._sha256(plan["plan_key"], where="decomposition plan plan_key")
    if canonical_sha256(blueprint) != recorded:
        pb._fail("decomposition plan plan_key does not match its blueprint")
    return {**blueprint, "plan_key": recorded}


def document_bytes(value: object) -> bytes:
    """The one on-disk spelling of a decomposition document.

    A plan, an envelope and a manifest are each hashed twice in two different
    senses: ``canonical_sha256`` over the value binds it into a key, and the
    CAS addresses the *file* by the digest of its bytes.  Those two differ by
    a trailing newline, which is exactly the kind of difference that reads as
    a tampered blob rather than as a formatting choice.  So there is one
    writer, and ``document_sha256`` is what it will produce.
    """

    return pb._canonical_file_bytes(value)


def document_sha256(value: object) -> str:
    """The digest the CAS will give these bytes, known before they are written."""

    return hashlib.sha256(document_bytes(value)).hexdigest()


def write_document(path: str | Path, value: object) -> str:
    """Write one decomposition document and return the digest it now has."""

    Path(path).write_bytes(document_bytes(value))
    return document_sha256(value)


def child_result_manifest_path(child_ordinal: int) -> str:
    """Where a child writes what it measured, relative to its working tree.

    A decomposed child's declared result is its manifest, not the tee'd log:
    the log says the process exited, and the merge needs to know which tasks
    it answered.  The name has to be agreed rather than discovered, because
    the action that declares it is sealed before the child that writes it
    runs -- so it is derived from the ordinal here, carried in the batch
    envelope the child is handed, and sealed as the child's ``result_path``.
    Every action gets its own private checkout, so two children of one parent
    never contend for the name.
    """

    if not isinstance(child_ordinal, int) or isinstance(child_ordinal, bool):
        pb._fail("child ordinal must be an integer")
    if child_ordinal < 0:
        pb._fail(f"child ordinal must not be negative: {child_ordinal}")
    return f"pb-child-{child_ordinal:05d}.result-manifest.json"


def resolve_task_batch(
    argv: Sequence[str], *, batch_path: str | Path
) -> list[str]:
    """Put one child's batch file where the producer reserved a slot for it.

    Substitution, not expansion: the placeholder is a whole argument and is
    replaced by a whole argument, so a path with a space, a quote or a ``$``
    in it reaches the child as one argument and never as shell text.  The
    producer's own command is otherwise untouched.
    """

    resolved = [
        str(batch_path) if part == TASK_BATCH_PLACEHOLDER else part
        for part in argv
    ]
    if sum(part == TASK_BATCH_PLACEHOLDER for part in argv) != 1:
        pb._fail(
            f"command must carry {TASK_BATCH_PLACEHOLDER} exactly once as a "
            "whole argument before a batch can be resolved into it"
        )
    return resolved


def batch_envelope(
    request: Mapping[str, Any], plan: Mapping[str, Any], *, child_ordinal: int
) -> dict[str, Any]:
    """The immutable file one child reads to learn exactly what it measures.

    It carries whole tasks rather than ids because the child has to act on the
    payload, and a child that had to resolve ids against the roster would need
    the roster's order to mean the same thing twice.
    """

    partitions = plan["partitions"]
    if not 0 <= child_ordinal < len(partitions):
        pb._fail(
            f"child ordinal {child_ordinal} is outside the plan's "
            f"{len(partitions)} batches"
        )
    by_id = {task["id"]: task for task in request["roster"]["tasks"]}
    return {
        "schema": BATCH_ENVELOPE_SCHEMA_V1,
        "parent_key": plan["parent_key"],
        "plan_key": plan["plan_key"],
        "roster_sha256": canonical_sha256(request["roster"]),
        "batch_policy_sha256": canonical_sha256(request["batch_policy"]),
        "child_ordinal": child_ordinal,
        # Told rather than derived: the child would otherwise have to
        # reimplement the naming rule to agree with the action that already
        # declared its result path.
        "result_manifest_path": child_result_manifest_path(child_ordinal),
        "tasks": [by_id[task_id] for task_id in partitions[child_ordinal]],
    }


def logical_batch_param(
    request: Mapping[str, Any], plan: Mapping[str, Any], *, child_ordinal: int
) -> dict[str, Any]:
    """What a child seals into ``params`` so its key binds its membership.

    Ids rather than payloads: the payload bytes already reach the key through
    the batch input's digest, and repeating them would make one child's key
    grow with its batch for no additional statement.
    """

    return {
        "schema": LOGICAL_BATCH_SCHEMA_V1,
        "parent_key": plan["parent_key"],
        "plan_key": plan["plan_key"],
        "roster_sha256": canonical_sha256(request["roster"]),
        "batch_policy_sha256": canonical_sha256(request["batch_policy"]),
        "child_ordinal": child_ordinal,
        "ordered_task_ids": list(plan["partitions"][child_ordinal]),
    }


def publication_index(
    plan: Mapping[str, Any],
    *,
    batch_input_digests: Sequence[str],
    child_action_keys: Sequence[str],
) -> dict[str, Any]:
    """Bind the frozen plan to the exact children it authorizes.

    Written before the first child is published, so a crash halfway through
    leaves a record that says which keys were meant to exist.  Recovery fills
    the missing ones; it never asks the batcher for a fresh opinion.
    """

    count = len(plan["partitions"])
    if len(batch_input_digests) != count or len(child_action_keys) != count:
        pb._fail(
            f"publication index must carry one batch digest and one child key "
            f"per batch: plan has {count}, got {len(batch_input_digests)} "
            f"digests and {len(child_action_keys)} keys"
        )
    return {
        "schema": PUBLICATION_INDEX_SCHEMA_V1,
        "parent_key": plan["parent_key"],
        "plan_key": plan["plan_key"],
        "batch_input_sha256": [
            pb._sha256(digest, where=f"publication index batch_input_sha256[{index}]")
            for index, digest in enumerate(batch_input_digests)
        ],
        "child_action_keys": [
            pb._sha256(key, where=f"publication index child_action_keys[{index}]")
            for index, key in enumerate(child_action_keys)
        ],
    }


# --------------------------------------------------------------------------
# The partitioner
# --------------------------------------------------------------------------


class PartitionRefused(ActionContractError):
    """No exact cover exists under the declared limits.

    Its own type because the caller's response is different in kind: a schema
    error is a producer typo, while this one is a statement about the limits
    the producer measured, and the operator needs the numbers to change them.
    """


def _runs(tasks: Sequence[Mapping[str, Any]]) -> list[tuple[int, int, str]]:
    """Split the roster into maximal contiguous same-residency runs."""

    runs: list[tuple[int, int, str]] = []
    start = 0
    for index in range(1, len(tasks) + 1):
        if (index == len(tasks)
                or tasks[index]["residency_key"] != tasks[start]["residency_key"]):
            runs.append((start, index, tasks[start]["residency_key"]))
            start = index
    return runs


def _partition_run(
    tasks: Sequence[Mapping[str, Any]],
    *,
    begin: int,
    end: int,
    residency_key: str,
    setup_seconds: float,
    max_setup_fraction: float,
    max_wall_seconds: float,
) -> list[list[int]]:
    """Cut one same-residency run into the most batches that stay feasible.

    A greedy packer is wrong here and the reason is the remainder: filling each
    batch to the wall bound can leave a final group too small to amortize its
    own setup, when a rebalanced partition of the same run has no such group.
    So this is a dynamic program over suffixes, in three passes.

    The two limits are a band, not a ceiling.  Rearranged, the setup-fraction
    bound says each batch must carry at least ``min_work`` seconds of useful
    work, and the wall bound says it may carry at most ``max_work``; a batch is
    feasible exactly when its estimate lands between them.

    Pass one maximizes the batch count, which is the design's "smallest
    sensible units".  Pass two, among partitions achieving that count,
    minimizes the largest estimated batch wall -- a max is monotone in its
    parts, so the optimal suffix of an optimal partition is optimal and the
    recursion is sound.  Pass three walks forward taking the earliest cut that
    still reaches both optima, which is the lexicographically smallest cut
    sequence among the winners.

    All arithmetic is prefix-sum differences in roster order.  The partition
    has to be a pure function of the sealed bytes on every box, and floating
    point only gives that when the operations are the same ones in the same
    order.
    """

    count = end - begin
    prefix = [0.0] * (count + 1)
    for index in range(count):
        prefix[index + 1] = prefix[index] + tasks[begin + index]["estimated_seconds"]

    max_work = max_wall_seconds - setup_seconds
    # setup / (setup + work) <= f  <=>  work >= setup * (1 - f) / f
    min_work = setup_seconds * (1.0 - max_setup_fraction) / max_setup_fraction

    def refuse(reason: str) -> None:
        raise PartitionRefused(
            f"no exact cover for residency {residency_key!r} over roster tasks "
            f"[{begin}, {end}) ({count} tasks, "
            f"{prefix[count]:.6g}s estimated): {reason}. "
            f"Each batch must carry at least {min_work:.6g}s of useful work "
            f"(setup {setup_seconds:.6g}s at max_setup_fraction "
            f"{max_setup_fraction:.6g}) and at most {max_work:.6g}s "
            f"(max_estimated_wall_seconds {max_wall_seconds:.6g}s). "
            "Change a limit or the roster; decomposition does not emit an "
            "oversized child and does not relax a declared limit."
        )

    if max_work <= 0.0:
        refuse(
            f"setup alone meets or exceeds the wall ceiling, so no batch of "
            f"this residency can run"
        )
    if min_work > max_work:
        refuse(
            "the setup-fraction floor is above the wall ceiling, so the two "
            "limits admit no batch at all"
        )
    for index in range(count):
        seconds = tasks[begin + index]["estimated_seconds"]
        if seconds > max_work:
            refuse(
                f"task {tasks[begin + index]['id']!r} alone estimates "
                f"{seconds:.6g}s, past the ceiling"
            )

    def window(start: int) -> tuple[int, int]:
        """The half-open range of batch ends that are feasible from ``start``.

        Work is nondecreasing in the end position, so both edges are a
        bisection rather than a scan; without this the DP is quadratic in a
        roster that can hold tens of thousands of tasks.
        """

        base = prefix[start]
        low, high = start + 1, count + 1
        first = high
        while low < high:                                  # first end >= min_work
            middle = (low + high) // 2
            if prefix[middle] - base >= min_work:
                first, high = middle, middle
            else:
                low = middle + 1
        low, high = start + 1, count + 1
        last = start                                       # last end <= max_work
        while low < high:
            middle = (low + high) // 2
            if prefix[middle] - base <= max_work:
                last, low = middle, middle + 1
            else:
                high = middle
        return first, last + 1

    unreachable = -1
    batches = [unreachable] * (count + 1)
    batches[count] = 0
    for start in range(count - 1, -1, -1):
        first, stop = window(start)
        best = unreachable
        for finish in range(first, min(stop, count + 1)):
            if batches[finish] != unreachable and batches[finish] + 1 > best:
                best = batches[finish] + 1
        batches[start] = best
    if batches[0] == unreachable:
        refuse("no contiguous partition of the run satisfies both limits")

    infinite = float("inf")
    walls = [infinite] * (count + 1)
    walls[count] = 0.0
    for start in range(count - 1, -1, -1):
        if batches[start] == unreachable:
            continue
        first, stop = window(start)
        best = infinite
        for finish in range(first, min(stop, count + 1)):
            if batches[finish] != batches[start] - 1:
                continue
            candidate = max(
                setup_seconds + (prefix[finish] - prefix[start]), walls[finish]
            )
            if candidate < best:
                best = candidate
        walls[start] = best

    ceiling = walls[0]
    partition: list[list[int]] = []
    start = 0
    while start < count:
        first, stop = window(start)
        for finish in range(first, min(stop, count + 1)):
            if batches[finish] != batches[start] - 1:
                continue
            wall = setup_seconds + (prefix[finish] - prefix[start])
            if wall <= ceiling and walls[finish] <= ceiling:
                partition.append(list(range(begin + start, begin + finish)))
                start = finish
                break
        else:                                      # pragma: no cover - unreachable
            refuse(
                "plan reconstruction found no cut reaching the optimum it just "
                "computed; this is a batcher defect, not a limits problem"
            )
    return partition


def partition_roster(
    roster: Mapping[str, Any], policy: Mapping[str, Any]
) -> list[list[str]]:
    """Cut a validated roster into the plan's ordered batches of task ids.

    Runs of different residency keys are never mixed: two tasks that need
    different resident state in one process would pay both setups, which is the
    cost this whole mechanism exists to avoid.
    """

    tasks = roster["tasks"]
    setup_by_key = {
        entry["key"]: entry["setup_seconds"] for entry in policy["residencies"]
    }
    fraction = float(policy["max_setup_fraction"])
    wall = float(policy["max_estimated_wall_seconds"])
    partitions: list[list[str]] = []
    for begin, end, residency_key in _runs(tasks):
        for indices in _partition_run(
            tasks,
            begin=begin,
            end=end,
            residency_key=residency_key,
            setup_seconds=setup_by_key[residency_key],
            max_setup_fraction=fraction,
            max_wall_seconds=wall,
        ):
            partitions.append([tasks[index]["id"] for index in indices])
    return partitions


# --------------------------------------------------------------------------
# Exact-cover verification
# --------------------------------------------------------------------------


def validate_child_result_manifest(value: object) -> dict[str, Any]:
    """Canonicalize what one child says it measured.

    The child's declared result is this manifest, not its log: a launcher log
    says a process ran, and what the group receipt needs to know is which
    roster tasks now have an answer.
    """

    manifest = pb._exact_mapping(
        value, keys=_MANIFEST_KEYS, where="child result manifest"
    )
    if manifest["schema"] != CHILD_RESULT_MANIFEST_SCHEMA_V1:
        pb._fail(
            f"child result manifest schema must be "
            f"{CHILD_RESULT_MANIFEST_SCHEMA_V1!r}"
        )
    raw_results = manifest["results"]
    if not isinstance(raw_results, Sequence) or isinstance(raw_results, (str, bytes)):
        pb._fail("child result manifest results must be an array")
    if not raw_results:
        pb._fail("child result manifest must carry at least one result")
    results: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_results):
        where = f"child result manifest results[{index}]"
        entry = pb._exact_mapping(raw, keys=_RESULT_KEYS, where=where)
        results.append({
            "task_id": pb._text(
                entry["task_id"], where=f"{where}.task_id", pattern=pb._ID_RE
            ),
            "output_id": pb._text(
                entry["output_id"], where=f"{where}.output_id", pattern=pb._ID_RE
            ),
            "value_sha256": pb._sha256(
                entry["value_sha256"], where=f"{where}.value_sha256"
            ),
        })
    return {
        "schema": CHILD_RESULT_MANIFEST_SCHEMA_V1,
        "parent_key": pb._sha256(
            manifest["parent_key"], where="child result manifest parent_key"
        ),
        "plan_key": pb._sha256(
            manifest["plan_key"], where="child result manifest plan_key"
        ),
        "child_ordinal": pb._nonnegative_integer(
            manifest["child_ordinal"], where="child result manifest child_ordinal"
        ),
        "results": results,
    }


def verify_exact_cover(
    request: Mapping[str, Any],
    plan: Mapping[str, Any],
    manifests: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Prove the children between them answered the roster exactly once.

    Closed on every way that can fail: a missing child, a child answering for a
    task that is not in its own batch, a task answered twice, a task not
    answered, a manifest from another plan.  An incomplete or failed set is
    never a group success, so this returns the group receipt only when the
    cover is exact.
    """

    roster_tasks = request["roster"]["tasks"]
    partitions = plan["partitions"]
    if len(manifests) != len(partitions):
        pb._fail(
            f"exact cover needs one result manifest per batch: the plan has "
            f"{len(partitions)}, {len(manifests)} were given"
        )
    output_by_task = {task["id"]: task["output_id"] for task in roster_tasks}
    answered: dict[str, int] = {}
    validated: list[dict[str, Any]] = []
    for manifest in manifests:
        checked = validate_child_result_manifest(manifest)
        ordinal = checked["child_ordinal"]
        if not 0 <= ordinal < len(partitions):
            pb._fail(
                f"child result manifest names ordinal {ordinal}, outside the "
                f"plan's {len(partitions)} batches"
            )
        if checked["parent_key"] != plan["parent_key"]:
            pb._fail(
                f"child {ordinal} reports parent {checked['parent_key'][:12]}, "
                f"not this plan's {str(plan['parent_key'])[:12]}"
            )
        if checked["plan_key"] != plan["plan_key"]:
            pb._fail(
                f"child {ordinal} reports plan {checked['plan_key'][:12]}, "
                f"not {str(plan['plan_key'])[:12]}"
            )
        validated.append(checked)
    by_ordinal: dict[int, dict[str, Any]] = {}
    for checked in validated:
        ordinal = checked["child_ordinal"]
        if ordinal in by_ordinal:
            pb._fail(f"two result manifests claim child ordinal {ordinal}")
        by_ordinal[ordinal] = checked
    for ordinal, batch in enumerate(partitions):
        checked = by_ordinal.get(ordinal)
        if checked is None:
            pb._fail(f"no result manifest for child ordinal {ordinal}")
        membership = set(batch)
        for entry in checked["results"]:
            task_id = entry["task_id"]
            if task_id not in membership:
                pb._fail(
                    f"child {ordinal} reports task {task_id!r}, which is not in "
                    "its own batch"
                )
            if output_by_task[task_id] != entry["output_id"]:
                pb._fail(
                    f"child {ordinal} reports task {task_id!r} under output id "
                    f"{entry['output_id']!r}, not the roster's "
                    f"{output_by_task[task_id]!r}"
                )
            if task_id in answered:
                pb._fail(
                    f"task {task_id!r} is answered by child {answered[task_id]} "
                    f"and again by child {ordinal}"
                )
            answered[task_id] = ordinal
        missing = [task_id for task_id in batch if task_id not in answered]
        if missing:
            pb._fail(
                f"child {ordinal} left {len(missing)} of its {len(batch)} tasks "
                f"unanswered, beginning with {missing[0]!r}"
            )
    unanswered = [task["id"] for task in roster_tasks if task["id"] not in answered]
    if unanswered:
        pb._fail(
            f"{len(unanswered)} roster tasks have no result, beginning with "
            f"{unanswered[0]!r}"
        )
    return {
        "schema": GROUP_RECEIPT_SCHEMA_V1,
        "parent_key": plan["parent_key"],
        "plan_key": plan["plan_key"],
        "task_count": len(roster_tasks),
        "child_count": len(partitions),
        "merged_result_sha256": canonical_sha256([
            [entry["task_id"], entry["output_id"], entry["value_sha256"]]
            for ordinal in range(len(partitions))
            for entry in by_ordinal[ordinal]["results"]
        ]),
    }
