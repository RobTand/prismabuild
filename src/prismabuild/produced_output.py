"""PB-produced-output staging scope: deterministic PB-owned path (R2).

An admitted GPU action writes new exact activation/cotangent/checkpoint
artifacts and reads them again during THE SAME action. Existing PB manifests
only stage pre-existing immutable external inputs. This module owns the
staging contract for produced outputs; it does not copy bytes itself
(existing movers do), creates no parallel cache/ledger/dispatcher, and never
rewrites a sealed input manifest or action key.

WRITE-ONLY (#912): a template marked ``write_only`` declares outputs its own
action never reads again. It reserves no stage window (every tier's minimum
and window are zero, so `owner_demand_terms` is empty and admission charges
no stage token), and its batches commit at their origin through
`commit_origin_batch`: no mover, no stage copy, no funding. A later action
declares such a batch in its data manifest (`origin_batch_manifest`) and
stages it through the ordinary input path. The durable charge and the path
ownership of an origin-only batch last until `reclaim_origin` proves its
origin gone, as for a staged batch. A template without ``write_only`` hashes
exactly as before.

ORIGIN LIFETIME (#914): an origin-only batch is committed with a lifetime.
``retain``, the default, is #912's batch unchanged: PB never deletes it. A
``consumed`` batch is a handoff. Each consumer that declares it files a
consumer declaration at submission (`declare_origin_consumer`), and
`origin_retirement_tick`, run once per tier-loop cycle, deletes its origin
files and frees its durable charge once every declared consumer has
succeeded. It also sweeps a consumed batch that no consumer declared and
whose producer attempt is dead.

READ-BACK ORIGIN COMMIT (#1034): a read-back owner that reads a group back
from its own local spool publishes no stage copy, so it commits the group at
its origin too, through `ProducedSpool.commit_origin_group` once the export is
acknowledged (`commit_origin_batch` with ``landed``). That ends the prewrite
and charges the actual bytes. No consumer can declare such a batch, so a
``consumed`` one is swept once its owner attempt has ended, success included. Every retirement checks each file against
the identity its commit recorded, and every unknown keeps the batch.

R2 split (root review, defects 1-7):

  * TEMPLATE (sealed pre-submit): authorized prefix/slots/classes, durable
    byte maxima, per-tier minimum/window demand, allowed tiers. Carries NO
    action key and NO broker nonce (the client cannot know either).
  * INSTANCE (runtime, bound post-admission): template reference +
    owner (action/nonce/scope) derived from the PROTECTED live claim, never
    from caller arguments. Foreign/stale attempt context refuses.
  * BATCH (immutable committed window): exact manifest + class bytes + mover
    ownership on one tier. One scope holds many batches; each batch composes
    under its own namespace with the existing one-manifest validator.

OWNER vs NAMESPACE: OWNER = (owner_action_key, owner_attempt) names the
running action for holder binding and for the authoritative terminal/
containment proof. NAMESPACE (instance/batch derived keys) names material,
maps, pins and ledger holders and has NO terminal record. A certificate
naming a namespace is invalid. Writer digests are reused from the streaming
receipt, never recomputed by rereading HDD payloads.

IDENTITY (R3, no fallback): binding requires BOTH halves complete and
equal: launch env (`PRISMABUILD_ACTION_KEY/NONCE/SCOPE`, set by the
resource_exec proxy from the exact launch identity) AND the live claim
row's broker-issued `resource_scope` control (32-hex nonce + broker-formula
scope_id + matching action_key). Either half missing, or any mismatch,
refuses (`no-launch-context` / `no-control-context` / `attempt-superseded`,
same names as the PB730 `injected_context` contract). No intent fallback,
no claim-derived nonce, no live-claim-only binding. Fixtures file a
realistic broker control through the existing `ResourceScope` verification
(`_adopt_created_scope` over a canned broker response); production accepts
only broker-issued controls.

CREDIT vs TOKENS (liveness R2 direction, binding here): the admission
record (`admit_instance`) is binding metadata, NOT funding and NOT
capacity. Physical tokens are held ONLY by batch movers, exact per range,
acquired under the batch holder and TRANSFERRED whole to mover ownership
with no free interval (existing `transfer_tier_reservation`). No standing
ledger pool is held beside movers. The funded-claim primitive family
(`reserve_fence` / `ResourceLedger.transfer_tokens` / `funded_cover`) is
delivered by the prepaid-output lane, so `admit_funded_window` reports the
delivered `prepaid-per-batch` binding (declaration-only: the owner action's
own tier demand, reserved once at its claim; per-batch funding is the
authority). The `funding-primitive-pending` refusal remains only for a
pool missing one of those primitives. No second ledger here, never
subtracts unrelated holders' tokens.

MATERIALIZATION (bounded-window restage): a committed BATCH is immutable and
carries ONE durable origin charge; the stage copy under it is a window the
fleet takes back at retirement. `ensure_batch_materialized` is the ONE
transition this adds -- make an already committed batch resident again over
the SAME origin files, under the same owner action and attempt, the same
logical batch, manifest, descriptors, namespace and output prefix, and the
same durable charge. It adds no v2 record, no parallel
cache and no second dispatcher (an origin-only batch has nothing to
re-materialize, and refuses): it reuses the published first-publisher
sealing (`_seal_output_mover`), `movement_actions`, exact prepaid funding, the
strict SDK, the existing egress and the existing recovery. The caller chooses
neither origins (they come from the immutable record), nor tokens (ordinary
prepaid transfer), nor the successor id (the content-addressed key of a
request PB seals over the filed materialization GENERATION). Safety for a DEV
null-digest batch rests on the origin identity tuple captured at FIRST commit
(`origin_identity`, via `reader_lease.portable_identity`) and rechecked before
every re-materialization -- never an lstat size, and never a new payload hash.

PB730 owns: corrected `pin_id_for` (canonical object set), the additive
owner/material-namespace SDK contract, the containment writer, and the
immutable helper-env injection. This lane does not edit `reader_lease.py`,
does not invent pin serialization (ours is the manifest object set only),
and returns the exact SDK dependency instead of a stub.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
import errno
import hashlib
import json
import os
from pathlib import Path
import socket
import stat
import sys
import tempfile
import time
import uuid

TEMPLATE_SCHEMA_V1 = "prismaquant.prismabuild.produced_output_template.v1"
INSTANCE_SCHEMA_V1 = "prismaquant.prismabuild.produced_output_instance.v1"
DESCRIPTOR_SCHEMA_V2 = "prismaquant.prismabuild.produced_output_descriptor.v2"
BATCH_SCHEMA_V1 = "prismaquant.prismabuild.produced_output_batch.v1"
BATCH_MANIFEST_SCHEMA_V1 = "prismaquant.prismabuild.produced_output_manifest.v1"
#: The queue item's projection of the sealed declaration: which immutable
#: template this action's tier demand was derived from. The runtime binds
#: from this ref (filed body + live claim), never from a caller-supplied
#: template object.
PRODUCED_OUTPUT_REF_SCHEMA_V1 = "prismabuild.produced_output_ref.v1"
#: The sealed params record that makes a RE-materialization's action key its
#: own: the batch stays identical, so the PB-filed materialization generation
#: is what distinguishes the successor's content-addressed mover/funding key
#: from the spent one. Never a caller nonce; see `ensure_batch_materialized`.
MATERIALIZATION_SCHEMA_V1 = (
    "prismaquant.prismabuild.produced_output_materialization.v1")
#: What a consumer declares to read one origin-only batch (#912): the batch's
#: filing coordinates plus its manifest digest. See `origin_batch_ref`.
ORIGIN_BATCH_REF_SCHEMA_V1 = "prismabuild.produced_output_origin_batch_ref.v1"
#: The data-manifest annotation that carries a consumer's declared batches.
ORIGIN_BATCHES_ANNOTATION = "produced_output_batches"
#: Where a data_manifest.v2 read plan reads its declared batches (#946): one
#: ``{"phase", "refs"}`` per read phase that reads committed batches, in plan
#: order. A deferred consumer's static plan names the ``--after`` edge that
#: fills each slot instead (``{"phase", "after"}``). See
#: `place_origin_batches`.
ORIGIN_SLOTS_ANNOTATION = "produced_output_slots"
#: An origin-only batch's lifetime (#914). ``retain`` is the default and is
#: never written into a record, so a retained batch is filed byte for byte as
#: #912 filed it; ``consumed`` is retired once its declared consumers succeed.
ORIGIN_LIFETIME_RETAIN = "retain"
ORIGIN_LIFETIME_CONSUMED = "consumed"
ORIGIN_LIFETIMES = frozenset({ORIGIN_LIFETIME_RETAIN, ORIGIN_LIFETIME_CONSUMED})
#: One consumer's declaration that it reads one consumed batch (#914), filed
#: under the batch's instance at ``consumers/<batch_id>/<consumer_key>.json``.
ORIGIN_CONSUMER_SCHEMA_V1 = "prismabuild.produced_output_origin_consumer.v1"
#: An operator's release of one declaration (#926), filed beside the
#: declarations at ``released-consumers/<batch_id>/<consumer_key>.json``.
#: The declaration itself is never removed.
ORIGIN_CONSUMER_RELEASE_SCHEMA_V1 = (
    "prismabuild.produced_output_origin_consumer_release.v1")
#: The queue-wide index of those releases (#954): one file per released
#: consumer and batch, ``<consumer_key>.<ref sha256>.json`` under
#: `pool.PoolQueue.released_origin_consumers_dir`, so a claim finds a
#: released key by listing one directory. It is written before the release
#: record, and only the record makes the release real.
ORIGIN_CONSUMER_RELEASE_INDEX_SCHEMA_V1 = (
    "prismabuild.produced_output_origin_consumer_release_index.v1")
#: The mover's typed refusal when a declared window's origin directories are
#: not on the tier host (`stage_move.origin_reachability_diagnosis`). Spelled
#: as data: this package imports no fleet tool.
ORIGIN_UNREACHABLE_REFUSAL = "origin_unreachable"

OUTPUT_TEMPLATES_SUBDIR = "produced-output-templates"
OUTPUT_SCOPES_SUBDIR = "produced-output-scopes"
OUTPUT_BATCHES_SUBDIR = "produced-output-batches"
OUTPUT_FRAGMENTS_SUBDIR = "produced-output-fragments"
#: One pointer per attempt of each template, ``<template_id>/<owner>.<nonce>``
#: (#1053): how the attempts that may own an origin path are found without
#: listing every owner's scopes. `declare_instance` files it before the
#: instance; the tier loop's origin-retirement tick files it for any scope
#: that predates the index. It lives inside `OUTPUT_TEMPLATES_SUBDIR`, which
#: every residency enumerator skips as a record directory, so an egress or a
#: census running older code never reads it as an unknown fragment
#: namespace (#798); a template is filed as ``<template_id>.json``, and a
#: dotted directory is never one.
OUTPUT_TEMPLATE_ATTEMPTS_SUBDIR = ".attempts"
#: Filed in that directory once a tick has indexed every scope it listed.
#: Until then the attempts are found by listing the scopes themselves.
ATTEMPT_INDEX_COMPLETE = ".index-complete-v1"

#: Sealed helper-root env name (spelling only; PB730 owns injection).
#: PQ resolves the published runtime helper from this value and verifies
#: `reader_lease.__file__` under it. Never a mutable `/repo` checkout.
READER_HELPER_ROOT_ENV = "PRISMABUILD_READER_HELPER_ROOT"

#: Sealed producer env that opts a restage mover into reserving pool fill
#: (#747).  "" or "0" is off (the default), "1" is on, anything else refuses
#: the restage.  Read from the producer's SEALED request, never from the
#: calling process: the switch is part of what the producer was admitted as.
RESTAGE_FILL_ENV = "PRISMABUILD_PRODUCED_OUTPUT_RESTAGE_FILL"

#: Delivered SDK + funding dependencies this lane binds to (not stubs).
SDK_DEPENDENCY = (
    "PB730 additive owner/material-namespace SDK contract (delivered on "
    "main: acquire/open/release binding material under the batch namespace "
    "to the registered OWNER attempt; pin files under the owner, proof "
    "resolves in the material namespace) + corrected pin_id_for including "
    "the canonical expected object set and material generations "
    "(candidate pin 2637a9d0f7, R7-returned: auto-cleanup paths excluded); "
    "LIVENESS funded-claim primitive family delivered by the prepaid-output "
    "lane (funding record binding credit to exact tier/plan-window/mover/"
    "range/generation, eligible-token verification, serialized transfer "
    "without free interval via transfer_tokens, window admission covering "
    "current+next need); `admit_funded_window` reports the delivered "
    "prepaid-per-batch binding and keeps the funding-primitive-pending "
    "refusal only for a pool missing a primitive"
)

ARTIFACT_CLASSES = frozenset({"payload", "checkpoint", "temp"})

_HEX = frozenset("0123456789abcdef")
_HEX32 = 32


class ProducedOutputError(ValueError):
    """A template, instance, descriptor or batch that does not say what it must."""


def _hex64(value: object, *, where: str) -> str:
    if (not isinstance(value, str) or len(value) != 64
            or any(c not in _HEX for c in value)):
        raise ProducedOutputError(f"{where} must be a 64-character hex key")
    return value


def _hex32(value: object, *, where: str) -> str:
    if (not isinstance(value, str) or len(value) != 32
            or any(c not in _HEX for c in value)):
        raise ProducedOutputError(f"{where} must be a 32-character hex nonce")
    return value


def _name(value: object, *, where: str) -> str:
    if not isinstance(value, str) or not value or "/" in value:
        raise ProducedOutputError(f"{where} must be a non-empty name with no '/'")
    return value


def _abs_norm(value: object, *, where: str) -> str:
    if not isinstance(value, str) or not value.startswith("/"):
        raise ProducedOutputError(f"{where} must be an absolute path")
    normal = os.path.normpath(value)
    if value != normal:
        raise ProducedOutputError(f"{where} must be normalized")
    return normal


def _int_exact(value: object, *, where: str) -> int:
    # type() is int: bool is NOT an int here (True == 1 must not pass).
    if type(value) is not int:
        raise ProducedOutputError(f"{where} must be an integer")
    return int(value)


def _positive_int(value: object, *, where: str) -> int:
    checked = _int_exact(value, where=where)
    if checked <= 0:
        raise ProducedOutputError(f"{where} must be positive")
    return checked


def _nonneg_int(value: object, *, where: str) -> int:
    checked = _int_exact(value, where=where)
    if checked < 0:
        raise ProducedOutputError(f"{where} must be non-negative")
    return checked


def mint_generation() -> str:
    """One materialization generation: new bytes always mean a new identity."""

    return uuid.uuid4().hex


# --------------------------------------------------------------------------
# Templates (sealed pre-submit; no action key, no nonce)
# --------------------------------------------------------------------------

def validate_template(value: object) -> dict[str, object]:
    """Check a sealed scope template."""

    if not isinstance(value, Mapping):
        raise ProducedOutputError("a produced-output template must be an object")
    allowed = frozenset({
        "schema", "version", "template_id", "output_prefix", "slots",
        "durable_maxima", "working_demands", "permitted_tiers", "write_only",
    })
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ProducedOutputError(f"unknown template fields: {unknown}")
    # A write-only template (#912) declares outputs its own action never
    # reads again: its batches commit at origin (`commit_origin_batch`) and a
    # later action stages them as declared input.  It reserves no stage
    # window, so every tier it names carries a zero window; the tier stays
    # named because the spool export is paced on that tier's pool-side fill.
    write_only = value.get("write_only", False)
    if type(write_only) is not bool:
        raise ProducedOutputError("template write_only must be true or false")
    if value.get("schema") != TEMPLATE_SCHEMA_V1:
        raise ProducedOutputError(f"template schema must be {TEMPLATE_SCHEMA_V1!r}")
    if type(value.get("version")) is not int or value.get("version") != 1:
        raise ProducedOutputError("template version must be integer 1")
    template_id = _name(value.get("template_id"), where="template template_id")
    prefix = _abs_norm(value.get("output_prefix"), where="template output_prefix")
    slots = value.get("slots")
    if not isinstance(slots, Mapping) or not slots:
        raise ProducedOutputError("template slots must be a non-empty object")
    checked_slots: dict[str, str] = {}
    for slot, spec in slots.items():
        name = _name(slot, where="template slots[]")
        if not isinstance(spec, Mapping) or set(spec) != {"class"}:
            raise ProducedOutputError(
                f"template slot {name!r} must carry exactly {{class}}")
        cls = spec.get("class")
        if cls not in ARTIFACT_CLASSES:
            raise ProducedOutputError(
                f"template slot {name!r} class must be one of "
                f"{sorted(ARTIFACT_CLASSES)}")
        if name in checked_slots:
            raise ProducedOutputError("template slots must not repeat a slot")
        # Preserve the sealed shape ({slot: {class}}) so validation is
        # idempotent: a validated template re-validates byte-identically.
        checked_slots[name] = {"class": str(cls)}
    maxima = value.get("durable_maxima")
    if not isinstance(maxima, Mapping):
        raise ProducedOutputError("template durable_maxima must be an object")
    if set(maxima) != {"payload_max_bytes", "checkpoint_max_bytes",
                       "temp_max_bytes"}:
        raise ProducedOutputError(
            "template durable_maxima must carry payload/checkpoint/temp maxima")
    payload = _positive_int(maxima.get("payload_max_bytes"),
                            where="template durable_maxima.payload_max_bytes")
    checkpoint = _nonneg_int(maxima.get("checkpoint_max_bytes"),
                             where="template durable_maxima.checkpoint_max_bytes")
    temp = _nonneg_int(maxima.get("temp_max_bytes"),
                       where="template durable_maxima.temp_max_bytes")
    demands = value.get("working_demands")
    if not isinstance(demands, Mapping) or not demands:
        raise ProducedOutputError("template working_demands must be non-empty")
    try:
        from prismabuild import storage_tiers as tiers_mod
    except ImportError as exc:
        raise ProducedOutputError(
            f"template tier-kind check needs storage_tiers: {exc}") from None
    checked_demands: dict[str, dict[str, int]] = {}
    for tier, spec in demands.items():
        if not isinstance(tier, str) or not tier:
            raise ProducedOutputError("template working_demands keys must be tier ids")
        try:
            kind = tiers_mod.tier_kind_of(tier)
        except Exception:
            kind = None
        if kind not in ("stage", "ram"):
            raise ProducedOutputError(
                f"template tier {tier!r} is not an authorized output kind "
                f"(stage/ram, never arc/pool)")
        if not isinstance(spec, Mapping) or set(spec) != {"minimum_gib", "window_gib"}:
            raise ProducedOutputError(
                f"template working_demands[{tier!r}] must carry minimum/window GiB")
        minimum = _nonneg_int(spec.get("minimum_gib"),
                              where=f"template working_demands[{tier}].minimum_gib")
        if write_only:
            window = _nonneg_int(
                spec.get("window_gib"),
                where=f"template working_demands[{tier}].window_gib")
            if minimum or window:
                raise ProducedOutputError(
                    f"template working_demands[{tier}] must be zero: a "
                    "write-only template reserves no stage window")
        else:
            window = _positive_int(
                spec.get("window_gib"),
                where=f"template working_demands[{tier}].window_gib")
        if minimum > window:
            raise ProducedOutputError(
                f"template working_demands[{tier}] minimum exceeds window")
        checked_demands[str(tier)] = {"minimum_gib": minimum, "window_gib": window}
    tiers = value.get("permitted_tiers")
    if not isinstance(tiers, list) or not tiers:
        raise ProducedOutputError("template permitted_tiers must be non-empty")
    checked_tiers = [str(t) for t in tiers]
    if len(set(checked_tiers)) != len(checked_tiers):
        raise ProducedOutputError("template permitted_tiers must not repeat a tier")
    if set(checked_tiers) != set(checked_demands):
        raise ProducedOutputError(
            "template permitted_tiers must equal the working_demands tiers")
    checked: dict[str, object] = {
        "schema": TEMPLATE_SCHEMA_V1,
        "version": 1,
        "template_id": template_id,
        "output_prefix": prefix,
        "slots": checked_slots,
        "durable_maxima": {
            "payload_max_bytes": payload,
            "checkpoint_max_bytes": checkpoint,
            "temp_max_bytes": temp,
        },
        "working_demands": checked_demands,
        "permitted_tiers": sorted(checked_tiers),
    }
    # Present only when true, so every read-back template -- all of them
    # before #912 -- keeps its canonical bytes and its `template_sha256`.
    if write_only:
        checked["write_only"] = True
    return checked


def is_write_only(template: Mapping[str, object]) -> bool:
    """Whether a template's batches commit at origin, never through a stage."""

    return bool(validate_template(template).get("write_only"))


def template_sha256(template: Mapping[str, object]) -> str:
    """Canonical identity of one sealed template."""

    checked = validate_template(template)
    raw = json.dumps(checked, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def template_path(queue_root: str | Path, template: Mapping[str, object]) -> Path:
    checked = validate_template(template)
    return (Path(queue_root) / "residency" / OUTPUT_TEMPLATES_SUBDIR
            / f"{checked['template_id']}.json")


def declare_template(queue_root: str | Path, template: Mapping[str, object]) -> Path:
    """File one template immutably; a conflicting body refuses."""

    from prismabuild import pool as pool_mod

    checked = validate_template(template)
    path = template_path(queue_root, checked)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(checked, sort_keys=True,
                     separators=(",", ":")).encode() + b"\n"
    try:
        pool_mod._publish_immutable(path, raw, where="produced-output template")
    except pool_mod.PoolContractError as exc:
        raise ProducedOutputError(
            f"a different template is already filed for {checked['template_id']}: {exc}"
        ) from None
    return path


def build_declaration(
    template: Mapping[str, object], template_input: Mapping[str, object]
) -> dict[str, object]:
    """The sealed action-params declaration for one validated template.

    Pure constructor owned here so the submitter (``pbrun``) and the core
    validator cannot drift: the template body is validated by
    :func:`validate_template`, the input row by the core input contract, and
    the digest is the canonical template identity. The file bytes stay in the
    CAS as an ordinary declared input; this declaration is the params half
    that makes the action key cover them.
    """

    from prismabuild.core import (
        PRODUCED_OUTPUT_DECLARATION_SCHEMA_V1,
        PRODUCED_OUTPUT_TEMPLATE_INPUT_ID,
        validate_input_contract,
    )

    checked = validate_template(template)
    inch = validate_input_contract(template_input)
    if inch["id"] != PRODUCED_OUTPUT_TEMPLATE_INPUT_ID:
        raise ProducedOutputError(
            "template input id must be "
            f"{PRODUCED_OUTPUT_TEMPLATE_INPUT_ID!r}")
    return {
        "schema": PRODUCED_OUTPUT_DECLARATION_SCHEMA_V1,
        "template_id": str(checked["template_id"]),
        "template_sha256": template_sha256(checked),
        "input": inch,
    }


def declared_template(queue, action_key: str) -> dict[str, object]:
    """Load the template this sealed action actually declared.

    Reads the queue item's ``produced_output`` ref (projected by
    ``PoolQueue.publish`` from the validated template whose working window
    the tier demand covers), then the filed immutable body, and verifies the
    digest. A caller-supplied template object is never trusted: an arbitrary
    template plus a real claim binds nothing. Looks in ``claimed/`` first
    (runtime) then ``ready/`` (submitter verification); anything else
    refuses.
    """

    from prismabuild import pool as pool_mod

    owner = _hex64(action_key, where="declared template action_key")
    item = pool_mod._read_json(queue.item_path(pool_mod.CLAIMED, owner))
    if item is None:
        item = pool_mod._read_json(queue.item_path(pool_mod.READY, owner))
    if not isinstance(item, Mapping):
        raise ProducedOutputError("no queued item for this action key")
    ref = item.get("produced_output")
    if not isinstance(ref, Mapping):
        raise ProducedOutputError("this action declares no produced-output template")
    if ref.get("schema") != PRODUCED_OUTPUT_REF_SCHEMA_V1:
        raise ProducedOutputError(
            f"produced-output ref schema must be {PRODUCED_OUTPUT_REF_SCHEMA_V1!r}")
    template_id = ref.get("template_id")
    digest = ref.get("template_sha256")
    if (not isinstance(template_id, str) or not template_id
            or "/" in template_id):
        raise ProducedOutputError("produced-output ref names no template")
    _hex64(digest, where="produced-output ref template_sha256")
    assert isinstance(digest, str)
    path = (Path(queue.root) / "residency" / OUTPUT_TEMPLATES_SUBDIR
            / f"{template_id}.json")
    # Bounded intake: filed bodies from this path are tiny envelopes; a
    # larger file at this name is foreign, never the declared template.
    try:
        from prismabuild.core import PRODUCED_OUTPUT_TEMPLATE_MAX_BYTES as _MAX

        with open(path, "rb") as handle:
            raw = handle.read(_MAX + 1)
    except OSError as exc:
        raise ProducedOutputError(f"undeclared-template: {exc}") from None
    if len(raw) > _MAX:
        raise ProducedOutputError(
            "tampered-template: filed body exceeds the template envelope")
    try:
        body = json.loads(raw.decode())
    except (UnicodeDecodeError, ValueError) as exc:
        raise ProducedOutputError(
            f"undeclared-template: filed body unreadable: {exc}") from None
    checked = validate_template(body)
    if (str(checked["template_id"]) != template_id
            or template_sha256(checked) != digest):
        raise ProducedOutputError(
            "tampered-template: filed body differs from the declared ref")
    return checked


def bind_declared_instance(
    queue, *, owner_action_key: str,
    claim_snapshot: Mapping[str, object],
    env: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Bind the runtime instance from the sealed declaration, not arguments.

    Loads :func:`declared_template` from the actual sealed queue item, then
    :func:`bind_instance` against the protected live claim + launch halves.
    There is no template argument to substitute, invent, or mismatch: a
    foreign, missing, or tampered declaration refuses before any byte is
    written.
    """

    template = declared_template(queue, owner_action_key)
    return bind_instance(
        queue, template, owner_action_key=owner_action_key,
        claim_snapshot=claim_snapshot, env=env)


# --------------------------------------------------------------------------
# Instances (runtime; bound to the live claim, never caller-supplied)
# --------------------------------------------------------------------------

def _broker_scope_id(action_key: str, nonce: str) -> str:
    """The scope id the broker issues for an action+nonce (existing rule).

    Same formula `ResourceScope._adopt_created_scope` enforces on every
    broker response: `prismabuild-job` + sha256(action+nonce)[:32] + `.slice`.
    Reused here as the control-identity check, never redefined.
    """

    return ("prismabuild-job"
            + hashlib.sha256((action_key + nonce).encode()).hexdigest()[:32]
            + ".slice")


def _launch_env(env: Mapping[str, str] | None) -> dict[str, str]:
    """Launch-bound identity halves (same names as PB730 `injected_context`).

    Read from the execution environment by default (the resource_exec proxy
    sets them from the exact launch identity); tests pass an explicit env
    mapping. Names resolve exactly as the candidate does: `core` constants
    where they exist, literal `PRISMABUILD_ACTION_*` otherwise (the NONCE /
    SCOPE names exist only in the PB730 lane on main).
    """

    import os as _os

    try:
        from prismabuild.core import ACTION_KEY_ENV as _KEY_ENV
    except ImportError:
        _KEY_ENV = "PRISMABUILD_ACTION_KEY"
    try:
        from prismabuild.core import ACTION_NONCE_ENV as _NONCE_ENV
    except ImportError:
        _NONCE_ENV = "PRISMABUILD_ACTION_NONCE"
    try:
        from prismabuild.core import ACTION_SCOPE_ENV as _SCOPE_ENV
    except ImportError:
        _SCOPE_ENV = "PRISMABUILD_ACTION_SCOPE"
    source = dict(_os.environ) if env is None else dict(env)
    return {"action_key": source.get(_KEY_ENV) or "",
            "nonce": source.get(_NONCE_ENV) or "",
            "scope_id": source.get(_SCOPE_ENV) or ""}


def bind_instance(queue, template: Mapping[str, object], *,
                  owner_action_key: str,
                  claim_snapshot: Mapping[str, object],
                  env: Mapping[str, str] | None = None) -> dict[str, object]:
    """Bind a runtime instance to launch + control identity; refuse all else.

    Requires BOTH halves complete and equal (the PB730 both-halves rule):
    launch env (`PRISMABUILD_ACTION_KEY/NONCE/SCOPE`) AND the live claim
    row's broker-issued `resource_scope` control (matching action_key,
    32-hex nonce, broker-formula scope_id). Refusals name the missing half
    (`no-launch-context` / `no-control-context`) or the mismatch
    (`attempt-superseded`); a missing/moved claim refuses
    (foreign/stale). The template must already be declared (byte-identical
    filed body) — an arbitrary caller template plus a real claim binds
    nothing. There is no intent fallback, no derived nonce, and no
    live-claim-only binding in production or fixture.
    """

    from prismabuild import pool as pool_mod

    checked = validate_template(template)
    owner = _hex64(owner_action_key, where="instance owner_action_key")
    if not isinstance(claim_snapshot, Mapping):
        raise ProducedOutputError("instance binding needs the claim snapshot")
    if claim_snapshot.get("action_key") != owner:
        raise ProducedOutputError("foreign claim snapshot: action key mismatch")
    live = pool_mod._read_json(queue.item_path(pool_mod.CLAIMED, owner))
    if live is None:
        raise ProducedOutputError("stale claim snapshot: no live claimed record")
    try:
        same = pool_mod._same_claim(live, claim_snapshot)
    except Exception as exc:
        raise ProducedOutputError(f"stale claim snapshot: {exc}") from None
    if not same:
        raise ProducedOutputError("stale claim snapshot: live claim moved on")
    # The template must be declared (filed, byte-identical) — an arbitrary
    # caller template plus a real claim binds nothing. (Attribution of the
    # declaration to this sealed owning action travels in the owner item
    # annotation; see the submission hook proposal.)
    canonical = json.dumps(checked, sort_keys=True,
                           separators=(",", ":")).encode() + b"\n"
    try:
        filed = template_path(queue.root, checked).read_bytes()
    except OSError as exc:
        raise ProducedOutputError(
            f"undeclared-template: {exc}") from None
    if filed != canonical:
        raise ProducedOutputError("undeclared-template: filed body differs")
    control = live.get("resource_scope")
    claim_nonce = claim_scope = ""
    if isinstance(control, Mapping):
        if control.get("action_key") == owner:
            candidate = control.get("nonce")
            if (isinstance(candidate, str) and len(candidate) == 32
                    and all(c in _HEX for c in candidate)):
                claim_nonce = candidate
            unit = control.get("scope_id")
            if isinstance(unit, str) and unit:
                claim_scope = unit
    if not claim_nonce or not claim_scope:
        raise ProducedOutputError("no-control-context: live claim names no "
                                  "complete broker-issued nonce/scope")
    if claim_scope != _broker_scope_id(owner, claim_nonce):
        raise ProducedOutputError("no-control-context: scope_id is not the "
                                  "broker-issued identity for this nonce")
    launch = _launch_env(env)
    if not launch["nonce"] or not launch["scope_id"]:
        raise ProducedOutputError("no-launch-context: launch env names no nonce/scope")
    if launch["action_key"] != owner:
        raise ProducedOutputError("foreign launch env: action key mismatch")
    if launch["nonce"] != claim_nonce or launch["scope_id"] != claim_scope:
        raise ProducedOutputError("attempt-superseded: launch and control disagree")
    return {
        "schema": INSTANCE_SCHEMA_V1,
        "version": 1,
        "template_id": str(checked["template_id"]),
        "template_sha256": template_sha256(checked),
        "owner_action_key": owner,
        "owner_attempt": {"nonce": claim_nonce, "scope_id": claim_scope},
        "attempt_source": "broker-launch",
        "output_prefix": str(checked["output_prefix"]),
        "bound_unix": time.time(),
    }


def validate_instance(value: object) -> dict[str, object]:
    """Check a bound instance (exact owner attempt, integer version)."""

    if not isinstance(value, Mapping):
        raise ProducedOutputError("a produced-output instance must be an object")
    allowed = frozenset({
        "schema", "version", "template_id", "template_sha256",
        "owner_action_key", "owner_attempt", "attempt_source",
        "output_prefix", "bound_unix",
    })
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ProducedOutputError(f"unknown instance fields: {unknown}")
    if value.get("schema") != INSTANCE_SCHEMA_V1:
        raise ProducedOutputError(f"instance schema must be {INSTANCE_SCHEMA_V1!r}")
    if type(value.get("version")) is not int or value.get("version") != 1:
        raise ProducedOutputError("instance version must be integer 1")
    template_id = _name(value.get("template_id"), where="instance template_id")
    digest = _hex64(value.get("template_sha256"), where="instance template_sha256")
    owner = _hex64(value.get("owner_action_key"), where="instance owner_action_key")
    attempt = value.get("owner_attempt")
    if not isinstance(attempt, Mapping) or set(attempt) != {"nonce", "scope_id"}:
        raise ProducedOutputError("instance owner_attempt must carry nonce + scope_id")
    nonce = _hex32(attempt.get("nonce"), where="instance owner_attempt.nonce")
    scope_id = _name(attempt.get("scope_id"), where="instance owner_attempt.scope_id")
    source = value.get("attempt_source")
    if source != "broker-launch":
        raise ProducedOutputError(
            "instance attempt_source must be broker-launch (both halves)")
    prefix = _abs_norm(value.get("output_prefix"), where="instance output_prefix")
    return {
        "schema": INSTANCE_SCHEMA_V1,
        "version": 1,
        "template_id": template_id,
        "template_sha256": digest,
        "owner_action_key": owner,
        "owner_attempt": {"nonce": nonce, "scope_id": scope_id},
        "attempt_source": str(source),
        "output_prefix": prefix,
        "bound_unix": value.get("bound_unix"),
    }


def _require_bound_contract(
    template: Mapping[str, object], instance: Mapping[str, object]
) -> tuple[dict[str, object], dict[str, object]]:
    """The single admitted-template boundary every mutation reuses.

    `admit_instance` established it: an instance is bound to exactly one
    sealed template, and no mutation may run a substituted larger
    `durable_maxima` (or any other template) under an admitted owner's
    name. Raises `ProducedOutputError("template-mismatch: ...")`; callers
    returning refusal dicts translate it to `{"ok": False, "refusal":
    "template-mismatch"}`.
    """

    checked_template = validate_template(template)
    checked_instance = validate_instance(instance)
    if (checked_instance["template_sha256"]
            != template_sha256(checked_template)):
        raise ProducedOutputError(
            "template-mismatch: instance bound to another template")
    return checked_template, checked_instance


def _require_live_owner(queue, checked_instance: Mapping[str, object]
                        ) -> dict[str, object] | None:
    """Refuse new writes from a stale or absent owner, or None to proceed.

    The live CLAIMED row for the owner key must name the instance's exact
    broker attempt (nonce + scope); a live row for another attempt means
    this instance is superseded (a retry owns the key now), and no live
    row means the owner is not running. Either way `require_prewrite`
    (whose SUCCESS authorizes the first payload write) and `commit_batch`
    (which consumes durable quota and moves tier tokens) must not run. A
    corrupt or unreadable live row is unknown state that retains rather
    than authorizing. The duplicate-commit replay path runs before this
    check (it mutates nothing); cleanup paths (abort, retire, release,
    reclaim) never call this: a stale reservation lands in its own
    superseded instance directory where it can neither consume quota
    nor move tokens, while freeing headroom must work after the owner
    is gone.
    """

    from prismabuild import pool as pool_mod

    owner = str(checked_instance["owner_action_key"])
    attempt = checked_instance["owner_attempt"]
    assert isinstance(attempt, dict)
    try:
        live = pool_mod._read_json(queue.item_path(pool_mod.CLAIMED, owner))
    except Exception as exc:
        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    if live is None:
        return {"ok": False, "refusal": "owner-not-running"}
    if not isinstance(live, Mapping):
        return {"ok": False, "refusal": "unknown-retain: claim-shape"}
    control = live.get("resource_scope")
    live_nonce = live_scope = ""
    if isinstance(control, Mapping):
        candidate = control.get("nonce")
        if isinstance(candidate, str) and candidate:
            live_nonce = candidate
        for field in ("scope_id", "scope_unit", "unit"):
            unit = control.get(field)
            if isinstance(unit, str) and unit:
                live_scope = unit
                break
    if (live_nonce == attempt["nonce"] and live_scope == attempt["scope_id"]):
        return None
    return {"ok": False, "refusal": "stale-superseded-owner"}


def instance_namespace(instance: Mapping[str, object]) -> str:
    """Material/ledger namespace for the instance (NOT the owner key)."""

    checked = validate_instance(instance)
    attempt = checked["owner_attempt"]
    assert isinstance(attempt, dict)
    raw = ("produced-output-instance:" + str(checked["template_sha256"]) + ":"
           + str(checked["owner_action_key"]) + ":" + str(attempt["nonce"])
           + ":" + str(attempt["scope_id"]))
    return hashlib.sha256(raw.encode()).hexdigest()


def instance_dir(queue_root: str | Path, instance: Mapping[str, object]) -> Path:
    checked = validate_instance(instance)
    attempt = checked["owner_attempt"]
    assert isinstance(attempt, dict)
    return (Path(queue_root) / "residency" / OUTPUT_SCOPES_SUBDIR
            / str(checked["owner_action_key"])
            / f"{checked['template_id']}.{attempt['nonce']}")


def declare_instance(queue_root: str | Path, instance: Mapping[str, object]) -> Path:
    """File one bound instance immutably; a conflicting body refuses."""

    from prismabuild import pool as pool_mod

    checked = validate_instance(instance)
    attempt = checked["owner_attempt"]
    assert isinstance(attempt, dict)
    # Indexed BEFORE the instance exists (#1053): an attempt that can
    # prewrite is always one another action's prewrite gate can find.
    _index_attempt(queue_root, str(checked["owner_action_key"]),
                   str(checked["template_id"]), str(attempt["nonce"]))
    directory = instance_dir(queue_root, checked)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "instance.json"
    raw = json.dumps(checked, sort_keys=True,
                     separators=(",", ":")).encode() + b"\n"
    try:
        pool_mod._publish_immutable(path, raw, where="produced-output instance")
    except pool_mod.PoolContractError as exc:
        raise ProducedOutputError(
            f"a different instance is already filed for this owner/template/nonce: {exc}"
        ) from None
    return path


# --------------------------------------------------------------------------
# Origin-path owners across attempts and actions (#1053)
# --------------------------------------------------------------------------
#
# A path under a template's output prefix can be named by any attempt of any
# template whose prefix contains it or is contained by it: a retry of the
# same key, or another action entirely (R13's relaunch was a new key on the
# same template).  These helpers find those attempts through the attempt
# index, bounded by the templates whose prefixes overlap, and read what each
# one names.  They never list every owner's scopes, except before the index
# is complete.

#: Each filed template's id and output prefix by ``(file path, inode)``, or
#: None for a file that names no prefix (`_filed_templates`).  A filed
#: template is immutable (`declare_template`), so a file is read once per
#: process.
_TEMPLATE_BODIES: dict[tuple[str, int], dict[str, object] | None] = {}
#: ``os.path.realpath`` of each output prefix, once per process.
_PREFIX_REALPATHS: dict[str, str] = {}


def _attempts_root(queue_root: str | Path) -> Path:
    return (Path(queue_root) / "residency" / OUTPUT_TEMPLATES_SUBDIR
            / OUTPUT_TEMPLATE_ATTEMPTS_SUBDIR)


def _index_attempt(queue_root: str | Path, owner_action_key: str,
                   template_id: str, nonce: str) -> None:
    """File one attempt's pointer, immutably and idempotently.

    Raises `ProducedOutputError` when it cannot be filed, or when a
    different body is already filed under the name.
    """

    from prismabuild import pool as pool_mod

    owner = _hex64(owner_action_key, where="attempt owner_action_key")
    template = _name(template_id, where="attempt template_id")
    attempt = _hex32(nonce, where="attempt nonce")
    directory = _attempts_root(queue_root) / template
    raw = (json.dumps({"owner_action_key": owner, "nonce": attempt,
                       "template_id": template},
                      sort_keys=True, separators=(",", ":")) + "\n").encode()
    try:
        directory.mkdir(parents=True, exist_ok=True)
        pool_mod._publish_immutable(directory / f"{owner}.{attempt}", raw,
                                    where="produced-output attempt index")
    except (OSError, pool_mod.PoolContractError) as exc:
        raise ProducedOutputError(
            f"unknown-retain: attempt index: {exc}") from None


def _mark_attempt_index_complete(queue_root: str | Path) -> None:
    from prismabuild import pool as pool_mod

    root = _attempts_root(queue_root)
    try:
        root.mkdir(parents=True, exist_ok=True)
        pool_mod._publish_immutable(root / ATTEMPT_INDEX_COMPLETE, b"1\n",
                                    where="produced-output attempt index")
    except (OSError, pool_mod.PoolContractError) as exc:
        raise ProducedOutputError(
            f"unknown-retain: attempt index: {exc}") from None


def _filed_templates(queue_root: str | Path) -> dict[str, dict[str, object]]:
    """Every filed template's id and output prefix, by id.

    One listing of the templates directory; a file not seen before is read
    once. Only ``template_id`` and ``output_prefix`` are read, not the whole
    schema: a template filed by an older or a newer PB still names the
    prefix its attempts write under, and that is all an owner lookup needs.
    A file with no absolute ``output_prefix`` -- not JSON, not an object,
    or without one -- is not a template `declare_template` filed, and no
    attempt can have been bound to it (`bind_instance` validates the
    template), so it names no paths and is skipped. A file that cannot be
    read raises `ProducedOutputError`: whether it covers a path is unknown,
    and an answer that leaves it out could miss an owner.
    """

    directory = Path(queue_root) / "residency" / OUTPUT_TEMPLATES_SUBDIR
    try:
        with os.scandir(directory) as iterator:
            listed = sorted((entry.name, entry.inode()) for entry in iterator
                            if entry.name.endswith(".json")
                            and not entry.name.startswith("."))
    except FileNotFoundError:
        return {}
    except OSError as exc:
        raise ProducedOutputError(f"unknown-retain: templates: {exc}") from None
    found: dict[str, dict[str, object]] = {}
    for name, inode in listed:
        key = (str(directory / name), inode)
        if key not in _TEMPLATE_BODIES:
            try:
                raw = (directory / name).read_text()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise ProducedOutputError(
                    f"unknown-retain: template {name}: {exc}") from None
            try:
                body = json.loads(raw)
            except ValueError:
                body = None
            prefix = body.get("output_prefix") if isinstance(body, Mapping) else None
            if not isinstance(prefix, str) or not os.path.isabs(prefix):
                _TEMPLATE_BODIES[key] = None
            else:
                named = body.get("template_id")
                _TEMPLATE_BODIES[key] = {
                    "template_id": (named if isinstance(named, str) and named
                                    else name[:-len(".json")]),
                    "output_prefix": prefix}
        body = _TEMPLATE_BODIES[key]
        if body is not None:
            found[str(body["template_id"])] = body
    return found


def _resolved_prefix(prefix: str) -> str:
    resolved = _PREFIX_REALPATHS.get(prefix)
    if resolved is None:
        resolved = os.path.realpath(prefix)
        _PREFIX_REALPATHS[prefix] = resolved
    return resolved


def _prefixes_overlap(first: str, second: str) -> bool:
    """Whether one output prefix contains the other, symlinks resolved."""

    a, b = _resolved_prefix(first), _resolved_prefix(second)
    common = os.path.commonpath([a, b])
    return common == a or common == b


def _overlapping_template_ids(queue_root: str | Path,
                              template: Mapping[str, object]) -> list[str]:
    """This template's id and every filed template whose prefix overlaps it."""

    own = str(template["output_prefix"])
    ids = {str(template["template_id"])}
    for template_id, body in _filed_templates(queue_root).items():
        if _prefixes_overlap(own, str(body["output_prefix"])):
            ids.add(template_id)
    return sorted(ids)


def _template_attempts(queue_root: str | Path, template_ids: Collection[str]
                       ) -> list[tuple[str, str, str]]:
    """``(owner, template_id, nonce)`` for every attempt of these templates.

    From the attempt index once a tick has marked it complete: one listing
    per template. Before that, from the scopes themselves: one listing of
    the owners and one per owner. A listing that fails raises
    `ProducedOutputError`, never an empty answer.
    """

    wanted = set(template_ids)
    found: set[tuple[str, str, str]] = set()
    root = _attempts_root(queue_root)
    if (root / ATTEMPT_INDEX_COMPLETE).exists():
        for template_id in sorted(wanted):
            try:
                names = os.listdir(root / template_id)
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise ProducedOutputError(
                    f"unknown-retain: attempt index: {exc}") from None
            for name in names:
                owner, dot, nonce = name.partition(".")
                if (dot and len(owner) == 64 and len(nonce) == _HEX32
                        and not name.startswith(".")):
                    found.add((owner, template_id, nonce))
        return sorted(found)
    scopes_root = Path(queue_root) / "residency" / OUTPUT_SCOPES_SUBDIR
    try:
        owners = _scope_owners(scopes_root)
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise ProducedOutputError(f"unknown-retain: scopes: {exc}") from None
    for owner in owners:
        try:
            names = os.listdir(scopes_root / owner)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise ProducedOutputError(
                f"unknown-retain: scopes {owner[:12]}: {exc}") from None
        for name in names:
            template_id, dot, nonce = name.rpartition(".")
            if dot and template_id in wanted and len(nonce) == _HEX32:
                found.add((owner, template_id, nonce))
    return sorted(found)


def _attempt_scope(queue_root: str | Path, owner: str, template_id: str,
                   nonce: str) -> Path:
    return (Path(queue_root) / "residency" / OUTPUT_SCOPES_SUBDIR / owner
            / f"{template_id}.{nonce}")


def _attempt_owned_paths(queue_root: str | Path, scope: Path,
                         batches: Mapping[str, object]
                         ) -> tuple[dict[str, str], dict[str, str]]:
    """``(committed, prewritten)``: the origin paths one attempt names.

    Each maps a path to a batch id. ``committed`` holds every path of a
    committed batch whose origin is not reclaimed, staged or origin-only,
    retired or not: the commit recorded that file as the batch's.
    ``prewritten`` holds every path of a prewrite record still filed, which
    is permission to write it. ``batches`` is the attempt's commitments.
    Raises `ProducedOutputError` on a record that cannot be read.
    """

    committed: dict[str, str] = {}
    for batch_id, entry in sorted(batches.items()):
        if not isinstance(entry, Mapping):
            raise ProducedOutputError(
                f"unknown-retain: {scope.name}: bad committed batch {batch_id!r}")
        if entry.get("origin_reclaimed"):
            continue
        indexed = entry.get("paths")
        if isinstance(indexed, list):
            paths = [str(item) for item in indexed]
        else:
            # An entry that predates the index: its immutable record.
            try:
                instance = validate_instance(json.loads(
                    (scope / "instance.json").read_text()))
                template = validate_template(json.loads(
                    (Path(queue_root) / "residency" / OUTPUT_TEMPLATES_SUBDIR
                     / f"{instance['template_id']}.json").read_text()))
            except (OSError, ValueError) as exc:
                raise ProducedOutputError(
                    f"unknown-retain: {scope.name}: {exc}") from None
            _filed, sealed = _load_batch_record(
                queue_root, instance, template, entry, batch_id)
            paths = [str(desc["path"]) for desc in sealed]
        for path in paths:
            committed.setdefault(path, batch_id)
    prewritten: dict[str, str] = {}
    directory = scope / "prewrites"
    try:
        names = sorted(name for name in os.listdir(directory)
                       if name.endswith(".prewrite.json"))
    except FileNotFoundError:
        names = []
    except OSError as exc:
        raise ProducedOutputError(
            f"unknown-retain: {scope.name}: {exc}") from None
    for name in names:
        batch_id = name[:-len(".prewrite.json")]
        record = _read_prewrite(directory / name)
        if record is None:
            continue
        reserved = record.get("paths")
        if not isinstance(reserved, list):
            raise ProducedOutputError(
                f"unknown-retain: {scope.name}: bad prewrite record {batch_id!r}")
        for path in reserved:
            prewritten.setdefault(str(path), batch_id)
    return committed, prewritten


def _claimed_nonce(queue, owner: str) -> str | None:
    """The nonce of the attempt holding ``owner``'s claim.

    ``""`` when nothing holds it, ``None`` when the claim cannot be read or
    names no attempt: unknown, which the gate treats as live.
    """

    from prismabuild import pool as pool_mod

    try:
        live = pool_mod._read_json(queue.item_path(pool_mod.CLAIMED, owner))
    except Exception:
        return None
    if live is None:
        return ""
    control = live.get("resource_scope") if isinstance(live, Mapping) else None
    nonce = control.get("nonce") if isinstance(control, Mapping) else None
    return nonce if isinstance(nonce, str) and nonce else None


def _foreign_live_path_owner(queue, checked_instance: Mapping[str, object],
                             checked_template: Mapping[str, object],
                             planned_paths: Sequence[str]
                             ) -> dict[str, object] | None:
    """Another action's live attempt that owns one of these paths (#1053).

    The cross-action half of `_live_path_owner`. The attempts it reads are
    those of every template whose output prefix overlaps this one's
    (`_template_attempts`), less this owner key's: only one attempt of a key
    holds its claim, and this one does. Each other key's claim is read once.
    An attempt that does not hold its key's claim cannot prewrite or commit
    (`_require_live_owner`), so it owns nothing a new write could disturb,
    and its records are not read at all: a dead attempt's commitments cost
    the gate nothing. A live attempt, or one whose claim cannot be read,
    owns every path its committed batches name (unless reclaimed) and every
    path a prewrite record of it names (`_attempt_owned_paths`).

    Returns the refusal's details for the first owned path in sorted order,
    or ``None``. Raises `ProducedOutputError` when an attempt that may be
    live cannot be read.
    """

    wanted = set(planned_paths)
    own = str(checked_instance["owner_action_key"])
    attempts = _template_attempts(
        queue.root, _overlapping_template_ids(queue.root, checked_template))
    claims: dict[str, str | None] = {}
    for owner, template_id, nonce in attempts:
        if owner == own:
            continue
        if owner not in claims:
            claims[owner] = _claimed_nonce(queue, owner)
        claimed = claims[owner]
        if claimed == "" or (claimed is not None and claimed != nonce):
            continue
        scope = _attempt_scope(queue.root, owner, template_id, nonce)
        batches = _read_commitments(scope / "commitments.json")["batches"]
        assert isinstance(batches, Mapping)
        committed, prewritten = _attempt_owned_paths(queue.root, scope, batches)
        for path in sorted(wanted):
            for kind, owned in (("batch", committed), ("prewrite", prewritten)):
                if path in owned:
                    return {"path": path, "owner_action_key": owner,
                            "owner_nonce": nonce, "owner_kind": kind,
                            "owner_batch_id": owned[path],
                            "owner_state": "live" if claimed else "unknown"}
    return None


def _file_version(path: Path | str) -> tuple[int, ...] | None:
    """``(device, inode, size, mtime, ctime)``, or ``None`` when absent.

    Any other failure to ``stat`` raises `ProducedOutputError`.
    """

    try:
        info = os.stat(path)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ProducedOutputError(f"unknown-retain: {exc}") from None
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            int(getattr(info, "st_ctime_ns", 0)))


class _TickReads:
    """What one origin-retirement tick has read, so it reads each thing once.

    Commitments are kept by the file's version (`_file_version`) and handed
    back while a fresh ``stat`` returns the same version: every writer
    replaces the file by rename (`_write_commitments`), so a changed file
    has another version. The ``stat`` comes before the read, so a file
    replaced between the two is read again next time, never remembered
    under the new version with the old bytes. What an attempt owns
    (`_attempt_owned_paths`) is kept the same way, under the versions of
    its commitments and of each prewrite record it has. An owner key's
    generation (`_key_generation`) and its funding census are read at most
    once per tick.
    """

    def __init__(self, queue) -> None:
        self.queue = queue
        self._commitments: dict[str, tuple[tuple[int, ...], dict[str, object]]] = {}
        self._owned: dict[str, tuple[object, dict[str, str], dict[str, str]]] = {}
        self._generations: dict[str, tuple[str, dict[str, object] | None]] = {}
        self._census: dict[str, tuple[list[dict], bool]] = {}
        self._templates: dict[str, dict[str, object]] | None = None

    def batches(self, scope: Path) -> dict[str, object]:
        path = scope / "commitments.json"
        version = _file_version(path)
        kept = self._commitments.get(str(path))
        if version is not None and kept is not None and kept[0] == version:
            return kept[1]
        batches = _read_commitments(path)["batches"]
        assert isinstance(batches, dict)
        if version is not None:
            self._commitments[str(path)] = (version, batches)
        return batches

    def owned(self, scope: Path
              ) -> tuple[object, dict[str, str], dict[str, str]]:
        """``(fingerprint, committed, prewritten)`` for one attempt.

        `_attempt_owned_paths`, re-derived only when the fingerprint -- the
        versions of the commitments and of each prewrite record, taken
        before either is read -- has changed.
        """

        directory = scope / "prewrites"
        try:
            names = sorted(name for name in os.listdir(directory)
                           if name.endswith(".prewrite.json"))
        except FileNotFoundError:
            names = []
        except OSError as exc:
            raise ProducedOutputError(
                f"unknown-retain: {scope.name}: {exc}") from None
        fingerprint = (_file_version(scope / "commitments.json"),
                       tuple((name, _file_version(directory / name))
                             for name in names))
        kept = self._owned.get(str(scope))
        if kept is not None and kept[0] == fingerprint:
            return kept
        committed, prewritten = _attempt_owned_paths(
            self.queue.root, scope, self.batches(scope))
        self._owned[str(scope)] = (fingerprint, committed, prewritten)
        return fingerprint, committed, prewritten

    def generation(self, owner: str) -> tuple[str, dict[str, object] | None]:
        if owner not in self._generations:
            self._generations[owner] = _key_generation(self.queue, owner)
        return self._generations[owner]

    def census(self, owner: str) -> tuple[list[dict], bool]:
        if owner not in self._census:
            try:
                self._census[owner] = self.queue.output_census_for_owner(owner)
            except Exception:
                self._census[owner] = ([], True)
        return self._census[owner]

    def templates(self) -> dict[str, dict[str, object]]:
        if self._templates is None:
            self._templates = _filed_templates(self.queue.root)
        return self._templates

    def path_owners(self, instance: Mapping[str, object],
                    template: Mapping[str, object]) -> "_PathOwners":
        """Every other attempt's claim on paths under this template's prefix.

        The attempts are those of every template whose output prefix
        overlaps this one's (`_template_attempts`), less this instance's
        own. Read now -- the index, and each attempt's commitments and
        prewrite records through this tick's versions -- so a caller under
        the output-prefix lock sees every commit and prewrite filed under it
        before the lock was taken.
        """

        own_template = str(template["template_id"])
        ids = {own_template}
        for template_id, body in self.templates().items():
            if _prefixes_overlap(str(template["output_prefix"]),
                                 str(body["output_prefix"])):
                ids.add(template_id)
        attempt = instance["owner_attempt"]
        assert isinstance(attempt, dict)
        mine = (str(instance["owner_action_key"]), own_template,
                str(attempt["nonce"]))
        owners = _PathOwners()
        for owner, template_id, nonce in _template_attempts(
                self.queue.root, sorted(ids)):
            if (owner, template_id, nonce) == mine:
                continue
            scope = _attempt_scope(self.queue.root, owner, template_id, nonce)
            version, committed, prewritten = self.owned(scope)
            if not committed and not prewritten:
                continue
            state = (_attempt_state(self.generation(owner), nonce)
                     if prewritten else "")
            unknown = None
            if state == "unknown":
                # Why, for the report a hold by it files (#1065). Whether it
                # is orphaned turns with the clock, so it is part of the
                # fingerprint: a kept decision is taken again when it turns.
                unknown = {**_unknown_attempt_detail(
                    self.generation(owner), nonce,
                    last_write_ns=_newest_write_ns(version),
                    now=time.time()), "template_id": template_id}
            owners.add(owner, nonce, foreign=owner != mine[0],
                       committed=committed, prewritten=prewritten,
                       state=state, unknown=unknown)
            owners.fingerprint.append((
                owner, template_id, nonce, version, state,
                None if unknown is None
                else (unknown["why"], unknown["orphaned"])))
        return owners


class _PathOwners:
    """Who else names an origin path, per attempt, asked one path at a time.

    A path is *committed* by another attempt when its commit names it (the
    commit recorded that file), or when a prewrite of an attempt that
    succeeded names it. It is *pending* when a prewrite of an attempt that
    can still commit names it: live, or in a state that cannot be read. A
    dead attempt's prewrite names nothing: it can never commit.
    """

    def __init__(self) -> None:
        self._attempts: list[tuple[str, str, bool, dict[str, str],
                                   dict[str, str], str]] = []
        #: Why each attempt whose state is ``unknown`` reads so (#1065), by
        #: ``(owner, nonce)``: ``{"why", "orphaned", "last_write_unix",
        #: "template_id"}`` (`_unknown_attempt_detail`).
        self._unknown: dict[tuple[str, str], dict[str, object]] = {}
        #: What the answers were read from: each attempt that names a path,
        #: its records' versions and its state. Equal fingerprints give
        #: equal answers.
        self.fingerprint: list[tuple[object, ...]] = []

    def add(self, owner: str, nonce: str, *, foreign: bool,
            committed: dict[str, str], prewritten: dict[str, str],
            state: str, unknown: Mapping[str, object] | None = None) -> None:
        self._attempts.append((owner, nonce, foreign, committed, prewritten,
                               state))
        if unknown is not None:
            self._unknown[(owner, nonce)] = dict(unknown)

    @staticmethod
    def _note(owner: str, nonce: str, batch_id: str,
              foreign: bool) -> dict[str, str]:
        # The #949 shape for a retry of the same key; another key is named.
        note = {"nonce": nonce, "batch_id": batch_id}
        if foreign:
            note["owner_action_key"] = owner
        return note

    def committed(self, path: str) -> dict[str, str] | None:
        for owner, nonce, foreign, committed, prewritten, state in self._attempts:
            batch_id = committed.get(path)
            if batch_id is None and state == "succeeded":
                batch_id = prewritten.get(path)
            if batch_id is not None:
                return self._note(owner, nonce, batch_id, foreign)
        return None

    def pending(self, path: str) -> dict[str, str] | None:
        for owner, nonce, foreign, _committed, prewritten, state in self._attempts:
            if state in _ENDED_ATTEMPT_STATES:
                continue
            batch_id = prewritten.get(path)
            if batch_id is not None:
                return self._note(owner, nonce, batch_id, foreign)
        return None

    def unknown_holders(self, path: str) -> list[dict[str, object]]:
        """Each attempt holding ``path`` whose state could not be read (#1065).

        `pending` holds a path for an attempt that is ``live`` or
        ``unknown``; a live one is a writer at work and holds quietly. An
        unknown one -- no queue row, a row that is queued or being moved, a
        row that cannot be read -- may never commit or end, so a hold it
        causes is reported: who holds it (``owner_action_key``, ``nonce``,
        ``batch_id``, ``foreign``), ``why`` its state is unknown and whether
        it is ``orphaned`` (`_unknown_attempt_detail`).
        """

        found: list[dict[str, object]] = []
        for owner, nonce, foreign, _committed, prewritten, state in self._attempts:
            if state != "unknown":
                continue
            batch_id = prewritten.get(path)
            if batch_id is None:
                continue
            detail = self._unknown.get((owner, nonce), {})
            found.append({"path": path, "owner_action_key": owner,
                          "nonce": nonce, "batch_id": batch_id,
                          "foreign": foreign, "state": state, **detail})
        return found


#: The event a hold by an attempt whose state is unknown files (#1065), once
#: per change. A live attempt's hold stays quiet: it will commit or end.
ORIGIN_HELD_BY_UNKNOWN_EVENT = "output-origin-held-by-unknown-attempt"
#: What frees a hold by an orphaned attempt (#1065). PB never ends an
#: attempt it cannot read, so an operator does.
ORPHANED_HOLDER_REMEDY = (
    "no queue row names this attempt, so nothing will end it: confirm no "
    "process of it is running, then remove its prewrite record "
    "(`holders[].prewrite_record`); the next tier cycle decides the held "
    "batch again from the files that are there")


def _unknown_attempt_detail(generation: tuple[str, dict[str, object] | None],
                            nonce: str, *, last_write_ns: int | None,
                            now: float) -> dict[str, object]:
    """Why `_attempt_state` reads ``unknown`` for this attempt (#1065).

    ``why`` is one of ``no-queue-row`` (no record of the key in any queue
    directory), ``queued`` (a ``ready`` row: the key waits for a claim),
    ``moving`` (a claim being moved), ``claim-names-no-nonce``,
    ``done-names-no-nonce``, ``done-status-<status>``,
    ``cache-hit-by-another-attempt`` or ``queue-row-unreadable``.

    ``orphaned`` is true for ``no-queue-row`` once the attempt's newest
    record (its commitments or a prewrite record, ``last_write_ns``) is
    older than the lease timeout (`pool.LEASE_TIMEOUT_S`). A running attempt
    holds a claimed row that its heartbeat renews within that timeout, so
    an attempt with no row that wrote nothing for as long cannot be one.
    The hold it causes stays in place; only the report says so.
    """

    from prismabuild import pool as pool_mod

    state, record = generation
    if state == "absent":
        why = "no-queue-row"
    elif state == pool_mod.READY:
        why = "queued"
    elif state == "moving":
        why = "moving"
    elif state == pool_mod.CLAIMED:
        why = "claim-names-no-nonce"
    elif state == pool_mod.DONE and record is not None:
        status = str(record.get("status"))
        if status not in ("executed", "cache_hit"):
            why = f"done-status-{status}"
        elif not _record_nonce(record):
            why = "done-names-no-nonce"
        else:
            why = "cache-hit-by-another-attempt"
    else:
        why = "queue-row-unreadable"
    last_write = (None if last_write_ns is None
                  else round(last_write_ns / 1e9, 3))
    orphaned = (why == "no-queue-row" and last_write is not None
                and now - last_write > pool_mod.LEASE_TIMEOUT_S)
    return {"why": why, "orphaned": orphaned, "last_write_unix": last_write}


def _newest_write_ns(version: object) -> int | None:
    """The newest mtime in a `_TickReads.owned` fingerprint, or None."""

    newest: int | None = None

    def visit(item: object) -> None:
        nonlocal newest
        if (isinstance(item, tuple) and len(item) == 5
                and all(isinstance(part, int) for part in item)):
            newest = item[3] if newest is None else max(newest, item[3])
            return
        if isinstance(item, tuple):
            for part in item:
                visit(part)

    visit(version)
    return newest


def _held_by_unknown_event(holders: Sequence[Mapping[str, object]],
                           queue_root: str | Path) -> dict[str, object]:
    """An `ORIGIN_HELD_BY_UNKNOWN_EVENT`'s body, without its subject (#1065).

    One entry per holding attempt and path, each with the attempt's
    ``prewrite_record`` (the record whose removal frees an orphaned hold),
    and ``reason``: ``held-by-orphaned-attempt`` when any holder is
    orphaned, else ``held-by-unknown-attempt``, and the ``remedy`` when one
    is. ``last_write_unix`` moves only when the holder writes a record, so
    the event, and the signature it is reported once per change by, stays
    the same while nothing changes.
    """

    listed: list[dict[str, object]] = []
    for holder in holders:
        item = dict(holder)
        template_id = str(item.pop("template_id", "") or "")
        if template_id:
            item["prewrite_record"] = str(
                _attempt_scope(queue_root, str(item["owner_action_key"]),
                               template_id, str(item["nonce"]))
                / "prewrites" / f"{item['batch_id']}.prewrite.json")
        listed.append(item)
    listed.sort(key=lambda item: (str(item["path"]),
                                  str(item["owner_action_key"]),
                                  str(item["nonce"])))
    orphaned = any(item.get("orphaned") for item in listed)
    event: dict[str, object] = {
        "reason": ("held-by-orphaned-attempt" if orphaned
                   else "held-by-unknown-attempt"),
        "holders": listed}
    if orphaned:
        event["remedy"] = ORPHANED_HOLDER_REMEDY
    return event


def _unknown_path_holders(owners: _PathOwners, paths: Sequence[object]
                          ) -> list[dict[str, object]]:
    """The retirement's hold test over ``paths``, for the listing (#1065).

    Empty when no path another attempt can still commit holds is held by an
    attempt whose state is unknown; a path already committed by another
    attempt holds nothing, as in the retirement.
    """

    found: list[dict[str, object]] = []
    for path in sorted(str(item) for item in paths):
        if owners.committed(path) is None and owners.pending(path) is not None:
            found.extend(owners.unknown_holders(path))
    return found


# --------------------------------------------------------------------------
# Descriptors (class-enforcing, prefix-realpath-checked)
# --------------------------------------------------------------------------

def _resolve_contained(prefix: str, path: str, *, where: str) -> str:
    """Realpath containment: string prefix alone never proves identity.

    Both sides go through `os.path.realpath` (symlinks resolved); containment
    is `os.path.commonpath`. Callers additionally hold the output-prefix
    ownership lock across the check-and-act so a symlink swap between check
    and use orders against retirement instead of racing it.
    """

    real_prefix = os.path.realpath(prefix)
    real_path = os.path.realpath(path)
    try:
        common = os.path.commonpath([real_prefix, real_path])
    except ValueError as exc:
        raise ProducedOutputError(f"{where} is not under the scope prefix: {exc}")
    if common != real_prefix:
        raise ProducedOutputError(f"{where} escapes the scope prefix")
    return real_path


def validate_descriptor(value: object, template: Mapping[str, object],
                        instance: Mapping[str, object]) -> dict[str, object]:
    """Check one descriptor against the sealed template + bound instance."""

    checked_template = validate_template(template)
    checked_instance = validate_instance(instance)
    if checked_instance["template_sha256"] != template_sha256(checked_template):
        raise ProducedOutputError("descriptor instance names another template")
    if not isinstance(value, Mapping):
        raise ProducedOutputError("a descriptor must be an object")
    allowed = frozenset({
        "schema", "slot", "artifact_class", "path", "bytes", "sha256",
        "producer_generation", "owner_action_key", "owner_attempt",
    })
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ProducedOutputError(f"unknown descriptor fields: {unknown}")
    if value.get("schema") != DESCRIPTOR_SCHEMA_V2:
        raise ProducedOutputError(
            f"descriptor schema must be {DESCRIPTOR_SCHEMA_V2!r}")
    slot = _name(value.get("slot"), where="descriptor slot")
    slot_spec = checked_template["slots"].get(slot)
    if slot_spec is None:
        raise ProducedOutputError(
            f"descriptor slot {slot!r} is not in the template's authorized slots")
    slot_class = slot_spec.get("class") if isinstance(slot_spec, Mapping) else slot_spec
    cls = value.get("artifact_class")
    if cls != slot_class:
        raise ProducedOutputError(
            f"descriptor class {cls!r} disagrees with slot {slot!r} class "
            f"{slot_class!r}")
    raw_path = value.get("path")
    _abs_norm(raw_path, where="descriptor path")
    assert isinstance(raw_path, str)
    _resolve_contained(str(checked_template["output_prefix"]), raw_path,
                       where="descriptor path")
    size = _positive_int(value.get("bytes"), where="descriptor bytes")
    # The existing DEV data-manifest convention: a descriptor digest is
    # either a real hex64 (verified, and the mover enforces it on copy) or
    # JSON null (DEV path -- no payload hashing prerequisite; identity is
    # path+size+order through the manifest digest over the descriptor list,
    # plus the mover's necessary-copy/material evidence). Anything else --
    # including the string "None" or any non-hex spelling -- refuses; a
    # null is never coerced into a digest and a supplied digest is never
    # weakened.
    raw_digest = value.get("sha256")
    if raw_digest is None:
        digest = None
    else:
        digest = _hex64(raw_digest, where="descriptor sha256")
    generation = value.get("producer_generation")
    if not isinstance(generation, str) or not generation or "/" in generation:
        raise ProducedOutputError(
            "descriptor producer_generation must be a non-empty name")
    if value.get("owner_action_key") != checked_instance["owner_action_key"]:
        raise ProducedOutputError("descriptor owner must equal the instance owner")
    attempt = value.get("owner_attempt")
    if not isinstance(attempt, Mapping) or dict(attempt) != dict(
            checked_instance["owner_attempt"]):
        raise ProducedOutputError("descriptor attempt must equal the instance attempt")
    maxima = checked_instance_maxima(checked_template)
    cap = {"payload": maxima["payload_max_bytes"], "checkpoint": maxima[
        "checkpoint_max_bytes"], "temp": maxima["temp_max_bytes"]}[str(cls)]
    if size > cap:
        raise ProducedOutputError(
            f"descriptor bytes exceed the {cls} durable maxima")
    return {
        "schema": DESCRIPTOR_SCHEMA_V2,
        "slot": slot,
        "artifact_class": str(cls),
        "path": os.path.normpath(raw_path),
        "bytes": size,
        "sha256": digest,
        "producer_generation": generation,
        "owner_action_key": str(checked_instance["owner_action_key"]),
        "owner_attempt": dict(checked_instance["owner_attempt"]),
    }


# --------------------------------------------------------------------------
# Manifest object set (ours) vs pin serialization (PB730's)
# --------------------------------------------------------------------------

def manifest_object_set_id(
    expected: Mapping[str, Mapping[str, object]],
) -> str:
    """Canonical object-set identity for a batch window (manifest side).

    Sorted `key:bytes:sha256-or-null`, `|`-joined, sha256 hex. This names the
    manifest's object set for the `expected` argument. PIN serialization is
    PB730-owned: the corrected `pin_id_for` must include this set plus
    material generations; this helper never serializes a pin.
    """

    parts = []
    for key in sorted(expected):
        spec = expected[key]
        if not isinstance(spec, Mapping):
            raise ProducedOutputError("expected specs must be objects")
        size = spec.get("bytes")
        if type(size) is not int or size <= 0:
            raise ProducedOutputError("expected bytes must be positive")
        digest = spec.get("sha256")
        if digest is not None and not (
                isinstance(digest, str) and len(digest) == 64
                and all(c in _HEX for c in digest)):
            raise ProducedOutputError("expected sha256 must be hex or null")
        parts.append(f"{key}:{size}:{digest}")
    return hashlib.sha256("|".join(parts).encode()).hexdigest()


def canonical_expected_id(expected: Mapping[str, Mapping[str, object]]) -> str:
    """Deprecated alias of `manifest_object_set_id` (kept for history)."""

    return manifest_object_set_id(expected)


def label_span_for_manifest(total_bytes: int,
                            coordinate_space: str = "output-manifest"
                            ) -> dict[str, object]:
    """Label-source span for a batch window (never a source-file span).

    `coordinate_space` names what `[start_bytes, end_bytes)` counts:
    `"output-manifest"` for a batch's logical manifest/read-plan span (a sum
    over files), never `"source-file"`. Physical staged offsets are always 0
    under content-addressed names; the logical window cursor is
    `accepted_phase`. Coverage is proven by `expected` + material
    generations, never by span arithmetic.
    """

    total = _positive_int(total_bytes, where="label span total_bytes")
    if coordinate_space != "output-manifest":
        raise ProducedOutputError(
            "label span coordinate_space must be 'output-manifest'")
    return {"coordinate_space": coordinate_space,
            "start_bytes": 0, "end_bytes": total}


def output_manifest_sha256(descriptors: list[Mapping[str, object]]) -> str:
    """Immutable batch-manifest digest over sealed descriptors."""

    canonical = json.dumps(
        [dict(d) for d in descriptors], sort_keys=True,
        separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


# --------------------------------------------------------------------------
# Admission credit (bound record, NOT ledger tokens) + per-batch prewrite
# --------------------------------------------------------------------------

def reservation_holder(instance: Mapping[str, object]) -> str:
    """Legacy name for the instance namespace (kept for audit continuity).

    Since the liveness R2 ruling this is NOT a ledger holder: no standing
    tokens are acquired under it. Physical tokens live only under batch
    namespaces (pre-transfer) and mover keys (post-transfer)."""

    return instance_namespace(instance)


def _prewrites_dir(queue_root: str | Path,
                   instance: Mapping[str, object]) -> Path:
    return instance_dir(queue_root, instance) / "prewrites"


def _funding_dir(queue_root: str | Path,
                 instance: Mapping[str, object]) -> Path:
    return instance_dir(queue_root, instance) / "funding"


def _check_class_bytes(value: object, *, where: str) -> dict[str, int]:
    """Exact class-bytes shape: all three classes, type-is-int, non-negative.

    Anything else (missing keys, strings, negatives, bools) is corrupt
    state: callers retain/refuse, never mint quota from it.
    """

    if not isinstance(value, Mapping) or set(value) != {
            "payload", "checkpoint", "temp"}:
        raise ProducedOutputError(f"{where} must name payload/checkpoint/temp")
    out: dict[str, int] = {}
    for cls in ("payload", "checkpoint", "temp"):
        out[cls] = _nonneg_int(value.get(cls), where=f"{where}.{cls}")
    return out


def _read_prewrite(path: Path) -> dict[str, object] | None:
    """One outstanding prewrite record, None when absent.

    Raises ProducedOutputError on corrupt/unreadable (unknown state, never
    an empty reservation). Class bytes are validated exact here so no
    caller can mint quota from a malformed record.
    """

    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise ProducedOutputError(
            f"prewrite record unreadable: {exc}") from None
    if not isinstance(raw, Mapping):
        raise ProducedOutputError("prewrite record is corrupt")
    record = dict(raw)
    record["class_bytes"] = _check_class_bytes(
        record.get("class_bytes"), where="prewrite class_bytes")
    paths = record.get("paths", [])
    if not isinstance(paths, list) or any(
            not isinstance(p, str) or not p for p in paths):
        raise ProducedOutputError("prewrite paths are corrupt")
    record["paths"] = list(paths)
    return record


def _outstanding_sums(queue_root: str | Path, instance: Mapping[str, object],
                      exclude_batch_id: str) -> dict[str, int]:
    """Committed + outstanding class bytes, excluding one batch's own record.

    Outstanding prewrites are real reservations: writers may already hold
    HDD bytes against them. A corrupt prewrite file fails the whole
    accounting closed (unknown, never zero).
    """

    checked = validate_instance(instance)
    try:
        commitments = _read_commitments(_commitments_path(queue_root, checked))
    except ProducedOutputError as exc:
        raise ProducedOutputError(f"unknown-retain: {exc}") from None
    sums = _class_sums(commitments["batches"])
    directory = _prewrites_dir(queue_root, checked)
    try:
        with os.scandir(directory) as iterator:
            names = sorted(entry.name for entry in iterator
                           if entry.name.endswith(".prewrite.json"))
    except FileNotFoundError:
        return sums
    except OSError as exc:
        raise ProducedOutputError(f"unknown-retain: {exc}") from None
    for name in names:
        if name == f"{exclude_batch_id}.prewrite.json":
            continue
        record = _read_prewrite(directory / name)
        if record is None:
            continue
        class_bytes = record.get("class_bytes")
        if not isinstance(class_bytes, Mapping):
            raise ProducedOutputError("unknown-retain: bad prewrite record")
        for cls in sums:
            sums[cls] += int(class_bytes.get(cls, 0) or 0)
    return sums


def _publish_prewrite_record(queue, instance: Mapping[str, object], *,
                             batch_id: str, tier: str,
                             class_bytes: Mapping[str, int],
                             paths: Sequence[str]) -> Path:
    """File one outstanding prewrite reservation immutably.

    The single construction of a prewrite record, so the gate above it
    and any caller that must file one cannot drift on its shape.
    Raises whatever `_publish_immutable` raises for a conflicting body;
    `require_prewrite` owns the idempotent-replay interpretation.
    """

    from prismabuild import pool as pool_mod

    directory = _prewrites_dir(queue.root, instance)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{batch_id}.prewrite.json"
    record = {"batch_id": batch_id, "tier": tier,
              "class_bytes": dict(class_bytes), "paths": list(paths),
              "owner_action_key": str(instance["owner_action_key"]),
              "owner_attempt": dict(instance["owner_attempt"])}
    raw = json.dumps(record, sort_keys=True,
                     separators=(",", ":")).encode() + b"\n"
    pool_mod._publish_immutable(path, raw, where="produced-output prewrite")
    return path


def _live_path_owner(queue_root: str | Path,
                     checked_instance: Mapping[str, object],
                     checked_template: Mapping[str, object],
                     planned_paths: Sequence[str],
                     exclude_batch_id: str) -> tuple[str, str, str] | None:
    """The live writer that already owns one of these origin paths.

    Staged material identity is keyed by the ORIGIN PATH, so a path
    belongs to at most one live writer: a second writer replacing those
    bytes would invalidate the owner's published material and anything
    fenced on it, which the mover refuses outright. That refusal lands
    only after the bytes are written and the batch committed, so
    ownership is answered HERE, at the write-authorization gate.

    Two kinds of owner, both live:

    * a committed batch whose ACTIVE materialization is not retired -- its
      staged copy stands (the first one, or a restaged successor) and its
      entries name the paths;
    * an outstanding prewrite for another batch id -- it already holds
      permission to write those paths, whether or not it has committed.

    Ownership ends at retirement, which evicts the staged copy; the
    durable charge outlives it but does not reserve the name, so a
    retired batch's paths regenerate. Unknown state fails closed: an
    unreadable batch record or prewrite raises rather than reading as
    unowned, because absent metadata never proves an absent owner.

    Returns ``(path, owner_kind, owner_batch_id)`` for the first owned
    path in sorted order, or None when every path is free.
    """

    wanted = set(planned_paths)
    if not wanted:
        return None
    commitments = _read_commitments(_commitments_path(queue_root,
                                                      checked_instance))
    batches = commitments["batches"]
    assert isinstance(batches, Mapping)
    owners: dict[str, tuple[str, str]] = {}
    for owner_id in sorted(batches):
        entry = batches[owner_id]
        if owner_id == exclude_batch_id:
            continue
        if not isinstance(entry, Mapping):
            raise ProducedOutputError(
                f"unknown-retain: bad committed batch {owner_id!r}")
        # `retired` on the entry describes only the FIRST materialization. A
        # batch that has been restaged has a LIVE stage copy under a successor
        # mover reading these very origin files, so authorizing a second
        # writer over them here would corrupt a copy in flight. Ownership
        # follows the active materialization.
        #
        # An origin-only batch (#912) has no copy, and its origin files are
        # what a consumer declared and will stage.  It owns its paths until
        # `reclaim_origin` proves them gone; regenerating them earlier would
        # change bytes under a declared read.
        if entry.get("origin_only") is True:
            if entry.get("origin_reclaimed"):
                continue
        elif _batch_stage_retired(entry):
            continue
        indexed = entry.get("paths")
        if isinstance(indexed, list):
            owned_paths = [str(item) for item in indexed]
        else:
            # Entry predating the index: fall back to the immutable
            # record, which fails closed when it cannot be validated.
            _filed, sealed = _load_batch_record(
                queue_root, checked_instance, checked_template, entry,
                owner_id)
            owned_paths = [str(desc["path"]) for desc in sealed]
        for owned_path in owned_paths:
            owners.setdefault(owned_path, ("batch", owner_id))
    directory = _prewrites_dir(queue_root, checked_instance)
    try:
        with os.scandir(directory) as iterator:
            names = sorted(entry.name for entry in iterator
                           if entry.name.endswith(".prewrite.json"))
    except FileNotFoundError:
        names = []
    except OSError as exc:
        raise ProducedOutputError(f"unknown-retain: {exc}") from None
    for name in names:
        owner_id = name[:-len(".prewrite.json")]
        if owner_id == exclude_batch_id:
            continue
        record = _read_prewrite(directory / name)
        if record is None:
            continue
        reserved = record.get("paths")
        if not isinstance(reserved, list):
            raise ProducedOutputError("unknown-retain: bad prewrite record")
        for reserved_path in reserved:
            owners.setdefault(str(reserved_path), ("prewrite", owner_id))
    for candidate in sorted(wanted):
        owner = owners.get(candidate)
        if owner is not None:
            return (candidate, owner[0], owner[1])
    return None


def _read_funding(path: Path) -> dict[str, object] | None:
    """One batch's durable funding intent, None when no attempt was made.

    Raises ProducedOutputError on corrupt/unreadable (unknown state, never
    an unfunded batch).
    """

    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise ProducedOutputError(
            f"funding intent unreadable: {exc}") from None
    if not isinstance(raw, Mapping):
        raise ProducedOutputError("funding intent is corrupt")
    return dict(raw)


def _write_funding(path: Path, record: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(
        dir=str(path.parent), prefix=".funding.")
    try:
        with os.fdopen(handle, "w") as stream:
            json.dump(dict(record), stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def _delete_funding(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError as exc:
        raise ProducedOutputError(
            f"funding intent unreleasable: {exc}") from None


def output_fragment_root(residency_root: str | Path) -> Path:
    """The output material namespace root, beside input fragments."""

    return Path(residency_root) / OUTPUT_FRAGMENTS_SUBDIR


def batch_namespace(instance: Mapping[str, object], batch_id: str,
                    manifest_digest: str) -> str:
    """Immutable batch namespace bound to its exact manifest + owner scope."""

    checked = validate_instance(instance)
    _name(batch_id, where="batch_id")
    _hex64(manifest_digest, where="manifest_digest")
    raw = ("produced-output-batch:" + instance_namespace(checked) + ":"
           + batch_id + ":" + manifest_digest)
    return hashlib.sha256(raw.encode()).hexdigest()


def namespace_for_batch_reference(*, owner_action_key: str,
                                  template_sha256: str, nonce: str,
                                  scope_id: str, template_id: str,
                                  output_prefix: str, batch_id: str,
                                  manifest_digest: str) -> tuple[str, str]:
    """Recompute (instance_ns, batch_ns) for a sealed batch reference.

    Pure helper reusing the existing identity validators (no formula
    duplication, no writer mutation): builds the minimal bound instance
    through :func:`validate_instance` (filed template supplies the prefix)
    and returns :func:`instance_namespace` + :func:`batch_namespace`.
    Raises :class:`ProducedOutputError` on any malformed field.
    """

    instance = validate_instance({
        "schema": INSTANCE_SCHEMA_V1,
        "version": 1,
        "template_id": _name(template_id, where="reference template_id"),
        "template_sha256": _hex64(template_sha256,
                                  where="reference template_sha256"),
        "owner_action_key": _hex64(owner_action_key,
                                   where="reference owner_action_key"),
        "owner_attempt": {
            "nonce": _hex32(nonce, where="reference nonce"),
            "scope_id": _name(scope_id, where="reference scope_id"),
        },
        "attempt_source": "broker-launch",
        "output_prefix": _abs_norm(output_prefix,
                                   where="reference output_prefix"),
        "bound_unix": 0,
    })
    return (instance_namespace(instance),
            batch_namespace(instance,
                            _name(batch_id, where="reference batch_id"),
                            _hex64(manifest_digest,
                                   where="reference manifest_digest")))


def refill_window(queue, instance: Mapping[str, object],
                   template: Mapping[str, object], *, tier: str
                   ) -> dict[str, object]:
    """Bounded lifecycle refill of a long-lived producer window.

    Retirement returns spent credits to FREE (the egress releases by mover
    key; the fleet never restores producer holdings), so a producer whose
    batches completed must re-acquire its OWN admitted window capacity
    before the next batch can spend it. This acquires from free, under the
    owner key, exactly up to the aggregate window bound -- holdings plus
    outstanding intent names never exceed the template's window_gib on the
    tier, before or after. It is a lifecycle return of the producer's own
    admitted window, never a batch's fresh acquisition: batches still fund
    only by exact transfer. Requires the live owner and a provable census;
    an unavailable top-up is deferred when positive holdings already meet
    the declared minimum. That is not batch funding: the subsequent exact
    transfer must still prove the actual batch fits. Otherwise the typed
    tier-reservation-unavailable refuses rather than exceeding the bound. The
    lane's sequential-writer contract applies (one producer action per
    instance, as everywhere in this lane).
    """

    from prismabuild import pool as pool_mod
    from prismabuild import storage_tiers as tiers_mod

    try:
        checked_template, checked_instance = _require_bound_contract(
            template, instance)
    except ProducedOutputError:
        return {"ok": False, "refusal": "template-mismatch"}
    if checked_template.get("write_only"):
        # A write-only template holds no window to refill (#912).
        return {"ok": False, "refusal": "template-is-write-only"}
    if tier not in checked_template["permitted_tiers"]:
        return {"ok": False, "refusal": "tier-not-permitted"}
    window = int(checked_template["working_demands"][tier]["window_gib"])
    minimum = max(1, int(
        checked_template["working_demands"][tier]["minimum_gib"]))
    owner = str(checked_instance["owner_action_key"])
    kind = tiers_mod.capacity_kind_of(tier)
    gated = _require_live_owner(queue, checked_instance)
    if gated is not None:
        return gated
    # Aggregate window accounting: owner-held names PLUS everything this
    # owner has live elsewhere -- transferred names and the fences of
    # claimed-but-unretired (consumed) batches, right up to the retirement
    # that releases them. Fail-retain on any unprovable record.
    outstanding, census_unknown = (
        queue._output_outstanding_window_tokens(owner, tier, kind))
    if census_unknown:
        return {"ok": False, "refusal": "unknown-retain: funding-census"}
    with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
        ledger = queue.tier_ledger(tier)
        held = ledger.holder_tokens(owner).get(kind, 0)
        room = window - held - outstanding
        if room <= 0:
            return {"ok": True, "tier": tier, "acquired": 0, "held": held,
                    "outstanding": outstanding, "window_gib": window}
        available = ledger.available().get(kind, 0)
        take = min(room, available)
        if take <= 0 or not ledger.acquire(owner, {kind: take}):
            # Replenishment is advisory when existing credit remains usable.
            # In particular a contended mint guard can decline acquisition
            # even beside free capacity. Do not make exact prepaid funding
            # wait for that optional acquisition. Re-read after the attempt:
            # only current holdings, never the earlier snapshot, qualify.
            held = ledger.holder_tokens(owner).get(kind, 0)
            if held >= minimum:
                return {"ok": True, "tier": tier, "acquired": 0,
                        "held": held, "outstanding": outstanding,
                        "window_gib": window, "kind": kind,
                        "refill_deferred": "tier-reservation-unavailable"}
            return {"ok": False, "refusal": "tier-reservation-unavailable",
                    "available": ledger.available(), "window_gib": window,
                    "held": held, "outstanding": outstanding}
        held = ledger.holder_tokens(owner).get(kind, 0)
    return {"ok": True, "tier": tier, "acquired": take, "held": held,
            "outstanding": outstanding, "window_gib": window,
            "kind": kind}


def unheld_window_gib(queue, tier_id: str) -> dict[str, object]:
    """The live producers' admitted windows that nobody holds, on one tier.

    A producer reserves its template's ``window_gib`` on the tier at claim
    (``owner_demand_terms``), and its batches spend that window by exact
    transfer. Retirement returns the spent credits to FREE, not to the
    owner, and the owner takes them back with `refill_window`. Between a
    retirement and the refill the window is owed but held by nobody, so a
    tier gate that counts only held tokens and queued demand counts it as
    zero, and a consumer's window can take the room the refill needs.

    Per live owner this is ``window - held - outstanding``, floored at zero,
    with the same terms `refill_window` bounds itself by: ``held`` is the
    owner's own holdings and ``outstanding`` is what its batches still hold
    elsewhere (`PoolQueue._output_outstanding_window_tokens`). Both are held
    tokens a gate already counts, so the result never counts a token twice.
    A queued owner's window is still in its ready demand and a finished or
    superseded owner owes nothing, so only an owner whose live CLAIMED row
    names the instance's own attempt contributes.

    Liveness is read first, from the claimed row alone, so a dead owner's
    records are never opened: a torn record under a finished owner costs
    nothing. For a live owner, an unreadable instance, claim, filed template
    or holding makes the obligation unknown: ``gib`` is ``None`` and
    ``unknown`` names the owner. An unreadable batch census counts no
    outstanding tokens, which can only raise the result, and ``bounded``
    names the owner. Returns ``{"gib", "owners", "unknown", "bounded"}``.
    Read-only: it takes no lock and changes nothing.
    """

    from prismabuild import pool as pool_mod
    from prismabuild import storage_tiers as tiers_mod

    tier = str(tier_id)
    kind = tiers_mod.capacity_kind_of(tier)
    owed: dict[str, int] = {}
    unknown: list[dict[str, str]] = []
    bounded: list[str] = []
    scopes_root = Path(queue.root) / "residency" / OUTPUT_SCOPES_SUBDIR
    try:
        owners = _scope_owners(scopes_root)
    except FileNotFoundError:
        owners = []
    except OSError as exc:
        return {"gib": None, "owners": {},
                "unknown": [{"owner": "", "error": f"scopes unreadable: {exc}"}],
                "bounded": []}
    templates_root = Path(queue.root) / "residency" / OUTPUT_TEMPLATES_SUBDIR
    for owner in owners:
        try:
            if not queue.item_path(pool_mod.CLAIMED, owner).exists():
                continue
            paths = _owner_instance_paths(scopes_root / owner)
        except OSError as exc:
            unknown.append({"owner": owner, "error": f"unreadable: {exc}"})
            continue
        window: int | None = None
        error = ""
        for path in paths:
            try:
                instance = validate_instance(json.loads(path.read_text()))
            except (OSError, ValueError) as exc:
                error = f"instance {path.name} unreadable: {exc}"
                break
            if str(instance["owner_action_key"]) != owner:
                error = f"instance {path.name} names another owner"
                break
            gated = _require_live_owner(queue, instance)
            if gated is not None:
                refusal = str(gated.get("refusal") or "")
                if refusal in ("owner-not-running", "stale-superseded-owner"):
                    continue
                error = f"claim unreadable: {refusal}"
                break
            try:
                template = validate_template(json.loads(
                    (templates_root
                     / f"{instance['template_id']}.json").read_text()))
            except (OSError, ValueError) as exc:
                error = f"template {instance['template_id']} unreadable: {exc}"
                break
            if template_sha256(template) != instance["template_sha256"]:
                error = (f"template {instance['template_id']} is not the one "
                         "the instance is bound to")
                break
            if tier not in template["permitted_tiers"]:
                continue
            bound = int(template["working_demands"][tier]["window_gib"])
            window = bound if window is None else max(window, bound)
        if error:
            unknown.append({"owner": owner, "error": error})
            continue
        if window is None:
            continue
        try:
            held = int(queue.tier_ledger(tier).holder_tokens(owner).get(kind, 0))
        except (OSError, pool_mod.PoolContractError, ValueError) as exc:
            unknown.append({"owner": owner, "error": f"holdings unreadable: {exc}"})
            continue
        outstanding, census_unknown = (
            queue._output_outstanding_window_tokens(owner, tier, kind))
        if census_unknown:
            outstanding = 0
            bounded.append(owner)
        owed[owner] = max(0, window - held - int(outstanding))
    return {"gib": None if unknown else sum(owed.values()),
            "owners": owed, "unknown": unknown, "bounded": bounded}


def _planned_omitted_absent(prewrite: Mapping[str, object],
                            sealed: list[dict[str, object]]) -> bool:
    """Every planned path the descriptors omit is proven absent.

    The conservative prewrite admitted a planned superset; the paths the
    commit's descriptors do NOT cover must be gone by commit time (a
    written temporary left behind would otherwise lose its charge with the
    prewrite consumed). Absence is lstat-only: no payload read, no hash.
    Present or unreadable retains (False); proven absent passes.
    """

    actual = {str(d["path"]) for d in sealed}
    for planned in prewrite.get("paths", []):
        text = str(planned)
        if text in actual:
            continue
        try:
            os.lstat(text)
        except FileNotFoundError:
            continue
        return False
    return True


def _actual_within_ceiling(prewrite: Mapping[str, object],
                           sealed: list[dict[str, object]]) -> bool:
    """Actual per-class bytes at or under the admitted prewrite ceiling.

    The prewrite admits per-class UPPER BOUNDS for the batch's temporary
    lifetime (the conservative shape a producer serializes before sizes
    are known); the actual descriptors must land at or under each bound.
    Exact equality remains the special case of an exact ceiling.
    """

    planned = prewrite.get("class_bytes")
    if not isinstance(planned, Mapping):
        return False
    actual = {"payload": 0, "checkpoint": 0, "temp": 0}
    for desc in sealed:
        actual[str(desc["artifact_class"])] += int(desc["bytes"])
    for cls, value in actual.items():
        try:
            if value > int(planned.get(cls, -1)):
                return False
        except (TypeError, ValueError):
            return False
    return True


def describe_output_precommit_for_funding(
        queue, instance: Mapping[str, object],
        template: Mapping[str, object], batch_id: str,
        descriptors: list[Mapping[str, object]],
        tier: str, mover_key: str) -> dict[str, object]:
    """Read-only validated precommit facts for one batch (base-compatible).

    New shared seam owned by the pool funding lane (read-only; no writer
    mutation, no edits to commit/prewrite/retire/release).  Uses only
    base-available helpers (no R3 loader): validates from LIVE/filed
    precommit records, never from caller-shaped hashes alone:

    * bound template/instance contract (exact template_sha binding);
    * live owner CLAIMED row still names the instance's exact nonce/scope
      (via _require_live_owner; stale/superseded/absent raises);
    * durable prewrite exists for batch_id and matches descriptors exactly
      (same tier/class_bytes/paths/owner/attempt checks as commit_batch);
    * every descriptor validates via validate_descriptor against the bound
      template/instance, with recomputed manifest digest.

    Does NOT require the filed batch (commit comes AFTER funding in the
    future writer order: prewrite -> publish mover -> fund -> commit).
    Returns the exact facts the pool output intent binds: owner/template/
    batch/manifest/total/range(0..total)/tier/mover/namespace/class_bytes.
    Raises ProducedOutputError on any mismatch.
    """

    checked_template, checked_instance = _require_bound_contract(
        template, instance)
    _name(batch_id, where="batch_id")
    _hex64(mover_key, where="precommit mover_key")
    if tier not in checked_template["permitted_tiers"]:
        raise ProducedOutputError("tier-not-permitted")
    if not isinstance(descriptors, list) or not descriptors:
        raise ProducedOutputError("precommit descriptors required")
    gated = _require_live_owner(queue, checked_instance)
    if gated is not None:
        raise ProducedOutputError(str(gated.get("refusal", "owner-not-running")))
    try:
        prewrite = _read_prewrite(
            _prewrites_dir(queue.root, checked_instance)
            / f"{batch_id}.prewrite.json")
    except ProducedOutputError as exc:
        raise ProducedOutputError(f"unknown-retain: {exc}") from None
    sealed = [validate_descriptor(dict(d), checked_template,
                                  checked_instance)
              for d in descriptors]
    class_bytes: dict[str, int] = {"payload": 0, "checkpoint": 0, "temp": 0}
    for desc in sealed:
        class_bytes[str(desc["artifact_class"])] += int(desc["bytes"])
    manifest = output_manifest_sha256(sealed)
    total = sum(class_bytes.values())
    if prewrite is None:
        # RESTAGE authority (`ensure_batch_materialized`): the prewrite was
        # consumed by the commit that made this batch durable, so a
        # re-materialization has none to present and the only conformant
        # authority is the COMMITTED record in its restageable state -- first
        # copy stage-retired, origin charge intact, origins still carrying the
        # identity that commit recorded, and PB's own filed materialization
        # row naming exactly this mover key. Every OTHER absent-prewrite case
        # -- an unretired committed batch (its own mover still owns the
        # material), a reclaimed-origin batch, a caller-invented mover key --
        # keeps today's `prewrite-reservation-missing` refusal unchanged.
        if _committed_restage_authority(
                queue, checked_instance, checked_template, batch_id,
                manifest, mover_key) is None:
            raise ProducedOutputError("prewrite-reservation-missing")
    else:
        # Ceiling reconciliation (conservative prewrite -> actual): same tier,
        # owner and attempt; actual paths are a SUBSET of the planned superset
        # (unwritten planned paths stay absent, which abort already requires);
        # actual per-class bytes land at or under the admitted ceiling.
        if (prewrite.get("tier") != tier
                or not _actual_within_ceiling(prewrite, sealed)
                or not set(str(d["path"]) for d in sealed)
                <= set(prewrite.get("paths", []))
                or prewrite.get("owner_action_key")
                != checked_instance["owner_action_key"]
                or dict(prewrite.get("owner_attempt", {})) != dict(
                    checked_instance["owner_attempt"])):
            raise ProducedOutputError("prewrite-mismatch")
        if not _planned_omitted_absent(prewrite, sealed):
            # A planned-but-omitted path still EXISTS (or is unstatable): the
            # prewrite's charge for it must not vanish with the commit.
            raise ProducedOutputError("planned-path-present-retain")
    if total <= 0:
        raise ProducedOutputError("unknown-retain: precommit-total")
    attempt = checked_instance["owner_attempt"]
    assert isinstance(attempt, dict)
    namespace = batch_namespace(checked_instance, batch_id, manifest)
    return {
        "owner_action_key": str(checked_instance["owner_action_key"]),
        "owner_nonce": str(attempt["nonce"]),
        "owner_scope_id": str(attempt["scope_id"]),
        "template_id": str(checked_template["template_id"]),
        "template_sha256": template_sha256(checked_template),
        "batch_id": batch_id,
        "manifest_digest": manifest,
        "total_bytes": total,
        "range_start_bytes": 0,
        "range_end_bytes": total,
        "tier": tier,
        "mover_key": mover_key,
        "batch_namespace": namespace,
        "class_bytes": dict(class_bytes),
    }


def _commitments_path(queue_root: str | Path,
                      instance: Mapping[str, object]) -> Path:
    return instance_dir(queue_root, instance) / "commitments.json"


def _read_commitments(path: Path) -> dict[str, object]:
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        return {"batches": {}, "admission": None}
    except (OSError, ValueError) as exc:
        # Corrupt/unreadable commitments are unknown state, never an empty
        # scope: every caller retains charge and names the record.
        raise ProducedOutputError(
            f"commitments record corrupt or unreadable: {exc}") from None
    if not isinstance(raw, Mapping) or not isinstance(raw.get("batches"), Mapping):
        raise ProducedOutputError("commitments record is corrupt")
    admission = raw.get("admission")
    if admission is not None and not isinstance(admission, Mapping):
        raise ProducedOutputError("commitments admission record is corrupt")
    return {"batches": dict(raw["batches"]), "admission": admission}


def _write_commitments(path: Path, record: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    admission = record.get("admission")
    if admission is None and "admission" not in record:
        # Read-modify-write callers pass batches only; never drop a bound
        # admission credit record on a batch update.  A caller that holds
        # the record it read under the same lock passes ``admission`` itself,
        # which spares a second parse of the whole document (#1072).
        try:
            previous = _read_commitments(path)
            admission = previous.get("admission")
        except ProducedOutputError:
            admission = None
    # One encode and one write.  ``json.dump`` runs the pure-Python encoder
    # (the C encoder serves only ``dumps``, CPython's ``_one_shot`` path) and
    # hands the stream one small chunk at a time; on R13's 9.2 MB document
    # that was a large part of every retirement (#1072).  The text is the
    # same either way.
    text = json.dumps({"batches": dict(record["batches"]),
                       "admission": admission}, sort_keys=True) + "\n"
    handle, temporary = tempfile.mkstemp(
        dir=str(path.parent), prefix=".commitments.")
    try:
        with os.fdopen(handle, "w") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def _class_sums(batches: Mapping[str, object]) -> dict[str, int]:
    """Committed class bytes still charged against durable-origin quota.

    Stage retirement (`retired`, set after a complete SSD/RAM egress) frees
    the tier working window, never the durable HDD payload/checkpoint/temp
    bytes: those stay charged until `reclaim_origin` proves every origin
    path absent. Only batches with `origin_reclaimed` set stop counting.
    Malformed records fail closed; records predating the flag (no key)
    count as unreclaimed.
    """

    sums = {"payload": 0, "checkpoint": 0, "temp": 0}
    for batch_id, record in batches.items():
        if not isinstance(record, Mapping):
            raise ProducedOutputError(
                f"unknown-retain: bad committed batch {batch_id!r}")
        if record.get("origin_reclaimed"):
            continue
        for cls in sums:
            sums[cls] += _check_class_bytes(
                record.get("class_bytes"),
                where=f"committed batch {batch_id!r}")[cls]
    return sums


# --------------------------------------------------------------------------
# Repeat materialization over one committed batch (bounded-window restage)
# --------------------------------------------------------------------------
#
# A committed batch is an IMMUTABLE logical unit with ONE durable charge. The
# stage copy under it is not: the bounded-window path stages a batch, lets a
# reader consume it, retires the copy to give the window credit back, and --
# when the same bytes are needed again -- stages the SAME batch a second time
# over the SAME origin files. The batch, its manifest, its descriptors, its
# namespace and its durable charge never move; only the materialization does.
#
# Everything below is that one transition. There is no second cache, no second
# dispatcher and no v2 record: a materialization reuses the published
# first-publisher helpers, the pool's prepaid funding, the existing strict SDK,
# the existing egress and the existing recovery. A write-only template's
# origin-only batch (`commit_origin_batch`, #912) is not re-materialized here;
# a consumer stages it as an input instead.


def _portable_identity_of(info) -> dict[str, int]:
    """The existing origin-identity tuple for one stat result.

    `reader_lease.portable_identity` is the fleet's one spelling of server-side
    file identity (`ino/size/mtime_ns/ctime_ns`, deliberately without st_dev,
    which is per-client on NFS). This lane reads it; it never defines a second
    identity and never edits that module.
    """

    from prismabuild import reader_lease as lease_mod

    return lease_mod.portable_identity(info)


def _origin_identity_at_commit(sealed: list[dict[str, object]]
                               ) -> tuple[dict[str, dict[str, int]] | None,
                                          dict[str, object] | None]:
    """Capture each sealed origin's identity, one lstat per path.

    Both commit paths call it under the output-prefix lock (#1064), so the
    move aside and put-back of a retirement holding that lock, which move
    the ctime, are either behind it or have not started. ONE stat answers
    both questions the commit asks -- is this file the length the
    descriptor claims, and what exactly is this file -- so the size that is
    checked and the identity that is recorded can never be two different
    moments. `os.lstat` (not `stat`) matches the check this path has always
    made and is strictly stronger for the proof: replacing a regular file
    with a symlink to identical bytes changes the recorded identity and
    refuses.
    Returns `(identity_map, None)` or `(None, refusal)`.
    """

    identity: dict[str, dict[str, int]] = {}
    for desc in sealed:
        path = str(desc["path"])
        try:
            info = os.lstat(path)
        except OSError as exc:
            return (None, {"ok": False,
                           "refusal": f"descriptor-unstatable: {exc}"})
        if int(info.st_size) != int(desc["bytes"]):
            return (None, {"ok": False, "refusal": "descriptor-size-mismatch",
                           "path": path})
        identity[path] = _portable_identity_of(info)
    return (identity, None)


def _read_landed_origins(sealed: list[dict[str, object]],
                         landed: Mapping[str, Mapping[str, object]],
                         verified: dict[str, dict[str, object]]
                         ) -> dict[str, object] | None:
    """Read each landed origin whose timestamps alone moved, with no lock held.

    ``landed`` is the identity each origin's writer recorded when it landed
    (`commit_origin_batch`). An origin that is still that file, or the file
    ``verified`` already read, costs one lstat. One with the landed inode and
    size and other timestamps (an NFS delegation recall, #1111) is read and
    hashed (`_verify_origin_content`), and its re-pin is kept in ``verified``
    by path for `_landed_identity` to check under the lock. Everything else
    is left to that check, which takes the identity the batch records: an
    origin that cannot be stat'ed here may be at a retirement's private name
    for a moment, and is back by the time the lock is granted (#1064).
    Returns the refusal of a read whose digest is not the descriptor's, or
    ``None``.
    """

    from prismabuild import reader_lease as lease_mod

    for desc in sealed:
        path = str(desc["path"])
        try:
            live = _portable_identity_of(os.lstat(path))
        except OSError:
            continue
        got = verified.get(path)
        if (lease_mod.file_id_matches(landed[path], live)
                or (got is not None
                    and lease_mod.file_id_matches(got["to"], live))
                or not lease_mod.timestamp_only_mismatch(landed[path], live)):
            continue
        repin, why = _verify_origin_content(
            path, landed[path], live, desc["sha256"], where="commit")
        if repin is None:
            return {"ok": False, "refusal": "origin-is-not-the-landed-copy",
                    "detail": f"{path}: {why}"}
        verified[path] = repin
    return None


def _landed_identity(sealed: list[dict[str, object]],
                     landed: Mapping[str, Mapping[str, object]] | None,
                     verified: Mapping[str, Mapping[str, object]],
                     origin_identity: dict[str, dict[str, int]]
                     ) -> tuple[list[dict[str, object]], list[str],
                                dict[str, object] | None]:
    """Is each origin, stat'ed under the lock, the copy that landed?

    ``origin_identity`` is the identity `_origin_identity_at_commit` took
    under the output-prefix lock. Each origin must be the landed file, or the
    file a read outside the lock hashed (``verified``, from
    `_read_landed_origins`); that re-pin is returned for the entry, and the
    identity committed is the hashed one. An origin whose inode or size is
    not the landed one refuses. One whose timestamps alone moved and that no
    read has hashed at that identity is returned in ``moved``: the caller lets
    the lock go and reads it. Returns ``(landed_repins, moved, refusal)``.
    """

    from prismabuild import reader_lease as lease_mod

    repins: list[dict[str, object]] = []
    moved: list[str] = []
    if landed is None:
        return (repins, moved, None)
    for desc in sealed:
        path = str(desc["path"])
        live = origin_identity[path]
        if lease_mod.file_id_matches(landed[path], live):
            continue
        got = verified.get(path)
        if got is not None and lease_mod.file_id_matches(got["to"], live):
            origin_identity[path] = dict(got["to"])
            repins.append(dict(got))
            continue
        if not lease_mod.timestamp_only_mismatch(landed[path], live):
            return (repins, moved,
                    {"ok": False, "refusal": "origin-is-not-the-landed-copy"})
        moved.append(path)
    return (repins, moved, None)


#: Why an origin identity was re-pinned; the only reason there is.
ORIGIN_REPIN_REASON = ("timestamps-only mismatch: same inode and size, content "
                       "sha256 verified (#1111, an NFS delegation recall)")
_ORIGIN_REPIN_FIELDS = frozenset({"path", "from", "to", "reason", "sha256",
                                  "bytes", "rehash_s", "reads", "host", "unix",
                                  "where"})
#: Content checks that refused, by ``(path, identity, sha256)``.  A file
#: whose identity has not moved since its content was refused is refused
#: again without a read, so a retirement tick does not re-read a changed
#: origin every cycle.  Only refusals are kept: a changed origin is an
#: anomaly an operator settles, so this grows with those, not with the work.
_CONTENT_REFUSED: dict[tuple[str, tuple[object, ...], str], str] = {}


def _identity_key(identity: Mapping[str, object]) -> tuple[object, ...]:
    return tuple(identity.get(field)
                 for field in ("ino", "size", "mtime_ns", "ctime_ns"))


def _verify_origin_content(path: str, published: Mapping[str, object],
                           observed: Mapping[str, object], sha256: object, *,
                           where: str
                           ) -> tuple[dict[str, object] | None, str | None]:
    """Settle one origin whose timestamps alone moved, by its content (#1111).

    ``published`` is the identity a check compares against, and ``observed``
    the one it found: the same inode and size, other timestamps
    (`reader_lease.timestamp_only_mismatch`).  That is what an NFS
    delegation recall does to a file its writer recorded: the server applies
    the writer's delegated timestamps, and the bytes stay the same.  The file
    is read through its parent without following a link and hashed
    (`reader_lease.content_identity`), with no lock held; the caller files
    the result under its own lock after a re-stat.

    Returns ``(repin, None)`` when the digest is the committed ``sha256`` and
    the inode and size are the published ones: ``repin`` names the path, the
    identity it replaces, the hashed one, the reason, the seconds the read
    took and where it was made.  Otherwise ``(None, why)``.  A batch with no
    digest (the DEV null convention) is never settled this way.
    """

    from prismabuild import reader_lease as lease_mod

    if not isinstance(sha256, str) or not sha256:
        return (None, "only the timestamps moved, and the batch carries no "
                      "sha256 to verify its content against")
    memo = (path, _identity_key(observed), sha256)
    if memo in _CONTENT_REFUSED:
        return (None, _CONTENT_REFUSED[memo])
    directory, name = os.path.split(path)
    try:
        parent = os.open(directory or ".",
                         os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    except OSError as exc:
        return (None, f"only the timestamps moved, and the content could not "
                      f"be verified: {exc}")
    try:
        read = lease_mod.content_identity(name, dir_fd=parent)
    except (OSError, lease_mod.ReaderLeaseError) as exc:
        return (None, f"only the timestamps moved, and the content could not "
                      f"be verified: {exc}")
    finally:
        os.close(parent)
    now = read["identity"]
    if (now["ino"], now["size"]) != (published.get("ino"), published.get("size")):
        return (None, "the file changed while its content was verified")
    if read["sha256"] != sha256:
        why = (f"only the timestamps moved, but the content sha256 "
               f"{read['sha256']} is not the committed {sha256}")
        _CONTENT_REFUSED[memo] = why
        _CONTENT_REFUSED[(path, _identity_key(now), sha256)] = why
        return (None, why)
    return ({"path": path, "from": dict(published), "to": dict(now),
             "reason": ORIGIN_REPIN_REASON, "sha256": sha256,
             "bytes": read["bytes"], "rehash_s": read["seconds"],
             "reads": read["reads"], "host": socket.gethostname(),
             "unix": round(time.time(), 3), "where": where}, None)


def _effective_origin_identity(filed: Mapping[str, object],
                               sealed: list[dict[str, object]],
                               entry: Mapping[str, object] | None
                               ) -> dict[str, dict[str, object]] | None:
    """The identity each origin is checked against: the commit's, re-pinned (#1111).

    The immutable batch record carries the identity its commit recorded.
    The mutable commitments entry may carry ``origin_repins``, each filed
    after a content check (`_verify_origin_content`); applied in order, each
    must start from the identity the ones before it left, change only the
    timestamps, and carry the batch's sha256 for its path.  A list that does
    not chain is corrupt and raises ``unknown-retain``, never reads as the
    committed identity.  Returns ``None`` when the record has no identity.
    """

    from prismabuild import reader_lease as lease_mod

    recorded = filed.get("origin_identity")
    if not isinstance(recorded, Mapping):
        return None
    current = {str(path): dict(identity) if isinstance(identity, Mapping)
               else identity for path, identity in recorded.items()}
    repins = (entry or {}).get("origin_repins")
    if repins is None:
        return current
    corrupt = ProducedOutputError(
        "unknown-retain: origin re-pins do not chain from the committed identity")
    if not isinstance(repins, list) or not repins:
        raise corrupt
    digests = {str(desc["path"]): desc.get("sha256") for desc in sealed}
    for repin in repins:
        if (not isinstance(repin, Mapping) or set(repin) != _ORIGIN_REPIN_FIELDS
                or repin["reason"] != ORIGIN_REPIN_REASON):
            raise corrupt
        path = repin["path"]
        if (not isinstance(path, str) or path not in current
                or not isinstance(digests.get(path), str)
                or repin["sha256"] != digests[path]
                or repin["from"] != current[path]):
            raise corrupt
        try:
            after = lease_mod._check_identity(repin["to"], where="re-pinned to")
        except lease_mod.ReaderLeaseError:
            raise corrupt from None
        if not lease_mod.timestamp_only_mismatch(repin["from"], after):
            raise corrupt
        current[path] = dict(after)
    return current


def _check_origin_identity(filed: Mapping[str, object],
                           sealed: list[dict[str, object]],
                           entry: Mapping[str, object] | None = None, *,
                           verify: bool = False, where: str = ""
                           ) -> dict[str, object]:
    """Do the sealed origins still carry the identity their commit recorded?

    This is the whole safety of restaging a DEV null-digest batch. The
    descriptor carries no payload digest, so the only thing that can say the
    bytes about to be copied a second time are the bytes the batch was
    committed over is the file identity captured when it was committed --
    `(ino, size, mtime_ns, ctime_ns)` through the same
    `reader_lease.file_id_matches` every strict reader and every publication
    proof uses. Size alone is NOT that proof: a rewritten file of identical
    length passes an lstat size check and is a different artifact.

    The identity compared is the commit's with the entry's re-pins applied
    (`_effective_origin_identity`). A mismatch in the timestamps alone, on an
    origin whose descriptor carries its sha256, is what an NFS delegation
    recall leaves (#1111). With ``verify`` the origin is read and hashed
    here, with no lock held (`_verify_origin_content`), and the answer
    carries the re-pins for the caller to file under its lock; without it
    the answer is ``restage-origin-unverified`` and names the paths. Nothing
    else is ever read: a match costs one lstat, and a batch with no digest
    stays strict.

    A batch committed before the proof existed has no `origin_identity` and
    refuses `restage-origin-proof-missing` -- the current bytes are never
    retroactively blessed as the committed ones. An unstatable path is unknown
    and refuses. Returns ``{"ok": True, "repins": [...]}`` or
    ``{"ok": False, "refusal", "detail"?, "paths"?}``.
    """

    from prismabuild import reader_lease as lease_mod

    try:
        recorded = _effective_origin_identity(filed, sealed, entry)
    except ProducedOutputError as exc:
        return {"ok": False, "refusal": str(exc)}
    if not recorded or len(recorded) != len(sealed):
        return {"ok": False, "refusal": "restage-origin-proof-missing"}
    repins: list[dict[str, object]] = []
    unverified: list[str] = []
    for desc in sealed:
        path = str(desc["path"])
        published = recorded.get(path)
        if not isinstance(published, Mapping):
            return {"ok": False, "refusal": "restage-origin-proof-missing"}
        # A proof that disagrees with the manifest it is filed beside is not a
        # proof of anything.
        if published.get("size") != int(desc["bytes"]):
            return {"ok": False, "refusal": "restage-origin-proof-missing"}
        try:
            live = _portable_identity_of(os.lstat(path))
        except OSError as exc:
            return {"ok": False, "refusal": f"restage-origin-unstatable: {exc}"}
        if lease_mod.file_id_matches(published, live):
            continue
        if (desc.get("sha256") is None
                or not lease_mod.timestamp_only_mismatch(published, live)):
            return {"ok": False, "refusal": "restage-origin-changed"}
        if not verify:
            unverified.append(path)
            continue
        repin, why = _verify_origin_content(path, published, live,
                                            desc["sha256"], where=where)
        if repin is None:
            return {"ok": False, "refusal": "restage-origin-changed",
                    "detail": f"{path}: {why}"}
        repins.append(repin)
    if unverified:
        return {"ok": False, "refusal": "restage-origin-unverified",
                "paths": unverified}
    return {"ok": True, "repins": repins}


def _recheck_origin_identity(filed: Mapping[str, object],
                             sealed: list[dict[str, object]],
                             entry: Mapping[str, object] | None = None
                             ) -> tuple[bool, str | None]:
    """`_check_origin_identity` without a read: ``(True, None)`` or ``(False, refusal)``."""

    verdict = _check_origin_identity(filed, sealed, entry)
    return (bool(verdict["ok"]), verdict.get("refusal"))


def _apply_origin_repins(entry: Mapping[str, object],
                         filed: Mapping[str, object],
                         sealed: list[dict[str, object]],
                         repins: Sequence[Mapping[str, object]]
                         ) -> tuple[dict[str, object] | None, dict[str, str] | None]:
    """``entry`` with verified re-pins appended, checked under the caller's lock.

    Each origin is stat'ed again: it must still be the identity its content
    check hashed, or it changed after the read and refuses. An origin whose
    entry already carries that identity (another caller filed the same
    re-pin) adds nothing. Returns ``(entry, None)`` -- the same mapping when
    nothing is added -- or ``(None, refusal)``.
    """

    from prismabuild import reader_lease as lease_mod

    try:
        current = _effective_origin_identity(filed, sealed, entry)
    except ProducedOutputError as exc:
        return (None, {"refusal": str(exc)})
    if current is None:
        return (None, {"refusal": "restage-origin-proof-missing"})
    added: list[dict[str, object]] = []
    for repin in repins:
        path = str(repin["path"])
        try:
            live = _portable_identity_of(os.lstat(path))
        except OSError as exc:
            return (None, {"refusal": f"restage-origin-unstatable: {exc}"})
        if not lease_mod.file_id_matches(repin["to"], live):
            return (None, {"refusal": "restage-origin-changed",
                           "detail": f"{path}: the file changed after its "
                                     f"content was verified"})
        if lease_mod.file_id_matches(current.get(path), live):
            continue
        if current.get(path) != repin["from"]:
            return (None, {"refusal": "restage-origin-changed",
                           "detail": f"{path}: another identity was filed "
                                     f"after its content was verified"})
        added.append(dict(repin))
        current[path] = dict(repin["to"])
    if not added:
        return (dict(entry), None)
    return ({**entry, "origin_repins": [*(entry.get("origin_repins") or []),
                                        *added]}, None)


def _materializations(entry: Mapping[str, object]) -> list[dict[str, object]]:
    """The entry's restage materializations, checked whole, oldest first.

    A malformed `materializations` list is unknown state and RAISES, never
    reads as empty: retirement, the censuses and the successor gate all turn
    on it, and an unreadable list that answered "none" would orphan a live
    stage copy. An absent or empty list is a batch that was never restaged.

    THE ONE validator. Per-row shape is not enough, because the dangerous
    histories are the internally inconsistent ones rather than the malformed
    ones: `[gen1 live, gen2 retired]` passes every row check, yet
    `_active_materialization` would read the last row, answer "retired", and
    authorize a reclaim over a live earlier mover. So the legal sequential
    history is enforced HERE, once, for every caller -- `_active_*`,
    `_live_*`, `_batch_stage_retired`, the censuses and the retain paths all
    read through this function and inherit it:

    * generations are 1..N in order (the per-row check above);
    * every generation before the last is retired -- only the LATEST may be
      live, so no earlier stage copy can be forgotten;
    * the batch's own first copy is retired whenever any successor exists;
    * no mover key repeats, including the batch's own first mover key, so a
      spent terminal key can never be read as a live successor.

    None of these are transitions the lane can reach: each one is corruption,
    and corruption fails RETAIN.
    """

    raw = entry.get("materializations")
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ProducedOutputError("unknown-retain: materializations")
    out: list[dict[str, object]] = []
    for index, item in enumerate(raw):
        if (not isinstance(item, Mapping)
                or len(str(item.get("mover_key") or "")) != 64
                or any(c not in _HEX for c in str(item.get("mover_key") or ""))
                or not isinstance(item.get("tier"), str)
                or not item.get("tier")
                or type(item.get("generation")) is not int
                or int(item["generation"]) != index + 1
                or type(item.get("retired")) is not bool
                or str(item.get("state") or "") not in ("intent", "funded")):
            raise ProducedOutputError("unknown-retain: materializations")
        out.append(dict(item))
    if out:
        if not bool(entry.get("retired")):
            raise ProducedOutputError("unknown-retain: materializations")
        seen = {str(entry.get("mover_key") or "")}
        for index, item in enumerate(out):
            key = str(item["mover_key"])
            if key in seen:
                raise ProducedOutputError("unknown-retain: materializations")
            seen.add(key)
            if index < len(out) - 1 and not item["retired"]:
                raise ProducedOutputError("unknown-retain: materializations")
    return out


def _live_materialization(entry: Mapping[str, object]
                          ) -> dict[str, object] | None:
    """The single live (unretired) restage materialization, or None.

    The lane is a sequential writer (one producer action per instance), and
    the bounded-window path stages one window at a time, so at most ONE
    materialization may be live or pending per logical batch. More than one is
    corrupt state and raises rather than guessing which one owns the material.
    """

    live = [item for item in _materializations(entry)
            if not item.get("retired")]
    if len(live) > 1:
        raise ProducedOutputError("unknown-retain: materializations")
    return live[0] if live else None


def _active_materialization(entry: Mapping[str, object]) -> dict[str, object]:
    """Which mover owns this batch's stage copy RIGHT NOW.

    The one question every retire, census and row-preparation path must ask
    instead of reading `entry["retired"]`, which only ever describes the FIRST
    materialization. After a restage the first one is retired and a successor
    owns the material; a path that kept reading the entry flag would call a
    live stage copy retired and orphan it.

    Returns the latest materialization: the last filed restage row when there
    is one, else the batch's own first publication as generation 0. Raises on
    a malformed list (unknown retains).
    """

    if entry.get("origin_only") is True:
        # An origin-only batch (#912) never had a stage copy, so every reader
        # that asks "is a copy live?" hears no.  Path ownership is the one
        # question that must not follow this answer; `_live_path_owner` asks
        # it of the origin instead.
        return {"mover_key": "", "tier": str(entry.get("tier") or ""),
                "generation": 0, "retired": True, "state": "origin-only",
                "staged_paths": [], "source": "origin"}
    mats = _materializations(entry)
    if mats:
        latest = dict(mats[-1])
        latest["source"] = "materialization"
        return latest
    return {"mover_key": str(entry.get("mover_key") or ""),
            "tier": str(entry.get("tier") or ""),
            "generation": 0,
            "retired": bool(entry.get("retired")),
            "state": "funded",
            "staged_paths": list(entry.get("staged_paths") or []),
            "source": "batch"}


def _all_materialization_movers(entry: Mapping[str, object]) -> list[str]:
    """Every mover key that has ever owned this batch's material, in order."""

    keys = [str(entry.get("mover_key") or "")]
    keys += [str(item.get("mover_key")) for item in _materializations(entry)]
    return [key for key in keys if len(key) == 64]


def _batch_stage_retired(entry: Mapping[str, object]) -> bool:
    """Is NO stage copy of this batch live or pending right now?

    The replacement for every `entry.get("retired")` test outside the commit
    path. A restaged batch's own flag says only that its FIRST copy is gone;
    what callers actually need is whether the LATEST materialization is
    retired, which is what this answers. Raises on a malformed materialization
    list, so unknown retains instead of reading as retired.
    """

    return bool(_active_materialization(entry).get("retired"))


def _committed_restage_authority(queue, checked_instance, checked_template,
                                 batch_id: str, manifest: str,
                                 mover_key: str) -> dict[str, object] | None:
    """The restageable committed batch as a funding authority, or None.

    Read-only. The prewrite was consumed by the commit that made this batch
    durable, so a re-materialization has no prewrite to present and must found
    its funding on the immutable record itself -- and ONLY in the exact state
    where restaging is the designed transition:

    * the mutable commitments entry agrees with the immutable record on
      mover/tier/namespace and the recomputed manifest digest equals the seal
      (so caller descriptor drift refuses);
    * the FIRST materialization is stage-retired -- its egress completed, so
      no pin can be holding it -- and the origins were never reclaimed;
    * exactly ONE live materialization is filed and it names THIS mover key.
      The caller therefore cannot pick a successor id: only the key PB itself
      sealed and filed under the ownership lock is fundable.
    * the sealed origins still carry the identity the first commit recorded,
      or the one its entry re-pinned after a content check (#1111).

    A damaged record or commitments file raises `unknown-retain` (fail
    closed), and a changed or unprovable origin raises its own typed reason so
    funding refuses by name; an absent entry, an unretired batch, a
    reclaimed-origin batch, a digest disagreement or a mover key with no filed
    materialization returns None and the caller keeps its ordinary refusal.
    """

    try:
        commitments = _read_commitments(
            _commitments_path(queue.root, checked_instance))
    except ProducedOutputError as exc:
        raise ProducedOutputError(f"unknown-retain: {exc}") from None
    batches = commitments["batches"]
    assert isinstance(batches, Mapping)
    entry = batches.get(batch_id)
    if not isinstance(entry, Mapping):
        return None
    try:
        filed, sealed = _load_batch_record(
            queue.root, checked_instance, checked_template, entry, batch_id)
    except ProducedOutputError as exc:
        raise ProducedOutputError(str(exc)) from None
    try:
        namespace = batch_namespace(checked_instance, batch_id, manifest)
    except ProducedOutputError as exc:
        raise ProducedOutputError(str(exc)) from None
    if (not entry.get("retired")
            or entry.get("origin_reclaimed")
            or str(entry.get("manifest_digest") or "") != manifest
            or str(filed.get("manifest_digest") or "") != manifest
            or str(entry.get("batch_namespace") or "") != namespace
            or str(filed.get("batch_namespace") or "") != namespace):
        return None
    live = _live_materialization(entry)
    if live is None or str(live.get("mover_key")) != str(mover_key):
        return None
    # Against the entry's re-pins, and never a read: the restage filed any
    # re-pin under the lock before this key was sealed (#1111).
    ok, refusal = _recheck_origin_identity(filed, sealed, entry)
    if not ok:
        raise ProducedOutputError(str(refusal))
    return dict(entry)


def _append_materialization_locked(
        queue, checked_instance, checked_template, batch_id: str, *,
        mover_key: str, tier: str, generation: int, host: str,
        fill: int | None = None) -> bool:
    """File one restage INTENT under the caller's ownership lock.

    This is the durable resumption point, and it is filed BEFORE any funding
    or movement side effect reaches the pool: a crash at any later prefix
    finds this row, re-drives the SAME sealed mover key, and allocates neither
    a fresh generation nor a second credit.

    Re-validates provenance exactly as the primary commit path does (the
    immutable record loads; the entry still agrees on mover/tier/namespace;
    the first materialization is stage-retired with its origin charge intact),
    re-derives the generation from the record on disk, and appends
    `{mover_key, tier, generation, retired: False, state: "intent", host}`,
    plus `fill_mb_s_pool_side` when the sealed mover reserved pool fill
    (#747): a resume republishes the row from this record without re-sealing,
    and a row's resources must equal its sealed demand.
    Idempotent: a materialization already filed with this mover key returns
    True without appending. Any provenance failure raises.
    """

    path = _commitments_path(queue.root, checked_instance)
    try:
        commitments = _read_commitments(path)
    except ProducedOutputError as exc:
        raise ProducedOutputError(f"unknown-retain: {exc}") from None
    batches = commitments["batches"]
    assert isinstance(batches, dict)
    entry = batches.get(batch_id)
    if not isinstance(entry, Mapping):
        raise ProducedOutputError("unknown batch_id for this instance")
    try:
        filed, _sealed = _load_batch_record(
            queue.root, checked_instance, checked_template, entry, batch_id)
    except ProducedOutputError as exc:
        raise ProducedOutputError(str(exc)) from None
    if (not entry.get("retired")
            or entry.get("origin_reclaimed")
            or str(entry.get("mover_key") or "")
            != str(filed.get("mover_key") or "")
            or str(entry.get("tier") or "") != str(filed.get("tier") or "")
            or str(filed.get("tier") or "") != tier):
        raise ProducedOutputError("unknown-retain: restage-target-mismatch")
    existing = _materializations(entry)
    for item in existing:
        if str(item.get("mover_key")) == str(mover_key):
            return True
    if _live_materialization(entry) is not None:
        raise ProducedOutputError("materialization-live-retain")
    if len(existing) + 1 != generation:
        # Another restage (or a crash-retry) advanced the list between this
        # caller's read and this append; the sealed key it published belongs
        # to the generation it read. Refuse rather than filing a
        # generation-mismatched row: every step is idempotent, so the retry
        # re-derives the current generation and heals by re-calling.
        raise ProducedOutputError("restage-generation-changed")
    row: dict[str, object] = {
        "mover_key": str(mover_key), "tier": tier,
        "generation": int(generation), "retired": False,
        "state": "intent", "host": str(host)}
    if fill is not None:
        row["fill_mb_s_pool_side"] = int(fill)
    updated = dict(entry)
    updated["materializations"] = existing + [row]
    batches[batch_id] = updated
    try:
        _write_commitments(path, {"batches": batches})
    except ProducedOutputError as exc:
        raise ProducedOutputError(f"unknown-retain: {exc}") from None
    return False


def _mark_materialization_funded_locked(
        queue, checked_instance, batch_id: str, *, mover_key: str) -> None:
    """Advance one filed materialization intent to `funded` under the lock.

    Bookkeeping only -- the authority is the pool's own funding record, which
    this caller has just read as `transferring` with the tokens held under the
    mover. Idempotent, and it never invents a row: a mover key with no filed
    materialization raises rather than filing one late.
    """

    path = _commitments_path(queue.root, checked_instance)
    try:
        commitments = _read_commitments(path)
    except ProducedOutputError as exc:
        raise ProducedOutputError(f"unknown-retain: {exc}") from None
    batches = commitments["batches"]
    assert isinstance(batches, dict)
    entry = batches.get(batch_id)
    if not isinstance(entry, Mapping):
        raise ProducedOutputError("unknown batch_id for this instance")
    items = _materializations(entry)
    changed = False
    for index, item in enumerate(items):
        if str(item.get("mover_key")) == str(mover_key):
            if item.get("retired"):
                raise ProducedOutputError(
                    "unknown-retain: materialization-retired")
            if str(item.get("state")) != "funded":
                item = dict(item)
                item["state"] = "funded"
                items[index] = item
                changed = True
            break
    else:
        raise ProducedOutputError("unknown-retain: materializations")
    if not changed:
        return
    updated = dict(entry)
    updated["materializations"] = items
    batches[batch_id] = updated
    try:
        _write_commitments(path, {"batches": batches})
    except ProducedOutputError as exc:
        raise ProducedOutputError(f"unknown-retain: {exc}") from None


def _mark_materialization_retired_locked(
        batches: dict, batch_id: str, *, mover_key: str,
        receipt: Mapping[str, object], canonical_ns: str,
        staged_paths: list[str]) -> bool:
    """Mark one materialization's stage retirement in ``batches``.

    ``batches`` is the commitments document the caller read under its
    ownership lock, and the caller writes it back under the same lock: this
    marks, it does not read or write (#1072).  Returns whether it changed
    anything.

    Proof-checked exactly like the primary retirement: the egress receipt must
    be complete, error-free, and name THIS materialization's mover with the
    batch's canonical namespace. The namespace is digest-derived and shared by
    every materialization of one batch, so the reader contract is unchanged.
    The durable charge is NOT touched -- retiring a materialization returns
    window credit only, and the origin charge stays constant across forward
    and reverse staging.
    """

    if not isinstance(receipt, Mapping) or receipt.get("complete") is not True:
        raise ProducedOutputError("retire needs a complete egress receipt")
    if receipt.get("errors"):
        raise ProducedOutputError("retire needs an error-free egress receipt")
    if (str(receipt.get("action_key") or "") != str(mover_key)
            or str(receipt.get("consumer_action_key") or "") != canonical_ns):
        raise ProducedOutputError("retire receipt names another materialization")
    entry = batches.get(batch_id)
    if not isinstance(entry, Mapping):
        raise ProducedOutputError("unknown batch_id for this instance")
    items = _materializations(entry)
    for index, item in enumerate(items):
        if str(item.get("mover_key")) == str(mover_key):
            if item.get("retired"):
                return False
            item = dict(item)
            item["retired"] = True
            item["staged_paths"] = sorted(set(staged_paths))
            items[index] = item
            break
    else:
        raise ProducedOutputError("unknown-retain: materializations")
    updated = dict(entry)
    updated["materializations"] = items
    batches[batch_id] = updated
    return True


# --------------------------------------------------------------------------
# Admission credit (bound record, NOT ledger tokens) + per-batch prewrite
# --------------------------------------------------------------------------

def admit_instance(queue, instance: Mapping[str, object],
                   template: Mapping[str, object]) -> dict[str, object]:
    """Record the instance's bound admission metadata (idempotent).

    Copies the template's per-tier minimum/window demands into commitments
    as the scope side of the funding contract. This record is BINDING
    metadata only: it authorizes no bytes and funds nothing. Physical
    funding happens per batch at `commit_batch` (exact ledger acquire +
    whole transfer); general window admission is `admit_funded_window`
    (pending on the liveness primitive). Returns {"ok": True, ...}.
    """

    try:
        checked_template, checked_instance = _require_bound_contract(
            template, instance)
    except ProducedOutputError:
        return {"ok": False, "refusal": "template-mismatch"}
    with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
        path = _commitments_path(queue.root, checked_instance)
        try:
            commitments = _read_commitments(path)
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        demands = checked_template["working_demands"]
        assert isinstance(demands, dict)
        admission = {
            "minimum_gib": {tier: int(demands[tier]["minimum_gib"])
                            for tier in checked_template["permitted_tiers"]},
            "window_gib": {tier: int(demands[tier]["window_gib"])
                           for tier in checked_template["permitted_tiers"]},
            "bound_unix": time.time(),
        }
        _write_commitments(path, {"batches": commitments["batches"],
                                  "admission": admission})
    return {"ok": True, "admission": admission}


def reserve_working_minimum(queue, instance: Mapping[str, object],
                            template: Mapping[str, object]) -> dict[str, object]:
    """Deprecated alias of `admit_instance` (binding metadata, never funding)."""

    return admit_instance(queue, instance, template)


def admit_funded_window(queue, instance: Mapping[str, object],
                        template: Mapping[str, object], *,
                        need_gib_per_tier: Mapping[str, int]) -> dict[str, object]:
    """General funded-window admission gate (liveness-owned primitive).

    Fully validates the window request (bound instance + template match,
    permitted tiers, positive-integer needs, need within window demand and
    within minted tier capacity). With the funded-claim primitive family
    delivered (the prepaid-output lane: ``reserve_fence`` /
    ``transfer_tokens`` / ``funded_cover`` on ``PoolQueue``), this is the
    supported window binding: the producer action reserves its window ONCE
    as its own tier demand (``owner_demand_terms`` at submit), and every
    batch funds from those holdings by exact transfer -- never a second
    acquisition from free. Returns the binding the producer driver and the
    PQ integrator seal against; a pool missing any primitive still answers
    the pending refusal.
    """

    from prismabuild import pool as pool_mod
    from prismabuild import storage_tiers as tiers_mod

    checked_template = validate_template(template)
    try:
        checked_instance = validate_instance(instance)
    except ProducedOutputError as exc:
        return {"ok": False, "refusal": f"bad-instance: {exc}"}
    if checked_instance["template_sha256"] != template_sha256(checked_template):
        return {"ok": False, "refusal": "template-mismatch"}
    if checked_template.get("write_only"):
        # A write-only template has no window to fund (#912).
        return {"ok": False, "refusal": "template-is-write-only"}
    if not isinstance(need_gib_per_tier, Mapping) or not need_gib_per_tier:
        return {"ok": False, "refusal": "bad-window-need"}
    for tier, need in need_gib_per_tier.items():
        if tier not in checked_template["permitted_tiers"]:
            return {"ok": False, "refusal": "tier-not-permitted", "tier_id": tier}
        if type(need) is not int or need <= 0:
            return {"ok": False, "refusal": "bad-window-need", "tier_id": tier}
        window = int(checked_template["working_demands"][tier]["window_gib"])
        if need > window:
            return {"ok": False, "refusal": "window-need-exceeds-demand",
                    "tier_id": tier}
        kind = tiers_mod.capacity_kind_of(tier)
        try:
            capacity = queue.tier_ledger(tier).capacity().get(kind, 0)
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if need > capacity:
            return {"ok": False, "refusal": "never-fits-tier-capacity",
                    "tier_id": tier}
    delivered = [name for name in ("reserve_fence", "funded_cover")
                 if callable(getattr(pool_mod.PoolQueue, name, None))]
    if callable(getattr(pool_mod.ResourceLedger, "transfer_tokens", None)):
        delivered.append("transfer_tokens")
    if len(delivered) == 3:
        # CAPABILITY/WINDOW DECLARATION ONLY (prepaid-output lane): this
        # reports that the funded-claim primitive family exists and what
        # the producer must declare -- the owner action's own tier demand
        # (reserved once at its claim). It does NOT verify that any owner
        # physically holds window tokens right now; the authority is the
        # per-batch funding itself (stage_output_intent ->
        # publish_prepaid_batch/fund_output_batch -> commit_batch), which
        # moves exact names owner->mover and never re-acquires from free.
        return {"ok": True, "mode": "prepaid-per-batch",
                "declaration_only": True,
                "authority": "per-batch-funding",
                "owner_action_key": str(checked_instance["owner_action_key"]),
                "owner_demand_terms": owner_demand_terms(checked_template),
                "window_gib": {tier: int(
                    checked_template["working_demands"][tier]["window_gib"])
                    for tier in checked_template["permitted_tiers"]},
                "batch_ref_schema": pool_mod.PRODUCED_OUTPUT_BATCH_REF_SCHEMA_V1,
                "sequence": ["require_prewrite", "publish_prepaid_batch",
                             "fleet-claims-the-mover", "retire_batch",
                             "reclaim_origin", "safe_release_instance"]}
    return {"ok": False, "refusal": "funding-primitive-pending",
            "liveness_draft_present": delivered,
            "dependency": ("liveness funded-claim primitive: funding record "
                           "binding credit to exact tier/plan-window/mover/"
                           "range/generation + eligible-token verification + "
                           "serialized transfer + current+next window "
                           "admission")}


def require_prewrite(queue, instance: Mapping[str, object],
                     template: Mapping[str, object], *, batch_id: str,
                     tier: str, class_bytes: Mapping[str, int],
                     paths: list[str]) -> dict[str, object]:
    """File a prewrite budget claim BEFORE any HDD byte is written.

    The production writer path must call this (not an optional helper):
    uncharged temp/checkpoint writes refuse here. `paths` names the PLANNED
    durable-origin files of this batch (absolute, normalized, under the
    template prefix, distinct) -- a conservative superset is allowed, and
    the commit's descriptors must be a subset of it. `class_bytes` are
    per-class CEILINGS for the batch's temporary lifetime: the headroom
    charges them while the prewrite lives, and the commit's actual bytes
    must land at or under each bound (exact sizes remain the special case
    of an exact ceiling). Abort requires every PLANNED path absent.
    Uncommitted does NOT mean unwritten: files without a batch never enter
    staged accounting.
    Checks the bound admission record (binding metadata, never funding),
    durable headroom for the planned class bytes, and PATH OWNERSHIP --
    one origin path has at most one live writer, so a path a committed
    unretired batch or another outstanding prewrite already owns refuses
    `prewrite-path-owned-by-live-batch` here rather than at the mover,
    after the bytes are written (`_live_path_owner`). Ownership ends at
    retirement, which evicts the staged copy. Across actions (#1053), a
    path another action's LIVE attempt has committed (unless reclaimed) or
    prewritten refuses `prewrite-path-owned-by-live-action`, naming that
    owner (`_foreign_live_path_owner`); a dead or finished action's paths
    are free to regenerate, which is how a relaunch resumes. Then files an immutable
    prewrite record the later commit must present. Physical funding happens
    only at `commit_batch` (exact ledger acquire) and in the liveness
    window primitive (pending). Zero-byte classes are valid (explicit
    zeros, never missing keys).
    """

    try:
        checked_template, checked_instance = _require_bound_contract(
            template, instance)
    except ProducedOutputError:
        return {"ok": False, "refusal": "template-mismatch"}
    _name(batch_id, where="batch_id")
    if tier not in checked_template["permitted_tiers"]:
        return {"ok": False, "refusal": "tier-not-permitted"}
    if not isinstance(class_bytes, Mapping) or set(class_bytes) != {
            "payload", "checkpoint", "temp"}:
        return {"ok": False, "refusal": "prewrite-classes-must-name-all-three"}
    planned = {}
    for cls in ("payload", "checkpoint", "temp"):
        planned[cls] = _nonneg_int(class_bytes.get(cls),
                                   where=f"prewrite class_bytes.{cls}")
    if not isinstance(paths, list) or not paths:
        return {"ok": False, "refusal": "prewrite-paths-required"}
    planned_paths: list[str] = []
    for path in paths:
        candidate = _abs_norm(path, where="prewrite paths[]")
        _resolve_contained(str(checked_template["output_prefix"]), candidate,
                           where="prewrite paths[]")
        planned_paths.append(os.path.normpath(candidate))
    if len(set(planned_paths)) != len(planned_paths):
        return {"ok": False, "refusal": "prewrite-paths-must-be-distinct"}
    planned_paths.sort()
    with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
        # Write authorization lives here: a SUCCESS return is the producer's
        # permission to start writing HDD bytes, so a stale, superseded, or
        # absent owner refuses BEFORE the first payload byte -- not later at
        # commit. The idempotent duplicate replay below re-checks the same
        # gate: a read-only duplicate grants no permission for new writes.
        gated = _require_live_owner(queue, checked_instance)
        if gated is not None:
            return gated
        try:
            sums = _outstanding_sums(queue.root, checked_instance, batch_id)
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": str(exc)}
        if not isinstance(_read_commitments(
                _commitments_path(queue.root, checked_instance)).get(
                "admission"), Mapping):
            # The bound admission record proves the instance was admitted;
            # without it nothing is prewritable. (Zero-minimum tiers still
            # need the bound record, never ledger presence.)
            return {"ok": False, "refusal": "prewrite-not-admitted"}
        maxima = checked_instance_maxima(checked_template)
        for cls in ("payload", "checkpoint", "temp"):
            cap = maxima[f"{cls}_max_bytes"]
            if sums[cls] + planned[cls] > cap:
                return {"ok": False, "refusal": f"prewrite-exceeds-{cls}-maxima",
                        "class": cls}
        # One origin path, one live writer. Staged material identity is
        # keyed by the path, so authorizing a second writer here would
        # authorize bytes the mover must later refuse -- after they are
        # written and committed. This gate is where that is answered,
        # before the first payload byte, which is the contract this
        # function already states for a stale or absent owner.
        try:
            owned = _live_path_owner(queue.root, checked_instance,
                                     checked_template, planned_paths,
                                     batch_id)
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if owned is not None:
            owned_path, owner_kind, owner_batch_id = owned
            return {"ok": False,
                    "refusal": "prewrite-path-owned-by-live-batch",
                    "path": owned_path, "owner_kind": owner_kind,
                    "owner_batch_id": owner_batch_id}
        # The same rule across actions (#1053): a path another action's
        # live attempt has committed or prewritten is that attempt's. Under
        # this lock for every template with this output prefix; a template
        # whose prefix only overlaps takes its own lock, and the delete side
        # (`_unlink_if_committed`) does not rely on this check alone.
        try:
            foreign = _foreign_live_path_owner(
                queue, checked_instance, checked_template, planned_paths)
        except ProducedOutputError as exc:
            reason = str(exc)
            if not reason.startswith("unknown-retain"):
                reason = f"unknown-retain: {reason}"
            return {"ok": False, "refusal": reason}
        if foreign is not None:
            return {"ok": False,
                    "refusal": "prewrite-path-owned-by-live-action", **foreign}
        path = _prewrites_dir(queue.root, checked_instance) / \
            f"{batch_id}.prewrite.json"
        try:
            _publish_prewrite_record(
                queue, checked_instance, batch_id=batch_id, tier=tier,
                class_bytes=planned, paths=planned_paths)
        except Exception as exc:
            # Same body republishes idempotently; a different body for a
            # live batch id refuses (the first reservation stands).
            try:
                existing = _read_prewrite(path)
            except ProducedOutputError as inner:
                return {"ok": False, "refusal": f"prewrite-unreadable: {inner}"}
            if (existing is not None and existing.get("tier") == tier
                    and dict(existing.get("class_bytes", {})) == planned
                    and list(existing.get("paths", [])) == planned_paths):
                return {"ok": True, "batch_id": batch_id,
                        "class_bytes": planned, "paths": planned_paths,
                        "duplicate": True}
            return {"ok": False, "refusal": f"prewrite-conflict: {exc}"}
    return {"ok": True, "batch_id": batch_id, "class_bytes": planned,
            "paths": planned_paths}


def abort_prewrite(queue, instance: Mapping[str, object],
                   template: Mapping[str, object], *, batch_id: str) -> dict[str, object]:
    """Abort an outstanding prewrite after proving nothing durable remains.

    Allowed only while the batch is uncommitted. Uncommitted does NOT mean
    unwritten, so every planned path must be absent (safe disposal is the
    producer's job: this lane never deletes durable-origin files): a
    present file refuses `abort-files-present-retain`, an unstatable one
    retains unknown. Frees the outstanding accounting headroom; no ledger
    tokens move because prewrites hold none (physical funding happens at
    commit). Returns {"ok": True, "aborted": ...}.
    """

    try:
        checked_template, checked_instance = _require_bound_contract(
            template, instance)
    except ProducedOutputError:
        return {"ok": False, "refusal": "template-mismatch"}
    _name(batch_id, where="batch_id")
    with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
        try:
            commitments = _read_commitments(
                _commitments_path(queue.root, checked_instance))
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        if batch_id in batches:
            return {"ok": False, "refusal": "batch-committed"}
        try:
            prewrite = _read_prewrite(
                _prewrites_dir(queue.root, checked_instance)
                / f"{batch_id}.prewrite.json")
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if prewrite is None:
            return {"ok": True, "batch_id": batch_id, "aborted": False}
        # Prepaid lane (R7 liveness): while a pool funding intent names this
        # batch, the prewrite is its precommit recovery authority (drive
        # accepts precommit-OR-commit only while the owner is live). Deleting
        # it here would strand the intent -- and any credits it already
        # transferred -- with no completion path. Retire the intent first
        # (release_output_funding proves mover nonexecution); abort refuses
        # while one exists. An unreadable census retains the same way.
        try:
            intents, census_unknown = queue.output_census_for_owner(
                str(checked_instance["owner_action_key"]))
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if census_unknown:
            return {"ok": False, "refusal": "unknown-retain: funding-census"}
        _attempt = checked_instance["owner_attempt"]
        assert isinstance(_attempt, dict)
        for record in intents:
            if (str(record.get("batch_id")) == batch_id
                    and str(record.get("tier_id"))
                    == str(prewrite.get("tier"))
                    and str(record.get("template_sha256"))
                    == template_sha256(checked_template)
                    and str(record.get("owner_nonce"))
                    == str(_attempt["nonce"])
                    and str(record.get("owner_scope_id"))
                    == str(_attempt["scope_id"])):
                return {"ok": False,
                        "refusal": "prepaid-intent-exists-retain",
                        "mover_action_key": str(
                            record.get("mover_action_key")),
                        "intent_state": str(record.get("state"))}
        for planned in prewrite.get("paths", []):
            try:
                os.lstat(str(planned))
            except FileNotFoundError:
                continue
            except OSError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            return {"ok": False, "refusal": "abort-files-present-retain",
                    "path": str(planned)}
        path = _prewrites_dir(queue.root, checked_instance) / f"{batch_id}.prewrite.json"
        try:
            path.unlink()
        except FileNotFoundError:
            return {"ok": True, "batch_id": batch_id, "aborted": False}
        except OSError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    return {"ok": True, "batch_id": batch_id, "aborted": True}


def commit_batch(queue, instance: Mapping[str, object],
                 template: Mapping[str, object],
                 descriptors: list[Mapping[str, object]], *, batch_id: str,
                 tier: str, mover_key: str) -> dict[str, object]:
    """Commit one immutable batch: check, prewrite-match, acquire exact under
    the batch holder, TRANSFER whole to mover ownership (no free interval),
    file the batch record. All-or-nothing with typed refusals."""

    from prismabuild import pool as pool_mod
    from prismabuild import storage_tiers as tiers_mod

    try:
        checked_template, checked_instance = _require_bound_contract(
            template, instance)
    except ProducedOutputError:
        return {"ok": False, "refusal": "template-mismatch"}
    _name(batch_id, where="batch_id")
    if checked_template.get("write_only"):
        # A write-only template's batches commit at origin and are never
        # staged by their owner (#912); this staged path is not theirs.
        return {"ok": False, "refusal": "template-is-write-only"}
    mover = _hex64(mover_key, where="batch mover_key")
    if tier not in checked_template["permitted_tiers"]:
        return {"ok": False, "refusal": "tier-not-permitted"}
    sealed = [validate_descriptor(d, checked_template, checked_instance)
              for d in descriptors]
    class_bytes = {"payload": 0, "checkpoint": 0, "temp": 0}
    for desc in sealed:
        class_bytes[str(desc["artifact_class"])] += int(desc["bytes"])
    manifest_digest = output_manifest_sha256(sealed)
    batch_ns = batch_namespace(checked_instance, batch_id, manifest_digest)
    kind = tiers_mod.capacity_kind_of(tier)
    batch_total = sum(class_bytes.values())
    batch_gib = tiers_mod.stage_tokens_for_bytes(batch_total)
    window = int(checked_template["working_demands"][tier]["window_gib"])
    if batch_gib > window:
        return {"ok": False, "refusal": "batch-exceeds-window",
                "batch_gib": batch_gib, "window_gib": window}
    with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
        # One lstat per origin, answering both questions at the same instant:
        # the size the descriptor claims, and the exact file identity this
        # commit is over. No payload reread -- digests ride the writer receipt
        # and the mover verifies on copy. The identity tuple is what lets the
        # SAME batch be materialized again later over provably the same bytes
        # (see `ensure_batch_materialized`): for a DEV null-digest descriptor
        # it is the only such proof, and a size check alone would bless a
        # rewritten file of equal length. Taken under this lock (#1064): a
        # retirement's delete, which holds it, can move an origin aside and
        # link it back (`_unlink_if_committed`), which moves its ctime, so an
        # identity taken before the lock could be stale before it was filed.
        origin_identity, identity_refusal = _origin_identity_at_commit(sealed)
        if identity_refusal is not None:
            return identity_refusal
        assert origin_identity is not None
        try:
            commitments = _read_commitments(
                _commitments_path(queue.root, checked_instance))
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        if batch_id in batches:
            existing = batches[batch_id]
            if (isinstance(existing, Mapping)
                    and existing.get("manifest_digest") == manifest_digest):
                return {"ok": True, "batch_id": batch_id, "duplicate": True,
                        "batch_namespace": batch_ns,
                        "manifest_digest": manifest_digest}
            return {"ok": False, "refusal": "batch-id-in-use"}
        gated = _require_live_owner(queue, checked_instance)
        if gated is not None:
            return gated
        try:
            prewrite = _read_prewrite(
                _prewrites_dir(queue.root, checked_instance)
                / f"{batch_id}.prewrite.json")
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"prewrite-unreadable: {exc}"}
        if prewrite is None:
            return {"ok": False, "refusal": "prewrite-reservation-missing"}
        # Ceiling reconciliation (conservative prewrite -> actual): same
        # binding fields; actual paths subset of the planned superset;
        # actual per-class bytes at or under the admitted ceiling. The
        # prewrite is consumed here either way, and the class maxima were
        # charged against the (larger) ceiling while it lived.
        if (prewrite.get("tier") != tier
                or not _actual_within_ceiling(prewrite, sealed)
                or not set(str(d["path"]) for d in sealed)
                <= set(prewrite.get("paths", []))
                or prewrite.get("owner_action_key")
                != checked_instance["owner_action_key"]
                or dict(prewrite.get("owner_attempt", {})) != dict(
                    checked_instance["owner_attempt"])):
            return {"ok": False, "refusal": "prewrite-mismatch"}
        if not _planned_omitted_absent(prewrite, sealed):
            return {"ok": False, "refusal": "planned-path-present-retain"}
        try:
            sums = _class_sums(batches)
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        maxima = checked_instance_maxima(checked_template)
        for cls in sums:
            if sums[cls] + class_bytes[cls] > maxima[f"{cls}_max_bytes"]:
                return {"ok": False, "refusal": f"commit-exceeds-{cls}-maxima"}
        # Prepaid lane (R7 integration): one authoritative pool funding
        # record at (mover, tier). A batch that staged its intent funds by
        # exact transfer from the producer's already-reserved window
        # (stage_output_intent -> publish -> fund_output_batch); commit
        # reconciles that record and files the batch with NO second
        # acquisition from free. The legacy per-batch intent below remains
        # only as in-flight recovery for batches that already filed it in
        # the acquire-from-free era. Unknown pool funding state never
        # downgrades to the legacy path (that would double-fund).
        funding_path = (_funding_dir(queue.root, checked_instance)
                        / f"{batch_id}.funding.json")
        ledger = queue.tier_ledger(tier)
        try:
            _frec, _fstate = queue.output_funding_file_state(mover, tier)
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if _fstate == "corrupt":
            return {"ok": False,
                    "refusal": "unknown-retain: pool funding unreadable"}
        prepaid = _frec
        if prepaid is not None:
            if (str(prepaid.get("batch_id")) != batch_id
                    or str(prepaid.get("manifest_digest")) != manifest_digest
                    or str(prepaid.get("tier_id")) != tier
                    or str(prepaid.get("owner_action_key")) != str(
                        checked_instance["owner_action_key"])
                    or str(prepaid.get("template_sha256")) != template_sha256(
                        checked_template)
                    or int(prepaid.get("range_start_bytes")) != 0
                    or int(prepaid.get("range_end_bytes")) != batch_total):
                return {"ok": False, "refusal": "batch-id-in-use"}
            if str(prepaid.get("state")) == "reserved":
                # Publish-before-fund crash prefix or an unwound drive:
                # finish the transfer through the same recovery API a
                # restart would use, never a second reservation.
                driven = queue.drive_output_funding(mover, tier)
                if not driven.get("ok"):
                    return {"ok": False,
                            "refusal": f"prepaid-drive: "
                                       f"{driven.get('refusal')}",
                            "drive": driven}
            live_funding = queue.read_output_funding(mover, tier)
            if (live_funding is None
                    or str(live_funding.get("state")) != "transferring"):
                return {"ok": False, "refusal": "prepaid-funding-terminal",
                        "state": (str(live_funding.get("state"))
                                  if live_funding is not None
                                  else "unknown")}
            mover_now = ledger.holder_tokens(mover).get(kind, 0)
            if mover_now < batch_gib:
                # Split tokens stay split; drive retries resume. Unknown
                # loss retains, never re-funds.
                return {"ok": False, "refusal": "transfer-short",
                        "moved": mover_now, "expected": batch_gib}
        else:
            # Legacy in-flight recovery only: the per-batch intent names
            # tokens acquired from free under the batch namespace.
            try:
                funding = _read_funding(funding_path)
            except ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            if funding is not None and (
                    funding.get("mover_key") != mover
                    or funding.get("tier") != tier
                    or int(funding.get("batch_gib", -1)) != batch_gib
                    or funding.get("manifest_digest") != manifest_digest):
                return {"ok": False, "refusal": "batch-id-in-use"}
            mover_now = ledger.holder_tokens(mover).get(kind, 0)
            holder_now = ledger.holder_tokens(batch_ns).get(kind, 0)
            if funding is None and (holder_now > 0 or mover_now > 0):
                # Tokens without intent: unknown provenance (no record names
                # this funding). Never top up blindly around them.
                return {"ok": False, "refusal": "unknown-retain: unfunded holdings"}
            if holder_now == 0 and mover_now < batch_gib:
                if funding is not None:
                    # A previous attempt moved some tokens and crashed before
                    # filing the batch; the remainder is gone to unknown hands.
                    return {"ok": False, "refusal": "unknown-retain: funded tokens lost"}
                if not ledger.acquire(batch_ns, {kind: batch_gib}):
                    return {"ok": False, "refusal": "tier-reservation-unavailable",
                            "available": ledger.available()}
                # Intent names allocated tokens only: nothing is recorded
                # before the ledger holds it, so a crash before this line
                # retries clean and a crash after it resumes from the record.
                _write_funding(funding_path, {
                    "batch_id": batch_id, "tier": tier,
                    "mover_key": mover, "batch_gib": batch_gib,
                    "manifest_digest": manifest_digest})
                holder_now = batch_gib
            if holder_now > 0:
                queue.transfer_tier_reservation(tier, batch_ns, mover)
                mover_now = ledger.holder_tokens(mover).get(kind, 0)
            if mover_now < batch_gib:
                # Split tokens stay split (sum intact, nothing released);
                # retry resumes from live holdings + intent. Unknown loss
                # retains, never re-funds.
                return {"ok": False, "refusal": "transfer-short",
                        "moved": mover_now, "expected": batch_gib}
        batch_record = {
            "schema": BATCH_SCHEMA_V1,
            "batch_id": batch_id,
            "batch_namespace": batch_ns,
            "manifest_schema": BATCH_MANIFEST_SCHEMA_V1,
            "manifest_digest": manifest_digest,
            "tier": tier,
            "mover_key": mover,
            "class_bytes": class_bytes,
            "total_bytes": batch_total,
            "entry_count": len(sealed),
            "entries": sealed,
            "template_id": str(checked_template["template_id"]),
            "template_sha256": template_sha256(checked_template),
            "owner_action_key": str(checked_instance["owner_action_key"]),
            "owner_attempt": dict(checked_instance["owner_attempt"]),
            # The immutable producer record of what the origins WERE at the
            # moment this batch became durable. Restage rechecks exactly this
            # before every re-materialization and refuses a changed file; a
            # batch filed before this field existed carries no proof and can
            # never be restaged (its current bytes are never retroactively
            # blessed).
            "origin_identity": origin_identity,
            "object_set_id": manifest_object_set_id({
                f"{d['bytes']}:{d['path']}": {"bytes": int(d["bytes"]),
                                             "sha256": (d["sha256"]
                                                        if d["sha256"]
                                                        is not None else
                                                        None)}
                for d in sealed}),
            "unix": time.time(),
        }
        batch_dir = (Path(queue.root) / "residency" / OUTPUT_BATCHES_SUBDIR
                     / instance_namespace(checked_instance))
        batch_dir.mkdir(parents=True, exist_ok=True)
        batch_path = batch_dir / f"{batch_id}.json"
        try:
            pool_mod._publish_immutable(
                batch_path,
                json.dumps(batch_record, sort_keys=True,
                           separators=(",", ":")).encode() + b"\n",
                where="produced-output batch")
        except pool_mod.PoolContractError as exc:
            return {"ok": False, "refusal": f"batch-conflict: {exc}"}
        batches[batch_id] = {
            "manifest_digest": manifest_digest,
            "batch_namespace": batch_ns,
            "tier": tier,
            "mover_key": mover,
            "class_bytes": class_bytes,
            # The origin paths this batch owns while it is live. Indexed
            # here so `_live_path_owner` can answer ownership from the
            # commitments record alone: re-deriving it from the immutable
            # batch record would make one damaged record freeze every
            # later prewrite for the instance, including paths that batch
            # never owned.
            "paths": sorted(str(d["path"]) for d in sealed),
            "retired": False,
            "origin_reclaimed": False,
        }
        _write_commitments(_commitments_path(queue.root, checked_instance),
                           {"batches": batches})
        # Legacy intent + prewrite consumed only here, after the durable
        # batch publication: a crash anywhere above resumes from the
        # intent, never by re-acquiring the same budget. The prepaid pool
        # record is deliberately NOT consumed at commit: it stays
        # `transferring` until the mover's claim marks it `consumed`, and
        # release refuses it while the filed commit stands.
        _delete_funding(funding_path)
        (_prewrites_dir(queue.root, checked_instance)
         / f"{batch_id}.prewrite.json").unlink(missing_ok=True)
    return {"ok": True, "batch_id": batch_id, "batch_namespace": batch_ns,
            "manifest_digest": manifest_digest, "class_bytes": class_bytes,
            "mover_key": mover, "tier": tier, "entries": sealed,
            "funding": "prepaid" if prepaid is not None else "legacy"}


def origin_batch_ref(instance: Mapping[str, object], *, batch_id: str,
                     manifest_digest: str) -> dict[str, object]:
    """The reference a consumer declares for one origin-only batch (#912).

    It names the batch by where PB filed it -- owner action, attempt nonce,
    template, batch id -- and pins its content by the manifest digest, so a
    reference can never come to mean other bytes.
    """

    checked = validate_instance(instance)
    attempt = checked["owner_attempt"]
    assert isinstance(attempt, dict)
    return {"schema": ORIGIN_BATCH_REF_SCHEMA_V1,
            "owner_action_key": str(checked["owner_action_key"]),
            "owner_nonce": str(attempt["nonce"]),
            "template_id": str(checked["template_id"]),
            "batch_id": _name(batch_id, where="batch_id"),
            "manifest_digest": _hex64(manifest_digest,
                                      where="batch manifest_digest")}


def _origin_record(checked_template: Mapping[str, object],
                   checked_instance: Mapping[str, object], *, batch_id: str,
                   batch_ns: str, manifest_digest: str, tier: str,
                   class_bytes: Mapping[str, int],
                   sealed: list[dict[str, object]],
                   origin_identity: Mapping[str, object],
                   lifetime: str = ORIGIN_LIFETIME_RETAIN) -> dict[str, object]:
    """The immutable record of one origin-only batch, minus its commit time.

    A ``consumed`` lifetime is written into the record; ``retain`` is not, so
    a retained batch's record is the one #912 filed.
    """

    record = {
        "schema": BATCH_SCHEMA_V1,
        "batch_id": batch_id,
        "batch_namespace": batch_ns,
        "manifest_schema": BATCH_MANIFEST_SCHEMA_V1,
        "manifest_digest": manifest_digest,
        # The tier whose pool-side fill paced the export; nothing is staged.
        "tier": tier,
        "mover_key": None,
        "origin_only": True,
        "class_bytes": dict(class_bytes),
        "total_bytes": sum(int(v) for v in class_bytes.values()),
        "entry_count": len(sealed),
        "entries": sealed,
        "template_id": str(checked_template["template_id"]),
        "template_sha256": template_sha256(checked_template),
        "owner_action_key": str(checked_instance["owner_action_key"]),
        "owner_attempt": dict(checked_instance["owner_attempt"]),
        "origin_identity": dict(origin_identity),
        "object_set_id": manifest_object_set_id({
            f"{d['bytes']}:{d['path']}": {"bytes": int(d["bytes"]),
                                         "sha256": d["sha256"]}
            for d in sealed}),
    }
    if lifetime != ORIGIN_LIFETIME_RETAIN:
        record["lifetime"] = lifetime
    return record


def _entry_lifetime(entry: Mapping[str, object]) -> str:
    """A commitments entry's origin lifetime; an entry without one retains."""

    return str(entry.get("lifetime") or ORIGIN_LIFETIME_RETAIN)


def commit_origin_batch(queue, instance: Mapping[str, object],
                        template: Mapping[str, object],
                        descriptors: list[Mapping[str, object]], *,
                        batch_id: str,
                        landed: Mapping[str, Mapping[str, object]] | None = None,
                        lifetime: str = ORIGIN_LIFETIME_RETAIN
                        ) -> dict[str, object]:
    """Commit one write-only batch at its origin, with no stage copy (#912).

    The write-only sibling of `commit_batch`. A write-only template's batches
    are never read again by the action that wrote them, so nothing is staged,
    no mover is sealed and no tier token moves: the batch is its origin files,
    which the producer wrote -- through the paced spool export or directly --
    under a prewrite this commit consumes. A later action declares the batch
    in its data manifest (`origin_batch_manifest`) and stages it like any
    other input.

    Checks are `commit_batch`'s, minus the window and the funding: the live
    owner under the output-prefix lock, the prewrite present and matched (its
    tier, owner and attempt, the paths a subset of the planned ones, each
    class within its ceiling), every planned path the batch omits absent, the
    durable maxima, and one lstat per origin for size and identity, taken
    under that lock (#1064). The identity is recorded in the batch, and
    `origin_batch_manifest` rechecks it before any consumer is told the bytes
    are there.

    Two checks are this commit's own:

    - Every descriptor carries its sha256. No mover copies an origin-only
      batch at commit, so the digest is the only thing that binds the bytes a
      consumer's mover later copies to the bytes committed here; that mover
      verifies it on copy.
    - ``landed`` maps each origin path to the identity its writer recorded
      when the file landed; the spool passes its export receipt's
      (`ProducedSpool.commit_origin_group`). Each origin must still be that
      file. An export retried before its receipt can replace a landed copy,
      so an identity taken from before the receipt is not the committed one.
      An origin whose timestamps alone moved since the receipt (an NFS
      delegation recall, #1111) is read and hashed before the lock is taken;
      when its digest is the descriptor's, the hashed identity is the one
      committed, and the entry and the answer carry ``landed_repins``. The
      lstat under the lock must find the identity the read hashed; one whose
      timestamps moved again meanwhile (a retirement's link-back that the
      lock ordered, #1064) is read once more with the lock let go, and a
      second move refuses rather than commit an identity the file no longer
      has.

    A read-back template's batch commits here too (#1034), but only with
    ``landed``: an owner that reads a group back from its own local spool
    (PQ #1118) publishes no stage copy, so `commit_batch` never runs for it,
    and without this commit its prewrite stayed outstanding at its ceiling
    for the owner's whole life. The spool commits it once the export is
    acknowledged; a caller with no export receipt still refuses
    ``template-reads-back``. Such a batch is no handoff: `load_origin_batch`
    and `declare_origin_consumer` refuse it, and a ``consumed`` one is
    retired once its owner attempt has ended, success included.

    The commitments entry carries ``origin_only: true`` and no mover. Its
    paths stay owned until `reclaim_origin` proves them absent, because a
    consumer may have declared them; its durable class bytes stay charged
    until then too, exactly as for a staged batch.

    ``lifetime`` (#914) says who ends the batch. ``retain``, the default,
    leaves that to the producer and the operator: PB never deletes it, and
    nothing about it differs from #912. ``consumed`` hands it to
    `origin_retirement_tick`, which deletes the origin once every consumer
    that declared the batch has succeeded, or once its producer attempt is
    dead if no consumer declared it. The lifetime is part of the record and
    the entry, so a replay that names another lifetime refuses.

    Idempotent: a replay with the same manifest answers the duplicate, and a
    crash between filing the record and filing the entry resumes from the
    filed record rather than refusing it. Returns ``{"ok": True, ..., "ref"}``
    or a typed refusal.
    """

    from prismabuild import pool as pool_mod

    try:
        checked_template, checked_instance = _require_bound_contract(
            template, instance)
    except ProducedOutputError:
        return {"ok": False, "refusal": "template-mismatch"}
    _name(batch_id, where="batch_id")
    if lifetime not in ORIGIN_LIFETIMES:
        raise ProducedOutputError(
            f"origin batch lifetime must be one of {sorted(ORIGIN_LIFETIMES)}, "
            f"not {lifetime!r}")
    if not checked_template.get("write_only") and landed is None:
        # A read-back template's batch commits here only when its owner reads
        # it back from its own local spool (#1034): the spool's export
        # receipt is what passes ``landed``.  Otherwise its batches are staged
        # for their owner to read again: `commit_batch`, through a mover and a
        # funded window.
        return {"ok": False, "refusal": "template-reads-back"}
    if not isinstance(descriptors, list) or not descriptors:
        return {"ok": False, "refusal": "batch-has-no-entries"}
    sealed = [validate_descriptor(d, checked_template, checked_instance)
              for d in descriptors]
    if any(desc["sha256"] is None for desc in sealed):
        return {"ok": False, "refusal": "origin-batch-needs-sha256"}
    if landed is not None and (
            not isinstance(landed, Mapping)
            or set(landed) != {str(desc["path"]) for desc in sealed}):
        return {"ok": False, "refusal": "origin-is-not-the-landed-copy"}
    class_bytes = {"payload": 0, "checkpoint": 0, "temp": 0}
    for desc in sealed:
        class_bytes[str(desc["artifact_class"])] += int(desc["bytes"])
    manifest_digest = output_manifest_sha256(sealed)
    batch_ns = batch_namespace(checked_instance, batch_id, manifest_digest)
    prewrite_path = (_prewrites_dir(queue.root, checked_instance)
                     / f"{batch_id}.prewrite.json")
    ref = origin_batch_ref(checked_instance, batch_id=batch_id,
                           manifest_digest=manifest_digest)
    # Each landed origin whose timestamps alone moved, read and hashed with
    # no lock held (#1111), by path; the lock's lstat must find the identity
    # the read hashed. Two rounds: one more if it moved again in between.
    commitments_path = _commitments_path(queue.root, checked_instance)
    verified: dict[str, dict[str, object]] = {}
    for _round in (1, 2):
        if landed is not None:
            refusal = _read_landed_origins(sealed, landed, verified)
            if refusal is not None:
                return refusal
        with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
            # The identity is taken under the lock (#1064): a retirement's
            # delete, which holds it, can move an origin aside and link it
            # back (`_unlink_if_committed`), which moves its ctime, so an
            # identity taken before the lock could be stale before it was
            # filed, and an lstat taken while the file was aside would refuse.
            origin_identity, identity_refusal = _origin_identity_at_commit(sealed)
            if identity_refusal is not None:
                return identity_refusal
            assert origin_identity is not None
            landed_repins, moved, refusal = _landed_identity(
                sealed, landed, verified, origin_identity)
            if refusal is not None:
                return refusal
            if moved:
                # Read again with the lock let go.
                continue
            try:
                commitments = _read_commitments(commitments_path)
            except ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            batches = commitments["batches"]
            assert isinstance(batches, dict)
            if batch_id in batches:
                existing = batches[batch_id]
                if (isinstance(existing, Mapping)
                        and existing.get("origin_only") is True
                        and existing.get("manifest_digest") == manifest_digest):
                    if _entry_lifetime(existing) != lifetime:
                        return {"ok": False, "refusal": "batch-lifetime-mismatch"}
                    # The commit consumed the prewrite; a crash after the entry
                    # and before the unlink leaves it, and the replay finishes it.
                    prewrite_path.unlink(missing_ok=True)
                    return {"ok": True, "batch_id": batch_id, "duplicate": True,
                            "batch_namespace": batch_ns,
                            "manifest_digest": manifest_digest,
                            "origin_only": True, "ref": ref}
                return {"ok": False, "refusal": "batch-id-in-use"}
            gated = _require_live_owner(queue, checked_instance)
            if gated is not None:
                return gated
            try:
                prewrite = _read_prewrite(prewrite_path)
            except ProducedOutputError as exc:
                return {"ok": False, "refusal": f"prewrite-unreadable: {exc}"}
            if prewrite is None:
                return {"ok": False, "refusal": "prewrite-reservation-missing"}
            tier = str(prewrite.get("tier") or "")
            if (tier not in checked_template["permitted_tiers"]
                    or not _actual_within_ceiling(prewrite, sealed)
                    or not set(str(d["path"]) for d in sealed)
                    <= set(prewrite.get("paths", []))
                    or prewrite.get("owner_action_key")
                    != checked_instance["owner_action_key"]
                    or dict(prewrite.get("owner_attempt", {})) != dict(
                        checked_instance["owner_attempt"])):
                return {"ok": False, "refusal": "prewrite-mismatch"}
            if not _planned_omitted_absent(prewrite, sealed):
                return {"ok": False, "refusal": "planned-path-present-retain"}
            try:
                sums = _class_sums(batches)
            except ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            maxima = checked_instance_maxima(checked_template)
            for cls in sums:
                if sums[cls] + class_bytes[cls] > maxima[f"{cls}_max_bytes"]:
                    return {"ok": False, "refusal": f"commit-exceeds-{cls}-maxima"}
            body = _origin_record(
                checked_template, checked_instance, batch_id=batch_id,
                batch_ns=batch_ns, manifest_digest=manifest_digest, tier=tier,
                class_bytes=class_bytes, sealed=sealed,
                origin_identity=origin_identity, lifetime=lifetime)
            batch_dir = (Path(queue.root) / "residency" / OUTPUT_BATCHES_SUBDIR
                         / instance_namespace(checked_instance))
            batch_dir.mkdir(parents=True, exist_ok=True)
            batch_path = batch_dir / f"{batch_id}.json"
            record = dict(body, unix=time.time())
            try:
                pool_mod._publish_immutable(
                    batch_path,
                    json.dumps(record, sort_keys=True,
                               separators=(",", ":")).encode() + b"\n",
                    where="produced-output batch")
            except pool_mod.PoolContractError as exc:
                # A crash after this record and before the entry below: the same
                # batch, over the same identities, resumes from what is filed.
                # Anything else is a different batch under this id.
                try:
                    filed = json.loads(batch_path.read_text())
                except (OSError, ValueError):
                    filed = None
                if (not isinstance(filed, Mapping)
                        or {k: v for k, v in filed.items() if k != "unix"}
                        != json.loads(json.dumps(body))):
                    return {"ok": False, "refusal": f"batch-conflict: {exc}"}
            # `retired` stays false for the life of the entry: nothing was staged
            # to retire, `_active_materialization` answers "no copy" from
            # `origin_only`, and `_committed_restage_authority` reads this flag,
            # so false is also what keeps the batch unfundable as a restage.
            entry = {
                "manifest_digest": manifest_digest,
                "batch_namespace": batch_ns,
                "tier": tier,
                "mover_key": None,
                "origin_only": True,
                "class_bytes": class_bytes,
                "paths": sorted(str(d["path"]) for d in sealed),
                "retired": False,
                "origin_reclaimed": False,
            }
            if lifetime != ORIGIN_LIFETIME_RETAIN:
                entry["lifetime"] = lifetime
            if landed_repins:
                # What the commit read to accept the landed copy; the record's
                # identity is already the hashed one, so nothing chains from it.
                entry["landed_repins"] = landed_repins
            batches[batch_id] = entry
            _write_commitments(commitments_path, {"batches": batches})
            prewrite_path.unlink(missing_ok=True)
        break
    else:
        # Its timestamps moved after each read. The identity the lock found
        # was never read, and committing it unread would bless bytes nobody
        # checked (#1111).
        return {"ok": False, "refusal": "origin-is-not-the-landed-copy",
                "detail": f"{', '.join(moved)}: its timestamps moved again "
                          f"after its content was verified"}
    result = {"ok": True, "batch_id": batch_id, "batch_namespace": batch_ns,
              "manifest_digest": manifest_digest, "class_bytes": class_bytes,
              "tier": tier, "entries": sealed, "origin_only": True, "ref": ref}
    if lifetime != ORIGIN_LIFETIME_RETAIN:
        result["lifetime"] = lifetime
    if landed_repins:
        result["landed_repins"] = landed_repins
    return result


def _producer_launch_context(queue, producer: str) -> dict[str, object]:
    """The producer's FILED launch context, for a mover row of its own.

    The mover row must be launched exactly like the producer action is:
    `worker_script` is the PB worker launcher (whose run-local verb takes the
    request/cas/checkout arguments Pool.execute builds), NEVER the stage tool
    itself (stage_move is the action PAYLOAD that launcher executes), and the
    row's checkout addressing is reused so the launcher materializes the same
    tree the producer ran from. An agent's validation priority stays with the
    row. Returns `{"ok": True, "worker_script", "addressing", "priority"}` or a
    typed refusal; one spelling, shared by first publication and restage, so
    the two can never drift.
    """

    from prismabuild import pool as pool_mod

    try:
        producer_row = pool_mod._read_json(
            queue.item_path(pool_mod.CLAIMED, producer))
    except (OSError, pool_mod.PoolContractError) as exc:
        return {"ok": False, "step": "launch-context",
                "refusal": f"producer-claim-unreadable: {exc}"}
    if not isinstance(producer_row, Mapping):
        return {"ok": False, "step": "launch-context",
                "refusal": "producer-not-claimed: the mover must be "
                           "published from inside the admitted producer"}
    worker_script = str(producer_row.get("worker_script") or "")
    if not worker_script.startswith("/"):
        return {"ok": False, "step": "launch-context",
                "refusal": "producer-launch-context-required: the producer "
                           "row names no absolute worker script"}
    addressing: dict[str, object] = {}
    if isinstance(producer_row.get("checkout_snapshot"), Mapping):
        addressing["checkout_snapshot"] = producer_row["checkout_snapshot"]
    elif str(producer_row.get("checkout_root") or "").startswith("/"):
        addressing["checkout_root"] = str(producer_row["checkout_root"])
    else:
        return {"ok": False, "step": "launch-context",
                "refusal": "producer-launch-context-required: the producer "
                           "row names no checkout addressing"}
    # The row's own CAS root, then the queue-sibling default: the same
    # precedence `stage_release` resolves a claim's CAS root with.
    row_cas = producer_row.get("cas_root")
    cas_root = (str(row_cas) if isinstance(row_cas, str) and row_cas
                else str(Path(queue.root).parent / "cas"))
    return {"ok": True, "worker_script": worker_script,
            "addressing": addressing,
            "priority": int(producer_row.get("priority") or 0),
            "cas_root": cas_root}


def _read_producer_request(cas_root, producer: str):
    """The producer's own sealed request, through the existing CAS interface.

    Returns `(cas, request)`, or a typed `{"ok": False, "step", ...}` refusal.
    The runtime parent context every node of this lane inherits: the output
    mover and the tier-host egress both seal off it.
    """

    from prismabuild import core as core_mod

    if len(producer) != 64:
        return {"ok": False, "step": "parent-request",
                "refusal": "producer-request-required: pass "
                           "producer_action_key or run under a launcher "
                           "that sets PRISMABUILD_ACTION_KEY"}
    request_path = None
    try:
        cas = core_mod.PrismaBuildCAS(cas_root)
        request_path = (Path(cas.root) / "requests" / producer[:2]
                        / f"{producer}.json")
        with open(request_path, "rb") as handle:
            raw = handle.read(8 * 1024 * 1024 + 1)
        if len(raw) > 8 * 1024 * 1024:
            raise ProducedOutputError("producer request oversize")
        request = core_mod.validate_action(core_mod._decode_strict_json(
            raw, where="producer action request"))
        if str(request.get("action_key")) != producer:
            raise ProducedOutputError("producer request key mismatch")
    except FileNotFoundError:
        return {"ok": False, "step": "parent-request",
                "refusal": f"producer-request-missing: {request_path}"}
    except Exception as exc:
        return {"ok": False, "step": "parent-request",
                "refusal": f"producer-request-unreadable: {exc}"}
    return cas, request


def _producer_movement_template(queue, request: Mapping[str, object],
                                producer: str, *,
                                extra_inputs: Sequence[Mapping[str, object]] = ()
                                ) -> dict[str, object]:
    """The movement template a node of this lane seals off its producer.

    The producer's task, code closure, environment and execution scope, its
    inputs minus its own data manifest, and its OWN sealed checkout addressing
    carried into the child. `extra_inputs` follow the inherited ones: the
    output mover appends the batch's data manifest, an egress appends nothing.
    Returns `{"ok": True, "template"}` or a typed refusal.
    """

    from prismabuild import core as core_mod
    from prismabuild import pool as pool_mod

    parent_inputs = [dict(entry) for entry in request.get("inputs") or ()
                     if isinstance(entry, Mapping)]
    child_inputs = [entry for entry in parent_inputs
                    if str(entry.get("id"))
                    != core_mod.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID]
    # The producer's OWN sealed checkout addressing, carried into the child.
    # The row already materializes the producer's snapshot; a sealed request
    # that does not say so is proved against the producer's pre-snapshot
    # closure stamp, which the materialized tree can never satisfy. Absent on
    # a legitimate non-snapshot producer: the legacy stamp proof and the
    # historical ownership digest are unchanged there.
    request_params = request.get("params")
    if not isinstance(request_params, Mapping):
        request_params = {}
    params: dict[str, object] = {
        "cwd": str(request_params.get("cwd") or ".")}
    raw_snapshot = request_params.get("checkout_snapshot")
    snapshot_sha256 = ""
    if raw_snapshot is not None:
        try:
            snapshot = core_mod.validate_pbrun_checkout_snapshot(raw_snapshot)
        except core_mod.ActionContractError as exc:
            return {"ok": False, "step": "parent-request",
                    "refusal": f"producer-checkout-snapshot-invalid: {exc}"}
        snapshot_input = snapshot["input"]
        assert isinstance(snapshot_input, Mapping)
        if snapshot_input not in child_inputs:
            return {"ok": False, "step": "parent-request",
                    "refusal": "producer-checkout-snapshot-input-missing: the "
                               "producer's sealed snapshot input is not among "
                               "the inputs the mover inherits"}
        params["checkout_snapshot"] = snapshot
        snapshot_sha256 = str(snapshot_input["sha256"])
    if not snapshot_sha256:
        # The ownership namespace keeps the historical digest over the first
        # inherited input, else the producer's own key.
        snapshot_sha256 = next((str(entry.get("sha256"))
                                for entry in child_inputs
                                if entry.get("sha256")), producer)
    return {"ok": True, "template": {
        "task": dict(request["task"]),
        "inputs": child_inputs + [dict(entry) for entry in extra_inputs],
        "code_closure": request["code_closure"],
        "environment": request["environment"],
        "execution_scope": request["execution_scope"],
        "params": params,
        "marker_root": Path(queue.root) / pool_mod.CONTAINER_OWNERS,
        "checkout_identity": {"checkout_snapshot": snapshot_sha256},
    }}


def _announced_tier_record(queue, tier: str) -> Mapping[str, object] | None:
    """The tier's announced record (`tier_loop.py` writes it), or None."""

    for candidate in queue.tiers():
        if isinstance(candidate, Mapping) and str(
                candidate.get("tier_id")) == tier:
            return candidate
    return None


def _sealed_restage_fill_setting(request: Mapping[str, object]) -> bool | None:
    """The producer's sealed `RESTAGE_FILL_ENV`: False, True, or None if bad.

    Absent, "" and "0" are off; "1" is on. Any other value is a sealing
    mistake that no restage should guess at, so it answers None and the
    caller refuses before anything is filed.
    """

    environment = request.get("environment")
    variables = (environment.get("variables")
                 if isinstance(environment, Mapping) else None)
    value = (variables.get(RESTAGE_FILL_ENV, "")
             if isinstance(variables, Mapping) else "")
    if value in ("", "0"):
        return False
    if value == "1":
        return True
    return None


def _seal_output_mover(queue, checked_instance: Mapping[str, object],
                       checked_template: Mapping[str, object],
                       descriptors: list[Mapping[str, object]], *,
                       batch_id: str, tier: str, cas_root,
                       producer_action_key: str | None,
                       command_extra: Sequence[str] = (),
                       retry_policy: Mapping[str, object] | None = None,
                       generation: int = 0,
                       restage_fill: bool | None = None) -> dict[str, object]:
    """Seal and file ONE output mover for this batch, and answer its facts.

    The single sealing path for the produced-output lane: first publication
    (`generation == 0`) and every later re-materialization
    (`generation >= 1`, `ensure_batch_materialized`) construct the mover the
    SAME way -- movement template recovered from the producer's own sealed
    request through the existing CAS request interface, batch data manifest
    sealed as that request's input, interpreter/tools/stage-root/placement off
    the ANNOUNCED TIER RECORD, the ordinary paced `stage_move` command behind
    `movement_actions.seal_movement_action`. Nothing here is a second sealing
    scheme and nothing is copied from `publish`.

    A re-materialization differs from first publication in exactly two sealed
    fields, both derived from the PB-sealed materialization sequence and
    neither chosen by a caller: the log name carries the generation, and the
    params carry the `produced_output_materialization` record. That is what
    makes the successor's content-addressed action key -- which IS its funding
    key -- unique per generation, deterministic on replay, and impossible to
    supply as a nonce. Every generation seals the same physical batch namespace
    under the registered tier root. The child's
    `params.checkout_snapshot` is the producer's validated sealed record when
    the producer has one, and absent when it does not.

    The producer's sealed checkout snapshot rides the child exactly as
    ``movement_actions.seal_movement_action`` already carries it for pbrun's
    own movement nodes: the row the mover is published under materializes that
    snapshot (``_producer_launch_context``), so the sealed request has to say
    which tree that is. A child that inherited the materialized snapshot
    without inheriting the record was proved against the producer's
    pre-snapshot closure stamp instead, which no materialized snapshot can
    satisfy; the worker refused the row in ~0.8 s before any byte moved
    (2026-09-21 Stage A live cycle). A malformed record, or one whose declared
    input the child does not inherit, is refused here rather than sealed for a
    worker to reject.

    A re-materialization may also reserve pool fill (#747), and only when the
    producer opted in: `restage_fill` when the caller passes it, else the
    producer's sealed `RESTAGE_FILL_ENV`. A restage copies a retired batch
    back off the pool, cold, the way a consumer's input mover does, so it is
    priced the way pbrun prices one: `storage_tiers.current_fill_offer` over
    the movement receipts and the tier's current offer. The demand gains the
    `fill_mb_s_pool_side@<tier>` term and the command the matching
    `--fill-mb-s-pool-side`, and the price is returned as `fill` so the
    materialization row can carry it for a resume. A first publication never
    reserves fill: it reads what the producer has just written. A tier that
    offers no fill and has no usable receipt prices nothing, and the mover is
    sealed unreserved exactly as before.

    Returns the sealed facts (`mover_key`, `action`, `host`, `kind`, `gib`,
    `fill`, `total`, `manifest_digest`, `batch_namespace`, `retry_policy`,
    `cas`) with the request already filed in CAS, or a typed
    `{"ok": False, "step", ...}`.
    """

    from prismabuild import core as core_mod
    from prismabuild import movement_actions
    from prismabuild import pool as pool_mod
    from prismabuild import storage_tiers as tiers_mod

    try:
        ref = pool_mod.PoolQueue.build_produced_output_batch_ref(
            instance=checked_instance, template=checked_template,
            batch_id=batch_id, descriptors=list(descriptors), tier_id=tier)
    except pool_mod.PoolContractError as exc:
        return {"ok": False, "step": "build-ref", "refusal": str(exc)}
    manifest_digest = str(ref["manifest_digest"])
    total = int(ref["range_end_bytes"])
    batch_ns = str(ref["batch_namespace"])
    # The producer's own sealed request, through the existing request
    # interface: this is the runtime parent context the mover inherits.
    producer = str(producer_action_key or
                   os.environ.get(core_mod.ACTION_KEY_ENV) or "")
    parent = _read_producer_request(cas_root, producer)
    if isinstance(parent, Mapping):
        return dict(parent)
    cas, request = parent
    reserve_fill = False
    if int(generation) > 0:
        setting = _sealed_restage_fill_setting(request)
        if setting is None:
            return {"ok": False, "step": "seal",
                    "refusal": (f"{RESTAGE_FILL_ENV} in the producer's sealed "
                                f"environment must be 0 or 1")}
        reserve_fill = setting if restage_fill is None else bool(restage_fill)
    # The batch's stage data manifest, sealed as the request's data-manifest
    # input exactly as a consumer submission's is: the mover finds it in its
    # own sealed request, never on a caller's filesystem. The parent's own
    # data-manifest input is dropped from the child's inputs so the child
    # carries exactly one -- the batch's.
    batch_view = {"entries": [dict(d) for d in descriptors],
                  "batch_id": batch_id, "manifest_digest": manifest_digest}
    mount_prefix = str(os.path.commonpath(
        [os.path.dirname(str(d["path"])) for d in descriptors]))
    try:
        manifest_body = build_stage_manifest(batch_view, mount_prefix)
        handle, manifest_tmp = tempfile.mkstemp(
            prefix="produced-output-manifest-", suffix=".json")
        with os.fdopen(handle, "w") as stream:
            json.dump(manifest_body, stream, sort_keys=True)
        try:
            core_mod.load_data_manifest(manifest_tmp)
            manifest_input, _ = cas.ingest_input(
                manifest_tmp,
                input_id=core_mod.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID)
        finally:
            os.unlink(manifest_tmp)
    except Exception as exc:
        return {"ok": False, "step": "manifest", "refusal": str(exc)}
    templated = _producer_movement_template(
        queue, request, producer, extra_inputs=[manifest_input])
    if not templated.get("ok"):
        return templated
    mover_template = templated["template"]
    # Movement-node resolution off the TIER RECORD (the ordinary path):
    # interpreter/tools/mountpoint/host are facts about the box that runs
    # the movers, announced beside the tier by tier_loop.py.
    record = _announced_tier_record(queue, tier)
    if record is None:
        return {"ok": False, "step": "resolve",
                "refusal": f"tier-not-announced: {tier}"}
    try:
        mover_python, mover_tool, _egress_tool = (
            movement_actions.movement_tools(record))
    except SystemExit as exc:
        return {"ok": False, "step": "resolve", "refusal": str(exc)}
    stage_root = str(record.get("mountpoint") or "")
    if not stage_root.startswith("/"):
        return {"ok": False, "step": "resolve",
                "refusal": (f"stage tier {tier} announces no mountpoint "
                            f"to write into")}
    host = str(record.get("host") or "")
    kind = tiers_mod.capacity_kind_of(tier)
    gib = tiers_mod.stage_tokens_for_bytes(total)
    demand: dict[str, int] = {"cpu": 1, "mem_gb": 1, f"{kind}@{tier}": gib}
    fill: int | None = None
    fill_basis: str | None = None
    if reserve_fill:
        # pbrun's mover rule, unchanged: one copy's measured rate -- this
        # batch's slowest landing in its latest window, else the median
        # single-reader share (#909) -- capped by what the tier offers on this
        # cycle (#708). Receipts from another pool behind the same tier id
        # price nothing (#611).
        identity = record.get("pool_identity")
        measured = tiers_mod.mover_fill_demand_from_receipts(
            queue.move_records(), tier_id=tier,
            pool_identity=identity if isinstance(identity, Mapping) else None,
            manifest_sha256=manifest_digest)
        priced, _offer, fill_basis = tiers_mod.current_fill_offer(
            record, measured)
        if priced is not None and int(priced) > 0:
            fill = int(priced)
            demand[f"{tiers_mod.FILL_KIND}"
                   f"{tiers_mod.TIER_DEMAND_SEPARATOR}{tier}"] = fill
    command = [mover_python, mover_tool,
               "--pool-root", str(queue.root),
               "--cas-root", str(cas.root),
               "--consumer-action-key", batch_ns,
               "--produced-output-namespace", batch_ns,
               "--tier-id", tier,
               "--stage-root", stage_root,
               "--manifest-sha256", manifest_digest,
               "--range-start-bytes", "0",
               "--range-end-bytes", str(total),
               # Output-batch fragments file under the produced-output
               # fragment root, not the tier's default residency root.
               "--residency-root", str(output_fragment_root(
                   Path(queue.root) / pool_mod.RESIDENCY)),
               "--readers", "2"]
    if fill is not None:
        # Receipt only: the mover records what its claim reserved, so a
        # later tier cycle can ask whether the pool delivered it.
        command += ["--fill-mb-s-pool-side", str(fill)]
    command += [str(flag) for flag in command_extra]
    mover_retry_policy = (dict(retry_policy) if retry_policy is not None
                          else {"max_attempts": 3, "retry_safe": True})
    extra_params: dict[str, object] = {
        "produced_output_batch": dict(ref),
        "data_manifest": {"input": manifest_input},
    }
    log_name = f"produced-output-mover-{batch_id}.log"
    if int(generation) > 0:
        log_name = (f"produced-output-mover-{batch_id}"
                    f"-m{int(generation)}.log")
        extra_params["produced_output_materialization"] = {
            "schema": MATERIALIZATION_SCHEMA_V1,
            "batch_id": batch_id,
            "manifest_digest": manifest_digest,
            "batch_namespace": batch_ns,
            "tier_id": tier,
            "generation": int(generation),
        }
    try:
        action = movement_actions.seal_movement_action(
            mover_template,
            command=command,
            demand=demand,
            tags=[host] if host else [],
            log_name=log_name,
            retry_policy=mover_retry_policy,
            extra_params=extra_params)
        cas.publish_action_request(action)
    except SystemExit as exc:
        return {"ok": False, "step": "seal", "refusal": str(exc)}
    except Exception as exc:
        return {"ok": False, "step": "seal", "refusal": str(exc)}
    return {"ok": True, "action": action,
            "mover_key": str(action["action_key"]),
            "cas": cas, "cas_root": str(cas.root), "host": host,
            "kind": kind, "gib": gib, "fill": fill, "fill_basis": fill_basis,
            "total": total,
            "manifest_digest": manifest_digest, "batch_namespace": batch_ns,
            "retry_policy": mover_retry_policy, "ref": dict(ref)}


def publish_prepaid_batch(queue, instance: Mapping[str, object],
                          template: Mapping[str, object],
                          descriptors: list[Mapping[str, object]], *,
                          batch_id: str, tier: str, cas_root,
                          producer_action_key: str | None = None,
                          command_extra: Sequence[str] = (),
                          retry_policy: Mapping[str, object] | None = None,
                          ) -> dict[str, object]:
    """The operational prepaid writer path for one finished batch (R7).

    Callable INSIDE the admitted producer action. The movement action is
    sealed by `_seal_output_mover` (the shared first-publisher construction:
    the movement template is RECOVERED from the producer's own sealed request
    through the existing CAS request interface, never from submitter-local
    state), and the launch context comes from the producer's FILED claimed row
    through `_producer_launch_context`.

    Sequence, all existing mechanisms: seal + file the mover request ->
    stage the funding intent (reserved, no tokens moved) -> publish the mover
    READY row -> fund by exact transfer of the producer's existing window
    (``fund_output_batch``) -> ``commit_batch`` (files the immutable batch
    against the pool record; no second acquisition from free). The fleet's
    ordinary claim then admits the mover through the prepaid cover, and the
    worker executes the sealed argv on the storage owner.

    ``command_extra`` exists ONLY so a fixture can append explicit flags such
    as ``--unpaced`` (no ZFS pacer in a sandbox) -- production passes nothing.
    ``retry_policy`` defaults to the ordinary mover policy (bounded attempts,
    retry-safe copy).

    The mover key is the content-addressed action key of the sealed request:
    retrying with identical inputs re-derives the same key and every step is
    idempotent (stage/publish/fund/commit duplicates are typed successes; a
    fully committed batch short-circuits to the duplicate), so a restart re-
    calls this method. Refusals return the failing step's typed result under
    ``step``/``refusal``. Re-STAGING an already retired batch is a different
    transition and is NOT this function: see `ensure_batch_materialized`.
    """

    try:
        checked_template = validate_template(template)
        checked_instance = validate_instance(instance)
    except ProducedOutputError as exc:
        return {"ok": False, "step": "validate", "refusal": str(exc)}
    _name(batch_id, where="batch_id")
    if checked_template.get("write_only"):
        # A write-only template's batches commit at origin and are never
        # staged by their owner (#912); this staged path is not theirs.
        return {"ok": False, "step": "validate",
                "refusal": "template-is-write-only"}
    if tier not in checked_template["permitted_tiers"]:
        return {"ok": False, "step": "validate", "refusal": "tier-not-permitted"}
    try:
        manifest_digest = output_manifest_sha256(
            [validate_descriptor(dict(d), checked_template, checked_instance)
             for d in descriptors])
    except ProducedOutputError as exc:
        return {"ok": False, "step": "build-ref", "refusal": str(exc)}
    # Retry after FULL success: the prewrite and the pool-side staging are
    # consumed by design, so creation steps would refuse exactly what they
    # finished. A filed commitments entry with the same manifest means the
    # batch is complete -- replay the idempotent commit and answer the
    # duplicate.
    try:
        commitments = _read_commitments(
            _commitments_path(queue.root, checked_instance))
    except ProducedOutputError as exc:
        return {"ok": False, "step": "validate",
                "refusal": f"unknown-retain: {exc}"}
    existing = commitments["batches"].get(batch_id)
    if isinstance(existing, Mapping):
        if existing.get("origin_only") is True:
            # A read-back batch its owner committed at its origin (#1034) has
            # no mover to replay; nothing stages it.
            return {"ok": False, "step": "validate",
                    "refusal": "batch-committed-at-origin"}
        if str(existing.get("manifest_digest")) != manifest_digest:
            return {"ok": False, "step": "validate",
                    "refusal": "batch-id-in-use"}
        mover = str(existing.get("mover_key"))
        committed = commit_batch(queue, checked_instance, checked_template,
                                 descriptors, batch_id=batch_id, tier=tier,
                                 mover_key=mover)
        if not committed.get("ok"):
            committed["step"] = "commit"
            return committed
        committed["mover_key"] = mover
        record = queue.read_output_funding(mover, tier)
        committed["generation"] = (str(record.get("generation"))
                                   if record is not None else "")
        committed["tokens"] = ([str(name) for name in record.get("tokens") or []]
                               if record is not None else [])
        committed["funding"] = "prepaid"
        committed["duplicate"] = True
        return committed
    sealed_mover = _seal_output_mover(
        queue, checked_instance, checked_template, list(descriptors),
        batch_id=batch_id, tier=tier, cas_root=cas_root,
        producer_action_key=producer_action_key,
        command_extra=command_extra, retry_policy=retry_policy,
        generation=0)
    if not sealed_mover.get("ok"):
        return sealed_mover
    launch = _producer_launch_context(
        queue, str(producer_action_key or
                   os.environ.get(_ACTION_KEY_ENV_NAME()) or ""))
    if not launch.get("ok"):
        return launch
    mover = str(sealed_mover["mover_key"])
    owner = str(checked_instance["owner_action_key"])
    kind = str(sealed_mover["kind"])
    gib = int(sealed_mover["gib"])
    total = int(sealed_mover["total"])
    host = str(sealed_mover["host"])
    mover_retry_policy = dict(sealed_mover["retry_policy"])
    staged = queue.stage_output_intent(
        tier_id=tier, owner_key=owner, mover_key=mover,
        instance=checked_instance, template=checked_template,
        batch_id=batch_id, descriptors=descriptors)
    if not staged.get("ok"):
        staged["step"] = "stage"
        return staged
    published = _publish_output_mover_row(
        queue, mover_key=mover, cas_root=str(sealed_mover["cas_root"]),
        launch=launch, host=host, tier=tier, kind=kind, gib=gib,
        manifest_digest=manifest_digest, total=total,
        retry_policy=mover_retry_policy)
    if not published.get("ok"):
        return published
    funded = queue.fund_output_batch(
        tier_id=tier, owner_key=owner, mover_key=mover,
        instance=checked_instance, template=checked_template,
        batch_id=batch_id, descriptors=descriptors)
    if not funded.get("ok"):
        funded["step"] = "fund"
        return funded
    committed = commit_batch(queue, checked_instance, checked_template,
                             descriptors, batch_id=batch_id, tier=tier,
                             mover_key=mover)
    if not committed.get("ok"):
        committed["step"] = "commit"
        return committed
    committed["mover_key"] = mover
    committed["generation"] = str(funded.get("generation"))
    committed["tokens"] = list(funded.get("tokens") or [])
    committed["funding"] = "prepaid"
    return committed


def _ACTION_KEY_ENV_NAME() -> str:
    from prismabuild import core as core_mod

    return core_mod.ACTION_KEY_ENV


def _publish_output_mover_row(queue, *, mover_key: str, cas_root: str,
                              launch: Mapping[str, object], host: str,
                              tier: str, kind: str, gib: int,
                              manifest_digest: str, total: int,
                              retry_policy: Mapping[str, object],
                              fill: int | None = None
                              ) -> dict[str, object]:
    """Publish one output mover's READY row through the EXISTING channel.

    Ordinary `queue.publish` semantics carried from the parent: the producer's
    worker script and checkout addressing, its validation priority, the
    effective sealed mover retry policy, the tier's placement tag, qualified
    tier demand, and the mover-variant residency block. One spelling for first
    publication and restage. A row already published for this content-
    addressed key is a typed duplicate, not a conflict: the sealed key IS the
    identity, so republishing the same key is the resume path, never a second
    unit of work. `fill` is the pool fill the sealed request reserved, if
    any: the row's resources must equal its sealed demand, or the launch
    refuses it.
    """

    from prismabuild import pool as pool_mod
    from prismabuild import storage_tiers as tiers_mod

    resources: dict[str, int] = {"cpu": 1, "mem_gb": 1, f"{kind}@{tier}": gib}
    if fill is not None:
        resources[f"{tiers_mod.FILL_KIND}"
                  f"{tiers_mod.TIER_DEMAND_SEPARATOR}{tier}"] = int(fill)
    state = _mover_live_state(queue, mover_key)
    if state == "unknown":
        return {"ok": False, "step": "publish",
                "refusal": "unknown-retain: mover-row-unreadable"}
    if state != "absent":
        return {"ok": True, "published": False, "state": state}
    try:
        queue.publish(
            action_key=mover_key, cas_root=str(cas_root),
            worker_script=str(launch["worker_script"]),
            tags=[host] if host else (),
            priority=int(launch["priority"]),
            max_attempts=int(retry_policy["max_attempts"]),
            retry_safe=bool(retry_policy.get("retry_safe", True)),
            **dict(launch["addressing"]),
            resources=resources,
            residency={"schema": pool_mod.RESIDENCY_SCHEMA_V1,
                       "tier_id": tier,
                       "manifest_sha256": manifest_digest,
                       "manifest_bytes": total,
                       "range_start_bytes": 0, "range_end_bytes": total},
            # The same question the state read above asked, asked again under
            # the queue's own transition lock.  The read can be stale on NFS
            # -- that is how the 2026-09-21 cycle republished live rows -- and
            # only the lock makes look-then-publish one decision (#810).
            refuse_if_live=True)
    except pool_mod.ActionAlreadyLiveError as exc:
        # The answer the state read would have given: the row is there.
        return {"ok": True, "published": False, "state": exc.state}
    except pool_mod.PoolContractError as exc:
        return {"ok": False, "step": "publish", "refusal": str(exc)}
    return {"ok": True, "published": True, "state": "ready"}


#: A committed batch's lifecycle as :func:`batch_record` reports it (#955).
BATCH_STATE_COMMITTED = "committed"
BATCH_STATE_RETIRING = "retiring"
BATCH_STATE_RECLAIMED = "reclaimed"


def _batch_state(entry: Mapping[str, object]) -> str:
    if entry.get("origin_reclaimed"):
        return BATCH_STATE_RECLAIMED
    if entry.get("retiring"):
        return BATCH_STATE_RETIRING
    return BATCH_STATE_COMMITTED


def _public_batch_record(queue_root: str | Path,
                         checked_instance: Mapping[str, object],
                         checked_template: Mapping[str, object],
                         entry: object, batch_id: str) -> dict[str, object]:
    if not isinstance(entry, Mapping):
        raise ProducedOutputError(
            f"unknown-retain: commitments entry for {batch_id!r} is not a record")
    filed, sealed = _load_batch_record(
        queue_root, checked_instance, checked_template, entry, batch_id)
    return {
        "batch_id": batch_id,
        "state": _batch_state(entry),
        "lifetime": _entry_lifetime(entry),
        "entries": [{"path": str(desc["path"]), "bytes": int(desc["bytes"]),
                     "sha256": desc["sha256"],
                     "artifact_class": str(desc["artifact_class"])}
                    for desc in sealed],
        "total_bytes": int(filed["total_bytes"]),
        "manifest_digest": str(filed["manifest_digest"]),
        "commitment": json.loads(json.dumps(entry)),
        "record": filed,
    }


def batch_records(queue, instance: Mapping[str, object],
                  template: Mapping[str, object]) -> list[dict[str, object]]:
    """Every batch this instance committed, as :func:`batch_record` reads it.

    In ``batch_id`` order, reclaimed and retiring batches included: the
    caller filters on ``state``.  An instance that committed nothing answers
    ``[]``; an unreadable commitments document, or any batch whose record
    does not read and validate, raises `ProducedOutputError` -- the census
    rule: unknown is never read as empty, so no batch is silently dropped
    from the list.
    """

    checked_template, checked_instance = _require_bound_contract(
        template, instance)
    commitments = _read_commitments(
        _commitments_path(queue.root, checked_instance))
    batches = commitments["batches"]
    assert isinstance(batches, Mapping)
    return [_public_batch_record(queue.root, checked_instance,
                                 checked_template, batches[batch_id],
                                 str(batch_id))
            for batch_id in sorted(batches)]


def batch_record(queue, instance: Mapping[str, object],
                 template: Mapping[str, object], *, batch_id: str
                 ) -> dict[str, object]:
    """Read-only: one committed batch's record, validated (#955).

    The public reader for a produced-output batch, so a caller outside PB
    never imports `_load_batch_record` or `_read_commitments`.  Returns:

    - ``entries``: every committed descriptor, ``path``, ``bytes``,
      ``sha256`` and ``artifact_class``, in the record's order;
    - ``lifetime``: ``retain`` or ``consumed`` (``ORIGIN_LIFETIME_*``);
    - ``commitment``: the batch's entry in the instance's commitments
      document, as filed (class bytes, manifest digest, materializations,
      a ``retiring`` decision, ``origin_reclaimed``);
    - ``state``: ``committed``, ``retiring`` (the retirement tick decided to
      delete its origin files) or ``reclaimed`` (PB stopped charging it);
    - ``record``: the filed batch record itself (``origin_identity``
      included), plus ``total_bytes`` and ``manifest_digest`` off it.

    Mutates nothing and takes no lock.  The record is validated exactly as
    retirement and reclaim validate it: bound to this instance, attempt and
    template, every descriptor re-checked, the manifest digest recomputed.
    Raises `ProducedOutputError` for a template the instance is not bound
    to (``template-mismatch``), a batch the instance never committed
    (``unknown-batch``), and a commitments document or batch record that is
    missing, unreadable or does not validate (``unknown-retain: ...``):
    unknown is never read as empty.
    """

    checked_template, checked_instance = _require_bound_contract(
        template, instance)
    _name(batch_id, where="batch_id")
    commitments = _read_commitments(
        _commitments_path(queue.root, checked_instance))
    batches = commitments["batches"]
    assert isinstance(batches, Mapping)
    if batch_id not in batches:
        raise ProducedOutputError(f"unknown-batch: {batch_id}")
    return _public_batch_record(queue.root, checked_instance,
                                checked_template, batches[batch_id], batch_id)


def materialization_state(queue, instance: Mapping[str, object],
                          template: Mapping[str, object], *, batch_id: str
                          ) -> dict[str, object]:
    """Read-only: which materialization of this batch is current, and where.

    The question a bounded-window reader asks before deciding whether it needs
    `ensure_batch_materialized`: is a stage copy of this batch live now, under
    which mover, at which generation, and has its mover's receipt said the
    bytes landed whole. Mutates nothing, takes no lock, and fails closed --
    an unreadable record or a malformed materialization list answers
    `unknown-retain`, never "nothing is staged".

    One receipt read answers both mover questions, so `mover_receipt_complete`
    and `mover_refusal` can never describe two different observations of a
    record the mover rewrites. A typed `origin_unreachable` -- the mover's
    proof that a produced-output prefix is not on the tier host -- is
    returned through the existing failure contract as `ok: False` with
    `refusal` beside the retained mover/materialization identity, which is
    the gate every caller already raises on; the owner therefore stops on
    the first failed attempt instead of reading the same symptom a mover
    defect produces. Only the ACTIVE materialization's own receipt can
    answer this: a retired predecessor's refusal is history, not this
    state. An absent or unreadable receipt leaves both answers None --
    silence is never a named failure and never readiness.
    """

    try:
        checked_template, checked_instance = _require_bound_contract(
            template, instance)
    except ProducedOutputError:
        return {"ok": False, "refusal": "template-mismatch"}
    _name(batch_id, where="batch_id")
    try:
        commitments = _read_commitments(
            _commitments_path(queue.root, checked_instance))
    except ProducedOutputError as exc:
        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    batches = commitments["batches"]
    assert isinstance(batches, dict)
    entry = batches.get(batch_id)
    if not isinstance(entry, Mapping):
        return {"ok": False, "refusal": "unknown-batch"}
    try:
        filed, _sealed = _load_batch_record(
            queue.root, checked_instance, checked_template, entry, batch_id)
        active = _active_materialization(entry)
    except ProducedOutputError as exc:
        return {"ok": False, "refusal": str(exc)}
    mover = str(active.get("mover_key") or "")
    receipt = _mover_receipt(queue, mover)
    complete = _receipt_complete(receipt)
    refusal = _receipt_refusal(receipt)
    state = {
        "ok": True,
        "batch_id": batch_id,
        "tier": str(filed.get("tier") or ""),
        "manifest_digest": str(filed.get("manifest_digest") or ""),
        "batch_namespace": str(filed.get("batch_namespace") or ""),
        "total_bytes": int(filed.get("total_bytes") or 0),
        "mover_key": mover,
        "generation": int(active.get("generation") or 0),
        "source": str(active.get("source") or ""),
        "funding_state": str(active.get("state") or ""),
        "stage_retired": bool(active.get("retired")),
        "origin_reclaimed": bool(entry.get("origin_reclaimed")),
        "mover_receipt_complete": complete,
        "mover_refusal": refusal,
        "mover_queue_state": _mover_live_state(queue, mover) if mover else "absent",
    }
    if refusal == ORIGIN_UNREACHABLE_REFUSAL:
        state["ok"] = False
        state["refusal"] = refusal
    return state


def _reconcile_materialization_funding(queue, *, mover: str, tier: str,
                                       kind: str, gib: int
                                       ) -> dict[str, object]:
    """Is this materialization's prepaid funding actually in hand?

    The restage analogue of the reconciliation `commit_batch` performs against
    the pool record: the filed funding must read `transferring` (driving a
    publish-before-fund prefix through the SAME recovery API a restart would
    use, never a second reservation), and the mover must actually hold the
    batch's exact window price. A short transfer leaves the split intact and
    retains -- the drive retries resume it; nothing here re-funds.
    """

    record = queue.read_output_funding(mover, tier)
    if record is None or str(record.get("state")) != "transferring":
        driven = queue.drive_output_funding(mover, tier)
        if not driven.get("ok"):
            return {"ok": False, "step": "reconcile",
                    "refusal": f"prepaid-drive: {driven.get('refusal')}",
                    "drive": driven}
        record = queue.read_output_funding(mover, tier)
    if record is None or str(record.get("state")) != "transferring":
        return {"ok": False, "step": "reconcile",
                "refusal": "prepaid-funding-terminal",
                "state": (str(record.get("state"))
                          if record is not None else "unknown")}
    held = queue.tier_ledger(tier).holder_tokens(mover).get(kind, 0)
    if held < gib:
        return {"ok": False, "step": "reconcile", "refusal": "transfer-short",
                "moved": held, "expected": gib}
    return {"ok": True, "record": dict(record), "held": held}


def ensure_batch_materialized(queue, instance: Mapping[str, object],
                              template: Mapping[str, object], *,
                              batch_id: str, cas_root,
                              producer_action_key: str | None = None,
                              command_extra: Sequence[str] = (),
                              retry_policy: Mapping[str, object] | None = None,
                              restage_fill: bool | None = None,
                              ) -> dict[str, object]:
    """Make one ALREADY COMMITTED batch resident on its tier again.

    The bounded-window transition, and the only one this lane adds. A produced
    batch is an immutable logical unit with ONE durable origin charge; the
    stage copy under it is a window the fleet gives back at retirement. When a
    later read needs those bytes again, this re-materializes the SAME batch --
    same owner action and attempt, same logical batch, manifest, descriptors,
    namespace and output prefix, same durable charge -- by publishing a fresh
    mover over the sealed origin files and funding it by exact transfer from
    the producer's existing window, exactly as the first publication did.

    There is no second schema, no parallel cache and no second dispatcher: the
    caller supplies neither origins, nor tokens, nor a successor id. A
    write-only template refuses `template-is-write-only`: its origin-only
    batches (#912) have no window to fund and are staged by the consumer that
    declares them. The descriptors come from the immutable batch record, the
    successor's mover/funding key is the content-addressed key of a request PB
    seals over the filed materialization generation, and the credit is the
    ordinary prepaid transfer.

    States it answers:

    * a live first materialization, or a live successor mid-flight, is
      reported (`state: "live"`) and nothing is republished;
    * a stage-retired batch with its origins intact and provably unchanged
      gets a new materialization (`state: "materializing"`);
    * an origin whose identity no longer matches what the first commit
      recorded refuses `restage-origin-changed` BEFORE any funding, and a
      batch with no such proof refuses `restage-origin-proof-missing` rather
      than blessing whatever bytes are there now. The one exception is an
      origin whose timestamps alone moved and whose descriptor carries its
      sha256 (an NFS delegation recall, #1111): it is hashed with no lock
      held, and when the digest is the committed one the re-pin is filed on
      the entry under the lock, after a re-stat, before the intent;
    * a batch whose origins were reclaimed refuses: its durable charge is
      gone and so are the only bytes that could be copied.

    Crash-safe at every prefix, without a fresh epoch or a duplicated credit:
    the materialization intent is filed under the output-prefix ownership lock
    BEFORE any funding or movement side effect reaches the pool, and a restart
    re-drives that exact row's sealed mover key through the same idempotent
    stage/publish/fund steps. Two concurrent callers collapse onto one
    generation (the lock serializes the append; the loser resumes the winner's
    intent).

    Window credit is NOT invented here: a producer whose window is spent must
    call `refill_window` first, and an unfunded successor reports the ordinary
    `tier-reservation-unavailable` with its intent standing, resumable.

    Pool fill is off by default (#747). With `restage_fill=True`, or with no
    argument and the producer's sealed `RESTAGE_FILL_ENV` set to "1", the
    successor's mover also reserves `fill_mb_s_pool_side@<tier>` at the price
    `_seal_output_mover` derives. That term is not prepaid: the claim takes it
    from the tier's free fill like any mover's, and the stop returns it. A
    resume republishes the price the intent was sealed at, whatever the switch
    or the tier's offer says by then.
    """

    kwargs = dict(batch_id=batch_id, cas_root=cas_root,
                  producer_action_key=producer_action_key,
                  command_extra=command_extra, retry_policy=retry_policy,
                  restage_fill=restage_fill)
    answer = _ensure_batch_materialized(queue, instance, template, **kwargs)
    if answer.get("refusal") != "restage-origin-unverified":
        return answer
    # Only the timestamps of a digest-carrying origin moved: read it here,
    # with no lock held, and let the locked pass re-stat and file the re-pins.
    verdict = _origin_verdict_unlocked(queue.root, instance, template,
                                       batch_id, where="restage")
    if not verdict.get("ok"):
        return {"ok": False, "step": "origin",
                **{k: v for k, v in verdict.items() if k != "ok"}}
    return _ensure_batch_materialized(queue, instance, template, **kwargs,
                                      verified=verdict["repins"])


def _origin_verdict_unlocked(queue_root, instance: Mapping[str, object],
                             template: Mapping[str, object], batch_id: str, *,
                             where: str) -> dict[str, object]:
    """`_check_origin_identity` with reads, on a lockless read of the batch (#1111)."""

    try:
        checked_template, checked_instance = _require_bound_contract(
            template, instance)
        entry = _read_commitments(_commitments_path(
            queue_root, checked_instance))["batches"].get(batch_id)
        if not isinstance(entry, Mapping):
            return {"ok": False, "refusal": "unknown-batch"}
        filed, sealed = _load_batch_record(
            queue_root, checked_instance, checked_template, entry, batch_id)
    except ProducedOutputError as exc:
        return {"ok": False, "refusal": str(exc)}
    return _check_origin_identity(filed, sealed, entry, verify=True,
                                  where=where)


def _origin_gate_locked(commitments_path: Path,
                        commitments: Mapping[str, object], batch_id: str,
                        entry: Mapping[str, object],
                        filed: Mapping[str, object],
                        sealed: list[dict[str, object]],
                        verified: Sequence[Mapping[str, object]] | None
                        ) -> dict[str, object]:
    """The origin check a caller holding the prefix lock makes (#1111).

    Without a read. When only re-pins are missing and ``verified`` carries
    them (hashed by the caller before it took the lock), they are checked
    against a fresh stat, appended to the entry and written, and the check
    is made again. Returns `_check_origin_identity`'s answer.
    """

    verdict = _check_origin_identity(filed, sealed, entry)
    if (verdict["ok"] or verdict.get("refusal") != "restage-origin-unverified"
            or not verified):
        return verdict
    updated, refusal = _apply_origin_repins(entry, filed, sealed, verified)
    if updated is None:
        return {"ok": False, **(refusal or {})}
    if updated != entry:
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        batches[batch_id] = updated
        _write_commitments(commitments_path, commitments)
    return _check_origin_identity(filed, sealed, updated)


def _ensure_batch_materialized(queue, instance: Mapping[str, object],
                               template: Mapping[str, object], *,
                               batch_id: str, cas_root,
                               producer_action_key: str | None = None,
                               command_extra: Sequence[str] = (),
                               retry_policy: Mapping[str, object] | None = None,
                               restage_fill: bool | None = None,
                               verified: Sequence[Mapping[str, object]] | None = None,
                               ) -> dict[str, object]:
    """One pass of `ensure_batch_materialized`; ``verified`` are hashed re-pins."""

    try:
        checked_template, checked_instance = _require_bound_contract(
            template, instance)
    except ProducedOutputError:
        return {"ok": False, "step": "validate", "refusal": "template-mismatch"}
    _name(batch_id, where="batch_id")
    if checked_template.get("write_only"):
        # A write-only template's batches commit at origin and are never
        # staged by their owner (#912); this staged path is not theirs.
        return {"ok": False, "step": "validate",
                "refusal": "template-is-write-only"}
    owner = str(checked_instance["owner_action_key"])
    prefix = str(checked_instance["output_prefix"])

    # ---- Phase 1: INTENT. Holds the output-prefix ownership lock ONLY. No
    # ledger token, no funding record, no queue row is touched here; the one
    # durable effect is the immutable CAS request (inert until a row names it)
    # and the materialization row that makes every later step resumable.
    with queue.stage_ownership_lock(prefix):
        gated = _require_live_owner(queue, checked_instance)
        if gated is not None:
            return {**gated, "step": "owner"}
        try:
            commitments = _read_commitments(
                _commitments_path(queue.root, checked_instance))
        except ProducedOutputError as exc:
            return {"ok": False, "step": "validate",
                    "refusal": f"unknown-retain: {exc}"}
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        entry = batches.get(batch_id)
        if not isinstance(entry, Mapping):
            return {"ok": False, "step": "validate", "refusal": "unknown-batch"}
        if entry.get("origin_only") is True:
            # A read-back batch its owner committed at its origin (#1034) is
            # read from the owner's local spool; nothing stages it.
            return {"ok": False, "step": "validate",
                    "refusal": "batch-committed-at-origin"}
        try:
            filed, sealed = _load_batch_record(
                queue.root, checked_instance, checked_template, entry,
                batch_id)
            active = _active_materialization(entry)
            mats = _materializations(entry)
        except ProducedOutputError as exc:
            return {"ok": False, "step": "validate", "refusal": str(exc)}
        tier = str(filed.get("tier") or "")
        manifest_digest = str(filed.get("manifest_digest") or "")
        total = int(filed.get("total_bytes") or 0)
        if tier not in checked_template["permitted_tiers"]:
            return {"ok": False, "step": "validate",
                    "refusal": "tier-not-permitted"}
        try:
            canonical_ns = batch_namespace(checked_instance, batch_id,
                                           manifest_digest)
        except ProducedOutputError as exc:
            return {"ok": False, "step": "validate",
                    "refusal": f"unknown-retain: {exc}"}
        if (str(entry.get("batch_namespace") or "") != canonical_ns
                or str(filed.get("batch_namespace") or "") != canonical_ns
                or str(entry.get("mover_key") or "")
                != str(filed.get("mover_key") or "")
                or str(entry.get("tier") or "") != tier):
            return {"ok": False, "step": "validate",
                    "refusal": "unknown-retain: batch-target-mismatch"}
        if not active.get("retired"):
            # Something already owns this batch's material: the first
            # publication, or a successor this call (or another) started.
            # An ACTIVE copy is never replaced -- a pin on it blocks its
            # retirement, and retirement is what makes a successor legal.
            if str(active.get("source")) == "batch":
                return {"ok": True, "state": "live", "step": "none",
                        "batch_id": batch_id, "tier": tier,
                        "mover_key": str(active.get("mover_key") or ""),
                        "generation": 0, "batch_namespace": canonical_ns,
                        "manifest_digest": manifest_digest,
                        "total_bytes": total, "duplicate": True}
            resume = dict(active)
            if str(resume.get("state")) == "funded":
                # A replay of a materialization that is already funded and
                # published: its credit is transferred and its row is the
                # fleet's to run. Re-driving the pool primitives would be
                # idempotent but pointless, and "live" is the same answer the
                # first publication's live copy gets. `materialization_state`
                # is where a reader asks whether the bytes have landed.
                return {"ok": True, "state": "live", "step": "none",
                        "batch_id": batch_id, "tier": tier,
                        "mover_key": str(resume.get("mover_key") or ""),
                        "generation": int(resume.get("generation") or 0),
                        "batch_namespace": canonical_ns,
                        "manifest_digest": manifest_digest,
                        "total_bytes": total, "duplicate": True}
        elif entry.get("origin_reclaimed"):
            # The durable charge was released on proven absence: there are no
            # origin bytes left to copy, and nothing here re-creates them.
            return {"ok": False, "step": "authority",
                    "refusal": "origin-reclaimed-no-restage"}
        else:
            resume = None
        if resume is None:
            # A NEW materialization. Prove the origins are the committed ones
            # BEFORE anything is funded or published.
            gate = _origin_gate_locked(
                _commitments_path(queue.root, checked_instance), commitments,
                batch_id, entry, filed, sealed, verified)
            if not gate["ok"]:
                return {"ok": False, "step": "origin",
                        **{k: v for k, v in gate.items() if k != "ok"}}
            generation = len(mats) + 1
            sealed_mover = _seal_output_mover(
                queue, checked_instance, checked_template, sealed,
                batch_id=batch_id, tier=tier, cas_root=cas_root,
                producer_action_key=producer_action_key,
                command_extra=command_extra, retry_policy=retry_policy,
                generation=generation, restage_fill=restage_fill)
            if not sealed_mover.get("ok"):
                return sealed_mover
            mover = str(sealed_mover["mover_key"])
            host = str(sealed_mover["host"])
            kind = str(sealed_mover["kind"])
            gib = int(sealed_mover["gib"])
            fill = sealed_mover.get("fill")
            assert fill is None or isinstance(fill, int)
            mover_retry_policy = dict(sealed_mover["retry_policy"])
            mover_cas_root = str(sealed_mover["cas_root"])
            try:
                _append_materialization_locked(
                    queue, checked_instance, checked_template, batch_id,
                    mover_key=mover, tier=tier, generation=generation,
                    host=host, fill=fill)
            except ProducedOutputError as exc:
                return {"ok": False, "step": "intent", "refusal": str(exc)}
        else:
            # RESUME the filed intent. Deliberately no re-seal: the sealed key
            # on the row is the funding key, and re-deriving it from a tier
            # record that drifted since the crash would mint a second
            # generation for work already funded under the first.
            mover = str(resume.get("mover_key") or "")
            generation = int(resume.get("generation") or 0)
            host = str(resume.get("host") or "")
            # The price the intent was sealed at (#747), absent when it
            # reserved none. Anything else on the row is not a price.
            fill = resume.get("fill_mb_s_pool_side")
            if fill is not None and (type(fill) is not int or fill <= 0):
                return {"ok": False, "step": "resume",
                        "refusal": "unknown-retain: materialization-fill"}
            if str(resume.get("state")) != "funded":
                gate = _origin_gate_locked(
                    _commitments_path(queue.root, checked_instance),
                    commitments, batch_id, entry, filed, sealed, verified)
                if not gate["ok"]:
                    return {"ok": False, "step": "origin",
                            **{k: v for k, v in gate.items() if k != "ok"}}
            from prismabuild import core as _core_mod
            from prismabuild import storage_tiers as _tiers_mod

            kind = _tiers_mod.capacity_kind_of(tier)
            gib = _tiers_mod.stage_tokens_for_bytes(total)
            mover_retry_policy = (dict(retry_policy)
                                  if retry_policy is not None
                                  else {"max_attempts": 3, "retry_safe": True})
            try:
                mover_cas_root = str(_core_mod.PrismaBuildCAS(cas_root).root)
            except Exception as exc:
                return {"ok": False, "step": "resume",
                        "refusal": f"unknown-retain: {exc}"}

    # ---- Phase 2: SIDE EFFECTS. No ownership lock is held. Each step is the
    # existing prepaid primitive and each is idempotent, so a crash between
    # any two of them resumes from the filed intent rather than restarting.
    launch = _producer_launch_context(
        queue, str(producer_action_key
                   or os.environ.get(_ACTION_KEY_ENV_NAME()) or ""))
    if not launch.get("ok"):
        return launch
    staged = queue.stage_output_intent(
        tier_id=tier, owner_key=owner, mover_key=mover,
        instance=checked_instance, template=checked_template,
        batch_id=batch_id, descriptors=sealed)
    if not staged.get("ok"):
        staged["step"] = "stage"
        return staged
    published = _publish_output_mover_row(
        queue, mover_key=mover, cas_root=mover_cas_root, launch=launch,
        host=host, tier=tier, kind=kind, gib=gib,
        manifest_digest=manifest_digest, total=total,
        retry_policy=mover_retry_policy, fill=fill)
    if not published.get("ok"):
        return published
    funded = queue.fund_output_batch(
        tier_id=tier, owner_key=owner, mover_key=mover,
        instance=checked_instance, template=checked_template,
        batch_id=batch_id, descriptors=sealed)
    if not funded.get("ok"):
        funded["step"] = "fund"
        return funded

    # ---- Phase 3: RECONCILE. The verification takes no ownership lock, and
    # the ownership lock it then takes to file the flag holds only plain
    # file/ledger reads underneath it -- no transition lock is ever taken
    # under it, so this adds no lock order to the fleet.
    reconciled = _reconcile_materialization_funding(
        queue, mover=mover, tier=tier, kind=kind, gib=gib)
    if not reconciled.get("ok"):
        return reconciled
    record = reconciled["record"]
    assert isinstance(record, Mapping)
    with queue.stage_ownership_lock(prefix):
        try:
            _mark_materialization_funded_locked(
                queue, checked_instance, batch_id, mover_key=mover)
        except ProducedOutputError as exc:
            return {"ok": False, "step": "reconcile", "refusal": str(exc)}
    return {"ok": True, "state": "materializing", "step": "done",
            "batch_id": batch_id, "tier": tier, "mover_key": mover,
            "generation": generation, "batch_namespace": canonical_ns,
            "manifest_digest": manifest_digest, "total_bytes": total,
            "funding": "prepaid",
            "funding_generation": str(record.get("generation")),
            "tokens": [str(name) for name in record.get("tokens") or []],
            "resumed": resume is not None}


def build_stage_manifest(batch: Mapping[str, object],
                         mount_prefix: str) -> dict[str, object]:
    """Synthetic data manifest for `stage_move.move` (one mover per batch)."""

    entries = batch.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ProducedOutputError("batch carries no entries to stage")
    manifest_entries = [
        {"path": str(e["path"]), "offset": 0, "bytes": int(e["bytes"]),
         # DEV null digests stay JSON null through the manifest (core's
         # data-manifest validator and the mover both accept null); never
         # stringified.
         "sha256": (e["sha256"] if e["sha256"] is not None else None)}
        for e in entries]
    total = sum(int(e["bytes"]) for e in manifest_entries)
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {"tool": "produced-output-batch"},
        "mount_prefix": mount_prefix,
        "entries": manifest_entries,
        "entry_count": len(manifest_entries),
        "total_bytes": total,
        "annotations": {
            "coordinate_space": "output-manifest-batch",
            "batch_id": str(batch.get("batch_id")),
            "manifest_digest": str(batch.get("manifest_digest")),
        },
    }


def _checked_origin_ref(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != {
            "schema", "owner_action_key", "owner_nonce", "template_id",
            "batch_id", "manifest_digest"}:
        raise ProducedOutputError(
            "origin batch ref must carry exactly schema, owner_action_key, "
            "owner_nonce, template_id, batch_id and manifest_digest")
    if value.get("schema") != ORIGIN_BATCH_REF_SCHEMA_V1:
        raise ProducedOutputError(
            f"origin batch ref schema must be {ORIGIN_BATCH_REF_SCHEMA_V1!r}")
    return {"schema": ORIGIN_BATCH_REF_SCHEMA_V1,
            "owner_action_key": _hex64(value.get("owner_action_key"),
                                       where="origin batch ref owner_action_key"),
            "owner_nonce": _hex32(value.get("owner_nonce"),
                                  where="origin batch ref owner_nonce"),
            "template_id": _name(value.get("template_id"),
                                 where="origin batch ref template_id"),
            "batch_id": _name(value.get("batch_id"),
                              where="origin batch ref batch_id"),
            "manifest_digest": _hex64(value.get("manifest_digest"),
                                      where="origin batch ref manifest_digest")}


def _origin_ref_scope(queue_root: str | Path, checked_ref: Mapping[str, str]
                      ) -> tuple[dict[str, object], dict[str, object]]:
    """The filed instance and template a checked ref names, bound together.

    Read from PB's own records only: the instance at
    ``<owner>/<template>.<nonce>`` and the template it is bound to. Raises
    `ProducedOutputError` naming what is missing or does not match.
    """

    scope_dir = (Path(queue_root) / "residency" / OUTPUT_SCOPES_SUBDIR
                 / checked_ref["owner_action_key"]
                 / f"{checked_ref['template_id']}.{checked_ref['owner_nonce']}")
    try:
        instance = validate_instance(json.loads(
            (scope_dir / "instance.json").read_text()))
        template = validate_template(json.loads(
            (Path(queue_root) / "residency" / OUTPUT_TEMPLATES_SUBDIR
             / f"{checked_ref['template_id']}.json").read_text()))
    except FileNotFoundError:
        raise ProducedOutputError(
            "origin-batch-unknown: no instance or template is filed for "
            f"{checked_ref['owner_action_key'][:12]}/"
            f"{checked_ref['template_id']}") from None
    except (OSError, ValueError) as exc:
        raise ProducedOutputError(
            f"unknown-retain: origin batch scope unreadable: {exc}") from None
    attempt = instance["owner_attempt"]
    assert isinstance(attempt, dict)
    if (instance["owner_action_key"] != checked_ref["owner_action_key"]
            or attempt["nonce"] != checked_ref["owner_nonce"]
            or instance["template_id"] != checked_ref["template_id"]
            or instance["template_sha256"] != template_sha256(template)):
        raise ProducedOutputError(
            "unknown-retain: origin batch scope is bound to another template "
            "or attempt")
    return instance, template


def load_origin_batch(queue_root: str | Path, ref: Mapping[str, object]
                      ) -> dict[str, object]:
    """Resolve one declared origin-only batch to its committed files (#912).

    Everything comes from what PB filed, never from the reference beyond its
    coordinates: the instance at ``<owner>/<template>.<nonce>``, the template
    it is bound to, the commitments entry, and the immutable batch record
    through the one batch loader. The batch must be origin-only, committed
    over the digest the reference names, and not reclaimed, and every origin
    must still carry the identity its commit recorded, and a consumed batch
    that `origin_retirement_tick` has started to retire refuses. That recheck
    is taken here, at submission; the consumer's mover later verifies each entry's
    sha256 as it copies, which is what binds the staged bytes to the
    committed ones. Raises `ProducedOutputError` naming what failed.

    An origin whose timestamps alone moved (an NFS delegation recall, #1111)
    is read and hashed, with no lock held; when its digest is the committed
    one the batch resolves, and the re-pin is filed on the commitments entry
    (`load_origin_batches`), so the next check compares against it and reads
    nothing.

    Returns ``{"ref", "instance", "template", "record", "entries"}`` with the
    sealed descriptors in the batch's own order.
    """

    return load_origin_batches(queue_root, [ref])[0]


def load_origin_batches(queue_root: str | Path,
                        refs: Sequence[Mapping[str, object]]
                        ) -> list[dict[str, object]]:
    """`load_origin_batch` for each ref, with one commitments write per instance.

    Each batch is checked with no lock held. The re-pins its content checks
    made are then filed per instance: one output-prefix lock, one re-read of
    the commitments document, a re-stat of each re-pinned origin, and one
    write (`_file_origin_repins`), whatever the number of batches.
    """

    loaded = [_load_origin_batch(queue_root, ref) for ref in refs]
    _file_origin_repins(queue_root, loaded)
    return [item for item, _repins in loaded]


def _load_origin_batch(queue_root: str | Path, ref: Mapping[str, object]
                       ) -> tuple[dict[str, object], list[dict[str, object]]]:
    """One batch resolved with no lock held, and the re-pins it needs filed."""

    checked_ref = _checked_origin_ref(ref)
    instance, template = _origin_ref_scope(queue_root, checked_ref)
    if not template.get("write_only"):
        raise ProducedOutputError(
            "origin-batch-not-write-only: its template stages its batches")
    commitments = _read_commitments(_commitments_path(queue_root, instance))
    entry = commitments["batches"].get(checked_ref["batch_id"])
    if not isinstance(entry, Mapping):
        raise ProducedOutputError(
            f"origin-batch-uncommitted: {checked_ref['batch_id']}")
    if (entry.get("origin_only") is not True
            or entry.get("manifest_digest") != checked_ref["manifest_digest"]):
        raise ProducedOutputError(
            "origin-batch-mismatch: the committed batch is not origin-only "
            "over this manifest digest")
    if entry.get("origin_reclaimed"):
        raise ProducedOutputError(
            f"origin-batch-reclaimed: {checked_ref['batch_id']}")
    if entry.get("retiring"):
        # `origin_retirement_tick` decided to delete it (#914); its files may
        # already be going, and no new consumer may count on them.
        raise ProducedOutputError(
            f"origin-batch-retiring: {checked_ref['batch_id']}")
    filed, sealed = _load_batch_record(
        queue_root, instance, template, entry, checked_ref["batch_id"])
    if filed.get("origin_only") is not True:
        raise ProducedOutputError(
            "unknown-retain: origin batch record is not origin-only")
    verdict = _check_origin_identity(filed, sealed, entry, verify=True,
                                     where="declare")
    if not verdict["ok"]:
        detail = f": {verdict['detail']}" if verdict.get("detail") else ""
        raise ProducedOutputError(
            f"origin-batch-changed: {verdict['refusal']} "
            f"({checked_ref['batch_id']}){detail}")
    return ({"ref": checked_ref, "instance": instance, "template": template,
             "record": filed, "entries": sealed}, list(verdict["repins"]))


def _file_origin_repins(queue_root: str | Path,
                        loaded: Sequence[tuple[Mapping[str, object],
                                               Sequence[Mapping[str, object]]]]
                        ) -> None:
    """File the re-pins lockless checks made, under each instance's prefix lock (#1111).

    Per instance: the output-prefix lock `commit_origin_batch` and the
    retirement take, the commitments document read again, each origin
    stat'ed again and required to still be the identity its content check
    hashed (`_apply_origin_repins`), and one write. The lock holds no read of
    an origin's bytes. A batch that was reclaimed or re-committed meanwhile
    is left alone: whatever reads it next reads its state then. Raises
    `ProducedOutputError` when an origin changed after its content was read.
    """

    from prismabuild import pool as pool_mod

    groups: dict[str, tuple[Mapping[str, object], list]] = {}
    for item, repins in loaded:
        if repins:
            instance = item["instance"]
            key = str(_commitments_path(queue_root, instance))
            groups.setdefault(key, (instance, []))[1].append((item, repins))
    if not groups:
        return
    queue = pool_mod.PoolQueue(queue_root)
    for key, (instance, items) in groups.items():
        path = Path(key)
        with queue.stage_ownership_lock(str(instance["output_prefix"])):
            commitments = _read_commitments(path)
            batches = commitments["batches"]
            assert isinstance(batches, dict)
            changed = False
            for item, repins in items:
                batch_id = str(item["ref"]["batch_id"])
                entry = batches.get(batch_id)
                if (not isinstance(entry, Mapping)
                        or entry.get("origin_only") is not True
                        or entry.get("manifest_digest")
                        != item["ref"]["manifest_digest"]
                        or entry.get("origin_reclaimed")):
                    continue
                updated, refusal = _apply_origin_repins(
                    entry, item["record"], item["entries"], repins)
                if updated is None:
                    detail = (f": {refusal['detail']}"
                              if refusal and refusal.get("detail") else "")
                    raise ProducedOutputError(
                        f"origin-batch-changed: "
                        f"{(refusal or {}).get('refusal')} ({batch_id}){detail}")
                if updated != entry:
                    batches[batch_id] = updated
                    changed = True
            if changed:
                _write_commitments(path, commitments)


def origin_batch_manifest(queue_root: str | Path,
                          refs: Sequence[Mapping[str, object]]
                          ) -> dict[str, object]:
    """The data manifest a consumer declares to read origin-only batches (#912).

    One v1 manifest over every declared batch, in the order given: each
    batch's entries in its own order, one read phase per batch, and the
    references themselves under the ``produced_output_batches`` annotation.
    The consumer submits it with ``pbrun --data-manifest`` and stages it with
    ``--residency stage`` like any other input; ``pbrun`` derives it again
    from the queue at submission and refuses a manifest that says anything
    else. The mount prefix is the common directory of the batches' output
    prefixes. Every batch is resolved through `load_origin_batch`, so an
    uncommitted, reclaimed or changed batch refuses here.
    """

    if (not isinstance(refs, Sequence) or isinstance(refs, (str, bytes))
            or not refs):
        raise ProducedOutputError("origin batch manifest needs at least one ref")
    loaded = load_origin_batches(queue_root, refs)
    keys = [tuple(item["ref"].values()) for item in loaded]
    if len(set(keys)) != len(keys):
        raise ProducedOutputError("origin batch manifest names a batch twice")
    prefixes = [str(item["instance"]["output_prefix"]) for item in loaded]
    mount_prefix = os.path.commonpath(prefixes)
    if mount_prefix == "/":
        raise ProducedOutputError(
            "origin batches share no directory below / to mount")
    entries: list[dict[str, object]] = []
    phases: list[dict[str, object]] = []
    running = 0
    seen: set[str] = set()
    for index, item in enumerate(loaded):
        size = 0
        for desc in item["entries"]:
            if str(desc["path"]) in seen:
                # Two committed batches over one origin path: a retried owner
                # attempt can file one (ownership is per attempt), and at most
                # one of them still names the file that is there.
                raise ProducedOutputError(
                    f"origin batch manifest names {desc['path']} twice")
            seen.add(str(desc["path"]))
            entries.append({"path": str(desc["path"]), "offset": 0,
                            "bytes": int(desc["bytes"]),
                            "sha256": desc["sha256"]})
            size += int(desc["bytes"])
        running += size
        phases.append({"name": f"{index:04d}-{item['ref']['batch_id']}",
                       "bytes": size, "cumulative_bytes": running})
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {"tool": "prismabuild.produced_output.origin_batch_manifest"},
        "mount_prefix": mount_prefix,
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": running,
        "annotations": {
            "phases": phases,
            ORIGIN_BATCHES_ANNOTATION: [dict(item["ref"]) for item in loaded],
        },
    }


def _slot_phases(checked: Mapping[str, object], slots: object, *,
                 where: str) -> dict[str, dict[str, object]]:
    """``slots`` by phase name, checked against a validated v2 manifest."""

    if (not isinstance(slots, Sequence) or isinstance(slots, (str, bytes))
            or not slots):
        raise ProducedOutputError(f"{where} must be a non-empty array")
    plan = checked["read_plan"]
    assert isinstance(plan, Mapping)
    names = {str(phase["name"]) for phase in plan["phases"]}
    out: dict[str, dict[str, object]] = {}
    for slot in slots:
        if not isinstance(slot, Mapping) or "phase" not in slot:
            raise ProducedOutputError(f"{where} entries must name a phase")
        name = slot["phase"]
        if not isinstance(name, str) or name not in names:
            raise ProducedOutputError(
                f"{where} names {name!r}, which is not a read phase")
        if name in out:
            raise ProducedOutputError(f"{where} names phase {name!r} twice")
        out[name] = dict(slot)
    return out


def place_origin_batches(queue_root: str | Path, static: Mapping[str, object],
                         slots: Sequence[Mapping[str, object]]
                         ) -> dict[str, object]:
    """A data_manifest.v2 that reads committed batches where its plan says (#946).

    ``static`` is the consumer's own v2 manifest: its entries, and a read
    plan in which each phase that will read batches reads nothing yet
    (``entry_indices: []``). ``slots`` is ``[{"phase", "refs"}]``: which
    committed origin-only batches each such phase reads. Every slot's batches
    are resolved through `origin_batch_manifest`, so an uncommitted,
    reclaimed, retiring or changed batch refuses here.

    The result is canonical, so ``pbrun`` can derive it again and compare:
    the static entries keep their indices, each slot's batch entries follow
    them in plan order, and each slot phase reads exactly its batch's entries
    in the batch's own order. Every other phase is unchanged, so its
    ``entry_indices`` keep their v2 meaning, re-reads included. Phase sizes,
    running sums, ``read_bytes`` and the totals are recomputed; the mount
    prefix is the common directory of the static prefix and the batches'.
    ``annotations.produced_output_batches`` lists every ref in plan order and
    ``annotations.produced_output_slots`` the ``{"phase", "refs"}`` placed.
    A batch is read in its slot phase only.
    """

    from prismabuild import core as core_mod

    checked = core_mod.validate_data_manifest(static)
    if checked["schema"] != core_mod.DATA_MANIFEST_SCHEMA_V2:
        raise ProducedOutputError(
            "placing batches in a read plan needs a data_manifest.v2")
    notes = dict(checked["annotations"])
    if ORIGIN_BATCHES_ANNOTATION in notes:
        raise ProducedOutputError(
            f"a v2 read plan declares its batches through "
            f"{ORIGIN_SLOTS_ANNOTATION}, not {ORIGIN_BATCHES_ANNOTATION}")
    wanted = _slot_phases(checked, slots, where="slots")
    plan = checked["read_plan"]
    assert isinstance(plan, Mapping)
    entries = [dict(entry) for entry in checked["entries"]]
    prefixes = [str(checked["mount_prefix"])]
    placed_slots: list[dict[str, object]] = []
    all_refs: list[dict[str, object]] = []
    orders: list[tuple[str, list[int]]] = []
    for phase in plan["phases"]:
        name = str(phase["name"])
        slot = wanted.get(name)
        if slot is None:
            orders.append((name, list(phase["entry_indices"])))
            continue
        if set(slot) != {"phase", "refs"}:
            raise ProducedOutputError(
                f"slot {name!r} must carry exactly phase and refs")
        if phase["entry_indices"]:
            raise ProducedOutputError(
                f"slot phase {name!r} already reads static entries")
        batch = origin_batch_manifest(queue_root, slot["refs"])
        start = len(entries)
        entries.extend(dict(entry) for entry in batch["entries"])
        orders.append((name, list(range(start, len(entries)))))
        refs = list(batch["annotations"][ORIGIN_BATCHES_ANNOTATION])
        placed_slots.append({"phase": name, "refs": refs})
        all_refs.extend(refs)
        prefixes.append(str(batch["mount_prefix"]))
    keys = [tuple(sorted(ref.items())) for ref in all_refs]
    if len(set(keys)) != len(keys):
        raise ProducedOutputError("slots name one batch twice")
    phases: list[dict[str, object]] = []
    running = 0
    for name, indices in orders:
        size = sum(int(entries[index]["bytes"]) for index in indices)
        running += size
        phases.append({"name": name, "entry_indices": indices,
                       "bytes": size, "cumulative_bytes": running})
    mount_prefix = os.path.commonpath(prefixes)
    if mount_prefix == "/":
        raise ProducedOutputError(
            "the static entries and the batches share no directory below / "
            "to mount")
    notes.pop(ORIGIN_SLOTS_ANNOTATION, None)
    notes[ORIGIN_BATCHES_ANNOTATION] = all_refs
    notes[ORIGIN_SLOTS_ANNOTATION] = placed_slots
    try:
        return core_mod.validate_data_manifest({
            "schema": core_mod.DATA_MANIFEST_SCHEMA_V2,
            "produced_by": dict(checked["produced_by"]),
            "mount_prefix": mount_prefix,
            "entries": entries,
            "entry_count": len(entries),
            "total_bytes": sum(int(entry["bytes"]) for entry in entries),
            "annotations": notes,
            "read_plan": {"phases": phases, "read_bytes": running},
        })
    except core_mod.PrismaBuildError as exc:
        raise ProducedOutputError(f"placed manifest refused: {exc}") from None


def verify_placed_origin_batches(queue_root: str | Path,
                                 manifest: Mapping[str, object]) -> None:
    """Refuse a v2 manifest whose batches are not placed as its slots say (#946).

    The v2 counterpart of deriving `origin_batch_manifest` again: the
    manifest's static part is recovered by taking off the trailing batch
    entries and emptying the slot phases, `place_origin_batches` places the
    declared slots into it again from the queue's own records, and the
    result must be the manifest, byte for byte in its normalized form. So a
    submitted plan can read static entries in any order and re-read them, but
    each batch it declares is exactly the committed one, read once, at its
    slot. Raises `ProducedOutputError` naming what does not match.
    """

    from prismabuild import core as core_mod

    checked = core_mod.validate_data_manifest(manifest)
    notes = dict(checked["annotations"])
    slots = notes.get(ORIGIN_SLOTS_ANNOTATION)
    wanted = _slot_phases(checked, slots, where=ORIGIN_SLOTS_ANNOTATION)
    for name, slot in wanted.items():
        if set(slot) != {"phase", "refs"}:
            raise ProducedOutputError(
                f"slot {name!r} must carry exactly phase and refs; an "
                "--after slot is placed by the release, not submitted")
    counts = {}
    for name, slot in wanted.items():
        counts[name] = int(origin_batch_manifest(
            queue_root, slot["refs"])["entry_count"])
    batch_entries = sum(counts.values())
    entries = list(checked["entries"])
    if batch_entries >= len(entries):
        raise ProducedOutputError(
            "a v2 read plan with slots needs static entries of its own")
    static_count = len(entries) - batch_entries
    plan = checked["read_plan"]
    assert isinstance(plan, Mapping)
    phases: list[dict[str, object]] = []
    running = 0
    for phase in plan["phases"]:
        indices = list(phase["entry_indices"])
        if str(phase["name"]) in wanted:
            indices = []
        elif any(index >= static_count for index in indices):
            raise ProducedOutputError(
                f"phase {phase['name']!r} reads a batch entry outside its slot")
        size = sum(int(entries[index]["bytes"]) for index in indices)
        running += size
        phases.append({"name": phase["name"], "entry_indices": indices,
                       "bytes": size, "cumulative_bytes": running})
    static_entries = entries[:static_count]
    skeleton_notes = {key: value for key, value in notes.items()
                      if key not in (ORIGIN_BATCHES_ANNOTATION,
                                     ORIGIN_SLOTS_ANNOTATION)}
    try:
        skeleton = core_mod.validate_data_manifest({
            "schema": core_mod.DATA_MANIFEST_SCHEMA_V2,
            "produced_by": dict(checked["produced_by"]),
            "mount_prefix": checked["mount_prefix"],
            "entries": static_entries,
            "entry_count": static_count,
            "total_bytes": sum(int(entry["bytes"]) for entry in static_entries),
            "annotations": skeleton_notes,
            "read_plan": {"phases": phases, "read_bytes": running},
        })
    except core_mod.PrismaBuildError as exc:
        raise ProducedOutputError(
            f"the manifest's static part is not a read plan: {exc}") from None
    placed = place_origin_batches(
        queue_root, skeleton,
        [{"phase": name, "refs": wanted[name]["refs"]}
         for name in [str(phase["name"]) for phase in plan["phases"]]
         if name in wanted])
    if placed != checked:
        raise ProducedOutputError(
            "the manifest does not read its declared batches where its slots "
            "say; build it with produced_output.place_origin_batches")


def _strict_int(value: object, *, where: str) -> int:
    # Exact JSON integers only: bools, strings, floats, and objects are
    # corrupt counts, never zero. `int()` would coerce them all.
    if type(value) is not int:
        raise ProducedOutputError(f"{where} must be an integer")
    return int(value)


def _load_batch_record(queue_root: str | Path,
                       checked_instance: Mapping[str, object],
                       checked_template: Mapping[str, object],
                       commitments_entry: Mapping[str, object],
                       batch_id: str) -> tuple[dict[str, object], list[dict[str, object]]]:
    """Load and fully validate one immutable batch record.

    The single loader for retire/reclaim (and any future reader): bounded
    intake, exact schema/id/instance binding, strict integer shapes (bools
    are not counts), a non-empty entry list whose per-class sizes and
    total match the record, and -- strongest -- every entry re-validated
    as a descriptor against the bound template and instance (mapping
    shape enforced by the descriptor validator itself, never `dict()`
    coercion) with the canonical manifest digest recomputed over them
    matching the seal. A missing, unreadable, or damaged record
    (entries removed, replaced, unbound, or non-mapping) raises
    `ProducedOutputError`: unknown or tampered state retains quota and
    refuses retirement, never filters away to a vacuous proof.
    """

    attempt = checked_instance["owner_attempt"]
    assert isinstance(attempt, dict)
    batch_file = (Path(queue_root) / "residency" / OUTPUT_BATCHES_SUBDIR
                  / instance_namespace(checked_instance)
                  / f"{batch_id}.json")
    try:
        with open(batch_file, "rb") as handle:
            raw = handle.read(4 * 1024 * 1024 + 1)
    except FileNotFoundError:
        raise ProducedOutputError(
            "unknown-retain: batch-record-missing") from None
    except OSError as exc:
        raise ProducedOutputError(f"unknown-retain: {exc}") from None
    if len(raw) > 4 * 1024 * 1024:
        raise ProducedOutputError("unknown-retain: batch-record-oversize")
    try:
        filed = json.loads(raw.decode())
    except (ValueError, UnicodeDecodeError) as exc:
        raise ProducedOutputError(
            f"unknown-retain: batch-record-unreadable: {exc}") from None
    if not isinstance(filed, Mapping):
        raise ProducedOutputError("unknown-retain: batch-record-missing")
    if filed.get("schema") != BATCH_SCHEMA_V1:
        raise ProducedOutputError("unknown-retain: batch-record-schema")
    filed_attempt = filed.get("owner_attempt")
    if (not isinstance(filed_attempt, Mapping)
            or set(filed_attempt) != {"nonce", "scope_id"}):
        raise ProducedOutputError("unknown-retain: batch-record-attempt")
    if (str(filed.get("batch_id") or "") != batch_id
            or str(filed.get("template_id") or "")
            != str(checked_template["template_id"])
            or str(filed.get("template_sha256") or "")
            != str(checked_instance["template_sha256"])
            or str(filed.get("owner_action_key") or "")
            != str(checked_instance["owner_action_key"])
            or str(filed_attempt.get("nonce") or "") != str(attempt["nonce"])
            or str(filed_attempt.get("scope_id") or "")
            != str(attempt["scope_id"])
            or str(filed.get("manifest_digest") or "")
            != str(commitments_entry.get("manifest_digest") or "")):
        raise ProducedOutputError(
            "unknown-retain: batch-record-binding-mismatch")
    entries = filed.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ProducedOutputError("unknown-retain: batch-record-no-entries")
    sealed = [validate_descriptor(entry, checked_template,
                                  checked_instance)
              for entry in entries]
    if _strict_int(filed.get("entry_count"),
                   where="batch record entry_count") != len(sealed):
        raise ProducedOutputError("unknown-retain: batch-record-count")
    classes = {"payload": 0, "checkpoint": 0, "temp": 0}
    for desc in sealed:
        classes[str(desc["artifact_class"])] += int(desc["bytes"])
    stored_classes = commitments_entry.get("class_bytes")
    if not isinstance(stored_classes, Mapping):
        raise ProducedOutputError("unknown-retain: batch-record-classes")
    for cls in classes:
        if int(classes[cls]) != _strict_int(
                stored_classes.get(cls),
                where=f"committed batch {batch_id!r}.{cls}"):
            raise ProducedOutputError("unknown-retain: batch-record-classes")
    if sum(classes.values()) != _strict_int(
            filed.get("total_bytes"), where="batch record total_bytes"):
        raise ProducedOutputError("unknown-retain: batch-record-total")
    if output_manifest_sha256(sealed) != str(filed.get("manifest_digest")):
        raise ProducedOutputError("unknown-retain: batch-record-manifest")
    return dict(filed), sealed


def _mark_batch_retired_locked(queue, checked_instance: Mapping[str, object],
                               checked_template: Mapping[str, object],
                               batches: dict, batch_id: str,
                               receipt: Mapping[str, object],
                               staged_paths: list[str]) -> bool:
    """Mark stage retirement in ``batches``, under the caller's ownership lock.

    ``batches`` is the commitments document the caller read under that lock,
    and the caller writes it back under the same lock: this marks, it does
    not read or write the document (#1072).  Returns whether it changed
    anything.

    Proof-checked, never a bare flag: the receipt must be a complete
    egress for this exact batch (mover + namespace match, no errors),
    and the batch must still be unretired. Records the staged paths the
    egress vouched so later census attribution can name them.
    """

    entry = batches.get(batch_id)
    if not isinstance(entry, Mapping):
        raise ProducedOutputError("unknown batch_id for this instance")
    if entry.get("retired"):
        return False
    # Owner/attempt/manifest proof comes from the filed immutable batch
    # record through the single loader: exact schema/id/binding plus
    # re-validated entries whose canonical digest matches the seal.
    # Neither a caller dict nor a bare commitments flag can retire
    # another attempt's batch or a damaged record.
    try:
        _load_batch_record(queue.root, checked_instance, checked_template,
                            entry, batch_id)
    except ProducedOutputError as exc:
        raise ProducedOutputError(str(exc)) from None
    if not isinstance(receipt, Mapping) or receipt.get("complete") is not True:
        raise ProducedOutputError("retire needs a complete egress receipt")
    if receipt.get("errors"):
        raise ProducedOutputError("retire needs an error-free egress receipt")
    if (str(receipt.get("action_key") or "") != str(entry.get("mover_key") or "")
            or str(receipt.get("consumer_action_key") or "")
            != str(entry.get("batch_namespace") or "")):
        raise ProducedOutputError("retire receipt names another batch")
    entry = dict(entry)
    entry["retired"] = True
    entry["staged_paths"] = sorted(set(staged_paths))
    batches[batch_id] = entry
    return True


#: The deferral `retire_batch` reports while the retirement's OWN egress action
#: is queued or running on the tier host. It rides the egress receipt's
#: `deferred_own` list beside `own-copy-in-flight` because it is the same kind
#: of answer: bytes, proof and full credit are kept, and an ordinary retry
#: completes the retirement once that action has ended.
OWN_EGRESS_IN_FLIGHT = "own-egress-in-flight"


def _egress_runs_in_process(record: Mapping[str, object] | None) -> bool:
    """Does THIS process sit on the box that owns the stage?

    The egress unlinks staged files, and only the tier host mounts the stage
    read-write: the GPU hosts mount it read-only, so an egress run in a
    producer there cannot delete anything (#801). The tier record's `host` is
    the fact `tier_loop.py` announces for exactly this purpose, and it is the
    same fact the movers are placed by.

    A wrong "elsewhere" is always safe -- the tier-host route works from any
    box, including the tier host itself -- so nothing here tries to be clever.
    Inside a container `socket.gethostname()` is the container's name, never
    the box's, and the answer is "elsewhere": that is the safe direction, and
    it must not be "fixed" by resolving `record["host"]` some other way. A
    tier with no announced record or host keeps the in-process egress. No
    mover can be sealed without the record, so its absence is a fixture or a
    tier that has gone away, and the in-process egress fails safe there: an
    unlink the mount refuses is an `errors` entry that keeps the bytes, the
    fragment and the tokens. `retire_batch` adds one condition: the
    in-process egress also needs the fleet tool importable, and an owner that
    cannot import it takes the tier-host route wherever it runs.
    """

    import socket

    host = str(record.get("host") or "") if isinstance(record, Mapping) else ""
    return not host or host == socket.gethostname()


def _seal_output_egress(queue, *, record: Mapping[str, object],
                        producer: str, cas_root, batch_id: str,
                        generation: int, target_mover: str, consumer: str
                        ) -> dict[str, object]:
    """Seal (never file) the egress action for one materialization.

    The egress node `pbrun` seals for a consumer's staged range, sealed the
    same way for a produced batch: `stage_release.py` off the ANNOUNCED TIER
    RECORD, placed on the box that owns the stage, one CPU and one GiB, and no
    tier demand -- an egress returns capacity, and one that had to reserve
    some before giving any back would deadlock exactly when the stage is full.
    It carries no data manifest, so nothing prewarms for it.

    Both roots are the ones the MOVER was sealed with -- the tier's announced
    mountpoint and the produced-output fragment root under this pool -- and
    never the calling process's own spelling of them: the action runs on
    another box, where a wrong stage root reads as an unregistered stage and
    a wrong fragment root reads as "nothing staged", which is a complete
    receipt that deleted nothing. The answer carries both so the caller can
    refuse a retirement whose own arguments name different ones.

    The key is content-addressed over the whole sealed action: this command,
    the tier's announced interpreter and tools, the placement tag and the
    producer's request. It is unique per materialization, and every call
    re-derives it instead of recording it; it moves only when one of those
    facts does, and an egress under the earlier key is then an idempotent
    no-op beside this one.
    """

    from prismabuild import movement_actions
    from prismabuild import pool as pool_mod

    parent = _read_producer_request(cas_root, producer)
    if isinstance(parent, Mapping):
        return dict(parent)
    cas, request = parent
    templated = _producer_movement_template(queue, request, producer)
    if not templated.get("ok"):
        return templated
    try:
        mover_python, _mover_tool, egress_tool = (
            movement_actions.movement_tools(record))
    except SystemExit as exc:
        return {"ok": False, "step": "resolve", "refusal": str(exc)}
    host = str(record.get("host") or "")
    stage_root = str(record.get("mountpoint") or "")
    if not stage_root.startswith("/"):
        return {"ok": False, "step": "resolve",
                "refusal": "the stage tier announces no mountpoint to "
                           "release from"}
    residency_root = str(output_fragment_root(
        Path(queue.root) / pool_mod.RESIDENCY))
    command = [mover_python, egress_tool,
               "--pool-root", str(queue.root),
               "--mover-action-key", target_mover,
               "--consumer-action-key", consumer,
               "--stage-root", stage_root,
               "--residency-root", residency_root]
    retry_policy = {"max_attempts": 3, "retry_safe": True}
    log_name = f"produced-output-egress-{batch_id}.log"
    if int(generation) > 0:
        log_name = (f"produced-output-egress-{batch_id}"
                    f"-m{int(generation)}.log")
    try:
        action = movement_actions.seal_movement_action(
            templated["template"], command=command,
            demand={"cpu": 1, "mem_gb": 1},
            tags=[host] if host else [],
            log_name=log_name, retry_policy=retry_policy)
    except SystemExit as exc:
        return {"ok": False, "step": "seal", "refusal": str(exc)}
    except Exception as exc:
        return {"ok": False, "step": "seal", "refusal": str(exc)}
    return {"ok": True, "action": action, "cas": cas,
            "egress_key": str(action["action_key"]), "host": host,
            "stage_root": stage_root, "residency_root": residency_root,
            "retry_policy": retry_policy}


def _tier_host_egress(queue, *, record: Mapping[str, object], producer: str,
                      cas_root, batch_id: str, generation: int,
                      target_mover: str, consumer: str, stage_root: str,
                      residency_root: str) -> dict[str, object]:
    """Run one materialization's egress ON THE TIER HOST, across calls.

    Returns `{"receipt"}` -- a complete, error-free egress receipt, ready for
    the caller's filing phase -- or `{"answer"}`, the caller's whole answer.

    One `retire_batch` call cannot wait for another box, so the egress is an
    ordinary queue action and the retirement is a re-driven call: the first
    call publishes the action and answers the typed `egress-incomplete`
    deferral `own-egress-in-flight`; a later call finds the action ended,
    reads the receipt it filed, and files the retirement. Nothing is decided
    from silence: only a filed receipt naming this egress key and this batch's
    namespace, complete and error-free, retires anything.

    `stage_release.evict` is idempotent, so an egress that runs twice -- a
    queue retry, or a key that moved because the tier announced other tools --
    is a no-op receipt, never a second delete.

    `stage_root` and `residency_root` are the CALLER's; the action is sealed
    with the tier's (`_seal_output_egress`), and a caller naming different
    ones is refused before anything is published.
    """

    from prismabuild import pool as pool_mod

    def incomplete(receipt: Mapping[str, object], key: str | None
                   ) -> dict[str, object]:
        answer: dict[str, object] = {
            "ok": False, "refusal": "egress-incomplete",
            "receipt": dict(receipt)}
        if key:
            answer["egress_action_key"] = key
        return {"answer": answer}

    def deferred(reason: str, key: str | None = None,
                 state: str | None = None) -> dict[str, object]:
        receipt: dict[str, object] = {
            "schema": pool_mod.POOL_EGRESS_SCHEMA_V1,
            "action_key": target_mover,
            "consumer_action_key": consumer,
            "stage_root": str(stage_root),
            "complete": False, "errors": [],
            "live_pins": [], "deferred_handoffs": [],
            "deferred_own": [reason]}
        if key:
            receipt["egress_action_key"] = key
        if state:
            receipt["egress_state"] = state
        return incomplete(receipt, key)

    # The mover's OWN copy first, exactly as the egress itself would answer
    # (#795): a copy still queued or running is never raced by its egress.
    mover_state = _mover_live_state(queue, target_mover)
    if mover_state == "unknown":
        return {"answer": {"ok": False,
                           "refusal": "unknown-retain: mover-row-unreadable"}}
    if mover_state in (pool_mod.READY, pool_mod.CLAIMED):
        return deferred("own-copy-in-flight")
    sealed = _seal_output_egress(
        queue, record=record, producer=producer, cas_root=cas_root,
        batch_id=batch_id, generation=generation, target_mover=target_mover,
        consumer=consumer)
    if not sealed.get("ok"):
        return {"answer": {"ok": False, "step": sealed.get("step"),
                           "refusal": "unknown-retain: egress-seal: "
                                      f"{sealed.get('refusal')}"}}
    for name, mine, sealed_root in (
            ("stage-root", stage_root, sealed["stage_root"]),
            ("residency-root", residency_root, sealed["residency_root"])):
        if os.path.normpath(str(mine)) != os.path.normpath(str(sealed_root)):
            return {"answer": {
                "ok": False,
                "refusal": f"unknown-retain: {name}-mismatch: the tier's is "
                           f"{sealed_root}, this retirement named {mine}"}}
    egress_key = str(sealed["egress_key"])
    state = _mover_live_state(queue, egress_key)
    if state == "unknown":
        return {"answer": {"ok": False, "egress_action_key": egress_key,
                           "refusal": "unknown-retain: egress-row-unreadable"}}
    if state in (pool_mod.READY, pool_mod.CLAIMED):
        return deferred(OWN_EGRESS_IN_FLIGHT, egress_key, state)
    # `Pool._file_move` files a node's receipt under the node's OWN key, so an
    # egress receipt is read by the egress key and names it; the mover it
    # retired is bound by that key, which hashes a command naming it. The
    # namespace must still be this batch's. Read whatever the row's state:
    # the tool files its receipt before it exits, and `finish` takes the
    # claim away before it writes the terminal, so a finished egress is
    # briefly in NO state directory with its complete receipt already filed.
    filed: dict[str, object] | None = None
    try:
        candidate = queue.move_record(egress_key)
    except Exception:
        candidate = None
    if (isinstance(candidate, Mapping)
            and str(candidate.get("action_key") or "") == egress_key
            and str(candidate.get("consumer_action_key") or "") == consumer):
        filed = dict(candidate)
        filed["schema"] = pool_mod.POOL_EGRESS_SCHEMA_V1
        filed["action_key"] = target_mover
        filed["egress_action_key"] = egress_key
    if (filed is not None and filed.get("complete") is True
            and not filed.get("errors")):
        return {"receipt": filed}
    # Nothing has completed this egress: publish it, or publish it AGAIN --
    # `recompute` is what makes a republished movement key run instead of
    # being answered from its old receipt.
    launch = _producer_launch_context(queue, producer)
    if not launch.get("ok"):
        return {"answer": {"ok": False, "step": launch.get("step"),
                           "egress_action_key": egress_key,
                           "refusal": "unknown-retain: egress-launch: "
                                      f"{launch.get('refusal')}"}}
    retry_policy = sealed["retry_policy"]
    assert isinstance(retry_policy, Mapping)
    host = str(sealed["host"])
    try:
        sealed["cas"].publish_action_request(sealed["action"])
        queue.publish(
            action_key=egress_key,
            cas_root=str(sealed["cas"].root),
            worker_script=str(launch["worker_script"]),
            tags=[host] if host else (),
            priority=int(launch["priority"]),
            max_attempts=int(retry_policy["max_attempts"]),
            retry_safe=bool(retry_policy["retry_safe"]),
            **dict(launch["addressing"]),
            # No tier demand, no residency block and no batch reference: the
            # row `pbrun` publishes for a consumer's egress, for its reasons.
            resources={"cpu": 1, "mem_gb": 1},
            recompute=True,
            # A re-driven retirement is an AUTOMATIC republication: it never
            # retires an operator's withdrawal of this egress by writing over
            # it.
            refuse_withdrawn=True,
            # And it never queues a second copy of an egress this queue is
            # already carrying.  The state read at the top of this function
            # asks the same question; on NFS its answer can be stale, and in
            # the 2026-09-21 cycle it was -- each egress was republished three
            # times and ran four. Only the queue's own transition lock makes
            # look-then-publish one decision (#810).
            refuse_if_live=True)
    except pool_mod.ActionAlreadyLiveError as exc:
        # Exactly what the state read above answers when it sees the row: this
        # egress is in flight, so the caller polls rather than publishes.
        return deferred(OWN_EGRESS_IN_FLIGHT, egress_key, exc.state)
    except pool_mod.WithdrawnActionError as exc:
        # No `deferred_own`: a caller polling a deferral must stop here.
        return {"answer": {"ok": False, "step": "egress-publish",
                           "egress_action_key": egress_key,
                           "refusal": f"egress-withdrawn: {exc}"}}
    except Exception as exc:
        return {"answer": {"ok": False, "step": "egress-publish",
                           "egress_action_key": egress_key,
                           "refusal": f"unknown-retain: egress-publish: {exc}"}}
    if filed is not None:
        # An attempt ended without completing, and its receipt is the cause:
        # a live pin, a handoff, an error. Answered as the in-process egress
        # answers it; the attempt just published is the next re-drive's.
        return incomplete(filed, egress_key)
    return deferred(OWN_EGRESS_IN_FLIGHT, egress_key, "published")


def _record_staged_paths(queue, checked_instance: Mapping[str, object],
                         batches: dict, batch_id: str,
                         entry: Mapping[str, object], source: str,
                         mover_key: str, staged_paths: list[str]) -> None:
    """File the staged paths BEFORE a tier-host egress can drop the fragment.

    Caller holds the output-prefix ownership lock. The in-process egress reads
    the fragment and deletes in one call, so the paths it vouched are in hand
    when the retirement is filed. A tier-host egress ends between two calls,
    and the call that files the retirement finds the fragment already gone:
    without this, a retired batch would record NO staged paths, and a live pin
    over one of them could never be attributed to it again. `retired` is not
    touched -- the same field, written early, on a copy that is still live.
    """

    paths = sorted(set(staged_paths))
    updated = dict(entry)
    if source == "materialization":
        items = _materializations(entry)
        for index, item in enumerate(items):
            if str(item.get("mover_key")) == str(mover_key):
                item = dict(item)
                item["staged_paths"] = paths
                items[index] = item
                break
        else:
            raise ProducedOutputError("unknown-retain: materializations")
        updated["materializations"] = items
    else:
        updated["staged_paths"] = paths
    batches[batch_id] = updated
    _write_commitments(_commitments_path(queue.root, checked_instance),
                       {"batches": batches})


def _select_retirement_locked(queue, checked_instance: Mapping[str, object],
                              checked_template: Mapping[str, object],
                              batches: Mapping[str, object],
                              batch_id: str) -> dict[str, object]:
    """Phase one of a retirement: validate and select, under the ownership lock.

    ``batches`` is the commitments document the caller read under the
    instance's output-prefix ownership lock, which it still holds.  Returns
    ``{"answer": ...}`` when the retirement ends here (a refusal, an
    origin-only batch, a duplicate), else ``{"selection": ...}``: the copy to
    retire and everything phase three revalidates against.

    Every check comes BEFORE any destructive call: the immutable record
    validates (schema/binding/entries/manifest), and the mutable
    commitments entry must agree with it on mover, tier, and the
    canonically derived namespace. A changed mover/tier in commitments
    never selects the egress target. Bad or foreign metadata refuses before
    fragments are read, files deleted, or tier ownership released.
    """

    from prismabuild import pool as pool_mod

    entry = batches.get(batch_id)
    if not isinstance(entry, Mapping):
        return {"answer": {"ok": False, "refusal": "unknown-batch"}}
    try:
        filed, _sealed = _load_batch_record(
            queue.root, checked_instance, checked_template,
            entry, batch_id)
    except ProducedOutputError as exc:
        return {"answer": {"ok": False, "refusal": str(exc)}}
    if entry.get("origin_only") is True and filed.get("origin_only") is True:
        # Nothing was staged, so there is no copy to evict and no window
        # to return (#912). The origin is `reclaim_origin`'s.
        return {"answer": {"ok": True, "batch_id": batch_id,
                           "origin_only": True, "staged": False}}
    mover = str(filed.get("mover_key") or "")
    tier = str(filed.get("tier") or "")
    try:
        canonical_ns = batch_namespace(
            checked_instance, batch_id,
            str(filed.get("manifest_digest") or ""))
    except ProducedOutputError as exc:
        return {"answer": {"ok": False, "refusal": f"unknown-retain: {exc}"}}
    if (len(mover) != 64
            or str(entry.get("mover_key") or "") != mover
            or str(entry.get("tier") or "") != tier
            or tier not in checked_template["permitted_tiers"]
            or str(entry.get("batch_namespace") or "") != canonical_ns
            or str(filed.get("batch_namespace") or "") != canonical_ns):
        return {"answer": {"ok": False,
                           "refusal": "unknown-retain: batch-target-mismatch"}}
    # WHICH copy is being retired: the batch's first materialization, or
    # a restaged successor that now owns the material under the same
    # canonical namespace. `entry["retired"]` answers only for the first,
    # so the active materialization is what selects the egress target --
    # otherwise a restaged batch reports a duplicate retirement and
    # orphans a live stage copy plus its window credit.
    try:
        active = _active_materialization(entry)
    except ProducedOutputError as exc:
        return {"answer": {"ok": False, "refusal": str(exc)}}
    if active.get("retired"):
        return {"answer": {"ok": True, "batch_id": batch_id,
                           "duplicate": True}}
    target_mover = str(active.get("mover_key") or "")
    if len(target_mover) != 64 or str(active.get("tier") or "") != tier:
        return {"answer": {"ok": False,
                           "refusal": "unknown-retain: batch-target-mismatch"}}
    consumer = canonical_ns
    # Capture the staged paths the egress is about to vouch while the
    # fragments still exist; after the delete only this record names
    # them for later live-path attribution. A fragment read that
    # fails is unknown attribution, never an empty set: retirement
    # refuses rather than recording known-empty paths.
    staged_paths: list[str] = []
    try:
        from prismabuild import residency_map as map_mod

        out_base = output_fragment_root(queue.root / pool_mod.RESIDENCY)
        fragments = map_mod.read_fragments(out_base, consumer)
        if fragments:
            composed = map_mod.compose(fragments)
            entries = composed.get("entries")
            if not isinstance(entries, Mapping):
                return {"answer": {"ok": False,
                                   "refusal": "unknown-retain: fragment-entries"}}
            for record in entries.values():
                if isinstance(record, Mapping) and record.get("stage_path"):
                    staged_paths.append(os.path.normpath(
                        str(record["stage_path"])))
    except ProducedOutputError as exc:
        return {"answer": {"ok": False, "refusal": f"unknown-retain: {exc}"}}
    except (OSError, ValueError) as exc:
        return {"answer": {"ok": False, "refusal": f"unknown-retain: {exc}"}}
    # Paths an earlier call of this retirement filed for this same copy:
    # a tier-host egress drops the fragment between two calls, so the
    # call that files the retirement may find nothing left to read.
    recorded = active.get("staged_paths")
    recorded_paths = {os.path.normpath(path) for path in recorded
                      if isinstance(path, str) and path} if isinstance(
                          recorded, list) else set()
    return {"selection": {
        "entry": entry, "tier": tier, "canonical_ns": canonical_ns,
        "target_mover": target_mover, "consumer": consumer,
        "manifest": str(filed.get("manifest_digest") or ""),
        "source": str(active.get("source") or ""),
        "generation": int(active.get("generation") or 0),
        "recorded_paths": recorded_paths,
        "staged_paths": sorted(set(staged_paths) | recorded_paths)}}


def _file_retirement_locked(queue, checked_instance: Mapping[str, object],
                            checked_template: Mapping[str, object],
                            batches: dict, batch_id: str,
                            selection: Mapping[str, object],
                            receipt: Mapping[str, object]
                            ) -> dict[str, object]:
    """Phase three of a retirement: revalidate and mark, under the ownership lock.

    ``batches`` is the commitments document the caller read again, under the
    lock it took back after the egress; the caller writes it once for every
    batch it marks.  The EXACT selection is revalidated before anything is
    marked: the record still loads, the batch still resolves to the same
    manifest, and the active materialization is still the generation whose
    copy this receipt just deleted.  A selection that moved underneath the
    egress is unknown state and retains.  The answer carries ``marked``:
    whether ``batches`` changed.
    """

    entry = batches.get(batch_id)
    if not isinstance(entry, Mapping):
        return {"ok": False, "refusal": "unknown-retain: unknown-batch"}
    try:
        filed, _sealed = _load_batch_record(
            queue.root, checked_instance, checked_template, entry,
            batch_id)
        active = _active_materialization(entry)
    except ProducedOutputError as exc:
        return {"ok": False, "refusal": str(exc)}
    target_mover = str(selection["target_mover"])
    generation = int(selection["generation"])  # type: ignore[arg-type]
    if (str(filed.get("manifest_digest") or "") != selection["manifest"]
            or str(active.get("mover_key") or "") != target_mover
            or str(active.get("source") or "") != selection["source"]
            or int(active.get("generation") or 0) != generation):
        return {"ok": False,
                "refusal": "unknown-retain: materialization-changed"}
    if active.get("retired"):
        return {"ok": True, "batch_id": batch_id, "duplicate": True,
                "receipt": receipt, "mover_key": target_mover,
                "generation": generation, "marked": False}
    staged_paths = list(selection["staged_paths"])  # type: ignore[arg-type]
    try:
        if selection["source"] == "materialization":
            marked = _mark_materialization_retired_locked(
                batches, batch_id, mover_key=target_mover, receipt=receipt,
                canonical_ns=str(selection["canonical_ns"]),
                staged_paths=staged_paths)
        else:
            marked = _mark_batch_retired_locked(
                queue, checked_instance, checked_template, batches, batch_id,
                receipt, staged_paths)
    except ProducedOutputError as exc:
        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    return {"ok": True, "batch_id": batch_id, "receipt": receipt,
            "mover_key": target_mover, "generation": generation,
            "staged_paths": sorted(set(staged_paths)), "marked": marked}


def _answer(result: Mapping[str, object]) -> dict[str, object]:
    """A phase-three answer as `retire_batch` returns it."""

    return {key: value for key, value in result.items() if key != "marked"}


def retire_batch(queue, instance: Mapping[str, object],
                 template: Mapping[str, object], batch_id: str, *,
                 stage_root: str, residency_root: str | Path,
                 cas_root=None) -> dict[str, object]:
    """Evict one batch's staged files, then retire its stage window.

    WHERE the egress runs follows the tier record. On the box that owns the
    stage it runs in this process, as it always has. Anywhere else it runs as
    a queue action placed on that box (`_tier_host_egress`), because only the
    tier host mounts the stage read-write (#801): the first call publishes
    the action and answers `egress-incomplete` with the receipt's
    `deferred_own` naming `own-egress-in-flight`, and the caller re-drives
    this call -- exactly as it already does for `own-copy-in-flight` -- until
    the action has ended and the retirement files. `cas_root` is where that
    action's request is filed; it defaults to the producer row's own root.

    Provenance is validated BEFORE anything destructive: the immutable
    batch record loads through the single loader (schema/binding/
    entries/manifest), the mutable commitments entry must agree with it
    on mover, tier, and the canonically derived namespace, and the
    egress target (mover/namespace) comes from that validated result --
    never from unchecked entry fields. WHICH copy is retired is the batch's
    ACTIVE materialization: its first publication, or the successor a
    `ensure_batch_materialized` restage put on the tier.

    Three phases, and the ownership lock is held for only two of them:
    select and capture under the output-prefix ownership lock
    (`_select_retirement_locked`); run the existing egress with NO lock of
    this lane held; reacquire, revalidate the exact same
    manifest/mover/generation, and file `retired` for a complete error-free
    receipt naming that mover and namespace (`_file_retirement_locked`). The
    egress takes the mover transition lock and then the stage root's
    ownership lock, so running it underneath this instance's ownership lock
    would nest two locks of one family with a blocking transition wait
    between them; nothing needs that, because the materialization stays
    unretired for the whole window and therefore keeps refusing both a
    second writer over its origins and any successor materialization.
    `retire_staged_batches` runs the same three phases for many batches of
    one instance, with one read and one write of the document for all of
    them.

    Durable-origin quota is NOT freed here -- origin files still exist; see
    `reclaim_origin`. Charge (durable) and window (tier) accounting
    stay distinct at every step, and a failed or partial egress files
    nothing and leaves the old materialization holding its own credit.
    """

    from prismabuild import core as core_mod

    try:
        checked_template, checked_instance = _require_bound_contract(
            template, instance)
    except ProducedOutputError as exc:
        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    _name(batch_id, where="batch_id")
    with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
        try:
            commitments = _read_commitments(
                _commitments_path(queue.root, checked_instance))
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        chosen = _select_retirement_locked(
            queue, checked_instance, checked_template, batches, batch_id)
        if "answer" in chosen:
            return dict(chosen["answer"])  # type: ignore[arg-type]
        selection = chosen["selection"]
        assert isinstance(selection, dict)
        entry = selection["entry"]
        tier = str(selection["tier"])
        target_mover = str(selection["target_mover"])
        consumer = str(selection["consumer"])
        staged_paths = list(selection["staged_paths"])
        recorded_paths = set(selection["recorded_paths"])
        try:
            tier_record = _announced_tier_record(queue, tier)
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        in_process = _egress_runs_in_process(tier_record)
        stage_release = None
        if in_process:
            # `stage_release` is a FLEET TOOL, not part of this package: the
            # published generation carries it under `tools/`, and an owner
            # that loads `prismabuild` from `<generation>/src` -- which is
            # how every production owner loads it -- cannot import it. The
            # tier host's own action can, so an owner without the tool takes
            # that route even on the tier host. Never imported on the
            # tier-host route, which is the route a GPU host always takes.
            try:
                import stage_release  # type: ignore[no-redef]
            except ImportError:
                in_process = False
        if not in_process and tier_record is None:
            return {"ok": False,
                    "refusal": f"unknown-retain: tier-not-announced: {tier}"}
        if not in_process and set(staged_paths) - recorded_paths:
            try:
                _record_staged_paths(
                    queue, checked_instance, batches, batch_id, entry,
                    str(selection["source"]), target_mover, staged_paths)
            except (ProducedOutputError, OSError) as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    # --- The ownership lock is RELEASED here, before the egress runs. ---
    # `stage_release.evict` takes the mover transition lock (blocking) and
    # then the STAGE ROOT's ownership lock, and its containment reclamation
    # reaches further locks below that. Holding this instance's output-prefix
    # ownership lock across all of it nests two locks of the same family and
    # parks a blocking transition wait underneath an ownership lock -- the
    # shape the egress cross-root cycle came from. Nothing this lane needs is
    # protected by holding it here: the materialization is still unretired
    # for the whole window, so `_live_path_owner` keeps refusing a second
    # writer over these origins and `ensure_batch_materialized` keeps
    # refusing a successor, and a failed or partial egress files nothing and
    # leaves the old materialization holding its own credit.
    if in_process:
        assert stage_release is not None
        receipt = stage_release.evict(
            queue, target_mover, consumer_action_key=consumer,
            stage_root=str(stage_root), residency_root=str(residency_root))
        if not receipt.get("complete"):
            return {"ok": False, "refusal": "egress-incomplete",
                    "receipt": receipt}
    else:
        # The same egress, on the box that can delete: see `_tier_host_egress`.
        assert tier_record is not None
        producer = str(checked_instance.get("owner_action_key")
                       or os.environ.get(core_mod.ACTION_KEY_ENV) or "")
        if cas_root is None:
            launch = _producer_launch_context(queue, producer)
            cas_root = (str(launch["cas_root"]) if launch.get("ok")
                        else str(Path(queue.root).parent / "cas"))
        routed = _tier_host_egress(
            queue, record=tier_record, producer=producer, cas_root=cas_root,
            batch_id=batch_id, generation=int(selection["generation"]),
            target_mover=target_mover, consumer=consumer,
            stage_root=str(stage_root), residency_root=str(residency_root))
        if "answer" in routed:
            answer = routed["answer"]
            assert isinstance(answer, dict)
            return answer
        receipt = routed["receipt"]
        assert isinstance(receipt, dict)
    with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
        path = _commitments_path(queue.root, checked_instance)
        try:
            commitments = _read_commitments(path)
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        result = _file_retirement_locked(
            queue, checked_instance, checked_template, batches, batch_id,
            selection, receipt)
        if result.get("marked"):
            try:
                _write_commitments(path, {
                    "batches": batches,
                    "admission": commitments.get("admission")})
            except ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    return _answer(result)


#: The unit `retire_staged_batches` asks a budget for: one batch's egress.
BATCH_RETIREMENT_UNIT = "staged-batch-retirement"


def retire_staged_batches(queue, instance: Mapping[str, object],
                          template: Mapping[str, object],
                          batch_ids: Sequence[str], *, stage_root: str,
                          residency_root: str | Path,
                          budget=None) -> dict[str, dict[str, object]]:
    """Retire many staged batches of one instance, with one commitments write (#1072).

    `retire_batch`, run for each batch, reads the instance's commitments
    document four times and writes it once, fsync included.  R13's document
    holds 436 batches in 9.2 MB, so a backlog of K batches of one instance
    cost K whole-document writes: the live tier loop's first cycle after
    the 09-24 publish spent 129.7 s retiring 137 batches of fourteen dead
    instances.  Here the three phases of `retire_batch` run for every batch
    at once: phase one selects all of them under one ownership lock and one
    read, phase two runs each egress with no lock of this lane held, and
    phase three revalidates each batch under one lock and one read and
    writes the document once for every batch it marked.

    Only the in-process egress: the caller is the tier loop, on the tier
    host.  A batch whose egress would take the tier-host route is answered
    ``not-on-the-tier-host`` and nothing of it is touched.

    **What a crash can leave.**  The two states that must never exist are a
    batch recorded retired while its copy is still on the stage, and a copy
    gone with nothing that will ever record it.

    * Recorded but not retired cannot happen.  Phase three marks a batch
      only for a complete, error-free egress receipt of this batch's exact
      selection (the same `_file_retirement_locked` checks as
      `retire_batch`), and the one write that records it is made after
      every such receipt is in hand.  A batch whose egress was incomplete,
      was deferred, or whose selection moved is left out of the write, not
      the transaction: the others are still recorded.
    * Retired but unrecorded is the state `retire_batch` already has for
      one batch, between its egress and its write, and it recovers: the
      unretired materialization keeps refusing a second writer and any
      successor, and the next tick finds the batch again (its mover ended,
      no tokens held) and runs the egress again, which finds nothing to
      delete and returns a complete receipt, and files it.  Batching widens
      that window to every batch egressed before the one write; it opens no
      new state.
    * The write itself is one atomic rename of the whole document, so a
      crash during it leaves either every mark or none.

    ``budget``, when given, is asked before each egress whether another
    fits this cycle (`budget.start`) and told when it ends (`budget.done`),
    which is where the tier loop re-announces its tiers between units.  A
    batch the budget defers is answered ``deferred`` and waits, untouched,
    for a later cycle.

    Returns ``{batch_id: answer}``, each answer shaped as `retire_batch`'s.
    """

    try:
        checked_template, checked_instance = _require_bound_contract(
            template, instance)
    except ProducedOutputError as exc:
        refusal = {"ok": False, "refusal": f"unknown-retain: {exc}"}
        return {batch_id: dict(refusal) for batch_id in batch_ids}
    for batch_id in batch_ids:
        _name(batch_id, where="batch_id")
    answers: dict[str, dict[str, object]] = {}
    selections: dict[str, dict[str, object]] = {}
    lock_name = str(checked_instance["output_prefix"])
    path = _commitments_path(queue.root, checked_instance)
    with queue.stage_ownership_lock(lock_name):
        try:
            commitments = _read_commitments(path)
        except ProducedOutputError as exc:
            refusal = {"ok": False, "refusal": f"unknown-retain: {exc}"}
            return {batch_id: dict(refusal) for batch_id in batch_ids}
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        tier_records: dict[str, object] = {}
        for batch_id in batch_ids:
            chosen = _select_retirement_locked(
                queue, checked_instance, checked_template, batches, batch_id)
            if "answer" in chosen:
                answers[batch_id] = dict(chosen["answer"])  # type: ignore[arg-type]
                continue
            selection = chosen["selection"]
            assert isinstance(selection, dict)
            tier = str(selection["tier"])
            if tier not in tier_records:
                try:
                    tier_records[tier] = _announced_tier_record(queue, tier)
                except Exception as exc:
                    tier_records[tier] = exc
            tier_record = tier_records[tier]
            if isinstance(tier_record, Exception):
                answers[batch_id] = {"ok": False,
                                     "refusal": f"unknown-retain: {tier_record}"}
                continue
            if not _egress_runs_in_process(tier_record):
                answers[batch_id] = {"ok": False,
                                     "refusal": "not-on-the-tier-host"}
                continue
            selections[batch_id] = selection
    if not selections:
        return answers
    try:
        import stage_release  # type: ignore[import-not-found]
    except ImportError:
        for batch_id in selections:
            answers[batch_id] = {"ok": False,
                                 "refusal": "stage-release-unimportable"}
        return answers
    # --- No lock of this lane is held while the egresses run; see
    # `retire_batch` for why nothing needs it.
    receipts: dict[str, dict[str, object]] = {}
    # An egress that raises ends the egresses, not the retirement: the
    # receipts already in hand are still filed below, as `retire_batch` would
    # have filed each of them before the next one ran, and the exception is
    # raised once they are.  An interrupt is not caught: it leaves those
    # batches egressed and unrecorded, the recoverable state described above.
    raised: Exception | None = None
    for batch_id, selection in selections.items():
        if raised is not None or (budget is not None and not budget.start(
                BATCH_RETIREMENT_UNIT, batch_id)):
            answers[batch_id] = {"ok": False, "refusal": "deferred",
                                 "deferred": True}
            continue
        try:
            receipt = stage_release.evict(
                queue, str(selection["target_mover"]),
                consumer_action_key=str(selection["consumer"]),
                stage_root=str(stage_root), residency_root=str(residency_root))
        except Exception as exc:  # noqa: BLE001 -- re-raised below
            raised = exc
            answers[batch_id] = {"ok": False, "refusal": "egress-raised"}
            continue
        finally:
            if budget is not None:
                budget.done(BATCH_RETIREMENT_UNIT)
        if not receipt.get("complete"):
            answers[batch_id] = {"ok": False, "refusal": "egress-incomplete",
                                 "receipt": receipt}
            continue
        receipts[batch_id] = receipt
    if not receipts:
        if raised is not None:
            raise raised
        return answers
    with queue.stage_ownership_lock(lock_name):
        try:
            commitments = _read_commitments(path)
        except ProducedOutputError as exc:
            for batch_id in receipts:
                answers[batch_id] = {"ok": False,
                                     "refusal": f"unknown-retain: {exc}"}
            if raised is not None:
                raise raised
            return answers
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        results = {batch_id: _file_retirement_locked(
            queue, checked_instance, checked_template, batches, batch_id,
            selections[batch_id], receipt)
            for batch_id, receipt in receipts.items()}
        if any(result.get("marked") for result in results.values()):
            try:
                _write_commitments(path, {
                    "batches": batches,
                    "admission": commitments.get("admission")})
            except ProducedOutputError as exc:
                for batch_id in receipts:
                    answers[batch_id] = {"ok": False,
                                         "refusal": f"unknown-retain: {exc}"}
                results = {}
    for batch_id, result in results.items():
        answers[batch_id] = _answer(result)
    if raised is not None:
        raise raised
    return answers


def reclaim_origin(queue, instance: Mapping[str, object],
                   template: Mapping[str, object], *,
                   batch_id: str) -> dict[str, object]:
    """Free durable-origin quota after proving every origin path absent.

    Stage retirement frees the tier window only. Each committed batch keeps
    charging its payload/checkpoint/temp classes until this call stats
    every sealed entry path and finds all of them absent (producer-side
    disposal: this call never unlinks an origin file). A present file
    refuses `origin-present-retain` keeping the charge; an unstatable
    path retains unknown. Exactly-once: already-reclaimed returns
    `{"ok": True, "reclaimed": False}`.

    The one place PB itself deletes origin files is the retirement of a
    ``consumed`` origin-only batch (`origin_retirement_tick`, #914), which
    sets the same ``origin_reclaimed`` flag when it is done.
    """

    try:
        checked_template, checked_instance = _require_bound_contract(
            template, instance)
    except ProducedOutputError as exc:
        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    _name(batch_id, where="batch_id")
    with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
        try:
            commitments = _read_commitments(
                _commitments_path(queue.root, checked_instance))
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        entry = batches.get(batch_id)
        if not isinstance(entry, Mapping):
            return {"ok": False, "refusal": "unknown-batch"}
        if entry.get("origin_reclaimed"):
            return {"ok": True, "batch_id": batch_id, "reclaimed": False}
        # Origin paths come from the single batch-record loader (exact
        # sealed descriptors, binding/entries/total re-validated), never
        # from caller arguments: a damaged record retains instead of
        # proving a vacuous absence.
        try:
            _, sealed = _load_batch_record(
                queue.root, checked_instance, checked_template,
                entry, batch_id)
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": str(exc)}
        for desc_path in sorted(str(desc["path"]) for desc in sealed):
            if not desc_path:
                return {"ok": False, "refusal": "unknown-retain: bad-entry"}
            try:
                os.lstat(desc_path)
            except FileNotFoundError:
                continue
            except OSError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            return {"ok": False, "refusal": "origin-present-retain",
                    "path": desc_path}
        entry = dict(entry)
        entry["origin_reclaimed"] = True
        batches[batch_id] = entry
        try:
            _write_commitments(_commitments_path(queue.root, checked_instance),
                               {"batches": batches})
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    return {"ok": True, "batch_id": batch_id, "reclaimed": True}


# --------------------------------------------------------------------------
# Consumed origin batches: consumer declarations and retirement (#914)
# --------------------------------------------------------------------------
#
# A ``consumed`` origin-only batch is a handoff: PB deletes its origin files
# and frees its durable charge once the actions that read it have succeeded.
# Nothing here copies, stages or moves a token. It reads the queue's own
# records to learn how each action ended, and it deletes only files whose
# identity is the one the batch's commit recorded.

#: The events `origin_retirement_tick` returns, one per batch.
ORIGIN_RETIRED_EVENT = "output-origin-retired"
ORIGIN_RETIREMENT_STALLED_EVENT = "output-origin-retirement-stalled"
ORIGIN_RETIREMENT_REFUSED_EVENT = "output-origin-retirement-refused"
#: The events it returns for a write-only prewrite whose attempt ended
#: before committing (#949): the reservation was dropped, or its files
#: belong to no batch and wait for an operator.
ORIGIN_PREWRITE_RECLAIMED_EVENT = "output-prewrite-reclaimed"
ORIGIN_PREWRITE_ORPHANED_EVENT = "output-prewrite-orphaned"
#: The one remedy an orphaned prewrite's event and listing name (#949,
#: #1053). PB never deletes a file whose identity no commit recorded, so an
#: operator does, unless a successor is about to commit the same paths.
ORPHANED_PREWRITE_REMEDY = (
    "these files belong to no committed batch: check and remove them, and "
    "the next tier cycle drops the prewrite's reservation. A successor that "
    "commits the same paths drops it too, and keeps its files")
#: The event for a dead producer's staged batch whose stage retirement the
#: tick filed (#1053). Its origin files are kept: ``origin_kept`` is always
#: true, and ``superseded`` names the paths another attempt now claims.
DEAD_PRODUCER_BATCH_RETIRED_EVENT = "output-dead-producer-batch-retired"

#: Attempt states after which an attempt can never commit again: its owner
#: gate (`_require_live_owner`) refuses a claim it no longer holds.
_ENDED_ATTEMPT_STATES = frozenset({"dead", "succeeded"})

#: Consumer states that hold a consumed batch and are reported as a stall:
#: the consumer ended without succeeding, or was declared and never
#: published, or its records could not be read.
_STALLED_CONSUMER_STATES = frozenset(
    {"failed", "withdrawn", "unpublished", "unknown"})

#: Consumer states that no longer hold a consumed batch (#926): it
#: succeeded, an operator released its declaration, or a supersession
#: names a successor declared against the same batch, which holds it in
#: its place.
_RESOLVED_CONSUMER_STATES = frozenset({"succeeded", "released", "superseded"})

#: The states that mean a consumer has ended without succeeding, and so can
#: be superseded (`action_edges.file_supersession`) or released.
_TERMINAL_CONSUMER_STATES = frozenset({"failed", "withdrawn"})

#: Reports the tick could not file on the batch itself (its records were
#: unreadable, or the step raised), keyed by batch coordinates. They are
#: logged once per change for the life of the process instead of on every
#: cycle; a restarted tier loop logs each one again once.
_UNFILED_REPORTS: dict[str, str] = {}


def _consumers_dir(queue_root: str | Path, instance: Mapping[str, object],
                   batch_id: str) -> Path:
    return instance_dir(queue_root, instance) / "consumers" / batch_id


def _released_consumers_dir(queue_root: str | Path,
                            instance: Mapping[str, object],
                            batch_id: str) -> Path:
    return instance_dir(queue_root, instance) / "released-consumers" / batch_id


def _consumer_release(queue_root: str | Path, instance: Mapping[str, object],
                      batch_id: str, key: str,
                      checked_ref: Mapping[str, str]) -> dict[str, object] | None:
    """The operator's release of one declaration, or ``None``; raise if bad."""

    path = _released_consumers_dir(queue_root, instance, batch_id) / f"{key}.json"
    try:
        body = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise ProducedOutputError(
            f"unknown-retain: consumer release {key[:12]} unreadable: {exc}"
        ) from None
    if (not isinstance(body, Mapping)
            or body.get("schema") != ORIGIN_CONSUMER_RELEASE_SCHEMA_V1
            or body.get("consumer_action_key") != key
            or body.get("ref") != dict(checked_ref)):
        raise ProducedOutputError(
            f"unknown-retain: consumer release {key[:12]} does not name this batch")
    return dict(body)


def _release_index_body(consumer: str, checked_ref: Mapping[str, str]) -> bytes:
    # Deterministic, so a second release of the same pair publishes the same
    # bytes and `_publish_immutable` accepts them.
    return json.dumps(
        {"schema": ORIGIN_CONSUMER_RELEASE_INDEX_SCHEMA_V1,
         "consumer_action_key": consumer, "ref": dict(checked_ref)},
        sort_keys=True, separators=(",", ":")).encode() + b"\n"


def _release_index_path(queue, consumer: str,
                        checked_ref: Mapping[str, str]) -> Path:
    digest = hashlib.sha256(json.dumps(
        dict(checked_ref), sort_keys=True,
        separators=(",", ":")).encode()).hexdigest()
    return queue.released_origin_consumers_dir() / f"{consumer}.{digest}.json"


def _index_release(queue, consumer: str, checked_ref: Mapping[str, str]) -> None:
    """File the queue-wide index entry for one release (#954).

    Written before the release record, never after it: a release whose
    record is filed is then always visible to `pool.PoolQueue._claim`'s one
    listing. A crash between the two leaves an entry with no record, which
    `origin_consumer_release` reads as no release.
    """

    from prismabuild import pool as pool_mod

    path = _release_index_path(queue, consumer, checked_ref)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        pool_mod._publish_immutable(
            path, _release_index_body(consumer, checked_ref),
            where="produced-output origin consumer release index")
    except pool_mod.PoolContractError as exc:
        raise ProducedOutputError(
            f"origin-consumer-release-conflict: {exc}") from None


def origin_consumer_release(queue, key: str) -> dict[str, object] | None:
    """The release of any batch ``key`` declared, or ``None`` (#954).

    What a claim asks before it runs a row for ``key``, and what the tier
    loop asks before it stages one. An action key seals its data manifest, so
    every row of a key reads the batches its declarations name; one release
    among them is enough. The index entries name the refs; each is confirmed
    by its release record, read through the same `_consumer_release` the
    retirement tick reads. An entry without a record is a release that
    stopped before its record, and does not count.

    Raises `ProducedOutputError` when an entry, its batch's scope or its
    record cannot be read: unknown is neither a release nor its absence.
    """

    from prismabuild import pool as pool_mod

    consumer = _hex64(key, where="consumer action key")
    directory = queue.released_origin_consumers_dir()
    try:
        names = sorted(name for name in os.listdir(directory)
                       if name.startswith(f"{consumer}.")
                       and name.endswith(".json"))
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ProducedOutputError(
            f"unknown-retain: release index unreadable: {exc}") from None
    for name in names:
        try:
            body = pool_mod._read_json(directory / name)
        except (OSError, pool_mod.PoolContractError) as exc:
            raise ProducedOutputError(
                f"unknown-retain: release index {name} unreadable: {exc}"
            ) from None
        if (not isinstance(body, Mapping)
                or body.get("schema") != ORIGIN_CONSUMER_RELEASE_INDEX_SCHEMA_V1
                or body.get("consumer_action_key") != consumer):
            raise ProducedOutputError(
                f"unknown-retain: release index {name} does not name "
                f"{consumer[:12]}")
        checked_ref = _checked_origin_ref(body.get("ref"))
        if _release_index_path(queue, consumer, checked_ref).name != name:
            raise ProducedOutputError(
                f"unknown-retain: release index {name} names another batch")
        instance, _template = _origin_ref_scope(queue.root, checked_ref)
        release = _consumer_release(queue.root, instance,
                                    checked_ref["batch_id"], consumer,
                                    checked_ref)
        if release is not None:
            return release
    return None


def release_origin_consumer(queue, ref: Mapping[str, object], *,
                            consumer_action_key: str, by: str,
                            reason: str = "",
                            live_runtime: str | Path | None = None,
                            cas_root: str | Path | None = None
                            ) -> dict[str, object]:
    """Release one consumer's declaration on one consumed batch (#926, #945).

    The operator's remedy for a declaration that can never succeed: a
    consumer that failed or was withdrawn and will not be run again under
    that key, or one that was declared and never published (#945). It files
    ``released-consumers/<batch_id>/<key>.json`` beside the declarations; the
    retirement tick then counts that consumer as resolved, and retires the
    batch once every other declared consumer has succeeded, with its usual
    identity checks. Nothing is deleted here. From then on the key cannot
    declare the batch again (`declare_origin_consumer`), so no later
    submission of it can reach the queue as a reader of the batch. A row an
    older ``pbrun`` publishes anyway, one that declared before the release,
    is refused where live code runs (#954): the release first files an entry
    in the queue-wide index (`origin_consumer_release`), the claim fails any
    ready row of the key with ``origin-consumer-released``, and the tier loop
    stages nothing for it.

    The consumer's transition lock is taken first, without waiting: a
    submitter holds it from its declarations through its row
    (`pbrun.submission_window`), so a consumer being submitted is refused
    (``origin-consumer-submitting``). Then, under the batch's output-prefix
    lock, the lock declarations and retirement take: the batch must be
    committed over the ref's digest, ``consumed``, and neither retiring nor
    reclaimed; the consumer must be declared against it; and its latest
    generation must be ``failed``, ``withdrawn`` or ``unpublished``. A
    consumer that is queued, claimed or being moved is refused
    (``origin-consumer-live``), and so is one whose records cannot be read.

    An ``unpublished`` consumer needs two more facts, read from ``cas_root``
    and the queue: the generation that sealed its request is not
    ``live_runtime`` (a submitter on the live generation clears the hold by
    submitting the key again), and no pinned, unpublished deferred release
    names it (`action_edges.pending_release_of`), because that release still
    publishes it. Without ``live_runtime`` and ``cas_root`` it is refused
    (``origin-consumer-unpublished-unknown``). Releasing the same consumer
    again finds its own record.

    Returns ``{"ok": True, "released": bool, "state": ...}``; raises
    `ProducedOutputError` naming the refusal.
    """

    checked_ref = _checked_origin_ref(ref)
    consumer = _hex64(consumer_action_key, where="consumer_action_key")
    instance, template = _origin_ref_scope(queue.root, checked_ref)
    if not template.get("write_only"):
        # A read-back owner's origin batch (#1034) is its own; no other
        # action reads it, so its retirement need not wait for one.
        raise ProducedOutputError(
            "origin-batch-not-write-only: its template reads its batches back")
    with queue._transition_locked(consumer, blocking=False) as owned:
        if not owned:
            raise ProducedOutputError(
                f"origin-consumer-submitting: {consumer[:12]} is being "
                "submitted or moved; release it once that has finished")
        return _release_origin_consumer_locked(
            queue, instance, checked_ref, consumer, by=by, reason=reason,
            live_runtime=live_runtime, cas_root=cas_root)


def _release_origin_consumer_locked(
        queue, instance: Mapping[str, object], checked_ref: Mapping[str, str],
        consumer: str, *, by: str, reason: str,
        live_runtime: str | Path | None,
        cas_root: str | Path | None) -> dict[str, object]:
    """`release_origin_consumer` under the consumer's transition lock."""

    from prismabuild import pool as pool_mod

    batch_id = checked_ref["batch_id"]
    with queue.stage_ownership_lock(str(instance["output_prefix"])):
        commitments = _read_commitments(_commitments_path(queue.root, instance))
        entry = commitments["batches"].get(batch_id)
        if not isinstance(entry, Mapping):
            raise ProducedOutputError(f"origin-batch-uncommitted: {batch_id}")
        if (entry.get("origin_only") is not True
                or entry.get("manifest_digest") != checked_ref["manifest_digest"]):
            raise ProducedOutputError(
                "origin-batch-mismatch: the committed batch is not origin-only "
                "over this manifest digest")
        if _entry_lifetime(entry) != ORIGIN_LIFETIME_CONSUMED:
            raise ProducedOutputError(
                f"origin-batch-retained: {batch_id} is never retired, so it has "
                "no declarations to release")
        if entry.get("origin_reclaimed"):
            raise ProducedOutputError(f"origin-batch-reclaimed: {batch_id}")
        if entry.get("retiring"):
            raise ProducedOutputError(f"origin-batch-retiring: {batch_id}")
        if consumer not in _declared_consumers(
                queue.root, instance, batch_id, checked_ref):
            raise ProducedOutputError(
                f"origin-consumer-undeclared: {consumer[:12]} did not declare "
                f"{batch_id}")
        if _consumer_release(queue.root, instance, batch_id, consumer,
                             checked_ref) is not None:
            # A record filed before the index existed (#945's releases) gets
            # its entry here, so running the release again makes the claim
            # see it (#954).
            _index_release(queue, consumer, checked_ref)
            return {"ok": True, "released": False, "state": "released"}
        state = _consumer_state(queue, consumer)
        if state == "live":
            raise ProducedOutputError(
                f"origin-consumer-live: {consumer[:12]} is queued, claimed or "
                "being moved; release it once it has failed or been withdrawn")
        sealed: dict[str, str] = {}
        if state == "unpublished":
            if live_runtime is None or cas_root is None:
                raise ProducedOutputError(
                    f"origin-consumer-unpublished-unknown: {consumer[:12]} is "
                    "unpublished, and without the live runtime generation and "
                    "the CAS nothing shows that no submitter can publish it")
            sealed = _unpublished_release_evidence(
                queue, consumer, live_runtime=live_runtime, cas_root=cas_root)
        elif state not in _TERMINAL_CONSUMER_STATES:
            raise ProducedOutputError(
                f"origin-consumer-not-terminal: {consumer[:12]} is {state}; "
                "only a failed, withdrawn or unpublished consumer can be "
                "released")
        directory = _released_consumers_dir(queue.root, instance, batch_id)
        directory.mkdir(parents=True, exist_ok=True)
        body = {"schema": ORIGIN_CONSUMER_RELEASE_SCHEMA_V1,
                "consumer_action_key": consumer, "ref": dict(checked_ref),
                "state": state, "by": str(by), "reason": str(reason),
                "released_unix": time.time(), **sealed}
        # The index first (#954). The record is what makes the release real
        # to the retirement tick; a record the claim's listing could not see
        # would let a row for this key run while the tick deletes its batch.
        _index_release(queue, consumer, checked_ref)
        try:
            pool_mod._publish_immutable(
                directory / f"{consumer}.json",
                json.dumps(body, sort_keys=True,
                           separators=(",", ":")).encode() + b"\n",
                where="produced-output origin consumer release")
        except pool_mod.PoolContractError as exc:
            raise ProducedOutputError(
                f"origin-consumer-release-conflict: {exc}") from None
    return {"ok": True, "released": True, "state": state}


def _unpublished_release_evidence(queue, consumer: str, *,
                                  live_runtime: str | Path,
                                  cas_root: str | Path) -> dict[str, str]:
    """Why an unpublished consumer's key cannot reach the queue (#945).

    A declaration with no row is a submission that stopped between its
    declaration and its row, or one still between them. The caller holds the
    consumer's transition lock, which every submitter holds across that
    window, so none is between them now. What could still publish the key is
    a submitter on the live generation, which clears the hold by submitting
    it again, or a pinned deferred release that resumes. This refuses both,
    and anything it cannot read, and returns what the release records: the
    wrapper that sealed the key.
    """

    from . import action_edges

    live_wrapper = str(Path(live_runtime) / "tools")
    try:
        wrapper = action_edges.request_wrapper(
            cas_root, consumer, where=f"sealed request {consumer[:12]}")
    except Exception as exc:                                  # noqa: BLE001
        raise ProducedOutputError(
            f"origin-consumer-unpublished-unknown: {consumer[:12]} has no "
            f"readable sealed request: {exc}") from None
    if wrapper == live_wrapper:
        raise ProducedOutputError(
            f"origin-consumer-unpublished-live: {consumer[:12]} was sealed by "
            "the live runtime generation, whose pbrun can still publish it; "
            "submit the same key again to clear the hold")
    try:
        pending = action_edges.pending_release_of(queue.root, consumer)
    except (OSError, action_edges.ActionEdgeError) as exc:
        raise ProducedOutputError(
            f"origin-consumer-unpublished-unknown: deferred releases "
            f"unreadable: {exc}") from None
    if pending is not None:
        raise ProducedOutputError(
            f"origin-consumer-release-pending: deferred release "
            f"{pending[:12]} pinned {consumer[:12]} and will publish it")
    return {"sealed_wrapper": wrapper}


def _resolved_consumers(queue, instance: Mapping[str, object], batch_id: str,
                        checked_ref: Mapping[str, str],
                        declared: Sequence[str]) -> list[dict[str, object]]:
    """Each declared consumer's state for retirement (#914, #926).

    A consumer's own state (`_consumer_state`) answers first while it is
    queued, running or has succeeded. A consumer that ended without
    succeeding is ``released`` when an operator released its declaration,
    and ``superseded`` when its supersessions (`pbrun --supersedes`, #913)
    lead to a key that is also declared against this batch: that successor
    holds the batch in its place, and must succeed. A supersession whose
    successor is not declared here yet, or not released yet, is named as
    ``superseded_by`` and changes nothing. Raises `ProducedOutputError` when
    a release record cannot be read.
    """

    from . import action_edges

    declared_set = set(declared)
    consumers: list[dict[str, object]] = []
    for key in declared:
        state = _consumer_state(queue, key)
        item: dict[str, object] = {"action_key": key, "state": state}
        if state == "unpublished":
            # Released only through `release_origin_consumer`'s proof that
            # nothing can publish the key (#945); nothing supersedes it.
            if _consumer_release(queue.root, instance, batch_id, key,
                                 checked_ref) is not None:
                item["state"] = "released"
        elif state in _TERMINAL_CONSUMER_STATES:
            if _consumer_release(queue.root, instance, batch_id, key,
                                 checked_ref) is not None:
                item["state"] = "released"
            else:
                try:
                    successor = action_edges.successor_of(queue.root, key)
                except action_edges.ActionEdgeError as exc:
                    item["supersession"] = f"unreadable: {exc}"
                    successor = None
                if successor is not None:
                    item["superseded_by"] = successor["id"]
                    if (successor["kind"] == action_edges.PRODUCER_KEY
                            and successor["id"] in declared_set):
                        item["state"] = "superseded"
        consumers.append(item)
    return consumers


def declare_origin_consumer(queue, ref: Mapping[str, object], *,
                            consumer_action_key: str) -> dict[str, object]:
    """File one consumer's declaration that it reads one origin batch (#914).

    ``pbrun`` calls this for each batch a consumer's data manifest declares,
    after the consumer's action key is sealed and before its row is
    published, so that no consumer can be queued that the retirement tick
    does not know about. Under the batch's output-prefix lock -- the lock
    retirement takes -- the batch must be committed over the ref's digest and
    neither retiring nor reclaimed. A ``consumed`` batch gets the file
    ``consumers/<batch_id>/<consumer_action_key>.json`` under its instance;
    the same consumer declaring again finds its own file and succeeds, unless
    an operator released its declaration (``origin-consumer-released``,
    #945). A ``retain`` batch is never retired, so nothing is filed for it.

    Returns ``{"ok": True, "declared": bool}``; raises `ProducedOutputError`
    naming the refusal.
    """

    from prismabuild import pool as pool_mod

    checked_ref = _checked_origin_ref(ref)
    consumer = _hex64(consumer_action_key, where="consumer_action_key")
    instance, template = _origin_ref_scope(queue.root, checked_ref)
    if not template.get("write_only"):
        # A read-back owner's origin batch (#1034) is its own; no other
        # action reads it, so its retirement need not wait for one.
        raise ProducedOutputError(
            "origin-batch-not-write-only: its template reads its batches back")
    batch_id = checked_ref["batch_id"]
    with queue.stage_ownership_lock(str(instance["output_prefix"])):
        commitments = _read_commitments(_commitments_path(queue.root, instance))
        entry = commitments["batches"].get(batch_id)
        if not isinstance(entry, Mapping):
            raise ProducedOutputError(f"origin-batch-uncommitted: {batch_id}")
        if (entry.get("origin_only") is not True
                or entry.get("manifest_digest") != checked_ref["manifest_digest"]):
            raise ProducedOutputError(
                "origin-batch-mismatch: the committed batch is not origin-only "
                "over this manifest digest")
        if entry.get("origin_reclaimed"):
            raise ProducedOutputError(f"origin-batch-reclaimed: {batch_id}")
        if entry.get("retiring"):
            raise ProducedOutputError(f"origin-batch-retiring: {batch_id}")
        if _entry_lifetime(entry) != ORIGIN_LIFETIME_CONSUMED:
            return {"ok": True, "declared": False}
        if _consumer_release(queue.root, instance, batch_id, consumer,
                             checked_ref) is not None:
            # A released key no longer holds the batch, so a row for it
            # could outlive the delete (#945).
            raise ProducedOutputError(
                f"origin-consumer-released: {consumer[:12]}'s declaration of "
                f"{batch_id} was released; this key cannot read the batch "
                "again")
        directory = _consumers_dir(queue.root, instance, batch_id)
        directory.mkdir(parents=True, exist_ok=True)
        body = {"schema": ORIGIN_CONSUMER_SCHEMA_V1,
                "consumer_action_key": consumer, "ref": dict(checked_ref)}
        try:
            pool_mod._publish_immutable(
                directory / f"{consumer}.json",
                json.dumps(body, sort_keys=True,
                           separators=(",", ":")).encode() + b"\n",
                where="produced-output origin consumer")
        except pool_mod.PoolContractError as exc:
            raise ProducedOutputError(f"origin-consumer-conflict: {exc}") from None
    return {"ok": True, "declared": True}


def _declared_consumers(queue_root: str | Path, instance: Mapping[str, object],
                        batch_id: str,
                        checked_ref: Mapping[str, str]) -> list[str]:
    """The consumer action keys filed for one batch, sorted.

    A missing directory is no declaration. An unreadable directory or
    declaration, or one that names another batch or another key than its
    file name, raises: an unknown declaration may be the one live reader.
    """

    directory = _consumers_dir(queue_root, instance, batch_id)
    try:
        names = sorted(os.listdir(directory))
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise ProducedOutputError(
            f"unknown-retain: consumer declarations unreadable: {exc}") from None
    keys: list[str] = []
    for name in names:
        if name.startswith("."):
            # A publication's temporary file; the declaration it becomes is
            # read on the next cycle.
            continue
        if not name.endswith(".json"):
            raise ProducedOutputError(
                f"unknown-retain: unexpected consumer declaration {name!r}")
        try:
            body = json.loads((directory / name).read_text())
        except (OSError, ValueError) as exc:
            raise ProducedOutputError(
                f"unknown-retain: consumer declaration {name} unreadable: "
                f"{exc}") from None
        if (not isinstance(body, Mapping)
                or body.get("schema") != ORIGIN_CONSUMER_SCHEMA_V1
                or body.get("consumer_action_key") != name[:-len(".json")]
                or body.get("ref") != dict(checked_ref)):
            raise ProducedOutputError(
                f"unknown-retain: consumer declaration {name} does not name "
                "this batch")
        keys.append(_hex64(body["consumer_action_key"],
                           where="consumer declaration key"))
    return keys


def _key_generation_once(queue, key: str) -> tuple[str, dict[str, object] | None]:
    """One read of where an action key stands: ``(state, record)``.

    A live row wins: ``claimed`` or ``ready``. (A claim's ``intent`` marker is
    not one: it names who claimed the key and outlives the claim.) A claim
    a finisher or a reaper has moved aside -- a tombstone or late-finish file
    beside the row -- is a transition in flight and reads ``moving``. A lease
    with no claim is a widowed lease, not a transition, and is not read.
    Otherwise the terminal record
    with the latest ``published_unix`` among ``done``, ``failed`` and
    ``withdrawn`` is the key's latest generation: a resubmission publishes a
    new generation and leaves the earlier terminal where it was, so the
    newest one is the one that answers for the key. No record at all reads
    ``absent``; an unreadable record, a terminal without a generation stamp,
    or two terminals of one generation read ``unknown``.
    """

    from prismabuild import pool as pool_mod

    try:
        for state in (pool_mod.CLAIMED, pool_mod.READY):
            record = pool_mod._read_json(queue.item_path(state, key))
            if record is not None:
                return (state, record)
        try:
            beside = [path.name for path in pool_mod._glob_visible(
                queue.dir(pool_mod.CLAIMED), f"{key}.*")]
        except FileNotFoundError:
            beside = []
        if any(name.endswith((pool_mod.TOMBSTONE_SUFFIX,
                              pool_mod.LATE_FINISH_SUFFIX))
               for name in beside):
            return ("moving", None)
        terminals: list[tuple[float, str, dict[str, object]]] = []
        for state in (pool_mod.DONE, pool_mod.FAILED, pool_mod.WITHDRAWN):
            record = pool_mod._read_json(queue.item_path(state, key))
            if record is None:
                continue
            stamp = record.get("published_unix")
            if isinstance(stamp, bool) or not isinstance(stamp, (int, float)):
                return ("unknown", None)
            terminals.append((float(stamp), state, record))
    except (OSError, pool_mod.PoolContractError):
        return ("unknown", None)
    if not terminals:
        return ("absent", None)
    terminals.sort(key=lambda item: item[0])
    if len(terminals) > 1 and terminals[-1][0] == terminals[-2][0]:
        return ("unknown", None)
    return (terminals[-1][1], terminals[-1][2])


def _key_generation(queue, key: str) -> tuple[str, dict[str, object] | None]:
    """`_key_generation_once`, taken twice; a disagreement reads ``unknown``.

    The single read visits several directories one after another, and a row
    moving between two of them (a requeue lands in ``ready`` after that
    directory was read) can make one read describe a state the key was never
    in. Two reads that agree have not been split by a move.
    """

    first = _key_generation_once(queue, key)
    second = _key_generation_once(queue, key)
    if first != second:
        return ("unknown", None)
    return first


def _record_nonce(record: Mapping[str, object] | None) -> str:
    control = (record or {}).get("resource_scope")
    if isinstance(control, Mapping):
        nonce = control.get("nonce")
        if isinstance(nonce, str) and nonce:
            return nonce
    return ""


def _consumer_state(queue, key: str) -> str:
    """How a declared consumer stands, for retirement.

    ``succeeded`` (its latest generation is ``done`` as executed or a cache
    hit), ``live`` (queued, claimed or moving), ``failed``, ``withdrawn``,
    ``unpublished`` (declared but no row filed) or ``unknown``.
    """

    from prismabuild import pool as pool_mod

    state, record = _key_generation(queue, key)
    if state in (pool_mod.CLAIMED, pool_mod.READY, "moving"):
        return "live"
    if state == pool_mod.DONE:
        assert record is not None
        if record.get("status") in ("executed", "cache_hit"):
            return "succeeded"
        return "unknown"
    if state == pool_mod.FAILED:
        return "failed"
    if state == pool_mod.WITHDRAWN:
        return "withdrawn"
    if state == "absent":
        return "unpublished"
    return "unknown"


def _producer_attempt_state(queue, instance: Mapping[str, object]) -> str:
    """Whether the attempt that filed this instance is dead, for the sweep.

    ``dead`` when nothing of the owner's is running and its latest
    generation ended without this attempt succeeding: the owner's claim names
    another attempt (a retry superseded this one), or its latest terminal is
    ``failed`` or ``withdrawn``, or another attempt executed it.
    ``succeeded`` when it is ``done`` by this attempt; ``live`` while this
    attempt holds the claim. Everything else -- queued, moving, absent, a
    cache hit by another attempt, a record without the attempt's nonce,
    anything unreadable -- is ``unknown``, and the sweep keeps the batch.
    """

    attempt = instance["owner_attempt"]
    assert isinstance(attempt, dict)
    return _attempt_state(
        _key_generation(queue, str(instance["owner_action_key"])),
        str(attempt["nonce"]))


def _attempt_state(generation: tuple[str, dict[str, object] | None],
                   nonce: str) -> str:
    """`_producer_attempt_state` for one attempt, over a generation already read.

    Every attempt of one owner key answers from the same read of the key
    (`_key_generation`), so a caller that asks about several attempts of it
    reads the key once.
    """

    from prismabuild import pool as pool_mod

    state, record = generation
    if state == pool_mod.CLAIMED:
        live = _record_nonce(record)
        if not live:
            return "unknown"
        return "live" if live == nonce else "dead"
    if state in (pool_mod.FAILED, pool_mod.WITHDRAWN):
        return "dead"
    if state == pool_mod.DONE:
        assert record is not None
        finished = _record_nonce(record)
        if record.get("status") not in ("executed", "cache_hit") or not finished:
            return "unknown"
        if finished == nonce:
            return "succeeded"
        # A cache hit by another attempt ran nothing: it found the receipt
        # this attempt may have published before it died, and then these
        # batches are the producer's only output (#913).
        return "dead" if record.get("status") == "executed" else "unknown"
    return "unknown"


def _fsync_directories(paths: Sequence[str]) -> None:
    """Make the unlinks of these paths durable before the charge is freed."""

    for directory in sorted({os.path.dirname(path) for path in paths}):
        try:
            handle = os.open(directory, os.O_RDONLY)
        except OSError:
            continue
        try:
            os.fsync(handle)
        except OSError:
            pass
        finally:
            os.close(handle)


def _retire_consumed_batch(queue, instance: Mapping[str, object],
                           template: Mapping[str, object],
                           batch_id: str, *,
                           deferred_holds: Collection[tuple[str, str]] = (),
                           reads: _TickReads | None = None
                           ) -> dict[str, object] | None:
    """One retirement step for one consumed batch; the event to log, or None.

    Every decision and every delete happens under the batch's output-prefix
    lock, the lock `declare_origin_consumer` takes, so no consumer can be
    declared between the decision and the delete.  The one thing done
    outside it is reading an origin whose timestamps alone moved (below).

    The decision, unless the batch is already ``retiring``:

    * With declared consumers: every one must have ``succeeded``. A live one
      holds the batch quietly. A failed, withdrawn, unpublished or unknown
      one holds it and is reported as a stall, once per change of the
      consumers' states, because a retry of that consumer needs the batch.
    * With none: the batch is an orphan once its producer attempt is dead
      (`_attempt_state` over the tick's one read of the owner key,
      `_TickReads.generation`, #977). Otherwise it waits, quietly, for a
      consumer. A read-back template's batch (#1034) can have no consumer,
      so it is an orphan once its owner attempt has ended, dead or
      succeeded.
    * Either way, a consumer filed with ``pbrun --after`` and not yet
      released (#913) holds it, quietly: ``deferred_holds`` is
      `action_edges.held_producer_batches`, and a batch it names waits for
      that consumer to be released and declared.

    The delete checks the output prefix first: a prefix that is not a
    directory here means the file system is not mounted on this host, and
    an absent file would prove nothing. Each origin is then compared with
    the identity its commit recorded:

    * the same file is deleted;
    * an absent file is already gone;
    * a file with another inode is not this batch's any more (a retried
      producer attempt wrote the path again, #912's per-attempt ownership),
      so it is left alone and the batch stops charging for it;
    * the same inode changed in place, or any unreadable stat, refuses and
      keeps the batch -- except when only its timestamps moved (an NFS
      delegation recall, #1111): the file is read and hashed outside the
      lock, and when its digest is the committed one the retirement takes
      the lock again, re-stats it, files the re-pin on the entry
      (``origin_repins``) and deletes it as the committed file.

    Another attempt's claim is read under the same lock first (#1053),
    over every attempt of every template whose prefix overlaps this one's
    (`_TickReads.path_owners`): a path another attempt has committed is
    that batch's, and is left alone as ``superseded`` even when its
    identity is still the recorded one; a path a prewrite of an attempt
    that can still commit names holds the batch until that attempt commits
    or ends: quietly while it is live, and while any holder's state is
    unknown with `ORIGIN_HELD_BY_UNKNOWN_EVENT`, once per change, naming
    each such holder, why it is unknown and whether it is orphaned (#1065).

    Before the first unlink the entry is marked ``retiring``: consumers can
    no longer declare it, and a crash resumes the delete rather than
    deciding again. Each delete is `_unlink_if_committed`, which moves the
    name aside and deletes only the file it moved if that is the committed
    one, so a writer's ``rename`` onto the name that lands at any point is
    never deleted. Afterwards ``origin_reclaimed`` frees its durable class
    bytes and its paths, as `reclaim_origin` does.
    """

    verified: dict[str, dict[str, object]] = {}
    for _attempt in (1, 2):
        outcome = _retire_consumed_batch_locked(
            queue, instance, template, batch_id,
            deferred_holds=deferred_holds, reads=reads, verified=verified)
        if not isinstance(outcome, _OriginsToVerify):
            return outcome
        # With no lock held: one read of each origin whose timestamps alone
        # moved.  A refusal is kept as one, and the locked pass reports it.
        for path, (published, observed, sha256) in outcome.pending.items():
            repin, why = _verify_origin_content(path, published, observed,
                                                sha256, where="retire")
            if repin is not None:
                verified[path] = repin
            else:
                # Other bytes are `origin-changed`, as before #1111; a read
                # that failed is `origin-unverified`, and the next tick reads
                # it again.
                changed = (path, _identity_key(observed), sha256) in _CONTENT_REFUSED
                verified[path] = {"refused": why, "reason": (
                    "origin-changed" if changed else "origin-unverified")}
    # An origin moved again between its read and the lock: the next tick
    # decides, from what is there then.
    return None


class _OriginsToVerify:
    """Origins a retirement must read, outside its lock, before it decides (#1111)."""

    def __init__(self, pending: dict[str, tuple[Mapping[str, object],
                                                 Mapping[str, object], str]]):
        self.pending = pending


def _retire_consumed_batch_locked(
        queue, instance: Mapping[str, object], template: Mapping[str, object],
        batch_id: str, *, deferred_holds: Collection[tuple[str, str]] = (),
        reads: _TickReads | None = None,
        verified: Mapping[str, Mapping[str, object]]
        ) -> dict[str, object] | None | _OriginsToVerify:
    """One pass of `_retire_consumed_batch` under the output-prefix lock."""

    from prismabuild import reader_lease as lease_mod

    tick = reads if reads is not None else _TickReads(queue)
    commitments_path = _commitments_path(queue.root, instance)
    with queue.stage_ownership_lock(str(instance["output_prefix"])):
        try:
            commitments = _read_commitments(commitments_path)
        except ProducedOutputError as exc:
            return _unfiled_report(instance, batch_id, {
                "event": ORIGIN_RETIREMENT_REFUSED_EVENT,
                "reason": str(exc)})
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        entry = batches.get(batch_id)
        if (not isinstance(entry, Mapping)
                or entry.get("origin_only") is not True
                or _entry_lifetime(entry) != ORIGIN_LIFETIME_CONSUMED
                or entry.get("origin_reclaimed")):
            return None
        entry = dict(entry)
        ref = origin_batch_ref(instance, batch_id=batch_id,
                               manifest_digest=str(entry["manifest_digest"]))
        try:
            total = sum(_check_class_bytes(
                entry.get("class_bytes"),
                where=f"committed batch {batch_id!r}").values())
        except ProducedOutputError as exc:
            return _unfiled_report(instance, batch_id, {
                "event": ORIGIN_RETIREMENT_REFUSED_EVENT, "ref": ref,
                "reason": str(exc)})
        base = {"ref": ref, "bytes": total}

        def report(event: dict[str, object]) -> dict[str, object] | None:
            # Once per change: the entry remembers the last report it made.
            signature = hashlib.sha256(json.dumps(
                event, sort_keys=True).encode()).hexdigest()
            if entry.get("retirement_report") == signature:
                return None
            entry["retirement_report"] = signature
            batches[batch_id] = entry
            _write_commitments(commitments_path, {"batches": batches})
            return event

        def quiet() -> None:
            # Nothing to report now; a later stall reports again.
            if "retirement_report" in entry:
                entry.pop("retirement_report")
                batches[batch_id] = entry
                _write_commitments(commitments_path, {"batches": batches})

        retiring = entry.get("retiring")
        if isinstance(retiring, Mapping):
            reason = str(retiring.get("reason") or "")
            consumers = list(retiring.get("consumers") or [])
        elif retiring:
            return report({"event": ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                           "reason": "unknown-retain: retiring record is corrupt"})
        elif _held_for_deferred(instance, deferred_holds):
            quiet()
            return None
        else:
            checked_ref = _checked_origin_ref(ref)
            try:
                declared = _declared_consumers(
                    queue.root, instance, batch_id, checked_ref)
            except ProducedOutputError as exc:
                return report({"event": ORIGIN_RETIREMENT_STALLED_EVENT, **base,
                               "consumers": [], "reason": str(exc)})
            if declared:
                try:
                    consumers = _resolved_consumers(
                        queue, instance, batch_id, checked_ref, declared)
                except ProducedOutputError as exc:
                    return report({"event": ORIGIN_RETIREMENT_STALLED_EVENT,
                                   **base, "consumers": [], "reason": str(exc)})
                if not all(item["state"] in _RESOLVED_CONSUMER_STATES
                           for item in consumers):
                    if any(item["state"] in _STALLED_CONSUMER_STATES
                           for item in consumers):
                        return report({"event": ORIGIN_RETIREMENT_STALLED_EVENT,
                                       **base, "consumers": consumers})
                    quiet()
                    return None
                reason = "consumed"
            else:
                # A write-only batch waits for a consumer while its producer
                # lives or has succeeded. A read-back one (#1034) can have no
                # consumer (`declare_origin_consumer` refuses it), so its
                # owner attempt ending, by success too, ends it.
                # The owner key's one read this tick (#977): every due
                # batch of the instance asks, and a running producer that
                # commits ahead of its consumers has many.  Read before the
                # lock, which is safe because an attempt that ended, dead or
                # succeeded, never runs again under its nonce; a stale
                # ``live`` only waits for the next cycle.
                ended = ({"dead"} if template.get("write_only")
                         else _ENDED_ATTEMPT_STATES)
                attempt = instance["owner_attempt"]
                assert isinstance(attempt, dict)
                if _attempt_state(tick.generation(
                        str(instance["owner_action_key"])),
                        str(attempt["nonce"])) not in ended:
                    quiet()
                    return None
                reason, consumers = "orphan", []

        # The delete.  First prove this host sees the output prefix.
        try:
            prefix_mode = os.stat(str(instance["output_prefix"])).st_mode
        except OSError as exc:
            return report({"event": ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                           "reason": "output-prefix-unreachable",
                           "detail": str(exc)})
        if not stat.S_ISDIR(prefix_mode):
            return report({"event": ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                           "reason": "output-prefix-unreachable",
                           "detail": "not a directory on this host"})
        try:
            filed, sealed = _load_batch_record(
                queue.root, instance, template, entry, batch_id)
        except ProducedOutputError as exc:
            return report({"event": ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                           "reason": str(exc)})
        recorded = filed.get("origin_identity")
        if (not isinstance(recorded, Mapping)
                or set(recorded) != {str(desc["path"]) for desc in sealed}):
            return report({"event": ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                           "reason": "origin-proof-missing"})
        # The committed identity with the entry's re-pins applied (#1111).
        try:
            recorded = _effective_origin_identity(filed, sealed, entry)
        except ProducedOutputError as exc:
            return report({"event": ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                           "reason": str(exc)})
        assert recorded is not None
        digests = {str(desc["path"]): desc.get("sha256") for desc in sealed}
        observed: dict[str, dict[str, object]] = {}
        repinned: list[dict[str, object]] = []

        def classify(path: str) -> tuple[str, str]:
            try:
                live = _portable_identity_of(os.lstat(path))
            except FileNotFoundError:
                return ("absent", "")
            except OSError as exc:
                return ("refuse", f"origin-unstatable: {exc}")
            observed[path] = live
            if lease_mod.file_id_matches(recorded[path], live):
                return ("unlink", "")
            if (isinstance(digests.get(path), str)
                    and lease_mod.timestamp_only_mismatch(recorded[path], live)):
                # A delegation recall: settled by the content, which is read
                # outside this lock (`_retire_consumed_batch`).
                got = verified.get(path)
                if got is None:
                    return ("verify", "")
                if "refused" in got:
                    return ("refuse", str(got["reason"]))
                if (got.get("from") == recorded[path]
                        and lease_mod.file_id_matches(got.get("to"), live)):
                    recorded[path] = dict(got["to"])
                    repinned.append(dict(got))
                    return ("unlink", "")
                return ("verify", "")
            if live.get("ino") != recorded[path].get("ino"):
                return ("superseded", "")
            return ("refuse", "origin-changed")

        def to_verify(paths_: Collection[str]) -> _OriginsToVerify:
            return _OriginsToVerify({
                path: (dict(recorded[path]), dict(observed[path]),
                       str(digests[path])) for path in paths_})

        def file_repins() -> None:
            # On the entry, in the same write as the decision it supports.
            if repinned:
                entry["origin_repins"] = [
                    *(entry.get("origin_repins") or []), *repinned]
                repinned.clear()

        paths = sorted(str(desc["path"]) for desc in sealed)
        # Another attempt's claim on these paths, read under this lock.
        try:
            owners = tick.path_owners(instance, template)
        except (ProducedOutputError, OSError, ValueError) as exc:
            return report({"event": ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                           "reason": f"unknown-retain: path owners: {exc}"})
        claimed = {path: note for path in paths
                   if (note := owners.committed(path)) is not None}
        held = [path for path in paths
                if path not in claimed and owners.pending(path) is not None]
        if held:
            # A live writer's hold is quiet; one whose state is unknown is
            # named, once per change, and so is an orphaned one (#1065).
            unknown = [holder for path in held
                       for holder in owners.unknown_holders(path)]
            if unknown:
                return report({"event": ORIGIN_HELD_BY_UNKNOWN_EVENT, **base,
                               **_held_by_unknown_event(unknown, queue.root)})
            quiet()
            return None
        pending: list[str] = []
        for path in paths:
            verdict, why = classify(path)
            if verdict == "refuse" and path not in claimed:
                return report({"event": ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                               "reason": why, "path": path})
            if verdict == "verify" and path not in claimed:
                pending.append(path)
        if pending:
            # Not a refusal and nothing is reported: the caller reads them
            # without this lock and comes back.
            return to_verify(pending)
        if not retiring:
            file_repins()
            entry["retiring"] = {"reason": reason, "consumers": consumers}
            entry.pop("retirement_report", None)
            batches[batch_id] = entry
            _write_commitments(commitments_path, {"batches": batches})
        elif repinned:
            file_repins()
            batches[batch_id] = entry
            _write_commitments(commitments_path, {"batches": batches})
        unlinked: list[str] = []
        superseded: list[str] = []
        absent: list[str] = []
        tag = hashlib.sha256(_batch_report_key(
            instance, batch_id).encode()).hexdigest()[:16]
        for path in paths:
            # A private name an interrupted delete left is settled first,
            # whoever owns the path now: it may hold another writer's file.
            outcome, why = _settle_retiring_leftover(path, recorded[path], tag)
            if outcome == "refuse":
                return report({"event": ORIGIN_RETIREMENT_REFUSED_EVENT,
                               **base, "reason": why, "path": path})
            if outcome == "unlinked":
                unlinked.append(path)
                continue
            if path in claimed:
                # Another attempt committed it: that batch's file, whatever
                # its identity (#1053).
                superseded.append(path)
                continue
            # Checked again at the unlink, not only above: the name is
            # removed only while it is still the committed file.
            verdict, why = classify(path)
            if verdict == "refuse":
                return report({"event": ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                               "reason": why, "path": path})
            if verdict == "verify":
                # Its timestamps moved again since the check above.  The
                # entry is ``retiring``, so the next pass resumes the delete.
                return to_verify([path])
            if repinned:
                file_repins()
                batches[batch_id] = entry
                _write_commitments(commitments_path, {"batches": batches})
            if verdict == "unlink":
                outcome, why = _unlink_if_committed(path, recorded[path], tag)
                if outcome == "refuse":
                    return report({"event": ORIGIN_RETIREMENT_REFUSED_EVENT,
                                   **base, "reason": why, "path": path})
                if outcome == "unlinked":
                    unlinked.append(path)
                elif outcome == "superseded":
                    superseded.append(path)
                else:
                    absent.append(path)
            elif verdict == "superseded":
                superseded.append(path)
            else:
                absent.append(path)
        _fsync_directories(unlinked)
        entry["origin_reclaimed"] = True
        entry.pop("retirement_report", None)
        batches[batch_id] = entry
        _write_commitments(commitments_path, {"batches": batches})
    _UNFILED_REPORTS.pop(_batch_report_key(instance, batch_id), None)
    event = {"event": ORIGIN_RETIRED_EVENT, **base, "reason": reason,
             "consumers": consumers, "origin_identity": dict(recorded),
             "unlinked": unlinked, "superseded": superseded, "absent": absent}
    if claimed:
        event["superseded_by"] = claimed
    return event


def _retiring_name(path: str, tag: str) -> str:
    """The private name `_unlink_if_committed` moves ``path`` to."""

    directory, name = os.path.split(path)
    return os.path.join(directory, f".{name}.pb-retiring-{tag}")


def _is_committed_file(info: os.stat_result,
                       recorded: Mapping[str, object]) -> bool:
    """Whether ``info`` is the file a commit recorded, at any name.

    Inode, size and mtime: a rename changes only ctime. A recorded identity
    without all three is never a match.
    """

    live = _portable_identity_of(info)
    fields = ("ino", "size", "mtime_ns")
    return (all(recorded.get(field) is not None for field in fields)
            and all(live.get(field) == recorded.get(field) for field in fields))


def _settle_retiring_leftover(path: str, recorded: Mapping[str, object],
                              tag: str, *, dir_fd: int | None = None
                              ) -> tuple[str, str]:
    """Finish what an interrupted `_unlink_if_committed` left at its private name.

    Returns ``("none", "")`` when there is no private name. Otherwise the
    private file is the committed one, which is deleted (``unlinked``), or a
    writer's file the interrupted call moved aside:

    * if ``path`` is that same file again (the call linked it back and
      stopped before removing the private name), the private name goes;
    * if ``path`` is free, the file is linked back and the private name
      goes;
    * if ``path`` holds a later file, the private one is kept and refused
      as ``origin-displaced``, naming it for an operator.

    The last two return ``superseded``; any unreadable step refuses.
    ``dir_fd`` is `_unlink_if_committed`'s.
    """

    private = _retiring_name(path, tag)
    try:
        try:
            left = os.stat(private, dir_fd=dir_fd, follow_symlinks=False)
        except FileNotFoundError:
            return ("none", "")
        if _is_committed_file(left, recorded):
            os.unlink(private, dir_fd=dir_fd)
            return ("unlinked", "")
        try:
            there = os.stat(path, dir_fd=dir_fd, follow_symlinks=False)
        except FileNotFoundError:
            there = None
        if there is not None and there.st_ino == left.st_ino:
            os.unlink(private, dir_fd=dir_fd)
            return ("superseded", "")
        if there is not None:
            return ("refuse", f"origin-displaced: another writer's file is "
                              f"kept at {private}")
        return _link_back(path, private, dir_fd=dir_fd)
    except OSError as exc:
        return ("refuse", f"origin-unlink: {private}: {exc}")


#: `renameat2`'s flag: refuse with ``EEXIST`` instead of replacing the target.
_RENAME_NOREPLACE = 1
#: `renameat2`'s directory for a path that is not resolved through a descriptor.
_AT_FDCWD = -100
#: libc's ``renameat2``, looked up on first use; ``[None]`` when there is none.
_RENAMEAT2: list[object] = []


def _rename_noreplace(source: str, target: str, *,
                      dir_fd: int | None = None) -> None:
    """``rename(source, target)`` that never replaces a file at ``target`` (#1064).

    Linux ``renameat2`` with ``RENAME_NOREPLACE``: one atomic step, which
    fails with ``EEXIST`` when ``target`` exists. Unlike a hard link it needs
    no ownership of the file, only write access to the directory, which the
    move aside already used. With ``dir_fd`` both names resolve through that
    descriptor. Raises ``FileExistsError`` when ``target`` exists, and
    ``OSError`` when the file system does not offer it (``EINVAL``: NFS
    rejects every ``renameat2`` flag) or libc has no ``renameat2``
    (``ENOSYS``).
    """

    import ctypes

    if not _RENAMEAT2:
        try:
            function = ctypes.CDLL(None, use_errno=True).renameat2
        except (AttributeError, OSError):
            function = None
        else:
            function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int,
                                 ctypes.c_char_p, ctypes.c_uint]
            function.restype = ctypes.c_int
        _RENAMEAT2.append(function)
    function = _RENAMEAT2[0]
    if function is None:
        raise OSError(errno.ENOSYS, "libc has no renameat2", source)
    directory = _AT_FDCWD if dir_fd is None else dir_fd
    if function(directory, os.fsencode(source), directory, os.fsencode(target),
                _RENAME_NOREPLACE) != 0:
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), source, None, target)


def _link_back(path: str, private: str, *, dir_fd: int | None = None
               ) -> tuple[str, str]:
    """Put a writer's file moved to ``private`` back at ``path``, never over one.

    A hard link, then the private name is removed. The kernel refuses the
    link with ``EPERM`` when ``fs.protected_hardlinks`` is 1 and the caller
    neither owns the file nor can write it -- dl380g10 sets it, and its tier
    loop runs as ``rob``, which need not own a producer's file -- and on a
    file system without hard links. The file is then renamed back with
    `_rename_noreplace`, which needs no ownership (#1064). Either way a file
    already at ``path`` is never replaced: the writer's file is kept at
    ``private`` and refused as ``origin-displaced``, and so is one that
    neither can put back, with both errors named.
    """

    displaced = f"origin-displaced: another writer's file is kept at {private}"
    try:
        os.link(private, path, src_dir_fd=dir_fd, dst_dir_fd=dir_fd,
                follow_symlinks=False)
    except FileExistsError:
        return ("refuse", displaced)
    except PermissionError as link_refused:
        try:
            _rename_noreplace(private, path, dir_fd=dir_fd)
        except FileExistsError:
            return ("refuse", displaced)
        except OSError as rename_refused:
            return ("refuse", f"{displaced}: neither a link ({link_refused}) "
                              f"nor a rename that never replaces "
                              f"({rename_refused}) can put it back")
        return ("superseded", "")
    os.unlink(private, dir_fd=dir_fd)
    return ("superseded", "")


def _unlink_if_committed(path: str, recorded: Mapping[str, object],
                         tag: str, *, dir_fd: int | None = None
                         ) -> tuple[str, str]:
    """Delete the file at ``path`` only if it is the one a commit recorded.

    The only delete of an origin file PB makes (#1053). A plain ``unlink``
    after an identity check removes whatever is at the name when it runs,
    and a producer writes by ``rename(tmp, path)`` under no PB lock, so a
    rename landing between the check and the unlink would lose its file.
    Instead:

    1. ``rename(path, private)``, where ``private`` is ``.<name>.pb-retiring-
       <tag>`` in the same directory (`_retiring_name`). The name is now
       free, and a writer's rename that lands from here on creates a new
       file at ``path``;
    2. ``lstat(private)``: if its inode, size and mtime are the recorded
       ones (a rename changes only ctime), it is the committed file, and it
       is unlinked -- under the private name, which no writer uses;
    3. otherwise a writer's rename landed before step 1, and this moved its
       file aside: it is put back at ``path`` (`_link_back`: a hard link and
       the private name removed, or, when the link is refused, a rename that
       never replaces; the same inode either way, so its bytes never moved).
       If ``path`` is taken again by then, the private file is kept and the
       refusal ``origin-displaced`` names it for an operator.

    Steps 1 and 3 move that writer's file's ctime, which
    `reader_lease.file_id_matches` compares. `origin_retirement_tick` holds
    the output-prefix lock across them, and a commit takes its identity
    under the same lock (#1064), so a commit of that file under a template
    with this prefix records the identity it has after the put-back.

    So a writer's file is never deleted, wherever its rename lands. A call
    interrupted between the steps leaves the private name, and the caller
    settles it before anything else (`_settle_retiring_leftover`). What
    this does not cover, and says so: a writer that rewrites the committed
    inode in place (``open`` and ``write`` with no rename) is outside the
    produced-output contract, which writes every origin file by rename;
    between steps 1 and 3 a reader of ``path`` can find it absent; a commit
    under a template whose prefix only overlaps this one takes another lock
    and can record the ctime from before step 1 (#1063); and a file this
    process may not link, on a file system without ``RENAME_NOREPLACE``
    (NFS), refuses at step 3 and keeps the file at the private name, named
    in the refusal.

    Returns ``(outcome, reason)``: ``unlinked``, ``absent`` (nothing was at
    the name), ``superseded`` (another file was, and is again) or
    ``refuse``.  With ``dir_fd``, ``path`` is a name in that pinned
    directory, and every step resolves through the descriptor: the produced
    spool's export retires a failed group's file that way (#1097).
    """

    private = _retiring_name(path, tag)
    try:
        try:
            os.rename(path, private, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        except FileNotFoundError:
            return ("absent", "")
        if _is_committed_file(os.stat(private, dir_fd=dir_fd, follow_symlinks=False),
                              recorded):
            os.unlink(private, dir_fd=dir_fd)
            return ("unlinked", "")
        return _link_back(path, private, dir_fd=dir_fd)
    except OSError as exc:
        return ("refuse", f"origin-unlink: {private}: {exc}")


def _held_for_deferred(instance: Mapping[str, object],
                       holds: Collection[tuple[str, str]]) -> bool:
    """Whether an unreleased deferred consumer may read this batch (#913)."""

    return (str(instance["owner_action_key"]),
            str(instance["template_id"])) in holds


def _batch_report_key(instance: Mapping[str, object], batch_id: str) -> str:
    attempt = instance["owner_attempt"]
    assert isinstance(attempt, dict)
    return (f"{instance['owner_action_key']}/{instance['template_id']}."
            f"{attempt['nonce']}/{batch_id}")


def _unfiled_report(instance: Mapping[str, object], batch_id: str,
                    event: dict[str, object]) -> dict[str, object] | None:
    """Report once per change what cannot be remembered on the batch itself."""

    event = {**event, "batch": _batch_report_key(instance, batch_id)}
    signature = hashlib.sha256(json.dumps(
        event, sort_keys=True).encode()).hexdigest()
    key = _batch_report_key(instance, batch_id)
    if _UNFILED_REPORTS.get(key) == signature:
        return None
    _UNFILED_REPORTS[key] = signature
    return event


def _outstanding_prewrite_ids(scope: Path,
                              batches: Mapping[str, object]) -> list[str]:
    """Batch ids with a prewrite record in ``scope`` and no committed batch.

    A commit consumes its prewrite, so a record whose batch id is not in the
    commitments is outstanding. Raises ``OSError`` when the directory cannot
    be listed.
    """

    try:
        with os.scandir(scope / "prewrites") as iterator:
            names = sorted(entry.name for entry in iterator
                           if entry.name.endswith(".prewrite.json"))
    except FileNotFoundError:
        return []
    ids = [name[:-len(".prewrite.json")] for name in names]
    return [batch_id for batch_id in ids if batch_id not in batches]


def _ended_prewrite_dispositions(
        queue, instance: Mapping[str, object], template: Mapping[str, object],
        records: Mapping[str, Mapping[str, object]],
        reads: _TickReads,
) -> dict[str, dict[str, object]]:
    """What each outstanding prewrite of an ended attempt is now (#949, #1053).

    ``records`` maps batch ids to their prewrite records. The caller has read
    that the attempt ended (`_attempt_state` is ``dead`` or ``succeeded``):
    its owner gate refuses a claim it no longer holds, so nothing can commit
    these prewrites. Their planned paths decide:

    * all absent: ``reclaim``, reason ``absent``. Nothing durable is left,
      the rule `abort_prewrite` applies.
    * each present one is committed by another attempt, of this key or of
      any other action whose template's prefix overlaps this one's
      (`_TickReads.path_owners`): ``reclaim``, reason ``superseded``. A
      retry or a relaunch wrote those files again and committed them, so
      they are that batch's, charged once, there. A prewrite of an attempt
      that succeeded counts as its commit.
    * any present one is still planned by another attempt that can commit,
      and none belongs to nobody: ``hold``, until that attempt commits or
      ends. Its ``unknown`` lists the holders whose state is unknown
      (`_PathOwners.unknown_holders`), which the sweep reports (#1065).
    * otherwise ``orphaned``: files that belong to no batch. PB never
      deletes a file whose identity no commit recorded, so an operator
      removes them, and then the reservation is reclaimed as ``absent``.

    The output prefix must be a directory on this host first, or an absent
    file proves nothing: ``refused``, reason ``output-prefix-unreachable``.
    State that cannot be read is ``refused``, reason ``unknown-retain``.
    The prefix is statted once and the other attempts' paths are read at
    most once, for all the records.
    """

    try:
        reachable = stat.S_ISDIR(os.stat(str(instance["output_prefix"])).st_mode)
        detail = "not a directory on this host"
    except OSError as exc:
        reachable, detail = False, str(exc)
    if not reachable:
        return {batch_id: {"action": "refused",
                           "reason": "output-prefix-unreachable",
                           "detail": detail}
                for batch_id in records}
    owners: _PathOwners | None = None
    owners_error: str | None = None
    decided: dict[str, dict[str, object]] = {}
    for batch_id, record in records.items():
        present: list[str] = []
        try:
            for path in record.get("paths", []):
                try:
                    os.lstat(str(path))
                except FileNotFoundError:
                    continue
                present.append(str(path))
        except OSError as exc:
            decided[batch_id] = {"action": "refused", "reason": "unknown-retain",
                                 "detail": str(exc)}
            continue
        if not present:
            decided[batch_id] = {"action": "reclaim", "reason": "absent",
                                 "superseded": []}
            continue
        if owners is None and owners_error is None:
            try:
                owners = reads.path_owners(instance, template)
            except (ProducedOutputError, OSError, ValueError) as exc:
                owners_error = str(exc)
        if owners is None:
            decided[batch_id] = {"action": "refused", "reason": "unknown-retain",
                                 "detail": str(owners_error)}
            continue
        superseded: list[dict[str, str]] = []
        held: list[str] = []
        orphaned: list[str] = []
        unknown: list[dict[str, object]] = []
        for path in present:
            note = owners.committed(path)
            if note is not None:
                superseded.append({"path": path, **note})
            elif owners.pending(path) is not None:
                held.append(path)
                unknown.extend(owners.unknown_holders(path))
            else:
                orphaned.append(path)
        if orphaned:
            decided[batch_id] = {"action": "orphaned", "paths": orphaned,
                                 "superseded": superseded, "held": held,
                                 "owners": owners.fingerprint}
        elif held:
            # ``unknown``: the holders whose state could not be read, which
            # the sweep names (#1065); a live holder's hold is quiet.
            decided[batch_id] = {"action": "hold", "owners": owners.fingerprint,
                                 "unknown": unknown}
        else:
            decided[batch_id] = {"action": "reclaim", "reason": "superseded",
                                 "superseded": superseded}
    return decided


def _prepaid_intent_names(reads: _TickReads, instance: Mapping[str, object],
                          template: Mapping[str, object], batch_id: str,
                          tier: str) -> bool | None:
    """Whether a pool funding intent still names this prewrite's batch.

    The prewrite is that intent's precommit recovery authority
    (`abort_prewrite` refuses while one exists), so it is not reclaimed
    under it. ``None`` when the owner's funding census cannot be read.
    """

    intents, unknown = reads.census(str(instance["owner_action_key"]))
    if unknown:
        return None
    attempt = instance["owner_attempt"]
    assert isinstance(attempt, dict)
    digest = template_sha256(template)
    return any(str(record.get("batch_id")) == batch_id
               and str(record.get("tier_id")) == tier
               and str(record.get("template_sha256")) == digest
               and str(record.get("owner_nonce")) == str(attempt["nonce"])
               and str(record.get("owner_scope_id")) == str(attempt["scope_id"])
               for record in intents)


#: What the tick last left in place for an ended attempt's prewrite (#1053),
#: by the prewrite's report key: ``(inputs, action)``. ``action`` is
#: ``orphaned``, or ``hold`` for a path another attempt still plans;
#: ``inputs`` is what it was decided on -- the record's version, the
#: version of each directory its paths are in (a file created, removed or
#: renamed there changes it), and the other attempts' claims
#: (`_PathOwners.fingerprint`). While all of those are unchanged the
#: decision is too, and the tick neither takes the lock nor ``lstat``s one
#: path: R13's 28 orphaned prewrites name 3,584 of them, every 5 s. A hold
#: for a pool funding intent is decided again each cycle.
_KEPT_PREWRITES: dict[str, tuple[tuple[object, ...], str]] = {}


def _prewrite_inputs(record_path: Path, record: Mapping[str, object] | None,
                     kept_dirs: Sequence[str] | None = None,
                     seen: dict[str, object] | None = None
                     ) -> tuple[object, ...] | None:
    """``(record version, directories, their versions)``, or None if unreadable.

    The record's version is taken before the caller reads it and the
    directories' before any path is ``lstat``ed, so a change that lands
    between is seen as a change next cycle, never remembered as none.
    ``seen`` keeps each directory's version for one caller's pass: an
    instance's prewrites usually name one directory.
    """

    def version_of(directory: str) -> object:
        if seen is None:
            return _file_version(directory)
        if directory not in seen:
            seen[directory] = _file_version(directory)
        return seen[directory]

    try:
        version = _file_version(record_path)
        if kept_dirs is None:
            paths = (record or {}).get("paths", [])
            dirs = tuple(sorted({os.path.dirname(str(path)) for path in paths}))
        else:
            dirs = tuple(kept_dirs)
        return (version, dirs, tuple(version_of(item) for item in dirs))
    except ProducedOutputError:
        return None


def _sweep_ended_prewrites(queue, instance: Mapping[str, object],
                           template: Mapping[str, object],
                           batch_ids: Sequence[str],
                           reads: _TickReads) -> list[dict[str, object]]:
    """The sweep of one instance's outstanding prewrites (#949, #1053).

    Nothing while the attempt can still commit them: the owner key's one
    read this tick (`_TickReads.generation`) says whether it ended.
    Otherwise under the output-prefix lock, which `require_prewrite` and
    `commit_batch` also take, so no attempt of this template can prewrite
    or commit between the decision and its effect
    (`_ended_prewrite_dispositions`). A ``reclaim`` removes the record,
    which frees the reservation the instance's accounting charged for it
    (`_outstanding_sums`), and returns ``output-prewrite-reclaimed``. An
    ``orphaned`` or ``refused`` prewrite is reported once per change
    (``output-prewrite-orphaned``, with its ``remedy``, and
    ``output-origin-retirement-refused``), and so is a ``hold`` in which an
    attempt whose state is unknown takes part (`ORIGIN_HELD_BY_UNKNOWN_EVENT`,
    #1065); a hold only live attempts take stays quiet.

    A staged template's prewrite (#1053) is also the precommit authority of
    any pool funding intent filed for its batch: while one names it, the
    record is held, as `abort_prewrite` holds it, and a census that cannot
    be read refuses. A write-only template stages nothing and has none.

    The generation is read before the lock. That is safe because ``dead``
    and ``succeeded`` are final for a nonce. A sibling that ends or starts
    after the read can change only a report or a hold, never a removal,
    which depends only on the files present and the batches committed, both
    read under the lock.
    """

    keys = {batch_id: f"{_batch_report_key(instance, batch_id)}.prewrite"
            for batch_id in batch_ids}
    attempt = instance["owner_attempt"]
    assert isinstance(attempt, dict)
    generation = reads.generation(str(instance["owner_action_key"]))
    if _attempt_state(generation, str(attempt["nonce"])) not in _ENDED_ATTEMPT_STATES:
        for key in keys.values():
            _UNFILED_REPORTS.pop(key, None)
        return []
    directory = _prewrites_dir(queue.root, instance)
    # First what is unchanged since the decision the tick last left in
    # place: no lock, and no path read (`_KEPT_PREWRITES`).
    owners_now: list[object] = []
    seen: dict[str, object] = {}

    def unchanged(batch_id: str) -> bool:
        kept = _KEPT_PREWRITES.get(keys[batch_id])
        if kept is None:
            return False
        (version, dirs, dir_versions, owners_then), _action = kept
        inputs = _prewrite_inputs(directory / f"{batch_id}.prewrite.json",
                                  None, kept_dirs=dirs, seen=seen)
        if inputs is None or inputs != (version, dirs, dir_versions):
            return False
        if not owners_now:
            try:
                owners_now.append(
                    reads.path_owners(instance, template).fingerprint)
            except (ProducedOutputError, OSError, ValueError):
                owners_now.append(None)
        return owners_now[0] is not None and owners_now[0] == owners_then

    batch_ids = [batch_id for batch_id in batch_ids if not unchanged(batch_id)]
    if not batch_ids:
        return []
    staged = not is_write_only(template)
    reports: list[tuple[str, dict[str, object]]] = []
    reclaimed: list[dict[str, object]] = []
    unlinked: list[str] = []
    with queue.stage_ownership_lock(str(instance["output_prefix"])):
        records: dict[str, dict[str, object]] = {}
        inputs: dict[str, tuple[object, ...] | None] = {}
        locked_seen: dict[str, object] = {}
        for batch_id in batch_ids:
            base = {"prewrite": _batch_report_key(instance, batch_id)}
            record_path = directory / f"{batch_id}.prewrite.json"
            version = _prewrite_inputs(record_path, {})
            try:
                record = _read_prewrite(record_path)
            except ProducedOutputError as exc:
                _KEPT_PREWRITES.pop(keys[batch_id], None)
                reports.append((batch_id, {
                    "event": ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                    "reason": "unknown-retain", "detail": str(exc)}))
                continue
            if record is None:
                _KEPT_PREWRITES.pop(keys[batch_id], None)
                _UNFILED_REPORTS.pop(keys[batch_id], None)
                continue
            records[batch_id] = record
            # The record's version from before it was read; its directories'
            # from before any of its paths is.
            dirs = _prewrite_inputs(record_path, record, seen=locked_seen)
            inputs[batch_id] = (None if version is None or dirs is None
                                else (version[0], dirs[1], dirs[2]))
        dispositions = _ended_prewrite_dispositions(
            queue, instance, template, records, reads)
        for batch_id, disposition in dispositions.items():
            base = {"prewrite": _batch_report_key(instance, batch_id),
                    "class_bytes": dict(records[batch_id]["class_bytes"])}
            action = disposition["action"]
            kept = inputs.get(batch_id)
            if action in ("orphaned", "hold") and kept is not None:
                _KEPT_PREWRITES[keys[batch_id]] = (
                    (*kept, disposition["owners"]), action)
            else:
                _KEPT_PREWRITES.pop(keys[batch_id], None)
            if action == "reclaim" and staged:
                named = _prepaid_intent_names(
                    reads, instance, template, batch_id,
                    str(records[batch_id].get("tier")))
                if named is None:
                    disposition = {"action": "refused",
                                   "reason": "unknown-retain",
                                   "detail": "funding-census"}
                    action = "refused"
                elif named:
                    action = "hold"
            if action == "hold" and disposition.get("unknown"):
                # Held by an attempt whose state is unknown (#1065): named,
                # once per change, not held silently.
                reports.append((batch_id, {
                    "event": ORIGIN_HELD_BY_UNKNOWN_EVENT, **base,
                    **_held_by_unknown_event(disposition["unknown"],
                                             queue.root)}))
            elif action == "hold":
                _UNFILED_REPORTS.pop(keys[batch_id], None)
            elif action == "reclaim":
                path = directory / f"{batch_id}.prewrite.json"
                try:
                    path.unlink()
                except FileNotFoundError:
                    continue
                unlinked.append(str(path))
                _UNFILED_REPORTS.pop(keys[batch_id], None)
                reclaimed.append({"event": ORIGIN_PREWRITE_RECLAIMED_EVENT, **base,
                                  "reason": disposition["reason"],
                                  "superseded": disposition["superseded"]})
            elif action == "orphaned":
                reports.append((batch_id, {
                    "event": ORIGIN_PREWRITE_ORPHANED_EVENT, **base,
                    "paths": disposition["paths"],
                    "superseded": disposition["superseded"],
                    "held": disposition["held"],
                    "remedy": ORPHANED_PREWRITE_REMEDY}))
            else:
                reports.append((batch_id, {
                    "event": ORIGIN_RETIREMENT_REFUSED_EVENT, **base,
                    "reason": disposition["reason"],
                    "detail": disposition["detail"]}))
        if unlinked:
            _fsync_directories(unlinked)
    events = list(reclaimed)
    for batch_id, event in sorted(reports, key=lambda item: item[0]):
        signature = hashlib.sha256(json.dumps(
            event, sort_keys=True).encode()).hexdigest()
        if _UNFILED_REPORTS.get(keys[batch_id]) == signature:
            continue
        _UNFILED_REPORTS[keys[batch_id]] = signature
        events.append(event)
    return events


def _orphaned_prewrites(queue, instance: Mapping[str, object],
                        template: Mapping[str, object],
                        batch_ids: Sequence[str],
                        reads: _TickReads, *,
                        unreadable: list[str], where: str,
                        held: list[dict[str, object]] | None = None
                        ) -> list[dict[str, object]]:
    """The listing's side of the sweep: an ended attempt's orphaned prewrites.

    Read-only and lock-free, over the same `_ended_prewrite_dispositions` the
    tick applies. A record or state it cannot read goes to ``unreadable``.
    A prewrite held by an attempt whose state is unknown (#1065) goes to
    ``held``, as the tick reports it.
    """

    attempt = instance["owner_attempt"]
    assert isinstance(attempt, dict)
    generation = reads.generation(str(instance["owner_action_key"]))
    if _attempt_state(generation, str(attempt["nonce"])) not in _ENDED_ATTEMPT_STATES:
        return []
    directory = _prewrites_dir(queue.root, instance)
    records: dict[str, dict[str, object]] = {}
    for batch_id in batch_ids:
        try:
            record = _read_prewrite(directory / f"{batch_id}.prewrite.json")
        except ProducedOutputError as exc:
            unreadable.append(f"{where}/{batch_id}.prewrite: {exc}")
            continue
        if record is not None:
            records[batch_id] = record
    listed: list[dict[str, object]] = []
    for batch_id, disposition in _ended_prewrite_dispositions(
            queue, instance, template, records, reads).items():
        if disposition["action"] == "orphaned":
            listed.append({
                "prewrite": _batch_report_key(instance, batch_id),
                "class_bytes": dict(records[batch_id]["class_bytes"]),
                "paths": disposition["paths"],
                "superseded": disposition["superseded"],
                "held": disposition["held"],
                "remedy": ORPHANED_PREWRITE_REMEDY})
        elif (disposition["action"] == "hold" and held is not None
              and disposition.get("unknown")):
            held.append({
                "prewrite": _batch_report_key(instance, batch_id),
                "class_bytes": dict(records[batch_id]["class_bytes"]),
                **_held_by_unknown_event(disposition["unknown"], queue.root)})
        elif (disposition["action"] == "refused"
              and disposition["reason"] == "unknown-retain"):
            unreadable.append(
                f"{where}/{batch_id}.prewrite: {disposition['detail']}")
    return listed


def blocked_origin_batches(queue) -> dict[str, list]:
    """The consumed batches held only by consumers that ended (#926), read-only.

    A batch is *blocked* when every declared consumer still holding it is
    ``failed`` or ``withdrawn``: nothing that is queued or running will
    release it, and it waits for a superseding resubmission or an operator
    release (`release_origin_consumer`). The tick reports that once per
    change; this reads the same records again, for a listing that does not
    scroll away.

    It scans the produced-output scopes as `origin_retirement_tick` does,
    skipping the scopes it skips, takes no lock and writes nothing. A batch
    that is retiring, reclaimed, or has no declared consumer is not listed;
    neither is one held for a deferred consumer's release (#913), which is
    waiting, not blocked. If those holds cannot be read, nothing is listed.
    Returns ``{"blocked": [...], "orphaned_prewrites": [...],
    "held_by_unknown": [...], "unreadable": [...]}``. Each blocked entry
    carries the batch's ``ref`` and ``bytes``, every declared consumer's
    state as the tick resolves it (`_resolved_consumers`), the ``holding``
    ones, and ``reported``: whether the entry carries the tick's report memo.

    ``orphaned_prewrites`` lists each prewrite, of any template (#1053),
    whose attempt ended before committing and whose files belong to no batch
    (#949, `_ended_prewrite_dispositions`): its ``prewrite`` coordinates
    (owner/template.nonce/batch), ``class_bytes``, the orphaned ``paths``,
    the ``superseded`` and ``held`` ones, and the ``remedy``.

    ``held_by_unknown`` lists what the tick reports as
    `ORIGIN_HELD_BY_UNKNOWN_EVENT` (#1065): a consumed batch the tick would
    retire now (every declared consumer resolved, or none declared and its
    producer attempt dead) and an ended attempt's prewrite it would reclaim,
    each held only because another attempt whose state is unknown has
    prewritten one of its paths. Each carries its ``ref`` and ``bytes`` (a
    batch) or ``prewrite`` and ``class_bytes``, the ``holders`` with ``why``
    each is unknown and whether it is ``orphaned``, the ``reason`` and, for
    an orphaned holder, the ``remedy``.
    """

    from . import action_edges

    blocked: list[dict[str, object]] = []
    orphaned: list[dict[str, object]] = []
    held_by_unknown: list[dict[str, object]] = []
    unreadable: list[str] = []

    def found() -> dict[str, list]:
        return {"blocked": blocked, "orphaned_prewrites": orphaned,
                "held_by_unknown": held_by_unknown,
                "unreadable": unreadable}

    scopes_root = Path(queue.root) / "residency" / OUTPUT_SCOPES_SUBDIR
    templates_root = Path(queue.root) / "residency" / OUTPUT_TEMPLATES_SUBDIR
    try:
        owners = _scope_owners(scopes_root)
    except FileNotFoundError:
        return found()
    except OSError as exc:
        unreadable.append(f"output scopes: {exc}")
        return found()
    holds: set[tuple[str, str]] | None = None
    reads = _TickReads(queue)
    for owner in owners:
        try:
            scopes = sorted(child for child in (scopes_root / owner).iterdir()
                            if child.is_dir())
        except OSError as exc:
            unreadable.append(f"{owner[:12]}: {exc}")
            continue
        for scope in scopes:
            where = f"{owner[:12]}/{scope.name}"
            try:
                batches = reads.batches(scope)
            except ProducedOutputError as exc:
                unreadable.append(f"{where}: {exc}")
                continue
            due = [(batch_id, entry) for batch_id, entry in sorted(batches.items())
                   if isinstance(entry, Mapping)
                   and entry.get("origin_only") is True
                   and _entry_lifetime(entry) == ORIGIN_LIFETIME_CONSUMED
                   and not entry.get("origin_reclaimed")
                   and not entry.get("retiring")]
            try:
                outstanding = _outstanding_prewrite_ids(scope, batches)
            except OSError as exc:
                unreadable.append(f"{where}/prewrites: {exc}")
                outstanding = []
            if not due and not outstanding:
                continue
            if not due:
                # As the tick: a producer that can still commit is skipped
                # before its instance is read.
                if (_attempt_state(reads.generation(owner),
                                   scope.name.rpartition(".")[2])
                        not in _ENDED_ATTEMPT_STATES):
                    continue
            try:
                instance = validate_instance(json.loads(
                    (scope / "instance.json").read_text()))
                template = validate_template(json.loads(
                    (templates_root / f"{instance['template_id']}.json"
                     ).read_text()))
            except (OSError, ValueError) as exc:
                unreadable.append(f"{where}: {exc}")
                continue
            # The tick skips a scope filed elsewhere or bound to another
            # template; so does the listing.
            if (instance_dir(queue.root, instance) != scope
                    or template_sha256(template) != instance["template_sha256"]):
                continue
            if outstanding:
                # Any template's, as the tick sweeps them (#1053).
                orphaned.extend(_orphaned_prewrites(
                    queue, instance, template, outstanding, reads,
                    unreadable=unreadable, where=where, held=held_by_unknown))
            if not due:
                continue
            if holds is None:
                try:
                    holds = action_edges.held_producer_batches(queue)
                except (OSError, ValueError) as exc:
                    # Without the holds no batch can be told apart from one
                    # waiting for a deferred release; list none rather than
                    # all of them.
                    unreadable.append(f"deferred holds: {exc}")
                    blocked.clear()
                    held_by_unknown[:] = [item for item in held_by_unknown
                                          if "ref" not in item]
                    return found()
            if _held_for_deferred(instance, holds):
                continue
            scope_owners: _PathOwners | None = None
            for batch_id, entry in due:
                try:
                    ref = origin_batch_ref(
                        instance, batch_id=batch_id,
                        manifest_digest=str(entry["manifest_digest"]))
                    checked_ref = _checked_origin_ref(ref)
                    declared = _declared_consumers(
                        queue.root, instance, batch_id, checked_ref)
                    consumers = (_resolved_consumers(
                        queue, instance, batch_id, checked_ref, declared)
                        if declared else [])
                    total = sum(_check_class_bytes(
                        entry.get("class_bytes"),
                        where=f"committed batch {batch_id!r}").values())
                except (ProducedOutputError, KeyError, ValueError) as exc:
                    unreadable.append(f"{where}/{batch_id}: {exc}")
                    continue
                # What the tick would retire now, but for a hold by an
                # attempt whose state is unknown (#1065).
                retirable = (all(item["state"] in _RESOLVED_CONSUMER_STATES
                                 for item in consumers) if declared
                             else _attempt_state(
                                 reads.generation(owner),
                                 str(instance["owner_attempt"]["nonce"]))
                             == "dead")
                if retirable:
                    try:
                        if scope_owners is None:
                            scope_owners = reads.path_owners(instance, template)
                        holders = _unknown_path_holders(
                            scope_owners, entry.get("paths") or [])
                    except (ProducedOutputError, OSError, ValueError) as exc:
                        unreadable.append(f"{where}/{batch_id}: owners: {exc}")
                        holders = []
                    if holders:
                        held_by_unknown.append({
                            "ref": ref, "bytes": total,
                            **_held_by_unknown_event(holders, queue.root)})
                if not declared:
                    continue
                holding = [item for item in consumers
                           if item["state"] not in _RESOLVED_CONSUMER_STATES]
                if holding and all(item["state"] in _TERMINAL_CONSUMER_STATES
                                   for item in holding):
                    blocked.append({
                        "ref": ref, "bytes": total, "consumers": consumers,
                        "holding": [str(item["action_key"]) for item in holding],
                        "reported": "retirement_report" in entry})
    return found()


#: Attempts this process has filed or found in the attempt index, by queue
#: root, owner and scope name: the tick's backfill stats each scope once
#: per process, not once per cycle.
_INDEXED_ATTEMPTS: set[tuple[str, str, str]] = set()
#: Queue roots whose attempt index this process has marked complete.
_INDEX_MARKED: set[str] = set()


def _backfill_attempt(queue_root: str | Path, owner: str, scope_name: str) -> bool:
    """Index one scope the tick listed, if it is not yet (#1053).

    A scope filed before the index existed, or by a producer running code
    that predates it, has no pointer; the tick files it, idempotently.
    Returns False when it could not be filed.
    """

    key = (str(queue_root), owner, scope_name)
    if key in _INDEXED_ATTEMPTS:
        return True
    template_id, dot, nonce = scope_name.rpartition(".")
    if (not dot or not template_id or len(owner) != 64
            or len(nonce) != _HEX32 or any(c not in _HEX for c in nonce)):
        # Not an attempt's scope (an older flat spelling): nothing to index.
        _INDEXED_ATTEMPTS.add(key)
        return True
    pointer = _attempts_root(queue_root) / template_id / f"{owner}.{nonce}"
    try:
        if not os.path.lexists(pointer):
            _index_attempt(queue_root, owner, template_id, nonce)
    except ProducedOutputError:
        return False
    _INDEXED_ATTEMPTS.add(key)
    return True


def _unretired_staged_batches(batches: Mapping[str, object]) -> list[str]:
    """Batch ids with a stage copy not yet retired, in order.

    Every committed batch that is not origin-only and whose ACTIVE
    materialization is unretired (`_batch_stage_retired`). A malformed
    materialization list is listed too: the retirement step reports it.
    """

    found: list[str] = []
    for batch_id, entry in sorted(batches.items()):
        if not isinstance(entry, Mapping) or entry.get("origin_only") is True:
            continue
        try:
            if _batch_stage_retired(entry):
                continue
        except ProducedOutputError:
            pass
        found.append(batch_id)
    return found


class _DeadStagedReports:
    """Once-per-change reports for one dead producer's staged batch (#1053)."""

    def __init__(self, instance: Mapping[str, object], batch_id: str) -> None:
        self.coordinates = _batch_report_key(instance, batch_id)
        self.memo = f"{self.coordinates}.stage"

    def refused(self, reason: str, **detail: object) -> dict[str, object] | None:
        event = {"event": ORIGIN_RETIREMENT_REFUSED_EVENT,
                 "batch": self.coordinates, "reason": reason, **detail}
        signature = hashlib.sha256(json.dumps(
            event, sort_keys=True, default=str).encode()).hexdigest()
        if _UNFILED_REPORTS.get(self.memo) == signature:
            return None
        _UNFILED_REPORTS[self.memo] = signature
        return event

    def quiet(self) -> None:
        _UNFILED_REPORTS.pop(self.memo, None)
        return None


def _dead_staged_batch_ready(queue, instance: Mapping[str, object],
                             batch_id: str, batches: Mapping[str, object], *,
                             reads: _TickReads) -> dict[str, object]:
    """Whether a dead producer's staged batch is the tick's to retire (#1053).

    Returns ``{"event": ...}`` (the report, or None) when it is not, else
    ``{"ready": ...}``: the mover, the tier and the tier's stage root.  The
    conditions are `_retire_dead_staged_batches`'s.
    """

    from prismabuild import pool as pool_mod

    report = _DeadStagedReports(instance, batch_id)
    entry = batches.get(batch_id)
    if not isinstance(entry, Mapping):
        return {"event": report.quiet()}
    try:
        active = _active_materialization(entry)
    except ProducedOutputError as exc:
        return {"event": report.refused(f"unknown-retain: {exc}")}
    if active.get("retired"):
        return {"event": report.quiet()}
    mover = str(active.get("mover_key") or "")
    tier = str(active.get("tier") or entry.get("tier") or "")
    if len(mover) != 64 or not tier:
        return {"event": report.refused("unknown-retain: batch-target-mismatch")}
    mover_state, _row = reads.generation(mover)
    if mover_state in (pool_mod.READY, pool_mod.CLAIMED, "moving"):
        return {"event": report.quiet()}
    if mover_state not in (pool_mod.DONE, pool_mod.FAILED, pool_mod.WITHDRAWN):
        return {"event": report.refused(
            f"unknown-retain: its mover {mover[:12]} is {mover_state}",
            mover_key=mover)}
    try:
        held = queue.tier_ledger(tier).holder_tokens(mover)
    except Exception as exc:
        return {"event": report.refused(f"unknown-retain: tier ledger: {exc}",
                                        mover_key=mover)}
    if held:
        return {"event": report.quiet()}
    try:
        record, file_state = queue.output_funding_file_state(mover, tier)
    except Exception as exc:
        return {"event": report.refused(f"unknown-retain: funding: {exc}",
                                        mover_key=mover)}
    if file_state not in ("absent", "ok"):
        return {"event": report.refused(
            f"unknown-retain: funding record is {file_state}", mover_key=mover)}
    if file_state == "ok" and str((record or {}).get("state")) not in (
            "consumed", "released"):
        return {"event": report.refused(f"funding-{(record or {}).get('state')}",
                                        mover_key=mover)}
    try:
        tier_record = _announced_tier_record(queue, tier)
    except Exception as exc:
        return {"event": report.refused(f"unknown-retain: tier record: {exc}",
                                        mover_key=mover)}
    if tier_record is None or not tier_record.get("mountpoint"):
        return {"event": report.refused(f"tier-not-announced: {tier}",
                                        mover_key=mover)}
    if not _egress_runs_in_process(tier_record):
        # Elsewhere `retire_batch` would publish an egress action; the tick
        # publishes nothing.  The tier loop runs on the tier host.
        return {"event": report.refused(
            "not-on-the-tier-host", mover_key=mover,
            tier_host=str(tier_record.get("host") or ""))}
    try:
        import stage_release  # type: ignore[import-not-found]  # noqa: F401
    except ImportError:
        return {"event": report.refused("stage-release-unimportable",
                                        mover_key=mover)}
    return {"ready": {"mover": mover, "tier": tier,
                      "stage_root": str(tier_record["mountpoint"])}}


def _dead_staged_batch_event(queue, instance: Mapping[str, object],
                             template: Mapping[str, object], batch_id: str,
                             batches: Mapping[str, object],
                             result: Mapping[str, object], *, mover: str,
                             tier: str, producer_state: str,
                             reads: _TickReads) -> dict[str, object] | None:
    """The event for one dead producer's batch retirement, from its answer."""

    report = _DeadStagedReports(instance, batch_id)
    if result.get("deferred"):
        # The cycle's budget ran out before this batch's egress: nothing of
        # it was touched, and the next cycle takes it up.
        return None
    if result.get("ok") is not True:
        receipt = result.get("receipt")
        errors = (list(receipt.get("errors") or [])
                  if isinstance(receipt, Mapping) else [])
        return report.refused(str(result.get("refusal") or "retirement refused"),
                              mover_key=mover, errors=errors)
    if result.get("duplicate"):
        return report.quiet()
    receipt = result.get("receipt")
    receipt = receipt if isinstance(receipt, Mapping) else {}
    event: dict[str, object] = {
        "event": DEAD_PRODUCER_BATCH_RETIRED_EVENT,
        "batch": report.coordinates,
        "mover_key": mover, "tier": tier, "producer_state": producer_state,
        "origin_kept": True,
        "entries_deleted": int(receipt.get("entries_deleted") or 0),
        "bytes_deleted": int(receipt.get("bytes_deleted") or 0),
        "tokens_released": int(receipt.get("tokens_released") or 0),
    }
    # Which of its paths another attempt now claims: for the record only.
    entry = batches.get(batch_id)
    entry = entry if isinstance(entry, Mapping) else {}
    try:
        owners = reads.path_owners(instance, template)
        superseded: list[dict[str, str]] = []
        for path in sorted(str(item) for item in entry.get("paths") or []):
            note = owners.committed(path)
            kind = "committed"
            if note is None:
                note, kind = owners.pending(path), "prewritten"
            if note is not None:
                superseded.append({"path": path, "kind": kind, **note})
        event["superseded"] = superseded
    except (ProducedOutputError, OSError, ValueError) as exc:
        event["superseded"] = None
        event["superseded_unreadable"] = str(exc)
    _UNFILED_REPORTS.pop(report.memo, None)
    return event


def _retire_dead_staged_batches(queue, instance: Mapping[str, object],
                                template: Mapping[str, object],
                                staged: Sequence[str],
                                batches: Mapping[str, object], *,
                                producer_state: str, reads: _TickReads,
                                budget=None) -> list[dict[str, object]]:
    """File the stage retirements of a dead producer's batches (#1053, #1072).

    R13 left ten staged batches that read ``retired: false`` for ever: its
    movers had finished and held no tier tokens, their funding was
    ``consumed``, and their stage copies were gone, but the producer died
    before its own `retire_batch` filed. The #929 sweep reaches only a
    mover that still holds tokens, so nothing retired them, and the
    unretired batches kept their paths from every later writer.

    The caller has read that the producer attempt ended (``dead`` or
    ``succeeded``). A batch is retired here when its active
    materialization's mover has ended (``done``, ``failed`` or
    ``withdrawn``), holds no tier tokens (a holder is the #929 sweep's), and
    its funding record is absent, ``consumed`` or ``released``
    (`_dead_staged_batch_ready`). Then the producer's own retirement runs, on
    the tier host only: every such batch of the instance on one stage root
    goes through `retire_staged_batches`, which validates each batch record,
    runs the ordinary egress on the batch's fragment root (which finds
    nothing to delete once the copy is gone, and deletes a copy that remains
    only against its fragment's identity), and files ``retired`` for all of
    them with one write of the instance's commitments (#1072).

    Retirement closes the stage records. It never reclaims, and never
    touches, the batch's origin files: a relaunch reads them (R13's reads
    392 of its predecessor's 436 batches), and their durable charge stays
    until a successor supersedes them or an operator reclaims them.

    A mover still queued or running, or a token holder, waits quietly, and
    so does a batch whose egress ``budget`` deferred to a later cycle.
    Anything else that stops the retirement is reported once per change as
    ``output-origin-retirement-refused``. Returns the events to log.
    """

    events: list[dict[str, object]] = []
    groups: dict[str, list[str]] = {}
    ready: dict[str, dict[str, object]] = {}
    for batch_id in staged:
        try:
            found = _dead_staged_batch_ready(queue, instance, batch_id, batches,
                                             reads=reads)
        except (ProducedOutputError, OSError, ValueError) as exc:
            found = {"event": _unfiled_report(instance, f"{batch_id}.stage", {
                "event": ORIGIN_RETIREMENT_REFUSED_EVENT,
                "reason": f"unknown-retain: {type(exc).__name__}: {exc}"})}
        if "ready" not in found:
            if found.get("event") is not None:
                events.append(found["event"])  # type: ignore[arg-type]
            continue
        ready[batch_id] = found["ready"]  # type: ignore[assignment]
        groups.setdefault(str(ready[batch_id]["stage_root"]), []).append(batch_id)
    from prismabuild import pool as pool_mod

    residency_root = output_fragment_root(Path(queue.root) / pool_mod.RESIDENCY)
    for stage_root, batch_ids in groups.items():
        try:
            results = retire_staged_batches(
                queue, instance, template, batch_ids, stage_root=stage_root,
                residency_root=residency_root, budget=budget)
        except (ProducedOutputError, OSError, ValueError) as exc:
            for batch_id in batch_ids:
                event = _unfiled_report(instance, f"{batch_id}.stage", {
                    "event": ORIGIN_RETIREMENT_REFUSED_EVENT,
                    "reason": f"unknown-retain: {type(exc).__name__}: {exc}"})
                if event is not None:
                    events.append(event)
            continue
        for batch_id in batch_ids:
            try:
                event = _dead_staged_batch_event(
                    queue, instance, template, batch_id, batches,
                    results.get(batch_id) or {"ok": False,
                                              "refusal": "no answer"},
                    mover=str(ready[batch_id]["mover"]),
                    tier=str(ready[batch_id]["tier"]),
                    producer_state=producer_state, reads=reads)
            except (ProducedOutputError, OSError, ValueError) as exc:
                event = _unfiled_report(instance, f"{batch_id}.stage", {
                    "event": ORIGIN_RETIREMENT_REFUSED_EVENT,
                    "reason": f"unknown-retain: {type(exc).__name__}: {exc}"})
            if event is not None:
                events.append(event)
    return events


#: The units of work `origin_retirement_tick` asks a budget for (#1072): one
#: instance's ended prewrites, one staged batch's egress
#: (`BATCH_RETIREMENT_UNIT`), and one consumed batch's origin retirement.
ENDED_PREWRITES_UNIT = "ended-prewrites"
CONSUMED_RETIREMENT_UNIT = "consumed-origin-retirement"
ORIGIN_RETIREMENT_UNITS = (ENDED_PREWRITES_UNIT, BATCH_RETIREMENT_UNIT,
                           CONSUMED_RETIREMENT_UNIT)


class _ScopeBudget:
    """A cycle budget that sees one scope's units under the scope's own key.

    `retire_staged_batches` asks for each batch by its id; the tick's
    rotation is by scope, so the budget records a deferral under the scope.
    """

    def __init__(self, budget, key: str) -> None:
        self._budget = budget
        self._key = key

    def start(self, kind: str, _key: str) -> bool:
        return bool(self._budget.start(kind, self._key))

    def done(self, kind: str) -> None:
        self._budget.done(kind)


def origin_retirement_tick(queue, *, budget=None) -> list[dict[str, object]]:
    """Retire what the produced-output lane's ended attempts left (#914, #1053).

    Called once per `tier_loop.cycle` on the tier host. It scans the same
    produced-output scopes `unheld_window_gib` and `output_scope_tick` scan
    and reads each instance's commitments once (`_TickReads`). Then:

    * every origin-only batch committed with the ``consumed`` lifetime and
      not yet reclaimed goes to `_retire_consumed_batch`. A ``retain``
      batch is never touched, whatever became of its producer;
    * the outstanding prewrites of an attempt that ended before committing
      are swept (`_sweep_ended_prewrites`, #949), whatever the template:
      a staged template's too (#1053);
    * a staged batch of an ended attempt whose stage copy was never
      retired has its stage retirement filed (`_retire_dead_staged_batches`,
      #1053), one commitments write per instance (#1072). Its origin files
      are kept.

    It also files the attempt index pointer of any scope that has none, and
    marks the index complete once one full pass has indexed every scope it
    listed (`_backfill_attempt`).

    ``budget``, when the tier loop passes one (#1072), is asked before each
    unit of work -- an instance's ended prewrites, a staged batch's egress, a
    consumed batch's retirement -- whether it still fits this cycle
    (``budget.start(kind, key)``) and told when it ends
    (``budget.done(kind)``).  A unit it refuses is left exactly as it was
    for a later cycle, and the scopes are visited from the one whose unit
    was refused first (``budget.order``), so a backlog is worked through
    rather than retried from the top.  The scan itself still visits every
    scope: the attempt index is filed and marked complete as before.

    Returns the events to log: one ``output-origin-retired`` per retired
    batch (its ref, bytes, consumers and the origin identity each deleted
    file was checked against), ``output-dead-producer-batch-retired`` per
    stage retirement filed, and ``output-origin-retirement-stalled`` or
    ``-refused`` once per change of what holds a batch. A swept prewrite
    adds ``output-prewrite-reclaimed``, or ``output-prewrite-orphaned`` once
    per change. A tick with nothing to do and nothing new to report returns
    ``[]``. An instance whose commitments cannot be read is skipped, as the
    other scans skip it: its batches stay charged and on disk.
    """

    events: list[dict[str, object]] = []
    scopes_root = Path(queue.root) / "residency" / OUTPUT_SCOPES_SUBDIR
    try:
        owners = _scope_owners(scopes_root)
    except OSError:
        return events
    templates_root = Path(queue.root) / "residency" / OUTPUT_TEMPLATES_SUBDIR
    reads = _TickReads(queue)
    indexed = True
    # What unreleased deferred consumers will read (#913), read once and only
    # when some batch is due.  A queue that cannot say keeps every batch for
    # this tick.
    holds: set[tuple[str, str]] | None = None
    visits: list[tuple[str, Path]] = []
    for owner in owners:
        try:
            visits.extend((owner, child) for child in sorted(
                child for child in (scopes_root / owner).iterdir()
                if child.is_dir()))
        except OSError:
            indexed = False
    if budget is not None:
        # From the scope whose unit the budget refused first last cycle.
        by_key = {str(scope): (owner, scope) for owner, scope in visits}
        visits = [by_key[key] for key in budget.order(
            ORIGIN_RETIREMENT_UNITS, list(by_key))]
    for owner, scope in visits:
        indexed = _backfill_attempt(queue.root, owner, scope.name) and indexed
        nonce = scope.name.rpartition(".")[2]
        try:
            batches = reads.batches(scope)
        except ProducedOutputError:
            continue
        due = [batch_id for batch_id, entry in sorted(batches.items())
               if isinstance(entry, Mapping)
               and entry.get("origin_only") is True
               and _entry_lifetime(entry) == ORIGIN_LIFETIME_CONSUMED
               and not entry.get("origin_reclaimed")]
        try:
            outstanding = _outstanding_prewrite_ids(scope, batches)
        except OSError:
            outstanding = []
        staged = _unretired_staged_batches(batches)
        if not due and not outstanding and not staged:
            continue
        ended = (_attempt_state(reads.generation(owner), nonce)
                 if staged or not due else "")
        if not due and ended not in _ENDED_ATTEMPT_STATES:
            # Only an ended attempt's prewrites and stage copies are the
            # tick's (#949, #1053).  The scope is named for the attempt's
            # nonce, so a producer that can still commit costs no
            # instance or template read.
            continue
        try:
            instance = validate_instance(json.loads(
                (scope / "instance.json").read_text()))
            template = validate_template(json.loads(
                (templates_root / f"{instance['template_id']}.json"
                 ).read_text()))
        except (OSError, ValueError) as exc:
            event = {"event": ORIGIN_RETIREMENT_REFUSED_EVENT,
                     "batch": f"{owner}/{scope.name}",
                     "reason": f"unknown-retain: scope unreadable: {exc}"}
            signature = hashlib.sha256(json.dumps(
                event, sort_keys=True).encode()).hexdigest()
            if _UNFILED_REPORTS.get(event["batch"]) != signature:
                _UNFILED_REPORTS[event["batch"]] = signature
                events.append(event)
            continue
        if (instance_dir(queue.root, instance) != scope
                or template_sha256(template) != instance["template_sha256"]):
            continue
        scope_budget = (None if budget is None
                        else _ScopeBudget(budget, str(scope)))
        if outstanding and (scope_budget is None or scope_budget.start(
                ENDED_PREWRITES_UNIT, str(scope))):
            # An attempt that ended before committing (#949), of any
            # template (#1053).
            try:
                events.extend(_sweep_ended_prewrites(
                    queue, instance, template, outstanding, reads))
            except (ProducedOutputError, OSError, ValueError) as exc:
                event = _unfiled_report(instance, "prewrites", {
                    "event": ORIGIN_RETIREMENT_REFUSED_EVENT,
                    "reason": f"unknown-retain: {type(exc).__name__}: {exc}"})
                if event is not None:
                    events.append(event)
            finally:
                if scope_budget is not None:
                    scope_budget.done(ENDED_PREWRITES_UNIT)
        if staged and ended in _ENDED_ATTEMPT_STATES:
            events.extend(_retire_dead_staged_batches(
                queue, instance, template, staged, batches,
                producer_state=ended, reads=reads, budget=scope_budget))
        if not due:
            continue
        if holds is None:
            from . import action_edges

            try:
                holds = action_edges.held_producer_batches(queue)
            except (OSError, ProducedOutputError, ValueError):
                return events
        for batch_id in due:
            if scope_budget is not None and not scope_budget.start(
                    CONSUMED_RETIREMENT_UNIT, batch_id):
                continue
            try:
                event = _retire_consumed_batch(
                    queue, instance, template, batch_id,
                    deferred_holds=holds, reads=reads)
            except (ProducedOutputError, OSError, ValueError) as exc:
                event = _unfiled_report(instance, batch_id, {
                    "event": ORIGIN_RETIREMENT_REFUSED_EVENT,
                    "reason": f"unknown-retain: {type(exc).__name__}: {exc}"})
            finally:
                if scope_budget is not None:
                    scope_budget.done(CONSUMED_RETIREMENT_UNIT)
            if event is not None:
                events.append(event)
    if indexed and str(queue.root) not in _INDEX_MARKED:
        try:
            if not os.path.lexists(_attempts_root(queue.root)
                                   / ATTEMPT_INDEX_COMPLETE):
                _mark_attempt_index_complete(queue.root)
            _INDEX_MARKED.add(str(queue.root))
        except ProducedOutputError:
            pass
    return events


def _require_coherent_lease_sdk(lease_sdk: object):
    """The accepted SDK object, or the reason it cannot vouch.

    Production release never runs without the coherent package: ``None``,
    a stub, or a foreign copy retains. Coherence is the accepted
    ``prismabuild.reader_lease`` package itself -- same tag, same file the
    fleet imports (no bare ``reader_lease`` import, no vendored copy) --
    exposing the census and containment predicates this path calls.
    Returns ``(sdk, None)`` or ``(None, refusal)``.
    """

    if lease_sdk is None:
        return None, "unknown-retain: lease-sdk-missing"
    try:
        from prismabuild import reader_lease as installed
    except ImportError as exc:
        return None, f"unknown-retain: lease-sdk-unimportable: {exc}"
    tag = getattr(lease_sdk, "READER_LEASE_TAG", None)
    if tag != getattr(installed, "READER_LEASE_TAG", "reader-lease-v1"):
        return None, "unknown-retain: lease-sdk-incoherent-tag"
    for name in ("live_for", "containment_certificate_ok", "leases_root"):
        if not callable(getattr(lease_sdk, name, None)):
            return None, f"unknown-retain: lease-sdk-missing-{name}"
    own_file = getattr(lease_sdk, "__file__", None)
    installed_file = getattr(installed, "__file__", None)
    try:
        same = (isinstance(own_file, str) and isinstance(installed_file, str)
                and os.path.realpath(own_file) == os.path.realpath(installed_file))
    except OSError:
        same = False
    if not same:
        return None, "unknown-retain: lease-sdk-foreign-package"
    return lease_sdk, None


def _dir_has_entries(path: Path) -> bool:
    """Whether a directory holds any entry; missing reads as empty.

    A stat/permission failure is unknown state that retains, never silent
    absence: `Path.is_dir/is_file` answer False on errors, which would
    prove nothing about the bytes the accounting vouches.
    """

    try:
        with os.scandir(path) as iterator:
            for _ in iterator:
                return True
            return False
    except FileNotFoundError:
        return False
    except (OSError, ValueError) as exc:
        raise ProducedOutputError(f"unknown-retain: {exc}") from None


def _lease_pin_files(lease_sdk: object, queue, consumer: str,
                     residency_root: str | Path | None = None
                     ) -> list[str] | None:
    """Pin file names under one consumer namespace, or None when unreadable."""

    try:
        root = lease_sdk.leases_root(queue, residency_root=residency_root)
    except Exception:
        return None
    directory = Path(root) / consumer
    try:
        with os.scandir(directory) as iterator:
            return sorted(entry.name for entry in iterator
                          if entry.is_file() and entry.name.endswith(".lease.json"))
    except FileNotFoundError:
        return []
    except OSError:
        return None


def _live_output_paths(queue, lease_sdk: object,
                       batches: Mapping[str, object],
                       residency_root: str | Path
                       ) -> tuple[list[str], str | None]:
    """Live pinned paths attributable to these batches, or unknown reason.

    Traces the staged paths each batch vouches -- live fragments while
    staged, plus `staged_paths` recorded at retire time after the delete
    -- and intersects the SDK pin census over the output fragment root.
    Returns `(paths, None)`, or `([], reason)` when attribution is
    impossible: a tainted/unreadable census, unreadable fragments beside
    live paths, or live paths beside batch records predating staged-path
    recording. Never ignores an attributable live path; never blocks on
    unrelated consumers' pins.
    """

    from prismabuild import residency_map as map_mod

    wanted: set[str] = set()
    unrecorded: list[str] = []
    for batch_id, entry in batches.items():
        if not isinstance(entry, Mapping):
            continue
        # Every materialization this batch has had vouches staged paths: the
        # first publication records them on the entry at retirement, each
        # restage records its own on its materialization row. All of them are
        # attributable to this instance, so all of them are wanted.
        try:
            active = _active_materialization(entry)
            sources: list[Mapping[str, object]] = [entry]
            sources += _materializations(entry)
        except ProducedOutputError:
            unrecorded.append(str(batch_id))
            continue
        for source in sources:
            recorded = source.get("staged_paths")
            if isinstance(recorded, list):
                for path in recorded:
                    if isinstance(path, str) and path:
                        wanted.add(os.path.normpath(path))
        if active.get("retired"):
            # The copy is gone; only what its egress vouched can be pinned.
            # A record that retired before staged paths were recorded leaves
            # its paths unknowable, so live paths cannot be ruled out.
            if not isinstance(active.get("staged_paths"), list):
                unrecorded.append(str(batch_id))
            continue
        ns = entry.get("batch_namespace")
        if not isinstance(ns, str) or not ns:
            unrecorded.append(str(batch_id))
            continue
        try:
            fragments = map_mod.read_fragments(str(residency_root), ns)
        except Exception:
            unrecorded.append(str(batch_id))
            continue
        if not fragments:
            # A live materialization that has vouched no bytes yet has
            # nothing pinnable.
            continue
        try:
            composed = map_mod.compose(fragments)
        except Exception:
            unrecorded.append(str(batch_id))
            continue
        entries = composed.get("entries")
        if not isinstance(entries, Mapping):
            unrecorded.append(str(batch_id))
            continue
        for record in entries.values():
            if isinstance(record, Mapping) and record.get("stage_path"):
                wanted.add(os.path.normpath(str(record["stage_path"])))
    try:
        census = lease_sdk.live_for(queue, None, residency_root=str(residency_root))
    except Exception as exc:
        return [], f"unknown-retain: {exc}"
    owners: Mapping[str, object] = {}
    tainted: list[object] = []
    if (isinstance(census, tuple) and len(census) == 2
            and isinstance(census[0], Mapping)):
        owners, tainted = census[0], list(census[1] or [])
    elif isinstance(census, Mapping):
        owners = census
    else:
        return [], "unknown-retain: lease-census-shape"
    if tainted:
        return [], "unknown-retain: pin-census-tainted"
    live = sorted({os.path.normpath(str(path)) for path in owners} & wanted)
    if live:
        return live, None
    if unrecorded and owners:
        return [], "unknown-retain: unattributable-live-paths"
    return [], None


def safe_release_instance(queue, instance: Mapping[str, object],
                          template: Mapping[str, object],
                          lease_sdk: object = None) -> dict[str, object]:
    """Release leftover holder tokens ONLY when retirement is proven safe.

    Movers release their exact tokens through egress (`retire_batch`); this
    reclaims remainders after a fresh census under the prefix lock. EVERY
    check is exact:

    - instance + bound template readable and matching, else refusal;
    - coherent accepted SDK required (same package the fleet imports; a
      missing, foreign, or unreadable SDK retains -- never
      `sdk-absent-structural`);
    - every committed batch retired AND its mover holder empty AND its
      fragments gone, else active-batches/movers-retain (directory stats
      that fail read as unknown, never as empty);
    - funding-intent-only movers ALWAYS retain with
      `funding-intent-reconcile-retain`: a mover that never published may
      still have copied partial bytes, and metadata absence never proves
      physical absence. The funding/reconciliation lane owns these intents;
      this path never releases them;
    - funding-intent-only movers ALWAYS retain with
      `funding-intent-reconcile-retain`: a mover that never published may
      still have copied partial bytes, and metadata absence never proves
      physical absence. The funding/reconciliation lane owns these intents;
      this path never releases them;
    - live pins: pin files under the owner and every batch namespace
      retain; live census paths attributable to this instance's recorded
      staged paths retain by name; a tainted or unattributable census
      retains as unknown (unrelated consumers' pins never block);
    - owner containment: a live claim in any form retains (same attempt =
      owner-active, other attempt = owner-superseded); a corrupt or
      unreadable live claim retains as unknown and never falls through to
      an old terminal. With no live claim, the accepted SDK
      `containment_certificate_ok` over the exact owner nonce/scope must
      authorize (broker attestation + matching terminal telemetry); ANY
      bare DONE/FAILED/WITHDRAWN record by key alone is insufficient;
    - every ledger release is attempted individually: the FIRST exception
      retains (ok False), never ok True after a release failure;
    - physical files are never deleted here (no unlink of staged bytes).

    Idempotent: the orderly path releases 0 (egress already released
    exactly once); leftovers release once.
    """

    from prismabuild import pool as pool_mod

    try:
        checked_template, checked = _require_bound_contract(template, instance)
    except ProducedOutputError as exc:
        if "template-mismatch" in str(exc):
            return {"ok": False, "refusal": "template-mismatch"}
        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    sdk, sdk_refusal = _require_coherent_lease_sdk(lease_sdk)
    if sdk is None:
        return {"ok": False, "refusal": str(sdk_refusal)}
    with queue.stage_ownership_lock(str(checked["output_prefix"])):
        try:
            commitments = _read_commitments(
                _commitments_path(queue.root, checked))
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        batches = commitments["batches"]
        assert isinstance(batches, dict)
        out_base = output_fragment_root(queue.root / pool_mod.RESIDENCY)
        for batch_id, entry in batches.items():
            if not isinstance(entry, Mapping):
                return {"ok": False, "refusal": "unknown-retain: bad-batch-entry"}
            # The ACTIVE materialization decides, not the entry flag: a batch
            # that was restaged has a live successor copy even though its own
            # `retired` says the first one is gone.
            try:
                if _batch_stage_retired(entry):
                    continue
            except ProducedOutputError as exc:
                return {"ok": False, "refusal": str(exc)}
            return {"ok": False, "refusal": "active-batches-retain",
                    "batch_id": batch_id}
        for batch_id, entry in batches.items():
            assert isinstance(entry, Mapping)
            tier = str(entry.get("tier") or "")
            try:
                movers = _all_materialization_movers(entry)
            except ProducedOutputError as exc:
                return {"ok": False, "refusal": str(exc)}
            for mover in movers:
                if mover and tier:
                    try:
                        if queue.tier_ledger(tier).holder_tokens(mover):
                            return {"ok": False,
                                    "refusal": "active-movers-retain",
                                    "batch_id": batch_id}
                    except Exception as exc:
                        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            ns = str(entry.get("batch_namespace") or "")
            if ns:
                try:
                    if _dir_has_entries(out_base / ns):
                        return {"ok": False, "refusal": "active-movers-retain",
                                "batch_id": batch_id}
                except ProducedOutputError as exc:
                    return {"ok": False, "refusal": str(exc)}
        # Funding-intent-only movers: physical disposition is unknown and
        # metadata absence never proves physical absence, so every such
        # intent retains for the funding/reconciliation lane with a
        # specific recoverable refusal. No mover state (absent, withdrawn,
        # failed, or done) authorizes release here. Directory enumeration
        # names files by suffix without stat-gating: a stat failure must
        # surface at the intent read, never silently drop an intent.
        try:
            with os.scandir(_funding_dir(queue.root, checked)) as iterator:
                intent_names = sorted(entry.name for entry in iterator
                                      if entry.name.endswith(".funding.json"))
        except FileNotFoundError:
            intent_names = []
        except OSError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        for name in intent_names:
            batch_id = name[:-len(".funding.json")]
            if batch_id in batches:
                continue
            try:
                funding = _read_funding(
                    _funding_dir(queue.root, checked) / name)
            except ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            if funding is None:
                continue
            return {"ok": False,
                    "refusal": "funding-intent-reconcile-retain",
                    "batch_id": batch_id}
        # Live pins: structural per-namespace scan (owner + every batch
        # namespace, under the output fragment root where produced pins
        # live) around the SDK census over the same root. Either scan
        # finding pins retains; an unreadable scan retains as unknown.
        # Census paths attributable to this instance's recorded staged
        # paths retain by name; unattributable live paths retain as
        # unknown; unrelated consumers' pins never block this instance.
        owner = str(checked["owner_action_key"])
        attempt = checked["owner_attempt"]
        assert isinstance(attempt, dict)
        namespaces = [owner] + [
            str(entry.get("batch_namespace") or "")
            for entry in batches.values()
            if isinstance(entry, Mapping) and entry.get("batch_namespace")]
        for consumer in namespaces:
            if not consumer:
                continue
            pins = _lease_pin_files(sdk, queue, consumer,
                                    residency_root=str(out_base))
            if pins is None:
                return {"ok": False, "refusal": "unknown-retain: pin-scan"}
            if pins:
                return {"ok": False, "refusal": "live-refs-retain",
                        "pins": pins[:8]}
        live_paths, census_refusal = _live_output_paths(
            queue, sdk, batches, str(out_base))
        if census_refusal is not None:
            return {"ok": False, "refusal": census_refusal}
        if live_paths:
            return {"ok": False, "refusal": "live-refs-retain",
                    "paths": live_paths[:8]}
        for consumer in namespaces:
            if not consumer:
                continue
            pins = _lease_pin_files(sdk, queue, consumer,
                                    residency_root=str(out_base))
            if pins is None:
                return {"ok": False, "refusal": "unknown-retain: pin-scan"}
            if pins:
                return {"ok": False, "refusal": "live-refs-retain",
                        "pins": pins[:8]}
        lease_proof = "sdk-census-clean"
        # Owner containment: any live claim retains; an unreadable claim
        # retains as unknown and never falls through to an old terminal.
        # With no live claim, only the accepted SDK containment predicate
        # over the exact nonce/scope authorizes.
        try:
            live_claim = pool_mod._read_json(queue.item_path(pool_mod.CLAIMED, owner))
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if isinstance(live_claim, Mapping):
            control = live_claim.get("resource_scope")
            live_nonce = live_scope = ""
            if isinstance(control, Mapping):
                candidate = control.get("nonce")
                if isinstance(candidate, str) and candidate:
                    live_nonce = candidate
                for field in ("scope_id", "scope_unit", "unit"):
                    unit = control.get(field)
                    if isinstance(unit, str) and unit:
                        live_scope = unit
                        break
            if (live_nonce == attempt["nonce"]
                    and live_scope == attempt["scope_id"]):
                return {"ok": False, "refusal": "owner-active-retain"}
            # A live claim for another attempt (successor or stranger) is
            # never freed by this attempt's terminal: exact containment or
            # nothing.
            return {"ok": False, "refusal": "owner-superseded-retain"}
        elif live_claim is not None:
            return {"ok": False, "refusal": "unknown-retain: claim-shape"}
        try:
            contained, reason = sdk.containment_certificate_ok(queue, {
                "action_key": owner,
                "nonce": attempt["nonce"],
                "scope_id": attempt["scope_id"],
            })
        except Exception as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if not contained:
            return {"ok": False, "refusal": str(reason)}
        released = 0
        holders: set[str] = set()
        for batch_id, entry in batches.items():
            assert isinstance(entry, Mapping)
            # Holder identity comes from the validated immutable record
            # plus the canonically derived namespace -- never from mutable
            # commitments fields alone. A changed mover/namespace in
            # commitments releases nothing here.
            try:
                filed, _sealed = _load_batch_record(
                    queue.root, checked, checked_template, entry, batch_id)
            except ProducedOutputError as exc:
                return {"ok": False, "refusal": str(exc)}
            try:
                canonical_ns = batch_namespace(
                    checked, batch_id, str(filed.get("manifest_digest") or ""))
            except ProducedOutputError as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
            if (str(entry.get("batch_namespace") or "") != canonical_ns
                    or str(filed.get("batch_namespace") or "") != canonical_ns):
                return {"ok": False,
                        "refusal": "unknown-retain: batch-target-mismatch"}
            holders.add(canonical_ns)
            mover = str(filed.get("mover_key") or "")
            if len(mover) == 64:
                holders.add(mover)
            # Every restage materialization held the same window under its own
            # sealed key; a leftover there is this instance's too.
            try:
                for extra in _materializations(entry):
                    key = str(extra.get("mover_key") or "")
                    if len(key) == 64:
                        holders.add(key)
            except ProducedOutputError as exc:
                return {"ok": False, "refusal": str(exc)}
        for tier in checked_template["permitted_tiers"]:
            for holder in sorted(holders):
                try:
                    released += queue.tier_ledger(tier).release(holder)
                except Exception as exc:
                    return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        # Holders are empty post-transfer by construction; leftovers
        # release here, once, idempotently. Any release failure above
        # already retained with ok False -- this line is unreachable
        # after a release exception.
        return {"ok": True, "released": released, "lease_proof": lease_proof}


def _scope_owners(scopes_root: Path) -> list[str]:
    """Owner directories under the produced-output scopes root, sorted."""

    return sorted(p.name for p in scopes_root.iterdir() if p.is_dir())


def _owner_instance_paths(owner_dir: Path) -> list[Path]:
    """Every filed instance record of one owner, in name order.

    Bound instances live at ``<owner>/<template>.<nonce>/instance.json``; a
    flat ``<owner>/<name>.json`` other than ``commitments.json`` is the older
    spelling and still counts. Raises ``OSError`` when the owner directory
    cannot be listed.
    """

    candidates: list[Path] = []
    for child in sorted(owner_dir.iterdir()):
        if child.is_dir():
            candidate = child / "instance.json"
            if candidate.is_file():
                candidates.append(candidate)
        elif child.suffix == ".json" and child.name != "commitments.json":
            candidates.append(child)
    return candidates


def output_scope_tick(queue, tiers: Mapping[str, object]) -> list[dict[str, object]]:
    """Deterministic read-only reconciliation for the tier-loop tick.

    NEW method owned by this lane. Staged means fragments compose AND the
    mover's receipt says complete; fragments whose completeness cannot be
    read are `output-recovery-unknown`, exactly as in `recover_batches`.
    For each bound instance: compose each
    unretired batch's fragments (existing validator, one manifest each),
    report staged/retire-needed events. No publishing (liveness), no deletion
    (lease), no placement. Proposed hook: call once per `tier_loop.cycle`
    after `residency_window` and extend its events (exact diff through root).
    """

    from prismabuild import pool as pool_mod
    from prismabuild import residency_map as map_mod

    events: list[dict[str, object]] = []
    scopes_root = Path(queue.root) / "residency" / OUTPUT_SCOPES_SUBDIR
    try:
        owners = _scope_owners(scopes_root)
    except OSError:
        return events
    out_base = output_fragment_root(queue.root / pool_mod.RESIDENCY)
    for owner in owners:
        try:
            candidates = _owner_instance_paths(scopes_root / owner)
        except OSError:
            continue
        for path in candidates:
            try:
                instance = validate_instance(json.loads(path.read_text()))
            except (OSError, ValueError):
                continue
            try:
                commitments = _read_commitments(
                    _commitments_path(queue.root, instance))
            except ProducedOutputError:
                continue
            batches = commitments["batches"]
            assert isinstance(batches, dict)
            for batch_id, entry in batches.items():
                if not isinstance(entry, Mapping):
                    continue
                # The ACTIVE materialization owns the material: after a
                # restage the entry flag describes a copy that is already
                # gone, and reading it here would report a live successor as
                # nothing to reconcile.
                try:
                    active = _active_materialization(entry)
                except ProducedOutputError:
                    events.append({"event": "output-recovery-unknown",
                                   "batch_id": batch_id})
                    continue
                if active.get("retired"):
                    continue
                ns = str(entry.get("batch_namespace") or "")
                try:
                    fragments = map_mod.read_fragments(out_base, ns)
                except Exception:
                    continue
                if not fragments:
                    events.append({"event": "output-batch-unstaged",
                                   "batch_id": batch_id, "namespace": ns})
                    continue
                try:
                    composed = map_mod.compose(fragments)
                except ValueError as exc:
                    events.append({"event": "output-batch-invalid",
                                   "batch_id": batch_id, "error": repr(exc)})
                    continue
                # Same question, same answer as `recover_batches`: composing
                # fragments prove bytes landed, not that the batch did, and
                # two censuses of one lane may not disagree about which.
                complete = _mover_receipt_complete(
                    queue, str(active.get("mover_key") or ""))
                if complete is not True:
                    events.append({"event": "output-recovery-unknown",
                                   "batch_id": batch_id, "namespace": ns})
                    continue
                entries = composed.get("entries")
                events.append({
                    "event": "output-batch-staged",
                    "batch_id": batch_id, "namespace": ns,
                    "manifest_digest": str(entry.get("manifest_digest")),
                    "generation": int(active.get("generation") or 0),
                    "entries": len(entries) if isinstance(entries, Mapping) else 0,
                })
    return events


def _output_funding_verdict(queue, mover_key: str,
                            tier: str) -> tuple[str, str | None]:
    """What the FILED funding record says about one mover's fence.

    ``("spent", None)`` a parsed `consumed` record -- the pool's own
    statement that the fence is gone, and `output_funded_cover` covers only
    a record in `transferring`, so nothing can re-cover it;
    ``("fundable", None)`` `reserved`/`transferring`, still coverable;
    ``("none", None)`` a PROVEN absent file -- no prepaid intent was ever
    filed, so the ordinary claim path applies; ``("unknown", reason)``
    anything unreadable, unparsable, or disagreeing with itself.

    Read through `PoolQueue.output_funding_file_state`, never
    `read_output_funding`, because this is a census path and that method
    cannot tell absent from corrupt -- its own docstring forbids it here.
    One question, asked identically wherever a batch's claimability is
    judged, so the READY and FAILED branches cannot drift apart.
    """

    if not mover_key or not tier:
        return ("unknown", "no mover or tier on the batch entry")
    try:
        record, file_state = queue.output_funding_file_state(mover_key, tier)
    except Exception as exc:
        return ("unknown", repr(exc))
    if file_state == "corrupt":
        return ("unknown", "funding record unreadable")
    if file_state == "absent":
        return ("none", None)
    state = str(record.get("state")) if isinstance(record, Mapping) else ""
    if state in ("reserved", "transferring"):
        return ("fundable", None)
    if state == "consumed":
        return ("spent", None)
    # `released` beside a batch the commitments still call live is evidence
    # disagreeing with itself, and so is a record with no readable state.
    return ("unknown", f"funding state {state!r} on a live batch")


def _mover_receipt(queue, mover_key: str) -> Mapping[str, object] | None:
    """One mover's filed receipt, or None when there is none to read."""

    if not mover_key:
        return None
    try:
        receipt = queue.move_record(mover_key)
    except Exception:
        return None
    if not isinstance(receipt, Mapping):
        return None
    return receipt


def _receipt_complete(receipt: Mapping[str, object] | None) -> bool | None:
    """Did one already-read receipt say the batch landed whole? (3-valued)

    True/False from a filed, unrefused receipt; None when there is none to
    read or it cannot be read. Fragments cannot answer this: `stage_move`
    publishes one per entry as the bytes land and files its receipt once at
    the end, so a half-staged batch composes exactly like a whole one and a
    killed mover leaves fragments with no receipt at all. Every census in
    this lane asks the same question the same way, and a reader that needs
    the refusal too derives both from ONE observation of the record.
    """

    if receipt is None:
        return None
    if receipt.get("refusal"):
        # A filed refusal is evidence, and it is not "complete".
        return False
    return receipt.get("complete") is True


def _receipt_refusal(receipt: Mapping[str, object] | None) -> str | None:
    """The refusal one already-read receipt filed, or None when it filed none.

    The mover's own typed verdict beside completeness: `residency_moved_nothing`
    and `residency_overran_reservation` are the mover's existing refusals, and
    `origin_unreachable` is the one that says the origin directories are not on
    the tier host (#804). An absent or unreadable receipt answers None, never a
    refusal: absence is silence, and a reader must not turn it into a named
    failure.
    """

    if receipt is None:
        return None
    refusal = receipt.get("refusal")
    return str(refusal) if refusal else None


def _mover_receipt_complete(queue, mover_key: str) -> bool | None:
    """Read one mover's receipt and answer the completeness question."""

    return _receipt_complete(_mover_receipt(queue, mover_key))


def _mover_live_state(queue, mover_key: str) -> str:
    """Where one mover key is queued right now (existing queue states only)."""

    from prismabuild import pool as pool_mod

    # Every one of these rows is moved by the box that runs the mover while
    # this one polls for it, so each read revalidates before answering no
    # (#808).  That matters most for the live legs: `_publish_output_mover_row`
    # republishes on "absent", and a stale miss on `claimed` would read a row
    # that is being copied right now as absent.
    for state in (pool_mod.CLAIMED, pool_mod.READY):
        try:
            if pool_mod._read_json_fresh(
                    queue.item_path(state, mover_key)) is not None:
                return state
        except Exception:
            return "unknown"
    for state in (pool_mod.DONE, pool_mod.FAILED, pool_mod.WITHDRAWN):
        try:
            record = pool_mod._read_json_fresh(
                queue.item_path(state, mover_key))
        except Exception:
            return "unknown"
        if isinstance(record, Mapping):
            return state
    return "absent"


def due_mover_rows(queue, instance: Mapping[str, object],
                   template: Mapping[str, object]) -> list[dict[str, object]]:
    """Frozen mover-row skeletons for batches needing (re)publication.

    NEW method owned by this lane (pure preparation, no queue mutation).
    For each committed unretired batch: composing fragments under their own
    namespace need no row; a FAILED mover whose fence is still fundable
    needs a retry row; an absent mover with no staged fragments needs its
    first row. A batch whose filed funding record reads `consumed` gets no
    row at all -- see `_output_funding_verdict`.

    Composing is deliberately the test HERE, unlike in the censuses, and it
    does not mean the batch is complete: a partially staged batch composes
    too, and it is skipped because it has no publishable row, not because
    it needs nothing. Its funding is spent and its mover key is fixed by
    the batch, so any row this could emit would be unfundable; the
    disposition that batch actually needs is the terminal route
    `recover_batches` names (retire -> reclaim -> re-plan). Emitting a
    retry row for it would be inventing work the pool cannot admit.

    Each row
    carries the mover-variant residency block (`pool.validate_residency`
    accepts it: batch manifest digest + 0..total range on the batch tier
    with demand at/above the range floor) and qualified tier demand, so the
    submitter lane seals it (pbrun movement-graph machinery: real declared
    inputs/argv/manifest) through the EXISTING `queue.publish` + claim
    channel unchanged. Publication itself (behind the funding gate) stays
    with the tier loop. Deterministic order: batch_id ascending.
    """

    from prismabuild import pool as pool_mod
    from prismabuild import residency_map as map_mod
    from prismabuild import storage_tiers as tiers_mod

    checked_template = validate_template(template)
    checked_instance = validate_instance(instance)
    try:
        commitments = _read_commitments(
            _commitments_path(queue.root, checked_instance))
    except ProducedOutputError:
        return []
    batches = commitments["batches"]
    assert isinstance(batches, dict)
    out_base = output_fragment_root(queue.root / pool_mod.RESIDENCY)
    rows: list[dict[str, object]] = []
    for batch_id in sorted(batches):
        entry = batches[batch_id]
        if not isinstance(entry, Mapping):
            continue
        # The row a batch needs is its ACTIVE materialization's row. After a
        # restage the entry's own mover is spent and terminal; publishing for
        # it would be work no claim can admit, while the live successor --
        # the one that actually needs a row -- would be skipped.
        try:
            active = _active_materialization(entry)
        except ProducedOutputError:
            continue
        if active.get("retired"):
            continue
        ns = str(entry.get("batch_namespace") or "")
        mover = str(active.get("mover_key") or "")
        tier = str(active.get("tier") or "")
        manifest = str(entry.get("manifest_digest") or "")
        generation = int(active.get("generation") or 0)
        if not ns or not mover or not tier or not manifest:
            continue
        try:
            fragments = map_mod.read_fragments(out_base, ns)
            staged = bool(fragments) and bool(map_mod.compose(fragments))
        except Exception:
            staged = False
        if staged:
            continue
        state = _mover_live_state(queue, mover)
        if state in ("claimed", "ready"):
            continue
        # A batch whose fence is spent can never fund another claim on this
        # mover key, so a row emitted for it would be work the pool cannot
        # admit -- the retry-pointing-at-unclaimable-work defect in row
        # form. An unreadable fence is not proof either way, and inventing
        # work on unknown is the wrong direction for a function that
        # creates it. Both defer to the terminal route `recover_batches`
        # names; neither is a claim that the batch is finished.
        verdict, _reason = _output_funding_verdict(queue, mover, tier)
        if verdict in ("spent", "unknown"):
            continue
        class_bytes = entry.get("class_bytes")
        total = sum(int(class_bytes.get(c, 0)) for c in
                    ("payload", "checkpoint", "temp")) \
            if isinstance(class_bytes, Mapping) else 0
        kind = tiers_mod.capacity_kind_of(tier)
        try:
            gib = tiers_mod.stage_tokens_for_bytes(total) if total > 0 else 0
        except ValueError:
            gib = 0
        demand = {f"{kind}{tiers_mod.TIER_DEMAND_SEPARATOR}{tier}": gib,
                  "cpu": 1, "mem_gb": 1} if gib > 0 else {"cpu": 1, "mem_gb": 1}
        rows.append({
            "batch_id": batch_id,
            "action_key": mover,
            "tier": tier,
            "manifest_digest": manifest,
            "batch_namespace": ns,
            "resources": demand,
            "residency": {
                "schema": pool_mod.RESIDENCY_SCHEMA_V1,
                "tier_id": tier,
                "manifest_sha256": manifest,
                "manifest_bytes": total if total > 0 else 1,
                "range_start_bytes": 0,
                "range_end_bytes": total if total > 0 else 1,
            },
            "generation": generation,
            "reason": ("retry-failed-mover" if state == "failed"
                       else "needs-publish"),
        })
    return rows


def recover_batches(queue, instance: Mapping[str, object],
                    template: Mapping[str, object]) -> list[dict[str, object]]:
    """Classify every batch for deterministic recovery (read-only).

    NEW method owned by this lane. Uses existing receipts/ledgers/fragments
    only: staged (fragments compose AND the mover's receipt says complete),
    unstaged (no fragments, mover absent → due), mover-failed (FAILED with
    a fence still fundable → retry), mover-live (claimed, or ready with a
    fundable fence → wait), unfundable-retire (ready OR failed with a spent
    fence → the terminal route), unknown (unreadable scan, unreadable
    fence, or fragments whose completeness cannot be read → defer).
    Returns events in batch_id order; callers act through existing
    publish/evict paths, never here.

    A mover whose fence is spent is NOT work anyone can wait for or retry.
    `output_funded_cover` covers only a record in `transferring` and
    nothing re-funds a spent one, so once the claim has consumed the
    funding, that mover key can never be claimed again -- whether the retry
    ladder requeued the row READY or gave up on it and left it FAILED.
    BOTH branches therefore report `output-mover-unfundable-retire` with
    the terminal route the batch does have: retire it, reclaim the origin,
    and re-plan the work as a new batch. A `failed-retry` event pointing at
    a row no claim can cover is the same wait-forever defect as a
    `live-wait` one, and the two branches are decided by one shared
    question (`_output_funding_verdict`) so they cannot drift apart again.

    The verdict rests on ATTRIBUTABLE evidence and nothing weaker: the
    filed funding record, read through `output_funding_file_state` because
    this is a census path and `read_output_funding` cannot tell absent from
    corrupt (its own docstring says so). `consumed` is spent; `corrupt` is
    unknown; `absent` means no prepaid intent was ever filed, so the
    ordinary claim/retry path still applies; `released` beside an unretired
    batch is evidence disagreeing with itself. Holdings are NOT an input:
    an absent holding read is a moment, not a proof, and a mover that
    retains tokens for the bytes it staged is just as unclaimable as one
    that holds none. A CLAIMED row is never told to retire -- an executor
    owns it and the lease reaper is its recovery.
    """

    from prismabuild import pool as pool_mod
    from prismabuild import residency_map as map_mod

    checked_template = validate_template(template)
    checked_instance = validate_instance(instance)
    try:
        commitments = _read_commitments(
            _commitments_path(queue.root, checked_instance))
    except ProducedOutputError as exc:
        return [{"event": "output-recovery-unknown", "error": repr(exc)}]
    batches = commitments["batches"]
    assert isinstance(batches, dict)
    out_base = output_fragment_root(queue.root / pool_mod.RESIDENCY)
    events: list[dict[str, object]] = []
    try:
        funding_names = sorted(
            p.name for p in _funding_dir(queue.root, checked_instance).iterdir()
            if p.is_file() and p.name.endswith(".funding.json"))
    except OSError:
        funding_names = []
    for name in funding_names:
        batch_id = name[:-len(".funding.json")]
        if batch_id in batches:
            continue
        events.append({"event": "output-funding-intent-pending",
                       "batch_id": batch_id})
    for batch_id in sorted(batches):
        entry = batches[batch_id]
        if not isinstance(entry, Mapping):
            events.append({"event": "output-recovery-unknown",
                           "batch_id": batch_id})
            continue
        # Which materialization is current. A restaged batch's own `retired`
        # flag describes a copy that is already gone; reporting it as retired
        # would orphan the live successor and its window credit, and a
        # malformed materialization list is unknown, never "nothing here".
        try:
            active = _active_materialization(entry)
        except ProducedOutputError as exc:
            events.append({"event": "output-recovery-unknown",
                           "batch_id": batch_id, "error": repr(exc)})
            continue
        if str(active.get("source")) == "origin":
            # Committed at origin, never staged (#912): nothing to recover.
            events.append({"event": "output-batch-origin-only",
                           "batch_id": batch_id})
            continue
        if active.get("retired"):
            events.append({"event": "output-batch-retired",
                           "batch_id": batch_id,
                           "generation": int(active.get("generation") or 0)})
            continue
        ns = str(entry.get("batch_namespace") or "")
        mover = str(active.get("mover_key") or "")
        if (str(active.get("source")) == "materialization"
                and str(active.get("state")) == "intent"
                and _mover_live_state(queue, mover) == "absent"):
            # A filed restage intent whose mover row was never published:
            # the durable resumption point `ensure_batch_materialized` left
            # behind. It is neither staged nor lost; re-calling ensure
            # re-drives this exact sealed key.
            events.append({"event": "output-materialization-intent-pending",
                           "batch_id": batch_id, "mover": mover,
                           "generation": int(active.get("generation") or 0),
                           "action": "ensure-batch-materialized"})
            continue
        try:
            fragments = map_mod.read_fragments(out_base, ns) if ns else []
            composed = bool(fragments) and bool(map_mod.compose(fragments))
        except Exception as exc:
            events.append({"event": "output-recovery-unknown",
                           "batch_id": batch_id, "error": repr(exc)})
            continue
        # Fragments prove bytes landed; they do NOT prove the batch landed.
        # A mover that staged 700 of 1200 declared bytes composes exactly
        # like one that staged all of them, and reporting that as staged
        # tells the caller this batch needs nothing -- while its row is
        # unclaimable and its origin files are still the only copy of the
        # 500 bytes that never arrived. The mover's own receipt is what
        # says which it was, and an unreadable one leaves it unknown
        # rather than letting the fragments answer a question they cannot.
        complete = _mover_receipt_complete(queue, mover) if composed else None
        state = _mover_live_state(queue, mover) if mover else "absent"
        if composed and complete is True:
            events.append({"event": "output-batch-staged",
                           "batch_id": batch_id, "namespace": ns})
        elif composed and complete is None:
            events.append({"event": "output-recovery-unknown",
                           "batch_id": batch_id, "namespace": ns})
        elif state in ("failed", "ready"):
            verdict, reason = _output_funding_verdict(
                queue, mover, str(active.get("tier") or ""))
            if verdict == "unknown":
                event = {"event": "output-recovery-unknown",
                         "batch_id": batch_id, "mover": mover}
                if reason is not None:
                    event["error"] = reason
                events.append(event)
            elif verdict == "spent":
                # Symmetric across both branches: a spent fence is spent
                # whether the ladder requeued the row or gave up on it, and
                # a retry event pointing at work no claim can ever cover is
                # the same wait-forever defect wearing the other state.
                events.append({"event": "output-mover-unfundable-retire",
                               "batch_id": batch_id, "mover": mover,
                               "action": "retire-reclaim-replan"})
            elif state == "failed":
                events.append({"event": "output-mover-failed-retry",
                               "batch_id": batch_id, "mover": mover})
            else:
                events.append({"event": "output-mover-live-wait",
                               "batch_id": batch_id, "mover": mover})
        elif state == "claimed":
            # An executor owns this row. Whatever its ledger reads say in
            # the instant this census runs, the recovery for a claim is the
            # lease reaper's, never a retire this function names.
            events.append({"event": "output-mover-live-wait",
                           "batch_id": batch_id, "mover": mover})
        elif state == "unknown":
            events.append({"event": "output-recovery-unknown",
                           "batch_id": batch_id})
        else:
            events.append({"event": "output-batch-unstaged",
                           "batch_id": batch_id, "namespace": ns})
    return events


def checked_instance_maxima(template: Mapping[str, object]) -> dict[str, int]:
    maxima = validate_template(template)["durable_maxima"]
    assert isinstance(maxima, dict)
    return {key: int(maxima[key]) for key in maxima}


def owner_demand_terms(template: Mapping[str, object]) -> dict[str, int]:
    """Owner action tier-demand terms derived from a template (pure).

    Returns the sealed-resources form (`{kind@tier: window_gib}`) the
    consumer action carries through the EXISTING demand/admission channel
    (`pbrun --demand` → `queue.publish(resources=...)` → claim-time
    `_begin/_commit_tier_acquire` with `tier_filed` accounting). The window
    (not the corpus) is what the owner reserves pre-execution; batch movers
    are funded from it through the liveness transfer once delivered
    (per-batch exact via existing ops until then). Durable-origin class
    budget and host decode memory (`mem_gb`) stay distinct keys beside it.
    """

    from prismabuild import storage_tiers as tiers_mod

    checked = validate_template(template)
    demands = checked["working_demands"]
    assert isinstance(demands, dict)
    terms: dict[str, int] = {}
    for tier in checked["permitted_tiers"]:
        kind = tiers_mod.capacity_kind_of(tier)
        window = int(demands[tier]["window_gib"])
        if window == 0:
            # Only a write-only template has a zero window (#912): it holds
            # no stage capacity, so its owner claims none.
            continue
        terms[f"{kind}{tiers_mod.TIER_DEMAND_SEPARATOR}{tier}"] = window
    return terms


__all__ = [
    "TEMPLATE_SCHEMA_V1",
    "INSTANCE_SCHEMA_V1",
    "DESCRIPTOR_SCHEMA_V2",
    "BATCH_SCHEMA_V1",
    "BATCH_MANIFEST_SCHEMA_V1",
    "PRODUCED_OUTPUT_REF_SCHEMA_V1",
    "ORIGIN_BATCH_REF_SCHEMA_V1",
    "ORIGIN_BATCHES_ANNOTATION",
    "ORIGIN_SLOTS_ANNOTATION",
    "OUTPUT_TEMPLATES_SUBDIR",
    "OUTPUT_SCOPES_SUBDIR",
    "OUTPUT_BATCHES_SUBDIR",
    "OUTPUT_FRAGMENTS_SUBDIR",
    "OUTPUT_TEMPLATE_ATTEMPTS_SUBDIR",
    "ATTEMPT_INDEX_COMPLETE",
    "READER_HELPER_ROOT_ENV",
    "RESTAGE_FILL_ENV",
    "SDK_DEPENDENCY",
    "ARTIFACT_CLASSES",
    "ProducedOutputError",
    "mint_generation",
    "validate_template",
    "template_sha256",
    "template_path",
    "declare_template",
    "bind_instance",
    "validate_instance",
    "instance_namespace",
    "instance_dir",
    "declare_instance",
    "validate_descriptor",
    "checked_instance_maxima",
    "manifest_object_set_id",
    "canonical_expected_id",
    "label_span_for_manifest",
    "output_manifest_sha256",
    "reservation_holder",
    "output_fragment_root",
    "batch_namespace",
    "admit_instance",
    "admit_funded_window",
    "abort_prewrite",
    "owner_demand_terms",
    "reserve_working_minimum",
    "require_prewrite",
    "commit_batch",
    "commit_origin_batch",
    "origin_batch_ref",
    "load_origin_batch",
    "load_origin_batches",
    "origin_batch_manifest",
    "place_origin_batches",
    "verify_placed_origin_batches",
    "is_write_only",
    "publish_prepaid_batch",
    "refill_window",
    "unheld_window_gib",
    "build_stage_manifest",
    "retire_batch",
    "reclaim_origin",
    "batch_record",
    "batch_records",
    "BATCH_STATE_COMMITTED",
    "BATCH_STATE_RETIRING",
    "BATCH_STATE_RECLAIMED",
    "declare_origin_consumer",
    "release_origin_consumer",
    "origin_consumer_release",
    "blocked_origin_batches",
    "origin_retirement_tick",
    "ORIGIN_LIFETIME_RETAIN",
    "ORIGIN_LIFETIME_CONSUMED",
    "ORIGIN_CONSUMER_SCHEMA_V1",
    "ORIGIN_CONSUMER_RELEASE_SCHEMA_V1",
    "ORIGIN_CONSUMER_RELEASE_INDEX_SCHEMA_V1",
    "ORIGIN_RETIRED_EVENT",
    "ORIGIN_RETIREMENT_STALLED_EVENT",
    "ORIGIN_HELD_BY_UNKNOWN_EVENT",
    "ORPHANED_HOLDER_REMEDY",
    "ORIGIN_RETIREMENT_REFUSED_EVENT",
    "ORPHANED_PREWRITE_REMEDY",
    "DEAD_PRODUCER_BATCH_RETIRED_EVENT",
    "safe_release_instance",
    "output_scope_tick",
    "due_mover_rows",
    "recover_batches",
    "build_declaration",
    "declared_template",
    "bind_declared_instance",
]
