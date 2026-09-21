"""PB-produced-output staging scope: deterministic PB-owned path (R2).

An admitted GPU action writes new exact activation/cotangent/checkpoint
artifacts and reads them again during THE SAME action. Existing PB manifests
only stage pre-existing immutable external inputs. This module owns the
staging contract for produced outputs; it does not copy bytes itself
(existing movers do), creates no parallel cache/ledger/dispatcher, and never
rewrites a sealed input manifest or action key.

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
same durable charge. It adds no origin-only commit, no v2 record, no parallel
cache and no second dispatcher: it reuses the published first-publisher
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

from collections.abc import Mapping, Sequence
import hashlib
import json
import os
from pathlib import Path
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

OUTPUT_TEMPLATES_SUBDIR = "produced-output-templates"
OUTPUT_SCOPES_SUBDIR = "produced-output-scopes"
OUTPUT_BATCHES_SUBDIR = "produced-output-batches"
OUTPUT_FRAGMENTS_SUBDIR = "produced-output-fragments"

#: Sealed helper-root env name (spelling only; PB730 owns injection).
#: PQ resolves the published runtime helper from this value and verifies
#: `reader_lease.__file__` under it. Never a mutable `/repo` checkout.
READER_HELPER_ROOT_ENV = "PRISMABUILD_READER_HELPER_ROOT"

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
        "durable_maxima", "working_demands", "permitted_tiers",
    })
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ProducedOutputError(f"unknown template fields: {unknown}")
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
        window = _positive_int(spec.get("window_gib"),
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
    return {
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
        if _batch_stage_retired(entry):
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
    a shortfall below the window refuses with the typed
    tier-reservation-unavailable rather than exceeding the bound. The
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
    if tier not in checked_template["permitted_tiers"]:
        return {"ok": False, "refusal": "tier-not-permitted"}
    window = int(checked_template["working_demands"][tier]["window_gib"])
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
        if take <= 0:
            return {"ok": False, "refusal": "tier-reservation-unavailable",
                    "available": ledger.available(), "window_gib": window,
                    "held": held, "outstanding": outstanding}
        if not ledger.acquire(owner, {kind: take}):
            return {"ok": False, "refusal": "tier-reservation-unavailable",
                    "available": ledger.available(), "window_gib": window,
                    "held": held, "outstanding": outstanding}
        held = ledger.holder_tokens(owner).get(kind, 0)
    return {"ok": True, "tier": tier, "acquired": take, "held": held,
            "outstanding": outstanding, "window_gib": window,
            "kind": kind}


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
        # admission credit record on a batch update.
        try:
            previous = _read_commitments(path)
            admission = previous.get("admission")
        except ProducedOutputError:
            admission = None
    handle, temporary = tempfile.mkstemp(
        dir=str(path.parent), prefix=".commitments.")
    try:
        with os.fdopen(handle, "w") as stream:
            json.dump({"batches": dict(record["batches"]),
                       "admission": admission}, stream, sort_keys=True)
            stream.write("\n")
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
# dispatcher, no origin-only commit and no v2 record: a materialization reuses
# the published first-publisher helpers, the pool's prepaid funding, the
# existing strict SDK, the existing egress and the existing recovery.


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

    ONE stat answers both questions the commit asks -- is this file the length
    the descriptor claims, and what exactly is this file -- so the size that is
    checked and the identity that is recorded can never be two different
    moments. `os.lstat` (not `stat`) matches the check this path has always
    made and is strictly stronger for the proof: replacing a regular file with
    a symlink to identical bytes changes the recorded identity and refuses.
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


def _recheck_origin_identity(filed: Mapping[str, object],
                             sealed: list[dict[str, object]]
                             ) -> tuple[bool, str | None]:
    """Do the sealed origins still have the identity the FIRST commit recorded?

    This is the whole safety of restaging a DEV null-digest batch. The
    descriptor carries no payload digest, so the only thing that can say the
    bytes about to be copied a second time are the bytes the batch was
    committed over is the file identity captured when it was committed --
    `(ino, size, mtime_ns, ctime_ns)` through the same
    `reader_lease.file_id_matches` every strict reader and every publication
    proof uses. Size alone is NOT that proof: a rewritten file of identical
    length passes an lstat size check and is a different artifact.
    Deliberately NOT a rehash: the writer digest, where one exists, already
    rides the manifest and the mover verifies it on copy; this path adds no
    payload read.

    A batch committed before the proof existed has no `origin_identity` and
    refuses `restage-origin-proof-missing` -- the current bytes are never
    retroactively blessed as the committed ones. An unstatable path is unknown
    and refuses. Returns `(True, None)` or `(False, refusal)`.
    """

    from prismabuild import reader_lease as lease_mod

    recorded = filed.get("origin_identity")
    if not isinstance(recorded, Mapping) or not recorded:
        return (False, "restage-origin-proof-missing")
    if len(recorded) != len(sealed):
        return (False, "restage-origin-proof-missing")
    for desc in sealed:
        path = str(desc["path"])
        published = recorded.get(path)
        if not isinstance(published, Mapping):
            return (False, "restage-origin-proof-missing")
        # A proof that disagrees with the manifest it is filed beside is not a
        # proof of anything.
        if published.get("size") != int(desc["bytes"]):
            return (False, "restage-origin-proof-missing")
        try:
            live = _portable_identity_of(os.lstat(path))
        except OSError as exc:
            return (False, f"restage-origin-unstatable: {exc}")
        if not lease_mod.file_id_matches(published, live):
            return (False, "restage-origin-changed")
    return (True, None)


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
    * the sealed origins still carry the identity the first commit recorded.

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
    ok, refusal = _recheck_origin_identity(filed, sealed)
    if not ok:
        raise ProducedOutputError(str(refusal))
    return dict(entry)


def _append_materialization_locked(
        queue, checked_instance, checked_template, batch_id: str, *,
        mover_key: str, tier: str, generation: int, host: str) -> bool:
    """File one restage INTENT under the caller's ownership lock.

    This is the durable resumption point, and it is filed BEFORE any funding
    or movement side effect reaches the pool: a crash at any later prefix
    finds this row, re-drives the SAME sealed mover key, and allocates neither
    a fresh generation nor a second credit.

    Re-validates provenance exactly as the primary commit path does (the
    immutable record loads; the entry still agrees on mover/tier/namespace;
    the first materialization is stage-retired with its origin charge intact),
    re-derives the generation from the record on disk, and appends
    `{mover_key, tier, generation, retired: False, state: "intent", host}`.
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
    updated = dict(entry)
    updated["materializations"] = existing + [{
        "mover_key": str(mover_key), "tier": tier,
        "generation": int(generation), "retired": False,
        "state": "intent", "host": str(host)}]
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
        queue, checked_instance, batch_id: str, *, mover_key: str,
        receipt: Mapping[str, object], canonical_ns: str,
        staged_paths: list[str]) -> None:
    """File one materialization's stage retirement under the caller's lock.

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
    for index, item in enumerate(items):
        if str(item.get("mover_key")) == str(mover_key):
            if item.get("retired"):
                return
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
    try:
        _write_commitments(path, {"batches": batches})
    except ProducedOutputError as exc:
        raise ProducedOutputError(f"unknown-retain: {exc}") from None


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
    retirement, which evicts the staged copy. Then files an immutable
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
    mover = _hex64(mover_key, where="batch mover_key")
    if tier not in checked_template["permitted_tiers"]:
        return {"ok": False, "refusal": "tier-not-permitted"}
    sealed = [validate_descriptor(d, checked_template, checked_instance)
              for d in descriptors]
    # One lstat per origin, answering both questions at the same instant: the
    # size the descriptor claims, and the exact file identity this commit is
    # over. No payload reread -- digests ride the writer receipt and the mover
    # verifies on copy. The identity tuple is what lets the SAME batch be
    # materialized again later over provably the same bytes (see
    # `ensure_batch_materialized`): for a DEV null-digest descriptor it is the
    # only such proof, and a size check alone would bless a rewritten file of
    # equal length.
    origin_identity, identity_refusal = _origin_identity_at_commit(sealed)
    if identity_refusal is not None:
        return identity_refusal
    assert origin_identity is not None
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


def _seal_output_mover(queue, checked_instance: Mapping[str, object],
                       checked_template: Mapping[str, object],
                       descriptors: list[Mapping[str, object]], *,
                       batch_id: str, tier: str, cas_root,
                       producer_action_key: str | None,
                       command_extra: Sequence[str] = (),
                       retry_policy: Mapping[str, object] | None = None,
                       generation: int = 0) -> dict[str, object]:
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
    supply as a nonce. `generation == 0` seals byte-for-byte what this lane
    has always sealed, once the producer's own record is carried: the child's
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

    Returns the sealed facts (`mover_key`, `action`, `host`, `kind`, `gib`,
    `total`, `manifest_digest`, `batch_namespace`, `retry_policy`, `cas`) with
    the request already filed in CAS, or a typed `{"ok": False, "step", ...}`.
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
    command = [mover_python, mover_tool,
               "--pool-root", str(queue.root),
               "--cas-root", str(cas.root),
               "--consumer-action-key", batch_ns,
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
            demand={"cpu": 1, "mem_gb": 1, f"{kind}@{tier}": gib},
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
            "kind": kind, "gib": gib, "total": total,
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
                              retry_policy: Mapping[str, object]
                              ) -> dict[str, object]:
    """Publish one output mover's READY row through the EXISTING channel.

    Ordinary `queue.publish` semantics carried from the parent: the producer's
    worker script and checkout addressing, its validation priority, the
    effective sealed mover retry policy, the tier's placement tag, qualified
    tier demand, and the mover-variant residency block. One spelling for first
    publication and restage. A row already published for this content-
    addressed key is a typed duplicate, not a conflict: the sealed key IS the
    identity, so republishing the same key is the resume path, never a second
    unit of work.
    """

    from prismabuild import pool as pool_mod

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
            resources={"cpu": 1, "mem_gb": 1, f"{kind}@{tier}": gib},
            residency={"schema": pool_mod.RESIDENCY_SCHEMA_V1,
                       "tier_id": tier,
                       "manifest_sha256": manifest_digest,
                       "manifest_bytes": total,
                       "range_start_bytes": 0, "range_end_bytes": total})
    except pool_mod.PoolContractError as exc:
        return {"ok": False, "step": "publish", "refusal": str(exc)}
    return {"ok": True, "published": True, "state": "ready"}


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
    return {
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
        "mover_receipt_complete": _mover_receipt_complete(queue, mover),
        "mover_queue_state": _mover_live_state(queue, mover) if mover else "absent",
    }


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

    There is no origin-only commit, no second schema, no parallel cache and no
    second dispatcher: the caller supplies neither origins, nor tokens, nor a
    successor id. The descriptors come from the immutable batch record, the
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
      than blessing whatever bytes are there now;
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
    """

    try:
        checked_template, checked_instance = _require_bound_contract(
            template, instance)
    except ProducedOutputError:
        return {"ok": False, "step": "validate", "refusal": "template-mismatch"}
    _name(batch_id, where="batch_id")
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
            ok, refusal = _recheck_origin_identity(filed, sealed)
            if not ok:
                return {"ok": False, "step": "origin", "refusal": str(refusal)}
            generation = len(mats) + 1
            sealed_mover = _seal_output_mover(
                queue, checked_instance, checked_template, sealed,
                batch_id=batch_id, tier=tier, cas_root=cas_root,
                producer_action_key=producer_action_key,
                command_extra=command_extra, retry_policy=retry_policy,
                generation=generation)
            if not sealed_mover.get("ok"):
                return sealed_mover
            mover = str(sealed_mover["mover_key"])
            host = str(sealed_mover["host"])
            kind = str(sealed_mover["kind"])
            gib = int(sealed_mover["gib"])
            mover_retry_policy = dict(sealed_mover["retry_policy"])
            mover_cas_root = str(sealed_mover["cas_root"])
            try:
                _append_materialization_locked(
                    queue, checked_instance, checked_template, batch_id,
                    mover_key=mover, tier=tier, generation=generation,
                    host=host)
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
            if str(resume.get("state")) != "funded":
                ok, refusal = _recheck_origin_identity(filed, sealed)
                if not ok:
                    return {"ok": False, "step": "origin",
                            "refusal": str(refusal)}
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
        retry_policy=mover_retry_policy)
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
                               batch_id: str, receipt: Mapping[str, object],
                               staged_paths: list[str]) -> None:
    """File stage retirement under the caller's ownership lock.

    Proof-checked, never a bare flag: the receipt must be a complete
    egress for this exact batch (mover + namespace match, no errors),
    and the batch must still be unretired. Records the staged paths the
    egress vouched so later census attribution can name them.
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
    if entry.get("retired"):
        return
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
    _write_commitments(path, {"batches": batches})


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
    tier with no announced record or host keeps the in-process egress, whose
    own stage-root check answers for it. `retire_batch` adds one condition:
    the in-process egress also needs the fleet tool importable, and an owner
    that cannot import it takes the tier-host route wherever it runs.
    """

    import socket

    host = str(record.get("host") or "") if isinstance(record, Mapping) else ""
    return not host or host == socket.gethostname()


def _seal_output_egress(queue, *, record: Mapping[str, object],
                        producer: str, cas_root, batch_id: str,
                        generation: int, target_mover: str, consumer: str,
                        stage_root: str, residency_root: str
                        ) -> dict[str, object]:
    """Seal (never file) the egress action for one materialization.

    The egress node `pbrun` seals for a consumer's staged range, sealed the
    same way for a produced batch: `stage_release.py` off the ANNOUNCED TIER
    RECORD, placed on the box that owns the stage, one CPU and one GiB, and no
    tier demand -- an egress returns capacity, and one that had to reserve
    some before giving any back would deadlock exactly when the stage is full.
    It carries no data manifest, so nothing prewarms for it.

    The key is content-addressed over a command naming the materialization's
    mover and the batch's namespace, so it is unique per materialization and
    is re-derived by every call instead of being recorded anywhere.
    """

    from prismabuild import movement_actions

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
    command = [mover_python, egress_tool,
               "--pool-root", str(queue.root),
               "--mover-action-key", target_mover,
               "--consumer-action-key", consumer,
               "--stage-root", str(stage_root),
               "--residency-root", str(residency_root)]
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
        consumer=consumer, stage_root=stage_root,
        residency_root=residency_root)
    if not sealed.get("ok"):
        return {"answer": {"ok": False, "step": sealed.get("step"),
                           "refusal": "unknown-retain: egress-seal: "
                                      f"{sealed.get('refusal')}"}}
    egress_key = str(sealed["egress_key"])
    state = _mover_live_state(queue, egress_key)
    if state == "unknown":
        return {"answer": {"ok": False, "egress_action_key": egress_key,
                           "refusal": "unknown-retain: egress-row-unreadable"}}
    if state in (pool_mod.READY, pool_mod.CLAIMED):
        return deferred(OWN_EGRESS_IN_FLIGHT, egress_key, state)
    filed: dict[str, object] | None = None
    if state != "absent":
        # `Pool._file_move` files a node's receipt under the node's OWN key,
        # so an egress receipt is read by the egress key and names it; the
        # mover it retired is bound by that key, which hashes a command
        # naming it. The namespace must still be this batch's.
        try:
            candidate = queue.move_record(egress_key)
        except Exception:
            candidate = None
        if (isinstance(candidate, Mapping)
                and str(candidate.get("action_key") or "") == egress_key
                and str(candidate.get("consumer_action_key") or "")
                == consumer):
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
            recompute=True)
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
    select and capture under the output-prefix ownership lock; run the
    existing egress with NO lock of this lane held; reacquire, revalidate
    the exact same manifest/mover/generation, and file `retired` for a
    complete error-free receipt naming that mover and namespace. The egress
    takes the mover transition lock and then the stage root's ownership lock,
    so running it underneath this instance's ownership lock would nest two
    locks of one family with a blocking transition wait between them; nothing
    needs that, because the materialization stays unretired for the whole
    window and therefore keeps refusing both a second writer over its origins
    and any successor materialization.

    Durable-origin quota is NOT freed here -- origin files still exist; see
    `reclaim_origin`. Charge (durable) and window (tier) accounting
    stay distinct at every step, and a failed or partial egress files
    nothing and leaves the old materialization holding its own credit.
    """

    from prismabuild import core as core_mod
    from prismabuild import pool as pool_mod

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
        # Provenance BEFORE any destructive call: the immutable record
        # validates (schema/binding/entries/manifest), and the mutable
        # commitments entry must agree with it on mover, tier, and the
        # canonically derived namespace. A changed mover/tier in
        # commitments never selects the egress target. Bad or foreign
        # metadata refuses before fragments are read, files deleted, or
        # tier ownership released.
        try:
            filed, _sealed = _load_batch_record(
                queue.root, checked_instance, checked_template,
                entry, batch_id)
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": str(exc)}
        mover = str(filed.get("mover_key") or "")
        tier = str(filed.get("tier") or "")
        try:
            canonical_ns = batch_namespace(
                checked_instance, batch_id,
                str(filed.get("manifest_digest") or ""))
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if (len(mover) != 64
                or str(entry.get("mover_key") or "") != mover
                or str(entry.get("tier") or "") != tier
                or tier not in checked_template["permitted_tiers"]
                or str(entry.get("batch_namespace") or "") != canonical_ns
                or str(filed.get("batch_namespace") or "") != canonical_ns):
            return {"ok": False, "refusal": "unknown-retain: batch-target-mismatch"}
        # WHICH copy is being retired: the batch's first materialization, or
        # a restaged successor that now owns the material under the same
        # canonical namespace. `entry["retired"]` answers only for the first,
        # so the active materialization is what selects the egress target --
        # otherwise a restaged batch reports a duplicate retirement and
        # orphans a live stage copy plus its window credit.
        try:
            active = _active_materialization(entry)
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": str(exc)}
        if active.get("retired"):
            return {"ok": True, "batch_id": batch_id, "duplicate": True}
        target_mover = str(active.get("mover_key") or "")
        if len(target_mover) != 64 or str(active.get("tier") or "") != tier:
            return {"ok": False,
                    "refusal": "unknown-retain: batch-target-mismatch"}
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
                    return {"ok": False,
                            "refusal": "unknown-retain: fragment-entries"}
                for record in entries.values():
                    if isinstance(record, Mapping) and record.get("stage_path"):
                        staged_paths.append(os.path.normpath(
                            str(record["stage_path"])))
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        except (OSError, ValueError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        selected_manifest = str(filed.get("manifest_digest") or "")
        selected_source = str(active.get("source") or "")
        selected_generation = int(active.get("generation") or 0)
        # Paths an earlier call of this retirement filed for this same copy:
        # a tier-host egress drops the fragment between two calls, so the
        # call that files the retirement may find nothing left to read.
        recorded = active.get("staged_paths")
        recorded_paths = {os.path.normpath(path) for path in recorded
                          if isinstance(path, str) and path} if isinstance(
                              recorded, list) else set()
        staged_paths = sorted(set(staged_paths) | recorded_paths)
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
                    selected_source, target_mover, staged_paths)
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
            batch_id=batch_id, generation=selected_generation,
            target_mover=target_mover, consumer=consumer,
            stage_root=str(stage_root), residency_root=str(residency_root))
        if "answer" in routed:
            answer = routed["answer"]
            assert isinstance(answer, dict)
            return answer
        receipt = routed["receipt"]
        assert isinstance(receipt, dict)
    with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
        # Revalidate the EXACT selection before filing anything: the record
        # still loads, the batch still resolves to the same manifest, and the
        # active materialization is still the generation whose copy this
        # receipt just deleted. A selection that moved underneath the egress
        # is unknown state and retains.
        try:
            commitments = _read_commitments(
                _commitments_path(queue.root, checked_instance))
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        batches = commitments["batches"]
        assert isinstance(batches, dict)
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
        if (str(filed.get("manifest_digest") or "") != selected_manifest
                or str(active.get("mover_key") or "") != target_mover
                or str(active.get("source") or "") != selected_source
                or int(active.get("generation") or 0) != selected_generation):
            return {"ok": False,
                    "refusal": "unknown-retain: materialization-changed"}
        if active.get("retired"):
            return {"ok": True, "batch_id": batch_id, "duplicate": True,
                    "receipt": receipt, "mover_key": target_mover,
                    "generation": selected_generation}
        try:
            if selected_source == "materialization":
                _mark_materialization_retired_locked(
                    queue, checked_instance, batch_id,
                    mover_key=target_mover, receipt=receipt,
                    canonical_ns=canonical_ns, staged_paths=staged_paths)
            else:
                _mark_batch_retired_locked(
                    queue, checked_instance, checked_template, batch_id,
                    receipt, staged_paths)
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    return {"ok": True, "batch_id": batch_id, "receipt": receipt,
            "mover_key": target_mover,
            "generation": selected_generation,
            "staged_paths": sorted(set(staged_paths))}


def reclaim_origin(queue, instance: Mapping[str, object],
                   template: Mapping[str, object], *,
                   batch_id: str) -> dict[str, object]:
    """Free durable-origin quota after proving every origin path absent.

    Stage retirement frees the tier window only. Each committed batch keeps
    charging its payload/checkpoint/temp classes until this call stats
    every sealed entry path and finds all of them absent (producer-side
    disposal; this lane never unlinks origin files). A present file
    refuses `origin-present-retain` keeping the charge; an unstatable
    path retains unknown. Exactly-once: already-reclaimed returns
    `{"ok": True, "reclaimed": False}`.
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
        owners = sorted(p.name for p in scopes_root.iterdir() if p.is_dir())
    except OSError:
        return events
    out_base = output_fragment_root(queue.root / pool_mod.RESIDENCY)
    for owner in owners:
        owner_dir = scopes_root / owner
        candidates: list[Path] = []
        try:
            for child in sorted(owner_dir.iterdir()):
                if child.is_dir():
                    # Bound instances live at <owner>/<template>.<nonce>/instance.json.
                    candidate = child / "instance.json"
                    if candidate.is_file():
                        candidates.append(candidate)
                elif child.suffix == ".json" and child.name != "commitments.json":
                    candidates.append(child)
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


def _mover_receipt_complete(queue, mover_key: str) -> bool | None:
    """Did this mover's own receipt say the batch landed whole? (3-valued)

    True/False from a filed, unrefused receipt; None when there is none to
    read or it cannot be read. Fragments cannot answer this: `stage_move`
    publishes one per entry as the bytes land and files its receipt once at
    the end, so a half-staged batch composes exactly like a whole one and a
    killed mover leaves fragments with no receipt at all. Every census in
    this lane asks the same question the same way.
    """

    if not mover_key:
        return None
    try:
        receipt = queue.move_record(mover_key)
    except Exception:
        return None
    if not isinstance(receipt, Mapping):
        return None
    if receipt.get("refusal"):
        # A filed refusal is evidence, and it is not "complete".
        return False
    return receipt.get("complete") is True


def _mover_live_state(queue, mover_key: str) -> str:
    """Where one mover key is queued right now (existing queue states only)."""

    from prismabuild import pool as pool_mod

    for state in (pool_mod.CLAIMED, pool_mod.READY):
        try:
            if pool_mod._read_json(queue.item_path(state, mover_key)) is not None:
                return state
        except Exception:
            return "unknown"
    for state in (pool_mod.DONE, pool_mod.FAILED, pool_mod.WITHDRAWN):
        try:
            record = pool_mod._read_json(queue.item_path(state, mover_key))
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
        terms[f"{kind}{tiers_mod.TIER_DEMAND_SEPARATOR}{tier}"] = window
    return terms


__all__ = [
    "TEMPLATE_SCHEMA_V1",
    "INSTANCE_SCHEMA_V1",
    "DESCRIPTOR_SCHEMA_V2",
    "BATCH_SCHEMA_V1",
    "BATCH_MANIFEST_SCHEMA_V1",
    "PRODUCED_OUTPUT_REF_SCHEMA_V1",
    "OUTPUT_TEMPLATES_SUBDIR",
    "OUTPUT_SCOPES_SUBDIR",
    "OUTPUT_BATCHES_SUBDIR",
    "OUTPUT_FRAGMENTS_SUBDIR",
    "READER_HELPER_ROOT_ENV",
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
    "publish_prepaid_batch",
    "refill_window",
    "build_stage_manifest",
    "retire_batch",
    "reclaim_origin",
    "safe_release_instance",
    "output_scope_tick",
    "due_mover_rows",
    "recover_batches",
    "build_declaration",
    "declared_template",
    "bind_declared_instance",
]
