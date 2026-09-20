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
ledger pool is held beside movers. The general funded-window admission
(current+next need) is the liveness lane's funded-claim primitive:
`admit_funded_window` validates the request fully, then refuses
`funding-primitive-pending` naming that exact dependency until it is
delivered. No second ledger here, never subtracts unrelated holders'
tokens.

PB730 owns: corrected `pin_id_for` (canonical object set), the additive
owner/material-namespace SDK contract, the containment writer, and the
immutable helper-env injection. This lane does not edit `reader_lease.py`,
does not invent pin serialization (ours is the manifest object set only),
and returns the exact SDK dependency instead of a stub.
"""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
import os
from pathlib import Path
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

OUTPUT_TEMPLATES_SUBDIR = "produced-output-templates"
OUTPUT_SCOPES_SUBDIR = "produced-output-scopes"
OUTPUT_BATCHES_SUBDIR = "produced-output-batches"
OUTPUT_FRAGMENTS_SUBDIR = "produced-output-fragments"

#: Sealed helper-root env name (spelling only; PB730 owns injection).
#: PQ resolves the published runtime helper from this value and verifies
#: `reader_lease.__file__` under it. Never a mutable `/repo` checkout.
READER_HELPER_ROOT_ENV = "PRISMABUILD_READER_HELPER_ROOT"

#: Exact SDK + funding dependencies until their lanes land (not stubs).
SDK_DEPENDENCY = (
    "PB730 additive owner/material-namespace SDK contract "
    "(acquire/open/release binding material under the batch namespace to "
    "the registered OWNER attempt; pin files under the owner, proof "
    "resolves in the material namespace) + corrected pin_id_for including "
    "the canonical expected object set and material generations "
    "(candidate pin 2637a9d0f7, R7-returned: auto-cleanup paths excluded); "
    "LIVENESS funded-claim primitive (funding record binding credit to "
    "exact tier/plan-window/mover/range/generation, eligible-token "
    "verification, serialized transfer without free interval, window "
    "admission covering current+next need) for the general window path; "
    "liveness `admit_funded_window` refuses funding-primitive-pending "
    "until that API is delivered"
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
    row means the owner is not running. Either way `commit_batch` -- the
    mutation that consumes durable quota and moves tier tokens -- must
    not run. A corrupt or unreadable live row is unknown state that
    retains rather than authorizing. The duplicate-commit replay path
    runs before this check (it mutates nothing); reservation-only
    (`require_prewrite`) and cleanup paths (abort, retire, release,
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
    digest = _hex64(value.get("sha256"), where="descriptor sha256")
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
    within minted tier capacity). Funding itself belongs to the liveness
    funded-claim primitive; this probe reports its delivery state read-only
    (liveness draft names are read, never called and never frozen here):
    absent → `funding-primitive-pending` with the exact dependency.
    Per-batch exact physical funding at `commit_batch` (existing ledger
    acquire + whole transfer) is unaffected: it funds one amount, not a
    window. Returns {"ok": False, ...} in all current states.
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
    delivered = [name for name in ("reserve_fence", "transfer_fence",
                                   "funded_cover")
                 if callable(getattr(pool_mod.PoolQueue, name, None))]
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
    uncharged temp/checkpoint writes refuse here. `paths` names the exact
    durable-origin files this batch will write (absolute, normalized, under
    the template prefix, distinct); the commit must present descriptors for
    exactly this set, and abort requires every one absent. Uncommitted does
    NOT mean unwritten: files without a batch never enter staged accounting.
    Checks the bound admission record (binding metadata, never funding) +
    durable headroom for the planned class bytes, and files an immutable
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
        directory = _prewrites_dir(queue.root, checked_instance)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{batch_id}.prewrite.json"
        record = {"batch_id": batch_id, "tier": tier, "class_bytes": planned,
                  "paths": planned_paths,
                  "owner_action_key": str(checked_instance["owner_action_key"]),
                  "owner_attempt": dict(checked_instance["owner_attempt"])}
        raw = json.dumps(record, sort_keys=True,
                         separators=(",", ":")).encode() + b"\n"
        try:
            from prismabuild import pool as pool_mod
            pool_mod._publish_immutable(path, raw, where="produced-output prewrite")
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
    # lstat size check only (no payload reread; digests ride the writer receipt
    # and the mover verifies on copy).
    for desc in sealed:
        try:
            if os.lstat(desc["path"]).st_size != int(desc["bytes"]):
                return {"ok": False, "refusal": "descriptor-size-mismatch",
                        "path": desc["path"]}
        except OSError as exc:
            return {"ok": False, "refusal": f"descriptor-unstatable: {exc}"}
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
        if (prewrite.get("tier") != tier
                or dict(prewrite.get("class_bytes", {})) != class_bytes
                or sorted(str(d["path"]) for d in sealed)
                != sorted(prewrite.get("paths", []))
                or prewrite.get("owner_action_key")
                != checked_instance["owner_action_key"]
                or dict(prewrite.get("owner_attempt", {})) != dict(
                    checked_instance["owner_attempt"])):
            return {"ok": False, "refusal": "prewrite-mismatch"}
        try:
            sums = _class_sums(batches)
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        maxima = checked_instance_maxima(checked_template)
        for cls in sums:
            if sums[cls] + class_bytes[cls] > maxima[f"{cls}_max_bytes"]:
                return {"ok": False, "refusal": f"commit-exceeds-{cls}-maxima"}
        # Durable funding intent covering acquire→transfer→publication:
        # exact token identity (tier/mover/range/generation) mirroring the
        # liveness funded-claim record shape for drop-in migration. Filed
        # only once the ledger holds the tokens and always before transfer,
        # so every crash point resumes from observed holdings + intent
        # instead of re-acquiring the same budget. All-or-nothing is proven
        # by the resume paths, never assumed from the success path.
        # Unknown occupancy (tokens neither holder names) is never released
        # to repair bookkeeping: it retains.
        funding_path = (_funding_dir(queue.root, checked_instance)
                        / f"{batch_id}.funding.json")
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
        ledger = queue.tier_ledger(tier)
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
            # Intent names allocated tokens only: nothing is recorded before
            # the ledger holds it, so a crash before this line retries clean
            # and a crash after it resumes from the record.
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
            "object_set_id": manifest_object_set_id({
                f"{d['bytes']}:{d['path']}": {"bytes": int(d["bytes"]),
                                             "sha256": str(d["sha256"])}
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
            "retired": False,
            "origin_reclaimed": False,
        }
        _write_commitments(_commitments_path(queue.root, checked_instance),
                           {"batches": batches})
        # Funding intent + prewrite consumed only here, after the durable
        # batch publication: a crash anywhere above resumes from the intent,
        # never by re-acquiring the same budget.
        _delete_funding(funding_path)
        (_prewrites_dir(queue.root, checked_instance)
         / f"{batch_id}.prewrite.json").unlink(missing_ok=True)
    return {"ok": True, "batch_id": batch_id, "batch_namespace": batch_ns,
            "manifest_digest": manifest_digest, "class_bytes": class_bytes,
            "mover_key": mover, "tier": tier, "entries": sealed}


def build_stage_manifest(batch: Mapping[str, object],
                         mount_prefix: str) -> dict[str, object]:
    """Synthetic data manifest for `stage_move.move` (one mover per batch)."""

    entries = batch.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ProducedOutputError("batch carries no entries to stage")
    manifest_entries = [
        {"path": str(e["path"]), "offset": 0, "bytes": int(e["bytes"]),
         "sha256": str(e["sha256"])} for e in entries]
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


def _mark_batch_retired_locked(queue, checked_instance: Mapping[str, object],
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
    # record (exact sealed descriptors), cross-checked against both the
    # commitments entry and this instance: neither a caller dict nor a
    # bare commitments flag can retire another attempt's batch.
    batch_file = (Path(queue.root) / "residency" / OUTPUT_BATCHES_SUBDIR
                  / instance_namespace(checked_instance)
                  / f"{batch_id}.json")
    try:
        with open(batch_file, "rb") as handle:
            filed = json.loads(handle.read(4 * 1024 * 1024 + 1).decode())
    except FileNotFoundError:
        raise ProducedOutputError(
            "unknown-retain: batch-record-missing") from None
    except (OSError, ValueError, UnicodeDecodeError) as exc:
        raise ProducedOutputError(f"unknown-retain: {exc}") from None
    if not isinstance(filed, Mapping):
        raise ProducedOutputError("unknown-retain: batch-record-missing")
    if (str(filed.get("owner_action_key") or "")
            != str(checked_instance["owner_action_key"])
            or dict(filed.get("owner_attempt") or {}) != dict(
                checked_instance["owner_attempt"])
            or str(filed.get("manifest_digest") or "")
            != str(entry.get("manifest_digest") or "")):
        raise ProducedOutputError("retire owner/attempt/manifest mismatch")
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


def retire_batch(queue, instance: Mapping[str, object],
                 template: Mapping[str, object], batch_id: str, *,
                 stage_root: str, residency_root: str | Path
                 ) -> dict[str, object]:
    """Evict one batch's staged files, then retire its stage window.

    Retirement is tied to the actual egress: the batch record is loaded
    from commitments (exact batch/manifest/attempt, never a caller
    dict), the staged paths are captured from the live fragments before
    the delete, and `retired` is filed under the output-prefix ownership
    lock only for a complete error-free receipt naming this mover and
    namespace. Durable-origin quota is NOT freed here -- origin files
    still exist; see `reclaim_origin`. Charge (durable) and window
    (tier) accounting stay distinct at every step.
    """

    import stage_release

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
        if entry.get("retired"):
            return {"ok": True, "batch_id": batch_id, "duplicate": True}
        consumer = str(entry.get("batch_namespace") or "")
        mover = str(entry.get("mover_key") or "")
        if len(consumer) != 64 or len(mover) != 64:
            return {"ok": False, "refusal": "bad-batch-namespace"}
        # Capture the staged paths the egress is about to vouch while the
        # fragments still exist; after the delete only this record names
        # them for later live-path attribution.
        staged_paths: list[str] = []
        try:
            from prismabuild import residency_map as map_mod

            out_base = output_fragment_root(queue.root / pool_mod.RESIDENCY)
            fragments = map_mod.read_fragments(out_base, consumer)
            if fragments:
                composed = map_mod.compose(fragments)
                entries = composed.get("entries")
                if isinstance(entries, Mapping):
                    for record in entries.values():
                        if isinstance(record, Mapping) and record.get("stage_path"):
                            staged_paths.append(os.path.normpath(
                                str(record["stage_path"])))
        except Exception:
            staged_paths = []
        receipt = stage_release.evict(
            queue, mover, consumer_action_key=consumer,
            stage_root=str(stage_root), residency_root=str(residency_root))
        if not receipt.get("complete"):
            return {"ok": False, "refusal": "egress-incomplete",
                    "receipt": receipt}
        try:
            _mark_batch_retired_locked(
                queue, checked_instance, batch_id, receipt, staged_paths)
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    return {"ok": True, "batch_id": batch_id, "receipt": receipt,
            "staged_paths": sorted(set(staged_paths))}


def mark_batch_retired(queue, instance: Mapping[str, object],
                       template: Mapping[str, object], batch_id: str, *,
                       receipt: Mapping[str, object]) -> dict[str, object]:
    """Record stage retirement for a caller-run egress receipt.

    Same proof as `retire_batch`'s internal filing, for callers that drove
    `stage_release.evict` themselves: bound contract, ownership lock, exact
    batch/manifest/attempt match, complete error-free receipt naming this
    mover and namespace. Returns a refusal dict instead of raising, so a
    bare call without proof refuses rather than retiring an active batch.
    """

    try:
        _require_bound_contract(template, instance)
        checked_instance = validate_instance(instance)
    except ProducedOutputError as exc:
        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    _name(batch_id, where="batch_id")
    if not isinstance(receipt, Mapping):
        return {"ok": False, "refusal": "retire-needs-receipt"}
    with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
        try:
            _mark_batch_retired_locked(
                queue, checked_instance, batch_id, receipt, [])
        except ProducedOutputError as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
    return {"ok": True, "batch_id": batch_id}


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
        _require_bound_contract(template, instance)
        checked_instance = validate_instance(instance)
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
        # Origin paths come from the filed immutable batch record (exact
        # sealed descriptors), never from caller arguments.
        batch_file = (Path(queue.root) / "residency" / OUTPUT_BATCHES_SUBDIR
                      / instance_namespace(checked_instance)
                      / f"{batch_id}.json")
        try:
            with open(batch_file, "rb") as handle:
                filed = json.loads(handle.read(4 * 1024 * 1024 + 1).decode())
        except FileNotFoundError:
            return {"ok": False, "refusal": "unknown-retain: batch-record-missing"}
        except (OSError, ValueError, UnicodeDecodeError) as exc:
            return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        if not isinstance(filed, Mapping):
            return {"ok": False, "refusal": "unknown-retain: batch-record-missing"}
        if (str(filed.get("manifest_digest") or "")
                != str(entry.get("manifest_digest") or "")):
            return {"ok": False, "refusal": "unknown-retain: batch-record-mismatch"}
        for desc_path in sorted(str(e.get("path") or "") for e in
                                (filed.get("entries") or [])
                                if isinstance(e, Mapping)):
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
        recorded = entry.get("staged_paths")
        if isinstance(recorded, list):
            for path in recorded:
                if isinstance(path, str) and path:
                    wanted.add(os.path.normpath(path))
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
            # No staged bytes vouched: nothing pinnable -- unless this
            # record retired before staged paths were recorded, in which
            # case its paths are unknowable and live paths can't be
            # ruled out.
            if entry.get("retired") and "staged_paths" not in entry:
                unrecorded.append(str(batch_id))
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
            if entry.get("retired"):
                continue
            return {"ok": False, "refusal": "active-batches-retain",
                    "batch_id": batch_id}
        for batch_id, entry in batches.items():
            assert isinstance(entry, Mapping)
            mover = str(entry.get("mover_key") or "")
            tier = str(entry.get("tier") or "")
            if mover and tier:
                try:
                    if queue.tier_ledger(tier).holder_tokens(mover):
                        return {"ok": False, "refusal": "active-movers-retain",
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
            ns = str(entry.get("batch_namespace") or "")
            if ns:
                holders.add(ns)
            mover = str(entry.get("mover_key") or "")
            if mover:
                holders.add(mover)
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

    NEW method owned by this lane. For each bound instance: compose each
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
                if not isinstance(entry, Mapping) or entry.get("retired"):
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
                entries = composed.get("entries")
                events.append({
                    "event": "output-batch-staged",
                    "batch_id": batch_id, "namespace": ns,
                    "manifest_digest": str(entry.get("manifest_digest")),
                    "entries": len(entries) if isinstance(entries, Mapping) else 0,
                })
    return events


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
    For each committed unretired batch: staged fragments composing under
    their own namespace need nothing; a FAILED mover needs a retry row; an
    absent mover with no staged fragments needs its first row. Each row
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
        if not isinstance(entry, Mapping) or entry.get("retired"):
            continue
        ns = str(entry.get("batch_namespace") or "")
        mover = str(entry.get("mover_key") or "")
        tier = str(entry.get("tier") or "")
        manifest = str(entry.get("manifest_digest") or "")
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
            "reason": ("retry-failed-mover" if state == "failed"
                       else "needs-publish"),
        })
    return rows


def recover_batches(queue, instance: Mapping[str, object],
                    template: Mapping[str, object]) -> list[dict[str, object]]:
    """Classify every batch for deterministic recovery (read-only).

    NEW method owned by this lane. Uses existing receipts/ledgers/fragments
    only: staged (composes), unstaged (no fragments, mover absent → due),
    mover-failed (terminal FAILED → retry), mover-live (claimed/ready →
    wait), unknown (unreadable scan → defer). Returns events in batch_id
    order; callers act through existing publish/evict paths, never here.
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
        if entry.get("retired"):
            events.append({"event": "output-batch-retired",
                           "batch_id": batch_id})
            continue
        ns = str(entry.get("batch_namespace") or "")
        mover = str(entry.get("mover_key") or "")
        try:
            fragments = map_mod.read_fragments(out_base, ns) if ns else []
            staged = bool(fragments) and bool(map_mod.compose(fragments))
        except Exception as exc:
            events.append({"event": "output-recovery-unknown",
                           "batch_id": batch_id, "error": repr(exc)})
            continue
        state = _mover_live_state(queue, mover) if mover else "absent"
        if staged:
            events.append({"event": "output-batch-staged",
                           "batch_id": batch_id, "namespace": ns})
        elif state == "failed":
            events.append({"event": "output-mover-failed-retry",
                           "batch_id": batch_id, "mover": mover})
        elif state in ("claimed", "ready"):
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
    "build_stage_manifest",
    "retire_batch",
    "mark_batch_retired",
    "reclaim_origin",
    "safe_release_instance",
    "output_scope_tick",
    "due_mover_rows",
    "recover_batches",
    "build_declaration",
    "declared_template",
    "bind_declared_instance",
]
