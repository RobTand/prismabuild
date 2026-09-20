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
    checked_demands: dict[str, dict[str, int]] = {}
    for tier, spec in demands.items():
        if not isinstance(tier, str) or not tier:
            raise ProducedOutputError("template working_demands keys must be tier ids")
        if not isinstance(spec, Mapping) or set(spec) != {"minimum_gib", "window_gib"}:
            raise ProducedOutputError(
                f"template working_demands[{tier!r}] must carry minimum/window GiB")
        minimum = _nonneg_int(spec.get("minimum_gib"),
                              where=f"template working_demands[{tier}].minimum_gib")
        window = _positive_int(spec.get("window_gib"),
                               where=f"template working_demands[{tier}].window_gib")
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
    (foreign/stale). There is no intent fallback, no derived nonce, and no
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


def checked_instance_maxima(template: Mapping[str, object]) -> dict[str, int]:
    maxima = validate_template(template)["durable_maxima"]
    assert isinstance(maxima, dict)
    return {key: int(maxima[key]) for key in maxima}


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
    sums = {"payload": 0, "checkpoint": 0, "temp": 0}
    for record in batches.values():
        if not isinstance(record, Mapping) or record.get("retired"):
            continue
        class_bytes = record.get("class_bytes")
        if not isinstance(class_bytes, Mapping):
            continue
        for cls in sums:
            sums[cls] += int(class_bytes.get(cls, 0) or 0)
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

    checked_template = validate_template(template)
    checked_instance = validate_instance(instance)
    if checked_instance["template_sha256"] != template_sha256(checked_template):
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
    """General funded-window admission gate (production: refuses pending).

    Fully validates the window request (bound instance + template match,
    permitted tiers, positive-integer needs, need within window demand and
    within minted tier capacity), then refuses `funding-primitive-pending`:
    the liveness-owned funded-claim primitive (funding record + eligible-
    token verification + serialized transfer + current+next admission) is
    the only authority that may fund a window, and it is not delivered yet.
    Per-batch exact physical funding at `commit_batch` (existing ledger
    acquire + whole transfer) is unaffected: it funds one amount, not a
    window. Returns {"ok": False, "refusal": ..., "dependency": ...}.
    """

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
    return {"ok": False, "refusal": "funding-primitive-pending",
            "dependency": ("liveness funded-claim primitive: funding record "
                           "binding credit to exact tier/plan-window/mover/"
                           "range/generation + eligible-token verification + "
                           "serialized transfer + current+next window "
                           "admission")}


def require_prewrite(queue, instance: Mapping[str, object],
                     template: Mapping[str, object], *, batch_id: str,
                     tier: str, class_bytes: Mapping[str, int]) -> dict[str, object]:
    """File a prewrite budget claim BEFORE any HDD byte is written.

    The production writer path must call this (not an optional helper):
    uncharged temp/checkpoint writes refuse here. Checks the bound admission
    record (binding metadata, never funding) + durable headroom for the
    planned class bytes, and files an immutable prewrite record the later
    commit must present. Physical funding happens only at `commit_batch`
    (exact ledger acquire) and in the liveness window primitive (pending).
    Zero-byte classes are valid (explicit zeros, never missing keys).
    """

    checked_template = validate_template(template)
    checked_instance = validate_instance(instance)
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
    with queue.stage_ownership_lock(str(checked_instance["output_prefix"])):
        commitments = _read_commitments(
            _commitments_path(queue.root, checked_instance))
        if not isinstance(commitments.get("admission"), Mapping):
            # Admission credit proves the instance was bound; without it
            # nothing is prewritable. (Zero-minimum tiers still need the
            # bound record, never ledger presence.)
            return {"ok": False, "refusal": "prewrite-not-admitted"}
        sums = _class_sums(commitments["batches"])
        assert isinstance(sums, dict)
        maxima = checked_instance_maxima(checked_template)
        for cls in sums:
            cap = maxima[f"{cls}_max_bytes"]
            if sums[cls] + planned[cls] > cap:
                return {"ok": False, "refusal": f"prewrite-exceeds-{cls}-maxima",
                        "class": cls}
        directory = instance_dir(queue.root, checked_instance) / "prewrites"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{batch_id}.prewrite.json"
        record = {"batch_id": batch_id, "tier": tier, "class_bytes": planned,
                  "owner_action_key": str(checked_instance["owner_action_key"]),
                  "owner_attempt": dict(checked_instance["owner_attempt"])}
        raw = json.dumps(record, sort_keys=True,
                         separators=(",", ":")).encode() + b"\n"
        try:
            from prismabuild import pool as pool_mod
            pool_mod._publish_immutable(path, raw, where="produced-output prewrite")
        except Exception as exc:
            return {"ok": False, "refusal": f"prewrite-conflict: {exc}"}
    return {"ok": True, "batch_id": batch_id, "class_bytes": planned}


def commit_batch(queue, instance: Mapping[str, object],
                 template: Mapping[str, object],
                 descriptors: list[Mapping[str, object]], *, batch_id: str,
                 tier: str, mover_key: str) -> dict[str, object]:
    """Commit one immutable batch: check, prewrite-match, acquire exact under
    the batch holder, TRANSFER whole to mover ownership (no free interval),
    file the batch record. All-or-nothing with typed refusals."""

    from prismabuild import pool as pool_mod
    from prismabuild import storage_tiers as tiers_mod

    checked_template = validate_template(template)
    checked_instance = validate_instance(instance)
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
        commitments = _read_commitments(
            _commitments_path(queue.root, checked_instance))
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
        prewrite_path = (instance_dir(queue.root, checked_instance) / "prewrites"
                         / f"{batch_id}.prewrite.json")
        try:
            prewrite = json.loads(prewrite_path.read_text())
        except FileNotFoundError:
            return {"ok": False, "refusal": "prewrite-reservation-missing"}
        except (OSError, ValueError) as exc:
            return {"ok": False, "refusal": f"prewrite-unreadable: {exc}"}
        if (not isinstance(prewrite, Mapping)
                or prewrite.get("tier") != tier
                or dict(prewrite.get("class_bytes", {})) != class_bytes):
            return {"ok": False, "refusal": "prewrite-mismatch"}
        sums = _class_sums(batches)
        maxima = checked_instance_maxima(checked_template)
        for cls in sums:
            if sums[cls] + class_bytes[cls] > maxima[f"{cls}_max_bytes"]:
                return {"ok": False, "refusal": f"commit-exceeds-{cls}-maxima"}
        ledger = queue.tier_ledger(tier)
        if not ledger.acquire(batch_ns, {kind: batch_gib}):
            return {"ok": False, "refusal": "tier-reservation-unavailable",
                    "available": ledger.available()}
        moved = queue.transfer_tier_reservation(tier, batch_ns, mover)
        if moved != batch_gib:
            return {"ok": False, "refusal": "transfer-short",
                    "moved": moved, "expected": batch_gib}
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
        }
        _write_commitments(_commitments_path(queue.root, checked_instance),
                           {"batches": batches})
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


def retire_batch(queue, batch: Mapping[str, object], *, stage_root: str,
                 residency_root: str | Path) -> dict[str, object]:
    """Evict one batch's staged files, then mark it retired. Charge retained
    on any incomplete/tainted result; retirement is recorded only after a
    complete egress."""

    from prismabuild import pool as pool_mod
    import stage_release

    if not isinstance(batch, Mapping):
        return {"ok": False, "refusal": "bad-batch"}
    consumer = str(batch.get("batch_namespace") or "")
    mover = str(batch.get("mover_key") or "")
    if len(consumer) != 64 or len(mover) != 64:
        return {"ok": False, "refusal": "bad-batch-namespace"}
    receipt = stage_release.evict(
        queue, mover, consumer_action_key=consumer,
        stage_root=str(stage_root), residency_root=str(residency_root))
    if not receipt.get("complete"):
        return {"ok": False, "refusal": "egress-incomplete", "receipt": receipt}
    return {"ok": True, "receipt": receipt}


def mark_batch_retired(queue_root: str | Path, instance: Mapping[str, object],
                       batch_id: str) -> None:
    """Record retirement after a complete egress (under the prefix lock)."""

    checked = validate_instance(instance)
    path = _commitments_path(queue_root, checked)
    record = _read_commitments(path)
    batches = record["batches"]
    assert isinstance(batches, dict)
    entry = batches.get(batch_id)
    if not isinstance(entry, Mapping):
        raise ProducedOutputError("unknown batch_id for this instance")
    entry = dict(entry)
    entry["retired"] = True
    batches[batch_id] = entry
    _write_commitments(path, {"batches": batches})


def safe_release_instance(queue, instance: Mapping[str, object],
                          template: Mapping[str, object]) -> dict[str, object]:
    """Release leftover batch-holder tokens ONLY when retirement is proven safe.

    Movers release their exact tokens through egress (`retire_batch`); this
    reclaims any remainder (e.g. partial-transfer leftovers) after a fresh
    census under the prefix lock: instance + commitments readable (unknown
    retains); every batch retired AND its mover holder empty AND its
    fragments gone (active retains); live-lease refs absent where the SDK is
    available (live retains, dependency named when not); owner terminal
    present (owner-active retains). Idempotent: the orderly path releases 0
    here because egress already released exactly once; leftovers release once.
    """

    from prismabuild import pool as pool_mod

    checked_template = validate_template(template)
    try:
        checked = validate_instance(instance)
    except ProducedOutputError as exc:
        return {"ok": False, "refusal": f"unknown-retain: {exc}"}
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
                frag_dir = out_base / ns
                try:
                    if frag_dir.is_dir() and any(frag_dir.iterdir()):
                        return {"ok": False, "refusal": "active-movers-retain",
                                "batch_id": batch_id}
                except OSError as exc:
                    return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        try:
            import reader_lease  # PB730 SDK when deployed; absent on main
        except ImportError:
            reader_lease = None  # type: ignore[assignment]
        if reader_lease is not None:
            try:
                staged: set[str] = set()
                live = reader_lease.live_for(queue, staged or None,
                                             residency_root=str(out_base))
                if live:
                    return {"ok": False, "refusal": "live-refs-retain",
                            "pins": sorted(live)[:8]}
            except Exception as exc:
                return {"ok": False, "refusal": f"unknown-retain: {exc}"}
        owner = str(checked["owner_action_key"])
        terminal = (pool_mod._read_json(queue.item_path(pool_mod.DONE, owner))
                    or pool_mod._read_json(queue.item_path(pool_mod.FAILED, owner))
                    or pool_mod._read_json(queue.item_path(pool_mod.WITHDRAWN, owner)))
        live_claim = pool_mod._read_json(queue.item_path(pool_mod.CLAIMED, owner))
        if terminal is None:
            if live_claim is not None:
                return {"ok": False, "refusal": "owner-active-retain"}
            return {"ok": False, "refusal": "unknown-retain: no-terminal"}
        released = 0
        for tier in checked_template["permitted_tiers"]:
            for batch_id, entry in batches.items():
                assert isinstance(entry, Mapping)
                ns = str(entry.get("batch_namespace") or "")
                if not ns:
                    continue
                try:
                    released += queue.tier_ledger(tier).release(ns)
                except Exception:
                    break
        # Batch holders are empty post-transfer by construction; leftovers
        # (partial transfers) release here, once, idempotently.
        return {"ok": True, "released": released}


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
    absent mover with no staged fragments needs its first row. Rows carry
    the frozen identity (batch/mover/tier/manifest/generation) a submitter
    seals; publication itself (`queue.publish`, behind the funding gate)
    stays with the tier loop. Deterministic order: batch_id ascending.
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
        if not ns or not mover or not tier:
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
        rows.append({
            "batch_id": batch_id,
            "action_key": mover,
            "tier": tier,
            "manifest_digest": str(entry.get("manifest_digest") or ""),
            "batch_namespace": ns,
            "resources": ({kind: gib, "cpu": 1, "mem_gb": 1}
                          if gib > 0 else {"cpu": 1, "mem_gb": 1}),
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


__all__ = [
    "TEMPLATE_SCHEMA_V1",
    "INSTANCE_SCHEMA_V1",
    "DESCRIPTOR_SCHEMA_V2",
    "BATCH_SCHEMA_V1",
    "BATCH_MANIFEST_SCHEMA_V1",
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
    "reserve_working_minimum",
    "require_prewrite",
    "commit_batch",
    "build_stage_manifest",
    "retire_batch",
    "mark_batch_retired",
    "safe_release_instance",
    "output_scope_tick",
    "due_mover_rows",
    "recover_batches",
]
