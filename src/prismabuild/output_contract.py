"""Pure produced-output identity, schema and error vocabulary."""
from __future__ import annotations
from collections.abc import Mapping
import os
from pathlib import Path
import uuid
from prismabuild import core, digest_primitives


TEMPLATE_SCHEMA_V1 = "prismaquant.prismabuild.produced_output_template.v1"



INSTANCE_SCHEMA_V1 = "prismaquant.prismabuild.produced_output_instance.v1"



DESCRIPTOR_SCHEMA_V2 = "prismaquant.prismabuild.produced_output_descriptor.v2"



BATCH_SCHEMA_V1 = "prismaquant.prismabuild.produced_output_batch.v1"



BATCH_MANIFEST_SCHEMA_V1 = "prismaquant.prismabuild.produced_output_manifest.v1"



PRODUCED_OUTPUT_REF_SCHEMA_V1 = "prismabuild.produced_output_ref.v1"



MATERIALIZATION_SCHEMA_V1 = (
    "prismaquant.prismabuild.produced_output_materialization.v1")



ORIGIN_BATCH_REF_SCHEMA_V1 = "prismabuild.produced_output_origin_batch_ref.v1"



ORIGIN_BATCHES_ANNOTATION = "produced_output_batches"



ORIGIN_SLOTS_ANNOTATION = "produced_output_slots"



ORIGIN_LIFETIME_RETAIN = "retain"



ORIGIN_LIFETIME_CONSUMED = "consumed"



ORIGIN_LIFETIMES = frozenset({ORIGIN_LIFETIME_RETAIN, ORIGIN_LIFETIME_CONSUMED})



ORIGIN_CONSUMER_SCHEMA_V1 = "prismabuild.produced_output_origin_consumer.v1"



ORIGIN_CONSUMER_RELEASE_SCHEMA_V1 = (
    "prismabuild.produced_output_origin_consumer_release.v1")



ORIGIN_CONSUMER_RELEASE_INDEX_SCHEMA_V1 = (
    "prismabuild.produced_output_origin_consumer_release_index.v1")



ORIGIN_UNREACHABLE_REFUSAL = "origin_unreachable"



OUTPUT_TEMPLATES_SUBDIR = "produced-output-templates"



OUTPUT_SCOPES_SUBDIR = "produced-output-scopes"



OUTPUT_BATCHES_SUBDIR = "produced-output-batches"



OUTPUT_FRAGMENTS_SUBDIR = "produced-output-fragments"



OUTPUT_TEMPLATE_ATTEMPTS_SUBDIR = ".attempts"



ATTEMPT_INDEX_COMPLETE = ".index-complete-v1"



READER_HELPER_ROOT_ENV = "PRISMABUILD_READER_HELPER_ROOT"



RESTAGE_FILL_ENV = "PRISMABUILD_PRODUCED_OUTPUT_RESTAGE_FILL"



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



def output_key(value: object, *, where: str) -> str:
    if (not isinstance(value, str) or len(value) != 64
            or any(c not in _HEX for c in value)):
        raise ProducedOutputError(f"{where} must be a 64-character hex key")
    return value



def _hex32(value: object, *, where: str) -> str:
    if (not isinstance(value, str) or len(value) != 32
            or any(c not in _HEX for c in value)):
        raise ProducedOutputError(f"{where} must be a 32-character hex nonce")
    return value



def output_name(value: object, *, where: str) -> str:
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



def mint_output_generation() -> str:
    """One materialization generation: new bytes always mean a new identity."""

    return uuid.uuid4().hex



def validate_template(value: object) -> dict[str, object]:
    """Check a sealed scope template."""

    if not isinstance(value, Mapping):
        raise ProducedOutputError("a produced-output template must be an object")
    allowed = frozenset({
        "schema", "version", "template_id", "output_prefix", "slots",
        "durable_maxima", "working_demands", "permitted_tiers", "write_only",
        "export_rate_family",
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
    template_id = output_name(value.get("template_id"), where="template template_id")
    prefix = _abs_norm(value.get("output_prefix"), where="template output_prefix")
    slots = value.get("slots")
    if not isinstance(slots, Mapping) or not slots:
        raise ProducedOutputError("template slots must be a non-empty object")
    checked_slots: dict[str, str] = {}
    for slot, spec in slots.items():
        name = output_name(slot, where="template slots[]")
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
    # Likewise present only when declared (#1126): the family is in the
    # canonical bytes, so `template_sha256`, and through the sealed
    # declaration the owner's action key, cover it.
    if "export_rate_family" in value:
        checked["export_rate_family"] = validate_export_rate_family(
            value["export_rate_family"])
    return checked



def validate_export_rate_family(value: object) -> str:
    """Check a template's ``export_rate_family`` (#1126).

    The family names the templates whose spool exports share one learned
    export rate on a host (``adaptive_cpu.export_slots``): a GLM-5.3 Stage B
    row is its own template, and a rate learned per template is never learned
    at all.  It is an identifier by the action contract's own rule
    (``core._ID_RE``, which input ids follow); anything else refuses, here
    and wherever a reference carrying it is read.
    """

    from prismabuild.core import _ID_RE

    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise ProducedOutputError(
            "template export_rate_family must be an identifier: 1 to 256 of "
            "[a-z0-9._/-], starting with a letter or digit")
    return value



def is_write_only(template: Mapping[str, object]) -> bool:
    """Whether a template's batches commit at origin, never through a stage."""

    return bool(validate_template(template).get("write_only"))



def template_sha256(template: Mapping[str, object]) -> str:
    """Canonical identity of one sealed template."""

    checked = validate_template(template)
    raw = core._compact_ascii_lf_bytes(checked)[:-1].decode("ascii").encode()
    return digest_primitives.raw_sha256(raw)



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
    template_id = output_name(value.get("template_id"), where="instance template_id")
    digest = output_key(value.get("template_sha256"), where="instance template_sha256")
    owner = output_key(value.get("owner_action_key"), where="instance owner_action_key")
    attempt = value.get("owner_attempt")
    if not isinstance(attempt, Mapping) or set(attempt) != {"nonce", "scope_id"}:
        raise ProducedOutputError("instance owner_attempt must carry nonce + scope_id")
    nonce = _hex32(attempt.get("nonce"), where="instance owner_attempt.nonce")
    scope_id = output_name(attempt.get("scope_id"), where="instance owner_attempt.scope_id")
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



def instance_namespace(instance: Mapping[str, object]) -> str:
    """Material/ledger namespace for the instance (NOT the owner key)."""

    checked = validate_instance(instance)
    attempt = checked["owner_attempt"]
    assert isinstance(attempt, dict)
    raw = ("produced-output-instance:" + str(checked["template_sha256"]) + ":"
           + str(checked["owner_action_key"]) + ":" + str(attempt["nonce"])
           + ":" + str(attempt["scope_id"]))
    return digest_primitives.raw_sha256(raw.encode())



ORIGIN_HELD_BY_UNKNOWN_EVENT = "output-origin-held-by-unknown-attempt"



ORPHANED_HOLDER_REMEDY = (
    "no queue row names this attempt, so nothing will end it: confirm no "
    "process of it is running, then remove its prewrite record "
    "(`holders[].prewrite_record`); the next tier cycle decides the held "
    "batch again from the files that are there")



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
    slot = output_name(value.get("slot"), where="descriptor slot")
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
        digest = output_key(raw_digest, where="descriptor sha256")
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
    return digest_primitives.raw_sha256("|".join(parts).encode())



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

    canonical = core._compact_ascii_lf_bytes([dict(d) for d in descriptors])[:-1].decode("ascii").encode()
    return digest_primitives.raw_sha256(canonical)



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



def batch_namespace(instance: Mapping[str, object], batch_id: str,
                    manifest_digest: str) -> str:
    """Immutable batch namespace bound to its exact manifest + owner scope."""

    checked = validate_instance(instance)
    output_name(batch_id, where="batch_id")
    output_key(manifest_digest, where="manifest_digest")
    raw = ("produced-output-batch:" + instance_namespace(checked) + ":"
           + batch_id + ":" + manifest_digest)
    return digest_primitives.raw_sha256(raw.encode())



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
        "template_id": output_name(template_id, where="reference template_id"),
        "template_sha256": output_key(template_sha256,
                                  where="reference template_sha256"),
        "owner_action_key": output_key(owner_action_key,
                                   where="reference owner_action_key"),
        "owner_attempt": {
            "nonce": _hex32(nonce, where="reference nonce"),
            "scope_id": output_name(scope_id, where="reference scope_id"),
        },
        "attempt_source": "broker-launch",
        "output_prefix": _abs_norm(output_prefix,
                                   where="reference output_prefix"),
        "bound_unix": 0,
    })
    return (instance_namespace(instance),
            batch_namespace(instance,
                            output_name(batch_id, where="reference batch_id"),
                            output_key(manifest_digest,
                                   where="reference manifest_digest")))



ORIGIN_REPIN_REASON = ("timestamps-only mismatch: same inode and size, content "
                       "sha256 verified (#1111, an NFS delegation recall)")






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
            "batch_id": output_name(batch_id, where="batch_id"),
            "manifest_digest": output_key(manifest_digest,
                                      where="batch manifest_digest")}



BATCH_STATE_COMMITTED = "committed"



BATCH_STATE_RETIRING = "retiring"



BATCH_STATE_RECLAIMED = "reclaimed"



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
            "owner_action_key": output_key(value.get("owner_action_key"),
                                       where="origin batch ref owner_action_key"),
            "owner_nonce": _hex32(value.get("owner_nonce"),
                                  where="origin batch ref owner_nonce"),
            "template_id": output_name(value.get("template_id"),
                                 where="origin batch ref template_id"),
            "batch_id": output_name(value.get("batch_id"),
                              where="origin batch ref batch_id"),
            "manifest_digest": output_key(value.get("manifest_digest"),
                                      where="origin batch ref manifest_digest")}



def _strict_int(value: object, *, where: str) -> int:
    # Exact JSON integers only: bools, strings, floats, and objects are
    # corrupt counts, never zero. `int()` would coerce them all.
    if type(value) is not int:
        raise ProducedOutputError(f"{where} must be an integer")
    return int(value)



OWN_EGRESS_IN_FLIGHT = "own-egress-in-flight"



BATCH_RETIREMENT_UNIT = "staged-batch-retirement"



ORIGIN_RETIRED_EVENT = "output-origin-retired"



ORIGIN_RETIREMENT_STALLED_EVENT = "output-origin-retirement-stalled"



ORIGIN_RETIREMENT_REFUSED_EVENT = "output-origin-retirement-refused"



ORIGIN_PREWRITE_RECLAIMED_EVENT = "output-prewrite-reclaimed"



ORIGIN_PREWRITE_ORPHANED_EVENT = "output-prewrite-orphaned"



ORPHANED_PREWRITE_REMEDY = (
    "these files belong to no committed batch: check and remove them, and "
    "the next tier cycle drops the prewrite's reservation. A successor that "
    "commits the same paths drops it too, and keeps its files")



DEAD_PRODUCER_BATCH_RETIRED_EVENT = "output-dead-producer-batch-retired"



_ENDED_ATTEMPT_STATES = frozenset({"dead", "succeeded"})



_STALLED_CONSUMER_STATES = frozenset(
    {"failed", "withdrawn", "unpublished", "unknown"})



_RESOLVED_CONSUMER_STATES = frozenset({"succeeded", "released", "superseded"})



_TERMINAL_CONSUMER_STATES = frozenset({"failed", "withdrawn"})















ENDED_PREWRITES_UNIT = "ended-prewrites"



CONSUMED_RETIREMENT_UNIT = "consumed-origin-retirement"



ORIGIN_RETIREMENT_UNITS = (ENDED_PREWRITES_UNIT, BATCH_RETIREMENT_UNIT,
                           CONSUMED_RETIREMENT_UNIT)



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

