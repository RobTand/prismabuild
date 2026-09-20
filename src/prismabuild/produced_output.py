"""PB-produced-output staging scope: the smallest explicit extension (prototype).

An admitted GPU action writes new exact activation/cotangent/checkpoint
artifacts and reads them again during THE SAME action. Existing PB manifests
only stage pre-existing immutable external inputs. This module owns the
staging/protection/cleanup contract for those produced outputs; it does not
copy bytes itself (existing movers do), does not create a parallel cache, and
does not rewrite any sealed input manifest or action key.

Separation (normative):
  * immutable external inputs: sealed parent manifest, pre-exist the action,
    staged per the frozen read plan (residency_plan/residency_map).
  * same-action produced outputs: declared here, filed separately, composed
    into their own material namespace, never merged into the external map.

Ownership:
  * PQ keeps the exact-activation owner
    (perturbed_x_cache.write_exact_activation_cache_entry): bytes + receipt.
  * PB owns copy/adopt + material generation + lease + retirement (this
    scope + existing movers/residency/ledgers + reader_lease lane).

Status: PROTOTYPE for root review. Shared production wiring waits for root
approval of the concrete API. No capability is advertised by this file.
"""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
import os
from pathlib import Path
import tempfile
import uuid

PRODUCED_OUTPUT_SCOPE_SCHEMA_V1 = (
    "prismaquant.prismabuild.produced_output_scope.v1"
)
PRODUCED_OUTPUT_DESCRIPTOR_SCHEMA_V1 = (
    "prismaquant.prismabuild.produced_output_descriptor.v1"
)
PRODUCED_OUTPUT_MANIFEST_SCHEMA_V1 = (
    "prismaquant.prismabuild.produced_output_manifest.v1"
)

#: Reserved subdirectories under the residency root. Fragments for outputs
#: live beside input fragments but under their own namespace so
#: residency_map.compose (one manifest identity) is never asked to merge
#: dynamic output fragments with external input fragments.
OUTPUT_SCOPES_SUBDIR = "produced-output-scopes"
OUTPUT_FRAGMENTS_SUBDIR = "produced-output-fragments"

_HEX = frozenset("0123456789abcdef")


class ProducedOutputError(ValueError):
    """A scope or descriptor that does not say what it must."""


def _hex64(value: object, *, where: str) -> str:
    if (not isinstance(value, str) or len(value) != 64
            or any(c not in _HEX for c in value)):
        raise ProducedOutputError(f"{where} must be a 64-character hex key")
    return value


def _abs_norm(value: object, *, where: str) -> str:
    if not isinstance(value, str) or not value.startswith("/"):
        raise ProducedOutputError(f"{where} must be an absolute path")
    normal = os.path.normpath(value)
    if value != normal:
        raise ProducedOutputError(f"{where} must be normalized")
    return normal


def _slot(value: object, *, where: str) -> str:
    if not isinstance(value, str) or not value or "/" in value:
        raise ProducedOutputError(f"{where} must be a non-empty name with no '/'")
    return value


def _positive_int(value: object, *, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ProducedOutputError(f"{where} must be a positive integer")
    return value


def _nonneg_int(value: object, *, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProducedOutputError(f"{where} must be a non-negative integer")
    return value


def _digest(value: object, *, where: str) -> str:
    return _hex64(value, where=where)


def mint_generation() -> str:
    """One materialization generation: new bytes always mean a new identity.

    A retry of the same mover republishes under the same key with a NEW
    generation, so mover_action_key alone is never the generation and a
    same-path/length republish with different bytes can never ABA-alias.
    """

    return uuid.uuid4().hex


def validate_scope(value: object) -> dict[str, object]:
    """Check a produced-output scope declaration.

    Required keys: schema, version (1), producer_action_key, attempt
    {nonce, scope_id}, output_prefix (abs, normalized), slots (distinct
    non-empty names), byte_envelopes {payload_max_bytes,
    checkpoint_max_bytes, temp_overlap_max_bytes}, permitted_tiers
    (distinct non-empty tier ids).

    The before-write budget is the SUM of the three envelopes: checkpoint
    and partial temp/overlap count BEFORE writing, never by registering
    bytes afterwards. The durable pool budget (output_prefix bytes on HDD)
    is distinct from the shared SSD/RAM reservation (tier ledger tokens)
    which is distinct from host decode buffers (the action's mem demand).
    """

    if not isinstance(value, Mapping):
        raise ProducedOutputError("a produced-output scope must be an object")
    allowed = frozenset({
        "schema", "version", "producer_action_key", "attempt",
        "output_prefix", "slots", "byte_envelopes", "permitted_tiers",
    })
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ProducedOutputError(f"unknown scope fields: {unknown}")
    if value.get("schema") != PRODUCED_OUTPUT_SCOPE_SCHEMA_V1:
        raise ProducedOutputError(
            f"scope schema must be {PRODUCED_OUTPUT_SCOPE_SCHEMA_V1!r}")
    if value.get("version") != 1:
        raise ProducedOutputError("scope version must be 1")
    producer = _hex64(value.get("producer_action_key"),
                      where="scope producer_action_key")
    attempt = value.get("attempt")
    if not isinstance(attempt, Mapping):
        raise ProducedOutputError("scope attempt must be an object")
    if set(attempt) != {"nonce", "scope_id"}:
        raise ProducedOutputError("scope attempt must carry nonce + scope_id")
    nonce = attempt.get("nonce")
    scope_id = attempt.get("scope_id")
    if not isinstance(nonce, str) or not nonce or "/" in nonce:
        raise ProducedOutputError("scope attempt.nonce must be a non-empty name")
    if not isinstance(scope_id, str) or not scope_id or "/" in scope_id:
        raise ProducedOutputError("scope attempt.scope_id must be a non-empty name")
    prefix = _abs_norm(value.get("output_prefix"), where="scope output_prefix")
    slots = value.get("slots")
    if not isinstance(slots, list) or not slots:
        raise ProducedOutputError("scope slots must be a non-empty array")
    checked_slots = [_slot(s, where="scope slots[]") for s in slots]
    if len(set(checked_slots)) != len(checked_slots):
        raise ProducedOutputError("scope slots must not repeat a slot")
    envelopes = value.get("byte_envelopes")
    if not isinstance(envelopes, Mapping):
        raise ProducedOutputError("scope byte_envelopes must be an object")
    if set(envelopes) != {"payload_max_bytes", "checkpoint_max_bytes",
                          "temp_overlap_max_bytes"}:
        raise ProducedOutputError(
            "scope byte_envelopes must carry payload/checkpoint/temp_overlap maxima")
    payload = _positive_int(envelopes.get("payload_max_bytes"),
                            where="scope byte_envelopes.payload_max_bytes")
    checkpoint = _positive_int(envelopes.get("checkpoint_max_bytes"),
                               where="scope byte_envelopes.checkpoint_max_bytes")
    overlap = _positive_int(envelopes.get("temp_overlap_max_bytes"),
                            where="scope byte_envelopes.temp_overlap_max_bytes")
    tiers = value.get("permitted_tiers")
    if not isinstance(tiers, list) or not tiers:
        raise ProducedOutputError("scope permitted_tiers must be a non-empty array")
    checked_tiers = []
    for tier in tiers:
        if not isinstance(tier, str) or not tier or "/" in tier.split(":")[0]:
            # Tier ids are "<kind>:<host>" or "arc:<host>"; keep the check
            # structural, not a discovery claim.
            raise ProducedOutputError("scope permitted_tiers[] must be tier ids")
        checked_tiers.append(tier)
    if len(set(checked_tiers)) != len(checked_tiers):
        raise ProducedOutputError("scope permitted_tiers must not repeat a tier")
    return {
        "schema": PRODUCED_OUTPUT_SCOPE_SCHEMA_V1,
        "version": 1,
        "producer_action_key": producer,
        "attempt": {"nonce": nonce, "scope_id": scope_id},
        "output_prefix": prefix,
        "slots": checked_slots,
        "byte_envelopes": {
            "payload_max_bytes": payload,
            "checkpoint_max_bytes": checkpoint,
            "temp_overlap_max_bytes": overlap,
        },
        "permitted_tiers": checked_tiers,
    }


def total_reservation_bytes(scope: Mapping[str, object]) -> int:
    """Before-write budget: payload + checkpoint + temp/overlap."""

    checked = validate_scope(scope)
    env = checked["byte_envelopes"]
    assert isinstance(env, dict)
    return int(env["payload_max_bytes"]) + int(env["checkpoint_max_bytes"]) \
        + int(env["temp_overlap_max_bytes"])


def output_consumer_key(scope: Mapping[str, object]) -> str:
    """The material/readset namespace for this scope, distinct from the owner.

    The lease owner is the running (producer_action_key, attempt). The
    composed output map lives under this derived 64-hex namespace so dynamic
    output fragments are never mixed into the external input map and
    compose() keeps its one-manifest-identity assumption. The namespace must
    never be confused with the owner action key and must never borrow
    another action's terminal proof.
    """

    checked = validate_scope(scope)
    attempt = checked["attempt"]
    assert isinstance(attempt, dict)
    raw = ("produced-output:" + str(checked["producer_action_key"]) + ":"
           + str(attempt["nonce"]) + ":" + str(attempt["scope_id"]) + ":"
           + str(checked["output_prefix"]))
    return hashlib.sha256(raw.encode()).hexdigest()


def validate_descriptor(value: object, scope: Mapping[str, object]) -> dict[str, object]:
    """Check one durably committed artifact descriptor against its scope.

    Immutable triple: path (under output_prefix), bytes, sha256, plus the
    producer generation and logical slot that make same-path/length
    republishes distinct. No descriptor is ever edited in place; a new
    generation mints a new descriptor.
    """

    checked_scope = validate_scope(scope)
    if not isinstance(value, Mapping):
        raise ProducedOutputError("a descriptor must be an object")
    allowed = frozenset({
        "schema", "slot", "path", "bytes", "sha256",
        "producer_generation", "producer_action_key", "attempt",
    })
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ProducedOutputError(f"unknown descriptor fields: {unknown}")
    if value.get("schema") != PRODUCED_OUTPUT_DESCRIPTOR_SCHEMA_V1:
        raise ProducedOutputError(
            f"descriptor schema must be {PRODUCED_OUTPUT_DESCRIPTOR_SCHEMA_V1!r}")
    slot = _slot(value.get("slot"), where="descriptor slot")
    if slot not in checked_scope["slots"]:
        raise ProducedOutputError(
            f"descriptor slot {slot!r} is not in the scope's authorized slots")
    path = _abs_norm(value.get("path"), where="descriptor path")
    prefix = str(checked_scope["output_prefix"])
    if not (path == prefix or path.startswith(prefix.rstrip("/") + "/")):
        raise ProducedOutputError(
            f"descriptor path must live under the scope prefix {prefix!r}")
    size = _positive_int(value.get("bytes"), where="descriptor bytes")
    digest = _digest(value.get("sha256"), where="descriptor sha256")
    generation = value.get("producer_generation")
    if not isinstance(generation, str) or not generation or "/" in generation:
        raise ProducedOutputError(
            "descriptor producer_generation must be a non-empty name")
    if value.get("producer_action_key") != checked_scope["producer_action_key"]:
        raise ProducedOutputError(
            "descriptor producer_action_key must equal the scope's")
    attempt = value.get("attempt")
    if not isinstance(attempt, Mapping) or dict(attempt) != dict(checked_scope["attempt"]):
        raise ProducedOutputError(
            "descriptor attempt must equal the scope's exact action/attempt")
    # Envelope check is against the payload class here; checkpoint-class
    # descriptors are validated by the caller against the checkpoint class
    # with the same shape (kind travels beside the descriptor, never inside
    # the sealed triple).
    env = checked_scope["byte_envelopes"]
    assert isinstance(env, dict)
    if size > int(env["payload_max_bytes"]):
        raise ProducedOutputError("descriptor bytes exceed the payload envelope")
    return {
        "schema": PRODUCED_OUTPUT_DESCRIPTOR_SCHEMA_V1,
        "slot": slot,
        "path": path,
        "bytes": size,
        "sha256": digest,
        "producer_generation": generation,
        "producer_action_key": str(checked_scope["producer_action_key"]),
        "attempt": dict(checked_scope["attempt"]),
    }


def output_manifest_sha256(descriptors: list[Mapping[str, object]]) -> str:
    """The immutable v2 output manifest digest over sealed descriptors."""

    canonical = json.dumps(
        [dict(d) for d in descriptors], sort_keys=True,
        separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def scope_file_path(queue_root: str | Path, scope: Mapping[str, object]) -> Path:
    """`<queue>/residency/produced-output-scopes/<producer>/<scope_id>.json`."""

    checked = validate_scope(scope)
    attempt = checked["attempt"]
    assert isinstance(attempt, dict)
    return (Path(queue_root) / "residency" / OUTPUT_SCOPES_SUBDIR
            / str(checked["producer_action_key"])
            / f"{str(attempt['scope_id'])}.json")


def declare_scope(queue_root: str | Path, scope: Mapping[str, object]) -> Path:
    """File one scope immutably; a conflicting body refuses, never replaces."""

    from prismabuild import pool as pool_mod

    checked = validate_scope(scope)
    path = scope_file_path(queue_root, checked)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(checked, sort_keys=True,
                     separators=(",", ":")).encode() + b"\n"
    try:
        pool_mod._publish_immutable(path, raw, where="produced-output scope")
    except pool_mod.PoolContractError as exc:
        raise ProducedOutputError(
            f"a different scope is already filed for this producer/scope_id: {exc}"
        ) from None
    return path


def output_fragment_root(residency_root: str | Path) -> Path:
    """The output material namespace root, beside input fragments."""

    return Path(residency_root) / OUTPUT_FRAGMENTS_SUBDIR


def reservation_key(scope: Mapping[str, object]) -> str:
    """The ledger holder for a scope's shared-tier reservation."""

    # The material namespace holds the reservation so input movers and output
    # staging never share one holder's accounting.
    return output_consumer_key(scope)


def reserve_scope(queue, scope: Mapping[str, object], tier_id: str) -> dict[str, object]:
    """Reserve shared-tier capacity for the scope BEFORE any byte is written.

    Uses the existing tier ledger (all-or-nothing acquire). The reserved
    GiB covers payload + checkpoint + temp/overlap (total_reservation_bytes),
    in the tier's own capacity kind. Returns {"ok": True, ...} or
    {"ok": False, "refusal": ...} with typed refusals:
    tier-not-permitted | tier-unknown | never-fits-tier-capacity |
    tier-reservation-unavailable. Never registers bytes posthoc.
    """

    from prismabuild import storage_tiers as tiers_mod

    checked = validate_scope(scope)
    if tier_id not in checked["permitted_tiers"]:
        return {"ok": False, "refusal": "tier-not-permitted"}
    total = total_reservation_bytes(checked)
    gib = tiers_mod.stage_tokens_for_bytes(total)
    kind = tiers_mod.capacity_kind_of(tier_id)
    demand = {f"{kind}@{tier_id}": gib}
    ledger = queue.tier_ledger(tier_id)
    if not ledger.base.is_dir():
        return {"ok": False, "refusal": "tier-unknown"}
    total_cap = ledger.capacity()
    if any(total_cap.get(k, 0) < need for k, need in demand.items()):
        return {"ok": False, "refusal": "never-fits-tier-capacity",
                "demand": demand, "capacity": total_cap}
    if not ledger.acquire(reservation_key(checked), demand):
        return {"ok": False, "refusal": "tier-reservation-unavailable",
                "demand": demand, "available": ledger.available()}
    return {"ok": True, "tier_id": tier_id, "demand": demand,
            "reservation_key": reservation_key(checked),
            "reservation_bytes": total}


def release_scope(queue, scope: Mapping[str, object]) -> int:
    """Return the scope's whole shared-tier reservation (reclaim path only).

    Must run AFTER physical reclaim (egress deletes), never before: release
    before reclaim would admit a mover onto capacity that is still occupied.
    Safe to call twice.
    """

    return queue.release_tier_reservations(reservation_key(scope))


def build_output_manifest(descriptors: list[Mapping[str, object]],
                          scope: Mapping[str, object]) -> dict[str, object]:
    """Seal validated descriptors into an immutable output manifest (v2)."""

    checked = validate_scope(scope)
    sealed = [validate_descriptor(d, checked) for d in descriptors]
    digest = output_manifest_sha256(sealed)
    total = sum(int(d["bytes"]) for d in sealed)
    env = checked["byte_envelopes"]
    assert isinstance(env, dict)
    if total > int(env["payload_max_bytes"]):
        raise ProducedOutputError(
            "sealed output bytes exceed the scope payload envelope")
    return {
        "schema": PRODUCED_OUTPUT_MANIFEST_SCHEMA_V1,
        "producer_action_key": str(checked["producer_action_key"]),
        "attempt": dict(checked["attempt"]),
        "output_prefix": str(checked["output_prefix"]),
        "output_consumer_key": output_consumer_key(checked),
        "manifest_sha256": digest,
        "total_bytes": total,
        "entry_count": len(sealed),
        "entries": sealed,
    }


def _write_atomic(path: Path, payload: Mapping[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.")
    try:
        with os.fdopen(handle, "w") as stream:
            json.dump(payload, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
    return path


__all__ = [
    "PRODUCED_OUTPUT_SCOPE_SCHEMA_V1",
    "PRODUCED_OUTPUT_DESCRIPTOR_SCHEMA_V1",
    "PRODUCED_OUTPUT_MANIFEST_SCHEMA_V1",
    "OUTPUT_SCOPES_SUBDIR",
    "OUTPUT_FRAGMENTS_SUBDIR",
    "ProducedOutputError",
    "mint_generation",
    "validate_scope",
    "total_reservation_bytes",
    "output_consumer_key",
    "validate_descriptor",
    "output_manifest_sha256",
    "scope_file_path",
    "declare_scope",
    "output_fragment_root",
    "reservation_key",
    "reserve_scope",
    "release_scope",
    "build_output_manifest",
]
