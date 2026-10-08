"""D38: refuse a GPU publication that has no matching preflight evidence (#1639).

D38 requires a CPU dry run before a new or changed GPU job.  This module is the
consumer side of that rule.  It reads evidence only -- a content-verified CAS
receipt or a scoped CEO grant -- and never asks anything to judge whether
evidence exists.  Any missing, unreadable or mismatched evidence refuses.

``pbrun`` calls :func:`require` at every point that would publish new runnable
work.  A cache hit or a live attachment publishes nothing and needs no receipt.

The producer that writes receipts (``--d38-plan``, ``--d38-preflight-for``) is a
later change.  Until its code identity binds a receipt, a receipt is as honest
as the submitter that filed the preflight action; the audited exception is the
other path.  Both write an immutable audit event before anything is published.
"""
from __future__ import annotations

import datetime
import getpass
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import sys
import uuid
from typing import Callable, Mapping, Sequence

from prismabuild import container_images, core as pb

RECEIPT_SCHEMA = "fleet.d38.preflight.v1"
AUDIT_SCHEMA = "fleet.d38.audit.v1"
PRODUCER_PARAM = "d38_preflight"
PRODUCER_ID = "fleet.d38.producer.v1"
NAMESPACE_PARAM = "d38_namespace"
AUDIT_DIR_NAME = "d38-audit"
#: The authoritative CEO decision store.  A decision id alone is not authority:
#: the file must grant this exact job.
DECISION_DIR = Path("/home/rob/fleet/ceo/inbox/done")
GPU_TAGS = frozenset({"gb10", "sparky", "sparklina"})
#: The gate is on.  This is the one switch, and it is a module attribute on
#: purpose: no flag, environment variable or caller input can reach it, because
#: D38 allows no caller-controlled exemption.  Only the existing test suite
#: turns it off (``tests/conftest.py``), because those tests submit as a Spark
#: from a box-local checkout, which the spec's own rule counts as GPU intent.
ENFORCE = True
MAX_RECEIPT_BYTES = 64 * 1024
MAX_NAMESPACE_BYTES = 64 * 1024

_HASH = re.compile(r"^[0-9a-f]{64}$")
_NAMESPACE = re.compile(r"^sha256:[0-9a-f]{64}$")
_DECISION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]{0,127}$")
_RECEIPT_KEYS = frozenset({
    "schema", "job_identity_hash", "image_digest", "namespace",
    "preflight_action_key", "host_class", "result", "created_at", "expires"})
_MACHINES = {"x86": frozenset({"x86_64", "amd64"}),
             "gb10": frozenset({"aarch64", "arm64"})}


class Refusal(Exception):
    """Evidence is missing, unreadable or does not bind this job."""


def now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def requires_receipt(demand: Mapping[str, object], required_tags: Sequence[str],
                     *, host_class: str | None = None) -> bool:
    """True when this publication has GPU intent.

    ``demand`` and ``required_tags`` are the sealed, final values, so ``--gpu``
    and ``--exclusive`` expansion and derived host pins are already in them.
    Nothing a caller types -- a ``--preflight`` token, an environment variable,
    ``--gpu`` with zero demand -- exempts a job.
    """

    gpu = demand.get("gpu", 0) if isinstance(demand, Mapping) else 0
    return (bool(gpu and gpu > 0)  # type: ignore[operator]
            or bool(set(required_tags or ()) & GPU_TAGS)
            or host_class == "gb10")


# ----------------------------------------------------------------- parsing

def _strict_json(raw: bytes) -> object:
    """JSON with no duplicate keys, NaN or Infinity."""

    def pairs(items):
        seen: set[str] = set()
        for key, _ in items:
            if key in seen:
                raise ValueError(f"duplicate key {key!r}")
            seen.add(key)
        return dict(items)

    def constant(name):
        raise ValueError(f"{name} is not valid JSON")

    return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                      parse_constant=constant)


def _aware(text: object, where: str) -> datetime.datetime:
    if not isinstance(text, str):
        raise Refusal(f"{where} must be a timestamp string")
    try:
        value = datetime.datetime.fromisoformat(text)
    except ValueError:
        raise Refusal(f"{where} is not an ISO timestamp") from None
    if value.tzinfo is None or value.utcoffset() is None:
        raise Refusal(f"{where} has no timezone")
    return value


def load_namespace(path: str | Path) -> tuple[dict, str]:
    """Read one namespace descriptor; return it with its ``sha256:`` digest."""

    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        raise SystemExit(f"pbrun: cannot read --d38-namespace: {exc}") from None
    if len(raw) > MAX_NAMESPACE_BYTES:
        raise SystemExit("pbrun: --d38-namespace is larger than 64 KiB")
    try:
        descriptor = _strict_json(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise SystemExit(f"pbrun: --d38-namespace is not valid JSON: {exc}") \
            from None
    if not isinstance(descriptor, dict) or not descriptor:
        raise SystemExit("pbrun: --d38-namespace must be a non-empty JSON object")
    return descriptor, "sha256:" + pb.canonical_sha256(descriptor)


def target_images(action: Mapping[str, object]) -> list[str]:
    declared = action["params"].get("container_images") or []  # type: ignore[index]
    return list(container_images.normalize_refs(declared))


def _parse_receipt(raw: bytes) -> dict:
    if len(raw) > MAX_RECEIPT_BYTES:
        raise Refusal("the receipt is larger than 64 KiB")
    try:
        body = _strict_json(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise Refusal(f"the receipt is not valid JSON: {exc}") from None
    if not isinstance(body, dict):
        raise Refusal("the receipt is not a JSON object")
    if set(body) != _RECEIPT_KEYS:
        raise Refusal("the receipt has missing or unknown fields")
    if body["schema"] != RECEIPT_SCHEMA:
        raise Refusal("the receipt schema is not fleet.d38.preflight.v1")
    for name in ("job_identity_hash", "preflight_action_key"):
        if not isinstance(body[name], str) or not _HASH.match(body[name]):
            raise Refusal(f"the receipt {name} is not a 64-character hash")
    if not isinstance(body["namespace"], str) \
            or not _NAMESPACE.match(body["namespace"]):
        raise Refusal("the receipt namespace is not sha256: plus 64 hex digits")
    images = body["image_digest"]
    if not isinstance(images, list) or any(
            not isinstance(item, str) or not item for item in images):
        raise Refusal("the receipt image_digest is not a list of references")
    try:
        normalized = list(container_images.normalize_refs(images))
    except Exception as exc:                                    # noqa: BLE001
        raise Refusal(f"the receipt names an invalid image: {exc}") from None
    if normalized != images:
        raise Refusal("the receipt images are not unique and normalized")
    if body["host_class"] not in _MACHINES:
        raise Refusal("the receipt host_class is not x86 or gb10")
    if body["result"] not in ("pass", "fail"):
        raise Refusal("the receipt result is not pass or fail")
    created = _aware(body["created_at"], "created_at")
    if body["expires"] is not None:
        expires = _aware(body["expires"], "expires")
        if not created < expires:
            raise Refusal("the receipt expires before it was created")
    return body


# --------------------------------------------------------- receipt evidence

def _verified_receipt(cas, key: str) -> tuple[dict, dict]:
    """The preflight action and its content-verified CAS receipt."""

    if not isinstance(key, str) or not _HASH.match(key):
        raise Refusal("--d38-receipt must name a full 64-character action key")
    try:
        preflight = cas.read_action_request(key)
    except Exception as exc:                                    # noqa: BLE001
        raise Refusal(f"the preflight request cannot be read: {exc}") from None
    if preflight is None:
        raise Refusal("no preflight action with that key was published")
    try:
        receipt = cas.lookup(preflight)
    except Exception as exc:                                    # noqa: BLE001
        raise Refusal(f"the preflight receipt failed verification: {exc}") \
            from None
    if receipt is None:
        raise Refusal("the preflight action has no verified receipt yet")
    return preflight, receipt


def _check_declaration(preflight: Mapping[str, object]) -> None:
    params = preflight["params"]
    declared = params.get(PRODUCER_PARAM)  # type: ignore[union-attr]
    if (not isinstance(declared, Mapping)
            or declared.get("producer") != PRODUCER_ID
            or not isinstance(declared.get("plan_sha256"), str)
            or not _HASH.match(declared["plan_sha256"])):
        raise Refusal("the preflight action does not declare the D38 producer "
                      "and a target plan input")
    demand = params.get("demand") or {}  # type: ignore[union-attr]
    if not isinstance(demand, Mapping) or demand.get("gpu", 0) not in (0, None):
        raise Refusal("the preflight action does not declare CPU-only demand")


def _check_host_evidence(receipt: Mapping[str, object], host_class: str) -> None:
    producer = receipt.get("producer")
    evidence = producer.get("evidence") if isinstance(producer, Mapping) else None
    if not isinstance(evidence, Mapping):
        raise Refusal("the receipt carries no producer host evidence")
    if str(evidence.get("machine")) not in _MACHINES[host_class]:
        raise Refusal(f"the producer ran on {evidence.get('machine')!r}, not on "
                      f"a {host_class} host")
    if evidence.get("accelerators"):
        raise Refusal("the producer exposed an accelerator; a preflight is "
                      "CPU-only")


def verify_receipt(cas, key: str, *, job: str, images: Sequence[str],
                   namespace: str, at: datetime.datetime) -> dict:
    """Prove that one preflight receipt binds this job, or raise Refusal."""

    preflight, receipt = _verified_receipt(cas, key)
    _check_declaration(preflight)
    result = receipt.get("result")
    if not isinstance(result, Mapping) or not isinstance(
            result.get("bytes"), int) or result["bytes"] > MAX_RECEIPT_BYTES:
        raise Refusal("the receipt result is missing or larger than 64 KiB")
    try:
        raw = Path(cas.blob_path(str(result["sha256"]))).read_bytes()
    except Exception as exc:                                    # noqa: BLE001
        raise Refusal(f"the receipt result cannot be read: {exc}") from None
    body = _parse_receipt(raw)
    if body["preflight_action_key"] != key:
        raise Refusal("the receipt names a different preflight action")
    if body["job_identity_hash"] != job:
        raise Refusal("the receipt binds a different job identity")
    if body["image_digest"] != list(images):
        raise Refusal("the receipt binds different container images")
    if body["namespace"] != namespace:
        raise Refusal("the receipt binds a different namespace")
    if body["result"] != "pass":
        raise Refusal("the preflight did not pass")
    created = _aware(body["created_at"], "created_at")
    if created > at:
        raise Refusal("the receipt is dated in the future")
    if body["expires"] is not None and not at < _aware(body["expires"], "expires"):
        raise Refusal("the receipt has expired")
    _check_host_evidence(receipt, body["host_class"])
    return {"authorization": "receipt", "preflight_action_key": key,
            "receipt_sha256": receipt["receipt_sha256"],
            "host_class": body["host_class"]}


# --------------------------------------------------------- exception grants

def verify_exception(decision_id: str, *, job: str, images: Sequence[str],
                     namespace: str, at: datetime.datetime) -> dict:
    """Prove that a CEO decision grants D38 for exactly this job."""

    if not isinstance(decision_id, str) or not _DECISION_ID.match(decision_id):
        raise Refusal("--d38-exception is not a decision id")
    path = Path(DECISION_DIR) / f"{decision_id}.json"
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise Refusal(f"the decision cannot be read: {exc}") from None
    try:
        decision = _strict_json(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise Refusal(f"the decision is not valid JSON: {exc}") from None
    if not isinstance(decision, dict):
        raise Refusal("the decision is not a JSON object")
    if (decision.get("kind") != "decision" or decision.get("state") != "decided"
            or decision.get("verdict") != "approve"):
        raise Refusal("the decision is not a decided approval")
    grant = decision.get("grant")
    scoped = grant.get("d38_exception") if isinstance(grant, dict) else None
    if not isinstance(scoped, dict):
        raise Refusal("the decision carries no structured d38_exception grant")
    if scoped.get("job_identity_hash") != job:
        raise Refusal("the grant binds a different job identity")
    granted = scoped.get("image_digest")
    if not isinstance(granted, list) or sorted(granted) != list(images):
        raise Refusal("the grant binds different container images")
    if scoped.get("namespace") != namespace:
        raise Refusal("the grant binds a different namespace")
    if not at < _aware(scoped.get("expires"), "grant expires"):
        raise Refusal("the grant has expired")
    return {"authorization": "exception", "decision_id": decision_id,
            "decision_sha256": hashlib.sha256(raw).hexdigest()}


# -------------------------------------------------------------------- audit

def write_audit(queue_root: str | Path, event: Mapping[str, object]) -> Path:
    """File one immutable audit event; raise OSError when it cannot be written."""

    directory = Path(queue_root) / AUDIT_DIR_NAME / str(event["job_identity_hash"])
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{int(now().timestamp() * 1e6):016d}-{uuid.uuid4().hex}.json"
    payload = (json.dumps(event, sort_keys=True, indent=1) + "\n").encode()
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
    try:
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return path


# --------------------------------------------------------------------- gate

def _refuse(reason: str, *, job: str, images: Sequence[str],
            namespace: str | None) -> "SystemExit":
    print(f"pbrun: D38 refuses GPU publication: {reason}.\n"
          f"job={job} images={json.dumps(list(images))} "
          f"namespace={namespace or 'none'}\n"
          "Run the same entry point on CPU through PrismaBuild. "
          "Supply --d38-receipt <action key>.\n"
          "Use --d38-exception <decision id> only for an explicit CEO "
          "exception. No runnable submission was published.",
          file=sys.stderr, flush=True)
    return SystemExit(2)


def authorize(args, action: Mapping[str, object], *, cas,
              at: datetime.datetime) -> dict:
    """The evidence that authorizes this job, or raise Refusal."""

    params = action["params"]
    namespace = params.get(NAMESPACE_PARAM)  # type: ignore[union-attr]
    job = str(action["action_key"])
    images = target_images(action)
    receipt = getattr(args, "d38_receipt", None)
    exception = getattr(args, "d38_exception", None)
    if receipt and exception:
        raise Refusal("supply --d38-receipt or --d38-exception, not both")
    if not isinstance(namespace, str) or not _NAMESPACE.match(namespace):
        raise Refusal("the job declares no --d38-namespace descriptor, so no "
                      "receipt can bind its namespace")
    if receipt:
        return verify_receipt(cas, receipt, job=job, images=images,
                              namespace=namespace, at=at)
    if exception:
        return verify_exception(exception, job=job, images=images,
                                namespace=namespace, at=at)
    raise Refusal("no preflight receipt was supplied")


def require(args, action: Mapping[str, object], *, cas, queue_root: str | Path,
            transport: str,
            publishes_nothing: Callable[[], bool] | None = None) -> None:
    """Refuse (exit 2) a new GPU publication without evidence; else audit it.

    ``publishes_nothing`` answers whether this submission would only attach to
    work that already exists.  A CAS hit or a live attachment never needs a
    receipt and never creates a second GPU run.
    """

    if not ENFORCE:
        return
    params = action["params"]
    demand = params.get("demand") or {}  # type: ignore[union-attr]
    tags = (params.get("placement") or {}).get("required_tags") or []  # type: ignore[union-attr]
    if not requires_receipt(demand, tags,
                            host_class=getattr(args, "host_class", None)):
        return
    try:
        if cas.lookup(action) is not None:
            return
    except Exception:                                           # noqa: BLE001
        pass                  # an unreadable cache is not a hit: keep checking
    if publishes_nothing is not None:
        try:
            if publishes_nothing():
                return
        except Exception:                                       # noqa: BLE001
            pass
    job = str(action["action_key"])
    images = target_images(action)
    namespace = params.get(NAMESPACE_PARAM)  # type: ignore[union-attr]
    at = now()
    try:
        evidence = authorize(args, action, cas=cas, at=at)
        event = {
            "schema": AUDIT_SCHEMA, "job_identity_hash": job,
            "image_digest": images, "namespace": namespace,
            "submitter": f"{_user()}@{socket.gethostname()}",
            "time": at.isoformat(), "transport": transport,
            "result": "authorized", **evidence}
        try:
            write_audit(queue_root, event)
        except OSError as exc:
            raise Refusal(f"the audit event cannot be written: {exc}") from None
    except Refusal as exc:
        raise _refuse(str(exc), job=job, images=images,
                      namespace=namespace if isinstance(namespace, str) else None)
    if evidence["authorization"] == "exception":
        print(f"pbrun: D38 exception {evidence['decision_id']} authorizes job "
              f"{job}", file=sys.stderr, flush=True)


def _user() -> str:
    try:
        return getpass.getuser()
    except Exception:                                           # noqa: BLE001
        return "unknown"
