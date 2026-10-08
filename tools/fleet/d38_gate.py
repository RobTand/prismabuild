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
import json
import os
from pathlib import Path
import re
import socket
import sys
import uuid
from typing import Callable, Mapping, Sequence

from prismabuild import action_result, container_images, core as pb, digest_primitives

RECEIPT_SCHEMA = "fleet.d38.preflight.v1"
AUDIT_SCHEMA = "fleet.d38.audit.v1"
PRODUCER_PARAM = "d38_preflight"
PRODUCER_ID = "fleet.d38.producer.v1"
NAMESPACE_PARAM = "d38_namespace"
PLAN_SCHEMA = "fleet.d38.plan.v1"
#: Reviewed invocation descriptors, keyed ``<kind>:<interpreter>:<target>`` for a
#: ``script`` or ``module`` entry point.  Each lists the exact CPU changes a
#: preflight of that entry point may make to the target's arguments:
#: ``{"cpu_changes": [{"flag": "--device", "from": "cuda", "to": "cpu"}]}``.
#: A harness owner adds an entry here through review.  None ships, so until one
#: does no receipt can authorize a job and the scoped exception is the only
#: path -- which is what D38 says of a launcher it cannot identify.
INVOCATIONS: dict[str, dict] = {}
#: The input id a preflight action files its plan under.  The plan is the CAS
#: object that binds the receipt to a target and to the CPU invocation that
#: stands in for it (D38).
PLAN_INPUT_ID = "d38-plan"
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


def utc_now() -> datetime.datetime:
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

def _json_without_duplicates(raw: bytes) -> object:
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
        descriptor = _json_without_duplicates(raw)
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
        body = _json_without_duplicates(raw)
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


def _plan_input(preflight: Mapping[str, object]) -> Mapping[str, object]:
    found = [entry for entry in preflight.get("inputs") or ()  # type: ignore[union-attr]
             if isinstance(entry, Mapping) and entry.get("id") == PLAN_INPUT_ID]
    if len(found) != 1:
        raise Refusal(f"the preflight action must carry exactly one {PLAN_INPUT_ID} "
                      "CAS input (its target plan)")
    return found[0]


def load_plan(cas, preflight: Mapping[str, object]) -> dict:
    """The target plan the preflight action declares, verified against the CAS.

    The plan is a CAS input of the preflight action, and its digest is the one
    the action declares, so a label and an arbitrary digest prove nothing: the
    bytes are read back and hashed.
    """

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
    entry = _plan_input(preflight)
    if entry.get("sha256") != declared["plan_sha256"]:
        raise Refusal("the declared plan digest is not the plan input's digest")
    try:
        raw = Path(cas.input_path(entry)).read_bytes()
    except Exception as exc:                                    # noqa: BLE001
        raise Refusal(f"the plan input cannot be read: {exc}") from None
    if digest_primitives.raw_sha256(raw) != declared["plan_sha256"]:
        raise Refusal("the plan input bytes do not match their digest")
    if len(raw) > MAX_RECEIPT_BYTES:
        raise Refusal("the plan is larger than 64 KiB")
    try:
        plan = _json_without_duplicates(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise Refusal(f"the plan is not valid JSON: {exc}") from None
    if not isinstance(plan, dict) or plan.get("schema") != PLAN_SCHEMA:
        raise Refusal("the plan schema is not fleet.d38.plan.v1")
    return plan


def _plan_token_list(value: object, where: str) -> list[str]:
    if not isinstance(value, list) or not value or any(
            not isinstance(item, str) for item in value):
        raise Refusal(f"the plan {where} is not a non-empty list of strings")
    return value


def _split_invocation(command: Sequence[str], entry: Mapping[str, object]
                      ) -> list[str]:
    """The arguments after the entry point, or Refusal if it is not the entry.

    The entry point is a script (``[interpreter, script, *args]``) or a module
    (``[interpreter, "-m", module, *args]``).  Nothing here parses a shell: a
    target that starts with ``-`` is an option, so ``bash -lc "..."`` and every
    other launcher whose program sits in a later slot cannot be identified, and
    needs a reviewed descriptor or a CEO exception.
    """

    kind, interpreter, target = (entry.get("kind"), entry.get("interpreter"),
                                 entry.get("target"))
    if kind not in ("script", "module") or not all(
            isinstance(item, str) and item for item in (interpreter, target)):
        raise Refusal("the plan's entry point is not a script or module "
                      "invocation")
    if target.startswith("-"):  # type: ignore[union-attr]
        raise Refusal("the entry point is an option, not a program: an opaque "
                      "launcher or shell needs a reviewed invocation "
                      "descriptor, or a CEO exception")
    head = ([interpreter, target] if kind == "script"
            else [interpreter, "-m", target])
    if list(command[:len(head)]) != head:
        raise Refusal("a command does not run the entry point the plan names")
    return list(command[len(head):])


def check_plan_binds(plan: Mapping[str, object], preflight: Mapping[str, object], *,
                     job: str, images: Sequence[str], namespace: str,
                     target_command: Sequence[str]) -> None:
    """The plan names this target, and the CPU run is this target's run.

    The entry point is identified by the plan (a script or module) and must be
    the one both commands run.  A reviewed descriptor (:data:`INVOCATIONS`) lists
    the CPU changes that entry point allows, and the CPU arguments must equal the
    target's arguments with exactly the plan's listed changes applied: a changed
    program, module, input or check argument is a difference no descriptor lists.
    """

    target = plan.get("target")
    if not isinstance(target, Mapping):
        raise Refusal("the plan names no target")
    if target.get("job_identity_hash") != job:
        raise Refusal("the plan binds a different job identity")
    if target.get("image_digest") != list(images):
        raise Refusal("the plan binds different container images")
    if target.get("namespace") != namespace:
        raise Refusal("the plan binds a different namespace")
    entry = plan.get("entry")
    if not isinstance(entry, Mapping):
        raise Refusal("the plan names no entry point")
    planned_target = _plan_token_list(plan.get("target_command"), "target_command")
    cpu = _plan_token_list(plan.get("cpu_command"), "cpu_command")
    if planned_target != list(target_command):
        raise Refusal("the plan's target command is not this job's command")
    if cpu != list(preflight["params"]["command"]):  # type: ignore[index]
        raise Refusal("the plan's CPU command is not the preflight action's command")
    target_args = _split_invocation(planned_target, entry)
    cpu_args = _split_invocation(cpu, entry)
    key = f"{entry['kind']}:{entry['interpreter']}:{entry['target']}"
    descriptor = INVOCATIONS.get(key)
    allowed = descriptor.get("cpu_changes") if isinstance(descriptor, Mapping) else None
    if not isinstance(allowed, list):
        raise Refusal(f"no reviewed invocation descriptor exists for the entry "
                      f"point {key}; a CEO exception is the only path until one "
                      "is added")
    changes = plan.get("changes")
    if not isinstance(changes, list):
        raise Refusal("the plan's changes are not a list")
    expected = list(target_args)
    for change in changes:
        if (not isinstance(change, Mapping)
                or set(change) != {"flag", "from", "to"}
                or any(not isinstance(change[name], str)
                       for name in ("flag", "from", "to"))
                or dict(change) not in allowed):
            raise Refusal("the plan lists a CPU change the reviewed descriptor "
                          "does not allow")
        at = [i for i in range(len(expected) - 1)
              if expected[i] == change["flag"] and expected[i + 1] == change["from"]]
        if len(at) != 1:
            raise Refusal("a listed CPU change does not occur exactly once in "
                          "the target's arguments")
        expected[at[0] + 1] = change["to"]
    if expected != cpu_args:
        raise Refusal("the CPU command differs from the target command by more "
                      "than the plan's reviewed CPU changes")


def check_cpu_visibility(preflight: Mapping[str, object], host_class: str) -> None:
    """The proved namespace hides every GPU, read from what the action sealed.

    The host's accelerator inventory is not task visibility: a CPU-only
    container on a Spark still has the Spark's GPU in its host evidence.  What
    proves the run was CPU-only is the environment the preflight action sealed.
    """

    variables = (preflight.get("environment") or {}).get("variables") or {}  # type: ignore[union-attr]
    if variables.get("CUDA_VISIBLE_DEVICES") != "":
        raise Refusal("the preflight action does not seal CUDA_VISIBLE_DEVICES "
                      "empty, so it did not hide the GPU")
    if host_class == "gb10" and variables.get("NVIDIA_VISIBLE_DEVICES") != "none":
        raise Refusal("a gb10 preflight must seal NVIDIA_VISIBLE_DEVICES=none")


def check_host_class(receipt: Mapping[str, object], host_class: str) -> None:
    """The producer ran on the host class the receipt says."""

    producer = receipt.get("producer")
    evidence = producer.get("evidence") if isinstance(producer, Mapping) else None
    if not isinstance(evidence, Mapping):
        raise Refusal("the receipt carries no producer host evidence")
    if str(evidence.get("machine")) not in _MACHINES[host_class]:
        raise Refusal(f"the producer ran on {evidence.get('machine')!r}, not on "
                      f"a {host_class} host")


def verify_receipt(cas, key: str, *, job: str, images: Sequence[str],
                   namespace: str, target_command: Sequence[str],
                   at: datetime.datetime) -> dict:
    """Prove that one preflight receipt binds this job, or raise Refusal."""

    preflight, receipt = _verified_receipt(cas, key)
    try:
        # What the worker executes is task.argv, not the descriptive
        # params.command: prove the first is the recipe of the second.
        action_result.bind_standard_capture_command(preflight)
    except action_result.ActionResultError as exc:
        raise Refusal(f"the preflight task does not execute its declared "
                      f"command: {exc}") from None
    plan = load_plan(cas, preflight)
    check_plan_binds(plan, preflight, job=job, images=images,
                     namespace=namespace, target_command=target_command)
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
    check_cpu_visibility(preflight, body["host_class"])
    check_host_class(receipt, body["host_class"])
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
        decision = _json_without_duplicates(raw)
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
            "decision_sha256": digest_primitives.raw_sha256(raw)}


# -------------------------------------------------------------------- audit

def write_audit(queue_root: str | Path, event: Mapping[str, object]) -> Path:
    """File one immutable audit event; raise OSError when it cannot be written."""

    directory = Path(queue_root) / AUDIT_DIR_NAME / str(event["job_identity_hash"])
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{int(utc_now().timestamp() * 1e6):016d}-{uuid.uuid4().hex}.json"
    payload = (digest_primitives.sorted_json(dict(event)) + "\n").encode()
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
    try:
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return path


# --------------------------------------------------------------------- gate

def _exit_refused(reason: str, *, job: str, images: Sequence[str],
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
        return verify_receipt(
            cas, receipt, job=job, images=images, namespace=namespace,
            target_command=params["command"], at=at)  # type: ignore[index]
    if exception:
        return verify_exception(exception, job=job, images=images,
                                namespace=namespace, at=at)
    raise Refusal("no preflight receipt was supplied")


def judge_publication(args, action: Mapping[str, object], *, cas, queue_root: str | Path,
           transport: str) -> tuple[str, str | None] | None:
    """Authorize a new GPU publication; return ``(reason, namespace)`` if refused.

    ``None`` means the job is not GPU intent or its evidence authorized it, and
    the authorization is audited.  There is no exemption here for a cache hit
    or a live run: whether new work would be created is decided inside the
    publication, and a liveness read taken earlier goes stale.  The caller turns
    a refusal into a path that cannot publish (a wait on a run it can see) or
    into :func:`refusal`.
    """

    if not ENFORCE:
        return None
    params = action["params"]
    demand = params.get("demand") or {}  # type: ignore[union-attr]
    tags = (params.get("placement") or {}).get("required_tags") or []  # type: ignore[union-attr]
    if not requires_receipt(demand, tags,
                            host_class=getattr(args, "host_class", None)):
        return None
    job = str(action["action_key"])
    images = target_images(action)
    namespace = params.get(NAMESPACE_PARAM)  # type: ignore[union-attr]
    at = utc_now()
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
        return (str(exc), namespace if isinstance(namespace, str) else None)
    if evidence["authorization"] == "exception":
        print(f"pbrun: D38 exception {evidence['decision_id']} authorizes job "
              f"{job}", file=sys.stderr, flush=True)
    return None


def refusal_exit(verdict: tuple[str, str | None], action: Mapping[str, object]
            ) -> SystemExit:
    """The exit-2 refusal for a verdict :func:`decide` returned."""

    reason, namespace = verdict
    return _exit_refused(reason, job=str(action["action_key"]),
                   images=target_images(action), namespace=namespace)


def require(args, action: Mapping[str, object], *, cas, queue_root: str | Path,
            transport: str) -> None:
    """Refuse (exit 2) a new GPU publication without evidence; else audit it."""

    verdict = judge_publication(args, action, cas=cas, queue_root=queue_root,
                     transport=transport)
    if verdict is not None:
        raise refusal_exit(verdict, action)


def refuse_deferred(args, params: Mapping[str, object]) -> None:
    """Refuse (exit 2) a deferred ``--after`` submission with GPU intent.

    A deferred job has no identity until its producer ends: the data manifest
    that completes the key is not known yet.  No receipt or grant can bind a
    key that does not exist, so D38 could never authorize it.  Filing it would
    queue work that is refused at release; refusing now says so while the
    submitter is still there.  The release checks the sealed key again, for a
    record an older client filed.
    """

    if not ENFORCE:
        return
    demand = params.get("demand") or {}
    tags = (params.get("placement") or {}).get("required_tags") or []  # type: ignore[union-attr]
    if not requires_receipt(demand, tags,  # type: ignore[arg-type]
                            host_class=getattr(args, "host_class", None)):
        return
    images = list(container_images.normalize_refs(
        params.get("container_images") or []))
    namespace = params.get(NAMESPACE_PARAM)
    raise _exit_refused(
        "a deferred --after submission has no job identity until its producer "
        "ends, so no receipt or grant can bind it; submit it after the "
        "producer succeeds", job="none (deferred)", images=images,
        namespace=namespace if isinstance(namespace, str) else None)


def _user() -> str:
    try:
        return getpass.getuser()
    except Exception:                                           # noqa: BLE001
        return "unknown"
