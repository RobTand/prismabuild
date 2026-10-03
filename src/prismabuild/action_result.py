"""Bounded, verified read of one finished action's result (#1446).

The public entry point is :func:`read_verified_action_result`, re-exported by
``prismabuild.client`` as SDK v5 under the capability tag
``verified-action-result-v1``.  It answers one question for a client that must
not reimplement the queue or CAS layout: *what did this exact generation and
attempt of this action publish, and is the evidence still the one the worker
filed?*

Every read is bounded and every answer is owned.  The selected terminal row,
its selected immutable attempt, that attempt's stdout, the sealed request and
the exact execution receipt are each read under an explicit cap through Core's
stable, no-follow regular-file readers, and the payload is returned as bytes
verified against the receipt.  No other attempt or log is read, no historical
``attempt_outcomes`` expansion runs, and no canonical action winner is
consulted.

This is a point-in-time read, not a lease.  A later operator action -- a
withdrawal, a replacement publication, a reaper -- is outside what a read can
hold; the completion recheck only refuses a change that already landed by the
time the read finished.  The caps bound each read and allocation; the decoded
JSON object overhead above them is additional.  Nothing here writes a queue
row, lease, receipt, blob or cache entry, and the synchronous filesystem calls
carry no hard NFS deadline.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from pathlib import Path

from . import core as pb
from . import movement_actions
from . import pool as _pool
from . import produced_output as _produced_output
from . import reader_lease as _reader_lease

#: The schema of the mapping :func:`read_verified_action_result` returns.
ACTION_RESULT_SCHEMA_V1 = "prismabuild.verified_action_result.v1"
#: The capability tag this tree advertises for the bounded verified-result read.
VERIFIED_ACTION_RESULT_TAG = "verified-action-result-v1"
NATIVE_PRODUCER_CONTEXT_SCHEMA_V1 = "prismabuild.native_producer_context.v1"
NATIVE_PRODUCER_CONTEXT_TAG = "native-producer-context-v1"

#: A full lowercase action key: 64 hexadecimal characters, nothing else.
_ACTION_KEY_RE = re.compile(r"[0-9a-f]{64}\Z")
#: Most declared inputs one read may return owned bytes for.  The declared
#: inputs are always returned as descriptors; this bounds only how many a
#: caller may ask to materialize.
MAX_SELECTED_INPUTS = 64


class ActionResultError(RuntimeError):
    """A bounded verified action-result read refused.

    Every refusal of :func:`read_verified_action_result` and
    :func:`bind_standard_capture_command` is this type: a missing, failed,
    withdrawn or contested generation, a wrong publication or attempt, a
    malformed or oversized or tampered record, a payload over its cap, or a
    recipe this first command-binding verifier does not support.  A refusal is
    always fail-closed; it never returns a smaller or guessed answer.
    """


def _checked_action_key(value: object) -> str:
    if not isinstance(value, str) or _ACTION_KEY_RE.fullmatch(value) is None:
        raise ActionResultError("action_key must be a full lowercase hex action key")
    return value


def _checked_publication_unix(value: object) -> float:
    """A caller's publication as a finite float, or a refusal.

    A JSON number reaching this reader may be a Python integer of any size,
    and ``float`` on one past the binary64 range raises ``OverflowError``.
    That is hostile input, so it leaves as :class:`ActionResultError`, never
    as an implementation-specific arithmetic exception (#1446).
    """

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ActionResultError("published_unix must be a finite number")
    try:
        number = float(value)
    except OverflowError as exc:
        raise ActionResultError("published_unix must be a finite number") from exc
    if not math.isfinite(number):
        raise ActionResultError("published_unix must be a finite number")
    return number


def _checked_positive_int(value: object, where: str) -> int:
    if type(value) is not int or value < 1:
        raise ActionResultError(f"{where} must be a positive integer")
    return value


def _checked_nonnegative_int(value: object, where: str) -> int:
    if type(value) is not int or value < 0:
        raise ActionResultError(f"{where} must be a non-negative integer")
    return value


def _normalize_input_limits(
    value: object, evidence_cap: int
) -> dict[str, int]:
    """Validate the optional ``input_limits`` mapping, bounding count and total.

    Keys are declared input ids; values are the per-input byte cap.  The
    number of selected inputs and the sum of their caps are both bounded, so a
    caller cannot turn the optional read into an unbounded one.  ``None`` and
    an empty mapping both mean "descriptors only, read no input bytes".
    """

    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ActionResultError("input_limits must be a mapping of input id to byte cap")
    if len(value) > MAX_SELECTED_INPUTS:
        raise ActionResultError(
            f"input_limits may select at most {MAX_SELECTED_INPUTS} inputs")
    limits: dict[str, int] = {}
    total = 0
    for raw_id, raw_cap in value.items():
        if not isinstance(raw_id, str) or not raw_id:
            raise ActionResultError("input_limits keys must be nonempty input ids")
        cap = _checked_nonnegative_int(raw_cap, f"input_limits[{raw_id!r}]")
        limits[raw_id] = cap
        total += cap
    if total > evidence_cap:
        raise ActionResultError(
            "input_limits aggregate byte caps exceed max_evidence_bytes")
    return limits


def _read_bounded(path, *, where: str, max_bytes: int, require_readonly: bool):
    try:
        return pb._read_regular_file_nofollow(
            path, where=where, require_readonly=require_readonly,
            max_bytes=max_bytes,
        )
    except FileNotFoundError as exc:
        raise ActionResultError(f"{where} is missing: {path}") from exc
    except pb.PrismaBuildError as exc:
        raise ActionResultError(f"{where} refused: {exc}") from exc
    except OSError as exc:
        raise ActionResultError(f"{where} could not be read: {exc}") from exc


def _decode_json(raw: bytes, *, where: str) -> object:
    try:
        return pb._decode_strict_json(raw, where=where)
    except pb.PrismaBuildError as exc:
        raise ActionResultError(f"{where} is not strict JSON: {exc}") from exc


def _select_terminal(queue, key, published, number, evidence_cap):
    """Capture the terminal slots once and resolve the requested generation.

    Returns ``(record, generation, attempt_path)`` for the selected successful
    generation, or refuses.  The capture is bounded and the resolution is
    derived from that same capture, so a census and a resolution can never
    order one record and report another.
    """

    try:
        readable, unreadable = queue.read_terminal_candidates(
            key, max_bytes=evidence_cap)
        ending = queue.resolve_ending(readable, unreadable)
    except (OSError, _pool.PoolError, pb.PrismaBuildError, ValueError) as exc:
        raise ActionResultError(
            f"terminal evidence for {key[:12]} could not be captured: {exc}") from exc
    if unreadable:
        states = ", ".join(str(entry["state"]) for entry in unreadable)
        raise ActionResultError(
            f"terminal evidence for {key[:12]} is present but unreadable: {states}")
    if ending["state"] != _pool.DONE or ending["ambiguous"]:
        raise ActionResultError(
            f"{key[:12]} does not have an unambiguous done ending "
            f"(state={ending['state']!r}, ambiguous={ending['ambiguous']})")
    record = ending["record"]
    generation = ending["generation"]
    if generation is None or generation != published:
        raise ActionResultError(
            f"{key[:12]} is at generation {generation!r}, not the requested "
            f"{published!r}")
    if record.get("action_key") != key:
        raise ActionResultError("the terminal record names another action key")
    if record.get("status") not in {"executed", "cache_hit"}:
        raise ActionResultError(
            f"the terminal status is not a success: {record.get('status')!r}")
    attempts = record.get("attempts")
    if type(attempts) is not int or attempts != number:
        raise ActionResultError(
            f"the requested attempt {number} is not the terminal attempt "
            f"{attempts!r}")
    try:
        _missing, history = queue._attempt_history_links(record)
    except (_pool.PoolError, pb.PrismaBuildError, ValueError) as exc:
        raise ActionResultError(
            f"the terminal's attempt history is malformed: {exc}") from exc
    links = {entry["attempt"]: entry for entry in history}
    link = links.get(number)
    if link is None:
        raise ActionResultError(
            f"attempt {number} is not linked by the terminal's history")
    attempt_path = queue.attempt_path(record, number)
    if link.get("outcome") != str(attempt_path.relative_to(queue.root)):
        raise ActionResultError("the selected attempt link is not its canonical path")
    return record, generation, attempt_path


def _read_selected_attempt(queue, path, evidence_cap):
    raw = _read_bounded(
        path, where="pool attempt outcome", max_bytes=evidence_cap,
        require_readonly=True)
    value = _decode_json(raw, where="pool attempt outcome")
    if not isinstance(value, dict):
        raise ActionResultError("pool attempt outcome is not an object")
    return value


def _bind_attempt(
    queue,
    record: Mapping[str, object],
    attempt_record: Mapping[str, object],
    *,
    key: str,
    number: int,
) -> None:
    """Bind the immutable attempt to the terminal summary, exactly.

    The identity check is the queue's own one owner
    (:meth:`PoolQueue._bind_linked_attempt`), which the queue also applies to
    every linked attempt it reads; this reader adds only the success-specific
    conditions, so the two cannot drift on which fields must agree -- the
    preemption context included (#1446).
    """

    try:
        queue._bind_linked_attempt(
            record, attempt_record, attempt=number, where=f"{key[:12]} attempt {number}")
    except (_pool.PoolError, pb.PrismaBuildError) as exc:
        raise ActionResultError(f"pool attempt outcome refused: {exc}") from exc
    if attempt_record.get("status") != record.get("status"):
        raise ActionResultError("pool attempt status differs from the terminal")
    if attempt_record.get("disposition") != _pool.DONE:
        raise ActionResultError("pool attempt disposition is not done")
    if attempt_record.get("status") not in {"executed", "cache_hit"}:
        raise ActionResultError("pool attempt status is not a success")
    detail = attempt_record.get("detail")
    if not isinstance(detail, Mapping):
        raise ActionResultError("pool attempt detail must be an object")
    returncode = detail.get("returncode")
    if type(returncode) is not int or returncode != 0:
        raise ActionResultError(
            f"pool attempt did not exit zero: {returncode!r}")


def _result_announcement(stdout: bytes) -> Mapping[str, object]:
    """The one worker result announcement in a selected attempt's stdout.

    ``core.main`` prints its result object as the last stdout line; the object
    carries the execution ``receipt``.  A line is a candidate only when it is a
    whole strict JSON object holding a ``receipt`` object with a string
    ``receipt_sha256``.  Exactly one candidate must exist: zero means the run
    did not announce a result, and two -- even identical -- mean the log is not
    the one unambiguous account the reader can bind.

    Each candidate line is decoded by Core's strict reader, not permissive
    ``json.loads``: a duplicate key or a non-finite number is not an
    announcement, so a poisoned line cannot name a receipt the run did not
    file.
    """

    text = stdout.decode("utf-8", errors="replace")
    found: list[Mapping[str, object]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("{") or not stripped.endswith("}"):
            continue
        try:
            value = pb._decode_strict_json(
                stripped.encode("utf-8"), where="pool attempt stdout line")
        except pb.PrismaBuildError:
            continue
        if not isinstance(value, dict):
            continue
        receipt = value.get("receipt")
        if (
            isinstance(receipt, Mapping)
            and isinstance(receipt.get("receipt_sha256"), str)
        ):
            found.append(value)
    if len(found) != 1:
        raise ActionResultError(
            f"expected exactly one result announcement, found {len(found)}")
    return found[0]


def _read_declared_input(
    cas: pb.PrismaBuildCAS,
    entry: Mapping[str, object],
    *,
    cap: int,
) -> bytes:
    """Owned bytes of one selected declared input, under its explicit cap.

    This reader owns the *selection* and the per-input cap; the bounded
    owned-blob read itself is Core's one owner
    (:meth:`PrismaBuildCAS.read_declared_blob`), so the CAS blob validation
    recipe is not spelled twice (#1446).
    """

    if int(entry["bytes"]) > cap:
        raise ActionResultError(
            f"declared input {entry['id']!r} exceeds its byte cap {cap}: "
            f"{cas.blob_path(str(entry['sha256']))}")
    try:
        return cas.read_declared_blob(
            entry, max_bytes=cap, where="CAS input payload")
    except (pb.PrismaBuildError, OSError) as exc:
        raise ActionResultError(str(exc)) from exc


def _native_producer_context(queue, record, attempt_record, action, receipt, *,
                             evidence_cap: int) -> dict[str, object]:
    """Bind native provenance to the held selection, never a live lookup."""

    key = action["action_key"]
    worker, host = attempt_record.get("claimed_by"), attempt_record.get("claimed_host")
    if (not isinstance(worker, str) or not worker
            or not isinstance(host, str) or not host
            or record.get("claimed_by") != worker
            or record.get("claimed_host") != host):
        raise ActionResultError("native producer claim identity is missing or inconsistent")
    detail = attempt_record["detail"]
    telemetry = detail.get("resource_telemetry")
    if not isinstance(telemetry, Mapping) or telemetry.get("complete") is not True:
        raise ActionResultError("selected immutable attempt has no complete native telemetry")
    nonce = telemetry.get("nonce")
    if not isinstance(nonce, str) or re.fullmatch(r"[0-9a-f]{32}", nonce) is None:
        raise ActionResultError("native producer nonce is invalid")
    scope_id = _produced_output._broker_scope_id(key, nonce)
    if (telemetry.get("action_key") != key or telemetry.get("host") != host
            or telemetry.get("scope_unit") != scope_id):
        raise ActionResultError("native producer telemetry identity differs from selection")
    scope = record.get("resource_scope")
    if (not isinstance(scope, Mapping) or scope.get("action_key") != key
            or scope.get("nonce") != nonce or scope.get("scope_id") != scope_id):
        raise ActionResultError("native producer scope differs from selected claim")
    resources = record.get("resources")
    demand = action["params"].get("demand")
    if (not isinstance(resources, Mapping) or not resources
            or not isinstance(demand, Mapping) or resources != demand
            or any(not isinstance(k, str) or not k or type(v) is not int or v < 0
                   for values in (resources, demand) for k, v in values.items())):
        raise ActionResultError("native producer claim resources differ from sealed demand")
    memory = resources.get("mem_gb")
    cpus = resources.get("cpu")
    if type(memory) is not int or memory < 1 or type(cpus) is not int or cpus < 1:
        raise ActionResultError("native producer demand has no positive CPU/memory budget")
    memory_bytes = memory * 1024 ** 3
    if (type(telemetry.get("memory_max_bytes")) is not int
            or telemetry["memory_max_bytes"] != memory_bytes
            or type(scope.get("memory_max_bytes")) is not int
            or scope["memory_max_bytes"] != memory_bytes):
        raise ActionResultError("native producer memory budget differs from sealed demand")
    allocation = detail.get("cpu_allocation")
    if not isinstance(allocation, Mapping) or allocation != record.get("cpu_allocation"):
        raise ActionResultError("native producer CPU allocation differs from selected claim")
    preferred, fallback = allocation.get("preferred"), allocation.get("fallback")
    if not isinstance(preferred, list) or not isinstance(fallback, list):
        raise ActionResultError("native producer CPU allocation is incomplete")
    assigned = preferred + fallback
    if (any(type(cpu) is not int or cpu < 0 for cpu in assigned)
            or len(assigned) != cpus or len(set(assigned)) != cpus):
        raise ActionResultError("native producer CPU allocation differs from sealed demand")
    cleanup = record.get("resource_scope_cleanup")
    if (not isinstance(cleanup, Mapping) or cleanup.get("complete") is not True
            or cleanup.get("nonce") != nonce):
        raise ActionResultError("native producer scope settlement is incomplete")
    clean_telemetry = cleanup.get("telemetry")
    if (not isinstance(clean_telemetry, Mapping)
            or clean_telemetry.get("complete") is not True
            or any(clean_telemetry.get(k) != telemetry.get(k) for k in
                   ("action_key", "nonce", "scope_unit", "host", "memory_max_bytes"))):
        raise ActionResultError("native producer settlement telemetry differs from attempt")
    if isinstance(cleanup.get("export"), Mapping):
        _checked_publication_unix(cleanup["export"].get("stopped_unix"))
    ok, reason = _reader_lease.export_verdict_proves_empty(
        cleanup.get("export"), scope_id=scope_id)
    if not ok:
        raise ActionResultError(f"native producer settlement refused: {reason}")
    proof_raw = _read_bounded(
        _reader_lease.attestation_path(queue, key, nonce),
        where="native producer scope attestation", max_bytes=evidence_cap,
        require_readonly=False)  # This owner atomically files a mutable proof.
    proof = _decode_json(proof_raw, where="native producer scope attestation")
    if isinstance(proof, Mapping):
        _checked_publication_unix(proof.get("stopped_unix"))
    ok, reason = _reader_lease._scope_attestation_payload_proves_empty(
        proof, action_key=key, nonce=nonce, scope_id=scope_id)
    if not ok:
        raise ActionResultError(f"native producer scope attestation refused: {reason}")
    assert isinstance(proof, Mapping)
    if (proof.get("worker") != worker or proof.get("incarnation") != worker
            or proof.get("host") != host
            or any(proof.get(k) != cleanup["export"].get(k) for k in
                   ("stopped_unix", "released", "retired", "settled", "empty",
                    "tickets_pending"))):
        raise ActionResultError("native producer scope proof differs from selected claim")
    producer = receipt["producer"]
    if (producer.get("worker_id") != host
            or producer["evidence"].get("hostname") != host):
        raise ActionResultError("execution receipt names a foreign native producer host")
    runtime = producer["runtime"]  # Already validated by the exact CAS read.
    core, launcher = runtime["core"], runtime["launcher"]
    if runtime["launch_kind"] != "script" or not isinstance(launcher, Mapping):
        raise ActionResultError("native producer receipt has no attested launcher")
    core_path, launcher_path = Path(core["path"]), Path(launcher["path"])
    root = core_path.parent.parent.parent
    if (not root.is_absolute() or core_path != root / "src/prismabuild/core.py"
            or launcher_path != root / "tools/prismabuild_worker.py"
            or record.get("worker_script") != str(launcher_path)):
        raise ActionResultError("native producer helper root differs from attested launcher")
    for source in (core, launcher):
        path = Path(source["path"])
        try:
            canonical = path.resolve(strict=True)
        except (OSError, RuntimeError) as exc:
            raise ActionResultError("native producer runtime path cannot be resolved") from exc
        if str(canonical) != source["path"] or source["path"] != source["resolved_path"]:
            raise ActionResultError("native producer runtime path is not canonical")
        raw = _read_bounded(path, where="native producer runtime source",
                            max_bytes=evidence_cap, require_readonly=True)
        if len(raw) != source["bytes"] or pb.raw_sha256(raw) != source["sha256"]:
            raise ActionResultError("native producer runtime source differs from receipt")
    return {
        "schema": NATIVE_PRODUCER_CONTEXT_SCHEMA_V1,
        "queue_root": str(Path(queue.root).resolve()),
        "action_key": key, "published_unix": record["published_unix"],
        "attempt": attempt_record["attempt"],
        "generation": queue.attempt_generation(record),
        "nonce": nonce, "scope_id": scope_id, "worker": worker, "host": host,
        "incarnation": worker, "helper_root": str(root),
        "resources": dict(resources),
        "resources_semantics": "selected-claim-sealed-demand",
        "attempt_source": "selected-immutable-attempt",
        "receipt_sha256": receipt["receipt_sha256"],
        "runtime_sha256": runtime["runtime_sha256"],
        "scope_attestation_sha256": pb.raw_sha256(proof_raw),
    }


def read_verified_action_result(
    queue,
    action_key: str,
    *,
    published_unix: float,
    attempt: int,
    max_result_bytes: int,
    max_evidence_bytes: int = 4 * 1024 * 1024,
    input_limits: Mapping[str, int] | None = None,
    require_native_producer_context: bool = False,
) -> dict[str, object]:
    """Read one action generation's verified result bytes.

    ``queue`` is a :class:`prismabuild.client.PoolQueue`; ``action_key``,
    ``published_unix`` and ``attempt`` select the exact generation and attempt
    the caller expects.  ``max_result_bytes`` and ``max_evidence_bytes`` bound
    the payload and every evidence read; ``input_limits`` optionally names
    declared inputs whose owned bytes the caller also wants, each under its own
    cap, bounded in count and in aggregate by ``max_evidence_bytes``.

    ``require_native_producer_context=True`` requires actual native production
    and adds ``producer_context`` from this held selection only. Missing, legacy
    and cache-hit native evidence refuses. The default preserves the generic
    result contract. Resources mean the selected claim equal to sealed demand,
    not ledger tokens or export allowances.
    The returned mapping is freshly owned and carries the schema, the exact
    identity, the selected worker and host, the validated sealed request, the
    validated execution receipt, the verified ``payload`` bytes, the declared
    input descriptors and, for each selected input, its owned bytes.  No
    payload path is returned to reopen and no domain-specific decoding is done;
    the consumer checks its own code, regime and counter schema over these
    bytes.
    """

    if type(require_native_producer_context) is not bool:
        raise ActionResultError("require_native_producer_context must be a boolean")
    key = _checked_action_key(action_key)
    published = _checked_publication_unix(published_unix)
    number = _checked_positive_int(attempt, "attempt")
    result_cap = _checked_nonnegative_int(max_result_bytes, "max_result_bytes")
    evidence_cap = _checked_positive_int(max_evidence_bytes, "max_evidence_bytes")
    limits = _normalize_input_limits(input_limits, evidence_cap)

    record, generation, attempt_path = _select_terminal(
        queue, key, published, number, evidence_cap)
    attempt_record = _read_selected_attempt(queue, attempt_path, evidence_cap)
    _bind_attempt(queue, record, attempt_record, key=key, number=number)

    cas_root = record.get("cas_root")
    if not isinstance(cas_root, str) or not cas_root:
        raise ActionResultError("the terminal record names no CAS root")
    try:
        cas = pb.PrismaBuildCAS(cas_root)
    except pb.PrismaBuildError as exc:
        raise ActionResultError(f"the terminal CAS root is invalid: {exc}") from exc

    # The sealed request: bounded, stable, no-follow, and validated as the
    # action whose key the caller asked for.  A tampered or foreign request is
    # a refusal, never a fallback to a live checkout.
    request_path = cas.root / "requests" / key[:2] / f"{key}.json"
    request = _decode_json(
        _read_bounded(request_path, where="PrismaBuild action request",
                      max_bytes=evidence_cap, require_readonly=True),
        where="PrismaBuild action request")
    try:
        action = pb.validate_action(request)
    except pb.PrismaBuildError as exc:
        raise ActionResultError(f"sealed action request refused: {exc}") from exc
    if action.get("action_key") != key:
        raise ActionResultError("sealed action request names another action key")

    # The worker's own result announcement, from this attempt's immutable
    # stdout log only.  The log's declared size and digest are checked by the
    # bounded stable read's caller below before the announcement is parsed.
    logs = attempt_record.get("logs")
    stdout_meta = logs.get("stdout") if isinstance(logs, Mapping) else None
    if not isinstance(stdout_meta, Mapping):
        raise ActionResultError("the selected attempt has no stdout log")
    declared_bytes = stdout_meta.get("bytes")
    declared_sha = stdout_meta.get("sha256")
    if type(declared_bytes) is not int or declared_bytes < 0:
        raise ActionResultError("the selected attempt stdout size is invalid")
    if (
        not isinstance(declared_sha, str)
        or len(declared_sha) != 64
        or any(character not in "0123456789abcdef" for character in declared_sha)
    ):
        raise ActionResultError("the selected attempt stdout digest is invalid")
    if declared_bytes > evidence_cap:
        raise ActionResultError("the selected attempt stdout exceeds the evidence cap")
    stdout_path = queue.attempt_log_path(record, number, "stdout", declared_sha)
    if stdout_meta.get("path") != str(stdout_path.relative_to(queue.root)):
        raise ActionResultError("the selected attempt stdout link is not canonical")
    stdout = _read_bounded(
        stdout_path, where="pool attempt stdout", max_bytes=evidence_cap,
        require_readonly=True)
    if len(stdout) != declared_bytes or pb.raw_sha256(stdout) != declared_sha:
        raise ActionResultError("the selected attempt stdout differs from its address")

    announcement = _result_announcement(stdout)
    announced_receipt = announcement.get("receipt")
    assert isinstance(announced_receipt, Mapping)
    receipt_sha256 = announced_receipt.get("receipt_sha256")
    if not isinstance(receipt_sha256, str) or len(receipt_sha256) != 64:
        raise ActionResultError("the result announcement names no receipt digest")

    try:
        receipt, payload = cas.read_execution_result(
            action, receipt_sha256, max_result_bytes=result_cap,
            max_receipt_bytes=evidence_cap)
    except (pb.PrismaBuildError, OSError) as exc:
        raise ActionResultError(f"execution receipt refused: {exc}") from exc
    if dict(announced_receipt) != receipt:
        raise ActionResultError(
            "the result announcement receipt differs from the execution receipt")

    native_context = None
    if require_native_producer_context:
        if announcement.get("status") != "published":
            raise ActionResultError("native producer context requires an actual publication, not a cache hit")
        native_context = _native_producer_context(
            queue, record, attempt_record, action, receipt, evidence_cap=evidence_cap)

    descriptors: list[dict[str, object]] = []
    input_payloads: dict[str, bytes] = {}
    declared = action.get("inputs")
    assert isinstance(declared, list)
    declared_ids = {str(entry["id"]) for entry in declared}
    for identity in limits:
        if identity not in declared_ids:
            raise ActionResultError(
                f"input_limits names an undeclared input: {identity!r}")
    for entry in declared:
        assert isinstance(entry, Mapping)
        descriptors.append(dict(entry))
        identity = str(entry["id"])
        if identity in limits:
            input_payloads[identity] = _read_declared_input(
                cas, entry, cap=limits[identity])

    # Completion recheck: the same generation, attempt, outcome and selected
    # identity must still stand, and the held attempt must still bind to the
    # terminal it was read from.  A newer failure, withdrawal or replacement
    # refuses; so does an attempt whose identity or preemption context changed
    # in place (#1446).
    rechecked, rechecked_generation, rechecked_path = _select_terminal(
        queue, key, published, number, evidence_cap)
    rechecked_attempt = _read_selected_attempt(queue, rechecked_path, evidence_cap)
    _bind_attempt(queue, rechecked, rechecked_attempt, key=key, number=number)
    if (
        rechecked_generation != generation
        or rechecked.get("status") != record.get("status")
        or rechecked_path != attempt_path
        or rechecked.get("attempts") != record.get("attempts")
        or rechecked_attempt != attempt_record
    ):
        raise ActionResultError(
            "the selected ending changed while the result was read")

    if require_native_producer_context:
        native_fields = ("claimed_by", "claimed_host", "resources", "resource_scope",
                         "resource_scope_cleanup", "cpu_allocation", "worker_script")
        if any(rechecked.get(k) != record.get(k) for k in native_fields):
            raise ActionResultError("the selected native ending changed while the result was read")
        if _native_producer_context(
                queue, rechecked, rechecked_attempt, action, receipt,
                evidence_cap=evidence_cap) != native_context:
            raise ActionResultError("the selected native proof changed while the result was read")

    worker = attempt_record.get("claimed_by")
    host = attempt_record.get("claimed_host")
    generation_digest = queue.attempt_generation(record)
    result = {
        "schema": ACTION_RESULT_SCHEMA_V1,
        "action_key": key,
        "published_unix": published,
        "attempt": number,
        "generation": generation_digest,
        "worker_id": worker if isinstance(worker, str) else None,
        "host": host if isinstance(host, str) else None,
        "request": action,
        "receipt": receipt,
        "payload": payload,
        "inputs": descriptors,
        "input_payloads": input_payloads,
    }
    if native_context is not None:
        result["producer_context"] = native_context
    return result


def bind_standard_capture_command(request: object) -> list[str]:
    """Prove a sealed request is pbrun's standard captured-log recipe.

    A generic result reader returns the sealed request honestly but does not
    claim that its ``params.command`` metadata is what the worker executed:
    Core runs ``task.argv``, and ``validate_action`` only checks the schema.
    This helper reconstructs the one standard capture wrapper pbrun seals --
    from the validated request's ``environment.variables.PATH``,
    ``params.command`` and ``task.result_path`` -- and requires the sealed
    ``task.argv`` to equal it byte for byte.  It returns the reconstructed argv
    on success.

    The first implementation supports the standard captured-log result only,
    where the declared ``result_path`` *is* the captured log's name.  A
    file-result recipe (a declared result other than the log), a scratch
    recorder, a legacy or any other wrapper refuses explicitly; nothing here
    parses a shell, imports a CLI, or clones PrismaQuant.
    """

    try:
        action = pb.validate_action(request)
    except pb.PrismaBuildError as exc:
        raise ActionResultError(f"request refused: {exc}") from exc
    task = action["task"]
    params = action["params"]
    environment = action["environment"]
    assert isinstance(task, Mapping) and isinstance(params, Mapping)
    assert isinstance(environment, Mapping)
    command = params.get("command")
    if (
        not isinstance(command, list)
        or not command
        or any(not isinstance(word, str) for word in command)
    ):
        raise ActionResultError(
            "the request declares no string params.command to bind")
    variables = environment.get("variables")
    path = variables.get("PATH") if isinstance(variables, Mapping) else None
    if not isinstance(path, str) or not path:
        raise ActionResultError("the request declares no environment PATH to bind")
    log_name = task.get("result_path")
    if not isinstance(log_name, str) or not log_name:
        raise ActionResultError("the request declares no result_path to bind")
    expected = movement_actions.standard_capture_argv(
        command, log_name, path_prefix=path.split(":", 1)[0])
    sealed = task.get("argv")
    if not isinstance(sealed, list) or list(sealed) != expected:
        raise ActionResultError(
            "the sealed task.argv is not the standard captured-log recipe; "
            "file-result, legacy and unsupported recipes are refused")
    return expected


__all__ = [
    "ACTION_RESULT_SCHEMA_V1",
    "VERIFIED_ACTION_RESULT_TAG",
    "NATIVE_PRODUCER_CONTEXT_SCHEMA_V1",
    "NATIVE_PRODUCER_CONTEXT_TAG",
    "MAX_SELECTED_INPUTS",
    "ActionResultError",
    "read_verified_action_result",
    "bind_standard_capture_command",
]
