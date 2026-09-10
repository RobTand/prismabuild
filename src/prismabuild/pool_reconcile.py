"""Explicit action-receipt recovery beside an immutable failed pool attempt.

Receipts bind actions, not pool attempts. This operation can verify reusable
output without inventing a successful broker exit or changing queue state.
"""
from __future__ import annotations

import hashlib
import math
from pathlib import Path
import re
import stat

from . import core as pb, pool


SCHEMA = "prismaquant.prismabuild.pool_receipt_reconciliation.v1"
BROKER_EOF = ("PrismaBuild resource execution refused: resource broker closed "
              "without an execution result\n")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise pool.PoolContractError(message)


def _number(value: object) -> bool:
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def _read(path: Path, *, readonly: bool = False) -> tuple[dict, bytes]:
    raw = pb._read_regular_file_nofollow(
        path, where="pool reconciliation evidence", require_readonly=readonly)
    value = pb._decode_strict_json(raw, where="pool reconciliation evidence")
    _require(isinstance(value, dict), f"evidence is not an object: {path}")
    return value, raw


def _absent(path: Path, reason: str) -> None:
    # lstat refuses symlinks and unreadable entries as well as ordinary files;
    # neither a corrupt active record nor an unreadable mount means absence.
    try:
        # Enumeration refreshes shared-directory knowledge before relying on
        # NFS's per-name negative cache, as the queue's withdrawal reader does.
        if any(entry.name == path.name for entry in path.parent.iterdir()):
            raise pool.PoolContractError(reason)
    except FileNotFoundError:
        pass
    try:
        path.lstat()
    except FileNotFoundError:
        return
    raise pool.PoolContractError(reason)


def _quiescent(q: pool.PoolQueue, key: str, record: dict | None = None) -> None:
    for state in (pool.READY, pool.CLAIMED, pool.DONE, pool.WITHDRAWN):
        _absent(q.item_path(state, key), f"conflicting {state} record for this action")
    _absent(q.lease_path(key), "action still has a lease")
    reservations = q.root / pool.RESERVATIONS
    _require(stat.S_ISDIR(reservations.lstat().st_mode), "reservation root is not a directory")
    for host in reservations.iterdir():
        _require(stat.S_ISDIR(host.lstat().st_mode), "reservation host is not a directory")
        _absent(host / "held" / key, "action still has a reservation")
    if record is not None:
        _require(q.withdrawal_covers(record) is None,
                 "durable withdrawal decision takes precedence over receipt recovery")
        # Claim intent normally survives finish. Only the intent of this
        # already-finished claimant is harmless; a newer one is a conflict.
        try:
            intent, _ = _read(q.item_path(pool.INTENT, key))
        except FileNotFoundError:
            return
        declared = intent.get("intent_unix")
        _require(intent.get("schema") == pool.POOL_CLAIM_INTENT_SCHEMA_V1
                 and intent.get("action_key") == key
                 and intent.get("owner") == record.get("claimed_by")
                 and intent.get("host") == record.get("claimed_host")
                 and _number(declared)
                 and record["published_unix"] <= declared <= record["claimed_unix"],
                 "conflicting claim intent for this action")


def _telemetry(value: object, *, key: str, nonce: str, unit: str, host: str) -> None:
    _require(isinstance(value, dict), "scope telemetry is missing")
    _require(value.get("complete") is True and value.get("errors") == [],
             "scope telemetry is incomplete")
    _require(value.get("action_key") == key and value.get("nonce") == nonce
             and value.get("scope_unit") == unit and value.get("host") == host,
             "scope telemetry belongs to another attempt")
    for field in ("oom_local", "oom_kill"):
        _require(type(value.get(field)) is int and value[field] == 0,
                 "scope has missing or nonzero OOM evidence")
    _require(value.get("termination_reason") in (None, "failed")
             and not value.get("termination_evidence"),
             "resource termination takes precedence over receipt recovery")


def _cleanup(record: dict, detail: dict) -> dict:
    key = record["action_key"]
    scope = record.get("resource_scope")
    cleanup = record.get("resource_scope_cleanup")
    _require(isinstance(scope, dict) and isinstance(cleanup, dict),
             "exact scope cleanup evidence is missing")
    nonce = scope.get("nonce")
    _require(isinstance(nonce, str) and re.fullmatch(r"[0-9a-f]{32}", nonce) is not None,
             "scope nonce is invalid")
    unit = "prismabuild-job" + hashlib.sha256((key + nonce).encode()).hexdigest()[:32] + ".slice"
    host = record.get("claimed_host")
    _require(isinstance(host, str) and bool(host) and record.get("finished_host") == host,
             "cleanup host differs from claiming host")
    _require(scope.get("action_key") == key and scope.get("scope_id") == unit
             and scope.get("cgroup_path") == "/sys/fs/cgroup/prismabuild.slice/" + unit,
             "scope identity differs from the failed attempt")
    _require(cleanup.get("complete") is True and cleanup.get("nonce") == nonce,
             "exact scope cleanup is incomplete")
    released = cleanup.get("released")
    _require(isinstance(released, dict) and released.get("ok") is True
             and released.get("scope_id") == unit and not released.get("error"),
             "broker did not confirm exact scope cleanup")
    _require(released.get("stop_reason") in (None, "failed")
             and not released.get("termination_evidence"),
             "broker stop reason takes precedence over receipt recovery")
    _telemetry(detail.get("resource_telemetry"), key=key, nonce=nonce, unit=unit, host=host)
    _telemetry(cleanup.get("telemetry"), key=key, nonce=nonce, unit=unit, host=host)
    claimed, checked, finished = (record.get("claimed_unix"), cleanup.get("checked_unix"),
                                  record.get("finished_unix"))
    _require(all(_number(v) for v in (claimed, checked, finished))
             and 0 < claimed <= checked <= finished,
             "scope cleanup is not dated within the failed attempt")
    return cleanup


def reconcile(q: pool.PoolQueue, key: str, *, cas: pb.PrismaBuildCAS,
              generation: str, attempt: int) -> dict:
    """Append verified action-result evidence, retaining the failed ending.

Only an explicit current generation and its final archived broker-EOF attempt
are eligible. An existing supplement is revalidated, never merely trusted.
Normal wait, retry and terminal-record semantics do not read this supplement.
"""
    _require(isinstance(key, str) and re.fullmatch(r"[0-9a-f]{64}", key) is not None,
             "reconciliation requires a full action key")
    _require(isinstance(generation, str)
             and re.fullmatch(r"[0-9a-f]{64}", generation) is not None,
             "reconciliation requires an exact pool generation digest")
    _require(type(attempt) is int and attempt > 0, "attempt must be a positive integer")
    # Publication, reset, claim and withdrawal use the same per-key exclusion.
    # No host admission lock is held while verifying CAS bytes.
    with q._transition_locked(key):
        _quiescent(q, key)
        terminal_path = q.item_path(pool.FAILED, key)
        record, terminal_raw = _read(terminal_path)
        _require(record.get("schema") == pool.POOL_OUTCOME_SCHEMA_V1
                 and record.get("action_key") == key and record.get("status") == "failed",
                 "selected ending is not a failed pool execution")
        _require(q.attempt_generation(record) == generation
                 and type(record.get("attempts")) is int and record["attempts"] == attempt,
                 "selected generation or final attempt differs from current ending")
        _require(not record.get("finish_pending") and not record.get("container_cleanup_pending"),
                 "attempt finish is still pending")
        _require(Path(str(record.get("cas_root"))).resolve() == Path(cas.root).resolve(),
                 "ending refers to a different CAS")
        outcomes = q.attempt_outcomes(record)
        _require(bool(outcomes) and outcomes[-1]["attempt"] == attempt,
                 "final immutable attempt is missing")
        archived = outcomes[-1]
        _require(archived.get("status") == "failed" and archived.get("disposition") == pool.FAILED,
                 "immutable attempt has a different terminal disposition")
        _require(isinstance(archived.get("detail"), dict), "immutable attempt detail is missing")
        detail = {**archived["detail"], "stdout": archived["stdout"], "stderr": archived["stderr"]}
        _require(record.get("detail") == detail,
                 "mutable ending differs from the immutable attempt")
        for field in ("claimed_by", "claimed_host", "claimed_unix", "finished_host", "finished_unix"):
            _require(record.get(field) == archived.get(field),
                     "mutable ending has a different attempt identity")
        _require(type(detail.get("returncode")) is int and detail["returncode"] == 125
                 and detail.get("status") == "failed" and detail.get("stderr") == BROKER_EOF,
                 "attempt is not the specific broker completion EOF failure")
        _require(not detail.get("termination_reason") and not detail.get("termination_evidence")
                 and not detail.get("action_survived_kill")
                 and (detail.get("action_returncode") is None
                      or (type(detail["action_returncode"]) is int and detail["action_returncode"] == 0))
                 and detail.get("action_signal") is None,
                 "action termination takes precedence over receipt recovery")
        elapsed, budget = detail.get("elapsed_s"), detail.get("execution_timeout_s")
        _require(_number(elapsed) and elapsed >= 0, "execution elapsed time is missing")
        _require("execution_timeout_s" in detail
                 and (budget is None or (_number(budget) and budget > elapsed)),
                 "execution deadline does not permit receipt recovery")
        cleanup = _cleanup(record, detail)
        _quiescent(q, key, record)
        request_path = Path(cas.root) / "requests" / key[:2] / f"{key}.json"
        action, action_raw = _read(request_path, readonly=True)
        action = pb.validate_action(action)
        _require(action["action_key"] == key and action_raw == pb._canonical_file_bytes(action),
                 "sealed action is not the canonical request for this key")
        _require(action["task"]["task_class"] == "generation",
                 "receipt recovery requires a generation action")
        receipt = cas.lookup(action)  # Full manifest, producer attestation and blob verification.
        _require(receipt is not None, "no verified canonical receipt for this action")
        attempt_path = q.attempt_path(record, attempt)
        _, attempt_raw = _read(attempt_path, readonly=True)
        supplement_path = attempt_path.with_name(f"{attempt:08d}.receipt-reconciliation.json")
        body = {
            "schema": SCHEMA, "action_key": key, "generation": generation, "attempt": attempt,
            "status": "payload_verified", "payload_status": "verified", "result_scope": "action",
            "transport_status": "failed", "returncode": 125,
            "terminal_path": str(terminal_path.relative_to(q.root)),
            "terminal_sha256": hashlib.sha256(terminal_raw).hexdigest(),
            "attempt_path": str(attempt_path.relative_to(q.root)),
            "attempt_sha256": hashlib.sha256(attempt_raw).hexdigest(),
            "logs": archived["logs"], "scope_cleanup": cleanup,
            "action_manifest_sha256": receipt["action_manifest_sha256"],
            "receipt_sha256": receipt["receipt_sha256"],
            "attestation_sha256": receipt["producer"]["attestation_sha256"],
            "result": receipt["result"],
            "note": "Verified action receipt and payload; original attempt remains failed. "
                    "The receipt does not establish this attempt's exit status.",
        }
        supplement = {**body, "reconciliation_sha256": pb.canonical_sha256(body)}
        # Retain a final readback against unsupported concurrent/manual writers
        # too; an earlier CAS read is no license to reconcile a changed ending.
        _quiescent(q, key, record)
        _require(_read(terminal_path)[1] == terminal_raw, "ending changed during reconciliation")
        pool._publish_immutable(supplement_path, pb._canonical_file_bytes(supplement),
                                where="pool receipt reconciliation")
        observed, raw = _read(supplement_path, readonly=True)
        _require(raw == pb._canonical_file_bytes(supplement), "reconciliation readback differs")
        return {**observed, "reconciliation_path": str(supplement_path)}
