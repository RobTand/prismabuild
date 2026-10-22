"""Bounded rollout verification and online attempt-log retirement (#2023)."""
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
import math
import heapq
import errno
import os
from pathlib import Path
import stat
import time

from . import attempt_archive as archive, digest_primitives as digests, immutable_files as native

READER_GATE_SCHEMA = "prismabuild.attempt-log-reader-gate.v1"
READER_PROOF_SCHEMA = "prismabuild.attempt-log-reader-proof.v1"
MAX_READER_GATE_BYTES = 256 * 1024
MAX_RETENTION_KEYS = 256
MAX_RETENTION_LOGS = 512
MAX_RETENTION_METADATA_BYTES = 16 * 1024 * 1024
MAX_REFERENCE_ENTRIES = 4096
MAX_READER_MEMBERS = 256
READER_ROLES = frozenset({"active_generation", "rollback_generation", "detached_client", "mcp",
                          "tl_computation_runner", "tl_test_runner", "deferred_runtime"})


class AttemptRetentionRefused(ValueError):
    """The window, rollout gate or retirement evidence is not sufficient."""


def _retention_finite_number(value: object) -> bool:
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def _verify_attempt_reader_gate(queue_root: Path, config: Mapping, *, now: float) -> dict:
    name = config.get("reader_gate")
    if (not isinstance(name, str) or not name or name != str(Path(name))
            or Path(name).is_absolute() or ".." in Path(name).parts):
        raise AttemptRetentionRefused("missing/invalid attempt log reader gate address")
    try:
        _attempt_reader_gate_custody(queue_root / name)
        gate = archive._attempt_archive_json(queue_root / name, limit=MAX_READER_GATE_BYTES)
        fields = {"schema", "queue_id", "store_id", "queue_root", "config_sha256", "issued_unix",
                  "expires_unix", "inventory_complete", "inventory_attested_by", "approvals", "members",
                  "generations", "deferred_inventory_complete"}
        issued, expires = gate.get("issued_unix"), gate.get("expires_unix")
        if (set(gate) != fields or gate.get("schema") != READER_GATE_SCHEMA
                or gate.get("queue_id") != config["queue_id"] or gate.get("store_id") != config["store_id"]
                or gate.get("queue_root") != str(queue_root)
                or gate.get("config_sha256") != digests.canonical_sha256(dict(config))
                or not _retention_finite_number(issued) or not _retention_finite_number(expires)
                or not issued <= now < expires or not 0 < expires - issued <= 86400
                or gate.get("inventory_complete") is not True
                or not isinstance(gate.get("inventory_attested_by"), str)
                or not gate["inventory_attested_by"].strip()):
            raise AttemptRetentionRefused("reader gate binding, completeness or validity interval refused")
        if gate["deferred_inventory_complete"] is not True:
            raise AttemptRetentionRefused("deferred reader inventory completeness is not attested")
        generations = _verify_attempt_generation_receipts(gate["generations"])
        approvals = gate["approvals"]
        if not isinstance(approvals, dict) or set(approvals) != {"pb_lead", "tl_lead"}:
            raise AttemptRetentionRefused("reader gate requires PB lead approval and TL lead go")
        inventory_sha = digests.canonical_sha256({field: gate[field] for field in (
            "members", "generations", "inventory_complete", "deferred_inventory_complete", "inventory_attested_by")})
        for approval in approvals.values():
            if (not isinstance(approval, dict)
                    or set(approval) != {"by", "reason", "unix", "config_sha256", "inventory_sha256"}
                    or approval["config_sha256"] != gate["config_sha256"]
                    or approval["inventory_sha256"] != inventory_sha
                    or any(not isinstance(approval.get(field), str) or not approval[field].strip()
                           for field in ("by", "reason"))
                    or not _retention_finite_number(approval.get("unix"))
                    or not issued <= approval["unix"] <= now):
                raise AttemptRetentionRefused("reader gate lead approval is incomplete or stale")
        members = gate["members"]
        if not isinstance(members, list) or not 6 <= len(members) <= MAX_READER_MEMBERS:
            raise AttemptRetentionRefused("reader gate inventory exceeds bounds or omits roles")
        roles, identities = set(), set()
        for member in members:
            if (not isinstance(member, dict) or set(member) != {"id", "role", "generation", "evidence"}
                    or any(not isinstance(member.get(field), str) or not member[field].strip()
                           for field in ("id", "role", "generation"))
                    or member["role"] not in READER_ROLES or member["id"] in identities):
                raise AttemptRetentionRefused("reader inventory member is invalid or duplicated")
            identities.add(member["id"])
            roles.add(member["role"])
            evidence = member["evidence"]
            if (not isinstance(evidence, dict) or set(evidence) != {"path", "bytes", "sha256"}
                    or not isinstance(evidence["path"], str)
                    or evidence["path"] != str(Path(evidence["path"]))
                    or Path(evidence["path"]).is_absolute() or ".." in Path(evidence["path"]).parts
                    or type(evidence["bytes"]) is not int or not 0 < evidence["bytes"] <= 16384
                    or not isinstance(evidence["sha256"], str)
                    or not archive._ARCHIVE_HEX.fullmatch(evidence["sha256"])):
                raise AttemptRetentionRefused("reader inventory evidence address is invalid")
            if not (queue_root / evidence["path"]).is_relative_to((queue_root / name).parent):
                raise AttemptRetentionRefused("reader proof must share operator gate custody")
            _attempt_reader_gate_custody(queue_root / evidence["path"])
            raw = native.read_immutable_file(queue_root / evidence["path"], where="attempt reader proof",
                                             max_bytes=16384)
            if len(raw) != evidence["bytes"] or digests.raw_sha256(raw) != evidence["sha256"]:
                raise AttemptRetentionRefused("reader inventory evidence differs from its address")
            proof = digests.decode_strict_json(raw, where="attempt reader proof")
            if (not isinstance(proof, dict)
                    or set(proof) != {"schema", "id", "role", "generation", "capabilities", "verified_unix"}
                    or proof.get("schema") != READER_PROOF_SCHEMA
                    or any(proof.get(field) != member[field] for field in ("id", "role", "generation"))
                    or not isinstance(proof.get("capabilities"), list)
                    or archive.ATTEMPT_LOG_RESOLVER_CAPABILITY not in proof["capabilities"]
                    or not _retention_finite_number(proof.get("verified_unix"))
                    or not issued <= proof["verified_unix"] <= now):
                raise AttemptRetentionRefused("reader inventory lacks exact-generation resolver proof")
        if roles - {"deferred_runtime"} != READER_ROLES - {"deferred_runtime"}:
            raise AttemptRetentionRefused("reader inventory omits an active/rollback/client/MCP/TL role")
        for member in members:
            if member["role"] not in {"tl_computation_runner", "tl_test_runner"}:
                if member["generation"] not in generations:
                    raise AttemptRetentionRefused("PB reader names an unqualified exact generation")
        _verify_current_attempt_readers(queue_root, members, generations, now=now)
        if not issued <= time.time() < expires:
            raise AttemptRetentionRefused("reader gate expired during verification")
        return {"verified": True, "members": len(members), "expires_unix": expires,
                "inventory_attested_by": gate["inventory_attested_by"]}
    except AttemptRetentionRefused:
        raise
    except (OSError, ValueError, native.CASTamperError, native.CASUnavailableError,
            archive.AttemptLogIntegrityError, KeyError, TypeError) as exc:
        raise AttemptRetentionRefused(f"attempt reader gate cannot be verified: {exc}") from exc


def _attempt_retention_settings(queue_root: Path, *, now: float, applying: bool = False) -> tuple[dict | None, float, dict]:
    config = archive._check_attempt_archive_store(queue_root, for_apply=applying)
    days = archive.QUEUE_EVIDENCE_RETENTION_DAYS if config is None else config.get(
        "retention_days", archive.QUEUE_EVIDENCE_RETENTION_DAYS)
    if not _retention_finite_number(days) or days < 0.5:
        raise AttemptRetentionRefused("attempt log retention has a hard minimum of 12 hours")
    gate = {"verified": False, "reason": "archive is not configured"}
    if config is not None:
        try:
            gate = _verify_attempt_reader_gate(queue_root, config, now=now)
        except AttemptRetentionRefused as exc:
            gate = {"verified": False, "reason": str(exc)}
    if (applying or days < archive.QUEUE_EVIDENCE_RETENTION_DAYS) and not gate["verified"]:
        raise AttemptRetentionRefused(str(gate["reason"]))
    return config, float(days), gate


def _retention_directory_entries(directory: Path, *, limit: int = MAX_REFERENCE_ENTRIES) -> list[Path]:
    """A bounded no-follow listing; unknown/oversized registries retain."""
    try:
        fd = native.open_directory_nofollow(directory, where="attempt retention reference directory")
    except FileNotFoundError:
        if native.confirmed_metadata_absence(directory):
            return []
        fd = native.open_directory_nofollow(directory, where="attempt retention reference directory")
    try:
        result = []
        with os.scandir(fd) as entries:
            for entry in entries:
                if len(result) >= limit:
                    raise AttemptRetentionRefused("attempt reference census exceeds bound")
                if entry.is_symlink():
                    raise AttemptRetentionRefused("attempt reference census contains a symlink")
                result.append(directory / entry.name)
        native.assert_directory_identity(fd, directory, where="attempt reference directory")
        return result
    finally:
        os.close(fd)


def _attempt_retention_snapshot(queue, key: str, *, cutoff: float) -> tuple[dict | None, str]:
    """Policy implementation for PoolQueue.attempt_log_retention_eligibility.

    The PB lead allows resolver-preserved references for logs only. Active
    ownership still retains. No recursive queue census is performed.
    """
    from . import record_store as records, pool
    try:
        if not archive._ARCHIVE_HEX.fullmatch(key):
            return None, "invalid action key"
        for state in (records.READY, records.CLAIMED):
            if not native.confirmed_metadata_absence(queue.records.item_path(state, key)):
                return None, "ready or claimed"
        if not native.confirmed_metadata_absence(queue.records.lease_path(key)):
            return None, "lease present"
        for state in (records.READY, records.CLAIMED):
            for path in _retention_directory_entries(queue.root / state):
                if queue.records.maintenance_live_key(path.name) == key:
                    return None, "ready/claim sidecar, tombstone or late finisher"
        candidates = []
        for state in (records.DONE, records.FAILED, records.WITHDRAWN):
            path = queue.records.item_path(state, key)
            if native.confirmed_metadata_absence(path):
                continue
            raw = native.read_stable_file(path, where="attempt retention terminal", max_bytes=1024 * 1024)
            value = digests.decode_strict_json(raw, where="attempt retention terminal")
            if (not isinstance(value, dict) or value.get("action_key") != key
                    or value.get("schema") != pool.POOL_OUTCOME_SCHEMA_V1
                    or not isinstance(value.get("status"), str) or not value["status"]
                    or (state == records.DONE and value["status"] not in {"executed", "cache_hit"})
                    or (state == records.FAILED and value["status"] in {"executed", "cache_hit", "withdrawn"})
                    or (state == records.WITHDRAWN and value["status"] != "withdrawn")
                    or value.get("finish_pending") is not None):
                return None, "invalid terminal identity"
            stamp = value.get("finished_unix")
            if stamp is None and state == records.WITHDRAWN:
                stamp = value.get("withdrawn_unix")
            if (not _retention_finite_number(stamp) or stamp >= cutoff or path.lstat().st_mtime >= cutoff
                    or not _retention_finite_number(value.get("published_unix"))):
                return None, "terminal timestamp or mtime inside retention window"
            candidates.append((path, {**value, "finished_unix": stamp}, digests.raw_sha256(raw)))
        if len(candidates) != 1:
            return None, "missing or ambiguous terminal generation"
        path, record, terminal_sha = candidates[0]
        active = _attempt_retention_active_owner_refusal(queue, key, record=record, cutoff=cutoff)
        if active:
            return None, active
        generation = queue.records.attempt_generation(record)
        logs, identities = [], {}
        attempt_count, metadata_bytes = 0, 0
        oldest = record["finished_unix"]
        generations = _retention_directory_entries(queue.root / "attempts" / key, limit=256)
        for directory in generations:
            if not archive._ARCHIVE_HEX.fullmatch(directory.name):
                return None, "unknown attempt generation address"
            attempts = []
            for attempt_path in _retention_directory_entries(directory, limit=MAX_RETENTION_LOGS * 3):
                if not (attempt_path.name.endswith(".json") and attempt_path.stem.isascii()
                        and attempt_path.stem.isdigit()):
                    continue  # Payloads and reconciliation sidecars remain at their addresses.
                attempt_count += 1
                metadata_bytes += attempt_path.lstat().st_size
                if attempt_count > MAX_RETENTION_LOGS // 2 or metadata_bytes > MAX_RETENTION_METADATA_BYTES:
                    return None, "attempt metadata exceeds per-key aggregate bound"
                attempt = archive._attempt_archive_json(attempt_path, limit=1024 * 1024)
                published = attempt.get("published_unix")
                if not _retention_finite_number(published) or published > record["published_unix"]:
                    return None, "unknown or newer archived generation"
                number = queue.records.archived_attempt_identity(attempt_path, attempt, action_key=key,
                                                                 generation=published)
                if directory.name != queue.records.attempt_generation(attempt):
                    return None, "attempt generation differs from its directory"
                if (not _retention_finite_number(attempt.get("finished_unix"))
                        or attempt["finished_unix"] >= cutoff or attempt_path.lstat().st_mtime >= cutoff):
                    return None, "attempt timestamp or mtime inside retention window"
                oldest = min(oldest, attempt["finished_unix"])
                attempts.append(attempt)
                if not isinstance(attempt.get("logs"), Mapping):
                    return None, "attempt descriptors missing"
                for stream in ("stdout", "stderr"):
                    fact = archive._validate_attempt_log_descriptor(attempt["logs"].get(stream))
                    canonical = queue.records.attempt_log_path(attempt, number, stream, fact["sha256"])
                    if canonical != queue.root / fact["path"]:
                        return None, "attempt descriptor not bound to generation"
                    if not native.confirmed_metadata_absence(canonical):
                        info = canonical.lstat()
                        if (not stat.S_ISREG(info.st_mode) or info.st_mode & 0o222
                                or info.st_mtime >= cutoff or info.st_size != fact["bytes"]):
                            return None, "log mutable or inside retention window"
                        logs.append(fact)
                        identities[fact["path"]] = _attempt_retention_source_identity(info)
                    if len(logs) > MAX_RETENTION_LOGS:
                        return None, "attempt log count exceeds bound"
            if attempts:
                last = max(attempts, key=lambda value: value["attempt"])
                if last.get("disposition") not in {records.DONE, records.FAILED, records.WITHDRAWN}:
                    return None, "archived generation has no immutable ending"
                if directory.name == generation and path.parent.name != records.WITHDRAWN:
                    queue._bind_linked_attempt(record, last, attempt=last["attempt"],
                                               where="attempt log retirement ending")
                    derived = {f"{stream}{suffix}" for stream in ("stdout", "stderr")
                               for suffix in ("", "_bytes", "_truncated", "_log")}
                    terminal_detail, attempt_detail = record.get("detail"), last.get("detail")
                    if (record.get("attempts") != last["attempt"]
                            or any(last.get(field) != record.get(field)
                                   for field in ("status", "finished_unix", "finished_host"))
                            or not isinstance(terminal_detail, dict) or not isinstance(attempt_detail, dict)
                            or {k: v for k, v in terminal_detail.items() if k not in derived}
                               != {k: v for k, v in attempt_detail.items() if k not in derived}
                            or any(terminal_detail.get(f"{stream}_log") != last["logs"][stream]
                                   for stream in ("stdout", "stderr"))):
                        return None, "terminal summary differs from immutable ending"
        if not logs:
            return None, "no live immutable attempt logs"
        return dict(key=key, generation=generation, terminal_path=str(path), terminal_sha256=terminal_sha,
                    logs=logs, source_identities=identities, oldest_finished_unix=oldest,
                    bytes=sum(fact["bytes"] for fact in logs)), ""

    except (OSError, ValueError, native.CASTamperError, native.CASUnavailableError,
            archive.AttemptLogIntegrityError) as exc:
        return None, f"eligibility evidence unavailable: {type(exc).__name__}"


def survey_attempt_log_retention(queue, *, now: float | None = None, max_keys: int = MAX_RETENTION_KEYS,
                                 after_key: str | None = None) -> dict:
    now = time.time() if now is None else now
    if type(max_keys) is not int or not 0 < max_keys <= MAX_RETENTION_KEYS:
        raise AttemptRetentionRefused("attempt retirement key bound must be between 1 and 256")
    config, days, gate = _attempt_retention_settings(queue.root, now=now)
    stored_cursor = _read_attempt_retention_cursor(config)
    cursor = stored_cursor if after_key is None else after_key
    if cursor and (not isinstance(cursor, str) or not archive._ARCHIVE_HEX.fullmatch(cursor)):
        raise AttemptRetentionRefused("attempt survey cursor must be an exact key")
    # Stream all flat terminal names but retain at most one bounded page in a
    # max-heap. History remains live, so stopping at the first N on every run
    # would starve later keys. A second pass wraps at the end of the namespace.
    keys = _select_attempt_retention_keys(queue.root, cursor=cursor, limit=max_keys)
    wrapped = not keys and bool(cursor)
    if wrapped:
        keys = _select_attempt_retention_keys(queue.root, cursor="", limit=max_keys)
    candidates, reasons = [], Counter()
    retained_known_bytes, retained_unknown_keys = 0, 0
    for key in keys:
        snapshot, reason = queue.attempt_log_retention_eligibility(key, cutoff=now - days * 86400)
        if snapshot is not None:
            candidates.append(snapshot)
        else:
            reasons[reason] += 1
            retained_size = _count_attempt_retention_bytes(queue.root, key)
            if retained_size is None:
                retained_unknown_keys += 1
            else:
                retained_known_bytes += retained_size
    return dict(schema="prismabuild.attempt-log-survey.v1", queue_root=str(queue.root), unix=now,
                retention_days=days, cutoff=now - days * 86400, keys_scanned=len(keys),
                source_cursor=stored_cursor, after_key=cursor, next_cursor=keys[-1] if keys else "",
                wrapped=wrapped, truncated=len(keys) == max_keys,
                config_sha256=digests.canonical_sha256(config),
                candidates=candidates, eligible_bytes=sum(row["bytes"] for row in candidates),
                retained_keys=dict(reasons), retained_known_bytes=retained_known_bytes,
                retained_bytes=retained_known_bytes if not retained_unknown_keys else None,
                retained_unknown_keys=retained_unknown_keys,
                oldest_eligible_finished_unix=min((row["oldest_finished_unix"] for row in candidates), default=None),
                archive_health="verified" if config else "unconfigured",
                reader_gate=gate)


def apply_attempt_log_retention(queue, plan: Mapping) -> dict:
    """Publish/read back outside locks, then recheck under the key lock per unlink."""
    if plan.get("queue_root") != str(queue.root):
        raise AttemptRetentionRefused("attempt retirement plan belongs to another queue")
    _config, days, _gate = _attempt_retention_settings(queue.root, now=time.time(), applying=True)
    if digests.canonical_sha256(_config) != plan.get("config_sha256") or days != plan.get("retention_days"):
        raise AttemptRetentionRefused("attempt log retention configuration changed after survey")
    removed, skipped, aborted = [], [], None
    for candidate in plan["candidates"]:
        key = candidate["key"]
        for fact in candidate["logs"]:
            try:
                archive._publish_attempt_log_archive(queue.root, fact)
                with queue.action_transition_lock(key, blocking=False) as acquired:
                    if not acquired:
                        skipped.append({"path": fact["path"], "reason": "transition lock busy"})
                        continue
                    _recheck_attempt_retirement_authority(queue.root, plan)
                    current, reason = queue.attempt_log_retention_eligibility(key, cutoff=plan["cutoff"])
                    if (current is None or reason or any(current[field] != candidate[field]
                                               for field in ("generation", "terminal_sha256", "terminal_path"))
                            or fact not in current["logs"]):
                        skipped.append({"path": fact["path"], "reason": reason or "generation/evidence changed"})
                        continue
                    config = archive._check_attempt_archive_store(queue.root, for_apply=True)
                    archive._stream_archived_attempt_log(config, fact, lambda raw: None)
                    path = queue.root / fact["path"]
                    # Native custody verifies the full source immediately before
                    # descriptor-relative unlink; the key lock excludes its owner.
                    native.file_identity_nofollow(path, where="retiring attempt log",
                        expected_sha256=fact["sha256"], expected_bytes=fact["bytes"])
                    _recheck_attempt_retirement_authority(queue.root, plan)
                    if _attempt_retention_source_identity(path.lstat()) != candidate["source_identities"][fact["path"]]:
                        raise AttemptRetentionRefused("attempt log source identity changed after survey")
                    # Re-observe active ownership after slow archive/source reads.
                    current, reason = queue.attempt_log_retention_eligibility(key, cutoff=plan["cutoff"])
                    if (current is None or reason or current["terminal_sha256"] != candidate["terminal_sha256"]):
                        raise AttemptRetentionRefused(reason or "terminal changed before unlink")
                    _recheck_attempt_retirement_authority(queue.root, plan)
                    native.unlink_nofollow(path, where="retiring attempt log")
                    removed.append({"path": fact["path"], "bytes": fact["bytes"]})
            except (native.CASTamperError, native.CASUnavailableError,
                    archive.AttemptLogIntegrityError, archive.AttemptArchiveUnavailable) as exc:
                if isinstance(exc, OSError) and exc.errno == errno.EDQUOT:
                    raise
                aborted = f"{type(exc).__name__}: {exc}"
                skipped.append({"path": fact["path"], "reason": aborted})
                break
            except (OSError, ValueError) as exc:
                if isinstance(exc, OSError) and exc.errno == errno.EDQUOT:
                    raise
                skipped.append({"path": fact["path"], "reason": f"{type(exc).__name__}: {exc}"})
        if aborted is not None:
            break
    receipt = dict(schema="prismabuild.attempt-log-retirement-receipt.v1", queue_root=str(queue.root),
                   unix=time.time(), retention_days=days, removed=removed, skipped=skipped,
                   aborted=aborted)
    raw = digests._canonical_bytes(receipt)
    destination = queue.root / "gc-receipts" / f"attempt-logs-{digests.raw_sha256(raw)}.json"
    native.publish_immutable(destination, raw)
    if aborted is not None:
        raise AttemptRetentionRefused(f"archive retirement stopped: {aborted}; receipt={destination.name}")
    return receipt


def _verify_attempt_generation_receipts(rows: object) -> dict:
    from .generation_receipt import GenerationReceipt
    if not isinstance(rows, list) or not 1 <= len(rows) <= 128:
        raise AttemptRetentionRefused("qualified generation inventory exceeds bounds")
    result = {}
    for row in rows:
        if (not isinstance(row, dict)
                or set(row) != {"generation", "directory", "commit", "receipt_sha256", "content_sha256"}
                or any(not isinstance(value, str) or not 0 < len(value) <= 1024 for value in row.values())
                or row["generation"] in result):
            raise AttemptRetentionRefused("qualified generation identity is invalid or duplicated")
        directory = Path(row["directory"])
        if (not directory.is_absolute() or str(directory) != row["directory"]
                or ".." in directory.parts or directory.name != row["generation"]):
            raise AttemptRetentionRefused("qualified generation address is not canonical")
        raw = native.read_stable_file(directory / "RUNTIME_VERSION.json", where="attempt reader generation receipt",
                                      max_bytes=1024 * 1024)
        if digests.raw_sha256(raw) != row["receipt_sha256"]:
            raise AttemptRetentionRefused("reader generation receipt changed")
        receipt = GenerationReceipt.capture(raw)
        value = receipt.require_generation(row["generation"])
        if (receipt.require_commit() != row["commit"]
                or digests.canonical_sha256(value["files"]) != row["content_sha256"]
                or len(value["files"]) > 4096):
            raise AttemptRetentionRefused("reader generation commit/content differs")
        relative = "src/prismabuild/attempt_archive.py"
        source = native.read_stable_file(directory / relative, where="receipt-covered attempt resolver source",
                                         max_bytes=1024 * 1024)
        if receipt.source_declarations(relative, source).protocol("ATTEMPT_LOG_RESOLVER_PROTOCOL") != 1:
            raise AttemptRetentionRefused("reader generation has no qualified resolver protocol")
        result[row["generation"]] = row
    return result


def _verify_current_attempt_readers(queue_root: Path, members: list, generations: Mapping, *, now: float) -> None:
    """Fresh exact loaded-generation offers, including draining, plus pinned runtimes.

    Process/MCP/detached/TL completeness still requires operator custody and
    qualification attestations. Offers establish worker identity, not every
    long-lived process sharing its host.
    """
    from . import action_edges, pool
    expected_workers = {row["id"][7:]: row["generation"] for row in members
                        if row["role"] == "active_generation" and row["id"].startswith("worker:")}
    observed, offer_stamps = {}, []
    for path in _retention_directory_entries(queue_root / "workers", limit=MAX_READER_MEMBERS):
        if not path.name.endswith(".json"):
            raise AttemptRetentionRefused("unknown worker inventory member")
        offer = archive._attempt_archive_json(path, limit=65536, immutable=False)
        stamp = offer.get("announced_unix")
        tags = offer.get("tags")
        if (offer.get("schema") != pool.POOL_OFFER_SCHEMA_V1
                or offer.get("host") != path.stem
                or not _retention_finite_number(stamp) or not 0 <= now - stamp <= min(300, pool.OFFER_TIMEOUT_S)
                or not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags)):
            raise AttemptRetentionRefused("worker inventory is stale, unknown or malformed")
        loaded = [tag.removeprefix("runtime-generation:") for tag in tags if tag.startswith("runtime-generation:")]
        if len(loaded) != 1 or loaded[0] not in generations:
            raise AttemptRetentionRefused("worker loaded generation is missing, mixed or unqualified")
        if offer.get("runtime_commit") != generations[loaded[0]]["commit"]:
            raise AttemptRetentionRefused("worker runtime commit differs from exact qualified generation")
        observed[path.stem] = loaded[0]
        offer_stamps.append(stamp)
    if not observed or observed != expected_workers:
        raise AttemptRetentionRefused("active worker generation inventory changed after approval")
    expected_deferred = {row["id"][9:]: row["generation"] for row in members
                         if row["role"] == "deferred_runtime" and row["id"].startswith("deferred:")}
    observed_deferred = {}
    for path in _retention_directory_entries(queue_root / action_edges.DEFERRED_SUBDIR, limit=MAX_READER_MEMBERS):
        pending = path.stem
        if not archive._ARCHIVE_HEX.fullmatch(pending):
            raise AttemptRetentionRefused("unknown deferred inventory member")
        if not native.confirmed_metadata_absence(action_edges.published_path(queue_root, pending)):
            continue
        record = archive._attempt_archive_json(path, limit=1024 * 1024)
        action_edges.validate_deferred(record, pending_id=pending)
        release_path = action_edges.release_path(queue_root, pending)
        if native.confirmed_metadata_absence(release_path):
            directory = Path(action_edges.template_wrapper(record["template"])).parent
            generation = action_edges.generation_name(directory)
        else:
            pinned = archive._attempt_archive_json(release_path, limit=1024 * 1024)
            runtime = pinned.get("runtime")
            if not isinstance(runtime, dict) or set(runtime) != {"root", "generation"}:
                raise AttemptRetentionRefused("pinned deferred runtime is malformed")
            generation, directory = runtime["generation"], Path(runtime["root"])
        if generation not in generations or str(directory) != generations[generation]["directory"]:
            raise AttemptRetentionRefused("deferred runtime is missing or unqualified")
        observed_deferred[pending] = generation
    if observed_deferred != expected_deferred:
        raise AttemptRetentionRefused("deferred reader inventory changed after approval")
    completed = time.time()
    if any(not 0 <= completed - stamp <= min(300, pool.OFFER_TIMEOUT_S) for stamp in offer_stamps):
        raise AttemptRetentionRefused("worker offers expired during inventory verification")


def _attempt_retention_source_identity(info: os.stat_result) -> list[int]:
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_mode]


def _attempt_retention_active_owner_refusal(queue, key: str, *, record: Mapping, cutoff: float) -> str:
    from . import pool, reader_lease
    intent_path = queue.root / "intent" / f"{key}.json"
    if not native.confirmed_metadata_absence(intent_path):
        intent = archive._attempt_archive_json(intent_path, limit=65536, immutable=False)
        declared = intent.get("intent_unix")
        if (intent.get("schema") != pool.POOL_CLAIM_INTENT_SCHEMA_V1 or intent.get("action_key") != key
                or not _retention_finite_number(declared) or declared > record["finished_unix"]
                or intent_path.lstat().st_mtime >= cutoff
                or any(not isinstance(intent.get(field), str) or not intent[field] for field in ("owner", "host"))
                or type(intent.get("pid")) is not int or intent["pid"] <= 0):
            return "new or unknown claim intent ownership"
        # Successful claims deliberately retain intent. This old valid marker
        # predates the verified terminal and is preserved as historical JSON.
    owner = record.get("container_owner")
    if owner is not None:
        if not isinstance(owner, str) or not archive._ARCHIVE_HEX.fullmatch(owner):
            return "unknown container ownership identity"
        if not native.confirmed_metadata_absence(queue.container_marker(owner)):
            return "container ownership present"
    for path in _retention_directory_entries(queue.root / "residency" / "leases" / key):
        if path.name.endswith((".retiring.json", ".tmp")):
            continue
        if not path.name.endswith(".lease.json"):
            return "unknown reader ownership evidence"
        pin = reader_lease.validate_pin(archive._attempt_archive_json(path, limit=1024 * 1024, immutable=False))
        if pin.get("owner_action_key") != key or pin["refs"]:
            return "active or unknown reader ownership present"
    # These are shallow ownership namespaces, not recursive reference discovery.
    for root_name in ("reservations", "tier-reservations"):
        for ledger in _retention_directory_entries(queue.root / root_name, limit=256):
            for holder in _retention_directory_entries(ledger / "held"):
                parts = holder.name.split(".")
                if holder.name == key or (len(parts) > 2 and parts[0] == "claiming" and parts[2] == key):
                    if _retention_directory_entries(holder):
                        return "active resource ownership present"
    return ""


def _select_attempt_retention_keys(queue_root: Path, *, cursor: str, limit: int) -> list[str]:
    from . import record_store as records
    heap, selected = [], set()
    for state in (records.DONE, records.FAILED, records.WITHDRAWN):
        try:
            fd = native.open_directory_nofollow(queue_root / state, where="attempt retirement terminal names")
        except FileNotFoundError:
            continue
        try:
            with os.scandir(fd) as entries:
                for entry in entries:
                    key = entry.name[:-5]
                    if (not entry.name.endswith(".json") or not archive._ARCHIVE_HEX.fullmatch(key)
                            or key <= cursor or key in selected):
                        continue
                    number = int(key, 16)
                    if len(heap) < limit:
                        heapq.heappush(heap, -number)
                        selected.add(key)
                    elif number < -heap[0]:
                        discarded = -heapq.heapreplace(heap, -number)
                        selected.remove(f"{discarded:064x}")
                        selected.add(key)
            native.assert_directory_identity(fd, queue_root / state, where="attempt terminal names")
        finally:
            os.close(fd)
    return sorted(selected)


def _read_attempt_retention_cursor(config: Mapping | None) -> str:
    if config is None:
        return ""
    path = Path(config["archive_root"]) / "survey-cursor.json"
    try:
        value = archive._attempt_archive_json(path, limit=4096, immutable=False)
    except FileNotFoundError:
        if native.confirmed_metadata_absence(path):
            return ""
        value = archive._attempt_archive_json(path, limit=4096, immutable=False)
    if (set(value) != {"schema", "queue_id", "after_key"}
            or value["schema"] != "prismabuild.attempt-log-survey-cursor.v1"
            or value["queue_id"] != config["queue_id"]
            or not isinstance(value["after_key"], str)
            or (value["after_key"] and not archive._ARCHIVE_HEX.fullmatch(value["after_key"]))):
        raise AttemptRetentionRefused("attempt survey cursor is invalid")
    return value["after_key"]


def checkpoint_attempt_log_survey(queue_root: Path, plan: Mapping) -> bool:
    """Advance archive-owned bookkeeping only, never queue evidence or log data.

    A stale concurrent survey cannot overwrite a newer cursor. With no archive
    configured, pass plan.next_cursor as --attempt-log-after on the next survey.
    """
    from . import posix_lock
    config = archive._check_attempt_archive_store(queue_root)
    if config is None:
        return False
    if digests.canonical_sha256(config) != plan["config_sha256"]:
        raise AttemptRetentionRefused("attempt survey configuration changed")
    root = Path(config["archive_root"])
    with posix_lock.held(root / "survey-cursor.lock", blocking=False) as acquired:
        if not acquired or _read_attempt_retention_cursor(config) != plan["source_cursor"]:
            return False
        native.write_bytes_atomic(root / "survey-cursor.json", digests._canonical_bytes(
            dict(schema="prismabuild.attempt-log-survey-cursor.v1", queue_id=config["queue_id"],
                 after_key=plan["next_cursor"])))
    return True


def _recheck_attempt_retirement_authority(queue_root: Path, plan: Mapping) -> None:
    config, days, gate = _attempt_retention_settings(queue_root, now=time.time(), applying=True)
    if (digests.canonical_sha256(config) != plan.get("config_sha256")
            or days != plan.get("retention_days") or not gate["verified"]):
        raise AttemptRetentionRefused("attempt retirement config/store/window changed after survey")


def _count_attempt_retention_bytes(queue_root: Path, key: str) -> int | None:
    total = 0
    try:
        for generation in _retention_directory_entries(queue_root / "attempts" / key, limit=256):
            for path in _retention_directory_entries(generation, limit=MAX_RETENTION_LOGS * 3):
                if path.name.endswith(".log"):
                    info = path.lstat()
                    if not stat.S_ISREG(info.st_mode):
                        return None
                    total += info.st_size
        return total
    except (OSError, ValueError, native.CASTamperError, native.CASUnavailableError):
        return None


def _attempt_reader_gate_custody(path: Path) -> None:
    fd = native.open_directory_nofollow(path.parent, where="operator reader-gate directory")
    try:
        directory, leaf = os.fstat(fd), os.stat(path.name, dir_fd=fd, follow_symlinks=False)
        if (directory.st_uid not in (0, os.getuid()) or directory.st_mode & 0o022
                or leaf.st_uid not in (0, os.getuid()) or leaf.st_mode & 0o222
                or not stat.S_ISREG(leaf.st_mode)):
            raise AttemptRetentionRefused("reader gate/proof lacks operator-owned immutable custody")
    finally:
        os.close(fd)
