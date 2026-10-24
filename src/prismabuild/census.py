"""Read-only fleet census facts, with explicit plain and retained inputs."""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
import heapq
import math
import os
from pathlib import Path
import re
import shlex
import time
from typing import Generic, Literal, TypeVar

from . import (core as pb, pool, produced_output, output_contract, residency_map, residency_plan,
               storage_tiers, tier_holdings, window_credit, slurm_lane as sl)
from . import kept_reads
read_cache = kept_reads

SHARED_ROOT = Path("/mnt/shared/prismabuild-fleet")
DEFAULT_QUEUE_ROOT = SHARED_ROOT / "pb-queue"
DEFAULT_RECENT = 20
DEFAULT_MOVER_RECEIPTS = 100
MAX_ENDING_RECORD_BYTES = 8 * 1024 * 1024
UNKNOWN = "?"
OFFER_RETIRED_AFTER_S = 86400.0
_OBSERVATION_IDENTITY = (("action_key", "action_key"), ("owner", "claimed_by"),
                         ("host", "claimed_host"), ("claimed_unix", "claimed_unix"),
                         ("published_unix", "published_unix"))
_DRAIN_OWNER_DISPLAY = 200
_DRAIN_REASON_DISPLAY = 1000
STARVATION_SCHEMA_V1 = "prismabuild.pbstatus.starvation.v1"
NOT_OBSERVABLE = "not_observable"
STARVATION_QUIET_FRACTION = 0.5
STARVATION_CHILD_SILENT_S = 30.0
BLOCKED_ORIGINS_SCHEMA_V1 = "prismabuild.pbstatus.blocked_origins.v1"
ORPHANED_PREWRITE_REMEDY = output_contract.ORPHANED_PREWRITE_REMEDY
_UNKNOWN_TIER = "unknown"
_UNREADABLE = object()
_NOT_A_RECEIPT = object()
_NOT_CAPTURED = object()

T = TypeVar("T")


@dataclass(frozen=True)
class Section(Generic[T]):
    value: T | None
    completeness: Literal["complete", "partial", "unavailable"]
    inputs: Mapping[str, object]
    bound: Mapping[str, object]
    errors: tuple[str, ...] = ()


def _section(value, *, inputs, bound=None, errors=(), complete=True, available=True):
    return Section(value, "unavailable" if value is None or not available else
                   "complete" if complete and not errors else "partial",
                   inputs, {} if bound is None else bound, tuple(errors))


def tiers(queue_root, *, view="announcements", now=None, reader=None) -> Section[dict]:
    queue = pool.PoolQueue(queue_root)
    inputs = {"queue_root": str(queue.root), "view": view}
    if view == "announcements":
        value = _tier_rows(queue.root)
        return _section(value, inputs=inputs, errors=[str(v["reason"]) for v in value["invalid"]])
    if view == "diagnostics":
        notes, unreadable = [], []
        value = _starvation_tiers(queue, now=time.time() if now is None else now,
                                  notes=notes, unreadable=unreadable, reader=reader)
        return _section({"tiers": value, "notes": notes, "unreadable": unreadable},
                        inputs=inputs, errors=unreadable)
    if view not in {"tokens", "sources"}:
        raise ValueError(f"unknown tier census view: {view}")
    reader = _fresh(reader)
    ok = True
    try:
        announced = {str(record.get("tier_id")): record for record in _tiers(reader, queue)
                     if isinstance(record, dict) and record.get("tier_id")}
    except Exception:
        announced, ok = {}, False
    try:
        tier_ids = _tier_ids(reader, queue)
    except Exception:
        tier_ids, ok = sorted(announced), False
    if view == "sources":
        return _section({"announced": announced, "tier_ids": tier_ids}, inputs=inputs, complete=ok)
    tokens = []
    for tier_id in sorted(set(tier_ids) | set(announced)):
        try:
            counted = _ledger_counts(reader, queue, tier_id)
        except Exception as exc:
            counted = exc
        if isinstance(counted, Exception):
            ok = False
            continue
        capacity, available = counted
        if not isinstance(capacity, dict) or not isinstance(available, dict):
            ok = False
            continue
        for kind in sorted(set(capacity) | set(available)):
            total, free = nonnegative_number(capacity.get(kind, 0)), nonnegative_number(available.get(kind, 0))
            if total is None or free is None:
                ok = False
                continue
            tokens.append((tier_id, str(kind), total, free, total - free))
    return _section({"announced": announced, "tokens": tokens}, inputs=inputs, complete=ok)


def movers(queue_root, *, view="live", limit=DEFAULT_MOVER_RECEIPTS, reader=None,
           receipts=None, now=None, window_seconds=None) -> Section:
    inputs = {"queue_root": str(queue_root), "view": view}
    if view == "live":
        unreadable = []
        value = _mover_live_rows(Path(queue_root), unreadable=unreadable)
        return _section(value, inputs=inputs, errors=unreadable)
    if view == "filed":
        value = _filed_move_receipts(Path(queue_root), limit=limit)
        return _section(value, inputs=inputs, bound={"receipts": limit},
                        complete=not value["unreadable"] and not value["truncated"])
    if view == "promotion":
        return _promotion_active(pool.PoolQueue(queue_root), _fresh(reader))
    if view == "delivery":
        return _delivery(pool.PoolQueue(queue_root), receipts, reader=_fresh(reader),
                         now=now, window_seconds=window_seconds)
    if view != "counts":
        raise ValueError(f"unknown mover census view: {view}")
    reader, ok, counts = _fresh(reader), True, {}
    for state in (pool.record_store.READY, pool.record_store.CLAIMED):
        count = 0
        complete = True
        try:
            listed = reader.queue_records(Path(queue_root) / state)
        except OSError:
            ok = False
            continue
        for _path, parsed in listed:
            record = parsed.record
            if not isinstance(record, dict):
                ok = False
                complete = False
                continue
            residency = record.get("residency")
            if isinstance(residency, Mapping) and "range_start_bytes" in residency:
                count += 1
        counts[state] = count if complete else None
    return _section(counts, inputs=inputs, complete=ok)


@dataclass(frozen=True)
class OfferCapture:
    workers: Mapping[str, dict | None]
    notes: list[str]
    admission: Mapping[str, dict]
    sampled_unix: float


def _offer_admission(queue, workers, *, reader):
    admission = {}
    for host, offer in workers.items():
        if _valid_pool_offer(host, offer):
            base = queue.root / pool.RESERVATIONS / host / "adaptive"
            admission[host] = {
                "cpu": _pool_sidecar(base / "cpu-sample.json", reader=reader),
                "gpu": _pool_sidecar(base / "gpu-state.json", reader=reader)
                       if offer.get("has_gpu") else None,
            }
    return admission

def nonnegative_number(value: object) -> float | None:
    if type(value) not in (int, float):
        return None
    try:
        result = float(value)
    except (OverflowError, ValueError):
        return None
    return result if math.isfinite(result) and result >= 0 else None

def absent(error: OSError) -> bool:
    """Whether this error says "nothing is there", not "the mount did not answer".

    ``ENOENT`` and ``ENOTDIR`` are absence: a key that was never published, a
    state directory a queue has not created yet.  Every other ``OSError`` --
    ``ESTALE`` and ``EIO`` are the two the fleet actually sees (#208) -- is a
    mount that failed to answer, and ``pool.record_store.read_document`` keeps those loud on
    purpose.  A reader that swallowed them would answer "no action starts with
    that prefix" during an NFS burst, with ``complete: true`` next to it, and
    those two sentences call for opposite responses.  Every call site below
    runs inside a ``call.read`` lambda, so a raised error becomes a named
    section with ``complete: false``, never an internal-error frame.
    """

    return isinstance(error, (FileNotFoundError, NotADirectoryError))

def _pool_records(directory: Path, *, reader=None) -> tuple[dict[str, dict | None], list[str]]:
    """Audit a live directory without treating failed reads as an empty queue."""
    if reader is not None:
        return reader.pool_records(directory)
    try:
        listed = pool.record_store.RecordStore(directory.parent).required_diagnostic_records_at(
            directory,
        )
    except OSError as exc:
        return {}, [f"pool {directory}: {_unreadable_reason(exc)}"]
    records: dict[str, dict | None] = {}
    notes: list[str] = []
    for path, parsed in listed:
        records[path.stem] = parsed.record
        if parsed.error is not None:
            notes.append(f"pool {path}: {parsed.error}")
    return records, notes

def _releases(record: Mapping[str, object]) -> int | None:
    """How often this action was returned to the queue without starting.

    ``reap_stale`` releases a claim that never wrote a lease and never
    published an attempt: the action leaves ``ready``, nothing runs, and it
    comes back with its attempt count untouched and this counter raised
    (issue #222).  The release is right; the silence was not.  ``None`` when
    the record has never been released, so an untouched action reads as absent
    rather than as a measured zero.
    """

    count = record.get("unstarted_releases")
    return count if type(count) is int and count > 0 else None

def _age(timestamp: object, now: float) -> float | None:
    if type(timestamp) not in (int, float) or not math.isfinite(timestamp):
        return None
    return now - timestamp

def _progress_observation(claim: dict, lease: dict | None, *, now: float) -> str | None:
    """How long this action has been quiet, against what it is allowed.

    The column beside it -- OUTPUT age -- is the number #480 says proves
    nothing: a launcher writing log lines is not an application committing
    work.  This is the other reading, and it is the one the watchdog acts on.
    ``None`` when the action declared no policy, so a whole-run ceiling still
    governs it and ``KILL AT`` is the number to read.

    Held to the same attempt identity as the execution observation: a lease
    naming another claim describes another run.
    """

    value = (lease or {}).get("progress_observation")
    if not isinstance(value, dict) or lease is None:
        return None
    if any(lease.get(left) is None or lease.get(left) != claim.get(right)
           for left, right in _OBSERVATION_IDENTITY):
        return None
    quiet, grace = value.get("quiet_s"), value.get("grace_s")
    if not all(type(v) in (int, float) and math.isfinite(v) and v >= 0
               for v in (quiet, grace)):
        return None
    phase = value.get("phase")
    accepted = value.get("accepted_count")
    said = f"{phase if isinstance(phase, str) and phase else '?'} "
    said += f"quiet {float(quiet):.0f}/{float(grace):g}s"
    if type(accepted) is int:
        said += f" ({accepted} accepted)"
    return said

def _execution_observation(claim: dict, lease: dict | None, *, now: float) -> dict:
    """Interpret optional execution evidence without granting recovery rights."""
    unknown = {"state": "unavailable", "launcher_alive": None,
               "age_s": None, "last_output_age_s": None,
               "note": "execution observation unavailable"}
    value = (lease or {}).get("execution_observation")
    if value is None:
        return unknown
    if (not isinstance(value, dict)
            or any(lease.get(left) is None or lease.get(left) != claim.get(right)
                   for left, right in _OBSERVATION_IDENTITY)):
        return {**unknown, "state": "invalid", "note": "execution observation identity invalid"}
    sampled = value.get("sampled_unix")
    output = value.get("last_output_unix")
    try:
        age = _age(sampled, now)
        beat_age = _age(lease.get("heartbeat_unix"), now)
        output_age = _age(output, now)
    except OverflowError:
        return {**unknown, "state": "invalid", "note": "execution observation timestamps invalid"}
    counts = [value.get(name) for name in ("stdout_bytes", "stderr_bytes")]
    if (age is None or age < 0 or beat_age is None or beat_age < 0 or age < beat_age
            or type(value.get("launcher_alive")) is not bool
            or any(type(count) is not int or count < 0 for count in counts)
            or (output is not None and (output_age is None or output_age < age))
            or (output is None and any(counts))):
        return {**unknown, "state": "invalid", "note": "execution observation values invalid"}
    if age > pool.LEASE_TIMEOUT_S or beat_age > pool.LEASE_TIMEOUT_S:
        return {**unknown, "state": "stale", "age_s": age,
                "note": "execution observation stale; launcher liveness unknown"}
    alive = value["launcher_alive"]
    note = ("launcher running" if alive else
            "launcher exited; descendant liveness unknown")
    note += ("; no output observed" if output is None else f"; output {output_age:.0f}s ago")
    note += f"; {_child_note(value.get('child'))}"
    return {**value, "state": "fresh", "age_s": age,
            "last_output_age_s": output_age, "note": note}

def _child_note(child: object) -> str:
    """One clause about the payload the resource daemon forked.

    The launcher's own liveness says nothing about it -- ``766d7ae5...`` held
    a claim for an hour on ``launcher_alive: true`` while the payload sat in a
    futex wait (#600) -- so the reader that decides whether to surface a claim
    needs the child's own liveness, CPU and silence in the same line.  An
    observation that predates the field, or one the worker could not take,
    reads as unobserved: never as an absent child.
    """

    if not isinstance(child, dict):
        return "child unobserved"
    if child.get("source") != "resource-scope-cgroup" or "alive" not in child:
        return "child unobserved"
    count = child.get("pid_count")
    clause = ("child running" if child.get("alive") else "child gone")
    if isinstance(count, int):
        clause += f" ({count} pid{'' if count == 1 else 's'})"
    cpu = child.get("cpu_seconds")
    if isinstance(cpu, (int, float)) and not isinstance(cpu, bool):
        clause += f", cpu {float(cpu):.0f}s"
    silent = child.get("silent_s")
    if isinstance(silent, (int, float)) and not isinstance(silent, bool):
        clause += f", silent {float(silent):.0f}s"
    return clause

def _pool_sidecar(path: Path, *, reader=None) -> dict | None | Exception:
    """Retain read failures for the row that owns this observation."""
    if reader is not None:
        return reader.sidecar(path)
    parsed = pool.record_store.RecordStore.diagnostic_sidecar_at(path)
    return parsed.error if parsed.error is not None else parsed.record

def _admission_sample(record: dict | None | Exception, *, now: float, max_age_s: float) -> dict:
    """Classify persisted evidence after every census read has finished."""
    if isinstance(record, Exception):
        return {"state": "unavailable", "note": str(record)}
    if record is None:
        return {"state": "unavailable", "note": "no sample recorded"}
    age = _age(record.get("sampled_unix"), now)
    return {"state": "fresh" if age is not None and 0 <= age <= max_age_s else "stale",
            "age_s": age, "record": record}

def _valid_pool_offer(host: str, offer: dict | None) -> bool:
    return (offer is not None and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', host) is not None
            and offer.get("schema") == pool.POOL_OFFER_SCHEMA_V1
            and offer.get("host") == host and isinstance(offer.get("capacity"), dict)
            and isinstance(offer.get("tags"), list)
            and all(type(v) is int and v >= 0 for v in offer['capacity'].values()))

def _valid_pool_item(key: str, record: dict | None) -> bool:
    return (record is not None and re.fullmatch('[a-f0-9]{64}', key) is not None
            and record.get('action_key') == key and record.get('schema') == pool.POOL_ITEM_SCHEMA_V1)

def _offer_drain(offer: Mapping[str, object]) -> dict | None:
    """The drain a *fresh* offer publishes, or ``None`` when it offers admission.

    The offer's ``state`` is written only by a loop holding the maintenance
    gate (#1204): an absent value is an offer from before the field existed or
    from an ordinary open gate, and anything other than ``"draining"`` is not
    evidence of a parked loop.  The holder, reason and change time are copied
    only in the shapes a reader can use; a malformed one reads as unknown
    rather than being rendered.
    """

    if offer.get("state") != "draining":
        return None
    owner = offer.get("drain_owner")
    reason = offer.get("drain_reason")
    stamp = offer.get("drain_changed_unix")
    changed = None
    if type(stamp) in (int, float):
        try:
            stamp = float(stamp)
        except OverflowError:
            stamp = None
        if stamp is not None and math.isfinite(stamp):
            changed = stamp
    return {
        "owner": owner[:_DRAIN_OWNER_DISPLAY] if isinstance(owner, str) else None,
        "reason": (reason[:_DRAIN_REASON_DISPLAY]
                   if isinstance(reason, str) else None),
        "changed_unix": changed,
    }



def _pool_claim_denials(queue: pool.PoolQueue, *, reader=None) -> tuple[list[dict], list[str]]:
    """Read optional host snapshots without turning a bad diagnostic into a bad job."""
    denials, notes = [], []
    try:
        if reader is not None:
            hosts = reader.claim_denial_hosts(queue)
        else:
            hosts, errors = queue.records.diagnostic_directories_at(queue.root / pool.RESERVATIONS)
            notes.extend(f"pool claim denials {name}: {exc}" for name, exc in errors)
    except FileNotFoundError:
        return denials, notes
    except OSError as exc:
        return denials, [f"pool claim denials: {exc}"]
    for directory in sorted(hosts):
        snapshot = _pool_sidecar(directory / 'adaptive' / pool.CLAIM_DENIALS, reader=reader)
        if snapshot is None:
            continue  # Older workers and hosts that have skipped nothing.
        if isinstance(snapshot, Exception):
            notes.append(f"pool claim denials {directory.name}: {snapshot}")
            continue
        records = snapshot.get('records')
        if (snapshot.get('schema') != pool.CLAIM_DENIALS_SCHEMA_V1
                or not isinstance(records, dict) or len(records) > pool.MAX_CLAIM_DENIALS):
            notes.append(f"pool claim denials {directory.name}: invalid snapshot")
            continue
        for denial in records.values():
            try:
                valid = (isinstance(denial, dict)
                         and re.fullmatch('[a-f0-9]{64}', str(denial.get('action_key'))) is not None
                         and denial.get('host') == directory.name
                         and isinstance(denial.get('reason'), str) and bool(denial['reason'])
                         and isinstance(denial.get('evidence'), dict)
                         and all(type(denial.get(field)) in (int, float)
                                 and math.isfinite(denial[field]) and denial[field] >= 0
                                 for field in ('published_unix', 'denied_unix')))
            except OverflowError:
                valid = False
            if valid:
                denials.append(denial)
            else:
                notes.append(f"pool claim denial {directory.name}: invalid record")
    return denials, notes

def _claim_denials_for(record: dict, denials: Sequence[dict], *, now: float) -> list[dict]:
    """Keep each host's latest observation for this exact submission generation."""
    by_host = {}
    published = record.get('published_unix')
    if type(published) not in (int, float):
        return []
    for denial in denials:
        if (denial['action_key'] != record['action_key']
                or denial['published_unix'] != published or denial['denied_unix'] > now):
            continue
        host = denial['host']
        if host in by_host and by_host[host]['denied_unix'] >= denial['denied_unix']:
            continue
        decision = denial['evidence'].get('decision')
        subreason = decision.get('reason') if isinstance(decision, dict) else None
        by_host[host] = {**denial, 'age_s': now - denial['denied_unix'],
                         'decision_reason': subreason if isinstance(subreason, str) else None}
    return [by_host[host] for host in sorted(by_host)]

def offers(queue_root: str | Path, *, reader=None,
           captured: OfferCapture | None = None) -> Section[dict]:
    """Worker offers and their conditional admission samples, independently read."""
    queue = pool.PoolQueue(Path(queue_root).absolute())
    if captured is None:
        workers, worker_notes = _pool_records(queue.root / pool.record_store.WORKERS, reader=reader)
        admission = _offer_admission(queue, workers, reader=reader)
        now = time.time()
    else:
        workers, worker_notes = captured.workers, captured.notes
        admission, now = captured.admission, captured.sampled_unix
    notes, unreadable = list(worker_notes), list(worker_notes)
    nodes: list[dict] = []
    live: list[dict] = []
    for host, offer in workers.items():
        if not _valid_pool_offer(host, offer):
            nodes.append({"node": host, "state": "unreadable", "healthy": False,
                          "reason": "invalid worker offer"})
            notes.append(f"pool worker {host}: invalid worker offer")
            unreadable.append(f"pool worker {host}: invalid worker offer")
            continue
        timing = pool.offer_timing(offer.get("announced_unix"), now=now)
        age = timing.age_s
        fresh = age is not None and age <= pool.OFFER_TIMEOUT_S
        if timing.clock_skew_s:
            disposition = "tolerated" if age is not None else "ignored"
            notes.append(
                f"pool worker {host}: offer announced {timing.clock_skew_s:.3f}s "
                f"in the future (clock skew; {disposition}, "
                f"limit {pool.OFFER_FUTURE_TOLERANCE_S:g}s)")
        if fresh:
            live.append(offer)
        else:
            notes.append(f"pool worker {host}: stale or invalid offer timestamp")
        # A fresh offer a parked loop republishes says it is draining, and
        # names the holder and reason the gate carries (#1204).  It is alive
        # -- just not admitting -- so it is not unhealthy, and it is not
        # stale: only a stopped loop leaves an expired offer.  A stale offer
        # is read with none of this, so a drain that stopped announcing
        # still reads as the loop having stopped.
        drain = _offer_drain(offer) if fresh else None
        if drain is not None:
            state = "draining"
            note = "draining for maintenance"
            if drain["owner"]:
                note += f"; owner {drain['owner']}"
            if drain["reason"]:
                note += f"; {drain['reason']}"
        else:
            state = "live" if fresh else "stale"
            note = None if fresh else "offer expired or timestamp invalid"
            # An offer nobody has refreshed for a day is a box that left the
            # fleet, not a loop that is restarting (#1040).  It is marked, never
            # deleted: the file is another box's record, and only that box, or
            # an operator who knows it is gone, may remove it.
            if not fresh and age is not None and age > OFFER_RETIRED_AFTER_S:
                state = "retired"
                note = (f"offer unrefreshed for {age / 86400:.1f} days; "
                        "box treated as retired (record kept)")
        nodes.append({
            "node": host, "transport": "pool", "state": state,
            "healthy": fresh, "age_s": age, "capacity": offer.get("capacity"),
            "offer_clock_skew_s": timing.clock_skew_s,
            "observed_capacity": offer.get("observed_capacity"),
            "foreign": offer.get("foreign"), "observed_detail": offer.get("observed_detail"),
            "features": offer.get("tags"), "has_gpu": offer.get("has_gpu"),
            "container_class_verdict": offer.get("container_class_verdict"),
            "runtime_commit": offer.get("runtime_commit"),
            # Absent on a pre-#254 offer, and that is "not measured": the
            # reader below prints it as unknown and never as zero.
            "loops": offer.get("loops"),
            "timeout_ceiling_s": offer.get("timeout_ceiling_s"),
            "draining": drain is not None,
            "drain_owner": drain["owner"] if drain else None,
            "drain_reason": drain["reason"] if drain else None,
            "drain_changed_unix": drain["changed_unix"] if drain else None,
            "reason": note,
            "admission": {
                "cpu": _admission_sample(admission[host]['cpu'], now=now,
                                         max_age_s=pool.cpu_admission.MAX_SAMPLE_AGE_S),
                "gpu": _admission_sample(admission[host]['gpu'], now=now,
                                         max_age_s=pool.gpu_admission.MAX_SAMPLE_AGE_S)
                       if offer.get('has_gpu') else {"state": "not applicable"},
            },
        })
    return _section({"nodes": nodes, "live": live, "notes": notes,
                     "unreadable": unreadable, "sampled_unix": now},
                    inputs={"workers": str(queue.root / pool.record_store.WORKERS),
                            "admission_hosts": tuple(admission), "sampled_unix": now},
                    errors=[*unreadable, *(f"pool admission {host} {kind}: {value}"
                        for host, samples in admission.items() for kind, value in samples.items()
                        if isinstance(value, Exception))])


def action_rows(queue_root: str | Path, *, reader=None) -> Section[dict]:
    """Read worker offers, active records and existing admission evidence.

    This is a non-atomic diagnostic census, never a new admission decision.
    File errors retain unknown counts and unreadable rows. PoolQueue supplies
    the resource and placement rules used by workers. Read all sidecars before
    sampling time, so a later stalled read cannot extend any earlier lifetime.
    """
    queue = pool.PoolQueue(Path(queue_root).absolute())
    workers, worker_notes = _pool_records(queue.root / pool.record_store.WORKERS, reader=reader)
    ready, ready_notes = _pool_records(queue.records.dir(pool.record_store.READY), reader=reader)
    claimed, claim_notes = _pool_records(
        queue.records.dir(pool.record_store.CLAIMED),
        reader=reader,
    )
    admission = _offer_admission(queue, workers, reader=reader)
    sidecars = {}
    for state, records in ((pool.record_store.READY, ready), (pool.record_store.CLAIMED, claimed)):
        for key, record in records.items():
            if _valid_pool_item(key, record):
                path = queue.records.passes_path(
                    key,
                ) if state == pool.record_store.READY else queue.records.lease_path(
                    key,
                )
                sidecars[state, key] = _pool_sidecar(path, reader=reader)
    prewarms = {key: _pool_sidecar(queue.records.prewarm_path(key), reader=reader)
                for key, record in ready.items() if _valid_pool_item(key, record)}
    denial_records, denial_notes = _pool_claim_denials(queue, reader=reader)
    now = time.time()
    notes = [*worker_notes, *ready_notes, *claim_notes, *denial_notes]
    # The half of ``notes`` that means "this census is missing something",
    # kept apart from the half that means "the fleet is in this state".  A
    # directory that would not list and a record that would not parse make the
    # census partial; a worker that stopped announcing is a fleet fact, fully
    # read.  ``complete`` below is the first, and only the first, because it is
    # what the run's exit status is derived from and a stale offer must not
    # spend the signal that says the mount did not answer.
    unreadable = [*worker_notes, *ready_notes, *claim_notes, *denial_notes]
    observed = offers(queue_root, reader=reader,
                      captured=OfferCapture(workers, worker_notes, admission, now))
    nodes, live = observed.value["nodes"], observed.value["live"]
    notes.extend(observed.value["notes"][len(worker_notes):])
    unreadable.extend(observed.value["unreadable"][len(worker_notes):])
    jobs: list[dict] = []
    valid_counts = {pool.record_store.READY: len(ready) if not ready_notes else None,
                    pool.record_store.CLAIMED: len(claimed) if not claim_notes else None}
    for state, records in ((pool.record_store.READY, ready), (pool.record_store.CLAIMED, claimed)):
        for key, record in records.items():
            row = {"action_key": key, "action_key_prefix": key[:12], "transport": "pool",
                   "state": state.upper(), "node": None, "reason": None}
            try:
                if record is None or not _valid_pool_item(key, record):
                    raise ValueError('invalid action record')
                sidecar = sidecars[state, key]
                if isinstance(sidecar, Exception):
                    raise sidecar
                row.update(resources=queue.demand_of(record), constraint=record.get('tags'),
                           priority=record.get('priority', 0),
                           priority_reason=pool.normalize_priority_reason(record.get('priority_reason')),
                           submitted_host=record.get('published_by'),
                           tested_repository=pool.tested_repository_of(record),
                           unstarted_releases=_releases(record),
                           age_s=_age(record.get(
                               'claimed_unix',
                           ) if state == pool.record_store.CLAIMED
                                      else record.get('published_unix'), now))
                row['admission_denials'] = _claim_denials_for(record, denial_records, now=now)
                if state == pool.record_store.READY:
                    denial = sidecar or {}
                    count, first = denial.get('passes', 0), denial.get('first_unix')
                    hosts = sorted({str(offer['host']) for offer in queue._matching_offers(record, live=live)})
                    if pool.is_box_local_path(record.get('checkout_root')):
                        hosts = [host for host in hosts if host == record.get('published_by')]
                    row.update(placeable_hosts=hosts if live else None,
                               admission_passes=int(count) if isinstance(count, (int, float)) else 0,
                               admission_wait_s=max(0.0, now - float(first))
                               if isinstance(first, (int, float)) else 0.0)
                    row['reason'] = ('no fresh worker offers; placement unknown' if not live
                                     else 'no matching live worker' if not hosts
                                     else 'awaiting admission; matching worker capacity is not a grant')
                    warm = prewarms.get(key)
                    if isinstance(warm, dict) and (
                            warm.get('schema') != pool.POOL_PREWARM_SCHEMA_V1
                            or warm.get('action_key') != key
                            or not isinstance(warm.get('errors', []), list)):
                        warm = ValueError('invalid prewarm receipt')
                    if isinstance(warm, Exception):
                        message = f'prewarm evidence unreadable for {key[:12]}: {warm}'
                        row['reason'] += '; ' + message
                        notes.append(message)
                        unreadable.append(message)
                    elif isinstance(warm, dict) and warm.get('errors'):
                        # This is an observation, not terminal authority: a
                        # prewarm receipt has no bound producer/retry-death
                        # proof, and can even predate a recompute generation.
                        errors = '; '.join(str(error) for error in warm['errors'][:3])
                        row['reason'] += (
                            f"; recorded prewarm input errors (permanence unproven, "
                            f"finished_unix={warm.get('finished_unix')}): {errors}")
                    row['preempted_by'] = record.get('preempted_by')
                    if row['preempted_by']:
                        # This row is a requeue, not a first submission, and an
                        # operator reading a queue that grew a row it did not
                        # submit should be told which admission decision put it
                        # there (#364).
                        row['reason'] += (
                            "; requeued after preemption by "
                            f"{str(row['preempted_by'])[:12]}")
                    if row['unstarted_releases']:
                        # A key the reaper keeps handing back reads exactly
                        # like a key nobody has got to yet, and the reason
                        # above is the one an operator acts on.  Issue #263:
                        # the count was on the record and no reader said it,
                        # so a bouncing action looked like a quiet queue for
                        # the whole of ``pbrun --wait-s``.
                        row['reason'] += (
                            f"; released {row['unstarted_releases']} time"
                            f"{'' if row['unstarted_releases'] == 1 else 's'} "
                            "before starting")
                else:
                    row.update(node=record.get('claimed_host'), owner=record.get('claimed_by'),
                               cpu_allocation=record.get('cpu_allocation'),
                               gpu_admission=record.get('gpu_admission'),
                               cleanup_pending=bool(record.get('finish_pending') or record.get('stop_pending')
                                                    or record.get('container_cleanup_pending')),
                               # How hard, and for how long.  A cleanup pending
                               # for three seconds and one pending for six hours
                               # over four hundred attempts wrote the same row,
                               # and an operator acts on them very differently
                               # (#288).  ``None`` on a record written before
                               # these fields existed.
                               cleanup_attempts=record.get('container_cleanup_attempts'),
                               cleanup_pending_s=_age(
                                   record.get('container_cleanup_first_failed_unix'), now))
                    beat = None if sidecar is None else queue._effective_heartbeat(
                        sidecar, now=now, read=lambda path: _pool_sidecar(path, reader=reader))
                    if sidecar is not None and not isinstance(beat, (int, float)):
                        raise pool.PoolContractError(f"lease has no heartbeat: {key}")
                    age = None if sidecar is None else now - float(beat)
                    row['lease_age_s'] = age
                    row['stale'] = age is None or not math.isfinite(age) or not 0 <= age <= pool.LEASE_TIMEOUT_S
                    if row['stale']:
                        row['reason'] = 'lease missing, stale or invalid; process liveness unknown'
                    elif row['cleanup_pending']:
                        row['reason'] = 'cleanup pending; reservation retained'
                        if row.get('cleanup_attempts'):
                            row['reason'] += (
                                f"; {row['cleanup_attempts']} attempt"
                                f"{'' if row['cleanup_attempts'] == 1 else 's'}")
                            if row.get('cleanup_pending_s') is not None:
                                row['reason'] += (
                                    f" over {row['cleanup_pending_s']:.0f}s")
                    observation = _execution_observation(record, sidecar, now=now)
                    row['execution_observation'] = observation
                    row['progress_observation'] = _progress_observation(
                        record, sidecar, now=now)
                    row['reason'] = '; '.join(filter(None, (row.get('reason'), observation['note'])))
            except (OSError, ValueError, TypeError, KeyError) as exc:
                row.update(state='UNREADABLE', reason=str(exc))
                valid_counts[state] = None
                notes.append(f"pool {state}/{key}: {exc}")
                unreadable.append(f"pool {state}/{key}: {exc}")
            jobs.append(row)
    empty = (valid_counts[pool.record_store.READY] == 0 and valid_counts[pool.record_store.CLAIMED] == 0)
    value = {"nodes": nodes, "jobs": jobs, "notes": notes,
            "queue": {"ready": valid_counts[pool.record_store.READY], "claimed": valid_counts[pool.record_store.CLAIMED],
                      "empty": empty if all(v is not None for v in valid_counts.values()) else None,
                      "complete": not unreadable, "unreadable": unreadable,
                      "live_workers": len(live),
                      "sampled_unix": now}}
    return _section(value, inputs={"workers": str(queue.root / pool.record_store.WORKERS),
                    "ready": tuple(ready), "claimed": tuple(claimed),
                    "sidecars": tuple(f"{state}/{key}" for state, key in sidecars),
                    "prewarms": tuple(prewarms), "denials": "latest per generation",
                    "sampled_unix": now}, errors=[*unreadable, *(error for error in observed.errors
                        if error not in unreadable)])


def _starvation_accepted_phase(lease: Mapping[str, object] | None) -> str | None:
    """The phase the consumer's progress record says it is reading now.

    The same field ``tier_loop.live_consumers`` reads through
    ``RecordStore.accepted_progress_phase`` (the lease's authenticated accepted
    observation), parsed here so this census does not import the tier loop.
    ``None`` is "no accepted progress": a ready consumer with no lease, or a
    claim whose worker has accepted nothing yet.
    """

    if not isinstance(lease, Mapping):
        return None
    observed = lease.get("progress_observation")
    if not isinstance(observed, Mapping):
        return None
    accepted = observed.get("last_accepted")
    if not isinstance(accepted, Mapping):
        return None
    phase = accepted.get("phase")
    return phase if isinstance(phase, str) and phase else None

def _starvation_quiet(lease: Mapping[str, object] | None,
                      ) -> tuple[float | None, float | None]:
    """How long this claim has been quiet, and against what it is allowed.

    The same ``quiet_s``/``grace_s`` pair ``_progress_observation`` renders
    for the jobs table, as numbers rather than prose.  ``(None, None)`` when
    the lease carries no progress observation, so "no policy" and "quiet"
    never read the same.
    """

    if not isinstance(lease, Mapping):
        return None, None
    observed = lease.get("progress_observation")
    if not isinstance(observed, Mapping):
        return None, None
    quiet, grace = observed.get("quiet_s"), observed.get("grace_s")
    if not all(type(value) in (int, float) and math.isfinite(value) and value >= 0
               for value in (quiet, grace)):
        return None, None
    return float(quiet), float(grace)

def _starvation_child(lease: Mapping[str, object] | None) -> dict | None:
    """The payload scope's own liveness, CPU and silence, or ``None``.

    Read off the lease's ``execution_observation`` beside the progress one:
    a launcher writing log lines is not an application committing work, and
    a claim quiet at the progress level with a busy child is not waiting on
    data.  Lenient where ``_execution_observation`` is strict -- identity and
    freshness there decide the jobs table, here the raw numbers travel with
    the verdict so the reader audits the rule rather than trusting it.
    """

    if not isinstance(lease, Mapping):
        return None
    observed = lease.get("execution_observation")
    if not isinstance(observed, Mapping):
        return None
    child = observed.get("child")
    if not isinstance(child, Mapping):
        return None
    out: dict[str, object] = {}
    for field in ("alive", "pid_count", "cpu_seconds", "silent_s"):
        value = child.get(field)
        if isinstance(value, bool) and field != "alive":
            continue
        if value is None or isinstance(value, (bool, int, float, str)):
            out[field] = value
    return out or None

def _starvation_waiting(quiet: float | None, grace: float | None,
                        child: Mapping[str, object] | None) -> tuple[bool, str]:
    """Whether this claimed consumer reads as waiting on data, and the rule.

    Quiet past half its grace with a payload scope to match: silent long
    enough to corroborate, or unobserved so nothing exonerates it.  The rule
    travels as a string because a boolean without its inputs is an opinion.
    """

    rule = (f"quiet_s/grace_s >= {STARVATION_QUIET_FRACTION:g} and "
            f"(child unobserved or silent_s >= {STARVATION_CHILD_SILENT_S:g}s)")
    if quiet is None or grace is None or grace <= 0:
        return False, rule
    if quiet / grace < STARVATION_QUIET_FRACTION:
        return False, rule
    silent = (child or {}).get("silent_s")
    if isinstance(silent, bool) or not isinstance(silent, (int, float)):
        return True, rule
    return bool(silent >= STARVATION_CHILD_SILENT_S), rule

def _starvation_holders(queue: pool.PoolQueue, tier_id: str,
                        keys: Iterable[str], notes: list[str]) -> dict[str, bool | None]:
    """Which mover keys hold tier tokens now, the pin the tier loop reads.

    A mover holding tokens is resident by definition -- the invariant
    ``tier_loop._mover_state`` stages from -- so the ledger answers "what is
    on the tier" without walking the device.  An unreadable ledger answers
    ``None`` per key rather than False: "could not read" is not "not staged".
    """

    try:
        ledger = queue.tier(tier_id)
    except (OSError, ValueError, pool.PoolContractError) as exc:
        notes.append(f"starvation tier ledger {tier_id}: {exc}")
        return {key: None for key in keys}
    staged: dict[str, bool | None] = {}
    for key in keys:
        try:
            staged[key] = bool(ledger.holder_state(key))
        except (OSError, ValueError, pool.PoolContractError) as exc:
            notes.append(f"starvation tier ledger {tier_id} holder {key[:12]}: {exc}")
            staged[key] = None
    return staged

def _starvation_plan_entry(queue: pool.PoolQueue, key: str, raw: object,
                           *, ready: set[str], claimed: set[str],
                           notes: list[str], unreadable: list[str],
                           now: float, reader=None, active_complete: bool = True) -> dict:
    """One residency plan's promotion state, derived, never decided."""

    prefix = key[:12]
    if not isinstance(raw, Mapping):
        notes.append(f"starvation plan {prefix}: not a JSON object")
        unreadable.append(f"starvation plan {prefix}: not a JSON object")
        return {"consumer_action_key_prefix": prefix, "valid": False,
                "note": "not a JSON object"}
    try:
        plan = residency_plan.validate_plan(raw)
    except ValueError as exc:
        notes.append(f"starvation plan {prefix}: {exc}")
        unreadable.append(f"starvation plan {prefix}: {exc}")
        return {"consumer_action_key_prefix": prefix, "valid": False,
                "note": str(exc)}
    tier_id = str(plan["tier_id"])
    ram_tier_id = plan.get("ram_tier_id")
    ram_tier_id = str(ram_tier_id) if isinstance(ram_tier_id, str) else None
    state = ("claimed" if key in claimed else
             "ready" if key in ready else "absent" if active_complete else None)
    if not active_complete:
        unreadable.append(f"starvation plan {prefix}: active queue population incomplete")
    accepted: str | None = None
    if state == "claimed":
        lease = _pool_sidecar(queue.records.lease_path(key), reader=reader)
        if isinstance(lease, Exception):
            notes.append(f"starvation plan {prefix} lease: {lease}")
            unreadable.append(f"starvation plan {prefix} lease: {lease}")
        else:
            accepted = _starvation_accepted_phase(lease)
    # Fragments are per consumer, one file per mover; an invalid one is
    # skipped by the reader, exactly as the consumer's own compose sees it.
    fragments_known = True
    fragments = []
    expected_fragment_keys = set(residency_plan.mover_keys(plan)) | set(residency_plan.ram_mover_keys(plan))
    try:
        directory = residency_map.fragment_path(
            queue.records.residency_fragment_root(), key, key).parent
        captured_fragments = queue.records.inspection_fragment_records_at(directory)
    except (OSError, ValueError) as exc:
        notes.append(f"starvation plan {prefix} fragments: {exc}")
        unreadable.append(f"starvation plan {prefix} fragments: {exc}")
        captured_fragments = []
        fragments_known = False
    for path, observed_fragment in captured_fragments:
        try:
            if observed_fragment.error is not None:
                raise observed_fragment.error
            fragments.append(residency_map.validate_fragment(observed_fragment.record))
        except (OSError, ValueError) as exc:
            if path.stem not in expected_fragment_keys:
                continue
            note = f"starvation plan {prefix} fragment {path.name}: {exc}"
            notes.append(note)
            unreadable.append(note)
            fragments_known = False
    fragment_movers = {str(fragment.get("mover_action_key")): str(fragment.get("tier_id"))
                       for fragment in fragments
                       if isinstance(fragment.get("mover_action_key"), str)}

    def fragment_present(mover: str) -> bool | None:
        if mover in fragment_movers:
            return True
        return False if fragments_known else None
    mover_keys = []
    for phase in plan["phases"]:
        chunks = phase.get("stage_chunks")
        if isinstance(chunks, list):
            mover_keys += [str(chunk["mover_row"]["action_key"])
                           for chunk in chunks if isinstance(chunk, Mapping)]
        elif "mover_row" in phase:
            mover_keys.append(str(phase["mover_row"]["action_key"]))
    ram_keys = []
    for phase in plan["phases"]:
        chunks = phase.get("ram_chunks")
        if isinstance(chunks, list):
            ram_keys += [str(chunk["ram_mover_row"]["action_key"])
                         for chunk in chunks if isinstance(chunk, Mapping)]
        elif "ram_mover_row" in phase:
            ram_keys.append(str(phase["ram_mover_row"]["action_key"]))
    staged = _starvation_holders(queue, tier_id, mover_keys, notes)
    ram_staged = (_starvation_holders(queue, ram_tier_id, ram_keys, notes)
                  if ram_tier_id is not None else {})
    if any(value is None for value in (*staged.values(), *ram_staged.values())):
        unreadable.append(f"starvation plan {prefix}: tier holder observations incomplete")

    def observed_publication(mover: str, holdings: Mapping[str, bool | None]) -> bool | None:
        if mover in ready or mover in claimed or holdings.get(mover) is True:
            return True
        return False if active_complete and holdings.get(mover) is False else None
    # Reservations and residency, reported apart (#759).  ``_starvation_
    # holders`` above answers the ledger's question -- what is booked -- and
    # the cursor used to print that as ``staged``, which is how it called the
    # head phase staged while the copy was still running and the identity
    # proof saw nothing.  ``staged`` is now the one shared readiness
    # predicate the tier window gates on, so the census and the gate cannot
    # disagree; ``reserved`` keeps the booking visible beside it rather than
    # hiding the capacity it is really using.  Evidence that cannot be read
    # reports ``None`` -- unknown -- never a clean ``False``.
    stage_resident: set[str] = set()
    ram_resident: set[str] = set()
    stage_known = ram_known = False
    try:
        stage_resident = residency_plan.resident_movers(queue, plan, tier_id)
        stage_known = True
        if ram_tier_id is not None:
            ram_resident = residency_plan.resident_movers(
                queue, plan, ram_tier_id)
            ram_known = True
    except (OSError, ValueError) as exc:
        notes.append(f"starvation plan {prefix} residency: {exc}")
        unreadable.append(f"starvation plan {prefix} residency: {exc}")

    def _resident(mover: str, *, ram: bool = False) -> bool | None:
        """Published residency for one key, or ``None`` when it is unknown."""

        if not (ram_known if ram else stage_known):
            return None
        if mover in (ram_resident if ram else stage_resident):
            return True
        return None if (ram_staged if ram else staged).get(mover) is None else False

    names = [str(phase["name"]) for phase in plan["phases"]]
    accepted_index = names.index(accepted) if accepted in names else None
    phases: list[dict] = []
    for index, phase in enumerate(plan["phases"]):
        stage: dict | None = None
        stage_chunks: list[dict] | None = None
        stage_fragment = False
        if "mover_row" in phase:
            mover_key = str(phase["mover_row"]["action_key"])
            stage = {"mover_action_key_prefix": mover_key[:12],
                     "published": observed_publication(mover_key, staged),
                     "staged": _resident(mover_key),
                     "reserved": staged.get(mover_key),
                     "fragment": fragment_present(mover_key)}
            stage_fragment = fragment_present(mover_key)
        elif isinstance(phase.get("stage_chunks"), list):
            # A chunked phase reports per chunk, beside the phase's own
            # ``stage`` staying ``None``: the census shape for whole-phase
            # legs is unchanged, and a chunked phase is never mistaken for
            # one with no leg at all.
            stage_chunks = []
            assert isinstance(phase["stage_chunks"], list)
            for chunk in phase["stage_chunks"]:
                chunk_key = str(chunk["mover_row"]["action_key"])  # type: ignore[index]
                chunk_fragment = fragment_present(chunk_key)
                stage_fragment = (True if stage_fragment is True or chunk_fragment is True else
                                  None if stage_fragment is None or chunk_fragment is None else False)
                stage_chunks.append({
                    "chunk_index": chunk.get("chunk_index"),  # type: ignore[union-attr]
                    "mover_action_key_prefix": chunk_key[:12],
                    "published": observed_publication(chunk_key, staged),
                    "staged": _resident(chunk_key),
                    "reserved": staged.get(chunk_key),
                    "fragment": chunk_fragment,
                    "fragment_tier_id": fragment_movers.get(chunk_key)})
        ram: dict | None = None
        ram_chunks: list[dict] | None = None
        if "ram_mover_row" in phase:
            ram_key = str(phase["ram_mover_row"]["action_key"])
            ram = {"mover_action_key_prefix": ram_key[:12],
                   "published": observed_publication(ram_key, ram_staged),
                   "staged": _resident(ram_key, ram=True),
                   "reserved": ram_staged.get(ram_key),
                   # The question the ram tier exists to answer: which phases
                   # have promotion fragments and which have none.
                   "fragment": fragment_present(ram_key),
                   "fragment_tier_id": fragment_movers.get(ram_key)}
        elif isinstance(phase.get("ram_chunks"), list):
            # A chunked phase reports per chunk, beside the phase's own
            # ``ram`` staying ``None``: the census shape for whole-phase
            # legs is unchanged, and a chunked phase is never mistaken for
            # one with no ram leg at all.
            ram_chunks = []
            assert isinstance(phase["ram_chunks"], list)
            for chunk in phase["ram_chunks"]:
                chunk_key = str(chunk["ram_mover_row"]["action_key"])  # type: ignore[index]
                ram_chunks.append({
                    "chunk_index": chunk.get("chunk_index"),  # type: ignore[union-attr]
                    "mover_action_key_prefix": chunk_key[:12],
                    "published": observed_publication(chunk_key, ram_staged),
                    "staged": _resident(chunk_key, ram=True),
                    "reserved": ram_staged.get(chunk_key),
                    "fragment": fragment_present(chunk_key),
                    "fragment_tier_id": fragment_movers.get(chunk_key)})
        phases.append({"name": str(phase["name"]),
                       "start_bytes": phase["start_bytes"],
                       "end_bytes": phase["end_bytes"],
                       "stage": stage, "ram": ram,
                       "stage_chunks": stage_chunks,
                       "ram_chunks": ram_chunks,
                       "stage_fragment": stage_fragment})
    # A retired window (#708) is a stall no cycle ends: nothing republishes
    # its movers until an operator resubmits the consumer.  When a mover's
    # terminal refusal retired it (#966, #1004) the marker names why, the
    # path and both owners, and so does this entry (#1004 item 3).
    try:
        superseded = residency_plan.supersession_summary(
            residency_plan.superseded(queue, plan))
    except (OSError, ValueError, pool.PoolContractError) as exc:
        notes.append(f"starvation plan {prefix} supersession: {exc}")
        unreadable.append(f"starvation plan {prefix} supersession: {exc}")
        superseded = {"unreadable": True, "error": str(exc)}
    if isinstance(superseded, Mapping) and superseded.get("unreadable"):
        notes.append(f"starvation plan {prefix} supersession: "
                     f"{superseded.get('error')}")
        unreadable.append(f"starvation plan {prefix} supersession: "
                          f"{superseded.get('error')}")
    entry = {"consumer_action_key_prefix": prefix, "valid": True,
             "tier_id": tier_id, "ram_tier_id": ram_tier_id,
             "state": state, "superseded": superseded,
             "accepted_phase": accepted,
             "accepted_index": accepted_index, "phases": phases,
             "cursor_gap": _starvation_cursor_gap(
                 names, accepted_index, phases, now=now)}
    return entry

def _starvation_chunked_staged(chunks: object):
    """Fold a chunked leg's per-chunk readiness into one tri-state.

    A chunked phase is staged only when every chunk is: a partially moved
    phase still needs room on the tier, and counting it staged would tell
    the census the frontier is further than the bytes prove.  One chunk
    known missing makes the phase known-unstaged, because that hole is a
    fact whatever the others say.  Otherwise -- no hole, but a chunk whose
    evidence could not be read -- the phase is UNKNOWN, and folding that to
    ``False`` here would assert the hole the census could not see (#759).
    """

    if not isinstance(chunks, list) or not chunks:
        return None
    states = [chunk.get("staged") for chunk in chunks
              if isinstance(chunk, dict)]
    if states and all(state is True for state in states):
        return True
    if any(state is False for state in states):
        return False
    return None

def _starvation_leg_present(phase: dict, leg: str) -> bool:
    """Whether this phase has that leg at all, chunked or whole.

    Absence is not readiness and not its negation: a phase with no ram leg
    has nothing to be unstaged about, so it is counted on neither side.
    """

    if leg == "stage":
        return phase["stage"] is not None or phase.get("stage_chunks") is not None
    return phase["ram"] is not None or phase.get("ram_chunks") is not None

def _starvation_leg_staged(phase: dict, leg: str):
    """Whether one census phase counts as staged on one leg.

    Three answers, and they stay three: ``True`` staged, ``False`` known
    not staged, ``None`` either no such leg or evidence that could not be
    read.  Callers must test ``is True`` and ``is False`` separately --
    ``is not True`` merges the last two and republishes an unknown as a
    fact, which is the #759 error one level up from the leg it fixed.
    """

    if leg == "stage":
        if phase["stage"] is not None:
            return phase["stage"].get("staged")
        return _starvation_chunked_staged(phase.get("stage_chunks"))
    if phase["ram"] is not None:
        return phase["ram"].get("staged")
    return _starvation_chunked_staged(phase.get("ram_chunks"))

def _starvation_span(states: list, wanted: object) -> int:
    """The bytes of every phase whose leg is in exactly ``wanted`` state."""

    return sum(int(phase["end_bytes"]) - int(phase["start_bytes"])
               for phase, state in states if state is wanted)

def _starvation_cursor_gap(names: list[str], accepted_index: int | None,
                           phases: list[dict], *, now: float) -> dict:
    """Where the read cursor stands against the promoted frontier, per leg.

    The read cursor is the accepted phase -- the last phase the consumer's
    progress record vouches for -- and the frontier is what the ledgers say
    is staged.  Both are phases, never bytes: the consumer's live byte offset
    inside the phase it is reading is not recorded anywhere, which is gap
    entry ``reader_live_cursor`` rather than a number invented here.
    """

    gap: dict[str, object] = {"accepted_phase": names[accepted_index]
                              if accepted_index is not None else None,
                              "live_byte_cursor": NOT_OBSERVABLE}
    for leg in ("stage", "ram"):
        remaining = [phase for index, phase in enumerate(phases)
                     if (accepted_index is None or index >= accepted_index)
                     and _starvation_leg_present(phase, leg)]
        states = [(phase, _starvation_leg_staged(phase, leg))
                  for phase in remaining]
        # ``unstaged`` counts ``False`` and only ``False``.  The previous
        # spelling was ``is not True``, which swept every unknown into a
        # count of bytes an operator reads as definitely missing -- the leg
        # reported honestly and the aggregate undid it (#759).
        gap[leg] = {
            "remaining_phases": len(remaining),
            "staged_phases": sum(1 for _, state in states if state is True),
            "unstaged_phases": [phase["name"] for phase, state in states
                                if state is False],
            "unstaged_bytes": _starvation_span(states, False),
            # Additive with the two above: staged + unstaged + unknown is
            # every remaining phase that has this leg.
            "unknown_phases": [phase["name"] for phase, state in states
                               if state is None],
            "unknown_bytes": _starvation_span(states, None),
        }
    return gap

def _starvation_tiers(queue: pool.PoolQueue, *, now: float,
                      notes: list[str], unreadable: list[str], reader=None) -> list[dict]:
    """What each tier currently offers and holds: announcement plus ledger."""

    try:
        announced = {str(record.get("tier_id")): record
                     for record in queue.tiers()
                     if isinstance(record, dict)}
    except (OSError, ValueError, pool.PoolContractError) as exc:
        notes.append(f"starvation tiers: {exc}")
        unreadable.append(f"starvation tiers: {exc}")
        announced = {}
    try:
        tier_ids = _tier_ids(reader, queue)
    except OSError as exc:
        notes.append(f"starvation tier ledgers: {exc}")
        unreadable.append(f"starvation tier ledgers: {exc}")
        tier_ids = sorted(announced)
    tiers: list[dict] = []
    for tier_id in sorted(set(tier_ids) | set(announced)):
        record = announced.get(tier_id)
        supply = (record.get("fill_supply") if isinstance(record, Mapping) else None)
        supply = dict(supply) if isinstance(supply, Mapping) else None
        sampled = (record.get("sampled_unix") if isinstance(record, Mapping) else None)
        # RAM diagnostics are the tier host's persisted sample, not this
        # status reader's sysfs or the liveness announcement's freshness.
        ram_record = (record if isinstance(record, Mapping)
                      and record.get("tier") == "ram" else {})
        try:
            ledger = queue.tier(tier_id)
            capacity, available = ledger.mint_supply(), ledger.free_supply()
        except (OSError, ValueError, pool.PoolContractError) as exc:
            notes.append(f"starvation tier ledger {tier_id}: {exc}")
            unreadable.append(f"starvation tier ledger {tier_id}: {exc}")
            capacity, available = None, None
        held: dict[str, int] | None = None
        if isinstance(capacity, dict) and isinstance(available, dict):
            held = {kind: int(capacity.get(kind, 0)) - int(available.get(kind, 0))
                    for kind in sorted(set(capacity) | set(available))}
        # What the tier loop's admission commitment saw on its last cycle
        # (#930), read as filed: the loop's own verdict, never re-priced
        # here.  None where no loop files one -- not a stage tier, or a loop
        # from before #930.
        try:
            commitment = queue.tier_commitment(tier_id)
        except (OSError, ValueError, pool.PoolContractError) as exc:
            notes.append(f"starvation tier commitment {tier_id}: {exc}")
            unreadable.append(f"starvation tier commitment {tier_id}: {exc}")
            commitment = None
        filed = (commitment.get("filed_unix") if isinstance(commitment, Mapping)
                 else None)
        tiers.append({
            "tier_id": tier_id,
            "announced": record is not None,
            # Identity off the announcement, read never derived: kind groups
            # ram/stage/arc records for one box, host names the box, epoch
            # dates the ram mount, and window_gib is the promotion window the
            # tier was announced with. Each is None where the tier announced
            # none, which is how an unannounced ledger row differs from a
            # tier that measured zero.
            "tier_kind": (record.get("tier") if isinstance(record, Mapping)
                          else None),
            "host": (record.get("host") if isinstance(record, Mapping)
                     else None),
            "epoch": (record.get("epoch") if isinstance(record, Mapping)
                      else None),
            "window_gib": (record.get("window_gib")
                           if isinstance(record, Mapping) else None),
            "memory_nodes": ram_record.get("memory_nodes"),
            "node_memfree_bytes": ram_record.get("node_memfree_bytes"),
            "node_shmem_bytes": ram_record.get("node_shmem_bytes"),
            # The tier loop's latest fold, read, not recomputed: best is the
            # highest pool delivery since the last ceiling, ceiling the most
            # recent measured shortfall, may_grow whether another reader fits.
            "fill_source": record.get("fill_source") if isinstance(record, Mapping) else None,
            "fill_supply": supply,
            "announced_fill_mb_s": (
                record.get(storage_tiers.FILL_RECORD_FIELD)
                if isinstance(record, Mapping) else None),
            "capacity_bytes": record.get("capacity_bytes") if isinstance(record, Mapping) else None,
            "sampled_age_s": (now - float(sampled)
                              if isinstance(sampled, (int, float)) else None),
            "ledger_capacity": capacity,
            "ledger_available": available,
            "ledger_held": held,
            "commitment": commitment,
            "commitment_age_s": (now - float(filed)
                                 if isinstance(filed, (int, float))
                                 and not isinstance(filed, bool) else None),
        })
    return tiers

def _starvation_commitment_waits(tiers: Sequence[Mapping[str, object]]) -> list[dict]:
    """Every newcomer waiting on a stage tier's joint commitment (#930).

    Flattened from each tier's filed commitment record: a newcomer the
    commitment refused, one whose tier's census did not read, and one
    waiting behind either.  Each carries its footprint, the committed
    total, the capacity and the terms that make up the gap -- the holders
    and windows, each marked evictable or not -- as the gate saw them.
    """

    waits: list[dict] = []
    for tier in tiers:
        record = tier.get("commitment")
        if not isinstance(record, Mapping):
            continue
        for entry in record.get("waiting") or ():
            if not isinstance(entry, Mapping):
                continue
            ahead = entry.get("waiting_on")
            ahead_reason = (ahead.get("reason") if isinstance(ahead, Mapping)
                            else None)
            if not (entry.get("reason") == window_credit.REASON_COMMITMENT
                    or ahead_reason == window_credit.REASON_COMMITMENT
                    or "error" in entry):
                continue
            waits.append({"tier_id": tier.get("tier_id"),
                          "age_s": tier.get("commitment_age_s"),
                          "over_committed_gib": record.get("over_committed_gib"),
                          **dict(entry)})
    return waits

def _starvation_denials(queue: pool.PoolQueue, *, notes: list[str], reader=None) -> list[dict]:
    """Top claim-denial reasons per host, and the ones blocking movers.

    The snapshot holds each host's latest verdict per action generation
    (bounded, ungated by time), so this is a census of what hosts last said,
    not a window.  A denial names no residency, so "blocking a mover" is a
    join against the live ready/claimed items carrying a range -- the same
    range test ``movers_claimed_on_tier`` stages on -- and a denial whose key
    has since left the active queue reads as unknown rather than as either.
    """

    denials, denial_notes = _pool_claim_denials(queue, reader=reader)
    notes.extend(denial_notes)
    movers: set[str] = set()
    live: set[str] = set()
    for state in (pool.record_store.READY, pool.record_store.CLAIMED):
        try:
            paths = queue.records.required_slot_paths(state)
        except OSError as exc:
            notes.append(f"starvation mover join {state}: {exc}")
            continue
        for path in paths:
            record = _pool_sidecar(path, reader=reader)
            if not isinstance(record, dict):
                continue
            key = record.get("action_key")
            if key != path.stem:
                continue
            live.add(str(key))
            residency = record.get("residency")
            if isinstance(residency, Mapping) and "range_start_bytes" in residency:
                movers.add(str(key))
    by_host: dict[str, dict] = {}
    for denial in denials:
        host = str(denial.get("host"))
        reason = str(denial.get("reason"))
        key = str(denial.get("action_key"))
        slot = by_host.setdefault(host, {"reasons": {}, "mover_reasons": {},
                                         "unknown_keys": 0})
        slot["reasons"][reason] = slot["reasons"].get(reason, 0) + 1
        if key in movers:
            slot["mover_reasons"][reason] = slot["mover_reasons"].get(reason, 0) + 1
        elif key not in live:
            slot["unknown_keys"] += 1
    top: list[dict] = []
    for host in sorted(by_host):
        slot = by_host[host]
        top.append({
            "host": host,
            "denials": sum(slot["reasons"].values()),
            "top_reasons": [{"reason": reason, "count": count} for reason, count in
                            sorted(slot["reasons"].items(),
                                   key=lambda item: (-item[1], item[0]))],
            "mover_blocked": sum(slot["mover_reasons"].values()),
            "mover_blocked_reasons": [
                {"reason": reason, "count": count} for reason, count in
                sorted(slot["mover_reasons"].items(),
                       key=lambda item: (-item[1], item[0]))],
            "unknown_keys": slot["unknown_keys"],
        })
    return top

def _starvation_starved(jobs: Sequence[Mapping[str, object]]) -> list[dict]:
    """Ready items a box refused and will not withhold for (#924).

    A starved item withholds its box only while the holders in its way will
    drain soon.  When one of them will not -- a progress-governed campaign
    holder with no total bound, a bounded one already past the pool's
    transient line, one past its own declared end (#939), load the pool does
    not own, or a veto that work ahead of
    it kept refilling -- the item stops holding the box shut, and this is
    where it says so instead of going quiet.  Each row is the host's latest
    verdict for this exact submission, with the reason and the holders it
    named.
    """

    starved: list[dict] = []
    for job in jobs:
        if job.get("state") != "READY":
            continue
        for denial in job.get("admission_denials") or ():
            reason = str(denial.get("reason", ""))
            if not reason.endswith(("_starved", "_past_ceiling")):
                continue
            evidence = denial.get("evidence")
            detail = evidence.get("starved") if isinstance(evidence, Mapping) else None
            starved.append({
                "action_key": job.get("action_key"),
                "action_key_prefix": job.get("action_key_prefix"),
                "host": denial.get("host"),
                "reason": reason,
                "why": detail.get("why") if isinstance(detail, Mapping) else None,
                "decision_reason": denial.get("decision_reason"),
                "admission_passes": job.get("admission_passes"),
                "admission_wait_s": job.get("admission_wait_s"),
                "denial_age_s": denial.get("age_s"),
                "holders": (detail.get("holders")
                            if isinstance(detail, Mapping) else None),
            })
    return starved

def _starvation_dependents(queue: pool.PoolQueue, *, notes: list[str],
                           unreadable: list[str], now: float) -> list[dict]:
    """A starved producer in one place (#990): what each claimed action waits on.

    For every claimed action that owns rows -- a residency plan's movers,
    egresses and RAM promotions, or produced-output exports naming it in
    ``dependent_of`` -- the same record a kill's ending carries: the rows,
    their state, how long they have been ready, each one's latest denial and
    reason ring, and the tier loops' newest verdicts about the action.  One
    census of ``ready/`` and ``claimed/`` for all of them.
    """

    live, listed = queue.records.advisory_live_rows()
    for error in listed:
        notes.append(f"starvation dependents: {error}")
        unreadable.append(f"starvation dependents: {error}")
    owners = {str(record.get("dependent_of")) for _state, record in live.values()
              if isinstance(record.get("dependent_of"), str)}
    out: list[dict] = []
    for key, (state, record) in sorted(live.items()):
        if state != pool.record_store.CLAIMED:
            continue
        residency = record.get("residency")
        planned = isinstance(residency, Mapping) and bool(residency.get("leads"))
        if key not in owners and not planned:
            continue
        try:
            found = queue.dependent_rows(key, now=now, live_records=(live, []),
                                         include_local=False)
        except (OSError, ValueError, pool.PoolContractError) as exc:
            notes.append(f"starvation dependents {key[:12]}: {exc}")
            unreadable.append(f"starvation dependents {key[:12]}: {exc}")
            continue
        events = queue.consumer_events(key)
        out.append({"action_key": key, "action_key_prefix": key[:12],
                    "node": record.get("claimed_host"),
                    **found,
                    "tier_events": events[-pool.MAX_ENDING_EVENTS:],
                    "tier_events_total": len(events)})
    return out

def _starvation_host_events(queue: pool.PoolQueue, *, notes: list[str],
                            unreadable: list[str]) -> list[dict]:
    """The newest tier-loop verdict every host filed about itself (#1006).

    The ARC ``primarycache`` refusal, a ram-admission refusal, a ram epoch
    change, and any tier-level verdict whose tier planned no consumer this
    cycle name nobody's plan to be filed under, so ``_emit`` writes them to
    ``residency-events/_host/<host>.jsonl`` instead of losing them to that
    host's own stdout.  One row per host that has ever filed one, newest
    verdict first read.
    """

    out: list[dict] = []
    try:
        hosts = queue.records.host_event_writers()
    except FileNotFoundError:
        return out
    except OSError as exc:
        notes.append(f"starvation host events: {exc}")
        unreadable.append(f"starvation host events: {exc}")
        return out
    for host in hosts:
        events = queue.host_events(host)
        if not events:
            continue
        out.append({"host": host, "newest": events[-1],
                    "events_total": len(events)})
    return out

def _starvation_gaps() -> list[dict]:
    """What this blob cannot derive, and what would have to be recorded.

    The gap list is a deliverable for the MCP observability design (#660):
    each entry names the field that reads ``"not_observable"``, why no
    existing record carries it, and the record that would.
    """

    return [
        {"field": "waiting_claims[].cpu_growth",
         "why_not_observable": "one census carries one cpu_seconds sample of "
            "the payload scope; growth is a difference of two samples of the "
            "same scope nonce, which a single snapshot cannot take",
         "would_need": "consecutive snapshots differenced per scope nonce "
            "(what pbmetrics recent-cores does across scrapes), or the scope "
            "sampler publishing a short-window rate beside the lifetime counter"},
        {"field": "residency_plans[].cursor_gap.live_byte_cursor",
         "why_not_observable": "progress records name the last accepted phase, "
            "never the consumer's byte offset inside the phase it is reading",
         "would_need": "the consumer reporting read_bytes beside accepted_phase "
            "on its progress record, in the manifest's read order"},
        {"field": "residency_plans[] accepted_phase for ready consumers",
         "why_not_observable": "a ready consumer has no lease, so no accepted "
            "observation exists until it is claimed; its window position reads "
            "as the beginning",
         "would_need": "the coordinator publishing the last accepted phase it "
            "saw for a ready consumer, or the progress channel pre-claim"},
        {"field": "denial history for finished movers",
         "why_not_observable": "each live key's reason transitions are kept "
            "in denial-transitions/ (#991) and a live consumer's tier-loop "
            "verdicts in residency-events/ (#990), but a finished mover's ring "
            "is retired with it; only a kill's ending record keeps the rings "
            "of the rows it was waiting on",
         "would_need": "a bounded terminal denial archive beside done/"},
        {"field": "tiers[] live disk delivery",
         "why_not_observable": "the announced fill is the last cycle's learned "
            "rate from mover receipts; instantaneous pool delivery between "
            "cycles is sampled by no record",
         "would_need": "the tier loop publishing its per-cycle disk pacing "
            "sample beside the announced record"},
    ]

def _starvation_census_unreadable(queue: pool.PoolQueue, *, notes: list[str],
                                  unreadable: list[str]) -> list[dict]:
    """Every ledger whose last retire could not read a holder (#936).

    A holder that stays unreadable refuses every mint on its ledger and
    bounds every shrink by the mint markers instead of the truth, and a
    claim the lower-bound total cannot seat is refused as
    ``tier_census_unreadable``.  The ledger's own report names the holder,
    its errno, the kinds asked and retired and how many consecutive
    censuses it has lasted, so the refusal is attributed here rather than
    read as an empty ledger.  Host ledgers and tier ledgers both file one;
    an exact census removes it.
    """

    report_paths: list[tuple[str, str, Path]] = []
    try:
        hosts = queue.records.required_directory_names_at(queue.root / pool.RESERVATIONS)
    except FileNotFoundError:
        hosts = []
    except OSError as exc:
        notes.append(f"starvation host ledgers: {exc}")
        unreadable.append(f"starvation host ledgers: {exc}")
        hosts = []
    report_paths.extend(("host", host, queue.root / pool.RESERVATIONS / host /
                         pool.CENSUS_UNREADABLE) for host in hosts)
    try:
        report_paths.extend(("tier", tier_id, queue.root / pool.record_store.TIER_RESERVATIONS /
                             queue.records.check_tier_id(tier_id) / pool.CENSUS_UNREADABLE)
                            for tier_id in tier_holdings.ids(queue.records))
    except (OSError, ValueError, pool.PoolContractError) as exc:
        notes.append(f"starvation tier census reports: {exc}")
        unreadable.append(f"starvation tier census reports: {exc}")
    reports: list[dict] = []
    for kind, name, report_path in report_paths:
        try:
            report = pool.record_store.read_document(report_path)
        except (OSError, pool.PoolContractError) as exc:
            notes.append(f"starvation {kind} ledger {name} census report: {exc}")
            unreadable.append(f"starvation {kind} ledger {name} census report: {exc}")
            continue
        if report is None:
            continue
        holders = report.get("unreadable")
        holders = [dict(entry) for entry in holders
                   if isinstance(entry, Mapping)] if isinstance(holders, list) else []
        reports.append({"ledger_kind": kind, "ledger_id": name, **report,
                        "unreadable": holders})
        named = ", ".join(f"{entry.get('holder')} errno {entry.get('errno')}"
                          for entry in holders)
        notes.append(
            f"{kind} ledger {name}: {len(holders)} unreadable holder(s) for "
            f"{report.get('cycles')} census cycle(s); mints refused, last "
            f"retire {report.get('retired')} ({named})")
    return reports

def blocked_origin_remedies(ref: Mapping[str, object], key: str) -> dict[str, str]:
    """The two commands that let one blocked consumer's batch go (#926).

    Computed here, at display time, from the batch's ref and the consumer's
    key; the tick's own report carries neither.  ``release`` is exact.
    ``resubmit`` names what only the submitter knows as placeholders: the
    consumer's own options and command, resubmitted with ``--supersedes``.
    """

    batch = shlex.quote(pb.compact_diagnostic_json(dict(ref)))
    return {
        "release": (f"pbrun.py --release-origin-consumer {batch} {key} "
                    "--reason '<why>'"),
        "resubmit": (f"pbrun.py --priority -10 --supersedes {key} "
                     "<the consumer's options> -- <its command>"),
    }

@read_cache.file_version_scope()
def blocked_origins(queue_root: str | Path) -> Section[dict]:
    """List the consumed origin batches that only an operator can free (#926).

    A batch is blocked when every declared consumer still holding it failed
    or was withdrawn (`produced_output.blocked_origin_batches`, which reads
    the same records the retirement tick does and writes nothing).  Each
    holding consumer is listed with its state, any ``superseded_by`` that did
    not apply, and the remedies.  ``complete`` is false when a record could
    not be read; the unreadable ones are named.

    ``orphaned_prewrites`` lists the prewrites, of any template (#1053),
    whose attempt ended before committing and whose files belong to no batch
    (#949), each with the remedy.

    ``held_by_unknown`` lists each consumed batch or ended prewrite that the
    tier cycle would free now but for another attempt, whose state cannot be
    read, that has prewritten one of its paths (#1065): the holders, why
    each is unknown, whether it is orphaned (no queue row past the lease
    timeout) and, for an orphaned one, the remedy.
    """

    queue = pool.PoolQueue(queue_root)
    found = produced_output.blocked_origin_batches(queue)
    blocked = []
    for batch in found["blocked"]:
        holding = set(batch["holding"])
        blocked.append({
            **batch,
            "remedies": [
                {"action_key": item["action_key"], "state": item["state"],
                 **({"superseded_by": item["superseded_by"]}
                    if "superseded_by" in item else {}),
                 **blocked_origin_remedies(batch["ref"], str(item["action_key"]))}
                for item in batch["consumers"]
                if item["action_key"] in holding],
        })
    orphaned = [{**item, "remedy": ORPHANED_PREWRITE_REMEDY}
                for item in found["orphaned_prewrites"]]
    value = {"schema": BLOCKED_ORIGINS_SCHEMA_V1, "blocked": blocked,
            "orphaned_prewrites": orphaned,
            "held_by_unknown": list(found["held_by_unknown"]),
            "unreadable": list(found["unreadable"]),
            "complete": not found["unreadable"]}
    return _section(value, inputs={"queue_root": str(queue.root), "source": "blocked_origin_batches"},
                    errors=found["unreadable"])


def starvation(queue_root: str | Path, *, now: float | None = None, reader=None) -> dict:
    """Answer "what is waiting on data" as one JSON-serializable blob.

    A non-atomic diagnostic census over the pool queue root, reusing
    :func:`action_rows` for the queue walk: claims and leases for quiet
    consumers, plans and fragments for promotion state, tier announcements
    and ledgers for fill and occupancy, denial snapshots for who blocks
    movers.  Nothing here writes, nothing admits, nothing schedules.

    Args:
        queue_root: The queue root holding ready/claimed state and the
            residency, tier and reservation records beside it.
        now: The clock to age samples against; ``time.time()`` when omitted.

    Returns:
        The starvation blob: waiting claims, residency plans with cursor
        gaps, tiers, per-host denial tops, the ``not_observable`` gap list,
        and the notes.  ``complete`` is False when any input was unreadable.
    """

    moment = time.time() if now is None else float(now)
    queue = pool.PoolQueue(Path(queue_root).absolute())
    census = action_rows(queue_root, reader=reader).value
    notes = list(census.get("notes", []))
    unreadable = list(census.get("queue", {}).get("unreadable", []))
    jobs = census.get("jobs", [])
    ready = {str(job["action_key"]) for job in jobs if job.get("state") == "READY"}
    claimed = {str(job["action_key"]) for job in jobs if job.get("state") == "CLAIMED"}
    wait_section = waits(queue_root, jobs=jobs, reader=reader,
                         consumers_complete=census["queue"].get("claimed") is not None)
    waiting = wait_section.value["waiting_claims"]
    notes.extend(wait_section.value["notes"])
    unreadable.extend(wait_section.value["unreadable"])
    plan_section = plans(queue_root, view="starvation", now=moment, reader=reader,
                         ready=ready, claimed=claimed,
                         active_complete=all(census["queue"].get(state) is not None
                                             for state in ("ready", "claimed")))
    plan_rows = plan_section.value["residency_plans"]
    notes.extend(plan_section.value["notes"])
    unreadable.extend(plan_section.value["unreadable"])
    tier_section = tiers(queue_root, view="diagnostics", now=moment, reader=reader)
    tier_rows = tier_section.value["tiers"]
    notes.extend(tier_section.value["notes"])
    unreadable.extend(tier_section.value["unreadable"])
    commitment_waits = _starvation_commitment_waits(tier_rows)
    denial_section = denials(queue_root, profile="latest", reader=reader)
    denial_top = denial_section.value
    notes.extend(denial_section.errors)
    starved = _starvation_starved(jobs)
    for entry in starved:
        # The sequence, not only the latest verdict (#991).
        entry["denial_transitions"] = (
            queue.denial_transitions(str(entry["action_key"]))
            if isinstance(entry.get("action_key"), str) else [])
    dependents = _starvation_dependents(queue, notes=notes, unreadable=unreadable,
                                        now=moment)
    host_events = _starvation_host_events(queue, notes=notes, unreadable=unreadable)
    census_unreadable = _starvation_census_unreadable(
        queue, notes=notes, unreadable=unreadable)
    return {"schema": STARVATION_SCHEMA_V1,
            "queue_root": str(queue.root),
            "sampled_unix": moment,
            "complete": bool(census.get("queue", {}).get("complete", False))
                        and not unreadable,
            "notes": notes,
            "waiting_claims": waiting,
            "residency_plans": plan_rows,
            "tiers": tier_rows,
            "joint_commitment_waits": commitment_waits,
            "denial_top": denial_top,
            "starved": starved,
            "claimed_dependents": dependents,
            "host_events": host_events,
            "census_unreadable": census_unreadable,
            "not_observable": _starvation_gaps()}

def waits(queue_root, *, view="claims", jobs=None, consumers=None, plan_files=None,
          reader=None, census_reader=None, consumers_complete: bool = True,
          plans_complete: bool = True) -> Section[dict]:
    queue = pool.PoolQueue(Path(queue_root).absolute())
    if view == "promotion":
        return _promotion_waits(queue, consumers, plan_files, reader=reader, census_reader=census_reader,
                                consumers_complete=consumers_complete, plans_complete=plans_complete)
    if view != "claims":
        raise ValueError(f"unknown wait census view: {view}")
    if jobs is None:
        active = action_rows(queue_root, reader=reader).value
        jobs = active["jobs"]
        consumers_complete = consumers_complete and active["queue"].get("claimed") is not None
    notes, unreadable = [], []
    waiting: list[dict] = []
    for job in jobs:
        if job.get("state") != "CLAIMED":
            continue
        key = str(job.get("action_key"))
        claim = _pool_sidecar(
            queue.records.item_path(pool.record_store.CLAIMED, key),
            reader=reader,
        )
        if isinstance(claim, Exception) or not isinstance(claim, dict):
            notes.append(f"starvation claim {key[:12]}: {claim}")
            unreadable.append(f"starvation claim {key[:12]}: {claim}")
            continue
        residency = claim.get("residency")
        if not isinstance(residency, Mapping) or not residency.get("leads"):
            continue  # Not a consumer: nothing to wait on.
        lease = _pool_sidecar(queue.records.lease_path(key), reader=reader)
        if isinstance(lease, Exception):
            notes.append(f"starvation lease {key[:12]}: {lease}")
            unreadable.append(f"starvation lease {key[:12]}: {lease}")
            lease = None
        quiet, grace = _starvation_quiet(lease)
        child = _starvation_child(lease)
        is_waiting, rule = _starvation_waiting(quiet, grace, child)
        observed = (lease.get("progress_observation") if isinstance(lease, Mapping)
                    else None)
        phase = (observed.get("phase") if isinstance(observed, Mapping)
                 and isinstance(observed.get("phase"), str) else None)
        waiting.append({
            "action_key_prefix": key[:12],
            "node": claim.get("claimed_host"),
            "phase": phase,
            "accepted_phase": _starvation_accepted_phase(lease),
            "quiet_s": quiet,
            "grace_s": grace,
            "quiet_fraction": (quiet / grace if quiet is not None and grace else None),
            "child": child,
            # Single-sample growth is gap entry cpu_growth, not a number here.
            "cpu_growth": NOT_OBSERVABLE,
            "waiting_on_data": is_waiting,
            "waiting_rule": rule,
        })
    return _section({"waiting_claims": waiting, "notes": notes, "unreadable": unreadable},
                    inputs={"queue_root": str(queue.root), "claims": tuple(
                        str(job.get("action_key")) for job in jobs if job.get("state") == "CLAIMED"),
                        "consumers_complete": consumers_complete},
                    errors=unreadable, complete=consumers_complete)


def plans(queue_root, *, view="cursors", now=None, reader=None, ready=None, claimed=None,
          listing=None, listing_error=None, active=None, announced=None, census_reader=None,
          active_complete: bool = True) -> Section[dict]:
    moment = time.time() if now is None else now
    queue = pool.PoolQueue(Path(queue_root).absolute())
    inputs = {"queue_root": str(queue.root), "view": view, "sampled_unix": moment}
    if view == "promotion":
        return _promotion_plans(queue, listing, listing_error, active, announced,
                                reader=reader, census_reader=census_reader, now=moment)
    if view == "cursors":
        value = _cursor_census(queue.root, now=moment)
        return _section(value, inputs=inputs, errors=value["unreadable"])
    if view != "starvation":
        raise ValueError(f"unknown plan census view: {view}")
    if ready is None or claimed is None:
        captured = action_rows(queue_root, reader=reader).value
        if ready is None:
            ready = {str(job["action_key"]) for job in captured["jobs"] if job.get("state") == "READY"}
        if claimed is None:
            claimed = {str(job["action_key"]) for job in captured["jobs"] if job.get("state") == "CLAIMED"}
        active_complete = active_complete and all(captured["queue"].get(state) is not None
                                                   for state in ("ready", "claimed"))
    notes, unreadable = [], []
    plans: list[dict] = []
    try:
        plan_paths = residency_plan.inspection_paths(queue)
    except FileNotFoundError:
        plan_paths = []  # No staged consumer has ever filed a plan.
    except OSError as exc:
        notes.append(f"starvation residency plans: {exc}")
        unreadable.append(f"starvation residency plans: {exc}")
        plan_paths = []
    for path in plan_paths:
        raw = _pool_sidecar(path, reader=reader)
        if isinstance(raw, Exception):
            notes.append(f"starvation plan {path.stem[:12]}: {raw}")
            unreadable.append(f"starvation plan {path.stem[:12]}: {raw}")
            plans.append({"consumer_action_key_prefix": path.stem[:12],
                          "valid": False, "note": str(raw)})
            continue
        plans.append(_starvation_plan_entry(
            queue, path.stem, raw, ready=ready, claimed=claimed,
            notes=notes, unreadable=unreadable, now=moment, reader=reader,
            active_complete=active_complete))
    return _section({"residency_plans": plans, "notes": notes, "unreadable": unreadable},
                    inputs={**inputs, "ready": tuple(sorted(ready)), "claimed": tuple(sorted(claimed)),
                            "active_complete": active_complete},
                    errors=unreadable, complete=active_complete)


def recent_ending_paths(queue_root: str | Path, limit: int, *,
                        states: Sequence[str] | None = None) -> list[os.DirEntry]:
    """Read-only terminal-entry selection shared by structured status readers.

    Returns the newest entries, including durable withdrawal decisions, without
    expanding record payloads. Filesystem errors propagate; callers must bound
    the shared-mount read.
    """
    return pool.record_store.RecordStore(queue_root).recent_ending_paths(limit, states=states)

def _transport(record: Mapping[str, object]) -> str:
    """Which transport filed one ending, read from the schema it was filed under.

    The two writers use distinct schema ids on purpose, so this reads the id
    rather than trusting a ``transport`` field only one of them writes.  An
    unrecognized schema prints verbatim: a reader that renames an unknown
    record after one it does know is how a wrong table becomes a confident one.
    """

    schema = str(record.get("schema") or "")
    if schema == sl.OUTCOME_SCHEMA_V1:
        return "slurm"
    if schema == pool.POOL_OUTCOME_SCHEMA_V1:
        return "pool"
    return schema or UNKNOWN

def _unreadable_row(entry: os.DirEntry, reason: str) -> dict:
    """One record this could not read, kept as a row instead of dropped.

    The row carries the path and the reason because those are the whole of
    what an operator can act on: the key is only what the file is named, and
    every other column is inside the file nobody could read.
    """

    try:
        mtime = entry.stat().st_mtime
    except OSError:
        mtime = 0.0
    return {
        "action_key": (Path(entry.path).parent.name
                       if Path(entry.path).parent.parent.name == "decisions"
                       else entry.name[:-5]),
        "status": "unreadable",
        "transport": UNKNOWN,
        "host": None,
        "elapsed_s": None,
        "returncode": None,
        "action_returncode": None,
        "action_signal": None,
        "receipt_published": None,
        "slurm_state": None,
        **{field: None for field in pool.RESOURCE_SUMMARY_FIELDS},
        "unreadable": reason,
        # The record's own finished time is unreadable too, so the file's
        # modification time is what places the row. It is the same clock
        # ``_ending_paths`` selected the record by.
        "finished_unix": mtime,
        "timing_finished_unix": None,
        "published_unix": None,
        "claimed_unix": None,
        "path": entry.path,
    }

def _unreadable_reason(exc: Exception) -> str:
    """Why a record could not be read, in the terms that pick the fix.

    A record nobody may read and a record nobody can parse are different
    faults: the first is a mode or a mount, the second is a writer that
    stopped. Terminal records are published by rename, so a half-written one
    is a fault rather than a write still in flight.
    """

    if isinstance(exc, pool.record_store.EndingRecordTooLarge):
        return f"record exceeds {MAX_ENDING_RECORD_BYTES}-byte reader limit"
    if isinstance(exc, PermissionError):
        return "permission denied"
    if isinstance(exc, OSError):
        return str(exc.strerror or type(exc).__name__).lower()
    return "not valid JSON"

def endings(queue_root: str | Path, *, limit: int = DEFAULT_RECENT, reader=None) -> list[dict]:
    """Describe how the newest actions ended, under either transport.

    Args:
        queue_root: The queue root holding ``done``, ``failed`` and ``withdrawn``.
        limit: How many records to read, newest by modification time first.

    Returns:
        One dictionary per ending, newest first by the time the record says the
        work finished, falling back to the file's modification time. A record
        that cannot be read is returned as a row with status ``unreadable``,
        naming its path and why, rather than left out.  A completed row also
        says whether it answers how its key ended: ``current`` is true for the
        row the queue's generation resolution selects, false for an older
        generation's retained record (``superseded_by`` names the answer), and
        ``terminal_conflict`` marks a key that carried more than one terminal
        record.  ``current`` is ``None`` on an unreadable row, which answers
        nothing, and on a row whose slot was replaced between its projection
        and the resolution (``current_unresolved`` says so): path equality
        alone is not identity, so the generation must match as well.

    A kept-reads delegate (``pbmetrics``) answers the same selected rows and
    projections from its retained listings but does not add the per-key
    currency marking: that marking resolves the opposite terminal slot,
    which the kept-listing cache deliberately does not re-read.  The gauges
    do not consume the field.
    """

    if reader is not None:
        return reader.endings(Path(queue_root), limit)
    rows = [ending_row(entry, queue_root)
            for entry in recent_ending_paths(queue_root, limit)]
    _annotate_current_endings(rows, queue_root)
    rows.sort(key=lambda row: row["finished_unix"], reverse=True)
    return rows

def _annotate_current_endings(rows: Sequence[dict], queue_root: str | Path) -> None:
    """Mark each selected ending row with the key's current terminal answer.

    ``done/`` and ``failed/`` are one slot each, so a key that was resubmitted
    can hold two generations' records; without this a reader of the endings
    table has two rows and no answer (#1178).  One
    ``PoolQueue.current_ending`` read per distinct key names the row that
    answers and the generations it supersedes.  The older row stays in the
    table -- it is evidence -- and the double-record state is reported rather
    than hidden.  Immutable withdrawal decisions are not the visible terminal
    answer, so their rows carry ``current: None``.
    """

    if not rows:
        return
    queue = pool.PoolQueue(Path(queue_root))
    answers: dict[str, dict] = {}
    for row in rows:
        key = str(row.get("action_key") or "")
        if row.get("unreadable"):
            row["current"] = None
            row["terminal_conflict"] = None
            continue
        if key not in answers:
            try:
                answers[key] = queue.current_ending(key)
            except (OSError, ValueError, pool.PoolContractError) as exc:
                answers[key] = {
                    "state": None, "path": None, "generation": None,
                    "history": [], "ambiguous": True, "coexisting": [],
                    "unreadable": [{"state": None, "path": None,
                                    "reason": repr(exc)}],
                }
        answer = answers[key]
        row["terminal_conflict"] = bool(
            len(answer["coexisting"]) > 1 or answer["unreadable"])
        row["terminal_ambiguous"] = bool(answer["ambiguous"])
        path = Path(str(row.get("path") or ""))
        if path.parent.parent.name == "decisions":
            # An immutable cancellation decision is generation-scoped
            # evidence, not the visible marker a key-level answer reads.
            row["current"] = None
            continue
        if answer["state"] is None:
            # No terminal answers for this key (an unreadable or unorderable
            # pair).  No row may claim to be it.
            row["current"] = None
            continue
        if answer["path"] is not None and Path(str(answer["path"])) == path:
            # The path alone is not identity: the slot can be replaced
            # between this row's projection and the resolution, and then the
            # OLD row would be marked current for a newer record (#1178
            # review, item 1).  The generation must match too.
            if row.get("published_unix") == answer["generation"]:
                row["current"] = True
            else:
                row["current"] = None
                row["current_unresolved"] = (
                    "the record at this path was replaced between its "
                    "projection and the key's resolution")
            continue
        row["current"] = False
        row["superseded_by"] = {
            "state": answer["state"],
            "path": None if answer["path"] is None else str(answer["path"]),
            "generation": answer["generation"],
        }

def ending_row(entry: os.DirEntry, queue_root: str | Path) -> dict:
    """One selected terminal record as :func:`endings` reports it.

    ``entry`` is anything with the ``path``, ``name`` and ``stat()`` of an
    ``os.DirEntry``: ``pbmetrics`` passes the entries it keeps between
    scrapes, so both readers project a record through this one function.
    """

    try:
        record = pool.record_store.RecordStore(queue_root).selected_ending(
            entry, max_bytes=MAX_ENDING_RECORD_BYTES)
    except (OSError, ValueError, pool.PoolContractError) as exc:
        # Not dropped. ``limit`` sliced the newest records before any of
        # them was read, so dropping one printed the same empty table a
        # fleet that had filed nothing prints, and the two states call for
        # opposite responses.
        return _unreadable_row(entry, _unreadable_reason(exc))
    if not isinstance(record, dict):
        return _unreadable_row(entry, "not a JSON object")
    detail = record.get("detail")
    detail = detail if isinstance(detail, dict) else {}
    slurm = detail.get("slurm")
    slurm = slurm if isinstance(slurm, dict) else {}
    try:
        mtime = entry.stat().st_mtime
    except OSError:
        mtime = 0.0
    finished = record.get("finished_unix")
    return {
        "action_key": str(record.get("action_key") or entry.name[:-5]),
        # Which repository this run tested (#1565): the row's own tag,
        # backfilled to explicit unknown for records filed before it.
        "tested_repository": pool.tested_repository_of(record),
        "status": str(record.get("status") or UNKNOWN),
        "transport": _transport(record),
        # The box the action was ON, not the box that filed its ending.
        # ``reap_stale`` stamps itself as ``finished_host`` -- correctly,
        # since the reaper is who filed the record -- and it files most of
        # the fleet's lost claims, so preferring that field named
        # whichever box happened to sweep.  That is how a diagnosis
        # started on the wrong box, and why the bias is not cosmetic: the
        # box that reaps most is the box that runs most, so misattributed
        # failures pile onto the machine that already looks busiest.
        #
        # ``finished_host`` stays as the fallback rather than being
        # dropped, because it is the only box an ending with no claimant
        # names at all -- a cache hit, or a SLURM record filed by the
        # waiter (#227, #262).
        "host": record.get("claimed_host") or record.get("finished_host"),
        "served_from": record.get("served_from"),
        "resident_set": record.get("resident_set"),
        "elapsed_s": detail.get("elapsed_s"),
        "returncode": detail.get("returncode"),
        # The action's own ending, on the records that carry one: the
        # launcher's status above is 1 for every failure.
        "action_returncode": detail.get("action_returncode"),
        "action_signal": detail.get("action_signal"),
        # The pull queue's records carry no receipt field at all, which is
        # a different thing from a receipt that was not published.
        "receipt_published": detail.get("receipt_published"),
        "slurm_state": slurm.get("state"),
        # Why a withdrawal happened, when admission rather than an operator
        # made it: the foreground action this one yielded the box to
        # (#364).  A field rather than prose in ``reason`` so a reader can
        # test it, and present as ``None`` everywhere else for the same
        # reason ``unreadable`` is.
        "preempted_by": record.get("preempted_by"),
        # What the run cost and how loaded its box was (#372 Tier 0).
        # Absent, not zero, on every record filed before it existed.
        **pool.resource_profile_summary(detail),
        # Where a profiled run left its profile (#372 Tier 1).  ``None``
        # on every other row, which is most of them.
        "profile": detail.get("profile"),
        "withdrawn_attempt": record.get("withdrawn_attempt"),
        "resource_scope_cleanup": record.get("resource_scope_cleanup"),
        "container_cleanup": record.get("container_cleanup"),
        # Present on every row so a reader of the JSON can test one field
        # rather than the absence of one.
        "unreadable": None,
        "finished_unix": (
            float(finished)
            if isinstance(finished, (int, float)) else mtime
        ),
        # Keep the record's own value separate from the filesystem-mtime
        # fallback above. The fallback orders and selects status rows; it
        # is not evidence of when execution finished.
        "timing_finished_unix": finished,
        "published_unix": record.get("published_unix"),
        "claimed_unix": record.get("claimed_unix"),
        "path": entry.path,
    }

class KeptReads:
    """What one exporter keeps from one scrape to the next (#1020).

    ``MetricsCache`` holds one for the life of the server, and a one-shot
    collection takes a fresh one, which reads everything exactly as the
    plain census does.  :meth:`begin` starts a scrape and :meth:`end`
    finishes one: a directory's version is read at most once per scrape, a
    directory is read at most once per scrape, and whatever the scrape did
    not touch is dropped at its end, so what is kept is bounded by what is
    on disk now.

    Counters, exported per scrape by :meth:`scrape_counts`: ``listed`` and
    ``kept`` count directories listed and directories answered from a kept
    listing, ``parsed`` the records ``DirectoryRecords`` read, and
    ``computed``/``reused`` the per-file, per-record and per-plan values
    computed from bytes and reused.

    The history directories -- ``done/``, ``failed/``, ``withdrawn/`` and
    ``movers/``, 50,000 names on the live queue and growing -- are kept by
    fixed Store history questions instead of ``DirectoryRecords``: one ``scandir`` and one
    ``stat`` per name when the directory changed, exactly the plain walk's
    cost, so a changed scrape costs no more than a fresh one.
    """

    def __init__(self, *, trust=None) -> None:
        self.trust = kept_reads.filesystem_trust if trust is None else trust
        self.records = kept_reads.DirectoryRecords(trust=self.trust)
        self._histories = kept_reads.DirectoryCaptures(self.trust)
        self._stores: dict[str, pool.record_store.RecordStore] = {}
        self._derived_values: dict[tuple[str, int], object] = {}
        self._at_begin: tuple[int, ...] = (0, 0, 0, 0, 0)
        #: path -> opaque directory evidence and a derived single-file value.
        self._files: dict[str, tuple[kept_reads.DirectoryEvidence, object]] = {}
        #: memo key -> ``(fence, value)`` for derived values.
        self._memos: dict[object, tuple[kept_reads.EvidenceSet, object]] = {}
        self._view = self.trust.scrape_view()
        self._reads: dict[str, tuple[object, object, list]] = {}
        self._named: set[str] = set()
        self._touched: set[object] = set()
        #: Entries whose derived value the previous scrape used, by kind.
        self._derived_now: dict[str, dict[int, pool.record_store.HistoryEntry]] = {}
        self._derived_entries: dict[str, dict[int, pool.record_store.HistoryEntry]] = {}
        self.computed = 0
        self.reused = 0

    # -- scrape boundaries -------------------------------------------------

    def _totals(self) -> tuple[int, ...]:
        return (self.records.listed + sum(store.history_listed for store in self._stores.values()),
                self.records.kept + sum(store.history_kept for store in self._stores.values()),
                self.records.parsed, self.computed, self.reused)

    def scrape_counts(self) -> dict[str, int]:
        """What this scrape has done so far: listed, kept, parsed, computed, reused."""

        return {name: now - then for name, now, then in zip(
            ("listed", "kept", "parsed", "computed", "reused"),
            self._totals(), self._at_begin)}

    def begin(self) -> None:
        self._view = self.trust.scrape_view()
        for store in self._stores.values():
            store.begin_read()
        self._reads = {}
        self._named = set()
        self._touched = set()
        self._derived_now = {}
        self._at_begin = self._totals()

    def end(self) -> None:
        # A directory this scrape did not read -- a withdrawal decision's,
        # once the decision is gone -- is forgotten with its records.
        wanted = set(self._reads) | self._named
        for store in self._stores.values():
            wanted.update(store.history_directories())
            store.end_read()
        self.records.retain(wanted)
        self._histories.retain(wanted)
        for name in [name for name in self._files if name not in self._touched]:
            del self._files[name]
        for key in [key for key in self._memos if key not in self._touched]:
            del self._memos[key]
        # A value derived for an entry the scrape did not use is dropped:
        # the newest-first windows move on, and an entry that has left them
        # must not keep its row for the rest of the fleet's history.
        for kind, previous in self._derived_entries.items():
            current = self._derived_now.get(kind, {})
            for ident, entry in previous.items():
                if ident not in current:
                    self._derived_values.pop((kind, ident), None)
        self._derived_entries = self._derived_now

    def used(self, kind: str, entry: pool.record_store.HistoryEntry) -> None:
        """Record that this scrape used the value for ``entry`` under ``kind``."""

        self._derived_now.setdefault(kind, {})[id(entry)] = entry

    def derive(self, kind: str, entry: pool.record_store.HistoryEntry, compute, *, keep=None):
        """``compute()`` of ``entry``'s bytes, kept by its identity while it lasts.

        The entry is replaced whenever its file's version changes, so a value
        kept under its identity is a value of the bytes now on disk.  ``keep(value)``
        false (a read that failed) is returned and computed again next time.
        """

        self.used(kind, entry)
        cached = self._derived_values.get((kind, id(entry)))
        if cached is not None:
            self.reused += 1
            return cached
        value = compute()
        self.computed += 1
        if keep is None or keep(value):
            self._derived_values[(kind, id(entry))] = value
        return value

    # -- fences ------------------------------------------------------------

    def available(self, directory: Path | str) -> bool:
        return self._view.available(directory)

    def fence(self, directory: Path) -> kept_reads.DirectoryEvidence | None:
        return self.trust.ancestor_evidence(directory)

    def holds(self, fences) -> bool:
        return self._view.holds(fences)

    def still(self, fences) -> bool:
        return self.trust.holds(fences)

    def memo(self, key: object, make_fences, compute, *, keep=None):
        """``compute()``, reused while every fence ``make_fences()`` took holds.

        The fences are taken before ``compute`` runs and checked again after
        it, and only a value with every fence trusted and unmoved across the
        computation is kept -- the protocol ``DirectoryRecords`` keeps for a
        listing.  ``make_fences`` returns ``None`` when no fence can be
        trusted, and the value is then computed every scrape.
        """

        self._touched.add(key)
        kept = self._memos.pop(key, None)
        if kept is not None and self.holds(kept[0]):
            self._memos[key] = kept
            self.reused += 1
            # A fenced directory is unchanged, so what ``DirectoryRecords``
            # keeps for it is still good: it must survive this scrape's end
            # even though nothing read it.
            self._named.update(fence.directory for fence in kept[0])
            return kept[1]
        # Nothing is kept while ``compute`` runs: one that raises leaves no
        # value behind, stale or otherwise.
        fences = make_fences()
        value = compute()
        self.computed += 1
        if (fences is not None and self.still(fences)
                and (keep is None or keep(value))):
            self._memos[key] = (fences, value)
        else:
            self._memos.pop(key, None)
        return value

    def fences(self, directories) -> kept_reads.EvidenceSet | None:
        """A trusted fence on each of ``directories``, or ``None``."""

        taken = tuple(self.fence(Path(directory)) for directory in directories)
        return (None if any(fence is None for fence in taken) else
                kept_reads.EvidenceSet(taken))

    def ledger_fences(self, ledger) -> kept_reads.EvidenceSet | None:
        """Fences on everything a tier ledger's tokens are read from.

        ``free/``, ``held/`` and each holder directory under ``held/``: a
        token moves by rename between them, and a holder appears or leaves
        by rename into or out of ``held/``.  The holders are listed after
        ``held/``'s stamp is taken, so the list is the one that stamp
        covers.  Taken at most once a scrape, and kept while it holds.
        """

        key = ("ledger-fences", ledger.fence_key)
        self._touched.add(key)
        kept = self._memos.get(key)
        if kept is not None and self.holds(kept[0]):
            return kept[0]
        evidence = ledger.observation_fences(self.fence)
        if evidence is None:
            self._memos.pop(key, None)
            return None
        self._memos[key] = (evidence, None)
        return evidence

    # -- reads ---------------------------------------------------------------


    def sidecar(self, path: Path) -> dict | None | Exception:
        """One small record by path, reused while its directory's stamp holds.

        What ``_pool_sidecar`` returns: the record, ``None`` when
        it is absent, or the failure.  A failure is never kept.
        """

        name = os.fspath(path)
        self._touched.add(name)
        kept = self._files.get(name)
        if kept is not None and self._view.holds(kept[0]):
            self.reused += 1
            return kept[1].record
        fence = self.fence(Path(name).parent)
        parsed = pool.record_store.RecordStore.diagnostic_sidecar_at(Path(name))
        value = parsed.error if parsed.error is not None else parsed.record
        self.computed += 1
        if (fence is not None and not isinstance(value, Exception)
                and self.still(fence)):
            self._files[name] = (fence, parsed)
        else:
            self._files.pop(name, None)
        return value

    # -- the pbstatus census's explicit reads ------------------------------

    def pool_records(self, directory: Path) -> tuple[dict[str, dict | None], list[str]]:
        """``_pool_records``, from the kept listing."""

        try:
            listed = self.queue_records(Path(directory))
        except OSError as exc:
            return {}, [f"pool {directory}: {_unreadable_reason(exc)}"]
        records: dict[str, dict | None] = {}
        notes: list[str] = []
        for path, parsed in listed:
            if parsed.error is not None:
                records[path.stem] = None
                notes.append(f"pool {path}: {parsed.error}")
            else:
                records[path.stem] = parsed.record
        return records, notes

    def endings(self, queue_root: Path, limit: int) -> list[dict]:
        """``endings``, from the kept terminal listings.

        The same rows and projections, without the per-key currency marking
        ``endings`` adds (#1178): resolving which of a key's
        terminal generations is current needs the opposite slot, and serving
        that from the kept listings would either read outside their retained
        versions or pin an answer beside a slot that has since been
        replaced.  The exporter's gauges do not consume the marking, so the
        limit is stated here rather than paid for on every scrape.
        """

        return _kept_endings(self, queue_root, limit)

    def record(self, directory: Path, key: str) -> dict | None:
        """One record of an already-read state directory, or ``None``."""

        try:
            listed = self.queue_records(directory)
        except OSError:
            return None
        wanted = f"{key}.json"
        for path, parsed in listed:
            if path.name == wanted:
                return parsed.record
        return None


    def queue_store(self, root: Path):
        name = os.fspath(root)
        store = self._stores.get(name)
        if store is None:
            store = pool.record_store.RecordStore.kept(
                root, records=self.records, history=self._histories)
            store.begin_read()
            self._stores[name] = store
        return store

    def _from_store(self, directory: Path, meaning: str, read):
        name = os.fspath(directory)
        done = self._reads.get(name)
        if done is not None:
            if done[0] != meaning:
                raise AssertionError(f"{name} read under two meanings")
            return done[2]
        out = read()
        self._reads[name] = (meaning, None, out)
        return out

    def queue_records(self, directory: Path):
        store = self.queue_store(directory.parent)
        return self._from_store(directory, 'queue',
                                lambda: store.diagnostic_records_at(directory))

    def tier_records(self, queue):
        store = self.queue_store(queue.root)
        directory = store.tier_directory()
        return self._from_store(directory, 'tier-records',
                                lambda: store.diagnostic_json_at(directory))

    def plan_entries(self, queue):
        store = self.queue_store(queue.root)
        return self._from_store(residency_plan.registry_root(queue), 'plan-entries',
                                lambda: residency_plan.inspection_entries(store))

    def tier_directories(self, queue):
        store = self.queue_store(queue.root)
        return self._from_store(queue.root / pool.record_store.TIER_RESERVATIONS, 'tier-directories',
                                store.tier_directories)

    def mover_entries(self, queue):
        return self.queue_store(queue.root).mover_entries()

    def released_claim_filings(self, queue_root):
        self._named.add(os.fspath(Path(queue_root) / pool.record_store.WITHDRAWN / 'superseded'))
        return self.queue_store(Path(queue_root)).released_claim_filings()

    def claim_denial_hosts(self, queue):
        directory = queue.root / pool.RESERVATIONS
        store = self.queue_store(queue.root)
        return self._from_store(directory, 'claim-denial-hosts',
                                lambda: store.claim_denial_hosts_at(directory))

def _fresh(reader: KeptReads | None) -> KeptReads:
    """``reader``, or a reader that keeps nothing: one read of everything."""

    if reader is not None:
        return reader
    fresh = KeptReads()
    fresh.begin()
    return fresh

def claim_observation(queue: pool.PoolQueue, key: str,
                reader: KeptReads | None = None) -> dict | None:
    return _fresh(reader).record(queue.records.dir(pool.record_store.CLAIMED), key)

def _kept_endings(reader: KeptReads, queue_root: Path, limit: int) -> list[dict]:
    queue_root = Path(queue_root)
    store = reader.queue_store(queue_root)
    def project():
        chosen = store.recent_ending_paths(limit)
        rows = [reader.derive("ending", entry,
                              lambda entry=entry: ending_row(entry, queue_root),
                              keep=lambda row: not row.get("unreadable"))
                for entry in chosen]
        rows.sort(key=lambda row: row["finished_unix"], reverse=True)
        return rows, chosen
    rows, used = reader.memo(
        ("endings", os.fspath(queue_root), int(limit)), store.ending_fences, project,
        keep=lambda value: not any(row.get("unreadable") for row in value[0]))
    for entry in used:
        reader.used("ending", entry)
    return list(rows)

def _tiers(reader: KeptReads, queue: pool.PoolQueue) -> list[dict[str, object]]:
    """``PoolQueue.tiers``, from the kept ``tiers/`` read.

    The same records in the same order, and the first unreadable one raises
    as the plain read raises it.
    """

    records = []
    for path, parsed in reader.tier_records(queue):
        if parsed.error is not None:
            raise parsed.error
        record = parsed.record
        if isinstance(record, dict) and record.get("tier_id") == path.stem:
            records.append(record)
    return records

def _tier_ids(reader: KeptReads | None, queue: pool.PoolQueue) -> list[str]:
    """Observed tier names; a failed listing cannot prove there are none."""

    paths = (queue.records.tier_directories() if reader is None
             else reader.tier_directories(queue))
    return sorted(path.name for path in paths)

def _ledger_counts(reader: KeptReads, queue: pool.PoolQueue, tier_id: str):
    """``(capacity, available)`` of one tier ledger, or the failure raised."""

    ledger = queue.tier(tier_id)

    def count():
        try:
            return ledger.mint_supply(), ledger.free_supply()
        except Exception as exc:  # noqa: BLE001 -- reported, as the plain read did
            return exc

    return reader.memo(("ledger", tier_id), lambda: reader.ledger_fences(ledger),
                       count, keep=lambda value: not isinstance(value, Exception))

def _tier(value: object) -> str | None:
    text = value if isinstance(value, str) else None
    # The shape ``record_store.RecordStore.check_tier_id`` enforces: a tier id names one
    # tier, never a demand kind and never a path.
    if not text or text.startswith(".") or "/" in text or "@" in text:
        return None
    return text

def _receipt_projection(entry: pool.record_store.HistoryEntry):
    """The six numbers the move gauges read from one receipt.

    Kept by the exporter under the entry's identity, because 7,500
    receipts' records are tens of megabytes an exporter has no use for.
    """

    try:
        record = pool.record_store.read_document(Path(entry.path))
    except (OSError, ValueError):
        record = None
    if not isinstance(record, dict):
        return _UNREADABLE
    if record.get("schema") != pool.POOL_MOVE_SCHEMA_V1:
        return _NOT_A_RECEIPT
    pacing = record.get("disk_pacing")
    pacing = pacing if isinstance(pacing, Mapping) else {}
    return (
        nonnegative_number(record.get("unix")),
        _tier(record.get("tier_id")) or _UNKNOWN_TIER,
        # One definition of "this window measured the pool": the same share
        # test the fill fold prices from, so the gauge and the mint cannot
        # disagree about which receipts count.
        bool(storage_tiers._measured_the_pool(record)),
        nonnegative_number(pacing.get("mean_pool_read_mb_s")),
        nonnegative_number(record.get("bytes_staged")),
        nonnegative_number(pacing.get("pool_read_bytes")),
    )

@dataclass(frozen=True)
class _PlanCensus:
    """What the plan gauges take from one ``_starvation_plan_entry``.

    ``legs`` is ``None`` when the entry carried no cursor gap mapping, and a
    leg's counts are ``None`` when its gap was malformed; both fail the
    collection, as they did when the gauges read the entry directly.
    """

    valid: bool
    unreadable: bool
    tier_id: str = ""
    state: str | None = ""
    legs: tuple | None = None

def _plan_slot(reader: KeptReads, entry: pool.record_store.HistoryEntry) -> dict:
    reader.used("plan", entry)
    key = ("plan", id(entry))
    slot = reader._derived_values.get(key)
    if slot is None:
        slot = {}
        reader._derived_values[key] = slot
    return slot

def _plan_tier(reader: KeptReads, entry: pool.record_store.HistoryEntry):
    """The tier a filed plan names; ``None`` for an empty file.

    ``_UNREADABLE`` for a plan that cannot be read or does not validate.  A
    plan file is written once (``residency_plan.freeze``), so its tier is
    kept for as long as the file's version holds.
    """

    slot = _plan_slot(reader, entry)
    if "tier" in slot:
        reader.reused += 1
        return slot["tier"]
    reader.computed += 1
    try:
        raw = pool.record_store.read_document(Path(entry.path))
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        return _UNREADABLE
    if raw is None:
        return None
    try:
        tier_id = str(residency_plan.validate_plan(raw)["tier_id"])
    except (ValueError, KeyError):
        return _UNREADABLE
    slot["tier"] = tier_id
    return tier_id

def _project_plan(entry: Mapping[str, object], unreadable: list[str]) -> _PlanCensus:
    """``_promotion_metrics``' reading of one plan entry, as numbers."""

    if not entry.get("valid"):
        return _PlanCensus(valid=False, unreadable=bool(unreadable))
    gap = entry.get("cursor_gap")
    legs: tuple | None = None
    if isinstance(gap, Mapping):
        counted = []
        for leg in ("stage", "ram"):
            leg_gap = gap.get(leg)
            if not isinstance(leg_gap, Mapping):
                counted.append((leg, None))
                continue
            staged = leg_gap.get("staged_phases")
            phases = leg_gap.get("unstaged_phases")
            missing = leg_gap.get("unstaged_bytes")
            if (type(staged) is not int or not isinstance(phases, list)
                    or type(missing) not in (int, float) or leg_gap.get("unknown_phases")):
                counted.append((leg, None))
                continue
            counted.append((leg, (staged, len(phases), float(missing))))
        legs = tuple(counted)
    return _PlanCensus(valid=True, unreadable=bool(unreadable),
                       tier_id=str(entry["tier_id"]), state=entry["state"],
                       legs=legs)

def _plan_inputs(
    reader: KeptReads,
    queue: pool.PoolQueue,
    key: str,
    movers: frozenset[str],
    ram_tier_id: str | None,
    *,
    ready: set[str],
    claimed: set[str],
    claimed_listed: set[str],
    announced: Mapping[str, Mapping[str, object]],
    receipts: Mapping[str, pool.record_store.HistoryEntry] | None,
    census_reader=None,
) -> tuple | None:
    """Everything one plan's census reads outside its fenced directories.

    ``_starvation_plan_entry`` reads, besides the plan itself, the
    fragment directory and the tier ledgers (fenced by their stamps), and:

    * the consumer's queue state and, when it is claimed, the phase its
      lease accepted -- the only thing it takes from the lease;
    * whether each of the plan's movers is ready or claimed, and whether a
      claim file is listed for it at all (``resident_movers`` asks the
      filesystem, which also sees a claim too torn to parse);
    * each mover's receipt in ``movers/``, compared by the kept entry, which
      is replaced whenever its file's version changes;
    * for a ram leg, the announced tier's ``epoch`` and ``mountpoint``, the
      two fields ``residency_plan.resident_movers`` reads from it.  The
      record itself is re-announced every tier-loop cycle, so its version
      would miss every scrape.

    ``None`` when one of them could not be read, which is never kept.  Each
    is read from this scrape's listings, and the census reads them afresh
    when it runs, so a change between the two is caught the scrape after:
    the listing then differs from what was kept.
    """

    if receipts is None:
        return None
    state = ("claimed" if key in claimed else "ready" if key in ready else "absent")
    accepted = None
    if state == "claimed":
        lease = _pool_sidecar(queue.records.lease_path(key), reader=census_reader)
        if isinstance(lease, Exception):
            return None
        accepted = _starvation_accepted_phase(lease)
    ram = None
    if ram_tier_id is not None:
        record = announced.get(ram_tier_id)
        ram = (None if record is None
               else (record.get("epoch"), record.get("mountpoint")))
    ordered = sorted(movers)
    return (state, accepted, frozenset(movers & ready), frozenset(movers & claimed),
            frozenset(movers & claimed_listed),
            tuple(receipts.get(mover) for mover in ordered), ram_tier_id, ram)

def _plan_fences(reader: KeptReads, queue: pool.PoolQueue, key: str,
                 tiers: list[str | None]) -> kept_reads.EvidenceSet | None:
    """Fences on the fragment directory and the ledgers one plan's census reads.

    ``None`` when any of them cannot be trusted; the census is then taken
    every scrape.
    """

    fragments = reader.fence(queue.records.residency_fragment_root() / key)
    if fragments is None:
        return None
    taken = [fragments]
    for tier in tiers:
        if tier is None:
            continue
        try:
            ledger = queue.tier(tier)
        except (OSError, ValueError, pool.PoolContractError):
            return None
        ledger_fences = reader.ledger_fences(ledger)
        if ledger_fences is None:
            return None
        taken.extend(ledger_fences)
    return kept_reads.EvidenceSet(tuple(taken))

def _plan_census(reader: KeptReads, queue: pool.PoolQueue, path: Path,
                 entry: pool.record_store.HistoryEntry, *, ready: set[str], claimed: set[str],
                 claimed_listed: set[str],
                 announced: Mapping[str, Mapping[str, object]],
                 receipts: Mapping[str, pool.record_store.HistoryEntry] | None,
                 now: float, census_reader=None, active_complete: bool = True) -> _PlanCensus:
    """One plan's census, reused while nothing it read has changed.

    The census (``_starvation_plan_entry``) lists the consumer's
    fragment directory, reads every fragment, counts each mover's ledger
    tokens and reads their receipts: on the live queue, 330 plans and 97 MB
    of plan documents every scrape.  It is kept on the plan's entry with a
    fence on the fragment directory and on the ledgers of the plan's tiers
    (:meth:`KeptReads.ledger_fences`), and with :func:`_plan_inputs`, and is
    reused while all of them hold.  ``now`` is passed through; the census
    does not read it.
    """

    slot = _plan_slot(reader, entry)
    key = path.stem
    if not active_complete:
        slot.pop("census", None)
    kept = slot.get("census")
    if kept is not None:
        fences, movers, ram_tier_id, inputs, census = kept
        if (_plan_inputs(reader, queue, key, movers, ram_tier_id, ready=ready,
                         claimed=claimed, claimed_listed=claimed_listed,
                         announced=announced,
                         receipts=receipts, census_reader=census_reader) == inputs
                and reader.holds(fences)):
            reader.reused += 1
            return census
        slot.pop("census", None)
    reader.computed += 1
    observation = queue.records.diagnostic_sidecar_at(path)
    raw = observation.error if observation.error is not None else observation.record
    if isinstance(raw, Exception):
        return _PlanCensus(valid=False, unreadable=True)
    fences = inputs = movers = ram_tier_id = None
    try:
        plan = residency_plan.validate_plan(raw) if isinstance(raw, Mapping) else None
    except ValueError:
        plan = None
    if plan is not None:
        movers = frozenset(residency_plan.mover_keys(plan))
        ram = plan.get("ram_tier_id")
        ram_tier_id = str(ram) if isinstance(ram, str) else None
        fences = _plan_fences(reader, queue, key,
                              [str(plan["tier_id"]), ram_tier_id])
        inputs = _plan_inputs(reader, queue, key, movers, ram_tier_id, ready=ready,
                              claimed=claimed, claimed_listed=claimed_listed,
                              announced=announced,
                              receipts=receipts, census_reader=census_reader)
    notes: list[str] = []
    unreadable: list[str] = []
    census = _project_plan(_starvation_plan_entry(
        queue, key, raw, ready=ready, claimed=claimed, notes=notes,
        unreadable=unreadable, now=now, reader=census_reader,
        active_complete=active_complete), unreadable)
    # Only a census with nothing to report is kept: a ledger holder that
    # could not be read is a note, not an unreadable plan, and its ``False``
    # must be read again rather than kept.
    if (active_complete and census.valid and not census.unreadable and not notes
            and fences is not None and inputs is not None
            and reader.still(fences)):
        slot["census"] = (fences, movers, ram_tier_id, inputs, census)
    return census

def _mover_live_rows(queue_root: Path, *, unreadable=None) -> list[dict]:
    """Live actions whose residency block names a byte range: the movers.

    A mover row is the one whose residency block names a range; a consumer's
    names leads.  That is the same line ``movers_claimed_on_tier`` draws, so
    a mover this misses or a consumer this keeps would be a disagreement
    between two readers of one queue rather than a wider net.  Only
    ``ready/`` and ``claimed/`` are walked: terminal records number in the
    thousands and a finished mover's story is in its filed receipt, which
    the next reader below covers.
    """

    now = time.time()
    rows: list[dict] = []
    for state in (pool.record_store.READY, pool.record_store.CLAIMED):
        for key in pool.record_store.RecordStore(Path(queue_root)).inspection_keys(state):
            record = pool.record_store.RecordStore(Path(queue_root)).inspection_slot(state, key)
            if record is None:
                if unreadable is not None:
                    unreadable.append(f"movers {state}/{key}: unreadable or disappeared")
                continue
            block = record.get("residency")
            if not isinstance(block, Mapping):
                continue
            start = block.get("range_start_bytes")
            end = block.get("range_end_bytes")
            if type(start) is not int or type(end) is not int:
                continue
            published = record.get("published_unix")
            rows.append({
                "action_key": key,
                "action_key_prefix": key[:12],
                "state": state,
                "tier_id": block.get("tier_id"),
                "range_start_bytes": start,
                "range_end_bytes": end,
                "range_bytes": end - start,
                "manifest_sha256": block.get("manifest_sha256"),
                "claimed_host": record.get("claimed_host"),
                "published_unix": published,
                "age_s": (now - float(published)
                          if type(published) in (int, float) else None),
            })
    rows.sort(key=lambda row: str(row["action_key"]))
    return rows

def _filed_move_receipts(queue_root: Path, *,
                         limit: int = DEFAULT_MOVER_RECEIPTS) -> dict:
    """The newest filed movement receipts, without reading the directory whole.

    ``movers/`` is append-only -- one JSON per mover, forever -- so the
    window is chosen by ``stat`` first, exactly as ``_scan_actions`` does for
    terminal records: the newest ``limit`` by modification time are read and
    ``truncated`` says whether that was all of them.  A receipt that cannot
    be read or is not a move receipt is counted in ``unreadable`` rather than
    raised: a submission must not fail because one older receipt was
    truncated, and neither must a reader of them.
    """

    directory = Path(queue_root) / pool.record_store.MOVERS
    stamped, scanned = pool.record_store.RecordStore(queue_root).inspection_mover_window(limit)
    truncated = scanned > max(0, int(limit))
    receipts: list[dict] = []
    unreadable = 0
    for _mtime, name in stamped[:max(0, int(limit))]:
        record = pool.record_store.inspection_document(directory / name)
        if (not isinstance(record, Mapping)
                or record.get("schema") != pool.POOL_MOVE_SCHEMA_V1):
            unreadable += 1
            continue
        consumer = record.get("consumer_action_key")
        consumer = str(consumer) if isinstance(consumer, str) else None
        receipts.append({
            "action_key": record.get("action_key"),
            "action_key_prefix": str(record.get("action_key") or "")[:12],
            "consumer_action_key": consumer,
            "consumer_action_key_prefix": (consumer[:12] if consumer else None),
            "tier_id": record.get("tier_id"),
            "range_bytes": record.get("range_bytes"),
            "bytes_staged": record.get("bytes_staged"),
            "complete": record.get("complete"),
            "seconds": record.get("seconds"),
            "unix": record.get("unix"),
        })
    return {"receipts": receipts, "scanned": scanned,
            "unreadable": unreadable, "truncated": truncated}

def _tier_rows(queue_root: Path) -> dict:
    """Every file under ``tiers/``, curated, with the unreadable ones named.

    A record whose ``tier_id`` does not match its file name is reported
    rather than served: ``PoolQueue.tiers`` drops such a file silently, which
    is right for admission and wrong for observation, where a loop filing
    under the wrong name is exactly what the reader is trying to see.
    """

    records = pool.record_store.RecordStore(queue_root)
    tiers: list[dict] = []
    invalid: list[dict] = []
    for stem, observed in records.inspection_tier_records():
        try:
            record = pool.record_store.value_or_raise(observed)
        except (ValueError, pool.PoolContractError) as exc:
            invalid.append({"tier_id": stem, "reason": str(exc)})
            continue
        if record is None:
            invalid.append({"tier_id": stem,
                            "reason": "unreadable or empty record"})
            continue
        if record.get("tier_id") != stem:
            invalid.append({"tier_id": stem,
                            "reason": "tier_id does not match the file name"})
            continue
        tiers.append(_tier_row(record))
    tiers.sort(key=lambda row: str(row.get("tier_id") or ""))
    return {"tiers": tiers, "invalid": invalid}

def _tier_row(record: Mapping[str, object]) -> dict:
    """The fields #660's debugging actually reached for, nothing reshaped."""

    now = time.time()
    sampled = record.get("sampled_unix")
    announced = record.get("announced_unix")
    return {
        "tier_id": record.get("tier_id"),
        "tier": record.get("tier"),
        "host": record.get("host"),
        "tokens": record.get("tokens"),
        "fill_supply": record.get("fill_supply"),
        "ram_admission": record.get("ram_admission"),
        "epoch": record.get("epoch"),
        "window_gib": record.get("window_gib"),
        "mountpoint": record.get("mountpoint"),
        "size_bytes": record.get("size_bytes"),
        "capacity_bytes": record.get("capacity_bytes"),
        "sampled_unix": sampled,
        "age_s": (now - float(sampled)
                  if type(sampled) in (int, float) and math.isfinite(sampled)
                  else None),
        # The loop's liveness, apart from its content's age (#1115): a loop
        # that is idle or busy re-announces within ``OFFER_TIMEOUT_S``; one
        # stuck mid-cycle stops writing, and this age grows past it.
        # ``liveness_refresh.after`` names the last step the loop finished.
        "announced_unix": announced,
        "announced_age_s": (now - float(announced)
                            if type(announced) in (int, float)
                            and math.isfinite(announced) else None),
        "liveness_refresh": record.get("liveness_refresh"),
    }

def _cursor_census(queue_root: Path, *, now: float) -> dict:
    """One row per filed residency plan: where its consumer's cursor is.

    The cursor and the frontier are derived by ``pbstatus``'s own plan census
    -- the same reader ``starvation`` and the pbmetrics gauges use -- so
    three consumers of one set of records cannot disagree about where a
    reader stands.  What this adds is the join #660 asks for at the per-key
    grain: the full consumer key, and the progress record's own timestamp for
    the accepted phase, lifted from the lease beside the claim.  A queue
    where nobody was ever staged has no plans and answers ``[]``; a mount
    that does not answer raises, and lands in ``unavailable``.
    """

    root = Path(queue_root)
    queue = pool.PoolQueue(root.absolute())
    notes: list[str] = []
    unreadable: list[str] = []
    names = residency_plan.inspection_names(queue)
    ready = set(pool.record_store.RecordStore(root).inspection_keys(pool.record_store.READY))
    claimed = set(pool.record_store.RecordStore(root).inspection_keys(pool.record_store.CLAIMED))
    rows: list[dict] = []
    for name in names:
        key = name[:-len(".json")]
        raw = _pool_sidecar(residency_plan.registry_root(queue) / name)
        if isinstance(raw, Exception) or not isinstance(raw, Mapping):
            reason = str(raw) if isinstance(raw, Exception) else "not a JSON object"
            note = f"cursors plan {key[:12]}: {reason}"
            notes.append(note)
            unreadable.append(note)
            rows.append({"consumer_action_key": key,
                         "consumer_action_key_prefix": key[:12],
                         "valid": False, "note": reason})
            continue
        entry = _starvation_plan_entry(
            queue, key, raw, ready=ready, claimed=claimed,
            notes=notes, unreadable=unreadable, now=now)
        rows.append(_cursor_row(queue, key, entry, now=now,
                                notes=notes, unreadable=unreadable))
    return {"rows": rows, "notes": notes, "unreadable": unreadable}

def _cursor_row(queue: pool.PoolQueue, key: str,
                entry: Mapping[str, object], *, now: float,
                notes: list[str], unreadable: list[str]) -> dict:
    """The cursor view of one plan entry: the accepted phase, and the gap.

    A ready or absent consumer carries no lease, so its accepted timestamp
    reads ``null``: nothing recorded one, which is the gap-list fact rather
    than a missing field.  A claimed consumer whose lease will not answer
    keeps its plan-derived row and names the failure, so one unreadable
    sidecar does not cost the caller every cursor.
    """

    row: dict[str, object] = {
        "consumer_action_key": key,
        "consumer_action_key_prefix": key[:12],
        "valid": entry.get("valid"),
        "note": entry.get("note"),
        "state": entry.get("state"),
        "tier_id": entry.get("tier_id"),
        "ram_tier_id": entry.get("ram_tier_id"),
        "accepted_phase": entry.get("accepted_phase"),
        "accepted_index": entry.get("accepted_index"),
        "phase_count": len(entry.get("phases") or []),
        "cursor_gap": entry.get("cursor_gap"),
        "accepted_reported_unix": None,
        "accepted_age_s": None,
        "accepted_units_completed": None,
    }
    if not entry.get("valid") or entry.get("state") != "claimed":
        return row
    lease = _pool_sidecar(queue.records.lease_path(key))
    if isinstance(lease, Exception):
        note = f"cursors lease {key[:12]}: {lease}"
        notes.append(note)
        unreadable.append(note)
        return row
    observed = (lease.get("progress_observation")
                if isinstance(lease, Mapping) else None)
    accepted = (observed.get("last_accepted")
                if isinstance(observed, Mapping) else None)
    if not isinstance(accepted, Mapping):
        return row
    reported = accepted.get("reported_unix")
    if type(reported) in (int, float) and math.isfinite(reported):
        row["accepted_reported_unix"] = reported
        row["accepted_age_s"] = now - float(reported)
    units = accepted.get("units_completed")
    if type(units) is int:
        row["accepted_units_completed"] = units
    return row

_MOVE_RECEIPT_CAP = 1000
_SKEW_S = pool.cpu_admission.MAX_SAMPLE_AGE_S


def _promotion_active(queue, reader):
    ok = True
    incomplete_states = set()
    queue_root = queue.root
    depth: dict[tuple[str, str], int] = defaultdict(int)
    ready: set[str] = set()
    claimed: set[str] = set()
    claimed_listed: set[str] = set()
    consumers: list[str] = []
    for state in (pool.record_store.READY, pool.record_store.CLAIMED):
        try:
            listed = reader.queue_records(Path(queue_root) / state)
        except OSError:
            ok = False
            incomplete_states.add(state)
            continue
        for path, parsed in listed:
            key = path.name[:-len(".json")]
            if state == pool.record_store.CLAIMED:
                claimed_listed.add(key)
            record = parsed.record
            if not isinstance(record, dict) or record.get("action_key") != key:
                # The census sets below classify plans; a row nobody could
                # read classifies nothing, exactly as read_pool treats it.
                ok = False
                incomplete_states.add(state)
                continue
            (claimed if state == pool.record_store.CLAIMED else ready).add(key)
            residency = record.get("residency")
            if not isinstance(residency, Mapping):
                continue
            if "range_start_bytes" in residency:
                depth[(_tier(residency.get("tier_id")) or _UNKNOWN_TIER,
                       state)] += 1
            elif state == pool.record_store.CLAIMED and residency.get("leads"):
                consumers.append(key)
    return _section({"depth": depth, "incomplete_states": tuple(sorted(incomplete_states)),
                     "ready": ready, "claimed": claimed,
                     "claimed_listed": claimed_listed, "consumers": consumers,
                     "active_complete": ok},
                    inputs={"queue_root": str(queue.root), "states": ("ready", "claimed")}, complete=ok)


def _promotion_waits(queue, consumers, plan_files, *, reader, census_reader,
                     consumers_complete, plans_complete):
    ok = consumers_complete and plans_complete
    waiting: dict[str, int] = defaultdict(int)
    quiet_max: dict[str, float] = {}
    for key in consumers:
        lease = _pool_sidecar(queue.records.lease_path(key), reader=census_reader)
        if isinstance(lease, Exception):
            ok = False
            continue
        quiet, grace = _starvation_quiet(
            lease if isinstance(lease, dict) else None)
        child = _starvation_child(
            lease if isinstance(lease, dict) else None)
        is_waiting, _rule = _starvation_waiting(quiet, grace, child)
        if not is_waiting:
            continue
        # No plan filed is the ordinary answer for a consumer the coordinator
        # never staged, which still counts as a waiter whose tier is unknown
        # rather than as a read failure.
        plan_entry = plan_files.get(residency_plan.filed_name(queue, key))
        tier_id = _UNKNOWN_TIER
        if plan_entry is not None:
            named = _plan_tier(reader, plan_entry)
            if isinstance(named, str):
                tier_id = named
            elif named is _UNREADABLE:
                ok = False
        waiting[tier_id] += 1
        if quiet is not None:
            quiet_max[tier_id] = max(quiet_max.get(tier_id, quiet), quiet)
    return _section({"waiting": waiting, "quiet_max": quiet_max},
                    inputs={"queue_root": str(queue.root), "consumers": tuple(consumers),
                            "plans": tuple(plan_files), "consumers_complete": consumers_complete,
                            "plans_complete": plans_complete}, complete=ok)


def _promotion_plans(queue, plan_listing, plan_listing_error, active, announced, *, reader, census_reader, now):
    ok = active["active_complete"]
    states_complete = active["active_complete"] and plan_listing_error is None
    legs_complete = {leg: plan_listing_error is None for leg in ("stage", "ram")}
    ready, claimed = active["ready"], active["claimed"]
    claimed_listed = active["claimed_listed"]
    plans: dict[tuple[str, str], int] = defaultdict(int)
    staged_phases: dict[tuple[str, str], int] = defaultdict(int)
    unstaged_phases: dict[tuple[str, str], int] = defaultdict(int)
    unstaged_bytes: dict[tuple[str, str], float] = defaultdict(float)
    invalid_plans = 0
    unreadable = False
    if plan_listing_error is not None:
        ok = False
    try:
        receipts = {entry.name[:-len(".json")]: entry for entry
                    in reader.mover_entries(queue)}
    except OSError:
        receipts = None
    for path, entry in plan_listing:
        if entry is None:
            invalid_plans += 1
            unreadable = True
            states_complete = False
            legs_complete = {leg: False for leg in legs_complete}
            continue
        census = _plan_census(reader, queue, path, entry, ready=ready,
                              claimed=claimed, claimed_listed=claimed_listed,
                              announced=announced,
                              receipts=receipts, now=now, census_reader=census_reader,
                              active_complete=active["active_complete"])
        unreadable = unreadable or census.unreadable
        if not census.valid:
            invalid_plans += 1
            states_complete = False
            legs_complete = {leg: False for leg in legs_complete}
            continue
        if census.state is not None:
            plans[(census.tier_id, census.state)] += 1
        if census.legs is None:
            ok = False
            legs_complete = {leg: False for leg in legs_complete}
            continue
        for leg, counted in census.legs:
            if counted is None:
                ok = False
                legs_complete[leg] = False
                continue
            staged, phases, missing = counted
            staged_phases[(census.tier_id, leg)] += staged
            unstaged_phases[(census.tier_id, leg)] += phases
            unstaged_bytes[(census.tier_id, leg)] += missing
    if unreadable:
        ok = False
    return _section({"plans": plans, "staged_phases": staged_phases,
                     "unstaged_phases": unstaged_phases, "unstaged_bytes": unstaged_bytes,
                     "invalid_plans": invalid_plans, "receipts": receipts,
                     "states_complete": states_complete, "legs_complete": legs_complete},
                    inputs={"queue_root": str(queue.root), "ready": tuple(sorted(ready)),
                            "claimed": tuple(sorted(claimed)), "sampled_unix": now,
                            "active_complete": active["active_complete"]}, complete=ok)


def _delivery(queue, receipts, *, reader, now, window_seconds):
    ok = True
    if receipts is None:
        receipts = {}
        ok = False
    candidates = heapq.nlargest(_MOVE_RECEIPT_CAP,
                                ((entry.mtime, entry.path, entry)
                                 for entry in receipts.values()))
    move_complete = len(receipts) <= _MOVE_RECEIPT_CAP
    move_jobs: dict[str, int] = defaultdict(int)
    measured_jobs: dict[str, int] = defaultdict(int)
    move_staged: dict[str, float] = defaultdict(float)
    move_pool_read: dict[str, float] = defaultdict(float)
    move_rates: dict[str, list[float]] = defaultdict(list)
    for _mtime, _path, entry in candidates:
        receipt = reader.derive("receipt", entry,
                                lambda entry=entry: _receipt_projection(entry),
                                keep=lambda projected: projected is not _UNREADABLE)
        if receipt is _UNREADABLE:
            ok = False
            continue
        if receipt is _NOT_A_RECEIPT:
            continue
        stamped, tier_id, measured, rate, staged, pool_read = receipt
        if stamped is None or not -_SKEW_S <= now - stamped <= window_seconds:
            continue
        move_jobs[tier_id] += 1
        if measured:
            measured_jobs[tier_id] += 1
        if rate is not None and rate > 0:
            move_rates[tier_id].append(rate)
        if staged is not None and pool_read is not None:
            move_staged[tier_id] += staged
            move_pool_read[tier_id] += pool_read
    return _section({"move_complete": move_complete, "move_jobs": move_jobs,
                     "measured_jobs": measured_jobs, "move_staged": move_staged,
                     "move_pool_read": move_pool_read, "move_rates": move_rates},
                    inputs={"queue_root": str(queue.root), "now": now,
                            "window_seconds": window_seconds},
                    bound={"mover_receipts": _MOVE_RECEIPT_CAP}, complete=ok)


def _qualified_counts(observed, population, *, complete):
    """Full counts require population coverage; observed values stay in their section."""
    return {key: observed.get(key, 0) if complete else None
            for key in set(observed) | set(population)}


def promotion(queue_root: Path, *, now: float, window_seconds: float,
              reader: KeptReads | None = None, census_reader=None) -> Section[dict]:
    """The exporter's selected sections in their original conditional read order."""
    reader = _fresh(reader)
    try:
        queue = pool.PoolQueue(queue_root)
    except Exception:
        return _section(None, inputs={"queue_root": str(queue_root)})
    tier_section = tiers(queue_root, view="sources", reader=reader)
    announced, tier_ids = tier_section.value["announced"], tier_section.value["tier_ids"]
    active_section = movers(queue_root, view="promotion", reader=reader)
    try:
        plan_listing = reader.plan_entries(queue)
        plan_listing_error = None
    except OSError as exc:
        plan_listing, plan_listing_error = [], exc
    plan_files = {path.name: entry for path, entry in plan_listing if entry is not None}
    wait_section = waits(queue_root, view="promotion", consumers=active_section.value["consumers"],
                         plan_files=plan_files, reader=reader, census_reader=census_reader,
                         consumers_complete=active_section.value["active_complete"],
                         plans_complete=plan_listing_error is None)
    plan_section = plans(queue_root, view="promotion", listing=plan_listing,
                         listing_error=plan_listing_error, active=active_section.value,
                         announced=announced, now=now, reader=reader, census_reader=census_reader)
    delivery_section = movers(queue_root, view="delivery", receipts=plan_section.value["receipts"],
                              reader=reader, now=now, window_seconds=window_seconds)
    sections = {"tiers": tier_section, "movers": active_section, "waits": wait_section,
                "plans": plan_section, "delivery": delivery_section}
    known_tiers = sorted(set(tier_ids) | set(announced))
    values = {"tiers": known_tiers, **wait_section.value,
                     **{k: v for k, v in plan_section.value.items() if k != "receipts"},
                     **delivery_section.value, "sections": sections}
    values["depth"] = {}
    for state in (pool.record_store.READY, pool.record_store.CLAIMED):
        observed = {key: count for key, count in active_section.value["depth"].items() if key[1] == state}
        values["depth"].update(_qualified_counts(observed, ((tier, state) for tier in known_tiers),
            complete=state not in active_section.value["incomplete_states"]))
    values["waiting"] = _qualified_counts(wait_section.value["waiting"], known_tiers,
        complete=wait_section.completeness == "complete")
    values["plans"] = _qualified_counts(plan_section.value["plans"],
        ((tier, state) for tier in known_tiers for state in ("claimed", "ready", "absent")),
        complete=plan_section.value["states_complete"])
    for field in ("staged_phases", "unstaged_phases", "unstaged_bytes"):
        values[field] = {}
        for leg in ("stage", "ram"):
            observed = {key: count for key, count in plan_section.value[field].items() if key[1] == leg}
            values[field].update(_qualified_counts(observed, ((tier, leg) for tier in known_tiers),
                complete=plan_section.value["legs_complete"][leg]))
    for field in ("move_jobs", "measured_jobs"):
        values[field] = _qualified_counts(delivery_section.value[field], known_tiers,
            complete=delivery_section.completeness == "complete")
    return _section(values,
                    inputs={"queue_root": str(queue_root), "sampled_unix": now},
                    complete=all(part.completeness == "complete" for part in sections.values()))


def denials(queue_root, *, hours=24.0, captured=_NOT_CAPTURED, now=None,
            profile="hours", window_seconds=None, reader=None) -> Section[dict]:
    """Explicit observation windows; zero hours and zero metric seconds differ."""
    if profile == "metrics":
        return _denial_counts(queue_root, now=now, window_seconds=window_seconds, reader=reader)
    if profile == "latest":
        notes = []
        value = _starvation_denials(pool.PoolQueue(queue_root), notes=notes, reader=reader)
        return _section(value, inputs={"queue_root": str(queue_root), "window": "latest, ungated by time",
                                      "join": "live residency range presence"}, errors=notes)
    if profile != "hours":
        raise ValueError(f"unknown denial census profile: {profile}")
    window_s = max(0.0, float(hours)) * 3600.0
    if captured is _NOT_CAPTURED:
        captured = _pool_claim_denials(pool.PoolQueue(queue_root), reader=reader)
    denials, notes = captured if captured is not None else (None, None)
    now = time.time() if now is None else now
    cutoff = None if window_s <= 0 else now - window_s
    hosts: dict[str, dict] = {}
    total = 0
    if denials is not None:
        for denial in denials:
            denied = denial.get("denied_unix")
            if (type(denied) not in (int, float)
                    or not math.isfinite(denied)
                    or denied > now
                    or (cutoff is not None and denied < cutoff)):
                continue
            host = str(denial.get("host"))
            reason = str(denial.get("reason"))
            key = str(denial.get("action_key"))
            entry = hosts.setdefault(
                host, {"denials_in_window": 0, "by_reason": {}})
            entry["denials_in_window"] += 1
            total += 1
            bucket = entry["by_reason"].setdefault(reason, {
                "count": 0, "latest_denied_unix": None,
                "latest_action_key": None,
                "latest_action_key_prefix": None,
                "latest_decision_reason": None,
            })
            bucket["count"] += 1
            if (bucket["latest_denied_unix"] is None
                    or float(denied) > float(bucket["latest_denied_unix"])):
                decision = denial.get("evidence")
                decision = (decision.get("decision")
                            if isinstance(decision, dict) else None)
                subreason = (decision.get("reason")
                             if isinstance(decision, dict) else None)
                bucket["latest_denied_unix"] = denied
                bucket["latest_action_key"] = key
                bucket["latest_action_key_prefix"] = key[:12]
                bucket["latest_decision_reason"] = (
                    subreason if isinstance(subreason, str) else None)
    value = {
        "queue_root": str(queue_root),
        "hours": float(hours),
        "cutoff_unix": cutoff,
        "hosts": hosts if denials is not None else None,
        "total_in_window": total if denials is not None else None,
        "reader_notes": notes,
    }
    return _section(value, inputs={"queue_root": str(queue_root), "hours": float(hours),
                     "now": now, "future": "excluded", "zero_hours": "all history"},
                    errors=notes or (), available=denials is not None)


def _denial_host(value):
    text = str(value or "")
    return text if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", text) else None


def _denial_counts(queue_root, *, now, window_seconds, reader=None):
    queue, ok = pool.PoolQueue(queue_root), True
    census_reader = reader
    try:
        denials, notes = _pool_claim_denials(queue, reader=census_reader)
        ok = not notes
    except Exception as exc:
        denials = []
        ok = False
        notes = [str(exc)]
    counts: dict[tuple[str, str], int] = defaultdict(int)
    for denial in denials:
        if not isinstance(denial, dict):
            ok = False
            continue
        host = _denial_host(denial.get("host"))
        reason = denial.get("reason")
        reason = reason if isinstance(reason, str) and reason else "unknown"
        age = nonnegative_number(now - float(denial.get("denied_unix", -math.inf)))
        if host is None or age is None:
            ok = False
            continue
        if age <= window_seconds:
            counts[(host, reason)] += 1
    return _section(counts, inputs={"queue_root": str(queue_root), "now": now,
                    "window_seconds": window_seconds, "future": "invalid age",
                    "zero_seconds": "only age zero"}, errors=notes, complete=ok)
