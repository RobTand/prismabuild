"""Conservative measurement selection, not resource-release prediction (#1419).

The existing passes sidecar carries one host election per publication. A complete
read is refreshed under measurement transition keys and host admission; directory
absence alone never retires an election. Payload deadlines are opportunity
metadata only. No current candidate has a proved admission-to-release bound.
"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager, ExitStack
import fcntl
import math
import os
from pathlib import Path
import stat
import time
from typing import TYPE_CHECKING

from . import adaptive_cpu, core, local_scratch
from . import _bounded_reader as reader

if TYPE_CHECKING:
    from .pool import PoolQueue, ResourceLedger

FIELD = "measurement_reservation"
SCHEMA = "prismabuild.measurement_reservation.v1"
READ_SECTION = "measurement-publications-v1"
READ_BUDGET_S = 5.0
MAX_RECORDS = 4096
MAX_RECORD_BYTES = 4 * 1024 * 1024
MAX_FENCE_BYTES = 4096
#: How long a census waits for a sibling loop's fence before it refuses as
#: "reader busy" (#1498). Taken before any M key or H, so a waiter holds
#: nothing a holder needs; a holder never waits on a waiter.
FENCE_WAIT_S = 2.0
FENCE_POLL_S = 0.02


class CensusUnavailable(RuntimeError):
    """An incomplete census cannot authorize lower-priority acquisition."""


def _read(path: Path, *, optional: bool = False, limit: int = MAX_RECORD_BYTES) -> dict | None:
    try:
        raw = core._read_regular_file_nofollow(
            path, where=READ_SECTION, max_bytes=limit, replaced_leaf=True)
    except FileNotFoundError:
        if optional:
            return None
        raise CensusUnavailable(f"publication disappeared during census: {path}") from None
    value = core._decode_strict_json(raw, where=READ_SECTION)
    if not isinstance(value, dict):
        raise CensusUnavailable(f"non-object census record: {path}")
    return value


def selection(record: dict) -> dict | None:
    value = record.get(FIELD)
    if FIELD not in record:
        return None
    if (not isinstance(value, dict) or set(value) != {
            "schema", "action_key", "generation", "host", "published_unix",
            "priority", "epoch_unix", "opportunity_unix"}
            or value.get("schema") != SCHEMA):
        raise CensusUnavailable("malformed canonical measurement selection")
    for field in ("action_key", "generation"):
        text = value[field]
        if not isinstance(text, str) or len(text) != 64 or any(c not in "0123456789abcdef" for c in text):
            raise CensusUnavailable(f"invalid selection {field}")
    host = value["host"]
    if not isinstance(host, str) or not host or host != host.strip():
        raise CensusUnavailable("invalid selection host")
    if type(value["priority"]) is not int:
        raise CensusUnavailable("invalid selection priority")
    for field in ("published_unix", "epoch_unix", "opportunity_unix"):
        number = value[field]
        if type(number) not in (int, float) or not math.isfinite(number):
            raise CensusUnavailable(f"invalid selection {field}")
    if value["opportunity_unix"] < value["epoch_unix"]:
        raise CensusUnavailable("selection opportunity precedes election")
    if core.canonical_sha256({"action_key": value["action_key"],
                              "published_unix": float(value["published_unix"])}) != value["generation"]:
        raise CensusUnavailable("selection publication identity mismatch")
    return value


def _capture(queue: PoolQueue) -> dict:
    """Strict full-publication discovery; called only by the read-only child.

    Include finish marks and elected sidecars even when neither live directory
    names M. The subsequent locked refresh, not this sequential discovery,
    closes CLAIMED -> tombstone -> READY. New M keys force another pass.
    """
    from . import pool
    rows: dict[str, list[dict]] = {}
    selected: dict[str, dict] = {}
    opportunities: dict[str, dict] = {}
    count = 0
    for state in (pool.READY, pool.CLAIMED, "passes"):
        directory = queue.root / state
        try:
            entries = os.scandir(directory)
        except FileNotFoundError:
            if state == pool.PASSES:
                continue  # the existing sidecar directory is created lazily
            raise
        with entries:
            for entry in entries:
                count += 1
                if count > MAX_RECORDS:
                    raise CensusUnavailable("measurement census record cap exceeded")
                name = entry.name
                is_mark = state == pool.CLAIMED and name.endswith((pool.TOMBSTONE_SUFFIX, pool.LATE_FINISH_SUFFIX))
                if not name.endswith(".json") and not is_mark:
                    continue  # leases/status/progress are not publications
                key = name[:64]
                if not pool._is_hex64(key):
                    raise CensusUnavailable(f"unaddressable publication: {entry.path}")
                record = _read(Path(entry.path))
                if record is None:
                    raise CensusUnavailable("missing census record")
                if record.get("action_key") != key:
                    raise CensusUnavailable(f"census action identity mismatch: {entry.path}")
                if state == "passes":
                    chosen = selection(record)
                    if chosen is not None:
                        if chosen["action_key"] != key:
                            raise CensusUnavailable("selection sidecar key mismatch")
                        selected[key] = chosen
                else:
                    queue.attempt_generation(record)  # strict publication identity
                    if type(record.get("priority", 0)) is not int:
                        raise CensusUnavailable("unreadable publication priority")
                    rows.setdefault(key, []).append(record)
                    if state == pool.CLAIMED and not is_mark:
                        governed, requested = pool._declared_run_bound(record, max_bytes=MAX_RECORD_BYTES)
                        if governed == "deadline" and requested is not None:
                            opportunities[key] = {
                                "host": record.get("claimed_host"), "requested": requested,
                                "claimed_unix": record.get("claimed_unix"),
                                "generation": queue.attempt_generation(record)}
    measurements: dict[str, list[dict]] = {}
    for key, versions in rows.items():
        for record in versions:
            # An absent request is the supported legacy direct-publication
            # path, not a sealed measurement. Corrupt/unreadable is NOT absent.
            action = pool._sealed_action_request(str(record.get("cas_root")), key,
                                                max_bytes=MAX_RECORD_BYTES)
            task = action.get("task") if action is not None else None
            if isinstance(task, Mapping) and task.get("task_class") == "measurement":
                measurements.setdefault(key, []).append(record)
    elections: dict[str, dict] = {}
    for key, chosen in selected.items():
        versions = rows.get(key, [])
        same = [row for row in versions if queue.attempt_generation(row) == chosen["generation"]]
        if same:
            if key not in measurements:
                raise CensusUnavailable("elected measurement sealed identity unavailable")
            elections[key] = chosen
            continue
        # Absence is not retirement. Exact ending or a strictly newer live
        # publication under this key may retire it, but marks/claims of the
        # old generation above take precedence over any success/cancellation.
        endings = []
        for state in (pool.DONE, pool.FAILED, pool.WITHDRAWN):
            row = _read(queue.item_path(state, key), optional=True)
            if row is None:
                continue
            stamp = row.get("withdrawn_unix" if state == pool.WITHDRAWN else "finished_unix")
            if (row.get("schema") != pool.POOL_OUTCOME_SCHEMA_V1 or row.get("action_key") != key
                    or not isinstance(row.get("status"), str) or not row["status"]
                    or (state == pool.WITHDRAWN and row["status"] != "withdrawn")
                    or not isinstance(stamp, (int, float)) or isinstance(stamp, bool)
                    or not math.isfinite(stamp)):
                raise CensusUnavailable("incomplete measurement retirement proof")
            endings.append(row)
        if any(queue.attempt_generation(row) == chosen["generation"] for row in endings):
            continue
        if versions and all(float(row["published_unix"]) > chosen["published_unix"] for row in versions):
            continue
        elections[key] = chosen  # missing authority stays fenced, indefinitely
    return {"measurements": measurements, "elections": elections, "selections": selected,
            "opportunities": opportunities, "keys": sorted(set(measurements) | set(selected))}


class CensusReader:
    """One protected host-local restart fence, shared by every queue observer.

    This storage is the existing supported box-state root, which MUST be local
    and preserved while readers survive. Small regular-file operations are
    cooperative; no filesystem syscall is claimed to have a hard deadline.
    Clearing/repointing that root requires stopped readers, just as clearing
    admission state requires stopped claimants. No arbitrary callback or shared
    persistence is used by the owned-reader grant.
    """

    def __init__(self, queue: PoolQueue, ledger: ResourceLedger):
        self.queue = queue
        directory, digest = adaptive_cpu.box_state(ledger.base)
        self.directory = directory
        self.name = digest + ".measurement-reader-v1"
        self.pool_identity = str(queue.root.resolve())
        self._held: tuple[int, int] | None = None

    @contextmanager
    def held(self):
        """Own the restart fence across a whole two-phase census (#1498).

        ``locked_census`` reads outside H for discovery and again under H for
        its refresh. Releasing the fence between the phases let a concurrent
        queue observer take it in the gap and deny the refresh with
        "measurement census reader busy" after the caller had already taken
        the measurement transition keys and host admission: the phase-two
        steal behind the sustained admission denials of #1498. One fence
        acquisition per ``locked_census`` keeps the single-reader contract
        exactly -- the ownership marker is persisted per acquisition, the
        retained-reader liveness check runs per acquisition, and at most one
        bounded census child runs at a time -- while the two reads of one
        census share that acquisition. Everything acquired inside the hold
        stays nonblocking, so a refused fence, transition key or admission
        gate still denies the pass and releases the fence.
        """
        directory = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        lock = None
        try:
            info = os.fstat(directory)
            if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_mode & 0o077):
                raise CensusUnavailable("unsafe local census directory")
            # Reuse the exact open-descriptor local mount observer under the
            # host-local state policy: a path named BOX_STATE_ROOT is not
            # itself evidence of local storage, and this small-file rendezvous
            # supports tmpfs while shared storage still refuses (#1451).
            local_scratch._descriptor_state_identity(directory)
            lock = os.open(self.name + ".guard", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC,
                           0o600, dir_fd=directory)
            info = os.fstat(lock)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_nlink != 1 or info.st_mode & 0o077):
                raise CensusUnavailable("unsafe local census fence lock")
            # Bounded wait, not an instant refusal (#1498): every loop on a
            # box censuses each candidate, so a nonblocking fence turned one
            # sibling's census into this candidate's denial. The waiter holds
            # only its own candidate key, never M or H, and still refuses once
            # FENCE_WAIT_S passes; nothing is read without the fence.
            waited = reader.Deadline(FENCE_WAIT_S)
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    remaining = waited.remaining()
                    if remaining is None or remaining <= 0:
                        raise CensusUnavailable("measurement census reader busy") from None
                    time.sleep(min(FENCE_POLL_S, remaining))
            marker = self.directory / self.name
            previous = _read(marker, optional=True, limit=MAX_FENCE_BYTES)
            if previous is not None:
                ownership = reader.ReaderOwnership.from_record(previous)
                if reader.reader_liveness(ownership, pool_identity=self.pool_identity,
                                          section=READ_SECTION) != "settled":
                    raise CensusUnavailable("retained measurement census reader unresolved")
            self._held = (directory, lock)
            try:
                yield
            finally:
                self._held = None
        except (OSError, ValueError, core.PrismaBuildError, reader.ReaderOwnershipUnavailable,
                local_scratch.LocalScratchError) as exc:
            raise CensusUnavailable(str(exc)) from exc
        finally:
            if lock is not None:
                os.close(lock)
            os.close(directory)

    def capture(self) -> dict:
        if self._held is not None:
            return self._census(self._held[0])
        with self.held():
            return self._census(self._held[0])

    def _census(self, directory: int) -> dict:
        """One strict bounded census read; the caller owns the held fence."""
        try:
            abandoned: list[dict] = []
            def persist(ownership: reader.ReaderOwnership) -> None:
                # Exact identity is durable BEFORE release. At most 4 KiB;
                # fixed private dir, no paths supplied by the child or CAS.
                payload = core._canonical_bytes(ownership.to_record())
                if len(payload) > MAX_FENCE_BYTES:
                    raise CensusUnavailable("census ownership exceeds local cap")
                temporary = self.name + ".writing"
                descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                     0o600, dir_fd=directory)
                try:
                    reader._write_reader_payload(descriptor, payload)
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                os.replace(temporary, self.name, src_dir_fd=directory, dst_dir_fd=directory)
                os.fsync(directory)
            # A previous interrupted pre-publication write granted no read.
            # Its inode is not removed automatically: unknown storage fails
            # closed instead of erasing potentially partial ownership.
            reply = reader.bounded(READ_SECTION, lambda: _capture(self.queue),
                                   deadline=reader.Deadline(READ_BUDGET_S), abandoned=abandoned,
                                   on_spawn=persist, pool_identity=self.pool_identity,
                                   announce_retained=False)
            if reply.get("status") != "ok" or abandoned:
                raise CensusUnavailable(f"measurement census unavailable: {reply}")
            value = reply.get("value")
            if (not isinstance(value, dict)
                    or set(value) != {"measurements", "elections", "selections", "opportunities", "keys"}
                    or not all(isinstance(value[field], dict)
                               for field in ("measurements", "elections", "selections", "opportunities"))
                    or not isinstance(value["keys"], list)
                    or len(value["keys"]) > MAX_RECORDS
                    or any(not isinstance(key, str) or len(key) != 64
                           or any(c not in "0123456789abcdef" for c in key) for key in value["keys"])
                    or not (set(value["measurements"]) | set(value["elections"])).issubset(value["keys"])):
                raise CensusUnavailable("incomplete measurement census reply")
            for key, chosen in value["elections"].items():
                checked = selection({FIELD: chosen})
                if checked is None or checked["action_key"] != key:
                    raise CensusUnavailable("census election identity mismatch")
            return value
        except (OSError, ValueError, core.PrismaBuildError, reader.ReaderOwnershipUnavailable,
                local_scratch.LocalScratchError) as exc:
            raise CensusUnavailable(str(exc)) from exc


@contextmanager
def locked_census(queue: PoolQueue, ledger: ResourceLedger, controller):
    """M transition keys (sorted/nonblocking) BEFORE H; refresh through acquire.

    The candidate key may already be owned: the existing reentrant transition
    contract is intentional. No cross-key wait or publisher host-gate protocol
    is added. The guarantee starts at canonical election, not at arbitrary
    future publication. An unlocked newly discovered M denies this pass.
    The reader fence is acquired once for both phases and released after the
    refresh (#1498): a concurrent observer cannot consume it between them, and
    a fence, transition or admission refusal releases what was taken.
    """
    census_reader = CensusReader(queue, ledger)
    with census_reader.held():
        discovered = census_reader.capture()
        with ExitStack() as held:
            keys = set(discovered["keys"])
            for key in sorted(keys):
                if not held.enter_context(queue._transition_locked(key, blocking=False)):
                    raise CensusUnavailable(f"measurement transition busy: {key}")
            held.enter_context(queue._admission_lock(controller))
            current = census_reader.capture()
            if not set(current["keys"]).issubset(keys):
                raise CensusUnavailable("new unlocked measurement generation; restart census")
            # A success/cancellation slot is not physical ownership settlement.
            # Under H, retained tokens on this elected host still prevent refill.
            for key in ledger.held_keys():
                chosen = current["selections"].get(key)
                if chosen is not None and chosen["host"] == ledger.base.name:
                    current["elections"][key] = chosen
            yield current


@contextmanager
def admission_census(queue: PoolQueue, ledger: ResourceLedger, controller):
    # Catch setup refusals only, never an exception thrown by the claim body.
    with ExitStack() as held:
        try:
            census = held.enter_context(locked_census(queue, ledger, controller))
        except CensusUnavailable as exc:
            yield {"unavailable": str(exc)}
            return
        yield census


def blocking_selection(census: dict, item: dict, *, host: str, funded_by: str | None) -> dict | None:
    """UNKNOWN-first: no current timed candidate proves a safe finish."""
    for key, chosen in sorted(census["elections"].items()):
        if (chosen["host"] == host and key != item["action_key"]
                and int(item.get("priority", 0)) < chosen["priority"] and funded_by != key):
            return chosen
    return None


def elect(queue: PoolQueue, ledger: ResourceLedger, controller, item: dict,
          verdict: dict, *, sampled_unix: object, gpu_sample: Mapping | None) -> dict | None:
    """Choose a host once; a finite incumbent *opportunity* is metadata, not a bound."""
    from . import pool
    generation = queue.attempt_generation(item)
    with locked_census(queue, ledger, controller) as census:
        prior = _read(queue.passes_path(item["action_key"]), optional=True) or {}
        chosen = selection(prior)
        if chosen is not None and chosen["generation"] == generation:
            return chosen
        if chosen is not None and item["action_key"] in census["elections"]:
            raise CensusUnavailable("previous measurement election has not retired")
        holders = verdict.get("holders")
        if (not isinstance(sampled_unix, (int, float)) or isinstance(sampled_unix, bool)
                or not 0 <= pool._now() - sampled_unix <= adaptive_cpu.MAX_SAMPLE_AGE_S):
            return None
        if item.get("needs_gpu"):
            stamp = gpu_sample.get("sampled_unix") if gpu_sample is not None else None
            if (gpu_sample is None or gpu_sample.get("schema") != "prismabuild.gpu_capacity.v1"
                    or gpu_sample.get("complete") is not True or gpu_sample.get("attributed") is not True
                    or gpu_sample.get("foreign_processes") != []
                    or not isinstance(stamp, (int, float)) or isinstance(stamp, bool)
                    or not 0 <= pool._now() - stamp <= adaptive_cpu.MAX_SAMPLE_AGE_S):
                return None
        if (verdict.get("withhold") is not True or verdict.get("why") != "draining_for_measurement"
                or not isinstance(holders, list) or not holders):
            return None
        if not any(queue.attempt_generation(row) == generation
                   for row in census["measurements"].get(item["action_key"], [])):
            raise CensusUnavailable("measurement publication changed before election")
        # Re-read sealed incumbent opportunities while H serializes admission.
        # No optimistic timeout-derived safe-fit/backfill permission follows.
        # An incumbent that declared no finite deadline (``pbrun`` without
        # ``--timeout-s``, a progress-governed action) still elects: the
        # opportunity is metadata only, and refusing to elect left the host
        # open to refill once the bounded attention lapsed, so a continuous
        # lower-priority stream starved the measurement (#1419).
        holders = ledger.held_keys()
        if not holders:
            return None
        ends = []
        for key in holders:
            opportunity = census["opportunities"].get(key, {})
            requested, claimed = opportunity.get("requested"), opportunity.get("claimed_unix")
            if (opportunity.get("host") != ledger.base.name
                    or not isinstance(requested, (int, float)) or isinstance(requested, bool)
                    or not isinstance(claimed, (int, float)) or isinstance(claimed, bool)):
                continue
            end = float(claimed) + float(requested)
            if math.isfinite(end):
                ends.append(end)
        chosen = {"schema": SCHEMA, "action_key": item["action_key"], "generation": generation,
                  "host": ledger.base.name, "published_unix": float(item["published_unix"]),
                  "priority": int(item.get("priority", 0)), "epoch_unix": pool._now(),
                  "opportunity_unix": max([pool._now(), *ends])}
        prior[FIELD] = chosen
        prior["action_key"] = item["action_key"]
        pool._write_json_atomic(queue.passes_path(item["action_key"]), prior)
        return chosen
