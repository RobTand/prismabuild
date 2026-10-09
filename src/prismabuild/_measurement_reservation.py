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

from . import adaptive_cpu, core, local_scratch, storage_tiers
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
#: How many times one census rescans when a listed publication vanishes under it
#: (#1571); the census child's own deadline still bounds the total.
CAPTURE_RESCANS = 3


class CensusUnavailable(RuntimeError):
    """An incomplete census cannot authorize lower-priority acquisition."""


class PublicationDisappeared(CensusUnavailable):
    """A listed publication was gone when the census read it (#1571)."""


def _read(path: Path, *, optional: bool = False, limit: int = MAX_RECORD_BYTES) -> dict | None:
    try:
        raw = core._read_regular_file_nofollow(
            path, where=READ_SECTION, max_bytes=limit, replaced_leaf=True)
    except FileNotFoundError:
        if optional:
            return None
        raise PublicationDisappeared(f"publication disappeared during census: {path}") from None
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
    """One strict census, rescanned when a listed publication vanishes (#1571).

    A publication is claimed, finished or withdrawn at any moment, so the
    directory listing can name a file that is gone by the time it is read. That
    is a race with the queue, not an unreadable census: refusing it denied the
    whole pass ('publication disappeared during census') on a queue whose
    rows were moving. Rescan a bounded number of times; a queue that keeps
    changing under every scan, or any other unreadable record, still refuses.
    """
    for attempt in range(CAPTURE_RESCANS):
        try:
            return _scan_publications(queue)
        except PublicationDisappeared:
            if attempt + 1 == CAPTURE_RESCANS:
                raise
    raise AssertionError("unreachable")  # pragma: no cover


def _scan_publications(queue: PoolQueue) -> dict:
    """Strict full-publication discovery; called only by the read-only child.

    Include finish marks and elected sidecars even when neither live directory
    names M. The subsequent locked refresh, not this sequential discovery,
    closes CLAIMED -> tombstone -> READY. New M keys force another pass.
    """
    from . import pool
    rows: dict[str, list[dict]] = {}
    unorderable: dict[str, list[dict]] = {}   # live members, never candidates
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
                        unorderable_field = pool.PoolQueue._unorderable_queue_field(record)
                        if (state == pool.READY and unorderable_field is not None
                                and unorderable_field[0] == "priority"):
                            # A READY record the queue itself cannot order
                            # holds no tokens and runs nothing, and the queue
                            # files it by name instead of raising
                            # (``ready_items``).  Refusing it denied every
                            # good claim on the host (#1506).  It is never
                            # claimed, so it cannot reach the CLAIMED census.
                            # It is still a live gang member: only candidate
                            # classification skips it.  A priority the queue
                            # CAN order (5.5, True, "5") stays strict, even
                            # when another ordering field is unreadable: the
                            # helper names the FIRST bad field, and the queue
                            # reads a bad ``passes`` as 0 before it orders, so
                            # it still lists and can claim such a record.  A
                            # CLAIMED one stays strict too: a running
                            # incumbent the census cannot read is unknown.
                            unorderable.setdefault(key, []).append(record)
                            continue
                        raise CensusUnavailable("unreadable publication priority")
                    rows.setdefault(key, []).append(record)
                    if state == pool.CLAIMED and not is_mark:
                        # Every claimed action is an incumbent; only a sealed
                        # deadline contributes a finite opportunity (#1419).
                        governed, requested = pool._declared_run_bound(record, max_bytes=MAX_RECORD_BYTES)
                        opportunities[key] = {
                            "host": record.get("claimed_host"),
                            "requested": requested if governed == "deadline" else None,
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
            "opportunities": opportunities, "keys": sorted(set(measurements) | set(selected)),
            "gang_elections": _gang_elections(
                queue, {key: rows.get(key, []) + unorderable.get(key, [])
                        for key in rows.keys() | unorderable.keys()}, count)}


def _gang_elections(queue: PoolQueue, rows: dict[str, list[dict]], count: int) -> dict:
    """Live gang host elections (#1517), read in the same bounded child.

    A gang election fences its host while any member row of a gang without a
    teardown is still READY or CLAIMED. An unreadable or malformed gang
    record fails the census closed, exactly as a malformed publication does.
    Elections are written only by the elected host's own pass under its host
    admission, which also serializes this host's readers, so no member key
    joins the M lock set.
    """
    from . import _gang
    directory = _gang.root(queue)
    try:
        entries = os.scandir(directory)
    except FileNotFoundError:
        return {}  # gang admission was never used on this pool
    found: dict[str, dict] = {}
    try:
        with entries:
            for entry in entries:
                count += 1
                if count > MAX_RECORDS:
                    raise CensusUnavailable("measurement census record cap exceeded")
                if not entry.name.endswith(".json") or entry.name.startswith("."):
                    continue
                group = entry.name[:-5]
                record = _gang.read_group(queue, group)
                if record is None or _gang.teardown(queue, group) is not None:
                    continue
                live = [member for member in record["members"]
                        if any(float(row.get("published_unix", math.nan)) == member["published_unix"]
                               for row in rows.get(member["action_key"], []))]
                if not live:
                    continue
                rank = list(_gang.rank(record))
                for index, election in _gang.elections(queue, group, record["size"]).items():
                    found[election["action_key"]] = {
                        "group": group, "index": index, "action_key": election["action_key"],
                        "host": election["host"], "priority": election["priority"], "rank": rank}
    except _gang.GangContractError as exc:
        raise CensusUnavailable(str(exc)) from exc
    return found


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
            # Only acquiring the fence is a census failure. The caller's body
            # runs outside this translation: a body exception (a GPU sample
            # write in ``reserve_probe``, a claim error) keeps its own type
            # instead of becoming CensusUnavailable (#1506).
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
            except (OSError, ValueError, core.PrismaBuildError, reader.ReaderOwnershipUnavailable,
                    local_scratch.LocalScratchError) as exc:
                raise CensusUnavailable(str(exc)) from exc
            self._held = (directory, lock)
            try:
                yield
            finally:
                self._held = None
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
                    or set(value) != {"measurements", "elections", "selections", "opportunities", "keys",
                                      "gang_elections"}
                    or not all(isinstance(value[field], dict)
                               for field in ("measurements", "elections", "selections", "opportunities",
                                             "gang_elections"))
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


class PassCensus:
    """One successful census a claim pass reuses for later candidates (#1571).

    The census reads the whole queue, not the candidate, so a pass with a
    free fence paid two bounded-child scans per candidate: the discovery
    read outside host admission and the refresh read under it. Both run
    inside one held reader fence, and the refresh runs while the pass
    holds host admission and the elected measurement keys of this host.
    Reuse keeps the first successful census for the rest of the pass:
    later candidates re-take only those locks, never the fence or scan.

    Reuse ends where authority may have changed: this pass claims a row,
    elects a gang member on this host, or elects a measurement on this
    host; or a sibling loop's claim, gang election, or measurement
    election lands under a later candidate. Each of those is found by a
    fresh read, under that candidate's transition lock, before its
    rename. A change that cannot retire authority -- a denial, a pass
    count, a withhold -- changes nothing. Retirement itself needs an
    exact ending or a strictly newer live publication, both durable, so
    a census held across candidates of one pass only ever errs toward
    fencing: a missing row is not retirement, exactly as in
    :func:`locked_census`. A refusal is never reused: it ends the census
    for the pass as before.
    """

    def __init__(self, queue: PoolQueue, ledger: ResourceLedger, controller):
        self._queue = queue
        self._ledger = ledger
        self._controller = controller
        self._census: dict | None = None
        self._selections: dict | None = None
        self._gang_elections: dict | None = None
        self._busy: set[str] = set()

    @property
    def census(self) -> dict | None:
        """The census the pass reuses, or ``None`` before the first read."""
        return self._census

    def invalidate(self) -> None:
        """Drop the reused census after authority may have changed (#1571)."""
        self._census = None
        self._selections = None
        self._gang_elections = None
        self._busy = set()

    def store(self, census: dict) -> None:
        """Keep one successful locked census for the rest of the pass."""
        self._census = census
        self._selections = dict(census.get("selections") or {})
        self._gang_elections = dict(census.get("gang_elections") or {})

    def _refresh_dynamic_elections(self) -> bool:
        """Re-read live elections into the stored census (#1571).

        A sibling loop's gang or measurement election lands between
        this pass's candidates, while these locks are released. The
        stored elections would miss it, and the blocking checks would
        admit work a live election fences -- the unsafe direction. So
        every reuse re-reads the two small election sources (one
        listing plus one small read per live gang group, one listing
        plus one small read per passes sidecar, no fence, no scan).
        Election files are no-clobber writes, and a member row this
        pass is about to elect re-checks standing under the re-taken
        admission lock anyway. Only fences are added, never removed:
        a newly found measurement selection joins the stored
        selections and elections (missing authority stays fenced,
        exactly as in :func:`locked_census`), while a stored election
        whose measurement ended between candidates stays fenced until
        the next pass reads fresh. A malformed election record fails
        closed: the stored census is dropped and the caller reads
        fresh. ``False`` means just that.
        """
        from . import _gang
        from . import pool as pool_mod
        assert self._census is not None
        try:
            try:
                gang_names = sorted(
                    entry.name for entry in os.scandir(_gang.root(self._queue)))
            except FileNotFoundError:
                gang_names = []
            gang_found: dict[str, dict] = {}
            for name in gang_names:
                if not name.endswith(".json") or name.startswith("."):
                    continue
                group = name[:-5]
                record = _gang.read_group(self._queue, group)
                if record is None or _gang.teardown(self._queue, group) is not None:
                    continue
                rank = list(_gang.rank(record))
                for index, election in _gang.elections(
                        self._queue, group, record["size"]).items():
                    gang_found[election["action_key"]] = {
                        "group": group, "index": index, "action_key": election["action_key"],
                        "host": election["host"], "priority": election["priority"], "rank": rank}
            try:
                pass_names = sorted(
                    entry.name for entry in os.scandir(
                        self._queue.root / pool_mod.PASSES))
            except FileNotFoundError:
                pass_names = []
            selections = dict(self._selections or {})
            elections = dict((self._census.get("elections") or {}))
            for name in pass_names:
                if not name.endswith(".json") or not pool_mod._is_hex64(name[:-5]):
                    continue
                record = _read(self._queue.root / pool_mod.PASSES / name,
                               optional=True)
                if record is None:
                    continue
                chosen = selection(record)
                if chosen is None:
                    continue
                key = chosen["action_key"]
                if key not in selections:
                    selections[key] = chosen
                    elections[key] = chosen
        except (_gang.GangContractError, OSError, ValueError,
                CensusUnavailable, pool_mod.PoolContractError):
            self.invalidate()
            return False
        current = dict(self._census)
        current["gang_elections"] = gang_found
        current["selections"] = selections
        current["elections"] = elections
        self._census = current
        self._gang_elections = dict(gang_found)
        self._selections = selections
        return True

    def acquire(self) -> ExitStack | None:
        """Take the locks a reused census needs, without a scan (#1571).

        The reader fence and the queue scan stay untouched. Only the
        elected measurement keys of this host plus host admission are
        re-taken, exactly as :func:`locked_census` takes them for a
        fresh read. Returns the lock stack the caller holds across
        the candidate, or ``None`` when the locks did not come. A
        busy elected key is recorded for :meth:`reused` to keep as a
        live election, the conservative reading :func:`locked_census`
        already uses.
        """
        if self._census is None:
            return None
        if not self._refresh_dynamic_elections():
            return None
        from . import pool as pool_mod
        held = ExitStack()
        try:
            busy: set[str] = set()
            here = self._ledger.base.name
            for key in sorted(self._selections or {}):
                chosen = (self._selections or {}).get(key)
                if not isinstance(chosen, dict) or chosen.get("host") != here:
                    continue
                if not held.enter_context(
                        self._queue._transition_locked(key, blocking=False)):
                    busy.add(key)
            held.enter_context(
                self._queue._admission_lock(self._controller))
        except pool_mod.cpu_admission.AdmissionBusy:
            held.close()
            return None
        except OSError:
            held.close()
            return None
        self._busy = busy
        return held

    def reused(self) -> dict:
        """The stored census, with the locks :meth:`acquire` holds (#1571).

        Applies the same two adjustments :func:`locked_census` makes
        after its refresh read: an elected key another loop holds
        mid-transition stays a live election for this candidate, and a
        held elected key on this host fences it. Call only while the
        stack :meth:`acquire` returned is held.
        """
        current = dict(self._census or {})
        elections = dict(current.get("elections") or {})
        for key in self._busy:
            prior = (self._selections or {}).get(key)
            kept = current.get("selections", {}).get(key, prior)
            if kept is not None:
                elections[key] = kept
        for key in self._ledger.held_keys():
            chosen = current.get("selections", {}).get(key)
            if (isinstance(chosen, dict)
                    and chosen.get("host") == self._ledger.base.name):
                elections[key] = chosen
        current["elections"] = elections
        self._census = current
        return current


@contextmanager
def locked_census(queue: PoolQueue, ledger: ResourceLedger, controller):
    """Elections on this host (sorted/nonblocking) BEFORE H; refresh through acquire.

    Only a measurement elected for *this* host can fence its admission
    (``blocking_selection`` is host-filtered), so only those keys are locked.
    Locking every READY/CLAIMED measurement key instead turned a large
    measurement batch into a fleet-wide livelock: each of a box's worker loops
    holds its own candidate's transition lock through its pass, so almost
    every census met one busy key and denied every row on every host
    (2026-10-05, 38-49 READY PACT rows, 6 loops per Spark).

    An elected key another loop holds mid-transition is not a refusal: it is
    kept as a live election for this pass, the conservative reading, so lower
    priority work stays fenced and the pass still decides everything else.
    Unlocked reads only ever err toward fencing: a missing row is not
    retirement, and retirement needs an exact ending or a strictly newer
    publication, both durable. Election writes stay serialized as before: a
    measurement elects only in its own claim pass, under its own transition
    key and the elected host's H, and this refresh runs under this host's H.
    The candidate key may already be owned by the caller (the reentrant
    transition contract). The reader fence is acquired once for both phases
    (#1498); a fence or admission refusal releases what was taken.
    """
    census_reader = CensusReader(queue, ledger)
    here = ledger.base.name
    with census_reader.held():
        discovered = census_reader.capture()
        with ExitStack() as held:
            busy: set[str] = set()
            for key in sorted(key for key, chosen in discovered["selections"].items()
                              if chosen["host"] == here):
                if not held.enter_context(queue._transition_locked(key, blocking=False)):
                    busy.add(key)
            held.enter_context(queue._admission_lock(controller))
            current = census_reader.capture()
            for key in busy:
                # Mid-transition under another loop: fenced for this pass.
                chosen = current["selections"].get(key, discovered["selections"][key])
                current["elections"][key] = chosen
            # A success/cancellation slot is not physical ownership settlement.
            # Under H, retained tokens on this elected host still prevent refill.
            for key in ledger.held_keys():
                chosen = current["selections"].get(key)
                if chosen is not None and chosen["host"] == here:
                    current["elections"][key] = chosen
            yield current


@contextmanager
def pass_admission_census(pass_census: PassCensus, queue: PoolQueue,
                           ledger: ResourceLedger, controller):
    """One census for one candidate of a claim pass, scanned once (#1571).

    The first candidate reads through :func:`admission_census`: the
    reader fence plus the discovery scan outside host admission and
    the refresh scan under it, holding the elected measurement keys
    of this host and host admission for its body. That census is
    stored, and every later candidate re-takes only those locks --
    never the fence or a scan -- and reads the stored bytes.

    The locks are held for the whole candidate body either way, so a
    gang election, gang ready mark, or measurement election the body
    writes on this host serializes against the next candidate's
    lock take, exactly as today. A refusal is never stored: it ends
    the census for the pass as before, and the next pass reads
    fresh. Where authority may have changed -- this pass's own
    claim, gang election, or measurement election -- the caller
    invalidates the stored census and the next candidate reads
    fresh.
    """
    reused = pass_census.acquire()
    if reused is not None:
        with reused:
            yield pass_census.reused()
        return
    with admission_census(queue, ledger, controller) as census:
        if "unavailable" in census:
            pass_census.invalidate()
            yield census
            return
        pass_census.store(census)
        yield census


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


def gang_blocking(census: dict, item: dict, *, host: str, group: str | None) -> dict | None:
    """A live gang election fences its host against strictly lower priority (#1517).

    The same rule as :func:`blocking_selection`; the gang's own members are
    never fenced by their siblings' elections.
    """
    for key, chosen in sorted(census.get("gang_elections", {}).items()):
        if (chosen["host"] == host and key != item["action_key"] and chosen["group"] != group
                and int(item.get("priority", 0)) < chosen["priority"]):
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
        # Every incumbent must be a claimed action on this host, whose own
        # lifetime bounds the wait. A claimed action that declared no finite
        # deadline (``pbrun`` without ``--timeout-s``, a progress-governed
        # action) still elects: refusing left the host open to refill once
        # the bounded attention lapsed, and a continuous lower-priority stream
        # starved the measurement (#1419). A RAM-tier fill hold (#1222) or a
        # raw holder with no readable claim is not an action lifetime: no
        # election, exactly as before, so the bounded episode still lapses.
        holders = [key for key in ledger.held_keys()
                   if not key.startswith(storage_tiers.RAM_HOST_MEMORY_PREFIX)]
        if not holders or len(holders) != len(ledger.held_keys()):
            return None
        ends = []
        for key in holders:
            opportunity = census["opportunities"].get(key)
            claimed = opportunity.get("claimed_unix") if isinstance(opportunity, dict) else None
            if (opportunity is None or opportunity.get("host") != ledger.base.name
                    or not isinstance(claimed, (int, float)) or isinstance(claimed, bool)):
                return None
            requested = opportunity.get("requested")
            if isinstance(requested, (int, float)) and not isinstance(requested, bool):
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
