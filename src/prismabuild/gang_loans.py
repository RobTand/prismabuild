"""Admission policy for work borrowing a gang's elected capacity."""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
import math
from pathlib import Path

from . import _gang, core, fleet_roster


def preemptible_candidate(queue, item) -> bool:
    return (item.get("priority") == -10 and item.get("gang") is None
            and _gang.backfill_enabled() and queue._preemption_eligible(item))


ABSENCE_S = 600.0
OBSERVATION_GAP_S = 30.0
MAX_RUNTIME_S = 1800.0
LOAN_SCHEMA = "prismabuild.gang_bounded_loan.v1"


def bounded_candidate(item) -> bool:
    from . import pool
    action = pool._sealed_action_request(str(item.get("cas_root")), str(item["action_key"]))
    timeout = (action.get("params") or {}).get("execution_timeout_s")
    return (item.get("priority") == -10 and item.get("gang") is None
            and _gang.backfill_enabled() and action["task"]["task_class"] == "generation"
            and type(timeout) in (int, float) and math.isfinite(timeout)
            and 0 < timeout <= MAX_RUNTIME_S)


def slot_path(queue, group):
    return _gang.state_dir(queue, group) / "bounded-loan.json"


def loan_binding(election, item):
    return {"schema": LOAN_SCHEMA, "group": election["group"],
            "index": election["index"], "member": election["action_key"],
            "host": election["host"],
            "action_key": item["action_key"], "published_unix": item["published_unix"]}


def read_slot(queue, group):
    from . import pool
    return pool.record_store.read_confirmed_document(
        slot_path(queue, group), max_bytes=_gang.MAX_RECORD_BYTES)


def available(queue, election, item) -> bool:
    standing = read_slot(queue, election["group"])
    return standing is None or standing == loan_binding(election, item)


def pending(queue, group, index=None) -> bool:
    from . import pool
    slot = read_slot(queue, group)
    if slot is None or (index is not None and slot["index"] != index):
        return False
    key = slot["action_key"]
    with queue.action_transition_lock(key, blocking=False) as acquired:
        if not acquired:
            return True
        claimed = pool.record_store.value_or_raise(queue.records.item(pool.record_store.CLAIMED, key))
        return ((claimed is not None and claimed.get("published_unix") == slot["published_unix"])
                or key in queue.ledger(slot["host"]).held_keys())


def reserve(queue, elections, item, now) -> bool:
    """Serialize the bounded exception with every member's claim transition."""
    from . import pool
    bounded = [election for election in elections if election.get("bounded_loan")]
    if not bounded:
        return True
    with ExitStack() as locks:
        records = {election["group"]: _gang.read_group(queue, election["group"])
                   for election in bounded}
        if any(record is None for record in records.values()):
            return False
        keys = sorted({member["action_key"] for record in records.values()
                       for member in record["members"]})
        for key in keys:
            if not locks.enter_context(queue.action_transition_lock(key, blocking=False)):
                return False
        for election in bounded:
            record = records[election["group"]]
            if (any(state != pool.record_store.READY
                    for state in _gang.member_states(queue, record).values())
                    or not queue._gang_loans.allowed(queue, election, item, now)):
                return False
        for election in bounded:
            binding = loan_binding(election, item)
            path = slot_path(queue, election["group"])
            if not _gang._link_new(path, binding) and read_slot(queue, election["group"]) != binding:
                return False
    return True


@dataclass(frozen=True)
class Absence:
    identity: tuple
    since: float
    checked: float


class Observer:
    def __init__(self):
        self.windows: dict[tuple, Absence] = {}

    def observe(self, queue, ready, host, now):
        """No persisted interval survives a restart or an incomplete observation."""
        from . import pool
        fresh = {}
        try:
            groups = {declared["group"] for row in ready
                      if (declared := _gang.declaration(row.get("gang"))) is not None}
            if not groups:
                self.windows = {}
                return
            presence = fleet_roster.runtime_presence(Path(__file__).resolve().parents[2])
            if presence["state"] != "known":
                self.windows = {}
                return
            offers = []
            directory = queue.records.worker_directory()
            # Unlike placement discovery, a disappeared listed record is unknown.
            paths = list(directory.iterdir())
            for path in paths:
                if path.suffix != ".json":
                    continue
                offer = pool.record_store.value_or_raise(queue.records.listed_record(path))
                if (offer is None or not isinstance(offer.get("host"), str)
                        or not isinstance(offer.get("tags"), list)):
                    raise ValueError("incomplete worker offer observation")
                age = pool.offer_timing(offer.get("announced_unix"), now=now).age_s
                if age is None:
                    raise ValueError("unknown worker offer time")
                if age <= pool.OFFER_TIMEOUT_S:
                    offers.append(offer)
            for group in groups:
                record = _gang.read_group(queue, group)
                if record is None or _gang.teardown(queue, group) is not None:
                    continue
                elected = _gang.elections(queue, group, record["size"])
                if _gang.backfill_reclaiming(queue, record):
                    continue
                states = _gang.member_states(queue, record)
                if any(state != pool.record_store.READY for state in states.values()):
                    continue
                for index, election in elected.items():
                    if election["host"] != host:
                        continue
                    for member in record["members"]:
                        if member["index"] == index:
                            continue
                        row = pool.record_store.value_or_raise(queue.records.item(
                            pool.record_store.READY, member["action_key"]))
                        if row is None:
                            continue
                        declared = _gang.declaration(row.get("gang"))
                        if declared != {"group": group, "size": record["size"], "index": member["index"]}:
                            continue
                        _gang.member(record, row, declared)
                        partner = elected.get(member["index"], {}).get("host")
                        if partner is None:
                            targets = {presence["hosts"][tag]["canonical_host"]
                                       for tag in row.get("tags", []) if tag in presence["hosts"]}
                            if len(targets) != 1:
                                continue
                            partner = targets.pop()
                        runtime = presence["hosts"].get(partner)
                        if (runtime is None or runtime["state"] != "absent"
                                or runtime["unix"] > now
                                or any(presence["hosts"].get(offer["host"], {}).get(
                                    "canonical_host", offer["host"])
                                       == runtime["canonical_host"] for offer in offers)
                                or queue._matching_offers(row, live=offers)):
                            continue
                        key = group, index, member["index"]
                        identity = (member["action_key"], member["published_unix"],
                                    election["epoch_unix"], partner, presence["roster_sha256"])
                        prior = self.windows.get(key)
                        since = (prior.since if prior is not None and prior.identity == identity
                                 and 0 <= now - prior.checked <= OBSERVATION_GAP_S else now)
                        fresh[key] = Absence(identity, since, now)
        except (OSError, ValueError, KeyError, TypeError, core.PrismaBuildError):
            fresh = {}
        self.windows = fresh

    def allowed(self, queue, election, item, now) -> bool:
        if not bounded_candidate(item) or not available(queue, election, item):
            return False
        if not _gang.backfill_allowed(queue, election, now):
            return False
        return any(key[:2] == (election["group"], election["index"])
                   and 0 <= now - window.checked <= OBSERVATION_GAP_S
                   and now - window.since >= ABSENCE_S
                   for key, window in self.windows.items())
