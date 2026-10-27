"""A submitter's deadline ends only the original, wholly unstarted gang."""
from __future__ import annotations

from contextlib import ExitStack
import math

from . import _gang

STARTED_SCHEMA = "prismabuild.gang_started.v1"


def timeout(value):
    if value is None:
        return None
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise _gang.GangContractError("queue_wait_timeout_s must be finite and positive")
    return float(value)


def started(queue, record, now):
    if record.get("queue_wait_timeout_s") is not None:
        _gang._link_new(_gang.state_dir(queue, record["group"]) / "started.json",
                        {"schema": STARTED_SCHEMA, "group": record["group"],
                         "started_unix": now})


def cancel_expired(queue, record, now) -> bool:
    wait = timeout(record.get("queue_wait_timeout_s"))
    if wait is None:
        return False
    from . import pool
    group = record["group"]
    with ExitStack() as locks:
        for member in sorted(record["members"], key=lambda member: member["action_key"]):
            if not locks.enter_context(queue.action_transition_lock(member["action_key"], blocking=False)):
                return False
        if _gang._gang_read(_gang.state_dir(queue, group) / "started.json") is not None:
            return False
        originals = []
        for member in record["members"]:
            claimed = pool.record_store.value_or_raise(queue.records.item(
                pool.record_store.CLAIMED, member["action_key"]))
            if claimed is not None:
                # Even a delayed publication of the started mark cannot cancel a claim.
                started(queue, record, now)
                return False
            row = pool.record_store.value_or_raise(queue.records.item(
                pool.record_store.READY, member["action_key"]))
            if row is None or row.get("published_unix") != member["published_unix"]:
                return False
            declared = _gang.declaration(row.get("gang"))
            if declared != {"group": group, "size": record["size"], "index": member["index"]}:
                return False
            _gang.member(record, row, declared)
            originals.append(row)
        deadline = max(member["published_unix"] for member in record["members"]) + wait
        if now < deadline:
            return False
        reason = "submitter queue-wait deadline expired"
        # File terminal gang evidence before withdrawal. The ordinary withdrawal
        # then cannot fan out an unscoped sibling teardown of its own.
        _gang.tear_down(queue, group, reason=reason, by=f"gang-wait:{group}", now=now)
        for row in originals:
            if queue.withdrawal_covers(row) is None:
                queue.withdraw(row["action_key"], reason=reason, by=f"gang-wait:{group}")
        return True


def poll(queue, ready, now):
    from . import core
    groups = {declared["group"] for row in ready
              if (declared := _gang.declaration(row.get("gang"))) is not None}
    for group in groups:
        try:
            record = _gang.read_group(queue, group)
            if record is not None:
                cancel_expired(queue, record, now)
        except (OSError, ValueError, KeyError, TypeError, core.PrismaBuildError):
            continue
