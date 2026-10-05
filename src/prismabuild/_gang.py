"""Gang reservation: admit N sealed actions together or not at all (#1517).

Default off. A member seals ``params.gang = {group, size, index}`` and its row
requires the worker tag :data:`TAG`, which a worker offers only when gang
admission is enabled. The publisher files one group record naming every
member; nothing is claimable before it exists.

Per member host the claim pass (``PoolQueue._claim_pass``):

1. elects the host for the member under host admission (H), which fences
   strictly lower-priority work there through the census, exactly like a
   #1419 measurement election;
2. after every ordinary gate passes, marks the member ready and abandons the
   acquisition unless every sibling is fresh-ready or claimed -- a ready
   member holds no tokens, so no cross-host hold can deadlock two gangs;
3. commits with the ordinary rename once the whole set is ready.

State lives under ``pb-queue/gangs/``: ``<group>.json`` (immutable record),
``<group>/elect-<i>.json`` (no-clobber host election), ``<group>/ready-<i>.json``
(refreshed every pass) and ``<group>/teardown.json``.
"""
from __future__ import annotations

from collections.abc import Mapping
import math
import os
from pathlib import Path
import uuid

from . import core

TAG = "gang-v1"
GANGS = "gangs"
GROUP_SCHEMA = "prismabuild.gang_group.v1"
ELECT_SCHEMA = "prismabuild.gang_election.v1"
READY_SCHEMA = "prismabuild.gang_ready.v1"
TEARDOWN_SCHEMA = "prismabuild.gang_teardown.v1"
#: A ready mark older than this no longer counts toward commit. Every claim
#: pass that finds the member admissible refreshes it.
READY_FRESH_S = 30.0
DEFAULT_SKEW_S = 120.0
MAX_MEMBERS = 16
MAX_RECORD_BYTES = 64 * 1024


class GangContractError(ValueError):
    """A gang declaration, record or mark that cannot be trusted."""


def _is_hex(text: object, length: int) -> bool:
    return (isinstance(text, str) and len(text) == length
            and all(c in "0123456789abcdef" for c in text))


def declaration(value: object) -> dict | None:
    """Validate a ``params.gang`` / row ``gang`` declaration, or ``None``."""
    if value is None:
        return None
    if (not isinstance(value, Mapping) or set(value) != {"group", "size", "index"}
            or not _is_hex(value["group"], 32)
            or type(value["size"]) is not int or type(value["index"]) is not int
            or not 2 <= value["size"] <= MAX_MEMBERS
            or not 0 <= value["index"] < value["size"]):
        raise GangContractError(f"malformed gang declaration: {value!r}")
    return {"group": value["group"], "size": value["size"], "index": value["index"]}


def sealed(item: Mapping[str, object]) -> dict | None:
    """The member's sealed declaration; it must equal the row's hint."""
    from . import pool
    row = declaration(item.get("gang"))
    action = pool._sealed_action_request(str(item.get("cas_root")), str(item["action_key"]),
                                         max_bytes=MAX_RECORD_BYTES * 16)
    params = action.get("params") if isinstance(action, Mapping) else None
    sealed_value = declaration(params.get("gang") if isinstance(params, Mapping) else None)
    if sealed_value != row:
        raise GangContractError("row gang declaration does not match the sealed request")
    return sealed_value


def root(queue) -> Path:
    return queue.root / GANGS


def group_path(queue, group: str) -> Path:
    return root(queue) / f"{group}.json"


def state_dir(queue, group: str) -> Path:
    return root(queue) / group


def _read(path: Path, *, optional: bool = True) -> dict | None:
    try:
        raw = core._read_regular_file_nofollow(path, where="gang record",
                                               max_bytes=MAX_RECORD_BYTES)
    except FileNotFoundError:
        if optional:
            return None
        raise
    value = core._decode_strict_json(raw, where="gang record")
    if not isinstance(value, dict):
        raise GangContractError(f"non-object gang record: {path}")
    return value


def _link_new(path: Path, payload: Mapping[str, object]) -> bool:
    """Publish ``payload`` at ``path`` only if nothing is there yet."""
    from . import pool
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}")
    pool._write_json_atomic(temporary, payload)
    try:
        os.link(temporary, path)
        return True
    except FileExistsError:
        return False
    finally:
        temporary.unlink(missing_ok=True)


def publish_group(queue, group: str, members: list[Mapping[str, object]], *,
                  skew_s: float = DEFAULT_SKEW_S) -> dict:
    """File the immutable group record once every member row is published.

    ``members`` are the published rows (``action_key``, ``published_unix``,
    ``priority``, ``gang``). Each row's declaration must name this group, the
    same size and its own distinct index.
    """
    if not _is_hex(group, 32):
        raise GangContractError("gang group must be 32 lowercase hex")
    if not (isinstance(skew_s, (int, float)) and not isinstance(skew_s, bool)
            and math.isfinite(skew_s) and 0 < skew_s <= 3600):
        raise GangContractError("gang skew_s must be finite in (0, 3600]")
    entries = []
    for row in members:
        gang = declaration(row.get("gang"))
        if gang is None or gang["group"] != group or gang["size"] != len(members):
            raise GangContractError("member row does not declare this gang")
        entries.append({"index": gang["index"], "action_key": str(row["action_key"]),
                        "published_unix": float(row["published_unix"]),  # type: ignore[arg-type]
                        "priority": int(row.get("priority", 0))})  # type: ignore[call-overload]
    entries.sort(key=lambda entry: entry["index"])
    if [entry["index"] for entry in entries] != list(range(len(members))):
        raise GangContractError("gang member indexes must be 0..size-1, once each")
    record = {"schema": GROUP_SCHEMA, "group": group, "size": len(members),
              "skew_s": float(skew_s), "members": entries,
              "priority": max(entry["priority"] for entry in entries)}
    if not _link_new(group_path(queue, group), record):
        if read_group(queue, group) != record:
            raise GangContractError("a different record already names this gang group")
    return record


def read_group(queue, group: str) -> dict | None:
    record = _read(group_path(queue, group))
    if record is None:
        return None
    members = record.get("members")
    if (record.get("schema") != GROUP_SCHEMA or record.get("group") != group
            or not isinstance(members, list) or record.get("size") != len(members)
            or not 2 <= len(members) <= MAX_MEMBERS
            or [m.get("index") if isinstance(m, Mapping) else None for m in members]
            != list(range(len(members)))
            or any(not _is_hex(m.get("action_key"), 64) for m in members)
            or type(record.get("priority")) is not int):
        raise GangContractError(f"malformed gang group record {group}")
    return record


def member(record: Mapping[str, object], item: Mapping[str, object], gang: Mapping[str, object]) -> dict:
    """This row's entry in its group record; identity must match exactly."""
    entry = record["members"][gang["index"]]  # type: ignore[index]
    if (entry["action_key"] != item["action_key"]
            or float(entry["published_unix"]) != float(item["published_unix"])):  # type: ignore[arg-type]
        raise GangContractError("row is not the member its group record names")
    return entry


def teardown(queue, group: str) -> dict | None:
    return _read(state_dir(queue, group) / "teardown.json")


def elections(queue, group: str, size: int) -> dict[int, dict]:
    found = {}
    for index in range(size):
        record = _read(state_dir(queue, group) / f"elect-{index}.json")
        if record is None:
            continue
        if (record.get("schema") != ELECT_SCHEMA or record.get("group") != group
                or record.get("index") != index or not isinstance(record.get("host"), str)
                or not _is_hex(record.get("action_key"), 64)
                or type(record.get("priority")) is not int):
            raise GangContractError(f"malformed gang election {group}/{index}")
        found[index] = record
    return found


def elect(queue, record: Mapping[str, object], entry: Mapping[str, object], host: str,
          now: float) -> dict:
    """No-clobber host election for one member; returns the standing one."""
    election = {"schema": ELECT_SCHEMA, "group": record["group"], "index": entry["index"],
                "action_key": entry["action_key"], "host": host,
                "priority": int(record["priority"]), "epoch_unix": float(now)}  # type: ignore[call-overload]
    path = state_dir(queue, str(record["group"])) / f"elect-{entry['index']}.json"
    if _link_new(path, election):
        return election
    standing = elections(queue, str(record["group"]), int(record["size"])).get(int(entry["index"]))  # type: ignore[call-overload]
    if standing is None:
        raise GangContractError("gang election vanished after a lost race")
    return standing


def mark_ready(queue, record: Mapping[str, object], entry: Mapping[str, object],
               host: str, now: float) -> None:
    from . import pool
    pool._write_json_atomic(
        state_dir(queue, str(record["group"])) / f"ready-{entry['index']}.json",
        {"schema": READY_SCHEMA, "group": record["group"], "index": entry["index"],
         "action_key": entry["action_key"], "host": host, "ready_unix": float(now)})


def sibling_readiness(queue, record: Mapping[str, object], entry: Mapping[str, object],
                      host: str, now: float) -> dict:
    """Whether every sibling is claimed or fresh-ready on a distinct host."""
    from . import pool
    waiting = []
    hosts = {host}
    for other in record["members"]:  # type: ignore[union-attr]
        if other["index"] == entry["index"]:
            continue
        claimed = _read(queue.item_path(pool.CLAIMED, other["action_key"]))
        if (claimed is not None and claimed.get("action_key") == other["action_key"]
                and float(claimed.get("published_unix", math.nan)) == float(other["published_unix"])):
            other_host = claimed.get("claimed_host")
            state = "claimed"
        else:
            ready = _read(state_dir(queue, str(record["group"])) / f"ready-{other['index']}.json")
            fresh = (ready is not None and ready.get("schema") == READY_SCHEMA
                     and ready.get("action_key") == other["action_key"]
                     and isinstance(ready.get("ready_unix"), (int, float))
                     and 0 <= now - float(ready["ready_unix"]) <= READY_FRESH_S)
            other_host = ready.get("host") if fresh else None  # type: ignore[union-attr]
            state = "ready" if fresh else "waiting"
        if state == "waiting" or not isinstance(other_host, str) or other_host in hosts:
            waiting.append({"index": other["index"], "action_key": other["action_key"][:12],
                            "state": state, "host": other_host})
            continue
        hosts.add(other_host)
    return {"complete": not waiting, "waiting": waiting}
