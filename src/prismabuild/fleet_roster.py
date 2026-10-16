"""Presence states for boxes in ``tools/fleet/fleet_boxes.json`` (#606).

A roster box that is gone still vetoes every barrier publish: the attestation
preflight demands the target agent's version from *every* declared box, so one
box 69 h silent and unreachable (``wsl-gpu`` on 2026-09-18) blocks the whole
fleet, and the only exit is a hand-written rolling reason that installs
nothing on the missing box.

The fix is a declared presence, not a liveness inference.  Each box carries
an optional ``status``:

* ``"active"`` (the default when absent): the box is offered, attests, and
  counts toward every barrier quorum.
* ``"retired"``: the box is gone for good; it is excluded from the
  attestation preflight, the barrier roster, and placement (its supervisor
  converges its loops to zero and starts none).
* ``"offline"``: the same exclusion, but the box is expected back; the
  operator clears the state by publishing a roster without it.

A non-active box must say who declared it absent and when: ``status_reason``
(non-blank), ``status_by`` (non-blank) and ``status_unix`` (a finite
number) are all required.  An unknown ``status`` value, or a non-active box
without its provenance, refuses loudly wherever the roster is read -- an
ambiguous presence fails closed, never as "active".

This module is the single reader of those fields.  ``publish_runtime`` (the
barrier side) and ``supervise`` (the placement side) both call it rather than
keeping a second copy of the spelling.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

from .digest_primitives import raw_sha256
import re
from collections.abc import Mapping


ACTIVE = "active"
RETIRED = "retired"
OFFLINE = "offline"

#: Every presence a roster box may declare.  Absent means active.
STATUSES = (ACTIVE, RETIRED, OFFLINE)

#: States excluded from barrier quorums, attestation preflight and placement.
ABSENT = (RETIRED, OFFLINE)


class RosterPresenceError(ValueError):
    """A roster box whose presence cannot be established."""


@dataclass(frozen=True)
class Roster:
    """One exact roster capture; layout, decoding and policy belong here."""

    path: Path
    raw: bytes
    sha256: str
    document: dict

    @classmethod
    def read(cls, path: str | Path, *, expected_sha256: str | None = None) -> Roster:
        path = Path(path)
        return cls.capture(path.read_bytes(), path=path, expected_sha256=expected_sha256)

    @classmethod
    def capture(cls, raw: bytes, *, path: Path = Path("<roster>"),
                expected_sha256: str | None = None) -> Roster:
        digest = raw_sha256(raw)
        if expected_sha256 is not None and digest != expected_sha256:
            raise ValueError("roster bytes differ from publication manifest")

        def pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise ValueError(f"duplicate roster key {key!r}")
                result[key] = value
            return result

        def constant(value):
            raise ValueError(f"nonfinite roster number {value}")

        def finite(value):
            number = float(value)
            if not math.isfinite(number):
                constant(value)
            return number

        document = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                              parse_constant=constant, parse_float=finite)
        if not isinstance(document, dict):
            raise ValueError("fleet roster must be a JSON object")
        return cls(Path(path), raw, digest, document)

    @staticmethod
    def paths(root: str | Path, *, flat_first: bool = False) -> tuple[Path, ...]:
        root = Path(root)
        nested = root / "tools" / "fleet" / "fleet_boxes.json"
        flat = root / "tools" / "fleet_boxes.json"
        return (flat, nested) if flat_first else (nested, flat)

    @classmethod
    def at(cls, root: str | Path, *, flat_first: bool = False) -> Roster:
        for path in cls.paths(root, flat_first=flat_first):
            try:
                return cls.read(path)
            except OSError:
                continue
        raise FileNotFoundError(f"no readable fleet roster under {root}")

    @classmethod
    def available(cls, *, roots=(), paths=(), flat_first: bool = False):
        """Readable boxed captures in deliberate layout/fallback order."""
        candidates = [p for root in roots for p in cls.paths(root, flat_first=flat_first)]
        for path in (*candidates, *paths):
            try:
                roster = cls.read(path)
                roster.boxes
            except (OSError, ValueError):
                continue
            yield roster

    @property
    def boxes(self) -> dict:
        boxes = self.document.get("boxes")
        if not isinstance(boxes, dict):
            raise ValueError("fleet roster must declare boxes as an object")
        return boxes

    @property
    def d38_enforce(self) -> bool:
        value = self.document.get("d38_enforce", False)
        if type(value) is not bool:
            raise ValueError("d38_enforce must be a JSON boolean")
        return value

    @property
    def gpu_interpreters(self) -> tuple[str, ...]:
        value = self.document.get("gpu_interpreters", [])
        if (not isinstance(value, list)
                or any(not isinstance(p, str) or not p.startswith("/") for p in value)):
            raise ValueError("gpu_interpreters must be an array of absolute paths")
        return tuple(value)

    def disk_floor_percent(self, entry: Mapping | None = None) -> int | None:
        field = "local_disk_free_floor_percent"
        value = (entry if entry is not None and field in entry else self.document).get(field)
        if value is not None and (type(value) is not int or not 0 <= value < 100):
            raise ValueError(f"{field} must be a whole percent from 0 to 99 (found {value!r})")
        return value

    def canonical_box(self, host: str) -> Box | None:
        entry = entry_for_host(self.boxes, host)
        if entry is None:
            return None
        key = next(key for key, value in self.boxes.items() if value is entry)
        return Box(key, entry, self)

    @classmethod
    def for_host(cls, host: str, *, roots=(), paths=(), flat_first: bool = False) -> Box | None:
        for roster in cls.available(roots=roots, paths=paths, flat_first=flat_first):
            box = roster.canonical_box(host)
            if box is not None:
                return box
        return None

    def declarations(self) -> list[tuple[str, frozenset[str], dict]]:
        if not self.boxes:
            raise ValueError("no boxes declared")
        return [(key, names, self.boxes[key])
                for key, names in generation_aliases(self.boxes).items()]

    def validate_publication(self) -> None:
        """Reject newly published bytes before strict generation readers use them."""
        self.d38_enforce
        self.gpu_interpreters
        self.disk_floor_percent()
        if "boxes" in self.document:
            boxes = self.boxes
            generation_aliases(boxes)
            for key, entry in boxes.items():
                if not isinstance(entry, dict):
                    raise ValueError(f"invalid roster box {key!r}")
                box_status(key, entry)
                box = Box(key, entry, self)
                box.disk_floor_percent
                box.loop_args
                box.loops
                box.roles


@dataclass(frozen=True)
class Box:
    """A canonical declaration and the exact capture supplying its defaults."""

    key: str
    entry: dict
    roster: Roster

    @property
    def loop_args(self) -> list[str]:
        args = self.entry.get("args") or []
        if not isinstance(args, list) or any(not isinstance(a, str) for a in args):
            raise ValueError(f"roster box {self.key!r} args must be an array of strings")
        return list(args)

    @property
    def loops(self) -> int:
        value = self.entry.get("loops", 1)
        if type(value) is not int or value < 0:
            raise ValueError(f"roster box {self.key!r} loops must be a nonnegative integer")
        return value

    @property
    def roles(self) -> dict[str, list[str]]:
        roles = self.entry.get("roles") or {}
        if not isinstance(roles, dict):
            raise ValueError(f"fleet_boxes.json roles for {self.key} must be an object")
        if any(not isinstance(args, list) or any(not isinstance(a, str) for a in args)
               for args in roles.values()):
            raise ValueError(f"roster box {self.key!r} role argv must be arrays of strings")
        return {name: list(args) for name, args in roles.items()}

    @property
    def disk_floor_percent(self) -> int | None:
        return self.roster.disk_floor_percent(self.entry)


def box_status(key: str, entry: object) -> tuple[str, dict[str, object]]:
    """The presence of roster box ``key``, and its declaration detail.

    Returns ``(status, detail)`` where ``detail`` carries ``reason``, ``by``
    and ``unix`` for a non-active box and is empty for an active one.
    Raises :class:`RosterPresenceError` for an unknown status or a
    non-active box without full provenance -- the caller turns that into the
    refusal its own layer owes (``SystemExit`` for the CLIs).
    """

    status: object = ACTIVE
    if isinstance(entry, Mapping):
        status = entry.get("status", ACTIVE)
    if status not in STATUSES:
        raise RosterPresenceError(
            f"roster box {key!r} declares unknown status {status!r}; "
            f"expected one of {list(STATUSES)}"
        )
    if status == ACTIVE:
        return ACTIVE, {}
    assert isinstance(entry, Mapping)
    reason = entry.get("status_reason")
    by = entry.get("status_by")
    unix = entry.get("status_unix")
    problems = []
    if not isinstance(reason, str) or not reason.strip():
        problems.append("status_reason must be a nonblank string")
    if not isinstance(by, str) or not by.strip():
        problems.append("status_by must be a nonblank string")
    if (not isinstance(unix, (int, float)) or isinstance(unix, bool)
            or not math.isfinite(unix)):
        problems.append("status_unix must be a finite number")
    if problems:
        raise RosterPresenceError(
            f"roster box {key!r} declares status {status!r} without provenance: "
            + "; ".join(problems)
        )
    return str(status), {
        "reason": str(reason).strip(),
        "by": str(by).strip(),
        "unix": float(unix),
    }


def is_absent(key: str, entry: object) -> bool:
    """Whether roster box ``key`` is declared absent (retired or offline)."""

    status, _ = box_status(key, entry)
    return status in ABSENT


def describe_absent(key: str, detail: Mapping[str, object]) -> str:
    """One legible line naming who declared the absence and when."""

    return (f"{key}: declared absent ({detail.get('reason')}) "
            f"by {detail.get('by')} at {detail.get('unix')}")


def class_members(roster: object, klass: str) -> dict[str, tuple[str, ...]]:
    """Distinct active class members; canonical names and aliases cannot overlap."""
    if not isinstance(roster, Mapping) or not isinstance(roster.get("boxes"), Mapping):
        raise RosterPresenceError("fleet roster must declare boxes")
    members = {}
    owners = {}
    for key, entry in roster["boxes"].items():
        if not isinstance(entry, Mapping):
            raise RosterPresenceError(f"invalid roster box {key!r}")
        if is_absent(key, entry):
            continue
        args = entry.get("args")
        if not isinstance(args, list) or "--class" not in args:
            raise RosterPresenceError(f"roster box {key!r} has no explicit class")
        index = args.index("--class") + 1
        if index >= len(args):
            raise RosterPresenceError(f"roster box {key!r} has no class value")
        if args[index] == klass:
            alias = entry.get("_alias", key)
            if not isinstance(alias, str) or not alias:
                raise RosterPresenceError(f"roster box {key!r} has an invalid alias")
            names = tuple(dict.fromkeys((key, alias)))
            for name in names:
                if name in owners:
                    raise RosterPresenceError(
                        f"roster class {klass!r} name {name!r} overlaps "
                        f"distinct boxes {owners[name]!r} and {key!r}")
                owners[name] = key
            members[key] = names
    return members



def generation_aliases(
        boxes: Mapping[str, object]) -> dict[str, frozenset[str]]:
    """Declared names per canonical box, refusing distinct owners of a name."""

    declared = {}
    owners = {}
    for key in sorted(boxes):
        entry = boxes[key]
        names = {key}
        alias = entry.get("_alias") if isinstance(entry, dict) else None
        if isinstance(alias, str) and alias:
            names.add(alias)
        for name in sorted(names):
            if name in owners:
                raise RosterPresenceError(
                    f"ambiguous fleet hostname alias {name!r}: distinct roster "
                    f"declarations {owners[name]!r} and {key!r}")
            owners[name] = key
        declared[key] = frozenset(names)
    return declared


def entry_for_host(boxes: Mapping[str, object], host: str) -> dict | None:
    """Prefer a canonical entry; otherwise require one declared alias match."""
    if host in boxes and isinstance(boxes[host], dict):
        return boxes[host]
    aliases = [shape for shape in boxes.values()
               if isinstance(shape, dict) and shape.get("_alias") == host]
    if len(aliases) > 1:
        raise RosterPresenceError(f"ambiguous fleet hostname alias {host}")
    return aliases[0] if aliases else None


def recorder_hostnames(boxes: Mapping[str, object], host: str) -> list[str]:
    """Names whose recorder evidence belongs to one unambiguous machine."""
    names = {host}
    groups = []
    for name, shape in boxes.items():
        group = {name}
        if isinstance(shape, dict) and shape.get("_alias") is not None:
            alias = shape["_alias"]
            if not isinstance(alias, str):
                if name == host:
                    raise ValueError(f"invalid recorder hostname alias for {host}")
                continue
            group.add(alias)
        groups.append(group)
    matches = [group for group in groups if host in group]
    if matches:
        names = matches[0]
        if sum(bool(names & group) for group in groups) != 1:
            raise ValueError(f"ambiguous fleet hostname alias {host}")
    if any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name) for name in names):
        raise ValueError("invalid recorder hostname")
    return sorted(names)
