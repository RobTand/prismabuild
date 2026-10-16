"""Standard-library-only maintenance gate and park-marker operations."""
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path

_DRAIN_OWNER_LIMIT = 200
_DRAIN_REASON_LIMIT = 1000
HOST_QUIESCENT = frozenset({"worker_loop.py", "worker.py", "prewarm_loop.py"})
MEMBER_LOOPS = frozenset({"worker_loop.py"})


@dataclass(frozen=True)
class DrainIdentity:
    key: str


@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    starttime: str


@dataclass(frozen=True)
class ProofUnknown:
    reason: str


@dataclass(frozen=True)
class ParkAcknowledgements:
    unparked: tuple[int, ...] = ()
    unknown: tuple[str, ...] = ()

    @property
    def complete(self) -> bool:
        return not self.unparked and not self.unknown


def drain_identity(gate: object) -> DrainIdentity | ProofUnknown:
    """Parse a closed drain into its exact, injective marker key."""
    if not isinstance(gate, dict) or gate.get("draining") is not True:
        return ProofUnknown("maintenance gate is not explicitly closed")
    stamp = gate.get("changed_unix")
    if type(stamp) not in (int, float):
        return ProofUnknown("maintenance gate has no numeric drain identity")
    try:
        finite = math.isfinite(stamp)
    except OverflowError:
        finite = False
    if not finite:
        return ProofUnknown("maintenance gate drain identity is not finite")
    key = str(stamp).replace("e-", "em").replace("e+", "ep")
    if key.startswith("-"):
        key = "_" + key[1:]
    return DrainIdentity(key)


def process_identity(pid: object, starttime: object) -> ProcessIdentity | ProofUnknown:
    """Parse PID and Linux start ticks without coercing unknown evidence."""
    if type(pid) is not int or pid <= 0:
        return ProofUnknown("process PID is not a positive integer")
    if (not isinstance(starttime, str) or not starttime
            or not starttime.isascii() or not starttime.isdecimal()):
        return ProofUnknown("process start ticks are not ASCII decimal digits")
    return ProcessIdentity(pid, starttime)


def park_marker_filename(process: ProcessIdentity, drain: DrainIdentity) -> str:
    return f"{process.pid}-{process.starttime}-{drain.key}"


def acknowledgements(*, processes: list[tuple[int, str]], markers: set[str],
                     gate_before: object, gate_after: object) -> ParkAcknowledgements:
    """A selected census acknowledges only a known, unchanged physical drain."""
    before = drain_identity(gate_before)
    after = drain_identity(gate_after)
    if isinstance(before, ProofUnknown):
        return ParkAcknowledgements(unknown=(before.reason,))
    if isinstance(after, ProofUnknown):
        return ParkAcknowledgements(unknown=(after.reason,))
    if before != after:
        return ParkAcknowledgements(unknown=("maintenance gate changed during census",))
    unparked = []
    unknown = []
    for pid, starttime in processes:
        process = process_identity(pid, starttime)
        if isinstance(process, ProofUnknown):
            unknown.append(process.reason)
        elif park_marker_filename(process, before) not in markers:
            unparked.append(process.pid)
    return ParkAcknowledgements(tuple(unparked), tuple(unknown))



def read_gate(path: Path) -> dict | None:
    """The drain in force on this box, or ``None`` when there is none.

    Split out of :func:`maintenance_requested` so a caller can read the
    drain's ``changed_unix`` without restating the rules below:

    * a missing gate means admission has not been initialized after boot;
    * a gate that cannot be read or parsed means draining, because the other
      reading admits work on a parse error;
    * a value that is not an object means draining, for the same reason;
    * ``draining`` anything other than ``False`` means draining.

    An open gate returns ``None`` rather than the parsed object, so a caller
    testing truthiness cannot mistake ``{"draining": false}`` for a stop.
    """

    try:
        value = json.loads(path.read_text())
    except FileNotFoundError:
        return {"draining": True, "reason": "maintenance gate not initialized"}
    except (OSError, ValueError):
        return {"draining": True}
    if not isinstance(value, dict):
        return {"draining": True}
    return None if value.get("draining") is False else value


def drain_offer_fields(gate: dict) -> dict:
    """The offer fields that keep a parked loop visible in the census (#1204).

    ``read_maintenance_gate`` returns a gate for every state that means
    "closed" -- including a missing or unparsable file, where it synthesizes
    the reason -- so ``state`` is always ``draining`` here.  ``owner``,
    ``reason`` and ``changed_unix`` are copied only when they have the shape a
    reader can use, and are bounded; a missing or malformed field is omitted
    rather than rendered, and the record still says the box is draining.
    ``changed_unix`` names one drain, which is what lets a reader match this
    offer to the gate that produced it; ``reason`` is descriptive text and is
    deliberately not identity.
    """

    fields = {"state": "draining"}
    owner = gate.get("owner")
    if isinstance(owner, str) and owner:
        fields["drain_owner"] = owner[:_DRAIN_OWNER_LIMIT]
    reason = gate.get("reason")
    if isinstance(reason, str) and reason:
        fields["drain_reason"] = reason[:_DRAIN_REASON_LIMIT]
    stamp = gate.get("changed_unix")
    if type(stamp) in (int, float):
        try:
            stamp = float(stamp)
        except OverflowError:
            stamp = None
        if stamp is not None and math.isfinite(stamp):
            fields["drain_changed_unix"] = stamp
    return fields


def process_starttime(pid: int) -> str:
    """Field 22 of ``/proc/<pid>/stat``, which makes a marker survive pid reuse.

    ``comm`` is field 2, is parenthesised, and may itself contain spaces and
    parentheses, so the fields are counted from the last ``)`` rather than by
    splitting the line.
    """

    try:
        line = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return "unknown"
    _, _, rest = line.rpartition(")")
    fields = rest.split()
    return fields[19] if len(fields) > 19 else "unknown"


def gate_key(gate: object) -> str:
    """The display key for this drain; ``unknown`` is never proof identity."""
    identity = drain_identity(gate)
    return identity.key if isinstance(identity, DrainIdentity) else "unknown"


def parked_markers(parked_root: Path, gate: dict | None) -> list[Path]:
    """Markers naming exactly this drain epoch (old epochs never count)."""
    drain = drain_identity(gate)
    if isinstance(drain, ProofUnknown):
        return []
    if not parked_root.is_dir():
        return []
    accepted = []
    try:
        for path in parked_root.glob(f"*-{drain.key}"):
            fields = path.name.split("-", 2)
            if (len(fields) != 3 or not fields[0].isascii()
                    or not fields[0].isdecimal()):
                continue
            process = process_identity(int(fields[0]), fields[1])
            if (isinstance(process, ProcessIdentity)
                    and park_marker_filename(process, drain) == path.name and path.is_file()):
                accepted.append(path)
    except OSError:
        return []
    return sorted(accepted)


def expected_markers(census: list[tuple[int, str]],
                     gate: dict | None) -> set[str] | ProofUnknown:
    """Full marker names each live loop must post (pid + starttime + epoch)."""
    drain = drain_identity(gate)
    if isinstance(drain, ProofUnknown):
        return drain
    expected = set()
    for pid, starttime in census:
        process = process_identity(pid, starttime)
        if isinstance(process, ProofUnknown):
            return process
        expected.add(park_marker_filename(process, drain))
    return expected


def write_park_marker(root: Path, gate: dict) -> Path | None:
    """Record that this process is parked on ``gate``, once, and return the path.

    Best effort by contract, and the direction of the failure is the point: a
    loop that cannot write the marker parks anyway, the host then reads as not
    drained, and whatever is waiting on the drain keeps waiting.  The opposite
    arrangement would let a box that failed to record itself pass for stopped.

    Reposting on every poll is idempotent: the name is fixed by the
    pid and the drain, and the create is exclusive.
    """

    drain = drain_identity(gate)
    if isinstance(drain, ProofUnknown):
        return None
    pid = os.getpid()
    process = process_identity(pid, process_starttime(pid))
    if isinstance(process, ProofUnknown):
        return None
    marker = root / park_marker_filename(process, drain)
    try:
        os.close(os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644))
    except FileExistsError:
        return marker
    except OSError:
        return None
    return marker
