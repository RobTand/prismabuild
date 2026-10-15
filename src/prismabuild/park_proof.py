"""Standard-library-only maintenance gate and park-marker operations."""
import json
import math
import os
from pathlib import Path

_KEY_SAFE = frozenset("0123456789abcdefghijklmnopqrstuvwxyz"
                      "ABCDEFGHIJKLMNOPQRSTUVWXYZ._")
_DRAIN_OWNER_LIMIT = 200
_DRAIN_REASON_LIMIT = 1000



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


def _proc_starttime(pid: int) -> str:
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


def gate_key(gate: dict) -> str:
    """This drain's identity, as the broker already stamps it.

    ``maintenance_begin`` stamps a new drain with a fresh ``changed_unix``
    and preserves it while that drain remains held, so the field names
    this drain and not the last one, and a reader matching on it reads a marker
    left by an earlier drain as what it is. ``reason`` is descriptive text,
    not the identity of a drain.

    A gate with no identity gets a key no reader matches, which leaves the host
    reading as not drained.
    """

    value = gate.get("changed_unix") if isinstance(gate, dict) else None
    if value is None:
        return "unknown"
    return "".join(c if c in _KEY_SAFE else "_" for c in str(value))


def write_park_marker(root: Path, gate: dict) -> Path | None:
    """Record that this process is parked on ``gate``, once, and return the path.

    Best effort by contract, and the direction of the failure is the point: a
    loop that cannot write the marker parks anyway, the host then reads as not
    drained, and whatever is waiting on the drain keeps waiting.  The opposite
    arrangement would let a box that failed to record itself pass for stopped.

    Reposting on every poll is idempotent: the name is fixed by the
    pid and the drain, and the create is exclusive.
    """

    marker = root / (f"{os.getpid()}-{_proc_starttime(os.getpid())}"
                            f"-{gate_key(gate)}")
    try:
        os.close(os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644))
    except FileExistsError:
        return marker
    except OSError:
        return None
    return marker
