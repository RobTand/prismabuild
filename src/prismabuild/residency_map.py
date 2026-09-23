"""What a consumer reads to find its bytes on a stage tier, and how movers write it (#583).

A movement node copies one byte range of a data manifest's read order from the
source pool onto a stage tier.  The consumer has to be told where those copies
landed, because the ARC is keyed by on-pool block pointer: a copy on another
device does not warm the pool path, so a consumer that keeps opening the
declared path gains nothing from the stage.  The residency map is that telling.
Its path arrives in the consumer's environment as
:data:`RESIDENCY_MAP_ENV`, beside the progress path and token the launcher
already injects, and a consumer falls back to the declared path for anything
the map does not name.

**The key is ``(path, offset)``, not ``path``.**  ``core.validate_data_manifest``
refuses a manifest that repeats a ``(path, offset)`` pair and permits the same
path at several offsets -- the live campaign manifest carries 46 byte-range
entries inside 43 model shards -- so a path on its own is not an entry's
identity and a map keyed by one would be ambiguous exactly where a partial copy
is most dangerous.  :func:`residency_map_key` spells the identity as
``"<offset>:<path>"``.  That is injective: the offset is decimal digits and the
split is on the first colon, so a path containing colons cannot collide with
another entry's offset.

**Movers write fragments; the map is composed from them.**  One file that every
mover read-modify-writes would lose entries the moment two movers of the same
consumer finish together, and rename is the only concurrency primitive this
fleet trusts on NFS.  So each mover writes its own fragment under its own name
(:func:`write_fragment`, atomic rename), and :func:`compose` merges the
fragments of a consumer's leads into the single document the consumer reads
(:func:`write_map`, also an atomic rename).  A fragment lists only entries whose
copy is complete *and* verified: it is written after the staged file's rename,
never before, so a map entry is a statement that the bytes are there and are
the bytes the manifest named.

**Conflicts refuse rather than merge.**  Two fragments naming one key with
different bytes are two claims about one range, and picking either would make
the map a guess.  :func:`compose` raises.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
import json
import os
from pathlib import Path
import re
import tempfile

from . import storage_tiers

#: The environment variable naming the composed map, injected by the launcher.
RESIDENCY_MAP_ENV = "PRISMABUILD_RESIDENCY_MAP"
#: What one mover writes about the range it staged.
RESIDENCY_MAP_FRAGMENT_SCHEMA_V1 = "prismaquant.prismabuild.residency_map_fragment.v1"
#: What a consumer reads.
RESIDENCY_MAP_SCHEMA_V1 = "prismaquant.prismabuild.residency_map.v1"

_HEX = frozenset("0123456789abcdef")
#: ``_HEX`` checked at C speed: ``[0-9a-f]`` is a literal ASCII range, so a
#: full match accepts exactly the strings whose every character is in
#: ``_HEX``.  Every fragment entry carries a digest, and the per-character
#: generator was the hottest line of a cover lookup (#893).
_HEX_RUN = re.compile("[0-9a-f]*")
#: ``ram_path`` is optional and names the tmpfs copy of an entry the stage
#: already vouches for (#640); it is the overlay's half of the entry.
_ENTRY_KEYS = frozenset({"stage_path", "bytes", "offset", "sha256", "ram_path"})
#: The epoch a ram fragment landed under; required on the ram tier only,
#: because a ram range nobody can place in time is not resident.
_FRAGMENT_KEYS = frozenset({
    "schema", "consumer_action_key", "mover_action_key", "tier_id",
    "stage_root", "manifest_sha256", "entries", "epoch",
})
#: The ram overlay's header: which tier, under which root, in which epoch.
_MAP_KEYS = frozenset({
    "schema", "tier_id", "stage_root", "manifest_sha256", "leads",
    "generation", "entries", "ram_tier_id", "ram_root", "ram_epoch",
})


class ResidencyMapError(ValueError):
    """A map, a fragment or an entry that does not say what it must."""


#: When the tier loop expects each of a consumer's pending ranges to land
#: (#989).  A document of its own beside the map, never a field in the map's
#: header: every map reader refuses unknown header fields, so a header field
#: would stop every reader that predates it.
RESIDENCY_LANDING_SCHEMA_V1 = "prismaquant.prismabuild.residency_landing.v1"
#: The states a pending range can be in.  ``ready`` and ``claimed`` name the
#: mover's queue state.  ``unpublished`` is an in-horizon leg the window has
#: not published (a stall, or a failed copy awaiting its recopy).
#: ``terminal-no-receipt`` is a leg nothing will publish again: its mover
#: failed and the plan is superseded.
LANDING_STATES = ("ready", "claimed", "unpublished", "evicted",
                  "done-not-resident", "terminal-no-receipt")
_LANDING_KEYS = frozenset({
    "schema", "consumer_action_key", "tier_id", "manifest_sha256",
    "written_unix", "landing_bytes_per_s", "landing_basis",
    "rates_measured_bytes_per_s", "rate_min_bytes_per_s",
    "rate_max_bytes_per_s", "report_latency_s", "tier_loop_liveness_s",
    "publish_s", "ranges"})
_LANDING_RANGE_KEYS = frozenset({
    "mover_action_key", "phase", "chunk_index", "range_start_bytes",
    "range_end_bytes", "state", "queue_position", "bytes_ahead",
    "expected_landing_unix", "claimed_unix", "waiting_for"})


def residency_map_key(path: str, offset: int = 0) -> str:
    """The identity of one manifest entry, as the map spells it.

    ``"0:/mnt/shared/model/shard-00001.safetensors"`` for a whole file;
    ``"1048576:/mnt/shared/…"`` for the range that begins one MiB in.  The
    consumer builds this from the entry it is about to read, which already
    carries both halves.
    """

    if not isinstance(path, str) or not path:
        raise ResidencyMapError("a residency map key needs a path")
    if isinstance(offset, bool) or type(offset) is not int or offset < 0:
        raise ResidencyMapError("a residency map key needs a non-negative offset")
    return f"{offset}:{path}"


def parse_residency_map_key(key: str) -> tuple[str, int]:
    """``"1048576:/a/b"`` -> ``("/a/b", 1048576)``; the inverse of the above."""

    if not isinstance(key, str):
        raise ResidencyMapError("a residency map key must be a string")
    head, separator, path = key.partition(":")
    if not separator or not path or not head.isdigit():
        raise ResidencyMapError(f"malformed residency map key {key!r}")
    return path, int(head)


def _digest(value: object, *, where: str) -> str:
    if (not isinstance(value, str) or len(value) != 64
            or _HEX_RUN.fullmatch(value) is None):
        raise ResidencyMapError(f"{where} must be a 64-character lowercase digest")
    return value


def _positive(value: object, *, where: str) -> int:
    if isinstance(value, bool) or type(value) is not int or value <= 0:
        raise ResidencyMapError(f"{where} must be a positive integer")
    return value


def _nonnegative(value: object, *, where: str) -> int:
    if isinstance(value, bool) or type(value) is not int or value < 0:
        raise ResidencyMapError(f"{where} must be a non-negative integer")
    return value


def _absolute(value: object, *, where: str) -> str:
    if not isinstance(value, str) or not value.startswith("/"):
        raise ResidencyMapError(f"{where} must be an absolute path")
    if value != os.path.normpath(value):
        raise ResidencyMapError(f"{where} must be normalized")
    return value


def _action_key(value: object, *, where: str) -> str:
    if (not isinstance(value, str) or len(value) != 64
            or _HEX_RUN.fullmatch(value) is None):
        raise ResidencyMapError(f"{where} must be a 64-character action key")
    return value


def _epoch(value: object, *, where: str) -> str:
    if not isinstance(value, str) or not value or "/" in value:
        raise ResidencyMapError(f"{where} must be a non-empty string with no '/'")
    return value


def validate_entry(key: object, value: object, *, stage_root: str,
                   ram_root: str | None = None) -> dict[str, object]:
    """One staged range: where it is, how long it is, and what it hashes to.

    ``sha256`` is required even though a data manifest may carry ``null`` on
    every entry.  The manifest's digest is optional because hashing a terabyte
    of calibration captures costs more than the residency it buys; a *copy* is
    different.  The map's whole claim is that these bytes are the manifest's
    bytes, on another device, and a copy nobody hashed cannot make it.

    ``ram_path`` is optional and only ever names the tmpfs copy of an entry
    the stage already vouches for (#640): same key, same digest, a second
    servant.  It must live under the map's ``ram_root``, because a map that
    could name a path outside the announced ram tier is a map that could
    redirect a consumer's read anywhere.
    """

    _, offset = parse_residency_map_key(str(key))
    if not isinstance(value, Mapping):
        raise ResidencyMapError(f"residency map entry {key!r} must be an object")
    unknown = sorted(set(value) - _ENTRY_KEYS)
    if unknown:
        raise ResidencyMapError(f"unknown residency map entry fields: {unknown}")
    stage_path = _absolute(value.get("stage_path"), where=f"entry {key!r} stage_path")
    if not (stage_path == stage_root or stage_path.startswith(stage_root.rstrip("/") + "/")):
        # A map that could name a path outside the stage is a map that could
        # redirect a consumer's read anywhere, which is not what a cache is.
        raise ResidencyMapError(
            f"entry {key!r} stage_path must live under {stage_root!r}")
    declared = _nonnegative(value.get("offset", offset), where=f"entry {key!r} offset")
    if declared != offset:
        raise ResidencyMapError(
            f"entry {key!r} offset {declared} disagrees with its key")
    checked: dict[str, object] = {
        "stage_path": stage_path,
        "bytes": _positive(value.get("bytes"), where=f"entry {key!r} bytes"),
        "offset": offset,
        "sha256": _digest(value.get("sha256"), where=f"entry {key!r} sha256"),
    }
    ram_path = value.get("ram_path")
    if ram_path is not None:
        if ram_root is None:
            raise ResidencyMapError(
                f"entry {key!r} names a ram_path, but the map announces no ram root")
        checked_ram = _absolute(ram_path, where=f"entry {key!r} ram_path")
        if not (checked_ram == ram_root
                or checked_ram.startswith(ram_root.rstrip("/") + "/")):
            raise ResidencyMapError(
                f"entry {key!r} ram_path must live under {ram_root!r}")
        checked["ram_path"] = checked_ram
    return checked


def _entries(raw: object, *, stage_root: str,
             ram_root: str | None = None) -> dict[str, dict[str, object]]:
    if not isinstance(raw, Mapping):
        raise ResidencyMapError("entries must be an object")
    out: dict[str, dict[str, object]] = {}
    for key, value in raw.items():
        out[str(key)] = validate_entry(key, value, stage_root=stage_root,
                                       ram_root=ram_root)
    return out


def validate_fragment(value: object) -> dict[str, object]:
    """What one mover says it staged, checked.

    A fragment of the ram tier must carry the epoch it landed under: the
    tmpfs empties on reboot while this fragment survives on the shared mount,
    and an undated ram range is a range nobody can place in time -- which is
    the one thing a residency claim may never be (#640).
    """

    if not isinstance(value, Mapping):
        raise ResidencyMapError("a residency map fragment must be an object")
    unknown = sorted(set(value) - _FRAGMENT_KEYS)
    if unknown:
        raise ResidencyMapError(f"unknown residency map fragment fields: {unknown}")
    if value.get("schema") != RESIDENCY_MAP_FRAGMENT_SCHEMA_V1:
        raise ResidencyMapError(
            f"fragment schema must be {RESIDENCY_MAP_FRAGMENT_SCHEMA_V1!r}")
    stage_root = _absolute(value.get("stage_root"), where="fragment stage_root")
    tier_id = value.get("tier_id")
    if not isinstance(tier_id, str) or not tier_id or "/" in tier_id:
        raise ResidencyMapError("fragment tier_id must be a tier id")
    epoch = value.get("epoch")
    if epoch is not None:
        epoch = _epoch(epoch, where="fragment epoch")
    if tier_id.startswith(storage_tiers.RAM_TIER_PREFIX) and epoch is None:
        raise ResidencyMapError(
            "a fragment of the ram tier must carry the epoch it landed under: "
            "a ram range nobody can place in time is not resident")
    checked: dict[str, object] = {
        "schema": RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": _action_key(
            value.get("consumer_action_key"), where="fragment consumer_action_key"),
        "mover_action_key": _action_key(
            value.get("mover_action_key"), where="fragment mover_action_key"),
        "tier_id": tier_id,
        "stage_root": stage_root,
        "manifest_sha256": _digest(
            value.get("manifest_sha256"), where="fragment manifest_sha256"),
        "entries": _entries(value.get("entries"), stage_root=stage_root),
    }
    if epoch is not None:
        checked["epoch"] = epoch
    return checked


def validate_map(value: object) -> dict[str, object]:
    """What the consumer reads, checked with the same rules as a fragment.

    A map that names ram paths must announce the tier, the root and the epoch
    they were laid under in its header: those three are what a reader compares
    against the announced ram tier to tell a current overlay from a stale one
    (#640), and an entry that could carry a ram path without them could
    carry one nobody can date.
    """

    if not isinstance(value, Mapping):
        raise ResidencyMapError("a residency map must be an object")
    unknown = sorted(set(value) - _MAP_KEYS)
    if unknown:
        raise ResidencyMapError(f"unknown residency map fields: {unknown}")
    if value.get("schema") != RESIDENCY_MAP_SCHEMA_V1:
        raise ResidencyMapError(f"residency map schema must be {RESIDENCY_MAP_SCHEMA_V1!r}")
    stage_root = _absolute(value.get("stage_root"), where="residency map stage_root")
    tier_id = value.get("tier_id")
    if not isinstance(tier_id, str) or not tier_id or "/" in tier_id:
        raise ResidencyMapError("residency map tier_id must be a tier id")
    ram_root = value.get("ram_root")
    if ram_root is not None:
        ram_root = _absolute(ram_root, where="residency map ram_root")
    ram_tier_id = value.get("ram_tier_id")
    if ram_tier_id is not None and (not isinstance(ram_tier_id, str)
                                    or not ram_tier_id or "/" in ram_tier_id):
        raise ResidencyMapError("residency map ram_tier_id must be a tier id")
    ram_epoch = value.get("ram_epoch")
    if ram_epoch is not None:
        ram_epoch = _epoch(ram_epoch, where="residency map ram_epoch")
    leads = value.get("leads")
    if not isinstance(leads, list):
        raise ResidencyMapError("residency map leads must be an array of action keys")
    checked = [_action_key(lead, where="residency map leads") for lead in leads]
    if len(set(checked)) != len(checked):
        raise ResidencyMapError("residency map leads must not repeat a key")
    entries = _entries(value.get("entries"), stage_root=stage_root,
                       ram_root=ram_root)
    if any("ram_path" in entry for entry in entries.values()) and (
            ram_root is None or ram_tier_id is None or ram_epoch is None):
        raise ResidencyMapError(
            "a residency map naming ram paths must announce its ram tier, "
            "root and epoch")
    out: dict[str, object] = {
        "schema": RESIDENCY_MAP_SCHEMA_V1,
        "tier_id": tier_id,
        "stage_root": stage_root,
        "manifest_sha256": _digest(
            value.get("manifest_sha256"), where="residency map manifest_sha256"),
        "leads": checked,
        # Monotonic, so a consumer re-reading a map its later movers extended
        # can tell a newer document from the one it already has.  It is the
        # count of composed fragments, which only grows for one consumer.
        "generation": _nonnegative(
            value.get("generation"), where="residency map generation"),
        "entries": entries,
    }
    for field, checked_value in (("ram_tier_id", ram_tier_id),
                                 ("ram_root", ram_root),
                                 ("ram_epoch", ram_epoch)):
        if checked_value is not None:
            out[field] = checked_value
    return out


def compose(fragments: Iterable[Mapping[str, object]]) -> dict[str, object]:
    """Merge one consumer's movers' fragments into the map it reads.

    Every fragment must agree about the consumer, the tier, the stage root and
    the manifest: a fragment that disagrees is about different work, and
    merging it would put another manifest's bytes behind this consumer's paths.
    Two fragments naming one key with different values refuse, because picking
    either would make the map a guess about which copy is on the device.
    """

    checked = [validate_fragment(fragment) for fragment in fragments]
    if not checked:
        raise ResidencyMapError("a residency map needs at least one fragment")
    first = checked[0]
    for fragment in checked[1:]:
        for field in ("consumer_action_key", "tier_id", "stage_root", "manifest_sha256"):
            if fragment[field] != first[field]:
                raise ResidencyMapError(
                    f"fragments disagree about {field}: "
                    f"{first[field]!r} and {fragment[field]!r}")
    entries: dict[str, dict[str, object]] = {}
    for fragment in checked:
        for key, entry in fragment["entries"].items():  # type: ignore[union-attr]
            existing = entries.get(key)
            if existing is not None and existing != entry:
                raise ResidencyMapError(
                    f"two movers staged {key!r} differently: {existing} and {entry}")
            entries[key] = entry
    return validate_map({
        "schema": RESIDENCY_MAP_SCHEMA_V1,
        "tier_id": first["tier_id"],
        "stage_root": first["stage_root"],
        "manifest_sha256": first["manifest_sha256"],
        "leads": sorted({str(fragment["mover_action_key"]) for fragment in checked}),
        "generation": len(checked),
        "entries": entries,
    })


def reissue(fragment: Mapping[str, object], *, consumer_action_key: str,
            mover_action_key: str) -> dict[str, object]:
    """The same staged bytes, vouched for by a different mover (#598).

    A later consumer whose phase names a range that is already on the tier
    takes the range over rather than copying it, and a fragment is the only
    document that says "this file is that mover's".  So the successor needs one
    of its own: the same entries, under its own consumer directory and its own
    mover name, because :func:`read_fragments` is per consumer and
    :func:`compose` refuses fragments that disagree about whose they are.

    Entries are copied unchanged on purpose.  They name the file, its length
    and its digest, and none of the three is a fact about which mover wrote it;
    rewriting any of them here would make the successor's map say something the
    original copy never verified.
    """

    checked = validate_fragment(fragment)
    return validate_fragment({**checked,
                              "consumer_action_key": consumer_action_key,
                              "mover_action_key": mover_action_key})


def overlay_ram(mapping: Mapping[str, object],
                fragments: Iterable[Mapping[str, object]], *,
                ram_tier_id: str, ram_root: str,
                ram_epoch: str) -> dict[str, object]:
    """Lay a ram tier's fragments over the stage map they serve (#640).

    ``compose`` refuses fragments that disagree about the tier, so a ram
    fragment is never composed *into* the stage map; it is laid over it.  An
    entry keeps the stage path it already had and gains ``ram_path``: the
    same ``(path, offset)`` identity, the same digest, the bytes on the tmpfs
    under the same content-addressed name.  A consumer prefers the ram copy
    and falls back to the staged copy the map already vouched for, which is
    what makes a stale ram entry a cache miss rather than an ENOENT.

    A ram fragment whose key the stage map does not carry is skipped rather
    than raised on: the two windows may disagree for a cycle -- a crash
    between the two egresses is the shape -- and refusing the whole compose
    would send the consumer to the pool for every entry, where skipping it
    leaves the map exactly as wide as the stage's own vouching.  A ram copy
    that disagrees with the stage's bytes or digest refuses, because picking
    either would make the map a guess about which bytes are the manifest's.
    """

    checked = [validate_fragment(fragment) for fragment in fragments]
    base = validate_map(mapping)
    entries = {key: dict(entry) for key, entry in base["entries"].items()}
    laid = 0
    for fragment in checked:
        if str(fragment["tier_id"]) != str(ram_tier_id):
            raise ResidencyMapError(
                f"an overlay fragment names tier {fragment['tier_id']!r}, "
                f"not the announced {ram_tier_id!r}")
        if str(fragment.get("epoch") or "") != str(ram_epoch):
            raise ResidencyMapError(
                "an overlay fragment must carry the epoch it is being laid "
                "under; a prior epoch's range is not resident")
        for key, entry in fragment["entries"].items():      # type: ignore[union-attr]
            existing = entries.get(str(key))
            if existing is None:
                continue
            if (entry["bytes"] != existing["bytes"]
                    or entry["sha256"] != existing["sha256"]):
                raise ResidencyMapError(
                    f"the ram copy of {key!r} is not the copy the stage "
                    f"vouches for: {entry} against {existing}")
            if "ram_path" in existing and existing["ram_path"] != entry["stage_path"]:
                raise ResidencyMapError(
                    f"two ram movers staged {key!r} differently: "
                    f"{existing['ram_path']} and {entry['stage_path']}")
            existing["ram_path"] = entry["stage_path"]
            laid += 1
    if not laid:
        return base
    return validate_map({**base, "ram_tier_id": str(ram_tier_id),
                         "ram_root": str(ram_root), "ram_epoch": str(ram_epoch),
                         "entries": entries})


def _write_atomic(path: Path, payload: Mapping[str, object]) -> Path:
    """Rename into place, so a reader never sees half a document."""

    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.")
    try:
        with os.fdopen(handle, "w") as stream:
            json.dump(payload, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
    return path


def fragment_path(root: str | Path, consumer_action_key: str, mover_action_key: str) -> Path:
    """``<root>/<consumer>/<mover>.json`` -- one writer per file, by construction."""

    return (Path(root)
            / _action_key(consumer_action_key, where="consumer_action_key")
            / f"{_action_key(mover_action_key, where='mover_action_key')}.json")


def map_path(root: str | Path, consumer_action_key: str) -> Path:
    """``<root>/<consumer>.map.json`` -- the composed document, beside the fragments.

    Beside rather than inside, because the fragment directory has one writer
    per file and the map has one writer altogether; a map inside it would be
    read back as a fragment by ``read_fragments`` and refused on every scan.
    """

    return (Path(root)
            / f"{_action_key(consumer_action_key, where='consumer_action_key')}.map.json")


def write_fragment(root: str | Path, fragment: Mapping[str, object]) -> Path:
    checked = validate_fragment(fragment)
    return _write_atomic(
        fragment_path(root, str(checked["consumer_action_key"]),
                      str(checked["mover_action_key"])),
        checked)


def read_fragments(root: str | Path, consumer_action_key: str) -> list[dict[str, object]]:
    """Every fragment filed for one consumer, oldest name first.

    A fragment that cannot be read or does not validate is skipped rather than
    raised on: a half-written or foreign file in this directory must not stop a
    consumer reading the copies its other movers really did make.
    """

    directory = Path(root) / _action_key(consumer_action_key, where="consumer_action_key")
    try:
        names = sorted(entry.name for entry in os.scandir(directory)
                       if entry.is_file() and entry.name.endswith(".json"))
    except OSError:
        return []
    out: list[dict[str, object]] = []
    for name in names:
        try:
            with open(directory / name) as stream:
                out.append(validate_fragment(json.load(stream)))
        except (OSError, ValueError):
            continue
    return out


def landing_path(root: str | Path, consumer_action_key: str) -> Path:
    """``<root>/<consumer>.landing.json`` -- beside the map, one writer.

    Outside the fragment directory for the map's reason: ``read_fragments``
    would read it back as a fragment and refuse it on every scan.
    """

    return (Path(root) / f"{_action_key(consumer_action_key, where='consumer_action_key')}"
            ".landing.json")


def _finite_or_none(value: object, *, where: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ResidencyMapError(f"{where} must be a number or null")
    number = float(value)
    if number != number or number in (float("inf"), float("-inf")):
        raise ResidencyMapError(f"{where} must be finite")
    return number


def validate_landing(value: object) -> dict[str, object]:
    """A landing record, checked field by field (#989).

    Every pending range names its mover, its read-order byte range and its
    state.  A queued range (``ready``/``claimed``) carries a number for
    ``expected_landing_unix`` with its queue position and the bytes ahead of
    it; any other state carries ``null`` there and says in ``waiting_for``
    what it waits on.
    """

    if not isinstance(value, Mapping):
        raise ResidencyMapError("a landing record must be an object")
    unknown = sorted(set(value) - _LANDING_KEYS)
    if unknown:
        raise ResidencyMapError(f"unknown landing record fields: {unknown}")
    if value.get("schema") != RESIDENCY_LANDING_SCHEMA_V1:
        raise ResidencyMapError(
            f"landing record schema must be {RESIDENCY_LANDING_SCHEMA_V1!r}")
    rates = value.get("rates_measured_bytes_per_s")
    if not isinstance(rates, list):
        raise ResidencyMapError("rates_measured_bytes_per_s must be an array")
    out: dict[str, object] = {
        "schema": RESIDENCY_LANDING_SCHEMA_V1,
        "consumer_action_key": _action_key(
            value.get("consumer_action_key"), where="landing consumer_action_key"),
        "tier_id": str(value.get("tier_id") or ""),
        "manifest_sha256": _digest(value.get("manifest_sha256"),
                                   where="landing manifest_sha256"),
        "written_unix": _finite_or_none(value.get("written_unix"),
                                        where="landing written_unix"),
        "landing_bytes_per_s": _finite_or_none(
            value.get("landing_bytes_per_s"), where="landing_bytes_per_s"),
        "landing_basis": str(value.get("landing_basis") or ""),
        "rates_measured_bytes_per_s": [
            _finite_or_none(rate, where="rates_measured_bytes_per_s")
            for rate in rates],
        "rate_min_bytes_per_s": _finite_or_none(
            value.get("rate_min_bytes_per_s"), where="rate_min_bytes_per_s"),
        "rate_max_bytes_per_s": _finite_or_none(
            value.get("rate_max_bytes_per_s"), where="rate_max_bytes_per_s"),
        "report_latency_s": _finite_or_none(
            value.get("report_latency_s"), where="report_latency_s"),
        "tier_loop_liveness_s": _finite_or_none(
            value.get("tier_loop_liveness_s"), where="tier_loop_liveness_s"),
        "publish_s": _finite_or_none(value.get("publish_s"), where="publish_s"),
    }
    if out["written_unix"] is None:
        raise ResidencyMapError("landing written_unix is required")
    ranges = value.get("ranges")
    if not isinstance(ranges, list):
        raise ResidencyMapError("landing ranges must be an array")
    checked: list[dict[str, object]] = []
    for entry in ranges:
        if not isinstance(entry, Mapping):
            raise ResidencyMapError("a landing range must be an object")
        extra = sorted(set(entry) - _LANDING_RANGE_KEYS)
        if extra:
            raise ResidencyMapError(f"unknown landing range fields: {extra}")
        state = entry.get("state")
        if state not in LANDING_STATES:
            raise ResidencyMapError(f"landing range state must be one of {LANDING_STATES}")
        start = _nonnegative(entry.get("range_start_bytes"), where="range_start_bytes")
        end = _positive(entry.get("range_end_bytes"), where="range_end_bytes")
        if end <= start:
            raise ResidencyMapError("a landing range must be non-empty")
        expected = _finite_or_none(entry.get("expected_landing_unix"),
                                   where="expected_landing_unix")
        row: dict[str, object] = {
            "mover_action_key": _action_key(entry.get("mover_action_key"),
                                            where="landing mover_action_key"),
            "phase": str(entry.get("phase") or ""),
            "chunk_index": entry.get("chunk_index"),
            "range_start_bytes": start, "range_end_bytes": end,
            "state": state, "expected_landing_unix": expected,
            "queue_position": entry.get("queue_position"),
            "bytes_ahead": entry.get("bytes_ahead"),
            "claimed_unix": _finite_or_none(entry.get("claimed_unix"),
                                            where="claimed_unix"),
            "waiting_for": str(entry.get("waiting_for") or ""),
        }
        if state in ("ready", "claimed"):
            if expected is None:
                raise ResidencyMapError("a queued range carries its expected landing")
            _nonnegative(row["queue_position"], where="queue_position")
            _nonnegative(row["bytes_ahead"], where="bytes_ahead")
        elif expected is not None or not row["waiting_for"]:
            raise ResidencyMapError(
                "a range that is not queued carries no expected landing and "
                "says what it waits for")
        checked.append(row)
    out["ranges"] = checked
    return out


def write_landing(path: str | Path, record: Mapping[str, object]) -> Path:
    return _write_atomic(Path(path), validate_landing(record))


def read_landing(path: str | Path) -> dict[str, object]:
    with open(path) as stream:
        return validate_landing(json.load(stream))


def write_map(path: str | Path, mapping: Mapping[str, object]) -> Path:
    return _write_atomic(Path(path), validate_map(mapping))


def read_map(path: str | Path) -> dict[str, object]:
    with open(path) as stream:
        return validate_map(json.load(stream))


def lookup(mapping: Mapping[str, object], path: str, offset: int = 0) -> dict[str, object] | None:
    """The staged copy of one manifest entry, or ``None`` to read the declared path.

    The fallback is the point: a map names the copies that exist, and a
    consumer that cannot find one reads the pool exactly as it does today.
    """

    entries = mapping.get("entries")
    if not isinstance(entries, Mapping):
        return None
    found = entries.get(residency_map_key(path, offset))
    return dict(found) if isinstance(found, Mapping) else None


__all__ = [
    "LANDING_STATES",
    "RESIDENCY_LANDING_SCHEMA_V1",
    "RESIDENCY_MAP_ENV",
    "RESIDENCY_MAP_FRAGMENT_SCHEMA_V1",
    "RESIDENCY_MAP_SCHEMA_V1",
    "ResidencyMapError",
    "compose",
    "fragment_path",
    "landing_path",
    "lookup",
    "map_path",
    "overlay_ram",
    "parse_residency_map_key",
    "read_fragments",
    "read_landing",
    "read_map",
    "reissue",
    "residency_map_key",
    "validate_entry",
    "validate_fragment",
    "validate_landing",
    "validate_map",
    "write_fragment",
    "write_landing",
    "write_map",
]
