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
import tempfile

#: The environment variable naming the composed map, injected by the launcher.
RESIDENCY_MAP_ENV = "PRISMABUILD_RESIDENCY_MAP"
#: What one mover writes about the range it staged.
RESIDENCY_MAP_FRAGMENT_SCHEMA_V1 = "prismaquant.prismabuild.residency_map_fragment.v1"
#: What a consumer reads.
RESIDENCY_MAP_SCHEMA_V1 = "prismaquant.prismabuild.residency_map.v1"

_HEX = frozenset("0123456789abcdef")
_ENTRY_KEYS = frozenset({"stage_path", "bytes", "offset", "sha256"})
_FRAGMENT_KEYS = frozenset({
    "schema", "consumer_action_key", "mover_action_key", "tier_id",
    "stage_root", "manifest_sha256", "entries",
})
_MAP_KEYS = frozenset({
    "schema", "tier_id", "stage_root", "manifest_sha256", "leads",
    "generation", "entries",
})


class ResidencyMapError(ValueError):
    """A map, a fragment or an entry that does not say what it must."""


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
            or any(character not in _HEX for character in value)):
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
    if not isinstance(value, str) or len(value) != 64 or any(c not in _HEX for c in value):
        raise ResidencyMapError(f"{where} must be a 64-character action key")
    return value


def validate_entry(key: object, value: object, *, stage_root: str) -> dict[str, object]:
    """One staged range: where it is, how long it is, and what it hashes to.

    ``sha256`` is required even though a data manifest may carry ``null`` on
    every entry.  The manifest's digest is optional because hashing a terabyte
    of calibration captures costs more than the residency it buys; a *copy* is
    different.  The map's whole claim is that these bytes are the manifest's
    bytes, on another device, and a copy nobody hashed cannot make it.
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
    return {
        "stage_path": stage_path,
        "bytes": _positive(value.get("bytes"), where=f"entry {key!r} bytes"),
        "offset": offset,
        "sha256": _digest(value.get("sha256"), where=f"entry {key!r} sha256"),
    }


def _entries(raw: object, *, stage_root: str) -> dict[str, dict[str, object]]:
    if not isinstance(raw, Mapping):
        raise ResidencyMapError("entries must be an object")
    out: dict[str, dict[str, object]] = {}
    for key, value in raw.items():
        out[str(key)] = validate_entry(key, value, stage_root=stage_root)
    return out


def validate_fragment(value: object) -> dict[str, object]:
    """What one mover says it staged, checked."""

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
    return {
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


def validate_map(value: object) -> dict[str, object]:
    """What the consumer reads, checked with the same rules as a fragment."""

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
    leads = value.get("leads")
    if not isinstance(leads, list):
        raise ResidencyMapError("residency map leads must be an array of action keys")
    checked = [_action_key(lead, where="residency map leads") for lead in leads]
    if len(set(checked)) != len(checked):
        raise ResidencyMapError("residency map leads must not repeat a key")
    return {
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
        "entries": _entries(value.get("entries"), stage_root=stage_root),
    }


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
    "RESIDENCY_MAP_ENV",
    "RESIDENCY_MAP_FRAGMENT_SCHEMA_V1",
    "RESIDENCY_MAP_SCHEMA_V1",
    "ResidencyMapError",
    "compose",
    "fragment_path",
    "lookup",
    "map_path",
    "parse_residency_map_key",
    "read_fragments",
    "read_map",
    "residency_map_key",
    "validate_entry",
    "validate_fragment",
    "validate_map",
    "write_fragment",
    "write_map",
]
