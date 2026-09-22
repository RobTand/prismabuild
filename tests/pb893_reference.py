"""Frozen pre-#893 cover lookup, the reference PB #893 is held equal to.

Verbatim from ``src/prismabuild/reader_lease.py`` and
``src/prismabuild/residency_map.py`` at ef997618e133 (origin/main before
PB #893): ``_cached_cover_docs``, ``covers_for_keys`` and the three
per-character hex checks.  The functions read through the live modules'
``read_material``, ``fragment_path`` and ``validate_fragment``;
``old_hex_checks`` swaps the live modules' hex checks for the frozen ones,
so a reference call validates exactly as origin/main did.  Do not edit the
frozen bodies: the equivalence tests' claim is "identical to this code".
"""
from __future__ import annotations

from collections.abc import Mapping
import contextlib
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import reader_lease, residency_map  # noqa: E402
from prismabuild.reader_lease import read_material  # noqa: E402

_HEX = frozenset("0123456789abcdef")
ReaderLeaseError = reader_lease.ReaderLeaseError
ResidencyMapError = residency_map.ResidencyMapError


# reader_lease.py @ ef997618e133
def _hex(value: object, length: int, *, where: str) -> str:
    if (not isinstance(value, str) or len(value) != length
            or any(c not in _HEX for c in value)):
        raise ReaderLeaseError(f"{where} must be {length} lowercase hex")
    return value


# residency_map.py @ ef997618e133
def _digest(value: object, *, where: str) -> str:
    if (not isinstance(value, str) or len(value) != 64
            or any(character not in _HEX for character in value)):
        raise ResidencyMapError(f"{where} must be a 64-character lowercase digest")
    return value


def _action_key(value: object, *, where: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(c not in _HEX for c in value):
        raise ResidencyMapError(f"{where} must be a 64-character action key")
    return value


@contextlib.contextmanager
def old_hex_checks():
    """Run the live validators with origin/main's per-character hex checks."""

    saved = (reader_lease._hex, residency_map._digest,
             residency_map._action_key)
    reader_lease._hex = _hex
    residency_map._digest = _digest
    residency_map._action_key = _action_key
    try:
        yield
    finally:
        (reader_lease._hex, residency_map._digest,
         residency_map._action_key) = saved


# reader_lease.py @ ef997618e133
def _cached_cover_docs(root: Path, consumer_action_key: str, mover: str,
                       context: dict | None):
    """Validated (material, fragment) for one mover, sidecar-checked cache.

    The cache holds VALIDATED documents.  The sidecar is re-read on every
    call; a repeat lookup reuses the cached fragment only while the fresh
    sidecar equals the cached one.  The stage mover republishes both
    documents incrementally as entries land under ONE generation per run
    (stage_move.publish / begin_material), so the generation alone cannot
    date the pair: a republished sidecar re-reads the fragment and replaces
    the cache entry.  Absence and malformation are NEVER cached, so newly
    published material is always seen.  Selection only: acquire revalidates
    under the ownership lock before anything pins.
    """

    material = read_material(root, consumer_action_key, mover)
    if not isinstance(material, dict):
        return None, None
    generation = str(material.get("generation") or "")
    if not generation:
        return None, None
    if context is not None:
        hit = context.get(f"cover:{consumer_action_key}:{mover}")
        if (isinstance(hit, dict) and hit.get("generation") == generation
                and hit.get("material") == material):
            return hit.get("material"), hit.get("fragment")
    try:
        from prismabuild import residency_map as map_mod
        with open(map_mod.fragment_path(
                root, consumer_action_key, mover)) as stream:
            fragment = map_mod.validate_fragment(json.load(stream))
    except (OSError, ValueError):
        return None, None
    if context is not None:
        context[f"cover:{consumer_action_key}:{mover}"] = {
            "generation": generation, "material": material,
            "fragment": fragment}
    return material, fragment


def covers_for_keys(root: str | Path, consumer_action_key: str,
                    keys: list[str], *, tier_id: str,
                    manifest_sha256: str, epoch: str,
                    context: dict | None = None) -> dict[str, object]:
    """Resolve a window's covering material from PB-owned records.

    Given requested map keys on one tier, returns the minimal covering
    mover set plus the expected per-key proof (``covers``/``expected``
    for :func:`acquire`) -- read off the consumer's fragments plus
    publish-time sidecars, batched at window granularity, never per
    tensor.  Both SSD and RAM tiers: RAM covers come from ram-tier
    fragments carrying the announced epoch (SSD fragments carry epoch
    ``""``, explicit absence, never a RAM epoch).  ``manifest_sha256``
    and ``epoch`` are REQUIRED keywords so no other readset's material
    can be adopted.  Callers (including PQ) must not invent RAM covers
    from SSD leads: only material the fleet published qualifies.
    Freshness is re-validated under the ownership lock inside
    :func:`acquire`; this lookup is selection, not admission.

    Minimal and nonconflicting: each requested key is attributed to
    exactly one mover; two movers vouching one key with different bytes
    or digests refuse as contradictory proofs, and a selected mover
    whose material or fragment is malformed fails the pass
    (ownership-uncertain) instead of being silently skipped into an
    ``unpublished`` that invites fallback.  The sealed caller's
    expected length/digest stays authoritative downstream: acquire
    proves it against this selection and refuses any gap.

    Returns ``{"ok": True, "covers":
    [{mover_action_key, manifest_sha256}], "manifest_sha256": ...,
    "expected": {key: {bytes, sha256}}}`` or ``{"ok": False,
    "refusal": ...}``.
    """

    base = Path(root)
    if context is None:
        context = {}
    try:
        names = sorted(entry.name for entry in os.scandir(
            base / "material" / consumer_action_key)
            if entry.is_file() and entry.name.endswith(".json"))
    except OSError:
        return {"ok": False, "refusal": "unpublished"}
    wanted = set(keys)
    # Per-key candidates: mover -> (bytes, digest, generation).
    candidates: dict[str, list[tuple[str, object, object, str]]] = {}
    selected: dict[str, dict[str, object]] = {}
    for name in names:
        mover = name[:-len(".json")]
        if len(mover) != 64 or any(c not in _HEX for c in mover):
            continue
        material, fragment = _cached_cover_docs(
            base, consumer_action_key, mover, context)
        if material is None or fragment is None:
            continue
        if str(material.get("tier_id") or "") != tier_id:
            continue
        if str(material.get("manifest_sha256") or "") != manifest_sha256:
            continue
        # Exact sidecar convention: SSD material carries no epoch (absent
        # or ""), RAM material carries the announced epoch it landed
        # under.  A non-RAM tier naming an epoch is corrupt, not staged.
        material_epoch = material.get("epoch")
        if tier_id.startswith("ram:"):
            if not isinstance(material_epoch, str) or not material_epoch:
                continue
            if material_epoch != str(epoch or ""):
                continue
        else:
            if isinstance(material_epoch, str) and material_epoch:
                return {"ok": False,
                        "refusal": "ownership-uncertain: staged epoch set"}
            if str(material.get("epoch") or "") != str(epoch or ""):
                continue
        if (str(fragment.get("tier_id") or "") != tier_id
                or str(fragment.get("manifest_sha256") or "")
                != manifest_sha256
                or str(fragment.get("epoch") or "") != str(epoch or "")):
            continue
        material_entries = material.get("entries")
        if not isinstance(material_entries, dict):
            continue
        fragment_entries = fragment.get("entries")
        if not isinstance(fragment_entries, dict):
            continue
        for key, mention in material_entries.items():
            if str(key) not in wanted or not isinstance(mention, dict):
                continue
            vouched = fragment_entries.get(str(key))
            # The sidecar dates the fragment's vouching: same path,
            # length, digest, or this cover is about different bytes.
            if (not isinstance(vouched, Mapping)
                    or str(vouched.get("stage_path") or "")
                    != str(mention.get("stage_path") or "")
                    or vouched.get("bytes") != mention.get("bytes")
                    or str(vouched.get("sha256") or "")
                    != str(mention.get("sha256") or "")):
                selected[str(key)] = {"tainted": True,
                                      "reason": "sidecar/fragment disagree"}
                continue
            candidates.setdefault(str(key), []).append((
                mover, mention.get("bytes"), mention.get("sha256"),
                str(material.get("generation"))))
            selected.setdefault(str(key), dict(mention))
    if not any(candidates.values()):
        # Nothing published for this readset at all: absence, not a gap.
        return {"ok": False, "refusal": "unpublished"}
    covers: list[dict[str, str]] = []
    expected: dict[str, dict[str, object]] = {}
    seen_movers: set[str] = set()
    for key in keys:
        if key in selected and selected[key].get("tainted"):
            # A selected cover disagreeing with its fragment is a
            # contradiction in the chosen proof, not an absence.
            return {"ok": False,
                    "refusal": "ownership-uncertain: sidecar/fragment disagree"}
        options = candidates.get(key, [])
        if not options:
            return {"ok": False, "refusal": "source-coverage-gap"}
        first = options[0]
        for other in options[1:]:
            if other[1] != first[1] or other[2] != first[2]:
                # Two movers vouch one key with different bytes: picking
                # either would make the pin a guess.
                return {"ok": False,
                        "refusal": "ownership-uncertain: contradictory covers"}
        mover = first[0]
        expected[key] = {"bytes": first[1], "sha256": first[2]}
        if mover not in seen_movers:
            seen_movers.add(mover)
            covers.append({"mover_action_key": mover,
                           "manifest_sha256": manifest_sha256})
    return {"ok": True,
            "covers": covers,
            "manifest_sha256": manifest_sha256,
            "expected": expected}
