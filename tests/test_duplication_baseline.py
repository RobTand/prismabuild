"""Duplicated functionality may shrink and never grow (PB #1328).

``tools/duplication_inventory.py`` finds cross-file structural
near-duplicates and module-level helpers that share a name across modules,
which are the duplication textual audits miss (adapted from PQ's ratchet;
stdlib only, no PQ import -- PB stands alone).
``tests/fixtures/duplication_baseline.json`` records today's state.
A new near-duplicate pair, a new same-name group, or a module joining an
existing group fails. So does a baseline entry that no longer exists: each
consolidation shrinks the baseline in the same change, by running
``python tools/duplication_inventory.py --write-baseline``.

A same-name collision whose contracts genuinely differ keeps its name and is
recorded in the baseline's ``same_name_distinct`` list as an exact
``(name, path)`` exception with its reason (#1386). The registry is policy,
not a growth path: a stale or malformed row fails, a third module joining an
exempt name fails, and the shrink-only map never absorbs an exempt path.

The baseline also ratchets primitive digest sites (PB #1328): a new raw
``hashlib`` constructor or literal ``sort_keys=True`` JSON encoding outside
``src/prismabuild/core.py`` -- the home of ``_canonical_bytes``,
``_canonical_file_bytes``, ``_sorted_json_bytes``, ``_sorted_lf_bytes``, ``_indented_lf_bytes``,
``canonical_sha256``, ``raw_sha256``, ``stream_sha256`` and
``_decode_strict_json`` -- fails, and consolidating a site onto one of those
named profiles lowers the baseline the same way.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

import duplication_inventory as inventory  # noqa: E402


def _baseline():
    return json.loads(inventory.BASELINE.read_text(encoding="utf-8"))


def test_scanner_finds_a_renamed_copy_and_ignores_same_file_pairs(tmp_path):
    body = (
        "    total = 0\n"
        "    for item in items:\n"
        "        if item.ready:\n"
        "            total += item.size\n"
        "        else:\n"
        "            total -= helper(item)\n"
        "    return {'total': total, 'count': len(items)}\n"
    )
    (tmp_path / "src" / "prismabuild").mkdir(parents=True)
    (tmp_path / "tools").mkdir()
    (tmp_path / "src" / "prismabuild" / "a.py").write_text(
        "def _sha(items):\n" + body + "\ndef other(things):\n" + body.replace("items", "things"))
    (tmp_path / "tools" / "b.py").write_text(
        "def _sha(rows):\n" + body.replace("items", "rows").replace("total", "acc"))
    live = inventory.scan(tmp_path)
    assert ["src/prismabuild/a.py::_sha", "tools/b.py::_sha"] in live["near_duplicates"]
    assert ["src/prismabuild/a.py::other", "tools/b.py::_sha"] in live["near_duplicates"]
    assert all(a.split("::")[0] != b.split("::")[0] for a, b in live["near_duplicates"])
    assert live["same_name_helpers"] == {"_sha": ["src/prismabuild/a.py", "tools/b.py"]}


def test_near_duplicate_pairs_only_shrink():
    live = {tuple(p) for p in inventory.scan()["near_duplicates"]}
    base = {tuple(p) for p in _baseline()["near_duplicates"]}
    assert not live - base, (
        f"new near-duplicate functions {sorted(live - base)}: reuse the existing "
        "implementation instead of a copy (PB #1328)")
    assert not base - live, (
        f"baseline pairs gone {sorted(base - live)}: shrink the baseline with "
        "tools/duplication_inventory.py --write-baseline")


def test_same_name_helpers_only_shrink():
    base = _baseline()
    live = inventory.scan()["same_name_helpers"]
    grown = inventory.same_name_growth(live, base["same_name_helpers"],
                                       inventory.registered_same_name(base))
    assert not grown, (
        f"helpers defined again in another module {grown}: import the existing "
        "one, or register the exact name and path with its reason in "
        "same_name_distinct (PB #1328, #1386)")
    shrunk = {n: sorted(set(m) - set(live.get(n, ())))
              for n, m in base["same_name_helpers"].items()
              if set(m) - set(live.get(n, ()))}
    assert not shrunk, (
        f"baseline entries gone {shrunk}: shrink the baseline with "
        "tools/duplication_inventory.py --write-baseline")


def test_same_name_distinct_records_are_exact_live_paths_with_reasons():
    """A registered collision names live paths, explains itself, and is not
    already allowed by the shrink-only baseline (#1386)."""

    live = inventory.scan()["same_name_helpers"]
    base = _baseline()
    seen: set[tuple[str, str]] = set()
    for row in base.get("same_name_distinct", []):
        assert set(row) == {"name", "modules", "reason"}, row
        name, modules, reason = row["name"], row["modules"], row["reason"]
        assert isinstance(name, str) and isinstance(reason, str), row
        assert isinstance(modules, list) and modules, row
        assert all(isinstance(module, str) for module in modules), row
        assert len(reason.split()) >= 8, f"{name} needs a real reason"
        assert live.get(name), (
            f"same_name_distinct names a group the code no longer defines: {name}")
        for module in modules:
            assert module in live[name], (
                "same_name_distinct names a path that no longer defines "
                f"{name}: {module}")
            assert module not in base["same_name_helpers"].get(name, ()), (
                f"same_name_distinct lists {module} for {name}, which the "
                "shrink-only baseline already allows")
            assert (name, module) not in seen, (
                f"same_name_distinct repeats {name} {module}")
            seen.add((name, module))


def test_a_third_definition_of_an_exempt_name_is_still_a_growth():
    """Registering one path never whitelists the name (#1386)."""

    live = {"_observe": ["a.py", "b.py", "c.py", "d.py"]}
    base = {"_observe": ["a.py", "b.py"]}
    exempt = {"_observe": {"c.py"}}
    assert inventory.same_name_growth(live, base, exempt) == {
        "_observe": ["d.py"]}


def test_must_differ_pairs_are_live_and_give_a_reason():
    """A pair kept as two implementations says why, and still exists."""
    base = _baseline()
    pairs = {tuple(p) for p in base["near_duplicates"]}
    seen = set()
    for row in base.get("must_differ", []):
        assert set(row) == {"pair", "reason"}, row
        pair = tuple(row["pair"])
        assert pair in pairs, f"must_differ names a pair the baseline lacks: {pair}"
        assert pair not in seen, f"must_differ repeats {pair}"
        seen.add(pair)
        assert len(row["reason"].split()) >= 8, f"{pair} needs a real reason"


def test_primitive_digest_sites_only_shrink():
    """Raw digest sites outside the owner may only disappear (PB #1328)."""
    live = set(inventory.scan()["primitive_digest_sites"])
    base = set(_baseline()["primitive_digest_sites"])
    assert not live - base, (
        f"new primitive digest sites outside {sorted(inventory.DIGEST_OWNERS)}: "
        f"{sorted(live - base)} -- use the digest owner's named profiles instead "
        "of a new raw hashlib or sorted-JSON call (PB #1328)")
    assert not base - live, (
        f"baseline digest sites gone {sorted(base - live)}: shrink the baseline "
        "with tools/duplication_inventory.py --write-baseline")


def test_the_digest_site_ratchet_fails_on_a_new_raw_site_and_passes_the_owner(tmp_path):
    """RED/GREEN for the gate itself: one raw site outside the owner fails.

    A fixture module with a fresh ``hashlib.sha256(json.dumps(...,
    sort_keys=True))`` site is an offender against an empty baseline (the RED
    side: this is exactly the new-code pattern the ratchet exists to stop),
    while the same raw recipe inside ``src/prismabuild/core.py`` is not
    ratcheted at all because that is where consolidation moves sites to.
    """
    raw = (
        "import hashlib\nimport json\n"
        "def _digest(value):\n"
        "    return hashlib.sha256(\n"
        "        json.dumps(value, sort_keys=True).encode()).hexdigest()\n"
    )
    (tmp_path / "src" / "prismabuild").mkdir(parents=True)
    (tmp_path / "tools").mkdir()
    (tmp_path / "src" / "prismabuild" / "core.py").write_text(raw)
    (tmp_path / "tools" / "fresh_site.py").write_text(raw)
    live = set(inventory.scan(tmp_path)["primitive_digest_sites"])
    # The owner's own raw site is not ratcheted.
    assert not any("src/prismabuild/core.py" in site for site in live)
    # Against an empty baseline the fresh raw site is a violation (RED).
    assert sorted(live - set()) == [
        "hashlib:tools/fresh_site.py::_digest",
        "sorted-json:tools/fresh_site.py::_digest",
    ]
    # With the site recorded, the same set arithmetic the shrink test uses
    # reports no violation (GREEN).
    baseline = {"hashlib:tools/fresh_site.py::_digest",
                "sorted-json:tools/fresh_site.py::_digest"}
    assert not live - baseline and not baseline - live
