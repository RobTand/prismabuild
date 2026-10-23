"""Acceptance contract for #2100's NEW, test-only connected CPU harness.

Implement tests/fullstack_lifecycle_harness.py, exposing
    run_case(case, root=Path, sources=dict[str, Path], observe=callable) -> dict
Cases are lifecycle, progress, recovery. observe(name, context) runs synchronously
at acknowledged process barriers, BEFORE the harness releases those barriers.
It must expose real queue/process objects and publications, not asserted booleans.
All case roots and child files belong under root. Sources are immutable; split
means the second complete fixture entry, not an arbitrary cut through an entry.

Common observer context: queue, residency_root, consumer, mover, tier_id,
stage_root, protected_path. Publication context adds publications (whole, split,
ram), each naming consumer/mover/tier_id/stage_root/manifest_sha256/epoch and an
expected mapping of range keys to source bytes. Held contexts add live_processes
(multiprocessing.Process handles), and pins (acquire result dictionaries).

The harness owns synchronization, worker execution, real PB calls and finally
cleanup. It must not manufacture worker acceptance, scope attestations, or CAS
results to satisfy this contract. Code review still checks that authority/source
requirement: a writable fixture queue cannot authenticate its own author.
Native qualification may be unavailable; that is a reasoned 'unqualified' cell,
NEVER permission to release a crashed reader's refs. File RAM is always labelled
file_backed and is not native RAM-broker qualification. No fleet submission here.
"""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import client, reader_lease, residency_map, staged_path  # noqa: E402
from prismabuild import residency_plan, tier_cycle  # noqa: E402
from prismabuild.action_result import ActionResultError  # noqa: E402
from prismabuild.record_store import CLAIMED  # noqa: E402


def _pb2100_lane():
    path = Path(__file__).with_name("fullstack_lifecycle_harness.py")
    assert path.is_file(), (
        "#2100 is not implemented: add a connected real-interface CPU lifecycle "
        "harness; the existing fullstack primitive suites are not restart evidence"
    )
    spec = importlib.util.spec_from_file_location("pb2100_lifecycle", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    assert callable(getattr(module, "run_case", None)), "harness needs run_case"
    return module


def _pb2100_sources(root):
    root.mkdir(parents=True)
    paths = {name: root / name for name in ("whole.bin", "split.bin")}
    paths["whole.bin"].write_bytes(bytes(range(256)) * 16)
    # Three entry-aligned extents, each with deliberately distinct content.
    paths["split.bin"].write_bytes(b"a" * 4096 + b"b" * 4096 + b"c" * 4096)
    return paths


def _pb2100_live(context):
    processes = context["live_processes"]
    assert len(processes) >= 2, "reader and competing actor must be real processes"
    assert len({p.pid for p in processes}) == len(processes)
    for process in processes:
        assert process.pid not in (None, os.getpid())
        assert process.is_alive(), "observer must run inside the acknowledged hold"


def _pb2100_owners(context):
    owners, tainted = reader_lease.live_for(
        context["queue"], None, residency_root=context["residency_root"])
    assert tainted == [], f"unknown leases are not an empty census: {tainted}"
    return owners


def _pb2100_protect(context):
    """Independently try retirement while the real reader's barrier is held."""
    queue = context["queue"]
    path = Path(context["protected_path"])
    assert str(path) in _pb2100_owners(context)
    before = reader_lease.stat_identity(path)
    content = path.read_bytes()
    ledger = queue.tier(context["tier_id"])
    holdings = ledger.holdings(context["mover"])
    supply = ledger.free_supply()
    assert holdings, "test needs charged occupancy, not an uncharged staged file"
    staged_path.evict(
        queue, context["mover"], consumer_action_key=context["consumer"],
        stage_root=str(context["stage_root"]),
        residency_root=context["residency_root"], whole=True)
    assert path.read_bytes() == content
    assert reader_lease.stat_identity(path) == before
    assert str(path) in _pb2100_owners(context)
    assert ledger.holdings(context["mover"]) == holdings
    assert ledger.free_supply() == supply


def _pb2100_publications(context, sources):
    publications = context["publications"]
    assert {p["kind"] for p in publications} >= {"whole", "split", "ram"}
    for publication in publications:
        material = reader_lease.read_material(
            context["residency_root"], publication["consumer"], publication["mover"])
        assert isinstance(material, dict), f"missing/tainted material: {material}"
        assert material["manifest_sha256"] == publication["manifest_sha256"]
        assert material["tier_id"] == publication["tier_id"]
        assert material["stage_root"] == str(publication["stage_root"])
        assert material.get("epoch", "") == publication["epoch"]
        receipt = context["queue"].move_record(publication["mover"], fresh=True)
        assert receipt is not None and receipt["complete"] is True
        assert receipt["action_key"] == publication["mover"]
        assert receipt["consumer_action_key"] == publication["consumer"]
        assert receipt["manifest_sha256"] == publication["manifest_sha256"]
        expected = publication["expected"]
        assert set(expected) == set(material["entries"])
        expected_bytes = b"".join(expected.values())
        if publication["kind"] in {"whole", "ram"}:
            assert expected_bytes == sources["whole.bin"].read_bytes()
        if publication["kind"] == "split":
            assert expected_bytes == sources["split.bin"].read_bytes()[4096:8192]
        selection = client.covers_for_keys(
            context["residency_root"], publication["consumer"], list(expected),
            tier_id=publication["tier_id"],
            manifest_sha256=publication["manifest_sha256"],
            epoch=publication["epoch"])
        assert selection["ok"] is True
        for key, payload in expected.items():
            entry = material["entries"][key]
            assert Path(entry["stage_path"]).read_bytes() == payload
            assert reader_lease.file_id_matches(
                entry["file_id"], reader_lease.stat_identity(entry["stage_path"]))
    mapping = residency_map.read_map(context["map_path"])
    assert any(entry.get("ram_path") for entry in mapping["entries"].values())


def _pb2100_finished(result, root):
    queue = result["queue"]
    assert (root / "sources" / "whole.bin").read_bytes() == bytes(range(256)) * 16
    assert (root / "sources" / "split.bin").read_bytes() == (
        b"a" * 4096 + b"b" * 4096 + b"c" * 4096)
    assert Path(queue.root).resolve().is_relative_to(root.resolve())
    residency_root = Path(result["residency_root"])
    assert residency_root.resolve().is_relative_to(root.resolve())
    owners, tainted = reader_lease.live_for(queue, None, residency_root=residency_root)
    assert tainted == [], "unknown lease evidence is not a successful census"
    retained = result.get("retained", {})
    if retained:
        assert result["recovery"]["status"] == "unqualified"
        assert retained["reason"], "unsafe cleanup must remain explicitly blocked"
    assert owners == retained.get("owners", {}), "fixture refs leaked or retired unsafely"
    claims = sorted(p.name for p in (queue.root / CLAIMED).glob("*.json"))
    assert claims == retained.get("claims", []), "unaccounted fixture claims"
    for process in result["processes"]:
        assert process.pid not in (None, os.getpid())
        assert not process.is_alive(), "fixture child escaped cleanup"
        assert process.exitcode is not None, "child not reaped"
    for tier_id in result["tier_ids"]:
        assert queue.tier(tier_id).holdings() == retained.get("holdings", {}).get(tier_id, {}), (
            "fixture occupancy leaked or was returned without retirement authority")
    assert result["ram_fixture"] == "file_backed"
    for name in ("native_containment", "ram_broker"):
        capability = result["capabilities"][name]
        assert capability["status"] in {"qualified", "unqualified"}
        assert capability["evidence"], "capability result needs an observation/reason"
    assert result["capabilities"]["ram_broker"]["status"] == "unqualified", (
        "this portable file-backed lane cannot qualify a native RAM broker")


def test_connected_movement_leases_and_generation_boundaries(tmp_path):
    lane = _pb2100_lane()
    sources = _pb2100_sources(tmp_path / "sources")
    seen = []
    previous_generation = None

    def observe(name, context):
        nonlocal previous_generation
        seen.append(name)
        if name == "published":
            _pb2100_publications(context, sources)
        elif name in {"readers_held", "copy_evict_held", "one_reader_left"}:
            _pb2100_live(context)
            _pb2100_protect(context)
            pins = context["pins"]
            assert len(pins) >= (2 if name == "readers_held" else 1)
            if name == "readers_held":
                assert len({p["pin"]["owner_action_key"] for p in pins}) >= 2, (
                    "shared-owner coverage needs distinct owners, not only duplicate refs")
            for acquired in pins:
                pin = reader_lease.validate_pin(acquired["pin"])
                fd, serving = client.open_pinned(
                    context["queue"], pin, acquired["ref_id"], context["range_key"],
                    residency_root=context["residency_root"])
                try:
                    assert os.read(fd, 16384) == context["expected_bytes"]
                    assert serving["pin_id"] == pin["pin_id"]
                finally:
                    os.close(fd)
            material = reader_lease.read_material(
                context["residency_root"], context["consumer"], context["mover"])
            if previous_generation is None:
                previous_generation = material["generation"]
            assert material["generation"] == previous_generation
        elif name == "wrong_epoch":
            assert context["acquisition"]["ok"] is False
            _pb2100_protect(context)
        elif name == "republished":
            material = reader_lease.read_material(
                context["residency_root"], context["consumer"], context["mover"])
            assert material["generation"] != previous_generation
            stale = context["old_acquisition"]
            with pytest.raises(reader_lease.ReaderLeaseError):
                client.open_pinned(
                    context["queue"], stale["pin"], stale["ref_id"],
                    context["range_key"], residency_root=context["residency_root"])
        elif name == "last_release":
            assert _pb2100_owners(context) == {}
            queue = context["queue"]
            args = dict(consumer_action_key=context["consumer"],
                        stage_root=str(context["stage_root"]),
                        residency_root=context["residency_root"])
            first = staged_path.evict(queue, context["mover"], **args)
            assert first["complete"] is True
            supply = queue.tier(context["tier_id"]).free_supply()
            second = staged_path.evict(queue, context["mover"], **args)
            assert second["complete"] is True
            assert queue.tier(context["tier_id"]).free_supply() == supply
            assert not Path(context["protected_path"]).exists()

    result = lane.run_case("lifecycle", root=tmp_path, sources=sources, observe=observe)
    assert seen == ["published", "readers_held", "copy_evict_held", "one_reader_left",
                    "wrong_epoch", "last_release", "republished"]
    _pb2100_finished(result, tmp_path)


def test_worker_accepted_progress_is_not_release_authority(tmp_path):
    lane = _pb2100_lane()
    sources = _pb2100_sources(tmp_path / "sources")
    seen = []

    def observe(name, context):
        seen.append(name)
        queue = context["queue"]
        observation = queue.records.accepted_progress_phase(
            context["consumer"], claimed_unix=context["claimed_unix"])
        assert observation is not None
        phase = observation["phase"]
        assert phase == ("p1" if name == "before_acceptance" else "p2")
        consumers = tier_cycle.live_consumers(queue)
        consumer = next(c for c in consumers if c["action_key"] == context["consumer"])
        assert consumer["accepted_phase"] == phase
        plan = residency_plan.read(queue, context["consumer"])
        assert plan is not None
        remaining = residency_plan.remaining(plan, consumer["accepted_phase"])
        assert remaining[0]["start_bytes"] == (0 if phase == "p1" else 4096)
        _pb2100_protect(context)

    result = lane.run_case("progress", root=tmp_path, sources=sources, observe=observe)
    assert seen == ["before_acceptance", "accepted", "repeated", "regressing",
                    "stale_attempt", "invalid"]
    _pb2100_finished(result, tmp_path)


def test_interruption_recovery_keeps_authority_and_expected_action_binding(tmp_path):
    lane = _pb2100_lane()
    sources = _pb2100_sources(tmp_path / "sources")
    seen = []

    def observe(name, context):
        seen.append(name)
        # These must be real refusal probes, not generated positive certificates.
        assert name in {"missing_scope", "wrong_scope"}
        ok, reason = client.containment_certificate_ok(
            context["queue"], context["certificate"])
        assert not ok and reason
        _pb2100_protect(context)

    result = lane.run_case("recovery", root=tmp_path, sources=sources, observe=observe)
    assert seen == ["missing_scope", "wrong_scope"]
    recovery = result["recovery"]
    assert recovery["status"] in {"qualified", "unqualified"}
    if recovery["status"] == "unqualified":
        assert recovery["reason"] and recovery["blocked_operation"]
        assert result["capabilities"]["native_containment"]["status"] == "unqualified"
        assert recovery["adoptions"] == 0, "unproven cleanup cannot authorize adoption"
    else:
        queue = result["queue"]
        key = recovery["action_key"]
        first, retry = recovery["attempts"]
        assert first["reason"] == recovery["interruption_reason"]
        assert first["reason"], "the interrupted attempt's terminal reason is retained"
        assert first["action_key"] == retry["action_key"] == key
        assert first["published_unix"] == retry["published_unix"]
        assert retry["attempt"] > first["attempt"]
        verified = client.read_verified_action_result(
            queue, key, published_unix=retry["published_unix"],
            attempt=retry["attempt"], max_result_bytes=65536)
        assert verified["payload"] == b"connected-lifecycle-result\n"
        assert recovery["adoptions"] == 1
        assert recovery["reverified_bytes"] > 0
        # A coherent receipt from another action/attempt is not our expected result.
        for wrong_key, wrong_attempt in (("0" * 64, retry["attempt"]),
                                         (key, first["attempt"])):
            with pytest.raises(ActionResultError):
                client.read_verified_action_result(
                    queue, wrong_key, published_unix=retry["published_unix"],
                    attempt=wrong_attempt, max_result_bytes=65536)
    _pb2100_finished(result, tmp_path)
