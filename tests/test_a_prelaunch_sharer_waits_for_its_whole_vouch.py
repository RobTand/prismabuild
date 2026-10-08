"""A declared prelaunch consumer is admitted onto its whole vouch, not part of it.

2026-10-08 04:58Z, dl380g10: a strict CPU consumer declared 899 entries
``resident_before_launch``.  Its lead was a mover another consumer had sealed,
so the range was shared (#1026) and the tier loop's fan-out gave the sharer its
own fragment, for the entries the source material had dated by then.  The
claim gate (``residency_verdict``) calls a map current when it names every
lead, and the one-entry fragment already did.  The consumer claimed, read
``frontier.json`` (entry 884) and failed with "no published covering lease".
The sharer's complete fragment was filed five seconds after the failure.

Everything runs on a ``tmp_path`` queue through the real fan-out and the real
tier cycle.  Nothing touches a live queue.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import (  # noqa: E402
    adaptive_cpu, pool, reader_lease, residency_map, residency_plan)
import tier_loop  # noqa: E402
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _claim, _fixture_queue, _land, _publish_consumer)
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    GIB, PHASE_GIB, TIER, _hexkey, _tier_record)
from test_n_consumers_of_one_range_share_one_copy import (  # noqa: E402
    MANIFEST, TOOLS_A, _consumers, _fan_out, _namespace, _shared_plan)

FIRST = _hexkey("1594first")
SECOND = _hexkey("1594second")
LATE = "/pool/late/frontier.json"


def _declare(monkeypatch) -> None:
    """Every phase of the plans built from here declares ``resident_before_launch``."""

    real = residency_plan.build_plan
    monkeypatch.setattr(residency_plan, "build_plan", lambda **kw: real(
        **{**kw, "phases": [{**phase, "resident_before_launch": True}
                            for phase in kw["phases"]]}))


def _shared(tmp_path: Path, *, declared: bool, monkeypatch):
    """Two sharers of one declared range; the source holds one dated entry and
    one the material has not dated yet (the last entry of the manifest)."""

    queue, stage = _fixture_queue(tmp_path, 20)
    # The lead's range is already on the tier and its tokens are held, which
    # is what an adopted lead is (#598); the gate then reads the map, which is
    # the part under test.
    monkeypatch.setattr(queue, "_lead_was_adopted", lambda *args: True)
    if declared:
        _declare(monkeypatch)
    plans = {key: _shared_plan(queue, key, label=key[:12], phases=1)
             for key in (FIRST, SECOND)}
    for key, plan in plans.items():
        _publish_consumer(queue, key, plan, manifest=MANIFEST)
    mover = str(plans[FIRST]["phases"][0]["mover_row"]["action_key"])  # type: ignore[index]
    phase = plans[FIRST]["phases"][0]                                  # type: ignore[index]
    start, end = int(phase["start_bytes"]), int(phase["end_bytes"])
    namespace = _namespace(FIRST, MANIFEST, start, end)
    _land(queue, stage, consumer=namespace, manifest=MANIFEST, mover=mover,
          name="phase-0", start=start, end=end)
    root = queue.residency_fragment_root()
    source = json.loads(residency_map.fragment_path(root, namespace, mover).read_text())
    key = residency_map.residency_map_key(LATE, 0)
    source["entries"][key] = {"stage_path": str(stage / "late.bin"), "bytes": 1,
                              "offset": 0, "sha256": "a" * 64}
    residency_map.write_fragment(root, source)
    return queue, stage, plans, mover, namespace, key


def _cycle(queue: pool.PoolQueue, stage: Path) -> None:
    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(),
                    discover=lambda **_kwargs: {TIER: _tier_record(stage, gib=20)})


def _verdict(queue: pool.PoolQueue, key: str) -> dict[str, object]:
    item = json.loads(queue.item_path(pool.READY, key).read_text())
    return queue.residency_verdict(item)


def _date_the_late_entry(queue, stage, namespace, mover, key) -> None:
    """The material now dates the entry the fan-out was waiting for."""

    root = queue.residency_fragment_root()
    material = reader_lease.read_material(root, namespace, mover)
    assert isinstance(material, dict)
    entries = dict(material["entries"])
    source = json.loads(residency_map.fragment_path(root, namespace, mover).read_text())
    late = source["entries"][key]
    (stage / "late.bin").write_bytes(b"x")
    entries[key] = {**late, "file_id": reader_lease.stat_identity(str(stage / "late.bin"))}
    reader_lease.write_material(
        root, consumer_action_key=namespace, mover_action_key=mover, tier_id=TIER,
        stage_root=str(stage), manifest_sha256=MANIFEST,
        generation=reader_lease.adopted_generation(material), entries=entries)


def test_a_declared_sharer_is_not_admitted_on_a_partial_vouch(
        tmp_path: Path, monkeypatch) -> None:
    queue, stage, _plans, mover, _ns, key = _shared(
        tmp_path, declared=True, monkeypatch=monkeypatch)
    _fan_out(queue, stage)
    _cycle(queue, stage)
    root = queue.residency_fragment_root()
    mine = json.loads(residency_map.fragment_path(root, SECOND, mover).read_text())
    assert key not in mine["entries"], "the fixture must reproduce the partial fan-out"
    assert [mover] == list(residency_map.read_map(
        queue.residency_map_path(SECOND))["leads"]), "the map names its lead"
    verdict = _verdict(queue, SECOND)
    assert verdict["state"] == "map_incomplete", verdict
    assert verdict["state"] in pool.RESIDENCY_REFUSAL_STATES
    assert verdict["missing"] == 1


def test_the_same_sharer_is_admitted_once_the_vouch_is_whole(
        tmp_path: Path, monkeypatch) -> None:
    """The fan-out completes the consumer's fragment a cycle before the map is
    recomposed from it.  The consumer reads the map, so it waits until then."""

    queue, stage, _plans, mover, namespace, key = _shared(
        tmp_path, declared=True, monkeypatch=monkeypatch)
    _fan_out(queue, stage)
    _cycle(queue, stage)
    assert _verdict(queue, SECOND)["state"] == "map_incomplete"
    _date_the_late_entry(queue, stage, namespace, mover, key)
    _fan_out(queue, stage)                      # the fragment is whole ...
    root = queue.residency_fragment_root()
    mine = json.loads(residency_map.fragment_path(root, SECOND, mover).read_text())
    assert key in mine["entries"], "the fixture must complete the fragment first"
    assert key not in residency_map.read_map(queue.residency_map_path(SECOND))["entries"]
    held, passes = queue.ledger().held(), queue.passes(SECOND)
    verdict = _verdict(queue, SECOND)           # ... and the map is not yet
    assert verdict["state"] == "map_incomplete", verdict
    assert queue.ledger().held() == held and queue.passes(SECOND) == passes
    _cycle(queue, stage)                        # recomposed
    assert _verdict(queue, SECOND)["state"] == "resident"


def test_control_an_undeclared_sharer_still_streams_on_a_partial_vouch(
        tmp_path: Path, monkeypatch) -> None:
    """Streaming consumers read lazily; only a declared prefix must be whole."""

    queue, stage, _plans, _mover, _ns, _key = _shared(
        tmp_path, declared=False, monkeypatch=monkeypatch)
    _fan_out(queue, stage)
    _cycle(queue, stage)
    assert _verdict(queue, SECOND)["state"] == "resident"


def test_control_a_declared_consumer_of_its_own_range_is_admitted(
        tmp_path: Path, monkeypatch) -> None:
    """Not a sharer: its own mover's fragment is the vouch, filed under the
    consumer.  There is no source fragment to compare, so nothing to wait for."""

    from test_a_consumer_stages_only_to_its_refill_horizon import _mover, _plan

    queue, stage = _fixture_queue(tmp_path, 20)
    monkeypatch.setattr(queue, "_lead_was_adopted", lambda *args: True)
    _declare(monkeypatch)
    owner = _hexkey("1594owner")
    manifest = _hexkey("1594ownmanifest")
    plan = _plan(queue, owner, label="own", manifest=manifest, phases=1)
    _publish_consumer(queue, owner, plan, manifest=manifest)
    phase = plan["phases"][0]                                    # type: ignore[index]
    _land(queue, stage, consumer=owner, manifest=manifest, mover=_mover("own", 0),
          name="phase-0", start=int(phase["start_bytes"]), end=int(phase["end_bytes"]))
    _fan_out(queue, stage)
    _cycle(queue, stage)
    assert _verdict(queue, owner)["state"] == "resident"


def _denial(queue: pool.PoolQueue, key: str) -> dict[str, object] | None:
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    matching = [entry for entry in records.values()
                if isinstance(entry, dict) and entry.get("action_key") == key]
    return max(matching, key=lambda entry: float(entry.get("denied_unix", 0.0)),
               default=None)


def test_the_claim_pass_waits_and_names_the_denial(
        tmp_path: Path, monkeypatch) -> None:
    """A wait, not a refusal: the item stays ready, no token is taken, no pass ages."""

    queue, stage, _plans, mover, namespace, key = _shared(
        tmp_path, declared=True, monkeypatch=monkeypatch)
    _fan_out(queue, stage)
    _cycle(queue, stage)
    held = queue.ledger().held()
    assert queue.claim(owner="worker", capacity={"cpu": 4, "mem_gb": 4}) is None
    assert queue.item_path(pool.READY, SECOND).exists()
    denial = _denial(queue, SECOND)
    assert denial is not None and denial["reason"] == "residency_map_incomplete", denial
    evidence = denial["evidence"]["residency"]                     # type: ignore[index]
    assert evidence["unvouched"] == {mover: 1}
    assert queue.ledger().held() == held
    assert queue.passes(SECOND) == 0


def _partial_sharer(tmp_path: Path, monkeypatch):
    queue, stage, _plans, mover, namespace, key = _shared(
        tmp_path, declared=True, monkeypatch=monkeypatch)
    _fan_out(queue, stage)
    _cycle(queue, stage)
    assert _verdict(queue, SECOND)["state"] == "map_incomplete"
    source = residency_map.fragment_path(queue.residency_fragment_root(), namespace, mover)
    return queue, source


def _waits(queue: pool.PoolQueue, key: str, source: Path) -> None:
    """The claim pass keeps the item ready, takes no token, ages no pass, and
    the denial names the source it could not read."""

    held, passes = queue.ledger().held(), queue.passes(key)
    assert queue.claim(owner="worker", capacity={"cpu": 4, "mem_gb": 4}) is None
    assert queue.item_path(pool.READY, key).exists()
    denial = _denial(queue, key)
    assert denial is not None and denial["reason"] == "residency_map_unreadable", denial
    assert str(source) in denial["evidence"]["residency"]["error"]   # type: ignore[index]
    assert queue.ledger().held() == held and queue.passes(key) == passes


def test_a_source_fragment_that_cannot_be_statted_is_a_wait_not_a_pass(
        tmp_path: Path, monkeypatch) -> None:
    queue, source = _partial_sharer(tmp_path, monkeypatch)
    real = Path.stat

    def stat(self, *args, **kwargs):
        if self == source:
            raise OSError(5, "Input/output error")
        return real(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", stat)
    verdict = _verdict(queue, SECOND)
    assert verdict["state"] == "map_unreadable", verdict
    assert verdict["state"] in pool.RESIDENCY_REFUSAL_STATES
    _waits(queue, SECOND, source)


def test_a_malformed_source_fragment_is_a_wait_not_a_pass(
        tmp_path: Path, monkeypatch) -> None:
    queue, source = _partial_sharer(tmp_path, monkeypatch)
    source.write_text("{ not json")
    verdict = _verdict(queue, SECOND)
    assert verdict["state"] == "map_unreadable", verdict
    _waits(queue, SECOND, source)
