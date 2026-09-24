"""N consumers of one staged range share one copy, charged once (#1026).

Before #1026 every consumer sealed its own mover for every range it reads: a
mover's action key hashes its whole sealed body, and that body carries
``--consumer-action-key``.  Two consumers of one manifest therefore named two
movers for the same bytes.  Both copied into the same content-addressed staged
names, both claims took the range's tokens from free, and the census charged
the range once per consumer.

Now the first submitter of a range registers its mover under the range's
identity (``residency_plan.register_shared_range``), and every later
submitter of the same manifest range on the same tier names that mover.  The
mover files its fragment under the range's share namespace, the tier loop
gives each consumer still reading it its own copy of the vouch
(``tier_loop.fan_out_shared_ranges``), and an egress drops only its own
consumer's interest until the last one deletes.

The fixtures here build the shared plans through the registry when the tree
has one, and fall back to per-consumer movers when it does not.  The same
state then runs on a tree without #1026, which is how each test's red on
main is recorded.  Everything runs on ``tmp_path`` queues and stage roots
(#628).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import time
import types

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import (  # noqa: E402
    pool, progress as pb_progress, reader_lease, residency_map,
    residency_plan, storage_tiers, window_credit)
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    _claim, _fixture_queue, _land, _publish_consumer)
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    GIB, PHASE_GIB, STAGE_KIND, TIER, _hexkey, _row, _tier_record)

MANIFEST = _hexkey("1026shared")
FIRST = _hexkey("1026first")
SECOND = _hexkey("1026second")


# ------------------------------------------------------------------ fixtures


def _namespace(consumer: str, manifest: str, start: int, end: int) -> str:
    """Where a range's vouch is filed: its share namespace, or on a tree
    without #1026 the consumer that sealed it."""

    share = getattr(residency_plan, "share_namespace", None)
    return consumer if share is None else share(manifest, TIER, start, end)


def _shared_plan(queue: pool.PoolQueue, consumer: str, *, label: str,
                 manifest: str = MANIFEST, phases: int = 4,
                 phase_gib: int = PHASE_GIB) -> dict[str, object]:
    """One consumer's frozen plan, every stage mover registered for sharing.

    The per-consumer row is what the real seal makes for this consumer: a
    mover keyed by ``label``.  With the registry, the first consumer to
    register a range files its row and every later one takes that row, the
    way ``pbrun.residency_stage_rows`` does.  Without it, each consumer keeps
    its own row, which is the seal before #1026.
    """

    register = getattr(residency_plan, "register_shared_range", None)
    total = phases * phase_gib * GIB
    built = []
    for ordinal in range(phases):
        start, end = ordinal * phase_gib * GIB, (ordinal + 1) * phase_gib * GIB
        row = {
            **_row(queue, _hexkey(f"{label}mover{ordinal}"),
                   {STAGE_KIND: phase_gib, "cpu": 1, "mem_gb": 1}),
            "residency": {
                "schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                "manifest_sha256": manifest, "manifest_bytes": total,
                "range_start_bytes": start, "range_end_bytes": end},
        }
        if register is not None:
            record, _sealed = register(
                queue, manifest_sha256=manifest, tier_id=TIER, start=start,
                end=end, seal=lambda row=row: (row, {}),
                registered_by=consumer)
            row = dict(record["mover_row"])
        built.append({
            "name": f"phase-{ordinal}",
            "start_bytes": start, "end_bytes": end, "stage_gib": phase_gib,
            "mover_row": row,
            "egress_row": _row(queue, _hexkey(f"{label}egress{ordinal}"),
                               {"mem_gb": 1}),
        })
    return residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root="/stage/prewarm",
        manifest_sha256=manifest, manifest_bytes=total, phases=built)


def _sharers(queue: pool.PoolQueue, stage: Path, *, reading: dict[str, str],
             phases: int = 4, land: tuple[int, ...] = (0,),
             ) -> tuple[dict[str, dict[str, object]], dict[int, str]]:
    """Consumers of one manifest, claimed at the phases ``reading`` names.

    Returns their plans and the phases' movers (the first consumer's, which
    is every consumer's when ranges are shared).  ``land`` phases are staged
    once, under the range's namespace, the way the shared mover files them.
    """

    plans: dict[str, dict[str, object]] = {}
    for key in reading:
        plans[key] = _shared_plan(queue, key, label=key[:12], phases=phases)
        _publish_consumer(queue, key, plans[key], manifest=MANIFEST)
    first = next(iter(reading))
    movers = {ordinal: str(phase["mover_row"]["action_key"])
              for ordinal, phase in enumerate(plans[first]["phases"])}  # type: ignore[arg-type]
    for ordinal in land:
        phase = plans[first]["phases"][ordinal]              # type: ignore[index]
        start, end = int(phase["start_bytes"]), int(phase["end_bytes"])
        _land(queue, stage, consumer=_namespace(first, MANIFEST, start, end),
              manifest=MANIFEST, mover=movers[ordinal], name=f"phase-{ordinal}",
              start=start, end=end)
    now = time.time()
    for key, phase in reading.items():
        _claim(queue, key, phase=phase, claimed_unix=now - 1000.0,
               reported_unix=now - 10.0)
    return plans, movers


def _consumers(queue: pool.PoolQueue, stage: Path, gib: int = 20) -> list:
    return tier_loop._planned_consumers(queue, {TIER: _tier_record(stage, gib=gib)})


def _fan_out(queue: pool.PoolQueue, stage: Path) -> None:
    """The tier loop's fan-out, when the tree has one."""

    fan_out = getattr(tier_loop, "fan_out_shared_ranges", None)
    if fan_out is not None:
        fan_out(queue, _consumers(queue, stage))


def _held(queue: pool.PoolQueue) -> int:
    return int(queue.tier_ledger(TIER).held().get("stage_gib", 0))


def _egress(queue: pool.PoolQueue, stage: Path, plan: dict[str, object],
            ordinal: int) -> dict[str, object]:
    """Run one consumer's egress row for ``phase-<ordinal>`` through the CLI
    it is sealed with, and return the receipt it filed."""

    phase = plan["phases"][ordinal]                            # type: ignore[index]
    egress = str(phase["egress_row"]["action_key"])
    written = queue.root / f"egress-{egress[:16]}.json"
    code = stage_release.main([
        "--pool-root", str(queue.root), "--action-key", egress,
        "--mover-action-key", str(phase["mover_row"]["action_key"]),
        "--consumer-action-key", str(plan["consumer_action_key"]),
        "--stage-root", str(stage), "--receipt", str(written)])
    receipt = json.loads(written.read_text())
    assert code == 0, receipt
    return receipt


def _advance(queue: pool.PoolQueue, key: str, phase: str) -> None:
    """A claimed consumer reports ``phase``: its accepted progress moves on."""

    queue.write_lease(
        key, owner="horizon-fixture",
        claim_snapshot=json.loads(queue.item_path(pool.CLAIMED, key).read_text()),
        progress_observation={
            "source": "action-progress",
            "last_accepted": {"phase": phase, "units_completed": 2,
                              "reported_unix": time.time() - 5.0}})


def _files(stage: Path, ordinal: int) -> Path:
    return stage / MANIFEST[:8] / f"phase-{ordinal}" / "part-0.bin"


# ---------------------------------------------------------------- the seal


def _seal_for(tmp_path: Path, queue: pool.PoolQueue, consumer: str, *,
              share: str = "auto"):
    """Seal ``consumer``'s window through ``pbrun.residency_stage_rows``."""

    import pbrun
    from test_the_sealed_stage_leg_is_chunked_per_phase import (
        READERS, STAGE_TIER, _Cas, _manifest, _template)

    manifest_path = tmp_path / "manifest.json"
    if not manifest_path.exists():
        manifest_path.write_text(json.dumps(_manifest()))
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    records = {STAGE_TIER: {
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
        "tier_id": STAGE_TIER, "host": "dl380g10", "tier": "stage",
        "mountpoint": str(tmp_path / "stage"), "capacity_bytes": 512 * GIB}}
    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(),
                    discover=lambda **_kwargs: records)
    tier = pbrun.resolve_stage_tier(queue, None)
    args = types.SimpleNamespace(
        priority=-10, max_attempts=1, retry_safe=False,
        residency="stage", residency_tier=None, residency_mover_mem_gb=1,
        residency_mover_readers=READERS, residency_mover_max_attempts=3,
        residency_share=share)
    cas = _Cas(manifest_path)
    staged = pbrun.residency_stage_rows(
        _template(digest, manifest_path.stat().st_size),
        consumer_action_key=consumer, tier=tier, args=args, queue=queue,
        cas=cas)
    return staged, cas, digest, STAGE_TIER


def _legs(plan: dict[str, object]) -> list[tuple[str, str, int, int]]:
    """Every stage leg: (mover, egress, start, end), in read order."""

    out = []
    for phase in plan["phases"]:                               # type: ignore[union-attr]
        for leg in (phase.get("stage_chunks") or [phase]):
            out.append((str(leg["mover_row"]["action_key"]),
                        str(leg["egress_row"]["action_key"]),
                        int(leg["start_bytes"]), int(leg["end_bytes"])))
    return out


def test_two_submissions_of_one_range_seal_one_mover(tmp_path: Path) -> None:
    """Two consumers of one manifest, sealed by the real submitter: every
    leg names one mover, each consumer keeps its own egress, and the mover's
    argv files it under the range's share namespace, never under the
    consumer that happened to seal it first.  ``--residency-share off``
    seals per consumer, as before."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    first, first_cas, digest, stage_tier = _seal_for(tmp_path, queue, FIRST)
    second, second_cas, _digest, _tier = _seal_for(tmp_path, queue, SECOND)
    first_legs, second_legs = _legs(first["plan"]), _legs(second["plan"])

    assert [leg[0] for leg in first_legs] == [leg[0] for leg in second_legs]
    assert not {leg[1] for leg in first_legs} & {leg[1] for leg in second_legs}
    for mover, _egress_key, start, end in first_legs:
        namespace = residency_plan.share_namespace(digest, stage_tier, start, end)
        command = first_cas.actions[mover]["params"]["command"]
        assert command[command.index("--consumer-action-key") + 1] == namespace
        assert residency_plan.share_namespace_of(queue, mover) == namespace
        # The second submitter published no second body for the same mover.
        assert mover not in second_cas.actions

    third, _cas, _digest, _tier = _seal_for(
        tmp_path, queue, _hexkey("1026third"), share="off")
    assert not ({leg[0] for leg in _legs(third["plan"])}
                & {leg[0] for leg in first_legs})


# --------------------------------------------------------------- the census


def test_one_holder_per_range_and_the_census_charges_it_once(
        tmp_path: Path) -> None:
    """Both plans name one mover for ``phase-0``; it lands once, the ledger
    holds it once, the window reads it as staged for both, and the census
    names one holder for it."""

    queue, stage = _fixture_queue(tmp_path, 20)
    plans, movers = _sharers(queue, stage,
                             reading={FIRST: "phase-0", SECOND: "phase-0"})
    mover = movers[0]

    assert str(plans[SECOND]["phases"][0]["mover_row"]["action_key"]) == mover  # type: ignore[index]
    assert _held(queue) == PHASE_GIB
    for key in (FIRST, SECOND):
        _published, staged = tier_loop._mover_state(queue, plans[key], TIER)
        assert mover in staged
    census = tier_loop._commitment_census(
        queue, {TIER: _tier_record(stage, gib=20)},
        consumers=_consumers(queue, stage), remember=False)[TIER]
    assert [entry["gib"] for entry in census["holders"]] == [PHASE_GIB]
    assert census["held_gib"] == PHASE_GIB


def test_a_shared_range_is_evictable_only_once_every_reader_passed_it(
        tmp_path: Path) -> None:
    """One reader past ``phase-0`` and the other still reading it: the range
    is in the second one's horizon and not evictable.  Both past it: it is
    evictable, and counted once."""

    queue, stage = _fixture_queue(tmp_path, 20)
    _plans, movers = _sharers(queue, stage,
                              reading={FIRST: "phase-1", SECOND: "phase-0"})
    tiers = {TIER: _tier_record(stage, gib=20)}

    census = tier_loop._commitment_census(
        queue, tiers, consumers=_consumers(queue, stage), remember=False)[TIER]
    holder = {entry["key"]: entry for entry in census["holders"]}[movers[0]]
    assert holder["evictable"] is False, census["holders"]
    assert census["evictable_gib"] == 0

    _advance(queue, SECOND, "phase-1")
    census = tier_loop._commitment_census(
        queue, tiers, consumers=_consumers(queue, stage), remember=False)[TIER]
    holder = {entry["key"]: entry for entry in census["holders"]}[movers[0]]
    assert holder["evictable"] is True, census["holders"]
    assert census["evictable_gib"] == PHASE_GIB


# ---------------------------------------------------------------- the egress


def test_one_readers_egress_leaves_the_range_and_the_last_one_deletes(
        tmp_path: Path) -> None:
    """The first reader past ``phase-0`` runs its egress row: its own vouch
    goes, the file, the tokens and the second reader's vouch stay.  The
    second reader then passes it and runs its own: the file is deleted and
    the tokens come back, once."""

    queue, stage = _fixture_queue(tmp_path, 20)
    plans, movers = _sharers(queue, stage,
                             reading={FIRST: "phase-0", SECOND: "phase-0"})
    root = queue.residency_fragment_root()
    _fan_out(queue, stage)
    for key in (FIRST, SECOND):
        assert residency_map.fragment_path(root, key, movers[0]).exists()
    _advance(queue, FIRST, "phase-1")

    first = _egress(queue, stage, plans[FIRST], 0)

    assert _files(stage, 0).exists(), first
    assert _held(queue) == PHASE_GIB
    assert first["tokens_released"] == 0 and first["tokens_decharged"] == 0
    assert residency_map.fragment_path(root, SECOND, movers[0]).exists()
    assert not residency_map.fragment_path(root, FIRST, movers[0]).exists()
    assert first.get("interest_dropped") is True, first

    _advance(queue, SECOND, "phase-1")
    second = _egress(queue, stage, plans[SECOND], 0)

    assert not _files(stage, 0).exists(), second
    assert _held(queue) == 0
    assert second["tokens_released"] == PHASE_GIB
    for namespace in (SECOND, _namespace(FIRST, MANIFEST, 0, PHASE_GIB * GIB)):
        assert not residency_map.fragment_path(root, namespace, movers[0]).exists()


def test_a_dead_sharers_eviction_keeps_the_range_for_the_live_one(
        tmp_path: Path) -> None:
    """The dead-owner sweep evicts a failed consumer's ranges whole, named
    for that consumer.  A shared range another consumer still reads is not
    that consumer's to delete or to decharge: its interest goes, nothing
    else."""

    queue, stage = _fixture_queue(tmp_path, 20)
    plans, movers = _sharers(queue, stage,
                             reading={FIRST: "phase-0", SECOND: "phase-0"})
    _fan_out(queue, stage)
    claimed = queue.item_path(pool.CLAIMED, SECOND)
    record = json.loads(claimed.read_text())
    claimed.unlink()
    queue.item_path(pool.FAILED, SECOND).write_text(
        json.dumps({**record, "status": "failed"}))

    receipt = stage_release.evict(
        queue, movers[0], consumer_action_key=SECOND, stage_root=str(stage),
        reason="dead-owner", whole=True)

    assert receipt["complete"] is True, receipt
    assert receipt["tokens_released"] == 0 and receipt["tokens_decharged"] == 0
    assert _files(stage, 0).exists()
    assert _held(queue) == PHASE_GIB
    assert residency_map.fragment_path(
        queue.residency_fragment_root(), FIRST, movers[0]).exists()


def test_a_dead_consumers_shared_mover_is_not_withdrawn_while_another_reads_it(
        tmp_path: Path) -> None:
    """A failed consumer's queued movers are withdrawn (#620), but a shared
    range's mover is every sharer's: it stays while another live consumer
    still reads the range, and goes once none does."""

    queue, stage = _fixture_queue(tmp_path, 20)
    plans, movers = _sharers(queue, stage, land=(),
                             reading={FIRST: "phase-0", SECOND: "phase-0"})
    queue.publish(**dict(plans[FIRST]["phases"][1]["mover_row"]))  # type: ignore[index]
    for key in (FIRST,):
        claimed = queue.item_path(pool.CLAIMED, key)
        record = json.loads(claimed.read_text())
        claimed.unlink()
        queue.item_path(pool.FAILED, key).write_text(
            json.dumps({**record, "status": "failed"}))

    tier_loop.withdraw_dead_consumer_movers(queue)

    assert queue.item_path(pool.READY, movers[1]).exists()
    assert not queue.item_path(pool.WITHDRAWN, movers[1]).exists()

    claimed = queue.item_path(pool.CLAIMED, SECOND)
    record = json.loads(claimed.read_text())
    claimed.unlink()
    queue.item_path(pool.FAILED, SECOND).write_text(
        json.dumps({**record, "status": "failed"}))
    tier_loop.withdraw_dead_consumer_movers(queue)

    assert not queue.item_path(pool.READY, movers[1]).exists()
    assert queue.item_path(pool.WITHDRAWN, movers[1]).exists()


# ------------------------------------------------------ the crash, the stale


TOKEN = "t" * 32


def test_a_mover_crash_with_two_dependents_copies_once_and_both_wait(
        tmp_path: Path) -> None:
    """A shared mover's worker dies mid-copy.  The reaper puts the one row
    back; both consumers' staged waits name it as their own dependency and
    stay exempt, and there is one row to copy the range, not two."""

    queue, stage = _fixture_queue(tmp_path, 20)
    plans, movers = _sharers(queue, stage, land=(),
                             reading={FIRST: "phase-0", SECOND: "phase-0"})
    row = dict(plans[FIRST]["phases"][0]["mover_row"])     # type: ignore[index]
    queue.publish(**row, max_attempts=3, retry_safe=True)
    ready = queue.item_path(pool.READY, movers[0])
    record = json.loads(ready.read_text())
    ready.unlink()
    record.update({"claimed_unix": 1.0, "claimed_by": "crashed-worker",
                   "claimed_host": "dl380g10"})
    queue.item_path(pool.CLAIMED, movers[0]).write_text(json.dumps(record))

    queue.reap_stale(timeout_s=0.0)

    assert queue.item_path(pool.READY, movers[0]).exists()
    ranged = [item for item in queue.ready_items()
              if isinstance(item, dict)
              and (item.get("residency") or {}).get("range_start_bytes") == 0]
    assert [item["action_key"] for item in ranged] == [movers[0]]
    for key in (FIRST, SECOND):
        progress_path = tmp_path / f"{key[:8]}.progress"
        Path(pb_progress.staged_wait_path(str(progress_path))).write_text(
            json.dumps({"schema": pb_progress.STAGED_WAIT_SCHEMA_V1,
                        "token": TOKEN, "since_unix": 1.0,
                        "movers": [movers[0]]}))
        verdict = queue.staged_wait_verdict(key, progress_path, token=TOKEN)
        assert verdict is not None
        assert verdict["exempt"] is True, verdict
        assert verdict["movers"][0]["key"] == movers[0]
        assert verdict["movers"][0]["state"] == pool.READY


def test_a_stale_done_shared_mover_holds_nothing_and_is_staged_again(
        tmp_path: Path) -> None:
    """A shared mover in ``done/`` whose range was evicted holds no tokens.
    Neither sharer reads it as staged, and the window publishes it again,
    once, for both."""

    queue, stage = _fixture_queue(tmp_path, 20)
    plans, movers = _sharers(queue, stage, land=(),
                             reading={FIRST: "phase-0", SECOND: "phase-0"})
    row = dict(plans[FIRST]["phases"][0]["mover_row"])     # type: ignore[index]
    done = queue.item_path(pool.DONE, movers[0])
    done.parent.mkdir(parents=True, exist_ok=True)
    done.write_text(json.dumps({**row, "status": "done"}))

    for key in (FIRST, SECOND):
        published, staged = tier_loop._mover_state(queue, plans[key], TIER)
        assert movers[0] not in published and movers[0] not in staged

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(),
                    discover=lambda **_kwargs: {TIER: _tier_record(stage, gib=20)})

    lead = [item for item in queue.ready_items() if isinstance(item, dict)
            and (item.get("residency") or {}).get("range_start_bytes") == 0
            and (item.get("residency") or {}).get("range_end_bytes")
            == PHASE_GIB * GIB]
    assert [item["action_key"] for item in lead] == [movers[0]]


# ------------------------------------------------------------ the claim order


def test_the_claim_order_spends_a_shared_range_once() -> None:
    """Two blocked consumers need the same 22 GiB range and 30 GiB is free:
    the first is granted, and the second is granted too, riding the first's
    grant, instead of being the head short of 14 GiB."""

    order = window_credit.claim_order([
        {"consumer": FIRST, "claimed_unix": 1.0, "blocked": True,
         "need_gib": 22, "need_mover": "h" * 64},
        {"consumer": SECOND, "claimed_unix": 2.0, "blocked": True,
         "need_gib": 22, "need_mover": "h" * 64},
    ], free_gib=30)

    standings = {entry["consumer"]: entry["standing"] for entry in order["entries"]}
    assert standings == {FIRST: window_credit.CLAIM_GRANTED,
                         SECOND: window_credit.CLAIM_GRANTED}, order
    assert order["head"] is None
    assert order["target_free_gib"] == 22


# ----------------------------------------------------------- the fan-out


def test_the_fan_out_copies_dated_entries_only_and_both_maps_name_the_range(
        tmp_path: Path) -> None:
    """The fan-out gives each reader its own copy of the vouch, and only for
    entries the shared material dates, material first: a fragment entry
    nothing dates would read to every later publisher of the same name as a
    vouch still pending (#1087)."""

    queue, stage = _fixture_queue(tmp_path, 20)
    _plans, movers = _sharers(queue, stage,
                              reading={FIRST: "phase-0", SECOND: "phase-0"})
    root = queue.residency_fragment_root()
    namespace = _namespace(FIRST, MANIFEST, 0, PHASE_GIB * GIB)
    source_path = residency_map.fragment_path(root, namespace, movers[0])
    source = json.loads(source_path.read_text())
    undated = residency_map.residency_map_key("/pool/undated/part-9.bin", 0)
    source["entries"][undated] = {"stage_path": str(stage / "undated.bin"),
                                  "bytes": 1, "offset": 0, "sha256": "a" * 64}
    residency_map.write_fragment(root, source)

    _fan_out(queue, stage)

    for key in (FIRST, SECOND):
        fragment = json.loads(residency_map.fragment_path(
            root, key, movers[0]).read_text())
        assert undated not in fragment["entries"]
        assert len(fragment["entries"]) == 1
        assert isinstance(reader_lease.read_material(root, key, movers[0]), dict)
    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(),
                    discover=lambda **_kwargs: {TIER: _tier_record(stage, gib=20)})
    for key in (FIRST, SECOND):
        mapped = residency_map.read_map(queue.residency_map_path(key))
        assert any(str(entry.get("stage_path", "")).endswith("phase-0/part-0.bin")
                   for entry in dict(mapped.get("entries") or {}).values()), mapped


# ----------------------------------------------------------- the campaign


CAMPAIGN_PHASES = 45
CAMPAIGN_GIB = 22
CAMPAIGN_CONSUMERS = 4
CAMPAIGN_HOSTS = ("sparky", "sparklina")


def _land_published(queue: pool.PoolQueue, stage: Path,
                    landings: dict[tuple[int, int], int]) -> None:
    """Every published mover finishes its copy: tokens, files, vouch, receipt.

    ``landings`` counts the copies per range, which is the number the
    campaign test holds to one.
    """

    for item in queue.ready_items():
        if not isinstance(item, dict):
            continue
        residency = item.get("residency")
        if not isinstance(residency, dict) or "range_start_bytes" not in residency:
            continue
        key = str(item["action_key"])
        start = int(residency["range_start_bytes"])
        end = int(residency["range_end_bytes"])
        queue.item_path(pool.READY, key).unlink()
        queue.item_path(pool.DONE, key).write_text(
            json.dumps({**item, "status": "done"}))
        landings[(start, end)] = landings.get((start, end), 0) + 1
        registered = getattr(residency_plan, "share_namespace_of", None)
        owner = (registered(queue, key) if registered is not None else None)
        _land(queue, stage, consumer=owner or FIRST, manifest=MANIFEST,
              mover=key, name=f"range-{start}", start=start, end=end)


def test_the_campaign_shape_copies_each_phase_once_charged_once(
        tmp_path: Path) -> None:
    """Four consumers on two hosts read one 45-phase plan of 22 GiB phases.
    Over the cycles the window runs, every range is copied at most once, one
    holder per range carries it, and the tier holds 22 GiB per landed range
    and no more."""

    queue, stage = _fixture_queue(tmp_path, 400)
    consumers = [_hexkey(f"1026campaign{n}") for n in range(CAMPAIGN_CONSUMERS)]
    plans = {}
    for key in consumers:
        plans[key] = _shared_plan(queue, key, label=key[:12],
                                  phases=CAMPAIGN_PHASES, phase_gib=CAMPAIGN_GIB)
        _publish_consumer(queue, key, plans[key], manifest=MANIFEST)
    now = time.time()
    for n, key in enumerate(consumers):
        _claim(queue, key, phase="phase-0", claimed_unix=now - 1000.0 + n,
               reported_unix=now - 10.0)
        claimed = queue.item_path(pool.CLAIMED, key)
        record = json.loads(claimed.read_text())
        record["claimed_host"] = CAMPAIGN_HOSTS[n % len(CAMPAIGN_HOSTS)]
        claimed.write_text(json.dumps(record))

    landings: dict[tuple[int, int], int] = {}
    for _cycle_n in range(4):
        tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                        receipts=tier_loop.ReceiptCache(),
                        discover=lambda **_kwargs: {
                            TIER: _tier_record(stage, gib=400)})
        _land_published(queue, stage, landings)

    assert landings, "the window published nothing: the test proves nothing"
    assert set(landings.values()) == {1}, landings
    ledger = queue.tier_ledger(TIER)
    ranges: dict[tuple[int, int], list[str]] = {}
    for holder in ledger.held_keys():
        receipt = queue.move_record(holder)
        if isinstance(receipt, dict) and ledger.holder_tokens(holder):
            ranges.setdefault((int(receipt["range_start_bytes"]),
                               int(receipt["range_end_bytes"])), []).append(holder)
    assert all(len(holders) == 1 for holders in ranges.values()), ranges
    held = {holder: int(ledger.holder_tokens(holder).get("stage_gib", 0))
            for holder in ledger.held_keys()}
    movers = sum(gib for holder, gib in held.items()
                 if not holder.startswith(window_credit.GRANT_PREFIX))
    fences = sum(gib for holder, gib in held.items()
                 if holder.startswith(window_credit.GRANT_PREFIX))
    assert movers == CAMPAIGN_GIB * len(landings), held
    # Every consumer is at the same place, so their windows share one
    # advance, fenced once rather than once per window.
    assert fences <= CAMPAIGN_GIB, held
