"""A mover is sealed at one copy's measured rate, never the tier's whole offer (#909).

A mover's fill seal is three things at once: the tokens it holds, so the
offer over the seal is how many copies run together; the rate ``_fell_short``
holds its delivery against; and the landing rate the tier loop assumes for
its plan until a copy lands.  pbrun sealed it at the largest single-reader
share any receipt on the tier had priced (440 MB/s live on 2026-09-23),
capped by the offer (428), so every seal was the whole offer and each copy
ran alone.

The seal is now, first of: the slowest landing among the copies of the same
manifest in its latest window, when that window holds two copies or more
(``landing``); the median single-reader share over the pool-reading receipts
of the tier's latest window (``single-reader-share``); nothing, when the
offer is the stated bound and ``demand_source`` names it.  Both statistics
read the latest window only, and a window of one copy prices no seal (#958):
over one copy the minimum bounds nothing.

Everything runs on ``tmp_path`` queues and stage roots (#628).
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import types

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import action_edges, pool, storage_tiers  # noqa: E402
import tier_loop  # noqa: E402
from test_a_stage_mover_declares_the_cpu_and_retries_it_owns import (  # noqa: E402
    CONSUMER, PHASE_BYTES, READERS, TIER, _Cas, _manifest, _template)

MANIFEST = "a" * 64
OLDER, NEWER = "e" * 64, "f" * 64
KIND = f"{storage_tiers.FILL_KIND}{storage_tiers.TIER_DEMAND_SEPARATOR}{TIER}"


def _landed(key: str, consumer: str, mb_s: float, *, unix: float,
            manifest: str = MANIFEST, **extra) -> dict[str, object]:
    """One complete copy that landed at ``mb_s``: 10 s over ``mb_s * 10`` MB."""

    return {"schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": key,
            "consumer_action_key": consumer, "tier_id": TIER,
            "manifest_sha256": manifest, "complete": True,
            "bytes_staged": int(mb_s * 10 * storage_tiers.MB), "seconds": 10.0,
            "unix": unix, **extra}


def _share(key: str, file_side: float, delivered: float, sharers: int
           ) -> dict[str, object]:
    """A receipt of another manifest that prices one reader's share."""

    return {"schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": key,
            "consumer_action_key": "b" * 64, "tier_id": TIER,
            "manifest_sha256": "9" * 64, "seconds": 44.5,
            "mb_per_s_file_side": file_side,
            storage_tiers.MOVER_CONCURRENCY_FIELD: sharers,
            "disk_pacing": {storage_tiers.POOL_FILL_FIELD: delivered},
            "unix": 1.0}


#: R11's window of R12's manifest, then R12's: live on 2026-09-23 the
#: slowest copies were 77.2 and 116.3 MB/s.
WINDOWS = [
    _landed("1" * 64, OLDER, 77.2, unix=100.0),
    _landed("2" * 64, OLDER, 150.0, unix=110.0),
    _landed("3" * 64, NEWER, 116.3, unix=200.0),
    _landed("4" * 64, NEWER, 180.0, unix=210.0),
]


def _price(records, manifest: str | None = MANIFEST) -> dict[str, object]:
    return storage_tiers.mover_fill_price(records, tier_id=TIER,
                                          manifest_sha256=manifest)


# ------------------------------------------------------------- the statistic


def test_the_seal_is_the_slowest_copy_of_the_latest_window() -> None:
    """R12's window prices the next seal, not R11's slower one.

    Receipts are append-only: the slowest copy ever measured would ratchet
    each generation's seal below the last.
    """

    price = _price(WINDOWS)
    assert price == {"mb_s": 116, "basis": "landing", "receipts_priced": 2,
                     "window_consumer": NEWER,
                     "landing": {"mb_s": 116, "copies": 2,
                                 "window_consumer": NEWER}}


def test_a_recopied_range_is_priced_at_its_latest_copy() -> None:
    """A copy landed again replaces its first rate, as the horizon's does."""

    again = _landed("3" * 64, NEWER, 160.0, unix=220.0)
    assert _price([*WINDOWS, again])["mb_s"] == 160


def test_a_window_that_read_the_stage_prices_nothing() -> None:
    """An adoption window read the stage, not the pool (#654)."""

    adopted = _landed("5" * 64, "d" * 64, 3.0, unix=300.0, disk_pacing={
        "pool_read_bytes": 1, storage_tiers.POOL_FILL_FIELD: 3.0})
    assert _price([*WINDOWS, adopted])["window_consumer"] == NEWER


def test_an_incomplete_or_sub_megabyte_copy_prices_nothing() -> None:
    partial = {**_landed("5" * 64, "d" * 64, 90.0, unix=300.0), "complete": False}
    crawl = _landed("6" * 64, "d" * 64, 0.4, unix=310.0)
    assert _price([*WINDOWS, partial, crawl])["window_consumer"] == NEWER


def test_with_no_copy_of_the_manifest_the_median_share_prices_it() -> None:
    """One reader's median share, not the largest: 100, not 440."""

    shares = [_share("7" * 64, 60.0, 50.0, 1), _share("8" * 64, 300.0, 200.0, 2),
              _share("9" * 64, 440.0, 1000.0, 1)]
    price = _price([*WINDOWS, *shares], manifest="c" * 64)
    assert price["mb_s"] == 100
    assert price["basis"] == "single-reader-share"
    assert storage_tiers.mover_fill_demand_from_receipts(
        shares, tier_id=TIER) == 100


# ------------------------------------------ the window, and a window of one (#958)


FLEET_OLD, FLEET_NEW = "5" * 64, "6" * 64


def _fleet(consumer: str, share: float, *, unix: float, count: int
           ) -> list[dict[str, object]]:
    """``count`` single-reader receipts of other manifests in one window.

    Each ran alone (one sharer) with its file side the binding term, so its
    share is exactly ``share``.
    """

    return [{**_share(f"{consumer[:1]}{index:063d}", share, 10 * share, 1),
             "consumer_action_key": consumer, "unix": unix + index}
            for index in range(count)]


def test_a_window_of_one_copy_prices_no_seal() -> None:
    """One slow copy is not a bound; the latest window's median share is.

    Live, manifest 2e607db1ebcc was sealed at 47 MB/s off one copy.  The
    minimum over a window is the rate every copy of it reached, which says
    something only when a copy other than the one that sets it stands beside
    it.  The seal names its basis, its n and its window; the lone landing
    stays on the price for the stall grace.
    """

    lone = _landed("1" * 64, NEWER, 47.0, unix=500.0)
    fleet = _fleet(FLEET_NEW, 138.0, unix=400.0, count=3)
    price = _price([lone, *fleet])
    assert price == {"mb_s": 138, "basis": "single-reader-share",
                     "receipts_priced": 3, "window_consumer": FLEET_NEW,
                     "landing": {"mb_s": 47, "copies": 1,
                                 "window_consumer": NEWER}}


def test_a_window_of_one_copy_is_not_rescued_by_an_older_window() -> None:
    """An older window of two copies is history, not this window's bound."""

    older = [_landed("1" * 64, OLDER, 40.0, unix=100.0),
             _landed("2" * 64, OLDER, 45.0, unix=110.0)]
    lone = _landed("3" * 64, NEWER, 47.0, unix=500.0)
    price = _price([*older, lone, *_fleet(FLEET_NEW, 138.0, unix=400.0,
                                          count=3)])
    assert (price["mb_s"], price["basis"]) == (138, "single-reader-share")


def test_with_no_copy_the_share_reads_only_the_latest_window() -> None:
    """An old, slow history does not lower the seal.

    Live: 59 MB/s over all 965 pool-reading receipts, 138 over the latest.
    """

    history = _fleet(FLEET_OLD, 59.0, unix=100.0, count=9)
    current = _fleet(FLEET_NEW, 138.0, unix=1000.0, count=3)
    price = _price([*history, *current], manifest="c" * 64)
    assert price == {"mb_s": 138, "basis": "single-reader-share",
                     "receipts_priced": 3, "window_consumer": FLEET_NEW,
                     "landing": None}
    assert storage_tiers.mover_fill_demand_from_receipts(
        [*history, *current], tier_id=TIER) == 138


@pytest.mark.parametrize("manifest", [MANIFEST, "c" * 64],
                         ids=["landing", "single-reader-share"])
def test_at_a_steady_rate_the_seal_does_not_fall_across_generations(
        manifest: str) -> None:
    """Two generations at one real rate seal at that rate, twice.

    An older, contended generation sits under both.  Read over all history,
    its slow receipts would keep pricing each new generation's seal, and a
    lower seal admits more copies, each slower: the ratchet.
    """

    def generation(consumer: str, index: int, rate: float, unix: float,
                   count: int) -> list[dict[str, object]]:
        copies = [_landed(f"{index}{copy:063d}", consumer, rate,
                          unix=unix + copy)
                  for copy in range(2)]
        return [*copies, *_fleet(consumer, rate, unix=unix, count=count)]

    contended = generation("7" * 64, 1, 59.0, 100.0, 9)
    first = generation("8" * 64, 2, 138.0, 1000.0, 3)
    second = generation("9" * 64, 3, 138.0, 2000.0, 3)
    sealed = [_price([*contended, *first], manifest)["mb_s"],
              _price([*contended, *first, *second], manifest)["mb_s"]]
    assert sealed == [138, 138]


def test_with_nothing_measured_nothing_is_priced() -> None:
    assert _price([]) == {"mb_s": None, "basis": "none", "receipts_priced": 0,
                          "window_consumer": None, "landing": None}


# ------------------------------------------------------------- the sealed rows


def _seal(tmp_path: Path, queue: pool.PoolQueue, *, offer: int | None,
          receipts: list[dict[str, object]]) -> dict[str, object]:
    """Seal one window through ``pbrun.residency_stage_rows`` at a stated offer."""

    import pbrun

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(_manifest()))
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()

    def discover(**_kwargs):
        return {TIER: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                       "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                       "mountpoint": str(tmp_path / "stage"),
                       "capacity_bytes": 8 * PHASE_BYTES}}

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)
    tier = dict(pbrun.resolve_stage_tier(queue, None))
    tier["tokens"] = ({} if offer is None
                      else {**dict(tier.get("tokens") or {}),
                            storage_tiers.FILL_KIND: offer})
    args = types.SimpleNamespace(
        priority=-10, max_attempts=1, retry_safe=False,
        residency="stage", residency_tier=None, residency_mover_mem_gb=1,
        residency_mover_readers=READERS, residency_mover_max_attempts=3)
    for record in receipts:
        record["manifest_sha256"] = (digest if record["manifest_sha256"] == MANIFEST
                                     else record["manifest_sha256"])
    return pbrun.residency_stage_rows(
        _template(digest, manifest_path.stat().st_size),
        consumer_action_key=CONSUMER, tier=tier, args=args, queue=queue,
        cas=_Cas(manifest_path), movement_receipts=receipts)


def _fills(staged: dict[str, object]) -> set[int]:
    plan = staged["plan"]
    rows = []
    for phase in plan["phases"]:                        # type: ignore[index]
        chunks = phase.get("stage_chunks")
        rows += ([chunk["mover_row"] for chunk in chunks] if chunks
                 else [phase["mover_row"]])
    return {int(row["resources"][KIND]) for row in rows}


def _queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    return queue


@pytest.mark.parametrize("offer", [428, 214])
def test_a_fresh_mover_is_sealed_at_the_measured_copy_not_the_offer(
        tmp_path: Path, offer: int) -> None:
    """R13 on R12's manifest: 116 MB/s whether the tier offers 428 or 214.

    Before #909 it was sealed at ``min(440, offer)``: the whole offer.
    """

    staged = _seal(tmp_path, _queue(tmp_path), offer=offer,
                   receipts=[dict(record) for record in WINDOWS])
    assert _fills(staged) == {116}
    source = staged["plan"]["demand_source"]            # type: ignore[index]
    assert source["fill"] == "receipts-under-offer"
    assert source["fill_measured"]["basis"] == "landing"
    assert source["tier_offer_mb_s"] == offer


def test_a_one_copy_window_is_sealed_at_the_named_share(
        tmp_path: Path) -> None:
    """The plan names what priced the seal, how many, and from which window."""

    lone = _landed("1" * 64, NEWER, 47.0, unix=500.0)
    fleet = _fleet(FLEET_NEW, 138.0, unix=400.0, count=3)
    staged = _seal(tmp_path, _queue(tmp_path), offer=428,
                   receipts=[lone, *fleet])
    assert _fills(staged) == {138}
    measured = staged["plan"]["demand_source"]["fill_measured"]  # type: ignore[index]
    assert (measured["basis"], measured["receipts_priced"],
            measured["window_consumer"]) == ("single-reader-share", 3, FLEET_NEW)
    assert measured["landing"]["copies"] == 1


def test_the_offer_still_caps_a_seal_admission_could_not_honour(
        tmp_path: Path) -> None:
    staged = _seal(tmp_path, _queue(tmp_path), offer=100,
                   receipts=[dict(record) for record in WINDOWS])
    assert _fills(staged) == {100}
    assert staged["plan"]["demand_source"]["fill"] == "tier-offer-cap"  # type: ignore[index]


def test_a_mover_with_nothing_measured_is_sealed_at_the_named_bound(
        tmp_path: Path) -> None:
    """No receipt prices a copy: the offer is the stated bound, and says so."""

    staged = _seal(tmp_path, _queue(tmp_path), offer=428, receipts=[])
    assert _fills(staged) == {428}
    source = staged["plan"]["demand_source"]            # type: ignore[index]
    assert source["fill"] == "tier-offer"
    assert source["fill_measured"]["basis"] == "none"


# ------------------------------------------------------ the reader's declaration


def test_the_submitters_declaration_reaches_the_plan(tmp_path: Path) -> None:
    import pbrun

    queue = _queue(tmp_path)
    staged = _seal(tmp_path, queue, offer=None, receipts=[])
    assert "reader" not in staged["plan"]           # undeclared: byte-identical
    args = argparse.Namespace(residency_prefetch_depth_gib=22,
                              residency_read_mb_s=21)
    assert pbrun.reader_declaration(args) == {
        "prefetch_depth_bytes": 22 * storage_tiers.GIB, "read_mb_s": 21}
    # A deferred record filed before #909 restores a Namespace without them.
    assert pbrun.reader_declaration(argparse.Namespace()) == {}


@pytest.mark.parametrize("depth,rate", [(-1, None), (None, 0), (True, None)])
def test_a_malformed_declaration_is_refused_at_submission(depth, rate) -> None:
    import pbrun

    with pytest.raises(SystemExit):
        pbrun.reader_declaration(argparse.Namespace(
            residency_prefetch_depth_gib=depth, residency_read_mb_s=rate))


def test_a_deferred_record_reads_with_or_without_the_declaration() -> None:
    """The two options are optional on a deferred record; nothing else is."""

    required = {name: None for name in action_edges._PUBLICATION_KEYS}
    declared = {**required, "residency_prefetch_depth_gib": 22,
                "residency_read_mb_s": 21}
    assert action_edges._publication_keys_read(required)
    assert action_edges._publication_keys_read(declared)
    assert not action_edges._publication_keys_read({**declared, "stray": 1})
    missing = dict(required)
    missing.pop(sorted(missing)[0])
    assert not action_edges._publication_keys_read(missing)
