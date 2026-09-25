"""The pricing log prices exactly what reading every receipt priced (#1044).

``PoolQueue.move_records`` now takes each receipt it has logged from one log
file (``movers-pricing/receipts.jsonl``) instead of opening the receipt.  The
prices a submission seals must not move because of that: the ``cpu`` and
``mem_gb`` a mover declares and the receipts that measured them, the fill it
reserves and the window that priced it, the landing its stall grace comes
from, the window's concurrency, and an egress's terms.

Each test builds one history with the cases that decide those prices -- two
tiers, two pool identities, two manifests, windows of one and several copies,
a refusal, a receipt with no pacing block and one whose pacing is not a
mapping, egress receipts, a receipt re-filed under its own key with a smaller
measurement, and files that are not receipts -- and compares every price
against the read this change replaced, kept here verbatim as
:func:`_full_read`.  It does so with no log, with a complete log, with the
log deleted, and with the log cut off and mangled.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import movement_actions, pool, storage_tiers  # noqa: E402

TIER_A = "prismabuild-stage:dl380g10"
TIER_B = "ram:dl380g10"
IDENTITY_1 = {"pool": "prismabuild-stage", "guid": "1", "state": "ONLINE"}
IDENTITY_2 = {"pool": "prismabuild-stage", "guid": "2", "state": "ONLINE"}
MANIFEST_1 = "1" * 64
MANIFEST_2 = "2" * 64
STAGE_ROOTS = ("/stage/a", "/stage/b")
GIB = 1 << 30
BOTH = (pool.POOL_MOVE_SCHEMA_V1, pool.POOL_EGRESS_SCHEMA_V1)


def _full_read(queue: pool.PoolQueue, *, schemas=(pool.POOL_MOVE_SCHEMA_V1,),
               ) -> list[dict[str, object]]:
    """``move_records`` as it was before #1044: every receipt, read whole."""

    directory = queue.root / pool.MOVERS
    out: list[dict[str, object]] = []
    try:
        paths = sorted(directory.glob("*.json"))
    except OSError:
        return out
    for path in paths:
        try:
            record = pool._read_json(path, tolerate_stale=True)
        except pool.PoolContractError:
            continue
        if not isinstance(record, dict):
            continue
        if record.get("schema") not in schemas:
            continue
        out.append(record)
    out.sort(key=lambda r: float(r.get("unix", 0.0) or 0.0))
    return out


def _key(tag: str, index: int) -> str:
    return (tag.encode().hex() + f"{index:x}").rjust(64, "0")[:64]


def _mover(index: int, *, tier: str, identity, manifest: str, consumer: str,
           unix: float, **extra) -> dict[str, object]:
    record = {
        "tier_id": tier, "consumer_action_key": consumer,
        "manifest_sha256": manifest, "complete": True,
        "bytes_staged": (1 + index % 4) * GIB, "seconds": 10.0 + index,
        "cpu_seconds": 5.0 + 3 * index, "peak_rss_bytes": (1 + index % 5) * GIB,
        "mb_per_s_file_side": 150.0 + 20 * index,
        storage_tiers.MOVER_CONCURRENCY_FIELD: 1 + index % 3,
        "disk_pacing": {storage_tiers.POOL_FILL_FIELD: 100.0 + 13 * index,
                        "pool_read_bytes": (1 + index % 4) * GIB,
                        "held_s": 1.5, "samples": list(range(20))},
        "progress_report": {"phases": ["copy"] * 30},
        "landing_report": {"landings": [{"entry": i} for i in range(10)]},
        "unix": unix,
    }
    if identity is not None:
        record["pool_identity"] = dict(identity)
    record.update(extra)
    return record


def _egress(index: int, *, stage_root: str, unix: float) -> dict[str, object]:
    return {
        "schema": pool.POOL_EGRESS_SCHEMA_V1, "action_key": _key("eg", index),
        "tier_id": TIER_A, "stage_root": stage_root, "entries_judged": 3 + index,
        "census_s": 0.5 * index, "census_validate_s": 0.25,
        "lock_held_s": 2.0 + index, "unlink_s": 0.1 * (1 + index),
        "unix": unix, **({"prune_s": 0.3} if index % 2 else {}),
    }


def _history(queue: pool.PoolQueue) -> dict[str, dict[str, object]]:
    """File the fixture history; returns the receipts filed through the API."""

    filed: dict[str, dict[str, object]] = {}
    unix = 1_000_000.0
    index = 0
    for tier in (TIER_A, TIER_B):
        for identity in (IDENTITY_1, IDENTITY_2, None):
            for manifest in (MANIFEST_1, MANIFEST_2):
                for window in range(3):
                    consumer = _key(f"c{window}", index)
                    # A window of one copy, then windows of several.
                    for _copy in range(1 if window == 0 else 3):
                        key = _key("mv", index)
                        record = _mover(index, tier=tier, identity=identity,
                                        manifest=manifest, consumer=consumer,
                                        unix=unix)
                        queue.record_move(key, record)
                        filed[key] = record
                        index += 1
                        # Two receipts share each ``unix``: the order ties.
                        unix += 0.5 if index % 2 else 0.0
    # A refusal, a receipt with no pacing, one whose pacing is not a mapping,
    # one with zero seconds, one that staged from the stage (adoption).
    oddities = [
        {"refusal": "residency_overran_reservation", "complete": False},
        {"disk_pacing": None},
        {"disk_pacing": "paced"},
        {"seconds": 0},
        {"disk_pacing": {storage_tiers.POOL_FILL_FIELD: 999.0,
                         "pool_read_bytes": 0}},
    ]
    for extra in oddities:
        key = _key("odd", index)
        record = _mover(index, tier=TIER_A, identity=IDENTITY_1,
                        manifest=MANIFEST_1, consumer=_key("codd", 0),
                        unix=unix, **extra)
        if extra.get("disk_pacing") is None and "disk_pacing" in extra:
            del record["disk_pacing"]
        queue.record_move(key, record)
        filed[key] = record
        index += 1
        unix += 1.0
    return filed


def _file_directly(queue: pool.PoolQueue, name: str, body: bytes) -> None:
    directory = queue.root / pool.MOVERS
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_bytes(body)


def _foreign_files(queue: pool.PoolQueue) -> None:
    """Receipts no writer logged, and files that are not receipts at all."""

    for index, root in enumerate(STAGE_ROOTS * 3):
        record = _egress(index, stage_root=root, unix=2_000_000.0 + index)
        _file_directly(queue, f"{record['action_key']}.json",
                       json.dumps(record).encode())
    older = _mover(99, tier=TIER_A, identity=IDENTITY_2, manifest=MANIFEST_2,
                   consumer=_key("cold", 0), unix=500.0,
                   schema=pool.POOL_MOVE_SCHEMA_V1,
                   action_key=_key("cold", 1))
    _file_directly(queue, f"{_key('cold', 1)}.json", json.dumps(older).encode())
    _file_directly(queue, f"{_key('bad', 0)}.json", b"{")
    _file_directly(queue, f"{_key('bad', 1)}.json", b"[1, 2]")
    _file_directly(queue, f"{_key('bad', 2)}.json", b"")
    _file_directly(queue, f"{_key('other', 0)}.json",
                   json.dumps({"schema": "someone.else.v1", "unix": 3.0}).encode())
    _file_directly(queue, "notes.txt", b"not a receipt")


def _prices(observed: list[dict[str, object]]) -> dict[str, object]:
    """Every price a submission reads off the receipts, as pbrun takes them."""

    receipts = [record for record in observed
                if record.get("schema") != pool.POOL_EGRESS_SCHEMA_V1]
    out: dict[str, object] = {}
    for tier in (TIER_A, TIER_B):
        for identity in (None, IDENTITY_1, IDENTITY_2):
            label = f"{tier}|{json.dumps(identity, sort_keys=True)}"
            out[f"demand|{label}"] = storage_tiers.mover_demand_from_receipts(
                receipts, tier_id=tier, readers=4, fallback_mem_gb=8,
                pool_identity=identity)
            for manifest in (None, MANIFEST_1, MANIFEST_2):
                price = storage_tiers.mover_fill_price(
                    receipts, tier_id=tier, pool_identity=identity,
                    manifest_sha256=manifest)
                out[f"fill|{label}|{manifest}"] = price
                landing = price.get("landing")
                counts = None
                if isinstance(landing, dict):
                    counts = [record.get(storage_tiers.MOVER_CONCURRENCY_FIELD)
                              for record in receipts
                              if str(record.get("manifest_sha256") or "") == manifest
                              and str(record.get("consumer_action_key") or "")
                              == str(landing.get("window_consumer") or "")]
                out[f"concurrency|{label}|{manifest}"] = counts
    for root in STAGE_ROOTS:
        out[f"egress|{root}"] = movement_actions.egress_price(
            observed, stage_root=root)
    return out


def _check(queue: pool.PoolQueue) -> None:
    for schemas in ((pool.POOL_MOVE_SCHEMA_V1,), BOTH):
        expected = _full_read(queue, schemas=schemas)
        got = queue.move_records(schemas=schemas)
        assert got == [pool.move_pricing_projection(r) for r in expected]
        assert _prices(got) == _prices(expected)
    # ``produced_output``'s exporter prices off the default schemas.
    assert (storage_tiers.mover_fill_demand_from_receipts(
        queue.move_records(), tier_id=TIER_A, pool_identity=IDENTITY_1,
        manifest_sha256=MANIFEST_1)
        == storage_tiers.mover_fill_demand_from_receipts(
            _full_read(queue), tier_id=TIER_A, pool_identity=IDENTITY_1,
            manifest_sha256=MANIFEST_1))


@pytest.fixture
def queue(tmp_path: Path) -> pool.PoolQueue:
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    filed = _history(queue)
    _foreign_files(queue)
    # A receipt re-filed under its own key with smaller measurements: the
    # all-history maxima must fall with it, as they did reading the file.
    biggest = max(filed, key=lambda key: filed[key]["peak_rss_bytes"])
    queue.record_move(biggest, {**filed[biggest], "peak_rss_bytes": 1,
                                "cpu_seconds": 0.5, "unix": 3_000_000.0})
    return queue


def test_the_projection_prices_every_price_as_the_whole_receipt(queue) -> None:
    for schemas in ((pool.POOL_MOVE_SCHEMA_V1,), BOTH):
        whole = _full_read(queue, schemas=schemas)
        projected = [pool.move_pricing_projection(r) for r in whole]
        assert _prices(projected) == _prices(whole)
        # The fixture exercises the prices: they are not all "unmeasured".
        prices = _prices(whole)
        assert any(value["basis"] == "landing" for name, value in prices.items()
                   if name.startswith("fill|"))
        assert any(value["basis"] == "single-reader-share"
                   for name, value in prices.items() if name.startswith("fill|"))
    assert movement_actions.egress_price(
        _full_read(queue, schemas=BOTH), stage_root=STAGE_ROOTS[0])["basis"] == "egress"


def test_no_log_a_complete_log_and_a_deleted_log_price_alike(queue) -> None:
    log = queue.move_pricing_log_path()
    # The writers logged what they filed; the foreign files are not logged.
    _check(queue)
    # That read logged the rest: now the log covers every receipt.
    assert log.exists()
    _check(queue)
    log.unlink()
    _check(queue)
    assert log.exists()
    _check(queue)


def test_a_cut_off_or_mangled_log_prices_alike(queue) -> None:
    log = queue.move_pricing_log_path()
    queue.move_records(schemas=BOTH)
    whole = log.read_bytes()
    lines = whole.split(b"\n")
    assert len(lines) > 20
    # Cut off mid-line, including the re-filed receipt's newer line.
    log.write_bytes(whole[: len(whole) // 2 + 7])
    _check(queue)
    # Garbage, a torn body for a logged name, a line with a foreign tag, and
    # a body that is not an object.
    name = pool._move_pricing_line_name(lines[3])
    assert name is not None
    log.write_bytes(
        b"\x00\xffgarbage\n"
        + whole
        + pool.MOVE_PRICING_LOG_TAG + b"\t" + name.encode() + b"\t{\"sch\n"
        + b"pricing.v0\t" + name.encode() + b"\t{}\n"
        + pool.MOVE_PRICING_LOG_TAG + b"\t" + name.encode() + b"\t[1]\n")
    _check(queue)
    # A log that ends mid-line: the next line a writer appends still parses.
    log.write_bytes(whole + pool.MOVE_PRICING_LOG_TAG + b"\tpartial")
    late = _mover(7, tier=TIER_A, identity=IDENTITY_1, manifest=MANIFEST_1,
                  consumer=_key("late", 0), unix=4_000_000.0)
    queue.record_move(_key("late", 1), late)
    _check(queue)
    assert queue.move_records()[-1]["action_key"] == _key("late", 1)


def test_a_logging_read_never_lands_an_older_line_after_a_newer(queue) -> None:
    """A reader logs a receipt it read; a writer re-files it before the append."""

    log = queue.move_pricing_log_path()
    log.unlink()
    _logged, since = queue._read_move_pricing_log()
    key = _key("cold", 1)
    path = queue.root / pool.MOVERS / f"{key}.json"
    stale = pool._read_json(path)
    newer = {**stale, "peak_rss_bytes": 77 * GIB, "unix": 5_000_000.0}
    queue.record_move(key, newer)
    queue._append_move_pricing(
        [(path.name, pool._move_pricing_line(path.name, stale))],
        since=since, blocking=True)
    logged, _size = queue._read_move_pricing_log()
    assert logged[path.name]["peak_rss_bytes"] == 77 * GIB
    _check(queue)


def test_the_log_is_not_a_file_in_the_receipt_directory(queue) -> None:
    queue.move_records()
    log = queue.move_pricing_log_path()
    assert log.exists()
    assert log.parent != queue.root / pool.MOVERS
    assert not list((queue.root / pool.MOVERS).glob("*.jsonl"))
    assert not list((queue.root / pool.MOVERS).glob("*.lock"))
