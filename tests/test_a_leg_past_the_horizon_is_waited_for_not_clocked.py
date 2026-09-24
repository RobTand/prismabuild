"""A leg past the refill horizon is waited for, not clocked (#1018).

PrismaQuant's staged-range reader (PQ ``residency_shard_reader.landing_verdict``)
follows PB's landing record while the record lists the range it waits for:
it waits while the range's mover is queued, copying or ``unpublished``, and
refuses only on evidence.  A range the record does not list is ``absent``,
and the reader then waits on a clock (``STAGED_RANGE_WAIT_S``, 300 s; R13
overrides it to 840 s) and refuses when the clock runs out.

Before this fix the record listed a leg only inside the consumer's refill
horizon (``tier_loop.publish_landing_expectations``: ``if not inside and state
is None: continue``).  A leg past the horizon had no row, so a reader that
asked for it waited on the clock and refused a range that was coming: the
window publishes it on the cycle the consumer's progress brings it inside.

The fixture is R12's plan (``tests/fixtures/r12_stage_20260922.json``) as
``test_claimed_consumers_drain_a_tier_in_admission_order`` builds it, one
consumer on the whole 565 GiB stage: reading ``chain-043`` at 20.7 MB/s,
horizon through ``chain-033``, ``chain-032`` the advance.  One 23.4 GB phase
takes R12 about 1130 s to read, so ``chain-032`` publishes about 1130 s after
a reader first asks for it: 3.8 times the reader's 300 s clock.

The reader here restates PQ's, because the tests may not import PQ: its
record checks (``residency_map._read_landing``) and its verdict
(``residency_shard_reader.landing_verdict``), pinned at PQ ``1e1db0e0d6f8``
the way ``READER_LANDING_STATES`` is pinned in the drain test.

Everything runs on ``tmp_path`` queues and stage roots (#628).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import (  # noqa: E402
    pool, progress as pb_progress, residency_map, residency_plan,
    storage_tiers, window_credit)
import tier_loop  # noqa: E402
from test_a_consumer_stages_only_to_its_refill_horizon import (  # noqa: E402
    MOVER_SECONDS, READER as SMALL_READER,
    READER_MANIFEST as SMALL_READER_MANIFEST, _cycle, _fixture_queue, _land,
    _mover as _small_mover, _plan as _small_plan, _publish_consumer, _reader)
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    TIER)
from test_claimed_consumers_drain_a_tier_in_admission_order import (  # noqa: E402
    EIGHT_HOLD, PHASES, _blocked, _key, _manifest, _mover)
from test_r12_and_the_capture_replay_under_the_refill_horizon import (  # noqa: E402
    DATA, SAMPLE_UNIX)

# --------------------------------------------------------------- the reader

#: PQ ``prismaquant/residency_map.py`` at ``1e1db0e0d6f8``: the landing
#: schema, the states ``_read_landing`` accepts (a record listing any other
#: is dropped whole), and the byte bound past which it is dropped whole.
READER_LANDING_SCHEMA = "prismaquant.prismabuild.residency_landing.v1"
READER_LANDING_STATES = ("ready", "claimed", "unpublished", "evicted",
                         "done-not-resident", "terminal-no-receipt")
READER_MAX_BYTES = 256 * 1024 * 1024
#: PQ ``residency_shard_reader.STAGED_RANGE_WAIT_S`` at ``1e1db0e0d6f8``:
#: the clock an ``absent`` verdict waits on.
READER_CLOCK_S = 300.0
#: What R12 takes to read one phase at its measured rate, which is when the
#: advance it asks for first publishes.
PHASE_READ_S = 1130.0
#: The slowest complete R12 copy on 2026-09-22 (134 MB/s): what a landed
#: range's receipt says it took.
LANDING_BYTES_PER_S = 134e6


def reader_landing(queue: pool.PoolQueue, consumer: str,
                   manifest: str) -> dict[str, object] | None:
    """PQ ``_read_landing`` plus ``landing_record``'s identity check."""

    path = residency_map.landing_path(queue.residency_fragment_root(), consumer)
    try:
        raw = path.read_bytes()
    except OSError:
        return None
    if len(raw) > READER_MAX_BYTES:
        return None
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeError, ValueError):
        return None
    if type(payload) is not dict or payload.get("schema") != READER_LANDING_SCHEMA:
        return None
    liveness = payload.get("tier_loop_liveness_s")
    if (type(liveness) not in (int, float) or not liveness > 0
            or liveness == float("inf")):
        return None
    if not isinstance(payload.get("tier_id"), str) or not isinstance(
            payload.get("manifest_sha256"), str):
        return None
    ranges = payload.get("ranges")
    if not isinstance(ranges, list):
        return None
    for row in ranges:
        if (type(row) is not dict or row.get("state") not in READER_LANDING_STATES
                or not isinstance(row.get("mover_action_key"), str)
                or type(row.get("range_start_bytes")) is not int
                or type(row.get("range_end_bytes")) is not int):
            return None
    if payload["manifest_sha256"] != manifest:
        return None
    return payload


def reader_tier_age(queue: pool.PoolQueue, tier_id: str) -> float | None:
    """PQ ``ResidencyResolver.tier_record_age``."""

    try:
        payload = json.loads(queue.tier_record_path(tier_id).read_text())
    except (OSError, ValueError):
        return None
    if (type(payload) is not dict
            or payload.get("schema") != storage_tiers.TIER_RECORD_SCHEMA_V1):
        return None
    announced = payload.get("announced_unix")
    if type(announced) not in (int, float):
        return None
    return max(0.0, time.time() - float(announced))


def reader_verdict(queue: pool.PoolQueue, consumer: str, manifest: str,
                   start: int, end: int) -> tuple[str, str, tuple[str, ...]]:
    """PQ ``landing_verdict`` for one span, in read-order bytes."""

    record = reader_landing(queue, consumer, manifest)
    if record is None:
        return "absent", "PrismaBuild published no landing record", ()
    found = [row for row in record["ranges"]  # type: ignore[union-attr]
             if row["range_start_bytes"] < end and start < row["range_end_bytes"]]
    if not found:
        return ("absent", f"the landing record lists no pending range for "
                f"[{start}, {end})", ())
    if all(row["state"] == "terminal-no-receipt" for row in found):
        return "refuse", f"[{start}, {end}) is terminal-no-receipt", ()
    covering = [row for row in found if row["state"] != "terminal-no-receipt"]
    liveness = float(record["tier_loop_liveness_s"])  # type: ignore[arg-type]
    age = reader_tier_age(queue, str(record["tier_id"]))
    if age is None or age > liveness:
        return "refuse", "the tier loop is silent", ()
    return ("wait", f"mover {covering[0]['mover_action_key'][:12]} is "
            f"{covering[0]['state']}",
            tuple(sorted({str(row["mover_action_key"]) for row in covering})))


def staged(queue: pool.PoolQueue, consumer: str, declared: str,
           size: int) -> Path | None:
    """The staged copy the composed map names for ``declared``, or ``None``.

    PQ's ``staged_range_outcome`` fences: an entry for the path at offset 0,
    a regular file of exactly the entry's byte count.
    """

    try:
        composed = json.loads(queue.residency_map_path(consumer).read_text())
    except (OSError, ValueError):
        return None
    entry = (composed.get("entries") or {}).get(
        residency_map.residency_map_key(declared, 0))
    if not isinstance(entry, dict) or int(entry.get("bytes", -1)) != size:
        return None
    path = Path(str(entry["stage_path"]))
    return path if path.is_file() and path.stat().st_size == size else None


def poll(queue: pool.PoolQueue, consumer: str, manifest: str, *, name: str,
         start: int, end: int, elapsed_s: float) -> tuple[str, str]:
    """One poll of the reader's wait, ``elapsed_s`` into it.

    ``hit`` when the map covers the span; ``wait`` while the record says
    the range is coming; ``refuse`` on evidence, or once an ``absent``
    verdict has waited out the clock; ``absent`` while the clock runs.
    """

    declared = f"/pool/{manifest[:8]}/{name}/part-0.bin"
    if staged(queue, consumer, declared, end - start) is not None:
        return "hit", declared
    kind, detail, _movers = reader_verdict(queue, consumer, manifest, start, end)
    if kind == "absent" and elapsed_s >= READER_CLOCK_S:
        return "refuse", (f"no landing record covers the wait ({detail}); the "
                          f"bounded wait of {READER_CLOCK_S:g} s ran out")
    return kind, detail


# ------------------------------------------------------------- the fixture

def _marker(mover: str) -> bytes:
    """What the mover writes at the head of its range: 64 KiB, per mover."""

    seed = hashlib.sha256(mover.encode()).digest()
    return (seed * (65536 // len(seed)))[:65536]


def _land_queued(queue: pool.PoolQueue, stage: Path, *, consumer: str,
                 manifest: str, plan: dict[str, object],
                 bytes_per_s: float) -> dict[str, Path]:
    """Every queued copy of ``consumer``'s plan lands, as a cycle's movers would.

    Each mover's row leaves ``ready/`` for ``done/``, a fence the window put
    on the leg is handed back, the range is filed the way ``stage_move``
    files it (``_land``), and the mover's head bytes are written into the
    copy.  The copies the reader has not reached keep landing while it
    waits, as they do in a run.
    """

    ledger = queue.tier_ledger(TIER)
    landed: dict[str, Path] = {}
    for phase in plan["phases"]:                               # type: ignore[union-attr]
        name = str(phase["name"])
        mover = str(phase["mover_row"]["action_key"])
        ready = queue.item_path(pool.READY, mover)
        if not ready.exists():
            continue
        item = json.loads(ready.read_text())
        ready.unlink()
        for holder in (mover, window_credit.grant_key(
                consumer, TIER, "mover_row", name, None)):
            if ledger.holder_tokens(holder):
                ledger.release(holder)
        done = queue.item_path(pool.DONE, mover)
        done.parent.mkdir(parents=True, exist_ok=True)
        done.write_text(json.dumps({**item, "status": "done"}))
        start, end = int(phase["start_bytes"]), int(phase["end_bytes"])
        [path] = _land(queue, stage, consumer=consumer, manifest=manifest,
                       mover=mover, name=name, start=start, end=end,
                       seconds=(end - start) / bytes_per_s)
        with open(path, "r+b") as stream:
            stream.write(_marker(mover))
        landed[name] = path
    return landed


def _report(queue: pool.PoolQueue, consumer: str, *, phase: str,
            reported_unix: float, units: int = 2) -> None:
    """The claimed consumer's worker files a later accepted phase.

    Into the lease the claim wrote, as the worker's heartbeat files it.
    ``write_lease`` would refuse it here: the fixture claims as
    ``dl380g10`` from whichever box runs the test, and a second write
    compares the two (``AmbiguousClaimHolder``).
    """

    lease_path = queue.lease_path(consumer)
    lease = json.loads(lease_path.read_text())
    lease["progress_observation"] = {
        "source": "action-progress",
        "last_accepted": {"phase": phase, "units_completed": units,
                          "reported_unix": reported_unix}}
    lease_path.write_text(json.dumps(lease))


def _r12(tmp_path: Path):
    """One R12 on the whole stage: reading ``chain-043``, horizon to ``chain-033``."""

    capacity = int(DATA["tier"]["capacity_gib"])
    queue, stage = _fixture_queue(tmp_path, capacity)
    plan = _blocked(queue, stage, 0, shift=time.time() - SAMPLE_UNIX,
                    holding=EIGHT_HOLD)
    return queue, stage, plan, capacity


# ------------------------------------------------------------------ the cases

ADVANCE = "chain-032"


def test_the_advance_past_the_horizon_is_waited_for_then_read(
        tmp_path: Path) -> None:
    """The acceptance case: published at 3.8 clocks, waited for, then read.

    On main ``chain-032`` has no row, the reader's verdict is ``absent``,
    and its clock refuses the wait at 300 s: the range published 830 s later
    is never read.  With every leg listed, the verdict is ``wait`` for the
    whole 1130 s, and the reader reads the bytes the mover landed.
    """

    queue, stage, plan, capacity = _r12(tmp_path)
    key, manifest = _key(0), _manifest(0)
    mover = _mover(0, ADVANCE)
    start = int(PHASES[ADVANCE]["start_bytes"])
    end = int(PHASES[ADVANCE]["end_bytes"])

    outcomes: list[tuple[float, str, str]] = []
    for elapsed in (0.0, READER_CLOCK_S, 2 * READER_CLOCK_S, 3 * READER_CLOCK_S):
        _cycle(queue, stage, gib=capacity)
        assert not queue.item_path(pool.READY, mover).exists(), (
            "the advance publishes only once progress brings it inside")
        outcomes.append((elapsed, *poll(queue, key, manifest, name=ADVANCE,
                                        start=start, end=end,
                                        elapsed_s=elapsed)))
        _land_queued(queue, stage, consumer=key, manifest=manifest, plan=plan,
                     bytes_per_s=LANDING_BYTES_PER_S)
    refused = [entry for entry in outcomes if entry[1] == "refuse"]
    assert not refused, (
        f"the reader refused {ADVANCE} while it was coming: {refused}")
    assert [kind for _elapsed, kind, _detail in outcomes] == ["wait"] * 4, outcomes

    # R12 reads chain-043 and reports chain-042 one phase later: the horizon
    # moves one leg and the advance is inside it.  (The polls above stand
    # for the 1130 s; the report is filed now, so its rate is R12's own.)
    _report(queue, key, phase="chain-042", reported_unix=time.time())
    _cycle(queue, stage, gib=capacity)
    assert queue.item_path(pool.READY, mover).exists()
    kind, detail = poll(queue, key, manifest, name=ADVANCE, start=start,
                        end=end, elapsed_s=3 * READER_CLOCK_S + PHASE_READ_S)
    assert (kind, detail.split(" is ")[-1]) == ("wait", "ready"), (kind, detail)

    landed = _land_queued(queue, stage, consumer=key, manifest=manifest,
                          plan=plan, bytes_per_s=LANDING_BYTES_PER_S)
    _cycle(queue, stage, gib=capacity)
    kind, declared = poll(queue, key, manifest, name=ADVANCE, start=start,
                          end=end, elapsed_s=3 * READER_CLOCK_S + PHASE_READ_S + 180)
    assert kind == "hit", (kind, declared)
    served = staged(queue, key, declared, end - start)
    assert served == landed[ADVANCE]
    with open(served, "rb") as stream:
        assert stream.read(65536) == _marker(mover)


def test_every_leg_ahead_of_the_consumer_has_a_row(tmp_path: Path) -> None:
    """The record answers for every leg the consumer has still to read.

    Resident legs are the map's; every other leg of the phases from the one
    being read onward has a row, in a state the reader knows.  On main the
    legs past the horizon, ``chain-032`` to ``chain-000``, have none.
    """

    queue, stage, plan, capacity = _r12(tmp_path)
    key = _key(0)
    _cycle(queue, stage, gib=capacity)
    record = residency_map.read_landing(residency_map.landing_path(
        queue.residency_fragment_root(), key))
    rows = {str(row["phase"]): row for row in record["ranges"]}  # type: ignore[union-attr]
    names = [str(phase["name"]) for phase in plan["phases"]]  # type: ignore[union-attr]
    ahead = names[names.index("chain-043"):]
    missing = [name for name in ahead
               if name not in rows and name not in EIGHT_HOLD]
    assert missing == [], f"legs with no row: {missing}"
    for name in ahead:
        if name in rows:
            assert rows[name]["state"] in READER_LANDING_STATES, rows[name]


# ------------------------------------------------- no accepted progress yet

FIRST = "9" * 64
FIRST_MANIFEST = "8" * 64


def test_a_leg_waiting_on_first_progress_is_waited_for(tmp_path: Path) -> None:
    """A claimed consumer with no accepted progress has no horizon.

    Its window publishes the phase it reads and one step past it
    (``residency_plan.window``: ``no_accepted_progress``), and publishes the
    rest once the consumer's first progress record defines the horizon.  On
    main those legs have no row, so the reader's clock refuses them.
    """

    queue, stage = _fixture_queue(tmp_path, 40)
    plan = _small_plan(queue, FIRST, label="first", manifest=FIRST_MANIFEST)
    _publish_consumer(queue, FIRST, plan, manifest=FIRST_MANIFEST)
    source = queue.item_path(pool.READY, FIRST)
    item = json.loads(source.read_text())
    source.unlink()
    now = time.time()
    item.update({"action_key": FIRST, "claimed_unix": now - 1000.0,
                 "claimed_by": "horizon-fixture", "claimed_host": "dl380g10"})
    queue.item_path(pool.CLAIMED, FIRST).write_text(json.dumps(item))
    queue.write_lease(FIRST, owner="horizon-fixture", claim_snapshot=item,
                      progress_observation={"source": "action-progress"})
    phase = plan["phases"][3]                                    # type: ignore[index]
    mover = _small_mover("first", 3)
    start, end = int(phase["start_bytes"]), int(phase["end_bytes"])

    rate = (end - start) / MOVER_SECONDS
    outcomes = []
    for elapsed in (0.0, READER_CLOCK_S, 2 * READER_CLOCK_S, 3 * READER_CLOCK_S):
        _cycle(queue, stage, gib=40)
        assert not queue.item_path(pool.READY, mover).exists()
        outcomes.append((elapsed, *poll(queue, FIRST, FIRST_MANIFEST,
                                        name="phase-3", start=start, end=end,
                                        elapsed_s=elapsed)))
        _land_queued(queue, stage, consumer=FIRST, manifest=FIRST_MANIFEST,
                     plan=plan, bytes_per_s=rate)
    assert [kind for _elapsed, kind, _detail in outcomes] == ["wait"] * 4, outcomes

    # The first accepted progress: phase-1.
    _report(queue, FIRST, phase="phase-1", reported_unix=time.time(), units=1)
    _cycle(queue, stage, gib=40)
    assert queue.item_path(pool.READY, mover).exists()
    landed = _land_queued(queue, stage, consumer=FIRST, manifest=FIRST_MANIFEST,
                          plan=plan, bytes_per_s=rate)
    _cycle(queue, stage, gib=40)
    kind, declared = poll(queue, FIRST, FIRST_MANIFEST, name="phase-3",
                          start=start, end=end, elapsed_s=4 * READER_CLOCK_S)
    assert kind == "hit", (kind, declared)
    served = staged(queue, FIRST, declared, end - start)
    assert served == landed["phase-3"]
    with open(served, "rb") as stream:
        assert stream.read(65536) == _marker(mover)


# --------------------------------------- a reader past its declared reach


def test_a_reader_blocked_past_its_horizon_pulls_the_horizon_to_it(
        tmp_path: Path) -> None:
    """A consumer that reads past what it declared is served, not deadlocked.

    The horizon is priced from the read-ahead the consumer declares.  A
    consumer that blocks on a leg past it, and whose progress needs that leg,
    would wait for ever now that the record lists the leg as coming: the leg
    publishes only when that progress arrives.  Its staged-wait record
    (#989) names the leg's mover, which is the consumer's own measurement of
    how far it reads.  The tier loop reads it and takes the horizon through
    that leg, so the next cycle publishes it, room permitting.

    The small fixture (``_reader``): reading ``phase-0``, which is landed,
    horizon ``phase-0`` to ``phase-2``, ``phase-3`` the advance.  The reader
    blocks on ``phase-5``.  On main nothing publishes ``phase-5`` until
    progress arrives.
    """

    queue, stage = _fixture_queue(tmp_path, 40)
    _reader(queue, stage, landed=(0,))
    wanted = {ordinal: _small_mover("reader", ordinal) for ordinal in range(8)}
    plan = residency_plan.read(queue, SMALL_READER)
    phase = plan["phases"][5]                                    # type: ignore[index]
    start, end = int(phase["start_bytes"]), int(phase["end_bytes"])

    _cycle(queue, stage, gib=40)
    published = {ordinal for ordinal, mover in wanted.items()
                 if queue.item_path(pool.READY, mover).exists()}
    assert published == {1, 2}, published

    # The reader asks for phase-5 and declares the wait the way PQ does.
    kind, _detail, _movers = reader_verdict(queue, SMALL_READER,
                                            SMALL_READER_MANIFEST, start, end)
    staged_wait = Path(pb_progress.staged_wait_path(
        str(queue.action_progress_path(SMALL_READER))))
    staged_wait.parent.mkdir(parents=True, exist_ok=True)
    staged_wait.write_text(json.dumps({
        "schema": pb_progress.STAGED_WAIT_SCHEMA_V1, "token": "t" * 32,
        "since_unix": time.time(), "movers": [wanted[5]]}))

    _cycle(queue, stage, gib=40)
    published = {ordinal for ordinal, mover in wanted.items()
                 if queue.item_path(pool.READY, mover).exists()}
    assert {3, 4, 5} <= published, (
        f"the reader blocked on phase-5 (verdict {kind}); published {published}")
    assert not {6, 7} & published, published
    kind, detail, _movers = reader_verdict(queue, SMALL_READER,
                                           SMALL_READER_MANIFEST, start, end)
    assert (kind, detail.split(" is ")[-1]) == ("wait", "ready"), (kind, detail)
    record = residency_map.read_landing(residency_map.landing_path(
        queue.residency_fragment_root(), SMALL_READER))
    horizon = record["horizon"]
    assert horizon["declared_wait_end_bytes"] == end, horizon  # type: ignore[index]
    assert horizon["end_bytes"] == int(plan["phases"][6]["start_bytes"]), horizon  # type: ignore[index]


# ------------------------------------------- the wait counts as declared (#989)

TOKEN = "t" * 32


def test_a_wait_on_a_deferred_leg_counts_while_the_tier_loop_lives(
        tmp_path: Path) -> None:
    """Constraint 4: the worker's watch and the reader agree, and both end.

    R12 declares its wait on the advance, which the record lists as
    deferred by the horizon.  While the tier loop announces its tier the
    watch counts the wait as declared, not quiet (PB #989's ``unpublished``
    rule), and the reader waits.  Once the loop stops announcing, both end
    it: the watch no longer exempts the wait and the reader refuses,
    naming the silent loop.
    """

    queue, stage, _plan, capacity = _r12(tmp_path)
    key, manifest = _key(0), _manifest(0)
    mover = _mover(0, ADVANCE)
    start = int(PHASES[ADVANCE]["start_bytes"])
    end = int(PHASES[ADVANCE]["end_bytes"])
    _cycle(queue, stage, gib=capacity)
    progress_path = Path(queue.action_progress_path(key))
    Path(pb_progress.staged_wait_path(str(progress_path))).write_text(json.dumps({
        "schema": pb_progress.STAGED_WAIT_SCHEMA_V1, "token": TOKEN,
        "since_unix": time.time(), "movers": [mover]}))

    verdict = queue.staged_wait_verdict(key, progress_path, token=TOKEN)
    assert verdict is not None
    assert [entry["state"] for entry in verdict["movers"]] == ["unpublished"], verdict
    assert verdict["tier_over_committed_gib"] <= 0, verdict
    assert verdict["exempt"] is True, verdict
    kind, _detail, movers = reader_verdict(queue, key, manifest, start, end)
    assert (kind, movers) == ("wait", (mover,))

    tier_path = queue.tier_record_path(TIER)
    announced = json.loads(tier_path.read_text())
    announced["announced_unix"] = time.time() - 2 * pool.OFFER_TIMEOUT_S
    tier_path.write_text(json.dumps(announced))

    verdict = queue.staged_wait_verdict(key, progress_path, token=TOKEN)
    assert verdict is not None and verdict["exempt"] is False, verdict
    kind, detail, _movers = reader_verdict(queue, key, manifest, start, end)
    assert (kind, detail) == ("refuse", "the tier loop is silent")


# ------------------------------------------------------------ the record

def test_the_record_says_where_the_horizon_ends_and_holds_it(
        tmp_path: Path) -> None:
    """The deferred rows and the ``horizon`` block, and what the check refuses.

    A deferred row is ``unpublished`` (or ``evicted``) with no expected
    landing; a row deferred by the horizon starts at or past the end the
    record states.  A record written before #1018, with neither field,
    still reads.
    """

    queue, stage, _plan, capacity = _r12(tmp_path)
    key = _key(0)
    _cycle(queue, stage, gib=capacity)
    path = residency_map.landing_path(queue.residency_fragment_root(), key)
    raw = json.loads(path.read_text())
    record = residency_map.validate_landing(raw)
    horizon = record["horizon"]
    start = int(PHASES[ADVANCE]["start_bytes"])
    assert horizon["end_bytes"] == start, horizon                 # type: ignore[index]
    assert horizon["accepted_phase"] == "chain-043", horizon      # type: ignore[index]
    assert horizon["advance_mover_action_key"] == _mover(0, ADVANCE)  # type: ignore[index]
    assert horizon["consumption_bytes_per_s"] > 0, horizon        # type: ignore[index]
    rows = {row["phase"]: row for row in record["ranges"]}        # type: ignore[union-attr]
    advance = rows[ADVANCE]
    assert (advance["state"], advance["deferred_by"]) == ("unpublished", "horizon")
    assert advance["expected_landing_unix"] is None
    assert "refill horizon" in str(advance["waiting_for"])
    assert rows["chain-000"]["deferred_by"] == "horizon"

    index = next(n for n, row in enumerate(raw["ranges"]) if row["phase"] == ADVANCE)
    for mutate in (
            lambda doc: doc["ranges"][index].update(deferred_by="later"),
            lambda doc: doc["ranges"][index].update(
                state="ready", expected_landing_unix=1.0, queue_position=0,
                bytes_ahead=0),
            lambda doc: doc["ranges"][index].update(expected_landing_unix=1.0),
            lambda doc: doc.update(horizon=None),
            lambda doc: doc["horizon"].update(end_bytes=start + 1),
            lambda doc: doc["horizon"].update(extra=1)):
        doc = json.loads(json.dumps(raw))
        mutate(doc)
        try:
            residency_map.validate_landing(doc)
        except residency_map.ResidencyMapError:
            continue
        raise AssertionError(f"the check took {doc['ranges'][index]}")

    before = json.loads(json.dumps(raw))
    before.pop("horizon")
    before["ranges"] = [{name: value for name, value in row.items()
                         if name != "deferred_by"} for row in before["ranges"]]
    assert "horizon" not in residency_map.validate_landing(before)
