"""A freshly started tier loop keeps its tier record alive through its first read (#1153).

Live shape (dl380g10, 2026-09-25): the publish of ``9ea9dc9ac4cd`` re-executed
the tier loop at 11:29:03Z (``tier-runtime-moved``).  The new process starts
with an empty ``ReceiptCache`` (#992), so its first ``receipts`` step read all
12,295 prewarm and movement receipts cold, one at a time, at about 4 receipts
a second on the loaded HDD pool.  #1148's checkpoints re-announce the record
the *previous* cycle minted, and a fresh process has none in memory, so the
stage record aged past the 120 s bound.  Stage B row 020 died in its staged
wait at 11:30:55Z: ``the tier loop last announced prismabuild-stage:dl380g10
120 s ago``.

Two fixes, and each alone keeps the consumer waiting:

1. On start, the loop adopts the record it finds on disk for its own tier,
   when a reader would still call that record alive, and re-announces it at
   once.  The #1148 checkpoints then refresh it through the first read.
2. Receipts are read by a bounded pool of readers, so a cold read costs
   (receipts / readers) x latency, not receipts x latency.

The terms are scaled so the test runs in seconds on a real clock:
``pool.OFFER_TIMEOUT_S`` (``L``) is 8 s and ``pool.HEARTBEAT_S`` (``P``) is
2 s.  The reader's bound travels in the landing record
(``tier_loop_liveness_s``), so PQ's reader applies the scaled bound too.
Every read of a cold receipt sleeps ``READ_S``; serially the read takes
``COLD_RECEIPTS * READ_S`` = 12 s, past ``L``.

The consumer is R12 on the whole stage, waiting on its advance leg
(``chain-032``), as in ``test_a_wait_on_a_deferred_leg_counts_while_the_tier
_loop_lives``.  It polls PQ's ``landing_verdict`` (restated in
``test_a_leg_past_the_horizon_is_waited_for_not_clocked``) and PB's
``PoolQueue._tier_loop_alive`` on its own thread while the fresh loop runs
its first cycle.

Every fixture is a temp queue and a temp stage root; nothing touches a real
``/stage``, ``/ram`` or the live queue.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import threading
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(ROOT / "tests"))

from prismabuild import pool  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402

import test_a_leg_past_the_horizon_is_waited_for_not_clocked as leg  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    _tier_record)

TIER = leg.TIER
HOST = "dl380g10"
#: The scaled liveness terms: ``L``, ``P``, and the loop's ``I``.
BOUND_S = 8.0
POLL_S = 2.0
INTERVAL_S = 5.0
#: Cold receipts filed after the replaced loop's last cycle, and what each
#: read costs.  Serially: 12 s, past ``L``.
COLD_RECEIPTS = 40
READ_S = 0.3
PREFIX = "cold-receipt-"
#: How often the consumer polls, as PQ's reader polls every second.
READER_POLL_S = 0.05


@pytest.fixture(autouse=True)
def _fresh_process_state():
    stage_release.reset_skip_checkpoints()
    stage_release.reset_holder_reports()
    yield
    stage_release.reset_skip_checkpoints()
    stage_release.reset_holder_reports()


@pytest.fixture
def scaled(monkeypatch):
    monkeypatch.setattr(pool, "OFFER_TIMEOUT_S", BOUND_S)
    monkeypatch.setattr(pool, "HEARTBEAT_S", POLL_S)


def _fresh_loop(queue: pool.PoolQueue):
    """What a freshly executed loop holds before its first cycle.

    ``tier_loop._start`` is the serving role's own start path.  Before #1153
    ``_serve`` built an empty ``ReceiptCache`` and a new ``Liveness`` and
    nothing else, which is what this falls back to on a tree without it.
    """

    start = getattr(tier_loop, "_start", None)
    if start is None:
        return tier_loop.ReceiptCache(), tier_loop.Liveness(interval_s=INTERVAL_S)
    return start(queue, host=HOST, interval_s=INTERVAL_S)


class _Consumer(threading.Thread):
    """R12 in its staged wait on the advance leg, polling as PQ's reader does."""

    def __init__(self, queue: pool.PoolQueue) -> None:
        super().__init__(daemon=True)
        self.queue = queue
        self.key, self.manifest = leg._key(0), leg._manifest(0)
        self.start_bytes = int(leg.PHASES[leg.ADVANCE]["start_bytes"])
        self.end_bytes = int(leg.PHASES[leg.ADVANCE]["end_bytes"])
        #: ``(age, alive, kind, detail)`` per poll.
        self.polls: list[tuple[float | None, bool, str, str]] = []
        self.error: BaseException | None = None
        self.halt = threading.Event()

    def poll(self) -> tuple[float | None, bool, str, str]:
        kind, detail, _movers = leg.reader_verdict(
            self.queue, self.key, self.manifest, self.start_bytes,
            self.end_bytes)
        alive, age = self.queue._tier_loop_alive(TIER, now=time.time())
        seen = (age, alive, kind, detail)
        self.polls.append(seen)
        return seen

    def run(self) -> None:
        try:
            while not self.halt.is_set():
                self.poll()
                self.halt.wait(READER_POLL_S)
        except BaseException as exc:  # noqa: BLE001 -- reported by the test
            self.error = exc


class _Slow:
    """``pool._read_json``, with every cold receipt's read costing ``READ_S``."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *,
                 read_s: float = READ_S) -> None:
        self.lock = threading.Lock()
        self.reads = 0
        self.read_s = read_s
        #: When set, the read of this name waits on ``release`` first.
        self.hang_name: str | None = None
        self.hung = threading.Event()
        self.release = threading.Event()
        real = pool._read_json

        def read(path, *args, **kwargs):
            name = Path(path).name
            if name.startswith(PREFIX):
                with self.lock:
                    self.reads += 1
                if name == self.hang_name:
                    self.hung.set()
                    self.release.wait(60.0)
                time.sleep(self.read_s)
            return real(path, *args, **kwargs)

        monkeypatch.setattr(pool, "_read_json", read)


def _file_cold_receipts(queue: pool.PoolQueue, count: int = COLD_RECEIPTS) -> None:
    """Half prewarm receipts, half movement receipts, none of them read yet."""

    for index in range(count):
        directory = queue.root / (
            pool.PREWARM if index % 2 else tier_loop.MOVER_RECEIPTS)
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"{PREFIX}{index:04d}.json").write_text(json.dumps(
            {"schema": "pb.test-1153-noise.v1", "unix": time.time()}))


def _replaced_loop(tmp_path: Path):
    """R12 on the stage, after the replaced loop's last cycle.

    The last cycle announced the tier and published R12's landing record,
    whose advance leg is waited for while the loop lives.
    """

    queue, stage, _plan, capacity = leg._r12(tmp_path)
    leg._cycle(queue, stage, gib=capacity)
    consumer = _Consumer(queue)
    age, alive, kind, detail = consumer.poll()
    assert alive and kind == "wait", (age, alive, kind, detail)
    return queue, stage, capacity, consumer


def _first_cycle(queue, stage, capacity, consumer: _Consumer
                 ) -> dict[str, object]:
    """The fresh loop's start and first cycle, with the consumer polling."""

    consumer.start()
    started = time.monotonic()
    try:
        receipts, liveness = _fresh_loop(queue)
        tier_loop.cycle(queue, host=HOST, source_pool="storage_pool",
                        receipts=receipts,
                        discover=lambda **_kw: {
                            TIER: _tier_record(stage, gib=capacity)},
                        liveness=liveness)
    finally:
        consumer.halt.set()
        consumer.join(30.0)
    consumer.poll()
    assert consumer.error is None, repr(consumer.error)
    return {"wall_s": round(time.monotonic() - started, 3),
            "receipts_s": tier_loop.LAST_CYCLE.get("phases", {}).get("receipts"),
            "liveness": tier_loop.LAST_CYCLE.get("liveness")}


def _assert_never_refused(consumer: _Consumer, line: dict[str, object],
                          label: str) -> None:
    ages = [age for age, _alive, _kind, _detail in consumer.polls
            if age is not None]
    refused = [poll for poll in consumer.polls
               if poll[2] == "refuse" or not poll[1]]
    print("cold-start-evidence " + json.dumps({
        "test": label, "polls": len(consumer.polls),
        "oldest_age_s": round(max(ages, default=-1.0), 3),
        "refused": len(refused), **line}, sort_keys=True, default=str))
    assert not refused, (
        f"a consumer's staged wait was refused during the fresh loop's first "
        f"cycle ({label}): first refusal {refused[0]}; {line}")
    assert max(ages) < BOUND_S, (max(ages), line)


def test_a_fresh_loop_keeps_the_consumer_waiting_through_a_cold_read(
        tmp_path, monkeypatch, scaled):
    """The acceptance case: both fixes, the live start path."""

    queue, stage, capacity, consumer = _replaced_loop(tmp_path)
    slow = _Slow(monkeypatch)
    _file_cold_receipts(queue)

    line = _first_cycle(queue, stage, capacity, consumer)

    assert slow.reads == COLD_RECEIPTS, slow.reads
    _assert_never_refused(consumer, line, "both")


def test_adoption_alone_keeps_the_consumer_waiting_through_a_serial_read(
        tmp_path, monkeypatch, scaled):
    """Fix 1 on its own: the read stays serial and outlasts ``L``."""

    monkeypatch.setattr(tier_loop, "RECEIPT_READERS", 1, raising=False)
    queue, stage, capacity, consumer = _replaced_loop(tmp_path)
    slow = _Slow(monkeypatch)
    _file_cold_receipts(queue)

    line = _first_cycle(queue, stage, capacity, consumer)

    assert slow.reads == COLD_RECEIPTS, slow.reads
    assert float(line["receipts_s"] or 0.0) > BOUND_S, line
    _assert_never_refused(consumer, line, "adoption-alone")


def test_parallel_reads_alone_keep_the_consumer_waiting(
        tmp_path, monkeypatch, scaled):
    """Fix 2 on its own: nothing is adopted, and the cold read is short."""

    monkeypatch.setattr(tier_loop.Liveness, "adopt",
                        lambda self, queue, *, host: [], raising=False)
    queue, stage, capacity, consumer = _replaced_loop(tmp_path)
    slow = _Slow(monkeypatch)
    _file_cold_receipts(queue)

    line = _first_cycle(queue, stage, capacity, consumer)

    assert slow.reads == COLD_RECEIPTS, slow.reads
    _assert_never_refused(consumer, line, "parallel-alone")


# ------------------------------------------------------------- adoption


def _announced(queue: pool.PoolQueue, record: dict[str, object], *,
               age_s: float) -> dict[str, object]:
    path = queue.tier_record_path(str(record["tier_id"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    written = dict(record, announced_unix=time.time() - age_s)
    path.write_text(json.dumps(written))
    return written


def _stage_record(stage: Path, **overrides) -> dict[str, object]:
    return {**_tier_record(stage, gib=5), **overrides}


def test_a_live_record_is_adopted_and_re_announced_at_once(tmp_path, scaled):
    queue, stage = leg._fixture_queue(tmp_path, 5)
    minted = _announced(queue, _stage_record(stage, fill_supply={"x": 1}),
                        age_s=3.0)
    refreshed = dict(minted, liveness_refresh={
        "after": "receipts:movers", "minted_unix": minted["announced_unix"] - 30,
        "refreshes": 4})
    queue.tier_record_path(TIER).write_text(json.dumps(refreshed))

    liveness = tier_loop.Liveness(interval_s=INTERVAL_S)
    before = time.time()
    assert liveness.adopt(queue, host=HOST) == [TIER]

    written = json.loads(queue.tier_record_path(TIER).read_text())
    assert written["announced_unix"] >= before
    content = {key: value for key, value in written.items()
               if key not in ("announced_unix", "liveness_refresh")}
    assert content == {key: value for key, value in minted.items()
                       if key != "announced_unix"}
    note = written["liveness_refresh"]
    # The mint's own stamp is carried over, not the adoption's.
    assert note["minted_unix"] == minted["announced_unix"] - 30, note
    assert note["after"] == "adopted", note
    assert note["refreshes"] == 5, note


@pytest.mark.parametrize("case", [
    "dead", "other-host", "other-schema", "stem-mismatch", "not-json",
    "no-stamp"])
def test_a_record_a_reader_would_not_accept_is_not_adopted(
        tmp_path, scaled, case):
    """What fails the readers' own checks stays as it is: nothing is written."""

    queue, stage = leg._fixture_queue(tmp_path, 5)
    tier_id = TIER
    record = _stage_record(stage)
    age = 1.0
    if case == "dead":
        age = BOUND_S + 1.0
    elif case == "other-host":
        tier_id = "prismabuild-stage:sparky"
        record = _stage_record(stage, tier_id=tier_id, host="sparky")
    elif case == "other-schema":
        record = _stage_record(stage, schema="prismabuild.storage_tier.v0")
    elif case == "stem-mismatch":
        record = _stage_record(stage, tier_id="ram:dl380g10")
    path = queue.tier_record_path(tier_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    if case == "not-json":
        path.write_text("{\"tier_id\": ")
    else:
        written = dict(record, announced_unix=time.time() - age)
        if case == "no-stamp":
            written.pop("announced_unix")
        path.write_text(json.dumps(written))
    before = path.read_bytes()
    stamp = path.stat().st_mtime_ns

    liveness = tier_loop.Liveness(interval_s=INTERVAL_S)
    assert liveness.adopt(queue, host=HOST) == []
    liveness.checkpoint("receipts:movers", ahead_s=10 * BOUND_S)

    assert path.read_bytes() == before
    assert path.stat().st_mtime_ns == stamp


def test_an_adopted_record_is_dropped_by_a_first_cycle_that_mints_nothing(
        tmp_path, scaled):
    """A first cycle that fails before its mint leaves nothing to refresh."""

    queue, stage = leg._fixture_queue(tmp_path, 5)
    _announced(queue, _stage_record(stage), age_s=1.0)
    liveness = tier_loop.Liveness(interval_s=INTERVAL_S)
    assert liveness.adopt(queue, host=HOST) == [TIER]
    liveness.begin_cycle()
    liveness.end_cycle(completed=False)
    path = queue.tier_record_path(TIER)
    before = path.read_bytes()

    liveness.checkpoint("receipts:movers", ahead_s=10 * BOUND_S)

    assert path.read_bytes() == before


# ------------------------------------------------------------- a hang


def test_a_hung_read_among_parallel_readers_reads_dead_within_the_bound(
        tmp_path, monkeypatch, scaled):
    """One read hangs; the others finish, and the record still goes dead.

    The in-order consumer stops at the hung entry, so no checkpoint runs
    and nothing writes the record: a reader sees a dead loop within
    ``L + P`` of the hang, as with a serial read.
    """

    queue, stage = leg._fixture_queue(tmp_path, 5)
    receipts = tier_loop.ReceiptCache()
    liveness = tier_loop.Liveness(interval_s=INTERVAL_S)

    def run_cycle() -> None:
        tier_loop.cycle(queue, host=HOST, source_pool="storage_pool",
                        receipts=receipts,
                        discover=lambda **_kw: {TIER: _tier_record(stage, gib=5)},
                        liveness=liveness)

    run_cycle()
    slow = _Slow(monkeypatch, read_s=0.01)
    _file_cold_receipts(queue, count=20)
    slow.hang_name = f"{PREFIX}0010.json"
    worker = threading.Thread(target=run_cycle, daemon=True)
    worker.start()
    try:
        assert slow.hung.wait(30.0), "the hung read never started"
        hung_at = time.time()
        dead_at = None
        while time.time() - hung_at <= BOUND_S + POLL_S + 2.0:
            alive, _age = queue._tier_loop_alive(TIER, now=time.time())
            if not alive:
                dead_at = time.time()
                break
            time.sleep(0.05)
        still_hung = worker.is_alive()
    finally:
        slow.release.set()
        worker.join(60.0)
    assert still_hung
    assert dead_at is not None, "a hung read kept the record alive"
    assert dead_at - hung_at <= BOUND_S + POLL_S, dead_at - hung_at
    # The other readers ran: every cold receipt was read, the hung one last.
    assert slow.reads == 20, slow.reads


# ------------------------------------------------------------- equivalence


def _past_the_tick() -> None:
    """Let the coarse clock pass every change just made (#1045).

    A version whose ctime is not before the read's fence is not kept, so
    without this whether a record is parsed again would depend on when the
    tick fell, and the two readers' counters could differ by chance.
    """

    time.sleep(0.05)


def _receipt_tree(root: Path) -> tuple[Path, Path]:
    """Two receipt directories: one clean, one holding an unreadable entry."""

    clean = root / "clean"
    clean.mkdir(parents=True)
    for index in range(30):
        (clean / f"r-{index:03d}.json").write_text(json.dumps(
            {"schema": "pb.test-1153.v1", "index": index}))
    (clean / "r-010.json").write_text("")              # listed, no record
    (clean / ".r-hidden.json").write_text("{}")         # not a receipt
    (clean / "notes.txt").write_text("not a receipt")
    broken = root / "broken"
    broken.mkdir()
    for index in range(10):
        (broken / f"r-{index:03d}.json").write_text(json.dumps(
            {"schema": "pb.test-1153.v1", "index": index}))
    # A receipt name that cannot be read as a file, whoever runs the test.
    (broken / "r-005-dir.json").mkdir()
    _past_the_tick()
    return clean, broken


def _snapshot(cache: tier_loop.ReceiptCache, read: list[dict[str, object]],
              directories: list[Path]) -> dict[str, object]:
    return {
        "read": read,
        "unreadable": {Path(name).name: why.split(":")[0]
                       for name, why in cache.unreadable.items()},
        "generations": [cache.records.generation(d) for d in directories],
        "parsed": cache.records.parsed, "listed": cache.records.listed,
        "kept": cache.records.kept,
    }


def test_the_parallel_reader_returns_what_the_serial_reader_returns(
        tmp_path, monkeypatch):
    clean, broken = _receipt_tree(tmp_path / "tree")
    directories = [clean, broken]
    seen = {}
    for readers in (1, 8):
        monkeypatch.setattr(tier_loop, "RECEIPT_READERS", readers, raising=False)
        cache = tier_loop.ReceiptCache()
        first = _snapshot(cache, cache.read(directories), directories)
        # Something changes: a receipt rewritten, one added, one removed.
        (clean / "r-003.json").write_text(json.dumps(
            {"schema": "pb.test-1153.v1", "index": 3, "again": readers}))
        (clean / "r-100.json").write_text(json.dumps(
            {"schema": "pb.test-1153.v1", "index": 100}))
        (clean / "r-004.json").unlink()
        _past_the_tick()
        second = _snapshot(cache, cache.read(directories), directories)
        seen[readers] = (first, second)
        # Put the tree back for the other reader count.
        (clean / "r-003.json").write_text(json.dumps(
            {"schema": "pb.test-1153.v1", "index": 3}))
        (clean / "r-004.json").write_text(json.dumps(
            {"schema": "pb.test-1153.v1", "index": 4}))
        (clean / "r-100.json").unlink()
        _past_the_tick()

    serial, parallel = seen[1], seen[8]
    for label, one, other in (("first", serial[0], parallel[0]),
                              ("second", serial[1], parallel[1])):
        # ``again`` differs by design; everything else must match.
        for record in one["read"] + other["read"]:
            record.pop("again", None)
        assert one == other, label
    first = serial[0]
    assert [record["index"] for record in first["read"]] == [
        index for index in range(30) if index != 10]
    assert first["unreadable"] == {"broken": "IsADirectoryError"}


def test_a_bad_record_raises_first_in_name_order_either_way(tmp_path):
    directory = tmp_path / "bad"
    directory.mkdir()
    for index in range(40):
        (directory / f"r-{index:03d}.json").write_text(json.dumps({"i": index}))
    (directory / "r-012.json").write_text("{\"schema\": ")   # a partial write
    (directory / "r-030-dir.json").mkdir()                   # unreadable
    raised = {}
    for readers in (1, 8):
        records = stage_release.DirectoryRecords()
        with pytest.raises(Exception) as caught:
            records.read(directory, select=tier_loop._receipt_name,
                         parse=pool._read_json, readers=readers)
        raised[readers] = (type(caught.value), str(caught.value),
                           records.generation(directory), records.parsed)
        # The directory is not remembered: the next read lists it again.
        (directory / "r-012.json").write_text(json.dumps({"i": 12}))
        with pytest.raises(IsADirectoryError):
            records.read(directory, select=tier_loop._receipt_name,
                         parse=pool._read_json, readers=readers)
        (directory / "r-012.json").write_text("{\"schema\": ")
    assert raised[1] == raised[8], raised
    assert raised[1][0] is pool.PoolContractError


def test_the_parallel_reader_hands_stat_parse_the_stat_it_versioned(tmp_path):
    directory = tmp_path / "stat"
    directory.mkdir()
    for index in range(12):
        (directory / f"r-{index:03d}.json").write_text("x" * index)
    results = {}
    for readers in (1, 4):
        records = stage_release.DirectoryRecords()
        results[readers] = [
            (path.name, record) for path, record in records.read(
                directory, select=tier_loop._receipt_name,
                parse=lambda path, info: (info.st_size, info.st_ino),
                stat_parse=True, readers=readers)]
    assert results[1] == results[4]
    assert [size for _name, (size, _ino) in results[1]] == list(range(12))

