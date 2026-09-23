"""pbmetrics: what a scrape keeps never outlives what it was read from (#1020).

The exporter keeps listings, per-record rows, receipt projections and each
residency plan's census from one scrape to the next.  Each of these tests
changes one thing the census reads -- a terminal record, a receipt, a lease,
a tier ledger's tokens, a fragment, a claim file, a plan, a tier record --
and checks that the kept scrape reports exactly what a scrape reading
everything afresh reports, both inside the clock tick of the change (when no
stamp can be trusted) and after it.  Where the change moves a gauge, the
test also checks that it moved, so an equality of two stale answers cannot
pass.

It also pins the exporter's placement: the ``metrics`` role the supervisor
spawns on the queue's host, and the singleton lock that keeps a second
exporter on that box from scanning the queue again.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import time
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_pbmetrics_kept_reads as shaped  # noqa: E402
from test_pbmetrics_kept_reads import (  # noqa: E402
    LIMIT, SMALL_SHAPE, STAGE_TIER, WINDOW_S, _key, _put, _replace,
    build_queue, frozen_clock, settle,
)

from prismabuild import pool, residency_map, storage_tiers  # noqa: E402
import pbmetrics  # noqa: E402
import supervise  # noqa: E402
import worker_loop  # noqa: E402

MANIFEST = "9" * 64
CONSUMER = _key("consumer", 0)
MOVER = _key("plan-mover-0", 0)
STAGED = f'prismabuild_residency_staged_phases{{leg="stage",tier="{STAGE_TIER}"}}'


def _collect(root: Path, now: float, reader) -> str:
    with frozen_clock(now):
        return pbmetrics.collect_metrics(
            root, now=now, terminal_window_seconds=WINDOW_S,
            terminal_limit=LIMIT, reader=reader)


def _sample(text: str, series: str) -> str | None:
    for line in text.splitlines():
        if line.startswith(series + " "):
            return line[len(series) + 1:]
    return None


class Scrapes:
    """One exporter's kept reader, checked against a fresh read every time."""

    def __init__(self, root: Path, now: float) -> None:
        self.root = root
        self.now = now
        self.reader = pbmetrics.KeptReads()

    def check(self, label: str) -> str:
        kept = _collect(self.root, self.now, self.reader)
        fresh = _collect(self.root, self.now, None)
        if kept != fresh:
            differ = sorted(set(kept.splitlines()) ^ set(fresh.splitlines()))
            pytest.fail(f"{label}: the kept scrape differs from a fresh one: "
                        f"{differ[:8]}")
        return kept

    def after(self, label: str, change) -> str:
        """Scrape once, change, scrape in the change's tick and after it."""

        self.check(f"{label} (before)")
        settle()
        self.check(f"{label} (settled before)")
        change()
        self.check(f"{label} (same tick)")
        settle()
        return self.check(label)


@pytest.fixture
def queue_and_scrapes(tmp_path):
    root = tmp_path / "pb-queue"
    now = time.time()
    queue = build_queue(root, now, SMALL_SHAPE)
    settle()
    return queue, Scrapes(root, now)


def _land(queue: pool.PoolQueue, *, complete: bool = True) -> None:
    """Make the first plan's first phase resident: fragment and receipt.

    Its tokens are already held (``build_queue`` books every fourth plan's
    first phase), so these two are what ``resident_movers`` still asks for.
    """

    stage = queue.root / "stage"
    path = stage / "part-0.bin"
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": STAGE_TIER, "stage_root": str(stage),
        "manifest_sha256": MANIFEST,
        "entries": {residency_map.residency_map_key("/pool/part-0.bin", 0): {
            "stage_path": str(path), "bytes": storage_tiers.GIB, "offset": 0,
            "sha256": "a" * 64}}})
    _file_receipt(queue, complete=complete)


def _file_receipt(queue: pool.PoolQueue, *, complete: bool) -> None:
    queue.record_move(MOVER, {
        "consumer_action_key": CONSUMER, "tier_id": STAGE_TIER,
        "stage_root": str(queue.root / "stage"), "manifest_sha256": MANIFEST,
        "range_start_bytes": 0, "range_end_bytes": storage_tiers.GIB,
        "range_bytes": storage_tiers.GIB, "bytes_staged": storage_tiers.GIB,
        "entries_declared": 1, "entries_staged": 1, "complete": complete,
        "seconds": 10.0, "unix": 2000.0})


def _file_raw(path: Path, text: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(text)
    os.replace(temporary, path)


def test_residency_follows_its_fragment_ledger_receipt_and_claim(queue_and_scrapes):
    queue, scrapes = queue_and_scrapes
    shaped._require_trusted(queue.root)
    assert _sample(scrapes.check("initial"), STAGED) == "0"

    text = scrapes.after("fragment and receipt filed", lambda: _land(queue))
    assert _sample(text, STAGED) == "1"

    ledger = queue.tier_ledger(STAGE_TIER)
    text = scrapes.after("tokens released", lambda: ledger.release(MOVER))
    assert _sample(text, STAGED) == "0"
    text = scrapes.after("tokens taken again",
                         lambda: ledger.acquire(MOVER, {"stage_gib": 1}))
    assert _sample(text, STAGED) == "1"

    text = scrapes.after("receipt replaced by an incomplete one",
                         lambda: _file_receipt(queue, complete=False))
    assert _sample(text, STAGED) == "0"
    text = scrapes.after("receipt complete again",
                         lambda: _file_receipt(queue, complete=True))
    assert _sample(text, STAGED) == "1"

    # A claim file for the mover makes its receipt a previous run's, even
    # one too torn to parse: ``resident_movers`` asks the filesystem.
    claim = queue.item_path(pool.CLAIMED, MOVER)
    text = scrapes.after("a torn claim for the mover",
                         lambda: _file_raw(claim, "{"))
    assert _sample(text, STAGED) == "0"
    text = scrapes.after("the torn claim removed", claim.unlink)
    assert _sample(text, STAGED) == "1"

    fragment = residency_map.fragment_path(
        queue.residency_fragment_root(), CONSUMER, MOVER)
    text = scrapes.after("fragment removed", fragment.unlink)
    assert _sample(text, STAGED) == "0"


def test_the_cursor_follows_the_lease(queue_and_scrapes):
    queue, scrapes = queue_and_scrapes
    _land(queue)
    settle()
    assert _sample(scrapes.check("landed"), STAGED) == "1"
    lease_path = queue.lease_path(CONSUMER)
    lease = json.loads(lease_path.read_text())
    lease["progress_observation"]["last_accepted"]["phase"] = "phase-0001"
    # The staged first phase is now behind the cursor, which counts only
    # what is at or ahead of it.
    text = scrapes.after("accepted phase moved",
                         lambda: _replace(lease_path, lease))
    assert _sample(text, STAGED) == "0"


def test_heartbeats_and_announcements_are_followed(queue_and_scrapes):
    """Records rewritten every few seconds: nothing kept may shadow them."""

    queue, scrapes = queue_and_scrapes
    scrapes.check("initial")

    def heartbeat() -> None:
        for path in sorted(queue.lease_path(CONSUMER).parent.glob("*.lease")):
            lease = json.loads(path.read_text())
            lease["heartbeat_unix"] = lease["heartbeat_unix"] + 0.5
            _replace(path, lease)

    scrapes.after("every lease heartbeated", heartbeat)

    record = next(record for record in queue.tiers()
                  if record.get("tier_id") == STAGE_TIER)
    scrapes.after("tier announced again",
                  lambda: queue.announce_tier({**record,
                                               "sampled_unix": scrapes.now - 1}))


def test_plans_follow_their_consumers_and_their_files(queue_and_scrapes):
    queue, scrapes = queue_and_scrapes
    ready_plans = (f'prismabuild_residency_plans'
                   f'{{state="ready",tier="{STAGE_TIER}"}}')
    before = _sample(scrapes.check("initial"), ready_plans)
    consumer = _key("consumer", 2)
    text = scrapes.after("a plan's consumer published", lambda: _put(
        queue.root / pool.READY / f"{consumer}.json",
        shaped._item(consumer, scrapes.now - 5.0)))
    assert int(_sample(text, ready_plans)) == int(before) + 1

    absent = (f'prismabuild_residency_plans'
              f'{{state="absent",tier="{STAGE_TIER}"}}')
    counted = int(_sample(text, absent))
    plan = queue.residency_plan_path(_key("consumer", 3))
    text = scrapes.after("a plan removed", plan.unlink)
    assert int(_sample(text, absent)) == counted - 1


def test_history_follows_new_records(queue_and_scrapes):
    queue, scrapes = queue_and_scrapes
    jobs = f'prismabuild_tier_move_jobs{{tier="{STAGE_TIER}"}}'
    window = "prismabuild_terminal_outcomes_window_jobs"
    before = scrapes.check("initial")

    key = _key("late-ending", 0)
    text = scrapes.after("a new ending", lambda: _put(
        queue.root / pool.DONE / f"{key}.json",
        shaped._ending(key, "executed", "sparky", scrapes.now - 1.0,
                       profiled=True), scrapes.now - 1.0))
    assert int(_sample(text, window)) == int(_sample(before, window)) + 1

    receipt = _key("late-receipt", 0)
    text = scrapes.after("a new receipt", lambda: _put(
        queue.root / pool.MOVERS / f"{receipt}.json", {
            "schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": receipt,
            "tier_id": STAGE_TIER, "host": "dl380g10",
            "unix": scrapes.now - 1.0, "complete": True,
            "bytes_staged": storage_tiers.GIB,
            "disk_pacing": {"mean_pool_read_mb_s": 900.0,
                            "pool_read_bytes": storage_tiers.GIB}},
        scrapes.now - 1.0))
    assert int(_sample(text, jobs)) == int(_sample(before, jobs)) + 1

    # A withdrawal decision newer than its summary displaces the summary.
    withdrawn = _key(pool.WITHDRAWN, 0)
    summary = queue.root / pool.WITHDRAWN / f"{withdrawn}.json"
    decided = os.stat(summary).st_mtime + 1.0
    scrapes.after("a newer withdrawal decision", lambda: _put(
        queue.root / pool.WITHDRAWN / "decisions" / withdrawn / "2.000000.json",
        {"status": "withdrawn", "action_key": withdrawn}, decided))

    # Filed by rename, as every queue writer files (``DirectoryRecords``).
    scrapes.after("a torn receipt filed", lambda: _file_raw(
        queue.root / pool.MOVERS / f"{receipt}.json", "{"))


def test_a_kept_reader_holds_nothing_the_queue_no_longer_has(queue_and_scrapes):
    """What is kept is bounded by what is on disk, not by history."""

    queue, scrapes = queue_and_scrapes
    scrapes.check("initial")
    records = scrapes.reader.records
    decisions = queue.root / pool.WITHDRAWN / "decisions"
    gone = sorted(decisions.iterdir())[0]
    assert str(gone) in records._directories
    receipts = sorted((queue.root / pool.MOVERS).glob("*.json"))
    for path in receipts[: len(receipts) // 2]:
        path.unlink()
    for path in gone.iterdir():
        path.unlink()
    gone.rmdir()
    settle()
    scrapes.check("half the receipts and one decision gone")
    assert str(gone) not in records._directories
    movers = records._directories[str(queue.root / pool.MOVERS)][1]
    assert len(movers) == len(receipts) - len(receipts) // 2
    derived = scrapes.reader._derived_entries.get("receipt", {})
    assert len(derived) <= len(movers)


# --------------------------------------------------------------------------
# Placement: one exporter per fleet, on the queue's host
# --------------------------------------------------------------------------

def test_the_metrics_role_runs_the_exporter_on_the_queue_host():
    assert supervise.ROLE_SCRIPTS["metrics"] == "pbmetrics.py"
    boxes = json.loads((shaped.REPOSITORY / "tools" / "fleet"
                        / "fleet_boxes.json").read_text())["boxes"]
    declared = {host: sorted((spec.get("roles") or {}))
                for host, spec in boxes.items() if isinstance(spec, dict)}
    assert [host for host, roles in declared.items() if "metrics" in roles] == ["dl380g10"]
    args = pbmetrics.parser().parse_args(boxes["dl380g10"]["roles"]["metrics"])
    assert args.queue_root == Path("/mnt/shared/prismabuild-fleet/pb-queue")
    assert not args.once
    import publish_runtime
    assert "pbmetrics.py" in publish_runtime.FLEET_SCRIPTS


def test_a_second_exporter_on_the_box_refuses(tmp_path, capsys):
    with mock.patch.object(worker_loop, "ROLE_LOCK_ROOT", tmp_path / "roles"):
        with worker_loop.role_singleton(Path(pbmetrics.__file__)):
            with mock.patch.object(pbmetrics, "ThreadingHTTPServer") as server:
                code = pbmetrics.main(["--port", "9469",
                                       "--queue-root", str(tmp_path / "q")])
        server.assert_not_called()
    assert code == worker_loop.ROLE_SINGLETON_HELD_EXIT
    assert "refusing a second metrics exporter" in capsys.readouterr().err


def test_the_exporter_serves_under_its_role_lock(tmp_path):
    held = []

    class Server:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def serve_forever(self) -> None:
            held.append(worker_loop.role_singleton_holder(
                Path(pbmetrics.__file__))[0])

        def server_close(self) -> None:
            pass

    with mock.patch.object(worker_loop, "ROLE_LOCK_ROOT", tmp_path / "roles"), \
            mock.patch.object(pbmetrics, "ThreadingHTTPServer", Server):
        assert pbmetrics.main(["--port", "9469",
                               "--queue-root", str(tmp_path / "q")]) == 0
        assert held == [True]
        assert worker_loop.role_singleton_holder(Path(pbmetrics.__file__)) == (False, None)


def test_once_takes_no_lock(tmp_path, capsys):
    with mock.patch.object(worker_loop, "ROLE_LOCK_ROOT", tmp_path / "roles"):
        with worker_loop.role_singleton(Path(pbmetrics.__file__)):
            assert pbmetrics.main(["--once", "--queue-root", str(tmp_path / "q")]) == 0
    assert "prismabuild_collection_success" in capsys.readouterr().out
