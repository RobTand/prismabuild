"""Independent census coverage and the three real rendering paths."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from history_inputs import materialize_sources
import time
from urllib.request import urlopen

import pytest

from prismabuild import census, core, pool
import pbmcp
import pbmcp_fixture as fixture
import pbmetrics


def test_offers_do_not_read_active_rows_and_keep_their_own_coverage(tmp_path, monkeypatch):
    fleet = fixture.build(tmp_path)
    read = census._pool_records
    seen = []

    def observe(directory, **kwargs):
        seen.append(directory.name)
        if directory.name != pool.record_store.WORKERS:
            raise AssertionError("offers read an unrelated section")
        return read(directory, **kwargs)

    monkeypatch.setattr(census, "_pool_records", observe)
    section = census.offers(fleet.queue_root)
    assert section.completeness == "complete"
    assert seen == [pool.record_store.WORKERS]
    assert section.inputs["workers"] == str(fleet.queue_root / pool.record_store.WORKERS)


def test_partial_rows_keep_known_claims_and_cannot_prove_empty(tmp_path):
    fleet = fixture.build(tmp_path)
    fleet.queue.records.item_path(pool.record_store.READY, fixture.READY_KEY).write_text("{")
    observed = census.action_rows(fleet.queue_root)
    assert observed.completeness == "partial"
    assert observed.value["queue"]["ready"] is None
    assert observed.value["queue"]["empty"] is None
    assert any(row["action_key"] == fixture.CLAIMED_KEY for row in observed.value["jobs"])
    assert observed.errors
    assert census.offers(fleet.queue_root).completeness == "complete"


def test_denial_profiles_preserve_the_different_zero_and_future_rules(tmp_path):
    now = time.time()
    records = [{"action_key": str(i) * 64, "host": "fixture-box", "reason": "capacity",
                "published_unix": now - 100, "denied_unix": stamp, "evidence": {}}
               for i, stamp in enumerate((now - 20, now, now + 1), 1)]
    hours = census.denials(tmp_path, hours=0, now=now, captured=(records, []))
    assert hours.value["total_in_window"] == 2
    assert hours.inputs["zero_hours"] == "all history"
    missing = census.denials(tmp_path, captured=None, now=now)
    assert missing.completeness == "unavailable"
    assert missing.value["hosts"] is None
    assert missing.value["total_in_window"] is None


def test_windowed_denials_keep_partial_counts_without_future_credit(tmp_path, monkeypatch):
    now = time.time()
    records = [{"action_key": str(i) * 64, "host": "fixture-box", "reason": "capacity",
                "published_unix": now - 100, "denied_unix": stamp, "evidence": {}}
               for i, stamp in enumerate((now - 20, now, now + 1), 1)]
    monkeypatch.setattr(census, "_pool_claim_denials", lambda *a, **k: (records, []))
    metrics = census.denials(tmp_path, profile="metrics", now=now, window_seconds=20)
    mcp = census.denials(tmp_path, now=now, hours=20 / 3600)
    assert metrics.value == {("fixture-box", "capacity"): 2}
    assert mcp.value["total_in_window"] == 2
    assert metrics.completeness == "partial"


def test_unreadable_live_mover_input_cannot_prove_an_empty_population(tmp_path):
    fleet = fixture.build(tmp_path)
    fleet.queue.records.item_path(pool.record_store.CLAIMED, fixture.CLAIMED_KEY).write_text("{")
    section = census.movers(fleet.queue_root)
    assert section.completeness == "partial"
    assert section.errors


def test_unreadable_tier_listing_cannot_prove_an_empty_population(tmp_path):
    for name in ("ready", "claimed", "workers", "tiers"):
        (tmp_path / name).mkdir()
    (tmp_path / "tier-reservations").symlink_to("tier-reservations", target_is_directory=True)
    for reader in (None, census.KeptReads()):
        if reader is not None:
            reader.begin()
        try:
            for view in ("diagnostics", "tokens", "sources"):
                assert census.tiers(tmp_path, view=view, reader=reader).completeness == "partial"
        finally:
            if reader is not None:
                reader.end()
    assert census.starvation(tmp_path, now=100.0)["complete"] is False
    assert pbmetrics._starvation_metrics(pbmetrics.Metrics(), tmp_path, now=100.0,
                                         window_seconds=20.0) is False


def test_partial_movers_keep_known_rows_without_empty_depth_credit(tmp_path):
    fleet = fixture.build_starved(tmp_path)
    path = fleet.queue.records.item_path(pool.record_store.READY, fixture._hexkey("mover1"))
    known = json.loads(path.read_text())
    known["action_key"] = "e" * 64
    fleet.queue.records.item_path(pool.record_store.READY, known["action_key"]).write_text(json.dumps(known))
    path.write_text("{")
    counts = census.movers(fleet.queue_root, view="counts")
    assert counts.completeness == "partial"
    assert counts.value["ready"] is None
    assert counts.value["claimed"] == 0
    answer = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                           repo_link=fleet.repo_link, deadline_s=10).call("pb_movers")
    stage = next(row for row in answer["tiers"] if row["tier_id"] == fixture.STAGE_TIER)
    assert answer["complete"] is False
    assert any(row["section"] == "movers-live" for row in answer["unavailable"])
    assert stage["queue_depth"] is None
    assert [row["action_key"] for row in stage["ready_movers"]] == [known["action_key"]]
    text = pbmetrics.collect_metrics(fleet.queue_root, now=time.time())
    assert 'prismabuild_mover_queue_depth{state="ready"}' not in text
    assert 'prismabuild_mover_queue_depth{state="claimed"} 0' in text
    assert not any(line.startswith("prismabuild_mover_queue_depth_tier{")
                   and 'state="ready"' in line for line in text.splitlines())


def test_partial_active_population_never_becomes_an_absent_plan(tmp_path):
    fleet = fixture.build_starved(tmp_path)
    claimed = fleet.queue.records.dir(pool.record_store.CLAIMED)
    claimed.rename(tmp_path / "saved-claimed")
    claimed.symlink_to(claimed.name, target_is_directory=True)
    plans = census.plans(fleet.queue_root, view="starvation")
    assert plans.completeness == "partial"
    row = next(row for row in plans.value["residency_plans"]
               if row["consumer_action_key_prefix"] == fixture.CONSUMER_KEY[:12])
    assert row["state"] is None
    assert row["phases"][1]["stage"]["published"] is True
    assert census.waits(fleet.queue_root).completeness == "partial"
    facts = census.promotion(fleet.queue_root, now=time.time(), window_seconds=3600)
    assert facts.value["depth"][(fixture.STAGE_TIER, "claimed")] is None
    assert facts.value["depth"][(fixture.STAGE_TIER, "ready")] == 1
    assert facts.value["plans"][(fixture.STAGE_TIER, "absent")] is None
    assert facts.value["sections"]["delivery"].completeness == "complete"
    text = pbmetrics.collect_metrics(fleet.queue_root, now=time.time())
    assert not any(line.startswith("prismabuild_residency_plans{")
                   and 'state="absent"' in line for line in text.splitlines())


def test_unknown_holder_stays_unknown_in_plan_reservation_and_publication(tmp_path):
    fleet = fixture.build_starved(tmp_path)
    ledger = fleet.queue.tier(fixture.STAGE_TIER)
    holder = ledger.paths.held / fixture._hexkey("mover0")
    holder.rename(tmp_path / "saved-holder")
    holder.symlink_to(holder.name, target_is_directory=True)
    plans = census.plans(fleet.queue_root, view="starvation")
    assert plans.completeness == "partial"
    row = next(row for row in plans.value["residency_plans"]
               if row["consumer_action_key_prefix"] == fixture.CONSUMER_KEY[:12])
    assert row["phases"][0]["stage"]["reserved"] is None
    assert row["phases"][0]["stage"]["published"] is None
    assert row["phases"][0]["stage"]["staged"] is None
    assert row["phases"][1]["stage"]["reserved"] is False
    assert census.starvation(fleet.queue_root)["complete"] is False


def test_unreadable_denial_snapshot_keeps_metric_coverage_partial(tmp_path):
    for name in ("workers", "ready", "claimed", "tiers"):
        (tmp_path / name).mkdir()
    directory = tmp_path / pool.RESERVATIONS / "fixture-box" / "adaptive"
    directory.mkdir(parents=True)
    (directory / pool.CLAIM_DENIALS).write_text("{")
    section = census.denials(tmp_path, profile="metrics", now=time.time(), window_seconds=20)
    assert section.completeness == "partial"
    assert section.errors
    assert pbmetrics._starvation_metrics(pbmetrics.Metrics(), tmp_path, now=time.time(),
                                         window_seconds=20) is False


def test_ram_read_failure_keeps_captured_stage_residency(tmp_path):
    fleet = fixture.build_starved(tmp_path)
    fleet.queue.records.tier_record_path(fixture.RAM_TIER).write_text("{")
    section = census.plans(fleet.queue_root, view="starvation")
    row = next(row for row in section.value["residency_plans"]
               if row["consumer_action_key_prefix"] == fixture.CONSUMER_KEY[:12])
    assert section.completeness == "partial"
    assert row["phases"][0]["stage"]["staged"] is True
    assert row["phases"][0]["ram"]["staged"] is None


def test_unreadable_filed_receipt_cannot_prove_no_receipts(tmp_path):
    fleet = fixture.build_starved(tmp_path)
    receipt = fleet.queue_root / pool.record_store.MOVERS / (fixture._hexkey("mover0") + ".json")
    receipt.write_text("{")
    answer = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                           repo_link=fleet.repo_link, deadline_s=10).call("pb_movers")
    stage = next(row for row in answer["tiers"] if row["tier_id"] == fixture.STAGE_TIER)
    assert answer["complete"] is False
    assert any(row["section"] == "movers-filed" for row in answer["unavailable"])
    assert stage["receipts_filed"] is None
    assert stage["filed_receipts"] == []


def test_unreadable_fragment_population_never_becomes_absent(tmp_path):
    fleet = fixture.build_starved(tmp_path)
    directory = fleet.queue.records.residency_fragment_root() / fixture.CONSUMER_KEY
    directory.rename(tmp_path / "saved-fragments")
    directory.symlink_to(directory.name, target_is_directory=True)
    section = census.plans(fleet.queue_root, view="starvation")
    row = next(row for row in section.value["residency_plans"]
               if row["consumer_action_key_prefix"] == fixture.CONSUMER_KEY[:12])
    assert section.completeness == "partial"
    assert row["phases"][0]["stage"]["fragment"] is None
    assert row["phases"][0]["stage_fragment"] is None
    facts = census.promotion(fleet.queue_root, now=time.time(), window_seconds=3600)
    assert facts.value["staged_phases"][(fixture.STAGE_TIER, "stage")] is None


def test_offer_sample_errors_keep_metadata_partial_and_population_counts_known(tmp_path):
    fleet = fixture.build(tmp_path)
    fleet.queue.announce(host="fixture-box", tags=["x86"], has_gpu=False,
                         capacity={"cpu": 8, "mem_gb": 16})
    sample = fleet.queue_root / pool.RESERVATIONS / "fixture-box" / "adaptive" / "cpu-sample.json"
    sample.parent.mkdir(parents=True, exist_ok=True)
    sample.write_text("{")
    assert census.offers(fleet.queue_root).completeness == "partial"
    rows = census.action_rows(fleet.queue_root)
    assert rows.completeness == "partial"
    assert rows.errors
    assert rows.value["queue"]["complete"] is True


def test_blocked_origin_commands_keep_the_original_json_spelling():
    ref = {"unicode": "é", "nested": {"z": float("nan"), "a": 1}}
    expected = json.dumps(ref, sort_keys=True, separators=(",", ":"))
    assert core.compact_diagnostic_json(ref) == expected
    assert expected.replace("'", "'\"'\"'") in census.blocked_origin_remedies(ref, "a" * 64)["release"]


def test_promotion_retains_each_sections_inputs_and_coverage(tmp_path):
    fleet = fixture.build_starved(tmp_path)
    facts = census.promotion(fleet.queue_root, now=time.time(), window_seconds=3600)
    assert set(facts.value["sections"]) == {"tiers", "movers", "waits", "plans", "delivery"}
    for section in facts.value["sections"].values():
        assert section.completeness in {"complete", "partial", "unavailable"}
        assert section.inputs["queue_root"] == str(fleet.queue_root)
    assert facts.value["sections"]["delivery"].bound["mover_receipts"] == 1000


def test_cli_mcp_and_http_observe_one_private_actual_store(tmp_path):
    fleet = fixture.build_starved(tmp_path)
    script = Path(pbmcp.__file__).with_name("pbstatus.py")
    run = subprocess.run([sys.executable, str(script), "--json", "--recent", "0",
                          "--queue-root", str(fleet.queue_root), "--timeout-s", "10"],
                         capture_output=True, text=True, timeout=20)
    assert run.returncode == 0, run.stderr
    cli = json.loads(run.stdout)
    session = pbmcp.Session(queue_root=fleet.queue_root, cas_root=fleet.cas_root,
                            repo_link=fleet.repo_link, deadline_s=10)
    mcp = session.call("pb_status", {"recent": 0})
    assert mcp["complete"] is True
    assert cli["pool"]["ready"] == mcp["queue"]["ready"]
    assert cli["pool"]["claimed"] == mcp["queue"]["claimed"]
    cache = pbmetrics.MetricsCache(fleet.queue_root, cache_seconds=0,
                                    terminal_window_seconds=3600, terminal_limit=500)
    server = pbmetrics.ThreadingHTTPServer(("127.0.0.1", 0), pbmetrics._handler(cache))
    worker = threading.Thread(target=server.serve_forever)
    worker.start()
    try:
        with urlopen(f"http://127.0.0.1:{server.server_port}/metrics", timeout=20) as reply:
            text = reply.read().decode()
        for state in ("ready", "claimed"):
            assert f'prismabuild_queue_items{{state="{state}"}} {mcp["queue"][state]}' in text
        assert 'prismabuild_starvation_waiting_consumers{tier="' + fixture.STAGE_TIER + '"} 1' in text
        assert session.call("pb_cursors")["cursors"][0]["cursor_gap"]["stage"]["unstaged_bytes"] >= 0
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)
    assert not worker.is_alive()
    assert server.socket.fileno() == -1


_OBSERVE = r'''
import io, json, os, sys, time
from pathlib import Path
source, root, moment = Path(sys.argv[1]), Path(sys.argv[2]), float(sys.argv[3])
exporter_families = json.loads(sys.argv[4])
sys.path[:0] = [str(source / p) for p in ("src", "tools/fleet", "tests")]
import pbstatus, pbmcp, pbmetrics
current = (source / "src/prismabuild/census.py").is_file()
if current:
    from prismabuild import census
time.time = lambda: moment
events = []
def audit(event, arguments):
    if event not in {"open", "os.scandir", "os.listdir", "fcntl.flock"}:
        return
    path = arguments[0]
    if isinstance(path, int):
        try:
            path = os.readlink("/proc/self/fd/" + str(path))
        except OSError:
            return
    path = os.fsdecode(path)
    if path.startswith(str(root)):
        events.append([event, path, list(arguments[1:])])
sys.addaudithook(audit)
values, schedules = {}, {}
questions = [
    ("rows", lambda: census.action_rows(root).value if current else pbstatus.read_pool(root)),
    ("starvation", lambda: census.starvation(root, now=moment) if current else pbstatus.read_starvation(root, now=moment)),
    ("blocked_origins", lambda: census.blocked_origins(root).value if current else pbstatus.read_blocked_origins(root)),
    ("cursors", lambda: census.plans(root, now=moment).value if current else pbmcp._cursor_census(root, now=moment)),
    ("tiers", lambda: census.tiers(root).value if current else pbmcp._tier_rows(root)),
    ("movers", lambda: census.movers(root).value if current else pbmcp._mover_live_rows(root)),
    ("filed", lambda: census.movers(root, view="filed").value if current else pbmcp._filed_move_receipts(root)),
    ("metrics", lambda: pbmetrics.collect_metrics(root, now=moment)),
]
for name, read in questions:
    events.clear()
    value = read()
    if name == "metrics":
        # Compare queue facts; the extra stamp read changes exporter work.
        prefixes = tuple(prefix + family + suffix for family in exporter_families
                         for prefix in ("", "# HELP ", "# TYPE ") for suffix in (" ", "{"))
        value = [line for line in value.splitlines() if not line.startswith(prefixes)]
    values[name], schedules[name] = value, list(events)
print(json.dumps({"values": values, "schedules": schedules}))
'''


def test_original_fact_values_and_observable_capture_order_match(tmp_path, monkeypatch):
    # A transported checkout may have none of the historical Git objects.
    monkeypatch.setenv("GIT_DIR", str(tmp_path / "unavailable-history.git"))
    repository = Path(__file__).resolve().parents[1]
    original = tmp_path / "original"
    original.mkdir()
    materialize_sources("8e8572ee324b8508b9a7dd6a6dcb7d3548b663a7", original)
    strict_reference = tmp_path / "strict-reference"
    strict_reference.mkdir()
    materialize_sources("082f75ea50b2601c8681fe070a5a342e1b042702", strict_reference)
    fleet = fixture.build_starved(tmp_path / "store")
    now = str(time.time())
    environment = {**os.environ, "PYTHONPATH": ""}
    readings = []
    for source in (original, repository, strict_reference):
        run = subprocess.run([sys.executable, "-c", _OBSERVE, str(source), str(fleet.queue_root), now,
                              json.dumps(sorted(pbmetrics.EXPORTER_FAMILIES))],
                             env=environment, capture_output=True, text=True, timeout=60)
        assert run.returncode == 0, run.stderr
        readings.append(json.loads(run.stdout))
    assert readings[0]["values"] == readings[1]["values"]
    assert readings[2]["values"] == readings[1]["values"]
    for reading in readings:
        for events in reading["schedules"].values():
            for event in events:
                relative = Path(event[1]).relative_to(fleet.queue_root).parts
                if (event[0] in {"os.listdir", "os.scandir"} and len(relative) == 4
                        and relative[0] == "tier-reservations" and relative[2] == "held"):
                    event[0] = "strict-holder-listing"
                    event[1] = str(Path(event[1]))
    # #2230 adds one exact stamp lookup per lease freshness observation.
    # Keep the historical comparison for every other read and fact.
    stamps = {fleet.queue._heartbeat_stamp_path(pool.record_store.read_document(path))
              for path in fleet.queue.records.claimed_lease_paths()}
    added = []
    for name, events in readings[1]["schedules"].items():
        kept = []
        for event in events:
            if Path(event[1]).is_relative_to(fleet.queue_root / "lease-heartbeats"):
                assert event[0] == "open" and Path(event[1]) in stamps
                added.append((name, event[1]))
            else:
                kept.append(event)
        readings[1]["schedules"][name] = kept
    assert added
    assert readings[2]["schedules"] == readings[1]["schedules"]
