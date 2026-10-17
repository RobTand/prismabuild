"""Independent census coverage and the three real rendering paths."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import threading
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
    observed = census.rows(fleet.queue_root)
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
