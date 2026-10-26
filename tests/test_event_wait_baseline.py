"""Slice 0 evidence cannot turn missing history into fast claim/drain latency.

#2067 also requires honest identity/coverage and bounded diagnostic evidence.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from maintenance import event_wait_baseline as baseline


@pytest.fixture
def ewb_evidence():
    return {"schema": baseline.EWB_SCHEMA, "started_unix": 100., "ended_unix": 200.,
            "duration_s": 100., "phase": "steady", "clock_ticks": 100,
            "roles": {}, "samples": [], "records": [], "tier_cycles": [], "tier_events": [],
            "limits_hit": [], "x86_tags": ["x86"], "x86_hosts": ["test-x86"]}


def test_ewb_percentiles_use_nearest_rank_and_empty_is_unknown():
    assert baseline.ewb_percentiles([]) == {"p50_s": None, "p95_s": None, "p99_s": None}
    assert baseline.ewb_percentiles(range(1, 101)) == {"p50_s": 50, "p95_s": 95, "p99_s": 99}


def test_ewb_same_key_distinct_publications_and_retries(ewb_evidence):
    for namespace, stamp, claimed, attempts in [
            ("claimed", 110., 111., 0), ("done", 110., 111., 1),
            ("claimed", 110., 117., 1), ("ready", 140., None, 0)]:
        ewb_evidence["records"].append({"namespace": namespace, "body": {
            "action_key": "a" * 64, "published_unix": stamp, "claimed_unix": claimed,
            "attempts": attempts, "tags": ["x86"]}})
    result = baseline.ewb_analyze(ewb_evidence)["claim_latency"]
    assert result["samples"] == 1 and result["p50_s"] == 1
    assert result["retry_publication_to_claim"]["p50_s"] == 7
    assert result["censored"][0]["published_unix"] == 140


def test_ewb_backlog_and_arm_are_explicit(ewb_evidence):
    for key, published, tags in [("a", 90, ["x86"]), ("b", 110, ["arm"]), ("c", 120, [])]:
        ewb_evidence["records"].append({"namespace": "ready", "body": {
            "action_key": key * 64, "published_unix": published, "tags": tags}})
    result = baseline.ewb_analyze(ewb_evidence)["claim_latency"]
    assert result["backlog_publications"] == 1
    assert result["non_x86_or_unknown"] == 2
    assert result["p50_s"] is None


def test_ewb_conflicting_claims_and_negative_clock_are_not_successes(ewb_evidence):
    for claimed in (119, 121, 122):
        ewb_evidence["records"].append({"namespace": "claimed", "body": {
            "action_key": "b" * 64, "published_unix": 120, "claimed_unix": claimed,
            "tags": ["x86"], "attempts": 0}})
    result = baseline.ewb_analyze(ewb_evidence)["claim_latency"]
    assert result["invalid_clock_records"] == 1
    assert result["conflicting_attempt_claims"]
    assert result["samples"] == 0


def test_ewb_drain_requires_first_refusal_and_matching_epoch_host_attempt(ewb_evidence):
    for host, published, attempt, observations in [
            ("host-a", 110, 0, [(120, False, 1), (125, False, 2), (130, True, 3)]),
            ("host-a", 140, 0, [(141, False, 1)]),
            ("host-b", 110, 0, [(122, True, 3)]),
            ("host-a", 110, 1, [(150, False, 2), (151, True, 3)])]:
        for stamp, eligible, count in observations:
            ewb_evidence["records"].append({"namespace": "denials", "body": {
                "action_key": "a" * 64, "published_unix": published, "host": host,
                "attempts": attempt, "denied_unix": stamp,
                "evidence": {"denials": count, "withhold": {"eligible": eligible}}}})
    result = baseline.ewb_analyze(ewb_evidence)["drain_maturity"]
    assert result["p50_s"] == 10 and len(result["windows"]) == 1
    assert result["right_censored"] == 1
    assert result["left_censored_or_immediate"] == 2


def test_ewb_proc_rates_do_not_join_pid_reuse(ewb_evidence):
    ewb_evidence["roles"] = {"tier": {}}
    for stamp, start, value in [(1, 7, 0), (11, 7, 100), (21, 8, 300)]:
        ewb_evidence["samples"].append({"monotonic": stamp, "roles": {"tier": {
            "start_ticks": start, "cpu_ticks": value, "read_bytes": value,
            "rchar": value * 10, "syscr": value, "syscw": value}}})
    result = baseline.ewb_analyze(ewb_evidence)["roles"]["tier"]
    assert result["covered_s"] == 10
    assert result["cpu_s_per_hour"] == 360
    assert result["io_syscalls_per_min"] == 1200
    assert result["total_syscalls_per_min"] is None


@pytest.fixture
def ewb_tier_rows():
    # Flat producer shapes from tier_cycle's RAM/window/eviction events, funding
    # settlement and tier_cycle_line. _emit adds the top-level log timestamp.
    return [
        {"event": "ram-window-gated", "unix": 123., "consumer": "a" * 64,
         "tier_id": "ram:test-x86", "reason": "output-not-ready", "permanent": False,
         "need_gib": 8., "output_note": "", "verdict_since_unix": 123., "waited_s": 0.},
        {"event": "window-gated", "unix": 123., "consumer": "a" * 64,
         "tier_id": "stage:test-x86", "reason": "claim-order", "permanent": False,
         "need_gib": 8., "output_note": "", "ahead": "b" * 64, "rank": 1,
         "need_phase": "compile", "expected_landing_unix": 140.,
         "expected_landing_basis": "claim-order",
         "commitment": {"until_unix": 140.}},
        {"event": "claim-order-evicted", "unix": 123., "consumer": "b" * 64,
         "tier_id": "stage:test-x86", "mover": "c" * 64, "phase": "compile",
         "chunk_index": 0, "stage_gib": 8., "seconds_until_needed": 120.,
         "needed_gib": 8, "tokens_released": 8, "tokens_decharged": 8,
         "declined": [], "live_pins": [], "errors": [], "for_consumer": "a" * 64},
        {"event": "advance-handed-off", "unix": 123., "consumer": "a" * 64,
         "tier_id": "stage:test-x86", "leg": "input", "mover": "d" * 64, "moved_gib": 8.},
        {"event": "ram-window-mover-published", "unix": 123., "consumer": "a" * 64,
         "phase": "compile", "tier_id": "ram:test-x86", "mover": "e" * 64, "chunk_index": 0},
        {"event": "tier-cycle", "unix": 124., "host": "test-x86", "tiers": []},
    ]


def test_ewb_parity_ignores_only_top_level_emission_time(ewb_evidence, ewb_tier_rows):
    ewb_evidence["tier_events"] = ewb_tier_rows
    ewb_evidence["tier_cycles"] = [ewb_tier_rows[-1]]
    other = copy.deepcopy(ewb_evidence)
    for row in other["tier_events"]:
        row["unix"] += 100
    assert baseline.ewb_compare(ewb_evidence, other)["equal"]
    other["tier_events"][1]["commitment"]["until_unix"] += 1
    assert not baseline.ewb_compare(ewb_evidence, other)["equal"]


@pytest.mark.parametrize("legacy", [False, True])
def test_ewb_parity_rejects_summary_only_and_legacy_captures(ewb_evidence, legacy):
    summary = {"event": "tier-cycle", "unix": 123., "tiers": []}
    ewb_evidence["tier_cycles"] = [summary]
    ewb_evidence["tier_events"] = [summary]
    if legacy:
        del ewb_evidence["tier_events"]
    result = baseline.ewb_compare(ewb_evidence, ewb_evidence)
    assert not result["equal"] and result["unavailable"]
    assert not baseline.ewb_compare({"tier_cycles": []}, {"tier_cycles": []})["equal"]


@pytest.mark.parametrize("problem", ["limits_hit", "errors"])
def test_ewb_parity_rejects_incomplete_captures(ewb_evidence, ewb_tier_rows, problem):
    ewb_evidence["tier_cycles"] = [ewb_tier_rows[-1]]
    ewb_evidence["tier_events"] = ewb_tier_rows
    other = copy.deepcopy(ewb_evidence)
    other[problem] = ["lost observation"]
    for left, right in ((ewb_evidence, other), (other, ewb_evidence)):
        result = baseline.ewb_compare(left, right)
        assert not result["equal"] and result["unavailable"]


@pytest.fixture
def ewb_capture_args(tmp_path):
    queue = tmp_path / "queue"
    for namespace in baseline.EWB_NAMESPACES:
        (queue / namespace).mkdir(parents=True)
    return argparse.Namespace(queue=queue, role=[f"tier={os.getpid()}"], denials=[],
        tier_log=None, phase="steady", duration_s=.12, sample_s=.1, load_note="fixture",
        source_revision="fixture", tree_size=[], x86_tag=["x86"], x86_host=[],
        max_entries=2, max_keys=4, max_records=40, max_bytes=4 * 1024 * 1024)


def test_ewb_capture_is_bounded_and_never_lists_terminal_trees(ewb_capture_args, monkeypatch):
    args = ewb_capture_args
    for index in range(5):
        key = f"{index:064x}"
        (args.queue / "ready" / f"{key}.json").write_text(json.dumps({
            "action_key": key, "published_unix": 120, "tags": ["x86"]}))
    original = os.scandir
    listed = []
    def bounded_scandir(path):
        listed.append(Path(path).name)
        assert Path(path).name in ("ready", "claimed")
        return original(path)
    with monkeypatch.context() as patch:
        patch.setattr(os, "scandir", bounded_scandir)
        raw = baseline.EventWaitCapture(args).run()
    assert listed == ["ready", "claimed"]
    assert raw["startup_queue"]["ready"] == {"entries": 2, "complete": False}
    assert "startup_entries:ready" in raw["limits_hit"]
    assert raw["roles"]["tier"]["initial"]["cpu_ticks"] >= 0
    assert baseline.ewb_analyze(raw)["roles"]["tier"]["samples"] >= 2


def test_ewb_capture_follows_completed_attempts_without_cross_epoch_pairing(ewb_capture_args):
    args = ewb_capture_args
    key, published = "a" * 64, 120.
    digest = baseline.canonical_sha256({"action_key": key, "published_unix": published})
    target = args.queue / "attempts" / key / digest / "00000001.json"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps({"action_key": key, "published_unix": published + 1,
                                  "claimed_unix": 130., "attempt": 1}))
    (args.queue / "ready" / f"{key}.json").write_text(json.dumps({
        "action_key": key, "published_unix": published, "attempts": 1,
        "attempt_history": [{"attempt": 1}]}))
    raw = baseline.EventWaitCapture(args).run()
    assert not any(row["namespace"] == "attempts" for row in raw["records"])


def test_ewb_capture_observes_atomic_publish_after_bootstrap(ewb_capture_args, monkeypatch):
    capture = baseline.EventWaitCapture(ewb_capture_args)
    original = capture.sample
    calls = []
    def publish_at_sample():
        original()
        if not calls:
            key = "f" * 64
            temporary = ewb_capture_args.queue / "ready" / "publishing.tmp"
            temporary.write_text(json.dumps({"action_key": key, "published_unix": 120.}))
            temporary.replace(temporary.with_name(f"{key}.json"))
        calls.append(True)
    monkeypatch.setattr(capture, "sample", publish_at_sample)
    raw = capture.run()
    assert raw["startup_queue"]["ready"]["entries"] == 0
    assert raw["selected_keys"] == 1
    assert raw["records"][0]["body"]["action_key"] == "f" * 64


@pytest.fixture
def ewb_capture_log(ewb_capture_args, tmp_path):
    def capture_rows(name, rows, suffix=""):
        args = copy.copy(ewb_capture_args)
        args.tier_log = tmp_path / f"{name}.jsonl"
        args.tier_log.write_text("")
        capture = baseline.EventWaitCapture(args)
        with args.tier_log.open("a") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")
            handle.write(suffix)
        return capture.run()
    return capture_rows


@pytest.mark.parametrize("index,field,value", [
    (0, "reason", "insufficient-tokens"), (0, "permanent", True), (0, "need_gib", 9.),
    (1, "expected_landing_unix", 141.), (1, "tier_id", "stage:other"),
    (2, "mover", "f" * 64), (2, "tokens_released", 7),
    (3, "moved_gib", 7.), (4, "mover", "f" * 64),
    (5, "tiers", [{"tier_id": "stage:test-x86", "free_gib": 7.}]),
])
def test_ewb_capture_compares_flat_runtime_decisions(ewb_capture_log, ewb_tier_rows, index, field, value):
    left = ewb_capture_log("left", ewb_tier_rows)
    changed = copy.deepcopy(ewb_tier_rows)
    changed[index][field] = value
    right = ewb_capture_log("right", changed)
    assert left["tier_events"] == ewb_tier_rows
    assert right["tier_events"] == changed
    assert left["tier_cycles"] == [ewb_tier_rows[-1]]
    result = baseline.ewb_compare(left, right)
    assert not result["equal"] and not result["unavailable"]
    assert result["left_events"] == result["right_events"] == len(ewb_tier_rows)


def test_ewb_capture_compares_order_duplicates_and_future_events(ewb_capture_log, ewb_tier_rows):
    # No consumer or known event-name allowlist is required for tier-level events.
    rows = ewb_tier_rows + [{"event": "future-policy-result", "unix": 125., "need_gib": 8.}]
    left = ewb_capture_log("left", rows)
    same = ewb_capture_log("same", [{**row, "unix": 999.} for row in rows])
    assert left["tier_events"] == rows
    assert baseline.ewb_compare(left, same)["equal"]
    variants = [rows[:-1], rows + [rows[-1]], list(reversed(rows))]
    # Keep event order relative to summaries, not merely order within each kind.
    variants.append([rows[-2]] + rows[:-2] + [rows[-1]])
    for index, changed in enumerate(variants):
        right = ewb_capture_log(f"variant-{index}", changed)
        assert not baseline.ewb_compare(left, right)["equal"]


def test_ewb_tier_events_share_the_queue_record_bound(ewb_capture_log, ewb_capture_args, ewb_tier_rows):
    ewb_capture_args.max_records = 3
    key = "a" * 64
    (ewb_capture_args.queue / "ready" / f"{key}.json").write_text(json.dumps({
        "action_key": key, "published_unix": 120.}))
    raw = ewb_capture_log("bounded", [ewb_tier_rows[0], ewb_tier_rows[-1], ewb_tier_rows[1]])
    assert len(raw["records"]) == 1 and len(raw["tier_events"]) == 2
    assert "max_records" in raw["limits_hit"]
    assert not baseline.ewb_compare(raw, raw)["equal"]


@pytest.mark.parametrize("suffix,limit", [
    ('{"event":"window-gated"}', "tier_log_incomplete"),
    ('{"event":broken}\n', "invalid_tier_log_json"),
    ('[]\n', "invalid_tier_log_event"),
    ('{"event":null}\n', "invalid_tier_log_event"),
])
def test_ewb_incomplete_or_invalid_tier_stream_cannot_prove_parity(ewb_capture_log, ewb_tier_rows, suffix, limit):
    raw = ewb_capture_log("invalid", ewb_tier_rows, suffix)
    assert raw["tier_events"] == ewb_tier_rows
    assert limit in raw["limits_hit"]
    assert not baseline.ewb_compare(raw, raw)["equal"]


def test_ewb_tier_stream_rejects_oversized_complete_line(ewb_capture_log, ewb_tier_rows):
    suffix = json.dumps({"event": "window-gated", "reason": "x" * 300000}) + "\n"
    raw = ewb_capture_log("oversized", ewb_tier_rows, suffix)
    assert raw["tier_events"] == ewb_tier_rows
    assert "tier_log_line" in raw["limits_hit"]
    assert not baseline.ewb_compare(raw, raw)["equal"]


def test_ewb_unread_tier_backlog_cannot_prove_parity(ewb_capture_args, ewb_capture_log, ewb_tier_rows):
    ewb_capture_args.duration_s = .001
    rows = ewb_tier_rows + [{"event": "window-gated", "reason": "x" * 200000}] * 5
    raw = ewb_capture_log("backlog", rows)
    assert len(raw["tier_events"]) < len(rows)
    assert "tier_log_incomplete" in raw["limits_hit"]
    assert not baseline.ewb_compare(raw, raw)["equal"]


def test_ewb_compare_cli_uses_captured_event_stream(ewb_capture_log, ewb_tier_rows, tmp_path, monkeypatch, capsys):
    left = ewb_capture_log("left", ewb_tier_rows)
    changed = copy.deepcopy(ewb_tier_rows)
    changed[0]["reason"] = "insufficient-tokens"
    right = ewb_capture_log("right", changed)
    paths = [tmp_path / "left.json", tmp_path / "right.json"]
    for path, raw in zip(paths, (left, right)):
        path.write_text(json.dumps(raw))
    monkeypatch.setattr(sys, "argv", ["event_wait_baseline.py", "compare", *map(str, paths)])
    baseline.ewb_cli()
    result = json.loads(capsys.readouterr().out)
    assert not result["equal"] and not result["unavailable"]


def test_ewb_tier_stream_carries_partial_lines_between_samples(ewb_capture_args, tmp_path, ewb_tier_rows):
    args = ewb_capture_args
    args.tier_log = tmp_path / "tier.jsonl"
    args.tier_log.write_text('{"event":"old-history"}\n')
    capture = baseline.EventWaitCapture(args)
    line = json.dumps(ewb_tier_rows[0])
    with args.tier_log.open("a") as handle:
        handle.write(line[:23])
    capture.tier_sample()
    assert capture.raw["tier_events"] == []
    with args.tier_log.open("a") as handle:
        handle.write(line[23:] + "\n" + json.dumps(ewb_tier_rows[-1]) + "\n")
    raw = capture.run()
    assert raw["tier_events"] == [ewb_tier_rows[0], ewb_tier_rows[-1]]
    assert not raw["limits_hit"]
    assert baseline.ewb_compare(raw, raw)["equal"]


def test_ewb_tier_stream_rotation_is_not_parity_evidence(ewb_capture_args, tmp_path, ewb_tier_rows):
    args = ewb_capture_args
    args.tier_log = tmp_path / "tier.jsonl"
    args.tier_log.write_text("")
    capture = baseline.EventWaitCapture(args)
    replacement = tmp_path / "replacement.jsonl"
    replacement.write_text("".join(json.dumps(row) + "\n" for row in ewb_tier_rows))
    replacement.replace(args.tier_log)
    raw = capture.run()
    assert raw["tier_events"] == ewb_tier_rows
    assert "tier_log_rotated" in raw["limits_hit"]
    assert not baseline.ewb_compare(raw, raw)["equal"]


def test_ewb_nonregular_and_oversized_records_are_bounded(ewb_capture_args, tmp_path):
    capture = baseline.EventWaitCapture(ewb_capture_args)
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    assert capture.read(fifo) is None
    large = tmp_path / "large"
    large.write_bytes(b"x" * 33)
    assert capture.read(large, 32) is None
    capture.selector.close()
    os.close(capture.fd)


def test_ewb_unavailable_ptrace_is_explicit(monkeypatch):
    def missing_strace(*args, **kwargs):
        raise FileNotFoundError("strace is not installed")
    monkeypatch.setattr(baseline.subprocess, "Popen", missing_strace)
    result = baseline.ewb_trace(1, .1)
    assert "not installed" in result["unavailable"]
    assert result["syscalls_per_min"] is None


def test_ewb_2067_reused_pid_cannot_become_the_original_role(ewb_evidence):
    ewb_evidence["roles"] = {"worker": {"pid": 101, "argv": ["worker_loop.py"],
                                             "initial": {"start_ticks": 7}}}
    for stamp, start, value in [(1, 7, 0), (11, 7, 100), (21, 8, 0), (31, 8, 900)]:
        ewb_evidence["samples"].append({"monotonic": stamp, "roles": {"worker": {
            "start_ticks": start, "cpu_ticks": value, "read_bytes": value,
            "rchar": value, "syscr": value, "syscw": value}}})
    result = baseline.ewb_analyze(ewb_evidence)["roles"]["worker"]
    assert result["covered_s"] == 10
    assert result["deltas"]["read_bytes"] == 100
    assert result["io_syscalls_per_min"] == 1200


@pytest.mark.parametrize("unreadable", ["cmdline", "stat", "io"])
def test_ewb_2067_initially_unavailable_role_retains_evidence(
        ewb_capture_args, monkeypatch, unreadable):
    capture = baseline.EventWaitCapture(ewb_capture_args)
    original_read = capture.read
    def unavailable_role_file(path, cap=262144):
        if path == Path("/proc") / str(os.getpid()) / unreadable:
            return None
        return original_read(path, cap)
    monkeypatch.setattr(capture, "read", unavailable_role_file)
    try:
        raw = capture.run()
    finally:
        # Old implementations abort before run() closes these descriptors.
        capture.selector.close()
        try:
            os.close(capture.fd)
        except OSError:
            pass
    assert raw["roles"]["tier"]["pid"] == os.getpid()
    role = raw["roles"]["tier"]
    assert role.get("error") or role.get("initial", {}).get("error") or raw["errors"]
    assert raw["ended_unix"] >= raw["started_unix"]
    result = baseline.ewb_analyze(raw)["roles"]["tier"]
    assert result["total_syscalls_per_min"] is None
    if unreadable != "cmdline":
        assert result["covered_s"] == 0
        assert result["io_syscalls_per_min"] is None


def test_ewb_2067_counter_read_checks_identity_on_both_sides(ewb_capture_args, monkeypatch):
    capture = baseline.EventWaitCapture(ewb_capture_args)
    original_read = capture.read
    starts = iter((7, 8))
    def changing_identity(path, cap=262144):
        if path == Path("/proc") / str(os.getpid()) / "stat":
            fields = ["0"] * 30
            fields[0], fields[11], fields[19] = "S", "100", str(next(starts, 8))
            return (f"{os.getpid()} (synthetic role) " + " ".join(fields)).encode()
        return original_read(path, cap)
    monkeypatch.setattr(capture, "read", changing_identity)
    try:
        sample = capture.role_sample(os.getpid())
    finally:
        capture.selector.close()
        os.close(capture.fd)
    assert sample.get("error"), "counters spanning PID reuse have no valid start identity"


def test_ewb_2067_trace_output_budget_is_not_a_complete_syscall_sample(tmp_path, monkeypatch):
    # This is an executable synthetic tracer, not a ptrace attachment. Exercise
    # real pipe/output handling with no live-host or tracing privileges.
    tracer = tmp_path / "strace"
    tracer.write_text(f"#!{sys.executable}\n"
                      "import os\n"
                      "os.write(1, b'x' * 131072)\n"
                      "os.write(2, b'y' * 131072)\n"
                      "os.write(2, b'\\n100.00 0.001 1 500 total\\n')\n")
    tracer.chmod(0o755)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ.get("PATH", ""))
    result = baseline.ewb_trace(os.getpid(), 1)
    retained = sum(len(result.get(stream, "").encode()) for stream in ("stdout", "stderr"))
    assert retained <= 65536, "trace evidence needs a combined retained-output budget"
    assert "trace_output_bytes" in result.get("limits_hit", [])
    assert result["syscalls_per_min"] is None
