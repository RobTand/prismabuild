"""Slice 0 evidence cannot turn missing history into fast claim/drain latency."""
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
            "roles": {}, "samples": [], "records": [], "tier_cycles": [],
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


def test_ewb_parity_preserves_decisions_accounting_and_nested_times(ewb_evidence):
    ewb_evidence["tier_cycles"] = [{"event": "tier-cycle", "unix": 123,
                                   "decision": {"bytes": 7, "until_unix": 140}}]
    other = copy.deepcopy(ewb_evidence)
    other["tier_cycles"][0]["unix"] = 200
    assert baseline.ewb_compare(ewb_evidence, other)["equal"]
    other["tier_cycles"][0]["decision"]["until_unix"] = 141
    assert not baseline.ewb_compare(ewb_evidence, other)["equal"]
    assert not baseline.ewb_compare({"tier_cycles": []}, {"tier_cycles": []})["equal"]


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
