"""Retained #1182 review boundaries: TESTS-ONLY witnesses/containment diagnostics.

Reuse the frozen 84-case real queue/core/CAS fixtures, without editing them.
The 1-KiB recorder executions below prove execution/result semantics only;
placement profiles and hardware samples remain labelled synthetic observations.
No physical profile/spill/GPU/deployment qualification or measured speed claim.
Parent runs these solely inside its admitted CPU-only PB action. No recursive
submission, identity/receipt/fit/decision/acquisition/claim or loader mocks.
"""
from __future__ import annotations

import hashlib
import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

import pytest
from test_local_scratch_measured_placement import (
    CAPACITY,
    DEVICES,
    FAST,
    PROFILES,
    REPO,
    SLOW,
    ScratchFleet,
    _profile_reader,
    _record,
    _recorder_action,
    _successful_profile_execution,
    adaptive_gpu,
    ls,
    pb,
    pool,
    worker_loop,
)


@pytest.fixture
def fleet(tmp_path, monkeypatch):
    return ScratchFleet(tmp_path, monkeypatch)


def _deep_json_bytes():
    # Build raw bytes, not recursively nested Python objects or json.dumps.
    # Parent ran this moderate-depth input safely: it is a containment
    # control, NOT proof that the C JSON decoder must raise RecursionError.
    # Keep its original bytes; do not infer C-stack limits from Python's limit.
    depth = sys.getrecursionlimit() + 100
    raw = b"[" * depth + b"0" + b"]" * depth + b"\n"
    assert 0 < len(raw) < 65536, "fixture nesting cannot fit the actual metadata bound"
    return raw


def _max_metadata_depth_json_bytes():
    raw = b"[" * 32766 + b"0" + b"]" * 32766 + b"\n"
    assert len(raw) == 65534 < 65536
    return raw


def _record_actual_decoder_precondition(case, raw):
    # Invoke the real configured decoder in the admitted test only. Do not
    # change recursion/stack limits, patch a parser or serialize its deep value.
    try:
        decoded = pb._decode_strict_json(raw, where="retained depth setup")
    except RecursionError:
        outcome, category, recursion = "RecursionError", "exception-precondition-met", True
    except ValueError as exc:
        outcome, category, recursion = type(exc).__name__, "finding-not-reproduced", False
    else:
        outcome, category, recursion = type(decoded).__name__, "finding-not-reproduced", False
    print("scratch-json-decoder-boundary: " + json.dumps({
        "case": case, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
        "python": sys.version, "executable": sys.executable, "outcome": outcome,
        "classification": category, "recursion_error_precondition": recursion,
    }, sort_keys=True))


def _check_deep_result_containment(fleet, monkeypatch, raw, *, decoder_probe=False):
    base = _recorder_action(fleet)
    _, good_raw = _successful_profile_execution(fleet, monkeypatch, base)
    good_body = json.loads(good_raw)
    assert good_body["completed"] is True and good_body["write_bytes"] == 1024
    good_reader = _profile_reader(fleet, base, pb.canonical_sha256(good_body))
    good_detail = worker_loop.scratch_observed_detail(None, good_reader)
    assert good_detail[PROFILES] and fleet.scratch in good_detail[DEVICES]
    good_ref = deepcopy(json.loads(good_reader.config_path.read_bytes())["profiles"][0])

    body = deepcopy(base)
    body.pop("action_key")
    command = base["params"]["command"]
    # Same genuine successful malformed-result primitive as the 84-case file:
    # execute the real copied recorder, then intentionally write bad metadata
    # before core publishes the declared file. No fake execution attestation.
    driver = ("import runpy; from pathlib import Path; "
              f"m=runpy.run_path({ls.RECORDER!r}); "
              f"assert m['main']({command[4:]!r}) == 0; "
              f"Path({ls.PROFILE_RESULT!r}).write_bytes({raw!r})")
    body["task"]["argv"] = [sys.executable, "-I", "-S", "-c", driver]
    action = cast(dict[str, Any], pb.seal_action(body))
    receipt, actual_raw = _successful_profile_execution(fleet, monkeypatch, action)
    # Actual successful receipt/result setup MUST precede the desired polling
    # assertion; a recorder/setup error is never the intended retained RED.
    assert receipt["action_key"] == action["action_key"]
    assert receipt["result"]["bytes"] == len(raw) < 65536
    assert receipt["result"]["sha256"] == hashlib.sha256(raw).hexdigest()
    assert actual_raw == raw
    if decoder_probe:
        # Successful real receipt/bytes precede the actual decoder setup probe.
        _record_actual_decoder_precondition("max-result", actual_raw)
    digest = hashlib.sha256(raw[:-1]).hexdigest()  # actual BODY, not BODY+LF
    reader = _profile_reader(fleet, action, digest)
    config = json.loads(reader.config_path.read_bytes())
    config["profiles"].insert(0, good_ref)
    reader.config_path.write_text(json.dumps(config))
    assert reader.config_path.stat().st_size < 65536
    detail = worker_loop.scratch_observed_detail(None, reader)
    assert detail[PROFILES] == [] and fleet.scratch not in detail[DEVICES]


def test_deep_successful_result_invalidates_older_good_root_without_poll_recursion(fleet, monkeypatch):
    _check_deep_result_containment(fleet, monkeypatch, _deep_json_bytes())


def test_max_depth_successful_result_contains_actual_decoder_boundary(fleet, monkeypatch):
    _check_deep_result_containment(fleet, monkeypatch, _max_metadata_depth_json_bytes(),
                                   decoder_probe=True)


def _check_deep_config_containment(fleet, raw, *, decoder_probe=False):
    config = fleet.root / "deep-profile-config.json"
    config.write_bytes(raw)
    assert config.read_bytes() == raw and config.stat().st_size < 65536
    if decoder_probe:
        _record_actual_decoder_precondition("max-config", config.read_bytes())
    reader = ls.ProfileInputs(config, source_root=REPO,
                              checkout_root=fleet.root / "verification",
                              producer_python=sys.executable)
    detail = worker_loop.scratch_observed_detail(None, reader)
    assert detail[PROFILES] == [] and detail[DEVICES] == {}


def test_deep_config_is_whole_input_refusal_without_poll_recursion(fleet):
    _check_deep_config_containment(fleet, _deep_json_bytes())


def test_max_depth_config_contains_actual_decoder_boundary(fleet):
    _check_deep_config_containment(fleet, _max_metadata_depth_json_bytes(), decoder_probe=True)


@pytest.mark.parametrize("mode", ["peer", "own"], ids=["peer-devices", "own-devices"])
def test_real_announced_truthy_device_list_is_excluded_or_unspent(fleet, mode):
    fleet.publish()
    path = fleet.queue.root / pool.WORKERS / f"{FAST}.json"
    original = _record(path)
    detail = deepcopy(original["observed_detail"])
    detail[DEVICES] = ["malformed"]
    # Actual announce preserves nested values. A top-level detail list would
    # instead be coerced by dict(...) and is NOT this reachable fault seam.
    fleet.queue.announce(host=FAST, tags=original["tags"], has_gpu=original["has_gpu"],
                         capacity=original["capacity"], cpu_tiers=original["cpu_tiers"],
                         observed_capacity=original["observed_capacity"], observed_detail=detail)
    assert _record(path)["observed_detail"][DEVICES] == ["malformed"]
    if mode == "peer":
        claimed = fleet.claim(SLOW)
        assert claimed is not None, "malformed peer device observation aborted actual local admission"
        _, candidates = fleet.receipt(claimed, SLOW)
        assert candidates[FAST]["fit"] is False
        assert isinstance(candidates[FAST]["exclusion"], str) and candidates[FAST]["exclusion"]
        fleet.queue.finish(fleet.key, status="executed", detail={})
        assert fleet.queue.ledger(SLOW).available() == CAPACITY
    else:
        assert fleet.claim(FAST) is None
        fleet.ready_unspent()
        assert "scratch" in fleet.denial(FAST)["reason"]


@pytest.mark.parametrize("unknown_field", ["complete", "attributed"])
def test_real_unknown_gpu_refusal_cannot_authorize_measured_pass_on(fleet, monkeypatch, unknown_field):
    fleet.publish()
    current = _record(fleet.queue.item_path(pool.READY, fleet.key))
    actual_sample = fleet.gpu_sample
    invalid = [True]

    def gpu_observation():
        sample = actual_sample()
        if invalid[0] and fleet.host == FAST:
            sample[unknown_field] = False
        return sample

    monkeypatch.setattr(adaptive_gpu.Controller, "sample", lambda _: gpu_observation())
    assert fleet.claim(FAST) is None
    fleet.mirror_real_denial(FAST, "adaptive_gpu_refused")
    denial = fleet.denial(FAST)
    decision = denial["evidence"]["decision"]
    assert decision["reason"] == "sample_invalid_or_stale"
    assert decision["sample"][unknown_field] is False
    assert denial["action_key"] == fleet.key and denial["host"] == FAST
    assert denial["published_unix"] == current["published_unix"]
    assert denial["attempts"] == current["attempts"] == 0
    assert 0 <= denial["denied_unix"] - decision["sample"]["sampled_unix"] <= adaptive_gpu.MAX_SAMPLE_AGE_S
    assert not fleet.queue.ledger(FAST).held_keys()
    # The actual FAST controller refusal already records a shared fairness pass
    # through the unchanged ordinary admission path. The later measured yield
    # must preserve it, not require its absence or spend another pass.
    passes = fleet.queue.passes_path(fleet.key)
    assert passes.is_file()
    before_passes = passes.read_bytes()
    # Restore low-level evidence only. No new FAST evaluation/offer or edited
    # refusal: its genuine old diagnostic is now the unknown-authority trap.
    invalid[0] = False
    assert gpu_observation()["complete"] is True and gpu_observation()["attributed"] is True
    claimed = fleet.claim(SLOW)
    assert claimed is None, "unknown GPU observation was treated as measured policy pass-on"
    assert _record(fleet.queue.item_path(pool.READY, fleet.key)) == current
    assert not fleet.queue.item_path(pool.CLAIMED, fleet.key).exists()
    assert not fleet.queue.lease_path(fleet.key).exists()
    assert passes.read_bytes() == before_passes
    for host in (SLOW, FAST):
        assert not fleet.queue.ledger(host).held_keys()
        assert fleet.queue.ledger(host).available() == CAPACITY
    assert fleet.denial(FAST) == denial  # SLOW inspection did not reevaluate FAST
    fleet.receipt(fleet.claim(FAST), FAST)
    fleet.queue.finish(fleet.key, status="executed", detail={})
    assert fleet.queue.ledger(FAST).available() == CAPACITY


def test_peer_original_price_expiry_during_real_view_read_ends_yield(fleet, monkeypatch):
    max_age = 1.0                            # explicit unchanged fixture input
    original = fleet.clock[0]
    fleet.profiles[FAST] = [fleet.profile(FAST, measured_unix=original - .9)]
    fleet.announce(FAST)
    fleet.publish(max_age=max_age)
    peer_map = fleet.queue.ledger(FAST).base / "cpu-map.json"
    assert peer_map.is_file() and fleet.queue.ledger(FAST).available() == CAPACITY
    read_json = pool._read_json
    reads = []

    def delayed_peer_ledger_observation(path, *args, **kwargs):
        value = read_json(path, *args, **kwargs)
        if Path(path) == peer_map and not reads:
            assert value == fleet.tiers       # actual peer view/map observation
            assert 0 <= fleet.clock[0] - fleet.profiles[FAST][0]["measured_unix"] <= max_age
            fleet.clock[0] += .2              # elapsed real shared-read seam only
            reads.append(str(path))
            assert fleet.clock[0] - fleet.profiles[FAST][0]["measured_unix"] > max_age
            assert 0 <= fleet.clock[0] - fleet.profiles[SLOW][0]["measured_unix"] <= max_age
        return value

    monkeypatch.setattr(pool, "_read_json", delayed_peer_ledger_observation)
    claimed = fleet.claim(SLOW)
    assert reads == [str(peer_map)], "fixture did not reach the genuine peer view read"
    assert fleet.declaration["max_profile_age_s"] == max_age
    assert fleet.profiles[FAST][0]["measured_unix"] == original - .9
    assert claimed is not None, "known-expired peer price still induced measured yield"
    _, candidates = fleet.receipt(claimed, SLOW)
    assert candidates[FAST]["fit"] is False and candidates[FAST]["exclusion"]
    fleet.queue.finish(fleet.key, status="executed", detail={})
    assert fleet.queue.ledger(SLOW).available() == CAPACITY
