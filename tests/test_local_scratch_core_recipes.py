"""#1182 Core recipe/three-source dependency transition controls, not qualification.

Run only in the parent's admitted PB action. Tiny real recorder executions use
existing queue/Core/CAS fixture helpers. Source changes are confined to private
copies, never the tracked checkout or the executing worker's Core. Independent
hashlib/JSON recipes here are expected-byte oracles, not production recipes.
Pbrun's action code closure contains only its identity stamp; the three producer
sources are proved from the sealed checkout snapshot through the real materializer.
"""
from __future__ import annotations

import hashlib
import json
import runpy
import sys
from copy import deepcopy
from pathlib import Path

import pytest
from test_local_scratch_measured_placement import (
    DEVICES,
    FAST,
    PROFILES,
    REPO,
    ScratchFleet,
    _profile_reader,
    _recorder_action,
    _successful_profile_execution,
    ls,
    materialize,
    pb,
    pbrun,
    worker_loop,
)


@pytest.fixture
def fleet(tmp_path, monkeypatch):
    return ScratchFleet(tmp_path, monkeypatch)


class IntegerSubclass(int):
    pass


class FloatSubclass(float):
    pass


@pytest.mark.parametrize("length", [1, 256, 1024, 65536])
def test_core_local_scratch_profile_block_matches_independent_shake_bytes(length):
    expected = hashlib.shake_256(b"prismabuild.scratch-profile.block.v1").digest(length)
    assert pb._local_scratch_profile_block(length) == expected
    assert len(expected) == length


@pytest.mark.parametrize("length", [True, False, 0, -1, 1.0, "256", None, IntegerSubclass(1)])
def test_core_local_scratch_profile_block_rejects_nonpositive_or_inexact_integer(length):
    with pytest.raises(ValueError, match="scratch profile block bytes"):
        pb._local_scratch_profile_block(length)


@pytest.mark.parametrize("raw", [b"", b"\x00\xff\n", "source café λ\n".encode(), b"body\n\n"])
def test_core_raw_source_digest_matches_independent_exact_byte_hash(raw):
    assert pb.raw_sha256(raw) == hashlib.sha256(raw).hexdigest()


@pytest.mark.parametrize("body", [
    {"unicode": "café λ", "float": 1.0, "integer": 1},
    {"tiny": 5e-324, "negative_zero": -0.0, "nested": [None, True, "\n"]},
])
def test_core_canonical_profile_file_matches_independent_unicode_float_body_plus_lf(body):
    expected_body = json.dumps(body, sort_keys=True, separators=(",", ":"),
                               ensure_ascii=False, allow_nan=False).encode("utf-8")
    assert pb._canonical_file_bytes(body) == expected_body + b"\n"
    assert pb.canonical_sha256(body) == hashlib.sha256(expected_body).hexdigest()
    assert pb.raw_sha256(expected_body + b"\n") != pb.canonical_sha256(body)


@pytest.mark.parametrize("value, accepted", [
    (True, False), (False, False), (IntegerSubclass(1), False),
    (FloatSubclass(1.0), False), (None, False), ("1", False), ([], False),
    ({}, False), (complex(1, 0), False), (0, False), (0.0, False),
    (-1, False), (-1.0, False), (float("nan"), False),
    (float("inf"), False), (float("-inf"), False),
    (1, True), (1.0, True), (5e-324, True), (10**308, True), (10**400, False),
], ids=["true", "false", "int-subclass", "float-subclass", "missing", "text",
        "list", "dict", "complex", "int-zero", "float-zero", "int-negative",
        "float-negative", "nan", "infinity", "negative-infinity", "int-positive",
        "float-positive", "subnormal", "large-finite-int", "overflowing-int"])
def test_local_scratch_positive_finite_adapter_delegates_core_contract(value, accepted):
    assert ls._core_positive_finite is pb._positive_finite
    assert ls._is_positive_finite(value) is accepted
    if accepted:
        assert pb._positive_finite(value, where="independent predicate control") > 0
    else:
        with pytest.raises((ValueError, OverflowError)):
            pb._positive_finite(value, where="independent predicate control")


@pytest.mark.parametrize("age", [60, 60.0])
def test_local_scratch_core_adapter_preserves_accepted_declaration_and_profile_types(fleet, age):
    _, action, _ = fleet.build(max_age=age)
    intent = ls.io_intent(action["environment"]["variables"])
    assert intent is not None
    assert type(intent["declaration"]["max_profile_age_s"]) is type(age)
    stamp = int(fleet.clock[0]) if type(age) is int else float(fleet.clock[0])
    profile = fleet.profile(FAST, measured_unix=stamp)
    offer = {"host": FAST, "observed_detail": {
        PROFILES: [profile], DEVICES: {fleet.scratch: deepcopy(fleet.devices[FAST])}}}
    result = ls.profile_cost(offer, intent, now=fleet.clock[0])
    assert type(result["measured_unix"]) is type(stamp)
    assert profile["measured_unix"] == stamp and result["io_seconds"] > 0


def test_local_scratch_bootstrap_binds_one_fixed_sibling_core_without_package_imports():
    assert ls._local_scratch_profile_block is pb._local_scratch_profile_block
    assert ls._canonical_file_bytes is pb._canonical_file_bytes
    isolated = runpy.run_path(str(REPO / "src/prismabuild/local_scratch.py"))
    block = isolated["_local_scratch_profile_block"]
    canonical = isolated["_canonical_file_bytes"]
    predicate = isolated["_core_positive_finite"]
    assert block.__globals__ is canonical.__globals__ is predicate.__globals__
    core_globals = block.__globals__
    assert Path(core_globals["__file__"]) == REPO / "src/prismabuild/core.py"
    assert core_globals["__package__"] == ""
    owner = core_globals["digest_primitives"]
    assert Path(owner.__file__) == REPO / "src/prismabuild/digest_primitives.py"
    body = {"unicode": "λ", "value": 1.0}
    assert owner._canonical_file_bytes(body) == pb._canonical_file_bytes(body)
    assert owner.canonical_sha256(body) == pb.canonical_sha256(body)
    assert block(256) == pb._local_scratch_profile_block(256)
    assert canonical(body) == pb._canonical_file_bytes(body)


def _assert_materialized_producer_snapshot(fleet, action, *, expected_source_root=REPO):
    assert ls.PRODUCER_FILES == (ls.RECORDER, "src/prismabuild/local_scratch.py",
                                 "src/prismabuild/core.py",
                                 "src/prismabuild/digest_primitives.py")
    stamp_files = action["code_closure"]["files"]
    assert len(stamp_files) == 1
    assert Path(stamp_files[0]["path"]).name.startswith(pb.PBRUN_STAMP_PREFIX)
    with materialize._execution_checkout(
            {"action_key": action["action_key"], "cas_root": str(fleet.cas.root),
             "checkout_snapshot": action["params"]["checkout_snapshot"]},
            local_checkout_root=fleet.root / "snapshot-proof") as checkout:
        pb._verify_pbrun_checkout_identity(action, checkout)
        pb.verify_code_closure(action["code_closure"], checkout)
        sources = {}
        for name in ls.PRODUCER_FILES:
            actual = pb._read_regular_file_nofollow(
                checkout / name, where="materialized producer snapshot control")
            expected = pb._read_regular_file_nofollow(
                expected_source_root / name, where="expected producer source control")
            assert actual == expected
            assert hashlib.sha256(actual).hexdigest() == hashlib.sha256(expected).hexdigest()
            sources[name] = actual
    return sources


def test_real_isolated_tiny_profile_receipt_and_loader_accept_all_four_sources(fleet, monkeypatch):
    action = _recorder_action(fleet)
    _assert_materialized_producer_snapshot(fleet, action)
    assert action["task"]["argv"][1:3] == ["-I", "-S"]
    receipt, raw = _successful_profile_execution(fleet, monkeypatch, action)
    body = json.loads(raw)
    assert body["completed"] is True and body["errors"] == []
    assert body["write_bytes"] == body["read_bytes"] == 1024
    assert raw == pb._canonical_file_bytes(body)
    assert receipt["result"]["sha256"] == hashlib.sha256(raw).hexdigest()
    assert receipt["producer"]["executable"]["path"] == sys.executable
    reader = _profile_reader(fleet, action, pb.canonical_sha256(body))
    assert reader.expected_sources is not None
    assert len(reader.expected_sources) == 4
    detail = worker_loop.scratch_observed_detail(None, reader)
    assert detail[PROFILES] == [{**body, "artifact_sha256": pb.canonical_sha256(body)}]
    assert detail[DEVICES][fleet.scratch] == fleet.actual_root_identity(fleet.scratch)
    assert len(reader.verified) == 1
    assert worker_loop.scratch_observed_detail(None, reader) == detail
    assert len(reader.verified) == 1


def _refreeze_changed_private_core(fleet, base):
    # Freeze the actual private bytes anew; do not forge closure/receipt fields.
    variables = {name: value for name, value in base["environment"]["variables"].items()
                 if name not in {ls.PROFILE_OWNER_ENV, ls.PROFILE_MARKER_ENV}}
    extra = [part for name, value in variables.items() for part in ("--env", f"{name}={value}")]
    args = pbrun.parse_args([
        "--cwd", str(fleet.work), "--transport", "pool", "--no-default-env", "--tag", "gb10",
        "--cpus", "1", "--demand", "mem_gb=4", "--timeout-s", "30", *extra,
        "--", *base["params"]["command"]])
    template = pbrun.prepare_submission(args)["template"]
    return pbrun.seal_action_from_template(template)


def test_harmless_changed_recipe_core_snapshot_executes_but_three_source_qualification_refuses(
        fleet, monkeypatch):
    base = _recorder_action(fleet)
    private_core = fleet.work / "src/prismabuild/core.py"
    private_core.write_bytes(private_core.read_bytes() + b"\n# private dependency transition control\n")
    changed = _refreeze_changed_private_core(fleet, base)
    assert changed["action_key"] != base["action_key"]
    sources = _assert_materialized_producer_snapshot(fleet, changed,
                                                   expected_source_root=fleet.work)
    materialized_core = sources["src/prismabuild/core.py"]
    assert materialized_core == private_core.read_bytes()
    installed_core = pb._read_regular_file_nofollow(
        REPO / "src/prismabuild/core.py", where="installed recipe Core control")
    assert materialized_core != installed_core
    assert hashlib.sha256(materialized_core).hexdigest() != hashlib.sha256(installed_core).hexdigest()
    receipt, raw = _successful_profile_execution(fleet, monkeypatch, changed)
    body = json.loads(raw)
    assert body["completed"] is True and body["write_bytes"] == body["read_bytes"] == 1024
    assert receipt["action_key"] == changed["action_key"]
    assert raw == pb._canonical_file_bytes(body)
    reader = _profile_reader(fleet, changed, pb.canonical_sha256(body))
    with pytest.raises(ls.LocalScratchError, match="executed scratch producer source differs"):
        reader._load(json.loads(reader.config_path.read_bytes())["profiles"][0])
    detail = worker_loop.scratch_observed_detail(None, reader)
    assert detail[PROFILES] == [] and detail[DEVICES] == {} and not reader.verified


def test_installed_recipe_core_change_after_cached_success_invalidates_all_same_root_profiles(
        fleet, monkeypatch):
    actions, bodies, refs = [], [], []
    for observation_id in ("older", "newer"):
        action = _recorder_action(fleet, extra_env={ls.PROFILE_OBSERVATION_ENV: observation_id})
        _assert_materialized_producer_snapshot(fleet, action)
        _, raw = _successful_profile_execution(fleet, monkeypatch, action)
        body = json.loads(raw)
        assert body["completed"] is True
        setup_reader = _profile_reader(fleet, action, pb.canonical_sha256(body))
        refs.extend(json.loads(setup_reader.config_path.read_bytes())["profiles"])
        actions.append(action)
        bodies.append(body)
    assert actions[0]["action_key"] != actions[1]["action_key"]
    installed = fleet.root / "private-installed-producer"
    for name in ls.PRODUCER_FILES:
        target = installed / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((REPO / name).read_bytes())
    config = fleet.root / "all-same-root-profiles.json"
    config.write_text(json.dumps({"schema": ls.CONFIG_SCHEMA, "profiles": refs}))
    reader = ls.ProfileInputs(config, source_root=installed,
                              checkout_root=fleet.root / "private-verification",
                              producer_python=sys.executable)
    before = worker_loop.scratch_observed_detail(None, reader)
    assert before[PROFILES] == [
        {**body, "artifact_sha256": pb.canonical_sha256(body)} for body in bodies]
    assert fleet.scratch in before[DEVICES] and len(reader.verified) == 2
    cached = set(reader.verified)
    assert worker_loop.scratch_observed_detail(None, reader) == before
    changed_core = installed / "src/prismabuild/core.py"
    changed_core.write_bytes(changed_core.read_bytes() + b"\n# changed installed dependency\n")
    with pytest.raises(ls.LocalScratchError, match="installed scratch producer source changed"):
        reader._load(refs[0])
    after = worker_loop.scratch_observed_detail(None, reader)
    assert after[PROFILES] == [] and after[DEVICES] == {}
    assert reader.verified == cached  # Cached proof is retained but cannot authorize old inputs.
