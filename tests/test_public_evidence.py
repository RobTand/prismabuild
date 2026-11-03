"""Synthetic-only contract tests for the shared derived public view (#2701)."""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import traceback

import pytest


SCHEMA = "prismabuild.public_evidence.v1"
ACTION = "1" * 64
MANIFEST = "2" * 64
RESULT = "3" * 64
STDOUT = "4" * 64
STDERR = "5" * 64


def _view(value):
    __tracebackhide__ = True
    # Missing support must be an assertion failure, not a collection/import error.
    assert importlib.util.find_spec("prismabuild.public_evidence") is not None, (
        "shared public projection is not implemented"
    )
    from prismabuild.public_evidence import public_evidence_view

    return public_evidence_view(value)


def _serialized(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False).encode("ascii")


def _assert_absent(data, sentinels):
    __tracebackhide__ = True
    # Do not let pytest's value introspection echo a private value on failure.
    if any(sentinel.encode("ascii") in data for sentinel in sentinels):
        raise AssertionError("public evidence contains unapproved data")


def _assert_equal(actual, expected):
    __tracebackhide__ = True
    if actual != expected:
        raise AssertionError("public evidence does not match its approved contract")


def test_approved_fields_keep_independently_supplied_expected_values():
    source = {
        "status": "succeeded", "returncode": 0, "action_key": ACTION,
        "action_manifest_sha256": MANIFEST,
        "stdout_sha256": STDOUT, "stderr_sha256": STDERR,
        "started_at": 1790000000, "finished_at": 1790000001.25,
        "observed_at": 1790000002.5,
        "result": {"sha256": RESULT, "bytes": 4096},
    }
    expected = {
        "schema": SCHEMA,
        "evidence": {
            "action_key": ACTION, "action_manifest_sha256": MANIFEST,
            "finished_at": 1790000001.25, "observed_at": 1790000002.5,
            "result": {"bytes": 4096, "sha256": RESULT}, "returncode": 0,
            "started_at": 1790000000, "status": "succeeded",
            "stderr_sha256": STDERR, "stdout_sha256": STDOUT,
        },
    }
    _assert_equal(_view(source), expected)


def test_view_is_marked_as_derived_public_view_not_a_receipt():
    projected = _view({"schema": "prismaquant.prismabuild.cas_receipt.v3",
                       "receipt_sha256": RESULT, "action_key": ACTION})
    _assert_equal(projected, {"schema": SCHEMA, "evidence": {"action_key": ACTION}})
    assert "receipt_sha256" not in projected
    assert "receipt_sha256" not in projected["evidence"]


def test_unknown_fields_and_nested_private_mappings_are_omitted():
    source = {
        "status": "running", "future_field": {"status": "failed"},
        "claim": {"status": "failed", "action_key": RESULT},
        "control": {"result": {"sha256": RESULT, "bytes": 12}},
        "result": {"sha256": RESULT, "bytes": 12, "status": "failed",
                   "children": [{"returncode": 99}]},
    }
    _assert_equal(_view(source), {"schema": SCHEMA, "evidence": {
        "status": "running", "result": {"sha256": RESULT, "bytes": 12}}})


def test_nested_sentinels_and_launch_argv_never_reach_serialized_bytes():
    sentinels = ["synthetic-root-A", "synthetic-child-B", "synthetic-deep-C",
                 "synthetic-native-argv-D", "synthetic-result-E", "b" * 64]
    source = {
        "status": "running", "token": sentinels[0],
        "children": [{"status": "failed", "bearer": sentinels[1],
                      "children": [[{"returncode": 7, "control": {"value": sentinels[2]},
                                     "launch": {"argv": ["native", "--token", sentinels[3]]}}]],
                      "result": {"bytes": 8, "capability": sentinels[4]}}],
        "authorization": sentinels[5],
    }
    projected = _view(source)
    _assert_absent(_serialized(projected), sentinels)
    _assert_equal(projected, {"schema": SCHEMA, "evidence": {
        "status": "running", "children": [{
            "status": "failed", "children": [[{"returncode": 7}]],
            "result": {"bytes": 8}}]}})


def test_several_secret_spellings_and_cases_are_dropped_by_allow_list():
    names = ["token", "TOKEN", "bearer", "capability", "Claim", "CONTROL",
             "secret", "authorization", "accessToken", "unrecognized_future_name"]
    sentinels = [f"synthetic-spelling-{index}" for index in range(len(names))]
    source = dict(zip(names, sentinels))
    source["status"] = "ready"
    projected = _view(source)
    _assert_absent(_serialized(projected), sentinels)
    _assert_equal(projected, {"schema": SCHEMA, "evidence": {"status": "ready"}})


def test_alternate_nesting_cannot_carry_argv_diagnostics_or_claims():
    secret = "synthetic-alternate-location"
    private = {"argv": ["native", "--token", secret], "diagnostic": secret,
               "claim": {"token": secret}, "control": {"bearer": secret}}
    source = {
        "status": private, "returncode": private, "action_key": private,
        "started_at": private, "stdout_sha256": private,
        "result": {"sha256": private, "bytes": private, "children": private},
        "children": [private, [secret, "--token", private],
                     {"children": [private], "result": private},
                     {"status": "running", "renamed_diagnostic": private}],
        "renamed_payload": {"children": [{"status": "failed"}], **private},
    }
    projected = _view(source)
    _assert_absent(_serialized(projected), [secret, "--token"])
    _assert_equal(projected, {"schema": SCHEMA, "evidence": {
        "children": [{"status": "running"}]}})
    _assert_equal(_view(["native", "--token", secret]), {"schema": SCHEMA, "evidence": {}})


def test_failure_diagnostics_never_echo_sentinels(capsys, caplog):
    secret = "synthetic-diagnostic-canary"

    class Hostile:
        def __repr__(self):
            raise AssertionError(secret)

        def __str__(self):
            raise AssertionError(secret)

    source = {"status": Hostile(), "returncode": Hostile(),
              "result": {"bytes": Hostile()}, "diagnostics": Hostile()}
    _assert_equal(_view(source), {"schema": SCHEMA, "evidence": {}})
    captured = capsys.readouterr()
    _assert_absent((captured.out + captured.err + caplog.text).encode(), [secret])
    # Negative control: even the regression's deliberate failure is value-free.
    with pytest.raises(AssertionError) as caught:
        _assert_absent(secret.encode(), [secret])
    diagnostics = "".join(traceback.format_exception(type(caught.value), caught.value,
                                                     caught.value.__traceback__))
    _assert_absent(diagnostics.encode(), [secret])
    assert str(caught.value) == "public evidence contains unapproved data"


def test_view_is_pure_deterministic_and_idempotent():
    source = {"status": "failed", "returncode": -9, "children": [
        {"status": "ready"}, [{"status": "pending", "observed_at": 0}]],
        "result": {"sha256": RESULT, "bytes": 0}}
    reversed_keys = {key: source[key] for key in reversed(source)}
    first = _view(source)
    _assert_equal(_serialized(first), _serialized(_view(reversed_keys)))
    _assert_equal(_view(first), first)
    _assert_equal(_view(source), first)
    assert list(first["evidence"]) == sorted(first["evidence"])
    _assert_equal(_view(_view([source, [source]])), _view([source, [source]]))


def test_view_does_not_mutate_or_alias_its_input():
    source = {"status": "running", "children": [{"returncode": 1}],
              "result": {"sha256": RESULT, "bytes": 19},
              "recovery": {"token": "synthetic-private-recovery"}}
    before = copy.deepcopy(source)
    projected = _view(source)
    _assert_equal(source, before)
    projected["evidence"]["result"]["bytes"] = 20
    projected["evidence"]["children"][0]["returncode"] = 2
    projected["evidence"]["children"].append({"status": "ready"})
    _assert_equal(source, before)
    source["result"]["sha256"] = MANIFEST
    _assert_equal(projected["evidence"]["result"]["sha256"], RESULT)


def test_malformed_values_are_handled_safely():
    class StringSubclass(str):
        pass

    class IntSubclass(int):
        pass

    class DictSubclass(dict):
        def items(self):
            raise AssertionError("must not invoke untrusted mapping methods")

    invalid_records = [
        {"status": value} for value in ["free-form diagnostic", "RUNNING", True,
                                       StringSubclass("running"), {}, []]
    ] + [
        {"returncode": value} for value in [True, 0.0, "0", IntSubclass(0), 2**31, -2**31-1]
    ] + [
        {"action_key": value, "result": {"sha256": value}}
        for value in ["a" * 63, "A" * 64, "g" * 64, "a" * 64 + "\n", StringSubclass(RESULT), 0]
    ] + [
        {"result": {"bytes": value}} for value in [True, -1, 1.5, "1", IntSubclass(1), 2**63]
    ] + [
        {"started_at": value, "finished_at": value, "observed_at": value}
        for value in [True, -1, float("nan"), float("inf"), float("-inf"),
                      "2026-09-20", 253402300800, IntSubclass(0)]
    ]
    for record in invalid_records:
        _assert_equal(_view(record), {"schema": SCHEMA, "evidence": {}})
    _assert_equal(_view({1: {"status": "failed"}, StringSubclass("status"): "failed"}),
                  {"schema": SCHEMA, "evidence": {}})
    for value in [None, "arbitrary text", 7, True, DictSubclass(status="failed")]:
        _assert_equal(_view(value), {"schema": SCHEMA, "evidence": {}})
    cyclic = {"status": "running"}
    cyclic["children"] = [cyclic]
    _assert_equal(_view(cyclic), {"schema": SCHEMA, "evidence": {"status": "running"}})
    cyclic_list = []
    cyclic_list.append(cyclic_list)
    _assert_equal(_view(cyclic_list), {"schema": SCHEMA, "evidence": {}})
    deep = {"status": "failed"}
    for _ in range(1000):
        deep = {"children": [deep]}
    _assert_equal(_view(deep), {"schema": SCHEMA, "evidence": {}})
    # Depth and item limits must also be idempotent at their cutoffs.
    boundary = {"status": "running"}
    for _ in range(8):
        boundary = {"status": "ready", "children": [boundary]}
    expected_boundary = {"status": "running"}
    for _ in range(8):
        expected_boundary = {"children": [expected_boundary], "status": "ready"}
    _assert_equal(_view(boundary), {"schema": SCHEMA, "evidence": expected_boundary})
    _assert_equal(_view(_view(boundary)), _view(boundary))
    _assert_equal(_view({"status": "failed", "children": [{"status": "ready"}] * 1025}),
                  {"schema": SCHEMA, "evidence": {"status": "failed"}})
    # Budget exhaustion must not hide approved parent scalars or gain data on
    # re-projection. Root + three full groups + one group with 1019 records
    # consume exactly 4096 nodes, in sorted-field/list order.
    wide = [[{"returncode": 0}] * 1024 for _ in range(5)]
    expected_wide = [[{"returncode": 0}] * 1024 for _ in range(3)]
    expected_wide.append([{"returncode": 0}] * 1019)
    _assert_equal(_view(wide), {"schema": SCHEMA, "evidence": expected_wide})
    _assert_equal(_view(_view(wide)), _view(wide))
    # The derived marker never authorizes pass-through of its evidence.
    _assert_equal(_view({"schema": SCHEMA, "evidence": {
        "status": "ready", "diagnostic": "synthetic-forged-envelope"}}),
        {"schema": SCHEMA, "evidence": {"status": "ready"}})


def test_canonical_receipt_bytes_hash_and_verification_unchanged_by_view():
    from prismabuild.digest_primitives import _canonical_bytes, canonical_sha256

    body = {
        "schema": "prismaquant.prismabuild.cas_receipt.v3", "action_key": ACTION,
        "action_manifest_sha256": MANIFEST, "result": {"sha256": RESULT, "bytes": 64},
        "producer": {"schema": "synthetic-producer.v1", "evidence": {
            "private_control": "synthetic-canonical-private"}},
    }
    # Use the actual schema-specific digest primitives, not the new projection.
    receipt = {**body, "receipt_sha256": canonical_sha256(body)}
    canonical_file_bytes = _canonical_bytes(receipt) + b"\n"
    raw_hash = hashlib.sha256(canonical_file_bytes).hexdigest()
    before = copy.deepcopy(receipt)
    projected = _view(receipt)
    _assert_equal(receipt, before)
    assert _canonical_bytes(receipt) + b"\n" == canonical_file_bytes
    assert hashlib.sha256(_canonical_bytes(receipt) + b"\n").hexdigest() == raw_hash
    verification_body = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    assert canonical_sha256(verification_body) == receipt["receipt_sha256"]
    assert hashlib.sha256(_canonical_bytes(verification_body)).hexdigest() == receipt["receipt_sha256"]
    _assert_equal(projected, {"schema": SCHEMA, "evidence": {
        "action_key": ACTION, "action_manifest_sha256": MANIFEST,
        "result": {"sha256": RESULT, "bytes": 64}}})
    _assert_absent(_serialized(projected), ["synthetic-canonical-private"])
    # The original verification still detects tampering after the view is made.
    tampered = copy.deepcopy(verification_body)
    tampered["result"]["bytes"] = 65
    assert canonical_sha256(tampered) != receipt["receipt_sha256"]


def test_private_recovery_state_stays_with_private_caller():
    recovery = {"token": "synthetic-recovery-token", "claim": {
        "owner": "synthetic-owner", "control": {"capability": "synthetic-recovery-control"}}}
    source = {"status": "running", "recovery": recovery, "children": [
        {"status": "ready", "raw_control": recovery}]}
    projected = _view(source)
    assert source["recovery"] is recovery
    _assert_equal(recovery, {"token": "synthetic-recovery-token", "claim": {
        "owner": "synthetic-owner", "control": {"capability": "synthetic-recovery-control"}}})
    _assert_equal(projected, {"schema": SCHEMA, "evidence": {
        "status": "running", "children": [{"status": "ready"}]}})
    _assert_absent(_serialized(projected), ["synthetic-recovery-token", "synthetic-owner",
                                           "synthetic-recovery-control"])
