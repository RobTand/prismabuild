"""Offline class-image contracts; fixtures do not qualify a live worker."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import container_images as ci  # noqa: E402
from test_container_image_inventory import (  # noqa: E402
    DIVERGENT_TAG, GLM_TAG, _box_inventory, _fixture_row,
)

POLICY_SCHEMA = "prismabuild.container_class_requirements.v1"
STORE = "/var/lib/docker"
CONTENT_A = "content:sha256:" + "a" * 64
CONTENT_B = "content:sha256:" + "b" * 64


def _name(content):
    return "fixture-" + content[-8:] + ":v1"


def _policy(*images, names=None):
    return {"schema": POLICY_SCHEMA, "classes": {
        "gb10": {"store_root": STORE, "images": (
            {_name(ref): ref for ref in images} if names is None else names)},
    }}


def _snapshot(base_entries, **changes):
    return {"schema": ci.INVENTORY_SCHEMA, "observed_unix": 1000.0,
            "entries": list(base_entries), "store_root": STORE,
            "image_contents": {_name(ref): ref for ref in base_entries
                               if isinstance(ref, str) and ref.startswith("content:")},
            **changes}


def _verdict(policy, snapshot, klass="gb10"):
    return ci.class_image_verdict(policy, klass, snapshot, now=1000.0)


@pytest.mark.parametrize("box", ["sparky", "sparklina"])
def test_named_comparable_content_satisfies_each_recorded_store(box):
    want = ci.content_ref(_fixture_row("sparky", GLM_TAG))
    entries, probe = _box_inventory(box)
    assert entries is not None and probe.calls  # captured bytes, not Docker
    aliases = {GLM_TAG: ci.content_ref(_fixture_row(box, GLM_TAG))}
    result = _verdict(_policy(names={GLM_TAG: want}),
                      _snapshot(entries, image_contents=aliases))
    assert result["status"] == "satisfied"
    assert result["container_work_eligible"] is True
    assert result["scope"] == "container_work_only"
    assert result["authority"] == "supplied_snapshot_only"
    assert result["missing_images"] == []


def test_extra_images_are_not_class_drift_and_inputs_are_unchanged():
    policy = _policy(CONTENT_A)
    snapshot = _snapshot([CONTENT_A, CONTENT_B])
    before = copy.deepcopy((policy, snapshot))
    assert _verdict(policy, snapshot)["status"] == "satisfied"
    assert (policy, snapshot) == before


def test_missing_required_name_refuses_container_work_only():
    result = _verdict(_policy(CONTENT_A), _snapshot([CONTENT_B]))
    assert result["status"] == "refused"
    assert result["container_work_eligible"] is False
    assert result["scope"] == "container_work_only"
    assert result["reason"] == "required_image_missing"
    assert result["missing_images"] == [_name(CONTENT_A)]


def test_required_content_under_an_unrelated_tag_does_not_excuse_tag_drift():
    names = {"required:tag": CONTENT_B, "unrelated:tag": CONTENT_A}
    result = _verdict(_policy(names={"required:tag": CONTENT_A}),
                      _snapshot([CONTENT_A, CONTENT_B], image_contents=names))
    assert result["status"] == "refused"
    assert result["reason"] == "image_content_mismatch"
    assert result["mismatched_images"] == [{"name": "required:tag",
                                            "expected": CONTENT_A,
                                            "observed": CONTENT_B}]


def test_same_tag_different_recorded_content_does_not_satisfy_a_class():
    want = ci.content_ref(_fixture_row("sparky", DIVERGENT_TAG))
    other = ci.content_ref(_fixture_row("sparklina", DIVERGENT_TAG))
    assert want != other
    result = _verdict(_policy(names={DIVERGENT_TAG: want}),
                      _snapshot([other], image_contents={DIVERGENT_TAG: other}))
    assert result["reason"] == "image_content_mismatch"


def test_wrong_store_refuses_even_when_all_named_content_is_present():
    result = _verdict(_policy(CONTENT_A),
                      _snapshot([CONTENT_A], store_root="/other/docker"))
    assert result["status"] == "refused"
    assert result["reason"] == "image_store_mismatch"
    assert result["container_work_eligible"] is False


@pytest.mark.parametrize("changes", [
    {"schema": "old"}, {"entries": None}, {"entries": ["mutable:tag"]},
    {"observed_unix": True}, {"observed_unix": float("nan")},
    {"observed_unix": 1001.0}, {"observed_unix": 900.0},
    {"store_root": None}, {"store_root": "relative"},
    {"store_root": "/var/lib/../docker"},
    {"image_contents": None}, {"image_contents": []},
    {"image_contents": {"bare-name": CONTENT_A}},
    {"image_contents": {"image:tag": "sha256:" + "a" * 64}},
    {"image_contents": {"image:tag": CONTENT_B}},
])
def test_stale_malformed_or_contradictory_inventory_is_unknown(changes):
    result = _verdict(_policy(CONTENT_A), _snapshot([CONTENT_A], **changes))
    assert result["status"] == "unknown"
    assert result["container_work_eligible"] is False
    assert result["reason"] == "inventory_unknown"


@pytest.mark.parametrize("field", ["store_root", "image_contents"])
def test_absent_projection_is_unknown_not_a_complete_empty_projection(field):
    snapshot = _snapshot([CONTENT_A])
    del snapshot[field]
    assert _verdict(_policy(CONTENT_A), snapshot)["status"] == "unknown"


@pytest.mark.parametrize("snapshot", [None, [], "unreadable"])
def test_missing_or_unreadable_inventory_never_satisfies_a_class(snapshot):
    assert _verdict(_policy(CONTENT_A), snapshot)["status"] == "unknown"


def test_complete_empty_alias_projection_establishes_a_missing_name():
    result = _verdict(_policy(CONTENT_A), _snapshot([], image_contents={}))
    assert result["status"] == "refused"
    assert result["reason"] == "required_image_missing"


def test_one_class_never_inherits_another_class_requirements():
    result = _verdict(_policy(CONTENT_A), _snapshot([CONTENT_A]), "x86")
    assert result["status"] == "unknown"
    assert result["reason"] == "class_requirements_missing"


def test_empty_fleet_policy_never_implies_a_default_class():
    policy = {"schema": POLICY_SCHEMA, "classes": {}}
    assert _verdict(policy, _snapshot([CONTENT_A]))["status"] == "unknown"


def test_explicit_empty_class_still_needs_positive_store_inventory():
    assert _verdict(_policy(), _snapshot([]))["status"] == "satisfied"
    assert _verdict(_policy(), None)["status"] == "unknown"


def test_offer_ttl_boundary_and_stricter_claim_freshness():
    snapshot = _snapshot([CONTENT_A], observed_unix=970.0)
    assert _verdict(_policy(CONTENT_A), snapshot)["status"] == "satisfied"
    snapshot["observed_unix"] = 969.99
    assert _verdict(_policy(CONTENT_A), snapshot)["status"] == "unknown"
    snapshot["observed_unix"] = 990.0
    result = ci.class_image_verdict(_policy(CONTENT_A), "gb10", snapshot,
                                   now=1000.0, max_age_s=ci.CLAIM_FRESHNESS_S)
    assert result["status"] == "unknown"


def test_inventory_and_alias_counts_are_bounded(monkeypatch):
    monkeypatch.setattr(ci, "MAX_INVENTORY_ENTRIES", 1)
    assert _verdict(_policy(CONTENT_A), _snapshot([CONTENT_A, CONTENT_B]))["status"] == "unknown"
    snapshot = _snapshot([CONTENT_A], image_contents={"first:tag": CONTENT_A,
                                                    "second:tag": CONTENT_A})
    assert _verdict(_policy(CONTENT_A), snapshot)["status"] == "unknown"


@pytest.mark.parametrize("bad", [
    None, {"schema": "old", "classes": {}},
    {"schema": POLICY_SCHEMA, "classes": []},
    {"schema": POLICY_SCHEMA, "classes": {}, "extra": True},
    {"schema": POLICY_SCHEMA, "classes": {"bad class": {}}},
    {"schema": POLICY_SCHEMA, "classes": {"gb10": {"images": {}}}},
    {"schema": POLICY_SCHEMA, "classes": {"gb10": {"store_root": "relative", "images": {}}}},
    _policy(names={"bare-name": CONTENT_A}),
    _policy(names={"bad::tag": CONTENT_A}),
    _policy(names={"repo/:tag": CONTENT_A}),
    _policy(names={"image:tag": "sha256:" + "a" * 64}),
    _policy(names={"image:tag": "repo@sha256:" + "a" * 64}),
    _policy(names={"image:tag": "image:tag"}),
])
def test_invalid_declarations_are_refused_before_evaluation(bad):
    with pytest.raises(ValueError, match="class image requirements"):
        _verdict(bad, _snapshot([CONTENT_A]))


def test_class_declarations_are_canonical_without_mutating_callers():
    policy = _policy(CONTENT_B, CONTENT_A, CONTENT_A)
    before = copy.deepcopy(policy)
    parsed = ci.normalize_class_image_requirements(policy)
    assert parsed["gb10"].images == ((_name(CONTENT_A), CONTENT_A),
                                     (_name(CONTENT_B), CONTENT_B))
    assert parsed["gb10"].store_root == STORE
    assert policy == before


def test_named_inspect_uses_the_existing_comparable_content_identity():
    row = _fixture_row("sparky", GLM_TAG)
    result = ci.parse_named_inspect(json.dumps(row))
    assert result[GLM_TAG] == ci.content_ref(row)
    assert ci.parse_inspect(json.dumps(row)) == frozenset([ci.content_ref(row)])


def test_named_inspect_refuses_conflicting_duplicate_aliases():
    first = copy.deepcopy(_fixture_row("sparky", DIVERGENT_TAG))
    second = copy.deepcopy(_fixture_row("sparklina", DIVERGENT_TAG))
    first["RepoTags"] = second["RepoTags"] = ["required:tag"]
    with pytest.raises(ValueError, match="conflicting"):
        ci.parse_named_inspect(json.dumps(first) + "\n" + json.dumps(second))


def test_named_inspect_deduplicates_equal_aliases_and_knows_untagged_rows():
    row = copy.deepcopy(_fixture_row("sparky", GLM_TAG))
    text = json.dumps(row)
    assert ci.parse_named_inspect(text + "\n" + text) == ci.parse_named_inspect(text)
    row["RepoTags"] = None
    assert ci.parse_named_inspect(json.dumps(row)) == {}
    del row["RepoTags"]
    with pytest.raises(ValueError):
        ci.parse_named_inspect(json.dumps(row))


@pytest.mark.parametrize("value", [True, 0, -1, float("nan"), float("inf"), 10 ** 400])
def test_invalid_evaluation_clock_never_creates_a_verdict(value):
    with pytest.raises(ValueError):
        ci.class_image_verdict(_policy(CONTENT_A), "gb10", _snapshot([CONTENT_A]),
                               now=value)


@pytest.mark.parametrize("age", [True, 0, -1, float("nan"), 31.0, 10 ** 400])
def test_freshness_override_cannot_disable_or_weaken_the_offer_bound(age):
    with pytest.raises(ValueError):
        ci.class_image_verdict(_policy(CONTENT_A), "gb10", _snapshot([CONTENT_A]),
                               now=1000.0, max_age_s=age)


def test_named_inspect_row_and_byte_limits_are_enforced(monkeypatch):
    text = json.dumps(_fixture_row("sparky", GLM_TAG))
    monkeypatch.setattr(ci, "MAX_INVENTORY_BYTES", 1)
    with pytest.raises(ValueError):
        ci.parse_named_inspect(text)
    monkeypatch.setattr(ci, "MAX_INVENTORY_BYTES", len(text.encode()) * 3)
    monkeypatch.setattr(ci, "MAX_INVENTORY_ENTRIES", 1)
    with pytest.raises(ValueError):
        ci.parse_named_inspect(text + "\n" + text)


def _cli_files(tmp_path):
    policy = tmp_path / "requirements.json"
    inventory = tmp_path / "inventory.json"
    policy.write_text(json.dumps(_policy(CONTENT_A)))
    inventory.write_text(json.dumps(_snapshot([CONTENT_A])))
    return policy, inventory


def _cli_args(policy, inventory, klass="gb10"):
    return ["--class-verdict", "--requirements", str(policy),
            "--class", klass, "--inventory", str(inventory)]


@pytest.fixture
def no_docker(monkeypatch):
    def prohibited(*a, **k):
        pytest.fail("offline class verdict must not probe Docker or refresh a cache")
    for name in ("local_content_ref", "observe", "_run_bounded", "InventoryCache"):
        monkeypatch.setattr(ci, name, prohibited)
    monkeypatch.setattr(ci.time, "time", lambda: 1000.0)


def test_offline_cli_json_has_no_live_admission_authority(tmp_path, no_docker, capsys):
    policy, inventory = _cli_files(tmp_path)
    assert ci.main(_cli_args(policy, inventory)) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "satisfied"
    assert result["authority"] == "supplied_snapshot_only"


@pytest.mark.parametrize("problem", ["unreadable", "malformed", "unknown", "stale"])
def test_offline_cli_bad_inventory_is_nonzero_without_probe(tmp_path, no_docker, capsys, problem):
    policy, inventory = _cli_files(tmp_path)
    if problem == "unreadable":
        inventory.unlink()
    elif problem == "malformed":
        inventory.write_text("not json")
    elif problem == "unknown":
        inventory.write_text("null")
    else:
        inventory.write_text(json.dumps(_snapshot([CONTENT_A], observed_unix=900.0)))
    assert ci.main(_cli_args(policy, inventory)) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "unknown"


def test_offline_cli_unknown_class_is_nonzero(tmp_path, no_docker, capsys):
    policy, inventory = _cli_files(tmp_path)
    assert ci.main(_cli_args(policy, inventory, "x86")) == 1
    assert json.loads(capsys.readouterr().out)["reason"] == "class_requirements_missing"


@pytest.mark.parametrize("problem", ["unreadable", "malformed"])
def test_offline_cli_bad_requirements_are_configuration_errors(tmp_path, no_docker, capsys, problem):
    policy, inventory = _cli_files(tmp_path)
    if problem == "unreadable":
        policy.unlink()
    else:
        policy.write_text("not json")
    assert ci.main(_cli_args(policy, inventory)) == 2
    assert json.loads(capsys.readouterr().out)["reason"] == "requirements_invalid"


def test_offline_cli_missing_options_are_usage_errors_without_probe(no_docker):
    with pytest.raises(SystemExit) as error:
        ci.main(["--class-verdict", "--class", "gb10"])
    assert error.value.code == 2


def test_offline_cli_duplicate_alias_never_uses_last_write_wins(tmp_path, no_docker, capsys):
    policy, inventory = _cli_files(tmp_path)
    snapshot = json.dumps(_snapshot([CONTENT_A, CONTENT_B]))
    alias = json.dumps(_name(CONTENT_A))
    needle = alias + ": " + json.dumps(CONTENT_A)
    inventory.write_text(snapshot.replace(needle, alias + ": " + json.dumps(CONTENT_B)
                                          + ", " + needle))
    assert ci.main(_cli_args(policy, inventory)) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "unknown"


def test_offline_cli_byte_limit_is_unknown_not_partial_inventory(tmp_path, no_docker, capsys, monkeypatch):
    policy, inventory = _cli_files(tmp_path)
    monkeypatch.setattr(ci, "MAX_INVENTORY_BYTES", 1024)
    inventory.write_bytes(b" " * 1025)
    assert ci.main(_cli_args(policy, inventory)) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "unknown"


def test_legacy_content_reference_cli_is_unchanged(monkeypatch, capsys):
    monkeypatch.setattr(ci, "local_content_ref", lambda ref: CONTENT_A)
    assert ci.main(["local:tag"]) == 0
    assert capsys.readouterr().out == "local:tag\t" + CONTENT_A + "\n"
