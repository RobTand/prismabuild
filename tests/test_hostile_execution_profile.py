"""Hostile profile intent cannot borrow a trusted execution or receipt."""
from __future__ import annotations

from pathlib import Path

import pytest

from prismabuild import core as pb, contracts, execution_profiles, pool, submission
from test_contract_registry_publication import _sealed_request


PROFILE = {"schema": "prismabuild.execution_profile.v1", "name": "hostile-v1",
           "policy_sha256": "a" * 64}


def test_an_unsupported_hostile_profile_refuses_before_trusted_cache_lookup(tmp_path, monkeypatch):
    _queue, action, _cas, checkout = _sealed_request(tmp_path, {"execution_profile": PROFILE})
    touched = []

    class TrustedCache:
        def __init__(self, root):
            touched.append("cas")

        def lookup(self, action):
            touched.append("lookup")
            return {"trusted": True}

        def _verified_receipt_result_path(self, receipt):
            return Path("trusted-result")

    monkeypatch.setattr(pb, "PrismaBuildCAS", TrustedCache)
    with pytest.raises(ValueError, match="hostile-v1"):
        pb.run_local_action(action, cas_root=tmp_path / "cas", checkout_root=checkout)
    assert touched == []


@pytest.mark.parametrize("profile", [
    None, {}, {**PROFILE, "extra": True}, {**PROFILE, "schema": "v0"},
    {**PROFILE, "name": "trusted"}, {**PROFILE, "policy_sha256": "a" * 63},
    {**PROFILE, "policy_sha256": "A" * 64}, {**PROFILE, "policy_sha256": True},
])
def test_malformed_or_ambiguous_profiles_never_seal(tmp_path, profile):
    with pytest.raises(execution_profiles.ProfileError):
        _sealed_request(tmp_path, {"execution_profile": profile})


def test_prepare_binds_policy_and_execution_fences_to_the_action_key(tmp_path, monkeypatch):
    from test_local_scratch_host_kind import _build

    template = _build(tmp_path, monkeypatch, "--execution-profile", "hostile-v1",
                      "--execution-policy-sha256", "a" * 64)
    assert template["params"]["execution_profile"] == PROFILE
    assert {"hostile-execution-v1", "hostile-policy:" + "a" * 64}.issubset(
        set(template["params"]["placement"]["required_tags"]))
    original = submission.seal(template)["action_key"]
    template["params"]["execution_profile"]["policy_sha256"] = "b" * 64
    assert submission.seal(template)["action_key"] != original


@pytest.mark.parametrize("options", [
    ("--execution-profile", "hostile-v1"),
    ("--execution-policy-sha256", "a" * 64),
    ("--execution-profile", "hostile-v1", "--execution-policy-sha256", "bad"),
])
def test_partial_cli_selection_refuses_instead_of_using_trusted_defaults(tmp_path, monkeypatch, capsys, options):
    from test_local_scratch_host_kind import _build

    with pytest.raises(SystemExit) as raised:
        _build(tmp_path, monkeypatch, *options)
    assert raised.value.code == 2
    assert "hostile-v1 requires" in capsys.readouterr().err


def test_trusted_prepare_has_no_profile_parameter_or_fence(tmp_path, monkeypatch):
    from test_local_scratch_host_kind import _build

    template = _build(tmp_path, monkeypatch)
    assert "execution_profile" not in template["params"]
    assert not any(tag.startswith("hostile-") for tag in template["params"]["placement"]["required_tags"])


def test_slurm_cannot_promise_hostile_containment():
    with pytest.raises(execution_profiles.ProfileError, match="pool"):
        execution_profiles.requested("hostile-v1", "a" * 64, transport="slurm")


def test_alternate_publisher_derives_fences_and_claim_refuses_without_a_backend(tmp_path, monkeypatch):
    queue, action, cas, checkout = _sealed_request(tmp_path, {"execution_profile": PROFILE})
    demand = action["params"]["demand"]
    path = queue.publish(action_key=action["action_key"], cas_root=cas.root,
                         checkout_root=checkout, worker_script="/worker.py", resources=demand)
    import json
    row = json.loads(path.read_text())
    assert {"hostile-execution-v1", "hostile-policy:" + "a" * 64}.issubset(set(row["tags"]))
    reasons = []
    monkeypatch.setattr(queue, "record_denial", lambda _item, reason, _detail=None: reasons.append(reason))
    assert queue.claim(tags=row["tags"], capacity=demand) is None
    assert "execution_profile_unavailable" in reasons
    assert queue.ledger().held() == {}
    assert queue.records.item_path(pool.record_store.READY, action["action_key"]).exists()


@pytest.mark.parametrize("tag", ["hostile-execution-v1", "hostile-policy:" + "a" * 64])
def test_a_tag_cannot_turn_a_trusted_request_into_a_hostile_profile(tmp_path, tag):
    queue, action, cas, checkout = _sealed_request(tmp_path, {})
    with pytest.raises(pool.PoolContractError, match="sealed execution profile"):
        queue.publish(action_key=action["action_key"], cas_root=cas.root,
                      checkout_root=checkout, worker_script="/worker.py",
                      resources=action["params"]["demand"], tags=[tag])


@pytest.mark.parametrize("change", ["fence", "demand"])
def test_a_forged_row_cannot_change_its_profile_contract(change):
    selected = execution_profiles.parse_selection(PROFILE)
    action = {"params": {"execution_profile": PROFILE, "demand": {"cpu": 1, "mem_gb": 4}}}
    tags, demand = list(selected.required_tags()), {"cpu": 1, "mem_gb": 4}
    if change == "fence":
        tags.pop()
    else:
        demand["mem_gb"] = 1
    with pytest.raises(execution_profiles.ProfileError):
        execution_profiles.check_row(action, tags=tags, resources=demand)


def test_normal_worker_attestation_cannot_certify_a_hostile_profile(tmp_path):
    _queue, action, _cas, _checkout = _sealed_request(tmp_path, {"execution_profile": PROFILE})
    with pytest.raises(execution_profiles.ProfileUnavailable, match="authenticated collection"):
        pb.validate_worker_attestation({}, action=action)


def test_a_hostile_keyed_receipt_is_a_typed_cas_refusal(tmp_path):
    _queue, action, cas, _checkout = _sealed_request(tmp_path, {"execution_profile": PROFILE})
    receipt = {"schema": pb.CAS_RECEIPT_SCHEMA_V3, "action_key": action["action_key"],
               "action_manifest_sha256": pb.canonical_sha256(action),
               "result": {"sha256": "b" * 64, "bytes": 0},
               "producer": {key: None for key in pb._PRODUCER_KEYS},
               "receipt_sha256": "c" * 64}
    path = cas.receipt_path(action["action_key"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(pb._canonical_file_bytes(receipt))
    with pytest.raises(pb.CASTamperError, match="authenticated collection"):
        cas.lookup(action)


def test_preflight_refuses_before_checking_or_opening_payload_code(tmp_path, monkeypatch):
    _queue, action, cas, checkout = _sealed_request(tmp_path, {"execution_profile": PROFILE})
    def unexpected(*_args, **_kwargs):
        pytest.fail("unsupported profile reached code verification")
    monkeypatch.setattr(pb, "verify_code_closure", unexpected)
    with pytest.raises(pb.ActionContractError, match="authenticated collection"):
        pb.preflight_action(action, cas_root=cas.root, checkout_root=checkout)


@pytest.mark.parametrize("change", ["missing-policy", "extra-policy"])
def test_forged_profile_row_claim_has_a_profile_diagnostic(tmp_path, monkeypatch, change):
    queue, action, cas, checkout = _sealed_request(tmp_path, {"execution_profile": PROFILE})
    demand = action["params"]["demand"]
    path = queue.publish(action_key=action["action_key"], cas_root=cas.root,
                         checkout_root=checkout, worker_script="/worker.py", resources=demand)
    import json
    row = json.loads(path.read_text())
    if change == "missing-policy":
        row["tags"].remove("hostile-policy:" + "a" * 64)
    else:
        row["tags"].append("hostile-policy:" + "b" * 64)
    path.write_text(json.dumps(row))
    reasons = []
    monkeypatch.setattr(queue, "record_denial", lambda _row, reason, _detail=None: reasons.append(reason))
    assert queue.claim(tags=row["tags"], capacity=demand) is None
    assert "execution_profile_intent_invalid" in reasons
    assert queue.ledger().held() == {}


def test_no_worker_capability_is_advertised_by_this_unqualified_implementation():
    offered = contracts.offered_contract_tags(contracts.WorkerFacts(
        loaded_generation="g1", gang_admission=True))
    assert "hostile-execution-v1" not in offered
    assert not any(tag.startswith("hostile-policy:") for tag in offered)
