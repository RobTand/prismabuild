"""Opt-in class placement retains real platform and producer identity (#366)."""
from __future__ import annotations

import copy
from pathlib import Path
from types import SimpleNamespace

import pytest

from test_pbrun_host_class import _sealed_body
from test_core import _body
from prismabuild import core as pb, pool
import pbrun


def _evidence(host="sparky", uuid="GPU-1111"):
    return {
        "source": "local", "hostname": host, "system": "linux",
        "machine": "aarch64", "libc": "glibc-2.39", "slurm": None,
        "accelerators": [{"kind": "nvidia", "compute_capability": "12.1",
                          "driver_version": "580.82.07", "name": "NVIDIA GB10",
                          "uuid": uuid}],
    }


def _measurement(tmp_path, monkeypatch):
    monkeypatch.setattr(pb, "_collect_worker_evidence", lambda **kw: _evidence())
    scope, toolchain = pbrun.host_class_scope("gb10", measurement=True, transport="pool")
    body = _body(tmp_path, task_class="measurement", determinism="stochastic",
                 argv=[pbrun.SEALED_ARGV0, "--noprofile", "--norc", "-c",
                       "printf 'before:%s\\nafter:%s\\n' \"$$\" \"$$\" > result.bin"])
    body["inputs"] = []
    body["execution_scope"] = scope
    body["environment"]["toolchain"] = toolchain
    body["params"]["placement"] = {"required_tags": ["gb10"]}
    return pb.seal_action(body)


@pytest.mark.parametrize("here", [False, True])
def test_pool_measurement_can_select_a_class_without_forcing_the_submitter(
    monkeypatch, tmp_path, here
):
    import pbrun

    monkeypatch.setattr(pbrun.socket, "gethostname", lambda: "sparky")
    arguments = ["--measurement", "--host-class", "gb10"]
    if here:
        arguments += ["--here"]
    body = _sealed_body([*arguments, "--", "true"], monkeypatch, tmp_path)
    assert body["task"]["task_class"] == "measurement"
    assert body["execution_scope"]["portability"] == "platform_keyed"
    assert body["execution_scope"]["host_class"] is None
    assert body["params"]["placement"]["required_tags"] == (
        ["gb10", "sparky"] if here else ["gb10"]
    )
    assert "accelerator_models.sha256" in body["environment"]["toolchain"]


@pytest.mark.parametrize("host,uuid", [("sparky", "GPU-1111"), ("sparklina", "GPU-2222")])
def test_either_matching_worker_claims_and_attests_one_complete_pair(
    tmp_path, monkeypatch, host, uuid
):
    action = _measurement(tmp_path, monkeypatch)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.publish(action_key=action["action_key"], cas_root=tmp_path / "cas",
                  checkout_root=tmp_path, worker_script="/worker.py",
                  tags=action["params"]["placement"]["required_tags"], resources={"cpu": 1})
    assert queue.claim(tags=["x86"], capacity={"cpu": 1}) is None
    monkeypatch.setattr(pool.socket, "gethostname", lambda: host)
    item = queue.claim(tags=["gb10", host], capacity={"cpu": 1})
    assert item is not None and item["action_key"] == action["action_key"]
    monkeypatch.setattr(pb, "_collect_worker_evidence", lambda **kw: _evidence(host, uuid))
    result = pb.run_local_action(action, cas_root=tmp_path / "cas", checkout_root=tmp_path)
    producer = result["receipt"]["producer"]
    assert producer["worker_id"] == host
    assert producer["evidence"]["accelerators"][0]["uuid"] == uuid
    assert producer["host_class"] is None  # No invented SLURM attestation.
    assert pb.validate_worker_attestation(producer, action=action) == producer
    arms = Path(result["payload_path"]).read_text().splitlines()
    assert [arm.split(":")[0] for arm in arms] == ["before", "after"]
    assert arms[0].split(":")[1] == arms[1].split(":")[1]
    assert pb.run_local_action(action, cas_root=tmp_path / "cas",
                               checkout_root=tmp_path)["status"] == "cache_hit"


@pytest.mark.parametrize("field,value", [
    ("machine", "x86_64"), ("libc", "glibc-2.40"),
    ("compute_capability", "9.0"), ("driver_version", "590.1"),
    ("name", "Another model with the same SM"), ("missing_identity", None),
])
def test_worker_drift_refuses_before_either_arm(tmp_path, monkeypatch, field, value):
    action = _measurement(tmp_path, monkeypatch)
    observed = _evidence("sparklina", "GPU-2222")
    if field in {"machine", "libc"}:
        observed[field] = value
    elif field == "missing_identity":
        observed["accelerators"][0].pop("name")
    else:
        observed["accelerators"][0][field] = value
    monkeypatch.setattr(pb, "_collect_worker_evidence", lambda **kw: observed)
    with pytest.raises(pb.ActionContractError):
        pb.run_local_action(action, cas_root=tmp_path / "cas", checkout_root=tmp_path)
    assert not (tmp_path / "result.bin").exists()


def test_executable_drift_refuses_before_either_arm(tmp_path, monkeypatch):
    action = _measurement(tmp_path, monkeypatch)
    executable = pb.identify_executable(pbrun.SEALED_ARGV0)
    monkeypatch.setattr(pb, "identify_executable", lambda path: dict(executable, sha256="0" * 64))
    with pytest.raises(pb.ActionContractError, match="argv0.sha256"):
        pb.preflight_action(action, cas_root=tmp_path / "cas", checkout_root=tmp_path)


def test_class_measurement_requires_exact_driver_even_when_prefix_would_match(
    tmp_path, monkeypatch
):
    action = _measurement(tmp_path, monkeypatch)
    body = {key: value for key, value in action.items() if key != "action_key"}
    body["environment"]["toolchain"]["nvidia_driver"] = "595.84"
    action = pb.seal_action(body)
    evidence = _evidence()
    evidence["accelerators"][0]["driver_version"] = "595.84.1"
    monkeypatch.setattr(pb, "_collect_worker_evidence", lambda **kw: evidence)
    with pytest.raises(pb.ActionContractError, match="nvidia_driver"):
        pb.preflight_action(action, cas_root=tmp_path / "cas", checkout_root=tmp_path)


def test_model_count_changes_identity_but_uuid_does_not():
    first = _evidence()
    other_device = _evidence("sparklina", "GPU-2222")
    assert pb.accelerator_models_contract(first) == pb.accelerator_models_contract(other_device)
    other_device["accelerators"].append(first["accelerators"][0])
    assert pb.accelerator_models_contract(first) != pb.accelerator_models_contract(other_device)


def test_cache_scope_changes_with_class_model_architecture_and_toolchain(tmp_path, monkeypatch):
    action = _measurement(tmp_path, monkeypatch)
    pb.run_local_action(action, cas_root=tmp_path / "cas", checkout_root=tmp_path)
    for change in ("class", "name", "machine", "libc", "driver_version"):
        other = copy.deepcopy(action)
        other.pop("action_key")
        observed = _evidence()
        if change == "class":
            other["params"]["placement"]["required_tags"] = ["another-class"]
        else:
            if change in {"name", "driver_version"}:
                observed["accelerators"][0][change] = (
                    "different model" if change == "name" else "590.1")
            else:
                observed[change] = "x86_64" if change == "machine" else "glibc-2.40"
            monkeypatch.setattr(pb, "_collect_worker_evidence", lambda **kw: observed)
            scope, toolchain = pbrun.host_class_scope("gb10", measurement=True, transport="pool")
            other["execution_scope"] = scope
            other["environment"]["toolchain"] = toolchain
        resealed = pb.seal_action(other)
        assert resealed["action_key"] != action["action_key"]
        assert pb.PrismaBuildCAS(tmp_path / "cas").lookup(resealed) is None


@pytest.mark.parametrize("field", ["name", "driver_version", "libc"])
def test_receipt_rederives_contract_from_recorded_evidence(tmp_path, monkeypatch, field):
    action = _measurement(tmp_path, monkeypatch)
    producer = pb.preflight_action(action, cas_root=tmp_path / "cas", checkout_root=tmp_path)
    if field == "libc":
        producer["evidence"]["libc"] = "glibc-2.40"
    else:
        producer["evidence"]["accelerators"][0][field] = (
            "another model" if field == "name" else "590.1")
    producer["attestation_sha256"] = pb.canonical_sha256(
        {key: value for key, value in producer.items() if key != "attestation_sha256"})
    with pytest.raises(pb.ActionContractError, match="differs from the action"):
        pb.validate_worker_attestation(producer, action=action)


def test_device_probe_records_uuid_and_refuses_unavailable_identity(monkeypatch):
    monkeypatch.setattr(pb.Path, "is_file", lambda path: True)
    calls = []

    def probe(argv, **kw):
        calls.append(argv)
        return SimpleNamespace(returncode=0, stdout="12.1, 580.82.07, NVIDIA GB10, GPU-1111\n")

    monkeypatch.setattr(pb.subprocess, "run", probe)
    assert pb._probe_nvidia_accelerators(include_identity=True) == _evidence()["accelerators"]
    assert "--query-gpu=compute_cap,driver_version,name,uuid" in calls[0]
    monkeypatch.setattr(pb.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=1))
    with pytest.raises(pb.ActionContractError, match="cannot attest"):
        pb._probe_nvidia_accelerators(include_identity=True)
    assert pb._probe_nvidia_accelerators() == []  # Existing ordinary behavior.
