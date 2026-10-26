"""Source-bound D38 plan and receipt production at the public CLI boundary."""
from __future__ import annotations

import json
import datetime
import hashlib
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from submission_support import patch_store
from test_pbrun_detach import _checkout
from test_pbrun_d38_gate import _queue

import pbrun
from prismabuild import core as pb, pool, d38_gate, materialize, contracts, capability_eligibility
from prismabuild.submission_types import SubmissionRequest

REPOSITORY = Path(__file__).resolve().parents[1]
IMAGE = "example.org/canary@sha256:" + "ab" * 32
NOW = datetime.datetime(2026, 10, 26, 2, tzinfo=datetime.timezone.utc)
COMMAND = ["/usr/bin/python3", "-I", "tools/fleet/pbcanary_legs/leg2.py", "--image", IMAGE,
           "--device", "cuda", "--checks-path", ".d38/leg2-checks.json"]


def _offers(tmp_path):
    queue = _queue(tmp_path)
    queue.announce(host="sparky", tags=["sparky", "gb10", contracts.CONTAINER_IMAGE_TAG,
        contracts.INTERPRETER_TAG], has_gpu=True, capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
        capabilities=capability_eligibility.OfferCapabilities(
            images=capability_eligibility.ImageAnswers.from_observed([IMAGE], None),
            interpreters=capability_eligibility.InterpreterAnswers.from_observed(["/usr/bin/python3"], None)))
    return queue


def test_plan_cli_seals_without_publishing_target(tmp_path, monkeypatch, capsys):
    from prismabuild import d38_producer

    work = _checkout(tmp_path)
    source = work / COMMAND[2]
    source.parent.mkdir(parents=True)
    source.write_bytes((REPOSITORY / COMMAND[2]).read_bytes())
    namespace = tmp_path / "namespace.json"
    namespace.write_text(json.dumps(d38_producer.namespace_for(COMMAND)))
    plan_path = tmp_path / "plan.json"
    store = patch_store(monkeypatch, client=pbrun, root=tmp_path)
    monkeypatch.setattr(pool, "is_box_local_path", lambda _: False)
    _offers(tmp_path)

    assert pbrun.main(["--cwd", str(work), "--d38-plan", str(plan_path),
        "--d38-namespace", str(namespace), "--container-image", IMAGE,
        "--tag", "gb10", "--demand", "gpu=1,mem_gb=8", "--", *COMMAND]) == 0

    output = json.loads(capsys.readouterr().out)
    plan = json.loads(plan_path.read_bytes())
    assert plan["schema"] == "fleet.d38.plan.v1"
    assert plan["target_command"] == COMMAND
    assert plan["cpu_command"] == [*COMMAND[:6], "cpu", *COMMAND[7:]]
    assert plan["changes"] == [{"flag": "--device", "from": "cuda", "to": "cpu"}]
    key = plan["target"]["job_identity_hash"]
    assert output["action_key"] == key
    assert pb.PrismaBuildCAS(store.cas_root).read_action_request(key) is None
    assert not list(store.queue_root.glob("ready/*.json"))


@pytest.mark.parametrize("options", [
    ["--d38-plan", "plan.json", "--detach"],
    ["--d38-plan", "plan.json", "--seal-only"],
    ["--d38-preflight-for", "plan.json", "--gpu"],
    ["--d38-preflight-for", "plan.json", "--env", "PYTHONPATH=unsafe"],
    ["--d38-preflight-for", "plan.json", "--", "/bin/true"],
])
def test_producer_cli_rejects_execution_overrides(options):
    with pytest.raises(SystemExit) as error:
        pbrun.parse_args(options)
    assert error.value.code == 2


@pytest.fixture
def planned(tmp_path, monkeypatch):
    from prismabuild import d38_producer

    work = _checkout(tmp_path)
    source = work / COMMAND[2]
    source.parent.mkdir(parents=True)
    source.write_bytes((REPOSITORY / COMMAND[2]).read_bytes())
    data = work / "input.bin"
    data.write_bytes(b"D38 required input\n")
    command = [*COMMAND, "--input", f"fixture=input.bin={hashlib.sha256(data.read_bytes()).hexdigest()}"]
    namespace = tmp_path / "namespace.json"
    namespace.write_text(json.dumps(d38_producer.namespace_for(command)))
    path = tmp_path / "plan.json"
    store = patch_store(monkeypatch, client=pbrun, root=tmp_path)
    monkeypatch.setattr(pool, "is_box_local_path", lambda _: False)
    monkeypatch.setattr(d38_gate, "utc_now", lambda: NOW)
    _offers(tmp_path)
    request = SubmissionRequest(cwd=str(work), command=command, tag=["gb10"],
        demand="gpu=1,mem_gb=8", container_image=[IMAGE], d38_namespace=str(namespace))
    d38_producer.write_plan(request, path, store=store)
    action, template, effective = d38_producer.prepare_preflight(path, SubmissionRequest(), store=store)
    return json.loads(path.read_bytes()), action, template["cas"], effective, store


def test_preflight_cli_publishes_only_its_derived_cpu_action(planned, tmp_path, capsys):
    _, _, cas, _, store = planned
    store.d38_config_path.write_text('{"d38_enforce": true}')
    assert pbrun.main(["--d38-preflight-for", str(tmp_path / "plan.json"), "--detach"]) == 0
    result = json.loads(capsys.readouterr().out)
    action = cas.read_action_request(result["action_key"])
    assert action["params"]["demand"]["gpu"] == 0
    assert action["environment"]["variables"]["CUDA_VISIBLE_DEVICES"] == ""
    assert action["task"]["result_path"] == ".d38/receipt.json"
    assert len(list(store.queue_root.glob("ready/*.json"))) == 1


def _execute_checks(plan, action, cas, monkeypatch, *, corrupt=None):
    from prismabuild import d38_preflight_driver as driver
    from pbcanary_legs import leg2
    from test_canary_leg2_d38 import _cpu_torch

    # Exercise the real source-owned kernel and file contract. This injected
    # container boundary is separate from the admitted GB10 Docker proof.
    def container_boundary(command, *, check):
        Path(".d38").mkdir(exist_ok=True)
        args = [*command[3:], "--inside", "--source-sha256", plan["descriptor"]["source_sha256"],
                "--payload-sha256", hashlib.sha256(b"pbcanary-leg2:v1").hexdigest()]
        status = leg2.main(args)
        if status == 0 and corrupt:
            path = Path(".d38/leg2-checks.json")
            value = json.loads(path.read_bytes())
            value["checks"]["input_reads"] = corrupt
            path.write_text(json.dumps(value))
        return SimpleNamespace(returncode=status)

    monkeypatch.setattr(driver.platform, "machine", lambda: "aarch64")
    class FixedDatetime:
        @staticmethod
        def now(timezone):
            return NOW
    monkeypatch.setattr(driver, "datetime", SimpleNamespace(datetime=FixedDatetime, timezone=datetime.timezone))
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    monkeypatch.setenv("NVIDIA_VISIBLE_DEVICES", "none")
    monkeypatch.setenv("PBCANARY_VERIFIED_IMAGE", IMAGE)
    monkeypatch.setenv(pb.ACTION_KEY_ENV, action["action_key"])
    monkeypatch.setitem(sys.modules, "torch", _cpu_torch())
    monkeypatch.setattr(driver.shutil, "which", lambda _: str(REPOSITORY / "tools/fleet/docker"))
    plan_input = next(item for item in action["inputs"] if item["id"] == "d38-plan")
    with pytest.MonkeyPatch.context() as boundary:
        boundary.setattr(driver.subprocess, "run", container_boundary)
        return driver.run(str(cas.input_path(plan_input)), plan_input["sha256"], ".d38/receipt.json")


@pytest.fixture
def produced(planned, tmp_path, monkeypatch):
    plan, action, cas, request, store = planned
    item = {"action_key": action["action_key"], "cas_root": str(cas.root),
            "checkout_snapshot": action["params"]["checkout_snapshot"]}
    with materialize._execution_checkout(item, local_checkout_root=tmp_path / "local") as checkout, \
            pytest.MonkeyPatch.context() as executing:
        executing.chdir(checkout)
        executing.setattr(pb.platform, "machine", lambda: "aarch64")
        attestation = pb.preflight_action(action, cas_root=cas.root, checkout_root=checkout)
        body = _execute_checks(plan, action, cas, executing)
        cas.publish_action_request(action)
        receipt, _ = cas.publish_result(action, checkout / ".d38/receipt.json",
            attestation=attestation, return_execution_receipt=True)
    monkeypatch.chdir(tmp_path)
    return plan, action, cas, request, store, receipt, body


def _proof(cas, plan, key, *, job=None):
    return d38_gate.verify_receipt(cas, key, job=job or plan["target"]["job_identity_hash"],
        images=[IMAGE], namespace=plan["target"]["namespace"],
        target_command=plan["target_command"], at=NOW)


def test_produced_receipt_passes_the_real_cas_and_gate(produced):
    plan, action, cas, request, store, receipt, body = produced
    assert body["result"] == "pass"
    assert body["host_class"] == "gb10"
    proof = _proof(cas, plan, action["action_key"])
    assert proof["authorization"] == "receipt"
    assert proof["receipt_sha256"] == receipt["receipt_sha256"]
    store.d38_config_path.write_text('{"d38_enforce": true}')
    d38_gate.require(request, action, cas=cas, queue_root=store.queue_root,
                     transport="pool", store=store)


def test_produced_receipt_refuses_a_different_target(produced):
    plan, action, cas, *_ = produced
    with pytest.raises(d38_gate.Refusal, match="different job identity"):
        _proof(cas, plan, action["action_key"], job="0" * 64)


def test_tampered_produced_receipt_refuses(produced):
    plan, action, cas, _, _, receipt, _ = produced
    blob = cas.blob_path(receipt["result"]["sha256"])
    blob.chmod(0o600)
    blob.write_bytes(b'{"result":"pass"}')
    with pytest.raises(d38_gate.Refusal, match="verification|verified receipt"):
        _proof(cas, plan, action["action_key"])


def test_missing_driver_is_failed_evidence_in_record_only_mode(produced):
    plan, action, cas, _, store, *_ = produced
    driver = next(entry for entry in action["inputs"] if entry["id"] == "d38-driver")
    cas.input_path(driver).unlink()
    request = SubmissionRequest(d38_receipt=action["action_key"])
    store.d38_config_path.write_text('{"d38_enforce": false}')
    d38_gate.require(request, plan["target_action"], cas=cas, queue_root=store.queue_root,
                     transport="pool", store=store)
    audits = list((store.queue_root / "d38-audit" / plan["target"]["job_identity_hash"]).glob("*.json"))
    assert len(audits) == 1
    assert json.loads(audits[0].read_bytes())["result"] == "failed"


def test_gpu_cli_publishes_with_the_produced_receipt(produced, tmp_path, capsys):
    plan, action, cas, _, store, *_ = produced
    store.d38_config_path.write_text('{"d38_enforce": true}')
    capsys.readouterr()
    assert pbrun.main(["--cwd", str(tmp_path / "work"), "--detach",
        "--d38-namespace", str(tmp_path / "namespace.json"), "--container-image", IMAGE,
        "--tag", "gb10", "--demand", "gpu=1,mem_gb=8", "--d38-receipt", action["action_key"],
        "--", *plan["target_command"]]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["action_key"] == plan["target"]["job_identity_hash"]
    target = cas.read_action_request(result["action_key"])
    assert target["params"]["demand"]["gpu"] == 1


@pytest.mark.parametrize("corrupt", ["skipped", "failed"])
def test_incomplete_checks_write_no_receipt(planned, tmp_path, monkeypatch, corrupt):
    plan, action, cas, _, _ = planned
    item = {"action_key": action["action_key"], "cas_root": str(cas.root),
            "checkout_snapshot": action["params"]["checkout_snapshot"]}
    with materialize._execution_checkout(item, local_checkout_root=tmp_path / "local") as checkout:
        monkeypatch.chdir(checkout)
        with pytest.raises(ValueError, match="every required"):
            _execute_checks(plan, action, cas, monkeypatch, corrupt=corrupt)
        assert not (checkout / ".d38/receipt.json").exists()


def test_label_cannot_exempt_an_unrelated_task(planned):
    from prismabuild import d38_producer

    _, action, cas, _, _ = planned
    body = {key: value for key, value in action.items() if key != "action_key"}
    body["task"] = {**body["task"], "argv": ["/bin/true"]}
    unrelated = pb.seal_action(body)
    assert not d38_producer.validated_cpu_preflight(cas, unrelated)


@pytest.mark.parametrize("name,value", [
    ("LD_PRELOAD", "/tmp/inject.so"),
    ("LD_LIBRARY_PATH", "/tmp/libraries"),
    ("PRISMABUILD_DOCKER_TESTING", "1"),
    ("PRISMABUILD_DOCKER_TESTING", ""),
    ("BASH_ENV", "input.bin"),
    ("ENV", "input.bin"),
])
def test_plan_refuses_execution_hooks(planned, name, value):
    import copy
    from prismabuild import d38_producer

    plan = copy.deepcopy(planned[0])
    target = plan["target_action"]
    target["environment"]["variables"][name] = value
    plan["target_action"] = pb.seal_action({key: item for key, item in target.items() if key != "action_key"})
    plan["target"]["job_identity_hash"] = plan["target_action"]["action_key"]
    with pytest.raises(d38_gate.Refusal, match="execution overrides"):
        d38_producer.validate_plan(plan)


def test_installed_flat_layout_resolves_owned_sources(tmp_path, monkeypatch):
    from prismabuild import d38_producer

    source = tmp_path / "tools/pbcanary_legs/leg2.py"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"canary source\n")
    docker = tmp_path / "tools/docker"
    docker.write_bytes(b"docker shim\n")
    monkeypatch.setattr(d38_producer, "_source_root", lambda: tmp_path)
    descriptor = d38_producer.descriptor()
    assert descriptor["source_sha256"] == hashlib.sha256(b"canary source\n").hexdigest()
    assert descriptor["docker_sha256"] == hashlib.sha256(b"docker shim\n").hexdigest()


def test_preflight_binds_the_shared_digest_owner(planned):
    import copy
    from prismabuild import d38_producer, digest_primitives

    plan, action, cas, _, _ = planned
    owners = [entry for entry in action["inputs"] if entry["id"] == "d38-digests"]
    assert len(owners) == 1
    assert cas.input_path(owners[0]).read_bytes() == Path(digest_primitives.__file__).read_bytes()
    assert d38_producer.validated_cpu_preflight(cas, action)
    changed = copy.deepcopy({key: value for key, value in action.items() if key != "action_key"})
    replacement, _ = cas.ingest_bytes(b"raise RuntimeError('unreviewed digest owner')\n", input_id="d38-digests")
    changed["inputs"] = [replacement if entry["id"] == "d38-digests" else entry for entry in changed["inputs"]]
    assert not d38_producer.validated_cpu_preflight(cas, pb.seal_action(changed))
