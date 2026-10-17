"""Exercise the real submission boundaries called out by the C14 review."""
from pathlib import Path
import subprocess
import sys

import pytest

import fleet_submit
import pbcampaign
import pbrun
from prismabuild import core, pool, submission
from prismabuild.submission_types import SubmissionRequest, SubmissionRefused
from submission_support import patch_store
from test_pbrun_detach import _checkout


def test_canary_driver_help_under_an_isolated_system_interpreter(tmp_path):
    script = Path(__file__).resolve().parents[1] / "tools/fleet/pbcanary.py"
    result = subprocess.run([sys.executable, "-I", "-S", str(script), "--help"],
                            cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


@pytest.mark.parametrize("field,value", [
    ("profile", "nsyss"), ("profile", "sample:0"), ("profile", "nsys:nan"),
    ("profile", "torch:1"), ("residency", "stgae"),
    ("residency_ram", "of"), ("residency_share", "of"),
])
def test_package_and_cli_reject_the_same_invalid_options(tmp_path, monkeypatch, capsys, field, value):
    store = patch_store(monkeypatch, root=tmp_path)
    work = _checkout(tmp_path)
    flag = "--" + field.replace("_", "-")
    with pytest.raises(SystemExit) as error:
        pbrun.parse_args([flag, value, "--", "/bin/true"])
    assert error.value.code == 2
    assert flag in capsys.readouterr().err
    request = submission.request_from_mapping({field: value, "argv": ["/bin/true"],
        "cwd": str(work), "here": True, "detach": True, "no_default_env": True})
    result = submission.submit_request(request, store=store)
    assert isinstance(result, SubmissionRefused)
    assert result.returncode == 2
    assert flag in result.reason
    assert not store.cas_root.exists()
    assert not store.queue_root.exists()


@pytest.mark.parametrize("field,value", [
    ("profile", "sample"), ("profile", "sample:100"), ("profile", "nsys:600"),
    ("profile", "torch"), ("residency", "none"), ("residency", "stage"),
    ("residency_ram", "auto"), ("residency_ram", "off"),
    ("residency_share", "auto"), ("residency_share", "off"),
    ("cpus", "2"), ("gpu_memory_gb", "32"), ("timeout_s", "600"),
])
def test_mapping_and_cli_normalize_valid_options_identically(field, value):
    cli = pbrun.request_from_args(pbrun.parse_args([
        "--" + field.replace("_", "-"), value, "--", "/bin/true"]))
    request = submission.request_from_mapping({field: value})
    submission.validate_request(request)
    assert getattr(request, field) == getattr(cli, field)


def test_preparation_argument_refusal_keeps_usage(tmp_path, monkeypatch, capsys):
    patch_store(monkeypatch, root=tmp_path)
    with pytest.raises(SystemExit) as error:
        pbrun.main(["--container-image", "mutable:latest", "--", "/bin/true"])
    assert error.value.code == 2
    diagnostic = capsys.readouterr().err
    assert diagnostic.startswith("usage:")
    assert diagnostic.count("error:") == 1
    assert "--container-image" in diagnostic


def test_d38_cli_renders_its_domain_reason_once_without_argument_usage(tmp_path, monkeypatch, capsys, d38_config):
    work = _checkout(tmp_path)
    store = patch_store(monkeypatch, root=tmp_path)
    d38_config.write_text('{"d38_enforce": true}')
    with pytest.raises(SystemExit) as error:
        pbrun.main(["--cwd", str(work), "--gpu", "--here", "--detach",
                    "--no-default-env", "--", "/bin/true"])
    assert error.value.code == 2
    diagnostic = capsys.readouterr().err
    assert diagnostic.count("D38 refuses GPU publication:") == 1
    assert "namespace descriptor" in diagnostic
    assert "pbrun: error: \n" not in diagnostic
    assert "usage:" not in diagnostic
    assert not list((store.queue_root / pool.READY).glob("*.json"))


def test_compiled_producer_retains_d38_reason_without_a_display(tmp_path, monkeypatch, d38_config):
    from test_slurm_lane import _runnable_action
    store = patch_store(monkeypatch, root=tmp_path)
    d38_config.write_text('{"d38_enforce": true}')
    cas = core.PrismaBuildCAS(store.cas_root)
    action = _runnable_action(tmp_path, cas)
    with pytest.raises(fleet_submit.SubmitRefused) as error:
        fleet_submit.submit(action, cas=cas, request_path=cas.publish_action_request(action),
            transport="pool", needs_gpu=True, resources={"cpu": 1, "mem_gb": 1},
            queue_root=store.queue_root)
    assert "D38 refuses GPU publication:" in str(error.value)
    assert "namespace descriptor" in str(error.value)
    assert not list((store.queue_root / pool.READY).glob("*.json"))


def test_decomposed_refusals_retain_each_childs_diagnosis(tmp_path, monkeypatch, d38_config):
    from test_a_decomposed_child_is_an_ordinary_sealed_action import _request, _checkout as logical_checkout
    work = logical_checkout(tmp_path)
    store = patch_store(monkeypatch, root=tmp_path)
    d38_config.write_text('{"d38_enforce": true}')
    request = _request(2)
    request["common"].update(cwd=str(work),
        demand={"cpu": 1, "mem_gb": 1, "gpu": 1}, here=True)
    records, frozen = pbcampaign.decompose(request, transport="pool", priority=-10)
    assert len(records) == len(frozen["children"])
    assert {record["status"] for record in records} == {"refused"}
    assert all("D38 refuses GPU publication:" in record["error"] for record in records)
    assert not list((store.queue_root / pool.READY).glob("*.json"))
