"""Receipting work across a publication must not schedule a second action."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools/fleet"))
import pbrun
import pbcampaign
from prismabuild import core as pb, pool
from test_pbrun_detach import _checkout, _queue, _run_pbrun, _one_json_line


def _generation(root, name):
    generation = root / "runtime-generations" / name
    tools = generation / "tools"
    tools.mkdir(parents=True)
    shim = tools / "docker"
    shim.write_text("#!/bin/sh\nexit 125\n")
    shim.chmod(0o555)
    receipt = generation / "RUNTIME_VERSION.json"
    receipt.write_text(json.dumps({
        "schema": "prismaquant.prismabuild.runtime_version.v1",
        "generation": name, "commit": "a" * 40,
        "files": {"tools/docker": hashlib.sha256(shim.read_bytes()).hexdigest()},
    }))
    receipt.chmod(0o444)
    return tools


@pytest.fixture
def original(tmp_path, monkeypatch, capsys):
    work, queue = _checkout(tmp_path), _queue(tmp_path)
    old = _generation(tmp_path, "old")
    new = _generation(tmp_path, "new")
    monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR", old)
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach") == 0
    first = _one_json_line(capsys.readouterr())
    key = first["action_key"]
    request = tmp_path / "cas/requests" / key[:2] / f"{key}.json"
    action = pb.validate_action(json.loads(request.read_text()))
    monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR", new)
    return work, queue, action, old, new


def test_publication_alone_moves_the_action_key(original, tmp_path, monkeypatch, capsys):
    work, queue, action, old, new = original
    assert (old / "docker").read_bytes() == (new / "docker").read_bytes()
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach") == 0
    second = _one_json_line(capsys.readouterr())
    assert second["status"] == "submitted"
    assert second["action_key"] != action["action_key"]
    assert len(list(queue.dir(pool.READY).glob("*.json"))) == 2


def test_request_bound_reseal_attaches_without_a_second_key(
    original, tmp_path, monkeypatch, capsys,
):
    work, queue, action, old, new = original
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach",
                      "--as-sealed-by", action["action_key"]) == 0
    repeated = _one_json_line(capsys.readouterr())
    assert repeated["status"] == "attached"
    assert repeated["action_key"] == action["action_key"]
    assert len(list(queue.dir(pool.READY).glob("*.json"))) == 1
    assert pbrun.CONTAINER_WRAPPER_DIR == new


def test_receipted_action_is_a_real_cache_hit_after_publication(
    original, tmp_path, monkeypatch, capsys,
):
    work, queue, action, old, new = original
    outcome = queue.serve_once(tags=["sparky", "gb10"], python=sys.executable,
                               capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
                               timeout_s=60)
    assert outcome["status"] == "executed", outcome
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    receipt = cas.lookup(action)
    assert receipt is not None
    assert cas.result_path(receipt, action).read_text() == "ok"
    capsys.readouterr()
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach",
                      "--as-sealed-by", action["action_key"]) == 0
    repeated = _one_json_line(capsys.readouterr())
    assert repeated["status"] == "cache_hit"
    assert repeated["action_key"] == action["action_key"]
    assert not list(queue.dir(pool.READY).glob("*.json"))
    assert cas.lookup(action) == receipt


@pytest.mark.parametrize("change", [
    "source", "command", "demand", "env", "placement", "timeout", "retry", "profile",
])
def test_changed_work_refuses_before_publishing_an_action(
    original, tmp_path, monkeypatch, capsys, change,
):
    work, queue, action, old, new = original
    options = {
        "demand": ["--cpus", "2"], "env": ["--env", "EXAMPLE=changed"],
        "placement": ["--tag", "sparky", "--tag", "gb10"], "timeout": ["--timeout-s", "300"],
        "retry": ["--retry-safe"], "profile": ["--profile", "sample"],
    }.get(change, [])
    command = ("/bin/bash", "-lc", "printf changed" if change == "command" else "printf ok")
    if change == "source":
        (work / "seed.txt").write_text("changed\n")
    before = {p: p.read_bytes() for p in (tmp_path / "cas/requests").rglob("*.json")}
    with pytest.raises(SystemExit, match="current work seals to.*nothing submitted"):
        _run_pbrun(tmp_path, monkeypatch, work, "--detach",
                   "--as-sealed-by", action["action_key"], *options, command=command)
    assert {p: p.read_bytes() for p in (tmp_path / "cas/requests").rglob("*.json")} == before
    assert len(list(queue.dir(pool.READY).glob("*.json"))) == 1


def test_priority_remains_a_queue_hint(original, tmp_path, monkeypatch, capsys):
    work, queue, action, old, new = original
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach", "--priority", "-10",
                      "--as-sealed-by", action["action_key"]) == 0
    assert _one_json_line(capsys.readouterr())["action_key"] == action["action_key"]


@pytest.mark.parametrize("damage", ["request", "address", "wrapper", "missing", "symlink", "generation"])
def test_damaged_reference_refuses_before_any_new_request(
    original, tmp_path, monkeypatch, damage,
):
    work, queue, action, old, new = original
    key = action["action_key"]
    path = tmp_path / "cas/requests" / key[:2] / f"{key}.json"
    if damage in ("request", "address"):
        path.chmod(0o644)
        body = dict(action, action_key="f" * 64)
        if damage == "address":
            body["params"] = dict(action["params"], command=["changed"])
            body = pb.seal_action({k: v for k, v in body.items() if k != "action_key"})
        path.write_text(json.dumps(body))
        path.chmod(0o444)
    elif damage == "wrapper":
        (old / "docker").chmod(0o755)
        (old / "docker").write_text("#!/bin/sh\nexit 0\n")
        (old / "docker").chmod(0o555)
    elif damage == "missing":
        (old / "docker").unlink()
    elif damage == "symlink":
        (old / "docker").unlink()
        (old / "docker").symlink_to(new / "docker")
    else:
        version = old.parent / "RUNTIME_VERSION.json"
        version.chmod(0o644)
        version.write_text('{"generation": "different"}')
        version.chmod(0o444)
    before = {p: p.read_bytes() for p in (tmp_path / "cas/requests").rglob("*.json")}
    with pytest.raises(SystemExit, match="cannot reseal"):
        _run_pbrun(tmp_path, monkeypatch, work, "--detach", "--as-sealed-by", key)
    assert {p: p.read_bytes() for p in (tmp_path / "cas/requests").rglob("*.json")} == before
    assert len(list(queue.dir(pool.READY).glob("*.json"))) == 1


def test_mixed_generation_campaign_reuses_each_rows_own_key(
    original, tmp_path, monkeypatch, capsys,
):
    work, queue, action, old, new = original
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach") == 0
    second = _one_json_line(capsys.readouterr())
    for _ in range(2):
        outcome = queue.serve_once(tags=["sparky", "gb10"], python=sys.executable,
                                   capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
                                   timeout_s=60)
        assert outcome["status"] == "executed", outcome
    third = _generation(tmp_path, "third")
    monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR", third)
    keys = [action["action_key"], second["action_key"]]
    manifest = tmp_path / "campaign.json"
    manifest.write_text(json.dumps([
        {"cwd": str(work), "argv": ["/bin/bash", "-lc", "printf ok"],
         "as_sealed_by": key} for key in keys
    ]))
    capsys.readouterr()
    assert pbcampaign.main(["--transport", "pool", "--detach", str(manifest)]) == 0
    rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [row["action_key"] for row in rows] == keys
    assert [row["status"] for row in rows] == ["cache_hit", "cache_hit"]
    assert not list(queue.dir(pool.READY).glob("*.json"))
    assert pbrun.CONTAINER_WRAPPER_DIR == third
    # Receipt collection normally asks for the final table rather than detach.
    assert pbcampaign.main(["--transport", "pool", "--wait-s", "0", str(manifest)]) == 0
    table = capsys.readouterr().out
    assert table.count("cache_hit") == 2
    assert all(key[:12] in table for key in keys)
    assert not list(queue.dir(pool.READY).glob("*.json"))


@pytest.mark.parametrize("key", [None, True, 123, [], {}, "", "abc", "A" * 64, "../bad"])
def test_bad_campaign_key_refuses_the_whole_manifest(tmp_path, key):
    manifest = tmp_path / "campaign.json"
    manifest.write_text(json.dumps([{"argv": ["true"]}, {"argv": ["true"], "as_sealed_by": key}]))
    with pytest.raises(pbcampaign.ManifestError, match="row 1: as_sealed_by"):
        pbcampaign.load_manifest(str(manifest), transport="pool")


@pytest.mark.parametrize("options", [
    ["--no-default-env"], ["--env", "PATH=/usr/bin:/bin"],
])
def test_reseal_preserves_the_callers_path_contract(tmp_path, monkeypatch, capsys, options):
    work, queue = _checkout(tmp_path), _queue(tmp_path)
    old, new = _generation(tmp_path, "old"), _generation(tmp_path, "new")
    monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR", old)
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach", *options) == 0
    first = _one_json_line(capsys.readouterr())
    monkeypatch.setattr(pbrun, "CONTAINER_WRAPPER_DIR", new)
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach", *options,
                      "--as-sealed-by", first["action_key"]) == 0
    assert _one_json_line(capsys.readouterr())["action_key"] == first["action_key"]


@pytest.mark.parametrize("prefix", ["/tmp/unrelated/tools", "/mnt/shared/prismabuild-fleet/repo/tools", ""])
def test_reference_cannot_select_an_arbitrary_wrapper(
    original, tmp_path, monkeypatch, prefix,
):
    work, queue, action, old, new = original
    action["environment"]["variables"]["PATH"] = prefix + ":/usr/bin:/bin"
    action = pb.seal_action({k: v for k, v in action.items() if k != "action_key"})
    pb.PrismaBuildCAS(tmp_path / "cas").publish_action_request(action)
    with pytest.raises(SystemExit, match="not in a retained fleet generation"):
        _run_pbrun(tmp_path, monkeypatch, work, "--detach", "--as-sealed-by", action["action_key"])
    assert len(list(queue.dir(pool.READY).glob("*.json"))) == 1
