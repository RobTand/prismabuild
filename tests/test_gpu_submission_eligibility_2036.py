"""Fleet placement decides whether a GPU cap shares the host RAM reservation."""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbcampaign
import pbrun
from prismabuild import decomposition as dc, submission
from submission_support import patch_store
from test_a_decomposed_child_is_an_ordinary_sealed_action import _request
from test_pbrun_detach import _checkout


class Sealed(Exception):
    pass


@pytest.fixture
def fleet(tmp_path, monkeypatch):
    roster = tmp_path / "fleet_boxes.json"
    roster.write_text(json.dumps({"boxes": {
        "unified-host": {"_alias": "unified-alias", "args": [
            "--class", "gb10", "--gpu", "--tag", "arm64"]},
        "discrete-host": {"args": [
            "--class", "x86", "--gpu", "--tag", "discrete"]},
        "cpu-host": {"args": ["--class", "gb10"]},
    }}))
    store = patch_store(monkeypatch, root=tmp_path / "fleet", roster_path=roster)
    monkeypatch.setattr(submission.socket, "gethostname", lambda: "submitter")
    monkeypatch.setattr(submission, "require_reachable_runtime", lambda *args, **kwargs: None)
    work = _checkout(tmp_path)
    monkeypatch.setattr(submission, "git_repository_root", lambda *args, **kwargs: work)
    monkeypatch.setattr(submission, "snapshot_checkout", lambda *args, **kwargs: {
        "input": {"id": "test"}})
    sealed = []

    def seal(body):
        sealed.append(body)
        raise Sealed()

    monkeypatch.setattr(submission.pb, "seal_action", seal)
    return store, work, sealed


def invoke(path, tags, *, fleet, tmp_path, cap=16, mem=8):
    store, work, sealed = fleet
    row = {"argv": ["true"], "cwd": str(work), "tags": tags,
           "anywhere": not tags, "demand": {"gpu": 1, "mem_gb": mem},
           "gpu_memory_gb": cap}
    if path == "cli":
        return pbrun.main(["--cwd", str(work), *(["--anywhere"] if not tags else []), "--demand",
                           f"gpu=1,mem_gb={mem}", "--gpu-memory-gb", str(cap),
                           *[word for tag in tags for word in ("--tag", tag)],
                           "--", "true"])
    if path == "mapping":
        request = submission.request_from_mapping(row, detach=True, transport="pool")
        result = submission.submit_request(request, store=store)
        if isinstance(result, submission.SubmissionRefused):
            raise result
        return result
    if path in {"campaign", "logical-manifest"}:
        manifest = tmp_path / "manifest.json"
        if path == "campaign":
            body = [{"argv": ["true"]}, row]
        else:
            body = _request()
            body["common"].update(row, argv=["true", dc.TASK_BATCH_PLACEHOLDER])
        manifest.write_text(json.dumps(body))
        return pbcampaign.load_manifest(manifest, transport="pool")
    request = _request()
    request["common"].update(row, argv=["true", dc.TASK_BATCH_PLACEHOLDER])
    return pbcampaign.decompose(request, transport="pool", priority=0)


PATHS = ["cli", "mapping", "campaign", "logical-manifest", "decompose"]


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("tags", [["unified-host"], ["unified-alias"], [], ["arm64"]])
def test_unified_eligible_cap_is_refused_before_sealing(path, tags, fleet, tmp_path, capsys):
    with pytest.raises((SystemExit, ValueError, submission.SubmissionRefused,
                        pbcampaign.ManifestError)) as error:
        invoke(path, tags, fleet=fleet, tmp_path=tmp_path)
    diagnostic = (str(error.value) + capsys.readouterr().err).lower()
    assert "gpu_memory_gb=16 exceeds mem_gb=8" in diagnostic
    assert "subset of mem_gb" in diagnostic
    assert fleet[2] == []


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("tags", [["discrete-host"], ["discrete"], ["x86"]])
def test_discrete_only_cap_remains_independent(path, tags, fleet, tmp_path):
    if path in {"campaign", "logical-manifest"}:
        result = invoke(path, tags, fleet=fleet, tmp_path=tmp_path)
        row = result[1] if path == "campaign" else result["common"]
        assert row["gpu_memory_gb"] == 16
        assert row["demand"]["mem_gb"] == 8
    else:
        with pytest.raises(Sealed):
            invoke(path, tags, fleet=fleet, tmp_path=tmp_path)
        params = fleet[2][0]["params"]
        assert params["gpu_memory_gb"] == 16
        assert params["demand"]["mem_gb"] == 8


@pytest.mark.parametrize("path", PATHS)
def test_unclassified_subset_still_accepts(path, fleet, tmp_path):
    if path in {"campaign", "logical-manifest"}:
        result = invoke(path, [], fleet=fleet, tmp_path=tmp_path, cap=8)
        row = result[1] if path == "campaign" else result["common"]
        assert row["gpu_memory_gb"] == 8
    else:
        with pytest.raises(Sealed):
            invoke(path, [], fleet=fleet, tmp_path=tmp_path, cap=8)
        assert fleet[2][0]["params"]["gpu_memory_gb"] == 8
