"""Fleet placement decides whether a GPU cap shares the host RAM reservation."""
from __future__ import annotations

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbcampaign
import pbrun
from prismabuild import contracts, decomposition as dc, submission
from prismabuild.submission_types import ProducerPublication
from submission_support import patch_store
from test_a_decomposed_child_is_an_ordinary_sealed_action import _request
from test_pbrun_detach import _checkout
from test_verified_action_result import _standard_action


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
    original_seal = submission.pb.seal_action

    def seal(body):
        sealed.append(body)
        raise Sealed()

    monkeypatch.setattr(submission.pb, "seal_action", seal)
    return SimpleNamespace(store=store, work=work, sealed=sealed,
                           seal_action=original_seal, monkeypatch=monkeypatch)


def invoke(path, tags, *, fleet, tmp_path, cap=16, mem=8):
    store, work, sealed = fleet.store, fleet.work, fleet.sealed
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
    if path in {"sealed", "producer"}:
        with fleet.monkeypatch.context() as patch:
            patch.setattr(submission.pb, "seal_action", fleet.seal_action)
            action = _standard_action(work, command=["true"], demand=row["demand"])
            body = {key: value for key, value in action.items() if key != "action_key"}
            body["params"].update(gpu_memory_gb=cap, placement={"required_tags": tags})
            action = submission.pb.seal_action(body)
        cas = submission.pb.PrismaBuildCAS(store.cas_root)

        def publish(action):
            sealed.append(action)
            raise Sealed()

        fleet.monkeypatch.setattr(cas, "publish_action_request", publish)
        request = submission.request_from_mapping(row, detach=True, transport="pool")
        if path == "sealed":
            result = submission.submit_action(action,
                template={"params": action["params"], "cas": cas}, request=request, store=store)
        else:
            producer = ProducerPublication(worker_script=store.wrapper_dir / "prismabuild_worker.py",
                checkout_root=None, tags=tags, needs_gpu=True, resources=row["demand"],
                retry_safe=None, lane_root=None, job_entry=store.wrapper_dir / "slurm_job.py")
            result = submission.submit_producer(action, cas=cas, request_path=None,
                producer=producer, request=request, store=store)
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


PATHS = ["cli", "mapping", "campaign", "logical-manifest", "decompose", "sealed", "producer"]


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("tags", [["unified-host"], ["unified-alias"], [], ["arm64"]])
def test_unified_eligible_cap_is_refused_before_sealing(path, tags, fleet, tmp_path, capsys):
    with pytest.raises((SystemExit, ValueError, submission.SubmissionRefused,
                        pbcampaign.ManifestError)) as error:
        invoke(path, tags, fleet=fleet, tmp_path=tmp_path)
    diagnostic = (str(error.value) + capsys.readouterr().err).lower()
    assert "gpu_memory_gb=16 exceeds mem_gb=8" in diagnostic
    assert "subset of mem_gb" in diagnostic
    assert fleet.sealed == []


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
        params = fleet.sealed[0]["params"]
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
        assert fleet.sealed[0]["params"]["gpu_memory_gb"] == 8


@pytest.mark.parametrize("presence", ["retired", "offline", "disabled"])
def test_absent_unified_worker_does_not_constrain_discrete_fleet(presence, fleet, tmp_path):
    document = json.loads(fleet.store.roster_path.read_text())
    entry = document["boxes"]["unified-host"]
    if presence == "disabled":
        entry["loops"] = 0
    else:
        entry.update(status=presence, status_reason="fixture", status_by="test", status_unix=1)
    fleet.store.roster_path.write_text(json.dumps(document))
    result = invoke("campaign", [], fleet=fleet, tmp_path=tmp_path)
    assert result[1]["gpu_memory_gb"] == 16
    assert result[1]["demand"]["mem_gb"] == 8


@pytest.mark.parametrize("worker_args", [
    ["--gpu"],
    ["--class=gb10", "--gpu-slots=1"],
    ["--class", "gb10", "--gpu", "--tag=extra"],
])
def test_roster_worker_defaults_and_legacy_gpu_capability_are_resolved(worker_args, fleet, tmp_path):
    document = json.loads(fleet.store.roster_path.read_text())
    document["boxes"]["unified-host"]["args"] = worker_args
    fleet.store.roster_path.write_text(json.dumps(document))
    with pytest.raises(pbcampaign.ManifestError, match="subset of mem_gb"):
        invoke("campaign", [], fleet=fleet, tmp_path=tmp_path)


@pytest.mark.parametrize("path", PATHS)
def test_runtime_contract_tags_do_not_hide_unified_placement(path, fleet, tmp_path, capsys):
    with pytest.raises((SystemExit, ValueError, submission.SubmissionRefused,
                        pbcampaign.ManifestError)) as error:
        invoke(path, [contracts.CONTAINER_IMAGE_TAG], fleet=fleet, tmp_path=tmp_path)
    diagnostic = str(error.value) + capsys.readouterr().err
    assert "subset of mem_gb" in diagnostic
    assert fleet.sealed == []


@pytest.mark.parametrize("field", ["here", "measurement"])
@pytest.mark.parametrize("host,refused", [("unified-alias", True), ("discrete-host", False)])
def test_campaign_local_scope_uses_the_actual_host(field, host, refused, fleet, tmp_path):
    fleet.monkeypatch.setattr(submission.socket, "gethostname", lambda: host)
    row = {"argv": ["true"], field: True, "demand": {"gpu": 1, "mem_gb": 8},
           "gpu_memory_gb": 16}
    manifest = tmp_path / "local-manifest.json"
    manifest.write_text(json.dumps([row]))
    if refused:
        with pytest.raises(pbcampaign.ManifestError, match="subset of mem_gb"):
            pbcampaign.load_manifest(manifest, transport="pool")
    else:
        assert pbcampaign.load_manifest(manifest, transport="pool") == [row]


@pytest.mark.parametrize("path", ["campaign", "logical-manifest"])
@pytest.mark.parametrize("host", ["discrete-host", "unified-host"])
@pytest.mark.parametrize("anywhere", [False, True])
def test_manifest_cap_uses_submission_dependency_placement(path, host, anywhere, fleet, tmp_path):
    fleet.monkeypatch.setattr(submission.socket, "gethostname", lambda: host)
    row = {"argv": ["true"], "cwd": str(fleet.work),
           "demand": {"gpu": 1, "mem_gb": 8}, "gpu_memory_gb": 16,
           "env": {"OMP_NUM_THREADS": "1" if path == "logical-manifest" else 1}}
    if anywhere:
        row["anywhere"] = True
    refused = anywhere or host == "unified-host"
    request = submission.request_from_mapping(row, detach=True, transport="pool")
    if refused:
        with pytest.raises(submission.SubmissionRefused, match="subset of mem_gb"):
            submission.prepare(request, store=fleet.store)
    else:
        prepared = submission.prepare(request, store=fleet.store)
        assert prepared["template"]["params"]["placement"]["required_tags"] == [host]

    def unexpected_publication(*args, **kwargs):
        pytest.fail("manifest preflight must not freeze, seal or publish a row")

    fleet.monkeypatch.setattr(submission, "freeze_template", unexpected_publication)
    fleet.monkeypatch.setattr(submission, "submit_request", unexpected_publication)
    fleet.monkeypatch.setattr(submission.pb.PrismaBuildCAS, "publish_action_request",
                             unexpected_publication)
    if path == "campaign":
        body = [{"argv": ["true"]}, row]
    else:
        body = _request()
        body["common"].update(row, argv=["true", dc.TASK_BATCH_PLACEHOLDER])
    manifest = tmp_path / "dependency-manifest.json"
    manifest.write_text(json.dumps(body))
    if refused:
        with pytest.raises(pbcampaign.ManifestError, match="subset of mem_gb"):
            pbcampaign.load_manifest(manifest, transport="pool")
    else:
        loaded = pbcampaign.load_manifest(manifest, transport="pool")
        loaded_row = loaded[1] if path == "campaign" else loaded["common"]
        assert loaded_row == (row if path == "campaign" else body["common"])
        assert "tags" not in loaded_row
        assert "here" not in loaded_row
        assert "anywhere" not in loaded_row
    assert fleet.sealed == []


@pytest.mark.parametrize("raw", ['{"boxes":', '{"boxes": {}}'])
def test_unresolvable_fleet_fails_closed(raw, fleet, tmp_path):
    fleet.store.roster_path.write_text(raw)
    with pytest.raises(pbcampaign.ManifestError, match="cannot resolve GPU placement from fleet roster"):
        invoke("campaign", [], fleet=fleet, tmp_path=tmp_path)
    assert fleet.sealed == []
