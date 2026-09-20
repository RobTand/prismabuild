"""``--container-image``: sealed, placed, owner-identified and campaign-able.

The four properties this module pins are the review's (#714): the requirement
is sealed into the action and its queue row rather than parsed from argv, a
dispatch into a fleet that cannot report the reference is refused instead of
queued, two actions that differ only in the image they require never share a
Docker ownership id, and a row (or any other producer) forwarding the field
reaches the same submission a hand-typed ``pbrun`` would.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import container_images as ci  # noqa: E402
from prismabuild import core as pb, pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import fleet_submit  # noqa: E402
import pbcampaign  # noqa: E402
import pbrun  # noqa: E402

from test_pbrun_detach import _checkout, _one_json_line, _queue, _run_pbrun  # noqa: E402

ID_A = "sha256:" + "a" * 64
ID_B = "sha256:" + "b" * 64
REF_A = "ghcr.io/example/stage-a@sha256:" + "a" * 64
REF_B = "ghcr.io/example/stage-b@sha256:" + "b" * 64

CAPABILITY = pb.CONTAINER_IMAGE_TAG


def _offer_images(queue, *images):
    queue.announce(
        host="sparky", tags=["sparky", "gb10", CAPABILITY], has_gpu=True,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
        observed_images=list(images),
    )


def _sealed_request(tmp_path: Path, key: str) -> dict:
    matches = list((tmp_path / "cas" / "requests").glob(f"*/{key}.json"))
    assert len(matches) == 1, matches
    return json.loads(matches[0].read_text(encoding="utf-8"))


def _item(tmp_path: Path, key: str) -> dict:
    path = tmp_path / "pb-queue" / pool.READY / f"{key}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _owner(action: dict) -> str:
    return action["environment"]["variables"][pbrun.CONTAINER_OWNER_ENV]


# --------------------------------------------------------------------------
# Declaration and dispatch
# --------------------------------------------------------------------------

def test_an_offer_reporting_the_image_places_the_submission(tmp_path, monkeypatch, capsys):
    work = _checkout(tmp_path)
    queue = _queue(tmp_path)
    _offer_images(queue, REF_A)

    assert _run_pbrun(
        tmp_path, monkeypatch, work, "--detach",
        "--container-image", REF_A) == 0
    key = _one_json_line(capsys.readouterr())["action_key"]

    item = _item(tmp_path, key)
    assert item["container_images"] == [REF_A]
    assert CAPABILITY in item["tags"]
    ready = next(record for record in queue.ready_items()
                 if record["action_key"] == key)
    assert queue.placeable(ready) is True


def test_an_offer_that_does_not_report_the_image_refuses_at_dispatch(
    tmp_path, monkeypatch,
):
    work = _checkout(tmp_path)
    queue = _queue(tmp_path)              # tags only: no inventory announced
    with pytest.raises(SystemExit) as exc:
        _run_pbrun(tmp_path, monkeypatch, work, "--detach",
                   "--container-image", REF_A)
    message = str(exc.value)
    assert REF_A in message
    assert "no recorded worker" in message
    # Nothing was sealed into the queue.
    assert queue.ready_items() == []


def test_no_offers_at_all_refuses_an_image_declaration_fail_closed(
    tmp_path, monkeypatch,
):
    work = _checkout(tmp_path)
    with pytest.raises(SystemExit) as exc:
        _run_pbrun(tmp_path, monkeypatch, work, "--detach",
                   "--container-image", REF_A)
    assert REF_A in str(exc.value)
    assert "no worker offers on record" in str(exc.value)
    assert not list((tmp_path / "pb-queue" / pool.READY).glob("*.json"))


def test_a_mutable_tag_is_an_argument_error(tmp_path, monkeypatch, capsys):
    work = _checkout(tmp_path)
    with pytest.raises(SystemExit) as exc:
        _run_pbrun(tmp_path, monkeypatch, work, "--detach",
                   "--container-image", "stage-a:latest")
    assert exc.value.code == 2
    assert "immutable" in capsys.readouterr().err
    assert not list((tmp_path / "pb-queue" / pool.READY).glob("*.json"))


def test_the_slurm_lane_cannot_declare_an_image(tmp_path, monkeypatch, capsys):
    work = _checkout(tmp_path)
    with pytest.raises(SystemExit) as exc:
        _run_pbrun(tmp_path, monkeypatch, work, "--detach",
                   "--transport", "slurm", "--container-image", REF_A)
    assert exc.value.code == 2
    assert "SLURM" in capsys.readouterr().err


def test_the_notice_names_the_capability_the_boxes_lack(tmp_path, capsys):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(host="new-box", tags=["gb10", CAPABILITY], has_gpu=False,
                   capacity={"cpu": 4, "mem_gb": 16}, observed_images=[REF_A])
    queue.announce(host="old-box", tags=["gb10"], has_gpu=False,
                   capacity={"cpu": 4, "mem_gb": 16})
    notice = pbrun.container_image_notice(queue, {
        "tags": ["gb10", CAPABILITY], "needs_gpu": False,
        "resources": {"cpu": 1, "mem_gb": 1}, "container_images": [REF_A],
    })
    assert "reported on record by: new-box" in notice
    assert f"not offering {CAPABILITY}: old-box" in notice


# --------------------------------------------------------------------------
# Identity: the declaration must move the owner, not only the key
# --------------------------------------------------------------------------

def _submit_images(tmp_path, monkeypatch, capsys, work, *images):
    options = ["--detach"]
    for image in images:
        options += ["--container-image", image]
    assert _run_pbrun(tmp_path, monkeypatch, work, *options) == 0
    return _one_json_line(capsys.readouterr())["action_key"]


def test_two_images_never_share_a_docker_owner_or_action_key(
    tmp_path, monkeypatch, capsys,
):
    # Each submission gets its own private fleet so the CAS request it names is
    # unambiguous.
    owners = {}
    keys = {}
    for image in (REF_A, REF_B):
        fleet = tmp_path / ("fleet-" + image[-1])
        fleet.mkdir()
        work = _checkout(fleet)
        _offer_images(_queue(fleet), REF_A, REF_B)
        key = _submit_images(fleet, monkeypatch, capsys, work, image)
        request = _sealed_request(fleet, key)
        assert request["action_key"] == key
        keys[image] = key
        owners[image] = _owner(request)
    assert keys[REF_A] != keys[REF_B]
    assert owners[REF_A] != owners[REF_B]


def test_reordered_and_duplicated_references_seal_one_key(
    tmp_path, monkeypatch, capsys,
):
    work = _checkout(tmp_path)
    _offer_images(_queue(tmp_path), REF_A, ID_B)
    first = _submit_images(tmp_path, monkeypatch, capsys, work, REF_A, ID_B)
    second = _submit_images(tmp_path, monkeypatch, capsys, work,
                            ID_B, REF_A, REF_A)
    assert first == second


def test_container_owner_includes_images_only_when_declared(tmp_path):
    common = dict(
        command=["true"], cwd=tmp_path, demand={"cpu": 1}, variables={},
        determinism="stochastic", retry_policy={"max_attempts": 1},
        marker_root=tmp_path / "markers", identity="checkout",
        logical_cwd=".", placement={"required_tags": []},
    )
    legacy = pbrun.container_owner(**common)
    empty = pbrun.container_owner(**common, container_images=[])
    with_a = pbrun.container_owner(**common, container_images=[REF_A])
    with_b = pbrun.container_owner(**common, container_images=[REF_B])
    assert legacy == empty
    assert with_a != with_b != legacy


def test_a_submission_without_images_keeps_its_legacy_shape(tmp_path, monkeypatch):
    work = _checkout(tmp_path)
    queue = _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach") == 0
    key = json.loads(next((tmp_path / "pb-queue" / pool.READY).glob("*.json"))
                     .read_text())["action_key"]
    request = _sealed_request(tmp_path, key)
    assert "container_images" not in request["params"]
    item = _item(tmp_path, key)
    assert "container_images" not in item
    assert CAPABILITY not in item["tags"]
    # Re-running the identical submission answers with the same key.
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach") == 0
    again = json.loads(next((tmp_path / "pb-queue" / pool.READY).glob("*.json"))
                       .read_text())["action_key"]
    assert again == key


# --------------------------------------------------------------------------
# Rows, producers and the sealed request
# --------------------------------------------------------------------------

def test_a_campaign_row_forwards_the_declaration() -> None:
    assert pbcampaign.pbrun_argv({
        "argv": ["python3", "-m", "mypkg.stage"],
        "container_images": [REF_A, ID_B],
    }) == ["--container-image", REF_A, "--container-image", ID_B,
           "--", "python3", "-m", "mypkg.stage"]


def test_a_manifest_row_is_refused_before_anything_is_sealed(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps([{
        "argv": ["/bin/true"], "container_images": ["stage-a:latest"],
    }]), encoding="utf-8")
    with pytest.raises(pbcampaign.ManifestError) as exc:
        pbcampaign.load_manifest(manifest, transport="pool")
    assert "immutable" in str(exc.value)

    manifest.write_text(json.dumps([{
        "argv": ["/bin/true"], "container_images": [REF_A],
    }]), encoding="utf-8")
    with pytest.raises(pbcampaign.ManifestError) as exc:
        pbcampaign.load_manifest(manifest, transport="slurm")
    assert "SLURM" in str(exc.value)
    assert pbcampaign.load_manifest(manifest, transport="pool")[0][
        "container_images"] == [REF_A]


def test_a_producer_submitting_a_sealed_action_keeps_the_requirement(tmp_path):
    action = {"action_key": "c" * 64, "params": {"container_images": [REF_A]}}
    fleet_submit.submit(
        action, cas=SimpleNamespace(root=tmp_path / "cas"),
        request_path=tmp_path / "request.json", transport="pool",
        worker_script=tmp_path / "worker.py", checkout_root=tmp_path / "co",
        tags=["gb10"], resources={"cpu": 1, "mem_gb": 1},
        queue_root=tmp_path / "pb-queue",
    )
    item = json.loads(
        (tmp_path / "pb-queue" / pool.READY / f"{'c' * 64}.json").read_text())
    assert item["container_images"] == [REF_A]
    assert CAPABILITY in item["tags"]


def test_a_producer_cannot_smuggle_an_image_requirement_onto_slurm(tmp_path):
    action = {"action_key": "c" * 64, "params": {"container_images": [REF_A]}}
    with pytest.raises(fleet_submit.SubmitRefused, match="SLURM"):
        fleet_submit.submit(
            action, cas=SimpleNamespace(root=tmp_path / "cas"),
            request_path=tmp_path / "request.json", transport="slurm",
            worker_script=tmp_path / "worker.py",
            checkout_root=tmp_path / "co", tags=["gb10"],
            resources={"cpu": 1, "mem_gb": 1},
            queue_root=tmp_path / "pb-queue",
        )
