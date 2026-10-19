"""Capability decisions through publication, advisory placement and live claims."""
import hashlib
import json

import pytest

from prismabuild import capability_eligibility as eligibility, contracts, pool


@pytest.mark.parametrize("capacity", [None, {"cpu": 2, "mem_gb": 2}])
def test_direct_gpu_publication_requires_a_gpu_host_without_rewriting_the_row(tmp_path, capacity):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.announce(host="cpu-only", tags=[], has_gpu=False,
                   capacity={"cpu": 2, "mem_gb": 2})
    queue.publish(action_key="a" * 64, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", resources={"cpu": 1, "gpu": 1})
    path = queue.records.item_path(pool.READY, "a" * 64)
    before = path.read_bytes()
    row = json.loads(before)
    assert row["needs_gpu"] is False
    assert row["resources"] == {"cpu": 1, "gpu": 1}
    assert queue.placeable_hosts(row) == []

    assert queue.claim(has_gpu=False, capacity=capacity) is None

    assert path.read_bytes() == before
    denials = queue.latest_denials({"a" * 64})["a" * 64]
    assert denials[0]["reason"] == "placement_mismatch"
    assert not queue.records.lease_path("a" * 64).exists()
    assert queue.passes("a" * 64) == 0
    assert queue.ledger().held_keys() == []

    queue.announce(host="gpu-host", tags=[], has_gpu=True,
                   capacity={"cpu": 2, "gpu": 1, "mem_gb": 2})
    assert queue.placeable_hosts(row) == ["gpu-host"]
    claimed = queue.claim(has_gpu=True, capacity={"cpu": 2, "gpu": 1, "mem_gb": 2})
    assert claimed["action_key"] == "a" * 64
    assert claimed["needs_gpu"] is False
    assert claimed["resources"] == {"cpu": 1, "gpu": 1}
    queue.finish("a" * 64, status="executed", claim_snapshot=claimed)
    assert queue.ledger().held_keys() == []


@pytest.mark.parametrize("field", ["local_dependencies", "dependency_queries"])
@pytest.mark.parametrize("value", [[], False, 0, ""])
def test_falsey_nonmapping_declarations_are_refused_before_publication(tmp_path, field, value):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.publish(action_key="a" * 64, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", resources={"cpu": 1})
    queue.withdraw("a" * 64, reason="retained declaration refusal control")
    withdrawn = queue.records.item_path(pool.WITHDRAWN, "a" * 64)
    before = withdrawn.read_bytes()
    with pytest.raises(pool.PoolContractError, match="path-to-kind mapping"):
        queue.publish(action_key="a" * 64, cas_root="/cas", checkout_root="/checkout",
                      worker_script="/worker.py", resources={"cpu": 1}, **{field: value})
    assert withdrawn.read_bytes() == before
    assert not queue.records.item_path(pool.READY, "a" * 64).exists()
    assert not queue.records.item_path(pool.CLAIMED, "a" * 64).exists()
    assert not queue.records.lease_path("a" * 64).exists()

    queue.publish(action_key="b" * 64, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", resources={"cpu": 1}, **{field: {}})
    claimed = queue.claim(capacity={"cpu": 1})
    assert claimed["action_key"] == "b" * 64
    queue.finish("b" * 64, status="executed", claim_snapshot=claimed)
    assert queue.ledger().held_keys() == []


@pytest.mark.parametrize("field,requirements,tags", [
    ("interpreters", {"interpreter": "/fixture/python"}, [contracts.INTERPRETER_TAG]),
    ("container_images", {"container_images": ["content:sha256:" + "a" * 64]},
     [contracts.CONTAINER_IMAGE_TAG]),
    ("dependency_files", {"requires_files": [{"path": "/fixture/data", "sha256": "a" * 64}]},
     [contracts.DEPENDENCY_DIGEST_TAG]),
])
def test_a_malformed_advertisement_cannot_supply_positive_presence(tmp_path, field, requirements, tags):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.announce(host="advertiser", tags=tags, has_gpu=False, capacity={"cpu": 1})
    path = queue.root / pool.WORKERS / "advertiser.json"
    record = json.loads(path.read_text())
    named = {"interpreters": "/fixture/python", "container_images": "content:sha256:" + "a" * 64,
             "dependency_files": "/fixture/data"}[field]
    record[field] = [named, 7]
    path.write_text(json.dumps(record))

    assert queue.placeable_hosts({"tags": tags, "resources": {"cpu": 1}, **requirements}) == []

    record[field] = [named]
    path.write_text(json.dumps(record))
    assert queue.placeable_hosts({"tags": tags, "resources": {"cpu": 1}, **requirements}) == ["advertiser"]


def test_a_malformed_gpu_image_requirement_keeps_no_room_for_a_busy_row(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    row = {"action_key": "a" * 64, "resources": {"cpu": 1, "gpu": 1}, "container_images": [7]}

    assert queue._ready_gpu_row_room(
        row, ledger=queue.ledger(), total={"cpu": 2, "gpu": 1}, controller=None,
        gpu_controller=None, observed_images=["7"]) is None

    row["container_images"] = ["content:sha256:" + "a" * 64]
    room = queue._ready_gpu_row_room(
        row, ledger=queue.ledger(), total={"cpu": 2, "gpu": 1}, controller=None,
        gpu_controller=None, observed_images=["content:sha256:" + "a" * 64])
    assert room == {"action_key": "a" * 64, "room": {"cpu": 1, "gpu": 1}}


@pytest.mark.parametrize("fault,reason", [
    (None, None),
    ("interpreter", "interpreter_not_present"),
    ("local", "local_dependency_not_present"),
    ("file", "dependency_not_present"),
    ("bytes", "dependency_digest_mismatch"),
    ("images", "container_image_absent"),
])
def test_combined_offer_and_claim_use_current_evidence_at_each_capability_gate(tmp_path, fault, reason):
    interpreter = tmp_path / "python"
    interpreter.write_text("#!/bin/sh\n")
    interpreter.chmod(0o700)
    local = tmp_path / "local"
    local.write_text("local")
    dependency = tmp_path / "dependency"
    dependency.write_bytes(b"pinned")
    image = "content:sha256:" + "a" * 64
    tags = [contracts.INTERPRETER_TAG, contracts.LOCAL_DEPENDENCY_TAG,
            contracts.DEPENDENCY_DIGEST_TAG, contracts.CONTAINER_IMAGE_TAG]
    declarations = {"interpreter": str(interpreter), "local_dependencies": {str(local): "path"},
                    "requires_files": [{"path": str(dependency), "sha256": hashlib.sha256(b"pinned").hexdigest()}],
                    "container_images": [image]}
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.announce(host="observed", tags=tags, has_gpu=False, capacity={"cpu": 1},
                   capabilities=eligibility.OfferCapabilities(
                       interpreters=eligibility.observe_interpreters([declarations]),
                       dependencies=eligibility.observe_dependencies([declarations], python=str(interpreter)),
                       images=eligibility.ImageAnswers.from_observed([image], None)))
    queue.publish(action_key="a" * 64, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", resources={"cpu": 1}, **declarations)
    path = queue.records.item_path(pool.READY, "a" * 64)
    before = path.read_bytes()
    references = [image]
    if fault == "interpreter":
        interpreter.unlink()
    elif fault == "local":
        local.unlink()
    elif fault == "file":
        dependency.unlink()
    elif fault == "bytes":
        dependency.write_bytes(b"changed")
    elif fault == "images":
        references.clear()

    assert queue.placeable_hosts(json.loads(before)) == ["observed"]
    claimed = queue.claim(tags=tags, capacity={"cpu": 1}, observed_images=references)

    assert (claimed is not None) is (fault is None)
    if reason is not None:
        assert queue.latest_denials({"a" * 64})["a" * 64][0]["reason"] == reason
        assert path.read_bytes() == before
        assert queue.passes("a" * 64) == 0
        assert queue.ledger().held_keys() == []
        assert not queue.records.lease_path("a" * 64).exists()


@pytest.mark.parametrize("corruption", [
    {"interpreter": 7},
    {"container_images": [7]},
    {"local_dependencies": []},
    {"requires_files": [{"path": 7}]},
])
def test_census_distinguishes_malformed_declarations_from_an_unplaceable_row(tmp_path, corruption):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.announce(host="observed", tags=[], has_gpu=False, capacity={"cpu": 1})
    queue.publish(action_key="a" * 64, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", resources={"cpu": 1})
    path = queue.records.item_path(pool.READY, "a" * 64)
    row = json.loads(path.read_text())
    row.update(corruption)
    path.write_text(json.dumps(row))

    census = queue.placement_census()

    assert census["unreadable"] == 1
    assert census["unplaceable"] == 0
    assert path.is_file()
    assert not queue.records.lease_path("a" * 64).exists()


@pytest.mark.parametrize("gpu", ["false", 1, None])
def test_gpu_presence_in_an_offer_requires_a_boolean_observation(tmp_path, gpu):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.announce(host="observed", tags=[], has_gpu=True, capacity={"gpu": 1})
    path = queue.root / pool.WORKERS / "observed.json"
    offer = json.loads(path.read_text())
    offer["has_gpu"] = gpu
    path.write_text(json.dumps(offer))
    item = {"tags": [], "resources": {"gpu": 1}}

    assert queue.placeable_hosts(item) == []

    offer["has_gpu"] = True
    path.write_text(json.dumps(offer))
    assert queue.placeable_hosts(item) == ["observed"]


def test_a_malformed_tag_advertisement_cannot_supply_code_support(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.announce(host="observed", tags=[contracts.INTERPRETER_TAG], has_gpu=False,
                   capabilities=eligibility.OfferCapabilities(
                       interpreters=eligibility.InterpreterAnswers.from_observed(["/fixture/python"], None)))
    path = queue.root / pool.WORKERS / "observed.json"
    offer = json.loads(path.read_text())
    offer["tags"].append(7)
    path.write_text(json.dumps(offer))

    assert queue.placeable_hosts({"tags": [contracts.INTERPRETER_TAG], "interpreter": "/fixture/python"}) == []


@pytest.mark.parametrize("value", [{}, False, 0, ""])
def test_falsey_malformed_images_cannot_cross_actual_offer_or_claim_ingress(tmp_path, value):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.announce(host="observer", tags=[], has_gpu=False, capacity={"cpu": 1})
    queue.publish(action_key="a" * 64, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", resources={"cpu": 1})
    path = queue.records.item_path(pool.READY, "a" * 64)
    row = json.loads(path.read_text())
    row["container_images"] = value
    path.write_text(json.dumps(row))
    before = path.read_bytes()

    with pytest.raises(ValueError, match="container_images"):
        queue.placeable_hosts(row)
    assert queue.claim(capacity={"cpu": 1}) is None

    assert queue.latest_denials({"a" * 64})["a" * 64][0]["reason"] == "malformed_container_images"
    assert path.read_bytes() == before
    assert not queue.records.lease_path("a" * 64).exists()
    assert queue.passes("a" * 64) == 0
    assert queue.ledger().held_keys() == []

    row["container_images"] = []
    path.write_text(json.dumps(row))
    assert queue.placeable_hosts(row) == ["observer"]
    claimed = queue.claim(capacity={"cpu": 1})
    assert claimed["action_key"] == "a" * 64
    queue.finish("a" * 64, status="executed", claim_snapshot=claimed)
    assert queue.ledger().held_keys() == []


@pytest.mark.parametrize("tag,reason", [
    (contracts.INTERPRETER_TAG, "interpreter_requirement_missing"),
    (contracts.LOCAL_DEPENDENCY_TAG, "local_dependencies_unavailable"),
    (contracts.DEPENDENCY_DIGEST_TAG, "dependency_requirement_missing"),
    (contracts.CONTAINER_IMAGE_TAG, "container_image_requirement_missing"),
])
def test_a_code_tag_without_its_declaration_cannot_authorize_a_claim(tmp_path, tag, reason):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.announce(host="advertiser", tags=[tag], has_gpu=False, capacity={"cpu": 1})
    with pytest.raises(pool.PoolContractError, match="requires"):
        queue.publish(action_key="b" * 64, cas_root="/cas", checkout_root="/checkout",
                      worker_script="/worker.py", tags=[tag], resources={"cpu": 1})
    queue.publish(action_key="a" * 64, cas_root="/cas", checkout_root="/checkout",
                  worker_script="/worker.py", resources={"cpu": 1})
    path = queue.records.item_path(pool.READY, "a" * 64)
    row = json.loads(path.read_text())
    row["tags"] = [tag]
    path.write_text(json.dumps(row))
    before = path.read_bytes()

    assert queue.claim(tags=[tag], capacity={"cpu": 1}) is None
    assert queue.placeable_hosts(row) == []

    assert queue.latest_denials({"a" * 64})["a" * 64][0]["reason"] == reason
    assert path.read_bytes() == before
    assert not queue.records.lease_path("a" * 64).exists()
    assert queue.passes("a" * 64) == 0
    assert queue.ledger().held_keys() == []

    row["tags"] = []
    path.write_text(json.dumps(row))
    assert queue.placeable_hosts(row) == ["advertiser"]
    claimed = queue.claim(capacity={"cpu": 1})
    assert claimed["action_key"] == "a" * 64
    queue.finish("a" * 64, status="executed", claim_snapshot=claimed)
    assert queue.ledger().held_keys() == []


def test_a_malformed_tag_advertisement_is_not_positive_class_membership_evidence(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.announce(host="member", tags=["fixture-class"], has_gpu=False, capacity={"cpu": 1})
    path = queue.root / pool.WORKERS / "member.json"
    record = json.loads(path.read_text())
    record["tags"] = ["fixture-class", 7]
    path.write_text(json.dumps(record))
    row = {"tags": ["fixture-class"], "resources": {"cpu": 1}}

    assert queue.placeable_hosts(row) == []
    assert queue.offered_tags() == []
    assert queue.class_dependency_gap("fixture-class", {"member": ["member"]}, {}) == (
        "member: missing or ambiguous fresh fixture-class offer")

    record["tags"] = ["fixture-class"]
    path.write_text(json.dumps(record))
    assert queue.class_dependency_gap("fixture-class", {"member": ["member"]}, {}) is None
    assert queue.placeable_hosts(row) == ["member"]
    assert queue.offered_tags() == ["fixture-class"]
