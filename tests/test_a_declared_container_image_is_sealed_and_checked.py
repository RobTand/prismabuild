"""A declared container image is sealed, matched, and checked before a claim.

#714: an action that needed one Spark's pinned image was claimed by the other
Spark and failed inside its wrapper, spending its only attempt.  The fix has
three parts and this module holds them together: the requirement is carried by
the sealed action and its queue item, an offer that cannot positively report
the reference is not a placeable box, and a claim on a box without it denies,
names the digest, and leaves the item ready -- with no pass, no attempt and no
token spent.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import adaptive_cpu  # noqa: E402
from prismabuild import container_images  # noqa: E402
from prismabuild import core as pb, pool  # noqa: E402

KEY_A, KEY_B = "a" * 64, "b" * 64
ID_A = "sha256:" + "a" * 64
ID_B = "sha256:" + "b" * 64
REF_A = "ghcr.io/example/stage-a@sha256:" + "a" * 64

#: What sparky and sparklina really answered on 2026-09-21 (#805): the same
#: image under two image stores, plus one name each box built for itself.
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "container_images"
GLM_TAG = "prismaquant-glm-derivative:causal-exp-v1-20260908"
DIVERGENT_TAG = "prismabuild-slurm-smoke:25.11"


def _store_probe(box):
    """The bounded probe, answering one box's two recorded reads."""

    listing = (FIXTURES / f"{box}_image_ls.txt").read_bytes()
    inspected = (FIXTURES / f"{box}_image_inspect.jsonl").read_bytes()

    def probe(argv, **kwargs):
        return inspected if "inspect" in argv else listing

    return probe


def _row(box, tag):
    for line in (FIXTURES / f"{box}_image_inspect.jsonl").read_text().splitlines():
        if line and tag in (json.loads(line).get("RepoTags") or []):
            return json.loads(line)
    raise AssertionError(f"{tag} is not in the {box} fixture")


CAPACITY = {"cpu": 4, "mem_gb": 16, "gpu": 1}
#: The claim check rides the declared-image capability, so a worker that may
#: take image-pinned work is one that offers the tag (pbrun seals it into the
#: placement, and publish adds it with the references).
CLAIM_TAGS = [pb.CONTAINER_IMAGE_TAG]


def publish(queue, key, **kwargs):
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", **kwargs)


def announce(queue, host, *, tags=("gb10", pb.CONTAINER_IMAGE_TAG),
             observed_images=None, capacity=CAPACITY, gpu=False):
    queue.announce(host=host, tags=list(tags), has_gpu=gpu,
                   capacity=dict(capacity), observed_images=observed_images)


def item_of(queue, key):
    return json.loads(queue.item_path(pool.READY, key).read_text(encoding="utf-8"))


def denials(queue):
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return list(records.values())


# --------------------------------------------------------------------------
# The declaration travels with the action
# --------------------------------------------------------------------------

def test_the_item_carries_the_sealed_references_sorted_and_deduplicated(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, container_images=[REF_A, ID_A, ID_A])
    record = item_of(queue, KEY_A)
    assert record["container_images"] == [REF_A, ID_A]
    # The capability the claim check rides is added by the queue itself, so no
    # producer can publish a requirement no worker is allowed to enforce.
    assert pb.CONTAINER_IMAGE_TAG in record["tags"]


def test_publish_refuses_the_capability_without_a_requirement(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    with pytest.raises(pool.PoolContractError, match="container_images"):
        publish(queue, KEY_A, tags=[pb.CONTAINER_IMAGE_TAG])
    assert not queue.item_path(pool.READY, KEY_A).exists()


def test_a_record_requiring_the_capability_without_refs_is_denied(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1})
    path = queue.item_path(pool.READY, KEY_A)
    record = json.loads(path.read_text(encoding="utf-8"))
    record["tags"] = [*record["tags"], pb.CONTAINER_IMAGE_TAG]
    path.write_text(json.dumps(record), encoding="utf-8")
    assert queue.claim(capacity=CAPACITY, tags=CLAIM_TAGS,
                       observed_images=[ID_A]) is None
    assert denials(queue)[-1]["reason"] == "container_image_requirement_missing"
    assert path.exists()


def test_a_mutable_tag_is_refused_at_publication(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    with pytest.raises(pool.PoolContractError, match="immutable"):
        publish(queue, KEY_A, container_images=["stage-a:replicate"])
    assert not queue.item_path(pool.READY, KEY_A).exists()


def test_a_refused_publication_retires_no_withdrawal(tmp_path):
    """Validation is a precondition, not a step after retiring a decision.

    A publication that is refused must leave the authoritative withdrawal
    exactly where it was; retiring a live cancellation belongs to a
    publication that actually replaces it (#708's contract, #714 review).
    """

    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A)
    queue.withdraw(KEY_A, by="rob", reason="not needed", signal_child=False)
    marker = queue.item_path(pool.WITHDRAWN, KEY_A)
    before = marker.read_bytes()

    with pytest.raises(pool.PoolContractError, match="container_images"):
        publish(queue, KEY_A, tags=[pb.CONTAINER_IMAGE_TAG])

    assert marker.exists()
    assert marker.read_bytes() == before
    assert queue.live_withdrawal(KEY_A) is not None
    # ``withdraw`` itself files the ready source it replaced; the refusal must
    # not have retired the withdrawal itself (``_supersede_withdrawal``'s
    # capture), which is what a live cancellation losing its authority leaves
    # behind.
    assert not list(
        queue.superseded_dir().glob(f"{KEY_A}*.withdrawal-source"))
    assert not queue.item_path(pool.READY, KEY_A).exists()


def test_an_action_without_images_has_no_field_and_no_requirement(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1})
    record = item_of(queue, KEY_A)
    assert "container_images" not in record
    # And it is claimable on a box with no inventory at all: ordinary work is
    # untouched by the new field, including a Docker-less box.
    assert queue.claim(capacity=CAPACITY, observed_images=None)["action_key"] == KEY_A


def test_an_offer_reports_only_what_it_positively_observed(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    announce(queue, "seen", observed_images=[ID_A])
    announce(queue, "blind", observed_images=None)
    announce(queue, "empty", observed_images=[])
    offers = {offer["host"]: offer for offer in queue.offers()}
    assert offers["seen"]["container_images"] == [ID_A]
    assert "container_images" not in offers["blind"]
    assert offers["empty"]["container_images"] == []


# --------------------------------------------------------------------------
# Placement reads the offer's inventory, fail-closed
# --------------------------------------------------------------------------

def test_a_box_that_reports_the_image_is_placeable_and_one_that_does_not_is_not(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1},
            container_images=[REF_A], tags=["gb10", pb.CONTAINER_IMAGE_TAG])
    announce(queue, "has-it", observed_images=[REF_A])
    announce(queue, "has-none", observed_images=[])
    announce(queue, "cannot-look", observed_images=None)

    ready = queue.ready_items()[0]
    assert queue.placeable(ready) is True
    assert queue.placeable_hosts(ready) == ["has-it"]


def test_an_old_loop_without_the_capability_tag_cannot_match_even_with_the_image(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1},
            container_images=[REF_A], tags=["gb10", pb.CONTAINER_IMAGE_TAG])
    # The pre-#714 loop announces a full inventory but has no claim check.
    announce(queue, "old-loop", tags=("gb10",), observed_images=[REF_A])
    assert queue.placeable(queue.ready_items()[0]) is False


def test_a_box_with_an_unknown_inventory_is_not_a_capable_box(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1},
            container_images=[REF_A], tags=["gb10", pb.CONTAINER_IMAGE_TAG])
    announce(queue, "cannot-look", observed_images=None)
    assert queue.placeable(queue.ready_items()[0]) is False


# --------------------------------------------------------------------------
# Claim: refuse the wrong box without consuming anything
# --------------------------------------------------------------------------

def test_a_missing_image_denies_by_name_and_leaves_the_item_ready(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    ledger.ensure_capacity(CAPACITY)
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1},
            container_images=[REF_A])

    assert queue.claim(capacity=CAPACITY, tags=CLAIM_TAGS,
                       observed_images=[]) is None
    denial = denials(queue)[-1]
    assert denial["reason"] == "container_image_absent"
    assert denial["evidence"]["absent"] == [REF_A]
    # No pass, no attempt, no reservation: this box simply cannot run it, and
    # the item stays ready for the one that can.
    assert queue.passes(KEY_A) == 0
    record = item_of(queue, KEY_A)
    assert record["attempts"] == 0
    assert ledger.held() == {}
    assert queue.claim(capacity=CAPACITY, tags=CLAIM_TAGS,
                       observed_images=[REF_A])["action_key"] == KEY_A


def test_an_action_sealed_on_one_image_store_is_claimed_from_the_other(tmp_path):
    """#805, end to end, on what the two Sparks really reported.

    The campaign image is on both boxes.  sparky's containerd store and
    sparklina's classic store name it differently, so an ID-sealed action is
    claimable by one Spark only; sealed on the content reference, either
    box's own inventory claims it and a box holding a *different* image of
    the same name still denies.
    """

    sparky = container_images.observe(probe=_store_probe("sparky"))
    sparklina = container_images.observe(probe=_store_probe("sparklina"))
    assert sparky is not None and sparklina is not None

    sparky_id = _row("sparky", GLM_TAG)["Id"]
    portable = container_images.content_ref(_row("sparky", GLM_TAG))

    # The reported failure, preserved: sealed on sparky's ID, sparklina
    # denies although it holds the image.
    queue = pool.PoolQueue(tmp_path / "by-id")
    queue.ledger().ensure_capacity(CAPACITY)
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1},
            container_images=[sparky_id])
    assert queue.claim(capacity=CAPACITY, tags=CLAIM_TAGS,
                       observed_images=sorted(sparklina)) is None
    assert denials(queue)[-1]["reason"] == "container_image_absent"

    # Sealed on the content reference, both boxes claim it.
    for index, observed in enumerate((sparky, sparklina)):
        portable_queue = pool.PoolQueue(tmp_path / f"by-content-{index}")
        portable_queue.ledger().ensure_capacity(CAPACITY)
        publish(portable_queue, KEY_A, resources={"cpu": 1, "mem_gb": 1},
                container_images=[portable])
        claimed = portable_queue.claim(capacity=CAPACITY, tags=CLAIM_TAGS,
                                       observed_images=sorted(observed))
        assert claimed is not None and claimed["action_key"] == KEY_A

    # And it is still a requirement, not a wildcard: neither box holds the
    # other's build of ``prismabuild-slurm-smoke:25.11``.
    divergent = container_images.content_ref(_row("sparky", DIVERGENT_TAG))
    refusing = pool.PoolQueue(tmp_path / "divergent")
    refusing.ledger().ensure_capacity(CAPACITY)
    publish(refusing, KEY_B, resources={"cpu": 1, "mem_gb": 1},
            container_images=[divergent])
    assert refusing.claim(capacity=CAPACITY, tags=CLAIM_TAGS,
                          observed_images=sorted(sparklina)) is None
    assert denials(refusing)[-1]["reason"] == "container_image_absent"
    assert refusing.claim(capacity=CAPACITY, tags=CLAIM_TAGS,
                          observed_images=sorted(sparky))["action_key"] == KEY_B


def test_a_content_sealed_action_is_placeable_on_either_store(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    sparky = container_images.observe(probe=_store_probe("sparky"))
    sparklina = container_images.observe(probe=_store_probe("sparklina"))
    portable = container_images.content_ref(_row("sparky", GLM_TAG))
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1},
            container_images=[portable], tags=["gb10", pb.CONTAINER_IMAGE_TAG])
    announce(queue, "sparky", observed_images=sorted(sparky))
    announce(queue, "sparklina", observed_images=sorted(sparklina))
    ready = queue.ready_items()[0]
    assert queue.placeable(ready) is True
    assert queue.placeable_hosts(ready) == ["sparklina", "sparky"]


def test_an_unknown_inventory_denies_fail_closed(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1},
            container_images=[REF_A])
    assert queue.claim(capacity=CAPACITY, tags=CLAIM_TAGS,
                       observed_images=None) is None
    denial = denials(queue)[-1]
    assert denial["reason"] == "container_image_presence_unknown"
    assert denial["evidence"]["required"] == [REF_A]
    assert queue.item_path(pool.READY, KEY_A).exists()
    assert queue.passes(KEY_A) == 0


def test_a_malformed_item_field_is_a_denial_not_a_poll_failure(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1})
    path = queue.item_path(pool.READY, KEY_A)
    record = json.loads(path.read_text(encoding="utf-8"))
    record["container_images"] = "sha256:" + "a" * 64      # not a list
    path.write_text(json.dumps(record), encoding="utf-8")
    assert queue.claim(capacity=CAPACITY, observed_images=[ID_A]) is None
    assert denials(queue)[-1]["reason"] == "malformed_container_images"


def test_presence_does_not_bypass_a_resource_gate(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1, "gpu": 1},
            container_images=[REF_A])
    # Image present, no GPU capacity: the ordinary never-fits refusal still
    # decides, and the item is not claimed.
    assert queue.claim(capacity={"cpu": 4, "mem_gb": 16}, has_gpu=True,
                       tags=CLAIM_TAGS, observed_images=[REF_A]) is None
    assert queue.item_path(pool.READY, KEY_A).exists()
    assert queue.claim(capacity=CAPACITY, has_gpu=True, tags=CLAIM_TAGS,
                       observed_images=[REF_A])["action_key"] == KEY_A


def test_one_box_denying_leaves_the_item_for_the_box_that_has_it(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1},
            container_images=[REF_A])
    # The wrong box polls first and refuses...
    assert queue.claim(capacity=CAPACITY, tags=CLAIM_TAGS,
                       observed_images=[ID_B]) is None
    # ...and the right box, on its next pass, takes it.
    assert queue.claim(capacity=CAPACITY, tags=CLAIM_TAGS,
                       observed_images=[ID_A, REF_A])["action_key"] == KEY_A


def test_a_stale_positive_is_reported_as_a_missing_reference(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1},
            container_images=[REF_A, ID_B])
    assert queue.claim(capacity=CAPACITY, tags=CLAIM_TAGS,
                       observed_images=[REF_A]) is None
    denial = denials(queue)[-1]
    assert denial["reason"] == "container_image_absent"
    assert denial["evidence"]["absent"] == [ID_B]
