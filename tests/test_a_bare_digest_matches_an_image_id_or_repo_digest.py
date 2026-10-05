"""A bare ``sha256:`` requirement is satisfied by an image ID or a RepoDigest.

The live GPU row ``fd9ca6b5...`` declares the bare digest
``sha256:5be13705...`` and could only ever place on sparky: sparky's
containerd store reports that digest as the image's ID, while sparklina's
classic store calls the same image ``sha256:acc684c0...`` and holds the
registry digest only inside ``localhost/prismaquant/spark-vllm-nccl230@
sha256:5be13705...``.  Exact membership therefore read the two-store fact of
#805 -- one image, two IDs -- as absence, and the row's gang could never
gather.  Decided 2026-10-05: a bare digest requirement is satisfied by the
image ID itself *or* by the digest part of any RepoDigest, because a
repository-qualified digest is content-addressed -- the same 64 hex under any
repository name is the same bytes.  The qualified forms stay exact: a
``repository@sha256:`` requirement is never satisfied by a bare ID, and
``content:`` matches content only.
"""
from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import adaptive_cpu  # noqa: E402
from prismabuild import container_images  # noqa: E402
from prismabuild import core as pb, pool  # noqa: E402

#: What the live row declares, and what the two stores really reported for
#: the same image (measured 2026-10-05 on sparky and sparklina).
BARE = "sha256:5be13705acaecc7b4aaf342a84f80d67844c9970ff8375bf9fbeecc9c98ce84a"
BARE_HEX = BARE[len("sha256:"):]
REPO_DIGEST = "localhost/prismaquant/spark-vllm-nccl230@" + BARE
OTHER_REPO_DIGEST = "ghcr.io/prismaquant/spark-vllm-nccl230@" + BARE

SPARKY_SET = [BARE, REPO_DIGEST]
SPARKLINA_SET = [
    "sha256:acc684c06ec79aa303deda50a15c422084690d3252d2449e9702b40de571fe63",
    REPO_DIGEST,
]

KEY_A = "a" * 64
CAPACITY = {"cpu": 4, "mem_gb": 16, "gpu": 1}
CLAIM_TAGS = [pb.CONTAINER_IMAGE_TAG]


def publish(queue, key, **kwargs):
    queue.publish(action_key=key, cas_root="/cas", checkout_root="/co",
                  worker_script="/w.py", **kwargs)


def announce(queue, host, *, tags=("gb10", pb.CONTAINER_IMAGE_TAG),
             observed_images=None, capacity=CAPACITY, gpu=False):
    queue.announce(host=host, tags=list(tags), has_gpu=gpu,
                   capacity=dict(capacity), observed_images=observed_images)


def item_of(queue, key):
    return queue.item_path(pool.READY, key).read_text(encoding="utf-8")


def denials(queue):
    path = adaptive_cpu.local_state_base(queue.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    return list(records.values())


# --------------------------------------------------------------------------
# The shared predicate, on the two stores' real answers
# --------------------------------------------------------------------------

def test_the_live_row_is_satisfied_by_either_stores_inventory():
    """(a) The two-store case, unit-sized.

    sparky holds the digest as the image's ID; sparklina holds it only as
    the digest part of a RepoDigest.  Both satisfy the row's declaration.
    """

    assert container_images.missing([BARE], SPARKY_SET) == ()
    assert container_images.missing([BARE], SPARKLINA_SET) == ()
    assert container_images.satisfied(BARE, SPARKLINA_SET) is True


def test_the_placement_surface_matches_on_both_stores(tmp_path):
    """(b) The offer filter: both boxes are placeable, not sparky alone.

    This is the filter the live gang stalls behind: sparklina's offer names
    its own store ID plus the RepoDigest, and only the RepoDigest carries
    the declared digest.
    """

    queue = pool.PoolQueue(tmp_path / "queue")
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1},
            container_images=[BARE], tags=["gb10", pb.CONTAINER_IMAGE_TAG])
    announce(queue, "sparky", observed_images=list(SPARKY_SET))
    announce(queue, "sparklina", observed_images=list(SPARKLINA_SET))
    ready = queue.ready_items()[0]
    assert queue.placeable(ready) is True
    assert queue.placeable_hosts(ready) == ["sparklina", "sparky"]


def test_the_claim_is_admitted_on_the_store_that_holds_only_the_repo_digest(tmp_path):
    """(c) The claim path admits sparklina and still denies a true absence."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ledger().ensure_capacity(CAPACITY)
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1},
            container_images=[BARE])
    claimed = queue.claim(capacity=CAPACITY, tags=CLAIM_TAGS,
                          observed_images=sorted(SPARKLINA_SET))
    assert claimed is not None and claimed["action_key"] == KEY_A


def test_a_box_holding_neither_the_id_nor_the_repo_digest_still_denies(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ledger().ensure_capacity(CAPACITY)
    publish(queue, KEY_A, resources={"cpu": 1, "mem_gb": 1},
            container_images=[BARE])
    # sparklina's own store ID alone: the image it names is a different
    # content-addressed object, so the requirement is absent, by name.
    assert queue.claim(capacity=CAPACITY, tags=CLAIM_TAGS,
                       observed_images=[SPARKLINA_SET[0]]) is None
    denial = denials(queue)[-1]
    assert denial["reason"] == "container_image_absent"
    assert denial["evidence"]["absent"] == [BARE]
    assert queue.item_path(pool.READY, KEY_A).exists()


# --------------------------------------------------------------------------
# The predicate stays a full-string, shape-checked match
# --------------------------------------------------------------------------

def test_a_repo_digest_of_different_hex_does_not_satisfy():
    assert container_images.missing(
        [BARE],
        ["localhost/prismaquant/spark-vllm-nccl230@sha256:" + "e" * 64],
    ) == (BARE,)


def test_a_prefix_a_suffix_or_a_bare_hex_does_not_satisfy():
    assert container_images.missing([BARE], ["sha256:" + BARE_HEX[:63]]) == (BARE,)
    assert container_images.missing([BARE], [BARE + "0"]) == (BARE,)
    assert container_images.missing(
        [BARE], ["localhost/prismaquant/spark-vllm-nccl230@" + BARE + "0"],
    ) == (BARE,)
    assert container_images.missing([BARE], [BARE_HEX]) == (BARE,)


def test_a_hex_inside_an_unrelated_string_does_not_satisfy():
    assert container_images.missing([BARE], ["spark-vllm-nccl230:" + BARE_HEX]) == (BARE,)
    assert container_images.missing(
        [BARE], ["nightly-" + BARE_HEX + "-built"]) == (BARE,)


def test_the_qualified_forms_keep_their_exact_matches():
    """The reverse direction and ``content:`` are unchanged."""

    # A repository-qualified requirement is never satisfied by a bare ID,
    # whatever the hex.
    assert container_images.missing([REPO_DIGEST], [BARE]) == (REPO_DIGEST,)
    assert container_images.satisfied(REPO_DIGEST, [BARE]) is False
    # ``content:`` matches content only, on both stores' inventories.
    content = "content:sha256:" + BARE_HEX
    assert container_images.missing([content], SPARKY_SET) == (content,)
    assert container_images.missing([content], SPARKLINA_SET) == (content,)


def test_any_repository_carrying_the_digest_satisfies_a_bare_requirement():
    """(e) Both repository names, in either order.

    Intended: a repository-qualified digest is content-addressed, so the
    repository context cannot make the same 64 hex a different image.
    """

    assert container_images.missing([BARE], [OTHER_REPO_DIGEST]) == ()
    assert container_images.missing([BARE], [REPO_DIGEST, OTHER_REPO_DIGEST]) == ()
    assert container_images.missing([BARE], [OTHER_REPO_DIGEST, REPO_DIGEST]) == ()
