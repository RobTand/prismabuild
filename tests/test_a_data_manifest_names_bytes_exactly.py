"""A data manifest names files, not a region of the filesystem.

The manifest is submitter-supplied and the loop that reads it runs as a user
on the file server, so the validator is the boundary that decides what that
loop may open.  "Exact identity" has to mean the literal thing: one path per
entry, inside the declared mount, already normalized, and totals that agree
with the list -- because the ARC budget is taken from ``total_bytes`` before a
single byte is read, and a manifest that could reserve one amount and read
another is a manifest that can evict the row that is running.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import prismabuild.core as pb  # noqa: E402

from test_core import _body  # noqa: E402


def _manifest(**overrides) -> dict:
    manifest = {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {"tool": "tests"},
        "annotations": {"row_id": "row-0079"},
        "mount_prefix": "/mnt/shared",
        "entries": [
            {"path": "/mnt/shared/a.pt", "offset": 0, "bytes": 10,
             "sha256": None},
            {"path": "/mnt/shared/w.safetensors", "offset": 1 << 20,
             "bytes": 20, "sha256": "b" * 64},
        ],
        "entry_count": 2,
        "total_bytes": 30,
    }
    manifest.update(overrides)
    return manifest


def test_a_well_formed_manifest_normalizes_to_its_own_entries() -> None:
    out = pb.validate_data_manifest(_manifest())
    assert out["total_bytes"] == 30
    assert out["entry_count"] == 2
    # Order is the consumption order and is never re-sorted: a partial warm
    # has to be a useful prefix of the read, not a random subset of it.
    assert [e["path"] for e in out["entries"]] == [
        "/mnt/shared/a.pt", "/mnt/shared/w.safetensors"]
    assert out["entries"][1]["offset"] == 1 << 20
    # A null digest is legal and deliberate: this contract carries residency,
    # not integrity, and the manifest file's own digest is what the action key
    # covers.
    assert out["entries"][0]["sha256"] is None


@pytest.mark.parametrize(
    "overrides, because",
    [
        ({"entries": [{"path": "/mnt/shared/../etc/shadow", "offset": 0,
                       "bytes": 1, "sha256": None}],
          "entry_count": 1, "total_bytes": 1}, "a traversal"),
        ({"entries": [{"path": "/etc/shadow", "offset": 0, "bytes": 1,
                       "sha256": None}],
          "entry_count": 1, "total_bytes": 1}, "a path outside the mount"),
        ({"entries": [{"path": "shared/a.pt", "offset": 0, "bytes": 1,
                       "sha256": None}],
          "entry_count": 1, "total_bytes": 1}, "a relative path"),
        ({"total_bytes": 31}, "a total that overstates the list"),
        ({"entry_count": 3}, "a count that disagrees with the list"),
        ({"mount_prefix": "/"}, "a prefix that is the whole filesystem"),
        ({"entries": [], "entry_count": 0, "total_bytes": 0}, "nothing to warm"),
        ({"entries": [{"path": "/mnt/shared/a.pt", "offset": 0, "bytes": 0,
                       "sha256": None}],
          "entry_count": 1, "total_bytes": 0}, "a zero-length entry"),
        ({"entries": [{"path": "/mnt/shared/a.pt", "offset": 0, "bytes": 5,
                       "sha256": None},
                      {"path": "/mnt/shared/a.pt", "offset": 0, "bytes": 5,
                       "sha256": None}],
          "entry_count": 2, "total_bytes": 10}, "the same byte range twice"),
        ({"schema": "prismaquant.prismabuild.data_manifest.v2"},
         "a schema this version does not know"),
    ],
)
def test_the_validator_refuses(overrides: dict, because: str) -> None:
    with pytest.raises(pb.ActionContractError):
        pb.validate_data_manifest(_manifest(**overrides))


def test_an_unknown_field_is_refused_rather_than_ignored() -> None:
    # Closed key sets, like every other action contract in this module: a
    # field the reader silently drops is a field the submitter believes in.
    with pytest.raises(pb.ActionContractError):
        pb.validate_data_manifest({**_manifest(), "row_id": "row-0079"})


def test_attaching_a_manifest_changes_the_action_key(tmp_path: Path) -> None:
    """The manifest is inside the hashed body, so it is part of the identity.

    This is the property that makes the input kind safe to add: an action with
    a manifest is a different action from the same command without one, so no
    receipt can be answered with a run that read different bytes.
    """

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    body = _body(checkout)
    manifest_row = {"id": pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID,
                    "sha256": "2" * 64, "bytes": 211663}
    without = pb.seal_action(body)["action_key"]
    with_manifest = pb.seal_action(
        {**body, "inputs": [*body["inputs"], manifest_row]})["action_key"]
    assert without != with_manifest
    # Input order is not part of the identity -- the contract sorts by id --
    # so a submitter that appends the manifest first seals the same action.
    reordered = pb.seal_action(
        {**body, "inputs": [manifest_row, *body["inputs"]]})["action_key"]
    assert reordered == with_manifest
