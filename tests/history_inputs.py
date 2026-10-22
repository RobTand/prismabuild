"""Verified historical source bytes, independent of the checkout's Git history."""
from __future__ import annotations

from functools import cache
import json
from pathlib import Path
import tarfile

from prismabuild.digest_primitives import raw_sha256

INPUTS = Path(__file__).resolve().parent / "fixtures/history_inputs"


@cache
def history_manifest():
    return json.loads((INPUTS / "manifest.json").read_bytes())


@cache
def history_blobs():
    with tarfile.open(INPUTS / "blobs.tar.gz", "r:gz") as archive:
        members = archive.getmembers()
        assert all(member.isfile() and "/" not in member.name for member in members)
        assert len({member.name for member in members}) == len(members)
        return {member.name: archive.extractfile(member).read() for member in members}


def source_bytes(revision, path):
    """Return the exact pinned bytes after checking both the count and SHA-256."""
    pin = history_manifest()["revisions"][revision]["files"][path]
    raw = history_blobs()[pin["sha256"]]
    assert len(raw) == pin["bytes"], f"historical input byte count differs: {revision}:{path}"
    assert raw_sha256(raw) == pin["sha256"], f"historical input digest differs: {revision}:{path}"
    return raw


def materialize_sources(revision, destination):
    """Write a census subprocess's pinned import/read closure into its own tree."""
    for relative in history_manifest()["revisions"][revision]["files"]:
        path = destination / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(source_bytes(revision, relative))
