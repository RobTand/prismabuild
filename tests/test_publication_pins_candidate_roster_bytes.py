"""Alias and option gates judge the roster bytes named by publication."""
from __future__ import annotations

import hashlib
import json

import pytest

from test_publication_refuses_a_dropped_roster_option_1664 import (
    CANDIDATE_ARGS, LIVE_ARGS, _checkout, _live, _publish, _roster,
    publish_runtime,
)


MEMBER = "tools/fleet/fleet_boxes.json"


def test_candidate_alias_validation_is_bound_to_the_publication_manifest(
        tmp_path, monkeypatch):
    invalid = {"boxes": {"old": {"_alias": "renamed"}, "renamed": {}}}
    checkout = _checkout(tmp_path / "checkout", invalid)
    mirror = _publish(tmp_path, monkeypatch, checkout, [])
    path = checkout / MEMBER
    manifested = path.read_bytes()
    transient = json.dumps({"boxes": {"one": {}, "two": {}}}).encode()
    manifests = []
    reads = []
    real_manifest = publish_runtime._publication_manifest
    real_candidate = publish_runtime._candidate_roster

    def observed_manifest():
        result = real_manifest()
        manifests.append(result[MEMBER])
        return result

    def transient_candidate(*args, **kwargs):
        reads.append(path.read_bytes())
        path.write_bytes(transient)
        try:
            return real_candidate(*args, **kwargs)
        finally:
            path.write_bytes(manifested)

    monkeypatch.setattr(publish_runtime, "_publication_manifest", observed_manifest)
    monkeypatch.setattr(publish_runtime, "_candidate_roster", transient_candidate)
    try:
        with pytest.raises(SystemExit, match="candidate roster.*manifest"):
            publish_runtime.main()
    finally:
        assert reads == [manifested]
        assert manifests and set(manifests) == {hashlib.sha256(manifested).hexdigest()}
        assert path.read_bytes() == manifested
    assert not mirror.exists()
    assert not (mirror.parent / "runtime-generations").exists()


def test_option_gate_uses_the_candidate_already_read_for_publication(
        tmp_path, monkeypatch):
    live, prior = _live(tmp_path, monkeypatch, _roster(LIVE_ARGS))
    checkout = _checkout(tmp_path / "checkout", _roster(LIVE_ARGS))
    _publish(tmp_path, monkeypatch, checkout, [])
    monkeypatch.setattr(publish_runtime, "MIRROR", live)
    path = checkout / MEMBER
    manifested = path.read_bytes()
    transient = json.dumps(_roster(CANDIDATE_ARGS), sort_keys=True).encode()
    reads = []
    real_candidate = publish_runtime._candidate_roster

    def changing_second_candidate(*args, **kwargs):
        reads.append(path.read_bytes())
        if len(reads) == 2:
            path.write_bytes(transient)
        try:
            return real_candidate(*args, **kwargs)
        finally:
            path.write_bytes(manifested)

    monkeypatch.setattr(publish_runtime, "_candidate_roster", changing_second_candidate)
    assert publish_runtime.main() == 0
    assert reads == [manifested]
    assert live.resolve() != prior
    assert (live.resolve() / MEMBER).read_bytes() == manifested


def test_live_option_gate_refuses_a_candidate_not_in_the_manifest(
        tmp_path, monkeypatch):
    live, prior = _live(tmp_path, monkeypatch, _roster(LIVE_ARGS))
    checkout = _checkout(tmp_path / "checkout", _roster(LIVE_ARGS))
    _publish(tmp_path, monkeypatch, checkout, [])
    monkeypatch.setattr(publish_runtime, "MIRROR", live)
    monkeypatch.setattr(publish_runtime, "FLEET_DATA", ())
    with pytest.raises(SystemExit, match="candidate roster.*manifest"):
        publish_runtime.main()
    assert live.resolve() == prior
    assert list((live.parent / "runtime-generations").iterdir()) == [prior]


def test_bootstrap_without_a_roster_keeps_its_existing_publication_behavior(
        tmp_path, monkeypatch):
    checkout = _checkout(tmp_path / "checkout", {})
    (checkout / MEMBER).unlink()
    mirror = _publish(tmp_path, monkeypatch, checkout, [])
    assert publish_runtime.main() == 0
    assert mirror.is_symlink()
