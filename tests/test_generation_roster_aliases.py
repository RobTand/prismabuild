"""Publication rejects ambiguity within a generation, preserving rename unions."""
from __future__ import annotations

import pytest

from test_publication_refuses_a_dropped_roster_option_1664 import (
    _checkout, _publish, publish_runtime as candidate_publisher,
)
from test_roster_presence_status import (
    _generation_with_roster, _offer, _roster, _roster_pair, fleet,
    post, SHA, publish_runtime as barrier_publisher,
)


COLLISIONS = [
    pytest.param({"old": {"_alias": "renamed"}, "renamed": {}},
                 "renamed", id="alias-to-key"),
    pytest.param({"one": {"_alias": "shared"},
                  "two": {"_alias": "shared"}},
                 "shared", id="alias-to-alias"),
]


@pytest.mark.parametrize("boxes,host", COLLISIONS)
@pytest.mark.parametrize("extra", [
    pytest.param([], id="rolling-bootstrap"),
    pytest.param(["--rollout", "barrier", "--stage-only"], id="barrier-stage"),
])
def test_ambiguous_candidate_refuses_before_publication(
        tmp_path, monkeypatch, boxes, host, extra):
    checkout = _checkout(tmp_path / "checkout", {"boxes": boxes})
    mirror = _publish(tmp_path, monkeypatch, checkout, extra)
    with pytest.raises(SystemExit, match=f"ambiguous.*{host}"):
        candidate_publisher.main()
    assert not mirror.exists()
    assert not (mirror.parent / "runtime-generations").exists()


@pytest.mark.parametrize("boxes", [
    pytest.param({"one": {"_alias": "new-one"},
                  "two": {"_alias": "new-two"}}, id="unique-aliases"),
    pytest.param({"one": {"_alias": "one"}, "two": {}}, id="self-alias"),
])
def test_unambiguous_candidate_publishes(tmp_path, monkeypatch, boxes):
    checkout = _checkout(tmp_path / "checkout", {"boxes": boxes})
    mirror = _publish(tmp_path, monkeypatch, checkout, [])
    assert candidate_publisher.main() == 0
    assert mirror.is_symlink()


@pytest.mark.parametrize("boxes,host", COLLISIONS)
def test_one_agent_marker_cannot_attest_two_generation_declarations(
        fleet, boxes, host):
    _roster(fleet, boxes)
    post(fleet, host, SHA)
    with pytest.raises(SystemExit, match=f"ambiguous.*{host}"):
        barrier_publisher._require_attested_fleet(SHA)


@pytest.mark.parametrize("boxes,host", COLLISIONS)
@pytest.mark.parametrize("bad_generation", ["source", "target"])
def test_barrier_refuses_a_generation_collision_before_union(
        fleet, boxes, host, bad_generation):
    valid = {host: {}}
    generations = [
        _generation_with_roster(fleet, "source", boxes if bad_generation == "source" else valid),
        _generation_with_roster(fleet, "target", boxes if bad_generation == "target" else valid),
    ]
    _offer(fleet, host)
    with pytest.raises(SystemExit, match=f"ambiguous.*{host}"):
        barrier_publisher._barrier_roster(generations)


def test_barrier_keeps_the_old_and_new_generation_rename_union(fleet):
    generations = [
        _generation_with_roster(fleet, "source", {"old": {"_alias": "middle"}}),
        _generation_with_roster(fleet, "target", {"middle": {"_alias": "current"}}),
    ]
    _offer(fleet, "current")
    assert barrier_publisher._barrier_roster(generations) == ["current"]


def test_barrier_accepts_an_alias_equal_to_its_own_key(fleet):
    _offer(fleet, "one")
    assert barrier_publisher._barrier_roster(
        _roster_pair(fleet, {"one": {"_alias": "one"}})) == ["one"]


@pytest.mark.parametrize("alias", [None, "", 7, ["not-a-name"], "bad*name"])
def test_candidate_preserves_its_alias_shape_policy(tmp_path, monkeypatch, alias):
    checkout = _checkout(tmp_path / "checkout", {"boxes": {"one": {"_alias": alias}}})
    mirror = _publish(tmp_path, monkeypatch, checkout, [])
    assert candidate_publisher.main() == 0
    assert mirror.is_symlink()


@pytest.mark.parametrize("alias,accepted", [
    (None, True), ("", True), (0, True), ([], True),
    (7, False), (["not-a-name"], False), ("bad*name", False),
])
def test_barrier_preserves_its_alias_shape_policy(fleet, alias, accepted):
    generations = _roster_pair(fleet, {"one": {"_alias": alias}})
    _offer(fleet, "one")
    if accepted:
        assert barrier_publisher._barrier_roster(generations) == ["one"]
    else:
        with pytest.raises(SystemExit, match="invalid barrier roster"):
            barrier_publisher._barrier_roster(generations)


@pytest.mark.parametrize("document", [{}, {"boxes": []}, {"boxes": {}}])
def test_candidate_preserves_its_roster_shape_policy(tmp_path, monkeypatch, document):
    checkout = _checkout(tmp_path / "checkout", document)
    mirror = _publish(tmp_path, monkeypatch, checkout, [])
    assert candidate_publisher.main() == 0
    assert mirror.is_symlink()
