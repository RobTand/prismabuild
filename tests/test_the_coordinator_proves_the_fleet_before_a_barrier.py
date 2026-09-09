"""A barrier is refused until every box has said it can hold one.

#458 wants a fleet-wide swap: every box stops admitting, changes generation,
and resumes together.  Only an agent carrying that code can do any part of it,
so a coordinator that arms a barrier without checking is asserting a fleet-wide
property it never looked at.

#467 supplied the fact.  Each host writes `rollout/agents/<host>.<sha>.json`
beside the generation store, naming the sha256 of the agent bytes it runs.
This is the coordinator's half: under `--rollout barrier`, compare that against
the agent the publication carries, and refuse by name on any miss.

Three things are load bearing.

The coordinator reads the agent's own module for the marker name and the member
key, rather than keeping a second copy of either spelling.  A marker counts
only when its body reproduces its own file name through that function, so a
name somebody typed is not a claim the agent made.

A box answers to its roster key or to its `_alias`, because `gx10-6b77` reports
itself as `sparklina` and both names mean that box.

A generation that changes the agent can never be its own first barrier: no host
can have attested bytes that did not exist when it started.  That is the
rolling-then-barrier bootstrap, enforced by arithmetic.

Nothing here touches the real shared mount.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


publish_runtime = load("publish_runtime_barrier", ROOT / "tools/fleet/publish_runtime.py")
upgrade = load("upgrade_client_barrier", ROOT / "tools/fleet/upgrade_client.py")

SHA = "a" * 64
OTHER = "b" * 64


@pytest.fixture
def fleet(tmp_path, monkeypatch):
    """A private mount and a private roster, with nothing posted yet."""

    mirror = tmp_path / "fleet" / "repo"
    mirror.mkdir(parents=True)
    monkeypatch.setattr(publish_runtime, "MIRROR", mirror)
    monkeypatch.setattr(publish_runtime, "CHECKOUT", tmp_path / "checkout")
    roster = tmp_path / "checkout" / "tools" / "fleet"
    roster.mkdir(parents=True)
    (roster / "fleet_boxes.json").write_text(json.dumps({
        "boxes": {
            "sparky": {"loops": 1},
            "gx10-6b77": {"_alias": "sparklina", "loops": 1},
        }
    }))
    # The agent module is read from the checkout being published, so the
    # private checkout needs the real file rather than a stand-in.
    (roster / "upgrade_client.py").write_bytes(
        (ROOT / "tools/fleet/upgrade_client.py").read_bytes()
    )
    return mirror.parent


def post(fleet, host, sha, *, name=None, body=None):
    """Write a marker the way the agent writes one, or a broken variant."""

    directory = fleet / upgrade.ROLLOUT_DIRNAME / upgrade.AGENTS_DIRNAME
    directory.mkdir(parents=True, exist_ok=True)
    content = upgrade.attestation_body(host, sha) if body is None else body
    target = directory / (upgrade.attestation_name(host, sha) if name is None else name)
    target.write_text(json.dumps(content, sort_keys=True) + "\n")
    return target


def test_a_marker_written_by_the_agent_is_read_back_by_the_coordinator(fleet):
    """The round trip, through both real functions and no shared spelling."""

    for host in ("sparky", "gx10-6b77", "sparklina.local", "a.b.c.d"):
        post(fleet, host, SHA)
    attested = publish_runtime._attested_agents(upgrade)
    assert attested == {
        "sparky": {SHA}, "gx10-6b77": {SHA},
        "sparklina.local": {SHA}, "a.b.c.d": {SHA},
    }


def test_a_host_whose_name_does_not_survive_the_file_name_still_reads_back(fleet):
    """The body carries the raw name; the file name carries a safe one."""

    post(fleet, "box with spaces", SHA)
    assert publish_runtime._attested_agents(upgrade) == {"box with spaces": {SHA}}


def test_two_versions_from_one_host_are_both_recorded(fleet):
    post(fleet, "sparky", SHA)
    post(fleet, "sparky", OTHER)
    assert publish_runtime._attested_agents(upgrade) == {"sparky": {SHA, OTHER}}


@pytest.mark.parametrize("name", [
    "sparky.json",                    # no hash
    f"sparky.{SHA}.txt",              # not a marker
    f"sparky.{SHA}",                  # no suffix
    "README",
])
def test_a_name_that_is_not_a_marker_is_not_a_claim(fleet, name):
    post(fleet, "sparky", SHA, name=name)
    assert publish_runtime._attested_agents(upgrade) == {}


def test_a_body_that_does_not_match_its_own_name_is_refused(fleet):
    """The whole point of reading the body: the name alone is somebody's typing."""

    post(fleet, "sparky", SHA, body=upgrade.attestation_body("sparky", OTHER))
    assert publish_runtime._attested_agents(upgrade) == {}


def test_a_body_carrying_another_schema_is_refused(fleet):
    body = upgrade.attestation_body("sparky", SHA)
    body["schema"] = "something.else.v1"
    post(fleet, "sparky", SHA, body=body)
    assert publish_runtime._attested_agents(upgrade) == {}


@pytest.mark.parametrize("body", [
    {"schema": upgrade.ATTESTATION_SCHEMA, "host": "sparky"},
    {"schema": upgrade.ATTESTATION_SCHEMA, "client_sha256": SHA},
    {"schema": upgrade.ATTESTATION_SCHEMA, "host": 7, "client_sha256": SHA},
    {"schema": upgrade.ATTESTATION_SCHEMA, "host": "sparky", "client_sha256": None},
    [],
])
def test_a_body_missing_what_it_claims_is_refused(fleet, body):
    post(fleet, "sparky", SHA, body=body)
    assert publish_runtime._attested_agents(upgrade) == {}


def test_unreadable_json_is_refused_rather_than_raised(fleet):
    target = post(fleet, "sparky", SHA)
    target.write_text("{ not json")
    assert publish_runtime._attested_agents(upgrade) == {}


def test_an_oversized_marker_is_refused(fleet):
    target = post(fleet, "sparky", SHA)
    target.write_bytes(b"x" * (upgrade.MAX_MARKER + 1))
    assert publish_runtime._attested_agents(upgrade) == {}


def test_no_rollout_tree_at_all_reads_as_nothing_posted(fleet):
    """A fleet that has never posted is a refusal, not a traceback."""

    assert publish_runtime._attested_agents(upgrade) == {}


def test_the_roster_accepts_a_box_under_its_alias(fleet):
    assert publish_runtime._roster_boxes() == [
        ("gx10-6b77", frozenset({"gx10-6b77", "sparklina"})),
        ("sparky", frozenset({"sparky"})),
    ]


def test_the_real_roster_still_keys_the_second_gb10_under_its_alias():
    """A rename in the fleet has to reach this check, not go around it."""

    boxes = dict(publish_runtime._roster_boxes())
    assert "sparklina" in boxes["gx10-6b77"]


def test_a_barrier_passes_when_every_box_attests_the_published_agent(fleet):
    post(fleet, "sparky", SHA)
    post(fleet, "sparklina", SHA)
    publish_runtime._require_attested_fleet(SHA)


def test_a_box_answering_under_its_roster_key_also_counts(fleet):
    post(fleet, "sparky", SHA)
    post(fleet, "gx10-6b77", SHA)
    publish_runtime._require_attested_fleet(SHA)


def test_a_silent_box_is_named_in_the_refusal(fleet):
    post(fleet, "sparky", SHA)
    with pytest.raises(SystemExit) as raised:
        publish_runtime._require_attested_fleet(SHA)
    message = str(raised.value)
    assert "gx10-6b77 (gx10-6b77/sparklina): has posted no attestation" in message
    # Rolling installs nothing, so the advice below must not be the only thing
    # a box with no agent at all is told.
    assert "cannot hold a barrier at all" in message
    assert "sparky:" not in message
    assert "--rollout rolling" in message


def test_a_stale_box_is_named_with_what_it_is_running(fleet):
    post(fleet, "sparky", SHA)
    post(fleet, "sparklina", OTHER)
    with pytest.raises(SystemExit) as raised:
        publish_runtime._require_attested_fleet(SHA)
    assert f"running {OTHER[:12]}" in str(raised.value)


def test_a_generation_that_changes_the_agent_cannot_be_its_own_first_barrier(fleet):
    """The bootstrap, as arithmetic rather than as a paragraph."""

    post(fleet, "sparky", SHA)
    post(fleet, "sparklina", SHA)
    with pytest.raises(SystemExit) as raised:
        publish_runtime._require_attested_fleet(OTHER)
    assert OTHER[:12] in str(raised.value)


def test_the_member_key_the_coordinator_reads_is_a_key_the_manifest_writes():
    """The other drift: the agent's member key must name a published file."""

    member = upgrade.MEMBERS["upgrade_client.py"]
    assert member in publish_runtime._publication_manifest()


def test_a_rolling_publication_never_looks_at_the_rollout_tree(monkeypatch):
    """The default is what every publication has always done."""

    def refuse(*args, **kwargs):
        raise AssertionError("a rolling publication proved the fleet")

    monkeypatch.setattr(publish_runtime, "_require_attested_fleet", refuse)
    monkeypatch.setattr(publish_runtime, "_commit_identity", lambda: "c" * 40)
    monkeypatch.setattr(publish_runtime, "_working_tree_dirty", lambda: False)
    monkeypatch.setattr(publish_runtime, "_publication_manifest", lambda: {"tools/x.py": SHA})
    monkeypatch.setattr(publish_runtime, "_git_index_modes", lambda: {})
    monkeypatch.setattr(publish_runtime.sys, "argv", ["publish_runtime.py", "--dry-run"])
    assert publish_runtime.main() == 0


def test_a_barrier_publication_refuses_before_anything_is_staged(fleet, monkeypatch):
    member = upgrade.MEMBERS["upgrade_client.py"]
    monkeypatch.setattr(publish_runtime, "_commit_identity", lambda: "c" * 40)
    monkeypatch.setattr(publish_runtime, "_working_tree_dirty", lambda: False)
    monkeypatch.setattr(publish_runtime, "_publication_manifest", lambda: {member: SHA})
    monkeypatch.setattr(publish_runtime, "_git_index_modes", lambda: {})
    monkeypatch.setattr(
        publish_runtime.sys, "argv",
        ["publish_runtime.py", "--dry-run", "--rollout", "barrier"],
    )
    with pytest.raises(SystemExit) as raised:
        publish_runtime.main()
    assert "has posted no attestation" in str(raised.value)


def test_a_barrier_publication_carrying_no_agent_refuses(fleet, monkeypatch):
    monkeypatch.setattr(publish_runtime, "_commit_identity", lambda: "c" * 40)
    monkeypatch.setattr(publish_runtime, "_working_tree_dirty", lambda: False)
    monkeypatch.setattr(publish_runtime, "_publication_manifest", lambda: {"tools/x.py": SHA})
    monkeypatch.setattr(publish_runtime, "_git_index_modes", lambda: {})
    monkeypatch.setattr(
        publish_runtime.sys, "argv",
        ["publish_runtime.py", "--dry-run", "--rollout", "barrier"],
    )
    with pytest.raises(SystemExit) as raised:
        publish_runtime.main()
    assert "nothing to prove the fleet against" in str(raised.value)


def _generation(fleet, member_sha):
    store = fleet / "runtime-generations"
    generation = store / "abc123-1-def"
    generation.mkdir(parents=True)
    receipt = {"commit": "c" * 40, "generation": generation.name, "files": {}}
    if member_sha is not None:
        receipt["files"] = {upgrade.MEMBERS["upgrade_client.py"]: member_sha}
    (generation / "RUNTIME_VERSION.json").write_text(json.dumps(receipt))
    return generation


def test_a_barrier_rollback_is_proved_against_the_receipt_it_restores(fleet):
    _generation(fleet, SHA)
    post(fleet, "sparky", SHA)
    post(fleet, "sparklina", SHA)
    assert publish_runtime._activate_existing(
        "abc123-1-def", dry_run=True, rollout="barrier"
    ) == 0


def test_a_barrier_rollback_to_an_unattested_agent_refuses(fleet):
    _generation(fleet, OTHER)
    post(fleet, "sparky", OTHER)
    with pytest.raises(SystemExit) as raised:
        publish_runtime._activate_existing(
            "abc123-1-def", dry_run=True, rollout="barrier"
        )
    assert "gx10-6b77" in str(raised.value)


def test_a_receipt_without_the_agent_cannot_prove_a_barrier_rollback(fleet):
    _generation(fleet, None)
    with pytest.raises(SystemExit) as raised:
        publish_runtime._activate_existing(
            "abc123-1-def", dry_run=True, rollout="barrier"
        )
    assert "records no sha256" in str(raised.value)


def test_a_rolling_rollback_reads_no_receipt_files_at_all(fleet):
    _generation(fleet, None)
    assert publish_runtime._activate_existing("abc123-1-def", dry_run=True) == 0
