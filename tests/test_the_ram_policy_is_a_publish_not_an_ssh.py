"""The ram tier's sizing is a versioned policy the loop reads, not a box
setting.

Everything the ram tier decides -- how high a ceiling PB will accept, how
much of the tmpfs the window may fill, the ARC floor and the system reserve
the ceiling must coexist with, how far ahead of the consumer the window may
run -- is declared in one small versioned file that travels with the
published runtime, the way ``fleet_boxes.json`` does.  A change to it is a
publish, not an ssh: the loop reads it fresh on every cycle, so a declared
change or a rare operator remount is picked up between cycles automatically,
and the numbers a refusal names are the numbers the policy declared (#640).
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prismabuild.storage_tiers as storage_tiers  # noqa: E402

import publish_runtime  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
POLICY = ROOT / "tools" / "fleet" / "ram_tier_policy.json"


def test_the_committed_policy_validates_and_says_what_the_direction_says() -> None:
    policy = storage_tiers.read_ram_policy(POLICY)

    assert policy is not None, "the policy is committed, not implied"
    assert policy["schema"] == storage_tiers.RAM_TIER_POLICY_SCHEMA_V1
    # The ceiling Rob sanctioned, and the window he directed: 96-128 GiB is
    # the range, 112 its midpoint, and never a phase container -- the largest
    # phase is 134.2 GiB.
    assert policy["ceiling_gib_max"] == 256
    assert 96 <= policy["window_gib_default"] <= 128
    # The floor guard's declared constants, and the run-ahead default: None
    # is the #633 semantics the stage window already runs.
    assert policy["arc_floor_gib"] > 0
    assert policy["system_reserve_gib"] > 0
    assert policy["prefill_depth"] is None
    assert policy["mountpoint"].startswith("/")


def test_a_policy_that_cannot_be_read_offers_no_ram_tier(tmp_path) -> None:
    """Absent is the honest answer for a generation that predates the file."""

    assert storage_tiers.read_ram_policy(tmp_path / "no-policy.json") is None


def test_a_policy_that_does_not_say_what_it_must_refuses(tmp_path) -> None:
    for bad in (
        {"schema": "prismabuild.ram_tier_policy.v0"},          # not this one
        {"schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
         "mountpoint": "ram/prewarm"},                          # not absolute
        {"schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
         "mountpoint": "/ram/prewarm", "ceiling_gib_max": 0},   # not positive
        {"schema": storage_tiers.RAM_TIER_POLICY_SCHEMA_V1,
         "mountpoint": "/ram/prewarm", "ceiling_gib_max": 256,
         "window_gib_default": 112, "arc_floor_gib": 20,
         "system_reserve_gib": 16, "prefill_depth": -3},        # not a depth
    ):
        path = tmp_path / "policy.json"
        path.write_text(json.dumps(bad))
        assert storage_tiers.read_ram_policy(path) is None, bad


def test_a_declared_change_is_read_fresh_not_cached(tmp_path) -> None:
    """The publish-not-ssh half: the next cycle mints the new window."""

    policy = dict(json.loads(POLICY.read_text()))
    policy["window_gib_default"] = 128
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(policy))

    assert storage_tiers.read_ram_policy(path)["window_gib_default"] == 128


def test_the_policy_and_the_promotion_tool_travel_with_the_runtime() -> None:
    """A publish is how a change reaches the storage box, so both must ride."""

    assert "ram_tier_policy.json" in publish_runtime.FLEET_DATA
    assert "ram_promote.py" in publish_runtime.FLEET_SCRIPTS
    manifest = publish_runtime._publication_manifest()
    assert "tools/ram_tier_policy.json" in manifest
    assert "tools/ram_promote.py" in manifest
