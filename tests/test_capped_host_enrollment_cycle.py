"""First-host preflight/bootstrap and activation in a fixture fleet."""
import json

import pytest

from prismabuild import fleet_roster, installed_bootstrap
from test_enrolling_worker import ENROLLING
from test_roster_presence_status import _generation_with_roster, _offer, _roster
from test_the_coordinator_proves_the_fleet_before_a_barrier import (
    SHA, fleet, post, publish_runtime,
)


@pytest.mark.parametrize("status,expected", [
    ("enrolling", {"sparky": ["sparky"]}),
    ("active", {"celestia": ["celestia"], "sparky": ["sparky"]}),
])
def test_installed_bootstrap_requires_only_active_participants(fleet, status, expected):
    entries = {"sparky": {"loops": 1},
               "celestia": {"loops": 1, **ENROLLING, "status": status}}
    source = _generation_with_roster(fleet, "source", entries)
    target = _generation_with_roster(fleet, "target", entries)
    assert installed_bootstrap.enrolled_roster(source[0], target[0]) == expected


def test_enrolling_publication_then_active_requires_the_new_host(fleet):
    entries = {"sparky": {"loops": 1}, "celestia": {"loops": 1, **ENROLLING}}
    _roster(fleet, entries)
    post(fleet, "sparky", SHA)
    enrolling = fleet_roster.Roster.at(publish_runtime.CHECKOUT)
    publish_runtime._require_attested_fleet(SHA, enrolling)
    source = _generation_with_roster(fleet, "enrolling-source", entries)
    target = _generation_with_roster(fleet, "enrolling-target", entries)
    _offer(fleet, "sparky")
    _offer(fleet, "celestia")
    assert publish_runtime._barrier_roster((source, target)) == ["sparky"]
    entries["celestia"] = {"loops": 1, "status": "active"}
    _roster(fleet, entries)
    active = fleet_roster.Roster.at(publish_runtime.CHECKOUT)
    with pytest.raises(SystemExit, match="celestia.*no attestation"):
        publish_runtime._require_attested_fleet(SHA, active)
    post(fleet, "celestia", SHA)
    publish_runtime._require_attested_fleet(SHA, active)
    activated = _generation_with_roster(fleet, "active-target", entries)
    assert publish_runtime._barrier_roster((source, activated)) == ["celestia", "sparky"]
