"""A declared-absent box neither vetoes nor joins a barrier (#606).

On 2026-09-18 ``wsl-gpu`` had been silent for 69 h and unreachable over ssh,
and the attestation preflight demanded its agent version anyway -- one offline
box blocked a barrier publish for the three live ones, with the only exit a
hand-written rolling reason that installs nothing on the missing box.

So the roster carries presence: ``status`` ``active`` (the default),
``retired`` or ``offline``, and a non-active box names its reason, who said
so, and when.  Absent boxes skip the preflight and the epoch roster, and
their exclusion is said out loud; an absent box that still announces, a group
mixing absent and active names, and a malformed declaration all refuse.  The
supervisor side converges the box's loops to zero, so placement stops seeing
it without anyone re-typing a command line.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import fleet_roster  # noqa: E402
import supervise  # noqa: E402

from test_the_coordinator_proves_the_fleet_before_a_barrier import (
    OTHER, SHA, fleet, post, publish_runtime, upgrade,
)


OFFLINE = {"status": "offline", "status_reason": "host down, ssh refused",
           "status_by": "rob", "status_unix": 1789750000.0}
RETIRED = {"status": "retired", "status_reason": "decommissioned",
           "status_by": "rob", "status_unix": 1789750000.0}


def _roster(fleet, boxes):
    path = publish_runtime.CHECKOUT / "tools" / "fleet" / "fleet_boxes.json"
    path.write_text(json.dumps({"boxes": boxes}))


# -- the contract itself -----------------------------------------------------


def test_absent_status_is_active():
    assert fleet_roster.box_status("boxa", {"loops": 1}) == ("active", {})
    assert fleet_roster.box_status("boxa", {"loops": 1, "status": "active"}) == ("active", {})
    assert fleet_roster.box_status("boxa", "not-a-mapping") == ("active", {})


def test_absent_states_carry_their_provenance():
    status, detail = fleet_roster.box_status("wsl-gpu", {"loops": 3, **OFFLINE})
    assert status == "offline"
    assert detail == {"reason": "host down, ssh refused", "by": "rob",
                      "unix": 1789750000.0}
    assert "rob" in fleet_roster.describe_absent("wsl-gpu", detail)


def test_unknown_status_refuses():
    with pytest.raises(fleet_roster.RosterPresenceError, match="unknown status"):
        fleet_roster.box_status("boxa", {"status": "standby"})


@pytest.mark.parametrize("drop", ["status_reason", "status_by", "status_unix"])
def test_absent_without_provenance_refuses(drop):
    entry = dict({"loops": 3, **RETIRED})
    del entry[drop]
    with pytest.raises(fleet_roster.RosterPresenceError, match=drop):
        fleet_roster.box_status("boxa", entry)


@pytest.mark.parametrize("bad", ["", "  ", None, 7, True, float("nan"),
                                 float("inf")])
def test_blank_or_nonfinite_provenance_refuses(bad):
    entry = dict({"loops": 3, **RETIRED})
    field = "status_reason" if isinstance(bad, str) or bad in (None, 7, True) else "status_unix"
    entry[field] = bad
    with pytest.raises(fleet_roster.RosterPresenceError):
        fleet_roster.box_status("boxa", entry)


# -- the preflight skips absent boxes ----------------------------------------


def test_preflight_passes_when_only_an_absent_box_is_silent(
    fleet, monkeypatch, capsys,
):
    _roster(fleet, {
        "sparky": {"loops": 1},
        "gx10-6b77": {"_alias": "sparklina", "loops": 1},
        "wsl-gpu": {"_alias": "DESKTOP-P5UOGNJ", "loops": 3, **OFFLINE},
    })
    post(fleet, "sparky", SHA)
    post(fleet, "sparklina", SHA)

    publish_runtime._require_attested_fleet(SHA)

    out = capsys.readouterr().out
    assert "skipping absent box" in out and "wsl-gpu" in out


def test_preflight_names_the_skipped_box(fleet, monkeypatch, capsys):
    _roster(fleet, {
        "sparky": {"loops": 1},
        "wsl-gpu": {"_alias": "DESKTOP-P5UOGNJ", "loops": 3, **OFFLINE},
    })
    post(fleet, "sparky", SHA)

    publish_runtime._require_attested_fleet(SHA)

    out = capsys.readouterr().out
    assert "wsl-gpu" in out and "host down, ssh refused" in out


def test_preflight_still_names_a_silent_active_box(fleet, monkeypatch):
    _roster(fleet, {
        "sparky": {"loops": 1},
        "wsl-gpu": {"_alias": "DESKTOP-P5UOGNJ", "loops": 3, **OFFLINE},
    })
    post(fleet, "wsl-gpu", SHA)
    with pytest.raises(SystemExit, match="has posted no attestation"):
        publish_runtime._require_attested_fleet(SHA)


def test_preflight_refuses_a_malformed_presence(fleet, monkeypatch):
    _roster(fleet, {
        "sparky": {"loops": 1},
        "wsl-gpu": {"loops": 3, "status": "offline"},
    })
    post(fleet, "sparky", SHA)
    with pytest.raises(SystemExit, match="cannot establish the fleet roster"):
        publish_runtime._require_attested_fleet(SHA)


def test_preflight_refuses_an_unknown_status(fleet, monkeypatch):
    _roster(fleet, {
        "sparky": {"loops": 1},
        "wsl-gpu": {"loops": 3, "status": "standby"},
    })
    post(fleet, "sparky", SHA)
    with pytest.raises(SystemExit, match="unknown status"):
        publish_runtime._require_attested_fleet(SHA)


# -- the epoch roster excludes absent boxes ----------------------------------


def _generation_with_roster(fleet, name, boxes, member_sha=None):
    generation = fleet / "runtime-generations" / name
    generation.mkdir(parents=True)
    member = "tools/fleet/fleet_boxes.json"
    data = json.dumps({"boxes": boxes}, sort_keys=True).encode()
    (generation / member).parent.mkdir(parents=True, exist_ok=True)
    (generation / member).write_bytes(data)
    files = {member: __import__("hashlib").sha256(data).hexdigest()}
    if member_sha is not None:
        files[upgrade.MEMBERS["upgrade_client.py"]] = member_sha
    receipt = {"commit": "c" * 40, "generation": name, "files": files}
    (generation / "RUNTIME_VERSION.json").write_text(json.dumps(receipt))
    return generation, receipt


def _offer(fleet, host, *, age_s=10.0):
    directory = fleet / "pb-queue" / "workers"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{host}.json").write_text(json.dumps({
        "schema": "prismaquant.prismabuild.pool_offer.v1",
        "host": host, "announced_unix": time.time() - age_s}))


def _roster_pair(fleet, boxes):
    source, source_receipt = _generation_with_roster(fleet, "gen-source-1-x", boxes)
    target, target_receipt = _generation_with_roster(fleet, "gen-target-2-y", boxes)
    return [(source, source_receipt), (target, target_receipt)]


def test_epoch_roster_excludes_an_absent_box(fleet):
    boxes = {"sparky": {"loops": 1},
             "wsl-gpu": {"loops": 3, **OFFLINE}}
    _offer(fleet, "sparky")
    _offer(fleet, "DESKTOP-P5UOGNJ", age_s=248394.0)  # the 69 h stale offer

    assert publish_runtime._barrier_roster(_roster_pair(fleet, boxes)) == ["sparky"]


def test_epoch_roster_retires_between_generations(fleet):
    live = {"sparky": {"loops": 1}, "wsl-gpu": {"loops": 3}}
    gone = {"sparky": {"loops": 1}, "wsl-gpu": {"loops": 3, **RETIRED}}
    source, source_receipt = _generation_with_roster(fleet, "gen-source-1-x", live)
    target, target_receipt = _generation_with_roster(fleet, "gen-target-2-y", gone)
    _offer(fleet, "sparky")

    assert publish_runtime._barrier_roster(
        [(source, source_receipt), (target, target_receipt)]) == ["sparky"]


def test_epoch_roster_refuses_an_absent_box_that_announces(fleet):
    boxes = {"sparky": {"loops": 1},
             "wsl-gpu": {"loops": 3, **OFFLINE}}
    _offer(fleet, "sparky")
    _offer(fleet, "wsl-gpu")

    with pytest.raises(SystemExit, match="declared absent are announcing"):
        publish_runtime._barrier_roster(_roster_pair(fleet, boxes))


def test_epoch_roster_refuses_an_all_absent_fleet(fleet):
    boxes = {"wsl-gpu": {"loops": 3, **OFFLINE}}
    (fleet / "pb-queue" / "workers").mkdir(parents=True)
    with pytest.raises(SystemExit, match="roster is empty"):
        publish_runtime._barrier_roster(_roster_pair(fleet, boxes))


def test_epoch_roster_refuses_a_malformed_presence(fleet):
    boxes = {"sparky": {"loops": 1},
             "wsl-gpu": {"loops": 3, "status": "offline"}}
    _offer(fleet, "sparky")
    with pytest.raises(SystemExit, match="invalid barrier roster"):
        publish_runtime._barrier_roster(_roster_pair(fleet, boxes))


# -- the supervisor side stops offering --------------------------------------


def _supervisor_config(tmp_path, monkeypatch, boxes, host="boxa"):
    config = tmp_path / "fleet_boxes.json"
    config.write_text(json.dumps({"boxes": boxes}))
    monkeypatch.setattr(supervise, "CONFIG", config)
    monkeypatch.setattr(supervise.socket, "gethostname", lambda: host)
    return config


def test_supervisor_starts_an_active_box(tmp_path, monkeypatch):
    _supervisor_config(tmp_path, monkeypatch, {"boxa": {"loops": 2, "args": []}})
    assert supervise._config("boxa") == {"loops": 2, "args": []}
    assert supervise.box_presence("boxa") == ("active", {})


def test_supervisor_refuses_an_absent_box_at_startup(tmp_path, monkeypatch):
    _supervisor_config(tmp_path, monkeypatch,
                       {"boxa": {"loops": 2, "args": [], **OFFLINE}})
    with pytest.raises(SystemExit, match="declares boxa offline"):
        supervise._config("boxa")
    status, detail = supervise.box_presence("boxa")
    assert status == "offline" and detail["by"] == "rob"


def test_supervisor_without_a_roster_answers_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr(supervise, "CONFIG", tmp_path / "missing.json")
    monkeypatch.setattr(supervise, "_current_root", lambda: tmp_path / "noroot")
    assert supervise.box_presence("boxa") is None
