"""Fleet storage declarations reach sealed actions and checkout materialization."""
from __future__ import annotations

import json

import pytest

from prismabuild import core, fleet_roster, local_scratch
from submission_support import patch_store
from test_pbrun_detach import _checkout, _one_json_line, _queue, _run_pbrun
from test_pbtest import _dispatch, pbtest


def _roster(tmp_path, **fields):
    path = tmp_path / "fleet_boxes.json"
    path.write_text(json.dumps({"boxes": {}, **fields}))
    return path


@pytest.mark.parametrize("override", [None, "/explicit/scratch"])
def test_pbrun_seals_configured_scratch_and_preserves_override(
    tmp_path, monkeypatch, capsys, override,
):
    work = _checkout(tmp_path)
    _queue(tmp_path)
    path = _roster(tmp_path, action_tmpdir="/fleet/nvme/scratch")
    patch_store(monkeypatch, roster_path=path)
    options = ["--detach"]
    if override is not None:
        options.extend(["--env", f"TMPDIR={override}"])
    assert _run_pbrun(tmp_path, monkeypatch, work, *options) == 0
    key = _one_json_line(capsys.readouterr())["action_key"]
    action = core.PrismaBuildCAS(tmp_path / "cas").read_action_request(key)
    assert action["environment"]["variables"]["TMPDIR"] == (override or "/fleet/nvme/scratch")


@pytest.mark.parametrize("override", [None, "/explicit/scratch"])
def test_pbtest_seals_the_same_configured_scratch(tmp_path, monkeypatch, override):
    path = _roster(tmp_path, action_tmpdir="/fleet/nvme/scratch")
    patch_store(monkeypatch, client=pbtest, roster_path=path)
    options = [] if override is None else ["--tmpdir", override]
    command = _dispatch(tmp_path, monkeypatch, options)
    from pbtest_shard_output import shard_environment
    assert shard_environment(command)["TMPDIR"] == (override or "/fleet/nvme/scratch")


@pytest.mark.parametrize("bad", ["", "relative", "/", "/scratch/../tmp", "/scratch\x00"])
def test_roster_rejects_ambiguous_scratch_before_publication(tmp_path, bad):
    roster = fleet_roster.Roster.read(_roster(tmp_path, action_tmpdir=bad))
    with pytest.raises(ValueError, match="action_tmpdir"):
        roster.validate_publication()


def test_legacy_roster_keeps_its_sealed_default(tmp_path):
    assert fleet_roster.Roster.read(_roster(tmp_path)).action_tmpdir == "/home/rob/tmp"


@pytest.mark.parametrize("tmpdir", ["/home/rob/tmp", "/home/rob/pbt17"])
def test_scratch_recorder_accepts_both_reviewed_fleet_defaults(tmpdir):
    local_scratch.profile_environment({"TMPDIR": tmpdir})
