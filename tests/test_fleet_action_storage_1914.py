"""Fleet storage declarations reach sealed actions and checkout materialization."""
from __future__ import annotations

import json
from pathlib import Path
import runpy
from threading import Event

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
        override = str(tmp_path)
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


@pytest.mark.parametrize("no_defaults", [False, True])
def test_explicit_environment_does_not_depend_on_the_roster_default(
    tmp_path, monkeypatch, capsys, no_defaults,
):
    work = _checkout(tmp_path)
    _queue(tmp_path)
    patch_store(monkeypatch, roster_path=_roster(tmp_path, action_tmpdir="invalid"))
    options = (["--no-default-env"] if no_defaults else ["--env", f"TMPDIR={tmp_path}"])
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach", *options) == 0
    key = _one_json_line(capsys.readouterr())["action_key"]
    action = core.PrismaBuildCAS(tmp_path / "cas").read_action_request(key)
    variables = action["environment"]["variables"]
    assert variables.get("TMPDIR") == (None if no_defaults else str(tmp_path))


@pytest.mark.parametrize("bad", ["", "relative", "/", "/scratch/../tmp", "/scratch\x00"])
def test_roster_rejects_ambiguous_scratch_before_publication(tmp_path, bad):
    roster = fleet_roster.Roster.read(_roster(tmp_path, action_tmpdir=bad))
    with pytest.raises(ValueError, match="action_tmpdir"):
        roster.validate_publication()


def test_legacy_roster_keeps_its_sealed_default(tmp_path):
    assert fleet_roster.Roster.read(_roster(tmp_path)).action_tmpdir == "/home/rob/tmp"


def test_missing_roster_refuses_before_action_publication(tmp_path, monkeypatch):
    work = _checkout(tmp_path)
    queue = _queue(tmp_path)
    patch_store(monkeypatch, roster_path=tmp_path / "missing-roster.json")
    with pytest.raises(SystemExit, match="cannot read fleet action_tmpdir"):
        _run_pbrun(tmp_path, monkeypatch, work, "--detach")
    assert queue.ready_items() == []


def test_pbrun_keeps_its_loaded_roster_after_the_live_symlink_moves(tmp_path):
    source = Path(__file__).resolve().parents[1]
    for name, scratch in [("loaded", "/loaded/nvme"), ("next", "/next/nvme")]:
        tools = tmp_path / name / "tools" / "fleet"
        tools.mkdir(parents=True)
        (tools / "pbrun.py").write_bytes((source / "tools/fleet/pbrun.py").read_bytes())
        (tools / "fleet_boxes.json").write_text(json.dumps({"boxes": {}, "action_tmpdir": scratch}))
    live = tmp_path / "repo"
    live.symlink_to(tmp_path / "loaded", target_is_directory=True)
    loaded = runpy.run_path(str(live / "tools/fleet/pbrun.py"))
    live.unlink()
    live.symlink_to(tmp_path / "next", target_is_directory=True)
    store = loaded["configured_store"]()
    assert fleet_roster.Roster.read(store.roster_path).action_tmpdir == "/loaded/nvme"


@pytest.mark.parametrize("tmpdir", ["/home/rob/tmp", "/home/rob/pbt17"])
def test_scratch_recorder_accepts_both_reviewed_fleet_defaults(tmpdir):
    local_scratch.profile_environment({"TMPDIR": tmpdir})


def test_declared_worker_root_reaches_real_materialization(tmp_path, monkeypatch):
    from prismabuild import pool
    from test_live_store_guard import _materialization_item
    from test_worker_loop_offers_its_affinity import _worker_loop

    loop = _worker_loop()
    root = tmp_path / "nvme-checkouts"
    item = _materialization_item(tmp_path)
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", pool.LOCAL_CHECKOUT_ROOT)
    monkeypatch.setattr(loop.cpu_topology, "inherited_tiers", lambda: None)

    class Materialized(Exception):
        pass

    def first_queue(_root):
        with pool._execution_checkout(item) as checkout:
            assert root in checkout.parents
        raise Materialized

    monkeypatch.setattr(pool, "PoolQueue", first_queue)
    with pytest.raises(Materialized):
        loop._run_loop(Event(), argv=["--all-cores", "--local-checkout-root", str(root)])
