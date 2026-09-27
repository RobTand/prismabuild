"""Poll-site re-pins survive in the existing commit answer (#1179)."""
import json

import pytest
from prismabuild import produced_spool as ps, produced_output as po
from test_produced_spool import world, _isolated_synthetic_launch_context
from test_an_export_survives_a_delegation_recall import _landed, _recall_ctime, _recall_mtime
from test_write_only_produced_output import _descriptor


@pytest.mark.parametrize("recall", [_recall_ctime, _recall_mtime])
@pytest.mark.parametrize("earlier_poll", [False, True])
def test_commit_retains_poll_repins_after_namespace_retirement(tmp_path, recall, earlier_poll):
    spool = world(tmp_path)
    _, destination, _ = _landed(spool)
    descriptor = _descriptor(spool.instance, spool.template, destination, b"hello")
    before, after = recall(destination)
    # Poll before commit too: evidence produced by an earlier caller must not
    # disappear just because commit's own poll no longer needs to re-hash.
    if earlier_poll:
        assert spool.poll_group("b1")["complete"]
    answer = spool.commit_origin_group("b1", [descriptor])
    assert answer["ok"]
    repins = answer.get("landed_repins", [])
    assert isinstance(repins, list)
    assert len(repins) == 1
    assert repins[0]["where"] == "poll"
    assert repins[0]["path"] == str(destination)
    assert repins[0]["from"] == before and repins[0]["to"] == after
    saved = tmp_path / "commit-answer.json"
    saved.write_text(json.dumps(answer))
    replay = spool.commit_origin_group("b1", [descriptor])
    assert replay["ok"] and replay["duplicate"]
    assert not replay.get("landed_repins"), "duplicate counted the old re-pin again"
    spool.queue.finish(spool.owner, status="executed")
    assert po._producer_attempt_state(spool.queue, spool.instance) == "succeeded"
    retired = ps.retire_namespace(spool.queue, spool.directory)
    assert retired.get("namespace_removed"), retired
    assert not (spool._group("b1") / "receipt.json").exists()
    assert json.loads(saved.read_text())["landed_repins"] == repins


def test_poll_and_commit_repins_are_both_retained(tmp_path, monkeypatch):
    spool = world(tmp_path)
    _, destination, _ = _landed(spool)
    descriptor = _descriptor(spool.instance, spool.template, destination, b"hello")
    _recall_ctime(destination)
    original = po.commit_origin_batch
    def commit(*args, **kwargs):
        _recall_mtime(destination)  # another recall after the spool poll
        return original(*args, **kwargs)
    monkeypatch.setattr(po, "commit_origin_batch", commit)
    answer = spool.commit_origin_group("b1", [descriptor])
    assert answer["ok"]
    repins = answer["landed_repins"]
    assert isinstance(repins, list)
    assert len(repins) == 2
    assert repins[0]["where"] == "poll"
    assert repins[0]["to"] == repins[1]["from"]
