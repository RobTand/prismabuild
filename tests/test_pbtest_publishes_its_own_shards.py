"""pbtest publishes each pool shard itself and reads it back like pbrun did.

Since pb-identity a plan's checkout is prepared once and each shard is sealed
off that template, published detached and observed by ``client_recovery``.
What a consumer reads must not move with it: each shard's lines are the
lines its ``pbrun`` printed (``pbrun: queued <key> ...`` and never the
detached JSON record), a refused preparation is each shard's refusal, an
offer-discovery timeout is still retried (#1102), and a checkout that moves
after its shards were sealed fails the run by name.
"""
from __future__ import annotations

import json
import subprocess
import sys
from types import SimpleNamespace

import pytest

from test_pbtest_identifies_its_checkout_once import _counted_run, _sealed, pbtest
from prismabuild import action_client, submission
from prismabuild.submission_types import OfferDiscoveryTimedOut, SubmissionRefused


def _rows(tmp_path):
    return json.loads((tmp_path / "report.json").read_text())


def test_each_shard_prints_what_its_pbrun_printed(tmp_path, monkeypatch, capsys):
    code, _, root = _counted_run(tmp_path, monkeypatch, shards=3)
    printed = capsys.readouterr()
    rows = _rows(tmp_path)
    keys = {action["action_key"] for action in _sealed(root)}
    assert {row["action_key"] for row in rows} == keys
    for row in rows:
        lines = row["output"].splitlines()
        queued = [line for line in lines if line.startswith("pbrun: queued ")]
        assert len(queued) == 1
        assert queued[0].startswith(f"pbrun: queued {row['action_key']} tags=")
        # Every line before it is a placement notice pbrun printed per shard.
        assert all(line.startswith("pbrun: ") for line in lines[:lines.index(queued[0])])
        assert action_client.DETACH_SCHEMA_V1 not in row["output"]
        assert '"schema"' not in row["output"]
        assert f"shard {row['shard']:>3} action {row['action_key']} (queued): " \
               f"{' '.join(row['files'])}" in printed.out
        for line in lines[:lines.index(queued[0]) + 1]:
            assert f"shard {row['shard']:>3} {line}" in printed.out
    assert "changed after its shards were sealed" not in printed.err
    assert code == 1


@pytest.mark.parametrize("change", ["uncommitted", "commit"])
def test_a_checkout_that_moves_after_publication_fails_the_run_by_name(
        tmp_path, monkeypatch, capsys, change):
    published = []
    real = submission.submit_action

    def submit_action(action, **kwargs):
        outcome = real(action, **kwargs)
        published.append(action["action_key"])
        if len(published) == 3:
            work = tmp_path / "work"
            if change == "uncommitted":
                (work / "tests" / "test_0.py").write_text("def test_0():\n    assert 0\n")
            else:
                subprocess.run(["git", "-C", str(work), "commit", "-q", "--allow-empty",
                                "-m", "moved"], check=True, timeout=30)
        return outcome

    monkeypatch.setattr(submission, "submit_action", submit_action)
    code, counts, root = _counted_run(tmp_path, monkeypatch, shards=3)
    err = capsys.readouterr().err
    assert code == 1
    assert counts == {"identity": 4, "bundle": 1}
    work = (tmp_path / "work").resolve()
    assert f"pbtest: the checkout {work} changed after its shards were sealed: " in err
    if change == "uncommitted":
        assert "its uncommitted changes are not the sealed ones (working-tree digest " in err
        assert "HEAD moved" not in err
    else:
        assert "HEAD moved from " in err
        assert "uncommitted" not in err
    # The rows are the run's ordinary rows: the shards ran the sealed tree.
    assert len(_rows(tmp_path)) == 3
    assert {row["verdict"]["schema"] for row in _rows(tmp_path)} == {
        "prismabuild.pbtest_shard_verdict.v1"}


def test_a_refused_preparation_is_each_shards_refusal_and_is_asked_once(
        tmp_path, monkeypatch):
    asked = []

    def prepare(args, **kwargs):
        asked.append(args)
        raise SubmissionRefused("pbrun: controlled refusal")

    monkeypatch.setattr(submission, "prepare", prepare)
    code, counts, root = _counted_run(tmp_path, monkeypatch, shards=3)
    assert len(asked) == 1
    assert counts == {"identity": 0, "bundle": 0}
    assert code == 1
    rows = _rows(tmp_path)
    assert [row["returncode"] for row in rows] == [1, 1, 1]
    assert all(row["output"] == "pbrun: controlled refusal\n" for row in rows)
    assert all(row["action_key"] is None and not row["ran"] for row in rows)
    assert not list((root / "pb-queue").rglob("ready/*.json"))


def test_an_offer_timeout_at_publication_is_retried_off_the_same_snapshot(
        tmp_path, monkeypatch):
    real = submission.bounded_offer_snapshot
    timeouts = []

    def bounded_offer_snapshot(queue):
        if not timeouts:
            timeouts.append(queue)
            raise OfferDiscoveryTimedOut("pbrun: controlled offer timeout")
        return real(queue)

    monkeypatch.setattr(submission, "bounded_offer_snapshot", bounded_offer_snapshot)
    monkeypatch.setattr(pbtest, "time", SimpleNamespace(**{
        **vars(pbtest.time), "sleep": lambda seconds: None,
    }))
    code, counts, root = _counted_run(tmp_path, monkeypatch, shards=3, wait_s=30)
    rows = _rows(tmp_path)
    assert sorted(row["attempts"] for row in rows) == [1, 1, 2]
    assert len(_sealed(root)) == 3
    assert counts == {"identity": 4, "bundle": 1}
    retried = next(row for row in rows if row["attempts"] == 2)
    assert retried["output"].startswith("pbrun: controlled offer timeout\n")
    assert f"\npbrun: queued {retried['action_key']} " in retried["output"]
