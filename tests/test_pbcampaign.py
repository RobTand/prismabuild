"""A manifest is N pbrun invocations, and it has to be exactly that.

The property the whole tool rests on is identity: a row's action key must be
the key a hand-typed ``pbrun`` produces for the same row, or a campaign cannot
be reproduced at the terminal and a re-run is not a cache hit.  That is checked
here against a deliberately differently-spelled command line -- different flag
order, ``--cpus`` where the row says ``demand.cpu``, tags the other way round
-- because those are the differences a person actually types.
"""
from __future__ import annotations

import json
from pathlib import Path
import socket
import sys
import threading
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import pool  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import pbrun  # noqa: E402
import pbcampaign  # noqa: E402

from test_pbrun_detach import _checkout, _queue, _one_json_line  # noqa: E402

CAPACITY = {"cpu": 8, "mem_gb": 16, "gpu": 1}


@pytest.fixture()
def fleet_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Point every tool at one private fleet, as the boxes share one for real."""

    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.01)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    return _checkout(tmp_path), _queue(tmp_path)


def _manifest(tmp_path: Path, rows) -> str:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(rows), encoding="utf-8")
    return str(path)


def _row(work: Path, script: str, **fields) -> dict:
    return {"argv": ["/bin/bash", "-lc", script], "cwd": str(work), **fields}


def _drain(queue, count: int, stop: threading.Event) -> None:
    """A worker loop, for as long as the campaign needs one."""

    served = 0
    while served < count and not stop.is_set():
        outcome = queue.serve_once(
            tags=["sparky", "gb10"], python=sys.executable, timeout_s=120.0,
            capacity=dict(CAPACITY),
        )
        if outcome is None:
            time.sleep(0.01)
            continue
        served += 1


def _campaign_against_a_worker(queue, manifest: str, *, rows: int, **kwargs):
    """Run the campaign with something draining the queue beside it."""

    stop = threading.Event()
    worker = threading.Thread(target=_drain, args=(queue, rows, stop))
    worker.start()
    try:
        return pbcampaign.main([*kwargs.pop("argv", []), manifest])
    finally:
        stop.set()
        worker.join(timeout=60)


# --------------------------------------------------------------------------
# Identity
# --------------------------------------------------------------------------

def test_a_campaign_rows_key_is_the_key_pbrun_would_have_produced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fleet_paths, capsys
) -> None:
    """Same action, two spellings.  If these differ, a campaign re-run is not a
    cache hit and nothing in the manifest can be reproduced by hand."""

    work, _queue_unused = fleet_paths
    target = _row(
        work, "printf hello",
        demand={"cpu": 4, "mem_gb": 16},
        tags=["gb10", "sparky"],
        env={"FOO": "bar"},
    )
    # Sealed second, so the first row's stamp droppings are already in the
    # checkout when this one is sealed.  They must not move identity.
    manifest = _manifest(tmp_path, [_row(work, "printf other"), target])
    assert pbcampaign.main(["--detach", manifest]) == 0
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()
             if line.strip()]
    assert len(lines) == 2
    from_campaign = lines[1]["action_key"]

    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--detach",
        "--tag", "sparky", "--tag", "gb10",
        "--env", "FOO=bar",
        "--cpus", "4",
        "--demand", "mem_gb=16",
        "--cwd", str(work),
        "--", "/bin/bash", "-lc", "printf hello",
    ])
    assert pbrun.main() == 0
    by_hand = _one_json_line(capsys.readouterr())["action_key"]

    assert from_campaign == by_hand


def test_a_row_is_one_pbrun_command_line_and_nothing_else() -> None:
    """The mapping is mechanical, so it can be read rather than trusted."""

    assert pbcampaign.pbrun_argv({
        "argv": ["python3", "-m", "pytest"],
        "cwd": "/home/rob/tree",
        "demand": {"mem_gb": 32, "gpu": 1},
        "tags": ["gb10"],
        "env": {"PYTHONPATH": "src"},
        "timeout_s": 600,
        "anywhere": True,
    }) == [
        "--cwd", "/home/rob/tree",
        "--timeout-s", "600",
        "--demand", "gpu=1,mem_gb=32",
        "--tag", "gb10",
        "--env", "PYTHONPATH=src",
        "--anywhere",
        "--", "python3", "-m", "pytest",
    ]


# --------------------------------------------------------------------------
# Endings
# --------------------------------------------------------------------------

def test_a_failed_row_fails_the_campaign_and_the_others_still_finish(
    tmp_path: Path, fleet_paths, capsys
) -> None:
    work, queue = fleet_paths
    manifest = _manifest(tmp_path, [
        _row(work, "printf worked > out.txt"),
        _row(work, "exit 3"),
        _row(work, "printf also-worked > other.txt"),
    ])
    assert _campaign_against_a_worker(queue, manifest, rows=3) == 1

    table = capsys.readouterr().out
    columns = [line.split() for line in table.splitlines()]
    assert columns[0][:2] == ["key", "status"]
    assert [line[1] for line in columns[1:]] == [
        "executed", "failed", "executed"], table
    # The returncode column, so the operator reads more than "something went
    # wrong".  Non-zero rather than 3: under the pull queue the status on the
    # record is the worker launcher's, and the command's own is in the attempt
    # output, so pinning 3 here would pin one transport's convention.
    rc = columns[0].index("rc")
    assert columns[1][rc] == "0" and columns[3][rc] == "0", table
    assert columns[2][rc] != "0", table


def test_re_running_the_same_manifest_runs_nothing(
    tmp_path: Path, fleet_paths, capsys
) -> None:
    """The point of a memoized action.  Not one new queue item, because a
    detached submission that reached a worker would be a box occupied to
    discover what the submitter already knew."""

    work, queue = fleet_paths
    manifest = _manifest(tmp_path, [
        _row(work, "printf one > one.txt"),
        _row(work, "printf two > two.txt"),
    ])
    assert _campaign_against_a_worker(queue, manifest, rows=2) == 0
    first = capsys.readouterr().out
    assert [line.split()[1] for line in first.splitlines()[1:]] == [
        "executed", "executed"]
    published_before = len(list(queue.dir(pool.DONE).glob("*.json")))

    assert pbcampaign.main([manifest]) == 0
    again = capsys.readouterr().out
    assert [line.split()[1] for line in again.splitlines()[1:]] == [
        "cache_hit", "cache_hit"], again
    assert not list(queue.dir(pool.READY).glob("*.json"))
    assert len(list(queue.dir(pool.DONE).glob("*.json"))) == published_before


def test_detach_prints_the_keys_and_does_not_wait(
    tmp_path: Path, fleet_paths, capsys
) -> None:
    """Nothing drains this queue, so a campaign that waited would sit here."""

    work, queue = fleet_paths
    manifest = _manifest(tmp_path, [
        _row(work, "printf a"), _row(work, "printf b")])
    assert pbcampaign.main(["--detach", manifest]) == 0

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()
             if line.strip()]
    assert [line["status"] for line in lines] == ["submitted", "submitted"]
    assert len({line["action_key"] for line in lines}) == 2
    assert len(list(queue.dir(pool.READY).glob("*.json"))) == 2


# --------------------------------------------------------------------------
# Manifests that cannot mean what they say
# --------------------------------------------------------------------------

def test_an_unknown_field_is_refused_before_anything_is_sealed(
    tmp_path: Path, fleet_paths
) -> None:
    """A dropped field seals an action nobody asked for, and the CAS then makes
    that wrong action permanent under a key that looks right."""

    work, queue = fleet_paths
    manifest = _manifest(tmp_path, [
        _row(work, "printf a"),
        {"argv": ["/bin/true"], "cwd": str(work), "timeout": 30},
    ])
    with pytest.raises(SystemExit) as raised:
        pbcampaign.main([manifest])
    assert "timeout" in str(raised.value)
    # Nothing was submitted: the refusal is at load, not at row two.
    assert not list(queue.dir(pool.READY).glob("*.json"))


def test_a_row_pbrun_refuses_does_not_stop_the_others(
    tmp_path: Path, fleet_paths, capsys
) -> None:
    """Forty rows are not worth losing to the one that named a checkout that is
    not there, and the refusal reaches the operator on the table with the rest."""

    work, queue = fleet_paths
    manifest = _manifest(tmp_path, [
        _row(work, "printf a"),
        _row(tmp_path / "not-a-checkout", "printf b"),
        _row(work, "printf c"),
    ])
    assert pbcampaign.main(["--detach", manifest]) == 1
    captured = capsys.readouterr()
    assert len([line for line in captured.out.splitlines() if line.strip()]) == 2
    assert "row 1 refused" in captured.err
    assert len(list(queue.dir(pool.READY).glob("*.json"))) == 2
