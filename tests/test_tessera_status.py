"""The status screen, read under both transports.

This is the one command a person runs to look at the fleet, so its failure
mode is not a crash: it is a line that still prints and no longer means what
it says.  Two of those arrive with SLURM.

``ready`` and ``claimed`` are pull-queue directories.  Under SLURM nothing is
ever written to them -- the pending work is in the scheduler -- so the queue
line reads ``{'ready': 0, 'claimed': 0}`` beside a controller holding forty
jobs, which is worse than printing nothing.

And a withdrawal is filed twice by design: a marker under ``withdrawn/`` and a
terminal record under ``failed/``, because ``pool_reset`` reads the first and
``merge_suite`` reads the second.  Counting both makes one cancellation look
like a cancellation plus a failure.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import textwrap

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

import tessera_status  # noqa: E402


_SQUEUE = '''\
import os, sys
rows = os.environ.get("FAKE_SQUEUE_ROWS", "")
if os.environ.get("FAKE_SQUEUE_BROKEN") == "1":
    sys.stderr.write("squeue: error: Unable to contact slurm controller\\n")
    raise SystemExit(1)
for row in rows.split(";"):
    if row.strip():
        print(row.strip())
'''


@pytest.fixture()
def squeue(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A fake ``squeue -h -o '%P|%T'`` whose rows the test dictates."""

    binaries = tmp_path / "bin"
    binaries.mkdir()
    script = binaries / "squeue"
    script.write_text(f"#!{sys.executable}\n" + textwrap.dedent(_SQUEUE),
                      encoding="utf-8")
    script.chmod(0o755)
    monkeypatch.setenv("PATH", f"{binaries}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv(
        "FAKE_SQUEUE_ROWS",
        "gb10|PENDING;gb10|PENDING;gb10|RUNNING;x86|RUNNING;x86|COMPLETING",
    )
    return script


def _queue(tmp_path: Path, **states: list[str]) -> Path:
    root = tmp_path / "pb-queue"
    for state, keys in states.items():
        directory = root / state
        directory.mkdir(parents=True, exist_ok=True)
        for key in keys:
            (directory / f"{key}.json").write_text(
                json.dumps({"action_key": key}), encoding="utf-8")
    (root / "withdrawn" / "superseded").mkdir(parents=True, exist_ok=True)
    return root


def test_under_slurm_the_depth_comes_from_squeue(tmp_path, squeue) -> None:
    """Pending and running per partition, which is what ``ready`` used to mean."""

    depth = tessera_status.squeue_depth(squeue=str(squeue))
    assert depth == {"gb10": {"pending": 2, "running": 1},
                     "x86": {"pending": 0, "running": 1}}
    line = tessera_status.describe_squeue_depth(squeue=str(squeue))
    assert "gb10 2 pending" in line and "1 running" in line


def test_under_slurm_the_queue_line_does_not_report_pull_queue_depth(
    tmp_path, squeue,
) -> None:
    """A zero that means "this directory is unused" is not a zero worth printing."""

    root = _queue(tmp_path, ready=[], claimed=[], done=["a" * 64],
                  failed=["b" * 64])
    counts = tessera_status.queue_counts(root, transport="slurm")
    assert "ready" not in counts and "claimed" not in counts
    assert counts["done"] == 1 and counts["failed"] == 1


def test_the_pull_queue_view_is_kept_while_the_pool_is_the_transport(
    tmp_path,
) -> None:
    root = _queue(tmp_path, ready=["c" * 64], claimed=["d" * 64],
                  done=["a" * 64], failed=["b" * 64])
    counts = tessera_status.queue_counts(root, transport="pool")
    assert counts["ready"] == 1 and counts["claimed"] == 1


def test_a_withdrawn_action_is_counted_once(tmp_path) -> None:
    """The marker and the failed record are one cancellation, not two."""

    key = "e" * 64
    root = _queue(tmp_path, done=[], failed=[key, "f" * 64], withdrawn=[key])
    for transport in ("pool", "slurm"):
        counts = tessera_status.queue_counts(root, transport=transport)
        assert counts["withdrawn"] == 1, transport
        assert counts["failed"] == 1, transport


def test_a_controller_that_cannot_be_reached_is_reported_not_raised(
    tmp_path, squeue, monkeypatch,
) -> None:
    """A status script must never be the thing that fails."""

    monkeypatch.setenv("FAKE_SQUEUE_BROKEN", "1")
    line = tessera_status.describe_squeue_depth(squeue=str(squeue))
    assert line.startswith("unavailable")
    assert tessera_status.describe_squeue_depth(
        squeue=str(tmp_path / "no-such-squeue")).startswith("unavailable")
