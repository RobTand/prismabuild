"""A shard's base temp does not outlive its usefulness.

#1542 gave each attempt its own base temp and removed it after exit 0, but
kept it forever after anything else: 1,084 kept ``pb-XXXXXXXX`` directories
(about 17 GB) filled dl380g10's root filesystem on 2026-10-15. A sealed
``--basetemp`` leaf was never removed at all, and the exit-0 removal itself
gave up on a test's read-only directory, leaving the tree behind.

These tests drive real shard programs and look only at the filesystem, so
they say the same thing about any layout: a passing attempt leaves nothing,
a failed or killed attempt stays for its retention window and then goes, a
live attempt stays at any age. "Ageing" an attempt backdates every entry in
its directory, which is where an attempt keeps its retention clock.
"""
from __future__ import annotations

import importlib.util
import json
import os
import signal
import stat
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest_expiry", ROOT / "tools" / "fleet" / "pbtest.py")
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)

ACTION_KEY = "ef56" * 16
MONTH_S = 30 * 24 * 3600
DAY_AND_AN_HOUR_S = 25 * 3600

READ_ONLY = (
    "    ro = tmp_path / 'ro'\n"
    "    ro.mkdir()\n"
    "    (ro / 'kept.txt').write_text('read-only parent')\n"
    "    ro.chmod(0o555)\n")
BLOCKING = (
    "    import time\n"
    "    from pathlib import Path\n"
    "    here = Path(__file__).parent\n"
    "    (here / 'started').write_text('1')\n"
    "    deadline = time.monotonic() + 120\n"
    "    while not (here / 'release').exists():\n"
    "        assert time.monotonic() < deadline\n"
    "        time.sleep(0.05)\n")


def _checkout(parent: Path, body: str) -> Path:
    checkout = parent / "co"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "tests" / "test_one.py").write_text(
        "def test_one(tmp_path):\n"
        "    (tmp_path / 'marker').write_text('x')\n"
        f"{body}")
    return checkout


def _command(checkout: Path, basetemp: Path | None) -> list[str]:
    program = pbtest.shard_entry(
        sys.executable, checkout, collection=True,
        basetemp=None if basetemp is None else str(basetemp))[2]
    return [sys.executable, "-c", program,
            json.dumps({"files": ["tests/test_one.py"],
                        "roots": [["tests", "directory"]]}),
            "-q", "--no-header", "-p", "no:cacheprovider", "tests/test_one.py"]


def _environment(scratch: Path) -> dict[str, str]:
    return dict(os.environ, TMPDIR=str(scratch), PRISMABUILD_ACTION_KEY=ACTION_KEY,
                PRISMABUILD_ACTION_NONCE=uuid.uuid4().hex,
                PYTHONDONTWRITEBYTECODE="1", PYTEST_ADDOPTS="")


def _shard(checkout: Path, scratch: Path, basetemp: Path | None = None
           ) -> subprocess.CompletedProcess:
    return subprocess.run(_command(checkout, basetemp), cwd=checkout,
                          env=_environment(scratch), capture_output=True,
                          text=True, timeout=240)


def _background(checkout: Path, scratch: Path, log: Path) -> subprocess.Popen:
    with log.open("w") as output:
        process = subprocess.Popen(_command(checkout, None), cwd=checkout,
                                   env=_environment(scratch), stdout=output,
                                   stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 120
    while not (checkout / "tests" / "started").exists():
        assert process.poll() is None, log.read_text()
        assert time.monotonic() < deadline, log.read_text()
        time.sleep(0.05)
    return process


def _tree(root: Path) -> list[str]:
    return sorted(path.relative_to(root).as_posix() for path in root.rglob("*"))


def _age(directory: Path, seconds: float) -> None:
    """Backdate an attempt directory and every entry directly inside it."""

    then = time.time() - seconds
    for path in (directory, *directory.iterdir()):
        os.utime(path, (then, then), follow_symlinks=False)


def _writable(root: Path) -> None:
    """Give a test's read-only directories back so tmp_path cleanup works."""

    for path in root.rglob("*"):
        if path.is_dir() and not path.is_symlink():
            path.chmod(0o700)


def test_a_passing_shard_removes_a_read_only_directory_in_its_base(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    checkout = _checkout(tmp_path, READ_ONLY)
    result = _shard(checkout, scratch)
    assert result.returncode == 0, result.stdout + result.stderr
    try:
        assert _tree(scratch) == [], "exit 0 left a partial base temp behind"
    finally:
        _writable(scratch)


def test_a_failed_base_is_kept_through_its_window_then_reaped(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    failing = _checkout(tmp_path / "failing", "    assert False, 'kept'\n")
    result = _shard(failing, scratch)
    assert result.returncode == 1, result.stdout + result.stderr
    (kept,) = scratch.iterdir()
    assert list(kept.rglob("marker")), "the failed test's tmp_path is gone"

    passing = _checkout(tmp_path / "passing", "")
    assert _shard(passing, scratch).returncode == 0
    assert list(scratch.iterdir()) == [kept], "reaped inside its window"

    _age(kept, DAY_AND_AN_HOUR_S)
    assert _shard(passing, scratch).returncode == 0
    assert _tree(scratch) == [], "an expired failed base outlived its window"


def test_an_expired_failed_base_with_a_read_only_directory_is_removed(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    failing = _checkout(tmp_path / "failing", READ_ONLY + "    assert False\n")
    assert _shard(failing, scratch).returncode == 1
    (kept,) = scratch.iterdir()
    assert list(kept.rglob("kept.txt"))
    _age(kept, DAY_AND_AN_HOUR_S)
    passing = _checkout(tmp_path / "passing", "")
    assert _shard(passing, scratch).returncode == 0
    try:
        assert _tree(scratch) == [], "expiry left a read-only directory behind"
    finally:
        _writable(scratch)


def test_a_killed_shard_base_is_reaped_once_its_window_passes(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    blocked = _checkout(tmp_path / "blocked", BLOCKING)
    process = _background(blocked, scratch, tmp_path / "blocked.log")
    process.send_signal(signal.SIGKILL)
    assert process.wait(timeout=60) == -signal.SIGKILL
    (killed,) = scratch.iterdir()

    # The first attempt to find its owner gone starts the clock; nothing
    # goes before the window has passed from then.
    passing = _checkout(tmp_path / "passing", "")
    assert _shard(passing, scratch).returncode == 0
    assert list(scratch.iterdir()) == [killed]

    _age(killed, DAY_AND_AN_HOUR_S)
    assert _shard(passing, scratch).returncode == 0
    assert _tree(scratch) == [], "a dead shard's base outlived its window"


def test_a_live_shard_base_is_never_reaped_at_any_age(tmp_path):
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    blocked = _checkout(tmp_path / "blocked", BLOCKING)
    process = _background(blocked, scratch, tmp_path / "blocked.log")
    try:
        (live,) = scratch.iterdir()
        _age(live, MONTH_S)
        passing = _checkout(tmp_path / "passing", "")
        for _ in range(2):
            assert _shard(passing, scratch).returncode == 0
            assert list(scratch.iterdir()) == [live]
            assert list(live.rglob("marker")), "a live shard lost its tmp_path"
    finally:
        (blocked / "tests" / "release").write_text("1")
        returncode = process.wait(timeout=120)
    assert returncode == 0, (tmp_path / "blocked.log").read_text()
    assert _tree(scratch) == []


def test_a_passing_sealed_basetemp_shard_removes_its_attempt_leaf(tmp_path):
    scratch, root = tmp_path / "scratch", tmp_path / "sealed"
    scratch.mkdir()
    root.mkdir()
    checkout = _checkout(tmp_path, READ_ONLY)
    result = _shard(checkout, scratch, basetemp=root)
    assert result.returncode == 0, result.stdout + result.stderr
    try:
        assert root.is_dir(), "the sealed root itself belongs to its caller"
        assert _tree(root) == [], "exit 0 left the sealed attempt leaf behind"
        assert _tree(scratch) == []
    finally:
        _writable(root)


def test_a_failed_sealed_leaf_is_kept_through_its_window_then_reaped(tmp_path):
    scratch, root = tmp_path / "scratch", tmp_path / "sealed"
    scratch.mkdir()
    root.mkdir()
    failing = _checkout(tmp_path / "failing", "    assert False\n")
    assert _shard(failing, scratch, basetemp=root).returncode == 1
    (key,) = root.iterdir()
    assert key.name == ACTION_KEY
    (attempt,) = key.iterdir()
    assert list(attempt.rglob("marker"))

    passing = _checkout(tmp_path / "passing", "")
    assert _shard(passing, scratch, basetemp=root).returncode == 0
    assert list(root.iterdir()) == [key] and list(key.iterdir()) == [attempt], (
        "reaped inside its window, or the passing leaf stayed")

    _age(attempt, DAY_AND_AN_HOUR_S)
    assert _shard(passing, scratch, basetemp=root).returncode == 0
    assert root.is_dir()
    assert _tree(root) == [], "an expired sealed leaf outlived its window"


def test_the_sealed_root_keeps_its_mode_and_survives_its_last_leaf(tmp_path):
    """The root is the caller's: removal never chmods or removes it."""

    scratch, root = tmp_path / "scratch", tmp_path / "sealed"
    scratch.mkdir()
    root.mkdir(mode=0o750)
    root.chmod(0o750)
    checkout = _checkout(tmp_path, "")
    assert _shard(checkout, scratch, basetemp=root).returncode == 0
    assert stat.S_IMODE(root.stat().st_mode) == 0o750
    assert _tree(root) == []
