"""pbtest forwards one pytest ini override, the tmp retention policy (#1535, D29).

A repository whose own config does not set ``tmp_path_retention_policy`` still
needs to choose it on an inode-limited scratch, and a sealed checkout cannot be
edited for that.  The closed vocabulary of ``--pytest-args`` grows by exactly
this key and its three pytest values; every other ``-o`` stays refused.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from pbtest_shard_output import admitted_child

from test_pbtest_reserves_its_threads import _dispatch, pbtest

KEY = "tmp_path_retention_policy"


@pytest.mark.parametrize("value", ["all", "failed", "none"])
@pytest.mark.parametrize("spelling", ["short", "long", "long-equals"])
def test_the_retention_policy_is_forwarded_in_its_own_spelling(spelling, value):
    argv = {"short": ["-o", f"{KEY}={value}"],
            "long": ["--override-ini", f"{KEY}={value}"],
            "long-equals": [f"--override-ini={KEY}={value}"]}[spelling]
    option = "-o" if spelling == "short" else "--override-ini"
    assert pbtest.parse_pytest_args(json.dumps(argv), gpu=False, workers=1) == [
        option, f"{KEY}={value}"]


@pytest.mark.parametrize("argv", [
    ["-o", f"{KEY}=bogus"],
    ["-o", f"{KEY}="],
    ["-o", f"{KEY}"],
    ["-o", f"{KEY} =none"],
    ["-o", f"{KEY}=none "],
    ["-o", f"{KEY}=None"],
    ["-o", f"{KEY}=none,all"],
    ["-o", "tmp_path_retention_count=0"],
    ["-o", "addopts=-n80"],
    ["-o", "python_files=*.py"],
    ["-o", "testpaths=/"],
    ["-o", "xfail_strict=true"],
    ["-o", "-n80"],
    ["-o"],
    ["--override-ini"],
    ["--override-ini=addopts=-n80"],
    ["-oaddopts=-n80"],
    [f"-o{KEY}=none"],
    ["-o", f"{KEY}=none", "-o", f"{KEY}=all"],
    ["-o", f"{KEY}=none", "--override-ini", f"{KEY}=none"],
    ["-o", f"{KEY}=none", "-o", "addopts=-n80"],
])
def test_every_other_override_stays_refused(argv):
    with pytest.raises(ValueError):
        pbtest.parse_pytest_args(json.dumps(argv), gpu=False, workers=1)


def test_the_resource_and_config_guards_are_unchanged():
    for argv in (["-n", "8"], ["-c", "alternate.ini"], ["--rootdir", "/"],
                 ["--basetemp", "x"], ["-p", "xdist"]):
        with pytest.raises(ValueError, match="unsupported pytest option"):
            pbtest.parse_pytest_args(json.dumps(argv), gpu=False, workers=1)


def test_the_policy_reaches_the_sealed_shard_command(tmp_path, monkeypatch):
    code, calls = _dispatch(tmp_path, monkeypatch, [
        "--timeout-s", "3600",
        "--pytest-args", json.dumps(["-o", f"{KEY}=none", "-k", "fast"])])
    assert code == 0
    command, = calls
    payload = command[command.index("--") + 1:]
    # pbtest seals its own empty addopts as an -o too, so look at every one.
    overrides = [payload[i + 1] for i, word in enumerate(payload) if word == "-o"]
    assert f"{KEY}=none" in overrides and overrides.count("addopts=") == 1
    assert payload[payload.index("-k") + 1] == "fast"


def test_a_refused_override_submits_nothing(tmp_path, monkeypatch):
    code, calls = _dispatch(tmp_path, monkeypatch, [
        "--pytest-args", json.dumps(["-o", f"{KEY}=none", "-o", "addopts=-n80"])])
    assert code == 2
    assert not calls


def _inner(tmp_path: Path, ini_policy: str, argv: list[str]) -> list[Path]:
    """Run one passing test in a sealed shard; return the tmp dirs left behind."""
    root = tmp_path / "checkout"
    root.mkdir(parents=True)
    (root / "pytest.ini").write_text(f"[pytest]\n{KEY} = {ini_policy}\n")
    (root / "test_fixture.py").write_text(
        "def test_passes(tmp_path):\n    (tmp_path / 'marker').write_text('x')\n")
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    entry = pbtest.shard_entry(sys.executable, root)
    command = [*entry, "-q", "-p", "no:cacheprovider", *argv, "test_fixture.py"]
    result = subprocess.run(command, cwd=root, text=True, capture_output=True,
                            timeout=120, env={**os.environ,
                                              "PYTEST_DEBUG_TEMPROOT": str(scratch)})
    assert result.returncode == 0, result.stdout + result.stderr
    return sorted(scratch.rglob("marker"))


def test_the_command_line_policy_overrides_the_checkouts_config(tmp_path):
    """The shipcard case: the checkout's own config drops the passing test's
    directory, and the forwarded policy keeps it."""

    assert _inner(tmp_path / "a", "failed", []) == []
    kept = _inner(tmp_path / "b", "failed", ["-o", f"{KEY}=all"])
    assert len(kept) == 1
    assert _inner(tmp_path / "c", "all", ["-o", f"{KEY}=none"]) == []


def test_the_policy_is_in_the_shard_command_that_keys_the_action(tmp_path, monkeypatch):
    """Two submissions that differ only in retention build different shard commands."""

    with monkeypatch.context() as scope:
        _, first = _dispatch(tmp_path / "x", scope, [
            "--pytest-args", json.dumps(["-o", f"{KEY}=none"])])
    with monkeypatch.context() as scope:
        _, second = _dispatch(tmp_path / "y", scope, [
            "--pytest-args", json.dumps(["-o", f"{KEY}=all"])])
    assert first[0][first[0].index("--") + 1:] != second[0][second[0].index("--") + 1:]
