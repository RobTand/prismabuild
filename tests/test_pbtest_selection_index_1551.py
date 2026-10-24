"""Bound selection bookkeeping work, not elapsed time or production speed.

Real pytest traversal drives the selection hooks; no hooks are invoked manually.
Only the captured collection owner's source is traced, after index construction.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from prismabuild import pbtest_collection


# Runs as an ordinary pytest plugin in the isolated consumer and its workers.
PROBE = '''\
import json
from pathlib import Path
import sys
import types

SOURCE = {source!r}
SPEC = {spec!r}
DESTINATION = {destination!r}
OWNER_FILENAME = "<selection-owner-1551>"
owner = types.ModuleType("selection_owner_1551")
exec(compile(SOURCE, OWNER_FILENAME, "exec"), owner.__dict__)
counters = {{"parents": 0, "lines": 0}}
active = False

class CountedPath(type(Path())):
    @property
    def parents(self):
        if active:
            counters["parents"] += 1
        return super().parents

owner.Path = CountedPath
selection = owner.selection_plugin(SPEC)
assert selection.targets == ["tests"]

def trace_owner(frame, event, arg):
    if event == "line" and frame.f_code.co_filename == OWNER_FILENAME:
        counters["lines"] += 1
    return trace_owner

def pytest_configure(config):
    global active
    config.pluginmanager.register(selection, "selection-under-test")
    active = True
    sys.settrace(trace_owner)

def pytest_sessionfinish(session, exitstatus):
    global active
    active = False
    sys.settrace(None)
    worker = getattr(session.config, "workerinput", {{}}).get("workerid", "master")
    result = {{"counters": counters, "report": selection.report()}}
    Path(DESTINATION, worker + ".json").write_text(json.dumps(result))
'''


@pytest.fixture(params=[1, 2], ids=["serial", "xdist"])
def selection_observation(tmp_path, request):
    """Keep assigned depth/population fixed while visiting many foreign paths."""
    workers = request.param
    checkout = tmp_path / "consumer"
    tests = checkout / "tests"
    hidden = tests / "blocked" / "deep"
    hidden.mkdir(parents=True)
    (checkout / "pytest.ini").write_text("[pytest]\n")
    (tests / "conftest.py").write_text(
        "def pytest_ignore_collect(collection_path, config):\n"
        "    if collection_path.name in {'blocked', 'test_ignored.py'}:\n"
        "        return True\n"
    )
    visible = ["tests/test_visible.py", "tests/blocked_sibling/test_sibling.py"]
    assigned = [f"tests/blocked/deep/test_hidden_{index:03d}.py"
                for index in range(96)] + visible + ["tests/test_ignored.py"]
    for name in assigned:
        path = checkout / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("def test_visible(): pass\n" if name in visible
                        else "raise AssertionError('ignored file was imported')\n")
    for index in range(64):
        directory = tests / f"unassigned_{index:03d}"
        directory.mkdir()
        (directory / "test_foreign.py").write_text(
            "raise AssertionError('selection widened to an unassigned file')\n")
    destination = tmp_path / "observations"
    destination.mkdir()
    spec = {"files": assigned, "roots": [["tests", "directory"]]}
    (checkout / "selection_probe.py").write_text(PROBE.format(
        source=Path(pbtest_collection.__file__).read_text(), spec=spec,
        destination=str(destination)))
    command = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
               "-p", "selection_probe"]
    if workers > 1:
        command += ["-p", "xdist.plugin", "-n", str(workers)]
    command += ["tests"]
    environment = dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
                       PYTEST_ADDOPTS="", PYTHONPATH=str(checkout))
    result = subprocess.run(command, cwd=checkout, env=environment,
                            capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "2 passed" in result.stdout, result.stdout + result.stderr
    ids = ["master"] if workers == 1 else ["gw0", "gw1"]
    observations = [json.loads((destination / f"{worker}.json").read_text())
                    for worker in ids]
    expected_ignored = sorted(set(assigned) - set(visible))
    for observation in observations:
        # Directory and exact-file ignores preserve same-prefix siblings.
        assert observation["report"] == {"files": assigned, "ignored": expected_ignored}
    return observations, len(assigned), 64


def test_ignore_recording_does_not_rewalk_assigned_parents(selection_observation):
    observations, _, _ = selection_observation
    for observation in observations:
        assert observation["counters"]["parents"] == 0, (
            "Build ancestor/descendant membership once, not for each ignored path", observation)


def test_ignore_recording_does_not_rescan_the_assigned_population(selection_observation):
    observations, assigned_count, foreign_count = selection_observation
    # A deliberately generous structural budget for this fixed-depth tree.
    # Repeated Python scans of all assigned files exceed it; indexed lookup
    # plus processing actual matched descendants stays comfortably below it.
    budget = 30 * (assigned_count + foreign_count)
    for observation in observations:
        assert observation["counters"]["lines"] <= budget, (
            "Ignore bookkeeping must be indexed, not assigned-files x visited-paths", observation)
