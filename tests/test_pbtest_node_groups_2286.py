"""Long files split into current pytest node groups with complete coverage."""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from prismabuild import pbtest_collection
from test_digest_sites_1547 import fleet_module
from pbtest_shard_output import recorded_shard_evidence


FILE = "tests/test_long.py"
NODES = [f"{FILE}::test_value[{index}]" for index in range(6)]


def _group(index, count=2, durations=None):
    return {"index": index, "count": count, "durations": durations or {}}


def _row(index, nodes, *, roster=NODES, group=None):
    group = _group(index) if group is None else group
    groups = {FILE: group}
    record = {
        "schema": "prismabuild.pbtest_outcomes.v1", "rootdir_relative": ".",
        "collect_only": False, "collected": nodes,
        "deselected": [node for node in roster if node not in nodes],
        "reports": [[node, "call", "passed", None, None] for node in nodes],
        "uncounted": [], "file_selection": {
            "files": [FILE], "ignored": [], "node_groups": groups,
            "node_rosters": {FILE: roster}},
    }
    return {"shard": index, "files": [FILE], "returncode": 0,
            "node_groups": groups, "record_binding": 1,
            "output": recorded_shard_evidence(record)
                      + f"\n{len(nodes)} passed in 0.01s\n"}


def test_node_groups_balance_recorded_work_and_include_new_nodes():
    durations = {NODES[0]: 100.0, **{node: 10.0 for node in NODES[1:5]}}
    assert pbtest_collection.partition_nodes(NODES, 2, durations) == [
        [NODES[0]], NODES[1:]]
    assert pbtest_collection.partition_nodes(NODES, 2, {}) == [
        NODES[::2], NODES[1::2]]


@pytest.mark.parametrize("workers", [1, 2])
def test_real_shards_collect_new_parameter_nodes_once(tmp_path, workers):
    if workers > 1:
        pytest.importorskip("xdist")
    pbtest = fleet_module("pbtest")
    checkout = tmp_path / "consumer"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "pytest.ini").write_text("[pytest]\n")
    (checkout / FILE).write_text(
        "import pytest\n"
        "@pytest.mark.parametrize('value', range(6))\n"
        "def test_value(value):\n    assert value >= 0\n")
    environment = dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
    environment.pop("PYTHONPATH", None)
    rows = []
    for index in range(2):
        groups = {FILE: _group(index, durations={NODES[0]: 100.0})}
        spec = {"files": [FILE], "roots": [["tests", "directory"]],
                "node_groups": groups}
        xdist = ["-p", "xdist.plugin", "-n", str(workers)] if workers > 1 else []
        result = subprocess.run([
            *pbtest.shard_entry(sys.executable, checkout, collection=True,
                                tmpdir=str(tmp_path)),
            json.dumps(spec), "-q", "-p", "no:cacheprovider", *xdist, FILE,
        ], cwd=checkout, env=environment, capture_output=True, text=True)
        assert result.returncode == 0, result.stdout + result.stderr
        rows.append({"shard": index, "files": [FILE], "returncode": 0,
                     "node_groups": groups, "record_binding": 1,
                     "output": result.stdout})
    verdict = pbtest.pbtest_outcomes.judge_shards(rows, requested_files=[FILE])
    assert verdict["green"], verdict
    selected = [node for shard in verdict["shards"] for node in shard["collected"]]
    assert sorted(selected) == NODES
    assert all(shard["collected"] for shard in verdict["shards"])
    for row in rows:
        record = pbtest.pbtest_outcomes.parse(row["output"], record_binding=1)
        assert set(record["node_durations"]) == set(record["collected"])
        assert sum(record["node_durations"].values()) == pytest.approx(
            sum(record["file_durations"].values()), abs=0.004)


def test_group_verdict_requires_complete_disjoint_ownership():
    outcomes = fleet_module("pbtest_outcomes")
    rows = [_row(0, NODES[::2]), _row(1, NODES[1::2])]
    assert outcomes.judge_shards(rows, requested_files=[FILE])["green"]
    missing = outcomes.judge_shards(rows[:1], requested_files=[FILE])
    assert not missing["green"]
    assert "node_group_assignment" in {problem["code"] for problem in missing["problems"]}


def test_ungrouped_file_cannot_be_assigned_twice():
    outcomes = fleet_module("pbtest_outcomes")
    rows = [_row(0, NODES[::2]), _row(1, NODES[1::2])]
    for row in rows:
        row.pop("node_groups")
    verdict = outcomes.judge_shards(rows, requested_files=[FILE])
    assert not verdict["green"]
    assert "assigned_duplicate" in {problem["code"] for problem in verdict["problems"]}


def test_groups_must_agree_on_current_collection():
    outcomes = fleet_module("pbtest_outcomes")
    rows = [_row(0, NODES[::2]), _row(1, NODES[1::2], roster=NODES[:-1])]
    verdict = outcomes.judge_shards(rows, requested_files=[FILE])
    assert not verdict["green"]
    assert "node_group_assignment" in {problem["code"] for problem in verdict["problems"]}


def test_declared_group_cannot_be_changed_by_stdout():
    outcomes = fleet_module("pbtest_outcomes")
    rows = [_row(0, NODES[::2]), _row(1, NODES[1::2])]
    rows[0] = copy.deepcopy(rows[0])
    rows[0]["node_groups"][FILE]["index"] = 1
    assert not outcomes.judge_shards(rows, requested_files=[FILE])["green"]


def test_zero_duration_nodes_still_fill_every_group():
    assert pbtest_collection.partition_nodes(NODES, 3, dict.fromkeys(NODES, 0.0)) == [
        NODES[::3], NODES[1::3], NODES[2::3]]


def test_grouped_shards_keep_native_ignore_and_keyword_selection(tmp_path):
    pbtest = fleet_module("pbtest")
    checkout = tmp_path / "consumer"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "pytest.ini").write_text("[pytest]\n")
    ignored = "tests/test_ignored.py"
    (checkout / "conftest.py").write_text(f"collect_ignore = [{ignored!r}]\n")
    (checkout / ignored).write_text("raise AssertionError('ignored module imported')\n")
    (checkout / FILE).write_text(
        "def test_keep(): pass\ndef test_drop(): raise AssertionError('deselected test ran')\n")
    environment = dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
    environment.pop("PYTHONPATH", None)
    rows = []
    for index in range(2):
        files = [FILE, ignored] if index == 0 else [FILE]
        groups = {FILE: _group(index)}
        spec = {"files": files, "roots": [["tests", "directory"]],
                "node_groups": groups}
        result = subprocess.run([
            *pbtest.shard_entry(sys.executable, checkout, collection=True,
                                tmpdir=str(tmp_path)),
            json.dumps(spec), "-q", "-p", "no:cacheprovider", "-k", "keep", *files,
        ], cwd=checkout, env=environment, capture_output=True, text=True)
        assert result.returncode == 0, result.stdout + result.stderr
        rows.append({"shard": index, "files": files, "returncode": 0,
                     "node_groups": groups, "record_binding": 1,
                     "output": result.stdout})
    verdict = pbtest.pbtest_outcomes.judge_shards(rows, requested_files=[FILE, ignored])
    assert verdict["green"], verdict
    assert [shard["collected"] for shard in verdict["shards"]] == [
        [f"{FILE}::test_keep"], []]


def test_large_node_map_survives_real_exec_and_preserves_current_nodes(tmp_path):
    checkout = tmp_path / "consumer"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "pytest.ini").write_text("[pytest]\n")
    (checkout / FILE).write_text(
        "import pytest\n@pytest.mark.parametrize('value', range(6))\n"
        "def test_value(value): assert value >= 0\n")
    durations = {f"{FILE}::test_old[{index}-{'x' * 64}]": 1.0 for index in range(3000)}
    durations.update(dict.fromkeys(NODES, 0.0))
    spec = {"files": [FILE], "roots": [["tests", "directory"]],
            "node_groups": {FILE: _group(0, durations=durations)}}
    raw = json.dumps(spec)
    assert len(raw.encode()) > 131072
    encoded = getattr(pbtest_collection, "encode_selection", json.dumps)(spec)
    program = '''\
import json, sys, pytest
from prismabuild.pbtest_collection import decode_selection, selection_plugin
selection = selection_plugin(decode_selection(sys.argv[1]))
class Probe:
    def pytest_collection_finish(self, session):
        print("selected-nodeids:" + json.dumps([item.nodeid for item in session.items]))
raise SystemExit(pytest.main(["-q", *selection.targets], plugins=[selection, Probe()]))
'''
    environment = dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
                       PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"))
    try:
        result = subprocess.run([sys.executable, "-c", program, encoded], cwd=checkout,
                                env=environment, capture_output=True, text=True)
    except OSError as exc:
        pytest.fail(f"cannot exec the sealed node selection: {exc}", pytrace=False)
    assert result.returncode == 0, result.stdout + result.stderr
    selected = next(line.removeprefix("selected-nodeids:") for line in result.stdout.splitlines()
                    if line.startswith("selected-nodeids:"))
    assert json.loads(selected) == NODES[::2]


def test_selection_encoding_preserves_ordinary_json_bytes():
    spec = {"files": [FILE], "roots": [["tests", "directory"]]}
    assert pbtest_collection.encode_selection(spec) == json.dumps(spec)
    assert pbtest_collection.decode_selection(json.dumps(spec)) == spec
