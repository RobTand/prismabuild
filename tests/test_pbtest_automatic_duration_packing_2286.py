"""Default history and sub-file quanta exercise the generated shard program."""
import json

from test_pbtest_balances_shards_by_measured_duration import (
    _dispatch, _prior_run, pbtest,
)


def _project(tmp_path):
    checkout = tmp_path / "project"
    tests = checkout / "tests"
    tests.mkdir(parents=True)
    (checkout / "pytest.ini").write_text("[pytest]\n")
    (tests / "test_long.py").write_text(
        "import pytest\n"
        "@pytest.mark.parametrize('case', range(6))\n"
        "def test_case(case): pass\n")
    for name in ("a", "b"):
        (tests / f"test_{name}.py").write_text("def test_small(): pass\n")
    return checkout


def test_next_run_packs_from_recorded_durations_without_history(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    checkout = _project(tmp_path)
    code, first = _dispatch(checkout, monkeypatch, ["--shards", "3"])
    assert code == 0
    assert all(row["predicted_s"] is None for row in first)
    code, second = _dispatch(checkout, monkeypatch, ["--shards", "3"])
    assert code == 0
    assert all(row["predicted_s"] is not None for row in second)


def test_long_file_is_spread_across_shards_and_every_current_node_runs_once(
        tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    checkout = _project(tmp_path)
    prior = _prior_run(tmp_path, {
        "tests/test_long.py": 600.0,
        "tests/test_a.py": 30.0,
        "tests/test_b.py": 30.0,
    })
    code, results = _dispatch(checkout, monkeypatch,
                              ["--shards", "3", "--history", str(prior)])
    assert code == 0
    long_shards = [row for row in results if "tests/test_long.py" in row["files"]]
    assert len(long_shards) == 3
    selected = [node for row in results for node in row["verdict"]["collected"]]
    expected = [f"tests/test_long.py::test_case[{i}]" for i in range(6)]
    expected += [f"tests/test_{name}.py::test_small" for name in ("a", "b")]
    assert sorted(selected) == sorted(expected)
    assert sorted(len([node for node in row["verdict"]["collected"]
                       if node.startswith("tests/test_long.py::")])
                  for row in long_shards) == [2, 2, 2]
    assert max(row["predicted_s"] for row in results) == 230.0


def test_split_file_samples_are_summed_within_each_history_report(tmp_path):
    prior = _prior_run(tmp_path, {"tests/test_long.py": 200.0})
    rows = json.loads(prior.read_text())
    rows.append(dict(rows[0], shard=1))
    prior.write_text(json.dumps(rows))
    history, problems = pbtest.load_history([str(prior)])
    assert not problems
    assert history == {"tests/test_long.py": [400.0]}


def test_an_unmeasured_file_above_target_uses_the_same_node_split_rule():
    plans = pbtest.plan_shards(["tests/test_small.py", "tests/test_new.py"], 3,
        durations={"tests/test_small.py": 10.0}, model_default=1000.0)
    assert len(plans) == 3
    assert sorted(plan.node_groups["tests/test_new.py"]["index"]
                  for plan in plans) == [0, 1, 2]


def test_zero_second_file_samples_keep_independent_shards():
    files = ["tests/test_a.py", "tests/test_b.py"]
    assert pbtest.allocate_shards({(): 0.0}, 2) == {(): 2}
    assert pbtest.shard(files, 2, durations=dict.fromkeys(files, 0.0)) == [
        [files[0]], [files[1]]]
