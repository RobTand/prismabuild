"""pbtest balances shards by measured per-file duration (#1246).

Round-robin by file balances only when files cost roughly the same, and they
do not: one 1513 s file shared with 18 others lost all 19 files' results when
its shard hit the 3600 s ceiling (action ``1afea851``).  The measurements to
pack by already flow through the shard: the recorder sees every report, and
each report carries its own duration.  So the outcome record carries each
file's summed wall time; a later run loads those numbers out of prior run
reports, isolates a file that alone fills a large share of the ceiling, packs
the rest longest-processing-time first on a high quantile of each file's
history, and prints predicted against actual seconds per shard.  With no
history anywhere, sharding stays round-robin.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "pbtest_balance_subject", ROOT / "tools" / "fleet" / "pbtest.py")
pbtest = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pbtest)  # type: ignore[union-attr]
outcomes = pbtest.pbtest_outcomes
REAL_POPEN = subprocess.Popen

SLOW_TESTS = '''\
import time


def test_slow():
    time.sleep(0.5)


def test_quick():
    pass
'''

FAST_TESTS = '''\
def test_one():
    pass


def test_two():
    pass
'''

MID_TESTS = '''\
def test_mid():
    pass
'''


def _checkout(tmp_path: Path) -> Path:
    checkout = tmp_path / "project"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "pytest.ini").write_text("[pytest]\n")
    (checkout / "tests" / "test_slow_file.py").write_text(SLOW_TESTS)
    (checkout / "tests" / "test_mid_file.py").write_text(MID_TESTS)
    (checkout / "tests" / "test_fast_file.py").write_text(FAST_TESTS)
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    return checkout


def _dispatch(checkout: Path, monkeypatch, extra: list[str]):
    def admitted_payload(command, **kwargs):
        if str(pbtest.PBRUN) not in [str(part) for part in command]:
            return REAL_POPEN(command, **kwargs)
        return REAL_POPEN(command[command.index("--") + 1:], cwd=checkout, **kwargs)

    monkeypatch.setattr(pbtest.subprocess, "Popen", admitted_payload)
    report = checkout.parent / "shards.json"
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(checkout), "--python", sys.executable,
        "--threads-per-shard", "1", "--json", str(report), *extra, "tests",
    ])
    return pbtest.main(), json.loads(report.read_text())


def _prior_run(tmp_path: Path, durations: dict[str, float]) -> Path:
    """A prior run report whose outcome record carries ``durations``.

    Keys are the real checkout-relative paths a live recorder writes
    (``tests/...``), never bare file names: bare keys would only exercise
    the unmeasured fallback (#1303 review).
    """

    record = {
        "schema": outcomes.SCHEMA,
        "rootdir_relative": ".",
        "collect_only": False,
        "collected": list(durations),
        "deselected": [],
        "reports": [],
        "uncounted": [],
        "file_durations": dict(durations),
    }
    path = tmp_path / "prior.json"
    path.write_text(json.dumps([{
        "shard": 0,
        "files": list(durations),
        "returncode": 0,
        "action_key": None,
        "receipt_path": None,
        "summary": "2 passed in 1.50s",
        "ran": True,
        "skipped": [],
        "attempts": 1,
        "output": "2 passed in 1.50s\n" + outcomes.PREFIX + json.dumps(record) + "\n",
    }]))
    return path


def test_the_outcome_record_carries_per_file_wall_time(tmp_path, monkeypatch):
    """Each file's summed phase durations ride in its shard's record."""

    code, results = _dispatch(_checkout(tmp_path), monkeypatch, ["--shards", "1"])
    assert code == 0
    record = outcomes.parse(results[0]["output"])
    assert record is not None
    durations = record.get("file_durations")
    assert durations is not None, "the record names no per-file durations"
    assert set(durations) == {
        "tests/test_slow_file.py", "tests/test_mid_file.py",
        "tests/test_fast_file.py"}
    assert durations["tests/test_slow_file.py"] >= 0.4
    assert durations["tests/test_fast_file.py"] < durations["tests/test_slow_file.py"]


def test_a_slow_file_is_isolated_from_its_shard_mates():
    """A file predicted past half the ceiling gets its own shard."""

    files = ["tests/test_slow.py"] + [f"tests/test_small_{n}.py" for n in range(18)]
    durations = {"tests/test_slow.py": 1513.0}
    durations.update({name: 10.0 for name in files[1:]})
    buckets = pbtest.shard(files, 4, durations=durations, ceiling=3600.0)
    alone = [bucket for bucket in buckets if "tests/test_slow.py" in bucket]
    assert len(alone) == 1
    assert alone[0] == ["tests/test_slow.py"]


def test_isolation_never_exceeds_the_requested_shard_count():
    """--shards is a ceiling: three past-half files with count=2 pack 2."""

    files = ["tests/test_huge_a.py", "tests/test_huge_b.py",
             "tests/test_huge_c.py", "tests/test_small.py"]
    durations = {"tests/test_huge_a.py": 2000.0, "tests/test_huge_b.py": 1900.0,
                 "tests/test_huge_c.py": 1850.0, "tests/test_small.py": 10.0}
    buckets = pbtest.shard(files, 2, durations=durations, ceiling=3600.0)
    assert len(buckets) == 2, buckets
    assert buckets[0] == ["tests/test_huge_a.py"]
    assert buckets[1] == ["tests/test_huge_b.py", "tests/test_huge_c.py",
                          "tests/test_small.py"]


def test_shards_pack_by_longest_processing_time_first():
    """The rest fill the least-loaded bucket, largest prediction first."""

    files = ["tests/test_a.py", "tests/test_b.py", "tests/test_c.py", "tests/test_d.py"]
    durations = {"tests/test_a.py": 100.0, "tests/test_b.py": 90.0,
                 "tests/test_c.py": 80.0, "tests/test_d.py": 10.0}
    assert pbtest.shard(files, 2, durations=durations, ceiling=None) == [
        ["tests/test_a.py", "tests/test_d.py"],
        ["tests/test_b.py", "tests/test_c.py"]]


def test_files_without_history_take_the_measured_default():
    """No magic constant: the default is the median of the measured files."""

    files = ["tests/test_a.py", "tests/test_b.py", "tests/test_new.py"]
    history = {"tests/test_a.py": [100.0], "tests/test_b.py": [10.0]}
    predicted = pbtest.predict_durations(history)
    assert predicted == {"tests/test_a.py": 100.0, "tests/test_b.py": 10.0}
    buckets = pbtest.shard(files, 2, durations=predicted, ceiling=None)
    assert buckets == [["tests/test_a.py"], ["tests/test_new.py", "tests/test_b.py"]]


def test_no_history_keeps_round_robin():
    """Without measurements sharding is exactly today's rotation."""

    files = [f"tests/test_{n}.py" for n in range(6)]
    assert pbtest.shard(files, 4) == [
        ["tests/test_0.py", "tests/test_4.py"],
        ["tests/test_1.py", "tests/test_5.py"],
        ["tests/test_2.py"],
        ["tests/test_3.py"]]
    assert pbtest.shard(files, 4, durations={}, ceiling=None) == pbtest.shard(files, 4)


def test_the_summary_reports_predicted_against_actual(tmp_path, monkeypatch, capsys):
    """Every shard prints its model's prediction beside pytest's own seconds."""

    checkout = _checkout(tmp_path)
    prior = _prior_run(tmp_path, {"tests/test_slow_file.py": 100.0,
                                  "tests/test_mid_file.py": 10.0,
                                  "tests/test_fast_file.py": 1.0})
    code, results = _dispatch(checkout, monkeypatch,
                              ["--shards", "2", "--history", str(prior)])
    printed = capsys.readouterr().out
    assert code == 0, printed
    assert "predicted" in printed and "pytest" in printed
    for result in results:
        assert result["predicted_s"] > 0
        assert result["pytest_s"] is not None and result["pytest_s"] >= 0
    # The split main() sealed is the longest-first packing, not the
    # rotation: slow alone, mid beside fast.
    buckets = [result["files"] for result in results]
    assert buckets == [["tests/test_slow_file.py"],
                        ["tests/test_mid_file.py", "tests/test_fast_file.py"]]
    files = [name for bucket in buckets for name in bucket]
    assert sorted(buckets, key=sorted) != sorted(
        pbtest._round_robin(sorted(files), 2), key=sorted)


def test_main_packs_shards_from_history_not_round_robin(tmp_path, monkeypatch):
    """main()'s history branch reaches the shards (#1303 review).

    The sealed split is the model's packing, not the rotation: reverting
    the ``shard(..., durations=predicted, ceiling=sealed_s,
    model_default=model_default)`` call in ``main()`` to the plain
    ``shard(files, args.shards)`` fails this test, and nothing else pins
    that call.
    """

    checkout = tmp_path / "project"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "pytest.ini").write_text("[pytest]\n")
    for name in ("test_a.py", "test_b.py", "test_c.py", "test_d.py"):
        (checkout / "tests" / name).write_text("def test_x():\n    pass\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    prior = _prior_run(tmp_path, {"tests/test_a.py": 100.0,
                                  "tests/test_b.py": 90.0,
                                  "tests/test_c.py": 80.0,
                                  "tests/test_d.py": 10.0})
    code, results = _dispatch(checkout, monkeypatch,
                              ["--shards", "2", "--history", str(prior)])
    assert code == 0
    buckets = [result["files"] for result in results]
    assert buckets == [["tests/test_a.py", "tests/test_d.py"],
                        ["tests/test_b.py", "tests/test_c.py"]]
    files = [name for bucket in buckets for name in bucket]
    assert sorted(buckets, key=sorted) != sorted(
        pbtest._round_robin(sorted(files), 2), key=sorted)


def test_main_isolates_a_file_past_half_the_sealed_deadline(tmp_path, monkeypatch):
    """The sealed ceiling reaches the packer through main().

    A file predicted past half of ``--timeout-s`` shards alone: the
    isolation is main()'s ceiling, not only shard()'s unit argument.
    """

    checkout = tmp_path / "project"
    (checkout / "tests").mkdir(parents=True)
    (checkout / "pytest.ini").write_text("[pytest]\n")
    for name in ("test_a.py", "test_b.py", "test_c.py", "test_d.py"):
        (checkout / "tests" / name).write_text("def test_x():\n    pass\n")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    prior = _prior_run(tmp_path, {"tests/test_a.py": 400.0,
                                  "tests/test_b.py": 10.0,
                                  "tests/test_c.py": 10.0,
                                  "tests/test_d.py": 10.0})
    code, results = _dispatch(checkout, monkeypatch,
                              ["--shards", "2", "--timeout-s", "600",
                               "--history", str(prior)])
    assert code == 0
    buckets = [result["files"] for result in results]
    assert buckets == [["tests/test_a.py"],
                        ["tests/test_b.py", "tests/test_c.py",
                         "tests/test_d.py"]]


def test_bad_history_is_ignored_with_a_stated_reason(tmp_path):
    """A rotted report never fails the submission, and never packs silently."""

    good = _prior_run(tmp_path, {"tests/test_a.py": 4.0})
    rotted = tmp_path / "rotted.json"
    rotted.write_text("{not json")
    foreign = tmp_path / "foreign.json"
    foreign.write_text(json.dumps({"not": "a run list"}))
    mixed = tmp_path / "mixed.json"
    mixed.write_text(json.dumps([{
        "shard": 0, "files": ["tests/test_c.py"], "returncode": 0,
        "summary": "1 passed in 2.00s", "ran": True, "output":
            "1 passed in 2.00s\n" + outcomes.PREFIX + json.dumps({
                "schema": outcomes.SCHEMA, "rootdir_relative": ".",
                "collect_only": False,
                "collected": ["tests/test_b.py::t", "tests/test_c.py::t"],
                "deselected": [], "reports": [], "uncounted": [],
                "file_durations": {"tests/test_b.py": "slow",
                                     "tests/test_c.py": 2.0}}) + "\n",
    }]))
    history, problems = pbtest.load_history(
        [str(good), str(rotted), str(foreign), str(mixed),
         str(tmp_path / "missing.json")])
    assert history == {"tests/test_a.py": [4.0],
                       "tests/test_c.py": [2.0]}, history
    assert len(problems) == 4, problems
    assert any("rotted.json" in problem for problem in problems)
    assert any("foreign.json" in problem for problem in problems)
    assert any("mixed.json" in problem and "1 entr" in problem
               for problem in problems)
    assert any("missing.json" in problem for problem in problems)


def test_history_flag_takes_one_report_per_flag(tmp_path, monkeypatch, capsys):
    """--history is repeatable single-value: it cannot swallow test paths."""

    checkout = _checkout(tmp_path)
    first = _prior_run(tmp_path, {"tests/test_slow_file.py": 0.5,
                                  "tests/test_fast_file.py": 0.01})
    one = tmp_path / "one.json"
    first.rename(one)
    two = _prior_run(tmp_path, {"tests/test_slow_file.py": 0.6,
                                "tests/test_fast_file.py": 0.02})
    code, _ = _dispatch(checkout, monkeypatch,
                        ["--shards", "2", "--history", str(one),
                         "--history", str(two)])
    printed = capsys.readouterr().out
    assert code == 0, printed
    assert "2 history file(s)" in printed, printed


def test_the_printed_default_is_the_packing_default():
    """main() and shard() share one default: the median of the measured."""

    assert pbtest.default_duration(
        {"tests/test_a.py": 100.0, "tests/test_b.py": 10.0}) == 55.0
    assert pbtest.default_duration({}) is None


def test_shard_uses_mains_default_when_history_holds_outside_files():
    """The packing default is main()'s model default, not the run's (#1303).

    History holds files outside this run, so the median over ALL history
    (what main() prints and reports) differs from the median of this
    run's files alone.  shard() must pack the unseen file on main()'s
    default; without a passed model_default it falls back to the run's.
    """

    files = ["tests/test_a.py", "tests/test_b.py", "tests/test_new.py"]
    predicted = {"tests/test_a.py": 10.0, "tests/test_b.py": 20.0,
                 "tests/test_gone_x.py": 1000.0,
                 "tests/test_gone_y.py": 1000.0,
                 "tests/test_gone_z.py": 1000.0}
    model_default = pbtest.default_duration(predicted)
    assert model_default == 1000.0
    assert pbtest.default_duration(
        {name: predicted[name] for name in files if name in predicted}) == 15.0
    packed = pbtest.shard(files, 2, durations=dict(predicted), ceiling=300.0,
                          model_default=model_default)
    assert packed == [["tests/test_new.py"],
                      ["tests/test_b.py", "tests/test_a.py"]]
    fallback = pbtest.shard(files, 2, durations=dict(predicted), ceiling=300.0)
    assert fallback == [["tests/test_b.py"],
                        ["tests/test_new.py", "tests/test_a.py"]]


def test_shard_packing_properties():
    """No drop or duplicate, the count is a ceiling, and history is stable."""

    import random
    rng = random.Random(1246)
    for _ in range(300):
        n = rng.randint(0, 12)
        files = [f"tests/test_{i:02d}.py" for i in range(n)]
        count = rng.randint(1, 5)
        durations = (
            {name: rng.choice([0.0, 0.5, 5.0, 60.0, 600.0, 2000.0])
             for name in rng.sample(files, rng.randint(0, n))} if n else {})
        ceiling = rng.choice([None, 60.0, 600.0, 3600.0])
        first = pbtest.shard(files, count, durations=dict(durations),
                             ceiling=ceiling)
        flat = [name for bucket in first for name in bucket]
        assert sorted(flat) == sorted(files)
        assert len(first) <= (max(1, min(count, n)) if n else 1)
        assert pbtest.shard(files, count, durations=dict(durations),
                            ceiling=ceiling) == first
        if not durations:
            assert first == pbtest.shard(files, count)
