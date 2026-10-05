"""Per-file capability cohorts in pbtest's fanout (#1495).

The declaration is PB's versioned config read through the fleet-data seam;
cohorting is exact declared-name sets, so a portable file is never packed into
a fenced shard; every selected file is dispatched exactly once with its
cohort's tags and sealed requirements; an unclassifiable or undefined
declaration refuses by name; and a population with no declarations dispatches
byte-identically to before.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from pbtest_shard_output import (  # noqa: E402
    ShardProcess, admitted_child, shard_output_for,
)

#: The true Popen, captured before any test patches the shared module: two
#: dispatches in one test must not wrap each other's substitution.
REAL_POPEN = subprocess.Popen
from test_pbtest_seals_its_shard_deadline import (  # noqa: E402
    _FinishedProcess, pbtest,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from prismabuild import dependency_digest, pool  # noqa: E402

TAG = dependency_digest.DEPENDENCY_DIGEST_TAG
CAPACITY = {"cpu": 8, "mem_gb": 16}
MARKED = (
    "import pytest\n\n"
    "@pytest.mark.pbtest_capability(\"x86_producer\")\n"
    "def test_one():\n    assert True\n"
)
PLAIN = "def test_one():\n    assert True\n"


def _dependency(tmp_path: Path, payload: bytes = b"secondary bytes") -> dict:
    path = tmp_path / "deps" / "secondary"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return {"path": str(path),
            "sha256": hashlib.sha256(payload).hexdigest()}


def _config(checkout: Path, tmp_path: Path, *, sha: str | None = None,
            tags=("x86",)) -> Path:
    """The versioned config, inside the checkout the snapshot carries."""
    entry = _dependency(tmp_path)
    if sha is not None:
        entry["sha256"] = sha
    config = checkout / "capabilities.json"
    config.write_text(json.dumps({
        "schema": "prismabuild.pbtest_capabilities.v1",
        "capabilities": {"x86_producer": {"tags": list(tags),
                                          "dependencies": [entry]}},
    }), encoding="utf-8")
    return config


def _stub_queue(tmp_path: Path, monkeypatch, *, answers=(), absent=(),
                capability=True):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    tags = [TAG, "x86"] if capability else ["cpu"]
    queue.announce(host="fenced-box", tags=tags, has_gpu=False,
                   capacity=dict(CAPACITY),
                   dependency_files=(list(answers) or None) if capability else None,
                   dependency_files_absent=list(absent) or None)
    monkeypatch.setattr(pbtest, "fleet_queue", lambda: queue)
    return queue


def _checkout(tmp_path: Path, *, marked=2, plain=2):
    checkout = tmp_path / "checkout"
    (checkout / "tests").mkdir(parents=True)
    for index in range(plain):
        (checkout / "tests" / f"test_plain_{index}.py").write_text(
            PLAIN, encoding="utf-8")
    for index in range(marked):
        (checkout / "tests" / f"test_marked_{index}.py").write_text(
            MARKED, encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    return checkout


def _dispatch(tmp_path: Path, monkeypatch, checkout, extra, *, shards=4,
              real=False):
    calls: list[list[str]] = []

    original = REAL_POPEN

    def _popen(command, **kwargs):
        calls.append(list(command))
        if real:
            # The child, not pbrun: the sealed program runs inside this
            # already-admitted test action (the dependency-pins pattern).
            argv, environment = admitted_child(command)
            return original(argv, cwd=checkout, env=environment, **kwargs)
        return _FinishedProcess(command)

    monkeypatch.setattr(pbtest.subprocess, "Popen", _popen)
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout),
         "--python", sys.executable, "--shards", str(shards),
         "--capabilities", "capabilities.json", *extra, "tests"])
    code = pbtest.main()
    return code, calls


def _flags(command) -> list[str]:
    return list(command[:command.index("--")])


def _arguments(command) -> list[str]:
    return list(command[command.index("--") + 1:])


def _spec(command) -> dict:
    arguments = _arguments(command)
    return json.loads(arguments[arguments.index("-c") + 2])


def _tag_values(command) -> list[str]:
    flags = _flags(command)
    return [flags[index + 1] for index, flag in enumerate(flags)
            if flag == "--tag"]


def _run_child(command, checkout: Path):
    """The sealed child, run by the true Popen: ``subprocess.run`` would
    re-enter the patched ``Popen`` with an already-child argv."""
    argv, environment = admitted_child(command)
    done = REAL_POPEN(argv, cwd=checkout,
                      env={**os.environ, **environment},
                      stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                      text=True)
    done.stdout, done.stderr = done.communicate()
    return done


def _requires(command):
    flags = _flags(command)
    if "--requires-files" not in flags:
        return None
    return json.loads(flags[flags.index("--requires-files") + 1])


def test_a_declared_file_without_a_config_is_refused_by_name(
        tmp_path, monkeypatch, capsys):
    checkout = _checkout(tmp_path)
    _stub_queue(tmp_path, monkeypatch)
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", sys.executable,
         "--shards", "4", "tests"])
    assert pbtest.main() == 2
    err = capsys.readouterr().err
    assert "test_marked_0.py" in err
    assert "does not define" in err


def test_a_marker_naming_an_undefined_capability_is_refused(
        tmp_path, monkeypatch, capsys):
    checkout = _checkout(tmp_path)
    _stub_queue(tmp_path, monkeypatch)
    (checkout / "capabilities.json").write_text(json.dumps({
        "schema": "prismabuild.pbtest_capabilities.v1",
        "capabilities": {"other_name": {"tags": ["x86"],
                                        "dependencies": [_dependency(tmp_path)]}},
    }), encoding="utf-8")
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", sys.executable,
         "--shards", "4", "tests"])
    assert pbtest.main() == 2
    err = capsys.readouterr().err
    assert "x86_producer" in err and "test_marked_0.py" in err


def test_a_nonconstant_marker_argument_is_refused(tmp_path, monkeypatch, capsys):
    checkout = _checkout(tmp_path, marked=1, plain=1)
    _stub_queue(tmp_path, monkeypatch)
    _config(checkout, tmp_path)
    (checkout / "tests" / "test_marked_0.py").write_text(
        "import pytest\nNAME = 'x86_producer'\n"
        "@pytest.mark.pbtest_capability(NAME)\ndef test_one():\n"
        "    assert True\n", encoding="utf-8")
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", sys.executable,
         "--shards", "4", "tests"])
    assert pbtest.main() == 2
    assert "cannot be classified statically" in capsys.readouterr().err


def test_an_invalid_config_refuses_by_name(tmp_path, monkeypatch, capsys):
    checkout = _checkout(tmp_path, marked=0, plain=1)
    _stub_queue(tmp_path, monkeypatch)
    (checkout / "tests" / "pbtest_capabilities.json").write_text(json.dumps({
        "schema": "prismabuild.pbtest_capabilities.v0", "capabilities": {}}),
        encoding="utf-8")
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", sys.executable,
         "--shards", "4", "tests"])
    assert pbtest.main() == 2
    assert "schema must be exactly" in capsys.readouterr().err


def test_capabilities_outside_the_checkout_are_refused(
        tmp_path, monkeypatch, capsys):
    checkout = _checkout(tmp_path, marked=0, plain=1)
    _stub_queue(tmp_path, monkeypatch)
    outside = tmp_path / "outside.json"
    outside.write_text(json.dumps({
        "schema": "prismabuild.pbtest_capabilities.v1", "capabilities": {}}),
        encoding="utf-8")
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", sys.executable,
         "--shards", "4", "--capabilities", str(outside), "tests"])
    assert pbtest.main() == 2
    assert "inside --checkout" in capsys.readouterr().err


def test_a_missing_explicit_config_is_refused(tmp_path, monkeypatch, capsys):
    checkout = _checkout(tmp_path, marked=0, plain=1)
    _stub_queue(tmp_path, monkeypatch)
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", sys.executable,
         "--shards", "4", "--capabilities", "nope.json", "tests"])
    assert pbtest.main() == 2
    assert "is not a file" in capsys.readouterr().err


def test_more_cohorts_than_shards_is_refused_before_any_submission(
        tmp_path, monkeypatch, capsys):
    checkout = _checkout(tmp_path, marked=1, plain=1)
    (checkout / "tests" / "test_third.py").write_text(
        MARKED.replace("x86_producer", "other_producer"), encoding="utf-8")
    _stub_queue(tmp_path, monkeypatch, capability=False)
    (checkout / "capabilities.json").write_text(json.dumps({
        "schema": "prismabuild.pbtest_capabilities.v1",
        "capabilities": {
            "x86_producer": {"tags": ["x86"],
                             "dependencies": [_dependency(tmp_path)]},
            "other_producer": {"tags": ["arm"]}},
    }), encoding="utf-8")
    code, calls = _dispatch(tmp_path, monkeypatch, checkout, [], shards=2)
    assert code == 2
    assert calls == []
    err = capsys.readouterr().err
    assert "need at least 3 shards" in err and "--shards is 2" in err
    assert "other_producer" in err


def test_a_mixed_population_never_mixes_cohorts_and_keeps_every_file(
        tmp_path, monkeypatch):
    checkout = _checkout(tmp_path)
    _config(checkout, tmp_path)
    _stub_queue(tmp_path, monkeypatch, answers=[_dependency(tmp_path)["path"]])
    code, calls = _dispatch(tmp_path, monkeypatch, checkout, [])
    assert code == 0
    dispatched = [name for command in calls
                  for name in _spec(command)["files"]]
    assert sorted(dispatched) == sorted(
        str(path.relative_to(checkout))
        for path in (checkout / "tests").glob("test_*.py"))
    assert len(dispatched) == len(set(dispatched)), "a file ran twice"
    for command in calls:
        spec = _spec(command)
        fenced = any("marked" in name for name in spec["files"])
        if fenced:
            assert "x86" in _tag_values(command)
            assert _requires(command) == [_dependency(tmp_path)]
            assert spec["capabilities"]["names"] == ["x86_producer"]
            assert spec["capabilities"]["tags"] == ["x86"]
            assert spec["capabilities"]["dependencies"] == [
                _dependency(tmp_path)]
        else:
            assert "x86" not in _tag_values(command)
            assert _requires(command) is None
            assert set(spec) == {"files", "roots"}


def test_a_fenced_dispatch_is_deterministic_and_identity_moves_with_declarations(
        tmp_path, monkeypatch):
    checkout = _checkout(tmp_path, marked=1, plain=1)
    _config(checkout, tmp_path)
    _stub_queue(tmp_path, monkeypatch, answers=[_dependency(tmp_path)["path"]])
    code, first = _dispatch(tmp_path, monkeypatch, checkout, [])
    assert code == 0
    code, second = _dispatch(tmp_path, monkeypatch, checkout, [])
    assert code == 0
    fenced = [command for command in first if _requires(command) is not None]
    assert fenced
    for before, after in zip(first, second):
        assert _spec(before) == _spec(after), "same inputs must re-key identically"
    # A changed digest is a changed declaration: the sealed identity moves.
    _config(checkout, tmp_path, sha="c" * 64)
    code, changed = _dispatch(tmp_path, monkeypatch, checkout, [])
    assert code == 0
    changed_fenced = [command for command in changed
                      if _requires(command) is not None]
    assert _spec(fenced[0])["capabilities"]["dependencies"][0]["sha256"] == \
        _dependency(tmp_path)["sha256"]
    assert _spec(changed_fenced[0])["capabilities"]["dependencies"][0][
        "sha256"] == "c" * 64
    assert _spec(fenced[0]) != _spec(changed_fenced[0])


def test_an_unused_config_leaves_the_dispatch_byte_identical(
        tmp_path, monkeypatch):
    plain_checkout = _checkout(tmp_path, marked=0, plain=2)
    _stub_queue(tmp_path, monkeypatch)
    calls: list[list[str]] = []

    def _popen(command, **kwargs):
        calls.append(list(command))
        return _FinishedProcess(command)

    monkeypatch.setattr(pbtest.subprocess, "Popen", _popen)
    # A run with no config file at all.
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(plain_checkout),
         "--python", sys.executable, "--shards", "2", "tests"])
    assert pbtest.main() == 0
    bare = [list(command) for command in calls]
    # The same population, with a config that defines an unused capability.
    config = plain_checkout / "tests" / "pbtest_capabilities.json"
    config.write_text(json.dumps({
        "schema": "prismabuild.pbtest_capabilities.v1",
        "capabilities": {"unused": {"tags": ["x86"],
                                    "dependencies": [_dependency(tmp_path)]}},
    }), encoding="utf-8")
    calls.clear()
    assert pbtest.main() == 0
    assert [_spec(command) for command in bare] == [
        _spec(command) for command in calls]
    assert [_flags(command) for command in bare] == [
        _flags(command) for command in calls]


def test_an_unanswered_path_publishes_with_a_notice(
        tmp_path, monkeypatch, capsys):
    checkout = _checkout(tmp_path)
    _config(checkout, tmp_path)
    _stub_queue(tmp_path, monkeypatch, answers=[])
    code, calls = _dispatch(tmp_path, monkeypatch, checkout, [])
    out = capsys.readouterr()
    assert code == 0
    assert calls, "an unanswered first submission still publishes"
    assert "no worker has answered" in out.out + out.err


def test_no_capable_worker_refuses_the_fenced_submission(
        tmp_path, monkeypatch, capsys):
    checkout = _checkout(tmp_path)
    _config(checkout, tmp_path)
    _stub_queue(tmp_path, monkeypatch, capability=False)
    code, calls = _dispatch(tmp_path, monkeypatch, checkout, [])
    assert code == 2
    assert calls == []
    err = capsys.readouterr().err
    assert TAG in err and "dependency-digest" in err


def test_unanimous_absent_answers_refuse_the_fenced_submission(
        tmp_path, monkeypatch, capsys):
    checkout = _checkout(tmp_path)
    entry = _dependency(tmp_path)
    _config(checkout, tmp_path)
    _stub_queue(tmp_path, monkeypatch, answers=[],
                absent=[entry["path"]])
    code, calls = _dispatch(tmp_path, monkeypatch, checkout, [])
    assert code == 2
    assert calls == []
    assert entry["path"] in capsys.readouterr().err


def test_a_fenced_shard_runs_its_real_preflight_before_pytest(
        tmp_path, monkeypatch, capsys):
    """The sealed verifier refuses before pytest, inside the real child."""
    checkout = _checkout(tmp_path, marked=1, plain=1)
    _config(checkout, tmp_path)
    _stub_queue(tmp_path, monkeypatch, answers=[_dependency(tmp_path)["path"]])
    code, calls = _dispatch(tmp_path, monkeypatch, checkout, [], shards=2,
                            real=True)
    assert code == 0
    fenced = [command for command in calls if _requires(command) is not None]
    assert len(fenced) == 1
    done = _run_child(fenced[0], checkout)
    assert done.returncode == 0, done.stderr
    assert dependency_digest.EVIDENCE_PREFIX in done.stdout
    assert "1 passed" in done.stdout
    # The same fence, drifted: the shard refuses before pytest.
    _config(checkout, tmp_path, sha="c" * 64)
    code, calls = _dispatch(tmp_path, monkeypatch, checkout, [], shards=2,
                            real=True)
    fenced = [command for command in calls if _requires(command) is not None]
    done = _run_child(fenced[0], checkout)
    assert done.returncode != 0
    assert "capability refusal" in done.stderr
    assert "passed" not in done.stdout


def test_allocation_is_deterministic_and_gives_every_cohort_one_shard():
    weights = {(): 10.0, ("a",): 2.0, ("b",): 2.0}
    # 3 extras split by weight (2.14, 0.43, 0.43); the tie between the two
    # equal cohorts breaks by key sort, so ("a",) takes the last shard.
    assert pbtest.allocate_shards(weights, 6) == {
        (): 3, ("a",): 2, ("b",): 1}
    assert pbtest.allocate_shards(weights, 6) == pbtest.allocate_shards(
        dict(reversed(list(weights.items()))), 6)
    assert pbtest.allocate_shards({}, 3) == {}
