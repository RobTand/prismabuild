"""An empty capability dependency union dispatches (#1495).

A capability may fence by placement tags alone; its cohort then seals a
selection whose dependency union holds no exact-file requirement.  pbrun's
standalone ``--requires-files`` refuses an empty list by contract, so a shard
that forwarded one would die at the argument on every box: pbtest instead
omits the flag entirely for such a cohort, and the row it publishes is an
ordinary tagged row -- no digest capability is demanded of the fleet, and no
digest question is asked of it.  The tags, the sealed selection and the
reports are unchanged; an explicit ``--requires-files`` on a standalone
submission still names at least one entry.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest

from test_pbtest_seals_its_shard_deadline import (  # noqa: E402
    _FinishedProcess, pbtest,
)
import pbrun  # noqa: E402

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
from prismabuild import dependency_digest, pool  # noqa: E402

TAG = dependency_digest.DEPENDENCY_DIGEST_TAG
CAPACITY = {"cpu": 8, "mem_gb": 16}
SCHEMA = "prismabuild.pbtest_capabilities.v1"
MARKED = (
    "import pytest\n\n"
    "@pytest.mark.pbtest_capability(\"x86_only\")\n"
    "def test_one():\n    assert True\n"
)
PLAIN = "def test_one():\n    assert True\n"


def _dependency(tmp_path: Path, payload: bytes = b"secondary bytes") -> dict:
    path = tmp_path / "deps" / "secondary"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return {"path": str(path),
            "sha256": hashlib.sha256(payload).hexdigest()}


def _config(checkout: Path, capabilities: dict) -> None:
    """The versioned config, inside the checkout the snapshot carries."""
    (checkout / "capabilities.json").write_text(
        json.dumps({"schema": SCHEMA, "capabilities": capabilities}),
        encoding="utf-8")


def _stub_queue(tmp_path: Path, monkeypatch, *, tags, answers=()):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    queue.announce(host="fenced-box", tags=list(tags), has_gpu=False,
                   capacity=dict(CAPACITY),
                   dependency_files=(list(answers) or None) if answers else None,
                   dependency_files_absent=None)
    monkeypatch.setattr(pbtest, "fleet_queue", lambda: queue)
    return queue


def _checkout(tmp_path: Path, *, marked: int, plain: int):
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


def _dispatch(tmp_path: Path, monkeypatch, checkout, *, shards=4):
    calls: list[list[str]] = []

    def _popen(command, **kwargs):
        calls.append(list(command))
        return _FinishedProcess(command)

    monkeypatch.setattr(pbtest.subprocess, "Popen", _popen)
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout),
         "--python", sys.executable, "--shards", str(shards),
         "--capabilities", "capabilities.json", "tests"])
    return pbtest.main(), calls


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


def _requires(command):
    flags = _flags(command)
    if "--requires-files" not in flags:
        return None
    return json.loads(flags[flags.index("--requires-files") + 1])


# --------------------------------------------------------------------------
# The empty union
# --------------------------------------------------------------------------

def test_a_tags_only_fence_dispatches_without_a_requires_files_flag(
        tmp_path, monkeypatch):
    checkout = _checkout(tmp_path, marked=1, plain=1)
    _config(checkout, {"x86_only": {"tags": ["x86"]}})
    # The offer carries the fence tag but not the digest capability: a
    # requirement-less cohort needs neither the flag nor the tag.
    _stub_queue(tmp_path, monkeypatch, tags=["x86"])
    code, calls = _dispatch(tmp_path, monkeypatch, checkout)
    assert code == 0
    dispatched = [name for command in calls
                  for name in _spec(command)["files"]]
    assert sorted(dispatched) == sorted(
        str(path.relative_to(checkout))
        for path in (checkout / "tests").glob("test_*.py"))
    assert len(dispatched) == len(set(dispatched)), "a file ran twice"
    fenced = [command for command in calls if "x86" in _tag_values(command)]
    assert len(fenced) == 1
    flags = _flags(fenced[0])
    assert "--requires-files" not in flags
    assert TAG not in _tag_values(fenced[0])
    spec = _spec(fenced[0])
    # The seal still travels: tags-only, empty union, named capabilities.
    assert spec["capabilities"]["names"] == ["x86_only"]
    assert spec["capabilities"]["tags"] == ["x86"]
    assert spec["capabilities"]["dependencies"] == []
    # The portable shard is untouched: no digest flag and no capabilities.
    portable = [command for command in calls if command is not fenced[0]]
    assert len(portable) == 1
    assert "--requires-files" not in _flags(portable[0])
    assert "capabilities" not in _spec(portable[0])


def test_a_seal_with_an_empty_union_verifies_to_nothing():
    spec = dependency_digest.validate_sealed(
        {"names": ["x86_only"], "tags": ["x86"], "dependencies": []})
    assert spec == {"names": ["x86_only"], "tags": ["x86"],
                    "files": [], "observations": []}


# --------------------------------------------------------------------------
# The nonempty union beside it
# --------------------------------------------------------------------------

def test_a_nonempty_union_still_forwards_the_flag(tmp_path, monkeypatch):
    checkout = _checkout(tmp_path, marked=1, plain=0)
    entry = _dependency(tmp_path)
    _config(checkout, {"x86_only": {"tags": ["x86"],
                                    "dependencies": [entry]}})
    _stub_queue(tmp_path, monkeypatch, tags=[TAG, "x86"],
                answers=[entry["path"]])
    code, calls = _dispatch(tmp_path, monkeypatch, checkout)
    assert code == 0
    assert len(calls) == 1
    assert _requires(calls[0]) == [entry]
    assert "x86" in _tag_values(calls[0])
    assert _spec(calls[0])["capabilities"]["dependencies"] == [entry]


# --------------------------------------------------------------------------
# The standalone contract, unchanged
# --------------------------------------------------------------------------

def test_a_standalone_empty_requires_files_is_still_refused(capsys):
    args = pbrun.parse_args(["--requires-files", "[]", "--", "true"])
    with pytest.raises(SystemExit) as excinfo:
        pbrun.prepare_submission(args)
    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert "--requires-files" in err and "nonempty" in err
