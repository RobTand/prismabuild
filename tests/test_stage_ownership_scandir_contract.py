"""Exercise #2013's fault injector without requiring #1973's report API.

Main lacks the original acceptance module. The committed text fixture is the
verbatim acceptance module from e73cb5388fcd34ceb36fb1943274b48c15a84c5c,
blob ad54ad794a4eb737652e8d8c557fc48f9080ff0f. Replay that wrapper until
its acceptance owner restores the module; thereafter exercise the actual source.
No test-time historical Git access or report implementation is required.
"""
from __future__ import annotations

import ast
import os
from pathlib import Path
import shutil

import pytest


def _acceptance_scandir(stage, real_scandir):
    tests = Path(__file__).resolve().parent
    source = tests / "test_stage_ownership_report.py"
    if not source.exists():
        source = tests / "fixtures" / "stage_ownership_unreadable_directory.py.txt"
    tree = ast.parse(source.read_text(), filename=str(source))
    test = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                and node.name == "test_unreadable_directory_keeps_useful_report")
    wrapper = next(node for node in test.body if isinstance(node, ast.FunctionDef)
                   and node.name == "scandir")
    module = ast.Module(body=[wrapper], type_ignores=[])
    namespace = {"os": os, "stage": stage, "real_scandir": real_scandir}
    exec(compile(module, str(source), "exec"), namespace)
    return namespace["scandir"]


def test_ownership_fault_injector_preserves_scandir_descriptor_contract(tmp_path, monkeypatch):
    stage = tmp_path / "stage"
    blocked = stage / "blocked"
    readable = stage / "readable"
    blocked.mkdir(parents=True)
    readable.mkdir()
    (blocked / "data").write_bytes(b"x" * 29)
    (readable / "data").write_bytes(b"x" * 7)
    native_scandir = os.scandir
    forwarded = []

    def real_scandir(path):
        forwarded.append(path)
        return native_scandir(path)

    scandir = _acceptance_scandir(stage, real_scandir)
    for path in (str(blocked), blocked):
        with pytest.raises(PermissionError, match="fixture denies stage directory"):
            scandir(path)
    assert forwarded == []
    for path in (str(readable), readable):
        with scandir(path) as entries:
            assert [entry.name for entry in entries] == ["data"]
        assert forwarded[-1] is path

    # A descriptor for even the denied pathname must be forwarded, not fspath'd.
    # Closing the iterator must not close the caller's descriptor.
    for directory in (readable, blocked):
        fd = os.open(directory, os.O_RDONLY)
        try:
            with scandir(fd) as entries:
                assert [entry.name for entry in entries] == ["data"]
            assert forwarded[-1] is fd
            assert os.fstat(fd) == directory.stat()
        finally:
            os.close(fd)

    # Explicit call-phase cleanup, not a dependence on pytest fixture ordering.
    # Undo the global replacement before pytest tears down this test's tmp_path.
    cleanup = tmp_path / "cleanup"
    (cleanup / "nested").mkdir(parents=True)
    (cleanup / "nested" / "data").write_bytes(b"cleanup")
    with monkeypatch.context() as patch:
        patch.setattr(os, "scandir", scandir)
        shutil.rmtree(cleanup)
    assert not cleanup.exists()
