"""Regression for #2017's descriptor-unsafe #2013 acceptance fault injector.

Main does not yet contain test_stage_ownership_report.py. Its original source
(blob ad54ad794a4eb737652e8d8c557fc48f9080ff0f) is preserved as an editable
text fixture, reused from #2013's research commit df70c5f88565. Exercise only
its scandir wrapper, not the absent ownership-report production API. Prefer the
actual acceptance module whenever that work is integrated. No historical Git
objects or production report implementation are needed at test runtime.
"""
from __future__ import annotations

import ast
import errno
import os
from pathlib import Path
import tempfile

import pytest


def _acceptance_scandir(stage, real_scandir):
    tests = Path(__file__).resolve().parent
    source = tests / "test_stage_ownership_report.py"
    if not source.exists():
        source = tests / "fixtures" / "stage_ownership_unreadable_directory.py.txt"
    tree = ast.parse(source.read_text(), filename=str(source))
    test = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                and node.name == "test_unreadable_directory_keeps_useful_report")
    wrapper = next(node for node in ast.walk(test) if isinstance(node, ast.FunctionDef)
                   and node.name == "scandir")
    module = ast.Module(body=[wrapper], type_ignores=[])
    namespace = {"os": os, "errno": errno, "stage": stage,
                 "blocked": stage / "blocked", "real_scandir": real_scandir}
    exec(compile(module, str(source), "exec"), namespace)
    return namespace["scandir"]


@pytest.mark.parametrize("operation", ["fd-readable", "fd-blocked", "temporary-directory"])
def test_ownership_fault_injector_preserves_scandir_contract(tmp_path, monkeypatch, operation):
    stage = tmp_path / "stage"
    blocked = stage / "blocked"
    readable = stage / "readable"
    blocked.mkdir(parents=True)
    readable.mkdir()
    (blocked / "data").write_bytes(b"x" * 29)
    (readable / "data").write_bytes(b"x" * 7)
    native_scandir = os.scandir
    forwarded = []

    def real_scandir(path, *args, **kwargs):
        forwarded.append(path)
        return native_scandir(path, *args, **kwargs)

    scandir = _acceptance_scandir(stage, real_scandir)
    # Descriptor compatibility must not weaken the original fault injection.
    for path in (str(blocked), blocked):
        with pytest.raises(PermissionError):
            scandir(path)
    assert forwarded == []
    for path in (str(readable), readable):
        with scandir(path) as entries:
            assert [entry.name for entry in entries] == ["data"]
        assert forwarded[-1] is path

    if operation.startswith("fd-"):
        directory = readable if operation == "fd-readable" else blocked
        fd = os.open(directory, os.O_RDONLY)
        try:
            with scandir(fd) as entries:
                assert [entry.name for entry in entries] == ["data"]
            assert forwarded[-1] is fd
            # scandir's iterator must not take ownership of the supplied fd.
            assert os.fstat(fd) == directory.stat()
        finally:
            os.close(fd)
    else:
        # Exit the temporary directory while the global patch is still active,
        # reproducing the reported post-body cleanup, independent of pytest's
        # fixture teardown order. Always restore os.scandir before our teardown.
        with monkeypatch.context() as patch:
            patch.setattr(os, "scandir", scandir)
            with tempfile.TemporaryDirectory(dir=tmp_path) as name:
                cleanup = Path(name)
                (cleanup / "nested").mkdir()
                (cleanup / "nested" / "data").write_bytes(b"cleanup")
            assert not cleanup.exists()
        assert any(isinstance(path, int) for path in forwarded)
