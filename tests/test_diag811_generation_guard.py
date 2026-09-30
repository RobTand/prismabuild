"""CPU source-confinement witnesses; no runtime generation is executed."""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

from test_diag811_experiment_boundaries import harness


def _fake_generation(monkeypatch, tmp_path, *, sibling):
    root = tmp_path / "generation"
    source = root / "src" / "prismabuild"
    source.mkdir(parents=True)
    (source / "materialize.py").touch()
    outside = tmp_path / "generation-other" / "src" / "prismabuild"
    outside.mkdir(parents=True)
    package = types.ModuleType("prismabuild")
    package.__path__ = [str(source)]
    monkeypatch.setitem(sys.modules, "prismabuild", package)
    for name in ("core", "materialize", "reader_lease"):
        module = types.ModuleType(f"prismabuild.{name}")
        module.__file__ = str((outside if sibling else source) / f"{name}.py")
        Path(module.__file__).touch()
        monkeypatch.setitem(sys.modules, module.__name__, module)
        setattr(package, name, module)
    monkeypatch.setenv("PRISMABUILD_READER_HELPER_ROOT", str(root))
    # load_generation prepends a path. Keep that change test-local too.
    monkeypatch.setattr(sys, "path", list(sys.path))
    return root


def test_generation_accepts_modules_contained_in_its_root(harness, monkeypatch, tmp_path):
    root = _fake_generation(monkeypatch, tmp_path, sibling=False)
    assert harness.load_generation()[0] == root


def test_generation_refuses_a_sibling_with_the_same_string_prefix(harness, monkeypatch, tmp_path):
    _fake_generation(monkeypatch, tmp_path, sibling=True)
    with pytest.raises(harness.E1Error, match="outside the generation"):
        harness.load_generation()
