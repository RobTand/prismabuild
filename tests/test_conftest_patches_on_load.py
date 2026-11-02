"""Conftest patches a module when the test loads it, rather than loading it for every test (#2582).

The autouse fixtures used to import ``pbrun``, ``d38_gate``,
``filesystem_capacity`` and the submission frontends so that they could
patch them, which made every test depend on most of the fleet. They now
register each patch with ``_when_loaded``: a module already loaded is patched
at once, and one the test loads later is patched by the live-store loader
hook. Each registration ends with its test.
"""
from __future__ import annotations

import sys

import pytest

import conftest
from prismabuild import fleet_roster, runtime_paths

PROBE = "pb_on_load_probe_2582"


@pytest.fixture
def probe_module(tmp_path, monkeypatch):
    (tmp_path / f"{PROBE}.py").write_text("VALUE = 'module'\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, PROBE, raising=False)
    yield
    sys.modules.pop(PROBE, None)


def test_a_module_the_test_loads_later_is_patched_and_released(probe_module):
    with pytest.MonkeyPatch.context() as patching:
        conftest._when_loaded(patching, PROBE, lambda module: patching.setattr(module, "VALUE", "patched"))
        assert PROBE not in sys.modules
        probe = __import__(PROBE)
        assert probe.VALUE == "patched"
    assert PROBE not in conftest._LOAD_PATCHES
    assert probe.VALUE == "module"


def test_a_module_already_loaded_is_patched_at_once(probe_module):
    probe = __import__(PROBE)
    with pytest.MonkeyPatch.context() as patching:
        conftest._when_loaded(patching, PROBE, lambda module: patching.setattr(module, "VALUE", "patched"))
        assert probe.VALUE == "patched"
    assert probe.VALUE == "module"


def test_the_store_built_without_pbrun_names_pbruns_runtime_and_roster():
    import pbrun

    assert runtime_paths.generation_root(conftest._FLEET_ROSTER) == pbrun.RUNTIME_ROOT
    assert fleet_roster.Roster.beside(conftest._FLEET_ROSTER) == fleet_roster.Roster.beside(pbrun.__file__)


def test_a_loaded_pbrun_reads_the_tests_explicit_store(tmp_path, d38_config):
    import pbrun

    store = pbrun.configured_store()
    assert (store.root, store.d38_config_path) == (tmp_path / "fleet", d38_config)


def test_a_freshly_loaded_filesystem_capacity_gets_the_tests_sample_os(monkeypatch):
    # The real fixture (_ample_queue_space), not a probe: a load inside the test body is patched.
    import importlib
    import os

    import prismabuild

    name = "prismabuild.filesystem_capacity"
    loaded = sys.modules.get(name)
    monkeypatch.delitem(sys.modules, name, raising=False)
    if loaded is not None:
        monkeypatch.setattr(prismabuild, "filesystem_capacity", loaded)
    fresh = importlib.import_module(name)
    assert fresh is not loaded
    assert fresh.os is not os


def test_a_store_module_loaded_without_its_frontend_is_refused_by_name(tmp_path, monkeypatch):
    probe = "pb_store_probe_2582"
    (tmp_path / f"{probe}.py").write_text("def configured_store():\n    return None\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "prismabuild.submission_frontend", raising=False)
    monkeypatch.delitem(sys.modules, probe, raising=False)
    try:
        with pytest.raises(RuntimeError, match="prismabuild.submission_frontend"):
            __import__(probe)
    finally:
        sys.modules.pop(probe, None)
