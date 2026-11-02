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

    entrypoint = str(conftest._FLEET_TOOLS / "pbrun.py")
    assert runtime_paths.generation_root(entrypoint) == pbrun.RUNTIME_ROOT
    assert fleet_roster.Roster.beside(entrypoint) == fleet_roster.Roster.beside(pbrun.__file__)


def test_a_loaded_pbrun_reads_the_tests_explicit_store(tmp_path, d38_config):
    import pbrun

    store = pbrun.configured_store()
    assert (store.root, store.d38_config_path) == (tmp_path / "fleet", d38_config)
