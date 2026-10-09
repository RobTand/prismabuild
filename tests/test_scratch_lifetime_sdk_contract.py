"""#1360: the public SDK exposes the versioned scratch-lifetime contract.

PR #1463 wires generation-bound scratch lifetime through the pool owner,
but `prismabuild.client` still exports no lifetime names and advertises no
`scratch-lifetime-v1` capability. A producer cannot declare or read the
contract through the public SDK.
"""
from __future__ import annotations

import inspect
import json

import pytest
from prismabuild import client, local_scratch


def test_sdk_advertises_versioned_scratch_lifetime():
    assert client.SDK_VERSION == 6
    assert client.SCRATCH_LIFETIME_TAG == "scratch-lifetime-v1"
    assert client.SCRATCH_LIFETIME_TAG in client.CAPABILITIES
    assert (client.SCRATCH_LIFETIME_SELECTION_SCHEMA_V1
            == "prismabuild.scratch_lifetime_selection.v1")
    assert (client.SCRATCH_LIFETIME_RECORD_SCHEMA_V1
            == "prismabuild.scratch_lifetime_record.v1")
    assert client.SCRATCH_LIFETIME_FIELD == "scratch_lifetime_record"
    assert client.SCRATCH_LIFETIME_DECLARATIONS_ENV == (
        local_scratch.DECLARATIONS_ENV)
    assert callable(client.build_scratch_lifetime_selection)
    parameters = inspect.signature(
        client.build_scratch_lifetime_selection).parameters
    assert list(parameters) == ["entries"]


def test_sdk_selection_builder_matches_pool_intent(tmp_path):
    variables = {
        local_scratch.PAIRS_ENV: "TEMP_ROOT:TEMP_MAX,CACHE_ROOT:CACHE_MAX",
        "TEMP_ROOT": str(tmp_path / "temporary"), "TEMP_MAX": "1024",
        "CACHE_ROOT": str(tmp_path / "persistent"), "CACHE_MAX": "2048",
    }
    selection = client.build_scratch_lifetime_selection([
        {"root_env": "TEMP_ROOT", "name": "row-temp",
         "lifetime": "ephemeral"},
        {"root_env": "CACHE_ROOT", "name": "compile",
         "lifetime": "persistent"},
    ])
    assert selection == {
        "schema": client.SCRATCH_LIFETIME_SELECTION_SCHEMA_V1,
        "entries": [
            {"root_env": "TEMP_ROOT", "name": "row-temp",
             "lifetime": "ephemeral"},
            {"root_env": "CACHE_ROOT", "name": "compile",
             "lifetime": "persistent"},
        ],
    }
    sealed = {**variables,
              client.SCRATCH_LIFETIME_DECLARATIONS_ENV: json.dumps(selection)}
    assert (local_scratch._scratch_lifetime_selections(sealed)
            == local_scratch._scratch_lifetime_selections(
                {**variables, local_scratch.DECLARATIONS_ENV:
                 json.dumps(selection)}))


@pytest.mark.parametrize("entries", [
    "row-temp", {}, None, True,
    [{}], [{"root_env": "TEMP_ROOT", "name": "row-temp"}],
    [{"root_env": "TEMP_ROOT", "name": "row-temp", "lifetime": "row"}],
    [{"root_env": "TEMP_ROOT", "name": "../foreign", "lifetime": "ephemeral"}],
    [{"root_env": "TEMP_ROOT", "name": "row-temp", "lifetime": "ephemeral",
      "path": "/foreign"}],
    [{"root_env": "TEMP_ROOT", "name": "row-temp", "lifetime": "ephemeral"},
     {"root_env": "TEMP_ROOT", "name": "row-temp", "lifetime": "persistent"}],
    [{"root_env": f"TEMP-{index}", "name": "row-temp", "lifetime": "ephemeral"}
     for index in range(65)],
])
def test_sdk_selection_builder_refuses_without_io(entries):
    with pytest.raises(local_scratch.LocalScratchError):
        client.build_scratch_lifetime_selection(entries)


def test_sdk_selection_builder_empty_is_opt_out():
    assert client.build_scratch_lifetime_selection([]) == {
        "schema": client.SCRATCH_LIFETIME_SELECTION_SCHEMA_V1, "entries": []}
