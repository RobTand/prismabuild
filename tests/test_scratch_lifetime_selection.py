"""Versioned lifetime intent is separate from legacy naming-only evidence."""
import copy
import json

import pytest

from prismabuild import client, local_scratch


SCHEMA = "prismabuild.scratch_lifetime_selection.v1"
TEMP = {"root_env": "TEMP_ROOT", "name": "row-temp", "lifetime": "ephemeral"}
CACHE = {"root_env": "CACHE_ROOT", "name": "compile", "lifetime": "persistent"}


@pytest.fixture
def variables(tmp_path):
    return {
        local_scratch.PAIRS_ENV: "TEMP_ROOT:TEMP_MAX,CACHE_ROOT:CACHE_MAX",
        "TEMP_ROOT": str(tmp_path / "temporary"), "TEMP_MAX": "1024",
        "CACHE_ROOT": str(tmp_path / "persistent"), "CACHE_MAX": "2048",
    }


def select(variables, entries):
    variables = {**variables, local_scratch.DECLARATIONS_ENV:
                 json.dumps({"schema": SCHEMA, "entries": entries})}
    return local_scratch._scratch_lifetime_selections(variables)


def test_sealed_versioned_intent_keeps_temporary_and_persistent_entries_distinct(variables):
    selected = select(variables, [TEMP, CACHE])
    assert selected == [
        {**TEMP, "max_env": "TEMP_MAX", "root": variables["TEMP_ROOT"], "max_bytes": 1024},
        {**CACHE, "max_env": "CACHE_MAX", "root": variables["CACHE_ROOT"], "max_bytes": 2048},
    ]
    assert local_scratch.scratch_terms(variables) == {"spool_gb": 2}


@pytest.mark.parametrize("legacy", [None, "", "[]", '[{"root_env":"TEMP_ROOT","name":"row-temp"}]'])
def test_legacy_naming_does_not_acquire_cleanup_lifetime(variables, legacy):
    if legacy is not None:
        variables[local_scratch.DECLARATIONS_ENV] = legacy
    assert local_scratch._scratch_lifetime_selections(variables) == []


def test_selection_is_nondestructive_and_sdk_advertises_versioned_capability(variables, tmp_path):
    cache = tmp_path / "persistent"
    cache.mkdir()
    sentinel = cache / "kernel.cache"
    sentinel.write_bytes(b"persistent compilation cache")
    before = copy.deepcopy(variables)
    select(variables, [TEMP, CACHE])
    assert variables == before
    assert sentinel.read_bytes() == b"persistent compilation cache"
    assert not (tmp_path / "temporary").exists()
    assert client.SCRATCH_LIFETIME_TAG in client.CAPABILITIES
    assert client.SDK_VERSION == 6


@pytest.mark.parametrize("entry", [
    {}, {**TEMP, "lifetime": "row"}, {**TEMP, "lifetime": True},
    {**TEMP, "root_env": "UNDECLARED"}, {**TEMP, "name": "../foreign"},
    {**TEMP, "name": "child/foreign"}, {**TEMP, "cleanup_required": True},
    {**TEMP, "path": "/foreign"}, {**TEMP, "owner_attempt": {"nonce": "a" * 32}},
])
def test_request_cannot_inject_authority_or_ambiguous_lifetime(variables, entry):
    with pytest.raises(local_scratch.LocalScratchError):
        select(variables, [entry])


@pytest.mark.parametrize("raw", [
    "null", "{}", "{", '{"schema":"unknown","entries":[]}',
    '{"schema":"prismabuild.scratch_lifetime_selection.v1","entries":{},"extra":0}',
    '{"schema":"prismabuild.scratch_lifetime_selection.v1","entries":null}',
    '{"schema":"prismabuild.scratch_lifetime_selection.v1","entries":[],"entries":[]}',
])
def test_malformed_or_unknown_version_refuses(variables, raw):
    variables[local_scratch.DECLARATIONS_ENV] = raw
    with pytest.raises(local_scratch.LocalScratchError):
        local_scratch._scratch_lifetime_selections(variables)


@pytest.mark.parametrize("entries", [[TEMP, TEMP], [TEMP, {**TEMP, "lifetime": "persistent"}]])
def test_duplicate_names_cannot_disagree_about_lifetime(variables, entries):
    with pytest.raises(local_scratch.LocalScratchError):
        select(variables, entries)


@pytest.mark.parametrize("overlap", ["equal", "cache-below-temp", "temp-below-cache"])
def test_persistent_cache_root_cannot_overlap_an_ephemeral_root(variables, overlap):
    if overlap == "equal":
        variables["CACHE_ROOT"] = variables["TEMP_ROOT"]
    elif overlap == "cache-below-temp":
        variables["CACHE_ROOT"] = variables["TEMP_ROOT"] + "/compile"
    else:
        variables["TEMP_ROOT"] = variables["CACHE_ROOT"] + "/temporary"
    with pytest.raises(local_scratch.LocalScratchError):
        select(variables, [TEMP, CACHE])


def test_selection_bounds_and_unknown_fields_refuse(variables):
    with pytest.raises(local_scratch.LocalScratchError):
        select(variables, [{**TEMP, "name": f"tmp-{i}"} for i in range(65)])
    variables[local_scratch.DECLARATIONS_ENV] = " " * (16 * 1024) + "[]"
    with pytest.raises(local_scratch.LocalScratchError):
        local_scratch._scratch_lifetime_selections(variables)


def test_existing_naming_recorder_refuses_required_intent_until_owner_integration(variables):
    variables[local_scratch.DECLARATIONS_ENV] = json.dumps({"schema": SCHEMA, "entries": [TEMP]})
    with pytest.raises(local_scratch.LocalScratchError):
        local_scratch._scratch_selections(variables)
