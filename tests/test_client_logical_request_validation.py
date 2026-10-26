"""SDK7 exposes PB's validator, not a second schema or a submission API (#2149)."""
from copy import deepcopy
import inspect

import pytest

from prismabuild import client, core as pb, decomposition as dc, immutable_files


def _d44_request(*, projection=False):
    """The adapter's emitted shape, without importing PQ or requiring its runtime."""
    request = {
        "schema": "prismabuild.logical_request.v1",
        "common": {
            "argv": ["python", "-m", "consumer.measure", "--pb-task-batch",
                     "{pb.task_batch}"],
            "cwd": ".", "data_manifest": None,
            "demand": {"cpu": 2, "mem_gb": 4, "gpu": 1},
            "env": {"OMP_NUM_THREADS": "1"}, "gpu_memory_gb": 4,
        },
        "roster": {
            "schema": "prismabuild.logical_task_roster.v1",
            # Deliberately non-lexical: order is the producer's declaration.
            "tasks": [{
                "id": identity, "output_id": "out/" + identity,
                "payload": {"rate": rate, "reads": []},
                "residency_key": "model-a", "estimated_seconds": 8,
                "estimate_evidence": "measured-pilot",
            } for identity, rate in (("z-last", 832), ("a-first", 864))],
        },
        "batch_policy": {
            "schema": "prismabuild.roster_batch_policy.v1",
            "residencies": [{"key": "model-a", "setup_seconds": 0,
                             "setup_evidence": "measured-pilot"}],
            "max_setup_fraction": 0.2, "max_estimated_wall_seconds": 300,
        },
    }
    if projection:
        request["task_data_manifest"] = {
            "schema": "prismabuild.task_data_manifest.v1",
            "payload_field": "reads", "mount_prefix": "/declared-inputs",
            "residency_tier": None, "residency_ram": "off",
            "mover_readers": 1, "mover_mem_gb": 1,
            "prefetch_depth_gib": 0, "read_mb_s": 10,
        }
        request["roster"]["tasks"][0]["payload"]["reads"] = [
            {"path": "pilot.bin", "offset": 0, "bytes": 16, "sha256": None}]
    return request


def test_public_validator_is_the_same_callable_with_the_full_signature():
    assert hasattr(client, "validate_logical_request"), "SDK lacks PB-owned validation"
    assert client.validate_logical_request is dc.validate_logical_request
    assert inspect.signature(client.validate_logical_request) == inspect.signature(dc.validate_logical_request)
    # Positional-or-keyword `value`, including keyword callers, stays supported.
    request = _d44_request()
    assert client.validate_logical_request(value=request) == dc.validate_logical_request(request)


@pytest.mark.parametrize("projection", [False, True], ids=["d44", "task-data"])
def test_public_validation_canonicalizes_without_mutation_or_reordering(projection):
    request = _d44_request(projection=projection)
    before = deepcopy(request)
    expected = dc.validate_logical_request(request)
    assert hasattr(client, "validate_logical_request"), "SDK lacks PB-owned validation"
    actual = client.validate_logical_request(request)
    assert actual == expected
    assert request == before
    assert actual is not request
    assert actual["roster"] is not request["roster"]
    assert [task["id"] for task in actual["roster"]["tasks"]] == ["z-last", "a-first"]
    assert type(actual["roster"]["tasks"][0]["estimated_seconds"]) is float
    assert type(actual["batch_policy"]["residencies"][0]["setup_seconds"]) is float
    # The canonical result owns its nested payloads too.
    actual["roster"]["tasks"][0]["payload"]["reads"].append({"not": "an input mutation"})
    assert request == before


# Edits describe malformed declarations, not planner infeasibility. Exact
# exception type and diagnostic parity protect the existing refusal semantics.
_BAD_DECLARATIONS = [
    pytest.param(("schema",), "unknown.v1", id="request-schema"),
    pytest.param(("unknown",), True, id="unknown-request-field"),
    pytest.param(("common", "demand"), {}, id="empty-demand"),
    pytest.param(("common", "env"), {"1BAD": "x"}, id="invalid-env"),
    pytest.param(("common", "timeout_s"), 0, id="common-policy"),
    pytest.param(("roster", "schema"), "unknown.v1", id="roster-schema"),
    pytest.param(("roster", "tasks"), [], id="empty-roster"),
    pytest.param(("roster", "tasks", 1, "id"), "z-last", id="duplicate-task-id"),
    pytest.param(("roster", "tasks", 1, "output_id"), "out/z-last", id="duplicate-output-id"),
    pytest.param(("roster", "tasks", 0, "estimated_seconds"), 0, id="zero-estimate"),
    pytest.param(("roster", "tasks", 0, "estimated_seconds"), -1, id="negative-estimate"),
    pytest.param(("roster", "tasks", 0, "estimated_seconds"), True, id="boolean-estimate"),
    pytest.param(("roster", "tasks", 0, "estimated_seconds"), "8", id="string-estimate"),
    pytest.param(("roster", "tasks", 0, "estimated_seconds"), float("inf"), id="infinite-estimate"),
    pytest.param(("roster", "tasks", 0, "estimate_evidence"), "", id="empty-evidence"),
    pytest.param(("roster", "tasks", 0, "residency_key"), "unpriced", id="unpriced-residency"),
    pytest.param(("batch_policy", "schema"), "unknown.v1", id="policy-schema"),
    pytest.param(("batch_policy", "max_setup_fraction"), 0, id="zero-fraction"),
    pytest.param(("batch_policy", "max_setup_fraction"), 1.1, id="excess-fraction"),
    pytest.param(("batch_policy", "max_setup_fraction"), True, id="boolean-fraction"),
    pytest.param(("batch_policy", "max_estimated_wall_seconds"), 0, id="invalid-wall-limit"),
    pytest.param(("batch_policy", "residencies", 0, "setup_seconds"), -1, id="negative-setup"),
    pytest.param(("common", "argv"), ["python", "collect.py"], id="missing-placeholder"),
    pytest.param(("common", "argv"), ["python", "{pb.task_batch}", "{pb.task_batch}"], id="duplicate-placeholder"),
    pytest.param(("common", "argv"), ["python", "--batch={pb.task_batch}"], id="embedded-placeholder"),
    pytest.param(("task_data_manifest", "worker_count"), 9, id="unknown-projection-policy"),
    pytest.param(("task_data_manifest", "read_mb_s"), 0, id="invalid-read-rate"),
    pytest.param(("common", "data_manifest"), "shared.json", id="projection-and-shared-manifest"),
    pytest.param(("roster", "tasks", 0, "payload"), {}, id="missing-reads"),
    pytest.param(("roster", "tasks", 0, "payload", "reads", 0, "path"), "../outside", id="projection-path-escape"),
    pytest.param(("roster", "tasks", 0, "payload", "reads", 0, "bytes"), -1, id="projection-size"),
]


@pytest.mark.parametrize("path,value", _BAD_DECLARATIONS)
def test_public_refusals_preserve_internal_exception_and_diagnostic(path, value):
    request = _d44_request(projection=True)
    target = request
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value
    before = deepcopy(request)
    with pytest.raises(pb.ActionContractError) as internal:
        dc.validate_logical_request(request)
    assert hasattr(client, "validate_logical_request"), "SDK lacks PB-owned validation"
    with pytest.raises(ValueError) as public:
        client.validate_logical_request(request)
    assert type(public.value) is type(internal.value)
    assert str(public.value) == str(internal.value)
    assert request == before


@pytest.mark.parametrize("value", [None, [], "not a request", 3])
def test_public_validator_accepts_object_and_refuses_non_mapping_values(value):
    with pytest.raises(pb.ActionContractError) as internal:
        dc.validate_logical_request(value)
    assert hasattr(client, "validate_logical_request"), "SDK lacks PB-owned validation"
    with pytest.raises(ValueError) as public:
        client.validate_logical_request(value)
    assert type(public.value) is type(internal.value)
    assert str(public.value) == str(internal.value)


def test_public_validation_does_not_plan_stage_publish_or_prove_feasibility(tmp_path, monkeypatch):
    request = _d44_request(projection=True)
    # Structurally sound but impossible to amortize or fit: validation must
    # still succeed. Paths/argv are declarations, not executable feasibility.
    request["batch_policy"]["residencies"][0]["setup_seconds"] = 1000
    request["batch_policy"]["max_estimated_wall_seconds"] = 1
    request["common"]["cwd"] = str(tmp_path / "not-a-checkout")
    expected = dc.validate_logical_request(request)

    def forbidden_effect(*args, **kwargs):
        pytest.fail("logical-request validation attempted planning or publication")

    for name in ("build_plan", "partition_roster", "freeze_common", "freeze_plan",
                 "publish_children", "write_document"):
        monkeypatch.setattr(dc, name, forbidden_effect)
    monkeypatch.setattr(immutable_files, "publish_immutable", forbidden_effect)
    monkeypatch.setattr(pb.PrismaBuildCAS, "__init__", forbidden_effect)
    monkeypatch.setattr(client.PoolQueue, "__init__", forbidden_effect)
    monkeypatch.chdir(tmp_path)
    before = set(tmp_path.rglob("*"))
    assert hasattr(client, "validate_logical_request"), "SDK lacks PB-owned validation"
    assert client.validate_logical_request(request) == expected
    request["roster"]["tasks"][0]["residency_key"] = "unpriced"
    with pytest.raises(ValueError, match="is not priced"):
        client.validate_logical_request(request)
    assert set(tmp_path.rglob("*")) == before
