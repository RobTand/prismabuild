"""Source controls for the mandatory finite caller cutover (Refs #1483).

Private CPU fixtures only. The actual filesystem/committed-operation owner is
qualified by its own integration controls; these tests exercise caller order,
unchanged intent, exact paths and unwind without creating a live operation.
"""
from __future__ import annotations

# These are caller-cutover source controls: they exercise caller order and
# exact paths against the shared filesystem guard, which lands in its own
# integration lane. Without that module present there is nothing to cut over
# to, so the controls skip rather than misreport a missing dependency.
pytest.importorskip("prismabuild.filesystem_capacity")

from contextlib import contextmanager
import importlib.util
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _tool(name):
    spec = importlib.util.spec_from_file_location(
        f"filesystem_submission_{name}", ROOT / "tools/fleet" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pbrun = _tool("pbrun")
pbcampaign = _tool("pbcampaign")
from prismabuild import filesystem_capacity as fs, movement_actions

GIB = 1 << 30


def _intent(root):
    def entry(classes, *, resource="spool_gb", maximum=4 * GIB + 17):
        return {"root": str(root.resolve()), "max_bytes": maximum,
                "classes": classes, "resource": resource}
    return {"schema": fs.OPERATION_SCHEMA, "operations": {
        "coordinator": [entry(["source", "cas", "queue", "logs", "scratch"])],
        "worker": [entry(["checkout", "cas", "queue", "logs", "output", "scratch"])],
        "storage": [entry(["cas", "queue", "logs", "stage"],
                          resource="stage_gib@fixture-stage", maximum=GIB + 17)],
    }}


@pytest.fixture
def scope(monkeypatch, tmp_path):
    events = []
    parent_owner = {"held": True, "released": False}
    @contextmanager
    def observe(queue, intent, paths, *, role, operation_key=None, demand=None):
        assert parent_owner["held"] and not parent_owner["released"]
        if role == "coordinator":
            assert operation_key is not None, "coordinator custody needs its committed key"
            assert re.fullmatch(r"operation-p-[0-9a-f]{64}", operation_key)
            assert demand is None, "no byte forecast substitutes for the sealed terms"
        else:
            assert operation_key is None, f"claimed {role} rides its action owner, not a key"
        events.append(("enter", intent, tuple(paths), role, operation_key))
        try:
            yield
        finally:
            # Exiting this finite caller never releases its containing owner.
            assert parent_owner == {"held": True, "released": False}
            events.append(("exit",))
    monkeypatch.setattr(fs, "reserve_operation", observe)
    monkeypatch.setattr(pbrun, "git_repository_root", lambda _path: None)
    raw = json.dumps(_intent(tmp_path))
    return raw, events, parent_owner


def test_main_guard_precedes_preparation_and_unwinds_partial_failure(scope, monkeypatch, tmp_path):
    raw, events, owner = scope
    args = SimpleNamespace(env=[fs.OPERATION_ENV + "=" + raw], cwd=str(tmp_path),
        data_manifest=None, produced_output_template=None, residency="none",
        withdraw=None, release_origin_consumer=None)
    monkeypatch.setattr(pbrun, "parse_args", lambda: args)
    marker = tmp_path / "first-publication-write"
    def prepare(_args, **kwargs):
        assert events[0][0] == "enter"
        marker.write_text("partial finite publication")
        raise RuntimeError("causal publication failure after first write")
    monkeypatch.setattr(pbrun, "_submission_main", prepare)
    with pytest.raises(RuntimeError, match="causal publication failure"):
        pbrun.main()
    assert marker.read_text() == "partial finite publication"
    assert events[-1] == ("exit",)
    assert owner == {"held": True, "released": False}


def test_direct_freeze_guard_precedes_git_exclude_and_cas(scope, monkeypatch, tmp_path):
    raw, events, _owner = scope
    monkeypatch.setattr(pbrun, "_git_identity", lambda _cwd: {"head": "a" * 40})
    monkeypatch.setattr(pbrun, "container_owner", lambda *a, **k: "b" * 64)
    def first_write(_cwd, **kwargs):
        assert events[0][0] == "enter"
        raise RuntimeError("first Git exclude write")
    monkeypatch.setattr(pbrun, "keep_droppings_out_of_git", first_write)
    def no_cas(*args, **kwargs):
        pytest.fail("CAS must not precede the first guarded Git write")
    monkeypatch.setattr(pbrun.pb, "PrismaBuildCAS", no_cas)
    with pytest.raises(RuntimeError, match="first Git exclude write"):
        pbrun.freeze_action_template(
            command=["/bin/true"], cwd=tmp_path, logical_cwd=".",
            demand={"cpu": 1, "mem_gb": 1, "spool_gb": 5},
            placement={"required_tags": []}, variables={fs.OPERATION_ENV: raw},
            determinism="stochastic", retry_policy={"max_attempts": 1, "retry_safe": False},
            host_class=None, measurement=False, transport="pool", pool_measurement_class=False,
            data_manifest_path=None, checkout_snapshot_max_bytes=GIB, snapshot_refs=[],
            exclusive=False, gpu_memory_gb=None, execution_timeout_s=None, progress=None,
            profile=None)
    assert events[-1] == ("exit",)


@pytest.mark.parametrize("change", ["missing", "bad-json", "no-worker", "boolean-bound", "zero-bound", "missing-class"])
def test_invalid_complete_intent_refuses_before_backend_or_write(scope, tmp_path, change):
    raw, events, _owner = scope
    intent = json.loads(raw)
    if change == "missing":
        variables = {}
    elif change == "bad-json":
        variables = {fs.OPERATION_ENV: "{not-json"}
    else:
        if change == "no-worker":
            intent["operations"].pop("worker")
        elif change == "boolean-bound":
            intent["operations"]["worker"][0]["max_bytes"] = True
        elif change == "zero-bound":
            intent["operations"]["coordinator"][0]["max_bytes"] = 0
        else:
            intent["operations"]["coordinator"][0]["classes"].remove("queue")
        variables = {fs.OPERATION_ENV: json.dumps(intent)}
    with pytest.raises((fs.LocalScratchError, pbrun.pb.PrismaBuildError)):
        with pbrun.coordinator_publication(variables, source=tmp_path):
            pytest.fail("invalid intent reached the write region")
    assert not events


def test_nested_call_forwards_new_actual_paths_and_keeps_parent_owner(scope, tmp_path):
    raw, events, owner = scope
    real_file = tmp_path / "existing-source-bank"
    real_file.write_text("declared source bytes")
    queue = SimpleNamespace(root=tmp_path)
    cas = SimpleNamespace(root=tmp_path)
    with pbrun.coordinator_publication({fs.OPERATION_ENV: raw}, cas=cas, queue=queue):
        with pbrun.coordinator_publication(
                {fs.OPERATION_ENV: raw}, cas=cas, queue=queue, inputs=(real_file,)):
            assert events[-1][0] == "enter"
            assert real_file in events[-1][2]
            # The nested window joins the enclosing committed operation: same
            # custody key, new exact paths, parent owner never re-committed.
            assert events[-1][4] == events[0][4]
        other_queue = SimpleNamespace(root=tmp_path / "other-queue")
        (tmp_path / "other-queue").mkdir()
        with pbrun.coordinator_publication({fs.OPERATION_ENV: raw}, cas=cas,
                                           queue=other_queue):
            # A different queue is its own committed operation, never a join.
            assert events[-1][4] != events[0][4]
    assert [event[0] for event in events] == ["enter", "enter", "enter", "exit", "exit", "exit"]
    assert owner == {"held": True, "released": False}
    first_key = events[0][4]
    events.clear()
    with pbrun.coordinator_publication({fs.OPERATION_ENV: raw}, cas=cas, queue=queue):
        pass
    # A later sequential phase is a fresh committed operation, not a reuse of
    # the released custody key.
    assert events[0][4] != first_key


def test_future_leaf_uses_existing_parent_but_real_file_namespace_is_not_dropped(tmp_path):
    real = tmp_path / "actual-file"
    real.write_text("real descriptor target")
    assert pbrun._existing_operation_path(real) == real
    assert pbrun._existing_operation_path(tmp_path / "future" / "result") == tmp_path


def test_worker_growth_is_the_whole_explicit_bound_not_the_compressed_snapshot(scope):
    raw, _events, _owner = scope
    assert pbrun.local_disk_terms({fs.OPERATION_ENV: raw}, transport="pool") == {"spool_gb": 5}
    with pytest.raises(SystemExit, match="existing pool containment owner"):
        pbrun.local_disk_terms({fs.OPERATION_ENV: raw}, transport="slurm")


def test_direct_publication_joins_sealed_action_and_unwinds_failure(scope, tmp_path):
    raw, events, owner = scope
    action = {"action_key": "a" * 64, "params": {"demand": {"cpu": 1, "spool_gb": 5}},
              "environment": {"variables": {fs.OPERATION_ENV: raw}}}
    row = {"action_key": action["action_key"], "resources": action["params"]["demand"],
           "cas_root": str(tmp_path)}
    def publish(**kwargs):
        assert events[0][0] == "enter"
        raise RuntimeError("actual finite queue failure")
    queue = SimpleNamespace(root=tmp_path, publish=publish)
    with pytest.raises(RuntimeError, match="actual finite queue failure"):
        pbrun.publish_or_refuse(queue, row, action=action)
    assert events[-1] == ("exit",)
    assert not owner["released"]
    events.clear()
    with pytest.raises(SystemExit, match="differs from its sealed action"):
        pbrun.publish_or_refuse(queue, {**row, "resources": {"spool_gb": 0}}, action=action)
    assert not events


def test_campaign_index_and_group_write_only_inside_exact_operation(scope, monkeypatch, tmp_path):
    raw, events, _owner = scope
    monkeypatch.setattr(pbcampaign, "pbrun", pbrun)
    def publish(path, payload):
        assert events[-1][0] == "enter"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return True
    monkeypatch.setattr(pbcampaign.pb, "_atomic_publish", publish)
    cas = SimpleNamespace(root=tmp_path)
    pbcampaign.publish_index({"children": []}, cas=cas, parent_key="a" * 64,
                            filesystem_operation=raw)
    pbcampaign.publish_group_receipt({"children": []}, cas=cas, parent_key="b" * 64,
                                    filesystem_operation=raw)
    assert [event[0] for event in events] == ["enter", "exit", "enter", "exit"]


def _movement_template(raw):
    return {"params": {"cwd": "."}, "environment": {
        "variables": {fs.OPERATION_ENV: raw, "TMPDIR": "/consumer-only"}, "toolchain": {}},
        "marker_root": Path("/fixture-markers"), "checkout_identity": {},
        "task": {"definition_id": "fixture", "definition_version": "v1", "working_directory": "."},
        "inputs": [], "code_closure": {}}


def test_mover_inherits_unchanged_storage_operation_and_real_role_terms(scope, monkeypatch):
    raw, _events, _owner = scope
    monkeypatch.setattr(movement_actions.pb, "seal_action", lambda body: body)
    action = movement_actions.seal_movement_action(
        _movement_template(raw), command=["/usr/bin/python3", "/fixture/stage_move.py"],
        demand={"cpu": 1, "mem_gb": 1, "stage_gib@fixture-stage": 1}, tags=[],
        log_name="mover.log", container_owner_fn=lambda *a, **k: "a" * 64)
    assert action["environment"]["variables"][fs.OPERATION_ENV] == raw
    assert "TMPDIR" not in action["environment"]["variables"]
    assert action["params"]["filesystem_role"] == "storage"
    assert action["params"]["demand"]["stage_gib@fixture-stage"] == 3


def test_mover_missing_storage_envelope_refuses_before_owner_or_seal(scope):
    raw, _events, _owner = scope
    intent = json.loads(raw)
    intent["operations"].pop("storage")
    def no_owner(*args, **kwargs):
        pytest.fail("missing storage operation reached owner construction")
    with pytest.raises(fs.LocalScratchError, match="storage growth intent missing"):
        movement_actions.seal_movement_action(
            _movement_template(json.dumps(intent)), command=["/usr/bin/python3", "mover.py"],
            demand={"cpu": 1}, tags=[], log_name="mover.log", container_owner_fn=no_owner)


def test_produced_spool_refuses_sealed_request_without_envelope_before_writes(
        scope, monkeypatch, tmp_path):
    raw, events, _owner = scope
    produced_spool = importlib.import_module("prismabuild.produced_spool")
    # An export request sealed without the envelope has no operation to
    # reserve: it refuses at spool entry, before any write and before the
    # window is ever entered -- under the guarded generation preflight
    # already holds such requests, so there is no reachable legacy path.
    # (ProducedSpool's own _operation refuses identically; the spool unit
    # fixtures that sealed no envelope are updated by the parent's fixture
    # pass, since only they can construct that owner context.)
    claim = {"cas_root": str(tmp_path / "cas")}
    with pytest.raises(fs.LocalScratchError):
        with produced_spool._claimed_operation(
                SimpleNamespace(root=tmp_path), claim, "a" * 64, tmp_path):
            pytest.fail("an envelope-less sealed request reached the window")
    assert not events
