"""A movement node is a DAG node with a declared tier hop and a bound consumer (#583).

The graph refuses a mover that reserves a GPU, a mover whose consumer is
unknown or does not read the same manifest, and a consumer whose edge to
the mover is bound to anything but the mover's residency descriptor.  Each
refusal is driven from a valid graph by changing one thing, so a passing
test says the check bites rather than that the fixture is malformed.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from prismabuild import core as pb  # noqa: E402
from prismabuild import dagster as pd  # noqa: E402
from prismabuild import slurm as ps  # noqa: E402
from test_dagster import _action, _resources, _spec  # noqa: E402

MANIFEST = {
    "id": pb.PBCAMPAIGN_DATA_MANIFEST_INPUT_ID,
    "sha256": hashlib.sha256(b"manifest").hexdigest(),
    "bytes": 4096,
}


def _movement(consumer_key: str, **overrides: object) -> pd.MovementSpec:
    fields: dict[str, object] = {
        "source_tier": "pool",
        "destination_tier": "stage",
        "manifest_input_id": MANIFEST["id"],
        "range_start_bytes": 0,
        "range_end_bytes": 34_400_000_000,
        "consumer_action_key": consumer_key,
    }
    fields.update(overrides)
    return pd.MovementSpec(**fields)  # type: ignore[arg-type]


def _graph(
    tmp_path: Path,
    *,
    mover_gpus: int = 0,
    consumer_manifest: dict[str, object] | None = MANIFEST,
    edge_input_id: str = f"{pd.RESIDENCY_INPUT_PREFIX}0",
    bind_descriptor: bool = True,
    consumer_depends: bool = True,
    consumer_key_override: str | None = None,
) -> tuple[pd.ActionSpec, pd.ActionSpec]:
    """A mover and its consumer, valid unless one knob says otherwise."""

    consumer_action = _action(tmp_path, "consumer", inputs=[MANIFEST])
    consumer_key = consumer_key_override or "c" * 64
    movement = _movement(consumer_key)
    mover_action = _action(tmp_path, "mover", inputs=[MANIFEST])
    resources = _resources()
    if mover_gpus:
        resources = ps.SlurmResources(
            cpus=resources.cpus, memory_mib=resources.memory_mib, gpus=mover_gpus,
            constraint=resources.constraint, partition=resources.partition,
            account=resources.account, qos=resources.qos, time_limit=resources.time_limit,
        )
    mover = pd.ActionSpec(
        action=mover_action, checkout_root=tmp_path, resources=resources,
        placement=ps.SlurmPlacement(platform_key=None, host_class=None),
        poll_interval_seconds=0.001, max_polls=3, movement=movement,
    )
    binding = pb.residency_descriptor_binding(movement.descriptor(MANIFEST))
    if not bind_descriptor:
        binding = {"sha256": hashlib.sha256(b"not the descriptor").hexdigest(), "bytes": 3}
    dependency = pd.CASDependency(
        upstream_action_key=mover.action_key, input_id=edge_input_id,
        result_sha256=str(binding["sha256"]), result_bytes=int(binding["bytes"]),
    )
    inputs = [{"id": edge_input_id, "sha256": binding["sha256"], "bytes": binding["bytes"]}]
    if consumer_manifest is not None:
        inputs.append(consumer_manifest)
    consumer_action = _action(tmp_path, "consumer", inputs=inputs)
    consumer = _spec(
        consumer_action, tmp_path,
        dependencies=(dependency,) if consumer_depends else (),
    )
    if consumer_key_override is None:
        # Sealing the consumer with the residency input changes its key, so the
        # mover's declared consumer is re-pointed at the sealed key.
        movement = _movement(consumer.action_key)
        mover = pd.ActionSpec(
            action=mover_action, checkout_root=tmp_path, resources=resources,
            placement=ps.SlurmPlacement(platform_key=None, host_class=None),
            poll_interval_seconds=0.001, max_polls=3, movement=movement,
        )
    return mover, consumer


def test_residency_descriptor_is_canonical_and_bounded() -> None:
    descriptor = pb.residency_descriptor(
        manifest_sha256=MANIFEST["sha256"], manifest_bytes=MANIFEST["bytes"],
        tier="stage", range_start_bytes=10, range_end_bytes=20,
    )
    assert descriptor["schema"] == pb.RESIDENCY_DESCRIPTOR_SCHEMA_V1
    binding = pb.residency_descriptor_binding(descriptor)
    canonical = json.dumps(descriptor, sort_keys=True, separators=(",", ":")).encode()
    assert binding == {"sha256": hashlib.sha256(canonical).hexdigest(), "bytes": len(canonical)}
    # Same inputs, same digest: the consumer can bind it before the mover runs.
    again = pb.residency_descriptor(
        manifest_sha256=MANIFEST["sha256"], manifest_bytes=MANIFEST["bytes"],
        tier="stage", range_start_bytes=10, range_end_bytes=20,
    )
    assert pb.residency_descriptor_binding(again) == binding
    for kwargs in (
        {"range_start_bytes": 20, "range_end_bytes": 20},
        {"range_start_bytes": 21, "range_end_bytes": 20},
        {"range_start_bytes": -1, "range_end_bytes": 20},
        {"range_start_bytes": 0, "range_end_bytes": 20, "tier": "nvme"},
        {"range_start_bytes": 0, "range_end_bytes": 20, "manifest_sha256": "abc"},
    ):
        fields = {"manifest_sha256": MANIFEST["sha256"], "manifest_bytes": MANIFEST["bytes"],
                  "tier": "stage", **kwargs}
        with pytest.raises(pb.ActionContractError):
            pb.residency_descriptor(**fields)  # type: ignore[arg-type]


def test_valid_mover_and_consumer_form_a_graph(tmp_path: Path) -> None:
    mover, consumer = _graph(tmp_path)
    graph = pd.ActionGraph([consumer, mover])
    ordered = [spec.action_key for spec in graph.ordered_specs()]
    assert ordered.index(mover.action_key) < ordered.index(consumer.action_key)
    assert mover.movement is not None
    assert mover.movement.consumer_action_key == consumer.action_key


def test_movement_spec_refuses_bad_tiers_and_empty_ranges(tmp_path: Path) -> None:
    key = "c" * 64
    _movement(key)
    with pytest.raises(pd.DagsterGraphError):
        _movement(key, source_tier="stage")
    with pytest.raises(pd.DagsterGraphError):
        _movement(key, destination_tier="nvme")
    with pytest.raises(pd.DagsterGraphError):
        _movement(key, range_start_bytes=5, range_end_bytes=5)
    with pytest.raises(pd.DagsterGraphError):
        _movement(key, consumer_action_key="xyz")


def test_mover_may_not_reserve_a_gpu(tmp_path: Path) -> None:
    with pytest.raises(pd.DagsterGraphError, match="must not reserve a GPU"):
        _graph(tmp_path, mover_gpus=1)


def test_mover_must_carry_the_manifest_it_names(tmp_path: Path) -> None:
    action = _action(tmp_path, "mover", inputs=[])
    with pytest.raises(pd.DagsterGraphError, match="manifest_input_id"):
        pd.ActionSpec(
            action=action, checkout_root=tmp_path, resources=_resources(),
            placement=ps.SlurmPlacement(platform_key=None, host_class=None),
            poll_interval_seconds=0.001, max_polls=3, movement=_movement("c" * 64),
        )


def test_unknown_consumer_is_refused(tmp_path: Path) -> None:
    mover, _consumer = _graph(tmp_path, consumer_key_override="d" * 64)
    with pytest.raises(pd.DagsterGraphError, match="unknown consumer"):
        pd.ActionGraph([mover])


def test_consumer_without_an_edge_to_its_mover_is_refused(tmp_path: Path) -> None:
    mover, consumer = _graph(tmp_path, consumer_depends=False)
    with pytest.raises(pd.DagsterGraphError, match="declares no dependency"):
        pd.ActionGraph([mover, consumer])


def test_consumer_must_bind_the_descriptor_not_another_blob(tmp_path: Path) -> None:
    mover, consumer = _graph(tmp_path, bind_descriptor=False)
    with pytest.raises(pd.DagsterGraphError, match="not that movement's residency descriptor"):
        pd.ActionGraph([mover, consumer])


def test_residency_edge_must_use_the_residency_input_prefix(tmp_path: Path) -> None:
    mover, consumer = _graph(tmp_path, edge_input_id="prismabuild/stage")
    with pytest.raises(pd.DagsterGraphError, match="residency inputs are named"):
        pd.ActionGraph([mover, consumer])


def test_consumer_must_read_the_same_manifest(tmp_path: Path) -> None:
    other = dict(MANIFEST, sha256=hashlib.sha256(b"other manifest").hexdigest())
    mover, consumer = _graph(tmp_path, consumer_manifest=other)
    with pytest.raises(pd.DagsterGraphError, match="do not bind the same"):
        pd.ActionGraph([mover, consumer])
    mover, consumer = _graph(tmp_path, consumer_manifest=None)
    with pytest.raises(pd.DagsterGraphError, match="do not bind the same"):
        pd.ActionGraph([mover, consumer])


def test_action_spec_config_round_trips_v1_and_v2(tmp_path: Path) -> None:
    mover, consumer = _graph(tmp_path)
    config = mover.as_config()
    assert config["schema"] == pd.DAGSTER_ACTION_SPEC_SCHEMA_V2
    assert config["movement"] == mover.movement.as_dict()  # type: ignore[union-attr]
    rebuilt = pd.ActionSpec.from_config(mover.action, config)
    assert rebuilt.movement == mover.movement
    plain = consumer.as_config()
    assert "movement" not in plain
    assert pd.ActionSpec.from_config(consumer.action, plain).movement is None
    # A v1 record, which predates the field, is still read.
    v1 = dict(plain, schema=pd.DAGSTER_ACTION_SPEC_SCHEMA_V1)
    assert pd.ActionSpec.from_config(consumer.action, v1).movement is None
    # A v2 record with an explicit null movement is a plain node.
    v2_null = dict(plain, movement=None)
    assert pd.ActionSpec.from_config(consumer.action, v2_null).movement is None
    # A v1 record must not carry a movement it has no schema for.
    with pytest.raises(pd.DagsterGraphError):
        pd.ActionSpec.from_config(mover.action, dict(config, schema=pd.DAGSTER_ACTION_SPEC_SCHEMA_V1))
