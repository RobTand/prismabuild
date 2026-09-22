"""#849: distinct produced namespaces must not share canonical basenames.

Real CAS-sealed publication, admitted private-queue mover execution, strict
SDK pinned reads and owner-aware retirement. Tiny CPU payloads only; the
outer test itself is submitted through PB. Fixture containment is synthetic,
with the same explicit unpaced mover concession as the restage suite.
"""
from __future__ import annotations

import copy
import os
import secrets
from pathlib import Path

import pytest

import test_produced_output_restage as fx

po, rlc, map_mod = fx.po, fx.rlc, fx.map_mod


@pytest.fixture(autouse=True)
def isolated_synthetic_launch(monkeypatch):
    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT", "PRISMABUILD_ACTION_KEY"):
        monkeypatch.delenv(name, raising=False)


def _second_producer(first):
    second = copy.copy(first)
    second.template = fx._template(str(first.tmp / "other-outputs"), window_gib=2)
    second.template["template_id"] = "other-producer-v1"
    producer_root = first.tmp / "other-producer"
    second.owner = fx._producer_request(producer_root, first.cas_root, second.template)
    assert second.owner != first.owner
    second.inst = fx._bind(first.q, second.template, second.owner, first.cas_root,
                           checkout_root=producer_root / "mover-checkout")
    second.publish_kwargs = dict(producer_action_key=second.owner,
                                 command_extra=["--unpaced"])
    return second


def _stage(world, batch_id, payload):
    desc = fx._descriptors(world.template, world.inst, "p-shared", payload,
                           digest_mode="sha256")
    fx._prewrite(world.q, world.inst, world.template, batch_id, desc)
    batch = world.first_publish(batch_id, desc)
    mover = batch["mover_key"]
    assert fx._claim_mover(world.q, "w-" + batch_id)["action_key"] == mover
    outcome = world.q.execute(fx.pool._read_json(world.q.item_path("claimed", mover)), timeout_s=240)
    receipt = world.q.move_record(mover)
    assert outcome.get("returncode") == 0, {
        "returncode": outcome.get("returncode"),
        "copy": {k: receipt.get(k) for k in ("complete", "refusal", "errors")} if receipt else None,
    }
    assert receipt["complete"] is True
    world.q.finish(mover, status="executed")
    return batch


def _pin(world, batch):
    fragments = map_mod.read_fragments(world.out_base, batch["batch_namespace"])
    entries = map_mod.compose(fragments)["entries"]
    assert len(entries) == 1
    key = next(iter(entries))
    selected = rlc.covers_for_keys(
        world.out_base, batch["batch_namespace"], [key], tier_id=fx.TIER,
        manifest_sha256=batch["manifest_digest"], epoch="")
    assert selected["ok"] is True, selected
    read = rlc.acquire(
        world.q, consumer_action_key=batch["batch_namespace"],
        attempt=dict(world.inst["owner_attempt"]), tier_id=fx.TIER, epoch="",
        span={"start_bytes": 0, "end_bytes": entries[key]["bytes"]},
        holder={"host": "fixture", "pid": os.getpid()},
        acquire_token=secrets.token_hex(16), covers=selected["covers"],
        expected=selected["expected"], owner_action_key=fx.DOWNSTREAM,
        residency_root=str(world.out_base))
    assert read["ok"] is True, read
    return read, key, Path(entries[key]["stage_path"])


def _read(world, read, key):
    fd, serving = rlc.open_pinned(world.q, read["pin"], read["ref_id"], key,
                                  residency_root=str(world.out_base))
    try:
        assert serving["tier_id"] == fx.TIER
        return os.read(fd, 65536)
    finally:
        os.close(fd)


def _release(world, read):
    assert rlc.release(world.q, read["pin_id"], read["ref_id"],
                       consumer_action_key=fx.DOWNSTREAM,
                       residency_root=str(world.out_base)) is True


def test_independent_producers_same_basename_keep_own_bytes_and_pins(tmp_path):
    first = fx._World(tmp_path, window_gib=2, gib=8)
    second = _second_producer(first)
    a = _stage(first, "b1", b"first producer payload")
    pin_a, key_a, path_a = _pin(first, a)
    assert _read(first, pin_a, key_a) == b"first producer payload"

    # RED: the second real mover refuses the first owner's different bytes.
    b = _stage(second, "b1", b"second producer bytes")
    pin_b, key_b, path_b = _pin(second, b)
    assert path_a != path_b
    assert _read(first, pin_a, key_a) == b"first producer payload"
    assert _read(second, pin_b, key_b) == b"second producer bytes"
    assert first.retire("b1").get("refusal") == "egress-incomplete"
    assert _read(second, pin_b, key_b) == b"second producer bytes"
    _release(first, pin_a)
    assert first.retire("b1")["ok"] is True
    assert not path_a.exists()
    assert _read(second, pin_b, key_b) == b"second producer bytes"

    # Same logical batch can restage while another producer's pin remains.
    assert po.refill_window(first.q, first.inst, first.template, tier=fx.TIER)["ok"]
    again = first.ensure("b1")
    assert again["ok"] and again["state"] == "materializing"
    first.run_mover(again["mover_key"], "w-restage")
    again_pin, again_key, again_path = _pin(first, again)
    assert again_path == path_a
    assert _read(first, again_pin, again_key) == b"first producer payload"
    assert _read(second, pin_b, key_b) == b"second producer bytes"
    _release(first, again_pin)
    _release(second, pin_b)
    assert first.retire("b1")["ok"]
    assert second.retire("b1")["ok"]
    assert not path_a.exists() and not path_b.exists()


@pytest.mark.parametrize("mutation", ["foreign", "traversal", "consumer", "digest",
                                      "range", "unsealed_manifest", "corrupt_request", "missing_reference"])
def test_unbound_or_unsafe_output_namespace_refuses_before_copy(tmp_path, mutation):
    import json
    import stage_move

    world = fx._World(tmp_path)
    desc = fx._descriptors(world.template, world.inst, "p-shared", b"owned bytes",
                           digest_mode="sha256")
    fx._prewrite(world.q, world.inst, world.template, "b1", desc)
    batch = world.first_publish("b1", desc)
    mover = batch["mover_key"]
    request_path = world.cas_root / "requests" / mover[:2] / (mover + ".json")
    request = json.loads(request_path.read_text())
    args = stage_move.build_parser().parse_args(request["params"]["command"][2:])
    args.action_key = mover
    if mutation == "foreign":
        args.produced_output_namespace = "f" * 64
    elif mutation == "traversal":
        args.produced_output_namespace = "../outside"
    elif mutation == "consumer":
        args.consumer_action_key = "f" * 64
    elif mutation == "digest":
        args.manifest_sha256 = "f" * 64
    elif mutation == "range":
        args.range_end_bytes += 1
    elif mutation == "unsealed_manifest":
        args.manifest = str(tmp_path / "untrusted.json")
    elif mutation == "missing_reference":
        del request["params"]["produced_output_batch"]
        del request["action_key"]
        replacement = fx.pb.seal_action(request)
        fx.pb.PrismaBuildCAS(world.cas_root).publish_action_request(replacement)
        args.action_key = replacement["action_key"]
    else:
        request["params"]["produced_output_batch"]["batch_namespace"] = "f" * 64
        # CAS requests are immutable files; replace only this private fixture
        # to exercise corruption instead of failing on its read-only mode.
        request_path.unlink()
        request_path.write_text(json.dumps(request))
    before = sorted(str(p.relative_to(world.stage_root)) for p in world.stage_root.rglob("*"))
    with pytest.raises(SystemExit, match="produced_stage_namespace"):
        stage_move.move(args)
    assert before == sorted(str(p.relative_to(world.stage_root)) for p in world.stage_root.rglob("*"))
    assert world.q.move_record(mover) is None


def test_same_owner_batches_with_matching_basenames_are_independent(tmp_path):
    world = fx._World(tmp_path, window_gib=2, gib=4)
    batches = []
    pins = []
    for name, payload in [("p-left", b"left batch"), ("p-right", b"right batch")]:
        (Path(world.template["output_prefix"]) / name).mkdir(parents=True)
        desc = fx._descriptors(world.template, world.inst, name + "/same", payload,
                               digest_mode="sha256")
        fx._prewrite(world.q, world.inst, world.template, name, desc)
        batch = world.first_publish(name, desc)
        world.run_mover(batch["mover_key"], "w-" + name)
        pin, key, path = _pin(world, batch)
        pins.append((pin, key, path, payload))
        batches.append(name)
    assert pins[0][2] != pins[1][2]
    for pin, key, path, payload in pins:
        assert _read(world, pin, key) == payload
        _release(world, pin)
    assert world.retire(batches[0])["ok"]
    assert pins[1][2].read_bytes() == pins[1][3]
    assert world.retire(batches[1])["ok"]
