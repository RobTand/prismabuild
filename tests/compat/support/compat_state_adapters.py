"""Private ownership and merge-queue records through their actual consumers."""
from __future__ import annotations

import base64
from pathlib import Path
import runpy


def mover_store(pool, store, variant):
    queue = pool.PoolQueue(store / "queue")
    queue.ensure_layout()
    baseline = queue.begin_mover_retirement_batch()
    if variant != "bootstrap":
        key = "c" * 64
        queue.record_move(key, {"status": "executed", "bytes": 8, "elapsed_s": 1.0})
        version = queue._receipt_version(queue.move_path(key))
        entry = queue.prepare_move_retirement(key, expected_version=version)
        assert isinstance(entry, dict), entry
        keys, identity, refusal = queue.commit_move_retirements([entry], expected_baseline=baseline)
        assert keys == [key] and identity is not None and not refusal
        if variant in ("replay", "recovered"):
            if variant == "recovered":
                queue.mover_retirement_state_path().unlink()
            assert queue.commit_move_retirements([entry], expected_baseline=identity) == ([key], identity, "")
    return queue


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    write = request["op"] == "write"
    if family in ("state-mover-checkpoint", "state-mover-state"):
        pool = load("prismabuild.pool")
        if write or family == "state-mover-state":
            queue = mover_store(pool, store, variant)
        else:
            queue = pool.PoolQueue(store / "queue")
            queue.ensure_layout()
        path = (queue.mover_retirement_checkpoint_path() if family.endswith("checkpoint")
                else queue.mover_retirement_state_path())
        if not write:
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                path.chmod(0o600)
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            value = queue.read_mover_retirements()
            assert len(value) == (0 if variant == "bootstrap" else 1)
            return value
    elif family == "state-canary-slot":
        core, canary = load("prismabuild.core"), load("prismabuild.publication_canary")
        factory = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["action_body"]
        body = factory(core, store)
        value = {"generation": "a" * 12 + "-100-" + "b" * 12, "host": "compat-host"}
        body["params"].update(publication_canary=value, execution_timeout_s=canary.EXECUTION_TIMEOUT_S, gpu_exclusive=True)
        action = core.seal_action(body)
        path = canary.slot_path(store, value)
        if write:
            assert canary.mint(store, action, run_id="compat-private") == path
            if variant == "bound":
                canary.bind(store, value, action["action_key"], 100.0)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = canary.load(store, value, action["action_key"])
            assert result["published_unix"] == (100.0 if variant == "bound" else None)
            return result
    elif family in ("state-reclaim-manifest", "state-reclaim-memo"):
        reclaim = load("stage_reclaim")
        directory = store / "compat-run"
        directory.mkdir()
        path = directory / (reclaim.MANIFEST_NAME if family.endswith("manifest") else reclaim.MEMO_NAME)
        if write:
            entries = [] if variant == "empty" else [{"stage_rel": "weights", "sha256": "a" * 64,
                "stage_identity": {"dev": 1, "ino": 2}, "original_identity": {"dev": 3, "ino": 4},
                "original_path": "/compat/weights", "offset": 0, "size": 8, "restore_command": "recorded-fixture"}]
            if family.endswith("manifest"):
                assert reclaim.write_manifest(directory, entries) == path
            else:
                assert reclaim.write_memo_file(path, entries) == path
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = reclaim.read_manifest(directory) if family.endswith("manifest") else reclaim.read_memo(path)
            entries = result["entries"] if family.endswith("manifest") else result
            assert len(entries) == (0 if variant == "empty" else 1)
            return result
    elif family == "state-stage-root":
        release, pool = load("stage_release"), load("prismabuild.pool")
        queue = pool.PoolQueue(store / "queue")
        stage = store / "stage"
        stage.mkdir()
        path = stage / release.STAGE_ROOT_MARKER
        if write:
            assert release.register_stage_root(queue, tier_id="prismabuild-stage:compat", stage_root=stage) == "registered"
            if variant == "repeated":
                before = path.read_bytes()
                assert release.register_stage_root(queue, tier_id="prismabuild-stage:compat", stage_root=stage) == "registered"
                assert path.read_bytes() == before
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            refusal = release.stage_root_refusal(queue, stage)
            assert refusal is None, refusal
            return {"owned": True}
    else:
        merge = load("pbmergeq")
        state = merge.Store(store / "mergeq")
        identity = "a" * 64
        path = state.baselines / ("known-failure-" + identity + ".json")
        if write:
            value = {"new": ["tests/compat_example.py::test_failure"], "files": ["tests/compat_example.py"]}
            if variant == "details":
                value.update(tree="b" * 40, base_tree="c" * 40, reason="compat caf\u00e9")
            state.remember_known_failure(identity, value)
        else:
            path.write_bytes(base64.b64decode(request["raw"]))
            request["_consumer_boundary"]()
            result = state.known_failure(identity)
            assert result and result["new"] == ["tests/compat_example.py::test_failure"]
            return result
    return {"raw": base64.b64encode(path.read_bytes()).decode()}
