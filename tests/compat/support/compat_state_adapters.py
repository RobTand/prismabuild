"""Private ownership and merge-queue records through their actual consumers."""
from __future__ import annotations

import base64


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    write = request["op"] == "write"
    if family == "state-stage-root":
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
            result = state.known_failure(identity)
            assert result and result["new"] == ["tests/compat_example.py::test_failure"]
            return result
    return {"raw": base64.b64encode(path.read_bytes()).decode()}
