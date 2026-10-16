"""Gang records over real sealed requests and private queue publications."""
from __future__ import annotations

import base64
import json
from pathlib import Path
import runpy

GROUP = "7" * 32


def context(gang, pool, core, store):
    body = runpy.run_path(str(Path(__file__).with_name("compat_core_adapters.py")))["action_body"]
    cas = core.PrismaBuildCAS(store / "cas")
    queue = pool.PoolQueue(store / "queue")
    rows = []
    for index in range(2):
        declaration = {"group": GROUP, "size": 2, "index": index}
        action = body(core, store)
        action["params"]["gang"] = declaration
        sealed = core.seal_action(action)
        cas.publish_action_request(sealed)
        path = queue.publish(action_key=sealed["action_key"], cas_root=cas.root,
                             checkout_root="/compat/checkout", worker_script="/compat/worker.py",
                             resources={"cpu": 1, "mem_gb": 1}, max_attempts=1, retry_safe=False,
                             gang=declaration)
        rows.append(json.loads(path.read_bytes()))
    record = gang.publish_group(queue, GROUP, rows, skew_s=2.0)
    return queue, record


def operate(request, store, load):
    family, variant = request["case"].split("/", 1)
    gang, pool, core = (load("prismabuild." + name) for name in ("_gang", "pool", "core"))
    queue, record = context(gang, pool, core, store)
    first, second = record["members"]
    directory = gang.state_dir(queue, GROUP)
    if family == "gang-group":
        path = gang.group_path(queue, GROUP)
    elif family == "gang-election":
        path = directory / "elect-0.json"
    elif family == "gang-ready":
        path = directory / "ready-1.json"
    elif family == "gang-teardown":
        path = directory / "teardown.json"
    elif family == "gang-terminal":
        path = gang.terminal_mark_path(queue, GROUP, first["action_key"])
    else:
        raise ValueError(f"unknown gang adapter: {family}")
    if request["op"] == "write":
        if family == "gang-election":
            gang.elect_gang_member(queue, record, first, "compat-first", 100.0)
        elif family == "gang-ready":
            gang.mark_ready(queue, record, second, "compat-second", 100.0)
        elif family == "gang-teardown":
            gang.tear_down(queue, GROUP, reason="compat", by="compat", now=100.0)
        elif family == "gang-terminal":
            gang.note_terminal(queue, GROUP, first["action_key"], "" if variant == "empty" else "compat", now=100.0)
        return {"raw": base64.b64encode(path.read_bytes()).decode()}
    path.parent.mkdir(parents=True, exist_ok=True)
    # The group fixture is itself a production publication. Replace only the
    # private read-side copy when the tested record is that immutable body.
    if path.exists():
        path.unlink()
    path.write_bytes(base64.b64decode(request["raw"]))
    if family == "gang-group":
        result = gang.read_group(queue, GROUP)
        assert result and result["size"] == 2
    elif family == "gang-election":
        result = gang.elections(queue, GROUP, 2)
        assert result[0]["host"] == "compat-first"
    elif family == "gang-ready":
        result = gang.sibling_readiness(queue, record, first, "compat-first", 101.0)
        assert result["complete"], "start barrier refused sibling readiness"
    elif family == "gang-teardown":
        result = gang.teardown(queue, GROUP)
        assert isinstance(result, dict) and result, "teardown presence was lost"
    else:
        confirmed = gang.note_terminal(queue, GROUP, first["action_key"],
                                       "" if variant == "empty" else "compat", now=102.0, confirm_s=1)
        assert confirmed is (variant != "empty"), "terminal confirmation refused the standing signature"
        result = {"confirmed": confirmed}
    return result
