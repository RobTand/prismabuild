"""#2109: native admission is not authority for a later Docker creation.

Exercise PoolQueue's real subprocess launch and the real Docker shim main.
Only the synthetic worker payload, inventory observation and daemon are doubles;
no future launch-context format is assumed.  In particular, policy enters at
serve_once, never as an action-supplied environment variable or verdict.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import time

import pytest

from prismabuild import container_images as ci, pool


ROOT = Path(__file__).resolve().parents[1]
SHIM = ROOT / "tools" / "fleet" / "docker"
KEY = "a" * 64
OWNER = "b" * 64
CONTENT = "content:sha256:" + "c" * 64
OTHER = "content:sha256:" + "d" * 64
CAPACITY = {"cpu": 1, "mem_gb": 1}
ALIASES = [
    ("run",), ("create",), ("container", "run"), ("container", "create"),
    ("exec",), ("container", "exec"),
]
# These are existing ClassImagePolicy reasons, not a proposed wire protocol.
REFUSAL_REASONS = {
    "content": "image_content_mismatch",
    "missing_image": "required_image_missing",
    # The existing evaluator deliberately classifies stale evidence as unknown.
    "expired": "inventory_unknown",
    "store": "image_store_mismatch",
    "unknown": "inventory_unknown",
    "endpoint": "endpoint",
    "endpoint_host": "endpoint",
}


# Record *every* CLI call, separately from the fake daemon's creation state.
# Context inspection is allowed before refusal; run/create must not arrive.
FAKE_DOCKER = r'''
import json
from pathlib import Path
import sys

settings = json.loads(Path(SETTINGS).read_text())
args = sys.argv[1:]
with Path(settings["calls"]).open("a") as stream:
    stream.write(json.dumps(args) + "\n")
sub = args[:]
while sub and sub[0] in ("--host", "--context"):
    sub = sub[2:]
if sub[:2] == ["context", "inspect"]:
    print(json.dumps(settings["endpoint"]))
    raise SystemExit(0)
if sub[:1] == ["inspect"]:
    print(json.dumps([{
        "Id": "e" * 64,
        "Config": {"Labels": {"prismabuild.action": settings["owner"],
                               "prismabuild.scope": settings["scope"]}},
        "HostConfig": {"CgroupParent": settings["scope"]},
        "State": {"Running": True},
    }]))
    raise SystemExit(0)
if sub[:1] == ["container"]:
    sub = sub[1:]
if sub[:1] in (["run"], ["create"]):
    state = Path(settings["state"])
    ids = json.loads(state.read_text())
    cid = format(len(ids) + 1, "064x")
    ids.append(cid)
    state.write_text(json.dumps(ids))
    if "--cidfile" in sub:
        Path(sub[sub.index("--cidfile") + 1]).write_text(cid + "\n")
    print(cid)
'''


# Like the small worker scripts in test_pool.py, this avoids historical CAS/git
# replay.  Keep the real executor environment and real shim implementation.
# The inventory double returns observations, not a precomputed success verdict.
WORKER = r'''
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import time

settings_path = Path(SETTINGS)
settings = json.loads(settings_path.read_text())
sys.path.insert(0, settings["source"])
from prismabuild import container_images as ci

observation = settings["inventory"]
ci.InventoryCache.snapshot = lambda self, **kwargs: observation
os.environ["PRISMABUILD_DOCKER_TESTING"] = "1"
os.environ["PRISMABUILD_DOCKER_REAL"] = settings["docker"]
os.environ["PRISMABUILD_CGROUP_FILE"] = settings["cgroup"]
os.environ["PRISMABUILD_CGROUP_ROOT"] = settings["cgroup_root"]
os.environ["PRISMABUILD_CONTAINER_OWNER"] = settings["owner"]
os.environ["PRISMABUILD_CONTAINER_MARKER"] = settings["marker"]
# The tested policy configuration must come from the worker/executor, not this
# payload.  These are only the existing ownership inputs a sealed task gets.
if sys.argv[1:2] == ["--shim"]:
    args = json.loads(sys.argv[2])
    sys.argv = [settings["shim"], *args]
    raise SystemExit(runpy.run_path(settings["shim"])["main"]())
results = {}

def invoke(name, args):
    # Inventory and owned exec forward with execv. A child also makes every
    # launch reload the real shim, rather than retaining module-local state.
    settings["inventory"] = observation
    settings_path.write_text(json.dumps(settings))
    completed = subprocess.run(
        [sys.executable, __file__, "--shim", json.dumps(args)],
        capture_output=True, text=True, timeout=15,
    )
    results[name] = {"returncode": completed.returncode, "stderr": completed.stderr}
    sys.stderr.write(completed.stderr)

def command(image):
    if settings["alias"][-1] == "exec":
        return [*settings["alias"], "owned-target", "true"]
    return [*settings["alias"], image]

# Native processes and inventory are usable without a class launch proof.
subprocess.run([sys.executable, "-c", "pass"], check=True, timeout=15)
invoke("inventory_before", ["image", "ls", "--quiet"])
assert not Path(settings["marker"]).exists()
if settings["change"] == "missing_context":
    invoke("refused", command("undeclared:test"))
else:
    # This positive control must reach Docker with ownership/cpuset labels.
    observation["observed_unix"] = time.time()
    invoke("authorized", command("pb-canary:test"))
    change = settings["change"]
    if change == "content":
        observation["image_contents"]["pb-canary:test"] = settings["other"]
        # Both digests already exist in entries: this is a valid changed tag
        # projection, not a malformed/unknown inventory record.
    elif change == "missing_image":
        observation["image_contents"].pop("pb-canary:test")
    elif change == "expired":
        observation["observed_unix"] = time.time() - max(ci.CLAIM_FRESHNESS_S, ci.INVENTORY_TTL_S) - 1
    elif change == "store":
        observation["store_root"] += "-different"
    elif change == "unknown":
        # Match InventoryCache.snapshot's actual unknown-inventory shape.
        observation["entries"] = None
    elif change in {"endpoint", "endpoint_host"}:
        settings["endpoint"] = settings["other_endpoint"]
    prefix = []
    if change == "endpoint":
        prefix = ["--context", "other-local"]
    elif change == "endpoint_host":
        prefix = ["--host", settings["other_endpoint"]]
    # No image was declared on the queue item, and this image isn't a class
    # requirement either. Neither omission may bypass the class launch gate.
    # For exec the fake inspect resolves owned-target to an owned immutable ID,
    # so a refusal cannot be explained by target ownership.
    invoke("refused", [*prefix, *command("undeclared:test")])
# An ineligible class must not poison later native work or local inventory.
invoke("inventory_after", ["image", "ls", "--quiet"])
subprocess.run([sys.executable, "-c", "pass"], check=True, timeout=15)
Path(settings["results"]).write_text(json.dumps(results))
'''


def _exercise(tmp_path, monkeypatch, *, alias, change):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ledger().ensure_capacity(CAPACITY)
    marker = queue.container_marker(OWNER)
    settings_path = tmp_path / "settings.json"
    state_path = tmp_path / "containers.json"
    state_path.write_text("[]")
    cgroup = tmp_path / "cgroup"
    cgroup_root = tmp_path / "cgroup-root"
    scope = "prismabuild-job" + "1" * 32 + ".slice"
    if alias[-1] == "exec":
        (cgroup_root / scope).mkdir(parents=True)
        (cgroup_root / scope / "memory.max").write_text(str(1024 ** 3))
        cgroup.write_text(f"0::/{scope}\n")
    else:
        cgroup.write_text("0::/\n")
    inventory = {
        "schema": ci.INVENTORY_SCHEMA,
        "observed_unix": time.time(),
        "entries": [CONTENT, OTHER],
        "store_root": str(tmp_path / "image-store"),
        "image_contents": {"pb-canary:test": CONTENT},
    }
    policy = ci.ClassImagePolicy({
        "schema": ci.CLASS_REQUIREMENTS_SCHEMA,
        "classes": {"test-class": {
            "store_root": inventory["store_root"],
            "images": {"pb-canary:test": CONTENT},
        }},
    }, klass="test-class")
    settings = {
        "source": str(ROOT / "src"), "shim": str(SHIM),
        "docker": str(tmp_path / "docker"), "owner": OWNER,
        "marker": str(marker), "inventory": inventory, "other": OTHER,
        "calls": str(tmp_path / "calls.jsonl"), "state": str(state_path),
        "cgroup": str(cgroup), "cgroup_root": str(cgroup_root), "scope": scope,
        "results": str(tmp_path / "results.json"), "alias": list(alias),
        "change": change, "endpoint": ci.LOCAL_ENDPOINT,
        "other_endpoint": "unix://" + str(tmp_path / "other-daemon.sock"),
    }
    settings_path.write_text(json.dumps(settings))
    docker = Path(settings["docker"])
    docker.write_text(f"#!{sys.executable}\nSETTINGS = {str(settings_path)!r}\n" + FAKE_DOCKER)
    docker.chmod(0o755)
    worker = tmp_path / "worker.py"
    worker.write_text(f"SETTINGS = {str(settings_path)!r}\n" + WORKER)

    # No real daemon or host-local inventory/cache may be consulted by the
    # parent.  The subprocess installs the same observation seam above.
    monkeypatch.setattr(ci.InventoryCache, "snapshot", lambda self, **kwargs: inventory)
    queried, removed = [], []

    def owned_ids(owner):
        queried.append(owner)
        return json.loads(state_path.read_text())

    def remove(ids):
        removed.append(list(ids))
        state_path.write_text("[]")
        return list(ids)

    monkeypatch.setattr(pool, "_docker_owned_container_ids", owned_ids)
    monkeypatch.setattr(pool, "_docker_remove_containers", remove)
    ready_path = queue.publish(
        action_key=KEY, cas_root=tmp_path / "cas", worker_script=worker,
        checkout_root=tmp_path, resources=CAPACITY, container_owner=OWNER,
        # Deliberately no container_images: native admission must persist.
    )
    # The existing evidence field is writable queue metadata, not authority.
    # Even a positive, correctly shaped verdict must not authorize launch two.
    row = json.loads(ready_path.read_text())
    row["container_class_verdict"] = policy.evaluate(inventory, now=time.time())
    assert row["container_class_verdict"]["status"] == "satisfied"
    ready_path.write_text(json.dumps(row))
    outcome = queue.serve_once(
        capacity=CAPACITY,
        container_class_policy=None if change == "missing_context" else policy,
        container_inventory=inventory,
    )
    assert outcome is not None, "native/undeclared-image work must still be admitted"
    assert outcome["status"] == "executed" and outcome["returncode"] == 0, outcome
    assert queue.ledger().available() == CAPACITY
    assert not marker.exists(), "cleanup must settle the existing ownership transaction"
    assert json.loads(state_path.read_text()) == []
    calls_path = Path(settings["calls"])
    calls = [json.loads(line) for line in calls_path.read_text().splitlines()]
    work_requests = []
    for args in calls:
        sub = args[:]
        while sub and sub[0] in ("--host", "--context"):
            sub = sub[2:]
        if sub[:1] == ["container"]:
            sub = sub[1:]
        if sub[:1] in (["run"], ["create"], ["exec"]):
            work_requests.append(args)
    return json.loads(Path(settings["results"]).read_text()), work_requests, queried, removed


@pytest.mark.parametrize("change,alias", [
    pytest.param(change, alias, id=change + "-" + "-".join(alias))
    for change in REFUSAL_REASONS for alias in ALIASES
    # Owned exec already refuses a foreign endpoint. Existing shim regressions
    # retain that coverage; new acceptance checks must expose missing behavior.
    if not (change in {"endpoint", "endpoint_host"} and alias[-1] == "exec")
])
def test_owned_launch_rechecks_class_at_each_actual_launch(tmp_path, monkeypatch, alias, change):
    attempts, work_requests, queried, removed = _exercise(
        tmp_path, monkeypatch, alias=alias, change=change)
    assert attempts["inventory_before"]["returncode"] == 0, attempts
    assert attempts["authorized"]["returncode"] == 0, attempts
    assert attempts["refused"]["returncode"] == 125, attempts
    diagnostic = attempts["refused"]["stderr"].lower()
    if change in {"endpoint", "endpoint_host"}:
        # Owned exec already has a stricter system-daemon check. Preserve that
        # valid refusal as well as a class-policy endpoint-binding refusal.
        assert "endpoint" in diagnostic or "local system docker daemon" in diagnostic, attempts
    else:
        assert REFUSAL_REASONS[change] in diagnostic, attempts
    assert attempts["inventory_after"]["returncode"] == 0, attempts
    assert len(work_requests) == 1, "refusal must precede the fake daemon's second work request"
    first = work_requests[0]
    assert first[:2] == ["--host", ci.LOCAL_ENDPOINT]
    if alias[-1] == "exec":
        assert first[-2:] == ["e" * 64, "true"], "exec must retain verified immutable target binding"
        assert queried == removed == []
    else:
        assert f"prismabuild.action={OWNER}" in first
        assert "--cpuset-cpus" in first
        assert queried and set(queried) == {OWNER}
        assert removed == [[format(1, "064x")]], "the earlier authorized container still needs cleanup"


@pytest.mark.parametrize("alias", ALIASES, ids=lambda alias: "-".join(alias))
def test_missing_worker_class_context_refuses_work_not_native_inventory(tmp_path, monkeypatch, alias):
    attempts, work_requests, queried, removed = _exercise(
        tmp_path, monkeypatch, alias=alias, change="missing_context")
    assert attempts["inventory_before"]["returncode"] == 0, attempts
    assert attempts["refused"]["returncode"] == 125, attempts
    # The project has no launch-context wire format yet. Require an intelligible
    # class/context diagnostic without inventing its future environment/API.
    diagnostic = attempts["refused"]["stderr"].lower()
    assert "class" in diagnostic and any(
        word in diagnostic for word in ("context", "missing", "unavailable", "unknown")
    ), attempts
    assert attempts["inventory_after"]["returncode"] == 0, attempts
    assert work_requests == [], "a supplied satisfied verdict cannot authorize a daemon request"
    assert queried == removed == [], "refusal must not open an ownership transaction"
