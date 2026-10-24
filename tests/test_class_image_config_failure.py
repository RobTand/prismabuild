"""An enabled but broken class config must not stop native worker admission."""
from __future__ import annotations

import json

import pytest

from prismabuild import immutable_files, pool
from test_a_draining_worker_stays_visible import _load_loop


@pytest.mark.parametrize("failure", ["missing", "malformed", "unreadable"])
def test_enabled_config_failure_preserves_native_availability(
        tmp_path, monkeypatch, failure):
    config = tmp_path / "class-images.json"
    if failure == "malformed":
        config.write_text("{not json")
    elif failure == "unreadable":
        config.write_text("{}")
        read = immutable_files.read_stable_file

        def unreadable(path, **options):
            # chmod alone is not a reliable refusal when pytest runs as root.
            if path == config:
                raise PermissionError("selected class image config is unreadable")
            return read(path, **options)

        monkeypatch.setattr(immutable_files, "read_stable_file", unreadable)

    loop = _load_loop(
        tmp_path, monkeypatch, {"draining": False, "changed_unix": 1.0},
        extra_args=("--class-image-config", config.name))
    monkeypatch.setattr(loop, "RUNTIME_ROOT", tmp_path)
    monkeypatch.setattr(loop.container_images.InventoryCache, "snapshot",
                        lambda self, **options: None)
    monkeypatch.setattr(loop.box_capacity, "worker_loops", lambda: [])
    monkeypatch.setattr(loop, "spool_retirement", lambda *a, **k: None)
    monkeypatch.setattr(loop.filesystem_floor, "loop_tick", lambda *a, **k: None)
    monkeypatch.setattr(loop.filesystem_floor, "host_admission", lambda *a, **k: True)

    # Keep real discovery reads and offer writes, but not their fork/timeout
    # wrappers. No broker, payload subprocess, Docker or live queue is needed.
    def discover(queue, **options):
        return loop.DiscoveryResult("ready", queue.ready_items(), None, "")

    def publish(announce, **options):
        announce()
        return loop.PublicationResult("published", 0.0, None, "")

    monkeypatch.setattr(loop, "discover_ready_snapshot", discover)
    monkeypatch.setattr(loop, "publish_offer", publish)
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    key = "a" * 64
    queue.publish(action_key=key, cas_root=tmp_path / "cas",
                  checkout_root=tmp_path, worker_script="not-executed.py",
                  resources={"cpu": 1, "mem_gb": 1})
    seen = []

    def serve_once(self, **options):
        policy = options.get("container_class_policy")
        claimed = self.claim(
            tags=options["tags"], has_gpu=options["has_gpu"],
            capacity=options["capacity"], ready=options["ready"],
            container_class_policy=policy,
            container_inventory=options.get("container_inventory"),
            admission_open=options["admission_open"])
        seen.append((policy, claimed))
        # The acceptance boundary is admission, not executing a fake payload.
        return {"action_key": key, "status": "executed", "returncode": 0}

    monkeypatch.setattr(pool.PoolQueue, "serve_once", serve_once)

    # Do not catch the current startup file/JSON exception: it is precisely
    # the availability failure this acceptance test is intended to expose.
    assert loop.main() == 0
    assert len(seen) == 1, "a broken configured policy must not stop native admission"
    policy, claimed = seen[0]
    assert claimed is not None and claimed["action_key"] == key
    assert policy is not None, "configured failure must not downgrade to legacy mode"
    offers = list((queue.root / pool.record_store.WORKERS).glob("*.json"))
    assert len(offers) == 1
    verdict = json.loads(offers[0].read_text())["container_class_verdict"]
    assert verdict["status"] == "unknown"
    assert verdict["container_work_eligible"] is False
    assert verdict["reason"], "the worker offer must explain the class refusal"
