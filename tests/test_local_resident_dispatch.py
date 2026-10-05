import json
from pathlib import Path

from prismabuild import pool
from test_resident_sets_records import source


def test_publish_dispatches_host_movers_after_record_publication(tmp_path, monkeypatch, capsys):
    import pbresident
    root, manifest = source(tmp_path)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.mint_tier_capacity("local:test-host", {"local_gib": 1})
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    observed = []
    def submit(store, set_id, **kwargs):
        observed.append(store.read(set_id))
        return {"test-host": {"copy": {"action_key": "a" * 64}}}
    monkeypatch.setattr(pbresident, "submit_copies", submit, raising=False)
    assert pbresident.main(["--pool-root", str(queue.root), "publish", "--manifest", str(path),
        "--canonical-root", str(root), "--hosts", "test-host", "--lease-until", "2099-01-01T00:00:00Z",
        "--hard-max", "2099-01-02T00:00:00Z"]) == 0
    assert len(observed) == 1
    assert "movements" in json.loads(capsys.readouterr().out)


def test_local_role_announces_own_interpreter_and_tools(tmp_path):
    import local_tier_loop
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    root = tmp_path / "local"
    spec = {"root": str(root), "maximum_gib": 0, "floor_fraction": .05, "docker_allowance_gib": 0}
    local_tier_loop.cycle(queue, "test-host", {"hosts": {"test-host": spec}})
    tier = next(row for row in queue.tiers() if row["tier_id"] == "local:test-host")
    assert tier["host"] == "test-host"
    assert Path(tier["mover_python"]).is_absolute()
    assert Path(tier["mover_tools_root"]).is_absolute()


def test_published_pairs_queue_only_copy_and_retain_egress_for_owner(tmp_path, monkeypatch):
    from prismabuild import local_resident
    from test_resident_sets_records import publish
    import test_a_stage_mover_declares_the_cpu_and_retries_it_owns as fixture
    store, record = publish(tmp_path)
    class Cas:
        root = tmp_path / "cas"
        def publish_action_request(self, action):
            pass
    template = fixture._template("c" * 64, 7)
    template["cas"] = Cas()
    tier = {"tier_id": "local:test-host", "host": "test-host", "mountpoint": str(tmp_path / "local"),
        "mover_python": "/usr/bin/python3", "mover_tools_root": "/generation/tools"}
    rows = local_resident.publish_actions(template, store, record["set_id"], [tier], policy_path="/policy.json")
    queue = pool.PoolQueue(store.queue_root)
    copy = rows["test-host"]["copy"]
    assert queue.item_path(pool.READY, copy["action_key"]).exists()
    assert not queue.item_path(pool.READY, rows["test-host"]["evict"]["action_key"]).exists()
    assert store.read_copy(record["set_id"], "test-host")["movement_rows"] == rows["test-host"]
