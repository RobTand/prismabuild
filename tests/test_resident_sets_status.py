import argparse
import json

from prismabuild import pool
from admitted_queue_fixture import AdmittedQueueFixture
from test_resident_sets_records import publish


def test_status_shows_sets_copies_leases_and_dynamic_capacity(tmp_path, capsys):
    import pbstatus
    from prismabuild import resident_sets
    store, record = publish(tmp_path)
    resident_sets.write_record(store.queue_root / "resident-capacity" / "test-host.json",
                              {"host": "test-host", "capacity": {"local_gib": 42}, "available_bytes": 123})
    view = pbstatus.read_resident_sets(store.queue_root, now=120)
    assert view["complete"]
    assert view["sets"][0]["set_id"] == record["set_id"]
    assert view["sets"][0]["copies"]["test-host"]["state"] == "absent"
    assert view["sets"][0]["lease_active"]
    assert view["capacity"][0]["capacity"]["local_gib"] == 42
    assert pbstatus.main(["--queue-root", str(store.queue_root), "--resident-sets"]) == 0
    assert json.loads(capsys.readouterr().out)["sets"][0]["set_id"] == record["set_id"]


def test_unreadable_resident_record_is_partial_not_empty(tmp_path):
    import pbstatus
    store, record = publish(tmp_path)
    store.copy_path(record["set_id"], "test-host").write_text("broken")
    view = pbstatus.read_resident_sets(store.queue_root)
    assert not view["complete"]
    assert view["unreadable"]


def test_new_claim_and_immutable_attempt_record_canonical_serving(tmp_path):
    import pbstatus
    queue = AdmittedQueueFixture(pool.PoolQueue(tmp_path / "queue"), capacity={"cpu": 1, "mem_gb": 2},
                                 default_demand={"cpu": 1, "mem_gb": 1})
    queue.ensure_layout()
    key = "a" * 64
    queue.publish(action_key=key, cas_root=str(tmp_path / "cas"), checkout_root=str(tmp_path),
                  worker_script="/worker.py", max_attempts=1)
    claimed = queue.claim()
    assert claimed["served_from"] == "canonical"
    path = queue.finish(key, status="executed")
    terminal = json.loads(path.read_text())
    assert queue.attempt_outcomes(terminal)[0]["served_from"] == "canonical"
    assert pbstatus.read_endings(queue.root)[0]["served_from"] == "canonical"


def test_explicit_resident_declaration_is_sealed_and_projected_not_a_gate(tmp_path):
    import pbrun
    import test_a_stage_mover_declares_the_cpu_and_retries_it_owns as fixture
    set_id = "b" * 64
    args = pbrun.parse_args(["--resident-set", set_id, "--", "/usr/bin/true"])
    assert args.resident_set == set_id
    template = fixture._template("c" * 64, 7)
    template["params"]["resident_set"] = set_id
    action = pbrun.seal_action_from_template(template)
    options = argparse.Namespace(priority=0, max_attempts=1, retry_safe=False)
    queue = pool.PoolQueue(tmp_path / "queue")
    row = pbrun.publication_row(action, args=options, queue=queue)
    assert row["resident_set"] == set_id
    queue.publish(**row)
    # An absent set cannot turn the optional accelerator into admission gating.
    item = json.loads(queue.item_path(pool.READY, action["action_key"]).read_text())
    assert item["resident_set"] == set_id
    assert item.get("residency") is None
