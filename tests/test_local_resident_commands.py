"""Retry failed publication dispatch without rewriting the immutable set."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess

import pytest

from prismabuild import core, local_resident, pool, resident_sets
from test_local_resident_mover import world
from test_resident_sets_records import source

from local_resident_space import roomy_disk  # noqa: F401

pytestmark = pytest.mark.usefixtures("roomy_disk")


def _dispatch_fixture(tmp_path, monkeypatch):
    import pbresident
    import test_a_stage_mover_declares_the_cpu_and_retries_it_owns as fixture
    store, record, spec = world(tmp_path)
    monkeypatch.setattr(local_resident.time, "time", lambda: 120)
    queue = pool.PoolQueue(store.queue_root)
    tier = {"tier_id": "local:test-host", "host": "test-host", "mountpoint": spec["root"],
            "mover_python": "/usr/bin/python3", "mover_tools_root": "/generation/tools"}
    queue.announce_tier(tier)
    template = fixture._template("c" * 64, 7)
    template["cas"] = core.PrismaBuildCAS(tmp_path / "cas")
    monkeypatch.setattr(pbresident, "movement_template", lambda *args: template)
    return store, record, spec, queue, tier, template


def test_failed_publication_dispatch_can_retry_without_republishing(tmp_path, monkeypatch, capsys):
    import pbresident
    canonical, manifest = source(tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.mint_tier_capacity("local:test-host", {"local_gib": 1})
    calls = []

    def dispatch(store, set_id, **kwargs):
        calls.append(set_id)
        if len(calls) == 1:
            raise ValueError("simulated dispatch failure")
        return {"test-host": {"copy": {"action_key": "a" * 64}}}

    monkeypatch.setattr(pbresident, "submit_copies", dispatch)
    common = ["--pool-root", str(queue.root)]
    assert pbresident.main(common + ["publish", "--manifest", str(manifest_path),
        "--canonical-root", str(canonical), "--hosts", "test-host", "--lease-until", "2099-01-01T00:00:00Z",
        "--hard-max", "2099-01-02T00:00:00Z"]) == 2
    assert "simulated dispatch failure" in capsys.readouterr().err
    store = resident_sets.ResidentSets(queue.root)
    record = store.status()[0]
    set_id = record["set_id"]
    body = store.set_path(set_id).read_bytes()
    assert pbresident.main(common + ["dispatch", set_id]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["movements"]["test-host"]["copy"]["action_key"] == "a" * 64
    assert calls == [set_id, set_id]
    assert store.set_path(set_id).read_bytes() == body
    assert len(store.read_lease_log(set_id)) == 1
    assert queue.tier_ledger("local:test-host").holder_tokens(set_id)["local_gib"] == 1


def test_repeated_movement_dispatch_attaches_to_the_live_generation(tmp_path, monkeypatch):
    store, record, spec, queue, tier, template = _dispatch_fixture(tmp_path, monkeypatch)
    first = local_resident.publish_actions(template, store, record["set_id"], [tier], policy_path="/policy.json")
    key = first["test-host"]["copy"]["action_key"]
    path = queue.item_path(pool.READY, key)
    before = json.loads(path.read_text())
    second = local_resident.publish_actions(template, store, record["set_id"], [tier], policy_path="/policy.json")
    assert second == first
    assert json.loads(path.read_text())["published_unix"] == before["published_unix"]
    assert len(list(queue.root.joinpath(pool.READY).glob("*.json"))) == 1


@pytest.mark.parametrize("entrypoint", ["command", "publisher"])
@pytest.mark.parametrize("expiry", ["released", "hard_max", "until"])
def test_dispatch_refuses_inactive_lease_without_publication(tmp_path, monkeypatch, capsys, entrypoint, expiry):
    import pbresident
    store, record, spec, queue, tier, template = _dispatch_fixture(tmp_path, monkeypatch)
    set_id = record["set_id"]
    first = local_resident.publish_actions(template, store, set_id, [tier], policy_path="/policy.json")
    movements = store.set_path(set_id).parent / "movements" / "test-host.json"
    before = movements.read_bytes()
    requests = set(template["cas"].root.rglob("*.json"))
    ready = {path: path.read_bytes() for path in (queue.root / pool.READY).glob("*.json")}
    if expiry == "released":
        store.release(set_id, by="test", now=120)
        now = 121
    else:
        now = 200 if expiry == "hard_max" else 150
    if expiry != "until":
        (queue.root / pool.READY / ("f" * 64 + ".json")).write_text(json.dumps({"resident_set": set_id}))
        ready = {path: path.read_bytes() for path in (queue.root / pool.READY).glob("*.json")}
    monkeypatch.setattr(local_resident.time, "time", lambda: now)
    assert not local_resident.lease_active(store, set_id)
    if entrypoint == "command":
        assert pbresident.main(["--pool-root", str(queue.root), "dispatch", set_id,
                               "--checkout", str(tmp_path), "--policy", "/changed-policy.json"]) == 2
        assert "resident lease expired" in capsys.readouterr().err
    else:
        with pytest.raises(ValueError, match="resident lease expired"):
            local_resident.publish_actions(template, store, set_id, [tier], policy_path="/changed-policy.json")
    assert movements.read_bytes() == before
    assert store.read_movements(set_id, "test-host") == first["test-host"]
    assert set(template["cas"].root.rglob("*.json")) == requests
    assert {path: path.read_bytes() for path in (queue.root / pool.READY).glob("*.json")} == ready

@pytest.mark.parametrize("state", [pool.READY, pool.CLAIMED])
@pytest.mark.parametrize("resident_host", [False, True])
def test_dispatch_honors_live_reference_before_hard_max(tmp_path, monkeypatch, capsys, state, resident_host):
    import pbresident
    store, record, spec, queue, tier, template = _dispatch_fixture(tmp_path, monkeypatch)
    set_id = record["set_id"]
    if resident_host:
        local_resident.copy(store, set_id, "test-host", spec, now=120)
    reference = queue.item_path(state, "f" * 64)
    reference.parent.mkdir(exist_ok=True)
    reference.write_text(json.dumps({"resident_set": set_id}))
    body = store.set_path(set_id).read_bytes()
    lease_log = store.read_lease_log(set_id)
    copy_state = store.copy_path(set_id, "test-host").read_bytes()
    monkeypatch.setattr(local_resident.time, "time", lambda: 151)
    assert pbresident.main(["--pool-root", str(queue.root), "dispatch", set_id,
                           "--checkout", str(tmp_path), "--policy", "/policy.json"]) == 0
    pair = json.loads(capsys.readouterr().out)["movements"]["test-host"]
    assert queue.item_path(pool.READY, pair["copy"]["action_key"]).exists() is not resident_host
    assert not queue.item_path(pool.READY, pair["evict"]["action_key"]).exists()
    assert store.read_movements(set_id, "test-host") == pair
    assert store.set_path(set_id).read_bytes() == body
    assert store.read_lease_log(set_id) == lease_log
    assert store.copy_path(set_id, "test-host").read_bytes() == copy_state


def test_resident_dispatch_publishes_descriptors_without_another_copy(tmp_path, monkeypatch):
    import pbresident
    store, record, spec, queue, tier, template = _dispatch_fixture(tmp_path, monkeypatch)
    local_resident.copy(store, record["set_id"], "test-host", spec, now=120)
    before = store.copy_path(record["set_id"], "test-host").read_bytes()
    pbresident.submit_copies(store, record["set_id"], policy_path="/policy.json", checkout=str(tmp_path))
    assert not list(queue.root.joinpath(pool.READY).glob("*.json")), "resident dispatch must not queue another copy"
    assert store.copy_path(record["set_id"], "test-host").read_bytes() == before
    assert set(store.read_movements(record["set_id"], "test-host")) == {"copy", "evict"}


def test_dispatch_command_is_idempotent_for_an_active_copy(tmp_path, monkeypatch, capsys):
    import pbresident
    store, record, spec, queue, tier, template = _dispatch_fixture(tmp_path, monkeypatch)
    options = ["--pool-root", str(queue.root), "dispatch", record["set_id"],
               "--checkout", str(tmp_path), "--policy", "/policy.json"]
    assert pbresident.main(options) == 0
    first = json.loads(capsys.readouterr().out)
    key = first["movements"]["test-host"]["copy"]["action_key"]
    generation = json.loads(queue.item_path(pool.READY, key).read_text())["published_unix"]
    assert pbresident.main(options) == 0
    assert json.loads(capsys.readouterr().out)["movements"] == first["movements"]
    assert json.loads(queue.item_path(pool.READY, key).read_text())["published_unix"] == generation


@pytest.mark.parametrize("beyond_ceiling", [False, True])
def test_renew_command_uses_the_policy_and_keeps_the_body_immutable(tmp_path, capsys, beyond_ceiling):
    import pbresident
    store, record, spec = world(tmp_path)
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"schema": resident_sets.POLICY_SCHEMA, "hosts": {}, "renewal_ceiling_s": 3 * 86400}))
    now = datetime.now(timezone.utc)
    until = (now + timedelta(days=1)).isoformat()
    maximum = (now + timedelta(days=4 if beyond_ceiling else 2)).isoformat()
    before = store.set_path(record["set_id"]).read_bytes()
    result = pbresident.main(["--pool-root", str(store.queue_root), "renew", record["set_id"],
        "--lease-until", until, "--hard-max", maximum, "--policy", str(policy), "--by", "test"])
    assert result == (2 if beyond_ceiling else 0)
    assert store.set_path(record["set_id"]).read_bytes() == before
    log = store.read_lease_log(record["set_id"])
    if beyond_ceiling:
        assert len(log) == 1 and "ceiling" in capsys.readouterr().err
    else:
        assert log[-1]["event"] == "renewed" and log[-1]["by"] == "test"
        assert log[-1]["renewal_ceiling_s"] == 3 * 86400


def test_dispatch_builds_a_real_snapshot_and_publishes_verified_requests(tmp_path, monkeypatch, capsys):
    import pbresident
    import pbrun
    store, record, spec = world(tmp_path)
    monkeypatch.setattr(local_resident.time, "time", lambda: 120)
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "source.txt").write_text("resident dispatch fixture\n")
    for arguments in (["init", "-q"], ["add", "source.txt"],
                      ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-q", "-m", "fixture"]):
        subprocess.run(["git", "-C", str(checkout), *arguments], check=True, capture_output=True)
    shared = tmp_path / "shared"
    monkeypatch.setattr(pbrun, "SH", shared)
    queue = pool.PoolQueue(store.queue_root)
    queue.announce_tier({"tier_id": "local:test-host", "host": "test-host", "mountpoint": spec["root"],
        "mover_python": "/usr/bin/python3", "mover_tools_root": "/generation/tools"})
    assert pbresident.main(["--pool-root", str(queue.root), "dispatch", record["set_id"],
                           "--checkout", str(checkout), "--policy", "/policy.json"]) == 0
    rows = json.loads(capsys.readouterr().out)["movements"]["test-host"]
    cas = core.PrismaBuildCAS(shared / "cas")
    for row in rows.values():
        request = json.loads((cas.root / "requests" / row["action_key"][:2] / (row["action_key"] + ".json")).read_text())
        core.validate_action(request)
        snapshot = request["params"]["checkout_snapshot"]
        assert cas.input_path(snapshot["input"]).is_file()
        assert request["params"]["placement"]["required_tags"] == ["test-host"]
    assert len(list(queue.root.joinpath(pool.READY).glob("*.json"))) == 1
    key = rows["copy"]["action_key"]
    generation = queue.item_path(pool.READY, key).read_bytes()
    options = ["--pool-root", str(queue.root), "dispatch", record["set_id"],
               "--checkout", str(checkout), "--policy", "/policy.json"]
    assert pbresident.main(options) == 0
    assert json.loads(capsys.readouterr().out)["movements"]["test-host"] == rows
    assert queue.item_path(pool.READY, key).read_bytes() == generation
    (checkout / "source.txt").write_text("changed resident dispatch fixture\n")
    assert pbresident.main(options) == 0
    changed = json.loads(capsys.readouterr().out)["movements"]["test-host"]
    assert changed["copy"]["action_key"] != key
    assert changed["evict"]["action_key"] != rows["evict"]["action_key"]
    assert queue.item_path(pool.READY, key).read_bytes() == generation
    assert queue.item_path(pool.READY, changed["copy"]["action_key"]).exists()
    assert store.read_movements(record["set_id"], "test-host") == changed
