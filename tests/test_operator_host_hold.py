"""Operator holds close every admission route without altering running actions."""
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from threading import Event
import time

import pytest

from prismabuild import census, host_hold, pool, role_session
from test_a_draining_worker_stays_visible import _load_loop, HOST, DRAIN_GATE
from test_gpu_evicts_cpu_guests import guest_fleet
import pbstatus


def put_hold(root, host=HOST, **kw):
    return host_hold.change_host_hold(root, host, owner="operator", reason="external load",
                                      expires_unix=time.time() + 3600, **kw)


@pytest.mark.parametrize("route", ["cpu", "gpu", "guest"])
@pytest.mark.parametrize("arrival", ["before-pass", "before-rename"])
def test_every_claim_route_obeys_the_same_hold_check(guest_fleet, monkeypatch, route, arrival):
    fleet = guest_fleet
    key = fleet.publish("a", gpu=route == "gpu",
                        **({"tags": ["sparky"]} if route == "cpu" else {}))
    commit = fleet.queue._rename_and_commit_claim
    if arrival == "before-pass":
        put_hold(fleet.queue.root, "sparky")
    else:
        def hold_at_commit(*args):
            put_hold(fleet.queue.root, "sparky")
            return commit(*args)

        monkeypatch.setattr(fleet.queue, "_rename_and_commit_claim", hold_at_commit)
    assert fleet.claim() is None
    assert fleet.queue.records.item_path(pool.READY, key).exists()
    assert not fleet.queue.records.item_path(pool.CLAIMED, key).exists()
    assert not fleet.queue.records.lease_path(key).exists()
    assert fleet.queue.ledger("sparky").held() == {}
    host_hold.change_host_hold(fleet.queue.root, "sparky", owner="operator", release=True)
    monkeypatch.setattr(fleet.queue, "_rename_and_commit_claim", commit)
    claimed = fleet.claim()
    assert claimed["action_key"] == key
    assert bool(claimed.get("cpu_guest")) is (route == "guest")


def test_a_hold_leaves_a_running_claim_and_its_reservation_untouched(guest_fleet):
    fleet = guest_fleet
    key = fleet.publish("a")
    running = fleet.claim()
    path = fleet.queue.records.item_path(pool.CLAIMED, key)
    before = path.read_bytes()
    held = fleet.queue.ledger("sparky").held()
    put_hold(fleet.queue.root, "sparky")
    fleet.publish("b", gpu=True, cpu=8)
    assert fleet.claim() is None
    assert path.read_bytes() == before
    assert fleet.queue.ledger("sparky").held() == held
    assert not fleet.stopped(key)
    fleet.finish(running)
    assert not path.exists()


@pytest.mark.parametrize("arrival", ["before-pass", "before-rename"])
def test_adaptive_claims_also_cross_the_hold_check(tmp_path, monkeypatch, arrival):
    queue = pool.PoolQueue(tmp_path / "queue")
    capacity = {"cpu": 2, "mem_gb": 4}
    tiers = {"preferred": [0, 1], "fallback": []}
    queue.ledger().configure_cpu_tiers(tiers)
    queue.publish(action_key="a" * 64, cas_root=tmp_path / "cas",
                  checkout_root=tmp_path, worker_script="worker.py", resources={"cpu": 1})
    monkeypatch.setattr(pool.cpu_admission, "action_identity", lambda item: ("shape", False))
    commit = queue._rename_and_commit_claim
    if arrival == "before-pass":
        put_hold(queue.root)
    else:
        def close_at_commit(*args):
            put_hold(queue.root)
            return commit(*args)
        monkeypatch.setattr(queue, "_rename_and_commit_claim", close_at_commit)
    options = dict(capacity=capacity, cpu_tiers=tiers, adaptive_cpu=True,
        cpu_sampler=lambda: dict(sampled_unix=time.time(), cpu_count=2,
                                interval_s=1., busy_cpus=0., psi_some=0.))
    assert queue.claim(**options) is None
    assert queue.ledger().held() == {}
    host_hold.change_host_hold(queue.root, HOST, owner="operator", release=True)
    monkeypatch.setattr(queue, "_rename_and_commit_claim", commit)
    assert queue.claim(**options)["action_key"] == "a" * 64


def test_expiry_resumes_claiming_and_is_logged_once(guest_fleet, monkeypatch, capsys):
    fleet = guest_fleet
    clock = time.time()
    host_hold.change_host_hold(fleet.queue.root, "sparky", owner="operator", reason="load",
                              now=clock, expires_unix=clock + 1)
    key = fleet.publish("a")
    assert fleet.claim() is None
    monkeypatch.setattr(host_hold.time, "time", lambda: clock + 2)
    assert fleet.claim()["action_key"] == key
    assert fleet.queue.claim_hold() is None
    assert fleet.queue.claim_hold() is None
    assert capsys.readouterr().out.count("operator hold expired") == 1


@pytest.mark.parametrize("fault", ["json", "schema", "owner", "expiry", "too-long", "bool", "directory", "symlink", "unreadable"])
def test_invalid_hold_is_fail_closed_and_visible(tmp_path, monkeypatch, fault):
    root = tmp_path / "queue"
    put_hold(root)
    path = host_hold.host_hold_path(root, HOST)
    body = json.loads(path.read_text())
    if fault == "json":
        path.write_text("{")
    elif fault in ("directory", "symlink"):
        path.unlink()
        path.mkdir() if fault == "directory" else path.symlink_to(tmp_path / "absent")
    elif fault == "unreadable":
        read = host_hold.immutable_files.read_replaced_record
        def denied(leaf, **kw):
            if leaf == path:
                raise PermissionError("fixture")
            return read(leaf, **kw)
        monkeypatch.setattr(host_hold.immutable_files, "read_replaced_record", denied)
    else:
        if fault == "schema": body["schema"] = "unknown"
        if fault == "owner": body["owner"] = ""
        if fault == "expiry": body["expires_unix"] = "later"
        if fault == "too-long": body["expires_unix"] += 12 * 3600
        if fault == "bool": body["created_unix"] = True
        path.write_text(json.dumps(body))
    queue = pool.PoolQueue(root)
    queue.announce(host=HOST, tags=[], has_gpu=False, capacity={"cpu": 1})
    assert queue.claim_hold().error
    node = census.offers(root).value["nodes"][0]
    assert node["state"] == "held" and node["hold"]["error"]
    assert "operator hold" in "\n".join(pbstatus.pool_node_lines([node]))


def test_only_the_owner_can_release_or_replace_and_force_needs_a_reason(tmp_path):
    put_hold(tmp_path)
    for release in (True, False):
        with pytest.raises(ValueError, match="another owner"):
            host_hold.change_host_hold(tmp_path, HOST, owner="other", reason="takeover",
                                      expires_unix=time.time() + 100, release=release)
    with pytest.raises(ValueError, match="requires --reason"):
        host_hold.change_host_hold(tmp_path, HOST, owner="other", force=True, release=True)
    host_hold.change_host_hold(tmp_path, HOST, owner="other", force=True,
                              reason="incident override", release=True)
    assert host_hold.read_host_hold(tmp_path, HOST) is None
    put_hold(tmp_path)
    host_hold.change_host_hold(tmp_path, HOST, owner="operator", release=True)
    host_hold.change_host_hold(tmp_path, HOST, owner="operator", release=True)


@pytest.mark.parametrize("seconds", [-1, 0, 43201, float("inf"), float("nan"), True])
def test_expiry_must_be_finite_future_and_at_most_twelve_hours(tmp_path, seconds):
    with pytest.raises(ValueError):
        host_hold.change_host_hold(tmp_path, HOST, owner="operator", reason="load",
                                  now=0, expires_unix=seconds)


@pytest.mark.parametrize("drain", [False, True])
def test_worker_offer_and_pbstatus_show_hold_and_compose_with_upgrade_drain(tmp_path, monkeypatch, capsys, drain):
    root = tmp_path / "pb-queue"
    queue = pool.PoolQueue(root)
    queue.records.ensure_layout()
    hold = put_hold(root)
    gate = DRAIN_GATE if drain else {"draining": False}
    loop = _load_loop(tmp_path, monkeypatch, gate)
    gate_bytes = role_session.MAINTENANCE_GATE.read_bytes()
    assert loop._run_loop(Event()) == 0
    offer = json.loads((root / "workers" / f"{HOST}.json").read_text())
    assert offer["hold"]["owner"] == "operator"
    assert all(value == 0 for value in offer["observed_capacity"].values())
    assert role_session.MAINTENANCE_GATE.read_bytes() == gate_bytes
    node = census.offers(root).value["nodes"][0]
    assert node["state"] == "held"
    assert node["hold"]["expires_unix"] == hold.expires_unix
    assert node["draining"] is drain
    assert "external load" in "\n".join(pbstatus.pool_node_lines([node]))
    capsys.readouterr()
    assert pbstatus.main(["--transport", "pool", "--json", "--recent", "0",
                          "--queue-root", str(root)]) == 0
    rendered = json.loads(capsys.readouterr().out)
    assert rendered["nodes"][0]["state"] == "held"
    assert rendered["nodes"][0]["hold"]["owner"] == "operator"
    host_hold.change_host_hold(root, HOST, owner="operator", release=True)
    assert loop._run_loop(Event()) == 0
    node = census.offers(root).value["nodes"][0]
    assert node["state"] == ("draining" if drain else "live")
    assert node["held"] is False


def test_pbhold_command_sets_lists_and_releases_a_fixture(tmp_path, capsys):
    tool = Path(__file__).resolve().parents[1] / "tools/fleet/pbhold.py"
    spec = importlib.util.spec_from_file_location("operator_pbhold", tool)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    until = datetime.fromtimestamp(time.time() + 100, timezone.utc).isoformat()
    common = ["--queue-root", str(tmp_path), "--host", HOST]
    assert module.hold_main(common + ["--owner", "operator", "--reason", "load", "--until", until]) == 0
    assert module.hold_main(common + ["--list"]) == 0
    assert '"owner": "operator"' in capsys.readouterr().out
    with pytest.raises(SystemExit):
        module.hold_main(common + ["--release", "--owner", "wrong"])
    assert module.hold_main(common + ["--release", "--owner", "wrong", "--force", "--reason", "incident"]) == 0
    assert host_hold.read_host_hold(tmp_path, HOST) is None
