"""Real e9 terminal shapes, stopped writers and descriptor namespace proofs."""
import json
import os
from pathlib import Path
import types

import pytest

from prismabuild import legacy_custody_host, materialize, pool, record_store, storage_tiers, tier_holdings
from prismabuild.legacy_custody import LegacyCustodyMigration
from legacy_custody_fixtures_1429 import stopped_legacy_hosts
from test_checkout_migration_r5_1429 import _legacy_fixture, _pre_contract


def _e9_terminal(tmp_path):
    queue, key, ledger = _legacy_fixture(tmp_path, snapshot=True, lease=False)
    path = queue.records.item_path(pool.CLAIMED, key)
    owner = record_store.read_document(path)
    # e9's serve_once result and the review's 63 real terminal holds use
    # detail.returncode. They have neither top-level field from r5's fixture.
    owner.update(status="executed", detail={"returncode": 0})
    queue.records.replace_record_at(queue.records.item_path(pool.DONE, key), owner)
    path.unlink()
    return queue, key, ledger


def test_e9_terminal_certifies_absence_through_the_normal_mint(tmp_path, monkeypatch):
    queue, key, ledger = _e9_terminal(tmp_path)
    migration = LegacyCustodyMigration(queue)
    issued = []
    mint = materialize.CheckoutCertifier.certify_checkout_cleanup
    def observe(*args, **kwargs):
        result = mint(*args, **kwargs)
        issued.append(result.get("certificate"))
        return result
    monkeypatch.setattr(materialize.CheckoutCertifier, "certify_checkout_cleanup", observe)
    rows = migration.apply(migration.inventory(), writers_stopped=True)
    assert rows[0]["status"] == "adopted" and rows[0]["certified_absent"]
    assert any(issued) and ledger.holder_tokens(key) == {"cpu": 1}
    journal = record_store.read_document(ledger.legacy_owner_path(key))
    proof = journal["legacy_proof"]
    assert proof["writers_stopped"] and proof["host"]
    assert {r["kind"] for r in proof["scan"]["roots"]} == {"checkout", "cleanup-failures", "quarantine"}
    assert all("entries_examined" in r for r in proof["scan"]["roots"])
    assert ledger.release(key, certificate=ledger.certify_release(key)) == 1
    import pbstatus
    rows = migration.status_rows()
    assert rows[0]["status"] == "certified absent" and rows[0]["proof"] == proof
    assert any(proof["host"] in line and "roots=" in line for line in pbstatus.legacy_custody_lines(rows))


def test_existing_legacy_checkout_adopts_its_inode_without_releasing(tmp_path, monkeypatch):
    queue, key, ledger = _e9_terminal(tmp_path)
    root = tmp_path / "legacy-materialized"
    root.mkdir()
    tree = root / (key[:12] + ".abcdefgh")
    tree.mkdir()
    (tree / "payload").write_text("legacy checkout")
    tree.with_name(tree.name + ".lock").touch()
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", root)
    migration = LegacyCustodyMigration(queue)
    rows = migration.apply(migration.inventory(), writers_stopped=True)
    assert rows[0]["status"] == "adopted" and not rows[0]["certified_absent"]
    assert ledger.holder_tokens(key) and tree.is_dir()
    with pytest.raises(pool.PoolContractError):
        ledger.certify_release(key)
    journal = record_store.read_document(ledger.legacy_owner_path(key))
    assert journal["checkout_registrations"][0]["inode"] == tree.stat().st_ino
    cleanup = queue.certify_checkout_cleanup(journal["action_key"])
    assert cleanup["complete"] and not tree.exists()
    assert ledger.release(key, certificate=ledger.certify_release(key)) == 1


@pytest.mark.parametrize("fault", ["symlink", "eacces", "quarantine", "renamed-quarantine", "failure", "collision", "rename", "unreadable-match"])
def test_any_namespace_uncertainty_keeps_custody(tmp_path, monkeypatch, fault):
    queue, key, ledger = _e9_terminal(tmp_path)
    root = tmp_path / "legacy-materialized"
    root.mkdir()
    target = root / (key[:12] + ".abcdefgh")
    if fault == "symlink":
        root.rmdir()
        root.symlink_to(tmp_path)
    elif fault in {"quarantine", "renamed-quarantine", "failure"}:
        parent = tmp_path / "legacy-quarantine" if fault in {"quarantine", "renamed-quarantine"} else root / "cleanup-failures"
        parent.mkdir()
        name = "renamed-without-a-key" if fault == "renamed-quarantine" else key + ".retained.json"
        (parent / name).write_text("surviving legacy evidence")
    elif fault == "collision":
        target.mkdir()
        other = key[:12] + "f" * 52
        queue.records.replace_record_at(queue.records.item_path(pool.DONE, other), {"action_key": other})
    else:
        original = os.listdir if fault == "rename" else os.stat
        injected = []
        def deny(*args, **kwargs):
            if fault == "eacces" and kwargs.get("dir_fd") is not None:
                raise PermissionError("root scan denied")
            if fault == "rename":
                result = original(*args, **kwargs)
                if not injected and isinstance(args[0], int):
                    injected.append(True)
                    target.mkdir()
                return result
            if str(args[0]).startswith(key[:12]):
                raise PermissionError("matching entry unreadable")
            return original(*args, **kwargs)
        if fault == "unreadable-match":
            target.mkdir()
        monkeypatch.setattr(os, "listdir" if fault == "rename" else "stat", deny)
    migration = LegacyCustodyMigration(queue)
    rows = migration.apply(migration.inventory(), writers_stopped=True)
    assert rows[0]["status"] == "retained"
    assert ledger.holder_tokens(key)


@pytest.mark.parametrize("fault", ["none", "open-gate", "scope", "supervisor", "writer", "cron", "gate-change"])
def test_writer_flag_cannot_replace_independent_host_proof(tmp_path, monkeypatch, stopped_legacy_hosts, fault):
    gate = dict(stopped_legacy_hosts["gate"])
    reads = []
    def read_gate(*args):
        reads.append(True)
        return None if fault == "open-gate" else dict(gate, changed_unix=2.) if fault == "gate-change" and len(reads) > 1 else gate
    monkeypatch.setattr(legacy_custody_host.park_proof, "read_gate", read_gate)
    monkeypatch.setattr(legacy_custody_host.resource_scope, "broker_request", lambda *a, **k: {
        "draining": True, "health": True, "active_scopes": 1 if fault == "scope" else 0,
        "maintenance_owner": gate["owner"]})
    monkeypatch.setattr(legacy_custody_host.subprocess, "run", lambda *a, **k: types.SimpleNamespace(
        stdout="ActiveState=" + ("active" if fault == "supervisor" else "inactive") + "\nMainPID=0\nExecStart=" + ("legacy" if fault == "cron" else "--systemd")))
    monkeypatch.setattr(legacy_custody_host.park_proof, "proc_census", lambda: [(42, "9", ["worker_loop.py"])] if fault == "writer" else [])
    if fault == "none":
        proof = stopped_legacy_hosts["real_stopped_host"]()
        assert proof["writer_processes"] == proof["active_scopes"] == 0
        assert proof["supervisor"] == "inactive" and proof["cron_fenced"]
    else:
        with pytest.raises(ValueError):
            stopped_legacy_hosts["real_stopped_host"]()


def test_tier_and_ram_mirror_need_separate_occupancy_proofs(tmp_path):
    queue, key, host = _e9_terminal(tmp_path)
    queue.ledger("storage").ensure_capacity({"mem_gb": 8})
    tier = queue.tier("ram:storage")
    tier.grow_supply({"ram_gib": 4})
    assert tier.take(key, {"ram_gib": 1})
    _pre_contract(tier._ledger, key)
    mirror = queue.ledger("storage")
    _pre_contract(mirror, storage_tiers.RAM_HOST_MEMORY_PREFIX + key)
    migration = LegacyCustodyMigration(queue)
    rows = migration.apply(migration.inventory(), writers_stopped=True)
    assert len(rows) == 3 and all(r["status"] == "adopted" for r in rows)
    assert all(r["proof"]["occupancy"] for r in rows if r["kind"] == "tier" or r["holder"].startswith(storage_tiers.RAM_HOST_MEMORY_PREFIX))
    # A later same-holder addition rotates the reservation nonce but keeps
    # the inventory-bound legacy owner, as the review's N-b requires.
    assert tier.take(key, {"ram_gib": 1})
    assert tier_holdings.egress_all(queue, key) >= 1
    assert not tier.holdings(key) and not mirror.holder_tokens(storage_tiers.RAM_HOST_MEMORY_PREFIX + key)


@pytest.mark.parametrize("fault", ["missing-coverage", "missing-occupancy", "occupied"])
def test_no_default_proof_for_unknown_historical_or_occupancy_roots(tmp_path, monkeypatch, stopped_legacy_hosts, fault):
    import socket
    queue, key, ledger = _e9_terminal(tmp_path)
    tier = queue.tier("stage:test")
    tier.grow_supply({"stage_gib": 2})
    assert tier.take(key, {"stage_gib": 1})
    _pre_contract(tier._ledger, key)
    root = tmp_path / "historical-checkouts"
    root.mkdir()
    occupancy = tmp_path / "physical-tier-root"
    occupancy.mkdir()
    if fault == "occupied":
        (occupancy / "arbitrary-artifact-name").write_text("charged bytes")
    covered = {"coverage_evidence": "test historical configuration", "all_historical_roots": fault != "missing-coverage",
               "roots": [{"path": str(root), "kind": "checkout"},
                         {"path": str(root / "cleanup-failures"), "kind": "cleanup-failures"}],
               "occupancy_roots": {} if fault == "missing-occupancy" else {"tier:stage:test:" + key: [str(occupancy)]}}
    migration = LegacyCustodyMigration(queue, roots={"runtime": "e9e9d907721282f517e4e8feca40eda1aec79e5d",
                                                   "hosts": {socket.gethostname(): covered}})
    monkeypatch.setattr(LegacyCustodyMigration, "_legacy_scan_evidence", stopped_legacy_hosts["real_scan_evidence"])
    rows = migration.apply(migration.inventory(), writers_stopped=True)
    assert next(row for row in rows if row["kind"] == "tier")["status"] == "retained"
    assert tier.holdings(key) == {"stage_gib": 1}


def test_cross_host_occupancy_retains_custody(tmp_path, monkeypatch, stopped_legacy_hosts):
    import socket
    from prismabuild import fleet_roster
    import prismabuild.legacy_custody as custody
    queue, key, host_ledger = _e9_terminal(tmp_path)
    tier_name = "prismabuild-stage:remote-storage"
    tier = queue.tier(tier_name)
    tier.grow_supply({"stage_gib": 2})
    assert tier.take(key, {"stage_gib": 1})
    _pre_contract(tier._ledger, key)
    host = socket.gethostname()
    roster = fleet_roster.Roster.capture(json.dumps({"boxes": {host: {}, "remote-storage": {}}}).encode())
    monkeypatch.setattr(fleet_roster.Roster, "at", classmethod(lambda cls, *a, **k: roster))
    root = tmp_path / "historical-checkouts"
    root.mkdir()
    physical = tmp_path / "remote-tier-root"
    physical.mkdir()
    covered = {"coverage_evidence": "private historical configuration", "all_historical_roots": True,
               "roots": [{"path": str(root), "kind": "checkout"},
                         {"path": str(root / "cleanup-failures"), "kind": "cleanup-failures"}],
               "occupancy_roots": {"tier:" + tier_name + ":" + key: [str(physical)]}}
    migration = LegacyCustodyMigration(queue, roots={"runtime": "e9e9d907721282f517e4e8feca40eda1aec79e5d", "hosts": {host: covered}})
    monkeypatch.setattr(LegacyCustodyMigration, "_legacy_scan_evidence", stopped_legacy_hosts["real_scan_evidence"])
    requests = []
    def remote(argv, **kwargs):
        assert argv[3] == "remote-storage"
        request = json.loads(kwargs["input"])
        requests.append(request)
        scans = {k: legacy_custody_host.legacy_namespace_scan(k, roots) for k, roots in request.items()}
        for scan in scans.values():
            scan["host"] = "remote-storage"
        return types.SimpleNamespace(stdout=json.dumps({"host": "remote-storage", "writers_stopped": {
            "host": "remote-storage", "writer_processes": 0}, "scans": scans}))
    monkeypatch.setattr(custody.subprocess, "run", remote)
    rows = migration.apply(migration.inventory(), writers_stopped=True)
    row = next(row for row in rows if row["kind"] == "tier")
    assert row["status"] == "retained" and not row.get("certified_absent")
    assert "owner and ledger host" in row["reason"]
    assert all(not request for request in requests), "remote peers prove stopped writers only"
    assert tier.holdings(key)
