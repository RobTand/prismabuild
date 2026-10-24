"""Round-6 independent review probes for #1429 at fc397d80 (Opus reviewer).

Naming:
  test_ok_*        claimed behaviour; expected to PASS on the head.
  test_blocking_*  an in-scope invariant; a FAIL on the head is a BLOCKING defect.
  test_nb_*        a NON-BLOCKING gap; a FAIL documents it.
"""
import copy
import importlib.util
import json
import os
import socket
import subprocess
import sys
import threading
import time
import types
from pathlib import Path

import pytest

from prismabuild import (fleet_roster, legacy_custody, legacy_custody_host, lifetime_fence, materialize,
                         pool, record_store, resource_scope, storage_tiers)
from prismabuild.legacy_custody import LegacyCustodyMigration
from lifetime_fixtures_1429 import WORKER, claim, fenced_snapshot_queue
from test_lifetime_contract_1429 import _execute, _finish, broker, Skew
from test_checkout_migration_r5_1429 import _pre_contract

ROOT = Path(__file__).resolve().parents[1]
E9 = "e9e9d907721282f517e4e8feca40eda1aec79e5d"
MIRROR = storage_tiers.RAM_HOST_MEMORY_PREFIX


# -- B1: no caller-chosen custody ----------------------------------------------------

def _refuse(*args, **kwargs):
    raise PermissionError("registered checkout retained by review fault")


def _uncertified(tmp_path, monkeypatch, *, lose):
    """An execute whose removal failed, then the claim or the lease is lost."""
    queue, action, cas = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", _refuse)
    item = claim(queue)
    outcome = _execute(queue, item)
    checkout = Path(outcome["checkout_cleanup"]["path"])
    assert checkout.is_dir() and outcome["checkout_cleanup"]["complete"] is False
    lost = (queue.records.item_path(pool.CLAIMED, key) if lose == "claim"
            else queue.records.lease_path(key))
    lost.rename(tmp_path / ("lost-" + lose + ".json"))
    assert queue.ledger().holder_tokens(key)
    return queue, key, item, outcome, checkout


def _owner(queue, key):
    lease = record_store.read_document(queue.records.lease_path(key))
    return lease if lease is not None else record_store.read_document(queue.records.item_path(pool.CLAIMED, key))


def _forged_unlink_proof(owner):
    """Every field is copied from the durable registration; nothing was unlinked."""
    entry = owner["checkout_registrations"][0]
    registration = {k: v for k, v in entry.items() if k not in ("cleanup", "completion_claim")}
    identity = dict(entry.get("completion_claim") or lifetime_fence.attempt_identity(owner))
    return {"complete": True, "unlinked": True, "claim": identity, "registration": registration,
            "path": registration["path"], "device": registration["device"], "inode": registration["inode"],
            "ended_unix": time.time()}


def _attempt_release(ledger, key, forge):
    try:
        certificate = forge()
    except Exception as exc:                                 # noqa: BLE001
        return f"forge-refused {type(exc).__name__}: {exc}"
    try:
        return f"released {ledger.release(key, certificate=certificate)}"
    except Exception as exc:                                 # noqa: BLE001
        return f"release-refused {type(exc).__name__}: {exc}"


@pytest.mark.parametrize("route", ["certify_release-claim_snapshot", "certify_checkout_cleanup-proof"])
def test_blocking_caller_unlink_proof_cannot_certify_a_live_tree(tmp_path, broker, monkeypatch, route):
    queue, key, item, outcome, checkout = _uncertified(tmp_path, monkeypatch, lose="claim")
    ledger = queue.ledger()
    lease = _owner(queue, key)
    proof = _forged_unlink_proof(lease)
    with pytest.raises(pool.PoolContractError):
        ledger.certify_release(key)                       # control: durable custody refuses

    def forge():
        if route == "certify_release-claim_snapshot":
            return ledger.certify_release(key, claim_snapshot=dict(lease, checkout_cleanup=proof))
        return queue.queue.certify_checkout_cleanup(
            queue.queue, dict(lease), proof=proof, recover=False,
            local_checkout_root=pool.LOCAL_CHECKOUT_ROOT, release_ledger=ledger,
            release_holder=key, release_custody_key=key).get("certificate")

    verdict = _attempt_release(ledger, key, forge)
    held = ledger.holder_tokens(key)
    print("caller-proof", route, "->", verdict, "| held", held, "| tree", checkout.is_dir())
    assert checkout.is_dir()
    assert held, f"[blocking] a caller-supplied unlink proof ({route}) returned a live tree's tokens"


def test_blocking_persisted_caller_proof_lets_the_reaper_release_a_live_tree(tmp_path, broker, monkeypatch):
    queue, key, item, outcome, checkout = _uncertified(tmp_path, monkeypatch, lose="claim")
    ledger = queue.ledger()
    lease = _owner(queue, key)
    with pytest.raises(TypeError):
        queue.queue.certify_checkout_cleanup(key, proof=_forged_unlink_proof(lease))
    # A forged dictionary planted directly in the durable lease is ignored too.
    forged = _forged_unlink_proof(lease)
    lease["checkout_cleanup"] = forged
    lease["checkout_registrations"][0]["cleanup"] = forged
    queue.records.replace_record_at(queue.records.lease_path(key), lease)
    result = {"complete": False}
    stored = record_store.read_document(queue.records.lease_path(key)) or {}
    clock = Skew(monkeypatch)
    clock.to(item["lifetime_deadline_unix"] + 2 * pool.LEASE_TIMEOUT_S + 60)
    errors = []
    for step in (lambda: queue.sweep_widowed_leases(timeout_s=pool.LEASE_TIMEOUT_S), queue.reap_stale,
                 lambda: queue.sweep_finish_tombstones(grace_s=0.0)):
        try:
            step()
        except Exception as exc:                             # noqa: BLE001
            errors.append(f"{type(exc).__name__}: {str(exc)[:120]}")
    held = ledger.holder_tokens(key)
    print("persisted-proof certify", result.get("complete"), "| stored cleanup",
          [(e.get("cleanup") or {}).get("unlinked") for e in stored.get("checkout_registrations") or []],
          "| errors", errors, "| held", held, "| tree", checkout.is_dir())
    assert checkout.is_dir()
    assert held, "[blocking] the public recover path persisted a caller proof and the reaper returned a live tree's tokens"


@pytest.mark.parametrize("lose", ["claim", "lease"])
def test_blocking_caller_queue_object_cannot_choose_the_owner(tmp_path, broker, monkeypatch, lose):
    queue, key, item, outcome, checkout = _uncertified(tmp_path, monkeypatch, lose=lose)
    ledger = queue.ledger()
    real = _owner(queue, key)
    assert real["checkout_registrations"]
    with pytest.raises(pool.PoolContractError):
        ledger.certify_release(key)                       # control: durable custody refuses
    forged = dict(real, checkout_registrations=[])
    forged.pop("checkout_cleanup", None)
    decoy = pool.PoolQueue(tmp_path / "caller-queue")
    decoy.records.ensure_layout()
    target = decoy.records.lease_path(key) if lose == "claim" else decoy.records.item_path(pool.CLAIMED, key)
    decoy.records.replace_record_at(target, forged)

    def forge():
        return queue.queue.certify_checkout_cleanup(
            decoy, forged, recover=False, local_checkout_root=pool.LOCAL_CHECKOUT_ROOT,
            release_ledger=ledger, release_holder=key, release_custody_key=key, select_owner=True).get("certificate")

    verdict = _attempt_release(ledger, key, forge)
    held = ledger.holder_tokens(key)
    print("caller-queue", "lost", lose, "->", verdict, "| held", held, "| tree", checkout.is_dir())
    assert checkout.is_dir()
    assert held, f"[blocking] a caller-chosen queue root (real {lose} lost) returned a live tree's tokens"


def test_ok_owner_path_is_unreachable_by_keyword_positional_or_splat(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 2})
    assert ledger.acquire("owned", {"cpu": 1})
    calls = {
        "keyword": lambda: ledger.certify_release("owned", owner_path=tmp_path),
        "splat": lambda: ledger.certify_release("owned", **{"owner_path": tmp_path}),
        "positional": lambda: ledger.certify_release("owned", None, None, tmp_path),
        "certifier-keyword": lambda: queue.certify_checkout_cleanup(
            queue, {"action_key": "owned"}, owner_path=tmp_path, local_checkout_root=tmp_path),
    }
    outcomes = {}
    for name, call in calls.items():
        try:
            call()
            outcomes[name] = "accepted"
        except TypeError as exc:
            outcomes[name] = "TypeError"
    print("owner_path routes", outcomes)
    assert set(outcomes.values()) == {"TypeError"}
    assert ledger.holder_tokens("owned") == {"cpu": 1}


# -- B2: recovery journals resume --------------------------------------------------------

def _state(queue, key):
    claimed = queue.records.dir(pool.CLAIMED)
    return {"ready": queue.records.item_path(pool.READY, key).exists(),
            "claimed": queue.records.item_path(pool.CLAIMED, key).exists(),
            "lease": queue.records.lease_path(key).exists(),
            "late_finish": sorted(p.name[65:85] for p in claimed.iterdir()
                                  if p.name.startswith(key + ".") and p.name.endswith(".late-finish")),
            "done": queue.records.item_path(pool.DONE, key).exists(),
            "failed": queue.records.item_path(pool.FAILED, key).exists(),
            "held": sorted(p.name for p in queue.ledger().held_dir.iterdir())}


def _recover(queue, clock, seconds, rounds=3):
    clock.to(clock.now() + seconds)
    errors = []
    for _ in range(rounds):
        for step in (queue.reap_stale,
                     lambda: queue.sweep_widowed_leases(timeout_s=pool.LEASE_TIMEOUT_S),
                     lambda: queue.ledger().sweep_stale_acquisitions(),
                     lambda: queue.sweep_finish_tombstones(grace_s=0.0)):
            try:
                step()
            except Exception as exc:                         # noqa: BLE001
                errors.append(f"{type(exc).__name__}: {str(exc)[:160]}")
    return sorted(set(errors))[:4]


def _reclaim(queue):
    try:
        again = queue.claim(tags=[lifetime_fence.LIFETIME_TAG])
        return f"claimed {again is not None}", again
    except Exception as exc:                                 # noqa: BLE001
        return f"raised {type(exc).__name__}: {exc}", None


def _predecessor_reaped(tmp_path, broker, monkeypatch, *, fence_s):
    queue, action, cas = fenced_snapshot_queue(tmp_path, fence_s=fence_s)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    first = claim(queue)
    _execute(queue, first)
    stale_claim = record_store.read_document(queue.records.item_path(pool.CLAIMED, key))
    stale_lease = record_store.read_document(queue.records.lease_path(key))
    clock = Skew(monkeypatch)
    clock.to(first["lifetime_deadline_unix"] + pool.LEASE_TIMEOUT_S + 1)
    queue.reap_stale()
    assert not queue.records.item_path(pool.CLAIMED, key).exists()
    queue.publish(action_key=key, cas_root=cas.root, checkout_snapshot=first["checkout_snapshot"],
                  worker_script=WORKER, max_attempts=1)
    return queue, key, first, stale_claim, stale_lease, clock


def _land_at_successor_lease_write(monkeypatch, first, stale_lease):
    original = pool.PoolQueue.write_lease
    raced = []

    def racing(self, action_key, **kwargs):
        snapshot = kwargs.get("claim_snapshot") or {}
        if not raced and snapshot.get("claimed_unix") != first.get("claimed_unix"):
            raced.append(action_key)
            self.records.replace_record_at(self.records.lease_path(action_key), stale_lease)
        return original(self, action_key, **kwargs)

    monkeypatch.setattr(pool.PoolQueue, "write_lease", racing)
    return raced, lambda: monkeypatch.setattr(pool.PoolQueue, "write_lease", original)


@pytest.mark.parametrize("fault", ["cleanup-incomplete-twice", "cleanup-raises-once",
                                   "journal-write-crash-once", "lease-unlink-crash-once",
                                   "journal-unlink-crash-twice"])
def test_blocking_recovery_resumes_after_double_failures_and_crash_points(tmp_path, broker, monkeypatch, fault):
    queue, key, first, _, stale_lease, clock = _predecessor_reaped(tmp_path, broker, monkeypatch, fence_s=3600.0)
    raced, restore = _land_at_successor_lease_write(monkeypatch, first, stale_lease)
    injected = []
    if fault.startswith("cleanup"):
        original_cleanup = pool.PoolQueue.cleanup_action_containers
        limit = 2 if fault.endswith("twice") else 1

        def flaky(self, record, **kwargs):
            if kwargs.get("reason") == "stale-heartbeat" and len(injected) < limit:
                injected.append("cleanup")
                if fault == "cleanup-raises-once":
                    raise OSError("review fault: scope inventory unreadable")
                return {"complete": False, "used": True, "removed": [], "remaining": [],
                        "error": "review fault: predecessor scope still populated"}
            return original_cleanup(self, record, **kwargs)

        monkeypatch.setattr(pool.PoolQueue, "cleanup_action_containers", flaky)
    elif fault == "journal-write-crash-once":
        original_replace = record_store.RecordStore.replace_record_at

        def crashing_write(self, path, payload):
            if Path(path).name.endswith(".late-finish") and Path(path).name.startswith(key) and not injected:
                injected.append("journal-write")
                raise OSError("review fault: crash at the journal write")
            return original_replace(self, path, payload)

        monkeypatch.setattr(record_store.RecordStore, "replace_record_at", crashing_write)
    else:
        suffix = ".lease" if fault.startswith("lease") else ".late-finish"
        limit = 2 if fault.endswith("twice") else 1
        original_unlink = Path.unlink

        def crashing(self, *args, **kwargs):
            if self.name.endswith(suffix) and self.name.startswith(key) and len(injected) < limit:
                injected.append("unlink" + suffix)
                raise OSError("review fault: crash before unlink")
            return original_unlink(self, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", crashing)
    verdict, _ = _reclaim(queue)
    restore()
    print(fault, "raced", bool(raced), "injected", injected, verdict, _state(queue, key))
    errors = _recover(queue, clock, 2 * pool.LEASE_TIMEOUT_S + 600)
    state = _state(queue, key)
    reclaim, _ = _reclaim(queue)
    print(fault, "after", state, "errors", errors, "| reclaim", reclaim)
    assert raced and injected
    assert not state["late_finish"], f"[blocking] recovery journal left in claimed/ after {fault}"
    assert reclaim == "claimed True", f"[blocking] key can never be claimed again after {fault}"


# -- B3: a stalled heartbeat publish ----------------------------------------------------

@pytest.mark.parametrize("landing", ["after-successor-execute", "after-successor-claim"])
def test_blocking_stalled_heartbeat_publish_cannot_overwrite_the_successor(tmp_path, broker, monkeypatch, landing):
    queue, action, cas = fenced_snapshot_queue(tmp_path, fence_s=3600.0)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    first = claim(queue)
    outcome = _execute(queue, first)
    clock = Skew(monkeypatch)
    path = queue.records.lease_path(key)
    replace = record_store.RecordStore.replace_record_at
    entered, resume = threading.Event(), threading.Event()
    errors = []
    def stalled(self, target, payload):
        if Path(target) == path and threading.current_thread().name == "predecessor-beat":
            entered.set()
            assert resume.wait(20), "test did not resume its stalled heartbeat"
        return replace(self, target, payload)
    monkeypatch.setattr(record_store.RecordStore, "replace_record_at", stalled)
    def beat():
        try:
            queue.queue.write_lease(key, owner=first["claimed_by"], claim_snapshot=first)
        except Exception as exc:
            errors.append(exc)
    thread = threading.Thread(target=beat, name="predecessor-beat")
    thread.start()
    try:
        assert entered.wait(20)
        # The heartbeat must have retained the key lock through publication.
        with queue.queue.action_transition_lock(key, blocking=False) as acquired:
            assert not acquired, "heartbeat publication owns no transition exclusion"
        # Another heartbeat skips, rather than blocking behind the stalled one.
        skipped = threading.Event()
        contender = threading.Thread(target=lambda: (queue.queue.write_lease(
            key, owner=first["claimed_by"], claim_snapshot=first), skipped.set()))
        contender.start()
        assert skipped.wait(5), "busy heartbeat waited instead of skipping"
        contender.join(5)
    finally:
        resume.set()
        thread.join(20)
    assert not thread.is_alive() and not errors
    monkeypatch.setattr(record_store.RecordStore, "replace_record_at", replace)
    clock.to(first["lifetime_deadline_unix"] + pool.LEASE_TIMEOUT_S + 1)
    queue.reap_stale()
    queue.publish(action_key=key, cas_root=cas.root, checkout_snapshot=first["checkout_snapshot"],
                  worker_script=WORKER, max_attempts=1)
    second = claim(queue)
    if landing == "after-successor-execute":
        next_outcome = _execute(queue, second)
    with pytest.raises(pool.PoolContractError):
        queue.queue.write_lease(key, owner=first["claimed_by"], claim_snapshot=first)
    queue.queue.write_lease(key, owner=second["claimed_by"], claim_snapshot=second)
    next_outcome = next_outcome if landing == "after-successor-execute" else _execute(queue, second)
    _finish(queue, second, next_outcome)
    state = _state(queue, key)
    assert state["done"] and not state["claimed"] and not state["lease"] and not state["held"]


# -- B4: stopped-writer legacy adoption --------------------------------------------------

def _stopped_fleet(monkeypatch, boxes, *, local_aliases=()):
    """Every roster box is a verified-stopped host. The coordinator is not a
    roster box, so every observation crosses the emulated SSH transport."""
    gate = {"draining": True, "owner": "pb1429-adoption", "changed_unix": 1.0}

    def stopped():
        return {"host": legacy_custody.socket.gethostname(), "observed_unix": time.time(), "gate": gate, "broker_draining": True,
                "active_scopes": 0, "supervisor": "inactive", "writer_processes": 0, "cron_fenced": True}

    monkeypatch.setattr(legacy_custody_host, "legacy_stopped_host", stopped)
    roster = fleet_roster.Roster.capture(json.dumps({"boxes": {box: {} for box in boxes}}).encode())
    monkeypatch.setattr(fleet_roster.Roster, "at", classmethod(lambda cls, *a, **k: roster))
    if local_aliases:
        canonical = fleet_roster.Roster.canonical_box
        monkeypatch.setattr(fleet_roster.Roster, "canonical_box", lambda self, name: canonical(
            self, local_aliases.get(name, name)))
    calls = []

    def ssh(argv, **kwargs):
        address = argv[3]
        request = json.loads(kwargs["input"])
        calls.append((address, sorted(request)))
        result = legacy_custody_host.observe_legacy_custody_host(request)
        result["host"] = address
        return types.SimpleNamespace(stdout=json.dumps(result))

    monkeypatch.setattr(legacy_custody, "subprocess", types.SimpleNamespace(
        run=ssh, SubprocessError=subprocess.SubprocessError))
    monkeypatch.setattr(legacy_custody, "socket", types.SimpleNamespace(gethostname=lambda: boxes[0]))
    return calls


def _host_roots(base, box):
    root = base / "hosts" / box / "prismabuild-checkouts"
    (root / "cleanup-failures").mkdir(parents=True, exist_ok=True)
    return root, {"coverage_evidence": "review fixture: complete e9 roots for " + box,
                  "all_historical_roots": True,
                  "roots": [{"path": str(root), "kind": "checkout"},
                            {"path": str(root / "cleanup-failures"), "kind": "cleanup-failures"}],
                  "occupancy_roots": {}}


def _e9_done(queue, key, host, *, tier=False):
    """An e9 terminal with the live census's field set (12 host / 30 stage-tier shapes)."""
    record = {
        "schema": "prismaquant.prismabuild.pool_item.v1", "action_key": key, "status": "executed",
        "attempt_history": [{"attempt": 1, "host": host}], "attempts": 1, "max_attempts": 1,
        "cas_root": "/mnt/shared/prismabuild-fleet/cas", "claimed_by": host + ":12345", "claimed_host": host,
        "claimed_unix": 1792700000.0 + int(key[:4], 16), "published_unix": 1792690000.0,
        "finished_host": host, "finished_unix": 1792700500.0, "container_owner": key[::-1],
        "cpu_allocation": {"cpus": [5]}, "detail": {"returncode": 0, "status": "executed"},
        "needs_gpu": False, "priority": 0, "published_by": "review", "reserved_on": host,
        "resource_scope": {"scope_id": key[:16]}, "resource_scope_cleanup": {"complete": True},
        "resource_scope_intent": {"scope_id": key[:16]}, "resources": {"cpu": 1, "mem_gb": 1},
        "retry_safe": False, "tags": [], "worker_script": "tools/fleet/pbtest_worker.py",
        "checkout_snapshot": {"schema": "prismaquant.prismabuild.pbrun_checkout_snapshot.v1",
                              "commit": key[:40], "subdirectory": ".", "input": {"sha256": key}},
    }
    if tier:
        record.update(recompute=False, residency={"tier": "prismabuild-stage:" + host, "range_start_bytes": 0,
                                                  "range_end_bytes": 0},
                      residency_verdict={"status": "served"}, served_from="stage")
    queue.records.replace_record_at(queue.records.item_path(pool.DONE, key), record)
    return record


SHAPES = [("host", "sparklina", 9), ("host", "sparky", 3), ("tier", "prismabuild-stage:dl380g10", 30)]


def _census_world(tmp_path, monkeypatch, *, mixed):
    """The 42 qualifying real shapes from today's census, on a stopped fleet."""
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    calls = _stopped_fleet(monkeypatch, ["sparklina", "sparky", "dl380g10"])
    covered, checkout_roots, holds = {}, {}, []
    for box in ("sparklina", "sparky", "dl380g10"):
        checkout_roots[box], covered[box] = _host_roots(tmp_path, box)
    index = 0
    for kind, ledger_name, count in SHAPES:
        for _ in range(count):
            key = f"{0xa00000000000 + index * 0x10101:012x}" + "c" * 52
            host = ledger_name.rsplit(":", 1)[-1]
            _e9_done(queue, key, host, tier=kind == "tier")
            if kind == "host":
                ledger = queue.ledger(ledger_name)
                ledger.ensure_capacity({"cpu": 64, "mem_gb": 64})
                assert ledger.acquire(key, {"cpu": 1})
                _pre_contract(ledger, key)
            else:
                tier = queue.tier(ledger_name)
                tier.grow_supply({"stage_gib": 64})
                assert tier.take(key, {"stage_gib": 1})
                _pre_contract(tier._ledger, key)
                stage = tmp_path / "hosts" / host / "stage" / key
                stage.mkdir(parents=True)
                covered[host]["occupancy_roots"]["tier:" + ledger_name + ":" + key] = [str(stage)]
            holds.append((kind, ledger_name, key))
            index += 1
    expect = {}
    if mixed:
        def tree(i, name="abcdefgh"):
            kind, ledger_name, key = holds[i]
            path = checkout_roots[ledger_name.rsplit(":", 1)[-1]] / (key[:12] + "." + name)
            path.mkdir()
            (path / "payload").write_text("surviving e9 checkout")
            path.with_name(path.name + ".lock").touch()
            return path
        expect["surviving"] = {holds[0][2]: tree(0), holds[9][2]: tree(9), holds[12][2]: tree(12)}
        kind, ledger_name, key = holds[1]
        failure = checkout_roots["sparklina"] / "cleanup-failures" / (key + "." + key[:12] + ".zzzzzzzz.json")
        failure.write_text("{}")
        os.symlink("/nonexistent", checkout_roots["sparklina"] / (holds[2][2][:12] + ".symlinkd"))
        (Path(covered["dl380g10"]["occupancy_roots"]["tier:" + holds[13][1] + ":" + holds[13][2]][0])
         / "staged-bytes").write_text("charged")
        del covered["dl380g10"]["occupancy_roots"]["tier:" + holds[14][1] + ":" + holds[14][2]]
        expect["retained"] = {holds[1][2], holds[2][2], holds[13][2], holds[14][2]}
    roots = {"runtime": E9, "hosts": covered}
    return queue, holds, roots, expect, calls


def _apply_on_owner(migration, inventory, monkeypatch):
    result = []
    for row in inventory["holds"]:
        host = row["ledger"].rsplit(":", 1)[-1]
        with monkeypatch.context() as local:
            local.setattr(legacy_custody, "socket", types.SimpleNamespace(gethostname=lambda: host))
            local.setattr(legacy_custody_host.socket, "gethostname", lambda: host)
            result.extend(migration.apply(dict(inventory, holds=[row]), writers_stopped=True))
    return result


def _token_facts(migration, queue, kind, ledger_name, holder):
    ledger = queue.ledger(ledger_name) if kind == "host" else queue.tier(ledger_name)._ledger
    return migration._tokens(ledger, holder)


@pytest.mark.parametrize("mixed", [False, True], ids=["all-absent", "mixed-evidence"])
def test_ok_census_shapes_follow_the_grandfathering_decision(tmp_path, monkeypatch, mixed):
    import pbstatus
    queue, holds, roots, expect, calls = _census_world(tmp_path, monkeypatch, mixed=mixed)
    migration = LegacyCustodyMigration(queue, roots=roots)
    inventory = migration.inventory()
    assert sum(r["status"] == "eligible" for r in inventory["holds"]) == 42
    before = {(r["kind"], r["ledger"], r["holder"]): r["tokens"] for r in inventory["holds"]}
    started = time.monotonic()
    rows = _apply_on_owner(migration, inventory, monkeypatch)
    elapsed = time.monotonic() - started
    verdict = {}
    for row in rows:
        verdict[row["holder"]] = ("absent" if row.get("certified_absent") else row["status"])
    counts = {}
    for value in verdict.values():
        counts[value] = counts.get(value, 0) + 1
    print("census world", "mixed" if mixed else "all-absent", counts, "| apply", round(elapsed, 2), "s",
          "| observations", len(calls))
    for row in rows:
        if row["status"] == "retained":
            print("retained", row["kind"], row["ledger"], row["holder"][:12], "|", row["reason"][:140])
    # Decision 2: absence only through the normal certificate, never a token return.
    for kind, ledger_name, key in holds:
        assert _token_facts(migration, queue, kind, ledger_name, key) == before[(kind, ledger_name, key)]
    surviving = expect.get("surviving", {})
    retained = expect.get("retained", set())
    for kind, ledger_name, key in holds:
        if key in surviving:
            assert verdict[key] == "adopted", (key[:12], verdict[key])
            journal = record_store.read_document(
                (queue.ledger(ledger_name) if kind == "host" else queue.tier(ledger_name)._ledger).legacy_owner_path(key))
            assert [e["inode"] for e in journal["checkout_registrations"]] == [surviving[key].stat().st_ino]
        elif key in retained:
            assert verdict[key] == "retained", (key[:12], verdict[key])
        else:
            assert verdict[key] == "absent", (key[:12], verdict[key])
    lines = pbstatus.legacy_custody_lines(migration.status_rows())
    assert all(any(key[:12] in line for line in lines) for _, _, key in holds)
    # Decision 4: nothing releases on a timer or by default.
    clock = Skew(monkeypatch)
    clock.to(time.time() + 7 * 86400)
    for step in (queue.reap_stale, lambda: queue.sweep_widowed_leases(timeout_s=pool.LEASE_TIMEOUT_S),
                 lambda: queue.sweep_finish_tombstones(grace_s=0.0)):
        try:
            step()
        except Exception as exc:                             # noqa: BLE001
            print("sweep", type(exc).__name__, str(exc)[:120])
    for kind, ledger_name, key in holds:
        assert _token_facts(migration, queue, kind, ledger_name, key) == before[(kind, ledger_name, key)]
    # Decision 3: a surviving inode never certifies.
    for kind, ledger_name, key in holds:
        ledger = queue.ledger(ledger_name) if kind == "host" else queue.tier(ledger_name)._ledger
        if key in surviving:
            with pytest.raises(pool.PoolContractError):
                ledger.certify_release(key)
            assert surviving[key].is_dir()


def _returns(queue, holds, *, operator):
    returned = {}
    for kind, ledger_name, key in holds:
        ledger = queue.ledger(ledger_name) if kind == "host" else queue.tier(ledger_name)._ledger
        try:
            if operator:
                returned[key] = queue.reclaim_terminal_reservation(key, unpin=kind == "tier")["released"]
            else:
                returned[key] = ledger.release(key, certificate=ledger.certify_release(key))
        except Exception as exc:                             # noqa: BLE001
            returned[key] = f"{type(exc).__name__}: {str(exc)[:90]}"
    return returned


@pytest.mark.parametrize("route", ["operator-reclaim", "public-certificate"])
def test_certified_absent_census_holds_have_a_documented_return_path(tmp_path, monkeypatch, route):
    queue, holds, roots, expect, calls = _census_world(tmp_path, monkeypatch, mixed=False)
    migration = LegacyCustodyMigration(queue, roots=roots)
    rows = _apply_on_owner(migration, migration.inventory(), monkeypatch)
    assert all(r.get("certified_absent") for r in rows)
    returned = _returns(queue, holds, operator=route == "operator-reclaim")
    summary = {}
    for (kind, _, _), value in zip(holds, returned.values()):
        summary.setdefault((kind, str(value)[:90]), 0)
        summary[(kind, str(value)[:90])] += 1
    print("return", route, summary)
    left = [key[:12] for kind, ledger_name, key in holds
            if (queue.ledger(ledger_name).holder_tokens(key) if kind == "host" else queue.tier(ledger_name).holdings(key))]
    assert not left, f"[nb] {len(left)} certified-absent holds cannot be returned through {route}"


def test_nb_host_hold_is_scanned_on_its_own_ledger_host(tmp_path, monkeypatch):
    """An earlier attempt's leaked host hold, whose tree survives on that host."""
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    _stopped_fleet(monkeypatch, ["alpha", "beta"])
    key = "a1b2c3d4e5f6" + "7" * 52
    record = _e9_done(queue, key, "alpha")
    record.update(attempts=2, attempt_history=[{"attempt": 1, "host": "beta"}, {"attempt": 2, "host": "alpha"}])
    queue.records.replace_record_at(queue.records.item_path(pool.DONE, key), record)
    ledger = queue.ledger("beta")
    ledger.ensure_capacity({"cpu": 4})
    assert ledger.acquire(key, {"cpu": 1})
    _pre_contract(ledger, key)
    _, alpha = _host_roots(tmp_path, "alpha")
    beta_root, beta = _host_roots(tmp_path, "beta")
    survivor = beta_root / (key[:12] + ".abcdefgh")
    survivor.mkdir()
    migration = LegacyCustodyMigration(queue, roots={"runtime": E9, "hosts": {"alpha": alpha, "beta": beta}})
    rows = migration.apply(migration.inventory(), writers_stopped=True)
    print("cross-host", [(r["ledger"], r["status"], r.get("certified_absent"), r["reason"][:80],
                          (r.get("proof") or {}).get("host")) for r in rows], "| tree on beta", survivor.is_dir())
    assert survivor.is_dir()
    assert not rows[0].get("certified_absent"), "[nb] absence certified from alpha for a hold whose tree is on beta"


def test_nb_missing_named_checkout_root_is_not_absence_evidence(tmp_path, monkeypatch):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    _stopped_fleet(monkeypatch, ["alpha"])
    key = "f1e2d3c4b5a6" + "9" * 52
    _e9_done(queue, key, "alpha")
    ledger = queue.ledger("alpha")
    ledger.ensure_capacity({"cpu": 4})
    assert ledger.acquire(key, {"cpu": 1})
    _pre_contract(ledger, key)
    real_root, covered = _host_roots(tmp_path, "alpha")
    survivor = real_root / (key[:12] + ".abcdefgh")
    survivor.mkdir()
    typo = real_root.with_name("prismabuild-checkout")       # one letter short; never existed
    covered["roots"] = [{"path": str(typo), "kind": "checkout"},
                        {"path": str(typo / "cleanup-failures"), "kind": "cleanup-failures"}]
    migration = LegacyCustodyMigration(queue, roots={"runtime": E9, "hosts": {"alpha": covered}})
    rows = _apply_on_owner(migration, migration.inventory(), monkeypatch)
    scans = (rows[0].get("proof") or {}).get("scan", {}).get("roots", [])
    print("typo root", rows[0]["status"], rows[0].get("certified_absent"), rows[0]["reason"][:80],
          [("absent_root" in s, s["entries_examined"]) for s in scans], "| real tree", survivor.is_dir())
    assert not rows[0].get("certified_absent"), "[nb] a named checkout root that does not exist certified absence"


# -- Deploy: the documented operator identity ------------------------------------------

class _Backend:
    def healthy(self):
        return True

    def inventory(self):
        return {}

    def exists(self, scope):
        return False


def test_blocking_documented_operator_can_prove_stopped_writers(tmp_path, monkeypatch):
    """Guide steps 3 and 5 run as the operator user; the real broker answers by SO_PEERCRED UID."""
    assert os.getuid() != 0, "this probe needs the unprivileged operator identity"
    spec = importlib.util.spec_from_file_location("resource_broker_r6_review", ROOT / "tools/fleet/resource_broker.py")
    broker_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(broker_module)
    authority = broker_module.Authority(tmp_path / "broker-state", os.getuid(), _Backend(), max_memory_bytes=1 << 30)
    monkeypatch.chdir(tmp_path)
    server = broker_module.Server("b.sock", broker_module.Handler)
    server.authority = authority
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        real_request = resource_scope.broker_request
        ask = lambda request, **_: real_request(request, socket_path=Path("b.sock"))
        try:
            began = ask({"op": "maintenance_begin", "owner": "pb1429-adoption"})
            step3 = f"operator maintenance_begin ok draining={began.get('draining')}"
        except OSError as exc:
            step3 = f"operator maintenance_begin refused: {exc}"
        root_status = authority.handle(0, os.getpid(), {"op": "maintenance_begin", "owner": "pb1429-adoption"})
        gate = {"schema": "x", "draining": True, "owner": root_status.get("maintenance_owner"),
                "changed_unix": root_status.get("changed_unix", 1.0)}
        monkeypatch.setattr(legacy_custody_host.park_proof, "read_gate", lambda path: dict(gate))
        monkeypatch.setattr(legacy_custody_host.resource_scope, "broker_request", ask)
        monkeypatch.setattr(legacy_custody_host, "subprocess", types.SimpleNamespace(run=lambda *a, **k: types.SimpleNamespace(
            stdout="ActiveState=inactive\nMainPID=0\nExecStart={ argv[]=supervise.py --systemd }")))
        monkeypatch.setattr(legacy_custody_host.park_proof, "proc_census", lambda: [])
        try:
            proof = legacy_custody_host.legacy_stopped_host()
            step5 = f"operator stopped-host proof ok: {sorted(proof)}"
        except (OSError, ValueError) as exc:
            step5 = f"operator stopped-host proof refused: {type(exc).__name__}: {exc}"
        monkeypatch.setattr(legacy_custody_host.resource_scope, "broker_request",
                            lambda request, **_: authority.handle(0, os.getpid(), request))
        try:
            legacy_custody_host.legacy_stopped_host()
            as_root = "root stopped-host proof ok"
        except (OSError, ValueError) as exc:
            as_root = f"root stopped-host proof refused: {exc}"
    finally:
        server.server_close()
        thread.join(timeout=10)
    print("uid", os.getuid(), "| step 3:", step3, "| step 5:", step5, "| control:", as_root)
    assert as_root == "root stopped-host proof ok"
    assert step5.startswith("operator stopped-host proof ok"), (
        "[blocking] the documented unprivileged adopter can never prove stopped writers: " + step5)
