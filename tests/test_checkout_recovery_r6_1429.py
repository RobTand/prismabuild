"""R6 regressions adopted from the independent r5 review, B1 to B3."""
import copy


import gc


import json


import os


import pickle


import types


from pathlib import Path


import pytest


from prismabuild import lifetime_fence, materialize, pool, record_store, storage_tiers, tier_holdings


from prismabuild.materialize import ReleaseCertificate


from lifetime_fixtures_1429 import WORKER, claim, fenced_snapshot_queue


from test_lifetime_contract_1429 import _execute, _finish, broker, Skew


def _refuse(*args, **kwargs):
    raise PermissionError("registered checkout retained by review fault")


def _widowed(tmp_path, monkeypatch, *, fence_s=300.0):
    """r3/r4 precondition: an uncertified checkout whose lease is the only custody."""
    queue, action, cas = fenced_snapshot_queue(tmp_path, fence_s=fence_s)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", _refuse)
    item = claim(queue)
    outcome = _execute(queue, item)
    checkout = Path(outcome["checkout_cleanup"]["path"])
    assert checkout.is_dir() and outcome["checkout_cleanup"]["complete"] is False
    queue.records.item_path(pool.CLAIMED, key).rename(tmp_path / "lost-claim.json")
    assert queue.ledger().holder_tokens(key)
    return queue, key, item, outcome, checkout, cas


def _attempt_release(ledger, key, forge):
    try:
        certificate = forge()
    except Exception as exc:                                 # noqa: BLE001
        return f"forge-refused {type(exc).__name__}: {exc}"
    try:
        return f"released {ledger.release(key, certificate=certificate)}"
    except Exception as exc:                                 # noqa: BLE001
        return f"release-refused {type(exc).__name__}: {exc}"


@pytest.mark.parametrize("route", ["certify_release", "certify_checkout_cleanup"])
def test_blocking_public_owner_path_cannot_substitute_durable_custody(tmp_path, broker, monkeypatch, route):
    """A caller-supplied owner document, through public names only."""
    queue, key, item, outcome, checkout, _ = _widowed(tmp_path, monkeypatch)
    ledger = queue.ledger()
    lease = record_store.read_document(queue.records.lease_path(key))
    forged = dict(lease, checkout_registrations=[])
    forged.pop("checkout_cleanup", None)
    forged_path = tmp_path / "caller-owner.json"
    forged_path.write_text(json.dumps(forged))

    def forge():
        if route == "certify_release":
            return ledger.certify_release(key, owner_path=forged_path)
        result = queue.queue.certify_checkout_cleanup(
            queue.queue, forged, owner_path=forged_path, recover=False,
            local_checkout_root=pool.LOCAL_CHECKOUT_ROOT, release_ledger=ledger,
            release_holder=key, release_custody_key=key, select_owner=True)
        return result.get("certificate")

    verdict = _attempt_release(ledger, key, forge)
    held = ledger.holder_tokens(key)
    print("owner-path", route, "->", verdict, "| held", held, "| checkout", checkout.is_dir(),
          "| real lease registrations", len(lease.get("checkout_registrations") or []))
    assert checkout.is_dir()
    assert held, f"[blocking] caller owner_path ({route}) returned a registered live checkout's tokens"


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
    _execute(queue, first)                              # certified removal
    stale_claim = record_store.read_document(queue.records.item_path(pool.CLAIMED, key))
    stale_lease = record_store.read_document(queue.records.lease_path(key))
    clock = Skew(monkeypatch)
    clock.to(first["lifetime_deadline_unix"] + pool.LEASE_TIMEOUT_S + 1)
    queue.reap_stale()                                  # the reaper concludes the paused claim
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


def test_nb_r4_zombie_probe_unchanged_300s_fence(tmp_path, broker, monkeypatch):
    """r4's probe exactly: fence 300 s, recovery past every deadline, then reclaim."""
    queue, key, first, _, stale_lease, clock = _predecessor_reaped(tmp_path, broker, monkeypatch, fence_s=300.0)
    raced, restore = _land_at_successor_lease_write(monkeypatch, first, stale_lease)
    verdict, _ = _reclaim(queue)
    restore()
    print("zombie300", "raced", bool(raced), verdict, _state(queue, key))
    errors = _recover(queue, clock, 2 * pool.LEASE_TIMEOUT_S + 600)
    state = _state(queue, key)
    reclaim, _ = _reclaim(queue)
    print("zombie300-after", state, "errors", errors, "| reclaim", reclaim)
    assert raced
    wedged = state["claimed"] or state["lease"] or state["late_finish"] or (state["held"] and not reclaim.endswith("True"))
    assert not wedged, "[nb] r4 zombie probe still leaves claim/lease/journal or tokens"


@pytest.mark.parametrize("fault", ["cleanup-incomplete-once", "journal-unlink-crash-once"])
def test_blocking_failed_recovery_leaves_no_claim_blocking_journal(tmp_path, broker, monkeypatch, fault):
    """The builder's zombie regression plus one transient failure inside recovery."""
    queue, key, first, _, stale_lease, clock = _predecessor_reaped(tmp_path, broker, monkeypatch, fence_s=3600.0)
    raced, restore = _land_at_successor_lease_write(monkeypatch, first, stale_lease)
    injected = []
    if fault == "cleanup-incomplete-once":
        original_cleanup = pool.PoolQueue.cleanup_action_containers

        def flaky(self, record, **kwargs):
            if kwargs.get("reason") == "stale-heartbeat" and not injected:
                injected.append("cleanup")
                return {"complete": False, "used": True, "removed": [], "remaining": [],
                        "error": "review fault: predecessor scope still populated"}
            return original_cleanup(self, record, **kwargs)

        monkeypatch.setattr(pool.PoolQueue, "cleanup_action_containers", flaky)
    else:
        original_unlink = Path.unlink

        def crashing(self, *args, **kwargs):
            if self.name.endswith(".late-finish") and self.name.startswith(key) and not injected:
                injected.append("unlink")
                raise OSError("review fault: crash before journal unlink")
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


def _zombie_write(queue, key, stale_claim, stale_lease):
    """The paused predecessor heartbeat: its claim and identity reads see its own
    records, and its final previous-lease read sees the successor's lease."""
    lease_path = queue.records.lease_path(key)
    real_read = record_store.read_document
    reads = []

    def read(path, *args, **kwargs):
        if Path(path) == lease_path:
            reads.append(1)
            if len(reads) <= 2:
                return copy.deepcopy(stale_lease)
        return real_read(path, *args, **kwargs)

    patch = pytest.MonkeyPatch()
    patch.setattr(pool.record_store, "read_document", read)
    patch.setattr(pool.record_store, "value_or_raise", lambda *a, **k: copy.deepcopy(stale_claim))
    try:
        queue.queue.write_lease(key, owner=str(stale_claim["claimed_by"]), claim_snapshot=stale_claim)
        return f"zombie wrote after {len(reads)} lease reads"
    except Exception as exc:                                 # noqa: BLE001
        return f"zombie raised {type(exc).__name__}: {exc}"
    finally:
        patch.undo()


@pytest.mark.parametrize("landing", ["after-successor-execute", "before-successor-execute"])
def test_blocking_zombie_carrying_successor_registrations_cannot_wedge(tmp_path, broker, monkeypatch, landing):
    queue, key, first, stale_claim, stale_lease, clock = _predecessor_reaped(
        tmp_path, broker, monkeypatch, fence_s=3600.0)
    second = claim(queue)
    outcome = None
    if landing == "after-successor-execute":
        outcome = _execute(queue, second)              # successor registers and certifies its tree
    zombie = _zombie_write(queue, key, stale_claim, stale_lease)
    lease = record_store.read_document(queue.records.lease_path(key)) or {}
    carried = [entry.get("claim", {}).get("claimed_unix") for entry in lease.get("checkout_registrations") or []]
    print(landing, zombie, "| zombie lease attempt", lease.get("claimed_unix"), "| carried registrations of",
          carried, "| successor", second.get("claimed_unix"))
    try:
        if outcome is None:
            outcome = _execute(queue, second)
        _finish(queue, second, outcome)
        finished = "finished"
    except Exception as exc:                                 # noqa: BLE001
        finished = f"raised {type(exc).__name__}: {str(exc)[:160]}"
    errors = _recover(queue, clock, pool.LEASE_TIMEOUT_S + 60)
    mid = _state(queue, key)
    errors += _recover(queue, clock, 3600 + 2 * pool.LEASE_TIMEOUT_S + 600)
    state = _state(queue, key)
    claim_now = record_store.read_document(queue.records.item_path(pool.CLAIMED, key)) or {}
    note = str((claim_now.get("container_cleanup_pending") or claim_now.get("cleanup_attempts") or ""))[:200]
    trees = sorted(p.name for p in (tmp_path / "materialized").iterdir()) if (tmp_path / "materialized").exists() else []
    print(landing, "finish", finished, "| mid", mid, "| final", state, "| errors", errors,
          "| claim note", note, "| trees", trees)
    assert not state["claimed"] and not state["lease"] and not state["late_finish"], (
        f"[blocking] zombie lease ({landing}) wedged the successor's claim past every deadline")
    assert state["held"] == [] or trees, "tokens held with no tree and no claim"




def test_callers_cannot_select_release_custody_paths():
    import ast
    root = Path(__file__).resolve().parents[1]
    violations = []
    for source in (root / "src/prismabuild").glob("*.py"):
        for node in ast.walk(ast.parse(source.read_text())):
            if isinstance(node, ast.Call) and getattr(node.func, "attr", getattr(node.func, "id", "")) in {
                    "certify_release", "certify_checkout_cleanup"}:
                if any(keyword.arg == "owner_path" for keyword in node.keywords):
                    violations.append((source.name, node.lineno))
    assert not violations


@pytest.mark.parametrize("stage", ["journal", "cleanup-proof", "lease-unlink", "journal-unlink"])
def test_heartbeat_recovery_resumes_after_each_durable_step(tmp_path, broker, monkeypatch, stage):
    queue, key, first, _, stale_lease, clock = _predecessor_reaped(tmp_path, broker, monkeypatch, fence_s=3600.)
    raced, restore = _land_at_successor_lease_write(monkeypatch, first, stale_lease)
    replace, unlink = record_store.RecordStore.replace_record_at, Path.unlink
    injected = []
    journal_writes = []
    def crashing_write(self, path, value, *args, **kwargs):
        result = replace(self, path, value, *args, **kwargs)
        if path.name.endswith(".late-finish"):
            journal_writes.append(path)
            wanted = 1 if stage == "journal" else 2 if stage == "cleanup-proof" else None
            if len(journal_writes) == wanted and not injected:
                injected.append(stage)
                raise OSError("crash after durable journal write")
        return result
    def crashing_unlink(path, *args, **kwargs):
        result = unlink(path, *args, **kwargs)
        wanted = (stage == "lease-unlink" and path == queue.records.lease_path(key)
                  or stage == "journal-unlink" and path.name.endswith(".late-finish"))
        if wanted and not injected:
            injected.append(stage)
            raise OSError("crash after durable unlink")
        return result
    monkeypatch.setattr(record_store.RecordStore, "replace_record_at", crashing_write)
    monkeypatch.setattr(Path, "unlink", crashing_unlink)
    _reclaim(queue)
    restore()
    assert injected
    _recover(queue, clock, 2 * pool.LEASE_TIMEOUT_S + 600)
    assert not _state(queue, key)["late_finish"]
    assert _reclaim(queue)[0] == "claimed True"


def test_pre_r6_heartbeat_with_borrowed_envelope_recovers_own_claim(tmp_path, broker, monkeypatch):
    queue, key, first, old_claim, old_lease, clock = _predecessor_reaped(tmp_path, broker, monkeypatch, fence_s=3600.)
    second = claim(queue)
    outcome = _execute(queue, second)
    own = record_store.read_document(queue.records.item_path(pool.CLAIMED, key))
    # Preserve an additional genuine predecessor debt beside the borrowed
    # successor list. Recovery must certify both authorities, never discard
    # the predecessor's own tree just because one entry was foreign.
    retained = tmp_path / "materialized" / (key[:12] + ".oldowned")
    retained.mkdir()
    identity = retained.stat()
    old_entry = dict(old_lease["checkout_registrations"][0])
    old_entry.pop("cleanup", None)
    old_entry.update(execution_id="1" * 32, path=str(retained), device=identity.st_dev, inode=identity.st_ino)
    borrowed = dict(old_lease, checkout_registrations=[old_entry, *own["checkout_registrations"]], checkout_cleanup=own["checkout_cleanup"])
    queue.records.replace_record_at(queue.records.lease_path(key), borrowed)
    _recover(queue, clock, pool.LEASE_TIMEOUT_S + 60)
    _recover(queue, clock, 3600 + 2 * pool.LEASE_TIMEOUT_S + 600)
    state = _state(queue, key)
    assert not state["lease"] and not state["claimed"] and not state["late_finish"]
    assert state["held"] == []
    assert not retained.exists()


@pytest.mark.parametrize("append", [False, True])
def test_recovery_journal_resumes_a_changed_heartbeat_without_losing_debt(tmp_path, broker, monkeypatch, append):
    queue, key, first, _, stale_lease, clock = _predecessor_reaped(tmp_path, broker, monkeypatch, fence_s=3600.)
    raced, restore = _land_at_successor_lease_write(monkeypatch, first, stale_lease)
    original = pool.PoolQueue.cleanup_action_containers
    failures = []
    def fail_once(self, record, **kwargs):
        if kwargs.get("reason") == "stale-heartbeat" and not failures:
            failures.append(True)
            return {"complete": False, "error": "one failed cleanup"}
        return original(self, record, **kwargs)
    monkeypatch.setattr(pool.PoolQueue, "cleanup_action_containers", fail_once)
    assert _reclaim(queue)[0] == "claimed False"
    restore()
    assert failures and _state(queue, key)["late_finish"]
    lease = record_store.read_document(queue.records.lease_path(key))
    lease["heartbeat_unix"] += 1.
    extra = None
    if append:
        extra = tmp_path / "materialized" / (key[:12] + ".oldowned")
        extra.mkdir()
        identity = extra.stat()
        entry = dict(lease["checkout_registrations"][0])
        entry.pop("cleanup", None)
        entry.update(execution_id="2" * 32, path=str(extra), device=identity.st_dev, inode=identity.st_ino)
        lease["checkout_registrations"].append(entry)
    queue.records.replace_record_at(queue.records.lease_path(key), lease)
    queue.sweep_finish_tombstones(grace_s=0.)
    assert not _state(queue, key)["late_finish"] and not _state(queue, key)["lease"]
    assert extra is None or not extra.exists()
    assert _reclaim(queue)[0] == "claimed True"
