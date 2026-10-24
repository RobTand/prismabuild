"""Round-7 independent review probes for #1429 at 2d784d1d (Opus reviewer).

Naming:
  test_ok_*        claimed behaviour; expected to PASS on the head.
  test_blocking_*  an in-scope invariant; a FAIL on the head is a BLOCKING defect.
  test_nb_*        a NON-BLOCKING gap; a FAIL documents it.
Every probe prints its observations; nothing here touches a live host.
"""
import json
import os
import threading
import time
from pathlib import Path

import pytest

import lifetime_fixtures_1429
from prismabuild import claim_identity, lifetime_fence, materialize, pool, record_store, tier_holdings
from lifetime_fixtures_1429 import WORKER, claim, fenced_snapshot_queue
from test_lifetime_contract_1429 import _execute, _finish, broker, Skew, wait_for


def _refuse(*args, **kwargs):
    raise PermissionError("registered checkout retained by review fault")


def _uncertified(tmp_path, monkeypatch, *, lose):
    """An execute whose removal failed, then the claim or the lease is lost (r6 shape)."""
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


def _registration(owner):
    entry = owner["checkout_registrations"][0]
    return {k: v for k, v in entry.items() if k not in ("cleanup", "completion_claim")}


# -- B1: identity alone must not decide that a live attempt has ended ------------------

@pytest.mark.parametrize("route", ["queue-cleanup-then-ledger", "certifier-with-ledger"])
def test_blocking_identity_only_call_removes_a_running_tree_and_returns_its_tokens(tmp_path, broker, monkeypatch, route):
    """Exact identity, no evidence: the payload is still sleeping in its checkout."""
    monkeypatch.setattr(lifetime_fixtures_1429, "TASK_OK",
                        "import time\nopen('entered', 'w').write('yes')\ntime.sleep(3)\nopen('result', 'w').write('ok')\n")
    queue, action, cas = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    item = claim(queue)
    identity = {"attempt": item["attempts"], "claim_nonce": claim_identity.claim_nonce(item)}
    seen = {}
    original = pool.PoolQueue.write_lease

    def attack():
        lease = record_store.read_document(queue.records.lease_path(key)) or {}
        regs = lease.get("checkout_registrations") or []
        tree = Path(regs[0]["path"]) if regs and "path" in regs[0] else None
        assert wait_for(lambda: tree is not None and (tree / 'checkout' / 'entered').exists(), 30)
        seen.update(tree=tree, tree_before=bool(tree and tree.is_dir()),
                    held_before=queue.ledger().holder_tokens(key),
                    lease_owner=lease.get("claimed_by"), owner_lock=bool(tree and tree.with_name(tree.name + ".lock").exists()))
        try:
            if route == "queue-cleanup-then-ledger":
                cleaned = queue.queue.certify_checkout_cleanup(key, **identity)
                seen["cleanup"] = (cleaned.get("complete"), str(cleaned.get("error"))[:120])
                ledger = queue.ledger()
                seen["released"] = ledger.release(key, certificate=ledger.certify_release(key, **identity))
            else:
                ledger = queue.ledger()
                result = materialize.CheckoutCertifier(queue.root, ledger=ledger).certify_checkout_cleanup(key, **identity)
                seen["cleanup"] = (result.get("complete"), str(result.get("error"))[:120])
                seen["released"] = (ledger.release(key, certificate=result["certificate"])
                                    if "certificate" in result else "no certificate")
        except Exception as exc:                                 # noqa: BLE001
            seen["released"] = f"refused {type(exc).__name__}: {str(exc)[:160]}"
        live = record_store.read_document(queue.records.item_path(pool.CLAIMED, key)) or {}
        seen.update(tree_after=bool(tree and tree.is_dir()), held_after=queue.ledger().holder_tokens(key),
                    claim_live=live.get("claimed_by") == item["claimed_by"])

    def hooked(self, action_key, **kwargs):
        if kwargs.get("child_pid") is not None and "started" not in seen:
            seen["started"] = True
            runner = threading.Thread(target=attack, name="identity-only-caller")
            runner.start()
            runner.join(60)
            seen["joined"] = not runner.is_alive()
        return original(self, action_key, **kwargs)

    monkeypatch.setattr(pool.PoolQueue, "write_lease", hooked)
    outcome = _execute(queue, item)
    monkeypatch.setattr(pool.PoolQueue, "write_lease", original)
    try:
        _finish(queue, item, outcome)
        finished = "finished"
    except Exception as exc:                                     # noqa: BLE001
        finished = f"finish raised {type(exc).__name__}: {str(exc)[:120]}"
    print("identity-only", route, {k: v for k, v in seen.items() if k != "tree"},
          "| payload status", outcome.get("status"), "rc", outcome.get("returncode"),
          "| stderr", str(outcome.get("stderr") or "")[-160:], "|", finished)
    assert seen.get("tree_before") and seen.get("claim_live"), "probe did not reach a live registered execution"
    assert seen["tree_after"], f"[blocking] identity-only {route} removed a running attempt's checkout"
    assert outcome["status"] == "executed" and outcome["returncode"] == 0
    assert seen["held_after"] == seen["held_before"], (
        f"[blocking] identity-only {route} returned a running attempt's tokens: {seen['released']}")


def _alias_queue(tmp_path, queue):
    """A caller-chosen queue root whose reservations name the real token store."""
    alias = tmp_path / "alias-queue"
    fake = pool.PoolQueue(alias)
    fake.records.ensure_layout()
    reservations = alias / pool.RESERVATIONS
    if reservations.is_dir() and not reservations.is_symlink():
        reservations.rmdir()
    reservations.symlink_to(queue.root / pool.RESERVATIONS, target_is_directory=True)
    return fake


@pytest.mark.parametrize("lose", ["claim", "lease"])
def test_blocking_aliased_queue_root_supplies_custody_for_real_tokens(tmp_path, broker, monkeypatch, lose):
    queue, key, item, outcome, checkout = _uncertified(tmp_path, monkeypatch, lose=lose)
    ledger = queue.ledger()
    with pytest.raises(pool.PoolContractError):
        ledger.certify_release(key)                       # control: durable custody refuses
    held = ledger.holder_tokens(key)
    real = _owner(queue, key)
    forged = dict(real, checkout_registrations=[])
    forged.pop("checkout_cleanup", None)
    fake = _alias_queue(tmp_path, queue)
    for target in (fake.records.lease_path(key), fake.records.item_path(pool.CLAIMED, key)):
        fake.records.replace_record_at(target, forged)
    alias_ledger = fake.ledger(ledger.host)
    try:
        certificate = alias_ledger.certify_release(key)
        verdict = f"released {alias_ledger.release(key, certificate=certificate)}"
    except Exception as exc:                                 # noqa: BLE001
        verdict = f"refused {type(exc).__name__}: {str(exc)[:160]}"
    left = queue.ledger().holder_tokens(key)
    print("aliased root", lose, "->", verdict, "| real held before", held, "after", left,
          "| alias base", alias_ledger.base, "resolves", alias_ledger.base.resolve() == ledger.base.resolve(),
          "| tree", checkout.is_dir())
    assert checkout.is_dir()
    assert left == held, f"[blocking] a caller-chosen aliased queue root returned the real ledger's tokens ({lose} lost)"


@pytest.mark.parametrize("tree", ["in-place", "renamed"])
def test_nb_planted_unlink_journal_certifies_a_renamed_live_inode(tmp_path, broker, monkeypatch, tree):
    """The journal is queue JSON like the lease; only a name lookup re-derives it."""
    queue, key, item, outcome, checkout = _uncertified(tmp_path, monkeypatch, lose="claim")
    ledger = queue.ledger()
    held = ledger.holder_tokens(key)
    # Settle the scope first so this case isolates journal authority.
    queue.cleanup_action_containers(_owner(queue, key))
    registration = _registration(_owner(queue, key))
    journal = queue.root / "checkout-unlinks" / (registration["execution_id"] + ".json")
    journal.parent.mkdir(parents=True, exist_ok=True)
    queue.records.replace_record_at(journal, {"registration": registration, "ended_unix": time.time()})
    inode = checkout.stat().st_ino
    if tree == "renamed":
        moved = tmp_path / ("moved-" + checkout.name)
        os.rename(checkout, moved)
        checkout = moved
    try:
        verdict = f"released {ledger.release(key, certificate=ledger.certify_release(key))}"
    except Exception as exc:                                 # noqa: BLE001
        verdict = f"refused {type(exc).__name__}: {str(exc)[:160]}"
    left = ledger.holder_tokens(key)
    print("planted journal", tree, "->", verdict, "| held", held, "->", left,
          "| inode alive", checkout.is_dir() and checkout.stat().st_ino == inode)
    assert checkout.is_dir() and checkout.stat().st_ino == inode
    assert left == held, f"[nb] a planted unlink journal certified a live inode ({tree})"


def test_ok_planted_witness_and_module_defaults_cannot_mint(tmp_path, broker, monkeypatch):
    queue, key, item, outcome, checkout = _uncertified(tmp_path, monkeypatch, lose="claim")
    ledger = queue.ledger()
    held = ledger.holder_tokens(key)
    owner = _owner(queue, key)
    registration = _registration(owner)
    identity = {"attempt": owner["attempts"], "claim_nonce": claim_identity.claim_nonce(owner)}
    fd = os.open(checkout, os.O_RDONLY | os.O_DIRECTORY)
    materialize._CHECKOUT_WITNESSES[(queue.root, registration["execution_id"])] = (
        fd, registration, identity["attempt"], identity["claim_nonce"])
    materialize.CheckoutCertifier(queue.root)._journal_unlinked(key, **identity)
    journal = queue.root / "checkout-unlinks" / (registration["execution_id"] + ".json")
    outcomes = {"planted-witness-journal": journal.exists()}
    for name, value in (("LOCAL_CHECKOUT_ROOT-parent", checkout.parent.parent),
                        ("LOCAL_CHECKOUT_ROOT-tree", checkout)):
        with monkeypatch.context() as patched:
            patched.setattr(pool, "LOCAL_CHECKOUT_ROOT", value)
            try:
                ledger.certify_release(key, **identity)
                outcomes[name] = "minted"
            except pool.PoolContractError as exc:
                outcomes[name] = "refused: " + str(exc)[:80]
    signatures = {}
    import inspect
    for cls in (pool.ResourceLedger, tier_holdings.TierLedger, pool.PoolQueue, materialize.CheckoutCertifier):
        for name, member in vars(cls).items():
            if callable(member) and ("certif" in name or name == "__init__" and cls is materialize.CheckoutCertifier):
                signatures[f"{cls.__name__}.{name}"] = list(inspect.signature(member).parameters)
    print("witness/defaults", outcomes, "| signatures", signatures)
    assert not outcomes["planted-witness-journal"]
    assert all(str(v).startswith("refused") for k, v in outcomes.items() if k.startswith("LOCAL"))
    assert checkout.is_dir() and ledger.holder_tokens(key) == held


# -- B3: cross-thread stall before the publish, and liveness under contention ----------

def _state(queue, key):
    claimed = queue.records.dir(pool.CLAIMED)
    return {"claimed": queue.records.item_path(pool.CLAIMED, key).exists(),
            "lease": queue.records.lease_path(key).exists(),
            "late_finish": sorted(p.name[65:85] for p in claimed.iterdir()
                                  if p.name.startswith(key + ".") and p.name.endswith(".late-finish")),
            "done": queue.records.item_path(pool.DONE, key).exists(),
            "failed": queue.records.item_path(pool.FAILED, key).exists(),
            "held": sorted(p.name for p in queue.ledger().held_dir.iterdir())}


@pytest.mark.parametrize("landing", ["after-successor-execute", "after-successor-claim"])
def test_blocking_cross_thread_stalled_heartbeat_cannot_overwrite_the_successor(tmp_path, broker, monkeypatch, landing):
    """r6's R3 probe with the reaper and successor on their own thread, as in production."""
    queue, action, cas = fenced_snapshot_queue(tmp_path, fence_s=3600.0)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    first = claim(queue)
    _execute(queue, first)
    clock = Skew(monkeypatch)
    lease_path = queue.records.lease_path(key)
    real_replace = record_store.RecordStore.replace_record_at
    entered, resume = threading.Event(), threading.Event()
    seen, errors = {}, []

    def stalled(self, path, payload):
        if (threading.current_thread().name == "predecessor-beat" and Path(path) == lease_path
                and payload.get("claimed_unix") == first["claimed_unix"] and not entered.is_set()):
            entered.set()
            assert resume.wait(60)
        return real_replace(self, path, payload)

    def racer():
        try:
            clock.to(first["lifetime_deadline_unix"] + pool.LEASE_TIMEOUT_S + 1)
            seen["reaped"] = queue.reap_stale()
            seen["claim_after_reap"] = queue.records.item_path(pool.CLAIMED, key).exists()
            queue.publish(action_key=key, cas_root=cas.root, checkout_snapshot=first["checkout_snapshot"],
                          worker_script=WORKER, max_attempts=1)
            seen["second"] = queue.queue.claim(capacity={"cpu": 8, "mem_gb": 16},
                                               tags=[lifetime_fence.LIFETIME_TAG])
            if seen["second"] is not None and landing == "after-successor-execute":
                seen["outcome"] = _execute(queue, seen["second"])
        except Exception as exc:                             # noqa: BLE001
            errors.append(f"racer {type(exc).__name__}: {str(exc)[:160]}")

    def beat():
        try:
            queue.queue.write_lease(key, owner=str(first["claimed_by"]), claim_snapshot=first)
            seen["zombie"] = "published"
        except Exception as exc:                             # noqa: BLE001
            seen["zombie"] = f"raised {type(exc).__name__}: {str(exc)[:100]}"

    monkeypatch.setattr(record_store.RecordStore, "replace_record_at", stalled)
    predecessor = threading.Thread(target=beat, name="predecessor-beat")
    predecessor.start()
    assert entered.wait(30), "predecessor never reached its lease write"
    race = threading.Thread(target=racer, name="reaper-and-successor")
    race.start()
    race.join(3)
    seen["racer_blocked_while_stalled"] = race.is_alive()
    resume.set()
    predecessor.join(60)
    race.join(120)
    monkeypatch.setattr(record_store.RecordStore, "replace_record_at", real_replace)
    second = seen.get("second")
    if second is None:
        # The racer skipped a busy key; replay the ordinary sequence after the publish.
        clock.to(clock.now() + pool.LEASE_TIMEOUT_S + 1)
        queue.reap_stale()
        queue.publish(action_key=key, cas_root=cas.root, checkout_snapshot=first["checkout_snapshot"],
                      worker_script=WORKER, max_attempts=1)
        second = claim(queue)
        seen["second_after_replay"] = True
        if landing == "after-successor-execute":
            seen["outcome"] = _execute(queue, second)
    lease = record_store.read_document(lease_path) or {}
    current = record_store.read_document(queue.records.item_path(pool.CLAIMED, key)) or {}
    foreign = (lease.get("claimed_unix") == first["claimed_unix"]
               and current.get("claimed_unix") == second["claimed_unix"])
    try:
        queue.queue.write_lease(key, owner=str(first["claimed_by"]), claim_snapshot=first)
        late = "late predecessor beat published"
    except Exception as exc:                                 # noqa: BLE001
        late = f"late predecessor beat raised {type(exc).__name__}"
    try:
        queue.queue.write_lease(key, owner=str(second["claimed_by"]), claim_snapshot=second)
        beat_s = "successor heartbeat ok"
    except Exception as exc:                                 # noqa: BLE001
        beat_s = f"successor heartbeat raised {type(exc).__name__}: {str(exc)[:120]}"
    try:
        outcome = seen.get("outcome") or _execute(queue, second)
        _finish(queue, second, outcome)
        finished = "finished"
    except Exception as exc:                                 # noqa: BLE001
        finished = f"raised {type(exc).__name__}: {str(exc)[:160]}"
    state = _state(queue, key)
    print(landing, {k: v for k, v in seen.items() if k not in ("second", "outcome")}, "| errors", errors,
          "| foreign lease", foreign, "|", late, "|", beat_s, "| finish", finished, "| final", state)
    assert not errors
    assert not foreign, f"[blocking] a stalled predecessor heartbeat published over the successor ({landing})"
    assert beat_s == "successor heartbeat ok" and finished == "finished"
    assert state["done"] and not state["claimed"] and not state["lease"] and not state["held"]


@pytest.mark.parametrize("fence_s", [None, 3600.0], ids=["unfenced", "fenced"])
def test_nb_held_key_lock_starves_a_live_heartbeat_into_a_reap(tmp_path, broker, monkeypatch, fence_s):
    """Another holder keeps the key's transition lock past the lease timeout."""
    queue, action, cas = fenced_snapshot_queue(tmp_path, fence_s=fence_s)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    first = claim(queue)
    clock = Skew(monkeypatch)
    beat0 = (record_store.read_document(queue.records.lease_path(key)) or {}).get("heartbeat_unix")
    taken, release = threading.Event(), threading.Event()

    def holder():
        with queue.queue.action_transition_lock(key):
            taken.set()
            release.wait(60)

    thread = threading.Thread(target=holder, name="long-key-holder")
    thread.start()
    assert taken.wait(30)
    skipped = 0
    try:
        for step in range(12):                                # one beat every 30 s for 360 s
            clock.to(beat0 + (step + 1) * pool.HEARTBEAT_S)
            queue.queue.write_lease(key, owner=str(first["claimed_by"]), claim_snapshot=first)
            if (record_store.read_document(queue.records.lease_path(key)) or {}).get("heartbeat_unix") == beat0:
                skipped += 1
    finally:
        release.set()
        thread.join(30)
    try:
        reaped = queue.reap_stale()
    except Exception as exc:                                 # noqa: BLE001
        reaped = f"raised {type(exc).__name__}: {str(exc)[:120]}"
    state = _state(queue, key)
    print("contended heartbeat", "fence", fence_s, "| beats skipped", skipped, "of 12",
          "| reaped", reaped, "| claim still live", state["claimed"], "| worker pid alive", os.getpid())
    assert skipped == 12
    assert state["claimed"], "[nb] a live worker was reaped after another holder starved its heartbeat"




def test_dead_owner_still_recovers_after_contended_heartbeat(tmp_path, broker, monkeypatch):
    import subprocess
    import sys
    queue, action, _ = fenced_snapshot_queue(tmp_path, max_attempts=2, fence_s=3600.)
    key = action['action_key']
    item = claim(queue)
    lease_path = queue.records.lease_path(key)
    lease = record_store.read_document(lease_path)
    clock = Skew(monkeypatch)
    # Owned child models the worker identity; this test alone owns its lifetime.
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'])
    try:
        lease['pid'] = child.pid
        lease['pid_start_ticks'] = pool._contained_worker_start_ticks(child.pid)
        queue.records.replace_record_at(lease_path, lease)
        stamp = {**{f: lease.get(f) for f in claim_identity.CLAIM_IDENTITY_FIELDS},
                 **{f: lease[f] for f in ('action_key', 'host', 'pid', 'pid_start_ticks')},
                 'heartbeat_unix': pool._now()}
        queue.records.replace_record_at(queue.queue._heartbeat_stamp_path(lease), stamp)
        child.terminate()
        child.wait(timeout=10)
        assert queue.queue._heartbeat_cleanup_due(lease)
        clock.to(stamp['heartbeat_unix'] + pool.LEASE_TIMEOUT_S + 1)
        queue.reap_stale()
        assert not queue.records.item_path(pool.CLAIMED, key).exists()
        assert not queue.ledger().holder_tokens(key)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)


def test_identity_only_certification_refuses_a_live_claim_before_mkdir(tmp_path, broker):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    item = claim(queue)
    key = action['action_key']
    held = queue.ledger().holder_tokens(key)
    assert not queue.queue.certify_checkout_cleanup(key)['complete']
    with pytest.raises(pool.PoolContractError):
        queue.ledger().certify_release(key)
    assert queue.ledger().holder_tokens(key) == held


def test_a_returned_launcher_does_not_certify_a_live_broker_scope(tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    monkeypatch.setattr(pool, 'LOCAL_CHECKOUT_ROOT', tmp_path / 'materialized')
    item = claim(queue)
    _execute(queue, item)
    key = action['action_key']
    scope = queue.scope_for_record(item)
    broker.kernel.groups[scope.unit]['populated'] = True
    retired, mine = queue.queue._entomb_claim(key, expect=item)
    assert mine and retired is not None
    held = queue.ledger().holder_tokens(key)
    calls = list(broker.kernel.calls)
    assert not queue.queue.certify_checkout_cleanup(key)['complete']
    with pytest.raises(pool.PoolContractError):
        queue.ledger().certify_release(key)
    assert broker.kernel.calls == calls, 'certification must not stop a live kernel scope'
    assert queue.ledger().holder_tokens(key) == held
    assert queue.cleanup_action_containers(item)['complete']
    certificate = queue.ledger().certify_release(key)
    assert queue.ledger().release(key, certificate=certificate) == 2
