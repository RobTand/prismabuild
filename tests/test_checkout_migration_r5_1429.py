"""R5 capability, deployment and stale heartbeat regressions for #1429."""
import copy
import errno
import os
import pickle
import types
from pathlib import Path

import pytest

from prismabuild import lifetime_fence, materialize, pool, record_store, storage_tiers, tier_cycle
from prismabuild.materialize import ReleaseCertificate
from lifetime_fixtures_1429 import WORKER, claim, fenced_snapshot_queue
from test_lifetime_contract_1429 import _execute, _finish, broker, Skew
from legacy_custody_fixtures_1429 import stopped_legacy_hosts

canonical = materialize.pb._canonical_bytes


def _refuse(*args, **kwargs):
    raise PermissionError("registered checkout retained by review fault")


def _widowed(tmp_path, monkeypatch, *, fence_s=300.0):
    """r3's precondition: an uncertified checkout whose lease is the only custody."""
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


def _bound_fields(queue, ledger, key):
    owner_path = queue.records.lease_path(key)
    owner = record_store.read_document(owner_path)
    manifest = record_store.read_document(ledger.release_owner_path(key))
    return {
        "_authority": getattr(materialize, "_RELEASE_AUTHORITY", object()), "_base": ledger.base, "_holder": key,
        "_manifest": canonical(manifest), "_owner_path": owner_path,
        "_claim": canonical(lifetime_fence.attempt_identity(owner)),
        "_attempt": owner.get("attempts"), "_custody_key": key,
        "_registrations": canonical(owner.get("checkout_registrations")),
        "_token_names": None,
    }, manifest, owner_path, owner


def _fill(target, fields):
    for name, value in fields.items():
        object.__setattr__(target, name, value)
    return target


def _attempt_release(ledger, key, forge):
    try:
        certificate = forge()
    except Exception as exc:                                 # noqa: BLE001
        return f"forge-refused {type(exc).__name__}: {exc}"
    try:
        return f"released {ledger.release(key, certificate=certificate)}"
    except Exception as exc:                                 # noqa: BLE001
        return f"release-refused {type(exc).__name__}: {exc}"


# -- 1. C: the certificate capability --------------------------------------

INSCOPE = ["none", "object_new_bare", "copy", "deepcopy", "pickle", "subclass",
           "duck", "decoy_as_is"]
PRIVATE = ["module_sentinel_object_new", "instance_sentinel_constructor", "retarget_by_object_setattr"]


def _forger(variant, queue, ledger, key, genuine):
    fields, manifest, owner_path, owner = _bound_fields(queue, ledger, key)

    class Sub(ReleaseCertificate):
        __slots__ = ()

    return {
        "none": lambda: None,
        "object_new_bare": lambda: object.__new__(ReleaseCertificate),
        "copy": lambda: copy.copy(genuine),
        "deepcopy": lambda: copy.deepcopy(genuine),
        "pickle": lambda: pickle.loads(pickle.dumps(genuine)),
        "subclass": lambda: _fill(object.__new__(Sub), fields),
        "duck": lambda: types.SimpleNamespace(**fields, permits=lambda name: True),
        "decoy_as_is": lambda: genuine,
        "module_sentinel_object_new": lambda: _fill(object.__new__(ReleaseCertificate), fields),
        "instance_sentinel_constructor": lambda: type(genuine)(
            genuine._authority, ledger, key, manifest, owner_path, owner, None, key),
        "retarget_by_object_setattr": lambda: _fill(genuine, fields),
    }[variant]


@pytest.mark.parametrize("variant", INSCOPE + PRIVATE)
def test_forged_certificate_cannot_release_uncertified_custody(tmp_path, broker, monkeypatch, variant):
    queue, key, item, outcome, checkout, _ = _widowed(tmp_path, monkeypatch)
    ledger = queue.ledger()
    assert ledger.acquire("decoy", {"cpu": 1})
    genuine = ledger.certify_release("decoy")
    verdict = _attempt_release(ledger, key, lambda: _forger(variant, queue, ledger, key, genuine)())
    held = ledger.holder_tokens(key)
    print("forgery", variant, "->", verdict, "| held", held, "| checkout", checkout.is_dir())
    expected = "nb" if variant in PRIVATE else "ok"
    assert checkout.is_dir()
    assert held, f"[{expected}] forged certificate ({variant}) returned an uncertified checkout's tokens"




def test_getattr_mint_cannot_release_uncertified_custody(tmp_path, broker, monkeypatch):
    queue, key, item, outcome, checkout, _ = _widowed(tmp_path, monkeypatch)
    ledger = queue.ledger()
    owner_path = queue.records.lease_path(key)
    owner = record_store.read_document(owner_path)
    manifest = record_store.read_document(ledger.release_owner_path(key))
    constructor = getattr(materialize, "Release" + "Certificate")
    verdict = _attempt_release(ledger, key, lambda: constructor(
        getattr(materialize, "_RELEASE_AUTHORITY", object()), ledger, key,
        manifest, owner_path, owner, None, key))
    assert ledger.holder_tokens(key), verdict
    assert checkout.is_dir()


def _pre_contract(ledger, holder):
    import shutil
    ledger.release_owner_path(holder).unlink()
    shutil.rmtree(ledger.base / "release-receipts" / holder, ignore_errors=True)


def _legacy_fixture(tmp_path, *, snapshot=False, lease=True):
    import socket
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    key = "e" * 64
    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 2, "mem_gb": 2})
    assert ledger.acquire(key, {"cpu": 1})
    _pre_contract(ledger, key)
    owner = {"action_key": key, "claimed_by": "old-worker", "claimed_host": socket.gethostname(),
             "claimed_unix": 10., "published_unix": 1., "attempts": 0}
    if snapshot:
        owner["checkout_snapshot"] = {"unknown": "legacy snapshot"}
    queue.records.replace_record_at(queue.records.item_path(pool.CLAIMED, key), owner)
    if lease:
        queue.records.replace_record_at(queue.records.lease_path(key), {
            "action_key": key, "owner": owner["claimed_by"], "host": owner["claimed_host"],
            "claimed_unix": 10., "published_unix": 1., "heartbeat_unix": 20., "pid": 12345})
    return queue, key, ledger


def _adopt_inventory(queue, *, stopped=True):
    from prismabuild.legacy_custody import LegacyCustodyMigration
    migration = LegacyCustodyMigration(queue)
    inventory = migration.inventory()
    return migration.apply(inventory, writers_stopped=stopped)


@pytest.mark.parametrize("lease", [False, True])
def test_legacy_hold_and_envelopeless_lease_adopt_then_return(tmp_path, lease):
    queue, key, ledger = _legacy_fixture(tmp_path, lease=lease)
    rows = _adopt_inventory(queue)
    assert any(r["status"] == "adopted" and r["holder"] == key for r in rows)
    assert ledger.holder_tokens(key) == {"cpu": 1}, "adoption must not release a token"
    assert queue._release_reservation(key, host=ledger.host) == 1
    assert ledger.held_keys() == []
    if lease:
        assert record_store.read_document(queue.records.lease_path(key))["checkout_registrations"] == []


def test_legacy_snapshot_without_cleanup_proof_stays_visible(tmp_path):
    from prismabuild.legacy_custody import LegacyCustodyMigration
    queue, key, ledger = _legacy_fixture(tmp_path, snapshot=True)
    migration = LegacyCustodyMigration(queue)
    rows = migration.apply(migration.inventory(), writers_stopped=True)
    assert rows[0]["status"] == "retained"
    assert "checkout" in rows[0]["reason"]
    assert ledger.holder_tokens(key) == {"cpu": 1}
    assert not ledger.release_owner_path(key).exists()
    assert any(r["holder"] == key and r["status"] == "retained" for r in migration.inventory()["holds"])
    import pbstatus
    lines = pbstatus.legacy_custody_lines(migration.inventory()["holds"])
    assert any(key[:12] in line and "retained" in line for line in lines)


def test_legacy_adoption_requires_stopped_writers_and_exact_inventory(tmp_path):
    from prismabuild.legacy_custody import LegacyCustodyMigration
    queue, key, ledger = _legacy_fixture(tmp_path)
    migration = LegacyCustodyMigration(queue)
    inventory = migration.inventory()
    with pytest.raises(pool.PoolContractError, match="stopped"):
        migration.apply(inventory, writers_stopped=False)
    token = next(p for p in (ledger.held_dir / key).iterdir() if p.name.startswith("cpu-"))
    token.rename(token.with_suffix(".replaced"))
    token.touch()
    rows = migration.apply(inventory, writers_stopped=True)
    assert rows[0]["status"] == "retained"
    assert not ledger.release_owner_path(key).exists()


def test_legacy_tier_and_ram_mirror_adopt_without_releasing_occupancy(tmp_path):
    from prismabuild import tier_holdings
    queue, key, host_ledger = _legacy_fixture(tmp_path)
    queue.ledger("storage").ensure_capacity({"mem_gb": 4})
    tier = queue.tier("ram:storage")
    tier.grow_supply({"ram_gib": 2})
    assert tier.take(key, {"ram_gib": 1})
    _pre_contract(tier._ledger, key)
    mirror = storage_tiers.RAM_HOST_MEMORY_PREFIX + key
    mirror_ledger = queue.ledger("storage")
    _pre_contract(mirror_ledger, mirror)
    rows = _adopt_inventory(queue)
    assert all(r["status"] == "adopted" for r in rows)
    assert tier.holdings(key) == {"ram_gib": 1}
    assert tier_holdings.egress_all(queue, key) == 1
    assert tier.holdings(key) == {}
    assert mirror_ledger.holder_tokens(mirror) == {}


def test_legacy_inventory_cannot_adopt_post_contract_custody(tmp_path):
    from prismabuild.legacy_custody import LegacyCustodyMigration
    queue, key, ledger = _legacy_fixture(tmp_path)
    migration = LegacyCustodyMigration(queue)
    plan = migration.inventory()
    ledger._register_reservation(key, "new-incarnation")
    rows = migration.apply(plan, writers_stopped=True)
    assert rows[0]["status"] == "retained"
    assert ledger.holder_tokens(key) == {"cpu": 1}


# -- PERSISTED_RETRY: realistic triggers ---------------------------------------


def _state(queue, key):
    return {"ready": queue.records.item_path(pool.READY, key).exists(),
            "claimed": queue.records.item_path(pool.CLAIMED, key).exists(),
            "lease": queue.records.lease_path(key).exists(),
            "held": sorted(p.name for p in queue.ledger().held_dir.iterdir())}


def test_ok_lease_write_io_failure_rolls_back_cleanly(tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    original = pool.PoolQueue.write_lease
    calls = []

    def failing(self, action_key, **kwargs):
        if not calls:
            calls.append(action_key)
            raise OSError(errno.EDQUOT, os.strerror(errno.EDQUOT))
        return original(self, action_key, **kwargs)

    monkeypatch.setattr(pool.PoolQueue, "write_lease", failing)
    try:
        item = queue.queue.claim(capacity={"cpu": 8, "mem_gb": 16}, tags=[lifetime_fence.LIFETIME_TAG])
        verdict = f"claimed {item is not None}"
    except Exception as exc:                                 # noqa: BLE001
        verdict = f"raised {type(exc).__name__}: {exc}"
    monkeypatch.setattr(pool.PoolQueue, "write_lease", original)
    state = _state(queue, key)
    print("lease-edquot", verdict, state)
    assert calls and verdict == "claimed False"
    assert state == {"ready": True, "claimed": False, "lease": False, "held": []}
    assert claim(queue)["action_key"] == key


def test_nb_zombie_heartbeat_lease_cannot_wedge_successor(tmp_path, broker, monkeypatch):
    # Leave the successor enough lifetime for a heartbeat-bound recovery.
    queue, action, cas = fenced_snapshot_queue(tmp_path, fence_s=3600.)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    first = claim(queue)
    _execute(queue, first)                              # certified removal
    stale = record_store.read_document(queue.records.lease_path(key))
    clock = Skew(monkeypatch)
    clock.to(first["lifetime_deadline_unix"] + pool.LEASE_TIMEOUT_S + 1)
    queue.reap_stale()                                  # the reaper concludes the paused claim
    assert _state(queue, key) == {"ready": False, "claimed": False, "lease": False, "held": []}
    queue.publish(action_key=key, cas_root=cas.root, checkout_snapshot=first["checkout_snapshot"],
                  worker_script=WORKER, max_attempts=1)
    original = pool.PoolQueue.write_lease
    raced = []

    def racing(self, action_key, **kwargs):
        snapshot = kwargs.get("claim_snapshot") or {}
        if not raced and snapshot.get("claimed_unix") != first.get("claimed_unix"):
            raced.append(action_key)
            # The predecessor's paused heartbeat read its live claim before
            # the reaper ran in a pre-contract writer; its
            # write lands after the successor passed the claim gate.
            self.records.replace_record_at(self.records.lease_path(action_key), stale)
        return original(self, action_key, **kwargs)

    monkeypatch.setattr(pool.PoolQueue, "write_lease", racing)
    try:
        second = queue.claim(tags=[lifetime_fence.LIFETIME_TAG])
        verdict = f"claimed {second is not None}"
    except Exception as exc:                                 # noqa: BLE001
        verdict = f"raised {type(exc).__name__}: {exc}"
    monkeypatch.setattr(pool.PoolQueue, "write_lease", original)
    print("zombie", "raced", bool(raced), verdict, _state(queue, key))
    # Recovery past every lease, lifetime and acquisition deadline.
    clock.to(clock.now() + 2 * pool.LEASE_TIMEOUT_S + 600)
    errors = []
    for _ in range(3):
        for step in (queue.reap_stale, lambda: queue.sweep_widowed_leases(timeout_s=pool.LEASE_TIMEOUT_S),
                     lambda: queue.ledger().sweep_stale_acquisitions()):
            try:
                step()
            except Exception as exc:                         # noqa: BLE001
                errors.append(f"{type(exc).__name__}: {exc}")
    state = _state(queue, key)
    try:
        again = queue.claim(tags=[lifetime_fence.LIFETIME_TAG])
        reclaim = f"claimed {again is not None}"
    except Exception as exc:                                 # noqa: BLE001
        reclaim = f"raised {type(exc).__name__}: {exc}"
    print("zombie-after-recovery", state, "errors", sorted(set(errors))[:3], "| reclaim", reclaim)
    assert raced
    assert reclaim == "claimed True", "[nb] a zombie lease wedged the successor's claim and reservation"


@pytest.mark.parametrize("body", [
    'getattr(materialize, "Release" + "Certificate")(None)',
    'object.__new__(ReleaseCertificate)',
    'materialize._RELEASE_AUTHORITY',
    'certificate._authority',
    'getattr(materialize, "_ISSUED_RELEASE_CERTIFICATES")',
])
def test_mint_guard_catches_getattr_and_private_authority(tmp_path, body):
    import shutil
    root = Path(__file__).resolve().parents[1]
    target = tmp_path / "guard-tree"
    directory = target / "src/prismabuild"
    directory.mkdir(parents=True)
    shutil.copyfile(root / "src/prismabuild/materialize.py", directory / "materialize.py")
    (directory / "mutant.py").write_text('def bypass():\n    return ' + body + '\n')
    test_path = target / "tests/guard.py"
    test_path.parent.mkdir()
    source = (root / "tests/test_checkout_certification_1429.py").read_text()
    namespace = {"__file__": str(test_path)}
    exec(compile(source, str(test_path), "exec"), namespace)
    with pytest.raises(AssertionError):
        namespace["test_scope_token_returns_have_one_conclusion_boundary"]()


def test_forged_handle_cannot_return_through_private_helper(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 1})
    assert ledger.acquire("owned", {"cpu": 1})
    certificate = ledger.certify_release("owned")
    assert not hasattr(certificate, "_authority")
    fake = object.__new__(materialize.ReleaseCertificate)
    with pytest.raises(pool.PoolContractError, match="certificate"):
        ledger._empty_into_free(ledger.held_dir / "owned", certificate=fake)
    assert ledger.holder_tokens("owned") == {"cpu": 1}
    binding = materialize.ReleaseCertificate.require(certificate, ledger, "owned")
    with pytest.raises((AttributeError, TypeError)):
        object.__setattr__(binding, "holder", "foreign")
    assert ledger.release("owned", certificate=certificate) == 1


def test_issuance_registry_does_not_retain_dead_handles(tmp_path):
    import gc
    import weakref
    ledger = pool.PoolQueue(tmp_path / "queue").ledger()
    ledger.ensure_capacity({"cpu": 1})
    assert ledger.acquire("owned", {"cpu": 1})
    certificate = ledger.certify_release("owned")
    reference = weakref.ref(certificate)
    del certificate
    gc.collect()
    assert reference() is None


@pytest.mark.parametrize("stage", ["journal", "mirror", "manifest"])
def test_legacy_adoption_resumes_exact_inventory_after_crash(tmp_path, monkeypatch, stage):
    from prismabuild.legacy_custody import LegacyCustodyMigration
    queue, key, ledger = _legacy_fixture(tmp_path)
    migration = LegacyCustodyMigration(queue)
    plan = migration.inventory()
    original = queue.records.replace_record_at
    once = []
    def interrupt(path, value):
        original(path, value)
        matches = ((stage == "journal" and path == ledger.legacy_owner_path(key))
                   or (stage == "mirror" and path == queue.records.item_path(pool.CLAIMED, key))
                   or (stage == "manifest" and path == ledger.release_owner_path(key)))
        if matches and not once:
            once.append(True)
            raise OSError("crash after durable write")
    monkeypatch.setattr(queue.records, "replace_record_at", interrupt)
    assert migration.apply(plan, writers_stopped=True)[0]["status"] == "retained"
    monkeypatch.setattr(queue.records, "replace_record_at", original)
    assert migration.apply(plan, writers_stopped=True)[0]["status"] == "adopted"
    assert migration.apply(plan, writers_stopped=True)[0]["status"] == "adopted"
    assert ledger.holder_tokens(key) == {"cpu": 1}
    assert queue._release_reservation(key, host=ledger.host) == 1


def test_legacy_terminal_adoption_preserves_immutable_bytes(tmp_path):
    from prismabuild.legacy_custody import LegacyCustodyMigration
    queue, key, ledger = _legacy_fixture(tmp_path, snapshot=True, lease=False)
    live = queue.records.item_path(pool.CLAIMED, key)
    owner = record_store.read_document(live)
    owner.update(status="executed", detail={"returncode": 0})
    terminal = queue.records.item_path(pool.DONE, key)
    queue.records.replace_record_at(terminal, owner)
    live.unlink()
    before = terminal.read_bytes()
    migration = LegacyCustodyMigration(queue)
    assert migration.apply(migration.inventory(), writers_stopped=True)[0]["status"] == "adopted"
    assert terminal.read_bytes() == before
    assert queue._release_reservation(key, host=ledger.host) == 1


def test_ownerless_legacy_hold_keeps_custody_and_is_visible(tmp_path):
    from prismabuild.legacy_custody import LegacyCustodyMigration
    queue, key, ledger = _legacy_fixture(tmp_path, lease=False)
    queue.records.item_path(pool.CLAIMED, key).unlink()
    migration = LegacyCustodyMigration(queue)
    result = migration.apply(migration.inventory(), writers_stopped=True)
    assert result[0]["status"] == "retained"
    assert "no unique durable" in result[0]["reason"]
    assert ledger.holder_tokens(key) == {"cpu": 1}
    assert migration.inventory()["holds"]


@pytest.mark.parametrize("host,alive,observed,beat,due", [
    ("local", True, 42, 100., False),
    ("local", False, None, 100., True),
    ("local", True, 43, 100., True),
    ("remote", False, None, 100., False),
    ("remote", True, 42, -10000., True),
    ("local", True, None, 100., False),
])
def test_heartbeat_recovery_requires_age_or_exact_process_death(monkeypatch, host, alive, observed, beat, due):
    monkeypatch.setattr(pool, "_now", lambda: 100.)
    monkeypatch.setattr(pool.socket, "gethostname", lambda: "local")
    monkeypatch.setattr(pool, "_process_alive", lambda pid: alive)
    monkeypatch.setattr(pool, "_contained_worker_start_ticks", lambda pid: observed)
    lease = {"pid": 123, "pid_start_ticks": 42, "host": host, "heartbeat_unix": beat}
    assert pool.PoolQueue._heartbeat_cleanup_due(lease) is due
    if beat == 100.:
        del lease["pid_start_ticks"]
        assert not pool.PoolQueue._heartbeat_cleanup_due(lease), "PID alone is not owner identity"



def test_legacy_operator_cli_inventory_and_resumable_apply(tmp_path):
    import json
    import subprocess
    import sys
    queue, key, ledger = _legacy_fixture(tmp_path)
    tool = Path(__file__).resolve().parents[1] / "tools/maintenance/adopt_legacy_custody.py"
    plan = tmp_path / "legacy.json"
    command = [sys.executable, str(tool), "--queue-root", str(queue.root)]
    result = subprocess.run(command + ["--inventory", str(plan)], capture_output=True, text=True)
    assert result.returncode == 2 and not plan.exists()
    result = subprocess.run(command + ["--inventory", str(plan), "--writers-stopped"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(plan.read_bytes())["holds"]
    for index in range(2):
        report = tmp_path / f"result-{index}.json"
        result = subprocess.run(command + ["--apply", str(plan), "--writers-stopped", "--report", str(report)], capture_output=True, text=True)
        assert result.returncode == 2, result.stderr
        assert all(row["status"] == "retained" for row in json.loads(report.read_bytes()))
    # A flag alone cannot prove a stopped real fleet. The isolated verified
    # host fixture exercises the same adoption and idempotence in-process.
    assert all(row["status"] == "adopted" for row in _adopt_inventory(queue))

    assert ledger.holder_tokens(key) == {"cpu": 1}
    assert queue._release_reservation(key, host=ledger.host) == 1



def test_stale_foreign_heartbeat_retains_registered_checkout_until_certified(tmp_path, broker, monkeypatch):
    queue, key, first, outcome, checkout, _ = _widowed(tmp_path, monkeypatch)
    predecessor = record_store.read_document(queue.records.lease_path(key))
    successor = dict(first, claimed_by="successor", claimed_unix=first["claimed_unix"] + 1,
                     checkout_registrations=[])
    live_path = queue.records.item_path(pool.CLAIMED, key)
    queue.records.replace_record_at(live_path, successor)
    Skew(monkeypatch).to(predecessor["heartbeat_unix"] + pool.LEASE_TIMEOUT_S + 1)
    with queue.action_transition_lock(key):
        assert not queue._recover_stale_heartbeat_lease(key, successor)
    assert checkout.is_dir()
    assert queue.ledger().holder_tokens(key)
    assert record_store.read_document(queue.records.lease_path(key))["owner"] == predecessor["owner"]
    live = record_store.read_document(live_path)
    assert live["claimed_by"] == "successor"
    assert not live.get("container_cleanup_pending"), "an unproved current owner must remain untouched"


def test_legacy_lease_without_attempt_envelope_is_retained_visibly(tmp_path):
    from prismabuild.legacy_custody import LegacyCustodyMigration
    queue, key, ledger = _legacy_fixture(tmp_path)
    queue.records.item_path(pool.CLAIMED, key).unlink()
    migration = LegacyCustodyMigration(queue)
    result = migration.apply(migration.inventory(), writers_stopped=True)
    assert result[0]["status"] == "retained"
    assert "exact attempt envelope" in result[0]["reason"]
    assert ledger.holder_tokens(key) == {"cpu": 1}
    assert queue.records.lease_path(key).exists()



def test_pbstatus_json_exposes_unqualified_legacy_hold(tmp_path, monkeypatch, capsys):
    import json
    import pbstatus
    queue, key, ledger = _legacy_fixture(tmp_path, snapshot=True)
    def immediate(section, read, **kwargs):
        try:
            return {"status": "ok", "value": read()}
        except Exception as exc:
            return {"status": "error", "type": type(exc).__name__, "error": str(exc)}
    monkeypatch.setattr(pbstatus, "bounded", immediate)
    pbstatus.main(["--transport", "pool", "--queue-root", str(queue.root), "--json", "--recent", "0"])
    value = json.loads(capsys.readouterr().out)
    rows = value["legacy_custody"]
    assert rows and rows[0]["holder"] == key and rows[0]["status"] == "retained"
    assert ledger.holder_tokens(key) == {"cpu": 1}



def test_foreign_heartbeat_cannot_borrow_an_older_same_claim_journal(tmp_path, broker, monkeypatch):
    queue, key, first, outcome, checkout, _ = _widowed(tmp_path, monkeypatch)
    lease = record_store.read_document(queue.records.lease_path(key))
    successor = dict(first, claimed_by="successor", claimed_unix=first["claimed_unix"] + 1,
                     checkout_registrations=[])
    queue.records.replace_record_at(queue.records.item_path(pool.CLAIMED, key), successor)
    journal = dict(lease, claimed_by=lease["owner"], claimed_host=lease["host"], checkout_registrations=[])
    queue.records.replace_record_at(queue.records.late_finish_path(journal), journal)
    Skew(monkeypatch).to(lease["heartbeat_unix"] + pool.LEASE_TIMEOUT_S + 1)
    calls = []
    def cleanup(*args, **kwargs):
        calls.append(True)
        return {"complete": True}
    monkeypatch.setattr(queue.queue, "cleanup_action_containers", cleanup)
    with queue.action_transition_lock(key):
        assert not queue._recover_stale_heartbeat_lease(key, successor)
    assert not calls, "an old journal must not substitute for the surviving lease's newer registration list"
    assert checkout.is_dir() and queue.ledger().holder_tokens(key)
