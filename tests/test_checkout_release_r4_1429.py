"""#1429 release certification regressions adopted from independent r3 review."""
import ast
import errno
import importlib.util
import json
import os
import shutil
import sys
import time
from collections import Counter
from pathlib import Path

import pytest

from prismabuild import lifetime_fence, materialize, owned_tree, pool, record_store
from lifetime_fixtures_1429 import WORKER, claim, fenced_snapshot_queue, wait_for
from test_lifetime_contract_1429 import _execute, _finish, broker, Skew

ROOT = Path(__file__).resolve().parents[1]


def _refuse(*args, **kwargs):
    raise PermissionError("registered checkout retained by review fault")


def _deny_checkout_creation(monkeypatch, root, how):
    root.mkdir()
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", root)
    real = materialize.os.mkdir
    if how == "eacces":
        if os.geteuid() == 0:
            pytest.skip("root ignores directory permission bits")
        root.chmod(0o555)

        def restore():
            root.chmod(0o755)
    else:
        def full(path, mode=0o777, *, dir_fd=None):
            if dir_fd is not None and Path(os.readlink(f"/proc/self/fd/{dir_fd}")) == root:
                raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))
            return real(path, mode, dir_fd=dir_fd)

        monkeypatch.setattr(materialize.os, "mkdir", full)

        def restore():
            monkeypatch.setattr(materialize.os, "mkdir", real)
    return restore


# -- A: a checkout that was never created strands its reservation forever --


@pytest.mark.parametrize("fence_s", [300.0, None], ids=["fenced", "unfenced"])
@pytest.mark.parametrize("how", ["eacces", "enospc"])
def test_blocking_checkout_creation_failure_does_not_strand_reservation(
        tmp_path, broker, monkeypatch, fence_s, how):
    queue, action, _ = fenced_snapshot_queue(tmp_path, fence_s=fence_s)
    key = action["action_key"]
    root = tmp_path / "materialized"
    restore = _deny_checkout_creation(monkeypatch, root, how)
    with pytest.raises(OSError):
        queue.serve_once(tags=[lifetime_fence.LIFETIME_TAG], containment=True)
    claimed = queue.records.item_path(pool.CLAIMED, key)
    custody = record_store.read_document(claimed)
    entries = (custody or {}).get("checkout_registrations") or []
    print("creation-failure", how, "fence", fence_s,
          "claim_retained", custody is not None,
          "entry_fields", [sorted(e) for e in entries],
          "cleanup_error", ((custody or {}).get("checkout_cleanup") or {}).get("error"),
          "held", queue.ledger().held_keys(),
          "trees", sorted(p.name for p in root.iterdir()))
    # Nothing was ever created. Let the fault clear and run recovery past
    # every lease and lifetime deadline: the reservation must come back.
    restore()
    Skew(monkeypatch).to(time.time() + 300.0 + pool.LEASE_TIMEOUT_S + 60)
    def recovered():
        queue.reap_stale()
        queue.sweep_widowed_leases(timeout_s=pool.LEASE_TIMEOUT_S)
        return queue.ledger().held_keys() == []

    wait_for(recovered, timeout_s=5.0)
    after = record_store.read_document(claimed)
    print("after-recovery", "claim_retained", after is not None,
          "cleanup_error", ((after or {}).get("checkout_cleanup") or {}).get("error"),
          "held", queue.ledger().held_keys())
    assert queue.ledger().held_keys() == [], (
        "an execute that failed before mkdtemp created anything holds its reservation forever")
    assert after is None


# -- B: a successor claim's rollback deletes widowed custody and returns its tokens --


@pytest.mark.parametrize("fence_s", [300.0, None], ids=["fenced", "unfenced"])
def test_blocking_successor_claim_rollback_keeps_widowed_checkout_custody(
        tmp_path, broker, monkeypatch, fence_s):
    queue, action, cas = fenced_snapshot_queue(tmp_path, fence_s=fence_s)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", _refuse)
    item = claim(queue)
    outcome = _execute(queue, item)
    checkout = Path(outcome["checkout_cleanup"]["path"])
    assert checkout.is_dir() and outcome["checkout_cleanup"]["complete"] is False
    # The same widowed-lease precondition as r2 finding 3 and the builder's
    # own regressions: the claim is gone and the lease is the custody owner.
    queue.records.item_path(pool.CLAIMED, key).rename(tmp_path / "lost-claim.json")
    assert queue.ledger().held_keys() == [key]
    # An explicit resubmission publishes a new generation. ``claim_blocked``
    # considers <key>.json, tombstones and late-finish marks, not a lease.
    queue.publish(action_key=key, cas_root=cas.root,
                  checkout_snapshot=item["checkout_snapshot"],
                  worker_script=WORKER, max_attempts=1)
    second = queue.claim(tags=[lifetime_fence.LIFETIME_TAG])
    lease = record_store.read_document(queue.records.lease_path(key))
    print("successor", "claimed", second is not None,
          "lease_present", lease is not None,
          "lease_paths", [e.get("path") for e in (lease or {}).get("checkout_registrations", [])],
          "ready_present", queue.records.item_path(pool.READY, key).exists(),
          "held", queue.ledger().held_keys(),
          "checkout_survives", checkout.is_dir())
    assert checkout.is_dir()
    assert lease is not None and any(
        e.get("path") == str(checkout) for e in lease.get("checkout_registrations", [])), (
        "successor claim rollback deleted the only custody record of a surviving checkout")
    assert queue.ledger().held_keys() == [key], (
        "successor claim rollback returned an uncertified checkout's host tokens")


# -- C: same-UID races during removal (expected to PASS: verified correct) --


@pytest.mark.parametrize("entry", ["finish", "reap"])
def test_swap_between_name_check_and_rmdir_cannot_certify(tmp_path, broker, monkeypatch, entry):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    root = tmp_path / "materialized"
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", root)
    real_rmdir = os.rmdir
    kept = []

    def swap_then_rmdir(path, *, dir_fd=None):
        name = os.fspath(path)
        if (dir_fd is not None and not kept and materialize.CHECKOUT_TREE.fullmatch(name)
                and Path(os.readlink(f"/proc/self/fd/{dir_fd}")) == root):
            # Same-UID racer: after remove_tree's name check, before rmdir.
            os.rename(name, name + ".kept", src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
            os.mkdir(name, dir_fd=dir_fd)
            kept.append(root / (name + ".kept"))
        return real_rmdir(path, dir_fd=dir_fd)

    monkeypatch.setattr(owned_tree.os, "rmdir", swap_then_rmdir)
    item = claim(queue)
    outcome = _execute(queue, item)
    proof = outcome["checkout_cleanup"]
    assert kept, "the race window was not exercised"
    info = kept[0].stat()
    assert (info.st_dev, info.st_ino) == (proof["device"], proof["inode"])
    assert proof["complete"] is False and not proof.get("unlinked")
    if entry == "finish":
        _finish(queue, item, outcome)
    else:
        Skew(monkeypatch).to(item["lifetime_deadline_unix"] + pool.LEASE_TIMEOUT_S + 1)
        queue.reap_stale()
    assert kept[0].is_dir()
    assert queue.ledger().held_keys() == [key]


@pytest.mark.parametrize("attack", ["parent-symlink-decoy", "name-symlink-to-moved-original",
                                    "original-renamed-away"])
def test_recovery_after_ancestor_or_name_swap_cannot_certify(tmp_path, broker, monkeypatch, attack):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    root = tmp_path / "materialized"
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", root)
    original = materialize.owned_tree.remove_tree
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", _refuse)
    item = claim(queue)
    outcome = _execute(queue, item)
    registered = Path(outcome["checkout_cleanup"]["path"])
    identity = (outcome["checkout_cleanup"]["device"], outcome["checkout_cleanup"]["inode"])
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", original)
    if attack == "parent-symlink-decoy":
        real = tmp_path / "materialized.real"
        root.rename(real)
        decoy = tmp_path / "decoy"
        decoy.mkdir()
        (decoy / registered.name).mkdir()
        root.symlink_to(decoy)
        survivor = real / registered.name
    elif attack == "name-symlink-to-moved-original":
        survivor = tmp_path / "elsewhere"
        registered.rename(survivor)
        registered.symlink_to(survivor)
    else:
        survivor = tmp_path / "elsewhere"
        registered.rename(survivor)
    Skew(monkeypatch).to(item["lifetime_deadline_unix"] + pool.LEASE_TIMEOUT_S + 1)
    queue.reap_stale()
    info = survivor.stat()
    durable = record_store.read_document(queue.records.item_path(pool.CLAIMED, key)) or {}
    print("attack", attack, "error", (durable.get("checkout_cleanup") or {}).get("error"))
    assert (info.st_dev, info.st_ino) == identity
    assert queue.ledger().held_keys() == [key]


def test_hard_link_keeps_only_a_file_not_the_registered_directory(tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    original = materialize.owned_tree.remove_tree
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", _refuse)
    item = claim(queue)
    outcome = _execute(queue, item)
    registered = Path(outcome["checkout_cleanup"]["path"])
    with pytest.raises(OSError):
        os.link(registered, tmp_path / "directory-link")
    task = next(registered.rglob("task.py"))
    os.link(task, tmp_path / "file-link")
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", original)
    Skew(monkeypatch).to(item["lifetime_deadline_unix"] + pool.LEASE_TIMEOUT_S + 1)
    wait_for(lambda: (queue.reap_stale(), queue.ledger().held_keys() == [])[1], timeout_s=5.0)
    assert not registered.exists() and (tmp_path / "file-link").is_file()
    print("hard-link", "held", queue.ledger().held_keys())
    assert queue.ledger().held_keys() == []




@pytest.mark.parametrize("primitive", ["release", "release_count", "release_kinds", "release_except", "egress", "release_unspent"])
def test_release_primitives_refuse_without_certificate(tmp_path, primitive):
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 1})
    assert ledger.acquire("owned", {"cpu": 1})
    if primitive in {"egress", "release_unspent"}:
        from prismabuild import tier_holdings
        target = queue.tier("ram:test")
        args = {"authority": tier_holdings.Unspent.NEVER_STARTED} if primitive == "release_unspent" else {}
    else:
        target = ledger
        args = {}
    positional = ({"cpu": 1},) if primitive == "release_count" else ({"cpu"},) if primitive in {"release_kinds", "release_except"} else ()
    with pytest.raises((pool.PoolContractError, TypeError), match="certificate"):
        getattr(target, primitive)("owned", *positional, **args)
    assert ledger.held_keys() == ["owned"]


@pytest.mark.parametrize("mutation", ["queue", "alias", "tier"])
def test_reviewer_direct_release_mutant_is_refused(tmp_path, broker, monkeypatch, mutation):
    from prismabuild import lifecycle
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    key = action["action_key"]
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", _refuse)
    item = claim(queue)
    outcome = _execute(queue, item)
    checkout = Path(outcome["checkout_cleanup"]["path"])
    queue.records.item_path(pool.CLAIMED, key).rename(tmp_path / "lost-claim.json")
    source = Path(lifecycle.__file__).read_text()
    anchor = '    container_cleanup = queue.cleanup_action_containers(\n        effective_record,'
    assert source.count(anchor) == 1
    bypass = {"queue": "        queue.ledger(holder).release(action_key)\n",
              "alias": "        ledger = queue.ledger(holder)\n        ledger.release(action_key)\n",
              "tier": "        queue.tier('stage:mutant').egress(action_key)\n"}[mutation]
    source = source.replace(anchor, '    if record is None and holder is not None:\n' + bypass + anchor)
    namespace = dict(vars(lifecycle))
    exec(compile(source, str(tmp_path / "mutant_lifecycle.py"), "exec"), namespace)
    monkeypatch.setattr(lifecycle, "_worker_finish", namespace["_worker_finish"])
    with pytest.raises((pool.PoolContractError, TypeError), match="certificate"):
        _finish(queue, item, outcome)
    assert checkout.is_dir()
    assert queue.ledger().held_keys() == [key]


def test_certificate_cannot_be_constructed_by_a_release_caller(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 1})
    assert ledger.acquire("owned", {"cpu": 1})
    with pytest.raises(Exception, match="only be minted"):
        materialize.ReleaseCertificate(object(), ledger, "owned", {}, None, {}, None, "owned")


def test_certificate_refuses_a_reused_reservation_key(tmp_path):
    queue = pool.PoolQueue(tmp_path / "queue")
    ledger = queue.ledger()
    ledger.ensure_capacity({"cpu": 1})
    assert ledger.acquire("owned", {"cpu": 1})
    certificate = ledger.certify_release("owned")
    assert ledger.release("owned", certificate=certificate) == 1
    assert ledger.acquire("owned", {"cpu": 1})
    with pytest.raises(pool.PoolContractError, match="stale"):
        ledger.release("owned", certificate=certificate)
    assert ledger.held_keys() == ["owned"]


def test_unbound_intent_with_an_existing_path_keeps_custody(tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", _refuse)
    item = claim(queue)
    outcome = _execute(queue, item)
    key = action["action_key"]
    path = queue.records.lease_path(key)
    owner = record_store.read_document(path)
    for entry in owner["checkout_registrations"]:
        for field in ("device", "inode", "cleanup", "completion_claim"):
            entry.pop(field, None)
    owner.pop("checkout_cleanup", None)
    queue.records.replace_record_at(path, owner)
    result = materialize.certify_checkout_cleanup(queue.queue, owner, local_checkout_root=pool.LOCAL_CHECKOUT_ROOT)
    assert result["complete"] is False
    assert Path(outcome["checkout_cleanup"]["path"]).is_dir()
    assert queue.ledger().held_keys() == [key]


@pytest.mark.parametrize("spelling", ["ReleaseCertificate", "materialize.ReleaseCertificate", "Alias"])
def test_certificate_construction_guard_detects_other_callers(tmp_path, spelling):
    root = tmp_path / "guard-tree"
    directory = root / "src/prismabuild"
    directory.mkdir(parents=True)
    shutil.copyfile(ROOT / "src/prismabuild/materialize.py", directory / "materialize.py")
    prefix = "from prismabuild.materialize import ReleaseCertificate as Alias\n" if spelling == "Alias" else ""
    (directory / "mutant.py").write_text(prefix + "def bypass():\n    return " + spelling + "(None)\n")
    (root / "tests").mkdir()
    source = (ROOT / "tests/test_checkout_certification_1429.py").read_text()
    target = root / "tests/guard_copy.py"
    target.write_text(source)
    namespace = {"__file__": str(target)}
    exec(compile(source, str(target), "exec"), namespace)
    with pytest.raises(AssertionError):
        namespace["test_scope_token_returns_have_one_conclusion_boundary"]()


def test_rollback_certificate_returns_only_its_exact_acquisition(tmp_path):
    ledger = pool.PoolQueue(tmp_path / "queue").ledger()
    ledger.ensure_capacity({"cpu": 3})
    first = ledger.begin_acquire("owned", {"cpu": 1})
    assert ledger.commit_acquire("owned", first) == 1
    second = ledger.begin_acquire("owned", {"cpu": 1})
    assert ledger.commit_acquire("owned", second) == 1
    certificate = ledger.certify_release("owned", nonce=second)
    assert ledger.release("owned", certificate=certificate) == 1
    assert ledger.holder_tokens("owned") == {"cpu": 1}
    with pytest.raises(pool.PoolContractError, match="nonce"):
        ledger.certify_release("owned", nonce="foreign")


@pytest.mark.parametrize("custody", ["lease", "tombstone"])
def test_ram_ghost_sweep_and_mirror_keep_checkout_custody(tmp_path, broker, monkeypatch, custody):
    from prismabuild import tier_cycle, storage_tiers
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", _refuse)
    item = claim(queue)
    outcome = _execute(queue, item)
    key = action["action_key"]
    queue.ledger("storage").ensure_capacity({"mem_gb": 4})
    tier = queue.tier("ram:storage")
    tier.grow_supply({"ram_gib": 2})
    assert tier.take(key, {"ram_gib": 1})
    if custody == "lease":
        queue.records.item_path(pool.CLAIMED, key).rename(tmp_path / "lost-claim.json")
    else:
        tombstone, mine = queue.queue._entomb_claim(key, expect=item)
        assert mine and tombstone is not None
        queue.records.lease_path(key).unlink()
    tier_cycle.drop_prior_ram_epochs(queue.queue, {})
    assert tier.holdings(key) == {"ram_gib": 1}
    mirror = storage_tiers.RAM_HOST_MEMORY_PREFIX + key
    assert queue.ledger("storage").holder_tokens(mirror) == {"mem_gb": 1}
    assert Path(outcome["checkout_cleanup"]["path"]).is_dir()


def test_certificate_cannot_return_a_later_take_of_the_same_token(tmp_path):
    ledger = pool.PoolQueue(tmp_path / "queue").ledger()
    ledger.ensure_capacity({"cpu": 2})
    first = ledger.begin_acquire("owned", {"cpu": 1})
    assert ledger.commit_acquire("owned", first) == 1
    second = ledger.begin_acquire("owned", {"cpu": 1})
    assert ledger.commit_acquire("owned", second) == 1
    old = ledger.certify_release("owned", nonce=second)
    assert ledger.release("owned", certificate=old) == 1
    third = ledger.begin_acquire("owned", {"cpu": 1})
    assert ledger.commit_acquire("owned", third) == 1
    with pytest.raises(pool.PoolContractError, match="stale"):
        ledger.release("owned", certificate=old)
    assert ledger.holder_tokens("owned") == {"cpu": 2}


def test_certificate_is_invalidated_by_another_execute(tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    item = claim(queue)
    _execute(queue, item)
    key = action["action_key"]
    certificate = queue.ledger().certify_release(key)
    monkeypatch.setattr(materialize.owned_tree, "remove_tree", _refuse)
    with pytest.raises(pool.PoolContractError, match="already owns"):
        _execute(queue, item)
    with pytest.raises(pool.PoolContractError, match="stale"):
        queue.ledger().release(key, certificate=certificate)
    assert Path(item["checkout_registrations"][-1]["path"]).is_dir()
    assert queue.ledger().held_keys() == [key]


def test_certificate_does_not_enter_persisted_cleanup_evidence(tmp_path, broker, monkeypatch):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    monkeypatch.setattr(pool, "LOCAL_CHECKOUT_ROOT", tmp_path / "materialized")
    item = claim(queue)
    outcome = _execute(queue, item)
    queue.ledger().certify_release(action["action_key"], claim_snapshot=item)
    json.dumps(item)
    assert "certificate" not in item["checkout_cleanup"]
    _finish(queue, item, outcome)
    assert queue.ledger().held_keys() == []


def test_certificate_is_bound_to_the_exact_attempt_counter(tmp_path, broker):
    queue, action, _ = fenced_snapshot_queue(tmp_path)
    claim(queue)
    key = action["action_key"]
    certificate = queue.ledger().certify_release(key)
    path = queue.records.lease_path(key)
    owner = record_store.read_document(path)
    owner["attempts"] += 1
    queue.records.replace_record_at(path, owner)
    with pytest.raises(pool.PoolContractError, match="stale"):
        queue.ledger().release(key, certificate=certificate)
    assert queue.ledger().held_keys() == [key]


def test_release_certificate_cannot_be_retargeted(tmp_path):
    ledger = pool.PoolQueue(tmp_path / "queue").ledger()
    ledger.ensure_capacity({"cpu": 1})
    assert ledger.acquire("owned", {"cpu": 1})
    certificate = ledger.certify_release("owned")
    with pytest.raises(pool.PoolContractError, match="immutable"):
        certificate._holder = "foreign"
    assert isinstance(certificate._manifest, bytes)
    assert isinstance(certificate._registrations, bytes)
    assert ledger.release("owned", certificate=certificate) == 1
