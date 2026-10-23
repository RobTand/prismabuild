"""Explicit, quiesced adoption of reservations made before #1429's contract.

Inventory is read-only. Apply never returns a token: it only establishes the
custody that the ordinary cleanup certifier must subsequently consume.
"""
from __future__ import annotations

import os
import stat
import uuid

from . import claim_identity, digest_primitives, lifetime_fence, record_store, storage_tiers


class LegacyCustodyMigration:
    CONTRACT = "pre-certificate-1429"

    def __init__(self, queue):
        self.queue = queue

    def _ledgers(self):
        from . import pool
        for kind, root in (("host", self.queue.root / pool.RESERVATIONS),
                           ("tier", self.queue.root / record_store.TIER_RESERVATIONS)):
            try:
                names = sorted(os.listdir(root))
            except FileNotFoundError:
                continue
            for name in names:
                if (root / name).is_dir():
                    ledger = (self.queue.ledger(name) if kind == "host"
                              else self.queue.tier(name)._ledger)
                    yield kind, name, ledger

    @staticmethod
    def _tokens(ledger, holder):
        facts = []
        for name in sorted(os.listdir(ledger.held_dir / holder)):
            info = (ledger.held_dir / holder / name).lstat()
            if not stat.S_ISREG(info.st_mode):
                raise record_store.PoolContractError("legacy holder has a non-file token")
            facts.append([name, info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns])
        return facts

    def _owner_envelope(self, key, *, selected_path=None):
        store = self.queue.records
        claim_path, lease_path = store.item_path(record_store.CLAIMED, key), store.lease_path(key)
        claim, lease = record_store.read_document(claim_path), record_store.read_document(lease_path)
        if claim is not None:
            claim_identity.check_lease_identity(key, claim, lease, invalid=record_store.PoolContractError)
            selected_path, owner = ((lease_path, lease) if lease and (
                selected_path == lease_path or (selected_path is None and "checkout_registrations" in lease))
                else (claim_path, claim))
            state = record_store.CLAIMED
        elif lease is not None:
            selected_path, owner, state = lease_path, lease, "lease"
        else:
            terminals = [(store.item_path(state, key), record_store.read_document(store.item_path(state, key)), state)
                         for state in (record_store.DONE, record_store.FAILED, record_store.WITHDRAWN)]
            terminals = [row for row in terminals if row[1] is not None]
            if len(terminals) != 1:
                raise record_store.PoolContractError("no unique durable legacy owner; restore it or drain with the previous runtime")
            selected_path, owner, state = terminals[0]
        if (owner.get("action_key") != key or not owner.get("claimed_by")
                or not owner.get("claimed_host") or type(owner.get("attempts")) is not int
                or type(owner.get("claimed_unix")) not in (int, float)
                or type(owner.get("published_unix")) not in (int, float)):
            raise record_store.PoolContractError("legacy owner lacks an exact attempt envelope")
        registrations = owner.get("checkout_registrations")
        if registrations is not None and not isinstance(registrations, list):
            raise record_store.PoolContractError("invalid legacy checkout registrations")
        qualification = "existing registration envelope"
        if registrations is None:
            if owner.get("checkout_snapshot") is None:
                qualification = "legacy live-root action has no owned checkout"
            elif (state == record_store.DONE and owner.get("status") in ("executed", "cache_hit")
                  and owner.get("returncode") == 0
                  and (owner.get("container_cleanup") or {}).get("complete") is True):
                # Explicit operator-only grandfathering under the cleanup rules
                # this terminal was made with, never a new inode-unlink proof.
                qualification = "successful terminal under pre-contract cleanup rules"
            else:
                raise record_store.PoolContractError("legacy checkout cleanup unproved; drain with the previous runtime")
            registrations = []
        writes = []
        # Never rewrite an immutable terminal. The adoption journal carries its
        # qualified legacy envelope after the live claim and lease are gone.
        if state == record_store.CLAIMED:
            for path, value in ((claim_path, claim), (lease_path, lease)):
                if value is not None:
                    updated = dict(value, checkout_registrations=registrations)
                    for field in ("claimed_by", "claimed_host", "attempts", "claimed_unix", "published_unix"):
                        updated[field] = owner[field]
                    if "checkout_snapshot" in owner:
                        updated["checkout_snapshot"] = owner["checkout_snapshot"]
                    writes.append((path, value, updated))
        return selected_path, owner, registrations, qualification, writes

    def _describe(self, kind, name, ledger, holder):
        key = holder.removeprefix(storage_tiers.RAM_HOST_MEMORY_PREFIX)
        row = {"kind": kind, "ledger": name, "holder": holder,
               "status": "retained", "reason": "legacy custody needs qualification"}
        try:
            row["tokens"] = self._tokens(ledger, holder)
            path, owner, registrations, qualification, writes = self._owner_envelope(key)
            row.update(status="eligible", reason=qualification,
                       owner_path=str(path.relative_to(self.queue.root)),
                       owner_fingerprint=digest_primitives.canonical_sha256(owner),
                       owner_after_fingerprint=digest_primitives.canonical_sha256(
                           next((after for p, before, after in writes if p == path), owner)))
        except (OSError, ValueError, TypeError, record_store.PoolContractError) as exc:
            row["reason"] = str(exc)
        return row

    def inventory(self):
        holds = []
        for kind, name, ledger in self._ledgers():
            for holder in sorted(ledger.held_keys()):
                if record_store.read_document(ledger.release_owner_path(holder)) is None:
                    holds.append(self._describe(kind, name, ledger, holder))
        return {"contract": self.CONTRACT, "queue_root": str(self.queue.root.absolute()),
                "inventory_id": uuid.uuid4().hex, "holds": holds}

    def apply(self, inventory, *, writers_stopped=False):
        from . import pool
        if not writers_stopped:
            raise record_store.PoolContractError("legacy writers must be stopped before adoption")
        if (inventory.get("contract") != self.CONTRACT
                or inventory.get("queue_root") != str(self.queue.root.absolute())
                or not isinstance(inventory.get("inventory_id"), str)
                or not inventory["inventory_id"]):
            raise record_store.PoolContractError("legacy inventory belongs to another queue or contract")
        result = []
        for row in inventory["holds"]:
            verdict = {k: row[k] for k in ("kind", "ledger", "holder")}
            verdict.update(status="retained", reason=row.get("reason", "unproved legacy hold"))
            try:
                if any(not isinstance(row[field], str) or row[field] in ("", ".", "..")
                       or "/" in row[field] for field in ("ledger", "holder")):
                    raise pool.PoolContractError("unsafe legacy inventory name")
                ledger = (self.queue.ledger(row["ledger"]) if row["kind"] == "host"
                          else self.queue.tier(row["ledger"])._ledger if row["kind"] == "tier"
                          else None)
                if ledger is None:
                    raise pool.PoolContractError("unknown legacy ledger kind")
                holder = row["holder"]
                key = holder.removeprefix(storage_tiers.RAM_HOST_MEMORY_PREFIX)
                with self.queue.action_transition_lock(key):
                    with ledger._mutation_locked():
                        self._adopt_one(ledger, holder, key, row, inventory["inventory_id"])
                verdict.update(status="adopted", reason="custody adopted; no tokens released")
            except (OSError, ValueError, TypeError, KeyError, pool.PoolContractError) as exc:
                verdict["reason"] = str(exc)
            result.append(verdict)
        return result

    def _adopt_one(self, ledger, holder, key, row, inventory_id):
        from . import pool
        manifest = record_store.read_document(ledger.release_owner_path(holder))
        journal_path = ledger.legacy_owner_path(holder)
        journal = record_store.read_document(journal_path)
        if manifest is not None:
            if (journal is not None and journal.get("inventory_id") == inventory_id
                    and journal.get("manifest_nonce") == manifest.get("nonce")):
                return
            raise pool.PoolContractError("reservation already has post-contract custody")
        if self._tokens(ledger, holder) != row.get("tokens"):
            raise pool.PoolContractError("legacy token inventory changed")
        path, owner, registrations, qualification, writes = self._owner_envelope(
            key, selected_path=self.queue.root / row.get("owner_path", ""))
        if (row.get("status") != "eligible" or str(path.relative_to(self.queue.root)) != row.get("owner_path")
                or digest_primitives.canonical_sha256(owner) not in (
                    row.get("owner_fingerprint"), row.get("owner_after_fingerprint"))):
            raise pool.PoolContractError("legacy owner inventory changed or was not qualified")
        if journal is None:
            journal = {k: owner[k] for k in ("action_key", "claimed_by", "claimed_host", "claimed_unix", "published_unix", "attempts")}
            journal.update(checkout_registrations=registrations,
                           checkout_snapshot=owner.get("checkout_snapshot"),
                           inventory_id=inventory_id, manifest_nonce=uuid.uuid4().hex,
                           qualification=qualification, token_facts=row["tokens"],
                           writes={str(p.relative_to(self.queue.root)): [digest_primitives.canonical_sha256(before), digest_primitives.canonical_sha256(after)]
                                   for p, before, after in writes})
            self.queue.records.replace_record_at(journal_path, journal)
        if journal.get("inventory_id") != inventory_id or journal.get("token_facts") != row["tokens"]:
            raise pool.PoolContractError("another legacy inventory owns this adoption")
        for path, before, updated in writes:
            fingerprints = journal["writes"].get(str(path.relative_to(self.queue.root)), [])
            if digest_primitives.canonical_sha256(before) not in fingerprints:
                raise pool.PoolContractError("legacy custody changed during adoption")
            self.queue.records.replace_record_at(path, updated)
        identity = {k: v for k, v in lifetime_fence.attempt_identity(journal).items()
                    if k not in ("nonce", "scope_id")}
        self.queue.records.replace_record_at(ledger.release_owner_path(holder), {
            "action_key": holder, "checkout_registrations": [], "claim": identity,
            "checkout_snapshot": journal.get("checkout_snapshot"),
            "nonce": journal["manifest_nonce"], "acquisition_nonce": "legacy:" + inventory_id,
            "legacy_adoption": inventory_id})
