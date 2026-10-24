"""Explicit, quiesced adoption of reservations made before #1429's contract.

Inventory is read-only. Apply never returns a token: it only establishes the
custody that the ordinary cleanup certifier must subsequently consume.
"""
from __future__ import annotations

import os
import json
from pathlib import Path
import socket
import stat
import subprocess
import time
import uuid

from . import claim_identity, digest_primitives, lifetime_fence, record_store, storage_tiers


def legacy_checkout_owner_path(root, key):
    """One checkout registration owner shared by host, tier and RAM holds."""
    return root / "legacy-checkout-custody" / (key + ".json")


class LegacyCustodyMigration:
    CONTRACT = "pre-certificate-1429"

    def __init__(self, queue, *, roots=None):
        self.queue = queue
        self.roots = roots

    def _legacy_scan_evidence(self, kind, name, holder, key, owner):
        from . import fleet_roster, legacy_custody_host
        if not isinstance(self.roots, dict) or self.roots.get("runtime") != "e9e9d907721282f517e4e8feca40eda1aec79e5d":
            raise record_store.PoolContractError("reviewed e9 historical root coverage unavailable")
        roster = fleet_roster.Roster.at(Path(__file__).resolve().parents[2])
        box = roster.canonical_box(str(owner["claimed_host"]))
        if box is None:
            raise record_store.PoolContractError("legacy owner host is not in the fleet roster")
        local = roster.canonical_box(socket.gethostname())
        ledger_host = roster.canonical_box(name if kind == "host" else name.rsplit(":", 1)[-1])
        if local is None or ledger_host is None or not (local.key == box.key == ledger_host.key):
            raise record_store.PoolContractError("legacy custody scan must run on the owner and ledger host")
        covered = self.roots.get("hosts", {}).get(box.key)
        if (not isinstance(covered, dict) or not covered.get("coverage_evidence")
                or covered.get("all_historical_roots") is not True):
            raise record_store.PoolContractError("historical checkout root coverage is incomplete")
        roots = covered.get("roots")
        if not isinstance(roots, list) or not roots:
            raise record_store.PoolContractError("legacy checkout roots are not named")
        if any(not isinstance(spec, dict) or not isinstance(spec.get("path"), str)
               or spec.get("kind") not in {"checkout", "cleanup-failures", "quarantine"} for spec in roots):
            raise record_store.PoolContractError("legacy root coverage is malformed")
        checkout_roots = [r["path"] for r in roots if r.get("kind") == "checkout"]
        if not checkout_roots or any(not any(r.get("kind") == "cleanup-failures" and
                r.get("path") == str(Path(path) / "cleanup-failures") for r in roots) for path in checkout_roots):
            raise record_store.PoolContractError("checkout or cleanup-failure root coverage missing")
        occupancy = None
        storage_box = None
        if kind == "tier" or holder.startswith(storage_tiers.RAM_HOST_MEMORY_PREFIX):
            occupancy = covered.get("occupancy_roots", {}).get(kind + ":" + name + ":" + holder)
            if not isinstance(occupancy, list) or not occupancy:
                raise record_store.PoolContractError("non-checkout occupancy lacks its own existence proof")
            physical_host = name if kind == "host" else name.rsplit(":", 1)[-1]
            storage_box = roster.canonical_box(physical_host)
            if storage_box is None:
                raise record_store.PoolContractError("non-checkout occupancy host is unknown")
        requests = {box.key: {key: roots}}
        if occupancy is not None:
            requests.setdefault(storage_box.key, {})[key + ":occupancy"] = [
                {"path": path, "kind": "occupancy"} for path in occupancy]
        observations = {}
        # All active participants must be stopped, including storage writers
        # on a host other than the checkout owner. No caller flag is proof.
        for host, entry in roster.boxes.items():
            status, _ = fleet_roster.box_status(host, entry)
            if status != fleet_roster.ACTIVE and host not in requests:
                continue
            address = entry.get("_alias", host)
            request = requests.get(host, {})
            if address == socket.gethostname():
                result = legacy_custody_host.observe_legacy_custody_host(request)
            else:
                tool = Path(__file__).resolve().parents[2] / "tools/maintenance/adopt_legacy_custody.py"
                # The operator stages the candidate on the shared mount; an
                # unavailable remote copy refuses rather than using old code.
                run = subprocess.run(["ssh", "-oBatchMode=yes", "-oConnectTimeout=10", address,
                                      "python3", "-I", str(tool), "--observe-host"],
                                     input=json.dumps(request), capture_output=True, text=True,
                                     check=True, timeout=60)
                result = json.loads(run.stdout)
            if result.get("host") != address:
                raise record_store.PoolContractError("stopped-writer host identity differs")
            observations[host] = result
        chosen = observations[box.key]
        scan = chosen["scans"][key]
        occupied = None if occupancy is None else observations[storage_box.key]["scans"][key + ":occupancy"]
        if occupied is not None and occupied["matches"]:
            raise record_store.PoolContractError("legacy non-checkout occupancy still exists; custody retained")
        return {"host": chosen["host"], "scan": scan, "occupancy": occupied,
                "writers_stopped": {h: r["writers_stopped"] for h, r in observations.items()},
                "coverage_evidence": covered["coverage_evidence"]}

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
                  and (owner.get("detail") or {}).get("returncode") == 0):
                qualification = "e9 terminal requires stopped-host namespace proof"
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

    def status_rows(self):
        rows = self.inventory()["holds"]
        for kind, name, ledger in self._ledgers():
            base = ledger.base / "legacy-adoptions"
            try:
                names = sorted(os.listdir(base))
            except FileNotFoundError:
                continue
            for filename in names:
                journal = record_store.read_document(base / filename)
                if journal is not None:
                    committed = record_store.read_document(ledger.release_owner_path(filename.removesuffix(".json")))
                    rows.append({"kind": kind, "ledger": name, "holder": filename.removesuffix(".json"),
                                 "status": ("adoption pending" if committed is None and ledger.holder_tokens(filename.removesuffix(".json"))
                                            else "certified absent" if journal.get("absence_certified_unix") is not None else "adopted"),
                                 "reason": journal.get("qualification", "legacy custody journal"),
                                 "proof": journal.get("legacy_proof")})
        return rows

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
                if self.queue.offers(max_age_s=pool.LEASE_TIMEOUT_S):
                    raise pool.PoolContractError("fresh worker offer contradicts stopped writers")
                for lease_path in self.queue.records.claimed_lease_paths():
                    lease = record_store.read_document(lease_path)
                    beat = (lease or {}).get("heartbeat_unix")
                    if type(beat) not in (int, float) or time.time() - beat <= pool.LEASE_TIMEOUT_S:
                        raise pool.PoolContractError("fresh or unknown heartbeat contradicts stopped writers")
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
                journal = record_store.read_document(ledger.legacy_owner_path(holder))
                absent = (journal.get("legacy_proof") or {}).get("absent") is True and not journal["checkout_registrations"]
                if absent:
                    ledger.certify_release(holder)  # The ordinary mint proves the adopted empty custody.
                    journal["absence_certified_unix"] = time.time()
                    self.queue.records.replace_record_at(ledger.legacy_owner_path(holder), journal)
                verdict.update(status="adopted", reason=journal["qualification"],
                               certified_absent=absent, proof=journal.get("legacy_proof"))
            except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError, pool.PoolContractError) as exc:
                verdict["reason"] = str(exc)
            result.append(verdict)
        return result

    def _adopt_one(self, ledger, holder, key, row, inventory_id):
        from . import pool
        manifest = record_store.read_document(ledger.release_owner_path(holder))
        journal_path = ledger.legacy_owner_path(holder)
        journal = record_store.read_document(journal_path)
        path, owner, registrations, qualification, writes = self._owner_envelope(
            key, selected_path=self.queue.root / row.get("owner_path", ""))
        evidence = self._legacy_scan_evidence(row["kind"], row["ledger"], holder, key, owner)
        if manifest is not None:
            if (journal is not None and journal.get("inventory_id") == inventory_id
                    and journal.get("manifest_nonce") == manifest.get("legacy_owner_nonce", manifest.get("nonce"))):
                return
            raise pool.PoolContractError("reservation already has post-contract custody")
        if self._tokens(ledger, holder) != row.get("tokens"):
            raise pool.PoolContractError("legacy token inventory changed")
        if (row.get("status") != "eligible" or str(path.relative_to(self.queue.root)) != row.get("owner_path")
                or digest_primitives.canonical_sha256(owner) not in (
                    row.get("owner_fingerprint"), row.get("owner_after_fingerprint"),
                    *(journal or {}).get("writes", {}).get(str(path.relative_to(self.queue.root)), []))):
            raise pool.PoolContractError("legacy owner inventory changed or was not qualified")
        if journal is None:
            matches = evidence["scan"]["matches"]
            if matches:
                # A prefix is not a full action identity. A second key sharing
                # it makes both candidates ambiguous, even under stopped writers.
                for state in (record_store.CLAIMED, record_store.DONE, record_store.FAILED, record_store.WITHDRAWN):
                    if any(p.stem != key and p.stem.startswith(key[:12]) for p in self.queue.records.json_slot_paths(state)):
                        raise pool.PoolContractError("legacy checkout prefix collision; custody retained")
                from . import materialize
                identity = {k: v for k, v in lifetime_fence.attempt_identity(owner).items() if k not in ("nonce", "scope_id")}
                registrations = []
                for match in matches:
                    if (match["kind"] == "checkout" and match.get("regular")
                            and match["name"].endswith(materialize.owned_tree.LOCK_SUFFIX)
                            and any(other["directory"] and other["path"] + materialize.owned_tree.LOCK_SUFFIX == match["path"]
                                    for other in matches)):
                        continue  # The surviving tree owns its lock; this is never an absence proof.
                    if (match["kind"] != "checkout" or not match["directory"]
                            or not materialize.CHECKOUT_TREE.fullmatch(match["name"])):
                        raise pool.PoolContractError("legacy matching failure/quarantine/name needs manual custody recovery")
                    registrations.append({"path": match["path"], "device": match["device"],
                                          "inode": match["inode"], "claim": identity, "execution_id": uuid.uuid4().hex})
            evidence["absent"] = not matches
            qualification = ("stopped-host legacy namespace certified absent" if not matches
                             else "legacy checkout inode adopted; cleanup remains owed")
            checkout_path = legacy_checkout_owner_path(self.queue.root, key)
            checkout_owner = record_store.read_document(checkout_path)
            if checkout_owner is None:
                checkout_owner = {k: owner[k] for k in ("action_key", "claimed_by", "claimed_host", "claimed_unix", "published_unix", "attempts")}
                checkout_owner.update(resource_scope=owner.get("resource_scope"), resource_scope_intent=owner.get("resource_scope_intent"), checkout_registrations=registrations, checkout_snapshot=owner.get("checkout_snapshot"),
                                      inventory_id=inventory_id, legacy_proof=evidence)
                self.queue.records.replace_record_at(checkout_path, checkout_owner)
            elif (checkout_owner.get("inventory_id") != inventory_id
                    or not claim_identity.same_claim(checkout_owner, owner)
                    or checkout_owner.get("claimed_host") != owner.get("claimed_host")):
                raise pool.PoolContractError("another legacy inventory owns checkout custody")
            registrations = checkout_owner["checkout_registrations"]
            writes = [(p, before, dict(after, checkout_registrations=registrations)) for p, before, after in writes]
            journal = {k: owner[k] for k in ("action_key", "claimed_by", "claimed_host", "claimed_unix", "published_unix", "attempts")}
            journal.update(resource_scope=owner.get("resource_scope"), resource_scope_intent=owner.get("resource_scope_intent"), checkout_registrations=registrations,
                           checkout_snapshot=owner.get("checkout_snapshot"),
                           inventory_id=inventory_id, manifest_nonce=uuid.uuid4().hex,
                           qualification=qualification, token_facts=row["tokens"], legacy_proof=evidence,
                           writes={str(p.relative_to(self.queue.root)): [digest_primitives.canonical_sha256(before), digest_primitives.canonical_sha256(after)]
                                   for p, before, after in writes})
            self.queue.records.replace_record_at(journal_path, journal)
        if journal.get("inventory_id") != inventory_id or journal.get("token_facts") != row["tokens"]:
            raise pool.PoolContractError("another legacy inventory owns this adoption")
        for path, before, updated in writes:
            fingerprints = journal["writes"].get(str(path.relative_to(self.queue.root)), [])
            if digest_primitives.canonical_sha256(before) not in fingerprints:
                raise pool.PoolContractError("legacy custody changed during adoption")
            updated["checkout_registrations"] = journal["checkout_registrations"]
            self.queue.records.replace_record_at(path, updated)
        identity = {k: v for k, v in lifetime_fence.attempt_identity(journal).items()
                    if k not in ("nonce", "scope_id")}
        self.queue.records.replace_record_at(ledger.release_owner_path(holder), {
            "action_key": holder, "checkout_registrations": [], "claim": identity,
            "checkout_snapshot": journal.get("checkout_snapshot"),
            "nonce": journal["manifest_nonce"], "acquisition_nonce": "legacy:" + inventory_id,
            "legacy_adoption": inventory_id, "legacy_owner_nonce": journal["manifest_nonce"]})
