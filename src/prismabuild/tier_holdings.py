"""Tier occupancy, unspent reservations and their RAM host mirrors.

ResourceLedger remains the token mechanism. Tier consumers name a disposition
instead of returning physical occupancy through a general release operation.
"""
from __future__ import annotations

from collections.abc import Callable, Container, Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING
import os

from . import holder_census, immutable_files, kept_reads, posix_lock, record_store, storage_tiers
from .digest_primitives import raw_sha256
from .record_store import PoolContractError

if TYPE_CHECKING:
    from .pool import ResourceLedger

RATE_KINDS = frozenset({storage_tiers.FILL_KIND})


class Unspent(Enum):
    """The consumer's positive authority for returning unused capacity."""

    NEVER_STARTED = "never-started"
    UNUSED_GRANT = "unused-grant"
    CLAIM_ROLLBACK = "claim-rollback"
    CONCLUDED_UNPINNED = "concluded-unpinned"
    OLD_RAM_EPOCH = "old-ram-epoch"


@dataclass(frozen=True)
class TierPaths:
    """The unchanged persisted protocol, for faults and directory fences."""

    base: Path
    free: Path
    held: Path
    minted: Path


class TierLedger:
    """One tier owns mutations and mirror repair; observations remain lazy."""

    def __init__(self, records: record_store.RecordStore,
                 host_ledgers: Callable, tier_id: str):
        from .pool import ResourceLedger

        self.records = records
        self.tier_id = records.check_tier_id(tier_id)
        self._host_ledgers = host_ledgers
        self._ledger = ResourceLedger(
            records.root / record_store.TIER_RESERVATIONS, host=self.tier_id,
            mutation_guard=self.mutation_lock, tier_census=True)

    def certify_release(self, holder, **kwargs):
        return self._ledger.certify_release(holder, **kwargs)

    @property
    def paths(self) -> TierPaths:
        ledger = self._ledger
        return TierPaths(ledger.base, ledger.free_dir, ledger.held_dir, ledger.minted_dir)

    @property
    def shortage(self):
        return self._ledger.last_token_shortage

    def holdings(self, holder: str | None = None) -> dict[str, int]:
        return (self._ledger.held() if holder is None
                else self._ledger.holder_tokens(holder))

    def holders(self) -> list[str]:
        return self._ledger.held_keys()

    def declared(self) -> bool:
        return self._ledger.base.is_dir()

    def holder_absent(self, holder: str) -> bool:
        return immutable_files.confirmed_metadata_absence(self._ledger.held_dir / holder)

    def holder_entries(self, holder: str) -> set[str]:
        with os.scandir(self._ledger.held_dir / holder) as entries:
            return {entry.name for entry in entries}

    def holder_has_entries(self, holder: str) -> bool:
        with os.scandir(self._ledger.held_dir / holder) as entries:
            return any(True for _ in entries)

    def holder_fresh_present(self, holder: str) -> bool:
        if holder not in os.listdir(self._ledger.held_dir):
            return False
        return any("-" in name for name in os.listdir(self._ledger.held_dir / holder))

    @property
    def fence_key(self) -> str:
        return os.fspath(self._ledger.base)

    def observation_fences(self, fence) -> kept_reads.EvidenceSet | None:
        free = fence(self._ledger.free_dir)
        held = fence(self._ledger.held_dir)
        if free is None or held is None:
            return None
        try:
            holders = (sorted(os.listdir(held.directory))
                       if held.directory == os.fspath(self._ledger.held_dir) else [])
        except OSError:
            return None
        taken = (free, held, *(fence(Path(held.directory) / name) for name in holders))
        if any(item is None for item in taken):
            return None
        return kept_reads.EvidenceSet(taken)

    def holder_state(self, holder: str) -> set[str]:
        try:
            return {path.name for path in self._ledger._glob_names(
                self._ledger.held_dir / holder, "*-*", strict=True)}
        except (FileNotFoundError, NotADirectoryError):
            return set()

    def token_names(self, holder: str) -> set[str]:
        return {path.name for path in immutable_files.match_optional(
            self._ledger.held_dir / holder, "*-*")}

    def holder_names(self) -> set[str]:
        return {path.name for path in self._ledger._census_scan(self._ledger.held_dir)}

    def uses_tier_census(self) -> bool:
        return self._ledger._strict_census()

    def mint_history(self) -> tuple[set[str], set[str]]:
        return self._ledger._minted_and_dead_names()

    @property
    def mint_issues(self):
        return self._ledger.last_census_report

    def mint_supply(self) -> dict[str, int]:
        return self._ledger.capacity()

    def mint_census(self):
        return self._ledger.capacity_census()

    @property
    def census_report(self) -> Path:
        return self._ledger.census_report_path

    def grow_supply(self, wanted: Mapping[str, int]):
        return self._ledger.ensure_capacity(wanted)

    def trim_supply(self, wanted: Mapping[str, int]):
        return self._ledger.retire_free_capacity(wanted)

    def retire_duplicate(self, holder: str, counts: Mapping[str, int]):
        return self._ledger.retire_held(holder, counts)

    def holder_observations(self):
        return self._ledger.holder_census()

    def token_paths(self, directory: Path, pattern: str):
        return self._ledger._census_glob(directory, pattern)

    def entry_paths(self, directory: Path, *, optional: bool = False):
        return self._ledger._census_scan(directory, optional=optional)

    def free_supply(self) -> dict[str, int]:
        return self._ledger.available()

    @staticmethod
    def _return_taken(ledger: ResourceLedger, holder: str, names: Sequence[str], *, certificate) -> int:
        keep = {p.name for p in immutable_files.match_optional(ledger.held_dir / holder, "*-*")} - set(names)
        return ledger.release_except(holder, keep, certificate=certificate)

    def _take_from(self, ledger: ResourceLedger, holder: str,
                   demand: Mapping[str, int], *,
                   moved_tokens: list[str] | None = None) -> bool:
        with ledger._mutation_locked(blocking=ledger._mutation_guard is None) as acquired:
            if not acquired:
                return False
            handle = ledger.begin_acquire(holder, demand)
            if handle is None:
                return False
            wanted = sum(int(count) for count in demand.values() if int(count) > 0)
            names: list[str] = []
            try:
                moved = ledger.commit_acquire(holder, handle, moved_tokens=names)
            except BaseException:
                try:
                    ledger.abandon_acquire(handle)
                except (OSError, PoolContractError, ValueError):
                    pass
                self._return_taken(ledger, holder, names, certificate=ledger.certify_release(holder, nonce=handle))
                raise
            if moved < wanted:
                ledger.abandon_acquire(handle)
                self._return_taken(ledger, holder, names, certificate=ledger.certify_release(holder, nonce=handle))
                return False
            if moved_tokens is not None:
                moved_tokens.extend(names)
            return True

    def take(self, holder: str, demand: Mapping[str, int]) -> bool:
        gib = (int(demand.get(storage_tiers.RAM_CAPACITY_KIND, 0))
               if storage_tiers.tier_kind_of(self.tier_id) == "ram" else 0)
        host_take: list[str] = []
        if gib > 0:
            state, detail = self._hold_host(holder, gib, moved_tokens=host_take)
            if state != "taken":
                self._ledger.last_token_shortage = {
                    "resource": "mem_gb", "requested": gib,
                    "available": None if state == "unknown" else 0,
                    "reason": "ram_host_" + state, "detail": detail}
                return False
        try:
            taken = self._take_from(self._ledger, holder, demand)
        except BaseException:
            if gib > 0:
                self._rollback_host_take(holder, host_take)
            raise
        if not taken and gib > 0:
            self._rollback_host_take(holder, host_take)
        return taken

    def begin_take(self, holder: str, demand: Mapping[str, int]):
        return self._ledger.begin_acquire(holder, demand)

    def commit_take(self, holder: str, handle: str) -> int:
        return self._ledger.commit_acquire(holder, handle)

    def abandon_take(self, handle: str) -> int:
        return self._ledger.abandon_acquire(handle)

    def pending_takes(self, holder: str):
        return self._ledger.acquisitions_of(holder)

    def unparsed_takes(self, holder: str) -> list[str]:
        return self._ledger.unparsed_acquisitions_of(holder)

    def reap_pending_takes(self, *, grace_s: float = 300.0):
        return self._ledger.sweep_stale_acquisitions(grace_s=grace_s)

    def handover_named(self, source: str, target: str, names: Sequence[str], *,
                       faults: list | None = None) -> int:
        ram = storage_tiers.tier_kind_of(self.tier_id) == "ram"
        actual: list[str] = []
        moved = (self._ledger.transfer_tokens(source, target, names, moved_tokens=actual)
                 if ram else self._ledger.transfer_tokens(source, target, names))
        if ram:
            try:
                occupancy = sum(name.rsplit("-", 1)[0] == storage_tiers.RAM_CAPACITY_KIND
                                for name in actual)
                if occupancy:
                    self._host_ledgers(self._ram_host()).transfer_count(
                        storage_tiers.RAM_HOST_MEMORY_PREFIX + source,
                        storage_tiers.RAM_HOST_MEMORY_PREFIX + target, occupancy)
            except (OSError, PoolContractError, ValueError) as exc:
                if faults is not None:
                    faults.append({"reason": "ram_host_transfer_failed",
                                   "tier_id": self.tier_id, "from": source,
                                   "to": target, "error": repr(exc)})
        return moved

    def release_unspent(self, holder: str, *, authority: Unspent,
                        keep: Container[str] | None = None,
                        counts: Mapping[str, int] | None = None, certificate=None) -> int | dict[str, int]:
        if counts is not None:
            released = self._ledger.release_count(holder, counts, certificate=certificate)
        elif keep is not None:
            released = self._ledger.release_except(holder, keep, certificate=certificate)
        else:
            released = self._ledger.release(holder, certificate=certificate)
        if storage_tiers.tier_kind_of(self.tier_id) == "ram":
            self._trim_host_to_occupancy(holder)
        return released

    def egress(self, holder: str, *, certificate=None) -> int:
        """Return a whole holder after the consumer proves physical egress."""
        released = self._ledger.release(holder, certificate=certificate)
        if storage_tiers.tier_kind_of(self.tier_id) == "ram":
            self._trim_host_to_occupancy(holder)
        return released

    def release_rates(self, holder: str, *, certificate=None) -> int:
        return self._ledger.release_kinds(holder, RATE_KINDS, certificate=certificate)

    def reclaim_idle_rates(self) -> list[dict[str, object]]:
        events: list[dict[str, object]] = []
        try:
            keys = self.holders()
        except (OSError, PoolContractError):
            return events
        for key in sorted(keys):
            try:
                if bool(record_store.value_or_raise(
                        self.records.item_presence(record_store.CLAIMED, key))):
                    continue
                rates = {kind: count for kind, count in self.holdings(key).items()
                         if kind in RATE_KINDS}
                if not rates:
                    continue
                released = self.release_rates(key)
            except (OSError, PoolContractError):
                continue
            events.append({"event": "tier-rate-reclaimed", "tier_id": self.tier_id,
                           "holder": key, "released": released, "rates": rates})
        return events

    def held_grants(self) -> list[str]:
        from .window_credit import GRANT_PREFIX

        try:
            keys = self.holders()
        except (OSError, ValueError):
            return []
        return sorted(key for key in keys if str(key).startswith(GRANT_PREFIX))


    def mutation_lock(self, *, blocking: bool=True):
        """Mint is the tier mutation leaf; host ledgers retain their own lock."""
        name = raw_sha256(f'tier-mint:{self.records.check_tier_id(self.tier_id)}'.encode())
        return posix_lock.held(self.records.root / 'tier-mint-locks' / f'{name}.lock', blocking=blocking)

    def _hold_host(self, grant: str, gib: int, *,
                   moved_tokens: list[str] | None = None) -> tuple[str, str]:
        try:
            taken = bool(self._take_from(
                self._host_ledgers(self._ram_host()),
                storage_tiers.RAM_HOST_MEMORY_PREFIX + str(grant), {"mem_gb": int(gib)},
                moved_tokens=moved_tokens))
        except (OSError, PoolContractError, ValueError) as exc:
            return ('unknown', f'host mem ledger unreadable: {exc!r}')
        if taken:
            return ('taken', '')
        return ('short', 'host mem pool refused the hold')

    def _release_host(self, grant: str) -> int:
        try:
            return int(self._host_ledgers(self._ram_host()).release(storage_tiers.RAM_HOST_MEMORY_PREFIX + str(grant), certificate=self._host_ledgers(self._ram_host()).certify_release(storage_tiers.RAM_HOST_MEMORY_PREFIX + str(grant))))
        except (OSError, PoolContractError, ValueError):
            return 0

    def _rollback_host_take(self, grant: str, names: Sequence[str]) -> int:
        try:
            ledger = self._host_ledgers(self._ram_host())
            with ledger._mutation_locked():
                return self._return_taken(
                    ledger, storage_tiers.RAM_HOST_MEMORY_PREFIX + str(grant), names,
                    certificate=ledger.certify_release(storage_tiers.RAM_HOST_MEMORY_PREFIX + str(grant)))
        except (OSError, PoolContractError, ValueError):
            return 0

    def _trim_host_to_occupancy(self, holder: str) -> int:
        """An unreadable tier retains its host half; retry heals by name."""
        try:
            target = sum(name.rsplit("-", 1)[0] == storage_tiers.RAM_CAPACITY_KIND
                         for name in self.holder_state(holder))
            ledger = self._host_ledgers(self._ram_host())
            mirror = storage_tiers.RAM_HOST_MEMORY_PREFIX + str(holder)
            current = int(ledger.holder_tokens(mirror).get("mem_gb", 0))
            if current <= target:
                return 0
            returned = ledger.release_count(mirror, {"mem_gb": current - target}, certificate=ledger.certify_release(mirror))
            return int(returned.get("mem_gb", 0))
        except (OSError, PoolContractError, ValueError):
            return 0

    def _ram_host(self) -> str:
        return str(self.tier_id).split(':', 1)[1]

    def take_advance(self, grant: str, gib: int, kind: str) -> tuple[str, str]:
        """Host first, tier second; unknown always defers the advance."""
        ram = storage_tiers.tier_kind_of(str(self.tier_id)) == 'ram'
        if kind in RATE_KINDS:
            raise PoolContractError(f'rate kind {kind!r} is not an occupancy kind: RAM host mirrors hold GiB, never a fill ration (#1245 review r2)')
        host_take: list[str] = []
        if ram:
            host_state, host_detail = self._hold_host(grant, gib, moved_tokens=host_take)
            if host_state == 'unknown':
                return ('unknown', host_detail)
            if host_state == 'short':
                return ('host-short', host_detail)
        try:
            taken = bool(self._take_from(self._ledger, str(grant), {str(kind): int(gib)}))
        except (OSError, PoolContractError, ValueError) as exc:
            if ram:
                self._rollback_host_take(grant, host_take)
            return ('unknown', f'tier ledger unreadable: {exc!r}')
        if not taken:
            if ram:
                self._rollback_host_take(grant, host_take)
            return ('tier-short', 'tier pool refused the take')
        return ('taken', '')

    def cancel_fence(self, holder: str) -> dict[str, int]:
        try:
            outcome = {'released': int(self._ledger.release(str(holder), certificate=self._ledger.certify_release(str(holder))))}
        except (OSError, ValueError):
            outcome = {'released': 0}
        if storage_tiers.tier_kind_of(str(self.tier_id)) == 'ram':
            self._trim_host_to_occupancy(str(holder))
        return outcome

    def release_unspent_best_effort(self, holder: str, *, authority: Unspent, certificate=None) -> int:
        return self._release_best_effort(holder, certificate=certificate)

    def egress_best_effort(self, holder: str, *, certificate=None) -> int:
        return self._release_best_effort(holder, certificate=certificate)

    def _release_best_effort(self, holder: str, *, certificate=None) -> int:
        from .materialize import ReleaseCertificate
        ReleaseCertificate.require(certificate, self._ledger, str(holder))
        try:
            released = int(self._ledger.release(str(holder), certificate=certificate))
        except (OSError, PoolContractError, ValueError):
            released = 0
        if storage_tiers.tier_kind_of(str(self.tier_id)) == 'ram':
            self._trim_host_to_occupancy(str(holder))
        return released

    @staticmethod
    def _mirror_holders(ledger: ResourceLedger) -> list[str]:
        try:
            names = record_store.RecordStore.required_directory_names_at(ledger.held_dir)
        except FileNotFoundError:
            return []
        return [name for name in names if not holder_census.is_private_holder(name)]

    @staticmethod
    def _host_mirror_tokens(ledger: ResourceLedger, mirror: str) -> set[str]:
        try:
            return {path.name for path in immutable_files.match_required(
                ledger.held_dir / mirror, "*-*")}
        except FileNotFoundError:
            return set()

    def _sync_host_mirror(self, ledger: ResourceLedger, holder: str,
                          target: int) -> tuple[bool, dict[str, object] | None]:
        mirror = storage_tiers.RAM_HOST_MEMORY_PREFIX + holder
        try:
            current = sum(name.rsplit("-", 1)[0] == "mem_gb"
                          for name in self._host_mirror_tokens(ledger, mirror))
        except (OSError, PoolContractError, ValueError):
            return False, None
        if current == target:
            return True, None
        if current < target:
            try:
                taken = self._take_from(ledger, mirror, {"mem_gb": target - current})
            except (OSError, PoolContractError, ValueError) as exc:
                taken = False
                detail = repr(exc)
            else:
                detail = str(ledger.last_token_shortage or '')
            if not taken:
                return False, {'reason': 'ram-host-hold-missing', 'holder': mirror,
                               'gib': target, 'detail': detail}
            return True, None
        try:
            released_counts = ledger.release_count(mirror, {'mem_gb': current - target}, certificate=ledger.certify_release(mirror))
        except (OSError, PoolContractError, ValueError) as exc:
            return False, {'reason': 'ram-host-hold-missing', 'holder': mirror,
                           'gib': target, 'detail': f'trim failed: {exc!r}'}
        released = int(released_counts.get('mem_gb', 0))
        if released:
            return released == current - target, {
                'reason': 'ram-host-hold-trimmed', 'holder': mirror,
                'released_gib': released}
        return False, None

    def reconcile_mirror(self, expected_holders: Iterable[str], *, release_orphans: bool=True) -> dict[str, object]:
        if storage_tiers.tier_kind_of(str(self.tier_id)) != 'ram':
            return {'events': [], 'converged': True}
        host = self._ram_host()
        try:
            tier_live = set(self._mirror_holders(self._ledger))
            host_ledger = self._host_ledgers(host)
            holders = self._mirror_holders(host_ledger)
        except (OSError, PoolContractError, ValueError):
            return {'events': [], 'converged': False}
        expected = {str(name) for name in expected_holders}
        events: list[dict[str, object]] = []
        converged = True
        prefix = storage_tiers.RAM_HOST_MEMORY_PREFIX
        for holder in sorted(tier_live):
            try:
                target = sum(name.rsplit("-", 1)[0] == storage_tiers.RAM_CAPACITY_KIND
                             for name in self.holder_state(holder))
            except (OSError, PoolContractError, ValueError):
                converged = False
                continue
            synced, event = self._sync_host_mirror(host_ledger, holder, target)
            converged = converged and synced
            if event is not None:
                events.append(event)
        if not release_orphans:
            return {'events': events, 'converged': converged}
        for holder in holders:
            if not holder.startswith(prefix):
                continue
            suffix = holder[len(prefix):]
            if suffix in tier_live or suffix in expected:
                continue
            try:
                tokens = self._host_mirror_tokens(host_ledger, holder)
                released = host_ledger.release(holder, certificate=host_ledger.certify_release(holder))
            except (OSError, PoolContractError, ValueError):
                converged = False
                continue
            if released != len(tokens):
                converged = False
            if released:
                events.append({'reason': 'orphan_ram_host_hold', 'holder': holder, 'released_gib': released})
        return {'events': events, 'converged': converged}

    def handover(self, from_key: str, to_key: str, *, faults: list | None=None) -> int:
        """Tier first, host second; reconcile repairs a named host failure."""
        moved = self._ledger.transfer(str(from_key), str(to_key))
        if storage_tiers.tier_kind_of(str(self.tier_id)) == 'ram':
            host = self._ram_host()
            try:
                self._host_ledgers(host).transfer(storage_tiers.RAM_HOST_MEMORY_PREFIX + str(from_key), storage_tiers.RAM_HOST_MEMORY_PREFIX + str(to_key))
            except (OSError, PoolContractError, ValueError) as exc:
                if faults is not None:
                    faults.append({'reason': 'ram_host_transfer_failed', 'tier_id': str(self.tier_id), 'from': str(from_key), 'to': str(to_key), 'error': repr(exc)})
        return moved

    def handover_count(self, from_key: str, to_key: str, count: int, *, faults: list | None=None) -> int:
        moved = self._ledger.transfer_count(str(from_key), str(to_key), count)
        if storage_tiers.tier_kind_of(str(self.tier_id)) == 'ram':
            host = self._ram_host()
            try:
                self._host_ledgers(host).transfer_count(storage_tiers.RAM_HOST_MEMORY_PREFIX + str(from_key), storage_tiers.RAM_HOST_MEMORY_PREFIX + str(to_key), moved)
            except (OSError, PoolContractError, ValueError) as exc:
                if faults is not None:
                    faults.append({'reason': 'ram_host_transfer_failed', 'tier_id': str(self.tier_id), 'from': str(from_key), 'to': str(to_key), 'error': repr(exc)})
        return moved

    def mint(self, tokens: Mapping[str, int]) -> dict[str, object]:
        with self.mutation_lock():
            ledger = self._ledger
            wanted = {str(kind): int(count) for kind, count in tokens.items()}
            if any((count < 0 for count in wanted.values())):
                raise PoolContractError('tier capacity must not be negative')
            return self._apply_mint(ledger, wanted)

    def _apply_mint(self, ledger: ResourceLedger, wanted: dict[str, int]) -> dict[str, object]:
        reclaimed = self._reclaim_mint(ledger, wanted)
        ledger.ensure_capacity({kind: count for kind, count in wanted.items() if count > 0})
        total, unreadable = ledger.capacity_census()
        if not unreadable:
            lower = {kind: wanted.get(kind, 0) for kind in total if total[kind] > wanted.get(kind, 0)}
        else:
            lower = {kind: wanted.get(kind, 0) for kind in set(total) | set(wanted)}
        if lower:
            retired = ledger.retire_free_capacity(lower)
            census = ledger.last_census_report
        else:
            retired = {}
            census = ledger.report_census(unreadable, asked=(), retired={})
        return {'tier_id': self.tier_id, 'capacity': ledger.capacity(), 'retired': retired, 'reclaimed': reclaimed, 'census_unreadable': census}

    def _reclaim_mint(self, ledger: ResourceLedger, wanted: Mapping[str, int]) -> dict[str, int]:
        from .pool import _holder_is_dir
        reclaimed: dict[str, int] = {}
        dead_dir = ledger.minted_dir / 'dead'
        try:
            names = sorted((path.name for path in ledger._census_scan(dead_dir, optional=True) if path.is_file()))
        except OSError:
            return reclaimed
        if not names:
            return reclaimed
        try:
            live: set[str] = set()
            for path in ledger._census_glob(ledger.free_dir, '*-*'):
                live.add(path.name)
            for holder in ledger._census_scan(ledger.held_dir):
                if _holder_is_dir(holder):
                    live.update((path.name for path in ledger._census_glob(holder, '*-*')))
        except OSError:
            return reclaimed
        try:
            live_count: dict[str, int] = {}
            for name in live:
                kind = name.rsplit('-', 1)[0]
                live_count[kind] = live_count.get(kind, 0) + 1
        except (AttributeError, TypeError):
            return reclaimed
        for name in names:
            kind, _, _ = name.rpartition('-')
            if not kind or kind not in wanted:
                continue
            if name in live:
                try:
                    (dead_dir / name).unlink()
                except OSError:
                    pass
                continue
            if live_count.get(kind, 0) >= int(wanted[kind]):
                continue
            try:
                os.rename(dead_dir / name, ledger.free_dir / name)
            except OSError:
                continue
            live_count[kind] = live_count.get(kind, 0) + 1
            live.add(name)
            reclaimed[kind] = reclaimed.get(kind, 0) + 1
        return reclaimed

    def mint_guarded(self, wanted_fn) -> dict[str, object]:
        with self.mutation_lock():
            ledger = self._ledger
            wanted = wanted_fn(self)
            wanted = {str(kind): int(count) for kind, count in dict(wanted).items()}
            if any((count < 0 for count in wanted.values())):
                raise PoolContractError('tier capacity must not be negative')
            return self._apply_mint(ledger, wanted)

    def settle_egress(self, action_key: str, *, destroy: Mapping[str, int], free: Mapping[str, int]) -> dict[str, object]:
        """Settle proven physical egress, destroying duplicates before returns."""
        ledger = self._ledger
        destroyed = ledger.retire_held(action_key, destroy)
        shortfall = {kind: int(count) - int(destroyed.get(kind, 0)) for kind, count in destroy.items() if int(count) - int(destroyed.get(kind, 0)) > 0}
        released = ledger.release_count(action_key, free, certificate=ledger.certify_release(action_key))
        if storage_tiers.tier_kind_of(str(self.tier_id)) == 'ram':
            self._trim_host_to_occupancy(action_key)
        return {'destroyed': destroyed, 'released': released, 'shortfall': shortfall}


def ids(records: record_store.RecordStore) -> list[str]:
    try:
        return sorted(directory.name for directory in immutable_files.scan_optional(
            records.root / record_store.TIER_RESERVATIONS)
            if directory.is_dir() and not directory.name.startswith("."))
    except OSError:
        return []


def for_action(queue, holder: str) -> dict[str, dict[str, int]]:
    holdings = {}
    for tier_id in ids(queue.records):
        tokens = queue.tier(tier_id).holdings(holder)
        if tokens:
            holdings[tier_id] = tokens
    return holdings


def release_rates_all(queue, holder: str) -> int:
    released = 0
    for tier_id in ids(queue.records):
        try:
            released += queue.tier(tier_id).release_rates(holder, certificate=queue.tier(tier_id).certify_release(holder))
        except (OSError, PoolContractError):
            continue
    return released


def release_unspent_all(queue, holder: str, *, authority: Unspent) -> int:
    released = 0
    for tier_id in ids(queue.records):
        try:
            released += queue.tier(tier_id).release_unspent(holder, authority=authority, certificate=queue.tier(tier_id).certify_release(holder))
        except (OSError, PoolContractError):
            continue
    return released


def egress_all(queue, holder: str) -> int:
    released = 0
    for tier_id in ids(queue.records):
        try:
            released += queue.tier(tier_id).egress(holder, certificate=queue.tier(tier_id).certify_release(holder))
        except (OSError, PoolContractError):
            continue
    return released
