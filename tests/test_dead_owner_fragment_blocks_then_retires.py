"""A dead owner's fragment blocks publication until housekeeping retires it (#839).

Production shape (R3 forward-004): a FAILED consumer's WITHDRAWN mover leaves
a fragment naming a staged destination with no material sidecar, no move
receipt, and no held tokens. The shared publisher's proof search walks every
fragment, finds the vouch without a date, and refuses replacement even after
the grace -- while the held-key sweep never sees the owner (no tokens, no
receipt) and reconcile keeps the marked file as prewarm-owned. The obstruction
is permanent until something routes that exact stale owner through `evict`.

These use a real synthetic stage (temp stage root registered to a fake
queue, never real /stage or /ram): real queue rows for the terminal states,
a validated fragment, marked staged files, the real `_StagedPublisher._decide`
for the refusal, and the real `sweep`/`evict` for housekeeping. No payload is
hashed beyond the small fixture bytes.
"""
from __future__ import annotations

import os
import uuid
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import pool, residency_map  # noqa: E402
import prewarm_loop  # noqa: E402
import stage_move  # noqa: E402
import stage_release  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
NAMES = ["model-00087-range.bin", "model-00090-prefix.bin"]
SIZE = 4096


def _key() -> str:
    return uuid.uuid4().hex + uuid.uuid4().hex


@pytest.fixture()
def fleet(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    cas = tmp_path / "cas"
    cas.mkdir()
    return queue, stage, cas


def _publish(queue: pool.PoolQueue, key: str, **kw: object) -> dict:
    queue.publish(
        action_key=key,
        cas_root=kw.pop("cas_root", "/cas"),
        checkout_root=kw.pop("checkout_root", "/co"),
        worker_script=kw.pop("worker_script", "/w.py"),
        resources=kw.pop("resources", {"cpu": 1}),
        **kw,
    )
    claimed = queue.claim(capacity={"cpu": 4})
    assert claimed is not None and claimed["action_key"] == key
    return claimed


def _fail_consumer(queue: pool.PoolQueue) -> tuple[str, float]:
    key = _key()
    holder = _publish(queue, key, max_attempts=1)
    generation = float(holder["published_unix"])
    queue.finish(key, status="failed", detail={"returncode": 1})
    assert queue.item_path(pool.FAILED, key).exists()
    return key, generation


def _withdraw_mover(queue: pool.PoolQueue, *, conclude: bool = True) -> str:
    key = _key()
    holder = _publish(queue, key, max_attempts=1)
    queue.withdraw(key, reason="stale test owner", by="test")
    if conclude:
        # The worker observes the cancellation and concludes: the claim row
        # is retired by its terminal transition, exactly as production
        # leaves it (withdrawn marker filed, no live row, zero tokens,
        # no receipt).
        queue.finish(key, status="withdrawn", detail={"returncode": -15},
                     claim_snapshot=holder)
    assert queue.item_path(pool.WITHDRAWN, key).exists()
    return key


def _stage_marked(stage: Path, name: str) -> Path:
    path = stage / name
    path.write_bytes(b"\0" * SIZE)
    os.setxattr(path, prewarm_loop.STAGE_SOURCE_XATTR,
                f"/originals/{name}@0".encode())
    return path


def _write_fragment(queue: pool.PoolQueue, stage: Path, consumer: str,
                    mover: str, names: list[str]) -> Path:
    return residency_map.write_fragment(queue.root / pool.RESIDENCY, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {
            residency_map.residency_map_key(str(stage / name), 0): {
                "stage_path": str(stage / name), "bytes": SIZE,
                "sha256": "b" * 64, "offset": 0,
            } for name in names
        },
    })


def _dead_owner(fleet) -> tuple[str, str]:
    """Failed consumer + withdrawn mover, fragment but nothing else."""
    queue, stage, _cas = fleet
    consumer, _generation = _fail_consumer(queue)
    mover = _withdraw_mover(queue)
    for name in NAMES:
        _stage_marked(stage, name)
    _write_fragment(queue, stage, consumer, mover, NAMES)
    assert queue.move_record(mover) is None, "dead mover must file no receipt"
    assert mover not in queue.tier_ledger(TIER).held_keys()
    assert not queue.item_path(pool.CLAIMED, mover).exists()
    return consumer, mover


def _publisher(fleet, mover: str, consumer: str) -> stage_move._StagedPublisher:
    queue, stage, cas = fleet
    return stage_move._StagedPublisher(
        queue=queue, stage_root=stage,
        residency_root=queue.root / pool.RESIDENCY,
        mover_action_key=mover, manifest_sha256="a" * 64,
        tier_id=TIER, cas_root=cas, consumer_action_key=consumer)


def test_dead_owner_fragment_blocks_a_same_path_mover(fleet) -> None:
    """The gap, characterized: real refusal, and reconcile keeps the files.

    The held-key sweep cannot see the owner (no tokens, no receipt) and
    reconcile keeps each marked file as prewarm-owned, while the shared
    publisher refuses replacement after the grace.  All three halves are
    pinned here; the retirement itself belongs to the test below.
    """
    queue, stage, _cas = fleet
    consumer, mover = _dead_owner(fleet)
    publisher = _publisher(fleet, _key(), _key())
    verdict = publisher._decide(stage / NAMES[0], SIZE, "c" * 64,
                                computed=None, source_id=None, heal=True)
    assert verdict[0] == "refuse"
    assert "published elsewhere" in verdict[1]
    assert "still unproven after the grace" in verdict[1]
    reconciled = stage_release.reconcile(
        queue, tier_id=TIER, stage_root=str(stage), wanted=set())
    assert reconciled["unowned_left"] == len(NAMES), reconciled
    assert all((stage / name).exists() for name in NAMES)
    assert residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover).exists()


def test_housekeeping_retires_the_dead_owner_and_unblocks(fleet) -> None:
    """Standard housekeeping routes the exact stale owner through `evict`."""
    queue, stage, _cas = fleet
    consumer, mover = _dead_owner(fleet)
    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    publisher = _publisher(fleet, _key(), _key())
    assert publisher._decide(stage / NAMES[0], SIZE, "c" * 64,
                             computed=None, source_id=None,
                             heal=True)[0] == "replace"
    retired = [entry for entry in receipts
               if entry.get("action_key") == mover
               and entry.get("complete") is True]
    assert retired, f"dead owner {mover[:12]} was not retired: {receipts}"
    assert not any((stage / name).exists() for name in NAMES)
    assert not residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover).exists()


def test_a_live_owner_is_retained(fleet) -> None:
    """Neither a live consumer nor a live mover is a dead owner."""
    queue, stage, _cas = fleet
    consumer = _key()
    _publish(queue, consumer, max_attempts=1)
    mover = _key()
    _publish(queue, mover, max_attempts=1)
    for name in NAMES:
        _stage_marked(stage, name)
    fragment = _write_fragment(queue, stage, consumer, mover, NAMES)
    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    assert not [entry for entry in receipts
                if entry.get("action_key") == mover
                and entry.get("complete") is True]
    assert all((stage / name).exists() for name in NAMES)
    assert fragment.exists()


def test_a_tainted_census_refuses_the_pass(fleet) -> None:
    """Unknown ownership retains: an unreadable fragment refuses the pass."""
    queue, stage, _cas = fleet
    consumer, mover = _dead_owner(fleet)
    residue = queue.root / pool.RESIDENCY / consumer / "residue.json"
    residue.write_bytes(b"{not json")
    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    assert receipts and all(entry.get("complete") is not True
                            for entry in receipts)
    assert all((stage / name).exists() for name in NAMES)
    assert residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover).exists()


def test_a_path_shared_with_a_live_owner_is_kept(fleet) -> None:
    """`evict` sharing discipline: the live co-owner's bytes survive."""
    queue, stage, _cas = fleet
    consumer, mover = _dead_owner(fleet)
    live_consumer = _key()
    _publish(queue, live_consumer, max_attempts=1)
    live_mover = _key()
    _publish(queue, live_mover, max_attempts=1)
    live_fragment = _write_fragment(
        queue, stage, live_consumer, live_mover, NAMES[:1])
    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    assert [entry for entry in receipts
            if entry.get("action_key") == mover
            and entry.get("complete") is True]
    assert (stage / NAMES[0]).exists(), "live co-owner's file must survive"
    assert not (stage / NAMES[1]).exists()
    assert live_fragment.exists()
    assert not residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover).exists()


def test_a_surviving_covered_claim_is_skipped_not_evicted(fleet) -> None:
    """No eviction while any claimed row survives, withdrawal or not.

    The worker's terminal transition is what retires a claim row; until it
    does, the row may still belong to a running copy, so discovery skips the
    owner outright -- the same live-row discipline the held-key sweep keeps.
    """
    queue, stage, _cas = fleet
    consumer, _generation = _fail_consumer(queue)
    mover = _withdraw_mover(queue, conclude=False)
    assert queue.item_path(pool.CLAIMED, mover).exists()
    for name in NAMES:
        _stage_marked(stage, name)
    fragment = _write_fragment(queue, stage, consumer, mover, NAMES)
    receipts = stage_release.sweep(
        queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    assert not [entry for entry in receipts
                if entry.get("action_key") == mover
                and entry.get("complete") is True]
    assert all((stage / name).exists() for name in NAMES)
    assert fragment.exists()


def _assert_retained(queue, stage, consumer, mover):
    assert all((stage / name).exists() for name in NAMES)
    assert residency_map.fragment_path(
        queue.root / pool.RESIDENCY, consumer, mover).exists()


@pytest.mark.parametrize('state', [pool.FAILED, pool.WITHDRAWN])
@pytest.mark.parametrize('damage', ['wrong-key', 'wrong-generation', 'wrong-schema',
                                    'malformed', 'empty', 'symlink', 'directory'])
def test_unknown_terminal_identity_retains(fleet, state, damage):
    import json
    queue, stage, _ = fleet
    consumer, mover = _dead_owner(fleet)
    path = queue.item_path(state, consumer if state == pool.FAILED else mover)
    record = json.loads(path.read_bytes())
    if damage == 'wrong-key':
        record['action_key'] = _key()
    elif damage == 'wrong-generation':
        record['published_unix'] += 1
    elif damage == 'wrong-schema':
        record['schema'] = 'unknown'
    if damage in {'wrong-key', 'wrong-generation', 'wrong-schema'}:
        path.write_text(json.dumps(record))
    elif damage == 'malformed':
        path.write_text('{broken')
    elif damage == 'empty':
        path.write_bytes(b'')
    elif damage == 'symlink':
        other = path.with_suffix('.saved')
        path.rename(other)
        path.symlink_to(other)
    else:
        path.unlink()
        path.mkdir()
    stage_release.sweep(queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    _assert_retained(queue, stage, consumer, mover)


@pytest.mark.parametrize('which', ['receipt', 'material', 'decision', 'attempt',
                                  'consumer-lease', 'mover-lease', 'plan'])
def test_missing_or_unknown_authority_is_not_absence(fleet, which):
    import json
    from prismabuild import reader_lease
    queue, stage, _ = fleet
    consumer, mover = _dead_owner(fleet)
    if which == 'decision':
        marker = json.loads(queue.item_path(pool.WITHDRAWN, mover).read_bytes())
        queue.withdrawal_decision_path(marker).unlink()
    elif which == 'attempt':
        failed = json.loads(queue.item_path(pool.FAILED, consumer).read_bytes())
        queue.attempt_path(failed, failed['attempts']).unlink()
    else:
        path = {'receipt': queue.move_path(mover),
                'material': reader_lease.material_path(queue.root / pool.RESIDENCY,
                                                      consumer, mover),
                'consumer-lease': queue.lease_path(consumer),
                'mover-lease': queue.lease_path(mover),
                'plan': queue.residency_plan_path(consumer)}[which]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"schema":"unknown"}')
    stage_release.sweep(queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    _assert_retained(queue, stage, consumer, mover)


@pytest.mark.parametrize('owner', ['consumer', 'mover'])
def test_republication_after_census_retains_new_generation(fleet, monkeypatch, owner):
    queue, stage, _ = fleet
    consumer, mover = _dead_owner(fleet)
    original = stage_release._fragment_census
    published = False

    def census(root, *args, **kwargs):
        nonlocal published
        result = original(root, *args, **kwargs)
        if not published:
            published = True
            queue.publish(action_key=consumer if owner == 'consumer' else mover,
                          cas_root='/cas', checkout_root='/co', worker_script='/w.py',
                          resources={'cpu': 1}, max_attempts=1, recompute=True)
        return result

    monkeypatch.setattr(stage_release, '_fragment_census', census)
    stage_release.sweep(queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    _assert_retained(queue, stage, consumer, mover)
    assert queue.item_path(pool.READY, consumer if owner == 'consumer' else mover).exists()


def _probe_locks(root, keys, result):
    queue = pool.PoolQueue(root)
    for key in keys:
        with queue._transition_locked(key, blocking=False) as acquired:
            result.put(acquired)


def test_consumer_and_mover_excluded_through_egress(fleet, monkeypatch):
    import multiprocessing
    queue, stage, _ = fleet
    consumer, mover = _dead_owner(fleet)
    original = stage_release.evict
    observed = []

    def evict(*args, **kwargs):
        if kwargs.get('reason') == 'dead-owner-sweep':
            ctx = multiprocessing.get_context('spawn')
            result = ctx.Queue()
            process = ctx.Process(target=_probe_locks,
                                  args=(queue.root, [consumer, mover], result))
            process.start()
            observed.extend([result.get(timeout=10), result.get(timeout=10)])
            process.join(timeout=10)
            assert process.exitcode == 0
        return original(*args, **kwargs)

    monkeypatch.setattr(stage_release, 'evict', evict)
    stage_release.sweep(queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    assert observed == [False, False]
    assert not (stage / NAMES[0]).exists()


def test_unreadable_ledger_retains_without_crashing(fleet, monkeypatch):
    queue, stage, _ = fleet
    consumer, mover = _dead_owner(fleet)
    held = queue.tier_ledger(TIER).held_dir
    original = os.listdir

    def listed(path):
        if Path(path) == held:
            raise PermissionError('fixture ledger inaccessible')
        return original(path)

    monkeypatch.setattr(os, 'listdir', listed)
    stage_release.sweep(queue, stage_roots={TIER: str(stage)}, pressure={TIER: 0})
    _assert_retained(queue, stage, consumer, mover)


def test_one_discovery_for_all_tiers(fleet, monkeypatch):
    queue, stage, _ = fleet
    _dead_owner(fleet)
    second = stage.parent / 'ram'
    second.mkdir()
    ram = 'prismabuild-ram:dl380g10'
    stage_release.register_stage_root(queue, tier_id=ram, stage_root=second)
    original = stage_release.sweep_dead_owner_fragments
    calls = []

    def discovery(*args, **kwargs):
        calls.append(set(kwargs['stage_roots']))
        return original(*args, **kwargs)

    monkeypatch.setattr(stage_release, 'sweep_dead_owner_fragments', discovery)
    stage_release.sweep(queue, stage_roots={TIER: str(stage), ram: str(second)},
                        pressure={TIER: 0, ram: 0})
    assert calls == [{TIER, ram}]
