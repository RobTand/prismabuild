"""Two boxes reserving the same tier: one wins, the other is told why (#583).

``cpus``/``mem_gb``/``gpu`` are reserved on the box that executes the action.
Storage-tier residency is not: the stage lives on dl380g10 while the consumers
run on the Sparks, so a reservation against it is a reservation against a box
*other* than the executing one.  That is the new thing in PB's model, and the
property that makes it worth having is this one -- two actions admitted on
different hosts cannot both be given the same gigabyte.

The phenomenon being priced is measured, not hypothetical: on 2026-09-14 three
concurrent export arms pinned dl380g10's HDDs at 92% util and 522 MB/s
pool-side, and withdrawing the third arm *raised* the other two from ~2.0+2.1
to 3.0+4.5 units/s.  PB admitted all three because it counted CPU, memory and
GPU, none of which were scarce.  A tier ledger is what lets the third be
refused, and the refusal is recorded rather than silent.

The two hosts here are this one process wearing two hostnames: the ledger's
identity is its directory name, and a second ``PoolQueue`` on the same shared
root with a different ``socket.gethostname`` contends through exactly the
renames a second machine would.  Nothing touches the live queue.
"""

from __future__ import annotations

from pathlib import Path
import sys
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import adaptive_cpu, pool  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
STAGE = f"stage_gib@{TIER}"
FILL = f"fill_mb_s_pool_side@{TIER}"
ON_SPARKY = "1" * 64
ON_SPARKLINA = "2" * 64


@pytest.fixture()
def shared_root(tmp_path: Path) -> Path:
    """One shared mount, as both boxes see it."""

    return tmp_path / "pb-queue"


def _as_host(monkeypatch: pytest.MonkeyPatch, host: str) -> None:
    monkeypatch.setattr(pool.socket, "gethostname", lambda: host)


def _queue(root: Path) -> pool.PoolQueue:
    q = pool.PoolQueue(root)
    q.ensure_layout()
    return q


def _publish(q: pool.PoolQueue, key: str, resources: dict[str, int]) -> None:
    q.publish(
        action_key=key,
        cas_root=q.root / "cas",
        checkout_root=q.root / "co",
        worker_script=q.root / "worker.py",
        resources=resources,
        tags=[],
    )


def _denial(q: pool.PoolQueue, key: str) -> dict[str, object] | None:
    """The denial log is host-local; read the one belonging to ``q``'s host."""

    path = adaptive_cpu.local_state_base(q.ledger().base) / pool.CLAIM_DENIALS
    records = adaptive_cpu.read_json(path).get("records", {})
    matching = [entry for entry in records.values()
                if isinstance(entry, dict) and entry.get("action_key") == key]
    if not matching:
        return None
    return max(matching, key=lambda entry: float(entry.get("denied_unix", 0.0)))


def test_the_second_box_is_refused_the_gigabytes_the_first_took(
    shared_root: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One 40 GiB stage, two 30 GiB movers on different boxes: one runs."""

    _as_host(monkeypatch, "dl380g10")
    minter = _queue(shared_root)
    minter.mint_tier_capacity(TIER, {"stage_gib": 40, "fill_mb_s_pool_side": 242})

    _as_host(monkeypatch, "sparky")
    sparky = _queue(shared_root)
    _publish(sparky, ON_SPARKY, {"cpu": 1, STAGE: 30, FILL: 200})
    _publish(sparky, ON_SPARKLINA, {"cpu": 1, STAGE: 30, FILL: 200})
    first = sparky.claim(owner="sparky-worker", capacity={"cpu": 8})
    assert first is not None
    winner = first["action_key"]
    assert first["claimed_host"] == "sparky"
    assert first["tier_reservations"] == {
        TIER: {"fill_mb_s_pool_side": 200, "stage_gib": 30}}

    # The other box now looks at the remaining row.  It has CPU to spare and
    # the placement matches; only the tier is short.
    _as_host(monkeypatch, "sparklina")
    sparklina = _queue(shared_root)
    loser = ON_SPARKLINA if winner == ON_SPARKY else ON_SPARKY
    assert sparklina.claim(owner="sparklina-worker", capacity={"cpu": 8}) is None

    denial = _denial(sparklina, loser)
    assert denial is not None
    assert denial["reason"] == "tier_reservation_unavailable"
    shortage = denial["evidence"]["tier_shortage"]
    assert shortage["tier_id"] == TIER
    assert shortage["demand"] == {"stage_gib": 30, "fill_mb_s_pool_side": 200}
    assert shortage["capacity_total"] == {"stage_gib": 40, "fill_mb_s_pool_side": 242}
    # Refused, not admitted blind, and not admitted on half a reservation.
    assert sparklina.tier_holdings(loser) == {}
    # Its own box's tokens went straight back: a cluster-scoped shortage must
    # not idle a box that has work it can do.
    assert sparklina.ledger("sparklina").held() == {}
    assert sparklina.passes(loser) == 0

    # Nothing was lost or double-counted: held plus free equals what was minted.
    ledger = sparklina.tier_ledger(TIER)
    held = ledger.held()
    free = ledger.available()
    assert held == {"stage_gib": 30, "fill_mb_s_pool_side": 200}
    assert {kind: held.get(kind, 0) + free.get(kind, 0) for kind in
            set(held) | set(free)} == {"stage_gib": 40, "fill_mb_s_pool_side": 242}

    # And when the winner finishes, the loser is admitted on the other box.
    _as_host(monkeypatch, "sparky")
    sparky.finish(winner, status="executed", claim_snapshot=first)
    assert sparky.tier_holdings(winner) == {}
    _as_host(monkeypatch, "sparklina")
    second = sparklina.claim(owner="sparklina-worker", capacity={"cpu": 8})
    assert second is not None and second["action_key"] == loser
    assert second["claimed_host"] == "sparklina"
    assert sparklina.tier_holdings(loser) == {
        TIER: {"stage_gib": 30, "fill_mb_s_pool_side": 200}}


def test_a_tier_shortage_on_one_box_does_not_hold_the_other_boxs_tokens(
    shared_root: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The refused box keeps claiming everything else it can seat."""

    _as_host(monkeypatch, "dl380g10")
    _queue(shared_root).mint_tier_capacity(TIER, {"stage_gib": 1})

    _as_host(monkeypatch, "sparky")
    sparky = _queue(shared_root)
    _publish(sparky, ON_SPARKY, {"cpu": 1, STAGE: 1})
    held = sparky.claim(owner="sparky-worker", capacity={"cpu": 8})
    assert held is not None and held["action_key"] == ON_SPARKY

    _as_host(monkeypatch, "sparklina")
    sparklina = _queue(shared_root)
    _publish(sparklina, ON_SPARKLINA, {"cpu": 1, STAGE: 1})
    ordinary = "3" * 64
    _publish(sparklina, ordinary, {"cpu": 1})
    claimed = sparklina.claim(owner="sparklina-worker", capacity={"cpu": 8})
    # The tier-hungry row was passed over; the ordinary row ran.
    assert claimed is not None and claimed["action_key"] == ordinary
    denial = _denial(sparklina, ON_SPARKLINA)
    assert denial is not None and denial["reason"] == "tier_reservation_unavailable"


def test_a_demand_no_tier_can_ever_meet_says_so_rather_than_waiting(
    shared_root: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _as_host(monkeypatch, "dl380g10")
    _queue(shared_root).mint_tier_capacity(TIER, {"stage_gib": 4})
    _as_host(monkeypatch, "sparky")
    sparky = _queue(shared_root)
    _publish(sparky, ON_SPARKY, {"cpu": 1, STAGE: 5})
    assert sparky.claim(owner="sparky-worker", capacity={"cpu": 8}) is None
    denial = _denial(sparky, ON_SPARKY)
    assert denial is not None and denial["reason"] == "never_fits_tier_capacity"

    # A tier nobody has minted at all is a different answer from a busy one.
    unknown = "4" * 64
    _publish(sparky, unknown, {"cpu": 1, "stage_gib@prismabuild-stage:nowhere": 1})
    assert sparky.claim(owner="sparky-worker", capacity={"cpu": 8}) is None
    assert _denial(sparky, unknown)["reason"] == "tier_unknown"
