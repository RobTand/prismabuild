"""``pbrun --residency stage`` submits its whole window through one path (#605).

``pbrun.residency_stage_rows`` and the ``--residency stage`` branch of
``pbrun.main`` are the path a campaign takes first, and the pieces around
them are each covered while the wiring between them is not: parsing the flag
shape, sealing distinct keys per range, freezing the plan, choosing the tier.
A mistake in that wiring surfaces only on a live submission -- and did, when
the consumer's row was stamped but no mover's was (#602's follow-up).

So this file builds a small CAS with a two-phase manifest, runs the real
submission path -- ``pbrun.main`` with ``--detach``, the same seal, freeze
and publish a campaign runs -- and asserts the published consumer row
carries a residency block whose leads are phase 0's mover, that only that
mover is published, and that the frozen plan's rows are the ones sealed.
The queue, the tier announcement and the manifest are local; the CAS is
real.

The last three tests drive #708's repricing contract through the same real
path: a withdrawal marks the frozen window superseded, a deliberate
resubmission reseals at the *current* measured fill offer through this
sealing path (key, CAS body and argv agreeing), and a resubmission while the
old window's work is still claimed refuses rather than replacing the old
plan.

The renewal tests drive the case a same-body reseal creates (#708 review):
the submission publishes its consumer and its lead and nothing else, so a
reaped predecessor's cancellations on its later stage and ram children
outlive it.  A fresh seal must retire those *visible* predecessor markers --
under the consumer's transition lock and then each child's, only after
`handoff_safe` proves nothing still names the old window -- while the
immutable decisions stay, and a cancellation filed after the seal still
supersedes the fresh plan.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import (  # noqa: E402
    pool, reader_lease, residency_map, residency_plan, storage_tiers,
)
import pbrun  # noqa: E402
import stage_release  # noqa: E402
import tier_loop  # noqa: E402
from test_pbrun_detach import _checkout  # noqa: E402

TIER = "prismabuild-stage:sparky"
RAM_TIER = "ram:sparky"
GIB = storage_tiers.GIB
PHASE_BYTES = 2 * GIB
FILL_KIND = f"fill_mb_s_pool_side@{TIER}"


def _manifest() -> dict[str, object]:
    """A two-phase v1 manifest, one entry per phase."""

    entries, table, running = [], [], 0
    for index in range(2):
        entries.append({"path": f"/mnt/shared/part-{index}", "offset": 0,
                        "bytes": PHASE_BYTES, "sha256": None})
        running += PHASE_BYTES
        table.append({"name": f"phase-{index}", "bytes": PHASE_BYTES,
                      "cumulative_bytes": running})
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {}, "annotations": {"phases": table},
        "mount_prefix": "/mnt/shared", "entries": entries,
        "entry_count": len(entries), "total_bytes": running,
    }


def _announce_tier(queue: pool.PoolQueue, *, fill: int | None = None,
                   mountpoint: Path | None = None) -> None:
    """Announce the stage tier, optionally with the fill offer it mints."""

    record: dict[str, object] = {
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
        "tier_id": TIER, "host": "sparky", "tier": "stage",
        "mountpoint": str(mountpoint if mountpoint is not None
                          else queue.root / "stage"),
        "mover_python": sys.executable,
        "mover_tools_root": str(Path(pbrun.__file__).resolve().parent),
    }
    if fill is not None:
        record["tokens"] = {storage_tiers.FILL_KIND: fill}
    queue.announce_tier(record)


def _prepare(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """The checkout, queue, tier and argv one submission path needs.

    Built once per test so a second ``pbrun.main`` is a resubmission of the
    same consumer rather than a fresh fixture.
    """

    work = _checkout(tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_raw = json.dumps(_manifest()).encode()
    manifest_path.write_bytes(manifest_raw)

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(
        host="sparky", tags=["sparky", "gb10"], has_gpu=True,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
    )
    _announce_tier(queue, mountpoint=tmp_path / "stage")

    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(work), "--wait-s", "0.01",
        "--detach", "--data-manifest", str(manifest_path),
        "--residency", "stage",
        "--", "/bin/bash", "-lc", "printf staged",
    ])
    return {"queue": queue, "manifest_raw": manifest_raw, "work": work}


def _submit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """One detached ``--residency stage`` submission against a local queue."""

    prepared = _prepare(tmp_path, monkeypatch)
    assert pbrun.main() == 0
    return prepared


def _submit_staged(tmp_path: Path,
                   monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """A submission plus the cycle that publishes its lead.

    The submitter publishes the consumer alone, so a test about what happens
    to a *queued* first phase has to let the loop publish it first -- the same
    cycle the fleet runs, not a hand-written row.
    """

    prepared = _submit(tmp_path, monkeypatch)
    _tier_cycle(prepared["queue"], tmp_path / "stage")
    return prepared


def _price_receipt(queue: pool.PoolQueue, key: str, *, delivered_mb_s: int) -> None:
    """One mover receipt that prices the next window's fill demand.

    ``mover_fill_demand_from_receipts`` takes ``min(file-side rate, window
    delivery / sharers)``, so a single reader with a high file-side rate
    prices the pool at exactly what the window measured it delivering.
    """

    queue.record_move(key, {
        "action_key": key, "tier_id": TIER, "consumer_action_key": "c" * 64,
        "stage_root": str(queue.root / "stage"), "manifest_sha256": "d" * 64,
        "range_start_bytes": 0, "range_end_bytes": PHASE_BYTES,
        "bytes_staged": PHASE_BYTES, "bytes_copied": PHASE_BYTES,
        "complete": True, "seconds": 20.0, "mb_per_s_file_side": 10_000.0,
        "movers_claimed_on_tier": 1,
        "disk_pacing": {"mean_pool_read_mb_s": float(delivered_mb_s)},
        "unix": 1000.0,
    })


def _receipt_blob(tmp_path: Path, key: str) -> dict[str, object]:
    path = tmp_path / "cas" / "requests" / key[:2] / f"{key}.json"
    assert path.exists(), f"sealed action {key[:12]} never reached the CAS"
    return json.loads(path.read_text())


def _detach_key(capsys) -> str:
    lines = [line for line in capsys.readouterr().out.splitlines()
             if line.strip()]
    assert len(lines) == 1, f"stdout carried {len(lines)} lines: {lines!r}"
    return json.loads(lines[0])["action_key"]


def _published(events: list[dict[str, object]]) -> list[str]:
    return [str(event.get("action_key")) for event in events
            if event.get("event") == "mover-published"]


def test_the_submission_publishes_the_consumer_and_no_mover_at_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """The consumer waits on phase 0's mover, and publishes no mover itself.

    The lead used to be published here, and that single row was what blinded
    the coordinator: adoption must skip a leg whose row already exists, so the
    lead alone could never be taken over from a range already on the tier
    (#598 review).  One publisher now, and it is the loop.
    """

    submitted = _submit(tmp_path, monkeypatch)
    queue = submitted["queue"]
    consumer_key = _detach_key(capsys)

    plan = residency_plan.read(queue, consumer_key)
    assert plan is not None, "the submission froze no plan"
    assert [phase["name"] for phase in plan["phases"]] == ["phase-0", "phase-1"]
    lead = str(plan["phases"][0]["mover_row"]["action_key"])
    second = str(plan["phases"][1]["mover_row"]["action_key"])
    assert lead != second, "two phases sealed the same mover"

    consumer = pool._read_json(queue.item_path(pool.READY, consumer_key))
    assert consumer is not None, "the consumer row was not published"
    block = consumer.get("residency")
    assert isinstance(block, dict), "the consumer row carries no residency block"
    assert block["tier_id"] == TIER
    assert block["leads"] == [lead], (
        "the consumer must wait on phase 0's mover and nothing else")
    assert block["manifest_sha256"] == hashlib.sha256(
        submitted["manifest_raw"]).hexdigest()
    assert block["manifest_bytes"] == len(submitted["manifest_raw"])

    assert not queue.item_path(pool.READY, lead).exists(), (
        "the lead is the loop's to adopt or publish, like every other phase")
    assert not queue.item_path(pool.READY, second).exists(), (
        "the second phase publishes as accepted progress advances, not here")
    assert {path.stem for path in queue.dir(pool.READY).glob("*.json")} == {
        consumer_key}, "the submission publishes the consumer and nothing else"


def test_the_frozen_plan_rows_are_the_ones_sealed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """Every sealed body reached the CAS, and the published rows match the plan."""

    submitted = _submit_staged(tmp_path, monkeypatch)
    queue = submitted["queue"]
    consumer_key = _detach_key(capsys)

    plan = residency_plan.read(queue, consumer_key)
    assert plan is not None
    sealed = [consumer_key]
    for phase in plan["phases"]:
        sealed.append(str(phase["mover_row"]["action_key"]))
        sealed.append(str(phase["egress_row"]["action_key"]))
        mover_row = pool._read_json(
            queue.item_path(pool.READY, str(phase["mover_row"]["action_key"])))
        if phase["name"] == "phase-0":
            assert mover_row is not None
            assert mover_row["residency"] == phase["mover_row"]["residency"], (
                "the published mover row is not the plan's sealed row")
            demand = mover_row["resources"]
            assert demand[f"stage_gib@{TIER}"] == 2, (
                "the mover's demand is its range's own ceiling in GiB")
    for key in sealed:
        blob = tmp_path / "cas" / "requests" / key[:2] / f"{key}.json"
        assert blob.exists(), f"sealed action {key[:12]} never reached the CAS"


# -- repricing after a withdrawal goes through this same sealing path (#708) --


STALE_RECEIPT = "a" * 64


def _claim(queue: pool.PoolQueue, key: str) -> None:
    source = queue.item_path(pool.READY, key)
    item = json.loads(source.read_text())
    source.unlink()
    item.update({"action_key": key, "claimed_unix": 1000.0,
                 "claimed_by": "worker", "claimed_host": "sparky"})
    queue.item_path(pool.CLAIMED, key).write_text(json.dumps(item))


def test_a_superseded_window_reseals_at_the_tier_current_offer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """RED before #708: the resubmission reused the frozen plan and its price.

    The wedge's numbers: the window was sealed at a fill demand of 259 MB/s,
    the only receipt then.  The operator withdrew the consumer -- marking the
    plan superseded -- and the tier, which mints what the pool currently
    offers, announced 65 on its next cycle.  The stale receipt is deliberately
    left filed, so a fresh seal at 65 can only come from that current offer.
    The deliberate resubmission seals a *new* plan whose row, CAS body and
    argv all carry 65 -- with a new action key, because a key is the hash of
    the body it seals.
    """

    prepared = _prepare(tmp_path, monkeypatch)
    queue = prepared["queue"]
    _price_receipt(queue, STALE_RECEIPT, delivered_mb_s=259)
    assert pbrun.main() == 0
    consumer_key = _detach_key(capsys)
    # The lead is the loop's to publish now, and this test
    # withdraws it: let the cycle that owns it queue it first.
    _tier_cycle(queue, tmp_path / "stage")

    stale = residency_plan.read(queue, consumer_key)
    assert stale is not None
    stale_lead = str(stale["phases"][0]["mover_row"]["action_key"])
    assert stale["phases"][0]["mover_row"]["resources"][FILL_KIND] == 259
    assert stale["demand_source"]["fill"] == "receipts"

    # The operator ends the stale-priced window.  Marking is immediate; the
    # body stays filed until the old work has ended.
    result = queue.withdraw(consumer_key, reason="stale price", by="operator")
    assert result.get("residency_plan_superseded") is True
    queue.withdraw(stale_lead, reason="stale price", by="operator")
    assert residency_plan.read(queue, consumer_key) is not None
    assert residency_plan.superseded(queue, stale) is not None

    # "Withdraw the consumer and let the tiers loop reap the old window, then
    # resubmit" -- the refusal's own instruction.  The loop publishes this
    # window now, so it is also the thing that takes it back.
    _tier_cycle(queue, tmp_path / "stage")

    # The tier announces what the pool currently offers.
    _announce_tier(queue, fill=65, mountpoint=tmp_path / "stage")

    assert pbrun.main() == 0
    assert _detach_key(capsys) == consumer_key
    fresh = residency_plan.read(queue, consumer_key)
    assert fresh is not None
    mover = fresh["phases"][0]["mover_row"]
    assert mover["resources"][FILL_KIND] == 65
    assert mover["action_key"] != stale_lead, "a repriced row is a new identity"
    assert fresh["demand_source"]["fill"] == "tier-offer-cap"
    assert fresh["demand_source"]["tier_offer_mb_s"] == 65
    assert residency_plan.superseded(queue, fresh) is None, (
        "the stale cancellation never covers the replacement")
    # The old body was reaped by the planner before the fresh one was frozen.
    directory = queue.residency_plan_path(consumer_key).parent
    archived = [path for path in (directory / residency_plan.SUPERSEDED).iterdir()
                if not path.name.endswith(".superseded.json")
                and not path.name.endswith(".marker.json")]
    assert archived

    # Key, sealed body and argv agree about the price the row was published
    # with -- the property #710 refused to break by rewriting in place.  The
    # loop publishes the row now, so the cycle comes before it is read.
    _tier_cycle(queue, tmp_path / "stage")
    ledger_row = pool._read_json(
        queue.item_path(pool.READY, str(mover["action_key"])))
    assert ledger_row is not None
    assert ledger_row["resources"][FILL_KIND] == 65
    blob = _receipt_blob(tmp_path, str(mover["action_key"]))
    assert blob["action_key"] == mover["action_key"]
    assert blob["params"]["demand"][FILL_KIND] == 65
    command = blob["params"]["command"]
    assert command[command.index("--fill-mb-s-pool-side") + 1] == "65"
    assert command[command.index("--range-start-bytes") + 1] == "0"
    consumer_row = pool._read_json(queue.item_path(pool.READY, consumer_key))
    assert consumer_row["residency"]["leads"] == [str(mover["action_key"])]


def test_a_resubmission_refuses_while_the_old_windows_work_is_claimed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """A live claim is never replaced: refuse, and leave the old plan filed."""

    submitted = _submit_staged(tmp_path, monkeypatch)
    queue = submitted["queue"]
    consumer_key = _detach_key(capsys)
    stale = residency_plan.read(queue, consumer_key)
    assert stale is not None
    lead = str(stale["phases"][0]["mover_row"]["action_key"])

    # The lead is running and the consumer is withdrawn: the old window's
    # ownership has not ended.
    _claim(queue, lead)
    queue.withdraw(consumer_key, reason="stale price", by="operator")

    with pytest.raises(SystemExit, match="has not ended"):
        pbrun.main()

    assert residency_plan.read(queue, consumer_key) == stale, (
        "the old plan binding is preserved, never replaced")


@pytest.mark.parametrize("damage", ["corrupt", "unreadable"])
def test_a_resubmission_refuses_an_unreadable_supersession_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys, damage: str,
) -> None:
    """Unknown retirement state is not "not retired": refuse rather than reuse."""

    submitted = _submit(tmp_path, monkeypatch)
    queue = submitted["queue"]
    consumer_key = _detach_key(capsys)
    stale = residency_plan.read(queue, consumer_key)
    assert stale is not None
    # The withdrawal writes a valid marker; damage it in place so the only
    # state the planner can read is "retirement, unreadable".
    queue.withdraw(consumer_key, reason="test", by="test")
    marker = residency_plan.superseded_path(queue, stale)
    if marker.exists():
        marker.unlink()
    marker.parent.mkdir(parents=True, exist_ok=True)
    if damage == "corrupt":
        marker.write_text("{ this is not a marker")
    else:
        marker.mkdir()        # present, and unreadable as a file

    with pytest.raises(SystemExit, match="unreadable"):
        pbrun.main()

    assert residency_plan.read(queue, consumer_key) == stale, (
        "a damaged marker never authorizes a replacement")


def test_an_admission_preemption_keeps_the_frozen_plan_binding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """Admission's own cancellation requeues its holder; the plan is not dead."""

    submitted = _submit_staged(tmp_path, monkeypatch)
    queue = submitted["queue"]
    consumer_key = _detach_key(capsys)
    stale = residency_plan.read(queue, consumer_key)
    assert stale is not None
    lead = str(stale["phases"][0]["mover_row"]["action_key"])

    _claim(queue, lead)
    result = queue.withdraw(lead, reason="preempted for foreground work",
                            by="admission", preempted_by="f" * 64)
    assert result["status"] == "withdrawn"
    assert result.get("residency_plan_superseded") is False

    # The plan is still the agreed decomposition; a retry reuses it rather
    # than refusing or repricing.
    reused = residency_plan.read(queue, consumer_key)
    assert reused == stale
    assert residency_plan.superseded(queue, reused) is None


# -- a fresh seal renews the cancellations a reaped window left (#708 review) --


def _stage_tier(tmp_path: Path) -> dict[str, object]:
    """The tier record ``residency_window`` runs a cycle against."""

    return {"tier_id": TIER, "tier": "stage",
            "mountpoint": str(tmp_path / "stage")}


def _announce_ram_tier(queue: pool.PoolQueue, mountpoint: Path) -> None:
    """Announce the ram tier this stage host fronts, so the seal carries one.

    The ram leg is sealed by the same submission and its promotion keys are
    children of the same plan, so the renewal's scope is exercised on both.
    """

    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
        "tier_id": RAM_TIER, "host": "sparky", "tier": "ram",
        "mountpoint": str(mountpoint),
        "mover_python": sys.executable,
        "mover_tools_root": str(Path(pbrun.__file__).resolve().parent),
    })


def _reaped_window_with_cancelled_children(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> tuple[pool.PoolQueue, str, dict[str, object], str, str]:
    """The causal chain a same-body renewal exists for, through the real path.

    A submission publishes its consumer and its lead only; the window
    publishes phase 1's stage copy as the consumer would advance, and the ram
    window's promotion has been published for it; an operator cancels both
    later children; the consumer is withdrawn; the dead-consumer pass stops
    the queued lead and archives the plan; and a deliberate resubmission
    seals the identical body -- same price, same tool, same consumer, so the
    same child keys.

    Returns ``(queue, consumer_key, fresh_plan, late_stage, late_ram)``.
    """

    prepared = _prepare(tmp_path, monkeypatch)
    queue = prepared["queue"]
    _announce_ram_tier(queue, tmp_path / "ram")
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    assert pbrun.main() == 0
    consumer_key = _detach_key(capsys)
    stale = residency_plan.read(queue, consumer_key)
    assert stale is not None
    late_stage = str(stale["phases"][1]["mover_row"]["action_key"])
    late_ram = str(stale["phases"][1]["ram_mover_row"]["action_key"])

    events = tier_loop.residency_window(queue, tiers={TIER: _stage_tier(tmp_path)})
    assert late_stage in _published(events), (
        "the window must publish the later stage copy before it can be cancelled")
    queue.publish(**dict(stale["phases"][1]["ram_mover_row"]), recompute=True)

    queue.withdraw(late_stage, reason="stale price", by="operator")
    queue.withdraw(late_ram, reason="stale price", by="operator")
    tier_loop.residency_window(queue, tiers={TIER: _stage_tier(tmp_path)})
    assert residency_plan.superseded(queue, stale) is not None

    # The old ownership ends: the consumer is withdrawn, and the pass that
    # stops a dead consumer's work withdraws the queued lead and archives the
    # plan once nothing names it.
    result = queue.withdraw(consumer_key, reason="stale price", by="operator")
    assert result.get("residency_plan_superseded") is True
    tier_loop.withdraw_dead_consumer_movers(queue)
    assert residency_plan.read(queue, consumer_key) is None, (
        "the old plan was not reaped; the renewal boundary is not reached")

    assert pbrun.main() == 0
    assert _detach_key(capsys) == consumer_key
    fresh = residency_plan.read(queue, consumer_key)
    assert fresh is not None
    assert fresh["phases"][1]["mover_row"]["action_key"] == late_stage
    assert fresh["phases"][1]["ram_mover_row"]["action_key"] == late_ram
    return queue, consumer_key, fresh, late_stage, late_ram


def test_a_deliberate_reseal_renews_a_reaped_windows_cancelled_children(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """RED before the renewal fix: the old markers outlived the reap.

    The fresh plan is a new filing of the same body, so the predecessor's
    visible cancellations on its later stage and ram movers still stand when
    the next window cycle reads them -- and would supersede the fresh plan
    before its second phase ever published.  The deliberate seal retires them
    as evidence, and the fresh window is a schedule again.
    """

    queue, _consumer_key, fresh, late_stage, late_ram = (
        _reaped_window_with_cancelled_children(tmp_path, monkeypatch, capsys))

    assert residency_plan.superseded(queue, fresh) is None, (
        "the predecessor's cancellations must not cover the fresh filing")
    assert queue.live_withdrawal(late_stage) is None
    assert queue.live_withdrawal(late_ram) is None
    assert queue.withdrawal_decisions(late_stage), (
        "the operator's decision is evidence, not something retirement erases")
    assert queue.withdrawal_decisions(late_ram)
    retired = {path.name for path in queue.superseded_dir().iterdir()}
    assert any(late_stage in name for name in retired)
    assert any(late_ram in name for name in retired)

    events = tier_loop.residency_window(queue, tiers={TIER: _stage_tier(tmp_path)})

    assert residency_plan.superseded(queue, fresh) is None, (
        "the fresh plan must survive its own window cycle")
    assert late_stage in _published(events), (
        "the fresh plan's later children must be publishable")
    assert queue.item_path(pool.READY, late_stage).exists()
    assert queue.live_withdrawal(late_ram) is None


def test_a_cancellation_after_the_renewal_still_supersedes_the_fresh_plan(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """The renewal boundary is the seal; a later decision is the new plan's.

    The automatic publisher never broadly ignores cancellations: a marker
    filed against a child of the renewed window supersedes the fresh plan
    exactly as one filed against any live window would, and no further mover
    is published.
    """

    queue, _consumer_key, fresh, late_stage, _late_ram = (
        _reaped_window_with_cancelled_children(tmp_path, monkeypatch, capsys))
    assert residency_plan.superseded(queue, fresh) is None, (
        "the renewal itself must not supersede the fresh plan")

    events = tier_loop.residency_window(queue, tiers={TIER: _stage_tier(tmp_path)})
    assert late_stage in _published(events), (
        "the renewed window is a schedule until a new decision cancels it")
    assert queue.item_path(pool.READY, late_stage).exists()

    queue.withdraw(late_stage, reason="changed my mind", by="operator")
    events = tier_loop.residency_window(queue, tiers={TIER: _stage_tier(tmp_path)})

    assert late_stage not in _published(events)
    assert not queue.item_path(pool.READY, late_stage).exists()
    assert residency_plan.superseded(queue, fresh) is not None, (
        "a cancellation filed after the renewal is a decision about the new plan")


def test_a_resubmission_refuses_an_unreadable_child_cancellation(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """Unknown cancellation state is not "no cancellation": refuse the seal.

    The marker is present and unreadable, so nobody can say which generation
    it stopped.  A fresh seal over it would be a guess, and the submission
    refuses by name instead of clearing it.
    """

    prepared = _prepare(tmp_path, monkeypatch)
    queue = prepared["queue"]
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    assert pbrun.main() == 0
    consumer_key = _detach_key(capsys)
    stale = residency_plan.read(queue, consumer_key)
    assert stale is not None
    late = str(stale["phases"][1]["mover_row"]["action_key"])
    events = tier_loop.residency_window(queue, tiers={TIER: _stage_tier(tmp_path)})
    assert late in _published(events)
    queue.withdraw(late, reason="stale price", by="operator")
    queue.withdraw(consumer_key, reason="stale price", by="operator")
    tier_loop.withdraw_dead_consumer_movers(queue)

    marker = queue.item_path(pool.WITHDRAWN, late)
    marker.unlink()
    marker.mkdir()        # present, and unreadable as a marker

    with pytest.raises(SystemExit, match="cannot be read"):
        pbrun.main()

    assert residency_plan.read(queue, consumer_key) is None, (
        "no fresh plan may be sealed over a cancellation nobody can read")
    assert not queue.item_path(pool.READY, consumer_key).exists()


# ------------------------------------- the lead the coordinator never saw

def _resident_donor(queue: pool.PoolQueue, *, stage: Path, mover: str,
                    consumer: str, manifest_sha256: str,
                    start: int = 0, end: int = PHASE_BYTES,
                    files: int = 2,
                    material: int | None = 2) -> list[Path]:
    """Drive the tier into the state a finished mover of this range leaves.

    Tokens held, files on the device, a fragment naming them, a dated sidecar
    over those files and a receipt saying the copy completed -- what
    ``adoptable_ranges`` indexes and what ``adopt`` validates, filed the way
    ``stage_move`` files it.  ``consumer`` is a *finished* consumer: it holds
    no queue row, which is what makes the range one a successor may take over
    rather than one in use.

    ``material`` is how many of the fragment's entries the sidecar dates: all
    of them by default, fewer for a partial vouch, ``None`` for a legacy range
    with no sidecar at all.
    """

    # What the loop's own cycle would have done for this tier: capacity minted
    # so a token can be held, and the stage root registered (#628).
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    stage.mkdir(parents=True, exist_ok=True)
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)

    entries: dict[str, object] = {}
    written: list[Path] = []
    for index in range(files):
        path = stage / "donor" / f"part-{index}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x" * 32)
        written.append(path)
        entries[residency_map.residency_map_key(
            f"/mnt/shared/part-{index}", 0)] = {
                "stage_path": str(path), "bytes": 32, "offset": 0,
                "sha256": "a" * 64}
    assert queue.tier_ledger(TIER).acquire(
        mover, {"stage_gib": (end - start) // GIB})
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": manifest_sha256, "entries": entries})
    if material is not None:
        reader_lease.write_material(
            queue.residency_fragment_root(), consumer_action_key=consumer,
            mover_action_key=mover, tier_id=TIER, stage_root=str(stage),
            manifest_sha256=manifest_sha256, generation="a" * 32,
            entries={key: {**dict(mention),
                           "file_id": reader_lease.stat_identity(
                               str(mention["stage_path"]))}
                     for key, mention in list(entries.items())[:material]})
    queue.record_move(mover, {
        "consumer_action_key": consumer, "tier_id": TIER,
        "stage_root": str(stage), "manifest_sha256": manifest_sha256,
        "range_start_bytes": start, "range_end_bytes": end,
        "range_bytes": end - start, "bytes_staged": end - start,
        "entries_declared": files, "entries_staged": files,
        "complete": True, "seconds": 1.0, "unix": 1000.0})
    return written


def _digest(prepared: dict[str, object]) -> str:
    return hashlib.sha256(prepared["manifest_raw"]).hexdigest()


def _tier_cycle(queue: pool.PoolQueue, stage: Path) -> None:
    """One whole tier cycle, the way the loop on the storage box runs it.

    Since #598 review this is also what publishes a submission's lead, so a
    test that needs a staged first phase runs a cycle rather than expecting
    the submitter to have queued one.
    """

    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    stage.mkdir(parents=True, exist_ok=True)
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    # The loop narrates its cycle on stdout; ``_detach_key`` reads stdout for
    # the submission's one JSON line, so the narration is kept out of it.
    with contextlib.redirect_stdout(io.StringIO()):
        tier_loop.cycle(queue, host="sparky", source_pool="storage_pool",
                        receipts=tier_loop.ReceiptCache(),
                        discover=lambda **_kwargs: {
                            TIER: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                                   "tier_id": TIER, "host": "sparky",
                                   "tier": "stage", "mountpoint": str(stage),
                                   "capacity_bytes": 8 * GIB}})


def test_a_cold_lead_is_published_by_the_loop_on_its_next_cycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """Nothing resident: the copy still happens, one cycle later.

    The accepted cost of having a single publisher.  A cold submission must
    still reach a staged lead by itself -- through the loop that already
    publishes every other phase -- or moving the publication would strand it.
    """

    prepared = _prepare(tmp_path, monkeypatch)
    queue = prepared["queue"]
    queue.mint_tier_capacity(TIER, {"stage_gib": 8})
    (tmp_path / "stage").mkdir(exist_ok=True)
    stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=tmp_path / "stage")

    assert pbrun.main() == 0
    consumer_key = _detach_key(capsys)
    plan = residency_plan.read(queue, consumer_key)
    assert plan is not None
    lead = str(plan["phases"][0]["mover_row"]["action_key"])
    assert not queue.item_path(pool.READY, lead).exists(), "precondition"

    _tier_cycle(queue, tmp_path / "stage")

    assert queue.item_path(pool.READY, lead).exists(), (
        "a cold lead nobody can adopt must be published by the loop")


def test_a_warm_lead_is_adopted_by_the_loop_and_never_copied(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """The win: an identical range already on the tier is taken over.

    The case the fleet hit -- a finished mover's range, complete, unreserved
    and descriptor-identical, sitting on the stage while a successor copied
    the same bytes again.  No copy is queued, no byte moves, and the lead
    holds the range under its own name.
    """

    prepared = _prepare(tmp_path, monkeypatch)
    queue = prepared["queue"]
    donor = "d" * 64
    _resident_donor(queue, stage=tmp_path / "stage", mover=donor,
                    consumer="f" * 64, manifest_sha256=_digest(prepared))

    assert pbrun.main() == 0
    consumer_key = _detach_key(capsys)
    plan = residency_plan.read(queue, consumer_key)
    assert plan is not None
    lead = str(plan["phases"][0]["mover_row"]["action_key"])

    _tier_cycle(queue, tmp_path / "stage")

    ledger = queue.tier_ledger(TIER)
    assert ledger.holder_tokens(lead) == {"stage_gib": PHASE_BYTES // GIB}
    assert ledger.holder_tokens(donor) == {}, (
        "the donor still holds tokens for bytes the lead now owns")
    receipt = queue.move_record(lead)
    assert isinstance(receipt, dict)
    assert receipt[pool.MOVE_ADOPTED_FROM_FIELD] == donor
    assert receipt["bytes_copied"] == 0, "adoption copies no byte"
    assert not queue.item_path(pool.READY, lead).exists(), (
        "an adopted range was queued for copying as well")
    assert not queue.item_path(pool.CLAIMED, lead).exists()


def test_an_eager_worker_finds_no_lead_to_claim_before_the_loop_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """A worker that claims the instant the submission returns copies nothing.

    The window the old ordering left open: the lead was in ``ready`` before
    any cycle could look, so an eager worker could claim and start copying a
    range that was already on the tier.  With nothing published there is
    nothing to claim, and the range is adopted instead.
    """

    prepared = _prepare(tmp_path, monkeypatch)
    queue = prepared["queue"]
    donor = "d" * 64
    _resident_donor(queue, stage=tmp_path / "stage", mover=donor,
                    consumer="f" * 64, manifest_sha256=_digest(prepared))

    assert pbrun.main() == 0
    consumer_key = _detach_key(capsys)
    plan = residency_plan.read(queue, consumer_key)
    assert plan is not None
    lead = str(plan["phases"][0]["mover_row"]["action_key"])

    # The eager worker, before any tier cycle: nothing of this plan is
    # claimable, so no copy of a resident range can start.
    claimable = {path.stem for path in queue.dir(pool.READY).glob("*.json")}
    assert lead not in claimable
    assert claimable == {consumer_key}

    _tier_cycle(queue, tmp_path / "stage")
    assert queue.move_record(lead)[pool.MOVE_ADOPTED_FROM_FIELD] == donor


@pytest.mark.parametrize("material, shape", [
    (None, "a legacy range with no sidecar at all"),
    (1, "a sidecar that dates only some of the range's files"),
])
def test_a_donor_the_reader_could_not_use_is_declined_and_the_copy_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
    material: int | None, shape: str,
) -> None:
    """Undated and partial donors are refused by adoption itself.

    ``adopt`` used to validate a donor's dated material only when there was
    any, and to carry it forward on the same condition -- so a donor with no
    sidecar adopted unvalidated and vouched its successor with a fragment
    alone: a range the strict reader cannot prove.  Handing a consumer that is
    worse than copying the bytes again, so it is declined, and the ordinary
    copy is published instead.
    """

    prepared = _prepare(tmp_path, monkeypatch)
    queue = prepared["queue"]
    donor = "d" * 64
    _resident_donor(queue, stage=tmp_path / "stage", mover=donor,
                    consumer="f" * 64, manifest_sha256=_digest(prepared),
                    material=material)

    assert pbrun.main() == 0
    consumer_key = _detach_key(capsys)
    plan = residency_plan.read(queue, consumer_key)
    assert plan is not None
    lead = str(plan["phases"][0]["mover_row"]["action_key"])

    _tier_cycle(queue, tmp_path / "stage")

    assert queue.move_record(lead) is None, (
        f"{shape} was adopted: the reader could not prove that range")
    assert queue.item_path(pool.READY, lead).exists(), (
        f"{shape} must fall back to the ordinary copy, not strand the lead")
    assert queue.tier_ledger(TIER).holder_tokens(donor) == {
        "stage_gib": PHASE_BYTES // GIB}, "a declined donor lost its tokens"


def test_a_valid_donor_is_still_taken_when_an_unusable_one_sorts_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """Declining one candidate must not cost the range a usable one.

    Candidates are tried in sorted key order, so an undated donor can be
    reached first.  Refusing it has to fall through to the next candidate for
    the same descriptor rather than end the attempt -- otherwise a single
    legacy range on the tier would suppress every adoption behind it.
    """

    prepared = _prepare(tmp_path, monkeypatch)
    queue = prepared["queue"]
    unusable, usable = "1" * 64, "2" * 64      # "1" sorts before "2"
    _resident_donor(queue, stage=tmp_path / "stage", mover=unusable,
                    consumer="e" * 64, manifest_sha256=_digest(prepared),
                    material=None)
    _resident_donor(queue, stage=tmp_path / "stage", mover=usable,
                    consumer="f" * 64, manifest_sha256=_digest(prepared))

    assert pbrun.main() == 0
    consumer_key = _detach_key(capsys)
    plan = residency_plan.read(queue, consumer_key)
    assert plan is not None
    lead = str(plan["phases"][0]["mover_row"]["action_key"])

    _tier_cycle(queue, tmp_path / "stage")

    receipt = queue.move_record(lead)
    assert isinstance(receipt, dict), (
        "the undated donor suppressed the usable one behind it")
    assert receipt[pool.MOVE_ADOPTED_FROM_FIELD] == usable
    assert not queue.item_path(pool.READY, lead).exists()
