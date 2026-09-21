"""Retiring a produced batch from a box that cannot write the stage (#801).

Only the tier host mounts the stage read-write. The GPU hosts mount it
read-only, and a produced-output owner runs on a GPU host, so an egress that
unlinks staged files in the owner's own process deletes nothing there: the
window credit never comes back and the next window never funds.

These tests put the owner on "another box" the only way a single-box fixture
can do it honestly: the ANNOUNCED TIER RECORD names a host that is not this
one, and the stage directory is made read-only for the owner's calls, which is
the owner's real view of it. The egress then has to run as the queue action
`retire_batch` publishes for the tier host, and it is executed for real through
the same `Pool.execute` seam the movers use, with the directory writable again
-- the tier host's real view.

Same real primitives and the same disclosed fixture concessions as
``tests/test_produced_output_restage.py``, whose world this reuses.
"""
from __future__ import annotations

import json
import os
import socket
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import prismabuild.pool as pool  # noqa: E402
import prismabuild.produced_output as po  # noqa: E402
from prismabuild import residency_map as map_mod  # noqa: E402
import test_produced_output_restage as restage  # noqa: E402

TIER = restage.TIER
KIND = restage.KIND
ELSEWHERE = "tier-host-elsewhere"
#: The wire value, spelled out: an owner in another repository matches on it.
OWN_EGRESS = "own-egress-in-flight"


@pytest.fixture(autouse=True)
def _isolated_synthetic_launch_context(monkeypatch):
    """Standalone synthetic launch contexts never inherit an outer tuple."""

    for name in ("PRISMABUILD_ACTION_NONCE", "PRISMABUILD_ACTION_SCOPE",
                 "PRISMABUILD_READER_HELPER_ROOT", "PRISMABUILD_ACTION_KEY"):
        monkeypatch.delenv(name, raising=False)


class _ReadOnlyStage:
    """The owner's view of the stage: mounted, readable, not writable."""

    def __init__(self, stage_root: Path):
        self.stage_root = stage_root

    def __enter__(self):
        os.chmod(self.stage_root, 0o555)
        return self

    def __exit__(self, *exc):
        os.chmod(self.stage_root, 0o755)


def _world_with_the_stage_elsewhere(tmp_path: Path, **kwargs):
    world = restage._World(tmp_path, **kwargs)
    path = Path(world.q.root) / "tiers" / f"{TIER}.json"
    record = json.loads(path.read_text())
    assert record["host"] == socket.gethostname()
    record["host"] = ELSEWHERE
    pool._write_json_atomic(path, record)
    assert restage._tier_host(world.q) == ELSEWHERE
    return world


def _staged_batch(world, batch_id: str, tag: str, payload: bytes):
    descs = restage._descriptors(world.template, world.inst, tag, payload)
    restage._prewrite(world.q, world.inst, world.template, batch_id, descs)
    first = world.first_publish(batch_id, descs)
    return descs, first


def _run_on_the_tier_host(world, action_key: str, worker: str) -> dict:
    """Claim and execute one row the way the tier host's worker would."""

    claimed = world.q.claim(owner=worker, tags=[ELSEWHERE])
    assert claimed is not None, "the row must be claimable on the tier host"
    assert claimed["action_key"] == action_key, claimed["action_key"]
    row = pool._read_json(world.q.item_path(pool.CLAIMED, action_key))
    assert isinstance(row, dict)
    return world.q.execute(row, timeout_s=240)


def _assert_own_egress_deferral(outcome: dict) -> str:
    said = json.dumps(outcome, sort_keys=True, default=str)
    assert outcome.get("ok") is False, said
    assert outcome.get("refusal") == "egress-incomplete", said
    receipt = outcome["receipt"]
    assert receipt["complete"] is False, said
    assert receipt["deferred_own"] == [OWN_EGRESS], said
    assert receipt["errors"] == [] and receipt["live_pins"] == [], said
    key = str(outcome["egress_action_key"])
    assert len(key) == 64 and receipt["egress_action_key"] == key
    return key


def test_a_batch_retires_through_an_egress_action_on_the_tier_host(
        tmp_path: Path) -> None:
    """Forward copy, retire from a read-only view, restage, retire again.

    RED on the base: `retire_batch` runs `stage_release.evict` in the owner's
    process, whose `os.unlink` the read-only stage refuses, so the answer is
    `egress-incomplete` carrying a permission error, no action is published,
    and no later call can ever do better.
    """

    world = _world_with_the_stage_elsewhere(tmp_path, window_gib=1, gib=4)
    payload = bytes(range(256)) * 4
    _descs, first = _staged_batch(world, "b1", "p1", payload)
    mover0 = str(first["mover_key"])
    ns = str(first["batch_namespace"])
    world.run_mover(mover0, "w-fwd")
    staged = world.stage_root / "p1.bin"
    assert staged.read_bytes() == payload
    assert world.ledger.holder_tokens(mover0).get(KIND, 0) == 1

    with _ReadOnlyStage(world.stage_root):
        deferred = world.retire("b1")
        egress0 = _assert_own_egress_deferral(deferred)
        assert deferred["receipt"]["egress_state"] == "published"
        assert egress0 != mover0
        assert po.OWN_EGRESS_IN_FLIGHT == OWN_EGRESS

        # Nothing was deleted from here, and nothing was filed as retired.
        assert staged.read_bytes() == payload
        assert world.ledger.holder_tokens(mover0).get(KIND, 0) == 1
        entry = world.entry("b1")
        assert not entry.get("retired")
        # The staged paths are on file BEFORE the egress can drop the fragment.
        assert entry["staged_paths"] == [str(staged)]

        # The row `pbrun` publishes for a consumer's egress: on the tier host,
        # one CPU and one GiB, no tier demand, no residency, no batch
        # reference, and marked so a republished key runs again.
        row = pool._read_json(world.q.item_path(pool.READY, egress0))
        assert isinstance(row, dict)
        assert row["tags"] == [ELSEWHERE]
        assert row["resources"]["cpu"] == 1 and row["resources"]["mem_gb"] == 1
        assert not [name for name in row["resources"] if "@" in str(name)]
        assert "residency" not in row and "produced_output_batch" not in row
        assert row.get("recompute") is True
        assert row["max_attempts"] == 3 and row.get("retry_safe") is True

        # A re-drive while the action is queued publishes nothing more.
        again = world.retire("b1")
        assert _assert_own_egress_deferral(again) == egress0
        assert again["receipt"]["egress_state"] == pool.READY

    # The tier host's view: writable. The egress is the fleet's own tool.
    outcome = _run_on_the_tier_host(world, egress0, "w-tier-egress-0")
    assert outcome.get("returncode") == 0, outcome
    world.q.finish(egress0, status="executed")
    assert not staged.exists()
    assert world.ledger.holder_tokens(mover0).get(KIND, 0) == 0
    assert map_mod.read_fragments(world.out_base, ns) == []

    with _ReadOnlyStage(world.stage_root):
        retired = world.retire("b1")
        assert retired.get("ok") is True, retired
        assert str(retired["mover_key"]) == mover0
        assert int(retired["generation"]) == 0
        assert retired["receipt"]["complete"] is True
        assert retired["receipt"]["action_key"] == mover0
        assert retired["receipt"]["egress_action_key"] == egress0
        # The fragment was gone by this call; the paths still are not.
        assert retired["staged_paths"] == [str(staged)]
        entry = world.entry("b1")
        assert entry["retired"] is True
        assert entry["staged_paths"] == [str(staged)]
        duplicate = world.retire("b1")
        assert duplicate.get("ok") is True and duplicate.get("duplicate") is True

    # The same batch again, as a successor materialization, through the same
    # route: a different mover, so a different egress action.
    refill = po.refill_window(world.q, world.inst, world.template, tier=TIER)
    assert refill.get("ok") is True, refill
    ensured = world.ensure("b1")
    assert ensured.get("ok") is True, ensured
    assert int(ensured["generation"]) == 1
    mover1 = str(ensured["mover_key"])
    assert mover1 != mover0
    world.run_mover(mover1, "w-rev")
    assert staged.read_bytes() == payload

    with _ReadOnlyStage(world.stage_root):
        egress1 = _assert_own_egress_deferral(world.retire("b1"))
        assert egress1 not in (egress0, mover0, mover1)
        assert staged.read_bytes() == payload
    outcome = _run_on_the_tier_host(world, egress1, "w-tier-egress-1")
    assert outcome.get("returncode") == 0, outcome
    world.q.finish(egress1, status="executed")
    assert not staged.exists()
    with _ReadOnlyStage(world.stage_root):
        retired = world.retire("b1")
        assert retired.get("ok") is True, retired
        assert str(retired["mover_key"]) == mover1
        assert int(retired["generation"]) == 1
        materialization = world.entry("b1")["materializations"][0]
        assert materialization["retired"] is True
        assert materialization["staged_paths"] == [str(staged)]
    assert world.ledger.holder_tokens(mover1).get(KIND, 0) == 0


def test_a_copy_still_in_flight_is_never_raced_by_its_egress(
        tmp_path: Path) -> None:
    """The own-copy answer (#795) is given before any egress is published."""

    world = _world_with_the_stage_elsewhere(tmp_path, window_gib=1, gib=4)
    _descs, first = _staged_batch(world, "b1", "p1", b"Q" * 900)
    mover0 = str(first["mover_key"])
    assert po._mover_live_state(world.q, mover0) == pool.READY

    outcome = world.retire("b1")
    assert outcome.get("ok") is False, outcome
    assert outcome.get("refusal") == "egress-incomplete", outcome
    assert outcome["receipt"]["deferred_own"] == ["own-copy-in-flight"]
    assert "egress_action_key" not in outcome
    ready = sorted(path.stem for path in
                   (Path(world.q.root) / pool.READY).glob("*.json"))
    assert ready == [mover0], ready


@pytest.mark.skipif(not restage.HAS_SDK,
                    reason="needs the accepted reader_lease SDK")
def test_an_egress_a_live_reader_blocks_answers_with_its_own_receipt(
        tmp_path: Path) -> None:
    """A refused egress is answered with the receipt the tier host filed.

    The cause has to reach the owner as the in-process egress reported it --
    `live_pins`, never a deferral -- and the next re-drive has to get a fresh
    attempt instead of the spent one's receipt.
    """

    rlc = restage.rlc
    world = _world_with_the_stage_elsewhere(tmp_path, window_gib=1, gib=4)
    payload = b"L" * 1500
    _descs, first = _staged_batch(world, "b1", "p1", payload)
    mover0 = str(first["mover_key"])
    manifest = str(first["manifest_digest"])
    world.run_mover(mover0, "w-fwd")
    staged = world.stage_root / "p1.bin"
    read = world.pin(mover0, manifest, len(payload))
    assert read.get("ok") is True, read

    egress = _assert_own_egress_deferral(world.retire("b1"))
    attempts = 0
    while po._mover_live_state(world.q, egress) == pool.READY:
        attempts += 1
        outcome = _run_on_the_tier_host(world, egress, f"w-tier-{attempts}")
        assert outcome.get("returncode") == 1, outcome
        world.q.finish(egress, status="failed")
        assert attempts <= 3, "the row's own retry budget is three"
    assert attempts == 3
    assert po._mover_live_state(world.q, egress) == pool.FAILED
    assert staged.read_bytes() == payload

    blocked = world.retire("b1")
    assert blocked.get("ok") is False, blocked
    assert blocked.get("refusal") == "egress-incomplete", blocked
    assert blocked["egress_action_key"] == egress
    assert blocked["receipt"]["live_pins"], blocked
    assert blocked["receipt"]["deferred_own"] == []
    assert blocked["receipt"]["action_key"] == mover0
    # ...and the same key is queued again, as a fresh attempt.
    assert po._mover_live_state(world.q, egress) == pool.READY
    assert not world.entry("b1").get("retired")

    assert rlc.release(world.q, read["pin_id"], read["ref_id"],
                       consumer_action_key=restage.DOWNSTREAM,
                       residency_root=str(world.out_base)) is True
    outcome = _run_on_the_tier_host(world, egress, "w-tier-after-release")
    assert outcome.get("returncode") == 0, outcome
    world.q.finish(egress, status="executed")
    assert not staged.exists()
    retired = world.retire("b1")
    assert retired.get("ok") is True, retired
    assert retired["receipt"]["egress_action_key"] == egress
    assert world.ledger.holder_tokens(mover0).get(KIND, 0) == 0


def test_the_tier_host_itself_still_retires_in_one_call(tmp_path: Path) -> None:
    """On the box that owns the stage nothing changes: no action, one call."""

    world = restage._World(tmp_path, window_gib=1, gib=4)
    assert restage._tier_host(world.q) == socket.gethostname()
    _descs, first = _staged_batch(world, "b1", "p1", b"H" * 800)
    mover0 = str(first["mover_key"])
    world.run_mover(mover0, "w-fwd")
    retired = world.retire("b1")
    assert retired.get("ok") is True, retired
    assert "egress_action_key" not in retired["receipt"]
    assert not (world.stage_root / "p1.bin").exists()
    assert list((Path(world.q.root) / pool.READY).glob("*.json")) == []
