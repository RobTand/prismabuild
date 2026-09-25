"""A stage egress reports what it has released, and a wedged egress ends (#1021).

An egress holds the stage's ownership lock from its snapshot to its release
(``stage_release._evict_locked``), and every mover takes the same lock at its
start gate before its first rename (``PoolQueue.ownership_start_gate``).  An
egress that stops advancing inside the lock -- an NFS stall, a slow unlink
run -- therefore blocks every mover on the stage.  Since #1010 the worker
credits a mover's wait while the lock is held, so the mover is not killed,
but the egress itself sealed no progress and no deadline: nothing reported
the stall and nothing ended it before the worker's execution ceiling.

Now pbrun seals each stage egress with the #480 contract, priced from the
egresses the stage has receipted; ``stage_release`` reports the bytes it has
released on its heartbeat; the worker ends an egress that stops; and the
mover's progress record names the egress it waited on.

Everything runs on ``tmp_path`` queues, stages and sources (#628).
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import textwrap
import threading
import time

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    TIER, _hexkey)
from test_a_stalled_mover_ends_no_progress import (  # noqa: E402
    CONSUMER, HEARTBEAT, MEMBERS, MIB, UNDER, _Disks, _contention,
    _mover_source, _policy, _real_window, _seal_rows)

#: The consumer the mover waiting at the gate stages for.  Its range is a
#: second manifest, so its staged names never meet the egressed range's.
WAITING = _hexkey("waiting-at-the-gate")

#: The egress under test: the real ``stage_release.main``, with ``os.unlink``
#: replaced in its own process for the staged files it drains.  Each unlink
#: of a staged file is noted in ``marker`` first; the ``after``-th and every
#: later one hangs (``None``: none does), and each one sleeps ``delay``.
EGRESS = textwrap.dedent('''
    import os, sys, time
    sys.path.insert(0, {tools!r})
    import stage_release
    from prismabuild import pool
    pool.HEARTBEAT_S = {heartbeat!r}
    stage = os.path.realpath({stage!r}) + os.sep
    real = os.unlink
    calls = [0]
    after = {after!r}
    def unlink(path, *args, **kwargs):
        name = os.fspath(path)
        if (os.path.realpath(name).startswith(stage)
                and not os.path.basename(name).startswith(".")):
            calls[0] += 1
            with open({marker!r}, "a") as stream:
                stream.write(name + "\\n")
            if after is not None and calls[0] >= after:
                time.sleep(3600)
            time.sleep({delay!r})
        return real(path, *args, **kwargs)
    os.unlink = unlink
    code = stage_release.main({argv!r})
    receipt = {receipt!r}
    if os.path.exists(receipt):
        with open(receipt, "rb") as src, open("result", "wb") as dst:
            dst.write(src.read())
    sys.exit(code)
''')


def _egress_receipt(stage_root: Path) -> dict[str, object]:
    """One earlier egress of three entries on this stage, as ``stage_release``
    files it (``_evict_owned`` and ``_hold_record``): the census it took
    before the lock and under it, its hold, its unlinks and its prune."""

    return {"schema": pool.POOL_EGRESS_SCHEMA_V1, "action_key": "9" * 64,
            "consumer_action_key": "e" * 64, "stage_root": str(stage_root),
            "reason": "egress", "complete": True, "errors": [],
            "entries_judged": 3, "entries_deleted": 3,
            "entries_already_gone": 0, "entries_shared": 0,
            "entries_deferred": 0, "bytes_deleted": 3 * MIB,
            "census_s": 4.0, "census_validate_s": 0.5, "lock_wait_s": 0.0,
            "lock_held_s": 1.0, "unlink_s": 0.03, "prune_s": 0.0,
            "unix": 100.0}


def _claimed(queue: pool.PoolQueue, cas: pb.PrismaBuildCAS, base: Path,
             name: str, source: str, params: dict[str, object]) -> dict:
    """Publish one action running ``source`` with ``params`` and claim it."""

    checkout = base / f"checkout-{name}"
    checkout.mkdir()
    (checkout / "task.py").write_text(source)
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": f"tests/wedged-egress-{name}",
                 "definition_version": "v1",
                 "task_class": "generation", "determinism": "deterministic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "argv": [sys.executable, "task.py"],
                 "working_directory": ".", "result_path": "result"},
        "inputs": [], "code_closure": pb.build_code_closure(checkout, ["task.py"]),
        "params": params,
        "environment": {"variables": {}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    })
    cas.publish_action_request(action)
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_root=checkout, max_attempts=1,
                  worker_script=ROOT / "tools" / "prismabuild_worker.py")
    item = queue.claim()
    assert item is not None and item["action_key"] == action["action_key"]
    return item


def _mover_argv(base: Path, *, consumer: str, manifest: Path, total: int,
                receipt: Path) -> list[str]:
    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    return ["--pool-root", str(base / "queue"),
            "--consumer-action-key", consumer, "--tier-id", TIER,
            "--stage-root", str(base / "stage"), "--manifest-sha256", digest,
            "--manifest", str(manifest),
            "--range-start-bytes", "0", "--range-end-bytes", str(total),
            "--readers", "1", "--max-readers", "1", "--block", str(MIB),
            "--warm-after-copy", "never", "--unpaced",
            "--receipt", str(receipt)]


def _second_window(base: Path) -> tuple[Path, int]:
    """Three 1 MiB entries under other names, for the mover at the gate."""

    mount = base / "gate-sources"
    mount.mkdir()
    listed = []
    for index in range(3):
        payload = hashlib.sha256(f"gate-{index}".encode()).digest() * (MIB // 32)
        source = mount / f"gate-{index}.bin"
        source.write_bytes(payload)
        listed.append({"path": str(source), "offset": 0, "bytes": MIB,
                       "sha256": hashlib.sha256(payload).hexdigest()})
    total = 3 * MIB
    manifest = base / "gate-manifest.json"
    manifest.write_text(json.dumps({
        "schema": pb.DATA_MANIFEST_SCHEMA_V1, "produced_by": {},
        "annotations": {"phases": [{"name": "layer-0", "bytes": total,
                                    "cumulative_bytes": total}]},
        "mount_prefix": str(mount), "entries": listed,
        "entry_count": 3, "total_bytes": total}))
    return manifest, total


def _egress_rows(plan: dict[str, object]) -> list[dict[str, object]]:
    rows = []
    for phase in plan["phases"]:                        # type: ignore[index]
        chunks = phase.get("stage_chunks")
        rows += ([chunk["egress_row"] for chunk in chunks] if chunks
                 else [phase["egress_row"]])
    return rows


def _sealed_egress(tmp_path: Path, window: dict[str, object]
                   ) -> tuple[dict[str, object], int | None]:
    """The params pbrun seals the window's egress with, and its grace.

    Sealed through the real ``pbrun.residency_stage_rows`` with a measured
    landing and one receipted egress on the stage, so an egress contract is
    priced if the head prices one.  Only the progress keys are kept: the
    sealed argv names the tier's tool root, and the test runs its own.
    """

    staged, cas, _digest = _seal_rows(
        tmp_path, window, measured_mb_s=1.0,
        receipts=[_egress_receipt(tmp_path / "stage")])
    (row,) = _egress_rows(staged["plan"])                # type: ignore[arg-type]
    sealed = cas.actions[str(row["action_key"])]["params"]
    params = {name: sealed[name] for name in (
        pb.PROGRESS_PARAM, "progress_pool_contention") if name in sealed}
    policy = params.get(pb.PROGRESS_PARAM)
    grace = (None if policy is None
             else max(int(phase["grace_s"]) for phase in policy["phases"]))
    return params, grace


def _staged_range(tmp_path: Path, queue: pool.PoolQueue,
                  cas: pb.PrismaBuildCAS) -> str:
    """Stage the sealed window for ``CONSUMER`` with a real mover; its key."""

    total = 3 * MIB
    argv = _mover_argv(tmp_path, consumer=CONSUMER,
                       manifest=tmp_path / "manifest.json", total=total,
                       receipt=tmp_path / "staged-receipt.json")
    item = _claimed(queue, cas, tmp_path, "stage", _mover_source(argv), {})
    outcome = queue.execute(item, timeout_s=60.0, heartbeat_s=HEARTBEAT,
                            timeout_grace_s=0.5)
    assert outcome["status"] == "executed", repr(outcome.get("stderr"))
    key = str(item["action_key"])
    queue.finish(key, status="executed", detail=outcome, claim_snapshot=item)
    return key


def _egress_source(tmp_path: Path, mover: str, *, after: int | None,
                   delay: float = 0.0) -> str:
    argv = ["--pool-root", str(tmp_path / "queue"),
            "--mover-action-key", mover, "--consumer-action-key", CONSUMER,
            "--stage-root", str(tmp_path / "stage"),
            "--receipt", str(tmp_path / "egress-receipt.json")]
    return EGRESS.format(tools=str(ROOT / "tools" / "fleet"),
                         heartbeat=HEARTBEAT, stage=str(tmp_path / "stage"),
                         marker=str(tmp_path / "unlinks.txt"), after=after,
                         delay=float(delay), argv=argv,
                         receipt=str(tmp_path / "egress-receipt.json"))


def _setup(tmp_path: Path, monkeypatch):
    import stage_release

    monkeypatch.setattr(pool, "HEARTBEAT_S", HEARTBEAT)
    monkeypatch.setattr(pool, "POOL_MEMBER_STAT", _Disks(UNDER), raising=False)
    window = _real_window(tmp_path)
    params, grace = _sealed_egress(tmp_path, window)
    (tmp_path / "stage").mkdir(exist_ok=True)
    queue = pool.PoolQueue(tmp_path / "queue")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=tmp_path / "stage") == "registered"
    mover = _staged_range(tmp_path, queue, cas)
    return queue, cas, mover, params, grace


def _unlinks(tmp_path: Path) -> list[str]:
    marker = tmp_path / "unlinks.txt"
    return marker.read_text().split() if marker.exists() else []


def _brief(outcome: dict) -> str:
    return repr({k: outcome.get(k) for k in (
        "status", "termination_reason", "elapsed_s", "progress_observation",
        "stall", "stderr")})


def test_a_wedged_egress_is_killed_and_the_mover_at_its_gate_proceeds(
        tmp_path: Path, monkeypatch) -> None:
    """The acceptance test of #1021, red on main.

    A real egress drains a staged range of three 1 MiB entries, unlinks the
    first and hangs unlinking the second, inside the stage's ownership lock.
    A mover of another range then reaches its start gate and waits on that
    lock.

    On main the egress is sealed with no progress contract, so nothing ends
    it before the worker's ceiling (here 30 s): the mover waits the whole
    time, and no record names the egress it waited on.  Now the egress's own
    ``no_progress`` rung ends it one grace after its last report, with the
    diagnosis every kill carries; the lock goes with it, the mover proceeds
    and completes, and its progress record names the egress, the wait and
    the credited seconds.
    """

    ceiling = 30.0
    queue, cas, mover, params, grace = _setup(tmp_path, monkeypatch)
    egress = _claimed(queue, cas, tmp_path, "egress",
                      _egress_source(tmp_path, mover, after=2), params)
    egress_key = str(egress["action_key"])
    ended: dict[str, dict] = {}

    def run_egress() -> None:
        ended["egress"] = queue.execute(egress, timeout_s=ceiling,
                                        heartbeat_s=HEARTBEAT,
                                        timeout_grace_s=0.5)

    runner = threading.Thread(target=run_egress, daemon=True)
    runner.start()
    deadline = time.monotonic() + 20.0
    while len(_unlinks(tmp_path)) < 2 and time.monotonic() < deadline:
        time.sleep(HEARTBEAT)
    # Wedged inside the lock: one staged entry unlinked, the next one hung.
    assert len(_unlinks(tmp_path)) == 2, _unlinks(tmp_path)

    manifest, total = _second_window(tmp_path)
    argv = _mover_argv(tmp_path, consumer=WAITING, manifest=manifest,
                       total=total, receipt=tmp_path / "gate-receipt.json")
    gated = _claimed(queue, cas, tmp_path, "gated", _mover_source(argv), {
        pb.PROGRESS_PARAM: _policy(4),
        "progress_pool_contention": _contention(tmp_path)})
    gated_outcome = queue.execute(gated, timeout_s=60.0, heartbeat_s=HEARTBEAT,
                                  timeout_grace_s=0.5)
    runner.join(ceiling + 30.0)
    egress_outcome = ended["egress"]

    # The egress ends at its own allowance, not at the worker's ceiling.
    assert egress_outcome.get("termination_reason") == "no_progress", _brief(
        egress_outcome)
    assert grace is not None
    assert egress_outcome["elapsed_s"] < ceiling
    observed = egress_outcome["progress_observation"]
    assert observed["grace_s"] == grace
    landed = observed["last_accepted"]
    # The first entry's bytes, released in the drain, of the named mover.
    assert landed["phase"] == "drain"
    assert landed["units_completed"] == MIB
    assert mover[:12] in landed["unit"]
    stall = egress_outcome["stall"]
    assert stall["allowance_s"] == grace
    # The egress's own hold of the stage's lock is not a wait.  Its worker
    # looks only until the drain's first report, and at most two edges of
    # one look each can fall between the grant and the holder record.
    assert stall["credited_s"]["start_gate"] <= 2 * HEARTBEAT + 1e-9
    # A kill names what it was waiting on (#990).
    assert "dependents_read_s" in egress_outcome

    # The mover waited out the egress at its gate, was credited for it and
    # completed; its record names the egress it waited on.
    assert gated_outcome["status"] == "executed", _brief(gated_outcome)
    receipt = json.loads((tmp_path / "gate-receipt.json").read_text())
    assert receipt["complete"] is True
    gate = gated_outcome["progress_observation"]
    contention = gate["pool_contention"]
    holder = contention.get("start_gate_holder")
    assert holder is not None and holder["action_key"] == egress_key, contention
    assert holder["role"] == "egress"
    assert holder["live"] is True
    held = {entry["action_key"]: entry for entry in contention["start_gate_holders"]}
    assert held[egress_key]["held_s"] > 0.0
    assert contention["start_gate_held_s"] > 0.0
    assert gate["start_gate_exempt_s"] > 0.0


def test_a_slow_but_advancing_egress_is_not_killed(tmp_path: Path,
                                                   monkeypatch) -> None:
    """Each unlink takes 0.6 of the egress's grace, so the drain runs well
    past one grace in all; every entry it releases is reported, and it
    completes.  Its record shows the drain it reported."""

    queue, cas, mover, params, grace = _setup(tmp_path, monkeypatch)
    assert grace is not None, "the egress was sealed with no progress contract"
    egress = _claimed(queue, cas, tmp_path, "egress",
                      _egress_source(tmp_path, mover, after=None,
                                     delay=0.6 * grace), params)
    outcome = queue.execute(egress, timeout_s=10.0 * grace,
                            heartbeat_s=HEARTBEAT, timeout_grace_s=0.5)

    assert outcome["status"] == "executed", _brief(outcome)
    assert outcome["elapsed_s"] > 1.5 * grace
    receipt = json.loads((tmp_path / "egress-receipt.json").read_text())
    assert receipt["complete"] is True
    assert receipt["entries_deleted"] == 3
    observed = outcome["progress_observation"]
    assert observed["last_accepted"]["units_completed"] == 3 * MIB
    assert observed["phase"] == "release"
    report = receipt["progress_report"]
    assert report["units_reported"] == 3 * MIB
    assert report["unwritten"] == 0 and report["refusal"] is None


# ------------------------------------------------------------ the price


def _slower_receipt(stage_root: Path) -> dict[str, object]:
    """A second receipted egress: a faster census, slower unlinks."""

    return {**_egress_receipt(stage_root), "action_key": "8" * 64,
            "entries_judged": 2, "census_s": 1.0, "census_validate_s": 0.2,
            "lock_held_s": 3.5, "unlink_s": 3.0, "prune_s": 0.1}


def test_an_egress_price_takes_each_term_at_its_slowest(tmp_path: Path) -> None:
    """The census, the unlinks per entry and the settle are each the slowest
    any receipted egress of this stage measured; the grace is the whole
    egress of the range at those terms plus the report latency."""

    from prismabuild import movement_actions as ma

    stage = tmp_path / "stage"
    stray = dict(_egress_receipt(stage))
    del stray["unlink_s"]
    records = [
        _egress_receipt(stage), _slower_receipt(stage),
        # Another stage's egress, one that judged nothing, one missing a
        # timing, and a copy's receipt: none of them prices this stage.
        {**_egress_receipt(tmp_path / "other"), "census_s": 100.0},
        {**_egress_receipt(stage), "entries_judged": 0, "census_s": 100.0},
        {**stray, "census_s": 100.0},
        {**_egress_receipt(stage), "schema": pool.POOL_MOVE_SCHEMA_V1,
         "census_s": 100.0}]
    price = ma.egress_price(records, stage_root=str(stage))
    assert price["basis"] == "egress" and price["receipts"] == 2
    assert price["census_s"] == 4.5
    assert price["unlink_s_per_entry"] == 1.5
    assert price["settle_s"] == pytest.approx(1.0 - 0.5 - 0.03)

    policy, derivation = ma.egress_progress_policy([MIB] * 3, price=price,
                                                   report_latency_s=0.1)
    priced = 4.5 + 3 * 1.5 + float(price["settle_s"])      # type: ignore[arg-type]
    grace = math.ceil(priced + 0.1)
    # Each term from a different receipt: neither one alone prices this.
    assert grace == 10
    assert derivation == {
        "basis": "egress", "entries": 3, "range_bytes": 3 * MIB,
        "egress_receipts": 2, "census_s": 4.5, "unlink_s_per_entry": 1.5,
        "settle_s": price["settle_s"], "priced_s": priced,
        "priced_bytes_per_s": 3 * MIB / priced, "report_latency_s": 0.1,
        "grace_s": grace}
    assert policy == {"schema": pb.PROGRESS_POLICY_SCHEMA_V1,
                      "phases": [{"name": name, "grace_s": grace}
                                 for name in ("snapshot", "drain", "release")]}

    nothing, unmeasured = ma.egress_progress_policy(
        [MIB], price=ma.egress_price([], stage_root=str(stage)))
    assert nothing is None
    assert unmeasured["basis"] == "unmeasured" and unmeasured["grace_s"] is None
    empty, _derivation = ma.egress_progress_policy([], price=price)
    assert empty is None


def test_a_measured_egress_is_sealed_with_its_grace_and_its_pool(
        tmp_path: Path) -> None:
    """Two egresses receipted on the stage price the next window's egress:
    its policy, the pool evidence the worker credits contention by, and the
    tags of a worker that never credits an egress's own hold.  The copy is
    priced as before: an egress receipt is not a landing."""

    from test_a_stalled_mover_ends_no_progress import _mover_rows

    stage = tmp_path / "stage"
    staged, cas, _digest = _seal_rows(
        tmp_path, _real_window(tmp_path), measured_mb_s=1.0,
        receipts=[_egress_receipt(stage), _slower_receipt(stage)])
    plan = staged["plan"]
    source = plan["demand_source"]                            # type: ignore[index]
    (row,) = _egress_rows(plan)                               # type: ignore[arg-type]
    (mover,) = _mover_rows(plan)                              # type: ignore[arg-type]
    key, mover_key = str(row["action_key"]), str(mover["action_key"])
    assert set(source["egress_progress"]) == {key}
    derivation = source["egress_progress"][key]
    priced = 4.5 + 3 * 1.5 + float(derivation["settle_s"])
    grace = math.ceil(priced + 2 * pool.HEARTBEAT_S)
    assert derivation["basis"] == "egress"
    assert derivation["grace_s"] == grace
    assert derivation["egress_receipts"] == 2
    assert derivation["entries"] == 3 and derivation["range_bytes"] == 3 * MIB
    assert derivation["mover_action_key"] == mover_key
    params = cas.actions[key]["params"]
    assert [(phase["name"], phase["grace_s"])
            for phase in params[pb.PROGRESS_PARAM]["phases"]] == [
        ("snapshot", grace), ("drain", grace), ("release", grace)]
    contention = params[pb.POOL_CONTENTION_PARAM]
    assert contention == derivation["pool_contention"]
    assert contention["members"] == sorted(MEMBERS)
    assert contention["stage_root"] == str(stage)
    assert contention["priced_bytes_per_s"] == pytest.approx(3 * MIB / priced)
    tags = params["placement"]["required_tags"]
    assert {pb.PROGRESS_TAG, pb.PROGRESS_HELPER_TAG, pb.POOL_CONTENTION_TAG,
            pb.EGRESS_PROGRESS_TAG} <= set(tags)
    assert pb.PROGRESS_CYCLE_TAG not in tags
    assert "dl380g10" in tags
    assert source["mover_progress"][mover_key]["basis"] == "landing"


@pytest.mark.parametrize("receipted,members,missing", [
    (False, None, "egress receipts"), (True, [], "pool members")])
def test_an_unmeasured_egress_is_sealed_as_before(
        tmp_path: Path, receipted, members, missing) -> None:
    """No egress receipted on the stage, or no pool members to judge
    contention by: no policy, no extra tag, and the plan says which."""

    staged, cas, _digest = _seal_rows(
        tmp_path, _real_window(tmp_path), measured_mb_s=1.0, members=members,
        receipts=[_egress_receipt(tmp_path / "stage")] if receipted else None)
    source = staged["plan"]["demand_source"]["egress_progress"]  # type: ignore[index]
    rows = _egress_rows(staged["plan"])                       # type: ignore[arg-type]
    assert rows and set(source) == {str(row["action_key"]) for row in rows}
    for row in rows:
        key = str(row["action_key"])
        assert source[key]["basis"] == "unmeasured"
        assert source[key]["unmeasured"] == missing
        assert source[key]["grace_s"] is None
        params = cas.actions[key]["params"]
        assert pb.PROGRESS_PARAM not in params
        assert pb.POOL_CONTENTION_PARAM not in params
        assert params["placement"]["required_tags"] == ["dl380g10"]


# ------------------------------------------------------------ the holder


def test_a_stage_holder_names_itself_and_ends_with_its_process_or_claim(
        tmp_path: Path) -> None:
    """The record beside the lock, and what a reader can check of it."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    path = queue.stage_ownership_holder_path(stage)
    assert path.parent == queue.root / pool.STAGE_OWNERSHIP_HOLDERS
    assert queue.stage_ownership_holder(stage) == {
        "recorded": False, "live": None, "why": "no holder record"}

    record = queue.write_stage_ownership_holder(stage, role="reconcile")
    held = queue.stage_ownership_holder(stage)
    assert held["recorded"] is True and held["live"] is True
    assert held["role"] == "reconcile" and held["action_key"] is None
    assert held["pid"] == os.getpid() and "why" not in held

    # A holder killed inside the lock leaves its record: its process is gone.
    ended = subprocess.run([sys.executable, "-c", "import os; print(os.getpid())"],
                           capture_output=True, text=True, check=True)
    path.write_text(json.dumps({**record, "pid": int(ended.stdout)}))
    gone = queue.stage_ownership_holder(stage)
    assert gone["live"] is False and gone["why"] == "holder process gone"

    # An action's record outlives its claim.
    queue.write_stage_ownership_holder(stage, role="egress", action_key="a" * 64)
    unclaimed = queue.stage_ownership_holder(stage)
    assert unclaimed["action_key"] == "a" * 64
    assert unclaimed["live"] is False and unclaimed["why"] == "holder claim ended"

    path.write_text(json.dumps({"schema": "something else", "pid": 1}))
    assert queue.stage_ownership_holder(stage) == {
        "recorded": False, "live": None, "why": "holder record malformed"}
    queue.clear_stage_ownership_holder(stage)
    assert not path.exists()
    queue.clear_stage_ownership_holder(stage)


#: An egress wedged inside its own hold, before its drain: the stage's lock
#: taken the way ``stage_release`` takes it, then nothing.
HOLDER = textwrap.dedent('''
    import sys, time
    sys.path.insert(0, {tools!r})
    import stage_release
    from prismabuild import pool
    queue = pool.PoolQueue({queue!r})
    with stage_release._stage_ownership(queue, {stage!r}, role="egress"):
        with open({marker!r}, "w") as stream:
            stream.write("held")
        time.sleep(3600)
''')


def test_an_egress_wedged_in_its_own_hold_is_not_credited_for_it(
        tmp_path: Path, monkeypatch) -> None:
    """Wedged under the stage's lock before its drain -- a census under the
    lock that never returns -- an egress is still in its first phase, where
    its worker looks at the start gate.  The hold it finds there is the
    egress's own, by its record, so it is not credited as a wait, and the
    egress ends one grace after launch instead of at the worker's ceiling."""

    monkeypatch.setattr(pool, "HEARTBEAT_S", HEARTBEAT)
    monkeypatch.setattr(pool, "POOL_MEMBER_STAT", _Disks(UNDER), raising=False)
    (tmp_path / "stage").mkdir()
    queue = pool.PoolQueue(tmp_path / "queue")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    grace = 2
    policy = {"schema": pb.PROGRESS_POLICY_SCHEMA_V1,
              "phases": [{"name": name, "grace_s": grace}
                         for name in ("snapshot", "drain", "release")]}
    source = HOLDER.format(tools=str(ROOT / "tools" / "fleet"),
                           queue=str(tmp_path / "queue"),
                           stage=str(tmp_path / "stage"),
                           marker=str(tmp_path / "held.txt"))
    item = _claimed(queue, cas, tmp_path, "holder", source, {
        pb.PROGRESS_PARAM: policy,
        "progress_pool_contention": _contention(tmp_path)})
    key = str(item["action_key"])
    ceiling = 30.0
    outcome = queue.execute(item, timeout_s=ceiling, heartbeat_s=HEARTBEAT,
                            timeout_grace_s=0.5)

    assert (tmp_path / "held.txt").read_text() == "held"
    assert outcome.get("termination_reason") == "no_progress", _brief(outcome)
    assert outcome["elapsed_s"] < ceiling / 2
    observed = outcome["progress_observation"]
    contention = observed["pool_contention"]
    assert contention["start_gate_self_probes"] > 0
    assert all(entry["action_key"] != key
               for entry in contention["start_gate_holders"])
    # At most the two edges a look can land between the grant and the record.
    assert observed["start_gate_exempt_s"] <= 2 * HEARTBEAT + 1e-9


# ------------------------------------------------------------ a mover's own hold


#: A mover wedged inside its own resume census (#1110): the real
#: ``stage_move._resume_own_coverage``, reached with a prior fragment on
#: disk, whose validation -- which runs under the stage's lock -- never
#: returns.  The mover's key is the one its launcher sets, as in production.
#:
#: Since #1008 item 1 the census parses that fragment *before* the lock, and
#: only re-parses under it when the file's version changed (or nothing
#: reusable came out of the pre-lock parse).  The fragment here is `{}` --
#: not a valid fragment -- so the pre-lock parse always fails to validate and
#: yields nothing to reuse, and the pass under the lock always re-parses.
#: Validation is left real on the first (pre-lock) call and only wedges from
#: the second call on, so the hang still lands where #1110 protects it: under
#: the stage's lock, in the mover's first phase.
MOVER_RESUME_HOLD = textwrap.dedent('''
    import os, sys, time
    from pathlib import Path
    sys.path.insert(0, {tools!r})
    import stage_move
    from prismabuild import core as pb
    from prismabuild import pool
    queue = pool.PoolQueue({queue!r})
    key = os.environ[pb.ACTION_KEY_ENV]
    residency_root = Path({residency!r})
    fragment = stage_move.residency_map.fragment_path(
        residency_root, {consumer!r}, key)
    fragment.parent.mkdir(parents=True, exist_ok=True)
    fragment.write_text("{{}}")
    real_validate = stage_move.residency_map.validate_fragment
    calls = []
    def wedged(document):
        calls.append(1)
        if len(calls) == 1:
            return real_validate(document)
        with open({marker!r}, "w") as stream:
            stream.write(str(os.getpid()))
        time.sleep(3600)
    stage_move.residency_map.validate_fragment = wedged
    stage_move._resume_own_coverage(
        queue, consumer_action_key={consumer!r}, mover_action_key=key,
        tier_id={tier!r}, stage_root=Path({stage!r}),
        manifest_sha256="0" * 64, residency_root=residency_root,
        window=[], mount_prefix="/")
''')

#: A mover wedged in the resume census's *pre-lock* parse (#1008 item 1): the
#: same real ``_resume_own_coverage``, but the very first validation --
#: which now runs before the stage lock is even requested -- never returns.
#: Nothing else is blocked by it, so no self-probe credit is needed; the
#: mover still ends on the ordinary no-progress ceiling alone.
MOVER_RESUME_PARSE_HOLD = textwrap.dedent('''
    import os, sys, time
    from pathlib import Path
    sys.path.insert(0, {tools!r})
    import stage_move
    from prismabuild import core as pb
    from prismabuild import pool
    queue = pool.PoolQueue({queue!r})
    key = os.environ[pb.ACTION_KEY_ENV]
    residency_root = Path({residency!r})
    fragment = stage_move.residency_map.fragment_path(
        residency_root, {consumer!r}, key)
    fragment.parent.mkdir(parents=True, exist_ok=True)
    fragment.write_text("{{}}")
    def wedged(document):
        with open({marker!r}, "w") as stream:
            stream.write(str(os.getpid()))
        time.sleep(3600)
    stage_move.residency_map.validate_fragment = wedged
    stage_move._resume_own_coverage(
        queue, consumer_action_key={consumer!r}, mover_action_key=key,
        tier_id={tier!r}, stage_root=Path({stage!r}),
        manifest_sha256="0" * 64, residency_root=residency_root,
        window=[], mount_prefix="/")
''')


def test_a_mover_wedged_in_its_own_resume_census_is_not_credited_for_it(
        tmp_path: Path, monkeypatch) -> None:
    """#1110, red on main.  The resume census reads the mover's own prior
    coverage under the stage's lock, in the mover's first phase, where its
    worker looks at the start gate.  On main the census filed no holder
    record, so the look read the mover's own hold as a wait on an
    unrecorded holder and credited it: a mover wedged there was kept alive
    to the worker's ceiling.  Now the census names the mover as its holder,
    the look counts it as a self probe, and the mover ends one grace after
    launch.

    Since #1008 item 1 the parse itself runs before the lock is requested
    (see :data:`MOVER_RESUME_HOLD` and
    :func:`test_a_mover_wedged_in_its_own_resume_parse_needs_no_credit`
    below, for that case) -- this test's fragment is deliberately invalid,
    so the pre-lock parse always fails to validate and the pass under the
    lock always re-parses, landing the wedge back under the lock, where
    #1110's guarantee still has to hold."""

    monkeypatch.setattr(pool, "HEARTBEAT_S", HEARTBEAT)
    monkeypatch.setattr(pool, "POOL_MEMBER_STAT", _Disks(UNDER), raising=False)
    (tmp_path / "stage").mkdir()
    queue = pool.PoolQueue(tmp_path / "queue")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    source = MOVER_RESUME_HOLD.format(
        tools=str(ROOT / "tools" / "fleet"), queue=str(tmp_path / "queue"),
        residency=str(tmp_path / "residency"), consumer=WAITING, tier=TIER,
        stage=str(tmp_path / "stage"), marker=str(tmp_path / "held.txt"))
    item = _claimed(queue, cas, tmp_path, "resume", source, {
        pb.PROGRESS_PARAM: _policy(2),
        "progress_pool_contention": _contention(tmp_path)})
    key = str(item["action_key"])
    ceiling = 20.0
    ended: dict[str, dict] = {}

    def run() -> None:
        ended["outcome"] = queue.execute(item, timeout_s=ceiling,
                                         heartbeat_s=HEARTBEAT,
                                         timeout_grace_s=0.5)

    runner = threading.Thread(target=run, daemon=True)
    runner.start()
    # On main the credited hold also outlived the worker's ceiling: the
    # action was never ended at all.  Bound the wait, and end the wedged
    # process ourselves if the worker does not, so a regression fails here
    # instead of hanging the suite.
    runner.join(ceiling + 10.0)
    if runner.is_alive():
        marker = tmp_path / "held.txt"
        if marker.exists():
            with contextlib.suppress(ProcessLookupError, ValueError):
                os.kill(int(marker.read_text()), signal.SIGKILL)
        runner.join(30.0)
        pytest.fail("the worker credited the mover's own resume-census hold "
                    "and never ended it")
    outcome = ended["outcome"]

    assert (tmp_path / "held.txt").read_text().isdigit()
    assert outcome.get("termination_reason") == "no_progress", _brief(outcome)
    assert outcome["elapsed_s"] < ceiling / 2
    observed = outcome["progress_observation"]
    contention = observed["pool_contention"]
    assert contention["start_gate_self_probes"] > 0
    assert all(entry["action_key"] != key
               for entry in contention["start_gate_holders"])
    # At most the two edges a look can land between the grant and the record.
    assert observed["start_gate_exempt_s"] <= 2 * HEARTBEAT + 1e-9
    # The kill ended the process, and its lock with it.
    with queue.stage_ownership_lock(str(tmp_path / "stage"), blocking=False) as got:
        assert got


def test_a_mover_wedged_in_its_own_resume_parse_needs_no_credit(
        tmp_path: Path, monkeypatch) -> None:
    """#1008 item 1.  A wedge in the pre-lock parse blocks nobody, so it
    needs no self-probe credit: the plain no-progress ceiling alone ends it.

    Before item 1 this same hang ran under the stage's lock (the case
    :func:`test_a_mover_wedged_in_its_own_resume_census_is_not_credited_for_it`
    covers) and would have parked every other mover's start gate and every
    reader's pin behind it for the run.  Now the parse that can be slow --
    opening and validating a same-key retry's own, potentially large,
    fragment -- runs before the lock is ever requested, so a hang there
    holds nothing but its own worker.
    """

    monkeypatch.setattr(pool, "HEARTBEAT_S", HEARTBEAT)
    monkeypatch.setattr(pool, "POOL_MEMBER_STAT", _Disks(UNDER), raising=False)
    (tmp_path / "stage").mkdir()
    queue = pool.PoolQueue(tmp_path / "queue")
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    source = MOVER_RESUME_PARSE_HOLD.format(
        tools=str(ROOT / "tools" / "fleet"), queue=str(tmp_path / "queue"),
        residency=str(tmp_path / "residency"), consumer=WAITING, tier=TIER,
        stage=str(tmp_path / "stage"), marker=str(tmp_path / "held.txt"))
    item = _claimed(queue, cas, tmp_path, "resume", source, {
        pb.PROGRESS_PARAM: _policy(2),
        "progress_pool_contention": _contention(tmp_path)})
    ceiling = 20.0
    ended: dict[str, dict] = {}

    def run() -> None:
        ended["outcome"] = queue.execute(item, timeout_s=ceiling,
                                         heartbeat_s=HEARTBEAT,
                                         timeout_grace_s=0.5)

    runner = threading.Thread(target=run, daemon=True)
    runner.start()
    runner.join(ceiling + 10.0)
    if runner.is_alive():
        marker = tmp_path / "held.txt"
        if marker.exists():
            with contextlib.suppress(ProcessLookupError, ValueError):
                os.kill(int(marker.read_text()), signal.SIGKILL)
        runner.join(30.0)
        pytest.fail("the worker never ended the mover wedged in its own "
                    "pre-lock parse")
    outcome = ended["outcome"]

    assert (tmp_path / "held.txt").read_text().isdigit()
    assert outcome.get("termination_reason") == "no_progress", _brief(outcome)
    assert outcome["elapsed_s"] < ceiling / 2
    # Nothing was blocked, so nothing needed a self-probe credit -- unlike
    # the under-the-lock case above, whose ``start_gate_self_probes`` is
    # positive for exactly this reason.
    contention = outcome["progress_observation"]["pool_contention"]
    assert contention["start_gate_self_probes"] == 0
    # And the kill released no lock, because the wedge never took one.
    with queue.stage_ownership_lock(str(tmp_path / "stage"), blocking=False) as got:
        assert got
