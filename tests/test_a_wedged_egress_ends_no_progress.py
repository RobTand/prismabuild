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

import hashlib
import json
from pathlib import Path
import sys
import textwrap
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    TIER, _hexkey)
from test_a_stalled_mover_ends_no_progress import (  # noqa: E402
    CONSUMER, HEARTBEAT, MIB, UNDER, _Disks, _contention, _mover_source,
    _policy, _real_window, _seal_rows)

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
    def unlink(path, *args, **kwargs):
        name = os.fspath(path)
        if (os.path.realpath(name).startswith(stage)
                and not os.path.basename(name).startswith(".")):
            calls[0] += 1
            with open({marker!r}, "a") as stream:
                stream.write(name + "\\n")
            if {after!r} is not None and calls[0] >= {after!r}:
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
    assert stall["credited_s"]["start_gate"] == 0.0
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
