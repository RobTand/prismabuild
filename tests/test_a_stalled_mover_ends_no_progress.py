"""A stage mover reports its landed bytes, and a stalled copy ends (#1010).

Since #989 a consumer blocked on its own staged range is exempt from its
``no_progress`` rung while that range's mover is ``claimed``.  The mover
itself declared no progress and no deadline, so a copy that stopped -- a hung
NFS read, a wedged ``cp`` -- held its claim, and through the exemption the
consumer's GPU, until the worker's execution ceiling.

Now pbrun seals each stage mover with a progress policy whose grace is the
time its next landing takes at the slowest measured landing of the manifest,
plus the time a report takes to reach the stall check; ``stage_move`` reports
its landed bytes on its heartbeat; and the worker's own ``no_progress`` rung
ends a copy that stops, with a record naming the bytes landed and the range.
The landing record prices a claimed copy from the same report.

Everything runs on ``tmp_path`` queues, stages and sources (#628).
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
import sys
import textwrap
import types

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from prismabuild import core as pb  # noqa: E402
from prismabuild import (  # noqa: E402
    pool, progress, residency_map, residency_plan, storage_tiers)
import tier_loop  # noqa: E402
from test_a_resident_range_is_adopted_rather_than_recopied import (  # noqa: E402
    STAGE_KIND, TIER, _hexkey, _row)
from test_a_stage_mover_declares_the_cpu_and_retries_it_owns import (  # noqa: E402
    READERS, _Cas, _manifest, _template)

MIB = 1 << 20
MB = storage_tiers.MB
CONSUMER = "c" * 64
FILL = f"{storage_tiers.FILL_KIND}{storage_tiers.TIER_DEMAND_SEPARATOR}{TIER}"
#: Short enough that a stall is decided in seconds.  The seal and the mover
#: both read it at call time, as production reads ``pool.HEARTBEAT_S``.
HEARTBEAT = 0.05


def _landed(digest: str, mb_s: float) -> dict[str, object]:
    """One complete earlier copy of ``digest`` that landed at ``mb_s``."""

    return {"schema": pool.POOL_MOVE_SCHEMA_V1, "action_key": "1" * 64,
            "consumer_action_key": "e" * 64, "tier_id": TIER,
            "manifest_sha256": digest, "complete": True,
            "bytes_staged": int(mb_s * 10 * MB), "seconds": 10.0,
            "unix": 100.0}


def _seal_rows(tmp_path: Path, manifest: dict[str, object], *,
               receipts: list[dict[str, object]] | None = None,
               measured_mb_s: float | None = None) -> tuple[dict, _Cas, str]:
    """Seal one window through ``pbrun.residency_stage_rows``.

    ``measured_mb_s`` files one complete earlier copy of this manifest at
    that rate, which is what ``mover_fill_price`` calls a ``landing``.
    """

    import pbrun

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    queue = pool.PoolQueue(tmp_path / "seal-queue")
    queue.ensure_layout()

    def discover(**_kwargs):
        return {TIER: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                       "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                       "mountpoint": str(tmp_path / "stage"),
                       "capacity_bytes": 64 * storage_tiers.GIB}}

    tier_loop.cycle(queue, host="dl380g10", source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)
    tier = dict(pbrun.resolve_stage_tier(queue, None))
    tier["tokens"] = {}
    args = types.SimpleNamespace(
        priority=-10, max_attempts=1, retry_safe=False,
        residency="stage", residency_tier=None, residency_mover_mem_gb=1,
        residency_mover_readers=READERS, residency_mover_max_attempts=3)
    records = list(receipts or [])
    if measured_mb_s is not None:
        records.append(_landed(digest, measured_mb_s))
    cas = _Cas(manifest_path)
    staged = pbrun.residency_stage_rows(
        _template(digest, manifest_path.stat().st_size),
        consumer_action_key=CONSUMER, tier=tier, args=args, queue=queue,
        cas=cas, movement_receipts=records)
    return staged, cas, digest


def _mover_rows(plan: dict[str, object]) -> list[dict[str, object]]:
    rows = []
    for phase in plan["phases"]:                        # type: ignore[index]
        chunks = phase.get("stage_chunks")
        rows += ([chunk["mover_row"] for chunk in chunks] if chunks
                 else [phase["mover_row"]])
    return rows


# ---------------------------------------------------------------- the grace


def test_the_grace_is_the_next_landing_at_the_slowest_rate_plus_two_heartbeats(
        monkeypatch) -> None:
    """Sixteen copy workers, each holding one entry, share the copy's rate.

    None of them lands until all of their bytes are read, so the unit is the
    sixteen largest entries of the range, not the largest one.
    """

    from prismabuild import movement_actions as ma

    monkeypatch.setattr(pool, "HEARTBEAT_S", 30.0)
    sizes = [5 * 10 ** 9] * 20 + [10 ** 6]
    policy, derivation = ma.mover_progress_policy(
        sizes, landing_bytes_per_s=134e6)
    unit = 16 * 5 * 10 ** 9
    grace = math.ceil(unit / 134e6 + 60.0)
    assert derivation == {"basis": "landing", "landing_bytes_per_s": 134e6,
                          "copy_depth": 16, "unit_bytes": unit,
                          "report_latency_s": 60.0, "grace_s": grace}
    assert policy == {"schema": pb.PROGRESS_POLICY_SCHEMA_V1,
                      "phases": [{"name": "copy", "grace_s": grace},
                                 {"name": "warm", "grace_s": grace}]}
    # A range of three entries can never have more than three in flight.
    _policy, small = ma.mover_progress_policy(
        [MIB] * 3, landing_bytes_per_s=1e6)
    assert small["unit_bytes"] == 3 * MIB


@pytest.mark.parametrize("rate", [None, 0.0, float("nan")])
def test_with_no_measured_landing_there_is_no_grace(rate) -> None:
    from prismabuild import movement_actions as ma

    policy, derivation = ma.mover_progress_policy([MIB], landing_bytes_per_s=rate)
    assert policy is None
    assert derivation["basis"] == "unmeasured"
    assert derivation["grace_s"] is None


def test_a_measured_mover_is_sealed_with_its_grace_and_placed_on_the_contract(
        tmp_path: Path) -> None:
    """R12's manifest landed at 116 MB/s at its slowest: every mover of the
    next window carries a grace priced from it, and requires the tags of a
    worker that enforces it."""

    staged, cas, _digest = _seal_rows(tmp_path, _manifest(), measured_mb_s=116.0)
    source = staged["plan"]["demand_source"]["mover_progress"]  # type: ignore[index]
    rows = _mover_rows(staged["plan"])                           # type: ignore[arg-type]
    assert rows and set(source) == {str(row["action_key"]) for row in rows}
    for row in rows:
        key = str(row["action_key"])
        derivation = source[key]
        assert derivation["basis"] == "landing"
        assert derivation["landing_bytes_per_s"] == 116 * MB
        # Each phase of ``_manifest`` is one 2 GiB entry.
        assert derivation["unit_bytes"] == 2 * storage_tiers.GIB
        assert derivation["grace_s"] == math.ceil(
            2 * storage_tiers.GIB / (116 * MB) + 2 * pool.HEARTBEAT_S)
        policy = cas.actions[key]["params"]["progress"]
        assert [phase["grace_s"] for phase in policy["phases"]] == [
            derivation["grace_s"]] * 2
        tags = cas.actions[key]["params"]["placement"]["required_tags"]
        assert {pb.PROGRESS_TAG, pb.PROGRESS_HELPER_TAG} <= set(tags)
        assert "dl380g10" in tags


def test_an_unmeasured_mover_is_sealed_as_before(tmp_path: Path) -> None:
    """Nothing measured this manifest: no policy, no extra tag, and the plan
    says ``unmeasured``.  The mover's key is what it was before #1010."""

    staged, cas, _digest = _seal_rows(tmp_path, _manifest())
    source = staged["plan"]["demand_source"]["mover_progress"]  # type: ignore[index]
    for row in _mover_rows(staged["plan"]):                     # type: ignore[arg-type]
        key = str(row["action_key"])
        assert source[key]["basis"] == "unmeasured"
        assert "progress" not in cas.actions[key]["params"]
        assert cas.actions[key]["params"]["placement"]["required_tags"] == [
            "dl380g10"]


# ------------------------------------------------- the stall, end to end

#: The mover under test: the real ``stage_move.main``, whose second read
#: never returns -- a hung NFS read.  The first entry lands; the second
#: hangs in its first chunk.
MOVER = textwrap.dedent('''
    import os, sys, time
    sys.path.insert(0, {tools!r})
    import stage_move
    from prismabuild import pool
    pool.HEARTBEAT_S = {heartbeat!r}
    real = os.readv
    calls = [0]
    def readv(fd, buffers):
        calls[0] += 1
        if calls[0] > 1:
            time.sleep(3600)
        return real(fd, buffers)
    os.readv = readv
    sys.exit(stage_move.main({argv!r}))
''')


def _real_window(tmp_path: Path, entries: int = 3) -> dict[str, object]:
    """``entries`` one-MiB files with real bytes, one phase over all of them."""

    mount = tmp_path / "sources"
    mount.mkdir()
    listed = []
    for index in range(entries):
        payload = hashlib.sha256(str(index).encode()).digest() * (MIB // 32)
        source = mount / f"shard-{index}.bin"
        source.write_bytes(payload)
        listed.append({"path": str(source), "offset": 0, "bytes": MIB,
                       "sha256": hashlib.sha256(payload).hexdigest()})
    total = entries * MIB
    return {"schema": pb.DATA_MANIFEST_SCHEMA_V1, "produced_by": {},
            "annotations": {"phases": [{"name": "layer-0", "bytes": total,
                                        "cumulative_bytes": total}]},
            "mount_prefix": str(mount), "entries": listed,
            "entry_count": entries, "total_bytes": total}


def _claimed_mover(tmp_path: Path, source: str, policy) -> tuple[pool.PoolQueue, dict]:
    """A claimed action running ``source``, sealed with ``policy``, one attempt."""

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    (checkout / "task.py").write_text(source)
    params = {} if policy is None else {pb.PROGRESS_PARAM: policy}
    action = pb.seal_action({
        "schema": pb.ACTION_SCHEMA_V2,
        "task": {"definition_id": "tests/stalled-mover", "definition_version": "v1",
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
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.publish(action_key=action["action_key"], cas_root=cas.root,
                  checkout_root=checkout, max_attempts=1,
                  worker_script=ROOT / "tools" / "prismabuild_worker.py")
    return queue, queue.claim()


def _waiting_consumer(queue: pool.PoolQueue, mover: str, *, total: int,
                      digest: str, tmp_path: Path) -> Path:
    """A consumer whose frozen plan names ``mover``, waiting on it.

    Returns the progress path its staged-wait record sits beside.
    """

    consumer = _hexkey("waiting-consumer")
    row = {**_row(queue, mover, {STAGE_KIND: 1, "cpu": 1, "mem_gb": 1}),
           "residency": {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                         "manifest_sha256": digest, "manifest_bytes": total,
                         "range_start_bytes": 0, "range_end_bytes": total}}
    plan = residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root=str(tmp_path / "stage"),
        manifest_sha256=digest, manifest_bytes=total, phases=[{
            "name": "layer-0", "start_bytes": 0, "end_bytes": total,
            "stage_gib": 1, "mover_row": row,
            "egress_row": _row(queue, _hexkey("waiting-egress"), {"mem_gb": 1})}])
    residency_plan.freeze(queue, plan)
    progress_path = tmp_path / "consumer.progress"
    Path(progress.staged_wait_path(str(progress_path))).write_text(
        json.dumps({"schema": "prismabuild.staged_wait.v1", "token": "t" * 32,
                    "since_unix": 1.0, "movers": [mover]}))
    return progress_path


def test_a_mover_whose_copy_stops_after_its_first_entry_ends_no_progress(
        tmp_path: Path, monkeypatch) -> None:
    """The acceptance test of #1010, red on main.

    The window is three 1 MiB entries, and this manifest's slowest copy
    landed at 1 MB/s.  The grace pbrun seals is the time its next landing
    takes -- all three entries can be in flight at once, 3 MiB at 1 MB/s,
    3.15 s -- plus two heartbeats of report latency, rounded up: 4 s.

    The mover lands the first entry and hangs reading the second.  On main
    it seals no policy, so nothing ends it before the worker's ceiling (here
    20 s): it stays claimed past the grace, and the consumer waiting on it
    stays exempt.  Now its own ``no_progress`` rung ends it within the
    grace of its last report, and the ending record names the bytes landed
    and the range they belong to.
    """

    import stage_release

    grace, ceiling = 4, 20.0
    monkeypatch.setattr(pool, "HEARTBEAT_S", HEARTBEAT)
    window = _real_window(tmp_path)
    total = int(window["total_bytes"])                   # type: ignore[arg-type]
    staged, cas, digest = _seal_rows(tmp_path, window, measured_mb_s=1.0)
    (sealed_row,) = _mover_rows(staged["plan"])          # type: ignore[arg-type]
    policy = cas.actions[str(sealed_row["action_key"])]["params"].get("progress")

    stage = tmp_path / "stage"
    stage.mkdir(exist_ok=True)
    manifest_path = tmp_path / "manifest.json"
    argv = ["--pool-root", str(tmp_path / "queue"),
            "--consumer-action-key", CONSUMER, "--tier-id", TIER,
            "--stage-root", str(stage), "--manifest-sha256", digest,
            "--manifest", str(manifest_path),
            "--range-start-bytes", "0", "--range-end-bytes", str(total),
            "--readers", "1", "--max-readers", "1", "--block", str(MIB),
            "--warm-after-copy", "never", "--unpaced"]
    source = MOVER.format(tools=str(ROOT / "tools" / "fleet"),
                          heartbeat=HEARTBEAT, argv=argv)
    queue, item = _claimed_mover(tmp_path, source, policy)
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    mover = str(item["action_key"])
    progress_path = _waiting_consumer(queue, mover, total=total, digest=digest,
                                      tmp_path=tmp_path)
    consumer = _hexkey("waiting-consumer")
    before = queue.staged_wait_verdict(consumer, progress_path, token="t" * 32)
    assert before["exempt"] is True
    assert before["movers"] == [{"key": mover, "state": "claimed"}]

    outcome = queue.execute(item, timeout_s=ceiling, heartbeat_s=HEARTBEAT,
                            timeout_grace_s=0.5)

    assert outcome.get("termination_reason") == "no_progress", repr(
        {k: outcome.get(k) for k in ("status", "termination_reason", "elapsed_s")})
    assert outcome["elapsed_s"] < ceiling
    observed = outcome["progress_observation"]
    assert observed["grace_s"] == grace
    landed = observed["last_accepted"]
    # The first entry's bytes, in the copy phase, over the named range.
    assert landed["units_completed"] == MIB
    assert landed["phase"] == "copy"
    assert f"[0, {total})" in landed["unit"]
    # A kill names what it was waiting on (#990): the mover's own dependents
    # are read into the same record.
    assert "dependents_read_s" in outcome

    queue.finish(mover, status=str(outcome["status"]), detail=outcome,
                 claim_snapshot=item)
    after = queue.staged_wait_verdict(consumer, progress_path, token="t" * 32)
    # No longer a claimed copy: the attempt is spent, the mover is in
    # ``failed/``, and with no live tier loop to recopy it (#627) the wait
    # is not exempt.
    assert after["movers"][0]["state"] == "failed"
    assert after["exempt"] is False


# ------------------------------------------------------ the reporter itself


def test_the_mover_reports_its_landed_bytes_and_says_so_on_its_receipt(
        tmp_path: Path, monkeypatch) -> None:
    """A whole copy under a progress channel: the last report is every byte
    it landed, and the receipt counts the reports."""

    import stage_move
    import stage_release

    window = _real_window(tmp_path, entries=4)
    total = int(window["total_bytes"])                   # type: ignore[arg-type]
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(window))
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    assert stage_release.register_stage_root(
        queue, tier_id=TIER, stage_root=stage) == "registered"
    report = tmp_path / "mover.progress"
    monkeypatch.setenv(progress.ACTION_PROGRESS_PATH_ENV, str(report))
    monkeypatch.setenv(progress.ACTION_PROGRESS_TOKEN_ENV, "t" * 32)
    monkeypatch.setenv(progress.ACTION_PROGRESS_PHASES_ENV,
                       json.dumps(["copy", "warm"]))
    args = stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root), "--action-key", _hexkey("reporting"),
        "--consumer-action-key", CONSUMER, "--tier-id", TIER,
        "--stage-root", str(stage), "--manifest-sha256", digest,
        "--manifest", str(manifest_path),
        "--residency-root", str(queue.residency_fragment_root()),
        "--range-start-bytes", "0", "--range-end-bytes", str(total),
        "--readers", "2", "--max-readers", "2", "--warm-after-copy", "never",
        "--unpaced", "--progress-interval-s", "0.01"])

    receipt = stage_move.move(args)

    assert receipt["complete"], receipt
    record = json.loads(report.read_text())
    assert record["token"] == "t" * 32
    assert record["phase"] == "copy"
    assert record["units_completed"] == total == receipt["bytes_staged"]
    made = receipt["progress_report"]
    assert made["channel"] is True and made["reports"] >= 1
    assert made["units_reported"] == total and made["refusal"] is None


def test_a_mover_with_no_channel_reports_nothing(tmp_path: Path, monkeypatch) -> None:
    """A mover sealed with no policy, or a direct run: nothing starts."""

    import stage_move

    for name in progress.ACTION_PROGRESS_ENV:
        monkeypatch.delenv(name, raising=False)
    copier = types.SimpleNamespace(lock=stage_move.threading.Lock(), bytes_staged=5)
    reporter = stage_move._ProgressReporter(
        copier, interval_s=0.01, range_start_bytes=0, range_end_bytes=10)
    reporter.start()
    reporter.stop()
    assert reporter.record()["channel"] is False
    assert reporter.record()["reports"] == 0


def test_the_read_back_is_reported_in_the_warm_phase(tmp_path: Path,
                                                     monkeypatch) -> None:
    """Entering ``warm`` is itself a report, and the count goes on growing."""

    import stage_move

    report = tmp_path / "mover.progress"
    monkeypatch.setenv(progress.ACTION_PROGRESS_PATH_ENV, str(report))
    monkeypatch.setenv(progress.ACTION_PROGRESS_TOKEN_ENV, "t" * 32)
    monkeypatch.setenv(progress.ACTION_PROGRESS_PHASES_ENV,
                       json.dumps(["copy", "warm"]))
    copier = types.SimpleNamespace(lock=stage_move.threading.Lock(), bytes_staged=0)
    reporter = stage_move._ProgressReporter(
        copier, interval_s=60.0, range_start_bytes=0, range_end_bytes=10)
    reporter.report()
    assert not report.exists()               # nothing landed, nothing said
    copier.bytes_staged = 10
    state = {"bytes": 0}
    reporter.enter_warm(state)
    assert json.loads(report.read_text())["phase"] == "warm"
    assert json.loads(report.read_text())["units_completed"] == 10
    state["bytes"] = 7
    reporter.report()
    assert json.loads(report.read_text())["units_completed"] == 17


def test_a_phase_the_launch_did_not_declare_is_recorded_not_raised(
        tmp_path: Path, monkeypatch) -> None:
    import stage_move

    monkeypatch.setenv(progress.ACTION_PROGRESS_PATH_ENV, str(tmp_path / "p"))
    monkeypatch.setenv(progress.ACTION_PROGRESS_TOKEN_ENV, "t" * 32)
    monkeypatch.setenv(progress.ACTION_PROGRESS_PHASES_ENV, json.dumps(["run"]))
    copier = types.SimpleNamespace(lock=stage_move.threading.Lock(), bytes_staged=3)
    reporter = stage_move._ProgressReporter(
        copier, interval_s=60.0, range_start_bytes=0, range_end_bytes=10)
    reporter.report()
    assert "copy" in str(reporter.record()["refusal"])
    assert not (tmp_path / "p").exists()


# ------------------------------------------------------ the live rate


def test_a_reported_copy_is_priced_from_its_landed_bytes() -> None:
    """Claimed at t=1000, 30 of 100 bytes landed by t=1010: 3 B/s live, so
    the rest lands at 1010 + 70/3.  The plan's rate prices only the queue."""

    order = [
        {"mover_action_key": "a", "state": "claimed", "range_bytes": 100,
         "claimed_unix": 1000.0, "landed_bytes": 30, "reported_unix": 1010.0,
         "landed_phase": "copy"},
        {"mover_action_key": "b", "state": "claimed", "range_bytes": 100,
         "claimed_unix": 1000.0},
        {"mover_action_key": "c", "state": "ready", "range_bytes": 50},
    ]
    out = residency_plan.expected_landings(order, now=1010.0,
                                           landing_bytes_per_s=10.0)
    a, b, c = out["a"], out["b"], out["c"]
    assert a["basis"] == "reported"
    assert a["landed_bytes"] == 30 and a["live_bytes_per_s"] == 3.0
    assert a["expected_landing_unix"] == pytest.approx(1010.0 + 70 / 3.0)
    assert b["basis"] == "claim"
    assert b["expected_landing_unix"] == 1010.0
    assert c["basis"] == "queue"
    # Behind a: its remaining seconds at the plan's rate; b has landed.
    assert c["bytes_ahead"] == round((70 / 3.0) * 10.0)


@pytest.mark.parametrize("report,basis", [
    ({"landed_bytes": 0, "reported_unix": 1010.0, "landed_phase": "copy"}, "claim"),
    ({"landed_bytes": 5, "reported_unix": 999.0, "landed_phase": "copy"}, "claim"),
    ({"landed_bytes": 100, "reported_unix": 1005.0, "landed_phase": "copy"}, "reported"),
    ({"landed_bytes": 140, "reported_unix": 1005.0, "landed_phase": "warm"}, "reported"),
])
def test_a_report_that_prices_nothing_keeps_the_claim_time(report, basis) -> None:
    out = residency_plan.expected_landings(
        [{"mover_action_key": "a", "state": "claimed", "range_bytes": 100,
          "claimed_unix": 1000.0, **report}],
        now=1010.0, landing_bytes_per_s=10.0)["a"]
    assert out["basis"] == basis
    if basis == "reported":
        # Landed: its range is covered, or it is reading its range back.
        assert out["expected_landing_unix"] == 1005.0
        assert out["landed_bytes"] == 100


def test_the_landing_record_carries_the_claimed_movers_live_rate(
        tmp_path: Path) -> None:
    """One cycle's landing record: the claimed mover that reported is priced
    from its report, and the record says so."""

    queue = pool.PoolQueue(tmp_path / "queue")
    queue.ensure_layout()
    size = 22 * 10 ** 9
    manifest = "d" * 64
    consumer = _hexkey("landing-consumer")
    movers = [_hexkey("landing-mover-0"), _hexkey("landing-mover-1")]
    phases = []
    for index, mover in enumerate(movers):
        start, end = index * size, (index + 1) * size
        phases.append({
            "name": f"chain-{index}", "start_bytes": start, "end_bytes": end,
            "stage_gib": 21,
            "mover_row": {
                **_row(queue, mover, {STAGE_KIND: 21, FILL: 134, "cpu": 2,
                                      "mem_gb": 1}),
                "residency": {"schema": pool.RESIDENCY_SCHEMA_V1, "tier_id": TIER,
                              "manifest_sha256": manifest,
                              "manifest_bytes": 2 * size,
                              "range_start_bytes": start, "range_end_bytes": end}},
            "egress_row": _row(queue, _hexkey(f"landing-egress-{index}"),
                               {"mem_gb": 1})})
    plan = residency_plan.build_plan(
        consumer_action_key=consumer, tier_id=TIER, stage_root=str(tmp_path / "stage"),
        manifest_sha256=manifest, manifest_bytes=2 * size, phases=phases)
    residency_plan.freeze(queue, plan)
    for phase in plan["phases"]:                         # type: ignore[union-attr]
        queue.publish(**dict(phase["mover_row"]))
    claimed = movers[0]
    source = queue.item_path(pool.READY, claimed)
    record = json.loads(source.read_text())
    source.unlink()
    record.update({"claimed_unix": 1000.0, "claimed_by": "copy-fixture",
                   "claimed_host": "dl380g10"})
    queue.item_path(pool.CLAIMED, claimed).write_text(json.dumps(record))
    queue.action_progress_path(claimed).write_text(json.dumps({
        "schema": pb.PROGRESS_RECORD_SCHEMA_V1, "token": "t" * 32,
        "phase": "copy", "units_completed": 11 * 10 ** 9,
        "reported_unix": 1100.0, "unit": "bytes landed"}))
    tiers = {TIER: {"schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                    "tier_id": TIER, "host": "dl380g10", "tier": "stage",
                    "mountpoint": str(tmp_path / "stage"),
                    "capacity_bytes": 565 * storage_tiers.GIB}}

    events = tier_loop.publish_landing_expectations(
        queue, tiers=tiers, consumers=[{"action_key": consumer,
                                        "state": pool.CLAIMED}], now=1100.0)

    assert events == []
    doc = residency_map.read_landing(residency_map.landing_path(
        queue.residency_fragment_root(), consumer))
    rows = {row["mover_action_key"]: row for row in doc["ranges"]}
    live = rows[claimed]
    assert live["state"] == "claimed" and live["basis"] == "reported"
    assert live["landed_bytes"] == 11 * 10 ** 9
    assert live["live_bytes_per_s"] == pytest.approx(11 * 10 ** 9 / 100.0)
    assert live["expected_landing_unix"] == pytest.approx(1100.0 + 100.0)
    queued = rows[movers[1]]
    assert queued["state"] == "ready" and queued["basis"] == "queue"
    # Without its report the same copy is priced from its claim time.
    queue.action_progress_path(claimed).unlink()
    tier_loop.publish_landing_expectations(
        queue, tiers=tiers, consumers=[{"action_key": consumer,
                                        "state": pool.CLAIMED}], now=1100.0)
    doc = residency_map.read_landing(residency_map.landing_path(
        queue.residency_fragment_root(), consumer))
    rows = {row["mover_action_key"]: row for row in doc["ranges"]}
    assert rows[claimed]["basis"] == "claim"
    assert rows[claimed]["expected_landing_unix"] == pytest.approx(
        1000.0 + size / (134 * MB))
