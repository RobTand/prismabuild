"""The watch owns dependency order and charges only the action's quiet."""
import json

from prismabuild import core, pool


class Clock:
    def __init__(self, now=10.0):
        self.now = now

    def monotonic(self):
        return self.now

    def unix(self):
        return 1000.0 + self.now


def watch_at(tmp_path, clock):
    return pool.ProgressWatch(
        tmp_path / "action.progress", "launch",
        pool.ProgressPolicy((pool.ProgressPhase("run", 10.0, None),), None),
        started=0.0, clock=pool.ExecutionClock(clock.monotonic, clock.unix))


def unexpected(*args):
    raise AssertionError("this dependency read is not due")


def test_a_healthy_allowance_does_not_read_reports_or_dependencies(tmp_path):
    clock = Clock(9.0)
    watch = watch_at(tmp_path, clock)
    watch.path.write_bytes(b"{broken")
    sources = pool.WaitEvidenceSources(unexpected, unexpected, unexpected, unexpected)
    assert watch.check_stall(sources) is None
    assert watch.as_record(now=clock.now)["rejected_count"] == 0
    assert watch.stall_deadline() == 10.0


def test_a_boundary_report_refunds_its_read_without_dependency_credit(tmp_path, monkeypatch):
    clock = Clock()
    watch = watch_at(tmp_path, clock)
    watch.path.write_text(json.dumps({
        "schema": core.PROGRESS_RECORD_SCHEMA_V1, "token": "launch",
        "phase": "run", "units_completed": 1}))
    original = core._read_regular_file_nofollow

    def delayed_read(*args, **kwargs):
        raw = original(*args, **kwargs)
        clock.now += 3.0
        return raw

    monkeypatch.setattr(core, "_read_regular_file_nofollow", delayed_read)
    check = watch.check_stall(pool.WaitEvidenceSources(unexpected, unexpected, unexpected, unexpected))
    assert check.expired is False
    assert check.checkpoint_s == 3.0
    assert check.checked_monotonic == 10.0 + 3.0
    assert watch.stall_deadline() == 23.0
    assert watch.as_record(now=clock.now)["accepted_count"] == 1
    stall = watch.stall_record(now=clock.now, contention=None)
    assert stall["quiet_s"] == 0.0
    assert stall["last_landing"]["units_completed"] == 1
    assert all(value == 0.0 for value in stall["credited_s"].values())


def test_dependency_reads_keep_their_order_and_refund_each_elapsed_interval(tmp_path):
    clock = Clock()
    watch = watch_at(tmp_path, clock)
    observed = []

    def read(name, delay):
        observed.append(name)
        clock.now += delay
        return {"exempt": False, "reason": "not waiting"}

    sources = pool.WaitEvidenceSources(
        lambda prior, window: read("staged", 2.0),
        lambda prior, window: read("exports", 3.0),
        lambda: read("reader", 4.0),
        lambda prior: read("children", 5.0))
    check = watch.check_stall(sources)
    assert check.expired is True
    assert check.checkpoint_s == 14.0
    assert check.checked_monotonic == 10.0
    assert observed == ["staged", "exports", "reader", "children"]
    assert clock.now == watch.stall_deadline() == 24.0
    stall = watch.stall_record(now=clock.now, contention=None)
    assert stall["quiet_s"] == stall["allowance_s"] == 10.0
    assert all(value == 0.0 for value in stall["credited_s"].values())
    assert watch.as_record(now=clock.now)["accepted_count"] == 0


def test_dependency_credit_skips_later_rungs_and_does_not_credit_a_shared_interval_twice(tmp_path):
    clock = Clock(5.0)
    watch = watch_at(tmp_path, clock)
    watch.exempt_pool_contention({"verdict": "over"}, now=5.0, since_monotonic=0.0)
    evidence = tmp_path / "dependency.json"
    evidence.write_text(json.dumps({"exempt": True, "since_unix": 1000.0, "movers": ["mover"]}))
    clock.now = 15.0

    def staged(prior, window):
        verdict = json.loads(evidence.read_text())
        clock.now += 2.0
        return verdict

    check = watch.check_stall(pool.WaitEvidenceSources(staged, unexpected, unexpected, unexpected))
    assert check.expired is False
    assert check.checkpoint_s == 2.0
    assert watch.stall_deadline() == 27.0
    stall = watch.stall_record(now=clock.now, contention=None)
    assert stall["credited_s"]["pool_contention"] == 5.0
    assert stall["credited_s"]["staged_wait"] == 10.0
    assert stall["quiet_s"] == 0.0
    assert watch.as_record(now=clock.now)["accepted_count"] == 0


def test_unreadable_dependency_is_recorded_without_credit_and_its_read_is_refunded(tmp_path):
    clock = Clock()
    watch = watch_at(tmp_path, clock)

    def unreadable(prior, window):
        clock.now += 2.0
        raise OSError("unavailable source")

    sources = pool.WaitEvidenceSources(unreadable, lambda prior, window: None,
                                       lambda: None, lambda prior: None)
    check = watch.check_stall(sources)
    assert check.expired is True
    assert check.checkpoint_s == 2.0
    record = watch.as_record(now=clock.now)
    assert record["staged_wait"] == {
        "exempt": False, "movers": [], "reason": "unreadable: OSError('unavailable source')"}
    assert watch.stall_record(now=clock.now, contention=None)["quiet_s"] == 10.0
    assert record["staged_wait_exempt_s"] == 0.0
