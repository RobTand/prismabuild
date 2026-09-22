"""A campaign must stop publishing when its declared window is full."""
import pytest

from test_pbcampaign import fleet_paths, _manifest, _row
from prismabuild import pool
import pbcampaign


def test_window_bounds_ready_work_and_reports_unsubmitted_suffix(
    tmp_path, fleet_paths, capsys,
):
    work, queue = fleet_paths
    manifest = _manifest(tmp_path, [_row(work, f"printf row-{i}") for i in range(4)])
    assert pbcampaign.main([
        "--transport", "pool", "--max-inflight", "2", "--wait-s", "0", manifest,
    ]) == pbcampaign.pbwait.GAVE_UP_EXIT
    assert len(list(queue.dir(pool.READY).glob("*.json"))) == 2
    output = capsys.readouterr()
    assert "not_submitted" in output.out
    assert "row 2" in output.err and "row 3" in output.err


def test_window_resumes_attached_prefix_and_replays_receipts(
    tmp_path, fleet_paths, capsys,
):
    from test_pbcampaign import _campaign_against_a_worker

    work, queue = fleet_paths
    manifest = _manifest(tmp_path, [_row(work, f"printf resume-{i}") for i in range(3)])
    flags = ["--transport", "pool", "--max-inflight", "2"]
    assert pbcampaign.main([*flags, "--wait-s", "0", manifest]) == 75
    before = {p.stem for p in queue.dir(pool.READY).glob("*.json")}
    assert len(before) == 2
    capsys.readouterr()
    assert pbcampaign.main([*flags, "--wait-s", "0", manifest]) == 75
    assert "attached" in capsys.readouterr().err
    assert {p.stem for p in queue.dir(pool.READY).glob("*.json")} == before
    assert _campaign_against_a_worker(queue, manifest, rows=3, argv=flags) == 0
    capsys.readouterr()
    done = {p.stem for p in queue.dir(pool.DONE).glob("*.json")}
    assert len(done) == 3 and before <= done
    assert not list(queue.dir(pool.READY).glob("*.json"))
    assert pbcampaign.main([*flags, manifest]) == 0
    assert capsys.readouterr().out.count("cache_hit") == 3
    assert {p.stem for p in queue.dir(pool.DONE).glob("*.json")} == done


def _window_fakes(monkeypatch, statuses, *, submitted_status="submitted"):
    events = []
    keys = [f"{i + 1:064x}" for i in range(5)]

    def submit(row, **kwargs):
        i = row["index"]
        events.append(("submit", i))
        return {"action_key": keys[i], "published_unix": 100.0 + i,
                "status": submitted_status, "transport": "pool"}

    polls = iter(statuses)

    def wait(queue, waiting, *, generations, **kwargs):
        events.append(("poll", [keys.index(k) for k in waiting]))
        assert generations == {k: 100.0 + keys.index(k) for k in waiting}
        state = next(polls)
        return [pbcampaign.pbwait._row(k, state[keys.index(k)], transport="pool")
                for k in waiting]

    monkeypatch.setattr(pbcampaign, "submit_row", submit)
    monkeypatch.setattr(pbcampaign.pbwait, "wait_for_keys", wait)
    monkeypatch.setattr(pbcampaign.time, "sleep", lambda seconds: None)
    return keys, events


def test_any_completed_row_refills_without_waiting_for_the_first(monkeypatch):
    keys, events = _window_fakes(monkeypatch, [
        {0: "waiting", 1: "executed"},
        {0: "waiting", 2: "executed"},
        {0: "executed", 3: "executed"},
    ])
    submissions, waited = pbcampaign.run_windowed(
        [{"index": i} for i in range(4)], transport="pool", max_inflight=2, wait_s=30,
    )
    assert events == [
        ("submit", 0), ("submit", 1), ("poll", [0, 1]),
        ("submit", 2), ("poll", [0, 2]), ("submit", 3), ("poll", [0, 3]),
    ]
    assert pbcampaign.pbwait.verdict(pbcampaign.rows_for(submissions, waited)) == 0


def test_receipt_does_not_release_a_slot_while_claim_cleanup_remains(
    monkeypatch, tmp_path,
):
    keys, events = _window_fakes(monkeypatch, [
        {0: "cache_hit"}, {0: "cache_hit"}, {1: "cache_hit"},
    ], submitted_status="cache_hit")
    queue = pool.PoolQueue(pbcampaign.pbrun.SH / "pb-queue")
    path = queue.item_path(pool.CLAIMED, keys[0])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{}')
    monkeypatch.setattr(pbcampaign.time, "sleep", lambda seconds: path.unlink())
    submissions, waited = pbcampaign.run_windowed(
        [{"index": i} for i in range(2)], transport="pool", max_inflight=1, wait_s=30,
    )
    assert events == [("submit", 0), ("poll", [0]), ("poll", [0]),
                      ("submit", 1), ("poll", [1])]
    assert pbcampaign.pbwait.verdict(pbcampaign.rows_for(submissions, waited)) == 0


@pytest.mark.parametrize("suffix", [pool.TOMBSTONE_SUFFIX, pool.LATE_FINISH_SUFFIX])
def test_an_ending_does_not_release_a_slot_while_its_finish_is_in_flight(
    monkeypatch, tmp_path, suffix,
):
    # ``PoolQueue.finish`` files the attempt archive, moves the claim aside to
    # a finish tombstone and only then releases capacity and files ``done/``.
    # A generation-pinned wait answers ``executed`` from the archive inside
    # that window (#886), so the slot check must see the entombed claim.
    keys, events = _window_fakes(monkeypatch, [
        {0: "executed"}, {0: "executed"}, {1: "executed"},
    ])
    queue = pool.PoolQueue(pbcampaign.pbrun.SH / "pb-queue")
    path = queue.dir(pool.CLAIMED) / f"{keys[0]}.1790000000000000.sparky.7.abcdef01{suffix}"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{}')
    monkeypatch.setattr(pbcampaign.time, "sleep", lambda seconds: path.unlink())
    submissions, waited = pbcampaign.run_windowed(
        [{"index": i} for i in range(2)], transport="pool", max_inflight=1, wait_s=30,
    )
    assert events == [("submit", 0), ("poll", [0]), ("poll", [0]),
                      ("submit", 1), ("poll", [1])]
    assert pbcampaign.pbwait.verdict(pbcampaign.rows_for(submissions, waited)) == 0


def test_only_this_keys_finish_records_occupy_its_slot(tmp_path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    key, other = "a" * 64, "b" * 64
    claimed = queue.dir(pool.CLAIMED)
    assert pbcampaign._pool_slot_occupied(queue, key) is False
    (claimed / f"{other}.1.h.1.x{pool.TOMBSTONE_SUFFIX}").write_text("{}")
    (claimed / f"{key}.lease").write_text("{}")
    assert pbcampaign._pool_slot_occupied(queue, key) is False
    for suffix in (pool.TOMBSTONE_SUFFIX, pool.LATE_FINISH_SUFFIX):
        record = claimed / f"{key}.1.h.1.y{suffix}"
        record.write_text("not json")
        # Occupancy is the name, exactly as the pool's claim gate reads it.
        assert pbcampaign._pool_slot_occupied(queue, key) is True
        record.unlink()
    assert pbcampaign._pool_slot_occupied(queue, key) is False


def test_unreadable_outcome_stops_publication_and_keeps_remaining_rows(monkeypatch):
    keys, events = _window_fakes(monkeypatch, [{0: "unreadable", 1: "waiting"}])
    submissions, waited = pbcampaign.run_windowed(
        [{"index": i} for i in range(4)], transport="pool", max_inflight=2, wait_s=30,
    )
    assert events == [("submit", 0), ("submit", 1), ("poll", [0, 1])]
    assert [s['status'] for s in submissions] == [
        "submitted", "submitted", "not_submitted", "not_submitted"]
    assert pbcampaign.pbwait.verdict(pbcampaign.rows_for(submissions, waited)) == 1


def test_failed_prefix_cannot_publish_a_later_window_before_resume(monkeypatch):
    keys, events = _window_fakes(monkeypatch, [{0: "failed", 1: "waiting"}])
    submissions, waited = pbcampaign.run_windowed(
        [{"index": i} for i in range(4)], transport="pool", max_inflight=2, wait_s=30,
    )
    assert events == [("submit", 0), ("submit", 1), ("poll", [0, 1])]
    assert [s['status'] for s in submissions] == [
        "submitted", "submitted", "not_submitted", "not_submitted"]
    assert pbcampaign.pbwait.verdict(pbcampaign.rows_for(submissions, waited)) == 1


def test_slot_read_error_stops_publication(monkeypatch):
    keys, events = _window_fakes(monkeypatch, [{0: "executed"}])

    def unavailable(*args):
        raise PermissionError("claim directory is unavailable")

    monkeypatch.setattr(pbcampaign, "_pool_slot_occupied", unavailable)
    submissions, waited = pbcampaign.run_windowed(
        [{"index": i} for i in range(2)], transport="pool", max_inflight=1, wait_s=30,
    )
    assert events == [("submit", 0), ("poll", [0])]
    assert waited[0]['status'] == 'record_error'
    assert submissions[1]['status'] == 'not_submitted'


def test_submission_error_stops_before_another_row_can_publish(monkeypatch):
    seen = []

    def unavailable(row, **kwargs):
        seen.append(row['index'])
        raise OSError("publication result unknown")

    monkeypatch.setattr(pbcampaign, "submit_row", unavailable)
    submissions, waited = pbcampaign.run_windowed(
        [{"index": i} for i in range(3)], transport="pool", max_inflight=2, wait_s=30,
    )
    assert seen == [0]
    assert [s['status'] for s in submissions] == ['refused', 'not_submitted', 'not_submitted']


@pytest.mark.parametrize("flags", [
    ["--max-inflight", "0"], ["--max-inflight", "-1"], ["--max-inflight", "1.5"],
    ["--max-inflight", "1", "--detach"],
    ["--max-inflight", "1", "--transport", "slurm"],
    ["--max-inflight", "1", "--wait-s", "nan"],
    ["--max-inflight", "1", "--wait-s", "inf"],
    ["--max-inflight", "1", "--wait-s", "-1"],
])
def test_invalid_window_refuses_before_loading_or_submitting(flags, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('invalid controller must not read or submit the manifest')

    monkeypatch.setattr(pbcampaign, "load_manifest", forbidden)
    with pytest.raises(SystemExit) as error:
        pbcampaign.main([*flags, 'not-read.json'])
    assert error.value.code == 2


def test_duplicate_rows_share_one_slot_and_retain_all_table_rows(monkeypatch):
    keys, events = _window_fakes(monkeypatch, [{0: "executed", 1: "executed"}])
    submissions, waited = pbcampaign.run_windowed(
        [{"index": 0}, {"index": 0}, {"index": 1}],
        transport="pool", max_inflight=2, wait_s=30,
    )
    assert events == [("submit", 0), ("submit", 0), ("submit", 1), ("poll", [0, 1])]
    table = pbcampaign.rows_for(submissions, waited)
    assert len(table) == 3 and table[0] == table[1]
    assert pbcampaign.pbwait.verdict(table) == 0


def test_refill_does_not_renew_the_campaign_deadline(monkeypatch):
    keys, events = _window_fakes(monkeypatch, [{0: "executed"}])
    clock = [100.0]
    monkeypatch.setattr(pbcampaign.time, "monotonic", lambda: clock[0])
    poll = pbcampaign.pbwait.wait_for_keys

    def expired(*args, **kwargs):
        result = poll(*args, **kwargs)
        clock[0] += 6.0
        return result

    monkeypatch.setattr(pbcampaign.pbwait, "wait_for_keys", expired)
    submissions, waited = pbcampaign.run_windowed(
        [{"index": 0}, {"index": 1}], transport="pool", max_inflight=1, wait_s=5,
    )
    assert events == [("submit", 0), ("poll", [0])]
    assert submissions[1]['status'] == 'not_submitted'


def test_cache_hit_can_continue_past_an_old_failed_attempt(monkeypatch):
    # pbrun has verified the reusable receipt. pbwait can also see an older
    # failed attempt for that key (e.g. a broker EOF after receipt publication).
    keys, events = _window_fakes(monkeypatch, [
        {0: "failed"}, {1: "cache_hit"},
    ], submitted_status="cache_hit")
    submissions, waited = pbcampaign.run_windowed(
        [{"index": 0}, {"index": 1}], transport="pool", max_inflight=1, wait_s=30,
    )
    assert events == [("submit", 0), ("poll", [0]), ("submit", 1), ("poll", [1])]
    assert pbcampaign.pbwait.verdict(pbcampaign.rows_for(submissions, waited)) == 0


def test_logical_request_refuses_window_before_decomposition(monkeypatch):
    monkeypatch.setattr(pbcampaign, "load_manifest", lambda *a, **k: {"schema": "logical"})
    def unexpected(*a, **k):
        raise AssertionError("unsupported window must refuse before decomposition")
    monkeypatch.setattr(pbcampaign, "decompose", unexpected)
    with pytest.raises(SystemExit) as error:
        pbcampaign.main(["--transport", "pool", "--max-inflight", "2", "unused.json"])
    assert error.value.code == 2
