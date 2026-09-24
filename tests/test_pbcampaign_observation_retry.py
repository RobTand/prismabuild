"""A ``--max-inflight`` window keeps waiting through one unobserved read (#558).

The window polls every pending key at zero patience. A reaped, timed-out read
is not news about the action, so it must not stop publication while the
campaign's own deadline lasts, and it must not release the key's slot.
"""
import pbcampaign


KEYS = [f"{i + 1:064x}" for i in range(3)]


def _fakes(monkeypatch, polls):
    events = []

    def submit(row, **kwargs):
        i = row["index"]
        events.append(("submit", i))
        return {"action_key": KEYS[i], "published_unix": 100.0 + i,
                "status": "submitted", "transport": "pool"}

    states = iter(polls)

    def wait(queue, waiting, *, generations, **kwargs):
        events.append(("poll", [KEYS.index(k) for k in waiting]))
        state = next(states)
        rows = []
        for key in waiting:
            status = state[KEYS.index(key)]
            if status == "unobserved":
                rows.append(pbcampaign.pbwait._row(
                    key, "record_error", observation_timed_out=True,
                    note="pbwait observation timed out after 5.0s"))
            else:
                rows.append(pbcampaign.pbwait._row(key, status, transport="pool"))
        return rows

    monkeypatch.setattr(pbcampaign, "submit_row", submit)
    monkeypatch.setattr(pbcampaign.pbwait, "wait_for_keys", wait)
    monkeypatch.setattr(pbcampaign, "_pool_slot_occupied", lambda *args: False)
    monkeypatch.setattr(pbcampaign.time, "sleep", lambda seconds: None)
    return events


def test_unobserved_read_keeps_the_slot_and_the_window_open(monkeypatch):
    events = _fakes(monkeypatch, [
        {0: "unobserved"}, {0: "executed"}, {1: "executed"},
    ])
    submissions, waited = pbcampaign.run_windowed(
        [{"index": i} for i in range(2)], transport="pool", max_inflight=1, wait_s=30,
    )
    assert events == [("submit", 0), ("poll", [0]), ("poll", [0]),
                      ("submit", 1), ("poll", [1])]
    assert [s["status"] for s in submissions] == ["submitted", "submitted"]
    assert pbcampaign.pbwait.verdict(pbcampaign.rows_for(submissions, waited)) == 0


def test_unobserved_read_at_the_campaign_deadline_is_exit_74(monkeypatch):
    """Guard: a deadline that passes on an unobserved read claims no verdict."""

    events = _fakes(monkeypatch, [{0: "unobserved"}])
    clock = [100.0]
    monkeypatch.setattr(pbcampaign.time, "monotonic", lambda: clock[0])
    poll = pbcampaign.pbwait.wait_for_keys

    def expired(*args, **kwargs):
        result = poll(*args, **kwargs)
        clock[0] += 6.0
        return result

    monkeypatch.setattr(pbcampaign.pbwait, "wait_for_keys", expired)
    submissions, waited = pbcampaign.run_windowed(
        [{"index": i} for i in range(2)], transport="pool", max_inflight=1, wait_s=5,
    )
    assert events == [("submit", 0), ("poll", [0])]
    assert submissions[1]["status"] == "not_submitted"
    assert pbcampaign.pbwait.verdict(pbcampaign.rows_for(submissions, waited)) == 74


# --------------------------------------------------------------------------
# #1048: a retained reader holds its key, and no second reader starts beside it
# --------------------------------------------------------------------------

def _retained_fakes(monkeypatch, polls, *, still_at_deadline=False):
    """``_fakes``, plus a ``retained`` state and a stub for the wait-out."""

    events = _fakes(monkeypatch, [])
    states = iter(polls)

    def wait(queue, waiting, *, generations, **kwargs):
        events.append(("poll", [KEYS.index(k) for k in waiting]))
        state = next(states)
        rows = []
        for key in waiting:
            status = state[KEYS.index(key)]
            if status == "retained":
                rows.append(pbcampaign.pbwait._row(
                    key, "record_error",
                    retained_readers=[{"pid": 4242, "starttime_ticks": 7}],
                    note="pbwait observation timed out after 5.0s and its "
                         "reader could not be reaped"))
            else:
                rows.append(pbcampaign.pbwait._row(key, status, transport="pool"))
        return rows

    def wait_out(retained, deadline, **kwargs):
        events.append(("wait-out", [child["pid"] for child in retained]))
        return (list(retained) if still_at_deadline else []), 1.5

    monkeypatch.setattr(pbcampaign.pbwait, "wait_for_keys", wait)
    monkeypatch.setattr(pbcampaign.pbrun, "wait_out_retained_readers", wait_out)
    return events


def test_a_retained_reader_is_waited_out_before_the_window_reads_again(monkeypatch):
    events = _retained_fakes(monkeypatch, [
        {0: "retained"}, {0: "executed"}, {1: "executed"},
    ])
    submissions, waited = pbcampaign.run_windowed(
        [{"index": i} for i in range(2)], transport="pool", max_inflight=1, wait_s=30,
    )
    assert events == [("submit", 0), ("poll", [0]), ("wait-out", [4242]),
                      ("poll", [0]), ("submit", 1), ("poll", [1])]
    assert pbcampaign.pbwait.verdict(pbcampaign.rows_for(submissions, waited)) == 0


def test_a_reader_retained_at_the_campaign_deadline_stops_it_with_74(monkeypatch):
    """No second read for that key, no new submission, and no verdict."""

    events = _retained_fakes(monkeypatch, [{0: "retained"}], still_at_deadline=True)
    submissions, waited = pbcampaign.run_windowed(
        [{"index": i} for i in range(2)], transport="pool", max_inflight=1, wait_s=30,
    )
    assert events == [("submit", 0), ("poll", [0]), ("wait-out", [4242])]
    assert submissions[1]["status"] == "not_submitted"
    table = pbcampaign.rows_for(submissions, waited)
    assert pbcampaign.pbwait.verdict(table) == 74
    assert "still retained" in table[0]["note"]
