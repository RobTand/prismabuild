"""Optional queue hints cannot take the place of authoritative reads (#1894)."""
import socket

import pytest

from prismabuild import action_wait, queue_bell

KEY = "a" * 64
INITIAL = queue_bell.Cursor("a" * 32, 0)


def await_outcome(observe, bell, deadline, **kwargs):
    return action_wait.await_action(
        observe, generation=None, patience=action_wait.WaitUntil(deadline),
        profile=action_wait.AwaitProfile.SYNCHRONOUS, tool="test", subject=KEY,
        notice=lambda event: None, bell=bell, bell_key=KEY, **kwargs)


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, delay):
        self.sleeps.append(delay)
        self.now += delay


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(action_wait, "time", clock)
    return clock


@pytest.mark.parametrize("with_bell", [False, True])
def test_ready_outcome_is_read_even_with_a_nonresponding_http_bell(tmp_path, with_bell):
    # TCP connects successfully, but this peer never sends any HTTP response.
    # The whole positive patience fits inside Client.now's one-second cap.
    with socket.socket() as peer:
        peer.bind(("127.0.0.1", 0))
        peer.listen(1)
        client = queue_bell.Client(tmp_path, host="127.0.0.1", port=peer.getsockname()[1])
        observations = []

        def observe(**kwargs):
            observations.append(kwargs["budget_s"])
            return action_wait.Ready("already done", None)

        result = await_outcome(observe, client if with_bell else None,
                               action_wait.time.monotonic() + 1.0)
    assert isinstance(result, action_wait.Ready)
    assert result.value == "already done"
    assert len(observations) == 1
    assert 0 < observations[0] <= 1.0


def test_a_slow_cursor_request_cannot_preempt_the_first_read(clock):
    class SlowBell:
        def now(self, *, budget_s):
            clock.now += budget_s
            raise queue_bell.Unavailable("silent peer")

    observations = []

    def observe(**kwargs):
        observations.append(clock.now)
        return action_wait.Ready("already done", None)

    result = await_outcome(observe, SlowBell(), 0.2)
    assert isinstance(result, action_wait.Ready)
    assert observations == [0.0]
    assert clock.now == 0.0


def test_a_near_deadline_wakeup_rereads_without_another_cursor_request(clock):
    cursor_calls, observations = [], []

    class LateBell:
        def now(self, *, budget_s):
            cursor_calls.append(budget_s)
            if len(cursor_calls) > 1:
                clock.now += min(1.0, budget_s)
                raise queue_bell.Unavailable("slow cursor response")
            return INITIAL

        def wait(self, since, dirs, keys, timeout):
            assert since == INITIAL
            assert dirs == queue_bell.TERMINAL_DIRS and keys == (KEY,)
            assert timeout == 10.0
            clock.now = 9.5
            return {"epoch": INITIAL.epoch, "seq": 1,
                    "events": [{"seq": 1, "dir": "done", "key": KEY}]}

    def observe(**kwargs):
        observations.append((clock.now, kwargs["budget_s"]))
        if clock.now == 9.5:
            return action_wait.Ready("notified ending", None)
        return action_wait.Pending(None)

    result = await_outcome(observe, LateBell(), 10.0)
    assert isinstance(result, action_wait.Ready)
    assert result.value == "notified ending"
    assert len(cursor_calls) == 1
    assert observations[-1] == (9.5, 0.5)
    assert all(start < 10.0 for start, _budget in observations)
    assert clock.sleeps == []


def test_publication_during_cursor_initialization_is_read_before_waiting(clock):
    published, observations = [], []

    class PublishingBell:
        def now(self, **kwargs):
            # This ending predates the returned cursor, so wait cannot report it.
            assert observations == [False]
            published.append(True)
            return INITIAL

        def wait(self, *args):
            pytest.fail("wait skipped the confirming authoritative read")

    def observe(**kwargs):
        observations.append(bool(published))
        return (action_wait.Ready("ending behind cursor", None) if published
                else action_wait.Pending(None))

    result = await_outcome(observe, PublishingBell(), 10.0)
    assert isinstance(result, action_wait.Ready)
    assert observations == [False, True]
    assert clock.sleeps == []


@pytest.mark.parametrize("reply_kind", ["events", "empty", "reset"])
def test_each_reply_cursor_covers_the_next_read_and_wait(clock, reply_kind):
    next_cursor = queue_bell.Cursor("b" * 32 if reply_kind == "reset" else INITIAL.epoch, 10)
    waits, observations, cursor_calls = [], [], []

    class AdvancingBell:
        def now(self, **kwargs):
            cursor_calls.append(True)
            assert len(cursor_calls) == 1, "a wake must not fetch another cursor"
            return INITIAL

        def wait(self, since, dirs, keys, timeout):
            waits.append(since)
            clock.now += 1.0
            reply = {"epoch": next_cursor.epoch, "seq": next_cursor.seq}
            if len(waits) == 1 and reply_kind == "reset":
                return {**reply, "reset": True}
            return {**reply, "events": [] if reply_kind == "empty" else [
                {"seq": 10, "dir": "done", "key": KEY}]}

    def observe(**kwargs):
        observations.append(clock.now)
        return (action_wait.Ready("done", None) if len(waits) == 2
                else action_wait.Pending(None))

    result = await_outcome(observe, AdvancingBell(), 10.0)
    assert isinstance(result, action_wait.Ready)
    assert waits == [INITIAL, next_cursor]
    assert observations == [0.0, 0.0, 1.0, 2.0]
    assert clock.sleeps == []


@pytest.mark.parametrize("expires_during", ["now", "wait"])
def test_bell_expiry_never_starts_a_late_synchronous_read(clock, expires_during):
    observations = []

    class ExpiringBell:
        def now(self, *, budget_s):
            if expires_during == "now":
                clock.now += budget_s
            return INITIAL

        def wait(self, since, dirs, keys, timeout):
            clock.now += timeout
            return {"epoch": INITIAL.epoch, "seq": 1,
                    "events": [{"seq": 1, "dir": "done", "key": KEY}]}

    def observe(**kwargs):
        observations.append(clock.now)
        assert clock.now < 1.0, "bell expiry must not authorize a late read"
        return action_wait.Pending(None)

    result = await_outcome(observe, ExpiringBell(), 1.0)
    assert isinstance(result, action_wait.PatienceExpired)
    assert observations == ([0.0] if expires_during == "now" else [0.0, 0.0])
    assert clock.now == 1.0


def test_disabled_callback_does_not_fetch_a_cursor(clock):
    class UnusedBell:
        def now(self, **kwargs):
            pytest.fail("discovery without a callback must not fetch a cursor")

    def observe(**kwargs):
        return (action_wait.Ready("discovered", None) if clock.now > 0
                else action_wait.Pending(None))

    result = await_outcome(observe, UnusedBell(), 1.0, use_bell=lambda: False)
    assert isinstance(result, action_wait.Ready)
    assert clock.sleeps == [0.25]
