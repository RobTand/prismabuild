"""A source change between observation and registration must remain latched."""
import socket
import threading

import pytest

from prismabuild import host_wait, queue_bell
from prismabuild.queue_bell_http import RequestCancellation

EPOCH = "a" * 32
KEY = "b" * 64


def body(seq=0, events=(), **extra):
    return {"epoch": EPOCH, "seq": seq, "events": list(events),
            "coverage": {"version": 1, "topics": ["done"]}, **extra}


class Clock:
    def __init__(self):
        self.now = 0.0
        self.delays = []

    def __call__(self):
        return self.now

    def wait(self, condition, delay):
        if delay is None:
            condition.wait()
        else:
            assert delay is not None
            self.delays.append(delay)
            self.now += delay


class Bell:
    def now_v2(self, topics):
        return body()


def local(kind):
    return host_wait.WakeInput(kind, "input")


def test_an_event_between_arm_and_registration_is_not_lost():
    source = host_wait.WakeInput(host_wait.WakeKind.RESOURCE, "cpu")
    with host_wait.HostWaitHub() as hub:
        before = hub.arm()
        hub.notify(source)
        wake = hub.wait_since(before, host_wait.ClaimWait(frozenset({source})))
    assert wake.kind is host_wait.WakeReasonKind.CHANGED
    assert wake.inputs == frozenset({source})


@pytest.mark.parametrize("kind", list(host_wait.WakeKind))
@pytest.mark.parametrize("when", ["before_registration", "during_wait"])
def test_each_source_is_latched_across_registration(kind, when):
    source = local(kind)
    calls = []

    def wait(condition, delay):
        calls.append(True)
        hub.notify(source)

    with host_wait.HostWaitHub(wait=wait) as hub:
        before = hub.arm()
        if when == "before_registration":
            hub.notify(source)
        wake = hub.wait_since(before, host_wait.ClaimWait(frozenset({source})))
    assert wake.inputs == frozenset({source})
    assert bool(calls) == (when == "during_wait")


def test_an_unrelated_source_does_not_wake_the_observer():
    clock = Clock()
    source = local(host_wait.WakeKind.RESOURCE)
    with host_wait.HostWaitHub(clock=clock, wait=clock.wait) as hub:
        before = hub.arm()
        hub.notify(local(host_wait.WakeKind.TIER))
        deadline = host_wait.RetryDeadline(host_wait.DeadlineReason.POLICY_EXPIRY, 7)
        wake = hub.wait_since(before, host_wait.ClaimWait(frozenset({source}), (deadline,)))
    assert wake.kind is host_wait.WakeReasonKind.DEADLINE
    assert wake.deadlines == (deadline,)
    assert clock.delays == [7]


def test_named_deadlines_keep_their_absolute_start_and_all_ties():
    clock = Clock()
    first = host_wait.RetryDeadline(host_wait.DeadlineReason.POLICY_EXPIRY, 5)
    tied = host_wait.RetryDeadline(host_wait.DeadlineReason.RENEWAL, 5)
    later = host_wait.RetryDeadline(host_wait.DeadlineReason.EVIDENCE_EXPIRY, 9)
    with host_wait.HostWaitHub(clock=clock, wait=clock.wait) as hub:
        before = hub.arm()
        clock.now = 3
        wake = hub.wait_since(before, host_wait.ClaimWait(deadlines=(first, later, tied)))
    assert clock.delays == [2]
    assert wake.deadlines == (first, tied)


def test_stop_wins_a_deadline_tie_and_remains_latched():
    clock = Clock()
    due = host_wait.RetryDeadline(host_wait.DeadlineReason.RENEWAL, 0)
    with host_wait.HostWaitHub(clock=clock) as hub:
        before = hub.arm()
        hub.stop()
        wake = hub.wait_since(before, host_wait.ClaimWait(deadlines=(due,)))
    assert wake.kind is host_wait.WakeReasonKind.STOP
    assert wake.deadlines == (due,)


def test_source_change_and_deadline_tie_are_both_reported():
    clock = Clock()
    source = local(host_wait.WakeKind.RESOURCE)
    deadline = host_wait.RetryDeadline(host_wait.DeadlineReason.RENEWAL, 0)
    with host_wait.HostWaitHub(clock=clock) as hub:
        before = hub.arm()
        hub.notify(source)
        wake = hub.wait_since(before, host_wait.ClaimWait(frozenset({source}), (deadline,)))
    assert wake.inputs == frozenset({source})
    assert wake.deadlines == (deadline,)


@pytest.mark.parametrize("kind", list(host_wait.WakeKind))
def test_each_source_during_cursor_capture_is_latched(kind):
    source = local(kind)

    class PublishingBell(Bell):
        def now_v2(self, topics):
            hub.notify(source)
            return body()

    with host_wait.HostWaitHub(client_factory=lambda c: PublishingBell()) as hub:
        before = hub.arm()
        wake = hub.wait_since(before, host_wait.ClaimWait(frozenset({source})))
    assert wake.inputs == frozenset({source})


def test_a_bell_event_during_authoritative_read_uses_the_armed_cursor():
    seen = []

    class ChangedBell(Bell):
        def wait_v2(self, since, version, topics, ids, timeout):
            seen.append(since)
            return body(1, ({"seq": 1, "topic": "done", "id": KEY},))

    with host_wait.HostWaitHub(topics=("done",), client_factory=lambda c: ChangedBell()) as hub:
        before = hub.arm()
        hub.reconciled(before)
        wake = hub.wait_since(before, host_wait.ClaimWait())
    assert seen == [queue_bell.Cursor(EPOCH, 0)]
    assert wake.hints == ({"seq": 1, "topic": "done", "id": KEY},)
    assert wake.coverage.complete


def test_empty_long_poll_renews_internally_without_a_consumer_wake():
    clock = Clock()
    renewals = []

    class RenewingBell(Bell):
        def wait_v2(self, since, version, topics, ids, timeout):
            renewals.append((since.seq, timeout))
            clock.now += timeout
            if len(renewals) == 3:
                return body(3, ({"seq": 3, "topic": "done", "id": KEY},))
            return body(len(renewals))

    with host_wait.HostWaitHub(topics=("done",), clock=clock,
                              client_factory=lambda c: RenewingBell()) as hub:
        wake = hub.wait_since(hub.arm(), host_wait.ClaimWait())
    assert renewals == [(0, 300), (1, 300), (2, 300)]
    assert wake.kind is host_wait.WakeReasonKind.CHANGED
    assert wake.hints[0]["seq"] == 3


def test_reconnection_does_not_acknowledge_reconstruction():
    class ResetBell(Bell):
        def wait_v2(self, *args):
            return body(reset=True)

    with host_wait.HostWaitHub(client_factory=lambda c: ResetBell()) as hub:
        old = hub.arm()
        assert hub.reconciled(old)
        wake = hub.wait_since(old, host_wait.ClaimWait())
        assert wake.kind is host_wait.WakeReasonKind.RESET
        assert not wake.coverage.complete
        fresh = hub.arm()
        assert not hub.coverage.complete
        assert not hub.reconciled(old)
        assert hub.reconciled(fresh)
        assert hub.coverage.complete


def test_repeated_immediate_resets_have_bounded_named_recovery():
    clock = Clock()

    class ResetBell(Bell):
        def wait_v2(self, *args):
            return body(reset=True)

    with host_wait.HostWaitHub(clock=clock, wait=clock.wait,
                              client_factory=lambda c: ResetBell()) as hub:
        kinds = []
        for _ in range(8):
            kinds.append(hub.wait_since(hub.arm(), host_wait.ClaimWait()).kind)
            assert not any(t.name == "prismabuild-bell-wait" for t in threading.enumerate())
    assert kinds == [host_wait.WakeReasonKind.RESET] * 8
    assert clock.delays == [0.25, 0.5, 1, 2, 4, 5, 5]


def test_unavailable_cursor_retries_stop_at_five_seconds():
    clock = Clock()

    class FailedBell:
        def now_v2(self, topics):
            raise queue_bell.Unavailable("queue callback unavailable")

    with host_wait.HostWaitHub(clock=clock, wait=clock.wait,
                              client_factory=lambda c: FailedBell()) as hub:
        for _ in range(8):
            wake = hub.wait_since(hub.arm(), host_wait.ClaimWait())
            assert wake.kind is host_wait.WakeReasonKind.UNAVAILABLE
            assert not wake.coverage.complete
            assert wake.diagnostic == "queue callback unavailable"
    assert clock.delays == [0.25, 0.5, 1, 2, 4, 5, 5, 5]


def test_a_local_event_cancels_recovery_without_advancing_the_retry():
    clock = Clock()
    source = local(host_wait.WakeKind.CAPABILITY)
    calls = []

    class FailedBell:
        def now_v2(self, topics):
            calls.append(True)
            raise queue_bell.Unavailable("no callback")

    with host_wait.HostWaitHub(clock=clock, wait=clock.wait,
                              client_factory=lambda c: FailedBell()) as hub:
        before = hub.arm()
        hub.notify(source)
        wake = hub.wait_since(before, host_wait.ClaimWait(frozenset({source})))
        before = hub.arm()
        assert len(calls) == 1
        assert before.failure == "no callback"
        assert wake.inputs == frozenset({source})
        assert clock.delays == []


def test_nfs_visibility_recovery_never_extends_the_original_read_budget():
    backoff = host_wait.RecoveryBackoff.nfs_visibility()
    now = 0.0
    times = []
    for _ in range(8):
        deadline = backoff.next(now, operation_deadline=2.5)
        assert deadline.reason is host_wait.DeadlineReason.NFS_VISIBILITY
        times.append(deadline.monotonic_at)
        now = deadline.monotonic_at
    assert times == [0.05, 0.15000000000000002, 0.35000000000000003, 0.75, 1.55, 2.5, 2.5, 2.5]


def test_cancellation_before_socket_attachment_prevents_connect():
    cancellation = RequestCancellation()
    cancellation.cancel()
    with socket.socket() as sock:
        with pytest.raises(OSError, match="cancelled"):
            cancellation.attach(sock)
        assert sock.fileno() == -1


@pytest.mark.parametrize("trigger", ["local", "stop", "close", "deadline"])
def test_local_event_or_stop_shutdowns_the_blocking_bell_and_joins(tmp_path, trigger):
    source = local(host_wait.WakeKind.RESOURCE)
    waiting = threading.Event()
    disconnected = threading.Event()
    accepted = []
    failure = []
    count = 5 if trigger == "local" else 1
    clock = Clock()

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()

        def serve():
            try:
                for _ in range(count):
                    with listener.accept()[0] as peer:
                        accepted.append(True)
                        request = b""
                        while b"\r\n\r\n" not in request:
                            request += peer.recv(4096)
                        waiting.set()
                        assert peer.recv(1) == b""
                        disconnected.set()
            except BaseException as exc:
                failure.append(exc)
                waiting.set()

        server = threading.Thread(target=serve)
        server.start()

        class Client(queue_bell.Client):
            def now_v2(self, topics):
                return body()

        with host_wait.HostWaitHub(topics=("done",), clock=clock, client_factory=lambda c: Client(
                tmp_path, host="127.0.0.1", port=listener.getsockname()[1], cancellation=c)) as hub:
            for _ in range(count):
                waiting.clear()
                disconnected.clear()
                before = hub.arm()
                outcomes = []
                deadlines = (host_wait.RetryDeadline(host_wait.DeadlineReason.RENEWAL, 100),)
                observer = threading.Thread(target=lambda: outcomes.append(hub.wait_since(
                    before, host_wait.ClaimWait(frozenset({source}), deadlines))))
                observer.start()
                assert waiting.wait()
                assert not failure
                if trigger == "local":
                    hub.notify(source)
                elif trigger == "stop":
                    hub.stop()
                elif trigger == "deadline":
                    clock.now = 100
                    hub.notify(local(host_wait.WakeKind.CONTROL))
                else:
                    hub.close()
                observer.join()
                assert not observer.is_alive()
                assert disconnected.wait()
                expected = {"local": host_wait.WakeReasonKind.CHANGED,
                            "deadline": host_wait.WakeReasonKind.DEADLINE,
                            "stop": host_wait.WakeReasonKind.STOP,
                            "close": host_wait.WakeReasonKind.STOP}[trigger]
                assert outcomes[0].kind is expected
                assert not any(t.name == "prismabuild-bell-wait" for t in threading.enumerate())
        server.join()
        assert not server.is_alive()
        assert not failure
    assert len(accepted) == count


@pytest.mark.parametrize("change", ["epoch", "coverage"])
def test_arm_latches_an_epoch_or_coverage_transition_until_reconstruction(change):
    calls = []

    class ChangingBell(Bell):
        def wait_v2(self, *args):
            return body(reset=True)

        def now_v2(self, topics):
            calls.append(True)
            answer = body()
            if len(calls) > 1:
                if change == "epoch":
                    answer["epoch"] = "c" * 32
                else:
                    answer["coverage"]["version"] = 2
            return answer

    with host_wait.HostWaitHub(client_factory=lambda c: ChangingBell()) as hub:
        old = hub.arm()
        assert hub.reconciled(old)
        assert hub.wait_since(old, host_wait.ClaimWait()).kind is host_wait.WakeReasonKind.RESET
        new = hub.arm()
        assert not hub.coverage.complete
        assert new.reset_generation > old.reset_generation
        assert not hub.reconciled(old)
        assert hub.reconciled(new)
        assert hub.coverage.complete


def test_immediate_empty_renewals_enter_backoff_instead_of_spinning():
    clock = Clock()

    class EmptyBell(Bell):
        def wait_v2(self, *args):
            return body()

    with host_wait.HostWaitHub(clock=clock, wait=clock.wait,
                              client_factory=lambda c: EmptyBell()) as hub:
        wake = hub.wait_since(hub.arm(), host_wait.ClaimWait())
    assert wake.kind is host_wait.WakeReasonKind.UNAVAILABLE
    assert clock.delays == [0.25]
    assert wake.diagnostic == "bell returned an immediate empty renewal"


def test_a_reader_factory_failure_is_delivered_to_the_observer():
    count = []

    def factory(cancellation):
        count.append(True)
        if len(count) > 1:
            raise RuntimeError("factory failed")
        return Bell()

    with host_wait.HostWaitHub(client_factory=factory) as hub:
        with pytest.raises(RuntimeError, match="factory failed"):
            hub.wait_since(hub.arm(), host_wait.ClaimWait())
    assert not any(t.name == "prismabuild-bell-wait" for t in threading.enumerate())


def test_arm_identity_change_reports_recovery_without_a_zero_wait_loop():
    clock = Clock()
    captures = []

    class ChangingBell(Bell):
        def now_v2(self, topics):
            answer = body()
            captures.append(True)
            if len(captures) > 1:
                answer["epoch"] = "c" * 32
            return answer

        def wait_v2(self, *args):
            raise AssertionError("reset requires reconstruction before remote waiting")

    class RecoveryBell(ChangingBell):
        def wait_v2(self, *args):
            return body(reset=True)

    with host_wait.HostWaitHub(clock=clock, wait=clock.wait,
                              client_factory=lambda c: RecoveryBell()) as hub:
        first = hub.arm()
        hub.reconciled(first)
        hub.wait_since(first, host_wait.ClaimWait())
        wake = hub.wait_since(hub.arm(), host_wait.ClaimWait())
    assert wake.kind is host_wait.WakeReasonKind.RESET
    assert clock.delays == [0.25]
    assert not wake.coverage.complete


def test_reconstruction_cancels_stale_recovery_before_the_next_wait():
    captures = []

    class ChangingBell(Bell):
        def now_v2(self, topics):
            answer = body()
            captures.append(True)
            if len(captures) > 1:
                answer["epoch"] = "c" * 32
            return answer

        def wait_v2(self, *args):
            if len(captures) == 1:
                return body(reset=True)
            return body(1, ({"seq": 1, "topic": "done", "id": KEY},))

    with host_wait.HostWaitHub(client_factory=lambda c: ChangingBell()) as hub:
        first = hub.arm()
        hub.reconciled(first)
        hub.wait_since(first, host_wait.ClaimWait())
        before = hub.arm()
        assert hub.reconciled(before)
        wake = hub.wait_since(before, host_wait.ClaimWait())
    assert wake.kind is host_wait.WakeReasonKind.CHANGED
    assert wake.coverage.complete


def test_cancelling_a_bell_read_keeps_its_cursor_for_the_next_observation():
    source = local(host_wait.WakeKind.RESOURCE)
    started = threading.Event()
    release = threading.Event()
    cursors = []
    captures = []

    class CancelSocket:
        def shutdown(self, how):
            release.set()

        def close(self):
            pass

    class InterruptedBell(Bell):
        def __init__(self, cancellation):
            self.cancellation = cancellation

        def now_v2(self, topics):
            captures.append(True)
            return body(len(captures) - 1)

        def wait_v2(self, since, *args):
            cursors.append(since.seq)
            if len(cursors) == 1:
                self.cancellation.attach(CancelSocket())
                started.set()
                assert release.wait()
                raise queue_bell.Unavailable("request cancelled before queued event delivery")
            if since.seq >= 1:
                hub.stop()
                return body(since.seq)
            return body(1, ({"seq": 1, "topic": "done", "id": KEY},))

    with host_wait.HostWaitHub(client_factory=InterruptedBell) as hub:
        before = hub.arm()
        outcomes = []
        observer = threading.Thread(target=lambda: outcomes.append(hub.wait_since(
            before, host_wait.ClaimWait(frozenset({source})))))
        observer.start()
        assert started.wait()
        hub.notify(source)
        observer.join()
        assert not observer.is_alive()
        assert outcomes[0].inputs == frozenset({source})
        wake = hub.wait_since(hub.arm(), host_wait.ClaimWait())
    assert captures == [True]
    assert cursors == [0, 0]
    assert wake.hints == ({"seq": 1, "topic": "done", "id": KEY},)


@pytest.mark.parametrize("deadline", [None, float("inf"), float("nan")])
def test_nfs_visibility_requires_a_finite_original_read_deadline(deadline):
    backoff = host_wait.RecoveryBackoff.nfs_visibility()
    with pytest.raises(ValueError, match="deadline"):
        backoff.next(0, operation_deadline=deadline)


def test_a_local_and_bell_change_in_one_wake_preserve_both_inputs():
    source = local(host_wait.WakeKind.RESOURCE)

    class ChangedBell(Bell):
        def wait_v2(self, *args):
            return body(1, ({"seq": 1, "topic": "done", "id": KEY},))

    def wait(condition, delay):
        assert condition.wait()
        hub.notify(source)

    with host_wait.HostWaitHub(wait=wait, client_factory=lambda c: ChangedBell()) as hub:
        wake = hub.wait_since(hub.arm(), host_wait.ClaimWait(frozenset({source})))
        after = hub.arm()
    assert wake.inputs == frozenset({source})
    assert wake.hints == ({"seq": 1, "topic": "done", "id": KEY},)
    assert after.bell.cursor.seq == 1
