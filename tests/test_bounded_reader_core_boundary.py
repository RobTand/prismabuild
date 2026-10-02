"""The diagnostic CLI delegates to the same core-private reader boundary.

Real fork/JSON observations exercise both entry points. Fault injection targets
its definition module, not an incidental copy of a private CLI name binding.
"""
import os

import pbstatus

import prismabuild._bounded_reader as reader


def test_existing_diagnostic_names_refer_to_the_core_definitions():
    assert pbstatus.Deadline is reader.Deadline
    assert pbstatus.KILL_GRACE_S == reader.KILL_GRACE_S == 0.25
    for name in ("_proc_stat_fields", "_reap_status_within",
                 "_reader_exit_detail", "_write_reader_payload",
                 "_isolate_child_fds", "_stop_reader", "_bounded_reader",
                 "_starttime_ticks", "_ANNOUNCE_RETAINED"):
        assert getattr(pbstatus, name) is getattr(reader, name)


def test_existing_retained_reader_consumers_share_the_core_reap_boundary(monkeypatch):
    calls = []

    def reaped(pid, grace_s):
        calls.append((pid, grace_s))
        return False

    monkeypatch.setattr(reader, "_reap_within", reaped)
    assert pbstatus._reap_within(123, 0.25) is False
    assert calls == [(123, 0.25)]


def test_zero_budget_preserves_in_process_value_identity(monkeypatch):
    def forbidden():
        raise AssertionError("the zero-budget compatibility path must not fork")

    monkeypatch.setattr(reader.os, "fork", forbidden)
    value = object()
    calls = []
    abandoned = []

    def read():
        calls.append(os.getpid())
        return value

    result = pbstatus.bounded("unbounded", read,
                              deadline=pbstatus.Deadline(0), abandoned=abandoned)
    assert result["status"] == "ok"
    assert result["value"] is value
    assert calls == [os.getpid()]
    assert abandoned == []


def test_both_entry_points_preserve_the_real_fork_and_json_boundary():
    parent = os.getpid()
    for observe in (reader.bounded, pbstatus.bounded):
        abandoned = []
        result = observe("core-extraction", lambda: (os.getpid(), "read"),
                         deadline=reader.Deadline(5), abandoned=abandoned)
        assert result["status"] == "ok"
        assert isinstance(result["value"], list), "JSON normalizes the child tuple"
        assert result["value"][0] != parent
        assert result["value"][1] == "read"
        assert abandoned == []


def test_core_setup_failure_is_reported_through_the_cli_wrapper(monkeypatch):
    def fail(_fd):
        raise OSError("core reader setup fixture")

    monkeypatch.setattr(reader, "_isolate_child_fds", fail)
    abandoned = []
    result = pbstatus.bounded("setup", lambda: "must not run",
                              deadline=reader.Deadline(5), abandoned=abandoned)
    assert result["status"] == "error"
    assert result["type"] == "OSError"
    assert "stage=isolate_fds: core reader setup fixture" in result["error"]
    assert "exit code 1" in result["error"]
    assert abandoned == []


def test_expired_section_cap_does_not_fork_or_fabricate_a_read(monkeypatch):
    def forbidden():
        raise AssertionError("an expired section cap must not fork or read")

    monkeypatch.setattr(reader.os, "fork", forbidden)
    abandoned = []
    result = pbstatus.bounded("expired", forbidden, deadline=reader.Deadline(5),
                              abandoned=abandoned, cap_s=0, announce_retained=False)
    assert result == {"status": "timed_out", "elapsed_s": 0.0, "started": False}
    assert abandoned == []
