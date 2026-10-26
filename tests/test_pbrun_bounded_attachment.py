"""Detached attachment discovery must not park its caller on a queue read."""
from __future__ import annotations
from pathlib import Path
import json
import os
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools" / "fleet"))
import pbrun  # noqa: E402
from prismabuild import submission as submit_core
from prismabuild import pool  # noqa: E402


@pytest.fixture(autouse=True)
def attachment_clock(monkeypatch):
    monkeypatch.setattr(pool, "_now", lambda: 2_000_000_000.)
    reader = submit_core.action_wait._reader
    # These are attachment-policy tests. Model deadline expiry explicitly, and
    # wait for our exact children instead of testing the reader's kill grace.
    monkeypatch.setattr(reader.Deadline, "remaining", lambda self: 60. if self.bounded else None)
    def reap_status(pid, _grace):
        child, status = os.waitpid(pid, 0)
        return child == pid, status
    monkeypatch.setattr(reader, "_reap_status_within", reap_status)
    monkeypatch.setattr(reader, "_reap_within", lambda pid, grace: reap_status(pid, grace)[0])

@pytest.fixture
def expire_at_read(monkeypatch):
    """A pipe acknowledges the child's actual FIFO read before clock expiry."""
    reader = submit_core.action_wait._reader
    parent = os.getpid()
    receive, send = os.pipe()
    isolate = reader._isolate_child_fds
    select = reader.select.select
    open_path = Path.open
    observation = submit_core.action_wait.read_observation
    def gate(fifo):
        def preserve(write_fd, *, keep_fds=()):
            return isolate(write_fd, keep_fds=(*keep_fds, send))
        def observed(path, *args, **kwargs):
            if path == fifo:
                assert os.getpid() != parent, "attachment read escaped its child"
                os.write(send, b"R")
            return open_path(path, *args, **kwargs)
        def expire(readers, writers, errors, _timeout):
            ready, _, _ = select([receive, *readers], [], [], 60.)
            if receive not in ready:
                raise AssertionError("child did not enter the gated FIFO read")
            if os.read(receive, 1) != b"R":
                raise AssertionError("child's read handshake was incomplete")
            # Inject the kernel's deadline event after the causal read barrier.
            return [], [], []
        monkeypatch.setattr(reader, "_isolate_child_fds", preserve)
        monkeypatch.setattr(Path, "open", observed)
        def attachment_only(section, read, **kwargs):
            if section != "attachment discovery":
                return observation(section, read, **kwargs)
            with monkeypatch.context() as scoped:
                scoped.setattr(reader.select, "select", expire)
                return observation(section, read, **kwargs)
        monkeypatch.setattr(submit_core.action_wait, "read_observation", attachment_only)
    try:
        yield gate
    finally:
        os.close(receive)
        os.close(send)


def test_detached_fifo_attachment_read_refuses_before_publication(tmp_path, monkeypatch, capsys, expire_at_read):
    from test_pbrun_detach import _checkout, _queue, _run_pbrun
    work = _checkout(tmp_path)
    _queue(tmp_path)
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach", "--transport", "pool") == 0
    row = json.loads(capsys.readouterr().out)
    fifo = Path(row["submission"])
    fifo.unlink()
    os.mkfifo(fifo)
    expire_at_read(fifo)
    monkeypatch.setattr(submit_core, "publish_or_refuse", lambda *_args, **_kwargs:
                        pytest.fail("unavailable attachment must not publish"))
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach", "--transport", "pool") == 74
    assert fifo.is_fifo()
    assert [p.name for p in fifo.parent.iterdir()] == [fifo.name]
    error = capsys.readouterr().err
    assert "unavailable detached attachment" in error
    assert "attachment discovery timed out" in error


@pytest.mark.parametrize("state,age,ended,attaches", [
    (pool.record_store.READY, None, False, True),
    (pool.record_store.CLAIMED, 0.0, False, True),
    (pool.record_store.CLAIMED, -2.0, False, True),
    (pool.record_store.CLAIMED, "invalid", False, True),
    (pool.record_store.CLAIMED, pool.LEASE_TIMEOUT_S + 60, False, False),
    (pool.record_store.READY, None, True, False),
])
def test_pool_attachment_keeps_generation_and_liveness(
    tmp_path: Path, state, age, ended, attaches,
) -> None:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    key = "b" * 64
    generation = 123.0
    row = {"action_key": key, "published_unix": generation}
    record = queue.records.item_path(state, key)
    record.write_text(json.dumps(row))
    if age is not None:
        queue.records.lease_path(key).write_text(json.dumps({"heartbeat_unix": 2_000_000_000. - age if isinstance(age, (float, int)) else age}))
    if ended:
        queue.records.replace_record_at(
            queue.records.item_path(pool.record_store.DONE, key),
            {**row, 'status': 'executed', 'detail': {'returncode': 0}},
        )
    result = submit_core.bounded_attachment(queue, key, lane_root=tmp_path / "lane")
    if attaches:
        assert result == {"transport": "pool", "generation": generation,
                          "submission": str(record), "job_id": ""}
    else:
        assert result is None


@pytest.mark.parametrize("state,attaches", [("RUNNING", True), ("COMPLETED", False),
                                           (None, False)])
def test_slurm_query_remains_in_parent_after_bounded_record_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, state, attaches,
) -> None:
    parent = os.getpid()
    observed = tmp_path / "reader-pid"
    directory = tmp_path / "lane"
    submission = {"job_id": "41", "attempt": 2, "directory": str(directory)}

    def records(*_args, **_kwargs):
        assert os.getpid() != parent
        observed.write_text(str(os.getpid()))
        return "slurm", 200.0, submission

    def query(job_id, **kwargs):
        assert os.getpid() == parent
        return None if state is None else (state, {})

    monkeypatch.setattr(submit_core, "outstanding_submission", records)
    monkeypatch.setattr(submit_core.action_wait, "await_ending", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(submit_core.slurm_lane, "query_state", query)
    result = submit_core.bounded_attachment(
        pool.PoolQueue(tmp_path / "queue"), "c" * 64, squeue="private-squeue")
    assert int(observed.read_text()) != parent
    if attaches:
        assert result["generation"] == 200.0
        assert result["job_id"] == "41"
        assert result["submission"] == str(directory / "submissions" / "200.000000-002.json")
    else:
        assert result is None


def test_retained_reader_refuses_before_scheduler_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def retained(_section, _read, *, deadline, abandoned, announce_retained=True):
        # The attachment path names the retained reader in its own refusal,
        # so it asks pbstatus not to announce it too (#1048, #1051).
        assert announce_retained is False
        abandoned.append({"section": _section, "pid": 12345,
                          "starttime_ticks": 67890, "since_unix": 1.0})
        return {"status": "ok", "value": {"transport": "slurm",
                "generation": 200.0, "job_id": "41", "submission": "unused"}}

    monkeypatch.setattr(submit_core.action_wait._reader, "bounded", retained)
    monkeypatch.setattr(submit_core.slurm_lane, "query_state", lambda *_args, **_kwargs:
                        pytest.fail("retained reader must stop discovery"))
    with pytest.raises(submit_core.action_wait.OutcomeReadUnavailable, match='"starttime_ticks": 67890'):
        submit_core.bounded_attachment(pool.PoolQueue(tmp_path / "queue"), "c" * 64)


def test_claim_lease_read_is_inside_attachment_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, expire_at_read,
) -> None:
    queue = pool.PoolQueue(tmp_path / "queue")
    queue.records.ensure_layout()
    key = "d" * 64
    queue.records.item_path(pool.record_store.CLAIMED, key).write_text(json.dumps({
        "action_key": key, "published_unix": 123.0}))
    os.mkfifo(queue.records.lease_path(key))
    expire_at_read(queue.records.lease_path(key))
    with pytest.raises(submit_core.action_wait.OutcomeReadUnavailable, match="attachment discovery timed out"):
        submit_core.bounded_attachment(queue, key, lane_root=tmp_path / "lane")


def test_attachment_setup_failure_returns_74_without_publishing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    from test_pbrun_detach import _checkout, _queue, _run_pbrun

    work = _checkout(tmp_path)
    queue = _queue(tmp_path)

    def failed(*_args, **_kwargs):
        raise OSError("reader setup refused")

    monkeypatch.setattr(submit_core, "bounded_attachment", failed)
    monkeypatch.setattr(submit_core, "publish_or_refuse", lambda *_args, **_kwargs:
                        pytest.fail("failed attachment must not publish"))
    assert _run_pbrun(tmp_path, monkeypatch, work, "--detach") == 74
    assert "reader setup refused" in capsys.readouterr().err
    assert not list((queue.root / pool.record_store.READY).glob("*.json"))
