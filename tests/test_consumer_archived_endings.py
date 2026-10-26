"""Pinned consumer waits verify their immutable generation without log expansion."""
from __future__ import annotations

import json
import multiprocessing
import time

import pytest

from test_consumer_surface import consumer_world, consumer_action
import pbwait
from prismabuild import core as pb, pool


def _overwritten_success(world, monkeypatch, *, padding=""):
    monkeypatch.setattr(pool, "_now", lambda: 1000.0)
    key, receipt = consumer_action(world, "same-work", log_padding=("stdout", padding))
    first = json.loads(world.queue.records.item_path("done", key).read_bytes())
    monkeypatch.setattr(pool, "_now", lambda: 2000.0)
    second, _ = consumer_action(world, "same-work")
    assert second == key
    assert json.loads(world.queue.records.item_path("done", key).read_bytes())["published_unix"] == 2000.0
    return key, first, receipt


def _wait(world, key, first):
    return pbwait.wait_one(world.queue, key, cas=pb.PrismaBuildCAS(world.root / "cas"),
                          generation=first["published_unix"], deadline=time.monotonic(),
                          consumer=True)


@pytest.mark.parametrize("missing_terminal", [False, True])
def test_pinned_archive_success_survives_replacement(consumer_world, monkeypatch, missing_terminal):
    key, first, receipt = _overwritten_success(consumer_world, monkeypatch)
    if missing_terminal:
        consumer_world.queue.records.item_path("done", key).unlink()
    row = _wait(consumer_world, key, first)
    assert (row["status"], row["succeeded"], row["exit_code"]) == ("executed", True, 0), row
    assert row["publication"]["published_unix"] == 1000.0
    assert row["result"]["verified"] is True
    assert row["result"]["receipt_sha256"] == receipt["receipt_sha256"]
    expected = json.loads(consumer_world.queue.records.attempt_path(first, 1).read_bytes())["logs"]["stdout"]
    assert row["result"]["stdout"]["sha256"] == expected["sha256"]
    assert row["result"]["stdout"]["path"] == str(consumer_world.queue.root / expected["path"])


def test_archive_selection_never_expands_logs(consumer_world, monkeypatch):
    cap = 16 * 1024 * 1024
    key, first, _ = _overwritten_success(consumer_world, monkeypatch, padding="x" * cap + "\n")
    maximum = multiprocessing.Value("Q", 0)
    original = pb.os.read

    def bounded_read(fd, size):
        with maximum.get_lock():
            maximum.value = max(maximum.value, size)
        return original(fd, size)

    def forbidden(*args, **kwargs):
        raise AssertionError("consumer expanded an unbounded archive record or log")

    monkeypatch.setattr(consumer_world.queue, "attempt_outcomes", forbidden)
    monkeypatch.setattr(consumer_world.queue.records, "attempt_bytes", forbidden)
    monkeypatch.setattr(pb.os, "read", bounded_read)
    row = _wait(consumer_world, key, first)
    assert row["status"] == "executed", row
    assert row["result"]["verified"] is True
    assert row["result"]["stdout"]["truncated"] is True
    assert row["result"]["stdout"]["evidence"]["bytes"] == cap
    assert 0 < maximum.value <= 4 * 1024 * 1024


@pytest.mark.parametrize("change", ["replace", "remove"])
def test_archive_source_is_rechecked_after_streaming(consumer_world, monkeypatch, change):
    from prismabuild import action_result

    key, first, _ = _overwritten_success(consumer_world, monkeypatch)
    source = consumer_world.queue.records.attempt_path(first, 1)
    original = action_result._stream_ending_log

    def stream_then_change(*args, **kwargs):
        result = original(*args, **kwargs)
        if source.exists():
            if change == "remove":
                source.unlink()
            else:
                record = json.loads(source.read_bytes())
                record["detail"]["action_returncode"] = 7
                source.chmod(0o600)
                source.write_bytes(pb._canonical_file_bytes(record))
                source.chmod(0o444)
        return result

    monkeypatch.setattr(action_result, "_stream_ending_log", stream_then_change)
    row = _wait(consumer_world, key, first)
    assert row["status"] == "unreadable"
    assert "selected archived ending changed or is missing" in row["reason"]


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_archive_log_digest_is_verified(consumer_world, monkeypatch, stream):
    key, first, _ = _overwritten_success(consumer_world, monkeypatch)
    selected = json.loads(consumer_world.queue.records.attempt_path(first, 1).read_bytes())
    path = consumer_world.queue.root / selected["logs"][stream]["path"]
    raw = path.read_bytes()
    path.chmod(0o600)
    path.write_bytes(b"!" + raw[1:])
    path.chmod(0o444)
    row = _wait(consumer_world, key, first)
    assert row["status"] == "unreadable"
    assert f"pool attempt {stream} refused" in row["reason"]
