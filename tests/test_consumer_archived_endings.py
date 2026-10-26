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
