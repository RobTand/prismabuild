"""A failed release names the step that failed and its errno (#1023).

R13 (action ``556d7a8030989e68``, GLM-5.3 Stage A on sparklina) died at
20:31:39Z on 2026-09-23 when one ``reader_lease.release`` returned ``False``
after hundreds of clean ones.  The pin it stranded read and validated from
another host afterwards, and nothing said which of release's steps had
failed or why: every failure path answered a bare ``False``, the
``OSError``'s errno went with it, and PB filed nothing.

Each fixture here injects one ``OSError`` into one step of a real release
against a real queue:

* the census of the leases root, which runs when the named consumer's own
  directory does not hold the pin;
* the first pin read, which runs outside the lock only to learn the stage
  root;
* the stage root's ownership lock;
* the re-read under that lock;
* the rewrite that keeps the other holders' refs;
* the unlink of a pin whose last ref this was (R13's branch).

A failure answers which step and which errno, files a
``reader-release-failed`` event in the consumer's tier-event directory
(#1002), and leaves the ref held.  A transient errno is retried inside the
call, and a retry after any failure drops the ref exactly once.  A caller
that names the stage root skips the unlocked read; the locked read stays
authoritative.
"""
from __future__ import annotations

import builtins
import contextlib
import errno
import json
import os
from pathlib import Path
import socket
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import pool, reader_lease, residency_map  # noqa: E402
import stage_release  # noqa: E402

CONSUMER = "c" * 64
#: A consumer whose directory does not hold the pin: its release falls back
#: to the census of the whole leases root.
ELSEWHERE = "d" * 64
MOVER = "e" * 64
TIER = "prismabuild-stage:dl380g10"
ATTEMPT = {"nonce": "n1", "scope_id": "s1"}
SOURCE = "/mnt/shared/model/w.safetensors"

#: The steps an ``OSError`` can be injected into.
STEPS = ("census", "read", "lock", "locked-read", "write", "unlink")


@pytest.fixture(autouse=True)
def _no_waiting(monkeypatch):
    """The retry schedule's shape, without its sleeps."""

    monkeypatch.setattr(reader_lease, "RELEASE_RETRY_DELAYS_S",
                        (0.0, 0.0, 0.0), raising=False)


@pytest.fixture()
def world(tmp_path: Path):
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.ensure_layout()
    stage = tmp_path / "stage"
    stage.mkdir()
    stage_release.register_stage_root(queue, tier_id=TIER, stage_root=stage)
    staged = stage / "model" / "w.safetensors"
    staged.parent.mkdir(parents=True)
    staged.write_bytes(b"\x01" * 4096)
    root = queue.root / pool.RESIDENCY
    residency_map.write_fragment(root, {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": CONSUMER, "mover_action_key": MOVER,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {residency_map.residency_map_key(SOURCE, 0): {
            "stage_path": str(staged), "bytes": 4096,
            "sha256": "b" * 64, "offset": 0}},
    })
    identity = reader_lease.stat_identity(str(staged))
    assert identity is not None
    reader_lease.write_material(
        root, consumer_action_key=CONSUMER, mover_action_key=MOVER,
        tier_id=TIER, stage_root=str(stage), manifest_sha256="a" * 64,
        generation=reader_lease.mint_generation(),
        entries={residency_map.residency_map_key(SOURCE, 0): {
            "stage_path": str(staged), "bytes": 4096, "sha256": "b" * 64,
            "file_id": identity}})
    return SimpleNamespace(queue=queue, stage=stage)


def _acquire(world, token: str, pid: int) -> dict:
    acquired = reader_lease.acquire(
        world.queue, consumer_action_key=CONSUMER, attempt=ATTEMPT,
        tier_id=TIER, epoch="", span={"start_bytes": 0, "end_bytes": 4096},
        holder={"host": "test-host", "pid": pid}, acquire_token=token,
        covers=[{"mover_action_key": MOVER, "manifest_sha256": "a" * 64}])
    assert acquired["ok"], acquired
    return acquired


def _hold(world, step: str) -> tuple[dict, dict | None, Path]:
    """This reader's ref, plus a sibling's unless the step is the unlink."""

    mine = _acquire(world, "mine", 1001)
    sibling = None if step == "unlink" else _acquire(world, "sibling", 1002)
    if sibling is not None:
        assert sibling["pin_id"] == mine["pin_id"]
    path = (reader_lease.leases_root(world.queue) / CONSUMER
            / f"{mine['pin_id']}.lease.json")
    assert path.is_file()
    return mine, sibling, path


def _refs(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return set(json.loads(path.read_text())["refs"])


def _failures(queue) -> list[dict]:
    return [event for event in queue.consumer_events(CONSUMER)
            + queue.consumer_events(ELSEWHERE)
            if event.get("event") == "reader-release-failed"]


def _inject(monkeypatch, world, path: Path, step: str, code: int, *,
            times: int = 1) -> dict:
    """Fail ``step`` with ``OSError(code)`` ``times`` times; release kwargs."""

    left = {"n": times}

    def due() -> bool:
        if left["n"] <= 0:
            return False
        left["n"] -= 1
        return True

    fault = OSError(code, os.strerror(code))
    kwargs: dict = {"consumer_action_key": CONSUMER}
    if step == "census":
        kwargs["consumer_action_key"] = ELSEWHERE
        root = reader_lease.leases_root(world.queue)
        real_scandir = os.scandir

        def scandir(target="."):
            if Path(target) == root and due():
                raise fault
            return real_scandir(target)

        monkeypatch.setattr(reader_lease.os, "scandir", scandir)
    elif step in {"read", "locked-read"}:
        # Without a named stage root a release opens the pin twice: once to
        # learn the stage root, once under that root's lock.
        opens = {"n": 0}
        first = 1 if step == "read" else 2

        def fake_open(file, mode="r", *args, **kwargs):
            if (isinstance(file, (str, os.PathLike)) and Path(file) == path
                    and "r" in mode):
                opens["n"] += 1
                if (opens["n"] - first) % 2 == 0 and opens["n"] >= first \
                        and due():
                    raise fault
            return builtins.open(file, mode, *args, **kwargs)

        monkeypatch.setattr(reader_lease, "open", fake_open, raising=False)
    elif step == "lock":
        real_lock = world.queue.stage_ownership_lock

        @contextlib.contextmanager
        def refusing():
            raise fault
            yield  # pragma: no cover

        def stage_ownership_lock(stage_root, **kwargs):
            if due():
                return refusing()
            return real_lock(stage_root, **kwargs)

        monkeypatch.setattr(world.queue, "stage_ownership_lock",
                            stage_ownership_lock)
    elif step == "write":
        def fake_open(file, mode="r", *args, **kwargs):
            if (isinstance(file, (str, os.PathLike))
                    and Path(file).parent == path.parent
                    and Path(file).name.startswith(f".{path.name}.")
                    and "w" in mode and due()):
                raise fault
            return builtins.open(file, mode, *args, **kwargs)

        monkeypatch.setattr(reader_lease, "open", fake_open, raising=False)
    elif step == "unlink":
        real_unlink = Path.unlink

        def unlink(self, missing_ok=False):
            if self == path and due():
                raise fault
            return real_unlink(self, missing_ok=missing_ok)

        monkeypatch.setattr(Path, "unlink", unlink)
    else:  # pragma: no cover
        raise AssertionError(step)
    return kwargs


@pytest.mark.parametrize("step", STEPS)
def test_a_failed_release_names_its_step_and_errno(world, monkeypatch,
                                                    step) -> None:
    """A structural errno: one attempt, the step and errno, one event."""

    mine, sibling, path = _hold(world, step)
    held = _refs(path)
    kwargs = _inject(monkeypatch, world, path, step, errno.EACCES)

    answer = reader_lease.release(world.queue, mine["pin_id"],
                                  mine["ref_id"], **kwargs)

    assert not answer, f"{step}: a failed release is falsy"
    assert answer is not False, "never a bare False"
    assert isinstance(answer, reader_lease.ReleaseFailure)
    assert answer.step == step and step in reader_lease.RELEASE_STEPS
    assert answer.errno == errno.EACCES
    assert answer.retryable is False
    assert answer.attempts == 1
    assert answer.pin_id == mine["pin_id"] and answer.ref_id == mine["ref_id"]
    assert _refs(path) == held, "a failed release keeps the ref"

    events = _failures(world.queue)
    assert len(events) == 1, events
    event = events[0]
    assert event["consumer"] == kwargs["consumer_action_key"]
    assert event["pin_id"] == mine["pin_id"]
    assert event["ref_id"] == mine["ref_id"]
    assert event["step"] == step
    assert event["errno"] == errno.EACCES
    assert event["errno_name"] == "EACCES"
    assert event["retryable"] is False


@pytest.mark.parametrize("step", STEPS)
def test_a_retry_after_each_failure_releases_exactly_once(world, monkeypatch,
                                                          step) -> None:
    """The failed call leaves the ref; the retry drops it and nothing else."""

    mine, sibling, path = _hold(world, step)
    kwargs = _inject(monkeypatch, world, path, step, errno.EACCES)

    assert not reader_lease.release(world.queue, mine["pin_id"],
                                    mine["ref_id"], **kwargs)
    assert mine["ref_id"] in _refs(path)

    assert reader_lease.release(world.queue, mine["pin_id"], mine["ref_id"],
                                **kwargs) is True
    remaining = _refs(path)
    assert mine["ref_id"] not in remaining
    if sibling is None:
        assert not path.exists(), "the last ref unlinks its pin"
    else:
        assert remaining == {sibling["ref_id"]}
        before = path.read_bytes()

    assert reader_lease.release(world.queue, mine["pin_id"], mine["ref_id"],
                                **kwargs) is True, "already gone is released"
    if sibling is None:
        assert not path.exists()
    else:
        assert path.read_bytes() == before, "a second release touches nothing"
    assert len(_failures(world.queue)) == 1


@pytest.mark.parametrize("step", STEPS)
def test_a_transient_errno_is_retried_inside_the_call(world, monkeypatch,
                                                       step) -> None:
    """One ESTALE at any step: the same call releases, exactly once."""

    mine, sibling, path = _hold(world, step)
    kwargs = _inject(monkeypatch, world, path, step, errno.ESTALE)

    assert reader_lease.release(world.queue, mine["pin_id"], mine["ref_id"],
                                **kwargs) is True
    if sibling is None:
        assert not path.exists()
    else:
        assert _refs(path) == {sibling["ref_id"]}
    assert _failures(world.queue) == [], "a release that happened is no failure"


@pytest.mark.parametrize("code", [errno.ESTALE, errno.EIO, errno.ETIMEDOUT,
                                  errno.EAGAIN])
def test_a_fault_that_outlasts_the_retries_is_reported_retryable(
        world, monkeypatch, code) -> None:
    """The bound holds; the answer says a later retry may still succeed."""

    mine, sibling, path = _hold(world, "unlink")
    kwargs = _inject(monkeypatch, world, path, "unlink", code, times=100)

    answer = reader_lease.release(world.queue, mine["pin_id"],
                                  mine["ref_id"], **kwargs)

    attempts = len(reader_lease.RELEASE_RETRY_DELAYS_S) + 1
    assert not answer
    assert answer.step == "unlink" and answer.errno == code
    assert answer.retryable is True
    assert answer.attempts == attempts
    assert [tuple(row) for row in answer.trail] == [("unlink", code)] * attempts
    assert path.exists() and mine["ref_id"] in _refs(path)
    events = _failures(world.queue)
    assert len(events) == 1, "one event per failed release, not per attempt"
    assert events[0]["attempts"] == attempts


def test_a_named_stage_root_skips_the_unlocked_read(world, monkeypatch
                                                    ) -> None:
    """The caller's stage root replaces the first read, not the locked one."""

    mine, sibling, path = _hold(world, "write")
    opened: list[str] = []

    def counting_open(file, mode="r", *args, **kwargs):
        if (isinstance(file, (str, os.PathLike)) and Path(file) == path
                and "r" in mode):
            opened.append(mode)
        return builtins.open(file, mode, *args, **kwargs)

    monkeypatch.setattr(reader_lease, "open", counting_open, raising=False)
    assert reader_lease.release(
        world.queue, mine["pin_id"], mine["ref_id"],
        consumer_action_key=CONSUMER, stage_root=str(world.stage)) is True
    assert len(opened) == 1, "one read, under the lock"
    assert _refs(path) == {sibling["ref_id"]}

    opened.clear()
    assert reader_lease.release(
        world.queue, sibling["pin_id"], sibling["ref_id"],
        consumer_action_key=CONSUMER) is True
    assert len(opened) == 2, "without it: a read for the root, then the lock"
    assert not path.exists()


def test_a_named_stage_root_fails_as_the_locked_read(world, monkeypatch
                                                     ) -> None:
    """With the stage root named, the first open of the pin is the locked one."""

    mine, sibling, path = _hold(world, "write")
    kwargs = _inject(monkeypatch, world, path, "read", errno.EACCES)
    answer = reader_lease.release(world.queue, mine["pin_id"],
                                  mine["ref_id"], stage_root=str(world.stage),
                                  **kwargs)
    assert not answer
    assert answer.step == "locked-read" and answer.errno == errno.EACCES


def test_a_wrong_stage_root_yields_to_the_locked_read(world, monkeypatch,
                                                      tmp_path) -> None:
    """A stage root the pin does not name: the mutation takes the pin's lock."""

    mine, sibling, path = _hold(world, "write")
    wrong = tmp_path / "another-stage"
    wrong.mkdir()
    real_lock = world.queue.stage_ownership_lock
    taken: list[str] = []
    depth = {"now": 0, "max": 0}

    @contextlib.contextmanager
    def watched(stage_root, **kwargs):
        with real_lock(stage_root, **kwargs) as got:
            depth["now"] += 1
            depth["max"] = max(depth["max"], depth["now"])
            taken.append(os.path.normpath(str(stage_root)))
            try:
                yield got
            finally:
                depth["now"] -= 1

    monkeypatch.setattr(world.queue, "stage_ownership_lock", watched)
    assert reader_lease.release(
        world.queue, mine["pin_id"], mine["ref_id"],
        consumer_action_key=CONSUMER, stage_root=str(wrong)) is True
    assert taken == [str(wrong), str(world.stage)]
    assert depth["max"] == 1, "one stage root's lock at a time (#780)"
    assert _refs(path) == {sibling["ref_id"]}


def test_a_census_failure_without_a_consumer_is_still_named(world,
                                                            monkeypatch,
                                                            capsys) -> None:
    """No consumer to file under: the step and errno still come back."""

    mine, sibling, path = _hold(world, "write")
    _inject(monkeypatch, world, path, "census", errno.EACCES)
    answer = reader_lease.release(world.queue, mine["pin_id"],
                                  mine["ref_id"])
    assert not answer
    assert answer.step == "census" and answer.errno == errno.EACCES
    assert answer.consumer_action_key is None
    assert "reader-release-failed" in capsys.readouterr().err
    assert mine["ref_id"] in _refs(path)


def test_the_failure_file_keeps_its_newest_lines(world, monkeypatch) -> None:
    """Bounded like the tier loop's file: rewritten to its newest lines."""

    monkeypatch.setattr(pool, "MAX_CONSUMER_EVENT_LINES", 2)
    mine, sibling, path = _hold(world, "unlink")
    kwargs = _inject(monkeypatch, world, path, "unlink", errno.EACCES,
                     times=5)
    for _ in range(5):
        assert not reader_lease.release(world.queue, mine["pin_id"],
                                        mine["ref_id"], **kwargs)
    files = sorted(world.queue.consumer_events_dir(CONSUMER).iterdir())
    assert [item.name for item in files] == [
        f"{socket.gethostname()}{reader_lease.RELEASE_EVENTS_SUFFIX}"]
    lines = files[0].read_text().splitlines()
    assert len(lines) == 3, "four lines rotate to the newest two, then one more"
    assert [json.loads(line)["step"] for line in lines] == ["unlink"] * 3
