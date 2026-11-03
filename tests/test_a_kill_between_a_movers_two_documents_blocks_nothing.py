"""A kill between a mover's fragment and its sidecar blocks nothing later (#1087).

Both movers file two documents per publication: the map fragment, which
vouches for a staged name, and the material sidecar, which dates the vouch.
Until #1087 the fragment went first.  A kill between the two writes left a
vouch that nothing dates, and the publication gate reads such an entry as
``owned`` -- "published elsewhere, not yet dated" -- for every later mover of
the name, the same key's retry included.  Each of them copied the entry again,
waited out the grace, and refused, and nothing would ever date the vouch.

The sidecar now goes first.  A kill between the two writes leaves a date that
no vouch cites, which is inert: the gate walks fragments only, so the name
reads as positive absence and the #1081 content proof adopts its bytes, and
the strict reader pins only names both documents carry.

Every case drives the real ``stage_move.move`` and ``ram_promote.promote``
over tiny real files and kills the mover with an exception nothing in it
catches, raised between the first and the second document of one
publication whichever of the two the mover writes first.  Then it runs the
same key again, and a second key of the same range, and asserts that both
complete without a copy, a grace or a replacement.  A test-owned clock
advances only at publication polling: no real scheduling delay can spend
its grace.  Every adoption must leave that clock and its wait spy untouched.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import socket
import sys
import threading

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, reader_lease, residency_map  # noqa: E402
from prismabuild import storage_tiers  # noqa: E402
from prismabuild import ram_promote  # noqa: E402
from prismabuild import stage_move  # noqa: E402
from prismabuild import staged_path

STAGE_TIER = "prismabuild-stage:testbox"
RAM_TIER = "ram:testbox"
CONSUMER = "a" * 64
OTHER_CONSUMER = "b" * 64
STAGE_MOVER = "1" * 64
OTHER_STAGE_MOVER = "2" * 64
RAM_MOVER = "e" * 64
OTHER_RAM_MOVER = "d" * 64
N = 3
SIZE = 16 * 1024
TOTAL = N * SIZE


class _Killed(BaseException):
    """A kill: nothing in a mover catches it, and nothing runs after it."""


class _KillBetweenDocuments:
    """Kill one mover after the first document of one publication has landed.

    Both writers are wrapped and counted for one ``(consumer, mover)``, so
    the kill lands between the two documents of the ``publication``-th pair
    whichever the mover writes first.  A killed process writes nothing more,
    so every later write for that mover raises too, until :meth:`revive`.
    ``after_first_pair`` runs once the first pair has landed.
    """

    def __init__(self, monkeypatch, *, consumer: str, mover: str,
                 publication: int, after_first_pair=None) -> None:
        self.kill_at = 2 * publication
        self.landed: list[tuple[str, frozenset[str]]] = []
        self.killed = False
        self.armed = True
        real_fragment = residency_map.write_fragment
        real_material = reader_lease.write_material

        def gate(kind: str) -> None:
            if not self.armed:
                return
            if self.killed or len(self.landed) + 1 == self.kill_at:
                self.killed = True
                raise _Killed(f"killed before this {kind} write")

        def landed(kind: str, entries) -> None:
            if not self.armed:
                return
            self.landed.append((kind, frozenset(entries)))
            if len(self.landed) == 2 and after_first_pair is not None:
                after_first_pair()

        def fragment(root, document, *args, **kwargs):
            ours = (document.get("consumer_action_key") == consumer
                    and document.get("mover_action_key") == mover)
            if ours:
                gate("fragment")
            path = real_fragment(root, document, *args, **kwargs)
            if ours:
                landed("fragment", document["entries"])
            return path

        def material(root, **kwargs):
            ours = (kwargs.get("consumer_action_key") == consumer
                    and kwargs.get("mover_action_key") == mover)
            if ours:
                gate("material")
            path = real_material(root, **kwargs)
            if ours:
                landed("material", kwargs["entries"])
            return path

        monkeypatch.setattr(residency_map, "write_fragment", fragment)
        monkeypatch.setattr(reader_lease, "write_material", material)

    def revive(self) -> None:
        self.armed = False


def _payload(index: int) -> bytes:
    return bytes((position * 11 + index * 17) % 251 + 1
                 for position in range(SIZE))


def _manifest(tmp_path: Path) -> tuple[Path, str, dict]:
    origin = tmp_path / "origin"
    origin.mkdir(parents=True, exist_ok=True)
    entries = []
    for index in range(N):
        path = origin / f"shard-{index}.bin"
        path.write_bytes(_payload(index))
        entries.append({"path": str(path), "offset": 0, "bytes": SIZE,
                        "sha256": hashlib.sha256(_payload(index)).hexdigest()})
    body = {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {"tool": "killed-publication-fixture"},
        "mount_prefix": str(origin),
        "entries": entries,
        "entry_count": N,
        "total_bytes": TOTAL,
        "annotations": {},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(body))
    return path, hashlib.sha256(path.read_bytes()).hexdigest(), body


def _stage_args(tmp_path: Path, queue: pool.PoolQueue, manifest: Path,
                manifest_sha: str, consumer: str, mover: str):
    return stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", mover,
        "--consumer-action-key", consumer,
        "--tier-id", STAGE_TIER,
        "--stage-root", str(tmp_path / "stage"),
        "--manifest-sha256", manifest_sha,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(TOTAL),
        "--manifest", str(manifest),
        "--residency-root", str(queue.root / pool.record_store.RESIDENCY),
        "--block", "4096",
        "--readers", "1",
        "--max-readers", "1",
        "--unpaced",
    ])


def _promote_args(tmp_path: Path, queue: pool.PoolQueue, manifest: Path,
                  manifest_sha: str, mover: str):
    return ram_promote.build_parser().parse_args([
        "--pool-root", str(queue.root),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", mover,
        "--consumer-action-key", CONSUMER,
        "--tier-id", RAM_TIER,
        "--ram-root", str(tmp_path / "ram"),
        "--source-stage-root", str(tmp_path / "stage"),
        "--manifest-sha256", manifest_sha,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(TOTAL),
        "--manifest", str(manifest),
        "--residency-root", str(queue.root / pool.record_store.RESIDENCY),
        "--block", "4096",
        "--readers", "1",
        "--max-readers", "1",
    ])


def _named(root: Path, body: dict) -> list[Path]:
    return [root / staged_path.stage_relative(
                str(entry["path"]), 0, SIZE,
                mount_prefix=str(body["mount_prefix"]))
            for entry in body["entries"]]


def _keys(body: dict) -> list[str]:
    return [residency_map.residency_map_key(str(entry["path"]), 0)
            for entry in body["entries"]]


def _identities(paths: list[Path]) -> list[dict[str, int] | None]:
    return [reader_lease.stat_identity(str(path)) for path in paths]


def _documents(queue: pool.PoolQueue, consumer: str, mover: str
               ) -> tuple[set[str] | None, set[str] | None]:
    """The keys the fragment vouches and the sidecar dates; None if absent."""

    residence = queue.root / pool.record_store.RESIDENCY
    try:
        fragment = json.loads(residency_map.fragment_path(
            residence, consumer, mover).read_bytes())
        vouched: set[str] | None = set(fragment["entries"])
    except FileNotFoundError:
        vouched = None
    material = reader_lease.read_material(residence, consumer, mover)
    dated = set(material["entries"]) if isinstance(material, dict) else None
    return vouched, dated


class _PublicationWait:
    """Own only staged_path's deadline and polling clock, not process time.

    Scheduling and filesystem work cannot advance this clock.  Only the
    publication polling boundary can, and every call is retained even if
    the mover catches an error or eventually succeeds.  Other time APIs
    (including receipt instrumentation) still use their real clocks.
    """

    def __init__(self, monkeypatch) -> None:
        self.calls: list[float] = []
        self.polled = threading.Event()
        self._real_time = staged_path.time
        monkeypatch.setattr(staged_path, "time", self)

    def __getattr__(self, name):
        return getattr(self._real_time, name)

    def monotonic(self) -> float:
        return sum(self.calls)

    def sleep(self, seconds: float) -> None:
        self.calls.append(seconds)
        self.polled.set()

    def assert_no_wait(self) -> None:
        assert self.calls == [], f"publication polling: {self.calls}"
        assert self.monotonic() == 0.0, "virtual publication grace elapsed"
        assert not self.polled.is_set(), "publication polling occurred"


def _finished_without_a_copy(receipt: dict, wait: _PublicationWait,
                             paths: list[Path], before: list) -> None:
    assert receipt["complete"] is True, (
        f"a later publication of the killed mover's names did not finish: "
        f"{receipt.get('errors')}")
    assert receipt["errors"] == [], receipt["errors"]
    timings = receipt["phase_timings"]
    phases = timings["thread_seconds"]
    assert "copy_read" not in phases, (
        f"bytes already equal to the declared digest were copied again: "
        f"{phases}")
    assert "publish_poll_sleep" not in phases, (
        f"a publication waited for a date nothing would write: {phases}")
    outcomes = timings["outcomes"]
    assert set(outcomes) <= {"adopted", "adopted_by_content"}, outcomes
    assert sum(outcomes.values()) == N, outcomes
    assert _identities(paths) == before, "a correct copy was replaced"
    wait.assert_no_wait()


def _whole_pair(queue: pool.PoolQueue, consumer: str, mover: str,
                keys: list[str]) -> None:
    vouched, dated = _documents(queue, consumer, mover)
    assert vouched == set(keys) and dated == set(keys), (vouched, dated)


# --- the stage mover ---------------------------------------------------------

def _killed_stage_move(tmp_path: Path, monkeypatch, *, behind: bool):
    """A stage mover killed between the two documents of its last publication.

    ``behind``: an incremental publication landed first (the first entry,
    both documents), so the kill leaves one document a publication behind
    the other.  Otherwise no incremental publication runs and the kill
    leaves the second document absent.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.records.ensure_layout()
    manifest, manifest_sha, body = _manifest(tmp_path)
    if behind:
        monkeypatch.setattr(stage_move, "FRAGMENT_PUBLISH_S", 0.0)
    else:
        monkeypatch.setattr(stage_move, "FRAGMENT_PUBLISH_S", 1e12)

    def only_the_final_publication_after_this() -> None:
        monkeypatch.setattr(stage_move, "FRAGMENT_PUBLISH_S", 1e12)

    kill = _KillBetweenDocuments(
        monkeypatch, consumer=CONSUMER, mover=STAGE_MOVER,
        publication=2 if behind else 1,
        after_first_pair=only_the_final_publication_after_this)
    args = _stage_args(tmp_path, queue, manifest, manifest_sha, CONSUMER,
                       STAGE_MOVER)
    with pytest.raises(_Killed):
        stage_move.move(args)
    kill.revive()
    assert kill.killed
    # The kill really fell between two documents of one publication.
    vouched, dated = _documents(queue, CONSUMER, STAGE_MOVER)
    assert vouched != dated, (vouched, dated)
    paths = _named(tmp_path / "stage", body)
    for index, path in enumerate(paths):
        assert path.read_bytes() == _payload(index)
    return queue, manifest, manifest_sha, body, paths


@pytest.mark.parametrize("behind", [False, True],
                         ids=["second-document-absent",
                              "second-document-a-publication-behind"])
def test_the_same_key_then_another_key_finish_a_killed_stage_range(
        tmp_path: Path, monkeypatch, behind: bool) -> None:
    queue, manifest, sha, body, paths = _killed_stage_move(
        tmp_path, monkeypatch, behind=behind)
    before = _identities(paths)
    wait = _PublicationWait(monkeypatch)

    retry = stage_move.move(_stage_args(
        tmp_path, queue, manifest, sha, CONSUMER, STAGE_MOVER))
    _finished_without_a_copy(retry, wait, paths, before)
    _whole_pair(queue, CONSUMER, STAGE_MOVER, _keys(body))

    other = stage_move.move(_stage_args(
        tmp_path, queue, manifest, sha, OTHER_CONSUMER, OTHER_STAGE_MOVER))
    _finished_without_a_copy(other, wait, paths, before)
    _whole_pair(queue, OTHER_CONSUMER, OTHER_STAGE_MOVER, _keys(body))


@pytest.mark.parametrize("behind", [False, True],
                         ids=["second-document-absent",
                              "second-document-a-publication-behind"])
def test_another_key_then_the_same_key_finish_a_killed_stage_range(
        tmp_path: Path, monkeypatch, behind: bool) -> None:
    """Another consumer's mover of the same content-addressed names.

    Before the fix the killed mover's undated vouch refused every mover of
    those names, whoever's it was, until something retired its owner.
    """

    queue, manifest, sha, body, paths = _killed_stage_move(
        tmp_path, monkeypatch, behind=behind)
    before = _identities(paths)
    wait = _PublicationWait(monkeypatch)

    other = stage_move.move(_stage_args(
        tmp_path, queue, manifest, sha, OTHER_CONSUMER, OTHER_STAGE_MOVER))
    _finished_without_a_copy(other, wait, paths, before)

    retry = stage_move.move(_stage_args(
        tmp_path, queue, manifest, sha, CONSUMER, STAGE_MOVER))
    _finished_without_a_copy(retry, wait, paths, before)
    _whole_pair(queue, CONSUMER, STAGE_MOVER, _keys(body))


# --- the RAM promotion -------------------------------------------------------

def _killed_promotion(tmp_path: Path, monkeypatch):
    """A promotion killed between the two documents of its one publication."""

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.records.ensure_layout()
    manifest, manifest_sha, body = _manifest(tmp_path)
    staged = stage_move.move(_stage_args(
        tmp_path, queue, manifest, manifest_sha, CONSUMER, STAGE_MOVER))
    assert staged["complete"] is True, staged
    ram = tmp_path / "ram"
    ram.mkdir()
    assert storage_tiers.ensure_ram_epoch(ram, host="testbox") is not None
    kill = _KillBetweenDocuments(monkeypatch, consumer=CONSUMER,
                                 mover=RAM_MOVER, publication=1)
    with pytest.raises(_Killed):
        ram_promote.promote(_promote_args(tmp_path, queue, manifest,
                                          manifest_sha, RAM_MOVER))
    kill.revive()
    assert kill.killed
    vouched, dated = _documents(queue, CONSUMER, RAM_MOVER)
    assert vouched != dated, (vouched, dated)
    paths = _named(ram, body)
    for index, path in enumerate(paths):
        assert path.read_bytes() == _payload(index)
    return queue, manifest, manifest_sha, body, paths


def test_the_same_key_then_another_key_finish_a_killed_promotion(
        tmp_path: Path, monkeypatch) -> None:
    queue, manifest, sha, body, paths = _killed_promotion(
        tmp_path, monkeypatch)
    before = _identities(paths)
    wait = _PublicationWait(monkeypatch)

    retry = ram_promote.promote(_promote_args(
        tmp_path, queue, manifest, sha, RAM_MOVER))
    _finished_without_a_copy(retry, wait, paths, before)
    _whole_pair(queue, CONSUMER, RAM_MOVER, _keys(body))

    other = ram_promote.promote(_promote_args(
        tmp_path, queue, manifest, sha, OTHER_RAM_MOVER))
    _finished_without_a_copy(other, wait, paths, before)
    _whole_pair(queue, CONSUMER, OTHER_RAM_MOVER, _keys(body))


def test_another_key_then_the_same_key_finish_a_killed_promotion(
        tmp_path: Path, monkeypatch) -> None:
    queue, manifest, sha, body, paths = _killed_promotion(
        tmp_path, monkeypatch)
    before = _identities(paths)
    wait = _PublicationWait(monkeypatch)

    other = ram_promote.promote(_promote_args(
        tmp_path, queue, manifest, sha, OTHER_RAM_MOVER))
    _finished_without_a_copy(other, wait, paths, before)

    retry = ram_promote.promote(_promote_args(
        tmp_path, queue, manifest, sha, RAM_MOVER))
    _finished_without_a_copy(retry, wait, paths, before)
    _whole_pair(queue, CONSUMER, RAM_MOVER, _keys(body))


# --- controls for the publication-wait observation ---------------------------

def test_the_publication_clock_advances_only_at_the_poll_boundary(
        monkeypatch) -> None:
    assert "_PublicationWait" in globals(), "publication wait is not observed"
    wait = _PublicationWait(monkeypatch)
    started = staged_path.time.monotonic()
    # Merely observing the clock, however often the mover is descheduled,
    # cannot use up any publication grace.
    for _ in range(10):
        assert staged_path.time.monotonic() == started
    wait.assert_no_wait()
    assert not wait.polled.is_set()

    staged_path.time.sleep(staged_path._PUBLISH_POLL_S)
    assert wait.polled.is_set()
    assert wait.calls == [staged_path._PUBLISH_POLL_S]
    assert staged_path.time.monotonic() - started == staged_path._PUBLISH_POLL_S
    with pytest.raises(AssertionError, match="publication polling"):
        wait.assert_no_wait()


def test_a_missing_sidecar_trips_the_publication_wait_check(
        tmp_path: Path, monkeypatch) -> None:
    """The old fragment-first residue really waits, on the virtual clock."""
    assert "_PublicationWait" in globals(), "publication wait is not observed"
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.records.ensure_layout()
    manifest, sha, body = _manifest(tmp_path)
    staged = stage_move.move(_stage_args(
        tmp_path, queue, manifest, sha, CONSUMER, STAGE_MOVER))
    assert staged["complete"] is True, staged
    residence = queue.root / pool.record_store.RESIDENCY
    reader_lease.material_path(residence, CONSUMER, STAGE_MOVER).unlink()
    vouched, dated = _documents(queue, CONSUMER, STAGE_MOVER)
    assert vouched == set(_keys(body)) and dated is None, (vouched, dated)
    paths = _named(tmp_path / "stage", body)
    before = _identities(paths)
    publisher = staged_path.StagedPublisher(
        queue=queue, stage_root=tmp_path / "stage", residency_root=residence,
        mover_action_key=OTHER_STAGE_MOVER, manifest_sha256=sha,
        tier_id=STAGE_TIER, cas_root=tmp_path / "cas",
        consumer_action_key=OTHER_CONSUMER)
    temporary = tmp_path / "private-copy"
    temporary.write_bytes(_payload(0))
    wait = _PublicationWait(monkeypatch)
    with pytest.raises(staged_path.PublicationRefused):
        publisher.publish(body["entries"][0], paths[0], temporary,
                          body["entries"][0]["sha256"])
    assert wait.polled.is_set()
    assert wait.calls
    assert wait.monotonic() == sum(wait.calls)
    assert wait.monotonic() >= staged_path._PUBLISH_GRACE_S
    with pytest.raises(AssertionError, match="publication polling"):
        wait.assert_no_wait()
    assert _identities(paths) == before


# --- the strict reader over the state the kill now leaves --------------------

def test_a_date_no_vouch_cites_is_inert_to_the_strict_reader(
        tmp_path: Path) -> None:
    """A sidecar one publication ahead of its fragment refuses no window.

    This is what the kill leaves now, and what every in-flight publication
    shows between its two writes.  The reader pins only a name both
    documents carry: the vouched and dated prefix acquires, the dated but
    unvouched name is a coverage gap rather than ``ownership-uncertain``,
    and nothing unvouched is ever pinned.
    """

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.records.ensure_layout()
    manifest, manifest_sha, body = _manifest(tmp_path)
    staged = stage_move.move(_stage_args(
        tmp_path, queue, manifest, manifest_sha, CONSUMER, STAGE_MOVER))
    assert staged["complete"] is True, staged
    residence = queue.root / pool.record_store.RESIDENCY
    keys = _keys(body)
    path = residency_map.fragment_path(residence, CONSUMER, STAGE_MOVER)
    fragment = json.loads(path.read_bytes())
    fragment["entries"] = {keys[0]: fragment["entries"][keys[0]]}
    residency_map.write_fragment(residence, fragment)
    reader_lease.clear_cover_docs_cache()

    found = reader_lease.covers_for_keys(
        residence, CONSUMER, [keys[0]], tier_id=STAGE_TIER,
        manifest_sha256=manifest_sha, epoch="", context={})
    assert found.get("ok"), found
    assert found["covers"] == [{"mover_action_key": STAGE_MOVER,
                                "manifest_sha256": manifest_sha}]
    # Exactly what the old order showed a reader between its two writes, a
    # vouch the sidecar did not date yet: a gap beside covered names, and
    # nothing published when it is all the reader asks for.
    gap = reader_lease.covers_for_keys(
        residence, CONSUMER, [keys[0], keys[1]], tier_id=STAGE_TIER,
        manifest_sha256=manifest_sha, epoch="", context={})
    assert gap == {"ok": False, "refusal": "source-coverage-gap"}, gap
    alone = reader_lease.covers_for_keys(
        residence, CONSUMER, [keys[1]], tier_id=STAGE_TIER,
        manifest_sha256=manifest_sha, epoch="", context={})
    assert alone == {"ok": False, "refusal": "unpublished"}, alone
    whole = reader_lease.resolve_window_covers(
        queue, consumer_action_key=CONSUMER, tier_id=STAGE_TIER, epoch="",
        keys=None, manifest_sha256=manifest_sha, residency_root=residence)
    assert whole.get("ok"), whole
    assert sorted(whole["expected"]) == [keys[0]], whole

    def acquire(expected, token: str) -> dict:
        return reader_lease.acquire(
            queue, consumer_action_key=CONSUMER,
            attempt={"nonce": "f" * 32, "scope_id": "fixture-scope"},
            tier_id=STAGE_TIER, epoch="",
            span={"start_bytes": 0, "end_bytes": SIZE},
            holder={"host": socket.gethostname(), "pid": os.getpid()},
            acquire_token=token,
            covers=[{"mover_action_key": STAGE_MOVER,
                     "manifest_sha256": manifest_sha}],
            expected=expected, residency_root=residence, file_pin=False)

    window = acquire({keys[0]: {"bytes": SIZE,
                                "sha256": body["entries"][0]["sha256"]}},
                     "fixture:prefix")
    assert window.get("ok"), window
    assert [entry["key"] for entry in window["entries"]] == [keys[0]]
    everything = acquire(None, "fixture:all")
    assert everything.get("ok"), everything
    assert [entry["key"] for entry in everything["entries"]] == [keys[0]], (
        "a name no fragment vouches was pinned")
    unvouched = acquire({keys[1]: {"bytes": SIZE,
                                   "sha256": body["entries"][1]["sha256"]}},
                        "fixture:unvouched")
    assert unvouched == {"ok": False, "refusal": "source-coverage-gap"}, (
        unvouched)
