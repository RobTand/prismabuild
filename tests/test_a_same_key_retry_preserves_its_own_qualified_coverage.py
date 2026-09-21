"""A same-key retry keeps the coverage its own attempt already proved.

A mover's action key is a content hash, so a retried mover -- a timeout, a
requeue -- runs under the **same** consumer and mover identity.  Everything
it files is keyed by that identity: one fragment, one material sidecar,
replaced wholesale by every publication.  A copier that starts every
dictionary empty therefore publishes, on its first incremental snapshot, a
fragment holding only the entries it has re-encountered *so far* -- and the
qualified suffix its previous attempt staged loses proof, is recopied, and
pays the publication grace per entry.  Head ``5c46f93b…`` did exactly this
after its 3600 s deadline: 2,022 entries of proof fell to 1 and grew back
at seconds per file.

``_resume_own_coverage`` is the narrow resume: the mover's own prior
fragment and sidecar are read back under the stage ownership lock, every
prior entry this window derives keeps its vouch, and an entry keeps its
*dation* only where the existing proof standard still holds -- declared
digest agreement and a sidecar ``file_id`` that matches the live file.  No
payload is rehashed; nothing unprovable is dated; a changed entry meets its
own preserved vouch at the publication gate and is refused, never replaced.

Every case here drives the real tool over tiny real files with the same
action and consumer identity: an interrupted partial first attempt (its
third source is truncated, so the copy fails the way a killed worker
leaves a prefix), then the retry whose first publication would be smaller
than the previous prefix.  The grace is shortened for the retry only --
the assertions are about which proof survives, never about the duration.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
import prismabuild.pool as pool  # noqa: E402
import prismabuild.reader_lease as reader_lease  # noqa: E402
import prismabuild.residency_map as residency_map  # noqa: E402
import stage_move  # noqa: E402

TIER = "prismabuild-stage:dl380g10"
CONSUMER = "c" * 64
MOVER = "a" * 64
MANIFEST_SHA = "9" * 64
SIZES = [4096, 8192, 2048]
PAYLOADS = [b"a", b"b", b"c"]
TOTAL = sum(SIZES)


def _sources(tmp_path: Path) -> tuple[Path, list[dict[str, object]],
                                      list[str], list[str]]:
    """Three tiny real sources, their manifest entries, digests and keys."""

    mount = tmp_path / "mnt"
    names = ["shard-1.bin", "sub/shard-2.bin", "shard-3.bin"]
    entries: list[dict[str, object]] = []
    for name, size, payload in zip(names, SIZES, PAYLOADS):
        path = mount / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload * size)
        entries.append({"path": str(path), "offset": 0, "bytes": size,
                        "sha256": hashlib.sha256(payload * size).hexdigest()})
    keys = [residency_map.residency_map_key(str(entry["path"]), 0)
            for entry in entries]
    return mount, entries, keys, names


def _args(tmp_path: Path, manifest: dict[str, object]):
    """The tool's own parsed arguments, one reader, so order is FIFO."""

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    return stage_move.build_parser().parse_args([
        "--pool-root", str(tmp_path / "queue"),
        "--cas-root", str(tmp_path / "cas"),
        "--action-key", MOVER,
        "--consumer-action-key", CONSUMER,
        "--tier-id", TIER,
        "--stage-root", str(tmp_path / "stage"),
        "--manifest-sha256", MANIFEST_SHA,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(TOTAL),
        "--manifest", str(manifest_path),
        "--residency-root", str(tmp_path / "queue" / pool.RESIDENCY),
        "--block", str(1 << 16),
        "--readers", "1",
        "--max-readers", "1",
        "--unpaced",
    ])


def _manifest(mount: Path, entries: list[dict[str, object]]) -> dict[str, object]:
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {"tool": "test"},
        "mount_prefix": str(mount),
        "entries": entries,
        "entry_count": len(entries),
        "total_bytes": TOTAL,
        "annotations": {},
    }


@pytest.fixture()
def fleet(tmp_path: Path):
    mount, entries, keys, names = _sources(tmp_path)
    args = _args(tmp_path, _manifest(mount, entries))
    return tmp_path, args, mount, entries, keys, names


def _interrupted_first_attempt(fleet) -> dict[str, object]:
    """A real partial attempt: the third source truncates under the copy."""

    tmp_path, args, mount, _entries, keys, _names = fleet
    (mount / "shard-3.bin").write_bytes(b"c" * (SIZES[2] // 2))
    first = stage_move.move(args)
    assert first["complete"] is False
    assert first["entries_staged"] == 2
    assert set(fragment_keys(args)) == set(keys[:2])
    # The truncated source is whole again: the retry has real work to do.
    (mount / "shard-3.bin").write_bytes(b"c" * SIZES[2])
    return first


def fragment_keys(args) -> list[str]:
    fragment = json.loads(residency_map.fragment_path(
        Path(args.residency_root), CONSUMER, MOVER).read_bytes())
    return list(fragment["entries"])


class _Recorder:
    """Captures every publication document a run writes, then writes it."""

    def __init__(self, monkeypatch, module, name: str) -> None:
        self.documents: list[dict[str, object]] = []
        real = getattr(module, name)

        def wrapper(path, fragment, *others, **kwargs):
            self.documents.append(json.loads(json.dumps(fragment)))
            return real(path, fragment, *others, **kwargs)

        monkeypatch.setattr(module, name, wrapper)


def _count_source_opens(monkeypatch, mount: Path) -> list[str]:
    """Which source payloads a run actually opens (adoption opens none)."""

    opened: list[str] = []
    real_open = os.open

    def counting(path, flags, *args, **kwargs):
        if isinstance(path, (str, Path)) and str(path).startswith(str(mount)):
            opened.append(str(path))
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", counting)
    return opened


def _material(args) -> dict[str, object]:
    got = reader_lease.read_material(
        Path(args.residency_root), CONSUMER, MOVER)
    assert isinstance(got, dict), f"material unreadable: {got}"
    return got


# --- the resume itself -------------------------------------------------------

def test_a_partial_retry_never_publishes_below_its_own_qualified_prefix(
        fleet, monkeypatch) -> None:
    """The retry's first publication is never smaller than what it owns.

    Before the resume, the first incremental snapshot replaced the fragment
    with the single entry that had re-landed, dropping the proof for every
    not-yet-reencountered suffix file: the RED below is exactly that first
    document.
    """

    _tmp, args, _mount, _entries, keys, _names = fleet
    _interrupted_first_attempt(fleet)
    documents = _Recorder(monkeypatch, residency_map, "write_fragment")
    opened = _count_source_opens(monkeypatch, fleet[2])
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", 0.2)
    suffix = Path(args.stage_root) / "sub" / "shard-2.bin"
    inode_before = suffix.stat().st_ino

    second = stage_move.move(args)

    assert second["complete"] is True and second["errors"] == []
    assert second["entries_staged"] == 3
    assert second["bytes_staged"] == TOTAL
    assert second["entries_resumed"] == 2, "the prior prefix was re-verified"
    assert documents.documents, "the retry published"
    assert all(keys[1] in document["entries"]
               for document in documents.documents), (
        "a retry publication dropped its own prior qualified coverage")
    # The unchanged suffix adopted: no payload read, no replacement, no
    # rehash -- the recorded digest came back with the proof.
    assert opened == [str(fleet[2] / "shard-3.bin")], (
        "an unchanged qualified file was copied again")
    assert suffix.stat().st_ino == inode_before, (
        "an unchanged qualified file was replaced again")


def test_a_repeated_partial_retry_keeps_its_coverage(fleet, monkeypatch) -> None:
    """Interrupting the retry, then finishing: coverage only ever grows."""

    _tmp, args, mount, _entries, keys, _names = fleet
    _interrupted_first_attempt(fleet)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", 0.2)

    (mount / "shard-3.bin").write_bytes(b"c" * (SIZES[2] // 2))
    documents = _Recorder(monkeypatch, residency_map, "write_fragment")
    second = stage_move.move(args)
    assert second["complete"] is False
    assert second["entries_resumed"] == 2
    assert documents.documents
    assert all(keys[1] in document["entries"]
               for document in documents.documents)

    (mount / "shard-3.bin").write_bytes(b"c" * SIZES[2])
    documents.documents.clear()
    third = stage_move.move(args)
    assert third["complete"] is True and third["errors"] == []
    assert third["entries_staged"] == 3
    assert third["bytes_staged"] == TOTAL
    assert third["entries_resumed"] == 2
    assert all(keys[1] in document["entries"]
               for document in documents.documents)
    final = fragment_keys(args)
    assert set(final) == set(keys)


# --- fail closed -------------------------------------------------------------

def test_changed_prior_bytes_fail_closed_and_are_never_replaced(
        fleet, monkeypatch) -> None:
    """A file tampered in place since the vouch is refused, not healed.

    Dropping the stale vouch instead of preserving it would turn the
    gate's refusal into a grace-then-heal replacement of bytes another
    publication once named -- the exact overwrite this path exists to
    refuse.
    """

    _tmp, args, mount, _entries, keys, _names = fleet
    _interrupted_first_attempt(fleet)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", 0.2)
    tampered_payload = b"b" * SIZES[1] + b"X"
    (Path(args.stage_root) / "sub" / "shard-2.bin").write_bytes(
        tampered_payload)

    second = stage_move.move(args)

    assert second["complete"] is False
    assert any("shard-2" in str(error) for error in second["errors"])
    assert (Path(args.stage_root) / "sub" / "shard-2.bin").read_bytes() \
        == tampered_payload, "changed bytes were replaced"
    # And the changed entry was never re-dated: no fabricated proof.
    assert keys[1] not in _material(args)["entries"]


def test_a_corrupt_prior_sidecar_preserves_vouches_but_dates_nothing(
        fleet, monkeypatch) -> None:
    """An unreadable date is preserved as exactly no date, never invented."""

    _tmp, args, _mount, _entries, keys, _names = fleet
    _interrupted_first_attempt(fleet)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", 0.2)
    reader_lease.material_path(
        Path(args.residency_root), CONSUMER, MOVER).write_text("not json")
    documents = _Recorder(monkeypatch, residency_map, "write_fragment")

    second = stage_move.move(args)

    assert second["complete"] is False
    assert second["errors"], "undatable vouches must refuse, not adopt"
    assert documents.documents
    assert all(keys[1] in document["entries"]
               for document in documents.documents), (
        "vouches were dropped rather than left to refuse fail-closed")
    # Whatever landed wrote a fresh, honest sidecar; the entries whose
    # dates could not be read back were never dated by this run.
    dated = _material(args)["entries"]
    assert keys[0] not in dated and keys[1] not in dated, (
        "an unreadable prior date was carried or invented")


# --- unknown or contradictory ownership refuses the invocation ----------


def _corrupt(fragment_or_sidecar: Path, payload: bytes) -> None:
    """Leave real corrupt bytes where authoritative metadata was."""

    fragment_or_sidecar.write_bytes(payload)


def test_a_corrupt_own_fragment_with_a_new_destination_refuses_everything(
        fleet, monkeypatch) -> None:
    """Unknown ownership is refused, not overwritten into absence.

    The causal shape from review: the prior fragment is corrupt, old
    destinations still hold bytes, and one declared destination never
    landed.  Without the refusal the absent destination replaces cleanly
    and its publication erases the tainted fragment -- converting unknown
    ownership into apparent absence.
    """

    _tmp, args, _mount, _entries, _keys, _names = fleet
    _interrupted_first_attempt(fleet)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", 0.2)
    fragment = residency_map.fragment_path(
        Path(args.residency_root), CONSUMER, MOVER)
    corrupt_bytes = b'{"schema": "prismaquant.prismabuild.resi'
    _corrupt(fragment, corrupt_bytes)
    stage = Path(args.stage_root)
    landed = {name: (stage / name).read_bytes()
              for name in ("shard-1.bin", "sub/shard-2.bin")}

    with pytest.raises(SystemExit, match="residency_prior_fragment_unreadable"):
        stage_move.move(args)

    # Nothing was copied, nothing was published, nothing was altered.
    assert fragment.read_bytes() == corrupt_bytes, (
        "the tainted ownership record was overwritten")
    assert not (stage / "shard-3.bin").exists(), (
        "the refusal did not precede the copy")
    for name, payload in landed.items():
        assert (stage / name).read_bytes() == payload
    # And a subsequent retry meets the same refusal, not an erased state.
    with pytest.raises(SystemExit, match="residency_prior_fragment_unreadable"):
        stage_move.move(args)
    assert fragment.read_bytes() == corrupt_bytes


def test_a_conflicting_own_fragment_refuses_and_keeps_the_record(
        fleet, monkeypatch) -> None:
    """A readable fragment with foreign headers is contradiction, not
    coverage to republish around."""

    _tmp, args, _mount, _entries, _keys, _names = fleet
    _interrupted_first_attempt(fleet)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", 0.2)
    fragment = residency_map.fragment_path(
        Path(args.residency_root), CONSUMER, MOVER)
    document = json.loads(fragment.read_bytes())
    document["manifest_sha256"] = "8" * 64
    conflicting = json.dumps(document, sort_keys=True).encode()
    _corrupt(fragment, conflicting)

    with pytest.raises(SystemExit, match="residency_prior_fragment_conflict"):
        stage_move.move(args)
    assert fragment.read_bytes() == conflicting, (
        "the conflicting ownership record was overwritten")


@pytest.mark.parametrize("poison", ["outside_window", "other_extent"])
def test_a_conflicting_extent_in_the_own_fragment_refuses(
        fleet, monkeypatch, poison: str) -> None:
    """Contradictory extents are never silently pruned into coverage."""

    _tmp, args, _mount, entries, keys, _names = fleet
    _interrupted_first_attempt(fleet)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", 0.2)
    fragment = residency_map.fragment_path(
        Path(args.residency_root), CONSUMER, MOVER)
    document = json.loads(fragment.read_bytes())
    if poison == "outside_window":
        document["entries"]["0:/mnt/never-declared.bin"] = {
            "stage_path": str(Path(args.stage_root) / "never-declared.bin"),
            "bytes": 8, "offset": 0, "sha256": "7" * 64}
    else:
        document["entries"][keys[1]]["bytes"] = SIZES[1] + 1
    conflicting = json.dumps(document, sort_keys=True).encode()
    _corrupt(fragment, conflicting)

    with pytest.raises(SystemExit, match="residency_prior_fragment_conflict"):
        stage_move.move(args)
    assert fragment.read_bytes() == conflicting


@pytest.mark.parametrize("header", [
    "manifest", "epoch", "consumer", "tier", "stage_root"])
def test_conflicting_own_material_headers_refuse(
        fleet, monkeypatch, header: str) -> None:
    """A readable sidecar with contradictory headers is never carried and
    republished under corrected headers."""

    _tmp, args, _mount, _entries, _keys, _names = fleet
    _interrupted_first_attempt(fleet)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", 0.2)
    sidecar = reader_lease.material_path(
        Path(args.residency_root), CONSUMER, MOVER)
    document = json.loads(sidecar.read_bytes())
    document[{"manifest": "manifest_sha256", "consumer": "consumer_action_key",
              "tier": "tier_id", "stage_root": "stage_root",
              "epoch": "epoch"}[header]] = {
        "manifest": "8" * 64, "consumer": "d" * 64,
        "tier": "prismabuild-stage:other",
        "stage_root": str(Path(args.stage_root).parent / "elsewhere"),
        "epoch": "some-epoch"}[header]
    conflicting = json.dumps(document, sort_keys=True).encode()
    _corrupt(sidecar, conflicting)

    with pytest.raises(SystemExit,
                       match="residency_prior_material_conflicting"):
        stage_move.move(args)
    assert sidecar.read_bytes() == conflicting, (
        "the contradictory sidecar was rewritten under corrected headers")


def test_concurrent_publications_keep_the_preserved_prefix(
        fleet, monkeypatch) -> None:
    """Two workers publishing per landing never drop preserved coverage.

    One controlled concurrency regression, per review: the snapshot guard
    and the publish lock must keep every out-of-order snapshot at or above
    the coverage this invocation inherited.
    """

    _tmp, args, _mount, _entries, keys, _names = fleet
    _interrupted_first_attempt(fleet)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", 0.2)
    monkeypatch.setattr(stage_move, "FRAGMENT_PUBLISH_S", 0.0)
    args.max_readers = 2
    documents = _Recorder(monkeypatch, residency_map, "write_fragment")

    second = stage_move.move(args)

    assert second["complete"] is True and second["errors"] == []
    assert documents.documents, "per-landing publications were recorded"
    assert all(keys[1] in document["entries"]
               for document in documents.documents), (
        "a concurrent publication dropped preserved coverage")
    assert set(documents.documents[-1]["entries"]) == set(keys)


# --- generations and live pins ----------------------------------------------

def _pin(fleet, **overrides):
    _tmp, args, _mount, entries, keys, _names = fleet
    queue = pool.PoolQueue(Path(args.pool_root))
    params = dict(
        consumer_action_key=CONSUMER,
        attempt={"nonce": "n1", "scope_id": "s1"},
        tier_id=TIER, epoch="",
        span={"start": 0, "end": TOTAL},
        holder={"host": "h", "worker": "w", "pid": os.getpid()},
        acquire_token="tok",
        covers=[{"mover_action_key": MOVER,
                 "manifest_sha256": MANIFEST_SHA}],
        expected={key: {"bytes": int(entry["bytes"]),
                        "sha256": str(entry["sha256"])}
                  for key, entry in zip(keys[:2], entries[:2])},
        residency_root=Path(args.residency_root))
    params.update(overrides)
    return queue, reader_lease.acquire(queue, **params)


def test_a_live_pin_on_a_resumed_entry_stays_valid_as_new_material_lands(
        fleet, monkeypatch) -> None:
    """The old pin keeps reading its entry while the retry lands new bytes.

    Without the resume the suffix file loses proof, is recopied, and the
    gate refuses against the very pin protecting it -- the retry cannot
    complete.  With it, unchanged bytes keep their inode and their date's
    generation, and the pin's descriptor still verifies.
    """

    _tmp, args, _mount, _entries, keys, _names = fleet
    _interrupted_first_attempt(fleet)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", 0.2)
    queue, pin = _pin(fleet)
    assert pin["ok"], pin
    prior = _material(args)

    second = stage_move.move(args)

    assert second["complete"] is True and second["errors"] == []
    after = _material(args)
    # New bytes landed (the third entry), so a new generation was minted
    # once -- the carried date no longer described the whole document.
    assert after["generation"] != prior["generation"]
    # The resumed entry kept its identity: same file, same recorded inode.
    assert after["entries"][keys[1]]["file_id"] \
        == prior["entries"][keys[1]]["file_id"]
    # And the pin taken before the retry still opens and reads it.
    fd, _serving = reader_lease.open_pinned(
        queue, pin["pin"], pin["ref_id"], keys[1],
        residency_root=Path(args.residency_root))
    try:
        assert os.read(fd, SIZES[1]) == b"b" * SIZES[1]
    finally:
        os.close(fd)


def test_an_all_adopted_resume_keeps_the_material_generation(
        fleet, monkeypatch) -> None:
    """Unchanged bytes keep their date: no spurious generation change.

    A resumed run that replaces nothing carries its prior generation, so a
    reader re-acquiring over the same window gets its own pin back rather
    than a ``generation-changed`` refusal -- and the adoption accounting is
    exact, with adopted bytes counted once and no resume double-counting.
    """

    _tmp, args, mount, _entries, keys, _names = fleet
    _interrupted_first_attempt(fleet)
    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", 0.2)
    queue, pin = _pin(fleet)
    assert pin["ok"], pin
    prior_generation = _material(args)["generation"]

    # Interrupt the retry the same way: everything it touches, it adopts.
    (mount / "shard-3.bin").write_bytes(b"c" * (SIZES[2] // 2))
    second = stage_move.move(args)

    assert second["complete"] is False
    assert second["entries_staged"] == 2
    assert second["bytes_staged"] == SIZES[0] + SIZES[1], (
        "adopted bytes counted once, resumed coverage counted never")
    assert second["entries_resumed"] == 2
    assert _material(args)["generation"] == prior_generation, (
        "a resume that replaced nothing minted a new generation")

    again = reader_lease.acquire(
        queue,
        consumer_action_key=CONSUMER,
        attempt={"nonce": "n1", "scope_id": "s1"},
        tier_id=TIER, epoch="",
        span={"start": 0, "end": TOTAL},
        holder={"host": "h", "worker": "w", "pid": os.getpid()},
        acquire_token="tok",
        covers=[{"mover_action_key": MOVER,
                 "manifest_sha256": MANIFEST_SHA}],
        expected={keys[0]: {"bytes": SIZES[0], "sha256": hashlib.sha256(
            b"a" * SIZES[0]).hexdigest()},
            keys[1]: {"bytes": SIZES[1], "sha256": hashlib.sha256(
            b"b" * SIZES[1]).hexdigest()}},
        residency_root=Path(args.residency_root))
    assert again.get("ok") is True and again.get("duplicate") is True, again
