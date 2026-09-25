"""Publish waits are recorded per entry, and the refusal names the verdict
it waited on (#994).

Pattern from #983 (patterns 3 and 4): a fragment names a staged destination
without a usable date -- no material sidecar, a live race or a dead owner's
gap the gate cannot and must not guess at (#839).  ``_StagedPublisher.publish``
(:func:`stage_move._StagedPublisher.publish`) polls the four ``PUBLISH_WAIT_*``
verdicts for the whole ``_PUBLISH_GRACE_S`` (30 s in production), re-taking
the host-wide lock every ``_PUBLISH_POLL_S``, then refuses.  Before #994 the
receipt carried ``phase_timings``, ``start_gate_wait_s`` and
``resume_lock_wait_s`` but nothing that said an entry had waited at all: no
per-entry count, no seconds, no verdict kind -- and the refusal that ended
the range was a free-text message, capped among 20 entries (#853), naming the
destination but never which of the four kinds it waited under.

This drives the real ``stage_move.move`` end to end on a synthetic queue and
stage root, never real ``/stage``: one destination is staged, a fragment
names it with no sidecar (the exact acceptance fixture the issue names --
"a destination named by a fragment with no sidecar"), and no housekeeping
runs to retire anything, so the successor's publish waits out the whole
grace and refuses.  Grace and poll are shrunk (this suite's own convention,
``test_stale_material_done_owner_retires.GRACE``) so the case proves the
30 s bound through a fake, sub-second grace rather than a real sleep.
"""
from __future__ import annotations

import hashlib
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

import test_dead_owner_fragment_blocks_then_retires as base  # noqa: E402
from test_dead_owner_fragment_blocks_then_retires import fleet  # noqa: E402,F401
import prismabuild.core as pb  # noqa: E402
from prismabuild import residency_map  # noqa: E402
import stage_move  # noqa: E402

SIZE = 4096
TIER = base.TIER
NAMES = ["entry-00.bin"]

#: One publication grace for the whole case, so the wait it proves is real
#: (the entry must actually sit for it) without ever sleeping 30 s.
GRACE = 0.5


def _payload(seed: int) -> bytes:
    return bytes((seed * 37 + index) % 251 for index in range(SIZE))


def _manifest(tmp_path: Path, names: list[str], payloads: dict[str, bytes]):
    """One real manifest, one real source file per name."""

    mount = tmp_path / "sources"
    mount.mkdir(exist_ok=True)
    entries = []
    for name in names:
        payload = payloads[name]
        source = mount / name
        source.write_bytes(payload)
        entries.append({"path": str(source), "offset": 0, "bytes": SIZE,
                        "sha256": hashlib.sha256(payload).hexdigest()})
    body = {"schema": pb.DATA_MANIFEST_SCHEMA_V1, "produced_by": {},
            "annotations": {}, "mount_prefix": str(mount),
            "entries": entries, "entry_count": len(entries),
            "total_bytes": len(entries) * SIZE}
    raw = pb._canonical_file_bytes(pb.validate_data_manifest(body))
    path = tmp_path / "manifest.json"
    path.write_bytes(raw)
    return mount, path, hashlib.sha256(raw).hexdigest(), entries


def _destination(stage: Path, mount: Path, entry: dict[str, object]) -> Path:
    relative = stage_move.stage_relative(
        str(entry["path"]), 0, SIZE, mount_prefix=str(mount))
    return stage / relative


def _args(queue, stage, cas, mover: str, consumer: str, manifest: Path,
         digest: str, entries: list[dict[str, object]]):
    return stage_move.build_parser().parse_args([
        "--pool-root", str(queue.root), "--cas-root", str(cas),
        "--action-key", mover, "--consumer-action-key", consumer,
        "--tier-id", TIER, "--stage-root", str(stage),
        "--manifest", str(manifest), "--manifest-sha256", digest,
        "--range-start-bytes", "0",
        "--range-end-bytes", str(len(entries) * SIZE),
        "--residency-root", str(queue.residency_fragment_root()),
        "--readers", "1", "--max-readers", "1", "--unpaced"])


def _fragment_no_sidecar(queue, stage: Path, consumer: str, mover: str,
                         path: Path, digest: str) -> None:
    """A fragment vouches for ``path`` with no material sidecar to date it.

    Every census a wait defers to -- pins, live claims, copies in flight --
    reads clean here; only the vouch itself, undated, stands in the way.
    Neither owner is ever published to the queue: liveness plays no part in
    the ``owned`` verdict (:meth:`stage_move._StagedPublisher._decide`), only
    the missing date does, so a fresh unqueued key names it exactly as a real
    mover's would.
    """

    key = residency_map.residency_map_key(str(path), 0)
    residency_map.write_fragment(queue.residency_fragment_root(), {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": TIER, "stage_root": str(stage),
        "manifest_sha256": "a" * 64,
        "entries": {key: {"stage_path": str(path), "bytes": SIZE,
                          "sha256": digest, "offset": 0}}})


def test_a_publish_wait_is_recorded_by_verdict_and_seconds(
        fleet, tmp_path, monkeypatch):
    """RED on main: the receipt must say which verdict waited, for how many
    entries and how many seconds, and the refusal must name both the verdict
    and the entry rather than a free-text sentence naming only the path."""

    queue, stage, cas = fleet
    payload = _payload(0)
    mount, manifest, digest, entries = _manifest(
        tmp_path, NAMES, {NAMES[0]: payload})
    destination = _destination(stage, mount, entries[0])
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Already staged, under bytes a proof would have to date -- the fragment
    # below vouches for the name but dates nothing.
    old = b"o" * SIZE
    destination.write_bytes(old)
    owner_consumer, owner_mover = base._key(), base._key()
    _fragment_no_sidecar(queue, stage, owner_consumer, owner_mover,
                        destination, hashlib.sha256(old).hexdigest())

    monkeypatch.setattr(stage_move, "_PUBLISH_GRACE_S", GRACE)
    monkeypatch.setattr(stage_move, "_PUBLISH_POLL_S", 0.02)
    mover, successor = base._key(), base._key()
    args = _args(queue, stage, cas, mover, successor, manifest, digest,
                entries)

    started = time.monotonic()
    result = stage_move.move(args)
    elapsed = time.monotonic() - started

    assert result["complete"] is False, result
    assert elapsed >= GRACE, (
        "the entry must actually wait out the grace, not heal or refuse "
        "early")

    # The receipt must say which verdict waited, for how many entries, and
    # for how many seconds -- absent entirely before #994.
    waits = result["publish_waits"]
    assert waits["owned"]["entries"] == 1, waits
    assert waits["owned"]["seconds"] >= GRACE * 0.9, waits
    assert set(waits) == {"owned"}, (
        f"only the owned wait fired in this fixture: {waits}")

    # The post-grace refusal must name both the verdict and the entry.
    assert len(result["errors"]) == 1, result["errors"]
    error = result["errors"][0]
    assert "owned" in error, (
        f"the refusal must name the verdict it waited on: {error}")
    assert str(destination) in error, (
        f"the refusal must name the entry it waited on: {error}")
