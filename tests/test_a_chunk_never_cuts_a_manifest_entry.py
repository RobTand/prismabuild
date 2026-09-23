"""A chunk is cut only where a manifest entry ends (#965).

``storage_tiers.split_range_into_chunks`` cut a phase at fixed byte offsets
and never saw the manifest's entries.  A mover stages whole entries --
``prewarm_loop.entries_between`` includes an entry straddling either edge of
its range -- so a chunk edge inside an entry handed both neighbouring movers
more bytes than their tokens reserved, and each refused
``residency_overran_reservation`` on every attempt.  Live: 16,308,005 B over
a 40 GiB chunk, for stage chunk 0 (mover 8faf2233c63a) of PQ consumer
a7d31a4da9c1.

Every case here seals through ``pbrun.residency_stage_rows``, the one sealing
path both legs share, and checks what it sealed against the movers' own
predicate: the entries covering ``[start, end)`` in the manifest's read
order.  The end-to-end case then runs the real ``stage_move.move`` and
``ram_promote.promote`` over every sealed range.  The seal's arithmetic is
in ``storage_tiers.GIB`` units, so that case shrinks the unit to 4 KiB: the
same 40-unit chunk over the same entry layout, small enough that the movers
copy real bytes.

Nothing here touches the live queue, a real pool, a stage or a tmpfs: every
root is under ``tmp_path``.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
import types

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import pool, storage_tiers  # noqa: E402
import prewarm_loop  # noqa: E402
import tier_loop  # noqa: E402

CONSUMER = "c" * 64
HOST = "dl380g10"
STAGE_TIER = f"prismabuild-stage:{HOST}"
RAM_TIER = f"ram:{HOST}"
STAGE_KIND = f"stage_gib@{STAGE_TIER}"
RAM_KIND = f"ram_gib@{RAM_TIER}"
GIB = storage_tiers.GIB
#: The live sizing: a 160-unit ram window cut into 40-unit chunks.
WINDOW_UNITS = 160
CHUNK_UNITS = 40
READERS = 2
#: The overrun the live chunk refused with, used as the odd tail that keeps
#: the GiB-scale entries off whole-GiB offsets, as real shards are.
LIVE_ODD_BYTES = 16_308_005
#: The end-to-end unit: 4 KiB stands in for a GiB.
SMALL_UNIT = 1 << 12
SMALL_ODD_BYTES = 1_234

#: Which plan table each leg's chunks live in, and the row roles in it.
LEGS = {
    "stage": {"table": "stage_chunks", "mover": "mover_row",
              "whole_mover": "mover_row", "kind": STAGE_KIND,
              "tier": STAGE_TIER},
    "ram": {"table": "ram_chunks", "mover": "ram_mover_row",
            "whole_mover": "ram_mover_row", "kind": RAM_KIND,
            "tier": RAM_TIER},
}


def _straddling_sizes(unit: int, odd: int) -> list[list[int]]:
    """Two phases whose first puts a 40-unit byte edge inside an entry.

    Phase 0 is eight entries of ``15 units + odd`` and a 3-unit tail: a
    byte-offset cut at 40 units lands inside the third entry.  Phase 1 fits
    in one chunk and seals the whole-phase pair.
    """

    return [[15 * unit + odd] * 8 + [3 * unit], [5 * unit, 5 * unit]]


def _manifest(phase_sizes: list[list[int]], *, mount: str = "/mnt/shared",
              paths: list[str] | None = None,
              digests: list[str | None] | None = None) -> dict[str, object]:
    entries, table, running, index = [], [], 0, 0
    for ordinal, sizes in enumerate(phase_sizes):
        for size in sizes:
            entries.append({
                "path": (paths[index] if paths is not None
                         else f"{mount}/part-{index}"),
                "offset": 0, "bytes": size,
                "sha256": None if digests is None else digests[index]})
            running += size
            index += 1
        table.append({"name": f"phase-{ordinal}", "bytes": sum(sizes),
                      "cumulative_bytes": running})
    return {
        "schema": pb.DATA_MANIFEST_SCHEMA_V1,
        "produced_by": {}, "annotations": {"phases": table},
        "mount_prefix": mount, "entries": entries,
        "entry_count": len(entries), "total_bytes": running,
    }


class _Cas:
    def __init__(self, manifest_path: Path,
                 actions: dict[str, dict] | None = None) -> None:
        self._manifest = manifest_path
        self.actions: dict[str, dict] = {} if actions is None else actions

    def input_path(self, entry):
        return self._manifest

    def publish_action_request(self, action) -> None:
        self.actions[str(action["action_key"])] = dict(action)


def _template(digest: str, size: int) -> dict[str, object]:
    import pbrun

    return {
        "cas": None, "marker_root": Path("/home/rob/tmp/markers"),
        "checkout_identity": {"commit": "a" * 40},
        "log_name": "x.log", "stamp_name": "pbrun.stamp",
        "task": {"definition_id": "fleet/pbrun", "definition_version": "v1",
                 "task_class": "generation", "determinism": "stochastic",
                 "artifact_family": "generic", "artifact_kind": "generic",
                 "working_directory": "."},
        "inputs": [{"id": "pbrun.checkout-snapshot", "sha256": "b" * 64,
                    "bytes": 4096}],
        "code_closure": pbrun.build_stamp_closure("pbrun.stamp", "{}"),
        "params": {"command": ["true"], "cwd": "/home/rob", "demand": {"cpu": 1},
                   "placement": {"required_tags": []},
                   "checkout_snapshot": {
                       "schema": pb.PBRUN_CHECKOUT_SNAPSHOT_SCHEMA_V1,
                       "commit": "a" * 40, "subdirectory": ".",
                       "input": {"id": pb.PBRUN_CHECKOUT_SNAPSHOT_INPUT_ID,
                                 "sha256": "b" * 64, "bytes": 4096}},
                   "retry_policy": {"max_attempts": 1, "retry_safe": False},
                   "data_manifest": {"input": {"sha256": digest, "bytes": size}}},
        "environment": {"variables": {"PATH": "/usr/bin"}, "toolchain": {}},
        "execution_scope": {"portability": "portable", "platform_key": None,
                            "host_class": None},
    }


def _seal(tmp_path: Path, manifest: dict[str, object], *,
          with_ram_tier: bool = True,
          published: dict[str, dict] | None = None) -> dict[str, object]:
    """Seal ``manifest`` through pbrun against a stage and a ram tier.

    The ram record announces the live sizing in whatever ``GIB`` currently
    is, and the stage record inherits its chunk the way ``tier_loop.cycle``
    announces it (#675).  ``published`` collects every action the seal
    publishes to the CAS, including when it refuses.
    """

    import pbrun

    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    unit = storage_tiers.GIB

    def discover(**_kwargs):
        tiers = {
            STAGE_TIER: {
                "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                "tier_id": STAGE_TIER, "host": HOST, "tier": "stage",
                "mountpoint": str(tmp_path / "stage"),
                "capacity_bytes": 512 * unit},
        }
        if with_ram_tier:
            tiers[RAM_TIER] = {
                "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
                "tier_id": RAM_TIER, "host": HOST, "tier": "ram",
                "mountpoint": str(tmp_path / "ram"),
                "capacity_bytes": WINDOW_UNITS * unit,
                "window_gib": WINDOW_UNITS,
                "promotion_chunk_gib": CHUNK_UNITS}
        return tiers

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    tier_loop.cycle(queue, host=HOST, source_pool="storage_pool",
                    receipts=tier_loop.ReceiptCache(), discover=discover)
    tier = pbrun.resolve_stage_tier(queue, None)
    args = types.SimpleNamespace(
        priority=-10, max_attempts=1, retry_safe=False,
        residency="stage", residency_tier=None, residency_mover_mem_gb=1,
        residency_mover_readers=READERS, residency_mover_max_attempts=3)
    cas = _Cas(manifest_path, published)
    staged = pbrun.residency_stage_rows(
        _template(digest, manifest_path.stat().st_size),
        consumer_action_key=CONSUMER, tier=tier, args=args, queue=queue,
        cas=cas)
    return {**staged, "cas": cas, "queue": queue, "digest": digest,
            "manifest_path": manifest_path}


def _leg_ranges(phase: dict, leg: str) -> list[tuple[int, int, dict]]:
    """``(start, end, mover_row)`` for every mover of one phase's leg."""

    spec = LEGS[leg]
    chunks = phase.get(spec["table"])
    if chunks is not None:
        return [(int(chunk["start_bytes"]), int(chunk["end_bytes"]),
                 chunk[spec["mover"]]) for chunk in chunks]
    return [(int(phase["start_bytes"]), int(phase["end_bytes"]),
             phase[spec["whole_mover"]])]


def _boundaries(manifest: dict[str, object]) -> set[int]:
    running, seen = 0, {0}
    for entry in prewarm_loop.manifest_read_entries(manifest):
        running += int(entry["bytes"])
        seen.add(running)
    return seen


@pytest.mark.parametrize("leg", sorted(LEGS))
def test_every_sealed_range_covers_exactly_the_entries_it_reserves(
        tmp_path, leg) -> None:
    """The movers' own predicate, as an equality, for every sealed range.

    ``stage_move`` and ``ram_promote`` refuse when the entries covering
    their range weigh more than the range; a range whose edges are entry
    boundaries weighs exactly its entries, and its reservation is those
    bytes in whole units.  On main, chunk 0 of phase 0 is ``[0, 40 GiB)``,
    which covers three entries of 15 GiB + 16,308,005 B.
    """

    manifest = _manifest(_straddling_sizes(GIB, LIVE_ODD_BYTES))
    staged = _seal(tmp_path, manifest)
    read = prewarm_loop.manifest_read_entries(manifest)
    edges = _boundaries(manifest)

    for phase in staged["plan"]["phases"]:  # type: ignore[index]
        for start, end, mover in _leg_ranges(phase, leg):
            covered = sum(int(entry["bytes"]) for entry in
                          prewarm_loop.entries_between(read, start, end))
            assert covered == end - start, (
                f"{leg} range [{start}, {end}) of {phase['name']} covers "
                f"{covered} bytes of entries: residency_overran_reservation")
            assert start in edges and end in edges
            assert mover["resources"][LEGS[leg]["kind"]] == (
                storage_tiers.stage_tokens_for_bytes(end - start))
            assert (mover["residency"]["range_start_bytes"],
                    mover["residency"]["range_end_bytes"]) == (start, end)


@pytest.mark.parametrize("leg", sorted(LEGS))
def test_the_straddling_phase_seals_entry_aligned_chunks(tmp_path, leg) -> None:
    """Greedy at the entry boundary: two 15 GiB entries fit in 40, three do
    not, and the 3 GiB tail rides the last chunk."""

    sizes = _straddling_sizes(GIB, LIVE_ODD_BYTES)
    entry = sizes[0][0]
    staged = _seal(tmp_path, _manifest(sizes))
    big = staged["plan"]["phases"][0]  # type: ignore[index]

    chunks = big[LEGS[leg]["table"]]
    assert [(chunk["chunk_index"], chunk["start_bytes"], chunk["end_bytes"])
            for chunk in chunks] == [
        (0, 0, 2 * entry), (1, 2 * entry, 4 * entry),
        (2, 4 * entry, 6 * entry), (3, 6 * entry, 8 * entry + 3 * GIB)]
    for chunk in chunks:
        assert chunk["stage_gib"] == storage_tiers.stage_tokens_for_bytes(
            chunk["end_bytes"] - chunk["start_bytes"])


@pytest.mark.parametrize("leg", sorted(LEGS))
def test_a_phase_s_chunks_stage_each_entry_once(tmp_path, leg) -> None:
    """No double count: the chunks' entries are the phase's entries, each
    once and in read order, and the chunks' bytes sum to the phase's."""

    manifest = _manifest(_straddling_sizes(GIB, LIVE_ODD_BYTES))
    staged = _seal(tmp_path, manifest)
    read = prewarm_loop.manifest_read_entries(manifest)

    for phase in staged["plan"]["phases"]:  # type: ignore[index]
        start, end = int(phase["start_bytes"]), int(phase["end_bytes"])
        spans = _leg_ranges(phase, leg)
        staged_entries = [entry["path"] for cstart, cend, _ in spans
                          for entry in prewarm_loop.entries_between(
                              read, cstart, cend)]
        assert staged_entries == [
            entry["path"] for entry in
            prewarm_loop.entries_between(read, start, end)]
        assert sum(cend - cstart for cstart, cend, _ in spans) == end - start


@pytest.mark.parametrize("leg", sorted(LEGS))
def test_an_entry_larger_than_the_chunk_is_a_chunk_of_its_own(
        tmp_path, leg) -> None:
    """A 41 GiB entry cannot share a 40 GiB chunk and cannot be cut, so it
    is sealed alone and reserved at its own size."""

    odd = LIVE_ODD_BYTES
    sizes = [[10 * GIB, 41 * GIB + odd, 10 * GIB, 10 * GIB], [5 * GIB]]
    staged = _seal(tmp_path, _manifest(sizes))
    big = staged["plan"]["phases"][0]  # type: ignore[index]

    chunks = big[LEGS[leg]["table"]]
    assert [(chunk["start_bytes"], chunk["end_bytes"]) for chunk in chunks] == [
        (0, 10 * GIB), (10 * GIB, 51 * GIB + odd),
        (51 * GIB + odd, 71 * GIB + odd)]
    alone = chunks[1][LEGS[leg]["mover"]]
    assert alone["resources"][LEGS[leg]["kind"]] == 42


def test_an_entry_larger_than_the_window_refuses_at_seal(tmp_path) -> None:
    """No ram chunk can hold a 161 GiB entry under a 160 GiB window, so its
    promotion could never be admitted.  The seal refuses by name and
    publishes nothing, rather than sealing a mover that waits forever."""

    sizes = [[10 * GIB, (WINDOW_UNITS + 1) * GIB, 10 * GIB], [5 * GIB]]
    published: dict[str, dict] = {}

    with pytest.raises(SystemExit, match="residency_entry_exceeds_window"):
        _seal(tmp_path, _manifest(sizes), published=published)

    assert published == {}


def test_a_stage_leg_without_a_ram_tier_seals_whole_phases(tmp_path) -> None:
    """No ram tier, no announced chunk: the stage seals whole-phase pairs,
    and a phase is always entry-aligned, so nothing changes there."""

    manifest = _manifest(_straddling_sizes(GIB, LIVE_ODD_BYTES))
    staged = _seal(tmp_path, manifest, with_ram_tier=False)

    for phase in staged["plan"]["phases"]:  # type: ignore[index]
        assert "stage_chunks" not in phase and "ram_chunks" not in phase
        assert "mover_row" in phase


def test_every_sealed_mover_completes_on_both_legs(tmp_path, monkeypatch) -> None:
    """Seal, then run every sealed range through the real movers.

    Stage movers copy the pool fixture onto the stage; promotions copy the
    landed stage ranges into the tmpfs.  Every one completes, each stages
    exactly the bytes its range names, and every entry lands once per tier.
    On main, stage chunk 0 refuses ``residency_overran_reservation``.
    """

    import ram_promote
    import stage_move

    monkeypatch.setattr(storage_tiers, "GIB", SMALL_UNIT)
    phase_sizes = _straddling_sizes(SMALL_UNIT, SMALL_ODD_BYTES)
    mount = tmp_path / "mnt"
    mount.mkdir()
    paths, digests, payloads = [], [], {}
    index = 0
    for sizes in phase_sizes:
        for size in sizes:
            payload = bytes([index % 251 + 1]) * size
            path = mount / f"shard-{index:02d}.bin"
            path.write_bytes(payload)
            paths.append(str(path))
            digests.append(hashlib.sha256(payload).hexdigest())
            payloads[str(path)] = payload
            index += 1
    manifest = _manifest(phase_sizes, mount=str(mount), paths=paths,
                         digests=digests)
    staged = _seal(tmp_path, manifest)
    plan = staged["plan"]
    queue = staged["queue"]
    assert isinstance(queue, pool.PoolQueue)
    queue.ensure_layout()
    digest = str(staged["digest"])
    manifest_path = str(staged["manifest_path"])
    (tmp_path / "ram").mkdir()
    assert storage_tiers.ensure_ram_epoch(tmp_path / "ram", host=HOST)

    def stage(mover_key: str, start: int, end: int) -> dict:
        args = stage_move.build_parser().parse_args([
            "--pool-root", str(queue.root),
            "--cas-root", str(tmp_path / "cas"),
            "--action-key", mover_key,
            "--consumer-action-key", CONSUMER,
            "--tier-id", STAGE_TIER,
            "--stage-root", str(tmp_path / "stage"),
            "--manifest-sha256", digest,
            "--range-start-bytes", str(start),
            "--range-end-bytes", str(end),
            "--manifest", manifest_path,
            "--residency-root", str(queue.root / pool.RESIDENCY),
            "--block", str(1 << 12),
            "--readers", "2", "--max-readers", "2", "--unpaced",
        ])
        return stage_move.move(args)

    def promote(mover_key: str, start: int, end: int) -> dict:
        args = ram_promote.build_parser().parse_args([
            "--pool-root", str(queue.root),
            "--action-key", mover_key,
            "--consumer-action-key", CONSUMER,
            "--tier-id", RAM_TIER,
            "--ram-root", str(tmp_path / "ram"),
            "--source-stage-root", str(tmp_path / "stage"),
            "--manifest-sha256", digest,
            "--range-start-bytes", str(start),
            "--range-end-bytes", str(end),
            "--manifest", manifest_path,
            "--residency-root", str(queue.root / pool.RESIDENCY),
        ])
        return ram_promote.promote(args)

    phases = plan["phases"]  # type: ignore[index]
    assert "stage_chunks" in phases[0] and "ram_chunks" in phases[0]
    for leg, run in (("stage", stage), ("ram", promote)):
        for phase in phases:
            for start, end, mover in _leg_ranges(phase, leg):
                key = str(mover["action_key"])
                receipt = run(key, start, end)
                assert receipt["complete"] is True, (leg, start, end, receipt)
                assert "refusal" not in receipt
                assert receipt["bytes_staged"] == end - start
                queue.record_move(key, receipt)

    for root in ("stage", "ram"):
        for path, payload in payloads.items():
            relative = stage_move.stage_relative(
                path, 0, len(payload), mount_prefix=str(mount))
            assert (tmp_path / root / relative).read_bytes() == payload
