"""``pbrun --residency stage`` submits its whole window through one path (#605).

``pbrun.residency_stage_rows`` and the ``--residency stage`` branch of
``pbrun.main`` are the path a campaign takes first, and the pieces around
them are each covered while the wiring between them is not: parsing the flag
shape, sealing distinct keys per range, freezing the plan, choosing the tier.
A mistake in that wiring surfaces only on a live submission -- and did, when
the consumer's row was stamped but no mover's was (#602's follow-up).

So this file builds a small CAS with a two-phase manifest, runs the real
submission path -- ``pbrun.main`` with ``--detach``, the same seal, freeze
and publish a campaign runs -- and asserts the published consumer row
carries a residency block whose leads are phase 0's mover, that only that
mover is published, and that the frozen plan's rows are the ones sealed.
The queue, the tier announcement and the manifest are local; the CAS is
real.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import socket
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))

from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import pbrun  # noqa: E402
from test_pbrun_detach import _checkout  # noqa: E402

TIER = "prismabuild-stage:sparky"
GIB = storage_tiers.GIB
PHASE_BYTES = 2 * GIB


def _manifest() -> dict[str, object]:
    """A two-phase v1 manifest, one entry per phase."""

    entries, table, running = [], [], 0
    for index in range(2):
        entries.append({"path": f"/mnt/shared/part-{index}", "offset": 0,
                        "bytes": PHASE_BYTES, "sha256": None})
        running += PHASE_BYTES
        table.append({"name": f"phase-{index}", "bytes": PHASE_BYTES,
                      "cumulative_bytes": running})
    return {
        "schema": "prismaquant.prismabuild.data_manifest.v1",
        "produced_by": {}, "annotations": {"phases": table},
        "mount_prefix": "/mnt/shared", "entries": entries,
        "entry_count": len(entries), "total_bytes": running,
    }


def _submit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """One detached ``--residency stage`` submission against a local queue."""

    work = _checkout(tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_raw = json.dumps(_manifest()).encode()
    manifest_path.write_bytes(manifest_raw)

    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(
        host="sparky", tags=["sparky", "gb10"], has_gpu=True,
        capacity={"cpu": 4, "mem_gb": 16, "gpu": 1},
    )
    queue.announce_tier({
        "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
        "tier_id": TIER, "host": "sparky", "tier": "stage",
        "mountpoint": str(tmp_path / "stage"),
        "mover_python": sys.executable,
        "mover_tools_root": str(Path(pbrun.__file__).resolve().parent),
    })

    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--cwd", str(work), "--wait-s", "0.01",
        "--detach", "--data-manifest", str(manifest_path),
        "--residency", "stage",
        "--", "/bin/bash", "-lc", "printf staged",
    ])
    assert pbrun.main() == 0
    return {"queue": queue, "manifest_raw": manifest_raw}


def _detach_key(capsys) -> str:
    lines = [line for line in capsys.readouterr().out.splitlines()
             if line.strip()]
    assert len(lines) == 1, f"stdout carried {len(lines)} lines: {lines!r}"
    return json.loads(lines[0])["action_key"]


def test_the_submission_publishes_the_consumer_and_only_its_first_mover(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """The consumer waits on phase 0's mover, and nothing else is in ``ready``."""

    submitted = _submit(tmp_path, monkeypatch)
    queue = submitted["queue"]
    consumer_key = _detach_key(capsys)

    plan = residency_plan.read(queue, consumer_key)
    assert plan is not None, "the submission froze no plan"
    assert [phase["name"] for phase in plan["phases"]] == ["phase-0", "phase-1"]
    lead = str(plan["phases"][0]["mover_row"]["action_key"])
    second = str(plan["phases"][1]["mover_row"]["action_key"])
    assert lead != second, "two phases sealed the same mover"

    consumer = pool._read_json(queue.item_path(pool.READY, consumer_key))
    assert consumer is not None, "the consumer row was not published"
    block = consumer.get("residency")
    assert isinstance(block, dict), "the consumer row carries no residency block"
    assert block["tier_id"] == TIER
    assert block["leads"] == [lead], (
        "the consumer must wait on phase 0's mover and nothing else")
    assert block["manifest_sha256"] == hashlib.sha256(
        submitted["manifest_raw"]).hexdigest()
    assert block["manifest_bytes"] == len(submitted["manifest_raw"])

    assert pool._read_json(queue.item_path(pool.READY, lead)) is not None
    assert not queue.item_path(pool.READY, second).exists(), (
        "the second phase publishes as accepted progress advances, not here")
    assert {path.stem for path in queue.dir(pool.READY).glob("*.json")} == {
        consumer_key, lead}


def test_the_frozen_plan_rows_are_the_ones_sealed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """Every sealed body reached the CAS, and the published rows match the plan."""

    submitted = _submit(tmp_path, monkeypatch)
    queue = submitted["queue"]
    consumer_key = _detach_key(capsys)

    plan = residency_plan.read(queue, consumer_key)
    assert plan is not None
    sealed = [consumer_key]
    for phase in plan["phases"]:
        sealed.append(str(phase["mover_row"]["action_key"]))
        sealed.append(str(phase["egress_row"]["action_key"]))
        mover_row = pool._read_json(
            queue.item_path(pool.READY, str(phase["mover_row"]["action_key"])))
        if phase["name"] == "phase-0":
            assert mover_row is not None
            assert mover_row["residency"] == phase["mover_row"]["residency"], (
                "the published mover row is not the plan's sealed row")
            demand = mover_row["resources"]
            assert demand[f"stage_gib@{TIER}"] == 2, (
                "the mover's demand is its range's own ceiling in GiB")
    for key in sealed:
        blob = tmp_path / "cas" / "requests" / key[:2] / f"{key}.json"
        assert blob.exists(), f"sealed action {key[:12]} never reached the CAS"
