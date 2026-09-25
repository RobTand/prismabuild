"""A campaign row carries the residency and cpus options ``pbrun`` takes (#1082).

A row could not say ``residency``, ``residency_ram``,
``residency_prefetch_depth_gib``, ``residency_read_mb_s`` or ``cpus``: the
manifest was refused as naming unknown fields.  A client splitting staged work
into independent rows then had to drop the staged read path, which changes the
IO path the row reads by, or leave the campaign interface.

Each field is one ``pbrun`` flag, validated at load in ``pbrun``'s vocabulary
and bounds, and omitted fields pass nothing, as every row field does.
"""

from __future__ import annotations

import json
from pathlib import Path
import socket
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import pool, residency_plan, storage_tiers  # noqa: E402
import pbcampaign  # noqa: E402
import pbrun  # noqa: E402
import tier_loop  # noqa: E402
from test_pbrun_detach import _checkout  # noqa: E402
from test_pbrun_residency_stage_submission import (  # noqa: E402
    RAM_TIER, TIER, _manifest,
)

FIELDS = {
    "residency": ("stage", ["--residency", "stage"]),
    "residency_ram": ("off", ["--residency-ram", "off"]),
    "residency_prefetch_depth_gib": (8, ["--residency-prefetch-depth-gib", "8"]),
    "residency_read_mb_s": (400, ["--residency-read-mb-s", "400"]),
    "cpus": (4, ["--cpus", "4"]),
}


def _write(tmp_path: Path, rows) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(rows), encoding="utf-8")
    return path


@pytest.mark.parametrize("field", sorted(FIELDS))
def test_a_row_carrying_the_field_is_accepted_at_load(tmp_path: Path, field: str) -> None:
    """main: ``row 0 names fields this does not know: <field>``."""

    row = {"argv": ["/bin/true"], field: FIELDS[field][0]}

    assert pbcampaign.load_manifest(_write(tmp_path, [row]), transport="pool") == [row]


def test_each_field_is_exactly_its_pbrun_flag() -> None:
    """The row is the ``pbrun`` command line typed by hand, flag for flag."""

    row = {"argv": ["./reader.sh", "--range", "3"],
           **{field: value for field, (value, _) in FIELDS.items()}}

    assert pbcampaign.pbrun_argv(row) == [
        *FIELDS["residency"][1], *FIELDS["residency_ram"][1],
        *FIELDS["residency_prefetch_depth_gib"][1],
        *FIELDS["residency_read_mb_s"][1], *FIELDS["cpus"][1],
        "--", "./reader.sh", "--range", "3",
    ]
    assert pbcampaign.pbrun_argv({"argv": ["./reader.sh"]}) == ["--", "./reader.sh"], (
        "a row that omits them passes nothing, as before")


@pytest.mark.parametrize("field,value,reason", [
    ("residency", "stag", "must be one of none, stage"),
    ("residency", True, "must be one of none, stage"),
    ("residency_ram", "on", "must be one of auto, off"),
    ("residency_prefetch_depth_gib", -1, "0 or more"),
    ("residency_prefetch_depth_gib", 1.5, "must be an integer"),
    ("residency_prefetch_depth_gib", True, "must be an integer"),
    ("residency_read_mb_s", 0, "positive whole MB/s"),
    ("residency_read_mb_s", "fast", "must be an integer"),
    ("cpus", 0, "must be at least 1"),
    ("cpus", "4.0", "must be an integer"),
])
def test_a_value_pbrun_would_refuse_is_refused_at_load(
    tmp_path: Path, field: str, value, reason: str,
) -> None:
    rows = [{"argv": ["/bin/true"]}, {"argv": ["/bin/true"], field: value}]

    with pytest.raises(pbcampaign.ManifestError, match=f"row 1: .*{reason}"):
        pbcampaign.load_manifest(_write(tmp_path, rows), transport="pool")


def test_the_argv_pbrun_is_given_is_the_hand_typed_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``submit_row`` hands ``pbrun.main`` this row's command line, byte for byte."""

    seen: list[list[str]] = []

    def fake_main() -> int:
        seen.append(list(sys.argv))
        print(json.dumps({"action_key": "a" * 64, "status": "submitted"}))
        return 0

    monkeypatch.setattr(pbrun, "main", fake_main)
    row = {"argv": ["./reader.sh"], "cwd": "/home/rob/tree",
           "data_manifest": "/home/rob/manifest.json",
           **{field: value for field, (value, _) in FIELDS.items()}}

    assert pbcampaign.submit_row(row, transport="pool")["status"] == "submitted"
    assert seen == [[
        "pbrun.py", "--detach", "--transport", "pool",
        "--cwd", "/home/rob/tree", "--data-manifest", "/home/rob/manifest.json",
        "--residency", "stage", "--residency-ram", "off",
        "--residency-prefetch-depth-gib", "8", "--residency-read-mb-s", "400",
        "--cpus", "4", "--", "./reader.sh",
    ]]


# -- the staged read path, end to end ----------------------------------------


def _staged_fleet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> pool.PoolQueue:
    """A queue with a stage tier and a ram tier on the stage's own host.

    The ram tier is what makes ``residency_ram`` observable: under ``auto``,
    the default, a submission seals a ram leg onto the plan.
    """

    monkeypatch.setattr(pbrun, "SH", tmp_path)
    monkeypatch.setattr(pbrun, "POLL_S", 0.001)
    monkeypatch.setattr(socket, "gethostname", lambda: "sparky")
    queue = pool.PoolQueue(tmp_path / "pb-queue")
    queue.announce(host="sparky", tags=["sparky", "gb10"], has_gpu=True,
                   capacity={"cpu": 8, "mem_gb": 16, "gpu": 1})
    for tier_id, kind in ((TIER, "stage"), (RAM_TIER, "ram")):
        queue.announce_tier({
            "schema": storage_tiers.TIER_RECORD_SCHEMA_V1,
            "tier_id": tier_id, "host": "sparky", "tier": kind,
            "mountpoint": str(tmp_path / kind),
            "mover_python": sys.executable,
            "mover_tools_root": str(Path(pbrun.__file__).resolve().parent),
        })
    return queue


def _keys(capsys) -> list[str]:
    return [json.loads(line)["action_key"]
            for line in capsys.readouterr().out.splitlines() if line.strip()]


def test_a_staged_row_publishes_the_request_and_plan_pbrun_publishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """main: the manifest is refused before any row is submitted.

    branch: the row's consumer is published with its residency block, its
    plan carries the reader declaration and no ram leg, and its demand holds
    the declared cores.  The same submission typed as ``pbrun`` seals the same
    key, so the same request bytes (the CAS refuses a key whose bytes differ),
    and reseals the same plan.  The plan's ``demand_source`` is excluded: it
    records the fleet's pricing evidence when each seal ran, and the first
    seal registered the shared range the second one finds.
    """

    queue = _staged_fleet(tmp_path, monkeypatch)
    work = _checkout(tmp_path)
    manifest = tmp_path / "data-manifest.json"
    manifest.write_text(json.dumps(_manifest()), encoding="utf-8")
    command = ["/bin/bash", "-lc", "printf staged"]
    staged = {"argv": command, "cwd": str(work), "data_manifest": str(manifest),
              **{field: value for field, (value, _) in FIELDS.items()}}
    # The control: the same row without ``residency_ram`` seals the ram leg.
    control = {**{k: v for k, v in staged.items() if k != "residency_ram"},
               "argv": ["/bin/bash", "-lc", "printf control"]}

    assert pbcampaign.main(["--transport", "pool", "--detach",
                            str(_write(tmp_path, [staged, control]))]) == 0
    key, control_key = _keys(capsys)

    control_plan = residency_plan.read(queue, control_key)
    assert control_plan is not None and control_plan.get("ram_tier_id") == RAM_TIER
    plan = residency_plan.read(queue, key)
    assert plan is not None, "the row froze no residency plan"
    assert "ram_tier_id" not in plan, "residency_ram off did not reach pbrun"
    assert plan["reader"] == {"prefetch_depth_bytes": 8 * storage_tiers.GIB,
                              "read_mb_s": 400}
    row = json.loads(queue.item_path(pool.READY, key).read_text())
    assert row["residency"]["tier_id"] == TIER
    assert row["resources"]["cpu"] == 4
    request = (tmp_path / "cas" / "requests" / key[:2] / f"{key}.json").read_bytes()

    # End that window so a hand submission seals its own rather than
    # attaching to this one (the path #708's reseal tests drive).
    queue.withdraw(key, reason="compare with pbrun", by="test")
    tier_loop.withdraw_dead_consumer_movers(queue)
    assert residency_plan.read(queue, key) is None

    monkeypatch.setattr(sys, "argv", [
        "pbrun.py", "--transport", "pool", "--detach", "--cwd", str(work),
        "--data-manifest", str(manifest),
        "--residency", "stage", "--residency-ram", "off",
        "--residency-prefetch-depth-gib", "8", "--residency-read-mb-s", "400",
        "--cpus", "4", "--", *command,
    ])
    assert pbrun.main() == 0
    assert _keys(capsys) == [key]
    assert (tmp_path / "cas" / "requests" / key[:2] / f"{key}.json").read_bytes() == request
    by_hand = residency_plan.read(queue, key)
    assert by_hand is not None
    assert ({k: v for k, v in by_hand.items() if k != "demand_source"}
            == {k: v for k, v in plan.items() if k != "demand_source"})
    by_hand_row = json.loads(queue.item_path(pool.READY, key).read_text())
    assert by_hand_row["residency"] == row["residency"]
    assert by_hand_row["resources"] == row["resources"]
