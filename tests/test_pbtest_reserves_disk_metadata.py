"""A shard can reserve its box's disk-metadata capacity (#1008 item 4).

Timing tests shaped like the stage (``test_an_egress_holds_the_stage_lock_
only_for_its_act.py``, ``bench_stage_adopt.py``) measure real hold times, and
two of them sharing a box distort each other's numbers: a 20,000-entry egress
measured 0.25 s alone and 1.22 s beside a second one on the same disk (#1005).
Neither ``cpu`` nor ``mem_gb`` demand serializes them, since directory/file
metadata throughput is not what either one prices.

``--disk-metadata`` reserves the new ``disk_metadata`` fleet demand kind
(``pbrun._FLEET_DEMAND_KINDS``) at 1 -- a box offers exactly one unit of it,
so declaring it is what gets a shard a box to itself for that contention.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest

from pbtest_shard_output import ShardProcess, shard_output_for  # noqa: E402

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

_SPEC = importlib.util.spec_from_file_location(
    "pbtest", REPOSITORY / "tools" / "fleet" / "pbtest.py"
)
pbtest = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pbtest)                      # type: ignore[union-attr]

import pbrun  # noqa: E402


class _FinishedProcess(ShardProcess):
    returncode = 0

    def __init__(self, command):
        self.output = shard_output_for(command)

    def communicate(self):
        return self.output, None


def _dispatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra):
    """One shard's dispatch, and the pbrun argv it built."""

    checkout = tmp_path / "checkout"
    test_file = checkout / "tests" / "test_one.py"
    test_file.parent.mkdir(parents=True)
    test_file.write_text("def test_one():\n    assert True\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)

    calls: list[list[str]] = []

    def _popen(command, **_kwargs):
        calls.append(list(command))
        return _FinishedProcess(command)

    monkeypatch.setattr(pbtest.subprocess, "Popen", _popen)
    monkeypatch.setattr(
        sys, "argv",
        ["pbtest.py", "--checkout", str(checkout), "--python", "/target/python",
         "--shards", "1", *extra, "tests"],
    )
    code = pbtest.main()
    return code, calls


def _demand(command) -> dict[str, int]:
    """The demand ``pbrun`` would seal, exactly as it parses ``--demand``."""

    flags = command[:command.index("--")]
    return pbrun._parse_demand(flags[flags.index("--demand") + 1])


def test_disk_metadata_is_off_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every other shard's demand is unchanged: no new key, uninvited."""

    code, calls = _dispatch(tmp_path, monkeypatch, [])

    assert code == 0
    assert "disk_metadata" not in _demand(calls[0])


def test_a_shard_reserves_the_boxs_whole_disk_metadata_capacity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--disk-metadata`` adds exactly one unit, beside the memory demand."""

    code, calls = _dispatch(tmp_path, monkeypatch, ["--disk-metadata"])

    assert code == 0
    command = calls[0]
    demand = _demand(command)
    assert demand["disk_metadata"] == 1
    # Its own kind, never in place of the memory demand pbtest always sends.
    assert demand["mem_gb"] == 3


def test_disk_metadata_is_a_validated_fleet_demand_kind() -> None:
    """The kind pbtest sends is one ``pbrun`` actually accepts (#1008 item 4).

    A shard whose demand ``pbrun`` refuses would never be admitted, so the
    flag's own kind must live in the closed vocabulary it validates against.
    """

    assert "disk_metadata" in pbrun._FLEET_DEMAND_KINDS
    pbrun.validate_fleet_demand({"disk_metadata": 1})


# --- SLURM has no GRES for it, so it refuses rather than under-delivering ---

def test_disk_metadata_is_refused_on_a_transport_that_cannot_enforce_it() -> None:
    """``slurm_lane.LaneResources.from_demand`` reads only cpu/gpu/mem_gb.

    Sealing ``disk_metadata`` there would admit the action on a promise of
    exclusivity SLURM never keeps -- two such jobs could still land on the
    same node -- so it is refused before sealing instead, the same reasoning
    ``require_progress_scope`` already applies to the stall watchdog.
    """

    pbrun.require_disk_metadata_scope({"disk_metadata": 1}, transport="pool")
    pbrun.require_disk_metadata_scope({}, transport="slurm")
    with pytest.raises(SystemExit, match="pull queue"):
        pbrun.require_disk_metadata_scope({"disk_metadata": 1}, transport="slurm")


def test_a_campaign_row_cannot_carry_disk_metadata_to_slurm() -> None:
    """Asked of pbrun in pbrun's own words, like every other row refusal."""

    import pbcampaign  # noqa: PLC0415  -- tools/fleet is on sys.path above

    row = {"argv": ["/bin/true"], "demand": {"disk_metadata": 1}}
    pbcampaign._require_submittable_row(row, index=2, transport="pool")
    with pytest.raises(pbcampaign.ManifestError, match="pull queue"):
        pbcampaign._require_submittable_row(row, index=2, transport="slurm")
