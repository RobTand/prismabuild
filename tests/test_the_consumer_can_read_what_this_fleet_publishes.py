"""The producer and the consumer of a residency map are in two repositories.

PrismaBuild writes the map; PrismaQuant reads it (`prismaquant/residency_map.py`,
merged as RobTand/prismaquant#708).  Neither repository can import the other, so
the only thing holding the two halves together is the document -- and a document
whose reader and writer disagree fails the way a cache miss looks: the consumer
reads the pool, at full cost, and says nothing was staged.  These tests pin the
surface the reader was written against, from this side.

They also pin the two other ends of the same hand-off: the flag PrismaQuant's
submitter emits, and the role that has to be running for any of it to exist.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools" / "fleet"))
from prismabuild import core as pb  # noqa: E402
from prismabuild import residency_map  # noqa: E402
import supervise  # noqa: E402

#: Exactly what ``prismaquant/residency_map.py`` declares it will accept.  Copied
#: rather than imported, because importing it is what this fleet must never do;
#: a change on either side has to break here rather than at 3 a.m. on a campaign.
CONSUMER_SCHEMA = "prismaquant.prismabuild.residency_map.v1"
CONSUMER_FRAGMENT_SCHEMA = "prismaquant.prismabuild.residency_map_fragment.v1"
CONSUMER_ENV_VAR = "PRISMABUILD_RESIDENCY_MAP"
CONSUMER_ROOT_KEYS = {"schema", "tier_id", "stage_root", "manifest_sha256",
                      "leads", "generation", "entries"}
CONSUMER_ENTRY_KEYS = {"stage_path", "bytes", "offset", "sha256"}


def _fragment(stage: Path, *, mover: str, consumer: str, path: str) -> dict:
    return {
        "schema": residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,
        "consumer_action_key": consumer, "mover_action_key": mover,
        "tier_id": "prismabuild-stage:dl380g10", "stage_root": str(stage),
        "manifest_sha256": "9" * 64,
        "entries": {residency_map.residency_map_key(path, 0): {
            "stage_path": str(stage / "shard.bin"), "bytes": 4096,
            "offset": 0, "sha256": "a" * 64}},
    }


def test_the_map_this_fleet_writes_is_the_map_the_consumer_declares(
        tmp_path: Path) -> None:
    stage = tmp_path / "stage"
    composed = residency_map.compose(
        [_fragment(stage, mover="1" * 64, consumer="2" * 64, path="/pool/a.bin")])

    assert composed["schema"] == CONSUMER_SCHEMA
    assert set(composed) == CONSUMER_ROOT_KEYS
    entry = next(iter(composed["entries"].values()))
    assert set(entry) == CONSUMER_ENTRY_KEYS
    # Decimal offset, one colon, then the path -- the reader splits on the
    # first colon so a path containing colons cannot collide.
    assert next(iter(composed["entries"])) == "0:/pool/a.bin"
    assert residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1 == CONSUMER_FRAGMENT_SCHEMA


def test_the_variable_the_launcher_sets_is_the_one_the_consumer_reads() -> None:
    assert pb.RESIDENCY_MAP_ENV == CONSUMER_ENV_VAR
    assert residency_map.RESIDENCY_MAP_ENV == CONSUMER_ENV_VAR


def test_the_map_survives_a_round_trip_through_a_file(tmp_path: Path) -> None:
    """The reader opens a path; what is on disk is the whole contract."""

    stage = tmp_path / "stage"
    written = residency_map.write_map(tmp_path / "x.map.json", residency_map.compose(
        [_fragment(stage, mover="1" * 64, consumer="2" * 64, path="/pool/a.bin")]))
    raw = json.loads(written.read_text())

    assert set(raw) == CONSUMER_ROOT_KEYS
    assert raw["manifest_sha256"] == "9" * 64
    assert raw["leads"] == ["1" * 64]
    assert isinstance(raw["generation"], int)


# -- the flag PrismaQuant's submitter emits ---------------------------------


def test_pbrun_takes_the_residency_flag_before_the_command() -> None:
    """``--residency stage`` precedes ``--``, and the command is untouched.

    That is the shape ``dispatch_tessera_campaign`` emits: a pbrun option like
    ``--data-manifest``, followed by the action's own argv after ``--``, which
    carries ``--data-manifest-sha256`` so the action can refuse a map composed
    for a different read set.
    """

    import pbrun

    args = pbrun.parse_args([
        "--tag", "gb10", "--priority", "-10",
        "--data-manifest", "/home/rob/tmp/manifest.json",
        "--residency", "stage", "--detach", "--",
        "python", "-m", "tool", "--data-manifest-sha256", "9" * 64])

    assert args.residency == "stage"
    assert args.data_manifest == "/home/rob/tmp/manifest.json"
    # Everything after ``--`` is the action's, including a flag whose name
    # begins the same way as one of pbrun's own.
    assert args.command[-2:] == ["--data-manifest-sha256", "9" * 64]


def test_a_submission_that_asks_for_no_stage_is_the_default() -> None:
    """An unasked-for option must leave the argv, and the key, as they were."""

    import pbrun

    args = pbrun.parse_args(["--tag", "gb10", "--", "true"])

    assert args.residency == "none"


def test_only_stage_is_a_residency_this_fleet_offers() -> None:
    import pbrun

    with pytest.raises(SystemExit):
        pbrun.parse_args(["--residency", "arc", "--", "true"])


# -- the role that mints any of it ------------------------------------------


def test_the_file_server_declares_the_tiers_role() -> None:
    """Nothing stages without it: no role, no tier, no token, no mover."""

    roles = dict(supervise.declared_roles("dl380g10"))

    assert "tiers" in roles, "dl380g10 is the box that owns the stage dataset"
    assert supervise.ROLE_SCRIPTS["tiers"] == "tier_loop.py"
    # The pool whose receipts the fill bandwidth is learned from, plus the
    # explicit 5 s minimum cycle interval #873 set for short campaign quanta.
    # Every capacity and price is still re-read each cycle.
    assert roles["tiers"] == ["--source-pool", "storage_pool",
                              "--interval-s", "5"]
    # And the storage role it runs beside is untouched.
    assert "storage" in roles


def test_the_tiers_role_arguments_are_ones_the_loop_accepts() -> None:
    """A role config the script refuses is a loop that dies on every respawn."""

    import tier_loop

    roles = dict(supervise.declared_roles("dl380g10"))
    # Drive the real entry point far enough to parse, then stop: --once would
    # discover this box's tiers, and a test may not mint anything.  The stop is
    # a deliberate refusal on a value the parser accepts, so that an argument
    # the parser *rejects* cannot pass this test by raising the same SystemExit
    # argparse raises on an unknown flag -- the message is what separates them.
    with pytest.raises(SystemExit) as accepted:
        tier_loop.main([*roles["tiers"], "--interval-s", "0"])
    assert "--interval-s" in str(accepted.value)

    with pytest.raises(SystemExit) as rejected:
        tier_loop.main([*roles["tiers"], "--no-such-flag"])
    assert "--interval-s" not in str(rejected.value)
