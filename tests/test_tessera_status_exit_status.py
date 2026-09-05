"""What the screen does with a record it cannot read, and what it returns.

A status command is what an operator reaches for once something is already
wrong, so its two failure modes both matter. It must not be the thing that
crashes: one manifest that parses but carries no ``total_bytes`` used to reach
``main``'s totals and take the whole screen down with a ``KeyError``, so the
119 shards it could read printed nothing. And it must return something a
script can branch on: ``main`` returned 0 whatever it found, so an operator
reading only the exit code was told the fleet was fine while the screen said
it had read nothing at all.

The vocabulary is in the tool's ``--help`` epilog: 0 read everything, 1
printed but part of it could not be read, 3 nothing to read.
"""
from __future__ import annotations

import contextlib
import io
import json
from pathlib import Path
import sys

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

import tessera_status  # noqa: E402


def _store(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    """Every root this screen consults, present and empty."""

    queue = tmp_path / "pb-queue"
    cas = tmp_path / "cas"
    results = tmp_path / "results"
    for root in (queue, cas, results):
        root.mkdir()
    plan = tmp_path / "plan.json"
    plan.write_text('{"rung": 896}\n', encoding="utf-8")
    return queue, cas, results, plan


def _run(queue: Path, cas: Path, results: Path, plan: Path) -> tuple[int, str]:
    """The screen, over this test's store rather than the fleet's."""

    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        code = tessera_status.main([
            "--transport", "pool",
            "--queue-root", str(queue),
            "--cas-root", str(cas),
            "--results-root", str(results),
            "--plan", str(plan),
        ])
    return code, output.getvalue()


def _manifest(results: Path, shard: int, *, drop: str | None = None) -> Path:
    """One shard manifest of the shape the exporter writes."""

    body = {
        "shard": shard,
        "total_bytes": 16,
        "quantized_bytes": 8,
        "quantized_params": 32,
    }
    if drop is not None:
        del body[drop]
    path = results / f"shard-{shard:05d}.json"
    path.write_text(json.dumps(body), encoding="utf-8")
    return path


def test_a_manifest_missing_a_field_costs_that_shard_its_line_and_nothing_else(
    tmp_path: Path,
) -> None:
    """The defect: one short record, and no screen at all."""

    queue, cas, results, plan = _store(tmp_path)
    _manifest(results, 1)
    short = _manifest(results, 2, drop="total_bytes")

    code, printed = _run(queue, cas, results, plan)

    assert "shards     1/120 encoded   missing 119" in printed
    assert "skipped  entries that could not be read: 1" in printed
    # Named, so the operator does not go looking for which file it was.
    assert str(short) in printed
    # And the shard it could read still reaches the totals.
    assert "over 32 params" in printed
    assert code == tessera_status.EXIT_PARTIAL


def test_a_screen_that_read_everything_it_consulted_exits_zero(
    tmp_path: Path,
) -> None:
    """An export that has not started is a complete screen, not a failure."""

    queue, cas, results, plan = _store(tmp_path)
    _manifest(results, 1)

    code, printed = _run(queue, cas, results, plan)

    assert "skipped" not in printed
    assert code == tessera_status.EXIT_OK


def test_a_screen_with_no_store_to_read_says_so_and_exits_nonzero(
    tmp_path: Path,
) -> None:
    """Pointed at nothing, the screen used to print zeroes and exit 0.

    Every zero on that screen was true of a store that does not exist, which
    is exactly what makes it unreadable: a fleet with nothing queued and a
    root that was never mounted print the same thing.
    """

    absent = tmp_path / "no-such-store"
    code, printed = _run(
        absent / "pb-queue", absent / "cas", absent / "results",
        tmp_path / "no-such-plan.json")

    assert "missing    queue root" in printed
    assert "missing    CAS root" in printed
    assert "no root this screen reads exists" in printed
    assert code == tessera_status.EXIT_NOTHING
