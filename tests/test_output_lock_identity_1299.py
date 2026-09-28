"""The output-lock exclusion identity has one owner (issue #1299).

``core._local_output_lock`` and ``tools/fleet/pb_gc.py::output_lock_name``
both named their ``.worker-locks`` file ``sha256(normpath(output))`` plus
``.lock``. The digest is now owned by ``core._output_lock_name``; ``pb_gc``
keeps its claim-to-path derivation (the caller's root names the output) and
calls the owner for the name. The three tests close the triangle: owner names
what the real lock creates, the gc derivation agrees with the owner over
every spelling of one file, and distinct files disagree.

Everything here is ``tmp_path``-only. Directory listings are filtered, never
asserted exact: the harness drops a ``live-guard`` file into ``tmp_path``.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "src"))
sys.path.insert(0, str(REPOSITORY / "tools" / "fleet"))

from prismabuild import core as pb  # noqa: E402

import pb_gc  # noqa: E402


def _claim(root: object, working: str, result: str) -> dict[str, object]:
    return {
        "checkout_root": str(root),
        "working_directory": working,
        "result_path": result,
    }


def test_owner_names_what_the_real_lock_creates(tmp_path: Path) -> None:
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    checkout = tmp_path / "checkout"
    (checkout / "sub").mkdir(parents=True)
    output = checkout / "sub" / "result.bin"
    with pb._local_output_lock(cas, checkout, output):
        created = [
            p.name
            for p in (cas.root / ".worker-locks").iterdir()
            if p.suffix == ".lock"
        ]
    assert created == [pb._output_lock_name(output)]


def test_gc_derivation_agrees_with_owner_over_spellings(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    (checkout / "sub").mkdir(parents=True)
    output = checkout / "sub" / "result.bin"
    expected = pb._output_lock_name(Path(os.path.realpath(output)))
    spellings = [
        _claim(checkout.resolve(), "sub", "result.bin"),
        _claim(checkout.resolve() / "sub", ".", "result.bin"),
        _claim(Path(str(checkout.resolve()) + "/"), "sub", "result.bin"),
        _claim(checkout.resolve(), "./sub", "result.bin"),
    ]
    for claim in spellings:
        assert pb_gc.output_lock_name(claim) == expected


def test_distinct_outputs_take_distinct_locks(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    (checkout / "sub").mkdir(parents=True)
    first = pb_gc.output_lock_name(
        _claim(checkout.resolve(), "sub", "result.bin"))
    second = pb_gc.output_lock_name(
        _claim(checkout.resolve(), "sub", "other.bin"))
    assert first != second
    assert first == pb._output_lock_name(
        Path(os.path.realpath(checkout / "sub" / "result.bin")))
    assert first.endswith(".lock")
