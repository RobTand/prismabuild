"""One physical declared output has one exclusion identity.

Issue #73: `_local_output_lock` hashed the resolved checkout root alongside the
declared output.  Two supported spellings of the same file,
`checkout_root=/repo` with `working_directory=sub` and
`checkout_root=/repo/sub` with `working_directory=.`, therefore took two
different locks over `/repo/sub/result.bin`.  Both passed the absent-result
check before either wrote, and the loser's bytes ended up published under the
winner's deterministic action key.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from prismabuild import core as pb  # noqa: E402

# A budget, not a schedule.  Under the fix the writer that loses the race is
# blocked on the shared lock and never appears, so the loop below always ends
# on this budget rather than on the marker; on main it ends on the marker in
# well under a second, which is what makes the concurrent case a fail-before.
OVERLAP_BUDGET_S = 1.5


def _action(root: Path, working_directory: str, argv: list[str]) -> dict[str, object]:
    return pb.seal_action(
        {
            "schema": pb.ACTION_SCHEMA_V2,
            "task": {
                "definition_id": "tests/overlapping-roots",
                "definition_version": "v1",
                "task_class": "generation",
                "determinism": "deterministic",
                "artifact_family": "generic",
                "artifact_kind": "generic",
                "argv": argv,
                "working_directory": working_directory,
                "result_path": "result.bin",
            },
            "inputs": [],
            "code_closure": pb.build_code_closure(root, ["code.txt"]),
            "params": {},
            "environment": {"variables": {}, "toolchain": {}},
            "execution_scope": {
                "portability": "portable",
                "platform_key": None,
                "host_class": None,
            },
        }
    )


def _nested_checkout(tmp_path: Path) -> tuple[Path, Path]:
    outer = tmp_path / "checkout"
    inner = outer / "sub"
    inner.mkdir(parents=True)
    (outer / "code.txt").write_text("closure\n")
    (inner / "code.txt").write_text("closure\n")
    return outer, inner


def _lock_files(cas: pb.PrismaBuildCAS) -> set[str]:
    directory = cas.root / ".worker-locks"
    if not directory.is_dir():
        return set()
    return {path.name for path in directory.glob("*.lock")}


def test_overlapping_checkout_spellings_share_one_output_lock(
    tmp_path: Path,
) -> None:
    """The exclusion identity is the file, not the caller's spelling of it."""

    outer, inner = _nested_checkout(tmp_path)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")
    output = inner / "result.bin"

    with pb._local_output_lock(cas, outer, output):
        outer_locks = _lock_files(cas)
    with pb._local_output_lock(cas, inner, output):
        inner_locks = _lock_files(cas)

    assert len(outer_locks) == 1
    assert inner_locks == outer_locks


def test_distinct_outputs_keep_independent_locks(tmp_path: Path) -> None:
    """Unifying the identity must not serialize unrelated actions."""

    outer, inner = _nested_checkout(tmp_path)
    cas = pb.PrismaBuildCAS(tmp_path / "cas")

    with pb._local_output_lock(cas, outer, outer / "result.bin"):
        with pb._local_output_lock(cas, inner, inner / "result.bin"):
            assert len(_lock_files(cas)) == 2


def test_repair_uses_the_execution_output_lock(tmp_path: Path) -> None:
    """The repair path must not take a second lock over the same result."""

    outer, inner = _nested_checkout(tmp_path)
    cas_root = tmp_path / "cas"
    cas = pb.PrismaBuildCAS(cas_root)
    execution = _action(outer, "sub", [sys.executable, "-c", "pass"])
    with pb._local_output_lock(cas, outer, inner / "result.bin"):
        execution_locks = _lock_files(cas)
    assert len(execution_locks) == 1

    repair = _action(inner, ".", [sys.executable, "-c", "pass"])
    assert repair["action_key"] != execution["action_key"]
    with pytest.raises(pb.LocalActionError, match="recovery claim"):
        pb.repair_local_result(repair, cas_root=cas_root, checkout_root=inner)

    assert _lock_files(cas) == execution_locks


def test_overlapping_spellings_never_publish_each_others_bytes(
    tmp_path: Path,
) -> None:
    """The reported interleaving: A must not publish B's result bytes."""

    outer, inner = _nested_checkout(tmp_path)
    cas_root = tmp_path / "cas"
    wait = (
        "import pathlib, time\n"
        "def wait(name):\n"
        f"    deadline = time.monotonic() + {OVERLAP_BUDGET_S}\n"
        "    while time.monotonic() < deadline:\n"
        "        if pathlib.Path(name).exists():\n"
        "            return True\n"
        "        time.sleep(0.01)\n"
        "    return False\n"
    )
    # A waits for B to be inside its own execution before writing, so that on
    # the unfixed code both actions pass the absent-result check, and then
    # waits for B to overwrite its bytes.  Under the fix B is blocked on the
    # shared lock and neither wait ever succeeds, which is why each one is a
    # budget rather than a rendezvous.
    a_source = wait + (
        "pathlib.Path('a.started').touch()\n"
        "wait('b.started')\n"
        "pathlib.Path('result.bin').write_bytes(b'A')\n"
        "pathlib.Path('a.wrote').touch()\n"
        "wait('b.wrote')\n"
    )
    b_source = wait + (
        "pathlib.Path('b.started').touch()\n"
        "wait('a.wrote')\n"
        "pathlib.Path('result.bin').write_bytes(b'B')\n"
        "pathlib.Path('b.wrote').touch()\n"
    )
    a = _action(outer, "sub", [sys.executable, "-c", a_source])
    b = _action(inner, ".", [sys.executable, "-c", b_source])
    outcomes: dict[str, object] = {}

    def run(name: str, action: dict[str, object], root: Path) -> None:
        try:
            result = pb.run_local_action(
                action,
                cas_root=cas_root,
                checkout_root=root,
                timeout_seconds=8 * OVERLAP_BUDGET_S,
            )
            outcomes[name] = Path(str(result["payload_path"])).read_bytes()
        except BaseException as exc:                     # noqa: BLE001
            outcomes[name] = exc

    threads = [
        threading.Thread(target=run, args=("a", a, outer)),
        threading.Thread(target=run, args=("b", b, inner)),
    ]
    threads[0].start()
    deadline = time.monotonic() + 10 * OVERLAP_BUDGET_S
    while not (inner / "a.started").exists():
        assert time.monotonic() < deadline, "the first action never started"
        time.sleep(0.01)
    threads[1].start()
    for thread in threads:
        thread.join(timeout=20 * OVERLAP_BUDGET_S)
        assert not thread.is_alive()

    assert outcomes["a"] == b"A", (
        f"the first action published {outcomes['a']!r} under its own key"
    )
    # The second writer is excluded, so it meets the declared result the first
    # one left behind and is refused by the unclaimed-result rule.
    assert isinstance(outcomes["b"], pb.LocalActionError)
    assert "must be absent before execution" in str(outcomes["b"])
    assert len(_lock_files(pb.PrismaBuildCAS(cas_root))) == 1
