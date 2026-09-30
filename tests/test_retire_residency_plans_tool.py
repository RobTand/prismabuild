"""The #1041 backfill tool: dry run by default, --apply archives.

#1381: the tool is an operator command, so it travels in the published
generation and must boot from the one that contains it -- both the flat
``tools/`` spelling a box without a checkout runs and the nested
``tools/fleet/`` copy.  Its original ``parents[2] / "src"`` bootstrap was
the repository root from a checkout but the parent of the generation store
from the flat published copy, so an import there died before argparse
printed anything.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools" / "fleet"))

from prismabuild import pool  # noqa: E402
import pbrun  # noqa: E402
import retire_residency_plans as tool  # noqa: E402
import test_pbrun_residency_stage_submission as submission  # noqa: E402
from test_pbrun_residency_stage_submission import _detach_key  # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "publish_runtime", ROOT / "tools" / "fleet" / "publish_runtime.py"
)
publish_runtime = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(publish_runtime)  # type: ignore[union-attr]


def test_dry_run_changes_nothing_and_apply_archives_a_concluded_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    prepared = submission._prepare(tmp_path, monkeypatch)
    queue = prepared["queue"]
    assert pbrun.main() == 0
    key = _detach_key(capsys)
    # A live (ready) consumer is not a candidate.
    assert tool.candidates(queue) == []
    # Conclude it the old way: the terminal file appears with no plan retirement.
    queue.item_path(pool.READY, key).unlink()
    assert tool.candidates(queue) == [key]

    assert tool.retire_concluded_plans(queue, apply=False)["would_retire"] == [key]
    assert queue.residency_plan_path(key).exists()

    assert tool.retire_concluded_plans(queue, apply=True)["retired"] == [key]
    assert not queue.residency_plan_path(key).exists()
    assert tool.candidates(queue) == []


def _synthetic_queue(tmp_path: Path) -> tuple[Path, Path]:
    """A queue root holding one filed plan whose consumer is already concluded."""

    queue_root = tmp_path / "queue"
    plans = queue_root / "residency-plans"
    plans.mkdir(parents=True)
    filed = plans / "synthetic.json"
    filed.write_text("{}\n")
    return queue_root, filed


def _published_release(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Materialize the publisher's own manifest the way a generation is."""

    monkeypatch.setattr(publish_runtime, "CHECKOUT", ROOT)
    release = tmp_path / "runtime-generations" / "abcdef012345"
    for name in publish_runtime._publication_manifest():
        target = release / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(publish_runtime._source_for(name), target)
    return release


#: Run the entry point the way a box without a checkout does, then report
#: which ``prismabuild`` its own bootstrap actually resolved.  Import success
#: alone would not say that: an inherited ``PYTHONPATH`` or an installed copy
#: answers the import from outside the generation, and the check under test
#: would be vacuous.  The dry run over a synthetic queue root exercises the
#: real CLI, and it writes nothing there.
_PROBE = """
import io, json, runpy, sys
from contextlib import redirect_stdout
from pathlib import Path
script, expected_src, queue_root = sys.argv[1], sys.argv[2], sys.argv[3]
sys.argv = [script, "--queue", queue_root]
out = io.StringIO()
try:
    with redirect_stdout(out):
        runpy.run_path(script, run_name="__main__")
except SystemExit as exit_code:
    if exit_code.code not in (0, None):
        raise
import prismabuild.pool, prismabuild.residency_plan
for module in (prismabuild.pool, prismabuild.residency_plan):
    if not Path(module.__file__).is_relative_to(expected_src):
        raise SystemExit(f"{module.__name__} came from {module.__file__}")
record = json.loads(out.getvalue())
if record["keys"] != {"retired": [], "kept": [], "would_retire": ["synthetic"]}:
    raise SystemExit(f"the dry run reported {record!r}")
"""


@pytest.mark.parametrize("layout", ["tools", "tools/fleet"])
def test_the_published_entry_point_boots_from_its_own_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, layout: str,
) -> None:
    """Both published spellings bind to the generation that carries them."""

    release = _published_release(tmp_path, monkeypatch)
    script = release / layout / "retire_residency_plans.py"
    assert script.is_file(), f"the publisher stopped writing {layout}/"
    queue_root, filed = _synthetic_queue(tmp_path)
    # A worker box runs this from a bare environment; this suite exports
    # PYTHONPATH for its own subprocesses, which would answer the import for
    # the wrong reason.
    environment = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    result = subprocess.run(
        [sys.executable, "-c", _PROBE, str(script), str(release / "src"),
         str(queue_root)],
        cwd=tmp_path, capture_output=True, text=True, env=environment,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    # A dry run changed no filed state.
    assert filed.read_text() == "{}\n"


def test_the_checkout_entry_point_boots_from_the_checkout_src(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The repository's own layout keeps working from outside the tree."""

    script = ROOT / "tools" / "fleet" / "retire_residency_plans.py"
    queue_root, filed = _synthetic_queue(tmp_path)
    environment = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    result = subprocess.run(
        [sys.executable, "-c", _PROBE, str(script), str(ROOT / "src"),
         str(queue_root)],
        cwd=tmp_path, capture_output=True, text=True, env=environment,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert filed.read_text() == "{}\n"
