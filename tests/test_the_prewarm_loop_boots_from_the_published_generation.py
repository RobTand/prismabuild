"""The supervisor spawns the published copy, and that copy must import.

``publish_runtime`` writes every fleet tool twice: ``tools/fleet/<name>`` and
a flattened ``tools/<name>``.  ``supervise._spawn_role`` resolves a role's
script as ``<generation>/tools/<script>`` -- the flat spelling -- so the flat
copy is the one the storage role actually executes.

``prewarm_loop.py`` bootstrapped its package with
``Path(__file__).resolve().parents[2] / "src"``.  That is the repository root
from ``tools/fleet/`` and the *parent of the generation store* from
``tools/``, so every spawn under the published layout died immediately with
``ModuleNotFoundError: No module named 'prismabuild'`` while the supervisor
retried it every cycle.  ``runtime_paths.generation_root`` is the supported
answer and is what ``pbtest.py`` already uses.

This builds a real generation out of the publisher's own manifest, so it
tests the publisher's flattening rather than an imitation of it, and runs the
flat entry point as a subprocess the way the supervisor does.
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
_SPEC = importlib.util.spec_from_file_location(
    "publish_runtime", ROOT / "tools" / "fleet" / "publish_runtime.py"
)
publish_runtime = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(publish_runtime)  # type: ignore[union-attr]

sys.path.insert(0, str(ROOT / "tools" / "fleet"))

import supervise  # noqa: E402


def _release(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(publish_runtime, "CHECKOUT", ROOT)
    release = tmp_path / "runtime-generations" / "abcdef012345"
    for name in publish_runtime._publication_manifest():
        target = release / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(publish_runtime._source_for(name), target)
    return release


#: Run the entry point the way a spawn does, then report which ``prismabuild``
#: its own bootstrap actually resolved.  Import success alone would not say
#: that: an inherited ``PYTHONPATH`` or an installed copy answers the import
#: from outside the generation, and then the assertion under test is vacuous.
_PROBE = """
import io, runpy, sys
from contextlib import redirect_stdout
script, generation = sys.argv[1], sys.argv[2]
sys.argv = [script, "--help"]
out = io.StringIO()
try:
    with redirect_stdout(out):
        runpy.run_path(script, run_name="__main__")
except SystemExit as exit_code:
    if exit_code.code not in (0, None):
        raise
import prismabuild.core, prismabuild.pool
for module in (prismabuild.core, prismabuild.pool):
    if not module.__file__.startswith(generation):
        raise SystemExit(f"{module.__name__} came from {module.__file__}")
if "--mount-map" not in out.getvalue():
    raise SystemExit("the entry point printed no usage")
"""


@pytest.mark.parametrize("layout", ["tools", "tools/fleet"])
def test_the_published_entry_point_imports_the_package_beside_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, layout: str
) -> None:
    """Both published spellings bind to their own generation's ``src``."""

    release = _release(tmp_path, monkeypatch)
    script = release / layout / "prewarm_loop.py"
    assert script.is_file(), f"the publisher stopped writing {layout}/"
    # A worker box spawns the role out of a bare environment; this suite may
    # run under one that already points at a checkout, which would answer the
    # import for the wrong reason.
    environment = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    result = subprocess.run(
        [sys.executable, "-c", _PROBE, str(script), str(release)],
        cwd=tmp_path, capture_output=True, text=True, env=environment,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_the_script_the_storage_role_spawns_is_the_flat_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``_spawn_role`` names ``tools/<script>``, not ``tools/fleet/<script>``.

    Nothing is spawned: ``Popen`` is this test's own, and it records the argv
    the supervisor would have run.
    """

    release = _release(tmp_path, monkeypatch)
    monkeypatch.setattr(supervise, "MIRROR", release)
    monkeypatch.setattr(supervise, "LOG_DIR", tmp_path / "log")
    monkeypatch.setattr(supervise, "_current_root", lambda: release)
    recorded: list[list[str]] = []

    class _Proc:
        pid = 1000000021

    def _popen(argv, **_kwargs):
        recorded.append([str(part) for part in argv])
        return _Proc()

    monkeypatch.setattr(supervise.subprocess, "Popen", _popen)
    assert supervise._spawn_role("storage", ["--poll-s", "20"]) == _Proc.pid
    assert recorded == [[sys.executable,
                         str(release / "tools" / "prewarm_loop.py"),
                         "--poll-s", "20"]]
