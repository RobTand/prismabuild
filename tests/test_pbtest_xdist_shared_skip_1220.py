"""Two modules that skip through one helper both reconcile under xdist (#1220).

xdist's controller reports one collection failure per distinct ``longrepr``.
A module-level skip raised inside a shared helper carries the helper's
location, so the second module's skip is dropped and the shard used to say
its file "had no collection or outcome".
"""
from pathlib import Path
import json
import re
import subprocess
import sys

import pytest

FLEET = Path(__file__).resolve().parents[1] / "tools/fleet"
sys.path.insert(0, str(FLEET))
import pbtest  # noqa: E402

DRIVER = """\
import sys
sys.path.insert(0, {fleet!r})
import pbtest_outcomes
raise SystemExit(pbtest_outcomes.main({argv!r}))
"""


def test_shared_helper_skips_are_both_reconciled(tmp_path):
    pytest.importorskip("xdist")
    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / "helper.py").write_text(
        "import pytest\n\n"
        "def require(name):\n"
        "    pytest.skip(name + ' missing', allow_module_level=True)\n")
    for name in ("a", "b"):
        (tests / f"test_{name}.py").write_text(
            "import helper\nhelper.require('box')\n\n"
            "def test_x():\n    pass\n")
    (tests / "test_ok.py").write_text("def test_ok():\n    pass\n")
    files = ["tests/test_a.py", "tests/test_b.py", "tests/test_ok.py"]
    argv = ["-q", "-p", "no:cacheprovider", "-n", "2",
            "--import-mode=prepend", *files]
    proc = subprocess.run(
        [sys.executable, "-c", DRIVER.format(fleet=str(FLEET), argv=argv)],
        cwd=tmp_path, capture_output=True, text=True, timeout=180)
    lines = [ln for ln in proc.stdout.splitlines()
             if ln.startswith("pbtest-outcomes: ")]
    assert lines, proc.stdout + proc.stderr
    summary = next(ln for ln in reversed(proc.stdout.splitlines())
                   if re.search(r" in [\d.]+s", ln))
    result = {"ran": True, "shard": 0, "files": files, "summary": summary,
              "output": proc.stdout}
    pbtest.reconcile_shards([result])
    assert result["reconciliation"]["missing_files"] == [], (
        result["reconciliation"], json.loads(lines[-1][17:]))
