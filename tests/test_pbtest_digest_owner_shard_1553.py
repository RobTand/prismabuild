"""A real pbtest shard must leave the tested package's owner importable."""
from __future__ import annotations

import subprocess
import sys

import pytest

from test_digest_sites_1547 import ROOT, fleet_module


@pytest.mark.parametrize("route", ["test_import", "conftest_import", "checkout_plugins"])
def test_real_shard_keeps_the_package_digest_owner_on_disk(tmp_path, route):
    pbtest = fleet_module("pbtest")
    probe = (
        "from pathlib import Path\n"
        "import sys\n"
        f"sys.path.insert(0, {str(ROOT / 'src')!r})\n"
        "from prismabuild import core\n"
        "owner_path = Path(core.digest_primitives.__file__)\n"
        "assert owner_path.is_file()\n"
        f"assert owner_path.resolve() == Path({str(ROOT / 'src/prismabuild/digest_primitives.py')!r})\n"
        "assert core.digest_primitives is sys.modules['prismabuild.digest_primitives']\n"
        "assert core._worker_runtime_identity(None)['digest_primitives']['path'] == str(Path(core.digest_primitives.__file__).resolve())\n"
    )
    if route == "checkout_plugins":
        checkout = ROOT
        selected = "tests/test_core.py"
    else:
        checkout = tmp_path / "consumer"
        checkout.mkdir()
        (checkout / "pytest.ini").write_text("[pytest]\n")
        if route == "conftest_import":
            (checkout / "conftest.py").write_text(probe)
        test_source = ("" if route == "conftest_import" else probe) + (
            "import hashlib\n"
            "def test_imported_core():\n"
            "    from prismabuild import core\n"
            "    assert core.raw_sha256(b'real package import') == hashlib.sha256(b'real package import').hexdigest()\n"
        )
        (checkout / "test_imported_core.py").write_text(test_source)
        selected = "test_imported_core.py"
    argv = pbtest.shard_entry(sys.executable, checkout)
    result = subprocess.run(
        [*argv, "-q", "--tb=short", "-o", "tmp_path_retention_policy=failed",
         "--basetemp", str(tmp_path / "child-pytest"), selected],
        cwd=checkout, capture_output=True, text=True, timeout=240,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    recorder = fleet_module("pbtest_outcomes")
    outcomes = recorder.parse(result.stdout)
    assert outcomes is not None
    category_index = recorder.REPORT_FIELDS.index("category")
    categories = {row[category_index] for row in outcomes["reports"]}
    assert "passed" in categories
    assert categories.isdisjoint({"error", "failed"})
