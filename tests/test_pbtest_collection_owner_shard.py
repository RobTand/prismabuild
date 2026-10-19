"""A sealed shard binds its recorder to its embedded collection helper."""
from __future__ import annotations

import os
import subprocess
import sys

import pytest

from test_digest_sites_1547 import fleet_module


@pytest.mark.parametrize("target", ["without_package", "hostile_package"])
def test_real_shard_uses_the_embedded_collection_owner(tmp_path, target):
    pbtest = fleet_module("pbtest")
    checkout = tmp_path / "consumer"
    checkout.mkdir()
    (checkout / "pytest.ini").write_text("[pytest]\n")
    if target == "hostile_package":
        package = checkout / "prismabuild"
        package.mkdir()
        (package / "__init__.py").write_text("")
        (package / "pbtest_collection.py").write_text(
            "ON_DISK = True\n"
            "def ignored_named_paths(*args, **kwargs):\n"
            "    raise AssertionError('on-disk collection bypassed sealed helper')\n"
        )
        package_control = (
            "    import prismabuild.pbtest_collection as on_disk\n"
            "    assert on_disk.ON_DISK is True\n"
            "    assert on_disk is not embedded\n"
            "    assert on_disk.ignored_named_paths is not recorder.ignored_named_paths\n"
        )
    else:
        package_control = (
            "    assert importlib.util.find_spec('prismabuild') is None\n"
            "    assert 'prismabuild.pbtest_collection' not in sys.modules\n"
        )
    (checkout / "test_owner.py").write_text(
        "import importlib.util\nimport sys\n"
        "def test_owner():\n"
        "    recorder = sys.modules['pbtest_outcomes']\n"
        "    embedded = sys.modules['pbtest_collection']\n"
        "    assert embedded.__file__ == '<pbtest pbtest_collection>'\n"
        "    assert recorder.ignored_named_paths is embedded.ignored_named_paths\n"
        "    assert recorder.ignored_named_paths.__code__.co_filename == embedded.__file__\n"
        + package_control
    )
    environment = dict(os.environ, PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
    environment.pop("PYTHONPATH", None)
    result = subprocess.run(
        [*pbtest.shard_entry(sys.executable, checkout), "-q", "test_owner.py"],
        cwd=checkout, env=environment, capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    recorder = fleet_module("pbtest_outcomes")
    outcomes = recorder.parse(result.stdout)
    assert outcomes is not None
    category_index = recorder.REPORT_FIELDS.index("category")
    assert [row[category_index] for row in outcomes["reports"]] == ["passed"]
