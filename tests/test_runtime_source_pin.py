"""The compatible runtime body still binds and checks the executed file owner."""
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
from pin_runtime_source import pin


def test_changed_helper_requires_a_new_attested_core_before_it_can_load(tmp_path):
    package = tmp_path / "src/prismabuild"
    package.mkdir(parents=True)
    for name in ("core.py", "immutable_files.py", "digest_primitives.py"):
        (package / name).write_bytes((ROOT / "src/prismabuild" / name).read_bytes())
    original = pin(tmp_path)
    owner = package / "immutable_files.py"
    owner.write_bytes(owner.read_bytes() + b"\n# changed extracted owner\n")
    with pytest.raises(ValueError, match="regenerate"):
        pin(tmp_path)
    command = [sys.executable, "-I", "-S", "-c", "import runpy,sys; runpy.run_path(sys.argv[1])", str(package / "core.py")]
    refused = subprocess.run(command, capture_output=True, text=True)
    assert refused.returncode != 0 and "differs from Core's pinned source" in refused.stderr
    replacement = pin(tmp_path, write=True)
    assert replacement != original and pin(tmp_path) == replacement
    accepted = subprocess.run(command, capture_output=True, text=True)
    assert accepted.returncode == 0, accepted.stderr
