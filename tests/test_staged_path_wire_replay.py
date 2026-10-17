"""Replay actual old and current writers, private bytes, ledgers and locks."""
import json
from pathlib import Path
import subprocess
import sys


def test_original_and_current_staged_path_wire_and_ordered_effects(tmp_path, request):
    root = Path(__file__).resolve().parents[1]
    output = tmp_path / "replay.json"
    run = subprocess.run(
        [sys.executable, str(root / "tools/maintenance/staged_path_replay.py"), "--output", str(output)],
        capture_output=True, text=True)
    assert run.returncode == 0, run.stdout + run.stderr
    replay = json.loads(output.read_bytes())
    assert replay["wire_and_effects_equal"] is True
    assert replay["old"]["facts"] == replay["current"]["facts"]
    assert replay["old"]["wire_documents"] and replay["current"]["wire_documents"]
    assert replay["runtime"]["version"] == sys.version
    assert replay["runtime"]["executable"] == sys.executable
    assert output.with_suffix(".python-executable").read_bytes() == Path(sys.executable).read_bytes()
    reporter = request.config.pluginmanager.get_plugin("terminalreporter")
    reporter.write_line("c09-runtime: " + json.dumps(replay["runtime"]))
