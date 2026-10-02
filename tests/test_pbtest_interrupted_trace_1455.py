"""Small real shards retain diagnostics through abrupt exits (#1455/#1421)."""
from __future__ import annotations

import ast
import json
import inspect
from pathlib import Path
import subprocess
import sys

import pytest

import pbtest


def _run(tmp_path: Path, tests: str, arguments: list[str], *, trace=True,
         unreadable_io=False):
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    (tmp_path / "test_fixture.py").write_text(tests)
    # An older sealer can still run the requested pytest option, which must
    # fail as unsupported rather than turn the RED into a helper TypeError.
    options = {"trace": True} if trace and "trace" in inspect.signature(
        pbtest.shard_entry).parameters else {}
    entry = pbtest.shard_entry(sys.executable, tmp_path, **options)
    if unreadable_io:
        # Inject unavailable counters into the sealed helper, before executing
        # the real controller/worker plugin. Unknown must never turn into zero.
        program = entry[-1]
        entry[-1] = program.replace(
            'pins = load("pbtest_pins")',
            'SOURCES["pbtest_resource_scope"] += '
            '"\\ndef read_process_io(pid):\\n    return None\\n"\n'
            'pins = load("pbtest_pins")')
    command = [*entry, *([] if "no:terminal" in arguments else ["-q"]),
               "-p", "no:cacheprovider", *arguments,
               *(["--pbtest-trace"] if trace else []), "test_fixture.py"]
    return subprocess.run(command, cwd=tmp_path, text=True, capture_output=True,
                          timeout=120)


def _events(output: str) -> list[dict]:
    prefix = "pbtest-trace: "
    return [json.loads(line[len(prefix):]) for line in output.splitlines()
            if line.startswith(prefix)]


@pytest.mark.parametrize("capture", ["fd", "sys"])
@pytest.mark.parametrize("terminal", [True, False])
def test_current_test_survives_abrupt_controller_exit(tmp_path, capture, terminal):
    result = _run(tmp_path, "import os\n\ndef test_dies():\n    os._exit(86)\n",
                  [f"--capture={capture}", *([] if terminal else ["-p", "no:terminal"])])
    assert result.returncode == 86, result.stdout + result.stderr
    events = _events(result.stdout)
    assert events and events[0]["event"] == "start", result.stdout + result.stderr
    assert events[0]["nodeid"] == "test_fixture.py::test_dies"
    setup = [event for event in events if event.get("when") == "setup"]
    assert len(setup) == 1 and setup[0]["resources"] is not None
    assert not any(event.get("when") == "call" for event in events)
    assert pbtest.pbtest_outcomes.parse(result.stdout) is None


@pytest.mark.parametrize("workers", [1, 2])
def test_resources_follow_the_test_process_and_outcomes_still_reconcile(tmp_path, workers, capsys):
    if workers > 1:
        import xdist  # Required scoped test dependency; do not silently skip.
    result = _run(tmp_path, '''import os
from pathlib import Path
import pytest

@pytest.mark.parametrize("n", [1, 2])
def test_writes(tmp_path, n):
    with (tmp_path / "bytes").open("wb") as handle:
        handle.write(b"x" * 4096)
        handle.flush()
        os.fsync(handle.fileno())
''', ([] if workers == 1 else ["-n", str(workers)]))
    assert result.returncode == 0, result.stdout + result.stderr
    events = _events(result.stdout)
    assert len(events) == 8
    calls = [event for event in events if event.get("when") == "call"]
    assert len(calls) == 2
    for call in calls:
        resources = call["resources"]
        assert resources["process_io_delta"]["wchar"] >= 4096
        assert resources["process_io_delta"]["write_bytes"] >= 0
        after = resources["after"]
        assert after["rss_bytes"] > 0 and after["max_rss_watermark_bytes"] > 0
        assert after["scope"] == "test-process-and-reaped-children"
        assert not after["errors"]
    if workers > 1:
        assert {call["worker"] for call in calls} == {"gw0", "gw1"}
        assert len({call["resources"]["after"]["pid"] for call in calls}) == 2
    with capsys.disabled():
        print("pbtest-trace-evidence: " + json.dumps({
            "workers_requested": workers, "actual_call_reports": calls},
            separators=(",", ":")), flush=True)
    record = pbtest.pbtest_outcomes.parse(result.stdout)
    assert pbtest.pbtest_outcomes.reconcile(record, {"passed": 2})["problems"] == []


def test_a_crashed_xdist_worker_retains_prior_phase_without_faking_resources(tmp_path):
    import xdist
    result = _run(tmp_path, '''import os

def test_dies():
    os._exit(86)

def test_survives():
    pass
''', ["-n", "2", "--max-worker-restart=0"])
    assert result.returncode == 1, result.stdout + result.stderr
    events = [event for event in _events(result.stdout)
              if event["nodeid"] == "test_fixture.py::test_dies"]
    assert events[0]["event"] == "start"
    assert any(event.get("when") == "setup" and event["resources"] is not None
               for event in events)
    failed = next(event for event in events if event.get("outcome") == "failed")
    assert failed["resources"] is None
    record = pbtest.pbtest_outcomes.parse(result.stdout)
    assert any(row[2] == "failed" for row in record["reports"])


def test_oversized_nodeids_are_capped_and_keep_an_exact_digest(tmp_path):
    import hashlib
    name = "x" * 5000
    result = _run(tmp_path, "import pytest\n@pytest.mark.parametrize('n', [1], ids=["
                  + repr(name) + "])\ndef test_long(n):\n    pass\n", [])
    assert result.returncode == 0, result.stdout + result.stderr
    event = _events(result.stdout)[0]
    full = "test_fixture.py::test_long[" + name + "]"
    assert len(event["nodeid"].encode()) <= 4096
    assert event["nodeid_truncated"] is True
    assert event["nodeid_sha256"] == hashlib.sha256(full.encode()).hexdigest()


def test_unreadable_io_is_unknown_and_retains_its_error(tmp_path):
    result = _run(tmp_path, "def test_passes():\n    pass\n", [], unreadable_io=True)
    assert result.returncode == 0, result.stdout + result.stderr
    resources = _events(result.stdout)[2]["resources"]
    assert resources["process_io_delta"] is None
    assert resources["after"]["process_io"] is None
    assert "process I/O unavailable" in resources["after"]["errors"]


def test_opt_out_has_no_trace_and_needs_no_accounting_helper(tmp_path):
    entry = pbtest.shard_entry(sys.executable, tmp_path)
    assignment = next(node for node in ast.parse(entry[-1]).body
                      if isinstance(node, ast.Assign) and any(
                          isinstance(target, ast.Name) and target.id == "SOURCES"
                          for target in node.targets))
    assert "pbtest_resource_scope" not in ast.literal_eval(assignment.value)
    result = _run(tmp_path, "def test_passes():\n    pass\n", [], trace=False)
    assert result.returncode == 0, result.stdout + result.stderr
    assert not _events(result.stdout)
    assert pbtest.pbtest_outcomes.parse(result.stdout)["reports"] == [
        ["test_fixture.py::test_passes", "call", "passed", None, None]]


def test_trace_is_an_explicit_closed_reporting_option():
    assert pbtest.parse_pytest_args('["--pbtest-trace"]', gpu=False, workers=1) == [
        "--pbtest-trace"]


def test_submission_seals_trace_and_the_existing_accounting_owner(tmp_path, monkeypatch):
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_ok.py").write_text("def test_ok():\n    pass\n")
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    real_popen = subprocess.Popen
    sealed = []

    def admitted_payload(command, **kwargs):
        if str(pbtest.PBRUN) not in [str(part) for part in command]:
            return real_popen(command, **kwargs)
        sealed.append(command)
        return real_popen(command[command.index("--") + 1:], cwd=tmp_path, **kwargs)

    monkeypatch.setattr(pbtest.subprocess, "Popen", admitted_payload)
    monkeypatch.setattr(sys, "argv", [
        "pbtest.py", "--checkout", str(tmp_path), "--python", sys.executable,
        "--shards", "1", "--threads-per-shard", "1", "--json",
        str(tmp_path / "report.json"), "--pytest-args", '["--pbtest-trace"]', "tests"])
    assert pbtest.main() == 0
    assert len(sealed) == 1 and "--pbtest-trace" in sealed[0]
    report = json.loads((tmp_path / "report.json").read_text())[0]
    assert report["reconciliation"]["problems"] == []
    assert len(_events(report["output"])) == 4
