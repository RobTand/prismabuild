"""Private profiling preserves real replay results and immutable source evidence."""
from contextlib import contextmanager
import base64
import gzip
import hashlib
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests/compat/support"))
import profile_record_compat as profile
import record_compat as replay


STAGES = {"compile_source", "profile_hook", "profile_calls", "setup",
          "adapter_with_attestation_hooks", "final_attestation"}
BRIDGE = "unpublished-bridge-6fa9b780304d"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot(directory):
    return {str(path.relative_to(directory)): digest(path)
            for path in directory.rglob("*") if path.is_file()
            and "__pycache__" not in path.parts and path.suffix != ".pyc"}


def golden(case, generation=None):
    pattern = "historical*.jsonl" if generation else "goldens*.jsonl"
    records = [json.loads(line) for path in (ROOT / "tests/compat").glob(pattern)
               for line in path.read_text().splitlines()]
    record = next(row for row in records if row["case"] == case
                  and row.get("generation") == generation)
    raw = base64.b64decode(record["raw"])
    assert hashlib.sha256(raw).hexdigest() == record["sha256"]
    return raw


@pytest.fixture(autouse=True)
def private_temporaries(tmp_path, monkeypatch):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr(replay, "_PRIOR_DRIVERS", None)
    monkeypatch.setattr(replay, "_C12_DRIVERS", None)
    yield
    for temporary in (replay._PRIOR_DRIVERS, replay._C12_DRIVERS):
        if temporary is not None:
            temporary.cleanup()


@pytest.fixture
def sources(tmp_path):
    with replay.retained() as generations:
        bridge = next(row for row in generations if row[0] == BRIDGE)
        batch = tmp_path / "batch-profile-source"
        shutil.copytree(bridge[1], batch)
        marker = batch / "src/prismabuild/batch_ledger.py"
        marker.write_text('"""Selection marker only; never imported by these replay cases."""\n')
        marker.chmod(0o444)
        manifest = json.loads(bridge[2].read_bytes())
        manifest["files"]["src/prismabuild/batch_ledger.py"] = digest(marker)
        manifest_path = tmp_path / "batch-profile-source.json"
        manifest_path.write_text(json.dumps(manifest))
        manifest_path.chmod(0o444)
        yield [("current", ROOT, None), *generations, ("batch-profile", batch, manifest_path)]


@pytest.mark.pbtest_capability("archival-replay")
def test_report_profiles_selected_drivers_and_keeps_golden_replay(sources, tmp_path, monkeypatch, capsys):
    selected_sources = [row for row in sources if row[0] in ("current", BRIDGE, "batch-profile")]
    assert [replay.owner_profile(root) for _, root, _ in selected_sources] == [
        "funding-v1", "produced-v1", "batch-v1"]
    original_here, original_selector = replay.HERE, replay.driver_for
    selected = {root: replay.driver_for(root) for _, root, _ in selected_sources}
    originals = {path: snapshot(path) for path in set(selected.values())}
    archives = {path: digest(path) for path in (ROOT / "tests/compat").rglob("*.tar.gz")}
    closures = {root: snapshot(root) for _, root, _ in selected_sources if root != ROOT}
    current = {root: snapshot(root) for root in (ROOT / "src", ROOT / "tools")}
    observed = {}
    invoke = replay.invoke

    def observe(root, case, store, **kwargs):
        result = invoke(root, case, store, **kwargs)
        observed[root, case] = result
        return result

    @contextmanager
    def retained():
        yield selected_sources[1:]

    monkeypatch.setattr(replay, "retained", retained)
    monkeypatch.setattr(replay, "invoke", observe)
    monkeypatch.setattr(sys, "argv", ["profile_record_compat.py", str(tmp_path / "report")])
    profile.profile_replays()
    report_path, = (tmp_path / "report").glob("profile-stages-*.json")
    rows = json.loads(report_path.read_text())
    assert json.loads(capsys.readouterr().out) == rows
    assert rows[0]["stage"] == "materialise_generations"
    assert len(rows) == 7
    for row in rows[1:]:
        assert set(row) == {"case", "generation", "seconds", "returncode", "error", "stages"}
        assert row["returncode"] == 0 and row["error"] is None, row
        assert set(row["stages"]) == STAGES
        assert row["stages"]["profile_calls"] > 0
        label, root, manifest = next(source for source in selected_sources if source[0] == row["generation"])
        expected = golden(row["case"], None if label == "current" else BRIDGE)
        result = observed[root, row["case"]]
        assert base64.b64decode(replay.accepted(result)["raw"]) == expected
        assert result["catalogued_entry_verified"] and result["executing_code_sha256"]
        assert result["driver_sha256"] != digest(selected[root] / "compat_isolate.py")
        ordinary = invoke(root, row["case"], tmp_path / "ordinary" / label / row["case"], manifest=manifest)
        assert base64.b64decode(replay.accepted(ordinary)["raw"]) == expected
        assert "profile_stages" not in ordinary
        assert ordinary["driver_sha256"] == digest(selected[root] / "compat_isolate.py")
        with profile.profiling_drivers(tmp_path / "readers" / label / row["case"], [root]):
            reader = invoke(root, row["case"], tmp_path / "read-store" / label / row["case"],
                            raw=expected, manifest=manifest)
        consumed = replay.accepted(reader)
        assert reader["catalogued_entry_verified"] and reader["consumer_boundary"]
        assert reader["profile_stages"]["profile_calls"] > 0
        if row["case"] == "shape-table/unhashed":
            assert consumed == json.loads(gzip.decompress(expected))
            assert consumed["source"]["total_bytes"] == 8
            assert len(consumed["entries"]) == 1
        else:
            assert consumed == json.loads(expected)["boxes"]["dl380g10"]
    assert replay.HERE == original_here and replay.driver_for is original_selector
    assert {root: replay.driver_for(root) for _, root, _ in selected_sources} == selected
    assert {path: snapshot(path) for path in originals} == originals
    assert {path: digest(path) for path in archives} == archives
    assert {root: snapshot(root) for root in closures} == closures
    assert {root: snapshot(root) for root in current} == current


@pytest.mark.pbtest_capability("archival-replay")
@pytest.mark.parametrize("failure", ["invoke", "instrument"])
def test_failure_restores_routing_and_source_bytes(sources, tmp_path, monkeypatch, failure):
    original_here, original_selector = replay.HERE, replay.driver_for
    selected = {root: replay.driver_for(root) for _, root, _ in sources}
    originals = {path: snapshot(path) for path in set(selected.values())}
    archives = {path: digest(path) for path in (ROOT / "tests/compat").rglob("*.tar.gz")}

    @contextmanager
    def retained():
        yield sources[1:]

    def fail(*args, **kwargs):
        raise RuntimeError("injected profiling failure")

    monkeypatch.setattr(replay, "retained", retained)
    monkeypatch.setattr(sys, "argv", ["profile_record_compat.py", str(tmp_path / "report")])
    if failure == "invoke":
        monkeypatch.setattr(replay, "invoke", fail)
    else:
        instrument = profile.instrument_driver

        def fail_historical(source):
            if source == (selected[sources[1][1]] / "compat_isolate.py").read_text():
                fail()
            return instrument(source)

        monkeypatch.setattr(profile, "instrument_driver", fail_historical)
    with pytest.raises(RuntimeError, match="injected profiling failure"):
        profile.profile_replays()
    assert replay.HERE == original_here and replay.driver_for is original_selector
    assert {root: replay.driver_for(root) for _, root, _ in sources} == selected
    assert {path: snapshot(path) for path in originals} == originals
    assert {path: digest(path) for path in archives} == archives


@pytest.mark.parametrize("driver", ["current", "pre-c12", "pre-c13"])
def test_stage_arithmetic_uses_known_clock_events(driver, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    directory = {"current": lambda: replay.HERE, "pre-c12": replay.pre_c12_drivers,
                 "pre-c13": replay.pre_c13_drivers}[driver]()
    path = directory / "compat_isolate.py"
    namespace = {"__name__": "clock_driver", "__file__": str(path)}
    exec(compile(profile.instrument_driver(path.read_text()), str(path), "exec"), namespace)
    source = tmp_path / "production.py"
    source.write_text("def reader():\n    return 7\n")
    production = {}
    exec(compile(source.read_bytes(), str(source), "exec", dont_inherit=True), production)
    frame = SimpleNamespace(f_code=production["reader"].__code__)
    hooks = {}

    def setprofile(callback):
        hooks["profile"] = callback

    def adapter(*args):
        hooks["profile"](frame, "call", None)
        hooks["profile"](frame, "return", 7)
        return 7

    clock_events = iter([100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210])
    namespace.update(
        time=SimpleNamespace(perf_counter=lambda: next(clock_events)),
        sys=SimpleNamespace(flags=sys.flags, path=[], executable=sys.executable, version=sys.version,
                            stdin=io.StringIO('{"case": "clock", "op": "write"}'),
                            setprofile=setprofile, settrace=lambda callback: None),
        os=SimpleNamespace(environ={}, sched_getaffinity=lambda pid: {0}, getpid=lambda: 1),
        isolate=lambda root: set(), private_writes_only=lambda store: None,
        sha=lambda path: "digest", attest=lambda *args: ({}, {}),
        runpy=SimpleNamespace(run_path=lambda path: {"operate": adapter}))
    monkeypatch.setattr(sys, "argv", ["driver", "--root", str(tmp_path), "--store", str(tmp_path / "store"),
                                      "--adapters", str(tmp_path / "adapter.py")])
    assert namespace["main"]() == 0
    output = json.loads(capsys.readouterr().out)
    assert output["profile_stages"] == {
        "compile_source": 10, "profile_hook": 40, "profile_calls": 2, "setup": 10,
        "adapter_with_attestation_hooks": 70, "final_attestation": 10}
    assert output["result"] == 7 and output["compiled_source_code_verified"]
    assert next(clock_events, None) is None


@pytest.mark.parametrize(("observed", "error"), [
    ({"returncode": 0}, "profiling stages unavailable or incomplete"),
    ({"returncode": 0, "profile_stages": {}}, "profiling stages unavailable or incomplete"),
    ({"returncode": 0, "profile_stages": dict.fromkeys(STAGES, 0)}, "profiling hook observed no calls"),
    ({"returncode": 1, "error": {"type": "ValueError", "message": "bad replay"}},
     {"type": "ValueError", "message": "bad replay"}),
    ({"returncode": 3, "entry_error": "entry missing"}, "entry missing"),
    ({"returncode": 2, "attestation_error": "source changed"}, "source changed"),
    ({"returncode": -9}, "profiling replay exited with status -9"),
])
def test_unavailable_and_failed_profiles_are_explicit(observed, error):
    row = profile.profile_row("config-fleet/source", "retained", observed, 3)
    assert row["error"] == error
    assert row["returncode"] == observed["returncode"]
    assert row["seconds"] == 3


def test_unknown_driver_layout_fails_closed():
    with pytest.raises(ValueError, match="requires one exact boundary"):
        profile.instrument_driver("def main():\n    pass\n")
