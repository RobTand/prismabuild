"""The attested standard-library half is identical to main and to shipped source."""
from __future__ import annotations

import ast
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from prismabuild import core, dependency_digest, digest_primitives as owner
from test_digest_sites_1547 import MAIN, ROOT, fleet_module, main_definitions


def old_core():
    return main_definitions("src/prismabuild/core.py", ["PrismaBuildError", "ActionContractError",
        "_canonical_bytes", "_sorted_json_bytes", "_sorted_lf_bytes", "canonical_sha256",
        "raw_sha256", "stream_sha256"])


def test_owner_imports_only_standard_library_and_core_reexports_the_same_objects():
    for node in ast.walk(ast.parse(Path(owner.__file__).read_text())):
        if isinstance(node, ast.ImportFrom):
            assert node.level == 0
            assert node.module.split(".")[0] in sys.stdlib_module_names
        elif isinstance(node, ast.Import):
            assert all(alias.name.split(".")[0] in sys.stdlib_module_names for alias in node.names)
    for name in ("PrismaBuildError", "ActionContractError", "_canonical_bytes", "_sorted_json_bytes",
                 "_sorted_lf_bytes", "canonical_sha256", "raw_sha256", "stream_sha256"):
        assert getattr(core, name) is getattr(owner, name)


def test_fixed_corpus_matches_main_and_real_isolated_shard(tmp_path):
    old = old_core()
    values = [{"unicode": "café λ", "floats": [1.25, -0.0, 1e-12],
               "nested": [[None, True], {"z": 3, "a": "\n"}]}, [], {"empty": {}}]
    raw = b"raw\x00\xff\n"
    stream = tmp_path / "payload.bin"
    stream.write_bytes(b"native\x00payload" * 200000)
    expected = {"canonical": [old.canonical_sha256(value) for value in values],
                "raw": old.raw_sha256(raw), "stream": old.stream_sha256(stream)}
    assert [owner.canonical_sha256(value) for value in values] == expected["canonical"]
    assert owner.raw_sha256(raw) == expected["raw"]
    assert owner.stream_sha256(stream) == expected["stream"]
    for value in values:
        assert owner._canonical_bytes(value) == old._canonical_bytes(value)
    mapping = {"z": "λ", "a": [1.25, None]}
    assert owner._sorted_lf_bytes(mapping) == old._sorted_lf_bytes(mapping)
    pbtest = fleet_module("pbtest")
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    (tmp_path / "test_shipped.py").write_text(
        "import sys\nfrom pathlib import Path\n"
        f"from {pbtest.SHARD_DIGEST_MODULE} import canonical_sha256, raw_sha256, stream_sha256\n"
        f"def test_exact_owner():\n"
        f"    assert [canonical_sha256(v) for v in {values!r}] == {expected['canonical']!r}\n"
        f"    assert raw_sha256({raw!r}) == {expected['raw']!r}\n"
        f"    assert stream_sha256(Path('payload.bin')) == {expected['stream']!r}\n"
        "    assert 'prismabuild.core' not in sys.modules\n")

    command = pbtest.shard_entry(sys.executable, tmp_path)
    command.insert(1, "-I")
    result = subprocess.run([*command, "-q", "-p", "no:cacheprovider", "test_shipped.py"],
                            cwd=tmp_path, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "1 passed" in result.stdout
    assert "pbtest-outcomes:" in result.stdout


@pytest.mark.parametrize("offset,length", [(0, None), (7, 1048593), (10, 0), (10000000, None)])
def test_stream_ranges_and_dependency_file_match_main(tmp_path, offset, length):
    path = tmp_path / "bytes.bin"
    path.write_bytes(b"native\x00payload" * 200000)
    assert owner.stream_sha256(path, offset=offset, length=length) == old_core().stream_sha256(path, offset=offset, length=length)
    old = main_definitions("src/prismabuild/dependency_digest.py", ["digest_file"], _READ_CHUNK=1 << 20)
    assert dependency_digest.digest_file(str(path)) == old.digest_file(str(path))


def test_capability_evidence_bytes_match_main_without_changing_its_checks(monkeypatch, capsys):
    evidence = {"names": ["λ"], "files": [{"path": "/a", "sha256": "a" * 64}], "distributions": []}
    old = main_definitions("src/prismabuild/dependency_digest.py", ["verify_sealed"],
        sys=sys, subprocess=subprocess, EVIDENCE_PREFIX=dependency_digest.EVIDENCE_PREFIX,
        validate_sealed=lambda value: value, observe_sealed=lambda value: evidence)
    monkeypatch.setattr(dependency_digest, "validate_sealed", lambda value: value)
    monkeypatch.setattr(dependency_digest, "observe_sealed", lambda value: evidence)
    assert old.verify_sealed({"capabilities": {}}) == 0
    expected = capsys.readouterr().out
    assert dependency_digest.verify_sealed({"capabilities": {}}) == 0
    assert capsys.readouterr().out == expected


@pytest.mark.parametrize("algorithm", ["sha256", "sha384", "sha512"])
def test_installed_record_hash_and_result_match_main(tmp_path, monkeypatch, algorithm):
    pins = fleet_module("pbtest_pins")
    path = tmp_path / "module.py"
    path.write_bytes(b"installed\x00bytes" * 100000)
    expected = base64.urlsafe_b64encode(hashlib.new(algorithm, path.read_bytes()).digest()).rstrip(b"=").decode()
    entry = SimpleNamespace(hash=SimpleNamespace(mode=algorithm, value=expected))
    dist = SimpleNamespace(files=[entry], locate_file=lambda value: path,
        read_text=lambda name: json.dumps({"vcs_info": {"vcs": "git", "commit_id": "a" * 40}}))
    monkeypatch.setattr(pins.metadata, "packages_distributions", lambda: {"fixture": ["fixture-dist"]})
    monkeypatch.setattr(pins.metadata, "distribution", lambda name: dist)
    monkeypatch.setattr(pins.importlib.util, "find_spec", lambda name:
        SimpleNamespace(origin=str(path), submodule_search_locations=[]))
    old = main_definitions("tools/fleet/pbtest_pins.py", ["verify_install"],
                          metadata=pins.metadata, importlib=pins.importlib, base64=base64)
    assert pins.verify_install("fixture", "a" * 40) == old.verify_install("fixture", "a" * 40)


def test_trace_long_node_digest_and_bytes_match_main(monkeypatch):
    import contextlib
    outcomes = fleet_module("pbtest_outcomes")
    source = subprocess.run(["git", "show", f"{MAIN}:tools/fleet/pbtest_outcomes.py"],
                            cwd=ROOT, check=True, text=True, capture_output=True).stdout
    trace = next(node for node in ast.walk(ast.parse(source))
                 if isinstance(node, ast.FunctionDef) and node.name == "trace")
    old_namespace = {"hashlib": hashlib, "json": json, "time": outcomes.time,
                     "sys": sys, "nullcontext": contextlib.nullcontext, "trace_enabled": True,
                     "TRACE_NODEID_MAX_BYTES": outcomes.TRACE_NODEID_MAX_BYTES,
                     "TRACE_PREFIX": outcomes.TRACE_PREFIX, "TRACE_SCHEMA": outcomes.TRACE_SCHEMA}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[trace], type_ignores=[])), "main trace", "exec"), old_namespace)
    lines = []
    terminal = SimpleNamespace(write=lambda text, **kwargs: lines.append(text))
    config = SimpleNamespace(pluginmanager=SimpleNamespace(getplugin=lambda name: terminal))
    nodeid = "tests/test_fixture.py::test_" + "λ" * outcomes.TRACE_NODEID_MAX_BYTES
    monkeypatch.setattr(outcomes.time, "time", lambda: 123.25)
    old_namespace["trace"](SimpleNamespace(config=config), "start", nodeid)
    expected = lines.pop()

    def run_pytest(argv, plugins):
        recorder = plugins[0]
        recorder.config = config
        recorder.trace("start", nodeid)
        return 0

    monkeypatch.setattr(pytest, "main", run_pytest)
    assert outcomes.main(["--pbtest-trace"], resource_source="") == 0
    assert expected in lines


def test_shipped_owner_bytes_equal_the_loaded_package_module():
    pbtest = fleet_module("pbtest")
    from pathlib import Path as _Path
    import ast as _ast
    entry = pbtest.shard_entry(sys.executable, _Path("."), collection=False)
    program = entry[-1]
    tree = _ast.parse(program, mode="exec")
    sources = next(node.value for node in tree.body
                   if isinstance(node, ast.Assign) and node.targets[0].id == "SOURCES")
    from ast import literal_eval
    assert pbtest.SHARD_DIGEST_MODULE != "prismabuild.digest_primitives"
    shipped = literal_eval(sources)[pbtest.SHARD_DIGEST_MODULE]
    loaded = Path(core.digest_primitives.__file__).read_bytes()
    assert shipped.encode("utf-8") == loaded
    assert hashlib.sha256(shipped.encode("utf-8")).hexdigest() == hashlib.sha256(loaded).hexdigest()


@pytest.mark.parametrize("part", ["core", "digest_primitives"])
def test_either_owner_file_changes_the_recorded_runtime_identity(tmp_path, monkeypatch, part):
    paths = {name: tmp_path / (name + ".py") for name in ("core", "digest_primitives")}
    for name, path in paths.items():
        path.write_text(f"# loaded {name}\n")
    names = {"core": "_LOADED_WORKER_CORE_IDENTITY", "digest_primitives": "_LOADED_WORKER_DIGEST_IDENTITY"}
    for name, path in paths.items():
        monkeypatch.setattr(core, names[name], core._identify_runtime_source(path, where=name))
    first = core._worker_runtime_identity(None)
    paths[part].write_text(f"# changed {part}\n")
    monkeypatch.setattr(core, names[part], core._identify_runtime_source(paths[part], where=part))
    second = core._worker_runtime_identity(None)
    assert first[part]["sha256"] != second[part]["sha256"]
    assert first["runtime_sha256"] != second["runtime_sha256"]
    assert core._validate_worker_runtime(second) == second


def test_retained_presplit_runtime_body_is_not_rewritten():
    value = core._worker_runtime_identity(None)
    value.pop("digest_primitives")
    value["runtime_sha256"] = core.canonical_sha256({key: item for key, item in value.items() if key != "runtime_sha256"})
    assert core._validate_worker_runtime(value) == value
