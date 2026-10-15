"""Ordinary installed imports and role operations survive unavailable broad owners."""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import venv
import zipfile

import pytest


ROOT = Path(__file__).resolve().parents[1]
BROAD = ("core", "pool", "adaptive_cpu", "residency_map", "storage_tiers", "materialize")
ROLE_MODULES = {
    "prismabuild", "prismabuild.progress", "prismabuild.role_session",
    "prismabuild.generation_receipt", "prismabuild.park_proof",
    "prismabuild.digest_primitives", "prismabuild.immutable_files",
    "prismabuild.flock_owner",
}


def _run(command, *, cwd, label, timeout=90):
    removed = {name for name in os.environ if name.startswith("PIP_") or name in {
        "PYTHONPATH", "PYTHONHOME", "PYTHONUSERBASE", "VIRTUAL_ENV",
        "_PIP_RUNNING_IN_SUBPROCESS"}}
    environment = {name: value for name, value in os.environ.items() if name not in removed}
    environment.update(PIP_CONFIG_FILE=os.devnull, PYTHONNOUSERSITE="1", PIP_NO_INPUT="1")
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True,
                            timeout=timeout, env=environment)
    (cwd / f"{label}.stdout.txt").write_text(result.stdout)
    (cwd / f"{label}.stderr.txt").write_text(result.stderr)
    (cwd / f"{label}.json").write_text(json.dumps({
        "command": command, "returncode": result.returncode,
        "removed_environment_keys": sorted(removed)}, indent=2))
    assert result.returncode == 0, (
        f"{command!r} exited {result.returncode}\n{result.stdout}\n{result.stderr}")
    return result.stdout


@pytest.fixture(scope="module")
def installed(tmp_path_factory):
    root = tmp_path_factory.mktemp("ordinary-wheel")
    versions = {}
    for name in ("pip", "setuptools"):
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            pytest.fail(f"offline installed-package gate requires {name} in {sys.executable}")
    wheels = root / "wheels"
    wheels.mkdir()
    _run([sys.executable, "-I", "-m", "pip", "--disable-pip-version-check",
          "--no-cache-dir", "wheel", "--no-index", "--no-deps",
          "--no-build-isolation", "--wheel-dir", str(wheels), str(ROOT)],
         cwd=root, label="wheel-build")
    wheel, = wheels.glob("prismabuild-*.whl")
    with zipfile.ZipFile(wheel) as archive:
        source_bytes = {name: archive.read(name) for name in archive.namelist()
                        if name.startswith("prismabuild/") and name.endswith(".py")}
    hashes = {name: hashlib.sha256(raw).hexdigest() for name, raw in source_bytes.items()}
    for name, raw in source_bytes.items():
        assert raw == (ROOT / "src" / name).read_bytes(), name
    expected = root / "expected.json"
    expected.write_text(json.dumps(hashes))
    environments = {}
    for flavor in ("complete", "leaves"):
        environment = root / flavor
        venv.EnvBuilder(with_pip=False).create(environment)
        python = environment / "bin" / "python"
        scheme = json.loads(_run([str(python), "-I", "-B", "-c",
            "import json, sys, sysconfig; print(json.dumps({"
            "'prefix':sys.prefix, 'purelib':sysconfig.get_path('purelib'), "
            "'platlib':sysconfig.get_path('platlib')}))"],
            cwd=root, label=f"{flavor}-scheme"))
        assert Path(scheme["prefix"]).resolve() == environment.resolve(), scheme
        for name in ("purelib", "platlib"):
            assert Path(scheme[name]).resolve().is_relative_to(environment.resolve()), scheme
        _run([sys.executable, "-I", "-m", "pip", "--disable-pip-version-check",
              "--python", str(python), "install", "--no-index", "--no-deps",
              "--ignore-installed", "--no-compile", str(wheel)],
             cwd=root, label=f"{flavor}-install")
        proof = json.loads(_run([str(python), "-I", "-B", "-c",
            "import importlib.metadata as m, json; d=m.distribution('prismabuild'); "
            "print(json.dumps({'location':str(d.locate_file('')), 'version':d.version, "
            "'init':str(d.locate_file('prismabuild/__init__.py')), "
            "'installer':d.read_text('INSTALLER')}))"],
            cwd=root, label=f"{flavor}-installed-proof"))
        package = Path(scheme["purelib"]) / "prismabuild"
        assert Path(proof["location"]).resolve() == package.parent.resolve(), proof
        assert Path(proof["init"]).resolve() == (package / "__init__.py").resolve(), proof
        assert proof["version"] == "0.1.0" and proof["installer"].strip() == "pip", proof
        for name, raw in source_bytes.items():
            assert (package.parent / name).read_bytes() == raw, name
        if flavor == "leaves":
            quarantine = root / "unavailable"
            quarantine.mkdir()
            for name in BROAD:
                (package / f"{name}.py").rename(quarantine / f"{name}.py")
                assert not list(package.glob(f"__pycache__/{name}.*.pyc"))
        environments[flavor] = (python, package, expected)
    (root / "build-evidence.json").write_text(json.dumps({
        "packaging": versions, "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        "module_sha256": hashes}, indent=2))
    return environments


BOOTSTRAP = """\
import hashlib, importlib.metadata, json, sys
from pathlib import Path
assert sys.flags.isolated and sys.dont_write_bytecode
assert not any(n == 'prismabuild' or n.startswith('prismabuild.') for n in sys.modules)
package = Path(sys.argv[1]).resolve()
expected = json.loads(Path(sys.argv[2]).read_text())
work = Path(sys.argv[3])
assert not any(Path(p).resolve() == Path(sys.argv[4]).resolve() for p in sys.path if p)
assert importlib.metadata.version('prismabuild') == '0.1.0'
def origins():
    loaded = {}
    for name, module in tuple(sys.modules.items()):
        if name != 'prismabuild' and not name.startswith('prismabuild.'):
            continue
        relative = 'prismabuild/' + ('__init__' if name == 'prismabuild' else name[12:].replace('.', '/')) + '.py'
        path = package.parent / relative
        assert Path(module.__file__).resolve() == path.resolve(), (name, module.__file__)
        assert Path(module.__spec__.origin).resolve() == path.resolve(), name
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected[relative], name
        loaded[name] = str(path)
    assert sys.modules['prismabuild'].__spec__.loader.__class__.__name__ == 'SourceFileLoader'
    assert [Path(p).resolve() for p in sys.modules['prismabuild'].__path__] == [package]
    return loaded
"""


def _child(installed, flavor, work, body):
    work.mkdir()
    (work / "prismabuild.py").write_text("raise RuntimeError('cwd supplied the package')")
    python, package, expected = installed[flavor]
    result = subprocess.run(
        [str(python), "-I", "-B", "-c", BOOTSTRAP + textwrap.dedent(body),
         str(package), str(expected), str(work), str(ROOT / "src")],
        cwd=work, capture_output=True, text=True, timeout=30,
        env={**os.environ, "PYTHONPATH": str(work)})
    assert result.returncode == 0, result.stdout + result.stderr
    evidence = json.loads(result.stdout)
    (work / "loaded-origins.json").write_text(json.dumps(evidence, indent=2))
    return evidence


@pytest.mark.parametrize("leaf", ["", "generation_receipt", "park_proof", "role_session"])
def test_installed_leaf_import_needs_no_broad_owner(installed, tmp_path, leaf):
    evidence = _child(installed, "leaves", tmp_path / "import", f"""
        import importlib
        import prismabuild
        importlib.import_module('prismabuild' + {('.' + leaf) if leaf else ''!r})
        for name in {BROAD!r}:
            try:
                importlib.import_module('prismabuild.' + name)
            except ModuleNotFoundError as error:
                assert error.name == 'prismabuild.' + name, (name, error)
            else:
                raise AssertionError(name + ' is available in the leaves installation')
        print(json.dumps(origins(), sort_keys=True))
    """)
    expected = ROLE_MODULES if leaf == "role_session" else {
        "prismabuild", "prismabuild.progress", *([f"prismabuild.{leaf}"] if leaf else [])}
    assert set(evidence) == expected


def test_installed_role_runs_real_operations_without_broad_owners(installed, tmp_path):
    evidence = _child(installed, "leaves", tmp_path / "operations", """
        import os, signal, socket, stat, subprocess
        from prismabuild import generation_receipt as receipts, role_session as role
        loaded, published = work / 'loaded.json', work / 'published.json'
        loaded.write_text('{"commit":"before","generation":"first"}')
        published.write_text('{"commit":"after","generation":"second"}')
        role.GENERATION_VERSION, role.RUNTIME_VERSION = loaded, published
        assert receipts.compare(role.loaded_runtime(), role.published_runtime()) == receipts.Drift(
            receipts.RuntimeIdentity('before', 'first'), receipts.RuntimeIdentity('after', 'second'))
        published.unlink()
        assert role.published_runtime() == receipts.ReceiptUnavailable('missing')
        published.write_text('{')
        assert role.published_runtime() == receipts.ReceiptUnavailable('malformed')
        published.write_text('{}')
        assert role.published_runtime() == receipts.ReceiptUnavailable('incomplete')
        assert receipts.observe(work) == receipts.ReceiptUnavailable('unreadable')
        assert isinstance(receipts.compare(role.loaded_runtime(), role.published_runtime()), receipts.Unknown)

        role.time.time = lambda: 123.456
        drift = {'loaded_commit':'before', 'loaded_generation':'first',
                 'published_commit':'after', 'published_generation':'second'}
        path = role.record_generation_drift(drift, actor='installed-role',
            queue_root=work / 'queue', detail={'message':'café'})
        host, pid = socket.gethostname(), os.getpid()
        raw = ('{"actor":"installed-role","detail":{"message":"café"},"host":'
               + json.dumps(host) + ',"loaded_commit":"before","loaded_generation":"first","pid":'
               + str(pid) + ',"published_commit":"after","published_generation":"second",'
               '"schema":"prismaquant.prismabuild.generation_drift.v1","unix":123.456}\\n').encode()
        assert path.read_bytes() == raw
        assert path.name == f'{123456:013d}-{host}-{pid}-{hashlib.sha256(raw).hexdigest()[:8]}.json'
        assert stat.S_IMODE(path.stat().st_mode) == 0o444
        inode = path.stat().st_ino
        assert role.record_generation_drift(drift, actor='installed-role',
            queue_root=work / 'queue', detail={'message':'café'}) == path
        assert path.stat().st_ino == inode and path.read_bytes() == raw
        (work / 'blocked').write_text('not a directory')
        assert role.record_generation_drift(drift, actor='installed-role', queue_root=work / 'blocked') is None

        role.ROLE_LOCK_ROOT = work / 'locks'
        script = Path('/retained-generation/tools/installed-role.py')
        with role.role_singleton(script):
            contender = subprocess.run([sys.executable, '-I', '-B', '-c',
                'import sys; from pathlib import Path; from prismabuild import role_session as r; '
                'r.ROLE_LOCK_ROOT=Path(sys.argv[1]); '
                'print(r.role_singleton_holder(sys.argv[2]))', str(role.ROLE_LOCK_ROOT), str(script)],
                capture_output=True, text=True, timeout=10)
            assert contender.returncode == 0, contender.stderr
            assert contender.stdout.strip() == f'(True, {pid})'
            holder = role.flock_owner.holder_pid
            role.flock_owner.holder_pid = lambda descriptor: None
            assert role.role_singleton_holder(script) == (True, None)
            role.flock_owner.holder_pid = holder
            with role.role_singleton('another-role.py'):
                pass
        assert role.role_singleton_holder(script) == (False, None)
        try:
            with role.role_singleton(script):
                raise ValueError('finish the protected operation')
        except ValueError:
            pass
        assert role.role_singleton_holder(script) == (False, None)
        assert role.role_lock_path(script) == role.role_lock_path('/new-generation/installed-role.py')

        previous = signal.getsignal(signal.SIGTERM)
        with role.stop_requests(signal.SIGTERM) as stop:
            os.kill(os.getpid(), signal.SIGTERM)
            assert stop.wait(1)
        assert signal.getsignal(signal.SIGTERM) == previous
        role.MAINTENANCE_GATE = work / 'maintenance.json'
        role.MAINTENANCE_GATE.write_text('{"draining":true,"changed_unix":101.5}')
        role.PARKED_ROOT = work / 'parked'
        role.PARKED_ROOT.mkdir()
        gate = role.read_maintenance_gate()
        marker = role.post_park_marker(gate)
        assert marker.name == f'{pid}-{role.park_proof.process_starttime(pid)}-101.5'
        assert marker.read_bytes() == b''
        before = marker.stat()
        assert role.post_park_marker(gate) == marker
        after = marker.stat()
        assert (before.st_ino, before.st_mtime_ns) == (after.st_ino, after.st_mtime_ns)
        role.PARKED_ROOT = work / 'missing-parked'
        assert role.post_park_marker(gate) is None
        print(json.dumps(origins(), sort_keys=True))
    """)
    assert set(evidence) == ROLE_MODULES


def test_installed_public_exports_keep_real_objects_and_progress_bytes(installed, tmp_path):
    _child(installed, "complete", tmp_path / "exports", """
        import importlib, os
        import prismabuild
        names = ['core', 'progress', 'residency_map', 'commit', 'report_action_progress']
        assert prismabuild.__all__ == names
        assert prismabuild.__version__ == '0.1.0'
        assert set(names) <= set(dir(prismabuild))
        namespace = {}
        exec('from prismabuild import *', namespace)
        from prismabuild import core, progress, residency_map, commit, report_action_progress
        for module in (core, progress, residency_map):
            assert getattr(prismabuild, module.__name__.rpartition('.')[2]) is module
            assert importlib.import_module(module.__name__) is module
        assert commit is prismabuild.commit is progress.commit is namespace['commit']
        assert report_action_progress is prismabuild.report_action_progress is progress.report_action_progress
        assert core.report_action_progress is not report_action_progress
        assert all(namespace[name] is getattr(prismabuild, name) for name in names)
        try:
            prismabuild.missing_public_export
        except AttributeError:
            pass
        else:
            raise AssertionError('unknown package attribute was accepted')
        path = work / 'progress.json'
        os.environ.update(PRISMABUILD_ACTION_PROGRESS_PATH=str(path),
            PRISMABUILD_ACTION_PROGRESS_TOKEN='installed-token',
            PRISMABUILD_ACTION_PROGRESS_PHASES='["run"]')
        assert commit(1, 'run')
        assert json.loads(path.read_text())['units_completed'] == 1
        assert report_action_progress('run', 2)
        assert json.loads(path.read_text())['units_completed'] == 2
        print(json.dumps(origins(), sort_keys=True))
    """)


def test_installed_sdk_uses_the_real_owners(installed, tmp_path):
    _child(installed, "complete", tmp_path / "sdk", """
        from prismabuild import client, core, digest_primitives, immutable_files, pool, reader_lease
        assert client.PoolQueue is pool.PoolQueue
        assert client.acquire_for is reader_lease.acquire_for
        assert client.canonical_sha256 is digest_primitives.canonical_sha256
        assert core.ActionContractError is digest_primitives.ActionContractError
        assert core.CASTamperError is immutable_files.CASTamperError
        queue = client.PoolQueue(work / 'queue')
        assert client.read_claimed_record(queue, 'a' * 64) is None
        print(json.dumps(origins(), sort_keys=True))
    """)
