"""Private publication and fresh installed-reader process fixtures."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
PREBRIDGE = "30ca9a6f6041f6d31dd5db1c5c4bdcc97a3a7edf"
PREBRIDGE_UPDATER_SHA256 = "b737d703c622a851f31e90ced5c9fca487597ee1e0adb38b52db44bef54c16e9"
CLIENT_SOURCES = {
    "resource_broker.py": ("tools/resource_broker.py", "tools/fleet/resource_broker.py"),
    "resource_payload.py": ("tools/resource_payload.py", "tools/fleet/resource_payload.py"),
    "gpu_memory.py": ("src/prismabuild/gpu_memory.py", "src/prismabuild/gpu_memory.py"),
    "gpu_capacity.py": ("src/prismabuild/gpu_capacity.py", "src/prismabuild/gpu_capacity.py"),
    "upgrade_client.py": ("tools/upgrade_client.py", "tools/fleet/upgrade_client.py"),
    "park_proof.py": ("src/prismabuild/park_proof.py", "src/prismabuild/park_proof.py"),
}
BROKER_LOADED = ("resource_broker.py", "resource_payload.py", "gpu_memory.py", "gpu_capacity.py")

# This child imports the actual installed program under isolated Python. Only
# broker/service I/O is private fixture state; export, validation, transaction,
# loaded equality and reporting are the installed reader's implementation.
INSTALLED_READER = """\
import hashlib
import json
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace

assert sys.flags.isolated == 1
config_path, backend_path = map(Path, sys.argv[1:3])
mode = sys.argv[3]
config = json.loads(config_path.read_text())
backend = json.loads(backend_path.read_text())
install = Path(config['install_dir'])
driver = (Path(config['runtime']) / 'tools/upgrade_client.py'
          if mode == 'direct' else install / 'upgrade_client.py')
program = runpy.run_path(str(driver), run_name='client_reader')
assert not any(name == 'prismabuild' or name.startswith('prismabuild.') for name in sys.modules)

def rpc(endpoint, operation, **fields):
    backend['operations'].append(operation)
    if operation == 'maintenance_begin':
        assert backend['holder'] in (None, fields['owner'])
        backend['holder'] = fields['owner']
    elif operation == 'maintenance_end':
        assert backend['holder'] == fields['owner']
        backend['holder'] = None
    Path(config['maintenance_gate']).write_text(json.dumps({
        'draining': backend['holder'] is not None, 'changed_unix': 458.0,
        'owner': backend['holder']}))
    reply = dict(ok=True, health=True, draining=backend['holder'] is not None,
                 active_scopes=backend['active'], maintenance_protocol=2,
                 maintenance_durable_protocol=1,
                 installed_sha256=backend['loaded'])
    if backend['holder'] is not None:
        reply['maintenance_owner'] = backend['holder']
    return reply

def command(argv, **kwargs):
    verb = argv[1]
    backend['operations'].append(verb)
    if verb == 'stop':
        assert backend['active'] == 0 and backend['holder'] is not None
        backend['running'] = False
    elif verb == 'start':
        backend['running'] = True
        backend['loaded'] = {
            name: hashlib.sha256((install / name).read_bytes()).hexdigest()
            for name in ('resource_broker.py', 'resource_payload.py',
                         'gpu_memory.py', 'gpu_capacity.py')}
    code = 0 if verb != 'is-active' or backend['running'] else 3
    return SimpleNamespace(returncode=code)

updater = program['Upgrader'](config, rpc=rpc, command=command, sleep=lambda _: None)
try:
    if mode == 'loaded':
        result = {'loaded_matches': updater.loaded_matches(rpc(None, 'maintenance_status'))}
    else:
        result = updater.run()
    outcome = {'result': result, 'exception': None}
except Exception as error:
    outcome = {'result': None, 'exception': type(error).__name__, 'error': str(error)}
backend_path.write_text(json.dumps(backend))
outcome['journal_exists'] = updater.journal.exists()
outcome['executed_sha256'] = updater.executing_sha256
print(json.dumps({'test_outcome': outcome}), flush=True)
"""


def _sha(data):
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def publication(tmp_path):
    generation = tmp_path / "generations" / "proof-bridge"
    install = tmp_path / "installed"
    state = tmp_path / "state"
    run = tmp_path / "run"
    for path in (generation, install, state, run):
        path.mkdir(parents=True)
    historical = subprocess.check_output(
        ["git", "show", f"{PREBRIDGE}:tools/fleet/upgrade_client.py"], cwd=ROOT)
    assert _sha(historical) == PREBRIDGE_UPDATER_SHA256
    for name in CLIENT_SOURCES:
        if name != "park_proof.py":
            (install / name).write_bytes(historical if name == "upgrade_client.py"
                                         else f"old:{name}".encode())
    files = {}
    for member, source in [*CLIENT_SOURCES.values(),
                            ("src/prismabuild/role_session.py", "src/prismabuild/role_session.py")]:
        destination = generation / member
        destination.parent.mkdir(parents=True, exist_ok=True)
        data = (ROOT / source).read_bytes()
        destination.write_bytes(data)
        files[member] = _sha(data)
    receipt = generation / "RUNTIME_VERSION.json"
    receipt.write_text(json.dumps({
        "schema": "prismaquant.prismabuild.runtime_version.v1",
        "generation": generation.name, "commit": "a" * 40, "files": files}))
    gate = run / "maintenance.json"
    gate.write_text(json.dumps({"draining": False}))
    config = tmp_path / "config.json"
    config.write_text(json.dumps({
        "runtime": str(generation), "generation_store": str(generation.parent),
        "install_dir": str(install), "state_dir": str(state),
        "maintenance_gate": str(gate), "reader_uid": os.getuid()}))
    backend = tmp_path / "backend.json"
    backend.write_text(json.dumps({
        "holder": None, "active": 0, "running": True, "operations": [],
        "loaded": {name: _sha((install / name).read_bytes()) for name in BROKER_LOADED}}))
    harness = tmp_path / "installed_reader.py"
    harness.write_text(INSTALLED_READER)
    return generation, install, state, config, backend, harness


def _fresh(publication, mode="run"):
    _, _, _, config, backend, harness = publication
    child = subprocess.run(
        [sys.executable, "-I", str(harness), str(config), str(backend), mode],
        cwd="/", text=True, capture_output=True, timeout=30)
    assert child.returncode == 0, (child.stdout, child.stderr)
    final = json.loads(child.stdout.splitlines()[-1])
    return final["test_outcome"]


