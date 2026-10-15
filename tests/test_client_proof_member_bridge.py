"""Fresh installed updater readers own transport, not broker-loaded proof code."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
PREBRIDGE = "30ca9a6f6041f6d31dd5db1c5c4bdcc97a3a7edf"
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
program = runpy.run_path(str(install / 'upgrade_client.py'), run_name='installed_reader')
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


def test_old_installed_reader_adopts_bridge_before_fresh_reader_introduces_proof(publication):
    generation, install, _, _, _, _ = publication
    receipt = (generation / "RUNTIME_VERSION.json").read_bytes()
    proof = (generation / "src/prismabuild/park_proof.py").read_bytes()
    first = _fresh(publication)
    assert first["exception"] is None, first
    assert first["result"]["state"] == "updated", first
    assert "park_proof.py" not in first["result"]["desired"]["files"]
    assert (install / "upgrade_client.py").read_bytes() == (ROOT / "tools/fleet/upgrade_client.py").read_bytes()
    assert not (install / "park_proof.py").exists()
    assert (generation / "RUNTIME_VERSION.json").read_bytes() == receipt
    assert (generation / "src/prismabuild/park_proof.py").read_bytes() == proof

    second = _fresh(publication)
    assert second["exception"] is None, second
    assert second["executed_sha256"] == _sha((ROOT / "tools/fleet/upgrade_client.py").read_bytes())
    assert second["result"]["state"] == "updated", second
    assert second["result"]["desired"]["files"]["park_proof.py"] == _sha(proof)
    assert (install / "park_proof.py").read_bytes() == proof
    assert (generation / "RUNTIME_VERSION.json").read_bytes() == receipt


@pytest.mark.parametrize("fault,expected", [("none", True), ("proof-loaded", False),
                                           ("missing-gpu", False)])
def test_broker_loaded_identity_excludes_only_updater_owned_members(publication, fault, expected):
    _, install, _, _, backend_path, _ = publication
    (install / "upgrade_client.py").write_bytes((ROOT / "tools/fleet/upgrade_client.py").read_bytes())
    (install / "park_proof.py").write_bytes((ROOT / "src/prismabuild/park_proof.py").read_bytes())
    backend = json.loads(backend_path.read_text())
    if fault == "proof-loaded":
        backend["loaded"]["park_proof.py"] = _sha((install / "park_proof.py").read_bytes())
    elif fault == "missing-gpu":
        backend["loaded"].pop("gpu_capacity.py")
    backend_path.write_text(json.dumps(backend))
    observed = _fresh(publication, "loaded")
    assert observed["exception"] is None, observed
    assert observed["result"]["loaded_matches"] is expected, observed


@pytest.mark.parametrize("incapable", ["previous", "candidate"])
def test_proof_transaction_requires_both_actual_reader_capabilities_before_mutation(publication, incapable):
    generation, install, state, _, backend_path, _ = publication
    candidate = (ROOT / "tools/fleet/upgrade_client.py").read_bytes()
    if incapable == "candidate":
        (install / "upgrade_client.py").write_bytes(candidate)
        candidate = candidate.replace(b"CLIENT_UPGRADE_PROOF_MEMBER_PROTOCOL = 1\n", b"")
        target = generation / "tools/upgrade_client.py"
        target.write_bytes(candidate)
        receipt_path = generation / "RUNTIME_VERSION.json"
        receipt = json.loads(receipt_path.read_text())
        receipt["files"]["tools/upgrade_client.py"] = _sha(candidate)
        receipt_path.write_text(json.dumps(receipt))
    before = {path.name: path.read_bytes() for path in install.iterdir()}
    observed = _fresh(publication)
    assert observed["exception"] == "ValueError", observed
    assert "proof" in observed["error"].lower() and "protocol" in observed["error"].lower(), observed
    assert observed["journal_exists"] is False
    assert not (state / "transaction.json").exists()
    assert {path.name: path.read_bytes() for path in install.iterdir()} == before
    operations = json.loads(backend_path.read_text())["operations"]
    assert not set(operations) & {"maintenance_begin", "stop", "start", "maintenance_end"}
