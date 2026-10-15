"""Private publication and fresh installed-reader process fixtures."""
import hashlib
from contextlib import contextmanager
import json
import os
from pathlib import Path
import select
import subprocess
import sys
from types import SimpleNamespace
import uuid

import pytest


ROOT = Path(__file__).resolve().parents[1]
PREBRIDGE = "30ca9a6f6041f6d31dd5db1c5c4bdcc97a3a7edf"
PREBRIDGE_UPDATER_SHA256 = "b737d703c622a851f31e90ced5c9fca487597ee1e0adb38b52db44bef54c16e9"
CAPABLE_BRIDGE = "f834a061b448e2bbfed686e5ef00ca21f6b2b65c"
CAPABLE_BRIDGE_SHA256 = "be33c1ad42df95da03322aa73ccd48d0ee086ed24829a9975871662cf0acdb1e"
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
import fcntl
import json
import os
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace

assert sys.flags.isolated == 1
config_path, backend_path = map(Path, sys.argv[1:3])
mode = sys.argv[3]
config = json.loads(config_path.read_text())
backend = json.loads(backend_path.read_text())
controls = json.loads(Path(sys.argv[4]).read_text()) if len(sys.argv) > 4 else {}
install = Path(config['install_dir'])
driver = (Path(config['runtime']) / 'tools/upgrade_client.py'
          if mode == 'direct' else install / 'upgrade_client.py')
program = runpy.run_path(str(driver), run_name='client_reader')
assert not any(name == 'prismabuild' or name.startswith('prismabuild.') for name in sys.modules)

def persist_backend():
    pending = backend_path.with_suffix('.pending')
    with pending.open('w') as stream:
        json.dump(backend, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(pending, backend_path)

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
    persist_backend()
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
    persist_backend()
    return SimpleNamespace(returncode=code)

namespace = program['Upgrader']._run_once.__globals__
original_replace = os.replace
original_unlink = Path.unlink
original_read_bytes = Path.read_bytes
original_bounded_read = namespace['bounded_read']
original_search = namespace['re'].search
original_atomic = namespace['atomic']
original_sync = namespace['sync_dir']
reads = {}
phase = {'value': 'forward'}
fired = set()

def pause(name):
    if controls.get('cut') != name or name in fired:
        return
    fired.add(name)
    persist_backend()
    print(json.dumps({'bridge_cut': name, 'phase': phase['value']}), flush=True)
    assert sys.stdin.readline().strip() == 'continue'

def replacement(source, target):
    name = Path(target).name
    event = {'park_proof.py': 'leaf', 'upgrade_client.py': 'updater'}.get(name)
    prefix = 'restore-' if phase['value'] == 'recover' else ''
    watched = Path(target).parent == install and event is not None
    if watched:
        pause(prefix + event + '-before-replace')
    original_replace(source, target)
    if watched:
        pause(prefix + event + '-after-replace')

def unlink(path, *args, **kwargs):
    if path == install / 'park_proof.py':
        pause('restore-leaf-before-absence')
    if path == Path(config['state_dir']) / 'transaction.json':
        pause(('restore-' if phase['value'] == 'recover' else '') + 'journal-before-clear')
    result = original_unlink(path, *args, **kwargs)
    if path == install / 'park_proof.py':
        pause('restore-leaf-after-absence')
    return result

def atomic(path, value):
    original_atomic(path, value)
    if path == Path(config['state_dir']) / 'transaction.json':
        pause('journal-durable')

def sync(path):
    original_sync(path)
    if path == install:
        pause(('restore-' if phase['value'] == 'recover' else '') + 'install-after-sync')

def read_bytes(path):
    data = original_read_bytes(path)
    reads[id(data)] = (data, path)
    return data

def bounded_read(path, limit):
    data = original_bounded_read(path, limit)
    reads[id(data)] = (data, path)
    return data

def search(pattern, data, *args, **kwargs):
    matched = original_search(pattern, data, *args, **kwargs)
    observation = reads.get(id(data))
    if (controls.get('swap_previous') and 'previous' not in fired and matched is not None
            and isinstance(pattern, bytes) and b'CLIENT_UPGRADE_PROOF_MEMBER_PROTOCOL' in pattern
            and observation is not None and observation[0] is data
            and observation[1] == install / 'upgrade_client.py'):
        fired.add('previous')
        target = install / 'upgrade_client.py'
        pending = target.with_suffix('.replacement')
        changed = original_read_bytes(Path(controls['swap_previous']))
        pending.write_bytes(changed)
        original_replace(pending, target)
        Path(controls['observation']).write_text(json.dumps({
            'captured_sha256': hashlib.sha256(data).hexdigest(),
            'replacement_sha256': hashlib.sha256(changed).hexdigest()}))
    return matched

def reader(config):
    observed = namespace['desired'](config)
    if controls.get('swap_candidate'):
        target = Path(config['runtime']) / 'tools/upgrade_client.py'
        pending = target.with_suffix('.replacement')
        pending.write_bytes(original_read_bytes(Path(controls['swap_candidate'])))
        original_replace(pending, target)
    return observed

os.replace = replacement
Path.unlink = unlink
Path.read_bytes = read_bytes
namespace['bounded_read'] = bounded_read
namespace['re'].search = search
namespace['atomic'] = atomic
namespace['sync_dir'] = sync
updater = program['Upgrader'](config, rpc=rpc, command=command, sleep=lambda _: None,
                             reader=reader)
original_recover = updater.recover
def recover(*args, **kwargs):
    phase['value'] = 'recover'
    try:
        return original_recover(*args, **kwargs)
    finally:
        phase['value'] = 'forward'
updater.recover = recover
lock = (Path(config['state_dir']) / 'upgrade.lock').open('a')
fcntl.flock(lock, fcntl.LOCK_EX)
try:
    if mode == 'loaded':
        result = {'loaded_matches': updater.loaded_matches(rpc(None, 'maintenance_status'))}
    else:
        result = updater.run()
    outcome = {'result': result, 'exception': None}
except Exception as error:
    outcome = {'result': None, 'exception': type(error).__name__, 'error': str(error)}
persist_backend()
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


@pytest.fixture
def capable_publication(publication):
    _, install, _, _, _, _ = publication
    code = subprocess.check_output(
        ["git", "show", f"{CAPABLE_BRIDGE}:tools/fleet/upgrade_client.py"], cwd=ROOT)
    assert _sha(code) == CAPABLE_BRIDGE_SHA256
    (install / "upgrade_client.py").write_bytes(code)
    return publication


def member_bytes(install):
    return {name: (install / name).read_bytes() if (install / name).exists() else None
            for name in CLIENT_SOURCES}


@contextmanager
def reader_process(publication, **controls):
    _, _, state, config, backend, harness = publication
    token = uuid.uuid4().hex
    control = state / f"control-{token}.json"
    control.write_text(json.dumps(controls))
    log = state / f"child-{token}.log"
    with log.open("w") as errors:
        child = subprocess.Popen(
            [sys.executable, "-I", str(harness), str(config), str(backend), "run", str(control)],
            cwd="/", text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=errors, start_new_session=True)
        def message():
            while True:
                available, _, _ = select.select([child.stdout], [], [], 15)
                assert available, f"child missed control boundary: {controls}; {log.read_text()}"
                line = child.stdout.readline()
                assert line, f"child exited {child.poll()}: {log.read_text()}"
                value = json.loads(line)
                if "bridge_cut" in value or "test_outcome" in value:
                    return value
        def resume():
            child.stdin.write("continue\n")
            child.stdin.flush()
        def finish():
            observed = message()
            assert "test_outcome" in observed, observed
            child.wait(timeout=15)
            assert child.returncode == 0, log.read_text()
            return observed["test_outcome"]
        def kill():
            child.kill()
            child.wait(timeout=15)
        try:
            yield SimpleNamespace(message=message, resume=resume, finish=finish, kill=kill)
        finally:
            if child.poll() is None:
                kill()
            child.stdin.close()
            child.stdout.close()


