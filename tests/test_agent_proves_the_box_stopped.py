"""Native process census and selector controls; trusted host actors live separately."""
import importlib.util
import json
import os
from pathlib import Path

import pytest

from prismabuild import park_proof

REPO = Path(__file__).resolve().parents[1]
BROKER = REPO / 'tools/fleet/resource_broker.py'
CLIENT = REPO / 'tools/fleet/upgrade_client.py'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


broker_module = load('resource_broker_proof', BROKER)
upgrade = load('upgrade_client_proof', CLIENT)


class Backend:
    def healthy(self):
        return True

    def inventory(self):
        return {}

    def exists(self, scope):
        return False


def authority(state_dir):
    return broker_module.Authority(state_dir, os.getuid(), Backend(),
                                   max_memory_bytes=1024 ** 3)


def agent(tmp_path, gate, *, census=()):
    return upgrade.Upgrader(dict(install_dir=str(tmp_path / 'installed'),
                                 state_dir=str(tmp_path / 'state'),
                                 maintenance_gate=str(gate),
                                 reader_uid=os.getuid()),
                            rpc=lambda *a, **k: {'ok': True},
                            sleep=lambda _: None,
                            procs=lambda: list(census))


def starttime_of_self() -> str:
    """Field 22 of `/proc/self/stat`, computed here so no module certifies itself."""
    _, _, rest = Path('/proc/self/stat').read_text().rpartition(')')
    return rest.split()[19]


#: One supervised loop, one operator one-shot run from the stable symlink, one
#: run from a local checkout, and one process that is none of those.
GENERATION = '/mnt/shared/prismabuild-fleet/runtime-generations/abc123-1-def/tools/fleet'
CENSUS = [
    (101, '900', ['/usr/bin/python3', f'{GENERATION}/worker_loop.py', '--poll-s', '15']),
    (102, '901', ['/usr/bin/python3',
                  '/mnt/shared/prismabuild-fleet/repo/tools/fleet/worker.py']),
    (103, '902', ['python3', '/home/rob/prismabuild/tools/fleet/worker.py']),
    (104, '903', ['/usr/bin/python3', '/home/rob/unrelated.py', 'worker_loop']),
]


def test_a_one_shot_run_by_hand_is_still_a_process_that_can_claim():
    """It hardcodes the live queue, so how it was invoked does not soften it."""

    assert park_proof.select_processes(CENSUS, park_proof.HOST_QUIESCENT) == [(101, '900'), (102, '901'), (103, '902')]


def test_both_proof_profiles_cover_the_supervised_work_and_storage_roles():
    from supervise import LOOP_SCRIPT, ROLE_SCRIPTS

    expected = {LOOP_SCRIPT, "worker.py", *ROLE_SCRIPTS.values()} - {"pbmetrics.py"}
    assert park_proof.HOST_QUIESCENT == expected
    assert park_proof.MEMBER_LOOPS == expected


def test_the_key_comes_from_the_gate_the_broker_actually_wrote(tmp_path):
    """The status reply does not carry it, so the file is the only source."""

    broker = authority(tmp_path / 'state')
    broker.handle(0, os.getpid(), {'op': 'maintenance_begin', 'owner': 'rob'})
    stamp = json.loads(broker.maintenance_path.read_text())['changed_unix']

    identity = park_proof.drain_identity(park_proof.read_gate(broker.maintenance_path))
    assert identity.key == str(stamp)
    assert 'changed_unix' not in broker.handle(
        0, os.getpid(), {'op': 'maintenance_status'})


def test_the_agent_creates_the_directory_the_loops_record_in(tmp_path):
    """The loops cannot: `/run/prismabuild` is root-owned and they are not root."""

    gate = tmp_path / 'maintenance.json'
    client = agent(tmp_path, gate)
    assert not client.parked_root.exists()

    assert client.ensure_parked_root() is True

    assert client.parked_root.is_dir()
    info = client.parked_root.stat()
    assert info.st_uid == os.getuid()
    assert info.st_mode & 0o777 == 0o755
    assert client.ensure_parked_root() is True


def test_a_box_that_is_not_draining_is_not_asked(tmp_path):
    """Observation costs a `/proc` walk, and an admitting box owes no proof."""

    gate = tmp_path / 'maintenance.json'
    taken = []
    client = agent(tmp_path, gate)
    client.procs = lambda: taken.append(1) or []

    assert client.drain_evidence({'draining': False, 'active_scopes': 0}) == {}
    assert taken == []


def test_the_census_reads_this_process(tmp_path):
    """The real reader, against the one process the test can vouch for."""

    census = {pid: (start, argv) for pid, start, argv in park_proof.proc_census()}
    start, argv = census[os.getpid()]
    assert start == starttime_of_self()
    assert argv and Path(argv[0]).name


def test_a_zero_start_tick_is_a_valid_process_identity(tmp_path):
    """PID 1 reports start tick 0 on this kernel, so 0 cannot read as malformed."""

    root, process = private_proc(tmp_path)
    (process / 'stat').write_text('101 (worker (loop)) S ' + '0 ' * 18 + '0 0')

    assert park_proof.proc_census(root) == [
        (101, '0', ['python3', '/checkout/worker_loop.py'])]


def test_a_replacement_after_tick_zero_is_still_a_reused_pid(tmp_path, monkeypatch):
    """The second stat read is compared, not retrusted, when the first tick is 0."""

    root, process = private_proc(tmp_path)
    (process / 'stat').write_text('101 (worker (loop)) S ' + '0 ' * 18 + '0 0')
    original = Path.read_bytes
    def replaced(path):
        result = original(path)
        if path == process / 'cmdline':
            (process / 'stat').write_text('101 (replacement) S ' + '0 ' * 18 + '901 0')
        return result
    monkeypatch.setattr(Path, 'read_bytes', replaced)

    with pytest.raises(ValueError, match='identity changed for pid 101'):
        park_proof.proc_census(root)


@pytest.mark.parametrize('tick', ['-5', 'x', '0.0', '900x'])
def test_a_malformed_or_negative_start_tick_is_refused(tmp_path, tick):
    root, process = private_proc(tmp_path)
    (process / 'stat').write_text('101 (worker (loop)) S ' + '0 ' * 18 + f'{tick} 0')

    with pytest.raises(ValueError, match='invalid process identity for pid 101'):
        park_proof.proc_census(root)


def private_proc(tmp_path):
    root = tmp_path / 'proc'
    process = root / '101'
    process.mkdir(parents=True)
    (process / 'stat').write_text('101 (worker (loop)) S ' + '0 ' * 18 + '900 0')
    (process / 'cmdline').write_bytes(b'python3\0/checkout/worker_loop.py\0')
    return root, process


def test_process_that_disappears_during_census_is_not_a_live_unknown(tmp_path, monkeypatch):
    root, process = private_proc(tmp_path)
    original = Path.read_bytes
    def exited(path):
        if path == process / 'cmdline':
            (process / 'cmdline').unlink()
            (process / 'stat').unlink()
            process.rmdir()
            raise FileNotFoundError('exited')
        return original(path)
    monkeypatch.setattr(Path, 'read_bytes', exited)
    assert park_proof.proc_census(root) == []
