"""Whether a box has stopped admitting is a fact about it, not elapsed time.

The broker reports `active_scopes`, which covers work already running under
containment, and says nothing about a loop sitting between actions that is
about to claim another one.  #458 cannot move the runtime symlink until every
host has stopped, so somebody has to be able to answer for a host.

The agent answers by naming every process that could still claim and checking
each one against the park marker it leaves when it parks.  Two things about
that matching are load bearing.

A serving process is recognised by the basename in its argv and not by a
generation-store prefix.  A supervised loop carries the resolved generation
path, but the one-shot in `tools/fleet/worker.py` is run by hand, from the
symlink or from a checkout, and it reaches the same live queue either way.
Matching on the prefix would miss precisely the process #459 was filed about.

The marker name is spelled in two modules that cannot import each other: the
loop runs as uid 1000 out of a published generation, the agent runs as root out
of the install directory.  Both spellings are asserted against each other here,
because a drift between them reads as a fleet that never drains.

Nothing here touches `/run`, a real broker, a real queue or a real process.
"""
import importlib.util
import json
import os
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
BROKER = REPO / 'tools/fleet/resource_broker.py'
CLIENT = REPO / 'tools/fleet/upgrade_client.py'
WORKER_LOOP = REPO / 'tools/fleet/worker_loop.py'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


broker_module = load('resource_broker_proof', BROKER)
upgrade = load('upgrade_client_proof', CLIENT)


def worker_loop(gate):
    """The loop, with its gate pointed somewhere writable.

    Read at import, the way a supervised loop child receives it.
    """
    previous = os.environ.get('PRISMABUILD_MAINTENANCE_GATE')
    os.environ['PRISMABUILD_MAINTENANCE_GATE'] = str(gate)
    try:
        return load('worker_loop_proof', WORKER_LOOP)
    finally:
        if previous is None:
            os.environ.pop('PRISMABUILD_MAINTENANCE_GATE', None)
        else:
            os.environ['PRISMABUILD_MAINTENANCE_GATE'] = previous


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


def test_the_two_spellings_of_a_park_marker_agree(tmp_path):
    """A drift here reads as a fleet that never drains, so it is pinned."""

    gate = tmp_path / 'maintenance.json'
    loop = worker_loop(gate)
    client = agent(tmp_path, gate)

    assert client.parked_root == loop.PARKED_ROOT
    loop.PARKED_ROOT.mkdir(parents=True)

    for stamp in (1788947072.5, 1788947072, '1788947072.5', 'a/b c'):
        gate.write_text(json.dumps({'draining': True, 'changed_unix': stamp}))
        written = loop.post_park_marker(loop.read_maintenance_gate())
        assert written is not None
        assert written.name == upgrade.park_marker_name(
            os.getpid(), starttime_of_self(), stamp)


def test_a_one_shot_run_by_hand_is_still_a_process_that_can_claim():
    """It hardcodes the live queue, so how it was invoked does not soften it."""

    assert upgrade.serving(CENSUS) == [(101, '900'), (102, '901'), (103, '902')]


def test_a_box_whose_processes_have_all_parked_has_stopped():
    markers = {upgrade.park_marker_name(pid, start, 12.5)
               for pid, start in upgrade.serving(CENSUS)}
    assert upgrade.drained(CENSUS, markers, 12.5, 0) == (True, [])


def test_a_loop_that_has_not_parked_is_named():
    markers = {upgrade.park_marker_name(pid, start, 12.5)
               for pid, start in upgrade.serving(CENSUS)[1:]}
    assert upgrade.drained(CENSUS, markers, 12.5, 0) == (False, [101])


def test_a_marker_from_an_earlier_drain_is_not_evidence_about_this_one():
    """`changed_unix` names the stop, and a stale marker names the last one."""

    markers = {upgrade.park_marker_name(pid, start, 11.0)
               for pid, start in upgrade.serving(CENSUS)}
    assert upgrade.drained(CENSUS, markers, 12.5, 0) == (False, [101, 102, 103])


def test_a_marker_left_by_a_reused_pid_is_not_evidence_either():
    """The start time is in the name because a pid alone is reused."""

    markers = {upgrade.park_marker_name(101, '899', 12.5)}
    assert upgrade.drained(CENSUS[:1], markers, 12.5, 0) == (False, [101])


def test_work_already_running_holds_the_drain_open():
    markers = {upgrade.park_marker_name(pid, start, 12.5)
               for pid, start in upgrade.serving(CENSUS)}
    assert upgrade.drained(CENSUS, markers, 12.5, 1) == (False, [])


def test_a_box_running_nothing_that_can_claim_has_stopped():
    assert upgrade.drained(CENSUS[3:], set(), 12.5, 0) == (True, [])


@pytest.mark.parametrize('content', [None, '{not json', '[]',
                                     '{"draining": false}', '{}'])
def test_a_gate_that_states_no_open_drain_yields_no_key(tmp_path, content):
    """No key means no marker matches, which leaves the box reading as admitting."""

    gate = tmp_path / 'maintenance.json'
    if content is not None:
        gate.write_text(content)
    assert upgrade.gate_changed_unix(gate) is None


def test_the_key_comes_from_the_gate_the_broker_actually_wrote(tmp_path):
    """The status reply does not carry it, so the file is the only source."""

    broker = authority(tmp_path / 'state')
    broker.handle(0, os.getpid(), {'op': 'maintenance_begin', 'owner': 'rob'})
    stamp = json.loads(broker.maintenance_path.read_text())['changed_unix']

    assert upgrade.gate_changed_unix(broker.maintenance_path) == stamp
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


def test_the_report_names_what_is_still_admitting(tmp_path):
    gate = tmp_path / 'maintenance.json'
    gate.write_text(json.dumps({'draining': True, 'changed_unix': 12.5}))
    client = agent(tmp_path, gate, census=CENSUS)
    client.ensure_parked_root()
    for pid, start in upgrade.serving(CENSUS)[1:]:
        (client.parked_root / upgrade.park_marker_name(pid, start, 12.5)).touch()

    evidence = client.drain_evidence({'draining': True, 'active_scopes': 0})

    assert evidence == {'drained': False, 'unparked': [101], 'active_scopes': 0}


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

    census = {pid: (start, argv) for pid, start, argv in upgrade.proc_census()}
    start, argv = census[os.getpid()]
    assert start == starttime_of_self()
    assert argv and Path(argv[0]).name


@pytest.mark.parametrize('census,markers', [([], set()),
    (CENSUS[:1], {upgrade.park_marker_name(101, '900', None)})])
def test_unknown_gate_never_proves_a_drain(census, markers):
    assert upgrade.drained(census, markers, None, 0)[0] is False


def private_proc(tmp_path):
    root = tmp_path / 'proc'
    process = root / '101'
    process.mkdir(parents=True)
    (process / 'stat').write_text('101 (worker (loop)) S ' + '0 ' * 18 + '900 0')
    (process / 'cmdline').write_bytes(b'python3\0/checkout/worker_loop.py\0')
    return root, process


@pytest.mark.parametrize('failure', ['directory', 'permission', 'malformed', 'missing_stat', 'reused'])
def test_incomplete_census_cannot_certify_a_drain(tmp_path, monkeypatch, failure):
    root, process = private_proc(tmp_path)
    if failure == 'directory':
        root = tmp_path / 'unavailable-proc'
    elif failure == 'malformed':
        (process / 'stat').write_text('broken stat')
    elif failure == 'missing_stat':
        (process / 'stat').unlink()
    elif failure == 'permission':
        original = Path.read_bytes
        def unreadable(path):
            if path == process / 'cmdline':
                raise PermissionError('census denied')
            return original(path)
        monkeypatch.setattr(Path, 'read_bytes', unreadable)
    else:
        original = Path.read_bytes
        def reused(path):
            result = original(path)
            if path == process / 'cmdline':
                (process / 'stat').write_text('101 (replacement) S ' + '0 ' * 18 + '901 0')
            return result
        monkeypatch.setattr(Path, 'read_bytes', reused)
    gate = tmp_path / 'maintenance.json'
    gate.write_text(json.dumps({'draining': True, 'changed_unix': 12.5}))
    client = agent(tmp_path, gate)
    client.procs = lambda: upgrade.proc_census(root)
    client.ensure_parked_root()
    (client.parked_root / upgrade.park_marker_name(101, '900', 12.5)).touch()
    evidence = client.drain_evidence({'draining': True, 'active_scopes': 0})
    assert evidence['drained'] is False
    assert evidence['evidence_errors']


def test_gate_changing_during_census_does_not_certify_either_drain(tmp_path):
    gate = tmp_path / 'maintenance.json'
    gate.write_text(json.dumps({'draining': True, 'changed_unix': 12.5}))
    client = agent(tmp_path, gate)
    def change_gate():
        gate.write_text(json.dumps({'draining': True, 'changed_unix': 13.5}))
        return []
    client.procs = change_gate
    assert client.drain_evidence({'draining': True, 'active_scopes': 0})['drained'] is False


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
    assert upgrade.proc_census(root) == []
