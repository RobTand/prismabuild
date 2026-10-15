"""Installed-root physical evidence over private census and marker fixtures."""
import json
from pathlib import Path

import pytest

from prismabuild import park_proof
from test_agent_proves_the_box_stopped import CENSUS, GENERATION, agent, private_proc, upgrade
from test_client_proof_prior_trust import tmp_path
from test_installed_park_proof_loader import installed_leaf


pytestmark = pytest.mark.usefixtures("installed_leaf")


@pytest.mark.parametrize('script', [
    f'{GENERATION}/prewarm_loop.py',
    f'{GENERATION.removesuffix("/fleet")}/prewarm_loop.py',
    '/mnt/shared/prismabuild-fleet/repo/tools/prewarm_loop.py',
    '/home/rob/prismabuild/tools/fleet/prewarm_loop.py',
])
def test_storage_reader_must_park_before_the_host_reports_drained(tmp_path, script):
    gate = tmp_path / 'maintenance.json'
    gate.write_text(json.dumps({'draining': True, 'changed_unix': 12.5}))
    census = [(105, '904', ['python3', script, '--once'])]
    client = agent(tmp_path, gate, census=census)
    client.ensure_parked_root()
    status = {'draining': True, 'active_scopes': 0}

    # A prior drain and a reused PID cannot vouch for this reader.
    for start, stamp in [('904', 11.0), ('903', 12.5)]:
        (client.parked_root / f"105-{start}-{stamp}").touch()
    assert client.drain_evidence(status) == {
        'drained': False, 'unparked': [105], 'active_scopes': 0}

    (client.parked_root / "105-904-12.5").touch()
    assert client.drain_evidence(status) == {
        'drained': True, 'unparked': [], 'active_scopes': 0}


def test_the_report_names_what_is_still_admitting(tmp_path):
    gate = tmp_path / 'maintenance.json'
    gate.write_text(json.dumps({'draining': True, 'changed_unix': 12.5}))
    client = agent(tmp_path, gate, census=CENSUS)
    client.ensure_parked_root()
    for pid, start in upgrade.serving(CENSUS, park_proof.HOST_QUIESCENT)[1:]:
        (client.parked_root / f"{pid}-{start}-12.5").touch()

    evidence = client.drain_evidence({'draining': True, 'active_scopes': 0})

    assert evidence == {'drained': False, 'unparked': [101], 'active_scopes': 0}


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
    (client.parked_root / "101-900-12.5").touch()
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
