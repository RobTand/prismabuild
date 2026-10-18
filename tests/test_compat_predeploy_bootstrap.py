"""Explicit bootstrap assessment supplements the fresh source-window census."""
from copy import deepcopy
import json
from pathlib import Path
import runpy
import time
from types import SimpleNamespace

import pytest
from bootstrap_fixture import ROOT, bootstrap, assess_case
from prismabuild import installed_bootstrap
from prismabuild.compatibility_window import WindowError
from prismabuild.digest_primitives import raw_sha256


def inputs(tmp_path, case):
    plan, _, _, designation = case
    status = {'complete': True, 'nodes': [
        {'state': 'live', 'node': host, 'runtime_commit': plan['source']['commit']} for host in plan['roster']]}
    raw = json.dumps(status).encode()
    (tmp_path / 'status.json').write_bytes(raw)
    census = tmp_path / 'capture.json'
    census.write_text(json.dumps({'captured_unix': time.time(), 'status_file': 'status.json',
                                  'status_sha256': raw_sha256(raw), 'status_exit': 0}))
    window = tmp_path / 'window.json'
    window.write_text(json.dumps({'complete': True, 'rollback': plan['source'], 'generations': [plan['source']]}))
    designated = tmp_path / 'designation.json'
    designated.write_text(json.dumps(designation))
    assessment = tmp_path / 'assessment.json'
    assessment.write_text(json.dumps({'schema': 'prismabuild.installed_bootstrap.assessment.v1',
        'plan': plan, 'qualification_actions': ['1' * 64], 'rollback_designation': designation}))
    return SimpleNamespace(census=census, window=window, designation=designated,
        candidate=Path(plan['target']['directory']), generations=Path(plan['target']['directory']).parent,
        installed_bootstrap=assessment, out=tmp_path / 'result.json')


@pytest.mark.parametrize('draining', [False, True])
def test_explicit_predeploy_keeps_operator_only_rollback(tmp_path, bootstrap, monkeypatch, draining):
    args = inputs(tmp_path, bootstrap)
    if draining:
        path = args.census.parent / 'status.json'
        status = json.loads(path.read_bytes())
        for node in status['nodes']:
            node['state'] = 'draining'
        raw = json.dumps(status).encode()
        path.write_bytes(raw)
        envelope = json.loads(args.census.read_bytes())
        envelope['status_sha256'] = raw_sha256(raw)
        args.census.write_text(json.dumps(envelope))
    monkeypatch.setattr(installed_bootstrap, 'verify_qualification', lambda *a, **k: bootstrap[2])
    monkeypatch.setattr(installed_bootstrap, 'collect_responses', lambda *a: bootstrap[1])
    validate = runpy.run_path(str(ROOT / 'tests/compat/support/compat_predeploy.py'))['validate']
    validate(args)
    result = json.loads(args.out.read_bytes())
    assert result['protocols']['automatic_reachability'] is False
    assert result['rollback'] == bootstrap[0]['source']


@pytest.mark.parametrize('fault', ['default', 'stale', 'ambiguous', 'missing-root', 'designation'])
def test_predeploy_refuses_without_complete_fresh_bound_inputs(tmp_path, bootstrap, monkeypatch, fault):
    args = inputs(tmp_path, bootstrap)
    monkeypatch.setattr(installed_bootstrap, 'verify_qualification', lambda *a, **k: bootstrap[2])
    monkeypatch.setattr(installed_bootstrap, 'collect_responses', lambda *a: bootstrap[1])
    if fault == 'default':
        args.installed_bootstrap = None
    elif fault == 'stale':
        envelope = json.loads(args.census.read_bytes())
        envelope['captured_unix'] -= 1000
        args.census.write_text(json.dumps(envelope))
    elif fault == 'ambiguous':
        other = args.generations / 'duplicate'
        other.mkdir()
        (other / 'RUNTIME_VERSION.json').write_bytes((args.generations / 'old/RUNTIME_VERSION.json').read_bytes())
    elif fault == 'missing-root':
        bootstrap[1].rows.pop('two')
    else:
        value = json.loads(args.designation.read_bytes())
        value['automatic_reachability'] = True
        args.designation.write_text(json.dumps(value))
    validate = runpy.run_path(str(ROOT / 'tests/compat/support/compat_predeploy.py'))['validate']
    with pytest.raises(WindowError):
        validate(args)
    assert not args.out.exists()


def test_predeploy_keeps_real_qualification_index_refusal(tmp_path, bootstrap):
    args = inputs(tmp_path, bootstrap)
    validate = runpy.run_path(str(ROOT / 'tests/compat/support/compat_predeploy.py'))['validate']
    with pytest.raises(WindowError, match='sealed qualification index'):
        validate(args)
    assert not args.out.exists()
