"""C26 baseline REDs use native observations and existing public consumers."""
from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

from c26_native_support import (EXIT_BLOCK, ROOT, consume, dispatch, mq, pbtest,
                                record, repository)
from prismabuild import core, pool
import pbrun
import shape_gate

FILES = ['tests/test_one.py']
PASS = 'def test_one():\n    assert True\n'


@pytest.mark.parametrize('mode', ['normal', 'nonzero', 'timeout'])
def test_native_client_ending_cannot_disappear_from_mergeq(tmp_path, monkeypatch, mode):
    root = repository(tmp_path / 'suite', {FILES[0]: PASS},
                      conftest=EXIT_BLOCK if mode == 'timeout' else '')
    code, report, rows = dispatch(root, monkeypatch, mode=mode)
    assert code == (0 if mode == 'normal' else 1)
    assert rows[0]['ran'] is True
    assert record(rows[0]['output'])['reports'] == [
        ['tests/test_one.py::test_one', 'call', 'passed', None, None]]
    assert rows[0]['timed_out'] is (mode == 'timeout')
    got = consume(tmp_path, report, FILES, code)
    assert not got.failed and not got.runtime_refusal
    assert got.inconclusive == ([] if mode == 'normal' else FILES), (
        'C26: native passing output hid an unsuccessful client ending', rows, got)


def test_outer_client_exit_cannot_hide_behind_a_valid_green_report(tmp_path, monkeypatch):
    root = repository(tmp_path / 'suite', {FILES[0]: PASS})
    code, report, _ = dispatch(root, monkeypatch)
    assert code == 0
    got = consume(tmp_path, report, FILES, 1)
    assert got.inconclusive == FILES, 'C26: outer nonzero client exit was ignored'


@pytest.mark.parametrize('damage', ['whole-report', 'whole-row', 'foreign-assignment'])
def test_requested_files_remain_an_independent_obligation(tmp_path, monkeypatch, damage):
    files = ['tests/test_a.py', 'tests/test_b.py']
    root = repository(tmp_path / 'suite', {name: PASS for name in files})
    code, report, rows = dispatch(root, monkeypatch, shards=2)
    assert code == 0 and sorted(f for row in rows for f in row['files']) == files
    if damage == 'whole-report':
        changed = []
        missing = files
    elif damage == 'whole-row':
        changed = rows[:1]
        missing = rows[1]['files']
    else:
        changed = rows
        missing = list(rows[1]['files'])
        changed[1]['files'] = ['tests/not_requested.py']
    report.write_text(json.dumps(changed))
    got = consume(tmp_path, report, files, 0)
    assert set(missing) <= set(got.inconclusive), (
        'C26: report contents erased the independent requested file roster', got)


@pytest.mark.parametrize('damage', ['native-duplicate', 'combined-record-fault'])
def test_all_unreconciled_problems_reach_the_consumer(tmp_path, monkeypatch, damage):
    duplicate = '''
def pytest_collection_modifyitems(items):
    items.append(items[0])
'''
    root = repository(tmp_path / 'suite', {FILES[0]: PASS},
                      conftest=duplicate if damage == 'native-duplicate' else '')
    code, report, rows = dispatch(root, monkeypatch,
                                  mode='combined' if damage == 'combined-record-fault' else 'normal')
    if damage == 'native-duplicate':
        assert code == 1
        assert rows[0]['reconciliation']['collected_twice'] == [
            'tests/test_one.py::test_one']
    else:
        assert code == 1
        problems = rows[0]['reconciliation']['problems']
        for phrase in ('never ran', 'did not hold', 'collected twice',
                       'summary counts', 'file selection evidence'):
            assert any(phrase in p for p in problems), (phrase, problems)
    got = consume(tmp_path, report, FILES, code)
    assert got.inconclusive == FILES, 'C26: unreconciled native evidence was treated as complete'
    shared = getattr(got, 'problems', None)
    assert shared, 'C26: no structured consumer problems retained'
    rendered = json.dumps(shared)
    for problem in rows[0]['reconciliation']['problems']:
        assert problem in rendered, ('C26: a reconciliation problem was dropped', problem)


DUPLICATE_TEARDOWN = '''
import pytest

@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logreport(report):
    if (report.when == 'teardown' and report.nodeid.endswith('::test_one')
            and not getattr(report, '_c26_repeated', False)):
        report._c26_repeated = True
        # A native hook sends the non-final test's passing teardown twice.
        # The recorder must see the contradiction before it emits completion.
        report._c26_config.hook.pytest_runtest_logreport(report=report)

@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    outcome.get_result()._c26_config = item.config
'''


@pytest.mark.parametrize('broken', [False, True], ids=['positive', 'duplicate-teardown'])
def test_native_completion_contradiction_is_not_a_pass(tmp_path, monkeypatch, broken):
    root = repository(tmp_path / 'suite', {FILES[0]: PASS + '\ndef test_two():\n    pass\n'},
                      conftest=DUPLICATE_TEARDOWN if broken else '')
    code, report, rows = dispatch(root, monkeypatch)
    completion = record(rows[0]['output'], 'pbtest-completion: ')
    assert completion['duplicate_teardown'] is broken
    assert rows[0]['ran'] and not rows[0]['reconciliation']['problems']
    assert code == (1 if broken else 0), 'C26: native completion contradiction did not fail pbtest'
    got = consume(tmp_path, report, FILES, code)
    assert got.inconclusive == (FILES if broken else [])


@pytest.mark.parametrize('population', ['useful-plus-excluded', 'whole-empty', 'collect-only'])
def test_native_population_preservation(tmp_path, monkeypatch, population):
    tests = {'tests/test_ignored.py': 'raise AssertionError("ignored file imported")\n'}
    if population != 'whole-empty':
        tests['tests/test_useful.py'] = PASS
    root = repository(tmp_path / 'suite', tests,
                      conftest="collect_ignore = ['tests/test_ignored.py']\n")
    code, _, rows = dispatch(root, monkeypatch,
        shards=2 if population == 'useful-plus-excluded' else 1,
        pytest_args=['--co'] if population == 'collect-only' else [])
    assert code == (1 if population == 'whole-empty' else 0)
    assert all(not row['reconciliation']['missing_files'] for row in rows)
    count = sum(len(record(row['output'])['collected']) for row in rows)
    assert count == (0 if population == 'whole-empty' else 1)


GATE_INJECTION = '''
import pytest

@pytest.hookimpl(tryfirst=True)
def pytest_sessionfinish(session, exitstatus):
    node = @NODE@
    report = pytest.TestReport(nodeid=node, location=('tests/gate_campaign_shape.py', 1, ''),
        keywords={}, outcome='passed', longrepr=None, when='call', sections=[], duration=0)
    session.config.hook.pytest_runtest_logreport(report=report)
'''


def gate_receipt(tmp_path, monkeypatch, fault):
    function = shape_gate.REFERENCE_TEST.split('::')[1]
    names = sorted(shape_gate.REFERENCE_TABLES)
    test = ('import pytest\n@pytest.mark.parametrize("table", ' + repr(names) + ')\n'
            + f'def {function}(table):\n    assert table\n')
    node = ('tests/gate_campaign_shape.py::foreign' if fault == 'foreign'
            else shape_gate.reference_nodes()[0])
    root = repository(tmp_path / 'suite', {'tests/gate_campaign_shape.py': test},
                      conftest=GATE_INJECTION.replace('@NODE@', repr(node)) if fault else '')
    payload = [*pbtest.shard_entry(sys.executable, root), '-q', '-p', 'no:cacheprovider',
               'tests/gate_campaign_shape.py']
    runner = root / 'runner.py'
    runner.write_text('import os, pathlib, subprocess, sys\n'
        + 'result = subprocess.run(' + repr(payload) + ', stdout=subprocess.PIPE, '
          'stderr=subprocess.STDOUT, timeout=60)\n'
        + "pathlib.Path('gate.log').write_bytes(result.stdout)\n"
        + 'raise SystemExit(result.returncode)\n')
    for args in (['add', 'runner.py'], ['commit', '-qm', 'native runner']):
        done = pbrun.subprocess.run(['git', *args], cwd=root, capture_output=True, text=True)
        assert done.returncode == 0, done.stderr
    commit = pbrun.subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    cas = core.PrismaBuildCAS(tmp_path / 'cas')
    snapshot = pbrun.build_git_checkout_snapshot(root, cas=cas, max_bytes=16 << 20)
    action = core.seal_action({'schema': core.ACTION_SCHEMA_V2,
        'task': {'definition_id': 'tests/c26-native-gate', 'definition_version': 'v1',
            'task_class': 'generation', 'determinism': 'deterministic',
            'artifact_family': 'generic', 'artifact_kind': 'generic',
            'argv': [sys.executable, 'runner.py'], 'working_directory': '.', 'result_path': 'gate.log'},
        'inputs': [snapshot['input']], 'code_closure': core.build_code_closure(root, ['runner.py']),
        'params': {'checkout_snapshot': snapshot},
        'environment': {'variables': {'PYTHONPATH': 'src', 'OMP_NUM_THREADS': '1',
            'OPENBLAS_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1'}, 'toolchain': {}},
        'execution_scope': {'portability': 'portable', 'platform_key': None, 'host_class': None}})
    cas.publish_action_request(action)
    queue = pool.PoolQueue(tmp_path / 'queue')
    queue.publish(action_key=action['action_key'], cas_root=cas.root,
                  worker_script=ROOT / 'tools/fleet/worker.py', checkout_snapshot=snapshot,
                  resources={'cpu': 1, 'mem_gb': 1}, priority=-10)
    item = queue.claim(tags=[], capacity={'cpu': 1, 'mem_gb': 1})
    assert item is not None, 'private native receipt action was not claimed'
    # Core publishes only after the actual argv exits zero and source is attested.
    # A checkout worker's runtime-upgrade gate is unrelated to log judgment.
    ending = core.run_local_action(action, cas_root=cas.root, checkout_root=root,
                                   timeout_seconds=90)
    assert ending['status'] == 'executed' and ending.get('receipt'), ending
    terminal = queue.finish(action['action_key'], status=ending['status'],
                            detail={**ending, 'returncode': 0}, claim_snapshot=item)
    assert terminal.parent.name == pool.DONE
    receipt = cas.lookup(action)
    assert receipt is not None
    log = cas.result_path(receipt, action).read_text()
    (tmp_path / 'verified-gate.log').write_text(log)
    observed = record(log)
    assert sorted(observed['collected']) == shape_gate.reference_nodes()
    assert len(observed['reports']) == len(names) + bool(fault)
    return lambda: shape_gate.verify_gate_receipt(action_key=action['action_key'], commit=commit,
        checkout=root, queue_root=queue.root, cas_root=cas.root)


@pytest.mark.parametrize('fault', ['', 'foreign', 'duplicate'], ids=['positive', 'foreign', 'duplicate'])
def test_native_receipted_reference_run_uses_complete_log_judgment(tmp_path, monkeypatch, fault):
    verify = gate_receipt(tmp_path, monkeypatch, fault)
    if fault:
        with pytest.raises(shape_gate.ShapeGateFailure) as caught:
            verify()
        assert caught.value.reason == 'receipt_refused'
    else:
        assert verify()['verdict'] == 'passed'
