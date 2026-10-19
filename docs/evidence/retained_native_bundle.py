"""Read-only native, raw-log and physical-scope reconciliation for verified PB primaries."""
from collections import Counter
import hashlib
import json
from pathlib import Path


def read_native_action(directory, proof):
    directory = Path(directory)
    raw = (directory / 'terminal.json').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == proof['terminal_sha256']
    terminal = json.loads(raw)
    assert terminal['action_key'] == proof['action_key']
    cleanup = terminal['resource_scope_cleanup']
    export = cleanup['export']
    assert cleanup['complete'] and export['tickets_pending'] is False
    assert all(export[field] is True for field in ('empty', 'ok', 'settled', 'released', 'stopped'))
    for name in ('stdout_log', 'stderr_log'):
        descriptor = terminal['detail'][name]
        content = (directory / (name + '.log')).read_bytes()
        assert len(content) == descriptor['bytes']
        assert hashlib.sha256(content).hexdigest() == descriptor['sha256']
    succeeded = terminal['detail']['returncode'] == 0
    assert (directory / 'receipt.json').exists() == succeeded
    payload = directory / ('result.log' if succeeded else 'stdout_log.log')
    if succeeded:
        receipt = json.loads((directory / 'receipt.json').read_bytes())
        content = payload.read_bytes()
        assert len(content) == receipt['result']['bytes']
        assert hashlib.sha256(content).hexdigest() == receipt['result']['sha256']
    lines = payload.read_text().splitlines()

    def marker(prefix):
        rows = [json.loads(line[len(prefix):]) for line in lines if line.startswith(prefix)]
        assert len(rows) == 1, prefix
        return rows[0]

    completion, outcomes = marker('pbtest-completion: '), marker('pbtest-outcomes: ')
    assert completion['collection_complete'] and not completion['collect_only']
    assert not completion['collection_errors'] and not completion['duplicate_teardown']
    for field in ('missing_teardown', 'missing_outcomes', 'unexpected_nodeids', 'problems'):
        assert not completion.get(field)
    collected = set(completion['collected'])
    assert len(collected) == len(completion['collected'])
    assert collected == set(completion['teardown_finished']) == set(completion['outcome_nodeids'])
    assert collected == set(outcomes['collected'])
    for field in ('collect_only', 'deselected', 'uncounted', 'file_selection_error'):
        assert not outcomes[field]
    files = {node.split('::', 1)[0] for node in collected}
    assert files == set(outcomes['file_selection']['files'])
    assert not outcomes['file_selection']['ignored']
    if 'files' in proof:
        assert files == set(proof['files'])
    reports = outcomes['reports']
    assert dict(Counter(row[2] for row in reports)) == proof['counts']
    native = [row for row in reports if row[2] in {'passed', 'failed', 'skipped'}]
    assert len(native) == len(collected) and {row[0] for row in native} == collected
    assert all(row[1] == 'call' or row[2] == 'skipped' for row in native)
    extra = [row for row in reports if row[2] not in {'passed', 'failed', 'skipped'}]
    assert all(row[0] in collected and row[1:3] == ['call', 'subtests passed'] for row in extra)
    return dict(action_key=proof['action_key'], source=terminal['checkout_snapshot']['parent'],
                counts=dict(Counter(row[2] for row in native)), files=files,
                nodes={row[0]: row[2] for row in native}, reports=native,
                auxiliary=extra, export=export, succeeded=succeeded,
                terminal_sha256=proof['terminal_sha256'], lines=lines)


def combine_actions(actions):
    nodes, files, reports, auxiliary = {}, set(), [], []
    for action in actions:
        assert not nodes.keys() & action['nodes'].keys()
        assert not files & action['files']
        nodes.update(action['nodes'])
        files.update(action['files'])
        reports.extend(action['reports'])
        auxiliary.extend(action['auxiliary'])
    return dict(nodes=nodes, files=files, reports=reports, auxiliary=auxiliary,
                counts=dict(Counter(nodes.values())),
                scopes=[dict(action_key=action['action_key'], export=action['export']) for action in actions])
