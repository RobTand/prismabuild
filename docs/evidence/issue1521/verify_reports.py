"""Read PB reports, authoritative endings, log blobs and CAS payloads."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
from prismabuild import core

parser = argparse.ArgumentParser()
parser.add_argument('reports', nargs='+')
parser.add_argument('--out', required=True)
args = parser.parse_args()
verified = []
for report_name in args.reports:
    report = Path(report_name)
    for row in json.loads(report.read_text()):
        key = row['action_key']
        if key is None:
            raise ValueError(f'{report}: shard has no key')
        queue = Path('/mnt/shared/prismabuild-fleet/pb-queue')
        endings = [queue / state / (key + '.json') for state in ('done', 'failed', 'withdrawn')]
        terminal_path = next(path for path in endings if path.exists())
        terminal = json.loads(terminal_path.read_text())
        assert terminal['action_key'] == key
        detail = terminal['detail']
        expected = row['returncode']
        assert detail['returncode'] == expected, (key, detail['returncode'], expected)
        logs = {}
        for stream in ('stdout', 'stderr'):
            descriptor = detail.get(stream + '_log')
            if not descriptor:
                continue
            path = queue / descriptor['path']
            raw = path.read_bytes()
            assert hashlib.sha256(raw).hexdigest() == descriptor['sha256']
            assert len(raw) == descriptor['bytes']
            logs[stream] = {'path': str(path), 'sha256': descriptor['sha256'], 'bytes': len(raw)}
        cas = core.PrismaBuildCAS(terminal['cas_root'])
        action = cas.read_action_request(key)
        receipt = cas.lookup(action)
        if expected == 0:
            assert terminal['status'] == 'executed'
            assert receipt is not None
            payload = cas.result_path(receipt, action)
            result = {'receipt_sha256': receipt['receipt_sha256'],
                      'receipt_path': str(cas.receipt_path(key)),
                      'result_path': str(payload), 'result_bytes': payload.stat().st_size}
        else:
            assert terminal['status'] in {'failed', 'withdrawn'}
            assert receipt is None
            result = {'receipt': 'absent as expected for failing regression'}
        completion = row.get('test_completion', {})
        assert completion.get('status') == 'complete'
        assert not row.get('skipped'), (key, row.get('skipped'))
        verified.append({'tests': len(completion.get('collected', [])), 'report': str(report), 'action_key': key, 'summary': row.get('summary'),
                         'returncode': expected, 'status': terminal['status'],
                         'terminal': str(terminal_path), 'host': terminal.get('finished_host'),
                         'cleanup': terminal.get('resource_scope_cleanup'),
                         'snapshot': terminal.get('checkout_snapshot'),
                         'logs': logs, **result})
Path(args.out).write_text(json.dumps(verified, indent=2) + '\n')
for row in verified:
    print(row['action_key'], row['host'], row['summary'], row['status'])
