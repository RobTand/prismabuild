"""Issue #1897 functional live reads through an ordinary admitted PB action.

Elapsed seconds are unqualified observations, explicitly not measurements.
--baseline loads the pinned pre-change MCP source for the size comparison.
"""
import json
from pathlib import Path
import sys
import time
import subprocess
import types

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tools' / 'fleet'))
if '--baseline' in sys.argv:
    path = Path(__file__).resolve().parents[2] / 'tools' / 'fleet' / 'pbmcp.py'
    source = subprocess.check_output(['git', 'show', '38eed1488e6e1a622da295dd69ae041f63da6c08:tools/fleet/pbmcp.py'])
    pbmcp = types.ModuleType('pbmcp')
    pbmcp.__file__ = str(path)
    sys.modules['pbmcp'] = pbmcp
    exec(compile(source, str(path), 'exec'), pbmcp.__dict__)
else:
    import pbmcp

KEYS = [
    '473655276a87335e1b1f82fcfec15c5b2beef4f5852b1d133e52b676bb3ec364',
    '2bc51e6144d2e23bb6deaf3f389728eaf42962aaec82fa95fb495ea972f09c93',
    'cebc1c306559ba9ee67706fe60aee0f618e73236181c12f02c5af39c3fb57781',
]
session = pbmcp.Session(queue_root=pbmcp.fleet_census.DEFAULT_QUEUE_ROOT,
                        cas_root=Path('/mnt/shared/prismabuild-fleet/cas'),
                        repo_link=Path('/mnt/shared/prismabuild-fleet/repo'))
queries = [('pb_action', {'key_prefix': key}) for key in KEYS]
queries += [('pb_actions', {'keys': KEYS}), ('pb_status', {})]
if '--baseline' not in sys.argv:
    queries.append(('pb_status', {'detail': True}))
for name, args in queries:
    start = time.perf_counter()
    try:
        body = session.call(name, args)
        result = {'bytes': len(json.dumps(body).encode()),
                  'complete': body['complete'], 'timed_out': body['timed_out'],
                  'unavailable': body['unavailable']}
        if name == 'pb_action':
            result.update(found=body.get('found'), state=body.get('state'))
        if name == 'pb_actions':
            result.update(returned=body.get('returned'))
    except Exception as exc:
        result = {'error': str(exc)}
    print(json.dumps({'tool': name, 'arguments': args,
                      'evidence_kind': 'functional_read_only',
                      'elapsed_s_unqualified_not_measurement': time.perf_counter() - start, **result}), flush=True)
