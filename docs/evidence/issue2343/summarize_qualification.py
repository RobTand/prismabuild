"""Index retained PB readbacks; the collectors perform terminal/CAS verification."""
import argparse
import hashlib
import json
from pathlib import Path


def summarize(root, filename, source, actions, tests):
    path = root / filename
    raw = path.read_bytes()
    rows = json.loads(raw)
    if len(rows) != actions or len({row['action_key'] for row in rows}) != actions:
        raise ValueError('unexpected action population: ' + filename)
    if sum(row['native_count'] for row in rows) != tests:
        raise ValueError('unexpected native population: ' + filename)
    trees = rows[0]['source_trees']
    for row in rows:
        if (row['expected_source'] != source or row['source_trees'] != trees
                or row['status'] != 'executed' or row['exit_code'] != 0
                or not row['cleanup_complete'] or not row['pbwait_result_verified']
                or not row['receipt']):
            raise ValueError('unqualified retained readback: ' + row['action_key'])
    fields = ['action_key', 'host', 'summary', 'files', 'native_count', 'run_nonce',
              'logs', 'receipt', 'snapshot', 'source_trees', 'cleanup_complete',
              'pbwait_result_verified']
    return {
        'schema': 'pb.issue2343.qualification_index.v1',
        'scope': 'source/component evidence; independent review and final integration pending',
        'expected_source': source,
        'source_trees': trees,
        'retained_readback': {'filename': filename, 'bytes': len(raw),
                              'sha256': hashlib.sha256(raw).hexdigest()},
        'actions': actions,
        'native_tests': tests,
        'failures': 0,
        'skips': 0,
        'worker_attestation_signature': 'not checked',
        'rows': [{field: row[field] for field in fields} for row in rows],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-root', required=True, type=Path)
    args = parser.parse_args()
    output = Path(__file__).resolve().parent
    for filename, source, actions, tests, target in [
        ('repeats-verification.json', 'ab0a19275f680b619ab3a7080392e4072f226b0b',
         30, 270, 'repeat-qualification.json'),
        ('final-qualification-verification.json', 'eb0eb392fc43228756727b79f91f66150d6b3c3a',
         44, 786, 'composition-qualification.json'),
    ]:
        value = summarize(args.evidence_root, filename, source, actions, tests)
        (output / target).write_text(json.dumps(value, indent=2) + '\n')
        print(json.dumps({'output': target, 'actions': actions, 'native_tests': tests}))
    value = summarize(args.evidence_root, 'integrated-qualification-verification.json',
                      '290042085b1e73e71d11c05f17038989acba6a29', 34, 776)
    value['scope'] = 'integrated source/component evidence; independent review and merge pending'
    (output / 'integrated-qualification.json').write_text(json.dumps(value, indent=2) + '\n')
    print(json.dumps({'output': 'integrated-qualification.json', 'actions': 34,
                      'native_tests': 776}))


if __name__ == '__main__':
    main()
