"""Freeze repeated validation including the importing checkpoint acceptance test."""
import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    here = Path(__file__).resolve().parent
    checkout = here.parents[2]
    rows = json.loads((here / 'repeats.json').read_text())
    selection = [
        'tests/test_sample_profile.py',
        'tests/test_profile_primary_checkpoint.py',
        'tests/test_sample_docker_coverage.py',
        'tests/test_contained_deadline_profile.py',
        'tests/test_profile_window_deadline.py',
        'tests/test_profile_checkpoint_deadline_determinism.py',
    ]
    for row in rows:
        row['cwd'] = str(checkout)
        argv = row['argv']
        first_test = next(i for i, value in enumerate(argv)
                          if value.startswith('tests/'))
        row['argv'] = argv[:first_test] + selection
        if row['tags'] == ['gb10']:
            row['tags'] = []
        row['env']['TMPDIR'] = '/home/rob/pbt17'
    args.output.write_text(json.dumps(rows, indent=2) + '\n')


if __name__ == '__main__':
    main()
