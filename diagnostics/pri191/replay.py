"""Replay #1551's retained 73-file cohort with current main's collection owner.

Run only as an admitted PrismaBuild action. The first arm is an unprofiled
session; the second runs the identical cohort with collection call counting. Their
elapsed times are not a before/after performance comparison.
"""
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile
import time

BUNDLE_SHA = '95960cee30a40c2814a1526072db88339f705d119ea7727320afaa4436c45e38'
BUNDLE = Path('/mnt/shared/prismabuild-fleet/cas/blobs/95') / BUNDLE_SHA
SNAPSHOT = '984dacdc03391b24da868e4fca92d4ffa90f7f73'
PB_MAIN = '6185a6ee60f'
HERE = Path(__file__).resolve().parent
OWNER = HERE.parents[1] / 'src/prismabuild/pbtest_collection.py'


def load():
    return {'loadavg': list(os.getloadavg()),
            'cpu_pressure': Path('/proc/pressure/cpu').read_text()}


def run(command, **kwargs):
    return subprocess.run(command, check=True, **kwargs)


def main():
    assert os.environ.get('PRISMABUILD_ACTION_PROGRESS_HELPER'), 'admitted action required'
    import runpy
    commit = runpy.run_path(os.environ['PRISMABUILD_ACTION_PROGRESS_HELPER'])['commit']
    assert hashlib.sha256(BUNDLE.read_bytes()).hexdigest() == BUNDLE_SHA
    source = Path(tempfile.mkdtemp(prefix='pri191-', dir=os.environ['TMPDIR']))
    checkout = source / 'cohort'
    run(['git', 'clone', '--quiet', '--no-checkout', str(BUNDLE), str(checkout)])
    run(['git', '-C', str(checkout), 'checkout', '--quiet', '--detach', SNAPSHOT])
    print(json.dumps({'event': 'source', 'pb_main': PB_MAIN,
                      'owner_sha256': hashlib.sha256(OWNER.read_bytes()).hexdigest(),
                      'cohort_snapshot': SNAPSHOT, 'bundle_sha256': BUNDLE_SHA,
                      'python': sys.version, 'executable': sys.executable,
                      'host': platform.node(), 'scratch_free': shutil.disk_usage(source).free}), flush=True)
    commit(1, 'session')
    files = json.loads((HERE / 'files.json').read_text())
    environment = dict(os.environ, PYTHONPATH=str(HERE) + ':src:experiments',
                       PYTEST_ADDOPTS='', PYTEST_PLUGINS='',
                       PRISMAQUANT_DEV_MODE='1')
    results = []
    for index, profiled in enumerate([False, True]):
        phase = 'session' if not profiled else 'counts'
        output = source / phase
        output.mkdir()
        environment.update(PRI191_OUTPUT=str(output), PRI191_PROFILED=str(int(profiled)),
                           PRI191_OWNER=str(OWNER), PRI191_FILES=str(HERE / 'files.json'))
        command = [sys.executable, '-m', 'pytest', '-p', 'probe_plugin', '-q',
                   '--no-header', '-p', 'no:cacheprovider', '-o', 'addopts=',
                   '--dist', 'worksteal', '-n', '4', '--basetemp', str(output / 'pytest')]
        command.extend(files)
        before = load()
        start = time.monotonic()
        with (output / 'stdout').open('w') as out, (output / 'stderr').open('w') as err:
            result = subprocess.run(command, cwd=checkout, env=environment,
                                    stdout=out, stderr=err, timeout=900)
        elapsed = time.monotonic() - start
        actors = [json.loads(path.read_text()) for path in sorted(output.glob('*.json'))]
        row = {'phase': phase, 'returncode': result.returncode, 'wall_s': elapsed,
               'load_before': before, 'load_after': load(), 'actors': actors,
               'stdout_tail': (output / 'stdout').read_text()[-12000:],
               'stderr_tail': (output / 'stderr').read_text()[-12000:]}
        results.append(row)
        print('PRI191_ARM ' + json.dumps(row, sort_keys=True), flush=True)
        commit(index + 2, 'counts' if not profiled else 'publish')
    destination = Path('/mnt/shared/paperclip-runs/PRI-191/replay-result.json')
    destination.write_text(json.dumps(results, sort_keys=True) + '\n')
    archive = Path('/mnt/shared/paperclip-runs/PRI-191/replay-artifacts.tar.gz')
    run(['tar', '-czf', str(archive), '-C', str(source), 'session', 'counts'])
    print('PRI191_ARTIFACTS ' + json.dumps({
        'archive': str(archive), 'sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
        'report': str(destination)}), flush=True)
    commit(4, 'publish')
    if any(row['returncode'] for row in results):
        return 1
    if not all(row['actors'] for row in results):
        return 2
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
