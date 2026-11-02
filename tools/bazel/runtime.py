"""Give a remote test the environment the hosted gate gives a shard."""
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import venv

# .github/workflows/gate.yml and tools/ci/gate_run.py set these for every shard.
GATE_ENVIRONMENT = {
    'PRISMABUILD_TEST_TIMEOUT_S': '300',
    'OMP_NUM_THREADS': '1',
    'MKL_NUM_THREADS': '1',
    'OPENBLAS_NUM_THREADS': '1',
    'GIT_AUTHOR_NAME': 'PrismaBuild CI',
    'GIT_AUTHOR_EMAIL': 'ci@example.invalid',
    'GIT_COMMITTER_NAME': 'PrismaBuild CI',
    'GIT_COMMITTER_EMAIL': 'ci@example.invalid',
}
# What Bazel's test runner adds. A hosted shard has none of it, and PrismaBuild
# refuses some of it in action environments (BASH_FUNC_rlocation%%).
BAZEL_PREFIXES = ('TEST_', 'RUNFILES_', 'BASH_FUNC_', 'GTEST_', 'COVERAGE_', 'TESTBRIDGE_', 'LD_')
BAZEL_NAMES = {'JAVA_RUNFILES', 'PYTHON_RUNFILES', 'XML_OUTPUT_FILE', 'BAZEL_TEST', 'PYTHONSAFEPATH',
               'RUNTEST_PRESERVE_CWD', 'TZ', 'BUILD_EXECROOT'}


def gate_environment(environ, root, scratch):
    """The pytest process's environment: the caller's minus Bazel's, plus the gate's.

    gate_run also drops loader overrides (LD_*) so proof capture sees the real
    interpreter. TMPDIR and HOME are short: hosted shards use /home/rob/pbt17,
    and Bazel's TEST_TMPDIR is too long for the AF_UNIX sockets tests bind.
    """
    environment = {name: value for name, value in environ.items()
                   if not name.startswith(BAZEL_PREFIXES) and name not in BAZEL_NAMES}
    environment.update(GATE_ENVIRONMENT)
    environment['TMPDIR'] = environment['HOME'] = str(scratch)
    # gate_run puts tools/ci first; the gate workflow adds src and experiments.
    environment['PYTHONPATH'] = os.pathsep.join(
        [str(root / 'tools/ci'), str(root / 'src'), str(root / 'experiments')])
    return environment


def short_scratch(private):
    """A new directory as near the root as possible, on TEST_TMPDIR's filesystem.

    Hosted shards use /home/rob/pbt17 on a local disk. A remote executor's /tmp
    is overlay, which PrismaBuild's state directories refuse, and TEST_TMPDIR
    is too deep for AF_UNIX sockets; its filesystem's top writable directory
    (/buildbuddy-execroot) is neither.
    """
    device, base = private.stat().st_dev, private
    for parent in private.parents:
        if parent.stat().st_dev != device or not os.access(parent, os.W_OK | os.X_OK):
            break
        base = parent
    return Path(tempfile.mkdtemp(prefix='t', dir=base))


# What tools/ci/gate_setup.py and the hosted image give a shard, and the Remote
# Bazel runner lacks until installed.
RUNNER_PACKAGES = {'bwrap': 'bubblewrap', 'zstd': 'zstd', 'gh': 'gh', 'rsync': 'rsync', 'ip': 'iproute2'}
CGROUP_ROOT = Path('/sys/fs/cgroup/prismabuild-tests')


def checkout_of(runfiles):
    """The runner's git checkout: runfiles of a local, unsandboxed test link into it."""
    root = (runfiles / 'conftest.py').resolve().parent
    if not (root / '.git').exists():
        raise RuntimeError(f'runner-lane test needs a git checkout; {root} is not one. '
                           'Run it on a Remote Bazel runner (docs/bazel.md).')
    return root


def prepare_runner(root, run=subprocess.run):
    """Make the runner VM look like a hosted gate shard, once, under a lock.

    Nothing here may leave a process running: Bazel's process wrapper waits for
    every descendant of a test, so a daemon started here would hold the test.
    """
    scratch = Path(json.loads((root / 'tools/fleet/fleet_boxes.json').read_text())['action_tmpdir'])
    with open('/tmp/prismabuild-runner-prepare.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not scratch.is_dir():
            run(['sudo', '-n', 'install', '-d', '-m', '0700', '-o', str(os.getuid()), '-g', str(os.getgid()),
                 str(scratch)], check=True)
        missing = sorted(package for tool, package in RUNNER_PACKAGES.items() if not shutil.which(tool))
        if missing:
            apt = ['sudo', '-n', 'env', 'DEBIAN_FRONTEND=noninteractive', 'apt-get', '-o', 'Acquire::Retries=3']
            run([*apt, 'update', '-qq'], check=True)
            run([*apt, 'install', '-y', '-qq', '--no-install-recommends', *missing], check=True)
        if Path('/proc/sys/kernel/apparmor_restrict_unprivileged_userns').exists():
            run(['sudo', '-n', 'sysctl', '-w', 'kernel.apparmor_restrict_unprivileged_userns=0'], check=True)
    # A hosted runner's processes live in a systemd service's cgroup, not the
    # root one; each test gets its own, delegated to it.
    leaf = CGROUP_ROOT / str(os.getpid())
    run(['sudo', '-n', 'sh', '-c', f'mkdir -p {leaf} && chown -R {os.getuid()}:{os.getgid()} {leaf} '
         f'&& echo {os.getpid()} > {leaf}/cgroup.procs'], check=True)
    return scratch


def prepare_remote_test(root):
    """Short scratch and an interpreter whose children keep Bazel's packages.

    Returns the scratch directory and the environment for pytest.
    """
    if os.environ.get('PB_BAZEL_LANE') == 'runner':
        # Exactly the hosted scratch root, shared as hosted xdist workers share it.
        scratch = prepare_runner(root)
    else:
        scratch = short_scratch(Path(os.environ['TEST_TMPDIR']).resolve())
    # Python's -I ignores PYTHONPATH. A real venv with a .pth keeps declared
    # Bazel dependencies for children that clear their environment.
    environment = Path(os.environ['TEST_TMPDIR']).resolve() / 'python-environment'
    if not (environment / 'pyvenv.cfg').exists():
        # Executors hardlink inputs from a shared file cache, and the
        # runfiles interpreter's ctime moves while other actions link and
        # unlink it; PrismaBuild refuses an action executable that changes
        # while it is read. Each test therefore runs a private copy on both
        # architectures, independent of other actions' cache activity.
        venv.EnvBuilder(with_pip=False, symlinks=False).create(environment)
    site = next(environment.glob('lib/python*/site-packages'))
    paths = [str(Path(path).resolve()) for path in sys.path if path]
    (site / 'bazel-runfiles.pth').write_text('\n'.join(paths) + '\n')
    sys.executable = str(environment / 'bin/python')
    # A pip install puts console scripts such as py-spy beside the interpreter,
    # where PrismaBuild's profiler looks; so do these.
    for script in sorted(Path('..').glob('*pb_pip*/bin/*')):
        link = environment / 'bin' / script.name
        if not link.exists():
            link.symlink_to(script.resolve())
    variables = gate_environment(os.environ, root, scratch)
    variables.pop('PB_BAZEL_LANE', None)
    # Tests run python3, python and those scripts from PATH.
    variables['PATH'] = os.pathsep.join([str(environment / 'bin'), os.environ.get('PATH', '')])
    return scratch, variables
