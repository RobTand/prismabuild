"""Run a supplied pytest command with four CPU competitors in two admitted CPUs."""
import json
import os
import subprocess
import sys


def main():
    admitted = sorted(os.sched_getaffinity(0))
    if len(admitted) < 2:
        raise RuntimeError('the load proof needs two admitted CPUs')
    os.sched_setaffinity(0, admitted[:2])
    children = []
    read_end, write_end = os.pipe()
    try:
        for _ in range(4):
            children.append(subprocess.Popen([
                sys.executable, '-c',
                'import os,sys; os.write(int(sys.argv[1]), b"r"); '
                'exec("while True: pass")', str(write_end)], pass_fds=(write_end,)))
        os.close(write_end)
        write_end = None
        ready = b''
        while len(ready) < len(children):
            observed = os.read(read_end, len(children) - len(ready))
            if not observed:
                raise RuntimeError('CPU competitor exited before readiness')
            ready += observed
        print('declared-load: ' + json.dumps({
            'admitted_cpus': admitted, 'execution_cpus': sorted(os.sched_getaffinity(0)),
            'cpu_competitors': len(children), 'native_threads_per_process': 1,
            'command': sys.argv[1:],
        }), flush=True)
        return subprocess.call(sys.argv[1:])
    finally:
        for child in children:
            child.kill()
        for child in children:
            child.wait()
        os.close(read_end)
        if write_end is not None:
            os.close(write_end)


if __name__ == '__main__':
    sys.exit(main())
