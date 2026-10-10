# Guarded install of jsonschema into the pool venv on dl380g10 (CEO request 2026-10-08, tessera#1042).
# Runs under /usr/bin/python3 so this action is not itself a user of the venv.  Waits for a quiet pool,
# records before and after, installs the five resolved versions, and requires the diff to be exactly them.
import json, os, subprocess, sys, time, shutil
V = '/home/rob/venvs/pb-cpu'
PY = V + '/bin/python'
OUT = '/mnt/shared/fleet-ceo/pb-cpu-jsonschema-20261008'
WANT = {'attrs': '26.1.0', 'jsonschema': '4.26.0', 'jsonschema-specifications': '2025.9.1',
        'referencing': '0.37.0', 'rpds-py': '2026.9.1'}
LIMIT_S, QUIET_SAMPLES, GAP_S = 45 * 60, 3, 10
me = os.getpid()


def users():
    out = subprocess.run(['ps', '-eo', 'pid,ppid,args'], capture_output=True, text=True).stdout.splitlines()[1:]
    found = []
    for line in out:
        parts = line.split(None, 2)
        if len(parts) < 3: continue
        pid, args = int(parts[0]), parts[2]
        if pid == me: continue
        if args.startswith(PY + ' ') or args == PY:
            found.append((pid, args[:110]))
    return found


def freeze():
    r = subprocess.run([PY, '-m', 'pip', 'freeze', '--all', '--disable-pip-version-check'], capture_output=True, text=True)
    return sorted(l.strip() for l in r.stdout.splitlines() if l.strip())


def log(msg):
    print(time.strftime('%H:%M:%SZ', time.gmtime()), msg, flush=True)


free_gb = shutil.disk_usage(V).free / 1e9
if free_gb < 2:
    log(f'ABORT: only {free_gb:.1f} GB free'); sys.exit(4)
t0, quiet = time.time(), 0
while quiet < QUIET_SAMPLES:
    u = users()
    quiet = quiet + 1 if not u else 0
    if not u:
        time.sleep(GAP_S); continue
    if time.time() - t0 > LIMIT_S:
        log(f'NOT QUIET after {int((time.time()-t0)/60)} min; {len(u)} users, e.g. {u[:2]}; nothing installed'); sys.exit(3)
    log(f'waiting: {len(u)} processes use the venv, oldest pid {u[0][0]}'); time.sleep(30)
log(f'quiet for {QUIET_SAMPLES} samples after {int(time.time()-t0)} s; installing')
before = freeze(); open(OUT + '/freeze-before.txt', 'w').write('\n'.join(before) + '\n')
spec = [f'{k}=={v}' for k, v in WANT.items()]
r = subprocess.run([PY, '-m', 'pip', 'install', '--no-input', '--disable-pip-version-check', '--no-deps'] + spec,
                   capture_output=True, text=True)
open(OUT + '/pip-install.log', 'w').write(r.stdout + '\n' + r.stderr)
log(f'pip rc={r.returncode}')
after = freeze(); open(OUT + '/freeze-after.txt', 'w').write('\n'.join(after) + '\n')
added = sorted(set(after) - set(before)); removed = sorted(set(before) - set(after))
want_lines = sorted(f'{k}=={v}' for k, v in WANT.items())
ok = (r.returncode == 0 and not removed and added == want_lines)
log(f'added={added}'); log(f'removed={removed}')
imp = subprocess.run([PY, '-c', 'import jsonschema,sys;print(jsonschema.__version__)'], capture_output=True, text=True)
log(f'import jsonschema: rc={imp.returncode} {imp.stdout.strip()} {imp.stderr.strip()[-120:]}')
json.dump(dict(ok=ok and imp.returncode == 0, added=added, removed=removed, pip_rc=r.returncode, waited_s=int(time.time() - t0),
               jsonschema=imp.stdout.strip()), open(OUT + '/result.json', 'w'), indent=1)
sys.exit(0 if ok and imp.returncode == 0 else 5)
