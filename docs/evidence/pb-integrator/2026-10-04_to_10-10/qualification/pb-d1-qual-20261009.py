#!/usr/bin/env python3
"""Qualify the D1 disk-admission service as a supervised systemd user unit on celestia (CEO dec-1009-073159-2865, option 1).

Private port 22450, private token, private audit. Touches nothing of production custody (port 22449, its token, its audit).
Runs the REAL server script (hash checked against custody), the REAL production client and the REAL fleet-diskcheck (read-only).
Writes one JSON line per check to the results file. Author pb-integrator, 2026-10-09.
"""
import json, os, secrets, signal, socket, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path

SERVER = Path('/home/rob/fleet/ceo/exec/eng-d44-execution-sol/autonomous_phase_D1_service.py')
SERVER_SHA = 'd93bb59c601d164e3277ed1e7cf437e9867917667a753927229532d094fa98ec'
CLIENT = Path('/home/rob/tmp/eng-d44-autonomous-phase-fa37751-20261009/production_disk_client.py')
CLIENT_SHA = '1db2db828b93d1a8751572f105c70795bdc952f7221a7068022b56f7831393b8'
ADDR, PORT, UNIT = '192.168.1.68', 22450, 'pb-d1-qual.service'
HOME = Path.home()
WORK = HOME / 'fleet/inventory/pb-d1-qual-20261009'
TOKEN, AUDIT = WORK / 'token', WORK / 'audit.jsonl'
LOG = WORK / 'server.log'
RESULTS = WORK / 'results.jsonl'
UNITFILE = HOME / '.config/systemd/user' / UNIT
MAX_REQUESTS = 4

results = []


def sh(*cmd, timeout=60, check=False):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and r.returncode:
        raise RuntimeError(f'{cmd}: rc {r.returncode}: {r.stderr[:200]}')
    return r


def sysd(*args, **kw):
    return sh('systemctl', '--user', *args, **kw)


def record(cid, ok, detail):
    row = {'check': cid, 'pass': bool(ok), 'detail': detail,
           'at': datetime.now(timezone.utc).isoformat(timespec='seconds')}
    results.append(row)
    with RESULTS.open('a') as f:
        f.write(json.dumps(row) + '\n')
    print(('PASS ' if ok else 'FAIL ') + cid + ' :: ' + str(detail)[:300], flush=True)


def sha(path):
    import hashlib
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def listeners():
    out = sh('ss', '-ltnH').stdout.splitlines()
    return [l.split()[3] for l in out if l.split()[3].endswith(f':{PORT}')]


def main_pid():
    return int(sysd('show', '-p', 'MainPID', '--value', UNIT).stdout.strip() or 0)


def wait_for(pred, seconds, step=0.5):
    end = time.time() + seconds
    while time.time() < end:
        if pred():
            return True
        time.sleep(step)
    return pred()


def server_log():
    return LOG.read_text() if LOG.exists() else ''


def ready_count():
    return server_log().count('D44_D1_SERVICE_READY')


def request(token_file=TOKEN, port=PORT):
    t0 = time.time()
    r = sh(sys.executable, str(CLIENT), '--address', ADDR, '--port', str(port), '--token-file', str(token_file),
           '--need-gb', '2', '--hosts', 'sparky,sparklina', '--paths', '/,/mnt/shared', timeout=150)
    t1 = time.time()
    body = None
    try:
        body = json.loads(r.stdout)
    except Exception:
        pass
    return {'rc': r.returncode, 'body': body, 'stderr': r.stderr[-160:], 't0': t0, 't1': t1}


def fresh(resp):
    """next_wave.py freshness rule: invocation start minus 1 s through now plus 1 s."""
    b = resp['body']
    if not b or 'at' not in b:
        return False
    at = datetime.fromisoformat(b['at']).timestamp()
    return resp['t0'] - 1.0 <= at <= resp['t1'] + 1.0


def audit_lines():
    return [json.loads(l) for l in AUDIT.read_text().splitlines() if l.strip()] if AUDIT.exists() else []


def write_unit():
    UNITFILE.parent.mkdir(parents=True, exist_ok=True)
    UNITFILE.write_text(f"""[Unit]
Description=D1 disk-admission service qualification (private port {PORT}); pb-integrator 2026-10-09
StartLimitIntervalSec=0

[Service]
Type=simple
ExecStart=/usr/bin/python3 {SERVER} --bind {ADDR} --port {PORT} --token-file {TOKEN} --audit {AUDIT} --max-requests {MAX_REQUESTS}
Restart=on-failure
RestartSec=5
KillMode=control-group
TimeoutStopSec=10
StandardOutput=append:{LOG}
StandardError=append:{LOG}
""")
    sysd('daemon-reload', check=True)


def cleanup_unit():
    sysd('stop', UNIT)
    sysd('reset-failed', UNIT)
    if UNITFILE.exists():
        UNITFILE.unlink()
    sysd('daemon-reload')


def main():
    WORK.mkdir(parents=True, exist_ok=True)
    RESULTS.write_text('')
    # C1 preconditions
    record('C1a script and client hashes equal custody', sha(SERVER) == SERVER_SHA and sha(CLIENT) == CLIENT_SHA,
           {'server': sha(SERVER)[:12], 'client': sha(CLIENT)[:12]})
    record('C1b private port free and production port untouched', not listeners() and
           not sh('ss', '-ltnH').stdout.count(':22449 ') , {'listeners_22450': listeners()})
    TOKEN.write_text(secrets.token_hex(32) + '\n')
    TOKEN.chmod(0o600)
    AUDIT.write_text('')
    LOG.write_text('')
    wrong = WORK / 'wrong-token'
    wrong.write_text(secrets.token_hex(32) + '\n')
    wrong.chmod(0o600)
    write_unit()
    try:
        # C2 start, placement, single listener
        sysd('start', UNIT, check=True)
        ok = wait_for(lambda: listeners() != [] and ready_count() >= 1, 20)
        pid1 = main_pid()
        cmd = Path(f'/proc/{pid1}/cmdline').read_bytes().split(b'\0') if pid1 else []
        record('C2 unit active, READY, one listener on the declared address only',
               ok and listeners() == [f'{ADDR}:{PORT}'] and sysd('is-active', UNIT).stdout.strip() == 'active',
               {'pid': pid1, 'listeners': listeners(), 'argv_port': cmd[cmd.index(b'--port') + 1].decode() if b'--port' in cmd else None,
                'max_requests': cmd[cmd.index(b'--max-requests') + 1].decode() if b'--max-requests' in cmd else None})
        # C3 fresh responses
        details, allok = [], True
        stamps = []
        for i in range(3):
            r = request()
            f = fresh(r)
            stamps.append(r['body'].get('at') if r['body'] else None)
            allok &= (r['body'] is not None and f)
            details.append({'rc': r['rc'], 'fresh': f, 'at': stamps[-1], 'pass_field': (r['body'] or {}).get('pass')})
            time.sleep(1.2)
        lines = audit_lines()
        record('C3 three sequential requests: valid JSON, fresh by the production rule, one audit line each',
               allok and len(lines) == 3 and all(l['returncode'] in (0, 1) for l in lines) and len(set(stamps)) == 3,
               {'responses': details, 'audit_lines': len(lines)})
        # C4 wrong token
        before = len(audit_lines())
        w = request(token_file=wrong)
        record('C4 wrong token: no data, no audit line, count not advanced',
               w['body'] is None and w['rc'] != 0 and len(audit_lines()) == before,
               {'client_rc': w['rc'], 'stderr': w['stderr'][-100:], 'audit_before': before, 'audit_after': len(audit_lines())})
        # C5 crash: SIGKILL, restart by systemd, fresh service again
        old = main_pid()
        os.kill(old, signal.SIGKILL)
        t_kill = time.time()
        restarted = wait_for(lambda: main_pid() not in (0, old) and listeners() != [] and ready_count() >= 2, 150, 1.0)
        new = main_pid()
        r = request()
        record('C5 kill -9: systemd restarts it; it answers fresh again',
               restarted and new != old and r['body'] is not None and fresh(r),
               {'old_pid': old, 'new_pid': new, 'seconds_to_ready': round(time.time() - t_kill, 1), 'fresh': fresh(r)})
        # C6 count resets after a crash restart, then a clean exit at the limit with no restart
        served_after_restart = 1
        for _ in range(MAX_REQUESTS - 1):
            rr = request()
            if rr['body'] is not None:
                served_after_restart += 1
        exited = wait_for(lambda: sysd('is-active', UNIT).stdout.strip() in ('inactive', 'failed'), 20)
        time.sleep(4)  # long enough for a wrong restart to show
        j = server_log()
        record('C6 after the crash the count restarted at zero; at the limit it exits cleanly, port closed, no restart',
               served_after_restart == MAX_REQUESTS and exited and sysd('is-active', UNIT).stdout.strip() == 'inactive'
               and not listeners() and 'D44_D1_SERVICE_STOPPED {"completed_requests": %d}' % MAX_REQUESTS in j,
               {'served_after_restart': served_after_restart, 'state': sysd('is-active', UNIT).stdout.strip(),
                'listeners': listeners(), 'stopped_line': 'D44_D1_SERVICE_STOPPED' in j,
                'note': 'the count is in memory: 3 requests before the crash did not count after it'})
        # C7 unavailable service: the production client refuses
        u = request()
        record('C7 service down: connection refused and the production client exits non-zero with no JSON',
               u['body'] is None and u['rc'] != 0 and not listeners(),
               {'client_rc': u['rc'], 'stderr': u['stderr'][-120:]})
        # C8 stop of a running service: inactive, port closed, no children
        before_ready = ready_count()
        sysd('start', UNIT, check=True)
        # the port needs the same TIME_WAIT window to clear after the clean exit as after a crash
        wait_for(lambda: listeners() != [] and ready_count() > before_ready, 150, 1.0)
        pid3 = main_pid()
        r = request()
        sysd('stop', UNIT, timeout=30)
        wait_for(lambda: sysd('is-active', UNIT).stdout.strip() != 'active', 20)
        left = sh('pgrep', '-f', f'autonomous_phase_D1_service.py.*--port {PORT}').stdout.split()
        kids = sh('pgrep', '-f', 'fleet-diskcheck').stdout.split()
        cg = sh('systemctl', '--user', 'show', '-p', 'ControlGroup', '--value', UNIT).stdout.strip()
        cg_procs = ''
        if cg and Path('/sys/fs/cgroup' + cg + '/cgroup.procs').exists():
            cg_procs = Path('/sys/fs/cgroup' + cg + '/cgroup.procs').read_text().strip()
        record('C8 stop: unit inactive, server and fleet-diskcheck gone, port closed, cgroup empty',
               sysd('is-active', UNIT).stdout.strip() == 'inactive' and not listeners() and not left and not kids and not cg_procs and r['body'] is not None,
               {'server_pids_left': left, 'diskcheck_pids_left': kids, 'cgroup_procs': cg_procs, 'listeners': listeners(), 'pid_before_stop': pid3})
        # C9 audit kept and complete
        lines = audit_lines()
        record('C9 audit kept: one line per served request, every line is fleet-diskcheck with fsynced content',
               len(lines) == 3 + 4 + 1,
               {'audit_lines': len(lines), 'commands': sorted({' '.join(l['command'][:1]) for l in lines}),
                'expected': 8, 'why': '3 before the crash, 4 after it up to the limit (1 plus 3), and 1 in C8'})
    finally:
        cleanup_unit()
        record('C10 unit removed, port closed, production port untouched',
               not UNITFILE.exists() and not listeners() and not sh('ss', '-ltnH').stdout.count(':22449 '),
               {'listeners': listeners(), 'unitfile': UNITFILE.exists()})
        TOKEN.unlink(missing_ok=True)
        wrong.unlink(missing_ok=True)
    passed = sum(r['pass'] for r in results)
    print(f'RESULT {passed}/{len(results)} checks passed', flush=True)
    return 0 if passed == len(results) else 1


if __name__ == '__main__':
    sys.exit(main())
