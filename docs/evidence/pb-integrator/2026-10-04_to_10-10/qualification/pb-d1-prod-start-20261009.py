#!/usr/bin/env python3
"""Start the production D1 service as a systemd user unit (CEO dec-1009-073159-2865, option 1; qualification 11/11 passed on 22450).
Uses the unchanged custody script, token and audit. Sends NO real-token request (that would append to the custody audit and use the count).
Author pb-integrator, 2026-10-09."""
import hashlib, json, os, socket, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path
HOME = Path.home()
SERVER = '/home/rob/fleet/ceo/exec/eng-d44-execution-sol/autonomous_phase_D1_service.py'
TOKEN = '/mnt/shared/tessera-measurements/eng-ldlq-indomain-20261006/frozen-method-01/campaign-01/native-qualification-b929a3dc-20261008-01/autonomous-phase-fa37751-20261009-01/inputs/D1-service-token'
AUDIT = '/home/rob/fleet/ceo/exec/eng-d44-execution-sol/autonomous-phase-fa37751-D1-service-audit.jsonl'
SHA = {'server': 'd93bb59c601d164e3277ed1e7cf437e9867917667a753927229532d094fa98ec', 'token': '3cdafea97cb3ee1987a2b2dee8030c6d765d729b45f3cfe54f793df0b0d2ee17'}
ADDR, PORT, UNIT, MAXREQ = '192.168.1.68', 22449, 'pb-d44-d1-22449.service', 400
WORK = HOME / 'fleet/inventory/pb-d1-prod-20261009'; WORK.mkdir(exist_ok=True)
LOG = WORK / 'server.log'; PROOF = WORK / 'proof.json'
UNITFILE = HOME / '.config/systemd/user' / UNIT
sh = lambda *c, **k: subprocess.run(c, capture_output=True, text=True, **k)
sysd = lambda *a: sh('systemctl', '--user', *a)
H = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
listeners = lambda: [l.split()[3] for l in sh('ss', '-ltnH').stdout.splitlines() if l.split()[3].endswith(f':{PORT}')]
proof = {'at': datetime.now(timezone.utc).isoformat(timespec='seconds'), 'unit': UNIT, 'checks': {}}
def chk(k, ok, d): proof['checks'][k] = {'pass': bool(ok), 'detail': d}; print(('PASS ' if ok else 'FAIL ') + k + ' :: ' + str(d)[:260], flush=True)
audit_before = (len(Path(AUDIT).read_text().splitlines()), Path(AUDIT).stat().st_size)
other = sysd('is-active', 'eng-d44-production-d1.service').stdout.strip()
chk('P1 preconditions: custody hashes match, port free, the owner unit is not active, audit untouched',
    H(SERVER) == SHA['server'] and H(TOKEN) == SHA['token'] and not listeners() and other != 'active',
    {'server': H(SERVER)[:12], 'token': H(TOKEN)[:12], 'port_listeners': listeners(), 'owner_unit_state': other, 'audit_lines': audit_before[0]})
if not all(c['pass'] for c in proof['checks'].values()):
    PROOF.write_text(json.dumps(proof, indent=1)); sys.exit(2)
UNITFILE.write_text(f"""[Unit]
Description=D44 D1 disk-admission service, production, port {PORT} (CEO dec-1009-073159-2865; pb-integrator 2026-10-09). The phase owner stops it at phase end.
StartLimitIntervalSec=0

[Service]
Type=simple
ExecStart=/usr/bin/python3 {SERVER} --bind {ADDR} --port {PORT} --token-file {TOKEN} --audit {AUDIT} --max-requests {MAXREQ}
Restart=on-failure
RestartSec=5
KillMode=control-group
TimeoutStopSec=10
StandardOutput=append:{LOG}
StandardError=append:{LOG}
""")
sysd('daemon-reload')
r = sysd('start', UNIT)
end = time.time() + 30
while time.time() < end and not (listeners() and 'D44_D1_SERVICE_READY' in (LOG.read_text() if LOG.exists() else '')): time.sleep(0.5)
pid = int(sysd('show', '-p', 'MainPID', '--value', UNIT).stdout.strip() or 0)
argv = Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0') if pid else []
g = lambda f: argv[argv.index(f.encode()) + 1].decode() if f.encode() in argv else None
chk('P2 active, one listener on the declared address, exact arguments, nothing else on the port',
    sysd('is-active', UNIT).stdout.strip() == 'active' and listeners() == [f'{ADDR}:{PORT}'] and g('--bind') == ADDR and g('--port') == str(PORT)
    and g('--token-file') == TOKEN and g('--audit') == AUDIT and g('--max-requests') == str(MAXREQ),
    {'pid': pid, 'listeners': listeners(), 'max_requests': g('--max-requests'), 'enabled': sysd('is-enabled', UNIT).stdout.strip(), 'restart': sysd('show', '-p', 'Restart', '--value', UNIT).stdout.strip()})
ready = [l for l in LOG.read_text().splitlines() if 'D44_D1_SERVICE_READY' in l]
chk('P3 READY line names the fixed command and the limit', bool(ready) and '"max_requests": 400' in ready[-1] and 'fleet-diskcheck' in ready[-1], ready[-1][:220] if ready else None)
# a wrong-token probe: no response, and no audit line (the handler returns before the audit)
with socket.create_connection((ADDR, PORT), timeout=30) as s:
    s.sendall(b'not-the-token\n'); data = s.recv(4096)
audit_after = (len(Path(AUDIT).read_text().splitlines()), Path(AUDIT).stat().st_size)
chk('P4 wrong-token probe: no data, custody audit unchanged (no real request used the count)', data == b'' and audit_after == audit_before,
    {'audit_before': audit_before, 'audit_after': audit_after, 'bytes_received': len(data)})
chk('P5 custody files unchanged', H(SERVER) == SHA['server'] and H(TOKEN) == SHA['token'], {'server': H(SERVER)[:12], 'token': H(TOKEN)[:12]})
proof['summary'] = {'unit': UNIT, 'unit_file': str(UNITFILE), 'pid': pid, 'address': f'{ADDR}:{PORT}', 'max_requests': MAXREQ,
                    'stop_command': f'systemctl --user stop {UNIT}', 'log': str(LOG), 'audit': AUDIT}
PROOF.write_text(json.dumps(proof, indent=1))
ok = all(c['pass'] for c in proof['checks'].values()); print('RESULT', 'all pass' if ok else 'FAILED', flush=True); sys.exit(0 if ok else 1)
