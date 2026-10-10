#!/usr/bin/env python3
"""Start the private D1 coverage unit on port 22450 for eng-d44-execution-sol's 285 fresh client checks plus recovery calls.
Private token and audit (not custody). Unchanged custody server script. Sends no real-token request. Author pb-integrator, 2026-10-09."""
import hashlib, json, secrets, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path
HOME = Path.home()
SERVER = '/home/rob/fleet/ceo/exec/eng-d44-execution-sol/autonomous_phase_D1_service.py'
SERVER_SHA = 'd93bb59c601d164e3277ed1e7cf437e9867917667a753927229532d094fa98ec'
ADDR, PORT, UNIT, MAXREQ = '192.168.1.68', 22450, 'pb-d44-d1-cov-22450.service', 400
W = HOME / 'fleet/inventory/pb-d1-cov-20261009'; W.mkdir(exist_ok=True)
TOKEN, AUDIT, LOG, PROOF = W / 'token', W / 'audit.jsonl', W / 'server.log', W / 'proof.json'
UNITFILE = HOME / '.config/systemd/user' / UNIT
sh = lambda *c, **k: subprocess.run(c, capture_output=True, text=True, **k)
sysd = lambda *a: sh('systemctl', '--user', *a)
listeners = lambda p: [l.split()[3] for l in sh('ss', '-ltnH').stdout.splitlines() if l.split()[3].endswith(f':{p}')]
assert hashlib.sha256(Path(SERVER).read_bytes()).hexdigest() == SERVER_SHA, 'server script differs from custody'
assert not listeners(PORT) and not listeners(22449), 'a required port is not free'
assert sysd('is-active', UNIT).stdout.strip() != 'active'
TOKEN.write_text(secrets.token_hex(32) + '\n'); TOKEN.chmod(0o600)
AUDIT.write_text(''); LOG.write_text('')
UNITFILE.write_text(f"""[Unit]
Description=D44 D1 service, private coverage window, port {PORT} (CEO dec-1009-073159-2865; pb-integrator 2026-10-09). Stop it when coverage ends.
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
sysd('daemon-reload'); sysd('start', UNIT)
end = time.time() + 30
while time.time() < end and not (listeners(PORT) and 'D44_D1_SERVICE_READY' in LOG.read_text()): time.sleep(0.5)
pid = int(sysd('show', '-p', 'MainPID', '--value', UNIT).stdout.strip() or 0)
enter = sysd('show', '-p', 'ActiveEnterTimestamp', '--value', UNIT).stdout.strip()
import socket
with socket.create_connection((ADDR, PORT), timeout=30) as s:
    s.sendall(b'not-the-token\n'); data = s.recv(4096)
ok = sysd('is-active', UNIT).stdout.strip() == 'active' and listeners(PORT) == [f'{ADDR}:{PORT}'] and not listeners(22449) and data == b'' and AUDIT.read_text() == ''
proof = {'unit': UNIT, 'pid': pid, 'epoch_active_enter': enter, 'address': f'{ADDR}:{PORT}', 'max_requests': MAXREQ, 'token_file': str(TOKEN), 'audit': str(AUDIT), 'log': str(LOG),
         'production_port_22449_listeners': listeners(22449), 'wrong_token_probe_bytes': len(data), 'audit_empty_after_probe': AUDIT.read_text() == '', 'ok': ok,
         'stop': f'systemctl --user stop {UNIT}', 'at': datetime.now(timezone.utc).isoformat(timespec='seconds')}
PROOF.write_text(json.dumps(proof, indent=1)); print(json.dumps(proof)); sys.exit(0 if ok else 1)
