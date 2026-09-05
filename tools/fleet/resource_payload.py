#!/usr/bin/python3
"""Fixed privileged trampoline. Enter own cgroup, drop identity, then read argv."""
import argparse
import ctypes
import json
import os
from pathlib import Path
import pwd


def main():
    p=argparse.ArgumentParser();p.add_argument('--leaf',required=True)
    p.add_argument('--uid',required=True,type=int);p.add_argument('--command-fd',required=True,type=int)
    p.add_argument('--ready-fd',required=True,type=int);a=p.parse_args()
    if os.geteuid()!=0:raise SystemExit('resource payload must start from the privileged broker')
    # Writing 0 migrates this very process; no external numeric PID can race.
    fd=os.open(str(Path(a.leaf)/'cgroup.procs'),os.O_WRONLY|os.O_NOFOLLOW)
    try:os.write(fd,b'0')
    finally:os.close(fd)
    # Work must not inherit the authority daemon's host-OOM protection.
    Path('/proc/self/oom_score_adj').write_text('0')
    account=pwd.getpwuid(a.uid)
    os.initgroups(account.pw_name,account.pw_gid)
    os.setresgid(account.pw_gid,account.pw_gid,account.pw_gid)
    os.setresuid(a.uid,a.uid,a.uid)
    if os.getresuid()!=(a.uid,)*3 or os.getresgid()!=(account.pw_gid,)*3:
        raise SystemExit('could not drop payload identity')
    if ctypes.CDLL(None,use_errno=True).prctl(38,1,0,0,0)!=0:
        raise OSError(ctypes.get_errno(),'could not set no-new-privileges')
    # Client code, cwd and environment are consumed only after dropping root.
    with os.fdopen(a.command_fd,'rb') as source:body=json.loads(source.read(65537))
    if 'affinity' in body:os.sched_setaffinity(0,body['affinity'])
    os.chdir(body['cwd'])
    os.write(a.ready_fd,b'1');os.close(a.ready_fd)
    os.execvpe(body['argv'][0],body['argv'],body['env'])

if __name__=='__main__':main()
