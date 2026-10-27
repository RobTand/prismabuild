"""Freeze one immutable pbtest selection and start its persistent controller."""
import datetime,json,os,pathlib,subprocess,sys
OUT=pathlib.Path(__file__).resolve().parent;ROOT=pathlib.Path('/home/rob/paperclip-worktrees/prismabuild/v21-integrate-2219')
name=sys.argv[1];files=sys.argv[2:];head=subprocess.check_output(['git','-C',str(ROOT),'rev-parse','HEAD'],text=True).strip()
assert not subprocess.check_output(['git','-C',str(ROOT),'status','--porcelain'])
client='/mnt/shared/prismabuild-fleet/repo/tools/fleet/pbtest.py'; report=OUT/(name+'.json');unit='pb-PRI-242-'+name+'-'+head[:10]
command=['python3',client,'--checkout',str(ROOT),'--python','/home/rob/venvs/pb-cpu/bin/python','--workers-per-shard','1','--threads-per-shard','1','--cpus-per-shard','2','--mem-gb','4','--priority','-10','--timeout-s','900','--wait-s','21600','--json',str(report),*files]
if name.startswith('compatibility'):command[-len(files):-len(files)]=['--pytest-args',json.dumps(['-k','upgrade-intent'])]
plan=OUT/(name+'.plan.json');subprocess.run(['python3',client,'--checkout',str(ROOT),'--plan-only','--json',str(plan),*files],check=True,stdout=subprocess.DEVNULL)
discovery=json.loads(plan.read_text());assert not discovery['left_out'] and not discovery['refusals'],discovery
now=datetime.datetime.now(datetime.timezone.utc)
record={'name':name,'source':head,'command':command,'unit':unit,'report':str(report),'files':files,'discovery':str(plan),'per_action':{'cpu':2,'mem_gb':4,'gpu':0,'native_threads':1},'aggregate_max':{'cpu':16,'mem_gb':32,'gpu':0},'max_clients':8,'payload_limit_s':900,'observation_limit_s':21600,'created_at':now.isoformat()}
path=OUT/(name+'.launch.json');assert not path.exists();path.write_text(json.dumps(record,indent=2)+'\n')
phase=OUT/'phase.json'
if not phase.exists():phase.write_text(json.dumps({'owner':'PRI-242','first_launch':now.isoformat(),'next_check':(now+datetime.timedelta(minutes=30)).isoformat(),'hard_limit':(now+datetime.timedelta(hours=6)).isoformat(),'checks':0,'max_checks':8,'timeout_action':'wake_owner','inherited_root_bounds':'unchanged: PRI-128 13:45:41 UTC/max40 cumulative39'},indent=2)+'\n')
env={**os.environ,'XDG_RUNTIME_DIR':'/run/user/1000','DBUS_SESSION_BUS_ADDRESS':'unix:path=/run/user/1000/bus'}
subprocess.run(['systemd-run','--user','--collect','--unit',unit,*command],env=env,check=True)
print(json.dumps(record))
