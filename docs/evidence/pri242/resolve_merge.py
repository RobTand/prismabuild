"""Replay the exact-parent PRI-242 integration without editing automatic merges."""
from pathlib import Path
import json,re,subprocess,shutil
ROOT=Path('/home/rob/paperclip-worktrees/prismabuild/v21-integrate-2219')
OUT=Path('/mnt/shared/paperclip-runs/PRI-242')
OURS='6a73af3edd365e8d10939bc8998f2dd527b4fa72'
MAIN='8be4d4056fbcd6285a932d3ac4bf5d8bf6015036'
def git(*a):return subprocess.check_output(['git','-C',str(ROOT),*a])
def side(ref,path):return git('show',ref+':'+path)
assert git('rev-parse','HEAD').decode().strip()==OURS
assert git('rev-parse','MERGE_HEAD').decode().strip()==MAIN
conflicts=git('diff','--name-only','--diff-filter=U').decode().splitlines()
resolutions=[]
for f in conflicts:
 if not f.endswith('.py'):continue
 raw=(ROOT/f).read_text()
 def resolve(m):
  a,b=m[1],m[2]
  if f=='tools/fleet/worker_loop.py' or 'from admitted_queue_fixture import' in a:return a+b
  # Preserve the capped claim call and use the incoming shared denial reader.
  return a.replace('_denial(', 'latest_claim_denial(')
 raw,count=re.subn(r'^<<<<<<< HEAD\n(.*?)^=======\n(.*?)^>>>>>>> '+MAIN+r'\n',resolve,raw,flags=re.M|re.S)
 assert count and '<<<<<<<' not in raw
 (ROOT/f).write_text(raw)
 resolutions.append(dict(path=f,blocks=count,resolution='Combine cap admission with shared denial reader; preserve all automatic main changes' if f.startswith('tests/') else 'Retain both cap detail and interpreter observation detail'))
f='docs/staged_read_requirements_2026-09-20.json';a,b=[json.loads(side(r,f)) for r in (OURS,MAIN)]
ix={r['id']:r for r in b['requirements']}
for row in a['requirements']:
 for k,v in row.items():
  if k in ix[row['id']]:assert ix[row['id']][k]==v,(row['id'],k)
  else:assert k=='scoped_repair_2141';ix[row['id']][k]=v
(ROOT/f).write_text(json.dumps(b,indent=2)+'\n')
resolutions.append(dict(path=f,resolution='Keep every main field plus five exact inherited scoped_repair_2141 fields; no existing axis changes'))
history='tests/compat/history-controls/superseded-upgrade-intent-f1d6ce6052c13d54'
# Retain the complete source-parent archive under its own immutable identity.
archive=history+'-pr2219-6a73af3edd36'
for name in git('ls-tree','-r','--name-only',OURS,history).decode().splitlines():
 dest=ROOT/(archive+name[len(history):]);dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(side(OURS,name))
(ROOT/(history+'/provenance.json')).write_bytes(side(MAIN,history+'/provenance.json'))
resolutions.append(dict(path=history+'/provenance.json',resolution='Main archive remains at existing identity; complete source-parent archive retained byte-exact at '+archive))
for name in ('capture-rollout.json','goldens-rollout.jsonl','field_sets.json'):
 path='tests/compat/'+name;(ROOT/path).write_bytes(side(MAIN,path))
 resolutions.append(dict(path=path,resolution='Seed main baseline; regenerate only through authoritative scanner or admitted strict capture/adoption'))
(OUT/'manual-resolutions.json').write_text(json.dumps(resolutions,indent=2)+'\n')
print(json.dumps(dict(conflicts=len(conflicts),resolutions=len(resolutions))))
