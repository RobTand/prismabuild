"""Compare immutable parents and the integrated artifact without importing product code."""
import ast,collections,hashlib,json,pathlib,subprocess,sys
ROOT=pathlib.Path('/home/rob/paperclip-worktrees/prismabuild/v21-integrate-2219');OUT=pathlib.Path(__file__).resolve().parent
OURS='6a73af3edd365e8d10939bc8998f2dd527b4fa72';MAIN='8be4d4056fbcd6285a932d3ac4bf5d8bf6015036'
def git(*a):return subprocess.check_output(['git','-C',str(ROOT),*a])
def side(ref,p):return subprocess.check_output(['git','-C',str(ROOT),'show',ref+':'+p],stderr=subprocess.DEVNULL)
class Normalize(ast.NodeTransformer):
 def visit_Name(self,n):
  if n.id=='_denial':n.id='latest_claim_denial'
  return n
 def visit_Call(self,n):
  n=self.generic_visit(n)
  if isinstance(n.func,ast.Name) and n.func.id=='simulated_capacity':return n.args[0]
  for k in n.keywords:
   if k.arg is None and isinstance(k.value,ast.Call) and isinstance(k.value.func,ast.Name) and k.value.func.id=='simulated_admission':k.arg='capacity';k.value=k.value.args[1]
  n.keywords.sort(key=lambda k:k.arg or '')
  return n
 def visit_Assert(self,n):return self.generic_visit(n)
def assertions(raw):
 tree=Normalize().visit(ast.parse(raw));return collections.Counter(ast.dump(n,include_attributes=False) for n in ast.walk(tree) if isinstance(n,ast.Assert))
ledger='docs/staged_read_requirements_2026-09-20.json';merged=json.loads((ROOT/ledger).read_bytes());mx={r['id']:r for r in merged['requirements']};assert len(mx)==69
for parent in (OURS,MAIN):
 prior=json.loads(side(parent,ledger));assert set(mx)=={r['id'] for r in prior['requirements']}
 for row in prior['requirements']:
  for k,v in row.items():assert mx[row['id']][k]==v,(parent,row['id'],k)
files=[r['path'] for r in json.loads((OUT/'manual-resolutions.json').read_text()) if r['path'].startswith('tests/') and r['path'].endswith('.py')]
conflict_files=set(files)
files+= [r['path'] for r in json.loads((OUT/'migrated-calls.json').read_text())]
proof=[]
for path in sorted(set(files)):
 current=assertions((ROOT/path).read_bytes());counts={}
 for parent in ((OURS,MAIN) if path in conflict_files else ('ec5517a2aee42e7205a166fb0e77a5f449f44db9',)):
  try:old=assertions(side(parent,path))
  except subprocess.CalledProcessError:continue
  missing=old-current;assert not missing,(path,parent,list(missing)[:2]);counts[parent]=sum(old.values())
 proof.append({'path':path,'preserved_assertions':counts,'current_assertions':sum(current.values())})
history='tests/compat/history-controls/superseded-upgrade-intent-f1d6ce6052c13d54'
for parent,dest in ((MAIN,history),(OURS,history+'-pr2219-6a73af3edd36')):
 for path in git('ls-tree','-r','--name-only',parent,history).decode().splitlines():assert (ROOT/(dest+path[len(history):])).read_bytes()==side(parent,path),(parent,path)
worker=(ROOT/'tools/fleet/worker_loop.py').read_text()
for exact in ('observed_detail.update(getattr(args, "host_cap_detail", {}))','observed_detail.update(interpreter_observation.detail())'):
 assert worker.count(exact)==1,exact
result={'parents':[OURS,MAIN],'ledger_rows':69,'all_parent_fields_preserved':True,'complete_collision_archives_preserved':True,'worker_observations_both_present':True,'assertions':proof}
(OUT/'preservation.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k!='assertions'}))
