"""Source-only, occurrence-preserving checks for the exact-dac C12 migration."""
import argparse,ast,hashlib,json,subprocess
from collections import Counter
from pathlib import Path

ap=argparse.ArgumentParser();ap.add_argument('root',type=Path);ap.add_argument('out',type=Path);ap.add_argument('--base',default='dac2006656264b4bd55e4e3837331205a9740fd3');ap.add_argument('--head',default='HEAD');a=ap.parse_args()
def c12_git(*args):return subprocess.check_output(['git','-C',str(a.root),*args])
def c12_blob(rev,path):return c12_git('show',rev+':'+path)
def c12_files(rev):return c12_git('ls-tree','-r','--name-only',rev).decode().splitlines()
oldfiles=c12_files(a.base);newfiles=c12_files(a.head)
more=json.loads((Path(__file__).parent/'c12_funding_migration.json').read_text())
moves=more['methods']
rev={v:k for k,v in moves.items()};rev['_transfer_window']='transfer_fence'
pre={v:k for k,v in more['prelaunch'].items()};tier={v:k for k,v in more['tier'].items()}
fields={'gated','protected','grants','permitted','prelaunch_open','unknown_ready','unknown_tiers','unknown_consumers','events','census','census_consumers','census_unknown'}
class C12Normalize(ast.NodeTransformer):
 def visit_Attribute(self,n):
  n=self.generic_visit(n)
  if n.attr in ('credit','bound','released','result') and isinstance(n.value,ast.Call):return n.value
  if n.attr=='funding':return n.value
  if isinstance(n.value,ast.Name) and n.value.id=='funding' and n.attr=='FundingLedger':return ast.Attribute(ast.Name('pool',ast.Load()),'PoolQueue',ast.Load())
  if n.attr in rev:n.attr=rev[n.attr]
  if isinstance(n.value,ast.Name) and n.value.id=='funding' and n.attr.startswith('TIER_FUNDING'):n.value.id='pool'
  if n.attr in fields:return ast.Call(ast.Name('_fact',ast.Load()),[n.value,ast.Constant(n.attr)],[])
  return n
 def visit_Name(self,n):
  if n.id in ['pool_mod','_pool_mod']:n.id='pool'
  if n.id in ['pg','group']:n.id='prelaunch_group'
  if n.id in ['tl']:n.id='tier_loop'
  return n
 def visit_Constant(self,n):
  if isinstance(n.value,str) and n.value in rev:n.value=rev[n.value]
  return n
 def visit_Subscript(self,n):
  n=self.generic_visit(n)
  if isinstance(n.slice,ast.Constant) and n.slice.value in fields:return ast.Call(ast.Name('_fact',ast.Load()),[n.value,n.slice],[])
  return n
 def visit_Call(self,n):
  # Recover the former call shape before stripping the owned-instance field.
  if isinstance(n.func,ast.Attribute) and isinstance(n.func.value,ast.Attribute) and n.func.value.attr=='funding':
   name=n.func.attr; q=n.func.value.value
   if name in pre or name in tier:
    owner,former=('prelaunch_group',pre[name]) if name in pre else ('tier_loop',tier[name])
    n.func=ast.Attribute(ast.Name(owner,ast.Load()),former,ast.Load());n.args.insert(0,q)
  if isinstance(n.func,ast.Attribute) and isinstance(n.func.value,ast.Call) and isinstance(n.func.value.func,ast.Name) and n.func.value.func.id=='LegacyFunding':
   name={'read':'_read_funding','file':'_write_funding','delete':'_delete_funding'}.get(n.func.attr)
   if name:n.args=n.func.value.args+n.args;n.func=ast.Attribute(ast.Name('bl',ast.Load()),name,ast.Load())
  if isinstance(n.func,ast.Attribute) and isinstance(n.func.value,ast.Name) and n.func.value.id=='LegacyFunding' and n.func.attr=='directory':
   call=n.args[0]
   if isinstance(call,ast.Call) and isinstance(call.func,ast.Attribute) and call.func.attr=='scope_dir':n.func=ast.Attribute(ast.Name('bl',ast.Load()),'_funding_dir',ast.Load());n.args=call.args
  n=self.generic_visit(n)
  if isinstance(n.func,ast.Attribute) and n.func.attr=='get' and n.args and isinstance(n.args[0],ast.Constant) and n.args[0].value in fields:return ast.Call(ast.Name('_fact',ast.Load()),[n.func.value,n.args[0]],[])
  return n

def c12_norm(n):return ast.dump(C12Normalize().visit(ast.fix_missing_locations(ast.parse(ast.unparse(n)))),include_attributes=False)
def c12_definitions(source):
 tree=ast.parse(source);result={}
 def walk(body,prefix=''):
  counts=Counter()
  for n in body:
   if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)):
    counts[n.name]+=1;name=prefix+n.name
    if not isinstance(n,ast.ClassDef):
     checks=[]
     for x in ast.walk(n):
      if isinstance(x,ast.Assert) or isinstance(x,ast.Call) and ((isinstance(x.func,ast.Attribute) and x.func.attr in ['raises','warns']) or (isinstance(x.func,ast.Name) and x.func.id.startswith('_assert'))):checks.append(c12_norm(x))
     result[name+'#'+str(counts[n.name])]=dict(signature=ast.dump(n.args,include_attributes=False),decorators=[c12_norm(d) for d in n.decorator_list],checks=checks,line=n.lineno)
    walk(n.body,name+'.')
 walk(tree.body);return result
mismatches=[];counts=Counter();test_files=[]
for p in oldfiles:
 if not (p.startswith('tests/') and p.endswith('.py')):continue
 old=c12_definitions(c12_blob(a.base,p));new=c12_definitions(c12_blob(a.head,p));test_files.append(p)
 if set(old)-set(new):mismatches.append(dict(path=p,missing_definitions=sorted(set(old)-set(new))))
 for name,v in old.items():
  counts['definitions']+=1;counts['checks']+=len(v['checks'])
  if name not in new:continue
  for key in ['signature','decorators','checks']:
   if v[key]!=new[name][key]:mismatches.append(dict(path=p,name=name,part=key,old=v[key],new=new[name][key]))
immutable=[p for p in oldfiles if p.startswith('tests/compat/') and not p.endswith('.py')]
changed_immutable=[p for p in immutable if c12_blob(a.base,p)!=c12_blob(a.head,p)]
client_exact=c12_blob(a.base,'src/prismabuild/client.py')==c12_blob(a.head,'src/prismabuild/client.py')
def c12_resource(rev):return ast.dump(next(n for n in ast.parse(c12_blob(rev,'src/prismabuild/pool.py')).body if isinstance(n,ast.ClassDef) and n.name=='ResourceLedger'),include_attributes=False)
resource_exact=c12_resource(a.base)==c12_resource(a.head)
owner=ast.parse(c12_blob(a.head,'src/prismabuild/funding.py'));imports=[n for n in ast.walk(owner) if isinstance(n,(ast.Import,ast.ImportFrom))]
no_reverse_import=all('pool' not in ast.unparse(n).split() and 'produced_output' not in ast.unparse(n) for n in imports)
pq=next(n for n in ast.parse(c12_blob(a.head,'src/prismabuild/pool.py')).body if isinstance(n,ast.ClassDef) and n.name=='PoolQueue')
retired_gone=not ({n.name for n in pq.body if isinstance(n,ast.FunctionDef)} & set(moves))
result=dict(base=a.base,head=c12_git('rev-parse',a.head).decode().strip(),original_test_files=len(test_files),**counts,immutable_artifacts=len(immutable),changed_immutable=changed_immutable,client_bytes_exact=client_exact,resource_ledger_ast_exact=resource_exact,no_reverse_import=no_reverse_import,retired_pool_methods_gone=retired_gone,mismatches=mismatches)
a.out.mkdir(parents=True,exist_ok=True);(a.out/'source-preservation.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k not in ['mismatches']}|dict(mismatch_count=len(mismatches))))
raise SystemExit(bool(mismatches or changed_immutable or not(client_exact and resource_exact and no_reverse_import and retired_gone)))
