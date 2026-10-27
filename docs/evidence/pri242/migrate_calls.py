"""Bind new-main positive claim fixtures to the existing capped admission helper."""
import ast,json,pathlib,subprocess
ROOT=pathlib.Path('/home/rob/paperclip-worktrees/prismabuild/v21-integrate-2219');OUT=pathlib.Path(__file__).resolve().parent
names=[row[0] for row in json.loads((OUT/'raw-capacity-calls.json').read_text())]
report=[]
for name in names:
 p=ROOT/name;s=p.read_text();t=ast.parse(s);lines=s.splitlines(keepends=True);offset=[0]
 for line in lines:offset.append(offset[-1]+len(line))
 def pos(n,end=False):return offset[(n.end_lineno if end else n.lineno)-1]+(n.end_col_offset if end else n.col_offset)
 edits=[];restarts={};calls=[]
 for n in ast.walk(t):
  if not isinstance(n,ast.Call) or not isinstance(n.func,ast.Attribute):continue
  if n.func.attr=='claim':receiver=n.func.value
  elif n.func.attr=='submit' and n.args and isinstance(n.args[0],ast.Attribute) and n.args[0].attr=='claim':receiver=n.args[0].value
  else:continue
  capacities=[k for k in n.keywords if k.arg=='capacity']
  if not capacities:continue
  k=capacities[0]
  # Leave claims through the already migrated family-owned adapter alone.
  if name=='tests/test_gpu_runtime_integration.py' and ast.unparse(receiver)=='queue':continue
  bound=ast.get_source_segment(s,receiver)
  if isinstance(receiver,ast.Call):
   assert ast.unparse(receiver.func)=='pool.PoolQueue'
   bound='restarted_queue';line=n.lineno
   # The receiver may begin on the assignment line before the call's first argument.
   restarts[line]=' '*(len(lines[line-1])-len(lines[line-1].lstrip()))+bound+' = '+ast.get_source_segment(s,receiver)+'\n'
   edits.append((pos(receiver),pos(receiver,True),bound))
  value=ast.get_source_segment(s,k.value)
  edits.append((pos(k),pos(k,True),'**simulated_admission('+bound+', simulated_capacity('+value+'))'))
  calls.append(n.lineno)
 for line,text in restarts.items():edits.append((offset[line-1],offset[line-1],text))
 for a,b,text in sorted(edits,reverse=True):s=s[:a]+text+s[b:]
 if calls:
  required={'simulated_admission','simulated_capacity'}
  imported={a.asname or a.name for n in t.body if isinstance(n,ast.ImportFrom) and n.module=='admitted_queue_fixture' for a in n.names}
  if not required <= imported:
   updated=ast.parse(s);future=[n for n in updated.body if isinstance(n,ast.ImportFrom) and n.module=='__future__'];at=future[-1].end_lineno if future else (updated.body[0].end_lineno if isinstance(updated.body[0],ast.Expr) and isinstance(updated.body[0].value,ast.Constant) else 0)
   updated_lines=s.splitlines(keepends=True);updated_lines.insert(at,'\nfrom admitted_queue_fixture import '+', '.join(sorted(required-imported))+'\n');s=''.join(updated_lines)
  ast.parse(s);p.write_text(s);report.append({'path':name,'calls':calls,'restart_lines':list(restarts)})
(OUT/'migrated-calls.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
