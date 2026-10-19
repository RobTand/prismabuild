"""Check the final C21 capture boundary against its fully retained source proof."""
import argparse,ast,copy,hashlib,json,runpy,subprocess
from pathlib import Path


def audit_final_boundary(checkout, head, prior_audit):
    original='4019622e8222b5c120ba4a710f66d2248d09cd50'
    reviewed='361dd2c2ba8baa2120713dc12fc33fd6ba9a7729'
    earlier=runpy.run_path(str(prior_audit))['audit'](checkout,original,reviewed)
    def git(*args):return subprocess.check_output(['git','-C',str(checkout),*args])
    def source(ref,path):return git('show',ref+':'+path).decode()
    def dump(node):return ast.dump(node,include_attributes=False)
    def function(tree,name):
        rows=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name]
        assert len(rows)==1,name
        return rows[0]
    def body(node):
        rows=copy.deepcopy(node.body)
        if isinstance(rows[0],ast.Expr) and isinstance(rows[0].value,ast.Constant) and isinstance(rows[0].value.value,str):rows.pop(0)
        return ast.Module(body=rows,type_ignores=[])
    changed=set(git('diff','--name-only',reviewed,head).decode().splitlines())
    expected={'src/prismabuild/residency_plan.py','tools/fleet/tier_loop.py','tools/fleet/manifest_promotion.py','tests/conftest.py','tests/test_planner_captures_the_floor_before_template_refusal.py','docs/design.md'}
    assert changed==expected,changed
    proofs=[]
    def equal(label,a,b):
        assert dump(a)==dump(b),label
        proofs.append(dict(claim=label,ast_sha256=hashlib.sha256(dump(a).encode()).hexdigest()))
    path='src/prismabuild/residency_plan.py';old=ast.parse(source(reviewed,path));new=ast.parse(source(head,path))
    cls=next(n for n in old.body if isinstance(n,ast.ClassDef) and n.name=='ConsumerWindow')
    method=function(cls,'pre_progress_stage_reach');cls.body.remove(method)
    geometry=function(new,'pre_progress_stage_reach');new.body.remove(geometry)
    assert ast.unparse(geometry.args)=='plan: Mapping[str, object], accepted_phase: object'
    assert ast.unparse(geometry.returns)=='set[str]' and not geometry.decorator_list
    class RestoreGeometry(ast.NodeTransformer):
        def __init__(self):self.counts={'plan':0,'accepted_phase':0}
        def visit_Name(self,node):
            if node.id in self.counts:
                self.counts[node.id]+=1
                return ast.copy_location(ast.Attribute(value=ast.Name(id='self',ctx=ast.Load()),attr=node.id,ctx=node.ctx),node)
            return node
    transform=RestoreGeometry();restored=transform.visit(body(geometry));assert transform.counts=={'plan':2,'accepted_phase':1}
    equal('complete pure geometry body retains exact captured inputs',body(method),restored)
    equal('all other residency module ASTs unchanged',old,new)
    path='tools/fleet/tier_loop.py';s=source(head,path)
    call='residency_plan.pre_progress_stage_reach(plan, accepted_phase)';assert s.count(call)==1
    s=s.replace(call,'residency_plan.ConsumerWindow(plan, accepted_phase, residency_plan.LegObservation()).pre_progress_stage_reach')
    doc=':func:`residency_plan.pre_progress_stage_reach`';assert s.count(doc)==1
    s=s.replace(doc,':attr:`residency_plan.ConsumerWindow.pre_progress_stage_reach`')
    equal('complete caller module and short circuit unchanged after exact binding reverse',ast.parse(source(reviewed,path)),ast.parse(s))
    path='tools/fleet/manifest_promotion.py';s=source(head,path)
    capture='            floor_request = SubmissionRequest()\n';assert s.count(capture)==1
    assert s.index('priority = -10 if raw_priority is None else int(raw_priority)') < s.index(capture) < s.index('template = movement_template_of(queue, request, key)')
    assert s.count('floor_request')==2 and s.count('with submission.write_window(floor_request, store=store):')==1
    s=s.replace(capture,'').replace('with submission.write_window(floor_request, store=store):','with submission.write_window(SubmissionRequest(), store=store):')
    equal('complete planner unchanged beyond original floor capture point',ast.parse(source(reviewed,path)),ast.parse(s))
    path='tests/conftest.py';old=ast.parse(source(reviewed,path));new=ast.parse(source(head,path));hook=function(new,'pytest_terminal_summary');new.body.remove(hook)
    equal('every original conftest hook fixture guard import and assertion unchanged',old,new)
    assert ast.unparse(hook.args)=='terminalreporter, exitstatus, config' and not hook.decorator_list
    tests='tests/test_planner_captures_the_floor_before_template_refusal.py'
    assert source(head,tests)==source('815243474599a70ca83617e25f00a9ae80b7786b',tests)==source('7af3e85c1200d078da36e2630d25e57229dde119',tests)
    assert git('show',head+':docs/design.md').startswith(git('show',reviewed+':docs/design.md'))
    paths=git('ls-tree','-rz','--name-only',head).decode().split('\0');parsed=0
    for path in paths:
        if path.endswith('.py'):ast.parse(source(head,path),filename=path);parsed+=1
    assert git('show',head+':src/prismabuild/client.py')==git('show',reviewed+':src/prismabuild/client.py')
    return dict(status='PASS',axis='source only',original_base=original,reviewed_head=reviewed,final_head=head,
        earlier_proof=earlier,exact_changed_paths=sorted(changed),final_complete_ast_proofs=proofs,
        runtime_hook_source_sha256=hashlib.sha256(ast.get_source_segment(source(head,'tests/conftest.py'),hook).encode()).hexdigest(),
        all_python_parsed=parsed,new_control_identical_in_old_changed_and_repaired_sources=True,
        prior_runtime_credit_not_retagged=True,limit='Fresh final-source native/runtime/CAS/scope proof remains separate.')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--checkout',type=Path,required=True);parser.add_argument('--head',required=True);parser.add_argument('--prior-audit',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    try:report=audit_final_boundary(args.checkout,args.head,args.prior_audit)
    except Exception as exc:report=dict(status='FAIL',error=str(exc))
    args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'status':report['status'],'error':report.get('error')}));raise SystemExit(report['status']!='PASS')
