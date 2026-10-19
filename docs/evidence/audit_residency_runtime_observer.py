"""Verify that the final observer refinement preserves the tested C21 domain."""
import argparse,ast,hashlib,json,runpy,subprocess
from pathlib import Path


def audit_c21_observer_source(checkout,head,boundary_audit,prior_audit):
    base='1c43fd4de593bc6154459e5e2e05aff3ac5ba434'
    earlier=runpy.run_path(str(boundary_audit))['audit_final_boundary'](checkout,base,prior_audit)
    def git(*args):return subprocess.check_output(['git','-C',str(checkout),*args])
    def source(ref,path):return git('show',ref+':'+path)
    def dump(node):return ast.dump(node,include_attributes=False)
    changed=set(git('diff','--name-only',base,head).decode().splitlines())
    assert changed=={'tests/conftest.py','tests/test_pytest_runtime_evidence.py','tools/fleet/tier_loop.py'},changed
    old=ast.parse(source(base,'tests/conftest.py'));new=ast.parse(source(head,'tests/conftest.py'))
    def hook(tree):return next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='pytest_terminal_summary')
    a,b=hook(old),hook(new);old.body.remove(a);new.body.remove(b)
    prefix=next(n for n in new.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id=='TEST_RUNTIME_EVIDENCE_PREFIX')
    assert ast.literal_eval(prefix.value)=='prismabuild-test-runtime: ';new.body.remove(prefix)
    assert dump(old)==dump(new),'existing conftest code changed'
    def assigns(nodes,name):return next(n for n in nodes if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and n.targets[0].id==name)
    attempt=next(n for n in b.body if isinstance(n,ast.Try))
    assert len(attempt.handlers)==1 and ast.unparse(attempt.handlers[0].type)=='(OSError, RuntimeError, AttributeError)'
    for name in ('executable','running'):assert dump(assigns(a.body,name))==dump(assigns(attempt.body,name))
    original={k.arg:k.value for k in assigns(a.body,'record').value.keywords}
    initial={k.arg:k.value for k in assigns(b.body,'record').value.keywords}
    update=next(n.value for n in attempt.body if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and ast.unparse(n.value.func)=='record.update')
    available={**initial,**{k.arg:k.value for k in update.keywords}}
    assert set(available)==set(original)|{'complete'}
    assert all(dump(available[k])==dump(v) for k,v in original.items()),'positive observed facts changed'
    assert ast.literal_eval(initial['complete']) is False and ast.literal_eval(available['complete']) is True
    assert not any(isinstance(n,(ast.Raise,ast.Assert,ast.Return)) for n in ast.walk(b))
    assert source(head,'tests/test_pytest_runtime_evidence.py')==source('878338d0abf5f383668c5ddb1e7759c7a414d1a0','tests/test_pytest_runtime_evidence.py')
    stale=b'#: The suffix a supersession marker\'s address ends with: the marker is\n#: ``{consumer_action_key}.{plan_sha256}.superseded.json`` beside the filed\n#: plan (``residency_plan._superseded_path``).\n'
    before=source(base,'tools/fleet/tier_loop.py');after=source(head,'tools/fleet/tier_loop.py')
    assert before.count(stale)==1 and before.replace(stale,b'')==after
    assert dump(ast.parse(before))==dump(ast.parse(after))
    assert not git('diff','--name-only',base,head,'--','src','tools/maintenance','docs')
    return dict(status='PASS',axis='source only',tested_domain_head=base,observer_head=head,earlier_boundary_proof=earlier,
        exact_changed_paths=sorted(changed),all_production_ASTs_unchanged=True,all_original_conftest_code_unchanged=True,
        all_available_runtime_expressions_unchanged=True,old_source_fault_control_byte_exact=True,
        observer_complete_false_is_not_qualification=True,new_hook_AST_sha256=hashlib.sha256(dump(b).encode()).hexdigest())


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--checkout',type=Path,required=True);p.add_argument('--head',required=True);p.add_argument('--boundary-audit',type=Path,required=True);p.add_argument('--prior-audit',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    try:r=audit_c21_observer_source(a.checkout,a.head,a.boundary_audit,a.prior_audit)
    except Exception as e:r=dict(status='FAIL',error=str(e))
    a.output.write_text(json.dumps(r,indent=2)+'\n');print(json.dumps({'status':r['status'],'error':r.get('error')}));raise SystemExit(r['status']!='PASS')
