#!/usr/bin/env python3
"""Account for each original C13 function body and its ordered AST after owner moves."""
from __future__ import annotations
import ast
import copy
import json
from pathlib import Path
import subprocess
import sys
from c13_batch_equivalence import c13_canonical_tree, c13_dotted

BASE='d2f92f5ff533489182dddaf286feb54afde6f34a'


class C13Effects(ast.NodeTransformer):
    def visit_Call(self, node):
        # The existing core compact-ASCII profile includes one LF, removed before the old encode.
        if (isinstance(node.func,ast.Attribute) and node.func.attr=='decode'
                and isinstance(node.func.value,ast.Subscript)
                and isinstance(node.func.value.value,ast.Call)
                and c13_dotted(node.func.value.value.func)=='prismabuild.core._compact_ascii_lf_bytes'):
            return ast.Call(func=ast.Attribute(value=ast.Name(id='json'),attr='dumps'),
                args=[self.visit(node.func.value.value.args[0])],keywords=[
                ast.keyword(arg='sort_keys',value=ast.Constant(True)),
                ast.keyword(arg='separators',value=ast.Tuple(elts=[ast.Constant(','),ast.Constant(':')],ctx=ast.Load()))])
        name=c13_dotted(node.func)
        if name=='prismabuild.digest_primitives.sorted_json':
            return ast.Call(func=ast.Attribute(value=ast.Name(id='json'),attr='dumps'),
                            args=[self.visit(x) for x in node.args],
                            keywords=[ast.keyword(arg='sort_keys',value=ast.Constant(True)),*node.keywords])
        if name=='prismabuild.digest_primitives.raw_sha256':
            return ast.Call(func=ast.Attribute(value=ast.Call(
                func=ast.Attribute(value=ast.Name(id='hashlib'),attr='sha256'),
                args=[self.visit(x) for x in node.args],keywords=[]),attr='hexdigest'),args=[],keywords=[])
        return self.generic_visit(node)
    def visit_Attribute(self,node):
        name=c13_dotted(node)
        if name in ('prismabuild.pool.PoolContractError','prismabuild.record_store.PoolContractError'):
            return ast.Name(id='existing_PoolContractError')
        if name and name.startswith('prismabuild.pool.record_store.'):
            return ast.Name(id=name.replace('prismabuild.pool.record_store.','prismabuild.record_store.'))
        if name and name.startswith('prismabuild.record_store.'):
            return ast.Name(id=name)
        if name=='prismabuild.pool.LEASE_TIMEOUT_S':
            return ast.Name(id='lease_timeout_s')
        return self.generic_visit(node)


def c13_bodies(tree, manifest):
    tree=C13Effects().visit(c13_canonical_tree(tree,manifest))
    for node in ast.walk(tree):
        if isinstance(node,(ast.Name,ast.Attribute,ast.Tuple,ast.List,ast.Subscript)):
            node.ctx=ast.Load()
    def scan(nodes,prefix=''):
        out={}
        for n in nodes:
            if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)):
                out[prefix+n.name]=n
            elif isinstance(n,ast.ClassDef):out.update(scan(n.body,prefix+n.name+'.'))
        return out
    return scan(tree.body)


def c13_body(fn):
    body=copy.deepcopy(fn.body)
    if body and isinstance(body[0],ast.Expr) and isinstance(body[0].value,ast.Constant) and isinstance(body[0].value.value,str):body.pop(0)
    body=[x for x in body if not isinstance(x,(ast.Import,ast.ImportFrom))]
    # Bound receiver replacements are exact addressing substitutions, without I/O.
    body=[x for x in body if not (isinstance(x,ast.Assign) and len(x.targets)==1
          and isinstance(x.targets[0],ast.Name) and x.targets[0].id.removeprefix('local:') in ('path','entry')
          and isinstance(x.value,ast.Attribute) and isinstance(x.value.value,ast.Name)
          and x.value.value.id.removeprefix('local:')=='self' and x.value.attr in ('path','entry','_entry'))]
    body=[x for x in body if not (isinstance(x,ast.Assign) and any(
          isinstance(t,ast.Attribute) and t.attr=='lease_timeout_s' for t in x.targets))]
    return ast.dump(ast.Module(body=body,type_ignores=[]),include_attributes=False)


def main():
    m=json.loads(Path('tools/maintenance/c13_batch_migration.json').read_text())
    original=ast.parse(subprocess.check_output(['git','show',BASE+':src/prismabuild/produced_output.py']).decode())
    old=c13_bodies(original,m)
    files={name:c13_bodies(ast.parse(Path('src/prismabuild/'+name+'.py').read_text()),m)
           for name in ('produced_output','batch_ledger','output_contract')}
    rows=[]
    for name,fn in old.items():
        parent,_,tail=name.partition('.')
        target='produced_output';new=name
        if parent in m['leaf']:
            target='output_contract';new=m.get('leaf_names',{}).get(parent,parent)+(('.'+tail) if tail else '')
        elif parent in m['facets']:
            target='batch_ledger';new='BatchFacets.'+m['facets'][parent]
        elif parent in m['methods']:
            target='batch_ledger';new=('BatchCapture.' if m['methods'][parent] in ('retire_materialization','retire_first_copy','record_staged_paths','file_retirement','origin_gate') else 'BatchLedger.')+m['methods'][parent]
        elif parent in ('_read_commitments','_write_commitments'):
            target='batch_ledger';new='BatchLedger.'+{'_read_commitments':'capture','_write_commitments':'file'}[parent]
        elif parent in m['owner']:
            target='batch_ledger';new=m['renames'].get(parent,parent)+(('.'+tail) if tail else '')
        current=files[target].get(new)
        status='normalized-body-equal' if current and c13_body(fn)==c13_body(current) else 'explicit-seam-required'
        rows.append(dict(original=name,current=target+'.'+new,status=status,old_line=fn.lineno,new_line=current and current.lineno))
    def tail_at(body, name):
        for index,node in enumerate(body):
            if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='local:'+name for t in node.targets):
                return body[index:]
            if isinstance(node,ast.With):
                found=tail_at(node.body,name)
                if found:return found
        return None
    blocks={}
    original_stage=tail_at(old['commit_batch'].body,'batch_record')
    stage=files['batch_ledger']['BatchLedger.publish_stage']
    expected=ast.FunctionDef(name='stage',args=stage.args,body=original_stage,decorator_list=[])
    blocks['immutable_stage_publication']=c13_body(expected)==c13_body(stage)
    def before_assignment(nodes, name):
        prefix=[]
        for node in nodes:
            if (isinstance(node,ast.Assign) and any(
                    isinstance(target,ast.Name) and target.id=='local:'+name
                    for target in node.targets)):
                return prefix,True
            if isinstance(node,ast.With):
                body,found=before_assignment(node.body,name)
                if found:
                    node=copy.deepcopy(node)
                    node.body=body
                    return [*prefix,node],True
            prefix.append(node)
        return prefix,False
    original_prefix=copy.deepcopy(old['commit_batch'])
    current_prefix=copy.deepcopy(files['produced_output']['commit_batch'])
    original_prefix.body,old_boundary=before_assignment(original_prefix.body,'batch_record')
    current_prefix.body,new_boundary=before_assignment(current_prefix.body,'refused')
    blocks['funding_and_commit_prefix']=(old_boundary and new_boundary
                                       and c13_body(original_prefix)==c13_body(current_prefix))
    repins=next(n for n in ast.walk(old['_file_origin_repins']) if isinstance(n,ast.For) and isinstance(n.target,ast.Tuple) and isinstance(n.target.elts[0],ast.Name) and n.target.elts[0].id=='local:key')
    current=files['batch_ledger']['BatchLedger.repin_scope']
    expected=ast.FunctionDef(name='repins',args=current.args,body=repins.body,decorator_list=[])
    blocks['one_scope_origin_repins']=c13_body(expected)==c13_body(current)
    result=dict(base=BASE,functions=rows,blocks=blocks,equal=sum(x['status']=='normalized-body-equal' for x in rows))
    expected_seams={'_attempt_owned_paths','_live_path_owner','_TickReads.path_owners',
                    '_unknown_attempt_detail',
                    'canonical_expected_id','_commitments_path','commit_batch',
                    '_file_origin_repins','_retire_consumed_batch_locked','due_mover_rows'}
    actual_seams={row['original'] for row in rows if row['status']!='normalized-body-equal'}
    result['expected_seams']=sorted(expected_seams)
    result['qualified']=len(rows)==234 and actual_seams==expected_seams and all(blocks.values())
    Path(sys.argv[1]).write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'original':len(rows),'equal':result['equal'],'blocks':blocks,'seams':[r['original'] for r in rows if r['status']!='normalized-body-equal']}))
    raise SystemExit(0 if result['qualified'] else 1)

if __name__=='__main__':main()
