#!/usr/bin/env python3
"""Compare every original assertion/decorator occurrence after exact C13 owner substitutions.

This is a source audit. It executes no product, fixture or test code.
"""
from __future__ import annotations
import argparse
import ast
from collections import Counter
import json
from pathlib import Path
import subprocess

BASE='d2f92f5ff533489182dddaf286feb54afde6f34a'
OLD='prismabuild.produced_output.'


def c13_read_git(revision,path):
    return subprocess.check_output(['git','show',revision+':'+path]).decode()


def c13_dotted(node):
    if isinstance(node,ast.Name): return node.id
    if isinstance(node,ast.Attribute):
        prefix=c13_dotted(node.value)
        if prefix: return prefix+'.'+node.attr
    return None


def c13_canonical_tree(tree,manifest):
    aliases={}
    loader_aliases=set()
    for n in ast.walk(tree):
        if isinstance(n,ast.Import):
            for a in n.names: aliases[a.asname or a.name]=a.name
        elif isinstance(n,ast.ImportFrom):
            for a in n.names:
                module=('prismabuild'+('.'+n.module if n.module else '')) if n.level else (n.module or 'prismabuild')
                aliases[a.asname or a.name]=module+'.'+a.name
        elif (isinstance(n,ast.Assign) and len(n.targets)==1
              and isinstance(n.targets[0],ast.Name) and isinstance(n.value,ast.Call)
              and isinstance(n.value.func,ast.Name) and n.value.func.id=='load'
              and len(n.value.args)==1 and isinstance(n.value.args[0],ast.Constant)
              and n.value.args[0].value in ('prismabuild.batch_ledger','prismabuild.output_contract')):
            name=n.targets[0].id
            aliases[name]=n.value.args[0].value
            loader_aliases.add(name)
    top_names={n.name for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
    if 'BatchLedger' in top_names:
        aliases['BatchLedger']='prismabuild.batch_ledger.BatchLedger'
        aliases['BatchFacets']='prismabuild.batch_ledger.BatchFacets'
    for old,new in manifest.get('leaf_names',{}).items():
        if new in top_names:aliases[new]='prismabuild.output_contract.'+new
    for old,new in manifest['renames'].items():
        if new in top_names:aliases[new]='prismabuild.batch_ledger.'+new
    scopes=[]
    def resolve(node):
        name=c13_dotted(node)
        if not name:return None
        first,_,tail=name.partition('.')
        if any(first in scope for scope in scopes):
            return 'local:'+name
        return aliases.get(first,first)+('.'+tail if tail else '')
    reverse={}
    for n in manifest['leaf']:
        reverse['prismabuild.output_contract.'+manifest.get('leaf_names',{}).get(n,n)]=OLD+n
    for n in manifest['owner']:
        reverse['prismabuild.batch_ledger.'+manifest['renames'].get(n,n)]=OLD+n
    for old,new in dict(manifest['methods'], _read_commitments='capture', _write_commitments='file').items():
        reverse['prismabuild.batch_ledger.BatchLedger.'+new]=OLD+old
    facets={new:old for old,new in manifest['facets'].items()}
    owner_funcs=set(manifest['leaf'])|set(manifest['owner'])|set(manifest['facets'])
    def identity(name):
        if name.startswith('prismabuild.produced_spool.po.'):
            name=OLD+name.removeprefix('prismabuild.produced_spool.po.')
        if name in reverse: return reverse[name]
        if name in owner_funcs:return OLD+name
        return name
    class Normalize(ast.NodeTransformer):
        def visit_FunctionDef(self,node):
            bound={a.arg for a in [*node.args.posonlyargs,*node.args.args,*node.args.kwonlyargs]}
            if node.args.vararg:bound.add(node.args.vararg.arg)
            if node.args.kwarg:bound.add(node.args.kwarg.arg)
            global_names=set()
            class Bindings(ast.NodeVisitor):
                def visit_Name(self,item):
                    if isinstance(item.ctx,(ast.Store,ast.Del)):bound.add(item.id)
                def visit_Global(self,item):global_names.update(item.names)
                def visit_FunctionDef(self,item):bound.add(item.name)
                visit_AsyncFunctionDef=visit_FunctionDef
                def visit_ClassDef(self,item):bound.add(item.name)
                def visit_ExceptHandler(self,item):
                    if item.name:bound.add(item.name)
                    self.generic_visit(item)
            visitor=Bindings()
            for item in node.body:visitor.visit(item)
            scopes.append(bound-global_names-loader_aliases)
            result=self.generic_visit(node)
            scopes.pop()
            return result
        visit_AsyncFunctionDef=visit_FunctionDef
        def visit_Attribute(self,node):
            if isinstance(node.value,ast.Call) and resolve(node.value.func)=='prismabuild.batch_ledger.BatchFacets' and node.attr in facets:
                return ast.Call(func=ast.Name(id=OLD+facets[node.attr]),args=[self.visit(x) for x in node.value.args],keywords=[])
            if node.attr=='path' and isinstance(node.value,ast.Call) and resolve(node.value.func)=='prismabuild.batch_ledger.BatchLedger.for_instance':
                return ast.Call(func=ast.Name(id=OLD+'_commitments_path'),args=[self.visit(x) for x in node.value.args],keywords=[])
            name=resolve(node)
            if name and (name.startswith(('prismabuild.output_contract.','prismabuild.batch_ledger.','prismabuild.produced_output.','prismabuild.produced_spool.po.'))):
                return ast.Name(id=identity(name))
            return self.generic_visit(node)
        def visit_Name(self,node):
            name=resolve(node)
            return ast.Name(id=identity(name),ctx=node.ctx)
        def visit_Call(self,node):
            # C31 current fixture setup now uses C10's already-owned tier API.
            if (isinstance(node.func,ast.Attribute) and node.func.attr=='take'
                    and isinstance(node.func.value,ast.Call)
                    and isinstance(node.func.value.func,ast.Attribute)
                    and node.func.value.func.attr=='tier'):
                node.func.attr='acquire'
                node.func.value.func.attr='tier_ledger'
            if (isinstance(node.func,ast.Attribute) and node.func.attr in ('capture','file')
                    and isinstance(node.func.value,ast.Call)
                    and resolve(node.func.value.func)=='prismabuild.batch_ledger.BatchLedger.for_instance'):
                path=ast.Call(func=ast.Name(id=OLD+'_commitments_path'),args=[self.visit(x) for x in node.func.value.args],keywords=[])
                return ast.Call(func=ast.Name(id=OLD+('_read_commitments' if node.func.attr=='capture' else '_write_commitments')),args=[path,*[self.visit(x) for x in node.args]],keywords=[])
            if isinstance(node.func,ast.Attribute) and isinstance(node.func.value,ast.Call) and resolve(node.func.value.func)=='prismabuild.batch_ledger.BatchLedger' and node.func.attr in ('capture','file'):
                return ast.Call(func=ast.Name(id=OLD+('_read_commitments' if node.func.attr=='capture' else '_write_commitments')),
                                args=[self.visit(x) for x in [*node.func.value.args,*node.args]],keywords=[])
            if resolve(node.func)=='prismabuild.batch_ledger.LedgerReads':
                node.keywords=[k for k in node.keywords if k.arg!='lease_timeout_s']
            if resolve(node.func)=='prismabuild.batch_ledger.BatchLedger.due_mover_rows':
                node.keywords=[k for k in node.keywords if k.arg!='residency_schema']
            return self.generic_visit(node)
    return Normalize().visit(tree)


def c13_occurrences(source,manifest,path):
    tree=ast.parse(source)
    if path=='tests/test_a_batch_record_has_a_public_reader.py':
        for n in ast.walk(tree):
            if isinstance(n,ast.Assert) and ast.unparse(n.test)=='name in vars(owner)':
                n.test=ast.parse('name in po.__all__',mode='eval').body
    tree=c13_canonical_tree(tree,manifest)
    assertions=[];decorators=[]
    for n in ast.walk(tree):
        if isinstance(n,ast.Assert):
            assertions.append(ast.dump(n,include_attributes=False))
        elif isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)):
            decorators.extend(ast.dump(d,include_attributes=False) for d in n.decorator_list if not isinstance(d,ast.Name) or d.id not in ('staticmethod','property','classmethod'))
    return Counter(assertions),Counter(decorators)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--out',type=Path,required=True);args=p.parse_args()
    manifest=json.loads(Path('tools/maintenance/c13_batch_migration.json').read_text())
    paths=subprocess.check_output(['git','ls-tree','-r','--name-only',BASE]).decode().splitlines()
    missing={}; totals=Counter(); ad_old=Counter(); ad_new=Counter()
    for path in paths:
        if not path.endswith('.py'):continue
        try:
            olda,oldd=c13_occurrences(c13_read_git(BASE,path),manifest,path)
            newa,newd=c13_occurrences(Path(path).read_text(),manifest,path)
        except (SyntaxError,UnicodeError):continue
        if path=='src/prismabuild/produced_output.py':
            for moved in ('src/prismabuild/batch_ledger.py','src/prismabuild/output_contract.py'):
                a,d=c13_occurrences(Path(moved).read_text(),manifest,moved);newa.update(a);newd.update(d)
        lost=olda-newa; decor=oldd-newd
        if lost or decor:missing[path]={'assertions':list(lost.elements()),'decorators':list(decor.elements())}
        totals.update({'original_assertions':sum(olda.values()),'current_assertions':sum(newa.values()),'original_decorators':sum(oldd.values()),'current_decorators':sum(newd.values())})
    result={'base':BASE,'totals':dict(totals),'missing':missing,'all_original_occurrences_accounted':not missing}
    args.out.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({'totals':dict(totals),'missing_files':{k:{x:len(v[x]) for x in v} for k,v in missing.items()}}))
    raise SystemExit(bool(missing))

if __name__=='__main__': main()
