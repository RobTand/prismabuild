#!/usr/bin/env python3
"""Export C13's importer graph and bounded whole-file native consumer selection.

This source audit imports no product and performs no test collection. PB owns
physical collection and execution. The full Queue importer graph is exported
for Root; it is not another whole-root test gate.
"""
from __future__ import annotations
import argparse
import ast
from collections import defaultdict
import json
from pathlib import Path
import subprocess

BASE='d2f92f5ff533489182dddaf286feb54afde6f34a'


def c13_population(root):
    files=subprocess.check_output(['git','ls-files','*.py'],cwd=root,text=True).splitlines()
    aliases=defaultdict(set)
    for file in files:
        p=Path(file);stem=p.stem
        aliases[stem].add(file);aliases[p.name].add(file);aliases[file].add(file)
        if file.startswith('src/'):
            aliases[file[4:-3].replace('/','.')].add(file)
    graph={}
    for file in files:
        try:tree=ast.parse((root/file).read_text())
        except (SyntaxError,UnicodeError):continue
        imports=set()
        for node in ast.walk(tree):
            if isinstance(node,ast.Import):
                for a in node.names:imports.update(aliases.get(a.name,()))
            elif isinstance(node,ast.ImportFrom):
                module=node.module or ''
                if node.level and file.startswith('src/prismabuild/'):
                    module='prismabuild'+('.'+module if module else '')
                imports.update(aliases.get(module,()))
                for a in node.names:
                    imports.update(aliases.get(module+'.'+a.name,()))
                    if not module:imports.update(aliases.get(a.name,()))
            elif isinstance(node,ast.Constant) and isinstance(node.value,str):
                imports.update(aliases.get(node.value,()))
        graph[file]=sorted(imports-{file})
    def closure(roots):
        reached=set(roots)
        while True:
            more={path for path,imports in graph.items() if set(imports)&reached}-reached
            if not more:return sorted(reached)
            reached.update(more)
    pattern='produced_output|produced_spool|test_prepaid_writer_integration|test_consumed_origin_retirement|test_write_only_produced_output|test_produced_output|r13_1053_replay'
    raw=subprocess.check_output(['git','grep','-l','-E',pattern,BASE,'--','tests/*.py'],cwd=root,text=True)
    seeds={row.split(':',1)[1] for row in raw.splitlines()}
    manifest=json.loads((root/'tools/maintenance/c13_batch_migration.json').read_text())
    seeds.update(p for p in manifest['changed_callers'] if p.startswith('tests/'))
    seeds.update(['tests/test_batch_ledger_ownership.py','tests/test_duplication_baseline.py'])
    # Keep the native boundary seed to test/fixture modules. Reverse edges
    # include every test that implicitly imports a changed domain fixture.
    domain=closure(seeds)
    selected=sorted(p for p in domain if p.startswith('tests/test_') and p.count('/')==1)
    changed=set(subprocess.check_output(['git','diff','--name-only',BASE],cwd=root,text=True).splitlines())
    changed_prod={p for p in changed if p.startswith(('src/','tools/fleet/')) and p.endswith('.py')}
    broad=closure(changed_prod)
    return dict(base=BASE,head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),
                seeds=sorted(seeds),selected_whole_files=selected,domain_closure=domain,
                full_transitive_importers=broad,imports=graph,
                note='Source selection only. No native collection, test pass or whole-root credit. Queue-transitive population is exported for Root and not submitted here.')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,required=True)
    args=parser.parse_args();root=Path(__file__).resolve().parents[2]
    report=c13_population(root)
    with args.out.open('x') as f:json.dump(report,f,indent=2);f.write('\n')
    print(json.dumps({k:len(report[k]) for k in ('seeds','selected_whole_files','full_transitive_importers')}))

if __name__=='__main__':main()
