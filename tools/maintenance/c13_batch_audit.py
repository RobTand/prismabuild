#!/usr/bin/env python3
"""Occurrence-preserving C13 source c13_inventory. Reads source; imports no product code."""
from __future__ import annotations
import argparse
import ast
from collections import Counter
import gzip
import json
from pathlib import Path
import subprocess

BASE = 'd2f92f5ff533489182dddaf286feb54afde6f34a'


def c13_git(*args):
    return subprocess.check_output(['git', *args])


def c13_inventory(revision):
    revision = c13_git('rev-parse', revision+'^{commit}').decode().strip()
    paths = c13_git('ls-tree', '-r', '--name-only', revision).decode().splitlines()
    result = {'revision': revision, 'files': {}}
    for path in paths:
        if not path.endswith('.py'):
            continue
        raw = c13_git('show', f'{revision}:{path}')
        try:
            source = raw.decode()
            tree = ast.parse(source)
        except (SyntaxError, UnicodeError):
            continue
        rows = []
        class Visit(ast.NodeVisitor):
            def __init__(self):
                self.scope = []
            def visit_FunctionDef(self, node):
                rows.append({'kind':'function', 'line':node.lineno,
                             'scope':'.'.join([*self.scope,node.name]),
                             'decorators':[ast.dump(d,include_attributes=False) for d in node.decorator_list],
                             'ast':ast.dump(node,include_attributes=False)})
                self.scope.append(node.name)
                self.generic_visit(node)
                self.scope.pop()
            visit_AsyncFunctionDef = visit_FunctionDef
            def visit_ClassDef(self, node):
                rows.append({'kind':'class', 'line':node.lineno,
                             'scope':'.'.join([*self.scope,node.name]),
                             'decorators':[ast.dump(d,include_attributes=False) for d in node.decorator_list],
                             'ast':ast.dump(node,include_attributes=False)})
                self.scope.append(node.name)
                self.generic_visit(node)
                self.scope.pop()
            def generic_visit(self, node):
                fields = {ast.Assert:'assert',ast.If:'if',ast.IfExp:'if-expression',
                          ast.Call:'call',ast.Import:'import',ast.ImportFrom:'import',
                          ast.Raise:'raise',ast.With:'with',ast.Match:'match',
                          ast.Assign:'assignment',ast.AnnAssign:'annotated-assignment',
                          ast.AugAssign:'augmented-assignment',ast.NamedExpr:'named-expression',
                          ast.Delete:'delete',ast.While:'while',ast.For:'for',
                          ast.comprehension:'comprehension'}
                kind = fields.get(type(node))
                if kind:
                    item = node.test if isinstance(node,(ast.If,ast.IfExp)) else node
                    line = node.target.lineno if isinstance(node,ast.comprehension) else node.lineno
                    rows.append({'kind':kind,'line':line,'scope':'.'.join(self.scope),
                                 'ast':ast.dump(item,include_attributes=False)})
                elif isinstance(node,ast.Constant) and isinstance(node.value,str):
                    rows.append({'kind':'quoted','line':node.lineno,'scope':'.'.join(self.scope),'value':node.value})
                super().generic_visit(node)
        Visit().visit(tree)
        result['files'][path] = {'git_blob':subprocess.check_output(['git','hash-object','--stdin'],input=raw).decode().strip(),'occurrences':rows}
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--revision',default=BASE)
    p.add_argument('--out',type=Path,required=True)
    args = p.parse_args()
    data = c13_inventory(args.revision)
    with gzip.open(args.out,'wt') as handle:
        json.dump(data,handle,separators=(',',':'))
    counts = Counter(row['kind'] for file in data['files'].values() for row in file['occurrences'])
    print(json.dumps({'revision':args.revision,'files':len(data['files']),'counts':counts,'out':str(args.out)}))

if __name__ == '__main__':
    main()
