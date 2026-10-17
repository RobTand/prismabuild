"""Pin Core to the actual sibling file owner without changing runtime records."""
import argparse
import ast
import hashlib
import json
from pathlib import Path

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--root',type=Path,required=True)
parser.add_argument('--report',type=Path,required=True)
parser.add_argument('--apply',action='store_true')
args=parser.parse_args()
path=args.root/'src/prismabuild/core.py'
text=path.read_text()
node=next(n for n in ast.parse(text).body if isinstance(n,ast.Assign)
          and any(isinstance(t,ast.Name) and t.id=='_IMMUTABLE_FILES_SOURCE' for t in n.targets))
old=ast.literal_eval(node.value)
raw=(path.parent/'immutable_files.py').read_bytes()
new=dict(sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))
assert set(old)==set(new)
lines=text.splitlines(keepends=True)
assert node.value.lineno==node.value.end_lineno
line=lines[node.value.lineno-1]
lines[node.value.lineno-1]=line[:node.value.col_offset]+json.dumps(new)+line[node.value.end_col_offset:]
updated=''.join(lines)
ast.parse(updated)
if args.apply:path.write_text(updated)
args.report.write_text(json.dumps(dict(old=old,new=new,changed=old!=new,applied=args.apply,
    limit='Source binding only. Fresh actual current/retained runtime captures and physical producer qualification remain required.'),indent=2)+'\n')
print(json.dumps(dict(changed=old!=new,applied=args.apply,new=new)))
