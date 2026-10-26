"""Read-only proof that a rebase retained reviewed source and imported main data."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
for option in ('checkout', 'reviewed', 'old-base', 'new-base', 'candidate', 'output'):
    parser.add_argument('--' + option, required=True)
args = parser.parse_args()


def git(*argv):
    return subprocess.check_output(['git', '-C', args.checkout, *argv], stderr=subprocess.DEVNULL)


def tree(ref):
    entries = git('ls-tree', '-rz', ref, '--', 'src', 'tests').split(b'\0')
    result = {}
    for entry in entries:
        if entry:
            metadata, path = entry.split(b'\t', 1)
            result[path] = metadata
    return result


reviewed, old_base, new_base, candidate = map(tree, (args.reviewed, args.old_base, args.new_base, args.candidate))
paths = reviewed.keys() | old_base.keys() | new_base.keys() | candidate.keys()
imported = {p for p in paths if old_base.get(p) != new_base.get(p)}
changed = {p for p in paths if reviewed.get(p) != candidate.get(p)}
if changed != imported:
    raise ValueError('Candidate changes differ from the incoming main change')
if any(p.endswith(b'.py') for p in changed):
    raise ValueError('Incoming change touches source or test logic; reassess validation')
for path in changed:
    if reviewed.get(path) != old_base.get(path) or candidate.get(path) != new_base.get(path):
        raise ValueError('Incoming main data was not retained exactly: ' + path.decode())
unchanged = sorted(paths - changed)
digest = hashlib.sha256(b'\0'.join(p + b'\t' + candidate[p] for p in unchanged)).hexdigest()
report = dict(reviewed=args.reviewed, old_base=args.old_base, new_base=args.new_base,
              candidate=args.candidate, unchanged_src_tests_entries=len(unchanged),
              unchanged_src_tests_inventory_sha256=digest,
              incoming_main_files=[dict(path=p.decode(), before=old_base.get(p, b'').decode(),
                                        after=new_base.get(p, b'').decode()) for p in sorted(changed)],
              source_or_test_logic_changed=False)
Path(args.output).write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(dict(unchanged_entries=len(unchanged), imported_main_files=len(changed),
                      source_or_test_logic_changed=False)))
