#!/usr/bin/env python3
"""Copy pb-integrator's files from ~/fleet/inventory into this directory, sorted by kind, with a manifest (#1746, D65.4).

Run once, 2026-10-10, by pb-integrator.  Copies, never moves: the originals stay in place.
Never copies a key-shaped file.  Redacts the value of one probe key where a log quotes it.
Files that belong to the #1738 test band and the gen12 publication go to their own branches and are skipped here.
"""
import hashlib, json, os, re, shutil, sys

SRC = os.path.expanduser('~/fleet/inventory')
OUT = os.path.dirname(os.path.abspath(__file__))
SECRET_FILES = ['pb-live-probe-20261008.key', 'pb-d1-cov-20261009/token']
REDACT_LOGS = {'pb-live-probe-20261008.log': 'pb-live-probe-20261008.key'}
SKIP = re.compile(r'gen12|pb-1738-testband')


def kind(name):
    n = os.path.basename(name)
    if n.endswith('.md'): return 'notes'
    if re.search(r'^pb-(publish|canary|gen\d|gate-gen|shape-gen|emitter|roster|carry|sdk4-1514|1598|wait-and-submit|submit-serve)', n) or 'publish' in n:
        return 'publication'
    if re.search(r'qualify|bisect|flake|gate-1619|1543|attrib|d1-|d2-|diskcheck|leg4|accel|confirm|stage-gate|1657|cpu-jsonschema', n):
        return 'qualification'
    if re.search(r'stage|ram-holders|tier-reclaim|dl380-root|sparky-image', n): return 'stage'
    if re.search(r'band|watch|probe|lifetime', n): return 'probes_and_watches'
    if n.startswith('pb-batch') or n.startswith('pb-integrator'): return 'batch_and_checker'
    return 'other'


def sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


secret_values = {f: open(os.path.join(SRC, f)).read().strip() for f in SECRET_FILES if os.path.exists(os.path.join(SRC, f))}
rows, skipped = [], []
for entry in sorted(os.listdir(SRC)):
    if not entry.startswith('pb-'): continue
    full = os.path.join(SRC, entry)
    paths = [entry] if os.path.isfile(full) else [os.path.relpath(os.path.join(r, f), SRC) for r, _, fs in os.walk(full) for f in sorted(fs)]
    for rel in paths:
        if rel in SECRET_FILES: skipped.append((rel, 'key-shaped file; never committed')); continue
        if SKIP.search(rel): skipped.append((rel, 'goes to its own branch (#1738 or gen12 evidence)')); continue
        group = kind(rel) if '/' not in rel else 'directories'
        dest_rel = os.path.join(group, rel)
        dest = os.path.join(OUT, dest_rel)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        note = ''
        if rel in REDACT_LOGS:
            text = open(os.path.join(SRC, rel), errors='replace').read()
            value = secret_values[REDACT_LOGS[rel]]
            text = text.replace(value, '<REDACTED-64HEX>')
            open(dest, 'w').write(text)
            note = 'REDACTED: the value of ' + REDACT_LOGS[rel] + ' replaced by <REDACTED-64HEX>'
        else:
            shutil.copy2(os.path.join(SRC, rel), dest)
        rows.append((dest_rel, os.path.join('~/fleet/inventory', rel), os.path.getsize(dest), sha(dest), note))

with open(os.path.join(OUT, 'MANIFEST.md'), 'w') as f:
    f.write('# Manifest: pb-integrator files copied from ~/fleet/inventory (prismabuild#1746)\n\n')
    f.write('Copies, byte for byte, except the one file marked REDACTED. Sizes and sha256 are of the copy in this branch.\n\n')
    f.write('| copy in this branch | original | bytes | sha256 | note |\n|---|---|---|---|---|\n')
    for r in rows: f.write('| `%s` | `%s` | %d | `%s` | %s |\n' % r)
    f.write('\n## Not copied\n\n| original | why |\n|---|---|\n')
    for s in skipped: f.write('| `%s` | %s |\n' % s)
print('copied', len(rows), 'files,', sum(r[2] for r in rows), 'bytes; skipped', len(skipped))
for s in skipped: print('  skipped', s)
