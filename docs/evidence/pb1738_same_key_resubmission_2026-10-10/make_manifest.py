#!/usr/bin/env python3
"""Rebuild the #1738 test-band data manifest (v2, one declared prelaunch phase). RECONSTRUCTION, 2026-10-10.

The original /home/rob/tmp/pb-1738-manifest.json was deleted with /home/rob/tmp on 2026-10-10 and I did not save it.
This script has the same structure and the same four entries. `produced_by.unix` differs, so the bytes and the
sha256 differ from the manifest the live row used (its manifest_sha256 was
4e3faeb10542e25cfc4ab9a6ee023062b30e77ed3c76ef39344b9cbf684ca269 in the row's residency block).
Run it after creating f0.bin..f3.bin (2 GiB random each) under /mnt/shared/fleet-ceo/pb-1738-testband.
"""
import json, os, sys, time
D = '/mnt/shared/fleet-ceo/pb-1738-testband'
out = sys.argv[1] if len(sys.argv) > 1 else 'pb-1738-manifest.json'
entries, total = [], 0
for i in range(4):
    p = f'{D}/f{i}.bin'
    n = os.path.getsize(p)
    assert n == 2 * 2**30, p
    entries.append({'path': p, 'offset': 0, 'bytes': n, 'sha256': None}); total += n
m = {'schema': 'prismaquant.prismabuild.data_manifest.v2',
     'produced_by': {'tool': 'pb-integrator/prismabuild#1738 test band', 'commit': '281006ef6374e4c058b84d0c306c2460a21771dd', 'unix': int(time.time())},
     'annotations': {'purpose': 'prismabuild#1738 same-key resubmission proof; throwaway'},
     'mount_prefix': '/mnt/shared', 'entries': entries, 'entry_count': len(entries), 'total_bytes': total,
     'read_plan': {'phases': [{'name': 'prelaunch-test', 'entry_indices': [0, 1, 2, 3], 'bytes': total, 'cumulative_bytes': total, 'resident_before_launch': True}], 'read_bytes': total}}
open(out, 'w').write(json.dumps(m, indent=1))
print('wrote', out, 'bytes', total)
