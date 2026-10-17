"""Compare actual a8/current prune bytes and effects on the same private files."""
from __future__ import annotations
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

EPOCH_BASE = 'a8ba9c0c7beeeb379908924a60c6933e997c8df6'
EPOCH_CHILD = r'''
import base64,hashlib,json,os,sys
from pathlib import Path
from prismabuild import pool,reader_lease,residency_map
arm,work,mode,fragment_form,material_form,policy,operation = sys.argv[1:]
if arm == 'a8':
    from prismabuild import stage_release as owner
else:
    from prismabuild import staged_path as owner
work=Path(work);stage=work/'stage';root=work/'queue'/'residency'
consumer,mover='1'*64,'d'*64
ram=policy.startswith('ram');tier='ram:epoch-control' if ram else 'prismabuild-stage:epoch-control'
queue=pool.PoolQueue(work/'queue')
def wire(path):return base64.b64encode(path.read_bytes()).decode()
fpath=residency_map.fragment_path(root,consumer,mover)
mpath=reader_lease.material_path(root,consumer,mover)
if operation=='prepare':
    queue.records.ensure_layout();stage.mkdir(parents=True)
    owner.register_stage_root(queue,tier_id=tier,stage_root=stage)
    kind='ram_gib' if ram else 'stage_gib'
    queue.tier(tier).mint({kind:2});assert queue.tier(tier).take(mover,{kind:1})
    entries={};material={}
    for name in ['keep','extra']:
        path=stage/name;payload=(name.encode()+b'-actual-payload')*4;path.write_bytes(payload)
        key=residency_map.residency_map_key('/origin/'+name,0)
        row={'stage_path':str(path),'bytes':len(payload),'sha256':hashlib.sha256(payload).hexdigest(),'offset':0}
        entries[key]=row;material[key]={**row,'file_id':reader_lease.stat_identity(str(path))}
    if mode=='trim':entries={k:v for k,v in entries.items() if k.endswith('/keep')}
    else:(stage/'extra').unlink()
    fragment={'schema':residency_map.RESIDENCY_MAP_FRAGMENT_SCHEMA_V1,'consumer_action_key':consumer,'mover_action_key':mover,'tier_id':tier,'stage_root':str(stage),'manifest_sha256':'a'*64,'entries':entries}
    if ram:fragment['epoch']='epoch-current'
    residency_map.write_fragment(root,fragment)
    material_epoch='epoch-current' if ram else ('' if material_form=='empty' else None)
    if policy=='ram-mismatch':material_epoch='epoch-other'
    material_tier='prismabuild-stage:other' if policy=='tier-mismatch' else tier
    reader_lease.write_material(root,consumer_action_key=consumer,mover_action_key=mover,tier_id=material_tier,stage_root=str(stage),manifest_sha256='a'*64,generation='b'*32,entries=material,epoch=material_epoch)
    # These raw optional-field variants intentionally exercise historical prune
    # inputs. Empty fragment epoch is invalid and must retain, never authorize.
    if not ram and fragment_form!='absent':
        body=json.loads(fpath.read_bytes());body['epoch']=None if fragment_form=='none' else ''
        fpath.write_text(json.dumps(body,sort_keys=True)+'\n')
    if not ram and material_form=='none':
        body=json.loads(mpath.read_bytes());body['epoch']=None
        mpath.write_text(json.dumps(body,sort_keys=True)+'\n')
    (work/'fragment.initial').write_bytes(fpath.read_bytes());(work/'material.initial').write_bytes(mpath.read_bytes())
    print(json.dumps({'prepared':True}));raise SystemExit(0)
fpath.write_bytes((work/'fragment.initial').read_bytes());mpath.write_bytes((work/'material.initial').read_bytes())
before={'fragment':wire(fpath),'material':wire(mpath)}
observed=json.loads(fpath.read_bytes())
try:observed=residency_map.validate_fragment(observed)
except ValueError:pass
payload=(stage/'keep').read_bytes();identity=reader_lease.stat_identity(str(stage/'keep'));effects=[]
write_fragment=residency_map.write_fragment;write_material=reader_lease.write_material
def fragment_write(*a,**kw):
    effects.append('fragment');return write_fragment(*a,**kw)
def material_write(*a,**kw):
    effects.append('material');return write_material(*a,**kw)
residency_map.write_fragment=fragment_write;reader_lease.write_material=material_write
with queue.mover_transition_lock(mover):
    result=owner.prune_stale_mentions(queue,mover,consumer_action_key=consumer,stage=stage,tier_id=tier,root=root,observed=observed)
assert (stage/'keep').read_bytes()==payload
assert reader_lease.stat_identity(str(stage/'keep'))==identity
assert queue.tier(tier).holdings(mover)=={('ram_gib' if ram else 'stage_gib'):1}
after={'fragment':wire(fpath),'material':wire(mpath)}
print(json.dumps({'before':before,'after':after,'effects':effects,'receipt':{k:v for k,v in result.items() if k in ('retained_reason','entries_pruned','entries_absent','partial','pair_complete','errors')},'owner_file':owner.__file__,'owner_source_sha256':hashlib.sha256(Path(owner.__file__).read_bytes()).hexdigest()},sort_keys=True))
'''


def epoch_cases():
    cases = [(mode, f, m, 'stage') for mode in ('trim', 'shrink')
             for f in ('absent', 'none', 'empty') for m in ('absent', 'none', 'empty')]
    cases += [(mode, 'current', 'current', policy) for mode in ('trim', 'shrink')
              for policy in ('ram', 'ram-mismatch', 'tier-mismatch')]
    return cases


def epoch_replay(output: Path, *, observe: bool):
    if not (os.environ.get('PRISMABUILD_ACTION_SCOPE') or os.environ.get('PRISMABUILD_TEST_TIMEOUT_S')):
        raise SystemExit('run through published PB; admitted test children execute directly')
    checkout = Path(__file__).resolve().parents[2]
    archive = subprocess.check_output(['git', '-C', str(checkout), 'archive', EPOCH_BASE, 'src', 'tools/fleet'])
    rows = []
    with tempfile.TemporaryDirectory(prefix='c09-epoch-', dir=os.environ.get('TMPDIR')) as temporary:
        temporary = Path(temporary);old = temporary / 'old';old.mkdir()
        with tarfile.open(fileobj=io.BytesIO(archive)) as tf:
            tf.extractall(old, filter='data')
        def child(arm, work, case, operation):
            root = old if arm == 'a8' else checkout
            env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(root/'src'),str(root/'tools/fleet')]))
            result = subprocess.run([sys.executable, '-c', EPOCH_CHILD, arm, str(work), *case, operation],
                cwd=root, env=env, capture_output=True, text=True, check=True)
            return json.loads(result.stdout.splitlines()[-1])
        for index, case in enumerate(epoch_cases()):
            work=temporary/f'case-{index}'
            child('current',work,case,'prepare')
            old_result=child('a8',work,case,'act')
            current=child('current',work,case,'act')
            equal = (old_result['before']==current['before'] and old_result['after']==current['after']
                     and old_result['effects']==current['effects'] and old_result['receipt']==current['receipt'])
            rows.append({'case':list(case),'old':old_result,'current':current,'equal':equal})
    executable=Path(sys.executable).resolve();raw=executable.read_bytes()
    runtime={'version':sys.version,'executable':sys.executable,'resolved_executable':str(executable),
        'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'affinity':sorted(os.sched_getaffinity(0)),
        'native_threads':{k:os.environ.get(k) for k in ('OMP_NUM_THREADS','MKL_NUM_THREADS','OPENBLAS_NUM_THREADS')}}
    report={'base':EPOCH_BASE,'cases':rows,'wire_and_effects_equal':all(r['equal'] for r in rows),'runtime':runtime}
    output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2)+'\n')
    output.with_suffix('.python-executable').write_bytes(raw)
    data=output.read_bytes();print(json.dumps({'wire_and_effects_equal':report['wire_and_effects_equal'],'cases':len(rows),
        'different_cases':[r['case'] for r in rows if not r['equal']],
        'report':{'path':str(output),'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()},'runtime':runtime}))
    return 0 if observe or report['wire_and_effects_equal'] else 1


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--observe',action='store_true')
    args=parser.parse_args();raise SystemExit(epoch_replay(args.output,observe=args.observe))
