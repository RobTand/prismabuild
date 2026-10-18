"""Admitted retained-data replay for proof-runtime cost and byte-identity checks."""
import argparse
from collections import defaultdict
import gc
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import types

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src'))
from prismabuild.digest_primitives import compact_ascii_json_bytes, new_sha256, raw_sha256

COST_BASE = 'f9c7fa76f0b09d8ef2b75c915d13778e822e0a32'
COST_RAW = Path('/mnt/shared/prismabuild-agent-evidence-rob/refactor-20261016/active/claude-master-20261018/uid0-capture-lifecycle-r6/packet-2dd080f5263a/run-root/standard3123/capture-retained-0/raw/d32b2a038ab6453c8a0fdc6d9bb745c0692ea30ac7c96d378b4a289d49277ec4')
COST_IMAGE_SHA = '3e02bffed0f40040792421ada698f6773b24c82b808166876fd8d8e196eb8b08'
COST_MOUNTED_SHA = '6abffce36ba9b70d5dbd56ba98e897bc147e841f93b835b2a6f04e0d825240a8'
COST_ARCHIVE_SHA = '9a5ed19be9cffc760c31a550529f44f5c1c40708e68f7ecfe327fc243cd3fcd0'
COST_FUNCTIONS = ('audit_runtime_closure','audit_runtime_completion','audit_native_closure',
    'runtime_namespace_state','runtime_namespace_state_record','runtime_replay_lookup',
    'runtime_absence_witness','runtime_namespace_entries','runtime_elf_search_directories',
    'runtime_directory_candidate_spellings','canonical','digest','inventory',
    'load_profile','verify_closure','verify_runtime_negatives','verify_runtime_components')


def cost_emit(value):
    print('proof-runtime-cost: ' + json.dumps(value, separators=(',', ':')), flush=True)


def cost_memory():
    status = dict(line.split(':', 1) for line in Path('/proc/self/status').read_text().splitlines() if ':' in line)
    result = {key: int(status[key].split()[0])*1024 for key in ('VmRSS','VmHWM','VmSwap')}
    cgroup = next(line.split(':',2)[2] for line in Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::'))
    location = Path('/sys/fs/cgroup') / cgroup.lstrip('/')
    if (location/'memory.stat').exists():
        values = dict(line.split() for line in (location/'memory.stat').read_text().splitlines())
        result.update({key:int(values.get(key,0)) for key in ('anon','file','shmem','kernel','slab')})
        result['cgroup_current'] = int((location/'memory.current').read_text())
    return result


class CostMemorySampler:
    def __init__(self):
        self.stop = threading.Event()
        self.peaks = defaultdict(int)
        self.thread = threading.Thread(target=self.sample, daemon=True)

    def sample(self):
        while not self.stop.is_set():
            for key,value in cost_memory().items():
                self.peaks[key] = max(self.peaks[key], value)
            self.stop.wait(.05)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *unused):
        self.stop.set()
        self.thread.join()


def cost_owner(label, source):
    module = types.ModuleType('proof_cost_' + label)
    module.__file__ = str(Path(__file__).resolve().parents[2]/'tests/proof_container_runtime.py')
    sys.modules[module.__name__] = module
    exec(compile(source, module.__file__, 'exec'), module.__dict__)
    return module


def cost_instrument(owner):
    totals = defaultdict(lambda: dict(calls=0,wall_s=0.,cpu_s=0.,max_wall_s=0.))
    for name in COST_FUNCTIONS:
        original = getattr(owner, name)
        def measured(*args, _name=name, _original=original, **kwargs):
            state = args[0].get('_namespace_state','base') if args and isinstance(args[0],dict) else 'base'
            record = totals[_name + ':' + state]
            wall,cpu = time.perf_counter(),time.process_time()
            try:
                return _original(*args, **kwargs)
            finally:
                elapsed = time.perf_counter()-wall
                record['calls'] += 1
                record['wall_s'] += elapsed
                record['cpu_s'] += time.process_time()-cpu
                record['max_wall_s'] = max(record['max_wall_s'], elapsed)
        setattr(owner, name, measured)
    return totals


def cost_read_json(path, sha):
    raw = path.read_bytes()
    if raw_sha256(raw) != sha:
        raise ValueError('retained cost input digest differs: ' + str(path))
    return json.loads(raw)


def cost_prepare(private):
    cas = Path('/mnt/shared/prismabuild-fleet/cas/blobs')
    image = cost_read_json(cas/COST_IMAGE_SHA[:2]/COST_IMAGE_SHA, COST_IMAGE_SHA)['value']
    historical = cost_read_json(COST_RAW/'checkpoints/mounted_runtime.json', COST_MOUNTED_SHA)['value']
    profile = json.loads(historical['profile_json'])
    old_closure = profile['closure']
    closure = dict(image['completed_namespace'],schema='prismabuild.proof_runtime_closure.v3',
        graph_policy=old_closure['graph_policy'],elf_machine=old_closure['elf_machine'],
        elf_roots=image['completed_namespace']['plan']['elf_roots'],loader=old_closure['loader'])
    if closure['completion']['image_loader_cache'] != closure['loader']['cache']:
        raise ValueError('retained loader facts belong to different image cache bytes')
    profile.update(schema='prismabuild.proof_runtime_profile.v3',closure=closure,
        primary={key:closure['completion']['host_observation'][key] for key in ('version','executable','elf_machine')},
        installed_reader=dict(closure['plan']['reader_identity'],uid=65534,gid=65534,
                              link_target=closure['plan']['reader_target']))
    del historical, old_closure, image
    roots=closure['roots']+closure['completion']['base_host_roots']+closure['completion']['base_image_roots']
    wanted={entry['sha256']:entry['bytes'] for root in roots for entry in root['entries'].values() if entry['kind']=='file'}
    members=private/'members';members.mkdir()
    archive=COST_RAW/'closure-archive.tar.gz'
    hasher=new_sha256()
    with archive.open('rb') as handle:
        for chunk in iter(lambda:handle.read(1024*1024),b''):
            hasher.update(chunk)
    if hasher.hexdigest()!=COST_ARCHIVE_SHA:
        raise ValueError('retained archive differs')
    with tarfile.open(archive,'r|gz') as stream:
        for entry in stream:
            sha=entry.name.removeprefix('by-sha256/')
            if sha not in wanted:
                continue
            if not entry.isfile() or entry.size!=wanted[sha] or entry.name!='by-sha256/'+sha:
                raise ValueError('retained member descriptor differs')
            hasher=new_sha256()
            with stream.extractfile(entry) as source, (members/sha).open('xb') as target:
                for chunk in iter(lambda:source.read(1024*1024),b''):
                    hasher.update(chunk);target.write(chunk)
            if hasher.hexdigest()!=sha:
                raise ValueError('retained member bytes differ')
    missing=set(wanted)-{path.name for path in members.iterdir()}
    if missing:
        raise ValueError('retained archive lacks completed runtime content: '+str(sorted(missing)))
    return profile,members


def cost_replay(owner, profile, members, label):
    totals=cost_instrument(owner)
    closure=profile['closure']
    entries=owner.runtime_namespace_entries(closure)
    wall,cpu=time.perf_counter(),time.process_time()
    cost_emit(dict(event='audit_start',label=label,memory=cost_memory()))
    with CostMemorySampler() as sample:
        result=owner.audit_runtime_closure(closure,
            lambda path:members/entries[path]['sha256'],content_path=lambda sha:members/sha)
    record=dict(event='audit_finished',label=label,wall_s=time.perf_counter()-wall,
        cpu_s=time.process_time()-cpu,peaks=dict(sample.peaks),functions=dict(totals),
        audit=result,audit_sha256=raw_sha256(compact_ascii_json_bytes(result)))
    cost_emit(record)
    return result,record


def profile_runtime_cost_main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--compare',action='store_true')
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[2]
    before=subprocess.check_output(['git','show',COST_BASE+':tests/proof_container_runtime.py'],cwd=root)
    current=(root/'tests/proof_container_runtime.py').read_bytes()
    executable=Path('/proc/self/exe').read_bytes()
    cost_emit(dict(event='start',version=list(sys.version_info[:3]),executable=dict(path=sys.executable,
        bytes=len(executable),sha256=raw_sha256(executable)),affinity=sorted(os.sched_getaffinity(0)),
        old_source_sha256=raw_sha256(before),new_source_sha256=raw_sha256(current)))
    with tempfile.TemporaryDirectory(prefix='proof-runtime-cost-',dir='/home/rob/pbt17') as work:
        wall=time.perf_counter()
        with CostMemorySampler() as sample:
            profile,members=cost_prepare(Path(work))
        cost_emit(dict(event='prepared',wall_s=time.perf_counter()-wall,peaks=dict(sample.peaks)))
        old,old_record=cost_replay(cost_owner('old',before),profile,members,'old')
        if args.compare:
            gc.collect()
            new,new_record=cost_replay(cost_owner('new',current),profile,members,'new')
            if compact_ascii_json_bytes(old)!=compact_ascii_json_bytes(new):
                raise ValueError('old/new retained audit bytes differ')
            cost_emit(dict(event='identity',equal=True,sha256=raw_sha256(compact_ascii_json_bytes(new)),
                old_wall_s=old_record['wall_s'],new_wall_s=new_record['wall_s']))
    cost_emit(dict(event='complete',ok=True,max_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024))


if __name__=='__main__':
    profile_runtime_cost_main()
