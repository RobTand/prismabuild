"""Read-only, admitted proof-runtime graph diagnosis using real retained inputs."""
import argparse
import ast
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time
import types


def preflight_module(name, source):
    module = types.ModuleType(name)
    module.__file__ = '/source/tests/' + name + '.py'
    sys.modules[name] = module
    exec(compile(source, module.__file__, 'exec'), module.__dict__)
    return module


def preflight_collect_loops(module, source, names, record):
    """Continue independent loop items after recording a production refusal."""
    class Collect(ast.NodeTransformer):
        def visit_For(self, node):
            self.generic_visit(node)
            handler = ast.parse("try:\n pass\nexcept (ValueError, OSError, KeyError, RuntimeError) as error:\n _proof_preflight_refusal(error, locals())\n continue").body[0]
            handler.body = node.body
            node.body = [ast.copy_location(handler, node)]
            return node
    tree = ast.parse(source)
    tree.body = [Collect().visit(node) for node in tree.body
                 if isinstance(node, ast.FunctionDef) and node.name in names]
    module.__dict__['_proof_preflight_refusal'] = record
    exec(compile(ast.fix_missing_locations(tree), module.__file__, 'exec'), module.__dict__)


def preflight_image(payload):
    sources = payload['sources']
    runtime = preflight_module('proof_container_runtime', sources['proof_container_runtime'])
    preflight_module('proof_capture_capacity', sources['proof_capture_capacity'])
    capture = preflight_module('capture_proof_runtime', sources['capture_proof_runtime'])
    host = payload['host']
    budget = dict(host['consumed_budget'], deadline=time.monotonic() + runtime.CAPTURE_BOUNDS['runtime_seconds'])
    image = capture.runtime_capture('image', budget)
    states, current, edges, calls = {}, ['before_installation'], [], [0]
    refusals = {}
    def record(error, context):
        selected = {key:value for key,value in context.items()
                    if key in ('path','spelling','needed','interpreter','directory') and isinstance(value,str)}
        value = dict(type=type(error).__name__, message=str(error), state=current[0], context=selected)
        refusals[json.dumps(value)] = value
        if isinstance(error, TimeoutError) or 'capture geometry exceeds' in str(error):
            raise error
    original_discovery = capture.discover_image_runtime_graph
    original_search = capture.runtime_elf_search_directories
    original_walk = capture.walk_runtime_path
    active = [{}]
    def discovery(*args, **kwargs):
        current[0] = ('before_installation','after_installation')[calls[0] % 2]
        calls[0] += 1
        return original_discovery(*args, **kwargs)
    def search(metadata, aliases):
        active[0] = metadata
        directories, own = original_search(metadata, aliases)
        states[current[0]] = dict(metadata=dict(metadata), aliases=dict(aliases),
            search_directories=sorted(directories), own_directories={key: sorted(value) for key,value in own.items()})
        return directories, own
    def walk(spelling, lookup):
        result = original_walk(spelling, lookup)
        frame = sys._getframe(1)
        if (frame.f_code.co_name == 'resolve_observation'
                and frame.f_back.f_code.co_name == 'retain_candidate'
                and isinstance(result, runtime.RuntimeResolved)):
            caller = frame.f_back.f_back
            metadata = caller.f_locals.get('metadata')
            origin = next((path for path,value in active[0].items() if value is metadata), None)
            edges.append(dict(state=current[0], source=origin, spelling=spelling,
                terminal=result.terminal, needed=caller.f_locals.get('needed'),
                interpreters=[] if metadata is None else metadata['interpreters']))
        return result
    capture.discover_image_runtime_graph = discovery
    capture.runtime_elf_search_directories = search
    capture.walk_runtime_path = walk
    refusal = None
    try:
        capture.complete_image_closure(host, image, budget, host_checkpoint=payload['host_descriptor'])
    except (ValueError,OSError,KeyError,RuntimeError) as error:
        refusal = dict(type=type(error).__name__, message=str(error), state=current[0])
        record(error,{})
        preflight_collect_loops(runtime,sources['proof_container_runtime'],
            {'runtime_elf_search_directories'},record)
        preflight_collect_loops(capture,sources['capture_proof_runtime'],
            {'discover_image_runtime_graph'},record)
        original_search = runtime.runtime_elf_search_directories
        original_discovery = capture.discover_image_runtime_graph
        capture.runtime_elf_search_directories = search
        capture.discover_image_runtime_graph = discovery
        budget = dict(image['consumed_budget'],deadline=budget['deadline'])
        calls[0] = 0
        try:
            capture.complete_image_closure(host,image,budget,host_checkpoint=payload['host_descriptor'])
        except (ValueError,OSError,KeyError,RuntimeError) as remaining:
            record(remaining,{})
    return dict(schema='prismabuild.proof_runtime_preflight.v1',
        source_ref=payload['source_ref'], source_sha256={key:runtime.digest(value.encode()) for key,value in sources.items()},
        host_checkpoint=payload['host_descriptor'], host_observation=host['observation'],
        image_observation=image['observation'], image_elf_roots=image['elf_roots'],
        image_executable_metadata=image['elf_index'][image['observation']['executable']['resolved_path']],
        image_executable_inventory=next(root['entries']['.'] for root in image['roots']
            if root['path'] == image['observation']['executable']['resolved_path']),
        states=states, candidate_edges=edges, refusal=refusal, refusals=list(refusals.values()),
        diagnostic_continuation=refusal is not None,
        zero_refusals_requires_unmodified_production_success=True,
        image_credentials=dict(uid=os.getuid(),gid=os.getgid()),
        peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        unobservable=['root-owned reader creation and root role credentials; this action creates no reader link'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image-child', action='store_true')
    parser.add_argument('--image')
    parser.add_argument('--image-python')
    parser.add_argument('--host-checkpoint', type=Path)
    parser.add_argument('--source-ref', default='HEAD')
    args = parser.parse_args()
    if args.image_child:
        print(json.dumps(preflight_image(json.load(sys.stdin))), flush=True)
        return 0
    root = Path(__file__).resolve().parents[2]
    sources = {name:subprocess.check_output(['git','show',args.source_ref + ':tests/' + name + '.py'],cwd=root).decode()
               for name in ('proof_container_runtime','capture_proof_runtime','proof_capture_capacity')}
    runtime = preflight_module('proof_container_runtime', sources['proof_container_runtime'])
    raw = args.host_checkpoint.read_bytes()
    host = json.loads(raw)['value']
    descriptor = dict(bytes=len(raw),sha256=runtime.digest(raw))
    payload = dict(sources=sources,host=host,host_descriptor=descriptor,source_ref=args.source_ref)
    command = ['docker','run','--rm','--read-only','--network','none','--cap-drop','ALL',
        '--security-opt','no-new-privileges','--user',str(os.getuid()) + ':' + str(os.getgid()),
        '--cpuset-cpus',','.join(map(str,sorted(os.sched_getaffinity(0)))),
        '-e','PYTHONDONTWRITEBYTECODE=1',
        '-e','OMP_NUM_THREADS=1','-e','MKL_NUM_THREADS=1','-e','OPENBLAS_NUM_THREADS=1','-v',str(root) + ':/source:ro','-i',
        '--entrypoint',args.image_python,args.image,'-B',
        '/source/tools/maintenance/preflight_proof_runtime.py','--image-child']
    result = subprocess.run(command,input=json.dumps(payload),capture_output=True,text=True,timeout=600)
    if result.returncode:
        raise ValueError('read-only image preflight failed: ' + result.stderr[-4000:])
    report = json.loads(result.stdout)
    selected = {path for state in report['states'].values() for path in state['metadata']}
    suspects = {path for path in host['elf_index'] if any(name in path for name in ('libLLVM','libclang','libhivex','libguestfs'))}
    verified = {}
    entries = runtime.runtime_namespace_entries(host)
    for path in sorted((selected & host['elf_index'].keys()) | suspects):
        actual = runtime.runtime_elf_observation(path)
        if actual != host['elf_index'][path]:
            raise ValueError('actual host ELF metadata differs from retained checkpoint: ' + path)
        raw = Path(path).read_bytes()
        if runtime.digest(raw) != entries[path]['sha256']:
            raise ValueError('actual host bytes differ from retained checkpoint: ' + path)
        verified[path] = dict(bytes=len(raw),sha256=entries[path]['sha256'],reached=path in selected,
                              metadata=actual)
    report.update(actual_host_elfs_verified=verified, image=args.image,
        host_credentials=dict(uid=os.getuid(),gid=os.getgid()),
        interpreter=dict(version=sys.version,version_info=list(sys.version_info[:3]),
            executable=dict(path=sys.executable,resolved_path=str(Path(sys.executable).resolve()),
                bytes=Path('/proc/self/exe').stat().st_size,sha256=runtime.digest(Path('/proc/self/exe').read_bytes()))),
        affinity=sorted(os.sched_getaffinity(0)),
        native_threads={key:os.environ.get(key) for key in ('OMP_NUM_THREADS','MKL_NUM_THREADS','OPENBLAS_NUM_THREADS')},
        host_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    print('proof-runtime-preflight: ' + json.dumps(report,separators=(',',':')),flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
