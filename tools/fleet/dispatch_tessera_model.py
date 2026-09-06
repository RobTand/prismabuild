"""Dispatch a complete Tessera serving export through PB-owned layer quanta.

The caller supplies the whole source, plan, scales and pinned encoder/image.
PB derives partition ownership using the producer contract, submits a campaign,
verifies its CAS outputs, then admits assembly behind the complete-set barrier.
"""
from __future__ import annotations

import argparse
import io
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile

sys.path.insert(0, str(Path(__file__).resolve().parent))
from runtime_paths import generation_root
ROOT = generation_root(__file__)
sys.path.insert(0, str(ROOT / 'src'))
from prismabuild import core, pool, tessera_model as model
# Development dispatchers still submit through the published client contract.
PUBLISHED_TOOLS = Path('/mnt/shared/prismabuild-fleet/repo/tools')


def published_client():
    """Import the published client tools, and return them.

    Reading this directory is reaching the live fleet, so it happens when a
    command runs and not when the module is imported: importing a tool must
    not depend on the mount being up, and a test that imports this one must
    not read or write the real store.
    """

    if PUBLISHED_TOOLS.is_dir() and str(PUBLISHED_TOOLS) not in sys.path:
        sys.path.insert(0, str(PUBLISHED_TOOLS))
    import pbcampaign
    import pbrun
    import pbwait
    return pbcampaign, pbrun, pbwait


def stage_checkout(encoder, revision, workspace, plan, scales):
    workspace = Path(workspace)
    if workspace.exists():
        raise ValueError('workspace already exists; use --resume for an existing export')
    commit = subprocess.check_output(['git', '-C', str(encoder), 'rev-parse', revision + '^{commit}'], text=True).strip()
    if commit != revision:
        raise ValueError('--encoder-revision must be the full immutable commit ID')
    archive = subprocess.check_output(['git', '-C', str(encoder), 'archive', commit])
    workspace.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
        bundle.extractall(workspace / 'encoder', filter='data')
    shutil.copyfile(ROOT / 'src/prismabuild/tessera_model.py', workspace / 'worker.py')
    shutil.copyfile(plan, workspace / 'plan.json')
    if not isinstance(model.read_json(workspace / 'plan.json'), dict):
        raise ValueError('plan must be a JSON object')
    if scales:
        shutil.copyfile(scales, workspace / 'scales.safetensors')
    # pbrun snapshots tracked and untracked bytes. Nothing reaches main here.
    subprocess.run(['git', 'init', '-q', str(workspace)], check=True)
    subprocess.run(['git', '-C', str(workspace), '-c', 'user.name=PrismaBuild',
                    '-c', 'user.email=prismabuild@localhost', 'commit', '--allow-empty',
                    '-qm', 'PrismaBuild sealed producer workspace'], check=True)
    (workspace / '.git/info/exclude').write_text('/.pb-state/\n')
    return commit


def campaign_row(workspace, spec, command, *, index=None):
    gpu = command == 'encode'
    argv = ['/usr/bin/python3', 'worker.py', command]
    if index is not None:
        argv += ['--index', str(index)]
    return {'cwd': str(workspace), 'argv': argv,
            'demand': {'cpu': spec['cpus'] if gpu else 1,
                       'mem_gb': spec['mem_gb'] if gpu else spec['assembly_mem_gb'],
                       **({'gpu': 1} if gpu else {})},
            'tags': spec['tags'],
            'env': {'OMP_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1', 'OPENBLAS_NUM_THREADS': '1'},
            'retry_safe': True}


def receipt_result(key, cas):
    action = model.read_json(cas.root / 'requests' / key[:2] / f'{key}.json')
    receipt = cas.lookup(action)
    if receipt is None:
        raise ValueError(f'no verified receipt for {key}')
    lines = cas.result_path(receipt, action).read_text().splitlines()
    records = [json.loads(line[len(model.RESULT_PREFIX):]) for line in lines if line.startswith(model.RESULT_PREFIX)]
    if len(records) != 1:
        raise ValueError(f'{key}: result must contain one producer completion record')
    return records[0], receipt


def run_stage(rows, workspace, stage, wait_s):
    # Existing campaign and wait interfaces own all placement and admission.
    pbcampaign, pbrun, pbwait = published_client()
    submissions = pbcampaign.submit(rows, transport='pool')
    model.atomic_json(workspace / '.pb-state' / f'{stage}-submissions.json', submissions)
    if any(row['status'] == 'refused' for row in submissions):
        raise ValueError(f'{stage}: refused submission; see {stage}-submissions.json')
    pending = [row for row in submissions if row['status'] in ('submitted', 'attached')]
    queue = pool.PoolQueue(pbrun.SH / 'pb-queue')
    cas = core.PrismaBuildCAS(pbrun.SH / 'cas')
    waited = pbwait.wait_for_keys(queue, [row['action_key'] for row in pending], cas=cas,
                                 wait_s=wait_s, generations={row['action_key']: row.get('published_unix') for row in pending})
    table = pbcampaign.rows_for(submissions, waited)
    model.atomic_json(workspace / '.pb-state' / f'{stage}-endings.json', table)
    print(pbwait.render(table), flush=True)
    if pbwait.verdict(table):
        for row in table:
            if not row.get('succeeded'):
                failure = queue.item_path(pool.FAILED, row['action_key'])
                if failure.exists():
                    detail = model.read_json(failure).get('detail', {})
                    print(str(detail.get('stdout', ''))[-4000:], file=sys.stderr)
        raise ValueError(f'{stage}: not all actions succeeded; assembly remains blocked')
    results, receipts = [], []
    for row in submissions:
        result, receipt = receipt_result(row['action_key'], cas)
        results.append(result)
        receipts.append(receipt)
    model.atomic_json(workspace / '.pb-state' / f'{stage}-receipts.json', receipts)
    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, help='the complete Tessera source export to dispatch')
    parser.add_argument('--plan', type=Path, help='the layer plan the campaign partitions into quanta')
    parser.add_argument('--input-scales', type=Path, help='optional pre-computed scales to seal into the workspace')
    parser.add_argument('--encoder-checkout', type=Path, help='Git checkout the pinned encoder is archived from')
    parser.add_argument('--encoder-revision', help='full immutable commit ID of the encoder to pin')
    parser.add_argument('--image', help='qualified producer repository@sha256 digest')
    parser.add_argument('--out', type=Path, help='shared path the assembled model is written to')
    parser.add_argument('--workspace', type=Path, required=True, help='directory this dispatch seals its job, receipts and state into')
    parser.add_argument('--resume', action='store_true', help='continue the dispatch already sealed in --workspace')
    parser.add_argument('--cpus', type=int, default=1, help='CPU reservation for each layer quantum')
    parser.add_argument('--mem-gb', type=int, default=16, help='memory reservation for each layer quantum, in GB')
    parser.add_argument('--assembly-mem-gb', type=int, default=4, help='memory reservation for the assembly action, in GB')
    parser.add_argument('--tag', action='append', dest='tags', default=None, help='placement tag; repeatable, defaults to gb10')
    parser.add_argument('--grid', default='E4M3', help='quantization grid the encoder is run with')
    parser.add_argument('--q256', type=int, default=1024, help='quanta per 256 rows the plan is partitioned at')
    parser.add_argument('--wait-s', type=float, default=86400., help='seconds to wait for a stage\'s actions before giving up')
    args = parser.parse_args(argv)
    workspace = args.workspace.resolve()
    if args.resume:
        if any(getattr(args, name) is not None for name in ('source', 'plan', 'input_scales', 'encoder_checkout', 'encoder_revision', 'image', 'out')):
            parser.error('--resume uses the sealed workspace; input overrides require a new workspace')
        spec = model.read_json(workspace / 'job.json')
    else:
        for name in ('source', 'plan', 'encoder_checkout', 'encoder_revision', 'image', 'out'):
            if getattr(args, name) is None:
                parser.error('--' + name.replace('_', '-') + ' is required')
        if not re.fullmatch(r'[^\s]+@sha256:[0-9a-f]{64}', args.image):
            parser.error('--image must be an immutable repository@sha256 digest')
        if min(args.cpus, args.mem_gb, args.assembly_mem_gb) < 1:
            parser.error('resource reservations must be positive')
        for path in (args.source.resolve(), args.out.resolve()):
            if not path.is_relative_to('/mnt/shared') or ':' in str(path):
                parser.error('source and output must be shared paths below /mnt/shared without colons')
        if args.out.exists():
            parser.error('--out already exists')
        commit = stage_checkout(args.encoder_checkout, args.encoder_revision, workspace,
                                args.plan, args.input_scales)
        spec = {'schema': model.SCHEMA, 'source': str(args.source.resolve()),
                'out': str(args.out.resolve()), 'parts': str(args.out.resolve()) + '.parts',
                'encoder_commit': commit, 'image': args.image,
                'adapter_sha256': model.digest_file(workspace / 'worker.py'),
                'plan_sha256': model.digest_file(workspace / 'plan.json'),
                'scales': model.digest_file(workspace / 'scales.safetensors') if args.input_scales else None,
                'cpus': args.cpus, 'mem_gb': args.mem_gb, 'assembly_mem_gb': args.assembly_mem_gb,
                'tags': args.tags or ['gb10'], 'grid': args.grid, 'q256': args.q256}
        model.atomic_json(workspace / 'job.json', spec)
    if 'contract' not in spec:
        _, pbrun, _ = published_client()
        queue = pool.PoolQueue(pbrun.SH / 'pb-queue')
        offers = queue._matching_offers(
            {'tags': spec['tags'], 'resources': {'gpu': 1, 'cpu': spec['cpus'], 'mem_gb': spec['mem_gb']},
             'needs_gpu': True}, live=queue.offers())
        if not offers:
            raise ValueError('no live eligible GPU worker offers')
        preparation = []
        for offer in offers:
            host = offer['host']
            if host not in offer.get('tags', []):
                raise ValueError(f'worker {host} lacks its dependency-verification tag')
            row = campaign_row(workspace, spec, 'prepare')
            row['tags'] = sorted(set(spec['tags'] + [host]))
            preparation.append(row)
        prepared_rows = run_stage(preparation, workspace, 'prepare', args.wait_s)
        prepared = prepared_rows[0]
        if any(row != prepared for row in prepared_rows[1:]):
            raise ValueError('eligible workers disagree on immutable source identity')
        spec.update(prepared)
        spec['contract'] = model.digest_json(spec)
        model.atomic_json(workspace / 'job.json', spec)
    print(f"export {spec['contract']}: {spec['count']} whole-layer actions", flush=True)
    rows = [campaign_row(workspace, spec, 'encode', index=index) for index in range(spec['count'])]
    parts = run_stage(rows, workspace, 'encode', args.wait_s)
    if [part['index'] for part in parts] != list(range(spec['count'])) or any(part['contract'] != spec['contract'] for part in parts):
        raise ValueError('receipt population or export identity mismatch')
    for index, part in enumerate(parts):
        # Full payload hashing runs in the admitted assembler, never as an
        # unreserved large validation stage on the submitting machine.
        if model.read_json(Path(spec['parts']) / f'part-{index:05d}' / 'pb-result.json') != part:
            raise ValueError('part metadata differs from CAS receipt')
    assembly = workspace / '.pb-state/assembly'
    if not assembly.exists():
        shutil.copytree(workspace, assembly, ignore=shutil.ignore_patterns('.git', '.pb-state'))
        subprocess.run(['git', 'init', '-q', str(assembly)], check=True)
        subprocess.run(['git', '-C', str(assembly), '-c', 'user.name=PrismaBuild',
                        '-c', 'user.email=prismabuild@localhost', 'commit', '--allow-empty',
                        '-qm', 'PrismaBuild assembly barrier'], check=True)
    model.atomic_json(assembly / 'barrier.json', parts)
    result = run_stage([campaign_row(assembly, spec, 'assemble')], workspace, 'assemble', args.wait_s)[0]
    if model.read_json(Path(spec['out']) / 'pb-result.json') != result:
        raise ValueError('assembled output metadata differs from its CAS receipt')
    model.atomic_json(workspace / '.pb-state/export-result.json', result)
    print(json.dumps({'out': spec['out'], 'contract': spec['contract'], 'workspace': str(workspace)}))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError, subprocess.CalledProcessError) as exc:
        raise SystemExit(f'dispatch_tessera_model: {exc}')
