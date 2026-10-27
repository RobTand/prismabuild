"""Audit retained PB reports and source bundles without executing product code."""
import argparse,hashlib,json,pathlib,subprocess
from collections import Counter
p=pathlib.Path(__file__).resolve().parent
parser=argparse.ArgumentParser();parser.add_argument('report',type=pathlib.Path);parser.add_argument('--source',required=True);parser.add_argument('--machine');parser.add_argument('--allow-failures',action='store_true');parser.add_argument('--allow-empty-selection',action='store_true');args=parser.parse_args()
ns={};exec(compile((p/'audit_run.py').read_text().split('\nreport = Path(sys.argv[1])')[0],'audit_run.py','exec'),ns)
root=pathlib.Path('/home/rob/paperclip-worktrees/prismabuild/v21-integrate-2219');base=pathlib.Path('/mnt/shared/prismabuild-fleet');bundles=p/'audit-bundles.git'
if not bundles.exists():subprocess.run(['git','init','--bare','--quiet',str(bundles)],check=True)
sha=lambda raw:hashlib.sha256(raw).hexdigest()
def git(*cmd):return subprocess.check_output(['git',*cmd],cwd=root,text=True).strip()
def marker(log,prefix):
 rows=[json.loads(l[len(prefix):]) for l in log.splitlines() if l.startswith(prefix)];assert len(rows)==1,(prefix,len(rows));return rows[0]
proofs=[];checked={}
for row in json.loads(args.report.read_text()):
 proof=ns['audit'](row,allow_empty=args.allow_empty_selection);terminal=json.loads(pathlib.Path(proof['terminal']).read_text());log=(base/'pb-queue'/proof['logs']['stdout']['path']).read_text();completion=marker(log,'pbtest-completion: ');outcomes=marker(log,'pbtest-outcomes: ');runtime=marker(log,'prismabuild-test-runtime: ')
 assert completion['collection_complete'] and not completion['collection_errors'] and not completion['collect_only']
 assert not completion['duplicate_teardown']
 assert set(completion['collected'])==set(completion['teardown_finished'])==set(completion['outcome_nodeids'])==set(outcomes['collected'])
 assert completion['run_nonce']==outcomes['run_nonce']==row['test_completion']['run_nonce']
 assert runtime['complete'] and runtime['exitstatus']==(5 if args.allow_empty_selection else row['returncode'])
 assert args.machine is None or runtime['kernel']['machine']==args.machine
 assert runtime['native_threads']==dict(OMP_NUM_THREADS='1',MKL_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
 for name,digest in runtime['selected_source_sha256'].items():assert sha(subprocess.check_output(['git','show',args.source+':'+name],cwd=root))==digest,name
 assert sha(subprocess.check_output(['git','show',args.source+':tests/conftest.py'],cwd=root))==runtime['conftest_sha256']
 snapshot=terminal['checkout_snapshot'];assert snapshot['parent']==args.source
 commit=snapshot['commit']
 if commit not in checked:
  desc=snapshot['input'];bundle=base/'cas/blobs'/desc['sha256'][:2]/desc['sha256']
  subprocess.run(['git','--git-dir='+str(bundles),'fetch','--quiet',str(bundle),commit],check=True)
  checked[commit]={}
  for directory in ('src','tools','tests'):
   tree=subprocess.check_output(['git','--git-dir='+str(bundles),'rev-parse',commit+':'+directory],text=True).strip();assert tree==git('rev-parse',args.source+':'+directory);checked[commit][directory]=tree
 proof.update(machine=args.machine,source_trees=checked[commit],native_runtime_sha256=sha(json.dumps(runtime,sort_keys=True).encode()),native_completion_sha256=sha(json.dumps(completion,sort_keys=True).encode()),failing_nodes=row['verdict'].get('failing_nodes',[]),files=row['files'])
 if args.allow_empty_selection:assert not row['verdict']['green'] and not completion['collected']
 elif not args.allow_failures:assert row['returncode']==0 and row['verdict']['green'],proof['action_key']
 proofs.append(proof)
summary={'report':str(args.report),'source':args.source,'machine':args.machine,'counts':dict(sum((Counter(r['counts']) for r in proofs),Counter())),'actions':proofs,'files':sorted({f for r in proofs for f in r['files']}),'failed_actions':sum(r['returncode']!=0 for r in proofs),'all_primary_evidence_verified':True,'qualifies':not args.allow_empty_selection and all(r['returncode']==0 for r in proofs)}
dest=args.report.with_suffix('.audited.json');dest.write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps({k:v for k,v in summary.items() if k not in ('actions','files')}))
