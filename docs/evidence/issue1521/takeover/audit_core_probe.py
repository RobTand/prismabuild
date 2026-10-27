"""Collect and verify the two original six-ID boundary actions; no submission."""
from pathlib import Path
import datetime,hashlib,json,subprocess
P=Path(__file__).resolve().parent;CAS=Path('/mnt/shared/prismabuild-fleet/cas');plan=json.loads((P/'core-plan.json').read_text())
main=json.loads((P/'core-main-proof.json').read_text())[0];nodes={r[0] for r in main['observed']}
def sha(raw):return hashlib.sha256(raw).hexdigest()
def check(ref):
 p=Path(ref.get('path') or CAS/'blobs'/ref['sha256'][:2]/ref['sha256'])
 with p.open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==ref['sha256']
 assert p.stat().st_size==ref['bytes'];return p
keys=[]
for row in plan['rows']:
 d=json.loads((P/('core-'+row['label']+'.json.progress.json')).read_text());ks=[s['action_key'] for s in d['shards'] if s.get('action_key')];assert len(ks)==1;keys+=ks
r=subprocess.run(['python3','/mnt/shared/prismabuild-fleet/repo/tools/pbwait.py','--endings','--wait-s','0',*keys],capture_output=True,text=True);(P/'core-endings.json').write_text(r.stdout);data=json.loads(r.stdout);assert data['complete'];by={e['action_key']:e for e in data['endings']}
evidence=[]
for row,key in zip(plan['rows'],keys):
 e=by[key]
 if not e.get('result'):evidence.append(dict(label=row['label'],key=key,status=e['status'],terminal=False));continue
 assert e['result']['verified'] and e['diagnostics']['cleanup']['complete'];cs=e['diagnostics']['cleanup']['resource_scope']['export'];assert cs['empty'] and cs['settled']
 req=json.loads((CAS/'requests'/key[:2]/(key+'.json')).read_text());assert req['params']['checkout_snapshot']['parent']==row['head'] and req['params']['demand']=={'cpu':2,'mem_gb':4} and req['params']['execution_timeout_s']==900
 for stream in ['stdout','stderr']:check(e['result'][stream])
 lines=check(e['result']['stdout']).read_text().splitlines();rec={}
 for kind,prefix in [('c','pbtest-completion: '),('o','pbtest-outcomes: '),('rt','prismabuild-test-runtime: ')]:
  found=[json.loads(l[len(prefix):]) for l in lines if l.startswith(prefix)];assert len(found)==1;rec[kind]=found[0]
 c,o,rt=[rec[k] for k in ['c','o','rt']]
 assert c['collection_complete'] and not c['collection_errors'] and c['run_nonce']==o['run_nonce']
 assert set(c['collected'])==set(c['teardown_finished'])==set(o['collected'])==nodes
 assert not o['uncounted'] and not o['bound_failures'] and len(o['deselected'])==67
 assert rt['complete'] and rt['exitstatus']==e['exit_code'] and rt['kernel']['machine']=='x86_64'
 assert len(rt['affinity'])==2 and rt['native_threads']=={'OMP_NUM_THREADS':'1','MKL_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1'}
 observed={r[0]:r[2] for r in o['reports'] if r[1]=='call'};assert set(observed)==nodes
 receipt=e['result']['receipt_sha256']
 if e['succeeded']:
  assert all(v=='passed' for v in observed.values()) and receipt
  rr=json.loads((CAS/'executions/v1'/receipt[:2]/(receipt+'.json')).read_text());assert rr['action_key']==key and req['inputs']==rr['producer']['inputs']
  assert sha(json.dumps({k:v for k,v in rr.items() if k!='receipt_sha256'},sort_keys=True,separators=(',',':')).encode())==receipt;check(rr['result'])
 else:assert not receipt and any(v=='failed' for v in observed.values())
 snap,=[i for i in req['inputs'] if i['id']=='pbrun.checkout-snapshot'];bundle=check(snap);repo=P/('core-source-'+snap['sha256'][:12]+'.git')
 if not repo.exists():subprocess.run(['git','clone','--bare','--quiet',str(bundle),str(repo)],check=True,capture_output=True)
 git=lambda *a:subprocess.check_output(['git','--git-dir',str(repo),*a]).decode().strip()
 commit=git('rev-parse','refs/heads/prismabuild-snapshot');assert git('show','-s','--format=%P',commit)==row['head']
 changes=git('diff-tree','--no-commit-id','--name-status','-r',row['head'],commit).splitlines();assert len(changes)==1 and changes[0].startswith('A\t.pbrun-closure.')
 for path,digest in rt['selected_source_sha256'].items():assert sha(subprocess.check_output(['git','--git-dir',str(repo),'show',row['head']+':'+path]))==digest
 golden_failure='AssertionError: current writer changed golden bytes' in '\n'.join(lines)
 evidence.append(dict(label=row['label'],head=row['head'],key=key,status=e['status'],terminal=True,observed=observed,logs={s:e['result'][s] for s in ['stdout','stderr']},receipt_sha256=receipt,snapshot=snap,native_runtime=rt,cleanup_complete=True,deselected=len(o['deselected']),golden_failure=golden_failure))
terminal=all(e['terminal'] for e in evidence)
causal=terminal and all(v=='passed' for v in evidence[0]['observed'].values()) and all(v=='failed' for v in evidence[1]['observed'].values()) and evidence[1]['golden_failure']
result=dict(observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),terminal=terminal,entries=evidence,main_key=main['key'],main_source_tree_equals_current_main=True,attributed_to=2344 if causal else None,limits='Six original failed IDs only; original statuses remain. No source fix, runtime operation or renewed approval.')
(P/'core-adjudication.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(dict(terminal=terminal,attributed_to=result['attributed_to'],entries=[{k:e.get(k) for k in ['label','key','status','terminal','receipt_sha256','deselected']} for e in evidence])))
