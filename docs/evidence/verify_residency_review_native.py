"""Reconcile actual C21 controls and final runtime records without test execution."""
import argparse,hashlib,json,subprocess
from pathlib import Path
from retained_native_bundle import read_native_action,combine_actions


def verify_c21_review_native(root,phases):
    results=[]
    for phase in phases:
        location=root/phase;verified=json.loads((location/'verified.json').read_bytes())
        assert verified['all_verified']
        native=[]
        for proof in verified['actions']:
            directory=location/'raw'/proof['action_key']
            row=read_native_action(directory,proof);native.append(row)
            assert row['source']==verified['source_head']
            runtime=[]
            plan=json.loads((root/(phase+'-plan.json')).read_bytes())
            prefix=plan.get('runtime_record_prefix','pbtest-runtime: ')
            assert prefix in ('pbtest-runtime: ','prismabuild-test-runtime: ')
            for line in row['lines']:
                if not line.startswith(prefix):continue
                record=json.loads(line.removeprefix(prefix))
                if set(record['files'])==row['files']:runtime.append(record)
            if phase.startswith(('final-boundary-','final-observer-')):
                plan=json.loads((root/(phase+'-plan.json')).read_bytes())
                assert len(runtime)==1,(phase,proof['action_key'],len(runtime))
                record=runtime[0]
                if prefix=='prismabuild-test-runtime: ':assert record['complete'] is True and 'reason' not in record
                else:assert 'complete' not in record
                assert record['version'][:2]==plan['expected_python_major_minor']
                assert record['executable_path_matches_running'] is True
                assert set(record['native_threads'].values())=={'1'}
                allocation=proof['cpu_allocation'];assert set(record['affinity'])==set(allocation['preferred']+allocation['fallback'])
                raw=subprocess.check_output(['git','-C',plan['checkout'],'show',plan['pin']+':tests/conftest.py'])
                assert hashlib.sha256(raw).hexdigest()==record['conftest_sha256']
                descriptor=record['executable'];copy=Path(descriptor['resolved_path'])
                if descriptor['sha256']=='c86f21d0da9290ece2d7d49a3f7ed52aed59cb029abdf0ea25c79634f6cbe960':
                    copy=Path('/mnt/shared/prismabuild-agent-evidence-rob/refactor-20261016/active/final-scratch-profile/sparky-python3.12')
                executable=copy.read_bytes();assert len(executable)==descriptor['bytes'] and hashlib.sha256(executable).hexdigest()==descriptor['sha256']
                request=json.loads((directory/'request.json').read_bytes());assert request['params']['command'][0]==plan['python']
        cohort=combine_actions(native)
        plan=json.loads((root/(phase+'-plan.json')).read_bytes())
        assert cohort['nodes'].keys()==set(Path(plan['expected_nodes_file']).read_text().splitlines())
        assert cohort['files']==set(plan['files'])
        if phase.startswith('budget-red-'):
            assert cohort['counts']=={'passed':8,'failed':1}
            assert [k for k,v in cohort['nodes'].items() if v=='failed']==['tests/test_residency_window_budget_authority.py::test_a_window_decision_requires_a_captured_free_budget']
        elif phase.startswith('runtime-unavailable-red-'):
            assert cohort['counts']=={'passed':8,'failed':1}
            assert [k for k,v in cohort['nodes'].items() if v=='failed']==['tests/test_pytest_runtime_evidence.py::test_an_unreadable_runtime_is_reported_as_unavailable']
        elif phase.startswith('planner-original-'):assert cohort['counts']=={'passed':9}
        elif phase.startswith('planner-moved-'):
            assert cohort['counts']=={'passed':8,'failed':1}
            assert [k for k,v in cohort['nodes'].items() if v=='failed']==['tests/test_planner_captures_the_floor_before_template_refusal.py::test_the_planner_captures_floor_cwd_before_refusing_a_template']
        elif phase.startswith(('final-boundary-','final-observer-')):assert cohort['counts']=={'passed':plan['expected_nodes']}
        assert len([k for k in cohort['nodes'] if k.startswith('tests/test_duplication_baseline.py::')])==8
        results.append(dict(phase=phase,source=verified['source_head'],actions=len(native),native=len(cohort['nodes']),counts=cohort['counts'],runtime_checked_per_action=phase.startswith(('final-boundary-','final-observer-')),all_native_and_physical_scopes_reconciled=True))
    return dict(verified=True,phases=results,source_runtime_deployment_axes_separate=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,required=True);parser.add_argument('--phases',nargs='+',required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();result=verify_c21_review_native(args.root,args.phases);args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
