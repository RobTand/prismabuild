"""One mutation proof per admitted action; never a test-suite entrypoint.

Invoke through published pbrun/pbcampaign. The worker owns its materialized
checkout. The original broker bytes are restored before the action ends.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

MUTANTS = {'cancel': ('        self._wake()\n',
            '        pass\n',
            ['test_monitor_close_wakes_blocked_event_wait']),
 'capacity': ('if (self.last_public is not None and (stopped or errors or not '
              'self.event_coverage_complete',
              'if (False and (stopped or errors or not self.event_coverage_complete',
              ['test_event_only_capacity_invalidation_keeps_numeric_timestamp']),
 'deleted_file': ('if mask&self.loss or replaced:',
                  'if False:',
                  ['test_linux_cgroup_event_source_reports_deleted_input_as_lost_coverage']),
 'dirty': ('for scope in dirty:',
           'for scope in ():',
           ['test_cgroup_event_invalidations_read_only_dirty_scope_and_do_not_sample_gpu']),
 'enrollment': ("self.events.watch(scope,self.authority.backend.path(scope),interests[scope]['cgroup_identity'])",
                'pass',
                ['test_cgroup_watch_is_enrolled_before_initial_oom_snapshot',
                 'test_new_scope_create_wakes_watch_reconciliation',
                 'test_recovered_scopes_are_watched_before_first_numeric_sample']),
 'event_health': ('    def poll_events_once(self):\n        self._consume_wake()',
                  '    def poll_events_once(self):\n'
                  '        self.last_success_monotonic=self.clock()\n'
                  '        self._consume_wake()',
                  ['test_cgroup_event_invalidations_read_only_dirty_scope_and_do_not_sample_gpu']),
 'identity': ('if scope not in interests or not '
              'self.observations[scope].matches(interests[scope],self.identity_fields):',
              'if scope not in interests:',
              ['test_cgroup_watch_identity_includes_attempt_and_budgets']),
 'modified_file': ('dirty.add(scope)',
                   'pass',
                   ['test_linux_cgroup_event_source_invalidates_on_each_watched_file']),
 'numeric_fairness': ('numeric=self.clock()>=next_numeric',
                      'numeric=self.last_success_monotonic is None',
                      ['test_continuous_events_do_not_postpone_numeric_deadline']),
 'overflow': ("raise OSError(errno.ENOBUFS,'cgroup notification queue overflow')",
              'continue',
              ['test_linux_cgroup_event_source_overflow_invalidates_all_retained_scopes']),
 'retired_interest': ("and _recorded_identity(record.get('cgroup_identity'))}",
                      "and not record.get('stopped_unix') and "
                      "_recorded_identity(record.get('cgroup_identity'))}",
                      ['test_retired_unreleased_scope_keeps_population_interest']),
 'source_reset': ('    def _source_failed(self,error):\n'
                  '        if self.events is not None:self.events.close()',
                  '    def _source_failed(self,error):\n        pass',
                  ['test_cgroup_source_failure_recreates_descriptor_after_recovery_deadline']),
 'stale_snapshot': ("entry.coverage='lost';entry.snapshot=None;entry.error",
                    "entry.coverage='lost';entry.error",
                    ['test_cgroup_watch_loss_is_visible_and_does_not_spin_on_numeric_ticks']),
 'stop_retry': ("if record.get('monitor_stop_pending'):",
                "if record.get('monitor_stop_pending') and scope in refreshed:",
                ['test_cgroup_event_stop_retry_does_not_require_another_notification']),
 'work_bound': ("raise OSError(errno.ENOBUFS,'cgroup notification event budget exceeded')",
                'pass',
                ['test_linux_cgroup_event_source_caps_work_before_discarding_uncertain_queue'])}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mutant',choices=sorted(MUTANTS))
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[2]
    path=root/'tools/fleet/resource_broker.py'
    before=path.read_bytes()
    old,new,tests=MUTANTS[args.mutant]
    text=before.decode()
    if text.count(old)!=1:raise RuntimeError('mutation no longer matches exactly once')
    changed=text.replace(old,new).encode()
    try:
        path.write_bytes(changed)
        with tempfile.TemporaryDirectory(prefix='broker-mutant-',dir=os.environ['TMPDIR']) as directory:
            report=Path(directory)/'junit.xml'
            command=[sys.executable,'-m','pytest','tests/test_resource_broker.py','-q',
                     '-k',' or '.join(tests),'--junitxml='+str(report)]
            result=subprocess.run(command,cwd=root)
            cases=list(ET.parse(report).getroot().iter('testcase'))
            failed=[case for case in cases if case.find('failure') is not None]
            errors=[case for case in cases if case.find('error') is not None]
            names={case.attrib['name'].split('[')[0] for case in cases}
            if result.returncode!=1 or not cases or errors or len(failed)!=len(cases) or names!=set(tests):
                raise RuntimeError('mutant was not killed by every intended call assertion')
            print(json.dumps({'schema':'prismabuild.broker_mutant.v1','mutant':args.mutant,
                'killed':True,'cases':[case.attrib['name'] for case in failed],
                'before_sha256':hashlib.sha256(before).hexdigest(),
                'mutated_sha256':hashlib.sha256(changed).hexdigest()},sort_keys=True),flush=True)
    finally:
        path.write_bytes(before)


if __name__=='__main__':
    main()
