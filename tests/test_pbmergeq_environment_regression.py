"""Failure attribution requires comparable observations, including actual workers."""

import dataclasses

import pytest

from test_pbmergeq import FakeRunner, entries, make_queue, mq, statuses


@pytest.mark.parametrize('different_run', ['base', 'rerun'])
def test_a_different_worker_cannot_make_an_unchanged_failure_new_or_flaky(tmp_path, different_run):
    queue, origin, github, _ = make_queue(tmp_path)
    origin.pr(1, {'readme.txt': 'documentation only\n'})
    node = 'tests/test_a.py::environment_sensitive'

    class DifferentWorkers(FakeRunner):
        def run_many(self, batch, outdir, jobs):
            results = super().run_many(batch, outdir, jobs)
            for name, checkout, files in jobs:
                # The same source fails on x86 and passes on GB10. The gate's
                # independent submissions change only the actual worker.
                worker = 'sparky' if name == different_run else 'dl380g10'
                result = results[name]
                results[name] = dataclasses.replace(result,
                    failed={node} if worker == 'dl380g10' else set(),
                    runtime={**mq.checkout_runtime(queue.cfg, checkout), 'worker': worker},
                    actions=[f'{name}-{worker}'])
            return results

    queue.runner = DifferentWorkers()
    batch = queue.run_batch([{**entries(1)[0], 'attempts': 1}])

    assert batch['verdict'] == 'attribution-inconclusive', batch
    assert batch['runs']['candidate']['failed'] == [node]
    assert 'culprit' not in batch
    assert statuses(github) == []
    assert queue.store.state['flakes'] == {}
    assert queue.store.state['queue'][0]['attempts'] == 1
