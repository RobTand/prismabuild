"""Opus review r4: harness control/canary and lifecycle live-key mutants (r3 #1-2)."""
import pytest

import review_r4_mutation_harness as h


def test_harness_control_unmutated_suite_passes(tmp_path):
    rc, summary, marks, elapsed = h.run_suite(tmp_path, [])
    h.warnings.warn(f'R4-MUTANT control: rc={rc} elapsed={elapsed}s summary={summary}')
    assert rc == 0, summary


def test_harness_canary_broken_judgment_is_caught(tmp_path):
    rc, summary, marks, elapsed = h.run_suite(tmp_path, h.CANARY)
    h.warnings.warn(f'R4-MUTANT canary: rc={rc} elapsed={elapsed}s summary={summary}')
    assert rc == 1, summary


@pytest.mark.parametrize('name', ['live_key_local_variable-laundered', 'live_key_local_variable-plain',
                                  'live_key_getattr-laundered', 'live_key_getattr-plain'])
def test_mutant_is_caught_by_the_branch_suite(tmp_path, name):
    h.check_caught(tmp_path, name)
