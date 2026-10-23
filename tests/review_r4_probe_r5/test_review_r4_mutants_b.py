"""Opus review r4: r3 mutants #3-6 (computed keys, endings, Pool receivers)."""
import pytest

import review_r4_mutation_harness as h


@pytest.mark.parametrize('name', ['computed_banned_keys-laundered', 'computed_banned_keys-plain',
                                  'second_judgment_function-laundered', 'second_judgment_function-plain',
                                  'unscanned_pool_local_receiver', 'unscanned_pool_self_receiver'])
def test_mutant_is_caught_by_the_branch_suite(tmp_path, name):
    h.check_caught(tmp_path, name)
