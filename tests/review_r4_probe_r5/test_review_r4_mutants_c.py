"""Opus review r4: r3 mutants #7-8 and the leaked-authority spellings."""
import pytest

import review_r4_mutation_harness as h


@pytest.mark.parametrize('name', ['census_unbanned_key-plain', 'census_unbanned_key-laundered',
                                  'retain_check_satisfied_by_comment', 'leaked_authority_type_of_instance',
                                  'leaked_authority_object_new'])
def test_mutant_is_caught_by_the_branch_suite(tmp_path, name):
    h.check_caught(tmp_path, name)
