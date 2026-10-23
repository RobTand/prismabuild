"""Opus review r4: consumers that duck-type instead of requiring the capability."""
import pytest

import review_r4_mutation_harness as h


@pytest.mark.parametrize('name', ['duck_pbstatus_accepts_any_mapping', 'duck_census_projects_stored_string',
                                  'duck_endings_keeps_stored_projection', 'duck_endings_routes_on_stored_reason'])
def test_mutant_is_caught_by_the_branch_suite(tmp_path, name):
    h.check_caught(tmp_path, name)
