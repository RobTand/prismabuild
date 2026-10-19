"""Literal admission boundaries without queue or controller substitution."""
import pytest

from prismabuild import candidate_policy as policy, local_scratch


@pytest.mark.parametrize("blocker,adaptive,funded,expected", [
    (None, None, False, False),
    ({"action_key": "measurement"}, None, False, True),
    ({"action_key": "measurement"}, {"funded_by": "measurement"}, True, False),
    ({"action_key": "measurement"}, {"funded_by": "other"}, True, True),
    ({"action_key": "measurement"}, {"funded_by": "measurement"}, False, True),
])
def test_only_the_elected_producers_funding_crosses_its_fence(blocker, adaptive, funded, expected):
    assert policy.unfunded_measurement_blocker(blocker, adaptive, funded=funded) is expected


@pytest.mark.parametrize("held_back,incumbent,measurement,expected", [
    (False, False, False, False), (False, True, True, False),
    (True, False, True, True), (True, True, False, True), (True, True, True, False),
])
def test_incumbent_dependents_bypass_only_the_current_measurement_withhold(
        held_back, incumbent, measurement, expected):
    order = policy.ScanOrder(withheld_for="head",
                             measurement_withholds=frozenset({"head"}) if measurement else frozenset())
    assert policy.held_back_unfunded(order, held_back=held_back, serves_incumbent=incumbent) is expected


@pytest.mark.parametrize("item,evidence,observed,expected", [
    ({}, {"generation": "g1", "attempts": 0}, "g1", False),
    ({"attempts": 0}, {"generation": "g1", "attempts": 0}, "g1", False),
    ({"attempts": 1}, {"generation": "g1", "attempts": 0}, "g1", True),
    ({}, {"generation": "g2", "attempts": 0}, "g1", True),
    ({}, {"generation": "g1", "attempts": 1}, "g1", True),
    ({}, {}, "g2", True),
])
def test_scratch_evidence_belongs_to_its_original_publication_and_attempt(
        item, evidence, observed, expected):
    assert local_scratch.admission_generation_changed(
        item, evidence, generation="g1", attempt=0, observed_generation=observed) is expected


@pytest.mark.parametrize("changed,expired,reason", [
    (False, False, None),
    (False, True, "local_scratch_io_profile_expired"),
    (True, False, "local_scratch_io_generation_changed"),
    (True, True, "local_scratch_io_generation_changed"),
])
def test_changed_scratch_identity_wins_over_expiry(changed, expired, reason):
    assert local_scratch.admission_refusal(changed=changed, expired=expired) == reason


def test_a_changed_generation_skips_later_mapping_reads():
    class Unread(dict):
        def get(self, *args):
            raise AssertionError("attempt read after changed generation")

        def __getitem__(self, key):
            raise AssertionError("evidence read after changed generation")

    assert local_scratch.admission_generation_changed(
        Unread(), Unread(), generation="old", attempt=0, observed_generation="new")


def test_unfunded_credit_never_looks_up_an_owner():
    class Unread(dict):
        def get(self, *args):
            raise AssertionError("unfunded owner read")

    assert policy.unfunded_measurement_blocker({"action_key": "head"}, Unread(), funded=False)


@pytest.mark.parametrize("filed,funded,borrowed,waived,wanted,incomplete", [
    (0, {}, None, None, 0, False),
    (3, {}, None, None, 4, True),
    (4, {}, None, None, 4, False),
    (2, {"stage": {"kinds": {"stage_gib": 2}}}, None, None, 4, False),
    (1, {"stage": {"kinds": {"stage_gib": 2}}}, None, None, 4, True),
    (1, {}, {"family": {"borrowed": "2"}}, {"waived": "1"}, 4, False),
    (3, {"unknown": None}, {"unknown": None}, None, 4, True),
    (9, {}, None, None, 4, False),
])
def test_only_filed_funded_borrowed_or_waived_tier_capacity_counts(
        filed, funded, borrowed, waived, wanted, incomplete):
    demand = {} if wanted == 0 else {"stage": {"stage_gib": 2}, "ram": {"ram_gib": 2}}
    verdict = policy.tier_completeness(demand, funded, borrowed, waived, filed=filed)
    assert verdict.wanted == wanted
    assert verdict.incomplete is incomplete


@pytest.mark.parametrize("verdict,backfill,priority,position", [
    (None, [], None, policy.MovedClaimPosition.PLACEMENT),
    ({"container_work_eligible": True}, [], 7, policy.MovedClaimPosition.PLACEMENT),
    ({"container_work_eligible": False}, ["election"], -10, policy.MovedClaimPosition.CLASS_REFUSED),
    (None, ["election"], -9, policy.MovedClaimPosition.BACKFILL_REFUSED),
    (None, ["election"], "-10", policy.MovedClaimPosition.BACKFILL_REFUSED),
    (None, ["election"], -10, policy.MovedClaimPosition.BACKFILL_BINDING),
])
def test_moved_class_and_backfill_priority_decide_the_next_required_proof(
        verdict, backfill, priority, position):
    assert policy.moved_claim_position(verdict, {"priority": priority}, backfill) is position


def test_class_refusal_and_an_ordinary_row_skip_backfill_priority():
    class Unread(dict):
        def get(self, *args):
            raise AssertionError("backfill priority observed")

    assert policy.moved_claim_position(
        {"container_work_eligible": False}, Unread(), ["election"]) is policy.MovedClaimPosition.CLASS_REFUSED
    assert policy.moved_claim_position(None, Unread(), []) is policy.MovedClaimPosition.PLACEMENT
